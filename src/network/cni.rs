use super::*;
use anyhow::{Context, Result};
use log::{debug, error, info};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;

/// CNI网络配置
#[derive(Debug, Clone)]
pub struct CniNetworkConfig {
    /// 网络名称
    pub name: String,
    /// CNI版本
    pub cni_version: String,
    /// 插件类型
    pub plugin_type: String,
    /// 其他配置参数
    pub config: Value,
}

/// CNI插件管理器
#[derive(Debug)]
pub struct CniManager {
    /// CNI插件目录
    plugin_dirs: Vec<PathBuf>,
    /// CNI配置文件目录
    config_dirs: Vec<PathBuf>,
    /// 缓存目录
    cache_dir: PathBuf,
    /// 网络配置缓存
    network_configs: std::collections::HashMap<String, CniNetworkConfig>,
}

impl CniManager {
    /// 创建新的CNI管理器
    pub fn new(
        plugin_dirs: Vec<String>,
        config_dirs: Vec<String>,
        cache_dir: String,
    ) -> Result<Self> {
        Ok(Self {
            plugin_dirs: plugin_dirs.into_iter().map(PathBuf::from).collect(),
            config_dirs: config_dirs.into_iter().map(PathBuf::from).collect(),
            cache_dir: PathBuf::from(cache_dir),
            network_configs: std::collections::HashMap::new(),
        })
    }

    /// 加载网络配置
    pub async fn load_network_configs(&mut self) -> Result<()> {
        for config_dir in &self.config_dirs {
            if !config_dir.exists() {
                debug!("CNI config directory does not exist: {:?}", config_dir);
                continue;
            }

            let mut entries = tokio::fs::read_dir(config_dir)
                .await
                .context("Failed to read CNI config directory")?;

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if file_name.ends_with(".conf") || file_name.ends_with(".json") {
                    match self.load_single_config(&path).await {
                        Ok(config) => {
                            info!("Loaded CNI config: {}", config.name);
                            self.network_configs.insert(config.name.clone(), config);
                        }
                        Err(e) => {
                            error!("Failed to load CNI config {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        info!("Loaded {} CNI network configs", self.network_configs.len());
        Ok(())
    }

    /// 加载单个CNI配置文件
    async fn load_single_config(&self, path: &PathBuf) -> Result<CniNetworkConfig> {
        let content = tokio::fs::read_to_string(path)
            .await
            .context("Failed to read CNI config file")?;

        let config_value: Value =
            serde_json::from_str(&content).context("Failed to parse CNI config JSON")?;

        let name = config_value
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let cni_version = config_value
            .get("cniVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("0.4.0")
            .to_string();

        let plugin_type = config_value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("bridge")
            .to_string();

        Ok(CniNetworkConfig {
            name,
            cni_version,
            plugin_type,
            config: config_value,
        })
    }

    /// 设置Pod网络
    pub async fn setup_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
    ) -> Result<NetworkStatus> {
        let config = self
            .network_configs
            .values()
            .next()
            .context("No CNI network configuration found")?;

        debug!("Using CNI network config: {}", config.name);

        let cni_args = self.build_cni_args(pod_id, netns, pod_name, pod_namespace);
        let result = self.exec_cni_plugin(config, "ADD", &cni_args).await?;
        let network_status = self.parse_cni_result(&result)?;

        info!("Pod {} network setup completed", pod_id);
        Ok(network_status)
    }

    /// 清理Pod网络
    pub async fn teardown_pod_network(&self, pod_id: &str, netns: &str) -> Result<()> {
        if let Some(config) = self.network_configs.values().next() {
            let cni_args = self.build_cni_args(pod_id, netns, "", "");

            match self.exec_cni_plugin(config, "DEL", &cni_args).await {
                Ok(_) => info!("Pod {} network teardown completed", pod_id),
                Err(e) => error!("Failed to teardown network for pod {}: {}", pod_id, e),
            }
        }
        Ok(())
    }

    /// 构建CNI参数
    fn build_cni_args(
        &self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
    ) -> Value {
        json!({
            "cniVersion": "0.4.0",
            "name": "crius-net",
            "type": "bridge",
            "bridge": "cni0",
            "isGateway": true,
            "ipMasq": true,
            "ipam": {
                "type": "host-local",
                "subnet": "10.88.0.0/16",
                "routes": [{"dst": "0.0.0.0/0"}]
            },
            "dns": {"nameservers": ["8.8.8.8"]},
            "args": {
                "cni": {
                    "podId": pod_id,
                    "podName": pod_name,
                    "podNamespace": pod_namespace
                }
            },
            "netns": netns,
            "ifName": "eth0"
        })
    }

    /// 执行CNI插件
    async fn exec_cni_plugin(
        &self,
        config: &CniNetworkConfig,
        command: &str,
        cni_args: &Value,
    ) -> Result<String> {
        let plugin_name = if config.plugin_type == "chain" {
            config
                .config
                .get("plugins")
                .and_then(|p| p.as_array())
                .and_then(|arr| arr.first())
                .and_then(|p| p.get("type"))
                .and_then(|t| t.as_str())
                .unwrap_or("bridge")
        } else {
            &config.plugin_type
        };

        let plugin_path = self
            .find_plugin(plugin_name)
            .context(format!("CNI plugin '{}' not found", plugin_name))?;

        let mut cmd = Command::new(&plugin_path);
        cmd.env("CNI_COMMAND", command)
            .env("CNI_NETNS", "")
            .env("CNI_CONTAINERID", "")
            .env("CNI_IFNAME", "eth0")
            .env(
                "CNI_PATH",
                self.plugin_dirs
                    .iter()
                    .map(|p| p.to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                    .join(":"),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().context("Failed to spawn CNI plugin")?;

        let mut stdin = child.stdin.take().context("Failed to get stdin")?;
        let cni_json = serde_json::to_string(cni_args)?;

        tokio::spawn(async move {
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stdin, cni_json.as_bytes()).await;
        });

        let output = child
            .wait_with_output()
            .await
            .context("Failed to wait for CNI plugin")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("CNI plugin failed: {}", stderr));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// 查找CNI插件
    fn find_plugin(&self, plugin_type: &str) -> Option<PathBuf> {
        self.plugin_dirs
            .iter()
            .map(|dir| dir.join(plugin_type))
            .find(|path| path.exists())
    }

    /// 解析CNI结果
    fn parse_cni_result(&self, result: &str) -> Result<NetworkStatus> {
        if result.trim().is_empty() {
            return Ok(NetworkStatus {
                name: "crius-net".to_string(),
                ip: Some("10.88.0.1".parse().unwrap()),
                mac: None,
                interfaces: vec![],
            });
        }

        let value: Value = serde_json::from_str(result).context("Failed to parse CNI result")?;

        let ip = value
            .get("ips")
            .and_then(|ips| ips.as_array())
            .and_then(|arr| arr.first())
            .and_then(|ip| ip.get("address"))
            .and_then(|addr| addr.as_str())
            .and_then(|s| s.split('/').next())
            .and_then(|ip_str| ip_str.parse().ok());

        Ok(NetworkStatus {
            name: "crius-net".to_string(),
            ip: ip.or_else(|| Some("10.88.0.1".parse().unwrap())),
            mac: None,
            interfaces: vec![],
        })
    }
}
