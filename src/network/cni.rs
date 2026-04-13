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
    _cache_dir: PathBuf,
    /// 网络配置缓存
    network_configs: std::collections::HashMap<String, CniNetworkConfig>,
    /// 默认使用的网络配置名称
    default_network_name: Option<String>,
}

impl CniManager {
    fn path_is_executable(path: &std::path::Path) -> bool {
        let Ok(metadata) = std::fs::metadata(path) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    fn primary_plugin_type(config_value: &Value) -> String {
        if let Some(plugin_type) = config_value.get("type").and_then(|v| v.as_str()) {
            if !plugin_type.trim().is_empty() {
                return plugin_type.to_string();
            }
        }

        config_value
            .get("plugins")
            .and_then(|plugins| plugins.as_array())
            .and_then(|plugins| plugins.first())
            .and_then(|plugin| plugin.get("type"))
            .and_then(|plugin_type| plugin_type.as_str())
            .filter(|plugin_type| !plugin_type.trim().is_empty())
            .unwrap_or("bridge")
            .to_string()
    }

    /// 创建新的CNI管理器
    pub fn new(
        plugin_dirs: Vec<String>,
        config_dirs: Vec<String>,
        cache_dir: String,
    ) -> Result<Self> {
        Ok(Self {
            plugin_dirs: plugin_dirs.into_iter().map(PathBuf::from).collect(),
            config_dirs: config_dirs.into_iter().map(PathBuf::from).collect(),
            _cache_dir: PathBuf::from(cache_dir),
            network_configs: std::collections::HashMap::new(),
            default_network_name: None,
        })
    }

    /// 加载网络配置
    pub async fn load_network_configs(&mut self) -> Result<()> {
        self.network_configs.clear();
        self.default_network_name = None;

        for config_dir in &self.config_dirs {
            if !config_dir.exists() {
                debug!("CNI config directory does not exist: {:?}", config_dir);
                continue;
            }

            let mut entries = tokio::fs::read_dir(config_dir)
                .await
                .context("Failed to read CNI config directory")?;
            let mut candidate_paths = Vec::new();

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if file_name.ends_with(".conf")
                    || file_name.ends_with(".json")
                    || file_name.ends_with(".conflist")
                {
                    candidate_paths.push(path);
                }
            }

            candidate_paths.sort();

            for path in candidate_paths {
                match self.load_single_config(&path).await {
                    Ok(config) => {
                        info!("Loaded CNI config: {}", config.name);
                        if self.default_network_name.is_none() {
                            self.default_network_name = Some(config.name.clone());
                        }
                        self.network_configs.insert(config.name.clone(), config);
                    }
                    Err(e) => {
                        error!("Failed to load CNI config {:?}: {}", path, e);
                    }
                }
            }
        }

        info!("Loaded {} CNI network configs", self.network_configs.len());
        Ok(())
    }

    fn default_network_config(&self) -> Option<&CniNetworkConfig> {
        self.default_network_name
            .as_ref()
            .and_then(|name| self.network_configs.get(name))
            .or_else(|| self.network_configs.values().next())
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

        let plugin_type = Self::primary_plugin_type(&config_value);

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
        pod_cidr: Option<&str>,
    ) -> Result<NetworkStatus> {
        let config = self
            .default_network_config()
            .context("No CNI network configuration found")?;

        debug!("Using CNI network config: {}", config.name);

        let cni_args =
            self.build_cni_args(config, pod_id, netns, pod_name, pod_namespace, pod_cidr);
        let result = self
            .exec_cni_plugin(config, "ADD", pod_id, netns, "eth0", &cni_args)
            .await?;
        let network_status = self.parse_cni_result(&result)?;

        info!("Pod {} network setup completed", pod_id);
        Ok(network_status)
    }

    /// 清理Pod网络
    pub async fn teardown_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_namespace: &str,
        pod_name: &str,
    ) -> Result<()> {
        if let Some(config) = self.default_network_config() {
            let cni_args =
                self.build_cni_args(config, pod_id, netns, pod_name, pod_namespace, None);

            match self
                .exec_cni_plugin(config, "DEL", pod_id, netns, "eth0", &cni_args)
                .await
            {
                Ok(_) => info!("Pod {} network teardown completed", pod_id),
                Err(e) => error!("Failed to teardown network for pod {}: {}", pod_id, e),
            }
        }
        Ok(())
    }

    /// 构建CNI参数
    fn build_cni_args(
        &self,
        config: &CniNetworkConfig,
        pod_id: &str,
        _netns: &str,
        pod_name: &str,
        pod_namespace: &str,
        pod_cidr: Option<&str>,
    ) -> Value {
        let mut config_value = config.config.clone();
        if config_value.get("cniVersion").is_none() {
            config_value["cniVersion"] = json!(config.cni_version);
        }
        if config_value.get("name").is_none() {
            config_value["name"] = json!(config.name);
        }

        if let Some(pod_cidr) = pod_cidr {
            if !pod_cidr.trim().is_empty() {
                if config_value.get("ipam").is_none() {
                    config_value["ipam"] = json!({});
                }
                if let Some(ipam) = config_value.get_mut("ipam") {
                    ipam["subnet"] = json!(pod_cidr);
                }
            }
        }

        config_value["args"] = json!({
            "cni": {
                "podId": pod_id,
                "podName": pod_name,
                "podNamespace": pod_namespace
            }
        });
        config_value
    }

    /// 执行CNI插件
    async fn exec_cni_plugin(
        &self,
        config: &CniNetworkConfig,
        command: &str,
        pod_id: &str,
        netns: &str,
        if_name: &str,
        cni_args: &Value,
    ) -> Result<String> {
        let plugin_name = &config.plugin_type;

        let plugin_path = self
            .find_plugin(plugin_name)
            .context(format!("CNI plugin '{}' not found", plugin_name))?;

        let mut cmd = Command::new(&plugin_path);
        cmd.env("CNI_COMMAND", command)
            .env("CNI_NETNS", netns)
            .env("CNI_CONTAINERID", pod_id)
            .env("CNI_IFNAME", if_name)
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
            .find(|path| Self::path_is_executable(path))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn load_network_configs_includes_conflist_files() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(
            config_dir.join("10-test.conflist"),
            r#"{
                "cniVersion":"0.4.0",
                "name":"test-net",
                "plugins":[
                    {"type":"bridge"},
                    {"type":"portmap"}
                ]
            }"#,
        )
        .await
        .unwrap();

        let mut manager = CniManager::new(
            vec![dir.path().join("bin").display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        manager.load_network_configs().await.unwrap();

        let config = manager
            .network_configs
            .get("test-net")
            .expect("expected conflist to be loaded");
        assert_eq!(config.plugin_type, "bridge");
        assert!(config.config.get("plugins").is_some());
        assert_eq!(manager.default_network_name.as_deref(), Some("test-net"));
    }

    #[test]
    fn primary_plugin_type_prefers_type_then_first_plugin() {
        let direct = serde_json::json!({
            "type": "macvlan",
            "plugins": [{"type": "bridge"}]
        });
        assert_eq!(CniManager::primary_plugin_type(&direct), "macvlan");

        let conflist = serde_json::json!({
            "plugins": [
                {"type": "bridge"},
                {"type": "portmap"}
            ]
        });
        assert_eq!(CniManager::primary_plugin_type(&conflist), "bridge");
    }

    #[test]
    fn find_plugin_ignores_non_executable_binaries() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("bin");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        let plugin_path = plugin_dir.join("bridge");
        std::fs::write(&plugin_path, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&plugin_path, perms).unwrap();

        let manager = CniManager::new(
            vec![plugin_dir.display().to_string()],
            vec![dir.path().join("conf").display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();

        assert!(manager.find_plugin("bridge").is_none());
    }

    #[tokio::test]
    async fn load_network_configs_selects_default_network_deterministically() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(
            config_dir.join("20-second.conf"),
            r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
        )
        .await
        .unwrap();
        tokio::fs::write(
            config_dir.join("10-first.conf"),
            r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
        )
        .await
        .unwrap();

        let mut manager = CniManager::new(
            vec![dir.path().join("bin").display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        manager.load_network_configs().await.unwrap();

        assert_eq!(manager.default_network_name.as_deref(), Some("first-net"));
        assert_eq!(
            manager
                .default_network_config()
                .map(|config| config.name.as_str()),
            Some("first-net")
        );
    }
}
