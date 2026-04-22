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
            cache_dir: PathBuf::from(cache_dir),
            network_configs: std::collections::HashMap::new(),
            default_network_name: None,
        })
    }

    fn plugin_chain(config_value: &Value) -> Vec<Value> {
        if let Some(plugins) = config_value
            .get("plugins")
            .and_then(|plugins| plugins.as_array())
        {
            return plugins.clone();
        }

        vec![config_value.clone()]
    }

    fn cache_result_path(&self, pod_id: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.result.json", pod_id))
    }

    async fn write_cached_result(&self, pod_id: &str, result: &Value) -> Result<()> {
        tokio::fs::create_dir_all(&self.cache_dir)
            .await
            .context("Failed to create CNI cache directory")?;
        tokio::fs::write(
            self.cache_result_path(pod_id),
            serde_json::to_vec_pretty(result)?,
        )
        .await
        .context("Failed to write CNI cached result")?;
        Ok(())
    }

    async fn read_cached_result(&self, pod_id: &str) -> Option<Value> {
        let path = self.cache_result_path(pod_id);
        let raw = tokio::fs::read(path).await.ok()?;
        serde_json::from_slice(&raw).ok()
    }

    async fn remove_cached_result(&self, pod_id: &str) {
        let _ = tokio::fs::remove_file(self.cache_result_path(pod_id)).await;
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

        let result = self
            .exec_cni_chain(
                config,
                "ADD",
                pod_id,
                netns,
                "eth0",
                pod_name,
                pod_namespace,
                pod_cidr,
            )
            .await?;
        let network_status = self.parse_cni_result(result.as_ref())?;

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
            match self
                .exec_cni_chain(
                    config,
                    "DEL",
                    pod_id,
                    netns,
                    "eth0",
                    pod_name,
                    pod_namespace,
                    None,
                )
                .await
            {
                Ok(_) => info!("Pod {} network teardown completed", pod_id),
                Err(e) => error!("Failed to teardown network for pod {}: {}", pod_id, e),
            }
        }
        Ok(())
    }

    /// 构建CNI参数
    fn base_cni_args(&self, pod_id: &str, pod_name: &str, pod_namespace: &str) -> Value {
        json!({
            "cni": {
                "podId": pod_id,
                "podName": pod_name,
                "podNamespace": pod_namespace
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_plugin_config(
        &self,
        config: &CniNetworkConfig,
        plugin: &Value,
        pod_id: &str,
        pod_name: &str,
        pod_namespace: &str,
        pod_cidr: Option<&str>,
        prev_result: Option<&Value>,
    ) -> Value {
        let mut config_value = plugin.clone();
        if !config_value.is_object() {
            config_value = json!({});
        }

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

        config_value["args"] = self.base_cni_args(pod_id, pod_name, pod_namespace);

        if let Some(prev_result) = prev_result {
            config_value["prevResult"] = prev_result.clone();
        }

        config_value
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_cni_chain(
        &self,
        config: &CniNetworkConfig,
        command: &str,
        pod_id: &str,
        netns: &str,
        if_name: &str,
        pod_name: &str,
        pod_namespace: &str,
        pod_cidr: Option<&str>,
    ) -> Result<Option<Value>> {
        let plugins = Self::plugin_chain(&config.config);
        let cached_result = self.read_cached_result(pod_id).await;

        let plugin_sequence: Vec<&Value> = if command == "DEL" {
            plugins.iter().rev().collect()
        } else {
            plugins.iter().collect()
        };

        let mut prev_result = if command == "DEL" {
            cached_result.clone()
        } else {
            None
        };

        for plugin in plugin_sequence {
            let plugin_type = plugin
                .get("type")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&config.plugin_type);
            let plugin_config = self.build_plugin_config(
                config,
                plugin,
                pod_id,
                pod_name,
                pod_namespace,
                pod_cidr,
                prev_result.as_ref(),
            );
            let output = self
                .exec_cni_plugin(plugin_type, command, pod_id, netns, if_name, &plugin_config)
                .await?;

            if command == "ADD" && !output.trim().is_empty() {
                prev_result = Some(
                    serde_json::from_str(&output)
                        .context("Failed to parse chained CNI plugin result")?,
                );
            }
        }

        if command == "ADD" {
            if let Some(result) = prev_result.as_ref() {
                self.write_cached_result(pod_id, result).await?;
            }
        } else {
            self.remove_cached_result(pod_id).await;
        }

        Ok(prev_result.or(cached_result))
    }

    /// 执行CNI插件
    async fn exec_cni_plugin(
        &self,
        plugin_name: &str,
        command: &str,
        pod_id: &str,
        netns: &str,
        if_name: &str,
        cni_args: &Value,
    ) -> Result<String> {
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
            let stdout = String::from_utf8_lossy(&output.stdout);
            let detail = if !stderr.trim().is_empty() {
                stderr.trim().to_string()
            } else if !stdout.trim().is_empty() {
                stdout.trim().to_string()
            } else {
                format!("exited with status {}", output.status)
            };
            return Err(anyhow::anyhow!(
                "CNI plugin {} failed: {}",
                plugin_name,
                detail
            ));
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
    fn parse_cni_result(&self, result: Option<&Value>) -> Result<NetworkStatus> {
        let Some(value) = result else {
            return Ok(NetworkStatus {
                name: "crius-net".to_string(),
                ip: None,
                mac: None,
                interfaces: vec![],
            });
        };

        let ip = value
            .get("ips")
            .and_then(|ips| ips.as_array())
            .and_then(|arr| arr.first())
            .and_then(|ip| ip.get("address"))
            .and_then(|addr| addr.as_str())
            .and_then(|s| s.split('/').next())
            .and_then(|ip_str| ip_str.parse().ok())
            .or_else(|| {
                value
                    .get("ip4")
                    .and_then(|ip4| ip4.get("ip"))
                    .and_then(|addr| addr.as_str())
                    .and_then(|s| s.split('/').next())
                    .and_then(|ip_str| ip_str.parse().ok())
            });

        Ok(NetworkStatus {
            name: "crius-net".to_string(),
            ip,
            mac: None,
            interfaces: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
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

    #[tokio::test]
    async fn setup_pod_network_executes_conflist_as_plugin_chain() {
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("bin");
        let config_dir = dir.path().join("net.d");
        let record_dir = dir.path().join("records");
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::create_dir_all(&record_dir).await.unwrap();

        tokio::fs::write(
            config_dir.join("10-test.conflist"),
            r#"{
                "cniVersion":"1.0.0",
                "name":"test-net",
                "plugins":[
                    {"type":"bridge","bridge":"cni0","ipam":{"type":"host-local"}},
                    {"type":"portmap","capabilities":{"portMappings":true}}
                ]
            }"#,
        )
        .await
        .unwrap();

        let bridge_script = format!(
            "#!/bin/sh\ncat > \"{}/bridge.input\"\nprintf '%s\\n' '{{\"cniVersion\":\"1.0.0\",\"ips\":[{{\"address\":\"10.88.0.2/16\"}}]}}'\n",
            record_dir.display()
        );
        let bridge_path = plugin_dir.join("bridge");
        tokio::fs::write(&bridge_path, bridge_script).await.unwrap();
        let mut bridge_perms = std::fs::metadata(&bridge_path).unwrap().permissions();
        bridge_perms.set_mode(0o755);
        std::fs::set_permissions(&bridge_path, bridge_perms).unwrap();

        let portmap_script = format!(
            "#!/bin/sh\ncat > \"{}/portmap.input\"\nprintf '%s\\n' '{{\"cniVersion\":\"1.0.0\",\"ips\":[{{\"address\":\"10.88.0.2/16\"}}]}}'\n",
            record_dir.display()
        );
        let portmap_path = plugin_dir.join("portmap");
        tokio::fs::write(&portmap_path, portmap_script)
            .await
            .unwrap();
        let mut portmap_perms = std::fs::metadata(&portmap_path).unwrap().permissions();
        portmap_perms.set_mode(0o755);
        std::fs::set_permissions(&portmap_path, portmap_perms).unwrap();

        let mut manager = CniManager::new(
            vec![plugin_dir.display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        manager.load_network_configs().await.unwrap();

        let status = manager
            .setup_pod_network(
                "pod-1",
                "/var/run/netns/test-pod",
                "test-pod",
                "default",
                None,
            )
            .await
            .unwrap();

        assert_eq!(status.ip, Some("10.88.0.2".parse().unwrap()));

        let bridge_input = tokio::fs::read_to_string(record_dir.join("bridge.input"))
            .await
            .unwrap();
        assert!(bridge_input.contains("\"type\":\"bridge\""));
        assert!(!bridge_input.contains("\"plugins\""));

        let portmap_input = tokio::fs::read_to_string(record_dir.join("portmap.input"))
            .await
            .unwrap();
        assert!(portmap_input.contains("\"type\":\"portmap\""));
        assert!(portmap_input.contains("\"prevResult\""));
    }

    #[test]
    fn parse_cni_result_returns_none_when_output_missing() {
        let manager = CniManager::new(vec![], vec![], "/tmp/cache".to_string()).unwrap();
        let status = manager.parse_cni_result(None).unwrap();
        assert!(status.ip.is_none());
    }
}
