use super::*;
use anyhow::{Context, Result};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CniLoadStatus {
    pub checked_at_unix_millis: i64,
    pub ready: bool,
    pub reason: String,
    pub message: String,
    pub discovered_files: Vec<String>,
    pub invalid_files: Vec<String>,
    pub loaded_networks: Vec<String>,
    pub declared_plugins: Vec<String>,
    pub missing_plugin_binaries: Vec<String>,
    pub default_network_name: Option<String>,
}

impl CniLoadStatus {
    fn new(
        ready: bool,
        reason: impl Into<String>,
        message: impl Into<String>,
        discovered_files: Vec<String>,
        invalid_files: Vec<String>,
        loaded_networks: Vec<String>,
        declared_plugins: Vec<String>,
        missing_plugin_binaries: Vec<String>,
        default_network_name: Option<String>,
    ) -> Self {
        Self {
            checked_at_unix_millis: chrono::Utc::now().timestamp_millis(),
            ready,
            reason: reason.into(),
            message: message.into(),
            discovered_files,
            invalid_files,
            loaded_networks,
            declared_plugins,
            missing_plugin_binaries,
            default_network_name,
        }
    }

    pub fn condition(&self) -> (bool, String, String) {
        (self.ready, self.reason.clone(), self.message.clone())
    }
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
    /// 配置期指定的默认网络名。
    configured_default_network_name: Option<String>,
    /// 最大允许加载的 CNI 配置文件数量；0 表示不限制。
    max_conf_num: usize,
    /// Pod 主 IP 选择策略。
    ip_pref: MainIpPreference,
    /// 默认使用的网络配置名称
    default_network_name: Option<String>,
    /// 最近一次加载结果
    last_load_status: Option<CniLoadStatus>,
}

impl CniManager {
    pub(crate) fn ordered_result_ips(value: &Value) -> Vec<std::net::IpAddr> {
        let mut ips = Vec::new();
        if let Some(entries) = value.get("ips").and_then(|ips| ips.as_array()) {
            for entry in entries {
                if let Some(ip) = entry
                    .get("address")
                    .and_then(|addr| addr.as_str())
                    .and_then(|addr| addr.split('/').next())
                    .and_then(|ip| ip.parse().ok())
                {
                    ips.push(ip);
                }
            }
        }

        if ips.is_empty() {
            if let Some(ip) = value
                .get("ip4")
                .and_then(|ip4| ip4.get("ip"))
                .and_then(|addr| addr.as_str())
                .and_then(|addr| addr.split('/').next())
                .and_then(|ip| ip.parse().ok())
            {
                ips.push(ip);
            }
            if let Some(ip) = value
                .get("ip6")
                .and_then(|ip6| ip6.get("ip"))
                .and_then(|addr| addr.as_str())
                .and_then(|addr| addr.split('/').next())
                .and_then(|ip| ip.parse().ok())
            {
                ips.push(ip);
            }
        }

        ips
    }

    #[cfg(test)]
    pub(crate) fn network_status_from_cni_result(result: Option<&Value>) -> Result<NetworkStatus> {
        Self::network_status_from_cni_result_with_preference(result, MainIpPreference::Cni)
    }

    pub(crate) fn select_pod_ips(
        ordered_ips: Vec<std::net::IpAddr>,
        preference: MainIpPreference,
    ) -> (Option<std::net::IpAddr>, Vec<std::net::IpAddr>) {
        let mut seen = std::collections::HashSet::new();
        let ordered_ips: Vec<std::net::IpAddr> = ordered_ips
            .into_iter()
            .filter(|ip| seen.insert(*ip))
            .collect();

        let is_match = |ip: &std::net::IpAddr| match preference {
            MainIpPreference::Ipv4 => ip.is_ipv4(),
            MainIpPreference::Ipv6 => ip.is_ipv6(),
            MainIpPreference::Cni => true,
        };

        if let Some((index, primary)) = ordered_ips.iter().enumerate().find(|(_, ip)| is_match(ip))
        {
            let mut additional = ordered_ips[..index].to_vec();
            additional.extend_from_slice(&ordered_ips[index + 1..]);
            return (Some(*primary), additional);
        }

        match ordered_ips.split_first() {
            Some((primary, additional)) => (Some(*primary), additional.to_vec()),
            None => (None, Vec::new()),
        }
    }

    pub(crate) fn network_status_from_cni_result_with_preference(
        result: Option<&Value>,
        preference: MainIpPreference,
    ) -> Result<NetworkStatus> {
        let Some(value) = result else {
            return Ok(NetworkStatus {
                name: "crius-net".to_string(),
                ip: None,
                mac: None,
                interfaces: vec![],
                raw_result: None,
            });
        };

        let (ip, additional_ips) =
            Self::select_pod_ips(Self::ordered_result_ips(value), preference);
        let interfaces = additional_ips
            .iter()
            .enumerate()
            .map(|(idx, ip)| NetworkInterface {
                name: format!("additional{}", idx),
                ip: Some(*ip),
                mac: None,
                netmask: None,
                gateway: None,
            })
            .collect();

        Ok(NetworkStatus {
            name: "crius-net".to_string(),
            ip,
            mac: None,
            interfaces,
            raw_result: Some(value.clone()),
        })
    }

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
            network_configs: HashMap::new(),
            configured_default_network_name: None,
            max_conf_num: 0,
            ip_pref: MainIpPreference::Cni,
            default_network_name: None,
            last_load_status: None,
        })
    }

    pub fn set_default_network_name(&mut self, default_network_name: Option<String>) {
        self.configured_default_network_name = default_network_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    }

    pub fn set_max_conf_num(&mut self, max_conf_num: usize) {
        self.max_conf_num = max_conf_num;
    }

    pub fn set_ip_pref(&mut self, ip_pref: MainIpPreference) {
        self.ip_pref = ip_pref;
    }

    pub fn last_load_status(&self) -> Option<&CniLoadStatus> {
        self.last_load_status.as_ref()
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
    pub async fn load_network_configs(&mut self) -> Result<CniLoadStatus> {
        self.network_configs.clear();
        self.default_network_name = None;
        self.last_load_status = None;
        let mut discovered_files = Vec::new();
        let mut invalid_files = Vec::new();
        let mut first_loaded_network_name = None;
        let mut remaining_budget = self.max_conf_num;

        for config_dir in &self.config_dirs {
            if self.max_conf_num != 0 && remaining_budget == 0 {
                break;
            }
            if !config_dir.exists() {
                debug!("CNI config directory does not exist: {:?}", config_dir);
                continue;
            }

            let mut entries = match tokio::fs::read_dir(config_dir).await {
                Ok(entries) => entries,
                Err(err) => {
                    let status = CniLoadStatus::new(
                        false,
                        "CNIConfigLoadFailed",
                        format!(
                            "failed to read CNI config directory {}: {}",
                            config_dir.display(),
                            err
                        ),
                        discovered_files,
                        invalid_files,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        None,
                    );
                    self.last_load_status = Some(status.clone());
                    return Err(err).context("Failed to read CNI config directory");
                }
            };
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
            if self.max_conf_num != 0 && candidate_paths.len() > remaining_budget {
                candidate_paths.truncate(remaining_budget);
            }
            let processed_paths = candidate_paths.len();

            for path in candidate_paths {
                discovered_files.push(path.display().to_string());
                match self.load_single_config(&path).await {
                    Ok(config) => {
                        info!("Loaded CNI config: {}", config.name);
                        if first_loaded_network_name.is_none() {
                            first_loaded_network_name = Some(config.name.clone());
                        }
                        self.network_configs.insert(config.name.clone(), config);
                    }
                    Err(e) => {
                        error!("Failed to load CNI config {:?}: {}", path, e);
                        invalid_files.push(path.display().to_string());
                    }
                }
            }

            if self.max_conf_num != 0 {
                remaining_budget = remaining_budget.saturating_sub(processed_paths);
            }
        }

        self.default_network_name = match self.configured_default_network_name.as_ref() {
            Some(configured_name) if self.network_configs.contains_key(configured_name) => {
                Some(configured_name.clone())
            }
            Some(_) => None,
            None => first_loaded_network_name,
        };

        let load_status = self.build_load_status(discovered_files, invalid_files);
        self.last_load_status = Some(load_status.clone());
        info!("Loaded {} CNI network configs", self.network_configs.len());
        Ok(load_status)
    }

    fn build_load_status(
        &self,
        discovered_files: Vec<String>,
        invalid_files: Vec<String>,
    ) -> CniLoadStatus {
        let loaded_networks: Vec<String> = self.network_configs.keys().cloned().collect();
        let declared_plugins: Vec<String> = self
            .network_configs
            .values()
            .flat_map(|config| Self::cni_plugin_types(&config.config))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let missing_plugin_binaries: Vec<String> = declared_plugins
            .iter()
            .filter(|plugin_type| self.find_plugin(plugin_type).is_none())
            .cloned()
            .collect();

        if let Some(configured_name) = self
            .configured_default_network_name
            .as_ref()
            .filter(|_| self.default_network_name.is_none())
        {
            return CniLoadStatus::new(
                false,
                "CNIDefaultNetworkMissing",
                format!(
                    "configured default CNI network {} was not found in configured CNI config directories",
                    configured_name
                ),
                discovered_files,
                invalid_files,
                loaded_networks,
                declared_plugins,
                missing_plugin_binaries,
                self.default_network_name.clone(),
            );
        }

        if discovered_files.is_empty() {
            CniLoadStatus::new(
                false,
                "CNIConfigMissing",
                "no CNI config file found in configured CNI config directories",
                discovered_files,
                invalid_files,
                loaded_networks,
                declared_plugins,
                missing_plugin_binaries,
                self.default_network_name.clone(),
            )
        } else if !invalid_files.is_empty() && loaded_networks.is_empty() {
            CniLoadStatus::new(
                false,
                "CNIConfigInvalid",
                format!(
                    "failed to parse {} CNI config file(s): {}",
                    invalid_files.len(),
                    invalid_files.join(", ")
                ),
                discovered_files,
                invalid_files,
                loaded_networks,
                declared_plugins,
                missing_plugin_binaries,
                self.default_network_name.clone(),
            )
        } else if !missing_plugin_binaries.is_empty() {
            CniLoadStatus::new(
                false,
                "CNIPluginMissing",
                format!(
                    "missing or non-executable CNI plugin binary/binaries: {}",
                    missing_plugin_binaries.join(", ")
                ),
                discovered_files,
                invalid_files,
                loaded_networks,
                declared_plugins,
                missing_plugin_binaries,
                self.default_network_name.clone(),
            )
        } else if !loaded_networks.is_empty() {
            CniLoadStatus::new(
                true,
                "CNINetworkConfigReady",
                format!(
                    "loaded {} CNI network config(s) and {} plugin type(s)",
                    loaded_networks.len(),
                    declared_plugins.len()
                ),
                discovered_files,
                invalid_files,
                loaded_networks,
                declared_plugins,
                missing_plugin_binaries,
                self.default_network_name.clone(),
            )
        } else {
            CniLoadStatus::new(
                true,
                "CNINetworkConfigReady",
                format!("discovered {} CNI config file(s)", discovered_files.len()),
                discovered_files,
                invalid_files,
                loaded_networks,
                declared_plugins,
                missing_plugin_binaries,
                self.default_network_name.clone(),
            )
        }
    }

    fn cni_plugin_types(config_value: &Value) -> Vec<String> {
        let mut plugin_types = Vec::new();
        if let Some(plugin_type) = config_value.get("type").and_then(|v| v.as_str()) {
            if !plugin_type.trim().is_empty() {
                plugin_types.push(plugin_type.to_string());
            }
        }

        if let Some(plugins) = config_value
            .get("plugins")
            .and_then(|plugins| plugins.as_array())
        {
            for plugin in plugins {
                if let Some(plugin_type) = plugin.get("type").and_then(|value| value.as_str()) {
                    if !plugin_type.trim().is_empty() {
                        plugin_types.push(plugin_type.to_string());
                    }
                }
            }
        }

        plugin_types
    }

    fn default_network_config(&self) -> Option<&CniNetworkConfig> {
        self.default_network_name
            .as_ref()
            .and_then(|name| self.network_configs.get(name))
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
        let config = self.default_network_config().with_context(|| {
            self.configured_default_network_name
                .as_ref()
                .map(|name| format!("configured default CNI network {} was not found", name))
                .unwrap_or_else(|| "No CNI network configuration found".to_string())
        })?;

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
        Self::network_status_from_cni_result_with_preference(result, self.ip_pref)
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
        let status = manager.load_network_configs().await.unwrap();

        let config = manager
            .network_configs
            .get("test-net")
            .expect("expected conflist to be loaded");
        assert_eq!(config.plugin_type, "bridge");
        assert!(config.config.get("plugins").is_some());
        assert_eq!(manager.default_network_name.as_deref(), Some("test-net"));
        assert_eq!(status.reason, "CNIPluginMissing");
        assert_eq!(status.loaded_networks, vec!["test-net"]);
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
        let status = manager.load_network_configs().await.unwrap();

        assert_eq!(manager.default_network_name.as_deref(), Some("first-net"));
        assert_eq!(
            manager
                .default_network_config()
                .map(|config| config.name.as_str()),
            Some("first-net")
        );
        assert_eq!(status.default_network_name.as_deref(), Some("first-net"));
    }

    #[tokio::test]
    async fn load_network_configs_honors_configured_default_network_name() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(
            config_dir.join("10-first.conf"),
            r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
        )
        .await
        .unwrap();
        tokio::fs::write(
            config_dir.join("20-second.conf"),
            r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
        )
        .await
        .unwrap();

        let mut manager = CniManager::new(
            vec![dir.path().join("bin").display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        manager.set_default_network_name(Some("second-net".to_string()));
        let status = manager.load_network_configs().await.unwrap();

        assert_eq!(manager.default_network_name.as_deref(), Some("second-net"));
        assert_eq!(
            manager
                .default_network_config()
                .map(|config| config.name.as_str()),
            Some("second-net")
        );
        assert_eq!(status.default_network_name.as_deref(), Some("second-net"));
    }

    #[tokio::test]
    async fn load_network_configs_limits_number_of_loaded_files() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(
            config_dir.join("10-first.conf"),
            r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
        )
        .await
        .unwrap();
        tokio::fs::write(
            config_dir.join("20-second.conf"),
            r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
        )
        .await
        .unwrap();

        let mut manager = CniManager::new(
            vec![dir.path().join("bin").display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        manager.set_max_conf_num(1);
        let status = manager.load_network_configs().await.unwrap();

        assert_eq!(status.discovered_files.len(), 1);
        assert_eq!(status.loaded_networks, vec!["first-net"]);
        assert!(manager.network_configs.contains_key("first-net"));
        assert!(!manager.network_configs.contains_key("second-net"));
        assert_eq!(manager.default_network_name.as_deref(), Some("first-net"));
    }

    #[tokio::test]
    async fn load_network_configs_reports_missing_configured_default_network() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
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
        manager.set_default_network_name(Some("missing-net".to_string()));
        let status = manager.load_network_configs().await.unwrap();

        assert!(!status.ready);
        assert_eq!(status.reason, "CNIDefaultNetworkMissing");
        assert!(status.message.contains("missing-net"));
        assert_eq!(status.loaded_networks, vec!["first-net"]);
        assert!(manager.default_network_config().is_none());
        assert!(manager
            .setup_pod_network(
                "pod-1",
                "/var/run/netns/test-pod",
                "test-pod",
                "default",
                None
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn load_network_configs_applies_limit_before_default_network_name_matching() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(
            config_dir.join("10-first.conf"),
            r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
        )
        .await
        .unwrap();
        tokio::fs::write(
            config_dir.join("20-second.conf"),
            r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
        )
        .await
        .unwrap();

        let mut manager = CniManager::new(
            vec![dir.path().join("bin").display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        manager.set_max_conf_num(1);
        manager.set_default_network_name(Some("second-net".to_string()));
        let status = manager.load_network_configs().await.unwrap();

        assert!(!status.ready);
        assert_eq!(status.reason, "CNIDefaultNetworkMissing");
        assert_eq!(status.discovered_files.len(), 1);
        assert_eq!(status.loaded_networks, vec!["first-net"]);
        assert!(status.message.contains("second-net"));
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
        let load_status = manager.load_network_configs().await.unwrap();
        assert!(load_status.ready);
        assert_eq!(load_status.reason, "CNINetworkConfigReady");

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

    #[test]
    fn network_status_from_cni_result_preserves_raw_result_and_ip_order() {
        let result = serde_json::json!({
            "cniVersion": "1.0.0",
            "ips": [
                {"address": "fd00::10/64"},
                {"address": "10.88.0.11/16"},
                {"address": "fd00::10/64"}
            ]
        });

        let status = CniManager::network_status_from_cni_result(Some(&result)).unwrap();

        assert_eq!(status.ip.unwrap().to_string(), "fd00::10");
        assert_eq!(status.interfaces.len(), 1);
        assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "10.88.0.11");
        assert_eq!(status.raw_result, Some(result));
    }

    #[test]
    fn network_status_from_cni_result_prefers_ipv4_when_configured() {
        let result = serde_json::json!({
            "cniVersion": "1.0.0",
            "ips": [
                {"address": "fd00::10/64"},
                {"address": "10.88.0.11/16"},
                {"address": "fd00::12/64"}
            ]
        });

        let status = CniManager::network_status_from_cni_result_with_preference(
            Some(&result),
            MainIpPreference::Ipv4,
        )
        .unwrap();

        assert_eq!(status.ip.unwrap().to_string(), "10.88.0.11");
        assert_eq!(status.interfaces.len(), 2);
        assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "fd00::10");
        assert_eq!(status.interfaces[1].ip.unwrap().to_string(), "fd00::12");
    }

    #[test]
    fn network_status_from_cni_result_prefers_ipv6_when_configured() {
        let result = serde_json::json!({
            "cniVersion": "1.0.0",
            "ips": [
                {"address": "10.88.0.11/16"},
                {"address": "fd00::10/64"},
                {"address": "10.88.0.12/16"}
            ]
        });

        let status = CniManager::network_status_from_cni_result_with_preference(
            Some(&result),
            MainIpPreference::Ipv6,
        )
        .unwrap();

        assert_eq!(status.ip.unwrap().to_string(), "fd00::10");
        assert_eq!(status.interfaces.len(), 2);
        assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "10.88.0.11");
        assert_eq!(status.interfaces[1].ip.unwrap().to_string(), "10.88.0.12");
    }

    #[test]
    fn network_status_from_cni_result_falls_back_to_cni_order_when_preferred_family_missing() {
        let result = serde_json::json!({
            "cniVersion": "1.0.0",
            "ips": [
                {"address": "fd00::10/64"},
                {"address": "fd00::12/64"}
            ]
        });

        let status = CniManager::network_status_from_cni_result_with_preference(
            Some(&result),
            MainIpPreference::Ipv4,
        )
        .unwrap();

        assert_eq!(status.ip.unwrap().to_string(), "fd00::10");
        assert_eq!(status.interfaces.len(), 1);
        assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "fd00::12");
    }

    #[tokio::test]
    async fn load_network_configs_reports_missing_plugin_binaries() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(
            config_dir.join("10-test.conflist"),
            r#"{
                "cniVersion":"1.0.0",
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
        let status = manager.load_network_configs().await.unwrap();

        assert!(!status.ready);
        assert_eq!(status.reason, "CNIPluginMissing");
        assert_eq!(status.loaded_networks, vec!["test-net"]);
        assert_eq!(status.declared_plugins, vec!["bridge", "portmap"]);
        assert_eq!(status.missing_plugin_binaries, vec!["bridge", "portmap"]);
        assert_eq!(status.default_network_name.as_deref(), Some("test-net"));
    }

    #[tokio::test]
    async fn load_network_configs_reports_invalid_configs() {
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("net.d");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::write(config_dir.join("10-bad.conf"), "{not-json")
            .await
            .unwrap();

        let mut manager = CniManager::new(
            vec![dir.path().join("bin").display().to_string()],
            vec![config_dir.display().to_string()],
            dir.path().join("cache").display().to_string(),
        )
        .unwrap();
        let status = manager.load_network_configs().await.unwrap();

        assert!(!status.ready);
        assert_eq!(status.reason, "CNIConfigInvalid");
        assert_eq!(
            status.invalid_files,
            vec![config_dir.join("10-bad.conf").display().to_string()]
        );
        assert!(status.loaded_networks.is_empty());
    }
}
