//! 网络模块
//!
//! 提供容器网络功能，包括网络命名空间管理、CNI 接口等。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use async_trait::async_trait;
use nix::mount::{mount, umount2, MntFlags, MsFlags};
use nix::sched::{setns, unshare, CloneFlags};
use nix::unistd::gettid;

pub mod cni;
mod error;
pub mod multi;
mod port_mapping;
mod types;

pub use cni::{CniLoadStatus, CniManager};
pub use error::NetworkError;
pub use multi::{
    MultiNetworkConfig, MultiNetworkManager, NetworkInterfaceStatus, NetworkSelector,
    PodNetworkStatus,
};
pub use port_mapping::{PortMapping, PortMappingBackend, PortMappingManager, Protocol};
pub use types::*;

/// 共享的 CNI 路径配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CniConfig {
    config_dirs: Vec<PathBuf>,
    plugin_dirs: Vec<PathBuf>,
    cache_dir: PathBuf,
    conf_template: Option<PathBuf>,
    max_conf_num: usize,
    ip_pref: MainIpPreference,
    teardown_timeout: std::time::Duration,
    runtime_handler_config_dirs: std::collections::HashMap<String, Vec<PathBuf>>,
    runtime_handler_max_conf_nums: std::collections::HashMap<String, usize>,
    default_network_name: Option<String>,
    disable_hostport_mapping: bool,
    netns_mount_dir: PathBuf,
    netns_mounts_under_state_dir: bool,
    namespace_helper_path: Option<PathBuf>,
    rootless: Option<RootlessNetworkConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootlessNetworkConfig {
    enabled: bool,
    mode: crate::rootless::NetworkMode,
    slirp4netns_path: PathBuf,
    pasta_path: PathBuf,
}

impl Default for CniConfig {
    fn default() -> Self {
        Self {
            config_dirs: vec![
                PathBuf::from("/etc/cni/net.d"),
                PathBuf::from("/etc/kubernetes/cni/net.d"),
            ],
            plugin_dirs: vec![
                PathBuf::from("/opt/cni/bin"),
                PathBuf::from("/usr/lib/cni"),
                PathBuf::from("/usr/libexec/cni"),
            ],
            cache_dir: PathBuf::from("/var/lib/cni/cache"),
            conf_template: None,
            max_conf_num: 0,
            ip_pref: MainIpPreference::Cni,
            teardown_timeout: std::time::Duration::from_secs(60),
            runtime_handler_config_dirs: std::collections::HashMap::new(),
            runtime_handler_max_conf_nums: std::collections::HashMap::new(),
            default_network_name: None,
            disable_hostport_mapping: false,
            netns_mount_dir: PathBuf::from("/var/run/netns"),
            netns_mounts_under_state_dir: false,
            namespace_helper_path: None,
            rootless: None,
        }
    }
}

impl CniConfig {
    pub fn new(
        config_dirs: Vec<PathBuf>,
        plugin_dirs: Vec<PathBuf>,
        cache_dir: PathBuf,
        max_conf_num: usize,
        ip_pref: MainIpPreference,
        default_network_name: Option<String>,
        disable_hostport_mapping: bool,
    ) -> Self {
        Self {
            config_dirs,
            plugin_dirs,
            cache_dir,
            conf_template: None,
            max_conf_num,
            ip_pref,
            teardown_timeout: std::time::Duration::from_secs(60),
            runtime_handler_config_dirs: std::collections::HashMap::new(),
            runtime_handler_max_conf_nums: std::collections::HashMap::new(),
            default_network_name: default_network_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            disable_hostport_mapping,
            netns_mount_dir: PathBuf::from("/var/run/netns"),
            netns_mounts_under_state_dir: false,
            namespace_helper_path: None,
            rootless: None,
        }
    }

    fn parse_dirs(raw: &str) -> Vec<PathBuf> {
        raw.split(':')
            .map(str::trim)
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from)
            .collect()
    }

    /// 从环境变量解析 CNI 目录配置。
    pub fn from_env() -> Self {
        let defaults = Self::default();

        let config_dirs = std::env::var("CRIUS_CNI_CONFIG_DIRS")
            .ok()
            .map(|raw| Self::parse_dirs(&raw))
            .filter(|dirs| !dirs.is_empty())
            .unwrap_or_else(|| defaults.config_dirs.clone());

        let plugin_dirs = std::env::var("CRIUS_CNI_PLUGIN_DIRS")
            .ok()
            .or_else(|| std::env::var("CNI_PATH").ok())
            .map(|raw| Self::parse_dirs(&raw))
            .filter(|dirs| !dirs.is_empty())
            .unwrap_or_else(|| defaults.plugin_dirs.clone());

        let cache_dir = std::env::var("CRIUS_CNI_CACHE_DIR")
            .ok()
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| defaults.cache_dir.clone());

        Self {
            config_dirs,
            plugin_dirs,
            cache_dir,
            conf_template: std::env::var("CRIUS_CNI_CONF_TEMPLATE")
                .ok()
                .map(PathBuf::from)
                .filter(|path| !path.as_os_str().is_empty()),
            max_conf_num: std::env::var("CRIUS_CNI_MAX_CONF_NUM")
                .ok()
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(defaults.max_conf_num),
            ip_pref: std::env::var("CRIUS_CNI_IP_PREF")
                .ok()
                .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
                    "ipv4" => Some(MainIpPreference::Ipv4),
                    "ipv6" => Some(MainIpPreference::Ipv6),
                    "cni" => Some(MainIpPreference::Cni),
                    _ => None,
                })
                .unwrap_or(defaults.ip_pref),
            teardown_timeout: std::env::var("CRIUS_CNI_TEARDOWN_TIMEOUT")
                .ok()
                .and_then(|value| crate::streaming::parse_duration(&value).ok())
                .unwrap_or(defaults.teardown_timeout),
            runtime_handler_config_dirs: std::collections::HashMap::new(),
            runtime_handler_max_conf_nums: std::collections::HashMap::new(),
            default_network_name: std::env::var("CRIUS_CNI_DEFAULT_NETWORK")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            disable_hostport_mapping: std::env::var("CRIUS_DISABLE_HOSTPORT_MAPPING")
                .ok()
                .map(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false),
            netns_mount_dir: defaults.netns_mount_dir.clone(),
            netns_mounts_under_state_dir: std::env::var("CRIUS_NETNS_MOUNTS_UNDER_STATE_DIR")
                .ok()
                .map(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(defaults.netns_mounts_under_state_dir),
            namespace_helper_path: None,
            rootless: None,
        }
    }

    pub fn config_dirs(&self) -> &[PathBuf] {
        &self.config_dirs
    }

    pub fn plugin_dirs(&self) -> &[PathBuf] {
        &self.plugin_dirs
    }

    pub fn set_config_dirs(&mut self, config_dirs: Vec<PathBuf>) {
        self.config_dirs = config_dirs;
    }

    pub fn set_plugin_dirs(&mut self, plugin_dirs: Vec<PathBuf>) {
        self.plugin_dirs = plugin_dirs;
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    pub fn conf_template(&self) -> Option<&Path> {
        self.conf_template.as_deref()
    }

    pub fn max_conf_num(&self) -> usize {
        self.max_conf_num
    }

    pub fn set_max_conf_num(&mut self, max_conf_num: usize) {
        self.max_conf_num = max_conf_num;
    }

    pub fn ip_pref(&self) -> MainIpPreference {
        self.ip_pref
    }

    pub fn teardown_timeout(&self) -> std::time::Duration {
        self.teardown_timeout
    }

    pub fn set_handler_config_dirs(
        &mut self,
        runtime_handler: impl Into<String>,
        config_dirs: Vec<PathBuf>,
    ) {
        self.runtime_handler_config_dirs
            .insert(runtime_handler.into(), config_dirs);
    }

    pub fn handler_config_dirs(&self, runtime_handler: &str) -> Option<&[PathBuf]> {
        self.runtime_handler_config_dirs
            .get(runtime_handler)
            .map(Vec::as_slice)
    }

    pub fn set_conf_template(&mut self, conf_template: Option<PathBuf>) {
        self.conf_template = conf_template.filter(|path| !path.as_os_str().is_empty());
    }

    pub fn set_teardown_timeout(&mut self, teardown_timeout: std::time::Duration) {
        self.teardown_timeout = teardown_timeout;
    }

    pub fn set_handler_max_conf_num(
        &mut self,
        runtime_handler: impl Into<String>,
        max_conf_num: usize,
    ) {
        self.runtime_handler_max_conf_nums
            .insert(runtime_handler.into(), max_conf_num);
    }

    pub fn handler_max_conf_num(&self, runtime_handler: &str) -> Option<usize> {
        self.runtime_handler_max_conf_nums
            .get(runtime_handler)
            .copied()
    }

    pub fn default_network_name(&self) -> Option<&str> {
        self.default_network_name.as_deref()
    }

    pub fn set_default_network_name(&mut self, default_network_name: Option<String>) {
        self.default_network_name = default_network_name;
    }

    pub fn disable_hostport_mapping(&self) -> bool {
        self.disable_hostport_mapping
    }

    pub fn netns_mount_dir(&self) -> &Path {
        &self.netns_mount_dir
    }

    pub fn set_netns_mount_dir(&mut self, mount_dir: PathBuf) {
        self.netns_mount_dir = mount_dir;
    }

    pub fn netns_mounts_under_state_dir(&self) -> bool {
        self.netns_mounts_under_state_dir
    }

    pub fn set_netns_mounts_under_state_dir(&mut self, enabled: bool) {
        self.netns_mounts_under_state_dir = enabled;
    }

    pub fn namespace_helper_path(&self) -> Option<&Path> {
        self.namespace_helper_path.as_deref()
    }

    pub fn set_namespace_helper_path(&mut self, helper_path: Option<PathBuf>) {
        self.namespace_helper_path = helper_path.filter(|path| !path.as_os_str().is_empty());
    }

    pub fn set_rootless_config(
        &mut self,
        rootless: Option<crate::rootless::EffectiveRootlessConfig>,
    ) {
        self.rootless = rootless.and_then(|effective| {
            effective.enabled.then(|| RootlessNetworkConfig {
                enabled: true,
                mode: effective.network_mode,
                slirp4netns_path: effective.slirp4netns_path,
                pasta_path: effective.pasta_path,
            })
        });
    }

    pub fn rootless_config(&self) -> Option<&RootlessNetworkConfig> {
        self.rootless.as_ref()
    }

    pub fn netns_path(&self, ns_name_or_path: &str) -> PathBuf {
        NamespaceManager::new(self.netns_mount_dir.clone()).resolve_path(ns_name_or_path)
    }

    pub fn namespace_manager(&self) -> NamespaceManager {
        NamespaceManager::with_helper(
            self.netns_mount_dir.clone(),
            self.namespace_helper_path.clone(),
        )
    }

    fn config_dir_strings(&self) -> Vec<String> {
        self.config_dirs
            .iter()
            .map(|dir| dir.to_string_lossy().to_string())
            .collect()
    }

    fn plugin_dir_strings(&self) -> Vec<String> {
        self.plugin_dirs
            .iter()
            .map(|dir| dir.to_string_lossy().to_string())
            .collect()
    }

    fn cache_dir_string(&self) -> String {
        self.cache_dir.to_string_lossy().to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceManager {
    mount_dir: PathBuf,
    helper_path: Option<PathBuf>,
}

impl NamespaceManager {
    pub fn new(mount_dir: PathBuf) -> Self {
        Self::with_helper(mount_dir, None::<PathBuf>)
    }

    pub fn with_helper(mount_dir: PathBuf, helper_path: Option<impl Into<PathBuf>>) -> Self {
        Self {
            mount_dir,
            helper_path: helper_path.map(Into::into),
        }
    }

    pub fn mount_dir(&self) -> &Path {
        &self.mount_dir
    }

    pub fn helper_path(&self) -> Option<&Path> {
        self.helper_path.as_deref()
    }

    pub fn resolve_path(&self, ns_name_or_path: &str) -> PathBuf {
        let candidate = Path::new(ns_name_or_path);
        if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            self.mount_dir.join(candidate)
        }
    }

    pub fn exists(&self, ns_name_or_path: &str) -> bool {
        self.resolve_path(ns_name_or_path).exists()
    }

    pub async fn create(&self, ns_name_or_path: &str) -> Result<PathBuf, NetworkError> {
        let manager = self.clone();
        let input = ns_name_or_path.to_string();
        tokio::task::spawn_blocking(move || manager.create_blocking(&input))
            .await
            .map_err(|err| {
                NetworkError::Other(format!("create network namespace task failed: {err}"))
            })?
    }

    fn create_blocking(&self, ns_name_or_path: &str) -> Result<PathBuf, NetworkError> {
        let path = self.resolve_path(ns_name_or_path);
        if path.exists() {
            return Ok(path);
        }

        if self.helper_path.is_some() && path.parent() == Some(self.mount_dir.as_path()) {
            self.create_with_helper(&path)?;
            return Ok(path);
        }

        self.create_internal(&path)?;
        Ok(path)
    }

    fn create_with_helper(&self, path: &Path) -> Result<(), NetworkError> {
        let helper = self.helper_path.as_ref().ok_or_else(|| {
            NetworkError::Other("namespace helper path is not configured".to_string())
        })?;
        let ns_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                NetworkError::Other(format!(
                    "failed to derive namespace name for helper-managed path {}",
                    path.display()
                ))
            })?;
        let helper_base_dir = self.mount_dir.parent().ok_or_else(|| {
            NetworkError::Other(format!(
                "failed to derive helper base directory from mount dir {}",
                self.mount_dir.display()
            ))
        })?;
        std::fs::create_dir_all(helper_base_dir)?;
        std::fs::create_dir_all(&self.mount_dir)?;

        let output = StdCommand::new(helper)
            .arg("-d")
            .arg(helper_base_dir)
            .arg("-f")
            .arg(ns_name)
            .arg("--net")
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let detail = if !stderr.is_empty() {
                stderr
            } else if !stdout.is_empty() {
                stdout
            } else {
                format!("status={}", output.status)
            };
            return Err(NetworkError::Other(format!(
                "namespace helper {} failed for {}: {}",
                helper.display(),
                path.display(),
                detail
            )));
        }

        if !path.exists() {
            return Err(NetworkError::Other(format!(
                "namespace helper {} completed without creating {}",
                helper.display(),
                path.display()
            )));
        }

        Ok(())
    }

    fn create_internal(&self, path: &Path) -> Result<(), NetworkError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _mountpoint = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;

        let mount_path = path.to_path_buf();
        std::thread::spawn(move || -> Result<(), NetworkError> {
            unshare(CloneFlags::CLONE_NEWNET).map_err(|err| {
                NetworkError::Other(format!("failed to unshare network namespace: {err}"))
            })?;
            mount(
                Some(current_thread_netns_path().as_str()),
                mount_path.as_path(),
                None::<&str>,
                MsFlags::MS_BIND,
                None::<&str>,
            )
            .map_err(|err| {
                NetworkError::Other(format!(
                    "failed to bind-mount network namespace at {}: {err}",
                    mount_path.display()
                ))
            })?;
            Ok(())
        })
        .join()
        .map_err(|_| {
            NetworkError::Other("network namespace creator thread panicked".to_string())
        })??;

        Ok(())
    }

    pub async fn remove(&self, ns_name_or_path: &str) -> Result<(), NetworkError> {
        let manager = self.clone();
        let input = ns_name_or_path.to_string();
        tokio::task::spawn_blocking(move || manager.remove_blocking(&input))
            .await
            .map_err(|err| {
                NetworkError::Other(format!("remove network namespace task failed: {err}"))
            })?
    }

    fn remove_blocking(&self, ns_name_or_path: &str) -> Result<(), NetworkError> {
        let path = self.resolve_path(ns_name_or_path);
        if !path.exists() {
            return Ok(());
        }

        match umount2(path.as_path(), MntFlags::MNT_DETACH) {
            Ok(()) => {}
            Err(nix::errno::Errno::EINVAL | nix::errno::Errno::ENOENT) => {}
            Err(err) => {
                return Err(NetworkError::Other(format!(
                    "failed to unmount network namespace {}: {err}",
                    path.display()
                )));
            }
        }

        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(NetworkError::Io(err)),
        }
    }
}

fn current_thread_netns_path() -> String {
    format!("/proc/{}/task/{}/ns/net", std::process::id(), gettid())
}

/// 网络管理器接口
#[async_trait]
pub trait NetworkManager: Send + Sync + 'static {
    /// 初始化网络
    async fn init(&self) -> Result<(), NetworkError>;

    /// 创建网络命名空间
    async fn create_network_namespace(&self, ns_path: &str) -> Result<(), NetworkError>;

    /// 删除网络命名空间
    async fn remove_network_namespace(&self, ns_path: &str) -> Result<(), NetworkError>;

    /// 设置 Pod 网络
    async fn setup_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
        pod_uid: &str,
        runtime_handler: &str,
        pod_cidr: Option<&str>,
    ) -> Result<NetworkStatus, NetworkError>;

    /// 清理 Pod 网络
    async fn teardown_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        runtime_handler: &str,
    ) -> Result<(), NetworkError>;
}

/// 默认网络管理器实现
#[derive(Debug)]
pub struct DefaultNetworkManager {
    cni_plugin_dirs: Vec<String>,
    cni_config_dirs: Vec<String>,
    cni_cache_dir: String,
    cni_max_conf_num: usize,
    cni_ip_pref: MainIpPreference,
    cni_teardown_timeout: std::time::Duration,
    cni_runtime_handler_config_dirs: std::collections::HashMap<String, Vec<String>>,
    cni_runtime_handler_max_conf_nums: std::collections::HashMap<String, usize>,
    cni_default_network_name: Option<String>,
    namespace_manager: NamespaceManager,
    rootless: Option<RootlessNetworkConfig>,
    rootless_processes: std::sync::Arc<std::sync::Mutex<HashMap<String, u32>>>,
}

impl DefaultNetworkManager {
    async fn ensure_loopback_up(&self, netns: &str) -> Result<(), NetworkError> {
        let netns_path = self.namespace_manager.resolve_path(netns);
        tokio::task::spawn_blocking(move || {
            use std::os::fd::AsRawFd;
            use std::os::unix::process::CommandExt;

            let netns_file = std::fs::File::open(&netns_path)?;
            let netns_fd = netns_file.as_raw_fd();
            let mut command = StdCommand::new("ip");
            command.args(["link", "set", "lo", "up"]);

            unsafe {
                command.pre_exec(move || {
                    setns(netns_fd, CloneFlags::CLONE_NEWNET).map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("failed to enter network namespace: {err}"),
                        )
                    })?;
                    Ok(())
                });
            }

            let status = command.status()?;
            if !status.success() {
                return Err(NetworkError::CommandExecutionError {
                    command: format!("ip link set lo up (netns {})", netns_path.display()),
                    status,
                });
            }

            Ok(())
        })
        .await
        .map_err(|err| NetworkError::Other(format!("loopback setup task failed: {err}")))?
    }

    /// 创建新的网络管理器实例
    pub fn new(
        cni_plugin_dirs: Option<Vec<String>>,
        cni_config_dirs: Option<Vec<String>>,
        cni_cache_dir: Option<String>,
    ) -> Self {
        let mut cni = CniConfig::default();
        if let Some(plugin_dirs) = cni_plugin_dirs {
            cni.plugin_dirs = plugin_dirs.into_iter().map(PathBuf::from).collect();
        }
        if let Some(config_dirs) = cni_config_dirs {
            cni.config_dirs = config_dirs.into_iter().map(PathBuf::from).collect();
        }
        if let Some(cache_dir) = cni_cache_dir {
            cni.cache_dir = PathBuf::from(cache_dir);
        }

        Self::from_cni_config(cni)
    }

    pub fn from_cni_config(cni: CniConfig) -> Self {
        Self {
            cni_plugin_dirs: cni.plugin_dir_strings(),
            cni_config_dirs: cni.config_dir_strings(),
            cni_cache_dir: cni.cache_dir_string(),
            cni_max_conf_num: cni.max_conf_num(),
            cni_ip_pref: cni.ip_pref(),
            cni_teardown_timeout: cni.teardown_timeout(),
            cni_runtime_handler_config_dirs: cni
                .runtime_handler_config_dirs
                .iter()
                .map(|(handler, dirs)| {
                    (
                        handler.clone(),
                        dirs.iter()
                            .map(|dir| dir.to_string_lossy().to_string())
                            .collect(),
                    )
                })
                .collect(),
            cni_runtime_handler_max_conf_nums: cni.runtime_handler_max_conf_nums.clone(),
            cni_default_network_name: cni.default_network_name().map(ToOwned::to_owned),
            namespace_manager: NamespaceManager::new(cni.netns_mount_dir().to_path_buf()),
            rootless: cni.rootless.clone(),
            rootless_processes: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn effective_cni_config_dirs(&self, runtime_handler: &str) -> Vec<String> {
        self.cni_runtime_handler_config_dirs
            .get(runtime_handler)
            .filter(|dirs| !dirs.is_empty())
            .cloned()
            .unwrap_or_else(|| self.cni_config_dirs.clone())
    }

    fn effective_cni_max_conf_num(&self, runtime_handler: &str) -> usize {
        self.cni_runtime_handler_max_conf_nums
            .get(runtime_handler)
            .copied()
            .unwrap_or(self.cni_max_conf_num)
    }

    fn rootless_network_status(&self, name: &str) -> NetworkStatus {
        NetworkStatus {
            name: name.to_string(),
            ip: None,
            mac: None,
            interfaces: Vec::new(),
            raw_result: None,
        }
    }

    fn start_rootless_network_helper(
        &self,
        pod_id: &str,
        netns: &str,
    ) -> Result<NetworkStatus, NetworkError> {
        let Some(rootless) = self.rootless.as_ref() else {
            return Err(NetworkError::Other(
                "rootless network config is not enabled".to_string(),
            ));
        };
        let (program, name) = match rootless.mode {
            crate::rootless::NetworkMode::Slirp4netns => {
                (&rootless.slirp4netns_path, "slirp4netns")
            }
            crate::rootless::NetworkMode::Pasta => (&rootless.pasta_path, "pasta"),
            crate::rootless::NetworkMode::None => return Ok(self.rootless_network_status("none")),
            crate::rootless::NetworkMode::Rootlesskit => {
                return Err(NetworkError::Other(
                    "rootlesskit network mode is not implemented".to_string(),
                ))
            }
        };
        let mut command = StdCommand::new(program);
        match rootless.mode {
            crate::rootless::NetworkMode::Slirp4netns => {
                command.args([
                    "--netns-type=path",
                    "--disable-host-loopback",
                    netns,
                    "tap0",
                ]);
            }
            crate::rootless::NetworkMode::Pasta => {
                command.args(["--netns", netns, "--config-net", "--quiet"]);
            }
            crate::rootless::NetworkMode::None | crate::rootless::NetworkMode::Rootlesskit => {}
        }
        command.stdout(std::process::Stdio::null());
        command.stderr(std::process::Stdio::null());
        let child = command.spawn().map_err(|err| {
            NetworkError::Other(format!(
                "failed to start rootless network helper {} for {}: {}",
                program.display(),
                pod_id,
                err
            ))
        })?;
        if let Ok(mut processes) = self.rootless_processes.lock() {
            processes.insert(pod_id.to_string(), child.id());
        }
        Ok(self.rootless_network_status(name))
    }

    fn stop_rootless_network_helper(&self, pod_id: &str) {
        let pid = self
            .rootless_processes
            .lock()
            .ok()
            .and_then(|mut processes| processes.remove(pod_id));
        if let Some(pid) = pid {
            #[cfg(unix)]
            {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGTERM,
                );
            }
        }
    }
}

#[async_trait]
impl NetworkManager for DefaultNetworkManager {
    async fn init(&self) -> Result<(), NetworkError> {
        // 创建缓存目录
        if self.rootless.is_none() && !Path::new(&self.cni_cache_dir).exists() {
            tokio::fs::create_dir_all(&self.cni_cache_dir).await?;
        }
        tokio::fs::create_dir_all(self.namespace_manager.mount_dir()).await?;
        Ok(())
    }

    async fn create_network_namespace(&self, ns_path: &str) -> Result<(), NetworkError> {
        self.namespace_manager.create(ns_path).await?;
        Ok(())
    }

    async fn remove_network_namespace(&self, ns_path: &str) -> Result<(), NetworkError> {
        self.namespace_manager.remove(ns_path).await?;
        Ok(())
    }

    async fn setup_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
        pod_uid: &str,
        runtime_handler: &str,
        pod_cidr: Option<&str>,
    ) -> Result<NetworkStatus, NetworkError> {
        self.ensure_loopback_up(netns).await?;

        if self.rootless.is_some() {
            return self.start_rootless_network_helper(pod_id, netns);
        }

        let mut cni = CniManager::new(
            self.cni_plugin_dirs.clone(),
            self.effective_cni_config_dirs(runtime_handler),
            self.cni_cache_dir.clone(),
        )
        .map_err(|e| NetworkError::Other(e.to_string()))?;
        cni.set_max_conf_num(self.effective_cni_max_conf_num(runtime_handler));
        cni.set_ip_pref(self.cni_ip_pref);
        cni.set_default_network_name(self.cni_default_network_name.clone());
        cni.set_teardown_timeout(self.cni_teardown_timeout);
        cni.load_network_configs()
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))?;

        cni.setup_pod_network(pod_id, netns, pod_name, pod_namespace, pod_uid, pod_cidr)
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))
    }

    async fn teardown_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        runtime_handler: &str,
    ) -> Result<(), NetworkError> {
        if self.rootless.is_some() {
            self.stop_rootless_network_helper(pod_id);
            return Ok(());
        }
        let mut cni = CniManager::new(
            self.cni_plugin_dirs.clone(),
            self.effective_cni_config_dirs(runtime_handler),
            self.cni_cache_dir.clone(),
        )
        .map_err(|e| NetworkError::Other(e.to_string()))?;
        cni.set_max_conf_num(self.effective_cni_max_conf_num(runtime_handler));
        cni.set_ip_pref(self.cni_ip_pref);
        cni.set_default_network_name(self.cni_default_network_name.clone());
        cni.set_teardown_timeout(self.cni_teardown_timeout);
        cni.load_network_configs()
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))?;
        cni.teardown_pod_network(pod_id, netns, pod_namespace, pod_name, pod_uid)
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn default_network_manager_prefers_runtime_specific_config_dirs() {
        let mut cni = CniConfig::default();
        cni.set_handler_config_dirs("kata", vec![PathBuf::from("/etc/cni/kata.d")]);
        let manager = DefaultNetworkManager::from_cni_config(cni);

        assert_eq!(
            manager.effective_cni_config_dirs("kata"),
            vec!["/etc/cni/kata.d".to_string()]
        );
        assert_eq!(
            manager.effective_cni_config_dirs("runc"),
            vec![
                "/etc/cni/net.d".to_string(),
                "/etc/kubernetes/cni/net.d".to_string()
            ]
        );
    }

    #[test]
    fn default_network_manager_prefers_runtime_specific_max_conf_num() {
        let mut cni = CniConfig::default();
        cni.set_handler_max_conf_num("kata", 2);
        let manager = DefaultNetworkManager::from_cni_config(cni);

        assert_eq!(manager.effective_cni_max_conf_num("kata"), 2);
        assert_eq!(manager.effective_cni_max_conf_num("runc"), 0);
    }

    #[test]
    fn default_network_manager_supports_rootless_none_mode() {
        let mut cni = CniConfig::default();
        cni.set_rootless_config(Some(crate::rootless::EffectiveRootlessConfig {
            enabled: true,
            current_uid: 1000,
            current_gid: 1000,
            in_user_namespace: false,
            xdg_runtime_dir: PathBuf::from("/tmp/rootless-runtime"),
            xdg_data_home: PathBuf::from("/tmp/rootless-data"),
            storage_root: PathBuf::from("/tmp/rootless-data/crius/storage"),
            runtime_root: PathBuf::from("/tmp/rootless-runtime/crius"),
            netns_dir: PathBuf::from("/tmp/rootless-runtime/crius/netns"),
            use_fuse_overlayfs: true,
            network_mode: crate::rootless::NetworkMode::None,
            slirp4netns_path: PathBuf::from("slirp4netns"),
            pasta_path: PathBuf::from("pasta"),
            disable_cgroup: true,
            tolerate_missing_hugetlb_controller: true,
        }));
        let manager = DefaultNetworkManager::from_cni_config(cni);
        let status = manager
            .start_rootless_network_helper("pod-1", "/tmp/nonexistent-netns")
            .unwrap();
        assert_eq!(status.name, "none");
        assert!(status.ip.is_none());
    }

    #[tokio::test]
    async fn namespace_manager_uses_configured_helper_path() {
        let dir = tempfile::tempdir().unwrap();
        let helper_path = dir.path().join("fake-pinns.sh");
        let args_path = dir.path().join("helper.args");
        fs::write(
            &helper_path,
            format!(
                r#"#!/bin/sh
set -eu
dir=""
name=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d)
      dir="$2"
      shift 2
      ;;
    -f)
      name="$2"
      shift 2
      ;;
    --net)
      shift
      ;;
    *)
      shift
      ;;
  esac
done
printf '%s\n' "$dir" "$name" > "{}"
mkdir -p "$dir/netns"
: > "$dir/netns/$name"
"#,
                args_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&helper_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&helper_path, perms).unwrap();

        let manager = NamespaceManager::with_helper(
            dir.path().join("state").join("netns"),
            Some(helper_path.clone()),
        );
        let path = manager.create("pod-1").await.unwrap();

        assert_eq!(path, dir.path().join("state").join("netns").join("pod-1"));
        assert!(path.exists());

        let args = fs::read_to_string(args_path).unwrap();
        let mut lines = args.lines();
        assert_eq!(
            lines.next(),
            Some(dir.path().join("state").display().to_string().as_str())
        );
        assert_eq!(lines.next(), Some("pod-1"));
    }
}
