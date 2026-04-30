//! 网络模块
//!
//! 提供容器网络功能，包括网络命名空间管理、CNI 接口等。

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
    runtime_handler_config_dirs: std::collections::HashMap<String, Vec<PathBuf>>,
    runtime_handler_max_conf_nums: std::collections::HashMap<String, usize>,
    default_network_name: Option<String>,
    disable_hostport_mapping: bool,
    netns_mount_dir: PathBuf,
    netns_mounts_under_state_dir: bool,
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
            runtime_handler_config_dirs: std::collections::HashMap::new(),
            runtime_handler_max_conf_nums: std::collections::HashMap::new(),
            default_network_name: None,
            disable_hostport_mapping: false,
            netns_mount_dir: PathBuf::from("/var/run/netns"),
            netns_mounts_under_state_dir: false,
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
        }
    }

    pub fn config_dirs(&self) -> &[PathBuf] {
        &self.config_dirs
    }

    pub fn plugin_dirs(&self) -> &[PathBuf] {
        &self.plugin_dirs
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

    pub fn ip_pref(&self) -> MainIpPreference {
        self.ip_pref
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

    pub fn netns_path(&self, ns_name_or_path: &str) -> PathBuf {
        NamespaceManager::new(self.netns_mount_dir.clone()).resolve_path(ns_name_or_path)
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
}

impl NamespaceManager {
    pub fn new(mount_dir: PathBuf) -> Self {
        Self { mount_dir }
    }

    pub fn mount_dir(&self) -> &Path {
        &self.mount_dir
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

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _mountpoint = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;

        let mount_path = path.clone();
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
        .map_err(|_| NetworkError::Other("network namespace creator thread panicked".to_string()))??;

        Ok(path)
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
    cni_runtime_handler_config_dirs: std::collections::HashMap<String, Vec<String>>,
    cni_runtime_handler_max_conf_nums: std::collections::HashMap<String, usize>,
    cni_default_network_name: Option<String>,
    namespace_manager: NamespaceManager,
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
}

#[async_trait]
impl NetworkManager for DefaultNetworkManager {
    async fn init(&self) -> Result<(), NetworkError> {
        // 创建缓存目录
        if !Path::new(&self.cni_cache_dir).exists() {
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
        runtime_handler: &str,
        pod_cidr: Option<&str>,
    ) -> Result<NetworkStatus, NetworkError> {
        self.ensure_loopback_up(netns).await?;

        let mut cni = CniManager::new(
            self.cni_plugin_dirs.clone(),
            self.effective_cni_config_dirs(runtime_handler),
            self.cni_cache_dir.clone(),
        )
        .map_err(|e| NetworkError::Other(e.to_string()))?;
        cni.set_max_conf_num(self.effective_cni_max_conf_num(runtime_handler));
        cni.set_ip_pref(self.cni_ip_pref);
        cni.set_default_network_name(self.cni_default_network_name.clone());
        cni.load_network_configs()
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))?;

        cni.setup_pod_network(pod_id, netns, pod_name, pod_namespace, pod_cidr)
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))
    }

    async fn teardown_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_namespace: &str,
        pod_name: &str,
        runtime_handler: &str,
    ) -> Result<(), NetworkError> {
        let mut cni = CniManager::new(
            self.cni_plugin_dirs.clone(),
            self.effective_cni_config_dirs(runtime_handler),
            self.cni_cache_dir.clone(),
        )
        .map_err(|e| NetworkError::Other(e.to_string()))?;
        cni.set_max_conf_num(self.effective_cni_max_conf_num(runtime_handler));
        cni.set_ip_pref(self.cni_ip_pref);
        cni.set_default_network_name(self.cni_default_network_name.clone());
        cni.load_network_configs()
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))?;
        let _ = cni
            .teardown_pod_network(pod_id, netns, pod_namespace, pod_name)
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::process::Command;

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

    #[tokio::test]
    #[ignore = "requires root privileges and iproute2"] // 需要root权限运行ip netns
    async fn test_network_namespace() -> Result<(), Box<dyn std::error::Error>> {
        let manager = DefaultNetworkManager::new(None, None, None);

        // 使用简单的命名空间名称（不是路径）
        let ns_name = "crius-test-ns";

        // 测试创建网络命名空间
        manager.create_network_namespace(ns_name).await?;

        // 验证命名空间存在（在 /var/run/netns/ 或 /run/netns/）
        let ns_exists = std::path::Path::new("/run/netns").join(ns_name).exists()
            || std::path::Path::new("/var/run/netns")
                .join(ns_name)
                .exists();
        assert!(ns_exists, "Network namespace should exist");

        // 测试删除网络命名空间
        manager.remove_network_namespace(ns_name).await?;

        let ns_exists_after = std::path::Path::new("/run/netns").join(ns_name).exists()
            || std::path::Path::new("/var/run/netns")
                .join(ns_name)
                .exists();
        assert!(!ns_exists_after, "Network namespace should be deleted");

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires root privileges and iproute2"]
    async fn test_setup_pod_network_brings_loopback_up() -> Result<(), Box<dyn std::error::Error>> {
        let manager = DefaultNetworkManager::new(None, None, None);
        let ns_name = "crius-test-loopback";

        manager.create_network_namespace(ns_name).await?;
        manager
            .setup_pod_network(
                "pod-1",
                &format!("/var/run/netns/{}", ns_name),
                "pod",
                "default",
                "runc",
                None,
            )
            .await?;

        let output = Command::new("ip")
            .args(["-n", ns_name, "addr", "show", "lo"])
            .output()
            .await?;
        assert!(
            output.status.success(),
            "expected ip addr show lo to succeed"
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("127.0.0.1/8"));
        assert!(stdout.contains("LOOPBACK,UP"));

        manager.remove_network_namespace(ns_name).await?;
        Ok(())
    }
}
