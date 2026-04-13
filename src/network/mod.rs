//! 网络模块
//!
//! 提供容器网络功能，包括网络命名空间管理、CNI 接口等。

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::process::Command;

pub mod cni;
mod error;
pub mod multi;
mod port_mapping;
mod types;

pub use cni::CniManager;
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
        }
    }
}

impl CniConfig {
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
        pod_cidr: Option<&str>,
    ) -> Result<NetworkStatus, NetworkError>;

    /// 清理 Pod 网络
    async fn teardown_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_namespace: &str,
        pod_name: &str,
    ) -> Result<(), NetworkError>;
}

/// 默认网络管理器实现
#[derive(Debug)]
pub struct DefaultNetworkManager {
    cni_plugin_dirs: Vec<String>,
    cni_config_dirs: Vec<String>,
    cni_cache_dir: String,
}

impl DefaultNetworkManager {
    fn netns_exists(ns: &str) -> bool {
        let p = Path::new(ns);
        p.exists()
            || Path::new("/var/run/netns").join(ns).exists()
            || Path::new("/run/netns").join(ns).exists()
    }

    async fn ensure_loopback_up(&self, netns: &str) -> Result<(), NetworkError> {
        let status = Command::new("ip")
            .args(["-n", netns, "link", "set", "lo", "up"])
            .status()
            .await?;

        if !status.success() {
            return Err(NetworkError::CommandExecutionError {
                command: format!("ip -n {} link set lo up", netns),
                status,
            });
        }

        Ok(())
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
        }
    }
}

#[async_trait]
impl NetworkManager for DefaultNetworkManager {
    async fn init(&self) -> Result<(), NetworkError> {
        // 创建缓存目录
        if !Path::new(&self.cni_cache_dir).exists() {
            tokio::fs::create_dir_all(&self.cni_cache_dir).await?;
        }
        Ok(())
    }

    async fn create_network_namespace(&self, ns_path: &str) -> Result<(), NetworkError> {
        if !DefaultNetworkManager::netns_exists(ns_path) {
            let status = Command::new("ip")
                .args(["netns", "add", ns_path])
                .status()
                .await?;

            if !status.success() {
                return Err(NetworkError::CommandExecutionError {
                    command: format!("ip netns add {}", ns_path),
                    status,
                });
            }
        }
        Ok(())
    }

    async fn remove_network_namespace(&self, ns_path: &str) -> Result<(), NetworkError> {
        if DefaultNetworkManager::netns_exists(ns_path) {
            let status = Command::new("ip")
                .args(["netns", "delete", ns_path])
                .status()
                .await?;

            if !status.success() {
                return Err(NetworkError::CommandExecutionError {
                    command: format!("ip netns delete {}", ns_path),
                    status,
                });
            }
        }
        Ok(())
    }

    async fn setup_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
        pod_cidr: Option<&str>,
    ) -> Result<NetworkStatus, NetworkError> {
        let netns_name = Path::new(netns)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(netns);
        self.ensure_loopback_up(netns_name).await?;

        let mut cni = CniManager::new(
            self.cni_plugin_dirs.clone(),
            self.cni_config_dirs.clone(),
            self.cni_cache_dir.clone(),
        )
        .map_err(|e| NetworkError::Other(e.to_string()))?;
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
    ) -> Result<(), NetworkError> {
        let mut cni = CniManager::new(
            self.cni_plugin_dirs.clone(),
            self.cni_config_dirs.clone(),
            self.cni_cache_dir.clone(),
        )
        .map_err(|e| NetworkError::Other(e.to_string()))?;
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
