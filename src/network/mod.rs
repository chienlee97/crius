//! 网络模块
//!
//! 提供容器网络功能，包括网络命名空间管理、CNI 接口等。

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
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
    ) -> Result<NetworkStatus, NetworkError>;

    /// 清理 Pod 网络
    async fn teardown_pod_network(&self, pod_id: &str, netns: &str) -> Result<(), NetworkError>;
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
        Self {
            cni_plugin_dirs: cni_plugin_dirs.unwrap_or_else(|| vec!["/opt/cni/bin".to_string()]),
            cni_config_dirs: cni_config_dirs.unwrap_or_else(|| vec!["/etc/cni/net.d".to_string()]),
            cni_cache_dir: cni_cache_dir.unwrap_or_else(|| "/var/lib/cni/cache".to_string()),
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
    ) -> Result<NetworkStatus, NetworkError> {
        let netns_name = Path::new(netns)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(netns);
        self.ensure_loopback_up(netns_name).await?;

        // 这里将调用 CNI 插件来设置网络
        // 简化实现，实际实现中需要调用 CNI 插件
        Ok(NetworkStatus {
            name: "default".to_string(),
            ip: None,
            mac: None,
            interfaces: vec![],
        })
    }

    async fn teardown_pod_network(&self, _pod_id: &str, _netns: &str) -> Result<(), NetworkError> {
        // 这里将调用 CNI 插件来清理网络
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
