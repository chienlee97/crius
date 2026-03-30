//! 网络模块
//! 
//! 提供容器网络功能，包括网络命名空间管理、CNI 接口等。

use std::path::Path;
use std::collections::HashMap;
use std::net::IpAddr;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::process::Command;

mod cni;
mod error;
mod types;
mod port_mapping;

pub use cni::CniManager;
pub use error::NetworkError;
pub use types::*;
pub use port_mapping::{PortMappingManager, PortMapping, Protocol, PortMappingBackend};

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
    async fn teardown_pod_network(
        &self,
        pod_id: &str,
        netns: &str,
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
    /// 创建新的网络管理器实例
    pub fn new(
        cni_plugin_dirs: Option<Vec<String>>,
        cni_config_dirs: Option<Vec<String>>,
        cni_cache_dir: Option<String>,
    ) -> Self {
        Self {
            cni_plugin_dirs: cni_plugin_dirs.unwrap_or_else(|| {
                vec![
                    "/opt/cni/bin".to_string(),
                ]
            }),
            cni_config_dirs: cni_config_dirs.unwrap_or_else(|| {
                vec![
                    "/etc/cni/net.d".to_string(),
                ]
            }),
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
        if !Path::new(ns_path).exists() {
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
        if Path::new(ns_path).exists() {
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
        // 这里将调用 CNI 插件来设置网络
        // 简化实现，实际实现中需要调用 CNI 插件
        Ok(NetworkStatus {
            name: "default".to_string(),
            ip: None,
            mac: None,
            interfaces: vec![],
        })
    }

    async fn teardown_pod_network(
        &self,
        _pod_id: &str,
        _netns: &str,
    ) -> Result<(), NetworkError> {
        // 这里将调用 CNI 插件来清理网络
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_network_namespace() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let ns_path = temp_dir.path().join("testns");
        let ns_path_str = ns_path.to_str().unwrap();

        let manager = DefaultNetworkManager::new(None, None, None);
        
        // 测试创建网络命名空间
        manager.create_network_namespace(ns_path_str).await?;
        assert!(ns_path.exists());

        // 测试删除网络命名空间
        manager.remove_network_namespace(ns_path_str).await?;
        assert!(!ns_path.exists());

        Ok(())
    }
}