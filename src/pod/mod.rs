//! Pod沙箱管理模块
//!
//! 提供Pod沙箱的创建、管理和清理功能

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use anyhow::{Context, Result};
use log::{info, debug, error};
use tokio::process::Command;

use crate::runtime::{ContainerRuntime, ContainerConfig, ContainerStatus};
use crate::network::{NetworkManager, DefaultNetworkManager, NetworkStatus, PortMappingManager, PortMapping as NetworkPortMapping, Protocol, PortMappingBackend};
use std::net::IpAddr;

/// Pod沙箱配置
#[derive(Debug, Clone)]
pub struct PodSandboxConfig {
    /// Pod名称
    pub name: String,
    /// 命名空间
    pub namespace: String,
    /// UID
    pub uid: String,
    /// 主机名
    pub hostname: String,
    /// 标签
    pub labels: Vec<(String, String)>,
    /// 注解
    pub annotations: Vec<(String, String)>,
    /// DNS配置
    pub dns_config: Option<DNSConfig>,
    /// 端口映射
    pub port_mappings: Vec<PortMapping>,
    /// 网络配置
    pub network_config: Option<NetworkConfig>,
}

/// DNS配置
#[derive(Debug, Clone)]
pub struct DNSConfig {
    pub servers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<String>,
}

/// 端口映射
#[derive(Debug, Clone)]
pub struct PortMapping {
    pub protocol: String,
    pub container_port: i32,
    pub host_port: i32,
    pub host_ip: String,
}

/// 网络配置
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub network_namespace: String,
    pub pod_cidr: String,
}

/// Pod沙箱信息
#[derive(Debug, Clone)]
pub struct PodSandbox {
    /// Pod ID
    pub id: String,
    /// 配置
    pub config: PodSandboxConfig,
    /// 网络命名空间路径
    pub netns_path: PathBuf,
    /// Pause容器ID
    pub pause_container_id: String,
    /// Pod状态
    pub state: PodSandboxState,
    /// 创建时间
    pub created_at: i64,
    /// IP地址
    pub ip: String,
    /// 网络状态
    pub network_status: Option<NetworkStatus>,
}

/// Pod沙箱状态
#[derive(Debug, Clone, PartialEq)]
pub enum PodSandboxState {
    Ready,
    NotReady,
    Terminated,
}

/// Pod沙箱管理器
pub struct PodSandboxManager<R: ContainerRuntime> {
    /// 运行时
    runtime: R,
    /// 网络管理器
    network_manager: DefaultNetworkManager,
    /// Pod沙箱根目录
    root_dir: PathBuf,
    /// 运行中的Pod沙箱
    pods: HashMap<String, PodSandbox>,
}

impl<R: ContainerRuntime> std::fmt::Debug for PodSandboxManager<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodSandboxManager")
            .field("root_dir", &self.root_dir)
            .field("pods", &self.pods)
            .finish()
    }
}

impl<R: ContainerRuntime> PodSandboxManager<R> {
    /// 创建新的Pod沙箱管理器
    pub fn new(runtime: R, root_dir: PathBuf) -> Self {
        let network_manager = DefaultNetworkManager::new(None, None, None);
        
        Self {
            runtime,
            network_manager,
            root_dir,
            pods: HashMap::new(),
        }
    }

    /// 创建Pod沙箱
    pub async fn create_pod_sandbox(&mut self, config: PodSandboxConfig) -> Result<String> {
        let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
        info!("Creating pod sandbox {} (name: {}, namespace: {})", 
              pod_id, config.name, config.namespace);

        // 1. 创建Pod目录
        let pod_dir = self.root_dir.join(&pod_id);
        tokio::fs::create_dir_all(&pod_dir).await?;

        // 2. 创建网络命名空间
        let netns_name = format!("crius-{}-{}", config.namespace, config.name);
        let netns_path = PathBuf::from(format!("/var/run/netns/{}" , netns_name));
        
        debug!("Creating network namespace: {}", netns_name);
        self.network_manager.create_network_namespace(&netns_name).await
            .context("Failed to create network namespace")?;

        // 3. 设置Pod网络（CNI）
        debug!("Setting up pod network for {}", pod_id);
        let network_status = self.network_manager.setup_pod_network(
            &pod_id,
            &netns_path.to_string_lossy(),
            &config.name,
            &config.namespace,
        ).await?;

        // 4. 创建pause容器
        debug!("Creating pause container for pod {}", pod_id);
        let pause_container_id = self.create_pause_container(&pod_id, &config, &netns_path).await
            .context("Failed to create pause container")?;

        // 5. 创建Pod沙箱对象
        let pod = PodSandbox {
            id: pod_id.clone(),
            config: config.clone(),
            netns_path: netns_path.clone(),
            pause_container_id: pause_container_id.clone(),
            state: PodSandboxState::Ready,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as i64,
            ip: network_status.ip.as_ref().map(|ip| ip.to_string()).unwrap_or_default(),
            network_status: Some(network_status),
        };

        self.pods.insert(pod_id.clone(), pod);

        info!("Pod sandbox {} created successfully", pod_id);
        Ok(pod_id)
    }

    /// 创建pause容器
    async fn create_pause_container(
        &self,
        pod_id: &str,
        pod_config: &PodSandboxConfig,
        netns_path: &Path,
    ) -> Result<String> {
        // Pause容器配置
        let pause_config = ContainerConfig {
            name: format!("pause-{}", pod_id),
            image: "registry.k8s.io/pause:3.9".to_string(),
            command: vec!["/pause".to_string()],
            args: vec![],
            env: vec![],
            working_dir: None,
            mounts: vec![],
            labels: vec![],
            annotations: vec![],
            privileged: false,
            user: None,
            hostname: Some(pod_config.hostname.clone()),
            // Pause容器使用自己的rootfs，实际应用中需要从镜像创建
            rootfs: self.root_dir.join(pod_id).join("pause-rootfs"),
        };

        // 创建pause容器
        let container_id = self.runtime.create_container(&pause_config)
            .context("Failed to create pause container")?;

        // 启动pause容器
        self.runtime.start_container(&container_id)
            .context("Failed to start pause container")?;

        // 验证pause容器运行状态
        match self.runtime.container_status(&container_id)? {
            ContainerStatus::Running => {
                debug!("Pause container {} is running", container_id);
            }
            status => {
                error!("Pause container {} is not running: {:?}", container_id, status);
                let _ = self.runtime.remove_container(&container_id);
                return Err(anyhow::anyhow!("Pause container failed to start: {:?}", status));
            }
        }

        Ok(container_id)
    }

    /// 停止Pod沙箱
    pub async fn stop_pod_sandbox(&mut self, pod_id: &str) -> Result<()> {
        info!("Stopping pod sandbox {}", pod_id);

        if let Some(pod) = self.pods.get(pod_id) {
            // 1. 停止pause容器
            debug!("Stopping pause container {}", pod.pause_container_id);
            let _ = self.runtime.stop_container(&pod.pause_container_id, Some(30));
            let _ = self.runtime.remove_container(&pod.pause_container_id);

            // 2. 清理网络
            debug!("Tearing down pod network for {}", pod_id);
            let _ = self.network_manager.teardown_pod_network(
                pod_id,
                &pod.netns_path.to_string_lossy(),
            ).await;

            // 3. 删除网络命名空间
            let netns_name = pod.netns_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            if !netns_name.is_empty() {
                let _ = self.network_manager.remove_network_namespace(netns_name).await;
            }

            // 4. 更新Pod状态
            if let Some(pod) = self.pods.get_mut(pod_id) {
                pod.state = PodSandboxState::Terminated;
            }
        }

        info!("Pod sandbox {} stopped", pod_id);
        Ok(())
    }

    /// 删除Pod沙箱
    pub async fn remove_pod_sandbox(&mut self, pod_id: &str) -> Result<()> {
        info!("Removing pod sandbox {}", pod_id);

        // 确保Pod已停止
        let _ = self.stop_pod_sandbox(pod_id).await;

        // 清理Pod目录
        let pod_dir = self.root_dir.join(pod_id);
        if pod_dir.exists() {
            tokio::fs::remove_dir_all(&pod_dir).await?;
        }

        // 从内存中移除
        self.pods.remove(pod_id);

        info!("Pod sandbox {} removed", pod_id);
        Ok(())
    }

    /// 获取Pod沙箱状态
    pub fn pod_sandbox_status(&self, pod_id: &str) -> Result<PodSandboxStatus> {
        let pod = self.pods.get(pod_id)
            .ok_or_else(|| anyhow::anyhow!("Pod sandbox {} not found", pod_id))?;

        // 检查pause容器状态
        let pause_status = self.runtime.container_status(&pod.pause_container_id)?;
        let pod_state = match pause_status {
            ContainerStatus::Running => PodSandboxState::Ready,
            ContainerStatus::Created => PodSandboxState::NotReady,
            _ => PodSandboxState::Terminated,
        };

        Ok(PodSandboxStatus {
            id: pod.id.clone(),
            state: pod_state,
            created_at: pod.created_at,
            ip: pod.ip.clone(),
            network_status: pod.network_status.clone(),
        })
    }

    /// 列出所有Pod沙箱
    pub fn list_pod_sandboxes(&self) -> Vec<&PodSandbox> {
        self.pods.values().collect()
    }

    /// 获取Pod网络命名空间路径
    pub fn get_pod_netns(&self, pod_id: &str) -> Option<PathBuf> {
        self.pods.get(pod_id).map(|p| p.netns_path.clone())
    }
}

/// Pod沙箱状态（简化版）
#[derive(Debug, Clone)]
pub struct PodSandboxStatus {
    pub id: String,
    pub state: PodSandboxState,
    pub created_at: i64,
    pub ip: String,
    pub network_status: Option<NetworkStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{RuncRuntime, ContainerConfig};
    use tempfile::tempdir;

    // 注意：这些测试需要root权限和runc环境
    #[tokio::test]
    #[ignore = "requires root and runc"]
    async fn test_pod_sandbox_creation() {
        let temp_dir = tempdir().unwrap();
        let runtime = RuncRuntime::new(
            PathBuf::from("runc"),
            temp_dir.path().join("runtime"),
        );

        let mut manager = PodSandboxManager::new(runtime, temp_dir.path().join("pods"));

        let config = PodSandboxConfig {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
            uid: "test-uid".to_string(),
            hostname: "test-host".to_string(),
            labels: vec![],
            annotations: vec![],
            dns_config: None,
            port_mappings: vec![],
            network_config: None,
        };

        let pod_id = manager.create_pod_sandbox(config).await.unwrap();
        assert!(!pod_id.is_empty());

        // 清理
        manager.remove_pod_sandbox(&pod_id).await.unwrap();
    }
}
