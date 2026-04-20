//! Pod沙箱管理模块
//!
//! 提供Pod沙箱的创建、管理和清理功能

use anyhow::{Context, Result};
use log::{debug, error, info};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::process::Command;

use crate::network::{
    CniConfig, DefaultNetworkManager, NetworkInterface, NetworkManager, NetworkStatus,
};
use crate::proto::runtime::v1::{LinuxContainerResources, NamespaceOption};
use crate::runtime::{
    ContainerConfig, ContainerRuntime, ContainerStatus, NamespacePaths, SeccompProfile,
};
use std::collections::HashSet;
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
    /// 容器日志目录
    pub log_directory: Option<PathBuf>,
    /// 运行时处理器
    pub runtime_handler: String,
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
    /// Pod cgroup父路径
    pub cgroup_parent: Option<String>,
    /// Pod sysctl
    pub sysctls: HashMap<String, String>,
    /// Pod namespace配置
    pub namespace_options: Option<NamespaceOption>,
    /// 安全上下文
    pub privileged: bool,
    pub run_as_user: Option<String>,
    pub run_as_group: Option<u32>,
    pub supplemental_groups: Vec<u32>,
    pub readonly_rootfs: bool,
    pub no_new_privileges: Option<bool>,
    pub apparmor_profile: Option<String>,
    pub selinux_label: Option<String>,
    pub seccomp_profile: Option<SeccompProfile>,
    /// Pod级资源限制（优先使用 resources）
    pub linux_resources: Option<LinuxContainerResources>,
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
    /// 默认pause镜像（CRI-O风格：由运行时配置提供）
    pause_image: String,
    /// 运行中的Pod沙箱
    pods: HashMap<String, PodSandbox>,
}

impl<R: ContainerRuntime> std::fmt::Debug for PodSandboxManager<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodSandboxManager")
            .field("root_dir", &self.root_dir)
            .field("pause_image", &self.pause_image)
            .field("pods", &self.pods)
            .finish()
    }
}

impl<R: ContainerRuntime> PodSandboxManager<R> {
    fn pod_resolv_path(&self, pod_id: &str) -> PathBuf {
        self.root_dir.join(pod_id).join("resolv.conf")
    }

    async fn create_resolv_conf(
        &self,
        pod_id: &str,
        dns_config: Option<&DNSConfig>,
    ) -> Result<PathBuf> {
        let resolv_path = self.pod_resolv_path(pod_id);
        let use_host_resolv = dns_config
            .map(|config| {
                config.servers.is_empty() && config.searches.is_empty() && config.options.is_empty()
            })
            .unwrap_or(true);

        if use_host_resolv {
            tokio::fs::copy("/etc/resolv.conf", &resolv_path)
                .await
                .context("Failed to copy host resolv.conf")?;
            return Ok(resolv_path);
        }

        let dns_config = dns_config.expect("dns_config checked above");
        let mut contents = String::new();
        if !dns_config.searches.is_empty() {
            contents.push_str("search ");
            contents.push_str(&dns_config.searches.join(" "));
            contents.push('\n');
        }
        for server in &dns_config.servers {
            contents.push_str("nameserver ");
            contents.push_str(server);
            contents.push('\n');
        }
        if !dns_config.options.is_empty() {
            contents.push_str("options ");
            contents.push_str(&dns_config.options.join(" "));
            contents.push('\n');
        }

        tokio::fs::write(&resolv_path, contents)
            .await
            .context("Failed to write pod resolv.conf")?;
        Ok(resolv_path)
    }

    async fn discover_netns_interfaces(&self, netns_name: &str) -> Vec<NetworkInterface> {
        let output = match Command::new("ip")
            .args(["-n", netns_name, "-o", "addr", "show"])
            .output()
            .await
        {
            Ok(output) if output.status.success() => output,
            Ok(output) => {
                debug!(
                    "Failed to probe netns addresses for {}: status {:?}",
                    netns_name,
                    output.status.code()
                );
                return Vec::new();
            }
            Err(e) => {
                debug!("Failed to execute ip addr in netns {}: {}", netns_name, e);
                return Vec::new();
            }
        };

        let mut seen = HashSet::new();
        let mut interfaces = Vec::new();
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 4 {
                continue;
            }
            let iface_name = fields[1].trim_end_matches(':');
            let family = fields[2];
            if family != "inet" && family != "inet6" {
                continue;
            }
            let addr = fields[3].split('/').next().unwrap_or_default();
            let ip = match addr.parse::<IpAddr>() {
                Ok(ip) => ip,
                Err(_) => continue,
            };
            if ip.is_loopback() {
                continue;
            }
            if !seen.insert(ip) {
                continue;
            }
            interfaces.push(NetworkInterface {
                name: iface_name.to_string(),
                ip: Some(ip),
                mac: None,
                netmask: None,
                gateway: None,
            });
        }

        interfaces
    }

    fn resolve_pause_image(&self, pod_config: &PodSandboxConfig) -> Result<String> {
        // Match CRI-O behavior: configured pause image is the default source.
        // Allow kubelet-provided sandbox image annotation to override when present.
        for (k, v) in &pod_config.annotations {
            if k == "io.kubernetes.cri.sandbox-image" && !v.trim().is_empty() {
                return Ok(v.clone());
            }
        }

        if self.pause_image.trim().is_empty() {
            return Err(anyhow::anyhow!("pause image is not configured"));
        }
        Ok(self.pause_image.clone())
    }

    /// 创建新的Pod沙箱管理器
    pub fn new(runtime: R, root_dir: PathBuf, pause_image: String, cni_config: CniConfig) -> Self {
        let network_manager = DefaultNetworkManager::from_cni_config(cni_config);

        Self {
            runtime,
            network_manager,
            root_dir,
            pause_image,
            pods: HashMap::new(),
        }
    }

    /// 创建Pod沙箱
    pub async fn create_pod_sandbox(&mut self, config: PodSandboxConfig) -> Result<String> {
        let pod_id = uuid::Uuid::new_v4().to_simple().to_string();
        info!(
            "Creating pod sandbox {} (name: {}, namespace: {})",
            pod_id, config.name, config.namespace
        );

        // 1. 创建Pod目录
        let pod_dir = self.root_dir.join(&pod_id);
        tokio::fs::create_dir_all(&pod_dir).await?;

        // 1.1 为 Pod 准备 resolv.conf，参考 CRI-O 的 pod 级 DNS 文件做法。
        self.create_resolv_conf(&pod_id, config.dns_config.as_ref())
            .await
            .context("Failed to create pod resolv.conf")?;

        // 2. 创建网络命名空间
        let netns_name = format!("crius-{}-{}", config.namespace, config.name);
        let netns_path = PathBuf::from(format!("/var/run/netns/{}", netns_name));

        debug!("Creating network namespace: {}", netns_name);
        self.network_manager
            .create_network_namespace(&netns_name)
            .await
            .context("Failed to create network namespace")?;

        // 3. 设置Pod网络（CNI）
        debug!("Setting up pod network for {}", pod_id);
        let mut network_status = self
            .network_manager
            .setup_pod_network(
                &pod_id,
                &netns_path.to_string_lossy(),
                &config.name,
                &config.namespace,
                config
                    .network_config
                    .as_ref()
                    .map(|network| network.pod_cidr.as_str()),
            )
            .await?;
        let discovered_interfaces = self.discover_netns_interfaces(&netns_name).await;
        if network_status.ip.is_none() {
            network_status.ip = discovered_interfaces
                .iter()
                .find_map(|iface| iface.ip.as_ref().copied());
        }
        if !discovered_interfaces.is_empty() {
            network_status.interfaces = discovered_interfaces;
        }

        // 4. 创建pause容器
        debug!("Creating pause container for pod {}", pod_id);
        let pause_container_id = self
            .create_pause_container(&pod_id, &config, &netns_path)
            .await
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
            ip: network_status
                .ip
                .as_ref()
                .map(|ip| ip.to_string())
                .unwrap_or_default(),
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
        let pause_image = self.resolve_pause_image(pod_config)?;
        let mut pause_mounts = Vec::new();
        let resolv_path = self.pod_resolv_path(pod_id);
        if resolv_path.exists() {
            pause_mounts.push(crate::runtime::MountConfig {
                source: resolv_path,
                destination: PathBuf::from("/etc/resolv.conf"),
                read_only: true,
            });
        }

        // Pause容器配置
        let pause_config = ContainerConfig {
            name: format!("pause-{}", pod_id),
            image: pause_image,
            command: vec!["/pause".to_string()],
            args: vec![],
            env: vec![],
            working_dir: None,
            mounts: pause_mounts,
            labels: vec![],
            annotations: vec![],
            privileged: pod_config.privileged,
            user: pod_config.run_as_user.clone(),
            run_as_group: pod_config.run_as_group,
            supplemental_groups: pod_config.supplemental_groups.clone(),
            hostname: Some(pod_config.hostname.clone()),
            tty: false,
            stdin: false,
            stdin_once: false,
            log_path: pod_config
                .log_directory
                .as_ref()
                .map(|dir| dir.join("sandbox.log")),
            readonly_rootfs: pod_config.readonly_rootfs,
            no_new_privileges: pod_config.no_new_privileges,
            apparmor_profile: pod_config.apparmor_profile.clone(),
            selinux_label: pod_config.selinux_label.clone(),
            seccomp_profile: pod_config.seccomp_profile.clone(),
            capabilities: None,
            cgroup_parent: pod_config.cgroup_parent.clone(),
            sysctls: pod_config.sysctls.clone(),
            namespace_options: pod_config.namespace_options.clone(),
            namespace_paths: NamespacePaths {
                network: Some(netns_path.to_path_buf()),
                ..Default::default()
            },
            linux_resources: pod_config.linux_resources.clone(),
            devices: vec![],
            // Pause容器使用自己的rootfs，实际应用中需要从镜像创建
            rootfs: self.root_dir.join(pod_id).join("pause-rootfs"),
        };

        // 创建 pause 容器，ID 由上层（pod 管理器）统一分配，避免 runtime 二次生成。
        let container_id = format!("pause-{}", pod_id);
        self.runtime
            .create_container(&container_id, &pause_config)
            .context("Failed to create pause container")?;

        // 启动pause容器
        self.runtime
            .start_container(&container_id)
            .context("Failed to start pause container")?;

        // 验证pause容器运行状态
        match self.runtime.container_status(&container_id)? {
            ContainerStatus::Running => {
                debug!("Pause container {} is running", container_id);
            }
            status => {
                error!(
                    "Pause container {} is not running: {:?}",
                    container_id, status
                );
                let _ = self.runtime.remove_container(&container_id);
                return Err(anyhow::anyhow!(
                    "Pause container failed to start: {:?}",
                    status
                ));
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
            let _ = self
                .runtime
                .stop_container(&pod.pause_container_id, Some(30));
            let _ = self.runtime.remove_container(&pod.pause_container_id);

            // 2. 清理网络
            debug!("Tearing down pod network for {}", pod_id);
            let _ = self
                .network_manager
                .teardown_pod_network(
                    pod_id,
                    &pod.netns_path.to_string_lossy(),
                    &pod.config.namespace,
                    &pod.config.name,
                )
                .await;

            // 3. 删除网络命名空间
            let netns_name = pod
                .netns_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            if !netns_name.is_empty() {
                let _ = self
                    .network_manager
                    .remove_network_namespace(netns_name)
                    .await;
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
        let pod = self
            .pods
            .get(pod_id)
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

    /// 获取Pod沙箱信息
    pub fn get_pod_sandbox(&self, pod_id: &str) -> Option<&PodSandbox> {
        self.pods.get(pod_id)
    }

    /// 获取Pod沙箱信息副本
    pub fn get_pod_sandbox_cloned(&self, pod_id: &str) -> Option<PodSandbox> {
        self.pods.get(pod_id).cloned()
    }

    /// 恢复Pod沙箱到内存管理器
    pub fn restore_pod_sandbox(&mut self, pod: PodSandbox) {
        self.pods.insert(pod.id.clone(), pod);
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
    use crate::runtime::RuncRuntime;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_resolv_conf_copies_host_config_when_dns_unspecified() {
        let temp_dir = tempdir().unwrap();
        let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("runtime"));
        let manager = PodSandboxManager::new(
            runtime,
            temp_dir.path().join("pods"),
            "registry.k8s.io/pause:3.9".to_string(),
            CniConfig::default(),
        );
        tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
            .await
            .unwrap();

        let resolv_path = manager.create_resolv_conf("pod-1", None).await.unwrap();
        let generated = tokio::fs::read_to_string(&resolv_path).await.unwrap();
        let host = tokio::fs::read_to_string("/etc/resolv.conf").await.unwrap();
        assert_eq!(generated, host);
    }

    #[tokio::test]
    async fn create_resolv_conf_writes_explicit_dns_config() {
        let temp_dir = tempdir().unwrap();
        let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("runtime"));
        let manager = PodSandboxManager::new(
            runtime,
            temp_dir.path().join("pods"),
            "registry.k8s.io/pause:3.9".to_string(),
            CniConfig::default(),
        );
        tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
            .await
            .unwrap();

        let dns = DNSConfig {
            servers: vec!["1.1.1.1".to_string(), "8.8.8.8".to_string()],
            searches: vec!["example.com".to_string()],
            options: vec!["ndots:5".to_string()],
        };
        let resolv_path = manager
            .create_resolv_conf("pod-1", Some(&dns))
            .await
            .unwrap();
        let generated = tokio::fs::read_to_string(&resolv_path).await.unwrap();

        assert!(generated.contains("search example.com"));
        assert!(generated.contains("nameserver 1.1.1.1"));
        assert!(generated.contains("nameserver 8.8.8.8"));
        assert!(generated.contains("options ndots:5"));
    }

    // 注意：这些测试需要root权限和runc环境
    #[tokio::test]
    #[ignore = "requires root and runc"]
    async fn test_pod_sandbox_creation() {
        let temp_dir = tempdir().unwrap();
        let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("runtime"));

        let mut manager = PodSandboxManager::new(
            runtime,
            temp_dir.path().join("pods"),
            "registry.k8s.io/pause:3.9".to_string(),
            CniConfig::default(),
        );

        let config = PodSandboxConfig {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
            uid: "test-uid".to_string(),
            hostname: "test-host".to_string(),
            log_directory: None,
            runtime_handler: "runc".to_string(),
            labels: vec![],
            annotations: vec![],
            dns_config: None,
            port_mappings: vec![],
            network_config: None,
            cgroup_parent: None,
            sysctls: HashMap::new(),
            namespace_options: None,
            privileged: false,
            run_as_user: None,
            run_as_group: None,
            supplemental_groups: vec![],
            readonly_rootfs: false,
            no_new_privileges: None,
            apparmor_profile: None,
            selinux_label: None,
            seccomp_profile: None,
            linux_resources: None,
        };

        let pod_id = manager.create_pod_sandbox(config).await.unwrap();
        assert!(!pod_id.is_empty());

        // 清理
        manager.remove_pod_sandbox(&pod_id).await.unwrap();
    }
}
