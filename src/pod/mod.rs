//! Pod沙箱管理模块
//!
//! 提供Pod沙箱的创建、管理和清理功能

use anyhow::{Context, Result};
use log::{debug, error, info};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::network::{
    CniConfig, DefaultNetworkManager, NamespaceManager, NetworkInterface, NetworkManager,
    NetworkSetupRequest, NetworkStatus, PortMapping as HostPortMapping, PortMappingManager,
    Protocol,
};
use crate::proto::runtime::v1::{LinuxContainerResources, NamespaceOption};
use crate::runtime::{
    ContainerConfig, ContainerRuntime, ContainerStatus, NamespacePaths, SeccompProfile,
};
use std::collections::HashSet;
use std::net::IpAddr;

const CRIO_CONTAINER_ID_ANNOTATION: &str = "io.kubernetes.cri-o.ContainerID";
const CRIO_CONTAINER_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.ContainerName";
const CRIO_CONTAINER_TYPE_ANNOTATION: &str = "io.kubernetes.cri-o.ContainerType";
const CRIO_LOG_PATH_ANNOTATION: &str = "io.kubernetes.cri-o.LogPath";
const CRIO_RUNTIME_HANDLER_ANNOTATION: &str = "io.kubernetes.cri-o.RuntimeHandler";
const CRIO_SANDBOX_ID_ANNOTATION: &str = "io.kubernetes.cri-o.SandboxID";
const CRIO_SANDBOX_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.SandboxName";
const CRIO_POD_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.Name";
const CRIO_POD_NAMESPACE_ANNOTATION: &str = "io.kubernetes.cri-o.Namespace";
const CONTAINERD_CONTAINER_TYPE_ANNOTATION: &str = "io.kubernetes.cri.container-type";
const CONTAINERD_SANDBOX_ID_ANNOTATION: &str = "io.kubernetes.cri.sandbox-id";
const CONTAINERD_SANDBOX_LOG_DIR_ANNOTATION: &str = "io.kubernetes.cri.sandbox-log-directory";
const CONTAINERD_SANDBOX_NAME_ANNOTATION: &str = "io.kubernetes.cri.sandbox-name";
const CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION: &str = "io.kubernetes.cri.sandbox-namespace";
const CONTAINERD_SANDBOX_UID_ANNOTATION: &str = "io.kubernetes.cri.sandbox-uid";
const CONTAINERD_RUNTIME_HANDLER_ANNOTATION: &str = "io.containerd.cri.runtime-handler";
const CONTAINER_TYPE_SANDBOX: &str = "sandbox";

#[derive(Debug, Error)]
pub enum PodSandboxError {
    #[error(
        "pause image {image} is not available locally; pull it before starting the pod sandbox"
    )]
    PauseImageNotPresentLocally { image: String },
    #[error("pause image {image} is invalid or incomplete: {detail}")]
    PauseImageInvalid { image: String, detail: String },
}

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
    pub pids_limit: Option<i64>,
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
pub struct PodSandboxManager<R: ContainerRuntime, N: NetworkManager = DefaultNetworkManager> {
    /// 运行时
    runtime: R,
    /// 网络管理器
    network_manager: N,
    /// host port 管理器
    port_mapper: Box<dyn PodPortMapper>,
    /// 是否禁用 hostPort 映射。
    disable_hostport_mapping: bool,
    /// Pod沙箱根目录
    root_dir: PathBuf,
    /// 受管 netns 的生命周期管理器。
    namespace_manager: NamespaceManager,
    /// 默认pause镜像（CRI-O风格：由运行时配置提供）
    pause_image: String,
    /// pause 镜像内的 infra 命令路径。
    pause_command: String,
    /// pause/infra 容器默认 cpuset。
    infra_ctr_cpuset: String,
    /// 运行中的Pod沙箱
    pods: HashMap<String, PodSandbox>,
}

pub(crate) struct PodSandboxManagerOptions {
    disable_hostport_mapping: bool,
    root_dir: PathBuf,
    pause_image: String,
    pause_command: String,
    infra_ctr_cpuset: String,
}

struct PodSandboxRollbackContext<'a> {
    pod_id: &'a str,
    pod_dir: &'a Path,
    netns_name: &'a str,
    netns_path: &'a Path,
    pause_container_id: Option<&'a str>,
    network_attempted: bool,
    netns_created: bool,
    pod_ip: Option<&'a str>,
}

impl<R: ContainerRuntime, N: NetworkManager> std::fmt::Debug for PodSandboxManager<R, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodSandboxManager")
            .field("root_dir", &self.root_dir)
            .field("pause_image", &self.pause_image)
            .field("pause_command", &self.pause_command)
            .field("infra_ctr_cpuset", &self.infra_ctr_cpuset)
            .field("pods", &self.pods)
            .finish()
    }
}

impl<R: ContainerRuntime> PodSandboxManager<R, DefaultNetworkManager> {
    /// 创建新的Pod沙箱管理器
    pub fn new(
        runtime: R,
        root_dir: PathBuf,
        pause_image: String,
        pause_command: String,
        infra_ctr_cpuset: String,
        cni_config: CniConfig,
    ) -> Self {
        let disable_hostport_mapping = cni_config.disable_hostport_mapping();
        let namespace_manager = cni_config.namespace_manager();
        let mut manager = Self::with_network_manager(
            runtime,
            DefaultNetworkManager::from_cni_config(cni_config),
            Box::new(DefaultPodPortMapper),
            PodSandboxManagerOptions {
                disable_hostport_mapping,
                root_dir,
                pause_image,
                pause_command,
                infra_ctr_cpuset,
            },
        );
        manager.namespace_manager = namespace_manager;
        manager
    }

    pub fn reload_runtime_network_settings(&mut self, pause_image: String, cni_config: CniConfig) {
        self.pause_image = pause_image;
        self.disable_hostport_mapping = cni_config.disable_hostport_mapping();
        self.namespace_manager = cni_config.namespace_manager();
        self.network_manager = DefaultNetworkManager::from_cni_config(cni_config);
    }
}

pub(crate) trait PodPortMapper: Send + Sync {
    fn add_port_mapping(&self, mapping: &HostPortMapping) -> Result<()>;
    fn remove_port_mapping(&self, mapping: &HostPortMapping) -> Result<()>;
    fn cleanup_all(&self) -> Result<()>;
}

struct DefaultPodPortMapper;

impl PodPortMapper for DefaultPodPortMapper {
    fn add_port_mapping(&self, mapping: &HostPortMapping) -> Result<()> {
        PortMappingManager::auto()?.add_port_mapping(mapping)
    }

    fn remove_port_mapping(&self, mapping: &HostPortMapping) -> Result<()> {
        PortMappingManager::auto()?.remove_port_mapping(mapping)
    }

    fn cleanup_all(&self) -> Result<()> {
        PortMappingManager::auto()?.cleanup_all_rules()
    }
}

impl<R: ContainerRuntime, N: NetworkManager> PodSandboxManager<R, N> {
    fn classify_pause_image_error(
        err: &anyhow::Error,
        pause_image: &str,
    ) -> Option<PodSandboxError> {
        for cause in err.chain() {
            let message = cause.to_string();
            if message.contains("is not available locally; pull it before creating the container") {
                return Some(PodSandboxError::PauseImageNotPresentLocally {
                    image: pause_image.to_string(),
                });
            }
            if message.contains("has no unpackable layers") {
                return Some(PodSandboxError::PauseImageInvalid {
                    image: pause_image.to_string(),
                    detail: message,
                });
            }
            if let Some(image_error) =
                cause.downcast_ref::<crate::runtime::ImageAvailabilityError>()
            {
                return Some(match image_error {
                    crate::runtime::ImageAvailabilityError::NotPresentLocally { .. } => {
                        PodSandboxError::PauseImageNotPresentLocally {
                            image: pause_image.to_string(),
                        }
                    }
                    crate::runtime::ImageAvailabilityError::NoLayers { .. } => {
                        PodSandboxError::PauseImageInvalid {
                            image: pause_image.to_string(),
                            detail: image_error.to_string(),
                        }
                    }
                });
            }
        }
        None
    }

    fn pod_requires_managed_netns(config: &PodSandboxConfig) -> bool {
        config
            .namespace_options
            .as_ref()
            .map(|options| options.network != crate::proto::runtime::v1::NamespaceMode::Node as i32)
            .unwrap_or(true)
    }

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

    async fn discover_netns_interfaces(&self, netns_path: &Path) -> Vec<NetworkInterface> {
        let netns_path = netns_path.to_path_buf();
        let netns_path_for_log = netns_path.clone();
        let output = match tokio::task::spawn_blocking(move || {
            use std::os::fd::AsRawFd;
            use std::os::unix::process::CommandExt;

            let netns_file = std::fs::File::open(&netns_path)?;
            let netns_fd = netns_file.as_raw_fd();
            let mut command = std::process::Command::new("ip");
            command.args(["-o", "addr", "show"]);
            unsafe {
                command.pre_exec(move || {
                    nix::sched::setns(netns_fd, nix::sched::CloneFlags::CLONE_NEWNET).map_err(
                        |err| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("failed to enter network namespace: {err}"),
                            )
                        },
                    )?;
                    Ok(())
                });
            }
            command.output()
        })
        .await
        {
            Ok(Ok(output)) if output.status.success() => output,
            Ok(Ok(output)) => {
                debug!(
                    "Failed to probe netns addresses for {}: status {:?}",
                    netns_path_for_log.display(),
                    output.status.code()
                );
                return Vec::new();
            }
            Ok(Err(e)) => {
                debug!(
                    "Failed to execute ip addr in netns {}: {}",
                    netns_path_for_log.display(),
                    e
                );
                return Vec::new();
            }
            Err(e) => {
                debug!(
                    "Netns interface probe task failed for {}: {}",
                    netns_path_for_log.display(),
                    e
                );
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

    fn resolve_pause_command(&self) -> Result<String> {
        if self.pause_command.trim().is_empty() {
            return Err(anyhow::anyhow!("pause command is not configured"));
        }
        Ok(self.pause_command.clone())
    }

    pub(crate) fn with_network_manager(
        runtime: R,
        network_manager: N,
        port_mapper: Box<dyn PodPortMapper>,
        options: PodSandboxManagerOptions,
    ) -> Self {
        let PodSandboxManagerOptions {
            disable_hostport_mapping,
            root_dir,
            pause_image,
            pause_command,
            infra_ctr_cpuset,
        } = options;
        Self {
            runtime,
            network_manager,
            port_mapper,
            disable_hostport_mapping,
            root_dir,
            namespace_manager: NamespaceManager::new(PathBuf::from("/var/run/netns")),
            pause_image,
            pause_command,
            infra_ctr_cpuset,
            pods: HashMap::new(),
        }
    }

    fn host_port_mappings(
        &self,
        pod: &PodSandboxConfig,
        pod_ip: &str,
    ) -> Result<Vec<HostPortMapping>> {
        if pod_ip.trim().is_empty() || !Self::pod_requires_managed_netns(pod) {
            return Ok(Vec::new());
        }

        let container_ip: IpAddr = pod_ip
            .parse()
            .with_context(|| format!("invalid pod IP for port mappings: {pod_ip}"))?;

        pod.port_mappings
            .iter()
            .filter(|mapping| mapping.host_port > 0 && mapping.container_port > 0)
            .map(|mapping| {
                let protocol = match mapping.protocol.to_ascii_uppercase().as_str() {
                    "TCP" => Protocol::Tcp,
                    "UDP" => Protocol::Udp,
                    "SCTP" => Protocol::Sctp,
                    other => {
                        return Err(anyhow::anyhow!("unsupported hostPort protocol {}", other))
                    }
                };
                let host_ip = if mapping.host_ip.trim().is_empty() {
                    None
                } else {
                    Some(
                        mapping
                            .host_ip
                            .parse()
                            .with_context(|| format!("invalid host IP {}", mapping.host_ip))?,
                    )
                };
                Ok(HostPortMapping {
                    protocol,
                    container_port: u16::try_from(mapping.container_port)
                        .context("container_port out of range")?,
                    host_port: u16::try_from(mapping.host_port)
                        .context("host_port out of range")?,
                    host_ip,
                    container_ip,
                })
            })
            .collect()
    }

    fn apply_port_mappings(
        &self,
        pod: &PodSandboxConfig,
        pod_ip: &str,
    ) -> Result<Vec<HostPortMapping>> {
        if self.disable_hostport_mapping {
            debug!(
                "hostPort mapping is disabled; skipping add for pod {}",
                pod.name
            );
            return Ok(Vec::new());
        }
        let mappings = self.host_port_mappings(pod, pod_ip)?;
        let mut applied = Vec::new();
        for mapping in mappings {
            if let Err(err) = self.port_mapper.add_port_mapping(&mapping) {
                for existing in applied.iter().rev() {
                    let _ = self.port_mapper.remove_port_mapping(existing);
                }
                return Err(err)
                    .with_context(|| format!("failed to add hostPort mapping {:?}", mapping));
            }
            applied.push(mapping);
        }
        Ok(applied)
    }

    fn remove_port_mappings(&self, pod: &PodSandboxConfig, pod_ip: &str) {
        if self.disable_hostport_mapping {
            debug!(
                "hostPort mapping is disabled; skipping cleanup for pod {}",
                pod.name
            );
            return;
        }
        match self.host_port_mappings(pod, pod_ip) {
            Ok(mappings) => {
                for mapping in mappings.iter().rev() {
                    if let Err(err) = self.port_mapper.remove_port_mapping(mapping) {
                        debug!("Failed to remove hostPort mapping {:?}: {}", mapping, err);
                    }
                }
            }
            Err(err) => debug!("Failed to build hostPort mappings for cleanup: {}", err),
        }
    }

    pub(crate) fn rebuild_port_mappings(&self) {
        if self.disable_hostport_mapping {
            debug!("hostPort mapping is disabled; skipping rebuild");
            return;
        }
        if let Err(err) = self.port_mapper.cleanup_all() {
            debug!(
                "Failed to cleanup existing hostPort rules before rebuild: {}",
                err
            );
        }

        for pod in self.pods.values() {
            if let Err(err) = self.apply_port_mappings(&pod.config, &pod.ip) {
                debug!(
                    "Failed to rebuild hostPort mappings for pod {}: {}",
                    pod.id, err
                );
            }
        }
    }

    async fn rollback_create_pod_sandbox(
        &mut self,
        rollback: PodSandboxRollbackContext<'_>,
        config: &PodSandboxConfig,
    ) {
        let PodSandboxRollbackContext {
            pod_id,
            pod_dir,
            netns_name,
            netns_path,
            pause_container_id,
            network_attempted,
            netns_created,
            pod_ip,
        } = rollback;
        if let Some(pause_container_id) = pause_container_id {
            if let Err(err) = self.runtime.stop_container(pause_container_id, None) {
                debug!(
                    "Failed to stop pause container {} during pod rollback: {}",
                    pause_container_id, err
                );
            }
            if let Err(err) = self.runtime.remove_container(pause_container_id) {
                debug!(
                    "Failed to remove pause container {} during pod rollback: {}",
                    pause_container_id, err
                );
            }
        }

        if let Some(pod_ip) = pod_ip {
            self.remove_port_mappings(config, pod_ip);
        }

        if network_attempted {
            if let Err(err) = self
                .network_manager
                .teardown_pod_network(
                    pod_id,
                    &netns_path.to_string_lossy(),
                    &config.namespace,
                    &config.name,
                    &config.uid,
                    &config.runtime_handler,
                )
                .await
            {
                debug!("Failed to teardown pod network during rollback: {}", err);
            }
        }

        if netns_created {
            if let Err(err) = self
                .network_manager
                .remove_network_namespace(netns_name)
                .await
            {
                debug!(
                    "Failed to remove network namespace {} during rollback: {}",
                    netns_name, err
                );
            }
        }

        if pod_dir.exists() {
            if let Err(err) = tokio::fs::remove_dir_all(pod_dir).await {
                debug!(
                    "Failed to remove pod workspace {} during rollback: {}",
                    pod_dir.display(),
                    err
                );
            }
        }
    }

    /// 创建Pod沙箱
    pub async fn create_pod_sandbox(&mut self, config: PodSandboxConfig) -> Result<String> {
        let pod_id = uuid::Uuid::new_v4().to_simple().to_string();
        self.create_pod_sandbox_with_id(pod_id, config).await
    }

    pub async fn create_pod_sandbox_with_id(
        &mut self,
        pod_id: String,
        config: PodSandboxConfig,
    ) -> Result<String> {
        info!(
            "Creating pod sandbox {} (name: {}, namespace: {})",
            pod_id, config.name, config.namespace
        );

        // 1. 创建Pod目录
        let pod_dir = self.root_dir.join(&pod_id);
        tokio::fs::create_dir_all(&pod_dir).await?;
        let mut netns_created = false;
        let mut network_attempted = false;

        // 1.1 为 Pod 准备 resolv.conf，参考 CRI-O 的 pod 级 DNS 文件做法。
        if let Err(err) = self
            .create_resolv_conf(&pod_id, config.dns_config.as_ref())
            .await
            .context("Failed to create pod resolv.conf")
        {
            self.rollback_create_pod_sandbox(
                PodSandboxRollbackContext {
                    pod_id: &pod_id,
                    pod_dir: &pod_dir,
                    netns_name: "",
                    netns_path: Path::new(""),
                    pause_container_id: None,
                    network_attempted,
                    netns_created,
                    pod_ip: None,
                },
                &config,
            )
            .await;
            return Err(err);
        }

        // 2. 创建网络命名空间
        let managed_netns = Self::pod_requires_managed_netns(&config);
        let netns_name = format!("crius-{}-{}", config.namespace, config.name);
        let netns_path = if managed_netns {
            self.namespace_manager.resolve_path(&netns_name)
        } else {
            PathBuf::new()
        };

        let network_status = if managed_netns {
            debug!("Creating network namespace: {}", netns_name);
            if let Err(err) = self
                .network_manager
                .create_network_namespace(&netns_name)
                .await
                .context("Failed to create network namespace")
            {
                self.rollback_create_pod_sandbox(
                    PodSandboxRollbackContext {
                        pod_id: &pod_id,
                        pod_dir: &pod_dir,
                        netns_name: &netns_name,
                        netns_path: &netns_path,
                        pause_container_id: None,
                        network_attempted,
                        netns_created,
                        pod_ip: None,
                    },
                    &config,
                )
                .await;
                return Err(err);
            }
            netns_created = true;

            debug!("Setting up pod network for {}", pod_id);
            network_attempted = true;
            let mut network_status = match self
                .network_manager
                .setup_pod_network(NetworkSetupRequest {
                    pod_id: &pod_id,
                    netns: &netns_path.to_string_lossy(),
                    pod_name: &config.name,
                    pod_namespace: &config.namespace,
                    pod_uid: &config.uid,
                    runtime_handler: &config.runtime_handler,
                    pod_cidr: config
                        .network_config
                        .as_ref()
                        .map(|network| network.pod_cidr.as_str()),
                })
                .await
            {
                Ok(status) => status,
                Err(err) => {
                    self.rollback_create_pod_sandbox(
                        PodSandboxRollbackContext {
                            pod_id: &pod_id,
                            pod_dir: &pod_dir,
                            netns_name: &netns_name,
                            netns_path: &netns_path,
                            pause_container_id: None,
                            network_attempted,
                            netns_created,
                            pod_ip: None,
                        },
                        &config,
                    )
                    .await;
                    return Err(err.into());
                }
            };
            let discovered_interfaces = self.discover_netns_interfaces(&netns_path).await;
            if network_status.ip.is_none() {
                network_status.ip = discovered_interfaces
                    .iter()
                    .find_map(|iface| iface.ip.as_ref().copied());
            }
            if !discovered_interfaces.is_empty() {
                network_status.interfaces = discovered_interfaces;
            }
            Some(network_status)
        } else {
            None
        };

        // 4. 创建pause容器
        debug!("Creating pause container for pod {}", pod_id);
        let created_pause_container_id = match self
            .create_pause_container(&pod_id, &config, &netns_path)
            .await
        {
            Ok(container_id) => container_id,
            Err(err) => {
                self.rollback_create_pod_sandbox(
                    PodSandboxRollbackContext {
                        pod_id: &pod_id,
                        pod_dir: &pod_dir,
                        netns_name: &netns_name,
                        netns_path: &netns_path,
                        pause_container_id: None,
                        network_attempted,
                        netns_created,
                        pod_ip: network_status
                            .as_ref()
                            .and_then(|status| status.ip.as_ref())
                            .map(|ip| ip.to_string())
                            .as_deref(),
                    },
                    &config,
                )
                .await;
                let has_specialized_pause_error = err
                    .chain()
                    .any(|cause| cause.downcast_ref::<PodSandboxError>().is_some());
                return if has_specialized_pause_error {
                    Err(err)
                } else {
                    Err(err).context("Failed to create pause container")
                };
            }
        };

        let pod_ip = network_status
            .as_ref()
            .and_then(|status| status.ip.as_ref())
            .map(|ip| ip.to_string())
            .unwrap_or_default();
        if let Err(err) = self.apply_port_mappings(&config, &pod_ip) {
            self.rollback_create_pod_sandbox(
                PodSandboxRollbackContext {
                    pod_id: &pod_id,
                    pod_dir: &pod_dir,
                    netns_name: &netns_name,
                    netns_path: &netns_path,
                    pause_container_id: Some(&created_pause_container_id),
                    network_attempted,
                    netns_created,
                    pod_ip: Some(&pod_ip),
                },
                &config,
            )
            .await;
            return Err(err).context("Failed to apply pod port mappings");
        }

        // 5. 创建Pod沙箱对象
        let pod = PodSandbox {
            id: pod_id.clone(),
            config: config.clone(),
            netns_path: netns_path.clone(),
            pause_container_id: created_pause_container_id.clone(),
            state: PodSandboxState::Ready,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as i64,
            ip: network_status
                .as_ref()
                .and_then(|status| status.ip.as_ref())
                .map(|ip| ip.to_string())
                .unwrap_or_default(),
            network_status,
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
        let pause_command = self.resolve_pause_command()?;
        let mut pause_mounts = Vec::new();
        let pause_name = format!("pause-{}", pod_id);
        let mut pause_annotations = pod_config.annotations.clone();
        pause_annotations.push((CRIO_CONTAINER_ID_ANNOTATION.to_string(), pod_id.to_string()));
        pause_annotations.push((
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            pause_name.clone(),
        ));
        pause_annotations.push((
            CRIO_CONTAINER_TYPE_ANNOTATION.to_string(),
            CONTAINER_TYPE_SANDBOX.to_string(),
        ));
        pause_annotations.push((CRIO_SANDBOX_ID_ANNOTATION.to_string(), pod_id.to_string()));
        pause_annotations.push((
            CRIO_SANDBOX_NAME_ANNOTATION.to_string(),
            pod_config.name.clone(),
        ));
        pause_annotations.push((
            CRIO_POD_NAME_ANNOTATION.to_string(),
            pod_config.name.clone(),
        ));
        pause_annotations.push((
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            pod_config.namespace.clone(),
        ));
        pause_annotations.push((
            CRIO_RUNTIME_HANDLER_ANNOTATION.to_string(),
            pod_config.runtime_handler.clone(),
        ));
        pause_annotations.push((
            CONTAINERD_SANDBOX_ID_ANNOTATION.to_string(),
            pod_id.to_string(),
        ));
        pause_annotations.push((
            CONTAINERD_CONTAINER_TYPE_ANNOTATION.to_string(),
            CONTAINER_TYPE_SANDBOX.to_string(),
        ));
        pause_annotations.push((
            CONTAINERD_SANDBOX_NAME_ANNOTATION.to_string(),
            pod_config.name.clone(),
        ));
        pause_annotations.push((
            CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION.to_string(),
            pod_config.namespace.clone(),
        ));
        pause_annotations.push((
            CONTAINERD_SANDBOX_UID_ANNOTATION.to_string(),
            pod_config.uid.clone(),
        ));
        pause_annotations.push((
            CONTAINERD_RUNTIME_HANDLER_ANNOTATION.to_string(),
            pod_config.runtime_handler.clone(),
        ));
        if let Some(log_directory) = pod_config.log_directory.as_ref() {
            pause_annotations.push((
                CONTAINERD_SANDBOX_LOG_DIR_ANNOTATION.to_string(),
                log_directory.to_string_lossy().to_string(),
            ));
        }
        let resolv_path = self.pod_resolv_path(pod_id);
        if resolv_path.exists() {
            pause_mounts.push(crate::runtime::MountConfig {
                source: resolv_path,
                destination: PathBuf::from("/etc/resolv.conf"),
                read_only: true,
                missing_source_policy: crate::runtime::MissingMountSourcePolicy::Ignore,
                selinux_relabel: false,
                propagation: crate::runtime::MountPropagationMode::Private,
                recursive_read_only: false,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                requested_image: None,
                image_sub_path: None,
            });
        }
        let pause_log_path = pod_config
            .log_directory
            .as_ref()
            .map(|dir| dir.join("sandbox.log"));
        if let Some(log_path) = pause_log_path.as_ref() {
            pause_annotations.push((
                CRIO_LOG_PATH_ANNOTATION.to_string(),
                log_path.to_string_lossy().to_string(),
            ));
        }

        // Pause容器配置
        let pause_config = ContainerConfig {
            name: pause_name.clone(),
            image: pause_image,
            command: vec![pause_command],
            args: vec![],
            env: vec![],
            working_dir: None,
            mounts: pause_mounts,
            labels: pod_config.labels.clone(),
            annotations: pause_annotations,
            privileged: pod_config.privileged,
            user: pod_config.run_as_user.clone(),
            run_as_group: pod_config.run_as_group,
            supplemental_groups: pod_config.supplemental_groups.clone(),
            hostname: Some(pod_config.hostname.clone()),
            tty: false,
            stdin: false,
            stdin_once: false,
            log_path: pause_log_path,
            readonly_rootfs: pod_config.readonly_rootfs,
            seccomp_notifier: None,
            pids_limit: pod_config.pids_limit,
            no_new_privileges: pod_config.no_new_privileges,
            apparmor_profile: pod_config.apparmor_profile.clone(),
            selinux_label: pod_config.selinux_label.clone(),
            seccomp_profile: pod_config.seccomp_profile.clone(),
            capabilities: None,
            cgroup_parent: pod_config.cgroup_parent.clone(),
            sysctls: pod_config.sysctls.clone(),
            namespace_options: pod_config.namespace_options.clone(),
            namespace_paths: NamespacePaths {
                network: (!netns_path.as_os_str().is_empty()).then(|| netns_path.to_path_buf()),
                ..Default::default()
            },
            linux_resources: {
                let mut resources = pod_config.linux_resources.clone().unwrap_or_default();
                if !self.infra_ctr_cpuset.trim().is_empty()
                    && resources.cpuset_cpus.trim().is_empty()
                {
                    resources.cpuset_cpus = self.infra_ctr_cpuset.trim().to_string();
                }
                Some(resources)
            },
            devices: vec![],
            masked_paths: Vec::new(),
            readonly_paths: Vec::new(),
            // Pause容器使用自己的rootfs，实际应用中需要从镜像创建
            rootfs: self.root_dir.join(pod_id).join("pause-rootfs"),
        };

        // 创建 pause 容器，ID 由上层（pod 管理器）统一分配，避免 runtime 二次生成。
        let container_id = pause_name;
        if let Err(err) = self.runtime.create_container(&container_id, &pause_config) {
            if let Some(classified) = Self::classify_pause_image_error(&err, &pause_config.image) {
                return Err(classified.into());
            }
            return Err(err).context("Failed to create pause container");
        }

        // 启动pause容器
        if let Err(err) = self.runtime.start_container(&container_id) {
            let _ = self.runtime.remove_container(&container_id);
            return Err(err).context("Failed to start pause container");
        }

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
            if pod.state == PodSandboxState::Terminated {
                return Ok(());
            }
            // 1. 停止pause容器
            debug!("Stopping pause container {}", pod.pause_container_id);
            let _ = self.runtime.stop_container(&pod.pause_container_id, None);
            let _ = self.runtime.remove_container(&pod.pause_container_id);

            self.remove_port_mappings(&pod.config, &pod.ip);

            // 2. 清理网络
            debug!("Tearing down pod network for {}", pod_id);
            if Self::pod_requires_managed_netns(&pod.config) {
                let _ = self
                    .network_manager
                    .teardown_pod_network(
                        pod_id,
                        &pod.netns_path.to_string_lossy(),
                        &pod.config.namespace,
                        &pod.config.name,
                        &pod.config.uid,
                        &pod.config.runtime_handler,
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
        self.pods
            .get(pod_id)
            .map(|p| p.netns_path.clone())
            .filter(|path| !path.as_os_str().is_empty())
    }

    /// 获取Pod沙箱信息
    pub fn get_pod_sandbox(&self, pod_id: &str) -> Option<&PodSandbox> {
        self.pods.get(pod_id)
    }

    /// 获取Pod沙箱信息副本
    pub fn get_pod_sandbox_cloned(&self, pod_id: &str) -> Option<PodSandbox> {
        self.pods.get(pod_id).cloned()
    }

    /// 更新Pod沙箱资源
    pub async fn update_pod_sandbox_resources(
        &mut self,
        pod_id: &str,
        resources: Option<LinuxContainerResources>,
    ) -> Result<()> {
        let Some(pod) = self.pods.get_mut(pod_id) else {
            return Err(anyhow::anyhow!("Pod sandbox {} not found", pod_id));
        };

        pod.config.linux_resources = resources;
        Ok(())
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
mod tests;
