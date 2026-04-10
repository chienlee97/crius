use log;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Command as TokioCommand;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::proto::runtime::v1::{
    runtime_service_server::RuntimeService, Container, ContainerState,
    ContainerStatus as CriContainerStatus, ExecRequest, ExecResponse, ExecSyncRequest,
    ExecSyncResponse, PortForwardRequest, PortForwardResponse, RunPodSandboxRequest,
    RunPodSandboxResponse, StatusRequest, StatusResponse, StopPodSandboxRequest,
    StopPodSandboxResponse, VersionRequest, VersionResponse,
};
use crate::proto::runtime::v1::{
    AttachRequest, AttachResponse, CheckpointContainerRequest, CheckpointContainerResponse,
    ContainerEventResponse, ContainerMetadata, ContainerResources, ContainerStatsRequest,
    ContainerStatsResponse, ContainerStatusRequest, ContainerStatusResponse,
    CreateContainerRequest, CreateContainerResponse, GetEventsRequest, ImageSpec,
    LinuxPodSandboxStatus, ListContainerStatsRequest, ListContainerStatsResponse,
    ListContainersRequest, ListContainersResponse, ListMetricDescriptorsRequest,
    ListMetricDescriptorsResponse, ListPodSandboxMetricsRequest, ListPodSandboxMetricsResponse,
    ListPodSandboxRequest, ListPodSandboxResponse, ListPodSandboxStatsRequest,
    ListPodSandboxStatsResponse, Namespace, NamespaceMode, NamespaceOption, PodIp,
    PodSandboxAttributes, PodSandboxMetadata, PodSandboxNetworkStatus, PodSandboxState, PodSandboxStats, PodSandboxStatsRequest,
    PodSandboxStatsResponse, PodSandboxStatus, PodSandboxStatusRequest, PodSandboxStatusResponse,
    RemoveContainerRequest, RemoveContainerResponse, RemovePodSandboxRequest,
    RemovePodSandboxResponse, ReopenContainerLogRequest, ReopenContainerLogResponse,
    RuntimeCondition, RuntimeConfigRequest, RuntimeConfigResponse, RuntimeStatus,
    StartContainerRequest, StartContainerResponse, StopContainerRequest, StopContainerResponse,
    UpdateContainerResourcesRequest, UpdateContainerResourcesResponse, UpdateRuntimeConfigRequest,
    UpdateRuntimeConfigResponse,
};
use crate::storage::persistence::{PersistenceConfig, PersistenceManager};

use crate::network::{DefaultNetworkManager, NetworkManager};
use crate::pod::{PodSandboxConfig, PodSandboxManager};
use crate::runtime::{
    ContainerConfig, ContainerRuntime, ContainerStatus, DeviceMapping, MountConfig, NamespacePaths,
    RuncRuntime, SeccompProfile, ShimConfig,
};
use crate::streaming::StreamingServer;
use crate::metrics::MetricsCollector;

/// 运行时服务实现
#[derive(Debug)]
pub struct RuntimeServiceImpl {
    // 存储容器状态的线程安全HashMap
    containers: Arc<Mutex<HashMap<String, Container>>>,
    // 存储Pod沙箱状态的线程安全HashMap
    pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    // 运行时配置
    config: RuntimeConfig,
    // 容器运行时
    runtime: RuncRuntime,
    // Pod沙箱管理器
    pod_manager: tokio::sync::Mutex<PodSandboxManager<RuncRuntime>>,
    // 持久化管理器
    persistence: Arc<Mutex<PersistenceManager>>,
    // 流式服务
    streaming: Arc<Mutex<Option<StreamingServer>>>,
}

/// 运行时配置
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: PathBuf,
    pub runtime: String,
    pub runtime_handlers: Vec<String>,
    pub runtime_root: PathBuf,
    pub log_dir: PathBuf,
    pub runtime_path: PathBuf,
    pub pause_image: String,
}

const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";
const INTERNAL_POD_STATE_KEY: &str = "io.crius.internal/pod-state";
const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredNamespaceOptions {
    network: i32,
    pid: i32,
    ipc: i32,
    target_id: String,
}

impl StoredNamespaceOptions {
    fn to_proto(&self) -> NamespaceOption {
        NamespaceOption {
            network: self.network,
            pid: self.pid,
            ipc: self.ipc,
            target_id: self.target_id.clone(),
            userns_options: None,
        }
    }
}

impl From<&NamespaceOption> for StoredNamespaceOptions {
    fn from(value: &NamespaceOption) -> Self {
        Self {
            network: value.network,
            pid: value.pid,
            ipc: value.ipc,
            target_id: value.target_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredHugepageLimit {
    page_size: String,
    limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredLinuxResources {
    cpu_period: i64,
    cpu_quota: i64,
    cpu_shares: i64,
    memory_limit_in_bytes: i64,
    oom_score_adj: i64,
    cpuset_cpus: String,
    cpuset_mems: String,
    hugepage_limits: Vec<StoredHugepageLimit>,
    unified: HashMap<String, String>,
    memory_swap_limit_in_bytes: i64,
}

impl StoredLinuxResources {
    fn to_proto(&self) -> crate::proto::runtime::v1::LinuxContainerResources {
        crate::proto::runtime::v1::LinuxContainerResources {
            cpu_period: self.cpu_period,
            cpu_quota: self.cpu_quota,
            cpu_shares: self.cpu_shares,
            memory_limit_in_bytes: self.memory_limit_in_bytes,
            oom_score_adj: self.oom_score_adj,
            cpuset_cpus: self.cpuset_cpus.clone(),
            cpuset_mems: self.cpuset_mems.clone(),
            hugepage_limits: self
                .hugepage_limits
                .iter()
                .map(|limit| crate::proto::runtime::v1::HugepageLimit {
                    page_size: limit.page_size.clone(),
                    limit: limit.limit,
                })
                .collect(),
            unified: self.unified.clone(),
            memory_swap_limit_in_bytes: self.memory_swap_limit_in_bytes,
        }
    }
}

impl From<&crate::proto::runtime::v1::LinuxContainerResources> for StoredLinuxResources {
    fn from(value: &crate::proto::runtime::v1::LinuxContainerResources) -> Self {
        Self {
            cpu_period: value.cpu_period,
            cpu_quota: value.cpu_quota,
            cpu_shares: value.cpu_shares,
            memory_limit_in_bytes: value.memory_limit_in_bytes,
            oom_score_adj: value.oom_score_adj,
            cpuset_cpus: value.cpuset_cpus.clone(),
            cpuset_mems: value.cpuset_mems.clone(),
            hugepage_limits: value
                .hugepage_limits
                .iter()
                .map(|limit| StoredHugepageLimit {
                    page_size: limit.page_size.clone(),
                    limit: limit.limit,
                })
                .collect(),
            unified: value.unified.clone(),
            memory_swap_limit_in_bytes: value.memory_swap_limit_in_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredSecurityProfile {
    profile_type: i32,
    localhost_ref: String,
}

impl StoredSecurityProfile {
    fn to_runtime_seccomp(&self) -> Option<SeccompProfile> {
        match self.profile_type {
            x if x
                == crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault
                    as i32 =>
            {
                Some(SeccompProfile::RuntimeDefault)
            }
            x if x
                == crate::proto::runtime::v1::security_profile::ProfileType::Unconfined as i32 =>
            {
                Some(SeccompProfile::Unconfined)
            }
            x if x
                == crate::proto::runtime::v1::security_profile::ProfileType::Localhost as i32 =>
            {
                if self.localhost_ref.is_empty() {
                    None
                } else {
                    Some(SeccompProfile::Localhost(PathBuf::from(
                        self.localhost_ref.clone(),
                    )))
                }
            }
            _ => None,
        }
    }
}

impl From<&crate::proto::runtime::v1::SecurityProfile> for StoredSecurityProfile {
    fn from(value: &crate::proto::runtime::v1::SecurityProfile) -> Self {
        Self {
            profile_type: value.profile_type,
            localhost_ref: value.localhost_ref.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredPodState {
    log_directory: Option<String>,
    runtime_handler: String,
    netns_path: Option<String>,
    pause_container_id: Option<String>,
    ip: Option<String>,
    additional_ips: Vec<String>,
    cgroup_parent: Option<String>,
    sysctls: HashMap<String, String>,
    namespace_options: Option<StoredNamespaceOptions>,
    privileged: bool,
    run_as_user: Option<String>,
    run_as_group: Option<u32>,
    supplemental_groups: Vec<u32>,
    readonly_rootfs: bool,
    no_new_privileges: Option<bool>,
    apparmor_profile: Option<String>,
    selinux_label: Option<String>,
    seccomp_profile: Option<StoredSecurityProfile>,
    linux_resources: Option<StoredLinuxResources>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredContainerState {
    log_path: Option<String>,
    tty: bool,
    stdin: bool,
    stdin_once: bool,
    readonly_rootfs: bool,
    cgroup_parent: Option<String>,
    network_namespace_path: Option<String>,
    linux_resources: Option<StoredLinuxResources>,
    run_as_group: Option<u32>,
    supplemental_groups: Vec<u32>,
    no_new_privileges: Option<bool>,
    apparmor_profile: Option<String>,
    metadata_name: Option<String>,
    metadata_attempt: Option<u32>,
    started_at: Option<i64>,
    finished_at: Option<i64>,
    exit_code: Option<i32>,
}

impl RuntimeServiceImpl {
    fn normalize_timestamp_nanos(ts: i64) -> i64 {
        // Backward-compatible normalization: old records may still be seconds.
        if ts > 0 && ts < 1_000_000_000_000 {
            ts.saturating_mul(1_000_000_000)
        } else {
            ts
        }
    }

    fn is_internal_annotation_key(key: &str) -> bool {
        key.starts_with(INTERNAL_ANNOTATION_PREFIX)
    }

    fn now_nanos() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    fn external_annotations(annotations: &HashMap<String, String>) -> HashMap<String, String> {
        annotations
            .iter()
            .filter(|(key, _)| !Self::is_internal_annotation_key(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn insert_internal_state<T: Serialize>(
        annotations: &mut HashMap<String, String>,
        key: &str,
        state: &T,
    ) -> Result<(), Status> {
        let encoded = serde_json::to_string(state)
            .map_err(|e| Status::internal(format!("Failed to encode internal state: {}", e)))?;
        annotations.insert(key.to_string(), encoded);
        Ok(())
    }

    fn read_internal_state<T: for<'de> Deserialize<'de>>(
        annotations: &HashMap<String, String>,
        key: &str,
    ) -> Option<T> {
        annotations
            .get(key)
            .and_then(|value| serde_json::from_str(value).ok())
    }

    fn security_profile_name(
        profile: Option<&crate::proto::runtime::v1::SecurityProfile>,
        deprecated_profile: &str,
    ) -> Option<String> {
        if let Some(profile) = profile {
            match profile.profile_type {
                x if x
                    == crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                        as i32 =>
                {
                    if profile.localhost_ref.is_empty() {
                        None
                    } else {
                        Some(profile.localhost_ref.clone())
                    }
                }
                _ => None,
            }
        } else if deprecated_profile.is_empty() {
            None
        } else {
            Some(deprecated_profile.to_string())
        }
    }

    fn selinux_label_from_proto(
        options: Option<&crate::proto::runtime::v1::SeLinuxOption>,
    ) -> Option<String> {
        let options = options?;
        if options.user.is_empty()
            && options.role.is_empty()
            && options.r#type.is_empty()
            && options.level.is_empty()
        {
            return None;
        }

        let user = if options.user.is_empty() {
            "system_u"
        } else {
            options.user.as_str()
        };
        let role = if options.role.is_empty() {
            "system_r"
        } else {
            options.role.as_str()
        };
        let selinux_type = if options.r#type.is_empty() {
            "container_t"
        } else {
            options.r#type.as_str()
        };
        let level = if options.level.is_empty() {
            "s0"
        } else {
            options.level.as_str()
        };
        Some(format!("{}:{}:{}:{}", user, role, selinux_type, level))
    }

    fn seccomp_profile_from_proto(
        profile: Option<&crate::proto::runtime::v1::SecurityProfile>,
        deprecated_profile: &str,
    ) -> Option<SeccompProfile> {
        if let Some(profile) = profile {
            return match profile.profile_type {
                x if x
                    == crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault
                        as i32 =>
                {
                    Some(SeccompProfile::RuntimeDefault)
                }
                x if x
                    == crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                        as i32 =>
                {
                    Some(SeccompProfile::Unconfined)
                }
                x if x
                    == crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                        as i32 =>
                {
                    if profile.localhost_ref.is_empty() {
                        None
                    } else {
                        Some(SeccompProfile::Localhost(PathBuf::from(
                            profile.localhost_ref.clone(),
                        )))
                    }
                }
                _ => None,
            };
        }

        if deprecated_profile.is_empty() {
            None
        } else {
            Some(SeccompProfile::Localhost(PathBuf::from(
                deprecated_profile.to_string(),
            )))
        }
    }

    fn stored_seccomp_profile_from_proto(
        profile: Option<&crate::proto::runtime::v1::SecurityProfile>,
        deprecated_profile: &str,
    ) -> Option<StoredSecurityProfile> {
        if let Some(profile) = profile {
            return Some(StoredSecurityProfile::from(profile));
        }
        if deprecated_profile.is_empty() {
            None
        } else {
            Some(StoredSecurityProfile {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                    as i32,
                localhost_ref: deprecated_profile.to_string(),
            })
        }
    }

    fn resolve_runtime_handler(&self, requested: &str) -> Result<String, Status> {
        if requested.is_empty() {
            return Ok(self.config.runtime.clone());
        }
        if self
            .config
            .runtime_handlers
            .iter()
            .any(|handler| handler == requested)
        {
            return Ok(requested.to_string());
        }
        Err(Status::invalid_argument(format!(
            "unsupported runtime handler: {}",
            requested
        )))
    }

    fn pod_network_status_from_state(
        state: Option<&StoredPodState>,
    ) -> Option<PodSandboxNetworkStatus> {
        let primary_ip = state.and_then(|pod| pod.ip.clone()).unwrap_or_default();
        let mut seen = HashSet::new();
        let mut additional: Vec<PodIp> = state
            .map(|pod| {
                pod.additional_ips
                    .iter()
                    .filter(|ip| !ip.is_empty())
                    .filter(|ip| primary_ip.is_empty() || *ip != &primary_ip)
                    .filter(|ip| seen.insert((*ip).clone()))
                    .map(|ip| PodIp { ip: ip.clone() })
                    .collect()
            })
            .unwrap_or_default();

        if primary_ip.is_empty() {
            if additional.is_empty() {
                return None;
            }

            let primary = additional.remove(0);
            Some(PodSandboxNetworkStatus {
                ip: primary.ip,
                additional_ips: additional,
            })
        } else {
            Some(PodSandboxNetworkStatus {
                ip: primary_ip,
                additional_ips: additional,
            })
        }
    }

    fn pod_linux_status_from_state(
        state: Option<&StoredPodState>,
    ) -> Option<LinuxPodSandboxStatus> {
        let options = state
            .and_then(|pod| pod.namespace_options.as_ref())
            .map(StoredNamespaceOptions::to_proto)
            .unwrap_or_else(|| NamespaceOption {
                network: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
                pid: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
                ipc: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            });

        Some(LinuxPodSandboxStatus {
            namespaces: Some(Namespace {
                options: Some(options),
            }),
        })
    }

    fn build_container_status_snapshot(
        container: &Container,
        runtime_state: i32,
    ) -> CriContainerStatus {
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let started_at = container_state
            .as_ref()
            .and_then(|state| state.started_at)
            .unwrap_or_default();
        let finished_at = container_state
            .as_ref()
            .and_then(|state| state.finished_at)
            .unwrap_or_default();
        let exit_code = container_state
            .as_ref()
            .and_then(|state| state.exit_code)
            .unwrap_or_default();

        CriContainerStatus {
            id: container.id.clone(),
            metadata: Some(container.metadata.clone().unwrap_or(ContainerMetadata {
                name: container.id.clone(),
                attempt: 1,
            })),
            state: runtime_state,
            created_at: Self::normalize_timestamp_nanos(container.created_at),
            started_at: Self::normalize_timestamp_nanos(started_at),
            finished_at: Self::normalize_timestamp_nanos(finished_at),
            exit_code,
            image: Some(container.image.clone().unwrap_or(ImageSpec {
                image: container.image_ref.clone(),
                ..Default::default()
            })),
            image_ref: container.image_ref.clone(),
            labels: container.labels.clone(),
            annotations: Self::external_annotations(&container.annotations),
            log_path: container_state
                .as_ref()
                .and_then(|state| state.log_path.clone())
                .unwrap_or_default(),
            resources: container_state.and_then(|state| {
                state.linux_resources.map(|linux| ContainerResources {
                    linux: Some(linux.to_proto()),
                    windows: None,
                })
            }),
            ..Default::default()
        }
    }

    fn map_runtime_container_state(status: crate::runtime::ContainerStatus) -> i32 {
        match status {
            ContainerStatus::Created => ContainerState::ContainerCreated as i32,
            ContainerStatus::Running => ContainerState::ContainerRunning as i32,
            ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
            ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
        }
    }

    async fn runtime_namespace_path_for_container(
        &self,
        runtime_container_id: &str,
        namespace: &str,
    ) -> Result<Option<PathBuf>, Status> {
        if runtime_container_id.is_empty() {
            return Ok(None);
        }

        let runtime = self.runtime.clone();
        let container_id = runtime_container_id.to_string();
        let pid = tokio::task::spawn_blocking(move || runtime.container_pid(&container_id))
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to query container PID for {}: {}",
                    runtime_container_id, e
                ))
            })?;

        Ok(pid.map(|pid| PathBuf::from(format!("/proc/{}/ns/{}", pid, namespace))))
    }

    async fn runtime_namespace_path_for_target(
        &self,
        requested_container_id: &str,
        namespace: &str,
    ) -> Result<Option<PathBuf>, Status> {
        if requested_container_id.is_empty() {
            return Ok(None);
        }

        let resolved_id = self.resolve_container_id(requested_container_id).await?;
        self.runtime_namespace_path_for_container(&resolved_id, namespace)
            .await
    }

    async fn resolve_pod_sandbox_id(&self, requested_id: &str) -> Result<String, Status> {
        let pod_sandboxes = self.pod_sandboxes.lock().await;
        if pod_sandboxes.contains_key(requested_id) {
            return Ok(requested_id.to_string());
        }

        let matches: Vec<String> = pod_sandboxes
            .keys()
            .filter(|id| id.starts_with(requested_id))
            .cloned()
            .collect();

        match matches.len() {
            0 => Err(Status::not_found("Pod sandbox not found")),
            1 => Ok(matches[0].clone()),
            _ => Err(Status::invalid_argument(format!(
                "ambiguous pod sandbox id prefix: {}",
                requested_id
            ))),
        }
    }

    async fn resolve_container_id(&self, requested_id: &str) -> Result<String, Status> {
        let containers = self.containers.lock().await;
        if containers.contains_key(requested_id) {
            return Ok(requested_id.to_string());
        }

        let matches: Vec<String> = containers
            .keys()
            .filter(|id| id.starts_with(requested_id))
            .cloned()
            .collect();

        match matches.len() {
            0 => Err(Status::not_found("Container not found")),
            1 => Ok(matches[0].clone()),
            _ => Err(Status::invalid_argument(format!(
                "ambiguous container id prefix: {}",
                requested_id
            ))),
        }
    }

    fn container_matches_filter(
        container: &Container,
        filter: &crate::proto::runtime::v1::ContainerFilter,
    ) -> bool {
        if !filter.id.is_empty()
            && !(container.id == filter.id
                || container.id.starts_with(&filter.id)
                || filter.id.starts_with(&container.id))
        {
            return false;
        }

        if let Some(state) = &filter.state {
            if container.state != state.state {
                return false;
            }
        }

        if !filter.pod_sandbox_id.is_empty()
            && !(container.pod_sandbox_id == filter.pod_sandbox_id
                || container.pod_sandbox_id.starts_with(&filter.pod_sandbox_id)
                || filter.pod_sandbox_id.starts_with(&container.pod_sandbox_id))
        {
            return false;
        }

        for (k, v) in &filter.label_selector {
            if container.labels.get(k) != Some(v) {
                return false;
            }
        }

        true
    }

    fn pod_sandbox_matches_filter(
        pod: &crate::proto::runtime::v1::PodSandbox,
        filter: &crate::proto::runtime::v1::PodSandboxFilter,
    ) -> bool {
        if !filter.id.is_empty()
            && !(pod.id == filter.id
                || pod.id.starts_with(&filter.id)
                || filter.id.starts_with(&pod.id))
        {
            return false;
        }

        if let Some(state) = &filter.state {
            if pod.state != state.state {
                return false;
            }
        }

        for (k, v) in &filter.label_selector {
            if pod.labels.get(k) != Some(v) {
                return false;
            }
        }

        true
    }

    pub fn new(config: RuntimeConfig) -> Self {
        let mut config = config;
        let mut handlers = Vec::new();
        for handler in &config.runtime_handlers {
            let trimmed = handler.trim();
            if !trimmed.is_empty() && !handlers.iter().any(|existing: &String| existing == trimmed)
            {
                handlers.push(trimmed.to_string());
            }
        }
        if !handlers.iter().any(|handler| handler == &config.runtime) {
            handlers.push(config.runtime.clone());
        }
        config.runtime_handlers = handlers;

        let mut shim_config = ShimConfig::default();
        shim_config.runtime_path = config.runtime_path.clone();
        shim_config.debug = std::env::var("CRIUS_SHIM_DEBUG")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        shim_config.shim_path = std::env::var("CRIUS_SHIM_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let local = PathBuf::from("/root/crius/target/debug/crius-shim");
                if local.exists() {
                    local
                } else {
                    PathBuf::from("crius-shim")
                }
            });

        let runtime = RuncRuntime::with_shim(
            config.runtime_path.clone(),
            config.runtime_root.clone(),
            shim_config,
        );

        let pod_manager = PodSandboxManager::new(
            runtime.clone(),
            config.root_dir.join("pods"),
            config.pause_image.clone(),
        );

        // 初始化持久化管理器
        let persistence_config = PersistenceConfig {
            db_path: config.root_dir.join("crius.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        };

        let persistence = PersistenceManager::new(persistence_config)
            .expect("Failed to create persistence manager");

        Self {
            containers: Arc::new(Mutex::new(HashMap::new())),
            pod_sandboxes: Arc::new(Mutex::new(HashMap::new())),
            config,
            runtime,
            pod_manager: tokio::sync::Mutex::new(pod_manager),
            persistence: Arc::new(Mutex::new(persistence)),
            streaming: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn set_streaming_server(&self, streaming_server: StreamingServer) {
        let mut streaming = self.streaming.lock().await;
        *streaming = Some(streaming_server);
    }

    async fn get_streaming_server(&self) -> Result<StreamingServer, Status> {
        let streaming = self.streaming.lock().await;
        streaming
            .clone()
            .ok_or_else(|| Status::unavailable("streaming server is not initialized"))
    }

    /// 从持久化存储恢复状态
    pub async fn recover_state(&self) -> Result<(), Status> {
        let mut persistence = self.persistence.lock().await;

        // 恢复容器状态
        match persistence.recover_containers() {
            Ok(containers) => {
                let mut memory_containers = self.containers.lock().await;
                for (_id, status, record) in containers {
                    let annotations: HashMap<String, String> =
                        serde_json::from_str(&record.annotations).unwrap_or_default();
                    let container_state = Self::read_internal_state::<StoredContainerState>(
                        &annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    );
                    let container_name = container_state
                        .as_ref()
                        .and_then(|state| state.metadata_name.clone())
                        .unwrap_or_else(|| {
                            record
                                .command
                                .split_whitespace()
                                .next()
                                .unwrap_or("unknown")
                                .to_string()
                        });

                    let container = crate::proto::runtime::v1::Container {
                        id: record.id.clone(),
                        metadata: Some(crate::proto::runtime::v1::ContainerMetadata {
                            name: container_name.clone(),
                            attempt: container_state
                                .as_ref()
                                .and_then(|state| state.metadata_attempt)
                                .unwrap_or(1),
                        }),
                        state: match status {
                            crate::runtime::ContainerStatus::Created => {
                                ContainerState::ContainerCreated as i32
                            }
                            crate::runtime::ContainerStatus::Running => {
                                ContainerState::ContainerRunning as i32
                            }
                            crate::runtime::ContainerStatus::Stopped(_) => {
                                ContainerState::ContainerExited as i32
                            }
                            crate::runtime::ContainerStatus::Unknown => {
                                ContainerState::ContainerUnknown as i32
                            }
                        },
                        image: Some(ImageSpec {
                            image: record.image.clone(),
                            ..Default::default()
                        }),
                        image_ref: record.image,
                        labels: serde_json::from_str(&record.labels).unwrap_or_default(),
                        annotations,
                        created_at: record.created_at,
                        ..Default::default()
                    };
                    let mut container = container;
                    if let Some(mut state) = container_state.clone() {
                        if state.finished_at.is_none() {
                            state.finished_at = record.exit_time;
                        }
                        if state.exit_code.is_none() {
                            state.exit_code = record.exit_code;
                        }
                        let mut annotations = container.annotations.clone();
                        if Self::insert_internal_state(
                            &mut annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        )
                        .is_ok()
                        {
                            container.annotations = annotations;
                        }
                    }
                    log::info!(
                        "Recovered container: {} with name {}",
                        record.id,
                        container_name
                    );
                    memory_containers.insert(record.id, container);
                }
                log::info!(
                    "Recovered {} containers from database",
                    memory_containers.len()
                );
            }
            Err(e) => {
                log::error!("Failed to recover containers: {}", e);
            }
        }

        // 恢复Pod沙箱状态
        match persistence.recover_pods() {
            Ok(pods) => {
                let mut memory_pods = self.pod_sandboxes.lock().await;
                let mut pod_manager = self.pod_manager.lock().await;
                for record in pods {
                    let annotations: HashMap<String, String> =
                        serde_json::from_str(&record.annotations).unwrap_or_default();
                    let pod_state = Self::read_internal_state::<StoredPodState>(
                        &annotations,
                        INTERNAL_POD_STATE_KEY,
                    )
                    .unwrap_or_default();
                    let recovered_runtime_handler = if pod_state.runtime_handler.is_empty()
                        || !self
                            .config
                            .runtime_handlers
                            .iter()
                            .any(|handler| handler == &pod_state.runtime_handler)
                    {
                        self.config.runtime.clone()
                    } else {
                        pod_state.runtime_handler.clone()
                    };
                    let labels: HashMap<String, String> =
                        serde_json::from_str(&record.labels).unwrap_or_default();
                    let pod = crate::proto::runtime::v1::PodSandbox {
                        id: record.id.clone(),
                        metadata: Some(PodSandboxMetadata {
                            name: record.name.clone(),
                            uid: record.uid.clone(),
                            namespace: record.namespace.clone(),
                            attempt: 1,
                        }),
                        state: match record.state.as_str() {
                            "ready" => PodSandboxState::SandboxReady as i32,
                            "notready" => PodSandboxState::SandboxNotready as i32,
                            _ => PodSandboxState::SandboxNotready as i32,
                        },
                        created_at: record.created_at,
                        labels: labels.clone(),
                        annotations: annotations.clone(),
                        runtime_handler: recovered_runtime_handler.clone(),
                        ..Default::default()
                    };
                    log::info!("Recovered pod: {} with state {}", record.id, record.state);
                    memory_pods.insert(record.id.clone(), pod);

                    let mut additional_ip_values: Vec<IpAddr> = pod_state
                        .additional_ips
                        .iter()
                        .filter_map(|ip| ip.parse::<IpAddr>().ok())
                        .collect();
                    let primary_ip = pod_state
                        .ip
                        .as_ref()
                        .and_then(|ip| ip.parse::<IpAddr>().ok())
                        .or_else(|| additional_ip_values.first().copied());
                    let network_status = primary_ip.map(|parsed_ip| {
                        additional_ip_values.retain(|ip| ip != &parsed_ip);
                        crate::network::NetworkStatus {
                            name: "default".to_string(),
                            ip: Some(parsed_ip),
                            mac: None,
                            interfaces: additional_ip_values
                                .iter()
                                .enumerate()
                                .map(|(idx, ip)| crate::network::NetworkInterface {
                                    name: format!("additional{}", idx),
                                    ip: Some(*ip),
                                    mac: None,
                                    netmask: None,
                                    gateway: None,
                                })
                                .collect(),
                        }
                    });

                    pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
                        id: record.id.clone(),
                        config: crate::pod::PodSandboxConfig {
                            name: record.name.clone(),
                            namespace: record.namespace.clone(),
                            uid: record.uid.clone(),
                            hostname: record.name.clone(),
                            log_directory: pod_state.log_directory.as_ref().map(PathBuf::from),
                            runtime_handler: recovered_runtime_handler.clone(),
                            labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                            annotations: Self::external_annotations(&annotations)
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                            dns_config: None,
                            port_mappings: vec![],
                            network_config: None,
                            cgroup_parent: pod_state.cgroup_parent.clone(),
                            sysctls: pod_state.sysctls.clone(),
                            namespace_options: pod_state
                                .namespace_options
                                .as_ref()
                                .map(StoredNamespaceOptions::to_proto),
                            privileged: pod_state.privileged,
                            run_as_user: pod_state.run_as_user.clone(),
                            run_as_group: pod_state.run_as_group,
                            supplemental_groups: pod_state.supplemental_groups.clone(),
                            readonly_rootfs: pod_state.readonly_rootfs,
                            no_new_privileges: pod_state.no_new_privileges,
                            apparmor_profile: pod_state.apparmor_profile.clone(),
                            selinux_label: pod_state.selinux_label.clone(),
                            seccomp_profile: pod_state
                                .seccomp_profile
                                .as_ref()
                                .and_then(StoredSecurityProfile::to_runtime_seccomp),
                            linux_resources: pod_state
                                .linux_resources
                                .as_ref()
                                .map(StoredLinuxResources::to_proto),
                        },
                        netns_path: PathBuf::from(
                            pod_state
                                .netns_path
                                .unwrap_or_else(|| record.netns_path.clone()),
                        ),
                        pause_container_id: pod_state.pause_container_id.unwrap_or_default(),
                        state: match record.state.as_str() {
                            "ready" => crate::pod::PodSandboxState::Ready,
                            "notready" => crate::pod::PodSandboxState::NotReady,
                            _ => crate::pod::PodSandboxState::Terminated,
                        },
                        created_at: record.created_at,
                        ip: pod_state
                            .ip
                            .or_else(|| {
                                pod_state
                                    .additional_ips
                                    .iter()
                                    .find(|ip| !ip.is_empty())
                                    .cloned()
                            })
                            .or(record.ip.clone())
                            .unwrap_or_default(),
                        network_status,
                    });
                }
                log::info!(
                    "Recovered {} pod sandboxes from database",
                    memory_pods.len()
                );
            }
            Err(e) => {
                log::error!("Failed to recover pod sandboxes: {}", e);
            }
        }

        Ok(())
    }

    /// 将内部 ContainerStats 转换为 proto 格式
    fn convert_to_proto_container_stats(
        stats: crate::metrics::ContainerStats,
    ) -> crate::proto::runtime::v1::ContainerStats {
        use crate::proto::runtime::v1::{ContainerStats, CpuUsage, MemoryUsage, UInt64Value};

        ContainerStats {
            attributes: Some(crate::proto::runtime::v1::ContainerAttributes {
                id: stats.container_id.clone(),
                metadata: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
            }),
            cpu: stats.cpu.map(|cpu| CpuUsage {
                timestamp: stats.timestamp as i64,
                usage_core_nano_seconds: Some(UInt64Value { value: cpu.usage_total }),
                usage_nano_cores: Some(UInt64Value { value: cpu.usage_user.saturating_add(cpu.usage_kernel) }),
            }),
            memory: stats.memory.map(|mem| MemoryUsage {
                timestamp: stats.timestamp as i64,
                working_set_bytes: Some(UInt64Value { value: mem.usage }),
                available_bytes: Some(UInt64Value { value: mem.limit.saturating_sub(mem.usage) }),
                usage_bytes: Some(UInt64Value { value: mem.usage }),
                rss_bytes: Some(UInt64Value { value: mem.usage }), // 简化处理
                page_faults: Some(UInt64Value { value: 0 }),
                major_page_faults: Some(UInt64Value { value: 0 }),
            }),
            writable_layer: None,
            swap: None,
        }
    }

    /// 辅助方法：收集 Pod 的统计信息
    async fn collect_pod_stats(
        &self,
        pod_id: &str,
        pod: &crate::proto::runtime::v1::PodSandbox,
    ) -> Option<crate::proto::runtime::v1::PodSandboxStats> {
        use crate::proto::runtime::v1::{ContainerAttributes, ContainerStats, CpuUsage, LinuxPodSandboxStats, MemoryUsage, NetworkInterfaceUsage, NetworkUsage, PodSandboxStats, PodSandboxAttributes, ProcessUsage, UInt64Value};

        let containers = self.containers.lock().await;
        
        // 收集该 Pod 下所有容器的统计
        let mut total_cpu_usage = 0u64;
        let mut total_memory_usage = 0u64;
        let mut total_memory_limit = 0u64;
        let mut has_stats = false;
        let mut container_stats_list = Vec::new();

        let collector = MetricsCollector::new().ok()?;

        // 获取 pod 的 UID 用于匹配容器
        let pod_uid = pod.metadata.as_ref().map(|m| m.uid.clone());

        for (container_id, container) in containers.iter() {
            // 检查容器是否属于该 Pod
            let belongs_to_pod = if let Some(ref uid) = pod_uid {
                container.annotations.get("io.kubernetes.pod.uid")
                    .map(|container_uid| container_uid == uid)
                    .unwrap_or(true)
            } else {
                true
            };

            if belongs_to_pod {
                let container_state = Self::read_internal_state::<StoredContainerState>(
                    &container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                );

                let cgroup_parent = container_state
                    .as_ref()
                    .and_then(|s| s.cgroup_parent.as_ref())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup"));

                if let Ok(stats) = collector.collect_container_stats(container_id, &cgroup_parent) {
                    if let Some(ref cpu) = stats.cpu {
                        total_cpu_usage += cpu.usage_total;
                    }
                    if let Some(ref mem) = stats.memory {
                        total_memory_usage += mem.usage;
                        total_memory_limit += mem.limit;
                    }
                    has_stats = true;

                    // 添加容器级别统计
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    
                    container_stats_list.push(ContainerStats {
                        attributes: Some(ContainerAttributes {
                            id: container_id.clone(),
                            metadata: container.metadata.clone(),
                            labels: container.labels.clone(),
                            annotations: container.annotations.clone(),
                        }),
                        cpu: stats.cpu.map(|cpu| CpuUsage {
                            timestamp,
                            usage_core_nano_seconds: Some(UInt64Value { value: cpu.usage_total }),
                            usage_nano_cores: Some(UInt64Value { value: cpu.usage_user.saturating_add(cpu.usage_kernel) }),
                        }),
                        memory: stats.memory.map(|mem| MemoryUsage {
                            timestamp,
                            working_set_bytes: Some(UInt64Value { value: mem.usage }),
                            available_bytes: Some(UInt64Value { value: mem.limit.saturating_sub(mem.usage) }),
                            usage_bytes: Some(UInt64Value { value: mem.usage }),
                            rss_bytes: Some(UInt64Value { value: mem.usage }),
                            page_faults: Some(UInt64Value { value: 0 }),
                            major_page_faults: Some(UInt64Value { value: 0 }),
                        }),
                        writable_layer: None,
                        swap: None,
                    });
                }
            }
        }

        if !has_stats {
            return None;
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let container_count = container_stats_list.len() as u64;

        Some(PodSandboxStats {
            attributes: Some(PodSandboxAttributes {
                id: pod_id.to_string(),
                metadata: pod.metadata.clone(),
                labels: pod.labels.clone(),
                annotations: pod.annotations.clone(),
            }),
            linux: Some(LinuxPodSandboxStats {
                cpu: Some(CpuUsage {
                    timestamp,
                    usage_core_nano_seconds: Some(UInt64Value { value: total_cpu_usage }),
                    usage_nano_cores: Some(UInt64Value { value: total_cpu_usage }),
                }),
                memory: Some(MemoryUsage {
                    timestamp,
                    working_set_bytes: Some(UInt64Value { value: total_memory_usage }),
                    available_bytes: Some(UInt64Value { value: total_memory_limit.saturating_sub(total_memory_usage) }),
                    usage_bytes: Some(UInt64Value { value: total_memory_usage }),
                    rss_bytes: Some(UInt64Value { value: total_memory_usage }),
                    page_faults: Some(UInt64Value { value: 0 }),
                    major_page_faults: Some(UInt64Value { value: 0 }),
                }),
                containers: container_stats_list,
                network: Some(NetworkUsage {
                    timestamp,
                    default_interface: Some(NetworkInterfaceUsage {
                        name: "eth0".to_string(),
                        rx_bytes: Some(UInt64Value { value: 0 }),
                        rx_errors: Some(UInt64Value { value: 0 }),
                        tx_bytes: Some(UInt64Value { value: 0 }),
                        tx_errors: Some(UInt64Value { value: 0 }),
                    }),
                    interfaces: Vec::new(),
                }),
                process: Some(ProcessUsage {
                    timestamp,
                    process_count: Some(UInt64Value { value: container_count }),
                }),
            }),
            windows: None,
        })
    }
}

#[tonic::async_trait]
impl RuntimeService for RuntimeServiceImpl {
    // 获取运行时版本
    async fn version(
        &self,
        _request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(VersionResponse {
            version: "0.1.0".to_string(),
            runtime_name: "crius".to_string(),
            runtime_version: "0.1.0".to_string(),
            runtime_api_version: "v1".to_string(),
        }))
    }

    // 创建Pod沙箱
    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        let req = request.into_inner();
        let pod_config = req
            .config
            .ok_or_else(|| Status::invalid_argument("Pod config not specified"))?;
        let runtime_handler = self.resolve_runtime_handler(req.runtime_handler.trim())?;
        let linux_config = pod_config.linux.clone();
        let sandbox_security = linux_config
            .as_ref()
            .and_then(|linux| linux.security_context.as_ref());
        let pod_linux_resources = linux_config
            .as_ref()
            .and_then(|linux| linux.resources.clone().or_else(|| linux.overhead.clone()));
        let pod_selinux_label = Self::selinux_label_from_proto(
            sandbox_security.and_then(|security| security.selinux_options.as_ref()),
        );
        let pod_seccomp_profile = Self::seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            sandbox_security
                .map(|security| security.seccomp_profile_path.as_str())
                .unwrap_or(""),
        );
        let stored_seccomp_profile = Self::stored_seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            sandbox_security
                .map(|security| security.seccomp_profile_path.as_str())
                .unwrap_or(""),
        );

        // 构建Pod沙箱配置
        let sandbox_config = PodSandboxConfig {
            name: pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
            namespace: pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
            uid: pod_config
                .metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default(),
            hostname: pod_config.hostname.clone(),
            log_directory: if pod_config.log_directory.is_empty() {
                None
            } else {
                Some(PathBuf::from(&pod_config.log_directory))
            },
            runtime_handler: runtime_handler.clone(),
            labels: pod_config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            annotations: pod_config
                .annotations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            dns_config: pod_config.dns_config.map(|d| crate::pod::DNSConfig {
                servers: d.servers,
                searches: d.searches,
                options: d.options,
            }),
            port_mappings: pod_config
                .port_mappings
                .iter()
                .map(|p| {
                    // protocol是i32枚举，需要转换为字符串
                    let protocol_str = match p.protocol {
                        0 => "TCP",
                        1 => "UDP",
                        2 => "SCTP",
                        _ => "TCP",
                    }
                    .to_string();
                    crate::pod::PortMapping {
                        protocol: protocol_str,
                        container_port: p.container_port,
                        host_port: p.host_port,
                        host_ip: p.host_ip.clone(),
                    }
                })
                .collect(),
            network_config: None,
            cgroup_parent: linux_config
                .as_ref()
                .and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                })
                .or_else(|| Some("crius".to_string())),
            sysctls: linux_config
                .as_ref()
                .map(|linux| linux.sysctls.clone())
                .unwrap_or_default(),
            namespace_options: sandbox_security
                .and_then(|security| security.namespace_options.clone()),
            privileged: sandbox_security
                .map(|security| security.privileged)
                .unwrap_or(false),
            run_as_user: sandbox_security
                .and_then(|security| security.run_as_user.as_ref())
                .map(|user| user.value.to_string()),
            run_as_group: sandbox_security
                .and_then(|security| security.run_as_group.as_ref())
                .and_then(|group| u32::try_from(group.value).ok()),
            supplemental_groups: sandbox_security
                .map(|security| {
                    security
                        .supplemental_groups
                        .iter()
                        .filter_map(|group| u32::try_from(*group).ok())
                        .collect()
                })
                .unwrap_or_default(),
            readonly_rootfs: sandbox_security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
            no_new_privileges: None,
            apparmor_profile: Self::security_profile_name(
                sandbox_security.and_then(|security| security.apparmor.as_ref()),
                "",
            ),
            selinux_label: pod_selinux_label.clone(),
            seccomp_profile: pod_seccomp_profile.clone(),
            linux_resources: pod_linux_resources.clone(),
        };

        // 创建Pod沙箱
        let mut pod_manager = self.pod_manager.lock().await;
        let pod_id = pod_manager
            .create_pod_sandbox(sandbox_config)
            .await
            .map_err(|e| Status::internal(format!("Failed to create pod sandbox: {}", e)))?;
        let created_pod = pod_manager.get_pod_sandbox_cloned(&pod_id);
        drop(pod_manager);

        let pod_state = created_pod
            .as_ref()
            .map(|pod| StoredPodState {
                log_directory: if pod_config.log_directory.is_empty() {
                    None
                } else {
                    Some(pod_config.log_directory.clone())
                },
                runtime_handler: runtime_handler.clone(),
                netns_path: Some(pod.netns_path.to_string_lossy().to_string()),
                pause_container_id: Some(pod.pause_container_id.clone()),
                ip: if pod.ip.is_empty() {
                    None
                } else {
                    Some(pod.ip.clone())
                },
                additional_ips: pod
                    .network_status
                    .as_ref()
                    .map(|status| {
                        status
                            .interfaces
                            .iter()
                            .filter_map(|iface| iface.ip.as_ref())
                            .map(|ip| ip.to_string())
                            .filter(|ip| pod.ip.is_empty() || ip != &pod.ip)
                            .collect()
                    })
                    .unwrap_or_default(),
                cgroup_parent: linux_config.as_ref().and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                }),
                sysctls: linux_config
                    .as_ref()
                    .map(|linux| linux.sysctls.clone())
                    .unwrap_or_default(),
                namespace_options: sandbox_security
                    .and_then(|security| security.namespace_options.as_ref())
                    .map(StoredNamespaceOptions::from),
                privileged: sandbox_security
                    .map(|security| security.privileged)
                    .unwrap_or(false),
                run_as_user: sandbox_security
                    .and_then(|security| security.run_as_user.as_ref())
                    .map(|user| user.value.to_string()),
                run_as_group: sandbox_security
                    .and_then(|security| security.run_as_group.as_ref())
                    .and_then(|group| u32::try_from(group.value).ok()),
                supplemental_groups: sandbox_security
                    .map(|security| {
                        security
                            .supplemental_groups
                            .iter()
                            .filter_map(|group| u32::try_from(*group).ok())
                            .collect()
                    })
                    .unwrap_or_default(),
                readonly_rootfs: sandbox_security
                    .map(|security| security.readonly_rootfs)
                    .unwrap_or(false),
                no_new_privileges: None,
                apparmor_profile: Self::security_profile_name(
                    sandbox_security.and_then(|security| security.apparmor.as_ref()),
                    "",
                ),
                selinux_label: pod_selinux_label.clone(),
                seccomp_profile: stored_seccomp_profile.clone(),
                linux_resources: pod_linux_resources.as_ref().map(StoredLinuxResources::from),
            })
            .unwrap_or_else(|| StoredPodState {
                log_directory: if pod_config.log_directory.is_empty() {
                    None
                } else {
                    Some(pod_config.log_directory.clone())
                },
                runtime_handler: runtime_handler.clone(),
                cgroup_parent: linux_config.as_ref().and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                }),
                sysctls: linux_config
                    .as_ref()
                    .map(|linux| linux.sysctls.clone())
                    .unwrap_or_default(),
                namespace_options: sandbox_security
                    .and_then(|security| security.namespace_options.as_ref())
                    .map(StoredNamespaceOptions::from),
                privileged: sandbox_security
                    .map(|security| security.privileged)
                    .unwrap_or(false),
                run_as_user: sandbox_security
                    .and_then(|security| security.run_as_user.as_ref())
                    .map(|user| user.value.to_string()),
                run_as_group: sandbox_security
                    .and_then(|security| security.run_as_group.as_ref())
                    .and_then(|group| u32::try_from(group.value).ok()),
                supplemental_groups: sandbox_security
                    .map(|security| {
                        security
                            .supplemental_groups
                            .iter()
                            .filter_map(|group| u32::try_from(*group).ok())
                            .collect()
                    })
                    .unwrap_or_default(),
                readonly_rootfs: sandbox_security
                    .map(|security| security.readonly_rootfs)
                    .unwrap_or(false),
                no_new_privileges: None,
                apparmor_profile: Self::security_profile_name(
                    sandbox_security.and_then(|security| security.apparmor.as_ref()),
                    "",
                ),
                selinux_label: pod_selinux_label.clone(),
                seccomp_profile: stored_seccomp_profile.clone(),
                linux_resources: pod_linux_resources.as_ref().map(StoredLinuxResources::from),
                ..Default::default()
            });
        let mut stored_annotations = pod_config.annotations.clone();
        Self::insert_internal_state(&mut stored_annotations, INTERNAL_POD_STATE_KEY, &pod_state)?;

        // 创建Pod沙箱元数据
        let pod_sandbox = crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: pod_config.metadata.clone(),
            state: PodSandboxState::SandboxReady as i32,
            created_at: created_pod
                .as_ref()
                .map(|pod| Self::normalize_timestamp_nanos(pod.created_at))
                .unwrap_or_else(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64
                }),
            labels: pod_config.labels.clone(),
            annotations: stored_annotations.clone(),
            runtime_handler: runtime_handler.clone(),
            ..Default::default()
        };

        // 存储Pod沙箱信息到内存
        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.insert(pod_id.clone(), pod_sandbox.clone());
        drop(pod_sandboxes);

        // 持久化Pod沙箱状态
        let fallback_netns_path = format!(
            "/var/run/netns/crius-{}-{}",
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default()
        );
        let netns_path = pod_state.netns_path.clone().unwrap_or(fallback_netns_path);
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.save_pod_sandbox(
            &pod_id,
            "ready",
            &pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
            &pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
            &pod_config
                .metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default(),
            &netns_path,
            &pod_config.labels,
            &stored_annotations,
            pod_state.pause_container_id.as_deref(),
            pod_state.ip.as_deref(),
        ) {
            log::error!("Failed to persist pod sandbox {}: {}", pod_id, e);
        } else {
            log::info!("Pod sandbox {} persisted to database", pod_id);
        }

        log::info!("Pod sandbox {} created successfully", pod_id);
        Ok(Response::new(RunPodSandboxResponse {
            pod_sandbox_id: pod_id,
        }))
    }

    // 停止Pod沙箱
    async fn stop_pod_sandbox(
        &self,
        request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;

        log::info!("Stopping pod sandbox {}", pod_id);

        // 先停止该Pod下的所有业务容器，避免出现Pod已NotReady但容器仍Running。
        let container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|c| c.pod_sandbox_id == pod_id)
                .map(|c| c.id.clone())
                .collect()
        };

        let mut post_stop_status: HashMap<String, ContainerStatus> = HashMap::new();
        for container_id in &container_ids {
            let runtime = self.runtime.clone();
            let container_id_clone = container_id.clone();
            tokio::task::spawn_blocking(move || {
                runtime.stop_container(&container_id_clone, Some(30))
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to stop container {} in pod {}: {}",
                    container_id, pod_id, e
                ))
            })?;

            let runtime = self.runtime.clone();
            let container_id_clone = container_id.clone();
            let status =
                tokio::task::spawn_blocking(move || runtime.container_status(&container_id_clone))
                    .await
                    .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
                    .unwrap_or(ContainerStatus::Unknown);
            post_stop_status.insert(container_id.clone(), status);
        }

        // 更新容器状态到内存
        {
            let mut containers = self.containers.lock().await;
            for container_id in &container_ids {
                if let Some(container) = containers.get_mut(container_id) {
                    let status = post_stop_status
                        .get(container_id)
                        .cloned()
                        .unwrap_or(ContainerStatus::Unknown);
                    container.state = Self::map_runtime_container_state(status.clone());
                    if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    ) {
                        state.finished_at = Some(Self::now_nanos());
                        if let ContainerStatus::Stopped(code) = status {
                            state.exit_code = Some(code);
                        }
                        if let Err(e) = Self::insert_internal_state(
                            &mut container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        ) {
                            log::warn!(
                                "Failed to persist in-memory container state for {}: {}",
                                container_id,
                                e
                            );
                        }
                    }
                }
            }
        }

        // 更新容器持久化状态
        {
            let mut persistence = self.persistence.lock().await;
            for container_id in &container_ids {
                let status = post_stop_status
                    .get(container_id)
                    .cloned()
                    .unwrap_or(ContainerStatus::Unknown);
                if let Err(e) = persistence.update_container_state(container_id, status) {
                    log::error!(
                        "Failed to update container {} state in database: {}",
                        container_id,
                        e
                    );
                }
            }
        }

        // 停止Pod沙箱
        let mut pod_manager = self.pod_manager.lock().await;
        pod_manager
            .stop_pod_sandbox(&pod_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to stop pod sandbox: {}", e)))?;

        // 更新Pod沙箱状态到内存
        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        if let Some(pod) = pod_sandboxes.get_mut(&pod_id) {
            pod.state = PodSandboxState::SandboxNotready as i32;
        }
        drop(pod_sandboxes);

        // 更新持久化状态
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.update_pod_state(&pod_id, "notready") {
            log::error!("Failed to update pod {} state in database: {}", pod_id, e);
        }

        log::info!("Pod sandbox {} stopped", pod_id);
        Ok(Response::new(StopPodSandboxResponse {}))
    }

    // 获取容器状态
    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;
        let containers = self.containers.lock().await;

        // 支持短ID匹配
        let found_container_id = if containers.contains_key(&container_id) {
            Some(container_id.clone())
        } else {
            // 尝试前缀匹配（支持短ID）
            containers
                .keys()
                .find(|full_id| full_id.starts_with(&container_id))
                .cloned()
        };

        if let Some(actual_container_id) = found_container_id {
            if let Some(container) = containers.get(&actual_container_id) {
                // 查询runtime获取实时状态
                let runtime_state = match self.runtime.container_status(&actual_container_id) {
                    Ok(status) => match status {
                        ContainerStatus::Created => ContainerState::ContainerCreated,
                        ContainerStatus::Running => ContainerState::ContainerRunning,
                        ContainerStatus::Stopped(_) => ContainerState::ContainerExited,
                        ContainerStatus::Unknown => ContainerState::ContainerUnknown,
                    },
                    Err(_) => ContainerState::ContainerUnknown,
                };
                let status = Self::build_container_status_snapshot(container, runtime_state as i32);

                Ok(Response::new(ContainerStatusResponse {
                    status: Some(status),
                    ..Default::default()
                }))
            } else {
                Err(Status::not_found("Container not found"))
            }
        } else {
            Err(Status::not_found("Container not found"))
        }
    }

    // 列出容器
    async fn list_containers(
        &self,
        request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter;
        let containers = self.containers.lock().await;
        let pod_meta_by_id: HashMap<String, (String, String, String)> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .iter()
                .map(|(id, pod)| {
                    let (name, namespace, uid) = pod
                        .metadata
                        .as_ref()
                        .map(|m| (m.name.clone(), m.namespace.clone(), m.uid.clone()))
                        .unwrap_or_else(|| {
                            ("unknown".to_string(), "default".to_string(), id.clone())
                        });
                    (id.clone(), (name, namespace, uid))
                })
                .collect()
        };

        // 克隆 runtime 以在闭包中使用
        let runtime = self.runtime.clone();
        let containers_list = containers
            .values()
            .cloned()
            .map(|mut c| {
                if c.metadata.is_none() {
                    c.metadata = Some(ContainerMetadata {
                        name: c.id.clone(),
                        attempt: 1,
                    });
                }
                if c.image.is_none() {
                    c.image = Some(ImageSpec {
                        image: c.image_ref.clone(),
                        ..Default::default()
                    });
                }
                c.annotations = Self::external_annotations(&c.annotations);
                if let Some((pod_name, pod_namespace, pod_uid)) =
                    pod_meta_by_id.get(&c.pod_sandbox_id)
                {
                    c.labels
                        .entry("io.kubernetes.pod.name".to_string())
                        .or_insert_with(|| pod_name.clone());
                    c.labels
                        .entry("io.kubernetes.pod.namespace".to_string())
                        .or_insert_with(|| pod_namespace.clone());
                    c.labels
                        .entry("io.kubernetes.pod.uid".to_string())
                        .or_insert_with(|| pod_uid.clone());
                }
                c.created_at = Self::normalize_timestamp_nanos(c.created_at);

                // 查询 runtime 获取实时状态，确保与 runc list 一致
                let runtime_state = match runtime.container_status(&c.id) {
                    Ok(status) => match status {
                        ContainerStatus::Created => ContainerState::ContainerCreated as i32,
                        ContainerStatus::Running => ContainerState::ContainerRunning as i32,
                        ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
                        ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
                    },
                    Err(_) => ContainerState::ContainerUnknown as i32,
                };
                c.state = runtime_state;

                c
            })
            .filter(|c| {
                if let Some(f) = &filter {
                    Self::container_matches_filter(c, f)
                } else {
                    true
                }
            })
            .collect();

        Ok(Response::new(ListContainersResponse {
            containers: containers_list,
        }))
    }

    // 执行命令
    async fn exec(&self, request: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        let mut req = request.into_inner();
        req.container_id = self.resolve_container_id(&req.container_id).await?;
        let streaming = self.get_streaming_server().await?;
        let response = streaming.get_exec(&req).await?;
        Ok(Response::new(response))
    }

    // 同步执行命令
    async fn exec_sync(
        &self,
        request: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        let req = request.into_inner();
        let container_id = self.resolve_container_id(&req.container_id).await?;
        let cmd = req.cmd;
        let timeout = req.timeout;

        log::info!("Exec sync in container {}: {:?}", container_id, cmd);

        if cmd.is_empty() {
            return Err(Status::invalid_argument("cmd must not be empty"));
        }

        let mut command = TokioCommand::new("runc");
        command.arg("exec");
        command.arg(&container_id);
        for arg in &cmd {
            command.arg(arg);
        }
        command.stdin(Stdio::null());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let mut child = command
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to spawn exec process: {}", e)))?;

        let stdout_task = child.stdout.take().map(|mut stdout| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                stdout.read_to_end(&mut buf).await.map(|_| buf)
            })
        });
        let stderr_task = child.stderr.take().map(|mut stderr| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                stderr.read_to_end(&mut buf).await.map(|_| buf)
            })
        });

        let status = if timeout > 0 {
            let timeout = std::time::Duration::from_secs(timeout as u64);
            match tokio::time::timeout(timeout, child.wait()).await {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    return Err(Status::internal(format!("Exec failed: {}", e)));
                }
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return Err(Status::deadline_exceeded(format!(
                        "Exec sync timed out after {}s",
                        timeout.as_secs()
                    )));
                }
            }
        } else {
            child
                .wait()
                .await
                .map_err(|e| Status::internal(format!("Exec failed: {}", e)))?
        };

        let stdout = match stdout_task {
            Some(task) => task
                .await
                .map_err(|e| Status::internal(format!("Failed to join stdout task: {}", e)))?
                .map_err(|e| Status::internal(format!("Failed to read stdout: {}", e)))?,
            None => Vec::new(),
        };
        let stderr = match stderr_task {
            Some(task) => task
                .await
                .map_err(|e| Status::internal(format!("Failed to join stderr task: {}", e)))?
                .map_err(|e| Status::internal(format!("Failed to read stderr: {}", e)))?,
            None => Vec::new(),
        };

        Ok(Response::new(ExecSyncResponse {
            stdout,
            stderr,
            exit_code: status.code().unwrap_or_default(),
        }))
    }

    // 端口转发
    async fn port_forward(
        &self,
        _request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        // 实现端口转发的逻辑
        Ok(Response::new(PortForwardResponse {
            url: "unix:///var/run/crius/crius.sock".to_string(),
        }))
    }

    // 获取运行时状态
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let mut info = HashMap::new();
        info.insert("runtime_name".to_string(), "crius".to_string());
        info.insert("runtime_version".to_string(), "0.1.0".to_string());
        info.insert("runtime_api_version".to_string(), "v1".to_string());
        info.insert(
            "root_dir".to_string(),
            self.config.root_dir.to_string_lossy().to_string(),
        );
        info.insert("runtime".to_string(), self.config.runtime.clone());

        Ok(Response::new(StatusResponse {
            status: Some(RuntimeStatus {
                conditions: vec![
                    RuntimeCondition {
                        r#type: "RuntimeReady".to_string(),
                        status: true,
                        reason: "RuntimeIsReady".to_string(),
                        message: "runtime is ready".to_string(),
                    },
                    RuntimeCondition {
                        r#type: "NetworkReady".to_string(),
                        status: true,
                        reason: "NetworkIsReady".to_string(),
                        message: "network is ready".to_string(),
                    },
                ],
            }),
            info,
        }))
    }

    // 删除pod_sandbox
    async fn remove_pod_sandbox(
        &self,
        request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;

        log::info!("Removing pod sandbox {}", pod_id);

        // Keep a fallback netns name from CRI metadata, so rmp can clean netns
        // even when PodSandboxManager in-memory state was lost after restart.
        let fallback_netns_name = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .get(&pod_id)
                .and_then(|p| p.metadata.as_ref())
                .map(|m| format!("crius-{}-{}", m.namespace, m.name))
        };

        // 删除Pod沙箱
        let mut pod_manager = self.pod_manager.lock().await;
        pod_manager
            .remove_pod_sandbox(&pod_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to remove pod sandbox: {}", e)))?;

        // 级联删除该Pod下的所有容器（匹配containerd/CRI-O期望行为）
        let container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|c| c.pod_sandbox_id == pod_id)
                .map(|c| c.id.clone())
                .collect()
        };

        for container_id in &container_ids {
            let runtime = self.runtime.clone();
            let container_id_clone = container_id.clone();
            tokio::task::spawn_blocking(move || runtime.remove_container(&container_id_clone))
                .await
                .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
                .map_err(|e| {
                    Status::internal(format!(
                        "Failed to remove container {} in pod {}: {}",
                        container_id, pod_id, e
                    ))
                })?;
        }

        // 从内存中移除容器
        {
            let mut containers = self.containers.lock().await;
            for container_id in &container_ids {
                containers.remove(container_id);
            }
        }

        // 从持久化中移除容器
        {
            let mut persistence = self.persistence.lock().await;
            for container_id in &container_ids {
                if let Err(e) = persistence.delete_container(container_id) {
                    log::error!(
                        "Failed to delete container {} from database: {}",
                        container_id,
                        e
                    );
                }
            }
        }

        // Best-effort fallback cleanup for stale netns.
        if let Some(netns_name) = fallback_netns_name {
            let network_manager = DefaultNetworkManager::new(None, None, None);
            if let Err(e) = network_manager.remove_network_namespace(&netns_name).await {
                log::warn!("Fallback netns cleanup failed for {}: {}", netns_name, e);
            }
        }

        // 从内存中移除
        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.remove(&pod_id);
        drop(pod_sandboxes);

        // 从持久化存储中删除
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.delete_pod_sandbox(&pod_id) {
            log::error!("Failed to delete pod {} from database: {}", pod_id, e);
        } else {
            log::info!("Pod sandbox {} removed from database", pod_id);
        }

        log::info!("Pod sandbox {} removed", pod_id);
        Ok(Response::new(RemovePodSandboxResponse {}))
    }

    // 获取pod_sandbox状态
    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        let req = request.into_inner();
        let resolved_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let pod_sandboxes = self.pod_sandboxes.lock().await;

        if let Some(pod_sandbox) = pod_sandboxes.get(&resolved_id) {
            let pod_state = Self::read_internal_state::<StoredPodState>(
                &pod_sandbox.annotations,
                INTERNAL_POD_STATE_KEY,
            );
            let mut info = HashMap::new();
            if req.verbose {
                info.insert("podSandboxId".to_string(), pod_sandbox.id.clone());
                if let Some(metadata) = &pod_sandbox.metadata {
                    info.insert("name".to_string(), metadata.name.clone());
                }
                if let Some(state) = &pod_state {
                    if let Some(netns_path) = &state.netns_path {
                        info.insert("netnsPath".to_string(), netns_path.clone());
                    }
                    if let Some(log_directory) = &state.log_directory {
                        info.insert("logDirectory".to_string(), log_directory.clone());
                    }
                    if let Some(pause_container_id) = &state.pause_container_id {
                        info.insert("pauseContainerId".to_string(), pause_container_id.clone());
                    }
                    if let Some(selinux_label) = &state.selinux_label {
                        info.insert("selinuxLabel".to_string(), selinux_label.clone());
                    }
                    if let Some(seccomp_profile) = &state.seccomp_profile {
                        let profile_desc = match seccomp_profile.profile_type {
                            x if x
                                == crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault
                                    as i32 =>
                            {
                                "RuntimeDefault".to_string()
                            }
                            x if x
                                == crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                                    as i32 =>
                            {
                                "Unconfined".to_string()
                            }
                            x if x
                                == crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                                    as i32 =>
                            {
                                if seccomp_profile.localhost_ref.is_empty() {
                                    "Localhost".to_string()
                                } else {
                                    format!("Localhost({})", seccomp_profile.localhost_ref)
                                }
                            }
                            _ => format!("Unknown({})", seccomp_profile.profile_type),
                        };
                        info.insert("seccompProfile".to_string(), profile_desc);
                    }
                }
            }

            let container_statuses = {
                let containers = self.containers.lock().await;
                containers
                    .values()
                    .filter(|container| container.pod_sandbox_id == resolved_id)
                    .map(|container| {
                        let runtime_state = match self.runtime.container_status(&container.id) {
                            Ok(status) => match status {
                                ContainerStatus::Created => ContainerState::ContainerCreated,
                                ContainerStatus::Running => ContainerState::ContainerRunning,
                                ContainerStatus::Stopped(_) => ContainerState::ContainerExited,
                                ContainerStatus::Unknown => ContainerState::ContainerUnknown,
                            },
                            Err(_) => ContainerState::ContainerUnknown,
                        };
                        Self::build_container_status_snapshot(container, runtime_state as i32)
                    })
                    .collect()
            };

            let status = PodSandboxStatus {
                id: pod_sandbox.id.clone(),
                metadata: Some(pod_sandbox.metadata.clone().unwrap_or(PodSandboxMetadata {
                    name: pod_sandbox.id.clone(),
                    uid: pod_sandbox.id.clone(),
                    namespace: "default".to_string(),
                    attempt: 1,
                })),
                state: pod_sandbox.state,
                created_at: Self::normalize_timestamp_nanos(pod_sandbox.created_at),
                network: Self::pod_network_status_from_state(pod_state.as_ref()),
                linux: Self::pod_linux_status_from_state(pod_state.as_ref()),
                labels: pod_sandbox.labels.clone(),
                annotations: Self::external_annotations(&pod_sandbox.annotations),
                runtime_handler: if pod_sandbox.runtime_handler.is_empty() {
                    let restored = pod_state
                        .as_ref()
                        .map(|state| state.runtime_handler.clone())
                        .filter(|handler| !handler.is_empty())
                        .unwrap_or_else(|| self.config.runtime.clone());
                    if self
                        .config
                        .runtime_handlers
                        .iter()
                        .any(|handler| handler == &restored)
                    {
                        restored
                    } else {
                        self.config.runtime.clone()
                    }
                } else {
                    pod_sandbox.runtime_handler.clone()
                },
                ..Default::default()
            };

            Ok(Response::new(PodSandboxStatusResponse {
                status: Some(status),
                info,
                containers_statuses: container_statuses,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
            }))
        } else {
            Err(Status::not_found("Pod sandbox not found"))
        }
    }

    // 列出pod_sandbox
    async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter;
        let pod_sandboxes = self.pod_sandboxes.lock().await;
        let items = pod_sandboxes
            .values()
            .cloned()
            .map(|mut p| {
                let pod_state = Self::read_internal_state::<StoredPodState>(
                    &p.annotations,
                    INTERNAL_POD_STATE_KEY,
                );
                if p.metadata.is_none() {
                    p.metadata = Some(PodSandboxMetadata {
                        name: p.id.clone(),
                        uid: p.id.clone(),
                        namespace: "default".to_string(),
                        attempt: 1,
                    });
                }
                if p.runtime_handler.is_empty() {
                    let restored = pod_state
                        .as_ref()
                        .map(|state| state.runtime_handler.clone())
                        .filter(|handler| !handler.is_empty())
                        .unwrap_or_else(|| self.config.runtime.clone());
                    p.runtime_handler = if self
                        .config
                        .runtime_handlers
                        .iter()
                        .any(|handler| handler == &restored)
                    {
                        restored
                    } else {
                        self.config.runtime.clone()
                    };
                }
                p.annotations = Self::external_annotations(&p.annotations);
                p.created_at = Self::normalize_timestamp_nanos(p.created_at);
                p
            })
            .filter(|p| {
                if let Some(f) = &filter {
                    Self::pod_sandbox_matches_filter(p, f)
                } else {
                    true
                }
            })
            .collect();

        Ok(Response::new(ListPodSandboxResponse { items }))
    }

    // 创建容器
    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        log::info!("CreateContainer called");
        let req = request.into_inner();
        let pod_sandbox_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("Container config not specified"))?;
        let sandbox_config = req.sandbox_config;

        let container_id = uuid::Uuid::new_v4().to_simple().to_string();

        log::info!("Creating container with ID: {}", container_id);
        log::debug!("Container config: {:?}", config);

        let pod_state = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_sandbox_id).and_then(|pod| {
                Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
            })
        };

        let sandbox_linux = sandbox_config
            .as_ref()
            .and_then(|config| config.linux.as_ref());
        let security = config
            .linux
            .as_ref()
            .and_then(|linux| linux.security_context.as_ref());
        let namespace_options = security.and_then(|security| security.namespace_options.clone());

        let pod_log_directory = sandbox_config
            .as_ref()
            .and_then(|config| {
                (!config.log_directory.is_empty()).then(|| config.log_directory.clone())
            })
            .or_else(|| {
                pod_state
                    .as_ref()
                    .and_then(|state| state.log_directory.clone())
            });
        let log_path = if config.log_path.is_empty() {
            None
        } else if let Some(log_directory) = pod_log_directory {
            Some(PathBuf::from(log_directory).join(&config.log_path))
        } else {
            Some(PathBuf::from(&config.log_path))
        };

        if let Some(path) = &log_path {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    Status::internal(format!("Failed to prepare log directory: {}", e))
                })?;
            }
        }

        let network_namespace_path = {
            let pod_manager = self.pod_manager.lock().await;
            pod_manager.get_pod_netns(&pod_sandbox_id)
        }
        .or_else(|| {
            pod_state
                .as_ref()
                .and_then(|state| state.netns_path.as_ref().map(PathBuf::from))
        });
        let pause_container_id = {
            let pod_manager = self.pod_manager.lock().await;
            pod_manager
                .get_pod_sandbox_cloned(&pod_sandbox_id)
                .map(|pod| pod.pause_container_id)
        }
        .or_else(|| {
            pod_state
                .as_ref()
                .and_then(|state| state.pause_container_id.clone())
        });
        let pid_namespace_path = if let Some(options) = namespace_options.as_ref() {
            if options.pid == NamespaceMode::Pod as i32 {
                if let Some(pause_id) = pause_container_id.as_deref() {
                    self.runtime_namespace_path_for_container(pause_id, "pid")
                        .await?
                } else {
                    None
                }
            } else if options.pid == NamespaceMode::Target as i32 {
                self.runtime_namespace_path_for_target(&options.target_id, "pid")
                    .await?
            } else {
                None
            }
        } else {
            None
        };
        let ipc_namespace_path = if let Some(options) = namespace_options.as_ref() {
            if options.ipc == NamespaceMode::Pod as i32 {
                if let Some(pause_id) = pause_container_id.as_deref() {
                    self.runtime_namespace_path_for_container(pause_id, "ipc")
                        .await?
                } else {
                    None
                }
            } else if options.ipc == NamespaceMode::Target as i32 {
                self.runtime_namespace_path_for_target(&options.target_id, "ipc")
                    .await?
            } else {
                None
            }
        } else {
            None
        };

        let apparmor_profile = Self::security_profile_name(
            security.and_then(|security| security.apparmor.as_ref()),
            security
                .map(|security| security.apparmor_profile.as_str())
                .unwrap_or(""),
        );
        let selinux_label =
            Self::selinux_label_from_proto(security.and_then(|ctx| ctx.selinux_options.as_ref()));
        let seccomp_profile = Self::seccomp_profile_from_proto(
            security.and_then(|ctx| ctx.seccomp.as_ref()),
            security
                .map(|ctx| ctx.seccomp_profile_path.as_str())
                .unwrap_or(""),
        );

        let linux_resources = config
            .linux
            .as_ref()
            .and_then(|linux| linux.resources.clone());
        let run_as_group = security
            .and_then(|security| security.run_as_group.as_ref())
            .and_then(|group| u32::try_from(group.value).ok());
        let supplemental_groups: Vec<u32> = security
            .map(|security| {
                security
                    .supplemental_groups
                    .iter()
                    .filter_map(|group| u32::try_from(*group).ok())
                    .collect()
            })
            .unwrap_or_default();
        let mut stored_annotations = config.annotations.clone();
        let container_state = StoredContainerState {
            log_path: log_path
                .as_ref()
                .map(|path| path.to_string_lossy().to_string()),
            tty: config.tty,
            stdin: config.stdin,
            stdin_once: config.stdin_once,
            readonly_rootfs: security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
            cgroup_parent: sandbox_linux
                .and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                })
                .or_else(|| {
                    pod_state
                        .as_ref()
                        .and_then(|state| state.cgroup_parent.clone())
                }),
            network_namespace_path: network_namespace_path
                .as_ref()
                .map(|path| path.to_string_lossy().to_string()),
            linux_resources: linux_resources.as_ref().map(StoredLinuxResources::from),
            run_as_group,
            supplemental_groups: supplemental_groups.clone(),
            no_new_privileges: security.map(|security| security.no_new_privs),
            apparmor_profile: apparmor_profile.clone(),
            metadata_name: config
                .metadata
                .as_ref()
                .map(|metadata| metadata.name.clone()),
            metadata_attempt: config.metadata.as_ref().map(|metadata| metadata.attempt),
            started_at: None,
            finished_at: None,
            exit_code: None,
        };
        Self::insert_internal_state(
            &mut stored_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &container_state,
        )?;

        // 构建容器配置
        let container_config = ContainerConfig {
            name: config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_else(|| container_id.clone()),
            image: config
                .image
                .as_ref()
                .map(|i| i.image.clone())
                .unwrap_or_default(),
            command: config.command.clone(),
            args: config.args.clone(),
            env: config
                .envs
                .iter()
                .map(|e| {
                    let key = e.key.clone();
                    let value = e.value.clone();
                    (key, value)
                })
                .collect(),
            working_dir: if config.working_dir.is_empty() {
                None
            } else {
                Some(PathBuf::from(&config.working_dir))
            },
            mounts: config
                .mounts
                .iter()
                .map(|m| MountConfig {
                    source: PathBuf::from(&m.host_path),
                    destination: PathBuf::from(&m.container_path),
                    read_only: m.readonly,
                })
                .collect(),
            labels: config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            annotations: stored_annotations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            privileged: config
                .linux
                .as_ref()
                .map(|l| {
                    l.security_context
                        .as_ref()
                        .map(|s| s.privileged)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            user: config
                .linux
                .as_ref()
                .and_then(|linux| linux.security_context.as_ref())
                .and_then(|security| {
                    security
                        .run_as_user
                        .as_ref()
                        .map(|user| user.value.to_string())
                        .or_else(|| {
                            if security.run_as_username.is_empty() {
                                None
                            } else {
                                Some(security.run_as_username.clone())
                            }
                        })
                }),
            run_as_group,
            supplemental_groups,
            hostname: None,
            tty: config.tty,
            stdin: config.stdin,
            stdin_once: config.stdin_once,
            log_path: log_path.clone(),
            readonly_rootfs: security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
            no_new_privileges: security.map(|security| security.no_new_privs),
            apparmor_profile,
            selinux_label,
            seccomp_profile,
            capabilities: security.and_then(|security| security.capabilities.clone()),
            cgroup_parent: sandbox_linux
                .and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                })
                .or_else(|| {
                    pod_state
                        .as_ref()
                        .and_then(|state| state.cgroup_parent.clone())
                }),
            sysctls: HashMap::new(),
            namespace_options: namespace_options.clone(),
            namespace_paths: NamespacePaths {
                network: network_namespace_path,
                pid: pid_namespace_path,
                ipc: ipc_namespace_path,
                ..Default::default()
            },
            linux_resources,
            devices: config
                .devices
                .iter()
                .map(|device| DeviceMapping {
                    source: PathBuf::from(&device.host_path),
                    destination: PathBuf::from(&device.container_path),
                    permissions: if device.permissions.is_empty() {
                        "rwm".to_string()
                    } else {
                        device.permissions.clone()
                    },
                })
                .collect(),
            rootfs: self
                .config
                .root_dir
                .join("containers")
                .join(&container_id)
                .join("rootfs"),
        };

        // 调用runtime创建容器（在阻塞线程中执行）
        let runtime = self.runtime.clone();
        let container_config_clone = container_config.clone();
        let created_id =
            tokio::task::spawn_blocking(move || runtime.create_container(&container_config_clone))
                .await
                .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
                .map_err(|e| Status::internal(format!("Failed to create container: {}", e)))?;

        // 创建容器元数据
        let container = Container {
            id: created_id.clone(),
            pod_sandbox_id: pod_sandbox_id.clone(),
            state: ContainerState::ContainerCreated as i32,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            labels: config.labels.clone(),
            metadata: config.metadata.clone(),
            annotations: stored_annotations.clone(),
            image: config.image.clone(),
            image_ref: config
                .image
                .as_ref()
                .map(|i| i.image.clone())
                .unwrap_or_default(),
            ..Default::default()
        };

        // 存储容器信息到内存
        let mut containers = self.containers.lock().await;
        containers.insert(created_id.clone(), container.clone());
        log::info!(
            "Container stored in memory, total containers: {}",
            containers.len()
        );
        drop(containers);

        // 持久化容器状态
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.save_container(
            &created_id,
            &pod_sandbox_id,
            crate::runtime::ContainerStatus::Created,
            &config
                .image
                .as_ref()
                .map(|i| i.image.clone())
                .unwrap_or_default(),
            &container_config.command,
            &config.labels,
            &stored_annotations,
        ) {
            log::error!("Failed to persist container {}: {}", created_id, e);
        } else {
            log::info!("Container {} persisted to database", created_id);
        }

        Ok(Response::new(CreateContainerResponse {
            container_id: created_id,
        }))
    }

    // 启动容器
    async fn start_container(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;

        log::info!("Starting container {}", container_id);

        // 检查容器是否存在 - 支持短ID匹配
        let containers = self.containers.lock().await;
        log::info!(
            "Current containers in memory: {:?}",
            containers.keys().collect::<Vec<_>>()
        );

        // 尝试精确匹配
        let found_container_id = if containers.contains_key(&container_id) {
            Some(container_id.clone())
        } else {
            // 尝试前缀匹配（支持短ID）
            containers
                .keys()
                .find(|full_id| full_id.starts_with(&container_id))
                .cloned()
        };

        let actual_container_id = match found_container_id {
            Some(id) => id,
            None => {
                log::error!(
                    "Container {} not found in memory. Available containers: {:?}",
                    container_id,
                    containers.keys().collect::<Vec<_>>()
                );
                return Err(Status::not_found("Container not found"));
            }
        };
        drop(containers);

        // 调用runtime启动容器
        let runtime = self.runtime.clone();
        let actual_container_id_clone = actual_container_id.clone();
        tokio::task::spawn_blocking(move || runtime.start_container(&actual_container_id_clone))
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .map_err(|e| Status::internal(format!("Failed to start container: {}", e)))?;

        // 等待运行时状态收敛，避免 shim 异步启动失败时仍然误报成功。
        let mut observed_state = ContainerState::ContainerUnknown as i32;
        let mut reached_known_state = false;
        for _ in 0..20 {
            let runtime = self.runtime.clone();
            let container_id_for_status = actual_container_id.clone();
            let current_status = tokio::task::spawn_blocking(move || {
                runtime.container_status(&container_id_for_status)
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .unwrap_or(ContainerStatus::Unknown);

            observed_state = Self::map_runtime_container_state(current_status.clone());
            match current_status {
                ContainerStatus::Unknown => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                _ => {
                    reached_known_state = true;
                    break;
                }
            }
        }

        if !reached_known_state {
            return Err(Status::internal(
                "Container failed to reach a known runtime state after start",
            ));
        }

        // 更新容器状态到内存
        let mut containers = self.containers.lock().await;
        if let Some(container) = containers.get_mut(&actual_container_id) {
            container.state = observed_state;
            if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            ) {
                match observed_state {
                    x if x == ContainerState::ContainerRunning as i32 => {
                        state.started_at = Some(Self::now_nanos());
                        state.finished_at = None;
                        state.exit_code = None;
                    }
                    x if x == ContainerState::ContainerExited as i32 => {
                        state.finished_at = Some(Self::now_nanos());
                        state.exit_code.get_or_insert(0);
                    }
                    _ => {}
                }

                if let Err(e) = Self::insert_internal_state(
                    &mut container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                ) {
                    log::warn!(
                        "Failed to persist in-memory container state for {}: {}",
                        actual_container_id,
                        e
                    );
                }
            }
        }
        drop(containers);

        // 更新持久化状态
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.update_container_state(
            &actual_container_id,
            match observed_state {
                x if x == ContainerState::ContainerRunning as i32 => {
                    crate::runtime::ContainerStatus::Running
                }
                x if x == ContainerState::ContainerCreated as i32 => {
                    crate::runtime::ContainerStatus::Created
                }
                x if x == ContainerState::ContainerExited as i32 => {
                    crate::runtime::ContainerStatus::Stopped(0)
                }
                _ => crate::runtime::ContainerStatus::Unknown,
            },
        ) {
            log::error!(
                "Failed to update container {} state in database: {}",
                container_id,
                e
            );
        }

        log::info!("Container {} started", container_id);
        Ok(Response::new(StartContainerResponse {}))
    }

    // 停止容器
    async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;
        let timeout = req.timeout as u32;

        log::info!("Stopping container {}", container_id);

        // 支持短ID匹配
        let containers = self.containers.lock().await;
        let found_container_id = if containers.contains_key(&container_id) {
            Some(container_id.clone())
        } else {
            // 尝试前缀匹配（支持短ID）
            containers
                .keys()
                .find(|full_id| full_id.starts_with(&container_id))
                .cloned()
        };
        drop(containers);

        let actual_container_id = match found_container_id {
            Some(id) => id,
            None => {
                return Err(Status::not_found("Container not found"));
            }
        };

        // 调用runtime停止容器
        let runtime = self.runtime.clone();
        let actual_container_id_clone = actual_container_id.clone();
        tokio::task::spawn_blocking(move || {
            runtime.stop_container(&actual_container_id_clone, Some(timeout))
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| Status::internal(format!("Failed to stop container: {}", e)))?;

        let final_runtime_status = {
            let runtime = self.runtime.clone();
            let container_id_for_status = actual_container_id.clone();
            tokio::task::spawn_blocking(move || runtime.container_status(&container_id_for_status))
                .await
                .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
                .unwrap_or(ContainerStatus::Unknown)
        };
        let mut resolved_exit_code = match &final_runtime_status {
            ContainerStatus::Stopped(code) => Some(*code),
            _ => None,
        };

        // 更新容器状态到内存
        let mut containers = self.containers.lock().await;
        if let Some(container) = containers.get_mut(&actual_container_id) {
            container.state = match &final_runtime_status {
                ContainerStatus::Created => ContainerState::ContainerCreated as i32,
                ContainerStatus::Running => ContainerState::ContainerRunning as i32,
                ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
                ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
            };
            if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            ) {
                state.finished_at = Some(Self::now_nanos());
                if resolved_exit_code.is_none() {
                    resolved_exit_code = state.exit_code;
                }
                if let Some(code) = resolved_exit_code {
                    state.exit_code = Some(code);
                }
                if let Err(e) = Self::insert_internal_state(
                    &mut container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                ) {
                    log::warn!(
                        "Failed to persist in-memory container state for {}: {}",
                        actual_container_id,
                        e
                    );
                }
            }
            if container.state == ContainerState::ContainerUnknown as i32
                && resolved_exit_code.is_some()
            {
                container.state = ContainerState::ContainerExited as i32;
            }
        }
        drop(containers);

        let persistence_status = match &final_runtime_status {
            ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
            ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
            ContainerStatus::Stopped(_) => {
                crate::runtime::ContainerStatus::Stopped(resolved_exit_code.unwrap_or_default())
            }
            ContainerStatus::Unknown => match resolved_exit_code {
                Some(code) => crate::runtime::ContainerStatus::Stopped(code),
                None => crate::runtime::ContainerStatus::Unknown,
            },
        };

        // 更新持久化状态（标记为已停止）
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.update_container_state(&actual_container_id, persistence_status)
        {
            log::error!(
                "Failed to update container {} state in database: {}",
                actual_container_id,
                e
            );
        }

        log::info!("Container {} stopped", actual_container_id);
        Ok(Response::new(StopContainerResponse {}))
    }

    // 删除容器
    async fn remove_container(
        &self,
        request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;

        log::info!("Removing container {}", container_id);

        // 支持短ID匹配
        let containers = self.containers.lock().await;
        let found_container_id = if containers.contains_key(&container_id) {
            Some(container_id.clone())
        } else {
            // 尝试前缀匹配（支持短ID）
            containers
                .keys()
                .find(|full_id| full_id.starts_with(&container_id))
                .cloned()
        };
        drop(containers);

        let actual_container_id = match found_container_id {
            Some(id) => id,
            None => {
                return Err(Status::not_found("Container not found"));
            }
        };

        // 调用runtime删除容器
        let runtime = self.runtime.clone();
        let actual_container_id_clone = actual_container_id.clone();
        tokio::task::spawn_blocking(move || runtime.remove_container(&actual_container_id_clone))
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .map_err(|e| Status::internal(format!("Failed to remove container: {}", e)))?;

        // 从内存中移除
        let mut containers = self.containers.lock().await;
        containers.remove(&actual_container_id);
        drop(containers);

        // 从持久化存储中删除
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.delete_container(&actual_container_id) {
            log::error!(
                "Failed to delete container {} from database: {}",
                actual_container_id,
                e
            );
        } else {
            log::info!("Container {} removed from database", actual_container_id);
        }

        log::info!("Container {} removed", actual_container_id);
        Ok(Response::new(RemoveContainerResponse {}))
    }

    //重新打开容器日志
    async fn reopen_container_log(
        &self,
        _request: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        // 实现重新打开容器日志的逻辑
        Ok(Response::new(ReopenContainerLogResponse {}))
    }

    //
    async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        let mut req = request.into_inner();
        req.container_id = self.resolve_container_id(&req.container_id).await?;
        let streaming = self.get_streaming_server().await?;
        let response = streaming.get_attach(&req).await?;
        Ok(Response::new(response))
    }

    // 容器统计信息
    async fn container_stats(
        &self,
        request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;

        log::info!("ContainerStats request for container: {}", container_id);

        let containers = self.containers.lock().await;
        
        log::debug!("Total containers in memory: {}", containers.len());
        
        let container = containers
            .get(&container_id)
            .ok_or_else(|| {
                log::warn!("Container {} not found in memory", container_id);
                Status::not_found("Container not found")
            })?;
        
        log::info!("Found container {} in memory, collecting stats...", container_id);

        // 从容器注解中读取 cgroup_parent
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );

        let cgroup_parent = container_state
            .as_ref()
            .and_then(|s| s.cgroup_parent.as_ref())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup"));

        // 创建指标采集器并收集容器统计信息
        let stats = match MetricsCollector::new() {
            Ok(collector) => {
                match collector.collect_container_stats(&container_id, &cgroup_parent) {
                    Ok(stats) => {
                        // 转换为 proto 格式
                        Some(Self::convert_to_proto_container_stats(stats))
                    }
                    Err(e) => {
                        log::warn!("Failed to collect stats for container {}: {}", container_id, e);
                        None
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to create MetricsCollector: {}", e);
                None
            }
        };

        Ok(Response::new(ContainerStatsResponse { stats }))
    }

    // 容器列表统计信息
    async fn list_container_stats(
        &self,
        _request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        let containers = self.containers.lock().await;
        let mut all_stats = Vec::new();

        // 创建指标采集器
        let collector = match MetricsCollector::new() {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Failed to create MetricsCollector: {}", e);
                return Ok(Response::new(ListContainerStatsResponse {
                    stats: Vec::new(),
                }));
            }
        };

        // 为每个容器收集统计信息
        for (container_id, container) in containers.iter() {
            // 从容器注解中读取 cgroup_parent
            let container_state = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            );

            let cgroup_parent = container_state
                .as_ref()
                .and_then(|s| s.cgroup_parent.as_ref())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup"));

            // 收集容器统计信息
            if let Ok(stats) = collector.collect_container_stats(container_id, &cgroup_parent) {
                let proto_stats = Self::convert_to_proto_container_stats(stats);
                all_stats.push(proto_stats);
            }
        }

        Ok(Response::new(ListContainerStatsResponse { stats: all_stats }))
    }

    // pod沙箱统计信息
    async fn pod_sandbox_stats(
        &self,
        request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        let req = request.into_inner();
        let pod_id = req.pod_sandbox_id;

        let pods = self.pod_sandboxes.lock().await;
        let pod = pods
            .get(&pod_id)
            .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;

        // 收集该 Pod 下所有容器的统计信息并聚合
        let stats = self.collect_pod_stats(&pod_id, pod).await;

        Ok(Response::new(PodSandboxStatsResponse { stats }))
    }

    // pod沙箱列表统计信息
    async fn list_pod_sandbox_stats(
        &self,
        _request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        let pods = self.pod_sandboxes.lock().await;
        let mut all_stats = Vec::new();

        for (pod_id, pod) in pods.iter() {
            if let Some(stats) = self.collect_pod_stats(pod_id, pod).await {
                all_stats.push(stats);
            }
        }

        Ok(Response::new(ListPodSandboxStatsResponse { stats: all_stats }))
    }

    // 更新运行时配置
    async fn update_runtime_config(
        &self,
        _request: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        // 实现 update_runtime_config 的逻辑
        Ok(Response::new(UpdateRuntimeConfigResponse {}))
    }

    //
    async fn checkpoint_container(
        &self,
        _request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        // 实现 checkpoint_container 的逻辑
        Ok(Response::new(CheckpointContainerResponse {}))
    }

    type GetContainerEventsStream = ReceiverStream<Result<ContainerEventResponse, Status>>;

    //
    async fn get_container_events(
        &self,
        _request: Request<GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        log::info!("Get container events");

        // 创建channel用于事件流
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let containers = self.containers.clone();
        let pods = self.pod_sandboxes.clone();

        // 在后台任务中读取当前状态并发送
        tokio::spawn(async move {
            // 获取当前容器状态
            let containers_map = containers.lock().await;
            for (id, container) in containers_map.iter() {
                let event = ContainerEventResponse {
                    container_id: id.clone(),
                    container_event_type: 0, // CONTAINER_CREATED_EVENT
                    created_at: container.created_at,
                    pod_sandbox_status: None,
                    containers_statuses: vec![crate::proto::runtime::v1::ContainerStatus {
                        id: id.clone(),
                        metadata: container.metadata.clone(),
                        state: container.state,
                        created_at: container.created_at,
                        ..Default::default()
                    }],
                };

                if let Err(e) = tx.send(Ok(event)).await {
                    log::error!("Failed to send container event: {}", e);
                    break;
                }
            }
            drop(containers_map);

            // 获取当前Pod状态
            let pods_map = pods.lock().await;
            for (id, pod) in pods_map.iter() {
                let event = ContainerEventResponse {
                    container_id: id.clone(),
                    container_event_type: 2, // POD_SANDBOX_CREATED_EVENT
                    created_at: pod.created_at,
                    pod_sandbox_status: Some(crate::proto::runtime::v1::PodSandboxStatus {
                        id: id.clone(),
                        state: pod.state,
                        created_at: pod.created_at,
                        ..Default::default()
                    }),
                    containers_statuses: vec![],
                };

                if let Err(e) = tx.send(Ok(event)).await {
                    log::error!("Failed to send pod event: {}", e);
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(stream))
    }

    //
    async fn list_metric_descriptors(
        &self,
        _request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        // 实现 list_metric_descriptors 的逻辑
        Ok(Response::new(ListMetricDescriptorsResponse {
            descriptors: Vec::new(),
        }))
    }

    //
    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        // 实现 list_pod_sandbox_metrics 的逻辑
        Ok(Response::new(ListPodSandboxMetricsResponse {
            pod_metrics: Vec::new(),
        }))
    }

    //
    async fn runtime_config(
        &self,
        _request: Request<RuntimeConfigRequest>,
    ) -> Result<Response<RuntimeConfigResponse>, Status> {
        // 返回当前运行时配置
        let config = RuntimeConfigResponse {
            linux: Some(crate::proto::runtime::v1::LinuxRuntimeConfiguration {
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(Response::new(config))
    }

    async fn update_container_resources(
        &self,
        request: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;

        // 检查容器是否存在
        let containers = self.containers.lock().await;
        let container = containers
            .get(&container_id)
            .ok_or_else(|| Status::not_found("Container not found"))?;

        // 检查容器状态，只有 running 状态的容器才能更新资源
        let is_running = container.state == ContainerState::ContainerRunning as i32;
        drop(containers);

        if !is_running {
            return Err(Status::failed_precondition(
                "Container must be in RUNNING state to update resources",
            ));
        }

        // 获取资源限制
        let linux = req.linux;
        let _windows = req.windows;

        if let Some(resources) = linux {
            log::info!(
                "Updating container {} resources: CPU shares={}, Memory limit={}",
                container_id,
                resources.cpu_shares,
                resources.memory_limit_in_bytes
            );

            // 调用 runtime 更新容器资源
            self.runtime
                .update_container_resources(&container_id, &resources)
                .map_err(|e| Status::internal(format!("Failed to update container resources: {}", e)))?;

            log::info!("Container {} resources updated successfully", container_id);
        }

        Ok(Response::new(UpdateContainerResourcesResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn pod_network_status_keeps_primary_and_additional_ips() {
        let state = StoredPodState {
            ip: Some("10.88.0.10".to_string()),
            additional_ips: vec![
                "10.88.0.11".to_string(),
                "10.88.0.10".to_string(),
                "".to_string(),
                "10.88.0.11".to_string(),
            ],
            ..Default::default()
        };

        let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
            .expect("expected network status");
        assert_eq!(status.ip, "10.88.0.10");
        assert_eq!(status.additional_ips.len(), 1);
        assert_eq!(status.additional_ips[0].ip, "10.88.0.11");
    }

    #[test]
    fn pod_network_status_promotes_first_additional_ip_when_primary_missing() {
        let state = StoredPodState {
            additional_ips: vec!["fd00::10".to_string(), "10.88.0.11".to_string()],
            ..Default::default()
        };

        let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
            .expect("expected network status");
        assert_eq!(status.ip, "fd00::10");
        assert_eq!(status.additional_ips.len(), 1);
        assert_eq!(status.additional_ips[0].ip, "10.88.0.11");
    }

    #[test]
    fn build_container_status_snapshot_uses_internal_timestamps_and_exit_code() {
        let mut annotations = HashMap::new();
        let stored = StoredContainerState {
            started_at: Some(5),
            finished_at: Some(8),
            exit_code: Some(42),
            ..Default::default()
        };
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &stored,
        )
        .expect("store internal state");

        let container = Container {
            id: "container-1".to_string(),
            metadata: Some(ContainerMetadata {
                name: "c1".to_string(),
                attempt: 1,
            }),
            image: Some(ImageSpec {
                image: "busybox:latest".to_string(),
                ..Default::default()
            }),
            image_ref: "busybox:latest".to_string(),
            annotations,
            created_at: 1,
            ..Default::default()
        };

        let status = RuntimeServiceImpl::build_container_status_snapshot(
            &container,
            ContainerState::ContainerExited as i32,
        );

        assert_eq!(
            status.started_at,
            RuntimeServiceImpl::normalize_timestamp_nanos(5)
        );
        assert_eq!(
            status.finished_at,
            RuntimeServiceImpl::normalize_timestamp_nanos(8)
        );
        assert_eq!(status.exit_code, 42);
    }

    #[test]
    fn selinux_label_from_proto_uses_defaults_for_missing_parts() {
        let label = RuntimeServiceImpl::selinux_label_from_proto(Some(
            &crate::proto::runtime::v1::SeLinuxOption {
                user: String::new(),
                role: String::new(),
                r#type: "spc_t".to_string(),
                level: String::new(),
            },
        ));

        assert_eq!(label.as_deref(), Some("system_u:system_r:spc_t:s0"));
    }

    #[test]
    fn seccomp_profile_from_proto_supports_localhost_and_unconfined() {
        let localhost_profile = RuntimeServiceImpl::seccomp_profile_from_proto(
            Some(&crate::proto::runtime::v1::SecurityProfile {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                    as i32,
                localhost_ref: "/tmp/seccomp/profile.json".to_string(),
            }),
            "",
        );
        assert!(matches!(
            localhost_profile,
            Some(SeccompProfile::Localhost(_))
        ));

        let unconfined_profile = RuntimeServiceImpl::seccomp_profile_from_proto(
            Some(&crate::proto::runtime::v1::SecurityProfile {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                    as i32,
                localhost_ref: String::new(),
            }),
            "",
        );
        assert!(matches!(
            unconfined_profile,
            Some(SeccompProfile::Unconfined)
        ));
    }
}
