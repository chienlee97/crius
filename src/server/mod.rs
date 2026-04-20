use anyhow::Context;
use log;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use tempfile::NamedTempFile;
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
    AttachRequest, AttachResponse, CgroupDriver, CheckpointContainerRequest,
    CheckpointContainerResponse, ContainerEventResponse, ContainerEventType, ContainerMetadata,
    ContainerResources, ContainerStatsRequest, ContainerStatsResponse, ContainerStatusRequest,
    ContainerStatusResponse, CreateContainerRequest, CreateContainerResponse, GetEventsRequest,
    ImageSpec, LinuxPodSandboxStatus, ListContainerStatsRequest, ListContainerStatsResponse,
    ListContainersRequest, ListContainersResponse, ListMetricDescriptorsRequest,
    ListMetricDescriptorsResponse, ListPodSandboxMetricsRequest, ListPodSandboxMetricsResponse,
    ListPodSandboxRequest, ListPodSandboxResponse, ListPodSandboxStatsRequest,
    ListPodSandboxStatsResponse, Namespace, NamespaceMode, NamespaceOption, PodIp,
    PodSandboxMetadata, PodSandboxNetworkStatus, PodSandboxState, PodSandboxStatsRequest,
    PodSandboxStatsResponse, PodSandboxStatus, PodSandboxStatusRequest, PodSandboxStatusResponse,
    RemoveContainerRequest, RemoveContainerResponse, RemovePodSandboxRequest,
    RemovePodSandboxResponse, ReopenContainerLogRequest, ReopenContainerLogResponse,
    RuntimeCondition, RuntimeConfigRequest, RuntimeConfigResponse, RuntimeStatus,
    StartContainerRequest, StartContainerResponse, StopContainerRequest, StopContainerResponse,
    UpdateContainerResourcesRequest, UpdateContainerResourcesResponse, UpdateRuntimeConfigRequest,
    UpdateRuntimeConfigResponse,
};
use crate::storage::persistence::{PersistenceConfig, PersistenceManager};

use crate::metrics::MetricsCollector;
use crate::network::{CniConfig, DefaultNetworkManager, NetworkManager};
use crate::config::NriConfig;
use crate::nri::{
    NopNri, NriApi, NriContainerEvent, NriManager, NriManagerConfig, NriPodEvent,
};
use crate::pod::{PodSandboxConfig, PodSandboxManager};
use crate::runtime::{
    default_shim_work_dir, ContainerConfig, ContainerRuntime, ContainerStatus, DeviceMapping,
    MountConfig, NamespacePaths, RuncRuntime, SeccompProfile, ShimConfig, ShimProcess,
};
use crate::streaming::StreamingServer;

/// 运行时服务实现
pub struct RuntimeServiceImpl {
    // 存储容器状态的线程安全HashMap
    containers: Arc<Mutex<HashMap<String, Container>>>,
    // 存储Pod沙箱状态的线程安全HashMap
    pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    // 运行时配置
    config: RuntimeConfig,
    // NRI 配置
    nri_config: NriConfig,
    // NRI API（关闭场景默认 Nop）
    nri: Arc<dyn NriApi>,
    // 容器运行时
    runtime: RuncRuntime,
    // Pod沙箱管理器
    pod_manager: tokio::sync::Mutex<PodSandboxManager<RuncRuntime>>,
    // 持久化管理器
    persistence: Arc<Mutex<PersistenceManager>>,
    // 流式服务
    streaming: Arc<Mutex<Option<StreamingServer>>>,
    // 持续事件广播
    events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
    // shim 工作目录
    shim_work_dir: PathBuf,
    // UpdateRuntimeConfig 下发的运行时网络配置
    runtime_network_config: Arc<Mutex<Option<crate::proto::runtime::v1::NetworkConfig>>>,
    // 已注册的退出监控任务，避免重复启动
    exit_monitors: Arc<Mutex<HashSet<String>>>,
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
    pub cni_config: CniConfig,
}

const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";
const INTERNAL_POD_STATE_KEY: &str = "io.crius.internal/pod-state";
const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";
const INTERNAL_CHECKPOINT_RESTORE_KEY: &str = "io.crius.internal/checkpoint-restore";
const CHECKPOINT_LOCATION_ANNOTATION_KEY: &str = "io.crius.checkpoint.location";

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
    runtime_pod_cidr: Option<String>,
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
    privileged: bool,
    readonly_rootfs: bool,
    cgroup_parent: Option<String>,
    network_namespace_path: Option<String>,
    linux_resources: Option<StoredLinuxResources>,
    mounts: Vec<StoredMount>,
    run_as_user: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCheckpointRestore {
    checkpoint_location: String,
    checkpoint_image_path: String,
    oci_config: serde_json::Value,
    image_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredRuntimeNetworkConfig {
    pod_cidr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct StoredMount {
    container_path: String,
    host_path: String,
    readonly: bool,
    selinux_relabel: bool,
    propagation: i32,
}

impl RuntimeServiceImpl {
    fn runtime_network_config_path(root_dir: &Path) -> PathBuf {
        root_dir.join("runtime_network_config.json")
    }

    fn load_runtime_network_config(
        root_dir: &Path,
    ) -> anyhow::Result<Option<crate::proto::runtime::v1::NetworkConfig>> {
        let path = Self::runtime_network_config_path(root_dir);
        if !path.exists() {
            return Ok(None);
        }

        let raw = std::fs::read(&path)
            .with_context(|| format!("Failed to read runtime network config {}", path.display()))?;
        let stored: StoredRuntimeNetworkConfig =
            serde_json::from_slice(&raw).with_context(|| {
                format!("Failed to parse runtime network config {}", path.display())
            })?;
        if stored.pod_cidr.trim().is_empty() {
            Ok(None)
        } else {
            Ok(Some(crate::proto::runtime::v1::NetworkConfig {
                pod_cidr: stored.pod_cidr,
            }))
        }
    }

    fn persist_runtime_network_config(
        root_dir: &Path,
        config: Option<&crate::proto::runtime::v1::NetworkConfig>,
    ) -> anyhow::Result<()> {
        let path = Self::runtime_network_config_path(root_dir);
        if let Some(config) = config {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "Failed to create runtime config directory {}",
                        parent.display()
                    )
                })?;
            }
            let stored = StoredRuntimeNetworkConfig {
                pod_cidr: config.pod_cidr.clone(),
            };
            std::fs::write(&path, serde_json::to_vec_pretty(&stored)?).with_context(|| {
                format!("Failed to write runtime network config {}", path.display())
            })?;
        } else if path.exists() {
            std::fs::remove_file(&path).with_context(|| {
                format!("Failed to remove runtime network config {}", path.display())
            })?;
        }
        Ok(())
    }

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

    async fn persist_container_annotations(
        &self,
        container_id: &str,
        annotations: &HashMap<String, String>,
    ) -> Result<(), Status> {
        let encoded_annotations = serde_json::to_string(annotations)
            .map_err(|e| Status::internal(format!("Failed to encode annotations: {}", e)))?;

        let mut persistence = self.persistence.lock().await;
        let Some(mut record) = persistence
            .storage()
            .get_container(container_id)
            .map_err(|e| Status::internal(format!("Failed to load container record: {}", e)))?
        else {
            return Ok(());
        };

        record.annotations = encoded_annotations;
        persistence
            .storage_mut()
            .save_container(&record)
            .map_err(|e| Status::internal(format!("Failed to persist container record: {}", e)))
    }

    fn persist_bundle_annotations(
        &self,
        container_id: &str,
        annotations: &HashMap<String, String>,
    ) -> Result<(), Status> {
        let config_path = self.checkpoint_config_path(container_id);
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config_path).map_err(|e| {
                Status::internal(format!(
                    "Failed to read container bundle config {}: {}",
                    config_path.display(),
                    e
                ))
            })?)
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to parse container bundle config {}: {}",
                    config_path.display(),
                    e
                ))
            })?;
        config["annotations"] = serde_json::to_value(annotations).map_err(|e| {
            Status::internal(format!("Failed to encode container annotations: {}", e))
        })?;
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&config).map_err(|e| {
                Status::internal(format!("Failed to encode updated bundle config: {}", e))
            })?,
        )
        .map_err(|e| {
            Status::internal(format!(
                "Failed to write container bundle config {}: {}",
                config_path.display(),
                e
            ))
        })
    }

    async fn mutate_container_internal_state<F>(
        &self,
        container_id: &str,
        mutator: F,
    ) -> Result<Option<Container>, Status>
    where
        F: FnOnce(&mut StoredContainerState),
    {
        let updated = {
            let mut containers = self.containers.lock().await;
            let Some(container) = containers.get_mut(container_id) else {
                return Ok(None);
            };
            let mut state = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            )
            .unwrap_or_default();
            mutator(&mut state);
            Self::insert_internal_state(
                &mut container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
                &state,
            )?;
            Some(container.clone())
        };

        if let Some(container) = &updated {
            self.persist_container_annotations(container_id, &container.annotations)
                .await?;
        }

        Ok(updated)
    }

    fn linux_resources_to_runtime_update_payload(
        resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> serde_json::Value {
        let mut payload = serde_json::Map::new();

        let mut cpu = serde_json::Map::new();
        if resources.cpu_shares > 0 {
            cpu.insert("shares".to_string(), json!(resources.cpu_shares));
        }
        if resources.cpu_quota > 0 {
            cpu.insert("quota".to_string(), json!(resources.cpu_quota));
        }
        if resources.cpu_period > 0 {
            cpu.insert("period".to_string(), json!(resources.cpu_period));
        }
        if !resources.cpuset_cpus.is_empty() {
            cpu.insert("cpus".to_string(), json!(resources.cpuset_cpus));
        }
        if !resources.cpuset_mems.is_empty() {
            cpu.insert("mems".to_string(), json!(resources.cpuset_mems));
        }
        if !cpu.is_empty() {
            payload.insert("cpu".to_string(), serde_json::Value::Object(cpu));
        }

        let mut memory = serde_json::Map::new();
        if resources.memory_limit_in_bytes > 0 {
            memory.insert("limit".to_string(), json!(resources.memory_limit_in_bytes));
        }
        if resources.memory_swap_limit_in_bytes > 0 {
            memory.insert(
                "swap".to_string(),
                json!(resources.memory_swap_limit_in_bytes),
            );
        }
        if !memory.is_empty() {
            payload.insert("memory".to_string(), serde_json::Value::Object(memory));
        }

        if resources.oom_score_adj != 0 {
            payload.insert("oomScoreAdj".to_string(), json!(resources.oom_score_adj));
        }
        if !resources.hugepage_limits.is_empty() {
            payload.insert(
                "hugepageLimits".to_string(),
                serde_json::Value::Array(
                    resources
                        .hugepage_limits
                        .iter()
                        .map(|limit| {
                            json!({
                                "pageSize": limit.page_size,
                                "limit": limit.limit,
                            })
                        })
                        .collect(),
                ),
            );
        }
        if !resources.unified.is_empty() {
            payload.insert("unified".to_string(), json!(resources.unified));
        }

        serde_json::Value::Object(payload)
    }

    async fn runtime_update_container_resources(
        &self,
        container_id: &str,
        resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> Result<(), Status> {
        let mut resource_file = NamedTempFile::new_in(&self.config.root_dir)
            .or_else(|_| NamedTempFile::new())
            .map_err(|e| {
                Status::internal(format!("Failed to create temporary resource file: {}", e))
            })?;
        let payload = Self::linux_resources_to_runtime_update_payload(resources);
        serde_json::to_writer(resource_file.as_file_mut(), &payload)
            .map_err(|e| Status::internal(format!("Failed to encode OCI resources: {}", e)))?;
        resource_file
            .as_file_mut()
            .flush()
            .map_err(|e| Status::internal(format!("Failed to flush OCI resources: {}", e)))?;

        let runtime_path = self.config.runtime_path.clone();
        let resource_path = resource_file.path().to_path_buf();
        let container_id = container_id.to_string();
        let error_container_id = container_id.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(runtime_path)
                .arg("update")
                .arg("--resources")
                .arg(&resource_path)
                .arg(&container_id)
                .output()
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn update task: {}", e)))?
        .map_err(|e| Status::internal(format!("Failed to execute runtime update: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let message = if stderr.is_empty() {
                format!("runtime update exited with status {}", output.status)
            } else {
                stderr
            };
            return Err(Status::internal(format!(
                "Failed to update runtime resources for {}: {}",
                error_container_id, message
            )));
        }

        Ok(())
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

    #[allow(deprecated)]
    fn legacy_linux_sandbox_seccomp_profile_path(
        security: Option<&crate::proto::runtime::v1::LinuxSandboxSecurityContext>,
    ) -> &str {
        security
            .map(|security| security.seccomp_profile_path.as_str())
            .unwrap_or("")
    }

    #[allow(deprecated)]
    fn legacy_linux_container_apparmor_profile(
        security: Option<&crate::proto::runtime::v1::LinuxContainerSecurityContext>,
    ) -> &str {
        security
            .map(|security| security.apparmor_profile.as_str())
            .unwrap_or("")
    }

    #[allow(deprecated)]
    fn legacy_linux_container_seccomp_profile_path(
        security: Option<&crate::proto::runtime::v1::LinuxContainerSecurityContext>,
    ) -> &str {
        security
            .map(|ctx| ctx.seccomp_profile_path.as_str())
            .unwrap_or("")
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

    fn pod_requires_managed_netns(state: Option<&StoredPodState>) -> bool {
        state
            .and_then(|pod| pod.namespace_options.as_ref())
            .map(|options| options.network != NamespaceMode::Node as i32)
            .unwrap_or(true)
    }

    fn pod_has_required_netns(pod: &crate::proto::runtime::v1::PodSandbox) -> bool {
        let state =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY);
        if !Self::pod_requires_managed_netns(state.as_ref()) {
            return true;
        }

        state
            .and_then(|pod| pod.netns_path)
            .filter(|path| !path.is_empty())
            .map(|path| Path::new(&path).exists())
            .unwrap_or(true)
    }

    fn runtime_state_name(runtime_state: i32) -> &'static str {
        match runtime_state {
            x if x == ContainerState::ContainerCreated as i32 => "created",
            x if x == ContainerState::ContainerRunning as i32 => "running",
            x if x == ContainerState::ContainerExited as i32 => "exited",
            _ => "unknown",
        }
    }

    fn checkpoint_bundle_path(&self, container_id: &str) -> PathBuf {
        self.config.runtime_root.join(container_id)
    }

    fn checkpoint_config_path(&self, container_id: &str) -> PathBuf {
        self.checkpoint_bundle_path(container_id)
            .join("config.json")
    }

    fn checkpoint_runtime_image_path(location: &Path) -> PathBuf {
        if location.extension().is_some() {
            location.with_extension("checkpoint")
        } else {
            location.join("checkpoint")
        }
    }

    fn checkpoint_location_is_json(location: &Path) -> bool {
        location
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
    }

    fn checkpoint_location_is_archive(location: &Path) -> bool {
        location.extension().is_some() && !Self::checkpoint_location_is_json(location)
    }

    fn write_checkpoint_metadata_dir(
        dir: &Path,
        manifest: &serde_json::Value,
        config_payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Failed to create checkpoint directory {}", dir.display()))?;
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_vec_pretty(manifest)?,
        )
        .with_context(|| {
            format!(
                "Failed to write checkpoint manifest {}",
                dir.join("manifest.json").display()
            )
        })?;
        std::fs::write(
            dir.join("config.json"),
            serde_json::to_vec_pretty(config_payload)?,
        )
        .with_context(|| {
            format!(
                "Failed to write checkpoint OCI config {}",
                dir.join("config.json").display()
            )
        })?;
        Ok(())
    }

    fn checkpoint_tar(dir: &Path, archive_path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = archive_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create checkpoint archive parent directory {}",
                    parent.display()
                )
            })?;
        }
        if archive_path.exists() {
            std::fs::remove_file(archive_path).with_context(|| {
                format!(
                    "Failed to remove existing checkpoint archive {}",
                    archive_path.display()
                )
            })?;
        }

        let output = Command::new("tar")
            .arg("-cf")
            .arg(archive_path)
            .arg("-C")
            .arg(dir)
            .arg(".")
            .output()
            .context("Failed to execute tar for checkpoint export")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let detail = if stderr.is_empty() {
                format!("status={}", output.status)
            } else {
                stderr
            };
            return Err(anyhow::anyhow!(
                "Failed to create checkpoint archive {}: {}",
                archive_path.display(),
                detail
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(archive_path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(archive_path, perms)?;
        }

        Ok(())
    }

    fn checkpoint_rootfs_snapshot(rootfs_path: &Path, snapshot_path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = snapshot_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create checkpoint rootfs snapshot parent {}",
                    parent.display()
                )
            })?;
        }
        if snapshot_path.exists() {
            std::fs::remove_file(snapshot_path).with_context(|| {
                format!(
                    "Failed to remove existing checkpoint rootfs snapshot {}",
                    snapshot_path.display()
                )
            })?;
        }

        let output = Command::new("tar")
            .arg("-cf")
            .arg(snapshot_path)
            .arg("-C")
            .arg(rootfs_path)
            .arg(".")
            .output()
            .with_context(|| {
                format!(
                    "Failed to create rootfs snapshot from {}",
                    rootfs_path.display()
                )
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let detail = if stderr.is_empty() {
                format!("status={}", output.status)
            } else {
                stderr
            };
            return Err(anyhow::anyhow!(
                "Failed to create checkpoint rootfs snapshot {}: {}",
                snapshot_path.display(),
                detail
            ));
        }

        Ok(())
    }

    fn extract_checkpoint_archive(archive_path: &Path, target_dir: &Path) -> anyhow::Result<()> {
        if target_dir.exists() {
            std::fs::remove_dir_all(target_dir).with_context(|| {
                format!(
                    "Failed to clear extracted checkpoint directory {}",
                    target_dir.display()
                )
            })?;
        }
        std::fs::create_dir_all(target_dir).with_context(|| {
            format!(
                "Failed to create extracted checkpoint directory {}",
                target_dir.display()
            )
        })?;

        let output = Command::new("tar")
            .arg("-xf")
            .arg(archive_path)
            .arg("-C")
            .arg(target_dir)
            .output()
            .context("Failed to execute tar for checkpoint import")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let detail = if stderr.is_empty() {
                format!("status={}", output.status)
            } else {
                stderr
            };
            return Err(anyhow::anyhow!(
                "Failed to extract checkpoint archive {}: {}",
                archive_path.display(),
                detail
            ));
        }

        Ok(())
    }

    fn load_checkpoint_artifact(
        location: &Path,
    ) -> anyhow::Result<(serde_json::Value, serde_json::Value)> {
        if Self::checkpoint_location_is_json(location) {
            let payload: serde_json::Value = serde_json::from_slice(&std::fs::read(location)?)
                .with_context(|| {
                    format!("Failed to parse checkpoint artifact {}", location.display())
                })?;
            let manifest = payload
                .get("manifest")
                .cloned()
                .context("checkpoint artifact is missing manifest")?;
            let oci_config = payload
                .get("ociConfig")
                .cloned()
                .context("checkpoint artifact is missing ociConfig")?;
            return Ok((manifest, oci_config));
        }

        let artifact_dir = if Self::checkpoint_location_is_archive(location) {
            let extracted_dir = Self::checkpoint_runtime_image_path(location);
            let manifest_path = extracted_dir.join("manifest.json");
            let config_path = extracted_dir.join("config.json");
            if !manifest_path.exists() || !config_path.exists() {
                Self::extract_checkpoint_archive(location, &extracted_dir)?;
            }
            extracted_dir
        } else {
            location.to_path_buf()
        };

        let manifest_path = artifact_dir.join("manifest.json");
        let config_path = artifact_dir.join("config.json");
        let manifest =
            serde_json::from_slice(&std::fs::read(&manifest_path)?).with_context(|| {
                format!(
                    "Failed to parse checkpoint manifest {}",
                    manifest_path.display()
                )
            })?;
        let oci_config =
            serde_json::from_slice(&std::fs::read(&config_path)?).with_context(|| {
                format!(
                    "Failed to parse checkpoint OCI config {}",
                    config_path.display()
                )
            })?;
        Ok((manifest, oci_config))
    }

    fn write_checkpoint_artifact(
        &self,
        location: &Path,
        checkpoint_image_path: &Path,
        manifest: &serde_json::Value,
        config_payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        if Self::checkpoint_location_is_json(location) {
            Self::write_checkpoint_metadata_dir(checkpoint_image_path, manifest, config_payload)?;
            if let Some(parent) = location.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "Failed to create checkpoint artifact directory {}",
                        parent.display()
                    )
                })?;
            }

            let payload = json!({
                "manifest": manifest,
                "ociConfig": config_payload,
            });
            std::fs::write(location, serde_json::to_vec_pretty(&payload)?).with_context(|| {
                format!("Failed to write checkpoint artifact {}", location.display())
            })?;
            return Ok(());
        }

        if Self::checkpoint_location_is_archive(location) {
            Self::write_checkpoint_metadata_dir(checkpoint_image_path, manifest, config_payload)?;
            Self::checkpoint_tar(checkpoint_image_path, location)?;
            return Ok(());
        }

        std::fs::create_dir_all(location).with_context(|| {
            format!(
                "Failed to create checkpoint artifact directory {}",
                location.display()
            )
        })?;
        Self::write_checkpoint_metadata_dir(location, manifest, config_payload)?;
        Ok(())
    }

    fn container_reason_message(runtime_state: i32, exit_code: i32) -> (String, String) {
        match runtime_state {
            x if x == ContainerState::ContainerCreated as i32 => (
                "Created".to_string(),
                "container has been created but not started".to_string(),
            ),
            x if x == ContainerState::ContainerRunning as i32 => {
                ("Running".to_string(), "container is running".to_string())
            }
            x if x == ContainerState::ContainerExited as i32 => {
                let reason = if exit_code == 0 {
                    "Completed"
                } else if exit_code == 137 {
                    "OOMKilled"
                } else {
                    "Error"
                };
                (
                    reason.to_string(),
                    format!("container exited with code {}", exit_code),
                )
            }
            _ => (
                "Unknown".to_string(),
                "runtime state could not be determined".to_string(),
            ),
        }
    }

    fn stored_mounts_to_proto(mounts: &[StoredMount]) -> Vec<crate::proto::runtime::v1::Mount> {
        mounts
            .iter()
            .map(|mount| crate::proto::runtime::v1::Mount {
                container_path: mount.container_path.clone(),
                host_path: mount.host_path.clone(),
                readonly: mount.readonly,
                selinux_relabel: mount.selinux_relabel,
                propagation: mount.propagation,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
            })
            .collect()
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
        let (reason, message) = Self::container_reason_message(runtime_state, exit_code);
        let mounts = Self::stored_mounts_to_proto(
            container_state
                .as_ref()
                .map(|state| state.mounts.as_slice())
                .unwrap_or(&[]),
        );

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
            reason,
            message,
            labels: container.labels.clone(),
            annotations: Self::external_annotations(&container.annotations),
            mounts,
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

    fn build_pod_sandbox_status_snapshot(
        &self,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> PodSandboxStatus {
        Self::build_pod_sandbox_status_snapshot_with_config(&self.config, pod_sandbox)
    }

    fn map_runtime_container_state(status: crate::runtime::ContainerStatus) -> i32 {
        match status {
            ContainerStatus::Created => ContainerState::ContainerCreated as i32,
            ContainerStatus::Running => ContainerState::ContainerRunning as i32,
            ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
            ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
        }
    }

    fn cgroup_driver(&self) -> CgroupDriver {
        match std::env::var("CRIUS_CGROUP_DRIVER")
            .ok()
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("systemd") => CgroupDriver::Systemd,
            Some("cgroupfs") => CgroupDriver::Cgroupfs,
            _ => {
                let systemd_active = std::path::Path::new("/run/systemd/system").exists()
                    || std::fs::read_to_string("/proc/1/comm")
                        .map(|content| content.trim() == "systemd")
                        .unwrap_or(false);
                let cgroup_v2 = std::path::Path::new("/sys/fs/cgroup/cgroup.controllers").exists();
                let systemd_cgroup_layout = std::path::Path::new("/sys/fs/cgroup/system.slice")
                    .exists()
                    || std::path::Path::new("/sys/fs/cgroup/user.slice").exists()
                    || std::path::Path::new("/sys/fs/cgroup/systemd").exists();

                if systemd_active && (cgroup_v2 || systemd_cgroup_layout) {
                    CgroupDriver::Systemd
                } else {
                    CgroupDriver::Cgroupfs
                }
            }
        }
    }

    fn runtime_binary_version(&self) -> Option<String> {
        if !self.config.runtime_path.exists() {
            return None;
        }

        let output = Command::new(&self.config.runtime_path)
            .arg("--version")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }

        String::from_utf8(output.stdout)
            .ok()?
            .lines()
            .find(|line| !line.trim().is_empty())
            .map(|line| line.trim().to_string())
    }

    fn runtime_readiness(&self) -> (bool, String, String) {
        let path = &self.config.runtime_path;
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(_) => {
                return (
                    false,
                    "RuntimeBinaryMissing".to_string(),
                    format!("runtime binary does not exist at {}", path.display()),
                );
            }
        };

        if !metadata.is_file() {
            return (
                false,
                "RuntimeBinaryInvalid".to_string(),
                format!("runtime path is not a regular file: {}", path.display()),
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return (
                    false,
                    "RuntimeBinaryNotExecutable".to_string(),
                    format!("runtime binary is not executable: {}", path.display()),
                );
            }
        }

        let version = self
            .runtime_binary_version()
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        (
            true,
            "RuntimeIsReady".to_string(),
            format!(
                "runtime binary is available at {} ({})",
                path.display(),
                version
            ),
        )
    }

    fn cni_plugin_types(config: &serde_json::Value) -> Vec<String> {
        let mut plugin_types = Vec::new();
        if let Some(plugin_type) = config.get("type").and_then(|value| value.as_str()) {
            if !plugin_type.trim().is_empty() {
                plugin_types.push(plugin_type.to_string());
            }
        }
        if let Some(plugins) = config.get("plugins").and_then(|value| value.as_array()) {
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

    fn path_is_executable(path: &Path) -> bool {
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

    fn runtime_feature_flags(&self) -> serde_json::Value {
        json!({
            "exec": true,
            "execSync": true,
            "attach": true,
            "portForward": true,
            "containerStats": true,
            "podSandboxStats": true,
            "podSandboxMetrics": true,
            "containerEvents": true,
            "reopenContainerLog": true,
            "updateContainerResources": true,
            "checkpointContainer": true,
        })
    }

    fn network_health(&self) -> (bool, String, String) {
        let config_dirs = self.config.cni_config.config_dirs();
        let plugin_dirs = self.config.cni_config.plugin_dirs();
        let mut discovered_files = Vec::new();
        let mut invalid_files = Vec::new();
        let mut declared_plugins = HashSet::new();

        for dir in config_dirs {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let matches = path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| matches!(ext, "conf" | "conflist" | "json"))
                    .unwrap_or(false);
                if !matches {
                    continue;
                }

                discovered_files.push(path.display().to_string());
                match std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                {
                    Some(config) => {
                        for plugin_type in Self::cni_plugin_types(&config) {
                            declared_plugins.insert(plugin_type);
                        }
                    }
                    None => invalid_files.push(path.display().to_string()),
                }
            }
        }

        if discovered_files.is_empty() {
            (
                false,
                "CNIConfigMissing".to_string(),
                "no CNI config file found in configured CNI config directories".to_string(),
            )
        } else if !invalid_files.is_empty() && declared_plugins.is_empty() {
            (
                false,
                "CNIConfigInvalid".to_string(),
                format!(
                    "failed to parse {} CNI config file(s): {}",
                    invalid_files.len(),
                    invalid_files.join(", ")
                ),
            )
        } else if !declared_plugins.is_empty() {
            let missing_plugins: Vec<String> = declared_plugins
                .iter()
                .filter(|plugin_type| {
                    !plugin_dirs
                        .iter()
                        .any(|dir| Self::path_is_executable(&dir.join(plugin_type.as_str())))
                })
                .cloned()
                .collect();
            if !missing_plugins.is_empty() {
                (
                    false,
                    "CNIPluginMissing".to_string(),
                    format!(
                        "missing or non-executable CNI plugin binary/binaries: {}",
                        missing_plugins.join(", ")
                    ),
                )
            } else {
                (
                    true,
                    "CNINetworkConfigReady".to_string(),
                    format!(
                        "discovered {} CNI config file(s) and {} plugin type(s)",
                        discovered_files.len(),
                        declared_plugins.len()
                    ),
                )
            }
        } else {
            (
                true,
                "CNINetworkConfigReady".to_string(),
                format!("discovered {} CNI config file(s)", discovered_files.len()),
            )
        }
    }

    async fn runtime_container_status_checked(&self, container_id: &str) -> ContainerStatus {
        let runtime = self.runtime.clone();
        let container_id = container_id.to_string();
        tokio::task::spawn_blocking(move || runtime.container_status(&container_id))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(ContainerStatus::Unknown)
    }

    fn runtime_container_status_name(status: &ContainerStatus) -> &'static str {
        match status {
            ContainerStatus::Created => "created",
            ContainerStatus::Running => "running",
            ContainerStatus::Stopped(_) => "stopped",
            ContainerStatus::Unknown => "unknown",
        }
    }

    async fn ensure_container_is_streamable(
        &self,
        container_id: &str,
        operation: &str,
    ) -> Result<(), Status> {
        let runtime_status = self.runtime_container_status_checked(container_id).await;
        if matches!(
            runtime_status,
            ContainerStatus::Created | ContainerStatus::Running
        ) {
            return Ok(());
        }

        Err(Status::failed_precondition(format!(
            "container {} is not in a streamable state for {}: current runtime state is {}",
            container_id,
            operation,
            Self::runtime_container_status_name(&runtime_status)
        )))
    }

    fn attach_socket_path(&self, container_id: &str) -> PathBuf {
        self.shim_work_dir.join(container_id).join("attach.sock")
    }

    async fn runtime_container_pid_checked(&self, container_id: &str) -> Option<i32> {
        let runtime = self.runtime.clone();
        let container_id = container_id.to_string();
        tokio::task::spawn_blocking(move || runtime.container_pid(&container_id))
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }

    async fn runtime_cgroup_hint_checked(&self, container_id: &str) -> Option<PathBuf> {
        let pid = self.runtime_container_pid_checked(container_id).await?;
        let raw = tokio::fs::read_to_string(format!("/proc/{}/cgroup", pid))
            .await
            .ok()?;

        Self::parse_cgroup_hint_from_procfs(&raw)
    }

    fn parse_cgroup_hint_from_procfs(raw: &str) -> Option<PathBuf> {
        for line in raw.lines() {
            let mut parts = line.splitn(3, ':');
            let _hierarchy = parts.next();
            let controllers = parts.next().unwrap_or_default();
            let path = parts.next().unwrap_or_default();
            if path.is_empty() {
                continue;
            }

            if controllers.is_empty()
                || controllers
                    .split(',')
                    .any(|controller| matches!(controller, "cpu" | "cpuacct" | "memory" | "pids"))
            {
                return Some(PathBuf::from(path));
            }
        }

        None
    }

    async fn container_cgroup_hint(&self, container_id: &str, container: &Container) -> PathBuf {
        if let Some(hint) = self.runtime_cgroup_hint_checked(container_id).await {
            return hint;
        }

        Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        )
        .as_ref()
        .and_then(|state| state.cgroup_parent.as_ref())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup"))
    }

    fn collect_path_usage(path: &Path) -> std::io::Result<(u64, u64)> {
        if !path.exists() {
            return Ok((0, 0));
        }

        let mut used_bytes = 0u64;
        let mut inodes_used = 0u64;
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            inodes_used += 1;
            if metadata.is_dir() {
                let (child_bytes, child_inodes) = Self::collect_path_usage(&entry.path())?;
                used_bytes = used_bytes.saturating_add(child_bytes);
                inodes_used = inodes_used.saturating_add(child_inodes);
            } else {
                used_bytes = used_bytes.saturating_add(metadata.len());
            }
        }

        Ok((used_bytes, inodes_used))
    }

    fn container_writable_layer_usage(
        &self,
        container_id: &str,
    ) -> Option<crate::proto::runtime::v1::FilesystemUsage> {
        use crate::proto::runtime::v1::{FilesystemIdentifier, FilesystemUsage, UInt64Value};

        let rootfs_path = self
            .config
            .root_dir
            .join("containers")
            .join(container_id)
            .join("rootfs");
        let (used_bytes, inodes_used) = Self::collect_path_usage(&rootfs_path).ok()?;

        Some(FilesystemUsage {
            timestamp: Self::now_nanos(),
            fs_id: Some(FilesystemIdentifier {
                mountpoint: rootfs_path.display().to_string(),
            }),
            used_bytes: Some(UInt64Value { value: used_bytes }),
            inodes_used: Some(UInt64Value { value: inodes_used }),
        })
    }

    async fn container_network_stats(
        &self,
        container_id: &str,
    ) -> Option<crate::metrics::NetworkStats> {
        let pid = self.runtime_container_pid_checked(container_id).await?;
        let contents = tokio::fs::read_to_string(format!("/proc/{}/net/dev", pid))
            .await
            .ok()?;
        Self::parse_network_stats_from_procfs(&contents)
    }

    fn parse_network_stats_from_procfs(contents: &str) -> Option<crate::metrics::NetworkStats> {
        let mut aggregated = crate::metrics::NetworkStats {
            name: "pod".to_string(),
            ..Default::default()
        };
        let mut saw_interface = false;

        for line in contents.lines().skip(2) {
            let Some((iface, payload)) = line.split_once(':') else {
                continue;
            };
            if iface.trim() == "lo" {
                continue;
            }

            let values: Vec<u64> = payload
                .split_whitespace()
                .filter_map(|value| value.parse::<u64>().ok())
                .collect();
            if values.len() < 16 {
                continue;
            }

            saw_interface = true;
            aggregated.rx_bytes = aggregated.rx_bytes.saturating_add(values[0]);
            aggregated.rx_packets = aggregated.rx_packets.saturating_add(values[1]);
            aggregated.rx_errors = aggregated.rx_errors.saturating_add(values[2]);
            aggregated.rx_dropped = aggregated.rx_dropped.saturating_add(values[3]);
            aggregated.tx_bytes = aggregated.tx_bytes.saturating_add(values[8]);
            aggregated.tx_packets = aggregated.tx_packets.saturating_add(values[9]);
            aggregated.tx_errors = aggregated.tx_errors.saturating_add(values[10]);
            aggregated.tx_dropped = aggregated.tx_dropped.saturating_add(values[11]);
        }

        saw_interface.then_some(aggregated)
    }

    fn encode_info_payload(value: serde_json::Value) -> Result<HashMap<String, String>, Status> {
        let encoded = serde_json::to_string(&value)
            .map_err(|e| Status::internal(format!("Failed to encode info payload: {}", e)))?;
        Ok(HashMap::from([("info".to_string(), encoded)]))
    }

    fn runtime_spec_snapshot(&self, container_id: &str) -> Option<serde_json::Value> {
        let config_path = self.checkpoint_config_path(container_id);
        std::fs::read(&config_path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok())
    }

    async fn build_container_verbose_info(
        &self,
        container: &Container,
        runtime_state: i32,
    ) -> Result<HashMap<String, String>, Status> {
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let runtime_spec = self.runtime_spec_snapshot(&container.id);
        let payload = json!({
            "id": container.id.clone(),
            "sandboxID": container.pod_sandbox_id.clone(),
            "podSandboxId": container.pod_sandbox_id.clone(),
            "runtimeState": Self::runtime_state_name(runtime_state),
            "pid": self.runtime_container_pid_checked(&container.id).await,
            "runtimeSpec": runtime_spec,
            "privileged": container_state.as_ref().map(|state| state.privileged).unwrap_or(false),
            "logPath": container_state.as_ref().and_then(|state| state.log_path.clone()),
            "tty": container_state.as_ref().map(|state| state.tty).unwrap_or(false),
            "stdin": container_state.as_ref().map(|state| state.stdin).unwrap_or(false),
            "stdinOnce": container_state.as_ref().map(|state| state.stdin_once).unwrap_or(false),
            "readonlyRootfs": container_state
                .as_ref()
                .map(|state| state.readonly_rootfs)
                .unwrap_or(false),
            "networkNamespacePath": container_state
                .as_ref()
                .and_then(|state| state.network_namespace_path.clone()),
            "cgroupParent": container_state.as_ref().and_then(|state| state.cgroup_parent.clone()),
            "user": container_state.as_ref().and_then(|state| state.run_as_user.clone()),
            "runAsGroup": container_state.as_ref().and_then(|state| state.run_as_group),
            "supplementalGroups": container_state
                .as_ref()
                .map(|state| state.supplemental_groups.clone())
                .unwrap_or_default(),
            "apparmorProfile": container_state
                .as_ref()
                .and_then(|state| state.apparmor_profile.clone()),
            "noNewPrivileges": container_state
                .as_ref()
                .and_then(|state| state.no_new_privileges),
            "mounts": container_state
                .as_ref()
                .map(|state| {
                    state.mounts.iter().map(|mount| {
                        json!({
                            "containerPath": mount.container_path.clone(),
                            "hostPath": mount.host_path.clone(),
                            "readonly": mount.readonly,
                            "selinuxRelabel": mount.selinux_relabel,
                            "propagation": mount.propagation,
                        })
                    }).collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        });
        Self::encode_info_payload(payload)
    }

    async fn build_pod_verbose_info(
        &self,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> Result<HashMap<String, String>, Status> {
        let pod_state = Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        );
        let pause_container_id = pod_state
            .as_ref()
            .and_then(|state| state.pause_container_id.clone());
        let pause_pid = if let Some(pause_container_id) = pause_container_id.as_deref() {
            self.runtime_container_pid_checked(pause_container_id).await
        } else {
            None
        };
        let runtime_spec = pause_container_id
            .as_deref()
            .and_then(|pause_container_id| self.runtime_spec_snapshot(pause_container_id));
        let pause_image = pod_sandbox
            .annotations
            .get("io.kubernetes.cri.sandbox-image")
            .filter(|image| !image.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| self.config.pause_image.clone());
        let payload = json!({
            "id": pod_sandbox.id.clone(),
            "image": pause_image,
            "pid": pause_pid,
            "runtimeSpec": runtime_spec,
            "runtimeHandler": pod_sandbox.runtime_handler.clone(),
            "runtimePodCIDR": pod_state.as_ref().and_then(|state| state.runtime_pod_cidr.clone()),
            "netnsPath": pod_state.as_ref().and_then(|state| state.netns_path.clone()),
            "logDirectory": pod_state.as_ref().and_then(|state| state.log_directory.clone()),
            "pauseContainerId": pause_container_id,
            "ip": pod_state.as_ref().and_then(|state| state.ip.clone()),
            "additionalIPs": pod_state
                .as_ref()
                .map(|state| state.additional_ips.clone())
                .unwrap_or_default(),
            "cgroupParent": pod_state.as_ref().and_then(|state| state.cgroup_parent.clone()),
            "sysctls": pod_state
                .as_ref()
                .map(|state| state.sysctls.clone())
                .unwrap_or_default(),
            "privileged": pod_state.as_ref().map(|state| state.privileged).unwrap_or(false),
            "readonlyRootfs": pod_state
                .as_ref()
                .map(|state| state.readonly_rootfs)
                .unwrap_or(false),
            "selinuxLabel": pod_state.as_ref().and_then(|state| state.selinux_label.clone()),
            "seccompProfile": pod_state.as_ref().map(|state| json!({
                "profileType": state.seccomp_profile.as_ref().map(|profile| profile.profile_type),
                "localhostRef": state
                    .seccomp_profile
                    .as_ref()
                    .map(|profile| profile.localhost_ref.clone())
                    .unwrap_or_default(),
            })),
        });
        Self::encode_info_payload(payload)
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

    async fn resolve_pod_sandbox_id_if_exists(
        &self,
        requested_id: &str,
    ) -> Result<Option<String>, Status> {
        match self.resolve_pod_sandbox_id(requested_id).await {
            Ok(id) => Ok(Some(id)),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }

    async fn resolve_container_id_if_exists(
        &self,
        requested_id: &str,
    ) -> Result<Option<String>, Status> {
        match self.resolve_container_id(requested_id).await {
            Ok(id) => Ok(Some(id)),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }

    async fn resolve_container_id_for_filter(&self, requested_id: &str) -> Option<String> {
        self.resolve_container_id_if_exists(requested_id)
            .await
            .ok()
            .flatten()
    }

    async fn resolve_pod_sandbox_id_for_filter(&self, requested_id: &str) -> Option<String> {
        self.resolve_pod_sandbox_id_if_exists(requested_id)
            .await
            .ok()
            .flatten()
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

    fn id_matches_filter_value(actual: &str, requested: &str) -> bool {
        actual == requested || actual.starts_with(requested) || requested.starts_with(actual)
    }

    fn container_matches_stats_filter(
        container: &Container,
        filter: &crate::proto::runtime::v1::ContainerStatsFilter,
    ) -> bool {
        if !filter.id.is_empty()
            && !Self::id_matches_filter_value(container.id.as_str(), filter.id.as_str())
        {
            return false;
        }

        if !filter.pod_sandbox_id.is_empty()
            && !Self::id_matches_filter_value(
                container.pod_sandbox_id.as_str(),
                filter.pod_sandbox_id.as_str(),
            )
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

    fn pod_sandbox_matches_stats_filter(
        pod: &crate::proto::runtime::v1::PodSandbox,
        filter: &crate::proto::runtime::v1::PodSandboxStatsFilter,
    ) -> bool {
        if !filter.id.is_empty()
            && !Self::id_matches_filter_value(pod.id.as_str(), filter.id.as_str())
        {
            return false;
        }

        for (k, v) in &filter.label_selector {
            if pod.labels.get(k) != Some(v) {
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
        Self::new_with_nri_config(config, NriConfig::default())
    }

    pub fn new_with_nri_config(config: RuntimeConfig, nri_config: NriConfig) -> Self {
        Self::new_with_shim_work_dir(config, nri_config, default_shim_work_dir())
    }

    fn new_with_shim_work_dir(
        config: RuntimeConfig,
        nri_config: NriConfig,
        shim_work_dir: PathBuf,
    ) -> Self {
        let nri_manager_config = NriManagerConfig::from(nri_config.clone());
        let nri: Arc<dyn NriApi> = if nri_manager_config.enable {
            Arc::new(NriManager::new(nri_manager_config))
        } else {
            Arc::new(NopNri)
        };
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
        shim_config.work_dir = shim_work_dir;
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
        let resolved_shim_work_dir = shim_config.work_dir.clone();

        let runtime = RuncRuntime::with_shim_and_image_storage(
            config.runtime_path.clone(),
            config.runtime_root.clone(),
            config.root_dir.join("storage"),
            shim_config,
        );

        let pod_manager = PodSandboxManager::new(
            runtime.clone(),
            config.root_dir.join("pods"),
            config.pause_image.clone(),
            config.cni_config.clone(),
        );

        // 初始化持久化管理器
        let persistence_config = PersistenceConfig {
            db_path: config.root_dir.join("crius.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        };

        let persistence = PersistenceManager::new(persistence_config)
            .expect("Failed to create persistence manager");
        let (events, _) = tokio::sync::broadcast::channel(256);
        let runtime_network_config = Self::load_runtime_network_config(&config.root_dir)
            .unwrap_or_else(|e| {
                log::warn!(
                    "Failed to load runtime network config from {}: {}",
                    config.root_dir.display(),
                    e
                );
                None
            });

        let service = Self {
            containers: Arc::new(Mutex::new(HashMap::new())),
            pod_sandboxes: Arc::new(Mutex::new(HashMap::new())),
            config,
            nri_config,
            nri,
            runtime,
            pod_manager: tokio::sync::Mutex::new(pod_manager),
            persistence: Arc::new(Mutex::new(persistence)),
            streaming: Arc::new(Mutex::new(None)),
            events,
            shim_work_dir: resolved_shim_work_dir,
            runtime_network_config: Arc::new(Mutex::new(runtime_network_config)),
            exit_monitors: Arc::new(Mutex::new(HashSet::new())),
        };

        service
    }

    pub async fn initialize_nri(&self) -> Result<(), Status> {
        if !self.nri_config.enable {
            return Ok(());
        }
        self.nri
            .start()
            .await
            .map_err(|e| Status::internal(format!("Failed to start NRI: {}", e)))?;
        self.nri
            .synchronize()
            .await
            .map_err(|e| Status::internal(format!("Failed to synchronize NRI: {}", e)))
    }

    fn nri_pod_event(&self, pod_id: &str) -> NriPodEvent {
        NriPodEvent {
            pod_id: pod_id.to_string(),
        }
    }

    fn nri_container_event(
        &self,
        pod_id: &str,
        container_id: &str,
        annotations: &HashMap<String, String>,
    ) -> NriContainerEvent {
        NriContainerEvent {
            pod_id: pod_id.to_string(),
            container_id: container_id.to_string(),
            annotations: Self::external_annotations(annotations),
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

    fn maybe_start_exit_monitor_task(
        monitor_key: String,
        container_id: String,
        exit_code_path: PathBuf,
        config: RuntimeConfig,
        containers: Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: Arc<Mutex<PersistenceManager>>,
        events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
        exit_monitors: Arc<Mutex<HashSet<String>>>,
    ) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let exit_code = loop {
                let current_state = {
                    let containers = containers.lock().await;
                    containers
                        .get(&container_id)
                        .map(|container| container.state)
                };

                match current_state {
                    Some(state)
                        if state == ContainerState::ContainerCreated as i32
                            || state == ContainerState::ContainerRunning as i32 => {}
                    _ => break None,
                }

                match tokio::fs::read_to_string(&exit_code_path).await {
                    Ok(raw) => match raw.trim().parse::<i32>() {
                        Ok(exit_code) => break Some(exit_code),
                        Err(err) => {
                            log::warn!(
                                "Ignoring invalid exit code file {} for container {}: {}",
                                exit_code_path.display(),
                                container_id,
                                err
                            );
                        }
                    },
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        log::debug!(
                            "Exit monitor could not read {} for {}: {}",
                            exit_code_path.display(),
                            container_id,
                            err
                        );
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };

            if let Some(exit_code) = exit_code {
                Self::record_container_exit_from_monitor(
                    &container_id,
                    exit_code,
                    &config,
                    &containers,
                    &pod_sandboxes,
                    &persistence,
                    &events,
                )
                .await;
            }

            let mut exit_monitors = exit_monitors.lock().await;
            exit_monitors.remove(&monitor_key);
        });
    }

    fn publish_event(&self, event: ContainerEventResponse) {
        if let Err(err) = self.events.send(event) {
            log::debug!("Dropping CRI event without subscribers: {}", err);
        }
    }

    fn exit_code_path(&self, container_id: &str) -> PathBuf {
        self.shim_work_dir.join(container_id).join("exit_code")
    }

    fn ensure_exit_monitor_registered(&self, container_id: &str) {
        let container_id = container_id.to_string();
        let monitor_key = format!("ctr:{}", container_id);
        let exit_code_path = self.exit_code_path(&container_id);
        let config = self.config.clone();
        let containers = self.containers.clone();
        let pod_sandboxes = self.pod_sandboxes.clone();
        let persistence = self.persistence.clone();
        let events = self.events.clone();
        let exit_monitors = self.exit_monitors.clone();

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let should_start = {
                let mut exit_monitors = exit_monitors.lock().await;
                exit_monitors.insert(monitor_key.clone())
            };

            if !should_start {
                return;
            }

            Self::maybe_start_exit_monitor_task(
                monitor_key,
                container_id,
                exit_code_path,
                config,
                containers,
                pod_sandboxes,
                persistence,
                events,
                exit_monitors,
            );
        });
    }

    fn ensure_pod_exit_monitor_registered(&self, pod_id: &str, pause_container_id: &str) {
        let pod_id = pod_id.to_string();
        let pause_container_id = pause_container_id.to_string();
        let monitor_key = format!("pod:{}", pod_id);
        let exit_code_path = self.exit_code_path(&pause_container_id);
        let config = self.config.clone();
        let containers = self.containers.clone();
        let pod_sandboxes = self.pod_sandboxes.clone();
        let persistence = self.persistence.clone();
        let events = self.events.clone();
        let exit_monitors = self.exit_monitors.clone();

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let should_start = {
                let mut exit_monitors = exit_monitors.lock().await;
                exit_monitors.insert(monitor_key.clone())
            };

            if !should_start {
                return;
            }

            let maybe_exit_code = loop {
                let current_state = {
                    let pod_sandboxes = pod_sandboxes.lock().await;
                    pod_sandboxes.get(&pod_id).map(|pod| pod.state)
                };

                match current_state {
                    Some(state) if state == PodSandboxState::SandboxReady as i32 => {}
                    _ => break None,
                }

                match tokio::fs::read_to_string(&exit_code_path).await {
                    Ok(raw) => match raw.trim().parse::<i32>() {
                        Ok(exit_code) => break Some(exit_code),
                        Err(err) => {
                            log::warn!(
                                "Ignoring invalid pause exit code file {} for pod {}: {}",
                                exit_code_path.display(),
                                pod_id,
                                err
                            );
                        }
                    },
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        log::debug!(
                            "Pause exit monitor could not read {} for pod {}: {}",
                            exit_code_path.display(),
                            pod_id,
                            err
                        );
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };

            if let Some(exit_code) = maybe_exit_code {
                Self::record_pod_exit_from_monitor(
                    &pod_id,
                    exit_code,
                    &config,
                    &containers,
                    &pod_sandboxes,
                    &persistence,
                    &events,
                )
                .await;
            }

            let mut exit_monitors = exit_monitors.lock().await;
            exit_monitors.remove(&monitor_key);
        });
    }

    async fn ensure_exit_monitors_for_active_containers(&self) {
        let active_container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|container| {
                    container.state == ContainerState::ContainerCreated as i32
                        || container.state == ContainerState::ContainerRunning as i32
                })
                .map(|container| container.id.clone())
                .collect()
        };

        for container_id in active_container_ids {
            self.ensure_exit_monitor_registered(&container_id);
        }

        let active_pods: Vec<(String, String)> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .values()
                .filter(|pod| pod.state == PodSandboxState::SandboxReady as i32)
                .filter_map(|pod| {
                    Self::read_internal_state::<StoredPodState>(
                        &pod.annotations,
                        INTERNAL_POD_STATE_KEY,
                    )
                    .and_then(|state| state.pause_container_id)
                    .filter(|pause_container_id| !pause_container_id.is_empty())
                    .map(|pause_container_id| (pod.id.clone(), pause_container_id))
                })
                .collect()
        };

        for (pod_id, pause_container_id) in active_pods {
            self.ensure_pod_exit_monitor_registered(&pod_id, &pause_container_id);
        }
    }

    async fn cleanup_orphaned_shim_artifacts(&self) {
        let known_container_ids: HashSet<String> =
            self.containers.lock().await.keys().cloned().collect();
        let known_pause_ids: HashSet<String> = self
            .pod_sandboxes
            .lock()
            .await
            .values()
            .filter_map(|pod| {
                Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .and_then(|state| state.pause_container_id)
            })
            .collect();

        let Ok(entries) = std::fs::read_dir(&self.shim_work_dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(id) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            if known_container_ids.contains(id) || known_pause_ids.contains(id) {
                continue;
            }

            let metadata = std::fs::read(path.join("shim.json"))
                .ok()
                .and_then(|raw| serde_json::from_slice::<ShimProcess>(&raw).ok());
            let live_process = metadata
                .as_ref()
                .map(|process| {
                    PathBuf::from("/proc")
                        .join(process.shim_pid.to_string())
                        .exists()
                })
                .unwrap_or(false);

            if live_process {
                log::warn!(
                    "Keeping orphaned shim directory {} because shim pid {} still appears live",
                    path.display(),
                    metadata
                        .as_ref()
                        .map(|process| process.shim_pid)
                        .unwrap_or_default()
                );
                continue;
            }

            if let Err(err) = std::fs::remove_dir_all(&path) {
                log::warn!(
                    "Failed to remove orphaned shim directory {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    #[allow(dead_code)]
    async fn current_container_snapshot(&self, container_id: &str) -> Option<CriContainerStatus> {
        let container = {
            let containers = self.containers.lock().await;
            containers.get(container_id).cloned()
        }?;
        let runtime_state = Self::map_runtime_container_state(
            self.runtime_container_status_checked(container_id).await,
        );
        Some(Self::build_container_status_snapshot(
            &container,
            runtime_state,
        ))
    }

    async fn current_pod_status_snapshot(&self, pod_id: &str) -> Option<PodSandboxStatus> {
        let pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(pod_id).cloned()
        }?;
        Some(self.build_pod_sandbox_status_snapshot(&pod))
    }

    async fn current_pod_container_snapshots(&self, pod_id: &str) -> Vec<CriContainerStatus> {
        let containers: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|container| container.pod_sandbox_id == pod_id)
                .cloned()
                .collect()
        };

        let mut snapshots = Vec::with_capacity(containers.len());
        for container in containers {
            let runtime_state = Self::map_runtime_container_state(
                self.runtime_container_status_checked(&container.id).await,
            );
            snapshots.push(Self::build_container_status_snapshot(
                &container,
                runtime_state,
            ));
        }
        snapshots
    }

    fn pod_event_container_id(pod_sandbox: &crate::proto::runtime::v1::PodSandbox) -> String {
        Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        )
        .and_then(|state| state.pause_container_id)
        .filter(|container_id| !container_id.is_empty())
        .unwrap_or_else(|| pod_sandbox.id.clone())
    }

    async fn emit_container_event(
        &self,
        event_type: ContainerEventType,
        container: &Container,
        runtime_state: Option<i32>,
    ) {
        let pod_status = self
            .current_pod_status_snapshot(&container.pod_sandbox_id)
            .await;
        let snapshot = Self::build_container_status_snapshot(
            container,
            runtime_state.unwrap_or_else(|| container.state),
        );
        self.publish_event(ContainerEventResponse {
            container_id: container.id.clone(),
            container_event_type: event_type as i32,
            created_at: Self::now_nanos(),
            pod_sandbox_status: pod_status,
            containers_statuses: vec![snapshot],
        });
    }

    async fn emit_pod_event(
        &self,
        event_type: ContainerEventType,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
        containers_statuses: Vec<CriContainerStatus>,
    ) {
        self.publish_event(ContainerEventResponse {
            container_id: Self::pod_event_container_id(pod_sandbox),
            container_event_type: event_type as i32,
            created_at: Self::now_nanos(),
            pod_sandbox_status: Some(self.build_pod_sandbox_status_snapshot(pod_sandbox)),
            containers_statuses,
        });
    }

    fn publish_event_via_sender(
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
        event: ContainerEventResponse,
    ) {
        if let Err(err) = events.send(event) {
            log::debug!("Dropping CRI event without subscribers: {}", err);
        }
    }

    async fn record_container_exit_from_monitor(
        container_id: &str,
        exit_code: i32,
        config: &RuntimeConfig,
        containers: &Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: &Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: &Arc<Mutex<PersistenceManager>>,
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
    ) {
        let now = Self::now_nanos();
        let updated_container = {
            let mut containers = containers.lock().await;
            let Some(container) = containers.get_mut(container_id) else {
                return;
            };

            if container.state == ContainerState::ContainerExited as i32 {
                return;
            }

            container.state = ContainerState::ContainerExited as i32;
            if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            ) {
                state.finished_at.get_or_insert(now);
                state.exit_code = Some(exit_code);
                let _ = Self::insert_internal_state(
                    &mut container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                );
            }

            container.clone()
        };

        {
            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence.update_container_state(
                container_id,
                crate::runtime::ContainerStatus::Stopped(exit_code),
            ) {
                log::warn!(
                    "Exit monitor failed to persist container {} state: {}",
                    container_id,
                    err
                );
            }
        }

        let mut updated_pod = None;
        {
            let mut pod_sandboxes = pod_sandboxes.lock().await;
            if let Some(pod) = pod_sandboxes.get_mut(&updated_container.pod_sandbox_id) {
                let is_pause_container = Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .and_then(|state| state.pause_container_id)
                .as_deref()
                    == Some(container_id);

                if is_pause_container && pod.state != PodSandboxState::SandboxNotready as i32 {
                    pod.state = PodSandboxState::SandboxNotready as i32;
                    updated_pod = Some(pod.clone());
                }
            }
        }

        if updated_pod.is_some() {
            let mut persistence = persistence.lock().await;
            if let Err(err) =
                persistence.update_pod_state(&updated_container.pod_sandbox_id, "notready")
            {
                log::warn!(
                    "Exit monitor failed to persist pod {} state: {}",
                    updated_container.pod_sandbox_id,
                    err
                );
            }
        }

        let pod_status = {
            let pod_sandboxes = pod_sandboxes.lock().await;
            pod_sandboxes
                .get(&updated_container.pod_sandbox_id)
                .cloned()
        }
        .map(|pod| Self::build_pod_sandbox_status_snapshot_with_config(config, &pod));

        let snapshot = Self::build_container_status_snapshot(
            &updated_container,
            ContainerState::ContainerExited as i32,
        );
        Self::publish_event_via_sender(
            events,
            ContainerEventResponse {
                container_id: updated_container.id.clone(),
                container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                created_at: now,
                pod_sandbox_status: pod_status,
                containers_statuses: vec![snapshot],
            },
        );

        if let Some(updated_pod) = updated_pod {
            let container_snapshots = {
                let containers = containers.lock().await;
                containers
                    .values()
                    .filter(|container| container.pod_sandbox_id == updated_pod.id)
                    .cloned()
                    .map(|container| {
                        Self::build_container_status_snapshot(&container, container.state)
                    })
                    .collect::<Vec<_>>()
            };

            Self::publish_event_via_sender(
                events,
                ContainerEventResponse {
                    container_id: Self::pod_event_container_id(&updated_pod),
                    container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                    created_at: now,
                    pod_sandbox_status: Some(Self::build_pod_sandbox_status_snapshot_with_config(
                        config,
                        &updated_pod,
                    )),
                    containers_statuses: container_snapshots,
                },
            );
        }
    }

    async fn record_pod_exit_from_monitor(
        pod_id: &str,
        _exit_code: i32,
        config: &RuntimeConfig,
        containers: &Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: &Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: &Arc<Mutex<PersistenceManager>>,
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
    ) {
        let now = Self::now_nanos();
        let updated_pod = {
            let mut pod_sandboxes = pod_sandboxes.lock().await;
            let Some(pod) = pod_sandboxes.get_mut(pod_id) else {
                return;
            };

            if pod.state == PodSandboxState::SandboxNotready as i32 {
                return;
            }

            pod.state = PodSandboxState::SandboxNotready as i32;
            pod.clone()
        };

        {
            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence.update_pod_state(pod_id, "notready") {
                log::warn!(
                    "Pause exit monitor failed to persist pod {} state: {}",
                    pod_id,
                    err
                );
            }
        }

        let container_snapshots = {
            let containers = containers.lock().await;
            containers
                .values()
                .filter(|container| container.pod_sandbox_id == pod_id)
                .cloned()
                .map(|container| Self::build_container_status_snapshot(&container, container.state))
                .collect::<Vec<_>>()
        };

        Self::publish_event_via_sender(
            events,
            ContainerEventResponse {
                container_id: Self::pod_event_container_id(&updated_pod),
                container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                created_at: now,
                pod_sandbox_status: Some(Self::build_pod_sandbox_status_snapshot_with_config(
                    config,
                    &updated_pod,
                )),
                containers_statuses: container_snapshots,
            },
        );
    }

    fn build_pod_sandbox_status_snapshot_with_config(
        config: &RuntimeConfig,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> PodSandboxStatus {
        let pod_state = Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        );

        PodSandboxStatus {
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
                    .unwrap_or_else(|| config.runtime.clone());
                if config
                    .runtime_handlers
                    .iter()
                    .any(|handler| handler == &restored)
                {
                    restored
                } else {
                    config.runtime.clone()
                }
            } else {
                pod_sandbox.runtime_handler.clone()
            },
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    async fn refresh_runtime_state_and_publish_events(
        runtime: &RuncRuntime,
        config: &RuntimeConfig,
        containers: &Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: &Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: &Arc<Mutex<PersistenceManager>>,
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
    ) {
        let container_ids: Vec<String> = {
            let containers = containers.lock().await;
            containers.keys().cloned().collect()
        };

        for container_id in container_ids {
            let runtime_clone = runtime.clone();
            let container_id_for_status = container_id.clone();
            let runtime_status = match tokio::task::spawn_blocking(move || {
                runtime_clone.container_status(&container_id_for_status)
            })
            .await
            {
                Ok(Ok(status)) => status,
                Ok(Err(err)) => {
                    log::debug!(
                        "Background state refresh failed to read runtime status for {}: {}",
                        container_id,
                        err
                    );
                    continue;
                }
                Err(err) => {
                    log::debug!(
                        "Background state refresh join error for {}: {}",
                        container_id,
                        err
                    );
                    continue;
                }
            };

            if matches!(runtime_status, ContainerStatus::Unknown) {
                continue;
            }

            let next_state = Self::map_runtime_container_state(runtime_status.clone());
            let mut previous_container = None;
            let mut next_container = None;
            let mut emitted_event = None;
            {
                let mut containers = containers.lock().await;
                if let Some(container) = containers.get_mut(&container_id) {
                    if container.state == next_state {
                        continue;
                    }
                    previous_container = Some(container.clone());
                    container.state = next_state;
                    if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    ) {
                        match runtime_status {
                            ContainerStatus::Running => {
                                state.started_at.get_or_insert(Self::now_nanos());
                                state.finished_at = None;
                                state.exit_code = None;
                            }
                            ContainerStatus::Stopped(code) => {
                                state.finished_at.get_or_insert(Self::now_nanos());
                                state.exit_code = Some(code);
                            }
                            ContainerStatus::Created => {}
                            ContainerStatus::Unknown => {}
                        }
                        let _ = Self::insert_internal_state(
                            &mut container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        );
                    }
                    next_container = Some(container.clone());
                    emitted_event = Some(match next_state {
                        x if x == ContainerState::ContainerRunning as i32 => {
                            ContainerEventType::ContainerStartedEvent
                        }
                        x if x == ContainerState::ContainerExited as i32 => {
                            ContainerEventType::ContainerStoppedEvent
                        }
                        x if x == ContainerState::ContainerCreated as i32 => {
                            ContainerEventType::ContainerCreatedEvent
                        }
                        _ => ContainerEventType::ContainerStoppedEvent,
                    });
                }
            }

            let Some(container_after_update) = next_container else {
                continue;
            };

            {
                let mut persistence = persistence.lock().await;
                if let Err(err) = persistence.update_container_state(
                    &container_id,
                    match runtime_status {
                        ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
                        ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
                        ContainerStatus::Stopped(code) => {
                            crate::runtime::ContainerStatus::Stopped(code)
                        }
                        ContainerStatus::Unknown => crate::runtime::ContainerStatus::Unknown,
                    },
                ) {
                    log::warn!(
                        "Background state refresh failed to persist container {}: {}",
                        container_id,
                        err
                    );
                }
            }

            if let Some(event_type) = emitted_event {
                let pod_status = {
                    let pod_sandboxes = pod_sandboxes.lock().await;
                    pod_sandboxes
                        .get(&container_after_update.pod_sandbox_id)
                        .cloned()
                        .map(|pod| {
                            Self::build_pod_sandbox_status_snapshot_with_config(config, &pod)
                        })
                };
                let snapshot =
                    Self::build_container_status_snapshot(&container_after_update, next_state);
                Self::publish_event_via_sender(
                    events,
                    ContainerEventResponse {
                        container_id: container_after_update.id.clone(),
                        container_event_type: event_type as i32,
                        created_at: Self::now_nanos(),
                        pod_sandbox_status: pod_status,
                        containers_statuses: vec![snapshot],
                    },
                );
            }

            let _ = previous_container;
        }

        let pod_ids: Vec<String> = {
            let pods = pod_sandboxes.lock().await;
            pods.keys().cloned().collect()
        };

        for pod_id in pod_ids {
            let current_pod = {
                let pods = pod_sandboxes.lock().await;
                pods.get(&pod_id).cloned()
            };
            let Some(current_pod) = current_pod else {
                continue;
            };
            let pause_container_id = Self::read_internal_state::<StoredPodState>(
                &current_pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .and_then(|state| state.pause_container_id);
            let Some(pause_container_id) = pause_container_id else {
                continue;
            };

            let runtime_clone = runtime.clone();
            let pause_id_for_status = pause_container_id.clone();
            let pause_status = match tokio::task::spawn_blocking(move || {
                runtime_clone.container_status(&pause_id_for_status)
            })
            .await
            {
                Ok(Ok(status)) => status,
                _ => continue,
            };

            let next_pod_state = match pause_status {
                ContainerStatus::Running => PodSandboxState::SandboxReady as i32,
                _ => PodSandboxState::SandboxNotready as i32,
            };
            if next_pod_state == current_pod.state {
                continue;
            }

            let updated_pod = {
                let mut pods = pod_sandboxes.lock().await;
                if let Some(pod) = pods.get_mut(&pod_id) {
                    pod.state = next_pod_state;
                    Some(pod.clone())
                } else {
                    None
                }
            };
            let Some(updated_pod) = updated_pod else {
                continue;
            };

            {
                let mut persistence = persistence.lock().await;
                let target_state = if next_pod_state == PodSandboxState::SandboxReady as i32 {
                    "ready"
                } else {
                    "notready"
                };
                if let Err(err) = persistence.update_pod_state(&pod_id, target_state) {
                    log::warn!(
                        "Background state refresh failed to persist pod {}: {}",
                        pod_id,
                        err
                    );
                }
            }

            let container_snapshots = {
                let containers_snapshot: Vec<Container> = {
                    let containers = containers.lock().await;
                    containers
                        .values()
                        .filter(|container| container.pod_sandbox_id == pod_id)
                        .cloned()
                        .collect()
                };
                let mut snapshots = Vec::with_capacity(containers_snapshot.len());
                for container in containers_snapshot {
                    snapshots.push(Self::build_container_status_snapshot(
                        &container,
                        container.state,
                    ));
                }
                snapshots
            };

            Self::publish_event_via_sender(
                events,
                ContainerEventResponse {
                    container_id: Self::pod_event_container_id(&updated_pod),
                    container_event_type: if next_pod_state == PodSandboxState::SandboxReady as i32
                    {
                        ContainerEventType::ContainerStartedEvent as i32
                    } else {
                        ContainerEventType::ContainerStoppedEvent as i32
                    },
                    created_at: Self::now_nanos(),
                    pod_sandbox_status: Some(Self::build_pod_sandbox_status_snapshot_with_config(
                        config,
                        &updated_pod,
                    )),
                    containers_statuses: container_snapshots,
                },
            );
        }
    }

    async fn reconcile_recovered_state(&self) -> Result<(), Status> {
        let container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers.keys().cloned().collect()
        };

        for container_id in container_ids {
            let runtime_status = self.runtime_container_status_checked(&container_id).await;
            if matches!(runtime_status, ContainerStatus::Unknown) {
                continue;
            }

            let runtime_state = Self::map_runtime_container_state(runtime_status.clone());
            let persistence_status = match runtime_status {
                ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
                ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
                ContainerStatus::Stopped(code) => crate::runtime::ContainerStatus::Stopped(code),
                ContainerStatus::Unknown => crate::runtime::ContainerStatus::Unknown,
            };

            {
                let mut containers = self.containers.lock().await;
                if let Some(container) = containers.get_mut(&container_id) {
                    container.state = runtime_state;
                    if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    ) {
                        match &persistence_status {
                            crate::runtime::ContainerStatus::Running => {
                                state.started_at.get_or_insert(Self::now_nanos());
                                state.finished_at = None;
                                state.exit_code = None;
                            }
                            crate::runtime::ContainerStatus::Stopped(code) => {
                                state.finished_at.get_or_insert(Self::now_nanos());
                                state.exit_code = Some(*code);
                            }
                            _ => {}
                        }
                        let _ = Self::insert_internal_state(
                            &mut container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        );
                    }
                }
            }

            let mut persistence = self.persistence.lock().await;
            if let Err(e) = persistence.update_container_state(&container_id, persistence_status) {
                log::warn!(
                    "Failed to reconcile container {} state in database: {}",
                    container_id,
                    e
                );
            }
        }

        let pod_ids: Vec<String> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.keys().cloned().collect()
        };

        for pod_id in pod_ids {
            let pause_container_id = {
                let pod_sandboxes = self.pod_sandboxes.lock().await;
                pod_sandboxes.get(&pod_id).and_then(|pod| {
                    Self::read_internal_state::<StoredPodState>(
                        &pod.annotations,
                        INTERNAL_POD_STATE_KEY,
                    )
                    .and_then(|state| state.pause_container_id)
                })
            };

            let Some(pause_container_id) = pause_container_id else {
                continue;
            };

            let pause_status = self
                .runtime_container_status_checked(&pause_container_id)
                .await;
            let pod_has_required_netns = {
                let pod_sandboxes = self.pod_sandboxes.lock().await;
                pod_sandboxes
                    .get(&pod_id)
                    .map(Self::pod_has_required_netns)
                    .unwrap_or(true)
            };
            if matches!(pause_status, ContainerStatus::Running) && !pod_has_required_netns {
                log::warn!(
                    "Recovered pod {} pause container is running but network namespace is missing; marking sandbox NotReady",
                    pod_id
                );
            }
            let next_state = match pause_status {
                ContainerStatus::Running if pod_has_required_netns => {
                    PodSandboxState::SandboxReady as i32
                }
                _ => PodSandboxState::SandboxNotready as i32,
            };

            {
                let mut pod_sandboxes = self.pod_sandboxes.lock().await;
                if let Some(pod) = pod_sandboxes.get_mut(&pod_id) {
                    pod.state = next_state;
                }
            }

            let mut persistence = self.persistence.lock().await;
            if let Err(e) = persistence.update_pod_state(
                &pod_id,
                if next_state == PodSandboxState::SandboxReady as i32 {
                    "ready"
                } else {
                    "notready"
                },
            ) {
                log::warn!(
                    "Failed to reconcile pod {} state in database: {}",
                    pod_id,
                    e
                );
            }
        }

        Ok(())
    }

    async fn best_effort_refresh_runtime_state(&self) {
        if let Err(err) = self.reconcile_recovered_state().await {
            log::warn!("Best-effort runtime state refresh failed: {}", err);
        }
    }

    /// 从持久化存储恢复状态
    pub async fn recover_state(&self) -> Result<(), Status> {
        let (recovered_containers, recovered_pods) = {
            let persistence = self.persistence.lock().await;
            (persistence.recover_containers(), persistence.recover_pods())
        };

        // 恢复容器状态
        match recovered_containers {
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
                        pod_sandbox_id: record.pod_id.clone(),
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
        match recovered_pods {
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

        self.reconcile_recovered_state().await?;
        self.ensure_exit_monitors_for_active_containers().await;
        self.cleanup_orphaned_shim_artifacts().await;
        Ok(())
    }

    /// 将内部 ContainerStats 转换为 proto 格式
    fn convert_to_proto_container_stats(
        &self,
        stats: crate::metrics::ContainerStats,
    ) -> crate::proto::runtime::v1::ContainerStats {
        use crate::proto::runtime::v1::{
            ContainerStats, CpuUsage, MemoryUsage, SwapUsage, UInt64Value,
        };

        let writable_layer = self.container_writable_layer_usage(&stats.container_id);
        let swap = stats.memory.as_ref().map(|mem| SwapUsage {
            timestamp: stats.timestamp as i64,
            swap_available_bytes: None,
            swap_usage_bytes: Some(UInt64Value { value: mem.swap }),
        });

        ContainerStats {
            attributes: Some(crate::proto::runtime::v1::ContainerAttributes {
                id: stats.container_id.clone(),
                metadata: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
            }),
            cpu: stats.cpu.map(|cpu| CpuUsage {
                timestamp: stats.timestamp as i64,
                usage_core_nano_seconds: Some(UInt64Value {
                    value: cpu.usage_total,
                }),
                usage_nano_cores: Some(UInt64Value {
                    value: cpu.usage_user.saturating_add(cpu.usage_kernel),
                }),
            }),
            memory: stats.memory.map(|mem| MemoryUsage {
                timestamp: stats.timestamp as i64,
                working_set_bytes: Some(UInt64Value { value: mem.usage }),
                available_bytes: Some(UInt64Value {
                    value: mem.limit.saturating_sub(mem.usage),
                }),
                usage_bytes: Some(UInt64Value { value: mem.usage }),
                rss_bytes: Some(UInt64Value { value: mem.rss }),
                page_faults: Some(UInt64Value { value: mem.pgfault }),
                major_page_faults: Some(UInt64Value {
                    value: mem.pgmajfault,
                }),
            }),
            writable_layer,
            swap,
        }
    }

    fn populate_container_stats_attributes(
        proto_stats: &mut crate::proto::runtime::v1::ContainerStats,
        container: &Container,
    ) {
        if let Some(attributes) = proto_stats.attributes.as_mut() {
            attributes.metadata = container.metadata.clone();
            attributes.labels = container.labels.clone();
            attributes.annotations = Self::external_annotations(&container.annotations);
        }
    }

    /// 辅助方法：收集 Pod 的统计信息
    async fn collect_pod_stats(
        &self,
        pod_id: &str,
        pod: &crate::proto::runtime::v1::PodSandbox,
    ) -> Option<crate::proto::runtime::v1::PodSandboxStats> {
        use crate::proto::runtime::v1::{
            CpuUsage, LinuxPodSandboxStats, MemoryUsage, NetworkInterfaceUsage, NetworkUsage,
            PodSandboxAttributes, PodSandboxStats, ProcessUsage, UInt64Value,
        };

        let containers = self.containers.lock().await;

        // 收集该 Pod 下所有容器的统计
        let mut total_cpu_usage = 0u64;
        let mut total_memory_usage = 0u64;
        let mut total_memory_limit = 0u64;
        let mut total_pids = 0u64;
        let mut total_rx_bytes = 0u64;
        let mut total_rx_errors = 0u64;
        let mut total_tx_bytes = 0u64;
        let mut total_tx_errors = 0u64;
        let mut has_stats = false;
        let mut container_stats_list = Vec::new();

        let collector = MetricsCollector::new().ok()?;

        // 获取 pod 的 UID 用于匹配容器
        let pod_uid = pod.metadata.as_ref().map(|m| m.uid.clone());

        for (container_id, container) in containers.iter() {
            // 检查容器是否属于该 Pod
            let belongs_to_pod = container.pod_sandbox_id == pod_id
                || pod_uid
                    .as_ref()
                    .and_then(|uid| {
                        container
                            .annotations
                            .get("io.kubernetes.pod.uid")
                            .map(|container_uid| container_uid == uid)
                    })
                    .unwrap_or(false);

            if belongs_to_pod {
                let cgroup_parent = self.container_cgroup_hint(container_id, container).await;

                if let Ok(stats) = collector.collect_container_stats(container_id, &cgroup_parent) {
                    if let Some(ref cpu) = stats.cpu {
                        total_cpu_usage += cpu.usage_total;
                    }
                    if let Some(ref mem) = stats.memory {
                        total_memory_usage += mem.usage;
                        total_memory_limit += mem.limit;
                    }
                    if let Some(ref pids) = stats.pids {
                        total_pids += pids.current;
                    }
                    if let Some(network) = self.container_network_stats(container_id).await {
                        total_rx_bytes = total_rx_bytes.saturating_add(network.rx_bytes);
                        total_rx_errors = total_rx_errors.saturating_add(network.rx_errors);
                        total_tx_bytes = total_tx_bytes.saturating_add(network.tx_bytes);
                        total_tx_errors = total_tx_errors.saturating_add(network.tx_errors);
                    }
                    has_stats = true;

                    let mut proto_stats = self.convert_to_proto_container_stats(stats);
                    Self::populate_container_stats_attributes(&mut proto_stats, container);
                    container_stats_list.push(proto_stats);
                }
            }
        }

        if !has_stats {
            return None;
        }

        if let Some(pause_container_id) =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY)
                .and_then(|state| state.pause_container_id)
        {
            if let Some(network) = self.container_network_stats(&pause_container_id).await {
                total_rx_bytes = network.rx_bytes;
                total_rx_errors = network.rx_errors;
                total_tx_bytes = network.tx_bytes;
                total_tx_errors = network.tx_errors;
            }
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let container_count = container_stats_list.len() as u64;

        Some(PodSandboxStats {
            attributes: Some(PodSandboxAttributes {
                id: pod_id.to_string(),
                metadata: pod.metadata.clone(),
                labels: pod.labels.clone(),
                annotations: Self::external_annotations(&pod.annotations),
            }),
            linux: Some(LinuxPodSandboxStats {
                cpu: Some(CpuUsage {
                    timestamp,
                    usage_core_nano_seconds: Some(UInt64Value {
                        value: total_cpu_usage,
                    }),
                    usage_nano_cores: Some(UInt64Value {
                        value: total_cpu_usage,
                    }),
                }),
                memory: Some(MemoryUsage {
                    timestamp,
                    working_set_bytes: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                    available_bytes: Some(UInt64Value {
                        value: total_memory_limit.saturating_sub(total_memory_usage),
                    }),
                    usage_bytes: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                    rss_bytes: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                    page_faults: Some(UInt64Value { value: 0 }),
                    major_page_faults: Some(UInt64Value { value: 0 }),
                }),
                containers: container_stats_list,
                network: Some(NetworkUsage {
                    timestamp,
                    default_interface: Some(NetworkInterfaceUsage {
                        name: "pod".to_string(),
                        rx_bytes: Some(UInt64Value {
                            value: total_rx_bytes,
                        }),
                        rx_errors: Some(UInt64Value {
                            value: total_rx_errors,
                        }),
                        tx_bytes: Some(UInt64Value {
                            value: total_tx_bytes,
                        }),
                        tx_errors: Some(UInt64Value {
                            value: total_tx_errors,
                        }),
                    }),
                    interfaces: Vec::new(),
                }),
                process: Some(ProcessUsage {
                    timestamp,
                    process_count: Some(UInt64Value {
                        value: total_pids.max(container_count),
                    }),
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
        let runtime_version = self
            .runtime_binary_version()
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        Ok(Response::new(VersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            runtime_name: self.config.runtime.clone(),
            runtime_version,
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
            Self::legacy_linux_sandbox_seccomp_profile_path(sandbox_security),
        );
        let stored_seccomp_profile = Self::stored_seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            Self::legacy_linux_sandbox_seccomp_profile_path(sandbox_security),
        );
        let runtime_network_config = self.runtime_network_config.lock().await.clone();
        let runtime_pod_cidr = runtime_network_config
            .as_ref()
            .map(|cfg| cfg.pod_cidr.clone());

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
            network_config: runtime_network_config
                .as_ref()
                .map(|cfg| crate::pod::NetworkConfig {
                    network_namespace: format!(
                        "crius-{}-{}",
                        pod_config
                            .metadata
                            .as_ref()
                            .map(|m| m.namespace.as_str())
                            .unwrap_or("default"),
                        pod_config
                            .metadata
                            .as_ref()
                            .map(|m| m.name.as_str())
                            .unwrap_or("pod"),
                    ),
                    pod_cidr: cfg.pod_cidr.clone(),
                }),
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
                runtime_pod_cidr: runtime_pod_cidr.clone(),
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
                runtime_pod_cidr: runtime_pod_cidr,
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
        self.nri
            .run_pod_sandbox(self.nri_pod_event(&pod_id))
            .await
            .map_err(|e| Status::internal(format!("NRI RunPodSandbox failed: {}", e)))?;

        log::info!("Pod sandbox {} created successfully", pod_id);
        if let Some(pause_container_id) = pod_state.pause_container_id.as_deref() {
            self.ensure_pod_exit_monitor_registered(&pod_id, pause_container_id);
        }
        self.emit_pod_event(
            ContainerEventType::ContainerCreatedEvent,
            &pod_sandbox,
            Vec::new(),
        )
        .await;
        self.emit_pod_event(
            ContainerEventType::ContainerStartedEvent,
            &pod_sandbox,
            Vec::new(),
        )
        .await;
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
        let Some(pod_id) = self
            .resolve_pod_sandbox_id_if_exists(&req.pod_sandbox_id)
            .await?
        else {
            return Ok(Response::new(StopPodSandboxResponse {}));
        };

        log::info!("Stopping pod sandbox {}", pod_id);
        self.nri
            .stop_pod_sandbox(self.nri_pod_event(&pod_id))
            .await
            .map_err(|e| Status::internal(format!("NRI StopPodSandbox failed: {}", e)))?;

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
        let stopped_containers: Vec<Container> = {
            let containers = self.containers.lock().await;
            container_ids
                .iter()
                .filter_map(|container_id| containers.get(container_id).cloned())
                .collect()
        };
        for container in stopped_containers {
            self.emit_container_event(
                ContainerEventType::ContainerStoppedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        if let Some(pod) = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_id).cloned()
        } {
            let container_statuses = self.current_pod_container_snapshots(&pod_id).await;
            self.emit_pod_event(
                ContainerEventType::ContainerStoppedEvent,
                &pod,
                container_statuses,
            )
            .await;
        }
        Ok(Response::new(StopPodSandboxResponse {}))
    }

    // 获取容器状态
    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let actual_container_id = self.resolve_container_id(&req.container_id).await?;
        let container = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Container not found"))?;

        let runtime_state = Self::map_runtime_container_state(
            self.runtime_container_status_checked(&actual_container_id)
                .await,
        );
        let status = Self::build_container_status_snapshot(&container, runtime_state);
        let info = if req.verbose {
            self.build_container_verbose_info(&container, runtime_state)
                .await?
        } else {
            HashMap::new()
        };

        Ok(Response::new(ContainerStatusResponse {
            status: Some(status),
            info,
        }))
    }

    // 列出容器
    async fn list_containers(
        &self,
        request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_container_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListContainersResponse {
                        containers: Vec::new(),
                    }));
                };
                filter.id = resolved_id;
            }

            if !filter.pod_sandbox_id.is_empty() {
                let Some(resolved_pod_id) = self
                    .resolve_pod_sandbox_id_for_filter(&filter.pod_sandbox_id)
                    .await
                else {
                    return Ok(Response::new(ListContainersResponse {
                        containers: Vec::new(),
                    }));
                };
                filter.pod_sandbox_id = resolved_pod_id;
            }

            Some(filter)
        } else {
            None
        };
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
        self.ensure_container_is_streamable(&req.container_id, "exec")
            .await?;
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

        self.ensure_container_is_streamable(&container_id, "exec_sync")
            .await?;

        let mut command = TokioCommand::new(&self.config.runtime_path);
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
        request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        if req.port.iter().any(|port| *port <= 0 || *port > 65535) {
            return Err(Status::invalid_argument(
                "all forwarded ports must be in 1..=65535",
            ));
        }

        let pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
        if pod.state != PodSandboxState::SandboxReady as i32 {
            return Err(Status::failed_precondition(format!(
                "pod sandbox {} is not ready",
                pod_id
            )));
        }

        let netns_path =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY)
                .and_then(|state| state.netns_path)
                .or_else(|| {
                    self.pod_manager
                        .try_lock()
                        .ok()
                        .and_then(|manager| manager.get_pod_netns(&pod_id))
                        .map(|path| path.to_string_lossy().to_string())
                })
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "pod sandbox {} has no network namespace path",
                        pod_id
                    ))
                })?;

        if !std::path::Path::new(&netns_path).exists() {
            return Err(Status::failed_precondition(format!(
                "pod sandbox {} network namespace does not exist: {}",
                pod_id, netns_path
            )));
        }

        let streaming = self.get_streaming_server().await?;
        let response = streaming
            .get_port_forward(&req, PathBuf::from(netns_path))
            .await?;
        Ok(Response::new(response))
    }

    // 获取运行时状态
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let (runtime_ready, runtime_reason, runtime_message) = self.runtime_readiness();
        let (network_ready, network_reason, network_message) = self.network_health();
        let info = if req.verbose {
            let runtime_network_config = self.runtime_network_config.lock().await.clone();
            let payload = json!({
                "runtimeName": "crius",
                "runtimeVersion": self
                    .runtime_binary_version()
                    .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
                "runtimeApiVersion": "v1",
                "rootDir": self.config.root_dir.display().to_string(),
                "runtime": self.config.runtime.clone(),
                "runtimePath": self.config.runtime_path.display().to_string(),
                "runtimeRoot": self.config.runtime_root.display().to_string(),
                "logDir": self.config.log_dir.display().to_string(),
                "pauseImage": self.config.pause_image.clone(),
                "runtimeHandlers": self.config.runtime_handlers.clone(),
                "runtimeFeatures": self.runtime_feature_flags(),
                "runtimeNetworkConfig": runtime_network_config.as_ref().map(|cfg| {
                    json!({
                        "podCIDR": cfg.pod_cidr,
                    })
                }),
                "networkReady": network_ready,
                "networkReason": network_reason.clone(),
                "cgroupDriver": self.cgroup_driver().as_str_name(),
                "recovery": {
                    "enabled": true,
                    "startupReconcile": true,
                    "eventReplayOnRecovery": false,
                },
            });
            let mut info = HashMap::new();
            info.insert(
                "config".to_string(),
                serde_json::to_string(&payload).map_err(|e| {
                    Status::internal(format!("Failed to encode runtime config info: {}", e))
                })?,
            );
            info
        } else {
            HashMap::new()
        };

        Ok(Response::new(StatusResponse {
            status: Some(RuntimeStatus {
                conditions: vec![
                    RuntimeCondition {
                        r#type: "RuntimeReady".to_string(),
                        status: runtime_ready,
                        reason: runtime_reason,
                        message: runtime_message,
                    },
                    RuntimeCondition {
                        r#type: "NetworkReady".to_string(),
                        status: network_ready,
                        reason: network_reason,
                        message: network_message,
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
        let Some(pod_id) = self
            .resolve_pod_sandbox_id_if_exists(&req.pod_sandbox_id)
            .await?
        else {
            return Ok(Response::new(RemovePodSandboxResponse {}));
        };

        log::info!("Removing pod sandbox {}", pod_id);
        self.nri
            .remove_pod_sandbox(self.nri_pod_event(&pod_id))
            .await
            .map_err(|e| Status::internal(format!("NRI RemovePodSandbox failed: {}", e)))?;
        let existing_pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_id).cloned()
        };
        let existing_container_statuses = self.current_pod_container_snapshots(&pod_id).await;

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
        let removed_containers: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|c| c.pod_sandbox_id == pod_id)
                .cloned()
                .collect()
        };
        let container_ids: Vec<String> = removed_containers
            .iter()
            .map(|container| container.id.clone())
            .collect();

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
            let network_manager =
                DefaultNetworkManager::from_cni_config(self.config.cni_config.clone());
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
        for container in removed_containers {
            self.emit_container_event(
                ContainerEventType::ContainerDeletedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        if let Some(pod) = existing_pod {
            self.emit_pod_event(
                ContainerEventType::ContainerDeletedEvent,
                &pod,
                existing_container_statuses,
            )
            .await;
        }
        Ok(Response::new(RemovePodSandboxResponse {}))
    }

    // 获取pod_sandbox状态
    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let resolved_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let pod_sandbox = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&resolved_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
        let container_statuses = self.current_pod_container_snapshots(&resolved_id).await;
        let status = self.build_pod_sandbox_status_snapshot(&pod_sandbox);
        let info = if req.verbose {
            self.build_pod_verbose_info(&pod_sandbox).await?
        } else {
            HashMap::new()
        };

        Ok(Response::new(PodSandboxStatusResponse {
            status: Some(status),
            info,
            containers_statuses: container_statuses,
            timestamp: Self::now_nanos(),
        }))
    }

    // 列出pod_sandbox
    async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_pod_sandbox_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListPodSandboxResponse { items: Vec::new() }));
                };
                filter.id = resolved_id;
            }

            Some(filter)
        } else {
            None
        };
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
            Self::legacy_linux_container_apparmor_profile(security),
        );
        let selinux_label =
            Self::selinux_label_from_proto(security.and_then(|ctx| ctx.selinux_options.as_ref()));
        let seccomp_profile = Self::seccomp_profile_from_proto(
            security.and_then(|ctx| ctx.seccomp.as_ref()),
            Self::legacy_linux_container_seccomp_profile_path(security),
        );
        let checkpoint_location = config
            .image
            .as_ref()
            .map(|image| image.image.trim().to_string())
            .filter(|image| !image.is_empty())
            .and_then(|image| {
                let path = PathBuf::from(&image);
                path.exists().then_some(path)
            })
            .or_else(|| {
                config
                    .annotations
                    .get(CHECKPOINT_LOCATION_ANNOTATION_KEY)
                    .filter(|location| !location.trim().is_empty())
                    .map(PathBuf::from)
            });

        let checkpoint_restore = checkpoint_location
            .as_ref()
            .map(|checkpoint_location| {
                let checkpoint_image_path =
                    Self::checkpoint_runtime_image_path(checkpoint_location);
                let (manifest, oci_config) = Self::load_checkpoint_artifact(checkpoint_location)
                    .map_err(|e| {
                        Status::failed_precondition(format!(
                            "Failed to load checkpoint artifact {}: {}",
                            checkpoint_location.display(),
                            e
                        ))
                    })?;
                let image_ref = manifest
                    .get("imageRef")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
                    .or_else(|| {
                        config
                            .annotations
                            .get(CHECKPOINT_LOCATION_ANNOTATION_KEY)
                            .and_then(|_| {
                                config
                                    .image
                                    .as_ref()
                                    .map(|image| image.user_specified_image.clone())
                            })
                            .filter(|image| !image.is_empty())
                    })
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "Checkpoint artifact {} is missing imageRef",
                            checkpoint_location.display()
                        ))
                    })?;

                Ok::<StoredCheckpointRestore, Status>(StoredCheckpointRestore {
                    checkpoint_location: checkpoint_location.display().to_string(),
                    checkpoint_image_path: checkpoint_image_path.display().to_string(),
                    oci_config,
                    image_ref,
                })
            })
            .transpose()?;

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
        let container_image_ref = checkpoint_restore
            .as_ref()
            .map(|restore| restore.image_ref.clone())
            .or_else(|| config.image.as_ref().map(|image| image.image.clone()))
            .unwrap_or_default();
        let pod_resolv_path = self
            .config
            .root_dir
            .join("pods")
            .join(&pod_sandbox_id)
            .join("resolv.conf");
        let mut runtime_mounts: Vec<MountConfig> = config
            .mounts
            .iter()
            .map(|m| MountConfig {
                source: PathBuf::from(&m.host_path),
                destination: PathBuf::from(&m.container_path),
                read_only: m.readonly,
            })
            .collect();
        if pod_resolv_path.exists()
            && !runtime_mounts
                .iter()
                .any(|mount| mount.destination == Path::new("/etc/resolv.conf"))
        {
            runtime_mounts.push(MountConfig {
                source: pod_resolv_path.clone(),
                destination: PathBuf::from("/etc/resolv.conf"),
                read_only: true,
            });
        }
        let mut stored_annotations = config.annotations.clone();
        let container_state = StoredContainerState {
            log_path: log_path
                .as_ref()
                .map(|path| path.to_string_lossy().to_string()),
            tty: config.tty,
            stdin: config.stdin,
            stdin_once: config.stdin_once,
            privileged: security
                .map(|security| security.privileged)
                .unwrap_or(false),
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
            mounts: runtime_mounts
                .iter()
                .map(|mount| StoredMount {
                    container_path: mount.destination.to_string_lossy().to_string(),
                    host_path: mount.source.to_string_lossy().to_string(),
                    readonly: mount.read_only,
                    selinux_relabel: false,
                    propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate
                        as i32,
                })
                .collect(),
            run_as_user: config
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
        if let Some(checkpoint_restore) = checkpoint_restore.as_ref() {
            Self::insert_internal_state(
                &mut stored_annotations,
                INTERNAL_CHECKPOINT_RESTORE_KEY,
                checkpoint_restore,
            )?;
        }

        // 构建容器配置
        let container_config = ContainerConfig {
            name: config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_else(|| container_id.clone()),
            image: container_image_ref.clone(),
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
            mounts: runtime_mounts,
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
        let nri_event = self.nri_container_event(&pod_sandbox_id, &container_id, &stored_annotations);
        self.nri
            .create_container(nri_event.clone())
            .await
            .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))?;

        // 调用runtime创建容器（在阻塞线程中执行）
        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let container_config_clone = container_config.clone();
        let created_id = tokio::task::spawn_blocking(move || {
            runtime.create_container(&requested_container_id, &container_config_clone)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| Status::internal(format!("Failed to create container: {}", e)))?;
        if created_id != container_id {
            return Err(Status::internal(format!(
                "Runtime returned mismatched container id: requested={}, got={}",
                container_id, created_id
            )));
        }

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
            image: config.image.clone().or_else(|| {
                (!container_image_ref.is_empty()).then(|| ImageSpec {
                    image: container_image_ref.clone(),
                    user_specified_image: container_image_ref.clone(),
                    ..Default::default()
                })
            }),
            image_ref: container_image_ref.clone(),
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
            &container_image_ref,
            &container_config.command,
            &config.labels,
            &stored_annotations,
        ) {
            log::error!("Failed to persist container {}: {}", created_id, e);
        } else {
            log::info!("Container {} persisted to database", created_id);
        }
        self.nri
            .post_create_container(nri_event)
            .await
            .map_err(|e| Status::internal(format!("NRI PostCreateContainer failed: {}", e)))?;
        self.emit_container_event(
            ContainerEventType::ContainerCreatedEvent,
            &container,
            Some(ContainerState::ContainerCreated as i32),
        )
        .await;

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
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (container.pod_sandbox_id.clone(), container.annotations.clone())
        };
        let nri_event = self.nri_container_event(
            &container_pod_id,
            &actual_container_id,
            &container_annotations,
        );
        self.nri
            .start_container(nri_event.clone())
            .await
            .map_err(|e| Status::internal(format!("NRI StartContainer failed: {}", e)))?;

        let checkpoint_restore = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).and_then(|container| {
                Self::read_internal_state::<StoredCheckpointRestore>(
                    &container.annotations,
                    INTERNAL_CHECKPOINT_RESTORE_KEY,
                )
            })
        };

        // 调用runtime启动容器
        let runtime = self.runtime.clone();
        let actual_container_id_clone = actual_container_id.clone();
        let checkpoint_restore_for_runtime = checkpoint_restore.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(checkpoint_restore) = checkpoint_restore_for_runtime.as_ref() {
                runtime.restore_container_from_checkpoint(
                    &actual_container_id_clone,
                    Path::new(&checkpoint_restore.checkpoint_image_path),
                )
            } else {
                runtime.start_container(&actual_container_id_clone)
            }
        })
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

        {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(&actual_container_id) {
                container.state = observed_state;
            }
        }
        let updated_container = self
            .mutate_container_internal_state(&actual_container_id, |state| match observed_state {
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
            })
            .await?;
        if let Some(container) = updated_container.as_ref() {
            self.persist_bundle_annotations(&actual_container_id, &container.annotations)?;
        }

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
        drop(persistence);
        if checkpoint_restore.is_some() {
            let updated_annotations = {
                let mut containers = self.containers.lock().await;
                containers.get_mut(&actual_container_id).map(|container| {
                    container
                        .annotations
                        .remove(INTERNAL_CHECKPOINT_RESTORE_KEY);
                    container.annotations.clone()
                })
            };
            if let Some(updated_annotations) = updated_annotations {
                self.persist_container_annotations(&actual_container_id, &updated_annotations)
                    .await?;
                self.persist_bundle_annotations(&actual_container_id, &updated_annotations)?;
            }
        }

        log::info!("Container {} started", container_id);
        if observed_state == ContainerState::ContainerCreated as i32
            || observed_state == ContainerState::ContainerRunning as i32
        {
            self.ensure_exit_monitor_registered(&actual_container_id);
        }
        if let Some(container) = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
        } {
            self.nri
                .post_start_container(nri_event)
                .await
                .map_err(|e| Status::internal(format!("NRI PostStartContainer failed: {}", e)))?;
            self.emit_container_event(
                ContainerEventType::ContainerStartedEvent,
                &container,
                Some(observed_state),
            )
            .await;
        }
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

        let Some(actual_container_id) = self.resolve_container_id_if_exists(&container_id).await?
        else {
            return Ok(Response::new(StopContainerResponse {}));
        };
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (container.pod_sandbox_id.clone(), container.annotations.clone())
        };
        self.nri
            .stop_container(self.nri_container_event(
                &container_pod_id,
                &actual_container_id,
                &container_annotations,
            ))
            .await
            .map_err(|e| Status::internal(format!("NRI StopContainer failed: {}", e)))?;

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
        if let Some(container) = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
        } {
            self.emit_container_event(
                ContainerEventType::ContainerStoppedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
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

        let Some(actual_container_id) = self.resolve_container_id_if_exists(&container_id).await?
        else {
            return Ok(Response::new(RemoveContainerResponse {}));
        };
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (container.pod_sandbox_id.clone(), container.annotations.clone())
        };
        self.nri
            .remove_container(self.nri_container_event(
                &container_pod_id,
                &actual_container_id,
                &container_annotations,
            ))
            .await
            .map_err(|e| Status::internal(format!("NRI RemoveContainer failed: {}", e)))?;
        let deleted_container = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
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
        if let Some(container) = deleted_container {
            self.emit_container_event(
                ContainerEventType::ContainerDeletedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        Ok(Response::new(RemoveContainerResponse {}))
    }

    //重新打开容器日志
    async fn reopen_container_log(
        &self,
        request: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        let req = request.into_inner();
        let container_id = self.resolve_container_id(&req.container_id).await?;
        let container = {
            let containers = self.containers.lock().await;
            containers.get(&container_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Container not found"))?;

        let runtime_state = self.runtime_container_status_checked(&container_id).await;
        if !matches!(runtime_state, ContainerStatus::Running) {
            return Err(Status::failed_precondition(format!(
                "container {} is not running",
                container_id
            )));
        }

        let log_path = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        )
        .and_then(|state| state.log_path)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "container {} does not have a configured log path",
                container_id
            ))
        })?;

        let log_path = PathBuf::from(log_path);
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Status::internal(format!(
                    "Failed to create log directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        self.runtime
            .reopen_container_log(&container_id)
            .map_err(|e| Status::internal(format!("Failed to reopen container log: {}", e)))?;

        Ok(Response::new(ReopenContainerLogResponse {}))
    }

    //
    async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        let mut req = request.into_inner();
        req.container_id = self.resolve_container_id(&req.container_id).await?;
        self.ensure_container_is_streamable(&req.container_id, "attach")
            .await?;
        let attach_socket_path = self.attach_socket_path(&req.container_id);
        if !attach_socket_path.exists() {
            let should_try_restore = {
                let containers = self.containers.lock().await;
                containers
                    .get(&req.container_id)
                    .and_then(|container| {
                        Self::read_internal_state::<StoredContainerState>(
                            &container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                        )
                    })
                    .map(|state| state.tty)
                    .unwrap_or(false)
            };

            if should_try_restore {
                let runtime = self.runtime.clone();
                let container_id = req.container_id.clone();
                if tokio::task::spawn_blocking(move || runtime.restore_attach_shim(&container_id))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some()
                {
                    for _ in 0..20 {
                        if attach_socket_path.exists() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
        if !attach_socket_path.exists() {
            let log_path = {
                let containers = self.containers.lock().await;
                containers
                    .get(&req.container_id)
                    .and_then(|container| {
                        Self::read_internal_state::<StoredContainerState>(
                            &container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                        )
                        .and_then(|state| state.log_path)
                    })
                    .filter(|path| !path.is_empty())
            };

            if !req.stdin && log_path.is_some() {
                let streaming = self.get_streaming_server().await?;
                let response = streaming
                    .get_attach_log(&req, PathBuf::from(log_path.unwrap()))
                    .await?;
                return Ok(Response::new(response));
            }

            return Err(Status::failed_precondition(format!(
                "attach is not available for container {}: attach socket {} is missing and no interactive recovery path is available",
                req.container_id,
                attach_socket_path.display()
            )));
        }
        let streaming = self.get_streaming_server().await?;
        let response = streaming.get_attach(&req).await?;
        Ok(Response::new(response))
    }

    // 容器统计信息
    async fn container_stats(
        &self,
        request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let container_id = self.resolve_container_id(&req.container_id).await?;

        log::info!("ContainerStats request for container: {}", container_id);

        let containers = self.containers.lock().await;

        log::debug!("Total containers in memory: {}", containers.len());

        let container = containers.get(&container_id).ok_or_else(|| {
            log::warn!("Container {} not found in memory", container_id);
            Status::not_found("Container not found")
        })?;

        log::info!(
            "Found container {} in memory, collecting stats...",
            container_id
        );

        let cgroup_parent = self.container_cgroup_hint(&container_id, container).await;

        // 创建指标采集器并收集容器统计信息
        let stats = match MetricsCollector::new() {
            Ok(collector) => {
                match collector.collect_container_stats(&container_id, &cgroup_parent) {
                    Ok(stats) => {
                        let mut proto_stats = self.convert_to_proto_container_stats(stats);
                        Self::populate_container_stats_attributes(&mut proto_stats, container);
                        Some(proto_stats)
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to collect stats for container {}: {}",
                            container_id,
                            e
                        );
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
        request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_container_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListContainerStatsResponse {
                        stats: Vec::new(),
                    }));
                };
                filter.id = resolved_id;
            }

            if !filter.pod_sandbox_id.is_empty() {
                let Some(resolved_pod_id) = self
                    .resolve_pod_sandbox_id_for_filter(&filter.pod_sandbox_id)
                    .await
                else {
                    return Ok(Response::new(ListContainerStatsResponse {
                        stats: Vec::new(),
                    }));
                };
                filter.pod_sandbox_id = resolved_pod_id;
            }

            Some(filter)
        } else {
            None
        };
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
            if matches!(
                container.state,
                x if x == ContainerState::ContainerExited as i32
                    || x == ContainerState::ContainerUnknown as i32
            ) {
                continue;
            }

            if let Some(filter) = &filter {
                if !Self::container_matches_stats_filter(container, filter) {
                    continue;
                }
            }

            let cgroup_parent = self.container_cgroup_hint(container_id, container).await;

            // 收集容器统计信息
            if let Ok(stats) = collector.collect_container_stats(container_id, &cgroup_parent) {
                let mut proto_stats = self.convert_to_proto_container_stats(stats);
                Self::populate_container_stats_attributes(&mut proto_stats, container);
                all_stats.push(proto_stats);
            }
        }

        Ok(Response::new(ListContainerStatsResponse {
            stats: all_stats,
        }))
    }

    // pod沙箱统计信息
    async fn pod_sandbox_stats(
        &self,
        request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;

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
        request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_pod_sandbox_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListPodSandboxStatsResponse {
                        stats: Vec::new(),
                    }));
                };
                filter.id = resolved_id;
            }

            Some(filter)
        } else {
            None
        };
        let pods: Vec<(String, crate::proto::runtime::v1::PodSandbox)> = {
            let pods = self.pod_sandboxes.lock().await;
            pods.iter()
                .map(|(pod_id, pod)| (pod_id.clone(), pod.clone()))
                .collect()
        };
        let mut all_stats = Vec::new();

        for (pod_id, pod) in pods {
            if let Some(filter) = &filter {
                if !Self::pod_sandbox_matches_stats_filter(&pod, filter) {
                    continue;
                }
            }

            if let Some(stats) = self.collect_pod_stats(&pod_id, &pod).await {
                all_stats.push(stats);
            }
        }

        Ok(Response::new(ListPodSandboxStatsResponse {
            stats: all_stats,
        }))
    }

    // 更新运行时配置
    async fn update_runtime_config(
        &self,
        request: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        let req = request.into_inner();
        let next_network_config = req
            .runtime_config
            .and_then(|runtime_config| runtime_config.network_config)
            .filter(|network_config| !network_config.pod_cidr.trim().is_empty());

        Self::persist_runtime_network_config(&self.config.root_dir, next_network_config.as_ref())
            .map_err(|e| Status::internal(format!("Failed to persist runtime config: {}", e)))?;

        let mut stored = self.runtime_network_config.lock().await;
        *stored = next_network_config;
        Ok(Response::new(UpdateRuntimeConfigResponse {}))
    }

    //
    async fn checkpoint_container(
        &self,
        request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        let req = request.into_inner();
        if req.container_id.trim().is_empty() {
            return Err(Status::invalid_argument("container_id must not be empty"));
        }
        if req.location.trim().is_empty() {
            return Err(Status::invalid_argument("location must not be empty"));
        }

        let container_id = self.resolve_container_id(&req.container_id).await?;
        let container = {
            let containers = self.containers.lock().await;
            containers
                .get(&container_id)
                .cloned()
                .ok_or_else(|| Status::not_found("Container not found"))?
        };

        let runtime_status = self.runtime_container_status_checked(&container_id).await;
        if matches!(runtime_status, ContainerStatus::Unknown) {
            return Err(Status::failed_precondition(format!(
                "container {} has no known runtime state",
                container_id
            )));
        }
        if !matches!(runtime_status, ContainerStatus::Running) {
            return Err(Status::failed_precondition(format!(
                "container {} must be running to be checkpointed",
                container_id
            )));
        }

        let runtime_state = Self::map_runtime_container_state(runtime_status);
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let config_path = self.checkpoint_config_path(&container_id);
        if !config_path.exists() {
            return Err(Status::failed_precondition(format!(
                "container {} bundle config is missing at {}",
                container_id,
                config_path.display()
            )));
        }

        let config_payload: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config_path).map_err(|e| {
                Status::internal(format!(
                    "Failed to read checkpoint config {}: {}",
                    config_path.display(),
                    e
                ))
            })?)
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to parse checkpoint config {}: {}",
                    config_path.display(),
                    e
                ))
            })?;

        let checkpoint_location = PathBuf::from(&req.location);
        let checkpoint_image_path = Self::checkpoint_runtime_image_path(&checkpoint_location);
        let rootfs_path = config_payload
            .get("root")
            .and_then(|root| root.get("path"))
            .and_then(|path| path.as_str())
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "container {} checkpoint config is missing root.path",
                    container_id
                ))
            })?;
        let runtime_for_pause = self.runtime.clone();
        let container_id_for_pause = container_id.clone();
        tokio::task::spawn_blocking(move || {
            runtime_for_pause.pause_container(&container_id_for_pause)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to join pause task: {}", e)))?
        .map_err(|e| {
            Status::internal(format!(
                "Failed to pause container before checkpoint: {}",
                e
            ))
        })?;

        let runtime = self.runtime.clone();
        let container_id_for_checkpoint = container_id.clone();
        let checkpoint_image_path_for_runtime = checkpoint_image_path.clone();
        let checkpoint_future = tokio::task::spawn_blocking(move || {
            runtime.checkpoint_container(
                &container_id_for_checkpoint,
                &checkpoint_image_path_for_runtime,
            )
        });
        let checkpoint_result = if req.timeout > 0 {
            tokio::time::timeout(
                std::time::Duration::from_secs(req.timeout as u64),
                checkpoint_future,
            )
            .await
            .map_err(|_| {
                Status::deadline_exceeded(format!("checkpoint timed out after {}s", req.timeout))
            })?
        } else {
            checkpoint_future.await
        };
        let checkpoint_result = checkpoint_result
            .map_err(|e| Status::internal(format!("Failed to join checkpoint task: {}", e)))?
            .map_err(|e| Status::internal(format!("Failed to checkpoint container: {}", e)));

        let runtime_for_resume = self.runtime.clone();
        let container_id_for_resume = container_id.clone();
        let resume_result = tokio::task::spawn_blocking(move || {
            runtime_for_resume.resume_container(&container_id_for_resume)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to join resume task: {}", e)))?;

        if let Err(checkpoint_err) = checkpoint_result {
            if let Err(resume_err) = resume_result {
                return Err(Status::internal(format!(
                    "{}; additionally failed to resume container after checkpoint attempt: {}",
                    checkpoint_err.message(),
                    resume_err
                )));
            }
            return Err(checkpoint_err);
        }

        resume_result.map_err(|e| {
            Status::internal(format!(
                "Failed to resume container after checkpoint: {}",
                e
            ))
        })?;

        let post_checkpoint_status = self.runtime_container_status_checked(&container_id).await;
        if !matches!(post_checkpoint_status, ContainerStatus::Running) {
            return Err(Status::internal(format!(
                "container {} is not running after checkpoint",
                container_id
            )));
        }

        Self::checkpoint_rootfs_snapshot(&rootfs_path, &checkpoint_image_path.join("rootfs.tar"))
            .map_err(|e| Status::internal(format!("Failed to snapshot checkpoint rootfs: {}", e)))?;

        let manifest = json!({
            "containerId": container_id,
            "podSandboxId": container.pod_sandbox_id,
            "imageRef": container.image_ref,
            "runtimeState": Self::runtime_state_name(runtime_state),
            "checkpointedAt": Self::now_nanos(),
            "timeoutSeconds": req.timeout,
            "bundlePath": self.checkpoint_bundle_path(&container.id).display().to_string(),
            "configPath": config_path.display().to_string(),
            "checkpointImagePath": checkpoint_image_path.display().to_string(),
            "createdAt": container.created_at,
            "startedAt": container_state.as_ref().and_then(|state| state.started_at),
            "finishedAt": container_state.as_ref().and_then(|state| state.finished_at),
            "exitCode": container_state.as_ref().and_then(|state| state.exit_code),
            "logPath": container_state.as_ref().and_then(|state| state.log_path.clone()),
            "metadata": {
                "name": container
                    .annotations
                    .get(INTERNAL_CONTAINER_STATE_KEY)
                    .and_then(|_| container_state.as_ref().and_then(|state| state.metadata_name.clone()))
                    .or_else(|| {
                        container
                            .metadata
                            .as_ref()
                            .map(|metadata| metadata.name.clone())
                    })
                    .unwrap_or_default(),
                "attempt": container
                    .annotations
                    .get(INTERNAL_CONTAINER_STATE_KEY)
                    .and_then(|_| container_state.as_ref().and_then(|state| state.metadata_attempt))
                    .or_else(|| {
                        container
                            .metadata
                            .as_ref()
                            .map(|metadata| metadata.attempt)
                    })
                    .unwrap_or(1),
            }
        });

        self.write_checkpoint_artifact(
            &checkpoint_location,
            &checkpoint_image_path,
            &manifest,
            &config_payload,
        )
        .map_err(|e| Status::internal(format!("Failed to write checkpoint artifact: {}", e)))?;

        Ok(Response::new(CheckpointContainerResponse {}))
    }

    type GetContainerEventsStream = ReceiverStream<Result<ContainerEventResponse, Status>>;

    //
    async fn get_container_events(
        &self,
        _request: Request<GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        log::info!("Get container events");

        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let mut subscriber = self.events.subscribe();
        tokio::spawn(async move {
            loop {
                match subscriber.recv().await {
                    Ok(event) => {
                        if let Err(e) = tx.send(Ok(event)).await {
                            log::debug!("Container events subscriber disconnected: {}", e);
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        if let Err(e) = tx
                            .send(Err(Status::resource_exhausted(format!(
                                "missed {} container events due to slow consumer",
                                skipped
                            ))))
                            .await
                        {
                            log::debug!("Container events subscriber disconnected: {}", e);
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(stream))
    }

    // 列出指标描述符
    async fn list_metric_descriptors(
        &self,
        _request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        use crate::proto::runtime::v1::MetricDescriptor;

        // 定义支持的指标描述符
        let descriptors = vec![
            MetricDescriptor {
                name: "container_cpu_usage_seconds_total".to_string(),
                help: "Total CPU usage in seconds".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_memory_working_set_bytes".to_string(),
                help: "Current working set memory in bytes".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_memory_usage_bytes".to_string(),
                help: "Total memory usage in bytes".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_spec_memory_limit_bytes".to_string(),
                help: "Memory limit in bytes".to_string(),
                label_keys: vec!["container_id".to_string()],
            },
            MetricDescriptor {
                name: "container_pids_current".to_string(),
                help: "Current number of processes in the container cgroup".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_filesystem_usage_bytes".to_string(),
                help: "Writable layer usage in bytes".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "pod_network_receive_bytes_total".to_string(),
                help: "Total received bytes across pod interfaces".to_string(),
                label_keys: vec!["pod_id".to_string()],
            },
            MetricDescriptor {
                name: "pod_network_transmit_bytes_total".to_string(),
                help: "Total transmitted bytes across pod interfaces".to_string(),
                label_keys: vec!["pod_id".to_string()],
            },
        ];

        Ok(Response::new(ListMetricDescriptorsResponse { descriptors }))
    }

    // 列出 Pod 沙箱指标
    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        use crate::proto::runtime::v1::{
            ContainerMetrics, Metric, MetricType, PodSandboxMetrics, UInt64Value,
        };
        use std::time::{SystemTime, UNIX_EPOCH};

        let pods = self.pod_sandboxes.lock().await;
        let containers = self.containers.lock().await;
        let mut pod_metrics_list = Vec::new();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        for (pod_id, pod) in pods.iter() {
            let mut metrics = Vec::new();
            let mut container_metrics_list = Vec::new();

            // 获取 pod 的 UID 用于匹配容器
            let pod_uid = pod.metadata.as_ref().map(|m| m.uid.clone());

            // 聚合该 Pod 下所有容器的指标
            let mut total_cpu_usage = 0u64;
            let mut total_memory_usage = 0u64;
            let mut total_memory_limit = 0u64;
            let mut total_pids = 0u64;
            let mut total_filesystem_usage = 0u64;
            let mut total_rx_bytes = 0u64;
            let mut total_tx_bytes = 0u64;

            for (container_id, container) in containers.iter() {
                // 检查容器是否属于该 Pod
                let belongs_to_pod = container.pod_sandbox_id == *pod_id
                    || pod_uid
                        .as_ref()
                        .and_then(|uid| {
                            container
                                .annotations
                                .get("io.kubernetes.pod.uid")
                                .map(|container_uid| container_uid == uid)
                        })
                        .unwrap_or(false);

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

                    // 收集容器指标
                    if let Ok(collector) = MetricsCollector::new() {
                        if let Ok(stats) =
                            collector.collect_container_stats(container_id, &cgroup_parent)
                        {
                            let container_cpu =
                                stats.cpu.as_ref().map(|c| c.usage_total).unwrap_or(0);
                            let container_mem = stats.memory.as_ref().map(|m| m.usage).unwrap_or(0);
                            let container_mem_limit =
                                stats.memory.as_ref().map(|m| m.limit).unwrap_or(0);
                            let container_pids =
                                stats.pids.as_ref().map(|p| p.current).unwrap_or(0);
                            let container_fs_usage = self
                                .container_writable_layer_usage(container_id)
                                .and_then(|usage| usage.used_bytes.map(|bytes| bytes.value))
                                .unwrap_or(0);
                            let container_network =
                                self.container_network_stats(container_id).await;

                            total_cpu_usage += container_cpu;
                            total_memory_usage += container_mem;
                            total_memory_limit += container_mem_limit;
                            total_pids += container_pids;
                            total_filesystem_usage += container_fs_usage;
                            if let Some(network) = container_network {
                                total_rx_bytes += network.rx_bytes;
                                total_tx_bytes += network.tx_bytes;
                            }

                            // 容器级别指标
                            let container_metric_list = vec![
                                Metric {
                                    name: "container_cpu_usage_seconds_total".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Counter as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_cpu,
                                    }),
                                },
                                Metric {
                                    name: "container_memory_working_set_bytes".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Gauge as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_mem,
                                    }),
                                },
                                Metric {
                                    name: "container_pids_current".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Gauge as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_pids,
                                    }),
                                },
                                Metric {
                                    name: "container_filesystem_usage_bytes".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Gauge as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_fs_usage,
                                    }),
                                },
                            ];

                            container_metrics_list.push(ContainerMetrics {
                                container_id: container_id.clone(),
                                metrics: container_metric_list,
                            });
                        }
                    }
                }
            }

            // Pod 级别聚合指标
            if !container_metrics_list.is_empty() {
                metrics.push(Metric {
                    name: "container_cpu_usage_seconds_total".to_string(),
                    timestamp,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_cpu_usage,
                    }),
                });

                metrics.push(Metric {
                    name: "container_memory_working_set_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                });

                metrics.push(Metric {
                    name: "container_memory_usage_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                });

                metrics.push(Metric {
                    name: "container_spec_memory_limit_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_memory_limit,
                    }),
                });

                metrics.push(Metric {
                    name: "container_pids_current".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value { value: total_pids }),
                });

                metrics.push(Metric {
                    name: "container_filesystem_usage_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_filesystem_usage,
                    }),
                });

                metrics.push(Metric {
                    name: "pod_network_receive_bytes_total".to_string(),
                    timestamp,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_rx_bytes,
                    }),
                });

                metrics.push(Metric {
                    name: "pod_network_transmit_bytes_total".to_string(),
                    timestamp,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_tx_bytes,
                    }),
                });

                pod_metrics_list.push(PodSandboxMetrics {
                    pod_sandbox_id: pod_id.clone(),
                    metrics,
                    container_metrics: container_metrics_list,
                });
            }
        }

        Ok(Response::new(ListPodSandboxMetricsResponse {
            pod_metrics: pod_metrics_list,
        }))
    }

    //
    async fn runtime_config(
        &self,
        _request: Request<RuntimeConfigRequest>,
    ) -> Result<Response<RuntimeConfigResponse>, Status> {
        let config = RuntimeConfigResponse {
            linux: Some(crate::proto::runtime::v1::LinuxRuntimeConfiguration {
                cgroup_driver: self.cgroup_driver() as i32,
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
        let container_id = self.resolve_container_id(&req.container_id).await?;

        {
            let containers = self.containers.lock().await;
            if !containers.contains_key(&container_id) {
                return Err(Status::not_found("Container not found"));
            }
        }

        // 获取资源限制
        let linux = req.linux;
        let _windows = req.windows;

        let runtime_status = self.runtime_container_status_checked(&container_id).await;
        if !matches!(
            runtime_status,
            ContainerStatus::Running | ContainerStatus::Created
        ) {
            return Err(Status::failed_precondition(format!(
                "container {} is not in a mutable state",
                container_id
            )));
        }

        let observed_state = Self::map_runtime_container_state(runtime_status);
        {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(&container_id) {
                container.state = observed_state;
            }
        }
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (container.pod_sandbox_id.clone(), container.annotations.clone())
        };
        let nri_event =
            self.nri_container_event(&container_pod_id, &container_id, &container_annotations);
        self.nri
            .update_container(nri_event.clone())
            .await
            .map_err(|e| Status::internal(format!("NRI UpdateContainer failed: {}", e)))?;

        if let Some(resources) = linux {
            self.runtime_update_container_resources(&container_id, &resources)
                .await?;
            self.mutate_container_internal_state(&container_id, |state| {
                state.linux_resources = Some(StoredLinuxResources::from(&resources));
            })
            .await?;
        }
        self.nri
            .post_update_container(nri_event)
            .await
            .map_err(|e| Status::internal(format!("NRI PostUpdateContainer failed: {}", e)))?;

        Ok(Response::new(UpdateContainerResourcesResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ContainerRecord;
    use crate::storage::PodSandboxRecord;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::{mpsc, Mutex as StdMutex, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;
    use tempfile::TempDir;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    fn test_runtime_config(root_dir: PathBuf) -> RuntimeConfig {
        RuntimeConfig {
            root_dir,
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string(), "kata".to_string()],
            runtime_root: PathBuf::from("/tmp/crius-test-runtime-root"),
            log_dir: PathBuf::from("/tmp/crius-test-logs"),
            runtime_path: PathBuf::from("/definitely/missing/runc"),
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            cni_config: crate::network::CniConfig::default(),
        }
    }

    fn test_service() -> RuntimeServiceImpl {
        RuntimeServiceImpl::new(test_runtime_config(tempdir().unwrap().keep()))
    }

    fn test_service_with_env_cni() -> RuntimeServiceImpl {
        let mut config = test_runtime_config(tempdir().unwrap().keep());
        config.cni_config = crate::network::CniConfig::from_env();
        RuntimeServiceImpl::new(config)
    }

    fn env_lock() -> &'static StdMutex<()> {
        static ENV_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| StdMutex::new(()))
    }

    fn write_fake_runtime_script(dir: &Path) -> PathBuf {
        let state_dir = dir.join("runtime-state");
        fs::create_dir_all(&state_dir).unwrap();

        let script_path = dir.join("fake-runc.sh");
        let script = format!(
            r#"#!/bin/bash
set -eu
STATE_DIR="{state_dir}"
cmd="${{1:-}}"
if [ "$#" -gt 0 ]; then
  shift
fi
case "$cmd" in
  state)
    id="${{1:-}}"
    file="$STATE_DIR/$id.state"
    if [ ! -f "$file" ]; then
      exit 1
    fi
    status="$(cat "$file")"
    pid=0
    if [ -f "$STATE_DIR/$id.pid" ]; then
      pid="$(cat "$STATE_DIR/$id.pid")"
    fi
    printf '{{"ociVersion":"1.0.2","id":"%s","status":"%s","pid":%s,"bundle":"%s","rootfs":"%s","created":"2024-01-01T00:00:00Z","owner":"root"}}\n' "$id" "$status" "$pid" "$STATE_DIR/bundle" "$STATE_DIR/rootfs"
    ;;
  kill)
    id="${{1:-}}"
    echo stopped > "$STATE_DIR/$id.state"
    ;;
  delete)
    id="${{1:-}}"
    if [ -f "$STATE_DIR/$id.state" ]; then
      rm -f "$STATE_DIR/$id.state" "$STATE_DIR/$id.pid"
      exit 0
    fi
    echo "container does not exist" >&2
    exit 1
    ;;
  start)
    id="${{1:-}}"
    echo running > "$STATE_DIR/$id.state"
    echo $$ > "$STATE_DIR/$id.pid"
    ;;
  pause)
    id="${{1:-}}"
    if [ -n "$id" ]; then
      echo paused > "$STATE_DIR/$id.state"
    fi
    ;;
  resume)
    id="${{1:-}}"
    if [ -n "$id" ]; then
      echo running > "$STATE_DIR/$id.state"
    fi
    ;;
  update)
    resource_file=""
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --resources)
          resource_file="${{2:-}}"
          shift 2
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$resource_file" ] && [ -n "$id" ]; then
      cp "$resource_file" "$STATE_DIR/$id.update.json"
    fi
    ;;
  checkpoint)
    image_path=""
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --file-locks)
          shift
          ;;
        --image-path|--work-path)
          if [ "$1" = "--image-path" ]; then
            image_path="${{2:-}}"
          fi
          shift 2
          ;;
        --leave-running)
          shift
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$image_path" ] && [ -n "$id" ]; then
      mkdir -p "$image_path"
      printf '{{"id":"%s","checkpointed":true}}\n' "$id" > "$image_path/checkpoint.json"
    fi
    ;;
  restore)
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -d|--image-path|--work-path|--bundle|--no-pivot)
          if [ "$1" = "-d" ] || [ "$1" = "--no-pivot" ]; then
            shift
          else
            shift 2
          fi
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$id" ]; then
      echo running > "$STATE_DIR/$id.state"
      echo $$ > "$STATE_DIR/$id.pid"
    fi
    ;;
esac
exit 0
"#,
            state_dir = state_dir.display()
        );

        fs::write(&script_path, script).unwrap();
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();
        script_path
    }

    fn test_service_with_fake_runtime() -> (TempDir, RuntimeServiceImpl) {
        let dir = tempdir().unwrap();
        let runtime_path = write_fake_runtime_script(dir.path());
        let shim_work_dir = dir.path().join("shims");
        let config = RuntimeConfig {
            root_dir: dir.path().join("root"),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_root: dir.path().join("runtime-root"),
            log_dir: dir.path().join("logs"),
            runtime_path,
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            cni_config: crate::network::CniConfig::default(),
        };
        let service = RuntimeServiceImpl::new_with_shim_work_dir(
            config,
            NriConfig::default(),
            shim_work_dir,
        );
        (dir, service)
    }

    fn fake_runtime_state_path(dir: &TempDir, id: &str) -> PathBuf {
        dir.path()
            .join("runtime-state")
            .join(format!("{}.state", id))
    }

    fn fake_runtime_update_path(dir: &TempDir, id: &str) -> PathBuf {
        dir.path()
            .join("runtime-state")
            .join(format!("{}.update.json", id))
    }

    fn set_fake_runtime_state(dir: &TempDir, id: &str, state: &str) {
        let state_path = fake_runtime_state_path(dir, id);
        fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        fs::write(&state_path, state).unwrap();
        let pid_path = dir.path().join("runtime-state").join(format!("{}.pid", id));
        if state == "running" {
            fs::write(pid_path, format!("{}", std::process::id())).unwrap();
        } else {
            let _ = fs::remove_file(pid_path);
        }
    }

    fn test_container(
        id: &str,
        pod_sandbox_id: &str,
        annotations: HashMap<String, String>,
    ) -> Container {
        Container {
            id: id.to_string(),
            pod_sandbox_id: pod_sandbox_id.to_string(),
            state: ContainerState::ContainerCreated as i32,
            metadata: Some(ContainerMetadata {
                name: format!("{}-name", id),
                attempt: 1,
            }),
            image: Some(ImageSpec {
                image: "busybox:latest".to_string(),
                ..Default::default()
            }),
            image_ref: "busybox:latest".to_string(),
            annotations,
            created_at: RuntimeServiceImpl::now_nanos(),
            ..Default::default()
        }
    }

    fn test_pod(
        id: &str,
        annotations: HashMap<String, String>,
    ) -> crate::proto::runtime::v1::PodSandbox {
        crate::proto::runtime::v1::PodSandbox {
            id: id.to_string(),
            metadata: Some(PodSandboxMetadata {
                name: format!("{}-pod", id),
                uid: format!("{}-uid", id),
                namespace: "default".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: HashMap::new(),
            annotations,
            runtime_handler: "runc".to_string(),
            ..Default::default()
        }
    }

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
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: true,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
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
        assert_eq!(status.reason, "Error");
        assert_eq!(status.message, "container exited with code 42");
        assert_eq!(status.mounts.len(), 1);
        assert_eq!(status.mounts[0].container_path, "/data");
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

    #[tokio::test]
    async fn resolve_container_id_handles_zero_single_and_multiple_matches() {
        let service = test_service();
        service.containers.lock().await.insert(
            "abcdef123456".to_string(),
            test_container("abcdef123456", "pod-1", HashMap::new()),
        );
        service.containers.lock().await.insert(
            "abc999999999".to_string(),
            test_container("abc999999999", "pod-1", HashMap::new()),
        );

        assert_eq!(
            service.resolve_container_id("abcdef").await.unwrap(),
            "abcdef123456"
        );
        assert_eq!(
            service
                .resolve_container_id("missing")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            service
                .resolve_container_id("abc")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn resolve_pod_sandbox_id_handles_zero_single_and_multiple_matches() {
        let service = test_service();
        service.pod_sandboxes.lock().await.insert(
            "podabcdef123456".to_string(),
            test_pod("podabcdef123456", HashMap::new()),
        );
        service.pod_sandboxes.lock().await.insert(
            "podabc999999999".to_string(),
            test_pod("podabc999999999", HashMap::new()),
        );

        assert_eq!(
            service.resolve_pod_sandbox_id("podabcdef").await.unwrap(),
            "podabcdef123456"
        );
        assert_eq!(
            service
                .resolve_pod_sandbox_id("missing")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            service
                .resolve_pod_sandbox_id("podabc")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn stop_and_remove_container_are_idempotent_when_missing() {
        let service = test_service();

        let stop = RuntimeService::stop_container(
            &service,
            Request::new(StopContainerRequest {
                container_id: "missing".to_string(),
                timeout: 0,
            }),
        )
        .await;
        assert!(stop.is_ok());

        let remove = RuntimeService::remove_container(
            &service,
            Request::new(RemoveContainerRequest {
                container_id: "missing".to_string(),
            }),
        )
        .await;
        assert!(remove.is_ok());
    }

    #[tokio::test]
    async fn stop_and_remove_pod_are_idempotent_when_missing() {
        let service = test_service();

        let stop = RuntimeService::stop_pod_sandbox(
            &service,
            Request::new(StopPodSandboxRequest {
                pod_sandbox_id: "missing".to_string(),
            }),
        )
        .await;
        assert!(stop.is_ok());

        let remove = RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: "missing".to_string(),
            }),
        )
        .await;
        assert!(remove.is_ok());
    }

    #[tokio::test]
    async fn exec_validates_container_is_streamable() {
        let (dir, service) = test_service_with_fake_runtime();
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        let missing = RuntimeService::exec(
            &service,
            Request::new(ExecRequest {
                container_id: "missing".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        service.containers.lock().await.insert(
            "container-stopped".to_string(),
            test_container("container-stopped", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-stopped", "stopped");
        let stopped = RuntimeService::exec(
            &service,
            Request::new(ExecRequest {
                container_id: "container-stopped".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

        service.containers.lock().await.insert(
            "container-created".to_string(),
            test_container("container-created", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-created", "created");
        let response = RuntimeService::exec(
            &service,
            Request::new(ExecRequest {
                container_id: "container-created".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.url.contains("/exec/"));
    }

    #[tokio::test]
    async fn exec_sync_validates_container_is_streamable() {
        let (dir, service) = test_service_with_fake_runtime();

        let missing = RuntimeService::exec_sync(
            &service,
            Request::new(ExecSyncRequest {
                container_id: "missing".to_string(),
                cmd: vec!["true".to_string()],
                timeout: 0,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        service.containers.lock().await.insert(
            "container-stopped".to_string(),
            test_container("container-stopped", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-stopped", "stopped");
        let stopped = RuntimeService::exec_sync(
            &service,
            Request::new(ExecSyncRequest {
                container_id: "container-stopped".to_string(),
                cmd: vec!["true".to_string()],
                timeout: 0,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-running", "running");
        let response = RuntimeService::exec_sync(
            &service,
            Request::new(ExecSyncRequest {
                container_id: "container-running".to_string(),
                cmd: vec!["true".to_string()],
                timeout: 0,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(response.exit_code, 0);
    }

    #[tokio::test]
    async fn attach_validates_container_is_streamable() {
        let (dir, service) = test_service_with_fake_runtime();
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        let missing = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "missing".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        service.containers.lock().await.insert(
            "container-stopped".to_string(),
            test_container("container-stopped", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-stopped", "stopped");
        let stopped = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "container-stopped".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-running", "running");
        let attach_socket = dir
            .path()
            .join("shims")
            .join("container-running")
            .join("attach.sock");
        fs::create_dir_all(attach_socket.parent().unwrap()).unwrap();
        fs::write(&attach_socket, "").unwrap();
        let response = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "container-running".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.url.contains("/attach/"));
    }

    #[tokio::test]
    async fn attach_uses_log_stream_fallback_when_recovered_socket_is_missing() {
        let (dir, service) = test_service_with_fake_runtime();
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        set_fake_runtime_state(&dir, "recover-container", "running");
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(
                    dir.path()
                        .join("logs")
                        .join("recover.log")
                        .display()
                        .to_string(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .persistence
            .lock()
            .await
            .save_container(
                "recover-container",
                "pod-1",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &annotations,
            )
            .unwrap();
        service.containers.lock().await.insert(
            "recover-container".to_string(),
            test_container("recover-container", "pod-1", annotations),
        );

        service.recover_state().await.unwrap();

        let response = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "recover-container".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.url.contains("/attach/"));
    }

    #[tokio::test]
    async fn attach_uses_log_stream_fallback_for_read_only_tty_when_socket_is_missing() {
        let (dir, service) = test_service_with_fake_runtime();
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        set_fake_runtime_state(&dir, "recover-container-tty", "running");
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(
                    dir.path()
                        .join("logs")
                        .join("recover-tty.log")
                        .display()
                        .to_string(),
                ),
                tty: true,
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "recover-container-tty".to_string(),
            test_container("recover-container-tty", "pod-1", annotations),
        );

        let response = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "recover-container-tty".to_string(),
                stdin: false,
                stdout: true,
                stderr: false,
                tty: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.url.contains("/attach/"));
    }

    #[tokio::test]
    async fn attach_recreates_tty_shim_when_socket_is_missing() {
        let _guard = env_lock().lock().unwrap();
        let dir = tempdir().unwrap();
        let runtime_path = write_fake_runtime_script(dir.path());
        let shim_work_dir = dir.path().join("shims");
        let fake_shim_path = dir.path().join("fake-shim.sh");
        fs::write(
            &fake_shim_path,
            r#"#!/bin/sh
set -eu
exit_code_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
shim_dir="$(dirname "$exit_code_file")"
mkdir -p "$shim_dir"
: > "$shim_dir/attach.sock"
: > "$shim_dir/resize.sock"
sleep 1
"#,
        )
        .unwrap();
        let mut perms = fs::metadata(&fake_shim_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&fake_shim_path, perms).unwrap();

        std::env::set_var("CRIUS_SHIM_PATH", &fake_shim_path);
        let service = RuntimeServiceImpl::new_with_shim_work_dir(
            RuntimeConfig {
                root_dir: dir.path().join("root"),
                runtime: "runc".to_string(),
                runtime_handlers: vec!["runc".to_string()],
                runtime_root: dir.path().join("runtime-root"),
                log_dir: dir.path().join("logs"),
                runtime_path,
                pause_image: "registry.k8s.io/pause:3.9".to_string(),
                cni_config: crate::network::CniConfig::default(),
            },
            NriConfig::default(),
            shim_work_dir.clone(),
        );
        std::env::remove_var("CRIUS_SHIM_PATH");
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        set_fake_runtime_state(&dir, "tty-restore", "running");
        let bundle_dir = dir.path().join("runtime-root").join("tty-restore");
        fs::create_dir_all(&bundle_dir).unwrap();
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "process": {
                    "terminal": true
                }
            })
            .to_string(),
        )
        .unwrap();

        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                tty: true,
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "tty-restore".to_string(),
            Container {
                state: ContainerState::ContainerRunning as i32,
                ..test_container("tty-restore", "pod-1", annotations)
            },
        );

        let response = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "tty-restore".to_string(),
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.url.contains("/attach/"));
        assert!(shim_work_dir
            .join("tty-restore")
            .join("attach.sock")
            .exists());
    }

    #[tokio::test]
    async fn attach_returns_error_when_socket_missing_and_interactive_recovery_is_requested() {
        let (dir, service) = test_service_with_fake_runtime();
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        set_fake_runtime_state(&dir, "recover-container", "running");
        service.containers.lock().await.insert(
            "recover-container".to_string(),
            test_container("recover-container", "pod-1", HashMap::new()),
        );

        let err = RuntimeService::attach(
            &service,
            Request::new(AttachRequest {
                container_id: "recover-container".to_string(),
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("interactive recovery"));
    }

    #[tokio::test]
    async fn container_status_verbose_returns_json_info() {
        let (dir, service) = test_service_with_fake_runtime();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some("/var/log/pods/c1.log".to_string()),
                tty: true,
                privileged: true,
                mounts: vec![StoredMount {
                    container_path: "/data".to_string(),
                    host_path: "/host/data".to_string(),
                    readonly: false,
                    selinux_relabel: false,
                    propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate
                        as i32,
                }],
                run_as_user: Some("1000".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "container-1".to_string(),
            test_container("container-1", "pod-1", annotations),
        );
        set_fake_runtime_state(&dir, "container-1", "running");
        let bundle_dir = dir.path().join("runtime-root").join("container-1");
        fs::create_dir_all(&bundle_dir).unwrap();
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "process": {
                    "terminal": true,
                    "cwd": "/"
                }
            })
            .to_string(),
        )
        .unwrap();

        let response = RuntimeService::container_status(
            &service,
            Request::new(ContainerStatusRequest {
                container_id: "container-1".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.info.contains_key("info"));
        let info: serde_json::Value =
            serde_json::from_str(response.info.get("info").unwrap()).unwrap();
        assert_eq!(info["id"], "container-1");
        assert_eq!(info["sandboxID"], "pod-1");
        assert_eq!(info["privileged"], true);
        assert_eq!(info["user"], "1000");
        assert_eq!(info["runtimeSpec"]["process"]["cwd"], "/");
        assert_eq!(response.status.unwrap().mounts.len(), 1);
    }

    #[tokio::test]
    async fn pod_sandbox_status_verbose_returns_json_info() {
        let (dir, service) = test_service_with_fake_runtime();
        let mut annotations = HashMap::new();
        annotations.insert(
            "io.kubernetes.cri.sandbox-image".to_string(),
            "registry.k8s.io/pause:3.10".to_string(),
        );
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                netns_path: Some("/var/run/netns/test-pod".to_string()),
                pause_container_id: Some("pause-1".to_string()),
                log_directory: Some("/var/log/pods/test-pod".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .pod_sandboxes
            .lock()
            .await
            .insert("pod-1".to_string(), test_pod("pod-1", annotations));
        set_fake_runtime_state(&dir, "pause-1", "running");
        let bundle_dir = dir.path().join("runtime-root").join("pause-1");
        fs::create_dir_all(&bundle_dir).unwrap();
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "root": {
                    "path": "rootfs"
                }
            })
            .to_string(),
        )
        .unwrap();

        let response = RuntimeService::pod_sandbox_status(
            &service,
            Request::new(PodSandboxStatusRequest {
                pod_sandbox_id: "pod-1".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.info.contains_key("info"));
        let info: serde_json::Value =
            serde_json::from_str(response.info.get("info").unwrap()).unwrap();
        assert_eq!(info["id"], "pod-1");
        assert_eq!(info["image"], "registry.k8s.io/pause:3.10");
        assert_eq!(info["pauseContainerId"], "pause-1");
        assert!(info["pid"].is_number());
        assert_eq!(info["runtimeSpec"]["root"]["path"], "rootfs");
    }

    #[tokio::test]
    async fn status_verbose_returns_structured_config() {
        let service = test_service();
        let response =
            RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
                .await
                .unwrap()
                .into_inner();

        assert!(response.info.contains_key("config"));
        let config: serde_json::Value =
            serde_json::from_str(response.info.get("config").unwrap()).unwrap();
        assert_eq!(config["runtimeName"], "crius");
        assert!(config["runtimeHandlers"].as_array().unwrap().len() >= 1);
        assert_eq!(config["recovery"]["startupReconcile"], true);
        assert_eq!(config["recovery"]["eventReplayOnRecovery"], false);
        assert_eq!(config["runtimeFeatures"]["updateContainerResources"], true);
        assert_eq!(config["runtimeFeatures"]["containerStats"], true);
        assert_eq!(config["runtimeFeatures"]["podSandboxStats"], true);
        assert_eq!(config["runtimeFeatures"]["podSandboxMetrics"], true);
        assert_eq!(
            response.status.unwrap().conditions.len(),
            2,
            "expected runtime and network conditions"
        );
    }

    #[tokio::test]
    async fn version_reports_runtime_binary_version_when_available() {
        let dir = tempdir().unwrap();
        let runtime_path = dir.path().join("fake-runtime-version.sh");
        fs::write(
            &runtime_path,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "runc version 1.2.3"
  exit 0
fi
exit 0
"#,
        )
        .unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&runtime_path, perms).unwrap();

        let service = RuntimeServiceImpl::new(RuntimeConfig {
            runtime_path: runtime_path.clone(),
            ..test_runtime_config(dir.path().join("root"))
        });
        let response = RuntimeService::version(
            &service,
            Request::new(VersionRequest {
                version: "0.1.0".to_string(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(response.runtime_name, "runc");
        assert_eq!(response.runtime_version, "runc version 1.2.3");
    }

    #[test]
    fn parse_cgroup_hint_from_procfs_prefers_relevant_controller_or_unified_line() {
        let v1 = "11:hugetlb:/\n10:memory:/kubepods.slice/pod123/container.scope\n9:cpuset:/\n";
        assert_eq!(
            RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v1),
            Some(PathBuf::from("/kubepods.slice/pod123/container.scope"))
        );

        let v2 = "0::/user.slice/user-0.slice/session-1.scope\n";
        assert_eq!(
            RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v2),
            Some(PathBuf::from("/user.slice/user-0.slice/session-1.scope"))
        );
    }

    #[tokio::test]
    async fn status_reports_runtime_not_ready_when_binary_is_not_executable() {
        let dir = tempdir().unwrap();
        let runtime_path = dir.path().join("fake-runtime");
        fs::write(&runtime_path, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&runtime_path, perms).unwrap();

        let service = RuntimeServiceImpl::new(RuntimeConfig {
            runtime_path: runtime_path.clone(),
            ..test_runtime_config(dir.path().join("root"))
        });
        let response =
            RuntimeService::status(&service, Request::new(StatusRequest { verbose: false }))
                .await
                .unwrap()
                .into_inner();
        let runtime_condition = response
            .status
            .unwrap()
            .conditions
            .into_iter()
            .find(|condition| condition.r#type == "RuntimeReady")
            .unwrap();
        assert!(!runtime_condition.status);
        assert_eq!(runtime_condition.reason, "RuntimeBinaryNotExecutable");
    }

    #[tokio::test]
    async fn runtime_config_reports_detected_cgroup_driver() {
        let service = test_service();
        let response =
            RuntimeService::runtime_config(&service, Request::new(RuntimeConfigRequest {}))
                .await
                .unwrap()
                .into_inner();

        assert_eq!(
            response.linux.unwrap().cgroup_driver,
            service.cgroup_driver() as i32
        );
    }

    #[tokio::test]
    async fn update_runtime_config_persists_network_config_and_exposes_it_via_status() {
        let root_dir = tempdir().unwrap().keep();
        let service = RuntimeServiceImpl::new(test_runtime_config(root_dir.clone()));

        RuntimeService::update_runtime_config(
            &service,
            Request::new(UpdateRuntimeConfigRequest {
                runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                    network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                        pod_cidr: "10.244.0.0/16".to_string(),
                    }),
                }),
            }),
        )
        .await
        .unwrap();

        let status =
            RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
                .await
                .unwrap()
                .into_inner();
        let config: serde_json::Value =
            serde_json::from_str(status.info.get("config").unwrap()).unwrap();
        assert_eq!(config["runtimeNetworkConfig"]["podCIDR"], "10.244.0.0/16");

        let reloaded = RuntimeServiceImpl::new(test_runtime_config(root_dir));
        let reloaded_status =
            RuntimeService::status(&reloaded, Request::new(StatusRequest { verbose: true }))
                .await
                .unwrap()
                .into_inner();
        let reloaded_config: serde_json::Value =
            serde_json::from_str(reloaded_status.info.get("config").unwrap()).unwrap();
        assert_eq!(
            reloaded_config["runtimeNetworkConfig"]["podCIDR"],
            "10.244.0.0/16"
        );
    }

    #[tokio::test]
    async fn run_pod_sandbox_persists_runtime_pod_cidr_in_verbose_info() {
        let (dir, service) = test_service_with_fake_runtime();
        RuntimeService::update_runtime_config(
            &service,
            Request::new(UpdateRuntimeConfigRequest {
                runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                    network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                        pod_cidr: "10.88.0.0/16".to_string(),
                    }),
                }),
            }),
        )
        .await
        .unwrap();

        let netns_path = dir.path().join("pod-runtime-cidr.netns");
        fs::write(&netns_path, "netns").unwrap();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                runtime_pod_cidr: Some("10.88.0.0/16".to_string()),
                netns_path: Some(netns_path.display().to_string()),
                pause_container_id: Some("pause-runtime-cidr".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        service.pod_sandboxes.lock().await.insert(
            "pod-runtime-cidr".to_string(),
            test_pod("pod-runtime-cidr", annotations),
        );

        let response = RuntimeService::pod_sandbox_status(
            &service,
            Request::new(PodSandboxStatusRequest {
                pod_sandbox_id: "pod-runtime-cidr".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let info: serde_json::Value =
            serde_json::from_str(response.info.get("info").unwrap()).unwrap();
        assert_eq!(info["runtimePodCIDR"], "10.88.0.0/16");
    }

    #[test]
    fn network_health_requires_declared_plugin_binary() {
        let _guard = env_lock().lock().unwrap();
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("cni-conf");
        let plugin_dir = dir.path().join("cni-bin");
        fs::create_dir_all(&config_dir).unwrap();
        fs::create_dir_all(&plugin_dir).unwrap();
        fs::write(
            config_dir.join("10-test.conflist"),
            r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
        )
        .unwrap();

        std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
        std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
        let service = test_service_with_env_cni();
        let (ready, reason, message) = service.network_health();
        std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
        std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

        assert!(!ready);
        assert_eq!(reason, "CNIPluginMissing");
        assert!(message.contains("bridge"));
    }

    #[test]
    fn network_health_requires_plugin_to_be_executable() {
        let _guard = env_lock().lock().unwrap();
        let dir = tempdir().unwrap();
        let config_dir = dir.path().join("cni-conf");
        let plugin_dir = dir.path().join("cni-bin");
        fs::create_dir_all(&config_dir).unwrap();
        fs::create_dir_all(&plugin_dir).unwrap();
        fs::write(
            config_dir.join("10-test.conflist"),
            r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
        )
        .unwrap();
        let plugin_path = plugin_dir.join("bridge");
        fs::write(&plugin_path, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&plugin_path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&plugin_path, perms).unwrap();

        std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
        std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
        let service = test_service_with_env_cni();
        let (ready, reason, message) = service.network_health();
        std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
        std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

        assert!(!ready);
        assert_eq!(reason, "CNIPluginMissing");
        assert!(message.contains("bridge"));
        assert!(message.contains("non-executable"));
    }

    #[tokio::test]
    async fn checkpoint_container_writes_checkpoint_artifact() {
        let (dir, service) = test_service_with_fake_runtime();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(dir.path().join("logs").join("c1.log").display().to_string()),
                metadata_name: Some("checkpointed".to_string()),
                metadata_attempt: Some(2),
                started_at: Some(10),
                ..Default::default()
            },
        )
        .unwrap();

        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", annotations),
        );
        set_fake_runtime_state(&dir, "container-running", "running");

        let bundle_dir = dir.path().join("runtime-root").join("container-running");
        fs::create_dir_all(&bundle_dir).unwrap();
        let rootfs_dir = dir.path().join("checkpoint-rootfs-json");
        fs::create_dir_all(&rootfs_dir).unwrap();
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "root": {
                    "path": rootfs_dir.display().to_string()
                },
            })
            .to_string(),
        )
        .unwrap();

        let artifact_path = dir.path().join("checkpoint.json");
        RuntimeService::checkpoint_container(
            &service,
            Request::new(CheckpointContainerRequest {
                container_id: "container-running".to_string(),
                location: artifact_path.display().to_string(),
                timeout: 30,
            }),
        )
        .await
        .unwrap();

        let artifact: serde_json::Value =
            serde_json::from_slice(&fs::read(&artifact_path).unwrap()).unwrap();
        assert_eq!(artifact["manifest"]["containerId"], "container-running");
        assert_eq!(artifact["manifest"]["runtimeState"], "running");
        assert_eq!(artifact["manifest"]["metadata"]["name"], "checkpointed");
        assert_eq!(artifact["manifest"]["metadata"]["attempt"], 2);
        assert_eq!(
            artifact["ociConfig"]["root"]["path"],
            rootfs_dir.display().to_string()
        );
        let checkpoint_image_path = PathBuf::from(
            artifact["manifest"]["checkpointImagePath"]
                .as_str()
                .expect("checkpoint image path should be recorded"),
        );
        assert!(checkpoint_image_path.join("checkpoint.json").exists());
        assert!(checkpoint_image_path.join("rootfs.tar").exists());
    }

    #[tokio::test]
    async fn checkpoint_container_writes_tar_export_when_location_is_archive() {
        let (dir, service) = test_service_with_fake_runtime();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                metadata_name: Some("checkpointed-tar".to_string()),
                metadata_attempt: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", annotations),
        );
        set_fake_runtime_state(&dir, "container-running", "running");

        let bundle_dir = dir.path().join("runtime-root").join("container-running");
        fs::create_dir_all(&bundle_dir).unwrap();
        let rootfs_dir = dir.path().join("checkpoint-rootfs-tar");
        fs::create_dir_all(&rootfs_dir).unwrap();
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "root": {
                    "path": rootfs_dir.display().to_string()
                },
            })
            .to_string(),
        )
        .unwrap();

        let artifact_path = dir.path().join("checkpoint.tar");
        RuntimeService::checkpoint_container(
            &service,
            Request::new(CheckpointContainerRequest {
                container_id: "container-running".to_string(),
                location: artifact_path.display().to_string(),
                timeout: 30,
            }),
        )
        .await
        .unwrap();

        let tar_output = Command::new("tar")
            .args(["-tf", artifact_path.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(tar_output.status.success());
        let stdout = String::from_utf8_lossy(&tar_output.stdout);
        assert!(stdout.contains("manifest.json"));
        assert!(stdout.contains("config.json"));
        assert!(stdout.contains("checkpoint.json"));
        assert!(stdout.contains("rootfs.tar"));
    }

    #[tokio::test]
    async fn start_container_restores_from_checkpoint_artifact_and_clears_pending_marker() {
        let (dir, service) = test_service_with_fake_runtime();

        let checkpoint_location = dir.path().join("restore-artifact.json");
        let checkpoint_image_path =
            RuntimeServiceImpl::checkpoint_runtime_image_path(&checkpoint_location);
        fs::create_dir_all(&checkpoint_image_path).unwrap();
        fs::write(checkpoint_image_path.join("checkpoint.json"), "{}").unwrap();
        fs::write(
            &checkpoint_location,
            serde_json::to_vec_pretty(&serde_json::json!({
                "manifest": {
                    "imageRef": "busybox:latest",
                    "checkpointImagePath": checkpoint_image_path.display().to_string(),
                },
                "ociConfig": {
                    "ociVersion": "1.0.2",
                    "root": {
                        "path": "/tmp/rootfs"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState::default(),
        )
        .unwrap();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CHECKPOINT_RESTORE_KEY,
            &StoredCheckpointRestore {
                checkpoint_location: checkpoint_location.display().to_string(),
                checkpoint_image_path: checkpoint_image_path.display().to_string(),
                oci_config: serde_json::json!({
                    "ociVersion": "1.0.2",
                    "root": {
                        "path": "/tmp/rootfs"
                    }
                }),
                image_ref: "busybox:latest".to_string(),
            },
        )
        .unwrap();

        service.containers.lock().await.insert(
            "restore-container".to_string(),
            Container {
                state: ContainerState::ContainerCreated as i32,
                ..test_container("restore-container", "pod-1", annotations.clone())
            },
        );
        service
            .persistence
            .lock()
            .await
            .save_container(
                "restore-container",
                "pod-1",
                crate::runtime::ContainerStatus::Created,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &annotations,
            )
            .unwrap();

        let bundle_dir = dir.path().join("runtime-root").join("restore-container");
        fs::create_dir_all(&bundle_dir).unwrap();
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "annotations": annotations,
                "root": {
                    "path": "/tmp/rootfs"
                }
            })
            .to_string(),
        )
        .unwrap();

        RuntimeService::start_container(
            &service,
            Request::new(StartContainerRequest {
                container_id: "restore-container".to_string(),
            }),
        )
        .await
        .unwrap();

        let container = service
            .containers
            .lock()
            .await
            .get("restore-container")
            .cloned()
            .unwrap();
        assert_eq!(container.state, ContainerState::ContainerRunning as i32);
        let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        )
        .expect("started_at should be persisted after restore");
        assert!(internal_state.started_at.is_some());
        assert!(
            !container
                .annotations
                .contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY),
            "restore marker should be cleared after successful restore"
        );

        let persisted = service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("restore-container")
            .unwrap()
            .unwrap();
        let persisted_annotations: HashMap<String, String> =
            serde_json::from_str(&persisted.annotations).unwrap();
        assert!(
            !persisted_annotations.contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY),
            "restore marker should be cleared from persistence after successful restore"
        );

        let bundle_config: serde_json::Value = serde_json::from_slice(
            &fs::read(
                dir.path()
                    .join("runtime-root")
                    .join("restore-container")
                    .join("config.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let bundle_annotations = bundle_config
            .get("annotations")
            .and_then(|value| value.as_object())
            .cloned()
            .unwrap_or_default();
        assert!(!bundle_annotations.contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY));
        let bundle_container_state: StoredContainerState = serde_json::from_str(
            bundle_annotations
                .get(INTERNAL_CONTAINER_STATE_KEY)
                .and_then(|value| value.as_str())
                .expect("bundle should persist internal container state"),
        )
        .unwrap();
        assert!(bundle_container_state.started_at.is_some());

        let exit_code_path = dir
            .path()
            .join("shims")
            .join("restore-container")
            .join("exit_code");
        fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
        fs::write(&exit_code_path, "0").unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    #[tokio::test]
    async fn reopen_container_log_validates_running_state_and_log_path() {
        let (dir, service) = test_service_with_fake_runtime();

        let missing = RuntimeService::reopen_container_log(
            &service,
            Request::new(ReopenContainerLogRequest {
                container_id: "missing".to_string(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        let mut stopped_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut stopped_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(
                    dir.path()
                        .join("logs")
                        .join("stopped.log")
                        .display()
                        .to_string(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "container-stopped".to_string(),
            test_container("container-stopped", "pod-1", stopped_annotations),
        );
        set_fake_runtime_state(&dir, "container-stopped", "stopped");

        let stopped = RuntimeService::reopen_container_log(
            &service,
            Request::new(ReopenContainerLogRequest {
                container_id: "container-stopped".to_string(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

        let log_path = dir.path().join("logs").join("running.log");
        let mut running_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut running_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(log_path.display().to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", running_annotations),
        );
        set_fake_runtime_state(&dir, "container-running", "running");

        let running = RuntimeService::reopen_container_log(
            &service,
            Request::new(ReopenContainerLogRequest {
                container_id: "container-running".to_string(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(running.code(), tonic::Code::Internal);
        assert!(running.message().contains("reopen log socket"));
    }

    #[tokio::test]
    #[ignore = "requires unix socket bind permissions in the current test environment"]
    async fn reopen_container_log_notifies_shim_socket_when_available() {
        let (dir, service) = test_service_with_fake_runtime();

        let log_path = dir.path().join("logs").join("running.log");
        let mut running_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut running_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(log_path.display().to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", running_annotations),
        );
        set_fake_runtime_state(&dir, "container-running", "running");

        let reopen_socket = dir
            .path()
            .join("shims")
            .join("container-running")
            .join("reopen.sock");
        fs::create_dir_all(reopen_socket.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&reopen_socket).unwrap();
        let log_path_for_server = log_path.clone();
        let (tx, rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            fs::write(&log_path_for_server, "").unwrap();
            stream.write_all(b"OK\n").unwrap();
            tx.send(()).unwrap();
        });

        RuntimeService::reopen_container_log(
            &service,
            Request::new(ReopenContainerLogRequest {
                container_id: "container-running".to_string(),
            }),
        )
        .await
        .unwrap();
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        server.join().unwrap();
        assert!(log_path.exists(), "expected reopen to touch the log file");
    }

    #[tokio::test]
    async fn update_container_resources_validates_state_and_persists_resources() {
        let (dir, service) = test_service_with_fake_runtime();

        let missing = RuntimeService::update_container_resources(
            &service,
            Request::new(UpdateContainerResourcesRequest {
                container_id: "missing".to_string(),
                linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                    cpu_shares: 2,
                    ..Default::default()
                }),
                windows: None,
                annotations: HashMap::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        service.containers.lock().await.insert(
            "container-stopped".to_string(),
            test_container("container-stopped", "pod-1", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-stopped", "stopped");
        let stopped = RuntimeService::update_container_resources(
            &service,
            Request::new(UpdateContainerResourcesRequest {
                container_id: "container-stopped".to_string(),
                linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                    cpu_shares: 2,
                    ..Default::default()
                }),
                windows: None,
                annotations: HashMap::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState::default(),
        )
        .unwrap();
        service.containers.lock().await.insert(
            "container-running".to_string(),
            test_container("container-running", "pod-1", annotations.clone()),
        );
        set_fake_runtime_state(&dir, "container-running", "running");
        service
            .persistence
            .lock()
            .await
            .save_container(
                "container-running",
                "pod-1",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &annotations,
            )
            .unwrap();

        let resources = crate::proto::runtime::v1::LinuxContainerResources {
            cpu_shares: 256,
            cpu_period: 100_000,
            cpu_quota: 50_000,
            memory_limit_in_bytes: 128 * 1024 * 1024,
            cpuset_cpus: "0-1".to_string(),
            cpuset_mems: "0".to_string(),
            ..Default::default()
        };

        RuntimeService::update_container_resources(
            &service,
            Request::new(UpdateContainerResourcesRequest {
                container_id: "container-running".to_string(),
                linux: Some(resources.clone()),
                windows: None,
                annotations: HashMap::new(),
            }),
        )
        .await
        .unwrap();

        let status = RuntimeService::container_status(
            &service,
            Request::new(ContainerStatusRequest {
                container_id: "container-running".to_string(),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
        let linux = status.resources.unwrap().linux.unwrap();
        assert_eq!(linux.cpu_shares, 256);
        assert_eq!(linux.memory_limit_in_bytes, 128 * 1024 * 1024);

        let persisted = service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("container-running")
            .unwrap()
            .unwrap();
        let persisted_annotations: HashMap<String, String> =
            serde_json::from_str(&persisted.annotations).unwrap();
        let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
            &persisted_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        )
        .unwrap();
        assert_eq!(
            persisted_state
                .linux_resources
                .unwrap()
                .memory_limit_in_bytes,
            128 * 1024 * 1024
        );

        let update_payload: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
        )
        .unwrap();
        assert_eq!(update_payload["cpu"]["shares"], 256);
        assert_eq!(update_payload["memory"]["limit"], 128 * 1024 * 1024);
    }

    #[tokio::test]
    async fn inspect_and_info_endpoints_return_consistent_snapshots() {
        let (dir, service) = test_service_with_fake_runtime();
        let netns_path = dir.path().join("inspect.netns");
        fs::write(&netns_path, "netns").unwrap();
        let log_path = dir.path().join("logs").join("inspect.log");

        let mut pod_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut pod_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                netns_path: Some(netns_path.display().to_string()),
                pause_container_id: Some("pause-inspect".to_string()),
                ip: Some("10.88.0.10".to_string()),
                additional_ips: vec!["fd00::10".to_string()],
                ..Default::default()
            },
        )
        .unwrap();
        service.pod_sandboxes.lock().await.insert(
            "pod-inspect".to_string(),
            test_pod("pod-inspect", pod_annotations),
        );

        let mut container_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut container_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                log_path: Some(log_path.display().to_string()),
                run_as_user: Some("1000".to_string()),
                mounts: vec![StoredMount {
                    container_path: "/data".to_string(),
                    host_path: "/host/data".to_string(),
                    readonly: false,
                    selinux_relabel: false,
                    propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate
                        as i32,
                }],
                ..Default::default()
            },
        )
        .unwrap();
        service.containers.lock().await.insert(
            "container-inspect".to_string(),
            test_container("container-inspect", "pod-inspect", container_annotations),
        );
        set_fake_runtime_state(&dir, "container-inspect", "running");

        let status =
            RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
                .await
                .unwrap()
                .into_inner();
        let container = RuntimeService::container_status(
            &service,
            Request::new(ContainerStatusRequest {
                container_id: "container-inspect".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let pod = RuntimeService::pod_sandbox_status(
            &service,
            Request::new(PodSandboxStatusRequest {
                pod_sandbox_id: "pod-inspect".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let containers = RuntimeService::list_containers(
            &service,
            Request::new(ListContainersRequest::default()),
        )
        .await
        .unwrap()
        .into_inner();
        let pods = RuntimeService::list_pod_sandbox(
            &service,
            Request::new(ListPodSandboxRequest::default()),
        )
        .await
        .unwrap()
        .into_inner();

        let config: serde_json::Value =
            serde_json::from_str(status.info.get("config").unwrap()).unwrap();
        assert_eq!(config["runtimeFeatures"]["containerEvents"], true);
        assert_eq!(container.status.as_ref().unwrap().reason, "Running");
        assert_eq!(container.status.as_ref().unwrap().mounts.len(), 1);
        assert!(container.info.contains_key("info"));
        assert_eq!(
            pod.status.as_ref().unwrap().network.as_ref().unwrap().ip,
            "10.88.0.10"
        );
        assert_eq!(pod.containers_statuses.len(), 1);
        assert_eq!(containers.containers.len(), 1);
        assert_eq!(pods.items.len(), 1);
    }

    #[tokio::test]
    async fn list_container_stats_applies_id_pod_and_label_filters() {
        let (dir, service) = test_service_with_fake_runtime();

        let mut container_a = test_container("container-alpha-123", "pod-alpha", HashMap::new());
        container_a
            .labels
            .insert("app".to_string(), "api".to_string());
        let mut container_b = test_container("container-bravo-456", "pod-bravo", HashMap::new());
        container_b
            .labels
            .insert("app".to_string(), "worker".to_string());

        {
            let mut pods = service.pod_sandboxes.lock().await;
            pods.insert(
                "pod-alpha".to_string(),
                test_pod("pod-alpha", HashMap::new()),
            );
            pods.insert(
                "pod-bravo".to_string(),
                test_pod("pod-bravo", HashMap::new()),
            );
        }
        {
            let mut containers = service.containers.lock().await;
            containers.insert(container_a.id.clone(), container_a);
            containers.insert(container_b.id.clone(), container_b);
        }
        set_fake_runtime_state(&dir, "container-alpha-123", "running");
        set_fake_runtime_state(&dir, "container-bravo-456", "running");

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "api".to_string());
        let response = RuntimeService::list_container_stats(
            &service,
            Request::new(ListContainerStatsRequest {
                filter: Some(crate::proto::runtime::v1::ContainerStatsFilter {
                    id: "container-alpha".to_string(),
                    pod_sandbox_id: "pod-alpha".to_string(),
                    label_selector: selector,
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.stats.len(), 1);
        assert_eq!(
            response.stats[0]
                .attributes
                .as_ref()
                .expect("container stats should include attributes")
                .id,
            "container-alpha-123"
        );
    }

    #[tokio::test]
    async fn list_containers_returns_empty_when_short_id_filter_is_ambiguous() {
        let service = test_service();

        {
            let mut containers = service.containers.lock().await;
            containers.insert(
                "container-ambiguous-a".to_string(),
                test_container("container-ambiguous-a", "pod-a", HashMap::new()),
            );
            containers.insert(
                "container-ambiguous-b".to_string(),
                test_container("container-ambiguous-b", "pod-b", HashMap::new()),
            );
        }

        let response = RuntimeService::list_containers(
            &service,
            Request::new(ListContainersRequest {
                filter: Some(crate::proto::runtime::v1::ContainerFilter {
                    id: "container-ambiguous".to_string(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(
            response.containers.is_empty(),
            "ambiguous short container ids should behave like cri-o filters and return no entries"
        );
    }

    #[tokio::test]
    async fn container_stats_resolves_short_id_and_list_container_stats_skips_exited() {
        let (dir, service) = test_service_with_fake_runtime();

        {
            let mut containers = service.containers.lock().await;
            containers.insert(
                "container-running-123".to_string(),
                test_container("container-running-123", "pod-1", HashMap::new()),
            );
            containers.insert(
                "container-stopped-456".to_string(),
                test_container("container-stopped-456", "pod-1", HashMap::new()),
            );
        }
        set_fake_runtime_state(&dir, "container-running-123", "running");
        set_fake_runtime_state(&dir, "container-stopped-456", "stopped");

        let stats = RuntimeService::container_stats(
            &service,
            Request::new(ContainerStatsRequest {
                container_id: "container-running".to_string(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .stats
        .expect("short container id should resolve to stats");
        assert_eq!(
            stats
                .attributes
                .as_ref()
                .expect("container stats should include attributes")
                .id,
            "container-running-123"
        );

        let list = RuntimeService::list_container_stats(
            &service,
            Request::new(ListContainerStatsRequest { filter: None }),
        )
        .await
        .unwrap()
        .into_inner();

        let ids = list
            .stats
            .iter()
            .filter_map(|stat| {
                stat.attributes
                    .as_ref()
                    .map(|attributes| attributes.id.clone())
            })
            .collect::<Vec<_>>();
        assert!(
            ids.iter().any(|id| id == "container-running-123"),
            "running containers should remain visible in list stats"
        );
        assert!(
            !ids.iter().any(|id| id == "container-stopped-456"),
            "exited containers should be filtered out from list stats by default"
        );
    }

    #[tokio::test]
    async fn list_pod_sandbox_stats_applies_id_and_label_filters() {
        let service = test_service();

        let mut pod_alpha = test_pod("pod-alpha", HashMap::new());
        pod_alpha
            .labels
            .insert("tier".to_string(), "frontend".to_string());
        let pod_alpha_uid = pod_alpha
            .metadata
            .as_ref()
            .expect("test pod should include metadata")
            .uid
            .clone();

        let mut pod_bravo = test_pod("pod-bravo", HashMap::new());
        pod_bravo
            .labels
            .insert("tier".to_string(), "backend".to_string());
        let pod_bravo_uid = pod_bravo
            .metadata
            .as_ref()
            .expect("test pod should include metadata")
            .uid
            .clone();

        {
            let mut pods = service.pod_sandboxes.lock().await;
            pods.insert("pod-alpha".to_string(), pod_alpha);
            pods.insert("pod-bravo".to_string(), pod_bravo);
        }

        let mut container_alpha_annotations = HashMap::new();
        container_alpha_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_alpha_uid);
        let mut container_bravo_annotations = HashMap::new();
        container_bravo_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_bravo_uid);
        {
            let mut containers = service.containers.lock().await;
            containers.insert(
                "container-alpha".to_string(),
                test_container("container-alpha", "pod-alpha", container_alpha_annotations),
            );
            containers.insert(
                "container-bravo".to_string(),
                test_container("container-bravo", "pod-bravo", container_bravo_annotations),
            );
        }

        let mut selector = HashMap::new();
        selector.insert("tier".to_string(), "frontend".to_string());
        let response = RuntimeService::list_pod_sandbox_stats(
            &service,
            Request::new(ListPodSandboxStatsRequest {
                filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                    id: "pod-al".to_string(),
                    label_selector: selector,
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.stats.len(), 1);
        assert_eq!(
            response.stats[0]
                .attributes
                .as_ref()
                .expect("pod stats should include attributes")
                .id,
            "pod-alpha"
        );
    }

    #[tokio::test]
    async fn pod_sandbox_stats_resolves_short_id_and_hides_internal_annotations() {
        let (dir, service) = test_service_with_fake_runtime();

        let mut pod_annotations = HashMap::new();
        pod_annotations.insert("visible".to_string(), "true".to_string());
        RuntimeServiceImpl::insert_internal_state(
            &mut pod_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                pause_container_id: Some("pause-pod-short-123".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        service.pod_sandboxes.lock().await.insert(
            "pod-short-123".to_string(),
            test_pod("pod-short-123", pod_annotations),
        );
        service.containers.lock().await.insert(
            "container-pod-short-123".to_string(),
            test_container("container-pod-short-123", "pod-short-123", HashMap::new()),
        );
        set_fake_runtime_state(&dir, "container-pod-short-123", "running");
        set_fake_runtime_state(&dir, "pause-pod-short-123", "running");

        let response = RuntimeService::pod_sandbox_stats(
            &service,
            Request::new(PodSandboxStatsRequest {
                pod_sandbox_id: "pod-short".to_string(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let stats = response
            .stats
            .expect("short pod id should resolve to pod sandbox stats");
        let attributes = stats
            .attributes
            .expect("pod stats should include attributes");
        assert_eq!(attributes.id, "pod-short-123");
        assert_eq!(
            attributes.annotations.get("visible").map(String::as_str),
            Some("true")
        );
        assert!(
            !attributes.annotations.contains_key(INTERNAL_POD_STATE_KEY),
            "internal pod annotations should not leak through statsp responses"
        );
    }

    #[tokio::test]
    async fn list_pod_sandbox_stats_returns_empty_when_short_id_filter_is_ambiguous() {
        let service = test_service();

        {
            let mut pods = service.pod_sandboxes.lock().await;
            pods.insert(
                "pod-ambiguous-a".to_string(),
                test_pod("pod-ambiguous-a", HashMap::new()),
            );
            pods.insert(
                "pod-ambiguous-b".to_string(),
                test_pod("pod-ambiguous-b", HashMap::new()),
            );
        }

        let response = RuntimeService::list_pod_sandbox_stats(
            &service,
            Request::new(ListPodSandboxStatsRequest {
                filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                    id: "pod-ambiguous".to_string(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(
            response.stats.is_empty(),
            "ambiguous short pod ids should return no stats entries"
        );
    }

    #[tokio::test]
    async fn list_metric_descriptors_returns_non_empty_supported_names() {
        let service = test_service();
        let response = RuntimeService::list_metric_descriptors(
            &service,
            Request::new(ListMetricDescriptorsRequest {}),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(
            !response.descriptors.is_empty(),
            "metric descriptors should not be empty"
        );
        let names: HashSet<String> = response
            .descriptors
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect();
        assert!(names.contains("container_cpu_usage_seconds_total"));
        assert!(names.contains("container_memory_working_set_bytes"));
        assert!(names.contains("container_memory_usage_bytes"));
        assert!(names.contains("container_spec_memory_limit_bytes"));
    }

    #[tokio::test]
    async fn list_pod_sandbox_metrics_returns_pod_and_container_entries() {
        let service = test_service();

        let pod = test_pod("pod-metrics", HashMap::new());
        let pod_uid = pod
            .metadata
            .as_ref()
            .expect("test pod should include metadata")
            .uid
            .clone();
        service
            .pod_sandboxes
            .lock()
            .await
            .insert("pod-metrics".to_string(), pod);

        let mut container_annotations = HashMap::new();
        container_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_uid);
        service.containers.lock().await.insert(
            "container-metrics".to_string(),
            test_container("container-metrics", "pod-metrics", container_annotations),
        );

        let response = RuntimeService::list_pod_sandbox_metrics(
            &service,
            Request::new(ListPodSandboxMetricsRequest {}),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.pod_metrics.len(), 1);
        let pod_metrics = &response.pod_metrics[0];
        assert_eq!(pod_metrics.pod_sandbox_id, "pod-metrics");
        assert!(
            !pod_metrics.metrics.is_empty(),
            "pod-level metrics should not be empty"
        );
        assert_eq!(pod_metrics.container_metrics.len(), 1);
        assert_eq!(
            pod_metrics.container_metrics[0].container_id,
            "container-metrics"
        );
        assert!(
            !pod_metrics.container_metrics[0].metrics.is_empty(),
            "container-level metrics should not be empty"
        );
    }

    #[tokio::test]
    async fn get_container_events_streams_broadcast_events() {
        let service = test_service();
        let mut stream =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        service.publish_event(ContainerEventResponse {
            container_id: "container-1".to_string(),
            container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
            created_at: 1,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });

        let event = stream.next().await.unwrap().unwrap();
        assert_eq!(event.container_id, "container-1");
        assert_eq!(
            event.container_event_type,
            ContainerEventType::ContainerCreatedEvent as i32
        );
    }

    #[tokio::test]
    async fn get_container_events_broadcasts_to_multiple_subscribers() {
        let service = test_service();
        let mut stream_a =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();
        let mut stream_b =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        service.publish_event(ContainerEventResponse {
            container_id: "shared".to_string(),
            container_event_type: ContainerEventType::ContainerStartedEvent as i32,
            created_at: 2,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });

        let event_a = stream_a.next().await.unwrap().unwrap();
        let event_b = stream_b.next().await.unwrap().unwrap();
        assert_eq!(event_a.container_id, "shared");
        assert_eq!(event_b.container_id, "shared");
        assert_eq!(event_a.container_event_type, event_b.container_event_type);
    }

    #[tokio::test]
    async fn get_container_events_preserves_publish_order() {
        let service = test_service();
        let mut stream =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        for (id, event_type) in [
            ("created", ContainerEventType::ContainerCreatedEvent),
            ("started", ContainerEventType::ContainerStartedEvent),
            ("stopped", ContainerEventType::ContainerStoppedEvent),
            ("deleted", ContainerEventType::ContainerDeletedEvent),
        ] {
            service.publish_event(ContainerEventResponse {
                container_id: id.to_string(),
                container_event_type: event_type as i32,
                created_at: RuntimeServiceImpl::now_nanos(),
                pod_sandbox_status: None,
                containers_statuses: Vec::new(),
            });
        }

        let mut received = Vec::new();
        for _ in 0..4 {
            let event = stream.next().await.unwrap().unwrap();
            received.push((event.container_id, event.container_event_type));
        }

        assert_eq!(
            received,
            vec![
                (
                    "created".to_string(),
                    ContainerEventType::ContainerCreatedEvent as i32
                ),
                (
                    "started".to_string(),
                    ContainerEventType::ContainerStartedEvent as i32
                ),
                (
                    "stopped".to_string(),
                    ContainerEventType::ContainerStoppedEvent as i32
                ),
                (
                    "deleted".to_string(),
                    ContainerEventType::ContainerDeletedEvent as i32
                ),
            ]
        );
    }

    #[tokio::test]
    async fn pod_events_use_pause_container_id_when_available_and_preserve_order() {
        let service = test_service();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                pause_container_id: Some("pause-event-1".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        let pod = test_pod("pod-event-1", annotations);

        let mut stream =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        service
            .emit_pod_event(ContainerEventType::ContainerCreatedEvent, &pod, Vec::new())
            .await;
        service
            .emit_pod_event(ContainerEventType::ContainerStartedEvent, &pod, Vec::new())
            .await;

        let created = stream.next().await.unwrap().unwrap();
        let started = stream.next().await.unwrap().unwrap();
        assert_eq!(created.container_id, "pause-event-1");
        assert_eq!(started.container_id, "pause-event-1");
        assert_eq!(
            created.container_event_type,
            ContainerEventType::ContainerCreatedEvent as i32
        );
        assert_eq!(
            started.container_event_type,
            ContainerEventType::ContainerStartedEvent as i32
        );
        assert_eq!(
            started
                .pod_sandbox_status
                .expect("pod status should be included")
                .id,
            "pod-event-1"
        );
    }

    #[tokio::test]
    async fn publish_event_without_subscribers_does_not_panic() {
        let service = test_service();
        service.publish_event(ContainerEventResponse {
            container_id: "orphan".to_string(),
            container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
            created_at: 1,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    #[tokio::test]
    async fn get_container_events_reports_lagged_consumers() {
        let service = test_service();
        let mut stream =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        for idx in 0..600 {
            service.publish_event(ContainerEventResponse {
                container_id: format!("container-{}", idx),
                container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
                created_at: idx,
                pod_sandbox_status: None,
                containers_statuses: Vec::new(),
            });
        }

        let mut saw_lagged = false;
        for _ in 0..260 {
            match timeout(Duration::from_millis(200), stream.next()).await {
                Ok(Some(Err(status))) if status.code() == tonic::Code::ResourceExhausted => {
                    saw_lagged = true;
                    break;
                }
                Ok(Some(_)) => continue,
                _ => break,
            }
        }

        assert!(saw_lagged, "expected lagged consumer error");
    }

    #[tokio::test]
    async fn exit_monitor_publishes_async_stop_events() {
        let (dir, service) = test_service_with_fake_runtime();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState::default(),
        )
        .unwrap();
        service.containers.lock().await.insert(
            "async-stop".to_string(),
            Container {
                state: ContainerState::ContainerRunning as i32,
                ..test_container("async-stop", "pod-1", annotations)
            },
        );
        service.ensure_exit_monitor_registered("async-stop");

        let mut stream =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        let exit_code_path = dir
            .path()
            .join("shims")
            .join("async-stop")
            .join("exit_code");
        fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
        fs::write(&exit_code_path, "17").unwrap();

        let event = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(Ok(event)) = stream.next().await {
                    if event.container_id == "async-stop"
                        && event.container_event_type
                            == ContainerEventType::ContainerStoppedEvent as i32
                    {
                        return event;
                    }
                }
            }
        })
        .await
        .expect("timed out waiting for async stop event");

        assert_eq!(event.container_id, "async-stop");
        let container = service
            .containers
            .lock()
            .await
            .get("async-stop")
            .cloned()
            .unwrap();
        assert_eq!(container.state, ContainerState::ContainerExited as i32);
        let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        )
        .unwrap();
        assert_eq!(state.exit_code, Some(17));
    }

    #[tokio::test]
    async fn port_forward_validates_pod_state() {
        let service = test_service();
        service
            .pod_sandboxes
            .lock()
            .await
            .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

        let mut not_ready_pod = test_pod("pod-2", HashMap::new());
        not_ready_pod.state = PodSandboxState::SandboxNotready as i32;
        service
            .pod_sandboxes
            .lock()
            .await
            .insert("pod-2".to_string(), not_ready_pod);
        let not_ready = RuntimeService::port_forward(
            &service,
            Request::new(PortForwardRequest {
                pod_sandbox_id: "pod-2".to_string(),
                port: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(not_ready.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn port_forward_requires_existing_netns_and_returns_stream_url() {
        let service = test_service();
        let mut missing_netns_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut missing_netns_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        service.pod_sandboxes.lock().await.insert(
            "pod-missing-netns".to_string(),
            test_pod("pod-missing-netns", missing_netns_annotations),
        );
        let missing_netns = RuntimeService::port_forward(
            &service,
            Request::new(PortForwardRequest {
                pod_sandbox_id: "pod-missing-netns".to_string(),
                port: vec![80],
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing_netns.code(), tonic::Code::FailedPrecondition);

        let netns_dir = tempdir().unwrap();
        let netns_path = netns_dir.path().join("netns");
        fs::write(&netns_path, "placeholder").unwrap();
        let mut ready_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut ready_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                netns_path: Some(netns_path.display().to_string()),
                runtime_handler: "runc".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        service.pod_sandboxes.lock().await.insert(
            "pod-ready".to_string(),
            test_pod("pod-ready", ready_annotations),
        );
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;
        let response = RuntimeService::port_forward(
            &service,
            Request::new(PortForwardRequest {
                pod_sandbox_id: "pod-ready".to_string(),
                port: vec![80],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.url.contains("/portforward/"));
    }

    #[tokio::test]
    async fn port_forward_refreshes_stale_pause_container_state() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "pause-portforward", "stopped");

        let netns_dir = tempdir().unwrap();
        let netns_path = netns_dir.path().join("netns");
        fs::write(&netns_path, "placeholder").unwrap();
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                netns_path: Some(netns_path.display().to_string()),
                runtime_handler: "runc".to_string(),
                pause_container_id: Some("pause-portforward".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service.pod_sandboxes.lock().await.insert(
            "pod-stale-ready".to_string(),
            crate::proto::runtime::v1::PodSandbox {
                state: PodSandboxState::SandboxReady as i32,
                ..test_pod("pod-stale-ready", annotations)
            },
        );
        service
            .set_streaming_server(crate::streaming::StreamingServer::for_test(
                "http://127.0.0.1:12345",
            ))
            .await;

        let err = RuntimeService::port_forward(
            &service,
            Request::new(PortForwardRequest {
                pod_sandbox_id: "pod-stale-ready".to_string(),
                port: vec![80],
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("not ready"));
    }

    #[tokio::test]
    async fn stop_and_remove_existing_container_support_repeat_calls() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "container-1", "running");
        service.containers.lock().await.insert(
            "container-1".to_string(),
            test_container("container-1", "pod-1", HashMap::new()),
        );

        RuntimeService::stop_container(
            &service,
            Request::new(StopContainerRequest {
                container_id: "container-1".to_string(),
                timeout: 1,
            }),
        )
        .await
        .unwrap();
        RuntimeService::stop_container(
            &service,
            Request::new(StopContainerRequest {
                container_id: "container-1".to_string(),
                timeout: 1,
            }),
        )
        .await
        .unwrap();

        set_fake_runtime_state(&dir, "container-2", "running");
        service.containers.lock().await.insert(
            "container-2".to_string(),
            test_container("container-2", "pod-1", HashMap::new()),
        );
        RuntimeService::remove_container(
            &service,
            Request::new(RemoveContainerRequest {
                container_id: "container-2".to_string(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::remove_container(
            &service,
            Request::new(RemoveContainerRequest {
                container_id: "container-2".to_string(),
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stop_and_remove_existing_pod_support_repeat_calls() {
        let (dir, service) = test_service_with_fake_runtime();

        let pod = test_pod("pod-1", HashMap::new());
        service
            .pod_sandboxes
            .lock()
            .await
            .insert("pod-1".to_string(), pod);
        RuntimeService::stop_pod_sandbox(
            &service,
            Request::new(StopPodSandboxRequest {
                pod_sandbox_id: "pod-1".to_string(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::stop_pod_sandbox(
            &service,
            Request::new(StopPodSandboxRequest {
                pod_sandbox_id: "pod-1".to_string(),
            }),
        )
        .await
        .unwrap();

        set_fake_runtime_state(&dir, "container-under-pod", "running");
        service.containers.lock().await.insert(
            "container-under-pod".to_string(),
            test_container("container-under-pod", "pod-2", HashMap::new()),
        );
        service
            .pod_sandboxes
            .lock()
            .await
            .insert("pod-2".to_string(), test_pod("pod-2", HashMap::new()));
        RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: "pod-2".to_string(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: "pod-2".to_string(),
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn recover_state_reconciles_running_container_to_exited() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "recover-container", "stopped");

        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                metadata_name: Some("recover".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        service
            .persistence
            .lock()
            .await
            .save_container(
                "recover-container",
                "pod-1",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &annotations,
            )
            .unwrap();

        service.recover_state().await.unwrap();

        let container = service
            .containers
            .lock()
            .await
            .get("recover-container")
            .cloned()
            .unwrap();
        assert_eq!(container.state, ContainerState::ContainerExited as i32);
        let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        )
        .unwrap();
        assert_eq!(state.exit_code, Some(0));
        assert!(state.finished_at.is_some());

        let persisted = service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("recover-container")
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, "stopped");
    }

    #[tokio::test]
    async fn recover_state_marks_ready_pod_notready_when_pause_is_stopped() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "pause-1", "stopped");
        let netns_path = dir.path().join("pause-netns");
        fs::write(&netns_path, "netns").unwrap();

        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                netns_path: Some(netns_path.display().to_string()),
                pause_container_id: Some("pause-1".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        service
            .persistence
            .lock()
            .await
            .storage_mut()
            .save_pod_sandbox(&PodSandboxRecord {
                id: "pod-recover".to_string(),
                state: "ready".to_string(),
                name: "pod-recover".to_string(),
                namespace: "default".to_string(),
                uid: "uid-1".to_string(),
                created_at: RuntimeServiceImpl::now_nanos(),
                netns_path: netns_path.display().to_string(),
                labels: "{}".to_string(),
                annotations: serde_json::to_string(&annotations).unwrap(),
                pause_container_id: Some("pause-1".to_string()),
                ip: None,
            })
            .unwrap();

        service.recover_state().await.unwrap();

        let pod = service
            .pod_sandboxes
            .lock()
            .await
            .get("pod-recover")
            .cloned()
            .unwrap();
        assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);
        let persisted = service
            .persistence
            .lock()
            .await
            .storage()
            .get_pod_sandbox("pod-recover")
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, "notready");
    }

    #[tokio::test]
    async fn recover_state_does_not_replay_historical_events() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "recover-container", "stopped");

        service
            .persistence
            .lock()
            .await
            .storage_mut()
            .save_container(&ContainerRecord {
                id: "recover-container".to_string(),
                pod_id: "pod-1".to_string(),
                state: "running".to_string(),
                image: "busybox:latest".to_string(),
                command: String::new(),
                created_at: RuntimeServiceImpl::now_nanos(),
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                exit_code: None,
                exit_time: None,
            })
            .unwrap();

        let mut stream =
            RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
                .await
                .unwrap()
                .into_inner();

        service.recover_state().await.unwrap();

        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "recover_state should not replay historical events"
        );
    }

    #[tokio::test]
    async fn recover_state_supports_inspect_and_list_after_restart() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "recover-container", "running");
        set_fake_runtime_state(&dir, "pause-recover", "running");
        let netns_path = dir.path().join("recover.netns");
        fs::write(&netns_path, "netns").unwrap();

        let mut pod_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut pod_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                netns_path: Some(netns_path.display().to_string()),
                pause_container_id: Some("pause-recover".to_string()),
                ip: Some("10.88.0.20".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .persistence
            .lock()
            .await
            .storage_mut()
            .save_pod_sandbox(&PodSandboxRecord {
                id: "pod-recover".to_string(),
                state: "ready".to_string(),
                name: "pod-recover".to_string(),
                namespace: "default".to_string(),
                uid: "uid-recover".to_string(),
                created_at: RuntimeServiceImpl::now_nanos(),
                netns_path: netns_path.display().to_string(),
                labels: "{}".to_string(),
                annotations: serde_json::to_string(&pod_annotations).unwrap(),
                pause_container_id: Some("pause-recover".to_string()),
                ip: Some("10.88.0.20".to_string()),
            })
            .unwrap();

        let mut container_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut container_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                metadata_name: Some("recover-container".to_string()),
                log_path: Some(
                    dir.path()
                        .join("logs")
                        .join("recover.log")
                        .display()
                        .to_string(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .persistence
            .lock()
            .await
            .save_container(
                "recover-container",
                "pod-recover",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &container_annotations,
            )
            .unwrap();

        service.recover_state().await.unwrap();

        let container_status = RuntimeService::container_status(
            &service,
            Request::new(ContainerStatusRequest {
                container_id: "recover-container".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let pod_status = RuntimeService::pod_sandbox_status(
            &service,
            Request::new(PodSandboxStatusRequest {
                pod_sandbox_id: "pod-recover".to_string(),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let containers = RuntimeService::list_containers(
            &service,
            Request::new(ListContainersRequest::default()),
        )
        .await
        .unwrap()
        .into_inner();
        let pods = RuntimeService::list_pod_sandbox(
            &service,
            Request::new(ListPodSandboxRequest::default()),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(
            container_status.status.as_ref().unwrap().state,
            ContainerState::ContainerRunning as i32
        );
        assert_eq!(
            pod_status.status.as_ref().unwrap().state,
            PodSandboxState::SandboxReady as i32
        );
        assert_eq!(containers.containers.len(), 1);
        assert_eq!(containers.containers[0].pod_sandbox_id, "pod-recover");
        assert_eq!(pods.items.len(), 1);
        let info: serde_json::Value =
            serde_json::from_str(container_status.info.get("info").unwrap()).unwrap();
        assert_eq!(info["sandboxID"], "pod-recover");
    }

    #[tokio::test]
    async fn recover_state_supports_stop_and_remove_after_restart() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "recover-container", "running");
        set_fake_runtime_state(&dir, "pause-recover", "running");
        let netns_path = dir.path().join("recover-remove.netns");
        fs::write(&netns_path, "netns").unwrap();

        let mut pod_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut pod_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                netns_path: Some(netns_path.display().to_string()),
                pause_container_id: Some("pause-recover".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .persistence
            .lock()
            .await
            .storage_mut()
            .save_pod_sandbox(&PodSandboxRecord {
                id: "pod-recover".to_string(),
                state: "ready".to_string(),
                name: "pod-recover".to_string(),
                namespace: "default".to_string(),
                uid: "uid-recover".to_string(),
                created_at: RuntimeServiceImpl::now_nanos(),
                netns_path: netns_path.display().to_string(),
                labels: "{}".to_string(),
                annotations: serde_json::to_string(&pod_annotations).unwrap(),
                pause_container_id: Some("pause-recover".to_string()),
                ip: None,
            })
            .unwrap();
        service
            .persistence
            .lock()
            .await
            .save_container(
                "recover-container",
                "pod-recover",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        service.recover_state().await.unwrap();

        RuntimeService::stop_container(
            &service,
            Request::new(StopContainerRequest {
                container_id: "recover-container".to_string(),
                timeout: 1,
            }),
        )
        .await
        .unwrap();
        RuntimeService::remove_container(
            &service,
            Request::new(RemoveContainerRequest {
                container_id: "recover-container".to_string(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::stop_pod_sandbox(
            &service,
            Request::new(StopPodSandboxRequest {
                pod_sandbox_id: "pod-recover".to_string(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: "pod-recover".to_string(),
            }),
        )
        .await
        .unwrap();

        assert!(service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("recover-container")
            .unwrap()
            .is_none());
        assert!(service
            .persistence
            .lock()
            .await
            .storage()
            .get_pod_sandbox("pod-recover")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn recover_state_marks_ready_pod_notready_when_netns_is_missing() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "pause-missing-netns", "running");

        let missing_netns_path = dir.path().join("missing.netns");
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                netns_path: Some(missing_netns_path.display().to_string()),
                pause_container_id: Some("pause-missing-netns".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        service
            .persistence
            .lock()
            .await
            .storage_mut()
            .save_pod_sandbox(&PodSandboxRecord {
                id: "pod-missing-netns".to_string(),
                state: "ready".to_string(),
                name: "pod-missing-netns".to_string(),
                namespace: "default".to_string(),
                uid: "uid-missing-netns".to_string(),
                created_at: RuntimeServiceImpl::now_nanos(),
                netns_path: missing_netns_path.display().to_string(),
                labels: "{}".to_string(),
                annotations: serde_json::to_string(&annotations).unwrap(),
                pause_container_id: Some("pause-missing-netns".to_string()),
                ip: Some("10.88.0.21".to_string()),
            })
            .unwrap();

        service.recover_state().await.unwrap();

        let pod = service
            .pod_sandboxes
            .lock()
            .await
            .get("pod-missing-netns")
            .cloned()
            .unwrap();
        assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);

        let persisted = service
            .persistence
            .lock()
            .await
            .storage()
            .get_pod_sandbox("pod-missing-netns")
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, "notready");
    }

    #[tokio::test]
    async fn remove_pod_sandbox_cascades_recovered_containers_after_restart() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "recover-container", "running");
        set_fake_runtime_state(&dir, "pause-recover", "running");
        let netns_path = dir.path().join("recover-cascade.netns");
        fs::write(&netns_path, "netns").unwrap();

        let mut pod_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut pod_annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                netns_path: Some(netns_path.display().to_string()),
                pause_container_id: Some("pause-recover".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .persistence
            .lock()
            .await
            .storage_mut()
            .save_pod_sandbox(&PodSandboxRecord {
                id: "pod-recover".to_string(),
                state: "ready".to_string(),
                name: "pod-recover".to_string(),
                namespace: "default".to_string(),
                uid: "uid-recover".to_string(),
                created_at: RuntimeServiceImpl::now_nanos(),
                netns_path: netns_path.display().to_string(),
                labels: "{}".to_string(),
                annotations: serde_json::to_string(&pod_annotations).unwrap(),
                pause_container_id: Some("pause-recover".to_string()),
                ip: None,
            })
            .unwrap();

        let mut container_annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut container_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &StoredContainerState {
                metadata_name: Some("recover-container".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .persistence
            .lock()
            .await
            .save_container(
                "recover-container",
                "pod-recover",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &container_annotations,
            )
            .unwrap();

        service.recover_state().await.unwrap();

        RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: "pod-recover".to_string(),
            }),
        )
        .await
        .unwrap();

        assert!(service
            .containers
            .lock()
            .await
            .get("recover-container")
            .is_none());
        assert!(service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("recover-container")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn container_status_refreshes_stale_runtime_state_on_query() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "refresh-container", "stopped");

        service.containers.lock().await.insert(
            "refresh-container".to_string(),
            Container {
                state: ContainerState::ContainerRunning as i32,
                ..test_container("refresh-container", "pod-1", HashMap::new())
            },
        );

        let response = RuntimeService::container_status(
            &service,
            Request::new(ContainerStatusRequest {
                container_id: "refresh-container".to_string(),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(
            response.status.unwrap().state,
            ContainerState::ContainerExited as i32
        );
    }

    #[tokio::test]
    async fn pod_queries_refresh_pause_container_state_on_query() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "pause-refresh", "stopped");

        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                pause_container_id: Some("pause-refresh".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        service.pod_sandboxes.lock().await.insert(
            "pod-refresh".to_string(),
            crate::proto::runtime::v1::PodSandbox {
                state: PodSandboxState::SandboxReady as i32,
                ..test_pod("pod-refresh", annotations)
            },
        );

        let list = RuntimeService::list_pod_sandbox(
            &service,
            Request::new(ListPodSandboxRequest::default()),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].state, PodSandboxState::SandboxNotready as i32);

        let inspect = RuntimeService::pod_sandbox_status(
            &service,
            Request::new(PodSandboxStatusRequest {
                pod_sandbox_id: "pod-refresh".to_string(),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            inspect.status.unwrap().state,
            PodSandboxState::SandboxNotready as i32
        );
    }

    #[tokio::test]
    async fn recover_state_ignores_stale_shim_metadata_artifacts() {
        let (dir, service) = test_service_with_fake_runtime();
        set_fake_runtime_state(&dir, "recover-container", "running");

        let stale_dir = dir.path().join("shims").join("orphan");
        fs::create_dir_all(&stale_dir).unwrap();
        fs::write(stale_dir.join("attach.sock"), "stale").unwrap();
        fs::write(
            stale_dir.join("shim.json"),
            serde_json::to_vec_pretty(&crate::runtime::ShimProcess {
                container_id: "orphan".to_string(),
                shim_pid: 999_999,
                exit_code_file: stale_dir.join("exit_code"),
                log_file: stale_dir.join("shim.log"),
                socket_path: stale_dir.join("attach.sock"),
                bundle_path: dir.path().join("bundles").join("orphan"),
            })
            .unwrap(),
        )
        .unwrap();

        service
            .persistence
            .lock()
            .await
            .save_container(
                "recover-container",
                "pod-1",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();

        service.recover_state().await.unwrap();

        let containers = RuntimeService::list_containers(
            &service,
            Request::new(ListContainersRequest::default()),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(containers.containers.len(), 1);
        assert!(
            !stale_dir.exists(),
            "recover_state should clean orphaned shim artifacts"
        );
    }
}
