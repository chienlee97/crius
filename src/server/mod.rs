use anyhow::Context;
use async_trait::async_trait;
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
    UpdateContainerResourcesRequest, UpdateContainerResourcesResponse,
    UpdatePodSandboxResourcesRequest, UpdatePodSandboxResourcesResponse,
    UpdateRuntimeConfigRequest, UpdateRuntimeConfigResponse,
};
use crate::storage::persistence::{PersistenceConfig, PersistenceManager};

use crate::config::{NriAnnotationWorkloadConfig, NriConfig};
use crate::metrics::MetricsCollector;
use crate::network::{CniConfig, DefaultNetworkManager, NetworkManager};
use crate::nri::{
    apply_container_adjustment_with_blockio_config, disallowed_annotation_adjustment_keys,
    filter_annotation_adjustments_by_allowlist, linux_resources_from_cri, oci_args, oci_env,
    oci_hooks, oci_linux_container, oci_mounts, oci_rlimits, oci_user, resolve_blockio_class,
    resolve_rdt_class, validate_adjustment_resources_with_min_memory,
    validate_container_adjustment, validate_container_update, validate_update_linux_resources,
    NopNri, NriApi, NriContainerEvent, NriCreateContainerResult, NriDomain, NriManager,
    NriManagerConfig, NriPodEvent, NriStopContainerResult, RuntimeSnapshot,
};
use crate::pod::{PodSandboxConfig, PodSandboxManager};
use crate::runtime::{
    default_shim_work_dir, ContainerConfig, ContainerRuntime, ContainerStatus, DeviceMapping,
    MountConfig, NamespacePaths, RuncRuntime, SeccompProfile, ShimConfig, ShimProcess,
};
use crate::streaming::StreamingServer;

mod annotations;
mod container_handlers;
mod events;
mod pod_handlers;
mod recovery;
mod responses;
mod service;
mod stats;
mod status;
mod streaming_handlers;

pub use service::{RuntimeConfig, RuntimeServiceImpl};

const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";
const INTERNAL_POD_STATE_KEY: &str = "io.crius.internal/pod-state";
const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";
const INTERNAL_CHECKPOINT_RESTORE_KEY: &str = "io.crius.internal/checkpoint-restore";
const CHECKPOINT_LOCATION_ANNOTATION_KEY: &str = "io.crius.checkpoint.location";
const CRIO_LABELS_ANNOTATION: &str = "io.kubernetes.cri-o.Labels";
const CRIO_CONTAINER_ID_ANNOTATION: &str = "io.kubernetes.cri-o.ContainerID";
const CRIO_CONTAINER_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.ContainerName";
const CRIO_CONTAINER_TYPE_ANNOTATION: &str = "io.kubernetes.cri-o.ContainerType";
const CRIO_USER_REQUESTED_IMAGE_ANNOTATION: &str = "io.kubernetes.cri-o.Image";
const CRIO_IMAGE_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.ImageName";
const CRIO_LOG_PATH_ANNOTATION: &str = "io.kubernetes.cri-o.LogPath";
const CRIO_RUNTIME_HANDLER_ANNOTATION: &str = "io.kubernetes.cri-o.RuntimeHandler";
const CRIO_SANDBOX_ID_ANNOTATION: &str = "io.kubernetes.cri-o.SandboxID";
const CRIO_SANDBOX_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.SandboxName";
const CRIO_POD_NAME_ANNOTATION: &str = "io.kubernetes.cri-o.Name";
const CRIO_POD_NAMESPACE_ANNOTATION: &str = "io.kubernetes.cri-o.Namespace";
const CONTAINERD_CONTAINER_TYPE_ANNOTATION: &str = "io.kubernetes.cri.container-type";
const CONTAINERD_IMAGE_NAME_ANNOTATION: &str = "io.kubernetes.cri.image-name";
const CONTAINERD_SANDBOX_ID_ANNOTATION: &str = "io.kubernetes.cri.sandbox-id";
const CONTAINERD_SANDBOX_NAME_ANNOTATION: &str = "io.kubernetes.cri.sandbox-name";
const CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION: &str = "io.kubernetes.cri.sandbox-namespace";
const CONTAINERD_SANDBOX_UID_ANNOTATION: &str = "io.kubernetes.cri.sandbox-uid";
const CONTAINERD_CONTAINER_NAME_ANNOTATION: &str = "io.kubernetes.cri.container-name";
const CONTAINERD_RUNTIME_HANDLER_ANNOTATION: &str = "io.containerd.cri.runtime-handler";
const KUBERNETES_CONTAINER_NAME_ANNOTATION: &str = "io.kubernetes.container.name";
const CONTAINER_TYPE_CONTAINER: &str = "container";
const NRI_ALLOWED_ANNOTATION_PREFIXES_ENV: &str = "CRIUS_NRI_ALLOWED_ANNOTATION_PREFIXES";
const NRI_MIN_MEMORY_LIMIT_ENV: &str = "CRIUS_NRI_CONTAINER_MIN_MEMORY_BYTES";

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
struct StoredLinuxDeviceCgroup {
    allow: bool,
    device_type: Option<String>,
    major: Option<i64>,
    minor: Option<i64>,
    access: Option<String>,
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
    memory_reservation_in_bytes: Option<i64>,
    memory_kernel_limit_in_bytes: Option<i64>,
    memory_kernel_tcp_limit_in_bytes: Option<i64>,
    memory_swappiness: Option<u64>,
    memory_disable_oom_killer: Option<bool>,
    memory_use_hierarchy: Option<bool>,
    cpu_realtime_runtime: Option<i64>,
    cpu_realtime_period: Option<u64>,
    pids_limit: Option<i64>,
    devices: Vec<StoredLinuxDeviceCgroup>,
    blockio_class: Option<String>,
    rdt_class: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CgroupResourceSupport {
    swap: bool,
    hugetlb: bool,
    memory_kernel: bool,
    memory_kernel_tcp: bool,
    memory_swappiness: bool,
    memory_disable_oom_killer: bool,
    memory_use_hierarchy: bool,
    cpu_realtime: bool,
    blockio: bool,
    rdt: bool,
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

    fn optional_int64(value: i64) -> protobuf::MessageField<crate::nri_proto::api::OptionalInt64> {
        let mut result = crate::nri_proto::api::OptionalInt64::new();
        result.value = value;
        protobuf::MessageField::some(result)
    }

    fn optional_uint64(
        value: u64,
    ) -> protobuf::MessageField<crate::nri_proto::api::OptionalUInt64> {
        let mut result = crate::nri_proto::api::OptionalUInt64::new();
        result.value = value;
        protobuf::MessageField::some(result)
    }

    fn optional_bool(value: bool) -> protobuf::MessageField<crate::nri_proto::api::OptionalBool> {
        let mut result = crate::nri_proto::api::OptionalBool::new();
        result.value = value;
        protobuf::MessageField::some(result)
    }

    fn optional_string(
        value: impl Into<String>,
    ) -> protobuf::MessageField<crate::nri_proto::api::OptionalString> {
        let mut result = crate::nri_proto::api::OptionalString::new();
        result.value = value.into();
        protobuf::MessageField::some(result)
    }

    fn apply_nri(&mut self, resources: &crate::nri_proto::api::LinuxResources) {
        if let Some(memory) = resources.memory.as_ref() {
            if let Some(limit) = memory.limit.as_ref() {
                self.memory_limit_in_bytes = limit.value;
            }
            if let Some(reservation) = memory.reservation.as_ref() {
                self.memory_reservation_in_bytes = Some(reservation.value);
            }
            if let Some(swap) = memory.swap.as_ref() {
                self.memory_swap_limit_in_bytes = swap.value;
            }
            if let Some(kernel) = memory.kernel.as_ref() {
                self.memory_kernel_limit_in_bytes = Some(kernel.value);
            }
            if let Some(kernel_tcp) = memory.kernel_tcp.as_ref() {
                self.memory_kernel_tcp_limit_in_bytes = Some(kernel_tcp.value);
            }
            if let Some(swappiness) = memory.swappiness.as_ref() {
                self.memory_swappiness = Some(swappiness.value);
            }
            if let Some(disable_oom_killer) = memory.disable_oom_killer.as_ref() {
                self.memory_disable_oom_killer = Some(disable_oom_killer.value);
            }
            if let Some(use_hierarchy) = memory.use_hierarchy.as_ref() {
                self.memory_use_hierarchy = Some(use_hierarchy.value);
            }
        }

        if let Some(cpu) = resources.cpu.as_ref() {
            if let Some(shares) = cpu.shares.as_ref() {
                self.cpu_shares = shares.value as i64;
            }
            if let Some(quota) = cpu.quota.as_ref() {
                self.cpu_quota = quota.value;
            }
            if let Some(period) = cpu.period.as_ref() {
                self.cpu_period = period.value as i64;
            }
            if let Some(runtime) = cpu.realtime_runtime.as_ref() {
                self.cpu_realtime_runtime = Some(runtime.value);
            }
            if let Some(period) = cpu.realtime_period.as_ref() {
                self.cpu_realtime_period = Some(period.value);
            }
            if !cpu.cpus.is_empty() {
                self.cpuset_cpus = cpu.cpus.clone();
            }
            if !cpu.mems.is_empty() {
                self.cpuset_mems = cpu.mems.clone();
            }
        }

        for limit in &resources.hugepage_limits {
            if let Some(existing) = self
                .hugepage_limits
                .iter_mut()
                .find(|existing| existing.page_size == limit.page_size)
            {
                existing.limit = limit.limit;
            } else {
                self.hugepage_limits.push(StoredHugepageLimit {
                    page_size: limit.page_size.clone(),
                    limit: limit.limit,
                });
            }
        }

        if let Some(pids) = resources.pids.as_ref() {
            self.pids_limit = Some(pids.limit);
        }
        if !resources.devices.is_empty() {
            self.devices = resources
                .devices
                .iter()
                .map(|device| StoredLinuxDeviceCgroup {
                    allow: device.allow,
                    device_type: (!device.type_.is_empty()).then(|| device.type_.clone()),
                    major: device.major.as_ref().map(|value| value.value),
                    minor: device.minor.as_ref().map(|value| value.value),
                    access: (!device.access.is_empty()).then(|| device.access.clone()),
                })
                .collect();
        }
        if let Some(blockio_class) = resources.blockio_class.as_ref() {
            self.blockio_class =
                (!blockio_class.value.trim().is_empty()).then(|| blockio_class.value.clone());
        }
        if let Some(rdt_class) = resources.rdt_class.as_ref() {
            self.rdt_class = resolve_rdt_class(&rdt_class.value).and_then(|rdt| rdt.clos_id);
        }
        for (key, value) in &resources.unified {
            self.unified.insert(key.clone(), value.clone());
        }
    }

    fn to_nri(&self) -> crate::nri_proto::api::LinuxResources {
        let mut result = linux_resources_from_cri(&self.to_proto());

        if self.memory_reservation_in_bytes.is_some()
            || self.memory_kernel_limit_in_bytes.is_some()
            || self.memory_kernel_tcp_limit_in_bytes.is_some()
            || self.memory_swappiness.is_some()
            || self.memory_disable_oom_killer.is_some()
            || self.memory_use_hierarchy.is_some()
        {
            let mut memory = result.memory.take().unwrap_or_default();
            if let Some(reservation) = self.memory_reservation_in_bytes {
                memory.reservation = Self::optional_int64(reservation);
            }
            if let Some(kernel) = self.memory_kernel_limit_in_bytes {
                memory.kernel = Self::optional_int64(kernel);
            }
            if let Some(kernel_tcp) = self.memory_kernel_tcp_limit_in_bytes {
                memory.kernel_tcp = Self::optional_int64(kernel_tcp);
            }
            if let Some(swappiness) = self.memory_swappiness {
                memory.swappiness = Self::optional_uint64(swappiness);
            }
            if let Some(disable_oom_killer) = self.memory_disable_oom_killer {
                memory.disable_oom_killer = Self::optional_bool(disable_oom_killer);
            }
            if let Some(use_hierarchy) = self.memory_use_hierarchy {
                memory.use_hierarchy = Self::optional_bool(use_hierarchy);
            }
            result.memory = protobuf::MessageField::some(memory);
        }

        if self.cpu_realtime_runtime.is_some() || self.cpu_realtime_period.is_some() {
            let mut cpu = result.cpu.take().unwrap_or_default();
            if let Some(runtime) = self.cpu_realtime_runtime {
                cpu.realtime_runtime = Self::optional_int64(runtime);
            }
            if let Some(period) = self.cpu_realtime_period {
                cpu.realtime_period = Self::optional_uint64(period);
            }
            result.cpu = protobuf::MessageField::some(cpu);
        }

        if let Some(limit) = self.pids_limit {
            result.pids = protobuf::MessageField::some(crate::nri_proto::api::LinuxPids {
                limit,
                ..Default::default()
            });
        }
        if !self.devices.is_empty() {
            result.devices = self
                .devices
                .iter()
                .map(|device| crate::nri_proto::api::LinuxDeviceCgroup {
                    allow: device.allow,
                    type_: device.device_type.clone().unwrap_or_default(),
                    major: device
                        .major
                        .map(|value| {
                            let mut value_msg = crate::nri_proto::api::OptionalInt64::new();
                            value_msg.value = value;
                            protobuf::MessageField::some(value_msg)
                        })
                        .unwrap_or_default(),
                    minor: device
                        .minor
                        .map(|value| {
                            let mut value_msg = crate::nri_proto::api::OptionalInt64::new();
                            value_msg.value = value;
                            protobuf::MessageField::some(value_msg)
                        })
                        .unwrap_or_default(),
                    access: device.access.clone().unwrap_or_default(),
                    ..Default::default()
                })
                .collect();
        }
        if let Some(blockio_class) = self.blockio_class.as_ref() {
            result.blockio_class = Self::optional_string(blockio_class.clone());
        }
        if let Some(rdt_class) = self.rdt_class.as_ref() {
            result.rdt_class = Self::optional_string(rdt_class.clone());
        }

        result
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
            memory_reservation_in_bytes: None,
            memory_kernel_limit_in_bytes: None,
            memory_kernel_tcp_limit_in_bytes: None,
            memory_swappiness: None,
            memory_disable_oom_killer: None,
            memory_use_hierarchy: None,
            cpu_realtime_runtime: None,
            cpu_realtime_period: None,
            pids_limit: None,
            devices: Vec::new(),
            blockio_class: None,
            rdt_class: None,
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

    fn to_nri_seccomp(&self) -> Option<crate::nri_proto::api::SecurityProfile> {
        let mut profile = crate::nri_proto::api::SecurityProfile::new();
        match self.profile_type {
            x if x
                == crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault
                    as i32 =>
            {
                profile.profile_type =
                    crate::nri_proto::api::security_profile::ProfileType::RUNTIME_DEFAULT.into();
            }
            x if x
                == crate::proto::runtime::v1::security_profile::ProfileType::Unconfined as i32 =>
            {
                profile.profile_type =
                    crate::nri_proto::api::security_profile::ProfileType::UNCONFINED.into();
            }
            x if x
                == crate::proto::runtime::v1::security_profile::ProfileType::Localhost as i32 =>
            {
                profile.profile_type =
                    crate::nri_proto::api::security_profile::ProfileType::LOCALHOST.into();
                profile.localhost_ref = self.localhost_ref.clone();
            }
            _ => return None,
        }

        Some(profile)
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
    overhead_linux_resources: Option<StoredLinuxResources>,
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
    seccomp_profile: Option<StoredSecurityProfile>,
    metadata_name: Option<String>,
    metadata_attempt: Option<u32>,
    started_at: Option<i64>,
    finished_at: Option<i64>,
    exit_code: Option<i32>,
    nri_stop_notified: bool,
    nri_remove_notified: bool,
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

#[derive(Clone)]
struct NriRuntimeDomain {
    containers: Arc<Mutex<HashMap<String, Container>>>,
    pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    config: RuntimeConfig,
    nri_config: NriConfig,
    runtime: RuncRuntime,
    persistence: Arc<Mutex<PersistenceManager>>,
    events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
}

impl RuntimeServiceImpl {
    fn cgroup_support_flags() -> CgroupResourceSupport {
        Self::cgroup_support_flags_for_root(Path::new("/sys/fs/cgroup"))
    }

    fn cgroup_support_flags_for_root(root: &Path) -> CgroupResourceSupport {
        let is_v2 = root.join("cgroup.controllers").exists();

        let swap = if is_v2 {
            root.join("memory.swap.max").exists()
        } else {
            root.join("memory")
                .join("memory.memsw.limit_in_bytes")
                .exists()
        };

        let hugetlb = if is_v2 {
            std::fs::read_dir(root)
                .ok()
                .into_iter()
                .flat_map(|entries| entries.filter_map(Result::ok))
                .map(|entry| entry.file_name())
                .filter_map(|name| name.into_string().ok())
                .any(|name| name.starts_with("hugetlb.") && name.ends_with(".max"))
        } else {
            root.join("hugetlb").exists()
        };

        let memory_kernel = if is_v2 {
            false
        } else {
            root.join("memory")
                .join("memory.kmem.limit_in_bytes")
                .exists()
        };
        let memory_kernel_tcp = if is_v2 {
            false
        } else {
            root.join("memory")
                .join("memory.kmem.tcp.limit_in_bytes")
                .exists()
        };
        let memory_swappiness = if is_v2 {
            false
        } else {
            root.join("memory").join("memory.swappiness").exists()
        };
        let memory_disable_oom_killer = if is_v2 {
            false
        } else {
            root.join("memory").join("memory.oom_control").exists()
        };
        let memory_use_hierarchy = if is_v2 {
            false
        } else {
            root.join("memory").join("memory.use_hierarchy").exists()
        };
        let cpu_realtime = if is_v2 {
            false
        } else {
            root.join("cpu").join("cpu.rt_runtime_us").exists()
                && root.join("cpu").join("cpu.rt_period_us").exists()
        };

        CgroupResourceSupport {
            swap,
            hugetlb,
            memory_kernel,
            memory_kernel_tcp,
            memory_swappiness,
            memory_disable_oom_killer,
            memory_use_hierarchy,
            cpu_realtime,
            blockio: true,
            rdt: true,
        }
    }

    fn sanitize_spec_runtime_resources(spec: &mut crate::oci::spec::Spec) {
        let support = Self::cgroup_support_flags();
        Self::sanitize_spec_runtime_resources_with_flags(spec, support);
    }

    fn sanitize_spec_runtime_resources_with_flags(
        spec: &mut crate::oci::spec::Spec,
        support: CgroupResourceSupport,
    ) {
        let Some(resources) = spec
            .linux
            .as_mut()
            .and_then(|linux| linux.resources.as_mut())
        else {
            return;
        };

        if let Some(memory) = resources.memory.as_mut() {
            if !support.swap {
                memory.swap = None;
            }
            if !support.memory_kernel {
                memory.kernel = None;
            }
            if !support.memory_kernel_tcp {
                memory.kernel_tcp = None;
            }
            if !support.memory_swappiness {
                memory.swappiness = None;
            }
            if !support.memory_disable_oom_killer {
                memory.disable_oom_killer = None;
            }
            if !support.memory_use_hierarchy {
                memory.use_hierarchy = None;
            }
        }

        if let Some(cpu) = resources.cpu.as_mut() {
            if !support.cpu_realtime {
                cpu.realtime_runtime = None;
                cpu.realtime_period = None;
            }
        }

        if !support.hugetlb {
            resources.hugepage_limits = None;
        }

        if !support.blockio {
            resources.block_io = None;
        }

        if !support.rdt {
            if let Some(linux) = spec.linux.as_mut() {
                linux.intel_rdt = None;
            }
        }
    }

    fn sanitize_stored_runtime_resources(resources: &mut StoredLinuxResources) {
        let support = Self::cgroup_support_flags();
        Self::sanitize_stored_runtime_resources_with_flags(resources, support);
    }

    fn sanitize_stored_runtime_resources_with_flags(
        resources: &mut StoredLinuxResources,
        support: CgroupResourceSupport,
    ) {
        if !support.swap {
            resources.memory_swap_limit_in_bytes = 0;
        }
        if !support.memory_kernel {
            resources.memory_kernel_limit_in_bytes = None;
        }
        if !support.memory_kernel_tcp {
            resources.memory_kernel_tcp_limit_in_bytes = None;
        }
        if !support.memory_swappiness {
            resources.memory_swappiness = None;
        }
        if !support.memory_disable_oom_killer {
            resources.memory_disable_oom_killer = None;
        }
        if !support.memory_use_hierarchy {
            resources.memory_use_hierarchy = None;
        }
        if !support.cpu_realtime {
            resources.cpu_realtime_runtime = None;
            resources.cpu_realtime_period = None;
        }
        if !support.hugetlb {
            resources.hugepage_limits.clear();
        }
        if !support.blockio {
            resources.blockio_class = None;
        }
        if !support.rdt {
            resources.rdt_class = None;
        }
    }

    fn sanitize_nri_linux_resources_with_flags(
        resources: &mut crate::nri_proto::api::LinuxResources,
        support: CgroupResourceSupport,
    ) {
        if let Some(memory) = resources.memory.as_mut() {
            if !support.swap {
                memory.swap = protobuf::MessageField::none();
            }
            if !support.memory_kernel {
                memory.kernel = protobuf::MessageField::none();
            }
            if !support.memory_kernel_tcp {
                memory.kernel_tcp = protobuf::MessageField::none();
            }
            if !support.memory_swappiness {
                memory.swappiness = protobuf::MessageField::none();
            }
            if !support.memory_disable_oom_killer {
                memory.disable_oom_killer = protobuf::MessageField::none();
            }
            if !support.memory_use_hierarchy {
                memory.use_hierarchy = protobuf::MessageField::none();
            }
        }
        if let Some(cpu) = resources.cpu.as_mut() {
            if !support.cpu_realtime {
                cpu.realtime_runtime = protobuf::MessageField::none();
                cpu.realtime_period = protobuf::MessageField::none();
            }
        }
        if !support.hugetlb {
            resources.hugepage_limits.clear();
        }
        if !support.blockio {
            resources.blockio_class = protobuf::MessageField::none();
        }
        if !support.rdt {
            resources.rdt_class = protobuf::MessageField::none();
        }
    }

    fn sanitize_nri_linux_resources(resources: &mut crate::nri_proto::api::LinuxResources) {
        Self::sanitize_nri_linux_resources_with_flags(resources, Self::cgroup_support_flags());
    }

    fn sanitize_nri_linux_resources_for_nri_config(
        resources: &mut crate::nri_proto::api::LinuxResources,
        nri_config: &NriConfig,
    ) {
        Self::sanitize_nri_linux_resources(resources);
        if nri_config.blockio_config_path.trim().is_empty() {
            resources.blockio_class = protobuf::MessageField::none();
        }
        if !Path::new("/sys/fs/resctrl").exists() {
            resources.rdt_class = protobuf::MessageField::none();
        }
    }

    fn sanitize_stored_runtime_resources_for_nri_config(
        resources: &mut StoredLinuxResources,
        nri_config: &NriConfig,
    ) {
        Self::sanitize_stored_runtime_resources(resources);
        if nri_config.blockio_config_path.trim().is_empty() {
            resources.blockio_class = None;
        }
        if !Path::new("/sys/fs/resctrl").exists() {
            resources.rdt_class = None;
        }
    }

    fn sanitize_nri_adjustment_for_nri_config(
        adjustment: &mut crate::nri_proto::api::ContainerAdjustment,
        nri_config: &NriConfig,
    ) {
        if let Some(linux) = adjustment.linux.as_mut() {
            if let Some(resources) = linux.resources.as_mut() {
                Self::sanitize_nri_linux_resources_for_nri_config(resources, nri_config);
            }
            if !Path::new("/sys/fs/resctrl").exists() {
                linux.rdt = protobuf::MessageField::none();
            }
        }
    }

    fn nri_min_memory_limit() -> Option<i64> {
        std::env::var(NRI_MIN_MEMORY_LIMIT_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value > 0)
    }

    fn validate_nri_adjustment_runtime_resources(
        adjustment: &crate::nri_proto::api::ContainerAdjustment,
    ) -> Result<(), Status> {
        if let Some(resources) = adjustment
            .linux
            .as_ref()
            .and_then(|linux| linux.resources.as_ref())
        {
            validate_adjustment_resources_with_min_memory(resources, Self::nri_min_memory_limit())
                .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))?;
        }
        Ok(())
    }

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

    fn now_nanos() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    fn nri_container_state(state: i32) -> crate::nri_proto::api::ContainerState {
        if state == ContainerState::ContainerCreated as i32 {
            crate::nri_proto::api::ContainerState::CONTAINER_CREATED
        } else if state == ContainerState::ContainerRunning as i32 {
            crate::nri_proto::api::ContainerState::CONTAINER_RUNNING
        } else if state == ContainerState::ContainerExited as i32 {
            crate::nri_proto::api::ContainerState::CONTAINER_STOPPED
        } else {
            crate::nri_proto::api::ContainerState::CONTAINER_UNKNOWN
        }
    }

    fn build_nri_pod_from_proto(
        runtime: &RuncRuntime,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> crate::nri_proto::api::PodSandbox {
        let pod_state = Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        );
        let pause_container_id = pod_state
            .as_ref()
            .and_then(|state| state.pause_container_id.clone());
        let pause_spec = pause_container_id
            .as_deref()
            .and_then(|container_id| runtime.load_spec(container_id).ok());

        let mut pod = crate::nri_proto::api::PodSandbox::new();
        pod.id = pod_sandbox.id.clone();
        let pause_spec_annotations = pause_spec
            .as_ref()
            .and_then(|spec| spec.annotations.as_ref());
        if let Some(metadata) = pod_sandbox.metadata.as_ref() {
            pod.name = metadata.name.clone();
            pod.uid = metadata.uid.clone();
            pod.namespace = metadata.namespace.clone();
        } else {
            pod.name = Self::spec_annotation_value(
                pause_spec_annotations,
                &[
                    CRIO_SANDBOX_NAME_ANNOTATION,
                    CRIO_POD_NAME_ANNOTATION,
                    CONTAINERD_SANDBOX_NAME_ANNOTATION,
                ],
            )
            .unwrap_or_default();
            pod.uid = Self::spec_annotation_value(
                pause_spec_annotations,
                &[CONTAINERD_SANDBOX_UID_ANNOTATION],
            )
            .unwrap_or_default();
            pod.namespace = Self::spec_annotation_value(
                pause_spec_annotations,
                &[
                    CRIO_POD_NAMESPACE_ANNOTATION,
                    CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION,
                ],
            )
            .unwrap_or_default();
        }
        pod.labels = Self::merge_labels(&pod_sandbox.labels, pause_spec_annotations);
        pod.annotations =
            Self::merge_external_annotations(&pod_sandbox.annotations, pause_spec_annotations);
        pod.runtime_handler = pod_sandbox.runtime_handler.clone();
        if let Some(state) = pod_state.as_ref() {
            if let Some(ip) = state.ip.as_ref().filter(|ip| !ip.is_empty()) {
                pod.ips.push(ip.clone());
            }
            pod.ips.extend(
                state
                    .additional_ips
                    .iter()
                    .filter(|ip| !ip.is_empty())
                    .cloned(),
            );
        }

        let mut linux = crate::nri_proto::api::LinuxPodSandbox::new();
        linux.cgroup_parent = pod_state
            .as_ref()
            .and_then(|state| state.cgroup_parent.clone())
            .unwrap_or_default();
        let overhead_resources = pod_state
            .as_ref()
            .and_then(|state| state.overhead_linux_resources.as_ref())
            .map(StoredLinuxResources::to_proto);
        let linux_resources = pod_state
            .as_ref()
            .and_then(|state| state.linux_resources.as_ref())
            .map(StoredLinuxResources::to_proto);
        if let Some(resources) = overhead_resources.as_ref() {
            linux.pod_overhead = protobuf::MessageField::some(linux_resources_from_cri(resources));
        }
        if let Some(resources) = linux_resources.as_ref() {
            linux.pod_resources = protobuf::MessageField::some(linux_resources_from_cri(resources));
        }
        if let Some(resources) = linux_resources.as_ref().or(overhead_resources.as_ref()) {
            linux.resources = protobuf::MessageField::some(linux_resources_from_cri(resources));
        }
        if let Some(spec) = pause_spec.as_ref() {
            if let Some(pause_linux) = oci_linux_container(spec) {
                linux.cgroups_path = pause_linux.cgroups_path.clone();
                linux.namespaces = pause_linux.namespaces;
                linux.resources = pause_linux.resources;
            }
        }
        pod.linux = protobuf::MessageField::some(linux);
        if let Some(pause_container_id) = pause_container_id.as_deref() {
            pod.pid = runtime
                .container_pid(pause_container_id)
                .ok()
                .flatten()
                .unwrap_or_default() as u32;
        }
        pod
    }

    fn build_nri_container_from_proto(
        runtime: &RuncRuntime,
        container: &Container,
    ) -> crate::nri_proto::api::Container {
        let stored_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let spec = runtime.load_spec(&container.id).ok();
        let spec_annotations = spec.as_ref().and_then(|loaded| loaded.annotations.as_ref());

        let mut nri_container = crate::nri_proto::api::Container::new();
        nri_container.id = container.id.clone();
        nri_container.pod_sandbox_id = if container.pod_sandbox_id.is_empty() {
            Self::spec_annotation_value(
                spec_annotations,
                &[CRIO_SANDBOX_ID_ANNOTATION, CONTAINERD_SANDBOX_ID_ANNOTATION],
            )
            .unwrap_or_default()
        } else {
            container.pod_sandbox_id.clone()
        };
        nri_container.name = container
            .metadata
            .as_ref()
            .map(|metadata| metadata.name.clone())
            .or_else(|| {
                stored_state
                    .as_ref()
                    .and_then(|state| state.metadata_name.clone())
            })
            .or_else(|| {
                Self::spec_annotation_value(
                    spec_annotations,
                    &[
                        CRIO_CONTAINER_NAME_ANNOTATION,
                        CONTAINERD_CONTAINER_NAME_ANNOTATION,
                        KUBERNETES_CONTAINER_NAME_ANNOTATION,
                    ],
                )
            })
            .unwrap_or_default();
        nri_container.state = if runtime.is_container_paused(&container.id).unwrap_or(false) {
            crate::nri_proto::api::ContainerState::CONTAINER_PAUSED.into()
        } else {
            Self::nri_container_state(container.state).into()
        };
        nri_container.labels = Self::merge_labels(&container.labels, spec_annotations);
        nri_container.annotations =
            Self::merge_external_annotations(&container.annotations, spec_annotations);
        nri_container.created_at = Self::normalize_timestamp_nanos(container.created_at);
        nri_container.started_at = stored_state
            .as_ref()
            .and_then(|state| state.started_at)
            .map(Self::normalize_timestamp_nanos)
            .unwrap_or_default();
        nri_container.finished_at = stored_state
            .as_ref()
            .and_then(|state| state.finished_at)
            .map(Self::normalize_timestamp_nanos)
            .unwrap_or_default();
        nri_container.exit_code = stored_state
            .as_ref()
            .and_then(|state| state.exit_code)
            .unwrap_or_default();
        let (reason, message) =
            Self::container_reason_message(container.state, nri_container.exit_code);
        nri_container.status_reason = reason;
        nri_container.status_message = message;
        nri_container.pid = runtime
            .container_pid(&container.id)
            .ok()
            .flatten()
            .unwrap_or_default() as u32;

        if let Some(spec) = spec.as_ref() {
            nri_container.args = oci_args(spec);
            nri_container.env = oci_env(spec);
            nri_container.mounts = oci_mounts(spec);
            if let Some(hooks) = oci_hooks(spec) {
                nri_container.hooks = protobuf::MessageField::some(hooks);
            }
            if let Some(linux) = oci_linux_container(spec) {
                nri_container.linux = protobuf::MessageField::some(linux);
            }
            nri_container.rlimits = oci_rlimits(spec);
            if let Some(user) = oci_user(spec) {
                nri_container.user = protobuf::MessageField::some(user);
            }
        }

        let stored_linux_resources = stored_state
            .as_ref()
            .and_then(|state| state.linux_resources.as_ref());
        let stored_cgroup_parent = stored_state
            .as_ref()
            .and_then(|state| state.cgroup_parent.clone())
            .unwrap_or_default();
        let stored_seccomp_profile = stored_state
            .as_ref()
            .and_then(|state| state.seccomp_profile.as_ref());

        if nri_container.linux.is_none() {
            nri_container.linux =
                protobuf::MessageField::some(crate::nri_proto::api::LinuxContainer::new());
        }

        if let Some(linux) = nri_container.linux.as_mut() {
            if let Some(resources) = stored_linux_resources {
                linux.resources = protobuf::MessageField::some(resources.to_nri());
                if resources.oom_score_adj != 0 {
                    let mut oom_score_adj = crate::nri_proto::api::OptionalInt::new();
                    oom_score_adj.value = resources.oom_score_adj;
                    linux.oom_score_adj = protobuf::MessageField::some(oom_score_adj);
                }
            }
            if linux.cgroups_path.is_empty() && !stored_cgroup_parent.is_empty() {
                linux.cgroups_path = stored_cgroup_parent;
            }
            if linux.seccomp_profile.is_none() {
                if let Some(seccomp_profile) =
                    stored_seccomp_profile.and_then(StoredSecurityProfile::to_nri_seccomp)
                {
                    linux.seccomp_profile = protobuf::MessageField::some(seccomp_profile);
                }
            }
        }

        nri_container
    }

    fn include_container_in_nri_snapshot(container: &Container) -> bool {
        matches!(
            container.state,
            x if x == ContainerState::ContainerCreated as i32
                || x == ContainerState::ContainerRunning as i32
        )
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

    async fn container_internal_state(&self, container_id: &str) -> Option<StoredContainerState> {
        let containers = self.containers.lock().await;
        containers.get(container_id).and_then(|container| {
            Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            )
        })
    }

    fn linux_resources_to_runtime_update_payload(
        resources: &StoredLinuxResources,
        blockio_config_path: Option<&str>,
    ) -> Result<serde_json::Value, Status> {
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
        if let Some(runtime) = resources.cpu_realtime_runtime {
            cpu.insert("realtimeRuntime".to_string(), json!(runtime));
        }
        if let Some(period) = resources.cpu_realtime_period {
            cpu.insert("realtimePeriod".to_string(), json!(period));
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
        if let Some(reservation) = resources.memory_reservation_in_bytes {
            memory.insert("reservation".to_string(), json!(reservation));
        }
        if resources.memory_swap_limit_in_bytes > 0 {
            memory.insert(
                "swap".to_string(),
                json!(resources.memory_swap_limit_in_bytes),
            );
        }
        if let Some(kernel) = resources.memory_kernel_limit_in_bytes {
            memory.insert("kernel".to_string(), json!(kernel));
        }
        if let Some(kernel_tcp) = resources.memory_kernel_tcp_limit_in_bytes {
            memory.insert("kernelTCP".to_string(), json!(kernel_tcp));
        }
        if let Some(swappiness) = resources.memory_swappiness {
            memory.insert("swappiness".to_string(), json!(swappiness));
        }
        if let Some(disable_oom_killer) = resources.memory_disable_oom_killer {
            memory.insert("disableOOMKiller".to_string(), json!(disable_oom_killer));
        }
        if let Some(use_hierarchy) = resources.memory_use_hierarchy {
            memory.insert("useHierarchy".to_string(), json!(use_hierarchy));
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
        if let Some(limit) = resources.pids_limit {
            payload.insert("pids".to_string(), json!({ "limit": limit }));
        }
        if !resources.devices.is_empty() {
            payload.insert(
                "devices".to_string(),
                serde_json::Value::Array(
                    resources
                        .devices
                        .iter()
                        .map(|device| {
                            let mut entry = serde_json::Map::new();
                            entry.insert("allow".to_string(), json!(device.allow));
                            if let Some(device_type) = device.device_type.as_ref() {
                                entry.insert("type".to_string(), json!(device_type));
                            }
                            if let Some(major) = device.major {
                                entry.insert("major".to_string(), json!(major));
                            }
                            if let Some(minor) = device.minor {
                                entry.insert("minor".to_string(), json!(minor));
                            }
                            if let Some(access) = device.access.as_ref() {
                                entry.insert("access".to_string(), json!(access));
                            }
                            serde_json::Value::Object(entry)
                        })
                        .collect(),
                ),
            );
        }
        if let Some(blockio_class) = resources.blockio_class.as_ref() {
            if let Some(block_io) = resolve_blockio_class(blockio_class, blockio_config_path)
                .map_err(|e| Status::internal(format!("Failed to resolve blockio class: {}", e)))?
            {
                payload.insert(
                    "blockIO".to_string(),
                    serde_json::to_value(block_io).map_err(|e| {
                        Status::internal(format!("Failed to encode OCI block IO resources: {}", e))
                    })?,
                );
            }
        }
        if let Some(rdt_class) = resources.rdt_class.as_ref() {
            if let Some(intel_rdt) = resolve_rdt_class(rdt_class) {
                payload.insert(
                    "intelRdt".to_string(),
                    serde_json::to_value(intel_rdt).map_err(|e| {
                        Status::internal(format!("Failed to encode OCI intel RDT resources: {}", e))
                    })?,
                );
            }
        }

        Ok(serde_json::Value::Object(payload))
    }

    async fn runtime_update_container_resources(
        &self,
        container_id: &str,
        resources: &StoredLinuxResources,
    ) -> Result<(), Status> {
        let mut resource_file = NamedTempFile::new_in(&self.config.root_dir)
            .or_else(|_| NamedTempFile::new())
            .map_err(|e| {
                Status::internal(format!("Failed to create temporary resource file: {}", e))
            })?;
        let payload = Self::linux_resources_to_runtime_update_payload(
            resources,
            Some(&self.nri_config.blockio_config_path),
        )?;
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

    fn map_runtime_container_state(status: crate::runtime::ContainerStatus) -> i32 {
        match status {
            ContainerStatus::Created => ContainerState::ContainerCreated as i32,
            ContainerStatus::Running => ContainerState::ContainerRunning as i32,
            ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
            ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
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

    async fn process_nri_stop_side_effects(
        &self,
        result: &NriStopContainerResult,
    ) -> Result<(), Status> {
        self.process_nri_update_side_effects(&result.updates, &[], "stop")
            .await
    }
}

impl NriRuntimeDomain {
    async fn resolve_container_id(&self, requested_id: &str) -> crate::nri::Result<Option<String>> {
        let containers = self.containers.lock().await;
        if containers.contains_key(requested_id) {
            return Ok(Some(requested_id.to_string()));
        }

        let mut matches = containers
            .keys()
            .filter(|id| id.starts_with(requested_id))
            .cloned()
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            _ => Err(crate::nri::NriError::InvalidInput(format!(
                "ambiguous container id prefix: {}",
                requested_id
            ))),
        }
    }

    async fn get_container(&self, container_id: &str) -> Option<Container> {
        let containers = self.containers.lock().await;
        containers.get(container_id).cloned()
    }

    async fn get_pod(&self, pod_id: &str) -> Option<crate::proto::runtime::v1::PodSandbox> {
        let pods = self.pod_sandboxes.lock().await;
        pods.get(pod_id).cloned()
    }

    async fn runtime_update_container_resources(
        &self,
        container_id: &str,
        resources: &StoredLinuxResources,
    ) -> crate::nri::Result<()> {
        let mut resource_file = NamedTempFile::new_in(&self.config.root_dir)
            .or_else(|_| NamedTempFile::new())
            .map_err(|e| {
                crate::nri::NriError::Plugin(format!(
                    "failed to create temporary resource file: {}",
                    e
                ))
            })?;
        let payload = RuntimeServiceImpl::linux_resources_to_runtime_update_payload(
            resources,
            Some(&self.nri_config.blockio_config_path),
        )
        .map_err(|e| crate::nri::NriError::Plugin(e.message().to_string()))?;
        serde_json::to_writer(resource_file.as_file_mut(), &payload).map_err(|e| {
            crate::nri::NriError::Plugin(format!("failed to encode OCI resources: {}", e))
        })?;
        resource_file.as_file_mut().flush().map_err(|e| {
            crate::nri::NriError::Plugin(format!("failed to flush OCI resources: {}", e))
        })?;

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
        .map_err(|e| crate::nri::NriError::Plugin(format!("failed to spawn update task: {}", e)))?
        .map_err(|e| {
            crate::nri::NriError::Plugin(format!("failed to execute runtime update: {}", e))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let message = if stderr.is_empty() {
                format!("runtime update exited with status {}", output.status)
            } else {
                stderr
            };
            return Err(crate::nri::NriError::Plugin(format!(
                "failed to update runtime resources for {}: {}",
                error_container_id, message
            )));
        }

        Ok(())
    }

    async fn persist_container_annotations(
        &self,
        container_id: &str,
        annotations: &HashMap<String, String>,
    ) -> crate::nri::Result<()> {
        let encoded_annotations = serde_json::to_string(annotations).map_err(|e| {
            crate::nri::NriError::Plugin(format!(
                "failed to encode annotations for {}: {}",
                container_id, e
            ))
        })?;

        let mut persistence = self.persistence.lock().await;
        let Some(mut record) = persistence
            .storage()
            .get_container(container_id)
            .map_err(|e| {
                crate::nri::NriError::Plugin(format!(
                    "failed to load container {} from persistence: {}",
                    container_id, e
                ))
            })?
        else {
            return Ok(());
        };
        record.annotations = encoded_annotations;
        persistence
            .storage_mut()
            .save_container(&record)
            .map_err(|e| {
                crate::nri::NriError::Plugin(format!(
                    "failed to persist container {} annotations: {}",
                    container_id, e
                ))
            })?;
        Ok(())
    }

    async fn apply_single_update(
        &self,
        update: &crate::nri_proto::api::ContainerUpdate,
    ) -> crate::nri::Result<()> {
        validate_container_update(update)?;

        let Some(container_id) = self.resolve_container_id(&update.container_id).await? else {
            return Ok(());
        };

        let runtime = self.runtime.clone();
        let container_id_for_status = container_id.clone();
        let runtime_status =
            tokio::task::spawn_blocking(move || runtime.container_status(&container_id_for_status))
                .await
                .map_err(|e| {
                    crate::nri::NriError::Plugin(format!("failed to spawn status task: {}", e))
                })?
                .map_err(|e| {
                    crate::nri::NriError::Plugin(format!(
                        "failed to inspect container state: {}",
                        e
                    ))
                })?;

        if !matches!(
            runtime_status,
            ContainerStatus::Running | ContainerStatus::Created
        ) {
            return Ok(());
        }

        let linux_update = update.linux.as_ref().ok_or_else(|| {
            crate::nri::NriError::InvalidInput(format!(
                "container {} update is missing linux payload",
                update.container_id
            ))
        })?;
        let resources = linux_update.resources.as_ref().ok_or_else(|| {
            crate::nri::NriError::InvalidInput(format!(
                "container {} update is missing linux resources",
                container_id
            ))
        })?;
        let mut resources = resources.clone();
        RuntimeServiceImpl::sanitize_nri_linux_resources(&mut resources);
        validate_adjustment_resources_with_min_memory(
            &resources,
            RuntimeServiceImpl::nri_min_memory_limit(),
        )
        .map_err(|e| crate::nri::NriError::Plugin(format!("{}", e)))?;
        let mut stored_resources = self
            .get_container(&container_id)
            .await
            .and_then(|container| {
                RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
                    &container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                )
            })
            .and_then(|state| state.linux_resources)
            .unwrap_or_default();
        stored_resources.apply_nri(&resources);
        RuntimeServiceImpl::sanitize_stored_runtime_resources_for_nri_config(
            &mut stored_resources,
            &self.nri_config,
        );

        self.runtime_update_container_resources(&container_id, &stored_resources)
            .await?;

        let mut updated_container = None;
        {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(&container_id) {
                container.state =
                    RuntimeServiceImpl::map_runtime_container_state(runtime_status.clone());
                let mut state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
                    &container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                )
                .unwrap_or_default();
                state.linux_resources = Some(stored_resources.clone());
                RuntimeServiceImpl::insert_internal_state(
                    &mut container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                )
                .map_err(|e| {
                    crate::nri::NriError::Plugin(format!(
                        "failed to update container {} internal state: {}",
                        container_id, e
                    ))
                })?;
                updated_container = Some(container.clone());
            }
        }

        let updated_container = updated_container.ok_or_else(|| {
            crate::nri::NriError::InvalidInput(format!(
                "container {} disappeared during update",
                container_id
            ))
        })?;
        self.persist_container_annotations(&container_id, &updated_container.annotations)
            .await?;

        let mut persistence = self.persistence.lock().await;
        persistence
            .update_container_state(
                &container_id,
                match runtime_status {
                    ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
                    ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
                    ContainerStatus::Stopped(code) => {
                        crate::runtime::ContainerStatus::Stopped(code)
                    }
                    ContainerStatus::Unknown => crate::runtime::ContainerStatus::Unknown,
                },
            )
            .map_err(|e| {
                crate::nri::NriError::Plugin(format!(
                    "failed to persist container {} state: {}",
                    container_id, e
                ))
            })?;
        drop(persistence);

        Ok(())
    }

    async fn evict_container(&self, container_id: &str, _reason: &str) -> crate::nri::Result<()> {
        let Some(container_id) = self.resolve_container_id(container_id).await? else {
            return Ok(());
        };
        let Some(mut container) = self.get_container(&container_id).await else {
            return Ok(());
        };

        let runtime = self.runtime.clone();
        let container_id_owned = container_id.clone();
        tokio::task::spawn_blocking(move || runtime.stop_container(&container_id_owned, Some(30)))
            .await
            .map_err(|e| crate::nri::NriError::Plugin(format!("failed to spawn stop task: {}", e)))?
            .map_err(|e| {
                crate::nri::NriError::Plugin(format!("failed to evict container: {}", e))
            })?;

        let runtime = self.runtime.clone();
        let container_id_owned = container_id.clone();
        let final_runtime_status =
            tokio::task::spawn_blocking(move || runtime.container_status(&container_id_owned))
                .await
                .map_err(|e| {
                    crate::nri::NriError::Plugin(format!("failed to spawn status task: {}", e))
                })?
                .map_err(|e| {
                    crate::nri::NriError::Plugin(format!(
                        "failed to inspect evicted container state: {}",
                        e
                    ))
                })?;

        let exit_code = match final_runtime_status {
            ContainerStatus::Stopped(code) => Some(code),
            _ => None,
        };

        {
            let mut containers = self.containers.lock().await;
            if let Some(entry) = containers.get_mut(&container_id) {
                entry.state = match final_runtime_status {
                    ContainerStatus::Created => ContainerState::ContainerCreated as i32,
                    ContainerStatus::Running => ContainerState::ContainerRunning as i32,
                    ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
                    ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
                };
                let mut state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
                    &entry.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                )
                .unwrap_or_default();
                state.finished_at = Some(RuntimeServiceImpl::now_nanos());
                state.nri_stop_notified = true;
                if let Some(code) = exit_code {
                    state.exit_code = Some(code);
                }
                RuntimeServiceImpl::insert_internal_state(
                    &mut entry.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                )
                .map_err(|e| {
                    crate::nri::NriError::Plugin(format!(
                        "failed to update evicted container {} internal state: {}",
                        container_id, e
                    ))
                })?;
                container = entry.clone();
            }
        }
        self.persist_container_annotations(&container_id, &container.annotations)
            .await?;

        let mut persistence = self.persistence.lock().await;
        persistence
            .update_container_state(
                &container_id,
                match exit_code {
                    Some(code) => crate::runtime::ContainerStatus::Stopped(code),
                    None => crate::runtime::ContainerStatus::Unknown,
                },
            )
            .map_err(|e| {
                crate::nri::NriError::Plugin(format!(
                    "failed to persist evicted container {} state: {}",
                    container_id, e
                ))
            })?;
        drop(persistence);

        let pod_status = self.get_pod(&container.pod_sandbox_id).await.map(|pod| {
            RuntimeServiceImpl::build_pod_sandbox_status_snapshot_with_config(&self.config, &pod)
        });
        let snapshot =
            RuntimeServiceImpl::build_container_status_snapshot(&container, container.state);
        RuntimeServiceImpl::publish_event_via_sender(
            &self.events,
            ContainerEventResponse {
                container_id: container.id.clone(),
                container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                created_at: RuntimeServiceImpl::now_nanos(),
                pod_sandbox_status: pod_status,
                containers_statuses: vec![snapshot],
            },
        );

        Ok(())
    }
}

#[async_trait]
impl NriDomain for NriRuntimeDomain {
    async fn snapshot(&self) -> crate::nri::Result<RuntimeSnapshot> {
        let pods: Vec<crate::proto::runtime::v1::PodSandbox> = {
            let pods = self.pod_sandboxes.lock().await;
            pods.values().cloned().collect()
        };
        let containers: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers.values().cloned().collect()
        };
        let mut snapshot_containers = Vec::new();
        for container in containers {
            if RuntimeServiceImpl::include_container_in_nri_snapshot(&container)
                || self
                    .runtime
                    .is_container_paused(&container.id)
                    .unwrap_or(false)
            {
                snapshot_containers.push(container);
            }
        }

        Ok(RuntimeSnapshot {
            pods: pods
                .iter()
                .map(|pod| RuntimeServiceImpl::build_nri_pod_from_proto(&self.runtime, pod))
                .collect(),
            containers: snapshot_containers
                .iter()
                .map(|container| {
                    RuntimeServiceImpl::build_nri_container_from_proto(&self.runtime, container)
                })
                .collect(),
        })
    }

    async fn apply_updates(
        &self,
        updates: &[crate::nri_proto::api::ContainerUpdate],
    ) -> crate::nri::Result<Vec<crate::nri_proto::api::ContainerUpdate>> {
        let mut failed = Vec::new();
        for update in updates {
            if let Err(err) = self.apply_single_update(update).await {
                if update.ignore_failure {
                    log::warn!(
                        "Ignoring failed NRI container update for {}: {}",
                        update.container_id,
                        err
                    );
                    continue;
                }
                log::warn!(
                    "NRI container update for {} failed: {}",
                    update.container_id,
                    err
                );
                failed.push(update.clone());
            }
        }
        Ok(failed)
    }

    async fn evict(&self, container_id: &str, reason: &str) -> crate::nri::Result<()> {
        self.evict_container(container_id, reason).await
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

    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        RuntimeServiceImpl::container_status(self, request).await
    }

    async fn list_containers(
        &self,
        request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        RuntimeServiceImpl::list_containers(self, request).await
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        RuntimeServiceImpl::status(self, request).await
    }

    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        RuntimeServiceImpl::pod_sandbox_status(self, request).await
    }

    async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        RuntimeServiceImpl::list_pod_sandbox(self, request).await
    }

    async fn list_metric_descriptors(
        &self,
        request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        RuntimeServiceImpl::list_metric_descriptors(self, request).await
    }

    async fn list_pod_sandbox_metrics(
        &self,
        request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        RuntimeServiceImpl::list_pod_sandbox_metrics(self, request).await
    }

    async fn container_stats(
        &self,
        request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        RuntimeServiceImpl::container_stats(self, request).await
    }

    async fn list_container_stats(
        &self,
        request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        RuntimeServiceImpl::list_container_stats(self, request).await
    }

    async fn pod_sandbox_stats(
        &self,
        request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        RuntimeServiceImpl::pod_sandbox_stats(self, request).await
    }

    async fn list_pod_sandbox_stats(
        &self,
        request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        RuntimeServiceImpl::list_pod_sandbox_stats(self, request).await
    }

    async fn exec(&self, request: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        RuntimeServiceImpl::exec(self, request).await
    }

    async fn exec_sync(
        &self,
        request: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        RuntimeServiceImpl::exec_sync(self, request).await
    }

    async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        RuntimeServiceImpl::attach(self, request).await
    }

    async fn port_forward(
        &self,
        request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        RuntimeServiceImpl::port_forward(self, request).await
    }

    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        RuntimeServiceImpl::run_pod_sandbox(self, request).await
    }

    async fn update_pod_sandbox_resources(
        &self,
        request: Request<UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<UpdatePodSandboxResourcesResponse>, Status> {
        RuntimeServiceImpl::update_pod_sandbox_resources(self, request).await
    }

    async fn stop_pod_sandbox(
        &self,
        request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        RuntimeServiceImpl::stop_pod_sandbox(self, request).await
    }

    async fn remove_pod_sandbox(
        &self,
        request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        RuntimeServiceImpl::remove_pod_sandbox(self, request).await
    }

    async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        RuntimeServiceImpl::stop_container(self, request).await
    }

    async fn remove_container(
        &self,
        request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        RuntimeServiceImpl::remove_container(self, request).await
    }

    async fn checkpoint_container(
        &self,
        request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        RuntimeServiceImpl::checkpoint_container(self, request).await
    }

    async fn update_container_resources(
        &self,
        request: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        RuntimeServiceImpl::update_container_resources(self, request).await
    }

    #[allow(unreachable_code)]
    // 创建容器
    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        RuntimeServiceImpl::create_container_impl(self, request).await
    }

    #[allow(unreachable_code)]
    // 启动容器
    async fn start_container(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        RuntimeServiceImpl::start_container_impl(self, request).await
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

    //
    async fn runtime_config(
        &self,
        _request: Request<RuntimeConfigRequest>,
    ) -> Result<Response<RuntimeConfigResponse>, Status> {
        let config = RuntimeConfigResponse {
            linux: Some(crate::proto::runtime::v1::LinuxRuntimeConfiguration {
                cgroup_driver: self.cgroup_driver() as i32,
            }),
        };

        Ok(Response::new(config))
    }
}

#[cfg(test)]
mod tests;
