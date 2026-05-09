use anyhow::{Context, Result};
use log::{debug, error, info};
use nix::libc;
use nix::sys::stat::{major, makedev, minor, mknod, stat, Mode, SFlag};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use thiserror::Error;

use crate::cgroups::{to_oci_resources, CgroupManager, CpuLimit, MemoryLimit, ResourceLimits};
use crate::config::CgroupDriverConfig;
use crate::oci::spec::{
    Device as OciDevice, Linux, LinuxCapabilities, LinuxDeviceCgroup, LinuxPids, LinuxResources,
    Mount, Namespace as OciNamespace, Process, Rlimit, Root, Spec, User,
};
use crate::proto::runtime::v1::{
    Capability, LinuxContainerResources, NamespaceMode, NamespaceOption,
};

pub mod shim_manager;
pub use shim_manager::{default_shim_work_dir, ShimConfig, ShimManager, ShimProcess};

const INTERNAL_CHECKPOINT_RESTORE_KEY: &str = "io.crius.internal/checkpoint-restore";
const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";
const INTERNAL_UID_MAPPINGS_MOUNT_OPTION_PREFIX: &str = "__crius_uidmappings=";
const INTERNAL_GID_MAPPINGS_MOUNT_OPTION_PREFIX: &str = "__crius_gidmappings=";
const DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS: u32 = 240;
const STOP_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);
const STOP_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
const STOP_KILL_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const SHIM_EXIT_CODE_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum ImageAvailabilityError {
    #[error("image {image_ref} is not available locally; pull it before creating the container")]
    NotPresentLocally { image_ref: String },
    #[error("image {image_ref} has no unpackable layers in {image_dir}")]
    NoLayers {
        image_ref: String,
        image_dir: PathBuf,
    },
}

#[derive(Debug, Error)]
pub enum LogReopenError {
    #[error("container {container_id} reopen log control socket {socket_path} is missing")]
    MissingSocket {
        container_id: String,
        socket_path: PathBuf,
    },
}

/// 容器运行时接口
pub trait ContainerRuntime {
    /// 创建容器
    fn create_container(&self, container_id: &str, config: &ContainerConfig) -> Result<String>;

    /// 启动容器
    fn start_container(&self, container_id: &str) -> Result<()>;

    /// 停止容器
    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> Result<()>;

    /// 删除容器
    fn remove_container(&self, container_id: &str) -> Result<()>;

    /// 获取容器状态
    fn container_status(&self, container_id: &str) -> Result<ContainerStatus>;

    /// 重新打开容器日志
    fn reopen_container_log(&self, container_id: &str) -> Result<()>;

    /// 在容器中执行命令
    fn exec_in_container(&self, container_id: &str, command: &[String], tty: bool) -> Result<i32>;

    /// 更新容器资源限制
    fn update_container_resources(
        &self,
        container_id: &str,
        resources: &LinuxContainerResources,
    ) -> Result<()>;
}

/// 容器配置
#[derive(Debug, Clone)]
pub struct ContainerConfig {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<PathBuf>,
    pub mounts: Vec<MountConfig>,
    pub labels: Vec<(String, String)>,
    pub annotations: Vec<(String, String)>,
    pub privileged: bool,
    pub user: Option<String>,
    pub run_as_group: Option<u32>,
    pub supplemental_groups: Vec<u32>,
    pub hostname: Option<String>,
    pub tty: bool,
    pub stdin: bool,
    pub stdin_once: bool,
    pub log_path: Option<PathBuf>,
    pub readonly_rootfs: bool,
    pub seccomp_notifier: Option<SeccompNotifierConfig>,
    pub pids_limit: Option<i64>,
    pub no_new_privileges: Option<bool>,
    pub apparmor_profile: Option<String>,
    pub selinux_label: Option<String>,
    pub seccomp_profile: Option<SeccompProfile>,
    pub capabilities: Option<Capability>,
    pub cgroup_parent: Option<String>,
    pub sysctls: HashMap<String, String>,
    pub namespace_options: Option<NamespaceOption>,
    pub namespace_paths: NamespacePaths,
    pub linux_resources: Option<LinuxContainerResources>,
    pub devices: Vec<DeviceMapping>,
    pub masked_paths: Vec<String>,
    pub readonly_paths: Vec<String>,
    pub rootfs: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeccompNotifierMode {
    Log,
    Stop,
}

#[derive(Debug, Clone)]
pub struct SeccompNotifierConfig {
    pub listener_path: PathBuf,
    pub listener_metadata: String,
    pub mode: SeccompNotifierMode,
}

#[derive(Debug, Default)]
struct ImageRuntimeDefaults {
    env: Vec<(String, String)>,
    entrypoint: Vec<String>,
    cmd: Vec<String>,
    working_dir: Option<PathBuf>,
}

/// Seccomp 配置来源
#[derive(Debug, Clone)]
pub enum SeccompProfile {
    RuntimeDefault,
    Unconfined,
    Localhost(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageVolumesMode {
    Mkdir,
    Bind,
    Ignore,
}

impl ImageVolumesMode {
    pub(crate) fn from_config(value: &str) -> Self {
        match value.trim() {
            "bind" => Self::Bind,
            "ignore" => Self::Ignore,
            _ => Self::Mkdir,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootfsSnapshotter {
    InternalOverlayUntar,
    InternalCachedRootfs,
}

impl RootfsSnapshotter {
    pub fn from_config(value: &str) -> Self {
        match value.trim() {
            "internal-cached-rootfs" => Self::InternalCachedRootfs,
            _ => Self::InternalOverlayUntar,
        }
    }
}

/// 命名空间路径覆盖
#[derive(Debug, Clone, Default)]
pub struct NamespacePaths {
    pub network: Option<PathBuf>,
    pub pid: Option<PathBuf>,
    pub ipc: Option<PathBuf>,
    pub uts: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingMountSourcePolicy {
    Ignore,
    Reject,
    CreateDirectory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountPropagationMode {
    Private,
    HostToContainer,
    Bidirectional,
}

impl MountPropagationMode {
    fn option(self) -> &'static str {
        match self {
            Self::Private => "rprivate",
            Self::HostToContainer => "rslave",
            Self::Bidirectional => "rshared",
        }
    }

    fn required_rootfs_propagation(self) -> Option<&'static str> {
        match self {
            Self::Private => None,
            Self::HostToContainer => Some("rslave"),
            Self::Bidirectional => Some("rshared"),
        }
    }
}

type ProcPaths = (Option<Vec<String>>, Option<Vec<String>>);
type UserNamespaceMappings = (
    Option<Vec<crate::oci::spec::IdMapping>>,
    Option<Vec<crate::oci::spec::IdMapping>>,
);

/// 挂载点配置
#[derive(Debug, Clone)]
pub struct MountConfig {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub read_only: bool,
    pub missing_source_policy: MissingMountSourcePolicy,
    pub selinux_relabel: bool,
    pub propagation: MountPropagationMode,
    pub recursive_read_only: bool,
    pub uid_mappings: Vec<crate::oci::spec::IdMapping>,
    pub gid_mappings: Vec<crate::oci::spec::IdMapping>,
    pub requested_image: Option<String>,
    pub image_sub_path: Option<String>,
}

#[derive(Debug, Error)]
pub enum MountSemanticsError {
    #[error("mount {destination} source {source_path} does not exist")]
    MissingSource {
        source_path: PathBuf,
        destination: PathBuf,
    },
    #[error(
        "mount {destination} requests SELinux relabel but the container has no SELinux mount label"
    )]
    SelinuxRelabelRequiresMountLabel { destination: PathBuf },
    #[error(
        "mount {destination} requests recursive read-only but runtime does not advertise rro mount option support"
    )]
    RecursiveReadOnlyUnsupported { destination: PathBuf },
    #[error(
        "mount {destination} requests idmapped mount but runtime does not advertise idmap mount support"
    )]
    IdmapMountUnsupported { destination: PathBuf },
    #[error(
        "mount {destination} requests recursive read-only for non-directory source {source_path}"
    )]
    RecursiveReadOnlyRequiresDirectory {
        source_path: PathBuf,
        destination: PathBuf,
    },
    #[error(
        "mount {destination} requests bidirectional propagation but source {source_path} is not a shared mount"
    )]
    BidirectionalPropagationRequiresShared {
        source_path: PathBuf,
        destination: PathBuf,
    },
    #[error(
        "mount {destination} requests host-to-container propagation but source {source_path} is neither a shared nor slave mount"
    )]
    HostToContainerPropagationRequiresSharedOrSlave {
        source_path: PathBuf,
        destination: PathBuf,
    },
    #[error("failed to inspect mount propagation for {source_path}: {message}")]
    MountPropagationInspectionFailed {
        source_path: PathBuf,
        message: String,
    },
}

impl MountSemanticsError {
    pub(crate) fn to_status(&self) -> tonic::Status {
        match self {
            Self::MissingSource {
                source_path,
                destination,
            } => tonic::Status::failed_precondition(
                format!(
                    "mount {} source {} does not exist",
                    destination.display(),
                    source_path.display()
                ),
            ),
            Self::SelinuxRelabelRequiresMountLabel { destination } => {
                tonic::Status::failed_precondition(format!(
                    "mount {} requests SELinux relabel but SELinux mount labeling is unavailable",
                    destination.display()
                ))
            }
            Self::RecursiveReadOnlyUnsupported { destination } => {
                tonic::Status::failed_precondition(format!(
                    "mount {} requests recursive_read_only but the selected runtime does not support it",
                    destination.display()
                ))
            }
            Self::IdmapMountUnsupported { destination } => tonic::Status::failed_precondition(
                format!(
                    "mount {} requests uidMappings/gidMappings but the selected runtime does not support idmapped mounts",
                    destination.display()
                ),
            ),
            Self::RecursiveReadOnlyRequiresDirectory {
                source_path,
                destination,
            } => {
                tonic::Status::invalid_argument(format!(
                    "mount {} source {} must be a directory when recursive_read_only=true",
                    destination.display(),
                    source_path.display()
                ))
            }
            Self::BidirectionalPropagationRequiresShared {
                source_path,
                destination,
            } => {
                tonic::Status::failed_precondition(format!(
                    "mount {} source {} must be a shared mount for bidirectional propagation",
                    destination.display(),
                    source_path.display()
                ))
            }
            Self::HostToContainerPropagationRequiresSharedOrSlave {
                source_path,
                destination,
            } => {
                tonic::Status::failed_precondition(format!(
                    "mount {} source {} must be a shared or slave mount for host-to-container propagation",
                    destination.display(),
                    source_path.display()
                ))
            }
            Self::MountPropagationInspectionFailed {
                source_path,
                message,
            } => {
                tonic::Status::failed_precondition(format!(
                    "failed to inspect mount propagation for {}: {}",
                    source_path.display(),
                    message
                ))
            }
        }
    }
}

/// 设备映射配置
#[derive(Debug, Clone)]
pub struct DeviceMapping {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub permissions: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreCheckpointMetadata {
    checkpoint_location: String,
    checkpoint_image_path: String,
    oci_config: serde_json::Value,
    image_ref: String,
}

#[derive(Debug, Deserialize, Default)]
struct RuntimeStoredContainerState {
    tty: bool,
}

/// 容器状态
#[derive(Debug, Clone, PartialEq)]
pub enum ContainerStatus {
    Created,
    Running,
    Stopped(i32), // 退出码
    Unknown,
}

const CRIO_LABELS_ANNOTATION: &str = "io.kubernetes.cri-o.Labels";

/// runc容器状态
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuncState {
    #[serde(rename = "ociVersion")]
    oci_version: String,
    id: String,
    status: String,
    pid: i32,
    bundle: String,
    rootfs: String,
    created: String,
    owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeFeatureProbe {
    pub available: bool,
    pub idmap_mounts: bool,
    pub recursive_read_only_mounts: bool,
    pub mount_options: Vec<String>,
    pub oci_version_min: Option<String>,
    pub oci_version_max: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OciRuntimeFeaturesDocument {
    #[serde(rename = "ociVersionMin", default)]
    oci_version_min: String,
    #[serde(rename = "ociVersionMax", default)]
    oci_version_max: String,
    #[serde(rename = "mountOptions", default)]
    mount_options: Vec<String>,
    linux: Option<OciRuntimeFeaturesLinux>,
}

#[derive(Debug, Deserialize)]
struct OciRuntimeFeaturesLinux {
    #[serde(rename = "mountExtensions")]
    mount_extensions: Option<OciRuntimeMountExtensions>,
}

#[derive(Debug, Deserialize)]
struct OciRuntimeMountExtensions {
    idmap: Option<OciRuntimeFeatureToggle>,
}

#[derive(Debug, Deserialize)]
struct OciRuntimeFeatureToggle {
    enabled: Option<bool>,
}

/// 使用 runc 作为容器运行时
#[derive(Debug, Clone)]
pub struct RuncRuntime {
    runtime_path: PathBuf,
    runtime_config_path: PathBuf,
    root: PathBuf,
    image_storage_root: PathBuf,
    shim_manager: Option<Arc<ShimManager>>,
    default_env: Vec<(String, String)>,
    default_capabilities: Vec<String>,
    default_sysctls: HashMap<String, String>,
    default_ulimits: Vec<Rlimit>,
    allowed_devices: HashSet<PathBuf>,
    additional_devices: Vec<DeviceMapping>,
    device_ownership_from_security_context: bool,
    privileged_without_host_devices: bool,
    privileged_without_host_devices_all_devices_allowed: bool,
    add_inheritable_capabilities: bool,
    base_runtime_spec: Option<Spec>,
    default_mounts_file: Option<PathBuf>,
    hooks_dirs: Vec<PathBuf>,
    absent_mount_sources_to_reject: Vec<PathBuf>,
    image_volumes: ImageVolumesMode,
    rootfs_snapshotter: RootfsSnapshotter,
    container_create_timeout_secs: u32,
    container_stop_timeout_secs: u32,
    criu_path: PathBuf,
    restrict_oom_score_adj: bool,
    bind_mount_prefix: PathBuf,
    disable_cgroup: bool,
    default_seccomp_profile_path: Option<PathBuf>,
    exec_cpu_affinity: String,
    no_pivot: bool,
    no_new_keyring: bool,
    disable_proc_mount: bool,
    timezone: String,
    cgroup_driver: CgroupDriverConfig,
}

impl RuncRuntime {
    fn ensure_container_create_not_expired(
        &self,
        deadline: std::time::Instant,
        phase: &str,
    ) -> Result<()> {
        if std::time::Instant::now() > deadline {
            return Err(anyhow::anyhow!(
                "container create phase {} exceeded runtime create timeout of {}s",
                phase,
                self.container_create_timeout_secs
            ));
        }
        Ok(())
    }

    fn wait_for_container_stop_until(
        &self,
        container_id: &str,
        deadline: std::time::Instant,
    ) -> Result<bool> {
        let mut backoff = STOP_INITIAL_BACKOFF;

        loop {
            match self.get_runc_state(container_id)? {
                None => return Ok(true),
                Some(state) if state.status == "stopped" => return Ok(true),
                _ => {}
            }

            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return Ok(false);
            };
            if remaining.is_zero() {
                return Ok(false);
            }

            std::thread::sleep(std::cmp::min(backoff, remaining));
            backoff = std::cmp::min(backoff.saturating_mul(2), STOP_MAX_BACKOFF);
        }
    }

    fn wait_for_shim_exit_code(&self, container_id: &str) {
        if let Some(shim_manager) = &self.shim_manager {
            let deadline = std::time::Instant::now() + SHIM_EXIT_CODE_WAIT_TIMEOUT;
            loop {
                if matches!(shim_manager.get_exit_code(container_id), Ok(Some(_))) {
                    return;
                }
                if !shim_manager.is_shim_running(container_id) {
                    return;
                }
                let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now())
                else {
                    debug!(
                        "Timed out waiting for shim to record exit code for container {}",
                        container_id
                    );
                    return;
                };
                if remaining.is_zero() {
                    debug!(
                        "Timed out waiting for shim to record exit code for container {}",
                        container_id
                    );
                    return;
                }
                std::thread::sleep(std::cmp::min(STOP_INITIAL_BACKOFF, remaining));
            }
        }
    }

    pub(crate) fn daemon_oom_score_adj() -> Result<i64> {
        let raw = std::fs::read_to_string("/proc/self/oom_score_adj")
            .context("could not get the daemon oom_score_adj")?;
        raw.trim()
            .parse::<i64>()
            .context("could not get the daemon oom_score_adj")
    }

    pub(crate) fn restrict_oom_score_adj_floor(preferred: i64) -> Result<i64> {
        let current = Self::daemon_oom_score_adj()?;
        Ok(preferred.max(current))
    }

    fn insert_label_annotation(
        annotations: &mut std::collections::HashMap<String, String>,
        labels: &[(String, String)],
    ) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }
        let encoded = serde_json::to_string(
            &labels
                .iter()
                .cloned()
                .collect::<std::collections::HashMap<_, _>>(),
        )
        .context("Failed to encode container labels as annotation")?;
        annotations.insert(CRIO_LABELS_ANNOTATION.to_string(), encoded);
        Ok(())
    }

    fn restore_rootfs_snapshot(&self, container_id: &str, image_path: &Path) -> Result<()> {
        let snapshot_path = image_path.join("rootfs.tar");
        if !snapshot_path.exists() {
            return Ok(());
        }

        let config = self.load_bundle_config_value(container_id)?;
        let rootfs_path = config
            .get("root")
            .and_then(|root| root.get("path"))
            .and_then(|path| path.as_str())
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "container {} bundle config is missing root.path for restore",
                    container_id
                )
            })?;

        if rootfs_path.exists() {
            std::fs::remove_dir_all(&rootfs_path).with_context(|| {
                format!(
                    "Failed to clear restore rootfs directory {}",
                    rootfs_path.display()
                )
            })?;
        }
        std::fs::create_dir_all(&rootfs_path).with_context(|| {
            format!(
                "Failed to recreate restore rootfs directory {}",
                rootfs_path.display()
            )
        })?;

        let output = Command::new("tar")
            .arg("-xf")
            .arg(&snapshot_path)
            .arg("-C")
            .arg(&rootfs_path)
            .output()
            .with_context(|| {
                format!(
                    "Failed to restore rootfs snapshot from {}",
                    snapshot_path.display()
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
                "Failed to extract rootfs snapshot {}: {}",
                snapshot_path.display(),
                detail
            ));
        }

        Ok(())
    }

    fn load_bundle_config_value(&self, container_id: &str) -> Result<Value> {
        let config_path = self.config_path(container_id);
        let raw = std::fs::read(&config_path)
            .with_context(|| format!("Failed to read OCI config {}", config_path.display()))?;
        serde_json::from_slice(&raw)
            .with_context(|| format!("Failed to parse OCI config {}", config_path.display()))
    }

    fn container_uses_terminal(&self, container_id: &str) -> Result<bool> {
        let config = self.load_bundle_config_value(container_id)?;
        let from_process = config
            .get("process")
            .and_then(|process| process.get("terminal"))
            .and_then(|value| value.as_bool());
        if let Some(tty) = from_process {
            return Ok(tty);
        }

        let from_internal_state = config
            .get("annotations")
            .and_then(|annotations| annotations.get(INTERNAL_CONTAINER_STATE_KEY))
            .and_then(|value| value.as_str())
            .and_then(|raw| serde_json::from_str::<RuntimeStoredContainerState>(raw).ok())
            .map(|state| state.tty)
            .unwrap_or(false);
        Ok(from_internal_state)
    }

    fn checkpoint_restore_from_annotations(
        annotations: &[(String, String)],
    ) -> Option<RestoreCheckpointMetadata> {
        annotations
            .iter()
            .find(|(key, _)| key == INTERNAL_CHECKPOINT_RESTORE_KEY)
            .and_then(|(_, value)| serde_json::from_str::<RestoreCheckpointMetadata>(value).ok())
    }

    fn normalize_capability_name(name: &str) -> String {
        let upper = name.trim().to_ascii_uppercase();
        if upper.starts_with("CAP_") {
            upper
        } else {
            format!("CAP_{}", upper)
        }
    }

    fn default_capabilities() -> Vec<String> {
        vec![
            "CAP_CHOWN".to_string(),
            "CAP_DAC_OVERRIDE".to_string(),
            "CAP_FSETID".to_string(),
            "CAP_FOWNER".to_string(),
            "CAP_MKNOD".to_string(),
            "CAP_NET_RAW".to_string(),
            "CAP_SETGID".to_string(),
            "CAP_SETUID".to_string(),
            "CAP_SETFCAP".to_string(),
            "CAP_SETPCAP".to_string(),
            "CAP_NET_BIND_SERVICE".to_string(),
            "CAP_SYS_CHROOT".to_string(),
            "CAP_KILL".to_string(),
            "CAP_AUDIT_WRITE".to_string(),
        ]
    }

    fn privileged_capabilities() -> Vec<String> {
        vec![
            "CAP_AUDIT_CONTROL".to_string(),
            "CAP_AUDIT_READ".to_string(),
            "CAP_AUDIT_WRITE".to_string(),
            "CAP_BLOCK_SUSPEND".to_string(),
            "CAP_BPF".to_string(),
            "CAP_CHECKPOINT_RESTORE".to_string(),
            "CAP_CHOWN".to_string(),
            "CAP_DAC_OVERRIDE".to_string(),
            "CAP_DAC_READ_SEARCH".to_string(),
            "CAP_FOWNER".to_string(),
            "CAP_FSETID".to_string(),
            "CAP_IPC_LOCK".to_string(),
            "CAP_IPC_OWNER".to_string(),
            "CAP_KILL".to_string(),
            "CAP_LEASE".to_string(),
            "CAP_LINUX_IMMUTABLE".to_string(),
            "CAP_MAC_ADMIN".to_string(),
            "CAP_MAC_OVERRIDE".to_string(),
            "CAP_MKNOD".to_string(),
            "CAP_NET_ADMIN".to_string(),
            "CAP_NET_BIND_SERVICE".to_string(),
            "CAP_NET_BROADCAST".to_string(),
            "CAP_NET_RAW".to_string(),
            "CAP_PERFMON".to_string(),
            "CAP_SETFCAP".to_string(),
            "CAP_SETGID".to_string(),
            "CAP_SETPCAP".to_string(),
            "CAP_SETUID".to_string(),
            "CAP_SYSLOG".to_string(),
            "CAP_SYS_ADMIN".to_string(),
            "CAP_SYS_BOOT".to_string(),
            "CAP_SYS_CHROOT".to_string(),
            "CAP_SYS_MODULE".to_string(),
            "CAP_SYS_NICE".to_string(),
            "CAP_SYS_PACCT".to_string(),
            "CAP_SYS_PTRACE".to_string(),
            "CAP_SYS_RAWIO".to_string(),
            "CAP_SYS_RESOURCE".to_string(),
            "CAP_SYS_TIME".to_string(),
            "CAP_SYS_TTY_CONFIG".to_string(),
            "CAP_WAKE_ALARM".to_string(),
        ]
    }

    fn capability_baseline(&self, privileged: bool) -> Vec<String> {
        if privileged {
            Self::privileged_capabilities()
        } else {
            self.default_capabilities.clone()
        }
    }

    fn apply_capability_overrides(
        default_caps: &[String],
        overrides: Option<&Capability>,
        add_inheritable_capabilities: bool,
    ) -> LinuxCapabilities {
        let mut base = default_caps.to_vec();
        let mut ambient = Vec::new();

        if let Some(capabilities) = overrides {
            let normalized_drops: Vec<String> = capabilities
                .drop_capabilities
                .iter()
                .map(|cap| Self::normalize_capability_name(cap))
                .collect();

            if normalized_drops.iter().any(|cap| cap == "CAP_ALL") {
                base.clear();
            } else {
                base.retain(|cap| !normalized_drops.iter().any(|drop| drop == cap));
            }

            for cap in &capabilities.add_capabilities {
                let normalized = Self::normalize_capability_name(cap);
                if !base.contains(&normalized) {
                    base.push(normalized);
                }
            }

            ambient = capabilities
                .add_ambient_capabilities
                .iter()
                .map(|cap| Self::normalize_capability_name(cap))
                .collect();

            for cap in &ambient {
                if !base.contains(cap) {
                    base.push(cap.clone());
                }
            }
        }

        LinuxCapabilities {
            bounding: Some(base.clone()),
            effective: Some(base.clone()),
            inheritable: Some(if add_inheritable_capabilities {
                base.clone()
            } else {
                Vec::new()
            }),
            permitted: Some(base),
            ambient: Some(ambient),
        }
    }

    fn merge_env_layers(
        &self,
        base: &[(String, String)],
        requested: &[(String, String)],
    ) -> Vec<(String, String)> {
        let mut merged = base.to_vec();
        for (key, value) in &self.default_env {
            if let Some((_, existing_value)) = merged
                .iter_mut()
                .find(|(existing_key, _)| existing_key == key)
            {
                *existing_value = value.clone();
            } else {
                merged.push((key.clone(), value.clone()));
            }
        }
        for (key, value) in requested {
            if let Some((_, existing_value)) = merged
                .iter_mut()
                .find(|(existing_key, _)| existing_key == key)
            {
                *existing_value = value.clone();
            } else {
                merged.push((key.clone(), value.clone()));
            }
        }
        merged
    }

    fn merged_env(&self, requested: &[(String, String)]) -> Vec<(String, String)> {
        self.merge_env_layers(&[], requested)
    }

    fn image_runtime_defaults(&self, image_ref: &str) -> Result<ImageRuntimeDefaults> {
        if image_ref.trim().is_empty() {
            return Ok(ImageRuntimeDefaults::default());
        }

        let metadata = self.image_metadata(image_ref)?;
        let env = metadata
            .config_env
            .iter()
            .filter_map(|entry| {
                let (key, value) = entry.split_once('=').unwrap_or((entry.as_str(), ""));
                let key = key.trim();
                (!key.is_empty()).then(|| (key.to_string(), value.to_string()))
            })
            .collect();

        Ok(ImageRuntimeDefaults {
            env,
            entrypoint: metadata.config_entrypoint,
            cmd: metadata.config_cmd,
            working_dir: metadata.config_working_dir.map(PathBuf::from),
        })
    }

    fn merged_sysctls(&self, requested: &HashMap<String, String>) -> HashMap<String, String> {
        let mut merged = self.default_sysctls.clone();
        for (key, value) in requested {
            merged.insert(key.clone(), value.clone());
        }
        merged
    }

    fn base_spec_template(&self) -> Spec {
        self.base_runtime_spec
            .clone()
            .unwrap_or_else(|| Spec::new("1.0.2"))
    }

    fn effective_proc_paths(
        &self,
        privileged: bool,
        requested_masked_paths: &[String],
        requested_readonly_paths: &[String],
    ) -> Result<ProcPaths> {
        if privileged {
            return Ok((None, None));
        }

        if self.disable_proc_mount {
            if !requested_masked_paths.is_empty() || !requested_readonly_paths.is_empty() {
                return Err(anyhow::anyhow!(
                    "Kubernetes ProcMount support is disabled by runtime.disable_proc_mount"
                ));
            }
            return Ok((
                Some(Spec::default_masked_paths()),
                Some(Spec::default_readonly_paths()),
            ));
        }

        Ok((
            Some(requested_masked_paths.to_vec()),
            Some(requested_readonly_paths.to_vec()),
        ))
    }

    fn apply_timezone_mount(&self, mounts: &mut Vec<Mount>, env: &mut Vec<String>) -> Result<()> {
        let timezone = self.timezone.trim();
        if timezone.is_empty()
            || mounts
                .iter()
                .any(|mount| mount.destination == "/etc/localtime")
        {
            return Ok(());
        }

        let source = if timezone == "Local" {
            PathBuf::from("/etc/localtime")
        } else {
            Path::new("/usr/share/zoneinfo").join(timezone)
        };
        if !source.exists() {
            return Err(anyhow::anyhow!(
                "configured timezone source does not exist: {}",
                source.display()
            ));
        }

        mounts.push(Mount {
            destination: "/etc/localtime".to_string(),
            source: Some(
                self.prefixed_bind_mount_source(&source)
                    .display()
                    .to_string(),
            ),
            mount_type: Some("bind".to_string()),
            options: Some(vec![
                "bind".to_string(),
                "ro".to_string(),
                "nodev".to_string(),
                "nosuid".to_string(),
                "noexec".to_string(),
            ]),
        });

        if timezone != "Local" && !env.iter().any(|entry| entry.starts_with("TZ=")) {
            env.push(format!("TZ={timezone}"));
        }

        Ok(())
    }

    fn apply_default_ulimits(&self, process: &mut Process) {
        if !self.default_ulimits.is_empty() {
            process.rlimits = Some(self.default_ulimits.clone());
        }
    }

    fn build_user(config: &ContainerConfig) -> Option<User> {
        let additional_gids = if config.supplemental_groups.is_empty() {
            None
        } else {
            Some(config.supplemental_groups.clone())
        };

        let Some(user) = config.user.as_ref() else {
            if config.run_as_group.is_none() && additional_gids.is_none() {
                return None;
            }
            return Some(User {
                uid: 0,
                gid: config.run_as_group.unwrap_or(0),
                additional_gids,
                username: None,
            });
        };

        if let Ok(uid) = user.parse::<u32>() {
            Some(User {
                uid,
                gid: config.run_as_group.unwrap_or(uid),
                additional_gids,
                username: None,
            })
        } else {
            Some(User {
                uid: 0,
                gid: config.run_as_group.unwrap_or(0),
                additional_gids,
                username: Some(user.clone()),
            })
        }
    }

    fn host_namespace_path(ns_type: &str) -> Option<String> {
        let path = PathBuf::from(format!("/proc/1/ns/{}", ns_type));
        path.exists().then(|| path.to_string_lossy().to_string())
    }

    fn target_namespace_path(&self, target_container_id: &str, ns_type: &str) -> Option<String> {
        if target_container_id.is_empty() {
            return None;
        }

        let state = self.get_runc_state(target_container_id).ok().flatten()?;
        (state.pid > 0).then(|| format!("/proc/{}/ns/{}", state.pid, ns_type))
    }

    fn build_namespaces(&self, config: &ContainerConfig) -> Vec<OciNamespace> {
        let mut namespaces = Spec::default_namespaces();
        let options = config.namespace_options.as_ref();

        for namespace in &mut namespaces {
            match namespace.ns_type.as_str() {
                "network" => {
                    if let Some(path) = &config.namespace_paths.network {
                        namespace.path = Some(path.to_string_lossy().to_string());
                    } else if matches!(
                        options.map(|o| o.network),
                        Some(mode) if mode == NamespaceMode::Node as i32
                    ) {
                        namespace.path = Self::host_namespace_path("net");
                    }
                }
                "pid" => {
                    if let Some(path) = &config.namespace_paths.pid {
                        namespace.path = Some(path.to_string_lossy().to_string());
                    } else if matches!(
                        options.map(|o| o.pid),
                        Some(mode) if mode == NamespaceMode::Node as i32
                    ) {
                        namespace.path = Self::host_namespace_path("pid");
                    } else if matches!(
                        options.map(|o| o.pid),
                        Some(mode) if mode == NamespaceMode::Target as i32
                    ) {
                        namespace.path =
                            options.and_then(|o| self.target_namespace_path(&o.target_id, "pid"));
                    }
                }
                "ipc" => {
                    if let Some(path) = &config.namespace_paths.ipc {
                        namespace.path = Some(path.to_string_lossy().to_string());
                    } else if matches!(
                        options.map(|o| o.ipc),
                        Some(mode) if mode == NamespaceMode::Node as i32
                    ) {
                        namespace.path = Self::host_namespace_path("ipc");
                    } else if matches!(
                        options.map(|o| o.ipc),
                        Some(mode) if mode == NamespaceMode::Target as i32
                    ) {
                        namespace.path =
                            options.and_then(|o| self.target_namespace_path(&o.target_id, "ipc"));
                    }
                }
                "uts" => {
                    if let Some(path) = &config.namespace_paths.uts {
                        namespace.path = Some(path.to_string_lossy().to_string());
                    }
                }
                _ => {}
            }
        }

        namespaces
    }

    fn build_user_namespace_mappings(config: &ContainerConfig) -> Result<UserNamespaceMappings> {
        let Some(userns) = config
            .namespace_options
            .as_ref()
            .and_then(|options| options.userns_options.as_ref())
        else {
            return Ok((None, None));
        };

        match userns.mode {
            mode if mode == NamespaceMode::Node as i32 => {
                if !userns.uids.is_empty() || !userns.gids.is_empty() {
                    return Err(anyhow::anyhow!(
                        "user namespace mode NODE must not include uid/gid mappings"
                    ));
                }
                Ok((None, None))
            }
            mode if mode == NamespaceMode::Pod as i32 => {
                if userns.uids.is_empty() || userns.gids.is_empty() {
                    return Err(anyhow::anyhow!(
                        "user namespace mode POD requires both uid and gid mappings"
                    ));
                }

                let uid_mappings = userns
                    .uids
                    .iter()
                    .map(|mapping| crate::oci::spec::IdMapping {
                        container_id: mapping.container_id,
                        host_id: mapping.host_id,
                        size: mapping.length,
                    })
                    .collect();
                let gid_mappings = userns
                    .gids
                    .iter()
                    .map(|mapping| crate::oci::spec::IdMapping {
                        container_id: mapping.container_id,
                        host_id: mapping.host_id,
                        size: mapping.length,
                    })
                    .collect();
                Ok((Some(uid_mappings), Some(gid_mappings)))
            }
            other => Err(anyhow::anyhow!("unsupported user namespace mode {}", other)),
        }
    }

    fn host_ipc_enabled(config: &ContainerConfig) -> bool {
        matches!(
            config.namespace_options.as_ref().map(|options| options.ipc),
            Some(mode) if mode == NamespaceMode::Node as i32
        )
    }

    fn requested_oom_score_adj(config: &ContainerConfig) -> Option<i64> {
        config
            .linux_resources
            .as_ref()
            .map(|resources| resources.oom_score_adj)
            .filter(|oom_score_adj| *oom_score_adj != 0)
    }

    fn normalized_oom_score_adj(&self, preferred: i64) -> Result<i32> {
        let adjusted = if self.restrict_oom_score_adj {
            Self::restrict_oom_score_adj_floor(preferred)?
        } else {
            preferred
        };
        i32::try_from(adjusted)
            .with_context(|| format!("oom_score_adj {adjusted} does not fit in i32"))
    }

    fn apply_oom_score_adj_policy(&self, spec: &mut Spec, requested: Option<i64>) -> Result<()> {
        let preferred = requested.or_else(|| {
            spec.process
                .as_ref()
                .and_then(|process| process.oom_score_adj.map(i64::from))
        });
        let Some(preferred) = preferred else {
            return Ok(());
        };
        let process = spec
            .process
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("OCI spec is missing process configuration"))?;
        process.oom_score_adj = Some(self.normalized_oom_score_adj(preferred)?);
        Ok(())
    }

    pub fn enforce_oom_score_adj_policy(&self, spec: &mut Spec) -> Result<()> {
        self.apply_oom_score_adj_policy(spec, None)
    }

    fn prefixed_bind_mount_source(&self, source: &Path) -> PathBuf {
        if self.bind_mount_prefix.as_os_str().is_empty() {
            return source.to_path_buf();
        }

        if source.is_absolute() {
            let suffix = source.strip_prefix(Path::new("/")).unwrap_or(source);
            self.bind_mount_prefix.join(suffix)
        } else {
            self.bind_mount_prefix.join(source)
        }
    }

    fn should_reject_absent_mount_source(&self, source: &Path) -> bool {
        self.absent_mount_sources_to_reject
            .iter()
            .any(|entry| entry == source)
    }

    fn resolve_bind_mount_source(
        &self,
        source: &Path,
        policy: MissingMountSourcePolicy,
    ) -> Result<Option<PathBuf>> {
        let prefixed = self.prefixed_bind_mount_source(source);
        if !prefixed.exists() {
            if self.should_reject_absent_mount_source(&prefixed) {
                return Err(anyhow::anyhow!(
                    "cannot mount {}: path does not exist and is configured to be rejected",
                    prefixed.display()
                ));
            }
            match policy {
                MissingMountSourcePolicy::Reject => {
                    return Err(anyhow::anyhow!(
                        "cannot mount {}: path does not exist",
                        prefixed.display()
                    ));
                }
                MissingMountSourcePolicy::Ignore => return Ok(None),
                MissingMountSourcePolicy::CreateDirectory => {
                    std::fs::create_dir_all(&prefixed).with_context(|| {
                        format!(
                            "failed to create missing bind mount source directory {}",
                            prefixed.display()
                        )
                    })?;
                }
            }
        }

        std::fs::canonicalize(&prefixed)
            .with_context(|| format!("failed to resolve mount source {}", prefixed.display()))
            .map(Some)
    }

    fn bind_mount_options_for_source(
        &self,
        source: &Path,
        mount: &MountConfig,
    ) -> Result<Vec<String>> {
        let recursive = source.is_dir();
        let mut options = if recursive {
            vec!["rbind".to_string()]
        } else {
            vec!["bind".to_string()]
        };
        options.push(mount.propagation.option().to_string());
        options.push(if mount.read_only { "ro" } else { "rw" }.to_string());
        if mount.recursive_read_only {
            options.push("rro".to_string());
        }
        if !mount.uid_mappings.is_empty() {
            options.push(format!(
                "{INTERNAL_UID_MAPPINGS_MOUNT_OPTION_PREFIX}{}",
                serde_json::to_string(&mount.uid_mappings)
                    .context("failed to encode mount uid mappings")?
            ));
        }
        if !mount.gid_mappings.is_empty() {
            options.push(format!(
                "{INTERNAL_GID_MAPPINGS_MOUNT_OPTION_PREFIX}{}",
                serde_json::to_string(&mount.gid_mappings)
                    .context("failed to encode mount gid mappings")?
            ));
        }
        Ok(options)
    }

    fn relabel_mount_source(&self, source: &Path, mount_label: &str) -> Result<()> {
        let mut command = Command::new("chcon");
        if source.is_dir() {
            command.arg("-R");
        }
        let output = command
            .arg(mount_label)
            .arg(source)
            .output()
            .with_context(|| format!("failed to relabel mount source {}", source.display()))?;
        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("status={}", output.status)
        };
        Err(anyhow::anyhow!(
            "failed to relabel mount source {}: {}",
            source.display(),
            detail
        ))
    }

    fn build_bind_mount(
        &self,
        mount: &MountConfig,
        mount_label: Option<&str>,
    ) -> Result<Option<Mount>> {
        let Some(source) =
            self.resolve_bind_mount_source(&mount.source, mount.missing_source_policy)?
        else {
            return Ok(None);
        };

        if mount.selinux_relabel {
            let label = mount_label.ok_or_else(|| {
                anyhow::anyhow!(
                    "mount {} requests SELinux relabel but the container has no SELinux mount label",
                    mount.destination.display()
                )
            })?;
            self.relabel_mount_source(&source, label)?;
        }

        Ok(Some(Mount {
            destination: mount.destination.to_string_lossy().to_string(),
            source: Some(source.to_string_lossy().to_string()),
            mount_type: Some("bind".to_string()),
            options: Some(self.bind_mount_options_for_source(&source, mount)?),
        }))
    }

    fn merge_rootfs_propagation(
        current: Option<&str>,
        requested: Option<&str>,
    ) -> Option<&'static str> {
        fn rank(value: &str) -> u8 {
            match value {
                "rshared" => 2,
                "rslave" => 1,
                _ => 0,
            }
        }

        match (current, requested) {
            (Some(existing), Some(candidate)) => {
                if rank(existing) >= rank(candidate) {
                    match existing {
                        "rshared" => Some("rshared"),
                        "rslave" => Some("rslave"),
                        _ => None,
                    }
                } else {
                    match candidate {
                        "rshared" => Some("rshared"),
                        "rslave" => Some("rslave"),
                        _ => None,
                    }
                }
            }
            (Some(existing), None) => match existing {
                "rshared" => Some("rshared"),
                "rslave" => Some("rslave"),
                _ => None,
            },
            (None, Some(candidate)) => match candidate {
                "rshared" => Some("rshared"),
                "rslave" => Some("rslave"),
                _ => None,
            },
            (None, None) => None,
        }
    }

    fn desired_rootfs_propagation(mounts: &[MountConfig]) -> Option<&'static str> {
        mounts.iter().fold(None, |current, mount| {
            Self::merge_rootfs_propagation(current, mount.propagation.required_rootfs_propagation())
        })
    }

    fn decode_mountinfo_path(raw: &str) -> PathBuf {
        let mut decoded = String::with_capacity(raw.len());
        let mut chars = raw.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                let mut octal = String::new();
                for _ in 0..3 {
                    if let Some(next) = chars.peek().copied() {
                        if ('0'..='7').contains(&next) {
                            octal.push(next);
                            chars.next();
                            continue;
                        }
                    }
                    break;
                }
                if octal.len() == 3 {
                    if let Ok(value) = u8::from_str_radix(&octal, 8) {
                        decoded.push(value as char);
                        continue;
                    }
                }
                decoded.push('\\');
                decoded.push_str(&octal);
                continue;
            }
            decoded.push(ch);
        }
        PathBuf::from(decoded)
    }

    fn is_path_within_mountpoint(path: &Path, mount_point: &Path) -> bool {
        path == mount_point || path.starts_with(mount_point)
    }

    fn mount_source_propagation_for_path_from_mountinfo(
        mountinfo: &str,
        path: &Path,
    ) -> Result<Option<(bool, bool)>> {
        let canonical = std::fs::canonicalize(path)
            .with_context(|| format!("failed to resolve mount source {}", path.display()))?;
        let mut best_match: Option<(usize, bool, bool)> = None;

        for line in mountinfo.lines() {
            let fields: Vec<&str> = line.split(' ').collect();
            let Some(separator_index) = fields.iter().position(|field| *field == "-") else {
                continue;
            };
            if separator_index < 6 || fields.len() <= separator_index + 3 {
                continue;
            }

            let mount_point = Self::decode_mountinfo_path(fields[4]);
            if !Self::is_path_within_mountpoint(&canonical, &mount_point) {
                continue;
            }

            let mut shared = false;
            let mut slave = false;
            for field in &fields[6..separator_index] {
                if field.starts_with("shared:") {
                    shared = true;
                } else if field.starts_with("master:") {
                    slave = true;
                }
            }

            let score = mount_point.components().count();
            if best_match
                .as_ref()
                .map(|(best_score, _, _)| score > *best_score)
                .unwrap_or(true)
            {
                best_match = Some((score, shared, slave));
            }
        }

        Ok(best_match.map(|(_, shared, slave)| (shared, slave)))
    }

    fn validate_mount_propagation_from_mountinfo(
        source: &Path,
        destination: &Path,
        propagation: MountPropagationMode,
        mountinfo: &str,
    ) -> std::result::Result<(), MountSemanticsError> {
        let Some((shared, slave)) =
            Self::mount_source_propagation_for_path_from_mountinfo(mountinfo, source).map_err(
                |err| MountSemanticsError::MountPropagationInspectionFailed {
                    source_path: source.to_path_buf(),
                    message: err.to_string(),
                },
            )?
        else {
            return Err(MountSemanticsError::MountPropagationInspectionFailed {
                source_path: source.to_path_buf(),
                message: "mount point not found in /proc/self/mountinfo".to_string(),
            });
        };

        match propagation {
            MountPropagationMode::Private => Ok(()),
            MountPropagationMode::HostToContainer if shared || slave => Ok(()),
            MountPropagationMode::HostToContainer => Err(
                MountSemanticsError::HostToContainerPropagationRequiresSharedOrSlave {
                    source_path: source.to_path_buf(),
                    destination: destination.to_path_buf(),
                },
            ),
            MountPropagationMode::Bidirectional if shared => Ok(()),
            MountPropagationMode::Bidirectional => Err(
                MountSemanticsError::BidirectionalPropagationRequiresShared {
                    source_path: source.to_path_buf(),
                    destination: destination.to_path_buf(),
                },
            ),
        }
    }

    fn validate_mount_propagation(
        &self,
        source: &Path,
        destination: &Path,
        propagation: MountPropagationMode,
    ) -> std::result::Result<(), MountSemanticsError> {
        if matches!(propagation, MountPropagationMode::Private) {
            return Ok(());
        }

        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(|err| {
            MountSemanticsError::MountPropagationInspectionFailed {
                source_path: source.to_path_buf(),
                message: err.to_string(),
            }
        })?;
        Self::validate_mount_propagation_from_mountinfo(
            source,
            destination,
            propagation,
            &mountinfo,
        )
    }

    fn validate_mount_semantics(
        &self,
        mounts: &[MountConfig],
        mount_label: Option<&str>,
    ) -> std::result::Result<(), MountSemanticsError> {
        let needs_rro_support = mounts.iter().any(|mount| mount.recursive_read_only);
        let needs_idmap_support = mounts
            .iter()
            .any(|mount| !mount.uid_mappings.is_empty() || !mount.gid_mappings.is_empty());
        let features =
            (needs_rro_support || needs_idmap_support).then(|| self.probe_runtime_features());

        for mount in mounts {
            let Some(source) = self
                .resolve_bind_mount_source(&mount.source, mount.missing_source_policy)
                .map_err(|_| MountSemanticsError::MissingSource {
                    source_path: self.prefixed_bind_mount_source(&mount.source),
                    destination: mount.destination.clone(),
                })?
            else {
                continue;
            };

            if mount.selinux_relabel && mount_label.is_none() {
                return Err(MountSemanticsError::SelinuxRelabelRequiresMountLabel {
                    destination: mount.destination.clone(),
                });
            }

            self.validate_mount_propagation(&source, &mount.destination, mount.propagation)?;

            if mount.recursive_read_only {
                if !source.is_dir() {
                    return Err(MountSemanticsError::RecursiveReadOnlyRequiresDirectory {
                        source_path: source,
                        destination: mount.destination.clone(),
                    });
                }
                let supported = features
                    .as_ref()
                    .map(|probe| probe.available && probe.recursive_read_only_mounts)
                    .unwrap_or(false);
                if !supported {
                    return Err(MountSemanticsError::RecursiveReadOnlyUnsupported {
                        destination: mount.destination.clone(),
                    });
                }
            }

            if !mount.uid_mappings.is_empty() || !mount.gid_mappings.is_empty() {
                let supported = features
                    .as_ref()
                    .map(|probe| probe.available && probe.idmap_mounts)
                    .unwrap_or(false);
                if !supported {
                    return Err(MountSemanticsError::IdmapMountUnsupported {
                        destination: mount.destination.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    fn parse_default_mounts_file(&self) -> Result<Vec<(PathBuf, PathBuf)>> {
        let Some(path) = self.default_mounts_file.as_ref() else {
            return Ok(Vec::new());
        };
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read default mounts file {}", path.display()))?;
        let mut mounts = Vec::new();
        for (index, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((src, dst)) = line.split_once(':') else {
                return Err(anyhow::anyhow!(
                    "invalid default mounts entry at {}:{}; expected /SRC:/DST",
                    path.display(),
                    index + 1
                ));
            };
            let src = PathBuf::from(src.trim());
            let dst = PathBuf::from(dst.trim());
            if !src.is_absolute() || !dst.is_absolute() {
                return Err(anyhow::anyhow!(
                    "default mounts entry at {}:{} must use absolute /SRC:/DST paths",
                    path.display(),
                    index + 1
                ));
            }
            mounts.push((src, dst));
        }
        Ok(mounts)
    }

    fn merge_hook_lists(
        base: &mut Option<Vec<crate::oci::spec::Hook>>,
        extra: Option<Vec<crate::oci::spec::Hook>>,
    ) {
        let Some(extra) = extra else {
            return;
        };
        base.get_or_insert_with(Vec::new).extend(extra);
    }

    fn merge_hooks(
        mut base: crate::oci::spec::Hooks,
        extra: crate::oci::spec::Hooks,
    ) -> crate::oci::spec::Hooks {
        Self::merge_hook_lists(&mut base.prestart, extra.prestart);
        Self::merge_hook_lists(&mut base.create_runtime, extra.create_runtime);
        Self::merge_hook_lists(&mut base.create_container, extra.create_container);
        Self::merge_hook_lists(&mut base.start_container, extra.start_container);
        Self::merge_hook_lists(&mut base.poststart, extra.poststart);
        Self::merge_hook_lists(&mut base.poststop, extra.poststop);
        base
    }

    fn parse_hooks_file(path: &Path) -> Result<Option<crate::oci::spec::Hooks>> {
        let raw = std::fs::read(path)
            .with_context(|| format!("Failed to read OCI hooks file {}", path.display()))?;
        if let Ok(spec) = serde_json::from_slice::<crate::oci::spec::Spec>(&raw) {
            if spec.hooks.is_some() {
                return Ok(spec.hooks);
            }
        }
        let hooks = serde_json::from_slice::<crate::oci::spec::Hooks>(&raw)
            .with_context(|| format!("Failed to parse OCI hooks file {}", path.display()))?;
        Ok(Some(hooks))
    }

    fn load_configured_hooks(&self) -> Result<Option<crate::oci::spec::Hooks>> {
        let mut selected = std::collections::BTreeMap::<String, PathBuf>::new();
        for dir in &self.hooks_dirs {
            if !dir.exists() {
                continue;
            }
            if !dir.is_dir() {
                return Err(anyhow::anyhow!(
                    "configured hooks directory is not a directory: {}",
                    dir.display()
                ));
            }
            for entry in std::fs::read_dir(dir)
                .with_context(|| format!("Failed to read hooks directory {}", dir.display()))?
            {
                let entry = entry?;
                let path = entry.path();
                if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json")
                {
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                    selected.insert(name.to_string(), path);
                }
            }
        }

        let mut merged: Option<crate::oci::spec::Hooks> = None;
        for path in selected.into_values() {
            if let Some(hooks) = Self::parse_hooks_file(&path)? {
                merged = Some(match merged.take() {
                    Some(existing) => Self::merge_hooks(existing, hooks),
                    None => hooks,
                });
            }
        }
        Ok(merged)
    }

    fn build_mounts(
        &self,
        container_id: &str,
        config: &ContainerConfig,
        keep_hugepages_mount: bool,
    ) -> Result<Vec<Mount>> {
        self.validate_mount_semantics(&config.mounts, config.selinux_label.as_deref())
            .map_err(anyhow::Error::from)?;
        let mut extra_mounts: Vec<Mount> = self
            .parse_default_mounts_file()?
            .into_iter()
            .filter_map(|(source, destination)| {
                self.build_bind_mount(
                    &MountConfig {
                        source,
                        destination,
                        read_only: false,
                        missing_source_policy: MissingMountSourcePolicy::Ignore,
                        selinux_relabel: false,
                        propagation: MountPropagationMode::Private,
                        recursive_read_only: false,
                        uid_mappings: Vec::new(),
                        gid_mappings: Vec::new(),
                        requested_image: None,
                        image_sub_path: None,
                    },
                    config.selinux_label.as_deref(),
                )
                .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        extra_mounts.extend(self.image_volume_mounts(container_id, config)?);
        for mount in &config.mounts {
            if let Some(custom_mount) =
                self.build_bind_mount(mount, config.selinux_label.as_deref())?
            {
                extra_mounts.retain(|existing| existing.destination != custom_mount.destination);
                extra_mounts.push(custom_mount);
            }
        }

        let mut overridden_destinations: HashSet<String> = extra_mounts
            .iter()
            .map(|mount| mount.destination.clone())
            .collect();
        if Self::host_ipc_enabled(config) {
            overridden_destinations.insert("/dev/shm".to_string());
            overridden_destinations.insert("/dev/mqueue".to_string());
        }

        let mut mounts: Vec<Mount> = Spec::default_mounts()
            .into_iter()
            .filter(|mount| {
                !overridden_destinations.contains(&mount.destination)
                    && mount.destination != "/sys/fs/cgroup"
                    && (keep_hugepages_mount || mount.destination != "/dev/hugepages")
            })
            .collect();

        if Self::host_ipc_enabled(config) {
            if let Some(dev_shm) = self.build_bind_mount(
                &MountConfig {
                    source: PathBuf::from("/dev/shm"),
                    destination: PathBuf::from("/dev/shm"),
                    read_only: false,
                    missing_source_policy: MissingMountSourcePolicy::Ignore,
                    selinux_relabel: false,
                    propagation: MountPropagationMode::Private,
                    recursive_read_only: false,
                    uid_mappings: Vec::new(),
                    gid_mappings: Vec::new(),
                    requested_image: None,
                    image_sub_path: None,
                },
                config.selinux_label.as_deref(),
            )? {
                mounts.push(dev_shm);
            }
            if let Some(dev_mqueue) = self.build_bind_mount(
                &MountConfig {
                    source: PathBuf::from("/dev/mqueue"),
                    destination: PathBuf::from("/dev/mqueue"),
                    read_only: false,
                    missing_source_policy: MissingMountSourcePolicy::Ignore,
                    selinux_relabel: false,
                    propagation: MountPropagationMode::Private,
                    recursive_read_only: false,
                    uid_mappings: Vec::new(),
                    gid_mappings: Vec::new(),
                    requested_image: None,
                    image_sub_path: None,
                },
                config.selinux_label.as_deref(),
            )? {
                mounts.push(dev_mqueue);
            }
        }

        mounts.extend(extra_mounts);
        Ok(mounts)
    }

    pub(crate) fn first_cpu_from_cpuset(cpuset: &str) -> Option<usize> {
        cpuset
            .split(',')
            .map(str::trim)
            .find(|entry| !entry.is_empty())
            .and_then(|entry| {
                entry
                    .split_once('-')
                    .map(|(start, _)| start)
                    .or(Some(entry))
            })
            .and_then(|entry| entry.parse::<usize>().ok())
    }

    pub(crate) fn apply_exec_cpu_affinity_to_std_command(
        command: &mut Command,
        cpu: Option<usize>,
    ) {
        let Some(cpu) = cpu else {
            return;
        };
        unsafe {
            command.pre_exec(move || {
                let mut set = nix::sched::CpuSet::new();
                set.set(cpu)
                    .map_err(|err| std::io::Error::other(err.to_string()))?;
                nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &set)
                    .map_err(|err| std::io::Error::other(err.to_string()))?;
                Ok(())
            });
        }
    }

    pub(crate) fn apply_exec_cpu_affinity_to_tokio_command(
        command: &mut tokio::process::Command,
        cpu: Option<usize>,
    ) {
        let Some(cpu) = cpu else {
            return;
        };
        unsafe {
            command.pre_exec(move || {
                let mut set = nix::sched::CpuSet::new();
                set.set(cpu)
                    .map_err(|err| std::io::Error::other(err.to_string()))?;
                nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &set)
                    .map_err(|err| std::io::Error::other(err.to_string()))?;
                Ok(())
            });
        }
    }

    fn load_seccomp_profile(
        &self,
        profile: Option<&SeccompProfile>,
        notifier: Option<&SeccompNotifierConfig>,
    ) -> Result<Option<crate::oci::spec::Seccomp>> {
        let mut seccomp: Option<crate::oci::spec::Seccomp> = match profile {
            None => None,
            Some(SeccompProfile::RuntimeDefault) => {
                if let Some(path) = self.default_seccomp_profile_path.as_ref() {
                    let content = std::fs::read_to_string(path).with_context(|| {
                        format!("Failed to read default seccomp profile from {:?}", path)
                    })?;
                    let seccomp = serde_json::from_str(&content).with_context(|| {
                        format!(
                            "Failed to parse default seccomp profile JSON from {:?}",
                            path
                        )
                    })?;
                    Some(seccomp)
                } else {
                    Some(Self::builtin_runtime_default_seccomp())
                }
            }
            Some(SeccompProfile::Unconfined) => None,
            Some(SeccompProfile::Localhost(path)) => {
                let content = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read seccomp profile from {:?}", path))?;
                let seccomp = serde_json::from_str(&content).with_context(|| {
                    format!("Failed to parse seccomp profile JSON from {:?}", path)
                })?;
                Some(seccomp)
            }
        };

        if let (Some(seccomp), Some(notifier)) = (seccomp.as_mut(), notifier) {
            if let Some(syscalls) = seccomp.syscalls.as_mut() {
                for syscall in syscalls.iter_mut() {
                    if matches!(
                        syscall.action.as_str(),
                        "SCMP_ACT_ERRNO"
                            | "SCMP_ACT_KILL"
                            | "SCMP_ACT_KILL_PROCESS"
                            | "SCMP_ACT_KILL_THREAD"
                    ) {
                        syscall.action = "SCMP_ACT_NOTIFY".to_string();
                        syscall.errno_ret = None;
                    }
                }
            }
            seccomp.listener_path = Some(notifier.listener_path.display().to_string());
            seccomp.listener_metadata = Some(notifier.listener_metadata.clone());
        }

        Ok(seccomp)
    }

    fn builtin_runtime_default_seccomp() -> crate::oci::spec::Seccomp {
        let architectures = match std::env::consts::ARCH {
            "x86_64" => vec![
                "SCMP_ARCH_X86_64".to_string(),
                "SCMP_ARCH_X86".to_string(),
                "SCMP_ARCH_X32".to_string(),
            ],
            "aarch64" => vec!["SCMP_ARCH_AARCH64".to_string(), "SCMP_ARCH_ARM".to_string()],
            "riscv64" => vec!["SCMP_ARCH_RISCV64".to_string()],
            "s390x" => vec!["SCMP_ARCH_S390X".to_string(), "SCMP_ARCH_S390".to_string()],
            _ => Vec::new(),
        };
        crate::oci::spec::Seccomp {
            default_action: "SCMP_ACT_ALLOW".to_string(),
            default_errno_ret: None,
            architectures: (!architectures.is_empty()).then_some(architectures),
            flags: None,
            listener_path: None,
            listener_metadata: None,
            syscalls: Some(vec![crate::oci::spec::SeccompSyscall {
                action: "SCMP_ACT_ERRNO".to_string(),
                names: vec![
                    "fsconfig".to_string(),
                    "fsmount".to_string(),
                    "fsopen".to_string(),
                    "fspick".to_string(),
                    "mount".to_string(),
                    "mount_setattr".to_string(),
                    "move_mount".to_string(),
                    "open_tree".to_string(),
                    "pivot_root".to_string(),
                    "setns".to_string(),
                    "umount2".to_string(),
                    "unshare".to_string(),
                ],
                args: None,
                errno_ret: Some(libc::EPERM as u32),
            }]),
        }
    }

    fn linux_resources_to_oci(resources: &LinuxContainerResources) -> LinuxResources {
        let limits = ResourceLimits {
            cpu: Some(CpuLimit {
                shares: (resources.cpu_shares > 0).then_some(resources.cpu_shares as u64),
                quota: (resources.cpu_quota > 0).then_some(resources.cpu_quota),
                period: (resources.cpu_period > 0).then_some(resources.cpu_period as u64),
                realtime_runtime: None,
                realtime_period: None,
                cpus: (!resources.cpuset_cpus.is_empty()).then(|| resources.cpuset_cpus.clone()),
                mems: (!resources.cpuset_mems.is_empty()).then(|| resources.cpuset_mems.clone()),
            }),
            memory: Some(MemoryLimit {
                limit: (resources.memory_limit_in_bytes > 0)
                    .then_some(resources.memory_limit_in_bytes),
                reservation: None,
                swap: (resources.memory_swap_limit_in_bytes > 0)
                    .then_some(resources.memory_swap_limit_in_bytes),
                kernel: None,
                kernel_tcp: None,
                swappiness: None,
                disable_oom_killer: None,
                use_hierarchy: None,
            }),
            blkio: None,
            network: None,
            pids: None,
        };

        let mut oci_resources = to_oci_resources(&limits);
        if !resources.unified.is_empty() {
            oci_resources.unified = Some(resources.unified.clone());
        }
        if !resources.hugepage_limits.is_empty() {
            oci_resources.hugepage_limits = Some(
                resources
                    .hugepage_limits
                    .iter()
                    .map(|limit| crate::oci::spec::LinuxHugepageLimit {
                        page_size: limit.page_size.clone(),
                        limit: limit.limit,
                    })
                    .collect(),
            );
        }

        oci_resources
    }

    fn device_mappings_to_oci(
        devices: &[DeviceMapping],
        owner_uid: Option<u32>,
        owner_gid: Option<u32>,
    ) -> Result<(Vec<OciDevice>, Vec<LinuxDeviceCgroup>)> {
        let mut oci_devices = Vec::new();
        let mut cgroup_rules = Vec::new();

        for device in devices {
            let file_stat = stat(&device.source)
                .with_context(|| format!("Failed to stat device path {:?}", device.source))?;
            let file_type = SFlag::from_bits_truncate(file_stat.st_mode);
            let device_type = if file_type.contains(SFlag::S_IFCHR) {
                "c"
            } else if file_type.contains(SFlag::S_IFBLK) {
                "b"
            } else {
                return Err(anyhow::anyhow!(
                    "Unsupported device type for {:?}",
                    device.source
                ));
            };

            let major_id = major(file_stat.st_rdev) as i64;
            let minor_id = minor(file_stat.st_rdev) as i64;
            let access = if device.permissions.trim().is_empty() {
                "rwm".to_string()
            } else {
                device.permissions.clone()
            };

            oci_devices.push(OciDevice {
                device_type: device_type.to_string(),
                path: device.destination.to_string_lossy().to_string(),
                major: Some(major_id),
                minor: Some(minor_id),
                file_mode: Some((file_stat.st_mode & 0o777) as u32),
                uid: owner_uid.or(Some(file_stat.st_uid)),
                gid: owner_gid.or(Some(file_stat.st_gid)),
            });
            cgroup_rules.push(LinuxDeviceCgroup {
                allow: true,
                device_type: Some(device_type.to_string()),
                major: Some(major_id),
                minor: Some(minor_id),
                access: Some(access),
            });
        }

        Ok((oci_devices, cgroup_rules))
    }

    fn host_devices() -> Result<Vec<OciDevice>> {
        let mut devices = Vec::new();
        Self::collect_host_devices(Path::new("/dev"), &mut devices)?;
        Ok(devices)
    }

    fn should_skip_host_device_dir(name: &str) -> bool {
        matches!(
            name,
            "pts" | "shm" | "fd" | "mqueue" | ".lxc" | ".lxd-mounts" | ".udev"
        )
    }

    fn should_skip_host_device_file(name: &str) -> bool {
        name == "console"
    }

    fn collect_host_devices(path: &Path, devices: &mut Vec<OciDevice>) -> Result<()> {
        for entry in std::fs::read_dir(path)
            .with_context(|| format!("failed to read host device directory {}", path.display()))?
        {
            let entry = entry?;
            let entry_path = entry.path();
            let entry_name = entry.file_name();
            let entry_name = entry_name.to_string_lossy();
            let metadata = std::fs::symlink_metadata(&entry_path).with_context(|| {
                format!("failed to stat host device path {}", entry_path.display())
            })?;
            let file_type = metadata.file_type();
            if file_type.is_dir() {
                if Self::should_skip_host_device_dir(&entry_name) {
                    continue;
                }
                Self::collect_host_devices(&entry_path, devices)?;
                continue;
            }
            if file_type.is_symlink() {
                continue;
            }
            if Self::should_skip_host_device_file(&entry_name) {
                continue;
            }
            if !(file_type.is_char_device() || file_type.is_block_device()) {
                continue;
            }

            let mode = metadata.mode();
            let sflag = SFlag::from_bits_truncate(mode);
            let device_type = if sflag.contains(SFlag::S_IFCHR) {
                "c"
            } else if sflag.contains(SFlag::S_IFBLK) {
                "b"
            } else {
                continue;
            };
            let major_id = major(metadata.rdev()) as i64;
            let minor_id = minor(metadata.rdev()) as i64;
            if major_id == 0 && minor_id == 0 {
                continue;
            }

            devices.push(OciDevice {
                device_type: device_type.to_string(),
                path: entry_path.display().to_string(),
                major: Some(major_id),
                minor: Some(minor_id),
                file_mode: Some(mode & 0o777),
                uid: Some(metadata.uid()),
                gid: Some(metadata.gid()),
            });
        }

        Ok(())
    }

    fn unpack_layer_with_tar(layer_file: &Path, rootfs_dir: &Path) -> Result<()> {
        let mut command = Command::new("tar");
        if layer_file.extension().and_then(|ext| ext.to_str()) == Some("gz") {
            command.arg("-xzf");
        } else {
            command.arg("-xf");
        }
        let output = command
            .arg(layer_file)
            .arg("-C")
            .arg(rootfs_dir)
            .arg("--no-same-owner")
            .arg("--no-same-permissions")
            .output()
            .with_context(|| format!("Failed to execute tar for {:?}", layer_file))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "Failed to unpack layer archive {:?}: {}",
                layer_file,
                stderr.trim()
            ));
        }
        Ok(())
    }

    fn normalize_image_id(id: &str) -> &str {
        id.strip_prefix("sha256:").unwrap_or(id)
    }

    fn image_id_matches(image_id: &str, candidate: &str) -> bool {
        if image_id == candidate {
            return true;
        }
        let normalized_image_id = Self::normalize_image_id(image_id);
        let normalized_candidate = Self::normalize_image_id(candidate);
        normalized_image_id == normalized_candidate
            || normalized_image_id.starts_with(normalized_candidate)
            || normalized_candidate.starts_with(normalized_image_id)
    }

    fn resolve_image_dir(&self, image_ref: &str) -> Result<PathBuf> {
        let images_dir = self.image_storage_root.join("images");
        if !images_dir.exists() {
            return Err(ImageAvailabilityError::NotPresentLocally {
                image_ref: image_ref.to_string(),
            }
            .into());
        }
        let entries = std::fs::read_dir(&images_dir)
            .with_context(|| format!("Failed to read images directory: {:?}", images_dir))?;

        for entry in entries {
            let entry = entry?;
            let image_dir = entry.path();
            if !image_dir.is_dir() {
                continue;
            }

            let metadata_path = image_dir.join("metadata.json");
            if !metadata_path.exists() {
                continue;
            }

            let metadata_bytes = std::fs::read(&metadata_path)
                .with_context(|| format!("Failed to read image metadata: {:?}", metadata_path))?;
            let metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes)
                .with_context(|| format!("Failed to parse image metadata: {:?}", metadata_path))?;

            let image_id = metadata
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let repo_tags: Vec<String> = metadata
                .get("repo_tags")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            if repo_tags.iter().any(|tag| tag == image_ref)
                || Self::image_id_matches(image_id, image_ref)
            {
                return Ok(image_dir);
            }
        }

        Err(ImageAvailabilityError::NotPresentLocally {
            image_ref: image_ref.to_string(),
        }
        .into())
    }

    fn image_metadata(&self, image_ref: &str) -> Result<crate::image::ImageMeta> {
        let image_dir = self.resolve_image_dir(image_ref)?;
        let metadata_path = image_dir.join("metadata.json");
        let metadata_bytes = std::fs::read(&metadata_path)
            .with_context(|| format!("Failed to read image metadata: {:?}", metadata_path))?;
        serde_json::from_slice(&metadata_bytes)
            .with_context(|| format!("Failed to parse image metadata: {:?}", metadata_path))
    }

    fn normalized_image_volume_paths(&self, image_ref: &str) -> Result<Vec<PathBuf>> {
        let metadata = self.image_metadata(image_ref)?;
        let mut declared = Vec::new();
        for raw in metadata.declared_volumes {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                continue;
            }
            let path = Path::new(trimmed);
            if !path.is_absolute() {
                return Err(anyhow::anyhow!(
                    "image {} declares non-absolute volume path {}",
                    image_ref,
                    trimmed
                ));
            }
            if path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::CurDir
                        | std::path::Component::Prefix(_)
                )
            }) {
                return Err(anyhow::anyhow!(
                    "image {} declares invalid volume path {}",
                    image_ref,
                    trimmed
                ));
            }
            declared.push(path.to_path_buf());
        }
        declared.sort();
        declared.dedup();
        Ok(declared)
    }

    fn ensure_image_volume_directories(&self, image_ref: &str, rootfs: &Path) -> Result<()> {
        if self.image_volumes != ImageVolumesMode::Mkdir {
            return Ok(());
        }
        for volume_path in self.normalized_image_volume_paths(image_ref)? {
            let relative = volume_path
                .strip_prefix("/")
                .context("image volume path must stay absolute")?;
            let target = rootfs.join(relative);
            std::fs::create_dir_all(&target).with_context(|| {
                format!(
                    "Failed to create image-defined volume directory {}",
                    target.display()
                )
            })?;
        }
        Ok(())
    }

    fn ensure_mount_targets(&self, rootfs: &Path, mounts: &[MountConfig]) -> Result<()> {
        for mount in mounts {
            let relative = mount.destination.strip_prefix("/").with_context(|| {
                format!(
                    "mount destination must be absolute: {}",
                    mount.destination.display()
                )
            })?;
            let target = rootfs.join(relative);
            let source_meta = std::fs::metadata(&mount.source).with_context(|| {
                format!("failed to stat mount source {}", mount.source.display())
            })?;

            if source_meta.is_dir() {
                if target.exists() && !target.is_dir() {
                    return Err(anyhow::anyhow!(
                        "mount target {} already exists and is not a directory",
                        target.display()
                    ));
                }
                std::fs::create_dir_all(&target).with_context(|| {
                    format!(
                        "failed to create mount target directory {}",
                        target.display()
                    )
                })?;
                continue;
            }

            let parent = target.parent().ok_or_else(|| {
                anyhow::anyhow!("mount target {} has no parent directory", target.display())
            })?;
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create mount target parent directory {}",
                    parent.display()
                )
            })?;
            if target.exists() {
                if target.is_dir() {
                    return Err(anyhow::anyhow!(
                        "mount target {} already exists and is a directory",
                        target.display()
                    ));
                }
            } else {
                std::fs::File::create(&target).with_context(|| {
                    format!("failed to create mount target file {}", target.display())
                })?;
            }
        }

        Ok(())
    }

    fn image_volume_state_dir(&self, container_id: &str) -> PathBuf {
        self.root.join("image-volumes").join(container_id)
    }

    fn image_volume_mounts(
        &self,
        container_id: &str,
        config: &ContainerConfig,
    ) -> Result<Vec<Mount>> {
        if self.image_volumes != ImageVolumesMode::Bind {
            return Ok(Vec::new());
        }

        let overridden_destinations: HashSet<PathBuf> = config
            .mounts
            .iter()
            .map(|mount| mount.destination.clone())
            .collect();
        let state_dir = self.image_volume_state_dir(container_id);
        let mut mounts = Vec::new();
        for (index, destination) in self
            .normalized_image_volume_paths(&config.image)?
            .into_iter()
            .enumerate()
        {
            if overridden_destinations.contains(&destination) {
                continue;
            }
            let source = state_dir.join(format!("volume-{index}"));
            std::fs::create_dir_all(&source).with_context(|| {
                format!(
                    "Failed to create image-defined volume source {}",
                    source.display()
                )
            })?;
            mounts.push(Mount {
                destination: destination.display().to_string(),
                source: Some(source.display().to_string()),
                mount_type: Some("bind".to_string()),
                options: Some(vec![
                    "private".to_string(),
                    "bind".to_string(),
                    "rw".to_string(),
                ]),
            });
        }
        Ok(mounts)
    }

    fn layer_files_for_image_dir(&self, image_ref: &str, image_dir: &Path) -> Result<Vec<PathBuf>> {
        if let Ok(metadata) = self.image_metadata(image_ref) {
            if !metadata.stored_layers.is_empty() {
                let mut layer_files = metadata
                    .stored_layers
                    .iter()
                    .map(|layer| image_dir.join(&layer.path))
                    .collect::<Vec<_>>();
                layer_files.sort_by_key(|p| {
                    p.file_stem()
                        .and_then(|s| s.to_str())
                        .and_then(|s| s.split('.').next())
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(u32::MAX)
                });
                return Ok(layer_files);
            }
        }
        let mut layer_files: Vec<PathBuf> = std::fs::read_dir(image_dir)?
            .filter_map(|e| e.ok().map(|v| v.path()))
            .filter(|p| matches!(p.extension().and_then(|s| s.to_str()), Some("gz" | "tar")))
            .collect();
        layer_files.sort_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.split('.').next())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(u32::MAX)
        });

        if layer_files.is_empty() {
            return Err(ImageAvailabilityError::NoLayers {
                image_ref: image_ref.to_string(),
                image_dir: image_dir.to_path_buf(),
            }
            .into());
        }

        Ok(layer_files)
    }

    fn ensure_minimum_rootfs_layout(&self, image_ref: &str, rootfs_dir: &Path) -> Result<()> {
        // Ensure minimum runtime paths exist for scratch-like images (e.g. pause).
        std::fs::create_dir_all(rootfs_dir.join("dev"))
            .context("Failed to create /dev in rootfs")?;
        std::fs::create_dir_all(rootfs_dir.join("proc"))
            .context("Failed to create /proc in rootfs")?;
        std::fs::create_dir_all(rootfs_dir.join("sys"))
            .context("Failed to create /sys in rootfs")?;

        self.ensure_image_volume_directories(image_ref, rootfs_dir)?;

        let dev_null = rootfs_dir.join("dev/null");
        if dev_null.exists() {
            let _ = std::fs::remove_file(&dev_null);
        }
        mknod(
            &dev_null,
            SFlag::S_IFCHR,
            Mode::from_bits_truncate(0o666),
            makedev(1, 3),
        )
        .context("Failed to create /dev/null char device in rootfs")?;
        Ok(())
    }

    fn populate_rootfs_from_image(
        &self,
        image_ref: &str,
        rootfs_dir: &Path,
        container_id: &str,
    ) -> Result<()> {
        if rootfs_dir.exists() {
            std::fs::remove_dir_all(rootfs_dir)
                .with_context(|| format!("Failed to clean rootfs directory: {:?}", rootfs_dir))?;
        }
        std::fs::create_dir_all(rootfs_dir)
            .with_context(|| format!("Failed to create rootfs directory: {:?}", rootfs_dir))?;

        let image_dir = self.resolve_image_dir(image_ref)?;
        let layer_files = self.layer_files_for_image_dir(image_ref, &image_dir)?;

        for layer_file in &layer_files {
            Self::unpack_layer_with_tar(layer_file, rootfs_dir)
                .with_context(|| format!("Failed to unpack layer archive: {:?}", layer_file))?;
        }

        self.ensure_minimum_rootfs_layout(image_ref, rootfs_dir)?;

        info!(
            "Prepared rootfs for container {} from image {}",
            container_id, image_ref
        );
        Ok(())
    }

    fn cached_rootfs_dir_for_image(&self, image_ref: &str) -> Result<PathBuf> {
        let image_dir = self.resolve_image_dir(image_ref)?;
        let image_id = image_dir
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid image directory name for {}", image_ref))?;
        Ok(self
            .image_storage_root
            .join("snapshots")
            .join(image_id)
            .join("rootfs"))
    }

    fn copy_rootfs_tree(&self, source: &Path, destination: &Path) -> Result<()> {
        if destination.exists() {
            std::fs::remove_dir_all(destination).with_context(|| {
                format!(
                    "Failed to clean destination rootfs directory {}",
                    destination.display()
                )
            })?;
        }
        std::fs::create_dir_all(destination).with_context(|| {
            format!(
                "Failed to create destination rootfs directory {}",
                destination.display()
            )
        })?;
        let output = Command::new("cp")
            .arg("-a")
            .arg(format!("{}/.", source.display()))
            .arg(destination)
            .output()
            .with_context(|| format!("Failed to execute cp for {}", source.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow::anyhow!(
                "failed to copy cached rootfs from {} to {}: {}",
                source.display(),
                destination.display(),
                stderr
            ));
        }
        Ok(())
    }

    fn prepare_rootfs_from_image(
        &self,
        image_ref: &str,
        rootfs_dir: &Path,
        container_id: &str,
    ) -> Result<()> {
        match self.rootfs_snapshotter {
            RootfsSnapshotter::InternalOverlayUntar => {
                self.populate_rootfs_from_image(image_ref, rootfs_dir, container_id)
            }
            RootfsSnapshotter::InternalCachedRootfs => {
                let cached_rootfs = self.cached_rootfs_dir_for_image(image_ref)?;
                if !cached_rootfs.exists() {
                    self.populate_rootfs_from_image(image_ref, &cached_rootfs, container_id)?;
                }
                self.copy_rootfs_tree(&cached_rootfs, rootfs_dir)?;
                self.ensure_minimum_rootfs_layout(image_ref, rootfs_dir)?;
                info!(
                    "Prepared rootfs for container {} from cached snapshot of image {}",
                    container_id, image_ref
                );
                Ok(())
            }
        }
    }

    fn spec_from_restore_template(
        &self,
        container_id: &str,
        config: &ContainerConfig,
        restore: &RestoreCheckpointMetadata,
    ) -> Result<Spec> {
        let mut spec: Spec = serde_json::from_value(restore.oci_config.clone())
            .context("Failed to parse OCI config from checkpoint artifact")?;

        spec.root = Some(Root {
            path: config.rootfs.to_string_lossy().to_string(),
            readonly: Some(config.readonly_rootfs),
        });
        spec.hostname = config.hostname.clone().or(spec.hostname);

        if let Some(process) = spec.process.as_mut() {
            process.terminal = Some(config.tty);
            process.apparmor_profile = config.apparmor_profile.clone();
            process.selinux_label = config.selinux_label.clone();
            process.no_new_privileges =
                Some(config.no_new_privileges.unwrap_or(!config.privileged));
            if let Some(working_dir) = config.working_dir.as_ref() {
                process.cwd = working_dir.to_string_lossy().to_string();
            }
            if !config.command.is_empty() {
                let mut args = config.command.clone();
                args.extend(config.args.clone());
                process.args = args;
            }
            let merged_env = self.merged_env(&config.env);
            if !merged_env.is_empty() {
                process.env = Some(
                    merged_env
                        .iter()
                        .map(|(key, value)| format!("{}={}", key, value))
                        .collect(),
                );
            }
            let capability_baseline = self.capability_baseline(config.privileged);
            process.capabilities = Some(Self::apply_capability_overrides(
                &capability_baseline,
                config.capabilities.as_ref(),
                self.add_inheritable_capabilities,
            ));
            self.apply_default_ulimits(process);
        }

        let linux = spec.linux.get_or_insert(Linux {
            namespaces: None,
            uid_mappings: None,
            gid_mappings: None,
            devices: None,
            net_devices: None,
            cgroups_path: None,
            resources: None,
            rootfs_propagation: None,
            seccomp: None,
            sysctl: None,
            mount_label: None,
            masked_paths: None,
            readonly_paths: None,
            intel_rdt: None,
        });
        linux.namespaces = Some(self.build_namespaces(config));
        linux.cgroups_path = if self.disable_cgroup {
            None
        } else {
            Self::oci_cgroups_path(config.cgroup_parent.as_deref(), container_id)
        };
        linux.seccomp = self.load_seccomp_profile(
            config.seccomp_profile.as_ref(),
            config.seccomp_notifier.as_ref(),
        )?;
        linux.mount_label = config.selinux_label.clone();
        let merged_sysctls = self.merged_sysctls(&config.sysctls);
        if !merged_sysctls.is_empty() {
            linux.sysctl = Some(merged_sysctls);
        }
        let (masked_paths, readonly_paths) = self.effective_proc_paths(
            config.privileged,
            &config.masked_paths,
            &config.readonly_paths,
        )?;
        linux.masked_paths = masked_paths;
        linux.readonly_paths = readonly_paths;
        if !self.disable_cgroup {
            if let Some(resources) = config
                .linux_resources
                .as_ref()
                .map(Self::linux_resources_to_oci)
            {
                linux.resources = Some(resources);
            }
            if let Some(limit) = config.pids_limit {
                linux
                    .resources
                    .get_or_insert(LinuxResources {
                        network: None,
                        pids: None,
                        memory: None,
                        cpu: None,
                        block_io: None,
                        hugepage_limits: None,
                        devices: None,
                        intel_rdt: None,
                        unified: None,
                    })
                    .pids = Some(LinuxPids { limit });
            }
        }
        self.apply_oom_score_adj_policy(&mut spec, Self::requested_oom_score_adj(config))?;
        let process_env = spec
            .process
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("OCI restore spec is missing process configuration"))?
            .env
            .get_or_insert_with(Vec::new);
        let mounts = spec.mounts.get_or_insert_with(Vec::new);
        self.apply_timezone_mount(mounts, process_env)?;

        let mut annotations = spec.annotations.unwrap_or_default();
        annotations.insert(
            "org.opencontainers.image.ref.name".to_string(),
            restore.image_ref.clone(),
        );
        Self::insert_label_annotation(&mut annotations, &config.labels)?;
        for (key, value) in &config.annotations {
            annotations.insert(key.clone(), value.clone());
        }
        spec.annotations = Some(annotations);

        Ok(spec)
    }

    pub fn checkpoint_container(
        &self,
        container_id: &str,
        image_path: &Path,
        work_path: &Path,
    ) -> Result<()> {
        std::fs::create_dir_all(image_path).with_context(|| {
            format!(
                "Failed to create checkpoint image directory {}",
                image_path.display()
            )
        })?;
        std::fs::create_dir_all(work_path).with_context(|| {
            format!(
                "Failed to create checkpoint work directory {}",
                work_path.display()
            )
        })?;

        let image_path = image_path.to_string_lossy().to_string();
        let work_path = work_path.to_string_lossy().to_string();
        let mut checkpoint_args = vec![
            "checkpoint",
            "--file-locks",
            "--image-path",
            image_path.as_str(),
            "--work-path",
            work_path.as_str(),
            "--leave-running",
        ];
        let criu_path = self.criu_path.to_string_lossy().to_string();
        if !criu_path.is_empty() {
            checkpoint_args.push("--criu");
            checkpoint_args.push(criu_path.as_str());
        }
        checkpoint_args.push(container_id);
        self.runc_exec(&checkpoint_args)
    }

    pub fn pause_container(&self, container_id: &str) -> Result<()> {
        match self.get_runc_state(container_id)? {
            Some(state) if state.status == "paused" => Ok(()),
            Some(state) if state.status == "running" => self.runc_exec(&["pause", container_id]),
            Some(state) => Err(anyhow::anyhow!(
                "container {} cannot be paused from state {}",
                container_id,
                state.status
            )),
            None => Err(anyhow::anyhow!("container {} not found", container_id)),
        }
    }

    pub fn resume_container(&self, container_id: &str) -> Result<()> {
        match self.get_runc_state(container_id)? {
            Some(state) if state.status == "running" => Ok(()),
            Some(state) if state.status == "paused" => self.runc_exec(&["resume", container_id]),
            Some(state) => Err(anyhow::anyhow!(
                "container {} cannot be resumed from state {}",
                container_id,
                state.status
            )),
            None => Err(anyhow::anyhow!("container {} not found", container_id)),
        }
    }

    pub fn is_container_paused(&self, container_id: &str) -> Result<bool> {
        Ok(matches!(
            self.get_runc_state(container_id)?,
            Some(state) if state.status == "paused"
        ))
    }

    pub fn restore_container_from_checkpoint(
        &self,
        container_id: &str,
        image_path: &Path,
        work_path: &Path,
    ) -> Result<()> {
        std::fs::create_dir_all(work_path).with_context(|| {
            format!(
                "Failed to create restore work directory {}",
                work_path.display()
            )
        })?;
        let bundle_path = self.bundle_path(container_id);

        self.restore_rootfs_snapshot(container_id, image_path)?;

        let image_path = image_path.to_string_lossy().to_string();
        let work_path = work_path.to_string_lossy().to_string();
        let bundle_path = bundle_path.to_string_lossy().to_string();
        let mut restore_args = vec![
            "restore",
            "-d",
            "--image-path",
            image_path.as_str(),
            "--work-path",
            work_path.as_str(),
            "--bundle",
            bundle_path.as_str(),
        ];
        let criu_path = self.criu_path.to_string_lossy().to_string();
        if !criu_path.is_empty() {
            restore_args.push("--criu");
            restore_args.push(criu_path.as_str());
        }
        if self.no_pivot {
            restore_args.push("--no-pivot");
        }
        restore_args.push(container_id);
        self.runc_exec(&restore_args)
    }

    pub fn restore_attach_shim(&self, container_id: &str) -> Result<()> {
        let shim_manager = self
            .shim_manager
            .as_ref()
            .context("attach recovery requires shim-enabled runtime")?;
        if !self.container_uses_terminal(container_id)? {
            return Err(anyhow::anyhow!(
                "attach recovery is only supported for tty containers"
            ));
        }

        let bundle_path = self.bundle_path(container_id);
        if !bundle_path.exists() {
            return Err(anyhow::anyhow!(
                "bundle path is missing for attach recovery: {}",
                bundle_path.display()
            ));
        }

        shim_manager
            .start_shim(container_id, &bundle_path)
            .context("failed to restore shim for attach recovery")?;
        Ok(())
    }

    pub fn new(runtime_path: PathBuf, root: PathBuf) -> Self {
        let image_storage_root = root
            .parent()
            .map(|parent| parent.join("storage"))
            .unwrap_or_else(|| root.join("storage"));
        Self {
            runtime_path,
            runtime_config_path: PathBuf::new(),
            root,
            image_storage_root,
            shim_manager: None,
            default_env: Vec::new(),
            default_capabilities: Self::default_capabilities(),
            default_sysctls: HashMap::new(),
            default_ulimits: Vec::new(),
            allowed_devices: HashSet::new(),
            additional_devices: Vec::new(),
            device_ownership_from_security_context: false,
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            add_inheritable_capabilities: false,
            base_runtime_spec: None,
            default_mounts_file: None,
            hooks_dirs: Vec::new(),
            absent_mount_sources_to_reject: Vec::new(),
            image_volumes: ImageVolumesMode::Mkdir,
            rootfs_snapshotter: RootfsSnapshotter::InternalOverlayUntar,
            container_create_timeout_secs: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
            container_stop_timeout_secs: 30,
            criu_path: PathBuf::new(),
            restrict_oom_score_adj: false,
            bind_mount_prefix: PathBuf::new(),
            disable_cgroup: false,
            default_seccomp_profile_path: None,
            exec_cpu_affinity: String::new(),
            no_pivot: false,
            no_new_keyring: false,
            disable_proc_mount: false,
            timezone: String::new(),
            cgroup_driver: CgroupDriverConfig::Cgroupfs,
        }
    }

    /// 创建带shim支持的运行时
    pub fn with_shim(runtime_path: PathBuf, root: PathBuf, shim_config: ShimConfig) -> Self {
        let image_storage_root = root
            .parent()
            .map(|parent| parent.join("storage"))
            .unwrap_or_else(|| root.join("storage"));
        Self::with_shim_and_image_storage(runtime_path, root, image_storage_root, shim_config)
    }

    pub fn with_shim_and_image_storage(
        runtime_path: PathBuf,
        root: PathBuf,
        image_storage_root: PathBuf,
        shim_config: ShimConfig,
    ) -> Self {
        let no_pivot = shim_config.no_pivot;
        let runtime_config_path = shim_config.runtime_config_path.clone();
        let shim_manager = Arc::new(ShimManager::new(shim_config));
        Self {
            runtime_path,
            runtime_config_path,
            root,
            image_storage_root,
            shim_manager: Some(shim_manager),
            default_env: Vec::new(),
            default_capabilities: Self::default_capabilities(),
            default_sysctls: HashMap::new(),
            default_ulimits: Vec::new(),
            allowed_devices: HashSet::new(),
            additional_devices: Vec::new(),
            device_ownership_from_security_context: false,
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            add_inheritable_capabilities: false,
            base_runtime_spec: None,
            default_mounts_file: None,
            hooks_dirs: Vec::new(),
            absent_mount_sources_to_reject: Vec::new(),
            image_volumes: ImageVolumesMode::Mkdir,
            rootfs_snapshotter: RootfsSnapshotter::InternalOverlayUntar,
            container_create_timeout_secs: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
            container_stop_timeout_secs: 30,
            criu_path: PathBuf::new(),
            restrict_oom_score_adj: false,
            bind_mount_prefix: PathBuf::new(),
            disable_cgroup: false,
            default_seccomp_profile_path: None,
            exec_cpu_affinity: String::new(),
            no_pivot,
            no_new_keyring: false,
            disable_proc_mount: false,
            timezone: String::new(),
            cgroup_driver: CgroupDriverConfig::Cgroupfs,
        }
    }

    /// 启用shim支持
    pub fn enable_shim(&mut self, config: ShimConfig) {
        self.no_pivot = config.no_pivot;
        self.runtime_config_path = config.runtime_config_path.clone();
        self.shim_manager = Some(Arc::new(ShimManager::new(config)));
    }

    pub fn set_restrict_oom_score_adj(&mut self, restrict: bool) {
        self.restrict_oom_score_adj = restrict;
    }

    pub fn set_container_stop_timeout_secs(&mut self, timeout_secs: u32) {
        self.container_stop_timeout_secs = timeout_secs;
    }

    pub fn set_container_create_timeout_secs(&mut self, timeout_secs: u32) {
        self.container_create_timeout_secs = timeout_secs;
    }

    pub fn set_default_env(&mut self, default_env: Vec<(String, String)>) {
        self.default_env = default_env;
    }

    pub fn set_default_capabilities(&mut self, default_capabilities: Vec<String>) {
        self.default_capabilities = default_capabilities
            .into_iter()
            .map(|capability| Self::normalize_capability_name(&capability))
            .collect();
    }

    pub fn set_default_sysctls(&mut self, default_sysctls: HashMap<String, String>) {
        self.default_sysctls = default_sysctls;
    }

    pub fn set_default_ulimits(&mut self, default_ulimits: Vec<Rlimit>) {
        self.default_ulimits = default_ulimits;
    }

    pub fn set_allowed_devices(&mut self, allowed_devices: Vec<PathBuf>) {
        self.allowed_devices = allowed_devices.into_iter().collect();
    }

    pub fn set_additional_devices(&mut self, additional_devices: Vec<DeviceMapping>) {
        self.additional_devices = additional_devices;
    }

    pub fn set_device_ownership_from_security_context(&mut self, enabled: bool) {
        self.device_ownership_from_security_context = enabled;
    }

    pub fn set_privileged_without_host_devices(&mut self, enabled: bool) {
        self.privileged_without_host_devices = enabled;
    }

    pub fn set_privileged_without_host_devices_all_devices_allowed(&mut self, enabled: bool) {
        self.privileged_without_host_devices_all_devices_allowed = enabled;
    }

    pub fn set_add_inheritable_capabilities(&mut self, enabled: bool) {
        self.add_inheritable_capabilities = enabled;
    }

    pub fn set_base_runtime_spec(&mut self, base_runtime_spec: Option<Spec>) {
        self.base_runtime_spec = base_runtime_spec;
    }

    pub fn set_default_mounts_file(&mut self, path: PathBuf) {
        self.default_mounts_file = (!path.as_os_str().is_empty()).then_some(path);
    }

    pub fn set_hooks_dirs(&mut self, values: Vec<PathBuf>) {
        self.hooks_dirs = values;
    }

    pub fn set_absent_mount_sources_to_reject(&mut self, values: Vec<PathBuf>) {
        self.absent_mount_sources_to_reject = values;
    }

    pub fn set_image_volumes_mode(&mut self, mode: ImageVolumesMode) {
        self.image_volumes = mode;
    }

    pub fn set_rootfs_snapshotter(&mut self, snapshotter: RootfsSnapshotter) {
        self.rootfs_snapshotter = snapshotter;
    }

    pub fn set_criu_path(&mut self, criu_path: PathBuf) {
        self.criu_path = criu_path;
    }

    pub fn set_bind_mount_prefix(&mut self, bind_mount_prefix: PathBuf) {
        self.bind_mount_prefix = bind_mount_prefix;
    }

    pub fn set_disable_cgroup(&mut self, disable_cgroup: bool) {
        self.disable_cgroup = disable_cgroup;
    }

    pub fn set_default_seccomp_profile_path(&mut self, path: PathBuf) {
        self.default_seccomp_profile_path = (!path.as_os_str().is_empty()).then_some(path);
    }

    pub fn set_exec_cpu_affinity(&mut self, exec_cpu_affinity: String) {
        self.exec_cpu_affinity = exec_cpu_affinity;
    }

    pub fn set_no_pivot(&mut self, no_pivot: bool) {
        self.no_pivot = no_pivot;
    }

    pub fn set_no_new_keyring(&mut self, no_new_keyring: bool) {
        self.no_new_keyring = no_new_keyring;
    }

    pub fn set_disable_proc_mount(&mut self, disable_proc_mount: bool) {
        self.disable_proc_mount = disable_proc_mount;
    }

    pub fn set_timezone(&mut self, timezone: String) {
        self.timezone = timezone;
    }

    pub fn set_cgroup_driver(&mut self, cgroup_driver: CgroupDriverConfig) {
        self.cgroup_driver = cgroup_driver;
    }

    /// 检查是否启用了shim
    pub fn is_shim_enabled(&self) -> bool {
        self.shim_manager.is_some()
    }

    /// 获取容器的bundle目录
    fn bundle_path(&self, container_id: &str) -> PathBuf {
        self.root.join(container_id)
    }

    pub fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.bundle_path(container_id)
    }

    pub fn runtime_root(&self) -> &Path {
        &self.root
    }

    pub fn runtime_path(&self) -> &Path {
        &self.runtime_path
    }

    pub fn probe_runtime_features(&self) -> RuntimeFeatureProbe {
        let output = match self.run_command_output(&["features"]) {
            Ok(output) => output,
            Err(err) => {
                return RuntimeFeatureProbe {
                    error: Some(format!("failed to execute runtime features command: {err}")),
                    ..Default::default()
                };
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let detail = if !stderr.is_empty() {
                stderr
            } else if !stdout.is_empty() {
                stdout
            } else {
                format!("status={}", output.status)
            };
            return RuntimeFeatureProbe {
                error: Some(format!("runtime features command failed: {detail}")),
                ..Default::default()
            };
        }

        let parsed = match serde_json::from_slice::<OciRuntimeFeaturesDocument>(&output.stdout) {
            Ok(parsed) => parsed,
            Err(err) => {
                return RuntimeFeatureProbe {
                    error: Some(format!("failed to parse runtime features output: {err}")),
                    ..Default::default()
                };
            }
        };

        if parsed.oci_version_min.trim().is_empty() || parsed.oci_version_max.trim().is_empty() {
            return RuntimeFeatureProbe {
                error: Some("runtime features structure is not valid".to_string()),
                ..Default::default()
            };
        }

        let idmap_mounts = parsed
            .linux
            .as_ref()
            .and_then(|linux| linux.mount_extensions.as_ref())
            .and_then(|extensions| extensions.idmap.as_ref())
            .and_then(|feature| feature.enabled)
            .unwrap_or(false);
        let recursive_read_only_mounts = parsed.mount_options.iter().any(|option| option == "rro");

        RuntimeFeatureProbe {
            available: true,
            idmap_mounts,
            recursive_read_only_mounts,
            mount_options: parsed.mount_options,
            oci_version_min: Some(parsed.oci_version_min),
            oci_version_max: Some(parsed.oci_version_max),
            error: None,
        }
    }

    pub fn runtime_config_path(&self) -> &Path {
        &self.runtime_config_path
    }

    pub fn set_runtime_config_path(&mut self, runtime_config_path: PathBuf) {
        self.runtime_config_path = runtime_config_path;
    }

    /// 获取容器的config.json路径
    fn config_path(&self, container_id: &str) -> PathBuf {
        self.bundle_path(container_id).join("config.json")
    }

    fn runtime_command(&self) -> Command {
        let mut cmd = Command::new(&self.runtime_path);
        if self.cgroup_driver == CgroupDriverConfig::Systemd {
            cmd.arg("--systemd-cgroup");
        }
        if !self.runtime_config_path.as_os_str().is_empty() {
            cmd.arg("--config").arg(&self.runtime_config_path);
        }
        cmd.env("XDG_RUNTIME_DIR", "/run/user/0");
        cmd
    }

    /// 执行runc命令并返回输出（仅用于需要解析stdout的查询类命令）
    fn run_command_output(&self, args: &[&str]) -> Result<Output> {
        debug!(
            "Executing: {} {}",
            self.runtime_path.display(),
            args.join(" ")
        );

        let output = self
            .runtime_command()
            .args(args)
            .output()
            .context("Failed to execute runc command")?;

        Ok(output)
    }

    /// 执行runc命令并检查状态（用于start/stop/run等动作类命令）
    /// 注意：不能对`runc run -d`使用output()，否则可能因后台子进程继承pipe导致阻塞。
    fn runc_exec(&self, args: &[&str]) -> Result<()> {
        debug!(
            "Executing (status): {} {}",
            self.runtime_path.display(),
            args.join(" ")
        );

        let status = self
            .runtime_command()
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("Failed to execute runc command")?;

        if !status.success() {
            let detail = self
                .run_command_output(args)
                .ok()
                .and_then(|out| {
                    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                    if stderr.is_empty() {
                        None
                    } else {
                        Some(stderr)
                    }
                })
                .unwrap_or_else(|| format!("status={}", status));
            error!("runc command failed: {}", detail);
            return Err(anyhow::anyhow!("runc command failed: {}", detail));
        }

        Ok(())
    }

    /// 创建OCI配置
    fn create_spec(&self, config: &ContainerConfig, container_id: &str) -> Result<Spec> {
        let mut spec = self.base_spec_template();
        let image_defaults = self.image_runtime_defaults(&config.image)?;

        // 设置root配置
        spec.root = Some(Root {
            path: config.rootfs.to_string_lossy().to_string(),
            readonly: Some(config.readonly_rootfs),
        });

        // 设置进程配置
        let mut args = if !config.command.is_empty() {
            let mut resolved = config.command.clone();
            if !config.args.is_empty() {
                resolved.extend(config.args.clone());
            }
            resolved
        } else {
            let mut resolved = image_defaults.entrypoint.clone();
            if !config.args.is_empty() {
                resolved.extend(config.args.clone());
            } else if !image_defaults.cmd.is_empty() {
                resolved.extend(image_defaults.cmd.clone());
            }
            resolved
        };

        // 如果没有命令，使用默认shell
        if args.is_empty() {
            args = vec!["sh".to_string()];
        }

        // 转换环境变量为字符串格式
        let env: Vec<String> = self
            .merge_env_layers(&image_defaults.env, &config.env)
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        let existing_process = spec.process.clone();
        let mut process = existing_process.unwrap_or(Process {
            terminal: None,
            user: None,
            args: Vec::new(),
            env: None,
            cwd: "/".to_string(),
            capabilities: None,
            rlimits: None,
            oom_score_adj: None,
            scheduler: None,
            no_new_privileges: None,
            apparmor_profile: None,
            selinux_label: None,
            io_priority: None,
        });
        process.terminal = Some(config.tty);
        process.user = Self::build_user(config);
        process.args = args;
        process.env = if env.is_empty() { None } else { Some(env) };
        process.cwd = config
            .working_dir
            .as_ref()
            .or(image_defaults.working_dir.as_ref())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "/".to_string());
        let capability_baseline = self.capability_baseline(config.privileged);
        process.capabilities = Some(Self::apply_capability_overrides(
            &capability_baseline,
            config.capabilities.as_ref(),
            self.add_inheritable_capabilities,
        ));
        process.no_new_privileges = Some(config.no_new_privileges.unwrap_or(!config.privileged));
        process.apparmor_profile = config.apparmor_profile.clone();
        process.selinux_label = config.selinux_label.clone();
        self.apply_default_ulimits(&mut process);
        spec.process = Some(process);

        self.apply_oom_score_adj_policy(&mut spec, Self::requested_oom_score_adj(config))?;

        // 设置主机名
        spec.hostname = config.hostname.clone();

        // 设置挂载点
        let keep_hugepages_mount = std::env::var("CRIUS_ENABLE_HUGEPAGES_MOUNT")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let mut mounts = self.build_mounts(container_id, config, keep_hugepages_mount)?;
        let process_env = spec
            .process
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("OCI spec is missing process configuration"))?
            .env
            .get_or_insert_with(Vec::new);
        self.apply_timezone_mount(&mut mounts, process_env)?;
        spec.mounts = Some(mounts);
        if let Some(hooks) = self.load_configured_hooks()? {
            spec.hooks = Some(match spec.hooks.take() {
                Some(existing) => Self::merge_hooks(existing, hooks),
                None => hooks,
            });
        }

        // 设置Linux配置
        let mut devices = if config.privileged {
            if self.privileged_without_host_devices {
                Vec::new()
            } else {
                Self::host_devices()?
            }
        } else {
            Spec::default_devices(config.tty)
        };

        let mut resources = config
            .linux_resources
            .as_ref()
            .map(Self::linux_resources_to_oci)
            .unwrap_or_else(|| LinuxResources {
                network: None,
                pids: None,
                memory: None,
                cpu: None,
                block_io: None,
                hugepage_limits: None,
                devices: None,
                intel_rdt: None,
                unified: None,
            });

        let (owner_uid, owner_gid) = if self.device_ownership_from_security_context {
            let uid = config
                .user
                .as_deref()
                .and_then(|value| value.trim().parse::<u32>().ok())
                .filter(|value| *value > 0);
            let gid = config.run_as_group.filter(|value| *value > 0);
            (uid, gid)
        } else {
            (None, None)
        };

        if !self.additional_devices.is_empty() {
            let (extra_devices, extra_cgroup_rules) =
                Self::device_mappings_to_oci(&self.additional_devices, owner_uid, owner_gid)?;
            devices.extend(extra_devices);
            resources
                .devices
                .get_or_insert_with(Vec::new)
                .extend(extra_cgroup_rules);
        }

        if !config.devices.is_empty() {
            if !self.allowed_devices.is_empty() {
                for device in &config.devices {
                    if !self.allowed_devices.contains(&device.source) {
                        return Err(anyhow::anyhow!(
                            "device {} is not allowed by runtime.allowed_devices",
                            device.source.display()
                        ));
                    }
                }
            }

            let (extra_devices, extra_cgroup_rules) =
                Self::device_mappings_to_oci(&config.devices, owner_uid, owner_gid)?;
            devices.extend(extra_devices);
            resources
                .devices
                .get_or_insert_with(Vec::new)
                .extend(extra_cgroup_rules);
        }

        if let Some(limit) = config.pids_limit {
            resources.pids = Some(LinuxPids { limit });
        }

        if config.privileged {
            if !self.privileged_without_host_devices
                || self.privileged_without_host_devices_all_devices_allowed
            {
                resources.devices = Some(vec![LinuxDeviceCgroup {
                    allow: true,
                    device_type: None,
                    major: None,
                    minor: None,
                    access: Some("rwm".to_string()),
                }]);
            } else {
                resources.devices.get_or_insert_with(Vec::new);
            }
        } else if resources.devices.is_none() {
            resources.devices = Some(vec![LinuxDeviceCgroup {
                allow: true,
                device_type: None,
                major: None,
                minor: None,
                access: Some("rwm".to_string()),
            }]);
        }

        let mut namespaces = self.build_namespaces(config);
        let (uid_mappings, gid_mappings) = Self::build_user_namespace_mappings(config)?;
        if uid_mappings.is_some()
            && !namespaces
                .iter()
                .any(|namespace| namespace.ns_type == "user")
        {
            namespaces.push(OciNamespace {
                ns_type: "user".to_string(),
                path: None,
            });
        }

        let (masked_paths, readonly_paths) = self.effective_proc_paths(
            config.privileged,
            &config.masked_paths,
            &config.readonly_paths,
        )?;
        let mut linux = spec.linux.unwrap_or(Linux {
            namespaces: None,
            uid_mappings: None,
            gid_mappings: None,
            devices: None,
            net_devices: None,
            cgroups_path: None,
            resources: None,
            rootfs_propagation: None,
            seccomp: None,
            sysctl: None,
            mount_label: None,
            masked_paths: None,
            readonly_paths: None,
            intel_rdt: None,
        });
        linux.namespaces = Some(namespaces);
        linux.uid_mappings = uid_mappings;
        linux.gid_mappings = gid_mappings;
        linux.devices = Some(devices);
        linux.cgroups_path = if self.disable_cgroup {
            None
        } else {
            Self::oci_cgroups_path(config.cgroup_parent.as_deref(), container_id)
        };
        linux.resources = if self.disable_cgroup {
            None
        } else {
            Some(resources)
        };
        linux.seccomp = self.load_seccomp_profile(
            config.seccomp_profile.as_ref(),
            config.seccomp_notifier.as_ref(),
        )?;
        let merged_sysctls = self.merged_sysctls(&config.sysctls);
        linux.sysctl = if merged_sysctls.is_empty() {
            None
        } else {
            Some(merged_sysctls)
        };
        linux.mount_label = config.selinux_label.clone();
        linux.rootfs_propagation = Self::merge_rootfs_propagation(
            linux.rootfs_propagation.as_deref(),
            Self::desired_rootfs_propagation(&config.mounts),
        )
        .map(ToString::to_string);
        linux.masked_paths = masked_paths;
        linux.readonly_paths = readonly_paths;
        spec.linux = Some(linux);
        self.apply_oom_score_adj_policy(&mut spec, Self::requested_oom_score_adj(config))?;

        // 设置注解
        let mut annotations = spec.annotations.unwrap_or_default();
        annotations.insert(
            "org.opencontainers.image.ref.name".to_string(),
            config.image.clone(),
        );
        Self::insert_label_annotation(&mut annotations, &config.labels)?;
        for (k, v) in &config.annotations {
            annotations.insert(k.clone(), v.clone());
        }
        spec.annotations = Some(annotations);

        Ok(spec)
    }

    /// 创建bundle目录结构
    fn create_bundle(&self, container_id: &str, rootfs: &Path, spec: &Spec) -> Result<()> {
        let bundle_path = self.bundle_path(container_id);

        // 创建bundle目录
        std::fs::create_dir_all(&bundle_path).context("Failed to create bundle directory")?;

        // 保存config.json
        self.save_spec_for_bundle(spec, self.config_path(container_id))?;

        // 当前运行时使用 OCI spec.root.path 作为 rootfs 来源，bundle 内不再强制准备 rootfs 目录。
        let _ = rootfs;

        info!(
            "Created bundle for container {} at {:?}",
            container_id, bundle_path
        );
        Ok(())
    }

    fn save_spec_for_bundle(&self, spec: &Spec, path: PathBuf) -> Result<()> {
        let mut value =
            serde_json::to_value(spec).context("Failed to encode OCI spec for bundle write")?;
        self.inject_mount_extension_fields(&mut value)?;
        let json =
            serde_json::to_string_pretty(&value).context("Failed to serialize OCI bundle JSON")?;
        std::fs::write(&path, json)
            .with_context(|| format!("Failed to write OCI bundle {}", path.display()))?;
        Ok(())
    }

    fn inject_mount_extension_fields(&self, spec_value: &mut Value) -> Result<()> {
        let Some(mounts) = spec_value.get_mut("mounts").and_then(Value::as_array_mut) else {
            return Ok(());
        };

        for mount in mounts {
            let Some(options) = mount.get_mut("options").and_then(Value::as_array_mut) else {
                continue;
            };

            let mut filtered_options = Vec::with_capacity(options.len());
            let mut uid_mappings = None;
            let mut gid_mappings = None;
            for option in options.iter() {
                let Some(raw) = option.as_str() else {
                    continue;
                };
                if let Some(encoded) = raw.strip_prefix(INTERNAL_UID_MAPPINGS_MOUNT_OPTION_PREFIX) {
                    uid_mappings = Some(
                        serde_json::from_str::<Vec<crate::oci::spec::IdMapping>>(encoded)
                            .context("Failed to decode internal mount uid mappings")?,
                    );
                    continue;
                }
                if let Some(encoded) = raw.strip_prefix(INTERNAL_GID_MAPPINGS_MOUNT_OPTION_PREFIX) {
                    gid_mappings = Some(
                        serde_json::from_str::<Vec<crate::oci::spec::IdMapping>>(encoded)
                            .context("Failed to decode internal mount gid mappings")?,
                    );
                    continue;
                }
                filtered_options.push(Value::String(raw.to_string()));
            }
            *options = filtered_options;

            if let Some(uid_mappings) = uid_mappings {
                mount["uidMappings"] = serde_json::to_value(uid_mappings)
                    .context("Failed to encode mount uid mappings")?;
            }
            if let Some(gid_mappings) = gid_mappings {
                mount["gidMappings"] = serde_json::to_value(gid_mappings)
                    .context("Failed to encode mount gid mappings")?;
            }
        }

        Ok(())
    }

    /// 分步创建：准备 rootfs（NRI 可在后续步骤介入 spec）。
    pub fn prepare_rootfs(&self, container_id: &str, config: &ContainerConfig) -> Result<()> {
        let checkpoint_restore = Self::checkpoint_restore_from_annotations(&config.annotations);
        let image_ref = checkpoint_restore
            .as_ref()
            .map(|restore| restore.image_ref.as_str())
            .unwrap_or(config.image.as_str());
        self.prepare_rootfs_from_image(image_ref, &config.rootfs, container_id)
            .context("Failed to prepare rootfs from image")?;
        self.ensure_mount_targets(&config.rootfs, &config.mounts)
            .context("Failed to prepare mount targets")?;
        Ok(())
    }

    /// 分步创建：构建 pristine OCI spec。
    pub fn build_spec(&self, container_id: &str, config: &ContainerConfig) -> Result<Spec> {
        let checkpoint_restore = Self::checkpoint_restore_from_annotations(&config.annotations);
        if let Some(checkpoint_restore) = checkpoint_restore.as_ref() {
            self.spec_from_restore_template(container_id, config, checkpoint_restore)
                .context("Failed to create OCI spec from checkpoint artifact")
        } else {
            self.create_spec(config, container_id)
                .context("Failed to create OCI spec")
        }
    }

    pub(crate) fn validate_mount_requests(
        &self,
        config: &ContainerConfig,
    ) -> std::result::Result<(), MountSemanticsError> {
        self.validate_mount_semantics(&config.mounts, config.selinux_label.as_deref())
    }

    /// 分步创建：落盘 bundle（config.json + bundle 目录）。
    pub fn write_bundle(&self, container_id: &str, rootfs: &Path, spec: &Spec) -> Result<()> {
        self.create_bundle(container_id, rootfs, spec)
    }

    /// 分步创建：从 bundle 读取 OCI spec。
    pub fn load_spec(&self, container_id: &str) -> Result<Spec> {
        Spec::load(self.config_path(container_id))
            .map_err(|e| anyhow::anyhow!("Failed to load OCI spec for {}: {}", container_id, e))
    }

    /// 获取runc容器状态
    fn get_runc_state(&self, container_id: &str) -> Result<Option<RuncState>> {
        let output = self.run_command_output(&["state", container_id])?;

        if !output.status.success() {
            return Ok(None);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let state: RuncState =
            serde_json::from_str(&stdout).context("Failed to parse runc state")?;

        Ok(Some(state))
    }

    fn oci_cgroups_path(cgroup_parent: Option<&str>, container_id: &str) -> Option<String> {
        let parent = cgroup_parent
            .map(str::trim)
            .filter(|parent| !parent.is_empty())?;
        if parent.ends_with(".slice") {
            let systemd_slice = Path::new(parent)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(parent);
            Some(format!("{systemd_slice}:crius:{container_id}"))
        } else {
            Some(parent.to_string())
        }
    }

    /// 获取容器 init 进程 PID
    pub fn container_pid(&self, container_id: &str) -> Result<Option<i32>> {
        match self.get_runc_state(container_id)? {
            Some(state) if state.pid > 0 => Ok(Some(state.pid)),
            _ => Ok(None),
        }
    }

    /// 将 CRI LinuxContainerResources 转换为 ResourceLimits
    fn cri_to_limits(resources: &LinuxContainerResources) -> ResourceLimits {
        ResourceLimits {
            cpu: Some(CpuLimit {
                shares: (resources.cpu_shares > 0).then_some(resources.cpu_shares as u64),
                quota: (resources.cpu_quota > 0).then_some(resources.cpu_quota),
                period: (resources.cpu_period > 0).then_some(resources.cpu_period as u64),
                realtime_runtime: None,
                realtime_period: None,
                cpus: (!resources.cpuset_cpus.is_empty()).then(|| resources.cpuset_cpus.clone()),
                mems: (!resources.cpuset_mems.is_empty()).then(|| resources.cpuset_mems.clone()),
            }),
            memory: Some(MemoryLimit {
                limit: (resources.memory_limit_in_bytes > 0)
                    .then_some(resources.memory_limit_in_bytes),
                reservation: None,
                swap: (resources.memory_swap_limit_in_bytes > 0)
                    .then_some(resources.memory_swap_limit_in_bytes),
                kernel: None,
                kernel_tcp: None,
                swappiness: None,
                disable_oom_killer: None,
                use_hierarchy: None,
            }),
            blkio: None,
            network: None,
            pids: None,
        }
    }

    fn notify_shim_to_reopen_log(&self, container_id: &str) -> Result<()> {
        let shim_manager = self
            .shim_manager
            .as_ref()
            .context("container log reopen requires shim-enabled runtime")?;
        let socket_path = shim_manager.shim_socket_path(container_id, "reopen.sock");
        if !socket_path.exists() {
            return Err(LogReopenError::MissingSocket {
                container_id: container_id.to_string(),
                socket_path,
            }
            .into());
        }
        let mut stream = StdUnixStream::connect(&socket_path).with_context(|| {
            format!(
                "Failed to connect reopen log socket {}",
                socket_path.display()
            )
        })?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .with_context(|| {
                format!(
                    "Failed to configure reopen log socket timeout {}",
                    socket_path.display()
                )
            })?;
        let mut response = String::new();
        stream.read_to_string(&mut response).with_context(|| {
            format!(
                "Failed to read reopen log response from {}",
                socket_path.display()
            )
        })?;

        let response = response.trim();
        if response == "OK" {
            Ok(())
        } else if response.is_empty() {
            Err(anyhow::anyhow!(
                "reopen log socket {} closed without acknowledgement",
                socket_path.display()
            ))
        } else if let Some(reason) = response.strip_prefix("ERR ") {
            Err(anyhow::anyhow!(
                "shim failed to reopen log file for {}: {}",
                container_id,
                reason
            ))
        } else {
            Err(anyhow::anyhow!(
                "unexpected reopen log response from {}: {}",
                socket_path.display(),
                response
            ))
        }
    }

    fn rootfs_cleanup_target(&self, container_id: &str) -> Option<PathBuf> {
        self.rootfs_cleanup_target_from_bundle(container_id)
            .or_else(|| self.rootfs_cleanup_target_from_runtime_state(container_id))
    }

    fn rootfs_cleanup_target_from_bundle(&self, container_id: &str) -> Option<PathBuf> {
        let spec = Spec::load(self.config_path(container_id)).ok()?;
        let root = spec.root?;
        self.rootfs_cleanup_target_from_path(container_id, Path::new(&root.path))
    }

    fn rootfs_cleanup_target_from_runtime_state(&self, container_id: &str) -> Option<PathBuf> {
        let state = self.get_runc_state(container_id).ok()??;
        self.rootfs_cleanup_target_from_path(container_id, Path::new(&state.rootfs))
    }

    fn rootfs_cleanup_target_from_path(
        &self,
        container_id: &str,
        rootfs_path: &Path,
    ) -> Option<PathBuf> {
        let resolved_rootfs = if rootfs_path.is_absolute() {
            rootfs_path.to_path_buf()
        } else {
            self.bundle_path(container_id).join(rootfs_path)
        };
        let parent = resolved_rootfs.parent()?;
        if resolved_rootfs.file_name() == Some(std::ffi::OsStr::new("rootfs"))
            && parent.file_name() == Some(std::ffi::OsStr::new(container_id))
        {
            Some(parent.to_path_buf())
        } else {
            Some(resolved_rootfs)
        }
    }
}

impl ContainerRuntime for RuncRuntime {
    fn create_container(&self, container_id: &str, config: &ContainerConfig) -> Result<String> {
        info!("Creating container {}", container_id);
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(self.container_create_timeout_secs as u64);

        // 分步 create 链路：prepare_rootfs -> build_spec -> write_bundle。
        self.prepare_rootfs(container_id, config)?;
        self.ensure_container_create_not_expired(deadline, "prepare_rootfs")?;
        let spec = self.build_spec(container_id, config)?;
        self.ensure_container_create_not_expired(deadline, "build_spec")?;
        self.write_bundle(container_id, &config.rootfs, &spec)?;
        self.ensure_container_create_not_expired(deadline, "write_bundle")?;

        // 延迟到start阶段再调用runc，避免create阶段阻塞导致CRI超时。
        info!("Container {} bundle prepared successfully", container_id);
        Ok(container_id.to_string())
    }

    fn start_container(&self, container_id: &str) -> Result<()> {
        info!("Starting container {}", container_id);

        let state = self.get_runc_state(container_id)?;

        // 如果启用了shim，使用shim启动
        if let Some(ref shim_manager) = self.shim_manager {
            let bundle_path = self.bundle_path(container_id);
            let _process = shim_manager.start_shim(container_id, &bundle_path)?;
            info!(
                "Container {} started via shim (PID: {})",
                container_id, _process.shim_pid
            );
        } else {
            match state {
                None => {
                    // runc run -d is used when this container has not been created in runc yet.
                    let bundle_path = self.bundle_path(container_id);
                    let bundle_path = bundle_path.to_string_lossy().to_string();
                    let mut run_args = vec!["run", "-d", "--bundle", bundle_path.as_str()];
                    if self.no_pivot {
                        run_args.push("--no-pivot");
                    }
                    if self.no_new_keyring {
                        run_args.push("--no-new-keyring");
                    }
                    run_args.push(container_id);
                    self.runc_exec(&run_args)?;
                    info!("Container {} started via runc run -d", container_id);
                }
                Some(s) if s.status == "created" => {
                    self.runc_exec(&["start", container_id])?;
                    info!("Container {} started via runc start", container_id);
                }
                Some(s) if s.status == "running" => {
                    info!("Container {} already running", container_id);
                }
                Some(_) => {
                    self.runc_exec(&["start", container_id])?;
                    info!("Container {} started via runc start", container_id);
                }
            }
        }

        Ok(())
    }

    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> Result<()> {
        info!("Stopping container {}", container_id);

        // 获取当前状态
        let state = self.get_runc_state(container_id)?;

        match state {
            None => {
                self.wait_for_shim_exit_code(container_id);
                info!("Container {} not found, already stopped", container_id);
                return Ok(());
            }
            Some(s) => {
                if s.status == "stopped" {
                    self.wait_for_shim_exit_code(container_id);
                    info!("Container {} already stopped", container_id);
                    return Ok(());
                }
                if s.status == "paused" {
                    self.resume_container(container_id)?;
                }
            }
        }

        let timeout_secs = timeout.unwrap_or(self.container_stop_timeout_secs);
        self.runc_exec(&["kill", container_id, "TERM"])?;

        let graceful_deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs as u64);
        if self.wait_for_container_stop_until(container_id, graceful_deadline)? {
            self.wait_for_shim_exit_code(container_id);
            info!("Container {} stopped gracefully", container_id);
            return Ok(());
        }

        info!(
            "Container {} did not stop gracefully, entering SIGKILL retry loop",
            container_id
        );
        let kill_deadline = std::time::Instant::now() + STOP_KILL_WAIT_TIMEOUT;
        let mut backoff = STOP_INITIAL_BACKOFF;
        loop {
            match self.get_runc_state(container_id)? {
                None => break,
                Some(state) if state.status == "stopped" => break,
                _ => {}
            }

            if std::time::Instant::now() >= kill_deadline {
                return Err(anyhow::anyhow!(
                    "container {} did not stop after SIGKILL retries",
                    container_id
                ));
            }

            if let Err(err) = self.runc_exec(&["kill", container_id, "KILL"]) {
                match self.get_runc_state(container_id)? {
                    None => break,
                    Some(state) if state.status == "stopped" => break,
                    _ => debug!(
                        "SIGKILL retry for container {} failed but container is still present: {}",
                        container_id, err
                    ),
                }
            }

            let remaining = kill_deadline
                .checked_duration_since(std::time::Instant::now())
                .unwrap_or_default();
            std::thread::sleep(std::cmp::min(backoff, remaining));
            backoff = std::cmp::min(backoff.saturating_mul(2), STOP_MAX_BACKOFF);
        }

        self.wait_for_shim_exit_code(container_id);
        info!("Container {} stopped", container_id);
        Ok(())
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        info!("Removing container {}", container_id);
        let rootfs_cleanup_target = self.rootfs_cleanup_target(container_id);

        // 首先停止容器（如果还在运行）
        let _ = self.stop_container(container_id, None);

        // 删除容器
        let output = self.run_command_output(&["delete", container_id])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // 检查是否已经是"container does not exist"错误
            if stderr.contains("does not exist") {
                info!("Container {} does not exist", container_id);
            } else {
                return Err(anyhow::anyhow!("Failed to delete container: {}", stderr));
            }
        }

        // 清理bundle目录
        let bundle_path = self.bundle_path(container_id);
        if bundle_path.exists() {
            std::fs::remove_dir_all(&bundle_path).context("Failed to remove bundle directory")?;
        }
        let image_volume_state_dir = self.image_volume_state_dir(container_id);
        if image_volume_state_dir.exists() {
            std::fs::remove_dir_all(&image_volume_state_dir)
                .context("Failed to remove image volume state directory")?;
        }
        if let Some(container_root_dir) = rootfs_cleanup_target {
            if container_root_dir.exists() {
                std::fs::remove_dir_all(&container_root_dir).with_context(|| {
                    format!(
                        "Failed to remove container root directory {}",
                        container_root_dir.display()
                    )
                })?;
            }
        }
        let legacy_container_root_dir = self.root.join("containers").join(container_id);
        if legacy_container_root_dir.exists() {
            std::fs::remove_dir_all(&legacy_container_root_dir).with_context(|| {
                format!(
                    "Failed to remove legacy container root directory {}",
                    legacy_container_root_dir.display()
                )
            })?;
        }

        info!("Container {} removed", container_id);
        Ok(())
    }

    fn container_status(&self, container_id: &str) -> Result<ContainerStatus> {
        // 如果启用了shim，检查shim是否还在运行
        if let Some(ref shim_manager) = self.shim_manager {
            // 检查是否有退出码
            if let Ok(Some(exit_code)) = shim_manager.get_exit_code(container_id) {
                return Ok(ContainerStatus::Stopped(exit_code));
            }

            // 检查shim是否还在运行
            if shim_manager.is_shim_running(container_id) {
                return Ok(ContainerStatus::Running);
            }
        }

        // 回退到runc状态查询
        match self.get_runc_state(container_id)? {
            None => Ok(ContainerStatus::Unknown),
            Some(state) => {
                let status = match state.status.as_str() {
                    "created" => ContainerStatus::Created,
                    "running" => ContainerStatus::Running,
                    "stopped" => ContainerStatus::Stopped(0),
                    _ => ContainerStatus::Unknown,
                };
                Ok(status)
            }
        }
    }

    fn reopen_container_log(&self, container_id: &str) -> Result<()> {
        self.notify_shim_to_reopen_log(container_id)
    }

    fn exec_in_container(&self, container_id: &str, command: &[String], tty: bool) -> Result<i32> {
        info!(
            "Executing command in container {}: {:?}",
            container_id, command
        );

        let mut cmd = self.runtime_command();
        cmd.arg("exec");

        if tty {
            cmd.arg("-t");
        }
        cmd.arg("-i"); // 始终启用stdin交互

        // 添加容器ID
        cmd.arg(container_id);

        // 添加命令
        for arg in command {
            cmd.arg(arg);
        }

        let affinity_cpu = if self.exec_cpu_affinity == "first" {
            self.load_bundle_config_value(container_id)
                .ok()
                .and_then(|config| {
                    config
                        .get("linux")
                        .and_then(|linux| linux.get("resources"))
                        .and_then(|resources| resources.get("cpu"))
                        .and_then(|cpu| cpu.get("cpus"))
                        .and_then(|cpus| cpus.as_str())
                        .and_then(Self::first_cpu_from_cpuset)
                })
        } else {
            None
        };
        Self::apply_exec_cpu_affinity_to_std_command(&mut cmd, affinity_cpu);

        // 执行命令并等待结果
        let output = cmd.output().context("Failed to execute runc exec")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("exec failed: {}", stderr));
        }

        // 返回退出码
        let exit_code = output.status.code().unwrap_or(0);
        info!(
            "Command executed in container {} with exit code {}",
            container_id, exit_code
        );
        Ok(exit_code)
    }

    fn update_container_resources(
        &self,
        container_id: &str,
        resources: &LinuxContainerResources,
    ) -> Result<()> {
        if self.disable_cgroup {
            return Err(anyhow::anyhow!(
                "cgroup support is disabled; container resource updates are unavailable"
            ));
        }
        info!(
            "Updating container {} resources: CPU shares={}, Memory limit={}",
            container_id, resources.cpu_shares, resources.memory_limit_in_bytes
        );

        // 将 CRI LinuxContainerResources 转换为 ResourceLimits
        let limits = Self::cri_to_limits(resources);

        // 使用 CgroupManager 直接更新 cgroup 资源
        let cgroup_manager = CgroupManager::new(container_id.to_string())
            .context("Failed to create cgroup manager")?;
        cgroup_manager
            .set_resources(&limits)
            .context("Failed to set cgroup resources")?;

        info!("Container {} resources updated successfully", container_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    fn create_test_runtime() -> (RuncRuntime, tempfile::TempDir) {
        create_test_runtime_with_restrict(false)
    }

    fn create_test_runtime_with_restrict(restrict: bool) -> (RuncRuntime, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let mut runtime =
            RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("containers"));
        runtime.set_restrict_oom_score_adj(restrict);
        (runtime, temp_dir)
    }

    fn write_arg_capture_runtime_script(dir: &Path, args_path: &Path) -> PathBuf {
        let script_path = dir.join("fake-runtime-args.sh");
        fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
printf '%s\n' "$@" > "{}"
case "$cmd" in
  run|restore)
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
                args_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
        script_path
    }

    fn write_checkpoint_restore_arg_capture_runtime_script(
        dir: &Path,
        checkpoint_args_path: &Path,
        restore_args_path: &Path,
    ) -> PathBuf {
        let script_path = dir.join("fake-runtime-checkpoint-restore-args.sh");
        fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
case "$cmd" in
  checkpoint)
    printf '%s\n' "$@" > "{checkpoint_args}"
    exit 0
    ;;
  restore)
    printf '%s\n' "$@" > "{restore_args}"
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
                checkpoint_args = checkpoint_args_path.display(),
                restore_args = restore_args_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
        script_path
    }

    fn write_runtime_features_script(dir: &Path, stdout_payload: &str, exit_code: i32) -> PathBuf {
        let script_path = dir.join("fake-runtime-features.sh");
        fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
set -eu
if [ "${{1:-}}" = "features" ]; then
  cat <<'EOF'
{stdout_payload}
EOF
  exit {exit_code}
fi
exit 1
"#,
                stdout_payload = stdout_payload,
                exit_code = exit_code
            ),
        )
        .unwrap();
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
        script_path
    }

    fn write_stop_runtime_script(
        dir: &Path,
        command_log_path: &Path,
        initial_state: &str,
    ) -> PathBuf {
        let script_path = dir.join("fake-runtime-stop.sh");
        let state_dir = dir.join("runtime-state");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("container-1.state"), initial_state).unwrap();
        fs::write(state_dir.join("container-1.pid"), "4242").unwrap();
        fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
set -eu
STATE_DIR="{state_dir}"
LOG_PATH="{log_path}"
cmd="${{1:-}}"
shift || true
case "$cmd" in
  state)
    id="${{1:-}}"
    file="$STATE_DIR/$id.state"
    if [ ! -f "$file" ]; then
      exit 1
    fi
    status="$(cat "$file")"
    printf '{{"ociVersion":"1.0.2","id":"%s","status":"%s","pid":4242,"bundle":"%s","rootfs":"%s","created":"2024-01-01T00:00:00Z","owner":"root"}}\n' "$id" "$status" "$STATE_DIR/bundle" "$STATE_DIR/rootfs"
    ;;
  resume)
    id="${{1:-}}"
    printf 'resume\n' >> "$LOG_PATH"
    echo running > "$STATE_DIR/$id.state"
    ;;
  kill)
    id="${{1:-}}"
    sig="${{2:-}}"
    printf 'kill:%s\n' "$sig" >> "$LOG_PATH"
    if [ "$sig" = "KILL" ]; then
      echo stopped > "$STATE_DIR/$id.state"
    fi
    ;;
  *)
    exit 1
    ;;
esac
"#,
                state_dir = state_dir.display(),
                log_path = command_log_path.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
        script_path
    }

    fn create_test_config() -> ContainerConfig {
        ContainerConfig {
            name: "test".to_string(),
            image: "test:latest".to_string(),
            command: vec!["echo".to_string(), "hello".to_string()],
            args: vec![],
            env: vec![],
            working_dir: None,
            mounts: vec![],
            labels: vec![],
            annotations: vec![],
            privileged: false,
            user: None,
            run_as_group: None,
            supplemental_groups: vec![],
            hostname: None,
            tty: false,
            stdin: false,
            stdin_once: false,
            log_path: None,
            readonly_rootfs: false,
            seccomp_notifier: None,
            pids_limit: None,
            no_new_privileges: None,
            apparmor_profile: None,
            selinux_label: None,
            seccomp_profile: None,
            capabilities: None,
            cgroup_parent: None,
            sysctls: HashMap::new(),
            namespace_options: None,
            namespace_paths: NamespacePaths::default(),
            linux_resources: None,
            devices: vec![],
            masked_paths: vec![],
            readonly_paths: vec![],
            rootfs: PathBuf::from("/tmp/rootfs"),
        }
    }

    fn write_test_image_metadata(
        runtime: &RuncRuntime,
        image_id: &str,
        repo_tag: &str,
        env: Vec<&str>,
        entrypoint: Vec<&str>,
        cmd: Vec<&str>,
        working_dir: Option<&str>,
    ) {
        let image_dir = runtime.image_storage_root.join("images").join(image_id);
        fs::create_dir_all(&image_dir).unwrap();
        let metadata = crate::image::ImageMeta {
            id: image_id.to_string(),
            repo_tags: vec![repo_tag.to_string()],
            config_env: env.into_iter().map(str::to_string).collect(),
            config_entrypoint: entrypoint.into_iter().map(str::to_string).collect(),
            config_cmd: cmd.into_iter().map(str::to_string).collect(),
            config_working_dir: working_dir.map(str::to_string),
            ..Default::default()
        };
        fs::write(
            image_dir.join("metadata.json"),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
    }

    fn test_mount_config(source: PathBuf, destination: &str) -> MountConfig {
        MountConfig {
            source,
            destination: PathBuf::from(destination),
            read_only: false,
            missing_source_policy: MissingMountSourcePolicy::Reject,
            selinux_relabel: false,
            propagation: MountPropagationMode::Private,
            recursive_read_only: false,
            uid_mappings: Vec::new(),
            gid_mappings: Vec::new(),
            requested_image: None,
            image_sub_path: None,
        }
    }

    #[test]
    fn test_create_spec() {
        let (runtime, _temp) = create_test_runtime();
        let config = create_test_config();

        let spec = runtime.create_spec(&config, "test-id").unwrap();

        assert_eq!(spec.oci_version, "1.0.2");
        assert!(spec.process.is_some());
        assert!(spec.root.is_some());
        assert!(spec.linux.is_some());
    }

    #[test]
    fn test_create_spec_merges_image_runtime_defaults() {
        let (runtime, _temp) = create_test_runtime();
        write_test_image_metadata(
            &runtime,
            "sha256:test-image",
            "test:latest",
            vec!["PATH=/usr/local/bin:/usr/bin", "FOO=image"],
            vec!["kube-apiserver"],
            vec!["--help"],
            Some("/workspace"),
        );

        let mut config = create_test_config();
        config.command = vec!["kube-apiserver".to_string()];
        config.args = vec!["--secure-port=6443".to_string()];
        config.env = vec![("FOO".to_string(), "override".to_string())];
        config.working_dir = None;

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let process = spec.process.expect("process config should exist");
        let env = process.env.expect("process env should exist");

        assert_eq!(
            process.args,
            vec![
                "kube-apiserver".to_string(),
                "--secure-port=6443".to_string()
            ]
        );
        assert!(env
            .iter()
            .any(|entry| entry == "PATH=/usr/local/bin:/usr/bin"));
        assert!(env.iter().any(|entry| entry == "FOO=override"));
        assert_eq!(process.cwd, "/workspace");
    }

    #[test]
    fn test_create_spec_encodes_labels_annotation_for_nri() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.labels = vec![("app".to_string(), "demo".to_string())];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let annotations = spec.annotations.unwrap();
        let labels: std::collections::HashMap<String, String> =
            serde_json::from_str(annotations.get(CRIO_LABELS_ANNOTATION).unwrap()).unwrap();

        assert_eq!(labels.get("app").map(String::as_str), Some("demo"));
    }

    #[test]
    fn test_create_spec_uses_configured_default_ulimits() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_default_ulimits(vec![Rlimit {
            rtype: "RLIMIT_NOFILE".to_string(),
            soft: 1024,
            hard: 2048,
        }]);

        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();
        let rlimits = spec.process.unwrap().rlimits.unwrap();

        assert_eq!(rlimits.len(), 1);
        assert_eq!(rlimits[0].rtype, "RLIMIT_NOFILE");
        assert_eq!(rlimits[0].soft, 1024);
        assert_eq!(rlimits[0].hard, 2048);
    }

    #[test]
    fn test_create_spec_adds_timezone_mount_and_env() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_timezone("UTC".to_string());

        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();
        let mounts = spec.mounts.unwrap();
        let process = spec.process.unwrap();

        assert!(mounts
            .iter()
            .any(|mount| mount.destination == "/etc/localtime"
                && mount.mount_type.as_deref() == Some("bind")));
        assert!(process
            .env
            .unwrap_or_default()
            .iter()
            .any(|entry| entry == "TZ=UTC"));
    }

    #[test]
    fn test_create_spec_applies_default_proc_protection_when_proc_mount_disabled() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_disable_proc_mount(true);

        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();
        let linux = spec.linux.unwrap();

        assert_eq!(linux.masked_paths, Some(Spec::default_masked_paths()));
        assert_eq!(linux.readonly_paths, Some(Spec::default_readonly_paths()));
    }

    #[test]
    fn test_create_spec_rejects_custom_proc_paths_when_proc_mount_disabled() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_disable_proc_mount(true);
        let mut config = create_test_config();
        config.masked_paths = vec!["/proc".to_string()];

        let err = runtime
            .create_spec(&config, "test-id")
            .expect_err("custom proc paths must fail when proc mount is disabled");
        assert!(err.to_string().contains("ProcMount support is disabled"));
    }

    #[test]
    fn test_create_spec_honors_base_runtime_spec() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_base_runtime_spec(Some(Spec {
            oci_version: "1.0.2".to_string(),
            process: Some(Process {
                terminal: Some(false),
                user: None,
                args: vec!["/bin/sh".to_string()],
                env: None,
                cwd: "/".to_string(),
                capabilities: None,
                rlimits: Some(vec![Rlimit {
                    rtype: "RLIMIT_NPROC".to_string(),
                    soft: 64,
                    hard: 128,
                }]),
                oom_score_adj: None,
                scheduler: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                io_priority: None,
            }),
            root: None,
            hostname: None,
            mounts: None,
            hooks: Some(crate::oci::spec::Hooks {
                prestart: Some(vec![crate::oci::spec::Hook {
                    path: "/bin/true".to_string(),
                    args: None,
                    env: None,
                    timeout: None,
                }]),
                create_runtime: None,
                create_container: None,
                start_container: None,
                poststart: None,
                poststop: None,
            }),
            linux: None,
            annotations: Some(HashMap::from([(
                "example.com/base".to_string(),
                "true".to_string(),
            )])),
        }));

        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();

        assert!(spec.hooks.is_some());
        assert_eq!(
            spec.annotations
                .as_ref()
                .and_then(|annotations| annotations.get("example.com/base"))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            spec.process
                .as_ref()
                .and_then(|process| process.rlimits.as_ref())
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn test_create_spec_merges_configured_hooks_dirs() {
        let (mut runtime, temp_dir) = create_test_runtime();
        let hooks_a = temp_dir.path().join("hooks-a");
        let hooks_b = temp_dir.path().join("hooks-b");
        fs::create_dir_all(&hooks_a).unwrap();
        fs::create_dir_all(&hooks_b).unwrap();
        fs::write(
            hooks_a.join("00-prestart.json"),
            serde_json::to_vec(&crate::oci::spec::Hooks {
                prestart: Some(vec![crate::oci::spec::Hook {
                    path: "/usr/bin/pre-a".to_string(),
                    args: None,
                    env: None,
                    timeout: None,
                }]),
                create_runtime: None,
                create_container: None,
                start_container: None,
                poststart: None,
                poststop: None,
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(
            hooks_b.join("01-poststop.json"),
            serde_json::to_vec(&crate::oci::spec::Spec {
                oci_version: "1.0.2".to_string(),
                process: None,
                root: None,
                hostname: None,
                mounts: None,
                hooks: Some(crate::oci::spec::Hooks {
                    prestart: None,
                    create_runtime: None,
                    create_container: None,
                    start_container: None,
                    poststart: None,
                    poststop: Some(vec![crate::oci::spec::Hook {
                        path: "/usr/bin/post-b".to_string(),
                        args: None,
                        env: None,
                        timeout: None,
                    }]),
                }),
                linux: None,
                annotations: None,
            })
            .unwrap(),
        )
        .unwrap();
        runtime.set_hooks_dirs(vec![hooks_a, hooks_b]);

        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();
        let hooks = spec
            .hooks
            .expect("configured hooks should be merged into spec");
        assert_eq!(hooks.prestart.unwrap()[0].path, "/usr/bin/pre-a");
        assert_eq!(hooks.poststop.unwrap()[0].path, "/usr/bin/post-b");
    }

    #[test]
    fn test_create_spec_leaves_inheritable_caps_empty_by_default() {
        let (runtime, _temp) = create_test_runtime();
        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();
        let inheritable = spec
            .process
            .unwrap()
            .capabilities
            .unwrap()
            .inheritable
            .unwrap();

        assert!(inheritable.is_empty());
    }

    #[test]
    fn test_create_spec_can_add_inheritable_caps() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_add_inheritable_capabilities(true);

        let spec = runtime
            .create_spec(&create_test_config(), "test-id")
            .unwrap();
        let inheritable = spec
            .process
            .unwrap()
            .capabilities
            .unwrap()
            .inheritable
            .unwrap();

        assert!(inheritable.iter().any(|cap| cap == "CAP_CHOWN"));
    }

    #[test]
    fn test_bundle_path() {
        let (runtime, _temp) = create_test_runtime();
        let path = runtime.bundle_path("test-container");
        assert!(path.to_string_lossy().contains("test-container"));
    }

    #[test]
    fn test_resolve_image_dir_uses_runtime_configured_storage_root() {
        let temp_dir = tempdir().unwrap();
        let storage_root = temp_dir
            .path()
            .join("storage")
            .join("images")
            .join("sha256:test-image");
        fs::create_dir_all(&storage_root).unwrap();
        fs::write(
            storage_root.join("metadata.json"),
            serde_json::json!({
                "id": "sha256:test-image",
                "repo_tags": ["busybox:latest"],
                "size": 123,
            })
            .to_string(),
        )
        .unwrap();

        let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("containers"));
        let resolved = runtime.resolve_image_dir("busybox:latest").unwrap();
        assert_eq!(resolved, storage_root);
    }

    #[test]
    fn test_spec_with_custom_mounts() {
        let (runtime, temp_dir) = create_test_runtime();
        let host_path = temp_dir.path().join("host-path");
        fs::write(&host_path, "data").unwrap();
        let mut config = create_test_config();
        config.mounts = vec![MountConfig {
            source: host_path,
            destination: PathBuf::from("/container/path"),
            read_only: true,
            missing_source_policy: MissingMountSourcePolicy::Reject,
            selinux_relabel: false,
            propagation: MountPropagationMode::Private,
            recursive_read_only: false,
            uid_mappings: Vec::new(),
            gid_mappings: Vec::new(),
            requested_image: None,
            image_sub_path: None,
        }];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let mounts = spec.mounts.unwrap();

        // Should have default mounts + 1 custom mount
        assert!(mounts.len() > 1);
        assert!(mounts.iter().any(|m| m.destination == "/container/path"));
    }

    #[test]
    fn test_spec_applies_bind_mount_prefix_to_custom_mount_sources() {
        let (mut runtime, temp_dir) = create_test_runtime();
        let prefix = temp_dir.path().join("host");
        let prefixed_source = prefix.join("var/lib/data");
        fs::create_dir_all(prefixed_source.parent().unwrap()).unwrap();
        fs::write(&prefixed_source, "data").unwrap();
        runtime.set_bind_mount_prefix(prefix);
        let mut config = create_test_config();
        config.mounts = vec![MountConfig {
            source: PathBuf::from("/var/lib/data"),
            destination: PathBuf::from("/container/path"),
            read_only: true,
            missing_source_policy: MissingMountSourcePolicy::Reject,
            selinux_relabel: false,
            propagation: MountPropagationMode::Private,
            recursive_read_only: false,
            uid_mappings: Vec::new(),
            gid_mappings: Vec::new(),
            requested_image: None,
            image_sub_path: None,
        }];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let mounts = spec.mounts.unwrap();

        assert!(mounts.iter().any(|mount| {
            mount.destination == "/container/path"
                && mount.source.as_deref() == Some(prefixed_source.to_string_lossy().as_ref())
        }));
    }

    #[test]
    fn test_spec_excludes_hugepages_mount_by_default() {
        let (runtime, _temp) = create_test_runtime();
        let config = create_test_config();

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let mounts = spec.mounts.unwrap();

        assert!(!mounts.iter().any(|m| m.destination == "/dev/hugepages"));
    }

    #[test]
    fn test_spec_with_user() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.user = Some("1000".to_string());

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let process = spec.process.unwrap();
        let user = process.user.unwrap();

        assert_eq!(user.uid, 1000);
        assert_eq!(user.gid, 1000);
    }

    #[test]
    fn test_spec_with_supplemental_groups_without_explicit_user() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.run_as_group = Some(3000);
        config.supplemental_groups = vec![2000, 2001];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let process = spec.process.unwrap();
        let user = process.user.unwrap();

        assert_eq!(user.uid, 0);
        assert_eq!(user.gid, 3000);
        assert_eq!(user.additional_gids, Some(vec![2000, 2001]));
    }

    #[test]
    fn test_spec_with_username() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.user = Some("nobody".to_string());

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let process = spec.process.unwrap();
        let user = process.user.unwrap();

        assert_eq!(user.username.as_deref(), Some("nobody"));
    }

    #[test]
    fn test_spec_privileged() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.privileged = true;

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();

        let devices = linux.devices.unwrap();
        assert!(devices.iter().any(|device| device.path == "/dev/null"));
        let device_rules = linux.resources.unwrap().devices.unwrap();
        assert_eq!(device_rules.len(), 1);
        assert!(device_rules[0].allow);
        assert_eq!(device_rules[0].access.as_deref(), Some("rwm"));
    }

    #[test]
    fn test_spec_privileged_without_host_devices_skips_host_devices() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_privileged_without_host_devices(true);
        let mut config = create_test_config();
        config.privileged = true;

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();

        assert!(linux.devices.unwrap().is_empty());
        assert!(linux.resources.unwrap().devices.unwrap().is_empty());
    }

    #[test]
    fn test_spec_privileged_without_host_devices_can_keep_allow_all_rule() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_privileged_without_host_devices(true);
        runtime.set_privileged_without_host_devices_all_devices_allowed(true);
        let mut config = create_test_config();
        config.privileged = true;

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();

        assert!(linux.devices.unwrap().is_empty());
        let device_rules = linux.resources.unwrap().devices.unwrap();
        assert_eq!(device_rules.len(), 1);
        assert!(device_rules[0].allow);
        assert_eq!(device_rules[0].access.as_deref(), Some("rwm"));
    }

    #[test]
    fn test_host_device_skip_rules_match_containerd_style_filters() {
        assert!(RuncRuntime::should_skip_host_device_dir("pts"));
        assert!(RuncRuntime::should_skip_host_device_dir("shm"));
        assert!(RuncRuntime::should_skip_host_device_dir("fd"));
        assert!(RuncRuntime::should_skip_host_device_dir("mqueue"));
        assert!(RuncRuntime::should_skip_host_device_dir(".udev"));
        assert!(!RuncRuntime::should_skip_host_device_dir("mapper"));

        assert!(RuncRuntime::should_skip_host_device_file("console"));
        assert!(!RuncRuntime::should_skip_host_device_file("null"));
    }

    #[test]
    fn test_privileged_spec_uses_full_capability_baseline() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_default_capabilities(vec!["CAP_CHOWN".to_string()]);

        let mut config = create_test_config();
        config.privileged = true;
        config.image.clear();

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let capabilities = spec
            .process
            .and_then(|process| process.capabilities)
            .expect("privileged container should include capabilities");
        let bounding = capabilities.bounding.unwrap_or_default();

        assert!(bounding.contains(&"CAP_CHOWN".to_string()));
        assert!(bounding.contains(&"CAP_NET_ADMIN".to_string()));
        assert!(bounding.contains(&"CAP_SYS_ADMIN".to_string()));
        assert!(!bounding.is_empty());
    }

    #[test]
    fn test_spec_with_runtime_options() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_default_env(vec![
            (
                "HTTP_PROXY".to_string(),
                "http://proxy.internal".to_string(),
            ),
            ("LANG".to_string(), "C".to_string()),
        ]);
        runtime.set_default_capabilities(vec!["NET_BIND_SERVICE".to_string(), "CHOWN".to_string()]);
        runtime.set_default_sysctls(HashMap::from([(
            "kernel.shm_rmid_forced".to_string(),
            "1".to_string(),
        )]));
        let mut config = create_test_config();
        config.tty = true;
        config.readonly_rootfs = true;
        config.cgroup_parent = Some("kubepods.slice/pod123".to_string());
        config.env = vec![
            ("LANG".to_string(), "C.UTF-8".to_string()),
            ("FOO".to_string(), "bar".to_string()),
        ];
        config
            .sysctls
            .insert("net.ipv4.ip_forward".to_string(), "1".to_string());
        config.namespace_paths.network = Some(PathBuf::from("/var/run/netns/test-pod"));
        config.linux_resources = Some(LinuxContainerResources {
            cpu_period: 100000,
            cpu_quota: 200000,
            cpu_shares: 2048,
            memory_limit_in_bytes: 536870912,
            oom_score_adj: 0,
            cpuset_cpus: "0-1".to_string(),
            cpuset_mems: "0".to_string(),
            hugepage_limits: vec![],
            unified: HashMap::new(),
            memory_swap_limit_in_bytes: 1073741824,
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let process = spec.process.unwrap();
        let root = spec.root.unwrap();
        let linux = spec.linux.unwrap();

        assert_eq!(process.terminal, Some(true));
        assert_eq!(root.readonly, Some(true));
        assert!(process
            .env
            .as_ref()
            .unwrap()
            .iter()
            .any(|entry| entry == "HTTP_PROXY=http://proxy.internal"));
        assert!(process
            .env
            .as_ref()
            .unwrap()
            .iter()
            .any(|entry| entry == "LANG=C.UTF-8"));
        assert!(process
            .env
            .as_ref()
            .unwrap()
            .iter()
            .any(|entry| entry == "FOO=bar"));
        assert_eq!(
            process
                .capabilities
                .as_ref()
                .and_then(|caps| caps.bounding.as_ref())
                .cloned()
                .unwrap_or_default(),
            vec!["CAP_NET_BIND_SERVICE".to_string(), "CAP_CHOWN".to_string()]
        );
        assert_eq!(linux.cgroups_path.as_deref(), Some("kubepods.slice/pod123"));
        assert_eq!(
            linux
                .sysctl
                .as_ref()
                .and_then(|sysctls| sysctls.get("kernel.shm_rmid_forced"))
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            linux
                .sysctl
                .as_ref()
                .and_then(|sysctls| sysctls.get("net.ipv4.ip_forward"))
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            linux
                .namespaces
                .as_ref()
                .and_then(|namespaces| namespaces.iter().find(|ns| ns.ns_type == "network"))
                .and_then(|namespace| namespace.path.as_deref()),
            Some("/var/run/netns/test-pod")
        );
        assert_eq!(
            linux
                .resources
                .as_ref()
                .and_then(|resources| resources.cpu.as_ref())
                .and_then(|cpu| cpu.quota),
            Some(200000)
        );
        assert_eq!(
            linux
                .resources
                .as_ref()
                .and_then(|resources| resources.memory.as_ref())
                .and_then(|memory| memory.limit),
            Some(536870912)
        );
    }

    #[test]
    fn test_spec_omits_cgroup_configuration_when_disabled() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_disable_cgroup(true);
        let mut config = create_test_config();
        config.cgroup_parent = Some("kubepods.slice/pod123".to_string());
        config.pids_limit = Some(256);
        config.linux_resources = Some(LinuxContainerResources {
            cpu_shares: 2048,
            memory_limit_in_bytes: 536870912,
            ..Default::default()
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();

        assert!(linux.cgroups_path.is_none());
        assert!(linux.resources.is_none());
    }

    #[test]
    fn test_oci_cgroups_path_keeps_non_systemd_parent_unchanged() {
        assert_eq!(
            RuncRuntime::oci_cgroups_path(Some("kubepods/pod123"), "container-1").as_deref(),
            Some("kubepods/pod123")
        );
    }

    #[test]
    fn test_oci_cgroups_path_derives_systemd_scope_from_slice_parent() {
        assert_eq!(
            RuncRuntime::oci_cgroups_path(
                Some("/kubepods.slice/kubepods-burstable.slice/pod123.slice"),
                "container-1",
            )
            .as_deref(),
            Some("pod123.slice:crius:container-1")
        );
    }

    #[test]
    fn test_oci_cgroups_path_uses_systemd_slice_basename() {
        assert_eq!(
            RuncRuntime::oci_cgroups_path(
                Some("/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podabc.slice",),
                "container-1",
            )
            .as_deref(),
            Some("kubepods-burstable-podabc.slice:crius:container-1")
        );
    }

    #[test]
    fn test_create_spec_sets_process_oom_score_adj_from_linux_resources() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.linux_resources = Some(LinuxContainerResources {
            oom_score_adj: 321,
            ..Default::default()
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();

        assert_eq!(
            spec.process
                .as_ref()
                .and_then(|process| process.oom_score_adj),
            Some(321)
        );
    }

    #[test]
    fn test_create_spec_restricts_oom_score_adj_to_daemon_floor() {
        let current = RuncRuntime::daemon_oom_score_adj().unwrap();
        let (runtime, _temp) = create_test_runtime_with_restrict(true);
        let mut config = create_test_config();
        config.linux_resources = Some(LinuxContainerResources {
            oom_score_adj: current - 1,
            ..Default::default()
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();

        assert_eq!(
            spec.process
                .as_ref()
                .and_then(|process| process.oom_score_adj),
            Some(current as i32)
        );
    }

    #[test]
    fn test_enforce_oom_score_adj_policy_clamps_existing_spec_after_adjustment() {
        let current = RuncRuntime::daemon_oom_score_adj().unwrap();
        let (runtime, _temp) = create_test_runtime_with_restrict(true);
        let mut spec = Spec::new("1.0.2");
        spec.process = Some(Process {
            terminal: Some(false),
            user: None,
            args: vec!["sleep".to_string(), "1".to_string()],
            env: None,
            cwd: "/".to_string(),
            capabilities: None,
            rlimits: None,
            oom_score_adj: Some((current - 1) as i32),
            scheduler: None,
            no_new_privileges: None,
            apparmor_profile: None,
            selinux_label: None,
            io_priority: None,
        });

        runtime.enforce_oom_score_adj_policy(&mut spec).unwrap();

        assert_eq!(
            spec.process
                .as_ref()
                .and_then(|process| process.oom_score_adj),
            Some(current as i32)
        );
    }

    #[test]
    fn test_readonly_rootfs_keeps_default_tmpfs_and_custom_rw_mounts() {
        let (runtime, temp_dir) = create_test_runtime();
        let host_path = temp_dir.path().join("host-rw");
        fs::write(&host_path, "data").unwrap();
        let mut config = create_test_config();
        config.readonly_rootfs = true;
        config.mounts = vec![MountConfig {
            source: host_path,
            destination: PathBuf::from("/var/lib/app"),
            read_only: false,
            missing_source_policy: MissingMountSourcePolicy::Reject,
            selinux_relabel: false,
            propagation: MountPropagationMode::Private,
            recursive_read_only: false,
            uid_mappings: Vec::new(),
            gid_mappings: Vec::new(),
            requested_image: None,
            image_sub_path: None,
        }];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let root = spec.root.unwrap();
        let mounts = spec.mounts.unwrap();

        assert_eq!(root.readonly, Some(true));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/proc" && mount.mount_type.as_deref() == Some("proc")
        }));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/dev"
                && mount.mount_type.as_deref() == Some("tmpfs")
                && mount
                    .options
                    .as_ref()
                    .map(|options| options.iter().any(|option| option == "mode=755"))
                    .unwrap_or(false)
        }));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/dev/shm"
                && mount.mount_type.as_deref() == Some("tmpfs")
                && mount
                    .options
                    .as_ref()
                    .map(|options| options.iter().any(|option| option == "mode=1777"))
                    .unwrap_or(false)
        }));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/var/lib/app"
                && mount.mount_type.as_deref() == Some("bind")
                && mount
                    .options
                    .as_ref()
                    .map(|options| options.iter().any(|option| option == "rw"))
                    .unwrap_or(false)
        }));
    }

    #[test]
    fn test_spec_uses_host_pid_namespace_when_requested() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.namespace_options = Some(NamespaceOption {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Node as i32,
            ipc: NamespaceMode::Pod as i32,
            target_id: String::new(),
            userns_options: None,
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();
        let pid_namespace = linux
            .namespaces
            .unwrap()
            .into_iter()
            .find(|namespace| namespace.ns_type == "pid")
            .unwrap();

        assert_eq!(pid_namespace.path.as_deref(), Some("/proc/1/ns/pid"));
    }

    #[test]
    fn test_spec_uses_host_ipc_namespace_and_host_ipc_mounts_when_requested() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.namespace_options = Some(NamespaceOption {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Node as i32,
            target_id: String::new(),
            userns_options: None,
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.clone().unwrap();
        let ipc_namespace = linux
            .namespaces
            .unwrap()
            .into_iter()
            .find(|namespace| namespace.ns_type == "ipc")
            .unwrap();
        let mounts = spec.mounts.unwrap();

        assert_eq!(ipc_namespace.path.as_deref(), Some("/proc/1/ns/ipc"));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/dev/shm"
                && mount.source.as_deref() == Some("/dev/shm")
                && mount.mount_type.as_deref() == Some("bind")
        }));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/dev/mqueue"
                && mount.source.as_deref() == Some("/dev/mqueue")
                && mount.mount_type.as_deref() == Some("bind")
        }));
    }

    #[test]
    fn test_spec_applies_bind_mount_prefix_to_host_ipc_mounts() {
        let (mut runtime, temp_dir) = create_test_runtime();
        let prefix = temp_dir.path().join("host");
        fs::create_dir_all(prefix.join("dev")).unwrap();
        fs::create_dir_all(prefix.join("dev/shm")).unwrap();
        fs::create_dir_all(prefix.join("dev/mqueue")).unwrap();
        runtime.set_bind_mount_prefix(prefix.clone());
        let mut config = create_test_config();
        config.namespace_options = Some(NamespaceOption {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Node as i32,
            target_id: String::new(),
            userns_options: None,
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let mounts = spec.mounts.unwrap();

        assert!(mounts.iter().any(|mount| {
            mount.destination == "/dev/shm"
                && mount.source.as_deref()
                    == Some(prefix.join("dev/shm").to_string_lossy().as_ref())
        }));
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/dev/mqueue"
                && mount.source.as_deref()
                    == Some(prefix.join("dev/mqueue").to_string_lossy().as_ref())
        }));
    }

    #[test]
    fn test_spec_adds_user_namespace_and_mappings_when_userns_pod_mode_requested() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.namespace_options = Some(NamespaceOption {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Pod as i32,
            target_id: String::new(),
            userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                mode: NamespaceMode::Pod as i32,
                uids: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 100000,
                    container_id: 0,
                    length: 65536,
                }],
                gids: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 200000,
                    container_id: 0,
                    length: 65536,
                }],
            }),
        });

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();

        assert!(linux
            .namespaces
            .as_ref()
            .unwrap()
            .iter()
            .any(|namespace| namespace.ns_type == "user"));
        assert_eq!(linux.uid_mappings.as_ref().unwrap()[0].host_id, 100000);
        assert_eq!(linux.gid_mappings.as_ref().unwrap()[0].host_id, 200000);
    }

    #[test]
    fn test_spec_rejects_userns_node_mode_with_mappings() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.namespace_options = Some(NamespaceOption {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Pod as i32,
            target_id: String::new(),
            userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                mode: NamespaceMode::Node as i32,
                uids: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 100000,
                    container_id: 0,
                    length: 65536,
                }],
                gids: vec![],
            }),
        });

        let err = runtime.create_spec(&config, "test-id").unwrap_err();
        assert!(err.to_string().contains("mode NODE"));
    }

    #[test]
    fn test_spec_with_device_mappings() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.devices = vec![DeviceMapping {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/dev/custom-null"),
            permissions: "rw".to_string(),
        }];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();
        let devices = linux.devices.unwrap();
        let cgroup_rules = linux.resources.unwrap().devices.unwrap();

        assert!(devices
            .iter()
            .any(|device| device.path == "/dev/custom-null"));
        assert!(cgroup_rules.iter().any(|rule| {
            rule.access.as_deref() == Some("rw") && rule.device_type.as_deref() == Some("c")
        }));
    }

    #[test]
    fn test_spec_injects_additional_devices() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_additional_devices(vec![DeviceMapping {
            source: PathBuf::from("/dev/zero"),
            destination: PathBuf::from("/dev/test-zero"),
            permissions: "r".to_string(),
        }]);
        let config = create_test_config();

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();
        let devices = linux.devices.unwrap();
        let rules = linux.resources.unwrap().devices.unwrap();

        assert!(devices.iter().any(|device| device.path == "/dev/test-zero"));
        assert!(rules.iter().any(|rule| {
            rule.device_type.as_deref() == Some("c")
                && rule.major == Some(1)
                && rule.minor == Some(5)
                && rule.access.as_deref() == Some("r")
        }));
    }

    #[test]
    fn test_spec_rejects_unlisted_requested_device() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_allowed_devices(vec![PathBuf::from("/dev/zero")]);
        let mut config = create_test_config();
        config.devices = vec![DeviceMapping {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/dev/null"),
            permissions: "rwm".to_string(),
        }];

        let err = runtime
            .create_spec(&config, "test-id")
            .expect_err("unlisted devices must be rejected");
        assert!(err
            .to_string()
            .contains("device /dev/null is not allowed by runtime.allowed_devices"));
    }

    #[test]
    fn test_spec_device_ownership_follows_security_context() {
        let (mut runtime, _temp) = create_test_runtime();
        runtime.set_device_ownership_from_security_context(true);
        let mut config = create_test_config();
        config.user = Some("1234".to_string());
        config.run_as_group = Some(2345);
        config.devices = vec![DeviceMapping {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/dev/custom-null"),
            permissions: "rwm".to_string(),
        }];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();
        let device = linux
            .devices
            .unwrap()
            .into_iter()
            .find(|device| device.path == "/dev/custom-null")
            .expect("device should be present");

        assert_eq!(device.uid, Some(1234));
        assert_eq!(device.gid, Some(2345));
    }

    #[test]
    fn test_spec_with_selinux_and_localhost_seccomp() {
        let (runtime, temp) = create_test_runtime();
        let mut config = create_test_config();
        config.selinux_label = Some("system_u:system_r:container_t:s0".to_string());

        let seccomp_profile_path = temp.path().join("seccomp.json");
        let seccomp_profile = crate::oci::spec::Seccomp {
            default_action: "SCMP_ACT_ALLOW".to_string(),
            default_errno_ret: None,
            architectures: None,
            flags: None,
            listener_path: None,
            listener_metadata: None,
            syscalls: None,
        };
        std::fs::write(
            &seccomp_profile_path,
            serde_json::to_vec(&seccomp_profile).unwrap(),
        )
        .unwrap();
        config.seccomp_profile = Some(SeccompProfile::Localhost(seccomp_profile_path));

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let process = spec.process.unwrap();
        let linux = spec.linux.unwrap();

        assert_eq!(
            process.selinux_label.as_deref(),
            Some("system_u:system_r:container_t:s0")
        );
        assert_eq!(
            linux.mount_label.as_deref(),
            Some("system_u:system_r:container_t:s0")
        );
        assert!(linux.seccomp.is_some());
    }

    #[test]
    fn test_spec_injects_seccomp_notifier_listener() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.seccomp_profile = Some(SeccompProfile::RuntimeDefault);
        config.seccomp_notifier = Some(SeccompNotifierConfig {
            listener_path: PathBuf::from("/run/crius/seccomp-notify/container-1"),
            listener_metadata: "container=container-1".to_string(),
            mode: SeccompNotifierMode::Stop,
        });

        let spec = runtime.create_spec(&config, "container-1").unwrap();
        let seccomp = spec
            .linux
            .and_then(|linux| linux.seccomp)
            .expect("seccomp config should be present");

        assert_eq!(
            seccomp.listener_path.as_deref(),
            Some("/run/crius/seccomp-notify/container-1")
        );
        assert_eq!(
            seccomp.listener_metadata.as_deref(),
            Some("container=container-1")
        );
        assert!(seccomp
            .syscalls
            .unwrap_or_default()
            .iter()
            .any(|syscall| syscall.action == "SCMP_ACT_NOTIFY"));
    }

    #[test]
    fn test_populate_rootfs_from_plain_tar_layer() {
        let (runtime, temp_dir) = create_test_runtime();
        let image_dir = temp_dir
            .path()
            .join("storage")
            .join("images")
            .join("sha256:plain-tar");
        std::fs::create_dir_all(&image_dir).unwrap();
        std::fs::write(
            image_dir.join("metadata.json"),
            serde_json::json!({
                "id": "sha256:plain-tar",
                "repo_tags": ["plain:latest"],
                "stored_layers": [{
                    "path": "0.tar",
                    "media_type": "application/vnd.oci.image.layer.v1.tar",
                    "source_media_type": "application/vnd.oci.image.layer.v1.tar",
                    "encrypted": false
                }]
            })
            .to_string(),
        )
        .unwrap();

        let content_dir = temp_dir.path().join("plain-layer");
        std::fs::create_dir_all(content_dir.join("etc")).unwrap();
        std::fs::write(content_dir.join("etc/hello"), "world").unwrap();
        let tar_path = image_dir.join("0.tar");
        let status = Command::new("tar")
            .arg("-cf")
            .arg(&tar_path)
            .arg("-C")
            .arg(&content_dir)
            .arg(".")
            .status()
            .unwrap();
        assert!(status.success());

        let rootfs = temp_dir.path().join("rootfs");
        runtime
            .populate_rootfs_from_image("plain:latest", &rootfs, "container-1")
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/hello")).unwrap(),
            "world"
        );
    }

    #[test]
    fn test_spec_with_runtime_default_seccomp_uses_builtin_profile() {
        let (runtime, _) = create_test_runtime();
        let mut config = create_test_config();
        config.seccomp_profile = Some(SeccompProfile::RuntimeDefault);

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();
        let seccomp = linux
            .seccomp
            .expect("runtime/default should produce seccomp");

        assert_eq!(seccomp.default_action, "SCMP_ACT_ALLOW");
        assert!(seccomp
            .syscalls
            .unwrap_or_default()
            .iter()
            .any(|syscall| syscall.names.iter().any(|name| name == "unshare")));
    }

    #[test]
    fn test_cri_to_limits() {
        let resources = LinuxContainerResources {
            cpu_period: 100000,
            cpu_quota: 200000,
            cpu_shares: 1024,
            memory_limit_in_bytes: 1073741824, // 1GB
            oom_score_adj: 0,
            cpuset_cpus: "0-3".to_string(),
            cpuset_mems: "0".to_string(),
            hugepage_limits: vec![],
            unified: HashMap::new(),
            memory_swap_limit_in_bytes: 2147483648, // 2GB
        };

        let limits = RuncRuntime::cri_to_limits(&resources);

        // 验证 CPU 限制
        let cpu = limits.cpu.as_ref().unwrap();
        assert_eq!(cpu.shares, Some(1024));
        assert_eq!(cpu.quota, Some(200000));
        assert_eq!(cpu.period, Some(100000));
        assert_eq!(cpu.cpus.as_deref(), Some("0-3"));
        assert_eq!(cpu.mems.as_deref(), Some("0"));

        // 验证内存限制
        let memory = limits.memory.as_ref().unwrap();
        assert_eq!(memory.limit, Some(1073741824));
        assert_eq!(memory.swap, Some(2147483648));
    }

    #[test]
    fn test_cri_to_limits_zero_values() {
        // 测试零值被过滤（不设置）
        let resources = LinuxContainerResources {
            cpu_period: 0,
            cpu_quota: 0,
            cpu_shares: 0,
            memory_limit_in_bytes: 0,
            oom_score_adj: 0,
            cpuset_cpus: "".to_string(),
            cpuset_mems: "".to_string(),
            hugepage_limits: vec![],
            unified: HashMap::new(),
            memory_swap_limit_in_bytes: 0,
        };

        let limits = RuncRuntime::cri_to_limits(&resources);

        // 零值应该被过滤为 None
        let cpu = limits.cpu.as_ref().unwrap();
        assert_eq!(cpu.shares, None);
        assert_eq!(cpu.quota, None);
        assert_eq!(cpu.period, None);
        assert_eq!(cpu.cpus, None);
        assert_eq!(cpu.mems, None);

        let memory = limits.memory.as_ref().unwrap();
        assert_eq!(memory.limit, None);
        assert_eq!(memory.swap, None);
    }

    #[test]
    fn test_create_spec_applies_container_pids_limit() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.pids_limit = Some(256);

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let linux = spec.linux.unwrap();
        let resources = linux.resources.unwrap();

        assert_eq!(resources.pids.as_ref().map(|pids| pids.limit), Some(256));
    }

    #[test]
    fn test_checkpoint_container_passes_configured_criu_path() {
        let temp_dir = tempdir().unwrap();
        let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
        let restore_args_path = temp_dir.path().join("restore.args");
        let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
            temp_dir.path(),
            &checkpoint_args_path,
            &restore_args_path,
        );
        let root = temp_dir.path().join("containers");
        let mut runtime = RuncRuntime::new(runtime_path, root);
        runtime.set_criu_path(PathBuf::from("/usr/sbin/criu"));

        let image_path = temp_dir.path().join("checkpoint");
        let work_path = temp_dir.path().join("checkpoint-work");
        runtime
            .checkpoint_container("container-1", &image_path, &work_path)
            .unwrap();

        let args = fs::read_to_string(&checkpoint_args_path).unwrap();
        assert!(args.lines().any(|line| line == "--image-path"));
        assert!(args
            .lines()
            .any(|line| line == image_path.to_string_lossy()));
        assert!(args.lines().any(|line| line == "--work-path"));
        assert!(args.lines().any(|line| line == work_path.to_string_lossy()));
        assert!(args.lines().any(|line| line == "--criu"));
        assert!(args.lines().any(|line| line == "/usr/sbin/criu"));
    }

    #[test]
    fn test_stop_container_resumes_paused_container_before_term() {
        let temp_dir = tempdir().unwrap();
        let command_log_path = temp_dir.path().join("stop.log");
        let runtime_path = write_stop_runtime_script(temp_dir.path(), &command_log_path, "paused");
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

        runtime.stop_container("container-1", Some(1)).unwrap();

        let log = fs::read_to_string(&command_log_path).unwrap();
        let lines = log.lines().collect::<Vec<_>>();
        assert_eq!(lines, vec!["resume", "kill:TERM", "kill:KILL"]);
    }

    #[test]
    fn test_stop_container_retries_with_sigkill_after_graceful_timeout() {
        let temp_dir = tempdir().unwrap();
        let command_log_path = temp_dir.path().join("stop.log");
        let runtime_path = write_stop_runtime_script(temp_dir.path(), &command_log_path, "running");
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

        runtime.stop_container("container-1", Some(1)).unwrap();

        let log = fs::read_to_string(&command_log_path).unwrap();
        let lines = log.lines().collect::<Vec<_>>();
        assert_eq!(lines.first().copied(), Some("kill:TERM"));
        assert!(lines.iter().any(|line| *line == "kill:KILL"));
        let final_state = fs::read_to_string(
            temp_dir
                .path()
                .join("runtime-state")
                .join("container-1.state"),
        )
        .unwrap();
        assert_eq!(final_state.trim(), "stopped");
    }

    #[test]
    fn test_exec_in_container_uses_first_cpu_affinity_when_configured() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = temp_dir.path().join("fake-runtime.sh");
        let affinity_path = temp_dir.path().join("affinity.txt");
        fs::write(
            &runtime_path,
            format!(
                r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
case "$cmd" in
  exec)
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -t|-i)
          shift
          ;;
        *)
          break
          ;;
      esac
    done
    shift || true
    awk '/Cpus_allowed_list/ {{print $2}}' /proc/self/status > "{}"
    ;;
  *)
    exit 1
    ;;
esac
"#,
                affinity_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

        let root = temp_dir.path().join("containers");
        let mut runtime = RuncRuntime::new(runtime_path, root.clone());
        runtime.set_exec_cpu_affinity("first".to_string());
        let bundle = root.join("container-1");
        fs::create_dir_all(&bundle).unwrap();
        fs::write(
            bundle.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "linux": {
                    "resources": {
                        "cpu": {
                            "cpus": "0-3"
                        }
                    }
                }
            })
            .to_string(),
        )
        .unwrap();

        let exit_code = runtime
            .exec_in_container("container-1", &["true".to_string()], false)
            .unwrap();
        assert_eq!(exit_code, 0);
        assert_eq!(fs::read_to_string(&affinity_path).unwrap().trim(), "0");
    }

    #[test]
    fn test_start_container_direct_run_uses_configured_no_pivot_policy() {
        let temp_dir = tempdir().unwrap();
        let args_path = temp_dir.path().join("run.args");
        let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
        let root = temp_dir.path().join("containers");
        let mut runtime = RuncRuntime::new(runtime_path, root.clone());
        runtime.set_no_pivot(true);
        fs::create_dir_all(root.join("container-1")).unwrap();

        runtime.start_container("container-1").unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--no-pivot"));
    }

    #[test]
    fn test_start_container_direct_run_omits_no_pivot_by_default() {
        let temp_dir = tempdir().unwrap();
        let args_path = temp_dir.path().join("run.args");
        let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
        let root = temp_dir.path().join("containers");
        let runtime = RuncRuntime::new(runtime_path, root.clone());
        fs::create_dir_all(root.join("container-1")).unwrap();

        runtime.start_container("container-1").unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(!args.lines().any(|line| line == "--no-pivot"));
    }

    #[test]
    fn test_start_container_direct_run_uses_configured_no_new_keyring_policy() {
        let temp_dir = tempdir().unwrap();
        let args_path = temp_dir.path().join("run.args");
        let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
        let root = temp_dir.path().join("containers");
        let mut runtime = RuncRuntime::new(runtime_path, root.clone());
        runtime.set_no_new_keyring(true);
        fs::create_dir_all(root.join("container-1")).unwrap();

        runtime.start_container("container-1").unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--no-new-keyring"));
    }

    #[test]
    fn test_start_container_direct_run_includes_runtime_config_path() {
        let temp_dir = tempdir().unwrap();
        let args_path = temp_dir.path().join("run.args");
        let runtime_path = temp_dir.path().join("fake-runtime-config.sh");
        fs::write(
            &runtime_path,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
if [ "${{1:-}}" = "--config" ]; then
  shift 2
fi
cmd="${{1:-}}"
shift || true
case "$cmd" in
  run|restore)
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
                args_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();
        let root = temp_dir.path().join("containers");
        let mut runtime = RuncRuntime::new(runtime_path, root.clone());
        runtime.set_runtime_config_path(PathBuf::from("/etc/kata/config.toml"));
        fs::create_dir_all(root.join("container-1")).unwrap();

        runtime.start_container("container-1").unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--config"));
        assert!(args.lines().any(|line| line == "/etc/kata/config.toml"));
    }

    #[test]
    fn test_remove_container_cleans_persistent_rootfs_directory() {
        let temp_dir = tempdir().unwrap();
        let runtime_root = temp_dir.path().join("runtime-root");
        let persistent_root = temp_dir.path().join("persistent-root");
        fs::create_dir_all(&runtime_root).unwrap();
        fs::create_dir_all(&persistent_root).unwrap();

        let args_path = temp_dir.path().join("runtime-args.txt");
        let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
        let runtime = RuncRuntime::new(runtime_path, runtime_root);

        let container_id = "container-1";
        let bundle_path = runtime.bundle_path(container_id);
        fs::create_dir_all(&bundle_path).unwrap();

        let container_root = persistent_root.join("containers").join(container_id);
        let rootfs = container_root.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();
        fs::write(rootfs.join("marker"), "data").unwrap();

        let spec = Spec {
            oci_version: "1.0.2".to_string(),
            process: None,
            root: Some(Root {
                path: rootfs.display().to_string(),
                readonly: Some(false),
            }),
            hostname: None,
            mounts: None,
            hooks: None,
            linux: None,
            annotations: None,
        };
        runtime
            .save_spec_for_bundle(&spec, runtime.config_path(container_id))
            .unwrap();

        runtime.remove_container(container_id).unwrap();

        assert!(!container_root.exists());
        assert!(!bundle_path.exists());
    }

    #[test]
    fn test_probe_runtime_features_reports_idmap_and_rro_support() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = write_runtime_features_script(
            temp_dir.path(),
            r#"{
  "ociVersionMin": "1.0.0",
  "ociVersionMax": "1.2.0",
  "mountOptions": ["ro", "rro"],
  "linux": {
    "mountExtensions": {
      "idmap": {
        "enabled": true
      }
    }
  }
}"#,
            0,
        );
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

        let features = runtime.probe_runtime_features();
        assert!(features.available);
        assert!(features.idmap_mounts);
        assert!(features.recursive_read_only_mounts);
        assert_eq!(features.mount_options, vec!["ro", "rro"]);
        assert_eq!(features.oci_version_min.as_deref(), Some("1.0.0"));
        assert_eq!(features.oci_version_max.as_deref(), Some("1.2.0"));
        assert!(features.error.is_none());
    }

    #[test]
    fn test_probe_runtime_features_reports_invalid_document() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = write_runtime_features_script(temp_dir.path(), "{}", 0);
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

        let features = runtime.probe_runtime_features();
        assert!(!features.available);
        assert!(features.error.is_some());
    }

    #[test]
    fn test_validate_mount_requests_rejects_recursive_read_only_without_runtime_support() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = write_runtime_features_script(temp_dir.path(), "{}", 0);
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
        let source = temp_dir.path().join("source");
        fs::create_dir_all(&source).unwrap();

        let mut config = create_test_config();
        let mut mount = test_mount_config(source, "/data");
        mount.read_only = true;
        mount.recursive_read_only = true;
        config.mounts = vec![mount];

        let err = runtime.validate_mount_requests(&config).unwrap_err();
        assert!(matches!(
            err,
            MountSemanticsError::RecursiveReadOnlyUnsupported { .. }
        ));
    }

    #[test]
    fn test_validate_mount_requests_rejects_idmapped_mount_without_runtime_support() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = write_runtime_features_script(temp_dir.path(), "{}", 0);
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
        let source = temp_dir.path().join("source");
        fs::create_dir_all(&source).unwrap();

        let mut config = create_test_config();
        let mut mount = test_mount_config(source, "/data");
        mount.uid_mappings = vec![crate::oci::spec::IdMapping {
            container_id: 0,
            host_id: 1000,
            size: 1,
        }];
        config.mounts = vec![mount];

        let err = runtime.validate_mount_requests(&config).unwrap_err();
        assert!(matches!(
            err,
            MountSemanticsError::IdmapMountUnsupported { .. }
        ));
    }

    #[test]
    fn test_validate_mount_propagation_from_mountinfo_rejects_private_source_for_bidirectional() {
        let temp_dir = tempdir().unwrap();
        let source = temp_dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let mountinfo = format!("1 0 0:1 / {} rw - ext4 /dev/root rw", source.display());

        let err = RuncRuntime::validate_mount_propagation_from_mountinfo(
            &source,
            Path::new("/data"),
            MountPropagationMode::Bidirectional,
            &mountinfo,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            MountSemanticsError::BidirectionalPropagationRequiresShared { .. }
        ));
    }

    #[test]
    fn test_validate_mount_propagation_from_mountinfo_accepts_slave_source_for_host_to_container() {
        let temp_dir = tempdir().unwrap();
        let source = temp_dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let mountinfo = format!(
            "1 0 0:1 / {} rw master:1 - ext4 /dev/root rw",
            source.display()
        );

        RuncRuntime::validate_mount_propagation_from_mountinfo(
            &source,
            Path::new("/data"),
            MountPropagationMode::HostToContainer,
            &mountinfo,
        )
        .unwrap();
    }

    #[test]
    fn test_build_mounts_includes_default_mounts_file_entries() {
        let (mut runtime, temp_dir) = create_test_runtime();
        let mounts_path = temp_dir.path().join("mounts.conf");
        let source_dir = temp_dir.path().join("secrets");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            &mounts_path,
            format!("{}:/run/secrets\n", source_dir.display()),
        )
        .unwrap();
        runtime.set_default_mounts_file(mounts_path);

        let mut config = create_test_config();
        config.rootfs = temp_dir.path().join("rootfs");

        let mounts = runtime.build_mounts("test-id", &config, false).unwrap();
        assert!(mounts.iter().any(|mount| {
            mount.destination == "/run/secrets"
                && mount.source.as_deref() == Some(source_dir.to_string_lossy().as_ref())
        }));
    }

    #[test]
    fn test_build_mounts_rejects_configured_absent_mount_sources() {
        let (mut runtime, temp_dir) = create_test_runtime();
        let missing_source = temp_dir.path().join("missing-hostname");
        runtime.set_absent_mount_sources_to_reject(vec![missing_source.clone()]);

        let mut config = create_test_config();
        config.rootfs = temp_dir.path().join("rootfs");
        config.mounts = vec![MountConfig {
            source: missing_source,
            destination: PathBuf::from("/host-etc-hostname"),
            read_only: true,
            missing_source_policy: MissingMountSourcePolicy::Reject,
            selinux_relabel: false,
            propagation: MountPropagationMode::Private,
            recursive_read_only: false,
            uid_mappings: Vec::new(),
            gid_mappings: Vec::new(),
            requested_image: None,
            image_sub_path: None,
        }];

        let err = runtime.build_mounts("test-id", &config, false).unwrap_err();
        assert!(format!("{err}").contains("does not exist"));
    }

    #[test]
    fn test_build_mounts_adds_bind_mounts_for_image_defined_volumes() {
        let (mut runtime, temp_dir) = create_test_runtime();
        runtime.set_image_volumes_mode(ImageVolumesMode::Bind);
        let image_dir = temp_dir
            .path()
            .join("storage")
            .join("images")
            .join("sha256:test-image");
        fs::create_dir_all(&image_dir).unwrap();
        fs::write(
            image_dir.join("metadata.json"),
            serde_json::json!({
                "id": "sha256:test-image",
                "repo_tags": ["busybox:latest"],
                "declared_volumes": ["/cache", "/var/lib/data"]
            })
            .to_string(),
        )
        .unwrap();

        let mut config = create_test_config();
        config.image = "busybox:latest".to_string();
        let mounts = runtime.build_mounts("container-1", &config, false).unwrap();

        assert!(mounts.iter().any(|mount| mount.destination == "/cache"));
        assert!(mounts
            .iter()
            .any(|mount| mount.destination == "/var/lib/data"));
        assert!(runtime
            .image_volume_state_dir("container-1")
            .join("volume-0")
            .exists());
        assert!(runtime
            .image_volume_state_dir("container-1")
            .join("volume-1")
            .exists());
    }

    #[test]
    fn test_build_mounts_skips_bind_image_volume_when_explicit_mount_overrides_destination() {
        let (mut runtime, temp_dir) = create_test_runtime();
        runtime.set_image_volumes_mode(ImageVolumesMode::Bind);
        let image_dir = temp_dir
            .path()
            .join("storage")
            .join("images")
            .join("sha256:test-image");
        fs::create_dir_all(&image_dir).unwrap();
        fs::write(
            image_dir.join("metadata.json"),
            serde_json::json!({
                "id": "sha256:test-image",
                "repo_tags": ["busybox:latest"],
                "declared_volumes": ["/var/lib/data"]
            })
            .to_string(),
        )
        .unwrap();

        let explicit_source = temp_dir.path().join("explicit");
        fs::create_dir_all(&explicit_source).unwrap();
        let mut config = create_test_config();
        config.image = "busybox:latest".to_string();
        config.mounts = vec![MountConfig {
            source: explicit_source.clone(),
            destination: PathBuf::from("/var/lib/data"),
            read_only: false,
            missing_source_policy: MissingMountSourcePolicy::Reject,
            selinux_relabel: false,
            propagation: MountPropagationMode::Private,
            recursive_read_only: false,
            uid_mappings: Vec::new(),
            gid_mappings: Vec::new(),
            requested_image: None,
            image_sub_path: None,
        }];

        let mounts = runtime.build_mounts("container-1", &config, false).unwrap();
        let matching: Vec<_> = mounts
            .iter()
            .filter(|mount| mount.destination == "/var/lib/data")
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(
            matching[0].source.as_deref(),
            Some(explicit_source.to_string_lossy().as_ref())
        );
    }

    #[test]
    fn test_desired_rootfs_propagation_tracks_mount_propagation_requirements() {
        let temp_dir = tempdir().unwrap();
        let source = temp_dir.path().join("source");
        let mut mount = test_mount_config(source, "/data");
        mount.propagation = MountPropagationMode::HostToContainer;

        assert_eq!(
            RuncRuntime::desired_rootfs_propagation(&[mount]),
            Some("rslave")
        );
    }

    #[test]
    fn test_write_bundle_serializes_mount_id_mappings() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = write_runtime_features_script(
            temp_dir.path(),
            r#"{
  "ociVersionMin": "1.0.0",
  "ociVersionMax": "1.2.0",
  "mountOptions": ["ro", "rro"],
  "linux": {
    "mountExtensions": {
      "idmap": {
        "enabled": true
      }
    }
  }
}"#,
            0,
        );
        let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
        let source = temp_dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let rootfs = temp_dir.path().join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        let mut config = create_test_config();
        config.rootfs = rootfs.clone();
        let mut mount = test_mount_config(source, "/data");
        mount.uid_mappings = vec![crate::oci::spec::IdMapping {
            container_id: 0,
            host_id: 1000,
            size: 1,
        }];
        mount.gid_mappings = vec![crate::oci::spec::IdMapping {
            container_id: 0,
            host_id: 2000,
            size: 1,
        }];
        config.mounts = vec![mount];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        runtime.write_bundle("test-id", &rootfs, &spec).unwrap();

        let raw = fs::read_to_string(runtime.config_path("test-id")).unwrap();
        let saved: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let mount = saved["mounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["destination"] == "/data")
            .unwrap();
        assert_eq!(mount["uidMappings"].as_array().unwrap().len(), 1);
        assert_eq!(mount["gidMappings"].as_array().unwrap().len(), 1);
        assert!(!mount["options"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(
                |option| option.starts_with(INTERNAL_UID_MAPPINGS_MOUNT_OPTION_PREFIX)
                    || option.starts_with(INTERNAL_GID_MAPPINGS_MOUNT_OPTION_PREFIX)
            ));
    }

    #[test]
    fn test_prepare_rootfs_creates_image_defined_volume_directories_for_mkdir() {
        let (runtime, temp_dir) = create_test_runtime();
        let image_dir = temp_dir
            .path()
            .join("storage")
            .join("images")
            .join("sha256:test-image");
        fs::create_dir_all(&image_dir).unwrap();
        fs::write(
            image_dir.join("metadata.json"),
            serde_json::json!({
                "id": "sha256:test-image",
                "repo_tags": ["busybox:latest"],
                "declared_volumes": ["/cache", "/var/lib/data"]
            })
            .to_string(),
        )
        .unwrap();

        let rootfs = temp_dir.path().join("rootfs");
        runtime
            .ensure_image_volume_directories("busybox:latest", &rootfs)
            .unwrap();

        assert!(rootfs.join("cache").is_dir());
        assert!(rootfs.join("var/lib/data").is_dir());
    }

    #[test]
    fn test_restore_container_uses_configured_no_pivot_policy() {
        let temp_dir = tempdir().unwrap();
        let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
        let args_path = temp_dir.path().join("restore.args");
        let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
            temp_dir.path(),
            &checkpoint_args_path,
            &args_path,
        );
        let root = temp_dir.path().join("containers");
        let mut runtime = RuncRuntime::new(runtime_path, root.clone());
        runtime.set_no_pivot(true);
        runtime.set_criu_path(PathBuf::from("/usr/sbin/criu"));
        fs::create_dir_all(root.join("container-1")).unwrap();
        let image_path = temp_dir.path().join("checkpoint");
        let work_path = temp_dir.path().join("checkpoint-work");
        fs::create_dir_all(&image_path).unwrap();

        runtime
            .restore_container_from_checkpoint("container-1", &image_path, &work_path)
            .unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--image-path"));
        assert!(args
            .lines()
            .any(|line| line == image_path.to_string_lossy()));
        assert!(args.lines().any(|line| line == "--work-path"));
        assert!(args.lines().any(|line| line == work_path.to_string_lossy()));
        assert!(args.lines().any(|line| line == "--no-pivot"));
        assert!(args.lines().any(|line| line == "--criu"));
        assert!(args.lines().any(|line| line == "/usr/sbin/criu"));
    }

    #[test]
    fn test_restore_container_replaces_rootfs_from_snapshot() {
        let temp_dir = tempdir().unwrap();
        let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
        let restore_args_path = temp_dir.path().join("restore.args");
        let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
            temp_dir.path(),
            &checkpoint_args_path,
            &restore_args_path,
        );
        let root = temp_dir.path().join("containers");
        let runtime = RuncRuntime::new(runtime_path, root.clone());
        let bundle = root.join("container-1");
        let rootfs = temp_dir.path().join("rootfs-live");
        fs::create_dir_all(&rootfs).unwrap();
        fs::write(rootfs.join("stale.txt"), "stale").unwrap();
        fs::create_dir_all(&bundle).unwrap();
        fs::write(
            bundle.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "root": { "path": rootfs.display().to_string() }
            })
            .to_string(),
        )
        .unwrap();

        let snapshot_src = temp_dir.path().join("rootfs-snapshot");
        fs::create_dir_all(&snapshot_src).unwrap();
        fs::write(snapshot_src.join("restored.txt"), "restored").unwrap();
        let image_path = temp_dir.path().join("checkpoint");
        let work_path = temp_dir.path().join("checkpoint-work");
        fs::create_dir_all(&image_path).unwrap();
        let tar_status = Command::new("tar")
            .args([
                "-cf",
                image_path.join("rootfs.tar").to_str().unwrap(),
                "-C",
                snapshot_src.to_str().unwrap(),
                ".",
            ])
            .status()
            .unwrap();
        assert!(tar_status.success());

        runtime
            .restore_container_from_checkpoint("container-1", &image_path, &work_path)
            .unwrap();

        assert!(!rootfs.join("stale.txt").exists());
        assert_eq!(
            fs::read_to_string(rootfs.join("restored.txt")).unwrap(),
            "restored"
        );
    }

    #[test]
    fn test_restore_container_keeps_existing_rootfs_when_snapshot_absent() {
        let temp_dir = tempdir().unwrap();
        let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
        let restore_args_path = temp_dir.path().join("restore.args");
        let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
            temp_dir.path(),
            &checkpoint_args_path,
            &restore_args_path,
        );
        let root = temp_dir.path().join("containers");
        let runtime = RuncRuntime::new(runtime_path, root.clone());
        let bundle = root.join("container-1");
        let rootfs = temp_dir.path().join("rootfs-live");
        fs::create_dir_all(&rootfs).unwrap();
        fs::write(rootfs.join("keep.txt"), "keep").unwrap();
        fs::create_dir_all(&bundle).unwrap();
        fs::write(
            bundle.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "root": { "path": rootfs.display().to_string() }
            })
            .to_string(),
        )
        .unwrap();

        let image_path = temp_dir.path().join("checkpoint");
        let work_path = temp_dir.path().join("checkpoint-work");
        fs::create_dir_all(&image_path).unwrap();

        runtime
            .restore_container_from_checkpoint("container-1", &image_path, &work_path)
            .unwrap();

        assert_eq!(fs::read_to_string(rootfs.join("keep.txt")).unwrap(), "keep");
    }

    #[test]
    fn test_reopen_container_log_reports_missing_socket() {
        let temp_dir = tempdir().unwrap();
        let shim_dir = temp_dir.path().join("shims");
        let container_id = "container-1";
        let container_shim_dir = shim_dir.join(container_id);
        fs::create_dir_all(&container_shim_dir).unwrap();

        let runtime = RuncRuntime::with_shim(
            PathBuf::from("runc"),
            temp_dir.path().join("containers"),
            ShimConfig {
                work_dir: shim_dir,
                attach_socket_dir: temp_dir.path().join("attach"),
                container_exits_dir: temp_dir.path().join("exits"),
                ..Default::default()
            },
        );

        let err = runtime.reopen_container_log(container_id).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<LogReopenError>(),
            Some(LogReopenError::MissingSocket { .. })
        ));
    }
}
