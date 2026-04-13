use anyhow::{Context, Result};
use log::{debug, error, info};
use nix::sys::stat::{major, makedev, minor, mknod, stat, Mode, SFlag};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use uuid::Uuid;

use crate::cgroups::{to_oci_resources, CgroupManager, CpuLimit, MemoryLimit, ResourceLimits};
use crate::oci::spec::{
    Device as OciDevice, Linux, LinuxCapabilities, LinuxDeviceCgroup, LinuxResources, Mount,
    Namespace as OciNamespace, Process, Root, Spec, User,
};
use crate::proto::runtime::v1::{
    Capability, LinuxContainerResources, NamespaceMode, NamespaceOption,
};

pub mod shim_manager;
pub use shim_manager::{default_shim_work_dir, ShimConfig, ShimManager, ShimProcess};

const INTERNAL_CHECKPOINT_RESTORE_KEY: &str = "io.crius.internal/checkpoint-restore";
const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";

/// 容器运行时接口
pub trait ContainerRuntime {
    /// 创建容器
    fn create_container(&self, config: &ContainerConfig) -> Result<String>;

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
    pub rootfs: PathBuf,
}

/// Seccomp 配置来源
#[derive(Debug, Clone)]
pub enum SeccompProfile {
    RuntimeDefault,
    Unconfined,
    Localhost(PathBuf),
}

/// 命名空间路径覆盖
#[derive(Debug, Clone, Default)]
pub struct NamespacePaths {
    pub network: Option<PathBuf>,
    pub pid: Option<PathBuf>,
    pub ipc: Option<PathBuf>,
    pub uts: Option<PathBuf>,
}

/// 挂载点配置
#[derive(Debug, Clone)]
pub struct MountConfig {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub read_only: bool,
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

/// 使用 runc 作为容器运行时
#[derive(Debug, Clone)]
pub struct RuncRuntime {
    runtime_path: PathBuf,
    root: PathBuf,
    image_storage_root: PathBuf,
    shim_manager: Option<Arc<ShimManager>>,
}

impl RuncRuntime {
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

    fn apply_capability_overrides(
        default_caps: &[String],
        overrides: Option<&Capability>,
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
            inheritable: Some(base.clone()),
            permitted: Some(base),
            ambient: Some(ambient),
        }
    }

    fn build_user(config: &ContainerConfig) -> Option<User> {
        let user = config.user.as_ref()?;
        let additional_gids = if config.supplemental_groups.is_empty() {
            None
        } else {
            Some(config.supplemental_groups.clone())
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

    fn load_seccomp_profile(
        &self,
        profile: Option<&SeccompProfile>,
    ) -> Result<Option<crate::oci::spec::Seccomp>> {
        match profile {
            None => Ok(None),
            Some(SeccompProfile::RuntimeDefault) => Ok(None),
            Some(SeccompProfile::Unconfined) => Ok(None),
            Some(SeccompProfile::Localhost(path)) => {
                let content = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read seccomp profile from {:?}", path))?;
                let seccomp = serde_json::from_str(&content).with_context(|| {
                    format!("Failed to parse seccomp profile JSON from {:?}", path)
                })?;
                Ok(Some(seccomp))
            }
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
                uid: Some(file_stat.st_uid),
                gid: Some(file_stat.st_gid),
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

    fn unpack_layer_with_tar(layer_file: &Path, rootfs_dir: &Path) -> Result<()> {
        let output = Command::new("tar")
            .arg("-xzf")
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

        Err(anyhow::anyhow!(
            "Image not found locally for reference: {}",
            image_ref
        ))
    }

    fn prepare_rootfs_from_image(
        &self,
        image_ref: &str,
        rootfs_dir: &Path,
        container_id: &str,
    ) -> Result<()> {
        if rootfs_dir.exists() {
            std::fs::remove_dir_all(&rootfs_dir)
                .with_context(|| format!("Failed to clean rootfs directory: {:?}", rootfs_dir))?;
        }
        std::fs::create_dir_all(&rootfs_dir)
            .with_context(|| format!("Failed to create rootfs directory: {:?}", rootfs_dir))?;

        let image_dir = self.resolve_image_dir(image_ref)?;
        let mut layer_files: Vec<PathBuf> = std::fs::read_dir(&image_dir)?
            .filter_map(|e| e.ok().map(|v| v.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("gz"))
            .collect();
        layer_files.sort_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.split('.').next())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(u32::MAX)
        });

        if layer_files.is_empty() {
            return Err(anyhow::anyhow!(
                "No image layers found in {:?} for image {}",
                image_dir,
                image_ref
            ));
        }

        for layer_file in &layer_files {
            Self::unpack_layer_with_tar(layer_file, &rootfs_dir)
                .with_context(|| format!("Failed to unpack layer archive: {:?}", layer_file))?;
        }

        // Ensure minimum runtime paths exist for scratch-like images (e.g. pause).
        std::fs::create_dir_all(rootfs_dir.join("dev"))
            .context("Failed to create /dev in rootfs")?;
        std::fs::create_dir_all(rootfs_dir.join("proc"))
            .context("Failed to create /proc in rootfs")?;
        std::fs::create_dir_all(rootfs_dir.join("sys"))
            .context("Failed to create /sys in rootfs")?;

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

        info!(
            "Prepared rootfs for container {} from image {}",
            container_id, image_ref
        );
        Ok(())
    }

    fn spec_from_restore_template(
        &self,
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
            if !config.env.is_empty() {
                process.env = Some(
                    config
                        .env
                        .iter()
                        .map(|(key, value)| format!("{}={}", key, value))
                        .collect(),
                );
            }
        }

        if let Some(linux) = spec.linux.as_mut() {
            linux.namespaces = Some(self.build_namespaces(config));
            linux.cgroups_path = config.cgroup_parent.clone();
            linux.seccomp = self.load_seccomp_profile(config.seccomp_profile.as_ref())?;
            linux.mount_label = config.selinux_label.clone();
            if !config.sysctls.is_empty() {
                linux.sysctl = Some(config.sysctls.clone());
            }
            if let Some(resources) = config
                .linux_resources
                .as_ref()
                .map(Self::linux_resources_to_oci)
            {
                linux.resources = Some(resources);
            }
        }

        let mut annotations = spec.annotations.unwrap_or_default();
        annotations.insert(
            "org.opencontainers.image.ref.name".to_string(),
            restore.image_ref.clone(),
        );
        for (key, value) in &config.annotations {
            annotations.insert(key.clone(), value.clone());
        }
        spec.annotations = Some(annotations);

        Ok(spec)
    }

    pub fn checkpoint_container(&self, container_id: &str, image_path: &Path) -> Result<()> {
        let work_path = image_path.join("work");
        std::fs::create_dir_all(image_path).with_context(|| {
            format!(
                "Failed to create checkpoint image directory {}",
                image_path.display()
            )
        })?;
        std::fs::create_dir_all(&work_path).with_context(|| {
            format!(
                "Failed to create checkpoint work directory {}",
                work_path.display()
            )
        })?;

        self.runc_exec(&[
            "checkpoint",
            "--file-locks",
            "--image-path",
            &image_path.to_string_lossy(),
            "--work-path",
            &work_path.to_string_lossy(),
            "--leave-running",
            container_id,
        ])
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

    pub fn restore_container_from_checkpoint(
        &self,
        container_id: &str,
        image_path: &Path,
    ) -> Result<()> {
        let work_path = image_path.join("work");
        std::fs::create_dir_all(&work_path).with_context(|| {
            format!(
                "Failed to create restore work directory {}",
                work_path.display()
            )
        })?;
        let bundle_path = self.bundle_path(container_id);

        self.restore_rootfs_snapshot(container_id, image_path)?;

        self.runc_exec(&[
            "restore",
            "-d",
            "--image-path",
            &image_path.to_string_lossy(),
            "--work-path",
            &work_path.to_string_lossy(),
            "--bundle",
            &bundle_path.to_string_lossy(),
            "--no-pivot",
            container_id,
        ])
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
            root,
            image_storage_root,
            shim_manager: None,
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
        let shim_manager = Arc::new(ShimManager::new(shim_config));
        Self {
            runtime_path,
            root,
            image_storage_root,
            shim_manager: Some(shim_manager),
        }
    }

    /// 启用shim支持
    pub fn enable_shim(&mut self, config: ShimConfig) {
        self.shim_manager = Some(Arc::new(ShimManager::new(config)));
    }

    /// 检查是否启用了shim
    pub fn is_shim_enabled(&self) -> bool {
        self.shim_manager.is_some()
    }

    /// 获取容器的bundle目录
    fn bundle_path(&self, container_id: &str) -> PathBuf {
        self.root.join(container_id)
    }

    /// 获取容器的config.json路径
    fn config_path(&self, container_id: &str) -> PathBuf {
        self.bundle_path(container_id).join("config.json")
    }

    /// 执行runc命令并返回输出（仅用于需要解析stdout的查询类命令）
    fn run_command_output(&self, args: &[&str]) -> Result<Output> {
        debug!(
            "Executing: {} {}",
            self.runtime_path.display(),
            args.join(" ")
        );

        let output = Command::new(&self.runtime_path)
            .args(args)
            .env("XDG_RUNTIME_DIR", "/run/user/0")
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

        let status = Command::new(&self.runtime_path)
            .args(args)
            .env("XDG_RUNTIME_DIR", "/run/user/0")
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
    fn create_spec(&self, config: &ContainerConfig, _container_id: &str) -> Result<Spec> {
        let mut spec = Spec::new("1.0.2");

        // 设置root配置
        spec.root = Some(Root {
            path: config.rootfs.to_string_lossy().to_string(),
            readonly: Some(config.readonly_rootfs),
        });

        // 设置进程配置
        let mut args = config.command.clone();
        if !config.args.is_empty() {
            args.extend(config.args.clone());
        }

        // 如果没有命令，使用默认shell
        if args.is_empty() {
            args = vec!["sh".to_string()];
        }

        // 转换环境变量为字符串格式
        let env: Vec<String> = config
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        spec.process = Some(Process {
            terminal: Some(config.tty),
            user: Self::build_user(config),
            args,
            env: if env.is_empty() { None } else { Some(env) },
            cwd: config
                .working_dir
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "/".to_string()),
            capabilities: Some(Self::apply_capability_overrides(
                &Self::default_capabilities(),
                config.capabilities.as_ref(),
            )),
            rlimits: None,
            no_new_privileges: Some(config.no_new_privileges.unwrap_or(!config.privileged)),
            apparmor_profile: config.apparmor_profile.clone(),
            selinux_label: config.selinux_label.clone(),
        });

        // 设置主机名
        spec.hostname = config.hostname.clone();

        // 设置挂载点
        let default_mounts = Spec::default_mounts();
        let custom_mounts: Vec<Mount> = config
            .mounts
            .iter()
            .map(|m| Mount {
                destination: m.destination.to_string_lossy().to_string(),
                source: Some(m.source.to_string_lossy().to_string()),
                mount_type: Some("bind".to_string()),
                options: if m.read_only {
                    Some(vec!["rbind".to_string(), "ro".to_string()])
                } else {
                    Some(vec!["rbind".to_string(), "rw".to_string()])
                },
            })
            .collect();

        let keep_hugepages_mount = std::env::var("CRIUS_ENABLE_HUGEPAGES_MOUNT")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let mut all_mounts: Vec<Mount> = default_mounts
            .into_iter()
            .filter(|m| {
                m.destination != "/sys/fs/cgroup"
                    && (keep_hugepages_mount || m.destination != "/dev/hugepages")
            })
            .collect();
        all_mounts.extend(custom_mounts);
        spec.mounts = Some(all_mounts);

        // 设置Linux配置
        let mut devices = if config.privileged {
            // 特权容器可以访问所有设备
            vec![]
        } else {
            Spec::default_devices()
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

        if !config.devices.is_empty() {
            let (extra_devices, extra_cgroup_rules) =
                Self::device_mappings_to_oci(&config.devices)?;
            devices.extend(extra_devices);
            resources.devices = Some(extra_cgroup_rules);
        }

        if resources.devices.is_none() {
            resources.devices = Some(vec![LinuxDeviceCgroup {
                allow: true,
                device_type: None,
                major: None,
                minor: None,
                access: Some("rwm".to_string()),
            }]);
        }

        spec.linux = Some(Linux {
            namespaces: Some(self.build_namespaces(config)),
            uid_mappings: None,
            gid_mappings: None,
            devices: Some(devices),
            cgroups_path: config.cgroup_parent.clone(),
            resources: Some(resources),
            rootfs_propagation: None,
            seccomp: self.load_seccomp_profile(config.seccomp_profile.as_ref())?,
            sysctl: if config.sysctls.is_empty() {
                None
            } else {
                Some(config.sysctls.clone())
            },
            mount_label: config.selinux_label.clone(),
            intel_rdt: None,
        });

        // 设置注解
        let mut annotations = std::collections::HashMap::new();
        annotations.insert(
            "org.opencontainers.image.ref.name".to_string(),
            config.image.clone(),
        );
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
        spec.save(&self.config_path(container_id))?;

        // 当前运行时使用 OCI spec.root.path 作为 rootfs 来源，bundle 内不再强制准备 rootfs 目录。
        let _ = rootfs;

        info!(
            "Created bundle for container {} at {:?}",
            container_id, bundle_path
        );
        Ok(())
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
        let socket_path = shim_manager.socket_path(container_id, "reopen.sock");
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
}

impl ContainerRuntime for RuncRuntime {
    fn create_container(&self, config: &ContainerConfig) -> Result<String> {
        // 生成容器ID
        let container_id = Uuid::new_v4().to_simple().to_string();
        let checkpoint_restore = Self::checkpoint_restore_from_annotations(&config.annotations);
        let image_ref = checkpoint_restore
            .as_ref()
            .map(|restore| restore.image_ref.as_str())
            .unwrap_or(config.image.as_str());

        info!(
            "Creating container {} with image {}",
            container_id, image_ref
        );

        // 先从镜像构建rootfs，再生成OCI配置
        self.prepare_rootfs_from_image(image_ref, &config.rootfs, &container_id)
            .context("Failed to prepare rootfs from image")?;

        // 创建OCI配置
        let spec = if let Some(checkpoint_restore) = checkpoint_restore.as_ref() {
            self.spec_from_restore_template(config, checkpoint_restore)
                .context("Failed to create OCI spec from checkpoint artifact")?
        } else {
            self.create_spec(config, &container_id)
                .context("Failed to create OCI spec")?
        };

        // 创建bundle
        self.create_bundle(&container_id, &config.rootfs, &spec)?;

        // 延迟到start阶段再调用runc，避免create阶段阻塞导致CRI超时。
        info!("Container {} bundle prepared successfully", container_id);
        Ok(container_id)
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
                    self.runc_exec(&[
                        "run",
                        "-d",
                        "--bundle",
                        &bundle_path.to_string_lossy(),
                        "--no-pivot",
                        container_id,
                    ])?;
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

        // 如果启用了shim，先停止shim
        if let Some(ref shim_manager) = self.shim_manager {
            if shim_manager.is_shim_running(container_id) {
                shim_manager.stop_shim(container_id)?;
                info!("Shim for container {} stopped", container_id);
            }
        }

        // 获取当前状态
        let state = self.get_runc_state(container_id)?;

        match state {
            None => {
                info!("Container {} not found, already stopped", container_id);
                return Ok(());
            }
            Some(s) => {
                if s.status == "stopped" {
                    info!("Container {} already stopped", container_id);
                    return Ok(());
                }
            }
        }

        // 发送SIGTERM信号
        let signal = "TERM";
        self.runc_exec(&["kill", container_id, signal])?;

        // 等待容器停止
        let timeout_secs = timeout.unwrap_or(10);
        for _ in 0..timeout_secs {
            std::thread::sleep(std::time::Duration::from_secs(1));

            match self.get_runc_state(container_id)? {
                None => break,
                Some(s) if s.status == "stopped" => break,
                _ => continue,
            }
        }

        // 如果还在运行，发送SIGKILL
        if let Ok(Some(state)) = self.get_runc_state(container_id) {
            if state.status != "stopped" {
                info!(
                    "Container {} did not stop gracefully, sending SIGKILL",
                    container_id
                );
                let _ = self.runc_exec(&["kill", container_id, "KILL"]);
            }
        }

        info!("Container {} stopped", container_id);
        Ok(())
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        info!("Removing container {}", container_id);

        // 首先停止容器（如果还在运行）
        let _ = self.stop_container(container_id, Some(5));

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

        let mut cmd = std::process::Command::new(&self.runtime_path);
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
    use std::io::Write;
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;
    use std::thread;
    use tempfile::tempdir;

    fn create_test_runtime() -> (RuncRuntime, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("containers"));
        (runtime, temp_dir)
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
            rootfs: PathBuf::from("/tmp/rootfs"),
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
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.mounts = vec![MountConfig {
            source: PathBuf::from("/host/path"),
            destination: PathBuf::from("/container/path"),
            read_only: true,
        }];

        let spec = runtime.create_spec(&config, "test-id").unwrap();
        let mounts = spec.mounts.unwrap();

        // Should have default mounts + 1 custom mount
        assert!(mounts.len() > 1);
        assert!(mounts.iter().any(|m| m.destination == "/container/path"));
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

        // Privileged containers have empty device list
        assert!(linux.devices.unwrap().is_empty());
    }

    #[test]
    fn test_spec_with_runtime_options() {
        let (runtime, _temp) = create_test_runtime();
        let mut config = create_test_config();
        config.tty = true;
        config.readonly_rootfs = true;
        config.cgroup_parent = Some("kubepods.slice/pod123".to_string());
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
        assert_eq!(linux.cgroups_path.as_deref(), Some("kubepods.slice/pod123"));
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
    fn test_spec_with_selinux_and_localhost_seccomp() {
        let (runtime, temp) = create_test_runtime();
        let mut config = create_test_config();
        config.selinux_label = Some("system_u:system_r:container_t:s0".to_string());

        let seccomp_profile_path = temp.path().join("seccomp.json");
        let seccomp_profile = crate::oci::spec::Seccomp {
            default_action: "SCMP_ACT_ALLOW".to_string(),
            architectures: None,
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
                ..Default::default()
            },
        );

        let err = runtime.reopen_container_log(container_id).unwrap_err();
        assert!(err.to_string().contains("reopen log socket"));
    }

    #[test]
    #[ignore = "requires unix socket bind permissions in the current test environment"]
    fn test_reopen_container_log_notifies_shim_control_socket() {
        let temp_dir = tempdir().unwrap();
        let shim_dir = temp_dir.path().join("shims");
        let container_id = "container-1";
        let container_shim_dir = shim_dir.join(container_id);
        fs::create_dir_all(&container_shim_dir).unwrap();
        let socket_path = container_shim_dir.join("reopen.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.write_all(b"OK\n").unwrap();
            tx.send(()).unwrap();
        });

        let runtime = RuncRuntime::with_shim(
            PathBuf::from("runc"),
            temp_dir.path().join("containers"),
            ShimConfig {
                work_dir: shim_dir,
                ..Default::default()
            },
        );

        runtime.reopen_container_log(container_id).unwrap();
        rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        server.join().unwrap();
    }

    #[test]
    #[ignore = "requires unix socket bind permissions in the current test environment"]
    fn test_reopen_container_log_surfaces_shim_error() {
        let temp_dir = tempdir().unwrap();
        let shim_dir = temp_dir.path().join("shims");
        let container_id = "container-1";
        let container_shim_dir = shim_dir.join(container_id);
        fs::create_dir_all(&container_shim_dir).unwrap();
        let socket_path = container_shim_dir.join("reopen.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(b"ERR failed to reopen underlying log file\n")
                .unwrap();
        });

        let runtime = RuncRuntime::with_shim(
            PathBuf::from("runc"),
            temp_dir.path().join("containers"),
            ShimConfig {
                work_dir: shim_dir,
                ..Default::default()
            },
        );

        let err = runtime.reopen_container_log(container_id).unwrap_err();
        assert!(err
            .to_string()
            .contains("failed to reopen underlying log file"));
        server.join().unwrap();
    }
}
