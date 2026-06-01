use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredNamespaceOptions {
    pub(super) network: i32,
    pub(super) pid: i32,
    pub(super) ipc: i32,
    pub(super) target_id: String,
    pub(super) userns_options: Option<StoredUserNamespace>,
}

impl StoredNamespaceOptions {
    pub(super) fn to_proto(&self) -> NamespaceOption {
        NamespaceOption {
            network: self.network,
            pid: self.pid,
            ipc: self.ipc,
            target_id: self.target_id.clone(),
            userns_options: self
                .userns_options
                .as_ref()
                .map(StoredUserNamespace::to_proto),
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
            userns_options: value.userns_options.as_ref().map(StoredUserNamespace::from),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredUserNamespace {
    pub(super) mode: i32,
    pub(super) uids: Vec<StoredIdMapping>,
    pub(super) gids: Vec<StoredIdMapping>,
}

impl StoredUserNamespace {
    pub(super) fn to_proto(&self) -> crate::proto::runtime::v1::UserNamespace {
        crate::proto::runtime::v1::UserNamespace {
            mode: self.mode,
            uids: self.uids.iter().map(StoredIdMapping::to_proto).collect(),
            gids: self.gids.iter().map(StoredIdMapping::to_proto).collect(),
        }
    }
}

impl From<&crate::proto::runtime::v1::UserNamespace> for StoredUserNamespace {
    fn from(value: &crate::proto::runtime::v1::UserNamespace) -> Self {
        Self {
            mode: value.mode,
            uids: value.uids.iter().map(StoredIdMapping::from).collect(),
            gids: value.gids.iter().map(StoredIdMapping::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredIdMapping {
    pub(super) host_id: u32,
    pub(super) container_id: u32,
    pub(super) length: u32,
}

impl StoredIdMapping {
    pub(super) fn to_proto(&self) -> crate::proto::runtime::v1::IdMapping {
        crate::proto::runtime::v1::IdMapping {
            host_id: self.host_id,
            container_id: self.container_id,
            length: self.length,
        }
    }
}

impl From<&crate::proto::runtime::v1::IdMapping> for StoredIdMapping {
    fn from(value: &crate::proto::runtime::v1::IdMapping) -> Self {
        Self {
            host_id: value.host_id,
            container_id: value.container_id,
            length: value.length,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredHugepageLimit {
    pub(super) page_size: String,
    pub(super) limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredLinuxDeviceCgroup {
    pub(super) allow: bool,
    pub(super) device_type: Option<String>,
    pub(super) major: Option<i64>,
    pub(super) minor: Option<i64>,
    pub(super) access: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredLinuxResources {
    pub(super) cpu_period: i64,
    pub(super) cpu_quota: i64,
    pub(super) cpu_shares: i64,
    pub(super) memory_limit_in_bytes: i64,
    pub(super) oom_score_adj: i64,
    pub(super) cpuset_cpus: String,
    pub(super) cpuset_mems: String,
    pub(super) hugepage_limits: Vec<StoredHugepageLimit>,
    pub(super) unified: HashMap<String, String>,
    pub(super) memory_swap_limit_in_bytes: i64,
    pub(super) memory_reservation_in_bytes: Option<i64>,
    pub(super) memory_kernel_limit_in_bytes: Option<i64>,
    pub(super) memory_kernel_tcp_limit_in_bytes: Option<i64>,
    pub(super) memory_swappiness: Option<u64>,
    pub(super) memory_disable_oom_killer: Option<bool>,
    pub(super) memory_use_hierarchy: Option<bool>,
    pub(super) cpu_realtime_runtime: Option<i64>,
    pub(super) cpu_realtime_period: Option<u64>,
    pub(super) pids_limit: Option<i64>,
    pub(super) devices: Vec<StoredLinuxDeviceCgroup>,
    pub(super) blockio_class: Option<String>,
    pub(super) rdt_class: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CgroupResourceSupport {
    pub(super) swap: bool,
    pub(super) hugetlb: bool,
    pub(super) memory_kernel: bool,
    pub(super) memory_kernel_tcp: bool,
    pub(super) memory_swappiness: bool,
    pub(super) memory_disable_oom_killer: bool,
    pub(super) memory_use_hierarchy: bool,
    pub(super) cpu_realtime: bool,
    pub(super) blockio: bool,
    pub(super) rdt: bool,
}

impl StoredLinuxResources {
    pub(super) fn to_proto(&self) -> crate::proto::runtime::v1::LinuxContainerResources {
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

    pub(super) fn optional_int64(
        value: i64,
    ) -> protobuf::MessageField<crate::nri_proto::api::OptionalInt64> {
        let mut result = crate::nri_proto::api::OptionalInt64::new();
        result.value = value;
        protobuf::MessageField::some(result)
    }

    pub(super) fn optional_uint64(
        value: u64,
    ) -> protobuf::MessageField<crate::nri_proto::api::OptionalUInt64> {
        let mut result = crate::nri_proto::api::OptionalUInt64::new();
        result.value = value;
        protobuf::MessageField::some(result)
    }

    pub(super) fn optional_bool(
        value: bool,
    ) -> protobuf::MessageField<crate::nri_proto::api::OptionalBool> {
        let mut result = crate::nri_proto::api::OptionalBool::new();
        result.value = value;
        protobuf::MessageField::some(result)
    }

    pub(super) fn optional_string(
        value: impl Into<String>,
    ) -> protobuf::MessageField<crate::nri_proto::api::OptionalString> {
        let mut result = crate::nri_proto::api::OptionalString::new();
        result.value = value.into();
        protobuf::MessageField::some(result)
    }

    pub(super) fn apply_nri(&mut self, resources: &crate::nri_proto::api::LinuxResources) {
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

    pub(super) fn to_nri(&self) -> crate::nri_proto::api::LinuxResources {
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
pub(super) struct StoredSecurityProfile {
    pub(super) profile_type: i32,
    pub(super) localhost_ref: String,
}

impl StoredSecurityProfile {
    pub(super) fn from_runtime_seccomp(profile: &SeccompProfile) -> Self {
        match profile {
            SeccompProfile::RuntimeDefault => Self {
                profile_type:
                    crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault as i32,
                localhost_ref: String::new(),
            },
            SeccompProfile::Unconfined => Self {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                    as i32,
                localhost_ref: String::new(),
            },
            SeccompProfile::Localhost(path) => Self {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                    as i32,
                localhost_ref: path.to_string_lossy().to_string(),
            },
        }
    }

    pub(super) fn to_runtime_seccomp(&self) -> Option<SeccompProfile> {
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

    pub(super) fn to_nri_seccomp(&self) -> Option<crate::nri_proto::api::SecurityProfile> {
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
pub(super) struct StoredPodState {
    pub(super) port_mappings: Vec<StoredPortMapping>,
    pub(super) raw_cni_result: Option<serde_json::Value>,
    pub(super) hostname: Option<String>,
    pub(super) log_directory: Option<String>,
    pub(super) runtime_handler: String,
    pub(super) runtime_pod_cidr: Option<String>,
    pub(super) netns_path: Option<String>,
    pub(super) pause_container_id: Option<String>,
    pub(super) ip: Option<String>,
    pub(super) additional_ips: Vec<String>,
    pub(super) cgroup_parent: Option<String>,
    pub(super) sysctls: HashMap<String, String>,
    pub(super) namespace_options: Option<StoredNamespaceOptions>,
    pub(super) privileged: bool,
    pub(super) run_as_user: Option<String>,
    pub(super) run_as_group: Option<u32>,
    pub(super) supplemental_groups: Vec<u32>,
    pub(super) readonly_rootfs: bool,
    pub(super) no_new_privileges: Option<bool>,
    pub(super) apparmor_profile: Option<String>,
    pub(super) selinux_label: Option<String>,
    pub(super) seccomp_profile: Option<StoredSecurityProfile>,
    pub(super) overhead_linux_resources: Option<StoredLinuxResources>,
    pub(super) linux_resources: Option<StoredLinuxResources>,
    pub(super) stop_notified: bool,
    pub(super) broken: Option<StoredBrokenState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredPortMapping {
    pub(super) protocol: String,
    pub(super) container_port: i32,
    pub(super) host_port: i32,
    pub(super) host_ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredContainerState {
    pub(super) log_path: Option<String>,
    pub(super) tty: bool,
    pub(super) stdin: bool,
    pub(super) stdin_once: bool,
    pub(super) privileged: bool,
    pub(super) readonly_rootfs: bool,
    pub(super) cgroup_parent: Option<String>,
    pub(super) network_namespace_path: Option<String>,
    pub(super) linux_resources: Option<StoredLinuxResources>,
    pub(super) mounts: Vec<StoredMount>,
    pub(super) run_as_user: Option<String>,
    pub(super) run_as_group: Option<u32>,
    pub(super) supplemental_groups: Vec<u32>,
    pub(super) no_new_privileges: Option<bool>,
    pub(super) apparmor_profile: Option<String>,
    pub(super) seccomp_profile: Option<StoredSecurityProfile>,
    pub(super) seccomp_notifier_action: Option<String>,
    pub(super) metadata_name: Option<String>,
    pub(super) metadata_attempt: Option<u32>,
    pub(super) started_at: Option<i64>,
    pub(super) finished_at: Option<i64>,
    pub(super) exit_code: Option<i32>,
    pub(super) nri_stop_notified: bool,
    pub(super) nri_remove_notified: bool,
    pub(super) broken: Option<StoredBrokenState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredBrokenState {
    pub(super) kind: String,
    pub(super) details: String,
    pub(super) detected_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredCheckpointRestore {
    pub(super) checkpoint_location: String,
    pub(super) checkpoint_image_path: String,
    pub(super) oci_config: serde_json::Value,
    pub(super) image_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredRuntimeNetworkConfig {
    pub(super) pod_cidr: String,
}

pub(super) struct CniTemplateContext {
    pub(super) pod_cidr: String,
    pub(super) pod_cidr_ranges: Vec<String>,
    pub(super) routes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct StoredMount {
    pub(super) container_path: String,
    pub(super) host_path: String,
    pub(super) image: String,
    pub(super) image_sub_path: String,
    pub(super) readonly: bool,
    pub(super) selinux_relabel: bool,
    pub(super) propagation: i32,
}

#[derive(Clone)]
pub(super) struct NriRuntimeDomain {
    pub(super) containers: Arc<Mutex<HashMap<String, Container>>>,
    pub(super) pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    pub(super) config: RuntimeConfig,
    pub(super) nri_config: NriConfig,
    pub(super) runtime: RuntimeRegistry,
    pub(super) persistence: Arc<Mutex<PersistenceManager>>,
    pub(super) events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
}
