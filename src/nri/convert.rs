use std::collections::HashMap;

use protobuf::{Enum, MessageField};

use crate::nri_proto::api as nri_api;
use crate::oci::spec::{LinuxMemory, Seccomp, Spec};
use crate::proto::runtime::v1::LinuxContainerResources;

const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";

pub fn external_annotations(annotations: &HashMap<String, String>) -> HashMap<String, String> {
    annotations
        .iter()
        .filter(|(k, _)| !k.starts_with(INTERNAL_ANNOTATION_PREFIX))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn optional_int(value: i64) -> MessageField<nri_api::OptionalInt> {
    let mut result = nri_api::OptionalInt::new();
    result.value = value;
    MessageField::some(result)
}

fn optional_int64(value: i64) -> MessageField<nri_api::OptionalInt64> {
    let mut result = nri_api::OptionalInt64::new();
    result.value = value;
    MessageField::some(result)
}

fn optional_uint32(value: u32) -> MessageField<nri_api::OptionalUInt32> {
    let mut result = nri_api::OptionalUInt32::new();
    result.value = value;
    MessageField::some(result)
}

fn optional_uint64(value: u64) -> MessageField<nri_api::OptionalUInt64> {
    let mut result = nri_api::OptionalUInt64::new();
    result.value = value;
    MessageField::some(result)
}

fn optional_bool(value: bool) -> MessageField<nri_api::OptionalBool> {
    let mut result = nri_api::OptionalBool::new();
    result.value = value;
    MessageField::some(result)
}

fn optional_string(value: impl Into<String>) -> MessageField<nri_api::OptionalString> {
    let mut result = nri_api::OptionalString::new();
    result.value = value.into();
    MessageField::some(result)
}

fn optional_repeated_string(values: Vec<String>) -> MessageField<nri_api::OptionalRepeatedString> {
    let mut result = nri_api::OptionalRepeatedString::new();
    result.value = values;
    MessageField::some(result)
}

fn io_prio_class(value: &str) -> nri_api::IOPrioClass {
    nri_api::IOPrioClass::from_str(value).unwrap_or(nri_api::IOPrioClass::IOPRIO_CLASS_NONE)
}

fn scheduler_policy(value: &str) -> nri_api::LinuxSchedulerPolicy {
    nri_api::LinuxSchedulerPolicy::from_str(value)
        .unwrap_or(nri_api::LinuxSchedulerPolicy::SCHED_NONE)
}

fn scheduler_flag(value: &str) -> Option<nri_api::LinuxSchedulerFlag> {
    nri_api::LinuxSchedulerFlag::from_str(value)
}

fn linux_memory(memory: &LinuxMemory) -> nri_api::LinuxMemory {
    let mut result = nri_api::LinuxMemory::new();
    if let Some(limit) = memory.limit {
        result.limit = optional_int64(limit);
    }
    if let Some(reservation) = memory.reservation {
        result.reservation = optional_int64(reservation);
    }
    if let Some(swap) = memory.swap {
        result.swap = optional_int64(swap);
    }
    if let Some(kernel) = memory.kernel {
        result.kernel = optional_int64(kernel);
    }
    if let Some(kernel_tcp) = memory.kernel_tcp {
        result.kernel_tcp = optional_int64(kernel_tcp);
    }
    if let Some(swappiness) = memory.swappiness {
        result.swappiness = optional_uint64(swappiness);
    }
    if let Some(disable_oom_killer) = memory.disable_oom_killer {
        result.disable_oom_killer = optional_bool(disable_oom_killer);
    }
    if let Some(use_hierarchy) = memory.use_hierarchy {
        result.use_hierarchy = optional_bool(use_hierarchy);
    }
    result
}

pub fn linux_resources_from_cri(resources: &LinuxContainerResources) -> nri_api::LinuxResources {
    let mut result = nri_api::LinuxResources::new();

    if resources.memory_limit_in_bytes > 0 || resources.memory_swap_limit_in_bytes > 0 {
        let mut memory = nri_api::LinuxMemory::new();
        if resources.memory_limit_in_bytes > 0 {
            memory.limit = optional_int64(resources.memory_limit_in_bytes);
        }
        if resources.memory_swap_limit_in_bytes > 0 {
            memory.swap = optional_int64(resources.memory_swap_limit_in_bytes);
        }
        result.memory = MessageField::some(memory);
    }

    if resources.cpu_shares > 0
        || resources.cpu_quota > 0
        || resources.cpu_period > 0
        || !resources.cpuset_cpus.is_empty()
        || !resources.cpuset_mems.is_empty()
    {
        let mut cpu = nri_api::LinuxCPU::new();
        if resources.cpu_shares > 0 {
            cpu.shares = optional_uint64(resources.cpu_shares as u64);
        }
        if resources.cpu_quota > 0 {
            cpu.quota = optional_int64(resources.cpu_quota);
        }
        if resources.cpu_period > 0 {
            cpu.period = optional_uint64(resources.cpu_period as u64);
        }
        cpu.cpus = resources.cpuset_cpus.clone();
        cpu.mems = resources.cpuset_mems.clone();
        result.cpu = MessageField::some(cpu);
    }

    result.hugepage_limits = resources
        .hugepage_limits
        .iter()
        .map(|limit| {
            let mut item = nri_api::HugepageLimit::new();
            item.page_size = limit.page_size.clone();
            item.limit = limit.limit;
            item
        })
        .collect();
    result.unified = resources.unified.clone();
    result
}

pub fn cri_linux_resources_from_nri(
    resources: &nri_api::LinuxResources,
) -> LinuxContainerResources {
    let mut result = LinuxContainerResources::default();

    if let Some(memory) = resources.memory.as_ref() {
        if let Some(limit) = memory.limit.as_ref() {
            result.memory_limit_in_bytes = limit.value;
        }
        if let Some(swap) = memory.swap.as_ref() {
            result.memory_swap_limit_in_bytes = swap.value;
        }
    }

    if let Some(cpu) = resources.cpu.as_ref() {
        if let Some(shares) = cpu.shares.as_ref() {
            result.cpu_shares = shares.value as i64;
        }
        if let Some(quota) = cpu.quota.as_ref() {
            result.cpu_quota = quota.value;
        }
        if let Some(period) = cpu.period.as_ref() {
            result.cpu_period = period.value as i64;
        }
        result.cpuset_cpus = cpu.cpus.clone();
        result.cpuset_mems = cpu.mems.clone();
    }

    result.hugepage_limits = resources
        .hugepage_limits
        .iter()
        .map(|limit| crate::proto::runtime::v1::HugepageLimit {
            page_size: limit.page_size.clone(),
            limit: limit.limit,
        })
        .collect();
    result.unified = resources.unified.clone();

    result
}

pub fn oci_mounts(spec: &Spec) -> Vec<nri_api::Mount> {
    spec.mounts
        .as_ref()
        .into_iter()
        .flatten()
        .map(|mount| {
            let mut result = nri_api::Mount::new();
            result.destination = mount.destination.clone();
            result.type_ = mount.mount_type.clone().unwrap_or_default();
            result.source = mount.source.clone().unwrap_or_default();
            result.options = mount.options.clone().unwrap_or_default();
            result
        })
        .collect()
}

fn oci_hook(hook: &crate::oci::spec::Hook) -> nri_api::Hook {
    let mut result = nri_api::Hook::new();
    result.path = hook.path.clone();
    result.args = hook.args.clone().unwrap_or_default();
    result.env = hook.env.clone().unwrap_or_default();
    if let Some(timeout) = hook.timeout {
        result.timeout = optional_int(timeout as i64);
    }
    result
}

pub fn oci_hooks(spec: &Spec) -> Option<nri_api::Hooks> {
    let hooks = spec.hooks.as_ref()?;
    let mut result = nri_api::Hooks::new();
    result.prestart = hooks
        .prestart
        .as_ref()
        .into_iter()
        .flatten()
        .map(oci_hook)
        .collect();
    result.create_runtime = hooks
        .create_runtime
        .as_ref()
        .into_iter()
        .flatten()
        .map(oci_hook)
        .collect();
    result.create_container = hooks
        .create_container
        .as_ref()
        .into_iter()
        .flatten()
        .map(oci_hook)
        .collect();
    result.start_container = hooks
        .start_container
        .as_ref()
        .into_iter()
        .flatten()
        .map(oci_hook)
        .collect();
    result.poststart = hooks
        .poststart
        .as_ref()
        .into_iter()
        .flatten()
        .map(oci_hook)
        .collect();
    result.poststop = hooks
        .poststop
        .as_ref()
        .into_iter()
        .flatten()
        .map(oci_hook)
        .collect();
    Some(result)
}

fn oci_seccomp(seccomp: &Seccomp) -> nri_api::LinuxSeccomp {
    let mut result = nri_api::LinuxSeccomp::new();
    result.default_action = seccomp.default_action.clone();
    if let Some(default_errno_ret) = seccomp.default_errno_ret {
        result.default_errno = optional_uint32(default_errno_ret);
    }
    result.architectures = seccomp.architectures.clone().unwrap_or_default();
    result.flags = seccomp.flags.clone().unwrap_or_default();
    result.listener_path = seccomp.listener_path.clone().unwrap_or_default();
    result.listener_metadata = seccomp.listener_metadata.clone().unwrap_or_default();
    result.syscalls = seccomp
        .syscalls
        .as_ref()
        .into_iter()
        .flatten()
        .map(|syscall| {
            let mut item = nri_api::LinuxSyscall::new();
            item.names = syscall.names.clone();
            item.action = syscall.action.clone();
            if let Some(errno_ret) = syscall.errno_ret {
                item.errno_ret = optional_uint32(errno_ret);
            }
            item.args = syscall
                .args
                .as_ref()
                .into_iter()
                .flatten()
                .map(|arg| {
                    let mut result = nri_api::LinuxSeccompArg::new();
                    result.index = arg.index;
                    result.value = arg.value;
                    result.value_two = arg.value_two.unwrap_or_default();
                    result.op = arg.op.clone();
                    result
                })
                .collect();
            item
        })
        .collect();
    result
}

pub fn oci_linux_container(spec: &Spec) -> Option<nri_api::LinuxContainer> {
    let linux = spec.linux.as_ref()?;
    let mut result = nri_api::LinuxContainer::new();
    result.namespaces = linux
        .namespaces
        .as_ref()
        .into_iter()
        .flatten()
        .map(|namespace| {
            let mut item = nri_api::LinuxNamespace::new();
            item.type_ = namespace.ns_type.clone();
            item.path = namespace.path.clone().unwrap_or_default();
            item
        })
        .collect();
    result.devices = linux
        .devices
        .as_ref()
        .into_iter()
        .flatten()
        .map(|device| {
            let mut item = nri_api::LinuxDevice::new();
            item.path = device.path.clone();
            item.type_ = device.device_type.clone();
            item.major = device.major.unwrap_or_default();
            item.minor = device.minor.unwrap_or_default();
            if let Some(file_mode) = device.file_mode {
                let mut value = nri_api::OptionalFileMode::new();
                value.value = file_mode;
                item.file_mode = MessageField::some(value);
            }
            if let Some(uid) = device.uid {
                item.uid = optional_uint32(uid);
            }
            if let Some(gid) = device.gid {
                item.gid = optional_uint32(gid);
            }
            item
        })
        .collect();

    if let Some(resources) = linux.resources.as_ref() {
        let mut nri_resources = nri_api::LinuxResources::new();
        if let Some(memory) = resources.memory.as_ref() {
            nri_resources.memory = MessageField::some(linux_memory(memory));
        }
        if let Some(cpu) = resources.cpu.as_ref() {
            let mut converted_cpu = nri_api::LinuxCPU::new();
            if let Some(shares) = cpu.shares {
                converted_cpu.shares = optional_uint64(shares);
            }
            if let Some(quota) = cpu.quota {
                converted_cpu.quota = optional_int64(quota);
            }
            if let Some(period) = cpu.period {
                converted_cpu.period = optional_uint64(period);
            }
            if let Some(realtime_runtime) = cpu.realtime_runtime {
                converted_cpu.realtime_runtime = optional_int64(realtime_runtime);
            }
            if let Some(realtime_period) = cpu.realtime_period {
                converted_cpu.realtime_period = optional_uint64(realtime_period);
            }
            converted_cpu.cpus = cpu.cpus.clone().unwrap_or_default();
            converted_cpu.mems = cpu.mems.clone().unwrap_or_default();
            nri_resources.cpu = MessageField::some(converted_cpu);
        }
        nri_resources.hugepage_limits = resources
            .hugepage_limits
            .as_ref()
            .into_iter()
            .flatten()
            .map(|limit| {
                let mut item = nri_api::HugepageLimit::new();
                item.page_size = limit.page_size.clone();
                item.limit = limit.limit;
                item
            })
            .collect();
        nri_resources.devices = resources
            .devices
            .as_ref()
            .into_iter()
            .flatten()
            .map(|device| {
                let mut item = nri_api::LinuxDeviceCgroup::new();
                item.allow = device.allow;
                item.type_ = device.device_type.clone().unwrap_or_default();
                if let Some(major) = device.major {
                    item.major = optional_int64(major);
                }
                if let Some(minor) = device.minor {
                    item.minor = optional_int64(minor);
                }
                item.access = device.access.clone().unwrap_or_default();
                item
            })
            .collect();
        nri_resources.unified = resources.unified.clone().unwrap_or_default();
        if let Some(pids) = resources.pids.as_ref() {
            let mut item = nri_api::LinuxPids::new();
            item.limit = pids.limit;
            nri_resources.pids = MessageField::some(item);
        }
        result.resources = MessageField::some(nri_resources);
    }

    if let Some(oom_score_adj) = spec
        .process
        .as_ref()
        .and_then(|process| process.oom_score_adj)
    {
        result.oom_score_adj = optional_int(oom_score_adj as i64);
    }
    if let Some(io_priority) = spec
        .process
        .as_ref()
        .and_then(|process| process.io_priority.as_ref())
    {
        let mut value = nri_api::LinuxIOPriority::new();
        value.class = io_prio_class(&io_priority.class).into();
        value.priority = io_priority.priority;
        result.io_priority = MessageField::some(value);
    }
    if let Some(scheduler) = spec
        .process
        .as_ref()
        .and_then(|process| process.scheduler.as_ref())
    {
        let mut value = nri_api::LinuxScheduler::new();
        value.policy = scheduler_policy(&scheduler.policy).into();
        value.nice = scheduler.nice.unwrap_or_default();
        value.priority = scheduler.priority.unwrap_or_default();
        value.flags = scheduler
            .flags
            .as_ref()
            .into_iter()
            .flatten()
            .filter_map(|flag| scheduler_flag(flag).map(Into::into))
            .collect();
        value.runtime = scheduler.runtime.unwrap_or_default();
        value.deadline = scheduler.deadline.unwrap_or_default();
        value.period = scheduler.period.unwrap_or_default();
        result.scheduler = MessageField::some(value);
    }
    result.cgroups_path = linux.cgroups_path.clone().unwrap_or_default();
    if let Some(seccomp) = linux.seccomp.as_ref() {
        result.seccomp_policy = MessageField::some(oci_seccomp(seccomp));
    }
    result.sysctl = linux.sysctl.clone().unwrap_or_default();
    result.net_devices = linux
        .net_devices
        .as_ref()
        .map(|devices| {
            devices
                .iter()
                .map(|(host, device)| {
                    let mut value = nri_api::LinuxNetDevice::new();
                    value.name = device.name.clone();
                    (host.clone(), value)
                })
                .collect()
        })
        .unwrap_or_default();
    if let Some(intel_rdt) = linux.intel_rdt.as_ref() {
        let mut rdt = nri_api::LinuxRdt::new();
        if let Some(clos_id) = intel_rdt.clos_id.as_ref() {
            rdt.clos_id = optional_string(clos_id.clone());
        }
        let mut schemata = Vec::new();
        if let Some(value) = intel_rdt.l3_cache_schema.as_ref() {
            schemata.push(format!("L3:{value}"));
        }
        if let Some(value) = intel_rdt.mem_bw_schema.as_ref() {
            schemata.push(format!("MB:{value}"));
        }
        if !schemata.is_empty() {
            rdt.schemata = optional_repeated_string(schemata);
        }
        match (intel_rdt.enable_cmt, intel_rdt.enable_mbm) {
            (Some(enable_cmt), Some(enable_mbm)) if enable_cmt == enable_mbm => {
                rdt.enable_monitoring = optional_bool(enable_cmt);
            }
            _ => {}
        }
        result.rdt = MessageField::some(rdt);
    }
    Some(result)
}

pub fn oci_args(spec: &Spec) -> Vec<String> {
    spec.process
        .as_ref()
        .map(|process| process.args.clone())
        .unwrap_or_default()
}

pub fn oci_env(spec: &Spec) -> Vec<String> {
    spec.process
        .as_ref()
        .and_then(|process| process.env.clone())
        .unwrap_or_default()
}

pub fn oci_rlimits(spec: &Spec) -> Vec<nri_api::POSIXRlimit> {
    spec.process
        .as_ref()
        .and_then(|process| process.rlimits.as_ref())
        .into_iter()
        .flatten()
        .map(|rlimit| {
            let mut item = nri_api::POSIXRlimit::new();
            item.type_ = rlimit.rtype.clone();
            item.hard = rlimit.hard;
            item.soft = rlimit.soft;
            item
        })
        .collect()
}

pub fn oci_user(spec: &Spec) -> Option<nri_api::User> {
    let user = spec.process.as_ref()?.user.as_ref()?;
    let mut result = nri_api::User::new();
    result.uid = user.uid;
    result.gid = user.gid;
    result.additional_gids = user.additional_gids.clone().unwrap_or_default();
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::{
        cri_linux_resources_from_nri, external_annotations, linux_resources_from_cri,
        oci_linux_container,
    };
    use std::collections::HashMap;

    use protobuf::MessageField;

    use crate::nri_proto::api as nri_api;
    use crate::oci::spec::{Linux, LinuxIntelRdt, Spec};
    use crate::proto::runtime::v1::LinuxContainerResources;

    #[test]
    fn hides_internal_annotations() {
        let mut annotations = HashMap::new();
        annotations.insert("io.crius.internal/state".to_string(), "x".to_string());
        annotations.insert("k8s.io/name".to_string(), "nginx".to_string());

        let filtered = external_annotations(&annotations);
        assert!(!filtered.contains_key("io.crius.internal/state"));
        assert_eq!(filtered.get("k8s.io/name"), Some(&"nginx".to_string()));
    }

    #[test]
    fn converts_cri_linux_resources() {
        let resources = LinuxContainerResources {
            cpu_period: 100_000,
            cpu_quota: 50_000,
            cpu_shares: 512,
            memory_limit_in_bytes: 1024,
            oom_score_adj: 0,
            cpuset_cpus: "0-1".to_string(),
            cpuset_mems: "0".to_string(),
            hugepage_limits: Vec::new(),
            unified: HashMap::from([("memory.high".to_string(), "512".to_string())]),
            memory_swap_limit_in_bytes: 2048,
        };

        let converted = linux_resources_from_cri(&resources);
        assert_eq!(
            converted
                .cpu
                .as_ref()
                .and_then(|cpu| cpu.quota.as_ref())
                .map(|q| q.value),
            Some(50_000)
        );
        assert_eq!(
            converted
                .memory
                .as_ref()
                .and_then(|memory| memory.limit.as_ref())
                .map(|limit| limit.value),
            Some(1024)
        );
        assert_eq!(
            converted.unified.get("memory.high"),
            Some(&"512".to_string())
        );
    }

    #[test]
    fn converts_nri_linux_resources() {
        let mut resources = nri_api::LinuxResources::new();
        let mut cpu = nri_api::LinuxCPU::new();
        let mut shares = nri_api::OptionalUInt64::new();
        shares.value = 512;
        cpu.shares = MessageField::some(shares);
        cpu.cpus = "0-1".to_string();
        resources.cpu = MessageField::some(cpu);

        let mut memory = nri_api::LinuxMemory::new();
        let mut limit = nri_api::OptionalInt64::new();
        limit.value = 4096;
        memory.limit = MessageField::some(limit);
        resources.memory = MessageField::some(memory);
        resources.unified = HashMap::from([("memory.high".to_string(), "1024".to_string())]);

        let converted = cri_linux_resources_from_nri(&resources);
        assert_eq!(converted.cpu_shares, 512);
        assert_eq!(converted.cpuset_cpus, "0-1");
        assert_eq!(converted.memory_limit_in_bytes, 4096);
        assert_eq!(
            converted.unified.get("memory.high"),
            Some(&"1024".to_string())
        );
    }

    #[test]
    fn converts_oci_intel_rdt_to_nri_linux_container() {
        let spec = Spec {
            oci_version: "1.0.2".to_string(),
            process: None,
            root: None,
            hostname: None,
            mounts: None,
            hooks: None,
            linux: Some(Linux {
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
                intel_rdt: Some(LinuxIntelRdt {
                    clos_id: Some("gold".to_string()),
                    l3_cache_schema: Some("0=ff".to_string()),
                    mem_bw_schema: Some("0=70".to_string()),
                    enable_cmt: Some(true),
                    enable_mbm: Some(true),
                }),
            }),
            annotations: None,
        };

        let linux = oci_linux_container(&spec).expect("linux container should be present");
        let rdt = linux.rdt.into_option().expect("rdt should be present");
        assert_eq!(
            rdt.clos_id.as_ref().map(|value| value.value.as_str()),
            Some("gold")
        );
        assert_eq!(
            rdt.schemata.as_ref().map(|value| value.value.clone()),
            Some(vec!["L3:0=ff".to_string(), "MB:0=70".to_string()])
        );
        assert_eq!(
            rdt.enable_monitoring.as_ref().map(|value| value.value),
            Some(true)
        );
    }
}
