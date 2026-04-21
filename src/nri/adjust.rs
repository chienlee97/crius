use std::collections::HashMap;

use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;
use crate::oci::spec::{
    Device, Hook, Hooks, Linux, LinuxCpu, LinuxDeviceCgroup, LinuxHugepageLimit, LinuxMemory, LinuxPids,
    LinuxResources, Mount, Namespace, Process, Rlimit, Seccomp, SeccompArg, SeccompSyscall, Spec,
};

const REMOVAL_PREFIX: &str = "-";
const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";
const CHECKPOINT_LOCATION_ANNOTATION_KEY: &str = "io.crius.checkpoint.location";

fn is_marked_for_removal(key: &str) -> Option<&str> {
    key.strip_prefix(REMOVAL_PREFIX)
}

fn adjusted_key(key: &str) -> &str {
    is_marked_for_removal(key).unwrap_or(key)
}

fn is_protected_annotation_key(key: &str) -> bool {
    key.starts_with(INTERNAL_ANNOTATION_PREFIX) || key == CHECKPOINT_LOCATION_ANNOTATION_KEY
}

fn reject_unsupported(field: &str) -> Result<()> {
    Err(NriError::Plugin(format!(
        "container adjustment uses unsupported field {field}"
    )))
}

fn validate_adjustment_resources(resources: &nri_api::LinuxResources) -> Result<()> {
    if resources.blockio_class.is_some() {
        return reject_unsupported("linux.resources.blockio_class");
    }
    if resources.rdt_class.is_some() {
        return reject_unsupported("linux.resources.rdt_class");
    }
    for hugepage in &resources.hugepage_limits {
        if hugepage.page_size.is_empty() {
            return Err(NriError::Plugin(
                "container adjustment hugepage limit must set page_size".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_container_adjustment(adjustment: &nri_api::ContainerAdjustment) -> Result<()> {
    for key in adjustment.annotations.keys() {
        let name = adjusted_key(key);
        if name.is_empty() {
            return Err(NriError::Plugin(
                "container adjustment annotation key must not be empty".to_string(),
            ));
        }
        if is_protected_annotation_key(name) {
            return Err(NriError::Plugin(format!(
                "container adjustment cannot modify protected annotation '{name}'"
            )));
        }
    }

    for mount in &adjustment.mounts {
        if adjusted_key(&mount.destination).is_empty() {
            return Err(NriError::Plugin(
                "container adjustment mount destination must not be empty".to_string(),
            ));
        }
    }

    for env in &adjustment.env {
        if adjusted_key(&env.key).is_empty() {
            return Err(NriError::Plugin(
                "container adjustment environment key must not be empty".to_string(),
            ));
        }
    }

    for rlimit in &adjustment.rlimits {
        if rlimit.type_.is_empty() {
            return Err(NriError::Plugin(
                "container adjustment rlimit type must not be empty".to_string(),
            ));
        }
    }

    if !adjustment.CDI_devices.is_empty() {
        return reject_unsupported("CDI_devices");
    }

    if let Some(linux) = adjustment.linux.as_ref() {
        for device in &linux.devices {
            if adjusted_key(&device.path).is_empty() {
                return Err(NriError::Plugin(
                    "container adjustment device path must not be empty".to_string(),
                ));
            }
        }
        if let Some(resources) = linux.resources.as_ref() {
            validate_adjustment_resources(resources)?;
        }
        if linux.io_priority.is_some() {
            return reject_unsupported("linux.io_priority");
        }
        for namespace in &linux.namespaces {
            if adjusted_key(&namespace.type_).is_empty() {
                return Err(NriError::Plugin(
                    "container adjustment namespace type must not be empty".to_string(),
                ));
            }
        }
        for key in linux.sysctl.keys() {
            if adjusted_key(key).is_empty() {
                return Err(NriError::Plugin(
                    "container adjustment sysctl key must not be empty".to_string(),
                ));
            }
        }
        if !linux.net_devices.is_empty() {
            return reject_unsupported("linux.net_devices");
        }
        if linux.scheduler.is_some() {
            return reject_unsupported("linux.scheduler");
        }
        if linux.rdt.is_some() {
            return reject_unsupported("linux.rdt");
        }
    }

    Ok(())
}

pub fn validate_update_linux_resources(resources: &nri_api::LinuxResources) -> Result<()> {
    if let Some(memory) = resources.memory.as_ref() {
        if memory.reservation.is_some() {
            return reject_unsupported("linux.resources.memory.reservation");
        }
        if memory.kernel.is_some() {
            return reject_unsupported("linux.resources.memory.kernel");
        }
        if memory.kernel_tcp.is_some() {
            return reject_unsupported("linux.resources.memory.kernel_tcp");
        }
        if memory.swappiness.is_some() {
            return reject_unsupported("linux.resources.memory.swappiness");
        }
        if memory.disable_oom_killer.is_some() {
            return reject_unsupported("linux.resources.memory.disable_oom_killer");
        }
        if memory.use_hierarchy.is_some() {
            return reject_unsupported("linux.resources.memory.use_hierarchy");
        }
    }
    if let Some(cpu) = resources.cpu.as_ref() {
        if cpu.realtime_runtime.is_some() {
            return reject_unsupported("linux.resources.cpu.realtime_runtime");
        }
        if cpu.realtime_period.is_some() {
            return reject_unsupported("linux.resources.cpu.realtime_period");
        }
    }
    for hugepage in &resources.hugepage_limits {
        if hugepage.page_size.is_empty() {
            return Err(NriError::Plugin(
                "container update hugepage limit must set page_size".to_string(),
            ));
        }
    }
    if resources.blockio_class.is_some() {
        return reject_unsupported("linux.resources.blockio_class");
    }
    if resources.rdt_class.is_some() {
        return reject_unsupported("linux.resources.rdt_class");
    }
    if !resources.devices.is_empty() {
        return reject_unsupported("linux.resources.devices");
    }
    if resources.pids.is_some() {
        return reject_unsupported("linux.resources.pids");
    }
    Ok(())
}

pub fn validate_container_update(update: &nri_api::ContainerUpdate) -> Result<()> {
    if update.container_id.is_empty() {
        return Err(NriError::InvalidInput(
            "container update is missing container_id".to_string(),
        ));
    }
    let linux = update.linux.as_ref().ok_or_else(|| {
        NriError::InvalidInput(format!(
            "container {} update is missing linux payload",
            update.container_id
        ))
    })?;
    let resources = linux.resources.as_ref().ok_or_else(|| {
        NriError::InvalidInput(format!(
            "container {} update is missing linux resources",
            update.container_id
        ))
    })?;
    validate_update_linux_resources(resources)
}

fn ensure_process(spec: &mut Spec) -> &mut Process {
    spec.process.get_or_insert_with(|| Process {
        terminal: None,
        user: None,
        args: Vec::new(),
        env: None,
        cwd: "/".to_string(),
        capabilities: None,
        rlimits: None,
        oom_score_adj: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
    })
}

fn ensure_linux(spec: &mut Spec) -> &mut Linux {
    spec.linux.get_or_insert_with(|| Linux {
        namespaces: None,
        uid_mappings: None,
        gid_mappings: None,
        devices: None,
        cgroups_path: None,
        resources: None,
        rootfs_propagation: None,
        seccomp: None,
        sysctl: None,
        mount_label: None,
        intel_rdt: None,
    })
}

fn ensure_linux_resources(spec: &mut Spec) -> &mut LinuxResources {
    ensure_linux(spec).resources.get_or_insert_with(|| LinuxResources {
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
}

fn env_name(entry: &str) -> &str {
    entry.split_once('=').map(|(name, _)| name).unwrap_or(entry)
}

fn cleanup_vec<T>(entries: &mut Option<Vec<T>>) {
    if entries.as_ref().is_some_and(Vec::is_empty) {
        *entries = None;
    }
}

fn cleanup_map<K, V>(entries: &mut Option<HashMap<K, V>>) {
    if entries.as_ref().is_some_and(HashMap::is_empty) {
        *entries = None;
    }
}

fn apply_env_adjustments(spec: &mut Spec, adjustments: &[nri_api::KeyValue]) {
    if adjustments.is_empty() {
        return;
    }

    let env = &mut ensure_process(spec).env;
    let entries = env.get_or_insert_with(Vec::new);
    for entry in adjustments {
        if let Some(name) = is_marked_for_removal(&entry.key) {
            entries.retain(|existing| env_name(existing) != name);
            continue;
        }

        let rendered = format!("{}={}", entry.key, entry.value);
        if let Some(existing) = entries.iter_mut().find(|current| env_name(current) == entry.key) {
            *existing = rendered;
        } else {
            entries.push(rendered);
        }
    }
    cleanup_vec(env);
}

fn apply_mount_adjustments(spec: &mut Spec, adjustments: &[nri_api::Mount]) {
    if adjustments.is_empty() {
        return;
    }

    let mounts = spec.mounts.get_or_insert_with(Vec::new);
    for mount in adjustments {
        if let Some(destination) = is_marked_for_removal(&mount.destination) {
            mounts.retain(|existing| existing.destination != destination);
            continue;
        }

        let updated = Mount {
            destination: mount.destination.clone(),
            source: (!mount.source.is_empty()).then(|| mount.source.clone()),
            mount_type: (!mount.type_.is_empty()).then(|| mount.type_.clone()),
            options: (!mount.options.is_empty()).then(|| mount.options.clone()),
        };
        if let Some(existing) = mounts
            .iter_mut()
            .find(|current| current.destination == updated.destination)
        {
            *existing = updated;
        } else {
            mounts.push(updated);
        }
    }
    cleanup_vec(&mut spec.mounts);
}

fn convert_hook(hook: &nri_api::Hook) -> Hook {
    Hook {
        path: hook.path.clone(),
        args: (!hook.args.is_empty()).then(|| hook.args.clone()),
        env: (!hook.env.is_empty()).then(|| hook.env.clone()),
        timeout: hook.timeout.as_ref().map(|value| value.value as i32),
    }
}

fn extend_hook_list(target: &mut Option<Vec<Hook>>, source: &[nri_api::Hook]) {
    if source.is_empty() {
        return;
    }
    target
        .get_or_insert_with(Vec::new)
        .extend(source.iter().map(convert_hook));
}

fn apply_hook_adjustments(spec: &mut Spec, hooks: Option<&nri_api::Hooks>) {
    let Some(adjustments) = hooks else {
        return;
    };

    let hooks = spec.hooks.get_or_insert_with(|| Hooks {
        prestart: None,
        create_runtime: None,
        create_container: None,
        start_container: None,
        poststart: None,
        poststop: None,
    });
    extend_hook_list(&mut hooks.prestart, &adjustments.prestart);
    extend_hook_list(&mut hooks.create_runtime, &adjustments.create_runtime);
    extend_hook_list(&mut hooks.create_container, &adjustments.create_container);
    extend_hook_list(&mut hooks.start_container, &adjustments.start_container);
    extend_hook_list(&mut hooks.poststart, &adjustments.poststart);
    extend_hook_list(&mut hooks.poststop, &adjustments.poststop);
}

fn convert_device(device: &nri_api::LinuxDevice) -> Device {
    Device {
        device_type: device.type_.clone(),
        path: device.path.clone(),
        major: Some(device.major),
        minor: Some(device.minor),
        file_mode: device.file_mode.as_ref().map(|value| value.value),
        uid: device.uid.as_ref().map(|value| value.value),
        gid: device.gid.as_ref().map(|value| value.value),
    }
}

fn apply_linux_devices(spec: &mut Spec, devices: &[nri_api::LinuxDevice]) {
    if devices.is_empty() {
        return;
    }

    let oci_devices = &mut ensure_linux(spec).devices;
    let entries = oci_devices.get_or_insert_with(Vec::new);
    for device in devices {
        if let Some(path) = is_marked_for_removal(&device.path) {
            entries.retain(|existing| existing.path != path);
            continue;
        }
        let updated = convert_device(device);
        if let Some(existing) = entries.iter_mut().find(|current| current.path == updated.path) {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
    cleanup_vec(oci_devices);
}

fn apply_linux_memory(resources: &mut LinuxResources, memory: &nri_api::LinuxMemory) {
    let target = resources.memory.get_or_insert_with(|| LinuxMemory {
        limit: None,
        swap: None,
        kernel: None,
        kernel_tcp: None,
        reservation: None,
        swappiness: None,
        disable_oom_killer: None,
        use_hierarchy: None,
    });
    if let Some(limit) = memory.limit.as_ref() {
        target.limit = Some(limit.value);
    }
    if let Some(reservation) = memory.reservation.as_ref() {
        target.reservation = Some(reservation.value);
    }
    if let Some(swap) = memory.swap.as_ref() {
        target.swap = Some(swap.value);
    }
    if let Some(kernel) = memory.kernel.as_ref() {
        target.kernel = Some(kernel.value);
    }
    if let Some(kernel_tcp) = memory.kernel_tcp.as_ref() {
        target.kernel_tcp = Some(kernel_tcp.value);
    }
    if let Some(swappiness) = memory.swappiness.as_ref() {
        target.swappiness = Some(swappiness.value);
    }
    if let Some(disable_oom_killer) = memory.disable_oom_killer.as_ref() {
        target.disable_oom_killer = Some(disable_oom_killer.value);
    }
    if let Some(use_hierarchy) = memory.use_hierarchy.as_ref() {
        target.use_hierarchy = Some(use_hierarchy.value);
    }
}

fn apply_linux_cpu(resources: &mut LinuxResources, cpu: &nri_api::LinuxCPU) {
    let target = resources.cpu.get_or_insert_with(|| LinuxCpu {
        shares: None,
        quota: None,
        period: None,
        realtime_runtime: None,
        realtime_period: None,
        cpus: None,
        mems: None,
    });
    if let Some(shares) = cpu.shares.as_ref() {
        target.shares = Some(shares.value);
    }
    if let Some(quota) = cpu.quota.as_ref() {
        target.quota = Some(quota.value);
    }
    if let Some(period) = cpu.period.as_ref() {
        target.period = Some(period.value);
    }
    if let Some(realtime_runtime) = cpu.realtime_runtime.as_ref() {
        target.realtime_runtime = Some(realtime_runtime.value);
    }
    if let Some(realtime_period) = cpu.realtime_period.as_ref() {
        target.realtime_period = Some(realtime_period.value);
    }
    if !cpu.cpus.is_empty() {
        target.cpus = Some(cpu.cpus.clone());
    }
    if !cpu.mems.is_empty() {
        target.mems = Some(cpu.mems.clone());
    }
}

fn convert_device_cgroup(device: &nri_api::LinuxDeviceCgroup) -> LinuxDeviceCgroup {
    LinuxDeviceCgroup {
        allow: device.allow,
        device_type: (!device.type_.is_empty()).then(|| device.type_.clone()),
        major: device.major.as_ref().map(|value| value.value),
        minor: device.minor.as_ref().map(|value| value.value),
        access: (!device.access.is_empty()).then(|| device.access.clone()),
    }
}

fn apply_hugepage_limits(resources: &mut LinuxResources, limits: &[nri_api::HugepageLimit]) {
    if limits.is_empty() {
        return;
    }

    let entries = resources.hugepage_limits.get_or_insert_with(Vec::new);
    for limit in limits {
        let updated = LinuxHugepageLimit {
            page_size: limit.page_size.clone(),
            limit: limit.limit,
        };
        if let Some(existing) = entries
            .iter_mut()
            .find(|current| current.page_size == updated.page_size)
        {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
}

fn apply_linux_resources(spec: &mut Spec, resources: Option<&nri_api::LinuxResources>) {
    let Some(adjustments) = resources else {
        return;
    };

    let target = ensure_linux_resources(spec);
    if let Some(memory) = adjustments.memory.as_ref() {
        apply_linux_memory(target, memory);
    }
    if let Some(cpu) = adjustments.cpu.as_ref() {
        apply_linux_cpu(target, cpu);
    }
    apply_hugepage_limits(target, &adjustments.hugepage_limits);
    if !adjustments.devices.is_empty() {
        target.devices = Some(adjustments.devices.iter().map(convert_device_cgroup).collect());
    }
    if let Some(pids) = adjustments.pids.as_ref() {
        target.pids = Some(LinuxPids { limit: pids.limit });
    }
    if !adjustments.unified.is_empty() {
        target
            .unified
            .get_or_insert_with(HashMap::new)
            .extend(adjustments.unified.clone());
    }
}

fn convert_namespace(namespace: &nri_api::LinuxNamespace) -> Namespace {
    Namespace {
        ns_type: namespace.type_.clone(),
        path: (!namespace.path.is_empty()).then(|| namespace.path.clone()),
    }
}

fn apply_linux_namespaces(spec: &mut Spec, namespaces: &[nri_api::LinuxNamespace]) {
    if namespaces.is_empty() {
        return;
    }

    let oci_namespaces = &mut ensure_linux(spec).namespaces;
    let entries = oci_namespaces.get_or_insert_with(Vec::new);
    for namespace in namespaces {
        if let Some(ns_type) = is_marked_for_removal(&namespace.type_) {
            entries.retain(|existing| existing.ns_type != ns_type);
            continue;
        }
        let updated = convert_namespace(namespace);
        if let Some(existing) = entries.iter_mut().find(|current| current.ns_type == updated.ns_type) {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
    cleanup_vec(oci_namespaces);
}

fn convert_seccomp(seccomp: &nri_api::LinuxSeccomp) -> Seccomp {
    Seccomp {
        default_action: seccomp.default_action.clone(),
        default_errno_ret: seccomp.default_errno.as_ref().map(|value| value.value),
        architectures: (!seccomp.architectures.is_empty()).then(|| seccomp.architectures.clone()),
        flags: (!seccomp.flags.is_empty()).then(|| seccomp.flags.clone()),
        listener_path: (!seccomp.listener_path.is_empty()).then(|| seccomp.listener_path.clone()),
        listener_metadata: (!seccomp.listener_metadata.is_empty()).then(|| seccomp.listener_metadata.clone()),
        syscalls: (!seccomp.syscalls.is_empty()).then(|| {
            seccomp
                .syscalls
                .iter()
                .map(|syscall| SeccompSyscall {
                    action: syscall.action.clone(),
                    names: syscall.names.clone(),
                    args: (!syscall.args.is_empty()).then(|| {
                        syscall
                            .args
                            .iter()
                            .map(|arg| SeccompArg {
                                index: arg.index,
                                value: arg.value,
                                value_two: (arg.value_two != 0).then_some(arg.value_two),
                                op: arg.op.clone(),
                            })
                            .collect()
                    }),
                    errno_ret: syscall.errno_ret.as_ref().map(|value| value.value),
                })
                .collect()
        }),
    }
}

fn apply_linux_adjustments(spec: &mut Spec, linux: Option<&nri_api::LinuxContainerAdjustment>) {
    let Some(adjustments) = linux else {
        return;
    };

    apply_linux_devices(spec, &adjustments.devices);
    apply_linux_resources(spec, adjustments.resources.as_ref());
    if !adjustments.cgroups_path.is_empty() {
        ensure_linux(spec).cgroups_path = Some(adjustments.cgroups_path.clone());
    }
    if let Some(oom_score_adj) = adjustments.oom_score_adj.as_ref() {
        ensure_process(spec).oom_score_adj = Some(oom_score_adj.value as i32);
    }
    if let Some(seccomp) = adjustments.seccomp_policy.as_ref() {
        ensure_linux(spec).seccomp = Some(convert_seccomp(seccomp));
    }
    apply_linux_namespaces(spec, &adjustments.namespaces);

    if !adjustments.sysctl.is_empty() {
        let sysctl = &mut ensure_linux(spec).sysctl;
        let entries = sysctl.get_or_insert_with(HashMap::new);
        for (key, value) in &adjustments.sysctl {
            if let Some(name) = is_marked_for_removal(key) {
                entries.remove(name);
            } else {
                entries.insert(key.clone(), value.clone());
            }
        }
        cleanup_map(sysctl);
    }
}

fn apply_rlimit_adjustments(spec: &mut Spec, adjustments: &[nri_api::POSIXRlimit]) {
    if adjustments.is_empty() {
        return;
    }

    let rlimits = &mut ensure_process(spec).rlimits;
    let entries = rlimits.get_or_insert_with(Vec::new);
    for rlimit in adjustments {
        let updated = Rlimit {
            rtype: rlimit.type_.clone(),
            hard: rlimit.hard,
            soft: rlimit.soft,
        };
        if let Some(existing) = entries.iter_mut().find(|current| current.rtype == updated.rtype) {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
    cleanup_vec(rlimits);
}

pub fn apply_annotation_adjustments(spec: &mut Spec, adjustments: &HashMap<String, String>) {
    if adjustments.is_empty() {
        return;
    }

    let annotations = spec.annotations.get_or_insert_with(HashMap::new);
    for (key, value) in adjustments {
        if let Some(name) = is_marked_for_removal(key) {
            annotations.remove(name);
        } else {
            annotations.insert(key.clone(), value.clone());
        }
    }
    cleanup_map(&mut spec.annotations);
}

pub fn apply_container_adjustment(spec: &mut Spec, adjustment: &nri_api::ContainerAdjustment) {
    apply_annotation_adjustments(spec, &adjustment.annotations);
    apply_mount_adjustments(spec, adjustment.mounts.as_slice());
    apply_env_adjustments(spec, adjustment.env.as_slice());
    apply_hook_adjustments(spec, adjustment.hooks.as_ref());
    apply_linux_adjustments(spec, adjustment.linux.as_ref());
    apply_rlimit_adjustments(spec, adjustment.rlimits.as_slice());

    if !adjustment.args.is_empty() {
        let process = ensure_process(spec);
        process.args = if adjustment.args.first().is_some_and(|arg| arg.is_empty()) {
            adjustment.args[1..].to_vec()
        } else {
            adjustment.args.clone()
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use protobuf::MessageField;

    use super::{
        apply_container_adjustment, validate_container_adjustment, validate_container_update,
        validate_update_linux_resources, REMOVAL_PREFIX,
    };
    use crate::nri_proto::api as nri_api;
    use crate::oci::spec::{
        Device, Linux, LinuxCpu, LinuxHugepageLimit, LinuxMemory, LinuxPids, LinuxResources, Mount, Namespace,
        Process, Rlimit, Spec,
    };

    fn removal(name: &str) -> String {
        format!("{REMOVAL_PREFIX}{name}")
    }

    fn opt_int(value: i64) -> MessageField<nri_api::OptionalInt> {
        let mut item = nri_api::OptionalInt::new();
        item.value = value;
        MessageField::some(item)
    }

    fn opt_int64(value: i64) -> MessageField<nri_api::OptionalInt64> {
        let mut item = nri_api::OptionalInt64::new();
        item.value = value;
        MessageField::some(item)
    }

    fn opt_uint32(value: u32) -> MessageField<nri_api::OptionalUInt32> {
        let mut item = nri_api::OptionalUInt32::new();
        item.value = value;
        MessageField::some(item)
    }

    fn opt_uint64(value: u64) -> MessageField<nri_api::OptionalUInt64> {
        let mut item = nri_api::OptionalUInt64::new();
        item.value = value;
        MessageField::some(item)
    }

    fn spec_with_process() -> Spec {
        Spec {
            oci_version: "1.1.0".to_string(),
            process: Some(Process {
                terminal: None,
                user: None,
                args: vec!["sleep".to_string(), "10".to_string()],
                env: Some(vec!["A=1".to_string(), "B=2".to_string()]),
                cwd: "/".to_string(),
                capabilities: None,
                rlimits: Some(vec![Rlimit {
                    rtype: "RLIMIT_NOFILE".to_string(),
                    hard: 1024,
                    soft: 512,
                }]),
                oom_score_adj: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
            }),
            root: None,
            hostname: None,
            mounts: Some(vec![Mount {
                destination: "/data".to_string(),
                source: Some("/old".to_string()),
                mount_type: Some("bind".to_string()),
                options: Some(vec!["ro".to_string()]),
            }]),
            hooks: None,
            linux: Some(Linux {
                namespaces: Some(vec![
                    Namespace {
                        ns_type: "pid".to_string(),
                        path: None,
                    },
                    Namespace {
                        ns_type: "network".to_string(),
                        path: None,
                    },
                ]),
                uid_mappings: None,
                gid_mappings: None,
                devices: Some(vec![Device {
                    device_type: "c".to_string(),
                    path: "/dev/null".to_string(),
                    major: Some(1),
                    minor: Some(3),
                    file_mode: Some(0o666),
                    uid: None,
                    gid: None,
                }]),
                cgroups_path: Some("/old/path".to_string()),
                resources: Some(LinuxResources {
                    network: None,
                    pids: Some(LinuxPids { limit: 64 }),
                    memory: Some(LinuxMemory {
                        limit: Some(1024),
                        swap: None,
                        kernel: None,
                        kernel_tcp: None,
                        reservation: None,
                        swappiness: None,
                        disable_oom_killer: None,
                        use_hierarchy: None,
                    }),
                    cpu: Some(LinuxCpu {
                        shares: Some(128),
                        quota: None,
                        period: None,
                        realtime_runtime: None,
                        realtime_period: None,
                        cpus: None,
                        mems: None,
                    }),
                    block_io: None,
                    hugepage_limits: Some(vec![LinuxHugepageLimit {
                        page_size: "2MB".to_string(),
                        limit: 1,
                    }]),
                    devices: None,
                    intel_rdt: None,
                    unified: Some(HashMap::from([("memory.high".to_string(), "128".to_string())])),
                }),
                rootfs_propagation: None,
                seccomp: None,
                sysctl: Some(HashMap::from([(
                    "net.ipv4.ip_forward".to_string(),
                    "0".to_string(),
                )])),
                mount_label: None,
                intel_rdt: None,
            }),
            annotations: Some(HashMap::from([
                ("keep".to_string(), "old".to_string()),
                ("drop".to_string(), "gone".to_string()),
            ])),
        }
    }

    #[test]
    fn applies_annotation_env_mount_hook_and_args_adjustments() {
        let mut spec = spec_with_process();
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment
            .annotations
            .insert("keep".to_string(), "new".to_string());
        adjustment
            .annotations
            .insert(removal("drop"), String::new());
        adjustment.env.push(nri_api::KeyValue {
            key: "B".to_string(),
            value: "3".to_string(),
            ..Default::default()
        });
        adjustment.env.push(nri_api::KeyValue {
            key: removal("A"),
            value: String::new(),
            ..Default::default()
        });
        adjustment.mounts.push(nri_api::Mount {
            destination: "/data".to_string(),
            source: "/new".to_string(),
            type_: "bind".to_string(),
            options: vec!["rw".to_string()],
            ..Default::default()
        });
        adjustment.mounts.push(nri_api::Mount {
            destination: removal("/tmp"),
            ..Default::default()
        });
        let mut hooks = nri_api::Hooks::new();
        hooks.prestart.push(nri_api::Hook {
            path: "/bin/hook".to_string(),
            args: vec!["hook".to_string()],
            ..Default::default()
        });
        adjustment.hooks = MessageField::some(hooks);
        adjustment.args = vec![String::new(), "/bin/echo".to_string(), "hi".to_string()];

        apply_container_adjustment(&mut spec, &adjustment);

        assert_eq!(spec.annotations.as_ref().unwrap().get("keep"), Some(&"new".to_string()));
        assert!(!spec.annotations.as_ref().unwrap().contains_key("drop"));
        assert_eq!(spec.process.as_ref().unwrap().env.as_ref().unwrap(), &vec!["B=3".to_string()]);
        let mount = spec.mounts.as_ref().unwrap().iter().find(|mount| mount.destination == "/data").unwrap();
        assert_eq!(mount.source.as_deref(), Some("/new"));
        assert_eq!(mount.options.as_ref().unwrap(), &vec!["rw".to_string()]);
        let hook = &spec.hooks.as_ref().unwrap().prestart.as_ref().unwrap()[0];
        assert_eq!(hook.path, "/bin/hook");
        assert_eq!(
            spec.process.as_ref().unwrap().args,
            vec!["/bin/echo".to_string(), "hi".to_string()]
        );
    }

    #[test]
    fn applies_linux_devices_resources_and_security_adjustments() {
        let mut spec = spec_with_process();
        let mut adjustment = nri_api::ContainerAdjustment::new();
        let mut linux = nri_api::LinuxContainerAdjustment::new();
        linux.devices.push(nri_api::LinuxDevice {
            path: "/dev/fuse".to_string(),
            type_: "c".to_string(),
            major: 10,
            minor: 229,
            file_mode: Some({
                let mut mode = nri_api::OptionalFileMode::new();
                mode.value = 0o660;
                mode
            })
            .into(),
            uid: opt_uint32(1000),
            gid: opt_uint32(1000),
            ..Default::default()
        });
        linux.devices.push(nri_api::LinuxDevice {
            path: removal("/dev/null"),
            ..Default::default()
        });

        let mut resources = nri_api::LinuxResources::new();
        let mut memory = nri_api::LinuxMemory::new();
        memory.limit = opt_int64(4096);
        memory.swap = opt_int64(8192);
        resources.memory = MessageField::some(memory);
        let mut cpu = nri_api::LinuxCPU::new();
        cpu.shares = opt_uint64(512);
        cpu.cpus = "0-1".to_string();
        resources.cpu = MessageField::some(cpu);
        resources.hugepage_limits.push(nri_api::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 4,
            ..Default::default()
        });
        resources.unified.insert("cpu.max".to_string(), "100000 100000".to_string());
        resources.pids = Some(nri_api::LinuxPids { limit: 128, ..Default::default() }).into();
        linux.resources = MessageField::some(resources);
        linux.cgroups_path = "/new/path".to_string();
        linux.oom_score_adj = opt_int(222);
        linux.sysctl.insert(removal("net.ipv4.ip_forward"), String::new());
        linux.sysctl.insert("vm.max_map_count".to_string(), "262144".to_string());
        linux.namespaces.push(nri_api::LinuxNamespace {
            type_: "pid".to_string(),
            path: "/proc/1/ns/pid".to_string(),
            ..Default::default()
        });
        linux.namespaces.push(nri_api::LinuxNamespace {
            type_: removal("network"),
            ..Default::default()
        });
        let mut seccomp = nri_api::LinuxSeccomp::new();
        seccomp.default_action = "SCMP_ACT_ERRNO".to_string();
        seccomp.architectures = vec!["SCMP_ARCH_X86_64".to_string()];
        seccomp.syscalls.push(nri_api::LinuxSyscall {
            names: vec!["clone3".to_string()],
            action: "SCMP_ACT_ALLOW".to_string(),
            errno_ret: opt_uint32(1),
            args: vec![nri_api::LinuxSeccompArg {
                index: 0,
                value: 1,
                value_two: 2,
                op: "SCMP_CMP_EQ".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        });
        linux.seccomp_policy = MessageField::some(seccomp);
        adjustment.linux = MessageField::some(linux);

        apply_container_adjustment(&mut spec, &adjustment);

        let linux = spec.linux.as_ref().unwrap();
        assert!(linux.devices.as_ref().unwrap().iter().all(|device| device.path != "/dev/null"));
        let fuse = linux.devices.as_ref().unwrap().iter().find(|device| device.path == "/dev/fuse").unwrap();
        assert_eq!(fuse.major, Some(10));
        assert_eq!(linux.cgroups_path.as_deref(), Some("/new/path"));
        assert_eq!(spec.process.as_ref().unwrap().oom_score_adj, Some(222));
        assert_eq!(linux.sysctl.as_ref().unwrap().get("vm.max_map_count"), Some(&"262144".to_string()));
        assert!(!linux.sysctl.as_ref().unwrap().contains_key("net.ipv4.ip_forward"));
        assert!(linux.namespaces.as_ref().unwrap().iter().all(|ns| ns.ns_type != "network"));
        assert_eq!(
            linux
                .namespaces
                .as_ref()
                .unwrap()
                .iter()
                .find(|ns| ns.ns_type == "pid")
                .unwrap()
                .path
                .as_deref(),
            Some("/proc/1/ns/pid")
        );
        let resources = linux.resources.as_ref().unwrap();
        assert_eq!(resources.memory.as_ref().unwrap().limit, Some(4096));
        assert_eq!(resources.memory.as_ref().unwrap().swap, Some(8192));
        assert_eq!(resources.cpu.as_ref().unwrap().shares, Some(512));
        assert_eq!(resources.cpu.as_ref().unwrap().cpus.as_deref(), Some("0-1"));
        assert_eq!(resources.hugepage_limits.as_ref().unwrap()[0].limit, 4);
        assert_eq!(resources.pids.as_ref().unwrap().limit, 128);
        assert_eq!(
            resources.unified.as_ref().unwrap().get("cpu.max"),
            Some(&"100000 100000".to_string())
        );
        let seccomp = linux.seccomp.as_ref().unwrap();
        assert_eq!(seccomp.default_action, "SCMP_ACT_ERRNO");
        assert_eq!(seccomp.syscalls.as_ref().unwrap()[0].names, vec!["clone3".to_string()]);
    }

    #[test]
    fn applies_rlimit_updates_by_type() {
        let mut spec = spec_with_process();
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment.rlimits.push(nri_api::POSIXRlimit {
            type_: "RLIMIT_NOFILE".to_string(),
            hard: 4096,
            soft: 2048,
            ..Default::default()
        });
        adjustment.rlimits.push(nri_api::POSIXRlimit {
            type_: "RLIMIT_NPROC".to_string(),
            hard: 1024,
            soft: 1024,
            ..Default::default()
        });

        apply_container_adjustment(&mut spec, &adjustment);

        let rlimits = spec.process.as_ref().unwrap().rlimits.as_ref().unwrap();
        assert_eq!(rlimits.len(), 2);
        assert_eq!(
            rlimits.iter().find(|limit| limit.rtype == "RLIMIT_NOFILE").unwrap().hard,
            4096
        );
        assert_eq!(
            rlimits.iter().find(|limit| limit.rtype == "RLIMIT_NPROC").unwrap().soft,
            1024
        );
    }

    #[test]
    fn rejects_protected_annotations_in_adjustment() {
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment.annotations.insert(
            "io.crius.internal/container-state".to_string(),
            "forbidden".to_string(),
        );

        let err = validate_container_adjustment(&adjustment).unwrap_err();
        assert!(format!("{err}").contains("protected annotation"));
    }

    #[test]
    fn rejects_unsupported_adjustment_fields() {
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment.CDI_devices.push(nri_api::CDIDevice {
            name: "vendor.com/device".to_string(),
            ..Default::default()
        });

        let err = validate_container_adjustment(&adjustment).unwrap_err();
        assert!(format!("{err}").contains("unsupported field CDI_devices"));
    }

    #[test]
    fn rejects_unsupported_update_resources() {
        let mut resources = nri_api::LinuxResources::new();
        resources.pids = Some(nri_api::LinuxPids {
            limit: 12,
            ..Default::default()
        })
        .into();

        let err = validate_update_linux_resources(&resources).unwrap_err();
        assert!(format!("{err}").contains("unsupported field linux.resources.pids"));
    }

    #[test]
    fn rejects_update_without_linux_resources() {
        let mut update = nri_api::ContainerUpdate::new();
        update.container_id = "ctr-1".to_string();

        let err = validate_container_update(&update).unwrap_err();
        assert!(format!("{err}").contains("missing linux payload"));
    }
}
