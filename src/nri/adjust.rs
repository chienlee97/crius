use std::collections::HashMap;

use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;
use crate::oci::spec::{
    Device, Hook, Hooks, Linux, LinuxBlockIo, LinuxCpu, LinuxDeviceCgroup, LinuxHugepageLimit,
    LinuxMemory, LinuxPids, LinuxResources, Mount, Namespace, Process, Rlimit, Seccomp, SeccompArg,
    SeccompSyscall, Spec,
};

const REMOVAL_PREFIX: &str = "-";
const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";
const CHECKPOINT_LOCATION_ANNOTATION_KEY: &str = "io.crius.checkpoint.location";

#[derive(Debug, Clone)]
pub struct AdjustmentOptions<'a> {
    pub blockio_config_path: Option<&'a str>,
    pub cdi_enabled: bool,
    pub cdi_spec_dirs: Option<&'a [String]>,
}

impl Default for AdjustmentOptions<'_> {
    fn default() -> Self {
        Self {
            blockio_config_path: None,
            cdi_enabled: true,
            cdi_spec_dirs: None,
        }
    }
}

fn is_marked_for_removal(key: &str) -> Option<&str> {
    key.strip_prefix(REMOVAL_PREFIX)
}

fn adjusted_key(key: &str) -> &str {
    is_marked_for_removal(key).unwrap_or(key)
}

fn is_protected_annotation_key(key: &str) -> bool {
    key.starts_with(INTERNAL_ANNOTATION_PREFIX) || key == CHECKPOINT_LOCATION_ANNOTATION_KEY
}

fn normalized_annotation_key(key: &str) -> &str {
    adjusted_key(key).trim()
}

fn has_only_valid_device_access(access: &str) -> bool {
    !access.is_empty()
        && access
            .bytes()
            .all(|byte| matches!(byte, b'r' | b'w' | b'm'))
}

fn is_valid_device_type(device_type: &str, allow_all: bool) -> bool {
    matches!(device_type, "b" | "c" | "u" | "p") || (allow_all && device_type == "a")
}

fn is_annotation_allowed(key: &str, allowed_prefixes: &[String]) -> bool {
    allowed_prefixes
        .iter()
        .map(|prefix| prefix.trim())
        .filter(|prefix| !prefix.is_empty())
        .any(|prefix| key.starts_with(prefix))
}

pub fn disallowed_annotation_adjustment_keys(
    adjustments: &HashMap<String, String>,
    allowed_prefixes: &[String],
) -> Vec<String> {
    let mut disallowed = adjustments
        .keys()
        .filter_map(|key| {
            let name = normalized_annotation_key(key);
            (!name.is_empty()
                && !is_protected_annotation_key(name)
                && !is_annotation_allowed(name, allowed_prefixes))
            .then(|| key.clone())
        })
        .collect::<Vec<_>>();
    disallowed.sort();
    disallowed
}

pub fn filter_annotation_adjustments_by_allowlist(
    adjustments: &HashMap<String, String>,
    allowed_prefixes: &[String],
) -> HashMap<String, String> {
    adjustments
        .iter()
        .filter(|(key, _)| {
            let name = normalized_annotation_key(key);
            !name.is_empty()
                && !is_protected_annotation_key(name)
                && is_annotation_allowed(name, allowed_prefixes)
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub fn resolve_blockio_class(
    class_name: &str,
    config_path: Option<&str>,
) -> Result<Option<LinuxBlockIo>> {
    crate::security::resource_classes::resolve_blockio_class(class_name, config_path)
        .map_err(|err| NriError::Plugin(err.to_string()))
}

pub fn resolve_rdt_class(class_name: &str) -> Option<crate::oci::spec::LinuxIntelRdt> {
    crate::security::resource_classes::resolve_rdt_class(class_name)
}

fn validate_adjustment_resources(
    resources: &nri_api::LinuxResources,
    min_memory_limit: Option<i64>,
) -> Result<()> {
    if let Some(memory) = resources.memory.as_ref() {
        validate_linux_memory(memory, min_memory_limit, "container adjustment")?;
    }
    if let Some(cpu) = resources.cpu.as_ref() {
        validate_linux_cpu(cpu, "container adjustment")?;
    }
    for hugepage in &resources.hugepage_limits {
        if hugepage.page_size.trim().is_empty() {
            return Err(NriError::Plugin(
                "container adjustment hugepage limit must set page_size".to_string(),
            ));
        }
    }
    for device in &resources.devices {
        validate_device_rule(device, "container adjustment")?;
    }
    for key in resources.unified.keys() {
        if key.trim().is_empty() {
            return Err(NriError::Plugin(
                "container adjustment unified resource key must not be empty".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_container_adjustment(adjustment: &nri_api::ContainerAdjustment) -> Result<()> {
    for key in adjustment.annotations.keys() {
        let name = normalized_annotation_key(key);
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
        if adjusted_key(&mount.destination).trim().is_empty() {
            return Err(NriError::Plugin(
                "container adjustment mount destination must not be empty".to_string(),
            ));
        }
    }

    for env in &adjustment.env {
        let key = adjusted_key(&env.key).trim();
        if key.is_empty() {
            return Err(NriError::Plugin(
                "container adjustment environment key must not be empty".to_string(),
            ));
        }
        if key.contains('=') {
            return Err(NriError::Plugin(
                "container adjustment environment key must not contain '='".to_string(),
            ));
        }
    }

    for rlimit in &adjustment.rlimits {
        if rlimit.type_.trim().is_empty() {
            return Err(NriError::Plugin(
                "container adjustment rlimit type must not be empty".to_string(),
            ));
        }
    }

    for device in &adjustment.CDI_devices {
        if device.name.trim().is_empty() {
            return Err(NriError::Plugin(
                "container adjustment CDI device name must not be empty".to_string(),
            ));
        }
    }

    if let Some(linux) = adjustment.linux.as_ref() {
        for device in &linux.devices {
            let path = adjusted_key(&device.path).trim();
            if path.is_empty() {
                return Err(NriError::Plugin(
                    "container adjustment device path must not be empty".to_string(),
                ));
            }
            if is_marked_for_removal(&device.path).is_none() {
                if device.type_.trim().is_empty() {
                    return Err(NriError::Plugin(
                        "container adjustment device type must not be empty".to_string(),
                    ));
                }
                if !is_valid_device_type(device.type_.trim(), false) {
                    return Err(NriError::Plugin(format!(
                        "container adjustment device type '{}' is invalid",
                        device.type_
                    )));
                }
            }
        }
        if let Some(resources) = linux.resources.as_ref() {
            validate_adjustment_resources(resources, None)?;
        }
        for namespace in &linux.namespaces {
            if adjusted_key(&namespace.type_).trim().is_empty() {
                return Err(NriError::Plugin(
                    "container adjustment namespace type must not be empty".to_string(),
                ));
            }
        }
        for key in linux.sysctl.keys() {
            if adjusted_key(key).trim().is_empty() {
                return Err(NriError::Plugin(
                    "container adjustment sysctl key must not be empty".to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn validate_linux_memory(
    memory: &nri_api::LinuxMemory,
    min_memory_limit: Option<i64>,
    context: &str,
) -> Result<()> {
    if let Some(swappiness) = memory.swappiness.as_ref() {
        if swappiness.value > 100 {
            return Err(NriError::Plugin(format!(
                "{context} memory swappiness must be between 0 and 100"
            )));
        }
    }

    if let (Some(limit), Some(reservation)) = (memory.limit.as_ref(), memory.reservation.as_ref()) {
        if limit.value >= 0 && reservation.value >= 0 && reservation.value > limit.value {
            return Err(NriError::Plugin(format!(
                "{context} memory reservation must not exceed memory limit"
            )));
        }
    }

    if let (Some(limit), Some(minimum)) = (memory.limit.as_ref(), min_memory_limit) {
        if limit.value >= 0 && limit.value < minimum {
            return Err(NriError::Plugin(format!(
                "{context} memory limit {} is below required minimum {}",
                limit.value, minimum
            )));
        }
    }

    Ok(())
}

fn validate_linux_cpu(cpu: &nri_api::LinuxCPU, context: &str) -> Result<()> {
    if !cpu.cpus.is_empty() && cpu.cpus.trim().is_empty() {
        return Err(NriError::Plugin(format!(
            "{context} CPU set must not be blank"
        )));
    }
    if !cpu.mems.is_empty() && cpu.mems.trim().is_empty() {
        return Err(NriError::Plugin(format!(
            "{context} memory node set must not be blank"
        )));
    }
    Ok(())
}

fn validate_device_rule(device: &nri_api::LinuxDeviceCgroup, context: &str) -> Result<()> {
    let device_type = device.type_.trim();
    if device_type.is_empty() {
        return Err(NriError::Plugin(format!(
            "{context} device rule must set type"
        )));
    }
    if !is_valid_device_type(device_type, true) {
        return Err(NriError::Plugin(format!(
            "{context} device rule type '{}' is invalid",
            device.type_
        )));
    }
    if !has_only_valid_device_access(&device.access) {
        return Err(NriError::Plugin(format!(
            "{context} device rule access must be a non-empty combination of 'r', 'w', and 'm'"
        )));
    }
    Ok(())
}

pub fn validate_adjustment_resources_with_min_memory(
    resources: &nri_api::LinuxResources,
    min_memory_limit: Option<i64>,
) -> Result<()> {
    validate_adjustment_resources(resources, min_memory_limit)
}

pub fn sanitize_linux_resources_for_capabilities(
    resources: &mut nri_api::LinuxResources,
    memory_swap_supported: bool,
    hugetlb_supported: bool,
) {
    if !memory_swap_supported {
        if let Some(memory) = resources.memory.as_mut() {
            memory.swap = protobuf::MessageField::none();
        }
    }

    if !hugetlb_supported {
        resources.hugepage_limits.clear();
    }
}

pub fn resource_class_names(
    resources: Option<&nri_api::LinuxResources>,
) -> (Option<String>, Option<String>) {
    let Some(resources) = resources else {
        return (None, None);
    };

    let blockio = resources
        .blockio_class
        .as_ref()
        .map(|class| class.value.trim())
        .filter(|class| !class.is_empty())
        .map(ToString::to_string);
    let rdt = resources
        .rdt_class
        .as_ref()
        .map(|class| class.value.trim())
        .filter(|class| !class.is_empty())
        .map(ToString::to_string);

    (blockio, rdt)
}

pub fn rdt_adjustment_name(rdt: Option<&nri_api::LinuxRdt>) -> Option<String> {
    let rdt = rdt.filter(|rdt| !rdt.remove)?;
    rdt.clos_id
        .as_ref()
        .map(|clos_id| clos_id.value.trim())
        .filter(|clos_id| !clos_id.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            rdt.schemata
                .as_ref()
                .filter(|schemata| !schemata.value.is_empty())
                .map(|_| "schemata".to_string())
        })
}

pub fn validate_update_linux_resources(resources: &nri_api::LinuxResources) -> Result<()> {
    if let Some(memory) = resources.memory.as_ref() {
        validate_linux_memory(memory, None, "container update")?;
    }
    if let Some(cpu) = resources.cpu.as_ref() {
        validate_linux_cpu(cpu, "container update")?;
    }
    for hugepage in &resources.hugepage_limits {
        if hugepage.page_size.trim().is_empty() {
            return Err(NriError::Plugin(
                "container update hugepage limit must set page_size".to_string(),
            ));
        }
    }
    for device in &resources.devices {
        validate_device_rule(device, "container update")?;
    }
    for key in resources.unified.keys() {
        if key.trim().is_empty() {
            return Err(NriError::Plugin(
                "container update unified resource key must not be empty".to_string(),
            ));
        }
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
        scheduler: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        io_priority: None,
    })
}

fn ensure_linux(spec: &mut Spec) -> &mut Linux {
    spec.linux.get_or_insert(Linux {
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
    })
}

fn ensure_linux_resources(spec: &mut Spec) -> &mut LinuxResources {
    ensure_linux(spec).resources.get_or_insert(LinuxResources {
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
        if let Some(existing) = entries
            .iter_mut()
            .find(|current| env_name(current) == entry.key)
        {
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

    let hooks = spec.hooks.get_or_insert(Hooks {
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
        if let Some(existing) = entries
            .iter_mut()
            .find(|current| current.path == updated.path)
        {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
    cleanup_vec(oci_devices);
}

fn apply_linux_memory(resources: &mut LinuxResources, memory: &nri_api::LinuxMemory) {
    let target = resources.memory.get_or_insert(LinuxMemory {
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
    let target = resources.cpu.get_or_insert(LinuxCpu {
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

fn apply_linux_resources(
    spec: &mut Spec,
    resources: Option<&nri_api::LinuxResources>,
    blockio_config_path: Option<&str>,
) -> Result<()> {
    let Some(adjustments) = resources else {
        return Ok(());
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
        target.devices = Some(
            adjustments
                .devices
                .iter()
                .map(convert_device_cgroup)
                .collect(),
        );
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
    if let Some(blockio_class) = adjustments.blockio_class.as_ref() {
        target.block_io = resolve_blockio_class(&blockio_class.value, blockio_config_path)?;
    }
    if let Some(rdt_class) = adjustments.rdt_class.as_ref() {
        ensure_linux(spec).intel_rdt = resolve_rdt_class(&rdt_class.value);
    }
    Ok(())
}

fn apply_linux_rdt(spec: &mut Spec, rdt: &nri_api::LinuxRdt) {
    if rdt.remove {
        ensure_linux(spec).intel_rdt = None;
        return;
    }

    let intel_rdt = ensure_linux(spec)
        .intel_rdt
        .get_or_insert(crate::oci::spec::LinuxIntelRdt {
            clos_id: None,
            l3_cache_schema: None,
            mem_bw_schema: None,
            enable_cmt: None,
            enable_mbm: None,
        });
    if let Some(clos_id) = rdt.clos_id.as_ref() {
        intel_rdt.clos_id = Some(clos_id.value.clone());
    }
    if let Some(schemata) = rdt.schemata.as_ref() {
        let mut l3 = None;
        let mut mem_bw = None;
        for entry in &schemata.value {
            if let Some(value) = entry.strip_prefix("L3:") {
                l3 = Some(value.to_string());
            } else if let Some(value) = entry.strip_prefix("MB:") {
                mem_bw = Some(value.to_string());
            }
        }
        if l3.is_none() && mem_bw.is_none() && !schemata.value.is_empty() {
            l3 = Some(schemata.value.join(";"));
        }
        if let Some(value) = l3 {
            intel_rdt.l3_cache_schema = Some(value);
        }
        if let Some(value) = mem_bw {
            intel_rdt.mem_bw_schema = Some(value);
        }
    }
    if let Some(enable_monitoring) = rdt.enable_monitoring.as_ref() {
        intel_rdt.enable_cmt = Some(enable_monitoring.value);
        intel_rdt.enable_mbm = Some(enable_monitoring.value);
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
        if let Some(existing) = entries
            .iter_mut()
            .find(|current| current.ns_type == updated.ns_type)
        {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
    cleanup_vec(oci_namespaces);
}

fn io_priority_class_name(class: nri_api::IOPrioClass) -> String {
    match class {
        nri_api::IOPrioClass::IOPRIO_CLASS_RT => "IOPRIO_CLASS_RT".to_string(),
        nri_api::IOPrioClass::IOPRIO_CLASS_BE => "IOPRIO_CLASS_BE".to_string(),
        nri_api::IOPrioClass::IOPRIO_CLASS_IDLE => "IOPRIO_CLASS_IDLE".to_string(),
        nri_api::IOPrioClass::IOPRIO_CLASS_NONE => "IOPRIO_CLASS_NONE".to_string(),
    }
}

fn scheduler_policy_name(policy: nri_api::LinuxSchedulerPolicy) -> String {
    match policy {
        nri_api::LinuxSchedulerPolicy::SCHED_OTHER => "SCHED_OTHER".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_FIFO => "SCHED_FIFO".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_RR => "SCHED_RR".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_BATCH => "SCHED_BATCH".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_ISO => "SCHED_ISO".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_IDLE => "SCHED_IDLE".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_DEADLINE => "SCHED_DEADLINE".to_string(),
        nri_api::LinuxSchedulerPolicy::SCHED_NONE => "SCHED_NONE".to_string(),
    }
}

fn scheduler_flag_name(flag: nri_api::LinuxSchedulerFlag) -> String {
    match flag {
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_RESET_ON_FORK => {
            "SCHED_FLAG_RESET_ON_FORK".to_string()
        }
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_RECLAIM => "SCHED_FLAG_RECLAIM".to_string(),
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_DL_OVERRUN => "SCHED_FLAG_DL_OVERRUN".to_string(),
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_KEEP_POLICY => "SCHED_FLAG_KEEP_POLICY".to_string(),
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_KEEP_PARAMS => "SCHED_FLAG_KEEP_PARAMS".to_string(),
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_UTIL_CLAMP_MIN => {
            "SCHED_FLAG_UTIL_CLAMP_MIN".to_string()
        }
        nri_api::LinuxSchedulerFlag::SCHED_FLAG_UTIL_CLAMP_MAX => {
            "SCHED_FLAG_UTIL_CLAMP_MAX".to_string()
        }
    }
}

fn apply_linux_io_priority(spec: &mut Spec, io_priority: &nri_api::LinuxIOPriority) {
    ensure_process(spec).io_priority = Some(crate::oci::spec::LinuxIoPriority {
        class: io_priority_class_name(io_priority.class.enum_value_or_default()),
        priority: io_priority.priority,
    });
}

fn apply_linux_scheduler(spec: &mut Spec, scheduler: &nri_api::LinuxScheduler) {
    ensure_process(spec).scheduler = Some(crate::oci::spec::Scheduler {
        policy: scheduler_policy_name(scheduler.policy.enum_value_or_default()),
        nice: (scheduler.nice != 0).then_some(scheduler.nice),
        priority: (scheduler.priority != 0).then_some(scheduler.priority),
        flags: (!scheduler.flags.is_empty()).then(|| {
            scheduler
                .flags
                .iter()
                .map(|flag| scheduler_flag_name(flag.enum_value_or_default()))
                .collect()
        }),
        runtime: (scheduler.runtime != 0).then_some(scheduler.runtime),
        deadline: (scheduler.deadline != 0).then_some(scheduler.deadline),
        period: (scheduler.period != 0).then_some(scheduler.period),
    });
}

fn apply_linux_net_devices(
    spec: &mut Spec,
    net_devices: &HashMap<String, nri_api::LinuxNetDevice>,
) -> Result<()> {
    if net_devices.is_empty() {
        return Ok(());
    }

    let entries = &mut ensure_linux(spec).net_devices;
    let devices = entries.get_or_insert_with(HashMap::new);
    for (host_name, device) in net_devices {
        if let Some(name) = is_marked_for_removal(host_name) {
            devices.remove(name);
            continue;
        }
        if host_name.is_empty() || device.name.is_empty() {
            return Err(NriError::Plugin(
                "linux net device must set both host and container names".to_string(),
            ));
        }
        devices.insert(
            host_name.clone(),
            crate::oci::spec::LinuxNetDevice {
                name: device.name.clone(),
            },
        );
    }
    cleanup_map(entries);
    Ok(())
}

fn convert_seccomp(seccomp: &nri_api::LinuxSeccomp) -> Seccomp {
    Seccomp {
        default_action: seccomp.default_action.clone(),
        default_errno_ret: seccomp.default_errno.as_ref().map(|value| value.value),
        architectures: (!seccomp.architectures.is_empty()).then(|| seccomp.architectures.clone()),
        flags: (!seccomp.flags.is_empty()).then(|| seccomp.flags.clone()),
        listener_path: (!seccomp.listener_path.is_empty()).then(|| seccomp.listener_path.clone()),
        listener_metadata: (!seccomp.listener_metadata.is_empty())
            .then(|| seccomp.listener_metadata.clone()),
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

fn apply_linux_adjustments(
    spec: &mut Spec,
    linux: Option<&nri_api::LinuxContainerAdjustment>,
    blockio_config_path: Option<&str>,
) -> Result<()> {
    let Some(adjustments) = linux else {
        return Ok(());
    };

    apply_linux_devices(spec, &adjustments.devices);
    apply_linux_resources(spec, adjustments.resources.as_ref(), blockio_config_path)?;
    if !adjustments.cgroups_path.is_empty() {
        ensure_linux(spec).cgroups_path = Some(adjustments.cgroups_path.clone());
    }
    if let Some(oom_score_adj) = adjustments.oom_score_adj.as_ref() {
        ensure_process(spec).oom_score_adj = Some(oom_score_adj.value as i32);
    }
    if let Some(io_priority) = adjustments.io_priority.as_ref() {
        apply_linux_io_priority(spec, io_priority);
    }
    if let Some(seccomp) = adjustments.seccomp_policy.as_ref() {
        ensure_linux(spec).seccomp = Some(convert_seccomp(seccomp));
    }
    apply_linux_namespaces(spec, &adjustments.namespaces);
    if let Some(scheduler) = adjustments.scheduler.as_ref() {
        apply_linux_scheduler(spec, scheduler);
    }
    apply_linux_net_devices(spec, &adjustments.net_devices)?;
    if let Some(rdt) = adjustments.rdt.as_ref() {
        apply_linux_rdt(spec, rdt);
    }

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

    Ok(())
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
        if let Some(existing) = entries
            .iter_mut()
            .find(|current| current.rtype == updated.rtype)
        {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }
    cleanup_vec(rlimits);
}

fn apply_cdi_device_adjustments(
    spec: &mut Spec,
    devices: &[nri_api::CDIDevice],
    enabled: bool,
    configured_dirs: Option<&[String]>,
) -> Result<()> {
    let device_names = devices
        .iter()
        .map(|device| device.name.clone())
        .collect::<Vec<_>>();
    crate::security::cdi::apply_cdi_devices(spec, &device_names, enabled, configured_dirs)
        .map_err(|err| NriError::Plugin(err.to_string()))
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

pub fn apply_container_adjustment(
    spec: &mut Spec,
    adjustment: &nri_api::ContainerAdjustment,
) -> Result<()> {
    apply_container_adjustment_with_options(spec, adjustment, AdjustmentOptions::default())
}

pub fn apply_container_adjustment_with_blockio_config(
    spec: &mut Spec,
    adjustment: &nri_api::ContainerAdjustment,
    blockio_config_path: Option<&str>,
) -> Result<()> {
    apply_container_adjustment_with_options(
        spec,
        adjustment,
        AdjustmentOptions {
            blockio_config_path,
            cdi_enabled: true,
            cdi_spec_dirs: None,
        },
    )
}

pub fn apply_container_adjustment_with_options(
    spec: &mut Spec,
    adjustment: &nri_api::ContainerAdjustment,
    options: AdjustmentOptions<'_>,
) -> Result<()> {
    apply_annotation_adjustments(spec, &adjustment.annotations);
    apply_mount_adjustments(spec, adjustment.mounts.as_slice());
    apply_env_adjustments(spec, adjustment.env.as_slice());
    apply_hook_adjustments(spec, adjustment.hooks.as_ref());
    apply_cdi_device_adjustments(
        spec,
        &adjustment.CDI_devices,
        options.cdi_enabled,
        options.cdi_spec_dirs,
    )?;
    apply_linux_adjustments(spec, adjustment.linux.as_ref(), options.blockio_config_path)?;
    apply_rlimit_adjustments(spec, adjustment.rlimits.as_slice());

    if !adjustment.args.is_empty() {
        let process = ensure_process(spec);
        process.args = if adjustment.args.first().is_some_and(|arg| arg.is_empty()) {
            adjustment.args[1..].to_vec()
        } else {
            adjustment.args.clone()
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests;
