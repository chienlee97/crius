use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;
use crate::oci::spec::{
    Device, Hook, Hooks, Linux, LinuxBlockIo, LinuxCpu, LinuxDeviceCgroup, LinuxHugepageLimit,
    LinuxMemory, LinuxPids, LinuxResources, Mount, Namespace, Process, Rlimit, Seccomp, SeccompArg,
    SeccompSyscall, Spec,
};
use serde::Deserialize;

const REMOVAL_PREFIX: &str = "-";
const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";
const CHECKPOINT_LOCATION_ANNOTATION_KEY: &str = "io.crius.checkpoint.location";
const DEFAULT_CDI_SPEC_DIRS: [&str; 2] = ["/etc/cdi", "/var/run/cdi"];
const BLOCKIO_CONFIG_ENV: &str = "CRIUS_NRI_BLOCKIO_CONFIG";
const POD_QOS_RDT_CLASS: &str = "/PodQos";

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct BlockIoClassConfig {
    classes: HashMap<String, LinuxBlockIo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum BlockIoConfigFile {
    Wrapped(BlockIoClassConfig),
    Flat(HashMap<String, LinuxBlockIo>),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiSpec {
    kind: String,
    devices: Vec<CdiDeviceSpec>,
    container_edits: CdiContainerEdits,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiDeviceSpec {
    name: String,
    container_edits: CdiContainerEdits,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiContainerEdits {
    env: Vec<String>,
    device_nodes: Vec<CdiDeviceNode>,
    hooks: Vec<CdiHook>,
    mounts: Vec<CdiMount>,
    intel_rdt: Option<CdiIntelRdt>,
    additional_gids: Vec<u32>,
    net_devices: Vec<CdiNetDevice>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiDeviceNode {
    path: String,
    host_path: Option<String>,
    #[serde(rename = "type")]
    device_type: String,
    major: i64,
    minor: i64,
    file_mode: Option<u32>,
    permissions: String,
    uid: Option<u32>,
    gid: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiMount {
    host_path: String,
    container_path: String,
    options: Vec<String>,
    #[serde(rename = "type")]
    mount_type: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiHook {
    hook_name: String,
    path: String,
    args: Vec<String>,
    env: Vec<String>,
    timeout: Option<i32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiIntelRdt {
    #[serde(alias = "closID")]
    clos_id: String,
    l3_cache_schema: String,
    mem_bw_schema: String,
    schemata: Vec<String>,
    enable_monitoring: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CdiNetDevice {
    host_interface_name: String,
    name: String,
}

fn cdi_spec_dirs() -> Vec<PathBuf> {
    std::env::var("CDI_SPEC_DIRS")
        .ok()
        .map(|dirs| {
            dirs.split(':')
                .filter(|entry| !entry.is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .filter(|dirs| !dirs.is_empty())
        .unwrap_or_else(|| DEFAULT_CDI_SPEC_DIRS.iter().map(PathBuf::from).collect())
}

fn load_cdi_specs() -> Result<Vec<CdiSpec>> {
    let mut specs = Vec::new();
    for dir in cdi_spec_dirs() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(NriError::Plugin(format!(
                    "failed to read CDI directory {}: {}",
                    dir.display(),
                    err
                )))
            }
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                NriError::Plugin(format!(
                    "failed to read CDI entry in {}: {}",
                    dir.display(),
                    err
                ))
            })?;
            let path = entry.path();
            if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let content = fs::read_to_string(&path).map_err(|err| {
                NriError::Plugin(format!(
                    "failed to read CDI spec {}: {}",
                    path.display(),
                    err
                ))
            })?;
            let spec = serde_json::from_str::<CdiSpec>(&content).map_err(|err| {
                NriError::Plugin(format!(
                    "failed to parse CDI spec {}: {}",
                    path.display(),
                    err
                ))
            })?;
            specs.push(spec);
        }
    }
    Ok(specs)
}

fn merge_cdi_edits(base: &mut CdiContainerEdits, extra: &CdiContainerEdits) {
    base.env.extend(extra.env.clone());
    base.device_nodes.extend(extra.device_nodes.clone());
    base.hooks.extend(extra.hooks.clone());
    base.mounts.extend(extra.mounts.clone());
    base.additional_gids.extend(extra.additional_gids.clone());
    base.net_devices.extend(extra.net_devices.clone());
    if extra.intel_rdt.is_some() {
        base.intel_rdt = extra.intel_rdt.clone();
    }
}

fn resolve_cdi_edits(device_ref: &str) -> Result<CdiContainerEdits> {
    let Some((kind, name)) = device_ref.split_once('=') else {
        return Err(NriError::Plugin(format!(
            "invalid CDI device reference {device_ref:?}, expected <vendor>/<class>=<device>"
        )));
    };

    for spec in load_cdi_specs()? {
        if spec.kind != kind {
            continue;
        }
        if let Some(device) = spec.devices.iter().find(|device| device.name == name) {
            let mut edits = spec.container_edits.clone();
            merge_cdi_edits(&mut edits, &device.container_edits);
            return Ok(edits);
        }
    }

    Err(NriError::Plugin(format!(
        "CDI device {device_ref} not found in configured CDI specs"
    )))
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

fn effective_blockio_config_path(config_path: Option<&str>) -> Option<PathBuf> {
    config_path
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var(BLOCKIO_CONFIG_ENV)
                .ok()
                .filter(|path| !path.trim().is_empty())
                .map(PathBuf::from)
        })
}

pub fn resolve_blockio_class(
    class_name: &str,
    config_path: Option<&str>,
) -> Result<Option<LinuxBlockIo>> {
    if class_name.trim().is_empty() {
        return Ok(None);
    }

    let path = effective_blockio_config_path(config_path).ok_or_else(|| {
        NriError::Plugin(format!(
            "blockio class '{}' requested but no blockio config path is configured",
            class_name
        ))
    })?;
    let content = fs::read_to_string(&path).map_err(|err| {
        NriError::Plugin(format!(
            "failed to read blockio config {}: {}",
            path.display(),
            err
        ))
    })?;
    let classes = match serde_json::from_str::<BlockIoConfigFile>(&content).map_err(|err| {
        NriError::Plugin(format!(
            "failed to parse blockio config {}: {}",
            path.display(),
            err
        ))
    })? {
        BlockIoConfigFile::Wrapped(config) => config.classes,
        BlockIoConfigFile::Flat(classes) => classes,
    };
    classes
        .get(class_name)
        .cloned()
        .ok_or_else(|| {
            NriError::Plugin(format!(
                "blockio class '{}' not found in {}",
                class_name,
                path.display()
            ))
        })
        .map(Some)
}

pub fn resolve_rdt_class(class_name: &str) -> Option<crate::oci::spec::LinuxIntelRdt> {
    let class_name = class_name.trim();
    if class_name.is_empty() || class_name == POD_QOS_RDT_CLASS {
        return None;
    }

    Some(crate::oci::spec::LinuxIntelRdt {
        clos_id: Some(class_name.to_string()),
        l3_cache_schema: None,
        mem_bw_schema: None,
        enable_cmt: None,
        enable_mbm: None,
    })
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

fn apply_cdi_env(spec: &mut Spec, env: &[String]) {
    if env.is_empty() {
        return;
    }

    let entries = &mut ensure_process(spec).env;
    let current = entries.get_or_insert_with(Vec::new);
    for rendered in env {
        let name = env_name(rendered);
        if let Some(existing) = current.iter_mut().find(|entry| env_name(entry) == name) {
            *existing = rendered.clone();
        } else {
            current.push(rendered.clone());
        }
    }
}

fn apply_cdi_device_nodes(spec: &mut Spec, nodes: &[CdiDeviceNode]) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    for node in nodes {
        if node.path.is_empty() {
            return Err(NriError::Plugin(
                "CDI device node path must not be empty".to_string(),
            ));
        }
        let updated = Device {
            device_type: if node.device_type.is_empty() {
                "c".to_string()
            } else {
                node.device_type.clone()
            },
            path: node.path.clone(),
            major: Some(node.major),
            minor: Some(node.minor),
            file_mode: node.file_mode,
            uid: node.uid,
            gid: node.gid,
        };
        {
            let devices = &mut ensure_linux(spec).devices;
            let entries = devices.get_or_insert_with(Vec::new);
            if let Some(existing) = entries
                .iter_mut()
                .find(|device| device.path == updated.path)
            {
                *existing = updated;
            } else {
                entries.push(updated);
            }
        }

        let resource = LinuxDeviceCgroup {
            allow: true,
            device_type: Some(if node.device_type.is_empty() {
                "c".to_string()
            } else {
                node.device_type.clone()
            }),
            major: Some(node.major),
            minor: Some(node.minor),
            access: Some(if node.permissions.is_empty() {
                "rwm".to_string()
            } else {
                node.permissions.clone()
            }),
        };
        {
            let resources = &mut ensure_linux_resources(spec).devices;
            let resource_entries = resources.get_or_insert_with(Vec::new);
            if let Some(existing) = resource_entries.iter_mut().find(|device| {
                device.device_type == resource.device_type
                    && device.major == resource.major
                    && device.minor == resource.minor
            }) {
                *existing = resource;
            } else {
                resource_entries.push(resource);
            }
        }
    }

    Ok(())
}

fn apply_cdi_mounts(spec: &mut Spec, mounts: &[CdiMount]) -> Result<()> {
    if mounts.is_empty() {
        return Ok(());
    }

    let entries = spec.mounts.get_or_insert_with(Vec::new);
    for mount in mounts {
        if mount.host_path.is_empty() || mount.container_path.is_empty() {
            return Err(NriError::Plugin(
                "CDI mount must set both hostPath and containerPath".to_string(),
            ));
        }
        let updated = Mount {
            destination: mount.container_path.clone(),
            source: Some(mount.host_path.clone()),
            mount_type: (!mount.mount_type.is_empty()).then(|| mount.mount_type.clone()),
            options: (!mount.options.is_empty()).then(|| mount.options.clone()),
        };
        if let Some(existing) = entries
            .iter_mut()
            .find(|current| current.destination == updated.destination)
        {
            *existing = updated;
        } else {
            entries.push(updated);
        }
    }

    Ok(())
}

fn apply_cdi_hooks(spec: &mut Spec, hooks: &[CdiHook]) -> Result<()> {
    if hooks.is_empty() {
        return Ok(());
    }

    let oci_hooks = spec.hooks.get_or_insert(Hooks {
        prestart: None,
        create_runtime: None,
        create_container: None,
        start_container: None,
        poststart: None,
        poststop: None,
    });
    for hook in hooks {
        if hook.path.is_empty() {
            return Err(NriError::Plugin(
                "CDI hook path must not be empty".to_string(),
            ));
        }
        let converted = Hook {
            path: hook.path.clone(),
            args: (!hook.args.is_empty()).then(|| hook.args.clone()),
            env: (!hook.env.is_empty()).then(|| hook.env.clone()),
            timeout: hook.timeout,
        };
        match hook.hook_name.as_str() {
            "prestart" => oci_hooks
                .prestart
                .get_or_insert_with(Vec::new)
                .push(converted),
            "createRuntime" => oci_hooks
                .create_runtime
                .get_or_insert_with(Vec::new)
                .push(converted),
            "createContainer" => oci_hooks
                .create_container
                .get_or_insert_with(Vec::new)
                .push(converted),
            "startContainer" => oci_hooks
                .start_container
                .get_or_insert_with(Vec::new)
                .push(converted),
            "poststart" => oci_hooks
                .poststart
                .get_or_insert_with(Vec::new)
                .push(converted),
            "poststop" => oci_hooks
                .poststop
                .get_or_insert_with(Vec::new)
                .push(converted),
            other => {
                return Err(NriError::Plugin(format!(
                    "unsupported CDI hook name {other:?}"
                )))
            }
        }
    }

    Ok(())
}

fn apply_cdi_intel_rdt(spec: &mut Spec, intel_rdt: &CdiIntelRdt) {
    ensure_linux(spec).intel_rdt = Some(crate::oci::spec::LinuxIntelRdt {
        clos_id: (!intel_rdt.clos_id.is_empty()).then(|| intel_rdt.clos_id.clone()),
        l3_cache_schema: (!intel_rdt.l3_cache_schema.is_empty())
            .then(|| intel_rdt.l3_cache_schema.clone())
            .or_else(|| (!intel_rdt.schemata.is_empty()).then(|| intel_rdt.schemata.join(";"))),
        mem_bw_schema: (!intel_rdt.mem_bw_schema.is_empty())
            .then(|| intel_rdt.mem_bw_schema.clone()),
        enable_cmt: Some(intel_rdt.enable_monitoring),
        enable_mbm: Some(intel_rdt.enable_monitoring),
    });
}

fn apply_cdi_additional_gids(spec: &mut Spec, gids: &[u32]) {
    if gids.is_empty() {
        return;
    }

    let user = ensure_process(spec)
        .user
        .get_or_insert(crate::oci::spec::User {
            uid: 0,
            gid: 0,
            additional_gids: None,
            username: None,
        });
    let existing = user.additional_gids.get_or_insert_with(Vec::new);
    let mut seen: HashSet<u32> = existing.iter().copied().collect();
    for gid in gids {
        if *gid != 0 && seen.insert(*gid) {
            existing.push(*gid);
        }
    }
}

fn apply_cdi_net_devices(spec: &mut Spec, devices: &[CdiNetDevice]) -> Result<()> {
    if devices.is_empty() {
        return Ok(());
    }

    let entries = &mut ensure_linux(spec).net_devices;
    let current = entries.get_or_insert_with(HashMap::new);
    for device in devices {
        if device.host_interface_name.is_empty() || device.name.is_empty() {
            return Err(NriError::Plugin(
                "CDI net device must set both hostInterfaceName and name".to_string(),
            ));
        }
        current.insert(
            device.host_interface_name.clone(),
            crate::oci::spec::LinuxNetDevice {
                name: device.name.clone(),
            },
        );
    }
    Ok(())
}

fn apply_cdi_edits(spec: &mut Spec, edits: &CdiContainerEdits) -> Result<()> {
    apply_cdi_env(spec, &edits.env);
    apply_cdi_device_nodes(spec, &edits.device_nodes)?;
    apply_cdi_mounts(spec, &edits.mounts)?;
    apply_cdi_hooks(spec, &edits.hooks)?;
    apply_cdi_net_devices(spec, &edits.net_devices)?;
    if let Some(intel_rdt) = edits.intel_rdt.as_ref() {
        apply_cdi_intel_rdt(spec, intel_rdt);
    }
    apply_cdi_additional_gids(spec, &edits.additional_gids);
    Ok(())
}

fn apply_cdi_device_adjustments(spec: &mut Spec, devices: &[nri_api::CDIDevice]) -> Result<()> {
    for device in devices {
        let edits = resolve_cdi_edits(&device.name)?;
        apply_cdi_edits(spec, &edits)?;
    }
    Ok(())
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
    apply_container_adjustment_with_blockio_config(spec, adjustment, None)
}

pub fn apply_container_adjustment_with_blockio_config(
    spec: &mut Spec,
    adjustment: &nri_api::ContainerAdjustment,
    blockio_config_path: Option<&str>,
) -> Result<()> {
    apply_annotation_adjustments(spec, &adjustment.annotations);
    apply_mount_adjustments(spec, adjustment.mounts.as_slice());
    apply_env_adjustments(spec, adjustment.env.as_slice());
    apply_hook_adjustments(spec, adjustment.hooks.as_ref());
    apply_cdi_device_adjustments(spec, &adjustment.CDI_devices)?;
    apply_linux_adjustments(spec, adjustment.linux.as_ref(), blockio_config_path)?;
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
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use protobuf::MessageField;
    use tempfile::tempdir;

    use super::{
        apply_container_adjustment, apply_container_adjustment_with_blockio_config,
        disallowed_annotation_adjustment_keys, filter_annotation_adjustments_by_allowlist,
        resolve_rdt_class, sanitize_linux_resources_for_capabilities,
        validate_adjustment_resources_with_min_memory, validate_container_adjustment,
        validate_container_update, validate_update_linux_resources, REMOVAL_PREFIX,
    };
    use crate::nri_proto::api as nri_api;
    use crate::oci::spec::{
        Device, Linux, LinuxCpu, LinuxHugepageLimit, LinuxMemory, LinuxPids, LinuxResources, Mount,
        Namespace, Process, Rlimit, Spec,
    };

    fn removal(name: &str) -> String {
        format!("{REMOVAL_PREFIX}{name}")
    }

    fn write_blockio_config(dir: &Path) -> PathBuf {
        let path = dir.join("blockio.json");
        fs::write(
            &path,
            serde_json::json!({
                "classes": {
                    "gold": {
                        "weight": 500
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        path
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
                scheduler: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                io_priority: None,
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
                net_devices: None,
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
                    unified: Some(HashMap::from([(
                        "memory.high".to_string(),
                        "128".to_string(),
                    )])),
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

        apply_container_adjustment(&mut spec, &adjustment).unwrap();

        assert_eq!(
            spec.annotations.as_ref().unwrap().get("keep"),
            Some(&"new".to_string())
        );
        assert!(!spec.annotations.as_ref().unwrap().contains_key("drop"));
        assert_eq!(
            spec.process.as_ref().unwrap().env.as_ref().unwrap(),
            &vec!["B=3".to_string()]
        );
        let mount = spec
            .mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|mount| mount.destination == "/data")
            .unwrap();
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
        resources
            .unified
            .insert("cpu.max".to_string(), "100000 100000".to_string());
        resources.pids = Some(nri_api::LinuxPids {
            limit: 128,
            ..Default::default()
        })
        .into();
        linux.resources = MessageField::some(resources);
        linux.cgroups_path = "/new/path".to_string();
        linux.oom_score_adj = opt_int(222);
        linux
            .sysctl
            .insert(removal("net.ipv4.ip_forward"), String::new());
        linux
            .sysctl
            .insert("vm.max_map_count".to_string(), "262144".to_string());
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

        apply_container_adjustment(&mut spec, &adjustment).unwrap();

        let linux = spec.linux.as_ref().unwrap();
        assert!(linux
            .devices
            .as_ref()
            .unwrap()
            .iter()
            .all(|device| device.path != "/dev/null"));
        let fuse = linux
            .devices
            .as_ref()
            .unwrap()
            .iter()
            .find(|device| device.path == "/dev/fuse")
            .unwrap();
        assert_eq!(fuse.major, Some(10));
        assert_eq!(linux.cgroups_path.as_deref(), Some("/new/path"));
        assert_eq!(spec.process.as_ref().unwrap().oom_score_adj, Some(222));
        assert_eq!(
            linux.sysctl.as_ref().unwrap().get("vm.max_map_count"),
            Some(&"262144".to_string())
        );
        assert!(!linux
            .sysctl
            .as_ref()
            .unwrap()
            .contains_key("net.ipv4.ip_forward"));
        assert!(linux
            .namespaces
            .as_ref()
            .unwrap()
            .iter()
            .all(|ns| ns.ns_type != "network"));
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
        assert_eq!(
            seccomp.syscalls.as_ref().unwrap()[0].names,
            vec!["clone3".to_string()]
        );
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

        apply_container_adjustment(&mut spec, &adjustment).unwrap();

        let rlimits = spec.process.as_ref().unwrap().rlimits.as_ref().unwrap();
        assert_eq!(rlimits.len(), 2);
        assert_eq!(
            rlimits
                .iter()
                .find(|limit| limit.rtype == "RLIMIT_NOFILE")
                .unwrap()
                .hard,
            4096
        );
        assert_eq!(
            rlimits
                .iter()
                .find(|limit| limit.rtype == "RLIMIT_NPROC")
                .unwrap()
                .soft,
            1024
        );
    }

    #[test]
    fn applies_rdt_class_and_linux_rdt_adjustments() {
        let mut spec = spec_with_process();
        let mut adjustment = nri_api::ContainerAdjustment::new();
        let mut linux = nri_api::LinuxContainerAdjustment::new();
        let mut resources = nri_api::LinuxResources::new();
        let mut rdt_class = nri_api::OptionalString::new();
        rdt_class.value = "gold".to_string();
        resources.rdt_class = MessageField::some(rdt_class);
        linux.resources = MessageField::some(resources);

        let mut rdt = nri_api::LinuxRdt::new();
        let mut clos_id = nri_api::OptionalString::new();
        clos_id.value = "silver".to_string();
        rdt.clos_id = MessageField::some(clos_id);
        let mut schemata = nri_api::OptionalRepeatedString::new();
        schemata.value = vec!["L3:0=ff".to_string(), "MB:0=70".to_string()];
        rdt.schemata = MessageField::some(schemata);
        let mut monitoring = nri_api::OptionalBool::new();
        monitoring.value = true;
        rdt.enable_monitoring = MessageField::some(monitoring);
        linux.rdt = MessageField::some(rdt);
        adjustment.linux = MessageField::some(linux);

        apply_container_adjustment(&mut spec, &adjustment).unwrap();

        let intel_rdt = spec
            .linux
            .as_ref()
            .and_then(|linux| linux.intel_rdt.as_ref())
            .expect("intel_rdt should be configured");
        assert_eq!(intel_rdt.clos_id.as_deref(), Some("silver"));
        assert_eq!(intel_rdt.l3_cache_schema.as_deref(), Some("0=ff"));
        assert_eq!(intel_rdt.mem_bw_schema.as_deref(), Some("0=70"));
        assert_eq!(intel_rdt.enable_cmt, Some(true));
        assert_eq!(intel_rdt.enable_mbm, Some(true));
    }

    #[test]
    fn resolves_pod_qos_rdt_class_to_none() {
        assert!(resolve_rdt_class("").is_none());
        assert!(resolve_rdt_class("/PodQos").is_none());
        assert_eq!(
            resolve_rdt_class("gold")
                .and_then(|rdt| rdt.clos_id)
                .as_deref(),
            Some("gold")
        );
    }

    #[test]
    fn applies_blockio_class_adjustments() {
        let dir = tempdir().unwrap();
        let config_path = write_blockio_config(dir.path());
        let mut spec = spec_with_process();
        let mut adjustment = nri_api::ContainerAdjustment::new();
        let mut linux = nri_api::LinuxContainerAdjustment::new();
        let mut resources = nri_api::LinuxResources::new();
        let mut blockio_class = nri_api::OptionalString::new();
        blockio_class.value = "gold".to_string();
        resources.blockio_class = MessageField::some(blockio_class);
        linux.resources = MessageField::some(resources);
        adjustment.linux = MessageField::some(linux);

        apply_container_adjustment_with_blockio_config(
            &mut spec,
            &adjustment,
            Some(config_path.to_str().unwrap()),
        )
        .unwrap();

        let block_io = spec
            .linux
            .as_ref()
            .and_then(|linux| linux.resources.as_ref())
            .and_then(|resources| resources.block_io.as_ref())
            .expect("block io should be configured");
        assert_eq!(block_io.weight, Some(500));
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
    fn filters_annotation_adjustments_by_allowlist() {
        let adjustments = HashMap::from([
            (
                "io.kubernetes.cri.sandbox-name".to_string(),
                "pod-a".to_string(),
            ),
            (
                "cpu-load-balancing.crio.io".to_string(),
                "disable".to_string(),
            ),
            ("-cpu-quota.crio.io".to_string(), String::new()),
        ]);
        let allowed = vec!["cpu-".to_string()];

        let filtered = filter_annotation_adjustments_by_allowlist(&adjustments, &allowed);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains_key("cpu-load-balancing.crio.io"));
        assert!(filtered.contains_key("-cpu-quota.crio.io"));
        assert!(!filtered.contains_key("io.kubernetes.cri.sandbox-name"));

        let disallowed = disallowed_annotation_adjustment_keys(&adjustments, &allowed);
        assert_eq!(
            disallowed,
            vec!["io.kubernetes.cri.sandbox-name".to_string()]
        );
    }

    #[test]
    fn rejects_invalid_adjustment_resource_values() {
        let mut resources = nri_api::LinuxResources::new();
        let mut memory = nri_api::LinuxMemory::new();
        let mut swappiness = nri_api::OptionalUInt64::new();
        swappiness.value = 101;
        memory.swappiness = protobuf::MessageField::some(swappiness);
        resources.memory = protobuf::MessageField::some(memory);
        resources.devices.push(nri_api::LinuxDeviceCgroup {
            allow: true,
            type_: "x".to_string(),
            access: "rwm".to_string(),
            ..Default::default()
        });

        let err =
            validate_adjustment_resources_with_min_memory(&resources, Some(1024)).unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.contains("swappiness") || rendered.contains("device rule type"));
    }

    #[test]
    fn rejects_adjustment_memory_limit_below_minimum() {
        let mut resources = nri_api::LinuxResources::new();
        let mut memory = nri_api::LinuxMemory::new();
        let mut limit = nri_api::OptionalInt64::new();
        limit.value = 512;
        memory.limit = protobuf::MessageField::some(limit);
        resources.memory = protobuf::MessageField::some(memory);

        let err =
            validate_adjustment_resources_with_min_memory(&resources, Some(1024)).unwrap_err();
        assert!(format!("{err}").contains("below required minimum"));
    }

    #[test]
    fn sanitizes_resources_for_missing_capabilities() {
        let mut resources = nri_api::LinuxResources::new();
        let mut memory = nri_api::LinuxMemory::new();
        let mut swap = nri_api::OptionalInt64::new();
        swap.value = 8192;
        memory.swap = protobuf::MessageField::some(swap);
        resources.memory = protobuf::MessageField::some(memory);
        resources.hugepage_limits.push(nri_api::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 4,
            ..Default::default()
        });

        sanitize_linux_resources_for_capabilities(&mut resources, false, false);

        assert!(resources
            .memory
            .as_ref()
            .and_then(|memory| memory.swap.as_ref())
            .is_none());
        assert!(resources.hugepage_limits.is_empty());
    }

    #[test]
    fn rejects_update_with_invalid_device_access() {
        let mut resources = nri_api::LinuxResources::new();
        resources.devices.push(nri_api::LinuxDeviceCgroup {
            allow: true,
            type_: "c".to_string(),
            access: "rx".to_string(),
            ..Default::default()
        });

        let err = validate_update_linux_resources(&resources).unwrap_err();
        assert!(format!("{err}").contains("device rule access"));
    }

    #[test]
    fn applies_cdi_device_edits_from_json_spec() {
        let dir = tempdir().unwrap();
        let spec_path = dir.path().join("vendor.json");
        fs::write(
            &spec_path,
            serde_json::json!({
                "cdiVersion": "0.7.0",
                "kind": "vendor.com/device",
                "containerEdits": {
                    "env": ["FROM_SPEC=1"],
                    "additionalGids": [2000]
                },
                "devices": [{
                    "name": "gpu0",
                    "containerEdits": {
                        "env": ["FROM_DEVICE=1"],
                        "deviceNodes": [{
                            "path": "/dev/vendor0",
                            "type": "c",
                            "major": 10,
                            "minor": 200,
                            "permissions": "rw"
                        }],
                        "mounts": [{
                            "hostPath": "/opt/vendor/lib.so",
                            "containerPath": "/usr/lib/libvendor.so",
                            "options": ["ro", "bind"],
                            "type": "bind"
                        }],
                        "hooks": [{
                            "hookName": "createContainer",
                            "path": "/bin/cdi-hook",
                            "args": ["hook"],
                            "env": ["A=B"]
                        }],
                        "intelRdt": {
                            "closID": "gold",
                            "l3CacheSchema": "0=ff",
                            "memBwSchema": "0=70",
                            "enableMonitoring": true
                        }
                    }
                }]
            })
            .to_string(),
        )
        .unwrap();

        std::env::set_var("CDI_SPEC_DIRS", dir.path());
        let mut spec = spec_with_process();
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment.CDI_devices.push(nri_api::CDIDevice {
            name: "vendor.com/device=gpu0".to_string(),
            ..Default::default()
        });

        apply_container_adjustment(&mut spec, &adjustment).unwrap();
        std::env::remove_var("CDI_SPEC_DIRS");

        let env = spec.process.as_ref().unwrap().env.as_ref().unwrap();
        assert!(env.iter().any(|entry| entry == "FROM_SPEC=1"));
        assert!(env.iter().any(|entry| entry == "FROM_DEVICE=1"));
        assert_eq!(
            spec.process
                .as_ref()
                .unwrap()
                .user
                .as_ref()
                .and_then(|user| user.additional_gids.as_ref())
                .unwrap(),
            &vec![2000]
        );
        assert!(spec
            .mounts
            .as_ref()
            .unwrap()
            .iter()
            .any(|mount| mount.destination == "/usr/lib/libvendor.so"));
        assert!(spec
            .linux
            .as_ref()
            .unwrap()
            .devices
            .as_ref()
            .unwrap()
            .iter()
            .any(|device| device.path == "/dev/vendor0"));
        assert!(spec
            .hooks
            .as_ref()
            .unwrap()
            .create_container
            .as_ref()
            .unwrap()
            .iter()
            .any(|hook| hook.path == "/bin/cdi-hook"));
        let intel_rdt = spec.linux.as_ref().unwrap().intel_rdt.as_ref().unwrap();
        assert_eq!(intel_rdt.clos_id.as_deref(), Some("gold"));
        assert_eq!(intel_rdt.l3_cache_schema.as_deref(), Some("0=ff"));
        assert_eq!(intel_rdt.mem_bw_schema.as_deref(), Some("0=70"));
        assert_eq!(intel_rdt.enable_cmt, Some(true));
        assert_eq!(intel_rdt.enable_mbm, Some(true));
    }

    #[test]
    fn rejects_invalid_cdi_reference() {
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment.CDI_devices.push(nri_api::CDIDevice {
            name: "vendor.com/device".to_string(),
            ..Default::default()
        });

        let mut spec = spec_with_process();
        let err = apply_container_adjustment(&mut spec, &adjustment).unwrap_err();
        assert!(format!("{err}").contains("expected <vendor>/<class>=<device>"));
    }

    #[test]
    fn rejects_invalid_linux_net_device_adjustment() {
        let mut adjustment = nri_api::ContainerAdjustment::new();
        adjustment.linux = Some({
            let mut linux = nri_api::LinuxContainerAdjustment::new();
            linux.net_devices.insert(
                "eth0".to_string(),
                nri_api::LinuxNetDevice {
                    name: String::new(),
                    ..Default::default()
                },
            );
            linux
        })
        .into();

        let mut spec = spec_with_process();
        let err = apply_container_adjustment(&mut spec, &adjustment).unwrap_err();
        assert!(
            format!("{err}").contains("linux net device must set both host and container names")
        );
    }

    #[test]
    fn accepts_extended_update_resources() {
        let mut resources = nri_api::LinuxResources::new();
        let mut memory = nri_api::LinuxMemory::new();
        let mut reservation = nri_api::OptionalInt64::new();
        reservation.value = 4096;
        memory.reservation = protobuf::MessageField::some(reservation);
        resources.memory = protobuf::MessageField::some(memory);
        resources.pids = Some(nri_api::LinuxPids {
            limit: 12,
            ..Default::default()
        })
        .into();
        resources.devices.push(nri_api::LinuxDeviceCgroup {
            allow: true,
            type_: "c".to_string(),
            access: "rwm".to_string(),
            ..Default::default()
        });

        validate_update_linux_resources(&resources)
            .expect("extended update resources should validate");
    }

    #[test]
    fn rejects_update_without_linux_resources() {
        let mut update = nri_api::ContainerUpdate::new();
        update.container_id = "ctr-1".to_string();

        let err = validate_container_update(&update).unwrap_err();
        assert!(format!("{err}").contains("missing linux payload"));
    }
}
