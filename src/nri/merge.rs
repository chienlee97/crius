use std::collections::HashMap;

use protobuf::MessageField;

use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;

const REMOVAL_PREFIX: &str = "-";

#[derive(Debug, Clone, Default)]
pub struct MergeResult {
    pub adjustment: nri_api::ContainerAdjustment,
    pub owners: nri_api::OwningPlugins,
}

#[derive(Debug, Clone, Default)]
pub struct MergedContainerUpdates {
    pub target_linux_resources: Option<nri_api::LinuxResources>,
    pub updates: Vec<nri_api::ContainerUpdate>,
}

fn is_marked_for_removal(key: &str) -> (String, bool) {
    if let Some(stripped) = key.strip_prefix(REMOVAL_PREFIX) {
        (stripped.to_string(), true)
    } else {
        (key.to_string(), false)
    }
}

fn mark_for_removal(key: &str) -> String {
    format!("{REMOVAL_PREFIX}{key}")
}

fn field_key(field: nri_api::Field) -> i32 {
    field as i32
}

fn owners_for_container<'a>(
    owners: &'a mut nri_api::OwningPlugins,
    container_id: &str,
) -> &'a mut nri_api::FieldOwners {
    owners
        .owners
        .entry(container_id.to_string())
        .or_insert_with(nri_api::FieldOwners::new)
}

fn conflict(
    field: nri_api::Field,
    plugin: &str,
    other: &str,
    qualifier: Option<&str>,
) -> NriError {
    let field_name = format!("{field:?}");
    let detail = qualifier
        .map(|q| format!("{field_name} ({q})"))
        .unwrap_or(field_name);
    NriError::Plugin(format!(
        "plugins '{}' and '{}' both tried to set {}",
        plugin, other, detail
    ))
}

fn claim_compound(
    owners: &mut nri_api::OwningPlugins,
    container_id: &str,
    field: nri_api::Field,
    key: &str,
    plugin: &str,
) -> Result<()> {
    let field_owners = owners_for_container(owners, container_id);
    let entry = field_owners
        .compound
        .entry(field_key(field))
        .or_insert_with(nri_api::CompoundFieldOwners::new);
    if let Some(current) = entry.owners.get(key) {
        let (other, removed) = is_marked_for_removal(current);
        if other != plugin && !(removed && other == plugin) {
            return Err(conflict(field, plugin, &other, Some(key)));
        }
    }
    entry.owners.insert(key.to_string(), plugin.to_string());
    Ok(())
}

fn clear_compound(
    owners: &mut nri_api::OwningPlugins,
    container_id: &str,
    field: nri_api::Field,
    key: &str,
    plugin: &str,
) {
    let field_owners = owners_for_container(owners, container_id);
    let entry = field_owners
        .compound
        .entry(field_key(field))
        .or_insert_with(nri_api::CompoundFieldOwners::new);
    entry
        .owners
        .insert(key.to_string(), mark_for_removal(plugin));
}

fn claim_simple(
    owners: &mut nri_api::OwningPlugins,
    container_id: &str,
    field: nri_api::Field,
    plugin: &str,
) -> Result<()> {
    let field_owners = owners_for_container(owners, container_id);
    if let Some(current) = field_owners.simple.get(&field_key(field)) {
        let (other, removed) = is_marked_for_removal(current);
        if other != plugin && !(removed && other == plugin) {
            return Err(conflict(field, plugin, &other, None));
        }
    }
    field_owners
        .simple
        .insert(field_key(field), plugin.to_string());
    Ok(())
}

fn clear_simple(
    owners: &mut nri_api::OwningPlugins,
    container_id: &str,
    field: nri_api::Field,
    plugin: &str,
) {
    let field_owners = owners_for_container(owners, container_id);
    field_owners
        .simple
        .insert(field_key(field), mark_for_removal(plugin));
}

fn claim_hooks(
    owners: &mut nri_api::OwningPlugins,
    container_id: &str,
    plugin: &str,
) -> Result<()> {
    let field_owners = owners_for_container(owners, container_id);
    let key = field_key(nri_api::Field::OciHooks);
    if let Some(current) = field_owners.simple.get(&key).cloned() {
        let chained = format!("{current},{plugin}");
        field_owners.simple.insert(key, chained);
        return Ok(());
    }
    field_owners.simple.insert(key, plugin.to_string());
    Ok(())
}

fn ensure_linux(adjustment: &mut nri_api::ContainerAdjustment) -> &mut nri_api::LinuxContainerAdjustment {
    if adjustment.linux.is_none() {
        adjustment.linux = MessageField::some(nri_api::LinuxContainerAdjustment::new());
    }
    adjustment.linux.as_mut().expect("linux adjustment initialized")
}

fn ensure_linux_resources(
    adjustment: &mut nri_api::ContainerAdjustment,
) -> &mut nri_api::LinuxResources {
    let linux = ensure_linux(adjustment);
    if linux.resources.is_none() {
        linux.resources = MessageField::some(nri_api::LinuxResources::new());
    }
    linux.resources.as_mut().expect("linux resources initialized")
}

fn ensure_linux_memory(adjustment: &mut nri_api::ContainerAdjustment) -> &mut nri_api::LinuxMemory {
    let resources = ensure_linux_resources(adjustment);
    if resources.memory.is_none() {
        resources.memory = MessageField::some(nri_api::LinuxMemory::new());
    }
    resources.memory.as_mut().expect("linux memory initialized")
}

fn ensure_linux_cpu(adjustment: &mut nri_api::ContainerAdjustment) -> &mut nri_api::LinuxCPU {
    let resources = ensure_linux_resources(adjustment);
    if resources.cpu.is_none() {
        resources.cpu = MessageField::some(nri_api::LinuxCPU::new());
    }
    resources.cpu.as_mut().expect("linux cpu initialized")
}

fn ensure_hooks(adjustment: &mut nri_api::ContainerAdjustment) -> &mut nri_api::Hooks {
    if adjustment.hooks.is_none() {
        adjustment.hooks = MessageField::some(nri_api::Hooks::new());
    }
    adjustment.hooks.as_mut().expect("hooks initialized")
}

fn ensure_rdt(linux: &mut nri_api::LinuxContainerAdjustment) -> &mut nri_api::LinuxRdt {
    if linux.rdt.is_none() {
        linux.rdt = MessageField::some(nri_api::LinuxRdt::new());
    }
    linux.rdt.as_mut().expect("rdt initialized")
}

fn upsert_key_value(
    values: &mut Vec<nri_api::KeyValue>,
    key: &str,
    value: nri_api::KeyValue,
) {
    if let Some(existing) = values.iter_mut().find(|entry| {
        let (entry_key, _) = is_marked_for_removal(&entry.key);
        entry_key == key
    }) {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn upsert_mount(values: &mut Vec<nri_api::Mount>, key: &str, value: nri_api::Mount) {
    if let Some(existing) = values.iter_mut().find(|entry| {
        let (entry_key, _) = is_marked_for_removal(&entry.destination);
        entry_key == key
    }) {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn upsert_device(values: &mut Vec<nri_api::LinuxDevice>, key: &str, value: nri_api::LinuxDevice) {
    if let Some(existing) = values.iter_mut().find(|entry| {
        let (entry_key, _) = is_marked_for_removal(&entry.path);
        entry_key == key
    }) {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn upsert_namespace(
    values: &mut Vec<nri_api::LinuxNamespace>,
    key: &str,
    value: nri_api::LinuxNamespace,
) {
    if let Some(existing) = values.iter_mut().find(|entry| {
        let (entry_key, _) = is_marked_for_removal(&entry.type_);
        entry_key == key
    }) {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn upsert_cdi_device(
    values: &mut Vec<nri_api::CDIDevice>,
    key: &str,
    value: nri_api::CDIDevice,
) {
    if let Some(existing) = values.iter_mut().find(|entry| entry.name == key) {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn upsert_rlimit(
    values: &mut Vec<nri_api::POSIXRlimit>,
    key: &str,
    value: nri_api::POSIXRlimit,
) {
    if let Some(existing) = values.iter_mut().find(|entry| entry.type_ == key) {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn set_optional_int(target: &mut MessageField<nri_api::OptionalInt>, value: &MessageField<nri_api::OptionalInt>) {
    *target = value.clone();
}

fn set_optional_string(
    target: &mut MessageField<nri_api::OptionalString>,
    value: &MessageField<nri_api::OptionalString>,
) {
    *target = value.clone();
}

fn set_optional_bool(
    target: &mut MessageField<nri_api::OptionalBool>,
    value: &MessageField<nri_api::OptionalBool>,
) {
    *target = value.clone();
}

fn merge_resources(
    container_id: &str,
    merged: &mut nri_api::ContainerAdjustment,
    owners: &mut nri_api::OwningPlugins,
    plugin: &str,
    resources: &nri_api::LinuxResources,
) -> Result<()> {
    if let Some(memory) = resources.memory.as_ref() {
        if memory.limit.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemLimit, plugin)?;
            ensure_linux_memory(merged).limit = memory.limit.clone();
        }
        if memory.reservation.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemReservation, plugin)?;
            ensure_linux_memory(merged).reservation = memory.reservation.clone();
        }
        if memory.swap.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemSwapLimit, plugin)?;
            ensure_linux_memory(merged).swap = memory.swap.clone();
        }
        if memory.kernel.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemKernelLimit, plugin)?;
            ensure_linux_memory(merged).kernel = memory.kernel.clone();
        }
        if memory.kernel_tcp.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemTCPLimit, plugin)?;
            ensure_linux_memory(merged).kernel_tcp = memory.kernel_tcp.clone();
        }
        if memory.swappiness.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemSwappiness, plugin)?;
            ensure_linux_memory(merged).swappiness = memory.swappiness.clone();
        }
        if memory.disable_oom_killer.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemDisableOomKiller, plugin)?;
            ensure_linux_memory(merged).disable_oom_killer = memory.disable_oom_killer.clone();
        }
        if memory.use_hierarchy.is_some() {
            claim_simple(owners, container_id, nri_api::Field::MemUseHierarchy, plugin)?;
            ensure_linux_memory(merged).use_hierarchy = memory.use_hierarchy.clone();
        }
    }

    if let Some(cpu) = resources.cpu.as_ref() {
        if cpu.shares.is_some() {
            claim_simple(owners, container_id, nri_api::Field::CPUShares, plugin)?;
            ensure_linux_cpu(merged).shares = cpu.shares.clone();
        }
        if cpu.quota.is_some() {
            claim_simple(owners, container_id, nri_api::Field::CPUQuota, plugin)?;
            ensure_linux_cpu(merged).quota = cpu.quota.clone();
        }
        if cpu.period.is_some() {
            claim_simple(owners, container_id, nri_api::Field::CPUPeriod, plugin)?;
            ensure_linux_cpu(merged).period = cpu.period.clone();
        }
        if cpu.realtime_runtime.is_some() {
            claim_simple(owners, container_id, nri_api::Field::CPURealtimeRuntime, plugin)?;
            ensure_linux_cpu(merged).realtime_runtime = cpu.realtime_runtime.clone();
        }
        if cpu.realtime_period.is_some() {
            claim_simple(owners, container_id, nri_api::Field::CPURealtimePeriod, plugin)?;
            ensure_linux_cpu(merged).realtime_period = cpu.realtime_period.clone();
        }
        if !cpu.cpus.is_empty() {
            claim_simple(owners, container_id, nri_api::Field::CPUSetCPUs, plugin)?;
            ensure_linux_cpu(merged).cpus = cpu.cpus.clone();
        }
        if !cpu.mems.is_empty() {
            claim_simple(owners, container_id, nri_api::Field::CPUSetMems, plugin)?;
            ensure_linux_cpu(merged).mems = cpu.mems.clone();
        }
    }

    for hugepage in &resources.hugepage_limits {
        claim_compound(
            owners,
            container_id,
            nri_api::Field::HugepageLimits,
            &hugepage.page_size,
            plugin,
        )?;
        upsert_hugepage(ensure_linux_resources(merged), hugepage);
    }

    if let Some(blockio_class) = resources.blockio_class.as_ref() {
        claim_simple(owners, container_id, nri_api::Field::BlockioClass, plugin)?;
        set_optional_string(&mut ensure_linux_resources(merged).blockio_class, &MessageField::some(blockio_class.clone()));
    }
    if let Some(rdt_class) = resources.rdt_class.as_ref() {
        claim_simple(owners, container_id, nri_api::Field::RdtClass, plugin)?;
        set_optional_string(&mut ensure_linux_resources(merged).rdt_class, &MessageField::some(rdt_class.clone()));
    }
    for (key, value) in &resources.unified {
        claim_compound(
            owners,
            container_id,
            nri_api::Field::CgroupsUnified,
            key,
            plugin,
        )?;
        ensure_linux_resources(merged)
            .unified
            .insert(key.clone(), value.clone());
    }
    if let Some(pids) = resources.pids.as_ref() {
        claim_simple(owners, container_id, nri_api::Field::PidsLimit, plugin)?;
        ensure_linux_resources(merged).pids = MessageField::some(pids.clone());
    }

    Ok(())
}

fn upsert_hugepage(resources: &mut nri_api::LinuxResources, value: &nri_api::HugepageLimit) {
    if let Some(existing) = resources
        .hugepage_limits
        .iter_mut()
        .find(|entry| entry.page_size == value.page_size)
    {
        *existing = value.clone();
    } else {
        resources.hugepage_limits.push(value.clone());
    }
}

fn merge_linux_adjustment(
    container_id: &str,
    merged: &mut nri_api::ContainerAdjustment,
    owners: &mut nri_api::OwningPlugins,
    plugin: &str,
    linux: &nri_api::LinuxContainerAdjustment,
) -> Result<()> {
    for device in &linux.devices {
        let (path, removed) = is_marked_for_removal(&device.path);
        if removed {
            clear_compound(owners, container_id, nri_api::Field::Devices, &path, plugin);
            let mut removal = nri_api::LinuxDevice::new();
            removal.path = mark_for_removal(&path);
            upsert_device(&mut ensure_linux(merged).devices, &path, removal);
            continue;
        }
        claim_compound(owners, container_id, nri_api::Field::Devices, &path, plugin)?;
        upsert_device(&mut ensure_linux(merged).devices, &path, device.clone());
    }

    if let Some(resources) = linux.resources.as_ref() {
        merge_resources(container_id, merged, owners, plugin, resources)?;
    }
    if !linux.cgroups_path.is_empty() {
        claim_simple(owners, container_id, nri_api::Field::CgroupsPath, plugin)?;
        ensure_linux(merged).cgroups_path = linux.cgroups_path.clone();
    }
    if linux.oom_score_adj.is_some() {
        claim_simple(owners, container_id, nri_api::Field::OomScoreAdj, plugin)?;
        set_optional_int(
            &mut ensure_linux(merged).oom_score_adj,
            &linux.oom_score_adj,
        );
    }
    if let Some(io_priority) = linux.io_priority.as_ref() {
        claim_simple(owners, container_id, nri_api::Field::IoPriority, plugin)?;
        ensure_linux(merged).io_priority = MessageField::some(io_priority.clone());
    }
    if let Some(seccomp_policy) = linux.seccomp_policy.as_ref() {
        claim_simple(owners, container_id, nri_api::Field::SeccompPolicy, plugin)?;
        ensure_linux(merged).seccomp_policy = MessageField::some(seccomp_policy.clone());
    }
    for namespace in &linux.namespaces {
        let (ns_type, removed) = is_marked_for_removal(&namespace.type_);
        if removed {
            clear_compound(owners, container_id, nri_api::Field::Namespace, &ns_type, plugin);
            let mut removal = nri_api::LinuxNamespace::new();
            removal.type_ = mark_for_removal(&ns_type);
            upsert_namespace(&mut ensure_linux(merged).namespaces, &ns_type, removal);
            continue;
        }
        claim_compound(owners, container_id, nri_api::Field::Namespace, &ns_type, plugin)?;
        upsert_namespace(&mut ensure_linux(merged).namespaces, &ns_type, namespace.clone());
    }
    for (key, value) in &linux.sysctl {
        let (name, removed) = is_marked_for_removal(key);
        if removed {
            clear_compound(owners, container_id, nri_api::Field::Sysctl, &name, plugin);
            ensure_linux(merged).sysctl.insert(mark_for_removal(&name), String::new());
            ensure_linux(merged).sysctl.remove(&name);
            continue;
        }
        claim_compound(owners, container_id, nri_api::Field::Sysctl, &name, plugin)?;
        ensure_linux(merged).sysctl.remove(&mark_for_removal(&name));
        ensure_linux(merged).sysctl.insert(name, value.clone());
    }
    for (key, value) in &linux.net_devices {
        let (name, removed) = is_marked_for_removal(key);
        if removed {
            clear_compound(
                owners,
                container_id,
                nri_api::Field::LinuxNetDevices,
                &name,
                plugin,
            );
            ensure_linux(merged)
                .net_devices
                .insert(mark_for_removal(&name), nri_api::LinuxNetDevice::new());
            ensure_linux(merged).net_devices.remove(&name);
            continue;
        }
        claim_compound(
            owners,
            container_id,
            nri_api::Field::LinuxNetDevices,
            &name,
            plugin,
        )?;
        ensure_linux(merged).net_devices.remove(&mark_for_removal(&name));
        ensure_linux(merged)
            .net_devices
            .insert(name, value.clone());
    }
    if let Some(scheduler) = linux.scheduler.as_ref() {
        claim_simple(owners, container_id, nri_api::Field::LinuxSched, plugin)?;
        ensure_linux(merged).scheduler = MessageField::some(scheduler.clone());
    }
    if let Some(rdt) = linux.rdt.as_ref() {
        if rdt.remove {
            clear_simple(owners, container_id, nri_api::Field::RdtClosID, plugin);
            clear_simple(owners, container_id, nri_api::Field::RdtSchemata, plugin);
            clear_simple(owners, container_id, nri_api::Field::RdtEnableMonitoring, plugin);
            let mut merged_rdt = ensure_linux(merged)
                .rdt
                .take()
                .unwrap_or_else(nri_api::LinuxRdt::new);
            merged_rdt.remove = true;
            merged_rdt.clos_id = MessageField::none();
            merged_rdt.schemata = MessageField::none();
            merged_rdt.enable_monitoring = MessageField::none();
            ensure_linux(merged).rdt = MessageField::some(merged_rdt);
        } else {
            let merged_rdt = ensure_rdt(ensure_linux(merged));
            if rdt.clos_id.is_some() {
                claim_simple(owners, container_id, nri_api::Field::RdtClosID, plugin)?;
                merged_rdt.clos_id = rdt.clos_id.clone();
            }
            if rdt.schemata.is_some() {
                claim_simple(owners, container_id, nri_api::Field::RdtSchemata, plugin)?;
                merged_rdt.schemata = rdt.schemata.clone();
            }
            if rdt.enable_monitoring.is_some() {
                claim_simple(
                    owners,
                    container_id,
                    nri_api::Field::RdtEnableMonitoring,
                    plugin,
                )?;
                set_optional_bool(
                    &mut merged_rdt.enable_monitoring,
                    &rdt.enable_monitoring,
                );
            }
            merged_rdt.remove = false;
        }
    }

    Ok(())
}

pub fn merge_container_adjustments(
    container_id: &str,
    plugins: &[(String, nri_api::ContainerAdjustment)],
) -> Result<MergeResult> {
    let mut merged = nri_api::ContainerAdjustment::new();
    let mut owners = nri_api::OwningPlugins::new();

    for (plugin, adjustment) in plugins {
        for (key, value) in &adjustment.annotations {
            let (name, removed) = is_marked_for_removal(key);
            if removed {
                clear_compound(&mut owners, container_id, nri_api::Field::Annotations, &name, plugin);
                merged.annotations.remove(&name);
                merged.annotations.insert(mark_for_removal(&name), String::new());
                continue;
            }
            claim_compound(&mut owners, container_id, nri_api::Field::Annotations, &name, plugin)?;
            merged.annotations.remove(&mark_for_removal(&name));
            merged.annotations.insert(name, value.clone());
        }

        for mount in &adjustment.mounts {
            let (destination, removed) = is_marked_for_removal(&mount.destination);
            if removed {
                clear_compound(&mut owners, container_id, nri_api::Field::Mounts, &destination, plugin);
                let mut removal = nri_api::Mount::new();
                removal.destination = mark_for_removal(&destination);
                upsert_mount(&mut merged.mounts, &destination, removal);
                continue;
            }
            claim_compound(&mut owners, container_id, nri_api::Field::Mounts, &destination, plugin)?;
            upsert_mount(&mut merged.mounts, &destination, mount.clone());
        }

        for env in &adjustment.env {
            let (name, removed) = is_marked_for_removal(&env.key);
            if removed {
                clear_compound(&mut owners, container_id, nri_api::Field::Env, &name, plugin);
                let mut removal = nri_api::KeyValue::new();
                removal.key = mark_for_removal(&name);
                upsert_key_value(&mut merged.env, &name, removal);
                continue;
            }
            claim_compound(&mut owners, container_id, nri_api::Field::Env, &name, plugin)?;
            upsert_key_value(&mut merged.env, &name, env.clone());
        }

        if let Some(hooks) = adjustment.hooks.as_ref() {
            let mut changed = false;
            let merged_hooks = ensure_hooks(&mut merged);
            if !hooks.prestart.is_empty() {
                merged_hooks.prestart.extend(hooks.prestart.clone());
                changed = true;
            }
            if !hooks.create_runtime.is_empty() {
                merged_hooks.create_runtime.extend(hooks.create_runtime.clone());
                changed = true;
            }
            if !hooks.create_container.is_empty() {
                merged_hooks.create_container.extend(hooks.create_container.clone());
                changed = true;
            }
            if !hooks.start_container.is_empty() {
                merged_hooks.start_container.extend(hooks.start_container.clone());
                changed = true;
            }
            if !hooks.poststart.is_empty() {
                merged_hooks.poststart.extend(hooks.poststart.clone());
                changed = true;
            }
            if !hooks.poststop.is_empty() {
                merged_hooks.poststop.extend(hooks.poststop.clone());
                changed = true;
            }
            if changed {
                claim_hooks(&mut owners, container_id, plugin)?;
            }
        }

        if let Some(linux) = adjustment.linux.as_ref() {
            merge_linux_adjustment(container_id, &mut merged, &mut owners, plugin, linux)?;
        }

        for rlimit in &adjustment.rlimits {
            claim_compound(
                &mut owners,
                container_id,
                nri_api::Field::Rlimits,
                &rlimit.type_,
                plugin,
            )?;
            upsert_rlimit(&mut merged.rlimits, &rlimit.type_, rlimit.clone());
        }

        for device in &adjustment.CDI_devices {
            claim_compound(
                &mut owners,
                container_id,
                nri_api::Field::CdiDevices,
                &device.name,
                plugin,
            )?;
            upsert_cdi_device(&mut merged.CDI_devices, &device.name, device.clone());
        }

        if !adjustment.args.is_empty() {
            let args = if adjustment.args.first().is_some_and(|v| v.is_empty()) {
                clear_simple(&mut owners, container_id, nri_api::Field::Args, plugin);
                adjustment.args[1..].to_vec()
            } else {
                adjustment.args.clone()
            };
            claim_simple(&mut owners, container_id, nri_api::Field::Args, plugin)?;
            merged.args = args;
        }
    }

    Ok(MergeResult {
        adjustment: merged,
        owners,
    })
}

pub fn merge_annotation_adjustments(
    plugins: &[(String, HashMap<String, String>)],
) -> Result<HashMap<String, String>> {
    let adjustments = plugins
        .iter()
        .map(|(plugin, annotations)| {
            let mut adjustment = nri_api::ContainerAdjustment::new();
            adjustment.annotations = annotations.clone();
            (plugin.clone(), adjustment)
        })
        .collect::<Vec<_>>();
    Ok(merge_container_adjustments("container", &adjustments)?
        .adjustment
        .annotations)
}

fn has_linux_resources(resources: &nri_api::LinuxResources) -> bool {
    resources.memory.is_some()
        || resources.cpu.is_some()
        || !resources.hugepage_limits.is_empty()
        || resources.blockio_class.is_some()
        || resources.rdt_class.is_some()
        || !resources.unified.is_empty()
        || resources.pids.is_some()
}

fn overlay_linux_resources(base: &mut nri_api::LinuxResources, delta: &nri_api::LinuxResources) {
    if let Some(memory) = delta.memory.as_ref() {
        if base.memory.is_none() {
            base.memory = MessageField::some(nri_api::LinuxMemory::new());
        }
        let target = base.memory.as_mut().expect("linux memory initialized");
        if memory.limit.is_some() {
            target.limit = memory.limit.clone();
        }
        if memory.reservation.is_some() {
            target.reservation = memory.reservation.clone();
        }
        if memory.swap.is_some() {
            target.swap = memory.swap.clone();
        }
        if memory.kernel.is_some() {
            target.kernel = memory.kernel.clone();
        }
        if memory.kernel_tcp.is_some() {
            target.kernel_tcp = memory.kernel_tcp.clone();
        }
        if memory.swappiness.is_some() {
            target.swappiness = memory.swappiness.clone();
        }
        if memory.disable_oom_killer.is_some() {
            target.disable_oom_killer = memory.disable_oom_killer.clone();
        }
        if memory.use_hierarchy.is_some() {
            target.use_hierarchy = memory.use_hierarchy.clone();
        }
    }

    if let Some(cpu) = delta.cpu.as_ref() {
        if base.cpu.is_none() {
            base.cpu = MessageField::some(nri_api::LinuxCPU::new());
        }
        let target = base.cpu.as_mut().expect("linux cpu initialized");
        if cpu.shares.is_some() {
            target.shares = cpu.shares.clone();
        }
        if cpu.quota.is_some() {
            target.quota = cpu.quota.clone();
        }
        if cpu.period.is_some() {
            target.period = cpu.period.clone();
        }
        if cpu.realtime_runtime.is_some() {
            target.realtime_runtime = cpu.realtime_runtime.clone();
        }
        if cpu.realtime_period.is_some() {
            target.realtime_period = cpu.realtime_period.clone();
        }
        if !cpu.cpus.is_empty() {
            target.cpus = cpu.cpus.clone();
        }
        if !cpu.mems.is_empty() {
            target.mems = cpu.mems.clone();
        }
    }

    for hugepage in &delta.hugepage_limits {
        upsert_hugepage(base, hugepage);
    }
    if delta.blockio_class.is_some() {
        base.blockio_class = delta.blockio_class.clone();
    }
    if delta.rdt_class.is_some() {
        base.rdt_class = delta.rdt_class.clone();
    }
    if delta.pids.is_some() {
        base.pids = delta.pids.clone();
    }
    for (key, value) in &delta.unified {
        base.unified.insert(key.clone(), value.clone());
    }
}

pub fn merge_container_updates(
    target_container_id: &str,
    requested_linux_resources: Option<&nri_api::LinuxResources>,
    plugins: &[(String, Vec<nri_api::ContainerUpdate>)],
) -> Result<MergedContainerUpdates> {
    let mut merged_resources = HashMap::<String, nri_api::LinuxResources>::new();
    let mut owners = nri_api::OwningPlugins::new();
    let mut ignore_failure = HashMap::<String, bool>::new();
    let mut order = Vec::<String>::new();

    for (plugin, updates) in plugins {
        for update in updates {
            let container_id = update.container_id.clone();
            if !merged_resources.contains_key(&container_id) {
                merged_resources.insert(container_id.clone(), nri_api::LinuxResources::new());
                ignore_failure.insert(container_id.clone(), update.ignore_failure);
                order.push(container_id.clone());
            } else if let Some(existing) = ignore_failure.get_mut(&container_id) {
                *existing = *existing && update.ignore_failure;
            }

            if let Some(resources) = update.linux.as_ref().and_then(|linux| linux.resources.as_ref()) {
                let mut delta = nri_api::ContainerAdjustment::new();
                merge_resources(&container_id, &mut delta, &mut owners, plugin, resources)?;
                if let Some(delta_resources) = delta
                    .linux
                    .as_ref()
                    .and_then(|linux| linux.resources.as_ref())
                {
                    let merged = merged_resources
                        .get_mut(&container_id)
                        .expect("merged resources initialized");
                    overlay_linux_resources(merged, delta_resources);
                }
            }
        }
    }

    let target_linux_resources = if let Some(delta) = merged_resources.remove(target_container_id) {
        let mut target = requested_linux_resources.cloned().unwrap_or_default();
        overlay_linux_resources(&mut target, &delta);
        Some(target)
    } else {
        requested_linux_resources.cloned()
    };

    let updates = order
        .into_iter()
        .filter(|container_id| container_id != target_container_id)
        .filter_map(|container_id| {
            let resources = merged_resources.remove(&container_id)?;
            if !has_linux_resources(&resources) {
                return None;
            }
            let mut linux = nri_api::LinuxContainerUpdate::new();
            linux.resources = MessageField::some(resources);
            Some(nri_api::ContainerUpdate {
                container_id: container_id.clone(),
                linux: MessageField::some(linux),
                ignore_failure: ignore_failure.remove(&container_id).unwrap_or(false),
                ..Default::default()
            })
        })
        .collect();

    Ok(MergedContainerUpdates {
        target_linux_resources,
        updates,
    })
}

#[cfg(test)]
mod tests {
    use super::{mark_for_removal, merge_container_adjustments, merge_container_updates};
    use crate::nri_proto::api as nri_api;
    use protobuf::MessageField;

    #[test]
    fn merges_annotation_removal_then_replacement_for_same_plugin() {
        let mut first = nri_api::ContainerAdjustment::new();
        first.annotations.insert(mark_for_removal("a"), String::new());
        let mut second = nri_api::ContainerAdjustment::new();
        second.annotations.insert("a".to_string(), "2".to_string());

        let result = merge_container_adjustments(
            "ctr-1",
            &[("p1".to_string(), first), ("p1".to_string(), second)],
        )
        .unwrap();

        assert_eq!(result.adjustment.annotations.get("a"), Some(&"2".to_string()));
        assert!(!result.adjustment.annotations.contains_key("-a"));
        let owners = result.owners.owners.get("ctr-1").unwrap();
        let field = owners
            .compound
            .get(&(nri_api::Field::Annotations as i32))
            .unwrap();
        assert_eq!(field.owners.get("a"), Some(&"p1".to_string()));
    }

    #[test]
    fn detects_conflicting_annotation_owners() {
        let mut first = nri_api::ContainerAdjustment::new();
        first.annotations.insert("a".to_string(), "1".to_string());
        let mut second = nri_api::ContainerAdjustment::new();
        second.annotations.insert("a".to_string(), "2".to_string());

        let err = merge_container_adjustments(
            "ctr-1",
            &[("p1".to_string(), first), ("p2".to_string(), second)],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("both tried to set Annotations"));
    }

    #[test]
    fn merges_update_resources_for_target_and_side_effects() {
        let mut requested = nri_api::LinuxResources::new();
        let mut requested_cpu = nri_api::LinuxCPU::new();
        let mut requested_shares = nri_api::OptionalUInt64::new();
        requested_shares.value = 512;
        requested_cpu.shares = MessageField::some(requested_shares);
        requested.cpu = MessageField::some(requested_cpu);

        let mut target_update = nri_api::ContainerUpdate::new();
        target_update.container_id = "target".to_string();
        let mut target_linux = nri_api::LinuxContainerUpdate::new();
        let mut target_resources = nri_api::LinuxResources::new();
        let mut target_memory = nri_api::LinuxMemory::new();
        let mut limit = nri_api::OptionalInt64::new();
        limit.value = 4096;
        target_memory.limit = MessageField::some(limit);
        target_resources.memory = MessageField::some(target_memory);
        target_linux.resources = MessageField::some(target_resources);
        target_update.linux = MessageField::some(target_linux);

        let mut other_update = nri_api::ContainerUpdate::new();
        other_update.container_id = "other".to_string();
        other_update.ignore_failure = true;
        let mut other_linux = nri_api::LinuxContainerUpdate::new();
        let mut other_resources = nri_api::LinuxResources::new();
        let mut other_cpu = nri_api::LinuxCPU::new();
        let mut other_quota = nri_api::OptionalInt64::new();
        other_quota.value = 1234;
        other_cpu.quota = MessageField::some(other_quota);
        other_resources.cpu = MessageField::some(other_cpu);
        other_linux.resources = MessageField::some(other_resources);
        other_update.linux = MessageField::some(other_linux);

        let result = merge_container_updates(
            "target",
            Some(&requested),
            &[("plugin-a".to_string(), vec![target_update, other_update])],
        )
        .unwrap();

        let target_cpu = result
            .target_linux_resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .unwrap();
        assert_eq!(target_cpu.shares.as_ref().map(|value| value.value), Some(512));
        let target_memory = result
            .target_linux_resources
            .as_ref()
            .and_then(|resources| resources.memory.as_ref())
            .unwrap();
        assert_eq!(target_memory.limit.as_ref().map(|value| value.value), Some(4096));
        assert_eq!(result.updates.len(), 1);
        assert_eq!(result.updates[0].container_id, "other");
        assert!(result.updates[0].ignore_failure);
    }

    #[test]
    fn detects_conflicting_update_resource_owners() {
        let mut first = nri_api::ContainerUpdate::new();
        first.container_id = "target".to_string();
        let mut first_linux = nri_api::LinuxContainerUpdate::new();
        let mut first_resources = nri_api::LinuxResources::new();
        let mut first_cpu = nri_api::LinuxCPU::new();
        let mut first_shares = nri_api::OptionalUInt64::new();
        first_shares.value = 100;
        first_cpu.shares = MessageField::some(first_shares);
        first_resources.cpu = MessageField::some(first_cpu);
        first_linux.resources = MessageField::some(first_resources);
        first.linux = MessageField::some(first_linux);

        let mut second = nri_api::ContainerUpdate::new();
        second.container_id = "target".to_string();
        let mut second_linux = nri_api::LinuxContainerUpdate::new();
        let mut second_resources = nri_api::LinuxResources::new();
        let mut second_cpu = nri_api::LinuxCPU::new();
        let mut second_shares = nri_api::OptionalUInt64::new();
        second_shares.value = 200;
        second_cpu.shares = MessageField::some(second_shares);
        second_resources.cpu = MessageField::some(second_cpu);
        second_linux.resources = MessageField::some(second_resources);
        second.linux = MessageField::some(second_linux);

        let err = merge_container_updates(
            "target",
            None,
            &[
                ("plugin-a".to_string(), vec![first]),
                ("plugin-b".to_string(), vec![second]),
            ],
        )
        .unwrap_err();

        assert!(format!("{err}").contains("both tried to set CPUShares"));
    }

    #[test]
    fn merges_resources_and_tracks_simple_owners() {
        let mut adjust = nri_api::ContainerAdjustment::new();
        let mut linux = nri_api::LinuxContainerAdjustment::new();
        let mut resources = nri_api::LinuxResources::new();
        let mut cpu = nri_api::LinuxCPU::new();
        let mut shares = nri_api::OptionalUInt64::new();
        shares.value = 1024;
        cpu.shares = MessageField::some(shares);
        resources.cpu = MessageField::some(cpu);
        let mut memory = nri_api::LinuxMemory::new();
        let mut limit = nri_api::OptionalInt64::new();
        limit.value = 4096;
        memory.limit = MessageField::some(limit);
        resources.memory = MessageField::some(memory);
        linux.resources = MessageField::some(resources);
        adjust.linux = MessageField::some(linux);

        let result = merge_container_adjustments("ctr-1", &[("p1".to_string(), adjust)]).unwrap();
        let cpu = result
            .adjustment
            .linux
            .as_ref()
            .and_then(|linux| linux.resources.as_ref())
            .and_then(|resources| resources.cpu.as_ref())
            .unwrap();
        assert_eq!(cpu.shares.as_ref().map(|v| v.value), Some(1024));
        let owners = result.owners.owners.get("ctr-1").unwrap();
        assert_eq!(
            owners.simple.get(&(nri_api::Field::CPUShares as i32)),
            Some(&"p1".to_string())
        );
        assert_eq!(
            owners.simple.get(&(nri_api::Field::MemLimit as i32)),
            Some(&"p1".to_string())
        );
    }

    #[test]
    fn merges_compound_entries_with_removals() {
        let mut first = nri_api::ContainerAdjustment::new();
        first.env.push(nri_api::KeyValue {
            key: "A".to_string(),
            value: "1".to_string(),
            ..Default::default()
        });
        let mut second = nri_api::ContainerAdjustment::new();
        second.env.push(nri_api::KeyValue {
            key: mark_for_removal("A"),
            value: String::new(),
            ..Default::default()
        });

        let result = merge_container_adjustments(
            "ctr-1",
            &[("p1".to_string(), first), ("p1".to_string(), second)],
        )
        .unwrap();

        assert_eq!(result.adjustment.env.len(), 1);
        assert_eq!(result.adjustment.env[0].key, "-A");
        let owners = result.owners.owners.get("ctr-1").unwrap();
        let env_owners = owners.compound.get(&(nri_api::Field::Env as i32)).unwrap();
        assert_eq!(env_owners.owners.get("A"), Some(&"-p1".to_string()));
    }

    #[test]
    fn hooks_are_chained_instead_of_conflicting() {
        let mut first = nri_api::ContainerAdjustment::new();
        let mut first_hooks = nri_api::Hooks::new();
        first_hooks.prestart.push(nri_api::Hook {
            path: "/bin/a".to_string(),
            ..Default::default()
        });
        first.hooks = MessageField::some(first_hooks);

        let mut second = nri_api::ContainerAdjustment::new();
        let mut second_hooks = nri_api::Hooks::new();
        second_hooks.prestart.push(nri_api::Hook {
            path: "/bin/b".to_string(),
            ..Default::default()
        });
        second.hooks = MessageField::some(second_hooks);

        let result = merge_container_adjustments(
            "ctr-1",
            &[("p1".to_string(), first), ("p2".to_string(), second)],
        )
        .unwrap();

        let hooks = result.adjustment.hooks.as_ref().unwrap();
        assert_eq!(hooks.prestart.len(), 2);
        let owners = result.owners.owners.get("ctr-1").unwrap();
        assert_eq!(
            owners.simple.get(&(nri_api::Field::OciHooks as i32)),
            Some(&"p1,p2".to_string())
        );
    }

    #[test]
    fn args_update_requires_clear_before_replacing_other_plugin() {
        let mut first = nri_api::ContainerAdjustment::new();
        first.args = vec!["sleep".to_string(), "10".to_string()];
        let mut second = nri_api::ContainerAdjustment::new();
        second.args = vec!["echo".to_string(), "hello".to_string()];

        let err = merge_container_adjustments(
            "ctr-1",
            &[("p1".to_string(), first), ("p2".to_string(), second)],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("Args"));
    }
}
