use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

use crate::oci::spec::{
    Device, Hook, Hooks, Linux, LinuxDeviceCgroup, LinuxResources, Mount, Process, Spec, User,
};

const DEFAULT_CDI_SPEC_DIRS: [&str; 2] = ["/etc/cdi", "/var/run/cdi"];

#[derive(Debug, Error)]
pub enum CdiError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, CdiError>;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CdiSpec {
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
pub struct CdiContainerEdits {
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

pub fn spec_dirs(configured_dirs: Option<&[String]>) -> Vec<PathBuf> {
    configured_dirs
        .filter(|dirs| !dirs.is_empty())
        .map(|dirs| dirs.iter().map(PathBuf::from).collect())
        .or_else(|| {
            std::env::var("CDI_SPEC_DIRS")
                .ok()
                .map(|dirs| {
                    dirs.split(':')
                        .filter(|entry| !entry.is_empty())
                        .map(PathBuf::from)
                        .collect::<Vec<_>>()
                })
                .filter(|dirs| !dirs.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_CDI_SPEC_DIRS.iter().map(PathBuf::from).collect())
}

pub fn load_specs(configured_dirs: Option<&[String]>) -> Result<Vec<CdiSpec>> {
    let mut specs = Vec::new();
    for dir in spec_dirs(configured_dirs) {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(CdiError::Io(format!(
                    "failed to read CDI directory {}: {}",
                    dir.display(),
                    err
                )))
            }
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                CdiError::Io(format!(
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
                CdiError::Io(format!(
                    "failed to read CDI spec {}: {}",
                    path.display(),
                    err
                ))
            })?;
            let spec = serde_json::from_str::<CdiSpec>(&content).map_err(|err| {
                CdiError::Parse(format!(
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

fn merge_edits(base: &mut CdiContainerEdits, extra: &CdiContainerEdits) {
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

pub fn resolve_edits(
    device_ref: &str,
    configured_dirs: Option<&[String]>,
) -> Result<CdiContainerEdits> {
    let Some((kind, name)) = device_ref.split_once('=') else {
        return Err(CdiError::Invalid(format!(
            "invalid CDI device reference {device_ref:?}, expected <vendor>/<class>=<device>"
        )));
    };

    for spec in load_specs(configured_dirs)? {
        if spec.kind != kind {
            continue;
        }
        if let Some(device) = spec.devices.iter().find(|device| device.name == name) {
            let mut edits = spec.container_edits.clone();
            merge_edits(&mut edits, &device.container_edits);
            return Ok(edits);
        }
    }

    Err(CdiError::Invalid(format!(
        "CDI device {device_ref} not found in configured CDI specs"
    )))
}

fn ensure_process(spec: &mut Spec) -> &mut Process {
    spec.process.get_or_insert(Process {
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
        devices: None,
        memory: None,
        cpu: None,
        pids: None,
        block_io: None,
        hugepage_limits: None,
        network: None,
        intel_rdt: None,
        unified: None,
    })
}

fn env_name(entry: &str) -> &str {
    entry.split_once('=').map(|(name, _)| name).unwrap_or(entry)
}

fn apply_env(spec: &mut Spec, env: &[String]) {
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

fn apply_device_nodes(spec: &mut Spec, nodes: &[CdiDeviceNode]) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    for node in nodes {
        if node.path.is_empty() {
            return Err(CdiError::Invalid(
                "CDI device node path must not be empty".to_string(),
            ));
        }
        let device_type = if node.device_type.is_empty() {
            "c".to_string()
        } else {
            node.device_type.clone()
        };
        let updated = Device {
            device_type: device_type.clone(),
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
            device_type: Some(device_type),
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
            let entries = resources.get_or_insert_with(Vec::new);
            if let Some(existing) = entries.iter_mut().find(|device| {
                device.device_type == resource.device_type
                    && device.major == resource.major
                    && device.minor == resource.minor
            }) {
                *existing = resource;
            } else {
                entries.push(resource);
            }
        }
    }

    Ok(())
}

fn apply_mounts(spec: &mut Spec, mounts: &[CdiMount]) -> Result<()> {
    if mounts.is_empty() {
        return Ok(());
    }

    let entries = spec.mounts.get_or_insert_with(Vec::new);
    for mount in mounts {
        if mount.host_path.is_empty() || mount.container_path.is_empty() {
            return Err(CdiError::Invalid(
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

fn apply_hooks(spec: &mut Spec, hooks: &[CdiHook]) -> Result<()> {
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
            return Err(CdiError::Invalid(
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
                return Err(CdiError::Invalid(format!(
                    "unsupported CDI hook name {other:?}"
                )))
            }
        }
    }

    Ok(())
}

fn apply_intel_rdt(spec: &mut Spec, intel_rdt: &CdiIntelRdt) {
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

fn apply_additional_gids(spec: &mut Spec, gids: &[u32]) {
    if gids.is_empty() {
        return;
    }

    let user = ensure_process(spec).user.get_or_insert(User {
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

fn apply_net_devices(spec: &mut Spec, devices: &[CdiNetDevice]) -> Result<()> {
    if devices.is_empty() {
        return Ok(());
    }

    let entries = &mut ensure_linux(spec).net_devices;
    let current = entries.get_or_insert_with(HashMap::new);
    for device in devices {
        if device.host_interface_name.is_empty() || device.name.is_empty() {
            return Err(CdiError::Invalid(
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

pub fn apply_edits(spec: &mut Spec, edits: &CdiContainerEdits) -> Result<()> {
    apply_env(spec, &edits.env);
    apply_device_nodes(spec, &edits.device_nodes)?;
    apply_mounts(spec, &edits.mounts)?;
    apply_hooks(spec, &edits.hooks)?;
    apply_net_devices(spec, &edits.net_devices)?;
    if let Some(intel_rdt) = edits.intel_rdt.as_ref() {
        apply_intel_rdt(spec, intel_rdt);
    }
    apply_additional_gids(spec, &edits.additional_gids);
    Ok(())
}

pub fn apply_cdi_devices(
    spec: &mut Spec,
    devices: &[String],
    enabled: bool,
    configured_dirs: Option<&[String]>,
) -> Result<()> {
    if !enabled && !devices.is_empty() {
        return Err(CdiError::Invalid(
            "CDI device adjustments are disabled by configuration".to_string(),
        ));
    }
    for device in devices {
        let edits = resolve_edits(device, configured_dirs)?;
        apply_edits(spec, &edits)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn applies_cdi_device_edits_independent_from_nri_adjustment() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("vendor-device.json"),
            r#"{
                "cdiVersion": "0.7.0",
                "kind": "vendor.com/device",
                "containerEdits": {
                    "env": ["GLOBAL=1"]
                },
                "devices": [{
                    "name": "gpu0",
                    "containerEdits": {
                        "env": ["DEVICE=1"],
                        "deviceNodes": [{
                            "path": "/dev/fake-gpu0",
                            "type": "c",
                            "major": 195,
                            "minor": 0,
                            "permissions": "rw"
                        }]
                    }
                }]
            }"#,
        )
        .unwrap();
        let dirs = vec![dir.path().display().to_string()];
        let mut spec = Spec::new("1.0.2");

        apply_cdi_devices(
            &mut spec,
            &["vendor.com/device=gpu0".to_string()],
            true,
            Some(&dirs),
        )
        .unwrap();

        let env = spec.process.unwrap().env.unwrap();
        assert!(env.contains(&"GLOBAL=1".to_string()));
        assert!(env.contains(&"DEVICE=1".to_string()));
        let linux = spec.linux.unwrap();
        assert_eq!(linux.devices.unwrap()[0].path, "/dev/fake-gpu0");
        assert_eq!(
            linux.resources.unwrap().devices.unwrap()[0]
                .access
                .as_deref(),
            Some("rw")
        );
    }

    #[test]
    fn rejects_invalid_cdi_reference() {
        let mut spec = Spec::new("1.0.2");
        let err = apply_cdi_devices(&mut spec, &["invalid".to_string()], true, None).unwrap_err();
        assert!(err
            .to_string()
            .contains("expected <vendor>/<class>=<device>"));
    }
}
