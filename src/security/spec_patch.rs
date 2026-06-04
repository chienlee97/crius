use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::Result;

use crate::oci::spec::{
    Linux, LinuxCapabilities, LinuxDeviceCgroup, LinuxResources, Process, Root, Seccomp, Spec,
};
use crate::proto::runtime::v1::Capability;
use crate::security::devices::{self, DeviceMapping};

type ProcPathOverrides = (Option<Vec<String>>, Option<Vec<String>>);

#[derive(Debug, Clone)]
pub struct SpecPatchInput<'a> {
    pub privileged: bool,
    pub tty: bool,
    pub readonly_rootfs: bool,
    pub no_new_privileges: Option<bool>,
    pub apparmor_profile: Option<&'a str>,
    pub selinux_label: Option<&'a str>,
    pub seccomp: Option<Seccomp>,
    pub capabilities: Option<&'a Capability>,
    pub default_capabilities: &'a [String],
    pub add_inheritable_capabilities: bool,
    pub requested_devices: &'a [DeviceMapping],
    pub additional_devices: &'a [DeviceMapping],
    pub existing_cgroup_rules: &'a [LinuxDeviceCgroup],
    pub allowed_devices: &'a HashSet<PathBuf>,
    pub device_ownership_from_security_context: bool,
    pub user: Option<&'a str>,
    pub run_as_group: Option<u32>,
    pub privileged_without_host_devices: bool,
    pub privileged_without_host_devices_all_devices_allowed: bool,
    pub rootless: bool,
    pub cgroup_devices_enabled: bool,
    pub disable_proc_mount: bool,
    pub masked_paths: &'a [String],
    pub readonly_paths: &'a [String],
}

#[derive(Debug, Clone)]
pub struct SpecSecurityPatch {
    pub root_readonly: bool,
    pub process: ProcessSecurityPatch,
    pub linux: LinuxSecurityPatch,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProcessSecurityPatch {
    pub capabilities: LinuxCapabilities,
    pub no_new_privileges: bool,
    pub apparmor_profile: Option<String>,
    pub selinux_label: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LinuxSecurityPatch {
    pub devices: Vec<crate::oci::spec::Device>,
    pub device_cgroup_rules: Vec<LinuxDeviceCgroup>,
    pub seccomp: Option<Seccomp>,
    pub mount_label: Option<String>,
    pub masked_paths: Option<Vec<String>>,
    pub readonly_paths: Option<Vec<String>>,
    pub cgroup_devices_enabled: bool,
}

impl SpecSecurityPatch {
    pub fn from_input(input: SpecPatchInput<'_>) -> Result<Self> {
        let devices = devices::resolve_devices(devices::DeviceResolverInput {
            privileged: input.privileged,
            tty: input.tty,
            requested_devices: input.requested_devices,
            additional_devices: input.additional_devices,
            existing_cgroup_rules: input.existing_cgroup_rules,
            allowed_devices: input.allowed_devices,
            device_ownership_from_security_context: input.device_ownership_from_security_context,
            user: input.user,
            run_as_group: input.run_as_group,
            privileged_without_host_devices: input.privileged_without_host_devices,
            privileged_without_host_devices_all_devices_allowed: input
                .privileged_without_host_devices_all_devices_allowed,
            rootless: input.rootless,
        })?;
        let (masked_paths, readonly_paths) = effective_proc_paths(
            input.privileged,
            input.disable_proc_mount,
            input.masked_paths,
            input.readonly_paths,
        )?;

        Ok(Self {
            root_readonly: input.readonly_rootfs,
            process: ProcessSecurityPatch {
                capabilities: apply_capability_overrides(
                    &capability_baseline(input.default_capabilities, input.privileged),
                    input.capabilities,
                    input.add_inheritable_capabilities,
                ),
                no_new_privileges: input.no_new_privileges.unwrap_or(!input.privileged),
                apparmor_profile: input.apparmor_profile.map(ToString::to_string),
                selinux_label: input.selinux_label.map(ToString::to_string),
            },
            linux: LinuxSecurityPatch {
                devices: devices.devices,
                device_cgroup_rules: devices.cgroup_rules,
                seccomp: input.seccomp,
                mount_label: input.selinux_label.map(ToString::to_string),
                masked_paths,
                readonly_paths,
                cgroup_devices_enabled: input.cgroup_devices_enabled,
            },
            degraded_reasons: devices.degraded_reasons,
        })
    }

    pub fn apply_root(&self, root: &mut Root) {
        root.readonly = Some(self.root_readonly);
    }

    pub fn apply_process(&self, process: &mut Process) {
        process.capabilities = Some(self.process.capabilities.clone());
        process.no_new_privileges = Some(self.process.no_new_privileges);
        process.apparmor_profile = self.process.apparmor_profile.clone();
        process.selinux_label = self.process.selinux_label.clone();
    }

    pub fn apply_linux(&self, linux: &mut Linux) {
        linux.devices = Some(self.linux.devices.clone());
        linux.seccomp = self.linux.seccomp.clone();
        linux.mount_label = self.linux.mount_label.clone();
        linux.masked_paths = self.linux.masked_paths.clone();
        linux.readonly_paths = self.linux.readonly_paths.clone();

        if self.linux.cgroup_devices_enabled {
            let resources = linux.resources.get_or_insert_with(empty_linux_resources);
            resources.devices = Some(self.linux.device_cgroup_rules.clone());
        }
    }

    pub fn apply_spec(&self, spec: &mut Spec) {
        if let Some(root) = spec.root.as_mut() {
            self.apply_root(root);
        }
        if let Some(process) = spec.process.as_mut() {
            self.apply_process(process);
        }
        if let Some(linux) = spec.linux.as_mut() {
            self.apply_linux(linux);
        }
    }
}

pub fn validate_rootless_requests(input: &SpecPatchInput<'_>) -> Result<()> {
    devices::resolve_devices(devices::DeviceResolverInput {
        privileged: input.privileged,
        tty: input.tty,
        requested_devices: input.requested_devices,
        additional_devices: input.additional_devices,
        existing_cgroup_rules: input.existing_cgroup_rules,
        allowed_devices: input.allowed_devices,
        device_ownership_from_security_context: input.device_ownership_from_security_context,
        user: input.user,
        run_as_group: input.run_as_group,
        privileged_without_host_devices: input.privileged_without_host_devices,
        privileged_without_host_devices_all_devices_allowed: input
            .privileged_without_host_devices_all_devices_allowed,
        rootless: true,
    })
    .map(|_| ())
}

pub fn normalize_capability_name(name: &str) -> String {
    let upper = name.trim().to_ascii_uppercase();
    if upper.starts_with("CAP_") {
        upper
    } else {
        format!("CAP_{upper}")
    }
}

pub fn default_capabilities() -> Vec<String> {
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

pub fn capability_baseline(default_capabilities: &[String], privileged: bool) -> Vec<String> {
    if privileged {
        privileged_capabilities()
    } else {
        default_capabilities.to_vec()
    }
}

pub fn apply_capability_overrides(
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
            .map(|cap| normalize_capability_name(cap))
            .collect();

        if normalized_drops.iter().any(|cap| cap == "CAP_ALL") {
            base.clear();
        } else {
            base.retain(|cap| !normalized_drops.iter().any(|drop| drop == cap));
        }

        for cap in &capabilities.add_capabilities {
            let normalized = normalize_capability_name(cap);
            if !base.contains(&normalized) {
                base.push(normalized);
            }
        }

        ambient = capabilities
            .add_ambient_capabilities
            .iter()
            .map(|cap| normalize_capability_name(cap))
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

pub fn effective_proc_paths(
    privileged: bool,
    disable_proc_mount: bool,
    requested_masked_paths: &[String],
    requested_readonly_paths: &[String],
) -> Result<ProcPathOverrides> {
    if privileged {
        return Ok((None, None));
    }

    if disable_proc_mount {
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

fn empty_linux_resources() -> LinuxResources {
    LinuxResources {
        network: None,
        pids: None,
        memory: None,
        cpu: None,
        block_io: None,
        hugepage_limits: None,
        devices: None,
        intel_rdt: None,
        unified: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_applies_process_labels_capabilities_root_and_proc_defaults() {
        let input = SpecPatchInput {
            privileged: false,
            tty: false,
            readonly_rootfs: true,
            no_new_privileges: None,
            apparmor_profile: Some("crius-default"),
            selinux_label: Some("system_u:system_r:container_t:s0"),
            seccomp: None,
            capabilities: None,
            default_capabilities: &["CAP_CHOWN".to_string()],
            add_inheritable_capabilities: false,
            requested_devices: &[],
            additional_devices: &[],
            existing_cgroup_rules: &[],
            allowed_devices: &HashSet::new(),
            device_ownership_from_security_context: false,
            user: None,
            run_as_group: None,
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            rootless: false,
            cgroup_devices_enabled: true,
            disable_proc_mount: true,
            masked_paths: &[],
            readonly_paths: &[],
        };

        let patch = SpecSecurityPatch::from_input(input).unwrap();
        let mut spec = Spec::new("1.0.2");
        spec.root = Some(Root {
            path: "/rootfs".to_string(),
            readonly: None,
        });
        spec.process = Some(Process {
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
        spec.linux = Some(Linux {
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

        patch.apply_spec(&mut spec);
        let root = spec.root.unwrap();
        let process = spec.process.unwrap();
        let linux = spec.linux.unwrap();

        assert_eq!(root.readonly, Some(true));
        assert_eq!(process.no_new_privileges, Some(true));
        assert_eq!(process.apparmor_profile.as_deref(), Some("crius-default"));
        assert_eq!(
            process.selinux_label.as_deref(),
            Some("system_u:system_r:container_t:s0")
        );
        assert_eq!(
            process
                .capabilities
                .and_then(|capabilities| capabilities.bounding)
                .unwrap(),
            vec!["CAP_CHOWN".to_string()]
        );
        assert_eq!(
            linux.mount_label.as_deref(),
            Some("system_u:system_r:container_t:s0")
        );
        assert_eq!(linux.masked_paths, Some(Spec::default_masked_paths()));
        assert_eq!(linux.readonly_paths, Some(Spec::default_readonly_paths()));
        assert!(linux
            .resources
            .and_then(|resources| resources.devices)
            .is_some());
    }

    #[test]
    fn capability_overrides_normalize_add_drop_and_ambient_sets() {
        let capabilities = Capability {
            add_capabilities: vec!["sys_admin".to_string()],
            drop_capabilities: vec!["chown".to_string()],
            add_ambient_capabilities: vec!["net_bind_service".to_string()],
        };

        let resolved =
            apply_capability_overrides(&["CAP_CHOWN".to_string()], Some(&capabilities), true);
        let bounding = resolved.bounding.unwrap();

        assert!(!bounding.contains(&"CAP_CHOWN".to_string()));
        assert!(bounding.contains(&"CAP_SYS_ADMIN".to_string()));
        assert!(bounding.contains(&"CAP_NET_BIND_SERVICE".to_string()));
        assert_eq!(resolved.inheritable.unwrap(), bounding);
        assert_eq!(
            resolved.ambient.unwrap(),
            vec!["CAP_NET_BIND_SERVICE".to_string()]
        );
    }
}
