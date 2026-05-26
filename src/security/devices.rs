use std::collections::HashSet;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Context, Result};
use nix::sys::stat::{major, minor, stat, SFlag};

use crate::oci::spec::{Device as OciDevice, LinuxDeviceCgroup, Spec};

#[derive(Debug, Clone)]
pub struct DeviceMapping {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub permissions: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceOwnership {
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct ResolvedDeviceMappings {
    pub devices: Vec<OciDevice>,
    pub cgroup_rules: Vec<LinuxDeviceCgroup>,
}

#[derive(Debug, Clone)]
pub struct DeviceResolverInput<'a> {
    pub privileged: bool,
    pub tty: bool,
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
}

#[derive(Debug, Clone)]
pub struct ResolvedDeviceSet {
    pub devices: Vec<OciDevice>,
    pub cgroup_rules: Vec<LinuxDeviceCgroup>,
}

pub fn ownership_from_security_context(
    enabled: bool,
    user: Option<&str>,
    run_as_group: Option<u32>,
) -> DeviceOwnership {
    if !enabled {
        return DeviceOwnership {
            uid: None,
            gid: None,
        };
    }

    DeviceOwnership {
        uid: user
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|value| *value > 0),
        gid: run_as_group.filter(|value| *value > 0),
    }
}

pub fn validate_allowed_devices(
    devices: &[DeviceMapping],
    allowed_devices: &HashSet<PathBuf>,
) -> Result<()> {
    if allowed_devices.is_empty() {
        return Ok(());
    }

    for device in devices {
        if !allowed_devices.contains(&device.source) {
            return Err(anyhow::anyhow!(
                "device {} is not allowed by runtime.allowed_devices",
                device.source.display()
            ));
        }
    }
    Ok(())
}

pub fn mappings_to_oci(
    devices: &[DeviceMapping],
    ownership: DeviceOwnership,
) -> Result<ResolvedDeviceMappings> {
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
            uid: ownership.uid.or(Some(file_stat.st_uid)),
            gid: ownership.gid.or(Some(file_stat.st_gid)),
        });
        cgroup_rules.push(LinuxDeviceCgroup {
            allow: true,
            device_type: Some(device_type.to_string()),
            major: Some(major_id),
            minor: Some(minor_id),
            access: Some(access),
        });
    }

    Ok(ResolvedDeviceMappings {
        devices: oci_devices,
        cgroup_rules,
    })
}

pub fn resolve_devices(input: DeviceResolverInput<'_>) -> Result<ResolvedDeviceSet> {
    validate_rootless_requests(&input)?;

    let ownership = ownership_from_security_context(
        input.device_ownership_from_security_context,
        input.user,
        input.run_as_group,
    );

    let mut devices = if input.privileged {
        if input.privileged_without_host_devices {
            Vec::new()
        } else {
            host_devices()?
        }
    } else {
        Spec::default_devices(input.tty)
    };
    let mut cgroup_rules = input.existing_cgroup_rules.to_vec();

    append_device_mappings(
        &mut devices,
        &mut cgroup_rules,
        input.additional_devices,
        ownership,
    )?;

    validate_allowed_devices(input.requested_devices, input.allowed_devices)?;
    append_device_mappings(
        &mut devices,
        &mut cgroup_rules,
        input.requested_devices,
        ownership,
    )?;

    if input.privileged {
        if !input.privileged_without_host_devices
            || input.privileged_without_host_devices_all_devices_allowed
        {
            cgroup_rules = vec![allow_all_devices_rule()];
        }
    } else if cgroup_rules.is_empty() {
        cgroup_rules = vec![allow_all_devices_rule()];
    }

    Ok(ResolvedDeviceSet {
        devices,
        cgroup_rules,
    })
}

fn validate_rootless_requests(input: &DeviceResolverInput<'_>) -> Result<()> {
    if !input.rootless {
        return Ok(());
    }

    if input.privileged {
        return Err(anyhow::anyhow!(
            "privileged containers are not supported in rootless mode"
        ));
    }

    if !input.requested_devices.is_empty() {
        return Err(anyhow::anyhow!(
            "explicit device requests are not supported in rootless mode because device cgroup rules are disabled"
        ));
    }

    if !input.additional_devices.is_empty() {
        return Err(anyhow::anyhow!(
            "runtime.additional_devices cannot be applied in rootless mode because device cgroup rules are disabled"
        ));
    }

    Ok(())
}

fn append_device_mappings(
    devices: &mut Vec<OciDevice>,
    cgroup_rules: &mut Vec<LinuxDeviceCgroup>,
    mappings: &[DeviceMapping],
    ownership: DeviceOwnership,
) -> Result<()> {
    if mappings.is_empty() {
        return Ok(());
    }

    let resolved = mappings_to_oci(mappings, ownership)?;
    devices.extend(resolved.devices);
    cgroup_rules.extend(resolved.cgroup_rules);
    Ok(())
}

pub fn allow_all_devices_rule() -> LinuxDeviceCgroup {
    LinuxDeviceCgroup {
        allow: true,
        device_type: None,
        major: None,
        minor: None,
        access: Some("rwm".to_string()),
    }
}

pub fn host_devices() -> Result<Vec<OciDevice>> {
    let mut devices = Vec::new();
    collect_host_devices(Path::new("/dev"), &mut devices)?;
    Ok(devices)
}

pub fn should_skip_host_device_dir(name: &str) -> bool {
    matches!(
        name,
        "pts" | "shm" | "fd" | "mqueue" | ".lxc" | ".lxd-mounts" | ".udev"
    )
}

pub fn should_skip_host_device_file(name: &str) -> bool {
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
        let metadata = std::fs::symlink_metadata(&entry_path)
            .with_context(|| format!("failed to stat host device path {}", entry_path.display()))?;
        let file_type = metadata.file_type();
        if file_type.is_dir() {
            if should_skip_host_device_dir(&entry_name) {
                continue;
            }
            collect_host_devices(&entry_path, devices)?;
            continue;
        }
        if file_type.is_symlink() {
            continue;
        }
        if should_skip_host_device_file(&entry_name) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_uses_security_context_only_when_enabled() {
        assert_eq!(
            ownership_from_security_context(false, Some("1000"), Some(2000)),
            DeviceOwnership {
                uid: None,
                gid: None
            }
        );
        assert_eq!(
            ownership_from_security_context(true, Some("1000"), Some(2000)),
            DeviceOwnership {
                uid: Some(1000),
                gid: Some(2000)
            }
        );
    }

    #[test]
    fn allowed_device_validation_rejects_unlisted_device() {
        let devices = vec![DeviceMapping {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/dev/null"),
            permissions: "rwm".to_string(),
        }];
        let allowed = HashSet::from([PathBuf::from("/dev/zero")]);

        let err = validate_allowed_devices(&devices, &allowed).unwrap_err();
        assert!(err.to_string().contains("/dev/null"));
    }

    #[test]
    fn resolver_adds_default_allow_all_rule_for_non_privileged_containers() {
        let allowed = HashSet::new();
        let resolved = resolve_devices(DeviceResolverInput {
            privileged: false,
            tty: false,
            requested_devices: &[],
            additional_devices: &[],
            existing_cgroup_rules: &[],
            allowed_devices: &allowed,
            device_ownership_from_security_context: false,
            user: None,
            run_as_group: None,
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            rootless: false,
        })
        .expect("default device resolution should succeed");

        assert!(resolved
            .devices
            .iter()
            .any(|device| device.path == "/dev/null"));
        assert_all_devices_allowed(&resolved.cgroup_rules);
    }

    #[test]
    fn resolver_can_suppress_privileged_host_devices_and_allow_all_rule() {
        let allowed = HashSet::new();
        let resolved = resolve_devices(DeviceResolverInput {
            privileged: true,
            tty: false,
            requested_devices: &[],
            additional_devices: &[],
            existing_cgroup_rules: &[],
            allowed_devices: &allowed,
            device_ownership_from_security_context: false,
            user: None,
            run_as_group: None,
            privileged_without_host_devices: true,
            privileged_without_host_devices_all_devices_allowed: false,
            rootless: false,
        })
        .expect("privileged device resolution should succeed");

        assert!(resolved.devices.is_empty());
        assert!(resolved.cgroup_rules.is_empty());
    }

    #[test]
    fn resolver_keeps_allow_all_rule_when_requested_without_host_devices() {
        let allowed = HashSet::new();
        let resolved = resolve_devices(DeviceResolverInput {
            privileged: true,
            tty: false,
            requested_devices: &[],
            additional_devices: &[],
            existing_cgroup_rules: &[],
            allowed_devices: &allowed,
            device_ownership_from_security_context: false,
            user: None,
            run_as_group: None,
            privileged_without_host_devices: true,
            privileged_without_host_devices_all_devices_allowed: true,
            rootless: false,
        })
        .expect("privileged device resolution should succeed");

        assert!(resolved.devices.is_empty());
        assert_all_devices_allowed(&resolved.cgroup_rules);
    }

    #[test]
    fn resolver_rejects_rootless_explicit_devices() {
        let allowed = HashSet::new();
        let devices = vec![DeviceMapping {
            source: PathBuf::from("/dev/null"),
            destination: PathBuf::from("/dev/null"),
            permissions: "rwm".to_string(),
        }];

        let err = resolve_devices(DeviceResolverInput {
            privileged: false,
            tty: false,
            requested_devices: &devices,
            additional_devices: &[],
            existing_cgroup_rules: &[],
            allowed_devices: &allowed,
            device_ownership_from_security_context: false,
            user: None,
            run_as_group: None,
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            rootless: true,
        })
        .expect_err("rootless explicit devices must be rejected");

        assert!(err
            .to_string()
            .contains("explicit device requests are not supported in rootless mode"));
    }

    #[test]
    fn resolver_preserves_existing_non_privileged_device_rules() {
        let allowed = HashSet::new();
        let existing = vec![LinuxDeviceCgroup {
            allow: false,
            device_type: Some("b".to_string()),
            major: Some(8),
            minor: None,
            access: Some("rwm".to_string()),
        }];

        let resolved = resolve_devices(DeviceResolverInput {
            privileged: false,
            tty: false,
            requested_devices: &[],
            additional_devices: &[],
            existing_cgroup_rules: &existing,
            allowed_devices: &allowed,
            device_ownership_from_security_context: false,
            user: None,
            run_as_group: None,
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            rootless: false,
        })
        .expect("device resolution should preserve existing cgroup rules");

        assert_eq!(resolved.cgroup_rules.len(), 1);
        assert!(!resolved.cgroup_rules[0].allow);
        assert_eq!(resolved.cgroup_rules[0].device_type.as_deref(), Some("b"));
        assert_eq!(resolved.cgroup_rules[0].major, Some(8));
        assert_eq!(resolved.cgroup_rules[0].access.as_deref(), Some("rwm"));
    }

    #[test]
    fn host_device_skip_rules_match_containerd_style_filters() {
        assert!(should_skip_host_device_dir("pts"));
        assert!(should_skip_host_device_dir("shm"));
        assert!(should_skip_host_device_dir("fd"));
        assert!(should_skip_host_device_dir("mqueue"));
        assert!(should_skip_host_device_dir(".udev"));
        assert!(!should_skip_host_device_dir("mapper"));

        assert!(should_skip_host_device_file("console"));
        assert!(!should_skip_host_device_file("null"));
    }

    fn assert_all_devices_allowed(rules: &[LinuxDeviceCgroup]) {
        assert_eq!(rules.len(), 1);
        assert!(rules[0].allow);
        assert_eq!(rules[0].device_type, None);
        assert_eq!(rules[0].major, None);
        assert_eq!(rules[0].minor, None);
        assert_eq!(rules[0].access.as_deref(), Some("rwm"));
    }
}
