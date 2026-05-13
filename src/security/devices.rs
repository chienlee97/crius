use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result};
use nix::sys::stat::{major, minor, stat, SFlag};

use crate::oci::spec::{Device as OciDevice, LinuxDeviceCgroup};

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
}
