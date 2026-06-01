use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::error::Error;
use crate::network::MainIpPreference;
use crate::prelude::*;

use super::{CgroupDriverConfig, ExternalSnapshotterConfig};

pub(super) fn rewrite_default_string(
    target: &mut String,
    current_defaults: &[&str],
    replacement: String,
) {
    let trimmed = target.trim();
    if trimmed.is_empty()
        || current_defaults
            .iter()
            .any(|candidate| trimmed == *candidate)
    {
        *target = replacement;
    }
}

pub(super) fn apply_string_override(env_name: &str, target: &mut String) {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value.to_string_lossy().trim().to_string();
    }
}

pub(super) fn apply_optional_string_override(env_name: &str, target: &mut Option<String>) {
    if let Some(value) = std::env::var_os(env_name) {
        let trimmed = value.to_string_lossy().trim().to_string();
        *target = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
}

pub(super) fn apply_csv_override(env_name: &str, target: &mut Vec<String>) {
    if let Some(value) = std::env::var_os(env_name) {
        *target = split_csv_list(&value.to_string_lossy());
    }
}

pub(super) fn apply_colon_dirs_override(env_name: &str, target: &mut Vec<String>) {
    if let Some(value) = std::env::var_os(env_name) {
        *target = split_colon_list(&value.to_string_lossy());
    }
}

pub(super) fn apply_bool_override(env_name: &str, target: &mut bool) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = parse_bool(&value.to_string_lossy())
            .map_err(|err| Error::Config(format!("{env_name}: {err}")))?;
    }
    Ok(())
}

pub(super) fn apply_u16_override(env_name: &str, target: &mut u16) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<u16>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid u16 value: {err}")))?;
    }
    Ok(())
}

pub(super) fn apply_u32_override(env_name: &str, target: &mut u32) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<u32>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid u32 value: {err}")))?;
    }
    Ok(())
}

pub(super) fn apply_u64_override(env_name: &str, target: &mut u64) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<u64>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid u64 value: {err}")))?;
    }
    Ok(())
}

pub(super) fn apply_i64_override(env_name: &str, target: &mut i64) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<i64>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid i64 value: {err}")))?;
    }
    Ok(())
}

pub(super) fn apply_usize_override(env_name: &str, target: &mut usize) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<usize>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid usize value: {err}")))?;
    }
    Ok(())
}

pub(super) fn apply_duration_override(
    env_name: &str,
    target: &mut std::time::Duration,
) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = crate::streaming::parse_duration(&value.to_string_lossy())
            .map_err(|err| Error::Config(format!("{env_name}: {err}")))?;
    }
    Ok(())
}

pub(super) fn split_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(super) fn split_colon_list(raw: &str) -> Vec<String> {
    raw.split(':')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(super) fn parse_bool(raw: &str) -> std::result::Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value {other}")),
    }
}

pub(super) fn parse_id_mappings(
    field_name: &str,
    raw: &str,
) -> Result<Vec<crate::proto::runtime::v1::IdMapping>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    trimmed
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let mut parts = entry.split(':').map(str::trim);
            let container_id = parts
                .next()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "{field_name}: invalid mapping {entry}; expected container:host:size"
                    ))
                })?
                .parse::<u32>()
                .map_err(|err| {
                    Error::Config(format!(
                        "{field_name}: invalid container id in mapping {entry}: {err}"
                    ))
                })?;
            let host_id = parts
                .next()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "{field_name}: invalid mapping {entry}; expected container:host:size"
                    ))
                })?
                .parse::<u32>()
                .map_err(|err| {
                    Error::Config(format!(
                        "{field_name}: invalid host id in mapping {entry}: {err}"
                    ))
                })?;
            let length = parts
                .next()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "{field_name}: invalid mapping {entry}; expected container:host:size"
                    ))
                })?
                .parse::<u32>()
                .map_err(|err| {
                    Error::Config(format!(
                        "{field_name}: invalid size in mapping {entry}: {err}"
                    ))
                })?;
            if length == 0 {
                return Err(Error::Config(format!(
                    "{field_name}: mapping {entry} must have size greater than zero"
                )));
            }
            if parts.next().is_some() {
                return Err(Error::Config(format!(
                    "{field_name}: invalid mapping {entry}; expected container:host:size"
                )));
            }
            Ok(crate::proto::runtime::v1::IdMapping {
                host_id,
                container_id,
                length,
            })
        })
        .collect()
}

pub(super) fn parse_cgroup_driver(raw: &str) -> std::result::Result<CgroupDriverConfig, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "systemd" => Ok(CgroupDriverConfig::Systemd),
        "cgroupfs" => Ok(CgroupDriverConfig::Cgroupfs),
        other => Err(format!(
            "invalid cgroup driver {other}; expected systemd or cgroupfs"
        )),
    }
}

pub(super) fn parse_main_ip_preference(raw: &str) -> std::result::Result<MainIpPreference, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "ipv4" => Ok(MainIpPreference::Ipv4),
        "ipv6" => Ok(MainIpPreference::Ipv6),
        "cni" => Ok(MainIpPreference::Cni),
        other => Err(format!(
            "invalid CNI ip preference {other}; expected ipv4, ipv6, or cni"
        )),
    }
}

pub(super) fn validate_runtime_backend_options(
    field_name: &str,
    backend: &str,
    options: &HashMap<String, String>,
) -> Result<()> {
    if options.iter().any(|(key, _value)| key.trim().is_empty()) {
        return Err(Error::Config(format!(
            "{field_name}: backend option keys must not be empty"
        )));
    }

    match backend.trim() {
        "" | "runc" => {
            const RUNC_OPTIONS: &[&str] = &[
                "no_pivot",
                "no_new_keyring",
                "systemd_cgroup",
                "rootless",
                "criu_path",
            ];
            for key in options.keys() {
                if !RUNC_OPTIONS.iter().any(|allowed| allowed == key) {
                    return Err(Error::Config(format!(
                        "{field_name}: unsupported runc backend option {key}"
                    )));
                }
            }
        }
        "wasm-direct" => {
            const WASM_DIRECT_OPTIONS: &[&str] =
                &["engine", "sandboxer", "state_dir", "allow_exec"];
            for (key, value) in options {
                if !WASM_DIRECT_OPTIONS.iter().any(|allowed| allowed == key) {
                    return Err(Error::Config(format!(
                        "{field_name}: unsupported wasm-direct backend option {key}"
                    )));
                }
                match key.as_str() {
                    "engine" | "sandboxer" => {
                        if value.trim().is_empty() {
                            return Err(Error::Config(format!(
                                "{field_name}.{key} must not be empty"
                            )));
                        }
                    }
                    "state_dir" => {
                        let path = Path::new(value.trim());
                        if !path.is_absolute() {
                            return Err(Error::Config(format!(
                                "{field_name}.state_dir must be an absolute path"
                            )));
                        }
                    }
                    "allow_exec" => {
                        parse_bool(value).map_err(|err| {
                            Error::Config(format!("{field_name}.allow_exec: {err}"))
                        })?;
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }

    Ok(())
}

pub(super) fn validate_included_pod_metrics(values: &[String]) -> Result<()> {
    const ALL: &str = "all";
    const AVAILABLE: &[&str] = &["cpu", "memory", "network", "process", "disk"];

    if values.len() == 1 && values[0].trim().eq_ignore_ascii_case(ALL) {
        return Ok(());
    }

    for value in values {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized == ALL {
            return Err(Error::Config(
                "'all' should be the only value in api.included_pod_metrics".to_string(),
            ));
        }
        if !AVAILABLE.iter().any(|candidate| *candidate == normalized) {
            return Err(Error::Config(format!(
                "invalid pod metric {normalized}; available metrics: {:?}",
                AVAILABLE
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_metrics_collectors(values: &[String]) -> Result<()> {
    const ALLOWED: &[&str] = &["runtime", "resources", "images"];
    if values.is_empty() {
        return Err(Error::Config(
            "metrics.collectors must contain at least one collector".to_string(),
        ));
    }
    for value in values {
        let normalized = value.trim();
        if !ALLOWED.contains(&normalized) {
            return Err(Error::Config(format!(
                "invalid metrics collector {normalized}; available collectors: {:?}",
                ALLOWED
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_configured_runtime_snapshotter(
    name: &str,
    value: &str,
    external_snapshotters: &HashMap<String, ExternalSnapshotterConfig>,
) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || matches!(trimmed, "internal-overlay-untar" | "internal-cached-rootfs")
        || external_snapshotters.contains_key(trimmed)
    {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{name} must be empty, an internal snapshotter, or one of configured image.external_snapshotters: {trimmed}"
    )))
}

pub(super) fn validate_external_snapshotters(
    snapshotters: &HashMap<String, ExternalSnapshotterConfig>,
) -> Result<()> {
    for (name, config) in snapshotters {
        let trimmed_name = name.trim();
        if trimmed_name.is_empty() {
            return Err(Error::Config(
                "image.external_snapshotters keys must not be empty".to_string(),
            ));
        }
        if matches!(
            trimmed_name,
            "internal-overlay-untar" | "internal-cached-rootfs"
        ) {
            return Err(Error::Config(format!(
                "image.external_snapshotters.{trimmed_name} conflicts with a built-in snapshotter"
            )));
        }
        ensure_non_empty(
            &format!("image.external_snapshotters.{trimmed_name}.type"),
            &config.snapshotter_type,
        )?;
        if config.endpoint.trim().is_empty() && config.path.trim().is_empty() {
            return Err(Error::Config(format!(
                "image.external_snapshotters.{trimmed_name} requires endpoint or path"
            )));
        }
        if !config.path.trim().is_empty() && !Path::new(config.path.trim()).is_absolute() {
            return Err(Error::Config(format!(
                "image.external_snapshotters.{trimmed_name}.path must be an absolute path when set"
            )));
        }
        if !config.endpoint.trim().is_empty() {
            validate_snapshotter_endpoint(
                &format!("image.external_snapshotters.{trimmed_name}.endpoint"),
                &config.endpoint,
            )?;
        }
        for capability in &config.capabilities {
            if capability.trim().is_empty() {
                return Err(Error::Config(format!(
                    "image.external_snapshotters.{trimmed_name}.capabilities entries must not be empty"
                )));
            }
        }
    }
    Ok(())
}

fn validate_snapshotter_endpoint(field: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.starts_with("unix://") {
        let path = trimmed.trim_start_matches("unix://");
        if path.is_empty() || !Path::new(path).is_absolute() {
            return Err(Error::Config(format!(
                "{field} must use unix:// followed by an absolute socket path"
            )));
        }
        return Ok(());
    }
    if trimmed.contains("://") {
        let scheme = trimmed
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("");
        if matches!(scheme, "tcp" | "http" | "https") {
            return Ok(());
        }
        return Err(Error::Config(format!(
            "{field} has unsupported endpoint scheme {scheme}"
        )));
    }
    if Path::new(trimmed).is_absolute() {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{field} must be unix://, tcp/http(s), or an absolute socket path"
    )))
}

pub(super) fn validate_runtime_backend(name: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() || matches!(trimmed, "runc" | "wasm-direct") {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{name} must be empty, \"runc\", or \"wasm-direct\", got {trimmed}"
    )))
}

pub(super) fn validate_cpu_set_string(field: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(());
    }

    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if let Some((start, end)) = entry.split_once('-') {
            let start = start.trim().parse::<u32>().map_err(|_| {
                Error::Config(format!("{field} contains invalid CPU range start: {entry}"))
            })?;
            let end = end.trim().parse::<u32>().map_err(|_| {
                Error::Config(format!("{field} contains invalid CPU range end: {entry}"))
            })?;
            if start > end {
                return Err(Error::Config(format!(
                    "{field} contains descending CPU range: {entry}"
                )));
            }
        } else {
            entry.parse::<u32>().map_err(|_| {
                Error::Config(format!("{field} contains invalid CPU entry: {entry}"))
            })?;
        }
    }

    Ok(())
}

pub(super) fn ensure_non_empty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::Config(format!("{name} must not be empty")));
    }
    Ok(())
}

pub(super) fn ensure_vec_non_empty(name: &str, values: &[String]) -> Result<()> {
    if values.is_empty() {
        return Err(Error::Config(format!("{name} must not be empty")));
    }
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(Error::Config(format!(
            "{name} must not contain empty entries"
        )));
    }
    Ok(())
}

pub(super) fn validate_monitor_env_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        let Some((key, _)) = value.split_once('=') else {
            return Err(Error::Config(format!(
                "{name} entries must be in KEY=value format"
            )));
        };
        if key.trim().is_empty() {
            return Err(Error::Config(format!(
                "{name} entries must have a non-empty KEY"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_optional_seccomp_profile_path(name: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(());
    }
    if !Path::new(value).is_absolute() {
        return Err(Error::Config(format!(
            "{name} must be an absolute path when set"
        )));
    }
    Ok(())
}

pub(super) fn validate_seccomp_profile_selector(name: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty()
        || value.eq_ignore_ascii_case("runtime/default")
        || value.eq_ignore_ascii_case("docker/default")
        || value.eq_ignore_ascii_case("unconfined")
        || value.starts_with("localhost/")
        || Path::new(value).is_absolute()
    {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{name} must be empty, runtime/default, docker/default, unconfined, localhost/<name>, or an absolute path"
    )))
}

pub(super) fn validate_annotation_prefix_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        if value.trim().is_empty() {
            return Err(Error::Config(format!("{name} entries must not be empty")));
        }
    }
    Ok(())
}

pub(super) fn validate_annotation_map(name: &str, values: &HashMap<String, String>) -> Result<()> {
    for (key, value) in values {
        if key.trim().is_empty() {
            return Err(Error::Config(format!("{name} keys must not be empty")));
        }
        if value.trim().is_empty() {
            return Err(Error::Config(format!("{name}.{key} must not be empty")));
        }
    }
    Ok(())
}

pub(super) fn validate_capability_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        if value.trim().is_empty() {
            return Err(Error::Config(format!("{name} entries must not be empty")));
        }
    }
    Ok(())
}

pub(super) fn validate_sysctl_assignments(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        parse_sysctl_assignment(value).map_err(|message| {
            Error::Config(format!(
                "{name} contains invalid entry {value:?}: {message}"
            ))
        })?;
    }
    Ok(())
}

pub(super) fn validate_ulimit_assignments(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        parse_ulimit_assignment(value).map_err(|message| {
            Error::Config(format!(
                "{name} contains invalid entry {value:?}: {message}"
            ))
        })?;
    }
    Ok(())
}

pub(super) fn validate_allowed_devices(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(Error::Config(format!("{name} entries must not be empty")));
        }
        if !Path::new(trimmed).is_absolute() {
            return Err(Error::Config(format!(
                "{name} contains non-absolute device path {trimmed}"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_additional_devices(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        parse_additional_device(value).map_err(|message| {
            Error::Config(format!(
                "{name} contains invalid entry {value:?}: {message}"
            ))
        })?;
    }
    Ok(())
}

pub(super) fn parse_additional_device(
    value: &str,
) -> std::result::Result<(String, String, String), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("entry must not be empty".to_string());
    }

    let parts: Vec<&str> = trimmed.split(':').collect();
    if parts.len() > 3 {
        return Err("expected /src[:/dst[:rwm]] format".to_string());
    }

    let source = parts[0].trim();
    if source.is_empty() || !Path::new(source).is_absolute() {
        return Err("source path must be an absolute path".to_string());
    }

    let mut destination = source.to_string();
    let mut permissions = "rwm".to_string();

    if let Some(second) = parts.get(1) {
        let second = second.trim();
        if !second.is_empty() {
            if second.starts_with('/') {
                destination = second.to_string();
            } else {
                permissions = parse_device_permissions(second)?;
            }
        }
    }

    if let Some(third) = parts.get(2) {
        permissions = parse_device_permissions(third.trim())?;
    }

    if !destination.starts_with("/dev/") {
        return Err("destination path must live under /dev".to_string());
    }

    Ok((source.to_string(), destination, permissions))
}

pub(super) fn parse_device_permissions(value: &str) -> std::result::Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("device permissions must not be empty".to_string());
    }
    let mut seen = HashSet::new();
    for ch in trimmed.chars() {
        if !matches!(ch, 'r' | 'w' | 'm') {
            return Err(format!("invalid device permission character {ch}"));
        }
        if !seen.insert(ch) {
            return Err(format!("duplicate device permission character {ch}"));
        }
    }
    Ok(trimmed.to_string())
}

pub(super) fn parse_sysctl_assignment(
    value: &str,
) -> std::result::Result<(String, String), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("entry must not be empty".to_string());
    }
    let (key, raw_value) = trimmed
        .split_once('=')
        .or_else(|| trimmed.split_once(char::is_whitespace))
        .ok_or_else(|| "expected key=value or key = value format".to_string())?;
    let key = key.trim();
    let raw_value = raw_value.trim();
    if key.is_empty() {
        return Err("key must not be empty".to_string());
    }
    if raw_value.is_empty() {
        return Err("value must not be empty".to_string());
    }
    Ok((key.to_string(), raw_value.to_string()))
}

pub(super) fn validate_image_storage_options(driver: &str, options: &[String]) -> Result<()> {
    let driver = driver.trim();
    let mut seen = HashSet::new();
    for option in options {
        let (key, value) = parse_sysctl_assignment(option).map_err(|err| {
            Error::Config(format!("image.storage_options entry {option:?}: {err}"))
        })?;
        if !seen.insert(key.clone()) {
            return Err(Error::Config(format!(
                "image.storage_options contains duplicate key {key}"
            )));
        }
        match (driver, key.as_str()) {
            ("overlay", "overlay.mount_program") => {
                if !Path::new(&value).is_absolute() {
                    return Err(Error::Config(format!(
                        "image.storage_options overlay.mount_program must be an absolute path, got {value}"
                    )));
                }
            }
            ("overlay", "overlay.ignore_chown_errors") => {
                parse_bool(&value).map_err(|err| {
                    Error::Config(format!(
                        "image.storage_options overlay.ignore_chown_errors: {err}"
                    ))
                })?;
            }
            ("overlay", other) => {
                return Err(Error::Config(format!(
                    "unsupported image.storage_options key {other}; supported overlay keys are overlay.mount_program and overlay.ignore_chown_errors"
                )));
            }
            (other_driver, _) => {
                return Err(Error::Config(format!(
                    "image.storage_options is not supported for image.driver {other_driver}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn parse_ulimit_assignment(
    value: &str,
) -> std::result::Result<crate::oci::spec::Rlimit, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("entry must not be empty".to_string());
    }
    let (name, limits) = trimmed
        .split_once('=')
        .ok_or_else(|| "expected name=soft:hard format".to_string())?;
    let (soft, hard) = limits
        .split_once(':')
        .ok_or_else(|| "expected name=soft:hard format".to_string())?;
    let name = name.trim();
    if name.is_empty() {
        return Err("name must not be empty".to_string());
    }
    let soft = soft
        .trim()
        .parse::<u64>()
        .map_err(|err| format!("invalid soft limit: {err}"))?;
    let hard = hard
        .trim()
        .parse::<u64>()
        .map_err(|err| format!("invalid hard limit: {err}"))?;
    let upper = name.to_ascii_uppercase();
    let rtype = if upper.starts_with("RLIMIT_") {
        upper
    } else {
        format!("RLIMIT_{upper}")
    };
    Ok(crate::oci::spec::Rlimit { rtype, soft, hard })
}

pub(super) fn validate_timezone(name: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "Local" {
        return Ok(());
    }
    if trimmed.starts_with('/') {
        return Err(Error::Config(format!(
            "{name} must be empty, \"Local\", or an IANA timezone name"
        )));
    }
    if trimmed
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(Error::Config(format!(
            "{name} must be a valid IANA timezone path"
        )));
    }
    let zoneinfo = Path::new("/usr/share/zoneinfo").join(trimmed);
    if !zoneinfo.exists() {
        return Err(Error::Config(format!(
            "invalid timezone {}: {} does not exist",
            trimmed,
            zoneinfo.display()
        )));
    }
    Ok(())
}

pub(super) fn validate_listen_address(listen: &str, allow_tcp_service: bool) -> Result<()> {
    if let Some(path) = listen.strip_prefix("unix://") {
        return ensure_non_empty("api.listen", path);
    }

    listen
        .parse::<std::net::SocketAddr>()
        .map_err(|err| Error::Config(format!("invalid api.listen {listen}: {err}")))?;

    if !allow_tcp_service {
        return Err(Error::Config(format!(
            "api.listen {listen} requires api.allow_tcp_service = true"
        )));
    }

    Ok(())
}

pub(super) fn validate_unix_listen_address(field_name: &str, listen: &str) -> Result<()> {
    let Some(path) = listen.strip_prefix("unix://") else {
        return Err(Error::Config(format!(
            "{field_name} value {listen} must use unix://"
        )));
    };
    ensure_non_empty(field_name, path)
}

pub(super) fn validate_log_filter(filter: &str) -> Result<()> {
    tracing_subscriber::EnvFilter::try_new(filter)
        .map(|_| ())
        .map_err(|err| Error::Config(format!("invalid logging.level {filter}: {err}")))
}

pub(super) fn detect_system_cgroup_driver() -> CgroupDriverConfig {
    let systemd_active = Path::new("/run/systemd/system").exists()
        || std::fs::read_to_string("/proc/1/comm")
            .map(|content| content.trim() == "systemd")
            .unwrap_or(false);
    let cgroup_v2 = Path::new("/sys/fs/cgroup/cgroup.controllers").exists();
    let systemd_cgroup_layout = Path::new("/sys/fs/cgroup/system.slice").exists()
        || Path::new("/sys/fs/cgroup/user.slice").exists()
        || Path::new("/sys/fs/cgroup/systemd").exists();

    if systemd_active && (cgroup_v2 || systemd_cgroup_layout) {
        CgroupDriverConfig::Systemd
    } else {
        CgroupDriverConfig::Cgroupfs
    }
}

pub(super) fn current_platform_key() -> String {
    format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
}

pub(super) fn resolve_platform_runtime_path(
    default_runtime_path: &str,
    platform_runtime_paths: &HashMap<String, String>,
) -> Result<String> {
    let default_runtime_path = default_runtime_path.trim();
    let selected = platform_runtime_paths
        .get(&current_platform_key())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(default_runtime_path);
    if selected.is_empty() {
        return Err(Error::Config(format!(
            "runtime path for platform {} must not be empty",
            current_platform_key()
        )));
    }
    Ok(selected.to_string())
}

pub(super) fn resolve_monitor_cgroup(
    raw: &str,
    cgroup_driver: CgroupDriverConfig,
) -> Result<String> {
    let trimmed = raw.trim();
    match cgroup_driver {
        CgroupDriverConfig::Systemd => {
            if trimmed.is_empty() {
                return Ok("system.slice".to_string());
            }
            if trimmed == "pod" || trimmed.ends_with(".slice") {
                return Ok(trimmed.to_string());
            }
            Err(Error::Config(format!(
                "monitor cgroup should be \"pod\", empty, or a systemd slice ending with .slice, got {trimmed}"
            )))
        }
        CgroupDriverConfig::Cgroupfs => {
            if trimmed.is_empty() || trimmed == "pod" {
                return Ok(trimmed.to_string());
            }
            Err(Error::Config(format!(
                "monitor cgroup should be \"pod\" or empty for cgroupfs, got {trimmed}"
            )))
        }
    }
}

pub(super) fn validate_optional_existing_absolute_path(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Ok(());
    }
    let path = Path::new(value.trim());
    if !path.is_absolute() {
        return Err(Error::Config(format!(
            "{name} must be an absolute path when set"
        )));
    }
    if !path.exists() {
        return Err(Error::Config(format!(
            "{name} does not exist: {}",
            path.display()
        )));
    }
    Ok(())
}

pub(super) fn validate_platform_runtime_paths(
    name: &str,
    values: &HashMap<String, String>,
) -> Result<()> {
    for (platform, path) in values {
        let platform = platform.trim();
        let path = path.trim();
        if platform.is_empty() {
            return Err(Error::Config(format!(
                "{name} contains an empty platform key"
            )));
        }
        if !platform.contains('/') {
            return Err(Error::Config(format!(
                "{name}.{platform} must use os/arch format"
            )));
        }
        if path.is_empty() {
            return Err(Error::Config(format!(
                "{name}.{platform} must not be empty"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_absolute_path_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(Error::Config(format!("{name} entries must not be empty")));
        }
        if !Path::new(trimmed).is_absolute() {
            return Err(Error::Config(format!(
                "{name} entry must be an absolute path, got {trimmed}"
            )));
        }
    }
    Ok(())
}
