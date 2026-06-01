use crius::network::CniLoadStatus;
use crius::services::HealthService;
use std::path::{Path, PathBuf};

#[test]
fn network_health_suite_uses_health_service_condition() {
    let status = CniLoadStatus {
        checked_at_unix_millis: 1,
        ready: false,
        reason: "NoNetwork".to_string(),
        message: "no configs".to_string(),
        discovered_files: Vec::new(),
        invalid_files: Vec::new(),
        loaded_networks: Vec::new(),
        declared_plugins: Vec::new(),
        missing_plugin_binaries: Vec::new(),
        default_network_name: None,
    };

    let condition = HealthService.network_condition(&status, None, None);
    assert!(!condition.ready);
    assert_eq!(condition.reason, "NoNetwork");
}

fn path_list_from_env(key: &str, default: &[&str]) -> Vec<PathBuf> {
    std::env::var(key)
        .ok()
        .map(|raw| {
            raw.split(':')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .filter(|paths| !paths.is_empty())
        .unwrap_or_else(|| default.iter().map(PathBuf::from).collect())
}

fn executable_exists(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn has_cni_config_file(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .file_name()
            .to_str()
            .map(|name| {
                name.ends_with(".conf") || name.ends_with(".json") || name.ends_with(".conflist")
            })
            .unwrap_or(false)
    })
}

#[test]
fn gated_real_cni_plugin_preflight_reports_environment_readiness() {
    if std::env::var("CRIUS_RUN_REAL_CNI_SMOKE").ok().as_deref() != Some("1") {
        eprintln!("skipping real CNI smoke preflight; set CRIUS_RUN_REAL_CNI_SMOKE=1 to enable");
        return;
    }

    let config_dirs = path_list_from_env(
        "CRIUS_REAL_CNI_CONFIG_DIRS",
        &["/etc/cni/net.d", "/etc/kubernetes/cni/net.d"],
    );
    let plugin_dirs = path_list_from_env(
        "CRIUS_REAL_CNI_PLUGIN_DIRS",
        &["/opt/cni/bin", "/usr/lib/cni", "/usr/libexec/cni"],
    );
    let required_plugins = std::env::var("CRIUS_REAL_CNI_REQUIRED_PLUGINS")
        .unwrap_or_else(|_| "loopback,bridge,host-local".to_string())
        .split(',')
        .map(str::trim)
        .filter(|plugin| !plugin.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    let existing_config_dirs = config_dirs
        .iter()
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert!(
        !existing_config_dirs.is_empty(),
        "real CNI smoke requires at least one config dir, checked: {:?}",
        config_dirs
    );
    assert!(
        existing_config_dirs
            .iter()
            .any(|dir| has_cni_config_file(dir)),
        "real CNI smoke requires a .conf/.json/.conflist file in one of {:?}",
        existing_config_dirs
    );

    let missing_plugins = required_plugins
        .iter()
        .filter(|plugin| {
            !plugin_dirs
                .iter()
                .any(|dir| executable_exists(&dir.join(plugin.as_str())))
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing_plugins.is_empty(),
        "real CNI smoke missing plugin binaries {:?} in {:?}",
        missing_plugins,
        plugin_dirs
    );
}

#[test]
fn gated_real_cni_add_del_reload_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_REAL_CNI_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping real CNI e2e; set CRIUS_RUN_REAL_CNI_E2E=1 to enable");
        return;
    }

    let config_dirs = path_list_from_env(
        "CRIUS_REAL_CNI_CONFIG_DIRS",
        &["/etc/cni/net.d", "/etc/kubernetes/cni/net.d"],
    );
    let plugin_dirs = path_list_from_env(
        "CRIUS_REAL_CNI_PLUGIN_DIRS",
        &["/opt/cni/bin", "/usr/lib/cni", "/usr/libexec/cni"],
    );
    assert!(
        config_dirs.iter().any(|dir| has_cni_config_file(dir)),
        "real CNI e2e requires a .conf/.json/.conflist file in {:?}",
        config_dirs
    );
    assert!(
        plugin_dirs.iter().any(|dir| dir.is_dir()),
        "real CNI e2e requires at least one plugin dir from {:?}",
        plugin_dirs
    );

    let command = std::env::var("CRIUS_REAL_CNI_E2E_COMMAND")
        .unwrap_or_else(|_| "crius-real-cni-smoke --add-del --reload --multi-network".to_string());
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .env(
            "CRIUS_REAL_CNI_CONFIG_DIRS",
            config_dirs
                .iter()
                .map(|dir| dir.display().to_string())
                .collect::<Vec<_>>()
                .join(":"),
        )
        .env(
            "CRIUS_REAL_CNI_PLUGIN_DIRS",
            plugin_dirs
                .iter()
                .map(|dir| dir.display().to_string())
                .collect::<Vec<_>>()
                .join(":"),
        )
        .status()
        .unwrap_or_else(|err| panic!("failed to run real CNI e2e command: {err}"));
    assert!(
        status.success(),
        "real CNI e2e command failed with {status}"
    );
}
