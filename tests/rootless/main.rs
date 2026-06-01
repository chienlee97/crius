use crius::rootless::{EffectiveRootlessConfig, NetworkMode};
use crius::services::IntrospectionService;
use crius::{
    image::{
        content_store::{ContentStore, FsContentStore},
        metadata_store::FilesystemImageMetadataStore,
        snapshotter::{FilesystemSnapshotter, SnapshotMode, SnapshotState, Snapshotter},
        CriusImage, StoredLayerMeta,
    },
    network::CniConfig,
    rootless::{RootlessConfig, RootlessManager},
    runtime::RuncRuntime,
    storage::StorageManager,
};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

#[test]
fn rootless_introspection_reports_effective_network_mode() {
    let mut rootless = EffectiveRootlessConfig::disabled();
    rootless.enabled = true;
    rootless.network_mode = NetworkMode::Pasta;

    let value = IntrospectionService.rootless(&rootless);

    assert_eq!(value["enabled"], true);
    assert_eq!(value["networkMode"], "pasta");
    assert_eq!(value["networkModeSupported"], true);
    assert_eq!(value["networkModeReason"], "RootlessPastaSupported");
    assert_eq!(value["networkHelperPath"], "pasta");
}

#[test]
fn rootless_snapshot_prepare_uses_xdg_storage_root() {
    let dir = tempfile::tempdir().unwrap();
    let xdg_runtime_dir = dir.path().join("run");
    let xdg_data_home = dir.path().join("data");
    let rootless = EffectiveRootlessConfig::resolve(&RootlessConfig {
        enabled: true,
        xdg_runtime_dir: xdg_runtime_dir.display().to_string(),
        xdg_data_home: xdg_data_home.display().to_string(),
        network_mode: NetworkMode::None,
        ..Default::default()
    })
    .unwrap();
    let storage_root = rootless.storage_root.clone();
    let db_path = storage_root.join("crius.db");
    let content_store =
        FsContentStore::new_with_ledger(&storage_root, Some(db_path.clone())).unwrap();
    let metadata_store =
        FilesystemImageMetadataStore::new(&storage_root, Vec::new(), Some(db_path.clone()));

    let source = dir.path().join("layer-src");
    std::fs::create_dir_all(source.join("etc")).unwrap();
    std::fs::write(source.join("etc/rootless"), "xdg").unwrap();
    let tar_path = dir.path().join("layer.tar");
    let status = Command::new("tar")
        .arg("-cf")
        .arg(&tar_path)
        .arg("-C")
        .arg(&source)
        .arg(".")
        .status()
        .unwrap();
    assert!(status.success());
    let layer = std::fs::read(&tar_path).unwrap();
    let blob = content_store
        .put_blob("", "application/vnd.oci.image.layer.v1.tar", &layer)
        .unwrap();
    metadata_store
        .save(&CriusImage {
            id: "sha256:rootless".to_string(),
            repo_tags: vec!["rootless:latest".to_string()],
            stored_layers: vec![StoredLayerMeta {
                digest: blob.digest,
                path: blob.relative_path.display().to_string(),
                media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                source_media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                encrypted: false,
            }],
            ..Default::default()
        })
        .unwrap();

    let snapshotter = FilesystemSnapshotter::new(
        SnapshotMode::InternalOverlayUntar,
        &storage_root,
        metadata_store,
        content_store,
        Some(db_path.clone()),
    );
    let rootfs = storage_root
        .join("snapshots")
        .join("container-1")
        .join("rootfs");
    let prepared = snapshotter
        .prepare("container-1", "rootless:latest", &rootfs)
        .unwrap();

    assert_eq!(prepared.rootfs_path, rootfs);
    assert!(prepared.rootfs_path.starts_with(&storage_root));
    assert!(prepared.rootfs_path.starts_with(&xdg_data_home));
    assert!(!prepared.rootfs_path.starts_with("/var/lib"));
    assert_eq!(
        std::fs::read_to_string(prepared.rootfs_path.join("etc/rootless")).unwrap(),
        "xdg"
    );
    let owner_uid = std::fs::metadata(&prepared.rootfs_path).unwrap().uid();
    assert_eq!(owner_uid, RootlessManager::current_uid());

    let info = snapshotter.commit("container-1").unwrap();
    assert_eq!(info.state, SnapshotState::Committed);
    assert!(info.mountpoint.starts_with(&storage_root));

    let storage = StorageManager::new(db_path).unwrap();
    let snapshots = storage.list_snapshots().unwrap();
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0]
        .mountpoint
        .starts_with(storage_root.to_str().unwrap()));
}

#[test]
fn rootless_baseline_keeps_runtime_storage_and_network_under_xdg() {
    let dir = tempfile::tempdir().unwrap();
    let xdg_runtime_dir = dir.path().join("xdg-runtime");
    let xdg_data_home = dir.path().join("xdg-data");
    let rootless = EffectiveRootlessConfig::resolve(&RootlessConfig {
        enabled: true,
        xdg_runtime_dir: xdg_runtime_dir.display().to_string(),
        xdg_data_home: xdg_data_home.display().to_string(),
        network_mode: NetworkMode::None,
        ..Default::default()
    })
    .unwrap();

    let mut runtime = RuncRuntime::new(
        dir.path().join("missing-runc"),
        rootless.runtime_root.join("containers"),
    );
    runtime.set_rootless(rootless.clone());

    let mut cni = CniConfig::default();
    cni.set_rootless_config(Some(rootless.clone()));
    cni.set_netns_mount_dir(rootless.netns_dir.clone());

    assert_eq!(rootless.network_mode, NetworkMode::None);
    assert!(rootless.storage_root.starts_with(&xdg_data_home));
    assert!(rootless.runtime_root.starts_with(&xdg_runtime_dir));
    assert!(rootless.netns_dir.starts_with(&xdg_runtime_dir));
    assert!(runtime.runtime_root().starts_with(&rootless.runtime_root));
    assert!(runtime
        .bundle_path_for("container-1")
        .starts_with(&xdg_runtime_dir));
    assert!(cni.rootless_config().is_some());
    assert_eq!(cni.netns_mount_dir(), rootless.netns_dir.as_path());
    assert!(cni.netns_path("pod-1").starts_with(&rootless.netns_dir));
    assert!(!rootless.storage_root.starts_with("/var/lib"));
    assert!(!rootless.runtime_root.starts_with("/run/crius"));
}

fn parse_network_mode(value: &str) -> NetworkMode {
    match value {
        "none" => NetworkMode::None,
        "slirp4netns" => NetworkMode::Slirp4netns,
        "pasta" => NetworkMode::Pasta,
        other => panic!("unsupported CRIUS_ROOTLESS_SOAK_NETWORK_MODE={other}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootlessSoakCase {
    name: &'static str,
    mode: NetworkMode,
    requires_userns: bool,
    requires_helper: bool,
    requires_port_forward: bool,
    requires_real_cni_reload: bool,
    min_duration: Duration,
}

fn rootless_soak_cases() -> Vec<RootlessSoakCase> {
    vec![
        RootlessSoakCase {
            name: "rootless-none-lifecycle",
            mode: NetworkMode::None,
            requires_userns: true,
            requires_helper: false,
            requires_port_forward: false,
            requires_real_cni_reload: false,
            min_duration: Duration::from_secs(5 * 60),
        },
        RootlessSoakCase {
            name: "rootless-slirp4netns-port-forward",
            mode: NetworkMode::Slirp4netns,
            requires_userns: true,
            requires_helper: true,
            requires_port_forward: true,
            requires_real_cni_reload: false,
            min_duration: Duration::from_secs(10 * 60),
        },
        RootlessSoakCase {
            name: "rootless-pasta-port-forward",
            mode: NetworkMode::Pasta,
            requires_userns: true,
            requires_helper: true,
            requires_port_forward: true,
            requires_real_cni_reload: false,
            min_duration: Duration::from_secs(10 * 60),
        },
        RootlessSoakCase {
            name: "rootless-real-cni-reload",
            mode: NetworkMode::None,
            requires_userns: true,
            requires_helper: false,
            requires_port_forward: false,
            requires_real_cni_reload: true,
            min_duration: Duration::from_secs(10 * 60),
        },
    ]
}

#[test]
fn rootless_soak_matrix_covers_nws4_axes() {
    let cases = rootless_soak_cases();

    for mode in [
        NetworkMode::None,
        NetworkMode::Slirp4netns,
        NetworkMode::Pasta,
    ] {
        assert!(
            cases.iter().any(|case| case.mode == mode),
            "missing rootless soak coverage for {}",
            mode.as_str()
        );
    }
    assert!(cases.iter().all(|case| case.requires_userns));
    assert!(cases
        .iter()
        .any(|case| case.mode == NetworkMode::Pasta && case.requires_helper));
    assert!(cases
        .iter()
        .any(|case| case.mode == NetworkMode::Slirp4netns && case.requires_helper));
    assert!(cases.iter().any(|case| case.requires_port_forward));
    assert!(cases.iter().any(|case| case.requires_real_cni_reload));
    assert!(cases
        .iter()
        .all(|case| case.min_duration >= Duration::from_secs(5 * 60)));
}

#[test]
fn gated_rootless_soak_preflight_reports_environment_readiness() {
    if std::env::var("CRIUS_RUN_ROOTLESS_SOAK").ok().as_deref() != Some("1") {
        eprintln!("skipping rootless soak preflight; set CRIUS_RUN_ROOTLESS_SOAK=1 to enable");
        return;
    }

    let modes = std::env::var("CRIUS_ROOTLESS_SOAK_NETWORK_MODES")
        .unwrap_or_else(|_| "none,slirp4netns,pasta".to_string());
    let modes = modes
        .split(',')
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .map(parse_network_mode)
        .collect::<Vec<_>>();
    assert!(
        !modes.is_empty(),
        "CRIUS_ROOTLESS_SOAK_NETWORK_MODES must name at least one mode"
    );

    for mode in modes {
        let config = RootlessConfig {
            enabled: true,
            network_mode: mode.clone(),
            slirp4netns_path: std::env::var("CRIUS_ROOTLESS_SOAK_SLIRP4NETNS")
                .unwrap_or_else(|_| "slirp4netns".to_string()),
            pasta_path: std::env::var("CRIUS_ROOTLESS_SOAK_PASTA")
                .unwrap_or_else(|_| "pasta".to_string()),
            ..Default::default()
        };
        let rootless = EffectiveRootlessConfig::resolve(&config).unwrap();
        let userns = rootless.user_namespace_status();
        assert!(
            userns.ready,
            "rootless soak for {} requires a ready user namespace: {}",
            rootless.network_mode.as_str(),
            userns.message
        );

        let helper = match rootless.network_mode {
            NetworkMode::None => None,
            NetworkMode::Slirp4netns => Some(rootless.slirp4netns_path.as_path()),
            NetworkMode::Pasta => Some(rootless.pasta_path.as_path()),
            NetworkMode::Rootlesskit => panic!("rootlesskit is not supported"),
        };
        if let Some(helper) = helper {
            assert!(
                executable_exists(helper),
                "rootless soak helper {} is missing or not executable",
                helper.display()
            );
        }
    }
}

#[test]
fn gated_rootless_soak_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_ROOTLESS_E2E_SOAK").ok().as_deref() != Some("1") {
        eprintln!("skipping rootless e2e soak; set CRIUS_RUN_ROOTLESS_E2E_SOAK=1 to enable");
        return;
    }

    let selected = std::env::var("CRIUS_ROOTLESS_E2E_CASE")
        .unwrap_or_else(|_| "rootless-pasta-port-forward".to_string());
    let case = rootless_soak_cases()
        .into_iter()
        .find(|case| case.name == selected)
        .unwrap_or_else(|| panic!("unknown rootless e2e soak case {selected}"));

    let config = RootlessConfig {
        enabled: true,
        network_mode: case.mode.clone(),
        slirp4netns_path: std::env::var("CRIUS_ROOTLESS_SOAK_SLIRP4NETNS")
            .unwrap_or_else(|_| "slirp4netns".to_string()),
        pasta_path: std::env::var("CRIUS_ROOTLESS_SOAK_PASTA")
            .unwrap_or_else(|_| "pasta".to_string()),
        ..Default::default()
    };
    let rootless = EffectiveRootlessConfig::resolve(&config).unwrap();
    let userns = rootless.user_namespace_status();
    assert!(
        !case.requires_userns || userns.ready,
        "rootless e2e case {} requires a ready user namespace: {}",
        case.name,
        userns.message
    );

    if case.requires_helper {
        let helper = match case.mode {
            NetworkMode::Slirp4netns => rootless.slirp4netns_path.as_path(),
            NetworkMode::Pasta => rootless.pasta_path.as_path(),
            NetworkMode::None | NetworkMode::Rootlesskit => {
                panic!("case {} incorrectly requires helper", case.name)
            }
        };
        assert!(
            executable_exists(helper),
            "rootless e2e helper {} is missing or not executable",
            helper.display()
        );
    }

    if case.requires_port_forward {
        let target = std::env::var("CRIUS_ROOTLESS_PORT_FORWARD_TARGET")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        assert!(
            target.contains(':'),
            "CRIUS_ROOTLESS_PORT_FORWARD_TARGET must be host:port"
        );
    }

    if case.requires_real_cni_reload {
        let config_dirs = std::env::var("CRIUS_REAL_CNI_CONFIG_DIRS")
            .unwrap_or_else(|_| "/etc/cni/net.d:/etc/kubernetes/cni/net.d".to_string());
        assert!(
            config_dirs.split(':').any(|dir| Path::new(dir).is_dir()),
            "rootless real CNI reload requires an existing config dir from {config_dirs}"
        );
    }

    if let Ok(command) = std::env::var("CRIUS_ROOTLESS_E2E_COMMAND") {
        let status = Command::new("sh")
            .arg("-c")
            .arg(command)
            .env("CRIUS_ROOTLESS_E2E_CASE", case.name)
            .env("CRIUS_ROOTLESS_NETWORK_MODE", case.mode.as_str())
            .status()
            .unwrap_or_else(|err| panic!("failed to run rootless e2e command: {err}"));
        assert!(
            status.success(),
            "rootless e2e command failed with {status}"
        );
    }
}
