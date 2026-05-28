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

#[test]
fn gated_rootless_soak_preflight_reports_environment_readiness() {
    if std::env::var("CRIUS_RUN_ROOTLESS_SOAK").ok().as_deref() != Some("1") {
        eprintln!("skipping rootless soak preflight; set CRIUS_RUN_ROOTLESS_SOAK=1 to enable");
        return;
    }

    let mode = match std::env::var("CRIUS_ROOTLESS_SOAK_NETWORK_MODE")
        .unwrap_or_else(|_| "pasta".to_string())
        .as_str()
    {
        "none" => NetworkMode::None,
        "slirp4netns" => NetworkMode::Slirp4netns,
        "pasta" => NetworkMode::Pasta,
        other => panic!("unsupported CRIUS_ROOTLESS_SOAK_NETWORK_MODE={other}"),
    };
    let config = RootlessConfig {
        enabled: true,
        network_mode: mode,
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
        "rootless soak requires a ready user namespace: {}",
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
