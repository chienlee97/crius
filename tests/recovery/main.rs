use std::fs;
use std::path::{Path, PathBuf};

use crius::state::{
    LedgerRepairOptions, RecoveryLedgerSnapshot, RuntimeArtifactLedgerState, ShimLedgerState,
    SnapshotLedgerState, StateLedger, StateLedgerWriter,
};
use crius::storage::persistence::{PersistenceConfig, PersistenceManager};
use crius::storage::{
    ContainerRecord, ImageRecord, ImageRefRecord, PodSandboxRecord, RuntimeArtifactRecord,
    ShimProcessRecord, SnapshotRecord,
};
use tempfile::TempDir;

#[test]
fn recovery_suite_has_persistence_config_entrypoint() {
    let dir = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        db_path: dir.path().join("crius.db"),
        enable_recovery: true,
        auto_save_interval: 30,
    };

    assert!(config.enable_recovery);
    assert_eq!(config.auto_save_interval, 30);
}

struct RecoveryFixture {
    _dir: TempDir,
    persistence: PersistenceManager,
    db_path: PathBuf,
    pod_id: String,
    container_id: String,
    bundle_path: PathBuf,
    rootfs_path: PathBuf,
    shim_socket_path: PathBuf,
    shim_work_dir: PathBuf,
    runtime_state_dir: PathBuf,
}

impl RecoveryFixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: db_path.clone(),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();

        let pod_id = "pod-a".to_string();
        let container_id = "container-a".to_string();
        let runtime_root = dir.path().join("runtime");
        let runtime_state_dir = dir.path().join("runtime-state");
        let bundle_path = runtime_root.join(&container_id).join("bundle");
        let rootfs_path = dir
            .path()
            .join("snapshots")
            .join("snapshot-a")
            .join("rootfs");
        let shim_work_dir = dir.path().join("shims").join(&container_id);
        let shim_socket_path = shim_work_dir.join("task.sock");
        let exit_code_file = shim_work_dir.join("exit");
        let shim_log_file = shim_work_dir.join("shim.log");
        let netns_path = dir.path().join("netns").join(&pod_id);

        fs::create_dir_all(&bundle_path).unwrap();
        fs::create_dir_all(&rootfs_path).unwrap();
        fs::create_dir_all(&shim_work_dir).unwrap();
        fs::create_dir_all(netns_path.parent().unwrap()).unwrap();
        fs::write(&shim_socket_path, b"socket placeholder").unwrap();
        fs::write(&exit_code_file, b"").unwrap();
        fs::write(&shim_log_file, b"").unwrap();
        fs::write(&netns_path, b"netns placeholder").unwrap();

        let pod = PodSandboxRecord {
            id: pod_id.clone(),
            state: "ready".to_string(),
            name: "sandbox".to_string(),
            namespace: "default".to_string(),
            uid: "uid-a".to_string(),
            created_at: 1_700_000_001,
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            pause_container_id: Some("pause-a".to_string()),
            ip: Some("10.88.0.2".to_string()),
        };
        let container = ContainerRecord {
            id: container_id.clone(),
            pod_id: Some(pod_id.clone()),
            state: "running".to_string(),
            image: "docker.io/library/busybox:latest".to_string(),
            command: "sleep 60".to_string(),
            created_at: 1_700_000_002,
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
            runtime_handler: Some("runc".to_string()),
            runtime_backend: Some("process".to_string()),
            snapshot_key: Some("snapshot-a".to_string()),
        };
        let image = ImageRecord {
            id: "sha256:image-a".to_string(),
            size: 64,
            pinned: false,
            pulled_at: 1_700_000_000,
            source_reference: Some("docker.io/library/busybox:latest".to_string()),
            os: Some("linux".to_string()),
            architecture: Some("amd64".to_string()),
            config_user: None,
            config_env_json: "[]".to_string(),
            config_entrypoint_json: "[]".to_string(),
            config_cmd_json: "[\"sleep\",\"60\"]".to_string(),
            config_working_dir: None,
            annotations_json: "{}".to_string(),
            declared_volumes_json: "[]".to_string(),
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: Some("sha256:manifest-a".to_string()),
            selected_platform: Some("linux/amd64".to_string()),
            stored_layers_json: "[]".to_string(),
            artifact_type: None,
            artifact_blobs_json: "[]".to_string(),
            cache_path: Some("images/sha256_image_a".to_string()),
        };
        let image_refs = [ImageRefRecord {
            reference: "docker.io/library/busybox:latest".to_string(),
            image_id: image.id.clone(),
            namespace: Some("k8s.io".to_string()),
            ref_kind: "tag".to_string(),
        }];
        let snapshot = SnapshotRecord {
            key: "snapshot-a".to_string(),
            image_id: image.id.clone(),
            owner_kind: "container".to_string(),
            owner_id: container_id.clone(),
            state: SnapshotLedgerState::Mounted.as_str().to_string(),
            mountpoint: rootfs_path.display().to_string(),
            snapshotter: "internal-overlay-untar".to_string(),
            runtime_managed: true,
        };
        let artifacts = vec![
            RuntimeArtifactRecord {
                owner_kind: "container".to_string(),
                owner_id: container_id.clone(),
                artifact_kind: "bundle".to_string(),
                path: bundle_path.display().to_string(),
                state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
                runtime_handler: Some("runc".to_string()),
                runtime_root: Some(runtime_root.display().to_string()),
            },
            RuntimeArtifactRecord {
                owner_kind: "container".to_string(),
                owner_id: container_id.clone(),
                artifact_kind: "rootfs".to_string(),
                path: rootfs_path.display().to_string(),
                state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
                runtime_handler: Some("runc".to_string()),
                runtime_root: Some(runtime_root.display().to_string()),
            },
        ];
        let shim = ShimProcessRecord {
            container_id: container_id.clone(),
            shim_pid: std::process::id(),
            work_dir: shim_work_dir.display().to_string(),
            socket_path: shim_socket_path.display().to_string(),
            exit_code_file: exit_code_file.display().to_string(),
            log_file: shim_log_file.display().to_string(),
            bundle_path: bundle_path.display().to_string(),
            state: ShimLedgerState::Running.as_str().to_string(),
            last_seen_at: 1_700_000_003,
        };

        {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger.save_pod_sandbox(&pod).unwrap();
            ledger.save_container(&container).unwrap();
            ledger.save_image(&image, &image_refs).unwrap();
            ledger.save_snapshot(&snapshot).unwrap();
            ledger
                .replace_runtime_artifacts("container", &container_id, &artifacts)
                .unwrap();
            ledger.save_shim_process(&shim).unwrap();
        }

        Self {
            _dir: dir,
            persistence,
            db_path,
            pod_id,
            container_id,
            bundle_path,
            rootfs_path,
            shim_socket_path,
            shim_work_dir,
            runtime_state_dir,
        }
    }

    fn snapshot(&self) -> RecoveryLedgerSnapshot {
        StateLedger::new(&self.persistence)
            .recovery_snapshot()
            .unwrap()
    }

    fn remove_bundle(&self) {
        fs::remove_dir_all(&self.bundle_path).unwrap();
    }

    fn remove_rootfs(&self) {
        fs::remove_dir_all(&self.rootfs_path).unwrap();
    }

    fn remove_shim_socket(&self) {
        fs::remove_file(&self.shim_socket_path).unwrap();
    }

    fn remove_shim_ledger_record(&mut self) {
        StateLedgerWriter::new(&mut self.persistence)
            .delete_shim_process(&self.container_id)
            .unwrap();
    }

    fn remove_shim_diagnostic_cache(&self) {
        let _ = fs::remove_file(self.shim_work_dir.join("shim.json"));
        let _ = fs::remove_file(self.shim_work_dir.join("shim.pid"));
    }

    fn runtime_state_path(&self, container_id: &str) -> PathBuf {
        self.runtime_state_dir.join(format!("{container_id}.state"))
    }

    fn runtime_pid_path(&self, container_id: &str) -> PathBuf {
        self.runtime_state_dir.join(format!("{container_id}.pid"))
    }

    fn set_live_runtime_state(&self, container_id: &str, state: &str) {
        fs::create_dir_all(&self.runtime_state_dir).unwrap();
        fs::write(self.runtime_state_path(container_id), state).unwrap();
        if state == "running" {
            fs::write(
                self.runtime_pid_path(container_id),
                std::process::id().to_string(),
            )
            .unwrap();
        } else {
            let _ = fs::remove_file(self.runtime_pid_path(container_id));
        }
    }

    fn clear_live_runtime_state(&self, container_id: &str) {
        let _ = fs::remove_file(self.runtime_state_path(container_id));
        let _ = fs::remove_file(self.runtime_pid_path(container_id));
    }

    fn live_runtime_state(&self, container_id: &str) -> Option<String> {
        fs::read_to_string(self.runtime_state_path(container_id))
            .ok()
            .map(|state| state.trim().to_string())
            .filter(|state| !state.is_empty())
    }

    fn live_runtime_pid(&self, container_id: &str) -> Option<u32> {
        fs::read_to_string(self.runtime_pid_path(container_id))
            .ok()
            .and_then(|pid| pid.trim().parse().ok())
    }

    fn add_orphan_bundle_artifact(&mut self, owner_id: &str) -> PathBuf {
        let runtime_root = self
            .bundle_path
            .parent()
            .and_then(|container_dir| container_dir.parent())
            .unwrap_or_else(|| Path::new("."));
        let path = runtime_root.join(owner_id).join("bundle");
        fs::create_dir_all(&path).unwrap();

        let artifact = RuntimeArtifactRecord {
            owner_kind: "container".to_string(),
            owner_id: owner_id.to_string(),
            artifact_kind: "bundle".to_string(),
            path: path.display().to_string(),
            state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
            runtime_handler: Some("runc".to_string()),
            runtime_root: Some(runtime_root.display().to_string()),
        };
        StateLedgerWriter::new(&mut self.persistence)
            .replace_runtime_artifacts("container", owner_id, &[artifact])
            .unwrap();

        path
    }

    fn add_live_orphan_bundle_artifact(&mut self, owner_id: &str) -> PathBuf {
        let path = self.add_orphan_bundle_artifact(owner_id);
        self.set_live_runtime_state(owner_id, "running");
        path
    }

    fn mark_snapshot_external_stale(&mut self) {
        let mut snapshot = self
            .snapshot()
            .snapshots
            .into_iter()
            .find(|snapshot| snapshot.key == "snapshot-a")
            .unwrap();
        snapshot.snapshotter = "stargz".to_string();
        snapshot.runtime_managed = false;
        snapshot.state = SnapshotLedgerState::Stale.as_str().to_string();
        StateLedgerWriter::new(&mut self.persistence)
            .save_snapshot(&snapshot)
            .unwrap();
    }
}

#[test]
fn recovery_fixture_builds_healthy_ledger_graph() {
    let fixture = RecoveryFixture::new();
    let snapshot = fixture.snapshot();

    assert!(fixture.bundle_path.exists());
    assert!(fixture.rootfs_path.exists());
    assert!(fixture.shim_socket_path.exists());
    assert_eq!(snapshot.pods.len(), 1);
    assert_eq!(snapshot.pods[0].id, fixture.pod_id);
    assert_eq!(snapshot.containers.len(), 1);
    assert_eq!(snapshot.containers[0].record.id, fixture.container_id);
    assert_eq!(snapshot.images.len(), 1);
    assert_eq!(snapshot.image_refs.len(), 1);
    assert_eq!(snapshot.snapshots.len(), 1);
    assert_eq!(snapshot.runtime_artifacts.len(), 2);
    assert_eq!(snapshot.shim_processes.len(), 1);
}

#[test]
fn recovery_fixture_round_trips_external_stale_snapshot_metadata() {
    let mut fixture = RecoveryFixture::new();
    fixture.mark_snapshot_external_stale();

    let snapshot = fixture.snapshot();
    let external = snapshot
        .snapshots
        .iter()
        .find(|snapshot| snapshot.key == "snapshot-a")
        .unwrap();

    assert_eq!(external.snapshotter, "stargz");
    assert!(!external.runtime_managed);
    assert_eq!(external.state, SnapshotLedgerState::Stale.as_str());
}

#[test]
fn recovery_fixture_models_missing_container_artifacts() {
    let fixture = RecoveryFixture::new();

    fixture.remove_bundle();
    fixture.remove_rootfs();
    let snapshot = fixture.snapshot();

    assert!(!fixture.bundle_path.exists());
    assert!(!fixture.rootfs_path.exists());
    assert!(snapshot.runtime_artifacts.iter().any(|artifact| {
        artifact.owner_id == fixture.container_id
            && artifact.artifact_kind == "bundle"
            && artifact.path == fixture.bundle_path.display().to_string()
    }));
    assert!(snapshot.runtime_artifacts.iter().any(|artifact| {
        artifact.owner_id == fixture.container_id
            && artifact.artifact_kind == "rootfs"
            && artifact.path == fixture.rootfs_path.display().to_string()
    }));
}

#[test]
fn recovery_fixture_models_missing_shim_socket() {
    let fixture = RecoveryFixture::new();

    fixture.remove_shim_socket();
    let snapshot = fixture.snapshot();

    assert!(!fixture.shim_socket_path.exists());
    assert_eq!(snapshot.shim_processes.len(), 1);
    assert_eq!(
        snapshot.shim_processes[0].socket_path,
        fixture.shim_socket_path.display().to_string()
    );
}

#[test]
fn recovery_fixture_models_missing_shim_metadata() {
    let mut fixture = RecoveryFixture::new();

    fixture.remove_shim_ledger_record();
    let snapshot = fixture.snapshot();

    assert!(fixture.shim_socket_path.exists());
    assert!(snapshot.shim_processes.is_empty());
    assert!(snapshot
        .containers
        .iter()
        .any(|entry| entry.record.id == fixture.container_id));
}

#[test]
fn recovery_fixture_recovers_shim_from_ledger_without_diagnostic_cache() {
    let fixture = RecoveryFixture::new();
    fixture.remove_shim_diagnostic_cache();

    let snapshot = fixture.snapshot();

    assert!(!fixture.shim_work_dir.join("shim.json").exists());
    assert!(!fixture.shim_work_dir.join("shim.pid").exists());
    assert_eq!(snapshot.shim_processes.len(), 1);
    assert_eq!(
        snapshot.shim_processes[0].container_id,
        fixture.container_id
    );
    assert_eq!(
        snapshot.shim_processes[0].socket_path,
        fixture.shim_socket_path.display().to_string()
    );
}

#[test]
fn recovery_fixture_schema_migration_is_idempotent_across_reopen() {
    let fixture = RecoveryFixture::new();
    let db_path = fixture.db_path.clone();
    drop(fixture);

    let first = PersistenceManager::new(PersistenceConfig {
        db_path: db_path.clone(),
        enable_recovery: true,
        auto_save_interval: 30,
    })
    .unwrap();
    assert_eq!(StateLedger::new(&first).schema_version().unwrap(), 2);
    let first_migration = StateLedger::new(&first)
        .latest_schema_migration()
        .unwrap()
        .unwrap();
    drop(first);

    let second = PersistenceManager::new(PersistenceConfig {
        db_path,
        enable_recovery: true,
        auto_save_interval: 30,
    })
    .unwrap();
    let second_migration = StateLedger::new(&second)
        .latest_schema_migration()
        .unwrap()
        .unwrap();
    assert_eq!(second_migration.version, first_migration.version);
    assert_eq!(second_migration.applied_at, first_migration.applied_at);
    assert_eq!(
        second_migration.migration_name,
        first_migration.migration_name
    );
}

#[test]
fn recovery_fixture_ledger_repair_dry_run_preserves_state_and_apply_marks_broken() {
    let mut fixture = RecoveryFixture::new();
    fixture.remove_bundle();
    fixture.remove_rootfs();
    fixture.remove_shim_socket();

    let dry_run = StateLedgerWriter::new(&mut fixture.persistence)
        .repair(LedgerRepairOptions::default(), true)
        .unwrap();

    assert!(dry_run.dry_run);
    assert!(dry_run
        .issues
        .iter()
        .any(|issue| issue.kind == "missingArtifact"));
    assert!(dry_run
        .issues
        .iter()
        .any(|issue| issue.kind == "brokenShim"));
    assert_eq!(dry_run.applied_action_count, 0);
    let after_dry_run = fixture.snapshot();
    assert!(after_dry_run
        .runtime_artifacts
        .iter()
        .all(|artifact| { artifact.state == RuntimeArtifactLedgerState::Active.as_str() }));
    assert_eq!(
        after_dry_run.shim_processes[0].state,
        ShimLedgerState::Running.as_str()
    );

    let repair = StateLedgerWriter::new(&mut fixture.persistence)
        .repair(LedgerRepairOptions::default(), false)
        .unwrap();

    assert!(!repair.dry_run);
    assert!(repair.applied_action_count >= 3);
    let repaired = fixture.snapshot();
    assert!(repaired
        .runtime_artifacts
        .iter()
        .all(|artifact| { artifact.state == RuntimeArtifactLedgerState::Broken.as_str() }));
    assert_eq!(
        repaired.shim_processes[0].state,
        ShimLedgerState::Dead.as_str()
    );
}

#[test]
fn recovery_fixture_can_model_orphan_bundle_artifact() {
    let mut fixture = RecoveryFixture::new();
    let orphan_path = fixture.add_orphan_bundle_artifact("orphan-container");
    let snapshot = fixture.snapshot();

    assert!(orphan_path.exists());
    assert!(!snapshot
        .containers
        .iter()
        .any(|entry| entry.record.id == "orphan-container"));
    assert!(snapshot.runtime_artifacts.iter().any(|artifact| {
        artifact.owner_id == "orphan-container"
            && artifact.artifact_kind == "bundle"
            && artifact.path == orphan_path.display().to_string()
    }));
}

#[test]
fn recovery_fixture_can_model_fake_live_runtime_state() {
    let fixture = RecoveryFixture::new();

    fixture.set_live_runtime_state(&fixture.container_id, "running");
    assert_eq!(
        fixture.live_runtime_state(&fixture.container_id).as_deref(),
        Some("running")
    );
    assert_eq!(
        fixture.live_runtime_pid(&fixture.container_id),
        Some(std::process::id())
    );

    fixture.set_live_runtime_state(&fixture.container_id, "stopped");
    assert_eq!(
        fixture.live_runtime_state(&fixture.container_id).as_deref(),
        Some("stopped")
    );
    assert_eq!(fixture.live_runtime_pid(&fixture.container_id), None);

    fixture.clear_live_runtime_state(&fixture.container_id);
    assert_eq!(fixture.live_runtime_state(&fixture.container_id), None);
}

#[test]
fn recovery_fixture_can_model_live_orphan_bundle_artifact() {
    let mut fixture = RecoveryFixture::new();
    let orphan_path = fixture.add_live_orphan_bundle_artifact("live-orphan-container");
    let snapshot = fixture.snapshot();

    assert!(orphan_path.exists());
    assert_eq!(
        fixture
            .live_runtime_state("live-orphan-container")
            .as_deref(),
        Some("running")
    );
    assert_eq!(
        fixture.live_runtime_pid("live-orphan-container"),
        Some(std::process::id())
    );
    assert!(!snapshot
        .containers
        .iter()
        .any(|entry| entry.record.id == "live-orphan-container"));
    assert!(snapshot.runtime_artifacts.iter().any(|artifact| {
        artifact.owner_id == "live-orphan-container"
            && artifact.artifact_kind == "bundle"
            && artifact.path == orphan_path.display().to_string()
    }));
}
