use anyhow::Result;

use crate::runtime::ContainerStatus;
use crate::storage::persistence::PersistenceManager;
use crate::storage::{
    ContainerRecord, ImageRecord, ImageRefRecord, PodSandboxRecord, RuntimeArtifactRecord,
    ShimProcessRecord, SnapshotRecord, StateEvent,
};

#[derive(Debug, Clone)]
pub struct RecoveryContainerEntry {
    pub status: ContainerStatus,
    pub record: ContainerRecord,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryLedgerSnapshot {
    pub containers: Vec<RecoveryContainerEntry>,
    pub pods: Vec<PodSandboxRecord>,
    pub images: Vec<ImageRecord>,
    pub image_refs: Vec<ImageRefRecord>,
    pub snapshots: Vec<SnapshotRecord>,
    pub runtime_artifacts: Vec<RuntimeArtifactRecord>,
    pub shim_processes: Vec<ShimProcessRecord>,
}

impl RecoveryLedgerSnapshot {
    pub fn load(persistence: &PersistenceManager) -> Result<Self> {
        StateLedger::new(persistence).recovery_snapshot()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateLedger<'a> {
    persistence: &'a PersistenceManager,
}

impl<'a> StateLedger<'a> {
    pub fn new(persistence: &'a PersistenceManager) -> Self {
        Self { persistence }
    }

    pub fn containers(&self) -> Result<Vec<RecoveryContainerEntry>> {
        Ok(self
            .persistence
            .recover_containers()?
            .into_iter()
            .map(|(_id, status, record)| RecoveryContainerEntry { status, record })
            .collect())
    }

    pub fn pod_sandboxes(&self) -> Result<Vec<PodSandboxRecord>> {
        self.persistence.recover_pods()
    }

    pub fn images(&self) -> Result<Vec<ImageRecord>> {
        self.persistence.list_image_records()
    }

    pub fn image(&self, image_id: &str) -> Result<Option<ImageRecord>> {
        self.persistence.get_image_record(image_id)
    }

    pub fn image_refs(&self, image_id: Option<&str>) -> Result<Vec<ImageRefRecord>> {
        self.persistence.list_image_refs(image_id)
    }

    pub fn snapshots(&self) -> Result<Vec<SnapshotRecord>> {
        self.persistence.list_snapshot_records()
    }

    pub fn runtime_artifacts(&self) -> Result<Vec<RuntimeArtifactRecord>> {
        self.persistence.list_runtime_artifacts()
    }

    pub fn shim_processes(&self) -> Result<Vec<ShimProcessRecord>> {
        self.persistence.list_shim_process_records()
    }

    pub fn shim_process(&self, container_id: &str) -> Result<Option<ShimProcessRecord>> {
        self.persistence.get_shim_process_record(container_id)
    }

    pub fn recent_events(&self, entity_type: &str, since: i64) -> Result<Vec<StateEvent>> {
        self.persistence
            .storage()
            .get_recent_events(entity_type, since)
    }

    pub fn recovery_snapshot(&self) -> Result<RecoveryLedgerSnapshot> {
        let containers = self.containers()?;
        let pods = self.pod_sandboxes()?;
        let images = self.images()?;
        let image_refs = self.image_refs(None)?;
        let snapshots = self.snapshots()?;
        let runtime_artifacts = self.runtime_artifacts()?;
        let shim_processes = self.shim_processes()?;

        Ok(RecoveryLedgerSnapshot {
            containers,
            pods,
            images,
            image_refs,
            snapshots,
            runtime_artifacts,
            shim_processes,
        })
    }
}

#[derive(Debug)]
pub struct StateLedgerWriter<'a> {
    persistence: &'a mut PersistenceManager,
}

impl<'a> StateLedgerWriter<'a> {
    pub fn new(persistence: &'a mut PersistenceManager) -> Self {
        Self { persistence }
    }

    pub fn save_container(&mut self, record: &ContainerRecord) -> Result<()> {
        self.persistence.storage_mut().save_container(record)
    }

    pub fn delete_container(&mut self, container_id: &str) -> Result<()> {
        self.persistence.delete_container(container_id)
    }

    pub fn save_pod_sandbox(&mut self, record: &PodSandboxRecord) -> Result<()> {
        self.persistence.storage_mut().save_pod_sandbox(record)
    }

    pub fn delete_pod_sandbox(&mut self, pod_id: &str) -> Result<()> {
        self.persistence.delete_pod_sandbox(pod_id)
    }

    pub fn save_image(&mut self, record: &ImageRecord, refs: &[ImageRefRecord]) -> Result<()> {
        self.persistence.save_image_record(record, refs)
    }

    pub fn delete_image(&mut self, image_id: &str) -> Result<()> {
        self.persistence.delete_image_record(image_id)
    }

    pub fn save_snapshot(&mut self, record: &SnapshotRecord) -> Result<()> {
        self.persistence.save_snapshot_record(record)
    }

    pub fn delete_snapshot(&mut self, key: &str) -> Result<()> {
        self.persistence.delete_snapshot_record(key)
    }

    pub fn replace_runtime_artifacts(
        &mut self,
        owner_kind: &str,
        owner_id: &str,
        records: &[RuntimeArtifactRecord],
    ) -> Result<()> {
        self.persistence
            .replace_runtime_artifacts(owner_kind, owner_id, records)
    }

    pub fn delete_runtime_artifacts(&mut self, owner_kind: &str, owner_id: &str) -> Result<()> {
        self.persistence
            .delete_runtime_artifacts(owner_kind, owner_id)
    }

    pub fn save_shim_process(&mut self, record: &ShimProcessRecord) -> Result<()> {
        self.persistence.save_shim_process_record(record)
    }

    pub fn delete_shim_process(&mut self, container_id: &str) -> Result<()> {
        self.persistence.delete_shim_process_record(container_id)
    }

    pub fn append_event(
        &mut self,
        entity_type: &str,
        entity_id: &str,
        old_state: Option<&str>,
        new_state: Option<&str>,
        details: Option<&str>,
    ) -> Result<()> {
        self.persistence.storage_mut().append_event(
            entity_type,
            entity_id,
            old_state,
            new_state,
            details,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::persistence::PersistenceConfig;
    use tempfile::tempdir;

    fn test_image_record() -> ImageRecord {
        ImageRecord {
            id: "sha256:image".to_string(),
            size: 4096,
            pinned: false,
            pulled_at: 1_700_000_000,
            source_reference: Some("docker.io/library/busybox:latest".to_string()),
            os: Some("linux".to_string()),
            architecture: Some("amd64".to_string()),
            config_user: Some("0".to_string()),
            config_env_json: "[]".to_string(),
            config_entrypoint_json: "[]".to_string(),
            config_cmd_json: "[\"sleep\",\"60\"]".to_string(),
            config_working_dir: Some("/".to_string()),
            annotations_json: "{}".to_string(),
            declared_volumes_json: "[]".to_string(),
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: Some("sha256:manifest".to_string()),
            selected_platform: Some("linux/amd64".to_string()),
            stored_layers_json: "[]".to_string(),
            artifact_type: None,
            artifact_blobs_json: "[]".to_string(),
            cache_path: Some("images/sha256_image".to_string()),
        }
    }

    #[test]
    fn state_ledger_round_trips_full_owner_graph() {
        let temp_dir = tempdir().unwrap();
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: temp_dir.path().join("state.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();

        let pod = PodSandboxRecord {
            id: "pod-a".to_string(),
            state: "ready".to_string(),
            name: "sandbox".to_string(),
            namespace: "default".to_string(),
            uid: "uid-a".to_string(),
            created_at: 1_700_000_001,
            netns_path: "/run/netns/pod-a".to_string(),
            labels: "{\"app\":\"demo\"}".to_string(),
            annotations: "{}".to_string(),
            pause_container_id: Some("pause-a".to_string()),
            ip: Some("10.88.0.2".to_string()),
        };
        let container = ContainerRecord {
            id: "container-a".to_string(),
            pod_id: pod.id.clone(),
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
        let image = test_image_record();
        let image_refs = vec![ImageRefRecord {
            reference: "docker.io/library/busybox:latest".to_string(),
            image_id: image.id.clone(),
            namespace: Some("k8s.io".to_string()),
            ref_kind: "tag".to_string(),
        }];
        let snapshot_record = SnapshotRecord {
            key: "snapshot-a".to_string(),
            image_id: image.id.clone(),
            owner_kind: "container".to_string(),
            owner_id: container.id.clone(),
            state: "mounted".to_string(),
            mountpoint: "/run/crius/snapshots/snapshot-a/rootfs".to_string(),
        };
        let artifacts = vec![
            RuntimeArtifactRecord {
                owner_kind: "container".to_string(),
                owner_id: container.id.clone(),
                artifact_kind: "bundle".to_string(),
                path: "/run/crius/container-a/bundle".to_string(),
                runtime_handler: Some("runc".to_string()),
                runtime_root: Some("/run/runc".to_string()),
            },
            RuntimeArtifactRecord {
                owner_kind: "pod".to_string(),
                owner_id: pod.id.clone(),
                artifact_kind: "netns".to_string(),
                path: pod.netns_path.clone(),
                runtime_handler: None,
                runtime_root: None,
            },
        ];
        let shim = ShimProcessRecord {
            container_id: container.id.clone(),
            shim_pid: 4242,
            work_dir: "/run/crius/container-a/shim".to_string(),
            socket_path: "/run/crius/container-a/shim.sock".to_string(),
            exit_code_file: "/run/crius/container-a/exit".to_string(),
            log_file: "/run/crius/container-a/shim.log".to_string(),
            bundle_path: "/run/crius/container-a/bundle".to_string(),
            state: "running".to_string(),
            last_seen_at: 1_700_000_003,
        };

        {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger.save_pod_sandbox(&pod).unwrap();
            ledger.save_container(&container).unwrap();
            ledger.save_image(&image, &image_refs).unwrap();
            ledger.save_snapshot(&snapshot_record).unwrap();
            ledger
                .replace_runtime_artifacts("container", &container.id, &artifacts[..1])
                .unwrap();
            ledger
                .replace_runtime_artifacts("pod", &pod.id, &artifacts[1..])
                .unwrap();
            ledger.save_shim_process(&shim).unwrap();
            ledger
                .append_event(
                    "container",
                    &container.id,
                    Some("created"),
                    Some("running"),
                    Some("state-ledger-test"),
                )
                .unwrap();
        }

        let ledger = StateLedger::new(&persistence);
        let snapshot = ledger.recovery_snapshot().unwrap();

        assert_eq!(snapshot.pods.len(), 1);
        assert_eq!(snapshot.pods[0].id, pod.id);
        assert_eq!(snapshot.containers.len(), 1);
        assert_eq!(snapshot.containers[0].status, ContainerStatus::Running);
        assert_eq!(snapshot.containers[0].record.pod_id, "pod-a");
        assert_eq!(
            snapshot.containers[0].record.snapshot_key.as_deref(),
            Some("snapshot-a")
        );
        assert_eq!(snapshot.images.len(), 1);
        assert_eq!(snapshot.images[0].id, image.id);
        assert_eq!(snapshot.image_refs.len(), 1);
        assert_eq!(snapshot.image_refs[0].image_id, "sha256:image");
        assert_eq!(snapshot.snapshots.len(), 1);
        assert_eq!(snapshot.snapshots[0].owner_id, "container-a");
        assert_eq!(snapshot.runtime_artifacts.len(), 2);
        assert!(snapshot
            .runtime_artifacts
            .iter()
            .any(|record| record.owner_kind == "pod" && record.owner_id == "pod-a"));
        assert_eq!(snapshot.shim_processes.len(), 1);
        assert_eq!(snapshot.shim_processes[0].container_id, "container-a");
        assert_eq!(
            ledger
                .shim_process("container-a")
                .unwrap()
                .map(|record| record.shim_pid),
            Some(4242)
        );
        assert_eq!(
            ledger
                .image("sha256:image")
                .unwrap()
                .map(|record| record.size),
            Some(4096)
        );

        let events = ledger.recent_events("container", 0).unwrap();
        assert!(events.iter().any(|event| event.entity_id == "container-a"
            && event.details.as_deref() == Some("state-ledger-test")));
    }
}
