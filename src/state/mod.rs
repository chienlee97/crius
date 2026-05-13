use anyhow::Result;

use crate::runtime::ContainerStatus;
use crate::storage::persistence::PersistenceManager;
use crate::storage::{
    ContainerRecord, ImageRecord, ImageRefRecord, PodSandboxRecord, RuntimeArtifactRecord,
    ShimProcessRecord, SnapshotRecord,
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
        let containers = persistence
            .recover_containers()?
            .into_iter()
            .map(|(_id, status, record)| RecoveryContainerEntry { status, record })
            .collect();
        let pods = persistence.recover_pods()?;
        let images = persistence.list_image_records()?;
        let image_refs = persistence.list_image_refs(None)?;
        let snapshots = persistence.list_snapshot_records()?;
        let runtime_artifacts = persistence.list_runtime_artifacts()?;
        let shim_processes = persistence.list_shim_process_records()?;

        Ok(Self {
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
