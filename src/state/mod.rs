use anyhow::Result;
use serde::Serialize;
use serde_json::json;
use std::collections::HashSet;
use std::fmt;
use std::path::Path;

use crate::runtime::ContainerStatus;
use crate::storage::persistence::PersistenceManager;
use crate::storage::{
    ContainerRecord, ContentGcCandidate, ContentTransferRecord, ImageRecord, ImageRefRecord,
    PodSandboxRecord, RuntimeArtifactRecord, SchemaMigrationRecord, ShimProcessRecord,
    SnapshotRecord, StateEvent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotLedgerState {
    Preparing,
    Prepared,
    Mounted,
    Stale,
    Committed,
    Removing,
    Deleted,
    Broken,
}

impl SnapshotLedgerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Prepared => "prepared",
            Self::Mounted => "mounted",
            Self::Stale => "stale",
            Self::Committed => "committed",
            Self::Removing => "removing",
            Self::Deleted => "deleted",
            Self::Broken => "broken",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "preparing" => Ok(Self::Preparing),
            "prepared" => Ok(Self::Prepared),
            "mounted" => Ok(Self::Mounted),
            "stale" => Ok(Self::Stale),
            "committed" => Ok(Self::Committed),
            "removing" => Ok(Self::Removing),
            "deleted" => Ok(Self::Deleted),
            "broken" => Ok(Self::Broken),
            other => anyhow::bail!("invalid snapshot ledger state: {other}"),
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Preparing,
                    Self::Prepared | Self::Removing | Self::Broken
                ) | (
                    Self::Prepared,
                    Self::Mounted | Self::Committed | Self::Removing | Self::Broken
                ) | (
                    Self::Mounted,
                    Self::Stale | Self::Committed | Self::Removing | Self::Broken
                ) | (Self::Stale, Self::Mounted | Self::Removing | Self::Broken)
                    | (Self::Committed, Self::Removing | Self::Broken)
                    | (Self::Removing, Self::Deleted | Self::Broken)
                    | (Self::Broken, Self::Removing | Self::Deleted)
            )
    }
}

impl fmt::Display for SnapshotLedgerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeArtifactLedgerState {
    Planned,
    Created,
    Active,
    Stale,
    Deleted,
    Broken,
}

impl RuntimeArtifactLedgerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Created => "created",
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Deleted => "deleted",
            Self::Broken => "broken",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "planned" => Ok(Self::Planned),
            "created" => Ok(Self::Created),
            "active" => Ok(Self::Active),
            "stale" => Ok(Self::Stale),
            "deleted" => Ok(Self::Deleted),
            "broken" => Ok(Self::Broken),
            other => anyhow::bail!("invalid runtime artifact ledger state: {other}"),
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Planned, Self::Created | Self::Deleted | Self::Broken)
                    | (
                        Self::Created,
                        Self::Active | Self::Stale | Self::Deleted | Self::Broken
                    )
                    | (Self::Active, Self::Stale | Self::Deleted | Self::Broken)
                    | (Self::Stale, Self::Active | Self::Deleted | Self::Broken)
                    | (Self::Broken, Self::Stale | Self::Deleted)
            )
    }
}

impl fmt::Display for RuntimeArtifactLedgerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShimLedgerState {
    Starting,
    Running,
    Exited,
    Dead,
    Broken,
    Degraded,
}

impl ShimLedgerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Exited => "exited",
            Self::Dead => "dead",
            Self::Broken => "broken",
            Self::Degraded => "degraded",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "planned" | "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "exited" => Ok(Self::Exited),
            "dead" => Ok(Self::Dead),
            "broken" => Ok(Self::Broken),
            "degraded" => Ok(Self::Degraded),
            other => anyhow::bail!("invalid shim ledger state: {other}"),
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Starting,
                    Self::Running | Self::Exited | Self::Dead | Self::Broken
                ) | (
                    Self::Running,
                    Self::Exited | Self::Dead | Self::Broken | Self::Degraded
                ) | (Self::Exited, Self::Dead | Self::Broken)
                    | (Self::Dead, Self::Starting | Self::Broken)
                    | (Self::Broken, Self::Starting | Self::Dead)
                    | (Self::Degraded, Self::Running | Self::Dead | Self::Broken)
            )
    }
}

impl fmt::Display for ShimLedgerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerCheckOptions {
    pub check_files: bool,
}

impl Default for LedgerCheckOptions {
    fn default() -> Self {
        Self { check_files: true }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerRepairOptions {
    pub check_files: bool,
}

impl Default for LedgerRepairOptions {
    fn default() -> Self {
        Self { check_files: true }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LedgerCheckIssue {
    pub kind: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub severity: String,
    pub repairable: bool,
    pub message: String,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LedgerRepairAction {
    pub action: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub dry_run: bool,
    pub applied: bool,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LedgerCheckReport {
    pub dry_run: bool,
    pub checked_at_unix_millis: i64,
    pub issue_count: usize,
    pub repairable_issue_count: usize,
    pub action_count: usize,
    pub applied_action_count: usize,
    pub issues: Vec<LedgerCheckIssue>,
    pub actions: Vec<LedgerRepairAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LedgerRepairOperation {
    MarkSnapshotBroken {
        key: String,
    },
    MarkRuntimeArtifactBroken {
        owner_kind: String,
        owner_id: String,
        artifact_kind: String,
        path: String,
    },
    MarkShimDead {
        container_id: String,
    },
    DeleteDanglingShim {
        container_id: String,
    },
}

#[derive(Debug, Clone)]
struct LedgerCheckPlan {
    report: LedgerCheckReport,
    operations: Vec<LedgerRepairOperation>,
}

impl RecoveryLedgerSnapshot {
    fn check_plan(&self, options: LedgerCheckOptions, dry_run: bool) -> LedgerCheckPlan {
        let pod_ids: HashSet<_> = self.pods.iter().map(|pod| pod.id.as_str()).collect();
        let container_ids: HashSet<_> = self
            .containers
            .iter()
            .map(|entry| entry.record.id.as_str())
            .collect();
        let image_ids: HashSet<_> = self.images.iter().map(|image| image.id.as_str()).collect();
        let snapshot_keys: HashSet<_> = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.key.as_str())
            .collect();

        let mut issues = Vec::new();
        let mut operations = Vec::new();

        for entry in &self.containers {
            let container = &entry.record;
            if !pod_ids.contains(container.pod_id.as_str()) {
                push_issue(
                    &mut issues,
                    "ownerGraphBroken",
                    "container",
                    &container.id,
                    false,
                    format!(
                        "container {} references missing pod {}",
                        container.id, container.pod_id
                    ),
                    json!({ "missingOwnerKind": "pod", "missingOwnerId": container.pod_id }),
                );
            }

            if let Some(snapshot_key) = &container.snapshot_key {
                if !snapshot_keys.contains(snapshot_key.as_str()) {
                    push_issue(
                        &mut issues,
                        "danglingRef",
                        "container",
                        &container.id,
                        false,
                        format!(
                            "container {} references missing snapshot {}",
                            container.id, snapshot_key
                        ),
                        json!({ "missingRefKind": "snapshot", "missingRefId": snapshot_key }),
                    );
                }
            }
        }

        for pod in &self.pods {
            if let Some(pause_container_id) = &pod.pause_container_id {
                if !container_ids.contains(pause_container_id.as_str()) {
                    push_issue(
                        &mut issues,
                        "danglingRef",
                        "pod",
                        &pod.id,
                        false,
                        format!(
                            "pod {} references missing pause container {}",
                            pod.id, pause_container_id
                        ),
                        json!({
                            "missingRefKind": "container",
                            "missingRefId": pause_container_id,
                            "refField": "pauseContainerId",
                        }),
                    );
                }
            }
        }

        for image_ref in &self.image_refs {
            if !image_ids.contains(image_ref.image_id.as_str()) {
                push_issue(
                    &mut issues,
                    "danglingRef",
                    "imageRef",
                    &image_ref.reference,
                    false,
                    format!(
                        "image ref {} points to missing image {}",
                        image_ref.reference, image_ref.image_id
                    ),
                    json!({ "missingRefKind": "image", "missingRefId": image_ref.image_id }),
                );
            }
        }

        for snapshot in &self.snapshots {
            if snapshot.state == SnapshotLedgerState::Broken.as_str() {
                push_issue(
                    &mut issues,
                    "brokenSnapshot",
                    "snapshot",
                    &snapshot.key,
                    false,
                    format!("snapshot {} is marked broken", snapshot.key),
                    json!({ "state": snapshot.state }),
                );
            } else if snapshot.state == SnapshotLedgerState::Stale.as_str() {
                push_issue(
                    &mut issues,
                    "staleSnapshot",
                    "snapshot",
                    &snapshot.key,
                    false,
                    format!("snapshot {} is marked stale", snapshot.key),
                    json!({ "state": snapshot.state }),
                );
            }

            if !image_ids.contains(snapshot.image_id.as_str()) {
                push_issue(
                    &mut issues,
                    "danglingRef",
                    "snapshot",
                    &snapshot.key,
                    true,
                    format!(
                        "snapshot {} references missing image {}",
                        snapshot.key, snapshot.image_id
                    ),
                    json!({ "missingRefKind": "image", "missingRefId": snapshot.image_id }),
                );
                operations.push(LedgerRepairOperation::MarkSnapshotBroken {
                    key: snapshot.key.clone(),
                });
            }

            let owner_exists = match snapshot.owner_kind.as_str() {
                "container" => container_ids.contains(snapshot.owner_id.as_str()),
                "pod" => pod_ids.contains(snapshot.owner_id.as_str()),
                "image" => image_ids.contains(snapshot.owner_id.as_str()),
                _ => false,
            };
            if !owner_exists {
                push_issue(
                    &mut issues,
                    "ownerGraphBroken",
                    "snapshot",
                    &snapshot.key,
                    snapshot.state != SnapshotLedgerState::Broken.as_str(),
                    format!(
                        "snapshot {} references missing {} owner {}",
                        snapshot.key, snapshot.owner_kind, snapshot.owner_id
                    ),
                    json!({
                        "missingOwnerKind": snapshot.owner_kind,
                        "missingOwnerId": snapshot.owner_id,
                    }),
                );
                if snapshot.state != SnapshotLedgerState::Broken.as_str() {
                    operations.push(LedgerRepairOperation::MarkSnapshotBroken {
                        key: snapshot.key.clone(),
                    });
                }
            }
        }

        for artifact in &self.runtime_artifacts {
            let owner_exists = match artifact.owner_kind.as_str() {
                "container" => container_ids.contains(artifact.owner_id.as_str()),
                "pod" => pod_ids.contains(artifact.owner_id.as_str()),
                "image" => image_ids.contains(artifact.owner_id.as_str()),
                _ => false,
            };
            let can_mark_broken = artifact.state != RuntimeArtifactLedgerState::Broken.as_str()
                && artifact.state != RuntimeArtifactLedgerState::Deleted.as_str();

            if !owner_exists {
                push_issue(
                    &mut issues,
                    "ownerGraphBroken",
                    "runtimeArtifact",
                    &artifact.path,
                    can_mark_broken,
                    format!(
                        "runtime artifact {} references missing {} owner {}",
                        artifact.path, artifact.owner_kind, artifact.owner_id
                    ),
                    json!({
                        "missingOwnerKind": artifact.owner_kind,
                        "missingOwnerId": artifact.owner_id,
                        "artifactKind": artifact.artifact_kind,
                    }),
                );
                if can_mark_broken {
                    operations.push(LedgerRepairOperation::MarkRuntimeArtifactBroken {
                        owner_kind: artifact.owner_kind.clone(),
                        owner_id: artifact.owner_id.clone(),
                        artifact_kind: artifact.artifact_kind.clone(),
                        path: artifact.path.clone(),
                    });
                }
            }

            if artifact.state == RuntimeArtifactLedgerState::Broken.as_str() {
                push_issue(
                    &mut issues,
                    "brokenRuntimeArtifact",
                    "runtimeArtifact",
                    &artifact.path,
                    false,
                    format!("runtime artifact {} is marked broken", artifact.path),
                    json!({
                        "ownerKind": artifact.owner_kind,
                        "ownerId": artifact.owner_id,
                        "artifactKind": artifact.artifact_kind,
                        "state": artifact.state,
                    }),
                );
            } else if options.check_files
                && can_mark_broken
                && !artifact.path.is_empty()
                && !Path::new(&artifact.path).exists()
            {
                push_issue(
                    &mut issues,
                    "missingArtifact",
                    "runtimeArtifact",
                    &artifact.path,
                    true,
                    format!("runtime artifact path does not exist: {}", artifact.path),
                    json!({
                        "ownerKind": artifact.owner_kind,
                        "ownerId": artifact.owner_id,
                        "artifactKind": artifact.artifact_kind,
                        "state": artifact.state,
                    }),
                );
                operations.push(LedgerRepairOperation::MarkRuntimeArtifactBroken {
                    owner_kind: artifact.owner_kind.clone(),
                    owner_id: artifact.owner_id.clone(),
                    artifact_kind: artifact.artifact_kind.clone(),
                    path: artifact.path.clone(),
                });
            }
        }

        for shim in &self.shim_processes {
            if !container_ids.contains(shim.container_id.as_str()) {
                push_issue(
                    &mut issues,
                    "ownerGraphBroken",
                    "shim",
                    &shim.container_id,
                    true,
                    format!(
                        "shim process record references missing container {}",
                        shim.container_id
                    ),
                    json!({ "state": shim.state, "pid": shim.shim_pid }),
                );
                operations.push(LedgerRepairOperation::DeleteDanglingShim {
                    container_id: shim.container_id.clone(),
                });
                continue;
            }

            if shim.state == ShimLedgerState::Dead.as_str() {
                push_issue(
                    &mut issues,
                    "deadShim",
                    "shim",
                    &shim.container_id,
                    false,
                    format!("shim {} is marked dead", shim.container_id),
                    json!({ "state": shim.state, "pid": shim.shim_pid }),
                );
            } else if shim.state == ShimLedgerState::Broken.as_str() {
                push_issue(
                    &mut issues,
                    "brokenShim",
                    "shim",
                    &shim.container_id,
                    false,
                    format!("shim {} is marked broken", shim.container_id),
                    json!({ "state": shim.state, "pid": shim.shim_pid }),
                );
            } else if shim.state == ShimLedgerState::Degraded.as_str() {
                push_issue(
                    &mut issues,
                    "degradedShim",
                    "shim",
                    &shim.container_id,
                    true,
                    format!("shim {} is marked degraded", shim.container_id),
                    json!({ "state": shim.state, "pid": shim.shim_pid }),
                );
                operations.push(LedgerRepairOperation::MarkShimDead {
                    container_id: shim.container_id.clone(),
                });
            } else if options.check_files && shim_requires_liveness_probe(&shim.state) {
                let socket_exists =
                    shim.socket_path.is_empty() || Path::new(&shim.socket_path).exists();
                let process_exists = shim_process_exists(shim.shim_pid);
                if !socket_exists || !process_exists {
                    push_issue(
                        &mut issues,
                        "brokenShim",
                        "shim",
                        &shim.container_id,
                        true,
                        format!(
                            "shim {} is not reachable: socket_exists={}, process_exists={}",
                            shim.container_id, socket_exists, process_exists
                        ),
                        json!({
                            "state": shim.state,
                            "pid": shim.shim_pid,
                            "socketPath": shim.socket_path,
                            "socketExists": socket_exists,
                            "processExists": process_exists,
                        }),
                    );
                    operations.push(LedgerRepairOperation::MarkShimDead {
                        container_id: shim.container_id.clone(),
                    });
                }
            }
        }

        let mut seen_operations = HashSet::new();
        operations.retain(|operation| seen_operations.insert(operation.clone()));

        let repairable_issue_count = issues.iter().filter(|issue| issue.repairable).count();
        let actions = operations
            .iter()
            .map(|operation| operation.dry_run_action())
            .collect::<Vec<_>>();

        LedgerCheckPlan {
            report: LedgerCheckReport {
                dry_run,
                checked_at_unix_millis: chrono::Utc::now().timestamp_millis(),
                issue_count: issues.len(),
                repairable_issue_count,
                action_count: actions.len(),
                applied_action_count: 0,
                issues,
                actions,
            },
            operations,
        }
    }
}

impl LedgerRepairOperation {
    fn dry_run_action(&self) -> LedgerRepairAction {
        let (action, subject_kind, subject_id, message) = match self {
            Self::MarkSnapshotBroken { key } => (
                "markSnapshotBroken",
                "snapshot",
                key.as_str(),
                format!("would mark snapshot {key} broken"),
            ),
            Self::MarkRuntimeArtifactBroken {
                artifact_kind,
                path,
                ..
            } => (
                "markRuntimeArtifactBroken",
                "runtimeArtifact",
                path.as_str(),
                format!("would mark {artifact_kind} runtime artifact {path} broken"),
            ),
            Self::MarkShimDead { container_id } => (
                "markShimDead",
                "shim",
                container_id.as_str(),
                format!("would mark shim {container_id} dead"),
            ),
            Self::DeleteDanglingShim { container_id } => (
                "deleteDanglingShim",
                "shim",
                container_id.as_str(),
                format!("would delete dangling shim record for {container_id}"),
            ),
        };
        LedgerRepairAction {
            action: action.to_string(),
            subject_kind: subject_kind.to_string(),
            subject_id: subject_id.to_string(),
            dry_run: true,
            applied: false,
            message,
        }
    }
}

fn push_issue(
    issues: &mut Vec<LedgerCheckIssue>,
    kind: &str,
    subject_kind: &str,
    subject_id: &str,
    repairable: bool,
    message: String,
    details: serde_json::Value,
) {
    issues.push(LedgerCheckIssue {
        kind: kind.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        severity: "warning".to_string(),
        repairable,
        message,
        details,
    });
}

fn shim_requires_liveness_probe(state: &str) -> bool {
    matches!(state, "starting" | "running" | "degraded")
}

fn shim_process_exists(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        Path::new("/proc").join(pid.to_string()).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        true
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

    pub fn content_transfers(&self) -> Result<Vec<ContentTransferRecord>> {
        self.persistence.list_content_transfer_records()
    }

    pub fn content_gc_candidates(&self) -> Result<Vec<ContentGcCandidate>> {
        self.persistence.list_content_gc_candidates()
    }

    pub fn schema_version(&self) -> Result<i64> {
        self.persistence.schema_version()
    }

    pub fn latest_schema_migration(&self) -> Result<Option<SchemaMigrationRecord>> {
        self.persistence.latest_schema_migration()
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

    pub fn recent_events_for_subject(
        &self,
        subject_kind: &str,
        subject_id: &str,
        limit: usize,
    ) -> Result<Vec<StateEvent>> {
        self.persistence
            .storage()
            .get_recent_events_for_subject(subject_kind, subject_id, limit)
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

    pub fn check(&self, options: LedgerCheckOptions) -> Result<LedgerCheckReport> {
        Ok(self.recovery_snapshot()?.check_plan(options, true).report)
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

    pub fn save_content_transfer(&mut self, record: &ContentTransferRecord) -> Result<()> {
        self.persistence.save_content_transfer_record(record)
    }

    pub fn mark_running_content_transfers_interrupted(&mut self) -> Result<usize> {
        self.persistence
            .mark_running_content_transfers_interrupted()
    }

    pub fn delete_image(&mut self, image_id: &str) -> Result<()> {
        self.persistence.delete_image_record(image_id)
    }

    pub fn save_snapshot(&mut self, record: &SnapshotRecord) -> Result<()> {
        SnapshotLedgerState::parse(&record.state)?;
        self.persistence.save_snapshot_record(record)
    }

    pub fn transition_snapshot_state(
        &mut self,
        key: &str,
        next: SnapshotLedgerState,
    ) -> Result<()> {
        let record = self
            .persistence
            .list_snapshot_records()?
            .into_iter()
            .find(|record| record.key == key)
            .ok_or_else(|| anyhow::anyhow!("snapshot {key} not found"))?;
        let current = SnapshotLedgerState::parse(&record.state)?;
        if !current.can_transition_to(next) {
            anyhow::bail!("invalid snapshot state transition: {current} -> {next}");
        }
        self.persistence
            .storage_mut()
            .update_snapshot_state(key, next.as_str())
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
        for record in records {
            RuntimeArtifactLedgerState::parse(&record.state)?;
        }
        self.persistence
            .replace_runtime_artifacts(owner_kind, owner_id, records)
    }

    pub fn transition_runtime_artifact_state(
        &mut self,
        owner_kind: &str,
        owner_id: &str,
        artifact_kind: &str,
        path: &str,
        next: RuntimeArtifactLedgerState,
    ) -> Result<()> {
        let record = self
            .persistence
            .list_runtime_artifacts()?
            .into_iter()
            .find(|record| {
                record.owner_kind == owner_kind
                    && record.owner_id == owner_id
                    && record.artifact_kind == artifact_kind
                    && record.path == path
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "runtime artifact {owner_kind}/{owner_id}/{artifact_kind} at {path} not found"
                )
            })?;
        let current = RuntimeArtifactLedgerState::parse(&record.state)?;
        if !current.can_transition_to(next) {
            anyhow::bail!("invalid runtime artifact state transition: {current} -> {next}");
        }
        self.persistence
            .storage_mut()
            .update_runtime_artifact_state(owner_kind, owner_id, artifact_kind, path, next.as_str())
    }

    pub fn delete_runtime_artifacts(&mut self, owner_kind: &str, owner_id: &str) -> Result<()> {
        self.persistence
            .delete_runtime_artifacts(owner_kind, owner_id)
    }

    pub fn save_shim_process(&mut self, record: &ShimProcessRecord) -> Result<()> {
        ShimLedgerState::parse(&record.state)?;
        self.persistence.save_shim_process_record(record)
    }

    pub fn transition_shim_state(
        &mut self,
        container_id: &str,
        next: ShimLedgerState,
    ) -> Result<()> {
        let record = self
            .persistence
            .get_shim_process_record(container_id)?
            .ok_or_else(|| anyhow::anyhow!("shim process {container_id} not found"))?;
        let current = ShimLedgerState::parse(&record.state)?;
        if !current.can_transition_to(next) {
            anyhow::bail!("invalid shim state transition: {current} -> {next}");
        }
        self.persistence
            .storage_mut()
            .update_shim_process_state(container_id, next.as_str())
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

    pub fn append_typed_event_at(
        &mut self,
        input: crate::storage::TypedEventInput<'_>,
    ) -> Result<()> {
        self.persistence.storage_mut().append_typed_event_at(input)
    }

    pub fn repair(
        &mut self,
        options: LedgerRepairOptions,
        dry_run: bool,
    ) -> Result<LedgerCheckReport> {
        let snapshot = StateLedger::new(self.persistence).recovery_snapshot()?;
        let mut plan = snapshot.check_plan(
            LedgerCheckOptions {
                check_files: options.check_files,
            },
            dry_run,
        );

        if dry_run {
            return Ok(plan.report);
        }

        let mut actions = Vec::with_capacity(plan.operations.len());
        for operation in plan.operations {
            actions.push(self.apply_repair_operation(operation)?);
        }
        plan.report.dry_run = false;
        plan.report.action_count = actions.len();
        plan.report.applied_action_count = actions.iter().filter(|action| action.applied).count();
        plan.report.actions = actions;
        Ok(plan.report)
    }

    fn apply_repair_operation(
        &mut self,
        operation: LedgerRepairOperation,
    ) -> Result<LedgerRepairAction> {
        match operation {
            LedgerRepairOperation::MarkSnapshotBroken { key } => {
                let mut applied = false;
                if let Some(record) = self
                    .persistence
                    .list_snapshot_records()?
                    .into_iter()
                    .find(|record| record.key == key)
                {
                    if record.state != SnapshotLedgerState::Broken.as_str() {
                        self.persistence
                            .storage_mut()
                            .update_snapshot_state(&key, SnapshotLedgerState::Broken.as_str())?;
                        applied = true;
                    }
                }
                Ok(LedgerRepairAction {
                    action: "markSnapshotBroken".to_string(),
                    subject_kind: "snapshot".to_string(),
                    subject_id: key.clone(),
                    dry_run: false,
                    applied,
                    message: format!("marked snapshot {key} broken"),
                })
            }
            LedgerRepairOperation::MarkRuntimeArtifactBroken {
                owner_kind,
                owner_id,
                artifact_kind,
                path,
            } => {
                let mut applied = false;
                if let Some(record) =
                    self.persistence
                        .list_runtime_artifacts()?
                        .into_iter()
                        .find(|record| {
                            record.owner_kind == owner_kind
                                && record.owner_id == owner_id
                                && record.artifact_kind == artifact_kind
                                && record.path == path
                        })
                {
                    if record.state != RuntimeArtifactLedgerState::Broken.as_str() {
                        self.persistence
                            .storage_mut()
                            .update_runtime_artifact_state(
                                &owner_kind,
                                &owner_id,
                                &artifact_kind,
                                &path,
                                RuntimeArtifactLedgerState::Broken.as_str(),
                            )?;
                        applied = true;
                    }
                }
                Ok(LedgerRepairAction {
                    action: "markRuntimeArtifactBroken".to_string(),
                    subject_kind: "runtimeArtifact".to_string(),
                    subject_id: path.clone(),
                    dry_run: false,
                    applied,
                    message: format!("marked {artifact_kind} runtime artifact {path} broken"),
                })
            }
            LedgerRepairOperation::MarkShimDead { container_id } => {
                let mut applied = false;
                if let Some(record) = self.persistence.get_shim_process_record(&container_id)? {
                    if record.state != ShimLedgerState::Dead.as_str() {
                        self.persistence.storage_mut().update_shim_process_state(
                            &container_id,
                            ShimLedgerState::Dead.as_str(),
                        )?;
                        applied = true;
                    }
                }
                Ok(LedgerRepairAction {
                    action: "markShimDead".to_string(),
                    subject_kind: "shim".to_string(),
                    subject_id: container_id.clone(),
                    dry_run: false,
                    applied,
                    message: format!("marked shim {container_id} dead"),
                })
            }
            LedgerRepairOperation::DeleteDanglingShim { container_id } => {
                let existed = self
                    .persistence
                    .get_shim_process_record(&container_id)?
                    .is_some();
                if existed {
                    self.persistence.delete_shim_process_record(&container_id)?;
                }
                Ok(LedgerRepairAction {
                    action: "deleteDanglingShim".to_string(),
                    subject_kind: "shim".to_string(),
                    subject_id: container_id.clone(),
                    dry_run: false,
                    applied: existed,
                    message: format!("deleted dangling shim record for {container_id}"),
                })
            }
        }
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
            snapshotter: "internal-overlay-untar".to_string(),
            runtime_managed: true,
        };
        let artifacts = vec![
            RuntimeArtifactRecord {
                owner_kind: "container".to_string(),
                owner_id: container.id.clone(),
                artifact_kind: "bundle".to_string(),
                path: "/run/crius/container-a/bundle".to_string(),
                state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
                runtime_handler: Some("runc".to_string()),
                runtime_root: Some("/run/runc".to_string()),
            },
            RuntimeArtifactRecord {
                owner_kind: "pod".to_string(),
                owner_id: pod.id.clone(),
                artifact_kind: "netns".to_string(),
                path: pod.netns_path.clone(),
                state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
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

        assert_eq!(ledger.schema_version().unwrap(), 1);
        assert_eq!(
            ledger
                .latest_schema_migration()
                .unwrap()
                .unwrap()
                .migration_name,
            "baseline-current-ledger-schema"
        );
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

    #[test]
    fn state_ledger_exposes_content_gc_blockers() {
        let temp_dir = tempdir().unwrap();
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: temp_dir.path().join("content-gc.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();
        {
            let storage = persistence.storage_mut();
            storage
                .save_content_blob(&crate::storage::ContentBlobRecord {
                    digest: "sha256:layer".to_string(),
                    media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                    size: 100,
                    relative_path: "blobs/sha256/la/yer".to_string(),
                    created_at: 1,
                    last_used_at: 1,
                })
                .unwrap();
            storage
                .replace_content_blob_refs(
                    "snapshot",
                    "snapshot-a",
                    &[crate::storage::ContentBlobRefRecord {
                        owner_kind: "snapshot".to_string(),
                        owner_id: "snapshot-a".to_string(),
                        digest: "sha256:layer".to_string(),
                        ref_kind: "lower".to_string(),
                    }],
                )
                .unwrap();
        }

        let ledger = StateLedger::new(&persistence);
        let candidates = ledger.content_gc_candidates().unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].blob.digest, "sha256:layer");
        assert_eq!(candidates[0].blockers.len(), 1);
    }

    #[test]
    fn ledger_state_transitions_are_validated() {
        let temp_dir = tempdir().unwrap();
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: temp_dir.path().join("states.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();

        let artifact_path = "/run/crius/container-state/bundle";
        {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger
                .save_snapshot(&SnapshotRecord {
                    key: "snapshot-state".to_string(),
                    image_id: "sha256:image".to_string(),
                    owner_kind: "container".to_string(),
                    owner_id: "container-state".to_string(),
                    state: SnapshotLedgerState::Prepared.as_str().to_string(),
                    mountpoint: "/run/crius/snapshots/snapshot-state".to_string(),
                    snapshotter: "internal-overlay-untar".to_string(),
                    runtime_managed: true,
                })
                .unwrap();
            ledger
                .replace_runtime_artifacts(
                    "container",
                    "container-state",
                    &[RuntimeArtifactRecord {
                        owner_kind: "container".to_string(),
                        owner_id: "container-state".to_string(),
                        artifact_kind: "bundle".to_string(),
                        path: artifact_path.to_string(),
                        state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
                        runtime_handler: Some("runc".to_string()),
                        runtime_root: Some("/run/runc".to_string()),
                    }],
                )
                .unwrap();
            ledger
                .save_shim_process(&ShimProcessRecord {
                    container_id: "container-state".to_string(),
                    shim_pid: 123,
                    work_dir: "/run/crius/shims".to_string(),
                    socket_path: "/run/crius/shims/container-state/task.sock".to_string(),
                    exit_code_file: "/run/crius/exits/container-state".to_string(),
                    log_file: "/run/crius/shims/container-state/shim.log".to_string(),
                    bundle_path: artifact_path.to_string(),
                    state: ShimLedgerState::Running.as_str().to_string(),
                    last_seen_at: 1,
                })
                .unwrap();

            ledger
                .transition_snapshot_state("snapshot-state", SnapshotLedgerState::Mounted)
                .unwrap();
            assert!(ledger
                .transition_snapshot_state("snapshot-state", SnapshotLedgerState::Prepared)
                .is_err());

            ledger
                .transition_runtime_artifact_state(
                    "container",
                    "container-state",
                    "bundle",
                    artifact_path,
                    RuntimeArtifactLedgerState::Stale,
                )
                .unwrap();
            assert!(ledger
                .transition_runtime_artifact_state(
                    "container",
                    "container-state",
                    "bundle",
                    artifact_path,
                    RuntimeArtifactLedgerState::Planned,
                )
                .is_err());

            ledger
                .transition_shim_state("container-state", ShimLedgerState::Dead)
                .unwrap();
            assert!(ledger
                .transition_shim_state("container-state", ShimLedgerState::Exited)
                .is_err());
        }

        let snapshot = StateLedger::new(&persistence).recovery_snapshot().unwrap();
        assert_eq!(snapshot.snapshots[0].state, "mounted");
        assert_eq!(snapshot.runtime_artifacts[0].state, "stale");
        assert_eq!(snapshot.shim_processes[0].state, "dead");
    }

    #[test]
    fn ledger_writer_rejects_unknown_object_states() {
        let temp_dir = tempdir().unwrap();
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: temp_dir.path().join("invalid-states.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();
        let mut ledger = StateLedgerWriter::new(&mut persistence);

        assert!(ledger
            .save_snapshot(&SnapshotRecord {
                key: "bad-snapshot".to_string(),
                image_id: "sha256:image".to_string(),
                owner_kind: "container".to_string(),
                owner_id: "container-state".to_string(),
                state: "half-mounted".to_string(),
                mountpoint: "/run/crius/snapshots/bad-snapshot".to_string(),
                snapshotter: "internal-overlay-untar".to_string(),
                runtime_managed: true,
            })
            .is_err());
        assert!(ledger
            .replace_runtime_artifacts(
                "container",
                "container-state",
                &[RuntimeArtifactRecord {
                    owner_kind: "container".to_string(),
                    owner_id: "container-state".to_string(),
                    artifact_kind: "bundle".to_string(),
                    path: "/run/crius/container-state/bundle".to_string(),
                    state: "warm".to_string(),
                    runtime_handler: None,
                    runtime_root: None,
                }],
            )
            .is_err());
        assert!(ledger
            .save_shim_process(&ShimProcessRecord {
                container_id: "container-state".to_string(),
                shim_pid: 123,
                work_dir: "/run/crius/shims".to_string(),
                socket_path: "/run/crius/shims/container-state/task.sock".to_string(),
                exit_code_file: "/run/crius/exits/container-state".to_string(),
                log_file: "/run/crius/shims/container-state/shim.log".to_string(),
                bundle_path: "/run/crius/container-state/bundle".to_string(),
                state: "awake".to_string(),
                last_seen_at: 1,
            })
            .is_err());
    }

    #[test]
    fn ledger_check_reports_broken_graph_missing_artifact_and_dangling_shim() {
        let temp_dir = tempdir().unwrap();
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: temp_dir.path().join("check.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();

        {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger
                .save_container(&ContainerRecord {
                    id: "container-check".to_string(),
                    pod_id: "missing-pod".to_string(),
                    state: "running".to_string(),
                    image: "busybox:latest".to_string(),
                    command: "sleep 60".to_string(),
                    created_at: 1,
                    labels: "{}".to_string(),
                    annotations: "{}".to_string(),
                    exit_code: None,
                    exit_time: None,
                    runtime_handler: Some("runc".to_string()),
                    runtime_backend: Some("process".to_string()),
                    snapshot_key: Some("missing-snapshot".to_string()),
                })
                .unwrap();
            ledger
                .replace_runtime_artifacts(
                    "container",
                    "container-check",
                    &[RuntimeArtifactRecord {
                        owner_kind: "container".to_string(),
                        owner_id: "container-check".to_string(),
                        artifact_kind: "bundle".to_string(),
                        path: temp_dir.path().join("missing-bundle").display().to_string(),
                        state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
                        runtime_handler: Some("runc".to_string()),
                        runtime_root: Some(temp_dir.path().display().to_string()),
                    }],
                )
                .unwrap();
            ledger
                .save_shim_process(&ShimProcessRecord {
                    container_id: "dangling-shim".to_string(),
                    shim_pid: 123,
                    work_dir: temp_dir.path().join("shims").display().to_string(),
                    socket_path: temp_dir.path().join("shim.sock").display().to_string(),
                    exit_code_file: temp_dir.path().join("exit").display().to_string(),
                    log_file: temp_dir.path().join("shim.log").display().to_string(),
                    bundle_path: temp_dir.path().join("bundle").display().to_string(),
                    state: ShimLedgerState::Running.as_str().to_string(),
                    last_seen_at: 1,
                })
                .unwrap();
        }

        let report = StateLedger::new(&persistence)
            .check(LedgerCheckOptions::default())
            .unwrap();

        assert!(report.dry_run);
        assert!(report.issue_count >= 4);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.kind == "ownerGraphBroken" && issue.subject_kind == "container"));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.kind == "danglingRef" && issue.subject_kind == "container"));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.kind == "missingArtifact"
                && issue.subject_kind == "runtimeArtifact"
                && issue.repairable));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.kind == "ownerGraphBroken"
                && issue.subject_kind == "shim"
                && issue.repairable));
        assert!(report
            .actions
            .iter()
            .any(|action| action.action == "markRuntimeArtifactBroken" && action.dry_run));
        assert!(report
            .actions
            .iter()
            .any(|action| action.action == "deleteDanglingShim" && action.dry_run));
    }

    #[test]
    fn ledger_repair_dry_run_does_not_mutate_but_repair_applies_safe_actions() {
        let temp_dir = tempdir().unwrap();
        let mut persistence = PersistenceManager::new(PersistenceConfig {
            db_path: temp_dir.path().join("repair.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap();
        let missing_artifact = temp_dir.path().join("missing-artifact");

        {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger
                .save_snapshot(&SnapshotRecord {
                    key: "snapshot-repair".to_string(),
                    image_id: "missing-image".to_string(),
                    owner_kind: "container".to_string(),
                    owner_id: "container-repair".to_string(),
                    state: SnapshotLedgerState::Mounted.as_str().to_string(),
                    mountpoint: temp_dir.path().join("rootfs").display().to_string(),
                    snapshotter: "internal-overlay-untar".to_string(),
                    runtime_managed: true,
                })
                .unwrap();
            ledger
                .replace_runtime_artifacts(
                    "container",
                    "container-repair",
                    &[RuntimeArtifactRecord {
                        owner_kind: "container".to_string(),
                        owner_id: "container-repair".to_string(),
                        artifact_kind: "bundle".to_string(),
                        path: missing_artifact.display().to_string(),
                        state: RuntimeArtifactLedgerState::Active.as_str().to_string(),
                        runtime_handler: Some("runc".to_string()),
                        runtime_root: Some(temp_dir.path().display().to_string()),
                    }],
                )
                .unwrap();
            ledger
                .save_shim_process(&ShimProcessRecord {
                    container_id: "dangling-shim".to_string(),
                    shim_pid: 123,
                    work_dir: temp_dir.path().join("shims").display().to_string(),
                    socket_path: temp_dir.path().join("shim.sock").display().to_string(),
                    exit_code_file: temp_dir.path().join("exit").display().to_string(),
                    log_file: temp_dir.path().join("shim.log").display().to_string(),
                    bundle_path: temp_dir.path().join("bundle").display().to_string(),
                    state: ShimLedgerState::Running.as_str().to_string(),
                    last_seen_at: 1,
                })
                .unwrap();
        }

        let dry_run = {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger.repair(LedgerRepairOptions::default(), true).unwrap()
        };
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.applied_action_count, 0);
        assert_eq!(
            StateLedger::new(&persistence)
                .snapshots()
                .unwrap()
                .into_iter()
                .find(|snapshot| snapshot.key == "snapshot-repair")
                .unwrap()
                .state,
            SnapshotLedgerState::Mounted.as_str()
        );
        assert!(StateLedger::new(&persistence)
            .shim_process("dangling-shim")
            .unwrap()
            .is_some());

        let repair = {
            let mut ledger = StateLedgerWriter::new(&mut persistence);
            ledger
                .repair(LedgerRepairOptions::default(), false)
                .unwrap()
        };

        assert!(!repair.dry_run);
        assert_eq!(repair.action_count, repair.applied_action_count);
        assert!(repair.applied_action_count >= 3);
        let snapshot = StateLedger::new(&persistence)
            .snapshots()
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.key == "snapshot-repair")
            .unwrap();
        assert_eq!(snapshot.state, SnapshotLedgerState::Broken.as_str());
        let artifact = StateLedger::new(&persistence)
            .runtime_artifacts()
            .unwrap()
            .into_iter()
            .find(|artifact| artifact.path == missing_artifact.display().to_string())
            .unwrap();
        assert_eq!(artifact.state, RuntimeArtifactLedgerState::Broken.as_str());
        assert!(StateLedger::new(&persistence)
            .shim_process("dangling-shim")
            .unwrap()
            .is_none());
    }
}
