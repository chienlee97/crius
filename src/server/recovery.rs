use super::service::{
    RecoveryCleanupCounter, RecoveryOrphanCleanupSummary, RecoveryReconcileSummary,
    RecoveryResultSummary, RecoveryStage,
};
use super::*;
use crate::services::{InternalEvent, InternalEventSeverity};
use crate::state::{
    RecoveryContainerEntry, RecoveryLedgerSnapshot, RuntimeArtifactLedgerState, ShimLedgerState,
    SnapshotLedgerState,
};
use crate::storage::{ContainerRecord, PodSandboxRecord};
use std::time::Instant;

#[derive(Default)]
struct RecoveryRuntimeLiveState {
    container_statuses: HashMap<String, ContainerStatus>,
    pause_statuses: HashMap<String, ContainerStatus>,
}

#[derive(Default)]
struct RecoveryShimReconnectResult {
    reconnected_shims: Vec<String>,
    broken_containers: HashMap<String, StoredBrokenState>,
}

impl RuntimeServiceImpl {
    fn broken_state(kind: impl Into<String>, details: impl Into<String>) -> StoredBrokenState {
        StoredBrokenState {
            kind: kind.into(),
            details: details.into(),
            detected_at: Self::now_nanos(),
        }
    }

    async fn mark_container_broken(
        &self,
        container_id: &str,
        broken: StoredBrokenState,
    ) -> Result<(), Status> {
        let updated_annotations = {
            let mut containers = self.containers.lock().await;
            let Some(container) = containers.get_mut(container_id) else {
                return Ok(());
            };
            let mut state = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            )
            .unwrap_or_default();
            state.broken = Some(broken.clone());
            Self::insert_internal_state(
                &mut container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
                &state,
            )?;
            container.annotations.clone()
        };

        if let Err(err) = self
            .persistence
            .lock()
            .await
            .update_container_annotations(container_id, &updated_annotations)
        {
            log::warn!(
                "Failed to persist broken-state annotations for container {}: {}",
                container_id,
                err
            );
        }

        self.publish_reconcile_event(InternalEvent::new(
            "reconcile.container_broken",
            "container",
            container_id,
            InternalEventSeverity::Warning,
            serde_json::json!({
                "state": "broken",
                "kind": broken.kind,
                "details": broken.details,
                "detectedAt": broken.detected_at,
            }),
        ))
        .await;

        Ok(())
    }

    async fn clear_container_broken(&self, container_id: &str) -> Result<(), Status> {
        let mut should_persist = None;
        {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(container_id) {
                if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                    &container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                ) {
                    if state.broken.take().is_some() {
                        Self::insert_internal_state(
                            &mut container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        )?;
                        should_persist = Some(container.annotations.clone());
                    }
                }
            }
        }
        if let Some(annotations) = should_persist {
            if let Err(err) = self
                .persistence
                .lock()
                .await
                .update_container_annotations(container_id, &annotations)
            {
                log::warn!(
                    "Failed to clear broken-state annotations for container {}: {}",
                    container_id,
                    err
                );
            }
        }
        Ok(())
    }

    async fn mark_runtime_artifact_broken(
        &self,
        owner_kind: &str,
        owner_id: &str,
        artifact_kind: &str,
        path: &str,
    ) {
        if let Err(err) = self
            .persistence
            .lock()
            .await
            .storage_mut()
            .update_runtime_artifact_state(
                owner_kind,
                owner_id,
                artifact_kind,
                path,
                RuntimeArtifactLedgerState::Broken.as_str(),
            )
        {
            log::warn!(
                "Failed to mark runtime artifact {owner_kind}/{owner_id}/{artifact_kind} at {path} broken: {err}"
            );
        }
    }

    async fn mark_snapshot_broken(&self, key: &str) {
        if let Err(err) = self
            .persistence
            .lock()
            .await
            .storage_mut()
            .update_snapshot_state(key, SnapshotLedgerState::Broken.as_str())
        {
            log::warn!("Failed to mark snapshot {key} broken: {err}");
        }
    }

    async fn mark_shim_process_state(
        &self,
        container_id: &str,
        old_state: &str,
        next_state: ShimLedgerState,
        details: &str,
    ) {
        let mut persistence = self.persistence.lock().await;
        if let Err(err) = persistence
            .storage_mut()
            .update_shim_process_state(container_id, next_state.as_str())
        {
            log::warn!("Failed to mark shim process {container_id} {next_state}: {err}");
            return;
        }

        drop(persistence);

        self.publish_reconcile_event(InternalEvent::new(
            "shim.state",
            "shim",
            container_id,
            InternalEventSeverity::Warning,
            serde_json::json!({
                "previousState": old_state,
                "state": next_state.as_str(),
                "details": details,
            }),
        ))
        .await;
    }

    async fn mark_pod_broken(&self, pod_id: &str, broken: StoredBrokenState) -> Result<(), Status> {
        let updated_annotations = {
            let mut pods = self.pod_sandboxes.lock().await;
            let Some(pod) = pods.get_mut(pod_id) else {
                return Ok(());
            };
            let mut state = Self::read_internal_state::<StoredPodState>(
                &pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .unwrap_or_default();
            state.broken = Some(broken);
            Self::insert_internal_state(&mut pod.annotations, INTERNAL_POD_STATE_KEY, &state)?;
            pod.annotations.clone()
        };
        if let Err(err) = self
            .persistence
            .lock()
            .await
            .update_pod_annotations(pod_id, &updated_annotations)
        {
            log::warn!(
                "Failed to persist broken-state annotations for pod {}: {}",
                pod_id,
                err
            );
        }
        Ok(())
    }

    async fn clear_pod_broken(&self, pod_id: &str) -> Result<(), Status> {
        let mut should_persist = None;
        {
            let mut pods = self.pod_sandboxes.lock().await;
            if let Some(pod) = pods.get_mut(pod_id) {
                if let Some(mut state) = Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                ) {
                    if state.broken.take().is_some() {
                        Self::insert_internal_state(
                            &mut pod.annotations,
                            INTERNAL_POD_STATE_KEY,
                            &state,
                        )?;
                        should_persist = Some(pod.annotations.clone());
                    }
                }
            }
        }
        if let Some(annotations) = should_persist {
            if let Err(err) = self
                .persistence
                .lock()
                .await
                .update_pod_annotations(pod_id, &annotations)
            {
                log::warn!(
                    "Failed to clear broken-state annotations for pod {}: {}",
                    pod_id,
                    err
                );
            }
        }
        Ok(())
    }

    async fn load_recovery_ledger_snapshot(&self) -> Result<RecoveryLedgerSnapshot, Status> {
        let persistence = self.persistence.lock().await;
        RecoveryLedgerSnapshot::load(&persistence).map_err(|err| {
            Status::internal(format!("Failed to load recovery ledger snapshot: {}", err))
        })
    }

    async fn prune_recovered_logical_duplicates(&self) -> Result<(), Status> {
        let pod_snapshots: Vec<crate::proto::runtime::v1::PodSandbox> = {
            let pods = self.pod_sandboxes.lock().await;
            pods.values().cloned().collect()
        };
        let mut best_pods = HashMap::new();
        let mut pod_losers = Vec::new();
        for mut pod in pod_snapshots {
            pod.state = self.live_pod_sandbox_state(&pod).await;
            let Some(key) = Self::sandbox_identity_key(&pod) else {
                continue;
            };
            match best_pods.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if Self::sandbox_rank(&pod) > Self::sandbox_rank(entry.get()) {
                        pod_losers.push(entry.insert(pod).id.clone());
                    } else {
                        pod_losers.push(pod.id.clone());
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(pod);
                }
            }
        }
        if !pod_losers.is_empty() {
            for pod_id in &pod_losers {
                self.cleanup_stale_logical_pod_sandbox(pod_id).await?;
            }
        }

        let container_snapshots: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers.values().cloned().collect()
        };
        let mut best_containers = HashMap::new();
        let mut container_losers = Vec::new();
        for mut container in container_snapshots {
            container.state = Self::map_runtime_container_state(
                self.runtime_container_status_checked(&container.id).await,
            );
            let Some(key) = Self::container_identity_key(&container) else {
                continue;
            };
            match best_containers.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if Self::container_rank(&container) > Self::container_rank(entry.get()) {
                        container_losers.push(entry.insert(container).id.clone());
                    } else {
                        container_losers.push(container.id.clone());
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(container);
                }
            }
        }
        if !container_losers.is_empty() {
            for container_id in &container_losers {
                self.cleanup_stale_logical_container(container_id).await?;
            }
        }

        Ok(())
    }

    pub(crate) fn pause_process_root_matches_runtime_pod_root(
        root_link: &Path,
        pod_root: &Path,
    ) -> bool {
        root_link.starts_with(pod_root)
    }

    async fn cleanup_orphaned_pause_processes(&self) -> RecoveryCleanupCounter {
        let mut summary = RecoveryCleanupCounter::default();
        let pod_root = self.config.root_dir.join("pods");
        let known_pause_pids: std::collections::HashSet<u32> = {
            let pause_ids: Vec<String> = self
                .pod_sandboxes
                .lock()
                .await
                .values()
                .filter_map(|pod| {
                    Self::read_internal_state::<StoredPodState>(
                        &pod.annotations,
                        INTERNAL_POD_STATE_KEY,
                    )
                    .and_then(|state| state.pause_container_id)
                })
                .collect();
            let mut pids = std::collections::HashSet::new();
            for pause_id in pause_ids {
                if let Some(pid) = self.runtime_container_pid_checked(&pause_id).await {
                    pids.insert(pid as u32);
                }
            }
            pids
        };

        let Ok(entries) = std::fs::read_dir("/proc") else {
            return summary;
        };

        for entry in entries.flatten() {
            let Ok(file_name) = entry.file_name().into_string() else {
                continue;
            };
            let Ok(pid) = file_name.parse::<u32>() else {
                continue;
            };
            if known_pause_pids.contains(&pid) {
                continue;
            }

            let proc_path = entry.path();
            let Ok(comm) = std::fs::read_to_string(proc_path.join("comm")) else {
                continue;
            };
            if comm.trim() != "pause" {
                continue;
            }

            let Ok(root_link) = std::fs::read_link(proc_path.join("root")) else {
                continue;
            };
            if !Self::pause_process_root_matches_runtime_pod_root(&root_link, &pod_root) {
                continue;
            }

            #[cfg(unix)]
            {
                use nix::sys::signal::{self, Signal};
                use nix::unistd::Pid;

                match signal::kill(Pid::from_raw(pid as i32), Signal::SIGKILL) {
                    Ok(()) => {
                        summary.removed += 1;
                        log::warn!(
                            "Killed orphaned pause process {} rooted at {}",
                            pid,
                            root_link.display()
                        );
                    }
                    Err(err) => {
                        summary.failures += 1;
                        log::warn!(
                            "Failed to kill orphaned pause process {} rooted at {}: {}",
                            pid,
                            root_link.display(),
                            err
                        );
                    }
                }
            }
        }
        summary
    }

    pub(super) async fn reconcile_recovered_state(&self) -> Result<(), Status> {
        let container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers.keys().cloned().collect()
        };

        for container_id in container_ids {
            let runtime_status = self.runtime_container_status_checked(&container_id).await;
            if matches!(runtime_status, ContainerStatus::Unknown) {
                continue;
            }

            let runtime_state = Self::map_runtime_container_state(runtime_status.clone());
            let persistence_status = match runtime_status {
                ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
                ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
                ContainerStatus::Stopped(code) => crate::runtime::ContainerStatus::Stopped(code),
                ContainerStatus::Unknown => crate::runtime::ContainerStatus::Unknown,
            };

            {
                let mut containers = self.containers.lock().await;
                if let Some(container) = containers.get_mut(&container_id) {
                    container.state = runtime_state;
                    if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    ) {
                        match &persistence_status {
                            crate::runtime::ContainerStatus::Running => {
                                state.started_at.get_or_insert(Self::now_nanos());
                                state.finished_at = None;
                                state.exit_code = None;
                            }
                            crate::runtime::ContainerStatus::Stopped(code) => {
                                state.finished_at.get_or_insert(Self::now_nanos());
                                state.exit_code = Some(*code);
                            }
                            _ => {}
                        }
                        let _ = Self::insert_internal_state(
                            &mut container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        );
                    }
                }
            }

            let mut persistence = self.persistence.lock().await;
            if let Err(e) = persistence.update_container_state(&container_id, persistence_status) {
                log::warn!(
                    "Failed to reconcile container {} state in database: {}",
                    container_id,
                    e
                );
            }
        }

        let pod_ids: Vec<String> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.keys().cloned().collect()
        };

        for pod_id in pod_ids {
            let pause_container_id = {
                let pod_sandboxes = self.pod_sandboxes.lock().await;
                pod_sandboxes.get(&pod_id).and_then(|pod| {
                    Self::read_internal_state::<StoredPodState>(
                        &pod.annotations,
                        INTERNAL_POD_STATE_KEY,
                    )
                    .and_then(|state| state.pause_container_id)
                })
            };

            let Some(pause_container_id) = pause_container_id else {
                continue;
            };

            let pause_status = self
                .runtime_container_status_checked(&pause_container_id)
                .await;
            let pause_has_active_runtime_state = self
                .runtime
                .container_has_active_runtime_state(&pause_container_id)
                || self
                    .runtime_container_pid_checked(&pause_container_id)
                    .await
                    .is_some();
            let pod_has_required_netns = {
                let pod_sandboxes = self.pod_sandboxes.lock().await;
                pod_sandboxes
                    .get(&pod_id)
                    .map(Self::pod_has_required_netns)
                    .unwrap_or(true)
            };
            if matches!(pause_status, ContainerStatus::Running) && !pod_has_required_netns {
                log::warn!(
                    "Recovered pod {} pause container is running but network namespace is missing; marking sandbox NotReady",
                    pod_id
                );
            }
            let current_state = {
                let pod_sandboxes = self.pod_sandboxes.lock().await;
                pod_sandboxes
                    .get(&pod_id)
                    .map(|pod| pod.state)
                    .unwrap_or(PodSandboxState::SandboxNotready as i32)
            };
            let next_state = match pause_status {
                ContainerStatus::Running if pod_has_required_netns => {
                    PodSandboxState::SandboxReady as i32
                }
                ContainerStatus::Unknown if pause_has_active_runtime_state => current_state,
                _ => PodSandboxState::SandboxNotready as i32,
            };

            {
                let mut pod_sandboxes = self.pod_sandboxes.lock().await;
                if let Some(pod) = pod_sandboxes.get_mut(&pod_id) {
                    pod.state = next_state;
                }
            }

            let mut persistence = self.persistence.lock().await;
            if let Err(e) = persistence.update_pod_state(
                &pod_id,
                if next_state == PodSandboxState::SandboxReady as i32 {
                    "ready"
                } else {
                    "notready"
                },
            ) {
                log::warn!(
                    "Failed to reconcile pod {} state in database: {}",
                    pod_id,
                    e
                );
            }
        }

        Ok(())
    }

    pub(super) async fn best_effort_refresh_runtime_state(&self) {
        if let Err(err) = self.reconcile_recovered_state().await {
            log::warn!("Best-effort runtime state refresh failed: {}", err);
        }
    }

    async fn probe_runtime_live_state(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
    ) -> RecoveryRuntimeLiveState {
        let mut live_state = RecoveryRuntimeLiveState::default();

        for RecoveryContainerEntry { record, .. } in &snapshot.containers {
            live_state.container_statuses.insert(
                record.id.clone(),
                self.runtime_container_status_checked(&record.id).await,
            );
        }

        for pod in &snapshot.pods {
            let pod_state = Self::read_internal_state::<StoredPodState>(
                &serde_json::from_str::<HashMap<String, String>>(&pod.annotations)
                    .unwrap_or_default(),
                INTERNAL_POD_STATE_KEY,
            )
            .unwrap_or_default();
            if let Some(pause_container_id) = pod_state.pause_container_id {
                let status = self
                    .runtime_container_status_checked(&pause_container_id)
                    .await;
                live_state.pause_statuses.insert(pause_container_id, status);
            }
        }

        live_state
    }

    async fn reconnect_shims_from_ledger(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
    ) -> RecoveryShimReconnectResult {
        let mut result = RecoveryShimReconnectResult::default();

        for shim_record in &snapshot.shim_processes {
            let container_id = shim_record.container_id.as_str();
            let shim_pid_live = PathBuf::from("/proc")
                .join(shim_record.shim_pid.to_string())
                .exists();
            let shim_socket_missing = !Path::new(&shim_record.socket_path).exists();
            if shim_socket_missing && !shim_pid_live {
                let broken = self
                    .mark_shim_socket_missing(container_id, &shim_record.state)
                    .await;
                result
                    .broken_containers
                    .insert(container_id.to_string(), broken);
                continue;
            }

            match self.runtime.shim_status(container_id) {
                Ok(Some(status))
                    if matches!(
                        status.state,
                        crate::shim_rpc::TaskState::Created
                            | crate::shim_rpc::TaskState::Running
                            | crate::shim_rpc::TaskState::Paused
                    ) =>
                {
                    self.publish_shim_reconnect_event(
                        container_id,
                        "shim.reconnect",
                        InternalEventSeverity::Info,
                        "task service reported active task state",
                        serde_json::json!({
                            "taskState": format!("{:?}", status.state),
                            "pid": status.pid,
                        }),
                    )
                    .await;
                    result.reconnected_shims.push(container_id.to_string());
                }
                Ok(None) if shim_pid_live => {
                    self.publish_shim_reconnect_event(
                        container_id,
                        "shim.reconnect",
                        InternalEventSeverity::Info,
                        "shim process is live without task status",
                        serde_json::json!({
                            "shimPid": shim_record.shim_pid,
                            "socketPath": shim_record.socket_path,
                        }),
                    )
                    .await;
                    result.reconnected_shims.push(container_id.to_string());
                }
                Ok(Some(status)) => {
                    self.publish_shim_reconnect_event(
                        container_id,
                        "shim.reconnect",
                        InternalEventSeverity::Info,
                        "task service responded with terminal task state",
                        serde_json::json!({
                            "taskState": format!("{:?}", status.state),
                            "pid": status.pid,
                        }),
                    )
                    .await;
                    result.reconnected_shims.push(container_id.to_string());
                }
                Ok(None) => {
                    let broken = self
                        .mark_shim_socket_missing(container_id, &shim_record.state)
                        .await;
                    result
                        .broken_containers
                        .insert(container_id.to_string(), broken);
                }
                Err(err) => {
                    let details = format!("failed to query shim live state: {}", err);
                    self.mark_shim_process_state(
                        container_id,
                        &shim_record.state,
                        ShimLedgerState::Degraded,
                        &details,
                    )
                    .await;
                    result.broken_containers.insert(
                        container_id.to_string(),
                        Self::broken_state("shim_reconcile_failed", details.clone()),
                    );
                    self.publish_shim_reconnect_event(
                        container_id,
                        "shim.reconnect_failed",
                        InternalEventSeverity::Warning,
                        "failed to query shim live state",
                        serde_json::json!({
                            "state": "degraded",
                            "details": details,
                        }),
                    )
                    .await;
                }
            }
        }

        result.reconnected_shims.sort();
        result.reconnected_shims.dedup();
        result
    }

    async fn publish_shim_reconnect_event(
        &self,
        container_id: &str,
        kind: &str,
        severity: InternalEventSeverity,
        reason: &str,
        mut details: serde_json::Value,
    ) {
        details["reason"] = serde_json::Value::String(reason.to_string());
        self.publish_reconcile_event(InternalEvent::new(
            kind,
            "shim",
            container_id,
            severity,
            details,
        ))
        .await;
    }

    async fn mark_shim_socket_missing(
        &self,
        container_id: &str,
        old_state: &str,
    ) -> StoredBrokenState {
        let details = "shim ledger entry exists but task socket is unavailable";
        self.mark_shim_process_state(container_id, old_state, ShimLedgerState::Dead, details)
            .await;
        Self::broken_state(
            "shim_socket_missing",
            format!(
                "shim ledger entry exists for {} but task socket is unavailable",
                container_id
            ),
        )
    }

    async fn detect_recovered_container_artifact_breakage(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
        record: &ContainerRecord,
    ) -> Option<StoredBrokenState> {
        let container_id = record.id.as_str();
        let runtime = record
            .runtime_handler
            .as_deref()
            .and_then(|handler| self.runtime.runtime_for_handler(handler).ok())
            .or_else(|| self.runtime.runtime_for_container(container_id).ok());

        if let Some(runtime) = runtime.as_ref() {
            let bundle_path = runtime.runtime_context().bundle_path_for(container_id);
            if !bundle_path.exists() {
                let bundle_path = bundle_path.display().to_string();
                self.mark_runtime_artifact_broken(
                    "container",
                    container_id,
                    "bundle",
                    &bundle_path,
                )
                .await;
                return Some(Self::broken_state(
                    "bundle_missing",
                    format!("runtime bundle {bundle_path} is missing"),
                ));
            }
        }

        if let Some(rootfs_artifact) = snapshot.runtime_artifacts.iter().find(|artifact| {
            artifact.owner_kind == "container"
                && artifact.owner_id == container_id
                && artifact.artifact_kind == "rootfs"
        }) {
            let rootfs_path = Path::new(&rootfs_artifact.path);
            if !rootfs_path.exists() {
                self.mark_runtime_artifact_broken(
                    "container",
                    container_id,
                    "rootfs",
                    &rootfs_artifact.path,
                )
                .await;
                if let Some(snapshot_key) = record.snapshot_key.as_deref() {
                    self.mark_snapshot_broken(snapshot_key).await;
                }
                return Some(Self::broken_state(
                    "rootfs_missing",
                    format!("rootfs artifact {} is missing", rootfs_path.display()),
                ));
            }
        }

        None
    }

    async fn reconcile_recovered_container_from_ledger(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
        record: &ContainerRecord,
        runtime_status: ContainerStatus,
        shim_reconnect: &RecoveryShimReconnectResult,
    ) -> Result<bool, Status> {
        let container_id = record.id.as_str();
        let broken = self
            .detect_recovered_container_artifact_breakage(snapshot, record)
            .await
            .or_else(|| shim_reconnect.broken_containers.get(container_id).cloned());

        let detected_broken = broken.clone();
        if let Some(broken) = broken {
            self.mark_container_broken(container_id, broken).await?;
        } else {
            self.clear_container_broken(container_id).await?;
        }

        let runtime_state = Self::map_runtime_container_state(runtime_status.clone());
        let persistence_status = match runtime_status {
            ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
            ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
            ContainerStatus::Stopped(code) => crate::runtime::ContainerStatus::Stopped(code),
            ContainerStatus::Unknown => crate::runtime::ContainerStatus::Unknown,
        };

        let updated_annotations = {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(container_id) {
                container.state = runtime_state;
                let mut state = Self::read_internal_state::<StoredContainerState>(
                    &container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                )
                .unwrap_or_default();
                state.broken = detected_broken.clone();
                match &persistence_status {
                    crate::runtime::ContainerStatus::Running => {
                        state.started_at.get_or_insert(Self::now_nanos());
                        state.finished_at = None;
                        state.exit_code = None;
                    }
                    crate::runtime::ContainerStatus::Stopped(code) => {
                        state.finished_at.get_or_insert(Self::now_nanos());
                        state.exit_code = Some(*code);
                    }
                    _ => {}
                }
                Self::insert_internal_state(
                    &mut container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                )?;
                Some(container.annotations.clone())
            } else {
                None
            }
        };
        let mut persistence = self.persistence.lock().await;
        if let Some(annotations) = updated_annotations {
            if let Err(e) = persistence.update_container_annotations(container_id, &annotations) {
                log::warn!(
                    "Failed to persist ledger-driven annotations for container {}: {}",
                    container_id,
                    e
                );
            }
        }
        if let Err(e) = persistence.update_container_state(container_id, persistence_status) {
            log::warn!(
                "Failed to reconcile ledger-driven container {} state in database: {}",
                container_id,
                e
            );
        }

        Ok(detected_broken.is_some())
    }

    async fn reconcile_recovered_pod_from_ledger(
        &self,
        pod: &PodSandboxRecord,
        live_state: &RecoveryRuntimeLiveState,
    ) -> Result<bool, Status> {
        let pod_id = pod.id.as_str();
        let pod_state = Self::read_internal_state::<StoredPodState>(
            &serde_json::from_str::<HashMap<String, String>>(&pod.annotations).unwrap_or_default(),
            INTERNAL_POD_STATE_KEY,
        )
        .unwrap_or_default();
        let pause_container_id = pod_state.pause_container_id.clone();
        let has_netns = !pod.netns_path.trim().is_empty() && Path::new(&pod.netns_path).exists();

        if !has_netns {
            self.mark_pod_broken(
                pod_id,
                Self::broken_state(
                    "netns_missing",
                    format!("pod network namespace {} is missing", pod.netns_path),
                ),
            )
            .await?;
        } else {
            self.clear_pod_broken(pod_id).await?;
        }

        if let Some(pause_container_id) = pause_container_id {
            let pause_status = live_state
                .pause_statuses
                .get(&pause_container_id)
                .cloned()
                .unwrap_or(ContainerStatus::Unknown);
            let next_state = match pause_status {
                ContainerStatus::Running if has_netns => "ready",
                ContainerStatus::Unknown if has_netns => pod.state.as_str(),
                _ => "notready",
            };
            if let Err(err) = self
                .persistence
                .lock()
                .await
                .update_pod_state(pod_id, next_state)
            {
                log::warn!(
                    "Failed to reconcile ledger-driven pod {} state in database: {}",
                    pod_id,
                    err
                );
            }
            {
                let mut pods = self.pod_sandboxes.lock().await;
                if let Some(pod) = pods.get_mut(pod_id) {
                    pod.state = if next_state == "ready" {
                        PodSandboxState::SandboxReady as i32
                    } else {
                        PodSandboxState::SandboxNotready as i32
                    };
                }
            }
        }

        Ok(!has_netns)
    }

    async fn reconcile_recovered_state_from_ledger(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
        live_state: &RecoveryRuntimeLiveState,
        shim_reconnect: &RecoveryShimReconnectResult,
    ) -> Result<RecoveryReconcileSummary, Status> {
        let mut summary = RecoveryReconcileSummary {
            reconnected_shims: shim_reconnect.reconnected_shims.clone(),
            ..Default::default()
        };

        for RecoveryContainerEntry { record, .. } in &snapshot.containers {
            let runtime_status = live_state
                .container_statuses
                .get(record.id.as_str())
                .cloned()
                .unwrap_or(ContainerStatus::Unknown);
            if self
                .reconcile_recovered_container_from_ledger(
                    snapshot,
                    record,
                    runtime_status,
                    shim_reconnect,
                )
                .await?
            {
                summary.broken_containers += 1;
            }
        }

        for pod in &snapshot.pods {
            if self
                .reconcile_recovered_pod_from_ledger(pod, live_state)
                .await?
            {
                summary.broken_pods += 1;
            }
        }

        Ok(summary)
    }

    async fn cleanup_orphaned_runtime_bundles_from_ledger(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
    ) -> RecoveryCleanupCounter {
        let mut summary = RecoveryCleanupCounter::default();
        let mut known_ids: std::collections::HashSet<String> = snapshot
            .containers
            .iter()
            .map(|entry| entry.record.id.clone())
            .collect();
        known_ids.extend(snapshot.pods.iter().map(|pod| pod.id.clone()));

        let mut scanned_roots = std::collections::HashSet::new();
        for runtime in self.runtime.all_runtimes() {
            let runtime_root = runtime.runtime_root().to_path_buf();
            if !scanned_roots.insert(runtime_root.clone()) {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(&runtime_root) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Some(id) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if known_ids.contains(id) {
                    continue;
                }
                if self.runtime.container_has_active_runtime_state(id) {
                    continue;
                }
                self.record_orphan_cleanup_event("container", id, &path)
                    .await;
                if let Err(err) = std::fs::remove_dir_all(&path) {
                    summary.failures += 1;
                    log::warn!(
                        "Failed to remove orphaned runtime bundle {}: {}",
                        path.display(),
                        err
                    );
                } else {
                    summary.removed += 1;
                    if let Err(err) = self
                        .persistence
                        .lock()
                        .await
                        .delete_runtime_artifacts("container", id)
                    {
                        summary.failures += 1;
                        log::warn!(
                            "Failed to delete orphaned runtime artifact ledger entries for {}: {}",
                            id,
                            err
                        );
                    }
                }
            }
        }
        summary
    }

    async fn cleanup_orphaned_pod_workspaces_from_ledger(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
    ) -> RecoveryCleanupCounter {
        let mut summary = RecoveryCleanupCounter::default();
        let known_pod_ids: std::collections::HashSet<String> =
            snapshot.pods.iter().map(|pod| pod.id.clone()).collect();
        let pod_root = self.config.root_dir.join("pods");
        let Ok(entries) = std::fs::read_dir(&pod_root) else {
            return summary;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(id) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if known_pod_ids.contains(id) {
                continue;
            }
            self.record_orphan_cleanup_event("pod", id, &path).await;
            if let Err(err) = std::fs::remove_dir_all(&path) {
                summary.failures += 1;
                log::warn!(
                    "Failed to remove orphaned pod workspace {}: {}",
                    path.display(),
                    err
                );
            } else {
                summary.removed += 1;
                if let Err(err) = self
                    .persistence
                    .lock()
                    .await
                    .delete_runtime_artifacts("pod", id)
                {
                    summary.failures += 1;
                    log::warn!(
                        "Failed to delete orphaned pod artifact ledger entries for {}: {}",
                        id,
                        err
                    );
                }
            }
        }
        summary
    }

    async fn restore_recovery_memory_state(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
    ) -> Result<usize, Status> {
        {
            let mut memory_containers = self.containers.lock().await;
            for RecoveryContainerEntry { status, record } in snapshot.containers.iter().cloned() {
                let annotations: HashMap<String, String> =
                    serde_json::from_str(&record.annotations).unwrap_or_default();
                if let Some(runtime_handler) = record
                    .runtime_handler
                    .as_deref()
                    .filter(|handler| !handler.trim().is_empty())
                {
                    self.runtime
                        .remember_recovered_container_handler(&record.id, runtime_handler);
                }
                let container_state = Self::read_internal_state::<StoredContainerState>(
                    &annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                );
                let container_name = container_state
                    .as_ref()
                    .and_then(|state| state.metadata_name.clone())
                    .unwrap_or_else(|| {
                        record
                            .command
                            .split_whitespace()
                            .next()
                            .unwrap_or("unknown")
                            .to_string()
                    });

                let mut container = crate::proto::runtime::v1::Container {
                    id: record.id.clone(),
                    metadata: Some(crate::proto::runtime::v1::ContainerMetadata {
                        name: container_name.clone(),
                        attempt: container_state
                            .as_ref()
                            .and_then(|state| state.metadata_attempt)
                            .unwrap_or(1),
                    }),
                    state: match status {
                        crate::runtime::ContainerStatus::Created => {
                            ContainerState::ContainerCreated as i32
                        }
                        crate::runtime::ContainerStatus::Running => {
                            ContainerState::ContainerRunning as i32
                        }
                        crate::runtime::ContainerStatus::Stopped(_) => {
                            ContainerState::ContainerExited as i32
                        }
                        crate::runtime::ContainerStatus::Unknown => {
                            ContainerState::ContainerUnknown as i32
                        }
                    },
                    pod_sandbox_id: record.pod_id.clone(),
                    image: Some(ImageSpec {
                        image: record.image.clone(),
                        ..Default::default()
                    }),
                    image_ref: record.image,
                    labels: serde_json::from_str(&record.labels).unwrap_or_default(),
                    annotations,
                    created_at: record.created_at,
                };
                if let Some(mut state) = container_state.clone() {
                    if state.finished_at.is_none() {
                        state.finished_at = record.exit_time;
                    }
                    if state.exit_code.is_none() {
                        state.exit_code = record.exit_code;
                    }
                    let mut annotations = container.annotations.clone();
                    if Self::insert_internal_state(
                        &mut annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                        &state,
                    )
                    .is_ok()
                    {
                        container.annotations = annotations;
                    }
                }
                log::info!(
                    "Recovered container: {} with name {}",
                    record.id,
                    container_name
                );
                memory_containers.insert(record.id, container);
            }
            log::info!(
                "Recovered {} containers from database",
                memory_containers.len()
            );
        }

        {
            let mut memory_pods = self.pod_sandboxes.lock().await;
            let mut pod_manager = self.pod_manager.lock().await;
            for record in snapshot.pods.iter().cloned() {
                let annotations: HashMap<String, String> =
                    serde_json::from_str(&record.annotations).unwrap_or_default();
                let pod_state = Self::read_internal_state::<StoredPodState>(
                    &annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .unwrap_or_default();
                let recovered_runtime_handler = if pod_state.runtime_handler.is_empty()
                    || !self
                        .config
                        .runtime_handlers
                        .iter()
                        .any(|handler| handler == &pod_state.runtime_handler)
                {
                    self.config.runtime.clone()
                } else {
                    pod_state.runtime_handler.clone()
                };
                let labels: HashMap<String, String> =
                    serde_json::from_str(&record.labels).unwrap_or_default();
                let pod = crate::proto::runtime::v1::PodSandbox {
                    id: record.id.clone(),
                    metadata: Some(PodSandboxMetadata {
                        name: record.name.clone(),
                        uid: record.uid.clone(),
                        namespace: record.namespace.clone(),
                        attempt: 1,
                    }),
                    state: match record.state.as_str() {
                        "ready" => PodSandboxState::SandboxReady as i32,
                        "notready" => PodSandboxState::SandboxNotready as i32,
                        _ => PodSandboxState::SandboxNotready as i32,
                    },
                    created_at: record.created_at,
                    labels: labels.clone(),
                    annotations: annotations.clone(),
                    runtime_handler: recovered_runtime_handler.clone(),
                };
                log::info!("Recovered pod: {} with state {}", record.id, record.state);
                memory_pods.insert(record.id.clone(), pod);
                if let Err(err) = self
                    .pod_names
                    .lock()
                    .map_err(|_| "pod name registry lock poisoned".to_string())
                    .and_then(|mut registry| {
                        registry.reserve(
                            &Self::pod_name_key(&PodSandboxMetadata {
                                name: record.name.clone(),
                                uid: record.uid.clone(),
                                namespace: record.namespace.clone(),
                                attempt: 1,
                            }),
                            &record.id,
                        )
                    })
                {
                    log::warn!(
                        "Failed to reserve recovered pod name for {}: {}",
                        record.id,
                        err
                    );
                }

                let ordered_ip_values: Vec<IpAddr> = pod_state
                    .raw_cni_result
                    .as_ref()
                    .map(crate::network::CniManager::ordered_result_ips)
                    .unwrap_or_else(|| {
                        pod_state
                            .additional_ips
                            .iter()
                            .filter_map(|ip| ip.parse::<IpAddr>().ok())
                            .collect()
                    });
                let (selected_primary_ip, _) = crate::network::CniManager::select_pod_ips(
                    ordered_ip_values.clone(),
                    self.config.cni_config.ip_pref(),
                );
                let primary_ip = pod_state
                    .ip
                    .as_ref()
                    .and_then(|ip| ip.parse::<IpAddr>().ok())
                    .or(selected_primary_ip);
                let network_status = primary_ip.map(|parsed_ip| {
                    let mut additional_ip_values = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    for ip in &ordered_ip_values {
                        if ip == &parsed_ip {
                            continue;
                        }
                        if seen.insert(*ip) {
                            additional_ip_values.push(*ip);
                        }
                    }
                    let mut status = pod_state
                        .raw_cni_result
                        .as_ref()
                        .and_then(|raw| {
                            crate::network::CniManager::network_status_from_cni_result_with_preference(
                                Some(raw),
                                self.config.cni_config.ip_pref(),
                            )
                            .ok()
                        })
                        .unwrap_or(crate::network::NetworkStatus {
                            name: "default".to_string(),
                            ip: Some(parsed_ip),
                            mac: None,
                            interfaces: additional_ip_values
                                .iter()
                                .enumerate()
                                .map(|(idx, ip)| crate::network::NetworkInterface {
                                    name: format!("additional{}", idx),
                                    ip: Some(*ip),
                                    mac: None,
                                    netmask: None,
                                    gateway: None,
                                })
                                .collect(),
                            raw_result: None,
                        });
                    status.ip = Some(parsed_ip);
                    status.interfaces = additional_ip_values
                        .iter()
                        .enumerate()
                        .map(|(idx, ip)| crate::network::NetworkInterface {
                            name: format!("additional{}", idx),
                            ip: Some(*ip),
                            mac: None,
                            netmask: None,
                            gateway: None,
                        })
                        .collect();
                    status
                });

                pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
                    id: record.id.clone(),
                    config: crate::pod::PodSandboxConfig {
                        name: record.name.clone(),
                        namespace: record.namespace.clone(),
                        uid: record.uid.clone(),
                        hostname: pod_state
                            .hostname
                            .clone()
                            .unwrap_or_else(|| record.name.clone()),
                        log_directory: pod_state.log_directory.as_ref().map(PathBuf::from),
                        runtime_handler: recovered_runtime_handler.clone(),
                        labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                        annotations: Self::external_container_annotations(&annotations)
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                        dns_config: None,
                        port_mappings: pod_state
                            .port_mappings
                            .iter()
                            .map(|mapping| crate::pod::PortMapping {
                                protocol: mapping.protocol.clone(),
                                container_port: mapping.container_port,
                                host_port: mapping.host_port,
                                host_ip: mapping.host_ip.clone(),
                            })
                            .collect(),
                        network_config: None,
                        cgroup_parent: pod_state.cgroup_parent.clone(),
                        sysctls: pod_state.sysctls.clone(),
                        namespace_options: pod_state
                            .namespace_options
                            .as_ref()
                            .map(StoredNamespaceOptions::to_proto),
                        privileged: pod_state.privileged,
                        run_as_user: pod_state.run_as_user.clone(),
                        run_as_group: pod_state.run_as_group,
                        supplemental_groups: pod_state.supplemental_groups.clone(),
                        readonly_rootfs: pod_state.readonly_rootfs,
                        pids_limit: pod_state
                            .linux_resources
                            .as_ref()
                            .and_then(|resources| resources.pids_limit)
                            .or_else(|| {
                                pod_state
                                    .overhead_linux_resources
                                    .as_ref()
                                    .and_then(|resources| resources.pids_limit)
                            }),
                        no_new_privileges: pod_state.no_new_privileges,
                        apparmor_profile: pod_state.apparmor_profile.clone(),
                        selinux_label: pod_state.selinux_label.clone(),
                        seccomp_profile: pod_state
                            .seccomp_profile
                            .as_ref()
                            .and_then(StoredSecurityProfile::to_runtime_seccomp),
                        linux_resources: pod_state
                            .linux_resources
                            .as_ref()
                            .map(StoredLinuxResources::to_proto)
                            .or_else(|| {
                                pod_state
                                    .overhead_linux_resources
                                    .as_ref()
                                    .map(StoredLinuxResources::to_proto)
                            }),
                    },
                    netns_path: PathBuf::from(
                        pod_state
                            .netns_path
                            .unwrap_or_else(|| record.netns_path.clone()),
                    ),
                    pause_container_id: pod_state.pause_container_id.unwrap_or_default(),
                    state: match record.state.as_str() {
                        "ready" => crate::pod::PodSandboxState::Ready,
                        "notready" => crate::pod::PodSandboxState::NotReady,
                        _ => crate::pod::PodSandboxState::Terminated,
                    },
                    created_at: record.created_at,
                    ip: pod_state
                        .ip
                        .or_else(|| {
                            pod_state
                                .additional_ips
                                .iter()
                                .find(|ip| !ip.is_empty())
                                .cloned()
                        })
                        .or(record.ip.clone())
                        .unwrap_or_default(),
                    network_status,
                });
            }
            pod_manager.rebuild_port_mappings();
            log::info!(
                "Recovered {} pod sandboxes from database",
                memory_pods.len()
            );
        }

        Ok(snapshot.containers.len() + snapshot.pods.len())
    }

    async fn reserve_recovered_container_names(&self) -> usize {
        let recovered_container_name_keys = {
            let containers = self.containers.lock().await;
            let pods = self.pod_sandboxes.lock().await;
            containers
                .values()
                .filter_map(|container| {
                    let metadata = container.metadata.clone()?;
                    let pod_metadata = pods.get(&container.pod_sandbox_id)?.metadata.clone()?;
                    Some((
                        container.id.clone(),
                        Self::container_name_key(&metadata, &pod_metadata),
                    ))
                })
                .collect::<Vec<_>>()
        };
        let recovered_container_name_key_count = recovered_container_name_keys.len();
        for (container_id, container_name_key) in recovered_container_name_keys {
            if let Err(err) = self
                .container_names
                .lock()
                .map_err(|_| "container name registry lock poisoned".to_string())
                .and_then(|mut registry| registry.reserve(&container_name_key, &container_id))
            {
                log::warn!(
                    "Failed to reserve recovered container name for {}: {}",
                    container_id,
                    err
                );
            }
        }
        recovered_container_name_key_count
    }

    async fn restore_recovered_seccomp_notifiers(&self) -> usize {
        let mut restored_seccomp_notifiers = 0;
        let recovered_seccomp_notifiers = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter_map(|container| {
                    let state = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    )?;
                    let mode = match state.seccomp_notifier_action.as_deref() {
                        Some("stop") => Some(crate::runtime::SeccompNotifierMode::Stop),
                        Some("log") | Some("") => Some(crate::runtime::SeccompNotifierMode::Log),
                        _ => None,
                    }?;
                    Some((container.id.clone(), mode))
                })
                .collect::<Vec<_>>()
        };
        for (container_id, mode) in recovered_seccomp_notifiers {
            if let Err(err) = self.ensure_seccomp_notifier(&container_id, mode) {
                log::warn!(
                    "Failed to restore seccomp notifier for recovered container {}: {}",
                    container_id,
                    err
                );
            } else {
                restored_seccomp_notifiers += 1;
            }
        }
        restored_seccomp_notifiers
    }

    async fn cleanup_recovery_orphans(
        &self,
        snapshot: &RecoveryLedgerSnapshot,
    ) -> RecoveryOrphanCleanupSummary {
        let mut summary = RecoveryOrphanCleanupSummary::default();
        if self.last_startup_clean_shutdown().unwrap_or(false)
            && !self.last_startup_detected_reboot().unwrap_or(false)
            && !self.last_startup_detected_upgrade().unwrap_or(false)
        {
            log::info!("Skipping orphan runtime sweeps because previous shutdown was clean");
            summary.skipped = true;
            summary.skip_reason = Some("clean-shutdown".to_string());
        } else if !self.config.internal_wipe {
            log::info!("Skipping orphan runtime sweeps because runtime.internal_wipe is disabled");
            summary.skipped = true;
            summary.skip_reason = Some("internal-wipe-disabled".to_string());
        } else {
            let runtime_cleanup = self
                .cleanup_orphaned_runtime_bundles_from_ledger(snapshot)
                .await;
            let pod_cleanup = self
                .cleanup_orphaned_pod_workspaces_from_ledger(snapshot)
                .await;
            let shim_cleanup = self.cleanup_orphaned_shim_artifacts().await;
            let pause_cleanup = self.cleanup_orphaned_pause_processes().await;
            summary.runtime_bundles_removed = runtime_cleanup.removed;
            summary.pod_workspaces_removed = pod_cleanup.removed;
            summary.shim_dirs_removed = shim_cleanup.shim_dirs_removed;
            summary.attach_socket_dirs_removed = shim_cleanup.attach_socket_dirs_removed;
            summary.pause_processes_killed = pause_cleanup.removed;
            summary.failures = runtime_cleanup.failures
                + pod_cleanup.failures
                + shim_cleanup.failures
                + pause_cleanup.failures;
        }
        summary
    }

    async fn record_orphan_cleanup_event(&self, owner_kind: &str, owner_id: &str, path: &Path) {
        let details = format!(
            "pending cleanup for orphaned {owner_kind} artifact at {}",
            path.display()
        );
        self.publish_reconcile_event(InternalEvent::new(
            "reconcile.orphan_cleanup",
            "orphan_cleanup",
            owner_id,
            InternalEventSeverity::Info,
            serde_json::json!({
                "state": "pending",
                "ownerKind": owner_kind,
                "path": path.display().to_string(),
                "details": details,
            }),
        ))
        .await;
    }

    async fn publish_reconcile_event(&self, event: InternalEvent) {
        if let Err(err) = self
            .internal_services
            .events
            .publish_internal(event.clone())
            .await
        {
            log::warn!(
                "Failed to publish internal recovery event {} for {} {}: {}",
                event.kind,
                event.subject_kind,
                event.subject_id,
                err
            );
        }
    }

    pub async fn recover_state(&self) -> Result<(), Status> {
        let recovery_started = Instant::now();
        let mut recovery_result = RecoveryResultSummary {
            finished_at_unix_millis: chrono::Utc::now().timestamp_millis(),
            success: false,
            ..Default::default()
        };

        let stage_started = Instant::now();
        let snapshot = match self.load_recovery_ledger_snapshot().await {
            Ok(snapshot) => {
                let items = snapshot.containers.len()
                    + snapshot.pods.len()
                    + snapshot.snapshots.len()
                    + snapshot.runtime_artifacts.len()
                    + snapshot.shim_processes.len();
                recovery_result.stages.push(Self::recovery_stage_summary(
                    RecoveryStage::LoadLedgerSnapshot,
                    stage_started,
                    true,
                    items,
                    None,
                ));
                snapshot
            }
            Err(status) => {
                recovery_result.stages.push(Self::recovery_stage_summary(
                    RecoveryStage::LoadLedgerSnapshot,
                    stage_started,
                    false,
                    0,
                    Some(status.message().to_string()),
                ));
                recovery_result.total_duration_millis =
                    recovery_started.elapsed().as_millis() as u64;
                recovery_result.finished_at_unix_millis = chrono::Utc::now().timestamp_millis();
                self.record_last_recovery_result(recovery_result);
                return Err(status);
            }
        };

        let stage_started = Instant::now();
        let restored_memory_items = match self.restore_recovery_memory_state(&snapshot).await {
            Ok(items) => items,
            Err(status) => {
                recovery_result.stages.push(Self::recovery_stage_summary(
                    RecoveryStage::RestoreMemoryState,
                    stage_started,
                    false,
                    0,
                    Some(status.message().to_string()),
                ));
                recovery_result.total_duration_millis =
                    recovery_started.elapsed().as_millis() as u64;
                recovery_result.finished_at_unix_millis = chrono::Utc::now().timestamp_millis();
                self.record_last_recovery_result(recovery_result);
                return Err(status);
            }
        };
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::RestoreMemoryState,
            stage_started,
            true,
            restored_memory_items,
            None,
        ));

        let stage_started = Instant::now();
        let recovered_container_name_key_count = self.reserve_recovered_container_names().await;
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::ReserveRecoveredNames,
            stage_started,
            true,
            recovered_container_name_key_count,
            None,
        ));

        let stage_started = Instant::now();
        match self.prune_recovered_logical_duplicates().await {
            Ok(()) => recovery_result.stages.push(Self::recovery_stage_summary(
                RecoveryStage::PruneLogicalDuplicates,
                stage_started,
                true,
                0,
                None,
            )),
            Err(status) => {
                recovery_result.stages.push(Self::recovery_stage_summary(
                    RecoveryStage::PruneLogicalDuplicates,
                    stage_started,
                    false,
                    0,
                    Some(status.message().to_string()),
                ));
                recovery_result.total_duration_millis =
                    recovery_started.elapsed().as_millis() as u64;
                recovery_result.finished_at_unix_millis = chrono::Utc::now().timestamp_millis();
                self.record_last_recovery_result(recovery_result);
                return Err(status);
            }
        }

        let stage_started = Instant::now();
        let restored_seccomp_notifiers = self.restore_recovered_seccomp_notifiers().await;
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::RestoreSeccompNotifiers,
            stage_started,
            true,
            restored_seccomp_notifiers,
            None,
        ));

        let stage_started = Instant::now();
        let live_state = self.probe_runtime_live_state(&snapshot).await;
        let live_state_items =
            live_state.container_statuses.len() + live_state.pause_statuses.len();
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::ProbeRuntimeLiveState,
            stage_started,
            true,
            live_state_items,
            None,
        ));

        let stage_started = Instant::now();
        let shim_reconnect = self.reconnect_shims_from_ledger(&snapshot).await;
        let shim_reconnect_items =
            shim_reconnect.reconnected_shims.len() + shim_reconnect.broken_containers.len();
        recovery_result.reconcile.reconnected_shims = shim_reconnect.reconnected_shims.clone();
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::ReconnectShims,
            stage_started,
            true,
            shim_reconnect_items,
            None,
        ));

        let stage_started = Instant::now();
        match self
            .reconcile_recovered_state_from_ledger(&snapshot, &live_state, &shim_reconnect)
            .await
        {
            Ok(summary) => {
                let items = snapshot.containers.len() + snapshot.pods.len();
                recovery_result.reconcile = summary;
                recovery_result.stages.push(Self::recovery_stage_summary(
                    RecoveryStage::ReconcileObjects,
                    stage_started,
                    true,
                    items,
                    None,
                ));
            }
            Err(status) => {
                recovery_result.stages.push(Self::recovery_stage_summary(
                    RecoveryStage::ReconcileObjects,
                    stage_started,
                    false,
                    snapshot.containers.len() + snapshot.pods.len(),
                    Some(status.message().to_string()),
                ));
                recovery_result.total_duration_millis =
                    recovery_started.elapsed().as_millis() as u64;
                recovery_result.finished_at_unix_millis = chrono::Utc::now().timestamp_millis();
                self.record_last_recovery_result(recovery_result);
                return Err(status);
            }
        }

        let stage_started = Instant::now();
        self.ensure_exit_monitors_for_active_containers().await;
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::RestoreExitMonitors,
            stage_started,
            true,
            0,
            None,
        ));

        let stage_started = Instant::now();
        recovery_result.orphan_cleanup = self.cleanup_recovery_orphans(&snapshot).await;
        let orphan_items = recovery_result.orphan_cleanup.runtime_bundles_removed
            + recovery_result.orphan_cleanup.pod_workspaces_removed
            + recovery_result.orphan_cleanup.shim_dirs_removed
            + recovery_result.orphan_cleanup.attach_socket_dirs_removed
            + recovery_result.orphan_cleanup.pause_processes_killed;
        recovery_result.stages.push(Self::recovery_stage_summary(
            RecoveryStage::CleanupOrphans,
            stage_started,
            true,
            orphan_items,
            None,
        ));

        recovery_result.success = true;
        recovery_result.total_duration_millis = recovery_started.elapsed().as_millis() as u64;
        recovery_result.finished_at_unix_millis = chrono::Utc::now().timestamp_millis();
        self.record_last_recovery_result(recovery_result);
        Ok(())
    }
}
