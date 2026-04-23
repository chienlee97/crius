use super::*;

impl RuntimeServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn maybe_start_exit_monitor_task(
        monitor_key: String,
        container_id: String,
        exit_code_path: PathBuf,
        config: RuntimeConfig,
        nri_config: NriConfig,
        runtime: RuncRuntime,
        nri: Arc<dyn NriApi>,
        containers: Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: Arc<Mutex<PersistenceManager>>,
        events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
        exit_monitors: Arc<Mutex<HashSet<String>>>,
    ) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let exit_code = loop {
                let current_state = {
                    let containers = containers.lock().await;
                    containers
                        .get(&container_id)
                        .map(|container| container.state)
                };

                match current_state {
                    Some(state)
                        if state == ContainerState::ContainerCreated as i32
                            || state == ContainerState::ContainerRunning as i32 => {}
                    _ => break None,
                }

                match tokio::fs::read_to_string(&exit_code_path).await {
                    Ok(raw) => match raw.trim().parse::<i32>() {
                        Ok(exit_code) => break Some(exit_code),
                        Err(err) => {
                            log::warn!(
                                "Ignoring invalid exit code file {} for container {}: {}",
                                exit_code_path.display(),
                                container_id,
                                err
                            );
                        }
                    },
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        log::debug!(
                            "Exit monitor could not read {} for {}: {}",
                            exit_code_path.display(),
                            container_id,
                            err
                        );
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };

            if let Some(exit_code) = exit_code {
                Self::record_container_exit_from_monitor(
                    &container_id,
                    exit_code,
                    &config,
                    &nri_config,
                    &runtime,
                    nri.as_ref(),
                    &containers,
                    &pod_sandboxes,
                    &persistence,
                    &events,
                )
                .await;
            }

            let mut exit_monitors = exit_monitors.lock().await;
            exit_monitors.remove(&monitor_key);
        });
    }

    pub(super) fn publish_event(&self, event: ContainerEventResponse) {
        if let Err(err) = self.events.send(event) {
            log::debug!("Dropping CRI event without subscribers: {}", err);
        }
    }

    pub(super) fn exit_code_path(&self, container_id: &str) -> PathBuf {
        self.shim_work_dir.join(container_id).join("exit_code")
    }

    pub(super) fn ensure_exit_monitor_registered(&self, container_id: &str) {
        let container_id = container_id.to_string();
        let monitor_key = format!("ctr:{}", container_id);
        let exit_code_path = self.exit_code_path(&container_id);
        let config = self.config.clone();
        let nri_config = self.nri_config.clone();
        let runtime = self.runtime.clone();
        let nri = self.nri.clone();
        let containers = self.containers.clone();
        let pod_sandboxes = self.pod_sandboxes.clone();
        let persistence = self.persistence.clone();
        let events = self.events.clone();
        let exit_monitors = self.exit_monitors.clone();

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let should_start = {
                let mut exit_monitors = exit_monitors.lock().await;
                exit_monitors.insert(monitor_key.clone())
            };

            if !should_start {
                return;
            }

            Self::maybe_start_exit_monitor_task(
                monitor_key,
                container_id,
                exit_code_path,
                config,
                nri_config,
                runtime,
                nri,
                containers,
                pod_sandboxes,
                persistence,
                events,
                exit_monitors,
            );
        });
    }

    pub(super) fn ensure_pod_exit_monitor_registered(
        &self,
        pod_id: &str,
        pause_container_id: &str,
    ) {
        let pod_id = pod_id.to_string();
        let pause_container_id = pause_container_id.to_string();
        let monitor_key = format!("pod:{}", pod_id);
        let exit_code_path = self.exit_code_path(&pause_container_id);
        let config = self.config.clone();
        let containers = self.containers.clone();
        let pod_sandboxes = self.pod_sandboxes.clone();
        let persistence = self.persistence.clone();
        let events = self.events.clone();
        let exit_monitors = self.exit_monitors.clone();

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let should_start = {
                let mut exit_monitors = exit_monitors.lock().await;
                exit_monitors.insert(monitor_key.clone())
            };

            if !should_start {
                return;
            }

            let maybe_exit_code = loop {
                let current_state = {
                    let pod_sandboxes = pod_sandboxes.lock().await;
                    pod_sandboxes.get(&pod_id).map(|pod| pod.state)
                };

                match current_state {
                    Some(state) if state == PodSandboxState::SandboxReady as i32 => {}
                    _ => break None,
                }

                match tokio::fs::read_to_string(&exit_code_path).await {
                    Ok(raw) => match raw.trim().parse::<i32>() {
                        Ok(exit_code) => break Some(exit_code),
                        Err(err) => {
                            log::warn!(
                                "Ignoring invalid pause exit code file {} for pod {}: {}",
                                exit_code_path.display(),
                                pod_id,
                                err
                            );
                        }
                    },
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        log::debug!(
                            "Pause exit monitor could not read {} for pod {}: {}",
                            exit_code_path.display(),
                            pod_id,
                            err
                        );
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };

            if let Some(exit_code) = maybe_exit_code {
                Self::record_pod_exit_from_monitor(
                    &pod_id,
                    exit_code,
                    &config,
                    &containers,
                    &pod_sandboxes,
                    &persistence,
                    &events,
                )
                .await;
            }

            let mut exit_monitors = exit_monitors.lock().await;
            exit_monitors.remove(&monitor_key);
        });
    }

    pub(super) async fn ensure_exit_monitors_for_active_containers(&self) {
        let active_container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|container| {
                    container.state == ContainerState::ContainerCreated as i32
                        || container.state == ContainerState::ContainerRunning as i32
                })
                .map(|container| container.id.clone())
                .collect()
        };

        for container_id in active_container_ids {
            self.ensure_exit_monitor_registered(&container_id);
        }

        let active_pods: Vec<(String, String)> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .values()
                .filter(|pod| pod.state == PodSandboxState::SandboxReady as i32)
                .filter_map(|pod| {
                    Self::read_internal_state::<StoredPodState>(
                        &pod.annotations,
                        INTERNAL_POD_STATE_KEY,
                    )
                    .and_then(|state| state.pause_container_id)
                    .filter(|pause_container_id| !pause_container_id.is_empty())
                    .map(|pause_container_id| (pod.id.clone(), pause_container_id))
                })
                .collect()
        };

        for (pod_id, pause_container_id) in active_pods {
            self.ensure_pod_exit_monitor_registered(&pod_id, &pause_container_id);
        }
    }

    pub(super) async fn cleanup_orphaned_shim_artifacts(&self) {
        let known_container_ids: HashSet<String> =
            self.containers.lock().await.keys().cloned().collect();
        let known_pause_ids: HashSet<String> = self
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

        let Ok(entries) = std::fs::read_dir(&self.shim_work_dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(id) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            if known_container_ids.contains(id) || known_pause_ids.contains(id) {
                continue;
            }

            let metadata = std::fs::read(path.join("shim.json"))
                .ok()
                .and_then(|raw| serde_json::from_slice::<ShimProcess>(&raw).ok());
            let live_process = metadata
                .as_ref()
                .map(|process| {
                    PathBuf::from("/proc")
                        .join(process.shim_pid.to_string())
                        .exists()
                })
                .unwrap_or(false);

            if live_process {
                log::warn!(
                    "Keeping orphaned shim directory {} because shim pid {} still appears live",
                    path.display(),
                    metadata
                        .as_ref()
                        .map(|process| process.shim_pid)
                        .unwrap_or_default()
                );
                continue;
            }

            if let Err(err) = std::fs::remove_dir_all(&path) {
                log::warn!(
                    "Failed to remove orphaned shim directory {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    #[allow(dead_code)]
    pub(super) async fn current_container_snapshot(
        &self,
        container_id: &str,
    ) -> Option<CriContainerStatus> {
        let container = {
            let containers = self.containers.lock().await;
            containers.get(container_id).cloned()
        }?;
        let runtime_state = Self::map_runtime_container_state(
            self.runtime_container_status_checked(container_id).await,
        );
        Some(Self::build_container_status_snapshot(
            &container,
            runtime_state,
        ))
    }

    pub(super) async fn current_pod_status_snapshot(
        &self,
        pod_id: &str,
    ) -> Option<PodSandboxStatus> {
        let pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(pod_id).cloned()
        }?;
        Some(self.build_pod_sandbox_status_snapshot(&pod))
    }

    pub(super) async fn current_pod_container_snapshots(
        &self,
        pod_id: &str,
    ) -> Vec<CriContainerStatus> {
        let containers: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|container| container.pod_sandbox_id == pod_id)
                .cloned()
                .collect()
        };

        let mut snapshots = Vec::with_capacity(containers.len());
        for container in containers {
            let runtime_state = Self::map_runtime_container_state(
                self.runtime_container_status_checked(&container.id).await,
            );
            snapshots.push(Self::build_container_status_snapshot(
                &container,
                runtime_state,
            ));
        }
        snapshots
    }

    pub(super) fn pod_event_container_id(
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> String {
        Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        )
        .and_then(|state| state.pause_container_id)
        .filter(|container_id| !container_id.is_empty())
        .unwrap_or_else(|| pod_sandbox.id.clone())
    }

    pub(super) async fn emit_container_event(
        &self,
        event_type: ContainerEventType,
        container: &Container,
        runtime_state: Option<i32>,
    ) {
        let pod_status = self
            .current_pod_status_snapshot(&container.pod_sandbox_id)
            .await;
        let snapshot = Self::build_container_status_snapshot(
            container,
            runtime_state.unwrap_or(container.state),
        );
        self.publish_event(ContainerEventResponse {
            container_id: container.id.clone(),
            container_event_type: event_type as i32,
            created_at: Self::now_nanos(),
            pod_sandbox_status: pod_status,
            containers_statuses: vec![snapshot],
        });
    }

    pub(super) async fn emit_pod_event(
        &self,
        event_type: ContainerEventType,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
        containers_statuses: Vec<CriContainerStatus>,
    ) {
        self.publish_event(ContainerEventResponse {
            container_id: Self::pod_event_container_id(pod_sandbox),
            container_event_type: event_type as i32,
            created_at: Self::now_nanos(),
            pod_sandbox_status: Some(self.build_pod_sandbox_status_snapshot(pod_sandbox)),
            containers_statuses,
        });
    }

    pub(super) fn publish_event_via_sender(
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
        event: ContainerEventResponse,
    ) {
        if let Err(err) = events.send(event) {
            log::debug!("Dropping CRI event without subscribers: {}", err);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn record_container_exit_from_monitor(
        container_id: &str,
        exit_code: i32,
        config: &RuntimeConfig,
        nri_config: &NriConfig,
        runtime: &RuncRuntime,
        nri: &dyn NriApi,
        containers: &Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: &Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: &Arc<Mutex<PersistenceManager>>,
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
    ) {
        let now = Self::now_nanos();
        let (updated_container, should_notify_nri_stop) = {
            let mut containers = containers.lock().await;
            let Some(container) = containers.get_mut(container_id) else {
                return;
            };

            if container.state == ContainerState::ContainerExited as i32 {
                return;
            }

            container.state = ContainerState::ContainerExited as i32;
            let mut state = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            )
            .unwrap_or_default();
            let should_notify_nri_stop = !state.nri_stop_notified;
            state.finished_at.get_or_insert(now);
            state.exit_code = Some(exit_code);
            let _ = Self::insert_internal_state(
                &mut container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
                &state,
            );

            (container.clone(), should_notify_nri_stop)
        };

        Self::persist_container_annotations_for_monitor(
            container_id,
            &updated_container.annotations,
            persistence,
        )
        .await;

        if should_notify_nri_stop {
            let pod = {
                let pod_sandboxes = pod_sandboxes.lock().await;
                pod_sandboxes
                    .get(&updated_container.pod_sandbox_id)
                    .map(|pod| Self::build_nri_pod_from_proto(runtime, pod))
            };
            let nri_event = NriContainerEvent {
                pod,
                container: Self::build_nri_container_from_proto(runtime, &updated_container),
                linux_resources: None,
            };

            match nri.stop_container(nri_event).await {
                Ok(result) => {
                    let domain = NriRuntimeDomain {
                        containers: containers.clone(),
                        pod_sandboxes: pod_sandboxes.clone(),
                        config: config.clone(),
                        nri_config: nri_config.clone(),
                        runtime: runtime.clone(),
                        persistence: persistence.clone(),
                        events: events.clone(),
                    };
                    match domain.apply_updates(&result.updates).await {
                        Ok(failed) if failed.is_empty() => {}
                        Ok(failed) => {
                            let failed_ids = failed
                                .iter()
                                .map(|update| update.container_id.as_str())
                                .collect::<Vec<_>>()
                                .join(", ");
                            log::warn!(
                                "Exit monitor failed to apply NRI StopContainer updates for {}: {}",
                                container_id,
                                failed_ids
                            );
                        }
                        Err(err) => {
                            log::warn!(
                                "Exit monitor failed to apply NRI StopContainer updates for {}: {}",
                                container_id,
                                err
                            );
                        }
                    }
                    let updated_annotations = {
                        let mut containers = containers.lock().await;
                        containers.get_mut(container_id).and_then(|container| {
                            let mut state = Self::read_internal_state::<StoredContainerState>(
                                &container.annotations,
                                INTERNAL_CONTAINER_STATE_KEY,
                            )
                            .unwrap_or_default();
                            if state.nri_stop_notified {
                                return None;
                            }
                            state.nri_stop_notified = true;
                            if Self::insert_internal_state(
                                &mut container.annotations,
                                INTERNAL_CONTAINER_STATE_KEY,
                                &state,
                            )
                            .is_err()
                            {
                                return None;
                            }
                            Some(container.annotations.clone())
                        })
                    };

                    if let Some(updated_annotations) = updated_annotations {
                        Self::persist_container_annotations_for_monitor(
                            container_id,
                            &updated_annotations,
                            persistence,
                        )
                        .await;
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Exit monitor failed to notify NRI StopContainer for {}: {}",
                        container_id,
                        err
                    );
                }
            }
        }

        {
            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence.update_container_state(
                container_id,
                crate::runtime::ContainerStatus::Stopped(exit_code),
            ) {
                log::warn!(
                    "Exit monitor failed to persist container {} state: {}",
                    container_id,
                    err
                );
            }
        }

        let mut updated_pod = None;
        {
            let mut pod_sandboxes = pod_sandboxes.lock().await;
            if let Some(pod) = pod_sandboxes.get_mut(&updated_container.pod_sandbox_id) {
                let is_pause_container = Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .and_then(|state| state.pause_container_id)
                .as_deref()
                    == Some(container_id);

                if is_pause_container && pod.state != PodSandboxState::SandboxNotready as i32 {
                    pod.state = PodSandboxState::SandboxNotready as i32;
                    updated_pod = Some(pod.clone());
                }
            }
        }

        if updated_pod.is_some() {
            let mut persistence = persistence.lock().await;
            if let Err(err) =
                persistence.update_pod_state(&updated_container.pod_sandbox_id, "notready")
            {
                log::warn!(
                    "Exit monitor failed to persist pod {} state: {}",
                    updated_container.pod_sandbox_id,
                    err
                );
            }
        }

        let pod_status = {
            let pod_sandboxes = pod_sandboxes.lock().await;
            pod_sandboxes
                .get(&updated_container.pod_sandbox_id)
                .cloned()
        }
        .map(|pod| Self::build_pod_sandbox_status_snapshot_with_config(config, &pod));

        let snapshot = Self::build_container_status_snapshot(
            &updated_container,
            ContainerState::ContainerExited as i32,
        );
        Self::publish_event_via_sender(
            events,
            ContainerEventResponse {
                container_id: updated_container.id.clone(),
                container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                created_at: now,
                pod_sandbox_status: pod_status,
                containers_statuses: vec![snapshot],
            },
        );

        if let Some(updated_pod) = updated_pod {
            let container_snapshots = {
                let containers = containers.lock().await;
                containers
                    .values()
                    .filter(|container| container.pod_sandbox_id == updated_pod.id)
                    .cloned()
                    .map(|container| {
                        Self::build_container_status_snapshot(&container, container.state)
                    })
                    .collect::<Vec<_>>()
            };

            Self::publish_event_via_sender(
                events,
                ContainerEventResponse {
                    container_id: Self::pod_event_container_id(&updated_pod),
                    container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                    created_at: now,
                    pod_sandbox_status: Some(Self::build_pod_sandbox_status_snapshot_with_config(
                        config,
                        &updated_pod,
                    )),
                    containers_statuses: container_snapshots,
                },
            );
        }
    }

    pub(super) async fn persist_container_annotations_for_monitor(
        container_id: &str,
        annotations: &HashMap<String, String>,
        persistence: &Arc<Mutex<PersistenceManager>>,
    ) {
        let encoded_annotations = match serde_json::to_string(annotations) {
            Ok(encoded) => encoded,
            Err(err) => {
                log::warn!(
                    "Exit monitor failed to encode annotations for {}: {}",
                    container_id,
                    err
                );
                return;
            }
        };

        let mut persistence = persistence.lock().await;
        match persistence.storage().get_container(container_id) {
            Ok(Some(mut record)) => {
                record.annotations = encoded_annotations;
                if let Err(err) = persistence.storage_mut().save_container(&record) {
                    log::warn!(
                        "Exit monitor failed to persist annotations for {}: {}",
                        container_id,
                        err
                    );
                }
            }
            Ok(None) => {}
            Err(err) => {
                log::warn!(
                    "Exit monitor failed to load container record {}: {}",
                    container_id,
                    err
                );
            }
        }
    }

    pub(super) async fn record_pod_exit_from_monitor(
        pod_id: &str,
        _exit_code: i32,
        config: &RuntimeConfig,
        containers: &Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: &Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: &Arc<Mutex<PersistenceManager>>,
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
    ) {
        let now = Self::now_nanos();
        let updated_pod = {
            let mut pod_sandboxes = pod_sandboxes.lock().await;
            let Some(pod) = pod_sandboxes.get_mut(pod_id) else {
                return;
            };

            if pod.state == PodSandboxState::SandboxNotready as i32 {
                return;
            }

            pod.state = PodSandboxState::SandboxNotready as i32;
            pod.clone()
        };

        {
            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence.update_pod_state(pod_id, "notready") {
                log::warn!(
                    "Pause exit monitor failed to persist pod {} state: {}",
                    pod_id,
                    err
                );
            }
        }

        let container_snapshots = {
            let containers = containers.lock().await;
            containers
                .values()
                .filter(|container| container.pod_sandbox_id == pod_id)
                .cloned()
                .map(|container| Self::build_container_status_snapshot(&container, container.state))
                .collect::<Vec<_>>()
        };

        Self::publish_event_via_sender(
            events,
            ContainerEventResponse {
                container_id: Self::pod_event_container_id(&updated_pod),
                container_event_type: ContainerEventType::ContainerStoppedEvent as i32,
                created_at: now,
                pod_sandbox_status: Some(Self::build_pod_sandbox_status_snapshot_with_config(
                    config,
                    &updated_pod,
                )),
                containers_statuses: container_snapshots,
            },
        );
    }

    #[allow(dead_code)]
    pub(super) async fn refresh_runtime_state_and_publish_events(
        runtime: &RuncRuntime,
        config: &RuntimeConfig,
        containers: &Arc<Mutex<HashMap<String, Container>>>,
        pod_sandboxes: &Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
        persistence: &Arc<Mutex<PersistenceManager>>,
        events: &tokio::sync::broadcast::Sender<ContainerEventResponse>,
    ) {
        let container_ids: Vec<String> = {
            let containers = containers.lock().await;
            containers.keys().cloned().collect()
        };

        for container_id in container_ids {
            let runtime_clone = runtime.clone();
            let container_id_for_status = container_id.clone();
            let runtime_status = match tokio::task::spawn_blocking(move || {
                runtime_clone.container_status(&container_id_for_status)
            })
            .await
            {
                Ok(Ok(status)) => status,
                Ok(Err(err)) => {
                    log::debug!(
                        "Background state refresh failed to read runtime status for {}: {}",
                        container_id,
                        err
                    );
                    continue;
                }
                Err(err) => {
                    log::debug!(
                        "Background state refresh join error for {}: {}",
                        container_id,
                        err
                    );
                    continue;
                }
            };

            if matches!(runtime_status, ContainerStatus::Unknown) {
                continue;
            }

            let next_state = Self::map_runtime_container_state(runtime_status.clone());
            let mut next_container = None;
            let mut emitted_event = None;
            {
                let mut containers = containers.lock().await;
                if let Some(container) = containers.get_mut(&container_id) {
                    if container.state == next_state {
                        continue;
                    }
                    container.state = next_state;
                    if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    ) {
                        match runtime_status {
                            ContainerStatus::Running => {
                                state.started_at.get_or_insert(Self::now_nanos());
                                state.finished_at = None;
                                state.exit_code = None;
                            }
                            ContainerStatus::Stopped(code) => {
                                state.finished_at.get_or_insert(Self::now_nanos());
                                state.exit_code = Some(code);
                            }
                            ContainerStatus::Created | ContainerStatus::Unknown => {}
                        }
                        let _ = Self::insert_internal_state(
                            &mut container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                            &state,
                        );
                    }
                    next_container = Some(container.clone());
                    emitted_event = Some(match next_state {
                        x if x == ContainerState::ContainerRunning as i32 => {
                            ContainerEventType::ContainerStartedEvent
                        }
                        x if x == ContainerState::ContainerExited as i32 => {
                            ContainerEventType::ContainerStoppedEvent
                        }
                        x if x == ContainerState::ContainerCreated as i32 => {
                            ContainerEventType::ContainerCreatedEvent
                        }
                        _ => ContainerEventType::ContainerStoppedEvent,
                    });
                }
            }

            let Some(container_after_update) = next_container else {
                continue;
            };

            {
                let mut persistence = persistence.lock().await;
                if let Err(err) = persistence.update_container_state(
                    &container_id,
                    match runtime_status {
                        ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
                        ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
                        ContainerStatus::Stopped(code) => {
                            crate::runtime::ContainerStatus::Stopped(code)
                        }
                        ContainerStatus::Unknown => crate::runtime::ContainerStatus::Unknown,
                    },
                ) {
                    log::warn!(
                        "Background state refresh failed to persist container {}: {}",
                        container_id,
                        err
                    );
                }
            }

            if let Some(event_type) = emitted_event {
                let pod_status = {
                    let pod_sandboxes = pod_sandboxes.lock().await;
                    pod_sandboxes
                        .get(&container_after_update.pod_sandbox_id)
                        .cloned()
                        .map(|pod| {
                            Self::build_pod_sandbox_status_snapshot_with_config(config, &pod)
                        })
                };
                let snapshot =
                    Self::build_container_status_snapshot(&container_after_update, next_state);
                Self::publish_event_via_sender(
                    events,
                    ContainerEventResponse {
                        container_id: container_after_update.id.clone(),
                        container_event_type: event_type as i32,
                        created_at: Self::now_nanos(),
                        pod_sandbox_status: pod_status,
                        containers_statuses: vec![snapshot],
                    },
                );
            }
        }

        let pod_ids: Vec<String> = {
            let pods = pod_sandboxes.lock().await;
            pods.keys().cloned().collect()
        };

        for pod_id in pod_ids {
            let current_pod = {
                let pods = pod_sandboxes.lock().await;
                pods.get(&pod_id).cloned()
            };
            let Some(current_pod) = current_pod else {
                continue;
            };
            let pause_container_id = Self::read_internal_state::<StoredPodState>(
                &current_pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .and_then(|state| state.pause_container_id);
            let Some(pause_container_id) = pause_container_id else {
                continue;
            };

            let runtime_clone = runtime.clone();
            let pause_id_for_status = pause_container_id.clone();
            let pause_status = match tokio::task::spawn_blocking(move || {
                runtime_clone.container_status(&pause_id_for_status)
            })
            .await
            {
                Ok(Ok(status)) => status,
                _ => continue,
            };

            let next_pod_state = match pause_status {
                ContainerStatus::Running => PodSandboxState::SandboxReady as i32,
                _ => PodSandboxState::SandboxNotready as i32,
            };
            if next_pod_state == current_pod.state {
                continue;
            }

            let updated_pod = {
                let mut pods = pod_sandboxes.lock().await;
                if let Some(pod) = pods.get_mut(&pod_id) {
                    pod.state = next_pod_state;
                    Some(pod.clone())
                } else {
                    None
                }
            };
            let Some(updated_pod) = updated_pod else {
                continue;
            };

            {
                let mut persistence = persistence.lock().await;
                let target_state = if next_pod_state == PodSandboxState::SandboxReady as i32 {
                    "ready"
                } else {
                    "notready"
                };
                if let Err(err) = persistence.update_pod_state(&pod_id, target_state) {
                    log::warn!(
                        "Background state refresh failed to persist pod {}: {}",
                        pod_id,
                        err
                    );
                }
            }

            let container_snapshots = {
                let containers_snapshot: Vec<Container> = {
                    let containers = containers.lock().await;
                    containers
                        .values()
                        .filter(|container| container.pod_sandbox_id == pod_id)
                        .cloned()
                        .collect()
                };
                let mut snapshots = Vec::with_capacity(containers_snapshot.len());
                for container in containers_snapshot {
                    snapshots.push(Self::build_container_status_snapshot(
                        &container,
                        container.state,
                    ));
                }
                snapshots
            };

            Self::publish_event_via_sender(
                events,
                ContainerEventResponse {
                    container_id: Self::pod_event_container_id(&updated_pod),
                    container_event_type: if next_pod_state == PodSandboxState::SandboxReady as i32
                    {
                        ContainerEventType::ContainerStartedEvent as i32
                    } else {
                        ContainerEventType::ContainerStoppedEvent as i32
                    },
                    created_at: Self::now_nanos(),
                    pod_sandbox_status: Some(Self::build_pod_sandbox_status_snapshot_with_config(
                        config,
                        &updated_pod,
                    )),
                    containers_statuses: container_snapshots,
                },
            );
        }
    }
}
