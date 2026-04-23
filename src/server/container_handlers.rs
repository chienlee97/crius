use super::*;

impl RuntimeServiceImpl {
    pub(super) async fn undo_failed_nri_start_container(&self, event: NriContainerEvent) {
        match self.nri.stop_container(event.clone()).await {
            Ok(result) => {
                if let Err(err) = self.process_nri_stop_side_effects(&result).await {
                    log::warn!(
                        "Undo stop side effects of failed NRI container start failed: {}",
                        err
                    );
                }
                if let Some(container) = self
                    .mutate_container_internal_state(&event.container.id, |state| {
                        state.nri_stop_notified = true;
                    })
                    .await
                    .ok()
                    .flatten()
                {
                    if let Err(err) =
                        self.persist_bundle_annotations(&event.container.id, &container.annotations)
                    {
                        log::warn!(
                            "Failed to persist undo stop annotations for {}: {}",
                            event.container.id,
                            err
                        );
                    }
                }
            }
            Err(err) => {
                log::warn!("Undo stop of failed NRI container start failed: {}", err);
            }
        }
    }

    pub(super) async fn undo_failed_nri_create_container(&self, event: NriContainerEvent) {
        match self.nri.stop_container(event.clone()).await {
            Ok(result) => {
                if let Err(err) = self.process_nri_stop_side_effects(&result).await {
                    log::warn!(
                        "Undo stop side effects of failed NRI container create failed: {}",
                        err
                    );
                }
            }
            Err(err) => {
                log::warn!("Undo stop of failed NRI container create failed: {}", err);
            }
        }
        if let Err(err) = self.nri.remove_container(event).await {
            log::warn!("Undo remove of failed NRI container create failed: {}", err);
        }
    }

    pub(super) async fn rollback_failed_container_create(
        &self,
        container_id: &str,
        nri_event: NriContainerEvent,
    ) {
        self.undo_failed_nri_create_container(nri_event).await;

        {
            let mut containers = self.containers.lock().await;
            containers.remove(container_id);
        }

        {
            let mut persistence = self.persistence.lock().await;
            if let Err(err) = persistence.delete_container(container_id) {
                log::warn!(
                    "Failed to delete container {} during create rollback: {}",
                    container_id,
                    err
                );
            }
        }

        let runtime = self.runtime.clone();
        let container_id_owned = container_id.to_string();
        if let Err(err) =
            tokio::task::spawn_blocking(move || runtime.remove_container(&container_id_owned)).await
        {
            log::warn!(
                "Failed to join runtime remove task for {} during create rollback: {}",
                container_id,
                err
            );
        }
    }

    pub(super) async fn finalize_container_stop_state(
        &self,
        container_id: &str,
        final_runtime_status: ContainerStatus,
    ) -> Result<Option<Container>, Status> {
        let mut resolved_exit_code = match &final_runtime_status {
            ContainerStatus::Stopped(code) => Some(*code),
            _ => None,
        };

        let updated_container = {
            let mut containers = self.containers.lock().await;
            let Some(container) = containers.get_mut(container_id) else {
                return Ok(None);
            };

            container.state = match &final_runtime_status {
                ContainerStatus::Created => ContainerState::ContainerCreated as i32,
                ContainerStatus::Running => ContainerState::ContainerRunning as i32,
                ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
                ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
            };
            if let Some(mut state) = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            ) {
                state.finished_at = Some(Self::now_nanos());
                if resolved_exit_code.is_none() {
                    resolved_exit_code = state.exit_code;
                }
                if let Some(code) = resolved_exit_code {
                    state.exit_code = Some(code);
                }
                if let Err(err) = Self::insert_internal_state(
                    &mut container.annotations,
                    INTERNAL_CONTAINER_STATE_KEY,
                    &state,
                ) {
                    log::warn!(
                        "Failed to persist in-memory container state for {}: {}",
                        container_id,
                        err
                    );
                }
            }
            if container.state == ContainerState::ContainerUnknown as i32
                && resolved_exit_code.is_some()
            {
                container.state = ContainerState::ContainerExited as i32;
            }

            Some(container.clone())
        };

        let persistence_status = match &final_runtime_status {
            ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
            ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
            ContainerStatus::Stopped(_) => {
                crate::runtime::ContainerStatus::Stopped(resolved_exit_code.unwrap_or_default())
            }
            ContainerStatus::Unknown => match resolved_exit_code {
                Some(code) => crate::runtime::ContainerStatus::Stopped(code),
                None => crate::runtime::ContainerStatus::Unknown,
            },
        };
        let mut persistence = self.persistence.lock().await;
        if let Err(err) = persistence.update_container_state(container_id, persistence_status) {
            log::error!(
                "Failed to update container {} state in database: {}",
                container_id,
                err
            );
        }
        drop(persistence);

        Ok(updated_container)
    }

    pub(super) async fn notify_nri_container_stop(
        &self,
        container_id: &str,
        pod_id: &str,
        annotations: &HashMap<String, String>,
    ) {
        let stop_event = self
            .nri_container_event(pod_id, container_id, annotations)
            .await;
        match self.nri.stop_container(stop_event).await {
            Ok(result) => {
                if let Err(err) = self.process_nri_stop_side_effects(&result).await {
                    log::warn!("NRI stop side effects failed for {}: {}", container_id, err);
                    return;
                }
                if let Err(err) = self
                    .mutate_container_internal_state(container_id, |state| {
                        state.nri_stop_notified = true;
                    })
                    .await
                {
                    log::warn!(
                        "Failed to mark NRI stop notification for {}: {}",
                        container_id,
                        err
                    );
                }
            }
            Err(err) => {
                log::warn!("NRI StopContainer failed for {}: {}", container_id, err);
            }
        }
    }

    pub(super) async fn stop_container_internal(
        &self,
        actual_container_id: &str,
        timeout: u32,
    ) -> Result<Option<Container>, Status> {
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (
                container.pod_sandbox_id.clone(),
                container.annotations.clone(),
            )
        };
        let stop_notified = self
            .container_internal_state(actual_container_id)
            .await
            .map(|state| state.nri_stop_notified)
            .unwrap_or_default();
        let runtime_status_before_stop = self
            .runtime_container_status_checked(actual_container_id)
            .await;
        let needs_nri_stop = matches!(
            runtime_status_before_stop,
            ContainerStatus::Created | ContainerStatus::Running
        );

        let runtime = self.runtime.clone();
        let actual_container_id_owned = actual_container_id.to_string();
        tokio::task::spawn_blocking(move || {
            runtime.stop_container(&actual_container_id_owned, Some(timeout))
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| Status::internal(format!("Failed to stop container: {}", e)))?;

        let final_runtime_status = {
            let runtime = self.runtime.clone();
            let container_id_for_status = actual_container_id.to_string();
            tokio::task::spawn_blocking(move || runtime.container_status(&container_id_for_status))
                .await
                .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
                .unwrap_or(ContainerStatus::Unknown)
        };
        let updated_container = self
            .finalize_container_stop_state(actual_container_id, final_runtime_status)
            .await?;

        if needs_nri_stop && !stop_notified {
            let current_annotations = updated_container
                .as_ref()
                .map(|container| container.annotations.clone())
                .unwrap_or(container_annotations);
            self.notify_nri_container_stop(
                actual_container_id,
                &container_pod_id,
                &current_annotations,
            )
            .await;
        }

        Ok(updated_container)
    }

    pub(super) async fn remove_container_internal(
        &self,
        actual_container_id: &str,
    ) -> Result<Option<Container>, Status> {
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (
                container.pod_sandbox_id.clone(),
                container.annotations.clone(),
            )
        };
        let stop_notified = self
            .container_internal_state(actual_container_id)
            .await
            .map(|state| state.nri_stop_notified)
            .unwrap_or_default();
        let runtime_status = self
            .runtime_container_status_checked(actual_container_id)
            .await;
        let needs_nri_stop_before_remove = matches!(
            runtime_status,
            ContainerStatus::Created | ContainerStatus::Running
        );
        if needs_nri_stop_before_remove && !stop_notified {
            let updated_container = self
                .stop_container_internal(actual_container_id, 30)
                .await?;
            if let Some(container) = updated_container {
                self.emit_container_event(
                    ContainerEventType::ContainerStoppedEvent,
                    &container,
                    Some(container.state),
                )
                .await;
            }
        }

        let remove_notified = self
            .container_internal_state(actual_container_id)
            .await
            .map(|state| state.nri_remove_notified)
            .unwrap_or_default();
        if !remove_notified {
            if let Err(err) = self
                .nri
                .remove_container(
                    self.nri_container_event(
                        &container_pod_id,
                        actual_container_id,
                        &container_annotations,
                    )
                    .await,
                )
                .await
            {
                log::warn!(
                    "NRI RemoveContainer failed for {}: {}",
                    actual_container_id,
                    err
                );
            }
            let _ = self
                .mutate_container_internal_state(actual_container_id, |state| {
                    state.nri_remove_notified = true;
                })
                .await?;
        }

        let deleted_container = {
            let containers = self.containers.lock().await;
            containers.get(actual_container_id).cloned()
        };

        let runtime = self.runtime.clone();
        let actual_container_id_owned = actual_container_id.to_string();
        tokio::task::spawn_blocking(move || runtime.remove_container(&actual_container_id_owned))
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .map_err(|e| Status::internal(format!("Failed to remove container: {}", e)))?;

        {
            let mut containers = self.containers.lock().await;
            containers.remove(actual_container_id);
        }

        let mut persistence = self.persistence.lock().await;
        if let Err(err) = persistence.delete_container(actual_container_id) {
            log::error!(
                "Failed to delete container {} from database: {}",
                actual_container_id,
                err
            );
        } else {
            log::info!("Container {} removed from database", actual_container_id);
        }

        Ok(deleted_container)
    }

    pub(super) fn apply_adjusted_annotations(
        annotations: &mut HashMap<String, String>,
        adjustment: &crate::nri_proto::api::ContainerAdjustment,
    ) {
        for (key, value) in &adjustment.annotations {
            if let Some(name) = key.strip_prefix('-') {
                annotations.remove(name);
            } else {
                annotations.insert(key.clone(), value.clone());
            }
        }
    }

    pub(super) fn refresh_nri_event_container_from_spec(
        event: &mut NriContainerEvent,
        spec: &crate::oci::spec::Spec,
        annotations: &HashMap<String, String>,
    ) {
        event.container.annotations = Self::external_annotations(annotations);
        event.container.args = oci_args(spec);
        event.container.env = oci_env(spec);
        event.container.mounts = oci_mounts(spec);
        if let Some(hooks) = oci_hooks(spec) {
            event.container.hooks = protobuf::MessageField::some(hooks);
        } else {
            event.container.hooks = protobuf::MessageField::none();
        }
        if let Some(linux) = oci_linux_container(spec) {
            event.container.linux = protobuf::MessageField::some(linux);
        } else {
            event.container.linux = protobuf::MessageField::none();
        }
        event.container.rlimits = oci_rlimits(spec);
        if let Some(user) = oci_user(spec) {
            event.container.user = protobuf::MessageField::some(user);
        } else {
            event.container.user = protobuf::MessageField::none();
        }
    }

    pub(super) async fn process_nri_create_side_effects(
        &self,
        result: &NriCreateContainerResult,
    ) -> Result<(), Status> {
        self.process_nri_update_side_effects(&result.updates, &result.evictions, "create")
            .await
    }

    pub(super) async fn process_nri_update_side_effects(
        &self,
        updates: &[crate::nri_proto::api::ContainerUpdate],
        evictions: &[crate::nri_proto::api::ContainerEviction],
        phase: &str,
    ) -> Result<(), Status> {
        let domain = NriRuntimeDomain {
            containers: self.containers.clone(),
            pod_sandboxes: self.pod_sandboxes.clone(),
            config: self.config.clone(),
            nri_config: self.nri_config.clone(),
            runtime: self.runtime.clone(),
            persistence: self.persistence.clone(),
            events: self.events.clone(),
        };
        let failed = domain
            .apply_updates(updates)
            .await
            .map_err(|e| Status::internal(format!("NRI {phase} updates failed: {}", e)))?;
        if !failed.is_empty() {
            let failed_ids = failed
                .iter()
                .map(|update| update.container_id.clone())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Status::internal(format!(
                "NRI {phase} updates failed for: {}",
                failed_ids
            )));
        }

        for eviction in evictions {
            domain
                .evict(&eviction.container_id, &eviction.reason)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "NRI {phase} eviction for {} failed: {}",
                        eviction.container_id, e
                    ))
                })?;
        }

        Ok(())
    }

    pub(super) async fn nri_container_event(
        &self,
        pod_id: &str,
        container_id: &str,
        annotations: &HashMap<String, String>,
    ) -> NriContainerEvent {
        let pod = {
            let pods = self.pod_sandboxes.lock().await;
            pods.get(pod_id)
                .map(|pod| Self::build_nri_pod_from_proto(&self.runtime, pod))
        };
        let container = {
            let containers = self.containers.lock().await;
            containers
                .get(container_id)
                .map(|container| Self::build_nri_container_from_proto(&self.runtime, container))
        }
        .unwrap_or_else(|| {
            let mut container = crate::nri_proto::api::Container::new();
            container.id = container_id.to_string();
            container.pod_sandbox_id = pod_id.to_string();
            container.annotations = Self::external_annotations(annotations);
            container
        });

        NriContainerEvent {
            pod,
            container,
            linux_resources: None,
        }
    }

    pub(super) async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let container_id = req.container_id;
        let timeout = req.timeout as u32;

        log::info!("Stopping container {}", container_id);

        let Some(actual_container_id) = self.resolve_container_id_if_exists(&container_id).await?
        else {
            return Ok(Response::new(StopContainerResponse {}));
        };
        let updated_container = self
            .stop_container_internal(&actual_container_id, timeout)
            .await?;

        log::info!("Container {} stopped", actual_container_id);
        if let Some(container) = updated_container {
            self.emit_container_event(
                ContainerEventType::ContainerStoppedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        Ok(Response::new(StopContainerResponse {}))
    }

    pub(super) async fn remove_container(
        &self,
        request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let container_id = req.container_id;

        log::info!("Removing container {}", container_id);

        let Some(actual_container_id) = self.resolve_container_id_if_exists(&container_id).await?
        else {
            return Ok(Response::new(RemoveContainerResponse {}));
        };
        let deleted_container = self.remove_container_internal(&actual_container_id).await?;

        log::info!("Container {} removed", actual_container_id);
        if let Some(container) = deleted_container {
            self.emit_container_event(
                ContainerEventType::ContainerDeletedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        Ok(Response::new(RemoveContainerResponse {}))
    }

    pub(super) async fn checkpoint_container(
        &self,
        request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        let req = request.into_inner();
        if req.container_id.trim().is_empty() {
            return Err(Status::invalid_argument("container_id must not be empty"));
        }
        if req.location.trim().is_empty() {
            return Err(Status::invalid_argument("location must not be empty"));
        }

        let container_id = self.resolve_container_id(&req.container_id).await?;
        let container = {
            let containers = self.containers.lock().await;
            containers
                .get(&container_id)
                .cloned()
                .ok_or_else(|| Status::not_found("Container not found"))?
        };

        let runtime_status = self.runtime_container_status_checked(&container_id).await;
        if matches!(runtime_status, ContainerStatus::Unknown) {
            return Err(Status::failed_precondition(format!(
                "container {} has no known runtime state",
                container_id
            )));
        }
        if !matches!(runtime_status, ContainerStatus::Running) {
            return Err(Status::failed_precondition(format!(
                "container {} must be running to be checkpointed",
                container_id
            )));
        }

        let runtime_state = Self::map_runtime_container_state(runtime_status);
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let config_path = self.checkpoint_config_path(&container_id);
        if !config_path.exists() {
            return Err(Status::failed_precondition(format!(
                "container {} bundle config is missing at {}",
                container_id,
                config_path.display()
            )));
        }

        let config_payload: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config_path).map_err(|e| {
                Status::internal(format!(
                    "Failed to read checkpoint config {}: {}",
                    config_path.display(),
                    e
                ))
            })?)
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to parse checkpoint config {}: {}",
                    config_path.display(),
                    e
                ))
            })?;

        let checkpoint_location = PathBuf::from(&req.location);
        let checkpoint_image_path = Self::checkpoint_runtime_image_path(&checkpoint_location);
        let rootfs_path = config_payload
            .get("root")
            .and_then(|root| root.get("path"))
            .and_then(|path| path.as_str())
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "container {} checkpoint config is missing root.path",
                    container_id
                ))
            })?;
        let runtime_for_pause = self.runtime.clone();
        let container_id_for_pause = container_id.clone();
        tokio::task::spawn_blocking(move || {
            runtime_for_pause.pause_container(&container_id_for_pause)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to join pause task: {}", e)))?
        .map_err(|e| {
            Status::internal(format!(
                "Failed to pause container before checkpoint: {}",
                e
            ))
        })?;

        let runtime = self.runtime.clone();
        let container_id_for_checkpoint = container_id.clone();
        let checkpoint_image_path_for_runtime = checkpoint_image_path.clone();
        let checkpoint_future = tokio::task::spawn_blocking(move || {
            runtime.checkpoint_container(
                &container_id_for_checkpoint,
                &checkpoint_image_path_for_runtime,
            )
        });
        let checkpoint_result = if req.timeout > 0 {
            tokio::time::timeout(
                std::time::Duration::from_secs(req.timeout as u64),
                checkpoint_future,
            )
            .await
            .map_err(|_| {
                Status::deadline_exceeded(format!("checkpoint timed out after {}s", req.timeout))
            })?
        } else {
            checkpoint_future.await
        };
        let checkpoint_result = checkpoint_result
            .map_err(|e| Status::internal(format!("Failed to join checkpoint task: {}", e)))?
            .map_err(|e| Status::internal(format!("Failed to checkpoint container: {}", e)));

        let runtime_for_resume = self.runtime.clone();
        let container_id_for_resume = container_id.clone();
        let resume_result = tokio::task::spawn_blocking(move || {
            runtime_for_resume.resume_container(&container_id_for_resume)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to join resume task: {}", e)))?;

        if let Err(checkpoint_err) = checkpoint_result {
            if let Err(resume_err) = resume_result {
                return Err(Status::internal(format!(
                    "{}; additionally failed to resume container after checkpoint attempt: {}",
                    checkpoint_err.message(),
                    resume_err
                )));
            }
            return Err(checkpoint_err);
        }

        resume_result.map_err(|e| {
            Status::internal(format!(
                "Failed to resume container after checkpoint: {}",
                e
            ))
        })?;

        let post_checkpoint_status = self.runtime_container_status_checked(&container_id).await;
        if !matches!(post_checkpoint_status, ContainerStatus::Running) {
            return Err(Status::internal(format!(
                "container {} is not running after checkpoint",
                container_id
            )));
        }

        Self::checkpoint_rootfs_snapshot(&rootfs_path, &checkpoint_image_path.join("rootfs.tar"))
            .map_err(|e| Status::internal(format!("Failed to snapshot checkpoint rootfs: {}", e)))?;

        let manifest = json!({
            "containerId": container_id,
            "podSandboxId": container.pod_sandbox_id,
            "imageRef": container.image_ref,
            "runtimeState": Self::runtime_state_name(runtime_state),
            "checkpointedAt": Self::now_nanos(),
            "timeoutSeconds": req.timeout,
            "bundlePath": self.checkpoint_bundle_path(&container.id).display().to_string(),
            "configPath": config_path.display().to_string(),
            "checkpointImagePath": checkpoint_image_path.display().to_string(),
            "createdAt": container.created_at,
            "startedAt": container_state.as_ref().and_then(|state| state.started_at),
            "finishedAt": container_state.as_ref().and_then(|state| state.finished_at),
            "exitCode": container_state.as_ref().and_then(|state| state.exit_code),
            "logPath": container_state.as_ref().and_then(|state| state.log_path.clone()),
            "metadata": {
                "name": container
                    .annotations
                    .get(INTERNAL_CONTAINER_STATE_KEY)
                    .and_then(|_| container_state.as_ref().and_then(|state| state.metadata_name.clone()))
                    .or_else(|| {
                        container
                            .metadata
                            .as_ref()
                            .map(|metadata| metadata.name.clone())
                    })
                    .unwrap_or_default(),
                "attempt": container
                    .annotations
                    .get(INTERNAL_CONTAINER_STATE_KEY)
                    .and_then(|_| container_state.as_ref().and_then(|state| state.metadata_attempt))
                    .or_else(|| {
                        container
                            .metadata
                            .as_ref()
                            .map(|metadata| metadata.attempt)
                    })
                    .unwrap_or(1),
            }
        });

        self.write_checkpoint_artifact(
            &checkpoint_location,
            &checkpoint_image_path,
            &manifest,
            &config_payload,
        )
        .map_err(|e| Status::internal(format!("Failed to write checkpoint artifact: {}", e)))?;

        Ok(Response::new(CheckpointContainerResponse {}))
    }

    pub(super) async fn update_container_resources(
        &self,
        request: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let container_id = self.resolve_container_id(&req.container_id).await?;

        {
            let containers = self.containers.lock().await;
            if !containers.contains_key(&container_id) {
                return Err(Status::not_found("Container not found"));
            }
        }

        let linux = req.linux;
        let _windows = req.windows;

        let runtime_status = self.runtime_container_status_checked(&container_id).await;
        if !matches!(
            runtime_status,
            ContainerStatus::Running | ContainerStatus::Created
        ) {
            return Err(Status::failed_precondition(format!(
                "container {} is not in a mutable state",
                container_id
            )));
        }

        let observed_state = Self::map_runtime_container_state(runtime_status);
        {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(&container_id) {
                container.state = observed_state;
            }
        }
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (
                container.pod_sandbox_id.clone(),
                container.annotations.clone(),
            )
        };
        let mut nri_event = self
            .nri_container_event(&container_pod_id, &container_id, &container_annotations)
            .await;
        nri_event.linux_resources = linux.as_ref().map(linux_resources_from_cri);
        let mut nri_result = self
            .nri
            .update_container(nri_event.clone())
            .await
            .map_err(|e| Status::internal(format!("NRI UpdateContainer failed: {}", e)))?;
        if let Some(resources) = nri_result.linux_resources.as_mut() {
            Self::sanitize_nri_linux_resources_for_nri_config(resources, &self.nri_config);
            validate_adjustment_resources_with_min_memory(resources, Self::nri_min_memory_limit())
                .map_err(|e| Status::internal(format!("NRI UpdateContainer failed: {}", e)))?;
        }
        for update in &nri_result.updates {
            validate_container_update(update)
                .map_err(|e| Status::internal(format!("NRI UpdateContainer failed: {}", e)))?;
        }
        if let Some(resources) = nri_result.linux_resources.as_ref() {
            validate_update_linux_resources(resources)
                .map_err(|e| Status::internal(format!("NRI UpdateContainer failed: {}", e)))?;
        }

        self.process_nri_update_side_effects(&nri_result.updates, &nri_result.evictions, "update")
            .await?;

        let mut final_resources = self
            .container_internal_state(&container_id)
            .await
            .and_then(|state| state.linux_resources)
            .unwrap_or_default();
        if let Some(original) = linux.as_ref() {
            final_resources = StoredLinuxResources::from(original);
        }
        if let Some(resources) = nri_result.linux_resources.as_ref() {
            final_resources.apply_nri(resources);
        }
        Self::sanitize_stored_runtime_resources_for_nri_config(
            &mut final_resources,
            &self.nri_config,
        );

        if linux.is_some() || nri_result.linux_resources.is_some() {
            self.runtime_update_container_resources(&container_id, &final_resources)
                .await?;
            self.mutate_container_internal_state(&container_id, |state| {
                state.linux_resources = Some(final_resources.clone());
            })
            .await?;
        }
        let post_update_annotations = {
            let containers = self.containers.lock().await;
            containers
                .get(&container_id)
                .map(|container| container.annotations.clone())
                .ok_or_else(|| Status::not_found("Container not found"))?
        };
        let mut post_update_event = self
            .nri_container_event(&container_pod_id, &container_id, &post_update_annotations)
            .await;
        post_update_event.linux_resources = Some(final_resources.to_nri());
        if let Err(err) = self.nri.post_update_container(post_update_event).await {
            log::warn!(
                "NRI PostUpdateContainer failed for {}: {}",
                container_id,
                err
            );
        }

        Ok(Response::new(UpdateContainerResourcesResponse {}))
    }

    pub(super) async fn create_container_impl(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        log::info!("CreateContainer called");
        let req = request.into_inner();
        let pod_sandbox_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("Container config not specified"))?;
        let sandbox_config = req.sandbox_config;

        let container_id = uuid::Uuid::new_v4().to_simple().to_string();

        log::info!("Creating container with ID: {}", container_id);
        log::debug!("Container config: {:?}", config);

        let pod_state = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_sandbox_id).and_then(|pod| {
                Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
            })
        };
        let nri_activation_annotations = {
            let mut annotations = {
                let pod_sandboxes = self.pod_sandboxes.lock().await;
                pod_sandboxes
                    .get(&pod_sandbox_id)
                    .map(|pod| Self::external_annotations(&pod.annotations))
                    .unwrap_or_default()
            };
            if let Some(sandbox_config) = sandbox_config.as_ref() {
                for (key, value) in &sandbox_config.annotations {
                    annotations.insert(key.clone(), value.clone());
                }
            }
            for (key, value) in &config.annotations {
                annotations.insert(key.clone(), value.clone());
            }
            annotations
        };

        let sandbox_linux = sandbox_config
            .as_ref()
            .and_then(|config| config.linux.as_ref());
        let security = config
            .linux
            .as_ref()
            .and_then(|linux| linux.security_context.as_ref());
        let namespace_options = security.and_then(|security| security.namespace_options.clone());

        let pod_log_directory = sandbox_config
            .as_ref()
            .and_then(|config| {
                (!config.log_directory.is_empty()).then(|| config.log_directory.clone())
            })
            .or_else(|| {
                pod_state
                    .as_ref()
                    .and_then(|state| state.log_directory.clone())
            });
        let log_path = if config.log_path.is_empty() {
            None
        } else if let Some(log_directory) = pod_log_directory {
            Some(PathBuf::from(log_directory).join(&config.log_path))
        } else {
            Some(PathBuf::from(&config.log_path))
        };

        if let Some(path) = &log_path {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    Status::internal(format!("Failed to prepare log directory: {}", e))
                })?;
            }
        }

        let network_namespace_path = {
            let pod_manager = self.pod_manager.lock().await;
            pod_manager.get_pod_netns(&pod_sandbox_id)
        }
        .or_else(|| {
            pod_state
                .as_ref()
                .and_then(|state| state.netns_path.as_ref().map(PathBuf::from))
        });
        let pause_container_id = {
            let pod_manager = self.pod_manager.lock().await;
            pod_manager
                .get_pod_sandbox_cloned(&pod_sandbox_id)
                .map(|pod| pod.pause_container_id)
        }
        .or_else(|| {
            pod_state
                .as_ref()
                .and_then(|state| state.pause_container_id.clone())
        });
        let pid_namespace_path = if let Some(options) = namespace_options.as_ref() {
            if options.pid == NamespaceMode::Pod as i32 {
                if let Some(pause_id) = pause_container_id.as_deref() {
                    self.runtime_namespace_path_for_container(pause_id, "pid")
                        .await?
                } else {
                    None
                }
            } else if options.pid == NamespaceMode::Target as i32 {
                self.runtime_namespace_path_for_target(&options.target_id, "pid")
                    .await?
            } else {
                None
            }
        } else {
            None
        };
        let ipc_namespace_path = if let Some(options) = namespace_options.as_ref() {
            if options.ipc == NamespaceMode::Pod as i32 {
                if let Some(pause_id) = pause_container_id.as_deref() {
                    self.runtime_namespace_path_for_container(pause_id, "ipc")
                        .await?
                } else {
                    None
                }
            } else if options.ipc == NamespaceMode::Target as i32 {
                self.runtime_namespace_path_for_target(&options.target_id, "ipc")
                    .await?
            } else {
                None
            }
        } else {
            None
        };

        let apparmor_profile = Self::security_profile_name(
            security.and_then(|security| security.apparmor.as_ref()),
            Self::legacy_linux_container_apparmor_profile(security),
        );
        let selinux_label =
            Self::selinux_label_from_proto(security.and_then(|ctx| ctx.selinux_options.as_ref()));
        let seccomp_profile = Self::seccomp_profile_from_proto(
            security.and_then(|ctx| ctx.seccomp.as_ref()),
            Self::legacy_linux_container_seccomp_profile_path(security),
        );
        let stored_seccomp_profile = Self::stored_seccomp_profile_from_proto(
            security.and_then(|ctx| ctx.seccomp.as_ref()),
            Self::legacy_linux_container_seccomp_profile_path(security),
        );
        let checkpoint_location = config
            .image
            .as_ref()
            .map(|image| image.image.trim().to_string())
            .filter(|image| !image.is_empty())
            .and_then(|image| {
                let path = PathBuf::from(&image);
                path.exists().then_some(path)
            })
            .or_else(|| {
                config
                    .annotations
                    .get(CHECKPOINT_LOCATION_ANNOTATION_KEY)
                    .filter(|location| !location.trim().is_empty())
                    .map(PathBuf::from)
            });

        let checkpoint_restore = checkpoint_location
            .as_ref()
            .map(|checkpoint_location| {
                let checkpoint_image_path =
                    Self::checkpoint_runtime_image_path(checkpoint_location);
                let (manifest, oci_config) = Self::load_checkpoint_artifact(checkpoint_location)
                    .map_err(|e| {
                        Status::failed_precondition(format!(
                            "Failed to load checkpoint artifact {}: {}",
                            checkpoint_location.display(),
                            e
                        ))
                    })?;
                let image_ref = manifest
                    .get("imageRef")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
                    .or_else(|| {
                        config
                            .image
                            .as_ref()
                            .map(|image| image.image.clone())
                            .filter(|image| !image.trim().is_empty())
                    })
                    .unwrap_or_default();
                Ok::<StoredCheckpointRestore, Status>(StoredCheckpointRestore {
                    checkpoint_image_path: checkpoint_image_path.display().to_string(),
                    checkpoint_location: checkpoint_location.display().to_string(),
                    image_ref,
                    oci_config,
                })
            })
            .transpose()?;
        let container_image_ref = checkpoint_restore
            .as_ref()
            .map(|restore| restore.image_ref.clone())
            .or_else(|| {
                config
                    .image
                    .as_ref()
                    .map(|image| image.image.clone())
                    .filter(|image| !image.trim().is_empty())
            })
            .unwrap_or_default();

        let run_as_group = security
            .and_then(|security| security.run_as_group.as_ref())
            .and_then(|group| u32::try_from(group.value).ok());
        let supplemental_groups: Vec<u32> = security
            .map(|security| {
                security
                    .supplemental_groups
                    .iter()
                    .filter_map(|group| u32::try_from(*group).ok())
                    .collect()
            })
            .unwrap_or_default();

        let linux_resources = config
            .linux
            .as_ref()
            .and_then(|linux| linux.resources.as_ref())
            .map(StoredLinuxResources::from);
        let mut stored_annotations = config.annotations.clone();
        Self::enrich_container_annotations(annotations::ContainerAnnotationContext {
            annotations: &mut stored_annotations,
            container_id: &container_id,
            pod_sandbox_id: &pod_sandbox_id,
            metadata_name: config
                .metadata
                .as_ref()
                .map(|metadata| metadata.name.as_str()),
            requested_image: config
                .image
                .as_ref()
                .map(|image| image.user_specified_image.as_str())
                .or_else(|| config.image.as_ref().map(|image| image.image.as_str())),
            resolved_image_name: Some(container_image_ref.as_str()),
            log_path: log_path.as_deref(),
            pod_state: pod_state.as_ref(),
            default_runtime: &self.config.runtime,
        });

        let mut runtime_mounts: Vec<MountConfig> = config
            .mounts
            .iter()
            .map(|m| MountConfig {
                source: PathBuf::from(&m.host_path),
                destination: PathBuf::from(&m.container_path),
                read_only: m.readonly,
            })
            .collect();
        let pod_resolv_path = self
            .config
            .root_dir
            .join("pods")
            .join(&pod_sandbox_id)
            .join("resolv.conf");
        if pod_resolv_path.exists()
            && !runtime_mounts
                .iter()
                .any(|mount| mount.destination == Path::new("/etc/resolv.conf"))
        {
            runtime_mounts.push(MountConfig {
                source: pod_resolv_path.clone(),
                destination: PathBuf::from("/etc/resolv.conf"),
                read_only: true,
            });
        }

        let container_state = StoredContainerState {
            cgroup_parent: sandbox_linux
                .and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                })
                .or_else(|| {
                    pod_state
                        .as_ref()
                        .and_then(|state| state.cgroup_parent.clone())
                }),
            log_path: log_path.as_ref().map(|path| path.display().to_string()),
            tty: config.tty,
            stdin: config.stdin,
            stdin_once: config.stdin_once,
            readonly_rootfs: security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
            privileged: security
                .map(|security| security.privileged)
                .unwrap_or(false),
            network_namespace_path: network_namespace_path
                .as_ref()
                .map(|path| path.display().to_string()),
            run_as_user: security
                .and_then(|security| security.run_as_user.as_ref())
                .map(|user| user.value.to_string())
                .or_else(|| {
                    security.and_then(|security| {
                        if security.run_as_username.is_empty() {
                            None
                        } else {
                            Some(security.run_as_username.clone())
                        }
                    })
                }),
            run_as_group,
            supplemental_groups: supplemental_groups.clone(),
            no_new_privileges: security.map(|security| security.no_new_privs),
            apparmor_profile: apparmor_profile.clone(),
            seccomp_profile: stored_seccomp_profile,
            metadata_name: config
                .metadata
                .as_ref()
                .map(|metadata| metadata.name.clone()),
            metadata_attempt: config.metadata.as_ref().map(|metadata| metadata.attempt),
            started_at: None,
            finished_at: None,
            exit_code: None,
            nri_stop_notified: false,
            nri_remove_notified: false,
            linux_resources,
            mounts: config
                .mounts
                .iter()
                .map(|mount| StoredMount {
                    container_path: mount.container_path.clone(),
                    host_path: mount.host_path.clone(),
                    readonly: mount.readonly,
                    selinux_relabel: mount.selinux_relabel,
                    propagation: mount.propagation,
                })
                .collect(),
        };
        Self::insert_internal_state(
            &mut stored_annotations,
            INTERNAL_CONTAINER_STATE_KEY,
            &container_state,
        )?;
        if let Some(checkpoint_restore) = checkpoint_restore.as_ref() {
            Self::insert_internal_state(
                &mut stored_annotations,
                INTERNAL_CHECKPOINT_RESTORE_KEY,
                checkpoint_restore,
            )?;
        }

        let container_config = ContainerConfig {
            name: config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_else(|| container_id.clone()),
            image: container_image_ref.clone(),
            command: config.command.clone(),
            args: config.args.clone(),
            env: config
                .envs
                .iter()
                .map(|e| (e.key.clone(), e.value.clone()))
                .collect(),
            working_dir: if config.working_dir.is_empty() {
                None
            } else {
                Some(PathBuf::from(&config.working_dir))
            },
            mounts: runtime_mounts,
            labels: config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            annotations: stored_annotations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            privileged: config
                .linux
                .as_ref()
                .map(|l| {
                    l.security_context
                        .as_ref()
                        .map(|s| s.privileged)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            user: config
                .linux
                .as_ref()
                .and_then(|linux| linux.security_context.as_ref())
                .and_then(|security| {
                    security
                        .run_as_user
                        .as_ref()
                        .map(|user| user.value.to_string())
                        .or_else(|| {
                            if security.run_as_username.is_empty() {
                                None
                            } else {
                                Some(security.run_as_username.clone())
                            }
                        })
                }),
            run_as_group,
            supplemental_groups,
            hostname: None,
            tty: config.tty,
            stdin: config.stdin,
            stdin_once: config.stdin_once,
            log_path: log_path.clone(),
            readonly_rootfs: security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
            no_new_privileges: security.map(|security| security.no_new_privs),
            apparmor_profile,
            selinux_label,
            seccomp_profile,
            capabilities: security.and_then(|security| security.capabilities.clone()),
            cgroup_parent: sandbox_linux
                .and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                })
                .or_else(|| {
                    pod_state
                        .as_ref()
                        .and_then(|state| state.cgroup_parent.clone())
                }),
            sysctls: HashMap::new(),
            namespace_options: namespace_options.clone(),
            namespace_paths: NamespacePaths {
                network: network_namespace_path,
                pid: pid_namespace_path,
                ipc: ipc_namespace_path,
                ..Default::default()
            },
            linux_resources: container_state
                .linux_resources
                .as_ref()
                .map(StoredLinuxResources::to_proto),
            devices: config
                .devices
                .iter()
                .map(|device| DeviceMapping {
                    source: PathBuf::from(&device.host_path),
                    destination: PathBuf::from(&device.container_path),
                    permissions: if device.permissions.is_empty() {
                        "rwm".to_string()
                    } else {
                        device.permissions.clone()
                    },
                })
                .collect(),
            rootfs: self
                .config
                .root_dir
                .join("containers")
                .join(&container_id)
                .join("rootfs"),
        };
        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let container_config_clone = container_config.clone();
        tokio::task::spawn_blocking(move || {
            runtime.prepare_rootfs(&requested_container_id, &container_config_clone)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| Status::internal(format!("Failed to prepare container rootfs: {}", e)))?;

        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let container_config_clone = container_config.clone();
        let pristine_spec = tokio::task::spawn_blocking(move || {
            runtime.build_spec(&requested_container_id, &container_config_clone)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| Status::internal(format!("Failed to build pristine OCI spec: {}", e)))?;

        let mut nri_event = self
            .nri_container_event(&pod_sandbox_id, &container_id, &stored_annotations)
            .await;
        nri_event.container.name = container_config.name.clone();
        nri_event.container.state = crate::nri_proto::api::ContainerState::CONTAINER_CREATED.into();
        nri_event.container.labels = config.labels.clone();
        nri_event.container.created_at = Self::now_nanos();
        nri_event.container.args = oci_args(&pristine_spec);
        nri_event.container.env = oci_env(&pristine_spec);
        nri_event.container.mounts = oci_mounts(&pristine_spec);
        if let Some(hooks) = oci_hooks(&pristine_spec) {
            nri_event.container.hooks = protobuf::MessageField::some(hooks);
        }
        if let Some(linux) = oci_linux_container(&pristine_spec) {
            nri_event.container.linux = protobuf::MessageField::some(linux);
        }
        nri_event.container.rlimits = oci_rlimits(&pristine_spec);
        if let Some(user) = oci_user(&pristine_spec) {
            nri_event.container.user = protobuf::MessageField::some(user);
        }

        let mut nri_create_result = self
            .nri
            .create_container(nri_event.clone())
            .await
            .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))?;
        Self::sanitize_nri_adjustment_for_nri_config(
            &mut nri_create_result.adjustment,
            &self.nri_config,
        );
        if let Err(status) = validate_container_adjustment(&nri_create_result.adjustment)
            .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))
        {
            self.rollback_failed_container_create(&container_id, nri_event.clone())
                .await;
            return Err(status);
        }
        if let Err(status) =
            Self::validate_nri_adjustment_runtime_resources(&nri_create_result.adjustment)
        {
            self.rollback_failed_container_create(&container_id, nri_event.clone())
                .await;
            return Err(status);
        }
        let runtime_handler = pod_state
            .as_ref()
            .map(|state| state.runtime_handler.as_str())
            .filter(|handler| !handler.is_empty())
            .unwrap_or(self.config.runtime.as_str());
        nri_create_result.adjustment.annotations = self.filter_nri_annotation_adjustments(
            &nri_activation_annotations,
            &nri_create_result.adjustment.annotations,
            &stored_annotations,
            runtime_handler,
        );
        for update in &nri_create_result.updates {
            if let Err(status) = validate_container_update(update)
                .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))
            {
                self.rollback_failed_container_create(&container_id, nri_event.clone())
                    .await;
                return Err(status);
            }
        }

        if let Err(status) = self
            .process_nri_create_side_effects(&nri_create_result)
            .await
        {
            self.rollback_failed_container_create(&container_id, nri_event.clone())
                .await;
            return Err(status);
        }

        let mut adjusted_spec = pristine_spec.clone();
        apply_container_adjustment_with_blockio_config(
            &mut adjusted_spec,
            &nri_create_result.adjustment,
            Some(&self.nri_config.blockio_config_path),
        )
        .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))?;
        Self::sanitize_spec_runtime_resources(&mut adjusted_spec);
        Self::apply_adjusted_annotations(&mut stored_annotations, &nri_create_result.adjustment);
        Self::refresh_nri_event_container_from_spec(
            &mut nri_event,
            &adjusted_spec,
            &stored_annotations,
        );

        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let rootfs = container_config.rootfs.clone();
        let write_bundle_result = tokio::task::spawn_blocking(move || {
            runtime.write_bundle(&requested_container_id, &rootfs, &adjusted_spec)
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))
        .and_then(|result| {
            result.map_err(|e| Status::internal(format!("Failed to write container bundle: {}", e)))
        });
        if let Err(status) = write_bundle_result {
            self.rollback_failed_container_create(&container_id, nri_event.clone())
                .await;
            return Err(status);
        }

        let created_id = container_id.clone();
        let container = Container {
            id: created_id.clone(),
            pod_sandbox_id: pod_sandbox_id.clone(),
            state: ContainerState::ContainerCreated as i32,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            labels: config.labels.clone(),
            metadata: config.metadata.clone(),
            annotations: stored_annotations.clone(),
            image: config.image.clone().or_else(|| {
                (!container_image_ref.is_empty()).then(|| ImageSpec {
                    image: container_image_ref.clone(),
                    user_specified_image: container_image_ref.clone(),
                    ..Default::default()
                })
            }),
            image_ref: container_image_ref.clone(),
        };

        let mut containers = self.containers.lock().await;
        containers.insert(created_id.clone(), container.clone());
        log::info!(
            "Container stored in memory, total containers: {}",
            containers.len()
        );
        drop(containers);

        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.save_container(
            &created_id,
            &pod_sandbox_id,
            crate::runtime::ContainerStatus::Created,
            &container_image_ref,
            &container_config.command,
            &config.labels,
            &stored_annotations,
        ) {
            log::error!("Failed to persist container {}: {}", created_id, e);
        } else {
            log::info!("Container {} persisted to database", created_id);
        }
        if let Err(err) = self.nri.post_create_container(nri_event.clone()).await {
            log::warn!("NRI PostCreateContainer failed for {}: {}", created_id, err);
        }
        self.emit_container_event(
            ContainerEventType::ContainerCreatedEvent,
            &container,
            Some(ContainerState::ContainerCreated as i32),
        )
        .await;

        Ok(Response::new(CreateContainerResponse {
            container_id: created_id,
        }))
    }

    pub(super) async fn start_container_impl(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let container_id = req.container_id;

        log::info!("Starting container {}", container_id);

        let containers = self.containers.lock().await;
        log::info!(
            "Current containers in memory: {:?}",
            containers.keys().collect::<Vec<_>>()
        );

        let found_container_id = if containers.contains_key(&container_id) {
            Some(container_id.clone())
        } else {
            containers
                .keys()
                .find(|full_id| full_id.starts_with(&container_id))
                .cloned()
        };

        let actual_container_id = match found_container_id {
            Some(id) => id,
            None => {
                log::error!(
                    "Container {} not found in memory. Available containers: {:?}",
                    container_id,
                    containers.keys().collect::<Vec<_>>()
                );
                return Err(Status::not_found("Container not found"));
            }
        };
        drop(containers);
        let (container_pod_id, container_annotations) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (
                container.pod_sandbox_id.clone(),
                container.annotations.clone(),
            )
        };
        let nri_event = self
            .nri_container_event(
                &container_pod_id,
                &actual_container_id,
                &container_annotations,
            )
            .await;
        self.nri
            .start_container(nri_event.clone())
            .await
            .map_err(|e| Status::internal(format!("NRI StartContainer failed: {}", e)))?;

        let checkpoint_restore = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).and_then(|container| {
                Self::read_internal_state::<StoredCheckpointRestore>(
                    &container.annotations,
                    INTERNAL_CHECKPOINT_RESTORE_KEY,
                )
            })
        };

        let runtime = self.runtime.clone();
        let actual_container_id_clone = actual_container_id.clone();
        let checkpoint_restore_for_runtime = checkpoint_restore.clone();
        let start_result = tokio::task::spawn_blocking(move || {
            if let Some(checkpoint_restore) = checkpoint_restore_for_runtime.as_ref() {
                runtime.restore_container_from_checkpoint(
                    &actual_container_id_clone,
                    Path::new(&checkpoint_restore.checkpoint_image_path),
                )
            } else {
                runtime.start_container(&actual_container_id_clone)
            }
        })
        .await
        .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))
        .and_then(|result| {
            result.map_err(|e| Status::internal(format!("Failed to start container: {}", e)))
        });
        if let Err(status) = start_result {
            self.undo_failed_nri_start_container(nri_event.clone())
                .await;
            return Err(status);
        }

        let mut observed_state = ContainerState::ContainerUnknown as i32;
        let mut reached_known_state = false;
        for _ in 0..20 {
            let runtime = self.runtime.clone();
            let container_id_for_status = actual_container_id.clone();
            let current_status = tokio::task::spawn_blocking(move || {
                runtime.container_status(&container_id_for_status)
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .unwrap_or(ContainerStatus::Unknown);

            observed_state = Self::map_runtime_container_state(current_status.clone());
            match current_status {
                ContainerStatus::Unknown => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                _ => {
                    reached_known_state = true;
                    break;
                }
            }
        }

        if !reached_known_state {
            self.undo_failed_nri_start_container(nri_event.clone())
                .await;
            return Err(Status::internal(
                "Container failed to reach a known runtime state after start",
            ));
        }

        {
            let mut containers = self.containers.lock().await;
            if let Some(container) = containers.get_mut(&actual_container_id) {
                container.state = observed_state;
            }
        }
        let updated_container = self
            .mutate_container_internal_state(&actual_container_id, |state| match observed_state {
                x if x == ContainerState::ContainerRunning as i32 => {
                    state.started_at = Some(Self::now_nanos());
                    state.finished_at = None;
                    state.exit_code = None;
                    state.nri_stop_notified = false;
                }
                x if x == ContainerState::ContainerExited as i32 => {
                    state.finished_at = Some(Self::now_nanos());
                    state.exit_code.get_or_insert(0);
                }
                _ => {}
            })
            .await?;
        if let Some(container) = updated_container.as_ref() {
            self.persist_bundle_annotations(&actual_container_id, &container.annotations)?;
        }

        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.update_container_state(
            &actual_container_id,
            match observed_state {
                x if x == ContainerState::ContainerRunning as i32 => {
                    crate::runtime::ContainerStatus::Running
                }
                x if x == ContainerState::ContainerCreated as i32 => {
                    crate::runtime::ContainerStatus::Created
                }
                x if x == ContainerState::ContainerExited as i32 => {
                    crate::runtime::ContainerStatus::Stopped(0)
                }
                _ => crate::runtime::ContainerStatus::Unknown,
            },
        ) {
            log::error!(
                "Failed to update container {} state in database: {}",
                container_id,
                e
            );
        }
        drop(persistence);
        if checkpoint_restore.is_some() {
            let updated_annotations = {
                let mut containers = self.containers.lock().await;
                containers.get_mut(&actual_container_id).map(|container| {
                    container
                        .annotations
                        .remove(INTERNAL_CHECKPOINT_RESTORE_KEY);
                    container.annotations.clone()
                })
            };
            if let Some(updated_annotations) = updated_annotations {
                self.persist_container_annotations(&actual_container_id, &updated_annotations)
                    .await?;
                self.persist_bundle_annotations(&actual_container_id, &updated_annotations)?;
            }
        }

        log::info!("Container {} started", container_id);
        if observed_state == ContainerState::ContainerCreated as i32
            || observed_state == ContainerState::ContainerRunning as i32
        {
            self.ensure_exit_monitor_registered(&actual_container_id);
        }
        if let Some(container) = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
        } {
            let post_start_event = self
                .nri_container_event(
                    &container_pod_id,
                    &actual_container_id,
                    &container.annotations,
                )
                .await;
            if let Err(err) = self.nri.post_start_container(post_start_event).await {
                log::warn!(
                    "NRI PostStartContainer failed for {}: {}",
                    actual_container_id,
                    err
                );
            }
            self.emit_container_event(
                ContainerEventType::ContainerStartedEvent,
                &container,
                Some(observed_state),
            )
            .await;
        }
        Ok(Response::new(StartContainerResponse {}))
    }
}
