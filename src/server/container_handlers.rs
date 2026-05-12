use super::service::NameReservationGuard;
use super::*;

impl RuntimeServiceImpl {
    fn merge_default_env(&self, requested: &[(String, String)]) -> Vec<(String, String)> {
        let mut merged = self.config.default_env.clone();
        for (key, value) in requested {
            if let Some((_, existing_value)) =
                merged.iter_mut().find(|(existing, _)| existing == key)
            {
                *existing_value = value.clone();
            } else {
                merged.push((key.clone(), value.clone()));
            }
        }
        merged
    }

    fn start_container_precondition_error(container_id: &str, runtime_state: i32) -> Status {
        match runtime_state {
            x if x == ContainerState::ContainerRunning as i32 => Status::failed_precondition(
                format!("container {} is already running", container_id),
            ),
            x if x == ContainerState::ContainerExited as i32 => Status::failed_precondition(
                format!("container {} has already exited", container_id),
            ),
            x if x == ContainerState::ContainerCreated as i32 => Status::failed_precondition(
                format!("container {} is already created", container_id),
            ),
            _ => Status::failed_precondition(format!(
                "container {} is not in created state (current state: {})",
                container_id,
                Self::runtime_state_name(runtime_state)
            )),
        }
    }

    fn create_container_sandbox_not_ready_error(
        pod_sandbox_id: &str,
        sandbox_state: i32,
    ) -> Status {
        let state_name = match sandbox_state {
            x if x == PodSandboxState::SandboxReady as i32 => "ready",
            x if x == PodSandboxState::SandboxNotready as i32 => "notready",
            _ => "unknown",
        };
        Status::failed_precondition(format!(
            "CreateContainer failed as the sandbox is not ready: {} (state: {})",
            pod_sandbox_id, state_name
        ))
    }

    pub(super) async fn reserve_container_name_for_create(
        &self,
        container_id: &str,
        name: &str,
    ) -> Result<NameReservationGuard, Status> {
        match self.reserve_container_name(container_id, name) {
            Ok(guard) => Ok(guard),
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                let Some(existing_id) = self.container_id_for_reserved_name(name) else {
                    return Err(status);
                };

                let existing_container = {
                    let containers = self.containers.lock().await;
                    containers.get(&existing_id).cloned()
                };
                let in_memory = existing_container.is_some();
                let persisted_record = {
                    let persistence = self.persistence.lock().await;
                    persistence
                        .storage()
                        .get_container(&existing_id)
                        .ok()
                        .flatten()
                };
                let in_storage = persisted_record.is_some();

                if !in_memory && !in_storage {
                    self.release_container_name(&existing_id);
                    return self.reserve_container_name(container_id, name);
                }

                let runtime_status = self.runtime_container_status_checked(&existing_id).await;
                let runtime_pid = self.runtime_container_pid_checked(&existing_id).await;
                let has_active_runtime_state = self
                    .runtime
                    .container_has_active_runtime_state(&existing_id);
                let stored_runtime_status = existing_container
                    .as_ref()
                    .map(|container| match container.state {
                        x if x == ContainerState::ContainerRunning as i32 => {
                            crate::runtime::ContainerStatus::Running
                        }
                        x if x == ContainerState::ContainerCreated as i32 => {
                            crate::runtime::ContainerStatus::Created
                        }
                        x if x == ContainerState::ContainerExited as i32 => {
                            let exit_code = Self::read_internal_state::<StoredContainerState>(
                                &container.annotations,
                                INTERNAL_CONTAINER_STATE_KEY,
                            )
                            .and_then(|state| state.exit_code)
                            .unwrap_or(-1);
                            crate::runtime::ContainerStatus::Stopped(exit_code)
                        }
                        _ => crate::runtime::ContainerStatus::Unknown,
                    })
                    .or_else(|| {
                        persisted_record
                            .as_ref()
                            .map(crate::storage::persistence::record_to_container_status)
                    })
                    .unwrap_or(crate::runtime::ContainerStatus::Unknown);
                let effective_state = if matches!(runtime_status, ContainerStatus::Unknown) {
                    Self::map_runtime_container_state(stored_runtime_status)
                } else {
                    Self::map_runtime_container_state(runtime_status.clone())
                };
                if effective_state == ContainerState::ContainerExited as i32
                    && !has_active_runtime_state
                    && runtime_pid.is_none()
                {
                    if in_memory {
                        let _ = self.remove_container_internal(&existing_id).await?;
                    } else {
                        let runtime = self.runtime.clone();
                        let existing_id_for_remove = existing_id.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            runtime.remove_container(&existing_id_for_remove)
                        })
                        .await;
                        let mut persistence = self.persistence.lock().await;
                        let _ = persistence.delete_container(&existing_id);
                        self.release_container_name(&existing_id);
                    }
                    return self.reserve_container_name(container_id, name);
                }
                Err(status)
            }
            Err(status) => Err(status),
        }
    }

    pub(super) fn resolve_container_log_path(
        sandbox_log_directory: Option<&str>,
        container_log_path: &str,
    ) -> Result<Option<PathBuf>, Status> {
        let log_path = container_log_path.trim();
        if log_path.is_empty() {
            return Ok(None);
        }

        let path = Path::new(log_path);
        let sandbox_log_directory = sandbox_log_directory
            .map(str::trim)
            .filter(|dir| !dir.is_empty());

        if let Some(log_directory) = sandbox_log_directory {
            if path.is_absolute() {
                return Err(Status::invalid_argument(
                    "container log_path must be relative when sandbox log_directory is set",
                ));
            }
            if path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            }) {
                return Err(Status::invalid_argument(
                    "container log_path must not escape the sandbox log_directory",
                ));
            }
            return Ok(Some(PathBuf::from(log_directory).join(path)));
        }

        if !path.is_absolute() {
            return Err(Status::invalid_argument(
                "container log_path must be absolute when sandbox log_directory is not set",
            ));
        }

        Ok(Some(path.to_path_buf()))
    }

    pub(super) fn runtime_mounts_from_proto(
        &self,
        mounts: &[crate::proto::runtime::v1::Mount],
        create_missing_bind_mount_sources: bool,
    ) -> Result<Vec<MountConfig>, Status> {
        let mut runtime_mounts = Vec::new();
        for mount in mounts {
            if mount.recursive_read_only && !mount.readonly {
                return Err(Status::invalid_argument(format!(
                    "mount {} sets recursive_read_only=true but readonly=false",
                    mount.container_path
                )));
            }
            if mount.recursive_read_only
                && mount.propagation
                    != crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32
            {
                return Err(Status::invalid_argument(format!(
                    "mount {} sets recursive_read_only=true but propagation is not private",
                    mount.container_path
                )));
            }

            let propagation = match mount.propagation {
                x if x
                    == crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32 =>
                {
                    crate::runtime::MountPropagationMode::Private
                }
                x if x
                    == crate::proto::runtime::v1::MountPropagation::PropagationHostToContainer
                        as i32 =>
                {
                    crate::runtime::MountPropagationMode::HostToContainer
                }
                x if x
                    == crate::proto::runtime::v1::MountPropagation::PropagationBidirectional
                        as i32 =>
                {
                    crate::runtime::MountPropagationMode::Bidirectional
                }
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "mount {} sets unsupported propagation value {}",
                        mount.container_path, mount.propagation
                    )));
                }
            };

            if let Some(image) = mount.image.as_ref() {
                if !mount.host_path.trim().is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "mount {} must not set both host_path and image",
                        mount.container_path
                    )));
                }
                if !self.config.image_oci_artifact_mount_support {
                    return Err(Status::failed_precondition(
                        "OCI artifact image volume mounts are disabled by image.oci_artifact_mount_support",
                    ));
                }
                let container_root = PathBuf::from(mount.container_path.trim());
                if !container_root.is_absolute() {
                    return Err(Status::invalid_argument(format!(
                        "OCI artifact mount container_path must be absolute: {}",
                        mount.container_path
                    )));
                }
                if container_root.extension().is_some() {
                    return Err(Status::failed_precondition(format!(
                        "OCI artifact mount container_path must reference a directory, got {}",
                        mount.container_path
                    )));
                }

                let resolved = crate::image::ImageServiceImpl::resolve_artifact_mounts(
                    &self.config.image_root,
                    &self.config.image_additional_artifact_stores,
                    image.image.as_str(),
                    (!mount.image_sub_path.trim().is_empty())
                        .then_some(mount.image_sub_path.as_str()),
                )?;
                for entry in resolved {
                    runtime_mounts.push(MountConfig {
                        source: entry.source,
                        destination: container_root.join(entry.relative_path),
                        read_only: true,
                        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
                        selinux_relabel: mount.selinux_relabel,
                        propagation,
                        recursive_read_only: false,
                        uid_mappings: mount
                            .uid_mappings
                            .iter()
                            .map(|mapping| crate::oci::spec::IdMapping {
                                container_id: mapping.container_id,
                                host_id: mapping.host_id,
                                size: mapping.length,
                            })
                            .collect(),
                        gid_mappings: mount
                            .gid_mappings
                            .iter()
                            .map(|mapping| crate::oci::spec::IdMapping {
                                container_id: mapping.container_id,
                                host_id: mapping.host_id,
                                size: mapping.length,
                            })
                            .collect(),
                        requested_image: Some(image.image.clone()),
                        image_sub_path: (!mount.image_sub_path.trim().is_empty())
                            .then(|| mount.image_sub_path.clone()),
                    });
                }
                continue;
            }

            if mount.host_path.trim().is_empty() {
                return Err(Status::invalid_argument(format!(
                    "mount {} must set host_path when image is not specified",
                    mount.container_path
                )));
            }

            runtime_mounts.push(MountConfig {
                source: PathBuf::from(&mount.host_path),
                destination: PathBuf::from(&mount.container_path),
                read_only: mount.readonly,
                missing_source_policy: if create_missing_bind_mount_sources {
                    crate::runtime::MissingMountSourcePolicy::CreateDirectory
                } else {
                    crate::runtime::MissingMountSourcePolicy::Reject
                },
                selinux_relabel: mount.selinux_relabel,
                propagation,
                recursive_read_only: mount.recursive_read_only,
                uid_mappings: mount
                    .uid_mappings
                    .iter()
                    .map(|mapping| crate::oci::spec::IdMapping {
                        container_id: mapping.container_id,
                        host_id: mapping.host_id,
                        size: mapping.length,
                    })
                    .collect(),
                gid_mappings: mount
                    .gid_mappings
                    .iter()
                    .map(|mapping| crate::oci::spec::IdMapping {
                        container_id: mapping.container_id,
                        host_id: mapping.host_id,
                        size: mapping.length,
                    })
                    .collect(),
                requested_image: None,
                image_sub_path: None,
            });
        }

        Ok(runtime_mounts)
    }

    async fn ensure_container_loaded(&self, container_id: &str) -> Result<bool, Status> {
        let already_loaded = {
            let containers = self.containers.lock().await;
            containers.contains_key(container_id)
        };
        if already_loaded {
            return Ok(false);
        }

        let persisted = {
            let persistence = self.persistence.lock().await;
            persistence
                .storage()
                .get_container(container_id)
                .map_err(|e| {
                    Status::internal(format!(
                        "Failed to load persisted container {}: {}",
                        container_id, e
                    ))
                })?
        };
        if let Some(record) = persisted {
            let rehydrated = Self::container_from_record(&record);
            let mut containers = self.containers.lock().await;
            containers.insert(container_id.to_string(), rehydrated);
            return Ok(true);
        }

        Ok(false)
    }

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
        self.remove_seccomp_notifier(container_id);
        self.release_container_name(container_id);
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
                match &final_runtime_status {
                    ContainerStatus::Created => {}
                    ContainerStatus::Running => {
                        state.finished_at = None;
                        state.exit_code = None;
                    }
                    ContainerStatus::Stopped(_) | ContainerStatus::Unknown => {
                        state.finished_at.get_or_insert(Self::now_nanos());
                        if resolved_exit_code.is_none() {
                            resolved_exit_code = state.exit_code;
                        }
                        if let Some(code) = resolved_exit_code {
                            state.exit_code = Some(code);
                        }
                    }
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
                crate::runtime::ContainerStatus::Stopped(resolved_exit_code.unwrap_or(-1))
            }
            ContainerStatus::Unknown => match resolved_exit_code {
                Some(code) => crate::runtime::ContainerStatus::Stopped(code),
                None => crate::runtime::ContainerStatus::Unknown,
            },
        };
        let persistence = self.persistence.clone();
        let container_id_owned = container_id.to_string();
        tokio::spawn(async move {
            let mut persistence = persistence.lock().await;
            if let Err(err) =
                persistence.update_container_state(&container_id_owned, persistence_status)
            {
                log::error!(
                    "Failed to update container {} state in database: {}",
                    container_id_owned,
                    err
                );
            }
        });

        Ok(updated_container)
    }

    async fn finalize_failed_container_start_state(
        &self,
        container_id: &str,
        final_runtime_status: ContainerStatus,
    ) -> Result<Option<Container>, Status> {
        let updated_container = {
            let mut containers = self.containers.lock().await;
            let Some(container) = containers.get_mut(container_id) else {
                return Ok(None);
            };

            container.state = match final_runtime_status {
                ContainerStatus::Created => ContainerState::ContainerCreated as i32,
                ContainerStatus::Running => ContainerState::ContainerRunning as i32,
                ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
                ContainerStatus::Unknown => ContainerState::ContainerCreated as i32,
            };

            let mut state = Self::read_internal_state::<StoredContainerState>(
                &container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
            )
            .unwrap_or_default();
            match final_runtime_status {
                ContainerStatus::Created => {
                    state.started_at = None;
                    state.finished_at = None;
                    state.exit_code = None;
                }
                ContainerStatus::Running => {
                    state.started_at.get_or_insert(Self::now_nanos());
                    state.finished_at = None;
                }
                ContainerStatus::Stopped(code) => {
                    state.started_at.get_or_insert(Self::now_nanos());
                    state.finished_at.get_or_insert(Self::now_nanos());
                    state.exit_code = Some(code);
                }
                ContainerStatus::Unknown => {}
            }
            Self::insert_internal_state(
                &mut container.annotations,
                INTERNAL_CONTAINER_STATE_KEY,
                &state,
            )?;

            Some(container.clone())
        };

        if let Some(container) = &updated_container {
            self.persist_container_annotations(container_id, &container.annotations)
                .await?;
            self.persist_bundle_annotations(container_id, &container.annotations)?;
        }

        let persistence_status = match final_runtime_status {
            ContainerStatus::Created => crate::runtime::ContainerStatus::Created,
            ContainerStatus::Running => crate::runtime::ContainerStatus::Running,
            ContainerStatus::Stopped(code) => crate::runtime::ContainerStatus::Stopped(code),
            ContainerStatus::Unknown => crate::runtime::ContainerStatus::Created,
        };
        let mut persistence = self.persistence.lock().await;
        if let Err(err) = persistence.update_container_state(container_id, persistence_status) {
            log::error!(
                "Failed to update failed-start container {} state in database: {}",
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
        if !needs_nri_stop {
            let updated_container =
                if matches!(runtime_status_before_stop, ContainerStatus::Unknown) {
                    self.finalize_container_stop_state(
                        actual_container_id,
                        ContainerStatus::Stopped(-1),
                    )
                    .await?
                } else {
                    let mut containers = self.containers.lock().await;
                    let Some(container) = containers.get_mut(actual_container_id) else {
                        return Ok(None);
                    };
                    let next_state =
                        Self::map_runtime_container_state(runtime_status_before_stop.clone());
                    if next_state != ContainerState::ContainerUnknown as i32 {
                        container.state = next_state;
                    }
                    Some(container.clone())
                };
            return Ok(updated_container);
        }

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
        if let Ok(mut removed) = self.removed_container_ids.lock() {
            removed.insert(actual_container_id.to_string());
        }
        self.remove_seccomp_notifier(actual_container_id);
        self.release_container_name(actual_container_id);

        let persistence = self.persistence.clone();
        let container_id_for_delete = actual_container_id.to_string();
        tokio::spawn(async move {
            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence.delete_container(&container_id_for_delete) {
                log::error!(
                    "Failed to delete container {} from database: {}",
                    container_id_for_delete,
                    err
                );
            } else {
                log::info!(
                    "Container {} removed from database",
                    container_id_for_delete
                );
            }
        });

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
        event.container.annotations = Self::external_container_annotations(annotations);
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
            container.annotations = Self::external_container_annotations(annotations);
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
        let timeout = self.effective_container_stop_timeout(req.timeout as u32);

        log::info!("Stopping container {}", container_id);

        let Some(actual_container_id) = self.resolve_container_id_if_exists(&container_id).await?
        else {
            return Ok(Response::new(StopContainerResponse {}));
        };
        self.ensure_container_loaded(&actual_container_id).await?;
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
        self.ensure_container_loaded(&actual_container_id).await?;
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
        if !self.config.enable_criu_support {
            return Err(self.criu_support_disabled_status("checkpoint"));
        }
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
        let checkpoint_image_path = self.checkpoint_runtime_image_path(&checkpoint_location);
        let checkpoint_work_path = self.checkpoint_runtime_work_path(&checkpoint_location);
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
        let checkpoint_work_path_for_runtime = checkpoint_work_path.clone();
        let checkpoint_future = tokio::task::spawn_blocking(move || {
            runtime.checkpoint_container(
                &container_id_for_checkpoint,
                &checkpoint_image_path_for_runtime,
                &checkpoint_work_path_for_runtime,
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
            "rootfsSnapshot": {
                "path": "rootfs.tar",
                "format": "tar",
                "restorePolicy": "replace",
            },
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
        if self.config.disable_cgroup {
            return Err(self.cgroup_updates_disabled_status());
        }
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
        let is_paused = matches!(runtime_status, ContainerStatus::Unknown)
            && self
                .runtime
                .is_container_paused(&container_id)
                .unwrap_or(false);
        if !matches!(
            runtime_status,
            ContainerStatus::Running | ContainerStatus::Created
        ) && !is_paused
        {
            return Err(Status::failed_precondition(format!(
                "container {} is not in a mutable state",
                container_id
            )));
        }

        let observed_state = if is_paused {
            ContainerState::ContainerRunning as i32
        } else {
            Self::map_runtime_container_state(runtime_status)
        };
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
        let cgroup_support = Self::cgroup_support_flags();
        Self::validate_stored_hugetlb_limits_with_flags(
            Some(&final_resources),
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
            "container resource update",
        )?;
        Self::sanitize_stored_runtime_resources_with_policy(
            &mut final_resources,
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
        );
        if self.nri_config.blockio_config_path.trim().is_empty() {
            final_resources.blockio_class = None;
        }
        if !Path::new("/sys/fs/resctrl").exists() {
            final_resources.rdt_class = None;
        }
        self.clamp_stored_oom_score_adj(&mut final_resources)?;

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
        let container_metadata = config
            .metadata
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Container config metadata not specified"))?;
        Self::validate_container_image_spec(&config)?;
        let sandbox_config = req.sandbox_config;
        let pod_metadata = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            let pod = pod_sandboxes
                .get(&pod_sandbox_id)
                .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
            if pod.state != PodSandboxState::SandboxReady as i32 {
                return Err(Self::create_container_sandbox_not_ready_error(
                    &pod_sandbox_id,
                    pod.state,
                ));
            }
            pod.metadata
                .clone()
                .ok_or_else(|| Status::failed_precondition("Pod sandbox metadata is missing"))?
        };

        let container_id = uuid::Uuid::new_v4().to_simple().to_string();
        let container_name_key = Self::container_name_key(container_metadata, &pod_metadata);
        let mut container_name_guard = self
            .reserve_container_name_for_create(&container_id, &container_name_key)
            .await?;

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
                    .map(|pod| Self::external_pod_annotations(&pod.annotations))
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
        let sandbox_namespace_options = sandbox_linux
            .and_then(|linux| linux.security_context.as_ref())
            .and_then(|security| security.namespace_options.as_ref())
            .map(StoredNamespaceOptions::from)
            .or_else(|| {
                pod_state
                    .as_ref()
                    .and_then(|state| state.namespace_options.clone())
            });
        let namespace_options = self.effective_container_namespace_options(
            security.and_then(|security| security.namespace_options.as_ref()),
            sandbox_namespace_options.as_ref(),
        );
        let run_as_user = security
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
            });
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
        self.validate_minimum_mappable_ids(
            namespace_options.as_ref(),
            run_as_user.as_deref(),
            run_as_group,
            &supplemental_groups,
        )?;

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
        let log_path =
            Self::resolve_container_log_path(pod_log_directory.as_deref(), &config.log_path)?;

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

        let container_privileged = security
            .map(|security| security.privileged)
            .unwrap_or(false);
        let apparmor_profile = self.effective_apparmor_profile_from_proto(
            security.and_then(|security| security.apparmor.as_ref()),
            Self::legacy_linux_container_apparmor_profile(security),
            container_privileged,
        )?;
        let host_network = network_namespace_path.is_none();
        let selinux_label = self.effective_selinux_label_from_proto(
            security.and_then(|ctx| ctx.selinux_options.as_ref()),
            host_network,
            Some(&pod_sandbox_id),
        );
        let seccomp_profile = self.effective_seccomp_profile_from_proto(
            security.and_then(|ctx| ctx.seccomp.as_ref()),
            Self::legacy_linux_container_seccomp_profile_path(security),
            container_privileged,
        );
        let stored_seccomp_profile = self.effective_stored_seccomp_profile_from_proto(
            security.and_then(|ctx| ctx.seccomp.as_ref()),
            Self::legacy_linux_container_seccomp_profile_path(security),
            container_privileged,
        );
        let requested_runtime_handler = pod_state
            .as_ref()
            .map(|state| state.runtime_handler.as_str())
            .filter(|handler| !handler.is_empty())
            .unwrap_or(self.config.runtime.as_str());
        let seccomp_notifier_mode = if matches!(seccomp_profile, Some(SeccompProfile::Unconfined)) {
            None
        } else {
            self.seccomp_notifier_action_from_annotations(
                &nri_activation_annotations,
                requested_runtime_handler,
            )?
        };
        let seccomp_notifier = seccomp_notifier_mode
            .map(|mode| {
                self.ensure_seccomp_notifier(&container_id, mode)
                    .map(|listener_path| crate::runtime::SeccompNotifierConfig {
                        listener_path,
                        listener_metadata: format!("container={container_id}"),
                        mode,
                    })
            })
            .transpose()?;
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
            .map(|checkpoint_location| self.checkpoint_restore_from_artifact(checkpoint_location))
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

        let readonly_rootfs = self.effective_readonly_rootfs(
            security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
        );

        let mut linux_resources = config
            .linux
            .as_ref()
            .and_then(|linux| linux.resources.as_ref())
            .map(StoredLinuxResources::from);
        match self.effective_pids_limit(
            linux_resources
                .as_ref()
                .and_then(|resources| resources.pids_limit),
        )? {
            Some(limit) => {
                linux_resources
                    .get_or_insert_with(Default::default)
                    .pids_limit = Some(limit);
            }
            None => {
                if let Some(resources) = linux_resources.as_mut() {
                    resources.pids_limit = None;
                }
            }
        }
        if let Some(resources) = linux_resources.as_mut() {
            self.clamp_stored_oom_score_adj(resources)?;
        }
        let cgroup_support = Self::cgroup_support_flags();
        Self::validate_stored_hugetlb_limits_with_flags(
            linux_resources.as_ref(),
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
            "container create",
        )?;
        if let Some(workload_resources) = self.workload_resources_from_annotation(
            &nri_activation_annotations,
            &container_metadata.name,
        )? {
            let resources = linux_resources.get_or_insert_with(Default::default);
            Self::apply_workload_resources(resources, &workload_resources);
        }
        if let Some(resources) = linux_resources.as_mut() {
            Self::sanitize_stored_runtime_resources_with_policy(
                resources,
                cgroup_support,
                self.config.tolerate_missing_hugetlb_controller,
            );
        }
        let mut stored_annotations = config.annotations.clone();
        if let Some(raw) =
            nri_activation_annotations.get(CRIO_SECCOMP_NOTIFIER_ACTION_V2_ANNOTATION)
        {
            stored_annotations.insert(
                CRIO_SECCOMP_NOTIFIER_ACTION_V2_ANNOTATION.to_string(),
                raw.clone(),
            );
        } else if let Some(raw) =
            nri_activation_annotations.get(CRIO_SECCOMP_NOTIFIER_ACTION_ANNOTATION)
        {
            stored_annotations.insert(
                CRIO_SECCOMP_NOTIFIER_ACTION_ANNOTATION.to_string(),
                raw.clone(),
            );
        }
        let runtime_handler = pod_state
            .as_ref()
            .map(|state| state.runtime_handler.as_str())
            .filter(|handler| !handler.is_empty())
            .unwrap_or(self.config.runtime.as_str());
        self.apply_runtime_handler_default_annotations(&mut stored_annotations, runtime_handler);
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

        let mut runtime_mounts =
            self.runtime_mounts_from_proto(&config.mounts, checkpoint_restore.is_none())?;
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
                missing_source_policy: crate::runtime::MissingMountSourcePolicy::Ignore,
                selinux_relabel: false,
                propagation: crate::runtime::MountPropagationMode::Private,
                recursive_read_only: false,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                requested_image: None,
                image_sub_path: None,
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
            readonly_rootfs,
            privileged: container_privileged,
            network_namespace_path: network_namespace_path
                .as_ref()
                .map(|path| path.display().to_string()),
            run_as_user: run_as_user.clone(),
            run_as_group,
            supplemental_groups: supplemental_groups.clone(),
            no_new_privileges: security.map(|security| security.no_new_privs),
            apparmor_profile: apparmor_profile.clone(),
            seccomp_profile: stored_seccomp_profile,
            seccomp_notifier_action: seccomp_notifier_mode.map(|mode| match mode {
                crate::runtime::SeccompNotifierMode::Log => "log".to_string(),
                crate::runtime::SeccompNotifierMode::Stop => "stop".to_string(),
            }),
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
                    image: mount
                        .image
                        .as_ref()
                        .map(|image| image.image.clone())
                        .unwrap_or_default(),
                    image_sub_path: mount.image_sub_path.clone(),
                    readonly: mount.readonly,
                    selinux_relabel: mount.selinux_relabel,
                    propagation: mount.propagation,
                })
                .collect(),
        };
        log::info!(
            "CreateContainer resolved cgroup_parent for container {} in sandbox {}: {:?} (host_network={})",
            container_id,
            pod_sandbox_id,
            container_state.cgroup_parent,
            host_network
        );
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
            env: self.merge_default_env(
                &config
                    .envs
                    .iter()
                    .map(|e| (e.key.clone(), e.value.clone()))
                    .collect::<Vec<_>>(),
            ),
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
            privileged: container_privileged,
            user: run_as_user,
            run_as_group,
            supplemental_groups,
            hostname: None,
            tty: config.tty,
            stdin: config.stdin,
            stdin_once: config.stdin_once,
            log_path: log_path.clone(),
            readonly_rootfs,
            seccomp_notifier,
            pids_limit: container_state
                .linux_resources
                .as_ref()
                .and_then(|resources| resources.pids_limit),
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
            sysctls: self.config.default_sysctls.clone(),
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
            masked_paths: security
                .map(|security| security.masked_paths.clone())
                .unwrap_or_default(),
            readonly_paths: security
                .map(|security| security.readonly_paths.clone())
                .unwrap_or_default(),
            rootfs: self
                .config
                .root_dir
                .join("containers")
                .join(&container_id)
                .join("rootfs"),
        };
        self.runtime
            .runtime_for_handler(runtime_handler)
            .map_err(|e| {
                Status::failed_precondition(format!("Failed to resolve runtime handler: {}", e))
            })?
            .validate_mount_requests(&container_config)
            .map_err(|e| e.to_status())?;
        let create_deadline = self.container_create_deadline_for_handler(runtime_handler);
        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let container_config_clone = container_config.clone();
        self.run_container_create_phase_until(create_deadline, "prepare_rootfs", async move {
            tokio::task::spawn_blocking(move || {
                runtime.prepare_rootfs(&requested_container_id, &container_config_clone)
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .map_err(|e| Status::internal(format!("Failed to prepare container rootfs: {}", e)))
        })
        .await?;

        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let container_config_clone = container_config.clone();
        let pristine_spec = self
            .run_container_create_phase_until(create_deadline, "build_spec", async move {
                tokio::task::spawn_blocking(move || {
                    runtime.build_spec(&requested_container_id, &container_config_clone)
                })
                .await
                .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
                .map_err(|e| Status::internal(format!("Failed to build pristine OCI spec: {}", e)))
            })
            .await?;

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
        )?;
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
        crate::nri::apply_container_adjustment_with_options(
            &mut adjusted_spec,
            &nri_create_result.adjustment,
            crate::nri::AdjustmentOptions {
                blockio_config_path: Some(&self.nri_config.blockio_config_path),
                cdi_enabled: self.nri_config.enable_cdi,
                cdi_spec_dirs: Some(&self.nri_config.cdi_spec_dirs),
            },
        )
        .map_err(|e| Status::internal(format!("NRI CreateContainer failed: {}", e)))?;
        let cgroup_support = Self::cgroup_support_flags();
        Self::validate_spec_hugetlb_limits_with_flags(
            &adjusted_spec,
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
            "container create",
        )?;
        Self::sanitize_spec_runtime_resources_with_policy(
            &mut adjusted_spec,
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
        );
        self.runtime
            .enforce_oom_score_adj_policy(&container_id, &mut adjusted_spec)
            .map_err(|e| {
                Status::internal(format!("Failed to enforce oom_score_adj policy: {}", e))
            })?;
        Self::apply_adjusted_annotations(&mut stored_annotations, &nri_create_result.adjustment);
        Self::refresh_nri_event_container_from_spec(
            &mut nri_event,
            &adjusted_spec,
            &stored_annotations,
        );

        let runtime = self.runtime.clone();
        let requested_container_id = container_id.clone();
        let rootfs = container_config.rootfs.clone();
        let write_bundle_result = self
            .run_container_create_phase_until(create_deadline, "write_bundle", async move {
                tokio::task::spawn_blocking(move || {
                    runtime.write_bundle(&requested_container_id, &rootfs, &adjusted_spec)
                })
                .await
                .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))
                .and_then(|result| {
                    result.map_err(|e| {
                        Status::internal(format!("Failed to write container bundle: {}", e))
                    })
                })
            })
            .await;
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

        let persistence = self.persistence.clone();
        let containers = self.containers.clone();
        let removed_container_ids = self.removed_container_ids.clone();
        let created_id_for_persist = created_id.clone();
        let pod_sandbox_id_for_persist = pod_sandbox_id.clone();
        let container_image_ref_for_persist = container_image_ref.clone();
        let command_for_persist = container_config.command.clone();
        let runtime_handler_for_persist = runtime_handler.to_string();
        tokio::spawn(async move {
            if removed_container_ids
                .lock()
                .ok()
                .map(|removed| removed.contains(&created_id_for_persist))
                .unwrap_or(false)
            {
                return;
            }

            let Some(current_container) = ({
                let containers = containers.lock().await;
                containers.get(&created_id_for_persist).cloned()
            }) else {
                return;
            };

            let state = match current_container.state {
                x if x == ContainerState::ContainerCreated as i32 => {
                    crate::runtime::ContainerStatus::Created
                }
                x if x == ContainerState::ContainerRunning as i32 => {
                    crate::runtime::ContainerStatus::Running
                }
                x if x == ContainerState::ContainerExited as i32 => {
                    let exit_code =
                        RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
                            &current_container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                        )
                        .and_then(|state| state.exit_code)
                        .unwrap_or_default();
                    crate::runtime::ContainerStatus::Stopped(exit_code)
                }
                _ => crate::runtime::ContainerStatus::Unknown,
            };

            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence.save_container(
                &created_id_for_persist,
                &pod_sandbox_id_for_persist,
                state,
                &container_image_ref_for_persist,
                &command_for_persist,
                &current_container.labels,
                &current_container.annotations,
            ) {
                log::error!(
                    "Failed to persist container {}: {}",
                    created_id_for_persist,
                    err
                );
            } else {
                if let Err(err) = persistence.update_container_ledger_metadata(
                    &created_id_for_persist,
                    Some(&runtime_handler_for_persist),
                    Some("runc"),
                    Some(&created_id_for_persist),
                ) {
                    log::error!(
                        "Failed to persist ledger metadata for container {}: {}",
                        created_id_for_persist,
                        err
                    );
                }
                log::info!("Container {} persisted to database", created_id_for_persist);
            }
        });
        if let Err(err) = self.nri.post_create_container(nri_event.clone()).await {
            log::warn!("NRI PostCreateContainer failed for {}: {}", created_id, err);
        }
        self.emit_container_event(
            ContainerEventType::ContainerCreatedEvent,
            &container,
            Some(ContainerState::ContainerCreated as i32),
        )
        .await;
        container_name_guard.disarm();

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

        let Some(actual_container_id) = self.resolve_container_id_if_exists(&container_id).await?
        else {
            return Err(Status::not_found("Container not found"));
        };
        self.ensure_container_loaded(&actual_container_id).await?;

        let runtime_status_before_start = self
            .runtime_container_status_checked(&actual_container_id)
            .await;
        let (container_pod_id, container_annotations, container_state) = {
            let containers = self.containers.lock().await;
            let container = containers
                .get(&actual_container_id)
                .ok_or_else(|| Status::not_found("Container not found"))?;
            (
                container.pod_sandbox_id.clone(),
                container.annotations.clone(),
                container.state,
            )
        };
        if let Some(mode) = self
            .container_internal_state(&actual_container_id)
            .await
            .and_then(|state| state.seccomp_notifier_action)
            .and_then(|action| match action.as_str() {
                "stop" => Some(crate::runtime::SeccompNotifierMode::Stop),
                "log" | "" => Some(crate::runtime::SeccompNotifierMode::Log),
                _ => None,
            })
        {
            let _ = self.ensure_seccomp_notifier(&actual_container_id, mode)?;
        }

        if container_state != ContainerState::ContainerCreated as i32 {
            let effective_state = if matches!(runtime_status_before_start, ContainerStatus::Unknown)
            {
                container_state
            } else {
                let runtime_state =
                    Self::map_runtime_container_state(runtime_status_before_start.clone());
                if runtime_state != container_state {
                    let _ = self
                        .finalize_failed_container_start_state(
                            &actual_container_id,
                            runtime_status_before_start.clone(),
                        )
                        .await?;
                }
                runtime_state
            };
            return Err(Self::start_container_precondition_error(
                &actual_container_id,
                effective_state,
            ));
        }

        if matches!(
            runtime_status_before_start,
            ContainerStatus::Running | ContainerStatus::Stopped(_)
        ) {
            let updated_container = self
                .finalize_failed_container_start_state(
                    &actual_container_id,
                    runtime_status_before_start.clone(),
                )
                .await?;
            let effective_state = updated_container
                .as_ref()
                .map(|container| container.state)
                .unwrap_or_else(|| {
                    Self::map_runtime_container_state(runtime_status_before_start.clone())
                });
            return Err(Self::start_container_precondition_error(
                &actual_container_id,
                effective_state,
            ));
        }

        let checkpoint_restore = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).and_then(|container| {
                Self::read_internal_state::<StoredCheckpointRestore>(
                    &container.annotations,
                    INTERNAL_CHECKPOINT_RESTORE_KEY,
                )
            })
        };
        if checkpoint_restore.is_some() && !self.config.enable_criu_support {
            return Err(self.criu_support_disabled_status("checkpoint restore start"));
        }
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

        let runtime = self.runtime.clone();
        let actual_container_id_clone = actual_container_id.clone();
        let checkpoint_restore_for_runtime = checkpoint_restore.clone();
        let checkpoint_work_path = checkpoint_restore.as_ref().map(|restore| {
            self.checkpoint_runtime_work_path(Path::new(&restore.checkpoint_location))
        });
        let start_result = tokio::task::spawn_blocking(move || {
            if let Some(checkpoint_restore) = checkpoint_restore_for_runtime.as_ref() {
                let checkpoint_work_path = checkpoint_work_path
                    .as_ref()
                    .expect("restore path should be computed when checkpoint restore exists");
                runtime.restore_container_from_checkpoint(
                    &actual_container_id_clone,
                    Path::new(&checkpoint_restore.checkpoint_image_path),
                    checkpoint_work_path,
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
            let final_runtime_status = self
                .runtime_container_status_checked(&actual_container_id)
                .await;
            let _ = self
                .finalize_failed_container_start_state(&actual_container_id, final_runtime_status)
                .await?;
            return Err(status);
        }

        let mut observed_state = ContainerState::ContainerUnknown as i32;
        let mut reached_known_state = false;
        let mut final_runtime_status = ContainerStatus::Unknown;
        for _ in 0..20 {
            let runtime = self.runtime.clone();
            let container_id_for_status = actual_container_id.clone();
            let current_status = tokio::task::spawn_blocking(move || {
                runtime.container_status(&container_id_for_status)
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to spawn blocking task: {}", e)))?
            .unwrap_or(ContainerStatus::Unknown);

            final_runtime_status = current_status.clone();
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
            let final_runtime_status = self
                .runtime_container_status_checked(&actual_container_id)
                .await;
            let _ = self
                .finalize_failed_container_start_state(&actual_container_id, final_runtime_status)
                .await?;
            return Err(Status::internal(
                "Container failed to reach a known runtime state after start",
            ));
        }

        if !matches!(final_runtime_status, ContainerStatus::Running) {
            self.undo_failed_nri_start_container(nri_event.clone())
                .await;
            let updated_container = self
                .finalize_failed_container_start_state(
                    &actual_container_id,
                    final_runtime_status.clone(),
                )
                .await?;
            if matches!(final_runtime_status, ContainerStatus::Stopped(_)) {
                if let Some(container) = updated_container {
                    self.emit_container_event(
                        ContainerEventType::ContainerStoppedEvent,
                        &container,
                        Some(container.state),
                    )
                    .await;
                }
            }
            return Err(Status::internal(format!(
                "Container did not reach running state after start; runtime reported {}",
                Self::runtime_state_name(Self::map_runtime_container_state(final_runtime_status))
            )));
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

        let persistence = self.persistence.clone();
        let actual_container_id_for_persist = actual_container_id.clone();
        let persistence_state = match observed_state {
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
        };
        tokio::spawn(async move {
            let mut persistence = persistence.lock().await;
            if let Err(err) = persistence
                .update_container_state(&actual_container_id_for_persist, persistence_state)
            {
                log::error!(
                    "Failed to update container {} state in database: {}",
                    actual_container_id_for_persist,
                    err
                );
            }
        });
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
