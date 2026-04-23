use super::*;

impl RuntimeServiceImpl {
    pub(super) async fn nri_pod_event(&self, pod_id: &str) -> NriPodEvent {
        let pod = {
            let pods = self.pod_sandboxes.lock().await;
            pods.get(pod_id)
                .map(|pod| Self::build_nri_pod_from_proto(&self.runtime, pod))
        };
        NriPodEvent {
            pod,
            overhead_linux_resources: None,
            linux_resources: None,
        }
    }

    pub(super) fn effective_pod_linux_resources(
        overhead: Option<&crate::proto::runtime::v1::LinuxContainerResources>,
        resources: Option<&crate::proto::runtime::v1::LinuxContainerResources>,
    ) -> Option<crate::proto::runtime::v1::LinuxContainerResources> {
        resources.cloned().or_else(|| overhead.cloned())
    }

    #[allow(dead_code)]
    pub(super) async fn nri_pod_update_event(
        &self,
        pod_id: &str,
        overhead: Option<&crate::proto::runtime::v1::LinuxContainerResources>,
        resources: Option<&crate::proto::runtime::v1::LinuxContainerResources>,
    ) -> NriPodEvent {
        let mut event = self.nri_pod_event(pod_id).await;
        event.overhead_linux_resources = overhead.map(linux_resources_from_cri);
        event.linux_resources = resources.map(linux_resources_from_cri);
        event
    }

    pub(super) async fn undo_failed_nri_run_pod_sandbox(&self, pod_id: &str) {
        let event = self.nri_pod_event(pod_id).await;
        if let Err(err) = self.nri.stop_pod_sandbox(event.clone()).await {
            log::warn!("Undo stop of failed NRI pod start failed: {}", err);
        }
        if let Err(err) = self.nri.remove_pod_sandbox(event).await {
            log::warn!("Undo remove of failed NRI pod start failed: {}", err);
        }
    }

    pub(super) async fn rollback_failed_pod_sandbox_run(&self, pod_id: &str) {
        self.undo_failed_nri_run_pod_sandbox(pod_id).await;

        {
            let mut pod_manager = self.pod_manager.lock().await;
            if let Err(err) = pod_manager.remove_pod_sandbox(pod_id).await {
                log::warn!(
                    "Failed to remove pod sandbox {} during run rollback: {}",
                    pod_id,
                    err
                );
            }
        }

        {
            let mut pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.remove(pod_id);
        }

        let mut persistence = self.persistence.lock().await;
        if let Err(err) = persistence.delete_pod_sandbox(pod_id) {
            log::warn!(
                "Failed to delete pod sandbox {} from persistence during run rollback: {}",
                pod_id,
                err
            );
        }
    }

    pub(super) async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let pod_config = req
            .config
            .ok_or_else(|| Status::invalid_argument("Pod config not specified"))?;
        let runtime_handler = self.resolve_runtime_handler(req.runtime_handler.trim())?;
        let linux_config = pod_config.linux.clone();
        let sandbox_security = linux_config
            .as_ref()
            .and_then(|linux| linux.security_context.as_ref());
        let pod_overhead = linux_config
            .as_ref()
            .and_then(|linux| linux.overhead.clone());
        let pod_linux_resources = linux_config
            .as_ref()
            .and_then(|linux| linux.resources.clone());
        let effective_pod_linux_resources = Self::effective_pod_linux_resources(
            pod_overhead.as_ref(),
            pod_linux_resources.as_ref(),
        );
        let pod_selinux_label = Self::selinux_label_from_proto(
            sandbox_security.and_then(|security| security.selinux_options.as_ref()),
        );
        let pod_seccomp_profile = Self::seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            Self::legacy_linux_sandbox_seccomp_profile_path(sandbox_security),
        );
        let stored_seccomp_profile = Self::stored_seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            Self::legacy_linux_sandbox_seccomp_profile_path(sandbox_security),
        );
        let runtime_network_config = self.runtime_network_config.lock().await.clone();
        let runtime_pod_cidr = runtime_network_config
            .as_ref()
            .map(|cfg| cfg.pod_cidr.clone());
        let mut sandbox_annotations: HashMap<String, String> = pod_config
            .annotations
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        sandbox_annotations.insert(
            CRIO_SANDBOX_NAME_ANNOTATION.to_string(),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
        );
        sandbox_annotations.insert(
            CRIO_POD_NAME_ANNOTATION.to_string(),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
        );
        sandbox_annotations.insert(
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
        );
        sandbox_annotations.insert(
            CONTAINERD_SANDBOX_NAME_ANNOTATION.to_string(),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
        );
        sandbox_annotations.insert(
            CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION.to_string(),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
        );
        sandbox_annotations.insert(
            CONTAINERD_SANDBOX_UID_ANNOTATION.to_string(),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default(),
        );

        let sandbox_config = PodSandboxConfig {
            name: pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
            namespace: pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
            uid: pod_config
                .metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default(),
            hostname: pod_config.hostname.clone(),
            log_directory: if pod_config.log_directory.is_empty() {
                None
            } else {
                Some(PathBuf::from(&pod_config.log_directory))
            },
            runtime_handler: runtime_handler.clone(),
            labels: pod_config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            annotations: sandbox_annotations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            dns_config: pod_config.dns_config.map(|d| crate::pod::DNSConfig {
                servers: d.servers,
                searches: d.searches,
                options: d.options,
            }),
            port_mappings: pod_config
                .port_mappings
                .iter()
                .map(|p| {
                    let protocol_str = match p.protocol {
                        0 => "TCP",
                        1 => "UDP",
                        2 => "SCTP",
                        _ => "TCP",
                    }
                    .to_string();
                    crate::pod::PortMapping {
                        protocol: protocol_str,
                        container_port: p.container_port,
                        host_port: p.host_port,
                        host_ip: p.host_ip.clone(),
                    }
                })
                .collect(),
            network_config: runtime_network_config
                .as_ref()
                .map(|cfg| crate::pod::NetworkConfig {
                    network_namespace: format!(
                        "crius-{}-{}",
                        pod_config
                            .metadata
                            .as_ref()
                            .map(|m| m.namespace.as_str())
                            .unwrap_or("default"),
                        pod_config
                            .metadata
                            .as_ref()
                            .map(|m| m.name.as_str())
                            .unwrap_or("pod"),
                    ),
                    pod_cidr: cfg.pod_cidr.clone(),
                }),
            cgroup_parent: linux_config
                .as_ref()
                .and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                })
                .or_else(|| Some("crius".to_string())),
            sysctls: linux_config
                .as_ref()
                .map(|linux| linux.sysctls.clone())
                .unwrap_or_default(),
            namespace_options: sandbox_security
                .and_then(|security| security.namespace_options.clone()),
            privileged: sandbox_security
                .map(|security| security.privileged)
                .unwrap_or(false),
            run_as_user: sandbox_security
                .and_then(|security| security.run_as_user.as_ref())
                .map(|user| user.value.to_string()),
            run_as_group: sandbox_security
                .and_then(|security| security.run_as_group.as_ref())
                .and_then(|group| u32::try_from(group.value).ok()),
            supplemental_groups: sandbox_security
                .map(|security| {
                    security
                        .supplemental_groups
                        .iter()
                        .filter_map(|group| u32::try_from(*group).ok())
                        .collect()
                })
                .unwrap_or_default(),
            readonly_rootfs: sandbox_security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
            no_new_privileges: None,
            apparmor_profile: Self::security_profile_name(
                sandbox_security.and_then(|security| security.apparmor.as_ref()),
                "",
            ),
            selinux_label: pod_selinux_label.clone(),
            seccomp_profile: pod_seccomp_profile.clone(),
            linux_resources: effective_pod_linux_resources.clone(),
        };

        let mut pod_manager = self.pod_manager.lock().await;
        let pod_id = pod_manager
            .create_pod_sandbox(sandbox_config)
            .await
            .map_err(|e| Status::internal(format!("Failed to create pod sandbox: {}", e)))?;
        let created_pod = pod_manager.get_pod_sandbox_cloned(&pod_id);
        drop(pod_manager);

        let pod_state = created_pod
            .as_ref()
            .map(|pod| StoredPodState {
                log_directory: if pod_config.log_directory.is_empty() {
                    None
                } else {
                    Some(pod_config.log_directory.clone())
                },
                runtime_handler: runtime_handler.clone(),
                runtime_pod_cidr: runtime_pod_cidr.clone(),
                netns_path: Some(pod.netns_path.to_string_lossy().to_string()),
                pause_container_id: Some(pod.pause_container_id.clone()),
                ip: if pod.ip.is_empty() {
                    None
                } else {
                    Some(pod.ip.clone())
                },
                additional_ips: pod
                    .network_status
                    .as_ref()
                    .map(|status| {
                        status
                            .interfaces
                            .iter()
                            .filter_map(|iface| iface.ip.as_ref())
                            .map(|ip| ip.to_string())
                            .filter(|ip| pod.ip.is_empty() || ip != &pod.ip)
                            .collect()
                    })
                    .unwrap_or_default(),
                cgroup_parent: linux_config.as_ref().and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                }),
                sysctls: linux_config
                    .as_ref()
                    .map(|linux| linux.sysctls.clone())
                    .unwrap_or_default(),
                namespace_options: sandbox_security
                    .and_then(|security| security.namespace_options.as_ref())
                    .map(StoredNamespaceOptions::from),
                privileged: sandbox_security
                    .map(|security| security.privileged)
                    .unwrap_or(false),
                run_as_user: sandbox_security
                    .and_then(|security| security.run_as_user.as_ref())
                    .map(|user| user.value.to_string()),
                run_as_group: sandbox_security
                    .and_then(|security| security.run_as_group.as_ref())
                    .and_then(|group| u32::try_from(group.value).ok()),
                supplemental_groups: sandbox_security
                    .map(|security| {
                        security
                            .supplemental_groups
                            .iter()
                            .filter_map(|group| u32::try_from(*group).ok())
                            .collect()
                    })
                    .unwrap_or_default(),
                readonly_rootfs: sandbox_security
                    .map(|security| security.readonly_rootfs)
                    .unwrap_or(false),
                no_new_privileges: None,
                apparmor_profile: Self::security_profile_name(
                    sandbox_security.and_then(|security| security.apparmor.as_ref()),
                    "",
                ),
                selinux_label: pod_selinux_label.clone(),
                seccomp_profile: stored_seccomp_profile.clone(),
                overhead_linux_resources: pod_overhead.as_ref().map(StoredLinuxResources::from),
                linux_resources: pod_linux_resources.as_ref().map(StoredLinuxResources::from),
            })
            .unwrap_or_else(|| StoredPodState {
                log_directory: if pod_config.log_directory.is_empty() {
                    None
                } else {
                    Some(pod_config.log_directory.clone())
                },
                runtime_handler: runtime_handler.clone(),
                runtime_pod_cidr,
                cgroup_parent: linux_config.as_ref().and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                }),
                sysctls: linux_config
                    .as_ref()
                    .map(|linux| linux.sysctls.clone())
                    .unwrap_or_default(),
                namespace_options: sandbox_security
                    .and_then(|security| security.namespace_options.as_ref())
                    .map(StoredNamespaceOptions::from),
                privileged: sandbox_security
                    .map(|security| security.privileged)
                    .unwrap_or(false),
                run_as_user: sandbox_security
                    .and_then(|security| security.run_as_user.as_ref())
                    .map(|user| user.value.to_string()),
                run_as_group: sandbox_security
                    .and_then(|security| security.run_as_group.as_ref())
                    .and_then(|group| u32::try_from(group.value).ok()),
                supplemental_groups: sandbox_security
                    .map(|security| {
                        security
                            .supplemental_groups
                            .iter()
                            .filter_map(|group| u32::try_from(*group).ok())
                            .collect()
                    })
                    .unwrap_or_default(),
                readonly_rootfs: sandbox_security
                    .map(|security| security.readonly_rootfs)
                    .unwrap_or(false),
                no_new_privileges: None,
                apparmor_profile: Self::security_profile_name(
                    sandbox_security.and_then(|security| security.apparmor.as_ref()),
                    "",
                ),
                selinux_label: pod_selinux_label.clone(),
                seccomp_profile: stored_seccomp_profile.clone(),
                overhead_linux_resources: pod_overhead.as_ref().map(StoredLinuxResources::from),
                linux_resources: pod_linux_resources.as_ref().map(StoredLinuxResources::from),
                ..Default::default()
            });
        let mut stored_annotations = sandbox_annotations;
        Self::insert_internal_state(&mut stored_annotations, INTERNAL_POD_STATE_KEY, &pod_state)?;

        let pod_sandbox = crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: pod_config.metadata.clone(),
            state: PodSandboxState::SandboxReady as i32,
            created_at: created_pod
                .as_ref()
                .map(|pod| Self::normalize_timestamp_nanos(pod.created_at))
                .unwrap_or_else(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64
                }),
            labels: pod_config.labels.clone(),
            annotations: stored_annotations.clone(),
            runtime_handler: runtime_handler.clone(),
        };

        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.insert(pod_id.clone(), pod_sandbox.clone());
        drop(pod_sandboxes);

        let fallback_netns_path = format!(
            "/var/run/netns/crius-{}-{}",
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
            pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default()
        );
        let netns_path = pod_state.netns_path.clone().unwrap_or(fallback_netns_path);
        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.save_pod_sandbox(
            &pod_id,
            "ready",
            &pod_config
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
            &pod_config
                .metadata
                .as_ref()
                .map(|m| m.namespace.clone())
                .unwrap_or_else(|| "default".to_string()),
            &pod_config
                .metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default(),
            &netns_path,
            &pod_config.labels,
            &stored_annotations,
            pod_state.pause_container_id.as_deref(),
            pod_state.ip.as_deref(),
        ) {
            log::error!("Failed to persist pod sandbox {}: {}", pod_id, e);
        } else {
            log::info!("Pod sandbox {} persisted to database", pod_id);
        }
        if let Err(err) = self
            .nri
            .run_pod_sandbox(self.nri_pod_event(&pod_id).await)
            .await
        {
            self.rollback_failed_pod_sandbox_run(&pod_id).await;
            return Err(Status::internal(format!(
                "NRI RunPodSandbox failed: {}",
                err
            )));
        }

        log::info!("Pod sandbox {} created successfully", pod_id);
        if let Some(pause_container_id) = pod_state.pause_container_id.as_deref() {
            self.ensure_pod_exit_monitor_registered(&pod_id, pause_container_id);
        }
        self.emit_pod_event(
            ContainerEventType::ContainerCreatedEvent,
            &pod_sandbox,
            Vec::new(),
        )
        .await;
        self.emit_pod_event(
            ContainerEventType::ContainerStartedEvent,
            &pod_sandbox,
            Vec::new(),
        )
        .await;
        Ok(Response::new(RunPodSandboxResponse {
            pod_sandbox_id: pod_id,
        }))
    }

    pub(super) async fn update_pod_sandbox_resources(
        &self,
        request: Request<UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<UpdatePodSandboxResourcesResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let overhead = req.overhead;
        let resources = req.resources;
        let effective_resources =
            Self::effective_pod_linux_resources(overhead.as_ref(), resources.as_ref());
        self.nri
            .update_pod_sandbox(
                self.nri_pod_update_event(&pod_id, overhead.as_ref(), resources.as_ref())
                    .await,
            )
            .await
            .map_err(|e| Status::internal(format!("NRI UpdatePodSandbox failed: {}", e)))?;
        let pause_container_id = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            let pod = pod_sandboxes
                .get(&pod_id)
                .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY)
                .and_then(|state| state.pause_container_id)
                .filter(|pause_container_id| !pause_container_id.is_empty())
        };

        if let (Some(pause_container_id), Some(resources)) =
            (pause_container_id.as_deref(), effective_resources.as_ref())
        {
            let stored_resources = StoredLinuxResources::from(resources);
            self.runtime_update_container_resources(pause_container_id, &stored_resources)
                .await?;
        }

        {
            let mut pod_manager = self.pod_manager.lock().await;
            pod_manager
                .update_pod_sandbox_resources(&pod_id, effective_resources.clone())
                .await
                .map_err(|e| {
                    Status::internal(format!("Failed to update pod sandbox resources: {}", e))
                })?;
        }

        let updated_pod = {
            let mut pod_sandboxes = self.pod_sandboxes.lock().await;
            let pod = pod_sandboxes
                .get_mut(&pod_id)
                .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
            let mut state = Self::read_internal_state::<StoredPodState>(
                &pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .unwrap_or_default();
            state.overhead_linux_resources = overhead.as_ref().map(StoredLinuxResources::from);
            state.linux_resources = resources.as_ref().map(StoredLinuxResources::from);
            Self::insert_internal_state(&mut pod.annotations, INTERNAL_POD_STATE_KEY, &state)?;
            pod.clone()
        };

        let netns_path = Self::read_internal_state::<StoredPodState>(
            &updated_pod.annotations,
            INTERNAL_POD_STATE_KEY,
        )
        .and_then(|state| state.netns_path)
        .unwrap_or_default();
        let mut persistence = self.persistence.lock().await;
        persistence
            .save_pod_sandbox(
                &updated_pod.id,
                if updated_pod.state == PodSandboxState::SandboxReady as i32 {
                    "ready"
                } else {
                    "notready"
                },
                &updated_pod
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.name.clone())
                    .unwrap_or_default(),
                &updated_pod
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.namespace.clone())
                    .unwrap_or_default(),
                &updated_pod
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.uid.clone())
                    .unwrap_or_default(),
                &netns_path,
                &updated_pod.labels,
                &updated_pod.annotations,
                Self::read_internal_state::<StoredPodState>(
                    &updated_pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .and_then(|state| state.pause_container_id)
                .as_deref(),
                Self::read_internal_state::<StoredPodState>(
                    &updated_pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .and_then(|state| state.ip)
                .as_deref(),
            )
            .map_err(|e| Status::internal(format!("Failed to persist pod sandbox: {}", e)))?;
        drop(persistence);

        if let Err(err) = self
            .nri
            .post_update_pod_sandbox(self.nri_pod_event(&pod_id).await)
            .await
        {
            log::warn!("NRI PostUpdatePodSandbox failed for {}: {}", pod_id, err);
        }

        Ok(Response::new(UpdatePodSandboxResourcesResponse {}))
    }

    pub(super) async fn stop_pod_sandbox(
        &self,
        request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let Some(pod_id) = self
            .resolve_pod_sandbox_id_if_exists(&req.pod_sandbox_id)
            .await?
        else {
            return Ok(Response::new(StopPodSandboxResponse {}));
        };

        log::info!("Stopping pod sandbox {}", pod_id);
        let container_ids: Vec<String> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|c| c.pod_sandbox_id == pod_id)
                .map(|c| c.id.clone())
                .collect()
        };

        let mut stopped_containers = Vec::new();
        for container_id in &container_ids {
            if let Some(container) = self.stop_container_internal(container_id, 30).await? {
                stopped_containers.push(container);
            }
        }

        let mut pod_manager = self.pod_manager.lock().await;
        pod_manager
            .stop_pod_sandbox(&pod_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to stop pod sandbox: {}", e)))?;

        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        if let Some(pod) = pod_sandboxes.get_mut(&pod_id) {
            pod.state = PodSandboxState::SandboxNotready as i32;
        }
        drop(pod_sandboxes);

        let pod_stop_event = self.nri_pod_event(&pod_id).await;
        if let Err(err) = self.nri.stop_pod_sandbox(pod_stop_event).await {
            log::warn!("NRI StopPodSandbox failed for {}: {}", pod_id, err);
        }

        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.update_pod_state(&pod_id, "notready") {
            log::error!("Failed to update pod {} state in database: {}", pod_id, e);
        }

        log::info!("Pod sandbox {} stopped", pod_id);
        for container in stopped_containers {
            self.emit_container_event(
                ContainerEventType::ContainerStoppedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        if let Some(pod) = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_id).cloned()
        } {
            let container_statuses = self.current_pod_container_snapshots(&pod_id).await;
            self.emit_pod_event(
                ContainerEventType::ContainerStoppedEvent,
                &pod,
                container_statuses,
            )
            .await;
        }
        Ok(Response::new(StopPodSandboxResponse {}))
    }

    pub(super) async fn remove_pod_sandbox(
        &self,
        request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let Some(pod_id) = self
            .resolve_pod_sandbox_id_if_exists(&req.pod_sandbox_id)
            .await?
        else {
            return Ok(Response::new(RemovePodSandboxResponse {}));
        };

        log::info!("Removing pod sandbox {}", pod_id);
        let existing_pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_id).cloned()
        };
        let pod_remove_event = self.nri_pod_event(&pod_id).await;
        let existing_container_statuses = self.current_pod_container_snapshots(&pod_id).await;

        let fallback_netns_name = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .get(&pod_id)
                .and_then(|p| p.metadata.as_ref())
                .map(|m| format!("crius-{}-{}", m.namespace, m.name))
        };

        let containers_in_pod: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|c| c.pod_sandbox_id == pod_id)
                .cloned()
                .collect()
        };
        let container_ids: Vec<String> = containers_in_pod
            .iter()
            .map(|container| container.id.clone())
            .collect();

        let mut removed_containers = Vec::new();
        for container_id in &container_ids {
            if let Some(container) = self.remove_container_internal(container_id).await? {
                removed_containers.push(container);
            }
        }

        let mut pod_manager = self.pod_manager.lock().await;
        pod_manager
            .remove_pod_sandbox(&pod_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to remove pod sandbox: {}", e)))?;

        if let Some(netns_name) = fallback_netns_name {
            let network_manager =
                DefaultNetworkManager::from_cni_config(self.config.cni_config.clone());
            if let Err(e) = network_manager.remove_network_namespace(&netns_name).await {
                log::warn!("Fallback netns cleanup failed for {}: {}", netns_name, e);
            }
        }

        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.remove(&pod_id);
        drop(pod_sandboxes);

        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.delete_pod_sandbox(&pod_id) {
            log::error!("Failed to delete pod {} from database: {}", pod_id, e);
        } else {
            log::info!("Pod sandbox {} removed from database", pod_id);
        }

        if let Err(err) = self.nri.remove_pod_sandbox(pod_remove_event).await {
            log::warn!("NRI RemovePodSandbox failed for {}: {}", pod_id, err);
        }

        log::info!("Pod sandbox {} removed", pod_id);
        for container in removed_containers {
            self.emit_container_event(
                ContainerEventType::ContainerDeletedEvent,
                &container,
                Some(container.state),
            )
            .await;
        }
        if let Some(pod) = existing_pod {
            self.emit_pod_event(
                ContainerEventType::ContainerDeletedEvent,
                &pod,
                existing_container_statuses,
            )
            .await;
        }
        Ok(Response::new(RemovePodSandboxResponse {}))
    }
}
