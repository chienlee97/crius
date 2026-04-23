use super::*;

impl RuntimeServiceImpl {
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
            let next_state = match pause_status {
                ContainerStatus::Running if pod_has_required_netns => {
                    PodSandboxState::SandboxReady as i32
                }
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

    pub async fn recover_state(&self) -> Result<(), Status> {
        let (recovered_containers, recovered_pods) = {
            let persistence = self.persistence.lock().await;
            (persistence.recover_containers(), persistence.recover_pods())
        };

        match recovered_containers {
            Ok(containers) => {
                let mut memory_containers = self.containers.lock().await;
                for (_id, status, record) in containers {
                    let annotations: HashMap<String, String> =
                        serde_json::from_str(&record.annotations).unwrap_or_default();
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

                    let container = crate::proto::runtime::v1::Container {
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
                    let mut container = container;
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
            Err(e) => {
                log::error!("Failed to recover containers: {}", e);
            }
        }

        match recovered_pods {
            Ok(pods) => {
                let mut memory_pods = self.pod_sandboxes.lock().await;
                let mut pod_manager = self.pod_manager.lock().await;
                for record in pods {
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

                    let mut additional_ip_values: Vec<IpAddr> = pod_state
                        .additional_ips
                        .iter()
                        .filter_map(|ip| ip.parse::<IpAddr>().ok())
                        .collect();
                    let primary_ip = pod_state
                        .ip
                        .as_ref()
                        .and_then(|ip| ip.parse::<IpAddr>().ok())
                        .or_else(|| additional_ip_values.first().copied());
                    let network_status = primary_ip.map(|parsed_ip| {
                        additional_ip_values.retain(|ip| ip != &parsed_ip);
                        crate::network::NetworkStatus {
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
                        }
                    });

                    pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
                        id: record.id.clone(),
                        config: crate::pod::PodSandboxConfig {
                            name: record.name.clone(),
                            namespace: record.namespace.clone(),
                            uid: record.uid.clone(),
                            hostname: record.name.clone(),
                            log_directory: pod_state.log_directory.as_ref().map(PathBuf::from),
                            runtime_handler: recovered_runtime_handler.clone(),
                            labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                            annotations: Self::external_annotations(&annotations)
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                            dns_config: None,
                            port_mappings: vec![],
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
                log::info!(
                    "Recovered {} pod sandboxes from database",
                    memory_pods.len()
                );
            }
            Err(e) => {
                log::error!("Failed to recover pod sandboxes: {}", e);
            }
        }

        self.reconcile_recovered_state().await?;
        self.ensure_exit_monitors_for_active_containers().await;
        self.cleanup_orphaned_shim_artifacts().await;
        Ok(())
    }
}
