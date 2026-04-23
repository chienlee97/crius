use super::*;

impl RuntimeServiceImpl {
    pub(super) fn pod_network_status_from_state(
        state: Option<&StoredPodState>,
    ) -> Option<PodSandboxNetworkStatus> {
        let primary_ip = state.and_then(|pod| pod.ip.clone()).unwrap_or_default();
        let mut seen = HashSet::new();
        let mut additional: Vec<PodIp> = state
            .map(|pod| {
                pod.additional_ips
                    .iter()
                    .filter(|ip| !ip.is_empty())
                    .filter(|ip| primary_ip.is_empty() || *ip != &primary_ip)
                    .filter(|ip| seen.insert((*ip).clone()))
                    .map(|ip| PodIp { ip: ip.clone() })
                    .collect()
            })
            .unwrap_or_default();

        if primary_ip.is_empty() {
            if additional.is_empty() {
                return None;
            }

            let primary = additional.remove(0);
            Some(PodSandboxNetworkStatus {
                ip: primary.ip,
                additional_ips: additional,
            })
        } else {
            Some(PodSandboxNetworkStatus {
                ip: primary_ip,
                additional_ips: additional,
            })
        }
    }

    pub(super) fn pod_linux_status_from_state(
        state: Option<&StoredPodState>,
    ) -> Option<LinuxPodSandboxStatus> {
        let options = state
            .and_then(|pod| pod.namespace_options.as_ref())
            .map(StoredNamespaceOptions::to_proto)
            .unwrap_or_else(|| NamespaceOption {
                network: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
                pid: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
                ipc: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            });

        Some(LinuxPodSandboxStatus {
            namespaces: Some(Namespace {
                options: Some(options),
            }),
        })
    }

    pub(super) fn pod_requires_managed_netns(state: Option<&StoredPodState>) -> bool {
        state
            .and_then(|pod| pod.namespace_options.as_ref())
            .map(|options| options.network != NamespaceMode::Node as i32)
            .unwrap_or(true)
    }

    pub(super) fn pod_has_required_netns(pod: &crate::proto::runtime::v1::PodSandbox) -> bool {
        let state =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY);
        if !Self::pod_requires_managed_netns(state.as_ref()) {
            return true;
        }

        state
            .and_then(|pod| pod.netns_path)
            .filter(|path| !path.is_empty())
            .map(|path| Path::new(&path).exists())
            .unwrap_or(true)
    }

    pub(super) fn runtime_state_name(runtime_state: i32) -> &'static str {
        match runtime_state {
            x if x == ContainerState::ContainerCreated as i32 => "created",
            x if x == ContainerState::ContainerRunning as i32 => "running",
            x if x == ContainerState::ContainerExited as i32 => "exited",
            _ => "unknown",
        }
    }

    pub(super) fn cgroup_driver(&self) -> CgroupDriver {
        match std::env::var("CRIUS_CGROUP_DRIVER")
            .ok()
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("systemd") => CgroupDriver::Systemd,
            Some("cgroupfs") => CgroupDriver::Cgroupfs,
            _ => {
                let systemd_active = std::path::Path::new("/run/systemd/system").exists()
                    || std::fs::read_to_string("/proc/1/comm")
                        .map(|content| content.trim() == "systemd")
                        .unwrap_or(false);
                let cgroup_v2 = std::path::Path::new("/sys/fs/cgroup/cgroup.controllers").exists();
                let systemd_cgroup_layout = std::path::Path::new("/sys/fs/cgroup/system.slice")
                    .exists()
                    || std::path::Path::new("/sys/fs/cgroup/user.slice").exists()
                    || std::path::Path::new("/sys/fs/cgroup/systemd").exists();

                if systemd_active && (cgroup_v2 || systemd_cgroup_layout) {
                    CgroupDriver::Systemd
                } else {
                    CgroupDriver::Cgroupfs
                }
            }
        }
    }

    pub(super) fn runtime_binary_version(&self) -> Option<String> {
        if !self.config.runtime_path.exists() {
            return None;
        }

        let output = Command::new(&self.config.runtime_path)
            .arg("--version")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }

        String::from_utf8(output.stdout)
            .ok()?
            .lines()
            .find(|line| !line.trim().is_empty())
            .map(|line| line.trim().to_string())
    }

    pub(super) fn runtime_readiness(&self) -> (bool, String, String) {
        let path = &self.config.runtime_path;
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(_) => {
                return (
                    false,
                    "RuntimeBinaryMissing".to_string(),
                    format!("runtime binary does not exist at {}", path.display()),
                );
            }
        };

        if !metadata.is_file() {
            return (
                false,
                "RuntimeBinaryInvalid".to_string(),
                format!("runtime path is not a regular file: {}", path.display()),
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return (
                    false,
                    "RuntimeBinaryNotExecutable".to_string(),
                    format!("runtime binary is not executable: {}", path.display()),
                );
            }
        }

        let version = self
            .runtime_binary_version()
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        (
            true,
            "RuntimeIsReady".to_string(),
            format!(
                "runtime binary is available at {} ({})",
                path.display(),
                version
            ),
        )
    }

    pub(super) fn cni_plugin_types(config: &serde_json::Value) -> Vec<String> {
        let mut plugin_types = Vec::new();
        if let Some(plugin_type) = config.get("type").and_then(|value| value.as_str()) {
            if !plugin_type.trim().is_empty() {
                plugin_types.push(plugin_type.to_string());
            }
        }
        if let Some(plugins) = config.get("plugins").and_then(|value| value.as_array()) {
            for plugin in plugins {
                if let Some(plugin_type) = plugin.get("type").and_then(|value| value.as_str()) {
                    if !plugin_type.trim().is_empty() {
                        plugin_types.push(plugin_type.to_string());
                    }
                }
            }
        }
        plugin_types
    }

    pub(super) fn path_is_executable(path: &Path) -> bool {
        let Ok(metadata) = std::fs::metadata(path) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    pub(super) fn runtime_feature_flags(&self) -> serde_json::Value {
        json!({
            "exec": true,
            "execSync": true,
            "attach": true,
            "portForward": true,
            "containerStats": true,
            "podSandboxStats": true,
            "podSandboxMetrics": true,
            "containerEvents": true,
            "reopenContainerLog": true,
            "updateContainerResources": true,
            "checkpointContainer": true,
        })
    }

    pub(super) fn network_health(&self) -> (bool, String, String) {
        let config_dirs = self.config.cni_config.config_dirs();
        let plugin_dirs = self.config.cni_config.plugin_dirs();
        let mut discovered_files = Vec::new();
        let mut invalid_files = Vec::new();
        let mut declared_plugins = HashSet::new();

        for dir in config_dirs {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let matches = path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| matches!(ext, "conf" | "conflist" | "json"))
                    .unwrap_or(false);
                if !matches {
                    continue;
                }

                discovered_files.push(path.display().to_string());
                match std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                {
                    Some(config) => {
                        for plugin_type in Self::cni_plugin_types(&config) {
                            declared_plugins.insert(plugin_type);
                        }
                    }
                    None => invalid_files.push(path.display().to_string()),
                }
            }
        }

        if discovered_files.is_empty() {
            (
                false,
                "CNIConfigMissing".to_string(),
                "no CNI config file found in configured CNI config directories".to_string(),
            )
        } else if !invalid_files.is_empty() && declared_plugins.is_empty() {
            (
                false,
                "CNIConfigInvalid".to_string(),
                format!(
                    "failed to parse {} CNI config file(s): {}",
                    invalid_files.len(),
                    invalid_files.join(", ")
                ),
            )
        } else if !declared_plugins.is_empty() {
            let missing_plugins: Vec<String> = declared_plugins
                .iter()
                .filter(|plugin_type| {
                    !plugin_dirs
                        .iter()
                        .any(|dir| Self::path_is_executable(&dir.join(plugin_type.as_str())))
                })
                .cloned()
                .collect();
            if !missing_plugins.is_empty() {
                (
                    false,
                    "CNIPluginMissing".to_string(),
                    format!(
                        "missing or non-executable CNI plugin binary/binaries: {}",
                        missing_plugins.join(", ")
                    ),
                )
            } else {
                (
                    true,
                    "CNINetworkConfigReady".to_string(),
                    format!(
                        "discovered {} CNI config file(s) and {} plugin type(s)",
                        discovered_files.len(),
                        declared_plugins.len()
                    ),
                )
            }
        } else {
            (
                true,
                "CNINetworkConfigReady".to_string(),
                format!("discovered {} CNI config file(s)", discovered_files.len()),
            )
        }
    }

    pub(super) fn container_matches_filter(
        container: &Container,
        filter: &crate::proto::runtime::v1::ContainerFilter,
    ) -> bool {
        if !(filter.id.is_empty()
            || container.id == filter.id
            || container.id.starts_with(&filter.id)
            || filter.id.starts_with(&container.id))
        {
            return false;
        }

        if let Some(state) = &filter.state {
            if container.state != state.state {
                return false;
            }
        }

        if !(filter.pod_sandbox_id.is_empty()
            || container.pod_sandbox_id == filter.pod_sandbox_id
            || container.pod_sandbox_id.starts_with(&filter.pod_sandbox_id)
            || filter.pod_sandbox_id.starts_with(&container.pod_sandbox_id))
        {
            return false;
        }

        for (k, v) in &filter.label_selector {
            if container.labels.get(k) != Some(v) {
                return false;
            }
        }

        true
    }

    pub(super) fn pod_sandbox_matches_filter(
        pod: &crate::proto::runtime::v1::PodSandbox,
        filter: &crate::proto::runtime::v1::PodSandboxFilter,
    ) -> bool {
        if !(filter.id.is_empty()
            || pod.id == filter.id
            || pod.id.starts_with(&filter.id)
            || filter.id.starts_with(&pod.id))
        {
            return false;
        }

        if let Some(state) = &filter.state {
            if pod.state != state.state {
                return false;
            }
        }

        for (k, v) in &filter.label_selector {
            if pod.labels.get(k) != Some(v) {
                return false;
            }
        }

        true
    }

    pub(super) async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let actual_container_id = self.resolve_container_id(&req.container_id).await?;
        let container = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Container not found"))?;

        let runtime_state = Self::map_runtime_container_state(
            self.runtime_container_status_checked(&actual_container_id)
                .await,
        );
        let status = Self::build_container_status_snapshot(&container, runtime_state);
        let info = if req.verbose {
            self.build_container_verbose_info(&container, runtime_state)
                .await?
        } else {
            HashMap::new()
        };

        Ok(Response::new(ContainerStatusResponse {
            status: Some(status),
            info,
        }))
    }

    pub(super) async fn list_containers(
        &self,
        request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_container_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListContainersResponse {
                        containers: Vec::new(),
                    }));
                };
                filter.id = resolved_id;
            }

            if !filter.pod_sandbox_id.is_empty() {
                let Some(resolved_pod_id) = self
                    .resolve_pod_sandbox_id_for_filter(&filter.pod_sandbox_id)
                    .await
                else {
                    return Ok(Response::new(ListContainersResponse {
                        containers: Vec::new(),
                    }));
                };
                filter.pod_sandbox_id = resolved_pod_id;
            }

            Some(filter)
        } else {
            None
        };
        let containers = self.containers.lock().await;
        let pod_meta_by_id: HashMap<String, (String, String, String)> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .iter()
                .map(|(id, pod)| {
                    let (name, namespace, uid) = pod
                        .metadata
                        .as_ref()
                        .map(|m| (m.name.clone(), m.namespace.clone(), m.uid.clone()))
                        .unwrap_or_else(|| {
                            ("unknown".to_string(), "default".to_string(), id.clone())
                        });
                    (id.clone(), (name, namespace, uid))
                })
                .collect()
        };

        let runtime = self.runtime.clone();
        let containers_list = containers
            .values()
            .cloned()
            .map(|mut c| {
                if c.metadata.is_none() {
                    c.metadata = Some(ContainerMetadata {
                        name: c.id.clone(),
                        attempt: 1,
                    });
                }
                if c.image.is_none() {
                    c.image = Some(ImageSpec {
                        image: c.image_ref.clone(),
                        ..Default::default()
                    });
                }
                c.annotations = Self::external_annotations(&c.annotations);
                if let Some((pod_name, pod_namespace, pod_uid)) =
                    pod_meta_by_id.get(&c.pod_sandbox_id)
                {
                    c.labels
                        .entry("io.kubernetes.pod.name".to_string())
                        .or_insert_with(|| pod_name.clone());
                    c.labels
                        .entry("io.kubernetes.pod.namespace".to_string())
                        .or_insert_with(|| pod_namespace.clone());
                    c.labels
                        .entry("io.kubernetes.pod.uid".to_string())
                        .or_insert_with(|| pod_uid.clone());
                }
                c.created_at = Self::normalize_timestamp_nanos(c.created_at);

                c.state = match runtime.container_status(&c.id) {
                    Ok(status) => match status {
                        ContainerStatus::Created => ContainerState::ContainerCreated as i32,
                        ContainerStatus::Running => ContainerState::ContainerRunning as i32,
                        ContainerStatus::Stopped(_) => ContainerState::ContainerExited as i32,
                        ContainerStatus::Unknown => ContainerState::ContainerUnknown as i32,
                    },
                    Err(_) => ContainerState::ContainerUnknown as i32,
                };

                c
            })
            .filter(|c| {
                if let Some(f) = &filter {
                    Self::container_matches_filter(c, f)
                } else {
                    true
                }
            })
            .collect();

        Ok(Response::new(ListContainersResponse {
            containers: containers_list,
        }))
    }

    pub(super) async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let (runtime_ready, runtime_reason, runtime_message) = self.runtime_readiness();
        let (network_ready, network_reason, network_message) = self.network_health();
        let info = if req.verbose {
            let runtime_network_config = self.runtime_network_config.lock().await.clone();
            let payload = json!({
                "runtimeName": "crius",
                "runtimeVersion": self
                    .runtime_binary_version()
                    .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
                "runtimeApiVersion": "v1",
                "rootDir": self.config.root_dir.display().to_string(),
                "runtime": self.config.runtime.clone(),
                "runtimePath": self.config.runtime_path.display().to_string(),
                "runtimeRoot": self.config.runtime_root.display().to_string(),
                "logDir": self.config.log_dir.display().to_string(),
                "pauseImage": self.config.pause_image.clone(),
                "runtimeHandlers": self.config.runtime_handlers.clone(),
                "runtimeFeatures": self.runtime_feature_flags(),
                "runtimeNetworkConfig": runtime_network_config.as_ref().map(|cfg| {
                    json!({
                        "podCIDR": cfg.pod_cidr,
                    })
                }),
                "networkReady": network_ready,
                "networkReason": network_reason.clone(),
                "cgroupDriver": self.cgroup_driver().as_str_name(),
                "recovery": {
                    "enabled": true,
                    "startupReconcile": true,
                    "eventReplayOnRecovery": false,
                },
            });
            let mut info = HashMap::new();
            info.insert(
                "config".to_string(),
                serde_json::to_string(&payload).map_err(|e| {
                    Status::internal(format!("Failed to encode runtime config info: {}", e))
                })?,
            );
            info
        } else {
            HashMap::new()
        };

        Ok(Response::new(StatusResponse {
            status: Some(RuntimeStatus {
                conditions: vec![
                    RuntimeCondition {
                        r#type: "RuntimeReady".to_string(),
                        status: runtime_ready,
                        reason: runtime_reason,
                        message: runtime_message,
                    },
                    RuntimeCondition {
                        r#type: "NetworkReady".to_string(),
                        status: network_ready,
                        reason: network_reason,
                        message: network_message,
                    },
                ],
            }),
            info,
        }))
    }

    pub(super) async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let resolved_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let pod_sandbox = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&resolved_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
        let container_statuses = self.current_pod_container_snapshots(&resolved_id).await;
        let status = self.build_pod_sandbox_status_snapshot(&pod_sandbox);
        let info = if req.verbose {
            self.build_pod_verbose_info(&pod_sandbox).await?
        } else {
            HashMap::new()
        };

        Ok(Response::new(PodSandboxStatusResponse {
            status: Some(status),
            info,
            containers_statuses: container_statuses,
            timestamp: Self::now_nanos(),
        }))
    }

    pub(super) async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_pod_sandbox_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListPodSandboxResponse { items: Vec::new() }));
                };
                filter.id = resolved_id;
            }

            Some(filter)
        } else {
            None
        };
        let pod_sandboxes = self.pod_sandboxes.lock().await;
        let items = pod_sandboxes
            .values()
            .cloned()
            .map(|mut p| {
                let pod_state = Self::read_internal_state::<StoredPodState>(
                    &p.annotations,
                    INTERNAL_POD_STATE_KEY,
                );
                p.annotations = Self::external_annotations(&p.annotations);
                if p.runtime_handler.is_empty() {
                    p.runtime_handler = pod_state
                        .as_ref()
                        .map(|state| state.runtime_handler.clone())
                        .filter(|handler| !handler.is_empty())
                        .unwrap_or_else(|| self.config.runtime.clone());
                }
                p.created_at = Self::normalize_timestamp_nanos(p.created_at);
                p
            })
            .filter(|p| {
                if let Some(f) = &filter {
                    Self::pod_sandbox_matches_filter(p, f)
                } else {
                    true
                }
            })
            .collect();

        Ok(Response::new(ListPodSandboxResponse { items }))
    }
}
