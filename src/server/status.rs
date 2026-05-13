use super::*;

impl RuntimeServiceImpl {
    pub(super) fn effective_runtime_state_for_container(
        container: &Container,
        runtime_status: crate::runtime::ContainerStatus,
    ) -> i32 {
        let runtime_state = Self::map_runtime_container_state(runtime_status);
        if runtime_state != ContainerState::ContainerUnknown as i32 {
            return runtime_state;
        }

        let stored_state = container.state;
        if stored_state == ContainerState::ContainerExited as i32
            || stored_state == ContainerState::ContainerRunning as i32
            || stored_state == ContainerState::ContainerCreated as i32
        {
            return stored_state;
        }

        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        if container_state
            .as_ref()
            .and_then(|state| state.finished_at)
            .is_some()
            || container_state
                .as_ref()
                .and_then(|state| state.exit_code)
                .is_some()
        {
            return ContainerState::ContainerExited as i32;
        }

        ContainerState::ContainerUnknown as i32
    }

    pub(super) fn detected_cgroup_version() -> &'static str {
        if std::path::Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
            "v2"
        } else if std::path::Path::new("/sys/fs/cgroup/cpu").exists() {
            "v1"
        } else {
            "unknown"
        }
    }

    pub(super) fn sandbox_identity_key(
        pod: &crate::proto::runtime::v1::PodSandbox,
    ) -> Option<String> {
        let metadata = pod.metadata.as_ref()?;
        let uid = pod
            .labels
            .get("io.kubernetes.pod.uid")
            .filter(|uid| !uid.is_empty())
            .cloned()
            .or_else(|| (!metadata.uid.is_empty()).then(|| metadata.uid.clone()))?;
        Some(format!(
            "{}\u{1f}{}\u{1f}{}",
            uid, metadata.namespace, metadata.name
        ))
    }

    pub(super) fn sandbox_rank(pod: &crate::proto::runtime::v1::PodSandbox) -> (u8, i64) {
        let ready = (pod.state == PodSandboxState::SandboxReady as i32) as u8;
        (ready, pod.created_at)
    }

    pub(super) fn demote_stale_ready_logical_pod_sandboxes(
        pods: &mut [crate::proto::runtime::v1::PodSandbox],
    ) {
        let mut newest_ready: HashMap<String, (i64, String)> = HashMap::new();
        for pod in pods.iter() {
            if pod.state != PodSandboxState::SandboxReady as i32 {
                continue;
            }
            let Some(identity) = Self::sandbox_identity_key(pod) else {
                continue;
            };
            let candidate = (pod.created_at, pod.id.clone());
            match newest_ready.get(&identity) {
                Some(current)
                    if current.0 > candidate.0
                        || (current.0 == candidate.0 && current.1 >= candidate.1) => {}
                _ => {
                    newest_ready.insert(identity, candidate);
                }
            }
        }

        for pod in pods.iter_mut() {
            if pod.state != PodSandboxState::SandboxReady as i32 {
                continue;
            }
            let Some(identity) = Self::sandbox_identity_key(pod) else {
                continue;
            };
            if newest_ready
                .get(&identity)
                .is_some_and(|(_, id)| id != &pod.id)
            {
                pod.state = PodSandboxState::SandboxNotready as i32;
            }
        }
    }

    pub(super) fn retain_best_logical_pod_sandboxes(
        pods: Vec<crate::proto::runtime::v1::PodSandbox>,
    ) -> Vec<crate::proto::runtime::v1::PodSandbox> {
        let mut best: HashMap<String, crate::proto::runtime::v1::PodSandbox> = HashMap::new();
        let mut passthrough = Vec::new();

        for pod in pods {
            let Some(identity) = Self::sandbox_identity_key(&pod) else {
                passthrough.push(pod);
                continue;
            };

            match best.entry(identity) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if Self::sandbox_rank(&pod) > Self::sandbox_rank(entry.get()) {
                        entry.insert(pod);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(pod);
                }
            }
        }

        let mut deduped: Vec<_> = best.into_values().collect();
        deduped.extend(passthrough);
        deduped
    }

    pub(super) async fn latest_ready_logical_pod_sandbox_id(
        &self,
        pod: &crate::proto::runtime::v1::PodSandbox,
    ) -> Option<String> {
        let identity = Self::sandbox_identity_key(pod)?;
        let candidates: Vec<_> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .values()
                .filter(|candidate| {
                    Self::sandbox_identity_key(candidate).as_ref() == Some(&identity)
                })
                .cloned()
                .collect()
        };

        let mut newest: Option<(i64, String)> = None;
        for mut candidate in candidates {
            candidate.state = self.live_pod_sandbox_state(&candidate).await;
            if candidate.state != PodSandboxState::SandboxReady as i32 {
                continue;
            }
            let next = (candidate.created_at, candidate.id);
            match &newest {
                Some(current)
                    if current.0 > next.0 || (current.0 == next.0 && current.1 >= next.1) => {}
                _ => newest = Some(next),
            }
        }

        newest.map(|(_, id)| id)
    }

    pub(super) fn container_identity_key(container: &Container) -> Option<String> {
        let metadata = container.metadata.as_ref()?;
        let pod_uid = container
            .labels
            .get("io.kubernetes.pod.uid")
            .filter(|uid| !uid.is_empty())?;
        Some(format!("{}\u{1f}{}", pod_uid, metadata.name))
    }

    pub(super) fn retain_best_logical_containers(containers: Vec<Container>) -> Vec<Container> {
        let mut best: HashMap<String, Container> = HashMap::new();
        let mut passthrough = Vec::new();

        for container in containers {
            let Some(identity) = Self::container_identity_key(&container) else {
                passthrough.push(container);
                continue;
            };

            match best.entry(identity) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if Self::container_rank(&container) > Self::container_rank(entry.get()) {
                        entry.insert(container);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(container);
                }
            }
        }

        let mut deduped: Vec<_> = best.into_values().collect();
        deduped.extend(passthrough);
        deduped
    }

    pub(super) fn container_rank(container: &Container) -> (u8, i64) {
        let live = matches!(
            container.state,
            x if x == ContainerState::ContainerRunning as i32
                || x == ContainerState::ContainerCreated as i32
        ) as u8;
        (live, container.created_at)
    }

    pub(super) async fn live_pod_sandbox_state(
        &self,
        pod: &crate::proto::runtime::v1::PodSandbox,
    ) -> i32 {
        let current_state = pod.state;
        let pod_state =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY);
        let Some(pause_container_id) = pod_state
            .as_ref()
            .and_then(|state| state.pause_container_id.as_deref())
            .filter(|pause_container_id| !pause_container_id.is_empty())
        else {
            return current_state;
        };

        let pause_status = self
            .runtime_container_status_checked(pause_container_id)
            .await;
        let pause_has_active_runtime_state = self
            .runtime
            .container_has_active_runtime_state(pause_container_id)
            || self
                .runtime_container_pid_checked(pause_container_id)
                .await
                .is_some();
        let pod_has_required_netns = Self::pod_has_required_netns(pod);

        match pause_status {
            ContainerStatus::Running if pod_has_required_netns => {
                PodSandboxState::SandboxReady as i32
            }
            ContainerStatus::Unknown
                if current_state == PodSandboxState::SandboxReady as i32
                    && pause_has_active_runtime_state =>
            {
                current_state
            }
            _ => PodSandboxState::SandboxNotready as i32,
        }
    }

    pub(super) fn pod_network_status_from_state(
        state: Option<&StoredPodState>,
    ) -> Option<PodSandboxNetworkStatus> {
        let host_network = state
            .and_then(|pod| pod.namespace_options.as_ref())
            .map(|options| options.network == NamespaceMode::Node as i32)
            .unwrap_or(false);
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
                if host_network {
                    return Some(PodSandboxNetworkStatus {
                        ip: String::new(),
                        additional_ips: Vec::new(),
                    });
                }
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
        if let Some(driver) = self.config.cgroup_driver {
            return driver;
        }

        let systemd_active = std::path::Path::new("/run/systemd/system").exists()
            || std::fs::read_to_string("/proc/1/comm")
                .map(|content| content.trim() == "systemd")
                .unwrap_or(false);
        let cgroup_v2 = std::path::Path::new("/sys/fs/cgroup/cgroup.controllers").exists();
        let systemd_cgroup_layout = std::path::Path::new("/sys/fs/cgroup/system.slice").exists()
            || std::path::Path::new("/sys/fs/cgroup/user.slice").exists()
            || std::path::Path::new("/sys/fs/cgroup/systemd").exists();

        if systemd_active && (cgroup_v2 || systemd_cgroup_layout) {
            CgroupDriver::Systemd
        } else {
            CgroupDriver::Cgroupfs
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

    pub(super) fn cri_runtime_name(&self) -> &'static str {
        env!("CARGO_PKG_NAME")
    }

    pub(super) fn cri_runtime_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
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

    pub(super) fn runtime_feature_flags(&self) -> serde_json::Value {
        json!({
            "exec": true,
            "execSync": true,
            "attach": true,
            "portForward": true,
            "containerStats": true,
            "podSandboxStats": true,
            "podSandboxMetrics": true,
            // Until crius aligns its event stream with containerd/cri-o's
            // exit-driven semantics, advertise polling-only readiness to kubelet.
            "containerEvents": false,
            "podLifecycleEvents": false,
            "reopenContainerLog": true,
            "updateContainerResources": !self.config.disable_cgroup,
            "checkpointContainer": self.config.enable_criu_support,
        })
    }

    pub(super) fn security_availability_info(&self) -> serde_json::Value {
        let security = crate::security::SecurityManager::new();
        json!({
            "selinuxAvailable": security.is_selinux_available(),
            "apparmorAvailable": security.is_apparmor_available(),
            "seccompAvailable": security.is_seccomp_available(),
            "seccompNotifierSupported": security.is_seccomp_available(),
            "seccompNotifierBaseDir": self.seccomp_notifier_dir().display().to_string(),
            "seccompNotifierActiveContainers": self.seccomp_notifier_active_containers(),
        })
    }

    pub(super) async fn probe_cni_load_status(&self) -> crate::network::CniLoadStatus {
        let cni_config = self.current_cni_config();
        let mut cni = match crate::network::CniManager::new(
            cni_config
                .plugin_dirs()
                .iter()
                .map(|dir| dir.display().to_string())
                .collect(),
            cni_config
                .config_dirs()
                .iter()
                .map(|dir| dir.display().to_string())
                .collect(),
            cni_config.cache_dir().display().to_string(),
        ) {
            Ok(cni) => cni,
            Err(err) => {
                return crate::network::CniLoadStatus {
                    checked_at_unix_millis: chrono::Utc::now().timestamp_millis(),
                    ready: false,
                    reason: "CNIConfigLoadFailed".to_string(),
                    message: format!("failed to initialize CNI manager: {}", err),
                    discovered_files: Vec::new(),
                    invalid_files: Vec::new(),
                    loaded_networks: Vec::new(),
                    declared_plugins: Vec::new(),
                    missing_plugin_binaries: Vec::new(),
                    default_network_name: None,
                };
            }
        };
        cni.set_max_conf_num(cni_config.max_conf_num());
        cni.set_default_network_name(cni_config.default_network_name().map(ToOwned::to_owned));

        match cni.load_network_configs().await {
            Ok(status) => status,
            Err(err) => {
                cni.last_load_status()
                    .cloned()
                    .unwrap_or_else(|| crate::network::CniLoadStatus {
                        checked_at_unix_millis: chrono::Utc::now().timestamp_millis(),
                        ready: false,
                        reason: "CNIConfigLoadFailed".to_string(),
                        message: format!("failed to load CNI network configs: {}", err),
                        discovered_files: Vec::new(),
                        invalid_files: Vec::new(),
                        loaded_networks: Vec::new(),
                        declared_plugins: Vec::new(),
                        missing_plugin_binaries: Vec::new(),
                        default_network_name: None,
                    })
            }
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
        let req = request.into_inner();
        let actual_container_id = self.resolve_container_id(&req.container_id).await?;
        let container = {
            let containers = self.containers.lock().await;
            containers.get(&actual_container_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Container not found"))?;

        let runtime_status = self
            .runtime_container_status_checked(&actual_container_id)
            .await;
        let container = if matches!(runtime_status, crate::runtime::ContainerStatus::Stopped(_)) {
            self.finalize_container_stop_state(&actual_container_id, runtime_status.clone())
                .await?
                .unwrap_or(container)
        } else {
            container
        };
        let runtime_state = Self::effective_runtime_state_for_container(&container, runtime_status);
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
        let pod_meta_by_id: HashMap<String, (String, String, String, Option<String>)> = {
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
                    (
                        id.clone(),
                        (name, namespace, uid, pod.labels.get("component").cloned()),
                    )
                })
                .collect()
        };
        let container_snapshots: Vec<Container> = {
            let containers = self.containers.lock().await;
            containers.values().cloned().collect()
        };

        let mut containers_list = Vec::with_capacity(container_snapshots.len());
        for mut c in container_snapshots {
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
            if let Some((pod_name, pod_namespace, pod_uid, component)) =
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
                if let Some(component) = component.as_ref() {
                    c.labels
                        .entry("component".to_string())
                        .or_insert_with(|| component.clone());
                }
            }
            c.created_at = Self::normalize_timestamp_nanos(c.created_at);
            let runtime_status = self.runtime_container_status_checked(&c.id).await;
            if matches!(runtime_status, crate::runtime::ContainerStatus::Stopped(_)) {
                if let Some(updated) = self
                    .finalize_container_stop_state(&c.id, runtime_status.clone())
                    .await?
                {
                    c = updated;
                }
            }
            c.state = Self::effective_runtime_state_for_container(&c, runtime_status);
            if let Some((pod_name, pod_namespace, pod_uid, component)) =
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
                if let Some(component) = component.as_ref() {
                    c.labels
                        .entry("component".to_string())
                        .or_insert_with(|| component.clone());
                }
            }
            c.annotations = Self::external_container_annotations(&c.annotations);
            containers_list.push(c);
        }
        let mut containers_list: Vec<_> = containers_list
            .into_iter()
            .filter(|c| {
                if let Some(f) = &filter {
                    Self::container_matches_filter(c, f)
                } else {
                    true
                }
            })
            .collect();
        if filter.as_ref().is_none_or(|f| f.id.is_empty()) {
            containers_list = Self::retain_best_logical_containers(containers_list);
        }

        Ok(Response::new(ListContainersResponse {
            containers: containers_list,
        }))
    }

    pub(super) async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let req = request.into_inner();
        let (runtime_ready, runtime_reason, runtime_message) = self.runtime_readiness();
        let cni_load_status = self.probe_cni_load_status().await;
        let reload_state = self.current_reload_state();
        let reloadable_config = self.current_reloadable_config();
        let (mut network_ready, mut network_reason, mut network_message) =
            cni_load_status.condition();
        if let Some(error) = reload_state.last_reload_error.clone() {
            network_ready = false;
            network_reason = "ConfigReloadFailed".to_string();
            network_message = error;
        } else if let Some(error) = reload_state.last_cni_watch_error.clone() {
            network_ready = false;
            network_reason = "NetworkConfigReloadFailed".to_string();
            network_message = error;
        }
        let info = if req.verbose {
            let runtime_network_config = self.runtime_network_config.lock().await.clone();
            let resource_class_support = crate::security::resource_classes::feature_support(Some(
                &self.nri_config.blockio_config_path,
            ));
            let payload = json!({
                "runtimeName": self.cri_runtime_name(),
                "runtimeVersion": self.cri_runtime_version(),
                "runtimeApiVersion": "v1",
                "rootDir": self.config.root_dir.display().to_string(),
                "runtime": self.config.runtime.clone(),
                "defaultRuntimeHandler": self.config.runtime.clone(),
                "runtimePath": self.config.runtime_path.display().to_string(),
                "ociRuntimeVersion": self.runtime_binary_version(),
                "runtimeRoot": self.config.runtime_root.display().to_string(),
                "attachSocketDir": self.config.attach_socket_dir.display().to_string(),
                "containerExitsDir": self.config.container_exits_dir.display().to_string(),
                "containerStopTimeoutSeconds": self.config.container_stop_timeout,
                "cleanShutdownFile": self.clean_shutdown_file.display().to_string(),
                "versionFile": self.version_file.display().to_string(),
                "versionFilePersist": self.version_file_persist.display().to_string(),
                "criuPath": self.config.criu_path.display().to_string(),
                "criuImagePath": self.config.criu_image_path.display().to_string(),
                "criuWorkPath": self.config.criu_work_path.display().to_string(),
                "enableCriuSupport": self.config.enable_criu_support,
                "internalWipe": self.config.internal_wipe,
                "internalRepair": self.config.internal_repair,
                "bindMountPrefix": self.config.bind_mount_prefix.display().to_string(),
                "uidMappings": self
                    .config
                    .uid_mappings
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|mapping| json!({
                        "containerId": mapping.container_id,
                        "hostId": mapping.host_id,
                        "length": mapping.length,
                    }))
                    .collect::<Vec<_>>(),
                "gidMappings": self
                    .config
                    .gid_mappings
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|mapping| json!({
                        "containerId": mapping.container_id,
                        "hostId": mapping.host_id,
                        "length": mapping.length,
                    }))
                    .collect::<Vec<_>>(),
                "minimumMappableUid": self.config.minimum_mappable_uid,
                "minimumMappableGid": self.config.minimum_mappable_gid,
                "ioUid": self.config.io_uid,
                "ioGid": self.config.io_gid,
                "disableCgroup": self.config.disable_cgroup,
                "tolerateMissingHugetlbController": self.config.tolerate_missing_hugetlb_controller,
                "separatePullCgroup": self.config.separate_pull_cgroup,
                "pullCgroup": {
                    "effective": self.image_service.pull_cgroup_effective_config(),
                    "lastScope": self.image_service.last_pull_cgroup_scope(),
                },
                "pidsLimit": self.config.pids_limit,
                "infraCtrCpuset": self.config.infra_ctr_cpuset.clone(),
                "sharedCpuset": self.config.shared_cpuset.clone(),
                "execCpuAffinity": self.config.exec_cpu_affinity,
                "irqbalanceConfigFile": self.config.irqbalance_config_file.display().to_string(),
                "irqbalanceConfigRestoreFile": self.config.irqbalance_config_restore_file.clone(),
                "irqbalanceRestore": self
                    .last_irqbalance_restore_status()
                    .map(|status| json!({
                        "attempted": status.attempted,
                        "restored": status.restored,
                        "message": status.message,
                    }))
                    .unwrap_or_else(|| json!(null)),
                "readOnly": self.config.read_only,
                "noPivot": self.config.no_pivot,
                "noNewKeyring": self.config.no_new_keyring,
                "logDir": self.config.log_dir.display().to_string(),
                "imageRoot": self.config.image_root.display().to_string(),
                "imageDriver": self.config.image_driver.clone(),
                "imageGlobalAuthFile": self.config.image_global_auth_file.display().to_string(),
                "imageNamespacedAuthDir": self
                    .config
                    .image_namespaced_auth_dir
                    .display()
                    .to_string(),
                "imageDefaultTransport": self.config.image_default_transport.clone(),
                "imageShortNameMode": self.config.image_short_name_mode.clone(),
                "imagePullProgressTimeoutMillis": self
                    .config
                    .image_pull_progress_timeout
                    .as_millis(),
                "imageMaxConcurrentDownloads": self.config.image_max_concurrent_downloads,
                "imagePullRetryCount": self.config.image_pull_retry_count,
                "imageRegistryConfigDir": self
                    .config
                    .image_registry_config_dir
                    .display()
                    .to_string(),
                "imageDecryptionKeysPath": self
                    .config
                    .image_decryption_keys_path
                    .display()
                    .to_string(),
                "imageDecryptionDecoderPath": self.config.image_decryption_decoder_path.clone(),
                "imageDecryptionKeyproviderConfig": self
                    .config
                    .image_decryption_keyprovider_config
                    .display()
                    .to_string(),
                "imageAdditionalArtifactStores": self
                    .config
                    .image_additional_artifact_stores
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
                "imageSignaturePolicy": self
                    .config
                    .image_signature_policy
                    .display()
                    .to_string(),
                "imageSignaturePolicyDir": self
                    .config
                    .image_signature_policy_dir
                    .display()
                    .to_string(),
                "imageStorageOptions": self.config.image_storage_options.clone(),
                "imageVolumes": self.config.image_volumes.clone(),
                "pinnedImages": self.config.image_pinned_images.clone(),
                "imageBigFilesTemporaryDir": self
                    .config
                    .image_big_files_temporary_dir
                    .display()
                    .to_string(),
                "imageLayout": {
                    "mode": "single-store-root",
                    "root": self.config.image_root.display().to_string(),
                    "imageRecordPathPattern": self
                        .config
                        .image_root
                        .join("images")
                        .join("<imageID>")
                        .display()
                        .to_string(),
                    "separateImageStoreSupported": false
                },
                "imageSnapshotModel": {
                    "snapshotter": "internal-overlay-untar",
                    "externalSnapshotterSupported": false,
                    "runtimeSnapshotterOverrideSupported": true,
                    "snapshotAnnotationPassthrough": false,
                    "discardUnpackedLayers": false,
                    "pullOptionPassthrough": false
                },
                "ociArtifactMountSupport": self.config.image_oci_artifact_mount_support,
                "imageDecryption": {
                    "enabled": !self.config.image_decryption_keys_path.as_os_str().is_empty(),
                    "keyModel": if self.config.image_decryption_keys_path.as_os_str().is_empty() {
                        ""
                    } else {
                        "node"
                    },
                },
                "snapshotStatsCollection": {
                    "strategy": "on-demand-rootfs-walk",
                    "backgroundCollector": false,
                    "containerStatsPeriodSeconds": self.config.stats_collection_period,
                    "podSandboxMetricsPeriodSeconds": self.config.pod_sandbox_metrics_collection_period
                },
                "maxContainerLogLineSize": self.config.max_container_log_line_size,
                "pauseImage": self.config.pause_image.clone(),
                "pauseCommand": self.config.pause_command.clone(),
                "dropInfraCtr": self.config.drop_infra_ctr,
                "pinnsPath": self
                    .config
                    .cni_config
                    .namespace_helper_path()
                    .map(|path| path.display().to_string()),
                "shimPidfilePattern": self
                    .shim_work_dir
                    .join("<containerID>")
                    .join("shim.pid")
                    .display()
                    .to_string(),
                "logToJournald": self.config.log_to_journald,
                "noSyncLog": self.config.no_sync_log,
                "enablePodEvents": self.config.enable_pod_events,
                "includedPodMetrics": self.config.included_pod_metrics,
                "statsCollectionPeriodSeconds": self.config.stats_collection_period,
                "podSandboxMetricsCollectionPeriodSeconds": self
                    .config
                    .pod_sandbox_metrics_collection_period,
                "restrictOomScoreAdj": self.config.restrict_oom_score_adj,
                "enableUnprivilegedPorts": self.config.enable_unprivileged_ports,
                "enableUnprivilegedIcmp": self.config.enable_unprivileged_icmp,
                "rootless": {
                    "enabled": self.config.rootless.enabled,
                    "currentUid": self.config.rootless.current_uid,
                    "currentGid": self.config.rootless.current_gid,
                    "inUserNamespace": self.config.rootless.in_user_namespace,
                    "xdgRuntimeDir": self.config.rootless.xdg_runtime_dir.display().to_string(),
                    "xdgDataHome": self.config.rootless.xdg_data_home.display().to_string(),
                    "storageRoot": self.config.rootless.storage_root.display().to_string(),
                    "runtimeRoot": self.config.rootless.runtime_root.display().to_string(),
                    "netnsDir": self.config.rootless.netns_dir.display().to_string(),
                    "networkMode": self.config.rootless.network_mode.as_str(),
                    "useFuseOverlayfs": self.config.rootless.use_fuse_overlayfs,
                    "disableCgroup": self.config.rootless.disable_cgroup,
                    "tolerateMissingHugetlbController": self
                        .config
                        .rootless
                        .tolerate_missing_hugetlb_controller,
                    "slirp4netnsPath": self.config.rootless.slirp4netns_path.display().to_string(),
                    "pastaPath": self.config.rootless.pasta_path.display().to_string(),
                },
                "cniMaxConfNum": self.config.cni_config.max_conf_num(),
                "cniConfTemplate": self
                    .config
                    .cni_config
                    .conf_template()
                    .map(|path| path.to_string_lossy().to_string()),
                "cniIpPref": self.config.cni_config.ip_pref().as_str(),
                "netnsMountDir": self
                    .config
                    .cni_config
                    .netns_mount_dir()
                    .display()
                    .to_string(),
                "netnsMountsUnderStateDir": self
                    .config
                    .cni_config
                    .netns_mounts_under_state_dir(),
                "disableHostportMapping": self.config.cni_config.disable_hostport_mapping(),
                "monitorEnv": self.config.monitor_env.clone(),
                "monitorCgroup": self.config.monitor_cgroup.clone(),
                "defaultEnv": self
                    .config
                    .default_env
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>(),
                "defaultCapabilities": self.config.default_capabilities.clone(),
                "defaultSysctls": self.config.default_sysctls.clone(),
                "defaultUlimits": self
                    .config
                    .default_ulimits
                    .iter()
                    .map(|limit| format!("{}={}:{}", limit.rtype, limit.soft, limit.hard))
                    .collect::<Vec<_>>(),
                "allowedDevices": self
                    .config
                    .allowed_devices
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
                "additionalDevices": self
                    .config
                    .additional_devices
                    .iter()
                    .map(|device| format!(
                        "{}:{}:{}",
                        device.source.display(),
                        device.destination.display(),
                        device.permissions
                    ))
                    .collect::<Vec<_>>(),
                "deviceOwnershipFromSecurityContext": self
                    .config
                    .device_ownership_from_security_context,
                "addInheritableCapabilities": self.config.add_inheritable_capabilities,
                "defaultMountsFile": self
                    .config
                    .default_mounts_file
                    .as_os_str()
                    .is_empty()
                    .then_some(serde_json::Value::Null)
                    .unwrap_or_else(|| json!(self.config.default_mounts_file.display().to_string())),
                "baseRuntimeSpecLoaded": self.config.base_runtime_spec.is_some(),
                "hooksDir": self
                    .config
                    .hooks_dir
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
                "absentMountSourcesToReject": self
                    .config
                    .absent_mount_sources_to_reject
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
                "disableProcMount": self.config.disable_proc_mount,
                "timezone": self.config.timezone.clone(),
                "grpcMaxSendMsgSize": self.config.grpc_max_send_msg_size,
                "grpcMaxRecvMsgSize": self.config.grpc_max_recv_msg_size,
                "metrics": {
                    "enabled": self.config.metrics_enable,
                    "host": self.config.metrics_host.clone(),
                    "port": self.config.metrics_port,
                    "socketPath": self.config.metrics_socket_path.display().to_string(),
                    "enableTls": self.config.metrics_enable_tls,
                    "tlsCertFile": self.config.metrics_tls_cert_file.display().to_string(),
                    "tlsKeyFile": self.config.metrics_tls_key_file.display().to_string(),
                    "tlsCaFile": self.config.metrics_tls_ca_file.display().to_string(),
                    "tlsMinVersion": self.config.metrics_tls_min_version.clone(),
                    "tlsCipherSuites": self.config.metrics_tls_cipher_suites.clone(),
                    "collectors": self.config.metrics_collectors.clone(),
                },
                "tracing": {
                    "enabled": self.config.tracing_enable,
                    "endpoint": self.config.tracing_endpoint.clone(),
                    "samplingRatePerMillion": self.config.tracing_sampling_rate_per_million,
                },
                "privilegedSeccompProfile": self.config.privileged_seccomp_profile.clone(),
                "apparmorDefaultProfile": self.config.apparmor_default_profile.clone(),
                "disableApparmor": self.config.disable_apparmor,
                "enableSelinux": self.config.enable_selinux,
                "selinuxCategoryRange": self.config.selinux_category_range,
                "hostnetworkDisableSelinux": self.config.hostnetwork_disable_selinux,
                "runtimeHandlers": self.config.runtime_handlers.clone(),
                "runtimeHandlerConfigs": self
                    .config
                    .runtime_configs
                    .iter()
                    .map(|(handler, config)| {
                        (
                            handler.clone(),
                            json!({
                                "backend": config.backend,
                                "runtimePath": config.runtime_path,
                                "runtimeConfigPath": config.runtime_config_path,
                                "runtimeRoot": config.runtime_root,
                                "platformRuntimePaths": config.platform_runtime_paths,
                                "monitorPath": config.monitor_path,
                                "monitorCgroup": config.monitor_cgroup,
                                "monitorEnv": config.monitor_env,
                                "streamWebsockets": config.stream_websockets,
                                "runtimeDetectedFeatures": self
                                    .runtime
                                    .runtime_for_handler(handler)
                                    .map(|runtime| serde_json::to_value(runtime.probe_runtime_features()).unwrap_or_else(|_| json!({"available": false, "error": "failed to encode runtime feature probe"})))
                                    .unwrap_or_else(|err| json!({
                                        "available": false,
                                        "error": format!("failed to resolve runtime handler: {}", err),
                                    })),
                                "allowedAnnotations": config.allowed_annotations,
                                "defaultAnnotations": config.default_annotations,
                                "privilegedWithoutHostDevices": config.privileged_without_host_devices,
                                "privilegedWithoutHostDevicesAllDevicesAllowed": config
                                    .privileged_without_host_devices_all_devices_allowed,
                                "containerCreateTimeoutSeconds": config.container_create_timeout,
                                "snapshotter": config.snapshotter,
                                "cniConfDir": self
                                    .config
                                    .cni_config
                                    .handler_config_dirs(handler)
                                    .and_then(|dirs| dirs.first())
                                    .map(|dir| dir.to_string_lossy().to_string()),
                                "cniMaxConfNum": self
                                    .config
                                    .cni_config
                                    .handler_max_conf_num(handler)
                                    .unwrap_or(self.config.cni_config.max_conf_num()),
                            }),
                        )
                    })
                    .collect::<serde_json::Map<String, serde_json::Value>>(),
                "workloads": self
                    .config
                    .workloads
                    .iter()
                    .map(|(name, workload)| {
                        (
                            name.clone(),
                            json!({
                                "activationAnnotation": workload.activation_annotation,
                                "annotationPrefix": workload.annotation_prefix,
                                "allowedAnnotations": workload.allowed_annotations,
                                "resources": {
                                    "cpuShares": workload.resources.cpu_shares,
                                    "cpuQuota": workload.resources.cpu_quota,
                                    "cpuPeriod": workload.resources.cpu_period,
                                    "cpusetCpus": workload.resources.cpuset_cpus,
                                    "cpuLimit": workload.resources.cpu_limit,
                                },
                            }),
                        )
                    })
                    .collect::<serde_json::Map<String, serde_json::Value>>(),
                "execSyncIoDrainTimeoutMillis": self.config.exec_sync_io_drain_timeout.as_millis(),
                "streaming": {
                    "address": self.config.streaming.address.clone(),
                    "port": self.config.streaming.port,
                    "enableTls": self.config.streaming.enable_tls,
                    "tlsCertFile": self.config.streaming.tls_cert_file.clone(),
                    "tlsKeyFile": self.config.streaming.tls_key_file.clone(),
                    "tlsCaFile": self.config.streaming.tls_ca_file.clone(),
                    "tlsMinVersion": self.config.streaming.tls_min_version.clone(),
                    "requestTokenTtlSeconds": self.config.streaming.request_token_ttl.as_secs(),
                    "portForwardStreamCreationTimeoutSeconds": self
                        .config
                        .streaming
                        .port_forward_stream_creation_timeout
                        .as_secs(),
                    "portForwardIdleTimeoutSeconds": self
                        .config
                        .streaming
                        .port_forward_idle_timeout
                        .as_secs(),
                },
                "runtimeFeatures": self.runtime_feature_flags(),
                "securityAvailability": self.security_availability_info(),
                "nri": {
                    "enabled": self.nri_config.enable,
                    "enableCdi": self.nri_config.enable_cdi,
                    "cdiSpecDirs": self.nri_config.cdi_spec_dirs,
                    "pluginPath": self.nri_config.plugin_path,
                    "pluginConfigPath": self.nri_config.plugin_config_path,
                    "blockioConfigPath": self.nri_config.blockio_config_path,
                },
                "reload": {
                    "strategy": "config-file-watch-and-cni-watch",
                    "signalReload": false,
                    "configFileWatch": reload_state.config_file_watch,
                    "configFilePath": self.config.config_path.as_ref().map(|path| {
                        path.display().to_string()
                    }),
                    "watcherActive": reload_state.watcher_active,
                    "cniWatchDirs": reload_state.cni_watch_dirs.clone(),
                    "reloadableFields": [
                        "runtime.pause_image",
                        "image.pinned_images",
                        "image.registry_config_dir",
                        "image.global_auth_file",
                        "image.namespaced_auth_dir",
                        "image.signature_policy",
                        "image.signature_policy_dir",
                        "image.decryption_keys_path",
                        "image.decryption_decoder_path",
                        "image.decryption_keyprovider_config",
                        "security.seccomp_profile",
                        "security.apparmor_default_profile",
                        "network.config_dirs",
                        "network.conf_template",
                        "network.max_conf_num",
                        "network.default_network_name",
                    ],
                    "current": {
                        "pauseImage": reloadable_config.pause_image.clone(),
                        "pinnedImages": reloadable_config.pinned_images.clone(),
                        "registryConfigDir": reloadable_config.registry_config_dir.display().to_string(),
                        "globalAuthFile": reloadable_config.global_auth_file.display().to_string(),
                        "namespacedAuthDir": reloadable_config.namespaced_auth_dir.display().to_string(),
                        "signaturePolicy": reloadable_config.signature_policy.display().to_string(),
                        "signaturePolicyDir": reloadable_config.signature_policy_dir.display().to_string(),
                        "decryptionKeysPath": reloadable_config.decryption_keys_path.display().to_string(),
                        "decryptionDecoderPath": reloadable_config.decryption_decoder_path.clone(),
                        "decryptionKeyproviderConfig": reloadable_config
                            .decryption_keyprovider_config
                            .display()
                            .to_string(),
                        "seccompProfile": reloadable_config.seccomp_profile.display().to_string(),
                        "apparmorDefaultProfile": reloadable_config.apparmor_default_profile.clone(),
                        "cniConfigDirs": reloadable_config
                            .cni_config_dirs
                            .iter()
                            .map(|dir| dir.display().to_string())
                            .collect::<Vec<_>>(),
                        "cniConfTemplate": reloadable_config
                            .cni_conf_template
                            .as_ref()
                            .map(|path| path.display().to_string()),
                        "cniMaxConfNum": reloadable_config.cni_max_conf_num,
                        "cniDefaultNetworkName": reloadable_config.cni_default_network_name.clone(),
                    },
                    "lastReloadAtUnixMillis": reload_state.last_reload_at_unix_millis,
                    "lastReloadSource": reload_state.last_reload_source.clone(),
                    "lastReloadFields": reload_state.last_reload_fields.clone(),
                    "lastReloadError": reload_state.last_reload_error.clone(),
                    "lastCniWatchAtUnixMillis": reload_state.last_cni_watch_at_unix_millis,
                    "lastCniWatchError": reload_state.last_cni_watch_error.clone(),
                    "runtimeConfigApiOnly": [
                        "UpdateRuntimeConfig.network_config.pod_cidr"
                    ],
                },
                "runtimeNetworkConfig": runtime_network_config.as_ref().map(|cfg| {
                    json!({
                        "podCIDR": cfg.pod_cidr,
                    })
                }),
                "lastCniLoadStatus": cni_load_status,
                "networkReady": network_ready,
                "networkReason": network_reason.clone(),
                "cgroupDriver": self.cgroup_driver().as_str_name(),
                "cgroupSupport": {
                    "activeVersion": Self::detected_cgroup_version(),
                    "resourceUpdateStrategy": "runtime-update-resources",
                    "disableCgroup": self.config.disable_cgroup,
                    "tolerateMissingHugetlbController": self.config.tolerate_missing_hugetlb_controller,
                    "resourceClasses": {
                        "blockio": {
                            "supported": resource_class_support.blockio_supported,
                            "configPath": resource_class_support
                                .blockio_config_path
                                .as_ref()
                                .map(|path| path.display().to_string())
                                .unwrap_or_default(),
                            "softFailure": "drop-class-when-config-missing",
                        },
                        "rdt": {
                            "supported": resource_class_support.rdt_supported,
                            "resctrlPath": resource_class_support.rdt_resctrl_path.display().to_string(),
                            "softFailure": "drop-class-when-resctrl-missing",
                        },
                    },
                    "drivers": {
                        "systemd": {
                            "supported": true,
                            "monitorCgroup": {
                                "default": "system.slice",
                                "acceptedValues": ["", "pod", "*.slice"],
                            },
                            "resourceUpdatePath": "runtime update --resources",
                        },
                        "cgroupfs": {
                            "supported": true,
                            "monitorCgroup": {
                                "default": "",
                                "acceptedValues": ["", "pod"],
                            },
                            "resourceUpdatePath": "runtime update --resources",
                        },
                    },
                    "versions": {
                        "v1": {
                            "supported": true,
                            "hierarchyMode": "legacy",
                            "hugetlbBehavior": if self.config.tolerate_missing_hugetlb_controller {
                                "best-effort"
                            } else {
                                "required"
                            },
                        },
                        "v2": {
                            "supported": true,
                            "hierarchyMode": "unified",
                            "hugetlbBehavior": if self.config.tolerate_missing_hugetlb_controller {
                                "best-effort"
                            } else {
                                "required"
                            },
                        },
                    },
                },
                "recovery": {
                    "enabled": true,
                    "startupReconcile": true,
                    "eventReplayOnRecovery": false,
                    "lastStartupWasCleanShutdown": self.last_startup_clean_shutdown(),
                    "lastStartupDetectedReboot": self.last_startup_detected_reboot(),
                    "lastStartupDetectedUpgrade": self.last_startup_detected_upgrade(),
                    "lastStartupAttemptedRepair": self.last_startup_attempted_repair(),
                    "lastStartupRepairSucceeded": self.last_startup_repair_succeeded(),
                    "internalWipe": self.config.internal_wipe,
                    "internalRepair": self.config.internal_repair,
                    "policy": {
                        "startupInputs": [
                            "cleanShutdownFile",
                            "versionFile",
                            "versionFilePersist",
                            "internalWipe",
                            "internalRepair",
                        ],
                        "wipeTriggers": [
                            "unclean-shutdown",
                            "reboot",
                            "upgrade",
                        ],
                        "wipeScope": [
                            "orphanRuntimeBundles",
                            "orphanShimArtifacts",
                            "orphanAttachArtifacts",
                            "orphanPodWorkspaces",
                        ],
                        "repairTriggers": [
                            "unclean-shutdown",
                        ],
                        "repairScope": "sqlite-persistence-only",
                        "repairActions": [
                            "integrity-check",
                            "reindex",
                            "vacuum",
                        ],
                    },
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
        let req = request.into_inner();
        let resolved_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let pod_sandbox = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&resolved_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
        let container_statuses = self.current_pod_container_snapshots(&resolved_id).await;
        let mut pod_sandbox = pod_sandbox;
        pod_sandbox.state = self.live_pod_sandbox_state(&pod_sandbox).await;
        if pod_sandbox.state == PodSandboxState::SandboxReady as i32
            && self
                .latest_ready_logical_pod_sandbox_id(&pod_sandbox)
                .await
                .is_some_and(|id| id != pod_sandbox.id)
        {
            pod_sandbox.state = PodSandboxState::SandboxNotready as i32;
        }
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
            // Do not advertise Evented PLEG pod-status support until runtime
            // events and timestamp semantics fully match kubelet expectations.
            timestamp: 0,
        }))
    }

    pub(super) async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
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
        let mut items: Vec<_> = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .values()
                .cloned()
                .map(|mut p| {
                    let pod_state = Self::read_internal_state::<StoredPodState>(
                        &p.annotations,
                        INTERNAL_POD_STATE_KEY,
                    );
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
                .collect()
        };
        for pod in &mut items {
            pod.state = self.live_pod_sandbox_state(pod).await;
            pod.annotations = Self::external_pod_annotations(&pod.annotations);
        }
        Self::demote_stale_ready_logical_pod_sandboxes(&mut items);
        items.retain(|pod| {
            if let Some(f) = &filter {
                Self::pod_sandbox_matches_filter(pod, f)
            } else {
                true
            }
        });
        if filter.as_ref().is_none_or(|f| f.id.is_empty()) {
            items = Self::retain_best_logical_pod_sandboxes(items);
        }
        items.sort_by(|a, b| {
            Self::sandbox_rank(b)
                .cmp(&Self::sandbox_rank(a))
                .then_with(|| b.id.cmp(&a.id))
        });
        Ok(Response::new(ListPodSandboxResponse { items }))
    }
}
