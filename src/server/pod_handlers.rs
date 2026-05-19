use super::*;

impl RuntimeServiceImpl {
    fn untrusted_workload_requested(config: &crate::proto::runtime::v1::PodSandboxConfig) -> bool {
        config
            .annotations
            .get(CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION)
            .map(|value| value.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    fn sandbox_requests_host_access(namespace_options: Option<&NamespaceOption>) -> bool {
        let Some(namespace_options) = namespace_options else {
            return false;
        };

        namespace_options.network == NamespaceMode::Node as i32
            || namespace_options.pid == NamespaceMode::Node as i32
            || namespace_options.ipc == NamespaceMode::Node as i32
    }

    pub(super) fn resolve_pod_runtime_handler(
        &self,
        config: &crate::proto::runtime::v1::PodSandboxConfig,
        requested_runtime_handler: &str,
        namespace_options: Option<&NamespaceOption>,
    ) -> Result<String, Status> {
        let requested_runtime_handler = requested_runtime_handler.trim();
        if !Self::untrusted_workload_requested(config) {
            return self.resolve_runtime_handler(requested_runtime_handler);
        }

        if !requested_runtime_handler.is_empty() && requested_runtime_handler != "untrusted" {
            return Err(Status::invalid_argument(
                "untrusted workload with explicit runtime handler is not allowed",
            ));
        }

        if Self::sandbox_requests_host_access(namespace_options) {
            return Err(Status::invalid_argument(
                "untrusted workload with host access is not allowed",
            ));
        }

        self.resolve_runtime_handler("untrusted")
    }

    pub(super) fn uid_map_indicates_user_namespace(uid_map: &str) -> bool {
        let uid_map = uid_map.trim();
        if uid_map.is_empty() {
            return true;
        }

        let mut fields = uid_map.split_whitespace();
        let Some(container_id) = fields.next().and_then(|value| value.parse::<u64>().ok()) else {
            return false;
        };
        let Some(host_id) = fields.next().and_then(|value| value.parse::<u64>().ok()) else {
            return false;
        };
        let Some(length) = fields.next().and_then(|value| value.parse::<u64>().ok()) else {
            return false;
        };

        !(container_id == 0 && host_id == 0 && length == 4_294_967_295)
    }

    fn running_in_user_namespace() -> bool {
        static IN_USER_NAMESPACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *IN_USER_NAMESPACE.get_or_init(|| {
            std::fs::read_to_string("/proc/self/uid_map")
                .ok()
                .and_then(|content| {
                    content
                        .lines()
                        .next()
                        .map(Self::uid_map_indicates_user_namespace)
                })
                .unwrap_or(false)
        })
    }

    fn pod_uses_user_namespace(namespace_options: Option<&NamespaceOption>) -> bool {
        matches!(
            namespace_options.and_then(|options| options.userns_options.as_ref()),
            Some(userns) if userns.mode != NamespaceMode::Node as i32
        )
    }

    pub(super) fn effective_pod_sysctls(
        &self,
        requested_sysctls: Option<&HashMap<String, String>>,
        namespace_options: Option<&NamespaceOption>,
    ) -> HashMap<String, String> {
        let mut sysctls = self.config.default_sysctls.clone();
        if let Some(requested_sysctls) = requested_sysctls {
            for (key, value) in requested_sysctls {
                sysctls.insert(key.clone(), value.clone());
            }
        }
        let host_network = matches!(
            namespace_options.map(|options| options.network),
            Some(mode) if mode == NamespaceMode::Node as i32
        );

        if host_network {
            return sysctls;
        }

        if self.config.enable_unprivileged_ports {
            sysctls
                .entry("net.ipv4.ip_unprivileged_port_start".to_string())
                .or_insert_with(|| "0".to_string());
        }

        if self.config.enable_unprivileged_icmp
            && !Self::pod_uses_user_namespace(namespace_options)
            && !Self::running_in_user_namespace()
        {
            sysctls
                .entry("net.ipv4.ping_group_range".to_string())
                .or_insert_with(|| "0 2147483647".to_string());
        }

        sysctls
    }

    fn map_run_pod_sandbox_error(err: anyhow::Error) -> Status {
        let message = err.to_string();
        for cause in err.chain() {
            if let Some(pod_error) = cause.downcast_ref::<crate::pod::PodSandboxError>() {
                return Status::failed_precondition(pod_error.to_string());
            }
        }
        if message.contains("pause image ")
            && message.contains("pull it before starting the pod sandbox")
        {
            return Status::failed_precondition(message);
        }
        if message.contains("pause image ") && message.contains("invalid or incomplete") {
            return Status::failed_precondition(message);
        }
        Status::internal(format!("Failed to create pod sandbox: {}", err))
    }

    fn requested_pause_image(
        &self,
        pod_config: &crate::proto::runtime::v1::PodSandboxConfig,
    ) -> String {
        pod_config
            .annotations
            .get("io.kubernetes.cri.sandbox-image")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| self.config.pause_image.clone())
    }

    fn pod_fallback_netns_name(
        pod: &crate::proto::runtime::v1::PodSandbox,
        pod_state: Option<&StoredPodState>,
    ) -> Option<String> {
        if !Self::pod_requires_managed_netns(pod_state) {
            return None;
        }

        pod_state
            .and_then(|state| state.netns_path.as_ref())
            .and_then(|path| Path::new(path).file_name())
            .and_then(|name| name.to_str())
            .map(|name| name.to_string())
            .or_else(|| {
                pod.metadata
                    .as_ref()
                    .map(|m| format!("crius-{}-{}", m.namespace, m.name))
            })
    }

    pub(super) async fn fallback_cleanup_pod_sandbox_resources(
        &self,
        pod_id: &str,
        pod: &crate::proto::runtime::v1::PodSandbox,
        pod_state: Option<&StoredPodState>,
        remove_workspace: bool,
    ) {
        if let Some(pause_container_id) = pod_state
            .and_then(|state| state.pause_container_id.as_deref())
            .filter(|pause_container_id| !pause_container_id.is_empty())
        {
            if let Err(err) = self
                .runtime
                .stop_container(pause_container_id, Some(self.config.container_stop_timeout))
            {
                log::debug!(
                    "Fallback pause stop failed for pod {} pause {}: {}",
                    pod_id,
                    pause_container_id,
                    err
                );
            }
            if let Err(err) = self.runtime.remove_container(pause_container_id) {
                log::debug!(
                    "Fallback pause remove failed for pod {} pause {}: {}",
                    pod_id,
                    pause_container_id,
                    err
                );
            }
        }

        if let Some(state) = pod_state {
            if self.config.cni_config.disable_hostport_mapping() {
                log::debug!(
                    "hostPort mapping is disabled; skipping fallback hostPort cleanup for pod {}",
                    pod_id
                );
            } else if let Ok(port_manager) = crate::network::PortMappingManager::auto() {
                for mapping in state.port_mappings.iter().rev() {
                    let protocol = match mapping.protocol.to_ascii_uppercase().as_str() {
                        "TCP" => crate::network::Protocol::Tcp,
                        "UDP" => crate::network::Protocol::Udp,
                        _ => continue,
                    };
                    let Some(ip) = state
                        .ip
                        .as_ref()
                        .and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
                    else {
                        continue;
                    };
                    let host_ip = if mapping.host_ip.trim().is_empty() {
                        None
                    } else {
                        mapping.host_ip.parse().ok()
                    };
                    let Ok(container_port) = u16::try_from(mapping.container_port) else {
                        continue;
                    };
                    let Ok(host_port) = u16::try_from(mapping.host_port) else {
                        continue;
                    };
                    let host_mapping = crate::network::PortMapping {
                        protocol,
                        container_port,
                        host_port,
                        host_ip,
                        container_ip: ip,
                    };
                    if let Err(err) = port_manager.remove_port_mapping(&host_mapping) {
                        log::debug!(
                            "Fallback hostPort cleanup failed for pod {} mapping {:?}: {}",
                            pod_id,
                            host_mapping,
                            err
                        );
                    }
                }
            }
        }

        if let Some(netns_name) = Self::pod_fallback_netns_name(pod, pod_state) {
            let network_manager =
                DefaultNetworkManager::from_cni_config(self.config.cni_config.clone());
            if let Some(netns_path) = pod_state
                .and_then(|state| state.netns_path.as_deref())
                .filter(|path| !path.is_empty())
            {
                let namespace = pod
                    .metadata
                    .as_ref()
                    .map(|m| m.namespace.clone())
                    .unwrap_or_else(|| "default".to_string());
                let name = pod
                    .metadata
                    .as_ref()
                    .map(|m| m.name.clone())
                    .unwrap_or_default();
                let uid = pod
                    .metadata
                    .as_ref()
                    .map(|m| m.uid.clone())
                    .unwrap_or_default();
                let runtime_handler = pod_state
                    .and_then(|state| {
                        (!state.runtime_handler.is_empty())
                            .then_some(state.runtime_handler.as_str())
                    })
                    .filter(|handler| !handler.is_empty())
                    .unwrap_or(pod.runtime_handler.as_str());
                if let Err(err) = network_manager
                    .teardown_pod_network(
                        pod_id,
                        netns_path,
                        &namespace,
                        &name,
                        &uid,
                        runtime_handler,
                    )
                    .await
                {
                    log::debug!("Fallback CNI teardown failed for pod {}: {}", pod_id, err);
                }
            }
            if let Err(err) = network_manager.remove_network_namespace(&netns_name).await {
                log::debug!(
                    "Fallback netns cleanup failed for pod {} netns {}: {}",
                    pod_id,
                    netns_name,
                    err
                );
            }
        }

        if remove_workspace {
            let pod_dir = self.config.root_dir.join("pods").join(pod_id);
            if pod_dir.exists() {
                if let Err(err) = tokio::fs::remove_dir_all(&pod_dir).await {
                    log::debug!(
                        "Fallback pod workspace cleanup failed for {}: {}",
                        pod_dir.display(),
                        err
                    );
                }
            }
        }
    }

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
        self.release_pod_name(pod_id);
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
        let pod_metadata = pod_config
            .metadata
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Pod config metadata not specified"))?;
        let pod_id = uuid::Uuid::new_v4().to_simple().to_string();
        let pod_name_key = Self::pod_name_key(pod_metadata);
        let mut pod_name_guard = self.reserve_pod_name(&pod_id, &pod_name_key)?;
        let linux_config = pod_config.linux.clone();
        let sandbox_security = linux_config
            .as_ref()
            .and_then(|linux| linux.security_context.as_ref());
        let effective_namespace_options = self.effective_userns_options(
            sandbox_security.and_then(|security| security.namespace_options.as_ref()),
        );
        let runtime_handler = self.resolve_pod_runtime_handler(
            &pod_config,
            req.runtime_handler.trim(),
            effective_namespace_options.as_ref(),
        )?;
        let run_as_user = sandbox_security
            .and_then(|security| security.run_as_user.as_ref())
            .map(|user| user.value.to_string());
        let run_as_group = sandbox_security
            .and_then(|security| security.run_as_group.as_ref())
            .and_then(|group| u32::try_from(group.value).ok());
        let supplemental_groups: Vec<u32> = sandbox_security
            .map(|security| {
                security
                    .supplemental_groups
                    .iter()
                    .filter_map(|group| u32::try_from(*group).ok())
                    .collect()
            })
            .unwrap_or_default();
        self.validate_minimum_mappable_ids(
            effective_namespace_options.as_ref(),
            run_as_user.as_deref(),
            run_as_group,
            &supplemental_groups,
        )?;
        let mut pod_overhead = linux_config
            .as_ref()
            .and_then(|linux| linux.overhead.clone());
        let mut pod_linux_resources = linux_config
            .as_ref()
            .and_then(|linux| linux.resources.clone());
        if let Some(overhead) = pod_overhead.as_mut() {
            self.clamp_proto_oom_score_adj(overhead)?;
        }
        if let Some(resources) = pod_linux_resources.as_mut() {
            self.clamp_proto_oom_score_adj(resources)?;
        }
        let mut effective_pod_linux_resources = Self::effective_pod_linux_resources(
            pod_overhead.as_ref(),
            pod_linux_resources.as_ref(),
        );
        if let Some(resources) = effective_pod_linux_resources.as_mut() {
            self.clamp_proto_oom_score_adj(resources)?;
        }
        let cgroup_support = Self::cgroup_support_flags();
        Self::validate_proto_hugetlb_limits_with_flags(
            effective_pod_linux_resources.as_ref(),
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
            "pod sandbox create",
        )?;
        if let Some(resources) = effective_pod_linux_resources.as_mut() {
            Self::sanitize_proto_runtime_resources_with_flags(
                resources,
                cgroup_support,
                self.config.tolerate_missing_hugetlb_controller,
            );
        }
        let effective_pids_limit = self.effective_pids_limit(None)?;
        let pod_privileged = sandbox_security
            .map(|security| security.privileged)
            .unwrap_or(false);
        let managed_netns = sandbox_security
            .and(effective_namespace_options.as_ref())
            .map(|options| options.network != NamespaceMode::Node as i32)
            .unwrap_or(true);
        let pod_metadata = pod_config.metadata.as_ref();
        let pod_selinux_seed = format!(
            "{}:{}:{}",
            pod_metadata
                .map(|metadata| metadata.namespace.as_str())
                .unwrap_or("default"),
            pod_metadata
                .map(|metadata| metadata.name.as_str())
                .unwrap_or("pod"),
            pod_metadata
                .map(|metadata| metadata.uid.as_str())
                .unwrap_or("uid"),
        );
        let pod_selinux_label = self.effective_selinux_label_from_proto(
            sandbox_security.and_then(|security| security.selinux_options.as_ref()),
            !managed_netns,
            Some(&pod_selinux_seed),
        );
        let pod_seccomp_profile = self.effective_seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            Self::legacy_linux_sandbox_seccomp_profile_path(sandbox_security),
            pod_privileged,
        );
        let stored_seccomp_profile = self.effective_stored_seccomp_profile_from_proto(
            sandbox_security.and_then(|security| security.seccomp.as_ref()),
            Self::legacy_linux_sandbox_seccomp_profile_path(sandbox_security),
            pod_privileged,
        );
        let pod_apparmor_profile = self.effective_apparmor_profile_from_proto(
            sandbox_security.and_then(|security| security.apparmor.as_ref()),
            "",
            pod_privileged,
        )?;
        let effective_sysctls = self.effective_pod_sysctls(
            linux_config.as_ref().map(|linux| &linux.sysctls),
            effective_namespace_options.as_ref(),
        );
        let runtime_network_config = self.runtime_network_config.lock().await.clone();
        let runtime_pod_cidr = managed_netns
            .then(|| {
                runtime_network_config
                    .as_ref()
                    .map(|cfg| cfg.pod_cidr.clone())
            })
            .flatten();
        let mut sandbox_annotations: HashMap<String, String> = pod_config
            .annotations
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.apply_runtime_handler_default_annotations(&mut sandbox_annotations, &runtime_handler);
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

        let readonly_rootfs = self.effective_readonly_rootfs(
            sandbox_security
                .map(|security| security.readonly_rootfs)
                .unwrap_or(false),
        );
        let stored_effective_pod_linux_resources = {
            let mut stored = effective_pod_linux_resources
                .as_ref()
                .map(StoredLinuxResources::from);
            if let Some(limit) = effective_pids_limit {
                stored.get_or_insert_with(Default::default).pids_limit = Some(limit);
            }
            stored
        };

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
            dns_config: pod_config
                .dns_config
                .clone()
                .map(|d| crate::pod::DNSConfig {
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
            sysctls: effective_sysctls.clone(),
            namespace_options: effective_namespace_options.clone(),
            privileged: pod_privileged,
            run_as_user: run_as_user.clone(),
            run_as_group,
            supplemental_groups: supplemental_groups.clone(),
            readonly_rootfs,
            pids_limit: effective_pids_limit,
            no_new_privileges: None,
            apparmor_profile: pod_apparmor_profile.clone(),
            selinux_label: pod_selinux_label.clone(),
            seccomp_profile: pod_seccomp_profile.clone(),
            linux_resources: effective_pod_linux_resources.clone(),
        };
        let stored_port_mappings: Vec<StoredPortMapping> = sandbox_config
            .port_mappings
            .iter()
            .map(|mapping| StoredPortMapping {
                protocol: mapping.protocol.clone(),
                container_port: mapping.container_port,
                host_port: mapping.host_port,
                host_ip: mapping.host_ip.clone(),
            })
            .collect();
        let pause_image = self.requested_pause_image(&pod_config);
        self.image_service
            .ensure_image_exists_for_sandbox(&pause_image, &pod_config)
            .await
            .map_err(|status| {
                Status::new(
                    status.code(),
                    format!(
                        "failed to ensure pause image {} for pod sandbox: {}",
                        pause_image,
                        status.message()
                    ),
                )
            })?;

        let mut pod_manager = self.pod_manager.lock().await;
        let pod_id = pod_manager
            .create_pod_sandbox_with_id(pod_id.clone(), sandbox_config)
            .await
            .map_err(Self::map_run_pod_sandbox_error)?;
        let created_pod = pod_manager.get_pod_sandbox_cloned(&pod_id);
        drop(pod_manager);

        let pod_state = created_pod
            .as_ref()
            .map(|pod| StoredPodState {
                port_mappings: stored_port_mappings.clone(),
                raw_cni_result: pod
                    .network_status
                    .as_ref()
                    .and_then(|status| status.raw_result.clone()),
                hostname: (!pod.config.hostname.is_empty()).then(|| pod.config.hostname.clone()),
                log_directory: if pod_config.log_directory.is_empty() {
                    None
                } else {
                    Some(pod_config.log_directory.clone())
                },
                runtime_handler: runtime_handler.clone(),
                runtime_pod_cidr: runtime_pod_cidr.clone(),
                netns_path: (!pod.netns_path.as_os_str().is_empty())
                    .then(|| pod.netns_path.to_string_lossy().to_string()),
                pause_container_id: Some(pod.pause_container_id.clone()),
                ip: if pod.ip.is_empty() {
                    None
                } else {
                    Some(pod.ip.clone())
                },
                additional_ips: pod
                    .network_status
                    .as_ref()
                    .and_then(|status| {
                        status.raw_result.as_ref().map(|raw| {
                            crate::network::CniManager::ordered_result_ips(raw)
                                .into_iter()
                                .map(|ip| ip.to_string())
                                .filter(|ip| pod.ip.is_empty() || ip != &pod.ip)
                                .collect::<Vec<_>>()
                        })
                    })
                    .unwrap_or_else(|| {
                        pod.network_status
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
                            .unwrap_or_default()
                    }),
                cgroup_parent: linux_config.as_ref().and_then(|linux| {
                    (!linux.cgroup_parent.is_empty()).then(|| linux.cgroup_parent.clone())
                }),
                sysctls: effective_sysctls.clone(),
                namespace_options: effective_namespace_options
                    .as_ref()
                    .map(StoredNamespaceOptions::from),
                privileged: pod_privileged,
                run_as_user: run_as_user.clone(),
                run_as_group,
                supplemental_groups: supplemental_groups.clone(),
                readonly_rootfs,
                no_new_privileges: None,
                apparmor_profile: pod_apparmor_profile.clone(),
                selinux_label: pod_selinux_label.clone(),
                seccomp_profile: stored_seccomp_profile.clone(),
                overhead_linux_resources: pod_overhead.as_ref().map(StoredLinuxResources::from),
                linux_resources: stored_effective_pod_linux_resources.clone(),
                stop_notified: false,
                broken: None,
            })
            .unwrap_or_else(|| StoredPodState {
                port_mappings: stored_port_mappings,
                raw_cni_result: None,
                hostname: (!pod_config.hostname.is_empty()).then(|| pod_config.hostname.clone()),
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
                sysctls: effective_sysctls,
                namespace_options: effective_namespace_options
                    .as_ref()
                    .map(StoredNamespaceOptions::from),
                privileged: pod_privileged,
                run_as_user,
                run_as_group,
                supplemental_groups,
                readonly_rootfs,
                no_new_privileges: None,
                apparmor_profile: pod_apparmor_profile,
                selinux_label: pod_selinux_label.clone(),
                seccomp_profile: stored_seccomp_profile.clone(),
                overhead_linux_resources: pod_overhead.as_ref().map(StoredLinuxResources::from),
                linux_resources: stored_effective_pod_linux_resources,
                ..Default::default()
            });
        log::info!(
            "RunPodSandbox resolved cgroup_parent for pod {}: {:?}",
            pod_id,
            pod_state.cgroup_parent
        );
        let mut stored_annotations = sandbox_annotations;
        Self::insert_internal_state(&mut stored_annotations, INTERNAL_POD_STATE_KEY, &pod_state)?;

        let pod_sandbox = crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: pod_config.metadata.clone(),
            state: PodSandboxState::SandboxReady as i32,
            created_at: created_pod
                .as_ref()
                .map(|pod| Self::normalize_timestamp_nanos(pod.created_at))
                .unwrap_or_else(Self::now_nanos),
            labels: pod_config.labels.clone(),
            annotations: stored_annotations.clone(),
            runtime_handler: runtime_handler.clone(),
        };

        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.insert(pod_id.clone(), pod_sandbox.clone());
        drop(pod_sandboxes);

        let fallback_netns_name = format!(
            "crius-{}-{}",
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
        let fallback_netns_path = self
            .config
            .cni_config
            .netns_path(&fallback_netns_name)
            .display()
            .to_string();
        let netns_path = if managed_netns {
            pod_state.netns_path.clone().unwrap_or(fallback_netns_path)
        } else {
            String::new()
        };
        let pod_root_dir_for_persist = self.config.root_dir.join("pods").join(&pod_id);
        let current_pod = pod_sandbox.clone();
        let pod_state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
            &current_pod.annotations,
            INTERNAL_POD_STATE_KEY,
        )
        .unwrap_or_default();
        let state = if current_pod.state == PodSandboxState::SandboxReady as i32 {
            "ready"
        } else {
            "notready"
        };
        let netns_path = if RuntimeServiceImpl::pod_requires_managed_netns(Some(&pod_state)) {
            pod_state.netns_path.clone().unwrap_or(netns_path)
        } else {
            String::new()
        };
        let metadata = current_pod.metadata.clone().unwrap_or_default();
        let mut artifacts = vec![crate::storage::RuntimeArtifactRecord {
            owner_kind: "pod".to_string(),
            owner_id: pod_id.clone(),
            artifact_kind: "workspace".to_string(),
            path: pod_root_dir_for_persist.display().to_string(),
            runtime_handler: Some(current_pod.runtime_handler.clone().trim().to_string())
                .filter(|value| !value.is_empty()),
            runtime_root: None,
        }];
        if !netns_path.is_empty() {
            artifacts.push(crate::storage::RuntimeArtifactRecord {
                owner_kind: "pod".to_string(),
                owner_id: pod_id.clone(),
                artifact_kind: "netns".to_string(),
                path: netns_path.clone(),
                runtime_handler: Some(current_pod.runtime_handler.clone().trim().to_string())
                    .filter(|value| !value.is_empty()),
                runtime_root: None,
            });
        }
        {
            let mut persistence = self.persistence.lock().await;
            if let Err(err) = persistence.save_pod_sandbox(
                &pod_id,
                state,
                &metadata.name,
                &metadata.namespace,
                &metadata.uid,
                &netns_path,
                &current_pod.labels,
                &current_pod.annotations,
                pod_state.pause_container_id.as_deref(),
                pod_state.ip.as_deref(),
            ) {
                drop(persistence);
                self.rollback_failed_pod_sandbox_run(&pod_id).await;
                return Err(Status::internal(format!(
                    "Failed to persist pod sandbox {}: {}",
                    pod_id, err
                )));
            }
            if let Err(err) = persistence.replace_runtime_artifacts("pod", &pod_id, &artifacts) {
                drop(persistence);
                self.rollback_failed_pod_sandbox_run(&pod_id).await;
                return Err(Status::internal(format!(
                    "Failed to persist runtime artifacts for pod sandbox {}: {}",
                    pod_id, err
                )));
            }
        }
        log::info!("Pod sandbox {} persisted to database", pod_id);
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
        pod_name_guard.disarm();
        Ok(Response::new(RunPodSandboxResponse {
            pod_sandbox_id: pod_id,
        }))
    }

    pub(super) async fn update_pod_sandbox_resources(
        &self,
        request: Request<UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<UpdatePodSandboxResourcesResponse>, Status> {
        if self.config.disable_cgroup {
            return Err(self.cgroup_updates_disabled_status());
        }
        let _sync_block = self.nri.block_plugin_sync().await;
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        let mut overhead = req.overhead;
        let mut resources = req.resources;
        if let Some(overhead_resources) = overhead.as_mut() {
            self.clamp_proto_oom_score_adj(overhead_resources)?;
        }
        if let Some(container_resources) = resources.as_mut() {
            self.clamp_proto_oom_score_adj(container_resources)?;
        }
        let mut effective_resources =
            Self::effective_pod_linux_resources(overhead.as_ref(), resources.as_ref());
        if let Some(effective) = effective_resources.as_mut() {
            self.clamp_proto_oom_score_adj(effective)?;
        }
        let (pod_state_value, pod_runtime_state, pause_container_id) = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            let pod = pod_sandboxes
                .get(&pod_id)
                .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
            let pod_state = Self::read_internal_state::<StoredPodState>(
                &pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .unwrap_or_default();
            (
                pod_state.clone(),
                pod.state,
                pod_state
                    .pause_container_id
                    .clone()
                    .filter(|pause_container_id| !pause_container_id.is_empty()),
            )
        };
        if pod_runtime_state != PodSandboxState::SandboxReady as i32 {
            return Err(Status::failed_precondition(format!(
                "pod sandbox {} is not ready",
                pod_id
            )));
        }
        if effective_resources.is_some() && pause_container_id.is_none() {
            return Err(Status::failed_precondition(format!(
                "pod sandbox {} has no pause container for resource update",
                pod_id
            )));
        }
        let cgroup_support = Self::cgroup_support_flags();
        Self::validate_proto_hugetlb_limits_with_flags(
            effective_resources.as_ref(),
            cgroup_support,
            self.config.tolerate_missing_hugetlb_controller,
            "pod sandbox resource update",
        )?;
        if let Some(effective) = effective_resources.as_mut() {
            Self::sanitize_proto_runtime_resources_with_flags(
                effective,
                cgroup_support,
                self.config.tolerate_missing_hugetlb_controller,
            );
        }
        self.nri
            .update_pod_sandbox(
                self.nri_pod_update_event(&pod_id, overhead.as_ref(), resources.as_ref())
                    .await,
            )
            .await
            .map_err(|e| Status::internal(format!("NRI UpdatePodSandbox failed: {}", e)))?;

        if let (Some(pause_container_id), Some(resources)) =
            (pause_container_id.as_deref(), effective_resources.as_ref())
        {
            let mut stored_resources = StoredLinuxResources::from(resources);
            self.clamp_stored_oom_score_adj(&mut stored_resources)?;
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
            .unwrap_or(pod_state_value);
            state.overhead_linux_resources = overhead.as_ref().map(StoredLinuxResources::from);
            state.linux_resources = effective_resources.as_ref().map(StoredLinuxResources::from);
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
        let pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes
                .get(&pod_id)
                .cloned()
                .ok_or_else(|| Status::not_found("Pod sandbox not found"))?
        };

        let pod_state =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY);
        if pod.state != PodSandboxState::SandboxReady as i32
            && pod_state
                .as_ref()
                .map(|state| state.stop_notified)
                .unwrap_or(false)
        {
            return Ok(Response::new(StopPodSandboxResponse {}));
        }

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

        let cleaned_via_manager = {
            let mut pod_manager = self.pod_manager.lock().await;
            let has_entry = pod_manager.get_pod_sandbox(&pod_id).is_some();
            if has_entry {
                pod_manager
                    .stop_pod_sandbox(&pod_id)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to stop pod sandbox: {}", e)))?;
            }
            has_entry
        };
        if !cleaned_via_manager {
            self.fallback_cleanup_pod_sandbox_resources(&pod_id, &pod, pod_state.as_ref(), false)
                .await;
        }

        let updated_pod = {
            let mut pod_sandboxes = self.pod_sandboxes.lock().await;
            if let Some(pod) = pod_sandboxes.get_mut(&pod_id) {
                pod.state = PodSandboxState::SandboxNotready as i32;
                let mut state = Self::read_internal_state::<StoredPodState>(
                    &pod.annotations,
                    INTERNAL_POD_STATE_KEY,
                )
                .unwrap_or_default();
                state.stop_notified = true;
                Self::insert_internal_state(&mut pod.annotations, INTERNAL_POD_STATE_KEY, &state)?;
                Some(pod.clone())
            } else {
                None
            }
        };

        let pod_stop_event = self.nri_pod_event(&pod_id).await;
        if let Err(err) = self.nri.stop_pod_sandbox(pod_stop_event).await {
            log::warn!("NRI StopPodSandbox failed for {}: {}", pod_id, err);
        }

        if let Some(updated_pod) = updated_pod {
            let netns_path = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
                &updated_pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .and_then(|state| state.netns_path)
            .unwrap_or_default();
            let pause_container_id = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
                &updated_pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .and_then(|state| state.pause_container_id);
            let ip = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
                &updated_pod.annotations,
                INTERNAL_POD_STATE_KEY,
            )
            .and_then(|state| state.ip);
            let mut persistence = self.persistence.lock().await;
            if let Err(err) = persistence.save_pod_sandbox(
                &updated_pod.id,
                "notready",
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
                pause_container_id.as_deref(),
                ip.as_deref(),
            ) {
                log::error!("Failed to persist stopped pod {}: {}", updated_pod.id, err);
            }
            drop(persistence);
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
        let pod_state = existing_pod.as_ref().and_then(|pod| {
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY)
        });
        let pod_remove_event = self.nri_pod_event(&pod_id).await;
        let existing_container_statuses = self.current_pod_container_snapshots(&pod_id).await;

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

        let cleaned_via_manager = {
            let mut pod_manager = self.pod_manager.lock().await;
            let has_entry = pod_manager.get_pod_sandbox(&pod_id).is_some();
            if has_entry {
                pod_manager.remove_pod_sandbox(&pod_id).await.map_err(|e| {
                    Status::internal(format!("Failed to remove pod sandbox: {}", e))
                })?;
            }
            has_entry
        };

        if !cleaned_via_manager {
            if let Some(pod) = existing_pod.as_ref() {
                self.fallback_cleanup_pod_sandbox_resources(&pod_id, pod, pod_state.as_ref(), true)
                    .await;
            }
        }

        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.remove(&pod_id);
        drop(pod_sandboxes);
        if let Ok(mut removed) = self.removed_pod_sandbox_ids.lock() {
            removed.insert(pod_id.clone());
        }
        self.release_pod_name(&pod_id);

        let mut persistence = self.persistence.lock().await;
        if let Err(e) = persistence.delete_pod_sandbox(&pod_id) {
            log::error!("Failed to delete pod {} from database: {}", pod_id, e);
        } else {
            let _ = persistence.delete_runtime_artifacts("pod", &pod_id);
            log::info!("Pod sandbox {} removed from database", pod_id);
        }
        drop(persistence);

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
