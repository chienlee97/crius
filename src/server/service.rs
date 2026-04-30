use super::*;
use std::sync::{Arc as StdArc, Mutex as StdMutex};
use std::time::Instant;

#[derive(Debug, Default)]
pub(super) struct NameRegistry {
    ids_by_name: HashMap<String, String>,
    names_by_id: HashMap<String, String>,
}

impl NameRegistry {
    pub(super) fn reserve(&mut self, name: &str, id: &str) -> Result<(), String> {
        match self.ids_by_name.get(name) {
            Some(existing_id) if existing_id == id => {
                self.names_by_id.insert(id.to_string(), name.to_string());
                Ok(())
            }
            Some(existing_id) => Err(existing_id.clone()),
            None => {
                if let Some(previous_name) =
                    self.names_by_id.insert(id.to_string(), name.to_string())
                {
                    self.ids_by_name.remove(&previous_name);
                }
                self.ids_by_name.insert(name.to_string(), id.to_string());
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(super) fn get_id(&self, name: &str) -> Option<String> {
        self.ids_by_name.get(name).cloned()
    }

    fn release_by_id(&mut self, id: &str) {
        if let Some(name) = self.names_by_id.remove(id) {
            self.ids_by_name.remove(&name);
        }
    }
}

#[derive(Debug)]
pub(super) struct NameReservationGuard {
    id: String,
    registry: StdArc<StdMutex<NameRegistry>>,
    active: bool,
}

impl NameReservationGuard {
    fn new(id: impl Into<String>, registry: StdArc<StdMutex<NameRegistry>>) -> Self {
        Self {
            id: id.into(),
            registry,
            active: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for NameReservationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut registry) = self.registry.lock() {
            registry.release_by_id(&self.id);
        }
    }
}

/// 运行时服务实现
pub struct RuntimeServiceImpl {
    pub(super) containers: Arc<Mutex<HashMap<String, Container>>>,
    pub(super) pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    pub(super) container_names: StdArc<StdMutex<NameRegistry>>,
    pub(super) pod_names: StdArc<StdMutex<NameRegistry>>,
    pub(super) config: RuntimeConfig,
    pub(super) nri_config: NriConfig,
    pub(super) nri: Arc<dyn NriApi>,
    pub(super) runtime: RuntimeRegistry,
    pub(super) pod_manager: tokio::sync::Mutex<PodSandboxManager<RuntimeRegistry>>,
    pub(super) persistence: Arc<Mutex<PersistenceManager>>,
    pub(super) streaming: Arc<Mutex<Option<StreamingServer>>>,
    pub(super) events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
    pub(super) shim_work_dir: PathBuf,
    pub(super) attach_socket_dir: PathBuf,
    pub(super) container_exits_dir: PathBuf,
    pub(super) clean_shutdown_file: PathBuf,
    pub(super) last_startup_clean_shutdown: StdArc<StdMutex<Option<bool>>>,
    pub(super) version_file: PathBuf,
    pub(super) version_file_persist: PathBuf,
    pub(super) last_startup_detected_reboot: StdArc<StdMutex<Option<bool>>>,
    pub(super) last_startup_detected_upgrade: StdArc<StdMutex<Option<bool>>>,
    pub(super) last_startup_attempted_repair: StdArc<StdMutex<Option<bool>>>,
    pub(super) last_startup_repair_succeeded: StdArc<StdMutex<Option<bool>>>,
    pub(super) runtime_network_config: Arc<Mutex<Option<crate::proto::runtime::v1::NetworkConfig>>>,
    pub(super) exit_monitors: Arc<Mutex<HashSet<String>>>,
    pub(super) container_stats_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::ContainerStats>>>>,
    pub(super) pod_stats_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::PodSandboxStats>>>>,
    pub(super) pod_metrics_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::PodSandboxMetrics>>>>,
}

#[derive(Debug, Clone)]
pub(super) struct CachedStatsEntry<T> {
    pub(super) collected_at: Instant,
    pub(super) value: T,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ContainerCreateDeadline {
    pub(super) timeout_secs: u32,
    pub(super) deadline: Instant,
}

/// 运行时配置
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: PathBuf,
    pub runtime: String,
    pub runtime_handlers: Vec<String>,
    pub runtime_configs: HashMap<String, crate::config::ResolvedRuntimeHandlerConfig>,
    pub runtime_root: PathBuf,
    pub log_dir: PathBuf,
    pub runtime_path: PathBuf,
    pub image_root: PathBuf,
    pub image_driver: String,
    pub workloads: HashMap<String, crate::config::RuntimeWorkloadConfig>,
    pub enable_pod_events: bool,
    pub included_pod_metrics: Vec<String>,
    pub stats_collection_period: u64,
    pub pod_sandbox_metrics_collection_period: u64,
    pub grpc_max_send_msg_size: u32,
    pub grpc_max_recv_msg_size: u32,
    pub monitor_env: Vec<String>,
    pub default_env: Vec<(String, String)>,
    pub default_capabilities: Vec<String>,
    pub default_sysctls: HashMap<String, String>,
    pub attach_socket_dir: PathBuf,
    pub container_exits_dir: PathBuf,
    pub clean_shutdown_file: PathBuf,
    pub container_stop_timeout: u32,
    pub version_file: PathBuf,
    pub version_file_persist: PathBuf,
    pub criu_path: PathBuf,
    pub criu_image_path: PathBuf,
    pub criu_work_path: PathBuf,
    pub enable_criu_support: bool,
    pub internal_wipe: bool,
    pub internal_repair: bool,
    pub bind_mount_prefix: PathBuf,
    pub disable_cgroup: bool,
    pub tolerate_missing_hugetlb_controller: bool,
    pub separate_pull_cgroup: String,
    pub seccomp_profile: PathBuf,
    pub unset_seccomp_profile: String,
    pub uid_mappings: Option<Vec<crate::proto::runtime::v1::IdMapping>>,
    pub gid_mappings: Option<Vec<crate::proto::runtime::v1::IdMapping>>,
    pub minimum_mappable_uid: i64,
    pub minimum_mappable_gid: i64,
    pub io_uid: u32,
    pub io_gid: u32,
    pub pids_limit: i64,
    pub exec_cpu_affinity: String,
    pub read_only: bool,
    pub no_pivot: bool,
    pub pause_image: String,
    pub pause_command: String,
    pub cni_config: CniConfig,
    pub cgroup_driver: Option<CgroupDriver>,
    pub exec_sync_io_drain_timeout: std::time::Duration,
    pub max_container_log_line_size: usize,
    pub log_to_journald: bool,
    pub no_sync_log: bool,
    pub restrict_oom_score_adj: bool,
    pub enable_unprivileged_ports: bool,
    pub enable_unprivileged_icmp: bool,
    pub shim: ShimConfig,
    pub streaming: crate::streaming::StreamingConfig,
}

#[derive(Clone)]
pub struct RuntimeRegistry {
    default_handler: String,
    runtimes: Arc<HashMap<String, RuncRuntime>>,
    container_create_timeouts: Arc<HashMap<String, u32>>,
    container_handlers: Arc<std::sync::Mutex<HashMap<String, String>>>,
}

impl RuntimeRegistry {
    pub(super) fn new(
        default_handler: String,
        runtimes: HashMap<String, RuncRuntime>,
        container_create_timeouts: HashMap<String, u32>,
    ) -> Self {
        Self {
            default_handler,
            runtimes: Arc::new(runtimes),
            container_create_timeouts: Arc::new(container_create_timeouts),
            container_handlers: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn handler_from_annotations(&self, annotations: &[(String, String)]) -> Option<String> {
        annotations.iter().find_map(|(key, value)| {
            matches!(
                key.as_str(),
                CRIO_RUNTIME_HANDLER_ANNOTATION | CONTAINERD_RUNTIME_HANDLER_ANNOTATION
            )
            .then(|| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
        })
    }

    fn remember_container_handler(&self, container_id: &str, handler: &str) {
        if let Ok(mut handlers) = self.container_handlers.lock() {
            handlers.insert(container_id.to_string(), handler.to_string());
        }
    }

    fn forget_container_handler(&self, container_id: &str) {
        if let Ok(mut handlers) = self.container_handlers.lock() {
            handlers.remove(container_id);
        }
    }

    pub(super) fn runtime_for_handler(&self, handler: &str) -> anyhow::Result<RuncRuntime> {
        let resolved = if handler.trim().is_empty() {
            self.default_handler.as_str()
        } else {
            handler.trim()
        };
        self.runtimes
            .get(resolved)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unsupported runtime handler: {}", resolved))
    }

    pub(crate) fn container_create_timeout_for_handler(&self, handler: &str) -> u32 {
        let resolved = if handler.trim().is_empty() {
            self.default_handler.as_str()
        } else {
            handler.trim()
        };
        self.container_create_timeouts
            .get(resolved)
            .copied()
            .unwrap_or(240)
    }

    pub(super) fn runtime_for_annotations_map(
        &self,
        annotations: &HashMap<String, String>,
    ) -> anyhow::Result<RuncRuntime> {
        let handler = annotations
            .get(CRIO_RUNTIME_HANDLER_ANNOTATION)
            .or_else(|| annotations.get(CONTAINERD_RUNTIME_HANDLER_ANNOTATION))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(self.default_handler.as_str());
        self.runtime_for_handler(handler)
    }

    pub(super) fn runtime_for_container(&self, container_id: &str) -> anyhow::Result<RuncRuntime> {
        if let Ok(handlers) = self.container_handlers.lock() {
            if let Some(handler) = handlers.get(container_id) {
                return self.runtime_for_handler(handler);
            }
        }

        for (handler, runtime) in self.runtimes.iter() {
            if runtime.bundle_path_for(container_id).exists() {
                self.remember_container_handler(container_id, handler);
                return Ok(runtime.clone());
            }
        }

        self.runtime_for_handler(&self.default_handler)
    }

    pub(super) fn bundle_path_for_container(&self, container_id: &str) -> anyhow::Result<PathBuf> {
        Ok(self
            .runtime_for_container(container_id)?
            .bundle_path_for(container_id))
    }

    pub(super) fn all_runtimes(&self) -> Vec<RuncRuntime> {
        self.runtimes.values().cloned().collect()
    }

    pub(super) fn container_has_active_runtime_state(&self, container_id: &str) -> bool {
        self.runtimes.values().any(|runtime| {
            matches!(
                runtime.container_status(container_id),
                Ok(crate::runtime::ContainerStatus::Created
                    | crate::runtime::ContainerStatus::Running)
            ) || runtime.is_container_paused(container_id).unwrap_or(false)
        })
    }

    pub(super) fn is_container_paused(&self, container_id: &str) -> anyhow::Result<bool> {
        self.runtime_for_container(container_id)?
            .is_container_paused(container_id)
    }

    pub(super) fn restore_attach_shim(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .restore_attach_shim(container_id)
    }

    pub(super) fn restore_container_from_checkpoint(
        &self,
        container_id: &str,
        checkpoint_path: &Path,
        work_path: &Path,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .restore_container_from_checkpoint(container_id, checkpoint_path, work_path)
    }

    pub(super) fn enforce_oom_score_adj_policy(
        &self,
        container_id: &str,
        spec: &mut crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .enforce_oom_score_adj_policy(spec)
    }

    pub(super) fn prepare_rootfs(
        &self,
        container_id: &str,
        config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<()> {
        let handler = self
            .handler_from_annotations(&config.annotations)
            .unwrap_or_else(|| self.default_handler.clone());
        self.remember_container_handler(container_id, &handler);
        self.runtime_for_handler(&handler)?
            .prepare_rootfs(container_id, config)
    }

    pub(super) fn build_spec(
        &self,
        container_id: &str,
        config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<crate::oci::spec::Spec> {
        let handler = self
            .handler_from_annotations(&config.annotations)
            .unwrap_or_else(|| self.default_handler.clone());
        self.remember_container_handler(container_id, &handler);
        self.runtime_for_handler(&handler)?
            .build_spec(container_id, config)
    }

    pub(super) fn write_bundle(
        &self,
        container_id: &str,
        rootfs: &Path,
        spec: &crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .write_bundle(container_id, rootfs, spec)
    }

    pub(super) fn pause_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .pause_container(container_id)
    }

    pub(super) fn checkpoint_container(
        &self,
        container_id: &str,
        location: &Path,
        work_path: &Path,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .checkpoint_container(container_id, location, work_path)
    }

    pub(super) fn resume_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .resume_container(container_id)
    }
}

impl crate::runtime::ContainerRuntime for RuntimeRegistry {
    fn create_container(
        &self,
        container_id: &str,
        config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<String> {
        let handler = self
            .handler_from_annotations(&config.annotations)
            .unwrap_or_else(|| self.default_handler.clone());
        let runtime = self.runtime_for_handler(&handler)?;
        let created = runtime.create_container(container_id, config)?;
        self.remember_container_handler(container_id, &handler);
        Ok(created)
    }

    fn start_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .start_container(container_id)
    }

    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .stop_container(container_id, timeout)
    }

    fn remove_container(&self, container_id: &str) -> anyhow::Result<()> {
        let result = self
            .runtime_for_container(container_id)?
            .remove_container(container_id);
        self.forget_container_handler(container_id);
        result
    }

    fn container_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<crate::runtime::ContainerStatus> {
        self.runtime_for_container(container_id)?
            .container_status(container_id)
    }

    fn reopen_container_log(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .reopen_container_log(container_id)
    }

    fn exec_in_container(
        &self,
        container_id: &str,
        command: &[String],
        tty: bool,
    ) -> anyhow::Result<i32> {
        self.runtime_for_container(container_id)?
            .exec_in_container(container_id, command, tty)
    }

    fn update_container_resources(
        &self,
        container_id: &str,
        resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .update_container_resources(container_id, resources)
    }
}

impl RuntimeServiceImpl {
    pub(super) fn container_create_deadline_for_handler(
        &self,
        runtime_handler: &str,
    ) -> ContainerCreateDeadline {
        let timeout_secs = self
            .runtime
            .container_create_timeout_for_handler(runtime_handler);
        ContainerCreateDeadline {
            timeout_secs,
            deadline: Instant::now() + std::time::Duration::from_secs(timeout_secs as u64),
        }
    }

    pub(super) async fn run_container_create_phase_until<T, F>(
        &self,
        deadline: ContainerCreateDeadline,
        phase: &str,
        future: F,
    ) -> Result<T, Status>
    where
        F: std::future::Future<Output = Result<T, Status>>,
    {
        let remaining = deadline
            .deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Err(Status::deadline_exceeded(format!(
                "container create phase {phase} exceeded runtime handler create timeout of {}s",
                deadline.timeout_secs
            )));
        }
        tokio::time::timeout(remaining, future)
            .await
            .map_err(|_| {
                Status::deadline_exceeded(format!(
                    "container create phase {phase} exceeded runtime handler create timeout of {}s",
                    deadline.timeout_secs
                ))
            })?
    }
}

impl RuntimeServiceImpl {
    pub(super) fn effective_container_stop_timeout(&self, requested_timeout_secs: u32) -> u32 {
        if requested_timeout_secs == 0 {
            self.config.container_stop_timeout
        } else {
            requested_timeout_secs.max(self.config.container_stop_timeout)
        }
    }

    pub(super) fn effective_readonly_rootfs(&self, requested: bool) -> bool {
        self.config.read_only || requested
    }

    pub(super) fn effective_pids_limit(
        &self,
        requested: Option<i64>,
    ) -> Result<Option<i64>, Status> {
        match requested {
            Some(limit) if limit > 0 => Ok(Some(limit)),
            Some(0) | None => Ok((self.config.pids_limit > 0).then_some(self.config.pids_limit)),
            Some(-1) => Ok(None),
            Some(limit) => Err(Status::invalid_argument(format!(
                "pids_limit must be -1, 0, or greater than zero, got {}",
                limit
            ))),
        }
    }

    pub(super) fn cgroup_updates_disabled_status(&self) -> Status {
        Status::failed_precondition(
            "cgroup support is disabled by runtime.disable_cgroup; resource updates are unavailable",
        )
    }

    pub(super) fn criu_support_disabled_status(&self, operation: &str) -> Status {
        Status::failed_precondition(format!(
            "CRIU support is disabled by runtime.enable_criu_support; {} is unavailable",
            operation
        ))
    }

    pub(super) async fn effective_exec_cpu_affinity(&self, container_id: &str) -> Option<usize> {
        if self.config.exec_cpu_affinity != "first" {
            return None;
        }
        self.container_internal_state(container_id)
            .await
            .and_then(|state| state.linux_resources)
            .and_then(|resources| {
                crate::runtime::RuncRuntime::first_cpu_from_cpuset(&resources.cpuset_cpus)
            })
    }

    pub(super) fn effective_userns_options(
        &self,
        requested: Option<&NamespaceOption>,
    ) -> Option<NamespaceOption> {
        let (Some(uid_mappings), Some(gid_mappings)) = (
            self.config.uid_mappings.as_ref(),
            self.config.gid_mappings.as_ref(),
        ) else {
            return requested.cloned();
        };

        if let Some(options) = requested {
            if let Some(userns) = options.userns_options.as_ref() {
                if userns.mode == NamespaceMode::Node as i32 {
                    return Some(options.clone());
                }
                if !userns.uids.is_empty() || !userns.gids.is_empty() {
                    return Some(options.clone());
                }
            }

            let mut effective = options.clone();
            effective.userns_options = Some(crate::proto::runtime::v1::UserNamespace {
                mode: NamespaceMode::Pod as i32,
                uids: uid_mappings.clone(),
                gids: gid_mappings.clone(),
            });
            return Some(effective);
        }

        Some(NamespaceOption {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Pod as i32,
            target_id: String::new(),
            userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                mode: NamespaceMode::Pod as i32,
                uids: uid_mappings.clone(),
                gids: gid_mappings.clone(),
            }),
        })
    }

    fn run_as_user_is_non_root(run_as_user: Option<&str>) -> bool {
        let Some(run_as_user) = run_as_user.map(str::trim).filter(|value| !value.is_empty()) else {
            return false;
        };

        run_as_user
            .parse::<u64>()
            .map(|value| value != 0)
            .unwrap_or(true)
    }

    fn run_as_group_or_supplemental_is_non_root(
        run_as_group: Option<u32>,
        supplemental_groups: &[u32],
    ) -> bool {
        run_as_group.is_some_and(|group| group != 0)
            || supplemental_groups.iter().any(|group| *group != 0)
    }

    pub(super) fn validate_minimum_mappable_ids(
        &self,
        namespace_options: Option<&NamespaceOption>,
        run_as_user: Option<&str>,
        run_as_group: Option<u32>,
        supplemental_groups: &[u32],
    ) -> Result<(), Status> {
        let Some(userns) = namespace_options.and_then(|options| options.userns_options.as_ref())
        else {
            return Ok(());
        };
        if userns.mode == NamespaceMode::Node as i32 {
            return Ok(());
        }

        let non_root_user = Self::run_as_user_is_non_root(run_as_user);
        let non_root_group =
            Self::run_as_group_or_supplemental_is_non_root(run_as_group, supplemental_groups);

        if self.config.minimum_mappable_uid >= 0 && non_root_user {
            for mapping in &userns.uids {
                if i64::from(mapping.host_id) < self.config.minimum_mappable_uid {
                    return Err(Status::invalid_argument(format!(
                        "uid mapping {}:{}:{} is below minimum mappable uid {} for non-root user namespace",
                        mapping.container_id,
                        mapping.host_id,
                        mapping.length,
                        self.config.minimum_mappable_uid
                    )));
                }
            }
        }

        if self.config.minimum_mappable_gid >= 0 && (non_root_user || non_root_group) {
            for mapping in &userns.gids {
                if i64::from(mapping.host_id) < self.config.minimum_mappable_gid {
                    return Err(Status::invalid_argument(format!(
                        "gid mapping {}:{}:{} is below minimum mappable gid {} for non-root user namespace",
                        mapping.container_id,
                        mapping.host_id,
                        mapping.length,
                        self.config.minimum_mappable_gid
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn clean_shutdown_file(&self) -> &Path {
        &self.clean_shutdown_file
    }

    pub fn version_file(&self) -> &Path {
        &self.version_file
    }

    pub fn version_file_persist(&self) -> &Path {
        &self.version_file_persist
    }

    pub fn record_startup_clean_shutdown(&self, clean: bool) {
        if let Ok(mut state) = self.last_startup_clean_shutdown.lock() {
            *state = Some(clean);
        }
    }

    pub fn last_startup_clean_shutdown(&self) -> Option<bool> {
        self.last_startup_clean_shutdown
            .lock()
            .ok()
            .and_then(|state| *state)
    }

    pub fn record_startup_detected_reboot(&self, detected: bool) {
        if let Ok(mut state) = self.last_startup_detected_reboot.lock() {
            *state = Some(detected);
        }
    }

    pub fn last_startup_detected_reboot(&self) -> Option<bool> {
        self.last_startup_detected_reboot
            .lock()
            .ok()
            .and_then(|state| *state)
    }

    pub fn record_startup_detected_upgrade(&self, detected: bool) {
        if let Ok(mut state) = self.last_startup_detected_upgrade.lock() {
            *state = Some(detected);
        }
    }

    pub fn last_startup_detected_upgrade(&self) -> Option<bool> {
        self.last_startup_detected_upgrade
            .lock()
            .ok()
            .and_then(|state| *state)
    }

    pub fn record_startup_attempted_repair(&self, attempted: bool, succeeded: Option<bool>) {
        if let Ok(mut state) = self.last_startup_attempted_repair.lock() {
            *state = Some(attempted);
        }
        if let Ok(mut state) = self.last_startup_repair_succeeded.lock() {
            *state = succeeded;
        }
    }

    pub fn last_startup_attempted_repair(&self) -> Option<bool> {
        self.last_startup_attempted_repair
            .lock()
            .ok()
            .and_then(|state| *state)
    }

    pub fn last_startup_repair_succeeded(&self) -> Option<bool> {
        self.last_startup_repair_succeeded
            .lock()
            .ok()
            .and_then(|state| *state)
    }

    pub async fn maybe_repair_persistence_after_unclean_shutdown(&self) -> Result<(), Status> {
        if self.last_startup_clean_shutdown().unwrap_or(false) || !self.config.internal_repair {
            self.record_startup_attempted_repair(false, None);
            return Ok(());
        }

        let mut persistence = self.persistence.lock().await;
        match persistence.check_integrity() {
            Ok(true) => {
                self.record_startup_attempted_repair(false, Some(true));
                Ok(())
            }
            Ok(false) => {
                let repaired = persistence.attempt_repair().map_err(|e| {
                    Status::internal(format!("Failed to repair persistence database: {}", e))
                })?;
                self.record_startup_attempted_repair(true, Some(repaired));
                Ok(())
            }
            Err(err) => {
                self.record_startup_attempted_repair(true, Some(false));
                Err(Status::internal(format!(
                    "Failed to check persistence database integrity: {}",
                    err
                )))
            }
        }
    }

    pub(super) fn clamp_proto_oom_score_adj(
        &self,
        resources: &mut crate::proto::runtime::v1::LinuxContainerResources,
    ) -> Result<(), Status> {
        if !self.config.restrict_oom_score_adj || resources.oom_score_adj == 0 {
            return Ok(());
        }
        resources.oom_score_adj =
            crate::runtime::RuncRuntime::restrict_oom_score_adj_floor(resources.oom_score_adj)
                .map_err(|e| {
                    Status::internal(format!("Failed to enforce oom_score_adj policy: {}", e))
                })?;
        Ok(())
    }

    pub(super) fn clamp_stored_oom_score_adj(
        &self,
        resources: &mut StoredLinuxResources,
    ) -> Result<(), Status> {
        if !self.config.restrict_oom_score_adj || resources.oom_score_adj == 0 {
            return Ok(());
        }
        resources.oom_score_adj =
            crate::runtime::RuncRuntime::restrict_oom_score_adj_floor(resources.oom_score_adj)
                .map_err(|e| {
                    Status::internal(format!("Failed to enforce oom_score_adj policy: {}", e))
                })?;
        Ok(())
    }

    pub(super) fn pod_name_key(metadata: &PodSandboxMetadata) -> String {
        format!(
            "{}:{}:{}:{}",
            metadata.name, metadata.namespace, metadata.uid, metadata.attempt
        )
    }

    pub(super) fn container_name_key(
        metadata: &ContainerMetadata,
        pod_metadata: &PodSandboxMetadata,
    ) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            metadata.name,
            pod_metadata.name,
            pod_metadata.namespace,
            pod_metadata.uid,
            metadata.attempt
        )
    }

    pub(super) fn validate_container_image_spec(
        config: &crate::proto::runtime::v1::ContainerConfig,
    ) -> Result<&ImageSpec, Status> {
        let image = config.image.as_ref().ok_or_else(|| {
            Status::invalid_argument("CreateContainerRequest.ContainerConfig.Image is nil")
        })?;
        if image.image.trim().is_empty() {
            return Err(Status::invalid_argument(
                "CreateContainerRequest.ContainerConfig.Image.Image is empty",
            ));
        }
        Ok(image)
    }

    pub(super) fn reserve_pod_name(
        &self,
        pod_id: &str,
        name: &str,
    ) -> Result<NameReservationGuard, Status> {
        let mut registry = self
            .pod_names
            .lock()
            .map_err(|_| Status::internal("pod name registry lock poisoned"))?;
        if let Err(existing_id) = registry.reserve(name, pod_id) {
            return Err(Status::already_exists(format!(
                "pod sandbox with name {name:?} already exists as {existing_id}"
            )));
        }
        drop(registry);
        Ok(NameReservationGuard::new(pod_id, self.pod_names.clone()))
    }

    pub(super) fn reserve_container_name(
        &self,
        container_id: &str,
        name: &str,
    ) -> Result<NameReservationGuard, Status> {
        let mut registry = self
            .container_names
            .lock()
            .map_err(|_| Status::internal("container name registry lock poisoned"))?;
        if let Err(existing_id) = registry.reserve(name, container_id) {
            return Err(Status::already_exists(format!(
                "container with name {name:?} already exists as {existing_id}"
            )));
        }
        drop(registry);
        Ok(NameReservationGuard::new(
            container_id,
            self.container_names.clone(),
        ))
    }

    pub(super) fn release_pod_name(&self, pod_id: &str) {
        if let Ok(mut registry) = self.pod_names.lock() {
            registry.release_by_id(pod_id);
        }
    }

    pub(super) fn release_container_name(&self, container_id: &str) {
        if let Ok(mut registry) = self.container_names.lock() {
            registry.release_by_id(container_id);
        }
    }

    #[cfg(test)]
    pub(super) fn pod_id_for_reserved_name(&self, name: &str) -> Option<String> {
        self.pod_names
            .lock()
            .ok()
            .and_then(|registry| registry.get_id(name))
    }

    #[cfg(test)]
    pub(super) fn container_id_for_reserved_name(&self, name: &str) -> Option<String> {
        self.container_names
            .lock()
            .ok()
            .and_then(|registry| registry.get_id(name))
    }

    pub fn new(config: RuntimeConfig) -> Self {
        Self::new_with_nri_config(config, NriConfig::default())
    }

    pub fn new_with_nri_config(config: RuntimeConfig, nri_config: NriConfig) -> Self {
        Self::new_with_shim_work_dir(config, nri_config, default_shim_work_dir())
    }

    pub fn new_with_nri_api(
        config: RuntimeConfig,
        nri_config: NriConfig,
        nri: Arc<dyn NriApi>,
    ) -> Self {
        Self::new_with_shim_work_dir_and_nri(config, nri_config, default_shim_work_dir(), Some(nri))
    }

    pub(super) fn new_with_shim_work_dir(
        config: RuntimeConfig,
        nri_config: NriConfig,
        shim_work_dir: PathBuf,
    ) -> Self {
        Self::new_with_shim_work_dir_and_nri(config, nri_config, shim_work_dir, None)
    }

    pub(super) fn new_with_shim_work_dir_and_nri(
        config: RuntimeConfig,
        nri_config: NriConfig,
        shim_work_dir: PathBuf,
        injected_nri: Option<Arc<dyn NriApi>>,
    ) -> Self {
        let nri_manager_config = NriManagerConfig::from(nri_config.clone());
        let containers = Arc::new(Mutex::new(HashMap::new()));
        let pod_sandboxes = Arc::new(Mutex::new(HashMap::new()));
        let container_names = StdArc::new(StdMutex::new(NameRegistry::default()));
        let pod_names = StdArc::new(StdMutex::new(NameRegistry::default()));
        let mut config = config;
        let mut handlers = Vec::new();
        for handler in &config.runtime_handlers {
            let trimmed = handler.trim();
            if !trimmed.is_empty() && !handlers.iter().any(|existing: &String| existing == trimmed)
            {
                handlers.push(trimmed.to_string());
            }
        }
        if !handlers.iter().any(|handler| handler == &config.runtime) {
            handlers.push(config.runtime.clone());
        }
        config.runtime_handlers = handlers;
        let mut runtime_configs = config.runtime_configs.clone();
        runtime_configs.insert(
            config.runtime.clone(),
            crate::config::ResolvedRuntimeHandlerConfig {
                runtime_path: config.runtime_path.display().to_string(),
                runtime_root: config.runtime_root.display().to_string(),
                monitor_path: config.shim.shim_path.display().to_string(),
                monitor_env: config.monitor_env.clone(),
                allowed_annotations: Vec::new(),
                default_annotations: HashMap::new(),
                container_create_timeout: 240,
            },
        );
        config.runtime_configs = runtime_configs;
        let container_create_timeouts = config
            .runtime_configs
            .iter()
            .map(|(handler, config)| (handler.clone(), config.container_create_timeout))
            .collect();

        let resolved_shim_work_dir = shim_work_dir;
        let attach_socket_dir = config.attach_socket_dir.clone();
        let container_exits_dir = config.container_exits_dir.clone();
        let clean_shutdown_file = config.clean_shutdown_file.clone();
        let version_file = config.version_file.clone();
        let version_file_persist = config.version_file_persist.clone();
        let runtimes = config
            .runtime_configs
            .iter()
            .map(|(handler, runtime_config)| {
                let mut shim_config = config.shim.clone();
                shim_config.work_dir = resolved_shim_work_dir.clone();
                shim_config.attach_socket_dir = config.attach_socket_dir.clone();
                shim_config.container_exits_dir = config.container_exits_dir.clone();
                shim_config.shim_path = PathBuf::from(&runtime_config.monitor_path);
                shim_config.io_uid = config.io_uid;
                shim_config.io_gid = config.io_gid;
                shim_config.runtime_path = PathBuf::from(&runtime_config.runtime_path);
                shim_config.monitor_env = runtime_config.monitor_env.clone();
                shim_config.no_sync_log = config.no_sync_log;
                (handler.clone(), {
                    let mut runtime = RuncRuntime::with_shim_and_image_storage(
                        PathBuf::from(&runtime_config.runtime_path),
                        PathBuf::from(&runtime_config.runtime_root),
                        config.root_dir.join("storage"),
                        shim_config,
                    );
                    runtime.set_default_env(config.default_env.clone());
                    runtime.set_default_capabilities(config.default_capabilities.clone());
                    runtime.set_default_sysctls(config.default_sysctls.clone());
                    runtime
                        .set_container_create_timeout_secs(runtime_config.container_create_timeout);
                    runtime.set_container_stop_timeout_secs(config.container_stop_timeout);
                    runtime.set_criu_path(config.criu_path.clone());
                    runtime.set_restrict_oom_score_adj(config.restrict_oom_score_adj);
                    runtime.set_bind_mount_prefix(config.bind_mount_prefix.clone());
                    runtime.set_disable_cgroup(config.disable_cgroup);
                    runtime.set_default_seccomp_profile_path(config.seccomp_profile.clone());
                    runtime.set_exec_cpu_affinity(config.exec_cpu_affinity.clone());
                    runtime.set_no_pivot(config.no_pivot);
                    runtime
                })
            })
            .collect();
        let runtime = RuntimeRegistry::new(
            config.runtime.clone(),
            runtimes,
            container_create_timeouts,
        );

        let pod_manager = PodSandboxManager::new(
            runtime.clone(),
            config.root_dir.join("pods"),
            config.pause_image.clone(),
            config.pause_command.clone(),
            config.cni_config.clone(),
        );
        let persistence_config = PersistenceConfig {
            db_path: config.root_dir.join("crius.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        };
        let persistence = PersistenceManager::new(persistence_config)
            .expect("Failed to create persistence manager");
        let persistence = Arc::new(Mutex::new(persistence));
        let (events, _) = tokio::sync::broadcast::channel(256);
        let nri: Arc<dyn NriApi> = injected_nri.unwrap_or_else(|| {
            if nri_manager_config.enable {
                Arc::new(NriManager::with_domain(
                    nri_manager_config,
                    Arc::new(NriRuntimeDomain {
                        containers: containers.clone(),
                        pod_sandboxes: pod_sandboxes.clone(),
                        config: config.clone(),
                        nri_config: nri_config.clone(),
                        runtime: runtime.clone(),
                        persistence: persistence.clone(),
                        events: events.clone(),
                    }),
                ))
            } else {
                Arc::new(NopNri)
            }
        });
        let runtime_network_config = Self::load_runtime_network_config(&config.root_dir)
            .unwrap_or_else(|e| {
                log::warn!(
                    "Failed to load runtime network config from {}: {}",
                    config.root_dir.display(),
                    e
                );
                None
            });

        Self {
            containers,
            pod_sandboxes,
            container_names,
            pod_names,
            config,
            nri_config,
            nri,
            runtime,
            pod_manager: tokio::sync::Mutex::new(pod_manager),
            persistence,
            streaming: Arc::new(Mutex::new(None)),
            events,
            shim_work_dir: resolved_shim_work_dir,
            attach_socket_dir,
            container_exits_dir,
            clean_shutdown_file,
            last_startup_clean_shutdown: StdArc::new(StdMutex::new(None)),
            version_file,
            version_file_persist,
            last_startup_detected_reboot: StdArc::new(StdMutex::new(None)),
            last_startup_detected_upgrade: StdArc::new(StdMutex::new(None)),
            last_startup_attempted_repair: StdArc::new(StdMutex::new(None)),
            last_startup_repair_succeeded: StdArc::new(StdMutex::new(None)),
            runtime_network_config: Arc::new(Mutex::new(runtime_network_config)),
            exit_monitors: Arc::new(Mutex::new(HashSet::new())),
            container_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            pod_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            pod_metrics_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn initialize_nri(&self) -> Result<(), Status> {
        if !self.nri_config.enable {
            return Ok(());
        }
        log::info!("Initializing NRI");
        self.nri
            .start()
            .await
            .map_err(|e| Status::internal(format!("Failed to start NRI: {}", e)))?;
        log::info!("NRI started");
        self.nri
            .synchronize()
            .await
            .map_err(|e| Status::internal(format!("Failed to synchronize NRI: {}", e)))?;
        log::info!("NRI synchronized");
        Ok(())
    }

    pub fn nri_handle(&self) -> Arc<dyn NriApi> {
        self.nri.clone()
    }

    pub async fn set_streaming_server(&self, streaming_server: StreamingServer) {
        let mut streaming = self.streaming.lock().await;
        *streaming = Some(streaming_server);
    }
}
