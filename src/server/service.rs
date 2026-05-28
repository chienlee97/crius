use super::*;
use crate::config::CgroupDriverConfig;
use crate::image::{ImageServiceImpl, ImageServiceOptions, ReloadableImageConfig};
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
    pub(super) removed_container_ids: StdArc<StdMutex<HashSet<String>>>,
    pub(super) removed_pod_sandbox_ids: StdArc<StdMutex<HashSet<String>>>,
    pub(super) config: RuntimeConfig,
    pub(super) nri_config: NriConfig,
    pub(super) nri: Arc<dyn NriApi>,
    pub(super) runtime: RuntimeRegistry,
    pub(super) pod_manager: Arc<tokio::sync::Mutex<PodSandboxManager<RuntimeRegistry>>>,
    pub(super) image_service: ImageServiceImpl,
    pub(super) persistence: Arc<Mutex<PersistenceManager>>,
    pub(super) streaming: Arc<Mutex<Option<StreamingServer>>>,
    pub(super) events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
    pub(super) internal_services: crate::services::InternalServices,
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
    pub(super) last_recovery_result: StdArc<StdMutex<Option<RecoveryResultSummary>>>,
    pub(super) last_irqbalance_restore_status: StdArc<StdMutex<Option<IrqBalanceRestoreStatus>>>,
    pub(super) seccomp_notifier_dir: PathBuf,
    pub(super) seccomp_notifiers:
        StdArc<StdMutex<HashMap<String, seccomp_notifier::SeccompNotifier>>>,
    pub(super) seccomp_notifier_snapshots:
        StdArc<StdMutex<HashMap<String, seccomp_notifier::SeccompNotifierSnapshot>>>,
    pub(super) seccomp_notification_tx:
        tokio::sync::mpsc::UnboundedSender<seccomp_notifier::SeccompNotificationEvent>,
    pub(super) runtime_network_config: Arc<Mutex<Option<crate::proto::runtime::v1::NetworkConfig>>>,
    pub(super) reloadable_config: StdArc<StdMutex<RuntimeReloadableConfig>>,
    pub(super) reload_state: StdArc<StdMutex<RuntimeReloadState>>,
    pub(super) reload_watcher_shutdown: tokio::sync::broadcast::Sender<()>,
    pub(super) exit_monitors: Arc<Mutex<HashSet<String>>>,
    pub(super) container_stats_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::ContainerStats>>>>,
    pub(super) pod_stats_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::PodSandboxStats>>>>,
    pub(super) pod_metrics_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::PodSandboxMetrics>>>>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryStageSummary {
    pub name: String,
    pub success: bool,
    pub duration_millis: u64,
    pub items: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryStage {
    LoadLedgerSnapshot,
    CheckLedger,
    RepairLedger,
    RestoreMemoryState,
    ReserveRecoveredNames,
    PruneLogicalDuplicates,
    RestoreSeccompNotifiers,
    ProbeRuntimeLiveState,
    ReconnectShims,
    ReconcileObjects,
    RestoreExitMonitors,
    CleanupOrphans,
}

impl RecoveryStage {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::LoadLedgerSnapshot => "loadLedgerSnapshot",
            Self::CheckLedger => "checkLedger",
            Self::RepairLedger => "repairLedger",
            Self::RestoreMemoryState => "restoreMemoryState",
            Self::ReserveRecoveredNames => "reserveRecoveredNames",
            Self::PruneLogicalDuplicates => "pruneLogicalDuplicates",
            Self::RestoreSeccompNotifiers => "restoreSeccompNotifiers",
            Self::ProbeRuntimeLiveState => "probeRuntimeLiveState",
            Self::ReconnectShims => "reconnectShims",
            Self::ReconcileObjects => "reconcileObjects",
            Self::RestoreExitMonitors => "restoreExitMonitors",
            Self::CleanupOrphans => "cleanupOrphans",
        }
    }
}

#[cfg(test)]
pub(super) const RECOVERY_STAGE_ORDER: &[RecoveryStage] = &[
    RecoveryStage::LoadLedgerSnapshot,
    RecoveryStage::CheckLedger,
    RecoveryStage::RepairLedger,
    RecoveryStage::RestoreMemoryState,
    RecoveryStage::ReserveRecoveredNames,
    RecoveryStage::PruneLogicalDuplicates,
    RecoveryStage::RestoreSeccompNotifiers,
    RecoveryStage::ProbeRuntimeLiveState,
    RecoveryStage::ReconnectShims,
    RecoveryStage::ReconcileObjects,
    RecoveryStage::RestoreExitMonitors,
    RecoveryStage::CleanupOrphans,
];

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryReconcileSummary {
    pub reconnected_shims: Vec<String>,
    pub broken_containers: usize,
    pub broken_pods: usize,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryShimCleanupSummary {
    pub shim_dirs_removed: usize,
    pub attach_socket_dirs_removed: usize,
    pub failures: usize,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryCleanupCounter {
    pub removed: usize,
    pub failures: usize,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryOrphanCleanupSummary {
    pub skipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    pub runtime_bundles_removed: usize,
    pub pod_workspaces_removed: usize,
    pub shim_dirs_removed: usize,
    pub attach_socket_dirs_removed: usize,
    pub pause_processes_killed: usize,
    pub failures: usize,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryResultSummary {
    pub finished_at_unix_millis: i64,
    pub success: bool,
    pub total_duration_millis: u64,
    pub stages: Vec<RecoveryStageSummary>,
    pub reconcile: RecoveryReconcileSummary,
    pub orphan_cleanup: RecoveryOrphanCleanupSummary,
}

#[derive(Clone)]
pub struct RuntimeMetricsProvider {
    containers: Arc<Mutex<HashMap<String, Container>>>,
    pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    config: RuntimeConfig,
    events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
    container_stats_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::ContainerStats>>>>,
    pod_stats_cache:
        Arc<Mutex<HashMap<String, CachedStatsEntry<crate::proto::runtime::v1::PodSandboxStats>>>>,
    pod_metrics_cache:
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

#[derive(Debug, Clone, Default)]
pub struct IrqBalanceRestoreStatus {
    pub attempted: bool,
    pub restored: bool,
    pub message: String,
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
    pub runtime_config_path: PathBuf,
    pub image_root: PathBuf,
    pub image_driver: String,
    pub image_global_auth_file: PathBuf,
    pub image_namespaced_auth_dir: PathBuf,
    pub image_default_transport: String,
    pub image_short_name_mode: String,
    pub image_pull_progress_timeout: std::time::Duration,
    pub image_max_concurrent_downloads: usize,
    pub image_pull_retry_count: u32,
    pub image_registry_config_dir: PathBuf,
    pub image_decryption_keys_path: PathBuf,
    pub image_decryption_decoder_path: String,
    pub image_decryption_keyprovider_config: PathBuf,
    pub image_additional_artifact_stores: Vec<PathBuf>,
    pub image_signature_policy: PathBuf,
    pub image_signature_policy_dir: PathBuf,
    pub image_storage_options: Vec<String>,
    pub image_external_snapshotters: HashMap<String, crate::config::ExternalSnapshotterConfig>,
    pub image_volumes: String,
    pub image_pinned_images: Vec<String>,
    pub image_big_files_temporary_dir: PathBuf,
    pub image_oci_artifact_mount_support: bool,
    pub workloads: HashMap<String, crate::config::RuntimeWorkloadConfig>,
    pub enable_pod_events: bool,
    pub included_pod_metrics: Vec<String>,
    pub stats_collection_period: u64,
    pub pod_sandbox_metrics_collection_period: u64,
    pub grpc_max_send_msg_size: u32,
    pub grpc_max_recv_msg_size: u32,
    pub metrics_enable: bool,
    pub metrics_host: String,
    pub metrics_port: u16,
    pub metrics_socket_path: PathBuf,
    pub metrics_enable_tls: bool,
    pub metrics_tls_cert_file: PathBuf,
    pub metrics_tls_key_file: PathBuf,
    pub metrics_tls_ca_file: PathBuf,
    pub metrics_tls_min_version: String,
    pub metrics_tls_cipher_suites: Vec<String>,
    pub metrics_collectors: Vec<String>,
    pub tracing_enable: bool,
    pub tracing_endpoint: String,
    pub tracing_sampling_rate_per_million: u32,
    pub monitor_env: Vec<String>,
    pub monitor_cgroup: String,
    pub default_env: Vec<(String, String)>,
    pub default_capabilities: Vec<String>,
    pub default_sysctls: HashMap<String, String>,
    pub default_ulimits: Vec<crate::oci::spec::Rlimit>,
    pub allowed_devices: Vec<PathBuf>,
    pub additional_devices: Vec<crate::runtime::DeviceMapping>,
    pub device_ownership_from_security_context: bool,
    pub add_inheritable_capabilities: bool,
    pub base_runtime_spec: Option<crate::oci::spec::Spec>,
    pub default_mounts_file: PathBuf,
    pub hooks_dir: Vec<PathBuf>,
    pub absent_mount_sources_to_reject: Vec<PathBuf>,
    pub disable_proc_mount: bool,
    pub timezone: String,
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
    pub privileged_seccomp_profile: String,
    pub unset_seccomp_profile: String,
    pub apparmor_default_profile: String,
    pub disable_apparmor: bool,
    pub enable_selinux: bool,
    pub selinux_category_range: u32,
    pub hostnetwork_disable_selinux: bool,
    pub uid_mappings: Option<Vec<crate::proto::runtime::v1::IdMapping>>,
    pub gid_mappings: Option<Vec<crate::proto::runtime::v1::IdMapping>>,
    pub minimum_mappable_uid: i64,
    pub minimum_mappable_gid: i64,
    pub io_uid: u32,
    pub io_gid: u32,
    pub pids_limit: i64,
    pub infra_ctr_cpuset: String,
    pub shared_cpuset: String,
    pub exec_cpu_affinity: String,
    pub irqbalance_config_file: PathBuf,
    pub irqbalance_config_restore_file: String,
    pub read_only: bool,
    pub no_pivot: bool,
    pub no_new_keyring: bool,
    pub pause_image: String,
    pub pause_command: String,
    pub drop_infra_ctr: bool,
    pub cni_config: CniConfig,
    pub cgroup_driver: Option<CgroupDriver>,
    pub exec_sync_io_drain_timeout: std::time::Duration,
    pub max_container_log_line_size: usize,
    pub log_to_journald: bool,
    pub no_sync_log: bool,
    pub restrict_oom_score_adj: bool,
    pub enable_unprivileged_ports: bool,
    pub enable_unprivileged_icmp: bool,
    pub rootless: crate::rootless::EffectiveRootlessConfig,
    pub shim: ShimConfig,
    pub streaming: crate::streaming::StreamingConfig,
    pub config_path: Option<PathBuf>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let loaded = crate::config::Config::default();
        let runtime_name = loaded.runtime.runtime_type.clone();
        let runtime_configs = loaded.runtime.resolved_runtimes().unwrap_or_else(|_| {
            HashMap::from([(
                runtime_name.clone(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    backend_options: HashMap::new(),
                    runtime_path: loaded.runtime.runtime_path.clone(),
                    runtime_config_path: loaded.runtime.runtime_config_path.clone(),
                    runtime_root: loaded.runtime.root.clone(),
                    platform_runtime_paths: loaded.runtime.platform_runtime_paths.clone(),
                    monitor_path: loaded.runtime.shim_path.clone(),
                    monitor_cgroup: loaded.runtime.monitor_cgroup.clone(),
                    monitor_env: loaded.runtime.monitor_env.clone(),
                    stream_websockets: false,
                    allowed_annotations: Vec::new(),
                    default_annotations: HashMap::new(),
                    privileged_without_host_devices: false,
                    privileged_without_host_devices_all_devices_allowed: false,
                    container_create_timeout: 240,
                    snapshotter: "internal-overlay-untar".to_string(),
                },
            )])
        });
        let mut cni_config = loaded.network.cni_config();
        if loaded.network.netns_mounts_under_state_dir {
            cni_config.set_netns_mount_dir(PathBuf::from(&loaded.runtime.root).join("netns"));
        }
        if !loaded.runtime.pinns_path.trim().is_empty() {
            cni_config.set_namespace_helper_path(Some(PathBuf::from(&loaded.runtime.pinns_path)));
        }
        for (handler, handler_config) in &loaded.runtime.runtimes {
            let cni_conf_dir = handler_config.cni_conf_dir.trim();
            if !cni_conf_dir.is_empty() {
                cni_config
                    .set_handler_config_dirs(handler.clone(), vec![PathBuf::from(cni_conf_dir)]);
            }
            if let Some(cni_max_conf_num) = handler_config.cni_max_conf_num {
                cni_config.set_handler_max_conf_num(handler.clone(), cni_max_conf_num);
            }
        }
        let root_dir = PathBuf::from(&loaded.root);
        let runtime_root = PathBuf::from(&loaded.runtime.root);
        let log_dir = PathBuf::from(&loaded.logging.dir);
        let runtime_path = PathBuf::from(&loaded.runtime.runtime_path);
        let runtime_config_path = PathBuf::from(&loaded.runtime.runtime_config_path);
        let attach_socket_dir = PathBuf::from(&loaded.runtime.attach_socket_dir);
        let container_exits_dir = PathBuf::from(&loaded.runtime.container_exits_dir);
        let cgroup_driver = loaded.runtime.cgroup_driver.map(|driver| driver.as_proto());
        let shim = ShimConfig {
            shim_path: PathBuf::from(&loaded.runtime.shim_path),
            runtime_config_path: runtime_config_path.clone(),
            monitor_cgroup: loaded.runtime.monitor_cgroup.clone(),
            work_dir: PathBuf::from(&loaded.runtime.shim_dir),
            attach_socket_dir: attach_socket_dir.clone(),
            container_exits_dir: container_exits_dir.clone(),
            io_uid: loaded.runtime.io_uid,
            io_gid: loaded.runtime.io_gid,
            monitor_env: loaded.runtime.monitor_env.clone(),
            debug: loaded.runtime.shim_debug,
            log_to_journald: loaded.runtime.log_to_journald,
            no_sync_log: loaded.runtime.no_sync_log,
            no_pivot: loaded.runtime.no_pivot,
            no_new_keyring: loaded.runtime.no_new_keyring,
            systemd_cgroup: matches!(
                loaded.runtime.cgroup_driver,
                Some(CgroupDriverConfig::Systemd)
            ),
            runtime_path: runtime_path.clone(),
            max_container_log_line_size: loaded.logging.max_container_log_line_size,
            state_db_path: root_dir.join("crius.db"),
        };

        Self {
            root_dir,
            runtime: runtime_name,
            runtime_handlers: loaded.runtime.normalized_handlers(),
            runtime_configs,
            runtime_root,
            log_dir,
            runtime_path,
            runtime_config_path,
            image_root: PathBuf::from(&loaded.image.root),
            image_driver: loaded.image.driver.clone(),
            image_global_auth_file: PathBuf::from(&loaded.image.global_auth_file),
            image_namespaced_auth_dir: PathBuf::from(&loaded.image.namespaced_auth_dir),
            image_default_transport: loaded.image.default_transport.clone(),
            image_short_name_mode: loaded.image.short_name_mode.clone(),
            image_pull_progress_timeout: loaded.image.pull_progress_timeout,
            image_max_concurrent_downloads: loaded.image.max_concurrent_downloads,
            image_pull_retry_count: loaded.image.pull_retry_count,
            image_registry_config_dir: PathBuf::from(&loaded.image.registry_config_dir),
            image_decryption_keys_path: PathBuf::from(&loaded.image.decryption_keys_path),
            image_decryption_decoder_path: loaded.image.decryption_decoder_path.clone(),
            image_decryption_keyprovider_config: PathBuf::from(
                &loaded.image.decryption_keyprovider_config,
            ),
            image_additional_artifact_stores: loaded
                .image
                .additional_artifact_stores
                .iter()
                .map(PathBuf::from)
                .collect(),
            image_signature_policy: PathBuf::from(&loaded.image.signature_policy),
            image_signature_policy_dir: PathBuf::from(&loaded.image.signature_policy_dir),
            image_storage_options: loaded.image.storage_options.clone(),
            image_external_snapshotters: loaded.image.external_snapshotters.clone(),
            image_volumes: loaded.image.image_volumes.clone(),
            image_pinned_images: loaded.image.pinned_images.clone(),
            image_big_files_temporary_dir: PathBuf::from(&loaded.image.big_files_temporary_dir),
            image_oci_artifact_mount_support: loaded.image.oci_artifact_mount_support,
            workloads: loaded.runtime.workloads.clone(),
            enable_pod_events: loaded.api.enable_pod_events,
            included_pod_metrics: loaded.api.included_pod_metrics.clone(),
            stats_collection_period: loaded.api.stats_collection_period,
            pod_sandbox_metrics_collection_period: loaded.api.pod_sandbox_metrics_collection_period,
            grpc_max_send_msg_size: loaded.api.grpc_max_send_msg_size,
            grpc_max_recv_msg_size: loaded.api.grpc_max_recv_msg_size,
            metrics_enable: loaded.metrics.enable,
            metrics_host: loaded.metrics.host.clone(),
            metrics_port: loaded.metrics.port,
            metrics_socket_path: PathBuf::from(&loaded.metrics.socket_path),
            metrics_enable_tls: loaded.metrics.enable_tls,
            metrics_tls_cert_file: PathBuf::from(&loaded.metrics.tls_cert_file),
            metrics_tls_key_file: PathBuf::from(&loaded.metrics.tls_key_file),
            metrics_tls_ca_file: PathBuf::from(&loaded.metrics.tls_ca_file),
            metrics_tls_min_version: loaded.metrics.tls_min_version.clone(),
            metrics_tls_cipher_suites: loaded.metrics.tls_cipher_suites.clone(),
            metrics_collectors: loaded.metrics.collectors.clone(),
            tracing_enable: loaded.tracing.enable,
            tracing_endpoint: loaded.tracing.endpoint.clone(),
            tracing_sampling_rate_per_million: loaded.tracing.sampling_rate_per_million,
            monitor_env: loaded.runtime.monitor_env.clone(),
            monitor_cgroup: loaded.runtime.monitor_cgroup.clone(),
            default_env: loaded.runtime.parsed_default_env(),
            default_capabilities: loaded
                .runtime
                .default_capabilities
                .iter()
                .map(|capability| {
                    let upper = capability.trim().to_ascii_uppercase();
                    if upper.starts_with("CAP_") {
                        upper
                    } else {
                        format!("CAP_{upper}")
                    }
                })
                .collect(),
            default_sysctls: loaded.runtime.parsed_default_sysctls().unwrap_or_default(),
            default_ulimits: loaded.runtime.parsed_default_ulimits().unwrap_or_default(),
            allowed_devices: loaded.runtime.parsed_allowed_devices(),
            additional_devices: loaded
                .runtime
                .parsed_additional_devices()
                .unwrap_or_default(),
            device_ownership_from_security_context: loaded
                .runtime
                .device_ownership_from_security_context,
            add_inheritable_capabilities: loaded.runtime.add_inheritable_capabilities,
            base_runtime_spec: None,
            default_mounts_file: PathBuf::from(&loaded.runtime.default_mounts_file),
            hooks_dir: loaded.runtime.hooks_dir.iter().map(PathBuf::from).collect(),
            absent_mount_sources_to_reject: loaded
                .runtime
                .absent_mount_sources_to_reject
                .iter()
                .map(PathBuf::from)
                .collect(),
            disable_proc_mount: loaded.runtime.disable_proc_mount,
            timezone: loaded.runtime.timezone.clone(),
            attach_socket_dir,
            container_exits_dir,
            clean_shutdown_file: PathBuf::from(&loaded.runtime.clean_shutdown_file),
            container_stop_timeout: loaded.runtime.container_stop_timeout,
            version_file: PathBuf::from(&loaded.runtime.version_file),
            version_file_persist: PathBuf::from(&loaded.runtime.version_file_persist),
            criu_path: PathBuf::from(&loaded.runtime.criu_path),
            criu_image_path: PathBuf::from(&loaded.runtime.criu_image_path),
            criu_work_path: PathBuf::from(&loaded.runtime.criu_work_path),
            enable_criu_support: loaded.runtime.enable_criu_support,
            internal_wipe: loaded.runtime.internal_wipe,
            internal_repair: loaded.runtime.internal_repair,
            bind_mount_prefix: PathBuf::from(&loaded.runtime.bind_mount_prefix),
            disable_cgroup: loaded.runtime.disable_cgroup,
            tolerate_missing_hugetlb_controller: loaded.runtime.tolerate_missing_hugetlb_controller,
            separate_pull_cgroup: loaded.runtime.separate_pull_cgroup.clone(),
            seccomp_profile: PathBuf::from(&loaded.security.seccomp_profile),
            privileged_seccomp_profile: loaded.security.privileged_seccomp_profile.clone(),
            unset_seccomp_profile: loaded.security.unset_seccomp_profile.clone(),
            apparmor_default_profile: loaded.security.apparmor_default_profile.clone(),
            disable_apparmor: loaded.security.disable_apparmor,
            enable_selinux: loaded.security.enable_selinux,
            selinux_category_range: loaded.security.selinux_category_range,
            hostnetwork_disable_selinux: loaded.security.hostnetwork_disable_selinux,
            uid_mappings: None,
            gid_mappings: None,
            minimum_mappable_uid: loaded.runtime.minimum_mappable_uid,
            minimum_mappable_gid: loaded.runtime.minimum_mappable_gid,
            io_uid: loaded.runtime.io_uid,
            io_gid: loaded.runtime.io_gid,
            pids_limit: loaded.runtime.pids_limit,
            infra_ctr_cpuset: loaded.runtime.infra_ctr_cpuset.clone(),
            shared_cpuset: loaded.runtime.shared_cpuset.clone(),
            exec_cpu_affinity: loaded.runtime.exec_cpu_affinity.clone(),
            irqbalance_config_file: PathBuf::from(&loaded.runtime.irqbalance_config_file),
            irqbalance_config_restore_file: loaded.runtime.irqbalance_config_restore_file.clone(),
            read_only: loaded.runtime.read_only,
            no_pivot: loaded.runtime.no_pivot,
            no_new_keyring: loaded.runtime.no_new_keyring,
            pause_image: loaded.runtime.pause_image.clone(),
            pause_command: loaded.runtime.pause_command.clone(),
            drop_infra_ctr: loaded.runtime.drop_infra_ctr,
            cni_config,
            cgroup_driver,
            exec_sync_io_drain_timeout: loaded.api.exec_sync_io_drain_timeout,
            max_container_log_line_size: loaded.logging.max_container_log_line_size,
            log_to_journald: loaded.runtime.log_to_journald,
            no_sync_log: loaded.runtime.no_sync_log,
            restrict_oom_score_adj: loaded.runtime.restrict_oom_score_adj,
            enable_unprivileged_ports: loaded.runtime.enable_unprivileged_ports,
            enable_unprivileged_icmp: loaded.runtime.enable_unprivileged_icmp,
            rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
            shim,
            streaming: loaded.api.streaming.clone(),
            config_path: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RuntimeReloadableConfig {
    pub pause_image: String,
    pub pinned_images: Vec<String>,
    pub registry_config_dir: PathBuf,
    pub global_auth_file: PathBuf,
    pub namespaced_auth_dir: PathBuf,
    pub signature_policy: PathBuf,
    pub signature_policy_dir: PathBuf,
    pub decryption_keys_path: PathBuf,
    pub decryption_decoder_path: String,
    pub decryption_keyprovider_config: PathBuf,
    pub seccomp_profile: PathBuf,
    pub apparmor_default_profile: String,
    pub cni_config_dirs: Vec<PathBuf>,
    pub cni_conf_template: Option<PathBuf>,
    pub cni_max_conf_num: usize,
    pub cni_default_network_name: Option<String>,
}

impl RuntimeReloadableConfig {
    pub fn from_runtime_config(config: &RuntimeConfig) -> Self {
        Self {
            pause_image: config.pause_image.clone(),
            pinned_images: config.image_pinned_images.clone(),
            registry_config_dir: config.image_registry_config_dir.clone(),
            global_auth_file: config.image_global_auth_file.clone(),
            namespaced_auth_dir: config.image_namespaced_auth_dir.clone(),
            signature_policy: config.image_signature_policy.clone(),
            signature_policy_dir: config.image_signature_policy_dir.clone(),
            decryption_keys_path: config.image_decryption_keys_path.clone(),
            decryption_decoder_path: config.image_decryption_decoder_path.clone(),
            decryption_keyprovider_config: config.image_decryption_keyprovider_config.clone(),
            seccomp_profile: config.seccomp_profile.clone(),
            apparmor_default_profile: config.apparmor_default_profile.clone(),
            cni_config_dirs: config.cni_config.config_dirs().to_vec(),
            cni_conf_template: config.cni_config.conf_template().map(Path::to_path_buf),
            cni_max_conf_num: config.cni_config.max_conf_num(),
            cni_default_network_name: config
                .cni_config
                .default_network_name()
                .map(ToOwned::to_owned),
        }
    }

    pub fn diff_fields(&self, next: &Self) -> Vec<String> {
        let mut changed = Vec::new();
        if self.pause_image != next.pause_image {
            changed.push("runtime.pause_image".to_string());
        }
        if self.pinned_images != next.pinned_images {
            changed.push("image.pinned_images".to_string());
        }
        if self.registry_config_dir != next.registry_config_dir {
            changed.push("image.registry_config_dir".to_string());
        }
        if self.global_auth_file != next.global_auth_file {
            changed.push("image.global_auth_file".to_string());
        }
        if self.namespaced_auth_dir != next.namespaced_auth_dir {
            changed.push("image.namespaced_auth_dir".to_string());
        }
        if self.signature_policy != next.signature_policy {
            changed.push("image.signature_policy".to_string());
        }
        if self.signature_policy_dir != next.signature_policy_dir {
            changed.push("image.signature_policy_dir".to_string());
        }
        if self.decryption_keys_path != next.decryption_keys_path {
            changed.push("image.decryption_keys_path".to_string());
        }
        if self.decryption_decoder_path != next.decryption_decoder_path {
            changed.push("image.decryption_decoder_path".to_string());
        }
        if self.decryption_keyprovider_config != next.decryption_keyprovider_config {
            changed.push("image.decryption_keyprovider_config".to_string());
        }
        if self.seccomp_profile != next.seccomp_profile {
            changed.push("security.seccomp_profile".to_string());
        }
        if self.apparmor_default_profile != next.apparmor_default_profile {
            changed.push("security.apparmor_default_profile".to_string());
        }
        if self.cni_config_dirs != next.cni_config_dirs {
            changed.push("network.config_dirs".to_string());
        }
        if self.cni_conf_template != next.cni_conf_template {
            changed.push("network.conf_template".to_string());
        }
        if self.cni_max_conf_num != next.cni_max_conf_num {
            changed.push("network.max_conf_num".to_string());
        }
        if self.cni_default_network_name != next.cni_default_network_name {
            changed.push("network.default_network_name".to_string());
        }
        changed
    }

    pub fn with_cni_config(&self, base: &crate::network::CniConfig) -> crate::network::CniConfig {
        let mut config = base.clone();
        config.set_config_dirs(self.cni_config_dirs.clone());
        config.set_plugin_dirs(base.plugin_dirs().to_vec());
        config.set_max_conf_num(self.cni_max_conf_num);
        config.set_default_network_name(self.cni_default_network_name.clone());
        config.set_conf_template(self.cni_conf_template.clone());
        config
    }

    pub fn to_image_reloadable_config(&self) -> ReloadableImageConfig {
        ReloadableImageConfig {
            global_auth_file: (!self.global_auth_file.as_os_str().is_empty())
                .then(|| self.global_auth_file.clone()),
            namespaced_auth_dir: (!self.namespaced_auth_dir.as_os_str().is_empty())
                .then(|| self.namespaced_auth_dir.clone()),
            registry_config_dir: (!self.registry_config_dir.as_os_str().is_empty())
                .then(|| self.registry_config_dir.clone()),
            decryption_keys_path: (!self.decryption_keys_path.as_os_str().is_empty())
                .then(|| self.decryption_keys_path.clone()),
            decryption_decoder_path: self.decryption_decoder_path.clone(),
            decryption_keyprovider_config: (!self
                .decryption_keyprovider_config
                .as_os_str()
                .is_empty())
            .then(|| self.decryption_keyprovider_config.clone()),
            pinned_image_patterns: self.pinned_images.clone(),
            signature_policy: (!self.signature_policy.as_os_str().is_empty())
                .then(|| self.signature_policy.clone()),
            signature_policy_dir: (!self.signature_policy_dir.as_os_str().is_empty())
                .then(|| self.signature_policy_dir.clone()),
        }
    }

    pub fn from_loaded_config(config: &crate::config::Config) -> Self {
        Self {
            pause_image: config.runtime.pause_image.clone(),
            pinned_images: config.image.pinned_images.clone(),
            registry_config_dir: PathBuf::from(&config.image.registry_config_dir),
            global_auth_file: PathBuf::from(&config.image.global_auth_file),
            namespaced_auth_dir: PathBuf::from(&config.image.namespaced_auth_dir),
            signature_policy: PathBuf::from(&config.image.signature_policy),
            signature_policy_dir: PathBuf::from(&config.image.signature_policy_dir),
            decryption_keys_path: PathBuf::from(&config.image.decryption_keys_path),
            decryption_decoder_path: config.image.decryption_decoder_path.clone(),
            decryption_keyprovider_config: PathBuf::from(
                &config.image.decryption_keyprovider_config,
            ),
            seccomp_profile: PathBuf::from(&config.security.seccomp_profile),
            apparmor_default_profile: config.security.apparmor_default_profile.clone(),
            cni_config_dirs: config
                .network
                .config_dirs
                .iter()
                .map(PathBuf::from)
                .collect(),
            cni_conf_template: (!config.network.conf_template.trim().is_empty())
                .then(|| PathBuf::from(&config.network.conf_template)),
            cni_max_conf_num: config.network.max_conf_num,
            cni_default_network_name: config.network.default_network_name.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RuntimeReloadState {
    pub last_reload_at_unix_millis: Option<i64>,
    pub last_reload_source: Option<String>,
    pub last_reload_fields: Vec<String>,
    pub last_reload_error: Option<String>,
    pub watcher_active: bool,
    pub watcher_status: RuntimeReloadWatcherStatus,
    pub watcher_backoff_count: u32,
    pub watcher_next_retry_unix_millis: Option<i64>,
    pub watcher_last_error: Option<String>,
    pub config_file_watch: bool,
    pub cni_watch_dirs: Vec<String>,
    pub last_cni_watch_at_unix_millis: Option<i64>,
    pub last_cni_watch_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeReloadWatcherStatus {
    #[default]
    Stopped,
    Running,
    Backoff,
    Error,
}

impl RuntimeReloadState {
    pub fn mark_watcher_running(&mut self) {
        self.watcher_active = true;
        self.watcher_status = RuntimeReloadWatcherStatus::Running;
        self.watcher_backoff_count = 0;
        self.watcher_next_retry_unix_millis = None;
        self.watcher_last_error = None;
    }

    pub fn mark_watcher_stopped(&mut self) {
        self.watcher_active = false;
        self.watcher_status = RuntimeReloadWatcherStatus::Stopped;
        self.watcher_next_retry_unix_millis = None;
    }

    pub fn mark_watcher_error(
        &mut self,
        error: impl Into<String>,
        now_unix_millis: i64,
    ) -> std::time::Duration {
        self.watcher_active = true;
        self.watcher_status = RuntimeReloadWatcherStatus::Backoff;
        self.watcher_backoff_count = self.watcher_backoff_count.saturating_add(1);
        let exponent = self.watcher_backoff_count.saturating_sub(1).min(5);
        let backoff_secs = 1_u64 << exponent;
        let backoff = std::time::Duration::from_secs(backoff_secs);
        self.watcher_next_retry_unix_millis =
            Some(now_unix_millis + i64::try_from(backoff.as_millis()).unwrap_or(i64::MAX));
        self.watcher_last_error = Some(error.into());
        backoff
    }
}

#[derive(Clone)]
pub struct RuntimeRegistry {
    default_handler: String,
    runtimes: Arc<HashMap<String, Arc<dyn crate::runtime::RuntimeBackend>>>,
    container_create_timeouts: Arc<HashMap<String, u32>>,
    container_handlers: Arc<std::sync::Mutex<HashMap<String, String>>>,
}

impl RuntimeRegistry {
    pub(super) fn new(
        default_handler: String,
        runtimes: HashMap<String, Arc<dyn crate::runtime::RuntimeBackend>>,
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

    pub(super) fn runtime_handler_name_for_annotations_map(
        &self,
        annotations: &HashMap<String, String>,
    ) -> String {
        annotations
            .get(CRIO_RUNTIME_HANDLER_ANNOTATION)
            .or_else(|| annotations.get(CONTAINERD_RUNTIME_HANDLER_ANNOTATION))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(self.default_handler.as_str())
            .to_string()
    }

    fn remember_container_handler(&self, container_id: &str, handler: &str) {
        if let Ok(mut handlers) = self.container_handlers.lock() {
            handlers.insert(container_id.to_string(), handler.to_string());
        }
    }

    pub(super) fn remember_recovered_container_handler(&self, container_id: &str, handler: &str) {
        self.remember_container_handler(container_id, handler);
    }

    fn forget_container_handler(&self, container_id: &str) {
        if let Ok(mut handlers) = self.container_handlers.lock() {
            handlers.remove(container_id);
        }
    }

    pub(super) fn runtime_for_handler(
        &self,
        handler: &str,
    ) -> anyhow::Result<Arc<dyn crate::runtime::RuntimeBackend>> {
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
    ) -> anyhow::Result<Arc<dyn crate::runtime::RuntimeBackend>> {
        let handler = annotations
            .get(CRIO_RUNTIME_HANDLER_ANNOTATION)
            .or_else(|| annotations.get(CONTAINERD_RUNTIME_HANDLER_ANNOTATION))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(self.default_handler.as_str());
        self.runtime_for_handler(handler)
    }

    pub(super) fn runtime_for_container(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Arc<dyn crate::runtime::RuntimeBackend>> {
        if let Ok(handlers) = self.container_handlers.lock() {
            if let Some(handler) = handlers.get(container_id) {
                return self.runtime_for_handler(handler);
            }
        }

        for (handler, runtime) in self.runtimes.iter() {
            if runtime
                .runtime_context()
                .bundle_path_for(container_id)
                .exists()
            {
                self.remember_container_handler(container_id, handler);
                return Ok(runtime.clone());
            }
        }

        self.runtime_for_handler(&self.default_handler)
    }

    pub(super) fn runtime_handler_name_for_container(
        &self,
        container_id: &str,
    ) -> anyhow::Result<String> {
        if let Ok(handlers) = self.container_handlers.lock() {
            if let Some(handler) = handlers.get(container_id) {
                return Ok(handler.clone());
            }
        }

        for (handler, runtime) in self.runtimes.iter() {
            if runtime
                .runtime_context()
                .bundle_path_for(container_id)
                .exists()
            {
                self.remember_container_handler(container_id, handler);
                return Ok(handler.clone());
            }
        }

        Ok(self.default_handler.clone())
    }

    pub(super) fn bundle_path_for_container(&self, container_id: &str) -> anyhow::Result<PathBuf> {
        Ok(self
            .runtime_for_container(container_id)?
            .runtime_context()
            .bundle_path_for(container_id))
    }

    pub(super) fn all_runtimes(&self) -> Vec<Arc<dyn crate::runtime::RuntimeBackend>> {
        self.runtimes.values().cloned().collect()
    }

    pub(super) fn container_has_active_runtime_state(&self, container_id: &str) -> bool {
        self.runtimes.values().any(|runtime| {
            let task = runtime.task_controller();
            matches!(
                task.container_status(container_id),
                Ok(crate::runtime::ContainerStatus::Created
                    | crate::runtime::ContainerStatus::Running)
            ) || task.is_container_paused(container_id).unwrap_or(false)
        })
    }

    pub(super) fn is_container_paused(&self, container_id: &str) -> anyhow::Result<bool> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .is_container_paused(container_id)
    }

    pub(super) fn restore_attach_shim(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .restore_attach_shim(container_id)
    }

    pub(super) fn shim_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Option<crate::shim_rpc::StatusResponse>> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .shim_status(container_id)
    }

    pub(super) fn restore_container_from_checkpoint(
        &self,
        container_id: &str,
        checkpoint_path: &Path,
        work_path: &Path,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .restore_container_from_checkpoint(container_id, checkpoint_path, work_path)
    }

    pub(super) fn enforce_oom_score_adj_policy(
        &self,
        container_id: &str,
        spec: &mut crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .runtime_context()
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
            .runtime_context()
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
            .runtime_context()
            .build_spec(container_id, config)
    }

    pub(super) fn write_bundle(
        &self,
        container_id: &str,
        rootfs: &Path,
        spec: &crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .runtime_context()
            .write_bundle(container_id, rootfs, spec)
    }

    pub(super) fn pause_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .pause_container(container_id)
    }

    pub(super) fn checkpoint_container(
        &self,
        container_id: &str,
        location: &Path,
        work_path: &Path,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .checkpoint_container(container_id, location, work_path)
    }

    pub(super) fn resume_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
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
        let created = runtime
            .task_controller()
            .create_container(container_id, config)?;
        self.remember_container_handler(container_id, &handler);
        Ok(created)
    }

    fn start_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .start_container(container_id)
    }

    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .stop_container(container_id, timeout)
    }

    fn remove_container(&self, container_id: &str) -> anyhow::Result<()> {
        let result = self
            .runtime_for_container(container_id)?
            .task_controller()
            .remove_container(container_id);
        self.forget_container_handler(container_id);
        result
    }

    fn container_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<crate::runtime::ContainerStatus> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .container_status(container_id)
    }

    fn reopen_container_log(&self, container_id: &str) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .reopen_container_log(container_id)
    }

    fn exec_in_container(
        &self,
        container_id: &str,
        command: &[String],
        tty: bool,
    ) -> anyhow::Result<i32> {
        self.runtime_for_container(container_id)?
            .task_controller()
            .exec_in_container(container_id, command, tty)
    }

    fn update_container_resources(
        &self,
        container_id: &str,
        resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> anyhow::Result<()> {
        self.runtime_for_container(container_id)?
            .task_controller()
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
        tokio::time::timeout(remaining, future).await.map_err(|_| {
            Status::deadline_exceeded(format!(
                "container create phase {phase} exceeded runtime handler create timeout of {}s",
                deadline.timeout_secs
            ))
        })?
    }

    pub fn metrics_provider(&self) -> RuntimeMetricsProvider {
        RuntimeMetricsProvider {
            containers: self.containers.clone(),
            pod_sandboxes: self.pod_sandboxes.clone(),
            config: self.config.clone(),
            events: self.events.clone(),
            container_stats_cache: self.container_stats_cache.clone(),
            pod_stats_cache: self.pod_stats_cache.clone(),
            pod_metrics_cache: self.pod_metrics_cache.clone(),
        }
    }

    pub fn image_service(&self) -> ImageServiceImpl {
        self.image_service.clone()
    }
}

impl Drop for RuntimeServiceImpl {
    fn drop(&mut self) {
        let _ = self.reload_watcher_shutdown.send(());
    }
}

impl RuntimeMetricsProvider {
    pub async fn snapshot(&self) -> crate::metrics::RuntimeMetricsSnapshot {
        let runtime_ready = RuntimeServiceImpl::runtime_binary_ready(&self.config.runtime_path);
        let network_ready = match crate::network::CniManager::new(
            self.config
                .cni_config
                .plugin_dirs()
                .iter()
                .map(|dir| dir.display().to_string())
                .collect(),
            self.config
                .cni_config
                .config_dirs()
                .iter()
                .map(|dir| dir.display().to_string())
                .collect(),
            self.config.cni_config.cache_dir().display().to_string(),
        ) {
            Ok(mut cni) => {
                cni.set_max_conf_num(self.config.cni_config.max_conf_num());
                cni.set_default_network_name(
                    self.config
                        .cni_config
                        .default_network_name()
                        .map(ToOwned::to_owned),
                );
                cni.load_network_configs()
                    .await
                    .map(|status| status.ready)
                    .unwrap_or(false)
            }
            Err(_) => false,
        };

        crate::metrics::RuntimeMetricsSnapshot {
            runtime_ready,
            network_ready,
            container_count: self.containers.lock().await.len(),
            pod_sandbox_count: self.pod_sandboxes.lock().await.len(),
            event_subscriber_count: self.events.receiver_count(),
            container_stats_cache_entries: self.container_stats_cache.lock().await.len(),
            pod_stats_cache_entries: self.pod_stats_cache.lock().await.len(),
            pod_metrics_cache_entries: self.pod_metrics_cache.lock().await.len(),
        }
    }
}

impl RuntimeServiceImpl {
    pub(super) fn runtime_binary_ready(path: &Path) -> bool {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(_) => return false,
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

    fn shared_cpuset_annotation_enabled(
        annotations: &HashMap<String, String>,
        container_name: Option<&str>,
    ) -> bool {
        let Some(container_name) = container_name
            .map(str::trim)
            .filter(|name| !name.is_empty())
        else {
            return false;
        };
        let key = format!("cpu-shared.crio.io/{container_name}");
        annotations
            .get(&key)
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on" | "enable" | "enabled"
                )
            })
            .unwrap_or(false)
    }

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
        let container = {
            let containers = self.containers.lock().await;
            containers.get(container_id).cloned()
        };
        let shared_enabled = container.as_ref().is_some_and(|container| {
            Self::shared_cpuset_annotation_enabled(
                &container.annotations,
                container
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.name.as_str()),
            )
        });
        if shared_enabled && !self.config.shared_cpuset.trim().is_empty() {
            if let Some(cpu) =
                crate::runtime::RuncRuntime::first_cpu_from_cpuset(&self.config.shared_cpuset)
            {
                return Some(cpu);
            }
        }

        self.container_internal_state(container_id)
            .await
            .and_then(|state| {
                state.linux_resources.and_then(|resources| {
                    crate::runtime::RuncRuntime::first_cpu_from_cpuset(&resources.cpuset_cpus)
                })
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

    pub(super) fn effective_container_namespace_options(
        &self,
        requested: Option<&NamespaceOption>,
        sandbox: Option<&StoredNamespaceOptions>,
    ) -> Option<NamespaceOption> {
        let mut effective = self.effective_userns_options(requested);
        let Some(sandbox) = sandbox else {
            return effective;
        };

        let sandbox = sandbox.to_proto();
        let requested_missing = requested.is_none();
        let effective = effective.get_or_insert_with(|| sandbox.clone());

        // Match CRI-O's behavior: the sandbox decides whether workload
        // containers must run in host namespaces, even if the container
        // request omitted namespace options or left them at proto defaults.
        if sandbox.network == NamespaceMode::Node as i32 || requested_missing {
            effective.network = sandbox.network;
        }

        if sandbox.pid == NamespaceMode::Node as i32 {
            effective.pid = sandbox.pid;
            effective.target_id.clear();
        } else if requested_missing {
            effective.pid = sandbox.pid;
            effective.target_id = sandbox.target_id.clone();
        }

        if sandbox.ipc == NamespaceMode::Node as i32 || requested_missing {
            effective.ipc = sandbox.ipc;
        }

        if effective.userns_options.is_none() {
            effective.userns_options = sandbox.userns_options;
        }

        Some(effective.clone())
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

    pub fn irqbalance_config_file(&self) -> &Path {
        &self.config.irqbalance_config_file
    }

    pub fn irqbalance_config_restore_file(&self) -> &str {
        &self.config.irqbalance_config_restore_file
    }

    pub fn seccomp_notifier_dir(&self) -> &Path {
        &self.seccomp_notifier_dir
    }

    pub(super) fn seccomp_notifier_snapshot(
        &self,
        container_id: &str,
    ) -> Option<seccomp_notifier::SeccompNotifierSnapshot> {
        self.seccomp_notifier_snapshots
            .lock()
            .ok()
            .and_then(|snapshots| snapshots.get(container_id).cloned())
    }

    pub fn seccomp_notifier_active_containers(&self) -> Vec<String> {
        self.seccomp_notifier_snapshots
            .lock()
            .map(|snapshots| snapshots.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn seccomp_notifier_socket_path(&self, container_id: &str) -> PathBuf {
        self.seccomp_notifier_dir.join(container_id)
    }

    pub(super) fn ensure_seccomp_notifier(
        &self,
        container_id: &str,
        mode: crate::runtime::SeccompNotifierMode,
    ) -> Result<PathBuf, Status> {
        let socket_path = self.seccomp_notifier_socket_path(container_id);
        let mut notifiers = self
            .seccomp_notifiers
            .lock()
            .map_err(|_| Status::internal("seccomp notifier mutex poisoned"))?;
        if !notifiers.contains_key(container_id) {
            let notifier = seccomp_notifier::SeccompNotifier::bind(
                socket_path.clone(),
                container_id.to_string(),
                mode,
                self.seccomp_notification_tx.clone(),
            )
            .map_err(|err| {
                Status::internal(format!(
                    "failed to bind seccomp notifier socket {}: {}",
                    socket_path.display(),
                    err
                ))
            })?;
            if let Ok(mut snapshots) = self.seccomp_notifier_snapshots.lock() {
                snapshots.insert(container_id.to_string(), notifier.snapshot());
            }
            notifiers.insert(container_id.to_string(), notifier);
        }
        Ok(socket_path)
    }

    pub(super) fn remove_seccomp_notifier(&self, container_id: &str) {
        if let Ok(mut notifiers) = self.seccomp_notifiers.lock() {
            if let Some(mut notifier) = notifiers.remove(container_id) {
                notifier.close();
            }
        }
        if let Ok(mut snapshots) = self.seccomp_notifier_snapshots.lock() {
            snapshots.remove(container_id);
        }
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

    pub fn record_irqbalance_restore_status(&self, status: IrqBalanceRestoreStatus) {
        if let Ok(mut state) = self.last_irqbalance_restore_status.lock() {
            *state = Some(status);
        }
    }

    pub fn last_irqbalance_restore_status(&self) -> Option<IrqBalanceRestoreStatus> {
        self.last_irqbalance_restore_status
            .lock()
            .ok()
            .and_then(|state| state.clone())
    }

    pub fn last_startup_repair_succeeded(&self) -> Option<bool> {
        self.last_startup_repair_succeeded
            .lock()
            .ok()
            .and_then(|state| *state)
    }

    pub(super) fn recovery_stage_summary(
        stage: RecoveryStage,
        started_at: Instant,
        success: bool,
        items: usize,
        error: Option<String>,
    ) -> RecoveryStageSummary {
        RecoveryStageSummary {
            name: stage.as_str().to_string(),
            success,
            duration_millis: started_at.elapsed().as_millis() as u64,
            items,
            error,
        }
    }

    pub(super) fn record_last_recovery_result(&self, result: RecoveryResultSummary) {
        if let Ok(mut state) = self.last_recovery_result.lock() {
            *state = Some(result);
        }
    }

    pub fn last_recovery_result(&self) -> Option<RecoveryResultSummary> {
        self.last_recovery_result
            .lock()
            .ok()
            .and_then(|state| state.clone())
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
        let shim_work_dir = config.shim.work_dir.clone();
        Self::new_with_shim_work_dir(config, nri_config, shim_work_dir)
    }

    pub fn new_with_runtime_backends(
        config: RuntimeConfig,
        runtimes: HashMap<String, Arc<dyn crate::runtime::RuntimeBackend>>,
    ) -> Self {
        let shim_work_dir = config.shim.work_dir.clone();
        Self::new_with_shim_work_dir_and_nri_and_runtime_backends(
            config,
            NriConfig::default(),
            shim_work_dir,
            None,
            Some(runtimes),
        )
    }

    pub fn new_with_nri_api(
        config: RuntimeConfig,
        nri_config: NriConfig,
        nri: Arc<dyn NriApi>,
    ) -> Self {
        let shim_work_dir = config.shim.work_dir.clone();
        Self::new_with_shim_work_dir_and_nri(config, nri_config, shim_work_dir, Some(nri))
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
        Self::new_with_shim_work_dir_and_nri_and_runtime_backends(
            config,
            nri_config,
            shim_work_dir,
            injected_nri,
            None,
        )
    }

    pub(super) fn new_with_shim_work_dir_and_nri_and_runtime_backends(
        config: RuntimeConfig,
        nri_config: NriConfig,
        shim_work_dir: PathBuf,
        injected_nri: Option<Arc<dyn NriApi>>,
        injected_runtimes: Option<HashMap<String, Arc<dyn crate::runtime::RuntimeBackend>>>,
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
        runtime_configs
            .entry(config.runtime.clone())
            .or_insert_with(|| crate::config::ResolvedRuntimeHandlerConfig {
                backend: "runc".to_string(),
                backend_options: HashMap::new(),
                runtime_path: config.runtime_path.display().to_string(),
                runtime_config_path: config.runtime_config_path.display().to_string(),
                runtime_root: config.runtime_root.display().to_string(),
                platform_runtime_paths: HashMap::new(),
                monitor_path: config.shim.shim_path.display().to_string(),
                monitor_cgroup: config.monitor_cgroup.clone(),
                monitor_env: config.monitor_env.clone(),
                stream_websockets: false,
                allowed_annotations: Vec::new(),
                default_annotations: HashMap::new(),
                privileged_without_host_devices: false,
                privileged_without_host_devices_all_devices_allowed: false,
                container_create_timeout: 240,
                snapshotter: "internal-overlay-untar".to_string(),
            });
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
        let runtimes: HashMap<String, Arc<dyn crate::runtime::RuntimeBackend>> =
            if let Some(runtimes) = injected_runtimes {
                runtimes
            } else {
                config
                    .runtime_configs
                    .iter()
                    .map(|(handler, runtime_config)| {
                        let mut shim_config = config.shim.clone();
                        shim_config.work_dir = resolved_shim_work_dir.clone();
                        shim_config.attach_socket_dir = config.attach_socket_dir.clone();
                        shim_config.container_exits_dir = config.container_exits_dir.clone();
                        shim_config.shim_path = PathBuf::from(&runtime_config.monitor_path);
                        shim_config.runtime_config_path =
                            PathBuf::from(runtime_config.runtime_config_path.as_str());
                        shim_config.monitor_cgroup = runtime_config.monitor_cgroup.clone();
                        shim_config.io_uid = config.io_uid;
                        shim_config.io_gid = config.io_gid;
                        shim_config.runtime_path = PathBuf::from(&runtime_config.runtime_path);
                        shim_config.monitor_env = runtime_config.monitor_env.clone();
                        shim_config.no_sync_log = config.no_sync_log;
                        shim_config.no_new_keyring = config.no_new_keyring;
                        shim_config.systemd_cgroup =
                            config.cgroup_driver == Some(CgroupDriver::Systemd);
                        (handler.clone(), {
                            let mut runtime = RuncRuntime::with_shim_and_image_storage(
                                PathBuf::from(&runtime_config.runtime_path),
                                PathBuf::from(&runtime_config.runtime_root),
                                config.image_root.clone(),
                                shim_config,
                            );
                            runtime.set_default_env(config.default_env.clone());
                            runtime.set_default_capabilities(config.default_capabilities.clone());
                            runtime.set_default_sysctls(config.default_sysctls.clone());
                            runtime.set_default_ulimits(config.default_ulimits.clone());
                            runtime.set_allowed_devices(config.allowed_devices.clone());
                            runtime.set_additional_devices(config.additional_devices.clone());
                            runtime.set_device_ownership_from_security_context(
                                config.device_ownership_from_security_context,
                            );
                            runtime.set_privileged_without_host_devices(
                                runtime_config.privileged_without_host_devices,
                            );
                            runtime.set_privileged_without_host_devices_all_devices_allowed(
                                runtime_config.privileged_without_host_devices_all_devices_allowed,
                            );
                            runtime.set_add_inheritable_capabilities(
                                config.add_inheritable_capabilities,
                            );
                            runtime.set_base_runtime_spec(config.base_runtime_spec.clone());
                            runtime.set_default_mounts_file(config.default_mounts_file.clone());
                            runtime.set_hooks_dirs(config.hooks_dir.clone());
                            runtime.set_absent_mount_sources_to_reject(
                                config.absent_mount_sources_to_reject.clone(),
                            );
                            runtime.set_image_volumes_mode(
                                crate::runtime::ImageVolumesMode::from_config(
                                    &config.image_volumes,
                                ),
                            );
                            runtime.set_rootfs_snapshotter(
                                crate::runtime::RootfsSnapshotter::from_config(
                                    runtime_config.snapshotter.as_str(),
                                ),
                            );
                            runtime.set_disable_proc_mount(config.disable_proc_mount);
                            runtime.set_timezone(config.timezone.clone());
                            runtime.set_runtime_config_path(PathBuf::from(
                                runtime_config.runtime_config_path.as_str(),
                            ));
                            runtime.set_container_create_timeout_secs(
                                runtime_config.container_create_timeout,
                            );
                            runtime.set_container_stop_timeout_secs(config.container_stop_timeout);
                            runtime.set_state_db_path(config.root_dir.join("crius.db"));
                            runtime.set_criu_path(config.criu_path.clone());
                            runtime.set_restrict_oom_score_adj(config.restrict_oom_score_adj);
                            runtime.set_bind_mount_prefix(config.bind_mount_prefix.clone());
                            runtime.set_disable_cgroup(config.disable_cgroup);
                            runtime.set_rootless(config.rootless.clone());
                            runtime
                                .set_default_seccomp_profile_path(config.seccomp_profile.clone());
                            runtime.set_exec_cpu_affinity(config.exec_cpu_affinity.clone());
                            runtime.set_no_pivot(config.no_pivot);
                            runtime.set_no_new_keyring(config.no_new_keyring);
                            runtime.set_cgroup_driver(match config.cgroup_driver {
                                Some(CgroupDriver::Systemd) => {
                                    crate::config::CgroupDriverConfig::Systemd
                                }
                                _ => crate::config::CgroupDriverConfig::Cgroupfs,
                            });
                            Arc::new(crate::runtime::RuncBackend::new(runtime))
                                as Arc<dyn crate::runtime::RuntimeBackend>
                        })
                    })
                    .collect()
            };
        let runtime =
            RuntimeRegistry::new(config.runtime.clone(), runtimes, container_create_timeouts);
        let image_service = ImageServiceImpl::new_with_options(ImageServiceOptions {
            storage_path: config.image_root.clone(),
            ledger_db_path: Some(config.root_dir.join("crius.db")),
            storage_driver: config.image_driver.clone(),
            storage_options: config.image_storage_options.clone(),
            global_auth_file: (!config.image_global_auth_file.as_os_str().is_empty())
                .then(|| config.image_global_auth_file.clone()),
            namespaced_auth_dir: (!config.image_namespaced_auth_dir.as_os_str().is_empty())
                .then(|| config.image_namespaced_auth_dir.clone()),
            default_transport: config.image_default_transport.clone(),
            short_name_mode: config.image_short_name_mode.clone(),
            pull_progress_timeout: config.image_pull_progress_timeout,
            max_concurrent_downloads: config.image_max_concurrent_downloads,
            pull_retry_count: config.image_pull_retry_count,
            registry_config_dir: (!config.image_registry_config_dir.as_os_str().is_empty())
                .then(|| config.image_registry_config_dir.clone()),
            decryption_keys_path: (!config.image_decryption_keys_path.as_os_str().is_empty())
                .then(|| config.image_decryption_keys_path.clone()),
            decryption_decoder_path: config.image_decryption_decoder_path.clone(),
            decryption_keyprovider_config: (!config
                .image_decryption_keyprovider_config
                .as_os_str()
                .is_empty())
            .then(|| config.image_decryption_keyprovider_config.clone()),
            additional_artifact_stores: config.image_additional_artifact_stores.clone(),
            pinned_image_patterns: config.image_pinned_images.clone(),
            signature_policy: (!config.image_signature_policy.as_os_str().is_empty())
                .then(|| config.image_signature_policy.clone()),
            signature_policy_dir: (!config.image_signature_policy_dir.as_os_str().is_empty())
                .then(|| config.image_signature_policy_dir.clone()),
            big_files_temporary_dir: (!config.image_big_files_temporary_dir.as_os_str().is_empty())
                .then(|| config.image_big_files_temporary_dir.clone()),
            separate_pull_cgroup: config.separate_pull_cgroup.clone(),
            cgroup_driver: match config.cgroup_driver {
                Some(CgroupDriver::Systemd) => crate::config::CgroupDriverConfig::Systemd,
                _ => crate::config::CgroupDriverConfig::Cgroupfs,
            },
            rootless: config.rootless.clone(),
            disable_cgroup: config.disable_cgroup,
            #[cfg(test)]
            pull_cgroup_root: None,
        })
        .expect("Failed to initialize image service");

        let mut pod_cni_config = config.cni_config.clone();
        pod_cni_config.set_rootless_config(Some(config.rootless.clone()));
        if config.rootless.enabled {
            pod_cni_config.set_netns_mount_dir(config.rootless.netns_dir.clone());
        }
        let pod_manager = PodSandboxManager::new(
            runtime.clone(),
            config.root_dir.join("pods"),
            config.pause_image.clone(),
            config.pause_command.clone(),
            config.infra_ctr_cpuset.clone(),
            pod_cni_config,
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
        let internal_services = crate::services::InternalServices::new(
            crate::services::EventService::from_sender(events.clone())
                .with_ledger(persistence.clone()),
        );
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
        let reloadable_config = StdArc::new(StdMutex::new(
            RuntimeReloadableConfig::from_runtime_config(&config),
        ));
        let reload_state = StdArc::new(StdMutex::new(RuntimeReloadState {
            config_file_watch: config.config_path.is_some(),
            cni_watch_dirs: config
                .cni_config
                .config_dirs()
                .iter()
                .map(|dir| dir.display().to_string())
                .collect(),
            ..Default::default()
        }));
        let (reload_watcher_shutdown, _) = tokio::sync::broadcast::channel(1);
        let seccomp_notifier_dir = config.runtime_root.join("seccomp-notifier");
        let seccomp_notifiers = StdArc::new(StdMutex::new(HashMap::new()));
        let seccomp_notifier_snapshots = StdArc::new(StdMutex::new(HashMap::<
            String,
            seccomp_notifier::SeccompNotifierSnapshot,
        >::new()));
        let (seccomp_notification_tx, mut seccomp_notification_rx) =
            tokio::sync::mpsc::unbounded_channel::<seccomp_notifier::SeccompNotificationEvent>();
        let runtime_for_seccomp = runtime.clone();
        let seccomp_snapshots_for_task = seccomp_notifier_snapshots.clone();
        let seccomp_event_worker = async move {
            let mut stop_tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
            while let Some(event) = seccomp_notification_rx.recv().await {
                log::info!(
                    "Got seccomp notifier message for container ID: {} (syscall = {})",
                    event.container_id,
                    event.syscall
                );
                if event.stop_mode {
                    if let Some(existing) = stop_tasks.remove(&event.container_id) {
                        existing.abort();
                    }
                    let runtime = runtime_for_seccomp.clone();
                    let container_id = event.container_id.clone();
                    stop_tasks.insert(
                        event.container_id.clone(),
                        tokio::spawn(async move {
                            tokio::time::sleep(seccomp_notifier::seccomp_stop_delay()).await;
                            let _ = tokio::task::spawn_blocking(move || {
                                runtime.stop_container(&container_id, Some(0))
                            })
                            .await;
                        }),
                    );
                }
                let _ = seccomp_snapshots_for_task.lock().map(|mut snapshots| {
                    if let Some(snapshot) = snapshots.get_mut(&event.container_id) {
                        *snapshot.syscalls.entry(event.syscall.clone()).or_insert(0) += 1;
                    }
                });
            }
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(seccomp_event_worker);
        } else {
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new()
                    .expect("failed to create background runtime for seccomp notifier worker");
                runtime.block_on(seccomp_event_worker);
            });
        }

        let service = Self {
            containers,
            pod_sandboxes,
            container_names,
            pod_names,
            removed_container_ids: StdArc::new(StdMutex::new(HashSet::new())),
            removed_pod_sandbox_ids: StdArc::new(StdMutex::new(HashSet::new())),
            config,
            nri_config,
            nri,
            runtime,
            pod_manager: Arc::new(tokio::sync::Mutex::new(pod_manager)),
            image_service,
            persistence,
            streaming: Arc::new(Mutex::new(None)),
            events,
            internal_services,
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
            last_recovery_result: StdArc::new(StdMutex::new(None)),
            last_irqbalance_restore_status: StdArc::new(StdMutex::new(None)),
            seccomp_notifier_dir,
            seccomp_notifiers,
            seccomp_notifier_snapshots,
            seccomp_notification_tx,
            runtime_network_config: Arc::new(Mutex::new(runtime_network_config)),
            reloadable_config,
            reload_state,
            reload_watcher_shutdown,
            exit_monitors: Arc::new(Mutex::new(HashSet::new())),
            container_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            pod_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            pod_metrics_cache: Arc::new(Mutex::new(HashMap::new())),
        };
        service.spawn_reload_watchers();
        service
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

    pub fn current_reloadable_config(&self) -> RuntimeReloadableConfig {
        self.reloadable_config
            .lock()
            .expect("reloadable config lock poisoned")
            .clone()
    }

    pub fn current_reload_state(&self) -> RuntimeReloadState {
        self.reload_state
            .lock()
            .expect("reload state lock poisoned")
            .clone()
    }

    pub(super) fn current_cni_config(&self) -> crate::network::CniConfig {
        let mut config = self
            .current_reloadable_config()
            .with_cni_config(&self.config.cni_config);
        config.set_rootless_config(Some(self.config.rootless.clone()));
        if self.config.rootless.enabled {
            config.set_netns_mount_dir(self.config.rootless.netns_dir.clone());
        }
        config
    }

    fn update_reload_state(&self, update: impl FnOnce(&mut RuntimeReloadState)) {
        if let Ok(mut state) = self.reload_state.lock() {
            update(&mut state);
        }
    }

    pub async fn apply_reloadable_config(
        &self,
        next: RuntimeReloadableConfig,
        source: &str,
    ) -> Result<Vec<String>, Status> {
        let previous = self.current_reloadable_config();
        let changed = previous.diff_fields(&next);
        if changed.is_empty() {
            self.update_reload_state(|state| {
                state.last_reload_at_unix_millis = Some(chrono::Utc::now().timestamp_millis());
                state.last_reload_source = Some(source.to_string());
                state.last_reload_fields.clear();
                state.last_reload_error = None;
            });
            return Ok(changed);
        }

        self.image_service
            .apply_reloadable_config(next.to_image_reloadable_config());

        {
            let mut pod_manager = self.pod_manager.lock().await;
            let mut next_cni = next.with_cni_config(&self.config.cni_config);
            next_cni.set_rootless_config(Some(self.config.rootless.clone()));
            if self.config.rootless.enabled {
                next_cni.set_netns_mount_dir(self.config.rootless.netns_dir.clone());
            }
            let runtime_network_config = self.runtime_network_config.lock().await.clone();
            Self::sync_generated_cni_config(&next_cni, runtime_network_config.as_ref()).map_err(
                |err| {
                    Status::invalid_argument(format!(
                        "Failed to render CNI config template: {}",
                        err
                    ))
                },
            )?;
            pod_manager.reload_runtime_network_settings(next.pause_image.clone(), next_cni);
        }

        if let Ok(mut current) = self.reloadable_config.lock() {
            *current = next.clone();
        }
        self.update_reload_state(|state| {
            state.last_reload_at_unix_millis = Some(chrono::Utc::now().timestamp_millis());
            state.last_reload_source = Some(source.to_string());
            state.last_reload_fields = changed.clone();
            state.last_reload_error = None;
            state.cni_watch_dirs = next
                .cni_config_dirs
                .iter()
                .map(|dir| dir.display().to_string())
                .collect();
        });

        Ok(changed)
    }

    pub async fn reload_config_file_once(&self) -> Result<Vec<String>, Status> {
        let path = self.config.config_path.clone().ok_or_else(|| {
            Status::failed_precondition("runtime config file path is not configured")
        })?;
        let mut config = crate::config::Config::load(&path)
            .map_err(|err| Status::internal(format!("Failed to load config file: {}", err)))?;
        config
            .apply_env_overrides()
            .map_err(|err| Status::internal(format!("Failed to apply env overrides: {}", err)))?;
        let next = RuntimeReloadableConfig::from_loaded_config(&config);
        match self.apply_reloadable_config(next, "config-file").await {
            Ok(changed) => Ok(changed),
            Err(status) => {
                self.update_reload_state(|state| {
                    state.last_reload_at_unix_millis = Some(chrono::Utc::now().timestamp_millis());
                    state.last_reload_source = Some("config-file".to_string());
                    state.last_reload_error = Some(status.message().to_string());
                });
                Err(status)
            }
        }
    }

    pub async fn reload_cni_watch_once(&self) -> crate::network::CniLoadStatus {
        let cni_config = self.current_cni_config();
        let runtime_network_config = self.runtime_network_config.lock().await.clone();
        let sync_error =
            Self::sync_generated_cni_config(&cni_config, runtime_network_config.as_ref())
                .err()
                .map(|err| err.to_string());
        let status = self.probe_cni_load_status().await;
        self.update_reload_state(|state| {
            state.last_cni_watch_at_unix_millis = Some(chrono::Utc::now().timestamp_millis());
            state.last_cni_watch_error = sync_error
                .clone()
                .or_else(|| (!status.ready).then(|| status.message.clone()));
        });
        self.publish_network_internal_event(
            "cni",
            "reload_result",
            if sync_error.is_none() && status.ready {
                crate::services::InternalEventSeverity::Info
            } else {
                crate::services::InternalEventSeverity::Warning
            },
            json!({
                "ready": status.ready,
                "reason": status.reason,
                "message": status.message,
                "loadedNetworks": status.loaded_networks,
                "declaredPlugins": status.declared_plugins,
                "missingPluginBinaries": status.missing_plugin_binaries,
                "syncError": sync_error,
            }),
        )
        .await;
        status
    }

    fn config_watch_signature(path: &Path) -> Option<(i64, u64)> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata.modified().ok()?;
        let modified = modified
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|value| value.as_secs() as i64)?;
        Some((modified, metadata.len()))
    }

    fn cni_watch_signature(config: &crate::network::CniConfig) -> Vec<(String, i64, u64)> {
        let mut entries = Vec::new();
        for dir in config.config_dirs() {
            if let Ok(read_dir) = std::fs::read_dir(dir) {
                for entry in read_dir.flatten() {
                    let path = entry.path();
                    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
                        continue;
                    };
                    if !matches!(ext, "conf" | "conflist" | "json") {
                        continue;
                    }
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if let Ok(since_epoch) = modified.duration_since(std::time::UNIX_EPOCH)
                            {
                                entries.push((
                                    path.display().to_string(),
                                    since_epoch.as_secs() as i64,
                                    metadata.len(),
                                ));
                            }
                        }
                    }
                }
            }
        }
        if let Some(template) = config.conf_template() {
            if let Ok(metadata) = std::fs::metadata(template) {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(since_epoch) = modified.duration_since(std::time::UNIX_EPOCH) {
                        entries.push((
                            template.display().to_string(),
                            since_epoch.as_secs() as i64,
                            metadata.len(),
                        ));
                    }
                }
            }
        }
        entries.sort();
        entries
    }

    fn spawn_reload_watchers(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let config_state = self.reload_state.clone();
        let config_clone = self.clone_for_background();
        let mut shutdown = self.reload_watcher_shutdown.subscribe();
        handle.spawn(async move {
            let config_path = config_clone.config.config_path.clone();
            let mut last_config_signature = config_path
                .as_deref()
                .and_then(Self::config_watch_signature);
            let mut last_cni_signature = Self::cni_watch_signature(
                &config_clone
                    .current_reloadable_config()
                    .with_cni_config(&config_clone.config.cni_config),
            );
            if let Ok(mut state) = config_state.lock() {
                state.mark_watcher_running();
            }
            let mut next_delay = std::time::Duration::from_secs(1);
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        if let Ok(mut state) = config_state.lock() {
                            state.mark_watcher_stopped();
                        }
                        break;
                    }
                    _ = tokio::time::sleep(next_delay) => {}
                }
                next_delay = std::time::Duration::from_secs(1);
                if let Some(path) = config_path.as_deref() {
                    let current_signature = Self::config_watch_signature(path);
                    if current_signature != last_config_signature {
                        match config_clone.reload_config_file_once().await {
                            Ok(_) => {
                                if let Ok(mut state) = config_state.lock() {
                                    state.mark_watcher_running();
                                }
                            }
                            Err(err) => {
                                if let Ok(mut state) = config_state.lock() {
                                    next_delay = state.mark_watcher_error(
                                        err.message().to_string(),
                                        chrono::Utc::now().timestamp_millis(),
                                    );
                                }
                            }
                        }
                        last_config_signature = current_signature;
                    }
                }
                let current_cni_signature = Self::cni_watch_signature(
                    &config_clone
                        .current_reloadable_config()
                        .with_cni_config(&config_clone.config.cni_config),
                );
                if current_cni_signature != last_cni_signature {
                    let status = config_clone.reload_cni_watch_once().await;
                    if status.ready {
                        if let Ok(mut state) = config_state.lock() {
                            state.mark_watcher_running();
                        }
                    } else if let Ok(mut state) = config_state.lock() {
                        next_delay = state.mark_watcher_error(
                            status.message,
                            chrono::Utc::now().timestamp_millis(),
                        );
                    }
                    last_cni_signature = current_cni_signature;
                }
            }
        });
    }

    fn clone_for_background(&self) -> Self {
        Self {
            containers: self.containers.clone(),
            pod_sandboxes: self.pod_sandboxes.clone(),
            container_names: self.container_names.clone(),
            pod_names: self.pod_names.clone(),
            removed_container_ids: self.removed_container_ids.clone(),
            removed_pod_sandbox_ids: self.removed_pod_sandbox_ids.clone(),
            config: self.config.clone(),
            nri_config: self.nri_config.clone(),
            nri: self.nri.clone(),
            runtime: self.runtime.clone(),
            pod_manager: self.pod_manager.clone(),
            image_service: self.image_service.clone(),
            persistence: self.persistence.clone(),
            streaming: self.streaming.clone(),
            events: self.events.clone(),
            internal_services: self.internal_services.clone(),
            shim_work_dir: self.shim_work_dir.clone(),
            attach_socket_dir: self.attach_socket_dir.clone(),
            container_exits_dir: self.container_exits_dir.clone(),
            clean_shutdown_file: self.clean_shutdown_file.clone(),
            last_startup_clean_shutdown: self.last_startup_clean_shutdown.clone(),
            version_file: self.version_file.clone(),
            version_file_persist: self.version_file_persist.clone(),
            last_startup_detected_reboot: self.last_startup_detected_reboot.clone(),
            last_startup_detected_upgrade: self.last_startup_detected_upgrade.clone(),
            last_startup_attempted_repair: self.last_startup_attempted_repair.clone(),
            last_startup_repair_succeeded: self.last_startup_repair_succeeded.clone(),
            last_recovery_result: self.last_recovery_result.clone(),
            last_irqbalance_restore_status: self.last_irqbalance_restore_status.clone(),
            seccomp_notifier_dir: self.seccomp_notifier_dir.clone(),
            seccomp_notifiers: self.seccomp_notifiers.clone(),
            seccomp_notifier_snapshots: self.seccomp_notifier_snapshots.clone(),
            seccomp_notification_tx: self.seccomp_notification_tx.clone(),
            runtime_network_config: self.runtime_network_config.clone(),
            reloadable_config: self.reloadable_config.clone(),
            reload_state: self.reload_state.clone(),
            reload_watcher_shutdown: self.reload_watcher_shutdown.clone(),
            exit_monitors: self.exit_monitors.clone(),
            container_stats_cache: self.container_stats_cache.clone(),
            pod_stats_cache: self.pod_stats_cache.clone(),
            pod_metrics_cache: self.pod_metrics_cache.clone(),
        }
    }
}
