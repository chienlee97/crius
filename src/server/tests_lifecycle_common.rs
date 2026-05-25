use super::*;
use crate::nri::{apply_container_adjustment, NriStopContainerResult, NriUpdateContainerResult};
use crate::proto::runtime::v1::image_service_server::ImageService;
use crate::server::service::ContainerCreateDeadline;
use crate::storage::ContainerRecord;
use crate::storage::PodSandboxRecord;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tempfile::tempdir;
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_stream::StreamExt;

#[path = "../../tests/common/mod.rs"]
mod common;

fn disabled_rootless() -> crate::rootless::EffectiveRootlessConfig {
    crate::rootless::EffectiveRootlessConfig::disabled()
}

fn test_cni_config() -> crate::network::CniConfig {
    let mut config = crate::network::CniConfig::default();
    config.set_teardown_timeout(Duration::from_millis(50));
    config
}

fn test_runtime_config(root_dir: PathBuf) -> RuntimeConfig {
    let test_base = root_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| root_dir.clone());
    RuntimeConfig {
        root_dir,
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string(), "kata".to_string()],
        runtime_configs: HashMap::from([
            (
                "runc".to_string(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    backend_options: HashMap::new(),
                    runtime_path: "/definitely/missing/runc".to_string(),
                    runtime_config_path: String::new(),
                    runtime_root: "/tmp/crius-test-runtime-root".to_string(),
                    platform_runtime_paths: HashMap::new(),
                    monitor_path: "/definitely/missing/crius-shim".to_string(),
                    monitor_cgroup: String::new(),
                    monitor_env: Vec::new(),
                    stream_websockets: false,
                    allowed_annotations: Vec::new(),
                    default_annotations: HashMap::new(),
                    privileged_without_host_devices: false,
                    privileged_without_host_devices_all_devices_allowed: false,
                    container_create_timeout: 240,
                    snapshotter: "internal-overlay-untar".to_string(),
                },
            ),
            (
                "kata".to_string(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    backend_options: HashMap::new(),
                    runtime_path: "/definitely/missing/kata-runtime".to_string(),
                    runtime_config_path: String::new(),
                    runtime_root: "/tmp/crius-test-kata-runtime-root".to_string(),
                    platform_runtime_paths: HashMap::new(),
                    monitor_path: "/definitely/missing/crius-shim".to_string(),
                    monitor_cgroup: String::new(),
                    monitor_env: Vec::new(),
                    stream_websockets: false,
                    allowed_annotations: Vec::new(),
                    default_annotations: HashMap::new(),
                    privileged_without_host_devices: false,
                    privileged_without_host_devices_all_devices_allowed: false,
                    container_create_timeout: 240,
                    snapshotter: "internal-overlay-untar".to_string(),
                },
            ),
        ]),
        runtime_root: PathBuf::from("/tmp/crius-test-runtime-root"),
        log_dir: PathBuf::from("/tmp/crius-test-logs"),
        runtime_path: PathBuf::from("/definitely/missing/runc"),
        runtime_config_path: PathBuf::new(),
        image_root: PathBuf::from("/tmp/crius-test-images"),
        image_driver: "overlay".to_string(),
        image_global_auth_file: PathBuf::new(),
        image_namespaced_auth_dir: PathBuf::new(),
        image_default_transport: "docker://".to_string(),
        image_short_name_mode: "disabled".to_string(),
        image_pull_progress_timeout: std::time::Duration::ZERO,
        image_max_concurrent_downloads: 3,
        image_pull_retry_count: 0,
        image_registry_config_dir: PathBuf::new(),
        image_decryption_keys_path: PathBuf::new(),
        image_decryption_decoder_path: "ctd-decoder".to_string(),
        image_decryption_keyprovider_config: PathBuf::new(),
        image_additional_artifact_stores: Vec::new(),
        image_signature_policy: PathBuf::new(),
        image_signature_policy_dir: PathBuf::new(),
        image_storage_options: Vec::new(),
        image_volumes: "mkdir".to_string(),
        image_pinned_images: Vec::new(),
        image_big_files_temporary_dir: PathBuf::new(),
        image_oci_artifact_mount_support: true,
        workloads: HashMap::new(),
        enable_pod_events: true,
        included_pod_metrics: vec!["all".to_string()],
        stats_collection_period: 0,
        pod_sandbox_metrics_collection_period: 0,
        grpc_max_send_msg_size: 80 * 1024 * 1024,
        grpc_max_recv_msg_size: 80 * 1024 * 1024,
        metrics_enable: false,
        metrics_host: "127.0.0.1".to_string(),
        metrics_port: 9090,
        metrics_socket_path: PathBuf::new(),
        metrics_enable_tls: false,
        metrics_tls_cert_file: PathBuf::new(),
        metrics_tls_key_file: PathBuf::new(),
        metrics_tls_ca_file: PathBuf::new(),
        metrics_tls_min_version: "VersionTLS12".to_string(),
        metrics_tls_cipher_suites: Vec::new(),
        metrics_collectors: vec![
            "runtime".to_string(),
            "resources".to_string(),
            "images".to_string(),
        ],
        tracing_enable: false,
        tracing_endpoint: String::new(),
        tracing_sampling_rate_per_million: 0,
        monitor_env: Vec::new(),
        monitor_cgroup: String::new(),
        default_env: Vec::new(),
        default_capabilities: vec![
            "CHOWN".to_string(),
            "DAC_OVERRIDE".to_string(),
            "FSETID".to_string(),
            "FOWNER".to_string(),
            "MKNOD".to_string(),
            "NET_RAW".to_string(),
            "SETGID".to_string(),
            "SETUID".to_string(),
            "SETFCAP".to_string(),
            "SETPCAP".to_string(),
            "NET_BIND_SERVICE".to_string(),
            "SYS_CHROOT".to_string(),
            "KILL".to_string(),
            "AUDIT_WRITE".to_string(),
        ],
        default_sysctls: HashMap::new(),
        default_ulimits: Vec::new(),
        allowed_devices: Vec::new(),
        additional_devices: Vec::new(),
        device_ownership_from_security_context: false,
        add_inheritable_capabilities: false,
        base_runtime_spec: None,
        default_mounts_file: PathBuf::new(),
        hooks_dir: Vec::new(),
        absent_mount_sources_to_reject: Vec::new(),
        disable_proc_mount: false,
        timezone: String::new(),
        attach_socket_dir: PathBuf::from("/tmp/crius-test-attach"),
        container_exits_dir: PathBuf::from("/tmp/crius-test-exits"),
        clean_shutdown_file: PathBuf::from("/tmp/crius-test-clean.shutdown"),
        container_stop_timeout: 30,
        version_file: PathBuf::from("/tmp/crius-test-version"),
        version_file_persist: PathBuf::from("/tmp/crius-test-version-persist"),
        criu_path: PathBuf::new(),
        criu_image_path: PathBuf::new(),
        criu_work_path: PathBuf::new(),
        enable_criu_support: true,
        internal_wipe: true,
        internal_repair: true,
        bind_mount_prefix: PathBuf::new(),
        disable_cgroup: false,
        tolerate_missing_hugetlb_controller: true,
        separate_pull_cgroup: String::new(),
        seccomp_profile: PathBuf::new(),
        privileged_seccomp_profile: "unconfined".to_string(),
        unset_seccomp_profile: "runtime/default".to_string(),
        apparmor_default_profile: "crius-default".to_string(),
        disable_apparmor: false,
        enable_selinux: false,
        selinux_category_range: 1024,
        hostnetwork_disable_selinux: true,
        uid_mappings: None,
        gid_mappings: None,
        minimum_mappable_uid: -1,
        minimum_mappable_gid: -1,
        io_uid: 0,
        io_gid: 0,
        pids_limit: -1,
        infra_ctr_cpuset: String::new(),
        shared_cpuset: String::new(),
        exec_cpu_affinity: String::new(),
        irqbalance_config_file: PathBuf::new(),
        irqbalance_config_restore_file: "disable".to_string(),
        read_only: false,
        no_pivot: false,
        no_new_keyring: false,
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        pause_command: "/pause".to_string(),
        drop_infra_ctr: false,
        cni_config: test_cni_config(),
        cgroup_driver: None,
        exec_sync_io_drain_timeout: Duration::ZERO,
        max_container_log_line_size: 4096,
        log_to_journald: false,
        no_sync_log: false,
        restrict_oom_score_adj: false,
        enable_unprivileged_ports: false,
        enable_unprivileged_icmp: false,
        rootless: disabled_rootless(),
        shim: ShimConfig {
            shim_path: PathBuf::from("/definitely/missing/crius-shim"),
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: test_base.join("shims"),
            attach_socket_dir: test_base.join("attach"),
            container_exits_dir: test_base.join("exits"),
            io_uid: 0,
            io_gid: 0,
            monitor_env: Vec::new(),
            debug: false,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
            runtime_path: PathBuf::from("/definitely/missing/runc"),
            max_container_log_line_size: 4096,
            state_db_path: test_base.join("crius-test.db"),
        },
        streaming: crate::streaming::StreamingConfig::default(),
        config_path: None,
    }
}

pub(super) fn test_service() -> RuntimeServiceImpl {
    RuntimeServiceImpl::new(test_runtime_config(tempdir().unwrap().keep()))
}

fn test_service_with_env_cni() -> RuntimeServiceImpl {
    let mut config = test_runtime_config(tempdir().unwrap().keep());
    config.cni_config = crate::network::CniConfig::from_env();
    RuntimeServiceImpl::new(config)
}

fn test_runtime_container_config_with_mounts(mounts: Vec<MountConfig>) -> ContainerConfig {
    ContainerConfig {
        name: "test".to_string(),
        image: "busybox:latest".to_string(),
        command: vec!["sleep".to_string()],
        args: vec!["1".to_string()],
        env: Vec::new(),
        working_dir: None,
        mounts,
        labels: Vec::new(),
        annotations: Vec::new(),
        cdi_devices: Vec::new(),
        privileged: false,
        user: None,
        run_as_group: None,
        supplemental_groups: Vec::new(),
        hostname: None,
        tty: false,
        stdin: false,
        stdin_once: false,
        log_path: None,
        readonly_rootfs: false,
        seccomp_notifier: None,
        pids_limit: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        seccomp_profile: None,
        capabilities: None,
        cgroup_parent: None,
        sysctls: HashMap::new(),
        namespace_options: None,
        namespace_paths: NamespacePaths::default(),
        linux_resources: None,
        devices: Vec::new(),
        masked_paths: Vec::new(),
        readonly_paths: Vec::new(),
        rootfs: PathBuf::from("/tmp/crius-test-rootfs"),
    }
}

#[test]
fn cgroup_support_flags_detect_v1_and_v2_layouts() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    fs::create_dir_all(root.join("memory")).unwrap();
    fs::create_dir_all(root.join("cpu")).unwrap();
    fs::create_dir_all(root.join("hugetlb")).unwrap();
    fs::write(root.join("memory").join("memory.memsw.limit_in_bytes"), "0").unwrap();
    fs::write(root.join("memory").join("memory.kmem.limit_in_bytes"), "0").unwrap();
    fs::write(
        root.join("memory").join("memory.kmem.tcp.limit_in_bytes"),
        "0",
    )
    .unwrap();
    fs::write(root.join("memory").join("memory.swappiness"), "60").unwrap();
    fs::write(root.join("memory").join("memory.oom_control"), "0").unwrap();
    fs::write(root.join("memory").join("memory.use_hierarchy"), "1").unwrap();
    fs::write(root.join("cpu").join("cpu.rt_runtime_us"), "0").unwrap();
    fs::write(root.join("cpu").join("cpu.rt_period_us"), "0").unwrap();
    assert_eq!(
        RuntimeServiceImpl::cgroup_support_flags_for_root(root),
        CgroupResourceSupport {
            swap: true,
            hugetlb: true,
            memory_kernel: true,
            memory_kernel_tcp: true,
            memory_swappiness: true,
            memory_disable_oom_killer: true,
            memory_use_hierarchy: true,
            cpu_realtime: true,
            blockio: true,
            rdt: true,
        }
    );

    fs::remove_dir_all(root.join("memory")).unwrap();
    fs::remove_dir_all(root.join("cpu")).unwrap();
    fs::remove_dir_all(root.join("hugetlb")).unwrap();
    fs::write(root.join("cgroup.controllers"), "cpu memory").unwrap();
    fs::write(root.join("memory.swap.max"), "max").unwrap();
    fs::write(root.join("hugetlb.2MB.max"), "max").unwrap();
    assert_eq!(
        RuntimeServiceImpl::cgroup_support_flags_for_root(root),
        CgroupResourceSupport {
            swap: true,
            hugetlb: true,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: true,
            rdt: true,
        }
    );
}

#[test]
fn stored_linux_resources_to_nri_omits_cleared_swap_and_hugepages() {
    let mut resources = StoredLinuxResources {
        memory_swap_limit_in_bytes: 8192,
        hugepage_limits: vec![StoredHugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
        }],
        ..Default::default()
    };
    RuntimeServiceImpl::sanitize_stored_runtime_resources_with_flags(
        &mut resources,
        CgroupResourceSupport {
            swap: false,
            hugetlb: false,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: true,
            rdt: true,
        },
    );

    let nri = resources.to_nri();
    assert!(nri
        .memory
        .as_ref()
        .and_then(|memory| memory.swap.as_ref())
        .is_none());
    assert!(nri.hugepage_limits.is_empty());
}

#[test]
fn sanitize_spec_runtime_resources_clears_unsupported_extended_resources() {
    let mut spec = crate::oci::spec::Spec {
        oci_version: "1.0.2".to_string(),
        process: None,
        root: None,
        hostname: None,
        mounts: None,
        hooks: None,
        linux: Some(crate::oci::spec::Linux {
            namespaces: None,
            uid_mappings: None,
            gid_mappings: None,
            devices: None,
            net_devices: None,
            cgroups_path: None,
            resources: Some(crate::oci::spec::LinuxResources {
                memory: Some(crate::oci::spec::LinuxMemory {
                    limit: None,
                    swap: Some(8192),
                    kernel: Some(4096),
                    kernel_tcp: Some(2048),
                    reservation: None,
                    swappiness: Some(60),
                    disable_oom_killer: Some(true),
                    use_hierarchy: Some(true),
                }),
                network: None,
                pids: None,
                cpu: Some(crate::oci::spec::LinuxCpu {
                    shares: None,
                    quota: None,
                    period: None,
                    realtime_runtime: Some(1000),
                    realtime_period: Some(2000),
                    cpus: None,
                    mems: None,
                }),
                block_io: None,
                hugepage_limits: Some(vec![crate::oci::spec::LinuxHugepageLimit {
                    page_size: "2MB".to_string(),
                    limit: 1,
                }]),
                devices: None,
                intel_rdt: None,
                unified: None,
            }),
            rootfs_propagation: None,
            seccomp: None,
            sysctl: None,
            mount_label: None,
            masked_paths: None,
            readonly_paths: None,
            intel_rdt: None,
        }),
        annotations: None,
    };

    RuntimeServiceImpl::sanitize_spec_runtime_resources_with_flags(
        &mut spec,
        CgroupResourceSupport {
            swap: false,
            hugetlb: false,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: false,
            rdt: false,
        },
    );

    let resources = spec
        .linux
        .as_ref()
        .and_then(|linux| linux.resources.as_ref())
        .unwrap();
    let memory = resources.memory.as_ref().unwrap();
    assert!(memory.swap.is_none());
    assert!(memory.kernel.is_none());
    assert!(memory.kernel_tcp.is_none());
    assert!(memory.swappiness.is_none());
    assert!(memory.disable_oom_killer.is_none());
    assert!(memory.use_hierarchy.is_none());
    let cpu = resources.cpu.as_ref().unwrap();
    assert!(cpu.realtime_runtime.is_none());
    assert!(cpu.realtime_period.is_none());
    assert!(resources.hugepage_limits.is_none());
}

#[test]
fn validate_proto_hugetlb_limits_rejects_when_controller_missing_and_tolerance_disabled() {
    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        hugepage_limits: vec![crate::proto::runtime::v1::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
        }],
        ..Default::default()
    };

    let err = RuntimeServiceImpl::validate_proto_hugetlb_limits_with_flags(
        Some(&resources),
        CgroupResourceSupport {
            swap: true,
            hugetlb: false,
            memory_kernel: true,
            memory_kernel_tcp: true,
            memory_swappiness: true,
            memory_disable_oom_killer: true,
            memory_use_hierarchy: true,
            cpu_realtime: true,
            blockio: true,
            rdt: true,
        },
        false,
        "container create",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("hugetlb controller is missing"));
}

#[test]
fn sanitize_proto_runtime_resources_clears_hugepages_when_tolerated() {
    let mut resources = crate::proto::runtime::v1::LinuxContainerResources {
        hugepage_limits: vec![crate::proto::runtime::v1::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
        }],
        ..Default::default()
    };

    RuntimeServiceImpl::sanitize_proto_runtime_resources_with_flags(
        &mut resources,
        CgroupResourceSupport {
            swap: true,
            hugetlb: false,
            memory_kernel: true,
            memory_kernel_tcp: true,
            memory_swappiness: true,
            memory_disable_oom_killer: true,
            memory_use_hierarchy: true,
            cpu_realtime: true,
            blockio: true,
            rdt: true,
        },
        true,
    );

    assert!(resources.hugepage_limits.is_empty());
}

#[derive(Default)]
struct FakeNri {
    calls: tokio::sync::Mutex<Vec<&'static str>>,
    post_start_events: tokio::sync::Mutex<Vec<NriContainerEvent>>,
    post_update_events: tokio::sync::Mutex<Vec<NriContainerEvent>>,
    stop_events: tokio::sync::Mutex<Vec<NriContainerEvent>>,
    stop_pod_events: tokio::sync::Mutex<Vec<NriPodEvent>>,
    fail_run_pod_sandbox: bool,
    fail_post_update_pod_sandbox: bool,
    fail_post_create_container: bool,
    fail_post_start_container: bool,
    fail_post_update_container: bool,
    fail_stop_container: bool,
    fail_remove_container: bool,
    create_result: tokio::sync::Mutex<Option<NriCreateContainerResult>>,
    update_result: tokio::sync::Mutex<Option<NriUpdateContainerResult>>,
    stop_result: tokio::sync::Mutex<Option<NriStopContainerResult>>,
}

#[async_trait::async_trait]
impl NriApi for FakeNri {
    async fn start(&self) -> crate::nri::Result<()> {
        self.calls.lock().await.push("start");
        Ok(())
    }

    async fn shutdown(&self) -> crate::nri::Result<()> {
        self.calls.lock().await.push("shutdown");
        Ok(())
    }

    async fn synchronize(&self) -> crate::nri::Result<()> {
        self.calls.lock().await.push("synchronize");
        Ok(())
    }

    async fn run_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("run_pod");
        if self.fail_run_pod_sandbox {
            return Err(crate::nri::NriError::Plugin(
                "forced pod run failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn stop_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("stop_pod");
        self.stop_pod_events.lock().await.push(_event);
        Ok(())
    }

    async fn remove_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("remove_pod");
        Ok(())
    }

    async fn update_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("update_pod");
        Ok(())
    }

    async fn post_update_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_update_pod");
        if self.fail_post_update_pod_sandbox {
            return Err(crate::nri::NriError::Plugin(
                "forced post update pod failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn create_container(
        &self,
        _event: NriContainerEvent,
    ) -> crate::nri::Result<NriCreateContainerResult> {
        self.calls.lock().await.push("create");
        Ok(self.create_result.lock().await.clone().unwrap_or_default())
    }

    async fn post_create_container(&self, _event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_create");
        if self.fail_post_create_container {
            return Err(crate::nri::NriError::Plugin(
                "forced post create failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn start_container(&self, _event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("start_container");
        Ok(())
    }

    async fn post_start_container(&self, event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_start_container");
        self.post_start_events.lock().await.push(event);
        if self.fail_post_start_container {
            return Err(crate::nri::NriError::Plugin(
                "forced post start failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn update_container(
        &self,
        event: NriContainerEvent,
    ) -> crate::nri::Result<NriUpdateContainerResult> {
        self.calls.lock().await.push("update_container");
        Ok(self
            .update_result
            .lock()
            .await
            .clone()
            .unwrap_or(NriUpdateContainerResult {
                linux_resources: event.linux_resources,
                ..Default::default()
            }))
    }

    async fn post_update_container(&self, event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_update_container");
        self.post_update_events.lock().await.push(event);
        if self.fail_post_update_container {
            return Err(crate::nri::NriError::Plugin(
                "forced post update failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn stop_container(
        &self,
        event: NriContainerEvent,
    ) -> crate::nri::Result<NriStopContainerResult> {
        self.calls.lock().await.push("stop_container");
        self.stop_events.lock().await.push(event);
        if self.fail_stop_container {
            return Err(crate::nri::NriError::Plugin(
                "forced stop failure".to_string(),
            ));
        }
        Ok(self.stop_result.lock().await.clone().unwrap_or_default())
    }

    async fn remove_container(&self, _event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("remove_container");
        if self.fail_remove_container {
            return Err(crate::nri::NriError::Plugin(
                "forced remove failure".to_string(),
            ));
        }
        Ok(())
    }
}

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn write_fake_runtime_script(dir: &Path) -> PathBuf {
    let state_dir = dir.join("runtime-state");
    fs::create_dir_all(&state_dir).unwrap();

    let script_path = dir.join("fake-runc.sh");
    let script = format!(
        r#"#!/bin/bash
set -eu
STATE_DIR="{state_dir}"
cmd="${{1:-}}"
if [ "$#" -gt 0 ]; then
  shift
fi
case "$cmd" in
  state)
    id="${{1:-}}"
    file="$STATE_DIR/$id.state"
    if [ ! -f "$file" ]; then
      exit 1
    fi
    status="$(cat "$file")"
    pid=0
    if [ -f "$STATE_DIR/$id.pid" ]; then
      pid="$(cat "$STATE_DIR/$id.pid")"
    fi
    printf '{{"ociVersion":"1.0.2","id":"%s","status":"%s","pid":%s,"bundle":"%s","rootfs":"%s","created":"2024-01-01T00:00:00Z","owner":"root"}}\n' "$id" "$status" "$pid" "$STATE_DIR/bundle" "$STATE_DIR/rootfs"
    ;;
  kill)
    id="${{1:-}}"
    echo stopped > "$STATE_DIR/$id.state"
    ;;
  delete)
    id="${{1:-}}"
    if [ -f "$STATE_DIR/$id.state" ]; then
      rm -f "$STATE_DIR/$id.state" "$STATE_DIR/$id.pid"
      exit 0
    fi
    echo "container does not exist" >&2
    exit 1
    ;;
  start)
    id="${{1:-}}"
    echo running > "$STATE_DIR/$id.state"
    echo $$ > "$STATE_DIR/$id.pid"
    ;;
  exec)
    id="${{1:-}}"
    if [ "$#" -gt 0 ]; then
      shift
    fi
    if [ "$#" -eq 0 ]; then
      exit 0
    fi
    exec "$@"
    ;;
  pause)
    id="${{1:-}}"
    if [ -n "$id" ]; then
      echo paused > "$STATE_DIR/$id.state"
    fi
    ;;
  resume)
    id="${{1:-}}"
    if [ -n "$id" ]; then
      echo running > "$STATE_DIR/$id.state"
    fi
    ;;
  update)
    resource_file=""
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --resources)
          resource_file="${{2:-}}"
          shift 2
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$resource_file" ] && [ -n "$id" ]; then
      cp "$resource_file" "$STATE_DIR/$id.update.json"
    fi
    ;;
  checkpoint)
    image_path=""
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --file-locks)
          shift
          ;;
        --image-path|--work-path)
          if [ "$1" = "--image-path" ]; then
            image_path="${{2:-}}"
          fi
          shift 2
          ;;
        --leave-running)
          shift
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$image_path" ] && [ -n "$id" ]; then
      mkdir -p "$image_path"
      printf '{{"id":"%s","checkpointed":true}}\n' "$id" > "$image_path/checkpoint.json"
    fi
    ;;
  restore)
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -d|--image-path|--work-path|--bundle|--no-pivot)
          if [ "$1" = "-d" ] || [ "$1" = "--no-pivot" ]; then
            shift
          else
            shift 2
          fi
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$id" ]; then
      echo running > "$STATE_DIR/$id.state"
      echo $$ > "$STATE_DIR/$id.pid"
    fi
    ;;
esac
exit 0
"#,
        state_dir = state_dir.display()
    );

    fs::write(&script_path, script).unwrap();
    let mut perms = fs::metadata(&script_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script_path, perms).unwrap();
    script_path
}

fn write_fake_runtime_script_with_start_state(dir: &Path, start_state: &str) -> PathBuf {
    let base_runtime = write_fake_runtime_script(dir);
    let state_dir = dir.join("runtime-state");
    let script_path = dir.join(format!("fake-runc-start-{}.sh", start_state));
    let script = format!(
        r#"#!/bin/bash
set -eu
BASE_RUNTIME="{base_runtime}"
STATE_DIR="{state_dir}"
cmd="${{1:-}}"
if [ "$#" -gt 0 ]; then
  shift
fi
case "$cmd" in
  start|run)
    id="${{1:-}}"
    if [ "$cmd" = "run" ]; then
      id=""
      while [ "$#" -gt 0 ]; do
        id="$1"
        shift
      done
    fi
    echo "{start_state}" > "$STATE_DIR/$id.state"
    if [ "{start_state}" = "running" ]; then
      echo $$ > "$STATE_DIR/$id.pid"
    else
      rm -f "$STATE_DIR/$id.pid"
    fi
    ;;
  *)
    exec "$BASE_RUNTIME" "$cmd" "$@"
    ;;
esac
exit 0
"#,
        base_runtime = base_runtime.display(),
        state_dir = state_dir.display(),
        start_state = start_state,
    );
    fs::write(&script_path, script).unwrap();
    let mut perms = fs::metadata(&script_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script_path, perms).unwrap();
    script_path
}

fn write_quick_exit_shim(dir: &Path) -> PathBuf {
    let shim_path = dir.join("quick-exit-shim.sh");
    fs::write(
        &shim_path,
        r#"#!/bin/sh
set -eu
exit 0
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&shim_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&shim_path, perms).unwrap();
    shim_path
}

fn write_test_shim_binary(dir: &Path) -> PathBuf {
    let shim_path = dir.join("fake-crius-shim.py");
    fs::write(
        &shim_path,
        r#"#!/usr/bin/env python3
import json
import os
import signal
import socket
import subprocess
import sys
import time

args = sys.argv[1:]
config = {}
idx = 0
while idx < len(args):
    key = args[idx]
    if key.startswith("--") and idx + 1 < len(args):
        config[key[2:].replace("-", "_")] = args[idx + 1]
        idx += 2
    else:
        idx += 1

container_id = config.get("id", "")
work_dir = config.get("work_dir", "")
runtime_path = config.get("runtime", "")
exit_code_file = config.get("exit_code_file", "")
attach_socket_dir = config.get("attach_socket_dir", "")
log_file = config.get("log", "")
state_dir = os.path.join(os.path.dirname(runtime_path), "runtime-state")
task_socket = os.path.join(work_dir, container_id, "task.sock")
running = True

def ensure_parent(path):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def write_file(path, value):
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(value)

def remove_file(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

def state_path():
    return os.path.join(state_dir, f"{container_id}.state")

def pid_path():
    return os.path.join(state_dir, f"{container_id}.pid")

def update_path():
    return os.path.join(state_dir, f"{container_id}.update.json")

def read_state():
    try:
        with open(state_path(), encoding="utf-8") as handle:
            return handle.read().strip()
    except FileNotFoundError:
        return "created"

def write_state(state):
    write_file(state_path(), state + "\n")
    if state == "running":
        write_file(pid_path(), str(os.getpid()) + "\n")
    elif state in ("stopped", "deleted"):
        remove_file(pid_path())

def exit_code():
    try:
        with open(exit_code_file, encoding="utf-8") as handle:
            return int(handle.read().strip())
    except Exception:
        return None

def task_state():
    state = read_state()
    if state == "running":
        return "running"
    if state == "paused":
        return "paused"
    if state == "stopped":
        return "stopped"
    if state == "deleted":
        return "deleted"
    return "created"

def task_pid():
    try:
        with open(pid_path(), encoding="utf-8") as handle:
            return int(handle.read().strip())
    except Exception:
        return os.getpid() if task_state() in ("running", "paused") else None

def empty():
    return {"kind": "empty"}

def success(payload):
    return {"ok": True, "payload": payload, "error": None}

def failure(message):
    return {"ok": False, "payload": None, "error": message}

def handle(request):
    method = request.get("method")
    payload = request.get("payload") or {}
    if method == "ping":
        return empty()
    if method == "create_task":
        write_state("created")
        return empty()
    if method == "start_task":
        write_state("running")
        return empty()
    if method == "kill_task":
        write_state("stopped")
        if exit_code_file:
            write_file(exit_code_file, "0\n")
        return empty()
    if method == "delete_task":
        write_state("deleted")
        remove_file(state_path())
        remove_file(pid_path())
        return empty()
    if method == "wait_process":
        return {"kind": "wait_process", "payload": {"exit_code": exit_code()}}
    if method == "status":
        code = exit_code()
        if code is None and task_state() == "stopped":
            code = 0
        return {
            "kind": "status",
            "payload": {
                "state": task_state(),
                "pid": task_pid(),
                "exit_code": code,
            },
        }
    if method == "container_pid":
        return {"kind": "container_pid", "payload": task_pid()}
    if method == "pause_task":
        write_state("paused")
        return empty()
    if method == "resume_task":
        write_state("running")
        return empty()
    if method == "checkpoint_task":
        image_path = payload.get("image_path")
        if image_path:
            os.makedirs(image_path, exist_ok=True)
            write_file(
                os.path.join(image_path, "checkpoint.json"),
                json.dumps({"id": container_id, "checkpointed": True}) + "\n",
            )
        return empty()
    if method == "restore_task":
        work_path = payload.get("work_path")
        if work_path:
            os.makedirs(work_path, exist_ok=True)
        write_state("running")
        return empty()
    if method == "update_resources":
        resources = payload.get("resources") or {}
        update = {
            "cpu": {
                "shares": resources.get("cpu_shares", 0),
                "quota": resources.get("cpu_quota", 0),
                "period": resources.get("cpu_period", 0),
                "cpus": resources.get("cpuset_cpus", ""),
                "mems": resources.get("cpuset_mems", ""),
            },
            "memory": {
                "limit": resources.get("memory_limit_in_bytes", 0),
                "swap": resources.get("memory_swap_limit_in_bytes", 0),
            },
        }
        write_file(update_path(), json.dumps(update) + "\n")
        return empty()
    if method == "reopen_log":
        return empty()
    if method == "resize_pty":
        return empty()
    if method == "exec_process":
        command = payload.get("command") or []
        capture = bool(payload.get("capture_output"))
        if command:
            timeout = payload.get("timeout_ms") or payload.get("io_drain_timeout_ms")
            completed = subprocess.run(
                command,
                capture_output=capture,
                check=False,
                timeout=(timeout / 1000.0) if timeout else None,
            )
            return {
                "kind": "exec_process",
                "payload": {
                    "exit_code": completed.returncode,
                    "stdout": list(completed.stdout or b""),
                    "stderr": list(completed.stderr or b""),
                },
            }
        return {"kind": "exec_process", "payload": {"exit_code": 0, "stdout": [], "stderr": []}}
    if method == "open_exec_session":
        io_socket = os.path.join(work_dir, container_id, "exec.sock")
        resize_socket = os.path.join(work_dir, container_id, "exec-resize.sock")
        ensure_parent(io_socket)
        open(io_socket, "a", encoding="utf-8").close()
        open(resize_socket, "a", encoding="utf-8").close()
        return {
            "kind": "open_exec_session",
            "payload": {
                "session_id": "test-session",
                "io_socket_path": io_socket,
                "resize_socket_path": resize_socket,
            },
        }
    raise RuntimeError(f"unsupported fake shim RPC method: {method}")

def stop(_signum, _frame):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

os.makedirs(state_dir, exist_ok=True)
ensure_parent(task_socket)
if attach_socket_dir:
    attach_dir = os.path.join(attach_socket_dir, container_id)
    os.makedirs(attach_dir, exist_ok=True)
    open(os.path.join(attach_dir, "attach.sock"), "a", encoding="utf-8").close()
    open(os.path.join(attach_dir, "resize.sock"), "a", encoding="utf-8").close()
if log_file:
    write_file(log_file, "")
remove_file(task_socket)

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(task_socket)
server.listen(8)
server.settimeout(0.1)

try:
    while running:
        try:
            conn, _ = server.accept()
        except socket.timeout:
            continue
        with conn:
            chunks = []
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            try:
                envelope = json.loads(b"".join(chunks).decode("utf-8"))
                response = success(handle(envelope.get("payload") or {}))
            except Exception as err:
                response = failure(str(err))
            conn.sendall(json.dumps(response).encode("utf-8"))
finally:
    server.close()
    remove_file(task_socket)
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&shim_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&shim_path, perms).unwrap();
    shim_path
}

fn test_service_with_fake_runtime() -> (TempDir, RuntimeServiceImpl) {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    test_service_with_runtime_path_and_shim_path_and_configure(dir, runtime_path, None, |_| {})
}

fn test_service_with_fake_runtime_configure<F>(configure: F) -> (TempDir, RuntimeServiceImpl)
where
    F: FnOnce(&mut RuntimeConfig),
{
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    test_service_with_runtime_path_and_shim_path_and_configure(dir, runtime_path, None, configure)
}

fn test_service_with_runtime_path_and_shim_path(
    dir: TempDir,
    runtime_path: PathBuf,
    shim_path_override: Option<PathBuf>,
) -> (TempDir, RuntimeServiceImpl) {
    test_service_with_runtime_path_and_shim_path_and_configure(
        dir,
        runtime_path,
        shim_path_override,
        |_| {},
    )
}

fn test_service_with_runtime_path_and_shim_path_and_configure<F>(
    dir: TempDir,
    runtime_path: PathBuf,
    shim_path_override: Option<PathBuf>,
    configure: F,
) -> (TempDir, RuntimeServiceImpl)
where
    F: FnOnce(&mut RuntimeConfig),
{
    test_service_with_runtime_path_and_shim_path_and_configure_all(
        dir,
        runtime_path,
        shim_path_override,
        |config, _nri_config| configure(config),
    )
}

fn test_service_with_runtime_path_and_shim_path_and_configure_all<F>(
    dir: TempDir,
    runtime_path: PathBuf,
    shim_path_override: Option<PathBuf>,
    configure: F,
) -> (TempDir, RuntimeServiceImpl)
where
    F: FnOnce(&mut RuntimeConfig, &mut NriConfig),
{
    let default_test_shim_path = write_test_shim_binary(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let shim_runtime_path = runtime_path.clone();
    let mut config = RuntimeConfig {
        root_dir: dir.path().join("root"),
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string()],
        runtime_configs: HashMap::from([(
            "runc".to_string(),
            crate::config::ResolvedRuntimeHandlerConfig {
                backend: "runc".to_string(),
                    backend_options: HashMap::new(),
                runtime_path: runtime_path.display().to_string(),
                runtime_config_path: String::new(),
                runtime_root: dir.path().join("runtime-root").display().to_string(),
                platform_runtime_paths: HashMap::new(),
                monitor_path: default_test_shim_path.display().to_string(),
                monitor_cgroup: String::new(),
                monitor_env: Vec::new(),
                stream_websockets: false,
                allowed_annotations: Vec::new(),
                default_annotations: HashMap::new(),
                privileged_without_host_devices: false,
                privileged_without_host_devices_all_devices_allowed: false,
                container_create_timeout: 240,
                snapshotter: "internal-overlay-untar".to_string(),
            },
        )]),
        runtime_root: dir.path().join("runtime-root"),
        log_dir: dir.path().join("logs"),
        runtime_path,
        runtime_config_path: PathBuf::new(),
        image_root: dir.path().join("images"),
        image_driver: "overlay".to_string(),
        image_global_auth_file: PathBuf::new(),
        image_namespaced_auth_dir: PathBuf::new(),
        image_default_transport: "docker://".to_string(),
        image_short_name_mode: "disabled".to_string(),
        image_pull_progress_timeout: std::time::Duration::ZERO,
        image_max_concurrent_downloads: 3,
        image_pull_retry_count: 0,
        image_registry_config_dir: PathBuf::new(),
        image_decryption_keys_path: PathBuf::new(),
        image_decryption_decoder_path: "ctd-decoder".to_string(),
        image_decryption_keyprovider_config: PathBuf::new(),
        image_additional_artifact_stores: Vec::new(),
        image_signature_policy: PathBuf::new(),
        image_signature_policy_dir: PathBuf::new(),
        image_storage_options: Vec::new(),
        image_volumes: "mkdir".to_string(),
        image_pinned_images: Vec::new(),
        image_big_files_temporary_dir: PathBuf::new(),
        image_oci_artifact_mount_support: true,
        workloads: HashMap::new(),
        enable_pod_events: true,
        included_pod_metrics: vec!["all".to_string()],
        stats_collection_period: 0,
        pod_sandbox_metrics_collection_period: 0,
        grpc_max_send_msg_size: 80 * 1024 * 1024,
        grpc_max_recv_msg_size: 80 * 1024 * 1024,
        metrics_enable: false,
        metrics_host: "127.0.0.1".to_string(),
        metrics_port: 9090,
        metrics_socket_path: PathBuf::new(),
        metrics_enable_tls: false,
        metrics_tls_cert_file: PathBuf::new(),
        metrics_tls_key_file: PathBuf::new(),
        metrics_tls_ca_file: PathBuf::new(),
        metrics_tls_min_version: "VersionTLS12".to_string(),
        metrics_tls_cipher_suites: Vec::new(),
        metrics_collectors: vec![
            "runtime".to_string(),
            "resources".to_string(),
            "images".to_string(),
        ],
        tracing_enable: false,
        tracing_endpoint: String::new(),
        tracing_sampling_rate_per_million: 0,
        monitor_env: Vec::new(),
        monitor_cgroup: String::new(),
        default_env: Vec::new(),
        default_capabilities: vec![
            "CHOWN".to_string(),
            "DAC_OVERRIDE".to_string(),
            "FSETID".to_string(),
            "FOWNER".to_string(),
            "MKNOD".to_string(),
            "NET_RAW".to_string(),
            "SETGID".to_string(),
            "SETUID".to_string(),
            "SETFCAP".to_string(),
            "SETPCAP".to_string(),
            "NET_BIND_SERVICE".to_string(),
            "SYS_CHROOT".to_string(),
            "KILL".to_string(),
            "AUDIT_WRITE".to_string(),
        ],
        default_sysctls: HashMap::new(),
        default_ulimits: Vec::new(),
        allowed_devices: Vec::new(),
        additional_devices: Vec::new(),
        device_ownership_from_security_context: false,
        add_inheritable_capabilities: false,
        base_runtime_spec: None,
        default_mounts_file: PathBuf::new(),
        hooks_dir: Vec::new(),
        absent_mount_sources_to_reject: Vec::new(),
        disable_proc_mount: false,
        timezone: String::new(),
        attach_socket_dir: dir.path().join("attach"),
        container_exits_dir: dir.path().join("exits"),
        clean_shutdown_file: dir.path().join("clean.shutdown"),
        container_stop_timeout: 30,
        version_file: dir.path().join("version"),
        version_file_persist: dir.path().join("version-persist"),
        criu_path: PathBuf::new(),
        criu_image_path: PathBuf::new(),
        criu_work_path: PathBuf::new(),
        enable_criu_support: true,
        internal_wipe: true,
        internal_repair: true,
        bind_mount_prefix: PathBuf::new(),
        disable_cgroup: false,
        tolerate_missing_hugetlb_controller: true,
        separate_pull_cgroup: String::new(),
        seccomp_profile: PathBuf::new(),
        privileged_seccomp_profile: "unconfined".to_string(),
        unset_seccomp_profile: "runtime/default".to_string(),
        apparmor_default_profile: "crius-default".to_string(),
        disable_apparmor: false,
        enable_selinux: false,
        selinux_category_range: 1024,
        hostnetwork_disable_selinux: true,
        uid_mappings: None,
        gid_mappings: None,
        minimum_mappable_uid: -1,
        minimum_mappable_gid: -1,
        io_uid: 0,
        io_gid: 0,
        pids_limit: -1,
        infra_ctr_cpuset: String::new(),
        shared_cpuset: String::new(),
        exec_cpu_affinity: String::new(),
        irqbalance_config_file: PathBuf::new(),
        irqbalance_config_restore_file: "disable".to_string(),
        read_only: false,
        no_pivot: false,
        no_new_keyring: false,
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        pause_command: "/pause".to_string(),
        drop_infra_ctr: false,
        cni_config: test_cni_config(),
        cgroup_driver: None,
        exec_sync_io_drain_timeout: Duration::ZERO,
        max_container_log_line_size: 4096,
        log_to_journald: false,
        no_sync_log: false,
        restrict_oom_score_adj: false,
        enable_unprivileged_ports: false,
        enable_unprivileged_icmp: false,
        rootless: disabled_rootless(),
        shim: ShimConfig {
            shim_path: shim_path_override.unwrap_or(default_test_shim_path),
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: shim_work_dir.clone(),
            attach_socket_dir: dir.path().join("attach"),
            container_exits_dir: dir.path().join("exits"),
            io_uid: 0,
            io_gid: 0,
            monitor_env: Vec::new(),
            debug: false,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
            runtime_path: shim_runtime_path,
            max_container_log_line_size: 4096,
            state_db_path: dir.path().join("root").join("crius.db"),
        },
        streaming: crate::streaming::StreamingConfig::default(),
        config_path: None,
    };
    let mut nri_config = NriConfig {
        blockio_config_path: write_blockio_config(&dir).display().to_string(),
        ..Default::default()
    };
    configure(&mut config, &mut nri_config);
    let service = RuntimeServiceImpl::new_with_shim_work_dir(config, nri_config, shim_work_dir);
    (dir, service)
}

fn fake_runtime_state_path(dir: &TempDir, id: &str) -> PathBuf {
    dir.path()
        .join("runtime-state")
        .join(format!("{}.state", id))
}

fn fake_runtime_update_path(dir: &TempDir, id: &str) -> PathBuf {
    dir.path()
        .join("runtime-state")
        .join(format!("{}.update.json", id))
}

fn write_blockio_config(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("blockio.json");
    fs::write(
        &path,
        serde_json::json!({
            "classes": {
                "gold": {
                    "weight": 500
                }
            }
        })
        .to_string(),
    )
    .unwrap();
    path
}

fn test_service_with_fake_runtime_and_nri(
    fake_nri: Arc<dyn NriApi>,
) -> (TempDir, RuntimeServiceImpl) {
    test_service_with_fake_runtime_and_nri_and_shim(fake_nri, None)
}

fn test_service_with_fake_runtime_and_nri_and_shim(
    fake_nri: Arc<dyn NriApi>,
    shim_path_override: Option<PathBuf>,
) -> (TempDir, RuntimeServiceImpl) {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let default_test_shim_path = write_test_shim_binary(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let shim_runtime_path = runtime_path.clone();
    let config = RuntimeConfig {
        root_dir: dir.path().join("root"),
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string()],
        runtime_configs: HashMap::from([(
            "runc".to_string(),
            crate::config::ResolvedRuntimeHandlerConfig {
                backend: "runc".to_string(),
                    backend_options: HashMap::new(),
                runtime_path: runtime_path.display().to_string(),
                runtime_config_path: String::new(),
                runtime_root: dir.path().join("runtime-root").display().to_string(),
                platform_runtime_paths: HashMap::new(),
                monitor_path: default_test_shim_path.display().to_string(),
                monitor_cgroup: String::new(),
                monitor_env: Vec::new(),
                stream_websockets: false,
                allowed_annotations: Vec::new(),
                default_annotations: HashMap::new(),
                privileged_without_host_devices: false,
                privileged_without_host_devices_all_devices_allowed: false,
                container_create_timeout: 240,
                snapshotter: "internal-overlay-untar".to_string(),
            },
        )]),
        runtime_root: dir.path().join("runtime-root"),
        log_dir: dir.path().join("logs"),
        runtime_path,
        runtime_config_path: PathBuf::new(),
        image_root: dir.path().join("images"),
        image_driver: "overlay".to_string(),
        image_global_auth_file: PathBuf::new(),
        image_namespaced_auth_dir: PathBuf::new(),
        image_default_transport: "docker://".to_string(),
        image_short_name_mode: "disabled".to_string(),
        image_pull_progress_timeout: std::time::Duration::ZERO,
        image_max_concurrent_downloads: 3,
        image_pull_retry_count: 0,
        image_registry_config_dir: PathBuf::new(),
        image_decryption_keys_path: PathBuf::new(),
        image_decryption_decoder_path: "ctd-decoder".to_string(),
        image_decryption_keyprovider_config: PathBuf::new(),
        image_additional_artifact_stores: Vec::new(),
        image_signature_policy: PathBuf::new(),
        image_signature_policy_dir: PathBuf::new(),
        image_storage_options: Vec::new(),
        image_volumes: "mkdir".to_string(),
        image_pinned_images: Vec::new(),
        image_big_files_temporary_dir: PathBuf::new(),
        image_oci_artifact_mount_support: true,
        workloads: HashMap::new(),
        enable_pod_events: true,
        included_pod_metrics: vec!["all".to_string()],
        stats_collection_period: 0,
        pod_sandbox_metrics_collection_period: 0,
        grpc_max_send_msg_size: 80 * 1024 * 1024,
        grpc_max_recv_msg_size: 80 * 1024 * 1024,
        metrics_enable: false,
        metrics_host: "127.0.0.1".to_string(),
        metrics_port: 9090,
        metrics_socket_path: PathBuf::new(),
        metrics_enable_tls: false,
        metrics_tls_cert_file: PathBuf::new(),
        metrics_tls_key_file: PathBuf::new(),
        metrics_tls_ca_file: PathBuf::new(),
        metrics_tls_min_version: "VersionTLS12".to_string(),
        metrics_tls_cipher_suites: Vec::new(),
        metrics_collectors: vec![
            "runtime".to_string(),
            "resources".to_string(),
            "images".to_string(),
        ],
        tracing_enable: false,
        tracing_endpoint: String::new(),
        tracing_sampling_rate_per_million: 0,
        monitor_env: Vec::new(),
        monitor_cgroup: String::new(),
        default_env: Vec::new(),
        default_capabilities: vec![
            "CHOWN".to_string(),
            "DAC_OVERRIDE".to_string(),
            "FSETID".to_string(),
            "FOWNER".to_string(),
            "MKNOD".to_string(),
            "NET_RAW".to_string(),
            "SETGID".to_string(),
            "SETUID".to_string(),
            "SETFCAP".to_string(),
            "SETPCAP".to_string(),
            "NET_BIND_SERVICE".to_string(),
            "SYS_CHROOT".to_string(),
            "KILL".to_string(),
            "AUDIT_WRITE".to_string(),
        ],
        default_sysctls: HashMap::new(),
        default_ulimits: Vec::new(),
        allowed_devices: Vec::new(),
        additional_devices: Vec::new(),
        device_ownership_from_security_context: false,
        add_inheritable_capabilities: false,
        base_runtime_spec: None,
        default_mounts_file: PathBuf::new(),
        hooks_dir: Vec::new(),
        absent_mount_sources_to_reject: Vec::new(),
        disable_proc_mount: false,
        timezone: String::new(),
        attach_socket_dir: dir.path().join("attach"),
        container_exits_dir: dir.path().join("exits"),
        clean_shutdown_file: dir.path().join("clean.shutdown"),
        container_stop_timeout: 30,
        version_file: dir.path().join("version"),
        version_file_persist: dir.path().join("version-persist"),
        criu_path: PathBuf::new(),
        criu_image_path: PathBuf::new(),
        criu_work_path: PathBuf::new(),
        enable_criu_support: true,
        internal_wipe: true,
        internal_repair: true,
        bind_mount_prefix: PathBuf::new(),
        disable_cgroup: false,
        tolerate_missing_hugetlb_controller: true,
        separate_pull_cgroup: String::new(),
        seccomp_profile: PathBuf::new(),
        privileged_seccomp_profile: "unconfined".to_string(),
        unset_seccomp_profile: "runtime/default".to_string(),
        apparmor_default_profile: "crius-default".to_string(),
        disable_apparmor: false,
        enable_selinux: false,
        selinux_category_range: 1024,
        hostnetwork_disable_selinux: true,
        uid_mappings: None,
        gid_mappings: None,
        minimum_mappable_uid: -1,
        minimum_mappable_gid: -1,
        io_uid: 0,
        io_gid: 0,
        pids_limit: -1,
        infra_ctr_cpuset: String::new(),
        shared_cpuset: String::new(),
        exec_cpu_affinity: String::new(),
        irqbalance_config_file: PathBuf::new(),
        irqbalance_config_restore_file: "disable".to_string(),
        read_only: false,
        no_pivot: false,
        no_new_keyring: false,
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        pause_command: "/pause".to_string(),
        drop_infra_ctr: false,
        cni_config: test_cni_config(),
        cgroup_driver: None,
        exec_sync_io_drain_timeout: Duration::ZERO,
        max_container_log_line_size: 4096,
        log_to_journald: false,
        no_sync_log: false,
        restrict_oom_score_adj: false,
        enable_unprivileged_ports: false,
        enable_unprivileged_icmp: false,
        rootless: disabled_rootless(),
        shim: ShimConfig {
            shim_path: shim_path_override.unwrap_or(default_test_shim_path),
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: shim_work_dir.clone(),
            attach_socket_dir: dir.path().join("attach"),
            container_exits_dir: dir.path().join("exits"),
            io_uid: 0,
            io_gid: 0,
            monitor_env: Vec::new(),
            debug: false,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
            runtime_path: shim_runtime_path,
            max_container_log_line_size: 4096,
            state_db_path: dir.path().join("root").join("crius.db"),
        },
        streaming: crate::streaming::StreamingConfig::default(),
        config_path: None,
    };
    let nri_config = NriConfig {
        enable: true,
        blockio_config_path: write_blockio_config(&dir).display().to_string(),
        ..Default::default()
    };
    let service = RuntimeServiceImpl::new_with_shim_work_dir_and_nri(
        config,
        nri_config,
        shim_work_dir,
        Some(fake_nri),
    );
    (dir, service)
}
