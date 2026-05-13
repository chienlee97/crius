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

fn disabled_rootless() -> crate::rootless::EffectiveRootlessConfig {
    crate::rootless::EffectiveRootlessConfig::disabled()
}

fn test_runtime_config(root_dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        root_dir,
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string(), "kata".to_string()],
        runtime_configs: HashMap::from([
            (
                "runc".to_string(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
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
        cni_config: crate::network::CniConfig::default(),
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
            work_dir: PathBuf::from("/tmp/crius-test-shims"),
            attach_socket_dir: PathBuf::from("/tmp/crius-test-attach"),
            container_exits_dir: PathBuf::from("/tmp/crius-test-exits"),
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
            state_db_path: PathBuf::from("/tmp/crius-test.db"),
        },
        streaming: crate::streaming::StreamingConfig::default(),
        config_path: None,
    }
}

fn test_service() -> RuntimeServiceImpl {
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
    let shim_path = dir.join("fake-crius-shim.sh");
    fs::write(
        &shim_path,
        r#"#!/bin/sh
set -eu
exit_code_file=""
log_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    --log)
      log_file="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -n "$exit_code_file" ]; then
  shim_dir="$(dirname "$exit_code_file")"
  mkdir -p "$shim_dir"
  : > "$shim_dir/attach.sock"
  : > "$shim_dir/resize.sock"
fi
if [ -n "$log_file" ]; then
  mkdir -p "$(dirname "$log_file")"
  : > "$log_file"
fi
trap 'exit 0' TERM INT
while :; do
  sleep 1
done
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
        cni_config: crate::network::CniConfig::default(),
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
    configure(&mut config);
    let nri_config = NriConfig {
        blockio_config_path: write_blockio_config(&dir).display().to_string(),
        ..Default::default()
    };
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
        cni_config: crate::network::CniConfig::default(),
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

#[tokio::test]
async fn initialize_nri_starts_then_synchronizes_once() {
    let fake_nri = Arc::new(FakeNri::default());
    let nri_config = NriConfig {
        enable: true,
        ..Default::default()
    };
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(tempdir().unwrap().keep()),
        nri_config,
        fake_nri.clone(),
    );

    service
        .initialize_nri()
        .await
        .expect("initialize_nri should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start", "synchronize"]
    );
}

#[tokio::test]
async fn initialize_nri_skips_disabled_configuration() {
    let fake_nri = Arc::new(FakeNri::default());
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(tempdir().unwrap().keep()),
        NriConfig::default(),
        fake_nri.clone(),
    );

    service
        .initialize_nri()
        .await
        .expect("disabled NRI should be a no-op");

    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn rollback_failed_pod_sandbox_run_undoes_nri_and_local_state() {
    let fake_nri = Arc::new(FakeNri {
        fail_run_pod_sandbox: true,
        ..Default::default()
    });
    let nri_config = NriConfig {
        enable: true,
        ..Default::default()
    };
    let root_dir = tempdir().unwrap().keep();
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(root_dir.clone()),
        nri_config,
        fake_nri.clone(),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-rollback".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-rollback".to_string(),
        test_pod("pod-rollback", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-rollback",
            "ready",
            "pod",
            "default",
            "uid",
            "/tmp/netns-rollback",
            &HashMap::new(),
            &annotations,
            Some("pause-rollback"),
            None,
        )
        .unwrap();

    service
        .rollback_failed_pod_sandbox_run("pod-rollback")
        .await;

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_pod", "remove_pod"]
    );
    assert!(!service
        .pod_sandboxes
        .lock()
        .await
        .contains_key("pod-rollback"));
    let persistence = service.persistence.lock().await;
    assert!(persistence
        .storage()
        .get_pod_sandbox("pod-rollback")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn rollback_failed_container_create_undoes_nri_and_local_state() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_create_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let container_id = "ctr-create-undo".to_string();
    let pod_id = "pod-create-undo".to_string();
    let sidecar_id = "ctr-create-undo-sidecar".to_string();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert(pod_id.clone(), test_pod(&pod_id, HashMap::new()));
    service.containers.lock().await.insert(
        container_id.clone(),
        test_container(&container_id, &pod_id, HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            &container_id,
            &pod_id,
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["/bin/sh".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    service.containers.lock().await.insert(
        sidecar_id.clone(),
        test_container(&sidecar_id, &pod_id, HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            &sidecar_id,
            &pod_id,
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, &sidecar_id, "running");

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = sidecar_id.clone();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 333;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    let event = service
        .nri_container_event(&pod_id, &container_id, &HashMap::new())
        .await;
    fake_nri.create_container(event.clone()).await.unwrap();
    let _ = fake_nri.post_create_container(event.clone()).await;

    service
        .rollback_failed_container_create(&container_id, event)
        .await;

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec![
            "create",
            "post_create",
            "stop_container",
            "remove_container"
        ]
    );
    let containers = service.containers.lock().await;
    assert!(!containers.contains_key(&container_id));
    assert!(containers.contains_key(&sidecar_id));
    drop(containers);
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .list_containers()
        .unwrap()
        .iter()
        .all(|container| container.id != container_id));
    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, &sidecar_id)).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 333);
}

#[tokio::test]
async fn update_pod_sandbox_resources_updates_pause_container_and_notifies_nri() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update".to_string();
    let pause_id = "pause-pod-update".to_string();
    let netns_path = "/var/run/netns/pod-update".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-update".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update".to_string(),
                attempt: 0,
            }),
            state: PodSandboxState::SandboxReady as i32,
            annotations: annotations.clone(),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update".to_string(),
                hostname: "pod-update".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id.clone(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            &pod_id,
            "ready",
            "pod-update",
            "default",
            "uid-update",
            &netns_path,
            &HashMap::new(),
            &annotations,
            Some(&pause_id),
            None,
        )
        .unwrap();

    let overhead = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 128,
        ..Default::default()
    };
    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 512,
        memory_limit_in_bytes: 4096,
        ..Default::default()
    };

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id.clone(),
            overhead: Some(overhead.clone()),
            resources: Some(resources.clone()),
        }),
    )
    .await
    .unwrap();

    let update_payload: serde_json::Value =
        serde_json::from_slice(&fs::read(fake_runtime_update_path(&dir, &pause_id)).unwrap())
            .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 512);
    assert_eq!(update_payload["memory"]["limit"], 4096);
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_pod", "post_update_pod"]
    );

    let updated_pod = service
        .pod_sandboxes
        .lock()
        .await
        .get(&pod_id)
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &updated_pod.annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.overhead_linux_resources.unwrap().cpu_shares, 128);
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 512);
    let pod = service.nri_pod_event(&pod_id).await.pod.unwrap();
    let pod_linux = pod.linux.unwrap();
    assert_eq!(
        pod_linux
            .pod_overhead
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(128)
    );
    assert_eq!(
        pod_linux
            .pod_resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(512)
    );

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox(&pod_id)
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &persisted_annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        persisted_state.overhead_linux_resources.unwrap().cpu_shares,
        128
    );
    assert_eq!(
        persisted_state
            .linux_resources
            .unwrap()
            .memory_limit_in_bytes,
        4096
    );
}

#[tokio::test]
async fn update_pod_sandbox_resources_succeeds_when_nri_post_update_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_update_pod_sandbox: true,
        ..Default::default()
    });
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-post-fail".to_string();
    let pause_id = "pause-pod-update-post-fail".to_string();
    let netns_path = "/var/run/netns/pod-update-post-fail".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-update-post-fail".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-post-fail".to_string(),
                attempt: 0,
            }),
            state: PodSandboxState::SandboxReady as i32,
            annotations,
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-post-fail".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-post-fail".to_string(),
                hostname: "pod-update-post-fail".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id,
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect("post-update pod failure should not fail sandbox resource update");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_pod", "post_update_pod"]
    );
}

#[tokio::test]
async fn update_pod_sandbox_resources_fails_when_cgroup_support_is_disabled() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.disable_cgroup = true;

    let err = RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: "pod-any".to_string(),
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 128,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("disable_cgroup"));
}

#[tokio::test]
async fn update_pod_sandbox_resources_fails_when_pod_is_not_ready() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-not-ready".to_string();
    let pause_id = "pause-pod-update-not-ready".to_string();
    let netns_path = "/var/run/netns/pod-update-not-ready".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            state: PodSandboxState::SandboxNotready as i32,
            annotations,
            ..test_pod(&pod_id, HashMap::new())
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-not-ready".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-not-ready".to_string(),
                hostname: "pod-update-not-ready".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id,
            state: crate::pod::PodSandboxState::NotReady,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    let err = RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect_err("not ready pod should reject resource updates");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("not ready"));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn update_pod_sandbox_resources_fails_when_pause_container_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-missing-pause".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: None,
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            state: PodSandboxState::SandboxReady as i32,
            annotations,
            ..test_pod(&pod_id, HashMap::new())
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-missing-pause".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-missing-pause".to_string(),
                hostname: "pod-update-missing-pause".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::new(),
            pause_container_id: String::new(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    let err = RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect_err("ready pod without pause container should reject resource updates");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("pause container"));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn update_pod_sandbox_resources_overhead_only_updates_pause_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-overhead-only".to_string();
    let pause_id = "pause-pod-update-overhead-only".to_string();
    let netns_path = "/var/run/netns/pod-update-overhead-only".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            state: PodSandboxState::SandboxReady as i32,
            annotations: annotations.clone(),
            runtime_handler: "runc".to_string(),
            ..test_pod(&pod_id, HashMap::new())
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-overhead-only".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-overhead-only".to_string(),
                hostname: "pod-update-overhead-only".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id.clone(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id.clone(),
            overhead: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 321,
                ..Default::default()
            }),
            resources: None,
        }),
    )
    .await
    .unwrap();

    let update_payload: serde_json::Value =
        serde_json::from_slice(&fs::read(fake_runtime_update_path(&dir, &pause_id)).unwrap())
            .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 321);

    let updated_pod = service
        .pod_sandboxes
        .lock()
        .await
        .get(&pod_id)
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &updated_pod.annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.overhead_linux_resources.unwrap().cpu_shares, 321);
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 321);
}

#[tokio::test]
async fn nri_pod_event_uses_pause_spec_resources_for_linux_resources() {
    let (dir, service) = test_service_with_fake_runtime();
    let pod_id = "pod-nri-linux-resources".to_string();
    let pause_id = "pause-nri-linux-resources".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            annotations,
            ..Default::default()
        },
    );

    write_test_bundle_config_with_linux(
        &dir,
        &pause_id,
        &HashMap::new(),
        serde_json::json!({
            "resources": {
                "memory": {
                    "limit": 8192
                },
                "cpu": {
                    "shares": 1024
                }
            }
        }),
    );

    let pod = service.nri_pod_event(&pod_id).await.pod.unwrap();
    let linux = pod.linux.unwrap();
    let resources = linux
        .resources
        .into_option()
        .expect("pause OCI resources should be exposed");
    assert_eq!(
        resources
            .memory
            .as_ref()
            .and_then(|memory| memory.limit.as_ref())
            .map(|limit| limit.value),
        Some(8192)
    );
    assert_eq!(
        resources
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(1024)
    );
}

#[tokio::test]
async fn nri_create_result_applies_adjustments_and_side_effects() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
    adjustment
        .annotations
        .insert("plugin.annotation".to_string(), "set".to_string());
    adjustment.env.push(crate::nri_proto::api::KeyValue {
        key: "PLUGIN_ENV".to_string(),
        value: "enabled".to_string(),
        ..Default::default()
    });
    adjustment.args = vec![String::new(), "/bin/echo".to_string(), "plugin".to_string()];

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-update".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 2048;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let create_result = NriCreateContainerResult {
        adjustment,
        updates: vec![update],
        evictions: vec![crate::nri_proto::api::ContainerEviction {
            container_id: "container-evict".to_string(),
            reason: "plugin request".to_string(),
            ..Default::default()
        }],
    };

    service.pod_sandboxes.lock().await.insert(
        "pod-create".to_string(),
        test_pod("pod-create", HashMap::new()),
    );

    let mut update_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut update_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let update_container =
        test_container("container-update", "pod-create", update_annotations.clone());
    let evict_container = test_container("container-evict", "pod-create", HashMap::new());
    service
        .containers
        .lock()
        .await
        .insert("container-update".to_string(), update_container.clone());
    service
        .containers
        .lock()
        .await
        .insert("container-evict".to_string(), evict_container.clone());
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-update",
            "pod-create",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &update_annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-evict",
            "pod-create",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-update", "running");
    set_fake_runtime_state(&dir, "container-evict", "running");

    let mut stored_annotations = HashMap::new();
    stored_annotations.insert("existing".to_string(), "value".to_string());
    let container_config = ContainerConfig {
        name: "created".to_string(),
        image: "busybox:latest".to_string(),
        command: vec!["sleep".to_string()],
        args: vec!["10".to_string()],
        env: Vec::new(),
        working_dir: None,
        mounts: Vec::new(),
        labels: Vec::new(),
        annotations: stored_annotations
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
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
        rootfs: dir
            .path()
            .join("root")
            .join("containers")
            .join("created")
            .join("rootfs"),
    };
    let mut spec = service
        .runtime
        .build_spec("created", &container_config)
        .unwrap();
    let mut nri_event = NriContainerEvent::default();

    service
        .process_nri_create_side_effects(&create_result)
        .await
        .unwrap();
    apply_container_adjustment(&mut spec, &create_result.adjustment).unwrap();
    RuntimeServiceImpl::apply_adjusted_annotations(
        &mut stored_annotations,
        &create_result.adjustment,
    );
    RuntimeServiceImpl::refresh_nri_event_container_from_spec(
        &mut nri_event,
        &spec,
        &stored_annotations,
    );

    assert_eq!(
        stored_annotations.get("plugin.annotation"),
        Some(&"set".to_string())
    );
    assert_eq!(
        spec.annotations.as_ref().unwrap().get("plugin.annotation"),
        Some(&"set".to_string())
    );
    assert_eq!(
        spec.process.as_ref().unwrap().env.as_ref().unwrap(),
        &vec!["PLUGIN_ENV=enabled".to_string()]
    );
    assert_eq!(
        spec.process.as_ref().unwrap().args,
        vec!["/bin/echo".to_string(), "plugin".to_string()]
    );
    assert_eq!(
        nri_event.container.annotations.get("plugin.annotation"),
        Some(&"set".to_string())
    );

    let updated = service
        .containers
        .lock()
        .await
        .get("container-update")
        .cloned()
        .unwrap();
    let updated_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &updated.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        updated_state
            .linux_resources
            .as_ref()
            .map(|resources| resources.cpu_shares),
        Some(2048)
    );
    let evicted = service
        .containers
        .lock()
        .await
        .get("container-evict")
        .cloned()
        .unwrap();
    assert_eq!(evicted.state, ContainerState::ContainerExited as i32);
}

#[test]
fn enrich_container_annotations_adds_upstream_runtime_metadata() {
    let mut annotations = HashMap::new();
    annotations.insert("existing".to_string(), "value".to_string());
    let pod_state = StoredPodState {
        runtime_handler: "runc".to_string(),
        ..Default::default()
    };
    let log_path = PathBuf::from("/var/log/pods/workload.log");

    RuntimeServiceImpl::enrich_container_annotations(
        super::annotations::ContainerAnnotationContext {
            annotations: &mut annotations,
            container_id: "container-1",
            pod_sandbox_id: "pod-1",
            metadata_name: Some("workload"),
            requested_image: Some("docker.io/library/busybox:latest"),
            resolved_image_name: Some("busybox:latest"),
            log_path: Some(&log_path),
            pod_state: Some(&pod_state),
            default_runtime: "fallback-runtime",
        },
    );

    assert_eq!(
        annotations.get("existing").map(String::as_str),
        Some("value")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_CONTAINER)
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_CONTAINER)
    );
    assert_eq!(
        annotations
            .get(CRIO_RUNTIME_HANDLER_ANNOTATION)
            .map(String::as_str),
        Some("runc")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_RUNTIME_HANDLER_ANNOTATION)
            .map(String::as_str),
        Some("runc")
    );
    assert_eq!(
        annotations
            .get(CRIO_LOG_PATH_ANNOTATION)
            .map(String::as_str),
        Some("/var/log/pods/workload.log")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("workload")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("workload")
    );
    assert_eq!(
        annotations
            .get(CRIO_USER_REQUESTED_IMAGE_ANNOTATION)
            .map(String::as_str),
        Some("docker.io/library/busybox:latest")
    );
    assert_eq!(
        annotations
            .get(CRIO_IMAGE_NAME_ANNOTATION)
            .map(String::as_str),
        Some("busybox:latest")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_IMAGE_NAME_ANNOTATION)
            .map(String::as_str),
        Some("busybox:latest")
    );
}

#[test]
fn apply_runtime_handler_default_annotations_injects_missing_values_only() {
    let mut service = test_service();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .default_annotations = HashMap::from([
        ("io.example/default".to_string(), "kata".to_string()),
        ("existing".to_string(), "from-default".to_string()),
    ]);
    let mut annotations = HashMap::from([("existing".to_string(), "user-value".to_string())]);

    service.apply_runtime_handler_default_annotations(&mut annotations, "kata");

    assert_eq!(
        annotations.get("io.example/default").map(String::as_str),
        Some("kata")
    );
    assert_eq!(
        annotations.get("existing").map(String::as_str),
        Some("user-value")
    );
}

#[test]
fn nri_allowed_annotation_prefixes_include_existing_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let annotations = HashMap::from([
        ("existing.annotation".to_string(), "value".to_string()),
        (
            INTERNAL_CONTAINER_STATE_KEY.to_string(),
            "{\"hidden\":true}".to_string(),
        ),
    ]);

    let allowed = service
        .nri_allowed_annotation_prefixes(&annotations, &annotations, "runc")
        .unwrap();
    assert!(allowed.iter().any(|item| item == "existing.annotation"));
    assert!(!allowed
        .iter()
        .any(|item| item == INTERNAL_CONTAINER_STATE_KEY));
}

#[test]
fn external_pod_annotations_filter_container_reserved_keys_but_keep_user_annotations() {
    let annotations = HashMap::from([
        (
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            "app".to_string(),
        ),
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "default".to_string(),
        ),
        ("example.com/custom".to_string(), "value".to_string()),
    ]);

    let filtered = RuntimeServiceImpl::external_pod_annotations(&annotations);

    assert!(!filtered.contains_key(CRIO_CONTAINER_NAME_ANNOTATION));
    assert_eq!(
        filtered
            .get(CRIO_POD_NAMESPACE_ANNOTATION)
            .map(String::as_str),
        Some("default")
    );
    assert_eq!(
        filtered.get("example.com/custom").map(String::as_str),
        Some("value")
    );
}

#[test]
fn external_container_annotations_filter_pod_reserved_keys_but_keep_user_annotations() {
    let annotations = HashMap::from([
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "default".to_string(),
        ),
        (
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            "app".to_string(),
        ),
        ("example.com/custom".to_string(), "value".to_string()),
    ]);

    let filtered = RuntimeServiceImpl::external_container_annotations(&annotations);

    assert!(!filtered.contains_key(CRIO_POD_NAMESPACE_ANNOTATION));
    assert_eq!(
        filtered
            .get(CRIO_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("app")
    );
    assert_eq!(
        filtered.get("example.com/custom").map(String::as_str),
        Some("value")
    );
}

#[test]
fn external_container_annotations_keep_kubelet_container_metadata_annotations() {
    let annotations = HashMap::from([
        (
            "io.kubernetes.container.restartCount".to_string(),
            "1".to_string(),
        ),
        (
            "io.kubernetes.container.terminationMessagePath".to_string(),
            "/dev/termination-log".to_string(),
        ),
        (
            "io.kubernetes.container.terminationMessagePolicy".to_string(),
            "File".to_string(),
        ),
    ]);

    let filtered = RuntimeServiceImpl::external_container_annotations(&annotations);

    assert_eq!(
        filtered
            .get("io.kubernetes.container.restartCount")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        filtered
            .get("io.kubernetes.container.terminationMessagePath")
            .map(String::as_str),
        Some("/dev/termination-log")
    );
    assert_eq!(
        filtered
            .get("io.kubernetes.container.terminationMessagePolicy")
            .map(String::as_str),
        Some("File")
    );
}

#[test]
fn merge_external_pod_annotations_prefers_spec_for_allowed_pod_keys_only() {
    let base = HashMap::from([
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "default".to_string(),
        ),
        ("example.com/custom".to_string(), "base".to_string()),
    ]);
    let spec = HashMap::from([
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "kube-system".to_string(),
        ),
        (
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            "should-be-hidden".to_string(),
        ),
        ("example.com/custom".to_string(), "spec".to_string()),
    ]);

    let merged = RuntimeServiceImpl::merge_external_pod_annotations(&base, Some(&spec));

    assert_eq!(
        merged
            .get(CRIO_POD_NAMESPACE_ANNOTATION)
            .map(String::as_str),
        Some("kube-system")
    );
    assert!(!merged.contains_key(CRIO_CONTAINER_NAME_ANNOTATION));
    assert_eq!(
        merged.get("example.com/custom").map(String::as_str),
        Some("spec")
    );
}

#[test]
fn filter_nri_annotation_adjustments_drops_unlisted_keys() {
    let (_dir, service) = test_service_with_fake_runtime();
    let existing = HashMap::from([("existing.annotation".to_string(), "value".to_string())]);
    let adjustments = HashMap::from([
        ("existing.annotation".to_string(), "updated".to_string()),
        ("disallowed.annotation".to_string(), "value".to_string()),
    ]);

    let filtered = service
        .filter_nri_annotation_adjustments(&existing, &adjustments, &existing, "runc")
        .unwrap();
    assert_eq!(
        filtered,
        HashMap::from([("existing.annotation".to_string(), "updated".to_string())])
    );
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_and_workload_overrides() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .nri_config
        .runtime_allowed_annotation_prefixes
        .insert(
            "kata".to_string(),
            vec!["io.kubernetes.cri-o.RuntimeHandler.kata".to_string()],
        );
    service.nri_config.workload_allowed_annotation_prefixes = vec![NriAnnotationWorkloadConfig {
        activation_annotation: "workload.example/class".to_string(),
        activation_value: "latency".to_string(),
        allowed_annotation_prefixes: vec!["workload.example/".to_string()],
    }];

    let activation_annotations =
        HashMap::from([("workload.example/class".to_string(), "latency".to_string())]);
    let existing_annotations = HashMap::new();

    let allowed = service
        .nri_allowed_annotation_prefixes(&activation_annotations, &existing_annotations, "kata")
        .unwrap();

    assert!(allowed
        .iter()
        .any(|item| item == "io.kubernetes.cri-o.RuntimeHandler.kata"));
    assert!(allowed.iter().any(|item| item == "workload.example/"));
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_workload_profile_allowed_annotations() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.workloads.insert(
        "management".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            allowed_annotations: vec!["workload.profile/".to_string()],
            ..Default::default()
        },
    );

    let activation_annotations = HashMap::from([(
        "target.workload.openshift.io/management".to_string(),
        "{\"effect\":\"PreferredDuringScheduling\"}".to_string(),
    )]);
    let allowed = service
        .nri_allowed_annotation_prefixes(&activation_annotations, &HashMap::new(), "runc")
        .unwrap();

    assert!(allowed.iter().any(|item| item == "workload.profile/"));
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_handler_allowed_annotations() {
    let mut service = test_service();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .allowed_annotations = vec!["io.example.runtime/".to_string()];

    let allowed = service
        .nri_allowed_annotation_prefixes(&HashMap::new(), &HashMap::new(), "kata")
        .unwrap();

    assert!(allowed.iter().any(|item| item == "io.example.runtime/"));
}

#[test]
fn workload_resources_from_annotation_merges_defaults_and_overrides() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.workloads.insert(
        "management".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            resources: crate::config::RuntimeWorkloadResources {
                cpu_shares: 1024,
                cpu_period: 100_000,
                cpu_limit: 1500,
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let annotations = HashMap::from([
        (
            "target.workload.openshift.io/management".to_string(),
            "{\"effect\":\"PreferredDuringScheduling\"}".to_string(),
        ),
        (
            "resources.workload.openshift.io/main".to_string(),
            "{\"cpushares\":2048,\"cpuset\":\"0-2\"}".to_string(),
        ),
    ]);

    let resources = service
        .workload_resources_from_annotation(&annotations, "main")
        .unwrap()
        .expect("workload resources should be present");
    assert_eq!(resources.cpu_shares, 2048);
    assert_eq!(resources.cpu_period, 100_000);
    assert_eq!(resources.cpu_limit, 1500);
    assert_eq!(resources.cpuset_cpus, "0-2");
}

#[test]
fn apply_workload_resources_overrides_cpu_settings() {
    let mut resources = StoredLinuxResources::default();
    RuntimeServiceImpl::apply_workload_resources(
        &mut resources,
        &crate::config::RuntimeWorkloadResources {
            cpu_shares: 2048,
            cpu_period: 100_000,
            cpu_limit: 1500,
            cpuset_cpus: "0-2".to_string(),
            ..Default::default()
        },
    );

    assert_eq!(resources.cpu_shares, 2048);
    assert_eq!(resources.cpu_period, 100_000);
    assert_eq!(resources.cpu_quota, 150_000);
    assert_eq!(resources.cpuset_cpus, "0-2");
}

#[test]
fn selected_workload_profile_rejects_multiple_activations() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.workloads.insert(
        "management".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            ..Default::default()
        },
    );
    service.config.workloads.insert(
        "latency".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/latency".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            ..Default::default()
        },
    );

    let err = service
        .selected_workload_profile(&HashMap::from([
            (
                "target.workload.openshift.io/management".to_string(),
                "{}".to_string(),
            ),
            (
                "target.workload.openshift.io/latency".to_string(),
                "{}".to_string(),
            ),
        ]))
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("multiple workload profiles"));
}

#[test]
fn sanitize_nri_linux_resources_with_flags_clears_unsupported_fields() {
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut memory = crate::nri_proto::api::LinuxMemory::new();
    let mut swap = crate::nri_proto::api::OptionalInt64::new();
    swap.value = 4096;
    memory.swap = protobuf::MessageField::some(swap);
    let mut kernel = crate::nri_proto::api::OptionalInt64::new();
    kernel.value = 1024;
    memory.kernel = protobuf::MessageField::some(kernel);
    resources.memory = protobuf::MessageField::some(memory);
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut runtime = crate::nri_proto::api::OptionalInt64::new();
    runtime.value = 1000;
    cpu.realtime_runtime = protobuf::MessageField::some(runtime);
    resources.cpu = protobuf::MessageField::some(cpu);
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = crate::nri_proto::api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);
    resources
        .hugepage_limits
        .push(crate::nri_proto::api::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
            ..Default::default()
        });

    RuntimeServiceImpl::sanitize_nri_linux_resources_with_flags(
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
            blockio: false,
            rdt: false,
        },
    );

    let memory = resources.memory.as_ref().unwrap();
    assert!(memory.swap.is_none());
    assert!(memory.kernel.is_none());
    assert!(resources.cpu.as_ref().unwrap().realtime_runtime.is_none());
    assert!(resources.hugepage_limits.is_empty());
    assert!(resources.blockio_class.is_none());
    assert!(resources.rdt_class.is_none());
}

#[test]
fn sanitize_nri_adjustment_for_nri_config_clears_blockio_without_config() {
    let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
    let mut linux = crate::nri_proto::api::LinuxContainerAdjustment::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = crate::nri_proto::api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);
    linux.resources = protobuf::MessageField::some(resources);
    let mut rdt = crate::nri_proto::api::LinuxRdt::new();
    let mut clos_id = crate::nri_proto::api::OptionalString::new();
    clos_id.value = "silver".to_string();
    rdt.clos_id = protobuf::MessageField::some(clos_id);
    linux.rdt = protobuf::MessageField::some(rdt);
    adjustment.linux = protobuf::MessageField::some(linux);

    RuntimeServiceImpl::sanitize_nri_adjustment_for_nri_config(
        &mut adjustment,
        &NriConfig::default(),
    );

    let linux = adjustment.linux.as_ref().unwrap();
    let resources = linux.resources.as_ref().unwrap();
    assert!(resources.blockio_class.is_none());
    if !Path::new("/sys/fs/resctrl").exists() {
        assert!(resources.rdt_class.is_none());
        assert!(linux.rdt.is_none());
    }
}

fn set_fake_runtime_state(dir: &TempDir, id: &str, state: &str) {
    let state_path = fake_runtime_state_path(dir, id);
    fs::create_dir_all(state_path.parent().unwrap()).unwrap();
    fs::write(&state_path, state).unwrap();
    let pid_path = dir.path().join("runtime-state").join(format!("{}.pid", id));
    if state == "running" {
        fs::write(pid_path, format!("{}", std::process::id())).unwrap();
    } else {
        let _ = fs::remove_file(pid_path);
    }
}

fn write_test_bundle_config(
    dir: &TempDir,
    container_id: &str,
    annotations: &HashMap<String, String>,
) {
    let bundle_dir = dir.path().join("runtime-root").join(container_id);
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": annotations,
            "root": {
                "path": "/tmp/rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();
}

fn write_test_bundle_config_with_linux(
    dir: &TempDir,
    container_id: &str,
    annotations: &HashMap<String, String>,
    linux: serde_json::Value,
) {
    let bundle_dir = dir.path().join("runtime-root").join(container_id);
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": annotations,
            "root": {
                "path": "/tmp/rootfs"
            },
            "linux": linux,
        })
        .to_string(),
    )
    .unwrap();
}

fn test_container(
    id: &str,
    pod_sandbox_id: &str,
    annotations: HashMap<String, String>,
) -> Container {
    Container {
        id: id.to_string(),
        pod_sandbox_id: pod_sandbox_id.to_string(),
        state: ContainerState::ContainerCreated as i32,
        metadata: Some(ContainerMetadata {
            name: format!("{}-name", id),
            attempt: 1,
        }),
        image: Some(ImageSpec {
            image: "busybox:latest".to_string(),
            ..Default::default()
        }),
        image_ref: "busybox:latest".to_string(),
        annotations,
        created_at: RuntimeServiceImpl::now_nanos(),
        ..Default::default()
    }
}

fn test_pod(
    id: &str,
    annotations: HashMap<String, String>,
) -> crate::proto::runtime::v1::PodSandbox {
    crate::proto::runtime::v1::PodSandbox {
        id: id.to_string(),
        metadata: Some(PodSandboxMetadata {
            name: format!("{}-pod", id),
            uid: format!("{}-uid", id),
            namespace: "default".to_string(),
            attempt: 1,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: RuntimeServiceImpl::now_nanos(),
        labels: HashMap::new(),
        annotations,
        runtime_handler: "runc".to_string(),
    }
}

fn host_network_run_pod_sandbox_request(
    name: &str,
    namespace: &str,
    uid: &str,
    annotations: HashMap<String, String>,
) -> RunPodSandboxRequest {
    RunPodSandboxRequest {
        config: Some(crate::proto::runtime::v1::PodSandboxConfig {
            metadata: Some(PodSandboxMetadata {
                name: name.to_string(),
                uid: uid.to_string(),
                namespace: namespace.to_string(),
                attempt: 1,
            }),
            hostname: name.to_string(),
            labels: HashMap::new(),
            annotations,
            log_directory: String::new(),
            dns_config: None,
            port_mappings: Vec::new(),
            linux: Some(crate::proto::runtime::v1::LinuxPodSandboxConfig {
                security_context: Some(crate::proto::runtime::v1::LinuxSandboxSecurityContext {
                    namespace_options: Some(NamespaceOption {
                        network: NamespaceMode::Node as i32,
                        pid: NamespaceMode::Pod as i32,
                        ipc: NamespaceMode::Pod as i32,
                        target_id: String::new(),
                        userns_options: None,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        runtime_handler: String::new(),
    }
}

fn pod_sandbox_request_with_namespace_options(
    name: &str,
    namespace: &str,
    uid: &str,
    annotations: HashMap<String, String>,
    runtime_handler: &str,
    namespace_options: NamespaceOption,
) -> RunPodSandboxRequest {
    RunPodSandboxRequest {
        config: Some(crate::proto::runtime::v1::PodSandboxConfig {
            metadata: Some(PodSandboxMetadata {
                name: name.to_string(),
                uid: uid.to_string(),
                namespace: namespace.to_string(),
                attempt: 1,
            }),
            hostname: name.to_string(),
            labels: HashMap::new(),
            annotations,
            log_directory: String::new(),
            dns_config: None,
            port_mappings: Vec::new(),
            linux: Some(crate::proto::runtime::v1::LinuxPodSandboxConfig {
                security_context: Some(crate::proto::runtime::v1::LinuxSandboxSecurityContext {
                    namespace_options: Some(namespace_options),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        runtime_handler: runtime_handler.to_string(),
    }
}

#[test]
fn pod_network_status_keeps_primary_and_additional_ips() {
    let state = StoredPodState {
        ip: Some("10.88.0.10".to_string()),
        additional_ips: vec![
            "10.88.0.11".to_string(),
            "10.88.0.10".to_string(),
            "".to_string(),
            "10.88.0.11".to_string(),
        ],
        ..Default::default()
    };

    let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
        .expect("expected network status");
    assert_eq!(status.ip, "10.88.0.10");
    assert_eq!(status.additional_ips.len(), 1);
    assert_eq!(status.additional_ips[0].ip, "10.88.0.11");
}

#[test]
fn uid_map_parser_distinguishes_initial_and_nested_user_namespaces() {
    assert!(!RuntimeServiceImpl::uid_map_indicates_user_namespace(
        "0 0 4294967295"
    ));
    assert!(RuntimeServiceImpl::uid_map_indicates_user_namespace(
        "0 1000 65536"
    ));
    assert!(RuntimeServiceImpl::uid_map_indicates_user_namespace(""));
    assert!(!RuntimeServiceImpl::uid_map_indicates_user_namespace(
        "garbage"
    ));
}

#[test]
fn effective_pod_sysctls_adds_unprivileged_defaults_for_non_host_network_pods() {
    let mut service = test_service();
    service.config.enable_unprivileged_ports = true;
    service.config.enable_unprivileged_icmp = true;
    service.config.default_sysctls =
        HashMap::from([("kernel.shm_rmid_forced".to_string(), "1".to_string())]);
    service.config.default_mounts_file = PathBuf::from("/etc/containers/mounts.conf");
    service.config.absent_mount_sources_to_reject = vec![PathBuf::from("/etc/hostname")];

    let namespace_options = NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: None,
    };
    let effective = service.effective_pod_sysctls(None, Some(&namespace_options));

    assert_eq!(
        effective.get("net.ipv4.ip_unprivileged_port_start"),
        Some(&"0".to_string())
    );
    assert_eq!(
        effective.get("net.ipv4.ping_group_range"),
        Some(&"0 2147483647".to_string())
    );
    assert_eq!(
        effective.get("kernel.shm_rmid_forced"),
        Some(&"1".to_string())
    );
}

#[test]
fn effective_pod_sysctls_preserves_user_overrides_and_skips_host_network_defaults() {
    let mut service = test_service();
    service.config.enable_unprivileged_ports = true;
    service.config.enable_unprivileged_icmp = true;
    service.config.default_sysctls =
        HashMap::from([("kernel.shm_rmid_forced".to_string(), "1".to_string())]);

    let requested = HashMap::from([
        (
            "net.ipv4.ip_unprivileged_port_start".to_string(),
            "500".to_string(),
        ),
        (
            "net.ipv4.ping_group_range".to_string(),
            "1 1000".to_string(),
        ),
        ("kernel.shm_rmid_forced".to_string(), "0".to_string()),
    ]);
    let host_network = NamespaceOption {
        network: NamespaceMode::Node as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: None,
    };
    let effective = service.effective_pod_sysctls(Some(&requested), Some(&host_network));

    assert_eq!(effective, requested);
}

#[test]
fn effective_pod_sysctls_skips_icmp_default_for_userns_pods() {
    let mut service = test_service();
    service.config.enable_unprivileged_icmp = true;

    let namespace_options = NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: Some(crate::proto::runtime::v1::UserNamespace {
            mode: NamespaceMode::Pod as i32,
            uids: vec![crate::proto::runtime::v1::IdMapping {
                container_id: 0,
                host_id: 100000,
                length: 65536,
            }],
            gids: vec![crate::proto::runtime::v1::IdMapping {
                container_id: 0,
                host_id: 100000,
                length: 65536,
            }],
        }),
    };
    let effective = service.effective_pod_sysctls(None, Some(&namespace_options));

    assert!(!effective.contains_key("net.ipv4.ping_group_range"));
}

#[test]
fn clamp_proto_oom_score_adj_respects_daemon_floor_when_enabled() {
    let current = crate::runtime::RuncRuntime::daemon_oom_score_adj().unwrap();
    let mut service = test_service();
    service.config.restrict_oom_score_adj = true;
    let mut resources = crate::proto::runtime::v1::LinuxContainerResources {
        oom_score_adj: current - 1,
        ..Default::default()
    };

    service.clamp_proto_oom_score_adj(&mut resources).unwrap();

    assert_eq!(resources.oom_score_adj, current);
}

#[test]
fn effective_container_stop_timeout_enforces_configured_minimum() {
    let mut service = test_service();
    service.config.container_stop_timeout = 30;

    assert_eq!(service.effective_container_stop_timeout(0), 30);
    assert_eq!(service.effective_container_stop_timeout(5), 30);
    assert_eq!(service.effective_container_stop_timeout(45), 45);
}

#[test]
fn effective_readonly_rootfs_honors_daemon_default() {
    let mut service = test_service();
    service.config.read_only = true;

    assert!(service.effective_readonly_rootfs(false));
    assert!(service.effective_readonly_rootfs(true));

    service.config.read_only = false;
    assert!(!service.effective_readonly_rootfs(false));
    assert!(service.effective_readonly_rootfs(true));
}

#[test]
fn effective_pids_limit_honors_daemon_default_and_explicit_override() {
    let mut service = test_service();
    service.config.pids_limit = 1024;

    assert_eq!(service.effective_pids_limit(None).unwrap(), Some(1024));
    assert_eq!(service.effective_pids_limit(Some(0)).unwrap(), Some(1024));
    assert_eq!(service.effective_pids_limit(Some(42)).unwrap(), Some(42));
    assert_eq!(service.effective_pids_limit(Some(-1)).unwrap(), None);

    let err = service.effective_pids_limit(Some(-2)).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("pids_limit"));
}

#[test]
fn stored_linux_resources_to_nri_preserves_pids_limit() {
    let resources = StoredLinuxResources {
        pids_limit: Some(321),
        ..Default::default()
    };

    let nri = resources.to_nri();
    assert_eq!(nri.pids.as_ref().map(|pids| pids.limit), Some(321));
}

#[test]
fn effective_userns_options_injects_daemon_default_mappings() {
    let mut service = test_service();
    service.config.uid_mappings = Some(vec![crate::proto::runtime::v1::IdMapping {
        host_id: 100000,
        container_id: 0,
        length: 65536,
    }]);
    service.config.gid_mappings = Some(vec![crate::proto::runtime::v1::IdMapping {
        host_id: 200000,
        container_id: 0,
        length: 65536,
    }]);

    let options = service
        .effective_userns_options(None)
        .expect("daemon defaults should inject namespace options");
    let userns = options
        .userns_options
        .expect("userns options should be present");
    assert_eq!(userns.mode, NamespaceMode::Pod as i32);
    assert_eq!(userns.uids[0].host_id, 100000);
    assert_eq!(userns.gids[0].host_id, 200000);
}

#[test]
fn effective_userns_options_preserves_explicit_node_request() {
    let mut service = test_service();
    service.config.uid_mappings = Some(vec![crate::proto::runtime::v1::IdMapping {
        host_id: 100000,
        container_id: 0,
        length: 65536,
    }]);
    service.config.gid_mappings = Some(vec![crate::proto::runtime::v1::IdMapping {
        host_id: 200000,
        container_id: 0,
        length: 65536,
    }]);
    let requested = NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: Some(crate::proto::runtime::v1::UserNamespace {
            mode: NamespaceMode::Node as i32,
            ..Default::default()
        }),
    };

    let options = service
        .effective_userns_options(Some(&requested))
        .expect("explicit node mode should be preserved");
    assert_eq!(
        options.userns_options.as_ref().map(|userns| userns.mode),
        Some(NamespaceMode::Node as i32)
    );
}

#[test]
fn effective_userns_options_preserves_explicit_request_mappings() {
    let mut service = test_service();
    service.config.uid_mappings = Some(vec![crate::proto::runtime::v1::IdMapping {
        host_id: 100000,
        container_id: 0,
        length: 65536,
    }]);
    service.config.gid_mappings = Some(vec![crate::proto::runtime::v1::IdMapping {
        host_id: 200000,
        container_id: 0,
        length: 65536,
    }]);
    let requested = NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: Some(crate::proto::runtime::v1::UserNamespace {
            mode: NamespaceMode::Pod as i32,
            uids: vec![crate::proto::runtime::v1::IdMapping {
                host_id: 300000,
                container_id: 0,
                length: 65536,
            }],
            gids: vec![crate::proto::runtime::v1::IdMapping {
                host_id: 400000,
                container_id: 0,
                length: 65536,
            }],
        }),
    };

    let options = service
        .effective_userns_options(Some(&requested))
        .expect("explicit mappings should be preserved");
    let userns = options
        .userns_options
        .expect("userns options should be present");
    assert_eq!(userns.uids[0].host_id, 300000);
    assert_eq!(userns.gids[0].host_id, 400000);
}

#[test]
fn effective_container_namespace_options_inherits_host_network_from_sandbox_defaults() {
    let service = test_service();
    let sandbox = StoredNamespaceOptions {
        network: NamespaceMode::Node as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: None,
    };
    let requested = NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: None,
    };

    let options = service
        .effective_container_namespace_options(Some(&requested), Some(&sandbox))
        .expect("sandbox namespace options should be applied");

    assert_eq!(options.network, NamespaceMode::Node as i32);
    assert_eq!(options.pid, NamespaceMode::Pod as i32);
    assert_eq!(options.ipc, NamespaceMode::Pod as i32);
}

#[test]
fn effective_container_namespace_options_inherits_sandbox_when_container_request_omits_it() {
    let service = test_service();
    let sandbox = StoredNamespaceOptions {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Node as i32,
        target_id: String::new(),
        userns_options: None,
    };

    let options = service
        .effective_container_namespace_options(None, Some(&sandbox))
        .expect("sandbox namespace options should be inherited");

    assert_eq!(options.network, NamespaceMode::Pod as i32);
    assert_eq!(options.pid, NamespaceMode::Pod as i32);
    assert_eq!(options.ipc, NamespaceMode::Node as i32);
}

#[test]
fn resolve_pod_runtime_handler_uses_untrusted_runtime_for_legacy_annotation() {
    let mut service = test_service();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());

    let config = crate::proto::runtime::v1::PodSandboxConfig {
        annotations: HashMap::from([(
            CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
            "true".to_string(),
        )]),
        ..Default::default()
    };

    let handler = service
        .resolve_pod_runtime_handler(
            &config,
            "",
            Some(&NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            }),
        )
        .unwrap();
    assert_eq!(handler, "untrusted");
}

#[test]
fn resolve_pod_runtime_handler_rejects_conflicting_handler_for_untrusted_workload() {
    let mut service = test_service();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());
    service.config.runtime_handlers.push("kata".to_string());

    let config = crate::proto::runtime::v1::PodSandboxConfig {
        annotations: HashMap::from([(
            CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
            "true".to_string(),
        )]),
        ..Default::default()
    };

    let err = service
        .resolve_pod_runtime_handler(
            &config,
            "kata",
            Some(&NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            }),
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("explicit runtime handler"));
}

#[test]
fn resolve_pod_runtime_handler_rejects_host_access_for_untrusted_workload() {
    let mut service = test_service();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());

    let config = crate::proto::runtime::v1::PodSandboxConfig {
        annotations: HashMap::from([(
            CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
            "true".to_string(),
        )]),
        ..Default::default()
    };

    let err = service
        .resolve_pod_runtime_handler(
            &config,
            "",
            Some(&NamespaceOption {
                network: NamespaceMode::Node as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            }),
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("host access"));
}

#[test]
fn validate_minimum_mappable_ids_rejects_non_root_uid_mapping_below_minimum() {
    let mut service = test_service();
    service.config.minimum_mappable_uid = 100000;

    let err = service
        .validate_minimum_mappable_ids(
            Some(&NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                    mode: NamespaceMode::Pod as i32,
                    uids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 99999,
                        container_id: 0,
                        length: 65536,
                    }],
                    gids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 200000,
                        container_id: 0,
                        length: 65536,
                    }],
                }),
            }),
            Some("1000"),
            None,
            &[],
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("minimum mappable uid"));
}

#[test]
fn validate_minimum_mappable_ids_rejects_non_root_gid_mapping_below_minimum() {
    let mut service = test_service();
    service.config.minimum_mappable_gid = 200000;

    let err = service
        .validate_minimum_mappable_ids(
            Some(&NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                    mode: NamespaceMode::Pod as i32,
                    uids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 100000,
                        container_id: 0,
                        length: 65536,
                    }],
                    gids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 199999,
                        container_id: 0,
                        length: 65536,
                    }],
                }),
            }),
            Some("1000"),
            Some(1000),
            &[],
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("minimum mappable gid"));
}

#[test]
fn validate_minimum_mappable_ids_allows_root_user_namespace_mappings_below_minimum() {
    let mut service = test_service();
    service.config.minimum_mappable_uid = 100000;
    service.config.minimum_mappable_gid = 200000;

    service
        .validate_minimum_mappable_ids(
            Some(&NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                    mode: NamespaceMode::Pod as i32,
                    uids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 1,
                        container_id: 0,
                        length: 1,
                    }],
                    gids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 1,
                        container_id: 0,
                        length: 1,
                    }],
                }),
            }),
            Some("0"),
            Some(0),
            &[0],
        )
        .expect("root user namespace mappings should bypass minimum mappable checks");
}

#[test]
fn validate_minimum_mappable_ids_ignores_node_mode() {
    let mut service = test_service();
    service.config.minimum_mappable_uid = 100000;

    service
        .validate_minimum_mappable_ids(
            Some(&NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: Some(crate::proto::runtime::v1::UserNamespace {
                    mode: NamespaceMode::Node as i32,
                    uids: vec![crate::proto::runtime::v1::IdMapping {
                        host_id: 1,
                        container_id: 0,
                        length: 1,
                    }],
                    gids: vec![],
                }),
            }),
            Some("1000"),
            None,
            &[],
        )
        .expect("node mode should bypass minimum mappable checks");
}

#[test]
fn pod_network_status_promotes_first_additional_ip_when_primary_missing() {
    let state = StoredPodState {
        additional_ips: vec!["fd00::10".to_string(), "10.88.0.11".to_string()],
        ..Default::default()
    };

    let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
        .expect("expected network status");
    assert_eq!(status.ip, "fd00::10");
    assert_eq!(status.additional_ips.len(), 1);
    assert_eq!(status.additional_ips[0].ip, "10.88.0.11");
}

#[test]
fn pod_network_status_returns_empty_network_for_host_network_pod() {
    let state = StoredPodState {
        namespace_options: Some(StoredNamespaceOptions {
            network: NamespaceMode::Node as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Pod as i32,
            target_id: String::new(),
            userns_options: None,
        }),
        ..Default::default()
    };

    let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
        .expect("host network pod should still report a network status object");
    assert!(status.ip.is_empty());
    assert!(status.additional_ips.is_empty());
}

#[test]
fn pod_linux_status_preserves_host_pid_and_host_ipc_namespace_modes() {
    let state = StoredPodState {
        namespace_options: Some(StoredNamespaceOptions {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Node as i32,
            ipc: NamespaceMode::Node as i32,
            target_id: String::new(),
            userns_options: None,
        }),
        ..Default::default()
    };

    let linux_status =
        RuntimeServiceImpl::pod_linux_status_from_state(Some(&state)).expect("linux status");
    let options = linux_status
        .namespaces
        .and_then(|namespaces| namespaces.options)
        .expect("namespace options");

    assert_eq!(options.network, NamespaceMode::Pod as i32);
    assert_eq!(options.pid, NamespaceMode::Node as i32);
    assert_eq!(options.ipc, NamespaceMode::Node as i32);
}

#[test]
fn pod_linux_status_preserves_userns_options() {
    let state = StoredPodState {
        namespace_options: Some(StoredNamespaceOptions {
            network: NamespaceMode::Pod as i32,
            pid: NamespaceMode::Pod as i32,
            ipc: NamespaceMode::Pod as i32,
            target_id: String::new(),
            userns_options: Some(StoredUserNamespace {
                mode: NamespaceMode::Pod as i32,
                uids: vec![StoredIdMapping {
                    host_id: 100000,
                    container_id: 0,
                    length: 65536,
                }],
                gids: vec![StoredIdMapping {
                    host_id: 200000,
                    container_id: 0,
                    length: 65536,
                }],
            }),
        }),
        ..Default::default()
    };

    let linux_status =
        RuntimeServiceImpl::pod_linux_status_from_state(Some(&state)).expect("linux status");
    let options = linux_status
        .namespaces
        .and_then(|namespaces| namespaces.options)
        .expect("namespace options");
    let userns = options
        .userns_options
        .expect("userns options should be preserved");

    assert_eq!(userns.mode, NamespaceMode::Pod as i32);
    assert_eq!(userns.uids[0].host_id, 100000);
    assert_eq!(userns.gids[0].host_id, 200000);
}

#[test]
fn build_container_status_snapshot_uses_internal_timestamps_and_exit_code() {
    let mut annotations = HashMap::new();
    let stored = StoredContainerState {
        started_at: Some(5),
        finished_at: Some(8),
        exit_code: Some(42),
        mounts: vec![StoredMount {
            container_path: "/data".to_string(),
            host_path: "/host/data".to_string(),
            image: String::new(),
            image_sub_path: String::new(),
            readonly: true,
            selinux_relabel: false,
            propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
        }],
        ..Default::default()
    };
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &stored,
    )
    .expect("store internal state");

    let container = Container {
        id: "container-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "c1".to_string(),
            attempt: 1,
        }),
        image: Some(ImageSpec {
            image: "busybox:latest".to_string(),
            ..Default::default()
        }),
        image_ref: "busybox:latest".to_string(),
        annotations,
        created_at: 1,
        ..Default::default()
    };

    let status = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerExited as i32,
    );

    assert_eq!(
        status.started_at,
        RuntimeServiceImpl::normalize_timestamp_nanos(5)
    );
    assert_eq!(
        status.finished_at,
        RuntimeServiceImpl::normalize_timestamp_nanos(8)
    );
    assert_eq!(status.exit_code, 42);
    assert_eq!(status.reason, "Error");
    assert_eq!(status.message, "container exited with code 42");
    assert_eq!(status.mounts.len(), 1);
    assert_eq!(status.mounts[0].container_path, "/data");
}

#[test]
fn build_container_status_snapshot_masks_stale_exit_fields_for_created_and_running() {
    let mut annotations = HashMap::new();
    let stored = StoredContainerState {
        started_at: Some(5),
        finished_at: Some(8),
        exit_code: Some(42),
        ..Default::default()
    };
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &stored,
    )
    .expect("store internal state");

    let container = Container {
        id: "container-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "c1".to_string(),
            attempt: 1,
        }),
        image: Some(ImageSpec {
            image: "busybox:latest".to_string(),
            ..Default::default()
        }),
        image_ref: "busybox:latest".to_string(),
        annotations,
        created_at: 1,
        ..Default::default()
    };

    let created = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerCreated as i32,
    );
    assert_eq!(created.started_at, 0);
    assert_eq!(created.finished_at, 0);
    assert_eq!(created.exit_code, 0);
    assert_eq!(created.reason, "Created");

    let running = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerRunning as i32,
    );
    assert_eq!(
        running.started_at,
        RuntimeServiceImpl::normalize_timestamp_nanos(5)
    );
    assert_eq!(running.finished_at, 0);
    assert_eq!(running.exit_code, 0);
    assert_eq!(running.reason, "Running");
}

#[test]
fn build_container_status_snapshot_uses_minus_one_when_exited_exit_code_is_unknown() {
    let mut annotations = HashMap::new();
    let stored = StoredContainerState {
        started_at: Some(5),
        finished_at: Some(8),
        exit_code: None,
        ..Default::default()
    };
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &stored,
    )
    .expect("store internal state");

    let container = Container {
        id: "container-unknown-exit".to_string(),
        metadata: Some(ContainerMetadata {
            name: "c1".to_string(),
            attempt: 1,
        }),
        image: Some(ImageSpec {
            image: "busybox:latest".to_string(),
            ..Default::default()
        }),
        image_ref: "busybox:latest".to_string(),
        annotations,
        created_at: 1,
        ..Default::default()
    };

    let status = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerExited as i32,
    );
    assert_eq!(status.exit_code, -1);
    assert_eq!(status.reason, "Error");
    assert_eq!(status.message, "container exited with unknown exit code");
}

#[test]
fn effective_runtime_state_for_container_prefers_stored_exit_when_runtime_is_unknown() {
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            started_at: Some(5),
            finished_at: Some(8),
            exit_code: Some(137),
            ..Default::default()
        },
    )
    .expect("store internal state");

    let container = Container {
        id: "container-exited".to_string(),
        state: ContainerState::ContainerExited as i32,
        metadata: Some(ContainerMetadata {
            name: "c1".to_string(),
            attempt: 1,
        }),
        annotations,
        ..Default::default()
    };

    let runtime_state = RuntimeServiceImpl::effective_runtime_state_for_container(
        &container,
        crate::runtime::ContainerStatus::Unknown,
    );

    assert_eq!(runtime_state, ContainerState::ContainerExited as i32);
}

#[test]
fn effective_runtime_state_for_container_preserves_live_store_state_when_runtime_is_unknown() {
    let container = Container {
        id: "container-running".to_string(),
        state: ContainerState::ContainerRunning as i32,
        metadata: Some(ContainerMetadata {
            name: "c1".to_string(),
            attempt: 1,
        }),
        ..Default::default()
    };

    let runtime_state = RuntimeServiceImpl::effective_runtime_state_for_container(
        &container,
        crate::runtime::ContainerStatus::Unknown,
    );

    assert_eq!(runtime_state, ContainerState::ContainerRunning as i32);
}

#[test]
fn record_to_container_status_uses_minus_one_for_missing_stopped_exit_code() {
    let record = crate::storage::ContainerRecord {
        id: "container-1".to_string(),
        pod_id: "pod-1".to_string(),
        state: "stopped".to_string(),
        image: "busybox:latest".to_string(),
        command: "sleep 1".to_string(),
        created_at: 1,
        labels: "{}".to_string(),
        annotations: "{}".to_string(),
        exit_code: None,
        exit_time: Some(2),
        runtime_handler: None,
        runtime_backend: None,
        snapshot_key: None,
    };

    assert_eq!(
        crate::storage::persistence::record_to_container_status(&record),
        crate::runtime::ContainerStatus::Stopped(-1)
    );
}

#[test]
fn selinux_label_from_proto_uses_defaults_for_missing_parts() {
    let label = RuntimeServiceImpl::selinux_label_from_proto(
        Some(&crate::proto::runtime::v1::SeLinuxOption {
            user: String::new(),
            role: String::new(),
            r#type: "spc_t".to_string(),
            level: String::new(),
        }),
        None,
        1024,
    );

    assert_eq!(label.as_deref(), Some("system_u:system_r:spc_t:s0"));
}

#[test]
fn selinux_label_from_proto_generates_mcs_level_within_configured_range() {
    let label = RuntimeServiceImpl::selinux_label_from_proto(
        Some(&crate::proto::runtime::v1::SeLinuxOption {
            user: String::new(),
            role: String::new(),
            r#type: "container_t".to_string(),
            level: String::new(),
        }),
        Some("pod-seed"),
        8,
    )
    .expect("label should be generated");

    assert!(label.starts_with("system_u:system_r:container_t:s0:c"));
    let categories = label
        .split(":s0:c")
        .nth(1)
        .expect("generated label should contain s0:c");
    let (left, right) = categories.split_once(",c").unwrap();
    let first: u32 = left.parse().unwrap();
    let second: u32 = right.parse().unwrap();
    assert!(first < 8);
    assert!(second < 8);
}

#[test]
fn build_nri_container_from_proto_preserves_spec_cgroups_path() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    spec.linux = Some(crate::oci::spec::Linux {
        namespaces: None,
        uid_mappings: None,
        gid_mappings: None,
        devices: None,
        net_devices: None,
        cgroups_path: Some("/kubepods.slice/pod123/ctr.scope".to_string()),
        resources: None,
        rootfs_propagation: None,
        seccomp: None,
        sysctl: None,
        mount_label: None,
        masked_paths: None,
        readonly_paths: None,
        intel_rdt: None,
    });
    service
        .runtime
        .write_bundle("container-cgroup-path", Path::new("/tmp"), &spec)
        .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            cgroup_parent: Some("kubepods.slice/pod123".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let container = Container {
        id: "container-cgroup-path".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    let linux = nri.linux.unwrap();
    assert_eq!(linux.cgroups_path, "/kubepods.slice/pod123/ctr.scope");
}

#[test]
fn build_nri_container_from_proto_exposes_stored_seccomp_profile() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            seccomp_profile: Some(StoredSecurityProfile {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                    as i32,
                localhost_ref: "/etc/seccomp/workload.json".to_string(),
            }),
            ..Default::default()
        },
    )
    .unwrap();

    let container = Container {
        id: "container-seccomp".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    let profile = nri
        .linux
        .as_ref()
        .and_then(|linux| linux.seccomp_profile.as_ref())
        .expect("seccomp profile should be present");
    assert_eq!(
        profile.profile_type.enum_value().unwrap(),
        crate::nri_proto::api::security_profile::ProfileType::LOCALHOST
    );
    assert_eq!(profile.localhost_ref, "/etc/seccomp/workload.json");
}

#[test]
fn build_nri_container_from_proto_exposes_paused_runtime_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "container-paused", "paused");

    let container = Container {
        id: "container-paused".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        state: ContainerState::ContainerRunning as i32,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(
        nri.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_PAUSED
    );
}

#[test]
fn build_nri_container_from_proto_merges_external_spec_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert("from-spec".to_string(), "visible".to_string());
    spec_annotations.insert(
        INTERNAL_CONTAINER_STATE_KEY.to_string(),
        "{\"hidden\":true}".to_string(),
    );
    spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("container-annotation-merge", Path::new("/tmp"), &spec)
        .unwrap();

    let mut annotations = HashMap::new();
    annotations.insert("from-cri".to_string(), "visible".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    let container = Container {
        id: "container-annotation-merge".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(
        nri.annotations.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.annotations.get("from-spec").map(String::as_str),
        Some("visible")
    );
    assert!(!nri.annotations.contains_key(INTERNAL_CONTAINER_STATE_KEY));
}

#[test]
fn build_nri_container_from_proto_merges_labels_from_spec_annotation() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CRIO_LABELS_ANNOTATION.to_string(),
        "{\"from-spec\":\"visible\"}".to_string(),
    );
    spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("container-label-merge", Path::new("/tmp"), &spec)
        .unwrap();

    let mut labels = HashMap::new();
    labels.insert("from-cri".to_string(), "visible".to_string());
    let container = Container {
        id: "container-label-merge".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        labels,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(
        nri.labels.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.labels.get("from-spec").map(String::as_str),
        Some("visible")
    );
}

#[test]
fn build_nri_container_from_proto_falls_back_to_spec_identity_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CRIO_SANDBOX_ID_ANNOTATION.to_string(),
        "pod-from-spec".to_string(),
    );
    spec_annotations.insert(
        CONTAINERD_CONTAINER_NAME_ANNOTATION.to_string(),
        "container-from-spec".to_string(),
    );
    spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("container-identity-merge", Path::new("/tmp"), &spec)
        .unwrap();

    let container = Container {
        id: "container-identity-merge".to_string(),
        pod_sandbox_id: String::new(),
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(nri.pod_sandbox_id, "pod-from-spec");
    assert_eq!(nri.name, "container-from-spec");
}

#[test]
fn build_nri_pod_from_proto_merges_external_pause_spec_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut pause_spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert("from-pause-spec".to_string(), "visible".to_string());
    spec_annotations.insert(
        INTERNAL_POD_STATE_KEY.to_string(),
        "{\"hidden\":true}".to_string(),
    );
    pause_spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("pause-annotation-merge", Path::new("/tmp"), &pause_spec)
        .unwrap();

    let mut annotations = HashMap::new();
    annotations.insert("from-cri".to_string(), "visible".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-annotation-merge".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let pod = crate::proto::runtime::v1::PodSandbox {
        id: "pod-annotation-merge".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_pod_from_proto(&service.runtime, &pod);
    assert_eq!(
        nri.annotations.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.annotations.get("from-pause-spec").map(String::as_str),
        Some("visible")
    );
    assert!(!nri.annotations.contains_key(INTERNAL_POD_STATE_KEY));
}

#[test]
fn build_nri_pod_from_proto_merges_labels_from_pause_spec_annotation() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut pause_spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CRIO_LABELS_ANNOTATION.to_string(),
        "{\"from-pause-spec\":\"visible\"}".to_string(),
    );
    pause_spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("pause-label-merge", Path::new("/tmp"), &pause_spec)
        .unwrap();

    let mut labels = HashMap::new();
    labels.insert("from-cri".to_string(), "visible".to_string());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-label-merge".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let pod = crate::proto::runtime::v1::PodSandbox {
        id: "pod-label-merge".to_string(),
        labels,
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_pod_from_proto(&service.runtime, &pod);
    assert_eq!(
        nri.labels.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.labels.get("from-pause-spec").map(String::as_str),
        Some("visible")
    );
}

#[test]
fn build_nri_pod_from_proto_falls_back_to_pause_spec_identity_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut pause_spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CONTAINERD_SANDBOX_UID_ANNOTATION.to_string(),
        "uid-from-spec".to_string(),
    );
    spec_annotations.insert(
        CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION.to_string(),
        "ns-from-spec".to_string(),
    );
    spec_annotations.insert(
        CRIO_SANDBOX_NAME_ANNOTATION.to_string(),
        "pod-from-spec".to_string(),
    );
    pause_spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("pause-identity-merge", Path::new("/tmp"), &pause_spec)
        .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-identity-merge".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let pod = crate::proto::runtime::v1::PodSandbox {
        id: "pod-identity-merge".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_pod_from_proto(&service.runtime, &pod);
    assert_eq!(nri.name, "pod-from-spec");
    assert_eq!(nri.namespace, "ns-from-spec");
    assert_eq!(nri.uid, "uid-from-spec");
}

#[test]
fn seccomp_profile_from_proto_supports_localhost_and_unconfined() {
    let localhost_profile = RuntimeServiceImpl::seccomp_profile_from_proto(
        Some(&crate::proto::runtime::v1::SecurityProfile {
            profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                as i32,
            localhost_ref: "/tmp/seccomp/profile.json".to_string(),
        }),
        "",
    );
    assert!(matches!(
        localhost_profile,
        Some(SeccompProfile::Localhost(_))
    ));

    let unconfined_profile = RuntimeServiceImpl::seccomp_profile_from_proto(
        Some(&crate::proto::runtime::v1::SecurityProfile {
            profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                as i32,
            localhost_ref: String::new(),
        }),
        "",
    );
    assert!(matches!(
        unconfined_profile,
        Some(SeccompProfile::Unconfined)
    ));
}

#[test]
fn effective_seccomp_profile_uses_configured_unset_fallback() {
    let mut service = test_service();
    service.config.unset_seccomp_profile = "runtime/default".to_string();

    let profile = service.effective_seccomp_profile_from_proto(None, "", false);
    assert!(matches!(profile, Some(SeccompProfile::RuntimeDefault)));
}

#[test]
fn effective_seccomp_profile_does_not_override_explicit_unconfined() {
    let mut service = test_service();
    service.config.unset_seccomp_profile = "runtime/default".to_string();

    let profile = service.effective_seccomp_profile_from_proto(
        Some(&crate::proto::runtime::v1::SecurityProfile {
            profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                as i32,
            localhost_ref: String::new(),
        }),
        "",
        false,
    );
    assert!(matches!(profile, Some(SeccompProfile::Unconfined)));
}

#[test]
fn effective_seccomp_profile_uses_privileged_fallback_for_privileged_container() {
    let mut service = test_service();
    service.config.privileged_seccomp_profile = "unconfined".to_string();

    let profile = service.effective_seccomp_profile_from_proto(None, "", true);
    assert!(matches!(profile, Some(SeccompProfile::Unconfined)));
}

#[test]
fn seccomp_notifier_action_supports_v1_and_v2_annotations() {
    let service = test_service();
    let mut annotations = HashMap::new();
    annotations.insert(
        "io.kubernetes.cri-o.seccompNotifierAction".to_string(),
        "stop".to_string(),
    );
    assert_eq!(
        service
            .seccomp_notifier_action_from_annotations(&annotations, "runc")
            .unwrap(),
        Some(crate::runtime::SeccompNotifierMode::Stop)
    );

    annotations.clear();
    annotations.insert(
        "seccomp-notifier-action.crio.io".to_string(),
        "".to_string(),
    );
    assert_eq!(
        service
            .seccomp_notifier_action_from_annotations(&annotations, "runc")
            .unwrap(),
        Some(crate::runtime::SeccompNotifierMode::Log)
    );
}

#[test]
fn seccomp_notifier_action_rejects_conflicting_annotations() {
    let service = test_service();
    let annotations = HashMap::from([
        (
            "io.kubernetes.cri-o.seccompNotifierAction".to_string(),
            "stop".to_string(),
        ),
        (
            "seccomp-notifier-action.crio.io".to_string(),
            "log".to_string(),
        ),
    ]);
    let err = service
        .seccomp_notifier_action_from_annotations(&annotations, "runc")
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn effective_apparmor_profile_uses_default_when_runtime_default_requested() {
    let mut service = test_service();
    service.config.apparmor_default_profile = "crius-default".to_string();

    let result = service.effective_apparmor_profile_from_proto(
        Some(&crate::proto::runtime::v1::SecurityProfile {
            profile_type: crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault
                as i32,
            localhost_ref: String::new(),
        }),
        "",
        false,
    );
    let availability = crate::security::SecurityManager::new();
    if availability.is_apparmor_available() {
        assert_eq!(result.unwrap().as_deref(), Some("crius-default"));
    } else {
        assert_eq!(
            result
                .expect_err("runtime/default must fail when apparmor is unavailable")
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}

#[test]
fn effective_apparmor_profile_rejects_explicit_profile_when_disabled() {
    let mut service = test_service();
    service.config.disable_apparmor = true;

    let err = service
        .effective_apparmor_profile_from_proto(
            Some(&crate::proto::runtime::v1::SecurityProfile {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                    as i32,
                localhost_ref: "custom-profile".to_string(),
            }),
            "",
            false,
        )
        .expect_err("disabled apparmor must reject explicit profile");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn effective_selinux_label_skips_host_network_when_configured() {
    let mut service = test_service();
    service.config.enable_selinux = true;
    service.config.hostnetwork_disable_selinux = true;

    let label = service.effective_selinux_label_from_proto(
        Some(&crate::proto::runtime::v1::SeLinuxOption {
            user: String::new(),
            role: String::new(),
            r#type: "spc_t".to_string(),
            level: String::new(),
        }),
        true,
        Some("pod-seed"),
    );
    assert!(label.is_none());
}

#[tokio::test]
async fn resolve_container_id_handles_zero_single_and_multiple_matches() {
    let service = test_service();
    service.containers.lock().await.insert(
        "abcdef123456".to_string(),
        test_container("abcdef123456", "pod-1", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "abc999999999".to_string(),
        test_container("abc999999999", "pod-1", HashMap::new()),
    );

    assert_eq!(
        service.resolve_container_id("abcdef").await.unwrap(),
        "abcdef123456"
    );
    assert_eq!(
        service
            .resolve_container_id("missing")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        service
            .resolve_container_id("abc")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn resolve_pod_sandbox_id_handles_zero_single_and_multiple_matches() {
    let service = test_service();
    service.pod_sandboxes.lock().await.insert(
        "podabcdef123456".to_string(),
        test_pod("podabcdef123456", HashMap::new()),
    );
    service.pod_sandboxes.lock().await.insert(
        "podabc999999999".to_string(),
        test_pod("podabc999999999", HashMap::new()),
    );

    assert_eq!(
        service.resolve_pod_sandbox_id("podabcdef").await.unwrap(),
        "podabcdef123456"
    );
    assert_eq!(
        service
            .resolve_pod_sandbox_id("missing")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        service
            .resolve_pod_sandbox_id("podabc")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn reserve_pod_name_rejects_duplicate_and_allows_reuse_after_release() {
    let service = test_service();
    let pod_metadata = PodSandboxMetadata {
        name: "dup-pod".to_string(),
        uid: "dup-pod-uid".to_string(),
        namespace: "default".to_string(),
        attempt: 1,
    };
    let pod_name_key = RuntimeServiceImpl::pod_name_key(&pod_metadata);

    let mut guard = service.reserve_pod_name("pod-1", &pod_name_key).unwrap();
    let err = service
        .reserve_pod_name("pod-2", &pod_name_key)
        .expect_err("duplicate pod name should be rejected");
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
    assert!(err.message().contains("already exists"));

    guard.disarm();
    service.release_pod_name("pod-1");
    service
        .reserve_pod_name("pod-2", &pod_name_key)
        .expect("pod name should be reusable after release");
}

#[test]
fn reserve_container_name_rejects_duplicate_and_allows_reuse_after_release() {
    let service = test_service();
    let pod_metadata = PodSandboxMetadata {
        name: "pod-create-dup".to_string(),
        uid: "pod-create-dup-uid".to_string(),
        namespace: "default".to_string(),
        attempt: 1,
    };
    let container_metadata = ContainerMetadata {
        name: "dup-container".to_string(),
        attempt: 1,
    };
    let container_name_key =
        RuntimeServiceImpl::container_name_key(&container_metadata, &pod_metadata);

    let mut guard = service
        .reserve_container_name("container-1", &container_name_key)
        .unwrap();
    let err = service
        .reserve_container_name("container-2", &container_name_key)
        .expect_err("duplicate container name should be rejected");
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
    assert!(err.message().contains("already exists"));

    guard.disarm();
    service.release_container_name("container-1");
    service
        .reserve_container_name("container-2", &container_name_key)
        .expect("container name should be reusable after release");
}

#[tokio::test]
async fn create_container_rejects_missing_image_spec() {
    let service = test_service();
    service.pod_sandboxes.lock().await.insert(
        "pod-missing-image".to_string(),
        test_pod("pod-missing-image", HashMap::new()),
    );

    let err = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-missing-image".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "missing-image".to_string(),
                    attempt: 1,
                }),
                image: None,
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect_err("missing image spec should be rejected");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("ContainerConfig.Image is nil"));
    let pod_metadata = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-missing-image")
        .and_then(|pod| pod.metadata.clone())
        .unwrap();
    let name_key = RuntimeServiceImpl::container_name_key(
        &ContainerMetadata {
            name: "missing-image".to_string(),
            attempt: 1,
        },
        &pod_metadata,
    );
    assert!(
        service.container_id_for_reserved_name(&name_key).is_none(),
        "invalid request must not reserve container names"
    );
}

#[tokio::test]
async fn create_container_rejects_not_ready_sandbox() {
    let service = test_service();
    let mut pod = test_pod("pod-notready", HashMap::new());
    pod.state = PodSandboxState::SandboxNotready as i32;
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-notready".to_string(), pod);

    let err = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-notready".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "workload".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    user_specified_image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect_err("not-ready sandbox should be rejected");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("sandbox is not ready"));
    assert!(err.message().contains("pod-notready"));
}

#[tokio::test]
async fn create_container_reclaims_stale_reserved_name_without_live_container() {
    let service = test_service();
    let name_key = "container:workload:pod-stale-name:default:uid:1".to_string();
    let mut stale_guard = service
        .reserve_container_name("stale-container", &name_key)
        .expect("stale reservation should succeed");
    stale_guard.disarm();

    let guard = service
        .reserve_container_name_for_create("fresh-container", &name_key)
        .await
        .expect("stale reservation should be reclaimed");
    let reserved = service
        .container_id_for_reserved_name(&name_key)
        .expect("name should be reserved for the new create attempt");
    assert_eq!(reserved, "fresh-container");
    drop(guard);
}

#[tokio::test]
async fn create_container_reclaims_exited_reserved_name_without_live_runtime() {
    let (_dir, service) = test_service_with_fake_runtime();
    let name_key = "container:workload:pod-exited-name:default:uid:1".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("workload".to_string()),
            metadata_attempt: Some(1),
            finished_at: Some(RuntimeServiceImpl::now_nanos()),
            exit_code: Some(-1),
            ..Default::default()
        },
    )
    .unwrap();

    let mut stale_guard = service
        .reserve_container_name("exited-container", &name_key)
        .expect("stale reservation should succeed");
    stale_guard.disarm();
    service.containers.lock().await.insert(
        "exited-container".to_string(),
        Container {
            state: ContainerState::ContainerExited as i32,
            metadata: Some(ContainerMetadata {
                name: "workload".to_string(),
                attempt: 1,
            }),
            pod_sandbox_id: "pod-exited-name".to_string(),
            annotations: annotations.clone(),
            ..Default::default()
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "exited-container",
            "pod-exited-name",
            crate::runtime::ContainerStatus::Stopped(-1),
            "busybox:latest",
            &["sleep".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let guard = service
        .reserve_container_name_for_create("fresh-container", &name_key)
        .await
        .expect("exited reservation should be reclaimed");
    let reserved = service
        .container_id_for_reserved_name(&name_key)
        .expect("name should be reserved for the new create attempt");
    assert_eq!(reserved, "fresh-container");
    assert!(service
        .containers
        .lock()
        .await
        .get("exited-container")
        .is_none());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("exited-container")
        .unwrap()
        .is_none());
    drop(guard);
}

#[tokio::test]
async fn create_container_rejects_empty_image_ref() {
    let service = test_service();
    service.pod_sandboxes.lock().await.insert(
        "pod-empty-image".to_string(),
        test_pod("pod-empty-image", HashMap::new()),
    );

    let err = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-empty-image".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "empty-image".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "   ".to_string(),
                    user_specified_image: String::new(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect_err("empty image ref should be rejected");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("Image.Image is empty"));
    let pod_metadata = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-empty-image")
        .and_then(|pod| pod.metadata.clone())
        .unwrap();
    let name_key = RuntimeServiceImpl::container_name_key(
        &ContainerMetadata {
            name: "empty-image".to_string(),
            attempt: 1,
        },
        &pod_metadata,
    );
    assert!(
        service.container_id_for_reserved_name(&name_key).is_none(),
        "invalid request must not reserve container names"
    );
}

#[test]
fn build_container_status_snapshot_preserves_empty_legacy_image_ref() {
    let container = Container {
        id: "legacy-empty-image".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        state: ContainerState::ContainerCreated as i32,
        metadata: Some(ContainerMetadata {
            name: "legacy".to_string(),
            attempt: 1,
        }),
        image: None,
        image_ref: String::new(),
        annotations: HashMap::new(),
        created_at: RuntimeServiceImpl::now_nanos(),
        ..Default::default()
    };

    let status = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerCreated as i32,
    );
    assert_eq!(status.image_ref, "");
    assert_eq!(
        status.image.as_ref().map(|image| image.image.as_str()),
        Some("")
    );
}

#[test]
fn resolve_container_log_path_joins_relative_path_under_sandbox_log_directory() {
    let path = RuntimeServiceImpl::resolve_container_log_path(
        Some("/var/log/pods/default_demo_uid"),
        "workload/0.log",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        path,
        PathBuf::from("/var/log/pods/default_demo_uid").join("workload/0.log")
    );
}

#[test]
fn resolve_container_log_path_rejects_absolute_path_when_sandbox_log_directory_is_set() {
    let err = RuntimeServiceImpl::resolve_container_log_path(
        Some("/var/log/pods/default_demo_uid"),
        "/var/log/pods/default_demo_uid/workload/0.log",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("must be relative"));
}

#[test]
fn resolve_container_log_path_rejects_parent_dir_escape_when_sandbox_log_directory_is_set() {
    let err = RuntimeServiceImpl::resolve_container_log_path(
        Some("/var/log/pods/default_demo_uid"),
        "../escape.log",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("must not escape"));
}

#[test]
fn resolve_container_log_path_requires_absolute_path_without_sandbox_log_directory() {
    let err = RuntimeServiceImpl::resolve_container_log_path(None, "relative.log").unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("must be absolute"));

    let absolute = RuntimeServiceImpl::resolve_container_log_path(
        None,
        "/var/log/pods/default_demo_uid/workload/0.log",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        absolute,
        PathBuf::from("/var/log/pods/default_demo_uid/workload/0.log")
    );
}

#[tokio::test]
async fn recover_state_rebuilds_reserved_pod_and_container_names() {
    let (_dir, service) = test_service_with_fake_runtime();
    let pod_metadata = PodSandboxMetadata {
        name: "recover-pod".to_string(),
        uid: "recover-pod-uid".to_string(),
        namespace: "default".to_string(),
        attempt: 1,
    };
    let pod_name_key = RuntimeServiceImpl::pod_name_key(&pod_metadata);
    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-recover-name".to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-recover-name",
            "ready",
            &pod_metadata.name,
            &pod_metadata.namespace,
            &pod_metadata.uid,
            "",
            &HashMap::new(),
            &pod_annotations,
            Some("pause-recover-name"),
            None,
        )
        .unwrap();

    let container_metadata = ContainerMetadata {
        name: "recover-container".to_string(),
        attempt: 1,
    };
    let container_name_key =
        RuntimeServiceImpl::container_name_key(&container_metadata, &pod_metadata);
    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some(container_metadata.name.clone()),
            metadata_attempt: Some(container_metadata.attempt),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-recover-name",
            "pod-recover-name",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["sleep".to_string(), "1".to_string()],
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert_eq!(
        service.pod_id_for_reserved_name(&pod_name_key).as_deref(),
        Some("pod-recover-name")
    );
    assert_eq!(
        service
            .container_id_for_reserved_name(&container_name_key)
            .as_deref(),
        Some("container-recover-name")
    );
}

#[tokio::test]
async fn run_pod_sandbox_auto_pulls_missing_pause_image_into_shared_image_service() {
    let (_dir, service) = test_service_with_fake_runtime();
    let pulls = Arc::new(StdMutex::new(Vec::new()));
    service.image_service().set_test_pull_handler(Arc::new({
        let pulls = pulls.clone();
        move |request| {
            pulls.lock().unwrap().push(request);
            Ok(crate::image::TestPullResponse {
                image_id: "sha256:pause-auto".to_string(),
                size: 64,
                ..Default::default()
            })
        }
    }));

    let response = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "pause-missing",
            "default",
            "pause-missing-uid",
            HashMap::new(),
        )),
    )
    .await
    .expect("missing local pause image should be auto-pulled");
    assert!(!response.into_inner().pod_sandbox_id.is_empty());

    {
        let calls = pulls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].requested_ref, "registry.k8s.io/pause:3.9");
        assert_eq!(calls[0].canonical_ref, "registry.k8s.io/pause:3.9");
        assert_eq!(calls[0].pull_namespace.as_deref(), Some("default"));
        assert_eq!(calls[0].auth, crate::image::TestRegistryAuth::Anonymous);
    }

    let status = ImageService::image_status(
        &service.image_service(),
        Request::new(crate::proto::runtime::v1::ImageStatusRequest {
            image: Some(crate::proto::runtime::v1::ImageSpec {
                image: "registry.k8s.io/pause:3.9".to_string(),
                user_specified_image: String::new(),
                runtime_handler: String::new(),
                annotations: HashMap::new(),
            }),
            verbose: false,
        }),
    )
    .await
    .expect("shared image service should expose the auto-pulled image")
    .into_inner();
    assert_eq!(
        status.image.expect("pause image should exist").id,
        "sha256:pause-auto"
    );
}

#[tokio::test]
async fn run_pod_sandbox_auto_pull_prefers_namespaced_auth_for_overridden_pause_image() {
    let (_dir, service) = test_service_with_fake_runtime_configure(|config| {
        let auth_root = config.root_dir.join("auth");
        let global_auth_file = auth_root.join("global.json");
        let namespaced_auth_dir = auth_root.join("namespaced");
        fs::create_dir_all(&namespaced_auth_dir).unwrap();
        fs::write(
            &global_auth_file,
            r#"{
                "auths": {
                    "registry.example": {
                        "auth": "Z2xvYmFsOnNlY3JldA=="
                    }
                }
            }"#,
        )
        .unwrap();
        let digest = format!("{:x}", Sha256::digest("registry.example/pause".as_bytes()));
        fs::write(
            namespaced_auth_dir.join(format!("default-{digest}.json")),
            r#"{
                "auths": {
                    "registry.example": {
                        "auth": "bmFtZXNwYWNlOnNlY3JldA=="
                    }
                }
            }"#,
        )
        .unwrap();
        config.image_global_auth_file = global_auth_file;
        config.image_namespaced_auth_dir = namespaced_auth_dir;
    });
    let pulls = Arc::new(StdMutex::new(Vec::new()));
    service.image_service().set_test_pull_handler(Arc::new({
        let pulls = pulls.clone();
        move |request| {
            pulls.lock().unwrap().push(request);
            Ok(crate::image::TestPullResponse {
                image_id: "sha256:pause-custom".to_string(),
                size: 64,
                ..Default::default()
            })
        }
    }));

    RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "pause-custom-missing",
            "default",
            "pause-custom-missing-uid",
            HashMap::from([(
                "io.kubernetes.cri.sandbox-image".to_string(),
                "registry.example/pause:custom".to_string(),
            )]),
        )),
    )
    .await
    .expect("overridden pause image should be auto-pulled with namespaced auth");

    let calls = pulls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].requested_ref, "registry.example/pause:custom");
    assert_eq!(calls[0].canonical_ref, "registry.example/pause:custom");
    assert_eq!(calls[0].pull_namespace.as_deref(), Some("default"));
    assert_eq!(
        calls[0].auth,
        crate::image::TestRegistryAuth::Basic {
            username: "namespace".to_string(),
            password: "secret".to_string(),
        }
    );
}

#[tokio::test]
async fn run_pod_sandbox_rejects_untrusted_workload_when_untrusted_handler_is_not_configured() {
    let (_dir, service) = test_service_with_fake_runtime();
    let err = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(pod_sandbox_request_with_namespace_options(
            "untrusted-missing-handler",
            "default",
            "untrusted-missing-handler-uid",
            HashMap::from([(
                CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
                "true".to_string(),
            )]),
            "",
            NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            },
        )),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err
        .message()
        .contains("unsupported runtime handler: untrusted"));
}

#[tokio::test]
async fn run_pod_sandbox_rejects_untrusted_workload_with_host_access() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());

    let err = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "untrusted-host-access",
            "default",
            "untrusted-host-access-uid",
            HashMap::from([(
                CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
                "true".to_string(),
            )]),
        )),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("host access"));
}

#[tokio::test]
async fn run_pod_sandbox_rejects_untrusted_workload_with_conflicting_handler() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());
    service.config.runtime_handlers.push("kata".to_string());

    let err = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(pod_sandbox_request_with_namespace_options(
            "untrusted-conflicting-handler",
            "default",
            "untrusted-conflicting-handler-uid",
            HashMap::from([(
                CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
                "true".to_string(),
            )]),
            "kata",
            NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            },
        )),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("explicit runtime handler"));
}

#[tokio::test]
async fn stop_and_remove_container_are_idempotent_when_missing() {
    let service = test_service();

    let stop = RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "missing".to_string(),
            timeout: 0,
        }),
    )
    .await;
    assert!(stop.is_ok());

    let remove = RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(remove.is_ok());
}

#[tokio::test]
async fn stop_and_remove_pod_are_idempotent_when_missing() {
    let service = test_service();

    let stop = RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(stop.is_ok());

    let remove = RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(remove.is_ok());
}

#[tokio::test]
async fn exec_validates_container_is_streamable() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let missing = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "missing".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "container-stopped".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    service.containers.lock().await.insert(
        "container-created".to_string(),
        test_container("container-created", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-created", "created");
    let response = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "container-created".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/exec/"));
}

#[tokio::test]
async fn exec_sync_validates_container_is_streamable() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "missing".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-stopped".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.exit_code, 0);
    assert!(response.stdout.is_empty());
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn exec_sync_returns_stdout_and_stderr_when_io_drains_cleanly() {
    let (dir, service) = test_service_with_fake_runtime();

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf hello-stdout; printf hello-stderr >&2".to_string(),
            ],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, b"hello-stdout");
    assert_eq!(response.stderr, b"hello-stderr");
}

#[tokio::test]
async fn exec_sync_prefers_task_shim_rpc_when_socket_is_available() {
    let (dir, service) = test_service_with_fake_runtime();

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let task_socket = dir
        .path()
        .join("shims")
        .join("container-running")
        .join("task.sock");
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_for_thread = running.clone();
    let socket_for_thread = task_socket.clone();
    let handle = std::thread::spawn(move || {
        crate::shim_rpc::server::serve(
            &socket_for_thread,
            running_for_thread,
            Arc::new(TestShimExecSyncHandler),
        )
        .unwrap();
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !task_socket.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("task shim socket was not created before deadline");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec!["echo".to_string(), "from-shim".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, b"shim:echo from-shim");
    assert_eq!(response.stderr, b"shim-stderr");

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = std::os::unix::net::UnixStream::connect(&task_socket);
    handle.join().unwrap();
}

#[tokio::test]
async fn exec_opens_shim_exec_session_when_task_socket_is_available() {
    let (dir, service) = test_service_with_fake_runtime();

    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let task_socket = dir
        .path()
        .join("shims")
        .join("container-running")
        .join("task.sock");
    let marker_path = dir.path().join("exec-session.marker");
    let session_dir = dir.path().join("exec-session");
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_for_thread = running.clone();
    let socket_for_thread = task_socket.clone();
    let marker_for_thread = marker_path.clone();
    let session_dir_for_thread = session_dir.clone();
    let handle = std::thread::spawn(move || {
        crate::shim_rpc::server::serve(
            &socket_for_thread,
            running_for_thread,
            Arc::new(TestShimExecSessionHandler {
                marker_path: marker_for_thread,
                session_dir: session_dir_for_thread,
            }),
        )
        .unwrap();
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !task_socket.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("task shim socket was not created before deadline");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let response = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "container-running".to_string(),
            cmd: vec!["echo".to_string(), "interactive".to_string()],
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.url.contains("/exec/"));
    assert_eq!(
        fs::read_to_string(&marker_path).unwrap(),
        "echo interactive"
    );
    assert!(session_dir.join("io.sock").exists());
    assert!(session_dir.join("resize.sock").exists());

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = std::os::unix::net::UnixStream::connect(&task_socket);
    handle.join().unwrap();
}

#[tokio::test]
async fn exec_sync_returns_deadline_exceeded_when_io_drain_timeout_expires() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path;
    config.exec_sync_io_drain_timeout = Duration::from_millis(50);
    let service = RuntimeServiceImpl::new(config);

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let err = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec![
                "sh".to_string(),
                "-c".to_string(),
                "(trap '' HUP; sleep 5) & printf held-open".to_string(),
            ],
            timeout: 0,
        }),
    )
    .await
    .expect_err("io drain timeout should be surfaced");

    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    assert!(err.message().contains("Exec sync stdout drain timed out"));
}

#[tokio::test]
async fn exec_sync_uses_runtime_binary_for_non_default_handler() {
    let dir = tempdir().unwrap();
    let runc_dir = dir.path().join("runc");
    let kata_dir = dir.path().join("kata");
    fs::create_dir_all(&runc_dir).unwrap();
    fs::create_dir_all(&kata_dir).unwrap();
    let runc_runtime_path = write_fake_runtime_script(&runc_dir);
    let kata_runtime_path = write_fake_runtime_script(&kata_dir);

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runc_runtime_path.clone();
    config.runtime_root = dir.path().join("runtime-root-runc");
    config.runtime_configs = HashMap::from([
        (
            "runc".to_string(),
            crate::config::ResolvedRuntimeHandlerConfig {
                backend: "runc".to_string(),
                runtime_path: runc_runtime_path.display().to_string(),
                runtime_config_path: String::new(),
                runtime_root: config.runtime_root.display().to_string(),
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
                runtime_path: kata_runtime_path.display().to_string(),
                runtime_config_path: String::new(),
                runtime_root: dir.path().join("runtime-root-kata").display().to_string(),
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
    ]);
    let service = RuntimeServiceImpl::new(config);

    let mut annotations = HashMap::new();
    annotations.insert(
        "io.kubernetes.cri-o.RuntimeHandler".to_string(),
        "kata".to_string(),
    );
    annotations.insert(
        "io.containerd.cri.runtime-handler".to_string(),
        "kata".to_string(),
    );
    service.containers.lock().await.insert(
        "container-kata".to_string(),
        test_container("container-kata", "pod-1", annotations),
    );
    fs::write(
        kata_dir.join("runtime-state").join("container-kata.state"),
        "running",
    )
    .unwrap();

    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-kata".to_string(),
            cmd: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf from-kata-runtime".to_string(),
            ],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, b"from-kata-runtime");
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn exec_sync_uses_first_cpu_affinity_from_container_cpuset() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-affinity.sh");
    let affinity_path = dir.path().join("affinity.txt");
    fs::write(
        &runtime_path,
        format!(
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
  exec)
    id="${{1:-}}"
    if [ "$#" -gt 0 ]; then
      shift
    fi
    awk '/Cpus_allowed_list/ {{print $2}}' /proc/self/status > "{affinity_path}"
    printf affinity-ok
    ;;
  *)
    exit 1
    ;;
esac
"#,
            state_dir = dir.path().join("runtime-state").display(),
            affinity_path = affinity_path.display()
        ),
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_root = dir.path().join("runtime-root");
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
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
    );
    config.exec_cpu_affinity = "first".to_string();
    let service = RuntimeServiceImpl::new(config);

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            linux_resources: Some(StoredLinuxResources {
                cpuset_cpus: "0-3".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, b"affinity-ok");
    assert_eq!(fs::read_to_string(&affinity_path).unwrap().trim(), "0");
}

#[tokio::test]
async fn exec_sync_prefers_shared_cpuset_when_annotation_enables_shared_cpu() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-affinity-shared.sh");
    let affinity_path = dir.path().join("affinity.txt");
    fs::write(
        &runtime_path,
        format!(
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
  exec)
    awk '/Cpus_allowed_list/ {{print $2}}' /proc/self/status > "{affinity_path}"
    printf affinity-ok
    ;;
  *)
    exit 1
    ;;
esac
"#,
            state_dir = dir.path().join("runtime-state").display(),
            affinity_path = affinity_path.display()
        ),
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_root = dir.path().join("runtime-root");
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
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
    );
    config.exec_cpu_affinity = "first".to_string();
    config.shared_cpuset = "2-3".to_string();
    let service = RuntimeServiceImpl::new(config);

    let mut annotations = HashMap::new();
    annotations.insert(
        "cpu-shared.crio.io/container-running".to_string(),
        "enable".to_string(),
    );
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            linux_resources: Some(StoredLinuxResources {
                cpuset_cpus: "0-1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap();
    let mut container = test_container("container-running", "pod-1", annotations);
    container.metadata = Some(crate::proto::runtime::v1::ContainerMetadata {
        name: "container-running".to_string(),
        attempt: 1,
    });
    service
        .containers
        .lock()
        .await
        .insert("container-running".to_string(), container);
    set_fake_runtime_state(&dir, "container-running", "running");

    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, b"affinity-ok");
    assert_eq!(fs::read_to_string(&affinity_path).unwrap().trim(), "2");
}

#[tokio::test]
async fn attach_validates_container_is_streamable() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let missing = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "missing".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "container-stopped".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    let attach_socket = dir
        .path()
        .join("attach")
        .join("container-running")
        .join("attach.sock");
    fs::create_dir_all(attach_socket.parent().unwrap()).unwrap();
    fs::write(&attach_socket, "").unwrap();
    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "container-running".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
async fn attach_uses_log_stream_fallback_when_recovered_socket_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "recover-container", "running");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    service.containers.lock().await.insert(
        "recover-container".to_string(),
        test_container("recover-container", "pod-1", annotations),
    );

    service.recover_state().await.unwrap();

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "recover-container".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
async fn attach_uses_log_stream_fallback_for_read_only_tty_when_socket_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "recover-container-tty", "running");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover-tty.log")
                    .display()
                    .to_string(),
            ),
            tty: true,
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "recover-container-tty".to_string(),
        test_container("recover-container-tty", "pod-1", annotations),
    );

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "recover-container-tty".to_string(),
            stdin: false,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn attach_read_only_tty_fallback_does_not_attempt_shim_restore() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let fake_shim_path = dir.path().join("fake-shim-readonly.sh");
    fs::write(
        &fake_shim_path,
        r#"#!/bin/sh
set -eu
id=""
exit_code_file=""
attach_socket_dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id)
      id="${2:-}"
      shift 2
      ;;
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    --attach-socket-dir)
      attach_socket_dir="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -n "$attach_socket_dir" ] && [ -n "$id" ]; then
  shim_dir="$attach_socket_dir/$id"
else
  shim_dir="$(dirname "$exit_code_file")"
fi
mkdir -p "$shim_dir"
: > "$shim_dir/attach.sock"
: > "$shim_dir/resize.sock"
sleep 1
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&fake_shim_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&fake_shim_path, perms).unwrap();

    let service = RuntimeServiceImpl::new_with_shim_work_dir(
        RuntimeConfig {
            root_dir: dir.path().join("root"),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_configs: HashMap::from([(
                "runc".to_string(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    runtime_path: runtime_path.display().to_string(),
                    runtime_config_path: String::new(),
                    runtime_root: dir.path().join("runtime-root").display().to_string(),
                    platform_runtime_paths: HashMap::new(),
                    monitor_path: fake_shim_path.display().to_string(),
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
            cni_config: crate::network::CniConfig::default(),
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
                shim_path: fake_shim_path.clone(),
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
                runtime_path: PathBuf::from("/definitely/missing/runc"),
                max_container_log_line_size: 4096,
                state_db_path: dir.path().join("root").join("crius.db"),
            },
            streaming: crate::streaming::StreamingConfig::default(),
            config_path: None,
            ..test_runtime_config(dir.path().join("root"))
        },
        NriConfig::default(),
        shim_work_dir.clone(),
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "tty-readonly-no-restore", "running");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            tty: true,
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("tty-readonly.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "tty-readonly-no-restore".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("tty-readonly-no-restore", "pod-1", annotations)
        },
    );

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "tty-readonly-no-restore".to_string(),
            stdin: false,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
    assert!(
        !dir.path()
            .join("attach")
            .join("tty-readonly-no-restore")
            .join("attach.sock")
            .exists(),
        "read-only tty fallback should not recreate attach shim sockets"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn attach_recreates_tty_shim_when_socket_is_missing() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let fake_shim_path = dir.path().join("fake-shim.sh");
    fs::write(
        &fake_shim_path,
        r#"#!/bin/sh
set -eu
id=""
exit_code_file=""
attach_socket_dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id)
      id="${2:-}"
      shift 2
      ;;
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    --attach-socket-dir)
      attach_socket_dir="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -n "$attach_socket_dir" ] && [ -n "$id" ]; then
  shim_dir="$attach_socket_dir/$id"
else
  shim_dir="$(dirname "$exit_code_file")"
fi
mkdir -p "$shim_dir"
: > "$shim_dir/attach.sock"
: > "$shim_dir/resize.sock"
sleep 1
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&fake_shim_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&fake_shim_path, perms).unwrap();

    let service = RuntimeServiceImpl::new_with_shim_work_dir(
        RuntimeConfig {
            root_dir: dir.path().join("root"),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_configs: HashMap::from([(
                "runc".to_string(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    runtime_path: runtime_path.display().to_string(),
                    runtime_config_path: String::new(),
                    runtime_root: dir.path().join("runtime-root").display().to_string(),
                    platform_runtime_paths: HashMap::new(),
                    monitor_path: fake_shim_path.display().to_string(),
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
            cni_config: crate::network::CniConfig::default(),
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
                shim_path: fake_shim_path.clone(),
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
                runtime_path: PathBuf::from("/definitely/missing/runc"),
                max_container_log_line_size: 4096,
                state_db_path: dir.path().join("root").join("crius.db"),
            },
            streaming: crate::streaming::StreamingConfig::default(),
            config_path: None,
        },
        NriConfig::default(),
        shim_work_dir.clone(),
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "tty-restore", "running");
    let bundle_dir = dir.path().join("runtime-root").join("tty-restore");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "process": {
                "terminal": true
            }
        })
        .to_string(),
    )
    .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            tty: true,
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "tty-restore".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("tty-restore", "pod-1", annotations)
        },
    );

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "tty-restore".to_string(),
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
    assert!(dir
        .path()
        .join("attach")
        .join("tty-restore")
        .join("attach.sock")
        .exists());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn attach_recovers_tty_shim_after_recover_state_when_socket_is_missing() {
    let _guard = env_lock().lock().await;
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let fake_shim_path = dir.path().join("fake-shim-recover.sh");
    fs::write(
        &fake_shim_path,
        r#"#!/bin/sh
set -eu
id=""
exit_code_file=""
attach_socket_dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id)
      id="${2:-}"
      shift 2
      ;;
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    --attach-socket-dir)
      attach_socket_dir="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -n "$attach_socket_dir" ] && [ -n "$id" ]; then
  shim_dir="$attach_socket_dir/$id"
else
  shim_dir="$(dirname "$exit_code_file")"
fi
mkdir -p "$shim_dir"
: > "$shim_dir/attach.sock"
: > "$shim_dir/resize.sock"
sleep 1
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&fake_shim_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&fake_shim_path, perms).unwrap();

    let service = RuntimeServiceImpl::new_with_shim_work_dir(
        RuntimeConfig {
            root_dir: dir.path().join("root"),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_configs: HashMap::from([(
                "runc".to_string(),
                crate::config::ResolvedRuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    runtime_path: runtime_path.display().to_string(),
                    runtime_config_path: String::new(),
                    runtime_root: dir.path().join("runtime-root").display().to_string(),
                    platform_runtime_paths: HashMap::new(),
                    monitor_path: fake_shim_path.display().to_string(),
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
            cni_config: crate::network::CniConfig::default(),
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
                shim_path: fake_shim_path.clone(),
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
                runtime_path: PathBuf::from("/definitely/missing/runc"),
                max_container_log_line_size: 4096,
                state_db_path: dir.path().join("root").join("crius.db"),
            },
            streaming: crate::streaming::StreamingConfig::default(),
            config_path: None,
        },
        NriConfig::default(),
        shim_work_dir.clone(),
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "tty-recover-after-restart", "running");
    let bundle_dir = dir
        .path()
        .join("runtime-root")
        .join("tty-recover-after-restart");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "process": {
                "terminal": true
            }
        })
        .to_string(),
    )
    .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            tty: true,
            metadata_name: Some("tty-recover-after-restart".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "tty-recover-after-restart",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "tty-recover-after-restart".to_string(),
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
    assert!(dir
        .path()
        .join("attach")
        .join("tty-recover-after-restart")
        .join("attach.sock")
        .exists());
}

#[tokio::test]
async fn attach_returns_error_when_socket_missing_and_interactive_recovery_is_requested() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "recover-container", "running");
    service.containers.lock().await.insert(
        "recover-container".to_string(),
        test_container("recover-container", "pod-1", HashMap::new()),
    );

    let err = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "recover-container".to_string(),
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("interactive recovery"));
}

#[tokio::test]
async fn container_status_verbose_returns_json_info() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some("/var/log/pods/c1.log".to_string()),
            tty: true,
            privileged: true,
            seccomp_notifier_action: Some("stop".to_string()),
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                image: String::new(),
                image_sub_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
            run_as_user: Some("1000".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-1".to_string(),
        test_container("container-1", "pod-1", annotations),
    );
    service
        .ensure_seccomp_notifier("container-1", crate::runtime::SeccompNotifierMode::Stop)
        .unwrap();
    set_fake_runtime_state(&dir, "container-1", "running");
    let bundle_dir = dir.path().join("runtime-root").join("container-1");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "process": {
                "terminal": true,
                "cwd": "/"
            }
        })
        .to_string(),
    )
    .unwrap();

    let response = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-1".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.info.contains_key("info"));
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["id"], "container-1");
    assert_eq!(info["sandboxID"], "pod-1");
    assert_eq!(info["privileged"], true);
    assert_eq!(info["user"], "1000");
    assert_eq!(info["seccompNotifierAction"], "stop");
    assert_eq!(info["runtimeSpec"]["process"]["cwd"], "/");
    assert_eq!(response.status.unwrap().mounts.len(), 1);
}

#[tokio::test]
async fn pod_sandbox_status_verbose_returns_json_info() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    annotations.insert(
        "io.kubernetes.cri.sandbox-image".to_string(),
        "registry.k8s.io/pause:3.10".to_string(),
    );
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            hostname: Some("pod-hostname".to_string()),
            port_mappings: vec![StoredPortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: "127.0.0.1".to_string(),
            }],
            supplemental_groups: vec![2000, 2001],
            raw_cni_result: Some(serde_json::json!({
                "cniVersion": "1.0.0",
                "ips": [
                    {"address": "10.88.0.10/16"},
                    {"address": "10.88.0.11/16"}
                ]
            })),
            ip: Some("10.88.0.10".to_string()),
            additional_ips: vec!["10.88.0.11".to_string()],
            netns_path: Some("/var/run/netns/test-pod".to_string()),
            pause_container_id: Some("pause-1".to_string()),
            log_directory: Some("/var/log/pods/test-pod".to_string()),
            readonly_rootfs: true,
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", annotations));
    set_fake_runtime_state(&dir, "pause-1", "running");
    let bundle_dir = dir.path().join("runtime-root").join("pause-1");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": "rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-1".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.info.contains_key("info"));
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["id"], "pod-1");
    assert_eq!(info["hostname"], "pod-hostname");
    assert_eq!(info["ip"], "10.88.0.10");
    assert_eq!(info["additionalIPs"][0], "10.88.0.11");
    assert_eq!(info["supplementalGroups"][0], 2000);
    assert_eq!(info["supplementalGroups"][1], 2001);
    assert_eq!(info["readonlyRootfs"], true);
    assert_eq!(info["rawCniResult"]["cniVersion"], "1.0.0");
    assert_eq!(info["portMappings"][0]["protocol"], "TCP");
    assert_eq!(info["portMappings"][0]["containerPort"], 80);
    assert_eq!(info["portMappings"][0]["hostPort"], 8080);
    assert_eq!(info["image"], "registry.k8s.io/pause:3.10");
    assert_eq!(info["pauseContainerId"], "pause-1");
    assert!(info["pid"].is_number());
    assert_eq!(info["runtimeSpec"]["root"]["path"], "rootfs");
}

#[tokio::test]
async fn pod_sandbox_status_verbose_normalizes_ip_fields_from_stored_state() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-ip".to_string()),
            additional_ips: vec![
                "fd00::10".to_string(),
                "10.88.0.11".to_string(),
                "fd00::10".to_string(),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-ip".to_string(), test_pod("pod-ip", annotations));

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-ip".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();

    assert_eq!(info["ip"], "fd00::10");
    assert_eq!(info["additionalIPs"][0], "10.88.0.11");
}

#[tokio::test]
async fn pod_sandbox_status_snapshot_uses_stored_ip_as_network_status() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            ip: Some("10.88.0.20".to_string()),
            additional_ips: vec!["fd00::20".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-net".to_string(), test_pod("pod-net", annotations));

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-net".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let network = response
        .status
        .and_then(|status| status.network)
        .expect("network status should be populated from stored state");
    assert_eq!(network.ip, "10.88.0.20");
    assert_eq!(network.additional_ips[0].ip, "fd00::20");
}

#[tokio::test]
async fn status_verbose_returns_structured_config() {
    let mut service = test_service();
    service.config.grpc_max_send_msg_size = 12_345;
    service.config.grpc_max_recv_msg_size = 23_456;
    service.config.streaming.enable_tls = true;
    service.config.streaming.tls_cert_file = "/etc/crius/tls/tls.crt".to_string();
    service.config.streaming.tls_key_file = "/etc/crius/tls/tls.key".to_string();
    service.config.streaming.tls_ca_file = "/etc/crius/tls/ca.crt".to_string();
    service.config.streaming.tls_min_version = "VersionTLS13".to_string();
    service.config.privileged_seccomp_profile = "unconfined".to_string();
    service.config.apparmor_default_profile = "crius-default".to_string();
    service.config.disable_apparmor = true;
    service.config.enable_selinux = true;
    service.config.selinux_category_range = 64;
    service.config.hostnetwork_disable_selinux = false;
    service.nri_config.enable = true;
    service.nri_config.enable_cdi = false;
    service.nri_config.cdi_spec_dirs = vec!["/etc/cdi".to_string(), "/var/run/cdi".to_string()];
    service.nri_config.plugin_path = "/opt/nri/plugins".to_string();
    service.nri_config.plugin_config_path = "/etc/nri/conf.d".to_string();
    service.nri_config.blockio_config_path = "/etc/nri/blockio.json".to_string();
    service.config.default_env = vec![
        (
            "HTTP_PROXY".to_string(),
            "http://proxy.internal".to_string(),
        ),
        ("LANG".to_string(), "C.UTF-8".to_string()),
    ];
    service.config.default_capabilities =
        vec!["CAP_CHOWN".to_string(), "CAP_NET_BIND_SERVICE".to_string()];
    service.config.default_sysctls =
        HashMap::from([("kernel.shm_rmid_forced".to_string(), "1".to_string())]);
    service.config.image_default_transport = "docker://".to_string();
    service.config.image_short_name_mode = "enforcing".to_string();
    service.config.image_pull_progress_timeout = std::time::Duration::from_secs(25);
    service.config.image_max_concurrent_downloads = 5;
    service.config.image_pull_retry_count = 2;
    service.config.image_registry_config_dir = PathBuf::from("/etc/containerd/certs.d");
    service.config.image_storage_options = Vec::new();
    service.config.image_volumes = "bind".to_string();
    service.config.image_pinned_images = vec![
        "busybox*".to_string(),
        "registry.k8s.io/pause:3.9".to_string(),
    ];
    service.config.hooks_dir = vec![
        PathBuf::from("/usr/share/containers/oci/hooks.d"),
        PathBuf::from("/etc/crius/hooks.d"),
    ];
    service.config.infra_ctr_cpuset = "1".to_string();
    service.config.shared_cpuset = "2-3".to_string();
    service.config.no_new_keyring = true;
    service.config.allowed_devices = vec![PathBuf::from("/dev/null"), PathBuf::from("/dev/zero")];
    service.config.additional_devices = vec![crate::runtime::DeviceMapping {
        source: PathBuf::from("/dev/null"),
        destination: PathBuf::from("/dev/custom-null"),
        permissions: "rw".to_string(),
    }];
    service.config.device_ownership_from_security_context = true;
    service.config.default_mounts_file = PathBuf::from("/etc/containers/mounts.conf");
    service.config.absent_mount_sources_to_reject = vec![PathBuf::from("/etc/hostname")];
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .container_create_timeout = 45;
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .runtime_config_path = "/etc/kata/config.toml".to_string();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .platform_runtime_paths = HashMap::from([(
        "linux/amd64".to_string(),
        "/usr/bin/kata-runtime-amd64".to_string(),
    )]);
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .monitor_cgroup = "system.slice".to_string();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .stream_websockets = true;
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .allowed_annotations = vec!["io.example.runtime/".to_string()];
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .default_annotations =
        HashMap::from([("io.example.runtime/default".to_string(), "kata".to_string())]);
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .privileged_without_host_devices = true;
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .privileged_without_host_devices_all_devices_allowed = true;
    service
        .config
        .cni_config
        .set_netns_mount_dir(PathBuf::from("/tmp/crius-test-runtime-root/netns"));
    service
        .config
        .cni_config
        .set_netns_mounts_under_state_dir(true);
    service
        .config
        .cni_config
        .set_namespace_helper_path(Some(PathBuf::from("/usr/bin/pinns")));
    service
        .config
        .cni_config
        .set_handler_config_dirs("kata", vec![PathBuf::from("/etc/cni/kata.d")]);
    service
        .config
        .cni_config
        .set_handler_max_conf_num("kata", 2);
    service.config.rootless = crate::rootless::EffectiveRootlessConfig {
        enabled: true,
        current_uid: 1000,
        current_gid: 1000,
        in_user_namespace: true,
        xdg_runtime_dir: PathBuf::from("/run/user/1000"),
        xdg_data_home: PathBuf::from("/home/test/.local/share"),
        storage_root: PathBuf::from("/home/test/.local/share/crius/storage"),
        runtime_root: PathBuf::from("/run/user/1000/crius"),
        netns_dir: PathBuf::from("/run/user/1000/crius/netns"),
        use_fuse_overlayfs: true,
        network_mode: crate::rootless::NetworkMode::Pasta,
        slirp4netns_path: PathBuf::from("/usr/bin/slirp4netns"),
        pasta_path: PathBuf::from("/usr/bin/pasta"),
        disable_cgroup: true,
        tolerate_missing_hugetlb_controller: true,
    };
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.info.contains_key("config"));
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeName"], "crius");
    assert_eq!(config["runtimeVersion"], env!("CARGO_PKG_VERSION"));
    assert!(config["ociRuntimeVersion"].is_null());
    assert_eq!(config["defaultRuntimeHandler"], "runc");
    assert!(!config["runtimeHandlers"].as_array().unwrap().is_empty());
    assert_eq!(config["imageRoot"], "/tmp/crius-test-images");
    assert_eq!(config["imageDriver"], "overlay");
    assert_eq!(config["imageGlobalAuthFile"], "");
    assert_eq!(config["imageNamespacedAuthDir"], "");
    assert_eq!(config["imageDefaultTransport"], "docker://");
    assert_eq!(config["imageShortNameMode"], "enforcing");
    assert_eq!(config["imagePullProgressTimeoutMillis"], 25000);
    assert_eq!(config["imageMaxConcurrentDownloads"], 5);
    assert_eq!(config["imagePullRetryCount"], 2);
    assert_eq!(config["imageRegistryConfigDir"], "/etc/containerd/certs.d");
    assert_eq!(config["rootless"]["enabled"], true);
    assert_eq!(config["rootless"]["networkMode"], "pasta");
    assert_eq!(config["rootless"]["xdgRuntimeDir"], "/run/user/1000");
    assert_eq!(
        config["rootless"]["storageRoot"],
        "/home/test/.local/share/crius/storage"
    );
    assert_eq!(config["imageStorageOptions"], serde_json::json!([]));
    assert_eq!(config["imageVolumes"], "bind");
    assert_eq!(
        config["pinnedImages"],
        serde_json::json!(["busybox*", "registry.k8s.io/pause:3.9"])
    );
    assert_eq!(config["imageLayout"]["mode"], "single-store-root");
    assert_eq!(
        config["imageLayout"]["imageRecordPathPattern"],
        "/tmp/crius-test-images/images/<imageID>"
    );
    assert_eq!(config["imageLayout"]["separateImageStoreSupported"], false);
    assert_eq!(
        config["imageSnapshotModel"]["snapshotter"],
        "internal-overlay-untar"
    );
    assert_eq!(
        config["imageSnapshotModel"]["externalSnapshotterSupported"],
        false
    );
    assert_eq!(
        config["imageSnapshotModel"]["runtimeSnapshotterOverrideSupported"],
        true
    );
    assert_eq!(
        config["imageSnapshotModel"]["snapshotAnnotationPassthrough"],
        false
    );
    assert_eq!(config["imageSnapshotModel"]["discardUnpackedLayers"], false);
    assert_eq!(config["imageSnapshotModel"]["pullOptionPassthrough"], false);
    assert_eq!(
        config["snapshotStatsCollection"]["strategy"],
        "on-demand-rootfs-walk"
    );
    assert_eq!(
        config["snapshotStatsCollection"]["backgroundCollector"],
        false
    );
    assert!(config["cgroupSupport"]["drivers"]["systemd"]["supported"]
        .as_bool()
        .unwrap());
    assert!(config["cgroupSupport"]["drivers"]["cgroupfs"]["supported"]
        .as_bool()
        .unwrap());
    assert_eq!(
        config["cgroupSupport"]["resourceUpdateStrategy"],
        "runtime-update-resources"
    );
    assert_eq!(
        config["cgroupSupport"]["resourceClasses"]["blockio"]["supported"],
        true
    );
    assert_eq!(
        config["cgroupSupport"]["resourceClasses"]["blockio"]["configPath"],
        "/etc/nri/blockio.json"
    );
    assert!(config["cgroupSupport"]["resourceClasses"]["rdt"]["supported"].is_boolean());
    assert_eq!(
        config["cgroupSupport"]["resourceClasses"]["rdt"]["resctrlPath"],
        "/sys/fs/resctrl"
    );
    assert_eq!(
        config["recovery"]["policy"]["repairScope"],
        "sqlite-persistence-only"
    );
    assert!(config["recovery"]["policy"]["wipeScope"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "orphanShimArtifacts"));
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["runtimePath"],
        "/definitely/missing/kata-runtime"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["monitorPath"],
        "/definitely/missing/crius-shim"
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["available"]
            .is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["idmapMounts"]
            .is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]
            ["recursiveReadOnlyMounts"]
            .is_boolean()
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeConfigPath"],
        "/etc/kata/config.toml"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["platformRuntimePaths"]["linux/amd64"],
        "/usr/bin/kata-runtime-amd64"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["monitorCgroup"],
        "system.slice"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["streamWebsockets"],
        true
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["privilegedWithoutHostDevices"],
        true
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["privilegedWithoutHostDevicesAllDevicesAllowed"],
        true
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["runc"]["runtimeRoot"],
        "/tmp/crius-test-runtime-root"
    );
    assert!(config["monitorEnv"].as_array().unwrap().is_empty());
    assert_eq!(config["defaultEnv"][0], "HTTP_PROXY=http://proxy.internal");
    assert_eq!(config["defaultCapabilities"][0], "CAP_CHOWN");
    assert_eq!(config["defaultSysctls"]["kernel.shm_rmid_forced"], "1");
    assert_eq!(config["allowedDevices"][0], "/dev/null");
    assert_eq!(
        config["additionalDevices"][0],
        "/dev/null:/dev/custom-null:rw"
    );
    assert_eq!(config["deviceOwnershipFromSecurityContext"], true);
    assert_eq!(config["defaultMountsFile"], "/etc/containers/mounts.conf");
    assert_eq!(
        config["hooksDir"],
        serde_json::json!(["/usr/share/containers/oci/hooks.d", "/etc/crius/hooks.d"])
    );
    assert_eq!(config["absentMountSourcesToReject"][0], "/etc/hostname");
    assert_eq!(config["grpcMaxSendMsgSize"], 12345);
    assert_eq!(config["grpcMaxRecvMsgSize"], 23456);
    assert_eq!(config["privilegedSeccompProfile"], "unconfined");
    assert_eq!(config["apparmorDefaultProfile"], "crius-default");
    assert_eq!(config["disableApparmor"], true);
    assert_eq!(config["enableSelinux"], true);
    assert_eq!(config["selinuxCategoryRange"], 64);
    assert_eq!(config["hostnetworkDisableSelinux"], false);
    assert_eq!(config["streaming"]["enableTls"], true);
    assert_eq!(config["streaming"]["tlsCertFile"], "/etc/crius/tls/tls.crt");
    assert_eq!(config["streaming"]["tlsMinVersion"], "VersionTLS13");
    assert_eq!(
        config["reload"]["strategy"],
        "config-file-watch-and-cni-watch"
    );
    assert_eq!(config["reload"]["signalReload"], false);
    assert_eq!(config["reload"]["configFileWatch"], false);
    assert_eq!(
        config["reload"]["current"]["pauseImage"],
        "registry.k8s.io/pause:3.9"
    );
    assert!(config["reload"]["reloadableFields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "network.conf_template"));
    assert_eq!(
        config["reload"]["runtimeConfigApiOnly"][0],
        "UpdateRuntimeConfig.network_config.pod_cidr"
    );
    assert!(config["runtimeHandlerConfigs"]["runc"]["monitorEnv"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(config["runtimeHandlerConfigs"]["kata"]["monitorEnv"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["allowedAnnotations"][0],
        "io.example.runtime/"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["defaultAnnotations"]["io.example.runtime/default"],
        "kata"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["containerCreateTimeoutSeconds"],
        45
    );
    assert_eq!(
        config["netnsMountDir"],
        "/tmp/crius-test-runtime-root/netns"
    );
    assert_eq!(config["netnsMountsUnderStateDir"], true);
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["cniConfDir"],
        "/etc/cni/kata.d"
    );
    assert_eq!(config["runtimeHandlerConfigs"]["kata"]["cniMaxConfNum"], 2);
    assert_eq!(config["runtimeHandlerConfigs"]["runc"]["cniMaxConfNum"], 0);
    assert!(config["runtimeHandlerConfigs"]["runc"]["cniConfDir"].is_null());
    assert!(config["securityAvailability"]["selinuxAvailable"].is_boolean());
    assert!(config["securityAvailability"]["apparmorAvailable"].is_boolean());
    assert!(config["securityAvailability"]["seccompAvailable"].is_boolean());
    assert!(config["securityAvailability"]["seccompNotifierSupported"].is_boolean());
    assert!(config["securityAvailability"]["seccompNotifierBaseDir"].is_string());
    assert!(config["securityAvailability"]["seccompNotifierActiveContainers"].is_array());
    assert_eq!(config["imageDecryption"]["enabled"], false);
    assert_eq!(config["imageDecryption"]["keyModel"], "");
    assert_eq!(config["nri"]["enabled"], true);
    assert_eq!(config["nri"]["enableCdi"], false);
    assert_eq!(config["nri"]["cdiSpecDirs"][0], "/etc/cdi");
    assert_eq!(config["nri"]["pluginPath"], "/opt/nri/plugins");
    assert_eq!(config["nri"]["pluginConfigPath"], "/etc/nri/conf.d");
    assert_eq!(config["nri"]["blockioConfigPath"], "/etc/nri/blockio.json");
    assert_eq!(
        config["workloads"]
            .as_object()
            .map(|workloads| workloads.len()),
        Some(0)
    );
    assert_eq!(config["attachSocketDir"], "/tmp/crius-test-attach");
    assert_eq!(config["containerExitsDir"], "/tmp/crius-test-exits");
    assert_eq!(config["containerStopTimeoutSeconds"], 30);
    assert_eq!(
        config["cleanShutdownFile"],
        "/tmp/crius-test-clean.shutdown"
    );
    assert_eq!(config["versionFile"], "/tmp/crius-test-version");
    assert_eq!(
        config["versionFilePersist"],
        "/tmp/crius-test-version-persist"
    );
    assert_eq!(config["criuPath"], "");
    assert_eq!(config["criuImagePath"], "");
    assert_eq!(config["criuWorkPath"], "");
    assert_eq!(config["enableCriuSupport"], true);
    assert_eq!(config["internalWipe"], true);
    assert_eq!(config["internalRepair"], true);
    assert_eq!(config["bindMountPrefix"], "");
    assert!(config["uidMappings"].as_array().unwrap().is_empty());
    assert!(config["gidMappings"].as_array().unwrap().is_empty());
    assert_eq!(config["minimumMappableUid"], -1);
    assert_eq!(config["minimumMappableGid"], -1);
    assert_eq!(config["ioUid"], 0);
    assert_eq!(config["ioGid"], 0);
    assert_eq!(config["disableCgroup"], false);
    assert_eq!(config["tolerateMissingHugetlbController"], true);
    assert_eq!(config["separatePullCgroup"], "");
    assert_eq!(config["pidsLimit"], -1);
    assert_eq!(config["infraCtrCpuset"], "1");
    assert_eq!(config["sharedCpuset"], "2-3");
    assert_eq!(config["execCpuAffinity"], "");
    assert_eq!(config["readOnly"], false);
    assert_eq!(config["noPivot"], false);
    assert_eq!(config["noNewKeyring"], true);
    assert_eq!(config["pauseImage"], "registry.k8s.io/pause:3.9");
    assert_eq!(config["pauseCommand"], "/pause");
    assert_eq!(config["dropInfraCtr"], false);
    assert_eq!(config["pinnsPath"], "/usr/bin/pinns");
    assert_eq!(
        config["shimPidfilePattern"],
        "/tmp/crius-test-shims/<containerID>/shim.pid"
    );
    assert_eq!(config["logToJournald"], false);
    assert_eq!(config["noSyncLog"], false);
    assert_eq!(config["enablePodEvents"], true);
    assert_eq!(config["includedPodMetrics"], serde_json::json!(["all"]));
    assert_eq!(config["statsCollectionPeriodSeconds"], 0);
    assert_eq!(config["podSandboxMetricsCollectionPeriodSeconds"], 0);
    assert_eq!(config["restrictOomScoreAdj"], false);
    assert_eq!(config["enableUnprivilegedPorts"], false);
    assert_eq!(config["enableUnprivilegedIcmp"], false);
    assert_eq!(config["cniMaxConfNum"], 0);
    assert!(config["cniConfTemplate"].is_null());
    assert_eq!(config["cniIpPref"], "cni");
    assert_eq!(config["disableHostportMapping"], false);
    assert_eq!(config["maxContainerLogLineSize"], 4096);
    assert_eq!(config["execSyncIoDrainTimeoutMillis"], 0);
    assert_eq!(config["streaming"]["address"], "127.0.0.1");
    assert_eq!(config["streaming"]["port"], 0);
    assert_eq!(config["streaming"]["requestTokenTtlSeconds"], 30);
    assert_eq!(
        config["streaming"]["portForwardStreamCreationTimeoutSeconds"],
        30
    );
    assert_eq!(config["streaming"]["portForwardIdleTimeoutSeconds"], 14400);
    assert!(config["lastCniLoadStatus"]["checked_at_unix_millis"].is_number());
    assert!(config["lastCniLoadStatus"]["ready"].is_boolean());
    assert!(config["lastCniLoadStatus"]["reason"].is_string());
    assert!(config["lastCniLoadStatus"]["message"].is_string());
    assert_eq!(config["recovery"]["startupReconcile"], true);
    assert_eq!(config["recovery"]["eventReplayOnRecovery"], false);
    assert!(config["recovery"]["lastStartupWasCleanShutdown"].is_null());
    assert!(config["recovery"]["lastStartupDetectedReboot"].is_null());
    assert!(config["recovery"]["lastStartupDetectedUpgrade"].is_null());
    assert!(config["recovery"]["lastStartupAttemptedRepair"].is_null());
    assert!(config["recovery"]["lastStartupRepairSucceeded"].is_null());
    assert_eq!(config["recovery"]["internalWipe"], true);
    assert_eq!(config["recovery"]["internalRepair"], true);
    assert_eq!(config["runtimeFeatures"]["updateContainerResources"], true);
    assert_eq!(config["runtimeFeatures"]["containerStats"], true);
    assert_eq!(config["runtimeFeatures"]["podSandboxStats"], true);
    assert_eq!(config["runtimeFeatures"]["podSandboxMetrics"], true);
    assert_eq!(config["runtimeFeatures"]["podLifecycleEvents"], false);
    assert_eq!(config["runtimeFeatures"]["checkpointContainer"], true);
    assert_eq!(
        response.status.unwrap().conditions.len(),
        2,
        "expected runtime and network conditions"
    );
}

#[test]
fn reloadable_config_diff_reports_changed_fields() {
    let service = test_service();
    let mut next = service.current_reloadable_config();
    next.pause_image = "registry.example/pause:v2".to_string();
    next.pinned_images = vec!["registry.example/*".to_string()];
    next.cni_conf_template = Some(PathBuf::from("/etc/crius/cni.template"));

    let changed = service.current_reloadable_config().diff_fields(&next);

    assert!(changed.contains(&"runtime.pause_image".to_string()));
    assert!(changed.contains(&"image.pinned_images".to_string()));
    assert!(changed.contains(&"network.conf_template".to_string()));
}

#[tokio::test]
async fn apply_reloadable_config_updates_image_security_and_network_state() {
    let service = test_service();
    let dir = tempdir().unwrap();
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();
    let seccomp_profile = dir.path().join("seccomp.json");
    fs::write(&seccomp_profile, "{}").unwrap();

    let mut next = service.current_reloadable_config();
    next.pause_image = "registry.example/pause:v2".to_string();
    next.pinned_images = vec!["registry.example/*".to_string()];
    next.registry_config_dir = dir.path().join("certs.d");
    next.global_auth_file = dir.path().join("auth.json");
    next.namespaced_auth_dir = dir.path().join("auth.d");
    next.signature_policy = dir.path().join("policy.json");
    next.signature_policy_dir = dir.path().join("policies");
    next.decryption_keys_path = dir.path().join("ocicrypt");
    next.decryption_decoder_path = "/usr/bin/ctd-decoder".to_string();
    next.decryption_keyprovider_config = dir.path().join("keyprovider.json");
    next.seccomp_profile = seccomp_profile.clone();
    next.apparmor_default_profile = "crius-reloaded".to_string();
    next.cni_config_dirs = vec![cni_dir.clone()];
    next.cni_max_conf_num = 1;
    next.cni_default_network_name = Some("reloaded-net".to_string());

    let changed = service
        .apply_reloadable_config(next.clone(), "test")
        .await
        .unwrap();

    assert!(changed.contains(&"runtime.pause_image".to_string()));
    assert!(changed.contains(&"security.seccomp_profile".to_string()));
    assert!(changed.contains(&"network.config_dirs".to_string()));
    assert_eq!(service.current_reloadable_config(), next);
    let image_config = service.image_service().reloadable_config_snapshot();
    assert_eq!(
        image_config.registry_config_dir.as_deref(),
        Some(dir.path().join("certs.d").as_path())
    );
    assert_eq!(
        image_config.pinned_image_patterns,
        vec!["registry.example/*".to_string()]
    );
    assert_eq!(
        service.current_reloadable_config().apparmor_default_profile,
        "crius-reloaded"
    );
    match service.effective_seccomp_profile_from_proto(None, "", false) {
        Some(crate::runtime::SeccompProfile::Localhost(path)) => {
            assert_eq!(path, seccomp_profile);
        }
        other => panic!("expected localhost seccomp profile, got {other:?}"),
    }
    let cni_config = service.current_cni_config();
    assert_eq!(cni_config.config_dirs(), &[cni_dir]);
    assert_eq!(cni_config.max_conf_num(), 1);
    assert_eq!(cni_config.default_network_name(), Some("reloaded-net"));
    assert_eq!(
        service.current_reload_state().last_reload_source.as_deref(),
        Some("test")
    );
}

#[tokio::test]
async fn reload_config_file_once_applies_reloadable_subset() {
    let mut service = test_service();
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("crius.toml");
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();

    let mut config = crate::config::Config::default();
    config.runtime.pause_image = "registry.example/pause:v3".to_string();
    config.image.pinned_images = vec!["registry.example/static:*".to_string()];
    config.image.registry_config_dir = dir.path().join("certs").display().to_string();
    config.security.apparmor_default_profile = "crius-file-reload".to_string();
    config.network.config_dirs = vec![cni_dir.display().to_string()];
    config.network.max_conf_num = 2;
    fs::write(&config_path, toml::to_string_pretty(&config).unwrap()).unwrap();
    service.config.config_path = Some(config_path);

    let changed = service.reload_config_file_once().await.unwrap();

    assert!(changed.contains(&"runtime.pause_image".to_string()));
    assert!(changed.contains(&"image.pinned_images".to_string()));
    assert!(changed.contains(&"network.config_dirs".to_string()));
    let current = service.current_reloadable_config();
    assert_eq!(current.pause_image, "registry.example/pause:v3");
    assert_eq!(
        current.pinned_images,
        vec!["registry.example/static:*".to_string()]
    );
    assert_eq!(current.cni_config_dirs, vec![cni_dir]);
    assert_eq!(
        service.current_reload_state().last_reload_source.as_deref(),
        Some("config-file")
    );
}

#[tokio::test]
async fn reload_cni_watch_once_records_last_error() {
    let service = test_service();
    let dir = tempdir().unwrap();
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();
    fs::write(cni_dir.join("10-broken.conflist"), "{not-json").unwrap();

    let mut next = service.current_reloadable_config();
    next.cni_config_dirs = vec![cni_dir];
    service.apply_reloadable_config(next, "test").await.unwrap();

    let status = service.reload_cni_watch_once().await;

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIConfigInvalid");
    assert!(service
        .current_reload_state()
        .last_cni_watch_error
        .is_some());
}

#[tokio::test]
async fn reload_failure_marks_network_not_ready_condition() {
    let service = test_service();
    service.reload_state.lock().unwrap().last_reload_error = Some("reload failed".to_string());

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: false }))
        .await
        .unwrap()
        .into_inner();

    let network = response
        .status
        .unwrap()
        .conditions
        .into_iter()
        .find(|condition| condition.r#type == "NetworkReady")
        .unwrap();
    assert!(!network.status);
    assert_eq!(network.reason, "ConfigReloadFailed");
    assert_eq!(network.message, "reload failed");
}

#[test]
fn runtime_registry_returns_handler_specific_create_timeout() {
    let runtime = RuntimeRegistry::new(
        "runc".to_string(),
        HashMap::from([(
            "runc".to_string(),
            Arc::new(crate::runtime::RuncBackend::new(RuncRuntime::new(
                PathBuf::from("/definitely/missing/runc"),
                PathBuf::from("/tmp/crius-test-runtime-root"),
            ))) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("runc".to_string(), 240), ("kata".to_string(), 600)]),
    );

    assert_eq!(runtime.container_create_timeout_for_handler(""), 240);
    assert_eq!(runtime.container_create_timeout_for_handler("kata"), 600);
}

#[derive(Debug)]
struct FakeBackend {
    name: &'static str,
    runtime_root: PathBuf,
}

struct TestShimExecSyncHandler;

impl crate::shim_rpc::server::ShimRpcHandler for TestShimExecSyncHandler {
    fn handle_request(
        &self,
        request: crate::shim_rpc::ShimRpcRequest,
    ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
        match request {
            crate::shim_rpc::ShimRpcRequest::Ping => Ok(crate::shim_rpc::ShimRpcResponse::Empty),
            crate::shim_rpc::ShimRpcRequest::ExecProcess(request) => {
                Ok(crate::shim_rpc::ShimRpcResponse::ExecProcess(
                    crate::shim_rpc::ExecProcessResponse {
                        exit_code: 0,
                        stdout: format!("shim:{}", request.command.join(" ")).into_bytes(),
                        stderr: b"shim-stderr".to_vec(),
                    },
                ))
            }
            other => Err(anyhow::anyhow!(
                "unexpected shim exec sync test request: {:?}",
                other
            )),
        }
    }
}

struct TestShimExecSessionHandler {
    marker_path: PathBuf,
    session_dir: PathBuf,
}

impl crate::shim_rpc::server::ShimRpcHandler for TestShimExecSessionHandler {
    fn handle_request(
        &self,
        request: crate::shim_rpc::ShimRpcRequest,
    ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
        match request {
            crate::shim_rpc::ShimRpcRequest::Ping => Ok(crate::shim_rpc::ShimRpcResponse::Empty),
            crate::shim_rpc::ShimRpcRequest::OpenExecSession(request) => {
                fs::write(&self.marker_path, request.command.join(" ")).unwrap();
                fs::create_dir_all(&self.session_dir).unwrap();
                let io_socket_path = self.session_dir.join("io.sock");
                let resize_socket_path = self.session_dir.join("resize.sock");
                let _ = std::os::unix::net::UnixListener::bind(&io_socket_path);
                let _ = std::os::unix::net::UnixListener::bind(&resize_socket_path);
                Ok(crate::shim_rpc::ShimRpcResponse::OpenExecSession(
                    crate::shim_rpc::OpenExecSessionResponse {
                        session_id: "session-1".to_string(),
                        io_socket_path,
                        resize_socket_path: Some(resize_socket_path),
                    },
                ))
            }
            other => Err(anyhow::anyhow!(
                "unexpected shim exec session test request: {:?}",
                other
            )),
        }
    }
}

impl crate::runtime::RuntimeBackend for FakeBackend {
    fn backend_name(&self) -> &str {
        self.name
    }

    fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    fn runtime_path(&self) -> &Path {
        Path::new("/fake/runtime")
    }

    fn runtime_config_path(&self) -> &Path {
        Path::new("/fake/runtime.conf")
    }

    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.runtime_root.join(container_id)
    }

    fn create_container(
        &self,
        _container_id: &str,
        _config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<String> {
        Ok("fake-created".to_string())
    }

    fn start_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop_container(&self, _container_id: &str, _timeout: Option<u32>) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn container_status(
        &self,
        _container_id: &str,
    ) -> anyhow::Result<crate::runtime::ContainerStatus> {
        Ok(crate::runtime::ContainerStatus::Created)
    }

    fn reopen_container_log(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn exec_in_container(
        &self,
        _container_id: &str,
        _command: &[String],
        _tty: bool,
    ) -> anyhow::Result<i32> {
        Ok(0)
    }

    fn update_container_resources(
        &self,
        _container_id: &str,
        _resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_container_paused(&self, _container_id: &str) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn restore_attach_shim(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn shim_status(
        &self,
        _container_id: &str,
    ) -> anyhow::Result<Option<crate::shim_rpc::StatusResponse>> {
        Ok(None)
    }

    fn restore_container_from_checkpoint(
        &self,
        _container_id: &str,
        _checkpoint_path: &Path,
        _work_path: &Path,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn enforce_oom_score_adj_policy(
        &self,
        _spec: &mut crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn prepare_rootfs(
        &self,
        _container_id: &str,
        _config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn build_spec(
        &self,
        _container_id: &str,
        _config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<crate::oci::spec::Spec> {
        Ok(crate::oci::spec::Spec::new("1.0.2"))
    }

    fn write_bundle(
        &self,
        _container_id: &str,
        _rootfs: &Path,
        _spec: &crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn load_spec(&self, _container_id: &str) -> anyhow::Result<crate::oci::spec::Spec> {
        Ok(crate::oci::spec::Spec::new("1.0.2"))
    }

    fn validate_mount_requests(
        &self,
        _config: &crate::runtime::ContainerConfig,
    ) -> std::result::Result<(), crate::runtime::MountSemanticsError> {
        Ok(())
    }

    fn pause_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn checkpoint_container(
        &self,
        _container_id: &str,
        _location: &Path,
        _work_path: &Path,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn resume_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn container_pid(&self, _container_id: &str) -> anyhow::Result<Option<i32>> {
        Ok(None)
    }

    fn probe_runtime_features(&self) -> crate::runtime::RuntimeFeatureProbe {
        crate::runtime::RuntimeFeatureProbe::default()
    }

    fn cgroup_driver(&self) -> crate::config::CgroupDriverConfig {
        crate::config::CgroupDriverConfig::Cgroupfs
    }
}

#[test]
fn runtime_registry_can_store_trait_object_backends() {
    let runtime = RuntimeRegistry::new(
        "fake".to_string(),
        HashMap::from([(
            "fake".to_string(),
            Arc::new(FakeBackend {
                name: "fake",
                runtime_root: PathBuf::from("/tmp/fake-runtime-root"),
            }) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("fake".to_string(), 33)]),
    );

    let backend = runtime.runtime_for_handler("fake").unwrap();
    assert_eq!(backend.backend_name(), "fake");
    assert_eq!(backend.runtime_root(), Path::new("/tmp/fake-runtime-root"));
    assert_eq!(runtime.container_create_timeout_for_handler("fake"), 33);
}

#[tokio::test]
async fn run_container_create_phase_until_returns_deadline_exceeded() {
    let service = test_service();
    let err = service
        .run_container_create_phase_until(
            ContainerCreateDeadline {
                timeout_secs: 1,
                deadline: std::time::Instant::now() + Duration::from_millis(1),
            },
            "prepare_rootfs",
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<_, Status>(())
            },
        )
        .await
        .expect_err("phase exceeding deadline must fail");

    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    assert!(err.message().contains("prepare_rootfs"));
}

#[tokio::test]
async fn status_verbose_reports_cgroup_disable_runtime_feature_state() {
    let mut service = test_service();
    service.config.disable_cgroup = true;

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();

    assert_eq!(config["disableCgroup"], true);
    assert_eq!(config["runtimeFeatures"]["updateContainerResources"], false);
}

#[tokio::test]
async fn version_reports_cri_runtime_identity_when_oci_runtime_version_is_available() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-version.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "runc version 1.2.3"
  exit 0
fi
exit 0
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
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
    );
    let service = RuntimeServiceImpl::new(config);
    let response = RuntimeService::version(
        &service,
        Request::new(VersionRequest {
            version: "0.1.0".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.runtime_name, "crius");
    assert_eq!(response.runtime_version, env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn status_verbose_reports_oci_runtime_version_separately() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-version.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "runc version 1.2.3"
  exit 0
fi
exit 0
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
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
    );
    let service = RuntimeServiceImpl::new(config);
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let payload: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();

    assert_eq!(payload["runtimeName"], "crius");
    assert_eq!(payload["runtimeVersion"], env!("CARGO_PKG_VERSION"));
    assert_eq!(payload["ociRuntimeVersion"], "runc version 1.2.3");
}

#[test]
fn parse_cgroup_hint_from_procfs_prefers_relevant_controller_or_unified_line() {
    let v1 = "11:hugetlb:/\n10:memory:/kubepods.slice/pod123/container.scope\n9:cpuset:/\n";
    assert_eq!(
        RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v1),
        Some(PathBuf::from("/kubepods.slice/pod123/container.scope"))
    );

    let v2 = "0::/user.slice/user-0.slice/session-1.scope\n";
    assert_eq!(
        RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v2),
        Some(PathBuf::from("/user.slice/user-0.slice/session-1.scope"))
    );
}

#[tokio::test]
async fn status_reports_runtime_not_ready_when_binary_is_not_executable() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime");
    fs::write(&runtime_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
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
    );
    let service = RuntimeServiceImpl::new(config);
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: false }))
        .await
        .unwrap()
        .into_inner();
    let runtime_condition = response
        .status
        .unwrap()
        .conditions
        .into_iter()
        .find(|condition| condition.r#type == "RuntimeReady")
        .unwrap();
    assert!(!runtime_condition.status);
    assert_eq!(runtime_condition.reason, "RuntimeBinaryNotExecutable");
}

#[tokio::test]
async fn runtime_config_reports_detected_cgroup_driver() {
    let service = test_service();
    let response = RuntimeService::runtime_config(&service, Request::new(RuntimeConfigRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.linux.unwrap().cgroup_driver,
        service.cgroup_driver() as i32
    );
}

#[tokio::test]
async fn runtime_config_prefers_configured_cgroup_driver() {
    let mut runtime_config = test_runtime_config(tempdir().unwrap().keep());
    runtime_config.cgroup_driver = Some(CgroupDriver::Cgroupfs);
    let service = RuntimeServiceImpl::new(runtime_config);

    let response = RuntimeService::runtime_config(&service, Request::new(RuntimeConfigRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.linux.unwrap().cgroup_driver,
        CgroupDriver::Cgroupfs as i32
    );
}

#[tokio::test]
async fn update_runtime_config_persists_network_config_and_exposes_it_via_status() {
    let root_dir = tempdir().unwrap().keep();
    let service = RuntimeServiceImpl::new(test_runtime_config(root_dir.clone()));

    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.244.0.0/16".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let status = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(status.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeNetworkConfig"]["podCIDR"], "10.244.0.0/16");

    let reloaded = RuntimeServiceImpl::new(test_runtime_config(root_dir));
    let reloaded_status =
        RuntimeService::status(&reloaded, Request::new(StatusRequest { verbose: true }))
            .await
            .unwrap()
            .into_inner();
    let reloaded_config: serde_json::Value =
        serde_json::from_str(reloaded_status.info.get("config").unwrap()).unwrap();
    assert_eq!(
        reloaded_config["runtimeNetworkConfig"]["podCIDR"],
        "10.244.0.0/16"
    );
}

#[tokio::test]
async fn update_runtime_config_renders_cni_config_template_from_pod_cidrs() {
    let root_dir = tempdir().unwrap().keep();
    let config_dir = root_dir.join("net.d");
    let cache_dir = root_dir.join("cache");
    let template_path = root_dir.join("template.conflist");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        &template_path,
        r#"
{
  "name": "test-pod-network",
  "cniVersion": "1.0.0",
  "plugins": [
    {
      "type": "ptp",
      "ipam": {
        "type": "host-local",
        "subnet": "{{.PodCIDR}}",
        "ranges": [{{range $i, $range := .PodCIDRRanges}}{{if $i}}, {{end}}[{"subnet": "{{$range}}"}]{{end}}],
        "routes": [{{range $i, $route := .Routes}}{{if $i}}, {{end}}{"dst": "{{$route}}"}{{end}}]
      }
    }
  ]
}
"#,
    )
    .unwrap();

    let mut config = test_runtime_config(root_dir.clone());
    let mut cni_config = crate::network::CniConfig::new(
        vec![config_dir.clone()],
        Vec::new(),
        cache_dir,
        0,
        crate::network::MainIpPreference::Cni,
        None,
        false,
    );
    cni_config.set_conf_template(Some(template_path));
    config.cni_config = cni_config;
    let service = RuntimeServiceImpl::new(config);

    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.0.0.0/24, 2001:4860:4860::/64".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let rendered = fs::read_to_string(config_dir.join("10-crius-net.conflist")).unwrap();
    assert_eq!(
        rendered.trim(),
        r#"{
  "name": "test-pod-network",
  "cniVersion": "1.0.0",
  "plugins": [
    {
      "type": "ptp",
      "ipam": {
        "type": "host-local",
        "subnet": "10.0.0.0/24",
        "ranges": [[{"subnet": "10.0.0.0/24"}], [{"subnet": "2001:4860:4860::/64"}]],
        "routes": [{"dst": "0.0.0.0/0"}, {"dst": "::/0"}]
      }
    }
  ]
}"#
    );
}

#[tokio::test]
async fn update_runtime_config_rejects_invalid_pod_cidr_for_cni_template() {
    let root_dir = tempdir().unwrap().keep();
    let config_dir = root_dir.join("net.d");
    let template_path = root_dir.join("template.conflist");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(&template_path, r#"{"subnet":"{{.PodCIDR}}"}"#).unwrap();

    let mut config = test_runtime_config(root_dir.clone());
    let mut cni_config = crate::network::CniConfig::new(
        vec![config_dir.clone()],
        Vec::new(),
        root_dir.join("cache"),
        0,
        crate::network::MainIpPreference::Cni,
        None,
        false,
    );
    cni_config.set_conf_template(Some(template_path));
    config.cni_config = cni_config;
    let service = RuntimeServiceImpl::new(config);

    let err = RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "not-a-cidr".to_string(),
                }),
            }),
        }),
    )
    .await
    .expect_err("invalid CIDR should reject template rendering");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err
        .message()
        .contains("Failed to render CNI config template"));
    assert!(!config_dir.join("10-crius-net.conflist").exists());
}

#[tokio::test]
async fn run_pod_sandbox_persists_runtime_pod_cidr_in_verbose_info() {
    let (dir, service) = test_service_with_fake_runtime();
    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.88.0.0/16".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let netns_path = dir.path().join("pod-runtime-cidr.netns");
    fs::write(&netns_path, "netns").unwrap();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            runtime_pod_cidr: Some("10.88.0.0/16".to_string()),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-runtime-cidr".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-runtime-cidr".to_string(),
        test_pod("pod-runtime-cidr", annotations),
    );

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-runtime-cidr".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["runtimePodCIDR"], "10.88.0.0/16");
}

#[tokio::test]
async fn run_pod_sandbox_persists_pod_runtime_artifacts_to_ledger() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri);
    service.image_service().set_test_pull_handler(Arc::new(|_| {
        Ok(crate::image::TestPullResponse {
            image_id: "sha256:pause-ledger-artifacts".to_string(),
            size: 1,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
        })
    }));
    let response = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "pod-ledger-artifacts",
            "default",
            "uid-ledger-artifacts",
            HashMap::new(),
        )),
    )
    .await
    .unwrap()
    .into_inner();
    let pod_id = response.pod_sandbox_id;

    for _ in 0..50 {
        let artifacts = service
            .persistence
            .lock()
            .await
            .list_runtime_artifacts()
            .unwrap();
        if artifacts.iter().any(|artifact| {
            artifact.owner_kind == "pod"
                && artifact.owner_id == pod_id
                && artifact.artifact_kind == "workspace"
        }) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("expected pod runtime artifacts to be persisted");
}

#[tokio::test]
async fn create_container_persists_container_ledger_metadata() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri);
    service.image_service().set_test_pull_handler(Arc::new(|_| {
        Ok(crate::image::TestPullResponse {
            image_id: "sha256:pause-container-ledger".to_string(),
            size: 1,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
        })
    }));
    let pod_response = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "container-ledger",
            "default",
            "uid-container-ledger",
            HashMap::new(),
        )),
    )
    .await
    .unwrap()
    .into_inner();

    ImageService::pull_image(
        &service.image_service(),
        Request::new(crate::proto::runtime::v1::PullImageRequest {
            image: Some(ImageSpec {
                image: "docker.io/library/busybox:latest".to_string(),
                user_specified_image: "docker.io/library/busybox:latest".to_string(),
                ..Default::default()
            }),
            auth: None,
            sandbox_config: None,
        }),
    )
    .await
    .unwrap();

    let container_id = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: pod_response.pod_sandbox_id,
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "ctr-ledger".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "docker.io/library/busybox:latest".to_string(),
                    user_specified_image: "docker.io/library/busybox:latest".to_string(),
                    ..Default::default()
                }),
                command: vec!["sleep".to_string(), "10".to_string()],
                linux: Some(crate::proto::runtime::v1::LinuxContainerConfig::default()),
                ..Default::default()
            }),
            sandbox_config: Some(crate::proto::runtime::v1::PodSandboxConfig {
                metadata: Some(PodSandboxMetadata {
                    name: "container-ledger".to_string(),
                    uid: "uid-container-ledger".to_string(),
                    namespace: "default".to_string(),
                    attempt: 1,
                }),
                hostname: "container-ledger".to_string(),
                labels: HashMap::new(),
                annotations: HashMap::new(),
                log_directory: String::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                linux: Some(crate::proto::runtime::v1::LinuxPodSandboxConfig {
                    security_context: Some(
                        crate::proto::runtime::v1::LinuxSandboxSecurityContext {
                            namespace_options: Some(NamespaceOption {
                                network: NamespaceMode::Pod as i32,
                                pid: NamespaceMode::Pod as i32,
                                ipc: NamespaceMode::Pod as i32,
                                target_id: String::new(),
                                userns_options: None,
                            }),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .container_id;

    for _ in 0..50 {
        if let Some(record) = service
            .persistence
            .lock()
            .await
            .storage()
            .get_container(&container_id)
            .unwrap()
        {
            if record.runtime_handler.as_deref() == Some("runc")
                && record.runtime_backend.as_deref() == Some("runc")
                && record.snapshot_key.as_deref() == Some(container_id.as_str())
            {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("expected container ledger metadata to be persisted");
}

#[tokio::test]
async fn host_network_pod_verbose_info_omits_managed_netns_and_runtime_pod_cidr() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            namespace_options: Some(StoredNamespaceOptions {
                network: NamespaceMode::Node as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-host-network".to_string(),
        test_pod("pod-host-network", annotations),
    );

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-host-network".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert!(info["netnsPath"].is_null());
    assert!(info["runtimePodCIDR"].is_null());
}

#[tokio::test]
async fn network_health_requires_declared_plugin_binary() {
    let _guard = env_lock().lock().await;
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("cni-conf");
    let plugin_dir = dir.path().join("cni-bin");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::write(
        config_dir.join("10-test.conflist"),
        r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
    )
    .unwrap();

    std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
    std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
    let service = test_service_with_env_cni();
    let status = service.probe_cni_load_status().await;
    std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
    std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIPluginMissing");
    assert!(status.message.contains("bridge"));
    assert_eq!(status.loaded_networks, vec!["test"]);
    assert_eq!(status.declared_plugins, vec!["bridge"]);
    assert_eq!(status.missing_plugin_binaries, vec!["bridge"]);
}

#[tokio::test]
async fn network_health_requires_plugin_to_be_executable() {
    let _guard = env_lock().lock().await;
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("cni-conf");
    let plugin_dir = dir.path().join("cni-bin");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::write(
        config_dir.join("10-test.conflist"),
        r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
    )
    .unwrap();
    let plugin_path = plugin_dir.join("bridge");
    fs::write(&plugin_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&plugin_path, perms).unwrap();

    std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
    std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
    let service = test_service_with_env_cni();
    let status = service.probe_cni_load_status().await;
    std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
    std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIPluginMissing");
    assert!(status.message.contains("bridge"));
    assert!(status
        .missing_plugin_binaries
        .contains(&"bridge".to_string()));
}

#[tokio::test]
async fn checkpoint_container_writes_checkpoint_artifact() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(dir.path().join("logs").join("c1.log").display().to_string()),
            metadata_name: Some("checkpointed".to_string()),
            metadata_attempt: Some(2),
            started_at: Some(10),
            ..Default::default()
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let bundle_dir = dir.path().join("runtime-root").join("container-running");
    fs::create_dir_all(&bundle_dir).unwrap();
    let rootfs_dir = dir.path().join("checkpoint-rootfs-json");
    fs::create_dir_all(&rootfs_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": rootfs_dir.display().to_string()
            },
        })
        .to_string(),
    )
    .unwrap();

    let artifact_path = dir.path().join("checkpoint.json");
    RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: artifact_path.display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap();

    let artifact: serde_json::Value =
        serde_json::from_slice(&fs::read(&artifact_path).unwrap()).unwrap();
    assert_eq!(artifact["manifest"]["containerId"], "container-running");
    assert_eq!(artifact["manifest"]["runtimeState"], "running");
    assert_eq!(artifact["manifest"]["metadata"]["name"], "checkpointed");
    assert_eq!(artifact["manifest"]["metadata"]["attempt"], 2);
    assert_eq!(
        artifact["ociConfig"]["root"]["path"],
        rootfs_dir.display().to_string()
    );
    assert_eq!(artifact["manifest"]["rootfsSnapshot"]["path"], "rootfs.tar");
    assert_eq!(artifact["manifest"]["rootfsSnapshot"]["format"], "tar");
    assert_eq!(
        artifact["manifest"]["rootfsSnapshot"]["restorePolicy"],
        "replace"
    );
    let checkpoint_image_path = PathBuf::from(
        artifact["manifest"]["checkpointImagePath"]
            .as_str()
            .expect("checkpoint image path should be recorded"),
    );
    assert!(checkpoint_image_path.join("checkpoint.json").exists());
    assert!(checkpoint_image_path.join("rootfs.tar").exists());
}

#[tokio::test]
async fn checkpoint_container_uses_configured_criu_staging_paths() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.criu_image_path = dir.path().join("criu-images");
    service.config.criu_work_path = dir.path().join("criu-work");

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let bundle_dir = dir.path().join("runtime-root").join("container-running");
    fs::create_dir_all(&bundle_dir).unwrap();
    let rootfs_dir = dir.path().join("checkpoint-rootfs-custom");
    fs::create_dir_all(&rootfs_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": rootfs_dir.display().to_string()
            },
        })
        .to_string(),
    )
    .unwrap();

    let artifact_path = dir.path().join("checkpoint.tar");
    let expected_image_path = service.checkpoint_runtime_image_path(&artifact_path);
    let expected_work_path = service.checkpoint_runtime_work_path(&artifact_path);
    RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: artifact_path.display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap();

    assert!(expected_image_path.join("checkpoint.json").exists());
    assert!(expected_image_path.join("rootfs.tar").exists());
    assert!(expected_work_path.is_dir());
    assert!(
        expected_image_path.starts_with(dir.path().join("criu-images")),
        "checkpoint image staging path should honor runtime.criu_image_path"
    );
    assert!(
        expected_work_path.starts_with(dir.path().join("criu-work")),
        "checkpoint work staging path should honor runtime.criu_work_path"
    );
}

#[tokio::test]
async fn checkpoint_container_writes_tar_export_when_location_is_archive() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("checkpointed-tar".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let bundle_dir = dir.path().join("runtime-root").join("container-running");
    fs::create_dir_all(&bundle_dir).unwrap();
    let rootfs_dir = dir.path().join("checkpoint-rootfs-tar");
    fs::create_dir_all(&rootfs_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": rootfs_dir.display().to_string()
            },
        })
        .to_string(),
    )
    .unwrap();

    let artifact_path = dir.path().join("checkpoint.tar");
    RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: artifact_path.display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap();

    let tar_output = Command::new("tar")
        .args(["-tf", artifact_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(tar_output.status.success());
    let stdout = String::from_utf8_lossy(&tar_output.stdout);
    assert!(stdout.contains("manifest.json"));
    assert!(stdout.contains("config.json"));
    assert!(stdout.contains("checkpoint.json"));
    assert!(stdout.contains("rootfs.tar"));
}

#[tokio::test]
async fn checkpoint_container_fails_when_criu_support_is_disabled() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.enable_criu_support = false;

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let err = RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: dir.path().join("checkpoint.json").display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("CRIU support is disabled"));
}

#[test]
fn validate_checkpoint_rootfs_snapshot_manifest_rejects_unsupported_format() {
    let dir = tempdir().unwrap();
    let checkpoint_image_path = dir.path().join("checkpoint");
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("rootfs.tar"), "placeholder").unwrap();

    let manifest = serde_json::json!({
        "rootfsSnapshot": {
            "path": "rootfs.tar",
            "format": "cpio",
            "restorePolicy": "replace",
        }
    });

    let err = RuntimeServiceImpl::validate_checkpoint_rootfs_snapshot_manifest(
        &manifest,
        &checkpoint_image_path,
    )
    .expect_err("unsupported rootfs snapshot format must be rejected");
    assert!(err
        .to_string()
        .contains("checkpoint rootfsSnapshot.format must be tar"));
}

#[test]
fn checkpoint_restore_from_artifact_requires_manifest_image_ref() {
    let (dir, service) = test_service_with_fake_runtime();
    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("rootfs.tar"), "placeholder").unwrap();
    fs::write(
        &checkpoint_location,
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifest": {
                "rootfsSnapshot": {
                    "path": "rootfs.tar",
                    "format": "tar",
                    "restorePolicy": "replace",
                }
            },
            "ociConfig": {
                "ociVersion": "1.0.2",
                "root": { "path": "/tmp/rootfs" }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let err = service
        .checkpoint_restore_from_artifact(&checkpoint_location)
        .expect_err("restore artifact without imageRef must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("manifest.imageRef"));
}

#[test]
fn checkpoint_restore_from_artifact_ignores_manifest_bundle_paths() {
    let (dir, service) = test_service_with_fake_runtime();
    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("rootfs.tar"), "placeholder").unwrap();
    fs::write(
        &checkpoint_location,
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifest": {
                "imageRef": "busybox:latest",
                "bundlePath": "/definitely/stale/bundle",
                "configPath": "/definitely/stale/config.json",
                "rootfsSnapshot": {
                    "path": "rootfs.tar",
                    "format": "tar",
                    "restorePolicy": "replace",
                }
            },
            "ociConfig": {
                "ociVersion": "1.0.2",
                "root": { "path": "/tmp/rootfs" }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let restore = service
        .checkpoint_restore_from_artifact(&checkpoint_location)
        .expect("bundle/config export paths are informational only");
    assert_eq!(restore.image_ref, "busybox:latest");
    assert_eq!(
        restore.checkpoint_image_path,
        checkpoint_image_path.display().to_string()
    );
}

#[tokio::test]
async fn start_container_restores_from_checkpoint_artifact_and_clears_pending_marker() {
    let (dir, service) = test_service_with_fake_runtime();

    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("checkpoint.json"), "{}").unwrap();
    fs::write(
        &checkpoint_location,
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifest": {
                "imageRef": "busybox:latest",
                "checkpointImagePath": checkpoint_image_path.display().to_string(),
            },
            "ociConfig": {
                "ociVersion": "1.0.2",
                "root": {
                    "path": "/tmp/rootfs"
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CHECKPOINT_RESTORE_KEY,
        &StoredCheckpointRestore {
            checkpoint_location: checkpoint_location.display().to_string(),
            checkpoint_image_path: checkpoint_image_path.display().to_string(),
            oci_config: serde_json::json!({
                "ociVersion": "1.0.2",
                "root": {
                    "path": "/tmp/rootfs"
                }
            }),
            image_ref: "busybox:latest".to_string(),
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "restore-container".to_string(),
        Container {
            state: ContainerState::ContainerCreated as i32,
            ..test_container("restore-container", "pod-1", annotations.clone())
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "restore-container",
            "pod-1",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let bundle_dir = dir.path().join("runtime-root").join("restore-container");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": annotations,
            "root": {
                "path": "/tmp/rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "restore-container".to_string(),
        }),
    )
    .await
    .unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("restore-container")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);
    let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .expect("started_at should be persisted after restore");
    assert!(internal_state.started_at.is_some());
    assert!(
        !container
            .annotations
            .contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY),
        "restore marker should be cleared after successful restore"
    );

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("restore-container")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    assert!(
        !persisted_annotations.contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY),
        "restore marker should be cleared from persistence after successful restore"
    );

    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            dir.path()
                .join("runtime-root")
                .join("restore-container")
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let bundle_annotations = bundle_config
        .get("annotations")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    assert!(!bundle_annotations.contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY));
    let bundle_container_state: StoredContainerState = serde_json::from_str(
        bundle_annotations
            .get(INTERNAL_CONTAINER_STATE_KEY)
            .and_then(|value| value.as_str())
            .expect("bundle should persist internal container state"),
    )
    .unwrap();
    assert!(bundle_container_state.started_at.is_some());

    let exit_code_path = dir.path().join("exits").join("restore-container");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "0").unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn start_container_rejects_checkpoint_restore_when_criu_support_is_disabled() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.enable_criu_support = false;

    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("checkpoint.json"), "{}").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CHECKPOINT_RESTORE_KEY,
        &StoredCheckpointRestore {
            checkpoint_location: checkpoint_location.display().to_string(),
            checkpoint_image_path: checkpoint_image_path.display().to_string(),
            oci_config: serde_json::json!({
                "ociVersion": "1.0.2",
                "root": { "path": "/tmp/rootfs" }
            }),
            image_ref: "busybox:latest".to_string(),
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "restore-container".to_string(),
        Container {
            state: ContainerState::ContainerCreated as i32,
            ..test_container("restore-container", "pod-1", annotations.clone())
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "restore-container",
            "pod-1",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "restore-container".to_string(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("CRIU support is disabled"));
    let container = service
        .containers
        .lock()
        .await
        .get("restore-container")
        .cloned()
        .unwrap();
    assert!(container
        .annotations
        .contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY));
}

#[tokio::test]
async fn start_container_notifies_nri_before_and_after_runtime_start() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start".to_string(),
        test_pod("pod-start", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start".to_string(),
        test_container("container-start", "pod-start", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start",
            "pod-start",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-start")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);

    let post_start_event = fake_nri
        .post_start_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("post start event should be recorded");
    assert_eq!(
        post_start_event.container.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_RUNNING
    );
    assert!(post_start_event.container.started_at > 0);
}

#[tokio::test]
async fn start_container_succeeds_when_nri_post_start_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_start_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-post-fail".to_string(),
        test_pod("pod-start-post-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-post-fail".to_string(),
        test_container(
            "container-start-post-fail",
            "pod-start-post-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-post-fail",
            "pod-start-post-fail",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-post-fail", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-post-fail".to_string(),
        }),
    )
    .await
    .expect("post-start failure should not fail start");

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-post-fail")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);
}

#[tokio::test]
async fn start_container_falls_back_to_persistence_when_memory_state_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-start".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-persisted-start".to_string(),
        test_pod("pod-persisted-start", HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-start",
            "pod-persisted-start",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-persisted-start", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-persisted-start".to_string(),
        }),
    )
    .await
    .expect("start should rehydrate persisted created containers");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-persisted-start")
        .cloned()
        .expect("start should load persisted container into memory");
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-persisted-start")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "running");
}

#[tokio::test]
async fn start_container_rejects_repeated_start_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-repeat".to_string(),
        test_pod("pod-start-repeat", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-repeat".to_string(),
        test_container(
            "container-start-repeat",
            "pod-start-repeat",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-repeat",
            "pod-start-repeat",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-repeat", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-repeat".to_string(),
        }),
    )
    .await
    .expect("first start should succeed");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-repeat".to_string(),
        }),
    )
    .await
    .expect_err("repeated start should be rejected");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("already running"));
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
}

#[tokio::test]
async fn start_container_rejects_exited_container_with_failed_precondition() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            exit_code: Some(17),
            finished_at: Some(RuntimeServiceImpl::now_nanos()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-exited".to_string(),
        test_pod("pod-start-exited", HashMap::new()),
    );
    let mut container = test_container(
        "container-start-exited",
        "pod-start-exited",
        annotations.clone(),
    );
    container.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-start-exited".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-exited",
            "pod-start-exited",
            crate::runtime::ContainerStatus::Stopped(17),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-start-exited", "stopped");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-exited".to_string(),
        }),
    )
    .await
    .expect_err("exited containers must not be startable again");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("already exited"));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn start_container_undoes_nri_when_runtime_start_fails() {
    let _guard = env_lock().lock().await;
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri_and_shim(
        fake_nri.clone(),
        Some(PathBuf::from("/definitely/missing/crius-shim")),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-fail".to_string(),
        test_pod("pod-start-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-fail".to_string(),
        test_container(
            "container-start-fail",
            "pod-start-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-fail",
            "pod-start-fail",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-fail", &annotations);

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-sidecar".to_string();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 256;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-start-fail", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-start-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-sidecar", "running");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-fail".to_string(),
        }),
    )
    .await
    .expect_err("runtime start failure should be surfaced");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("Failed to start container"));

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "stop_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-start-fail")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerCreated as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);

    let sidecar_update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar_update_payload["cpu"]["shares"], 256);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-start-fail")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "created");
    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            dir.path()
                .join("runtime-root")
                .join("container-start-fail")
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let bundle_annotations = bundle_config
        .get("annotations")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    let bundle_state: StoredContainerState = serde_json::from_str(
        bundle_annotations
            .get(INTERNAL_CONTAINER_STATE_KEY)
            .and_then(|value| value.as_str())
            .expect("bundle should persist internal container state after failed start"),
    )
    .unwrap();
    assert!(bundle_state.nri_stop_notified);
    assert!(bundle_state.started_at.is_none());
    assert!(bundle_state.finished_at.is_none());
}

#[tokio::test]
async fn start_container_fails_when_runtime_keeps_container_in_created_state() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script_with_start_state(dir.path(), "created");
    let shim_path = write_quick_exit_shim(dir.path());
    let (dir, mut service) =
        test_service_with_runtime_path_and_shim_path(dir, runtime_path.clone(), Some(shim_path));
    service.runtime = RuntimeRegistry::new(
        "runc".to_string(),
        HashMap::from([(
            "runc".to_string(),
            Arc::new(crate::runtime::RuncBackend::new(RuncRuntime::new(
                runtime_path.clone(),
                dir.path().join("runtime-root"),
            ))) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("runc".to_string(), 240)]),
    );
    let mut rx = service.events.subscribe();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-start-created".to_string(),
        test_pod("pod-start-created", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-created".to_string(),
        test_container(
            "container-start-created",
            "pod-start-created",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-created",
            "pod-start-created",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-created", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-created".to_string(),
        }),
    )
    .await
    .expect_err("container that stays created should fail start");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("did not reach running"));

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-created")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerCreated as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
    assert!(state.started_at.is_none());
    assert!(state.finished_at.is_none());
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-start-created")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "created");
    assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());
}

#[tokio::test]
async fn start_container_fails_when_runtime_exits_immediately_and_persists_exit_state() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script_with_start_state(dir.path(), "stopped");
    let shim_path = write_quick_exit_shim(dir.path());
    let (dir, mut service) =
        test_service_with_runtime_path_and_shim_path(dir, runtime_path.clone(), Some(shim_path));
    service.runtime = RuntimeRegistry::new(
        "runc".to_string(),
        HashMap::from([(
            "runc".to_string(),
            Arc::new(crate::runtime::RuncBackend::new(RuncRuntime::new(
                runtime_path.clone(),
                dir.path().join("runtime-root"),
            ))) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("runc".to_string(), 240)]),
    );
    let mut rx = service.events.subscribe();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-start-stopped".to_string(),
        test_pod("pod-start-stopped", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-stopped".to_string(),
        test_container(
            "container-start-stopped",
            "pod-start-stopped",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-stopped",
            "pod-start-stopped",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-stopped", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-stopped".to_string(),
        }),
    )
    .await
    .expect_err("container that exits immediately should fail start");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("exited"));

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-stopped")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
    assert_eq!(state.exit_code, Some(0));
    assert!(state.finished_at.is_some());
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-start-stopped")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    let event = timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("stopped event should be published")
        .expect("stopped event receiver should succeed");
    assert_eq!(
        event.container_event_type,
        ContainerEventType::ContainerStoppedEvent as i32
    );
}

#[tokio::test]
async fn finalize_container_stop_state_only_sets_finished_at_for_exited_state() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            started_at: Some(11),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-stop-finalize".to_string(),
        test_container("container-stop-finalize", "pod-1", annotations),
    );
    let persisted_annotations = service
        .containers
        .lock()
        .await
        .get("container-stop-finalize")
        .unwrap()
        .annotations
        .clone();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-finalize",
            "pod-1",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &persisted_annotations,
        )
        .unwrap();

    service
        .finalize_container_stop_state("container-stop-finalize", ContainerStatus::Created)
        .await
        .unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("container-stop-finalize")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.started_at, Some(11));
    assert!(state.finished_at.is_none());
    assert!(state.exit_code.is_none());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn remove_after_failed_start_does_not_repeat_nri_stop() {
    let _guard = env_lock().lock().await;
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri_and_shim(
        fake_nri.clone(),
        Some(PathBuf::from("/definitely/missing/crius-shim")),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-remove".to_string(),
        test_pod("pod-start-remove", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-remove".to_string(),
        test_container(
            "container-start-remove",
            "pod-start-remove",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-remove",
            "pod-start-remove",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-remove", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-remove".to_string(),
        }),
    )
    .await
    .expect_err("runtime start failure should be surfaced");
    assert_eq!(err.code(), tonic::Code::Internal);

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-start-remove".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "stop_container", "remove_container"]
    );
}

#[tokio::test]
async fn stop_container_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-stop".to_string(), test_pod("pod-stop", HashMap::new()));
    service.containers.lock().await.insert(
        "container-stop".to_string(),
        test_container("container-stop", "pod-stop", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop",
            "pod-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("first stop should succeed");
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("repeated stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &service
            .containers
            .lock()
            .await
            .get("container-stop")
            .unwrap()
            .annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
}

#[tokio::test]
async fn stop_container_notifies_nri_with_post_stop_state() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-state".to_string(),
        test_pod("pod-stop-state", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-stop-state".to_string(),
        test_container(
            "container-stop-state",
            "pod-stop-state",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-state",
            "pod-stop-state",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop-state", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop-state".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed");

    let stop_event = fake_nri
        .stop_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("stop event should be recorded");
    assert_eq!(
        stop_event.container.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_STOPPED
    );
}

#[tokio::test]
async fn stop_container_succeeds_when_nri_stop_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_stop_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-fail".to_string(),
        test_pod("pod-stop-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-stop-fail".to_string(),
        test_container("container-stop-fail", "pod-stop-fail", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-fail",
            "pod-stop-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop-fail", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop-fail".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("NRI stop failure should not fail container stop");
}

#[tokio::test]
async fn stop_container_skips_nri_for_already_exited_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-exited".to_string(),
        test_pod("pod-exited", HashMap::new()),
    );
    let mut container = test_container("container-exited", "pod-exited", annotations.clone());
    container.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-exited".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-exited",
            "pod-exited",
            crate::runtime::ContainerStatus::Stopped(17),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-exited", "stopped");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-exited".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed");

    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn stop_container_converts_unknown_runtime_state_to_exited() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-unknown".to_string(),
        test_pod("pod-unknown", HashMap::new()),
    );
    let mut container = test_container("container-unknown", "pod-unknown", annotations.clone());
    container.state = ContainerState::ContainerRunning as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-unknown".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-unknown",
            "pod-unknown",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-unknown".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed for unknown runtime state");

    let container = service
        .containers
        .lock()
        .await
        .get("container-unknown")
        .cloned()
        .expect("container should remain tracked");
    assert_eq!(container.state, ContainerState::ContainerExited as i32);

    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .expect("internal state should be updated");
    assert!(state.finished_at.is_some());
    assert_eq!(state.exit_code, Some(-1));

    tokio::time::sleep(Duration::from_millis(50)).await;
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-unknown")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    assert_eq!(persisted.exit_code, Some(-1));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn stop_container_falls_back_to_persistence_when_memory_state_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-stop".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-stop",
            "pod-persisted-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-persisted-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("fallback stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    assert!(
        fake_runtime_state_path(&dir, "container-persisted-stop").exists(),
        "runtime stop should leave a stopped runtime state artifact"
    );

    let container = service
        .containers
        .lock()
        .await
        .get("container-persisted-stop")
        .cloned()
        .expect("fallback stop should rehydrate container into memory");
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
    assert!(state.finished_at.is_some());
    assert_eq!(state.exit_code, Some(0));

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-persisted-stop")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    assert_eq!(persisted.exit_code, Some(0));
}

#[tokio::test]
async fn stop_container_fallback_is_idempotent_across_repeated_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-repeat-stop".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-repeat-stop",
            "pod-persisted-repeat-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-repeat-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-persisted-repeat-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("first fallback stop should succeed");
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-persisted-repeat-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("second fallback stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
}

#[tokio::test]
async fn remove_container_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove".to_string(),
        test_pod("pod-remove", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove".to_string(),
        test_container("container-remove", "pod-remove", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove",
            "pod-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove".to_string(),
        }),
    )
    .await
    .expect("first remove should succeed");
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove".to_string(),
        }),
    )
    .await
    .expect("repeated remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
}

#[tokio::test]
async fn remove_container_succeeds_when_nri_remove_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_remove_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-fail".to_string(),
        test_pod("pod-remove-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove-fail".to_string(),
        test_container(
            "container-remove-fail",
            "pod-remove-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove-fail",
            "pod-remove-fail",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove-fail", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove-fail".to_string(),
        }),
    )
    .await
    .expect("NRI remove failure should not fail container removal");
}

#[tokio::test]
async fn remove_container_notifies_nri_stop_before_remove_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-running".to_string(),
        test_pod("pod-remove-running", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove-running".to_string(),
        test_container(
            "container-remove-running",
            "pod-remove-running",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove-running",
            "pod-remove-running",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove-running", "running");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove-running".to_string(),
        }),
    )
    .await
    .expect("remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "remove_container"]
    );
}

#[tokio::test]
async fn remove_container_falls_back_to_persistence_when_memory_state_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-remove".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-remove",
            "pod-persisted-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-persisted-remove".to_string(),
        }),
    )
    .await
    .expect("fallback remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
    assert!(
        !dir.path()
            .join("runtime-root")
            .join("container-persisted-remove")
            .exists(),
        "bundle directory should be removed"
    );
    assert!(
        service
            .containers
            .lock()
            .await
            .get("container-persisted-remove")
            .is_none(),
        "fallback remove should clean rehydrated memory state"
    );
    assert!(
        service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("container-persisted-remove")
            .unwrap()
            .is_none(),
        "fallback remove should delete persisted state"
    );
}

#[tokio::test]
async fn remove_container_fallback_is_idempotent_across_repeated_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-repeat-remove".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-repeat-remove",
            "pod-persisted-repeat-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-repeat-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-persisted-repeat-remove".to_string(),
        }),
    )
    .await
    .expect("first fallback remove should succeed");
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-persisted-repeat-remove".to_string(),
        }),
    )
    .await
    .expect("second fallback remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
}

#[tokio::test]
async fn stop_pod_sandbox_stops_member_containers_before_pod_notification() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-order".to_string(),
        test_pod("pod-stop-order", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-under-pod-stop".to_string(),
        test_container("container-under-pod-stop", "pod-stop-order", HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-under-pod-stop",
            "pod-stop-order",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-under-pod-stop", "running");

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-stop-order".to_string(),
        }),
    )
    .await
    .expect("pod stop should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "stop_pod"]
    );
    assert_eq!(fake_nri.stop_pod_events.lock().await.len(), 1);
}

#[tokio::test]
async fn remove_pod_sandbox_removes_member_containers_before_pod_notification() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-order".to_string(),
        test_pod("pod-remove-order", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-under-pod-remove".to_string(),
        test_container(
            "container-under-pod-remove",
            "pod-remove-order",
            HashMap::new(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-under-pod-remove",
            "pod-remove-order",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-under-pod-remove", "running");

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-remove-order".to_string(),
        }),
    )
    .await
    .expect("pod remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "remove_container", "remove_pod"]
    );
}

#[tokio::test]
async fn start_container_clears_nri_stop_notification_marker() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            nri_stop_notified: true,
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-restart".to_string(),
        test_pod("pod-restart", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-restart".to_string(),
        test_container("container-restart", "pod-restart", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-restart",
            "pod-restart",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-restart", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-restart".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-restart".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container", "stop_container",]
    );
}

#[tokio::test]
async fn reopen_container_log_validates_running_state_and_log_path() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "missing".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    let mut stopped_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut stopped_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("stopped.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", stopped_annotations),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");

    let stopped = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-stopped".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    let log_path = dir.path().join("logs").join("running.log");
    let mut running_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut running_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", running_annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let running = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(running.code(), tonic::Code::FailedPrecondition);
    assert!(running.message().contains("reopen log control socket"));
}

#[tokio::test]
async fn update_container_resources_validates_state_and_persists_resources() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "missing".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-stopped".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 256,
        cpu_period: 100_000,
        cpu_quota: 50_000,
        memory_limit_in_bytes: 128 * 1024 * 1024,
        cpuset_cpus: "0-1".to_string(),
        cpuset_mems: "0".to_string(),
        ..Default::default()
    };

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(resources.clone()),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let status = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-running".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let linux = status.resources.unwrap().linux.unwrap();
    assert_eq!(linux.cpu_shares, 256);
    assert_eq!(linux.memory_limit_in_bytes, 128 * 1024 * 1024);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        persisted_state
            .linux_resources
            .unwrap()
            .memory_limit_in_bytes,
        128 * 1024 * 1024
    );

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 256);
    assert_eq!(update_payload["memory"]["limit"], 128 * 1024 * 1024);
}

#[tokio::test]
async fn update_container_resources_fails_when_cgroup_support_is_disabled() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.disable_cgroup = true;

    let err = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("disable_cgroup"));
}

#[tokio::test]
async fn update_container_resources_fails_when_hugetlb_is_missing_and_tolerance_is_disabled() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let err = RuntimeServiceImpl::validate_proto_hugetlb_limits_with_flags(
        Some(&crate::proto::runtime::v1::LinuxContainerResources {
            hugepage_limits: vec![crate::proto::runtime::v1::HugepageLimit {
                page_size: "2MB".to_string(),
                limit: 1,
            }],
            ..Default::default()
        }),
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
        "container resource update",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn update_container_resources_accepts_paused_container() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-paused".to_string(),
        test_container("container-paused", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-paused", "paused");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-paused",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-paused".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 777,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .expect("paused container should accept resource updates");

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-paused")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 777);
}

#[tokio::test]
async fn update_container_resources_applies_nri_result_before_post_update() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut target_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut target_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", target_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &target_annotations,
        )
        .unwrap();

    let mut sidecar_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut sidecar_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-1", sidecar_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-sidecar", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &sidecar_annotations,
        )
        .unwrap();

    let mut target_resources = crate::nri_proto::api::LinuxResources::new();
    let mut target_cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 1024;
    target_cpu.shares = protobuf::MessageField::some(shares);
    target_resources.cpu = protobuf::MessageField::some(target_cpu);

    let mut sidecar_update = crate::nri_proto::api::ContainerUpdate::new();
    sidecar_update.container_id = "container-sidecar".to_string();
    let mut sidecar_linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut sidecar_resources = crate::nri_proto::api::LinuxResources::new();
    let mut sidecar_memory = crate::nri_proto::api::LinuxMemory::new();
    let mut sidecar_limit = crate::nri_proto::api::OptionalInt64::new();
    sidecar_limit.value = 64 * 1024 * 1024;
    sidecar_memory.limit = protobuf::MessageField::some(sidecar_limit);
    sidecar_resources.memory = protobuf::MessageField::some(sidecar_memory);
    sidecar_linux.resources = protobuf::MessageField::some(sidecar_resources);
    sidecar_update.linux = protobuf::MessageField::some(sidecar_linux);

    *fake_nri.update_result.lock().await = Some(NriUpdateContainerResult {
        linux_resources: Some(target_resources),
        updates: vec![sidecar_update],
        evictions: Vec::new(),
    });

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let target_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(target_payload["cpu"]["shares"], 1024);

    let sidecar_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar_payload["memory"]["limit"], 64 * 1024 * 1024);

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_container", "post_update_container"]
    );

    let post_update_event = fake_nri
        .post_update_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("post update event should be recorded");
    let post_update_linux = post_update_event
        .container
        .linux
        .as_ref()
        .expect("post update container should include linux state");
    assert_eq!(
        post_update_linux
            .resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|value| value.value),
        Some(1024)
    );
}

#[tokio::test]
async fn update_container_resources_succeeds_when_nri_post_update_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_update_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-update-post-fail".to_string(),
        test_pod("pod-update-post-fail", HashMap::new()),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-update-post-fail".to_string(),
        test_container(
            "container-update-post-fail",
            "pod-update-post-fail",
            annotations.clone(),
        ),
    );
    set_fake_runtime_state(&dir, "container-update-post-fail", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-update-post-fail",
            "pod-update-post-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-update-post-fail".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 512,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .expect("post-update failure should not fail resource update");

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-update-post-fail")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 512);
}

#[tokio::test]
async fn update_container_resources_applies_extended_nri_resources() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let mut extended = crate::nri_proto::api::LinuxResources::new();
    let mut memory = crate::nri_proto::api::LinuxMemory::new();
    let mut reservation = crate::nri_proto::api::OptionalInt64::new();
    reservation.value = 4096;
    memory.reservation = protobuf::MessageField::some(reservation);
    extended.memory = protobuf::MessageField::some(memory);
    extended.pids = Some(crate::nri_proto::api::LinuxPids {
        limit: 42,
        ..Default::default()
    })
    .into();
    extended
        .devices
        .push(crate::nri_proto::api::LinuxDeviceCgroup {
            allow: true,
            type_: "c".to_string(),
            access: "rwm".to_string(),
            major: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 10;
                protobuf::MessageField::some(value)
            },
            minor: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 200;
                protobuf::MessageField::some(value)
            },
            ..Default::default()
        });
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    extended.blockio_class = protobuf::MessageField::some(blockio_class);
    *fake_nri.update_result.lock().await = Some(NriUpdateContainerResult {
        linux_resources: Some(extended),
        updates: Vec::new(),
        evictions: Vec::new(),
    });

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 256);
    assert_eq!(payload["memory"]["reservation"], 4096);
    assert_eq!(payload["pids"]["limit"], 42);
    assert_eq!(payload["devices"][0]["type"], "c");
    assert_eq!(payload["devices"][0]["major"], 10);
    assert_eq!(payload["devices"][0]["minor"], 200);
    assert_eq!(payload["devices"][0]["access"], "rwm");
    assert_eq!(payload["blockIO"]["weight"], 500);
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_container", "post_update_container"]
    );

    let container = service
        .containers
        .lock()
        .await
        .get("container-running")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    let resources = state.linux_resources.unwrap();
    assert_eq!(resources.cpu_shares, 256);
    assert_eq!(resources.memory_reservation_in_bytes, Some(4096));
    assert_eq!(resources.pids_limit, Some(42));
    assert_eq!(resources.devices.len(), 1);
    assert_eq!(resources.devices[0].device_type.as_deref(), Some("c"));
    assert_eq!(resources.devices[0].major, Some(10));
    assert_eq!(resources.devices[0].minor, Some(200));
    assert_eq!(resources.blockio_class.as_deref(), Some("gold"));
}

#[tokio::test]
async fn stop_container_applies_nri_update_side_effects() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut target_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut target_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let mut sidecar_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut sidecar_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));
    service.containers.lock().await.insert(
        "container-stop".to_string(),
        test_container("container-stop", "pod-1", target_annotations.clone()),
    );
    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-1", sidecar_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-stop", "running");
    set_fake_runtime_state(&dir, "container-sidecar", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &target_annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &sidecar_annotations,
        )
        .unwrap();

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-sidecar".to_string();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 2048;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 2048);
    let sidecar = service
        .containers
        .lock()
        .await
        .get("container-sidecar")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &sidecar.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 2048);
}

#[tokio::test]
async fn nri_domain_apply_updates_updates_runtime_and_persistence() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", annotations.clone()));
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-running".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 512;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 512);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(persisted_state.linux_resources.unwrap().cpu_shares, 512);
}

#[tokio::test]
async fn nri_domain_snapshot_excludes_exited_containers() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut running = test_container("container-running", "pod-1", annotations.clone());
    running.state = ContainerState::ContainerRunning as i32;
    let mut exited = test_container("container-exited", "pod-1", annotations);
    exited.state = ContainerState::ContainerExited as i32;

    let mut containers = service.containers.lock().await;
    containers.insert(running.id.clone(), running);
    containers.insert(exited.id.clone(), exited);
    drop(containers);

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let snapshot = domain.snapshot().await.unwrap();
    let ids = snapshot
        .containers
        .iter()
        .map(|container| container.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["container-running"]);
}

#[tokio::test]
async fn nri_domain_snapshot_includes_paused_runtime_containers() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut paused = test_container("container-paused", "pod-1", annotations);
    paused.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert(paused.id.clone(), paused);
    set_fake_runtime_state(&dir, "container-paused", "paused");

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let snapshot = domain.snapshot().await.unwrap();
    assert_eq!(snapshot.containers.len(), 1);
    assert_eq!(snapshot.containers[0].id, "container-paused");
    assert_eq!(
        snapshot.containers[0].state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_PAUSED
    );
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_failures_marked_ignore_failure() {
    let (_dir, service) = test_service_with_fake_runtime();
    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "missing".to_string();
    update.ignore_failure = true;

    let failed = domain.apply_updates(&[update.clone()]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_missing_containers() {
    let (_dir, service) = test_service_with_fake_runtime();
    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "missing".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    linux_update.resources =
        protobuf::MessageField::some(crate::nri_proto::api::LinuxResources::new());
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update.clone()]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_accepts_short_container_ids() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running-long".to_string(),
        test_container("container-running-long", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running-long", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running-long",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-run".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 768;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running-long")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 768);
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_non_mutable_containers() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-exited".to_string(),
        test_container("container-exited", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-exited", "stopped");

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-exited".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    linux_update.resources =
        protobuf::MessageField::some(crate::nri_proto::api::LinuxResources::new());
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_accepts_extended_resources() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-running".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 512;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    resources.pids = Some(crate::nri_proto::api::LinuxPids {
        limit: 7,
        ..Default::default()
    })
    .into();
    resources
        .devices
        .push(crate::nri_proto::api::LinuxDeviceCgroup {
            allow: false,
            type_: "b".to_string(),
            access: "rw".to_string(),
            major: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 8;
                protobuf::MessageField::some(value)
            },
            minor: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 0;
                protobuf::MessageField::some(value)
            },
            ..Default::default()
        });
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 512);
    assert_eq!(payload["pids"]["limit"], 7);
    assert_eq!(payload["devices"][0]["type"], "b");
    assert_eq!(payload["devices"][0]["allow"], false);
    assert_eq!(payload["devices"][0]["major"], 8);
    assert_eq!(payload["blockIO"]["weight"], 500);
}

#[tokio::test]
async fn nri_domain_evict_stops_container_and_persists_state() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let mut container = test_container("container-running", "pod-1", annotations.clone());
    container.state = ContainerState::ContainerRunning as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-running".to_string(), container);
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    domain.evict("container-running", "policy").await.unwrap();

    let containers = service.containers.lock().await;
    assert_eq!(
        containers.get("container-running").unwrap().state,
        ContainerState::ContainerExited as i32
    );
    drop(containers);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.nri_stop_notified);
}

#[tokio::test]
async fn inspect_and_info_endpoints_return_consistent_snapshots() {
    let (dir, service) = test_service_with_fake_runtime();
    let netns_path = dir.path().join("inspect.netns");
    fs::write(&netns_path, "netns").unwrap();
    let log_path = dir.path().join("logs").join("inspect.log");

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-inspect".to_string()),
            ip: Some("10.88.0.10".to_string()),
            additional_ips: vec!["fd00::10".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-inspect".to_string(),
        test_pod("pod-inspect", pod_annotations),
    );

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            run_as_user: Some("1000".to_string()),
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                image: String::new(),
                image_sub_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-inspect".to_string(),
        test_container("container-inspect", "pod-inspect", container_annotations),
    );
    set_fake_runtime_state(&dir, "container-inspect", "running");

    let status = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let container = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-inspect".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let pod = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-inspect".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    let pods =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    let config: serde_json::Value =
        serde_json::from_str(status.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeFeatures"]["containerEvents"], false);
    assert_eq!(container.status.as_ref().unwrap().reason, "Running");
    assert_eq!(container.status.as_ref().unwrap().mounts.len(), 1);
    assert!(container.info.contains_key("info"));
    assert_eq!(
        pod.status.as_ref().unwrap().network.as_ref().unwrap().ip,
        "10.88.0.10"
    );
    assert_eq!(pod.containers_statuses.len(), 1);
    assert_eq!(containers.containers.len(), 1);
    assert_eq!(pods.items.len(), 1);
}

#[tokio::test]
async fn list_container_stats_applies_id_pod_and_label_filters() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut container_a = test_container("container-alpha-123", "pod-alpha", HashMap::new());
    container_a
        .labels
        .insert("app".to_string(), "api".to_string());
    let mut container_b = test_container("container-bravo-456", "pod-bravo", HashMap::new());
    container_b
        .labels
        .insert("app".to_string(), "worker".to_string());

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(
            "pod-alpha".to_string(),
            test_pod("pod-alpha", HashMap::new()),
        );
        pods.insert(
            "pod-bravo".to_string(),
            test_pod("pod-bravo", HashMap::new()),
        );
    }
    {
        let mut containers = service.containers.lock().await;
        containers.insert(container_a.id.clone(), container_a);
        containers.insert(container_b.id.clone(), container_b);
    }
    set_fake_runtime_state(&dir, "container-alpha-123", "running");
    set_fake_runtime_state(&dir, "container-bravo-456", "running");

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "api".to_string());
    let response = RuntimeService::list_container_stats(
        &service,
        Request::new(ListContainerStatsRequest {
            filter: Some(crate::proto::runtime::v1::ContainerStatsFilter {
                id: "container-alpha".to_string(),
                pod_sandbox_id: "pod-alpha".to_string(),
                label_selector: selector,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.stats.len(), 1);
    assert_eq!(
        response.stats[0]
            .attributes
            .as_ref()
            .expect("container stats should include attributes")
            .id,
        "container-alpha-123"
    );
}

#[tokio::test]
async fn list_pod_sandbox_prefers_ready_newer_sandboxes_first() {
    let service = test_service();

    let old_not_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-old".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-1".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 1,
        }),
        state: PodSandboxState::SandboxNotready as i32,
        created_at: 1,
        ..Default::default()
    };
    let new_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-new".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-1".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 2,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: 2,
        ..Default::default()
    };
    let newer_not_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-newer-not-ready".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "kube-controller-manager-vm-crius".to_string(),
            uid: "uid-2".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 3,
        }),
        state: PodSandboxState::SandboxNotready as i32,
        created_at: 3,
        ..Default::default()
    };

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(old_not_ready.id.clone(), old_not_ready);
        pods.insert(new_ready.id.clone(), new_ready);
        pods.insert(newer_not_ready.id.clone(), newer_not_ready);
    }

    let response =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    let ids: Vec<_> = response.items.into_iter().map(|pod| pod.id).collect();
    assert_eq!(ids[0], "pod-new");
    assert_eq!(ids[1], "pod-newer-not-ready");
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn pod_sandbox_status_demotes_stale_ready_duplicate_sandbox() {
    let service = test_service();

    let old_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-old-ready".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-duplicate".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 1,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: 1,
        ..Default::default()
    };
    let new_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-new-ready".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-duplicate".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 2,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: 2,
        ..Default::default()
    };

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(old_ready.id.clone(), old_ready);
        pods.insert(new_ready.id.clone(), new_ready);
    }

    let old_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-old-ready".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let new_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-new-ready".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(old_status.state, PodSandboxState::SandboxNotready as i32);
    assert_eq!(new_status.state, PodSandboxState::SandboxReady as i32);
    assert_eq!(list.items[0].id, "pod-new-ready");
    assert_eq!(list.items[0].state, PodSandboxState::SandboxReady as i32);
    assert_eq!(list.items.len(), 1);
}

#[test]
fn pause_process_root_match_scopes_to_runtime_pod_root() {
    let pod_root = PathBuf::from("/var/lib/crius/pods");
    assert!(
        RuntimeServiceImpl::pause_process_root_matches_runtime_pod_root(
            Path::new("/var/lib/crius/pods/pod-a/pause-rootfs"),
            &pod_root
        )
    );
    assert!(
        !RuntimeServiceImpl::pause_process_root_matches_runtime_pod_root(Path::new("/"), &pod_root)
    );
    assert!(
        !RuntimeServiceImpl::pause_process_root_matches_runtime_pod_root(
            Path::new("/var/lib/other-runtime/pods/pod-a/pause-rootfs"),
            &pod_root
        )
    );
}

#[test]
fn convert_to_proto_container_stats_preserves_optional_fields() {
    let service = test_service();
    let stats = crate::metrics::ContainerStats {
        container_id: "container-stats-proto".to_string(),
        cpu: Some(crate::metrics::CpuStats {
            usage_total: 11,
            usage_user: 7,
            usage_kernel: 5,
            ..Default::default()
        }),
        memory: Some(crate::metrics::MemoryStats {
            usage: 101,
            limit: 1000,
            rss: 55,
            pgfault: 9,
            pgmajfault: 3,
            swap: 77,
            ..Default::default()
        }),
        timestamp: 1234,
        ..Default::default()
    };

    let proto = service.convert_to_proto_container_stats(stats);
    assert_eq!(
        proto
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.usage_core_nano_seconds.as_ref())
            .map(|value| value.value),
        Some(11)
    );
    assert_eq!(
        proto
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.usage_nano_cores.as_ref())
            .map(|value| value.value),
        Some(12)
    );
    assert_eq!(
        proto
            .memory
            .as_ref()
            .and_then(|memory| memory.working_set_bytes.as_ref())
            .map(|value| value.value),
        Some(101)
    );
    assert_eq!(
        proto
            .memory
            .as_ref()
            .and_then(|memory| memory.available_bytes.as_ref())
            .map(|value| value.value),
        Some(899)
    );
    assert_eq!(
        proto
            .swap
            .as_ref()
            .and_then(|swap| swap.swap_usage_bytes.as_ref())
            .map(|value| value.value),
        Some(77)
    );
}

#[test]
fn stats_cache_is_fresh_only_within_positive_period() {
    let now = std::time::Instant::now();
    let just_before = now - std::time::Duration::from_secs(4);
    let just_after = now - std::time::Duration::from_secs(6);

    assert!(RuntimeServiceImpl::stats_cache_is_fresh(
        just_before,
        5,
        now
    ));
    assert!(!RuntimeServiceImpl::stats_cache_is_fresh(
        just_after, 5, now
    ));
    assert!(!RuntimeServiceImpl::stats_cache_is_fresh(
        just_before,
        0,
        now
    ));
}

#[test]
fn build_pod_metrics_returns_all_metrics_when_configured_with_all() {
    let service = test_service();
    let metrics = service.build_pod_metrics(
        "pod-all",
        1,
        crate::server::stats::PodMetricTotals {
            cpu_usage: 10,
            memory_usage: 20,
            memory_limit: 30,
            pids: 40,
            filesystem_usage: 50,
            rx_bytes: 60,
            tx_bytes: 70,
        },
    );
    let names: Vec<&str> = metrics.iter().map(|metric| metric.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "container_cpu_usage_seconds_total",
            "container_memory_working_set_bytes",
            "container_memory_usage_bytes",
            "container_spec_memory_limit_bytes",
            "container_pids_current",
            "container_filesystem_usage_bytes",
            "pod_network_receive_bytes_total",
            "pod_network_transmit_bytes_total",
        ]
    );
}

#[tokio::test]
async fn list_containers_returns_empty_when_short_id_filter_is_ambiguous() {
    let service = test_service();

    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-ambiguous-a".to_string(),
            test_container("container-ambiguous-a", "pod-a", HashMap::new()),
        );
        containers.insert(
            "container-ambiguous-b".to_string(),
            test_container("container-ambiguous-b", "pod-b", HashMap::new()),
        );
    }

    let response = RuntimeService::list_containers(
        &service,
        Request::new(ListContainersRequest {
            filter: Some(crate::proto::runtime::v1::ContainerFilter {
                id: "container-ambiguous".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        response.containers.is_empty(),
        "ambiguous short container ids should behave like cri-o filters and return no entries"
    );
}

#[tokio::test]
async fn container_stats_resolves_short_id_and_list_container_stats_skips_exited() {
    let (dir, service) = test_service_with_fake_runtime();

    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-running-123".to_string(),
            test_container("container-running-123", "pod-1", HashMap::new()),
        );
        containers.insert(
            "container-stopped-456".to_string(),
            test_container("container-stopped-456", "pod-1", HashMap::new()),
        );
    }
    set_fake_runtime_state(&dir, "container-running-123", "running");
    set_fake_runtime_state(&dir, "container-stopped-456", "stopped");

    let stats = RuntimeService::container_stats(
        &service,
        Request::new(ContainerStatsRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .stats
    .expect("short container id should resolve to stats");
    assert_eq!(
        stats
            .attributes
            .as_ref()
            .expect("container stats should include attributes")
            .id,
        "container-running-123"
    );

    let list = RuntimeService::list_container_stats(
        &service,
        Request::new(ListContainerStatsRequest { filter: None }),
    )
    .await
    .unwrap()
    .into_inner();

    let ids = list
        .stats
        .iter()
        .filter_map(|stat| {
            stat.attributes
                .as_ref()
                .map(|attributes| attributes.id.clone())
        })
        .collect::<Vec<_>>();
    assert!(
        ids.iter().any(|id| id == "container-running-123"),
        "running containers should remain visible in list stats"
    );
    assert!(
        !ids.iter().any(|id| id == "container-stopped-456"),
        "exited containers should be filtered out from list stats by default"
    );
}

#[tokio::test]
async fn list_pod_sandbox_stats_applies_id_and_label_filters() {
    let service = test_service();

    let mut pod_alpha = test_pod("pod-alpha", HashMap::new());
    pod_alpha
        .labels
        .insert("tier".to_string(), "frontend".to_string());
    let pod_alpha_uid = pod_alpha
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();

    let mut pod_bravo = test_pod("pod-bravo", HashMap::new());
    pod_bravo
        .labels
        .insert("tier".to_string(), "backend".to_string());
    let pod_bravo_uid = pod_bravo
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert("pod-alpha".to_string(), pod_alpha);
        pods.insert("pod-bravo".to_string(), pod_bravo);
    }

    let mut container_alpha_annotations = HashMap::new();
    container_alpha_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_alpha_uid);
    let mut container_bravo_annotations = HashMap::new();
    container_bravo_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_bravo_uid);
    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-alpha".to_string(),
            test_container("container-alpha", "pod-alpha", container_alpha_annotations),
        );
        containers.insert(
            "container-bravo".to_string(),
            test_container("container-bravo", "pod-bravo", container_bravo_annotations),
        );
    }

    let mut selector = HashMap::new();
    selector.insert("tier".to_string(), "frontend".to_string());
    let response = RuntimeService::list_pod_sandbox_stats(
        &service,
        Request::new(ListPodSandboxStatsRequest {
            filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                id: "pod-al".to_string(),
                label_selector: selector,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.stats.len(), 1);
    assert_eq!(
        response.stats[0]
            .attributes
            .as_ref()
            .expect("pod stats should include attributes")
            .id,
        "pod-alpha"
    );
}

#[tokio::test]
async fn pod_sandbox_stats_resolves_short_id_and_hides_internal_annotations() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut pod_annotations = HashMap::new();
    pod_annotations.insert("visible".to_string(), "true".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-pod-short-123".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-short-123".to_string(),
        test_pod("pod-short-123", pod_annotations),
    );
    service.containers.lock().await.insert(
        "container-pod-short-123".to_string(),
        test_container("container-pod-short-123", "pod-short-123", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-pod-short-123", "running");
    set_fake_runtime_state(&dir, "pause-pod-short-123", "running");

    let response = RuntimeService::pod_sandbox_stats(
        &service,
        Request::new(PodSandboxStatsRequest {
            pod_sandbox_id: "pod-short".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let stats = response
        .stats
        .expect("short pod id should resolve to pod sandbox stats");
    let attributes = stats
        .attributes
        .expect("pod stats should include attributes");
    assert_eq!(attributes.id, "pod-short-123");
    assert_eq!(
        attributes.annotations.get("visible").map(String::as_str),
        Some("true")
    );
    assert!(
        !attributes.annotations.contains_key(INTERNAL_POD_STATE_KEY),
        "internal pod annotations should not leak through statsp responses"
    );
}

#[test]
fn parse_network_stats_from_procfs_aggregates_non_loopback_interfaces() {
    let raw = "\
Inter-|   Receive                                                |  Transmit\n\
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
    lo: 100 1 0 0 0 0 0 0 100 1 0 0 0 0 0 0\n\
  eth0: 200 2 3 4 0 0 0 0 300 5 6 7 0 0 0 0\n\
  eth1: 400 8 9 10 0 0 0 0 500 11 12 13 0 0 0 0\n";

    let stats = RuntimeServiceImpl::parse_network_stats_from_procfs(raw)
        .expect("non-loopback interfaces should produce network stats");
    assert_eq!(stats.rx_bytes, 600);
    assert_eq!(stats.rx_packets, 10);
    assert_eq!(stats.rx_errors, 12);
    assert_eq!(stats.rx_dropped, 14);
    assert_eq!(stats.tx_bytes, 800);
    assert_eq!(stats.tx_packets, 16);
    assert_eq!(stats.tx_errors, 18);
    assert_eq!(stats.tx_dropped, 20);
}

#[tokio::test]
async fn collect_pod_stats_returns_none_when_pod_has_no_collectable_containers() {
    let service = test_service();
    let pod = test_pod("pod-no-stats", HashMap::new());

    let stats = service.collect_pod_stats("pod-no-stats", &pod).await;
    assert!(stats.is_none());
}

#[tokio::test]
async fn list_pod_sandbox_stats_returns_empty_when_short_id_filter_is_ambiguous() {
    let service = test_service();

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(
            "pod-ambiguous-a".to_string(),
            test_pod("pod-ambiguous-a", HashMap::new()),
        );
        pods.insert(
            "pod-ambiguous-b".to_string(),
            test_pod("pod-ambiguous-b", HashMap::new()),
        );
    }

    let response = RuntimeService::list_pod_sandbox_stats(
        &service,
        Request::new(ListPodSandboxStatsRequest {
            filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                id: "pod-ambiguous".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        response.stats.is_empty(),
        "ambiguous short pod ids should return no stats entries"
    );
}

#[tokio::test]
async fn list_metric_descriptors_returns_non_empty_supported_names() {
    let service = test_service();
    let response = RuntimeService::list_metric_descriptors(
        &service,
        Request::new(ListMetricDescriptorsRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        !response.descriptors.is_empty(),
        "metric descriptors should not be empty"
    );
    let names: HashSet<String> = response
        .descriptors
        .into_iter()
        .map(|descriptor| descriptor.name)
        .collect();
    assert!(names.contains("container_cpu_usage_seconds_total"));
    assert!(names.contains("container_memory_working_set_bytes"));
    assert!(names.contains("container_memory_usage_bytes"));
    assert!(names.contains("container_spec_memory_limit_bytes"));
}

#[tokio::test]
async fn list_pod_sandbox_metrics_returns_pod_and_container_entries() {
    let service = test_service();

    let pod = test_pod("pod-metrics", HashMap::new());
    let pod_uid = pod
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-metrics".to_string(), pod);

    let mut container_annotations = HashMap::new();
    container_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_uid);
    service.containers.lock().await.insert(
        "container-metrics".to_string(),
        test_container("container-metrics", "pod-metrics", container_annotations),
    );

    let response = RuntimeService::list_pod_sandbox_metrics(
        &service,
        Request::new(ListPodSandboxMetricsRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.pod_metrics.len(), 1);
    let pod_metrics = &response.pod_metrics[0];
    assert_eq!(pod_metrics.pod_sandbox_id, "pod-metrics");
    assert!(
        !pod_metrics.metrics.is_empty(),
        "pod-level metrics should not be empty"
    );
    assert_eq!(pod_metrics.container_metrics.len(), 1);
    assert_eq!(
        pod_metrics.container_metrics[0].container_id,
        "container-metrics"
    );
    assert!(
        !pod_metrics.container_metrics[0].metrics.is_empty(),
        "container-level metrics should not be empty"
    );
}

#[tokio::test]
async fn list_pod_sandbox_metrics_respects_included_pod_metrics_configuration() {
    let mut service = test_service();
    service.config.included_pod_metrics = vec!["cpu".to_string(), "network".to_string()];

    let pod_metrics = service.build_pod_metrics(
        "pod-metrics-filtered",
        1,
        crate::server::stats::PodMetricTotals {
            cpu_usage: 10,
            memory_usage: 20,
            memory_limit: 30,
            pids: 40,
            filesystem_usage: 50,
            rx_bytes: 60,
            tx_bytes: 70,
        },
    );
    let metric_names: Vec<&str> = pod_metrics
        .iter()
        .map(|metric| metric.name.as_str())
        .collect();
    assert_eq!(
        metric_names,
        vec![
            "container_cpu_usage_seconds_total",
            "pod_network_receive_bytes_total",
            "pod_network_transmit_bytes_total",
        ]
    );
}

#[tokio::test]
async fn get_container_events_streams_broadcast_events() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.publish_event(ContainerEventResponse {
        container_id: "container-1".to_string(),
        container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.container_id, "container-1");
    assert_eq!(
        event.container_event_type,
        ContainerEventType::ContainerCreatedEvent as i32
    );
}

#[tokio::test]
async fn get_container_events_broadcasts_to_multiple_subscribers() {
    let service = test_service();
    let mut stream_a =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();
    let mut stream_b =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.publish_event(ContainerEventResponse {
        container_id: "shared".to_string(),
        container_event_type: ContainerEventType::ContainerStartedEvent as i32,
        created_at: 2,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event_a = stream_a.next().await.unwrap().unwrap();
    let event_b = stream_b.next().await.unwrap().unwrap();
    assert_eq!(event_a.container_id, "shared");
    assert_eq!(event_b.container_id, "shared");
    assert_eq!(event_a.container_event_type, event_b.container_event_type);
}

#[tokio::test]
async fn get_container_events_preserves_publish_order() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    for (id, event_type) in [
        ("created", ContainerEventType::ContainerCreatedEvent),
        ("started", ContainerEventType::ContainerStartedEvent),
        ("stopped", ContainerEventType::ContainerStoppedEvent),
        ("deleted", ContainerEventType::ContainerDeletedEvent),
    ] {
        service.publish_event(ContainerEventResponse {
            container_id: id.to_string(),
            container_event_type: event_type as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    let mut received = Vec::new();
    for _ in 0..4 {
        let event = stream.next().await.unwrap().unwrap();
        received.push((event.container_id, event.container_event_type));
    }

    assert_eq!(
        received,
        vec![
            (
                "created".to_string(),
                ContainerEventType::ContainerCreatedEvent as i32
            ),
            (
                "started".to_string(),
                ContainerEventType::ContainerStartedEvent as i32
            ),
            (
                "stopped".to_string(),
                ContainerEventType::ContainerStoppedEvent as i32
            ),
            (
                "deleted".to_string(),
                ContainerEventType::ContainerDeletedEvent as i32
            ),
        ]
    );
}

#[tokio::test]
async fn pod_events_use_pause_container_id_when_available_and_preserve_order() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-event-1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let pod = test_pod("pod-event-1", annotations);

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service
        .emit_pod_event(ContainerEventType::ContainerCreatedEvent, &pod, Vec::new())
        .await;
    service
        .emit_pod_event(ContainerEventType::ContainerStartedEvent, &pod, Vec::new())
        .await;

    let created = stream.next().await.unwrap().unwrap();
    let started = stream.next().await.unwrap().unwrap();
    assert_eq!(created.container_id, "pause-event-1");
    assert_eq!(started.container_id, "pause-event-1");
    assert_eq!(
        created.container_event_type,
        ContainerEventType::ContainerCreatedEvent as i32
    );
    assert_eq!(
        started.container_event_type,
        ContainerEventType::ContainerStartedEvent as i32
    );
    assert_eq!(
        started
            .pod_sandbox_status
            .expect("pod status should be included")
            .id,
        "pod-event-1"
    );
}

#[tokio::test]
async fn pod_events_can_be_disabled() {
    let mut service = test_service();
    service.config.enable_pod_events = false;

    let pod = test_pod("pod-event-disabled", HashMap::new());
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service
        .emit_pod_event(ContainerEventType::ContainerCreatedEvent, &pod, Vec::new())
        .await;

    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "disabled pod events should not enqueue stream items"
    );
}

#[tokio::test]
async fn publish_event_without_subscribers_does_not_panic() {
    let service = test_service();
    service.publish_event(ContainerEventResponse {
        container_id: "orphan".to_string(),
        container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });
}

#[tokio::test]
async fn get_container_events_reports_lagged_consumers() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    for idx in 0..600 {
        service.publish_event(ContainerEventResponse {
            container_id: format!("container-{}", idx),
            container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
            created_at: idx,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    let mut saw_lagged = false;
    for _ in 0..260 {
        match timeout(Duration::from_millis(200), stream.next()).await {
            Ok(Some(Err(status))) if status.code() == tonic::Code::ResourceExhausted => {
                saw_lagged = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }

    assert!(saw_lagged, "expected lagged consumer error");
}

#[tokio::test]
async fn exit_monitor_publishes_async_stop_events() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "async-stop".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("async-stop", "pod-1", annotations)
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "async-stop",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &service
                .containers
                .lock()
                .await
                .get("async-stop")
                .unwrap()
                .annotations,
        )
        .unwrap();
    service.ensure_exit_monitor_registered("async-stop");

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    let exit_code_path = dir.path().join("exits").join("async-stop");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "17").unwrap();

    let event = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Ok(event)) = stream.next().await {
                if event.container_id == "async-stop"
                    && event.container_event_type
                        == ContainerEventType::ContainerStoppedEvent as i32
                {
                    return event;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for async stop event");

    assert_eq!(event.container_id, "async-stop");
    let container = service
        .containers
        .lock()
        .await
        .get("async-stop")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(17));
    assert!(state.nri_stop_notified);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("async-stop")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.nri_stop_notified);
}

#[tokio::test]
async fn port_forward_validates_pod_state() {
    let service = test_service();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut not_ready_pod = test_pod("pod-2", HashMap::new());
    not_ready_pod.state = PodSandboxState::SandboxNotready as i32;
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-2".to_string(), not_ready_pod);
    let not_ready = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-2".to_string(),
            port: Vec::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(not_ready.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn port_forward_requires_existing_netns_and_returns_stream_url() {
    let service = test_service();
    let mut missing_netns_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut missing_netns_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-missing-netns".to_string(),
        test_pod("pod-missing-netns", missing_netns_annotations),
    );
    let missing_netns = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-missing-netns".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing_netns.code(), tonic::Code::FailedPrecondition);

    let netns_dir = tempdir().unwrap();
    let netns_path = netns_dir.path().join("netns");
    fs::write(&netns_path, "placeholder").unwrap();
    let mut ready_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut ready_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-ready".to_string(),
        test_pod("pod-ready", ready_annotations),
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;
    let response = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-ready".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/portforward/"));

    let mut host_network_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut host_network_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            namespace_options: Some(StoredNamespaceOptions {
                network: NamespaceMode::Node as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-host-network".to_string(),
        test_pod("pod-host-network", host_network_annotations),
    );
    let host_network = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-host-network".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(host_network.url.contains("/portforward/"));

    let multi_port = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-ready".to_string(),
            port: vec![80, 443],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(multi_port.url.contains("/portforward/"));
}

#[tokio::test]
async fn port_forward_rejects_invalid_ports() {
    let service = test_service();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let zero = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: vec![0],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(zero.code(), tonic::Code::InvalidArgument);
    assert!(zero.message().contains("1..=65535"));

    let too_large = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: vec![65536],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(too_large.code(), tonic::Code::InvalidArgument);
    assert!(too_large.message().contains("1..=65535"));
}

#[tokio::test]
async fn port_forward_refreshes_stale_pause_container_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-portforward", "stopped");

    let netns_dir = tempdir().unwrap();
    let netns_path = netns_dir.path().join("netns");
    fs::write(&netns_path, "placeholder").unwrap();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-portforward".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-stale-ready".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-stale-ready", annotations)
        },
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let err = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-stale-ready".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("not ready"));
}

#[tokio::test]
async fn stop_and_remove_existing_container_support_repeat_calls() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "container-1", "running");
    service.containers.lock().await.insert(
        "container-1".to_string(),
        test_container("container-1", "pod-1", HashMap::new()),
    );

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-1".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-1".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    set_fake_runtime_state(&dir, "container-2", "running");
    service.containers.lock().await.insert(
        "container-2".to_string(),
        test_container("container-2", "pod-1", HashMap::new()),
    );
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-2".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-2".to_string(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_and_remove_existing_pod_support_repeat_calls() {
    let (dir, service) = test_service_with_fake_runtime();

    let pod = test_pod("pod-1", HashMap::new());
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), pod);
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-1".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-1".to_string(),
        }),
    )
    .await
    .unwrap();

    set_fake_runtime_state(&dir, "container-under-pod", "running");
    service.containers.lock().await.insert(
        "container-under-pod".to_string(),
        test_container("container-under-pod", "pod-2", HashMap::new()),
    );
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-2".to_string(), test_pod("pod-2", HashMap::new()));
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-2".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-2".to_string(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_pod_sandbox_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    set_fake_runtime_state(&dir, "pause-repeat", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-repeat".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let pod = test_pod("pod-repeat", annotations.clone());
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-repeat".to_string(), pod);
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: "pod-repeat".to_string(),
            config: crate::pod::PodSandboxConfig {
                name: "pod-repeat".to_string(),
                namespace: "default".to_string(),
                uid: "uid-repeat".to_string(),
                hostname: "pod-repeat".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: vec![],
                annotations: vec![],
                dns_config: None,
                port_mappings: vec![],
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: vec![],
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from("/var/run/netns/pod-repeat"),
            pause_container_id: "pause-repeat".to_string(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: RuntimeServiceImpl::now_nanos(),
            ip: String::new(),
            network_status: None,
        });
    }
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-repeat",
            "ready",
            "pod-repeat",
            "default",
            "uid-repeat",
            "/var/run/netns/pod-repeat",
            &HashMap::new(),
            &annotations,
            Some("pause-repeat"),
            None,
        )
        .unwrap();

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-repeat".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-repeat".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(fake_nri.stop_pod_events.lock().await.len(), 1);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_pod"]);
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-repeat")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &persisted_annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.stop_notified);
}

#[tokio::test]
async fn stop_pod_sandbox_fallback_cleans_pause_when_pod_manager_state_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-fallback-stop", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-fallback-stop".to_string()),
            netns_path: Some("/var/run/netns/pod-fallback-stop".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-fallback-stop".to_string(),
        test_pod("pod-fallback-stop", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-fallback-stop",
            "ready",
            "pod-fallback-stop",
            "default",
            "uid-fallback-stop",
            "/var/run/netns/pod-fallback-stop",
            &HashMap::new(),
            &annotations,
            Some("pause-fallback-stop"),
            None,
        )
        .unwrap();

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-fallback-stop".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(!fake_runtime_state_path(&dir, "pause-fallback-stop").exists());
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-fallback-stop")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn remove_pod_sandbox_fallback_cleans_workspace_when_pod_manager_state_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-fallback-remove", "running");
    let pod_workspace = dir
        .path()
        .join("root")
        .join("pods")
        .join("pod-fallback-remove");
    fs::create_dir_all(&pod_workspace).unwrap();
    fs::write(pod_workspace.join("marker"), "workspace").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-fallback-remove".to_string()),
            netns_path: Some("/var/run/netns/pod-fallback-remove".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-fallback-remove".to_string(),
        test_pod("pod-fallback-remove", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-fallback-remove",
            "ready",
            "pod-fallback-remove",
            "default",
            "uid-fallback-remove",
            "/var/run/netns/pod-fallback-remove",
            &HashMap::new(),
            &annotations,
            Some("pause-fallback-remove"),
            None,
        )
        .unwrap();

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-fallback-remove".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(!fake_runtime_state_path(&dir, "pause-fallback-remove").exists());
    assert!(!pod_workspace.exists());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-fallback-remove")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn recover_state_reconciles_running_container_to_exited() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "stopped");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("recover-container")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(0));
    assert!(state.finished_at.is_some());

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
}

#[tokio::test]
async fn recover_state_marks_ready_pod_notready_when_pause_is_stopped() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-1", "stopped");
    let netns_path = dir.path().join("pause-netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-1".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-1".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-recover")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn recover_state_does_not_replay_historical_events() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "stopped");

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_container(&ContainerRecord {
            id: "recover-container".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: String::new(),
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        })
        .unwrap();

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.recover_state().await.unwrap();

    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "recover_state should not replay historical events"
    );
}

#[tokio::test]
async fn recover_state_re_registers_exit_monitor_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    set_fake_runtime_state(&dir, "recover-running", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_container(&ContainerRecord {
            id: "recover-running".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: String::new(),
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        })
        .unwrap();

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.recover_state().await.unwrap();

    let exit_code_path = dir.path().join("exits").join("recover-running");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "23").unwrap();

    let event = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Ok(event)) = stream.next().await {
                if event.container_id == "recover-running"
                    && event.container_event_type
                        == ContainerEventType::ContainerStoppedEvent as i32
                {
                    return event;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for recovered container stop event");

    assert_eq!(event.container_id, "recover-running");
    let container = service
        .containers
        .lock()
        .await
        .get("recover-running")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(23));
    assert!(state.nri_stop_notified);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
}

#[tokio::test]
async fn recover_state_supports_inspect_and_list_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ip: Some("10.88.0.20".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: Some("10.88.0.20".to_string()),
        })
        .unwrap();

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-container".to_string()),
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let container_status = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "recover-container".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let pod_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-recover".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    let pods =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(
        container_status.status.as_ref().unwrap().state,
        ContainerState::ContainerRunning as i32
    );
    assert_eq!(
        pod_status.status.as_ref().unwrap().state,
        PodSandboxState::SandboxReady as i32
    );
    assert_eq!(containers.containers.len(), 1);
    assert_eq!(containers.containers[0].pod_sandbox_id, "pod-recover");
    assert_eq!(pods.items.len(), 1);
    let info: serde_json::Value =
        serde_json::from_str(container_status.info.get("info").unwrap()).unwrap();
    assert_eq!(info["sandboxID"], "pod-recover");
}

#[tokio::test]
async fn recover_state_restores_pod_hostname_from_internal_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-hostname.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            hostname: Some("restored-hostname".to_string()),
            port_mappings: vec![StoredPortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: String::new(),
            }],
            raw_cni_result: Some(serde_json::json!({
                "cniVersion": "1.0.0",
                "ips": [
                    {"address": "fd00::10/64"},
                    {"address": "10.88.0.11/16"}
                ]
            })),
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            readonly_rootfs: true,
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-name".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = {
        let pod_manager = service.pod_manager.lock().await;
        pod_manager
            .get_pod_sandbox_cloned("pod-recover")
            .expect("pod should be recovered")
    };
    assert_eq!(pod.config.hostname, "restored-hostname");
    assert_eq!(pod.config.port_mappings.len(), 1);
    assert_eq!(pod.config.port_mappings[0].host_port, 8080);
    assert!(pod.config.readonly_rootfs);
    let network_status = pod
        .network_status
        .expect("raw cni result should restore network");
    assert_eq!(network_status.ip.unwrap().to_string(), "fd00::10");
    assert_eq!(
        network_status.interfaces[0].ip.unwrap().to_string(),
        "10.88.0.11"
    );
}

#[tokio::test]
async fn recover_state_uses_configured_ipv4_preference_when_primary_ip_is_missing() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.cni_config = crate::network::CniConfig::new(
        Vec::new(),
        Vec::new(),
        dir.path().join("cache"),
        0,
        crate::network::MainIpPreference::Ipv4,
        None,
        false,
    );
    set_fake_runtime_state(&dir, "pause-recover-ip-pref", "running");
    let netns_path = dir.path().join("recover-ip-pref.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            raw_cni_result: Some(serde_json::json!({
                "cniVersion": "1.0.0",
                "ips": [
                    {"address": "fd00::10/64"},
                    {"address": "10.88.0.11/16"},
                    {"address": "fd00::12/64"}
                ]
            })),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover-ip-pref".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover-ip-pref".to_string(),
            state: "ready".to_string(),
            name: "pod-ip-pref".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover-ip-pref".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover-ip-pref".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = {
        let pod_manager = service.pod_manager.lock().await;
        pod_manager
            .get_pod_sandbox_cloned("pod-recover-ip-pref")
            .expect("pod should be recovered")
    };
    let network_status = pod
        .network_status
        .expect("raw cni result should restore network");
    assert_eq!(network_status.ip.unwrap().to_string(), "10.88.0.11");
    assert_eq!(network_status.interfaces.len(), 2);
    assert_eq!(
        network_status.interfaces[0].ip.unwrap().to_string(),
        "fd00::10"
    );
    assert_eq!(
        network_status.interfaces[1].ip.unwrap().to_string(),
        "fd00::12"
    );
}

#[tokio::test]
async fn recover_state_restores_pod_userns_options_from_internal_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-recover-userns", "running");
    let netns_path = dir.path().join("recover-userns.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover-userns".to_string()),
            namespace_options: Some(StoredNamespaceOptions {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: Some(StoredUserNamespace {
                    mode: NamespaceMode::Pod as i32,
                    uids: vec![StoredIdMapping {
                        host_id: 100000,
                        container_id: 0,
                        length: 65536,
                    }],
                    gids: vec![StoredIdMapping {
                        host_id: 200000,
                        container_id: 0,
                        length: 65536,
                    }],
                }),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover-userns".to_string(),
            state: "ready".to_string(),
            name: "pod-userns".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover-userns".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover-userns".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = {
        let pod_manager = service.pod_manager.lock().await;
        pod_manager
            .get_pod_sandbox_cloned("pod-recover-userns")
            .expect("pod should be recovered")
    };
    let userns = pod
        .config
        .namespace_options
        .and_then(|options| options.userns_options)
        .expect("userns options should be restored");
    assert_eq!(userns.mode, NamespaceMode::Pod as i32);
    assert_eq!(userns.uids[0].host_id, 100000);
    assert_eq!(userns.gids[0].host_id, 200000);
}

#[tokio::test]
async fn recover_state_supports_stop_and_remove_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-remove.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "recover-container".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "recover-container".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .is_none());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn recover_state_marks_ready_pod_notready_when_netns_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-missing-netns", "running");

    let missing_netns_path = dir.path().join("missing.netns");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(missing_netns_path.display().to_string()),
            pause_container_id: Some("pause-missing-netns".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-missing-netns".to_string(),
            state: "ready".to_string(),
            name: "pod-missing-netns".to_string(),
            namespace: "default".to_string(),
            uid: "uid-missing-netns".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: missing_netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-missing-netns".to_string()),
            ip: Some("10.88.0.21".to_string()),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-missing-netns")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-missing-netns")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn recover_state_preserves_ready_pod_when_pause_runtime_state_is_unknown() {
    let (dir, service) = test_service_with_fake_runtime();
    let netns_path = dir.path().join("recover-unknown.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-unknown".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover-unknown".to_string(),
            state: "ready".to_string(),
            name: "pod-recover-unknown".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover-unknown".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-unknown".to_string()),
            ip: Some("10.88.0.22".to_string()),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-recover-unknown")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxReady as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover-unknown")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "ready");
}

#[tokio::test]
async fn list_pod_sandbox_preserves_ready_state_when_pause_running_but_pid_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-running", "running");
    let _ = fs::remove_file(dir.path().join("runtime-state").join("pause-running.pid"));

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-ready-running-no-pid".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "pod-ready-running-no-pid".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "kube-apiserver-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: HashMap::from([(
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            )]),
            annotations,
            runtime_handler: "runc".to_string(),
        },
    );

    let response =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(response.items.len(), 1);
    assert_eq!(
        response.items[0].state,
        PodSandboxState::SandboxReady as i32
    );
}

#[tokio::test]
async fn runtime_state_refresh_keeps_sandbox_ready_when_pause_running_but_pid_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-refresh-running", "running");
    let _ = fs::remove_file(
        dir.path()
            .join("runtime-state")
            .join("pause-refresh-running.pid"),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-refresh-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-refresh-running-no-pid".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "pod-refresh-running-no-pid".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "kube-apiserver-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: HashMap::from([(
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            )]),
            annotations,
            runtime_handler: "runc".to_string(),
        },
    );

    RuntimeServiceImpl::refresh_runtime_state_and_publish_events(
        &service.runtime,
        &service.config,
        &service.containers,
        &service.pod_sandboxes,
        &service.persistence,
        &service.events,
    )
    .await;

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-refresh-running-no-pid")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxReady as i32);
}

#[tokio::test]
async fn remove_pod_sandbox_cascades_recovered_containers_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-cascade.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-container".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(service
        .containers
        .lock()
        .await
        .get("recover-container")
        .is_none());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn container_status_refreshes_stale_runtime_state_on_query() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "refresh-container", "stopped");

    service.containers.lock().await.insert(
        "refresh-container".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("refresh-container", "pod-1", HashMap::new())
        },
    );

    let response = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "refresh-container".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(
        response.status.unwrap().state,
        ContainerState::ContainerExited as i32
    );
}

#[tokio::test]
async fn pod_queries_refresh_pause_container_state_on_query() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-refresh", "stopped");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-refresh".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-refresh".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-refresh", annotations)
        },
    );

    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].state, PodSandboxState::SandboxNotready as i32);

    let inspect = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-refresh".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        inspect.status.unwrap().state,
        PodSandboxState::SandboxNotready as i32
    );
}

#[tokio::test]
async fn pod_queries_mark_ready_sandbox_notready_when_pause_runtime_state_is_unknown() {
    let (_dir, service) = test_service_with_fake_runtime();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-unknown".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-unknown".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-unknown", annotations)
        },
    );

    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].state, PodSandboxState::SandboxNotready as i32);

    let inspect = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-unknown".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        inspect.status.unwrap().state,
        PodSandboxState::SandboxNotready as i32
    );
}

#[tokio::test]
async fn list_pod_sandbox_deduplicates_logical_control_plane_duplicates() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-old", "stopped");
    set_fake_runtime_state(&dir, "pause-new", "running");

    let make_annotations = |pause_container_id: &str| {
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                pause_container_id: Some(pause_container_id.to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        annotations
    };

    service.pod_sandboxes.lock().await.insert(
        "sandbox-old".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "sandbox-old".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "etcd-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxNotready as i32,
            created_at: 100,
            labels: HashMap::from([
                ("component".to_string(), "etcd".to_string()),
                (
                    "io.kubernetes.pod.uid".to_string(),
                    "logical-pod-uid".to_string(),
                ),
            ]),
            annotations: make_annotations("pause-old"),
            ..Default::default()
        },
    );
    service.pod_sandboxes.lock().await.insert(
        "sandbox-new".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "sandbox-new".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "etcd-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 2,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: 200,
            labels: HashMap::from([
                ("component".to_string(), "etcd".to_string()),
                (
                    "io.kubernetes.pod.uid".to_string(),
                    "logical-pod-uid".to_string(),
                ),
            ]),
            annotations: make_annotations("pause-new"),
            ..Default::default()
        },
    );

    let response =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(response.items.len(), 1);
    assert_eq!(response.items[0].id, "sandbox-new");
    assert_eq!(
        response.items[0].state,
        PodSandboxState::SandboxReady as i32
    );
    assert_eq!(
        response.items[0].metadata.as_ref().map(|m| m.attempt),
        Some(2)
    );
}

#[tokio::test]
async fn list_containers_deduplicates_logical_control_plane_duplicates() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "ctr-old", "stopped");
    set_fake_runtime_state(&dir, "ctr-new", "running");

    service.pod_sandboxes.lock().await.insert(
        "pod-1".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "pod-1".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "kube-controller-manager-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: 10,
            labels: HashMap::from([
                (
                    "component".to_string(),
                    "kube-controller-manager".to_string(),
                ),
                (
                    "io.kubernetes.pod.uid".to_string(),
                    "logical-pod-uid".to_string(),
                ),
            ]),
            ..Default::default()
        },
    );

    let old = Container {
        id: "ctr-old".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "kube-controller-manager".to_string(),
            attempt: 1,
        }),
        state: ContainerState::ContainerExited as i32,
        created_at: 100,
        image: Some(ImageSpec {
            image: "k8s.m.daocloud.io/kube-controller-manager:v1.29.1".to_string(),
            ..Default::default()
        }),
        image_ref: "sha256:old".to_string(),
        labels: HashMap::from([
            (
                "component".to_string(),
                "kube-controller-manager".to_string(),
            ),
            (
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            ),
        ]),
        annotations: HashMap::new(),
    };
    let new = Container {
        id: "ctr-new".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "kube-controller-manager".to_string(),
            attempt: 2,
        }),
        state: ContainerState::ContainerRunning as i32,
        created_at: 200,
        image: Some(ImageSpec {
            image: "k8s.m.daocloud.io/kube-controller-manager:v1.29.1".to_string(),
            ..Default::default()
        }),
        image_ref: "sha256:new".to_string(),
        labels: HashMap::from([
            (
                "component".to_string(),
                "kube-controller-manager".to_string(),
            ),
            (
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            ),
        ]),
        annotations: HashMap::new(),
    };
    service.containers.lock().await.insert(old.id.clone(), old);
    service.containers.lock().await.insert(new.id.clone(), new);

    let response =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(response.containers.len(), 1);
    assert_eq!(response.containers[0].id, "ctr-new");
    assert_eq!(
        response.containers[0].state,
        ContainerState::ContainerRunning as i32
    );
    assert_eq!(
        response.containers[0]
            .metadata
            .as_ref()
            .map(|metadata| metadata.attempt),
        Some(2)
    );
}

#[tokio::test]
async fn recover_state_ignores_stale_shim_metadata_artifacts() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");

    let stale_dir = dir.path().join("shims").join("orphan");
    fs::create_dir_all(&stale_dir).unwrap();
    fs::write(stale_dir.join("attach.sock"), "stale").unwrap();
    fs::write(
        stale_dir.join("shim.json"),
        serde_json::to_vec_pretty(&crate::runtime::ShimProcess {
            container_id: "orphan".to_string(),
            shim_pid: 999_999,
            exit_code_file: stale_dir.join("exit_code"),
            log_file: stale_dir.join("shim.log"),
            socket_path: stale_dir.join("attach.sock"),
            bundle_path: dir.path().join("bundles").join("orphan"),
        })
        .unwrap(),
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(containers.containers.len(), 1);
    assert!(
        !stale_dir.exists(),
        "recover_state should clean orphaned shim artifacts"
    );
}

#[tokio::test]
async fn recover_state_skips_orphan_sweeps_after_clean_shutdown_marker() {
    let (dir, service) = test_service_with_fake_runtime();
    service.record_startup_clean_shutdown(true);

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(stale_shim_dir.exists());
    assert!(stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_does_not_skip_orphan_sweeps_after_reboot_even_if_last_shutdown_was_clean() {
    let (dir, service) = test_service_with_fake_runtime();
    service.record_startup_clean_shutdown(true);
    service.record_startup_detected_reboot(true);

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(!stale_shim_dir.exists());
    assert!(!stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_does_not_skip_orphan_sweeps_after_upgrade_even_if_last_shutdown_was_clean() {
    let (dir, service) = test_service_with_fake_runtime();
    service.record_startup_clean_shutdown(true);
    service.record_startup_detected_upgrade(true);

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(!stale_shim_dir.exists());
    assert!(!stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_skips_orphan_sweeps_when_internal_wipe_is_disabled() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.internal_wipe = false;

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(stale_shim_dir.exists());
    assert!(stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_cleans_orphaned_attach_socket_artifacts_from_separate_directory() {
    let (dir, mut service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    service.attach_socket_dir = dir.path().join("attach");

    let attach_dir = service.attach_socket_dir.join("orphan");
    let shim_dir = dir.path().join("shims").join("orphan");
    fs::create_dir_all(&attach_dir).unwrap();
    fs::create_dir_all(&shim_dir).unwrap();
    fs::write(attach_dir.join("attach.sock"), "stale").unwrap();
    fs::write(
        shim_dir.join("shim.json"),
        serde_json::to_vec_pretty(&crate::runtime::ShimProcess {
            container_id: "orphan".to_string(),
            shim_pid: 999_999,
            exit_code_file: shim_dir.join("exit_code"),
            log_file: shim_dir.join("shim.log"),
            socket_path: attach_dir.join("attach.sock"),
            bundle_path: dir.path().join("bundles").join("orphan"),
        })
        .unwrap(),
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(
        !attach_dir.exists(),
        "recover_state should clean orphaned attach socket directories"
    );
}

#[tokio::test]
async fn recover_state_recovers_running_container_without_shim_metadata_file_from_ledger() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-ledger-only", "running");
    fs::create_dir_all(dir.path().join("runtime-root").join("recover-ledger-only")).unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-ledger-only",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .update_container_ledger_metadata("recover-ledger-only", Some("runc"), Some("runc"), None)
        .unwrap();

    let mut shim_child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_shim_process_record(&crate::storage::ShimProcessRecord {
            container_id: "recover-ledger-only".to_string(),
            shim_pid: shim_child.id(),
            work_dir: dir.path().join("shims").display().to_string(),
            socket_path: dir
                .path()
                .join("shims")
                .join("recover-ledger-only")
                .join("task.sock")
                .display()
                .to_string(),
            exit_code_file: dir
                .path()
                .join("exits")
                .join("recover-ledger-only")
                .display()
                .to_string(),
            log_file: dir
                .path()
                .join("shims")
                .join("recover-ledger-only")
                .join("shim.log")
                .display()
                .to_string(),
            bundle_path: dir
                .path()
                .join("runtime-root")
                .join("recover-ledger-only")
                .display()
                .to_string(),
            state: "running".to_string(),
            last_seen_at: RuntimeServiceImpl::now_nanos(),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let recovered = service
        .containers
        .lock()
        .await
        .get("recover-ledger-only")
        .cloned()
        .unwrap();
    assert_eq!(recovered.state, ContainerState::ContainerRunning as i32);
    let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &recovered.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap_or_default();
    assert!(internal_state.broken.is_none());

    let _ = shim_child.kill();
    let _ = shim_child.wait();
}

#[tokio::test]
async fn recover_state_marks_container_broken_when_runtime_bundle_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "broken-bundle", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("broken-bundle".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "broken-bundle",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .update_container_ledger_metadata("broken-bundle", Some("runc"), Some("runc"), None)
        .unwrap();

    service.recover_state().await.unwrap();

    let recovered = service
        .containers
        .lock()
        .await
        .get("broken-bundle")
        .cloned()
        .unwrap();
    let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &recovered.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        internal_state
            .broken
            .as_ref()
            .map(|broken| broken.kind.as_str()),
        Some("bundle_missing")
    );
}

#[tokio::test]
async fn recover_state_keeps_live_orphaned_shim_directories_when_ledger_has_live_process() {
    let (dir, service) = test_service_with_fake_runtime();
    let live_shim_dir = dir.path().join("shims").join("orphan-live");
    let live_attach_dir = dir.path().join("attach").join("orphan-live");
    let stale_shim_dir = dir.path().join("shims").join("orphan-stale");
    let stale_attach_dir = dir.path().join("attach").join("orphan-stale");
    fs::create_dir_all(&live_shim_dir).unwrap();
    fs::create_dir_all(&live_attach_dir).unwrap();
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(live_attach_dir.join("attach.sock"), "live").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    let mut shim_child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_shim_process_record(&crate::storage::ShimProcessRecord {
            container_id: "orphan-live".to_string(),
            shim_pid: shim_child.id(),
            work_dir: dir.path().join("shims").display().to_string(),
            socket_path: live_attach_dir.join("attach.sock").display().to_string(),
            exit_code_file: dir
                .path()
                .join("exits")
                .join("orphan-live")
                .display()
                .to_string(),
            log_file: live_shim_dir.join("shim.log").display().to_string(),
            bundle_path: dir
                .path()
                .join("runtime-root")
                .join("orphan-live")
                .display()
                .to_string(),
            state: "running".to_string(),
            last_seen_at: RuntimeServiceImpl::now_nanos(),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(live_shim_dir.exists());
    assert!(live_attach_dir.exists());
    assert!(!stale_shim_dir.exists());
    assert!(!stale_attach_dir.exists());

    let _ = shim_child.kill();
    let _ = shim_child.wait();
}

#[tokio::test]
async fn recover_state_cleans_orphaned_runtime_bundles_but_keeps_recovered_ones() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");

    let recovered_bundle = dir.path().join("runtime-root").join("recover-container");
    let orphan_bundle = dir.path().join("runtime-root").join("orphan-bundle");
    let live_orphan_bundle = dir.path().join("runtime-root").join("live-orphan");
    fs::create_dir_all(&recovered_bundle).unwrap();
    fs::create_dir_all(&orphan_bundle).unwrap();
    fs::create_dir_all(&live_orphan_bundle).unwrap();
    fs::write(recovered_bundle.join("config.json"), "{}").unwrap();
    fs::write(orphan_bundle.join("config.json"), "{}").unwrap();
    fs::write(live_orphan_bundle.join("config.json"), "{}").unwrap();
    set_fake_runtime_state(&dir, "live-orphan", "running");

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(
        recovered_bundle.exists(),
        "recover_state should keep recovered runtime bundles"
    );
    assert!(
        !orphan_bundle.exists(),
        "recover_state should clean orphaned runtime bundles"
    );
    assert!(
        live_orphan_bundle.exists(),
        "recover_state should not delete live runtime bundles that still have runtime state"
    );
}

#[tokio::test]
async fn recover_state_cleans_orphaned_pod_workspaces_but_keeps_recovered_ones() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-recover", "running");

    let recovered_workspace = dir.path().join("root").join("pods").join("pod-recover");
    let orphan_workspace = dir.path().join("root").join("pods").join("orphan-pod");
    fs::create_dir_all(&recovered_workspace).unwrap();
    fs::create_dir_all(&orphan_workspace).unwrap();
    fs::write(
        recovered_workspace.join("resolv.conf"),
        "nameserver 8.8.8.8",
    )
    .unwrap();
    fs::write(orphan_workspace.join("marker"), "stale").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-recover",
            "ready",
            "pod-recover-name",
            "default",
            "pod-recover-uid",
            "",
            &HashMap::new(),
            &annotations,
            Some("pause-recover"),
            None,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(
        recovered_workspace.exists(),
        "recover_state should keep recovered pod workspaces"
    );
    assert!(
        !orphan_workspace.exists(),
        "recover_state should clean orphaned pod workspaces"
    );
}

#[test]
fn runtime_mounts_from_proto_resolves_oci_artifact_mounts() {
    let (dir, service) = test_service_with_fake_runtime();
    let artifact_dir = service
        .config
        .image_root
        .join("artifacts")
        .join("sha256:test-artifact");
    fs::create_dir_all(&artifact_dir).unwrap();
    fs::write(artifact_dir.join("0.tar.gz"), b"hello artifact").unwrap();
    fs::write(
        artifact_dir.join("metadata.json"),
        serde_json::to_vec(&crate::image::CriusImage {
            id: "sha256:test-artifact".to_string(),
            repo_tags: vec!["registry.example.com/artifact:latest".to_string()],
            repo_digests: vec!["registry.example.com/artifact@sha256:test-artifact".to_string()],
            size: 14,
            pinned: false,
            pulled_at: 0,
            source_reference: None,
            os: None,
            architecture: None,
            config_user: None,
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: Some("application/vnd.example.artifact".to_string()),
            artifact_blobs: vec![crate::image::ArtifactBlobMeta {
                digest: "sha256:blob".to_string(),
                media_type: "text/plain".to_string(),
                path: "artifact.txt".to_string(),
                size: 14,
                annotations: HashMap::from([(
                    "org.opencontainers.image.title".to_string(),
                    "artifact.txt".to_string(),
                )]),
            }],
        })
        .unwrap(),
    )
    .unwrap();

    let mounts = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/root/artifact".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: Some(crate::proto::runtime::v1::ImageSpec {
                    image: "registry.example.com/artifact:latest".to_string(),
                    user_specified_image: "registry.example.com/artifact:latest".to_string(),
                    runtime_handler: String::new(),
                    annotations: HashMap::new(),
                }),
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap();

    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].source, artifact_dir.join("0.tar.gz"));
    assert_eq!(
        mounts[0].destination,
        PathBuf::from("/root/artifact/artifact.txt")
    );
    assert!(mounts[0].read_only);
    assert_eq!(
        mounts[0].requested_image.as_deref(),
        Some("registry.example.com/artifact:latest")
    );

    drop(dir);
}

#[test]
fn runtime_mounts_from_proto_rejects_oci_artifact_file_target() {
    let (dir, service) = test_service_with_fake_runtime();
    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/root/artifact.txt".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: Some(crate::proto::runtime::v1::ImageSpec {
                    image: "registry.example.com/artifact:latest".to_string(),
                    user_specified_image: "registry.example.com/artifact:latest".to_string(),
                    runtime_handler: String::new(),
                    annotations: HashMap::new(),
                }),
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("directory"));

    drop(dir);
}

#[test]
fn runtime_mounts_from_proto_rejects_missing_host_path_for_bind_mount() {
    let (_dir, service) = test_service_with_fake_runtime();
    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("host_path"));
}

#[test]
fn runtime_mounts_from_proto_preserves_mount_semantics_for_runtime() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mounts = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: true,
                selinux_relabel: true,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationHostToContainer
                    as i32,
                uid_mappings: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 1000,
                    container_id: 0,
                    length: 1,
                }],
                gid_mappings: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 2000,
                    container_id: 0,
                    length: 1,
                }],
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap();

    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].source, PathBuf::from("/host/data"));
    assert_eq!(mounts[0].destination, PathBuf::from("/data"));
    assert!(mounts[0].read_only);
    assert_eq!(
        mounts[0].missing_source_policy,
        crate::runtime::MissingMountSourcePolicy::CreateDirectory
    );
    assert!(mounts[0].selinux_relabel);
    assert_eq!(
        mounts[0].propagation,
        crate::runtime::MountPropagationMode::HostToContainer
    );
    assert_eq!(mounts[0].uid_mappings.len(), 1);
    assert_eq!(mounts[0].gid_mappings.len(), 1);
}

#[test]
fn runtime_mounts_from_proto_rejects_recursive_read_only_with_non_private_propagation() {
    let (_dir, service) = test_service_with_fake_runtime();
    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: true,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationHostToContainer
                    as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: true,
                image: None,
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("propagation"));
}

#[test]
fn runtime_mounts_from_proto_keeps_restore_mount_sources_strict() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mounts = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            }],
            false,
        )
        .unwrap();

    assert_eq!(
        mounts[0].missing_source_policy,
        crate::runtime::MissingMountSourcePolicy::Reject
    );
}

#[test]
fn runtime_mount_validation_reports_rro_unsupported_as_failed_precondition() {
    let (dir, service) = test_service_with_fake_runtime();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let runtime = service.runtime.runtime_for_handler("runc").unwrap();
    let config = test_runtime_container_config_with_mounts(vec![MountConfig {
        source,
        destination: PathBuf::from("/data"),
        read_only: true,
        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: crate::runtime::MountPropagationMode::Private,
        recursive_read_only: true,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }]);

    let err = runtime.validate_mount_requests(&config).unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("recursive_read_only"));
}

#[test]
fn runtime_mount_validation_reports_selinux_relabel_without_label() {
    let (dir, service) = test_service_with_fake_runtime();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let runtime = service.runtime.runtime_for_handler("runc").unwrap();
    let config = test_runtime_container_config_with_mounts(vec![MountConfig {
        source,
        destination: PathBuf::from("/data"),
        read_only: false,
        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
        selinux_relabel: true,
        propagation: crate::runtime::MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }]);

    let err = runtime.validate_mount_requests(&config).unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("SELinux"));
}

#[test]
fn runtime_mount_validation_reports_missing_source_as_failed_precondition() {
    let (dir, service) = test_service_with_fake_runtime();
    let source = dir.path().join("missing");
    let runtime = service.runtime.runtime_for_handler("runc").unwrap();
    let config = test_runtime_container_config_with_mounts(vec![MountConfig {
        source: source.clone(),
        destination: PathBuf::from("/data"),
        read_only: false,
        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: crate::runtime::MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }]);

    let err = runtime.validate_mount_requests(&config).unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains(source.to_string_lossy().as_ref()));
}
