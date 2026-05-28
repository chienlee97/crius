use std::collections::HashMap;
use std::path::PathBuf;

use crius::runtime::{
    ContainerConfig, ContainerStatus, NamespacePaths, RuntimeBackend, RuntimeContextKind,
    TaskController, WasmDirectBackend, WasmDirectBackendOptions,
};

#[path = "../common/mod.rs"]
mod common;
use common::{DirectTaskRuntimeBackend, FakeRuntimeBackend, FakeRuntimeCall, FakeRuntimeOperation};

fn test_container_config() -> ContainerConfig {
    ContainerConfig {
        name: "container-1".to_string(),
        image: "image-1".to_string(),
        command: vec!["sleep".to_string()],
        args: vec!["1".to_string()],
        env: Vec::new(),
        working_dir: None,
        mounts: Vec::new(),
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
        rootfs: PathBuf::from("/tmp/rootfs"),
    }
}

#[test]
fn fake_runtime_backend_exposes_runtime_contract() {
    let dir = tempfile::tempdir().unwrap();
    let runtime_root = dir.path().join("runtime");
    let backend = FakeRuntimeBackend::new(&runtime_root)
        .with_backend_name("fake-runc")
        .with_status(ContainerStatus::Running);

    assert_eq!(backend.backend_name(), "fake-runc");
    assert_eq!(
        backend.runtime_context().bundle_path_for("container-1"),
        runtime_root.join("container-1")
    );
    assert!(matches!(
        backend
            .task_controller()
            .container_status("container-1")
            .unwrap(),
        ContainerStatus::Running
    ));
}

#[test]
fn fake_runtime_backend_records_calls_and_state_transitions() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FakeRuntimeBackend::new(dir.path().join("runtime"));
    let config = test_container_config();

    backend.create_container("container-1", &config).unwrap();
    assert_eq!(backend.status_snapshot(), ContainerStatus::Created);
    backend.start_container("container-1").unwrap();
    assert_eq!(backend.status_snapshot(), ContainerStatus::Running);
    backend.stop_container("container-1", Some(10)).unwrap();
    assert_eq!(backend.status_snapshot(), ContainerStatus::Stopped(0));
    backend.remove_container("container-1").unwrap();
    assert_eq!(backend.status_snapshot(), ContainerStatus::Unknown);

    assert_eq!(
        backend.calls(),
        vec![
            FakeRuntimeCall::CreateContainer {
                container_id: "container-1".to_string()
            },
            FakeRuntimeCall::StartContainer {
                container_id: "container-1".to_string()
            },
            FakeRuntimeCall::StopContainer {
                container_id: "container-1".to_string(),
                timeout: Some(10)
            },
            FakeRuntimeCall::RemoveContainer {
                container_id: "container-1".to_string()
            },
        ]
    );
}

#[test]
fn fake_runtime_backend_can_script_operation_failures() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FakeRuntimeBackend::new(dir.path().join("runtime")).with_failure(
        FakeRuntimeOperation::StartContainer,
        "scripted start failure",
    );
    let config = test_container_config();

    backend.create_container("container-1", &config).unwrap();
    let err = backend.start_container("container-1").unwrap_err();

    assert!(err.to_string().contains("scripted start failure"));
    assert_eq!(backend.status_snapshot(), ContainerStatus::Created);
    assert_eq!(
        backend.calls(),
        vec![
            FakeRuntimeCall::CreateContainer {
                container_id: "container-1".to_string()
            },
            FakeRuntimeCall::StartContainer {
                container_id: "container-1".to_string()
            },
        ]
    );
}

#[test]
fn direct_task_backend_advertises_non_oci_contract() {
    let dir = tempfile::tempdir().unwrap();
    let backend = DirectTaskRuntimeBackend::new(dir.path().join("direct-runtime"));
    let config = test_container_config();

    assert_eq!(backend.backend_name(), "direct-task");
    assert_eq!(backend.context_kind(), RuntimeContextKind::DirectTask);
    assert!(backend
        .runtime_context()
        .prepare_rootfs("container-1", &config)
        .unwrap_err()
        .to_string()
        .contains("must not use OCI context operation"));

    backend.create_container("container-1", &config).unwrap();
    backend.start_container("container-1").unwrap();

    assert_eq!(
        backend.calls(),
        vec![
            FakeRuntimeCall::CreateContainer {
                container_id: "container-1".to_string()
            },
            FakeRuntimeCall::StartContainer {
                container_id: "container-1".to_string()
            },
        ]
    );
}

#[test]
fn wasm_direct_backend_lifecycle_is_persisted_without_oci_context() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WasmDirectBackend::new(
        "/bin/true",
        dir.path().join("runtime"),
        "",
        WasmDirectBackendOptions::default(),
    );
    let config = test_container_config();

    assert_eq!(backend.backend_name(), "wasm-direct");
    assert_eq!(backend.context_kind(), RuntimeContextKind::DirectTask);
    assert!(backend
        .runtime_context()
        .prepare_rootfs("wasm-1", &config)
        .unwrap_err()
        .to_string()
        .contains("does not support OCI context operation"));

    backend.create_container("wasm-1", &config).unwrap();
    assert_eq!(
        backend.container_status("wasm-1").unwrap(),
        ContainerStatus::Created
    );
    backend.start_container("wasm-1").unwrap();
    assert_eq!(
        backend.container_status("wasm-1").unwrap(),
        ContainerStatus::Running
    );
    backend.stop_container("wasm-1", Some(1)).unwrap();
    assert_eq!(
        backend.container_status("wasm-1").unwrap(),
        ContainerStatus::Stopped(0)
    );

    let recovered = WasmDirectBackend::new(
        "/bin/true",
        dir.path().join("runtime"),
        "",
        WasmDirectBackendOptions::default(),
    );
    assert_eq!(
        recovered.container_status("wasm-1").unwrap(),
        ContainerStatus::Stopped(0)
    );
    recovered.remove_container("wasm-1").unwrap();
    assert_eq!(
        recovered.container_status("wasm-1").unwrap(),
        ContainerStatus::Unknown
    );
}

#[test]
fn wasm_direct_backend_rejects_unsupported_features_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let backend = WasmDirectBackend::new(
        "/bin/true",
        dir.path().join("runtime"),
        "",
        WasmDirectBackendOptions::default(),
    );
    let config = test_container_config();

    backend.create_container("wasm-1", &config).unwrap();
    backend.start_container("wasm-1").unwrap();

    assert!(backend
        .exec_in_container("wasm-1", &["sh".to_string()], false)
        .unwrap_err()
        .to_string()
        .contains("does not support task operation exec_in_container"));
    assert!(backend
        .pause_container("wasm-1")
        .unwrap_err()
        .to_string()
        .contains("does not support task operation pause_container"));
    assert_eq!(backend.container_pid("wasm-1").unwrap(), None);
}
