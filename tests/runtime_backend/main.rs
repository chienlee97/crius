use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
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

fn write_fake_wasm_engine(dir: &std::path::Path) -> PathBuf {
    let engine_path = dir.join("fake-wasm-engine.sh");
    fs::write(
        &engine_path,
        r#"#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' "fake-wasm-engine 1.0.0"
  exit 0
fi
if [ "${1:-}" != "run" ]; then
  printf '%s\n' "unsupported command: ${1:-}" >&2
  exit 64
fi
shift
id=""
state_dir=""
sandboxer=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id)
      id="$2"
      shift 2
      ;;
    --state-dir)
      state_dir="$2"
      shift 2
      ;;
    --sandboxer)
      sandboxer="$2"
      shift 2
      ;;
    *)
      image="$1"
      shift
      break
      ;;
  esac
done
mkdir -p "$state_dir"
printf '%s\n' "$id" "$sandboxer" "$image" "$@" > "$state_dir/engine.args"
trap 'printf "%s\n" 0 > "$state_dir/exit_code"; exit 0' TERM INT
while :; do sleep 0.05; done
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&engine_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&engine_path, perms).unwrap();
    engine_path
}

fn wait_for_file(path: &std::path::Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("timed out waiting for {}", path.display());
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
    let engine_path = write_fake_wasm_engine(dir.path());
    let backend = WasmDirectBackend::new(
        &engine_path,
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
    assert!(backend.container_pid("wasm-1").unwrap().is_some());
    let engine_args_path = dir
        .path()
        .join("runtime")
        .join("wasm-direct")
        .join("wasm-1")
        .join("engine.args");
    wait_for_file(&engine_args_path);
    let engine_args = fs::read_to_string(engine_args_path).unwrap();
    assert_eq!(
        engine_args.lines().collect::<Vec<_>>(),
        ["wasm-1", "process", "image-1", "sleep", "1"]
    );
    backend.stop_container("wasm-1", Some(1)).unwrap();
    assert_eq!(
        backend.container_status("wasm-1").unwrap(),
        ContainerStatus::Stopped(0)
    );

    let recovered = WasmDirectBackend::new(
        &engine_path,
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
    let engine_path = write_fake_wasm_engine(dir.path());
    let backend = WasmDirectBackend::new(
        &engine_path,
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
    assert!(backend.container_pid("wasm-1").unwrap().is_some());
    backend.remove_container("wasm-1").unwrap();
}
