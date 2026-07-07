use super::*;
use crate::services::{EventService, InternalEventSeverity};
use crate::storage::persistence::{PersistenceConfig, PersistenceManager};
use crate::storage::{SnapshotRecord, StorageManager};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tempfile::tempdir;

fn parse_cri_log_lines(contents: &str) -> Vec<(String, String, String, String)> {
    contents
        .lines()
        .map(|line| {
            let mut parts = line.splitn(4, ' ');
            (
                parts.next().unwrap_or_default().to_string(),
                parts.next().unwrap_or_default().to_string(),
                parts.next().unwrap_or_default().to_string(),
                parts.next().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

fn new_attach_test_daemon(temp_dir: &tempfile::TempDir, container_id: &str) -> Daemon {
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();
    let daemon = Daemon::new(
        container_id.to_string(),
        bundle_dir,
        temp_dir.path().join("runtime"),
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: None,
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );
    daemon.set_task_state(DaemonTaskState::Created);
    daemon
}

fn open_attach_stream(daemon: &Daemon, container_id: &str, tty: bool) -> OpenAttachStreamResponse {
    daemon
        .open_attach_stream_internal(&OpenAttachStreamRequest {
            container_id: container_id.to_string(),
            stdin: true,
            stdout: true,
            stderr: !tty,
            tty,
        })
        .unwrap()
}

#[test]
fn attach_resize_requires_open_stream_id() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");

    let err = daemon
        .resize_attach_pty_internal(&crate::shim_rpc::ResizeAttachPtyRequest {
            container_id: "container-1".to_string(),
            stream_id: Some("attach-container-1-1".to_string()),
            width: 80,
            height: 24,
        })
        .unwrap_err();

    assert!(err.to_string().contains("is not open"));
}

#[test]
fn attach_resize_rejects_missing_stream_id() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");

    let err = daemon
        .resize_attach_pty_internal(&crate::shim_rpc::ResizeAttachPtyRequest {
            container_id: "container-1".to_string(),
            stream_id: None,
            width: 80,
            height: 24,
        })
        .unwrap_err();

    assert!(err.to_string().contains("requires stream id"));
}

#[test]
fn attach_resize_rejects_closed_stream() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");
    let stream = open_attach_stream(&daemon, "container-1", true);

    daemon
        .close_attach_stream_internal(&crate::shim_rpc::CloseAttachStreamRequest {
            container_id: "container-1".to_string(),
            stream_id: stream.stream_id.clone(),
        })
        .unwrap();
    let err = daemon
        .resize_attach_pty_internal(&crate::shim_rpc::ResizeAttachPtyRequest {
            container_id: "container-1".to_string(),
            stream_id: Some(stream.stream_id),
            width: 80,
            height: 24,
        })
        .unwrap_err();

    assert!(err.to_string().contains("is not open"));
}

#[test]
fn attach_resize_rejects_non_tty_stream() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");
    let stream = open_attach_stream(&daemon, "container-1", false);

    let err = daemon
        .resize_attach_pty_internal(&crate::shim_rpc::ResizeAttachPtyRequest {
            container_id: "container-1".to_string(),
            stream_id: Some(stream.stream_id),
            width: 80,
            height: 24,
        })
        .unwrap_err();

    assert!(err.to_string().contains("is not a TTY stream"));
}

#[test]
fn attach_resize_for_open_tty_stream_reaches_io_manager() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");
    let stream = open_attach_stream(&daemon, "container-1", true);

    let err = daemon
        .resize_attach_pty_internal(&crate::shim_rpc::ResizeAttachPtyRequest {
            container_id: "container-1".to_string(),
            stream_id: Some(stream.stream_id),
            width: 80,
            height: 24,
        })
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("terminal console is not available"));
}

#[test]
fn attach_stream_rejects_mismatched_container_id() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");

    let err = daemon
        .open_attach_stream_internal(&OpenAttachStreamRequest {
            container_id: "missing-container".to_string(),
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        })
        .unwrap_err();

    assert!(err.to_string().contains("does not match shim container"));
}

#[test]
fn attach_close_rejects_duplicate_close() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");
    let stream = open_attach_stream(&daemon, "container-1", true);

    daemon
        .close_attach_stream_internal(&crate::shim_rpc::CloseAttachStreamRequest {
            container_id: "container-1".to_string(),
            stream_id: stream.stream_id.clone(),
        })
        .unwrap();
    let err = daemon
        .close_attach_stream_internal(&crate::shim_rpc::CloseAttachStreamRequest {
            container_id: "container-1".to_string(),
            stream_id: stream.stream_id,
        })
        .unwrap_err();

    assert!(err.to_string().contains("is not open"));
}

#[test]
fn attach_streams_close_when_task_stops() {
    let temp_dir = tempdir().unwrap();
    let daemon = new_attach_test_daemon(&temp_dir, "container-1");
    let stream = open_attach_stream(&daemon, "container-1", true);

    daemon.close_all_attach_streams();
    daemon.set_task_state(DaemonTaskState::Stopped);

    let resize_err = daemon
        .resize_attach_pty_internal(&crate::shim_rpc::ResizeAttachPtyRequest {
            container_id: "container-1".to_string(),
            stream_id: Some(stream.stream_id.clone()),
            width: 80,
            height: 24,
        })
        .unwrap_err();
    assert!(resize_err.to_string().contains("is not open"));

    let close_err = daemon
        .close_attach_stream_internal(&crate::shim_rpc::CloseAttachStreamRequest {
            container_id: "container-1".to_string(),
            stream_id: stream.stream_id,
        })
        .unwrap_err();
    assert!(close_err.to_string().contains("is not open"));
}

#[test]
fn shim_daemon_owns_rootfs_snapshot_lifecycle() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state").join("crius.db");
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": "rootfs"
            },
            "process": {
                "terminal": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let rootfs = temp_dir
        .path()
        .join("snapshots")
        .join("ctr1")
        .join("rootfs");
    fs::create_dir_all(rootfs.join("etc")).unwrap();
    fs::write(rootfs.join("etc/hostname"), "ctr1").unwrap();

    let mut storage = StorageManager::new(&db_path).unwrap();
    storage
        .save_snapshot(&SnapshotRecord {
            key: "ctr1".to_string(),
            image_id: "sha256:test".to_string(),
            owner_kind: "container".to_string(),
            owner_id: "ctr1".to_string(),
            state: "prepared".to_string(),
            mountpoint: rootfs.display().to_string(),
            snapshotter: "internal-overlay-untar".to_string(),
            runtime_managed: true,
        })
        .unwrap();
    drop(storage);

    let daemon = Daemon::new(
        "ctr1".to_string(),
        bundle_dir.clone(),
        temp_dir.path().join("runtime"),
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: Some(db_path.clone()),
            exit_code_file: None,
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    daemon
        .handle_request(ShimRpcRequest::CreateTask(CreateTaskRequest {
            container_id: "ctr1".to_string(),
            rootfs_path: rootfs.clone(),
            snapshot_key: Some("ctr1".to_string()),
            mount_options: vec!["rw".to_string()],
            rootfs: Some(crate::image::snapshotter::RootfsHandle::internal_path(
                "ctr1",
                "container",
                "ctr1",
                rootfs.clone(),
                false,
            )),
        }))
        .unwrap();

    let config: serde_json::Value =
        serde_json::from_slice(&fs::read(bundle_dir.join("config.json")).unwrap()).unwrap();
    assert_eq!(config["root"]["path"], rootfs.display().to_string());
    let storage = StorageManager::new(&db_path).unwrap();
    let snapshot = storage
        .list_snapshots()
        .unwrap()
        .into_iter()
        .find(|record| record.key == "ctr1")
        .unwrap();
    assert_eq!(snapshot.state, "mounted");
    drop(storage);

    daemon
        .handle_request(ShimRpcRequest::DeleteTask(DeleteTaskRequest {
            container_id: "ctr1".to_string(),
            snapshot_key: None,
            rootfs_path: None,
        }))
        .unwrap();

    assert!(!rootfs.exists());
    assert!(StorageManager::new(&db_path)
        .unwrap()
        .list_snapshots()
        .unwrap()
        .is_empty());
}

#[test]
fn create_task_mount_failure_marks_snapshot_broken() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state").join("crius.db");
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": "rootfs"
            },
            "process": {
                "terminal": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let rootfs = temp_dir.path().join("external-rootfs");
    let target = temp_dir.path().join("mount-target");

    let mut storage = StorageManager::new(&db_path).unwrap();
    storage
        .save_snapshot(&SnapshotRecord {
            key: "external-ctr".to_string(),
            image_id: "sha256:test".to_string(),
            owner_kind: "container".to_string(),
            owner_id: "ctr1".to_string(),
            state: "prepared".to_string(),
            mountpoint: rootfs.display().to_string(),
            snapshotter: "external-test".to_string(),
            runtime_managed: false,
        })
        .unwrap();
    drop(storage);

    let daemon = Daemon::new(
        "ctr1".to_string(),
        bundle_dir,
        temp_dir.path().join("runtime"),
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: Some(db_path.clone()),
            exit_code_file: None,
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    let err = daemon
        .handle_request(ShimRpcRequest::CreateTask(CreateTaskRequest {
            container_id: "ctr1".to_string(),
            rootfs_path: rootfs.clone(),
            snapshot_key: Some("external-ctr".to_string()),
            mount_options: Vec::new(),
            rootfs: Some(
                crate::image::snapshotter::RootfsHandle::external_mount_spec(
                    "external-ctr",
                    "container",
                    "ctr1",
                    rootfs,
                    vec![crate::image::snapshotter::RootfsMountSpec {
                        mount_type: "definitely-not-a-real-fs".to_string(),
                        source: PathBuf::new(),
                        target,
                        options: Vec::new(),
                    }],
                    false,
                ),
            ),
        }))
        .unwrap_err();

    assert!(err.to_string().contains("Failed to mount rootfs"));
    let snapshot = StorageManager::new(&db_path)
        .unwrap()
        .list_snapshots()
        .unwrap()
        .into_iter()
        .find(|record| record.key == "external-ctr")
        .unwrap();
    assert_eq!(snapshot.state, "broken");
}

#[tokio::test]
async fn shim_daemon_records_task_and_exec_internal_events() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state").join("crius.db");
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let daemon = Daemon::new(
        "container-1".to_string(),
        bundle_dir,
        temp_dir.path().join("runtime"),
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: Some(db_path.clone()),
            exit_code_file: None,
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    daemon.set_task_state(DaemonTaskState::Created);
    daemon.record_exec_event("exit:0:[\"/bin/true\"]").unwrap();

    let persistence = Arc::new(tokio::sync::Mutex::new(
        PersistenceManager::new(PersistenceConfig {
            db_path,
            enable_recovery: true,
            auto_save_interval: 30,
        })
        .unwrap(),
    ));
    let events = EventService::with_capacity(16).with_ledger(persistence);

    let task_events = events
        .recent_internal_events("task", "container-1", 10)
        .await
        .unwrap();
    assert_eq!(task_events.len(), 1);
    assert_eq!(task_events[0].kind, "task.state");
    assert_eq!(task_events[0].severity, InternalEventSeverity::Info);
    assert_eq!(task_events[0].details["previousState"], "init");
    assert_eq!(task_events[0].details["state"], "created");

    let shim_events = events
        .recent_internal_events("shim", "container-1", 10)
        .await
        .unwrap();
    assert_eq!(shim_events.len(), 1);
    assert_eq!(shim_events[0].kind, "exec.event");
    assert_eq!(shim_events[0].severity, InternalEventSeverity::Info);
    assert_eq!(shim_events[0].details["message"], "exit:0:[\"/bin/true\"]");
}

#[test]
fn pipe_exec_session_only_opens_interactive_stdin_when_requested() {
    let temp_dir = tempdir().unwrap();
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let args_path = temp_dir.path().join("runtime.args");
    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    fs::write(
        &runtime_path,
        format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
exit 0
"#,
            args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let daemon = Daemon::new(
        "container-1".to_string(),
        bundle_dir,
        runtime_path,
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: None,
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );
    let io_manager = IoManager::new();

    daemon
        .serve_pipe_exec_session(
            &OpenExecSessionRequest {
                container_id: "container-1".to_string(),
                command: vec!["/bin/true".to_string()],
                tty: false,
                stdin: false,
                stdout: true,
                stderr: true,
                exec_cpu_affinity: None,
            },
            &io_manager,
        )
        .unwrap();
    let args = fs::read_to_string(&args_path).unwrap();
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        vec!["exec", "container-1", "/bin/true"]
    );

    daemon
        .serve_pipe_exec_session(
            &OpenExecSessionRequest {
                container_id: "container-1".to_string(),
                command: vec!["/bin/cat".to_string()],
                tty: false,
                stdin: true,
                stdout: true,
                stderr: true,
                exec_cpu_affinity: None,
            },
            &io_manager,
        )
        .unwrap();
    let args = fs::read_to_string(&args_path).unwrap();
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        vec!["exec", "-i", "container-1", "/bin/cat"]
    );
}

#[test]
fn test_non_terminal_container_stdio_capture() {
    let temp_dir = tempdir().unwrap();
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let log_path = temp_dir.path().join("logs").join("container.log");
    let internal_state = json!({
        "log_path": log_path.to_string_lossy(),
        "tty": false,
        "stdin": false,
        "stdin_once": false,
    });
    let config = json!({
        "process": {
            "terminal": false
        },
        "annotations": {
            INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
        }
    });
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();

    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
set -eu

cmd="$1"
shift || true

case "$cmd" in
  run)
    echo "stdout:hello"
    echo "stderr:world" >&2
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let exit_code_file = temp_dir.path().join("shim").join("exit_code");
    fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
    let mut daemon = Daemon::new(
        "test-container".to_string(),
        bundle_dir,
        runtime_path,
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: Some(exit_code_file),
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    daemon
        .io_manager
        .configure(IoConfig {
            stdout: Some(log_path.clone()),
            terminal: false,
            ..Default::default()
        })
        .unwrap();

    let exit_code = daemon.run_non_terminal_container().unwrap();
    assert_eq!(exit_code, 0);

    let log_content = fs::read_to_string(&log_path).unwrap();
    let records = parse_cri_log_lines(&log_content);
    assert_eq!(records.len(), 2);
    for record in &records {
        assert!(chrono::DateTime::parse_from_rfc3339(&record.0).is_ok());
        assert_eq!(record.2, "F");
    }
    assert!(records
        .iter()
        .any(|record| record.1 == "stdout" && record.3 == "stdout:hello"));
    assert!(records
        .iter()
        .any(|record| record.1 == "stderr" && record.3 == "stderr:world"));
}

#[test]
fn test_non_terminal_container_passes_no_pivot_when_enabled() {
    let temp_dir = tempdir().unwrap();
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let log_path = temp_dir.path().join("logs").join("container.log");
    let args_path = temp_dir.path().join("runtime.args");
    let internal_state = json!({
        "log_path": log_path.to_string_lossy(),
        "tty": false,
        "stdin": false,
        "stdin_once": false,
    });
    let config = json!({
        "process": {
            "terminal": false
        },
        "annotations": {
            INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
        }
    });
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();

    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    fs::write(
        &runtime_path,
        format!(
            r#"#!/bin/sh
set -eu

cmd="$1"
shift || true

case "$cmd" in
  run)
    printf '%s\n' "$@" > "{}"
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
            args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let exit_code_file = temp_dir.path().join("shim").join("exit_code");
    fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
    let mut daemon = Daemon::new(
        "test-container".to_string(),
        bundle_dir,
        runtime_path,
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: Some(exit_code_file),
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: true,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    daemon
        .io_manager
        .configure(IoConfig {
            stdout: Some(log_path),
            terminal: false,
            ..Default::default()
        })
        .unwrap();

    daemon.run_non_terminal_container().unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-pivot"));
}

#[test]
fn test_non_terminal_container_passes_no_new_keyring_when_enabled() {
    let temp_dir = tempdir().unwrap();
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let log_path = temp_dir.path().join("logs").join("container.log");
    let args_path = temp_dir.path().join("runtime.args");
    let internal_state = json!({
        "log_path": log_path.to_string_lossy(),
        "tty": false,
        "stdin": false,
        "stdin_once": false,
    });
    let config = json!({
        "process": {
            "terminal": false
        },
        "annotations": {
            INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
        }
    });
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();

    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    fs::write(
        &runtime_path,
        format!(
            r#"#!/bin/sh
set -eu

cmd="$1"
shift || true

case "$cmd" in
  run)
    printf '%s\n' "$@" > "{}"
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
            args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let exit_code_file = temp_dir.path().join("shim").join("exit_code");
    fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
    let mut daemon = Daemon::new(
        "test-container".to_string(),
        bundle_dir,
        runtime_path,
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: Some(exit_code_file),
            attach_socket_dir: None,
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: true,
            systemd_cgroup: false,
        },
    );

    daemon
        .io_manager
        .configure(IoConfig {
            stdout: Some(log_path),
            terminal: false,
            ..Default::default()
        })
        .unwrap();

    daemon.run_non_terminal_container().unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-new-keyring"));
}

#[test]
fn test_setup_io_places_reopen_socket_under_shim_work_dir() {
    let temp_dir = tempdir().unwrap();
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let log_path = temp_dir.path().join("logs").join("container.log");
    let internal_state = json!({
        "log_path": log_path.to_string_lossy(),
        "tty": false,
        "stdin": false,
        "stdin_once": false,
    });
    let config = json!({
        "process": {
            "terminal": false
        },
        "annotations": {
            INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
        }
    });
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();

    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
set -eu
exit 0
"#,
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let exit_code_file = temp_dir.path().join("exits").join("container-1");
    fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
    let attach_socket_dir = temp_dir.path().join("attach");
    let mut daemon = Daemon::new(
        "container-1".to_string(),
        bundle_dir,
        runtime_path,
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: Some(exit_code_file.clone()),
            attach_socket_dir: Some(attach_socket_dir.clone()),
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    daemon.setup_io().unwrap();

    let expected = temp_dir
        .path()
        .join("shim")
        .join("container-1")
        .join("reopen.sock");
    let unexpected = exit_code_file.parent().unwrap().join("reopen.sock");
    assert!(expected.exists());
    assert!(!unexpected.exists());
    assert!(!attach_socket_dir
        .join("container-1")
        .join("reopen.sock")
        .exists());

    daemon.io_manager.shutdown().unwrap();
    daemon.cleanup_attach_socket_directory();
}

#[test]
fn test_restore_task_sets_up_io_under_attach_socket_dir() {
    let temp_dir = tempdir().unwrap();
    let bundle_dir = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle_dir).unwrap();

    let log_path = temp_dir.path().join("logs").join("container.log");
    let internal_state = json!({
        "log_path": log_path.to_string_lossy(),
        "tty": false,
        "stdin": false,
        "stdin_once": false,
    });
    let config = json!({
        "process": {
            "terminal": false
        },
        "annotations": {
            INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
        }
    });
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();

    let args_path = temp_dir.path().join("restore.args");
    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    fs::write(
        &runtime_path,
        format!(
            r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
case "$cmd" in
  restore)
    printf '%s\n' "$@" > "{}"
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
            args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let attach_socket_dir = temp_dir.path().join("attach");
    let exit_code_file = temp_dir.path().join("exits").join("container-1");
    fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
    let daemon = Daemon::new(
        "container-1".to_string(),
        bundle_dir.clone(),
        runtime_path,
        DaemonOptions {
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
            work_dir: temp_dir.path().join("shim"),
            state_db_path: None,
            exit_code_file: Some(exit_code_file),
            attach_socket_dir: Some(attach_socket_dir.clone()),
            io_uid: 0,
            io_gid: 0,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            no_new_keyring: false,
            systemd_cgroup: false,
        },
    );

    daemon
        .restore_task_internal(&crate::shim_rpc::RestoreTaskRequest {
            container_id: "container-1".to_string(),
            image_path: temp_dir.path().join("checkpoint"),
            work_path: temp_dir.path().join("work"),
            bundle_path: bundle_dir,
            criu_path: PathBuf::new(),
            no_pivot: false,
        })
        .unwrap();

    assert_eq!(daemon.task_status().state, TaskState::Running);
    assert!(attach_socket_dir
        .join("container-1")
        .join("attach.sock")
        .exists());
    assert!(temp_dir
        .path()
        .join("shim")
        .join("container-1")
        .join("reopen.sock")
        .exists());
    assert!(fs::read_to_string(args_path)
        .unwrap()
        .contains("--image-path"));

    daemon.io_manager.shutdown().unwrap();
    daemon.cleanup_attach_socket_directory();
}
