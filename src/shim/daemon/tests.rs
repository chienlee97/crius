use super::*;
use crate::services::{EventService, InternalEventSeverity};
use crate::storage::persistence::{PersistenceConfig, PersistenceManager};
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
