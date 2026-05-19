use super::*;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

fn write_ping_shim(dir: &Path, args_path: Option<&Path>) -> PathBuf {
    let shim_path = dir.join("fake-shim.py");
    let args_path = args_path
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    fs::write(
        &shim_path,
        format!(
            r#"#!/usr/bin/env python3
import os
import signal
import socket
import sys
import time

args = sys.argv[1:]
args_path = {args_path:?}
if args_path:
    os.makedirs(os.path.dirname(args_path), exist_ok=True)
    with open(args_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(args) + "\n")

env_path = os.environ.get("TEST_MONITOR_ENV_PATH")
if env_path:
    os.makedirs(os.path.dirname(env_path), exist_ok=True)
    with open(env_path, "w", encoding="utf-8") as handle:
        handle.write(os.environ.get("TEST_MONITOR_VALUE", "") + "\n")

config = {{}}
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
socket_path = os.path.join(work_dir, container_id, "task.sock")
os.makedirs(os.path.dirname(socket_path), exist_ok=True)
try:
    os.unlink(socket_path)
except FileNotFoundError:
    pass

running = True
def stop(_signum, _frame):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(socket_path)
server.listen(8)
server.settimeout(0.1)
try:
    while running:
        try:
            conn, _ = server.accept()
        except socket.timeout:
            continue
        with conn:
            while conn.recv(65536):
                pass
            conn.sendall(b'{{"ok":true,"payload":{{"kind":"empty"}},"error":null}}')
finally:
    server.close()
    try:
        os.unlink(socket_path)
    except FileNotFoundError:
        pass
"#
        ),
    )
    .unwrap();
    let mut perms = fs::metadata(&shim_path).unwrap().permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
    }
    fs::set_permissions(&shim_path, perms).unwrap();
    shim_path
}

#[test]
fn test_shim_config_default() {
    let config = ShimConfig::default();
    assert_eq!(config.shim_path, PathBuf::from("crius-shim"));
    assert_eq!(config.work_dir, default_shim_work_dir());
    assert_eq!(config.attach_socket_dir, default_shim_work_dir());
    assert_eq!(
        config.container_exits_dir,
        PathBuf::from("/var/run/crius/exits")
    );
    assert!(config.monitor_env.is_empty());
    assert!(!config.debug);
    assert!(!config.no_sync_log);
    assert!(!config.no_pivot);
}

#[test]
fn test_shim_manager_creation() {
    let temp_dir = tempdir().unwrap();
    let config = ShimConfig {
        work_dir: temp_dir.path().to_path_buf(),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        ..Default::default()
    };

    let manager = ShimManager::new(config);
    let shims = manager.list_shims();
    assert!(shims.is_empty());
}

#[test]
fn test_shim_manager_restores_live_metadata_from_disk() {
    let temp_dir = tempdir().unwrap();
    let work_dir = temp_dir.path().join("shims");
    let container_dir = work_dir.join("container-1");
    let exits_dir = temp_dir.path().join("exits");
    fs::create_dir_all(&container_dir).unwrap();
    fs::create_dir_all(&exits_dir).unwrap();

    let process = ShimProcess {
        container_id: "container-1".to_string(),
        shim_pid: std::process::id(),
        exit_code_file: exits_dir.join("container-1"),
        log_file: container_dir.join("shim.log"),
        socket_path: container_dir.join("attach.sock"),
        bundle_path: temp_dir.path().join("bundle"),
    };
    fs::write(
        container_dir.join(SHIM_METADATA_FILE),
        serde_json::to_vec_pretty(&process).unwrap(),
    )
    .unwrap();

    let manager = ShimManager::new(ShimConfig {
        work_dir,
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: exits_dir,
        ..Default::default()
    });
    let shims = manager.list_shims();
    assert_eq!(shims.len(), 1);
    assert_eq!(shims[0].container_id, "container-1");
    assert_eq!(
        manager.read_shim_pidfile("container-1").unwrap(),
        Some(std::process::id())
    );
}

#[test]
fn test_shim_manager_restores_live_metadata_from_ledger() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("crius.db");
    let mut storage = StorageManager::new(&db_path).unwrap();
    storage
        .save_shim_process(&ShimProcessRecord {
            container_id: "container-ledger".to_string(),
            shim_pid: std::process::id(),
            work_dir: temp_dir.path().join("shims").display().to_string(),
            socket_path: temp_dir
                .path()
                .join("attach")
                .join("container-ledger")
                .join("attach.sock")
                .display()
                .to_string(),
            exit_code_file: temp_dir
                .path()
                .join("exits")
                .join("container-ledger")
                .display()
                .to_string(),
            log_file: temp_dir
                .path()
                .join("shims")
                .join("container-ledger")
                .join("shim.log")
                .display()
                .to_string(),
            bundle_path: temp_dir.path().join("bundle").display().to_string(),
            state: "running".to_string(),
            last_seen_at: 1,
        })
        .unwrap();

    let manager = ShimManager::new(ShimConfig {
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        state_db_path: db_path,
        ..Default::default()
    });
    let shims = manager.list_shims();
    assert_eq!(shims.len(), 1);
    assert_eq!(shims[0].container_id, "container-ledger");
}

#[test]
fn test_shim_manager_ignores_stale_metadata_from_disk() {
    let temp_dir = tempdir().unwrap();
    let work_dir = temp_dir.path().join("shims");
    let container_dir = work_dir.join("stale-container");
    let exits_dir = temp_dir.path().join("exits");
    fs::create_dir_all(&container_dir).unwrap();
    fs::create_dir_all(&exits_dir).unwrap();

    let process = ShimProcess {
        container_id: "stale-container".to_string(),
        shim_pid: 999_999,
        exit_code_file: exits_dir.join("stale-container"),
        log_file: container_dir.join("shim.log"),
        socket_path: container_dir.join("attach.sock"),
        bundle_path: temp_dir.path().join("bundle"),
    };
    fs::write(
        container_dir.join(SHIM_METADATA_FILE),
        serde_json::to_vec_pretty(&process).unwrap(),
    )
    .unwrap();

    let manager = ShimManager::new(ShimConfig {
        work_dir,
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: exits_dir,
        ..Default::default()
    });
    assert!(manager.list_shims().is_empty());
}

#[test]
fn test_start_shim_uses_configured_container_exits_dir() {
    let temp_dir = tempdir().unwrap();
    let shim_path = write_ping_shim(temp_dir.path(), None);

    let config = ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    };
    let manager = ShimManager::new(config.clone());
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    let process = manager.start_shim("container-1", &bundle).unwrap();
    assert_eq!(
        process.exit_code_file,
        config.container_exits_dir.join("container-1")
    );
    assert_eq!(
        manager.read_shim_pidfile("container-1").unwrap(),
        Some(process.shim_pid)
    );
    assert!(manager.pidfile_path("container-1").exists());

    manager.stop_shim("container-1").unwrap();
    assert!(manager.pidfile_path("container-1").exists());
}

#[test]
fn test_get_exit_code_falls_back_to_global_exit_file_without_metadata() {
    let temp_dir = tempdir().unwrap();
    let exits_dir = temp_dir.path().join("exits");
    fs::create_dir_all(&exits_dir).unwrap();
    fs::write(exits_dir.join("container-1"), "17\n").unwrap();

    let manager = ShimManager::new(ShimConfig {
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: exits_dir,
        ..Default::default()
    });

    assert_eq!(manager.get_exit_code("container-1").unwrap(), Some(17));
}

#[test]
fn test_cleanup_exited_shims_keeps_metadata_while_exit_file_exists() {
    let temp_dir = tempdir().unwrap();
    let work_dir = temp_dir.path().join("shims");
    let container_dir = work_dir.join("container-1");
    let exits_dir = temp_dir.path().join("exits");
    fs::create_dir_all(&container_dir).unwrap();
    fs::create_dir_all(&exits_dir).unwrap();

    let process = ShimProcess {
        container_id: "container-1".to_string(),
        shim_pid: 999_999,
        exit_code_file: exits_dir.join("container-1"),
        log_file: container_dir.join("shim.log"),
        socket_path: container_dir.join("attach.sock"),
        bundle_path: temp_dir.path().join("bundle"),
    };
    fs::write(
        container_dir.join(SHIM_METADATA_FILE),
        serde_json::to_vec_pretty(&process).unwrap(),
    )
    .unwrap();
    fs::write(&process.exit_code_file, "3\n").unwrap();

    let manager = ShimManager::new(ShimConfig {
        work_dir: work_dir.clone(),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: exits_dir,
        ..Default::default()
    });

    assert_eq!(manager.cleanup_exited_shims().unwrap(), 0);
    assert!(!manager.list_shims().is_empty());
    assert!(container_dir.join(SHIM_METADATA_FILE).exists());
}

#[test]
fn test_start_shim_passes_no_pivot_when_enabled() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("shim.args");
    let shim_path = write_ping_shim(temp_dir.path(), Some(&args_path));

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        no_pivot: true,
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if args_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-pivot"));

    manager.stop_shim("container-1").unwrap();
}

#[test]
fn test_start_shim_passes_no_new_keyring_when_enabled() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("shim.args");
    let shim_path = write_ping_shim(temp_dir.path(), Some(&args_path));

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        no_new_keyring: true,
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if args_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-new-keyring"));

    manager.stop_shim("container-1").unwrap();
}

#[test]
fn test_start_shim_passes_configured_io_owner_flags() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("shim.args");
    let shim_path = write_ping_shim(temp_dir.path(), Some(&args_path));

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        io_uid: 1234,
        io_gid: 2345,
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if args_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--io-uid"));
    assert!(args.lines().any(|line| line == "1234"));
    assert!(args.lines().any(|line| line == "--io-gid"));
    assert!(args.lines().any(|line| line == "2345"));

    manager.stop_shim("container-1").unwrap();
}

#[test]
fn test_start_shim_passes_no_sync_log_flag() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("shim.args");
    let shim_path = write_ping_shim(temp_dir.path(), Some(&args_path));

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        no_sync_log: true,
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if args_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-sync-log"));

    manager.stop_shim("container-1").unwrap();
}

#[test]
fn test_start_shim_passes_runtime_config_path_and_monitor_cgroup() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("shim.args");
    let shim_path = write_ping_shim(temp_dir.path(), Some(&args_path));

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        runtime_config_path: PathBuf::from("/etc/kata/config.toml"),
        monitor_cgroup: "system.slice".to_string(),
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if args_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--runtime-config-path"));
    assert!(args.lines().any(|line| line == "/etc/kata/config.toml"));
    assert!(args.lines().any(|line| line == "--monitor-cgroup"));
    assert!(args.lines().any(|line| line == "system.slice"));

    manager.stop_shim("container-1").unwrap();
}

#[test]
fn test_start_shim_omits_empty_runtime_config_path_and_monitor_cgroup() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("shim.args");
    let shim_path = write_ping_shim(temp_dir.path(), Some(&args_path));

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if args_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let args = fs::read_to_string(&args_path).unwrap();
    assert!(!args.lines().any(|line| line == "--runtime-config-path"));
    assert!(!args.lines().any(|line| line == "--monitor-cgroup"));

    manager.stop_shim("container-1").unwrap();
}

#[test]
fn test_start_shim_injects_configured_monitor_env() {
    let temp_dir = tempdir().unwrap();
    let shim_path = write_ping_shim(temp_dir.path(), None);
    let env_out = temp_dir.path().join("shim.env");

    let manager = ShimManager::new(ShimConfig {
        shim_path,
        work_dir: temp_dir.path().join("shims"),
        attach_socket_dir: temp_dir.path().join("attach"),
        container_exits_dir: temp_dir.path().join("exits"),
        monitor_env: vec![
            format!("TEST_MONITOR_ENV_PATH={}", env_out.display()),
            "TEST_MONITOR_VALUE=hello-from-monitor-env".to_string(),
        ],
        runtime_path: PathBuf::from("/bin/false"),
        ..Default::default()
    });
    let bundle = temp_dir.path().join("bundle");
    fs::create_dir_all(&bundle).unwrap();

    manager.start_shim("container-1", &bundle).unwrap();
    for _ in 0..50 {
        if env_out.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(
        fs::read_to_string(&env_out).unwrap().trim(),
        "hello-from-monitor-env"
    );

    manager.stop_shim("container-1").unwrap();
}
