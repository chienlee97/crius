//! 集成测试
//!
//! 本文件统一承载两类测试：
//! 1. 系统级集成测试：直接依赖 `runc`、rootfs、root 权限。
//! 2. 跨模块集成测试：验证 `storage` / 持久化恢复 / pod-container 关联流程。

use crius::storage::{ContainerRecord, PodSandboxRecord, StorageManager};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn check_runc_installed() -> bool {
    std::process::Command::new("which")
        .arg("runc")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn check_root() -> bool {
    nix::unistd::getuid().is_root()
}

fn setup_minimal_rootfs(rootfs: &std::path::Path) -> anyhow::Result<()> {
    use std::fs;
    use std::os::unix::fs::symlink;

    for dir in &["bin", "lib", "lib64", "proc", "sys", "dev", "tmp"] {
        fs::create_dir_all(rootfs.join(dir))?;
    }

    let busybox_paths = ["/bin/busybox", "/usr/bin/busybox"];
    let mut busybox_found = false;

    for path in &busybox_paths {
        if std::path::Path::new(path).exists() {
            let dest = rootfs.join("bin/busybox");
            fs::copy(path, &dest)?;

            for cmd in &["sh", "echo", "sleep", "ls", "cat"] {
                let _ = symlink("busybox", rootfs.join("bin").join(cmd));
            }
            busybox_found = true;
            break;
        }
    }

    if !busybox_found {
        let shells = ["/bin/sh", "/bin/bash", "/usr/bin/sh"];
        for shell in &shells {
            if std::path::Path::new(shell).exists() {
                let dest = rootfs.join("bin/sh");
                fs::copy(shell, &dest)?;
                break;
            }
        }
    }

    let dev_dir = rootfs.join("dev");

    let null_path = dev_dir.join("null");
    if !null_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", null_path.to_str().unwrap(), "c", "1", "3"])
            .output()?;
    }

    let zero_path = dev_dir.join("zero");
    if !zero_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", zero_path.to_str().unwrap(), "c", "1", "5"])
            .output()?;
    }

    let random_path = dev_dir.join("random");
    if !random_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", random_path.to_str().unwrap(), "c", "1", "8"])
            .output()?;
    }

    let urandom_path = dev_dir.join("urandom");
    if !urandom_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", urandom_path.to_str().unwrap(), "c", "1", "9"])
            .output()?;
    }

    Ok(())
}

mod system_runtime {
    use super::{check_root, check_runc_installed, setup_minimal_rootfs};
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crius::runtime::{
        ContainerConfig, ContainerRuntime, ContainerStatus, NamespacePaths, RuncRuntime,
    };

    #[tokio::test]
    #[ignore = "requires runc and root privileges"]
    async fn test_container_lifecycle() {
        if !check_runc_installed() {
            eprintln!("Skipping test: runc not installed");
            return;
        }
        if !check_root() {
            eprintln!("Skipping test: requires root privileges");
            return;
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_root = temp_dir.path().join("runc");
        std::fs::create_dir_all(&runtime_root).unwrap();

        let rootfs = temp_dir.path().join("rootfs");
        std::fs::create_dir_all(&rootfs).unwrap();
        setup_minimal_rootfs(&rootfs).expect("Failed to setup rootfs");

        let runtime = RuncRuntime::new(PathBuf::from("runc"), runtime_root.clone());

        let config = ContainerConfig {
            name: "test-container".to_string(),
            image: "test:latest".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                "while :; do :; done".to_string(),
            ],
            args: vec![],
            env: vec![],
            working_dir: None,
            mounts: vec![],
            labels: vec![],
            annotations: vec![],
            privileged: false,
            user: None,
            run_as_group: None,
            supplemental_groups: vec![],
            hostname: None,
            tty: false,
            stdin: false,
            stdin_once: false,
            log_path: None,
            readonly_rootfs: false,
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
            devices: vec![],
            rootfs: rootfs.clone(),
        };

        let container_id = runtime
            .create_container(&config.name, &config)
            .expect("Failed to create container");

        let status = runtime
            .container_status(&container_id)
            .expect("Failed to get status");
        assert_eq!(status, ContainerStatus::Created);

        runtime
            .start_container(&container_id)
            .expect("Failed to start container");

        std::thread::sleep(std::time::Duration::from_secs(1));

        let status = runtime
            .container_status(&container_id)
            .expect("Failed to get running status");
        assert_eq!(status, ContainerStatus::Running);

        runtime
            .stop_container(&container_id, Some(5))
            .expect("Failed to stop container");
        runtime
            .remove_container(&container_id)
            .expect("Failed to remove container");

        let status = runtime
            .container_status(&container_id)
            .expect("Failed to get status");
        assert_eq!(status, ContainerStatus::Unknown);
    }

    #[tokio::test]
    #[ignore = "requires runc and root privileges"]
    async fn test_stop_graceful_timeout() {
        if !check_runc_installed() {
            eprintln!("Skipping test: runc not installed");
            return;
        }
        if !check_root() {
            eprintln!("Skipping test: requires root privileges");
            return;
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_root = temp_dir.path().join("runc");
        std::fs::create_dir_all(&runtime_root).unwrap();

        let rootfs = temp_dir.path().join("rootfs");
        setup_minimal_rootfs(&rootfs).expect("Failed to setup rootfs");

        let runtime = RuncRuntime::new(PathBuf::from("runc"), runtime_root.clone());

        let config = ContainerConfig {
            name: "sleep-container".to_string(),
            image: "test:latest".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                "trap '' TERM; while :; do :; done".to_string(),
            ],
            args: vec![],
            env: vec![],
            working_dir: None,
            mounts: vec![],
            labels: vec![],
            annotations: vec![],
            privileged: false,
            user: None,
            run_as_group: None,
            supplemental_groups: vec![],
            hostname: None,
            tty: false,
            stdin: false,
            stdin_once: false,
            log_path: None,
            readonly_rootfs: false,
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
            devices: vec![],
            rootfs: rootfs.clone(),
        };

        let container_id = runtime
            .create_container(&config.name, &config)
            .expect("Failed to create container");
        runtime
            .start_container(&container_id)
            .expect("Failed to start container");

        std::thread::sleep(std::time::Duration::from_secs(1));

        let status = runtime
            .container_status(&container_id)
            .expect("Failed to get status");
        assert_eq!(status, ContainerStatus::Running);

        let start = std::time::Instant::now();
        runtime
            .stop_container(&container_id, Some(2))
            .expect("Failed to stop container");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_secs(2),
            "stop timeout should wait before force-killing, elapsed: {:?}",
            elapsed
        );

        let status = runtime
            .container_status(&container_id)
            .expect("Failed to get status");
        match status {
            ContainerStatus::Stopped(_) | ContainerStatus::Unknown => {}
            _ => panic!("Unexpected status after stop: {:?}", status),
        }

        let _ = runtime.remove_container(&container_id);
    }
}

mod cross_module {
    use std::collections::HashMap;

    use super::{temp_dir, ContainerRecord, PodSandboxRecord, StorageManager};
    use crius::runtime::ContainerStatus;
    use crius::storage::persistence::{PersistenceConfig, PersistenceManager};

    #[test]
    fn test_pod_with_containers_integration() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.path().join("pod_integration.db");

        let mut storage = StorageManager::new(&db_path).unwrap();

        let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
        let pod = PodSandboxRecord {
            id: pod_id.clone(),
            state: "ready".to_string(),
            name: "test-app".to_string(),
            namespace: "production".to_string(),
            uid: format!("uid-{}", uuid::Uuid::new_v4()),
            created_at: chrono::Utc::now().timestamp(),
            netns_path: format!("/var/run/netns/{}", pod_id),
            labels: r#"{"app": "test-app", "tier": "frontend"}"#.to_string(),
            annotations: "{}".to_string(),
            pause_container_id: Some(format!("pause-{}", uuid::Uuid::new_v4())),
            ip: Some("10.88.0.10".to_string()),
        };

        storage.save_pod_sandbox(&pod).unwrap();

        let container1_id = format!("container-{}", uuid::Uuid::new_v4());
        let container2_id = format!("container-{}", uuid::Uuid::new_v4());

        let container1 = ContainerRecord {
            id: container1_id.clone(),
            pod_id: pod_id.clone(),
            state: "running".to_string(),
            image: "nginx:latest".to_string(),
            command: "nginx".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
        };

        let container2 = ContainerRecord {
            id: container2_id.clone(),
            pod_id: pod_id.clone(),
            state: "running".to_string(),
            image: "redis:latest".to_string(),
            command: "redis-server".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
        };

        storage.save_container(&container1).unwrap();
        storage.save_container(&container2).unwrap();

        let containers = storage.list_containers_by_pod(&pod_id).unwrap();
        assert_eq!(containers.len(), 2);

        storage.delete_pod_sandbox(&pod_id).unwrap();

        assert!(storage.get_pod_sandbox(&pod_id).unwrap().is_none());
        assert!(storage.get_container(&container1_id).unwrap().is_none());
        assert!(storage.get_container(&container2_id).unwrap().is_none());
    }

    #[test]
    fn test_storage_reopen_preserves_pod_container_relationship() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.path().join("recovery.db");

        {
            let mut storage = StorageManager::new(&db_path).unwrap();

            let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
            let pod = PodSandboxRecord {
                id: pod_id.clone(),
                state: "ready".to_string(),
                name: "recover-test".to_string(),
                namespace: "default".to_string(),
                uid: format!("uid-{}", uuid::Uuid::new_v4()),
                created_at: chrono::Utc::now().timestamp(),
                netns_path: "/var/run/netns/test".to_string(),
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                pause_container_id: None,
                ip: Some("10.88.0.5".to_string()),
            };
            storage.save_pod_sandbox(&pod).unwrap();

            let container_id = format!("container-{}", uuid::Uuid::new_v4());
            let container = ContainerRecord {
                id: container_id.clone(),
                pod_id: pod_id.clone(),
                state: "running".to_string(),
                image: "alpine:latest".to_string(),
                command: "sh".to_string(),
                created_at: chrono::Utc::now().timestamp(),
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                exit_code: None,
                exit_time: None,
            };
            storage.save_container(&container).unwrap();

            storage.close().unwrap();
        }

        {
            let storage = StorageManager::new(&db_path).unwrap();

            let pods = storage.list_pod_sandboxes().unwrap();
            assert_eq!(pods.len(), 1);
            assert_eq!(pods[0].state, "ready");
            assert_eq!(pods[0].ip, Some("10.88.0.5".to_string()));

            let containers = storage.list_containers().unwrap();
            assert_eq!(containers.len(), 1);
            assert_eq!(containers[0].state, "running");
            assert_eq!(containers[0].image, "alpine:latest");

            let pod_containers = storage.list_containers_by_pod(&pods[0].id).unwrap();
            assert_eq!(pod_containers.len(), 1);
            assert_eq!(pod_containers[0].id, containers[0].id);
        }
    }

    #[test]
    fn test_persistence_manager_roundtrip_integration() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.path().join("persistence-roundtrip.db");
        let config = PersistenceConfig {
            db_path: db_path.clone(),
            enable_recovery: true,
            auto_save_interval: 30,
        };

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "demo".to_string());
        let mut annotations = HashMap::new();
        annotations.insert("owner".to_string(), "integration".to_string());

        let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
        let container_id = format!("container-{}", uuid::Uuid::new_v4());

        {
            let mut manager = PersistenceManager::new(config.clone()).unwrap();
            manager
                .save_pod_sandbox(
                    &pod_id,
                    "ready",
                    "demo-pod",
                    "default",
                    "uid-123",
                    "/var/run/netns/demo",
                    &labels,
                    &annotations,
                    Some("pause-1"),
                    Some("10.88.0.20"),
                )
                .unwrap();
            manager
                .save_container(
                    &container_id,
                    &pod_id,
                    ContainerStatus::Running,
                    "demo:latest",
                    &["sh".to_string(), "-c".to_string(), "sleep 1".to_string()],
                    &labels,
                    &annotations,
                )
                .unwrap();
            manager
                .update_container_state(&container_id, ContainerStatus::Stopped(17))
                .unwrap();
            manager.close().unwrap();
        }

        let manager = PersistenceManager::new(config).unwrap();
        let recovered_containers = manager.recover_containers().unwrap();
        assert_eq!(recovered_containers.len(), 1);
        assert_eq!(recovered_containers[0].0, container_id);
        assert_eq!(recovered_containers[0].1, ContainerStatus::Stopped(17));

        let recovered_container_labels: HashMap<String, String> =
            serde_json::from_str(&recovered_containers[0].2.labels).unwrap();
        let recovered_container_annotations: HashMap<String, String> =
            serde_json::from_str(&recovered_containers[0].2.annotations).unwrap();
        assert_eq!(
            recovered_container_labels.get("app"),
            Some(&"demo".to_string())
        );
        assert_eq!(
            recovered_container_annotations.get("owner"),
            Some(&"integration".to_string())
        );

        let recovered_pods = manager.recover_pods().unwrap();
        assert_eq!(recovered_pods.len(), 1);
        assert_eq!(recovered_pods[0].id, pod_id);
        assert_eq!(
            recovered_pods[0].pause_container_id.as_deref(),
            Some("pause-1")
        );
        assert_eq!(recovered_pods[0].ip.as_deref(), Some("10.88.0.20"));
    }
}
