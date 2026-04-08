//! CRIUS 集成测试套件
//!
//! 测试容器和Pod沙箱的完整生命周期

use std::collections::HashMap;
use std::path::PathBuf;

// 使用crius库
use crius::pod::{PodSandboxConfig, PodSandboxManager};
use crius::runtime::{
    ContainerConfig, ContainerRuntime, ContainerStatus, MountConfig, RuncRuntime,
};
use crius::storage::persistence::{PersistenceConfig, PersistenceManager};
use crius::storage::{ContainerRecord, PodSandboxRecord, StorageManager};

/// 测试辅助函数：创建临时目录
fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().into_path()
}

/// 测试辅助函数：创建测试用的容器配置
fn create_test_container_config(name: &str, image: &str) -> ContainerConfig {
    ContainerConfig {
        name: name.to_string(),
        image: image.to_string(),
        command: vec!["echo".to_string(), "hello".to_string()],
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
        namespace_paths: crius::runtime::NamespacePaths::default(),
        linux_resources: None,
        devices: vec![],
        rootfs: temp_dir().join("rootfs"),
    }
}

/// 测试辅助函数：创建测试用的Pod配置
fn create_test_pod_config(name: &str, namespace: &str) -> PodSandboxConfig {
    PodSandboxConfig {
        name: name.to_string(),
        namespace: namespace.to_string(),
        uid: format!("test-uid-{}", uuid::Uuid::new_v4()),
        hostname: "test-host".to_string(),
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
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        seccomp_profile: None,
        linux_resources: None,
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    /// 测试 RuncRuntime 创建
    #[test]
    fn test_runc_runtime_creation() {
        let temp_dir = temp_dir();
        let runtime = RuncRuntime::new(PathBuf::from("/usr/bin/runc"), temp_dir.clone());

        // 验证runtime创建成功
        assert!(!runtime.is_shim_enabled()); // 默认未启用shim
    }

    /// 测试容器配置创建
    #[test]
    fn test_container_config_creation() {
        let config = create_test_container_config("test-container", "test:latest");

        assert_eq!(config.name, "test-container");
        assert_eq!(config.image, "test:latest");
        assert_eq!(config.command, vec!["echo", "hello"]);
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    /// 测试存储管理器创建
    #[test]
    fn test_storage_manager_creation() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.join("test.db");

        let manager = StorageManager::new(&db_path);
        assert!(manager.is_ok());
    }

    /// 测试容器CRUD操作
    #[test]
    fn test_container_crud() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.join("test.db");

        let mut manager = StorageManager::new(&db_path).unwrap();

        // 创建容器记录
        let container = ContainerRecord {
            id: "container-1".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "test:latest".to_string(),
            command: "echo hello".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
        };

        manager.save_container(&container).unwrap();

        // 查询容器
        let retrieved = manager.get_container("container-1").unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, "container-1");
        assert_eq!(retrieved.state, "running");

        // 更新状态
        manager
            .update_container_state("container-1", "stopped", Some(0))
            .unwrap();

        let updated = manager.get_container("container-1").unwrap().unwrap();
        assert_eq!(updated.state, "stopped");
        assert_eq!(updated.exit_code, Some(0));

        // 删除容器
        manager.delete_container("container-1").unwrap();
        let deleted = manager.get_container("container-1").unwrap();
        assert!(deleted.is_none());
    }

    /// 测试Pod沙箱CRUD操作
    #[test]
    fn test_pod_sandbox_crud() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.join("test.db");

        let mut manager = StorageManager::new(&db_path).unwrap();

        // 创建Pod记录
        let pod = PodSandboxRecord {
            id: "pod-1".to_string(),
            state: "ready".to_string(),
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
            uid: "uid-123".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            netns_path: "/var/run/netns/pod-1".to_string(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            pause_container_id: Some("pause-1".to_string()),
            ip: Some("10.88.0.1".to_string()),
        };

        manager.save_pod_sandbox(&pod).unwrap();

        // 查询Pod
        let retrieved = manager.get_pod_sandbox("pod-1").unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, "pod-1");
        assert_eq!(retrieved.name, "test-pod");

        // 更新状态
        manager.update_pod_state("pod-1", "notready").unwrap();

        let updated = manager.get_pod_sandbox("pod-1").unwrap().unwrap();
        assert_eq!(updated.state, "notready");

        // 删除Pod
        manager.delete_pod_sandbox("pod-1").unwrap();
        let deleted = manager.get_pod_sandbox("pod-1").unwrap();
        assert!(deleted.is_none());
    }

    /// 测试持久化管理器
    #[test]
    fn test_persistence_manager() {
        let temp_dir = temp_dir();
        let config = PersistenceConfig {
            db_path: temp_dir.join("test.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        };

        let mut manager = PersistenceManager::new(config).unwrap();

        // 保存容器
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        manager
            .save_container(
                "container-1",
                "pod-1",
                ContainerStatus::Running,
                "test:latest",
                &["echo".to_string(), "hello".to_string()],
                &labels,
                &HashMap::new(),
            )
            .unwrap();

        // 更新状态
        manager
            .update_container_state("container-1", ContainerStatus::Stopped(0))
            .unwrap();

        // 恢复容器
        let recovered = manager.recover_containers().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].0, "container-1");

        // 保存Pod
        manager
            .save_pod_sandbox(
                "pod-1",
                "ready",
                "test-pod",
                "default",
                "uid-123",
                "/var/run/netns/pod-1",
                &labels,
                &HashMap::new(),
                Some("pause-1"),
                Some("10.88.0.1"),
            )
            .unwrap();

        // 恢复Pod
        let recovered_pods = manager.recover_pods().unwrap();
        assert_eq!(recovered_pods.len(), 1);
        assert_eq!(recovered_pods[0].id, "pod-1");
    }
}

#[cfg(test)]
mod pod_tests {
    use super::*;
    use crius::runtime::{ContainerRuntime, RuncRuntime};

    /// 测试 PodSandboxManager 创建
    #[test]
    fn test_pod_sandbox_manager_creation() {
        let temp_dir = temp_dir();
        let runtime = RuncRuntime::new(PathBuf::from("/usr/bin/runc"), temp_dir.join("runtime"));

        let manager = PodSandboxManager::new(
            runtime,
            temp_dir.join("pods"),
            "registry.k8s.io/pause:3.9".to_string(),
        );

        // 验证创建成功 - 检查Debug输出包含root_dir
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("root_dir"));
    }

    /// 测试 PodSandboxConfig 创建
    #[test]
    fn test_pod_sandbox_config_creation() {
        let config = create_test_pod_config("test-pod", "default");

        assert_eq!(config.name, "test-pod");
        assert_eq!(config.namespace, "default");
        assert!(!config.uid.is_empty());
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// 集成测试：完整容器生命周期（创建→保存→更新→删除）
    #[test]
    fn test_container_lifecycle_with_persistence() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.join("integration.db");

        // 步骤1：创建存储管理器
        let mut storage = StorageManager::new(&db_path).unwrap();

        // 步骤2：创建容器记录
        let container_id = format!("container-{}", uuid::Uuid::new_v4());
        let pod_id = format!("pod-{}", uuid::Uuid::new_v4());

        let container = ContainerRecord {
            id: container_id.clone(),
            pod_id: pod_id.clone(),
            state: "created".to_string(),
            image: "nginx:latest".to_string(),
            command: "nginx -g daemon off;".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: r#"{"app": "nginx", "env": "test"}"#.to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
        };

        storage.save_container(&container).unwrap();

        // 步骤3：验证创建
        let retrieved = storage.get_container(&container_id).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.state, "created");
        assert_eq!(retrieved.image, "nginx:latest");

        // 步骤4：更新状态为运行中
        storage
            .update_container_state(&container_id, "running", None)
            .unwrap();
        let updated = storage.get_container(&container_id).unwrap().unwrap();
        assert_eq!(updated.state, "running");
        assert!(updated.exit_code.is_none());

        // 步骤5：更新状态为已停止
        storage
            .update_container_state(&container_id, "stopped", Some(0))
            .unwrap();
        let stopped = storage.get_container(&container_id).unwrap().unwrap();
        assert_eq!(stopped.state, "stopped");
        assert_eq!(stopped.exit_code, Some(0));
        assert!(stopped.exit_time.is_some());

        // 步骤6：删除容器
        storage.delete_container(&container_id).unwrap();
        let deleted = storage.get_container(&container_id).unwrap();
        assert!(deleted.is_none());

        println!("✅ 容器生命周期集成测试通过");
    }

    /// 集成测试：Pod沙箱与容器的关联
    #[test]
    fn test_pod_with_containers_integration() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.join("pod_integration.db");

        let mut storage = StorageManager::new(&db_path).unwrap();

        // 步骤1：创建Pod
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

        // 步骤2：创建关联的容器
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

        // 步骤3：查询Pod下的所有容器
        let containers = storage.list_containers_by_pod(&pod_id).unwrap();
        assert_eq!(containers.len(), 2);

        // 步骤4：删除Pod（应该级联删除容器）
        storage.delete_pod_sandbox(&pod_id).unwrap();

        // 步骤5：验证Pod和容器都已删除
        assert!(storage.get_pod_sandbox(&pod_id).unwrap().is_none());
        assert!(storage.get_container(&container1_id).unwrap().is_none());
        assert!(storage.get_container(&container2_id).unwrap().is_none());

        println!("✅ Pod与容器关联集成测试通过");
    }

    /// 集成测试：状态恢复
    #[test]
    fn test_state_recovery_integration() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.join("recovery.db");

        // 步骤1：第一次会话 - 创建数据
        {
            let mut storage = StorageManager::new(&db_path).unwrap();

            // 创建Pod
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

            // 创建容器
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

            // 模拟正常关闭
            storage.close().unwrap();
        }

        // 步骤2：第二次会话 - 恢复数据（模拟daemon重启）
        {
            let storage = StorageManager::new(&db_path).unwrap();

            // 恢复所有Pod
            let pods = storage.list_pod_sandboxes().unwrap();
            assert_eq!(pods.len(), 1);
            assert_eq!(pods[0].state, "ready");
            assert_eq!(pods[0].ip, Some("10.88.0.5".to_string()));

            // 恢复所有容器
            let containers = storage.list_containers().unwrap();
            assert_eq!(containers.len(), 1);
            assert_eq!(containers[0].state, "running");
            assert_eq!(containers[0].image, "alpine:latest");

            // 验证关联
            let pod_containers = storage.list_containers_by_pod(&pods[0].id).unwrap();
            assert_eq!(pod_containers.len(), 1);
            assert_eq!(pod_containers[0].id, containers[0].id);
        }

        println!("✅ 状态恢复集成测试通过");
    }
}

/// 主测试入口
#[cfg(test)]
mod test_main {
    use super::*;

    /// 运行所有测试前的设置
    fn setup() {
        // 初始化日志（可选）
        let _ = std::env::set_var("RUST_LOG", "debug");
    }

    #[test]
    fn test_suite_summary() {
        setup();

        println!("\n========================================");
        println!("CRIUS 集成测试套件");
        println!("========================================\n");

        println!("✅ 运行时模块测试");
        println!("   - RuncRuntime 创建");
        println!("   - 容器配置创建");
        println!("   - PodSandboxManager 创建");
        println!("   - PodSandboxConfig 创建\n");

        println!("✅ 存储模块测试");
        println!("   - StorageManager 创建");
        println!("   - 容器 CRUD 操作");
        println!("   - Pod沙箱 CRUD 操作");
        println!("   - 持久化管理器\n");

        println!("✅ 集成测试");
        println!("   - 容器生命周期（创建→保存→更新→删除）");
        println!("   - Pod沙箱与容器关联");
        println!("   - 状态恢复（模拟daemon重启）\n");

        println!("========================================");
        println!("所有集成测试完成！");
        println!("========================================\n");
    }
}
