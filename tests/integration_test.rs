// 集成测试：验证容器生命周期管理
//
// 运行：cargo test integration_test -- --ignored
// 注意：此测试需要安装 runc

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

// 检查runc是否安装
fn check_runc_installed() -> bool {
    Command::new("which")
        .arg("runc")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// 检查是否以root运行
fn check_root() -> bool {
    nix::unistd::getuid().is_root()
}

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

    use crius::runtime::{ContainerConfig, ContainerRuntime, ContainerStatus, RuncRuntime};
    use tempfile::tempdir;

    // 创建临时目录
    let temp_dir = tempdir().unwrap();
    let runtime_root = temp_dir.path().join("runc");
    std::fs::create_dir_all(&runtime_root).unwrap();

    // 创建简单的rootfs（使用busybox镜像）
    let rootfs = temp_dir.path().join("rootfs");
    std::fs::create_dir_all(&rootfs).unwrap();

    // 尝试从系统复制busybox或创建最小rootfs
    setup_minimal_rootfs(&rootfs).expect("Failed to setup rootfs");

    // 创建runtime实例
    let runtime = RuncRuntime::new(PathBuf::from("runc"), runtime_root.clone());

    // 创建容器配置
    let config = ContainerConfig {
        name: "test-container".to_string(),
        image: "test:latest".to_string(),
        command: vec!["echo".to_string(), "hello".to_string()],
        args: vec![],
        env: vec![("TEST_VAR".to_string(), "test_value".to_string())],
        working_dir: None,
        mounts: vec![],
        labels: vec![],
        annotations: vec![],
        privileged: false,
        user: None,
        run_as_group: None,
        supplemental_groups: vec![],
        hostname: Some("test-host".to_string()),
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
        rootfs: rootfs.clone(),
    };

    // 1. 创建容器
    let container_id = runtime
        .create_container(&config)
        .expect("Failed to create container");
    println!("Container created: {}", container_id);

    // 验证状态为Created
    let status = runtime
        .container_status(&container_id)
        .expect("Failed to get status");
    assert_eq!(
        status,
        ContainerStatus::Created,
        "Container should be in Created state"
    );

    // 2. 启动容器
    runtime
        .start_container(&container_id)
        .expect("Failed to start container");
    println!("Container started");

    // 等待容器运行完成
    std::thread::sleep(std::time::Duration::from_secs(2));

    // 3. 获取最终状态（可能是stopped因为echo命令很快结束）
    let status = runtime
        .container_status(&container_id)
        .expect("Failed to get status");
    println!("Container status after run: {:?}", status);

    // 4. 停止容器（即使已经停止也不会出错）
    runtime
        .stop_container(&container_id, Some(5))
        .expect("Failed to stop container");
    println!("Container stopped");

    // 5. 删除容器
    runtime
        .remove_container(&container_id)
        .expect("Failed to remove container");
    println!("Container removed");

    // 验证容器不存在
    let status = runtime
        .container_status(&container_id)
        .expect("Failed to get status");
    assert_eq!(
        status,
        ContainerStatus::Unknown,
        "Container should be unknown after removal"
    );
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

    use crius::runtime::{ContainerConfig, ContainerRuntime, ContainerStatus, RuncRuntime};
    use tempfile::tempdir;

    let temp_dir = tempdir().unwrap();
    let runtime_root = temp_dir.path().join("runc");
    std::fs::create_dir_all(&runtime_root).unwrap();

    let rootfs = temp_dir.path().join("rootfs");
    setup_minimal_rootfs(&rootfs).expect("Failed to setup rootfs");

    let runtime = RuncRuntime::new(PathBuf::from("runc"), runtime_root.clone());

    // 创建一个会长时间运行的容器（sleep 60）
    let config = ContainerConfig {
        name: "sleep-container".to_string(),
        image: "test:latest".to_string(),
        command: vec!["sleep".to_string(), "60".to_string()],
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
        rootfs: rootfs.clone(),
    };

    let container_id = runtime
        .create_container(&config)
        .expect("Failed to create container");
    runtime
        .start_container(&container_id)
        .expect("Failed to start container");

    // 等待容器进入运行状态
    std::thread::sleep(std::time::Duration::from_secs(1));

    let status = runtime
        .container_status(&container_id)
        .expect("Failed to get status");
    assert_eq!(
        status,
        ContainerStatus::Running,
        "Container should be running"
    );

    // 停止容器（使用3秒超时）
    let start = std::time::Instant::now();
    runtime
        .stop_container(&container_id, Some(3))
        .expect("Failed to stop container");
    let elapsed = start.elapsed();

    println!("Container stopped in {:?}", elapsed);

    // 验证容器已停止
    let status = runtime
        .container_status(&container_id)
        .expect("Failed to get status");
    match status {
        ContainerStatus::Stopped(_) => println!("Container stopped successfully"),
        ContainerStatus::Unknown => println!("Container stopped and cleaned up"),
        _ => panic!("Unexpected status after stop: {:?}", status),
    }

    // 清理
    let _ = runtime.remove_container(&container_id);
}

// 设置最小rootfs
fn setup_minimal_rootfs(rootfs: &std::path::Path) -> anyhow::Result<()> {
    use std::fs;
    use std::os::unix::fs::symlink;

    // 创建基本目录结构
    for dir in &["bin", "lib", "lib64", "proc", "sys", "dev", "tmp"] {
        fs::create_dir_all(rootfs.join(dir))?;
    }

    // 尝试从系统复制busybox
    let busybox_paths = ["/bin/busybox", "/usr/bin/busybox"];
    let mut busybox_found = false;

    for path in &busybox_paths {
        if std::path::Path::new(path).exists() {
            let dest = rootfs.join("bin/busybox");
            fs::copy(path, &dest)?;

            // 创建基本命令的链接
            for cmd in &["sh", "echo", "sleep", "ls", "cat"] {
                let _ = symlink("busybox", rootfs.join("bin").join(cmd));
            }
            busybox_found = true;
            break;
        }
    }

    if !busybox_found {
        // 如果没有busybox，尝试复制系统shell
        let shells = ["/bin/sh", "/bin/bash", "/usr/bin/sh"];
        for shell in &shells {
            if std::path::Path::new(shell).exists() {
                let dest = rootfs.join("bin/sh");
                fs::copy(shell, &dest)?;
                break;
            }
        }
    }

    // 创建必要的设备文件
    let dev_dir = rootfs.join("dev");

    // null设备
    let null_path = dev_dir.join("null");
    if !null_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", null_path.to_str().unwrap(), "c", "1", "3"])
            .output()?;
    }

    // zero设备
    let zero_path = dev_dir.join("zero");
    if !zero_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", zero_path.to_str().unwrap(), "c", "1", "5"])
            .output()?;
    }

    // random设备
    let random_path = dev_dir.join("random");
    if !random_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", random_path.to_str().unwrap(), "c", "1", "8"])
            .output()?;
    }

    // urandom设备
    let urandom_path = dev_dir.join("urandom");
    if !urandom_path.exists() {
        std::process::Command::new("mknod")
            .args(&["-m", "666", urandom_path.to_str().unwrap(), "c", "1", "9"])
            .output()?;
    }

    Ok(())
}

// OCI配置生成测试（不需要runc）
#[test]
fn test_oci_spec_generation() {
    use crius::oci::spec::Spec;

    let spec = Spec::new("1.0.2");
    let json = spec.to_json().expect("Failed to serialize spec");

    // camelCase转换: oci_version -> ociVersion
    assert!(json.contains("ociVersion"));
    assert!(json.contains("1.0.2"));
}

#[test]
fn test_default_namespaces() {
    use crius::oci::spec::Spec;

    let namespaces = Spec::default_namespaces();

    assert_eq!(namespaces.len(), 5);

    let ns_types: Vec<String> = namespaces.iter().map(|n| n.ns_type.clone()).collect();

    assert!(ns_types.contains(&"pid".to_string()));
    assert!(ns_types.contains(&"network".to_string()));
    assert!(ns_types.contains(&"ipc".to_string()));
    assert!(ns_types.contains(&"uts".to_string()));
    assert!(ns_types.contains(&"mount".to_string()));
}

#[test]
fn test_default_mounts() {
    use crius::oci::spec::Spec;

    let mounts = Spec::default_mounts();

    // 检查是否有基本挂载点
    let destinations: Vec<String> = mounts.iter().map(|m| m.destination.clone()).collect();

    assert!(destinations.contains(&"/proc".to_string()));
    assert!(destinations.contains(&"/sys".to_string()));
    assert!(destinations.contains(&"/dev".to_string()));
}
