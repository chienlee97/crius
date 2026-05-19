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
    set_fake_runtime_state_without_shim(&dir, "container-running", "running");

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
    set_fake_runtime_state_without_shim(&dir, "container-running", "running");

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
    assert!(err.message().contains("exec stdout drain timed out"));
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
    set_fake_runtime_state(&dir, "container-kata", "running");

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
            cmd: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "awk '/Cpus_allowed_list/ {{print $2}}' /proc/self/status > '{}'; printf affinity-ok",
                    affinity_path.display()
                ),
            ],
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
            cmd: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "awk '/Cpus_allowed_list/ {{print $2}}' /proc/self/status > '{}'; printf affinity-ok",
                    affinity_path.display()
                ),
            ],
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
