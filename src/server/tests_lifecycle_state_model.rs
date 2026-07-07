fn set_fake_runtime_state(dir: &TempDir, id: &str, state: &str) {
    set_fake_runtime_state_without_shim(dir, id, state);
    start_fake_shim_rpc(dir, id);
}

fn set_fake_runtime_state_without_shim(dir: &TempDir, id: &str, state: &str) {
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

fn start_fake_shim_rpc(dir: &TempDir, id: &str) {
    static STARTED: OnceLock<StdMutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();

    let socket_path = dir.path().join("shims").join(id).join("task.sock");
    let started = STARTED.get_or_init(|| StdMutex::new(std::collections::HashSet::new()));
    {
        let mut started = started.lock().unwrap();
        if !started.insert(socket_path.clone()) {
            return;
        }
    }

    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let handler = Arc::new(FakeShimRpcHandler {
        root: dir.path().to_path_buf(),
        id: id.to_string(),
    });
    let socket_path_for_thread = socket_path.clone();
    std::thread::spawn(move || {
        let _ = crate::shim_rpc::server::serve(&socket_path_for_thread, running, handler);
    });

    let client =
        crate::shim_rpc::ShimRpcClient::new(socket_path.clone(), std::time::Duration::from_secs(1));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if matches!(
            client.request(crate::shim_rpc::ShimRpcRequest::Ping),
            Ok(crate::shim_rpc::ShimRpcResponse::Empty)
        ) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!(
        "fake shim RPC socket {} was not ready before deadline",
        socket_path.display()
    );
}

struct FakeShimRpcHandler {
    root: PathBuf,
    id: String,
}

impl FakeShimRpcHandler {
    fn state_dir(&self) -> PathBuf {
        self.root.join("runtime-state")
    }

    fn state_path(&self) -> PathBuf {
        self.state_dir().join(format!("{}.state", self.id))
    }

    fn pid_path(&self) -> PathBuf {
        self.state_dir().join(format!("{}.pid", self.id))
    }

    fn exit_code_path(&self) -> PathBuf {
        self.root.join("exits").join(&self.id)
    }

    fn update_path(&self) -> PathBuf {
        self.state_dir().join(format!("{}.update.json", self.id))
    }

    fn read_state(&self) -> String {
        fs::read_to_string(self.state_path())
            .map(|value| value.trim().to_string())
            .unwrap_or_else(|_| "created".to_string())
    }

    fn write_state(&self, state: &str) -> anyhow::Result<()> {
        fs::create_dir_all(self.state_dir())?;
        fs::write(self.state_path(), state)?;
        if state == "running" {
            fs::write(self.pid_path(), std::process::id().to_string())?;
        } else if matches!(state, "stopped" | "deleted") {
            let _ = fs::remove_file(self.pid_path());
        }
        Ok(())
    }

    fn exit_code(&self) -> Option<i32> {
        fs::read_to_string(self.exit_code_path())
            .ok()
            .and_then(|value| value.trim().parse().ok())
    }

    fn pid(&self) -> Option<i32> {
        fs::read_to_string(self.pid_path())
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .or_else(|| {
                matches!(
                    self.task_state(),
                    crate::shim_rpc::TaskState::Running | crate::shim_rpc::TaskState::Paused
                )
                .then_some(std::process::id() as i32)
            })
    }

    fn task_state(&self) -> crate::shim_rpc::TaskState {
        match self.read_state().as_str() {
            "running" => crate::shim_rpc::TaskState::Running,
            "paused" => crate::shim_rpc::TaskState::Paused,
            "stopped" => crate::shim_rpc::TaskState::Stopped,
            "deleted" => crate::shim_rpc::TaskState::Deleted,
            _ => crate::shim_rpc::TaskState::Created,
        }
    }
}

impl crate::shim_rpc::server::ShimRpcHandler for FakeShimRpcHandler {
    fn handle_request(
        &self,
        request: crate::shim_rpc::ShimRpcRequest,
    ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
        match request {
            crate::shim_rpc::ShimRpcRequest::Ping => Ok(crate::shim_rpc::ShimRpcResponse::Empty),
            crate::shim_rpc::ShimRpcRequest::CreateTask(_) => {
                self.write_state("created")?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::StartTask(_) => {
                self.write_state("running")?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::KillTask(_) => {
                self.write_state("stopped")?;
                fs::create_dir_all(self.exit_code_path().parent().unwrap())?;
                fs::write(self.exit_code_path(), "0\n")?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::DeleteTask(_) => {
                self.write_state("deleted")?;
                let _ = fs::remove_file(self.state_path());
                let _ = fs::remove_file(self.pid_path());
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::WaitProcess(_) => {
                Ok(crate::shim_rpc::ShimRpcResponse::WaitProcess(
                    crate::shim_rpc::WaitProcessResponse {
                        exit_code: self.exit_code(),
                    },
                ))
            }
            crate::shim_rpc::ShimRpcRequest::Status(_) => {
                Ok(crate::shim_rpc::ShimRpcResponse::Status(
                    crate::shim_rpc::StatusResponse {
                        state: self.task_state(),
                        pid: self.pid(),
                        exit_code: self.exit_code().or_else(|| {
                            (self.task_state() == crate::shim_rpc::TaskState::Stopped).then_some(0)
                        }),
                    },
                ))
            }
            crate::shim_rpc::ShimRpcRequest::ContainerPid(_) => {
                Ok(crate::shim_rpc::ShimRpcResponse::ContainerPid(self.pid()))
            }
            crate::shim_rpc::ShimRpcRequest::PauseTask(_) => {
                self.write_state("paused")?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::ResumeTask(_) => {
                self.write_state("running")?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::CheckpointTask(request) => {
                fs::create_dir_all(&request.image_path)?;
                fs::create_dir_all(&request.work_path)?;
                fs::write(
                    request.image_path.join("checkpoint.json"),
                    serde_json::json!({"id": self.id, "checkpointed": true}).to_string(),
                )?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::RestoreTask(request) => {
                fs::create_dir_all(&request.work_path)?;
                self.write_state("running")?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::UpdateResources(request) => {
                fs::create_dir_all(self.state_dir())?;
                fs::write(
                    self.update_path(),
                    serde_json::json!({
                        "cpu": {
                            "shares": request.resources.cpu_shares,
                            "quota": request.resources.cpu_quota,
                            "period": request.resources.cpu_period,
                            "cpus": request.resources.cpuset_cpus,
                            "mems": request.resources.cpuset_mems,
                        },
                        "memory": {
                            "limit": request.resources.memory_limit_in_bytes,
                            "swap": request.resources.memory_swap_limit_in_bytes,
                        }
                    })
                    .to_string(),
                )?;
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::ReopenLog(_)
            | crate::shim_rpc::ShimRpcRequest::ResizePty(_)
            | crate::shim_rpc::ShimRpcRequest::ResizeAttachPty(_)
            | crate::shim_rpc::ShimRpcRequest::CloseAttachStream(_) => {
                Ok(crate::shim_rpc::ShimRpcResponse::Empty)
            }
            crate::shim_rpc::ShimRpcRequest::ExecProcess(request) => {
                let output = run_fake_exec_process(&request)?;
                Ok(crate::shim_rpc::ShimRpcResponse::ExecProcess(
                    crate::shim_rpc::ExecProcessResponse {
                        exit_code: output.status.code().unwrap_or_default(),
                        stdout: output.stdout,
                        stderr: output.stderr,
                    },
                ))
            }
            crate::shim_rpc::ShimRpcRequest::OpenExecSession(request) => {
                let session_dir = self
                    .root
                    .join("exec-sessions")
                    .join(&self.id)
                    .join(format!("exec-{}", RuntimeServiceImpl::now_nanos()));
                fs::create_dir_all(&session_dir)?;
                let io_socket_path = session_dir.join("io.sock");
                let resize_socket_path = request.tty.then(|| session_dir.join("resize.sock"));
                Ok(crate::shim_rpc::ShimRpcResponse::OpenExecSession(
                    crate::shim_rpc::OpenExecSessionResponse {
                        session_id: session_dir
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("exec")
                            .to_string(),
                        io_socket_path,
                        resize_socket_path,
                    },
                ))
            }
            crate::shim_rpc::ShimRpcRequest::OpenAttachStream(request) => {
                let attach_dir = self.root.join("attach-sessions").join(&self.id);
                fs::create_dir_all(&attach_dir)?;
                let io_socket_path = attach_dir.join("attach.sock");
                let resize_socket_path = request.tty.then(|| attach_dir.join("resize.sock"));
                Ok(crate::shim_rpc::ShimRpcResponse::OpenAttachStream(
                    crate::shim_rpc::OpenAttachStreamResponse {
                        stream_id: "attach".to_string(),
                        io_socket_path,
                        resize_socket_path,
                    },
                ))
            }
        }
    }
}

fn run_fake_exec_process(
    request: &crate::shim_rpc::ExecProcessRequest,
) -> anyhow::Result<std::process::Output> {
    if request.command.is_empty() {
        return Ok(std::process::Command::new("true").output()?);
    }

    let mut command = std::process::Command::new(&request.command[0]);
    command.args(&request.command[1..]);
    crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_std_command(
        &mut command,
        request.exec_cpu_affinity,
    );
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());

    let mut child = command.spawn()?;
    let stdout = child.stdout.take().map(|mut stdout| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = tx.send(std::io::Read::read_to_end(&mut stdout, &mut buf).map(|_| buf));
        });
        rx
    });
    let stderr = child.stderr.take().map(|mut stderr| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = tx.send(std::io::Read::read_to_end(&mut stderr, &mut buf).map(|_| buf));
        });
        rx
    });

    let deadline = request
        .timeout_ms
        .map(|timeout| std::time::Instant::now() + std::time::Duration::from_millis(timeout));
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if let Some(deadline) = deadline {
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(anyhow::anyhow!(
                    "exec timed out after {}ms in container {}",
                    request.timeout_ms.unwrap_or_default(),
                    request.container_id
                ));
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    let drain_deadline = request
        .io_drain_timeout_ms
        .map(|timeout| std::time::Instant::now() + std::time::Duration::from_millis(timeout));
    Ok(std::process::Output {
        status,
        stdout: wait_fake_exec_reader(stdout, drain_deadline, "stdout")?,
        stderr: wait_fake_exec_reader(stderr, drain_deadline, "stderr")?,
    })
}

fn wait_fake_exec_reader(
    reader: Option<std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>>,
    deadline: Option<std::time::Instant>,
    stream_name: &str,
) -> anyhow::Result<Vec<u8>> {
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };

    match deadline {
        Some(deadline) => {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(anyhow::anyhow!("exec {} drain timed out", stream_name));
            }
            reader
                .recv_timeout(deadline.saturating_duration_since(now))
                .map_err(|_| anyhow::anyhow!("exec {} drain timed out", stream_name))?
                .map_err(Into::into)
        }
        None => reader
            .recv()
            .map_err(|_| anyhow::anyhow!("failed to join {} reader for exec", stream_name))?
            .map_err(Into::into),
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
        pod_id: Some("pod-1".to_string()),
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
