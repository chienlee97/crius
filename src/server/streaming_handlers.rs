use super::*;

impl RuntimeServiceImpl {
    async fn await_exec_sync_reader(
        &self,
        mut task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
        stream_name: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Vec<u8>, Status> {
        let join_result = if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, &mut task).await {
                Ok(result) => result,
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    return Err(Status::deadline_exceeded(format!(
                        "Exec sync {} drain timed out after {}ms",
                        stream_name,
                        self.config.exec_sync_io_drain_timeout.as_millis()
                    )));
                }
            }
        } else {
            task.await
        };

        join_result
            .map_err(|e| Status::internal(format!("Failed to join {} task: {}", stream_name, e)))?
            .map_err(|e| Status::internal(format!("Failed to read {}: {}", stream_name, e)))
    }

    async fn drain_exec_sync_io(
        &self,
        stdout_task: Option<tokio::task::JoinHandle<std::io::Result<Vec<u8>>>>,
        stderr_task: Option<tokio::task::JoinHandle<std::io::Result<Vec<u8>>>>,
    ) -> Result<(Vec<u8>, Vec<u8>), Status> {
        let deadline = (!self.config.exec_sync_io_drain_timeout.is_zero())
            .then(|| tokio::time::Instant::now() + self.config.exec_sync_io_drain_timeout);

        let stdout = match stdout_task {
            Some(task) => {
                self.await_exec_sync_reader(task, "stdout", deadline)
                    .await?
            }
            None => Vec::new(),
        };
        let stderr = match stderr_task {
            Some(task) => {
                self.await_exec_sync_reader(task, "stderr", deadline)
                    .await?
            }
            None => Vec::new(),
        };

        Ok((stdout, stderr))
    }

    pub(super) async fn get_streaming_server(&self) -> Result<StreamingServer, Status> {
        let streaming = self.streaming.lock().await;
        streaming
            .clone()
            .ok_or_else(|| Status::unavailable("streaming server is not initialized"))
    }

    pub(super) async fn ensure_container_is_streamable(
        &self,
        container_id: &str,
        operation: &str,
    ) -> Result<(), Status> {
        let runtime_status = self.runtime_container_status_checked(container_id).await;
        if matches!(
            runtime_status,
            ContainerStatus::Created | ContainerStatus::Running
        ) {
            return Ok(());
        }

        Err(Status::failed_precondition(format!(
            "container {} is not in a streamable state for {}: current runtime state is {}",
            container_id,
            operation,
            Self::runtime_container_status_name(&runtime_status)
        )))
    }

    pub(super) fn attach_socket_path(&self, container_id: &str) -> PathBuf {
        self.attach_socket_dir
            .join(container_id)
            .join("attach.sock")
    }

    pub(super) fn task_socket_path(&self, container_id: &str) -> PathBuf {
        crate::shim_rpc::default_task_socket_path(&self.shim_work_dir, container_id)
    }

    async fn fail_if_shim_owned_task_socket_missing(
        &self,
        container_id: &str,
        operation: &str,
        task_socket_path: &Path,
    ) -> Result<(), Status> {
        let shim_record = {
            let persistence = self.persistence.lock().await;
            persistence
                .get_shim_process_record(container_id)
                .map_err(|err| {
                    Status::internal(format!(
                        "Failed to inspect shim ledger for container {}: {}",
                        container_id, err
                    ))
                })?
        };

        if shim_record.is_none() {
            return Ok(());
        }

        let details = format!(
            "{} requires shim task socket {}, but the ledger-owned shim task socket is missing",
            operation,
            task_socket_path.display()
        );
        if let Err(err) = self
            .persistence
            .lock()
            .await
            .storage_mut()
            .append_typed_event(
                "exec",
                "task",
                container_id,
                None,
                Some("shim_socket_missing"),
                Some(&details),
            )
        {
            log::warn!(
                "Failed to record missing shim task socket event for {}: {}",
                container_id,
                err
            );
        }

        Err(Status::failed_precondition(format!(
            "{} is not available for container {}: shim task socket {} is missing",
            operation,
            container_id,
            task_socket_path.display()
        )))
    }

    pub(super) async fn exec(
        &self,
        request: Request<ExecRequest>,
    ) -> Result<Response<ExecResponse>, Status> {
        let mut req = request.into_inner();
        req.container_id = self.resolve_container_id(&req.container_id).await?;
        let task_socket_path = self.task_socket_path(&req.container_id);
        if !task_socket_path.exists() {
            self.fail_if_shim_owned_task_socket_missing(
                &req.container_id,
                "exec",
                &task_socket_path,
            )
            .await?;
        }
        self.ensure_container_is_streamable(&req.container_id, "exec")
            .await?;
        let runtime = self
            .runtime_for_container_request(&req.container_id)
            .await?;
        let runtime_path = runtime.runtime_path().to_path_buf();
        let runtime_config_path = runtime.runtime_config_path().to_path_buf();
        let runtime_handler = self
            .runtime_handler_name_for_container_request(&req.container_id)
            .await?;
        let websocket_enabled = self
            .config
            .runtime_configs
            .get(runtime_handler.as_str())
            .map(|config| config.stream_websockets)
            .unwrap_or(false);
        let exec_cpu_affinity = self.effective_exec_cpu_affinity(&req.container_id).await;
        let (exec_io_socket_path, exec_resize_socket_path) = if task_socket_path.exists() {
            let request = crate::shim_rpc::ShimRpcRequest::OpenExecSession(
                crate::shim_rpc::OpenExecSessionRequest {
                    container_id: req.container_id.clone(),
                    command: req.cmd.clone(),
                    tty: req.tty,
                    stdin: req.stdin,
                    stdout: req.stdout,
                    stderr: req.stderr,
                    exec_cpu_affinity,
                },
            );
            let task_socket_path_clone = task_socket_path.clone();
            let response = tokio::task::spawn_blocking(move || {
                crate::shim_rpc::ShimRpcClient::new(
                    task_socket_path_clone,
                    std::time::Duration::from_secs(5),
                )
                .request(request)
            })
            .await
            .map_err(|err| Status::internal(format!("Failed to join shim exec task: {}", err)))?
            .map_err(|err| {
                Status::internal(format!("Shim exec session request failed: {}", err))
            })?;
            match response {
                crate::shim_rpc::ShimRpcResponse::OpenExecSession(response) => {
                    (Some(response.io_socket_path), response.resize_socket_path)
                }
                other => {
                    return Err(Status::internal(format!(
                        "unexpected shim exec session response for container {}: {:?}",
                        req.container_id, other
                    )));
                }
            }
        } else {
            self.fail_if_shim_owned_task_socket_missing(
                &req.container_id,
                "exec",
                &task_socket_path,
            )
            .await?;
            (None, None)
        };
        let streaming = self.get_streaming_server().await?;
        let response = streaming
            .get_exec(
                &req,
                crate::streaming::ExecStreamOptions {
                    runtime_path,
                    runtime_config_path,
                    exec_cpu_affinity,
                    exec_io_socket_path,
                    exec_resize_socket_path,
                    websocket_enabled,
                },
            )
            .await?;
        Ok(Response::new(response))
    }

    pub(super) async fn exec_sync(
        &self,
        request: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        let req = request.into_inner();
        let container_id = self.resolve_container_id(&req.container_id).await?;
        let cmd = req.cmd;
        let timeout = req.timeout;

        log::info!("Exec sync in container {}: {:?}", container_id, cmd);

        if cmd.is_empty() {
            return Err(Status::invalid_argument("cmd must not be empty"));
        }

        let task_socket_path = self.task_socket_path(&container_id);
        if !task_socket_path.exists() {
            self.fail_if_shim_owned_task_socket_missing(
                &container_id,
                "exec_sync",
                &task_socket_path,
            )
            .await?;
        }

        self.ensure_container_is_streamable(&container_id, "exec_sync")
            .await?;

        let runtime = self.runtime_for_container_request(&container_id).await?;
        let exec_cpu_affinity = self.effective_exec_cpu_affinity(&container_id).await;
        if task_socket_path.exists() {
            let request =
                crate::shim_rpc::ShimRpcRequest::ExecProcess(crate::shim_rpc::ExecProcessRequest {
                    container_id: container_id.clone(),
                    command: cmd.clone(),
                    tty: false,
                    capture_output: true,
                    timeout_ms: (timeout > 0).then_some(timeout as u64 * 1000),
                    io_drain_timeout_ms: (!self.config.exec_sync_io_drain_timeout.is_zero())
                        .then_some(self.config.exec_sync_io_drain_timeout.as_millis() as u64),
                    exec_cpu_affinity,
                });
            let task_socket_path_clone = task_socket_path.clone();
            let response = tokio::task::spawn_blocking(move || {
                crate::shim_rpc::ShimRpcClient::new(
                    task_socket_path_clone,
                    std::time::Duration::from_secs(5),
                )
                .request(request)
            })
            .await
            .map_err(|err| {
                Status::internal(format!("Failed to join shim exec_sync task: {}", err))
            })?
            .map_err(|err| {
                let message = err.to_string();
                if message.contains("timed out") {
                    Status::deadline_exceeded(message)
                } else {
                    Status::internal(format!("Shim exec_sync failed: {}", message))
                }
            })?;

            if let crate::shim_rpc::ShimRpcResponse::ExecProcess(response) = response {
                return Ok(Response::new(ExecSyncResponse {
                    stdout: response.stdout,
                    stderr: response.stderr,
                    exit_code: response.exit_code,
                }));
            }
            return Err(Status::internal(format!(
                "unexpected shim exec_sync response for container {}",
                container_id
            )));
        }

        self.fail_if_shim_owned_task_socket_missing(&container_id, "exec_sync", &task_socket_path)
            .await?;

        let runtime_path = runtime.runtime_path().to_path_buf();
        let runtime_config_path = runtime.runtime_config_path().to_path_buf();
        let mut command = TokioCommand::new(&runtime_path);
        if !runtime_config_path.as_os_str().is_empty() {
            command.arg("--config").arg(&runtime_config_path);
        }
        command.arg("exec");
        command.arg(&container_id);
        for arg in &cmd {
            command.arg(arg);
        }
        crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_tokio_command(
            &mut command,
            exec_cpu_affinity,
        );
        command.stdin(Stdio::null());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let mut child = command
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to spawn exec process: {}", e)))?;

        let stdout_task = child.stdout.take().map(|mut stdout| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                stdout.read_to_end(&mut buf).await.map(|_| buf)
            })
        });
        let stderr_task = child.stderr.take().map(|mut stderr| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                stderr.read_to_end(&mut buf).await.map(|_| buf)
            })
        });

        let status = if timeout > 0 {
            let timeout = std::time::Duration::from_secs(timeout as u64);
            match tokio::time::timeout(timeout, child.wait()).await {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    return Err(Status::internal(format!("Exec failed: {}", e)));
                }
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    let _ = self.drain_exec_sync_io(stdout_task, stderr_task).await;
                    return Err(Status::deadline_exceeded(format!(
                        "Exec sync timed out after {}s",
                        timeout.as_secs()
                    )));
                }
            }
        } else {
            child
                .wait()
                .await
                .map_err(|e| Status::internal(format!("Exec failed: {}", e)))?
        };

        let (stdout, stderr) = self.drain_exec_sync_io(stdout_task, stderr_task).await?;

        Ok(Response::new(ExecSyncResponse {
            stdout,
            stderr,
            exit_code: status.code().unwrap_or_default(),
        }))
    }

    pub(super) async fn port_forward(
        &self,
        request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;
        if req.port.iter().any(|port| *port <= 0 || *port > 65535) {
            return Err(Status::invalid_argument(
                "all forwarded ports must be in 1..=65535",
            ));
        }

        let pod = {
            let pod_sandboxes = self.pod_sandboxes.lock().await;
            pod_sandboxes.get(&pod_id).cloned()
        }
        .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
        if pod.state != PodSandboxState::SandboxReady as i32 {
            return Err(Status::failed_precondition(format!(
                "pod sandbox {} is not ready",
                pod_id
            )));
        }

        let pod_state =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY);
        let netns_path = if !Self::pod_requires_managed_netns(pod_state.as_ref()) {
            "/proc/thread-self/ns/net".to_string()
        } else {
            let netns_path = pod_state
                .as_ref()
                .and_then(|state| state.netns_path.clone())
                .or_else(|| {
                    self.pod_manager
                        .try_lock()
                        .ok()
                        .and_then(|manager| manager.get_pod_netns(&pod_id))
                        .map(|path| path.to_string_lossy().to_string())
                })
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "pod sandbox {} has no network namespace path",
                        pod_id
                    ))
                })?;

            if !std::path::Path::new(&netns_path).exists() {
                return Err(Status::failed_precondition(format!(
                    "pod sandbox {} network namespace does not exist: {}",
                    pod_id, netns_path
                )));
            }
            netns_path
        };

        let streaming = self.get_streaming_server().await?;
        let runtime_handler = pod_state
            .as_ref()
            .map(|state| state.runtime_handler.as_str())
            .filter(|handler| !handler.is_empty())
            .unwrap_or(pod.runtime_handler.as_str());
        let websocket_enabled = self
            .config
            .runtime_configs
            .get(runtime_handler)
            .map(|config| config.stream_websockets)
            .unwrap_or(false);
        let response = streaming
            .get_port_forward(&req, PathBuf::from(netns_path), websocket_enabled)
            .await?;
        Ok(Response::new(response))
    }

    pub(super) async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        let mut req = request.into_inner();
        req.container_id = self.resolve_container_id(&req.container_id).await?;
        self.ensure_container_is_streamable(&req.container_id, "attach")
            .await?;
        let runtime_handler = self
            .runtime_handler_name_for_container_request(&req.container_id)
            .await?;
        let websocket_enabled = self
            .config
            .runtime_configs
            .get(runtime_handler.as_str())
            .map(|config| config.stream_websockets)
            .unwrap_or(false);
        let attach_socket_path = self.attach_socket_path(&req.container_id);
        if !attach_socket_path.exists() {
            let should_try_restore = {
                let containers = self.containers.lock().await;
                containers
                    .get(&req.container_id)
                    .and_then(|container| {
                        Self::read_internal_state::<StoredContainerState>(
                            &container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                        )
                    })
                    .map(|state| state.tty && req.stdin)
                    .unwrap_or(false)
            };

            if should_try_restore {
                let runtime = self.runtime.clone();
                let container_id = req.container_id.clone();
                if tokio::task::spawn_blocking(move || runtime.restore_attach_shim(&container_id))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some()
                {
                    for _ in 0..20 {
                        if attach_socket_path.exists() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
        if !attach_socket_path.exists() {
            let log_path = {
                let containers = self.containers.lock().await;
                containers
                    .get(&req.container_id)
                    .and_then(|container| {
                        Self::read_internal_state::<StoredContainerState>(
                            &container.annotations,
                            INTERNAL_CONTAINER_STATE_KEY,
                        )
                        .and_then(|state| state.log_path)
                    })
                    .filter(|path| !path.is_empty())
            };

            if !req.stdin {
                let Some(log_path) = log_path else {
                    return Err(Status::failed_precondition(format!(
                        "attach is not available for container {}: attach socket {} is missing and no interactive recovery path is available",
                        req.container_id,
                        attach_socket_path.display()
                    )));
                };
                let streaming = self.get_streaming_server().await?;
                let response = streaming
                    .get_attach_log(&req, PathBuf::from(log_path), websocket_enabled)
                    .await?;
                return Ok(Response::new(response));
            }

            return Err(Status::failed_precondition(format!(
                "attach is not available for container {}: attach socket {} is missing and no interactive recovery path is available",
                req.container_id,
                attach_socket_path.display()
            )));
        }
        let streaming = self.get_streaming_server().await?;
        let response = streaming.get_attach(&req, websocket_enabled).await?;
        Ok(Response::new(response))
    }
}
