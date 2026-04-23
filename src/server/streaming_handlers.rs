use super::*;

impl RuntimeServiceImpl {
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
        self.shim_work_dir.join(container_id).join("attach.sock")
    }

    pub(super) async fn exec(
        &self,
        request: Request<ExecRequest>,
    ) -> Result<Response<ExecResponse>, Status> {
        let mut req = request.into_inner();
        req.container_id = self.resolve_container_id(&req.container_id).await?;
        self.ensure_container_is_streamable(&req.container_id, "exec")
            .await?;
        let streaming = self.get_streaming_server().await?;
        let response = streaming.get_exec(&req).await?;
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

        self.ensure_container_is_streamable(&container_id, "exec_sync")
            .await?;

        let mut command = TokioCommand::new(&self.config.runtime_path);
        command.arg("exec");
        command.arg(&container_id);
        for arg in &cmd {
            command.arg(arg);
        }
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

        let stdout = match stdout_task {
            Some(task) => task
                .await
                .map_err(|e| Status::internal(format!("Failed to join stdout task: {}", e)))?
                .map_err(|e| Status::internal(format!("Failed to read stdout: {}", e)))?,
            None => Vec::new(),
        };
        let stderr = match stderr_task {
            Some(task) => task
                .await
                .map_err(|e| Status::internal(format!("Failed to join stderr task: {}", e)))?
                .map_err(|e| Status::internal(format!("Failed to read stderr: {}", e)))?,
            None => Vec::new(),
        };

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

        let netns_path =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY)
                .and_then(|state| state.netns_path)
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

        let streaming = self.get_streaming_server().await?;
        let response = streaming
            .get_port_forward(&req, PathBuf::from(netns_path))
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
                    .map(|state| state.tty)
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

            if !req.stdin && log_path.is_some() {
                let streaming = self.get_streaming_server().await?;
                let response = streaming
                    .get_attach_log(&req, PathBuf::from(log_path.unwrap()))
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
        let response = streaming.get_attach(&req).await?;
        Ok(Response::new(response))
    }
}
