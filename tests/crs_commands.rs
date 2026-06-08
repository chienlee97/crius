use std::{
    collections::HashSet,
    net::SocketAddr,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crius::proto::{
    diagnostics::v1::{
        diagnostics_service_server::{DiagnosticsService, DiagnosticsServiceServer},
        ContainerLogChunk, ContainerLogRequest, ContentGcCandidate, ContentGcRequest,
        ContentGcResponse, EffectiveConfigRequest, EffectiveConfigResponse, ImageTransferInfo,
        ImageTransfersRequest, ImageTransfersResponse, NriStatusRequest, NriStatusResponse,
        RecoveryAction, RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest,
        RecoveryStatusResponse, RuntimeHandlerInfo, RuntimeHandlersRequest,
        RuntimeHandlersResponse, SecurityStatusRequest, SecurityStatusResponse, ServerInfoRequest,
        ServerInfoResponse, ShimStatusRequest, ShimStatusResponse,
    },
    runtime::v1::{
        image_service_server::{ImageService, ImageServiceServer},
        runtime_service_server::{RuntimeService, RuntimeServiceServer},
        *,
    },
};
use nix::libc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone, Default)]
struct MockRuntimeService {
    state: MockState,
}

#[derive(Clone, Default)]
struct MockImageService {
    state: MockState,
}

#[derive(Clone, Default)]
struct MockDiagnosticsService {
    state: MockState,
}

#[derive(Copy, Clone, Debug, Default)]
enum MockEventMode {
    #[default]
    TwoThenEof,
    Pending,
}

#[derive(Clone)]
struct MockState {
    version_requests: Arc<AtomicUsize>,
    status_requests: Arc<AtomicUsize>,
    runtime_config_requests: Arc<AtomicUsize>,
    update_runtime_config_requests: Arc<AtomicUsize>,
    run_pod_requests: Arc<AtomicUsize>,
    stop_pod_requests: Arc<AtomicUsize>,
    remove_pod_requests: Arc<AtomicUsize>,
    update_pod_resources_requests: Arc<AtomicUsize>,
    create_container_requests: Arc<AtomicUsize>,
    start_container_requests: Arc<AtomicUsize>,
    stop_container_requests: Arc<AtomicUsize>,
    remove_container_requests: Arc<AtomicUsize>,
    update_container_resources_requests: Arc<AtomicUsize>,
    checkpoint_container_requests: Arc<AtomicUsize>,
    reopen_container_log_requests: Arc<AtomicUsize>,
    exec_requests: Arc<AtomicUsize>,
    exec_sync_requests: Arc<AtomicUsize>,
    attach_requests: Arc<AtomicUsize>,
    port_forward_requests: Arc<AtomicUsize>,
    container_events_requests: Arc<AtomicUsize>,
    container_stats_requests: Arc<AtomicUsize>,
    list_container_stats_requests: Arc<AtomicUsize>,
    pod_stats_requests: Arc<AtomicUsize>,
    list_pod_stats_requests: Arc<AtomicUsize>,
    list_pod_metrics_requests: Arc<AtomicUsize>,
    metric_descriptors_requests: Arc<AtomicUsize>,
    list_pod_requests: Arc<AtomicUsize>,
    pod_inspect_requests: Arc<AtomicUsize>,
    list_container_requests: Arc<AtomicUsize>,
    container_inspect_requests: Arc<AtomicUsize>,
    list_image_requests: Arc<AtomicUsize>,
    image_inspect_requests: Arc<AtomicUsize>,
    image_fs_info_requests: Arc<AtomicUsize>,
    pull_image_requests: Arc<AtomicUsize>,
    remove_image_requests: Arc<AtomicUsize>,
    effective_config_requests: Arc<AtomicUsize>,
    runtime_handlers_requests: Arc<AtomicUsize>,
    image_transfers_requests: Arc<AtomicUsize>,
    container_log_requests: Arc<AtomicUsize>,
    recovery_status_requests: Arc<AtomicUsize>,
    recovery_check_requests: Arc<AtomicUsize>,
    content_gc_requests: Arc<AtomicUsize>,
    event_mode: Arc<Mutex<MockEventMode>>,
    metrics_endpoint: Arc<Mutex<Option<String>>>,
    metrics_enabled: Arc<Mutex<bool>>,
    last_update_runtime_config: Arc<Mutex<Option<UpdateRuntimeConfigRequest>>>,
    last_run_pod: Arc<Mutex<Option<RunPodSandboxRequest>>>,
    last_stop_pod: Arc<Mutex<Option<StopPodSandboxRequest>>>,
    last_remove_pod: Arc<Mutex<Option<RemovePodSandboxRequest>>>,
    last_update_pod_resources: Arc<Mutex<Option<UpdatePodSandboxResourcesRequest>>>,
    last_create_container: Arc<Mutex<Option<CreateContainerRequest>>>,
    last_start_container: Arc<Mutex<Option<StartContainerRequest>>>,
    last_stop_container: Arc<Mutex<Option<StopContainerRequest>>>,
    last_remove_container: Arc<Mutex<Option<RemoveContainerRequest>>>,
    last_update_container_resources: Arc<Mutex<Option<UpdateContainerResourcesRequest>>>,
    last_checkpoint_container: Arc<Mutex<Option<CheckpointContainerRequest>>>,
    last_reopen_container_log: Arc<Mutex<Option<ReopenContainerLogRequest>>>,
    last_exec: Arc<Mutex<Option<ExecRequest>>>,
    last_exec_sync: Arc<Mutex<Option<ExecSyncRequest>>>,
    last_attach: Arc<Mutex<Option<AttachRequest>>>,
    last_port_forward: Arc<Mutex<Option<PortForwardRequest>>>,
    last_container_stats: Arc<Mutex<Option<ContainerStatsRequest>>>,
    last_list_container_stats: Arc<Mutex<Option<ListContainerStatsRequest>>>,
    last_pod_stats: Arc<Mutex<Option<PodSandboxStatsRequest>>>,
    last_list_pod_stats: Arc<Mutex<Option<ListPodSandboxStatsRequest>>>,
    exec_url: Arc<Mutex<Option<String>>>,
    attach_url: Arc<Mutex<Option<String>>>,
    port_forward_url: Arc<Mutex<Option<String>>>,
    existing_images: Arc<Mutex<HashSet<String>>>,
    rpc_sequence: Arc<Mutex<Vec<String>>>,
    last_pull_image: Arc<Mutex<Option<PullImageRequest>>>,
    last_remove_image: Arc<Mutex<Option<RemoveImageRequest>>>,
    last_effective_config: Arc<Mutex<Option<EffectiveConfigRequest>>>,
    last_image_transfers: Arc<Mutex<Option<ImageTransfersRequest>>>,
    last_container_log: Arc<Mutex<Option<ContainerLogRequest>>>,
    last_recovery_check: Arc<Mutex<Option<RecoveryCheckRequest>>>,
    last_content_gc: Arc<Mutex<Option<ContentGcRequest>>>,
    fail_non_host_pod_network: Arc<Mutex<bool>>,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            version_requests: Arc::default(),
            status_requests: Arc::default(),
            runtime_config_requests: Arc::default(),
            update_runtime_config_requests: Arc::default(),
            run_pod_requests: Arc::default(),
            stop_pod_requests: Arc::default(),
            remove_pod_requests: Arc::default(),
            update_pod_resources_requests: Arc::default(),
            create_container_requests: Arc::default(),
            start_container_requests: Arc::default(),
            stop_container_requests: Arc::default(),
            remove_container_requests: Arc::default(),
            update_container_resources_requests: Arc::default(),
            checkpoint_container_requests: Arc::default(),
            reopen_container_log_requests: Arc::default(),
            exec_requests: Arc::default(),
            exec_sync_requests: Arc::default(),
            attach_requests: Arc::default(),
            port_forward_requests: Arc::default(),
            container_events_requests: Arc::default(),
            container_stats_requests: Arc::default(),
            list_container_stats_requests: Arc::default(),
            pod_stats_requests: Arc::default(),
            list_pod_stats_requests: Arc::default(),
            list_pod_metrics_requests: Arc::default(),
            metric_descriptors_requests: Arc::default(),
            list_pod_requests: Arc::default(),
            pod_inspect_requests: Arc::default(),
            list_container_requests: Arc::default(),
            container_inspect_requests: Arc::default(),
            list_image_requests: Arc::default(),
            image_inspect_requests: Arc::default(),
            image_fs_info_requests: Arc::default(),
            pull_image_requests: Arc::default(),
            remove_image_requests: Arc::default(),
            effective_config_requests: Arc::default(),
            runtime_handlers_requests: Arc::default(),
            image_transfers_requests: Arc::default(),
            container_log_requests: Arc::default(),
            recovery_status_requests: Arc::default(),
            recovery_check_requests: Arc::default(),
            content_gc_requests: Arc::default(),
            event_mode: Arc::default(),
            metrics_endpoint: Arc::default(),
            metrics_enabled: Arc::new(Mutex::new(true)),
            last_update_runtime_config: Arc::default(),
            last_run_pod: Arc::default(),
            last_stop_pod: Arc::default(),
            last_remove_pod: Arc::default(),
            last_update_pod_resources: Arc::default(),
            last_create_container: Arc::default(),
            last_start_container: Arc::default(),
            last_stop_container: Arc::default(),
            last_remove_container: Arc::default(),
            last_update_container_resources: Arc::default(),
            last_checkpoint_container: Arc::default(),
            last_reopen_container_log: Arc::default(),
            last_exec: Arc::default(),
            last_exec_sync: Arc::default(),
            last_attach: Arc::default(),
            last_port_forward: Arc::default(),
            last_container_stats: Arc::default(),
            last_list_container_stats: Arc::default(),
            last_pod_stats: Arc::default(),
            last_list_pod_stats: Arc::default(),
            exec_url: Arc::default(),
            attach_url: Arc::default(),
            port_forward_url: Arc::default(),
            existing_images: Arc::default(),
            rpc_sequence: Arc::default(),
            last_pull_image: Arc::default(),
            last_remove_image: Arc::default(),
            last_effective_config: Arc::default(),
            last_image_transfers: Arc::default(),
            last_container_log: Arc::default(),
            last_recovery_check: Arc::default(),
            last_content_gc: Arc::default(),
            fail_non_host_pod_network: Arc::default(),
        }
    }
}

#[tonic::async_trait]
impl RuntimeService for MockRuntimeService {
    async fn version(
        &self,
        request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        assert_eq!(request.into_inner().version, "");
        self.state.version_requests.fetch_add(1, Ordering::SeqCst);

        Ok(Response::new(VersionResponse {
            version: "0.1.0".into(),
            runtime_name: "crius-test".into(),
            runtime_version: "9.8.7".into(),
            runtime_api_version: "v1".into(),
        }))
    }

    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        let request = request.into_inner();
        let name = request
            .config
            .as_ref()
            .and_then(|config| config.metadata.as_ref())
            .map(|metadata| metadata.name.as_str())
            .unwrap_or_default()
            .to_string();
        self.state.run_pod_requests.fetch_add(1, Ordering::SeqCst);
        let uses_host_network = request
            .config
            .as_ref()
            .and_then(|config| config.linux.as_ref())
            .and_then(|linux| linux.security_context.as_ref())
            .and_then(|security| security.namespace_options.as_ref())
            .map(|namespaces| namespaces.network == NamespaceMode::Node as i32)
            .unwrap_or(false);
        *self.state.last_run_pod.lock().expect("last run pod lock") = Some(request);

        if *self
            .state
            .fail_non_host_pod_network
            .lock()
            .expect("fail non-host pod network lock")
            && !uses_host_network
        {
            return Err(Status::failed_precondition(
                "Calico Kubernetes API is unavailable while loading cluster information",
            ));
        }

        if name == "bad-runtime" {
            return Err(Status::failed_precondition(
                "runtime handler is not available",
            ));
        }

        Ok(Response::new(RunPodSandboxResponse {
            pod_sandbox_id: format!("pod-{name}"),
        }))
    }

    async fn stop_pod_sandbox(
        &self,
        request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        let request = request.into_inner();
        let pod = request.pod_sandbox_id.clone();
        self.state.stop_pod_requests.fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("stop-pod:{pod}"));
        *self.state.last_stop_pod.lock().expect("last stop pod lock") = Some(request);

        if pod == "missing" {
            return Err(Status::not_found("pod not found"));
        }

        Ok(Response::new(StopPodSandboxResponse {}))
    }

    async fn remove_pod_sandbox(
        &self,
        request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        let request = request.into_inner();
        let pod = request.pod_sandbox_id.clone();
        self.state
            .remove_pod_requests
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("remove-pod:{pod}"));
        *self
            .state
            .last_remove_pod
            .lock()
            .expect("last remove pod lock") = Some(request);

        if pod == "missing" {
            return Err(Status::not_found("pod not found"));
        }
        if pod == "busy" {
            return Err(Status::failed_precondition("pod still has containers"));
        }

        Ok(Response::new(RemovePodSandboxResponse {}))
    }
    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        let request = request.into_inner();
        self.state
            .pod_inspect_requests
            .fetch_add(1, Ordering::SeqCst);

        if matches!(
            request.pod_sandbox_id.as_str(),
            "missing" | "ctr-only" | "image-only" | "none"
        ) {
            return Err(Status::not_found("pod not found"));
        }

        let annotations = if request.pod_sandbox_id == "internal-pod" {
            [("crius.crs/internal-sandbox".to_string(), "true".to_string())].into()
        } else {
            Default::default()
        };

        Ok(Response::new(PodSandboxStatusResponse {
            status: Some(PodSandboxStatus {
                id: request.pod_sandbox_id,
                metadata: Some(PodSandboxMetadata {
                    name: "pod-a".into(),
                    uid: "uid-a".into(),
                    namespace: "default".into(),
                    attempt: 1,
                }),
                state: PodSandboxState::SandboxReady as i32,
                created_at: 42,
                network: Some(PodSandboxNetworkStatus {
                    ip: "10.0.0.2".into(),
                    additional_ips: vec![],
                }),
                labels: Default::default(),
                annotations,
                runtime_handler: "runc".into(),
                linux: None,
            }),
            info: [("network".to_string(), r#"{"namespace":"test"}"#.to_string())].into(),
            containers_statuses: vec![],
            timestamp: 123,
        }))
    }

    async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        let request = request.into_inner();
        self.state.list_pod_requests.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request
                .filter
                .and_then(|filter| filter.state.map(|state| state.state)),
            Some(PodSandboxState::SandboxReady as i32)
        );

        Ok(Response::new(ListPodSandboxResponse {
            items: vec![
                PodSandbox {
                    id: "pod123456789".into(),
                    metadata: Some(PodSandboxMetadata {
                        name: "pod-a".into(),
                        uid: "uid-a".into(),
                        namespace: "default".into(),
                        attempt: 1,
                    }),
                    state: PodSandboxState::SandboxReady as i32,
                    created_at: 42,
                    labels: Default::default(),
                    annotations: Default::default(),
                    runtime_handler: "runc".into(),
                },
                PodSandbox {
                    id: "internal-pod".into(),
                    metadata: Some(PodSandboxMetadata {
                        name: "crius-internal".into(),
                        uid: "uid-internal".into(),
                        namespace: "default".into(),
                        attempt: 1,
                    }),
                    state: PodSandboxState::SandboxReady as i32,
                    created_at: 42,
                    labels: Default::default(),
                    annotations: [("crius.crs/internal-sandbox".to_string(), "true".to_string())]
                        .into(),
                    runtime_handler: "runc".into(),
                },
            ],
        }))
    }
    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        let request = request.into_inner();
        let pod = request.pod_sandbox_id.clone();
        let image = request
            .config
            .as_ref()
            .and_then(|config| config.image.as_ref())
            .map(|image| image.image.as_str())
            .unwrap_or_default()
            .to_string();
        self.state
            .create_container_requests
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("create-container:{image}"));
        *self
            .state
            .last_create_container
            .lock()
            .expect("last create container lock") = Some(request);

        if pod == "missing" {
            return Err(Status::not_found("pod not found"));
        }
        if image == "bad-config" {
            return Err(Status::failed_precondition("container config rejected"));
        }

        Ok(Response::new(CreateContainerResponse {
            container_id: format!("ctr-{pod}-{image}"),
        }))
    }

    async fn start_container(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .start_container_requests
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("start-container:{id}"));
        *self
            .state
            .last_start_container
            .lock()
            .expect("last start container lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }
        if id == "running" {
            return Err(Status::failed_precondition("container is already running"));
        }

        Ok(Response::new(StartContainerResponse {}))
    }

    async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .stop_container_requests
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("stop-container:{id}"));
        *self
            .state
            .last_stop_container
            .lock()
            .expect("last stop container lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }

        Ok(Response::new(StopContainerResponse {}))
    }

    async fn remove_container(
        &self,
        request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .remove_container_requests
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("remove-container:{id}"));
        *self
            .state
            .last_remove_container
            .lock()
            .expect("last remove container lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }
        if id == "running" {
            return Err(Status::failed_precondition("container is running"));
        }

        Ok(Response::new(RemoveContainerResponse {}))
    }
    async fn list_containers(
        &self,
        request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        let request = request.into_inner();
        self.state
            .list_container_requests
            .fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request
                .filter
                .and_then(|filter| filter.state.map(|state| state.state)),
            Some(ContainerState::ContainerRunning as i32)
        );

        Ok(Response::new(ListContainersResponse {
            containers: vec![Container {
                id: "ctr123456789".into(),
                pod_sandbox_id: "pod123456789".into(),
                metadata: Some(ContainerMetadata {
                    name: "ctr-a".into(),
                    attempt: 2,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".into(),
                    user_specified_image: "busybox:latest".into(),
                    ..Default::default()
                }),
                image_ref: "sha256:image".into(),
                state: ContainerState::ContainerRunning as i32,
                created_at: 42,
                labels: Default::default(),
                annotations: Default::default(),
            }],
        }))
    }

    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        let request = request.into_inner();
        self.state
            .container_inspect_requests
            .fetch_add(1, Ordering::SeqCst);

        if matches!(
            request.container_id.as_str(),
            "missing" | "pod-only" | "image-only" | "none"
        ) {
            return Err(Status::not_found("container not found"));
        }

        Ok(Response::new(ContainerStatusResponse {
            status: Some(ContainerStatus {
                id: request.container_id,
                metadata: Some(ContainerMetadata {
                    name: "ctr-a".into(),
                    attempt: 2,
                }),
                state: ContainerState::ContainerRunning as i32,
                created_at: 42,
                started_at: 43,
                finished_at: 0,
                exit_code: 0,
                image: Some(ImageSpec {
                    image: "busybox:latest".into(),
                    user_specified_image: "busybox:latest".into(),
                    ..Default::default()
                }),
                image_ref: "sha256:image".into(),
                reason: String::new(),
                message: String::new(),
                labels: Default::default(),
                annotations: Default::default(),
                mounts: vec![],
                log_path: "ctr-a.log".into(),
                resources: None,
            }),
            info: [("pid".to_string(), "1234".to_string())].into(),
        }))
    }
    async fn update_container_resources(
        &self,
        request: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .update_container_resources_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_update_container_resources
            .lock()
            .expect("last update container resources lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }
        if id == "unsupported" {
            return Err(Status::failed_precondition(
                "container update is unsupported",
            ));
        }

        Ok(Response::new(UpdateContainerResourcesResponse {}))
    }

    async fn reopen_container_log(
        &self,
        request: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .reopen_container_log_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_reopen_container_log
            .lock()
            .expect("last reopen container log lock") = Some(request);

        if id == "missing-log" {
            return Err(Status::failed_precondition(
                "container log is not configured",
            ));
        }

        Ok(Response::new(ReopenContainerLogResponse {}))
    }
    async fn exec(&self, request: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state.exec_requests.fetch_add(1, Ordering::SeqCst);
        *self.state.last_exec.lock().expect("last exec lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }

        let url = self
            .state
            .exec_url
            .lock()
            .expect("exec url lock")
            .clone()
            .unwrap_or_else(|| "ws://127.0.0.1:9/exec/mock".to_string());
        Ok(Response::new(ExecResponse { url }))
    }
    async fn exec_sync(
        &self,
        request: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        let cmd = request.cmd.clone();
        self.state.exec_sync_requests.fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_exec_sync
            .lock()
            .expect("last exec-sync lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }

        let exit_code = cmd
            .iter()
            .find_map(|arg| arg.strip_prefix("exit-"))
            .and_then(|code| code.parse::<i32>().ok())
            .unwrap_or_default();
        Ok(Response::new(ExecSyncResponse {
            stdout: b"sync-stdout".to_vec(),
            stderr: b"sync-stderr".to_vec(),
            exit_code,
        }))
    }
    async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state.attach_requests.fetch_add(1, Ordering::SeqCst);
        *self.state.last_attach.lock().expect("last attach lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }

        let url = self
            .state
            .attach_url
            .lock()
            .expect("attach url lock")
            .clone()
            .unwrap_or_else(|| "ws://127.0.0.1:9/attach/mock".to_string());
        Ok(Response::new(AttachResponse { url }))
    }
    async fn port_forward(
        &self,
        request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        let request = request.into_inner();
        let pod = request.pod_sandbox_id.clone();
        self.state
            .port_forward_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_port_forward
            .lock()
            .expect("last port-forward lock") = Some(request);

        if pod == "missing" {
            return Err(Status::not_found("pod not found"));
        }

        let url = self
            .state
            .port_forward_url
            .lock()
            .expect("port-forward url lock")
            .clone()
            .unwrap_or_else(|| "ws://127.0.0.1:9/portforward/mock".to_string());
        Ok(Response::new(PortForwardResponse { url }))
    }
    async fn container_stats(
        &self,
        request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .container_stats_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_container_stats
            .lock()
            .expect("last container stats lock") = Some(request);

        if id == "missing" {
            return Err(Status::not_found("container not found"));
        }

        Ok(Response::new(ContainerStatsResponse {
            stats: Some(test_container_stats(
                &id,
                "single-container",
                250_000_000,
                4096,
            )),
        }))
    }

    async fn list_container_stats(
        &self,
        request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        let request = request.into_inner();
        self.state
            .list_container_stats_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_list_container_stats
            .lock()
            .expect("last list container stats lock") = Some(request.clone());

        if request.filter.is_none() {
            return Ok(Response::new(ListContainerStatsResponse {
                stats: Vec::new(),
            }));
        }

        Ok(Response::new(ListContainerStatsResponse {
            stats: vec![test_container_stats(
                "ctr-stats-a",
                "container-a",
                500_000_000,
                8192,
            )],
        }))
    }
    async fn pod_sandbox_stats(
        &self,
        request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        let request = request.into_inner();
        let pod = request.pod_sandbox_id.clone();
        self.state.pod_stats_requests.fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_pod_stats
            .lock()
            .expect("last pod stats lock") = Some(request);

        if pod == "missing" {
            return Err(Status::not_found("pod not found"));
        }

        Ok(Response::new(PodSandboxStatsResponse {
            stats: Some(test_pod_stats(&pod, "pod-single")),
        }))
    }

    async fn list_pod_sandbox_stats(
        &self,
        request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        let request = request.into_inner();
        self.state
            .list_pod_stats_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_list_pod_stats
            .lock()
            .expect("last list pod stats lock") = Some(request);

        Ok(Response::new(ListPodSandboxStatsResponse {
            stats: vec![test_pod_stats("pod-stats-a", "pod-a")],
        }))
    }
    async fn update_runtime_config(
        &self,
        request: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        let request = request.into_inner();
        let pod_cidr = request
            .runtime_config
            .as_ref()
            .and_then(|config| config.network_config.as_ref())
            .map(|network| network.pod_cidr.as_str())
            .unwrap_or_default()
            .to_string();
        self.state
            .update_runtime_config_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_update_runtime_config
            .lock()
            .expect("last update runtime config lock") = Some(request);

        if pod_cidr == "10.250.0.0/16" {
            return Err(Status::failed_precondition(
                "pod CIDR overlaps node network",
            ));
        }

        Ok(Response::new(UpdateRuntimeConfigResponse {}))
    }
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let request = request.into_inner();
        self.state.status_requests.fetch_add(1, Ordering::SeqCst);

        Ok(Response::new(StatusResponse {
            status: Some(RuntimeStatus {
                conditions: vec![
                    RuntimeCondition {
                        r#type: "RuntimeReady".into(),
                        status: true,
                        reason: String::new(),
                        message: "runtime is ready".into(),
                    },
                    RuntimeCondition {
                        r#type: "NetworkReady".into(),
                        status: false,
                        reason: "CniNotReady".into(),
                        message: "network is not ready".into(),
                    },
                ],
            }),
            info: if request.verbose {
                [
                    (
                        "config".to_string(),
                        r#"{"cgroupDriver":"systemd"}"#.to_string(),
                    ),
                    ("bad".to_string(), "not-json".to_string()),
                ]
                .into()
            } else {
                Default::default()
            },
        }))
    }
    async fn checkpoint_container(
        &self,
        request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        let request = request.into_inner();
        let id = request.container_id.clone();
        self.state
            .checkpoint_container_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_checkpoint_container
            .lock()
            .expect("last checkpoint container lock") = Some(request);

        if id == "unsupported" {
            return Err(Status::failed_precondition("checkpoint is unsupported"));
        }

        Ok(Response::new(CheckpointContainerResponse {}))
    }

    type GetContainerEventsStream = ReceiverStream<Result<ContainerEventResponse, Status>>;

    async fn get_container_events(
        &self,
        _request: Request<GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        self.state
            .container_events_requests
            .fetch_add(1, Ordering::SeqCst);
        let mode = *self.state.event_mode.lock().expect("event mode lock");
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            match mode {
                MockEventMode::TwoThenEof => {
                    let _ = tx
                        .send(Ok(test_container_event(
                            "ctr-event-a",
                            "pod-event-a",
                            ContainerEventType::ContainerCreatedEvent,
                            1_000_000_000,
                        )))
                        .await;
                    let _ = tx
                        .send(Ok(test_container_event(
                            "ctr-event-b",
                            "pod-event-b",
                            ContainerEventType::ContainerStartedEvent,
                            2_000_000_000,
                        )))
                        .await;
                }
                MockEventMode::Pending => {
                    let _ = tx
                        .send(Ok(test_container_event(
                            "ctr-event-pending",
                            "pod-event-pending",
                            ContainerEventType::ContainerStartedEvent,
                            3_000_000_000,
                        )))
                        .await;
                    std::future::pending::<()>().await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_metric_descriptors(
        &self,
        _request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        self.state
            .metric_descriptors_requests
            .fetch_add(1, Ordering::SeqCst);

        Ok(Response::new(ListMetricDescriptorsResponse {
            descriptors: vec![MetricDescriptor {
                name: "container_cpu_usage_seconds_total".into(),
                help: "Cumulative container CPU usage".into(),
                label_keys: vec!["pod".into(), "container".into()],
            }],
        }))
    }
    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        self.state
            .list_pod_metrics_requests
            .fetch_add(1, Ordering::SeqCst);

        Ok(Response::new(ListPodSandboxMetricsResponse {
            pod_metrics: vec![test_pod_metrics()],
        }))
    }
    async fn runtime_config(
        &self,
        _request: Request<RuntimeConfigRequest>,
    ) -> Result<Response<RuntimeConfigResponse>, Status> {
        self.state
            .runtime_config_requests
            .fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RuntimeConfigResponse {
            linux: Some(LinuxRuntimeConfiguration {
                cgroup_driver: CgroupDriver::Systemd as i32,
            }),
        }))
    }
    async fn update_pod_sandbox_resources(
        &self,
        request: Request<UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<UpdatePodSandboxResourcesResponse>, Status> {
        let request = request.into_inner();
        let pod = request.pod_sandbox_id.clone();
        self.state
            .update_pod_resources_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_update_pod_resources
            .lock()
            .expect("last update pod resources lock") = Some(request);

        if pod == "missing" {
            return Err(Status::not_found("pod not found"));
        }
        if pod == "unsupported" {
            return Err(Status::failed_precondition(
                "pod resource update is unsupported",
            ));
        }

        Ok(Response::new(UpdatePodSandboxResourcesResponse {}))
    }
}

#[tonic::async_trait]
impl ImageService for MockImageService {
    async fn list_images(
        &self,
        request: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let request = request.into_inner();
        self.state
            .list_image_requests
            .fetch_add(1, Ordering::SeqCst);
        if let Some(filter) = request.filter {
            assert_eq!(filter.image.expect("image filter").image, "busybox:latest");
        }

        Ok(Response::new(ListImagesResponse {
            images: vec![test_image()],
        }))
    }

    async fn image_status(
        &self,
        request: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        let request = request.into_inner();
        self.state
            .image_inspect_requests
            .fetch_add(1, Ordering::SeqCst);
        let image = request.image.expect("image spec").image;
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("image-status:{image}"));

        if matches!(image.as_str(), "missing" | "ctr-only" | "pod-only" | "none")
            || !self
                .state
                .existing_images
                .lock()
                .expect("existing images lock")
                .contains(&image)
        {
            return Err(Status::not_found("image not found"));
        }

        Ok(Response::new(ImageStatusResponse {
            image: Some(test_image()),
            info: [("manifest".to_string(), r#"{"mediaType":"oci"}"#.to_string())].into(),
        }))
    }

    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let request = request.into_inner();
        let image = request
            .image
            .as_ref()
            .map(|image| image.image.as_str())
            .unwrap_or_default()
            .to_string();
        self.state
            .pull_image_requests
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .rpc_sequence
            .lock()
            .expect("rpc sequence lock")
            .push(format!("pull-image:{image}"));
        *self
            .state
            .last_pull_image
            .lock()
            .expect("last pull image lock") = Some(request);

        if image == "missing" {
            return Err(Status::not_found("image not found"));
        }
        if image == "denied" {
            return Err(Status::permission_denied("registry denied"));
        }

        Ok(Response::new(PullImageResponse {
            image_ref: format!("sha256:pulled-{image}"),
        }))
    }

    async fn remove_image(
        &self,
        request: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        let request = request.into_inner();
        let image = request
            .image
            .as_ref()
            .map(|image| image.image.as_str())
            .unwrap_or_default()
            .to_string();
        self.state
            .remove_image_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_remove_image
            .lock()
            .expect("last remove image lock") = Some(request);

        if image == "missing" {
            return Err(Status::not_found("image not found"));
        }
        if image == "used" {
            return Err(Status::failed_precondition("image is used by a container"));
        }

        Ok(Response::new(RemoveImageResponse {}))
    }

    async fn image_fs_info(
        &self,
        _request: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        self.state
            .image_fs_info_requests
            .fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(ImageFsInfoResponse {
            image_filesystems: vec![FilesystemUsage {
                timestamp: 99,
                fs_id: Some(FilesystemIdentifier {
                    mountpoint: "/var/lib/crius/images".into(),
                }),
                used_bytes: Some(UInt64Value {
                    value: 64 * 1024 * 1024,
                }),
                inodes_used: Some(UInt64Value { value: 12 }),
            }],
            container_filesystems: vec![],
        }))
    }
}

#[tonic::async_trait]
impl DiagnosticsService for MockDiagnosticsService {
    async fn server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        Ok(Response::new(ServerInfoResponse {
            version: "0.1.0".into(),
            git_commit: "test".into(),
            config_path: "/etc/crius/crius.conf".into(),
            state_dir: "/run/crius".into(),
            socket_path: "/run/crius/crius.sock".into(),
        }))
    }

    async fn effective_config(
        &self,
        request: Request<EffectiveConfigRequest>,
    ) -> Result<Response<EffectiveConfigResponse>, Status> {
        let request = request.into_inner();
        self.state
            .effective_config_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_effective_config
            .lock()
            .expect("last effective config lock") = Some(request);
        let metrics_enabled = *self
            .state
            .metrics_enabled
            .lock()
            .expect("metrics enabled lock");
        let metrics_endpoint = self
            .state
            .metrics_endpoint
            .lock()
            .expect("metrics endpoint lock")
            .clone()
            .unwrap_or_else(|| "127.0.0.1:9090".to_string());

        Ok(Response::new(EffectiveConfigResponse {
            config_json: serde_json::json!({
                "reload": {
                    "watcherStatus": "running",
                    "lastReloadAtUnixMillis": 1234,
                    "lastReloadError": null,
                    "lastCniWatchError": "template pending"
                },
                "image": {
                    "snapshotter": "internal-overlay-untar",
                    "signaturePolicy": "/etc/containers/policy.json",
                    "globalAuthFile": "/var/lib/crius/auth.json",
                    "password": "<redacted>",
                    "pinnedImages": ["registry.example/static:*"]
                },
                "streaming": {
                    "enabled": true,
                    "address": "127.0.0.1:10250",
                    "tls": {"enabled": false},
                    "tokenTTL": "10m",
                    "portForwardTimeout": "4h"
                },
                "metrics": {
                    "enabled": metrics_enabled,
                    "endpoint": metrics_endpoint,
                    "collector": "default",
                    "podMetrics": true
                },
                "tracing": {
                    "enabled": true,
                    "exporter": "otlp",
                    "endpoint": "http://collector:4317",
                    "samplingRate": "0.50",
                    "headers": {"authorization": "<redacted>"}
                }
            })
            .to_string(),
            redacted_fields: vec!["image.password".into()],
            warnings: vec![],
        }))
    }

    async fn runtime_handlers(
        &self,
        _request: Request<RuntimeHandlersRequest>,
    ) -> Result<Response<RuntimeHandlersResponse>, Status> {
        self.state
            .runtime_handlers_requests
            .fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RuntimeHandlersResponse {
            handlers: vec![RuntimeHandlerInfo {
                name: "runc".into(),
                runtime_type: "io.containerd.runc.v2".into(),
                runtime_path: "/usr/bin/runc".into(),
                runtime_config_path: "/etc/crius/runc.toml".into(),
                features: vec!["snapshotter=internal-overlay-untar".into()],
                warnings: vec![],
            }],
        }))
    }

    async fn image_transfers(
        &self,
        request: Request<ImageTransfersRequest>,
    ) -> Result<Response<ImageTransfersResponse>, Status> {
        let request = request.into_inner();
        self.state
            .image_transfers_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_image_transfers
            .lock()
            .expect("last image transfers lock") = Some(request);
        Ok(Response::new(ImageTransfersResponse {
            transfers: vec![
                ImageTransferInfo {
                    image: "busybox:latest".into(),
                    status: "running".into(),
                    updated_at_unix_nanos: 2_000_000_000,
                    error: String::new(),
                },
                ImageTransferInfo {
                    image: "done:latest".into(),
                    status: "succeeded".into(),
                    updated_at_unix_nanos: 1_000_000_000,
                    error: String::new(),
                },
            ],
        }))
    }

    async fn recovery_status(
        &self,
        _request: Request<RecoveryStatusRequest>,
    ) -> Result<Response<RecoveryStatusResponse>, Status> {
        self.state
            .recovery_status_requests
            .fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RecoveryStatusResponse {
            status: "degraded".into(),
            last_startup: "2026-06-06T01:02:03Z".into(),
            unhealthy_object_count: 2,
            ledger_summary_json: serde_json::json!({
                "pods": 1,
                "containers": 1
            })
            .to_string(),
            warnings: vec!["ledger contains stale objects".into()],
        }))
    }

    async fn recovery_check(
        &self,
        request: Request<RecoveryCheckRequest>,
    ) -> Result<Response<RecoveryCheckResponse>, Status> {
        let request = request.into_inner();
        self.state
            .recovery_check_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_recovery_check
            .lock()
            .expect("last recovery check lock") = Some(request.clone());
        Ok(Response::new(RecoveryCheckResponse {
            dry_run: !request.execute,
            actions: vec![
                RecoveryAction {
                    object_type: "container".into(),
                    object_id: "ctr-stale".into(),
                    action: "remove-stale-record".into(),
                    reason: "missing runtime task".into(),
                    executed: request.execute,
                    error: String::new(),
                },
                RecoveryAction {
                    object_type: "pod".into(),
                    object_id: "pod-broken".into(),
                    action: "repair-network-state".into(),
                    reason: "network checkpoint is incomplete".into(),
                    executed: request.execute,
                    error: if request.execute {
                        "network namespace is unavailable".into()
                    } else {
                        String::new()
                    },
                },
            ],
            warnings: vec![],
        }))
    }

    async fn nri_status(
        &self,
        _request: Request<NriStatusRequest>,
    ) -> Result<Response<NriStatusResponse>, Status> {
        Ok(Response::new(NriStatusResponse {
            enabled: false,
            cdi_enabled: true,
            cdi_spec_dirs: vec!["/etc/cdi".into()],
            plugin_path: "/opt/nri/plugins".into(),
            plugin_config_path: "/etc/nri/conf.d".into(),
            blockio_config_path: "/etc/crius/blockio.yaml".into(),
            blockio_supported: true,
            rdt_supported: false,
            warnings: vec![],
        }))
    }

    async fn security_status(
        &self,
        _request: Request<SecurityStatusRequest>,
    ) -> Result<Response<SecurityStatusResponse>, Status> {
        Ok(Response::new(SecurityStatusResponse {
            seccomp_available: true,
            seccomp_notifier_supported: true,
            apparmor_available: false,
            selinux_enabled: false,
            rootless_enabled: false,
            devices_policy_json: r#"{"defaultAction":"deny"}"#.into(),
            warnings: vec![],
        }))
    }

    async fn shim_status(
        &self,
        _request: Request<ShimStatusRequest>,
    ) -> Result<Response<ShimStatusResponse>, Status> {
        Ok(Response::new(ShimStatusResponse {
            shims: vec![crius::proto::diagnostics::v1::ShimInfo {
                container_id: "ctr1".into(),
                pid: 1234,
                task_socket: "/run/crius/shim/ctr1.sock".into(),
                attach_socket: "/run/crius/shim/ctr1.attach".into(),
                state: "running".into(),
                error: String::new(),
            }],
        }))
    }

    async fn content_gc(
        &self,
        request: Request<ContentGcRequest>,
    ) -> Result<Response<ContentGcResponse>, Status> {
        let request = request.into_inner();
        self.state
            .content_gc_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_content_gc
            .lock()
            .expect("last content gc lock") = Some(request.clone());
        Ok(Response::new(ContentGcResponse {
            dry_run: !request.execute,
            candidates: vec![
                ContentGcCandidate {
                    object_type: "blob".into(),
                    object_id: "sha256:unused".into(),
                    path: "/var/lib/crius/content/unused".into(),
                    size_bytes: 4096,
                    reason: "unreferenced".into(),
                    deleted: request.execute,
                    error: String::new(),
                },
                ContentGcCandidate {
                    object_type: "snapshot".into(),
                    object_id: "snap-failed".into(),
                    path: "/var/lib/crius/snapshots/snap-failed".into(),
                    size_bytes: 8192,
                    reason: "orphaned".into(),
                    deleted: false,
                    error: if request.execute {
                        "permission denied".into()
                    } else {
                        String::new()
                    },
                },
            ],
            reclaimed_bytes: if request.execute { 4096 } else { 0 },
            warnings: vec![],
        }))
    }

    type ContainerLogStream = ReceiverStream<Result<ContainerLogChunk, Status>>;

    async fn container_log(
        &self,
        request: Request<ContainerLogRequest>,
    ) -> Result<Response<Self::ContainerLogStream>, Status> {
        let request = request.into_inner();
        self.state
            .container_log_requests
            .fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_container_log
            .lock()
            .expect("last container log lock") = Some(request.clone());
        if request.container_id == "missing-log" {
            return Err(Status::not_found("container log not found"));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ContainerLogChunk {
                    data: b"hello\n".to_vec(),
                    stream: "stdout".into(),
                    timestamp_unix_nanos: 1_000_000_000,
                }))
                .await;
            let _ = tx
                .send(Ok(ContainerLogChunk {
                    data: b"warn\n".to_vec(),
                    stream: "stderr".into(),
                    timestamp_unix_nanos: 2_000_000_000,
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_command_reaches_mock_runtime_service() {
    let state = MockState::default();
    let version_requests = Arc::clone(&state.version_requests);
    let endpoint = spawn_mock_services(state).await;
    let output = run_crs(endpoint, ["version"]);

    assert!(
        output.status.success(),
        "crs exited with {:?}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(version_requests.load(Ordering::SeqCst), 1);

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("RUNTIME"));
    assert!(stdout.contains("crius-test"));
    assert!(stdout.contains("9.8.7"));
    assert!(stdout.contains("v1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_json_envelope_has_expected_kind() {
    let endpoint = spawn_mock_services(MockState::default()).await;
    let output = run_crs(endpoint, ["--output", "json", "version"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "RuntimeVersion");
    assert_eq!(value["items"][0]["runtimeName"], "crius-test");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_version_outputs_are_stable() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let table = run_crs(endpoint, ["version"]);
    assert_success(&table);
    assert_stdout(
        &table,
        "RUNTIME     VERSION  API VERSION\ncrius-test  9.8.7    v1\n",
    );

    let json = run_crs(endpoint, ["--output", "json", "version"]);
    assert_success(&json);
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "RuntimeVersion");
    assert_eq!(value["summary"]["count"], 1);
    assert_eq!(value["items"][0]["runtimeName"], "crius-test");
    assert_eq!(value["items"][0]["runtimeVersion"], "9.8.7");
    assert_eq!(value["items"][0]["runtimeApiVersion"], "v1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_table_and_verbose_json_report_conditions_and_warnings() {
    let state = MockState::default();
    let status_requests = Arc::clone(&state.status_requests);
    let endpoint = spawn_mock_services(state).await;

    let table = run_crs(endpoint, ["status"]);
    assert_success(&table);
    let stdout = String::from_utf8(table.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("RUNTIME READY"));
    assert!(stdout.contains("false"));

    let json = run_crs(endpoint, ["--output", "json", "status", "--verbose"]);
    assert_success(&json);
    assert!(json.stderr.is_empty());
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "RuntimeStatus");
    assert_eq!(value["items"][0]["runtimeReady"], true);
    assert_eq!(value["items"][0]["networkReady"], false);
    assert_eq!(
        value["items"][0]["infoJson"]["config"]["cgroupDriver"],
        "systemd"
    );
    assert!(value["warnings"][0]
        .as_str()
        .expect("warning")
        .contains("bad"));
    assert_eq!(status_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_status_outputs_are_stable() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let table = run_crs(endpoint, ["status"]);
    assert_success(&table);
    assert_stdout(
        &table,
        "RUNTIME READY  NETWORK READY  CONDITIONS\ntrue           false          2\n",
    );

    let json = run_crs(endpoint, ["--output", "json", "status", "--verbose"]);
    assert_success(&json);
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "RuntimeStatus");
    assert_eq!(value["summary"]["count"], 1);
    assert_eq!(value["items"][0]["runtimeReady"], true);
    assert_eq!(value["items"][0]["networkReady"], false);
    assert_eq!(value["items"][0]["conditions"][0]["kind"], "RuntimeReady");
    assert_eq!(value["items"][0]["conditions"][1]["kind"], "NetworkReady");
    assert_eq!(
        value["warnings"][0],
        "failed to parse verbose info field \"bad\" as JSON: expected ident at line 1 column 2"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_config_reports_cgroup_driver() {
    let state = MockState::default();
    let requests = Arc::clone(&state.runtime_config_requests);
    let endpoint = spawn_mock_services(state).await;
    let output = run_crs(endpoint, ["--output", "json", "runtime", "config"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "RuntimeConfig");
    assert_eq!(value["items"][0]["cgroupDriver"], "systemd");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_update_validates_cidr_and_calls_update_runtime_config() {
    let state = MockState::default();
    let requests = Arc::clone(&state.update_runtime_config_requests);
    let last_update = Arc::clone(&state.last_update_runtime_config);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "runtime",
            "update",
            "--pod-cidr",
            "10.244.0.0/16,fd00::/64",
        ],
    );

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "RuntimeConfigUpdate");
    assert_eq!(value["summary"]["updated"], true);
    assert_eq!(value["summary"]["podCidrs"][0], "10.244.0.0/16");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let request = last_update
        .lock()
        .expect("last update runtime config lock")
        .clone()
        .expect("update runtime config request");
    let pod_cidr = request
        .runtime_config
        .expect("runtime config")
        .network_config
        .expect("network config")
        .pod_cidr;
    assert_eq!(pod_cidr, "10.244.0.0/16,fd00::/64");

    let invalid = run_crs(endpoint, ["runtime", "update", "--pod-cidr", "not-a-cidr"]);
    assert_eq!(invalid.status.code(), Some(2));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_commands_use_diagnostics_effective_config_and_redact_sensitive_fields() {
    let state = MockState::default();
    let config_requests = Arc::clone(&state.effective_config_requests);
    let last_config = Arc::clone(&state.last_effective_config);
    let endpoint = spawn_mock_services(state).await;

    let show = run_crs(endpoint, ["--output", "json", "config", "show"]);
    assert_success(&show);
    let stdout = String::from_utf8(show.stdout).expect("stdout should be utf8");
    assert!(!stdout.contains("secret"));
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("stdout json");
    assert_eq!(value["kind"], "EffectiveConfig");
    assert_eq!(
        value["items"][0]["config"]["image"]["password"],
        "<redacted>"
    );
    assert_eq!(value["items"][0]["redactedFields"][0], "image.password");

    let request = last_config
        .lock()
        .expect("last effective config lock")
        .clone()
        .expect("effective config request should be recorded");
    assert!(!request.include_sensitive);

    let reload = run_crs(endpoint, ["--output", "json", "config", "reload-status"]);
    assert_success(&reload);
    let value = stdout_json(&reload);
    assert_eq!(value["kind"], "ConfigReloadStatus");
    assert_eq!(value["items"][0]["watcher"], "running");
    assert_eq!(value["items"][0]["cniWatcher"], "template pending");
    assert_eq!(config_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_handlers_uses_diagnostics_and_reports_kind() {
    let state = MockState::default();
    let requests = Arc::clone(&state.runtime_handlers_requests);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        ["--output", "json", "runtime", "handlers", "--verbose"],
    );

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "RuntimeHandlers");
    assert_eq!(value["items"][0]["name"], "runc");
    assert_eq!(
        value["items"][0]["features"][0],
        "snapshotter=internal-overlay-untar"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_read_only_commands_return_expected_kinds() {
    let state = MockState::default();
    let list_requests = Arc::clone(&state.list_image_requests);
    let inspect_requests = Arc::clone(&state.image_inspect_requests);
    let fs_requests = Arc::clone(&state.image_fs_info_requests);
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    let endpoint = spawn_mock_services(state).await;

    let list = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "image",
            "list",
            "--image",
            "busybox:latest",
        ],
    );
    assert_success(&list);
    let value = stdout_json(&list);
    assert_eq!(value["kind"], "ImageList");
    assert_eq!(value["items"][0]["image"], "busybox:latest");

    let inspect = run_crs(
        endpoint,
        ["--output", "json", "image", "inspect", "busybox:latest"],
    );
    assert_success(&inspect);
    let value = stdout_json(&inspect);
    assert_eq!(value["kind"], "ImageInspect");
    assert_eq!(
        value["items"][0]["infoJson"]["manifest"]["mediaType"],
        "oci"
    );

    let fs_info = run_crs(endpoint, ["--output", "json", "image", "fs-info"]);
    assert_success(&fs_info);
    let value = stdout_json(&fs_info);
    assert_eq!(value["kind"], "ImageFsInfo");
    assert_eq!(value["summary"]["totalUsedBytes"], 64 * 1024 * 1024);

    let shortcut = run_crs(endpoint, ["--output", "json", "images"]);
    assert_success(&shortcut);
    assert_eq!(stdout_json(&shortcut)["kind"], "ImageList");

    assert_eq!(list_requests.load(Ordering::SeqCst), 2);
    assert_eq!(inspect_requests.load(Ordering::SeqCst), 1);
    assert_eq!(fs_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_image_list_outputs_are_stable() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let table = run_crs(endpoint, ["image", "list"]);
    assert_success(&table);
    assert_stdout(
        &table,
        "IMAGE           IMAGE ID          SIZE    USER SPEC       PINNED\nbusybox:latest  sha256:testimage  1.0KiB  busybox:latest  false\n",
    );

    let json = run_crs(endpoint, ["--output", "json", "image", "list"]);
    assert_success(&json);
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "ImageList");
    assert_eq!(value["summary"]["count"], 1);
    assert_eq!(value["items"][0]["image"], "busybox:latest");
    assert_eq!(value["items"][0]["imageId"], "sha256:testimage");
    assert_eq!(value["items"][0]["sizeBytes"], 1024);
    assert_eq!(value["items"][0]["userSpec"], "busybox:latest");
    assert_eq!(value["items"][0]["pinned"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_diagnostics_commands_report_transfers_and_config_without_secrets() {
    let state = MockState::default();
    let transfer_requests = Arc::clone(&state.image_transfers_requests);
    let last_transfers = Arc::clone(&state.last_image_transfers);
    let endpoint = spawn_mock_services(state).await;

    let transfers = run_crs(endpoint, ["--output", "json", "image", "transfers"]);
    assert_success(&transfers);
    let value = stdout_json(&transfers);
    assert_eq!(value["kind"], "ImageTransfers");
    assert_eq!(value["summary"]["count"], 1);
    assert_eq!(value["items"][0]["image"], "busybox:latest");
    assert_eq!(transfer_requests.load(Ordering::SeqCst), 1);
    let request = last_transfers
        .lock()
        .expect("last image transfers lock")
        .clone()
        .expect("image transfers request should be recorded");
    assert!(!request.include_completed);

    let config = run_crs(endpoint, ["--output", "json", "image", "config"]);
    assert_success(&config);
    let stdout = String::from_utf8(config.stdout).expect("stdout should be utf8");
    assert!(!stdout.contains("secret"));
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("stdout json");
    assert_eq!(value["kind"], "ImageConfig");
    assert_eq!(value["items"][0]["snapshotter"], "internal-overlay-untar");
    assert_eq!(value["items"][0]["authConfigured"], "configured");
    assert_eq!(value["items"][0]["config"]["password"], "<redacted>");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_status_reports_unhealthy_summary_without_failing() {
    let state = MockState::default();
    let requests = Arc::clone(&state.recovery_status_requests);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "recovery", "status"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "RecoveryStatus");
    assert_eq!(value["summary"]["unhealthyObjectCount"], 2);
    assert_eq!(value["summary"]["ledgerSummary"]["pods"], 1);
    assert_eq!(value["warnings"][0], "ledger contains stale objects");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_check_and_repair_dry_run_do_not_execute_actions() {
    let state = MockState::default();
    let requests = Arc::clone(&state.recovery_check_requests);
    let last_check = Arc::clone(&state.last_recovery_check);
    let endpoint = spawn_mock_services(state).await;

    let check = run_crs(endpoint, ["--output", "json", "recovery", "check"]);
    assert_success(&check);
    let value = stdout_json(&check);
    assert_eq!(value["kind"], "RecoveryCheck");
    assert_eq!(value["summary"]["dryRun"], true);
    assert_eq!(value["summary"]["executed"], 0);
    let request = last_check
        .lock()
        .expect("last recovery check lock")
        .clone()
        .expect("recovery check request");
    assert!(!request.execute);

    let repair = run_crs(
        endpoint,
        ["--output", "json", "recovery", "repair", "--dry-run"],
    );
    assert_success(&repair);
    let value = stdout_json(&repair);
    assert_eq!(value["kind"], "RecoveryRepairPlan");
    assert_eq!(value["summary"]["dryRun"], true);
    assert_eq!(value["summary"]["executed"], 0);
    let request = last_check
        .lock()
        .expect("last recovery check lock")
        .clone()
        .expect("recovery repair request");
    assert!(!request.execute);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_repair_execute_returns_failure_for_item_errors() {
    let state = MockState::default();
    let last_check = Arc::clone(&state.last_recovery_check);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        ["--output", "json", "recovery", "repair", "--execute"],
    );

    assert_eq!(output.status.code(), Some(1));
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "RecoveryRepairPlan");
    assert_eq!(value["summary"]["dryRun"], false);
    assert_eq!(value["summary"]["executed"], 2);
    assert_eq!(value["summary"]["failed"], 1);
    let request = last_check
        .lock()
        .expect("last recovery check lock")
        .clone()
        .expect("recovery repair request");
    assert!(request.execute);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_candidates_and_dry_run_do_not_execute_deletions() {
    let state = MockState::default();
    let requests = Arc::clone(&state.content_gc_requests);
    let last_gc = Arc::clone(&state.last_content_gc);
    let endpoint = spawn_mock_services(state).await;

    let candidates = run_crs(endpoint, ["--output", "json", "gc", "candidates"]);
    assert_success(&candidates);
    let value = stdout_json(&candidates);
    assert_eq!(value["kind"], "GcCandidates");
    assert_eq!(value["summary"]["dryRun"], true);
    assert_eq!(value["summary"]["totalBytes"], 12_288);
    let request = last_gc
        .lock()
        .expect("last content gc lock")
        .clone()
        .expect("content gc request");
    assert!(!request.execute);

    let dry_run = run_crs(endpoint, ["--output", "json", "gc", "run", "--dry-run"]);
    assert_success(&dry_run);
    let value = stdout_json(&dry_run);
    assert_eq!(value["kind"], "GcPlan");
    assert_eq!(value["summary"]["dryRun"], true);
    assert_eq!(value["summary"]["deleted"], 0);
    let request = last_gc
        .lock()
        .expect("last content gc lock")
        .clone()
        .expect("content gc request");
    assert!(!request.execute);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_run_execute_returns_failure_for_item_errors() {
    let state = MockState::default();
    let last_gc = Arc::clone(&state.last_content_gc);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "gc", "run", "--execute"]);

    assert_eq!(output.status.code(), Some(1));
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "GcPlan");
    assert_eq!(value["summary"]["dryRun"], false);
    assert_eq!(value["summary"]["reclaimedBytes"], 4096);
    assert_eq!(value["summary"]["deleted"], 1);
    assert_eq!(value["summary"]["failed"], 1);
    let request = last_gc
        .lock()
        .expect("last content gc lock")
        .clone()
        .expect("content gc request");
    assert!(request.execute);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn debug_commands_emit_stable_json_kinds() {
    let endpoint = spawn_mock_services(MockState::default()).await;
    let cases = [
        (["debug", "network"].as_slice(), "DebugNetwork"),
        (["debug", "runtime"].as_slice(), "DebugRuntime"),
        (["debug", "shims"].as_slice(), "DebugShims"),
        (["debug", "nri"].as_slice(), "DebugNri"),
        (["debug", "security"].as_slice(), "DebugSecurity"),
        (["debug", "cgroups"].as_slice(), "DebugCgroups"),
        (["debug", "streaming"].as_slice(), "DebugStreaming"),
        (["debug", "metrics"].as_slice(), "DebugMetrics"),
        (["debug", "tracing"].as_slice(), "DebugTracing"),
        (["debug", "rootless"].as_slice(), "DebugRootless"),
    ];

    for (args, expected_kind) in cases {
        let output = run_crs(
            endpoint,
            std::iter::once("--output")
                .chain(std::iter::once("json"))
                .chain(args.iter().copied()),
        );
        assert_success(&output);
        let value = stdout_json(&output);
        assert_eq!(value["kind"], expected_kind);
        assert!(value["summary"].is_object());
        assert!(value["items"][0]["details"].is_object());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn debug_security_preserves_raw_devices_policy_on_parse_failure() {
    let endpoint = spawn_mock_services(MockState::default()).await;
    let output = run_crs(endpoint, ["--output", "json", "debug", "security"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "DebugSecurity");
    assert_eq!(value["summary"]["devicesPolicy"]["defaultAction"], "deny");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostics_unavailable_errors_and_fallbacks_are_stable() {
    let endpoint = spawn_mock_cri_services(MockState::default()).await;

    let recovery = run_crs(endpoint, ["--output", "json", "recovery", "status"]);
    assert_eq!(recovery.status.code(), Some(1));
    let stderr = String::from_utf8(recovery.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("diagnostics service is not available from this crius daemon"));

    let shims = run_crs(endpoint, ["--output", "json", "debug", "shims"]);
    assert_eq!(shims.status.code(), Some(1));
    let stderr = String::from_utf8(shims.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("diagnostics service is not available from this crius daemon"));

    let config = run_crs(endpoint, ["--output", "json", "config", "show"]);
    assert_success(&config);
    let value = stdout_json(&config);
    assert_eq!(value["kind"], "EffectiveConfig");
    assert_eq!(value["summary"]["source"], "status");
    assert!(value["warnings"][0]
        .as_str()
        .expect("warning")
        .contains("diagnostics service is not available from this crius daemon"));

    let handlers = run_crs(endpoint, ["--output", "json", "runtime", "handlers"]);
    assert_success(&handlers);
    let value = stdout_json(&handlers);
    assert_eq!(value["kind"], "RuntimeHandlers");
    assert!(value["warnings"][0]
        .as_str()
        .expect("warning")
        .contains("diagnostics service is not available from this crius daemon"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_pull_sends_auth_only_in_request_and_reports_ref() {
    let state = MockState::default();
    let pull_requests = Arc::clone(&state.pull_image_requests);
    let last_pull = Arc::clone(&state.last_pull_image);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "image",
            "pull",
            "--username",
            "user-a",
            "--password",
            "secret-password",
            "--registry-token",
            "secret-token",
            "registry.example/app:1",
        ],
    );

    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(!stdout.contains("secret-password"));
    assert!(!stdout.contains("secret-token"));
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("stdout json");
    assert_eq!(value["kind"], "ImagePull");
    assert_eq!(value["items"][0]["image"], "registry.example/app:1");
    assert_eq!(
        value["items"][0]["imageRef"],
        "sha256:pulled-registry.example/app:1"
    );
    assert_eq!(pull_requests.load(Ordering::SeqCst), 1);

    let request = last_pull
        .lock()
        .expect("last pull image lock")
        .clone()
        .expect("pull request should be recorded");
    assert_eq!(
        request.image.expect("image spec").image,
        "registry.example/app:1"
    );
    let auth = request.auth.expect("auth should be forwarded");
    assert_eq!(auth.username, "user-a");
    assert_eq!(auth.password, "secret-password");
    assert_eq!(auth.registry_token, "secret-token");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_pull_with_pod_checks_pod_before_pull() {
    let state = MockState::default();
    let pod_status_requests = Arc::clone(&state.pod_inspect_requests);
    let pull_requests = Arc::clone(&state.pull_image_requests);
    let last_pull = Arc::clone(&state.last_pull_image);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "image",
            "pull",
            "--pod",
            "pod123",
            "busybox:latest",
        ],
    );

    assert_success(&output);
    assert_eq!(pod_status_requests.load(Ordering::SeqCst), 1);
    assert_eq!(pull_requests.load(Ordering::SeqCst), 1);
    let request = last_pull
        .lock()
        .expect("last pull image lock")
        .clone()
        .expect("pull request should be recorded");
    let sandbox_metadata = request
        .sandbox_config
        .expect("sandbox config should be forwarded")
        .metadata
        .expect("sandbox metadata should be forwarded");
    assert_eq!(sandbox_metadata.name, "pod-a");
    assert_eq!(sandbox_metadata.namespace, "default");

    let missing = run_crs(
        endpoint,
        ["image", "pull", "--pod", "missing", "busybox:latest"],
    );
    assert_eq!(missing.status.code(), Some(4));
    assert_eq!(pull_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortcut_pull_reuses_image_pull_request_shape() {
    let state = MockState::default();
    let pull_requests = Arc::clone(&state.pull_image_requests);
    let last_pull = Arc::clone(&state.last_pull_image);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--quiet", "pull", "busybox:latest"]);

    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)
            .expect("stdout should be utf8")
            .trim_end(),
        "sha256:pulled-busybox:latest"
    );
    assert_eq!(pull_requests.load(Ordering::SeqCst), 1);
    let request = last_pull
        .lock()
        .expect("last pull image lock")
        .clone()
        .expect("pull request should be recorded");
    let image = request.image.expect("image spec");
    assert_eq!(image.image, "busybox:latest");
    assert_eq!(image.user_specified_image, "busybox:latest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_remove_reports_removed_summary_and_quiet_value() {
    let state = MockState::default();
    let remove_requests = Arc::clone(&state.remove_image_requests);
    let last_remove = Arc::clone(&state.last_remove_image);
    let endpoint = spawn_mock_services(state).await;

    let json = run_crs(
        endpoint,
        ["--output", "json", "image", "remove", "busybox:latest"],
    );
    assert_success(&json);
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "ImageRemove");
    assert_eq!(value["summary"]["removed"], true);
    assert_eq!(value["items"][0]["image"], "busybox:latest");

    let quiet = run_crs(endpoint, ["--quiet", "image", "remove", "busybox:latest"]);
    assert_success(&quiet);
    assert_eq!(
        String::from_utf8(quiet.stdout)
            .expect("stdout should be utf8")
            .trim_end(),
        "busybox:latest"
    );
    assert_eq!(remove_requests.load(Ordering::SeqCst), 2);

    let request = last_remove
        .lock()
        .expect("last remove image lock")
        .clone()
        .expect("remove request should be recorded");
    assert_eq!(request.image.expect("image spec").image, "busybox:latest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_write_errors_map_to_documented_exit_codes() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let not_found = run_crs(endpoint, ["image", "remove", "missing"]);
    assert_eq!(not_found.status.code(), Some(4));

    let precondition = run_crs(endpoint, ["image", "remove", "used"]);
    assert_eq!(precondition.status.code(), Some(6));
    let stderr = String::from_utf8(precondition.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("image is used by a container"));

    let denied = run_crs(endpoint, ["image", "pull", "denied"]);
    assert_eq!(denied.status.code(), Some(13));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_and_container_lists_and_shortcuts_use_running_filters() {
    let state = MockState::default();
    let pod_requests = Arc::clone(&state.list_pod_requests);
    let container_requests = Arc::clone(&state.list_container_requests);
    let endpoint = spawn_mock_services(state).await;

    let pods = run_crs(endpoint, ["--output", "json", "pod", "list"]);
    assert_success(&pods);
    assert_eq!(stdout_json(&pods)["kind"], "PodList");

    let pods_shortcut = run_crs(endpoint, ["--output", "json", "pods"]);
    assert_success(&pods_shortcut);
    assert_eq!(stdout_json(&pods_shortcut)["kind"], "PodList");

    let containers = run_crs(endpoint, ["--output", "json", "container", "list"]);
    assert_success(&containers);
    assert_eq!(stdout_json(&containers)["kind"], "ContainerList");

    let ps = run_crs(endpoint, ["--output", "json", "ps"]);
    assert_success(&ps);
    assert_eq!(stdout_json(&ps)["kind"], "ContainerList");

    assert_eq!(pod_requests.load(Ordering::SeqCst), 2);
    assert_eq!(container_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_pod_list_outputs_are_stable() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let table = run_crs(endpoint, ["pod", "list"]);
    assert_success(&table);
    assert_stdout(
        &table,
        "POD ID        NAME   NAMESPACE  STATE  IP  CREATED  ATTEMPT\npod123456789  pod-a  default    ready  -   42       1\n",
    );

    let json = run_crs(endpoint, ["--output", "json", "pod", "list"]);
    assert_success(&json);
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "PodList");
    assert_eq!(value["summary"]["count"], 1);
    assert_eq!(value["items"][0]["podId"], "pod123456789");
    assert_eq!(value["items"][0]["name"], "pod-a");
    assert_eq!(value["items"][0]["namespace"], "default");
    assert_eq!(value["items"][0]["state"], "ready");
    assert_eq!(value["items"][0]["attempt"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_container_list_outputs_are_stable() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let table = run_crs(endpoint, ["container", "list"]);
    assert_success(&table);
    assert_stdout(
        &table,
        "CONTAINER ID  POD           IMAGE           STATE    CREATED  NAME   ATTEMPT\nctr123456789  pod123456789  busybox:latest  running  42       ctr-a  2\n",
    );

    let json = run_crs(endpoint, ["--output", "json", "container", "list"]);
    assert_success(&json);
    let value = stdout_json(&json);
    assert_eq!(value["kind"], "ContainerList");
    assert_eq!(value["summary"]["count"], 1);
    assert_eq!(value["items"][0]["containerId"], "ctr123456789");
    assert_eq!(value["items"][0]["pod"], "pod123456789");
    assert_eq!(value["items"][0]["image"], "busybox:latest");
    assert_eq!(value["items"][0]["state"], "running");
    assert_eq!(value["items"][0]["name"], "ctr-a");
    assert_eq!(value["items"][0]["attempt"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_run_stop_remove_and_update_resources_call_runtime_rpcs() {
    let state = MockState::default();
    let run_requests = Arc::clone(&state.run_pod_requests);
    let stop_requests = Arc::clone(&state.stop_pod_requests);
    let remove_requests = Arc::clone(&state.remove_pod_requests);
    let update_requests = Arc::clone(&state.update_pod_resources_requests);
    let last_run = Arc::clone(&state.last_run_pod);
    let last_stop = Arc::clone(&state.last_stop_pod);
    let last_remove = Arc::clone(&state.last_remove_pod);
    let last_update = Arc::clone(&state.last_update_pod_resources);
    let endpoint = spawn_mock_services(state).await;

    let run = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "pod",
            "run",
            "--name",
            "demo",
            "--namespace",
            "ns-a",
            "--runtime-handler",
            "runc",
            "--label",
            "app=demo",
        ],
    );
    assert_success(&run);
    let value = stdout_json(&run);
    assert_eq!(value["kind"], "PodRun");
    assert_eq!(value["summary"]["podSandboxId"], "pod-demo");
    let request = last_run
        .lock()
        .expect("last run pod lock")
        .clone()
        .expect("run pod request");
    assert_eq!(request.runtime_handler, "runc");
    let config = request.config.expect("pod config");
    assert_eq!(config.metadata.expect("metadata").namespace, "ns-a");
    assert_eq!(config.labels["app"], "demo");
    assert_eq!(
        config.annotations.get("crius.crs/network-domain"),
        Some(&"local".to_string())
    );

    let stop = run_crs(endpoint, ["--output", "json", "pod", "stop", "pod-demo"]);
    assert_success(&stop);
    assert_eq!(stdout_json(&stop)["kind"], "PodStop");
    assert_eq!(
        last_stop
            .lock()
            .expect("last stop pod lock")
            .clone()
            .expect("stop pod request")
            .pod_sandbox_id,
        "pod-demo"
    );

    let remove = run_crs(endpoint, ["--quiet", "pod", "remove", "pod-demo"]);
    assert_success(&remove);
    assert_eq!(
        String::from_utf8(remove.stdout)
            .expect("stdout should be utf8")
            .trim_end(),
        "pod-demo"
    );
    assert_eq!(
        last_remove
            .lock()
            .expect("last remove pod lock")
            .clone()
            .expect("remove pod request")
            .pod_sandbox_id,
        "pod-demo"
    );

    let update = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "pod",
            "update-resources",
            "pod-demo",
            "--overhead",
            "memory=1KiB",
            "--pod-resource",
            "cpu-quota=2000,memory=2KiB",
        ],
    );
    assert_success(&update);
    assert_eq!(stdout_json(&update)["kind"], "PodResourceUpdate");
    let request = last_update
        .lock()
        .expect("last update pod resources lock")
        .clone()
        .expect("update pod resources request");
    assert_eq!(request.pod_sandbox_id, "pod-demo");
    assert_eq!(
        request.overhead.expect("overhead").memory_limit_in_bytes,
        1024
    );
    let resources = request.resources.expect("resources");
    assert_eq!(resources.cpu_quota, 2000);
    assert_eq!(resources.memory_limit_in_bytes, 2048);

    assert_eq!(run_requests.load(Ordering::SeqCst), 1);
    assert_eq!(stop_requests.load(Ordering::SeqCst), 1);
    assert_eq!(remove_requests.load(Ordering::SeqCst), 1);
    assert_eq!(update_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_pull_policy_controls_image_status_and_pull_order() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("present:latest".to_string());
    let image_status_requests = Arc::clone(&state.image_inspect_requests);
    let pull_requests = Arc::clone(&state.pull_image_requests);
    let sequence = Arc::clone(&state.rpc_sequence);
    let endpoint = spawn_mock_services(state).await;

    let present = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--pull",
            "missing",
            "present:latest",
            "true",
        ],
    );
    assert_success(&present);
    assert_eq!(stdout_json(&present)["kind"], "Run");
    assert_eq!(image_status_requests.load(Ordering::SeqCst), 1);
    assert_eq!(pull_requests.load(Ordering::SeqCst), 0);

    let absent = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--pull",
            "missing",
            "absent:latest",
            "true",
        ],
    );
    assert_success(&absent);
    assert_eq!(image_status_requests.load(Ordering::SeqCst), 2);
    assert_eq!(pull_requests.load(Ordering::SeqCst), 1);

    let always = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--pull",
            "always",
            "present:latest",
            "true",
        ],
    );
    assert_success(&always);
    assert_eq!(image_status_requests.load(Ordering::SeqCst), 2);
    assert_eq!(pull_requests.load(Ordering::SeqCst), 2);

    let never_missing = run_crs(
        endpoint,
        [
            "run",
            "--detach",
            "--pull",
            "never",
            "still-missing:latest",
            "true",
        ],
    );
    assert_eq!(never_missing.status.code(), Some(4));
    assert_eq!(image_status_requests.load(Ordering::SeqCst), 3);
    assert_eq!(pull_requests.load(Ordering::SeqCst), 2);

    let sequence = sequence.lock().expect("rpc sequence lock").clone();
    assert!(
        sequence
            .windows(2)
            .any(|pair| pair == ["image-status:absent:latest", "pull-image:absent:latest"]),
        "missing policy should inspect before pulling, got {sequence:?}"
    );
    assert!(
        !sequence
            .iter()
            .any(|entry| entry == "pull-image:still-missing:latest"),
        "never policy must not pull missing images, got {sequence:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_detach_creates_or_reuses_pod_and_starts_container() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    let run_pod_requests = Arc::clone(&state.run_pod_requests);
    let pod_status_requests = Arc::clone(&state.pod_inspect_requests);
    let create_container_requests = Arc::clone(&state.create_container_requests);
    let start_container_requests = Arc::clone(&state.start_container_requests);
    let last_run_pod = Arc::clone(&state.last_run_pod);
    let last_create_container = Arc::clone(&state.last_create_container);
    let last_start_container = Arc::clone(&state.last_start_container);
    let endpoint = spawn_mock_services(state).await;

    let created = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--name",
            "ctr-demo",
            "--namespace",
            "ns-a",
            "--pod-name",
            "pod-demo",
            "busybox:latest",
            "echo",
            "ok",
        ],
    );
    assert_success(&created);
    let value = stdout_json(&created);
    assert_eq!(value["kind"], "Run");
    assert_eq!(value["summary"]["podSandboxId"], "pod-pod-demo");
    assert_eq!(
        value["summary"]["containerId"],
        "ctr-pod-pod-demo-busybox:latest"
    );
    assert_eq!(value["summary"]["detached"], true);
    assert_eq!(value["summary"]["podCreated"], true);

    let pod_request = last_run_pod
        .lock()
        .expect("last run pod lock")
        .clone()
        .expect("run pod request");
    let pod_config = pod_request.config.expect("pod config");
    let pod_metadata = pod_config.metadata.expect("pod metadata");
    assert_eq!(pod_metadata.name, "pod-demo");
    assert_eq!(pod_metadata.namespace, "ns-a");
    assert_eq!(
        pod_config.annotations.get("crius.crs/internal-sandbox"),
        Some(&"true".to_string())
    );
    let namespace_options = pod_config
        .linux
        .as_ref()
        .and_then(|linux| linux.security_context.as_ref())
        .and_then(|security| security.namespace_options.as_ref())
        .expect("sandbox namespace options");
    assert_eq!(namespace_options.network, NamespaceMode::Node as i32);
    assert_eq!(namespace_options.pid, NamespaceMode::Node as i32);
    assert_eq!(namespace_options.ipc, NamespaceMode::Node as i32);

    let create_request = last_create_container
        .lock()
        .expect("last create container lock")
        .clone()
        .expect("create container request");
    assert_eq!(create_request.pod_sandbox_id, "pod-pod-demo");
    assert_eq!(
        create_request
            .sandbox_config
            .as_ref()
            .and_then(|config| config.metadata.as_ref())
            .map(|metadata| metadata.name.as_str()),
        Some("pod-demo")
    );
    let container_config = create_request.config.expect("container config");
    assert_eq!(
        container_config.metadata.expect("container metadata").name,
        "ctr-demo"
    );
    assert_eq!(container_config.command, vec!["echo", "ok"]);

    assert_eq!(
        last_start_container
            .lock()
            .expect("last start container lock")
            .clone()
            .expect("start container request")
            .container_id,
        "ctr-pod-pod-demo-busybox:latest"
    );

    let reused = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--pod",
            "pod-existing",
            "busybox:latest",
            "true",
        ],
    );
    assert_success(&reused);
    let value = stdout_json(&reused);
    assert_eq!(value["summary"]["podSandboxId"], "pod-existing");
    assert_eq!(value["summary"]["podCreated"], false);

    assert_eq!(run_pod_requests.load(Ordering::SeqCst), 1);
    assert_eq!(pod_status_requests.load(Ordering::SeqCst), 1);
    assert_eq!(create_container_requests.load(Ordering::SeqCst), 2);
    assert_eq!(start_container_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_run_uses_host_internal_sandbox_when_cri_cni_fails() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    *state
        .fail_non_host_pod_network
        .lock()
        .expect("fail non-host pod network lock") = true;
    let endpoint = spawn_mock_services(state).await;

    let default_run = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--pull",
            "never",
            "busybox:latest",
            "true",
        ],
    );
    assert_success(&default_run);
    assert_eq!(stdout_json(&default_run)["kind"], "Run");

    let explicit_pod_run = run_crs(
        endpoint,
        [
            "pod",
            "run",
            "--name",
            "local-pod",
            "--namespace",
            "default",
        ],
    );
    assert_eq!(explicit_pod_run.status.code(), Some(6));
    let stderr = String::from_utf8(explicit_pod_run.stderr).expect("stderr should be utf8");
    assert!(
        stderr.contains("Calico Kubernetes API is unavailable"),
        "unexpected stderr: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_exec_mode_sync_executes_command_after_start_and_returns_exit_code() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    let exec_sync_requests = Arc::clone(&state.exec_sync_requests);
    let last_exec_sync = Arc::clone(&state.last_exec_sync);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        ["run", "--exec-mode", "sync", "busybox:latest", "exit-7"],
    );

    assert_eq!(output.status.code(), Some(7));
    assert_eq!(String::from_utf8_lossy(&output.stdout), "sync-stdout");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "sync-stderr");
    assert_eq!(exec_sync_requests.load(Ordering::SeqCst), 1);
    let request = last_exec_sync
        .lock()
        .expect("last exec-sync lock")
        .clone()
        .expect("exec-sync request");
    assert!(request.container_id.starts_with("ctr-pod-crius-"));
    assert_eq!(request.cmd, vec!["exit-7"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_exec_mode_attach_streams_started_container() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    let attach_requests = Arc::clone(&state.attach_requests);
    let last_attach = Arc::clone(&state.last_attach);
    let websocket = spawn_mock_websocket().await;
    *state.attach_url.lock().expect("attach url lock") =
        Some(format!("ws://{websocket}/attach/token"));
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        ["run", "--exec-mode", "attach", "busybox:latest", "true"],
    );

    assert_success(&output);
    assert_eq!(attach_requests.load(Ordering::SeqCst), 1);
    let request = last_attach
        .lock()
        .expect("last attach lock")
        .clone()
        .expect("attach request");
    assert!(request.container_id.starts_with("ctr-pod-crius-"));
    assert!(!request.stdin);
    assert!(request.stdout);
    assert!(request.stderr);
    assert!(!request.tty);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_rm_cleans_created_resources_in_order() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    let sequence = Arc::clone(&state.rpc_sequence);
    let endpoint = spawn_mock_services(state).await;

    let created = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--rm",
            "--pod-name",
            "cleanup",
            "busybox:latest",
            "true",
        ],
    );
    assert_success(&created);

    let sequence = sequence.lock().expect("rpc sequence lock").clone();
    assert!(
        sequence.windows(4).any(|window| {
            window
                == [
                    "stop-container:ctr-pod-cleanup-busybox:latest",
                    "remove-container:ctr-pod-cleanup-busybox:latest",
                    "stop-pod:pod-cleanup",
                    "remove-pod:pod-cleanup",
                ]
        }),
        "temporary pod cleanup should stop/remove container and pod in order, got {sequence:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_rm_reused_pod_only_cleans_container() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("busybox:latest".to_string());
    let stop_pod_requests = Arc::clone(&state.stop_pod_requests);
    let remove_pod_requests = Arc::clone(&state.remove_pod_requests);
    let sequence = Arc::clone(&state.rpc_sequence);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "run",
            "--detach",
            "--rm",
            "--pod",
            "pod-existing",
            "busybox:latest",
            "true",
        ],
    );
    assert_success(&output);
    assert_eq!(stop_pod_requests.load(Ordering::SeqCst), 0);
    assert_eq!(remove_pod_requests.load(Ordering::SeqCst), 0);

    let sequence = sequence.lock().expect("rpc sequence lock").clone();
    assert!(
        sequence.windows(2).any(|window| {
            window
                == [
                    "stop-container:ctr-pod-existing-busybox:latest",
                    "remove-container:ctr-pod-existing-busybox:latest",
                ]
        }),
        "reused pod cleanup should only remove the created container, got {sequence:?}"
    );
    assert!(
        !sequence.iter().any(|entry| entry.starts_with("stop-pod:")),
        "reused pod cleanup must not stop pod, got {sequence:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_failure_warns_about_created_pod_leftover() {
    let state = MockState::default();
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("bad-config".to_string());
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        [
            "run",
            "--detach",
            "--pod-name",
            "leftover",
            "bad-config",
            "true",
        ],
    );

    assert_eq!(output.status.code(), Some(6));
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("container config rejected"));
    assert!(stderr.contains("run created pod pod-leftover before failing"));
    assert!(stderr.contains("crs pod remove pod-leftover"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_write_errors_map_to_documented_exit_codes() {
    let state = MockState::default();
    let update_requests = Arc::clone(&state.update_pod_resources_requests);
    let endpoint = spawn_mock_services(state).await;

    let missing_stop = run_crs(endpoint, ["pod", "stop", "missing"]);
    assert_eq!(missing_stop.status.code(), Some(4));

    let busy_remove = run_crs(endpoint, ["pod", "remove", "busy"]);
    assert_eq!(busy_remove.status.code(), Some(6));
    let stderr = String::from_utf8(busy_remove.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("pod still has containers"));

    let no_fields = run_crs(endpoint, ["pod", "update-resources", "pod-demo"]);
    assert_eq!(no_fields.status.code(), Some(2));
    assert_eq!(update_requests.load(Ordering::SeqCst), 0);

    let unsupported = run_crs(
        endpoint,
        [
            "pod",
            "update-resources",
            "unsupported",
            "--pod-resource",
            "memory=1KiB",
        ],
    );
    assert_eq!(unsupported.status.code(), Some(6));

    let runtime_reject = run_crs(
        endpoint,
        ["runtime", "update", "--pod-cidr", "10.250.0.0/16"],
    );
    assert_eq!(runtime_reject.status.code(), Some(6));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_write_commands_call_runtime_rpcs() {
    let state = MockState::default();
    let create_requests = Arc::clone(&state.create_container_requests);
    let start_requests = Arc::clone(&state.start_container_requests);
    let stop_requests = Arc::clone(&state.stop_container_requests);
    let remove_requests = Arc::clone(&state.remove_container_requests);
    let update_requests = Arc::clone(&state.update_container_resources_requests);
    let checkpoint_requests = Arc::clone(&state.checkpoint_container_requests);
    let reopen_requests = Arc::clone(&state.reopen_container_log_requests);
    let pod_status_requests = Arc::clone(&state.pod_inspect_requests);
    let last_create = Arc::clone(&state.last_create_container);
    let last_start = Arc::clone(&state.last_start_container);
    let last_stop = Arc::clone(&state.last_stop_container);
    let last_remove = Arc::clone(&state.last_remove_container);
    let last_update = Arc::clone(&state.last_update_container_resources);
    let last_checkpoint = Arc::clone(&state.last_checkpoint_container);
    let last_reopen = Arc::clone(&state.last_reopen_container_log);
    let endpoint = spawn_mock_services(state).await;

    let create = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "container",
            "create",
            "--name",
            "demo-ctr",
            "--env",
            "A=B",
            "--memory",
            "4KiB",
            "pod123",
            "busybox:latest",
            "--",
            "sh",
        ],
    );
    assert_success(&create);
    let value = stdout_json(&create);
    assert_eq!(value["kind"], "ContainerCreate");
    assert_eq!(value["summary"]["created"], true);
    assert_eq!(pod_status_requests.load(Ordering::SeqCst), 1);
    let request = last_create
        .lock()
        .expect("last create container lock")
        .clone()
        .expect("create container request");
    assert_eq!(request.pod_sandbox_id, "pod123");
    assert_eq!(
        request
            .sandbox_config
            .expect("sandbox config")
            .metadata
            .expect("sandbox metadata")
            .name,
        "pod-a"
    );
    let config = request.config.expect("container config");
    assert_eq!(
        config.metadata.expect("container metadata").name,
        "demo-ctr"
    );
    assert_eq!(config.command, ["sh"]);
    assert_eq!(
        config
            .linux
            .expect("linux config")
            .resources
            .expect("resources")
            .memory_limit_in_bytes,
        4096
    );

    let start = run_crs(endpoint, ["--output", "json", "container", "start", "ctr1"]);
    assert_success(&start);
    assert_eq!(stdout_json(&start)["kind"], "ContainerStart");
    assert_eq!(
        last_start
            .lock()
            .expect("last start container lock")
            .clone()
            .expect("start container request")
            .container_id,
        "ctr1"
    );

    let stop = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "container",
            "stop",
            "--timeout",
            "7",
            "ctr1",
        ],
    );
    assert_success(&stop);
    assert_eq!(stdout_json(&stop)["summary"]["timeoutSeconds"], 7);
    let stop_request = last_stop
        .lock()
        .expect("last stop container lock")
        .clone()
        .expect("stop container request");
    assert_eq!(stop_request.container_id, "ctr1");
    assert_eq!(stop_request.timeout, 7);

    let remove = run_crs(endpoint, ["--quiet", "container", "remove", "ctr1"]);
    assert_success(&remove);
    assert_eq!(
        String::from_utf8(remove.stdout)
            .expect("stdout should be utf8")
            .trim_end(),
        "ctr1"
    );
    assert_eq!(
        last_remove
            .lock()
            .expect("last remove container lock")
            .clone()
            .expect("remove container request")
            .container_id,
        "ctr1"
    );

    let update = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "container",
            "update",
            "ctr1",
            "--resource",
            "cpu-quota=3000,memory=8KiB",
            "--annotation",
            "resize=true",
        ],
    );
    assert_success(&update);
    assert_eq!(stdout_json(&update)["kind"], "ContainerUpdate");
    let update_request = last_update
        .lock()
        .expect("last update container resources lock")
        .clone()
        .expect("update container request");
    assert_eq!(update_request.container_id, "ctr1");
    assert_eq!(update_request.annotations["resize"], "true");
    let resources = update_request.linux.expect("linux resources");
    assert_eq!(resources.cpu_quota, 3000);
    assert_eq!(resources.memory_limit_in_bytes, 8192);

    let checkpoint = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "container",
            "checkpoint",
            "--location",
            "/tmp/ctr1.checkpoint",
            "--timeout",
            "5",
            "ctr1",
        ],
    );
    assert_success(&checkpoint);
    assert_eq!(stdout_json(&checkpoint)["kind"], "ContainerCheckpoint");
    let checkpoint_request = last_checkpoint
        .lock()
        .expect("last checkpoint container lock")
        .clone()
        .expect("checkpoint container request");
    assert_eq!(checkpoint_request.container_id, "ctr1");
    assert_eq!(checkpoint_request.location, "/tmp/ctr1.checkpoint");
    assert_eq!(checkpoint_request.timeout, 5);

    let reopen = run_crs(
        endpoint,
        ["--output", "json", "container", "reopen-log", "ctr1"],
    );
    assert_success(&reopen);
    assert_eq!(stdout_json(&reopen)["kind"], "ContainerReopenLog");
    assert_eq!(
        last_reopen
            .lock()
            .expect("last reopen container log lock")
            .clone()
            .expect("reopen container log request")
            .container_id,
        "ctr1"
    );

    assert_eq!(create_requests.load(Ordering::SeqCst), 1);
    assert_eq!(start_requests.load(Ordering::SeqCst), 1);
    assert_eq!(stop_requests.load(Ordering::SeqCst), 1);
    assert_eq!(remove_requests.load(Ordering::SeqCst), 1);
    assert_eq!(update_requests.load(Ordering::SeqCst), 1);
    assert_eq!(checkpoint_requests.load(Ordering::SeqCst), 1);
    assert_eq!(reopen_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_exec_calls_runtime_exec_and_streams_websocket() {
    let state = MockState::default();
    let exec_requests = Arc::clone(&state.exec_requests);
    let last_exec = Arc::clone(&state.last_exec);
    let websocket = spawn_mock_websocket().await;
    *state.exec_url.lock().expect("exec url lock") = Some(format!("ws://{websocket}/exec/token"));
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["container", "exec", "ctr1", "--", "echo", "ok"]);

    assert_success(&output);
    assert_eq!(exec_requests.load(Ordering::SeqCst), 1);
    let request = last_exec
        .lock()
        .expect("last exec lock")
        .clone()
        .expect("exec request");
    assert_eq!(request.container_id, "ctr1");
    assert_eq!(request.cmd, vec!["echo".to_string(), "ok".to_string()]);
    assert!(request.stdout);
    assert!(request.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn top_level_exec_reuses_container_exec() {
    let state = MockState::default();
    let exec_requests = Arc::clone(&state.exec_requests);
    let websocket = spawn_mock_websocket().await;
    *state.exec_url.lock().expect("exec url lock") = Some(format!("ws://{websocket}/exec/token"));
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["exec", "ctr1", "--", "true"]);

    assert_success(&output);
    assert_eq!(exec_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_logs_streams_text_without_json_envelope() {
    let state = MockState::default();
    let log_requests = Arc::clone(&state.container_log_requests);
    let last_log = Arc::clone(&state.last_container_log);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(
        endpoint,
        [
            "--output",
            "text",
            "container",
            "logs",
            "--follow",
            "--tail",
            "10",
            "--since",
            "2026-06-03T12:00:00Z",
            "--timestamps",
            "ctr1",
        ],
    );

    assert_success(&output);
    assert_eq!(String::from_utf8_lossy(&output.stdout), "hello\nwarn\n");
    assert!(output.stderr.is_empty());
    assert_eq!(log_requests.load(Ordering::SeqCst), 1);
    let request = last_log
        .lock()
        .expect("last container log lock")
        .clone()
        .expect("container log request");
    assert_eq!(request.container_id, "ctr1");
    assert!(request.follow);
    assert_eq!(request.tail_lines, 10);
    assert!(request.since_unix_nanos > 0);
    assert!(request.timestamps);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_logs_json_outputs_parseable_ndjson() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let output = run_crs(endpoint, ["--output", "json", "container", "logs", "ctr1"]);

    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let first: serde_json::Value = serde_json::from_str(lines[0]).expect("first log json");
    let second: serde_json::Value = serde_json::from_str(lines[1]).expect("second log json");
    assert_eq!(first["data"], "hello\n");
    assert_eq!(first["stream"], "stdout");
    assert_eq!(first["timestamp"], 1_000_000_000);
    assert_eq!(second["stream"], "stderr");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn top_level_logs_reuses_container_logs_request_shape() {
    let state = MockState::default();
    let log_requests = Arc::clone(&state.container_log_requests);
    let last_log = Arc::clone(&state.last_container_log);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["logs", "--tail", "5", "ctr1"]);

    assert_success(&output);
    assert_eq!(log_requests.load(Ordering::SeqCst), 1);
    let request = last_log
        .lock()
        .expect("last container log lock")
        .clone()
        .expect("container log request");
    assert_eq!(request.container_id, "ctr1");
    assert_eq!(request.tail_lines, 5);
    assert!(!request.follow);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_exec_sync_writes_streams_and_returns_exit_code() {
    let state = MockState::default();
    let exec_sync_requests = Arc::clone(&state.exec_sync_requests);
    let last_exec_sync = Arc::clone(&state.last_exec_sync);
    let endpoint = spawn_mock_services(state).await;

    let success = run_crs(endpoint, ["container", "exec-sync", "ctr1", "--", "true"]);
    assert_success(&success);
    assert_eq!(success.stdout, b"sync-stdout");
    assert_eq!(success.stderr, b"sync-stderr");

    let failed = run_crs(
        endpoint,
        ["container", "exec-sync", "ctr1", "--", "exit-127"],
    );
    assert_eq!(failed.status.code(), Some(127));
    assert_eq!(exec_sync_requests.load(Ordering::SeqCst), 2);
    let request = last_exec_sync
        .lock()
        .expect("last exec-sync lock")
        .clone()
        .expect("exec-sync request");
    assert_eq!(request.container_id, "ctr1");
    assert_eq!(request.cmd, vec!["exit-127".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_exec_sync_json_keeps_stderr_in_envelope() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let output = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "container",
            "exec-sync",
            "ctr1",
            "--",
            "exit-1",
        ],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        output.stderr.is_empty(),
        "JSON exec-sync stderr should be in envelope, got {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "ContainerExecSync");
    assert_eq!(value["summary"]["exitCode"], 1);
    assert_eq!(value["stdout"], "sync-stdout");
    assert_eq!(value["stderr"], "sync-stderr");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_attach_calls_runtime_attach_and_streams_websocket() {
    let state = MockState::default();
    let attach_requests = Arc::clone(&state.attach_requests);
    let last_attach = Arc::clone(&state.last_attach);
    let websocket = spawn_mock_websocket().await;
    *state.attach_url.lock().expect("attach url lock") =
        Some(format!("ws://{websocket}/attach/token"));
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["container", "attach", "ctr1"]);

    assert_success(&output);
    assert_eq!(attach_requests.load(Ordering::SeqCst), 1);
    let request = last_attach
        .lock()
        .expect("last attach lock")
        .clone()
        .expect("attach request");
    assert_eq!(request.container_id, "ctr1");
    assert!(request.stdout);
    assert!(request.stderr);
    assert!(!request.stdin);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_port_forward_calls_runtime_and_binds_local_port() {
    let state = MockState::default();
    let port_forward_requests = Arc::clone(&state.port_forward_requests);
    let last_port_forward = Arc::clone(&state.last_port_forward);
    *state
        .port_forward_url
        .lock()
        .expect("port-forward url lock") = Some("ws://127.0.0.1:9/portforward/token".to_string());
    let endpoint = spawn_mock_services(state).await;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe listener should bind");
    let local_port = probe.local_addr().expect("probe addr").port();
    drop(probe);

    let output = run_crs(
        endpoint,
        [
            "pod",
            "port-forward",
            "pod1",
            "--forward",
            &format!("{local_port}:80"),
        ],
    );

    assert_success(&output);
    assert_eq!(port_forward_requests.load(Ordering::SeqCst), 1);
    let request = last_port_forward
        .lock()
        .expect("last port-forward lock")
        .clone()
        .expect("port-forward request");
    assert_eq!(request.pod_sandbox_id, "pod1");
    assert_eq!(request.port, vec![80]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_port_forward_errors_map_to_documented_codes() {
    let state = MockState::default();
    *state
        .port_forward_url
        .lock()
        .expect("port-forward url lock") = Some("ws://127.0.0.1:9/portforward/token".to_string());
    let endpoint = spawn_mock_services(state).await;

    let missing = run_crs(
        endpoint,
        ["pod", "port-forward", "missing", "--forward", "18080:80"],
    );
    assert_eq!(missing.status.code(), Some(4));

    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("occupied listener should bind");
    let occupied_port = occupied.local_addr().expect("occupied addr").port();
    let conflict = run_crs(
        endpoint,
        [
            "pod",
            "port-forward",
            "pod1",
            "--forward",
            &format!("{occupied_port}:80"),
        ],
    );
    assert_eq!(conflict.status.code(), Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_write_errors_map_to_documented_exit_codes() {
    let state = MockState::default();
    let create_requests = Arc::clone(&state.create_container_requests);
    let update_requests = Arc::clone(&state.update_container_resources_requests);
    let endpoint = spawn_mock_services(state).await;

    let missing_pod = run_crs(
        endpoint,
        ["container", "create", "missing", "busybox:latest"],
    );
    assert_eq!(missing_pod.status.code(), Some(4));
    assert_eq!(create_requests.load(Ordering::SeqCst), 0);

    let already_running = run_crs(endpoint, ["container", "start", "running"]);
    assert_eq!(already_running.status.code(), Some(6));

    let missing_stop = run_crs(endpoint, ["container", "stop", "missing"]);
    assert_eq!(missing_stop.status.code(), Some(4));

    let running_remove = run_crs(endpoint, ["container", "remove", "running"]);
    assert_eq!(running_remove.status.code(), Some(6));
    let stderr = String::from_utf8(running_remove.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("container is running"));

    let no_update = run_crs(endpoint, ["container", "update", "ctr1"]);
    assert_eq!(no_update.status.code(), Some(2));
    assert_eq!(update_requests.load(Ordering::SeqCst), 0);

    let unsupported_update = run_crs(
        endpoint,
        [
            "container",
            "update",
            "unsupported",
            "--resource",
            "memory=1KiB",
        ],
    );
    assert_eq!(unsupported_update.status.code(), Some(6));

    let unsupported_checkpoint = run_crs(
        endpoint,
        [
            "container",
            "checkpoint",
            "unsupported",
            "--location",
            "/tmp/checkpoint",
        ],
    );
    assert_eq!(unsupported_checkpoint.status.code(), Some(6));

    let missing_log = run_crs(endpoint, ["container", "reopen-log", "missing-log"]);
    assert_eq!(missing_log.status.code(), Some(6));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortcut_stop_resolves_unique_container_or_pod_target() {
    let state = MockState::default();
    let stop_container_requests = Arc::clone(&state.stop_container_requests);
    let stop_pod_requests = Arc::clone(&state.stop_pod_requests);
    let endpoint = spawn_mock_services(state).await;

    let container_stop = run_crs(endpoint, ["--output", "json", "stop", "ctr-only"]);
    assert_success(&container_stop);
    assert_eq!(stdout_json(&container_stop)["kind"], "ContainerStop");

    let pod_stop = run_crs(endpoint, ["--output", "json", "stop", "pod-only"]);
    assert_success(&pod_stop);
    assert_eq!(stdout_json(&pod_stop)["kind"], "PodStop");

    let internal_pod_stop = run_crs(endpoint, ["--output", "json", "stop", "internal-pod"]);
    assert_success(&internal_pod_stop);
    assert_eq!(
        stdout_json(&internal_pod_stop)["kind"],
        "ContainerStop",
        "internal run sandbox must not be treated as a user pod candidate"
    );

    assert_eq!(stop_container_requests.load(Ordering::SeqCst), 2);
    assert_eq!(stop_pod_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortcut_rm_resolves_unique_container_pod_or_image_target() {
    let state = MockState::default();
    let remove_container_requests = Arc::clone(&state.remove_container_requests);
    let remove_pod_requests = Arc::clone(&state.remove_pod_requests);
    let remove_image_requests = Arc::clone(&state.remove_image_requests);
    state
        .existing_images
        .lock()
        .expect("existing images lock")
        .insert("image-only".to_string());
    let endpoint = spawn_mock_services(state).await;

    let container_rm = run_crs(endpoint, ["--output", "json", "rm", "ctr-only"]);
    assert_success(&container_rm);
    assert_eq!(stdout_json(&container_rm)["kind"], "ContainerRemove");

    let pod_rm = run_crs(endpoint, ["--output", "json", "rm", "pod-only"]);
    assert_success(&pod_rm);
    assert_eq!(stdout_json(&pod_rm)["kind"], "PodRemove");

    let image_rm = run_crs(endpoint, ["--output", "json", "rm", "image-only"]);
    assert_success(&image_rm);
    assert_eq!(stdout_json(&image_rm)["kind"], "ImageRemove");

    let internal_pod_rm = run_crs(endpoint, ["--output", "json", "rm", "internal-pod"]);
    assert_success(&internal_pod_rm);
    assert_eq!(
        stdout_json(&internal_pod_rm)["kind"],
        "ContainerRemove",
        "internal run sandbox must not be treated as a user pod candidate"
    );

    assert_eq!(remove_container_requests.load(Ordering::SeqCst), 2);
    assert_eq!(remove_pod_requests.load(Ordering::SeqCst), 1);
    assert_eq!(remove_image_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortcut_stop_and_rm_require_type_for_ambiguous_targets() {
    let state = MockState::default();
    let stop_container_requests = Arc::clone(&state.stop_container_requests);
    let remove_container_requests = Arc::clone(&state.remove_container_requests);
    let endpoint = spawn_mock_services(state).await;

    let stop = run_crs(endpoint, ["stop", "ambiguous"]);
    assert_eq!(stop.status.code(), Some(2));
    let stderr = String::from_utf8(stop.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--type container|pod"));
    assert_eq!(stop_container_requests.load(Ordering::SeqCst), 0);

    let rm = run_crs(endpoint, ["rm", "ambiguous"]);
    assert_eq!(rm.status.code(), Some(2));
    let stderr = String::from_utf8(rm.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--type container|pod|image"));
    assert_eq!(remove_container_requests.load(Ordering::SeqCst), 0);

    let no_match = run_crs(endpoint, ["rm", "none"]);
    assert_eq!(no_match.status.code(), Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortcut_stop_and_rm_honor_explicit_type() {
    let state = MockState::default();
    let stop_pod_requests = Arc::clone(&state.stop_pod_requests);
    let remove_image_requests = Arc::clone(&state.remove_image_requests);
    let endpoint = spawn_mock_services(state).await;

    let stop = run_crs(endpoint, ["stop", "--type", "pod", "ambiguous"]);
    assert_success(&stop);
    assert_eq!(stop_pod_requests.load(Ordering::SeqCst), 1);

    let rm = run_crs(endpoint, ["rm", "--type", "image", "ambiguous"]);
    assert_success(&rm);
    assert_eq!(remove_image_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_and_container_inspect_parse_verbose_info() {
    let state = MockState::default();
    let pod_requests = Arc::clone(&state.pod_inspect_requests);
    let container_requests = Arc::clone(&state.container_inspect_requests);
    let endpoint = spawn_mock_services(state).await;

    let pod = run_crs(endpoint, ["--output", "json", "pod", "inspect", "pod123"]);
    assert_success(&pod);
    let value = stdout_json(&pod);
    assert_eq!(value["kind"], "PodInspect");
    assert_eq!(
        value["items"][0]["infoJson"]["network"]["namespace"],
        "test"
    );

    let container = run_crs(
        endpoint,
        ["--output", "json", "container", "inspect", "ctr123"],
    );
    assert_success(&container);
    let value = stdout_json(&container);
    assert_eq!(value["kind"], "ContainerInspect");
    assert_eq!(value["items"][0]["infoJson"]["pid"], 1234);

    assert_eq!(pod_requests.load(Ordering::SeqCst), 1);
    assert_eq!(container_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_not_found_maps_to_exit_code_four() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    for args in [
        vec!["image", "inspect", "missing"],
        vec!["pod", "inspect", "missing"],
        vec!["container", "inspect", "missing"],
    ] {
        let output = run_crs(endpoint, args);
        assert_eq!(output.status.code(), Some(4));
        let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
        assert!(stderr.contains("not found"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doctor_uses_cri_and_warns_when_diagnostics_is_unavailable() {
    let endpoint = spawn_mock_services(MockState::default()).await;
    let output = run_crs(endpoint, ["doctor"]);

    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stdout.contains("runtime"));
    assert!(stdout.contains("diagnostics"));
    assert!(stderr.contains("diagnostics service is not available"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_table_prints_single_header_and_exits_on_eof() {
    let state = MockState::default();
    let event_requests = Arc::clone(&state.container_events_requests);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["events"]);

    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert_eq!(stdout.matches("TIME").count(), 1);
    assert!(stdout.contains("TYPE"));
    assert!(stdout.contains("created"));
    assert!(stdout.contains("started"));
    assert!(stdout.contains("ctr-event-a"));
    assert!(stdout.contains("pod-event-b"));
    assert_eq!(event_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_json_outputs_parseable_ndjson_items() {
    let endpoint = spawn_mock_services(MockState::default()).await;

    let output = run_crs(endpoint, ["--output", "json", "events"]);

    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("event JSON line");
        assert!(value["createdAtUnixNanos"].as_i64().expect("event time") > 0);
        assert!(value["eventType"].as_str().expect("event type").len() > 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_sigint_returns_interrupted_exit_code() {
    let state = MockState::default();
    *state.event_mode.lock().expect("event mode lock") = MockEventMode::Pending;
    let endpoint = spawn_mock_services(state).await;

    let mut child = Command::new(env!("CARGO_BIN_EXE_crs"))
        .args([
            "--address",
            &format!("http://{endpoint}"),
            "--connect-timeout",
            "2s",
            "--timeout",
            "2s",
            "events",
        ])
        .spawn()
        .expect("crs process should start");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    unsafe {
        libc::kill(child.id() as i32, libc::SIGINT);
    }
    let status = child.wait().expect("crs process should exit");

    assert_eq!(status.code(), Some(130));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn top_level_stats_empty_list_returns_successful_summary() {
    let state = MockState::default();
    let requests = Arc::clone(&state.list_container_stats_requests);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "stats"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "ContainerStats");
    assert_eq!(value["items"].as_array().expect("items").len(), 0);
    assert_eq!(value["summary"]["count"], 0);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_stats_single_and_list_paths_share_view_shape() {
    let state = MockState::default();
    let single_requests = Arc::clone(&state.container_stats_requests);
    let list_requests = Arc::clone(&state.list_container_stats_requests);
    let last_single = Arc::clone(&state.last_container_stats);
    let last_list = Arc::clone(&state.last_list_container_stats);
    let endpoint = spawn_mock_services(state).await;

    let single = run_crs(
        endpoint,
        ["--output", "json", "container", "stats", "ctr123"],
    );
    assert_success(&single);
    let value = stdout_json(&single);
    assert_eq!(value["kind"], "ContainerStats");
    assert_eq!(value["items"][0]["id"], "ctr123");
    assert_eq!(value["items"][0]["name"], "single-container");
    assert_eq!(value["items"][0]["cpuNanoCores"], 250_000_000);
    assert_eq!(
        last_single
            .lock()
            .expect("last container stats lock")
            .clone()
            .expect("container stats request")
            .container_id,
        "ctr123"
    );

    let listed = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "container",
            "stats",
            "--pod",
            "pod123",
            "--label",
            "app=demo",
        ],
    );
    assert_success(&listed);
    let value = stdout_json(&listed);
    assert_eq!(value["kind"], "ContainerStats");
    assert_eq!(value["items"][0]["id"], "ctr-stats-a");
    assert_eq!(value["items"][0]["memoryBytes"], 8192);
    let request = last_list
        .lock()
        .expect("last list container stats lock")
        .clone()
        .expect("list container stats request");
    let filter = request.filter.expect("filter");
    assert_eq!(filter.pod_sandbox_id, "pod123");
    assert_eq!(filter.label_selector["app"], "demo");

    assert_eq!(single_requests.load(Ordering::SeqCst), 1);
    assert_eq!(list_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_stats_single_and_list_paths_report_resource_columns() {
    let state = MockState::default();
    let single_requests = Arc::clone(&state.pod_stats_requests);
    let list_requests = Arc::clone(&state.list_pod_stats_requests);
    let last_single = Arc::clone(&state.last_pod_stats);
    let last_list = Arc::clone(&state.last_list_pod_stats);
    let endpoint = spawn_mock_services(state).await;

    let single = run_crs(endpoint, ["--output", "json", "pod", "stats", "pod123"]);
    assert_success(&single);
    let value = stdout_json(&single);
    assert_eq!(value["kind"], "PodStats");
    assert_eq!(value["items"][0]["id"], "pod123");
    assert_eq!(value["items"][0]["cpuNanoCores"], 750_000_000);
    assert_eq!(value["items"][0]["memoryBytes"], 16_384);
    assert_eq!(value["items"][0]["networkBytes"], 1536);
    assert_eq!(value["items"][0]["pids"], 12);
    assert_eq!(
        last_single
            .lock()
            .expect("last pod stats lock")
            .clone()
            .expect("pod stats request")
            .pod_sandbox_id,
        "pod123"
    );

    let listed = run_crs(
        endpoint,
        [
            "--output",
            "json",
            "pod",
            "stats",
            "--label",
            "tier=frontend",
        ],
    );
    assert_success(&listed);
    let value = stdout_json(&listed);
    assert_eq!(value["kind"], "PodStats");
    assert_eq!(value["items"][0]["id"], "pod-stats-a");
    let request = last_list
        .lock()
        .expect("last list pod stats lock")
        .clone()
        .expect("list pod stats request");
    let filter = request.filter.expect("pod stats filter");
    assert_eq!(filter.label_selector["tier"], "frontend");

    assert_eq!(single_requests.load(Ordering::SeqCst), 1);
    assert_eq!(list_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_metrics_reports_items_and_count_summary() {
    let state = MockState::default();
    let requests = Arc::clone(&state.list_pod_metrics_requests);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "pod", "metrics"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "PodMetrics");
    assert_eq!(value["items"][0]["podId"], "pod-metrics-a");
    assert_eq!(value["items"][0]["podMetricCount"], 1);
    assert_eq!(value["items"][0]["containerMetricCount"], 2);
    assert_eq!(value["summary"]["count"], 3);
    assert_eq!(value["summary"]["totalMetrics"], 3);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_descriptors_reports_cri_descriptor_fields() {
    let state = MockState::default();
    let requests = Arc::clone(&state.metric_descriptors_requests);
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "metrics", "descriptors"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "MetricDescriptors");
    assert_eq!(
        value["items"][0]["name"],
        "container_cpu_usage_seconds_total"
    );
    assert_eq!(value["items"][0]["labels"][0], "pod");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_scrape_text_outputs_only_metrics_body() {
    let metrics_endpoint = spawn_mock_metrics_server().await;
    let state = MockState::default();
    *state
        .metrics_endpoint
        .lock()
        .expect("metrics endpoint lock") = Some(format!("127.0.0.1:{}", metrics_endpoint.port()));
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "text", "metrics", "scrape"]);

    assert_success(&output);
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert_eq!(stdout, "crius_test_metric 42\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_scrape_json_reports_summary_without_embedding_body() {
    let metrics_endpoint = spawn_mock_metrics_server().await;
    let state = MockState::default();
    *state
        .metrics_endpoint
        .lock()
        .expect("metrics endpoint lock") = Some(format!(
        "http://127.0.0.1:{}/metrics",
        metrics_endpoint.port()
    ));
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "metrics", "scrape"]);

    assert_success(&output);
    let value = stdout_json(&output);
    assert_eq!(value["kind"], "MetricsScrape");
    assert_eq!(value["summary"]["contentType"], "text/plain; version=0.0.4");
    assert_eq!(value["summary"]["bytes"], "crius_test_metric 42\n".len());
    assert!(value["summary"]["scrapedAt"]
        .as_str()
        .expect("scrapedAt")
        .contains('T'));
    assert!(value.get("body").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_scrape_disabled_endpoint_returns_failed_precondition() {
    let state = MockState::default();
    *state.metrics_enabled.lock().expect("metrics enabled lock") = false;
    let endpoint = spawn_mock_services(state).await;

    let output = run_crs(endpoint, ["--output", "json", "metrics", "scrape"]);

    assert_eq!(output.status.code(), Some(6));
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    let value: serde_json::Value = serde_json::from_str(stderr.trim()).expect("json error");
    assert_eq!(value["error"]["exitCode"], 6);
    assert!(value["error"]["message"]
        .as_str()
        .expect("message")
        .contains("enable the daemon metrics endpoint"));
}

#[test]
fn json_error_is_written_to_stderr_with_empty_stdout() {
    let socket_dir = tempfile::tempdir().expect("tempdir should be created");
    let missing_socket = socket_dir.path().join("missing.sock");

    let output = Command::new(env!("CARGO_BIN_EXE_crs"))
        .args([
            "--address",
            missing_socket.to_str().expect("socket path should be utf8"),
            "--output",
            "json",
            "version",
        ])
        .output()
        .expect("crs process should run");

    assert_eq!(output.status.code(), Some(125));
    assert!(
        output.stdout.is_empty(),
        "stdout should be empty for JSON errors, got:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    let value: serde_json::Value = serde_json::from_str(stderr.trim()).expect("json error");

    assert_eq!(value["error"]["exitCode"], 125);
    assert_eq!(
        value["error"]["context"]["endpoint"],
        format!("unix://{}", missing_socket.display())
    );
    assert!(value["error"]["suggestion"]
        .as_str()
        .expect("suggestion should be a string")
        .contains("--address"));
}

#[test]
fn text_error_includes_endpoint_and_next_step() {
    let socket_dir = tempfile::tempdir().expect("tempdir should be created");
    let missing_socket = socket_dir.path().join("missing.sock");

    let output = Command::new(env!("CARGO_BIN_EXE_crs"))
        .args([
            "--address",
            missing_socket.to_str().expect("socket path should be utf8"),
            "version",
        ])
        .output()
        .expect("crs process should run");

    assert_eq!(output.status.code(), Some(125));
    assert!(
        output.stdout.is_empty(),
        "stdout should be empty for errors, got:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("error: daemon is unavailable"));
    assert!(stderr.contains(&format!("endpoint: unix://{}", missing_socket.display())));
    assert!(stderr.contains("next step: verify that crius is running"));
}

fn test_image() -> Image {
    Image {
        id: "sha256:testimage".into(),
        repo_tags: vec!["busybox:latest".into()],
        repo_digests: vec!["busybox@sha256:test".into()],
        size: 1024,
        uid: None,
        username: String::new(),
        spec: Some(ImageSpec {
            image: "busybox:latest".into(),
            user_specified_image: "busybox:latest".into(),
            ..Default::default()
        }),
        pinned: false,
    }
}

fn test_container_event(
    container_id: &str,
    pod_id: &str,
    event_type: ContainerEventType,
    created_at: i64,
) -> ContainerEventResponse {
    ContainerEventResponse {
        container_id: container_id.to_string(),
        container_event_type: event_type as i32,
        created_at,
        pod_sandbox_status: Some(PodSandboxStatus {
            id: pod_id.to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-a".into(),
                uid: "uid-a".into(),
                namespace: "default".into(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at,
            network: None,
            linux: None,
            labels: Default::default(),
            annotations: Default::default(),
            runtime_handler: "runc".into(),
        }),
        containers_statuses: Vec::new(),
    }
}

fn test_container_stats(
    id: &str,
    name: &str,
    cpu_nano_cores: u64,
    memory_bytes: u64,
) -> ContainerStats {
    ContainerStats {
        attributes: Some(ContainerAttributes {
            id: id.to_string(),
            metadata: Some(ContainerMetadata {
                name: name.to_string(),
                attempt: 1,
            }),
            labels: Default::default(),
            annotations: Default::default(),
        }),
        cpu: Some(CpuUsage {
            timestamp: 1_000_000_000,
            usage_core_nano_seconds: Some(UInt64Value { value: 2 }),
            usage_nano_cores: Some(UInt64Value {
                value: cpu_nano_cores,
            }),
        }),
        memory: Some(MemoryUsage {
            timestamp: 1_000_000_000,
            working_set_bytes: Some(UInt64Value {
                value: memory_bytes,
            }),
            available_bytes: None,
            usage_bytes: None,
            rss_bytes: None,
            page_faults: None,
            major_page_faults: None,
        }),
        writable_layer: None,
        swap: None,
    }
}

fn test_pod_stats(id: &str, name: &str) -> PodSandboxStats {
    PodSandboxStats {
        attributes: Some(PodSandboxAttributes {
            id: id.to_string(),
            metadata: Some(PodSandboxMetadata {
                name: name.to_string(),
                uid: format!("uid-{name}"),
                namespace: "default".into(),
                attempt: 1,
            }),
            labels: Default::default(),
            annotations: Default::default(),
        }),
        linux: Some(LinuxPodSandboxStats {
            cpu: Some(CpuUsage {
                timestamp: 1_000_000_000,
                usage_core_nano_seconds: Some(UInt64Value { value: 3 }),
                usage_nano_cores: Some(UInt64Value { value: 750_000_000 }),
            }),
            memory: Some(MemoryUsage {
                timestamp: 1_000_000_000,
                working_set_bytes: Some(UInt64Value { value: 16_384 }),
                available_bytes: None,
                usage_bytes: None,
                rss_bytes: None,
                page_faults: None,
                major_page_faults: None,
            }),
            network: Some(NetworkUsage {
                timestamp: 1_000_000_000,
                default_interface: Some(NetworkInterfaceUsage {
                    name: "eth0".into(),
                    rx_bytes: Some(UInt64Value { value: 1024 }),
                    rx_errors: None,
                    tx_bytes: Some(UInt64Value { value: 512 }),
                    tx_errors: None,
                }),
                interfaces: Vec::new(),
            }),
            process: Some(ProcessUsage {
                timestamp: 1_000_000_000,
                process_count: Some(UInt64Value { value: 12 }),
            }),
            containers: Vec::new(),
        }),
        windows: None,
    }
}

fn test_pod_metrics() -> PodSandboxMetrics {
    PodSandboxMetrics {
        pod_sandbox_id: "pod-metrics-a".into(),
        metrics: vec![Metric {
            name: "pod_cpu_usage_seconds_total".into(),
            timestamp: 1_000_000_000,
            metric_type: MetricType::Counter as i32,
            label_values: vec!["default".into()],
            value: Some(UInt64Value { value: 7 }),
        }],
        container_metrics: vec![ContainerMetrics {
            container_id: "ctr-metrics-a".into(),
            metrics: vec![
                Metric {
                    name: "container_memory_working_set_bytes".into(),
                    timestamp: 1_000_000_000,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec!["ctr-a".into()],
                    value: Some(UInt64Value { value: 8192 }),
                },
                Metric {
                    name: "container_cpu_usage_seconds_total".into(),
                    timestamp: 1_000_000_000,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec!["ctr-a".into()],
                    value: Some(UInt64Value { value: 3 }),
                },
            ],
        }],
    }
}

fn run_crs<I, S>(endpoint: SocketAddr, args: I) -> std::process::Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut command = Command::new(env!("CARGO_BIN_EXE_crs"));
    command.args([
        "--address",
        &format!("http://{endpoint}"),
        "--connect-timeout",
        "2s",
        "--timeout",
        "2s",
    ]);
    for arg in args {
        command.arg(arg.as_ref());
    }
    command.output().expect("crs process should run")
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "crs exited with {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_stdout(output: &std::process::Output, expected: &str) {
    let stdout = String::from_utf8(output.stdout.clone()).expect("stdout should be utf8");
    assert_eq!(stdout, expected);
}

fn stdout_json(output: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).expect("stdout should be valid JSON")
}

async fn spawn_mock_services(state: MockState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock server should bind");
    let endpoint = listener
        .local_addr()
        .expect("mock server should expose addr");

    tokio::spawn(async move {
        Server::builder()
            .add_service(RuntimeServiceServer::new(MockRuntimeService {
                state: state.clone(),
            }))
            .add_service(ImageServiceServer::new(MockImageService {
                state: state.clone(),
            }))
            .add_service(DiagnosticsServiceServer::new(MockDiagnosticsService {
                state,
            }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mock server should serve");
    });

    endpoint
}

async fn spawn_mock_cri_services(state: MockState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock server should bind");
    let endpoint = listener
        .local_addr()
        .expect("mock server should expose addr");

    tokio::spawn(async move {
        Server::builder()
            .add_service(RuntimeServiceServer::new(MockRuntimeService {
                state: state.clone(),
            }))
            .add_service(ImageServiceServer::new(MockImageService { state }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mock server should serve");
    });

    endpoint
}

async fn spawn_mock_metrics_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock metrics server should bind");
    let endpoint = listener
        .local_addr()
        .expect("mock metrics server should expose addr");

    tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("metrics client should connect");
        let mut stream = BufReader::new(stream);
        loop {
            let mut line = String::new();
            stream
                .read_line(&mut line)
                .await
                .expect("metrics request should read");
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        let mut stream = stream.into_inner();
        let body = "crius_test_metric 42\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("metrics response should write");
    });

    endpoint
}

async fn spawn_mock_websocket() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock websocket should bind");
    let endpoint = listener
        .local_addr()
        .expect("mock websocket should expose addr");

    tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("websocket client should connect");
        let mut stream = BufReader::new(stream);
        loop {
            let mut line = String::new();
            stream
                .read_line(&mut line)
                .await
                .expect("websocket request should read");
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        let mut stream = stream.into_inner();
        stream
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: SuPNh+KH/O8XLfKlUV5MeIq+Nng=\r\nSec-WebSocket-Protocol: v4.channel.k8s.io\r\n\r\n",
            )
            .await
            .expect("websocket response should write");
        stream
            .write_all(&[0x88, 0x00])
            .await
            .expect("websocket close should write");
    });

    endpoint
}
