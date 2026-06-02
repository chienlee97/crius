use std::{
    net::SocketAddr,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crius::proto::runtime::v1::{
    runtime_service_server::{RuntimeService, RuntimeServiceServer},
    *,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{transport::Server, Request, Response, Status};

macro_rules! unimplemented_runtime_rpc {
    ($name:ident, $request:ty, $response:ty) => {
        fn $name<'life0, 'async_trait>(
            &'life0 self,
            _request: Request<$request>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Response<$response>, Status>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            Self: 'async_trait,
            'life0: 'async_trait,
        {
            Box::pin(async { Err(Status::unimplemented("mock only implements Version")) })
        }
    };
}

#[derive(Clone, Default)]
struct MockRuntimeService {
    version_requests: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl RuntimeService for MockRuntimeService {
    async fn version(
        &self,
        request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        assert_eq!(request.into_inner().version, "");
        self.version_requests.fetch_add(1, Ordering::SeqCst);

        Ok(Response::new(VersionResponse {
            version: "0.1.0".into(),
            runtime_name: "crius-test".into(),
            runtime_version: "9.8.7".into(),
            runtime_api_version: "v1".into(),
        }))
    }

    unimplemented_runtime_rpc!(run_pod_sandbox, RunPodSandboxRequest, RunPodSandboxResponse);
    unimplemented_runtime_rpc!(
        stop_pod_sandbox,
        StopPodSandboxRequest,
        StopPodSandboxResponse
    );
    unimplemented_runtime_rpc!(
        remove_pod_sandbox,
        RemovePodSandboxRequest,
        RemovePodSandboxResponse
    );
    unimplemented_runtime_rpc!(
        pod_sandbox_status,
        PodSandboxStatusRequest,
        PodSandboxStatusResponse
    );
    unimplemented_runtime_rpc!(
        list_pod_sandbox,
        ListPodSandboxRequest,
        ListPodSandboxResponse
    );
    unimplemented_runtime_rpc!(
        create_container,
        CreateContainerRequest,
        CreateContainerResponse
    );
    unimplemented_runtime_rpc!(
        start_container,
        StartContainerRequest,
        StartContainerResponse
    );
    unimplemented_runtime_rpc!(stop_container, StopContainerRequest, StopContainerResponse);
    unimplemented_runtime_rpc!(
        remove_container,
        RemoveContainerRequest,
        RemoveContainerResponse
    );
    unimplemented_runtime_rpc!(
        list_containers,
        ListContainersRequest,
        ListContainersResponse
    );
    unimplemented_runtime_rpc!(
        container_status,
        ContainerStatusRequest,
        ContainerStatusResponse
    );
    unimplemented_runtime_rpc!(
        update_container_resources,
        UpdateContainerResourcesRequest,
        UpdateContainerResourcesResponse
    );
    unimplemented_runtime_rpc!(
        reopen_container_log,
        ReopenContainerLogRequest,
        ReopenContainerLogResponse
    );
    unimplemented_runtime_rpc!(exec_sync, ExecSyncRequest, ExecSyncResponse);
    unimplemented_runtime_rpc!(exec, ExecRequest, ExecResponse);
    unimplemented_runtime_rpc!(attach, AttachRequest, AttachResponse);
    unimplemented_runtime_rpc!(port_forward, PortForwardRequest, PortForwardResponse);
    unimplemented_runtime_rpc!(
        container_stats,
        ContainerStatsRequest,
        ContainerStatsResponse
    );
    unimplemented_runtime_rpc!(
        list_container_stats,
        ListContainerStatsRequest,
        ListContainerStatsResponse
    );
    unimplemented_runtime_rpc!(
        pod_sandbox_stats,
        PodSandboxStatsRequest,
        PodSandboxStatsResponse
    );
    unimplemented_runtime_rpc!(
        list_pod_sandbox_stats,
        ListPodSandboxStatsRequest,
        ListPodSandboxStatsResponse
    );
    unimplemented_runtime_rpc!(
        update_runtime_config,
        UpdateRuntimeConfigRequest,
        UpdateRuntimeConfigResponse
    );
    unimplemented_runtime_rpc!(status, StatusRequest, StatusResponse);
    unimplemented_runtime_rpc!(
        checkpoint_container,
        CheckpointContainerRequest,
        CheckpointContainerResponse
    );

    type GetContainerEventsStream = ReceiverStream<Result<ContainerEventResponse, Status>>;

    async fn get_container_events(
        &self,
        _request: Request<GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        Err(Status::unimplemented("mock only implements Version"))
    }

    unimplemented_runtime_rpc!(
        list_metric_descriptors,
        ListMetricDescriptorsRequest,
        ListMetricDescriptorsResponse
    );
    unimplemented_runtime_rpc!(
        list_pod_sandbox_metrics,
        ListPodSandboxMetricsRequest,
        ListPodSandboxMetricsResponse
    );
    unimplemented_runtime_rpc!(runtime_config, RuntimeConfigRequest, RuntimeConfigResponse);
    unimplemented_runtime_rpc!(
        update_pod_sandbox_resources,
        UpdatePodSandboxResourcesRequest,
        UpdatePodSandboxResourcesResponse
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_command_reaches_mock_runtime_service() {
    let service = MockRuntimeService::default();
    let version_requests = Arc::clone(&service.version_requests);
    let endpoint = spawn_runtime_service(service).await;

    let output = Command::new(env!("CARGO_BIN_EXE_crs"))
        .args([
            "--address",
            &format!("http://{endpoint}"),
            "--connect-timeout",
            "2s",
            "--timeout",
            "2s",
            "version",
        ])
        .output()
        .expect("crs process should run");

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

async fn spawn_runtime_service(service: MockRuntimeService) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock server should bind");
    let endpoint = listener
        .local_addr()
        .expect("mock server should expose addr");

    tokio::spawn(async move {
        Server::builder()
            .add_service(RuntimeServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mock server should serve");
    });

    endpoint
}
