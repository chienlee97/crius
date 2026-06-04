use std::{
    net::SocketAddr,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crius::proto::runtime::v1::{
    image_service_server::{ImageService, ImageServiceServer},
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
    state: MockState,
}

#[derive(Clone, Default)]
struct MockImageService {
    state: MockState,
}

#[derive(Clone, Default)]
struct MockState {
    version_requests: Arc<AtomicUsize>,
    status_requests: Arc<AtomicUsize>,
    runtime_config_requests: Arc<AtomicUsize>,
    list_pod_requests: Arc<AtomicUsize>,
    pod_inspect_requests: Arc<AtomicUsize>,
    list_container_requests: Arc<AtomicUsize>,
    container_inspect_requests: Arc<AtomicUsize>,
    list_image_requests: Arc<AtomicUsize>,
    image_inspect_requests: Arc<AtomicUsize>,
    image_fs_info_requests: Arc<AtomicUsize>,
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
    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        let request = request.into_inner();
        assert!(request.verbose);
        self.state
            .pod_inspect_requests
            .fetch_add(1, Ordering::SeqCst);

        if request.pod_sandbox_id == "missing" {
            return Err(Status::not_found("pod not found"));
        }

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
                annotations: Default::default(),
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
            items: vec![PodSandbox {
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
            }],
        }))
    }
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
        assert!(request.verbose);
        self.state
            .container_inspect_requests
            .fetch_add(1, Ordering::SeqCst);

        if request.container_id == "missing" {
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
    unimplemented_runtime_rpc!(
        update_pod_sandbox_resources,
        UpdatePodSandboxResourcesRequest,
        UpdatePodSandboxResourcesResponse
    );
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
        assert!(request.verbose);
        self.state
            .image_inspect_requests
            .fetch_add(1, Ordering::SeqCst);
        let image = request.image.expect("image spec").image;

        if image == "missing" {
            return Err(Status::not_found("image not found"));
        }

        Ok(Response::new(ImageStatusResponse {
            image: Some(test_image()),
            info: [("manifest".to_string(), r#"{"mediaType":"oci"}"#.to_string())].into(),
        }))
    }

    async fn pull_image(
        &self,
        _request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        Err(Status::unimplemented(
            "mock only implements read-only image RPCs",
        ))
    }

    async fn remove_image(
        &self,
        _request: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        Err(Status::unimplemented(
            "mock only implements read-only image RPCs",
        ))
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
async fn image_read_only_commands_return_expected_kinds() {
    let state = MockState::default();
    let list_requests = Arc::clone(&state.list_image_requests);
    let inspect_requests = Arc::clone(&state.image_inspect_requests);
    let fs_requests = Arc::clone(&state.image_fs_info_requests);
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
            .add_service(ImageServiceServer::new(MockImageService { state }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mock server should serve");
    });

    endpoint
}
