use crius::proto::diagnostics::v1::{
    diagnostics_service_client::DiagnosticsServiceClient,
    diagnostics_service_server::{DiagnosticsService, DiagnosticsServiceServer},
    ContainerLogChunk, ContainerLogRequest, ContentGcCandidate, ContentGcRequest,
    ContentGcResponse, EffectiveConfigRequest, EffectiveConfigResponse, ImageTransferInfo,
    ImageTransfersRequest, ImageTransfersResponse, NriStatusRequest, NriStatusResponse,
    RecoveryAction, RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest,
    RecoveryStatusResponse, RuntimeHandlerInfo, RuntimeHandlersRequest, RuntimeHandlersResponse,
    SecurityStatusRequest, SecurityStatusResponse, ServerInfoRequest, ServerInfoResponse, ShimInfo,
    ShimStatusRequest, ShimStatusResponse,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

#[derive(Debug, Default)]
struct SmokeDiagnostics;

#[tonic::async_trait]
impl DiagnosticsService for SmokeDiagnostics {
    async fn server_info(
        &self,
        _request: tonic::Request<ServerInfoRequest>,
    ) -> Result<tonic::Response<ServerInfoResponse>, tonic::Status> {
        Ok(tonic::Response::new(ServerInfoResponse {
            version: "0.1.0".into(),
            git_commit: "abc123".into(),
            config_path: "/etc/crius/crius.conf".into(),
            state_dir: "/run/crius".into(),
            socket_path: "/run/crius/crius.sock".into(),
        }))
    }

    async fn effective_config(
        &self,
        request: tonic::Request<EffectiveConfigRequest>,
    ) -> Result<tonic::Response<EffectiveConfigResponse>, tonic::Status> {
        Ok(tonic::Response::new(EffectiveConfigResponse {
            config_json: format!(
                r#"{{"includeSensitive":{}}}"#,
                request.into_inner().include_sensitive
            ),
            redacted_fields: vec!["image.global_auth_file".into()],
            warnings: Vec::new(),
        }))
    }

    async fn runtime_handlers(
        &self,
        _request: tonic::Request<RuntimeHandlersRequest>,
    ) -> Result<tonic::Response<RuntimeHandlersResponse>, tonic::Status> {
        Ok(tonic::Response::new(RuntimeHandlersResponse {
            handlers: vec![RuntimeHandlerInfo {
                name: "runc".into(),
                runtime_type: "runc".into(),
                runtime_path: "/usr/bin/runc".into(),
                runtime_config_path: String::new(),
                features: vec!["snapshotter=internal-overlay-untar".into()],
                warnings: Vec::new(),
            }],
        }))
    }

    async fn image_transfers(
        &self,
        request: tonic::Request<ImageTransfersRequest>,
    ) -> Result<tonic::Response<ImageTransfersResponse>, tonic::Status> {
        let status = if request.into_inner().include_completed {
            "succeeded"
        } else {
            "failed"
        };
        Ok(tonic::Response::new(ImageTransfersResponse {
            transfers: vec![ImageTransferInfo {
                image: "registry.example.com/app:latest".into(),
                status: status.into(),
                updated_at_unix_nanos: 42,
                error: String::new(),
            }],
        }))
    }

    async fn recovery_status(
        &self,
        _request: tonic::Request<RecoveryStatusRequest>,
    ) -> Result<tonic::Response<RecoveryStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(RecoveryStatusResponse {
            status: "healthy".into(),
            last_startup: "clean_shutdown".into(),
            unhealthy_object_count: 0,
            ledger_summary_json: "{}".into(),
            warnings: Vec::new(),
        }))
    }

    async fn recovery_check(
        &self,
        request: tonic::Request<RecoveryCheckRequest>,
    ) -> Result<tonic::Response<RecoveryCheckResponse>, tonic::Status> {
        let execute = request.into_inner().execute;
        Ok(tonic::Response::new(RecoveryCheckResponse {
            dry_run: !execute,
            actions: vec![RecoveryAction {
                object_type: "snapshot".into(),
                object_id: "snapshot-1".into(),
                action: "markBroken".into(),
                reason: "missing mountpoint".into(),
                executed: execute,
                error: String::new(),
            }],
            warnings: Vec::new(),
        }))
    }

    async fn nri_status(
        &self,
        _request: tonic::Request<NriStatusRequest>,
    ) -> Result<tonic::Response<NriStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(NriStatusResponse {
            enabled: true,
            cdi_enabled: true,
            cdi_spec_dirs: vec!["/etc/cdi".into()],
            plugin_path: "/opt/nri/plugins".into(),
            plugin_config_path: "/etc/nri/conf.d".into(),
            blockio_config_path: "/etc/blockio.json".into(),
            blockio_supported: true,
            rdt_supported: false,
            warnings: Vec::new(),
        }))
    }

    async fn security_status(
        &self,
        _request: tonic::Request<SecurityStatusRequest>,
    ) -> Result<tonic::Response<SecurityStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(SecurityStatusResponse {
            seccomp_available: true,
            seccomp_notifier_supported: true,
            apparmor_available: false,
            selinux_enabled: false,
            rootless_enabled: false,
            devices_policy_json: "{}".into(),
            warnings: Vec::new(),
        }))
    }

    async fn shim_status(
        &self,
        request: tonic::Request<ShimStatusRequest>,
    ) -> Result<tonic::Response<ShimStatusResponse>, tonic::Status> {
        let container_id = request.into_inner().container_id;
        Ok(tonic::Response::new(ShimStatusResponse {
            shims: vec![ShimInfo {
                container_id,
                pid: 1234,
                task_socket: "/run/crius/shims/c/task.sock".into(),
                attach_socket: "/run/crius/attach/c/attach.sock".into(),
                state: "running".into(),
                error: String::new(),
            }],
        }))
    }

    async fn content_gc(
        &self,
        request: tonic::Request<ContentGcRequest>,
    ) -> Result<tonic::Response<ContentGcResponse>, tonic::Status> {
        let execute = request.into_inner().execute;
        Ok(tonic::Response::new(ContentGcResponse {
            dry_run: !execute,
            candidates: vec![ContentGcCandidate {
                object_type: "content_blob".into(),
                object_id: "sha256:blob".into(),
                path: "blobs/sha256/blob".into(),
                size_bytes: 64,
                reason: "unreferenced content blob".into(),
                deleted: execute,
                error: String::new(),
            }],
            reclaimed_bytes: if execute { 64 } else { 0 },
            warnings: Vec::new(),
        }))
    }

    type ContainerLogStream = ReceiverStream<Result<ContainerLogChunk, tonic::Status>>;

    async fn container_log(
        &self,
        _request: tonic::Request<ContainerLogRequest>,
    ) -> Result<tonic::Response<Self::ContainerLogStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(Ok(ContainerLogChunk {
            data: b"hello\n".to_vec(),
            stream: "stdout".into(),
            timestamp_unix_nanos: 1,
        }))
        .await
        .expect("receiver should be alive");
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::test]
async fn diagnostics_codegen_exports_expected_types() {
    let _client_type = std::any::type_name::<DiagnosticsServiceClient<tonic::transport::Channel>>();
    let _server = DiagnosticsServiceServer::new(SmokeDiagnostics);

    let request = EffectiveConfigRequest {
        include_sensitive: false,
    };
    let response = ServerInfoResponse {
        version: "0.1.0".into(),
        git_commit: "unknown".into(),
        config_path: "/etc/crius/crius.conf".into(),
        state_dir: "/run/crius".into(),
        socket_path: "/run/crius/crius.sock".into(),
    };
    let chunk = ContainerLogChunk {
        data: b"hello".to_vec(),
        stream: "stdout".into(),
        timestamp_unix_nanos: 1,
    };

    assert!(!request.include_sensitive);
    assert_eq!(response.socket_path, "/run/crius/crius.sock");
    assert_eq!(chunk.stream, "stdout");
}

#[tokio::test]
async fn diagnostics_client_round_trips_every_rpc() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have address");
    let server = tonic::transport::Server::builder()
        .add_service(DiagnosticsServiceServer::new(SmokeDiagnostics))
        .serve_with_incoming(TcpListenerStream::new(listener));
    tokio::spawn(server);

    let mut client = DiagnosticsServiceClient::connect(format!("http://{address}"))
        .await
        .expect("client should connect");

    let server_info = client
        .server_info(ServerInfoRequest {})
        .await
        .expect("server info should succeed")
        .into_inner();
    assert_eq!(server_info.version, "0.1.0");

    let config = client
        .effective_config(EffectiveConfigRequest {
            include_sensitive: false,
        })
        .await
        .expect("effective config should succeed")
        .into_inner();
    assert!(config.config_json.contains("includeSensitive"));

    let handlers = client
        .runtime_handlers(RuntimeHandlersRequest {})
        .await
        .expect("runtime handlers should succeed")
        .into_inner();
    assert_eq!(handlers.handlers[0].name, "runc");

    let transfers = client
        .image_transfers(ImageTransfersRequest {
            include_completed: true,
        })
        .await
        .expect("image transfers should succeed")
        .into_inner();
    assert_eq!(transfers.transfers[0].status, "succeeded");

    let recovery_status = client
        .recovery_status(RecoveryStatusRequest {})
        .await
        .expect("recovery status should succeed")
        .into_inner();
    assert_eq!(recovery_status.status, "healthy");

    let recovery_check = client
        .recovery_check(RecoveryCheckRequest { execute: true })
        .await
        .expect("recovery check should succeed")
        .into_inner();
    assert!(recovery_check.actions[0].executed);

    let nri = client
        .nri_status(NriStatusRequest {})
        .await
        .expect("nri status should succeed")
        .into_inner();
    assert!(nri.enabled);

    let security = client
        .security_status(SecurityStatusRequest {})
        .await
        .expect("security status should succeed")
        .into_inner();
    assert!(security.seccomp_available);

    let shim = client
        .shim_status(ShimStatusRequest {
            container_id: "container-1".into(),
        })
        .await
        .expect("shim status should succeed")
        .into_inner();
    assert_eq!(shim.shims[0].container_id, "container-1");

    let gc = client
        .content_gc(ContentGcRequest { execute: true })
        .await
        .expect("content GC should succeed")
        .into_inner();
    assert_eq!(gc.reclaimed_bytes, 64);

    let mut log_stream = client
        .container_log(ContainerLogRequest {
            container_id: "container-1".into(),
            ..Default::default()
        })
        .await
        .expect("container log should succeed")
        .into_inner();
    let chunk = log_stream
        .next()
        .await
        .expect("log chunk should be present")
        .expect("log chunk should be ok");
    assert_eq!(chunk.data, b"hello\n");
}
