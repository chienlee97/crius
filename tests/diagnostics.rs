use crius::proto::diagnostics::v1::{
    diagnostics_service_client::DiagnosticsServiceClient, diagnostics_service_server::{
        DiagnosticsService, DiagnosticsServiceServer,
    }, ContainerLogChunk, ContentGcRequest, ContentGcResponse, EffectiveConfigRequest,
    EffectiveConfigResponse, ImageTransfersRequest, ImageTransfersResponse, NriStatusRequest,
    NriStatusResponse, RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest,
    RecoveryStatusResponse, RuntimeHandlersRequest, RuntimeHandlersResponse,
    SecurityStatusRequest, SecurityStatusResponse, ServerInfoRequest, ServerInfoResponse,
    ShimStatusRequest, ShimStatusResponse,
};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Default)]
struct SmokeDiagnostics;

#[tonic::async_trait]
impl DiagnosticsService for SmokeDiagnostics {
    async fn server_info(
        &self,
        _request: tonic::Request<ServerInfoRequest>,
    ) -> Result<tonic::Response<ServerInfoResponse>, tonic::Status> {
        Ok(tonic::Response::new(ServerInfoResponse::default()))
    }

    async fn effective_config(
        &self,
        _request: tonic::Request<EffectiveConfigRequest>,
    ) -> Result<tonic::Response<EffectiveConfigResponse>, tonic::Status> {
        Ok(tonic::Response::new(EffectiveConfigResponse::default()))
    }

    async fn runtime_handlers(
        &self,
        _request: tonic::Request<RuntimeHandlersRequest>,
    ) -> Result<tonic::Response<RuntimeHandlersResponse>, tonic::Status> {
        Ok(tonic::Response::new(RuntimeHandlersResponse::default()))
    }

    async fn image_transfers(
        &self,
        _request: tonic::Request<ImageTransfersRequest>,
    ) -> Result<tonic::Response<ImageTransfersResponse>, tonic::Status> {
        Ok(tonic::Response::new(ImageTransfersResponse::default()))
    }

    async fn recovery_status(
        &self,
        _request: tonic::Request<RecoveryStatusRequest>,
    ) -> Result<tonic::Response<RecoveryStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(RecoveryStatusResponse::default()))
    }

    async fn recovery_check(
        &self,
        _request: tonic::Request<RecoveryCheckRequest>,
    ) -> Result<tonic::Response<RecoveryCheckResponse>, tonic::Status> {
        Ok(tonic::Response::new(RecoveryCheckResponse::default()))
    }

    async fn nri_status(
        &self,
        _request: tonic::Request<NriStatusRequest>,
    ) -> Result<tonic::Response<NriStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(NriStatusResponse::default()))
    }

    async fn security_status(
        &self,
        _request: tonic::Request<SecurityStatusRequest>,
    ) -> Result<tonic::Response<SecurityStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(SecurityStatusResponse::default()))
    }

    async fn shim_status(
        &self,
        _request: tonic::Request<ShimStatusRequest>,
    ) -> Result<tonic::Response<ShimStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(ShimStatusResponse::default()))
    }

    async fn content_gc(
        &self,
        _request: tonic::Request<ContentGcRequest>,
    ) -> Result<tonic::Response<ContentGcResponse>, tonic::Status> {
        Ok(tonic::Response::new(ContentGcResponse::default()))
    }

    type ContainerLogStream = ReceiverStream<Result<ContainerLogChunk, tonic::Status>>;

    async fn container_log(
        &self,
        _request: tonic::Request<crius::proto::diagnostics::v1::ContainerLogRequest>,
    ) -> Result<tonic::Response<Self::ContainerLogStream>, tonic::Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[test]
fn diagnostics_codegen_exports_expected_types() {
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
