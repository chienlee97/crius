use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::proto::diagnostics::v1::{
    diagnostics_service_server::DiagnosticsService, ContainerLogChunk, ContainerLogRequest,
    ContentGcRequest, ContentGcResponse, EffectiveConfigRequest, EffectiveConfigResponse,
    ImageTransfersRequest, ImageTransfersResponse, NriStatusRequest, NriStatusResponse,
    RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest, RecoveryStatusResponse,
    RuntimeHandlersRequest, RuntimeHandlersResponse, SecurityStatusRequest, SecurityStatusResponse,
    ServerInfoRequest, ServerInfoResponse, ShimStatusRequest, ShimStatusResponse,
};

#[derive(Clone, Debug, Default)]
pub struct DiagnosticsState {}

impl DiagnosticsState {
    pub fn empty() -> Self {
        Self {}
    }
}

#[derive(Clone, Debug)]
pub struct DiagnosticsServiceImpl {
    state: DiagnosticsState,
}

impl DiagnosticsServiceImpl {
    pub fn new(state: DiagnosticsState) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &DiagnosticsState {
        &self.state
    }
}

fn unimplemented(method: &str) -> Status {
    Status::unimplemented(format!("diagnostics {method} is not implemented yet"))
}

#[tonic::async_trait]
impl DiagnosticsService for DiagnosticsServiceImpl {
    async fn server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        Err(unimplemented("ServerInfo"))
    }

    async fn effective_config(
        &self,
        _request: Request<EffectiveConfigRequest>,
    ) -> Result<Response<EffectiveConfigResponse>, Status> {
        Err(unimplemented("EffectiveConfig"))
    }

    async fn runtime_handlers(
        &self,
        _request: Request<RuntimeHandlersRequest>,
    ) -> Result<Response<RuntimeHandlersResponse>, Status> {
        Err(unimplemented("RuntimeHandlers"))
    }

    async fn image_transfers(
        &self,
        _request: Request<ImageTransfersRequest>,
    ) -> Result<Response<ImageTransfersResponse>, Status> {
        Err(unimplemented("ImageTransfers"))
    }

    async fn recovery_status(
        &self,
        _request: Request<RecoveryStatusRequest>,
    ) -> Result<Response<RecoveryStatusResponse>, Status> {
        Err(unimplemented("RecoveryStatus"))
    }

    async fn recovery_check(
        &self,
        _request: Request<RecoveryCheckRequest>,
    ) -> Result<Response<RecoveryCheckResponse>, Status> {
        Err(unimplemented("RecoveryCheck"))
    }

    async fn nri_status(
        &self,
        _request: Request<NriStatusRequest>,
    ) -> Result<Response<NriStatusResponse>, Status> {
        Err(unimplemented("NriStatus"))
    }

    async fn security_status(
        &self,
        _request: Request<SecurityStatusRequest>,
    ) -> Result<Response<SecurityStatusResponse>, Status> {
        Err(unimplemented("SecurityStatus"))
    }

    async fn shim_status(
        &self,
        _request: Request<ShimStatusRequest>,
    ) -> Result<Response<ShimStatusResponse>, Status> {
        Err(unimplemented("ShimStatus"))
    }

    async fn content_gc(
        &self,
        _request: Request<ContentGcRequest>,
    ) -> Result<Response<ContentGcResponse>, Status> {
        Err(unimplemented("ContentGc"))
    }

    type ContainerLogStream = ReceiverStream<Result<ContainerLogChunk, Status>>;

    async fn container_log(
        &self,
        _request: Request<ContainerLogRequest>,
    ) -> Result<Response<Self::ContainerLogStream>, Status> {
        Err(unimplemented("ContainerLog"))
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[tokio::test]
    async fn service_starts_with_unimplemented_methods() {
        let service = DiagnosticsServiceImpl::new(DiagnosticsState::empty());

        let error = service
            .server_info(Request::new(ServerInfoRequest {}))
            .await
            .expect_err("stub should be unimplemented");

        assert_eq!(error.code(), Code::Unimplemented);
        assert!(error.message().contains("ServerInfo"));
    }
}
