use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use serde_json::Value;

use crate::proto::diagnostics::v1::{
    diagnostics_service_server::DiagnosticsService, ContainerLogChunk, ContainerLogRequest,
    ContentGcRequest, ContentGcResponse, EffectiveConfigRequest, EffectiveConfigResponse,
    ImageTransfersRequest, ImageTransfersResponse, NriStatusRequest, NriStatusResponse,
    RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest, RecoveryStatusResponse,
    RuntimeHandlersRequest, RuntimeHandlersResponse, SecurityStatusRequest, SecurityStatusResponse,
    ServerInfoRequest, ServerInfoResponse, ShimStatusRequest, ShimStatusResponse,
};

#[derive(Clone, Debug, Default)]
pub struct DiagnosticsState {
    version: String,
    git_commit: String,
    config_path: String,
    state_dir: String,
    socket_path: String,
    config_json: String,
    redacted_config_json: String,
    redacted_fields: Vec<String>,
}

impl DiagnosticsState {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn new(
        version: impl Into<String>,
        git_commit: impl Into<String>,
        config_path: impl Into<String>,
        state_dir: impl Into<String>,
        socket_path: impl Into<String>,
        config: &crate::config::Config,
    ) -> Self {
        let config_value = serde_json::to_value(config).unwrap_or_else(|_| Value::Null);
        let config_json = serde_json::to_string(&config_value).unwrap_or_else(|_| "{}".into());
        let mut redacted_config = config_value;
        let mut redacted_fields = Vec::new();
        redact_sensitive_config(&mut redacted_config, "", &mut redacted_fields);
        let redacted_config_json =
            serde_json::to_string(&redacted_config).unwrap_or_else(|_| "{}".into());

        Self {
            version: version.into(),
            git_commit: git_commit.into(),
            config_path: config_path.into(),
            state_dir: state_dir.into(),
            socket_path: socket_path.into(),
            config_json,
            redacted_config_json,
            redacted_fields,
        }
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
        Ok(Response::new(ServerInfoResponse {
            version: self.state.version.clone(),
            git_commit: self.state.git_commit.clone(),
            config_path: self.state.config_path.clone(),
            state_dir: self.state.state_dir.clone(),
            socket_path: self.state.socket_path.clone(),
        }))
    }

    async fn effective_config(
        &self,
        request: Request<EffectiveConfigRequest>,
    ) -> Result<Response<EffectiveConfigResponse>, Status> {
        let include_sensitive = request.into_inner().include_sensitive;
        Ok(Response::new(EffectiveConfigResponse {
            config_json: if include_sensitive {
                self.state.config_json.clone()
            } else {
                self.state.redacted_config_json.clone()
            },
            redacted_fields: if include_sensitive {
                Vec::new()
            } else {
                self.state.redacted_fields.clone()
            },
            warnings: Vec::new(),
        }))
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

fn redact_sensitive_config(value: &mut Value, path: &str, redacted_fields: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if is_sensitive_key(key) {
                    *child = Value::String("<redacted>".into());
                    redacted_fields.push(child_path);
                } else {
                    redact_sensitive_config(child, &child_path, redacted_fields);
                }
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter_mut().enumerate() {
                redact_sensitive_config(child, &format!("{path}[{index}]"), redacted_fields);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("password")
        || key.contains("token")
        || key.contains("secret")
        || key.contains("auth")
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[tokio::test]
    async fn service_starts_with_unimplemented_methods() {
        let service = DiagnosticsServiceImpl::new(DiagnosticsState::empty());

        let error = service
            .runtime_handlers(Request::new(RuntimeHandlersRequest {}))
            .await
            .expect_err("stub should be unimplemented");

        assert_eq!(error.code(), Code::Unimplemented);
        assert!(error.message().contains("RuntimeHandlers"));
    }

    #[tokio::test]
    async fn server_info_returns_state() {
        let service = DiagnosticsServiceImpl::new(DiagnosticsState::new(
            "0.1.0",
            "abc123",
            "/etc/crius/crius.conf",
            "/var/lib/crius",
            "/run/crius/crius.sock",
            &crate::config::Config::default(),
        ));

        let response = service
            .server_info(Request::new(ServerInfoRequest {}))
            .await
            .expect("server info should succeed")
            .into_inner();

        assert_eq!(response.version, "0.1.0");
        assert_eq!(response.git_commit, "abc123");
        assert_eq!(response.config_path, "/etc/crius/crius.conf");
        assert_eq!(response.state_dir, "/var/lib/crius");
        assert_eq!(response.socket_path, "/run/crius/crius.sock");
    }

    #[tokio::test]
    async fn effective_config_redacts_sensitive_fields() {
        let mut config = crate::config::Config::default();
        config.image.global_auth_file = "/var/lib/kubelet/config.json".into();
        let service = DiagnosticsServiceImpl::new(DiagnosticsState::new(
            "0.1.0",
            "abc123",
            "/etc/crius/crius.conf",
            "/var/lib/crius",
            "/run/crius/crius.sock",
            &config,
        ));

        let response = service
            .effective_config(Request::new(EffectiveConfigRequest {
                include_sensitive: false,
            }))
            .await
            .expect("effective config should succeed")
            .into_inner();

        assert!(response.config_json.contains("<redacted>"));
        assert!(response
            .redacted_fields
            .iter()
            .any(|field| field.contains("global_auth_file")));
    }
}
