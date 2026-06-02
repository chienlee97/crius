use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::image::{content_store::TransferState, ImageServiceImpl};
use crate::proto::diagnostics::v1::{
    diagnostics_service_server::DiagnosticsService, ContainerLogChunk, ContainerLogRequest,
    ContentGcRequest, ContentGcResponse, EffectiveConfigRequest, EffectiveConfigResponse,
    ImageTransferInfo, ImageTransfersRequest, ImageTransfersResponse, NriStatusRequest,
    NriStatusResponse, RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest,
    RecoveryStatusResponse, RuntimeHandlerInfo, RuntimeHandlersRequest, RuntimeHandlersResponse,
    SecurityStatusRequest, SecurityStatusResponse, ServerInfoRequest, ServerInfoResponse,
    ShimStatusRequest, ShimStatusResponse,
};

#[derive(Clone, Default)]
pub struct DiagnosticsState {
    version: String,
    git_commit: String,
    config_path: String,
    state_dir: String,
    socket_path: String,
    config_json: String,
    redacted_config_json: String,
    redacted_fields: Vec<String>,
    runtime_handlers: Vec<RuntimeHandlerInfo>,
    runtime_handler_warnings: Vec<String>,
    image_service: Option<ImageServiceImpl>,
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
        let (runtime_handlers, runtime_handler_warnings) = runtime_handlers_from_config(config);

        Self {
            version: version.into(),
            git_commit: git_commit.into(),
            config_path: config_path.into(),
            state_dir: state_dir.into(),
            socket_path: socket_path.into(),
            config_json,
            redacted_config_json,
            redacted_fields,
            runtime_handlers,
            runtime_handler_warnings,
            image_service: None,
        }
    }

    pub fn from_runtime(
        version: impl Into<String>,
        git_commit: impl Into<String>,
        config: &crate::config::Config,
        runtime: &crate::server::RuntimeServiceImpl,
        socket_path: impl Into<String>,
    ) -> Self {
        let snapshot = runtime.diagnostics_snapshot(socket_path);
        let mut state = Self::new(
            version,
            git_commit,
            snapshot
                .config_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            snapshot.state_dir.display().to_string(),
            snapshot.socket_path,
            config,
        );
        state.image_service = Some(snapshot.image_service);
        state
    }
}

#[derive(Clone)]
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
        let mut handlers = self.state.runtime_handlers.clone();
        for handler in &mut handlers {
            handler
                .warnings
                .extend(self.state.runtime_handler_warnings.clone());
        }

        Ok(Response::new(RuntimeHandlersResponse { handlers }))
    }

    async fn image_transfers(
        &self,
        request: Request<ImageTransfersRequest>,
    ) -> Result<Response<ImageTransfersResponse>, Status> {
        let include_completed = request.into_inner().include_completed;
        let Some(image_service) = self.state.image_service.as_ref() else {
            return Ok(Response::new(ImageTransfersResponse {
                transfers: Vec::new(),
            }));
        };
        let status = image_service.content_transfer_status();
        let transfers = status
            .active
            .into_iter()
            .chain(status.recent)
            .filter(|record| include_completed || record.state != TransferState::Succeeded)
            .map(|record| ImageTransferInfo {
                image: record.source,
                status: record.state.as_str().to_string(),
                updated_at_unix_nanos: record
                    .finished_at_unix_nanos
                    .unwrap_or(record.started_at_unix_nanos),
                error: record.error.unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(ImageTransfersResponse { transfers }))
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

fn runtime_handlers_from_config(
    config: &crate::config::Config,
) -> (Vec<RuntimeHandlerInfo>, Vec<String>) {
    match config.runtime.resolved_runtimes() {
        Ok(resolved) => {
            let mut handlers = resolved
                .into_iter()
                .map(|(name, handler)| RuntimeHandlerInfo {
                    name,
                    runtime_type: handler.backend,
                    runtime_path: handler.runtime_path,
                    runtime_config_path: handler.runtime_config_path,
                    features: vec![format!("snapshotter={}", handler.snapshotter)],
                    warnings: Vec::new(),
                })
                .collect::<Vec<_>>();
            handlers.sort_by(|left, right| left.name.cmp(&right.name));
            (handlers, Vec::new())
        }
        Err(err) => (
            Vec::new(),
            vec![format!("failed to resolve runtime handlers: {err}")],
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use tonic::Code;

    use super::*;
    use crate::image::{ImageServiceOptions, TestPullResponse};
    use crate::proto::runtime::v1::{
        image_service_server::ImageService, ImageSpec, PullImageRequest,
    };

    #[tokio::test]
    async fn service_still_reports_unimplemented_methods_for_future_rpcs() {
        let service = DiagnosticsServiceImpl::new(DiagnosticsState::empty());

        let error = service
            .recovery_status(Request::new(RecoveryStatusRequest {}))
            .await
            .expect_err("stub should be unimplemented");

        assert_eq!(error.code(), Code::Unimplemented);
        assert!(error.message().contains("RecoveryStatus"));
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

    #[tokio::test]
    async fn runtime_handlers_returns_configured_handlers() {
        let config = crate::config::Config::default();
        let service = DiagnosticsServiceImpl::new(DiagnosticsState::new(
            "0.1.0",
            "abc123",
            "/etc/crius/crius.conf",
            "/var/lib/crius",
            "/run/crius/crius.sock",
            &config,
        ));

        let response = service
            .runtime_handlers(Request::new(RuntimeHandlersRequest {}))
            .await
            .expect("runtime handlers should succeed")
            .into_inner();

        assert!(response
            .handlers
            .iter()
            .any(|handler| handler.runtime_type == "runc"
                && handler
                    .features
                    .iter()
                    .any(|feature| feature.starts_with("snapshotter="))));
    }

    #[tokio::test]
    async fn state_from_runtime_uses_runtime_diagnostics_snapshot() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let mut runtime_config = crate::server::RuntimeConfig::default();
        runtime_config.root_dir = tempdir.path().join("state");
        runtime_config.config_path = Some(tempdir.path().join("crius.conf"));
        let runtime = crate::server::RuntimeServiceImpl::new(runtime_config);

        let state = DiagnosticsState::from_runtime(
            "0.1.0",
            "abc123",
            &crate::config::Config::default(),
            &runtime,
            "/run/crius/crius.sock",
        );
        let service = DiagnosticsServiceImpl::new(state);

        let response = service
            .server_info(Request::new(ServerInfoRequest {}))
            .await
            .expect("server info should succeed")
            .into_inner();

        assert_eq!(response.version, "0.1.0");
        assert_eq!(response.git_commit, "abc123");
        assert_eq!(
            response.config_path,
            tempdir.path().join("crius.conf").display().to_string()
        );
        assert_eq!(
            response.state_dir,
            tempdir.path().join("state").display().to_string()
        );
        assert_eq!(response.socket_path, "/run/crius/crius.sock");
        assert!(service.state().image_service.is_some());
    }

    #[tokio::test]
    async fn image_transfers_reports_active_and_recent_transfer_status() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let image_service = test_image_service(tempdir.path().join("images"));
        image_service.set_test_pull_handler(Arc::new(|request| {
            if request.requested_ref.contains("failure") {
                Err(Status::internal("registry failed"))
            } else {
                Ok(TestPullResponse {
                    image_id: format!("sha256:{}", request.requested_ref.replace('/', "-")),
                    size: 1,
                    ..Default::default()
                })
            }
        }));

        ImageService::pull_image(
            &image_service,
            Request::new(PullImageRequest {
                image: Some(ImageSpec {
                    image: "repo/success:latest".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("successful pull should complete");
        let _ = ImageService::pull_image(
            &image_service,
            Request::new(PullImageRequest {
                image: Some(ImageSpec {
                    image: "repo/failure:latest".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await;

        let mut state = DiagnosticsState::empty();
        state.image_service = Some(image_service);
        let service = DiagnosticsServiceImpl::new(state);

        let default_response = service
            .image_transfers(Request::new(ImageTransfersRequest {
                include_completed: false,
            }))
            .await
            .expect("image transfers should succeed")
            .into_inner();

        assert_eq!(default_response.transfers.len(), 1);
        assert_eq!(default_response.transfers[0].status, "failed");
        assert_eq!(default_response.transfers[0].error, "registry failed");

        let all_response = service
            .image_transfers(Request::new(ImageTransfersRequest {
                include_completed: true,
            }))
            .await
            .expect("image transfers should include completed records")
            .into_inner();

        assert_eq!(all_response.transfers.len(), 2);
        assert!(all_response
            .transfers
            .iter()
            .any(|transfer| transfer.status == "succeeded"));
        assert!(all_response
            .transfers
            .iter()
            .all(|transfer| transfer.updated_at_unix_nanos > 0));
    }

    fn test_image_service(storage_path: PathBuf) -> ImageServiceImpl {
        ImageServiceImpl::new_with_options(ImageServiceOptions {
            storage_path,
            ledger_db_path: None,
            storage_driver: "overlay".to_string(),
            storage_options: Vec::new(),
            global_auth_file: None,
            namespaced_auth_dir: None,
            default_transport: "docker://".to_string(),
            short_name_mode: "permissive".to_string(),
            pull_progress_timeout: Duration::from_secs(1),
            max_concurrent_downloads: 2,
            pull_retry_count: 0,
            registry_config_dir: None,
            decryption_keys_path: None,
            decryption_decoder_path: String::new(),
            decryption_keyprovider_config: None,
            additional_artifact_stores: Vec::new(),
            pinned_image_patterns: Vec::new(),
            signature_policy: None,
            signature_policy_dir: None,
            big_files_temporary_dir: None,
            separate_pull_cgroup: String::new(),
            cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
            rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
            disable_cgroup: true,
            pull_cgroup_root: None,
        })
        .expect("image service should be created")
    }
}
