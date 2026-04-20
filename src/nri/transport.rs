use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use protobuf::{CodedInputStream, CodedOutputStream, Message};
use ttrpc::context;

use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;

const RUNTIME_SERVICE: &str = "nri.pkg.api.v1alpha1.Runtime";
const PLUGIN_SERVICE: &str = "nri.pkg.api.v1alpha1.Plugin";

fn map_ttrpc_error(err: ttrpc::Error) -> NriError {
    NriError::Transport(err.to_string())
}

fn request_context(timeout: Duration) -> context::Context {
    if timeout.is_zero() {
        context::Context::default()
    } else {
        context::with_duration(timeout)
    }
}

#[async_trait]
pub trait RuntimeServiceHandler: Send + Sync {
    async fn register_plugin(
        &self,
        req: nri_api::RegisterPluginRequest,
    ) -> Result<nri_api::Empty>;
    async fn update_containers(
        &self,
        req: nri_api::UpdateContainersRequest,
    ) -> Result<nri_api::UpdateContainersResponse>;
}

#[derive(Debug, Default)]
struct NopRuntimeServiceHandler;

#[async_trait]
impl RuntimeServiceHandler for NopRuntimeServiceHandler {
    async fn register_plugin(
        &self,
        _req: nri_api::RegisterPluginRequest,
    ) -> Result<nri_api::Empty> {
        Ok(nri_api::Empty::new())
    }

    async fn update_containers(
        &self,
        _req: nri_api::UpdateContainersRequest,
    ) -> Result<nri_api::UpdateContainersResponse> {
        Ok(nri_api::UpdateContainersResponse::new())
    }
}

struct RuntimeServiceDispatcher {
    handler: Arc<dyn RuntimeServiceHandler>,
}

struct RegisterPluginMethod {
    dispatcher: Arc<RuntimeServiceDispatcher>,
}

struct UpdateContainersMethod {
    dispatcher: Arc<RuntimeServiceDispatcher>,
}

#[async_trait]
impl ttrpc::r#async::MethodHandler for RegisterPluginMethod {
    async fn handler(
        &self,
        _ctx: ttrpc::r#async::TtrpcContext,
        req: ttrpc::Request,
    ) -> ttrpc::Result<ttrpc::Response> {
        let request = decode_message::<nri_api::RegisterPluginRequest>(&req.payload)?;
        let response = self
            .dispatcher
            .handler
            .register_plugin(request)
            .await
            .map_err(|e| ttrpc::Error::Others(e.to_string()))?;
        encode_ok_response(&response)
    }
}

#[async_trait]
impl ttrpc::r#async::MethodHandler for UpdateContainersMethod {
    async fn handler(
        &self,
        _ctx: ttrpc::r#async::TtrpcContext,
        req: ttrpc::Request,
    ) -> ttrpc::Result<ttrpc::Response> {
        let request = decode_message::<nri_api::UpdateContainersRequest>(&req.payload)?;
        let response = self
            .dispatcher
            .handler
            .update_containers(request)
            .await
            .map_err(|e| ttrpc::Error::Others(e.to_string()))?;
        encode_ok_response(&response)
    }
}

pub struct RuntimeTtrpcServer {
    socket_path: String,
    registration_timeout: Duration,
    request_timeout: Duration,
    enable_external_connections: bool,
    handler: Arc<dyn RuntimeServiceHandler>,
    server: tokio::sync::Mutex<Option<ttrpc::r#async::Server>>,
}

impl std::fmt::Debug for RuntimeTtrpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeTtrpcServer")
            .field("socket_path", &self.socket_path)
            .field("registration_timeout", &self.registration_timeout)
            .field("request_timeout", &self.request_timeout)
            .field(
                "enable_external_connections",
                &self.enable_external_connections,
            )
            .finish()
    }
}

impl Clone for RuntimeTtrpcServer {
    fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            registration_timeout: self.registration_timeout,
            request_timeout: self.request_timeout,
            enable_external_connections: self.enable_external_connections,
            handler: self.handler.clone(),
            server: tokio::sync::Mutex::new(None),
        }
    }
}

impl RuntimeTtrpcServer {
    pub fn new(
        socket_path: String,
        registration_timeout: Duration,
        request_timeout: Duration,
        enable_external_connections: bool,
    ) -> Self {
        Self::with_handler(
            socket_path,
            registration_timeout,
            request_timeout,
            enable_external_connections,
            Arc::new(NopRuntimeServiceHandler),
        )
    }

    pub fn with_handler(
        socket_path: String,
        registration_timeout: Duration,
        request_timeout: Duration,
        enable_external_connections: bool,
        handler: Arc<dyn RuntimeServiceHandler>,
    ) -> Self {
        Self {
            socket_path,
            registration_timeout,
            request_timeout,
            enable_external_connections,
            handler,
            server: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.socket_path.is_empty() {
            return Err(NriError::InvalidInput(
                "runtime NRI socket path is empty".to_string(),
            ));
        }

        let mut server = ttrpc::r#async::Server::new()
            .bind(&self.socket_path)
            .map_err(map_ttrpc_error)?;

        let dispatcher = Arc::new(RuntimeServiceDispatcher {
            handler: self.handler.clone(),
        });
        let mut methods: std::collections::HashMap<
            String,
            Box<dyn ttrpc::r#async::MethodHandler + Send + Sync>,
        > = std::collections::HashMap::new();
        methods.insert(
            "RegisterPlugin".to_string(),
            Box::new(RegisterPluginMethod {
                dispatcher: dispatcher.clone(),
            }),
        );
        methods.insert(
            "UpdateContainers".to_string(),
            Box::new(UpdateContainersMethod { dispatcher }),
        );
        let mut services = std::collections::HashMap::new();
        services.insert(
            RUNTIME_SERVICE.to_string(),
            ttrpc::r#async::Service {
                methods,
                streams: std::collections::HashMap::new(),
            },
        );
        server = server.register_service(services);
        server.start().await.map_err(map_ttrpc_error)?;

        let mut guard = self.server.lock().await;
        *guard = Some(server);
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut guard = self.server.lock().await;
        if let Some(server) = guard.as_mut() {
            server.shutdown().await.map_err(map_ttrpc_error)?;
        }
        *guard = None;
        Ok(())
    }
}

pub struct PluginTtrpcClient {
    socket_path: String,
    request_timeout: Duration,
    client: tokio::sync::Mutex<Option<ttrpc::r#async::Client>>,
}

impl std::fmt::Debug for PluginTtrpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginTtrpcClient")
            .field("socket_path", &self.socket_path)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl PluginTtrpcClient {
    pub fn new(socket_path: String, request_timeout: Duration) -> Self {
        Self {
            socket_path,
            request_timeout,
            client: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn connect(&self) -> Result<()> {
        if self.socket_path.is_empty() {
            return Err(NriError::InvalidInput(
                "plugin NRI socket path is empty".to_string(),
            ));
        }
        let inner = ttrpc::r#async::Client::connect(&self.socket_path)
            .await
            .map_err(map_ttrpc_error)?;
        let mut guard = self.client.lock().await;
        *guard = Some(inner);
        Ok(())
    }

    async fn with_client<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(ttrpc::r#async::Client) -> Fut,
        Fut: std::future::Future<Output = ttrpc::Result<T>>,
    {
        let guard = self.client.lock().await;
        let client = guard
            .as_ref()
            .cloned()
            .ok_or_else(|| NriError::Transport("plugin ttRPC client is not connected".to_string()))?;
        drop(guard);
        f(client).await.map_err(map_ttrpc_error)
    }

    pub async fn configure(
        &self,
        req: &nri_api::ConfigureRequest,
    ) -> Result<nri_api::ConfigureResponse> {
        self.call_plugin("Configure", req).await
    }

    pub async fn synchronize(
        &self,
        req: &nri_api::SynchronizeRequest,
    ) -> Result<nri_api::SynchronizeResponse> {
        self.call_plugin("Synchronize", req).await
    }

    pub async fn create_container(
        &self,
        req: &nri_api::CreateContainerRequest,
    ) -> Result<nri_api::CreateContainerResponse> {
        self.call_plugin("CreateContainer", req).await
    }

    pub async fn update_container(
        &self,
        req: &nri_api::UpdateContainerRequest,
    ) -> Result<nri_api::UpdateContainerResponse> {
        self.call_plugin("UpdateContainer", req).await
    }

    pub async fn stop_container(
        &self,
        req: &nri_api::StopContainerRequest,
    ) -> Result<nri_api::StopContainerResponse> {
        self.call_plugin("StopContainer", req).await
    }

    pub async fn update_pod_sandbox(
        &self,
        req: &nri_api::UpdatePodSandboxRequest,
    ) -> Result<nri_api::UpdatePodSandboxResponse> {
        self.call_plugin("UpdatePodSandbox", req).await
    }

    pub async fn state_change(&self, req: &nri_api::StateChangeEvent) -> Result<nri_api::Empty> {
        self.call_plugin("StateChange", req).await
    }

    pub async fn validate_container_adjustment(
        &self,
        req: &nri_api::ValidateContainerAdjustmentRequest,
    ) -> Result<nri_api::ValidateContainerAdjustmentResponse> {
        self.call_plugin("ValidateContainerAdjustment", req).await
    }

    async fn call_plugin<Req, Resp>(&self, method: &str, req: &Req) -> Result<Resp>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let service = PLUGIN_SERVICE.to_string();
        let method_name = method.to_string();
        let payload = encode_message(req).map_err(map_ttrpc_error)?;
        let ctx = request_context(self.request_timeout);
        self.with_client(move |client| async move {
            let request = ttrpc::Request {
                service,
                method: method_name,
                timeout_nano: ctx.timeout_nano,
                metadata: ttrpc::context::to_pb(ctx.metadata),
                payload,
                ..Default::default()
            };
            let response = client.request(request).await?;
            decode_message::<Resp>(&response.payload)
        })
        .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let req = nri_api::Empty::new();
        let result = self.call_plugin::<_, nri_api::Empty>("Shutdown", &req).await;

        let mut guard = self.client.lock().await;
        *guard = None;
        result.map(|_| ())
    }
}

fn encode_message<T: Message>(message: &T) -> ttrpc::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(message.compute_size() as usize);
    {
        let mut stream = CodedOutputStream::vec(&mut payload);
        message
            .write_to(&mut stream)
            .map_err(|e| ttrpc::Error::Others(e.to_string()))?;
        stream
            .flush()
            .map_err(|e| ttrpc::Error::Others(e.to_string()))?;
    }
    Ok(payload)
}

fn decode_message<T: Message + Default>(payload: &[u8]) -> ttrpc::Result<T> {
    let mut message = T::default();
    let mut stream = CodedInputStream::from_bytes(payload);
    message
        .merge_from(&mut stream)
        .map_err(|e| ttrpc::Error::Others(e.to_string()))?;
    Ok(message)
}

fn encode_ok_response<T: Message>(message: &T) -> ttrpc::Result<ttrpc::Response> {
    let mut response = ttrpc::Response::new();
    response.set_status(ttrpc::get_status(ttrpc::Code::OK, "".to_string()));
    response.payload = encode_message(message)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct RecordingHandler {
        registered: Mutex<Vec<String>>,
        updates: Mutex<usize>,
    }

    #[async_trait]
    impl RuntimeServiceHandler for RecordingHandler {
        async fn register_plugin(
            &self,
            req: nri_api::RegisterPluginRequest,
        ) -> Result<nri_api::Empty> {
            self.registered
                .lock()
                .await
                .push(format!("{}:{}", req.plugin_name, req.plugin_idx));
            Ok(nri_api::Empty::new())
        }

        async fn update_containers(
            &self,
            _req: nri_api::UpdateContainersRequest,
        ) -> Result<nri_api::UpdateContainersResponse> {
            let mut updates = self.updates.lock().await;
            *updates += 1;
            Ok(nri_api::UpdateContainersResponse::new())
        }
    }

    #[tokio::test]
    async fn runtime_server_rejects_empty_socket() {
        let server = RuntimeTtrpcServer::new(
            String::new(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            false,
        );
        let err = server.start().await.expect_err("empty socket should fail");
        match err {
            NriError::InvalidInput(msg) => assert!(msg.contains("socket path")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn plugin_client_requires_connection_before_rpc() {
        let client = PluginTtrpcClient::new(
            "unix:///tmp/crius-test-nri.sock".to_string(),
            Duration::from_secs(1),
        );
        let req = nri_api::ConfigureRequest::new();
        let err = client
            .configure(&req)
            .await
            .expect_err("unconnected client should fail");
        match err {
            NriError::Transport(msg) => assert!(msg.contains("not connected")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn runtime_server_starts_and_stops() {
        let socket = format!("unix:///tmp/crius-nri-{}.sock", std::process::id());
        let handler = Arc::new(RecordingHandler::default());
        let server = RuntimeTtrpcServer::with_handler(
            socket,
            Duration::from_secs(1),
            Duration::from_secs(1),
            false,
            handler,
        );
        server.start().await.expect("server should start");
        server.shutdown().await.expect("server should stop");
    }
}
