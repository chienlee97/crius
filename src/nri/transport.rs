use std::collections::{HashMap, VecDeque};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_trait::async_trait;
use protobuf::{CodedInputStream, CodedOutputStream, Message};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::iter;
use ttrpc::context;

use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;

const RUNTIME_SERVICE: &str = "nri.pkg.api.v1alpha1.Runtime";
const PLUGIN_SERVICE: &str = "nri.pkg.api.v1alpha1.Plugin";
const PLUGIN_SERVICE_CONN_ID: u32 = 1;
const RUNTIME_SERVICE_CONN_ID: u32 = 2;

type RuntimeServices = HashMap<String, ttrpc::r#async::Service>;

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
    async fn register_plugin(&self, req: nri_api::RegisterPluginRequest) -> Result<nri_api::Empty>;
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

struct MuxConnState {
    read_buf: Mutex<VecDeque<u8>>,
    read_waker: Mutex<Option<Waker>>,
    closed: AtomicBool,
}

impl MuxConnState {
    fn new() -> Self {
        Self {
            read_buf: Mutex::new(VecDeque::new()),
            read_waker: Mutex::new(None),
            closed: AtomicBool::new(false),
        }
    }

    fn push(&self, data: &[u8]) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        let mut read_buf = self.read_buf.lock().unwrap();
        read_buf.extend(data.iter().copied());
        drop(read_buf);
        if let Some(waker) = self.read_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(waker) = self.read_waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

struct MuxWriteFrame {
    conn_id: u32,
    payload: Vec<u8>,
}

struct MuxShared {
    write_tx: mpsc::UnboundedSender<MuxWriteFrame>,
    connections: Mutex<HashMap<u32, Arc<MuxConnState>>>,
    closed: AtomicBool,
}

impl MuxShared {
    fn new(write_tx: mpsc::UnboundedSender<MuxWriteFrame>) -> Arc<Self> {
        Arc::new(Self {
            write_tx,
            connections: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        })
    }

    fn open(self: &Arc<Self>, conn_id: u32) -> MuxConn {
        let state = {
            let mut connections = self.connections.lock().unwrap();
            connections
                .entry(conn_id)
                .or_insert_with(|| Arc::new(MuxConnState::new()))
                .clone()
        };
        MuxConn {
            conn_id,
            shared: self.clone(),
            state,
        }
    }

    fn dispatch(&self, conn_id: u32, payload: Vec<u8>) {
        let state = {
            let connections = self.connections.lock().unwrap();
            connections.get(&conn_id).cloned()
        };
        if let Some(state) = state {
            state.push(&payload);
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let states = {
            let connections = self.connections.lock().unwrap();
            connections.values().cloned().collect::<Vec<_>>()
        };
        for state in states {
            state.close();
        }
    }
}

struct MuxConn {
    conn_id: u32,
    shared: Arc<MuxShared>,
    state: Arc<MuxConnState>,
}

impl AsyncRead for MuxConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut read_buf = self.state.read_buf.lock().unwrap();
        if !read_buf.is_empty() {
            let len = buf.remaining().min(read_buf.len());
            let bytes = read_buf.drain(..len).collect::<Vec<_>>();
            buf.put_slice(&bytes);
            return Poll::Ready(Ok(()));
        }
        if self.state.closed.load(Ordering::SeqCst) || self.shared.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Ok(()));
        }
        *self.state.read_waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for MuxConn {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.shared.closed.load(Ordering::SeqCst) || self.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "multiplexed connection is closed",
            )));
        }
        self.shared
            .write_tx
            .send(MuxWriteFrame {
                conn_id: self.conn_id,
                payload: buf.to_vec(),
            })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "multiplexed trunk writer is closed",
                )
            })?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.state.close();
        Poll::Ready(Ok(()))
    }
}

pub struct MultiplexedRuntimeConnection {
    pub plugin_client: PluginTtrpcClient,
    pub runtime_listener: ttrpc::r#async::transport::Listener,
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

fn build_runtime_services(handler: Arc<dyn RuntimeServiceHandler>) -> RuntimeServices {
    let dispatcher = Arc::new(RuntimeServiceDispatcher { handler });
    let mut methods: HashMap<String, Box<dyn ttrpc::r#async::MethodHandler + Send + Sync>> =
        HashMap::new();
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
    let mut services = HashMap::new();
    services.insert(
        RUNTIME_SERVICE.to_string(),
        ttrpc::r#async::Service {
            methods,
            streams: HashMap::new(),
        },
    );
    services
}

pub fn multiplex_connection(
    stream: StdUnixStream,
    request_timeout: Duration,
) -> Result<MultiplexedRuntimeConnection> {
    stream
        .set_nonblocking(true)
        .map_err(|e| NriError::Transport(e.to_string()))?;
    let stream = UnixStream::from_std(stream).map_err(|e| NriError::Transport(e.to_string()))?;
    let (mut read_half, mut write_half) = stream.into_split();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<MuxWriteFrame>();
    let shared = MuxShared::new(write_tx);

    let writer_shared = shared.clone();
    tokio::spawn(async move {
        while let Some(frame) = write_rx.recv().await {
            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&frame.conn_id.to_be_bytes());
            header[4..].copy_from_slice(&(frame.payload.len() as u32).to_be_bytes());
            if write_half.write_all(&header).await.is_err() {
                writer_shared.close();
                return;
            }
            if write_half.write_all(&frame.payload).await.is_err() {
                writer_shared.close();
                return;
            }
        }
        writer_shared.close();
    });

    let reader_shared = shared.clone();
    tokio::spawn(async move {
        loop {
            let mut header = [0u8; 8];
            if read_half.read_exact(&mut header).await.is_err() {
                reader_shared.close();
                return;
            }
            let conn_id = u32::from_be_bytes(header[..4].try_into().unwrap());
            let payload_len = u32::from_be_bytes(header[4..].try_into().unwrap()) as usize;
            let mut payload = vec![0u8; payload_len];
            if read_half.read_exact(&mut payload).await.is_err() {
                reader_shared.close();
                return;
            }
            reader_shared.dispatch(conn_id, payload);
        }
    });

    let plugin_socket = ttrpc::r#async::transport::Socket::new(shared.open(PLUGIN_SERVICE_CONN_ID));
    let plugin_client =
        PluginTtrpcClient::from_inner(ttrpc::r#async::Client::new(plugin_socket), request_timeout);
    let runtime_listener = ttrpc::r#async::transport::Listener::new(iter(vec![Ok(
        shared.open(RUNTIME_SERVICE_CONN_ID)
    )]));

    Ok(MultiplexedRuntimeConnection {
        plugin_client,
        runtime_listener,
    })
}

pub struct RuntimeTtrpcServer {
    socket_path: String,
    registration_timeout: Duration,
    request_timeout: Duration,
    enable_external_connections: bool,
    handler: Arc<dyn RuntimeServiceHandler>,
    servers: Arc<tokio::sync::Mutex<Vec<ttrpc::r#async::Server>>>,
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
            servers: self.servers.clone(),
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
            servers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.socket_path.is_empty() {
            return Err(NriError::InvalidInput(
                "runtime NRI socket path is empty".to_string(),
            ));
        }

        let server = ttrpc::r#async::Server::new()
            .bind(&self.socket_path)
            .map_err(map_ttrpc_error)?;
        self.start_with_server(server).await
    }

    pub async fn start_with_listener(
        &self,
        listener: ttrpc::r#async::transport::Listener,
    ) -> Result<()> {
        let server = ttrpc::r#async::Server::new().add_listener(listener);
        self.start_with_server(server).await
    }

    async fn start_with_server(&self, server: ttrpc::r#async::Server) -> Result<()> {
        let mut server = server.register_service(build_runtime_services(self.handler.clone()));
        server.start().await.map_err(map_ttrpc_error)?;

        let mut guard = self.servers.lock().await;
        guard.push(server);
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut guard = self.servers.lock().await;
        for server in guard.iter_mut() {
            server.shutdown().await.map_err(map_ttrpc_error)?;
        }
        guard.clear();
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

    pub fn from_inner(client: ttrpc::r#async::Client, request_timeout: Duration) -> Self {
        Self {
            socket_path: String::new(),
            request_timeout,
            client: tokio::sync::Mutex::new(Some(client)),
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
        let client = guard.as_ref().cloned().ok_or_else(|| {
            NriError::Transport("plugin ttRPC client is not connected".to_string())
        })?;
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
        let result = self
            .call_plugin::<_, nri_api::Empty>("Shutdown", &req)
            .await;

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
    use super::*;

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
    async fn multiplex_connection_exposes_runtime_listener_and_plugin_client() {
        let (left, _right) = std::os::unix::net::UnixStream::pair().unwrap();
        let multiplexed =
            multiplex_connection(left, Duration::from_secs(1)).expect("mux setup should succeed");
        let _listener = multiplexed.runtime_listener;
        let req = nri_api::ConfigureRequest::new();
        let _ = tokio::time::timeout(
            Duration::from_millis(10),
            multiplexed.plugin_client.configure(&req),
        )
        .await;
    }
}
