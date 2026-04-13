pub mod spdy;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream as StdTcpStream, ToSocketAddrs};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use hyper::body::Body;
use hyper::header::{
    CONNECTION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
    SEC_WEBSOCKET_VERSION, UPGRADE,
};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Method, Request, Response, Server, StatusCode};
use nix::pty::openpty;
use nix::sched::CloneFlags;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::process::Command as TokioCommand;
use tokio::sync::Mutex;

use crate::attach::{AttachOutputDecoder, ATTACH_PIPE_STDERR, ATTACH_PIPE_STDOUT};
use crate::proto::runtime::v1::{
    AttachRequest, AttachResponse, ExecRequest, ExecResponse, PortForwardRequest,
    PortForwardResponse,
};

const PORT_FORWARD_PROTOCOL_V1: &str = "portforward.k8s.io";
const PORT_FORWARD_WS_PROTOCOL_V4_BINARY: &str = "v4.channel.k8s.io";
const PORT_FORWARD_WS_PROTOCOL_V4_BASE64: &str = "v4.base64.channel.k8s.io";
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const WS_CHANNEL_STDIN: u8 = 0;
const WS_CHANNEL_STDOUT: u8 = 1;
const WS_CHANNEL_STDERR: u8 = 2;
const WS_CHANNEL_ERROR: u8 = 3;
const WS_CHANNEL_RESIZE: u8 = 4;
const STREAMING_REQUEST_TTL: Duration = Duration::from_secs(30);
const PORT_FORWARD_STREAM_CREATION_TIMEOUT: Duration = Duration::from_secs(30);
const PORT_FORWARD_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);

#[derive(Debug, Clone)]
enum StreamingRequest {
    Exec(ExecRequest),
    Attach(AttachRequest),
    AttachLog(AttachLogRequestContext),
    PortForward(PortForwardRequestContext),
}

#[derive(Debug, Clone)]
struct CachedStreamingRequest {
    created_at: Instant,
    request: StreamingRequest,
}

#[derive(Debug, Clone)]
struct PortForwardRequestContext {
    req: PortForwardRequest,
    netns_path: PathBuf,
}

#[derive(Debug, Clone)]
struct AttachLogRequestContext {
    req: AttachRequest,
    log_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct StreamingServer {
    cache: Arc<Mutex<HashMap<String, CachedStreamingRequest>>>,
    base_url: String,
}

impl StreamingServer {
    #[cfg(test)]
    pub(crate) fn for_test(base_url: impl Into<String>) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            base_url: base_url.into(),
        }
    }

    pub async fn start(bind_addr: &str, runtime_path: PathBuf) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let service_cache = cache.clone();
        let service_runtime_path = runtime_path.clone();

        let make_service = make_service_fn(move |_| {
            let cache = service_cache.clone();
            let runtime_path = service_runtime_path.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let cache = cache.clone();
                    let runtime_path = runtime_path.clone();
                    async move { Ok::<_, Infallible>(handle_request(cache, runtime_path, req).await) }
                }))
            }
        });

        let server = Server::from_tcp(listener)?.serve(make_service);
        tokio::spawn(async move {
            if let Err(e) = server.await {
                log::error!("Streaming server exited: {}", e);
            }
        });

        Ok(Self {
            cache,
            base_url: format!("http://{}", local_addr),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn get_exec(&self, req: &ExecRequest) -> Result<ExecResponse, tonic::Status> {
        Self::validate_exec_request(req)?;
        let token = self
            .insert_request(StreamingRequest::Exec(req.clone()))
            .await;
        Ok(ExecResponse {
            url: format!("{}/exec/{}", self.base_url, token),
        })
    }

    pub async fn get_attach(&self, req: &AttachRequest) -> Result<AttachResponse, tonic::Status> {
        Self::validate_attach_request(req)?;
        let token = self
            .insert_request(StreamingRequest::Attach(req.clone()))
            .await;
        Ok(AttachResponse {
            url: format!("{}/attach/{}", self.base_url, token),
        })
    }

    pub async fn get_attach_log(
        &self,
        req: &AttachRequest,
        log_path: PathBuf,
    ) -> Result<AttachResponse, tonic::Status> {
        Self::validate_attach_request(req)?;
        let token = self
            .insert_request(StreamingRequest::AttachLog(AttachLogRequestContext {
                req: req.clone(),
                log_path,
            }))
            .await;
        Ok(AttachResponse {
            url: format!("{}/attach/{}", self.base_url, token),
        })
    }

    pub async fn get_port_forward(
        &self,
        req: &PortForwardRequest,
        netns_path: PathBuf,
    ) -> Result<PortForwardResponse, tonic::Status> {
        Self::validate_port_forward_request(req)?;
        let token = self
            .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
                req: req.clone(),
                netns_path,
            }))
            .await;
        Ok(PortForwardResponse {
            url: format!("{}/portforward/{}", self.base_url, token),
        })
    }

    async fn insert_request(&self, request: StreamingRequest) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        let mut cache = self.cache.lock().await;
        Self::prune_expired_requests_locked(&mut cache);
        cache.insert(
            token.clone(),
            CachedStreamingRequest {
                created_at: Instant::now(),
                request,
            },
        );
        token
    }

    fn prune_expired_requests_locked(cache: &mut HashMap<String, CachedStreamingRequest>) {
        let now = Instant::now();
        cache.retain(|_, entry| now.duration_since(entry.created_at) <= STREAMING_REQUEST_TTL);
    }

    #[cfg(test)]
    async fn insert_request_for_test(&self, request: StreamingRequest, age: Duration) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        let mut cache = self.cache.lock().await;
        cache.insert(
            token.clone(),
            CachedStreamingRequest {
                created_at: Instant::now() - age,
                request,
            },
        );
        token
    }

    fn validate_exec_request(req: &ExecRequest) -> Result<(), tonic::Status> {
        if req.container_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "missing required container_id",
            ));
        }
        if req.tty && req.stderr {
            return Err(tonic::Status::invalid_argument(
                "tty and stderr cannot both be true",
            ));
        }
        if req.cmd.is_empty() {
            return Err(tonic::Status::invalid_argument("cmd must not be empty"));
        }
        if !req.stdin && !req.stdout && !req.stderr {
            return Err(tonic::Status::invalid_argument(
                "one of stdin, stdout, or stderr must be set",
            ));
        }
        Ok(())
    }

    fn validate_attach_request(req: &AttachRequest) -> Result<(), tonic::Status> {
        if req.container_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "missing required container_id",
            ));
        }
        if req.tty && req.stderr {
            return Err(tonic::Status::invalid_argument(
                "tty and stderr cannot both be true",
            ));
        }
        if !req.stdin && !req.stdout && !req.stderr {
            return Err(tonic::Status::invalid_argument(
                "one of stdin, stdout, or stderr must be set",
            ));
        }
        Ok(())
    }

    fn validate_port_forward_request(req: &PortForwardRequest) -> Result<(), tonic::Status> {
        if req.pod_sandbox_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "missing required pod_sandbox_id",
            ));
        }
        if let Some(port) = req
            .port
            .iter()
            .copied()
            .find(|port| *port <= 0 || *port > 65535)
        {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid forwarded port {}",
                port
            )));
        }

        Ok(())
    }
}

async fn handle_request(
    cache: Arc<Mutex<HashMap<String, CachedStreamingRequest>>>,
    runtime_path: PathBuf,
    req: Request<Body>,
) -> Response<Body> {
    if req.method() != Method::GET && req.method() != Method::POST {
        return response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }

    let path = req.uri().path().trim_matches('/');
    let mut parts = path.split('/');
    let action = match parts.next() {
        Some(value) if !value.is_empty() => value,
        _ => return response(StatusCode::NOT_FOUND, "streaming token not found"),
    };
    let token = match parts.next() {
        Some(value) if !value.is_empty() => value,
        _ => return response(StatusCode::NOT_FOUND, "streaming token not found"),
    };

    let request = {
        let mut cache = cache.lock().await;
        StreamingServer::prune_expired_requests_locked(&mut cache);
        cache.remove(token)
    };

    match (action, request.map(|entry| entry.request)) {
        ("exec", Some(StreamingRequest::Exec(exec_req))) => {
            if is_websocket_upgrade_request(&req) {
                let Some(protocol) = negotiate_remotecommand_websocket_protocol(&req) else {
                    return response(
                        StatusCode::FORBIDDEN,
                        "no supported Sec-WebSocket-Protocol was requested",
                    );
                };

                let response = websocket_switching_response(&req, protocol);
                let on_upgrade = hyper::upgrade::on(req);
                tokio::spawn(async move {
                    if let Err(e) =
                        serve_exec_websocket(on_upgrade, exec_req, runtime_path, protocol).await
                    {
                        log::error!("Exec websocket session failed: {}", e);
                    }
                });

                return response;
            }
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "exec requires SPDY or websocket upgrade headers",
                );
            }

            let Some(protocol) = negotiate_remotecommand_protocol(&req) else {
                return response(
                    StatusCode::FORBIDDEN,
                    "no supported X-Stream-Protocol-Version was requested",
                );
            };

            let on_upgrade = hyper::upgrade::on(req);
            tokio::spawn(async move {
                if let Err(e) = serve_exec_spdy(on_upgrade, exec_req, runtime_path, protocol).await
                {
                    log::error!("Exec SPDY session failed: {}", e);
                }
            });

            Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(CONNECTION, "Upgrade")
                .header(UPGRADE, spdy::SPDY_31)
                .header("X-Stream-Protocol-Version", protocol)
                .body(Body::empty())
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
        ("attach", Some(StreamingRequest::Attach(attach_req))) => {
            if is_websocket_upgrade_request(&req) {
                let Some(protocol) = negotiate_remotecommand_websocket_protocol(&req) else {
                    return response(
                        StatusCode::FORBIDDEN,
                        "no supported Sec-WebSocket-Protocol was requested",
                    );
                };

                let response = websocket_switching_response(&req, protocol);
                let on_upgrade = hyper::upgrade::on(req);
                tokio::spawn(async move {
                    if let Err(e) = serve_attach_websocket(on_upgrade, attach_req, protocol).await {
                        log::error!("Attach websocket session failed: {}", e);
                    }
                });

                return response;
            }
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "attach requires SPDY or websocket upgrade headers",
                );
            }

            let Some(protocol) = negotiate_remotecommand_protocol(&req) else {
                return response(
                    StatusCode::FORBIDDEN,
                    "no supported X-Stream-Protocol-Version was requested",
                );
            };

            let on_upgrade = hyper::upgrade::on(req);
            tokio::spawn(async move {
                if let Err(e) = serve_attach_spdy(on_upgrade, attach_req, protocol).await {
                    log::error!("Attach SPDY session failed: {}", e);
                }
            });

            Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(CONNECTION, "Upgrade")
                .header(UPGRADE, spdy::SPDY_31)
                .header("X-Stream-Protocol-Version", protocol)
                .body(Body::empty())
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
        ("attach", Some(StreamingRequest::AttachLog(attach_log_ctx))) => {
            if is_websocket_upgrade_request(&req) {
                let Some(protocol) = negotiate_remotecommand_websocket_protocol(&req) else {
                    return response(
                        StatusCode::FORBIDDEN,
                        "no supported Sec-WebSocket-Protocol was requested",
                    );
                };

                let response = websocket_switching_response(&req, protocol);
                let on_upgrade = hyper::upgrade::on(req);
                tokio::spawn(async move {
                    if let Err(e) =
                        serve_attach_log_websocket(on_upgrade, attach_log_ctx, protocol).await
                    {
                        log::error!("Attach log websocket session failed: {}", e);
                    }
                });

                return response;
            }
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "attach requires SPDY or websocket upgrade headers",
                );
            }

            let Some(protocol) = negotiate_remotecommand_protocol(&req) else {
                return response(
                    StatusCode::FORBIDDEN,
                    "no supported X-Stream-Protocol-Version was requested",
                );
            };

            let on_upgrade = hyper::upgrade::on(req);
            tokio::spawn(async move {
                if let Err(e) = serve_attach_log_spdy(on_upgrade, attach_log_ctx, protocol).await {
                    log::error!("Attach log fallback session failed: {}", e);
                }
            });

            Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(CONNECTION, "Upgrade")
                .header(UPGRADE, spdy::SPDY_31)
                .header("X-Stream-Protocol-Version", protocol)
                .body(Body::empty())
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
        ("portforward", Some(StreamingRequest::PortForward(port_forward_ctx))) => {
            if is_websocket_upgrade_request(&req) {
                let Some(protocol) = negotiate_portforward_websocket_protocol(&req) else {
                    return response(
                        StatusCode::FORBIDDEN,
                        "no supported Sec-WebSocket-Protocol was requested",
                    );
                };

                let response = websocket_switching_response(&req, protocol);
                let on_upgrade = hyper::upgrade::on(req);
                tokio::spawn(async move {
                    if let Err(e) =
                        serve_portforward_websocket(on_upgrade, port_forward_ctx, protocol).await
                    {
                        log::error!("Port-forward websocket session failed: {}", e);
                    }
                });

                return response;
            }
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "portforward requires SPDY or websocket upgrade headers",
                );
            }

            let Some(protocol) = negotiate_portforward_protocol(&req) else {
                return response(
                    StatusCode::FORBIDDEN,
                    "no supported port-forward protocol was requested",
                );
            };

            let on_upgrade = hyper::upgrade::on(req);
            tokio::spawn(async move {
                if let Err(e) = serve_portforward_spdy(on_upgrade, port_forward_ctx).await {
                    log::error!("Port-forward SPDY session failed: {}", e);
                }
            });

            Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(CONNECTION, "Upgrade")
                .header(UPGRADE, spdy::SPDY_31)
                .header("X-Stream-Protocol-Version", protocol)
                .body(Body::empty())
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
        ("exec", Some(_)) | ("attach", Some(_)) | ("portforward", Some(_)) => {
            response(StatusCode::BAD_REQUEST, "streaming token kind mismatch")
        }
        _ => response(StatusCode::NOT_FOUND, "streaming token not found"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttachStreamRole {
    Error,
    Stdin,
    Stdout,
    Stderr,
    Resize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecStreamRole {
    Error,
    Stdin,
    Stdout,
    Stderr,
    Resize,
}

enum ExecConsoleEvent {
    Data(Vec<u8>),
    Eof,
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortForwardStreamRole {
    Data,
    Error,
}

#[derive(Debug, Clone)]
struct PortForwardPair {
    request_id: String,
    port: u16,
    data_stream: Option<spdy::StreamId>,
    error_stream: Option<spdy::StreamId>,
    created_at: Instant,
}

#[derive(Debug)]
struct ParsedPortForwardStream {
    role: PortForwardStreamRole,
    request_id: String,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PortForwardStreamParseError {
    request_id: Option<String>,
    message: String,
}

impl std::fmt::Display for PortForwardStreamParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PortForwardStreamParseError {}

#[derive(Debug)]
enum PortForwardStreamRegistration {
    Pending,
    Ready(PortForwardPair),
    RejectCurrent(String),
    RejectPendingPair {
        pair: PortForwardPair,
        message: String,
    },
}

type StreamActivity = Arc<std::sync::Mutex<Instant>>;

#[derive(Debug, Deserialize)]
struct TerminalSizePayload {
    #[serde(alias = "Width")]
    width: u16,
    #[serde(alias = "Height")]
    height: u16,
}

#[derive(Debug)]
struct WebSocketFrame {
    opcode: u8,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortForwardWebsocketProtocol {
    Legacy,
    V4Binary,
    V4Base64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttachLogRecord {
    stream: String,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct AttachLogFollowState {
    initialized: bool,
    start_from_end_on_next_open: bool,
    read_offset: usize,
    pending: Vec<u8>,
}

impl Default for AttachLogFollowState {
    fn default() -> Self {
        Self {
            initialized: false,
            start_from_end_on_next_open: true,
            read_offset: 0,
            pending: Vec::new(),
        }
    }
}

fn split_once_byte(input: &[u8], needle: u8) -> Option<(&[u8], &[u8])> {
    let pos = input.iter().position(|byte| *byte == needle)?;
    Some((&input[..pos], &input[pos + 1..]))
}

fn parse_cri_text_log_record(line: &[u8]) -> anyhow::Result<AttachLogRecord> {
    let (timestamp, rest) =
        split_once_byte(line, b' ').ok_or_else(|| anyhow::anyhow!("missing timestamp"))?;
    let (stream, rest) =
        split_once_byte(rest, b' ').ok_or_else(|| anyhow::anyhow!("missing stream"))?;
    let (tag, content) = split_once_byte(rest, b' ').unwrap_or((rest, &[][..]));

    let timestamp = std::str::from_utf8(timestamp)?;
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map_err(|e| anyhow::anyhow!("invalid CRI log timestamp: {}", e))?;

    let stream = std::str::from_utf8(stream)?;
    if stream != "stdout" && stream != "stderr" {
        return Err(anyhow::anyhow!("unsupported CRI log stream {}", stream));
    }

    let tag = std::str::from_utf8(tag)?;
    // Match kubelet's CRI log reader: only the first tag decides whether this
    // record is partial, and unknown future tags are treated as full.
    let is_partial = tag.split(':').next().unwrap_or_default() == "P";

    let mut payload = content.to_vec();
    if is_partial {
        if matches!(payload.last(), Some(b'\n')) {
            payload.pop();
            if matches!(payload.last(), Some(b'\r')) {
                payload.pop();
            }
        }
    } else {
        payload.push(b'\n');
    }

    Ok(AttachLogRecord {
        stream: stream.to_string(),
        payload,
    })
}

async fn read_attach_log_records(
    log_path: &Path,
    state: &mut AttachLogFollowState,
) -> std::io::Result<Vec<AttachLogRecord>> {
    match tokio::fs::read(log_path).await {
        Ok(bytes) => {
            if !state.initialized {
                state.initialized = true;
                if state.start_from_end_on_next_open {
                    state.read_offset = bytes.len();
                    state.start_from_end_on_next_open = false;
                    return Ok(Vec::new());
                }
            }

            if bytes.len() < state.read_offset {
                state.read_offset = 0;
                state.pending.clear();
            }

            if bytes.len() > state.read_offset {
                state.pending.extend_from_slice(&bytes[state.read_offset..]);
                state.read_offset = bytes.len();
            }

            let mut records = Vec::new();
            while let Some(pos) = state.pending.iter().position(|byte| *byte == b'\n') {
                let mut line = state.pending.drain(..=pos).collect::<Vec<u8>>();
                line.pop();
                if line.is_empty() {
                    continue;
                }

                match parse_cri_text_log_record(&line) {
                    Ok(record) => records.push(record),
                    Err(err) => {
                        log::debug!("Skipping malformed attach-log record: {}", err);
                    }
                }
            }

            Ok(records)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            state.initialized = false;
            state.start_from_end_on_next_open = false;
            state.read_offset = 0;
            state.pending.clear();
            Ok(Vec::new())
        }
        Err(err) => Err(err),
    }
}

fn expected_attach_roles(req: &AttachRequest) -> Vec<AttachStreamRole> {
    let mut roles = vec![AttachStreamRole::Error];
    if req.stdin {
        roles.push(AttachStreamRole::Stdin);
    }
    if req.stdout {
        roles.push(AttachStreamRole::Stdout);
    }
    if req.stderr && !req.tty {
        roles.push(AttachStreamRole::Stderr);
    }
    roles
}

fn expected_exec_roles(req: &ExecRequest) -> Vec<ExecStreamRole> {
    let mut roles = vec![ExecStreamRole::Error];
    if req.stdin {
        roles.push(ExecStreamRole::Stdin);
    }
    if req.stdout {
        roles.push(ExecStreamRole::Stdout);
    }
    if req.stderr && !req.tty {
        roles.push(ExecStreamRole::Stderr);
    }
    roles
}

fn shim_work_dir() -> PathBuf {
    crate::runtime::default_shim_work_dir()
}

fn shim_socket_path(container_id: &str, socket_name: &str) -> PathBuf {
    shim_work_dir().join(container_id).join(socket_name)
}

fn parse_terminal_size(payload: &[u8]) -> anyhow::Result<(u16, u16)> {
    let size: TerminalSizePayload = serde_json::from_slice(payload)
        .map_err(|e| anyhow::anyhow!("invalid resize payload: {}", e))?;
    if size.width == 0 || size.height == 0 {
        return Err(anyhow::anyhow!(
            "terminal width and height must be greater than zero"
        ));
    }
    Ok((size.width, size.height))
}

fn apply_terminal_resize(tty: &File, width: u16, height: u16) -> anyhow::Result<()> {
    let winsize = nix::libc::winsize {
        ws_row: height,
        ws_col: width,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let rc = unsafe { nix::libc::ioctl(tty.as_raw_fd(), nix::libc::TIOCSWINSZ, &winsize) };
    if rc < 0 {
        return Err(anyhow::anyhow!(
            "failed to apply terminal resize: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

async fn write_error_stream(
    writer: &Arc<Mutex<spdy::AsyncSpdyWriter<tokio::io::WriteHalf<hyper::upgrade::Upgraded>>>>,
    error_stream: Option<spdy::StreamId>,
    message: &str,
) -> anyhow::Result<()> {
    if let Some(stream_id) = error_stream {
        writer
            .lock()
            .await
            .write_data(stream_id, message.as_bytes(), false)
            .await?;
    }
    Ok(())
}

fn sha1_digest(input: &[u8]) -> [u8; 20] {
    fn left_rotate(value: u32, bits: u32) -> u32 {
        (value << bits) | (value >> (32 - bits))
    }

    let mut message = input.to_vec();
    let bit_len = (message.len() as u64) * 8;
    message.push(0x80);
    while (message.len() % 64) != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    let mut h0: u32 = 0x6745_2301;
    let mut h1: u32 = 0xEFCD_AB89;
    let mut h2: u32 = 0x98BA_DCFE;
    let mut h3: u32 = 0x1032_5476;
    let mut h4: u32 = 0xC3D2_E1F0;

    for chunk in message.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).take(16).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = left_rotate(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for (i, word) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => (((b & c) | ((!b) & d)), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => (((b & c) | (b & d) | (c & d)), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = left_rotate(a, 5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = left_rotate(b, 30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

fn websocket_accept_value(key: &str) -> String {
    let mut payload = key.as_bytes().to_vec();
    payload.extend_from_slice(WS_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(sha1_digest(&payload))
}

fn is_websocket_upgrade_request(req: &Request<Body>) -> bool {
    let connection = req
        .headers()
        .get(CONNECTION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let upgrade = req
        .headers()
        .get(UPGRADE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let version = req
        .headers()
        .get(SEC_WEBSOCKET_VERSION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    connection.contains("upgrade")
        && upgrade == "websocket"
        && version == "13"
        && req.headers().contains_key(SEC_WEBSOCKET_KEY)
}

fn negotiate_remotecommand_websocket_protocol(req: &Request<Body>) -> Option<&'static str> {
    let requested: Vec<String> = req
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    for candidate in [
        "v5.channel.k8s.io",
        "v4.channel.k8s.io",
        "v3.channel.k8s.io",
        "v2.channel.k8s.io",
        "channel.k8s.io",
    ] {
        if requested.iter().any(|value| value == candidate) {
            return Some(candidate);
        }
    }
    None
}

fn websocket_switching_response(req: &Request<Body>, protocol: &str) -> Response<Body> {
    let accept_value = req
        .headers()
        .get(SEC_WEBSOCKET_KEY)
        .and_then(|value| value.to_str().ok())
        .map(websocket_accept_value)
        .unwrap_or_default();

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_ACCEPT, accept_value)
        .header(SEC_WEBSOCKET_PROTOCOL, protocol)
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

async fn read_websocket_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<Option<WebSocketFrame>> {
    let mut header = [0u8; 2];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }

    let opcode = header[0] & 0x0f;
    let masked = (header[1] & 0x80) != 0;
    let mut payload_len = (header[1] & 0x7f) as u64;
    if payload_len == 126 {
        let mut extended = [0u8; 2];
        reader.read_exact(&mut extended).await?;
        payload_len = u16::from_be_bytes(extended) as u64;
    } else if payload_len == 127 {
        let mut extended = [0u8; 8];
        reader.read_exact(&mut extended).await?;
        payload_len = u64::from_be_bytes(extended);
    }

    let mut masking_key = [0u8; 4];
    if masked {
        reader.read_exact(&mut masking_key).await?;
    }

    let mut payload = vec![0u8; payload_len as usize];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= masking_key[index % 4];
        }
    }

    Ok(Some(WebSocketFrame { opcode, payload }))
}

async fn write_websocket_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    opcode: u8,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut header = vec![0x80 | (opcode & 0x0f)];
    if payload.len() < 126 {
        header.push(payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        header.push(126);
        header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        header.push(127);
        header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }

    writer.write_all(&header).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    writer.flush().await?;
    Ok(())
}

async fn write_websocket_channel_frame<W: AsyncWriteExt + Unpin>(
    writer: &Arc<Mutex<W>>,
    channel: u8,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut frame = Vec::with_capacity(1 + payload.len());
    frame.push(channel);
    frame.extend_from_slice(payload);
    write_websocket_frame(&mut *writer.lock().await, 0x2, &frame).await
}

async fn write_portforward_websocket_channel_frame<W: AsyncWriteExt + Unpin>(
    writer: &Arc<Mutex<W>>,
    activity: &StreamActivity,
    protocol: PortForwardWebsocketProtocol,
    channel: u8,
    payload: &[u8],
) -> anyhow::Result<()> {
    let result = match protocol {
        PortForwardWebsocketProtocol::Legacy | PortForwardWebsocketProtocol::V4Binary => {
            write_websocket_channel_frame(writer, channel, payload).await
        }
        PortForwardWebsocketProtocol::V4Base64 => {
            let mut frame = Vec::with_capacity(1 + payload.len() * 2);
            frame.push(b'0' + channel);
            frame.extend_from_slice(
                base64::engine::general_purpose::STANDARD
                    .encode(payload)
                    .as_bytes(),
            );
            write_websocket_frame(&mut *writer.lock().await, 0x1, &frame).await
        }
    };
    if result.is_ok() {
        mark_stream_activity(activity, Instant::now());
    }
    result
}

fn decode_portforward_websocket_frame(
    protocol: PortForwardWebsocketProtocol,
    frame: &WebSocketFrame,
) -> anyhow::Result<Option<(u8, Vec<u8>)>> {
    if frame.payload.is_empty() {
        return Ok(None);
    }

    match protocol {
        PortForwardWebsocketProtocol::Legacy | PortForwardWebsocketProtocol::V4Binary => {
            Ok(Some((frame.payload[0], frame.payload[1..].to_vec())))
        }
        PortForwardWebsocketProtocol::V4Base64 => {
            let channel = frame.payload[0]
                .checked_sub(b'0')
                .ok_or_else(|| anyhow::anyhow!("invalid websocket channel prefix"))?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&frame.payload[1..])
                .map_err(|e| anyhow::anyhow!("invalid base64 websocket payload: {}", e))?;
            Ok(Some((channel, decoded)))
        }
    }
}

fn is_spdy_upgrade_request(req: &Request<Body>) -> bool {
    let connection = req
        .headers()
        .get(CONNECTION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let upgrade = req
        .headers()
        .get(UPGRADE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    connection.contains("upgrade") && upgrade.contains("spdy/3.1")
}

fn negotiate_remotecommand_protocol(req: &Request<Body>) -> Option<&'static str> {
    let requested: Vec<String> = req
        .headers()
        .get_all("X-Stream-Protocol-Version")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    if requested
        .iter()
        .any(|value| value == spdy::STREAM_PROTOCOL_V2)
    {
        return Some(spdy::STREAM_PROTOCOL_V2);
    }
    if requested.iter().any(|value| value == "channel.k8s.io") {
        return Some("channel.k8s.io");
    }
    None
}

fn negotiate_portforward_protocol(req: &Request<Body>) -> Option<&'static str> {
    let requested: Vec<String> = req
        .headers()
        .get_all("X-Stream-Protocol-Version")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    requested
        .iter()
        .any(|value| value == PORT_FORWARD_PROTOCOL_V1)
        .then_some(PORT_FORWARD_PROTOCOL_V1)
}

fn negotiate_portforward_websocket_protocol(req: &Request<Body>) -> Option<&'static str> {
    let requested: Vec<String> = req
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    for candidate in [
        PORT_FORWARD_WS_PROTOCOL_V4_BINARY,
        PORT_FORWARD_WS_PROTOCOL_V4_BASE64,
        PORT_FORWARD_PROTOCOL_V1,
    ] {
        if requested.iter().any(|value| value == candidate) {
            return Some(candidate);
        }
    }

    None
}

fn portforward_websocket_protocol(protocol: &str) -> PortForwardWebsocketProtocol {
    match protocol {
        PORT_FORWARD_WS_PROTOCOL_V4_BINARY => PortForwardWebsocketProtocol::V4Binary,
        PORT_FORWARD_WS_PROTOCOL_V4_BASE64 => PortForwardWebsocketProtocol::V4Base64,
        _ => PortForwardWebsocketProtocol::Legacy,
    }
}

fn portforward_request_id(
    stream_id: spdy::StreamId,
    stream_type: &str,
    request_id: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(request_id) = request_id.filter(|request_id| !request_id.is_empty()) {
        return Ok(request_id.to_string());
    }

    match stream_type {
        "error" => Ok(stream_id.to_string()),
        "data" if stream_id >= 2 => Ok((stream_id - 2).to_string()),
        _ => Err(anyhow::anyhow!(
            "port-forward stream is missing requestID header"
        )),
    }
}

fn portforward_request_id_from_headers(
    stream_id: spdy::StreamId,
    stream_type: &str,
    headers: &[(String, String)],
) -> Result<String, PortForwardStreamParseError> {
    portforward_request_id(
        stream_id,
        stream_type,
        spdy::header_value(headers, "requestid"),
    )
    .map_err(|err| PortForwardStreamParseError {
        request_id: spdy::header_value(headers, "requestid").map(str::to_string),
        message: err.to_string(),
    })
}

fn parse_portforward_stream(
    frame: &spdy::SynStreamFrame,
    decompressor: &mut spdy::HeaderDecompressor,
) -> Result<ParsedPortForwardStream, PortForwardStreamParseError> {
    let headers = spdy::decode_header_block(&frame.header_block, decompressor).map_err(|err| {
        PortForwardStreamParseError {
            request_id: None,
            message: format!("failed to decode port-forward headers: {}", err),
        }
    })?;
    let stream_type =
        spdy::header_value(&headers, "streamtype").ok_or_else(|| PortForwardStreamParseError {
            request_id: spdy::header_value(&headers, "requestid").map(str::to_string),
            message: "port-forward stream is missing streamtype header".to_string(),
        })?;
    let role = match stream_type {
        "data" => PortForwardStreamRole::Data,
        "error" => PortForwardStreamRole::Error,
        other => {
            return Err(PortForwardStreamParseError {
                request_id: spdy::header_value(&headers, "requestid").map(str::to_string),
                message: format!("unsupported port-forward streamtype {}", other),
            })
        }
    };
    let request_id = portforward_request_id_from_headers(frame.stream_id, stream_type, &headers)?;
    let port_header =
        spdy::header_value(&headers, "port").ok_or_else(|| PortForwardStreamParseError {
            request_id: Some(request_id.clone()),
            message: "port-forward stream is missing port header".to_string(),
        })?;
    let port = port_header
        .parse::<u16>()
        .map_err(|e| PortForwardStreamParseError {
            request_id: Some(request_id.clone()),
            message: format!("invalid port-forward port {}: {}", port_header, e),
        })?;
    if port == 0 {
        return Err(PortForwardStreamParseError {
            request_id: Some(request_id),
            message: "port-forward port must be > 0".to_string(),
        });
    }
    Ok(ParsedPortForwardStream {
        role,
        request_id,
        port,
    })
}

fn register_portforward_stream(
    pending_pairs: &mut HashMap<String, PortForwardPair>,
    active_request_ids: &HashSet<String>,
    now: Instant,
    stream_id: spdy::StreamId,
    role: PortForwardStreamRole,
    request_id: String,
    port: u16,
) -> PortForwardStreamRegistration {
    if active_request_ids.contains(&request_id) {
        return PortForwardStreamRegistration::RejectCurrent(format!(
            "port-forward request {} already has an active stream pair",
            request_id
        ));
    }

    if let Some(existing_pair) = pending_pairs.get(&request_id) {
        if existing_pair.port != port {
            let existing_port = existing_pair.port;
            let rejected = pending_pairs
                .remove(&request_id)
                .expect("pending pair should exist");
            return PortForwardStreamRegistration::RejectPendingPair {
                pair: rejected,
                message: format!(
                    "port-forward request {} used conflicting ports {} and {}",
                    request_id, existing_port, port
                ),
            };
        }

        let duplicate_role = matches!(
            role,
            PortForwardStreamRole::Data if existing_pair.data_stream.is_some()
        ) || matches!(
            role,
            PortForwardStreamRole::Error if existing_pair.error_stream.is_some()
        );
        if duplicate_role {
            let rejected = pending_pairs
                .remove(&request_id)
                .expect("pending pair should exist");
            let role_name = match role {
                PortForwardStreamRole::Data => "data",
                PortForwardStreamRole::Error => "error",
            };
            return PortForwardStreamRegistration::RejectPendingPair {
                pair: rejected,
                message: format!(
                    "duplicate port-forward {} stream for request {}",
                    role_name, request_id
                ),
            };
        }

        let pair = pending_pairs
            .get_mut(&request_id)
            .expect("pending pair should exist");
        match role {
            PortForwardStreamRole::Data => pair.data_stream = Some(stream_id),
            PortForwardStreamRole::Error => pair.error_stream = Some(stream_id),
        }
        if pair.data_stream.is_some() && pair.error_stream.is_some() {
            let ready = pending_pairs
                .remove(&request_id)
                .expect("pending pair should exist");
            return PortForwardStreamRegistration::Ready(ready);
        }
        return PortForwardStreamRegistration::Pending;
    }

    let mut pair = PortForwardPair {
        request_id: request_id.clone(),
        port,
        data_stream: None,
        error_stream: None,
        created_at: now,
    };
    match role {
        PortForwardStreamRole::Data => pair.data_stream = Some(stream_id),
        PortForwardStreamRole::Error => pair.error_stream = Some(stream_id),
    }
    pending_pairs.insert(request_id, pair);
    PortForwardStreamRegistration::Pending
}

fn next_portforward_pair_timeout(
    pending_pairs: &HashMap<String, PortForwardPair>,
    now: Instant,
) -> Option<Duration> {
    pending_pairs
        .values()
        .map(|pair| {
            (pair.created_at + PORT_FORWARD_STREAM_CREATION_TIMEOUT).saturating_duration_since(now)
        })
        .min()
}

fn take_expired_portforward_pairs(
    pending_pairs: &mut HashMap<String, PortForwardPair>,
    now: Instant,
) -> Vec<PortForwardPair> {
    let expired_request_ids: Vec<String> = pending_pairs
        .iter()
        .filter_map(|(request_id, pair)| {
            (now.duration_since(pair.created_at) >= PORT_FORWARD_STREAM_CREATION_TIMEOUT)
                .then(|| request_id.clone())
        })
        .collect();
    expired_request_ids
        .into_iter()
        .filter_map(|request_id| pending_pairs.remove(&request_id))
        .collect()
}

fn new_stream_activity(now: Instant) -> StreamActivity {
    Arc::new(std::sync::Mutex::new(now))
}

fn mark_stream_activity(activity: &StreamActivity, now: Instant) {
    if let Ok(mut last_activity) = activity.lock() {
        *last_activity = now;
    }
}

fn stream_idle_remaining(
    activity: &StreamActivity,
    now: Instant,
    idle_timeout: Duration,
) -> Option<Duration> {
    if idle_timeout.is_zero() {
        return None;
    }
    let last_activity = activity
        .lock()
        .map(|last_activity| *last_activity)
        .unwrap_or(now);
    Some((last_activity + idle_timeout).saturating_duration_since(now))
}

fn stream_is_idle(activity: &StreamActivity, now: Instant, idle_timeout: Duration) -> bool {
    matches!(
        stream_idle_remaining(activity, now, idle_timeout),
        Some(remaining) if remaining.is_zero()
    )
}

fn next_stream_idle_timeout(
    activity: &StreamActivity,
    now: Instant,
    idle_timeout: Duration,
) -> Duration {
    stream_idle_remaining(activity, now, idle_timeout).unwrap_or(Duration::MAX)
}

fn next_portforward_wait_timeout(
    pending_pairs: &HashMap<String, PortForwardPair>,
    activity: &StreamActivity,
    now: Instant,
    idle_timeout: Duration,
) -> Duration {
    let idle_timeout_remaining = next_stream_idle_timeout(activity, now, idle_timeout);
    match next_portforward_pair_timeout(pending_pairs, now) {
        Some(pair_timeout) => idle_timeout_remaining.min(pair_timeout),
        None => idle_timeout_remaining,
    }
}

fn with_netns_path<T, F>(netns_path: &Path, callback: F) -> anyhow::Result<T>
where
    F: FnOnce() -> anyhow::Result<T>,
{
    let current_meta = std::fs::metadata("/proc/thread-self/ns/net")
        .map_err(|e| anyhow::anyhow!("failed to stat current netns: {}", e))?;
    let target_meta = std::fs::metadata(netns_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to stat target netns {}: {}",
            netns_path.display(),
            e
        )
    })?;
    if current_meta.ino() == target_meta.ino() && current_meta.dev() == target_meta.dev() {
        return callback();
    }

    let current_netns = File::open("/proc/thread-self/ns/net")
        .map_err(|e| anyhow::anyhow!("failed to open current netns: {}", e))?;
    let target_netns = File::open(netns_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to open target netns {}: {}",
            netns_path.display(),
            e
        )
    })?;

    nix::sched::setns(target_netns.as_raw_fd(), CloneFlags::CLONE_NEWNET)
        .map_err(|e| anyhow::anyhow!("failed to enter netns {}: {}", netns_path.display(), e))?;

    let result = callback();
    let restore_result = nix::sched::setns(current_netns.as_raw_fd(), CloneFlags::CLONE_NEWNET)
        .map_err(|e| anyhow::anyhow!("failed to restore original netns: {}", e));

    match (result, restore_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(err), Ok(())) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Err(primary), Err(restore)) => Err(anyhow::anyhow!("{}; {}", primary, restore)),
    }
}

fn connect_to_port_in_netns_blocking(
    netns_path: PathBuf,
    port: u16,
) -> anyhow::Result<StdTcpStream> {
    with_netns_path(&netns_path, || {
        let mut last_error = None;
        for address in ("localhost", port).to_socket_addrs()? {
            match StdTcpStream::connect(address) {
                Ok(stream) => {
                    stream.set_nonblocking(true)?;
                    return Ok(stream);
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        Err(anyhow::anyhow!(
            "failed to connect to localhost:{} inside namespace {}: {}",
            port,
            netns_path.display(),
            last_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "no resolved addresses".to_string())
        ))
    })
}

async fn connect_to_port_in_netns(netns_path: PathBuf, port: u16) -> anyhow::Result<TcpStream> {
    let stream =
        tokio::task::spawn_blocking(move || connect_to_port_in_netns_blocking(netns_path, port))
            .await??;
    TcpStream::from_std(stream).map_err(Into::into)
}

async fn write_portforward_error(
    writer: &Arc<Mutex<spdy::AsyncSpdyWriter<tokio::io::WriteHalf<hyper::upgrade::Upgraded>>>>,
    activity: &StreamActivity,
    data_stream: spdy::StreamId,
    error_stream: spdy::StreamId,
    message: &str,
) -> anyhow::Result<()> {
    let mut writer = writer.lock().await;
    writer
        .write_data(error_stream, message.as_bytes(), false)
        .await?;
    writer.write_data(error_stream, &[], true).await?;
    if data_stream != error_stream {
        writer.write_data(data_stream, &[], true).await?;
    }
    mark_stream_activity(activity, Instant::now());
    Ok(())
}

async fn write_portforward_stream_error(
    writer: &Arc<Mutex<spdy::AsyncSpdyWriter<tokio::io::WriteHalf<hyper::upgrade::Upgraded>>>>,
    activity: &StreamActivity,
    stream_id: spdy::StreamId,
    message: &str,
) -> anyhow::Result<()> {
    let mut writer = writer.lock().await;
    if !message.is_empty() {
        writer
            .write_data(stream_id, message.as_bytes(), false)
            .await?;
    }
    writer.write_data(stream_id, &[], true).await?;
    mark_stream_activity(activity, Instant::now());
    Ok(())
}

async fn write_portforward_pair_setup_error(
    writer: &Arc<Mutex<spdy::AsyncSpdyWriter<tokio::io::WriteHalf<hyper::upgrade::Upgraded>>>>,
    activity: &StreamActivity,
    pair: PortForwardPair,
    extra_stream: Option<spdy::StreamId>,
    message: &str,
) -> anyhow::Result<()> {
    let mut closed_streams = HashSet::new();
    match (pair.data_stream, pair.error_stream) {
        (Some(data_stream), Some(error_stream)) => {
            closed_streams.insert(data_stream);
            closed_streams.insert(error_stream);
            write_portforward_error(writer, activity, data_stream, error_stream, message).await?;
        }
        (Some(data_stream), None) => {
            closed_streams.insert(data_stream);
            write_portforward_stream_error(writer, activity, data_stream, message).await?;
        }
        (None, Some(error_stream)) => {
            closed_streams.insert(error_stream);
            write_portforward_stream_error(writer, activity, error_stream, message).await?;
        }
        (None, None) => {}
    }

    if let Some(stream_id) = extra_stream.filter(|stream_id| !closed_streams.contains(stream_id)) {
        write_portforward_stream_error(writer, activity, stream_id, message).await?;
    }
    Ok(())
}

async fn serve_portforward_pair(
    writer: Arc<Mutex<spdy::AsyncSpdyWriter<tokio::io::WriteHalf<hyper::upgrade::Upgraded>>>>,
    activity: StreamActivity,
    netns_path: PathBuf,
    port: u16,
    data_stream: spdy::StreamId,
    error_stream: spdy::StreamId,
    mut input_rx: tokio::sync::mpsc::Receiver<Option<Vec<u8>>>,
) -> anyhow::Result<()> {
    let tcp_stream = match connect_to_port_in_netns(netns_path, port).await {
        Ok(stream) => stream,
        Err(err) => {
            write_portforward_error(
                &writer,
                &activity,
                data_stream,
                error_stream,
                &err.to_string(),
            )
            .await?;
            return Err(err);
        }
    };

    let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();
    let writer_for_output = writer.clone();
    let activity_for_output = activity.clone();
    let output_task = tokio::spawn(async move {
        let mut buffer = [0u8; 8192];
        loop {
            match tcp_read.read(&mut buffer).await {
                Ok(0) => {
                    writer_for_output
                        .lock()
                        .await
                        .write_data(data_stream, &[], true)
                        .await?;
                    mark_stream_activity(&activity_for_output, Instant::now());
                    break;
                }
                Ok(n) => {
                    writer_for_output
                        .lock()
                        .await
                        .write_data(data_stream, &buffer[..n], false)
                        .await?;
                    mark_stream_activity(&activity_for_output, Instant::now());
                }
                Err(err) => {
                    write_portforward_error(
                        &writer_for_output,
                        &activity_for_output,
                        data_stream,
                        error_stream,
                        &format!("port-forward read failed: {}", err),
                    )
                    .await?;
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    while let Some(message) = input_rx.recv().await {
        match message {
            Some(data) if !data.is_empty() => {
                tcp_write.write_all(&data).await?;
            }
            Some(_) => {}
            None => {
                let _ = tcp_write.shutdown().await;
                break;
            }
        }
    }

    output_task.await??;
    writer
        .lock()
        .await
        .write_data(error_stream, &[], true)
        .await?;
    mark_stream_activity(&activity, Instant::now());
    Ok(())
}

async fn serve_exec_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: ExecRequest,
    runtime_path: PathBuf,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (read_half, write_half) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(spdy::AsyncSpdyWriter::new(write_half)));
    let mut reader = read_half;

    let expected_roles = expected_exec_roles(&req);
    let mut header_decompressor = spdy::HeaderDecompressor::new();
    let mut stdin_stream = None;
    let mut stdout_stream = None;
    let mut stderr_stream = None;
    let mut error_stream = None;
    let mut resize_stream = None;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while [error_stream, stdin_stream, stdout_stream, stderr_stream]
        .into_iter()
        .flatten()
        .count()
        < expected_roles.len()
    {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, spdy::read_frame_async(&mut reader)).await??;
        match frame {
            spdy::Frame::SynStream(frame) => {
                let headers =
                    spdy::decode_header_block(&frame.header_block, &mut header_decompressor)?;
                let stream_type = spdy::header_value(&headers, "streamtype")
                    .ok_or_else(|| anyhow::anyhow!("exec stream is missing streamtype header"))?;
                let role = match stream_type {
                    "error" => ExecStreamRole::Error,
                    "stdin" => ExecStreamRole::Stdin,
                    "stdout" => ExecStreamRole::Stdout,
                    "stderr" => ExecStreamRole::Stderr,
                    "resize" => ExecStreamRole::Resize,
                    other => return Err(anyhow::anyhow!("unsupported exec streamtype {}", other)),
                };

                if role != ExecStreamRole::Resize && !expected_roles.contains(&role) {
                    return Err(anyhow::anyhow!(
                        "unexpected exec stream {:?} for request",
                        role
                    ));
                }
                if role == ExecStreamRole::Resize && !req.tty {
                    return Err(anyhow::anyhow!("exec resize stream requires tty mode"));
                }

                match role {
                    ExecStreamRole::Error if error_stream.is_none() => {
                        error_stream = Some(frame.stream_id)
                    }
                    ExecStreamRole::Stdin if stdin_stream.is_none() => {
                        stdin_stream = Some(frame.stream_id)
                    }
                    ExecStreamRole::Stdout if stdout_stream.is_none() => {
                        stdout_stream = Some(frame.stream_id)
                    }
                    ExecStreamRole::Stderr if stderr_stream.is_none() => {
                        stderr_stream = Some(frame.stream_id)
                    }
                    ExecStreamRole::Resize if resize_stream.is_none() => {
                        resize_stream = Some(frame.stream_id)
                    }
                    _ => {
                        return Err(anyhow::anyhow!("duplicate exec stream {:?} received", role));
                    }
                }

                writer
                    .lock()
                    .await
                    .write_syn_reply(frame.stream_id, &[], false)
                    .await?;
            }
            spdy::Frame::Ping(frame) => {
                writer.lock().await.write_ping(frame.id).await?;
            }
            _ => {}
        }
    }

    let mut command = TokioCommand::new(&runtime_path);
    command.arg("exec");
    if req.tty {
        command.arg("-t");
    }
    command.arg(&req.container_id);
    for arg in &req.cmd {
        command.arg(arg);
    }

    let mut tty_master = None;
    let mut tty_resize = None;
    if req.tty {
        let pty = openpty(None, None)?;
        let master = unsafe { File::from_raw_fd(pty.master) };
        let slave = unsafe { File::from_raw_fd(pty.slave) };
        let slave_stdin = slave.try_clone()?;
        let slave_stdout = slave.try_clone()?;
        let slave_stderr = slave;
        let slave_fd = slave_stderr.as_raw_fd();

        command.stdin(Stdio::from(slave_stdin));
        command.stdout(Stdio::from(slave_stdout));
        command.stderr(Stdio::from(slave_stderr));
        unsafe {
            command.pre_exec(move || {
                if nix::unistd::setsid().is_err() {
                    return Err(std::io::Error::last_os_error());
                }
                if nix::libc::ioctl(slave_fd, nix::libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        tty_resize = Some(master.try_clone()?);
        tty_master = Some(master);
    } else {
        command.stdin(if req.stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        command.stdout(if req.stdout {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        command.stderr(if req.stderr {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            if let Some(stream_id) = error_stream {
                let mut writer = writer.lock().await;
                let _ = writer
                    .write_data(
                        stream_id,
                        format!("failed to spawn exec process: {}", e).as_bytes(),
                        false,
                    )
                    .await;
                let _ = writer.write_data(stream_id, &[], true).await;
                let _ = writer.write_goaway(stream_id).await;
            }
            return Err(e.into());
        }
    };

    let stdout_task;
    let stderr_task;
    let mut console_input_task = None;
    let mut console_stdin_tx = None;

    if req.tty {
        let master = tty_master
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing exec tty master"))?;
        let master_reader = master.try_clone()?;
        let master_writer = master;
        let (console_out_tx, mut console_out_rx) =
            tokio::sync::mpsc::channel::<ExecConsoleEvent>(8);

        tokio::task::spawn_blocking(move || {
            let mut reader = master_reader;
            let mut buffer = [0u8; 8192];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let _ = console_out_tx.blocking_send(ExecConsoleEvent::Eof);
                        break;
                    }
                    Ok(n) => {
                        if console_out_tx
                            .blocking_send(ExecConsoleEvent::Data(buffer[..n].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ =
                            console_out_tx.blocking_send(ExecConsoleEvent::Error(e.to_string()));
                        break;
                    }
                }
            }
        });

        let writer_for_console_output = writer.clone();
        stdout_task = Some(tokio::spawn(async move {
            while let Some(event) = console_out_rx.recv().await {
                match event {
                    ExecConsoleEvent::Data(data) => {
                        if let Some(stream_id) = stdout_stream {
                            writer_for_console_output
                                .lock()
                                .await
                                .write_data(stream_id, &data, false)
                                .await?;
                        }
                    }
                    ExecConsoleEvent::Eof => {
                        if let Some(stream_id) = stdout_stream {
                            writer_for_console_output
                                .lock()
                                .await
                                .write_data(stream_id, &[], true)
                                .await?;
                        }
                        break;
                    }
                    ExecConsoleEvent::Error(message) => {
                        if let Some(stream_id) = error_stream {
                            writer_for_console_output
                                .lock()
                                .await
                                .write_data(stream_id, message.as_bytes(), false)
                                .await?;
                        }
                        if let Some(stream_id) = stdout_stream {
                            writer_for_console_output
                                .lock()
                                .await
                                .write_data(stream_id, &[], true)
                                .await?;
                        }
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
        stderr_task = None;

        if req.stdin {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<Vec<u8>>>(8);
            console_stdin_tx = Some(tx);
            console_input_task = Some(tokio::task::spawn_blocking(move || {
                let mut writer = master_writer;
                while let Some(data) = rx.blocking_recv() {
                    match data {
                        Some(bytes) => {
                            writer.write_all(&bytes)?;
                            writer.flush()?;
                        }
                        None => break,
                    }
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
    } else {
        stdout_task = child
            .stdout
            .take()
            .zip(stdout_stream)
            .map(|(mut stdout, stream_id)| {
                let writer = writer.clone();
                tokio::spawn(async move {
                    let mut buffer = [0u8; 8192];
                    loop {
                        match stdout.read(&mut buffer).await {
                            Ok(0) => {
                                writer.lock().await.write_data(stream_id, &[], true).await?;
                                break;
                            }
                            Ok(n) => {
                                writer
                                    .lock()
                                    .await
                                    .write_data(stream_id, &buffer[..n], false)
                                    .await?;
                            }
                            Err(e) => return Err(anyhow::Error::new(e)),
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                })
            });
        stderr_task = child
            .stderr
            .take()
            .zip(stderr_stream)
            .map(|(mut stderr, stream_id)| {
                let writer = writer.clone();
                tokio::spawn(async move {
                    let mut buffer = [0u8; 8192];
                    loop {
                        match stderr.read(&mut buffer).await {
                            Ok(0) => {
                                writer.lock().await.write_data(stream_id, &[], true).await?;
                                break;
                            }
                            Ok(n) => {
                                writer
                                    .lock()
                                    .await
                                    .write_data(stream_id, &buffer[..n], false)
                                    .await?;
                            }
                            Err(e) => return Err(anyhow::Error::new(e)),
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                })
            });
    }

    let writer_for_input = writer.clone();
    let mut child_stdin = child.stdin.take();
    let console_stdin_tx_for_input = console_stdin_tx.clone();
    let tty_resize_for_input = tty_resize;
    let mut header_decompressor = header_decompressor;
    let stdin_task = tokio::spawn(async move {
        loop {
            match spdy::read_frame_async(&mut reader).await {
                Ok(spdy::Frame::Data(frame)) => {
                    if Some(frame.stream_id) == resize_stream {
                        if frame.data.is_empty() {
                            continue;
                        }
                        if let Some(tty) = tty_resize_for_input.as_ref() {
                            match parse_terminal_size(&frame.data).and_then(|(width, height)| {
                                apply_terminal_resize(tty, width, height)
                            }) {
                                Ok(()) => {}
                                Err(e) => {
                                    write_error_stream(
                                        &writer_for_input,
                                        error_stream,
                                        &format!("failed to apply exec resize: {}", e),
                                    )
                                    .await?;
                                }
                            }
                        }
                    } else if Some(frame.stream_id) == stdin_stream {
                        if req.tty {
                            if let Some(tx) = console_stdin_tx_for_input.as_ref() {
                                if !frame.data.is_empty() {
                                    tx.send(Some(frame.data)).await.map_err(|e| {
                                        anyhow::anyhow!("failed to forward tty stdin: {}", e)
                                    })?;
                                }
                                if frame.flags & 0x01 != 0 {
                                    let _ = tx.send(None).await;
                                }
                            }
                        } else if let Some(stdin) = child_stdin.as_mut() {
                            if !frame.data.is_empty() {
                                stdin.write_all(&frame.data).await?;
                            }
                            if frame.flags & 0x01 != 0 {
                                let _ = stdin.shutdown().await;
                                child_stdin = None;
                            }
                        }
                    }
                }
                Ok(spdy::Frame::SynStream(frame)) => {
                    let headers =
                        spdy::decode_header_block(&frame.header_block, &mut header_decompressor)?;
                    let stream_type =
                        spdy::header_value(&headers, "streamtype").ok_or_else(|| {
                            anyhow::anyhow!("exec stream is missing streamtype header")
                        })?;
                    if stream_type != "resize" || !req.tty {
                        return Err(anyhow::anyhow!(
                            "unexpected exec streamtype {} after session start",
                            stream_type
                        ));
                    }
                    if resize_stream.replace(frame.stream_id).is_some() {
                        return Err(anyhow::anyhow!("duplicate exec resize stream received"));
                    }
                    writer_for_input
                        .lock()
                        .await
                        .write_syn_reply(frame.stream_id, &[], false)
                        .await?;
                }
                Ok(spdy::Frame::Ping(frame)) => {
                    writer_for_input.lock().await.write_ping(frame.id).await?;
                }
                Ok(spdy::Frame::GoAway(_)) => break,
                Ok(_) => {}
                Err(e) => {
                    log::debug!("Exec input loop stopped: {}", e);
                    break;
                }
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let status = child.wait().await?;

    if let Some(tx) = console_stdin_tx.as_ref() {
        let _ = tx.send(None).await;
    }

    stdin_task.abort();
    let _ = stdin_task.await;
    if let Some(task) = console_input_task {
        task.await??;
    }

    let mut stdout_fin_sent = false;
    if let Some(task) = stdout_task {
        if req.tty {
            let mut task = task;
            match tokio::time::timeout(Duration::from_millis(200), &mut task).await {
                Ok(joined) => {
                    joined??;
                    stdout_fin_sent = true;
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        } else {
            task.await??;
            stdout_fin_sent = true;
        }
    }
    if let Some(task) = stderr_task {
        task.await??;
    }

    if let Some(stream_id) = error_stream {
        let mut writer = writer.lock().await;
        if req.tty && !stdout_fin_sent {
            if let Some(stdout_stream_id) = stdout_stream {
                writer.write_data(stdout_stream_id, &[], true).await?;
            }
        }
        if !status.success() {
            let exit_code = status.code().unwrap_or_default();
            writer
                .write_data(
                    stream_id,
                    format!("command terminated with non-zero exit code: {}", exit_code).as_bytes(),
                    false,
                )
                .await?;
        }
        writer.write_data(stream_id, &[], true).await?;
        writer.write_goaway(stream_id).await?;
    } else if let Some(stream_id) = stdout_stream.or(stderr_stream).or(stdin_stream) {
        writer.lock().await.write_goaway(stream_id).await?;
    }

    Ok(())
}

async fn serve_exec_websocket(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: ExecRequest,
    runtime_path: PathBuf,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (mut reader, writer) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(writer));

    let mut command = TokioCommand::new(&runtime_path);
    command.arg("exec");
    if req.tty {
        command.arg("-t");
    }
    command.arg(&req.container_id);
    for arg in &req.cmd {
        command.arg(arg);
    }

    let mut tty_master = None;
    let mut tty_resize = None;
    if req.tty {
        let pty = openpty(None, None)?;
        let master = unsafe { File::from_raw_fd(pty.master) };
        let slave = unsafe { File::from_raw_fd(pty.slave) };
        let slave_stdin = slave.try_clone()?;
        let slave_stdout = slave.try_clone()?;
        let slave_stderr = slave;
        let slave_fd = slave_stderr.as_raw_fd();

        command.stdin(Stdio::from(slave_stdin));
        command.stdout(Stdio::from(slave_stdout));
        command.stderr(Stdio::from(slave_stderr));
        unsafe {
            command.pre_exec(move || {
                if nix::unistd::setsid().is_err() {
                    return Err(std::io::Error::last_os_error());
                }
                if nix::libc::ioctl(slave_fd, nix::libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        tty_resize = Some(master.try_clone()?);
        tty_master = Some(master);
    } else {
        command.stdin(if req.stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        command.stdout(if req.stdout {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        command.stderr(if req.stderr {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    }

    let mut child = command.spawn()?;
    let stdout_task;
    let stderr_task;
    let mut console_input_task = None;
    let mut console_stdin_tx = None;

    if req.tty {
        let master = tty_master
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing exec tty master"))?;
        let master_reader = master.try_clone()?;
        let master_writer = master;
        let (console_out_tx, mut console_out_rx) =
            tokio::sync::mpsc::channel::<ExecConsoleEvent>(8);

        tokio::task::spawn_blocking(move || {
            let mut reader = master_reader;
            let mut buffer = [0u8; 8192];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let _ = console_out_tx.blocking_send(ExecConsoleEvent::Eof);
                        break;
                    }
                    Ok(n) => {
                        if console_out_tx
                            .blocking_send(ExecConsoleEvent::Data(buffer[..n].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ =
                            console_out_tx.blocking_send(ExecConsoleEvent::Error(e.to_string()));
                        break;
                    }
                }
            }
        });

        let writer_for_console_output = writer.clone();
        stdout_task = Some(tokio::spawn(async move {
            while let Some(event) = console_out_rx.recv().await {
                match event {
                    ExecConsoleEvent::Data(data) => {
                        write_websocket_channel_frame(
                            &writer_for_console_output,
                            WS_CHANNEL_STDOUT,
                            &data,
                        )
                        .await?;
                    }
                    ExecConsoleEvent::Eof => break,
                    ExecConsoleEvent::Error(message) => {
                        write_websocket_channel_frame(
                            &writer_for_console_output,
                            WS_CHANNEL_ERROR,
                            message.as_bytes(),
                        )
                        .await?;
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
        stderr_task = None;

        if req.stdin {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Option<Vec<u8>>>(8);
            console_stdin_tx = Some(tx);
            console_input_task = Some(tokio::task::spawn_blocking(move || {
                let mut writer = master_writer;
                while let Some(data) = rx.blocking_recv() {
                    match data {
                        Some(bytes) => {
                            writer.write_all(&bytes)?;
                            writer.flush()?;
                        }
                        None => break,
                    }
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
    } else {
        stdout_task = child.stdout.take().map(|mut stdout| {
            let writer = writer.clone();
            tokio::spawn(async move {
                let mut buffer = [0u8; 8192];
                loop {
                    match stdout.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(n) => {
                            write_websocket_channel_frame(&writer, WS_CHANNEL_STDOUT, &buffer[..n])
                                .await?;
                        }
                        Err(e) => return Err(anyhow::Error::new(e)),
                    }
                }
                Ok::<(), anyhow::Error>(())
            })
        });
        stderr_task = child.stderr.take().map(|mut stderr| {
            let writer = writer.clone();
            tokio::spawn(async move {
                let mut buffer = [0u8; 8192];
                loop {
                    match stderr.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(n) => {
                            write_websocket_channel_frame(&writer, WS_CHANNEL_STDERR, &buffer[..n])
                                .await?;
                        }
                        Err(e) => return Err(anyhow::Error::new(e)),
                    }
                }
                Ok::<(), anyhow::Error>(())
            })
        });
    }

    let writer_for_input = writer.clone();
    let mut child_stdin = child.stdin.take();
    let console_stdin_tx_for_input = console_stdin_tx.clone();
    let tty_resize_for_input = tty_resize;
    let input_task = tokio::spawn(async move {
        loop {
            let Some(frame) = read_websocket_frame(&mut reader).await? else {
                break;
            };
            match frame.opcode {
                0x8 => break,
                0x9 => {
                    write_websocket_frame(&mut *writer_for_input.lock().await, 0xA, &frame.payload)
                        .await?;
                }
                0x1 | 0x2 => {
                    if frame.payload.is_empty() {
                        continue;
                    }
                    let channel = frame.payload[0];
                    let payload = &frame.payload[1..];
                    match channel {
                        WS_CHANNEL_RESIZE if req.tty => {
                            if let Some(tty) = tty_resize_for_input.as_ref() {
                                match parse_terminal_size(payload).and_then(|(width, height)| {
                                    apply_terminal_resize(tty, width, height)
                                }) {
                                    Ok(()) => {}
                                    Err(e) => {
                                        write_websocket_channel_frame(
                                            &writer_for_input,
                                            WS_CHANNEL_ERROR,
                                            format!("failed to apply exec resize: {}", e)
                                                .as_bytes(),
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
                        WS_CHANNEL_STDIN => {
                            if req.tty {
                                if let Some(tx) = console_stdin_tx_for_input.as_ref() {
                                    if !payload.is_empty() {
                                        tx.send(Some(payload.to_vec())).await.map_err(|e| {
                                            anyhow::anyhow!("failed to forward tty stdin: {}", e)
                                        })?;
                                    }
                                }
                            } else if let Some(stdin) = child_stdin.as_mut() {
                                if !payload.is_empty() {
                                    stdin.write_all(payload).await?;
                                }
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let status = child.wait().await?;

    if let Some(tx) = console_stdin_tx.as_ref() {
        let _ = tx.send(None).await;
    }
    input_task.abort();
    let _ = input_task.await;
    if let Some(task) = console_input_task {
        task.await??;
    }
    if let Some(task) = stdout_task {
        task.await??;
    }
    if let Some(task) = stderr_task {
        task.await??;
    }

    if !status.success() {
        let exit_code = status.code().unwrap_or_default();
        write_websocket_channel_frame(
            &writer,
            WS_CHANNEL_ERROR,
            format!("command terminated with non-zero exit code: {}", exit_code).as_bytes(),
        )
        .await?;
    }
    write_websocket_frame(&mut *writer.lock().await, 0x8, &[]).await?;
    Ok(())
}

async fn serve_portforward_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    ctx: PortForwardRequestContext,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (read_half, write_half) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(spdy::AsyncSpdyWriter::new(write_half)));
    let mut reader = read_half;
    let mut header_decompressor = spdy::HeaderDecompressor::new();
    let activity = new_stream_activity(Instant::now());

    let mut pending_pairs: HashMap<String, PortForwardPair> = HashMap::new();
    let mut active_request_ids = HashSet::new();
    let mut active_data_stream_request_ids: HashMap<spdy::StreamId, String> = HashMap::new();
    let mut data_stream_inputs: HashMap<
        spdy::StreamId,
        tokio::sync::mpsc::Sender<Option<Vec<u8>>>,
    > = HashMap::new();
    let mut active_streams = 0usize;

    loop {
        if stream_is_idle(&activity, Instant::now(), PORT_FORWARD_STREAM_IDLE_TIMEOUT) {
            let _ = writer.lock().await.write_goaway(0).await;
            break;
        }

        for pair in take_expired_portforward_pairs(&mut pending_pairs, Instant::now()) {
            let message = format!(
                "timed out waiting for matching port-forward stream for request {}",
                pair.request_id
            );
            write_portforward_pair_setup_error(&writer, &activity, pair, None, &message).await?;
        }

        let next_frame = match tokio::time::timeout(
            next_portforward_wait_timeout(
                &pending_pairs,
                &activity,
                Instant::now(),
                PORT_FORWARD_STREAM_IDLE_TIMEOUT,
            ),
            spdy::read_frame_async(&mut reader),
        )
        .await
        {
            Ok(frame) => frame,
            Err(_) => continue,
        };

        match next_frame {
            Ok(spdy::Frame::SynStream(frame)) => {
                mark_stream_activity(&activity, Instant::now());
                let parsed = match parse_portforward_stream(&frame, &mut header_decompressor) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        writer
                            .lock()
                            .await
                            .write_syn_reply(frame.stream_id, &[], false)
                            .await?;
                        if let Some(request_id) = err.request_id.as_deref() {
                            if let Some(pair) = pending_pairs.remove(request_id) {
                                write_portforward_pair_setup_error(
                                    &writer,
                                    &activity,
                                    pair,
                                    Some(frame.stream_id),
                                    &err.message,
                                )
                                .await?;
                                continue;
                            }
                        }
                        write_portforward_stream_error(
                            &writer,
                            &activity,
                            frame.stream_id,
                            &err.message,
                        )
                        .await?;
                        continue;
                    }
                };

                if !ctx.req.port.is_empty()
                    && !ctx
                        .req
                        .port
                        .iter()
                        .any(|allowed| *allowed == parsed.port as i32)
                {
                    writer
                        .lock()
                        .await
                        .write_syn_reply(frame.stream_id, &[], false)
                        .await?;
                    let message = format!("port {} was not requested by the client", parsed.port);
                    if let Some(pair) = pending_pairs.remove(&parsed.request_id) {
                        write_portforward_pair_setup_error(
                            &writer,
                            &activity,
                            pair,
                            Some(frame.stream_id),
                            &message,
                        )
                        .await?;
                    } else {
                        write_portforward_stream_error(
                            &writer,
                            &activity,
                            frame.stream_id,
                            &message,
                        )
                        .await?;
                    }
                    continue;
                }

                writer
                    .lock()
                    .await
                    .write_syn_reply(frame.stream_id, &[], false)
                    .await?;

                match register_portforward_stream(
                    &mut pending_pairs,
                    &active_request_ids,
                    Instant::now(),
                    frame.stream_id,
                    parsed.role,
                    parsed.request_id.clone(),
                    parsed.port,
                ) {
                    PortForwardStreamRegistration::Pending => {}
                    PortForwardStreamRegistration::Ready(pair) => {
                        let data_stream = pair.data_stream.expect("ready pair should have data");
                        let error_stream = pair
                            .error_stream
                            .expect("ready pair should have error stream");
                        active_request_ids.insert(pair.request_id.clone());
                        active_data_stream_request_ids.insert(data_stream, pair.request_id.clone());
                        let (input_tx, input_rx) = tokio::sync::mpsc::channel(32);
                        data_stream_inputs.insert(data_stream, input_tx);
                        active_streams += 1;
                        let writer = writer.clone();
                        let activity = activity.clone();
                        let netns_path = ctx.netns_path.clone();
                        tokio::spawn(async move {
                            if let Err(err) = serve_portforward_pair(
                                writer,
                                activity,
                                netns_path,
                                pair.port,
                                data_stream,
                                error_stream,
                                input_rx,
                            )
                            .await
                            {
                                log::debug!(
                                    "port-forward pair {}:{} failed: {}",
                                    pair.request_id,
                                    pair.port,
                                    err
                                );
                            }
                        });
                    }
                    PortForwardStreamRegistration::RejectCurrent(message) => {
                        write_portforward_stream_error(
                            &writer,
                            &activity,
                            frame.stream_id,
                            &message,
                        )
                        .await?;
                    }
                    PortForwardStreamRegistration::RejectPendingPair { pair, message } => {
                        write_portforward_pair_setup_error(
                            &writer,
                            &activity,
                            pair,
                            Some(frame.stream_id),
                            &message,
                        )
                        .await?;
                    }
                }
            }
            Ok(spdy::Frame::Data(frame)) => {
                mark_stream_activity(&activity, Instant::now());
                if let Some(sender) = data_stream_inputs.get(&frame.stream_id) {
                    if !frame.data.is_empty() {
                        sender.send(Some(frame.data)).await.map_err(|e| {
                            anyhow::anyhow!("port-forward input send failed: {}", e)
                        })?;
                    }
                    if frame.flags & 0x01 != 0 {
                        let _ = sender.send(None).await;
                        data_stream_inputs.remove(&frame.stream_id);
                        active_streams = active_streams.saturating_sub(1);
                        if let Some(request_id) =
                            active_data_stream_request_ids.remove(&frame.stream_id)
                        {
                            active_request_ids.remove(&request_id);
                        }
                    }
                }
            }
            Ok(spdy::Frame::Ping(frame)) => {
                mark_stream_activity(&activity, Instant::now());
                writer.lock().await.write_ping(frame.id).await?;
                mark_stream_activity(&activity, Instant::now());
            }
            Ok(spdy::Frame::GoAway(_)) => break,
            Ok(_) => {}
            Err(err) => {
                log::debug!("Port-forward input loop stopped: {}", err);
                break;
            }
        }
    }

    for (_, sender) in data_stream_inputs {
        let _ = sender.send(None).await;
    }
    if active_streams > 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

async fn serve_attach_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: AttachRequest,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (read_half, write_half) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(spdy::AsyncSpdyWriter::new(write_half)));
    let mut reader = read_half;

    let expected_roles = expected_attach_roles(&req);
    let mut header_decompressor = spdy::HeaderDecompressor::new();
    let mut stdin_stream = None;
    let mut stdout_stream = None;
    let mut stderr_stream = None;
    let mut error_stream = None;
    let mut resize_stream = None;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while [error_stream, stdin_stream, stdout_stream, stderr_stream]
        .into_iter()
        .flatten()
        .count()
        < expected_roles.len()
    {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, spdy::read_frame_async(&mut reader)).await??;
        match frame {
            spdy::Frame::SynStream(frame) => {
                let headers =
                    spdy::decode_header_block(&frame.header_block, &mut header_decompressor)?;
                let stream_type = spdy::header_value(&headers, "streamtype")
                    .ok_or_else(|| anyhow::anyhow!("attach stream is missing streamtype header"))?;
                let role = match stream_type {
                    "error" => AttachStreamRole::Error,
                    "stdin" => AttachStreamRole::Stdin,
                    "stdout" => AttachStreamRole::Stdout,
                    "stderr" => AttachStreamRole::Stderr,
                    "resize" => AttachStreamRole::Resize,
                    other => {
                        return Err(anyhow::anyhow!("unsupported attach streamtype {}", other))
                    }
                };

                if role != AttachStreamRole::Resize && !expected_roles.contains(&role) {
                    return Err(anyhow::anyhow!(
                        "unexpected attach stream {:?} for request",
                        role
                    ));
                }
                if role == AttachStreamRole::Resize && !req.tty {
                    return Err(anyhow::anyhow!("attach resize stream requires tty mode"));
                }

                match role {
                    AttachStreamRole::Error if error_stream.is_none() => {
                        error_stream = Some(frame.stream_id)
                    }
                    AttachStreamRole::Stdin if stdin_stream.is_none() => {
                        stdin_stream = Some(frame.stream_id)
                    }
                    AttachStreamRole::Stdout if stdout_stream.is_none() => {
                        stdout_stream = Some(frame.stream_id)
                    }
                    AttachStreamRole::Stderr if stderr_stream.is_none() => {
                        stderr_stream = Some(frame.stream_id)
                    }
                    AttachStreamRole::Resize if resize_stream.is_none() => {
                        resize_stream = Some(frame.stream_id)
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "duplicate attach stream {:?} received",
                            role
                        ));
                    }
                }

                writer
                    .lock()
                    .await
                    .write_syn_reply(frame.stream_id, &[], false)
                    .await?;
            }
            spdy::Frame::Ping(frame) => {
                writer.lock().await.write_ping(frame.id).await?;
            }
            _ => {}
        }
    }

    let attach_socket_path = shim_socket_path(&req.container_id, "attach.sock");
    let shim = match UnixStream::connect(&attach_socket_path).await {
        Ok(shim) => shim,
        Err(e) => {
            let message = format!(
                "failed to connect attach socket {}: {}",
                attach_socket_path.display(),
                e
            );
            write_error_stream(&writer, error_stream, &message).await?;
            if let Some(stream_id) = error_stream {
                let mut writer = writer.lock().await;
                writer.write_data(stream_id, &[], true).await?;
                writer.write_goaway(stream_id).await?;
                return Ok(());
            }
            return Err(anyhow::anyhow!(message));
        }
    };
    let (mut shim_read, shim_write) = shim.into_split();
    let shim_write = Arc::new(Mutex::new(shim_write));

    let mut resize_socket = if req.tty && resize_stream.is_some() {
        let resize_socket_path = shim_socket_path(&req.container_id, "resize.sock");
        match UnixStream::connect(&resize_socket_path).await {
            Ok(stream) => Some(stream),
            Err(e) => {
                let message = format!(
                    "failed to connect resize socket {}: {}",
                    resize_socket_path.display(),
                    e
                );
                write_error_stream(&writer, error_stream, &message).await?;
                None
            }
        }
    } else {
        None
    };

    let writer_for_output = writer.clone();
    let error_stream_for_output = error_stream;
    let stdout_stream_for_output = stdout_stream;
    let stderr_stream_for_output = stderr_stream;
    tokio::spawn(async move {
        let mut decoder = AttachOutputDecoder::default();
        let mut buffer = [0u8; 8192];
        loop {
            match shim_read.read(&mut buffer).await {
                Ok(0) => {
                    if let Some(stream_id) = stdout_stream_for_output {
                        let _ = writer_for_output
                            .lock()
                            .await
                            .write_data(stream_id, &[], true)
                            .await;
                    }
                    if let Some(stream_id) = stderr_stream_for_output {
                        let _ = writer_for_output
                            .lock()
                            .await
                            .write_data(stream_id, &[], true)
                            .await;
                    }
                    if let Some(stream_id) = error_stream_for_output {
                        let _ = writer_for_output
                            .lock()
                            .await
                            .write_data(stream_id, &[], true)
                            .await;
                    }
                    if let Err(e) = decoder.finish() {
                        log::debug!("Attach output decoder finished with trailing data: {}", e);
                    }
                    if let Some(stream_id) = stdout_stream_for_output
                        .or(stderr_stream_for_output)
                        .or(error_stream_for_output)
                    {
                        let _ = writer_for_output.lock().await.write_goaway(stream_id).await;
                    }
                    break;
                }
                Ok(n) => match decoder.push(&buffer[..n]) {
                    Ok(frames) => {
                        for frame in frames {
                            let target_stream = match frame.pipe {
                                ATTACH_PIPE_STDOUT => stdout_stream_for_output,
                                ATTACH_PIPE_STDERR => {
                                    stderr_stream_for_output.or(stdout_stream_for_output)
                                }
                                _ => None,
                            };
                            if let Some(stream_id) = target_stream {
                                if writer_for_output
                                    .lock()
                                    .await
                                    .write_data(stream_id, &frame.payload, false)
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::debug!("Attach output decoder stopped: {}", e);
                        if let Some(stream_id) = error_stream_for_output {
                            let _ = writer_for_output
                                .lock()
                                .await
                                .write_data(stream_id, e.to_string().as_bytes(), false)
                                .await;
                        }
                        break;
                    }
                },
                Err(e) => {
                    log::debug!("Attach output pump stopped: {}", e);
                    break;
                }
            }
        }
    });

    let mut header_decompressor = header_decompressor;
    loop {
        match spdy::read_frame_async(&mut reader).await {
            Ok(spdy::Frame::Data(frame)) => {
                if Some(frame.stream_id) == resize_stream {
                    if frame.data.is_empty() {
                        continue;
                    }
                    if resize_socket.is_none() && req.tty {
                        let resize_socket_path = shim_socket_path(&req.container_id, "resize.sock");
                        match UnixStream::connect(&resize_socket_path).await {
                            Ok(stream) => resize_socket = Some(stream),
                            Err(e) => {
                                write_error_stream(
                                    &writer,
                                    error_stream,
                                    &format!(
                                        "failed to connect resize socket {}: {}",
                                        resize_socket_path.display(),
                                        e
                                    ),
                                )
                                .await?;
                                continue;
                            }
                        }
                    }
                    if let Some(socket) = resize_socket.as_mut() {
                        socket.write_all(&frame.data).await?;
                        if !frame.data.ends_with(b"\n") {
                            socket.write_all(b"\n").await?;
                        }
                    }
                } else if Some(frame.stream_id) == stdin_stream {
                    let mut shim_write = shim_write.lock().await;
                    if !frame.data.is_empty() {
                        shim_write.write_all(&frame.data).await?;
                    }
                    if frame.flags & 0x01 != 0 {
                        let _ = shim_write.shutdown().await;
                    }
                }
            }
            Ok(spdy::Frame::SynStream(frame)) => {
                let headers =
                    spdy::decode_header_block(&frame.header_block, &mut header_decompressor)?;
                let stream_type = spdy::header_value(&headers, "streamtype")
                    .ok_or_else(|| anyhow::anyhow!("attach stream is missing streamtype header"))?;
                if stream_type != "resize" || !req.tty {
                    return Err(anyhow::anyhow!(
                        "unexpected attach streamtype {} after session start",
                        stream_type
                    ));
                }
                if resize_stream.replace(frame.stream_id).is_some() {
                    return Err(anyhow::anyhow!("duplicate attach resize stream received"));
                }
                writer
                    .lock()
                    .await
                    .write_syn_reply(frame.stream_id, &[], false)
                    .await?;
            }
            Ok(spdy::Frame::Ping(frame)) => {
                writer.lock().await.write_ping(frame.id).await?;
            }
            Ok(spdy::Frame::GoAway(_)) => break,
            Ok(_) => {}
            Err(e) => {
                log::debug!("Attach input loop stopped: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn serve_portforward_websocket(
    on_upgrade: hyper::upgrade::OnUpgrade,
    ctx: PortForwardRequestContext,
    protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (mut reader, writer) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(writer));

    let ports = ctx.req.port.clone();
    let netns_path = ctx.netns_path.clone();
    let protocol = portforward_websocket_protocol(protocol);
    let activity = new_stream_activity(Instant::now());
    let mut data_channels: HashMap<u8, tokio::sync::mpsc::Sender<Option<Vec<u8>>>> = HashMap::new();

    loop {
        if stream_is_idle(&activity, Instant::now(), PORT_FORWARD_STREAM_IDLE_TIMEOUT) {
            write_websocket_frame(&mut *writer.lock().await, 0x8, &[]).await?;
            break;
        }

        let frame = match tokio::time::timeout(
            next_stream_idle_timeout(&activity, Instant::now(), PORT_FORWARD_STREAM_IDLE_TIMEOUT),
            read_websocket_frame(&mut reader),
        )
        .await
        {
            Ok(frame) => frame?,
            Err(_) => continue,
        };
        let Some(frame) = frame else {
            break;
        };
        mark_stream_activity(&activity, Instant::now());

        match frame.opcode {
            0x8 => break,
            0x9 => {
                write_websocket_frame(&mut *writer.lock().await, 0xA, &frame.payload).await?;
                mark_stream_activity(&activity, Instant::now());
            }
            0x1 | 0x2 => {
                let Some((channel, payload)) =
                    decode_portforward_websocket_frame(protocol, &frame)?
                else {
                    continue;
                };
                if channel % 2 != 0 {
                    continue;
                }
                let port_index = (channel / 2) as usize;
                if port_index >= ports.len() {
                    continue;
                }
                let port = ports[port_index] as u16;

                if !data_channels.contains_key(&channel) {
                    let (input_tx, mut input_rx) =
                        tokio::sync::mpsc::channel::<Option<Vec<u8>>>(32);
                    data_channels.insert(channel, input_tx.clone());
                    let writer_for_output = writer.clone();
                    let netns_for_output = netns_path.clone();
                    let protocol_for_output = protocol;
                    let activity_for_output = activity.clone();
                    tokio::spawn(async move {
                        let error_channel = channel + 1;
                        let port_bytes = port.to_le_bytes();
                        let _ = write_portforward_websocket_channel_frame(
                            &writer_for_output,
                            &activity_for_output,
                            protocol_for_output,
                            channel,
                            &port_bytes,
                        )
                        .await;
                        let _ = write_portforward_websocket_channel_frame(
                            &writer_for_output,
                            &activity_for_output,
                            protocol_for_output,
                            error_channel,
                            &port_bytes,
                        )
                        .await;
                        let tcp_stream =
                            match connect_to_port_in_netns(netns_for_output, port).await {
                                Ok(stream) => stream,
                                Err(err) => {
                                    let _ = write_portforward_websocket_channel_frame(
                                        &writer_for_output,
                                        &activity_for_output,
                                        protocol_for_output,
                                        error_channel,
                                        err.to_string().as_bytes(),
                                    )
                                    .await;
                                    return;
                                }
                            };

                        let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();
                        let writer_for_reader = writer_for_output.clone();
                        let activity_for_reader = activity_for_output.clone();
                        let reader_task = tokio::spawn(async move {
                            let mut buffer = [0u8; 8192];
                            loop {
                                match tcp_read.read(&mut buffer).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        if write_portforward_websocket_channel_frame(
                                            &writer_for_reader,
                                            &activity_for_reader,
                                            protocol_for_output,
                                            channel,
                                            &buffer[..n],
                                        )
                                        .await
                                        .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    Err(err) => {
                                        let _ = write_portforward_websocket_channel_frame(
                                            &writer_for_reader,
                                            &activity_for_reader,
                                            protocol_for_output,
                                            error_channel,
                                            format!("port-forward read failed: {}", err).as_bytes(),
                                        )
                                        .await;
                                        return;
                                    }
                                }
                            }
                        });

                        while let Some(message) = input_rx.recv().await {
                            match message {
                                Some(data) if !data.is_empty() => {
                                    if tcp_write.write_all(&data).await.is_err() {
                                        break;
                                    }
                                }
                                Some(_) => {}
                                None => {
                                    let _ = tcp_write.shutdown().await;
                                    break;
                                }
                            }
                        }

                        let _ = reader_task.await;
                    });
                }

                if let Some(tx) = data_channels.get(&channel) {
                    if !payload.is_empty() {
                        let _ = tx.send(Some(payload)).await;
                    }
                }
            }
            _ => {}
        }
    }

    for tx in data_channels.into_values() {
        let _ = tx.send(None).await;
    }
    write_websocket_frame(&mut *writer.lock().await, 0x8, &[]).await?;
    mark_stream_activity(&activity, Instant::now());
    Ok(())
}

async fn serve_attach_log_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    ctx: AttachLogRequestContext,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (read_half, write_half) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(spdy::AsyncSpdyWriter::new(write_half)));
    let mut reader = read_half;

    let req = ctx.req;
    let expected_roles = expected_attach_roles(&req);
    let mut header_decompressor = spdy::HeaderDecompressor::new();
    let mut stdout_stream = None;
    let mut stderr_stream = None;
    let mut error_stream = None;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while [error_stream, stdout_stream, stderr_stream]
        .into_iter()
        .flatten()
        .count()
        < expected_roles.len()
    {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, spdy::read_frame_async(&mut reader)).await??;
        match frame {
            spdy::Frame::SynStream(frame) => {
                let headers =
                    spdy::decode_header_block(&frame.header_block, &mut header_decompressor)?;
                let stream_type = spdy::header_value(&headers, "streamtype")
                    .ok_or_else(|| anyhow::anyhow!("attach stream is missing streamtype header"))?;
                let role = match stream_type {
                    "error" => AttachStreamRole::Error,
                    "stdin" => AttachStreamRole::Stdin,
                    "stdout" => AttachStreamRole::Stdout,
                    "stderr" => AttachStreamRole::Stderr,
                    "resize" => AttachStreamRole::Resize,
                    other => {
                        return Err(anyhow::anyhow!(
                            "unsupported attach-log streamtype {}",
                            other
                        ))
                    }
                };

                if matches!(role, AttachStreamRole::Stdin | AttachStreamRole::Resize) {
                    writer
                        .lock()
                        .await
                        .write_syn_reply(frame.stream_id, &[], false)
                        .await?;
                    continue;
                }

                if !expected_roles.contains(&role) {
                    return Err(anyhow::anyhow!(
                        "unexpected attach-log stream {:?} for request",
                        role
                    ));
                }

                match role {
                    AttachStreamRole::Error if error_stream.is_none() => {
                        error_stream = Some(frame.stream_id)
                    }
                    AttachStreamRole::Stdout if stdout_stream.is_none() => {
                        stdout_stream = Some(frame.stream_id)
                    }
                    AttachStreamRole::Stderr if stderr_stream.is_none() => {
                        stderr_stream = Some(frame.stream_id)
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "duplicate attach-log stream {:?} received",
                            role
                        ))
                    }
                }

                writer
                    .lock()
                    .await
                    .write_syn_reply(frame.stream_id, &[], false)
                    .await?;
            }
            spdy::Frame::Ping(frame) => {
                writer.lock().await.write_ping(frame.id).await?;
            }
            _ => {}
        }
    }

    let message = "attach socket missing; falling back to read-only log stream";
    write_error_stream(&writer, error_stream, message).await?;
    if let Some(stream_id) = error_stream {
        writer.lock().await.write_data(stream_id, &[], true).await?;
    }

    let writer_for_logs = writer.clone();
    let log_path = ctx.log_path.clone();
    let stdout_stream_for_logs = stdout_stream;
    let stderr_stream_for_logs = stderr_stream;
    tokio::spawn(async move {
        let mut state = AttachLogFollowState::default();
        loop {
            match read_attach_log_records(&log_path, &mut state).await {
                Ok(records) => {
                    for record in records {
                        let target_stream = match record.stream.as_str() {
                            "stdout" => stdout_stream_for_logs,
                            "stderr" => stderr_stream_for_logs.or(stdout_stream_for_logs),
                            _ => None,
                        };

                        if let Some(stream_id) = target_stream {
                            if writer_for_logs
                                .lock()
                                .await
                                .write_data(stream_id, &record.payload, false)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::debug!(
                        "Attach log fallback could not read {}: {}",
                        log_path.display(),
                        err
                    );
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    loop {
        match spdy::read_frame_async(&mut reader).await {
            Ok(spdy::Frame::Ping(frame)) => {
                writer.lock().await.write_ping(frame.id).await?;
            }
            Ok(spdy::Frame::GoAway(_)) => break,
            Ok(spdy::Frame::Data(_)) => {}
            Ok(_) => {}
            Err(e) => {
                log::debug!("Attach log fallback input loop stopped: {}", e);
                break;
            }
        }
    }

    if let Some(stream_id) = stdout_stream.or(stderr_stream).or(error_stream) {
        let mut writer = writer.lock().await;
        if let Some(stdout_stream) = stdout_stream {
            let _ = writer.write_data(stdout_stream, &[], true).await;
        }
        if let Some(stderr_stream) = stderr_stream {
            let _ = writer.write_data(stderr_stream, &[], true).await;
        }
        let _ = writer.write_goaway(stream_id).await;
    }

    Ok(())
}

async fn serve_attach_websocket(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: AttachRequest,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (mut reader, writer) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(writer));

    let attach_socket_path = shim_socket_path(&req.container_id, "attach.sock");
    let shim = UnixStream::connect(&attach_socket_path)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to connect attach socket {}: {}",
                attach_socket_path.display(),
                e
            )
        })?;
    let (mut shim_read, shim_write) = shim.into_split();
    let shim_write = Arc::new(Mutex::new(shim_write));

    let mut resize_socket = if req.tty {
        let resize_socket_path = shim_socket_path(&req.container_id, "resize.sock");
        UnixStream::connect(&resize_socket_path).await.ok()
    } else {
        None
    };

    let writer_for_output = writer.clone();
    let stdout_enabled = req.stdout;
    let stderr_enabled = req.stderr;
    let tty = req.tty;
    tokio::spawn(async move {
        let mut decoder = AttachOutputDecoder::default();
        let mut buffer = [0u8; 8192];
        loop {
            match shim_read.read(&mut buffer).await {
                Ok(0) => {
                    let _ =
                        write_websocket_frame(&mut *writer_for_output.lock().await, 0x8, &[]).await;
                    if let Err(e) = decoder.finish() {
                        log::debug!(
                            "Attach websocket decoder finished with trailing data: {}",
                            e
                        );
                    }
                    break;
                }
                Ok(n) => match decoder.push(&buffer[..n]) {
                    Ok(frames) => {
                        for frame in frames {
                            let channel = match frame.pipe {
                                ATTACH_PIPE_STDOUT => Some(WS_CHANNEL_STDOUT),
                                ATTACH_PIPE_STDERR if stderr_enabled && !tty => {
                                    Some(WS_CHANNEL_STDERR)
                                }
                                ATTACH_PIPE_STDERR if stdout_enabled => Some(WS_CHANNEL_STDOUT),
                                _ => None,
                            };
                            if let Some(channel) = channel {
                                if write_websocket_channel_frame(
                                    &writer_for_output,
                                    channel,
                                    &frame.payload,
                                )
                                .await
                                .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let _ = write_websocket_channel_frame(
                            &writer_for_output,
                            WS_CHANNEL_ERROR,
                            err.to_string().as_bytes(),
                        )
                        .await;
                        break;
                    }
                },
                Err(err) => {
                    let _ = write_websocket_channel_frame(
                        &writer_for_output,
                        WS_CHANNEL_ERROR,
                        format!("attach output error: {}", err).as_bytes(),
                    )
                    .await;
                    break;
                }
            }
        }
    });

    loop {
        let Some(frame) = read_websocket_frame(&mut reader).await? else {
            break;
        };
        match frame.opcode {
            0x8 => break,
            0x9 => {
                write_websocket_frame(&mut *writer.lock().await, 0xA, &frame.payload).await?;
            }
            0x2 | 0x1 => {
                if frame.payload.is_empty() {
                    continue;
                }
                let channel = frame.payload[0];
                let payload = &frame.payload[1..];
                match channel {
                    WS_CHANNEL_STDIN if req.stdin => {
                        let mut shim_write = shim_write.lock().await;
                        if !payload.is_empty() {
                            shim_write.write_all(payload).await?;
                        }
                    }
                    WS_CHANNEL_RESIZE if req.tty => {
                        if payload.is_empty() {
                            continue;
                        }
                        if resize_socket.is_none() {
                            let resize_socket_path =
                                shim_socket_path(&req.container_id, "resize.sock");
                            resize_socket = UnixStream::connect(&resize_socket_path).await.ok();
                        }
                        if let Some(socket) = resize_socket.as_mut() {
                            socket.write_all(payload).await?;
                            if !payload.ends_with(b"\n") {
                                socket.write_all(b"\n").await?;
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    let _ = shim_write.lock().await.shutdown().await;
    Ok(())
}

async fn serve_attach_log_websocket(
    on_upgrade: hyper::upgrade::OnUpgrade,
    ctx: AttachLogRequestContext,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (mut reader, writer) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(writer));

    write_websocket_channel_frame(
        &writer,
        WS_CHANNEL_ERROR,
        b"attach socket missing; falling back to read-only log stream",
    )
    .await?;

    let writer_for_logs = writer.clone();
    let log_path = ctx.log_path.clone();
    let stdout_enabled = ctx.req.stdout;
    let stderr_enabled = ctx.req.stderr;
    tokio::spawn(async move {
        let mut state = AttachLogFollowState::default();
        loop {
            match read_attach_log_records(&log_path, &mut state).await {
                Ok(records) => {
                    for record in records {
                        let channel = match record.stream.as_str() {
                            "stdout" if stdout_enabled => Some(WS_CHANNEL_STDOUT),
                            "stderr" if stderr_enabled => Some(WS_CHANNEL_STDERR),
                            "stderr" if stdout_enabled => Some(WS_CHANNEL_STDOUT),
                            _ => None,
                        };
                        if let Some(channel) = channel {
                            if write_websocket_channel_frame(
                                &writer_for_logs,
                                channel,
                                &record.payload,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    let _ = write_websocket_channel_frame(
                        &writer_for_logs,
                        WS_CHANNEL_ERROR,
                        format!("attach log fallback read failed: {}", err).as_bytes(),
                    )
                    .await;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    loop {
        let Some(frame) = read_websocket_frame(&mut reader).await? else {
            break;
        };
        match frame.opcode {
            0x8 => break,
            0x9 => {
                write_websocket_frame(&mut *writer.lock().await, 0xA, &frame.payload).await?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn response(status: StatusCode, body: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| Response::new(Body::from(body.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body::to_bytes;
    use std::fs;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::sync::mpsc;
    use std::thread;
    use tempfile::tempdir;

    fn parse_http_url(url: &str) -> (String, u16, String) {
        let remainder = url
            .strip_prefix("http://")
            .expect("streaming test URLs must be http");
        let (authority, path) = remainder.split_once('/').expect("missing URL path");
        let (host, port) = authority.rsplit_once(':').expect("missing URL port");
        (
            host.to_string(),
            port.parse().expect("invalid URL port"),
            format!("/{}", path),
        )
    }

    fn read_http_response_head(stream: &mut StdTcpStream) -> String {
        let mut response = Vec::new();
        let mut byte = [0u8; 1];
        while response.len() < 8192 {
            stream.read_exact(&mut byte).unwrap();
            response.push(byte[0]);
            if response.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8(response).unwrap()
    }

    async fn response_body_string(response: Response<Body>) -> String {
        let bytes = to_bytes(response.into_body()).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_exec_url_generation() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let req = ExecRequest {
            container_id: "abc".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        };

        let response = server.get_exec(&req).await.unwrap();
        assert!(response.url.contains("/exec/"));
        assert!(response.url.starts_with("http://127.0.0.1:12345"));
    }

    #[test]
    fn test_validate_exec_request_rejects_empty_command() {
        let req = ExecRequest {
            container_id: "abc".to_string(),
            cmd: Vec::new(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        };

        let err = StreamingServer::validate_exec_request(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_expected_exec_roles_omit_stderr_for_tty() {
        let req = ExecRequest {
            container_id: "abc".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        };

        assert_eq!(
            expected_exec_roles(&req),
            vec![
                ExecStreamRole::Error,
                ExecStreamRole::Stdin,
                ExecStreamRole::Stdout
            ]
        );
    }

    #[tokio::test]
    async fn test_attach_url_generation() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let req = AttachRequest {
            container_id: "abc".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        };

        let response = server.get_attach(&req).await.unwrap();
        assert!(response.url.contains("/attach/"));
    }

    #[tokio::test]
    async fn test_exec_transport_explicitly_rejects_non_spdy_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::Exec(ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            }))
            .await;
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/exec/{}", token))
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("SPDY or websocket upgrade headers"));
    }

    #[tokio::test]
    async fn test_exec_transport_accepts_websocket_upgrade_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::Exec(ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            }))
            .await;
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/exec/{}", token))
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_VERSION, "13")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response
                .headers()
                .get(UPGRADE)
                .and_then(|value| value.to_str().ok()),
            Some("websocket")
        );
    }

    #[tokio::test]
    async fn test_attach_transport_explicitly_rejects_non_spdy_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::Attach(AttachRequest {
                container_id: "abc".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }))
            .await;
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/attach/{}", token))
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("SPDY or websocket upgrade headers"));
    }

    #[tokio::test]
    async fn test_attach_log_url_generation() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let req = AttachRequest {
            container_id: "abc".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        };

        let response = server
            .get_attach_log(&req, PathBuf::from("/var/log/pods/abc.log"))
            .await
            .unwrap();
        assert!(response.url.contains("/attach/"));
        assert!(response.url.starts_with("http://127.0.0.1:12345"));
    }

    #[tokio::test]
    async fn test_attach_transport_accepts_websocket_upgrade_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::Attach(AttachRequest {
                container_id: "abc".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            }))
            .await;
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/attach/{}", token))
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_VERSION, "13")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response
                .headers()
                .get(UPGRADE)
                .and_then(|value| value.to_str().ok()),
            Some("websocket")
        );
    }

    #[tokio::test]
    async fn test_attach_log_transport_accepts_websocket_upgrade_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::AttachLog(AttachLogRequestContext {
                req: AttachRequest {
                    container_id: "abc".to_string(),
                    stdin: false,
                    stdout: true,
                    stderr: true,
                    tty: false,
                },
                log_path: PathBuf::from("/var/log/pods/abc.log"),
            }))
            .await;
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/attach/{}", token))
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_VERSION, "13")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response
                .headers()
                .get(UPGRADE)
                .and_then(|value| value.to_str().ok()),
            Some("websocket")
        );
    }

    #[tokio::test]
    async fn test_portforward_url_generation() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let req = PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: vec![8080, 9090],
        };

        let response = server
            .get_port_forward(&req, PathBuf::from("/proc/self/ns/net"))
            .await
            .unwrap();
        assert!(response.url.contains("/portforward/"));
        assert!(response.url.starts_with("http://127.0.0.1:12345"));
    }

    #[tokio::test]
    async fn test_portforward_transport_explicitly_rejects_non_spdy_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
                req: PortForwardRequest {
                    pod_sandbox_id: "pod-1".to_string(),
                    port: vec![8080],
                },
                netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            }))
            .await;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/portforward/{}", token))
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("SPDY or websocket upgrade headers"));
    }

    #[tokio::test]
    async fn test_portforward_transport_accepts_websocket_upgrade_requests() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
                req: PortForwardRequest {
                    pod_sandbox_id: "pod-1".to_string(),
                    port: vec![8080],
                },
                netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            }))
            .await;
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/portforward/{}", token))
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_VERSION, "13")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(SEC_WEBSOCKET_PROTOCOL, PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response
                .headers()
                .get(UPGRADE)
                .and_then(|value| value.to_str().ok()),
            Some("websocket")
        );
        assert_eq!(
            response
                .headers()
                .get(SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok()),
            Some(PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
        );
    }

    #[tokio::test]
    async fn test_portforward_transport_requires_supported_protocol_version() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
                req: PortForwardRequest {
                    pod_sandbox_id: "pod-1".to_string(),
                    port: vec![8080],
                },
                netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            }))
            .await;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/portforward/{}", token))
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, spdy::SPDY_31)
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(response_body_string(response)
            .await
            .contains("no supported port-forward protocol was requested"));
    }

    #[tokio::test]
    async fn test_portforward_route_rejects_token_kind_mismatch() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request(StreamingRequest::Exec(ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            }))
            .await;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/portforward/{}", token))
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("streaming token kind mismatch"));
    }

    #[tokio::test]
    async fn test_expired_streaming_token_is_rejected() {
        let server = StreamingServer::for_test("http://127.0.0.1:12345");
        let token = server
            .insert_request_for_test(
                StreamingRequest::Exec(ExecRequest {
                    container_id: "abc".to_string(),
                    cmd: vec!["sh".to_string()],
                    stdin: false,
                    stdout: true,
                    stderr: true,
                    tty: false,
                }),
                STREAMING_REQUEST_TTL + Duration::from_secs(1),
            )
            .await;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/exec/{}", token))
            .body(Body::empty())
            .unwrap();

        let response =
            handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response_body_string(response)
            .await
            .contains("streaming token not found"));
    }

    #[test]
    fn test_validate_portforward_request_rejects_invalid_values() {
        let missing_pod = PortForwardRequest {
            pod_sandbox_id: String::new(),
            port: vec![8080],
        };
        let err = StreamingServer::validate_port_forward_request(&missing_pod).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let missing_ports = PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: Vec::new(),
        };
        StreamingServer::validate_port_forward_request(&missing_ports).unwrap();

        let invalid_port = PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: vec![0],
        };
        let err = StreamingServer::validate_port_forward_request(&invalid_port).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_portforward_request_id_falls_back_from_stream_id() {
        assert_eq!(
            portforward_request_id(5, "error", None).unwrap(),
            "5".to_string()
        );
        assert_eq!(
            portforward_request_id(7, "data", None).unwrap(),
            "5".to_string()
        );
        assert!(portforward_request_id(1, "data", None).is_err());
        assert_eq!(
            portforward_request_id(11, "data", Some("42")).unwrap(),
            "42".to_string()
        );
    }

    #[test]
    fn test_register_portforward_stream_rejects_duplicate_pending_data_stream() {
        let now = Instant::now();
        let mut pending_pairs = HashMap::new();
        let active_request_ids = HashSet::new();

        assert!(matches!(
            register_portforward_stream(
                &mut pending_pairs,
                &active_request_ids,
                now,
                1,
                PortForwardStreamRole::Data,
                "1".to_string(),
                8080,
            ),
            PortForwardStreamRegistration::Pending
        ));

        match register_portforward_stream(
            &mut pending_pairs,
            &active_request_ids,
            now,
            3,
            PortForwardStreamRole::Data,
            "1".to_string(),
            8080,
        ) {
            PortForwardStreamRegistration::RejectPendingPair { pair, message } => {
                assert_eq!(pair.request_id, "1");
                assert_eq!(pair.data_stream, Some(1));
                assert!(pair.error_stream.is_none());
                assert!(message.contains("duplicate"));
            }
            other => panic!("unexpected registration result: {:?}", other),
        }
        assert!(pending_pairs.is_empty());
    }

    #[test]
    fn test_register_portforward_stream_rejects_conflicting_port_for_same_request() {
        let now = Instant::now();
        let mut pending_pairs = HashMap::new();
        let active_request_ids = HashSet::new();

        assert!(matches!(
            register_portforward_stream(
                &mut pending_pairs,
                &active_request_ids,
                now,
                1,
                PortForwardStreamRole::Data,
                "7".to_string(),
                8080,
            ),
            PortForwardStreamRegistration::Pending
        ));

        match register_portforward_stream(
            &mut pending_pairs,
            &active_request_ids,
            now,
            3,
            PortForwardStreamRole::Error,
            "7".to_string(),
            9090,
        ) {
            PortForwardStreamRegistration::RejectPendingPair { pair, message } => {
                assert_eq!(pair.request_id, "7");
                assert_eq!(pair.port, 8080);
                assert!(message.contains("conflicting ports"));
            }
            other => panic!("unexpected registration result: {:?}", other),
        }
        assert!(pending_pairs.is_empty());
    }

    #[test]
    fn test_register_portforward_stream_rejects_reuse_of_active_request_id() {
        let now = Instant::now();
        let mut pending_pairs = HashMap::new();
        let active_request_ids = HashSet::from(["5".to_string()]);

        match register_portforward_stream(
            &mut pending_pairs,
            &active_request_ids,
            now,
            9,
            PortForwardStreamRole::Data,
            "5".to_string(),
            8080,
        ) {
            PortForwardStreamRegistration::RejectCurrent(message) => {
                assert!(message.contains("already has an active stream pair"));
            }
            other => panic!("unexpected registration result: {:?}", other),
        }
        assert!(pending_pairs.is_empty());
    }

    #[test]
    fn test_take_expired_portforward_pairs_returns_only_timed_out_pairs() {
        let now = Instant::now();
        let mut pending_pairs = HashMap::from([
            (
                "expired".to_string(),
                PortForwardPair {
                    request_id: "expired".to_string(),
                    port: 8080,
                    data_stream: Some(1),
                    error_stream: None,
                    created_at: now - PORT_FORWARD_STREAM_CREATION_TIMEOUT - Duration::from_secs(1),
                },
            ),
            (
                "fresh".to_string(),
                PortForwardPair {
                    request_id: "fresh".to_string(),
                    port: 9090,
                    data_stream: Some(3),
                    error_stream: None,
                    created_at: now,
                },
            ),
        ]);

        let expired = take_expired_portforward_pairs(&mut pending_pairs, now);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].request_id, "expired");
        assert!(pending_pairs.contains_key("fresh"));
        assert!(!pending_pairs.contains_key("expired"));
    }

    #[test]
    fn test_next_portforward_pair_timeout_uses_earliest_pending_deadline() {
        let now = Instant::now();
        let pending_pairs = HashMap::from([
            (
                "first".to_string(),
                PortForwardPair {
                    request_id: "first".to_string(),
                    port: 8080,
                    data_stream: Some(1),
                    error_stream: None,
                    created_at: now - Duration::from_secs(10),
                },
            ),
            (
                "second".to_string(),
                PortForwardPair {
                    request_id: "second".to_string(),
                    port: 9090,
                    data_stream: Some(3),
                    error_stream: None,
                    created_at: now - Duration::from_secs(2),
                },
            ),
        ]);

        let timeout = next_portforward_pair_timeout(&pending_pairs, now).unwrap();
        assert_eq!(
            timeout,
            PORT_FORWARD_STREAM_CREATION_TIMEOUT - Duration::from_secs(10)
        );
    }

    #[test]
    fn test_stream_idle_helpers_use_latest_activity() {
        let now = Instant::now();
        let activity = Arc::new(std::sync::Mutex::new(now - Duration::from_secs(5)));

        assert_eq!(
            stream_idle_remaining(&activity, now, Duration::from_secs(10)),
            Some(Duration::from_secs(5))
        );
        assert!(!stream_is_idle(&activity, now, Duration::from_secs(10)));
        assert_eq!(
            next_stream_idle_timeout(&activity, now, Duration::from_secs(10)),
            Duration::from_secs(5)
        );

        mark_stream_activity(&activity, now);
        assert_eq!(
            stream_idle_remaining(&activity, now, Duration::from_secs(10)),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn test_next_portforward_wait_timeout_prefers_earlier_pair_timeout_than_idle_timeout() {
        let now = Instant::now();
        let activity = Arc::new(std::sync::Mutex::new(now));
        let pending_pairs = HashMap::from([(
            "first".to_string(),
            PortForwardPair {
                request_id: "first".to_string(),
                port: 8080,
                data_stream: Some(1),
                error_stream: None,
                created_at: now - Duration::from_secs(29),
            },
        )]);

        assert_eq!(
            next_portforward_wait_timeout(&pending_pairs, &activity, now, Duration::from_secs(60)),
            Duration::from_secs(1)
        );
    }

    #[tokio::test]
    async fn test_write_portforward_websocket_channel_frame_roundtrips_v4_binary() {
        let (writer_stream, mut reader_stream) = tokio::io::duplex(128);
        let writer = Arc::new(Mutex::new(writer_stream));
        let activity = new_stream_activity(Instant::now() - Duration::from_secs(5));

        write_portforward_websocket_channel_frame(
            &writer,
            &activity,
            PortForwardWebsocketProtocol::V4Binary,
            2,
            b"hello",
        )
        .await
        .unwrap();

        let frame = read_websocket_frame(&mut reader_stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, 0x2);
        let decoded =
            decode_portforward_websocket_frame(PortForwardWebsocketProtocol::V4Binary, &frame)
                .unwrap()
                .unwrap();
        assert_eq!(decoded.0, 2);
        assert_eq!(decoded.1, b"hello");
        assert!(!stream_is_idle(
            &activity,
            Instant::now(),
            Duration::from_secs(1)
        ));
    }

    #[tokio::test]
    async fn test_write_portforward_websocket_channel_frame_roundtrips_v4_base64() {
        let (writer_stream, mut reader_stream) = tokio::io::duplex(128);
        let writer = Arc::new(Mutex::new(writer_stream));
        let activity = new_stream_activity(Instant::now() - Duration::from_secs(5));

        write_portforward_websocket_channel_frame(
            &writer,
            &activity,
            PortForwardWebsocketProtocol::V4Base64,
            3,
            b"hello",
        )
        .await
        .unwrap();

        let frame = read_websocket_frame(&mut reader_stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, 0x1);
        let decoded =
            decode_portforward_websocket_frame(PortForwardWebsocketProtocol::V4Base64, &frame)
                .unwrap()
                .unwrap();
        assert_eq!(decoded.0, 3);
        assert_eq!(decoded.1, b"hello");
        assert!(!stream_is_idle(
            &activity,
            Instant::now(),
            Duration::from_secs(1)
        ));
    }

    #[test]
    fn test_negotiate_portforward_protocol_accepts_v1() {
        let request = Request::builder()
            .header("X-Stream-Protocol-Version", PORT_FORWARD_PROTOCOL_V1)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            negotiate_portforward_protocol(&request),
            Some(PORT_FORWARD_PROTOCOL_V1)
        );
    }

    #[test]
    fn test_negotiate_portforward_websocket_protocol_prefers_v4() {
        let request = Request::builder()
            .header(
                SEC_WEBSOCKET_PROTOCOL,
                format!(
                    "{}, {}",
                    PORT_FORWARD_PROTOCOL_V1, PORT_FORWARD_WS_PROTOCOL_V4_BINARY
                ),
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            negotiate_portforward_websocket_protocol(&request),
            Some(PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
        );

        let request = Request::builder()
            .header(SEC_WEBSOCKET_PROTOCOL, PORT_FORWARD_WS_PROTOCOL_V4_BASE64)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            negotiate_portforward_websocket_protocol(&request),
            Some(PORT_FORWARD_WS_PROTOCOL_V4_BASE64)
        );
    }

    #[test]
    fn test_parse_terminal_size_accepts_kubernetes_payload() {
        assert_eq!(
            parse_terminal_size(br#"{"Width":120,"Height":40}"#).unwrap(),
            (120, 40)
        );
        assert_eq!(
            parse_terminal_size(br#"{"width":80,"height":24}"#).unwrap(),
            (80, 24)
        );
    }

    #[test]
    fn test_parse_cri_text_log_record_full_appends_newline() {
        let record =
            parse_cri_text_log_record(b"2024-01-02T03:04:05.000000000Z stdout F hello world")
                .unwrap();
        assert_eq!(record.stream, "stdout");
        assert_eq!(record.payload, b"hello world\n");
    }

    #[test]
    fn test_parse_cri_text_log_record_partial_preserves_open_line() {
        let record =
            parse_cri_text_log_record(b"2024-01-02T03:04:05.000000000Z stderr P partial line")
                .unwrap();
        assert_eq!(record.stream, "stderr");
        assert_eq!(record.payload, b"partial line");
    }

    #[test]
    fn test_parse_cri_text_log_record_accepts_future_full_tags() {
        let record = parse_cri_text_log_record(
            b"2024-01-02T03:04:05.000000000Z stdout X:meta future compatible",
        )
        .unwrap();
        assert_eq!(record.stream, "stdout");
        assert_eq!(record.payload, b"future compatible\n");
    }

    #[test]
    fn test_parse_cri_text_log_record_uses_first_tag_for_partial_semantics() {
        let record =
            parse_cri_text_log_record(b"2024-01-02T03:04:05.000000000Z stderr P:meta partial line")
                .unwrap();
        assert_eq!(record.stream, "stderr");
        assert_eq!(record.payload, b"partial line");
    }

    #[test]
    fn test_decode_portforward_websocket_frame_supports_base64_v4() {
        let payload = {
            let mut payload = vec![b'0'];
            payload.extend_from_slice(
                base64::engine::general_purpose::STANDARD
                    .encode(b"hello")
                    .as_bytes(),
            );
            payload
        };
        let frame = WebSocketFrame {
            opcode: 0x1,
            payload,
        };
        let decoded =
            decode_portforward_websocket_frame(PortForwardWebsocketProtocol::V4Base64, &frame)
                .unwrap()
                .unwrap();
        assert_eq!(decoded.0, 0);
        assert_eq!(decoded.1, b"hello");
    }

    #[tokio::test]
    async fn test_read_attach_log_records_starts_from_tail_and_handles_truncate() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("attach.log");
        fs::write(
            &log_path,
            b"2024-01-02T03:04:05.000000000Z stdout F old line\n",
        )
        .unwrap();

        let mut state = AttachLogFollowState::default();
        let initial = read_attach_log_records(&log_path, &mut state)
            .await
            .unwrap();
        assert!(
            initial.is_empty(),
            "initial attach fallback should start from the current end of file"
        );

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        file.write_all(b"2024-01-02T03:04:06.000000000Z stdout F next line\n")
            .unwrap();
        let appended = read_attach_log_records(&log_path, &mut state)
            .await
            .unwrap();
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].payload, b"next line\n");

        fs::write(
            &log_path,
            b"2024-01-02T03:04:07.000000000Z stdout F rotated line\n",
        )
        .unwrap();
        let rotated = read_attach_log_records(&log_path, &mut state)
            .await
            .unwrap();
        assert_eq!(rotated.len(), 1);
        assert_eq!(rotated[0].payload, b"rotated line\n");
    }

    #[test]
    fn test_parse_terminal_size_rejects_zero_values() {
        let err = parse_terminal_size(br#"{"Width":0,"Height":24}"#).unwrap_err();
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn test_shim_socket_path_honors_env_override() {
        std::env::set_var("CRIUS_SHIM_DIR", "/tmp/crius-shims");
        let socket_path = shim_socket_path("abc123", "attach.sock");
        assert_eq!(
            socket_path,
            PathBuf::from("/tmp/crius-shims/abc123/attach.sock")
        );
        std::env::remove_var("CRIUS_SHIM_DIR");
    }

    #[tokio::test]
    #[ignore = "requires local TCP bind permissions in the current test environment"]
    async fn test_portforward_spdy_roundtrip_single_port() {
        let backend = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_port = backend.local_addr().unwrap().port();
        let backend_thread = thread::spawn(move || {
            let (mut stream, _) = backend.accept().unwrap();
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).unwrap();
            stream.write_all(&buffer).unwrap();
        });

        let server = StreamingServer::start("127.0.0.1:0", PathBuf::from("/bin/false"))
            .await
            .unwrap();
        let response = server
            .get_port_forward(
                &PortForwardRequest {
                    pod_sandbox_id: "pod-1".to_string(),
                    port: vec![target_port as i32],
                },
                PathBuf::from("/proc/thread-self/ns/net"),
            )
            .await
            .unwrap();
        let (host, port, path) = parse_http_url(&response.url);

        let mut stream = StdTcpStream::connect((host.as_str(), port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\nX-Stream-Protocol-Version: {}\r\n\r\n",
            path, host, port, PORT_FORWARD_PROTOCOL_V1
        )
        .unwrap();
        stream.flush().unwrap();

        let response_head = read_http_response_head(&mut stream);
        assert!(
            response_head.starts_with("HTTP/1.1 101"),
            "unexpected upgrade response: {}",
            response_head
        );

        let writer_stream = stream.try_clone().unwrap();
        let mut writer = spdy::SpdyWriter::new(writer_stream);
        let data_headers = vec![
            ("streamtype".to_string(), "data".to_string()),
            ("port".to_string(), target_port.to_string()),
            ("requestid".to_string(), "1".to_string()),
        ];
        let error_headers = vec![
            ("streamtype".to_string(), "error".to_string()),
            ("port".to_string(), target_port.to_string()),
            ("requestid".to_string(), "1".to_string()),
        ];
        writer.write_syn_stream(1, 0, &data_headers, false).unwrap();
        writer
            .write_syn_stream(3, 0, &error_headers, false)
            .unwrap();
        writer.write_data(1, b"hello port-forward", true).unwrap();

        let mut syn_reply_count = 0usize;
        let mut data_payload = Vec::new();
        let mut error_payload = Vec::new();
        let mut saw_data_fin = false;
        let mut saw_error_fin = false;
        while !(syn_reply_count >= 2 && saw_data_fin && saw_error_fin) {
            match spdy::read_frame(&mut stream).unwrap() {
                spdy::Frame::SynReply(_) => {
                    syn_reply_count += 1;
                }
                spdy::Frame::Data(frame) if frame.stream_id == 1 => {
                    data_payload.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_data_fin = true;
                    }
                }
                spdy::Frame::Data(frame) if frame.stream_id == 3 => {
                    error_payload.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_error_fin = true;
                    }
                }
                spdy::Frame::GoAway(_) => break,
                other => panic!("unexpected SPDY frame: {:?}", other),
            }
        }

        assert_eq!(data_payload, b"hello port-forward");
        assert!(
            error_payload.is_empty(),
            "unexpected port-forward error payload: {}",
            String::from_utf8_lossy(&error_payload)
        );
        backend_thread.join().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires local TCP bind permissions in the current test environment"]
    async fn test_portforward_spdy_roundtrip_multiple_ports() {
        let backend_a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_port_a = backend_a.local_addr().unwrap().port();
        let backend_a_thread = thread::spawn(move || {
            let (mut stream, _) = backend_a.accept().unwrap();
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).unwrap();
            let mut reply = b"reply-a:".to_vec();
            reply.extend_from_slice(&buffer);
            stream.write_all(&reply).unwrap();
        });

        let backend_b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_port_b = backend_b.local_addr().unwrap().port();
        let backend_b_thread = thread::spawn(move || {
            let (mut stream, _) = backend_b.accept().unwrap();
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).unwrap();
            let mut reply = b"reply-b:".to_vec();
            reply.extend_from_slice(&buffer);
            stream.write_all(&reply).unwrap();
        });

        let server = StreamingServer::start("127.0.0.1:0", PathBuf::from("/bin/false"))
            .await
            .unwrap();
        let response = server
            .get_port_forward(
                &PortForwardRequest {
                    pod_sandbox_id: "pod-1".to_string(),
                    port: vec![target_port_a as i32, target_port_b as i32],
                },
                PathBuf::from("/proc/thread-self/ns/net"),
            )
            .await
            .unwrap();
        let (host, port, path) = parse_http_url(&response.url);

        let mut stream = StdTcpStream::connect((host.as_str(), port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\nX-Stream-Protocol-Version: {}\r\n\r\n",
            path, host, port, PORT_FORWARD_PROTOCOL_V1
        )
        .unwrap();
        stream.flush().unwrap();

        let response_head = read_http_response_head(&mut stream);
        assert!(
            response_head.starts_with("HTTP/1.1 101"),
            "unexpected upgrade response: {}",
            response_head
        );

        let writer_stream = stream.try_clone().unwrap();
        let mut writer = spdy::SpdyWriter::new(writer_stream);
        let data_headers_a = vec![
            ("streamtype".to_string(), "data".to_string()),
            ("port".to_string(), target_port_a.to_string()),
            ("requestid".to_string(), "1".to_string()),
        ];
        let error_headers_a = vec![
            ("streamtype".to_string(), "error".to_string()),
            ("port".to_string(), target_port_a.to_string()),
            ("requestid".to_string(), "1".to_string()),
        ];
        let data_headers_b = vec![
            ("streamtype".to_string(), "data".to_string()),
            ("port".to_string(), target_port_b.to_string()),
            ("requestid".to_string(), "2".to_string()),
        ];
        let error_headers_b = vec![
            ("streamtype".to_string(), "error".to_string()),
            ("port".to_string(), target_port_b.to_string()),
            ("requestid".to_string(), "2".to_string()),
        ];
        writer
            .write_syn_stream(1, 0, &data_headers_a, false)
            .unwrap();
        writer
            .write_syn_stream(3, 0, &error_headers_a, false)
            .unwrap();
        writer
            .write_syn_stream(5, 0, &data_headers_b, false)
            .unwrap();
        writer
            .write_syn_stream(7, 0, &error_headers_b, false)
            .unwrap();
        writer.write_data(1, b"hello-a", true).unwrap();
        writer.write_data(5, b"hello-b", true).unwrap();

        let mut syn_reply_count = 0usize;
        let mut data_payload_a = Vec::new();
        let mut data_payload_b = Vec::new();
        let mut error_payload_a = Vec::new();
        let mut error_payload_b = Vec::new();
        let mut saw_data_fin_a = false;
        let mut saw_data_fin_b = false;
        let mut saw_error_fin_a = false;
        let mut saw_error_fin_b = false;
        while !(syn_reply_count >= 4
            && saw_data_fin_a
            && saw_data_fin_b
            && saw_error_fin_a
            && saw_error_fin_b)
        {
            match spdy::read_frame(&mut stream).unwrap() {
                spdy::Frame::SynReply(_) => {
                    syn_reply_count += 1;
                }
                spdy::Frame::Data(frame) if frame.stream_id == 1 => {
                    data_payload_a.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_data_fin_a = true;
                    }
                }
                spdy::Frame::Data(frame) if frame.stream_id == 3 => {
                    error_payload_a.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_error_fin_a = true;
                    }
                }
                spdy::Frame::Data(frame) if frame.stream_id == 5 => {
                    data_payload_b.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_data_fin_b = true;
                    }
                }
                spdy::Frame::Data(frame) if frame.stream_id == 7 => {
                    error_payload_b.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_error_fin_b = true;
                    }
                }
                spdy::Frame::GoAway(_) => break,
                other => panic!("unexpected SPDY frame: {:?}", other),
            }
        }

        assert_eq!(data_payload_a, b"reply-a:hello-a");
        assert_eq!(data_payload_b, b"reply-b:hello-b");
        assert!(
            error_payload_a.is_empty(),
            "unexpected port-forward error payload A: {}",
            String::from_utf8_lossy(&error_payload_a)
        );
        assert!(
            error_payload_b.is_empty(),
            "unexpected port-forward error payload B: {}",
            String::from_utf8_lossy(&error_payload_b)
        );

        backend_a_thread.join().unwrap();
        backend_b_thread.join().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires local TCP bind permissions in the current test environment"]
    async fn test_exec_spdy_interactive_tty_resize_roundtrip() {
        let temp_dir = tempdir().unwrap();
        let runtime_path = temp_dir.path().join("fake-runc.sh");
        fs::write(
            &runtime_path,
            r#"#!/bin/bash
set -eu
cmd="${1:-}"
if [ "$cmd" = "exec" ]; then
  shift
  if [ "${1:-}" = "-t" ]; then
    shift
  fi
  shift
  exec "$@"
fi
echo "unsupported command" >&2
exit 1
"#,
        )
        .unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&runtime_path, perms).unwrap();

        let server = StreamingServer::start("127.0.0.1:0", runtime_path)
            .await
            .unwrap();
        let response = server
            .get_exec(&ExecRequest {
                container_id: "container-1".to_string(),
                cmd: vec![
                    "sh".to_string(),
                    "-lc".to_string(),
                    "stty -echo; stty size; cat".to_string(),
                ],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            })
            .await
            .unwrap();
        let (host, port, path) = parse_http_url(&response.url);

        let mut stream = StdTcpStream::connect((host.as_str(), port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\nX-Stream-Protocol-Version: v4.channel.k8s.io\r\n\r\n",
            path, host, port
        )
        .unwrap();
        stream.flush().unwrap();

        let response_head = read_http_response_head(&mut stream);
        assert!(
            response_head.starts_with("HTTP/1.1 101"),
            "unexpected upgrade response: {}",
            response_head
        );

        let writer_stream = stream.try_clone().unwrap();
        let mut writer = spdy::SpdyWriter::new(writer_stream);
        writer
            .write_syn_stream(
                1,
                0,
                &[("streamtype".to_string(), "error".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_syn_stream(
                3,
                0,
                &[("streamtype".to_string(), "stdin".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_syn_stream(
                5,
                0,
                &[("streamtype".to_string(), "stdout".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_syn_stream(
                7,
                0,
                &[("streamtype".to_string(), "resize".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_data(7, br#"{"Width":101,"Height":37}"#, false)
            .unwrap();
        writer.write_data(3, b"hello-exec\n", true).unwrap();

        let mut stdout_payload = Vec::new();
        let mut error_payload = Vec::new();
        let mut saw_stdout_fin = false;
        let mut saw_error_fin = false;
        while !(saw_stdout_fin && saw_error_fin) {
            match spdy::read_frame(&mut stream).unwrap() {
                spdy::Frame::Data(frame) if frame.stream_id == 5 => {
                    stdout_payload.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_stdout_fin = true;
                    }
                }
                spdy::Frame::Data(frame) if frame.stream_id == 1 => {
                    error_payload.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_error_fin = true;
                    }
                }
                spdy::Frame::GoAway(_) => break,
                _ => {}
            }
        }

        let stdout_text = String::from_utf8_lossy(&stdout_payload);
        assert!(
            stdout_text.contains("37 101"),
            "stdout was: {}",
            stdout_text
        );
        assert!(
            stdout_text.contains("hello-exec"),
            "stdout was: {}",
            stdout_text
        );
        assert!(
            error_payload.is_empty(),
            "unexpected exec error payload: {}",
            String::from_utf8_lossy(&error_payload)
        );
    }

    #[tokio::test]
    #[ignore = "requires local TCP bind permissions in the current test environment"]
    async fn test_attach_spdy_interactive_tty_resize_roundtrip() {
        let temp_dir = tempdir().unwrap();
        let shim_root = temp_dir.path().join("shims");
        let container_dir = shim_root.join("container-1");
        fs::create_dir_all(&container_dir).unwrap();
        let attach_socket_path = container_dir.join("attach.sock");
        let resize_socket_path = container_dir.join("resize.sock");
        let _ = fs::remove_file(&attach_socket_path);
        let _ = fs::remove_file(&resize_socket_path);
        std::env::set_var("CRIUS_SHIM_DIR", &shim_root);

        let resize_listener = StdUnixListener::bind(&resize_socket_path).unwrap();
        let (resize_tx, resize_rx) = mpsc::channel();
        let resize_thread = thread::spawn(move || {
            let (stream, _) = resize_listener.accept().unwrap();
            let mut reader = std::io::BufReader::new(stream);
            let mut payload = String::new();
            use std::io::BufRead;
            reader.read_line(&mut payload).unwrap();
            resize_tx.send(payload).unwrap();
        });

        let attach_listener = StdUnixListener::bind(&attach_socket_path).unwrap();
        let attach_thread = thread::spawn(move || {
            let (mut stream, _) = attach_listener.accept().unwrap();
            let mut input = Vec::new();
            stream.read_to_end(&mut input).unwrap();
            let mut output =
                crate::attach::encode_attach_output_frame(ATTACH_PIPE_STDOUT, b"attach-ready\n");
            output.extend_from_slice(&crate::attach::encode_attach_output_frame(
                ATTACH_PIPE_STDOUT,
                &input,
            ));
            stream.write_all(&output).unwrap();
        });

        let server = StreamingServer::start("127.0.0.1:0", PathBuf::from("/bin/false"))
            .await
            .unwrap();
        let response = server
            .get_attach(&AttachRequest {
                container_id: "container-1".to_string(),
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            })
            .await
            .unwrap();
        let (host, port, path) = parse_http_url(&response.url);

        let mut stream = StdTcpStream::connect((host.as_str(), port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\nX-Stream-Protocol-Version: v4.channel.k8s.io\r\n\r\n",
            path, host, port
        )
        .unwrap();
        stream.flush().unwrap();

        let response_head = read_http_response_head(&mut stream);
        assert!(
            response_head.starts_with("HTTP/1.1 101"),
            "unexpected upgrade response: {}",
            response_head
        );

        let writer_stream = stream.try_clone().unwrap();
        let mut writer = spdy::SpdyWriter::new(writer_stream);
        writer
            .write_syn_stream(
                1,
                0,
                &[("streamtype".to_string(), "error".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_syn_stream(
                3,
                0,
                &[("streamtype".to_string(), "stdin".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_syn_stream(
                5,
                0,
                &[("streamtype".to_string(), "stdout".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_syn_stream(
                7,
                0,
                &[("streamtype".to_string(), "resize".to_string())],
                false,
            )
            .unwrap();
        writer
            .write_data(7, br#"{"Width":101,"Height":37}"#, false)
            .unwrap();
        writer.write_data(3, b"hello-attach\n", true).unwrap();

        let mut stdout_payload = Vec::new();
        let mut error_payload = Vec::new();
        let mut saw_stdout_fin = false;
        let mut saw_error_fin = false;
        while !(saw_stdout_fin && saw_error_fin) {
            match spdy::read_frame(&mut stream).unwrap() {
                spdy::Frame::Data(frame) if frame.stream_id == 5 => {
                    stdout_payload.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_stdout_fin = true;
                    }
                }
                spdy::Frame::Data(frame) if frame.stream_id == 1 => {
                    error_payload.extend_from_slice(&frame.data);
                    if frame.flags & 0x01 != 0 {
                        saw_error_fin = true;
                    }
                }
                spdy::Frame::GoAway(_) => break,
                _ => {}
            }
        }

        let stdout_text = String::from_utf8_lossy(&stdout_payload);
        assert!(
            stdout_text.contains("attach-ready"),
            "stdout was: {}",
            stdout_text
        );
        assert!(
            stdout_text.contains("hello-attach"),
            "stdout was: {}",
            stdout_text
        );
        assert!(
            error_payload.is_empty(),
            "unexpected attach error payload: {}",
            String::from_utf8_lossy(&error_payload)
        );

        let resize_payload = resize_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            resize_payload.contains("\"Width\":101") || resize_payload.contains("\"width\":101")
        );
        assert!(
            resize_payload.contains("\"Height\":37") || resize_payload.contains("\"height\":37")
        );

        attach_thread.join().unwrap();
        resize_thread.join().unwrap();
        std::env::remove_var("CRIUS_SHIM_DIR");
    }

    #[test]
    fn test_with_netns_path_allows_current_namespace() {
        let result = with_netns_path(Path::new("/proc/thread-self/ns/net"), || Ok(42)).unwrap();
        assert_eq!(result, 42);
    }
}
