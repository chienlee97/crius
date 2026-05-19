mod config;
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
use tokio::net::{TcpListener as TokioTcpListener, TcpStream, UnixStream};
use tokio::process::Command as TokioCommand;
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;

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

use config::load_streaming_tls_config;
pub use config::StreamingConfig;
pub(crate) use config::{deserialize_duration, parse_duration, serialize_duration};

#[derive(Debug, Clone)]
enum StreamingRequest {
    Exec(ExecRequestContext),
    Attach(AttachRequestContext),
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
    websocket_enabled: bool,
}

#[derive(Debug, Clone)]
struct AttachRequestContext {
    req: AttachRequest,
    websocket_enabled: bool,
}

#[derive(Debug, Clone)]
struct AttachLogRequestContext {
    req: AttachRequest,
    log_path: PathBuf,
    websocket_enabled: bool,
}

#[derive(Debug, Clone)]
struct ExecRequestContext {
    req: ExecRequest,
    runtime_path: PathBuf,
    runtime_config_path: PathBuf,
    exec_cpu_affinity: Option<usize>,
    exec_io_socket_path: Option<PathBuf>,
    exec_resize_socket_path: Option<PathBuf>,
    websocket_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ExecStreamOptions {
    pub runtime_path: PathBuf,
    pub runtime_config_path: PathBuf,
    pub exec_cpu_affinity: Option<usize>,
    pub exec_io_socket_path: Option<PathBuf>,
    pub exec_resize_socket_path: Option<PathBuf>,
    pub websocket_enabled: bool,
}

#[derive(Debug, Clone)]
struct ExecServeContext {
    req: ExecRequest,
    runtime_path: PathBuf,
    runtime_config_path: PathBuf,
    exec_cpu_affinity: Option<usize>,
    exec_io_socket_path: Option<PathBuf>,
    exec_resize_socket_path: Option<PathBuf>,
    protocol: &'static str,
}

#[derive(Debug, Clone)]
pub struct StreamingServer {
    cache: Arc<Mutex<HashMap<String, CachedStreamingRequest>>>,
    base_url: String,
    config: StreamingConfig,
}

impl StreamingServer {
    #[cfg(test)]
    pub(crate) fn for_test(base_url: impl Into<String>) -> Self {
        Self::for_test_with_config(base_url, StreamingConfig::default())
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_config(
        base_url: impl Into<String>,
        config: StreamingConfig,
    ) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            base_url: base_url.into(),
            config,
        }
    }

    #[cfg(test)]
    async fn handle_request_for_test(
        &self,
        runtime_path: PathBuf,
        req: Request<Body>,
    ) -> Response<Body> {
        handle_request(self.cache.clone(), self.config.clone(), runtime_path, req).await
    }

    pub async fn start(bind_addr: &str, runtime_path: PathBuf) -> anyhow::Result<Self> {
        let mut config = StreamingConfig::default();
        let mut addrs = bind_addr.to_socket_addrs()?;
        let addr = addrs
            .next()
            .ok_or_else(|| anyhow::anyhow!("streaming bind address resolved to no addresses"))?;
        config.address = addr.ip().to_string();
        config.port = addr.port();
        Self::start_with_config(config, runtime_path).await
    }

    pub async fn start_with_config(
        config: StreamingConfig,
        runtime_path: PathBuf,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let listener = TcpListener::bind(config.bind_target())?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let cache = Arc::new(Mutex::new(HashMap::new()));
        if config.enable_tls {
            let tls_config = Arc::new(load_streaming_tls_config(&config)?);
            let tls_listener = TokioTcpListener::from_std(listener)?;
            let acceptor = TlsAcceptor::from(tls_config);
            let service_cache = cache.clone();
            let service_runtime_path = runtime_path.clone();
            let service_config = config.clone();
            tokio::spawn(async move {
                loop {
                    let (stream, _) = match tls_listener.accept().await {
                        Ok(accepted) => accepted,
                        Err(err) => {
                            log::error!("Streaming TLS listener accept failed: {}", err);
                            continue;
                        }
                    };
                    let acceptor = acceptor.clone();
                    let cache = service_cache.clone();
                    let runtime_path = service_runtime_path.clone();
                    let config = service_config.clone();
                    tokio::spawn(async move {
                        let tls_stream = match acceptor.accept(stream).await {
                            Ok(stream) => stream,
                            Err(err) => {
                                log::error!("Streaming TLS handshake failed: {}", err);
                                return;
                            }
                        };
                        let service = service_fn(move |req| {
                            let cache = cache.clone();
                            let runtime_path = runtime_path.clone();
                            let config = config.clone();
                            async move {
                                Ok::<_, Infallible>(
                                    handle_request(cache, config, runtime_path, req).await,
                                )
                            }
                        });
                        if let Err(err) = hyper::server::conn::Http::new()
                            .serve_connection(tls_stream, service)
                            .with_upgrades()
                            .await
                        {
                            log::error!("Streaming TLS connection exited: {}", err);
                        }
                    });
                }
            });
        } else {
            let service_cache = cache.clone();
            let service_runtime_path = runtime_path.clone();
            let service_config = config.clone();

            let make_service = make_service_fn(move |_| {
                let cache = service_cache.clone();
                let runtime_path = service_runtime_path.clone();
                let config = service_config.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |req| {
                        let cache = cache.clone();
                        let runtime_path = runtime_path.clone();
                        let config = config.clone();
                        async move {
                            Ok::<_, Infallible>(
                                handle_request(cache, config, runtime_path, req).await,
                            )
                        }
                    }))
                }
            });

            let server = Server::from_tcp(listener)?.serve(make_service);
            tokio::spawn(async move {
                if let Err(e) = server.await {
                    log::error!("Streaming server exited: {}", e);
                }
            });
        }

        Ok(Self {
            cache,
            base_url: config.base_url(local_addr),
            config,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn get_exec(
        &self,
        req: &ExecRequest,
        options: ExecStreamOptions,
    ) -> Result<ExecResponse, tonic::Status> {
        Self::validate_exec_request(req)?;
        let token = self
            .insert_request(StreamingRequest::Exec(ExecRequestContext {
                req: req.clone(),
                runtime_path: options.runtime_path,
                runtime_config_path: options.runtime_config_path,
                exec_cpu_affinity: options.exec_cpu_affinity,
                exec_io_socket_path: options.exec_io_socket_path,
                exec_resize_socket_path: options.exec_resize_socket_path,
                websocket_enabled: options.websocket_enabled,
            }))
            .await;
        Ok(ExecResponse {
            url: format!("{}/exec/{}", self.base_url, token),
        })
    }

    pub async fn get_attach(
        &self,
        req: &AttachRequest,
        websocket_enabled: bool,
    ) -> Result<AttachResponse, tonic::Status> {
        Self::validate_attach_request(req)?;
        let token = self
            .insert_request(StreamingRequest::Attach(AttachRequestContext {
                req: req.clone(),
                websocket_enabled,
            }))
            .await;
        Ok(AttachResponse {
            url: format!("{}/attach/{}", self.base_url, token),
        })
    }

    pub async fn get_attach_log(
        &self,
        req: &AttachRequest,
        log_path: PathBuf,
        websocket_enabled: bool,
    ) -> Result<AttachResponse, tonic::Status> {
        Self::validate_attach_request(req)?;
        let token = self
            .insert_request(StreamingRequest::AttachLog(AttachLogRequestContext {
                req: req.clone(),
                log_path,
                websocket_enabled,
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
        websocket_enabled: bool,
    ) -> Result<PortForwardResponse, tonic::Status> {
        Self::validate_port_forward_request(req)?;
        let token = self
            .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
                req: req.clone(),
                netns_path,
                websocket_enabled,
            }))
            .await;
        Ok(PortForwardResponse {
            url: format!("{}/portforward/{}", self.base_url, token),
        })
    }

    async fn insert_request(&self, request: StreamingRequest) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        let mut cache = self.cache.lock().await;
        Self::prune_expired_requests_locked(&mut cache, self.config.request_token_ttl);
        cache.insert(
            token.clone(),
            CachedStreamingRequest {
                created_at: Instant::now(),
                request,
            },
        );
        token
    }

    fn prune_expired_requests_locked(
        cache: &mut HashMap<String, CachedStreamingRequest>,
        request_token_ttl: Duration,
    ) {
        let now = Instant::now();
        cache.retain(|_, entry| now.duration_since(entry.created_at) <= request_token_ttl);
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
    config: StreamingConfig,
    _runtime_path: PathBuf,
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
        StreamingServer::prune_expired_requests_locked(&mut cache, config.request_token_ttl);
        cache.remove(token)
    };

    match (action, request.map(|entry| entry.request)) {
        ("exec", Some(StreamingRequest::Exec(exec_ctx))) => {
            let ExecRequestContext {
                req: exec_req,
                runtime_path,
                runtime_config_path,
                exec_cpu_affinity,
                exec_io_socket_path,
                exec_resize_socket_path,
                websocket_enabled,
            } = exec_ctx;
            if is_websocket_upgrade_request(&req) {
                if !websocket_enabled {
                    return response(
                        StatusCode::BAD_REQUEST,
                        "websocket streaming is disabled for this runtime handler",
                    );
                }
                let Some(protocol) = negotiate_remotecommand_websocket_protocol(&req) else {
                    return response(
                        StatusCode::FORBIDDEN,
                        "no supported Sec-WebSocket-Protocol was requested",
                    );
                };

                let response = websocket_switching_response(&req, protocol);
                let on_upgrade = hyper::upgrade::on(req);
                tokio::spawn(async move {
                    if let Err(e) = serve_exec_websocket(
                        on_upgrade,
                        ExecServeContext {
                            req: exec_req,
                            runtime_path,
                            runtime_config_path,
                            exec_cpu_affinity,
                            exec_io_socket_path,
                            exec_resize_socket_path,
                            protocol,
                        },
                    )
                    .await
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
                if let Err(e) = serve_exec_spdy(
                    on_upgrade,
                    ExecServeContext {
                        req: exec_req,
                        runtime_path,
                        runtime_config_path,
                        exec_cpu_affinity,
                        exec_io_socket_path,
                        exec_resize_socket_path,
                        protocol,
                    },
                )
                .await
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
        ("attach", Some(StreamingRequest::Attach(attach_ctx))) => {
            let AttachRequestContext {
                req: attach_req,
                websocket_enabled,
            } = attach_ctx;
            if is_websocket_upgrade_request(&req) {
                if !websocket_enabled {
                    return response(
                        StatusCode::BAD_REQUEST,
                        "websocket streaming is disabled for this runtime handler",
                    );
                }
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
            let websocket_enabled = attach_log_ctx.websocket_enabled;
            if is_websocket_upgrade_request(&req) {
                if !websocket_enabled {
                    return response(
                        StatusCode::BAD_REQUEST,
                        "websocket streaming is disabled for this runtime handler",
                    );
                }
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
            let websocket_enabled = port_forward_ctx.websocket_enabled;
            if is_websocket_upgrade_request(&req) {
                if !websocket_enabled {
                    return response(
                        StatusCode::BAD_REQUEST,
                        "websocket streaming is disabled for this runtime handler",
                    );
                }
                let Some(protocol) = negotiate_portforward_websocket_protocol(&req) else {
                    return response(
                        StatusCode::FORBIDDEN,
                        "no supported Sec-WebSocket-Protocol was requested",
                    );
                };

                let response = websocket_switching_response(&req, protocol);
                let on_upgrade = hyper::upgrade::on(req);
                let config = config.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        serve_portforward_websocket(on_upgrade, port_forward_ctx, protocol, config)
                            .await
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
            let config = config.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_portforward_spdy(on_upgrade, port_forward_ctx, config).await {
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

fn attach_socket_dir() -> PathBuf {
    std::env::var("CRIUS_ATTACH_SOCKET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| crate::runtime::default_shim_work_dir())
}

fn shim_socket_path(container_id: &str, socket_name: &str) -> PathBuf {
    attach_socket_dir().join(container_id).join(socket_name)
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

    [
        "v5.channel.k8s.io",
        "v4.channel.k8s.io",
        "v3.channel.k8s.io",
        "v2.channel.k8s.io",
        "channel.k8s.io",
    ]
    .into_iter()
    .find(|candidate| requested.iter().any(|value| value == candidate))
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

    [
        PORT_FORWARD_WS_PROTOCOL_V4_BINARY,
        PORT_FORWARD_WS_PROTOCOL_V4_BASE64,
        PORT_FORWARD_PROTOCOL_V1,
    ]
    .into_iter()
    .find(|candidate| requested.iter().any(|value| value == candidate))
}

fn portforward_websocket_protocol(protocol: &str) -> PortForwardWebsocketProtocol {
    match protocol {
        PORT_FORWARD_WS_PROTOCOL_V4_BINARY => PortForwardWebsocketProtocol::V4Binary,
        PORT_FORWARD_WS_PROTOCOL_V4_BASE64 => PortForwardWebsocketProtocol::V4Base64,
        _ => PortForwardWebsocketProtocol::Legacy,
    }
}

fn exec_exit_error_message(exit_code: i32) -> String {
    format!("command terminated with non-zero exit code: {}", exit_code)
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
    creation_timeout: Duration,
) -> Option<Duration> {
    pending_pairs
        .values()
        .map(|pair| (pair.created_at + creation_timeout).saturating_duration_since(now))
        .min()
}

fn take_expired_portforward_pairs(
    pending_pairs: &mut HashMap<String, PortForwardPair>,
    now: Instant,
    creation_timeout: Duration,
) -> Vec<PortForwardPair> {
    let expired_request_ids: Vec<String> = pending_pairs
        .iter()
        .filter(|(_, pair)| now.duration_since(pair.created_at) >= creation_timeout)
        .map(|(request_id, _)| request_id.clone())
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
    creation_timeout: Duration,
) -> Duration {
    let idle_timeout_remaining = next_stream_idle_timeout(activity, now, idle_timeout);
    match next_portforward_pair_timeout(pending_pairs, now, creation_timeout) {
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
    context: ExecServeContext,
) -> anyhow::Result<()> {
    let ExecServeContext {
        req,
        runtime_path,
        runtime_config_path,
        exec_cpu_affinity,
        exec_io_socket_path,
        exec_resize_socket_path,
        protocol,
    } = context;

    if let Some(exec_io_socket_path) = exec_io_socket_path {
        return serve_exec_spdy_via_shim(
            on_upgrade,
            req,
            exec_io_socket_path,
            exec_resize_socket_path,
            protocol,
        )
        .await;
    }

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
    if !runtime_config_path.as_os_str().is_empty() {
        command.arg("--config").arg(&runtime_config_path);
    }
    command.arg("exec");
    if req.tty {
        command.arg("-t");
    }
    command.arg(&req.container_id);
    for arg in &req.cmd {
        command.arg(arg);
    }
    crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_tokio_command(
        &mut command,
        exec_cpu_affinity,
    );

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
                    exec_exit_error_message(exit_code).as_bytes(),
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
    context: ExecServeContext,
) -> anyhow::Result<()> {
    let ExecServeContext {
        req,
        runtime_path,
        runtime_config_path,
        exec_cpu_affinity,
        exec_io_socket_path,
        exec_resize_socket_path,
        protocol,
    } = context;

    if let Some(exec_io_socket_path) = exec_io_socket_path {
        return serve_exec_websocket_via_shim(
            on_upgrade,
            req,
            exec_io_socket_path,
            exec_resize_socket_path,
            protocol,
        )
        .await;
    }

    let upgraded = on_upgrade.await?;
    let (mut reader, writer) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(writer));

    let mut command = TokioCommand::new(&runtime_path);
    if !runtime_config_path.as_os_str().is_empty() {
        command.arg("--config").arg(&runtime_config_path);
    }
    command.arg("exec");
    if req.tty {
        command.arg("-t");
    }
    command.arg(&req.container_id);
    for arg in &req.cmd {
        command.arg(arg);
    }
    crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_tokio_command(
        &mut command,
        exec_cpu_affinity,
    );

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
            exec_exit_error_message(exit_code).as_bytes(),
        )
        .await?;
    }
    write_websocket_frame(&mut *writer.lock().await, 0x8, &[]).await?;
    Ok(())
}

async fn serve_portforward_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    ctx: PortForwardRequestContext,
    config: StreamingConfig,
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
        if stream_is_idle(&activity, Instant::now(), config.port_forward_idle_timeout) {
            let _ = writer.lock().await.write_goaway(0).await;
            break;
        }

        for pair in take_expired_portforward_pairs(
            &mut pending_pairs,
            Instant::now(),
            config.port_forward_stream_creation_timeout,
        ) {
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
                config.port_forward_idle_timeout,
                config.port_forward_stream_creation_timeout,
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
    config: StreamingConfig,
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
        if stream_is_idle(&activity, Instant::now(), config.port_forward_idle_timeout) {
            write_websocket_frame(&mut *writer.lock().await, 0x8, &[]).await?;
            break;
        }

        let frame = match tokio::time::timeout(
            next_stream_idle_timeout(&activity, Instant::now(), config.port_forward_idle_timeout),
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

                if let std::collections::hash_map::Entry::Vacant(entry) =
                    data_channels.entry(channel)
                {
                    let (input_tx, mut input_rx) =
                        tokio::sync::mpsc::channel::<Option<Vec<u8>>>(32);
                    entry.insert(input_tx.clone());
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

async fn serve_exec_spdy_via_shim(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: ExecRequest,
    exec_io_socket_path: PathBuf,
    exec_resize_socket_path: Option<PathBuf>,
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

    let shim = match UnixStream::connect(&exec_io_socket_path).await {
        Ok(shim) => shim,
        Err(e) => {
            let message = format!(
                "failed to connect exec session socket {}: {}",
                exec_io_socket_path.display(),
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
        match exec_resize_socket_path.as_ref() {
            Some(path) => match UnixStream::connect(path).await {
                Ok(stream) => Some(stream),
                Err(e) => {
                    let message = format!(
                        "failed to connect exec resize socket {}: {}",
                        path.display(),
                        e
                    );
                    write_error_stream(&writer, error_stream, &message).await?;
                    None
                }
            },
            None => None,
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
                        log::debug!("Exec output decoder finished with trailing data: {}", e);
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
                        log::debug!("Exec output decoder stopped: {}", e);
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
                    log::debug!("Exec output pump stopped: {}", e);
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
                        if let Some(path) = exec_resize_socket_path.as_ref() {
                            match UnixStream::connect(path).await {
                                Ok(stream) => resize_socket = Some(stream),
                                Err(e) => {
                                    write_error_stream(
                                        &writer,
                                        error_stream,
                                        &format!(
                                            "failed to connect exec resize socket {}: {}",
                                            path.display(),
                                            e
                                        ),
                                    )
                                    .await?;
                                    continue;
                                }
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
                    .ok_or_else(|| anyhow::anyhow!("exec stream is missing streamtype header"))?;
                if stream_type != "resize" || !req.tty {
                    return Err(anyhow::anyhow!(
                        "unexpected exec streamtype {} after session start",
                        stream_type
                    ));
                }
                if resize_stream.replace(frame.stream_id).is_some() {
                    return Err(anyhow::anyhow!("duplicate exec resize stream received"));
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
                log::debug!("Exec input loop stopped: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn serve_exec_websocket_via_shim(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: ExecRequest,
    exec_io_socket_path: PathBuf,
    exec_resize_socket_path: Option<PathBuf>,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (mut reader, writer) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(writer));

    let shim = UnixStream::connect(&exec_io_socket_path)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to connect exec session socket {}: {}",
                exec_io_socket_path.display(),
                e
            )
        })?;
    let (mut shim_read, shim_write) = shim.into_split();
    let shim_write = Arc::new(Mutex::new(shim_write));

    let mut resize_socket = if req.tty {
        match exec_resize_socket_path.as_ref() {
            Some(path) => UnixStream::connect(path).await.ok(),
            None => None,
        }
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
                        log::debug!("Exec websocket decoder finished with trailing data: {}", e);
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
                    Err(e) => {
                        let _ = write_websocket_channel_frame(
                            &writer_for_output,
                            WS_CHANNEL_ERROR,
                            e.to_string().as_bytes(),
                        )
                        .await;
                        break;
                    }
                },
                Err(e) => {
                    log::debug!("Exec websocket output pump stopped: {}", e);
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
            0x1 | 0x2 => {
                if frame.payload.is_empty() {
                    continue;
                }
                let channel = frame.payload[0];
                let payload = &frame.payload[1..];
                match channel {
                    WS_CHANNEL_STDIN => {
                        if !payload.is_empty() {
                            shim_write.lock().await.write_all(payload).await?;
                        }
                    }
                    WS_CHANNEL_RESIZE if req.tty => {
                        if resize_socket.is_none() {
                            if let Some(path) = exec_resize_socket_path.as_ref() {
                                resize_socket = UnixStream::connect(path).await.ok();
                            }
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
    write_websocket_frame(&mut *writer.lock().await, 0x8, &[]).await?;
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
mod tests;
