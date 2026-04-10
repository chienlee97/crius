pub mod spdy;

use std::collections::HashMap;
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
use std::time::Duration;

use hyper::body::Body;
use hyper::header::{CONNECTION, UPGRADE};
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

#[derive(Debug, Clone)]
enum StreamingRequest {
    Exec(ExecRequest),
    Attach(AttachRequest),
    PortForward(PortForwardRequestContext),
}

#[derive(Debug, Clone)]
struct PortForwardRequestContext {
    req: PortForwardRequest,
    netns_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct StreamingServer {
    cache: Arc<Mutex<HashMap<String, StreamingRequest>>>,
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
        cache.insert(token.clone(), request);
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
        if req.port.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "port-forward requires at least one forwarded port",
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
    cache: Arc<Mutex<HashMap<String, StreamingRequest>>>,
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
        cache.remove(token)
    };

    match (action, request) {
        ("exec", Some(StreamingRequest::Exec(exec_req))) => {
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "exec requires SPDY upgrade headers; websocket transport is not supported",
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
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "attach requires SPDY upgrade headers; websocket transport is not supported",
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
        ("portforward", Some(StreamingRequest::PortForward(port_forward_ctx))) => {
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "portforward requires SPDY upgrade headers; websocket transport is not supported",
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

#[derive(Debug, Default)]
struct PortForwardPair {
    request_id: String,
    port: u16,
    data_stream: Option<spdy::StreamId>,
    error_stream: Option<spdy::StreamId>,
}

#[derive(Debug, Deserialize)]
struct TerminalSizePayload {
    #[serde(alias = "Width")]
    width: u16,
    #[serde(alias = "Height")]
    height: u16,
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

fn parse_portforward_stream(
    frame: &spdy::SynStreamFrame,
    decompressor: &mut spdy::HeaderDecompressor,
) -> anyhow::Result<(PortForwardStreamRole, String, u16)> {
    let headers = spdy::decode_header_block(&frame.header_block, decompressor)?;
    let stream_type = spdy::header_value(&headers, "streamtype")
        .ok_or_else(|| anyhow::anyhow!("port-forward stream is missing streamtype header"))?;
    let role = match stream_type {
        "data" => PortForwardStreamRole::Data,
        "error" => PortForwardStreamRole::Error,
        other => {
            return Err(anyhow::anyhow!(
                "unsupported port-forward streamtype {}",
                other
            ))
        }
    };
    let port_header = spdy::header_value(&headers, "port")
        .ok_or_else(|| anyhow::anyhow!("port-forward stream is missing port header"))?;
    let port = port_header
        .parse::<u16>()
        .map_err(|e| anyhow::anyhow!("invalid port-forward port {}: {}", port_header, e))?;
    if port == 0 {
        return Err(anyhow::anyhow!("port-forward port must be > 0"));
    }
    let request_id = portforward_request_id(
        frame.stream_id,
        stream_type,
        spdy::header_value(&headers, "requestid"),
    )?;
    Ok((role, request_id, port))
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
    Ok(())
}

async fn serve_portforward_pair(
    writer: Arc<Mutex<spdy::AsyncSpdyWriter<tokio::io::WriteHalf<hyper::upgrade::Upgraded>>>>,
    netns_path: PathBuf,
    port: u16,
    data_stream: spdy::StreamId,
    error_stream: spdy::StreamId,
    mut input_rx: tokio::sync::mpsc::Receiver<Option<Vec<u8>>>,
) -> anyhow::Result<()> {
    let tcp_stream = match connect_to_port_in_netns(netns_path, port).await {
        Ok(stream) => stream,
        Err(err) => {
            write_portforward_error(&writer, data_stream, error_stream, &err.to_string()).await?;
            return Err(err);
        }
    };

    let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();
    let writer_for_output = writer.clone();
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
                    break;
                }
                Ok(n) => {
                    writer_for_output
                        .lock()
                        .await
                        .write_data(data_stream, &buffer[..n], false)
                        .await?;
                }
                Err(err) => {
                    write_portforward_error(
                        &writer_for_output,
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

async fn serve_portforward_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    ctx: PortForwardRequestContext,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (read_half, write_half) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(spdy::AsyncSpdyWriter::new(write_half)));
    let mut reader = read_half;
    let mut header_decompressor = spdy::HeaderDecompressor::new();

    let mut pending_pairs: HashMap<String, PortForwardPair> = HashMap::new();
    let mut data_stream_inputs: HashMap<
        spdy::StreamId,
        tokio::sync::mpsc::Sender<Option<Vec<u8>>>,
    > = HashMap::new();
    let mut active_streams = 0usize;

    loop {
        match spdy::read_frame_async(&mut reader).await {
            Ok(spdy::Frame::SynStream(frame)) => {
                let (role, request_id, port) =
                    parse_portforward_stream(&frame, &mut header_decompressor)?;
                if !ctx.req.port.is_empty()
                    && !ctx.req.port.iter().any(|allowed| *allowed == port as i32)
                {
                    write_portforward_error(
                        &writer,
                        frame.stream_id,
                        frame.stream_id,
                        &format!("port {} was not requested by the client", port),
                    )
                    .await?;
                    continue;
                }

                writer
                    .lock()
                    .await
                    .write_syn_reply(frame.stream_id, &[], false)
                    .await?;

                let pair =
                    pending_pairs
                        .entry(request_id.clone())
                        .or_insert_with(|| PortForwardPair {
                            request_id: request_id.clone(),
                            port,
                            ..Default::default()
                        });
                pair.port = port;
                match role {
                    PortForwardStreamRole::Data if pair.data_stream.is_none() => {
                        pair.data_stream = Some(frame.stream_id);
                    }
                    PortForwardStreamRole::Error if pair.error_stream.is_none() => {
                        pair.error_stream = Some(frame.stream_id);
                    }
                    PortForwardStreamRole::Data => {
                        return Err(anyhow::anyhow!(
                            "duplicate port-forward data stream for request {}",
                            pair.request_id
                        ));
                    }
                    PortForwardStreamRole::Error => {
                        return Err(anyhow::anyhow!(
                            "duplicate port-forward error stream for request {}",
                            pair.request_id
                        ));
                    }
                }

                if let (Some(data_stream), Some(error_stream)) =
                    (pair.data_stream, pair.error_stream)
                {
                    let pair = pending_pairs.remove(&request_id).expect("pair must exist");
                    let (input_tx, input_rx) = tokio::sync::mpsc::channel(32);
                    data_stream_inputs.insert(data_stream, input_tx);
                    active_streams += 1;
                    let writer = writer.clone();
                    let netns_path = ctx.netns_path.clone();
                    tokio::spawn(async move {
                        if let Err(err) = serve_portforward_pair(
                            writer,
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
            }
            Ok(spdy::Frame::Data(frame)) => {
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
                    }
                }
            }
            Ok(spdy::Frame::Ping(frame)) => {
                writer.lock().await.write_ping(frame.id).await?;
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
    use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
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

        let response = handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("websocket transport is not supported"));
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

        let response = handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("websocket transport is not supported"));
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

        let response = handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("websocket transport is not supported"));
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

        let response = handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
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

        let response = handle_request(server.cache.clone(), PathBuf::from("/bin/false"), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response_body_string(response)
            .await
            .contains("streaming token kind mismatch"));
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
        let err = StreamingServer::validate_port_forward_request(&missing_ports).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

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
        writer.write_syn_stream(1, 0, &data_headers_a, false).unwrap();
        writer.write_syn_stream(3, 0, &error_headers_a, false).unwrap();
        writer.write_syn_stream(5, 0, &data_headers_b, false).unwrap();
        writer.write_syn_stream(7, 0, &error_headers_b, false).unwrap();
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
            .write_syn_stream(1, 0, &[("streamtype".to_string(), "error".to_string())], false)
            .unwrap();
        writer
            .write_syn_stream(3, 0, &[("streamtype".to_string(), "stdin".to_string())], false)
            .unwrap();
        writer
            .write_syn_stream(5, 0, &[("streamtype".to_string(), "stdout".to_string())], false)
            .unwrap();
        writer
            .write_syn_stream(7, 0, &[("streamtype".to_string(), "resize".to_string())], false)
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
        assert!(stdout_text.contains("37 101"), "stdout was: {}", stdout_text);
        assert!(stdout_text.contains("hello-exec"), "stdout was: {}", stdout_text);
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
            let mut output = crate::attach::encode_attach_output_frame(
                ATTACH_PIPE_STDOUT,
                b"attach-ready\n",
            );
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
            .write_syn_stream(1, 0, &[("streamtype".to_string(), "error".to_string())], false)
            .unwrap();
        writer
            .write_syn_stream(3, 0, &[("streamtype".to_string(), "stdin".to_string())], false)
            .unwrap();
        writer
            .write_syn_stream(5, 0, &[("streamtype".to_string(), "stdout".to_string())], false)
            .unwrap();
        writer
            .write_syn_stream(7, 0, &[("streamtype".to_string(), "resize".to_string())], false)
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
        assert!(stdout_text.contains("attach-ready"), "stdout was: {}", stdout_text);
        assert!(stdout_text.contains("hello-attach"), "stdout was: {}", stdout_text);
        assert!(
            error_payload.is_empty(),
            "unexpected attach error payload: {}",
            String::from_utf8_lossy(&error_payload)
        );

        let resize_payload = resize_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(resize_payload.contains("\"Width\":101") || resize_payload.contains("\"width\":101"));
        assert!(resize_payload.contains("\"Height\":37") || resize_payload.contains("\"height\":37"));

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
