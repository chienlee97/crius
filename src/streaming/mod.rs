pub mod spdy;

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Body;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Method, Request, Response, Server, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use crate::attach::{AttachOutputDecoder, ATTACH_PIPE_STDERR, ATTACH_PIPE_STDOUT};
use crate::proto::runtime::v1::{AttachRequest, AttachResponse, ExecRequest, ExecResponse};

#[derive(Debug, Clone)]
pub enum StreamingRequest {
    Exec(ExecRequest),
    Attach(AttachRequest),
}

#[derive(Debug, Clone)]
pub struct StreamingServer {
    cache: Arc<Mutex<HashMap<String, StreamingRequest>>>,
    base_url: String,
}

impl StreamingServer {
    #[cfg(test)]
    fn for_test(base_url: impl Into<String>) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            base_url: base_url.into(),
        }
    }

    pub async fn start(bind_addr: &str) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let service_cache = cache.clone();

        let make_service = make_service_fn(move |_| {
            let cache = service_cache.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let cache = cache.clone();
                    async move { Ok::<_, Infallible>(handle_request(cache, req).await) }
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
}

async fn handle_request(
    cache: Arc<Mutex<HashMap<String, StreamingRequest>>>,
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
        ("exec", Some(StreamingRequest::Exec(_))) => response(
            StatusCode::NOT_IMPLEMENTED,
            "crius streaming exec endpoint exists, but SPDY data-plane handling is not implemented yet",
        ),
        ("attach", Some(StreamingRequest::Attach(attach_req))) => {
            if !is_spdy_upgrade_request(&req) {
                return response(
                    StatusCode::BAD_REQUEST,
                    "attach requires SPDY upgrade headers",
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
        ("exec", Some(_)) | ("attach", Some(_)) => {
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

async fn serve_attach_spdy(
    on_upgrade: hyper::upgrade::OnUpgrade,
    req: AttachRequest,
    _protocol: &'static str,
) -> anyhow::Result<()> {
    let upgraded = on_upgrade.await?;
    let (read_half, write_half) = tokio::io::split(upgraded);
    let writer = Arc::new(Mutex::new(spdy::AsyncSpdyWriter::new(write_half)));
    let mut reader = read_half;

    let attach_socket_path = format!("/var/run/crius/shims/{}/attach.sock", req.container_id);
    let shim = UnixStream::connect(&attach_socket_path).await?;
    let (mut shim_read, shim_write) = shim.into_split();
    let shim_write = Arc::new(Mutex::new(shim_write));

    let expected_roles = expected_attach_roles(&req);
    let mut header_decompressor = spdy::HeaderDecompressor::new();
    let mut stdin_stream = None;
    let mut stdout_stream = None;
    let mut stderr_stream = None;
    let mut error_stream = None;

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
                    other => {
                        return Err(anyhow::anyhow!("unsupported attach streamtype {}", other))
                    }
                };

                if !expected_roles.contains(&role) {
                    return Err(anyhow::anyhow!(
                        "unexpected attach stream {:?} for request",
                        role
                    ));
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

    loop {
        match spdy::read_frame_async(&mut reader).await {
            Ok(spdy::Frame::Data(frame)) => {
                if Some(frame.stream_id) == stdin_stream {
                    let mut shim_write = shim_write.lock().await;
                    if !frame.data.is_empty() {
                        shim_write.write_all(&frame.data).await?;
                    }
                    if frame.flags & 0x01 != 0 {
                        let _ = shim_write.shutdown().await;
                    }
                }
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
}
