use std::collections::HashMap;

use base64::Engine;
use nix::sys::termios::{self, SetArg, Termios};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::crs::{
    args::{StreamOptions, StreamProtocolArg},
    error::{CliError, CommandResult},
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum StreamProtocol {
    Websocket,
    Spdy,
}

impl From<StreamProtocolArg> for StreamProtocol {
    fn from(value: StreamProtocolArg) -> Self {
        match value {
            StreamProtocolArg::Websocket => Self::Websocket,
            StreamProtocolArg::Spdy => Self::Spdy,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecStreamOptions {
    pub stream_url: Option<String>,
    pub container_id: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub resize: Option<String>,
    pub protocol: StreamProtocol,
}

impl ExecStreamOptions {
    pub(crate) fn from_args(
        container_id: String,
        command: Vec<String>,
        stream: StreamOptions,
    ) -> Result<Self, CliError> {
        if container_id.trim().is_empty() {
            return Err(CliError::invalid_input("container id must not be empty")
                .with_command("crs container exec"));
        }
        if command.is_empty() {
            return Err(CliError::invalid_input("exec command must not be empty")
                .with_command("crs container exec"));
        }

        Ok(Self {
            stream_url: None,
            container_id,
            command,
            tty: stream.tty,
            stdin: stream.stdin,
            stdout: stream.stdout,
            stderr: stream.stderr && !stream.tty,
            resize: stream.resize,
            protocol: stream.protocol.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttachStreamOptions {
    pub stream_url: Option<String>,
    pub container_id: String,
    pub tty: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub resize: Option<String>,
    pub protocol: StreamProtocol,
}

impl AttachStreamOptions {
    pub(crate) fn from_args(container_id: String, stream: StreamOptions) -> Result<Self, CliError> {
        if container_id.trim().is_empty() {
            return Err(CliError::invalid_input("container id must not be empty")
                .with_command("crs container attach"));
        }
        if !stream.stdin && !stream.stdout && !stream.stderr {
            return Err(CliError::invalid_input(
                "attach requires at least one of stdin, stdout, or stderr",
            )
            .with_command("crs container attach"));
        }

        Ok(Self {
            stream_url: None,
            container_id,
            tty: stream.tty,
            stdin: stream.stdin,
            stdout: stream.stdout,
            stderr: stream.stderr && !stream.tty,
            resize: stream.resize,
            protocol: stream.protocol.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortForwardOptions {
    pub pod_id: String,
    pub forwards: Vec<PortForwardSpec>,
    pub protocol: StreamProtocol,
}

impl PortForwardOptions {
    pub(crate) fn from_args(pod_id: String, forward: Vec<String>) -> Result<Self, CliError> {
        if pod_id.trim().is_empty() {
            return Err(CliError::invalid_input("pod id must not be empty")
                .with_command("crs pod port-forward"));
        }

        let forwards = forward
            .iter()
            .map(|value| parse_port_forward(value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|message| {
                CliError::invalid_input(message).with_command("crs pod port-forward")
            })?;

        if forwards.is_empty() {
            return Err(CliError::invalid_input(
                "pod port-forward requires at least one --forward LOCAL:REMOTE",
            )
            .with_command("crs pod port-forward"));
        }

        Ok(Self {
            pod_id,
            forwards,
            protocol: StreamProtocol::Websocket,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortForwardSpec {
    pub local: u16,
    pub remote: u16,
}

fn parse_port_forward(value: &str) -> Result<PortForwardSpec, String> {
    let (local, remote) = value
        .split_once(':')
        .ok_or_else(|| format!("invalid port forward \"{value}\": expected LOCAL:REMOTE"))?;

    Ok(PortForwardSpec {
        local: parse_forward_port(value, local)?,
        remote: parse_forward_port(value, remote)?,
    })
}

fn parse_forward_port(source: &str, value: &str) -> Result<u16, String> {
    let port = value
        .parse::<u16>()
        .map_err(|_| format!("invalid port forward \"{source}\": port must be 1-65535"))?;
    if port == 0 {
        return Err(format!(
            "invalid port forward \"{source}\": port must be 1-65535"
        ));
    }
    Ok(port)
}

pub(crate) async fn exec(options: ExecStreamOptions) -> Result<CommandResult, CliError> {
    let _raw_mode = SystemRawModeBackend.enter_raw_mode(options.tty)?;
    let resize = ResizeEvents::start(options.tty);
    let _initial_size = resize.initial();
    let _resize_events = resize.into_receiver();
    let url = select_stream_url("exec", options.protocol, options.stream_url.as_deref())?;
    let _output = websocket_stream(url, WebsocketIo::default()).await?;
    Ok(CommandResult::success())
}

pub(crate) async fn attach(options: AttachStreamOptions) -> Result<CommandResult, CliError> {
    let _raw_mode = SystemRawModeBackend.enter_raw_mode(options.tty)?;
    let resize = ResizeEvents::start(options.tty);
    let _initial_size = resize.initial();
    let _resize_events = resize.into_receiver();
    let url = select_stream_url("attach", options.protocol, options.stream_url.as_deref())?;
    let _output = websocket_stream(url, WebsocketIo::default()).await?;
    Ok(CommandResult::success())
}

fn select_stream_url<'a>(
    operation: &str,
    protocol: StreamProtocol,
    stream_url: Option<&'a str>,
) -> Result<&'a str, CliError> {
    match protocol {
        StreamProtocol::Websocket => stream_url.ok_or_else(|| {
            CliError::not_implemented(format!("crs streaming {operation} URL resolution"))
        }),
        StreamProtocol::Spdy => Err(CliError::internal(
            "SPDY streaming is not supported by crs yet; use --protocol websocket",
        )),
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct TerminalSize {
    pub width: u16,
    pub height: u16,
}

struct ResizeEvents {
    initial: Option<TerminalSize>,
    receiver: mpsc::Receiver<TerminalSize>,
}

impl ResizeEvents {
    fn start(enabled: bool) -> Self {
        if !enabled {
            return Self::empty();
        }

        let fd = nix::libc::STDIN_FILENO;
        if !nix::unistd::isatty(fd).unwrap_or(false) {
            return Self::empty();
        }

        let initial = terminal_size(fd);
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let Ok(mut signal) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
            else {
                return;
            };

            while signal.recv().await.is_some() {
                let Some(size) = terminal_size(fd) else {
                    continue;
                };
                if sender.send(size).await.is_err() {
                    break;
                }
            }
        });

        Self { initial, receiver }
    }

    fn empty() -> Self {
        let (_sender, receiver) = mpsc::channel(1);
        Self {
            initial: None,
            receiver,
        }
    }

    fn initial(&self) -> Option<TerminalSize> {
        self.initial
    }

    fn into_receiver(self) -> mpsc::Receiver<TerminalSize> {
        self.receiver
    }
}

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const WS_BINARY_OPCODE: u8 = 0x2;
const WS_CLOSE_OPCODE: u8 = 0x8;
const WS_PING_OPCODE: u8 = 0x9;
const WS_PONG_OPCODE: u8 = 0xa;
const WS_CHANNEL_STDIN: u8 = 0;
const WS_CHANNEL_STDOUT: u8 = 1;
const WS_CHANNEL_STDERR: u8 = 2;
const WS_CHANNEL_ERROR: u8 = 3;
const REMOTE_COMMAND_PROTOCOLS: &[&str] = &[
    "v5.channel.k8s.io",
    "v4.channel.k8s.io",
    "v3.channel.k8s.io",
    "v2.channel.k8s.io",
    "channel.k8s.io",
];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WebsocketIo {
    pub stdin: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WebsocketStreamOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub(crate) async fn websocket_stream(
    url: &str,
    io: WebsocketIo,
) -> Result<WebsocketStreamOutput, CliError> {
    let target = WebsocketTarget::parse(url)?;
    let mut stream = TcpStream::connect((target.host.as_str(), target.port))
        .await
        .map_err(|source| CliError::internal(format!("failed to connect websocket: {source}")))?;
    let key = websocket_client_key();
    let request = websocket_handshake_request(&target, &key);
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|source| {
            CliError::internal(format!("failed to send websocket handshake: {source}"))
        })?;
    stream.flush().await.map_err(|source| {
        CliError::internal(format!("failed to flush websocket handshake: {source}"))
    })?;

    let mut reader = BufReader::new(stream);
    let response = read_http_response(&mut reader).await?;
    validate_websocket_handshake(&response, &key)?;
    let mut stream = reader.into_inner();

    if !io.stdin.is_empty() {
        let mut payload = Vec::with_capacity(1 + io.stdin.len());
        payload.push(WS_CHANNEL_STDIN);
        payload.extend_from_slice(&io.stdin);
        write_websocket_frame(&mut stream, WS_BINARY_OPCODE, &payload, true).await?;
    }

    let mut output = WebsocketStreamOutput::default();
    loop {
        let Some(frame) = read_websocket_frame(&mut stream).await? else {
            break;
        };
        match frame.opcode {
            WS_BINARY_OPCODE => apply_remotecommand_frame(&mut output, &frame.payload)?,
            WS_CLOSE_OPCODE => break,
            WS_PING_OPCODE => {
                write_websocket_frame(&mut stream, WS_PONG_OPCODE, &frame.payload, true).await?;
            }
            _ => {}
        }
    }

    Ok(output)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WebsocketTarget {
    host: String,
    port: u16,
    path_and_query: String,
}

impl WebsocketTarget {
    fn parse(url: &str) -> Result<Self, CliError> {
        let without_scheme = url
            .strip_prefix("ws://")
            .or_else(|| url.strip_prefix("http://"))
            .ok_or_else(|| {
                CliError::invalid_input("only ws:// and http:// streaming URLs are supported")
            })?;
        let (authority, path) = without_scheme
            .split_once('/')
            .map(|(authority, path)| (authority, format!("/{path}")))
            .unwrap_or((without_scheme, "/".to_string()));
        let (host, port) = authority
            .rsplit_once(':')
            .map(|(host, port)| {
                let port = port.parse::<u16>().map_err(|_| {
                    CliError::invalid_input(format!("invalid websocket URL port: {port}"))
                })?;
                Ok((host.to_string(), port))
            })
            .transpose()?
            .unwrap_or_else(|| (authority.to_string(), 80));
        if host.is_empty() {
            return Err(CliError::invalid_input(
                "websocket URL host must not be empty",
            ));
        }

        Ok(Self {
            host,
            port,
            path_and_query: path,
        })
    }

    fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HttpResponseHead {
    status: u16,
    headers: HashMap<String, String>,
}

async fn read_http_response<R>(reader: &mut R) -> Result<HttpResponseHead, CliError>
where
    R: AsyncBufRead + Unpin,
{
    let mut status_line = String::new();
    reader.read_line(&mut status_line).await.map_err(|source| {
        CliError::internal(format!("failed to read websocket response: {source}"))
    })?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| CliError::internal("invalid websocket response status line"))?;

    let mut headers = HashMap::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await.map_err(|source| {
            CliError::internal(format!(
                "failed to read websocket response headers: {source}"
            ))
        })?;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    Ok(HttpResponseHead { status, headers })
}

fn validate_websocket_handshake(response: &HttpResponseHead, key: &str) -> Result<(), CliError> {
    if response.status != 101 {
        return Err(CliError::internal(format!(
            "websocket handshake failed with HTTP {}",
            response.status
        )));
    }
    let expected_accept = websocket_accept_value(key);
    let accept = response
        .headers
        .get("sec-websocket-accept")
        .map(String::as_str)
        .unwrap_or_default();
    if accept != expected_accept {
        return Err(CliError::internal(
            "websocket handshake returned invalid accept value",
        ));
    }
    let protocol = response
        .headers
        .get("sec-websocket-protocol")
        .map(String::as_str)
        .unwrap_or_default();
    if !REMOTE_COMMAND_PROTOCOLS.contains(&protocol) {
        return Err(CliError::internal(
            "websocket handshake did not negotiate a remotecommand protocol",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WebsocketFrame {
    opcode: u8,
    payload: Vec<u8>,
}

async fn read_websocket_frame<R>(reader: &mut R) -> Result<Option<WebsocketFrame>, CliError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 2];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(source) => {
            return Err(CliError::internal(format!(
                "failed to read websocket frame: {source}"
            )));
        }
    }

    let opcode = header[0] & 0x0f;
    let masked = (header[1] & 0x80) != 0;
    let mut payload_len = (header[1] & 0x7f) as u64;
    if payload_len == 126 {
        let mut extended = [0u8; 2];
        reader.read_exact(&mut extended).await.map_err(|source| {
            CliError::internal(format!("failed to read websocket frame length: {source}"))
        })?;
        payload_len = u16::from_be_bytes(extended) as u64;
    } else if payload_len == 127 {
        let mut extended = [0u8; 8];
        reader.read_exact(&mut extended).await.map_err(|source| {
            CliError::internal(format!("failed to read websocket frame length: {source}"))
        })?;
        payload_len = u64::from_be_bytes(extended);
    }

    let mut mask = [0u8; 4];
    if masked {
        reader.read_exact(&mut mask).await.map_err(|source| {
            CliError::internal(format!("failed to read websocket frame mask: {source}"))
        })?;
    }

    let mut payload = vec![0u8; payload_len as usize];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await.map_err(|source| {
            CliError::internal(format!("failed to read websocket frame payload: {source}"))
        })?;
    }
    if masked {
        apply_websocket_mask(&mut payload, mask);
    }

    Ok(Some(WebsocketFrame { opcode, payload }))
}

async fn write_websocket_frame<W>(
    writer: &mut W,
    opcode: u8,
    payload: &[u8],
    masked: bool,
) -> Result<(), CliError>
where
    W: AsyncWrite + Unpin,
{
    let mut header = vec![0x80 | (opcode & 0x0f)];
    let mask_bit = if masked { 0x80 } else { 0 };
    if payload.len() < 126 {
        header.push(mask_bit | payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        header.push(mask_bit | 126);
        header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        header.push(mask_bit | 127);
        header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }

    let mut payload = payload.to_vec();
    if masked {
        let mask = [0x12, 0x34, 0x56, 0x78];
        header.extend_from_slice(&mask);
        apply_websocket_mask(&mut payload, mask);
    }

    writer.write_all(&header).await.map_err(|source| {
        CliError::internal(format!("failed to write websocket frame header: {source}"))
    })?;
    if !payload.is_empty() {
        writer.write_all(&payload).await.map_err(|source| {
            CliError::internal(format!("failed to write websocket frame payload: {source}"))
        })?;
    }
    writer.flush().await.map_err(|source| {
        CliError::internal(format!("failed to flush websocket frame: {source}"))
    })?;
    Ok(())
}

fn apply_remotecommand_frame(
    output: &mut WebsocketStreamOutput,
    payload: &[u8],
) -> Result<(), CliError> {
    let Some((&channel, payload)) = payload.split_first() else {
        return Ok(());
    };
    match channel {
        WS_CHANNEL_STDOUT => output.stdout.extend_from_slice(payload),
        WS_CHANNEL_STDERR => output.stderr.extend_from_slice(payload),
        WS_CHANNEL_ERROR => {
            return Err(CliError::internal(format!(
                "remote stream error: {}",
                String::from_utf8_lossy(payload)
            )));
        }
        _ => {}
    }
    Ok(())
}

fn websocket_handshake_request(target: &WebsocketTarget, key: &str) -> String {
    format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Protocol: {}\r\n\r\n",
        target.path_and_query,
        target.authority(),
        key,
        REMOTE_COMMAND_PROTOCOLS.join(", ")
    )
}

fn websocket_client_key() -> String {
    base64::engine::general_purpose::STANDARD.encode(b"crius-crs-stream")
}

fn websocket_accept_value(key: &str) -> String {
    let mut payload = key.as_bytes().to_vec();
    payload.extend_from_slice(WS_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(sha1_digest(&payload))
}

fn apply_websocket_mask(payload: &mut [u8], mask: [u8; 4]) {
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[index % 4];
    }
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

fn terminal_size(fd: i32) -> Option<TerminalSize> {
    let mut winsize = nix::libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let rc = unsafe { nix::libc::ioctl(fd, nix::libc::TIOCGWINSZ, &mut winsize) };
    if rc != 0 || winsize.ws_col == 0 || winsize.ws_row == 0 {
        return None;
    }

    Some(TerminalSize {
        width: winsize.ws_col,
        height: winsize.ws_row,
    })
}

trait RawModeBackend {
    type Guard;

    fn enter_raw_mode(&self, enabled: bool) -> Result<Option<Self::Guard>, CliError>;
}

struct SystemRawModeBackend;

impl RawModeBackend for SystemRawModeBackend {
    type Guard = TtyRawModeGuard;

    fn enter_raw_mode(&self, enabled: bool) -> Result<Option<Self::Guard>, CliError> {
        TtyRawModeGuard::enter(enabled)
    }
}

struct TtyRawModeGuard {
    fd: i32,
    original: Termios,
}

impl TtyRawModeGuard {
    fn enter(enabled: bool) -> Result<Option<Self>, CliError> {
        if !enabled {
            return Ok(None);
        }

        let fd = nix::libc::STDIN_FILENO;
        if !nix::unistd::isatty(fd).unwrap_or(false) {
            return Ok(None);
        }

        let original = termios::tcgetattr(fd).map_err(|source| {
            CliError::internal(format!("failed to read terminal mode: {source}"))
        })?;
        let mut raw = original.clone();
        termios::cfmakeraw(&mut raw);
        termios::tcsetattr(fd, SetArg::TCSANOW, &raw).map_err(|source| {
            CliError::internal(format!("failed to enter terminal raw mode: {source}"))
        })?;

        Ok(Some(Self { fd, original }))
    }
}

impl Drop for TtyRawModeGuard {
    fn drop(&mut self) {
        let _ = termios::tcsetattr(self.fd, SetArg::TCSANOW, &self.original);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::net::TcpListener;

    fn stream_options() -> StreamOptions {
        StreamOptions {
            stdin: true,
            tty: false,
            stdout: true,
            stderr: true,
            resize: Some("80x24".to_string()),
            protocol: StreamProtocolArg::Websocket,
        }
    }

    #[test]
    fn exec_options_disable_stderr_for_tty() {
        let options = ExecStreamOptions::from_args(
            "ctr".to_string(),
            vec!["sh".to_string()],
            StreamOptions {
                tty: true,
                ..stream_options()
            },
        )
        .expect("exec stream options should build");

        assert!(options.tty);
        assert!(options.stdin);
        assert!(options.stdout);
        assert!(!options.stderr);
        assert_eq!(options.protocol, StreamProtocol::Websocket);
    }

    #[test]
    fn exec_options_reject_empty_command() {
        let error = ExecStreamOptions::from_args("ctr".to_string(), Vec::new(), stream_options())
            .expect_err("empty command should fail");

        assert_eq!(error.exit_status().code(), 2);
        assert!(error.to_string().contains("exec command must not be empty"));
    }

    #[test]
    fn attach_options_require_at_least_one_stream() {
        let error = AttachStreamOptions::from_args(
            "ctr".to_string(),
            StreamOptions {
                stdin: false,
                stdout: false,
                stderr: false,
                ..stream_options()
            },
        )
        .expect_err("disabled attach streams should fail");

        assert_eq!(error.exit_status().code(), 2);
    }

    #[test]
    fn port_forward_options_parse_local_and_remote_ports() {
        let options = PortForwardOptions::from_args(
            "pod".to_string(),
            vec!["8080:80".to_string(), "8443:443".to_string()],
        )
        .expect("port forward options should build");

        assert_eq!(
            options.forwards,
            vec![
                PortForwardSpec {
                    local: 8080,
                    remote: 80
                },
                PortForwardSpec {
                    local: 8443,
                    remote: 443
                }
            ]
        );
        assert_eq!(options.protocol, StreamProtocol::Websocket);
    }

    #[test]
    fn port_forward_options_reject_invalid_ports() {
        let error = PortForwardOptions::from_args("pod".to_string(), vec!["8080:0".to_string()])
            .expect_err("zero port should fail");

        assert_eq!(error.exit_status().code(), 2);
        assert!(error.to_string().contains("port must be 1-65535"));
    }

    #[test]
    fn raw_mode_guard_restores_after_error() {
        let restored = Arc::new(AtomicUsize::new(0));
        let backend = MockRawModeBackend {
            restored: Arc::clone(&restored),
        };

        let error = run_with_raw_mode(&backend, true, || -> Result<(), CliError> {
            Err(CliError::internal("streaming connection failed"))
        })
        .expect_err("body error should be returned");

        assert!(error.to_string().contains("streaming connection failed"));
        assert_eq!(restored.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn raw_mode_guard_skips_restore_when_disabled() {
        let restored = Arc::new(AtomicUsize::new(0));
        let backend = MockRawModeBackend {
            restored: Arc::clone(&restored),
        };

        run_with_raw_mode(&backend, false, || Ok(())).expect("body should succeed");

        assert_eq!(restored.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn resize_events_skip_cleanly_when_disabled() {
        let events = ResizeEvents::start(false);

        assert_eq!(events.initial(), None);
    }

    #[test]
    fn terminal_size_returns_none_for_invalid_fd() {
        assert_eq!(terminal_size(-1), None);
    }

    #[test]
    fn select_stream_url_rejects_spdy_without_fallback() {
        let error = select_stream_url(
            "exec",
            StreamProtocol::Spdy,
            Some("ws://127.0.0.1/exec/token"),
        )
        .expect_err("spdy should fail clearly");

        assert!(error
            .to_string()
            .contains("SPDY streaming is not supported by crs yet"));
    }

    #[test]
    fn select_stream_url_requires_websocket_url() {
        let error = select_stream_url("attach", StreamProtocol::Websocket, None)
            .expect_err("missing websocket URL should fail");

        assert!(error.to_string().contains("URL resolution"));
    }

    #[tokio::test]
    async fn websocket_stream_sends_stdin_and_receives_output_channels() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener addr should exist");
        let received_stdin = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_stdin_for_server = Arc::clone(&received_stdin);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client should connect");
            let mut stream = BufReader::new(stream);
            let mut request_headers = Vec::new();
            loop {
                let mut line = String::new();
                stream
                    .read_line(&mut line)
                    .await
                    .expect("request line should read");
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                request_headers.push(line);
            }
            let key = request_headers
                .iter()
                .find_map(|line| {
                    line.strip_prefix("Sec-WebSocket-Key:")
                        .map(|value| value.trim().to_string())
                })
                .expect("client should send websocket key");
            let response = format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\nSec-WebSocket-Protocol: v4.channel.k8s.io\r\n\r\n",
                websocket_accept_value(&key)
            );
            let mut stream = stream.into_inner();
            stream
                .write_all(response.as_bytes())
                .await
                .expect("handshake response should write");

            let frame = read_websocket_frame(&mut stream)
                .await
                .expect("stdin frame should read")
                .expect("stdin frame should exist");
            assert_eq!(frame.opcode, WS_BINARY_OPCODE);
            assert_eq!(frame.payload.first().copied(), Some(WS_CHANNEL_STDIN));
            *received_stdin_for_server.lock().await = frame.payload[1..].to_vec();

            write_websocket_frame(
                &mut stream,
                WS_BINARY_OPCODE,
                &[WS_CHANNEL_STDOUT, b'o', b'k'],
                false,
            )
            .await
            .expect("stdout should write");
            write_websocket_frame(
                &mut stream,
                WS_BINARY_OPCODE,
                &[WS_CHANNEL_STDERR, b'e', b'r', b'r'],
                false,
            )
            .await
            .expect("stderr should write");
            write_websocket_frame(&mut stream, WS_CLOSE_OPCODE, &[], false)
                .await
                .expect("close should write");
        });

        let output = websocket_stream(
            &format!("ws://{addr}/exec/token"),
            WebsocketIo {
                stdin: b"input".to_vec(),
            },
        )
        .await
        .expect("websocket stream should complete");

        server.await.expect("server task should finish");
        assert_eq!(*received_stdin.lock().await, b"input".to_vec());
        assert_eq!(output.stdout, b"ok".to_vec());
        assert_eq!(output.stderr, b"err".to_vec());
    }

    struct MockRawModeBackend {
        restored: Arc<AtomicUsize>,
    }

    fn run_with_raw_mode<B, F, T>(backend: &B, enabled: bool, body: F) -> Result<T, CliError>
    where
        B: RawModeBackend,
        F: FnOnce() -> Result<T, CliError>,
    {
        let _guard = backend.enter_raw_mode(enabled)?;
        body()
    }

    impl RawModeBackend for MockRawModeBackend {
        type Guard = MockRawModeGuard;

        fn enter_raw_mode(&self, enabled: bool) -> Result<Option<Self::Guard>, CliError> {
            Ok(enabled.then(|| MockRawModeGuard {
                restored: Arc::clone(&self.restored),
            }))
        }
    }

    struct MockRawModeGuard {
        restored: Arc<AtomicUsize>,
    }

    impl Drop for MockRawModeGuard {
        fn drop(&mut self) {
            self.restored.fetch_add(1, Ordering::SeqCst);
        }
    }
}
