//! IO管理 - 处理容器的stdin/stdout/stderr
//!
//! 功能：
//! 1. 重定向容器IO流到文件或socket
//! 2. 支持attach功能（多客户端连接）
//! 3. 支持TTY模式
//! 4. 日志持久化（CRI text log格式）

use crate::attach::{encode_attach_output_frame, ATTACH_PIPE_STDERR, ATTACH_PIPE_STDOUT};
use anyhow::{Context, Result};
use log::{debug, error, info};
use nix::unistd::{chown, Gid, Uid};
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixDatagram, UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const CRI_LOG_STREAM_STDOUT: &str = "stdout";
const CRI_LOG_STREAM_STDERR: &str = "stderr";
const CRI_LOG_TAG_FULL: &str = "F";
const CRI_LOG_TAG_PARTIAL: &str = "P";
// Match containerd/cri-o's default CRI logger buffer so long lines split
// into `P` / `F` records at the same boundary `crictl logs` expects.
const DEFAULT_CRI_LOG_LINE_BUFFER_SIZE: usize = 4096;
pub const DEFAULT_JOURNALD_SOCKET_PATH: &str = "/run/systemd/journal/socket";

#[derive(Debug, Default)]
struct PendingLogBytes {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// IO配置
#[derive(Debug, Clone)]
pub struct IoConfig {
    /// stdin重定向文件
    pub stdin: Option<PathBuf>,
    /// stdout重定向文件
    pub stdout: Option<PathBuf>,
    /// stderr重定向文件
    pub stderr: Option<PathBuf>,
    /// TTY模式
    pub terminal: bool,
    /// attach socket地址
    pub attach_socket: Option<PathBuf>,
    /// resize socket地址
    pub resize_socket: Option<PathBuf>,
    /// reopen log socket地址
    pub reopen_socket: Option<PathBuf>,
    /// journald 双写配置
    pub journald: Option<JournalConfig>,
    /// 是否在日志轮转和容器退出时跳过 sync。
    pub no_sync_log: bool,
    /// shim 创建的宿主 IO 工件默认 UID。
    pub io_uid: u32,
    /// shim 创建的宿主 IO 工件默认 GID。
    pub io_gid: u32,
    /// CRI 单条日志记录切分阈值（字节）。
    pub max_log_line_size: usize,
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            stdin: None,
            stdout: None,
            stderr: None,
            terminal: false,
            attach_socket: None,
            resize_socket: None,
            reopen_socket: None,
            journald: None,
            no_sync_log: false,
            io_uid: 0,
            io_gid: 0,
            max_log_line_size: DEFAULT_CRI_LOG_LINE_BUFFER_SIZE,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub socket_path: PathBuf,
    pub container_id: String,
    pub container_name: Option<String>,
    pub syslog_identifier: String,
}

/// IO管理器
#[derive(Clone)]
pub struct IoManager {
    config: Arc<Mutex<IoConfig>>,
    /// 当前attach的客户端
    clients: Arc<Mutex<Vec<ClientConnection>>>,
    /// 日志文件
    log_file: Arc<Mutex<Option<File>>>,
    /// 尚未形成完整 CRI 记录的 stdout/stderr 缓冲
    pending_logs: Arc<Mutex<PendingLogBytes>>,
    /// 当前TTY控制台文件，用于resize
    console_file: Arc<Mutex<Option<File>>>,
    /// attach/resize/reopen listener 生命周期
    server_threads: Arc<Mutex<Vec<ServerThread>>>,
}

/// 客户端连接
#[derive(Debug)]
struct ClientConnection {
    id: usize,
    stream: UnixStream,
}

#[derive(Debug, Deserialize)]
struct TerminalResizeRequest {
    #[serde(alias = "Width")]
    width: u16,
    #[serde(alias = "Height")]
    height: u16,
}

struct ServerThread {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
    sockets: Vec<PathBuf>,
}

impl IoManager {
    fn should_record_logs(&self) -> bool {
        let config = self.config.lock().unwrap();
        config.stdout.is_some() || config.journald.is_some()
    }

    fn parse_cri_log_record(record: &[u8]) -> Option<(&str, &str, &[u8])> {
        let first_space = record.iter().position(|byte| *byte == b' ')?;
        let second_space = record[first_space + 1..]
            .iter()
            .position(|byte| *byte == b' ')?;
        let second_space = first_space + 1 + second_space;
        let third_space = record[second_space + 1..]
            .iter()
            .position(|byte| *byte == b' ')?;
        let third_space = second_space + 1 + third_space;

        let stream = std::str::from_utf8(&record[first_space + 1..second_space]).ok()?;
        let tag = std::str::from_utf8(&record[second_space + 1..third_space]).ok()?;
        let payload = record
            .get(third_space + 1..)
            .map(|payload| payload.strip_suffix(b"\n").unwrap_or(payload))?;
        Some((stream, tag, payload))
    }

    fn append_journald_text_field(entry: &mut Vec<u8>, key: &str, value: &str) {
        entry.extend_from_slice(key.as_bytes());
        entry.push(b'=');
        entry.extend_from_slice(value.as_bytes());
        entry.push(b'\n');
    }

    fn append_journald_binary_field(entry: &mut Vec<u8>, key: &str, value: &[u8]) {
        entry.extend_from_slice(key.as_bytes());
        entry.push(b'\n');
        entry.extend_from_slice(&(value.len() as u64).to_le_bytes());
        entry.extend_from_slice(value);
        entry.push(b'\n');
    }

    fn send_journald_records(&self, records: &[Vec<u8>]) {
        let journal = self.config.lock().unwrap().journald.clone();
        let Some(journal) = journal.as_ref() else {
            return;
        };
        if records.is_empty() {
            return;
        }

        let socket = match UnixDatagram::unbound() {
            Ok(socket) => socket,
            Err(err) => {
                debug!("Failed to create journald datagram socket: {}", err);
                return;
            }
        };

        for record in records {
            let Some((stream, tag, payload)) = Self::parse_cri_log_record(record) else {
                debug!("Skipping unparsable CRI log record for journald duplication");
                continue;
            };

            let mut entry = Vec::new();
            Self::append_journald_text_field(
                &mut entry,
                "SYSLOG_IDENTIFIER",
                &journal.syslog_identifier,
            );
            Self::append_journald_text_field(&mut entry, "CONTAINER_ID", &journal.container_id);
            if let Some(container_name) = journal.container_name.as_deref() {
                Self::append_journald_text_field(&mut entry, "CONTAINER_NAME", container_name);
            }
            Self::append_journald_text_field(&mut entry, "CONTAINER_STREAM", stream);
            Self::append_journald_text_field(&mut entry, "CRI_LOG_TAG", tag);
            Self::append_journald_text_field(
                &mut entry,
                "PRIORITY",
                if stream == CRI_LOG_STREAM_STDERR {
                    "3"
                } else {
                    "6"
                },
            );
            Self::append_journald_binary_field(&mut entry, "MESSAGE", payload);

            if let Err(err) = socket.send_to(&entry, &journal.socket_path) {
                debug!(
                    "Failed to duplicate container log to journald socket {}: {}",
                    journal.socket_path.display(),
                    err
                );
                break;
            }
        }
    }

    fn cleanup_socket_path(socket: &PathBuf) {
        if let Err(err) = std::fs::remove_file(socket) {
            if err.kind() != std::io::ErrorKind::NotFound {
                debug!("Failed to remove socket {}: {}", socket.display(), err);
            }
        }
    }

    fn register_server_thread(
        &self,
        stop: Arc<AtomicBool>,
        handle: JoinHandle<()>,
        sockets: Vec<PathBuf>,
    ) {
        self.server_threads.lock().unwrap().push(ServerThread {
            stop,
            handle,
            sockets,
        });
    }

    fn max_log_line_size(&self) -> usize {
        self.config.lock().unwrap().max_log_line_size.max(1)
    }

    fn apply_io_ownership(path: &PathBuf, io_uid: u32, io_gid: u32) -> Result<()> {
        chown(
            path,
            Some(Uid::from_raw(io_uid)),
            Some(Gid::from_raw(io_gid)),
        )
        .with_context(|| {
            format!(
                "Failed to set IO ownership on {} to {}:{}",
                path.display(),
                io_uid,
                io_gid
            )
        })?;
        Ok(())
    }

    fn open_output_file(path: &PathBuf, io_uid: u32, io_gid: u32) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("Failed to open log file {}", path.display()))?;
        Self::apply_io_ownership(path, io_uid, io_gid)?;
        Ok(file)
    }

    fn pending_buffer_mut<'a>(
        pending_logs: &'a mut PendingLogBytes,
        stream: &str,
    ) -> &'a mut Vec<u8> {
        match stream {
            CRI_LOG_STREAM_STDOUT => &mut pending_logs.stdout,
            CRI_LOG_STREAM_STDERR => &mut pending_logs.stderr,
            _ => unreachable!("unsupported log stream {}", stream),
        }
    }

    fn encode_cri_log_record(stream: &str, tag: &str, content: &[u8]) -> Vec<u8> {
        let mut record = chrono::Local::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
            .into_bytes();
        record.push(b' ');
        record.extend_from_slice(stream.as_bytes());
        record.push(b' ');
        record.extend_from_slice(tag.as_bytes());
        record.push(b' ');
        record.extend_from_slice(content);
        record.push(b'\n');
        record
    }

    fn drain_cri_log_records(
        &self,
        pending: &mut Vec<u8>,
        stream: &str,
        data: &[u8],
        flush_partial: bool,
    ) -> Vec<Vec<u8>> {
        pending.extend_from_slice(data);
        let mut records = Vec::new();

        loop {
            let search_len = pending.len().min(self.max_log_line_size());
            if let Some(pos) = pending[..search_len].iter().position(|byte| *byte == b'\n') {
                let mut content = pending.drain(..=pos).collect::<Vec<u8>>();
                content.pop();
                if matches!(content.last(), Some(b'\r')) {
                    content.pop();
                }
                records.push(Self::encode_cri_log_record(
                    stream,
                    CRI_LOG_TAG_FULL,
                    &content,
                ));
                continue;
            }

            if pending.len() < self.max_log_line_size() {
                break;
            }

            let mut split_at = self.max_log_line_size();
            if matches!(pending.get(split_at - 1), Some(b'\r')) {
                split_at -= 1;
            }
            if split_at == 0 {
                break;
            }

            let content = pending.drain(..split_at).collect::<Vec<u8>>();
            records.push(Self::encode_cri_log_record(
                stream,
                CRI_LOG_TAG_PARTIAL,
                &content,
            ));
        }

        if flush_partial && !pending.is_empty() {
            let content = std::mem::take(pending);
            records.push(Self::encode_cri_log_record(
                stream,
                CRI_LOG_TAG_PARTIAL,
                &content,
            ));
        }

        records
    }

    fn take_log_records(&self, stream: &str, data: &[u8], flush_partial: bool) -> Vec<Vec<u8>> {
        let mut pending_logs = self.pending_logs.lock().unwrap();
        let pending = Self::pending_buffer_mut(&mut pending_logs, stream);
        self.drain_cri_log_records(pending, stream, data, flush_partial)
    }

    fn write_log_records(&self, records: &[Vec<u8>]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            for record in records {
                file.write_all(record)?;
            }
            file.flush()?;
        }
        self.send_journald_records(records);

        Ok(())
    }

    fn maybe_sync_log_file(&self, file: &File) -> Result<bool> {
        if self.config.lock().unwrap().no_sync_log {
            return Ok(false);
        }
        file.sync_all().context("Failed to sync log file to disk")?;
        Ok(true)
    }

    fn remove_clients(&self, disconnected_ids: &[usize]) {
        if disconnected_ids.is_empty() {
            return;
        }

        let mut clients = self.clients.lock().unwrap();
        clients.retain(|client| !disconnected_ids.contains(&client.id));
    }

    fn broadcast(&self, data: &[u8]) -> Result<()> {
        let streams: Vec<(usize, UnixStream)> = {
            let clients = self.clients.lock().unwrap();
            clients
                .iter()
                .filter_map(|client| {
                    client
                        .stream
                        .try_clone()
                        .ok()
                        .map(|stream| (client.id, stream))
                })
                .collect()
        };

        let mut disconnected = Vec::new();
        for (id, mut stream) in streams {
            if let Err(e) = stream.write_all(data) {
                debug!("Client {} disconnected: {}", id, e);
                disconnected.push(id);
            }
        }

        self.remove_clients(&disconnected);
        Ok(())
    }

    fn broadcast_output(&self, pipe: u8, data: &[u8]) -> Result<()> {
        let frame = encode_attach_output_frame(pipe, data);
        self.broadcast(&frame)
    }

    fn first_client_stream(&self) -> Option<(usize, UnixStream)> {
        let clients = self.clients.lock().unwrap();
        clients.iter().find_map(|client| {
            client
                .stream
                .try_clone()
                .ok()
                .map(|stream| (client.id, stream))
        })
    }

    /// 创建新的IO管理器
    pub fn new() -> Self {
        Self {
            config: Arc::new(Mutex::new(IoConfig::default())),
            clients: Arc::new(Mutex::new(Vec::new())),
            log_file: Arc::new(Mutex::new(None)),
            pending_logs: Arc::new(Mutex::new(PendingLogBytes::default())),
            console_file: Arc::new(Mutex::new(None)),
            server_threads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 配置IO
    pub fn configure(&mut self, config: IoConfig) -> Result<()> {
        *self.config.lock().unwrap() = config.clone();

        // 设置日志文件
        if let Some(stdout) = &config.stdout {
            let parent = stdout.parent().context("Invalid stdout path")?;
            std::fs::create_dir_all(parent)?;
            let mut log_file = self.log_file.lock().unwrap();
            *log_file = Some(Self::open_output_file(
                stdout,
                config.io_uid,
                config.io_gid,
            )?);
            info!("Log file configured: {:?}", stdout);
        }

        Ok(())
    }

    /// 启动attach服务器
    pub fn start_attach_server(&mut self) -> Result<()> {
        let config = self.config.lock().unwrap().clone();
        if let Some(socket) = &config.attach_socket {
            if let Some(parent) = socket.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // 删除已存在的socket文件
            Self::cleanup_socket_path(socket);

            let listener = UnixListener::bind(socket)?;
            Self::apply_io_ownership(socket, config.io_uid, config.io_gid)?;
            listener.set_nonblocking(true)?;
            info!("Attach server listening on {:?}", socket);

            let clients = self.clients.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = stop.clone();
            let handle = std::thread::spawn(move || {
                let mut next_id = 0usize;
                while !stop_for_thread.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            let id = next_id;
                            next_id += 1;
                            debug!("New attach client connected: {}", id);
                            let mut clients = clients.lock().unwrap();
                            clients.push(ClientConnection { id, stream });
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(25));
                        }
                        Err(_err) if stop_for_thread.load(Ordering::Relaxed) => break,
                        Err(err) => {
                            error!("Failed to accept client: {}", err);
                        }
                    }
                }
            });
            self.register_server_thread(stop, handle, vec![socket.clone()]);
        }

        Ok(())
    }

    /// 启动resize服务器
    pub fn start_resize_server(&self) -> Result<()> {
        let config = self.config.lock().unwrap().clone();
        if let Some(socket) = &config.resize_socket {
            if let Some(parent) = socket.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Self::cleanup_socket_path(socket);

            let listener = UnixListener::bind(socket)?;
            Self::apply_io_ownership(socket, config.io_uid, config.io_gid)?;
            listener.set_nonblocking(true)?;
            info!("Resize server listening on {:?}", socket);
            let io_manager = self.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = stop.clone();
            let handle = std::thread::spawn(move || {
                while !stop_for_thread.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            let io_manager = io_manager.clone();
                            std::thread::spawn(move || {
                                let reader = BufReader::new(stream);
                                for line in reader.lines() {
                                    match line {
                                        Ok(payload) if payload.trim().is_empty() => continue,
                                        Ok(payload) => {
                                            match serde_json::from_str::<TerminalResizeRequest>(
                                                &payload,
                                            ) {
                                                Ok(size) => {
                                                    if let Err(e) = io_manager
                                                        .apply_terminal_resize(
                                                            size.width,
                                                            size.height,
                                                        )
                                                    {
                                                        debug!(
                                                            "Failed to apply terminal resize: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    debug!(
                                                        "Ignoring invalid resize payload {:?}: {}",
                                                        payload, e
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            debug!("Resize client disconnected: {}", e);
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(25));
                        }
                        Err(_err) if stop_for_thread.load(Ordering::Relaxed) => break,
                        Err(err) => {
                            error!("Failed to accept resize client: {}", err);
                        }
                    }
                }
            });
            self.register_server_thread(stop, handle, vec![socket.clone()]);
        }

        Ok(())
    }

    /// 启动日志重开服务器
    pub fn start_reopen_log_server(&self) -> Result<()> {
        let config = self.config.lock().unwrap().clone();
        if let Some(socket) = &config.reopen_socket {
            if let Some(parent) = socket.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Self::cleanup_socket_path(socket);

            let listener = UnixListener::bind(socket)?;
            Self::apply_io_ownership(socket, config.io_uid, config.io_gid)?;
            listener.set_nonblocking(true)?;
            info!("Log reopen server listening on {:?}", socket);
            let io_manager = self.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = stop.clone();
            let handle = std::thread::spawn(move || {
                while !stop_for_thread.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut stream, _addr)) => {
                            let response = match io_manager.reopen_log_file() {
                                Ok(()) => "OK\n".to_string(),
                                Err(e) => {
                                    debug!("Failed to reopen log file on control request: {}", e);
                                    format!("ERR {}\n", e)
                                }
                            };
                            if let Err(e) = stream.write_all(response.as_bytes()) {
                                debug!("Failed to write reopen-log response: {}", e);
                                continue;
                            }
                            if let Err(e) = stream.flush() {
                                debug!("Failed to flush reopen-log response: {}", e);
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(25));
                        }
                        Err(_err) if stop_for_thread.load(Ordering::Relaxed) => break,
                        Err(err) => {
                            error!("Failed to accept reopen-log client: {}", err);
                        }
                    }
                }
            });
            self.register_server_thread(stop, handle, vec![socket.clone()]);
        }

        Ok(())
    }

    fn set_console_for_resize(&self, console: &File) -> Result<()> {
        let mut stored_console = self.console_file.lock().unwrap();
        *stored_console = Some(
            console
                .try_clone()
                .context("Failed to clone console file for resize handling")?,
        );
        Ok(())
    }

    pub fn apply_terminal_resize(&self, width: u16, height: u16) -> Result<()> {
        if width == 0 || height == 0 {
            return Err(anyhow::anyhow!(
                "terminal width and height must be greater than zero"
            ));
        }

        let console = self.console_file.lock().unwrap();
        let console = console
            .as_ref()
            .context("terminal console is not available for resize")?;
        let winsize = nix::libc::winsize {
            ws_row: height,
            ws_col: width,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let result =
            unsafe { nix::libc::ioctl(console.as_raw_fd(), nix::libc::TIOCSWINSZ, &winsize) };
        if result < 0 {
            return Err(anyhow::anyhow!(
                "failed to apply terminal resize: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    pub fn reopen_log_file(&self) -> Result<()> {
        let config = self.config.lock().unwrap().clone();
        let Some(stdout) = config.stdout.as_ref() else {
            return Ok(());
        };
        let parent = stdout.parent().context("Invalid stdout path")?;
        std::fs::create_dir_all(parent)?;

        let reopened = Self::open_output_file(stdout, config.io_uid, config.io_gid)?;
        let mut log_file = self.log_file.lock().unwrap();
        if let Some(file) = log_file.as_mut() {
            file.flush()?;
            self.maybe_sync_log_file(file)?;
        }
        *log_file = Some(reopened);
        info!("Reopened log file: {:?}", stdout);
        Ok(())
    }

    pub fn finish_stdout(&self) -> Result<()> {
        if self.should_record_logs() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDOUT, &[], true);
            self.write_log_records(&records)?;
        }
        Ok(())
    }

    pub fn finish_stderr(&self) -> Result<()> {
        if self.should_record_logs() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDERR, &[], true);
            self.write_log_records(&records)?;
        }
        Ok(())
    }

    /// 写入stdout
    pub fn write_stdout(&self, data: &[u8]) -> Result<()> {
        if self.should_record_logs() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDOUT, data, false);
            self.write_log_records(&records)?;
        }

        // 发送到所有attach客户端
        self.broadcast_output(ATTACH_PIPE_STDOUT, data)?;

        Ok(())
    }

    /// 写入stderr
    pub fn write_stderr(&self, data: &[u8]) -> Result<()> {
        if self.should_record_logs() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDERR, data, false);
            self.write_log_records(&records)?;
        }

        self.broadcast_output(ATTACH_PIPE_STDERR, data)
    }

    /// 读取stdin
    pub fn read_stdin(&self) -> Result<Vec<u8>> {
        if let Some((id, mut stream)) = self.first_client_stream() {
            let mut buffer = [0u8; 1024];
            match stream.read(&mut buffer) {
                Ok(n) if n > 0 => return Ok(buffer[..n].to_vec()),
                Ok(_) => {}
                Err(e) => {
                    debug!("Client {} disconnected: {}", id, e);
                    self.remove_clients(&[id]);
                }
            }
        }
        Ok(Vec::new())
    }

    /// 启动控制台转发
    pub fn start_console_bridge(&self, console: File) -> Result<()> {
        self.set_console_for_resize(&console)?;
        let reader = console
            .try_clone()
            .context("Failed to clone console file for reading")?;
        let writer = console;
        let io_for_output = self.clone();
        let io_for_input = self.clone();

        std::thread::spawn(move || {
            let mut reader = reader;
            let mut buffer = [0u8; 8192];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let _ = io_for_output.finish_stdout();
                        break;
                    }
                    Ok(n) => {
                        if let Err(e) = io_for_output.write_stdout(&buffer[..n]) {
                            error!("Failed to forward console output: {}", e);
                        }
                    }
                    Err(e) => {
                        debug!("Console output pump stopped: {}", e);
                        let _ = io_for_output.finish_stdout();
                        break;
                    }
                }
            }
        });

        std::thread::spawn(move || {
            let mut writer = writer;
            loop {
                match io_for_input.read_stdin() {
                    Ok(data) if !data.is_empty() => {
                        if let Err(e) = writer.write_all(&data) {
                            debug!("Console input pump stopped: {}", e);
                            break;
                        }
                        let _ = writer.flush();
                    }
                    Ok(_) => {
                        std::thread::sleep(std::time::Duration::from_millis(25));
                    }
                    Err(e) => {
                        debug!("Console input pump stopped: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 设置容器IO重定向（用于runc）
    pub fn setup_stdio_pipes(&self) -> Result<(Option<File>, Option<File>, Option<File>)> {
        let config = self.config.lock().unwrap().clone();
        // 创建管道用于与容器通信
        // stdin
        let stdin = if let Some(stdin) = &config.stdin {
            let file = File::open(stdin)?;
            Some(file)
        } else {
            None
        };

        // stdout - 直接写入日志文件
        let stdout = if let Some(stdout) = &config.stdout {
            let parent = stdout.parent().context("Invalid stdout path")?;
            std::fs::create_dir_all(parent)?;
            let file = Self::open_output_file(stdout, config.io_uid, config.io_gid)?;
            Some(file)
        } else {
            None
        };

        // stderr
        let stderr = if let Some(stderr) = &config.stderr {
            let parent = stderr.parent().context("Invalid stderr path")?;
            std::fs::create_dir_all(parent)?;
            let file = Self::open_output_file(stderr, config.io_uid, config.io_gid)?;
            Some(file)
        } else {
            None
        };

        Ok((stdin, stdout, stderr))
    }

    /// 关闭所有客户端连接
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down IO manager");

        let server_threads = {
            let mut server_threads = self.server_threads.lock().unwrap();
            std::mem::take(&mut *server_threads)
        };
        for server_thread in server_threads {
            server_thread.stop.store(true, Ordering::Relaxed);
            let _ = server_thread.handle.join();
            for socket in &server_thread.sockets {
                Self::cleanup_socket_path(socket);
            }
        }

        let mut clients = self.clients.lock().unwrap();
        clients.clear();
        drop(clients);

        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            file.flush()?;
            self.maybe_sync_log_file(file)?;
        }
        *self.console_file.lock().unwrap() = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::unistd::{getegid, geteuid};
    use std::os::unix::fs::MetadataExt;
    use tempfile::tempdir;

    fn parse_cri_log_lines(contents: &str) -> Vec<(String, String, String, String)> {
        contents
            .lines()
            .map(|line| {
                let mut parts = line.splitn(4, ' ');
                (
                    parts.next().unwrap_or_default().to_string(),
                    parts.next().unwrap_or_default().to_string(),
                    parts.next().unwrap_or_default().to_string(),
                    parts.next().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    fn test_io_owner_ids() -> (u32, u32) {
        let current_uid = geteuid().as_raw();
        let current_gid = getegid().as_raw();
        if current_uid == 0 && current_gid == 0 {
            (1, 1)
        } else {
            (current_uid, current_gid)
        }
    }

    #[test]
    fn test_io_manager_creation() {
        let manager = IoManager::new();
        assert!(!manager.config.lock().unwrap().terminal);
    }

    #[test]
    fn test_io_config_default() {
        let config = IoConfig::default();
        assert!(!config.terminal);
        assert!(config.stdin.is_none());
        assert!(config.stdout.is_none());
        assert!(config.stderr.is_none());
    }

    #[test]
    fn test_io_manager_configure() {
        let temp_dir = tempdir().unwrap();
        let mut manager = IoManager::new();

        let config = IoConfig {
            stdout: Some(temp_dir.path().join("stdout.log")),
            ..Default::default()
        };

        manager.configure(config).unwrap();
        assert!(manager.log_file.lock().unwrap().is_some());
    }

    #[test]
    fn test_write_stdout_emits_cri_full_record() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"hello world\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert!(chrono::DateTime::parse_from_rfc3339(&records[0].0).is_ok());
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].2, "F");
        assert_eq!(records[0].3, "hello world");
    }

    #[test]
    fn test_write_stderr_emits_cri_full_record() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stderr.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        manager.write_stderr(b"error line\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert!(chrono::DateTime::parse_from_rfc3339(&records[0].0).is_ok());
        assert_eq!(records[0].1, "stderr");
        assert_eq!(records[0].2, "F");
        assert_eq!(records[0].3, "error line");
    }

    #[test]
    fn test_write_stdout_flushes_partial_record_on_finish() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"partial line").unwrap();
        manager.finish_stdout().unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].2, "P");
        assert_eq!(records[0].3, "partial line");
    }

    #[test]
    fn test_write_stdout_splits_multiline_chunk_into_multiple_records() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"first line\nsecond line\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].2, "F");
        assert_eq!(records[0].3, "first line");
        assert_eq!(records[1].2, "F");
        assert_eq!(records[1].3, "second line");
    }

    #[test]
    fn test_write_stdout_buffers_short_partial_across_multiple_writes() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"hello ").unwrap();
        assert_eq!(std::fs::read_to_string(&log_path).unwrap(), "");

        manager.write_stdout(b"world\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].2, "F");
        assert_eq!(records[0].3, "hello world");
    }

    #[test]
    fn test_write_stdout_splits_long_line_on_cri_buffer_boundary() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        let long_prefix = "a".repeat(DEFAULT_CRI_LOG_LINE_BUFFER_SIZE);
        manager.write_stdout(long_prefix.as_bytes()).unwrap();

        let contents = std::fs::read_to_string(&log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].2, "P");
        assert_eq!(records[0].3.len(), DEFAULT_CRI_LOG_LINE_BUFFER_SIZE);

        manager.write_stdout(b"tail\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].2, "P");
        assert_eq!(records[0].3.len(), DEFAULT_CRI_LOG_LINE_BUFFER_SIZE);
        assert_eq!(records[1].2, "F");
        assert_eq!(records[1].3, "tail");
    }

    #[test]
    fn test_write_stdout_uses_configured_log_line_size_boundary() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                max_log_line_size: 8,
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"abcdefgh").unwrap();

        let contents = std::fs::read_to_string(&log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].2, "P");
        assert_eq!(records[0].3, "abcdefgh");
    }

    #[test]
    fn test_reopen_log_file_keeps_logging_available() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();

        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();
        manager.write_stdout(b"before\n").unwrap();
        manager.reopen_log_file().unwrap();
        manager.write_stdout(b"after\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].3, "before");
        assert_eq!(records[1].3, "after");
    }

    #[test]
    fn test_reopen_log_file_after_rotation_writes_new_records_to_fresh_path() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let rotated_path = temp_dir.path().join("stdout.log.1");
        let mut manager = IoManager::new();

        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();
        manager.write_stdout(b"before-rotate\n").unwrap();

        std::fs::rename(&log_path, &rotated_path).unwrap();

        manager.reopen_log_file().unwrap();
        manager.write_stdout(b"after-rotate\n").unwrap();

        let rotated_contents = std::fs::read_to_string(&rotated_path).unwrap();
        let rotated_records = parse_cri_log_lines(&rotated_contents);
        assert_eq!(rotated_records.len(), 1);
        assert_eq!(rotated_records[0].3, "before-rotate");

        let fresh_contents = std::fs::read_to_string(&log_path).unwrap();
        let fresh_records = parse_cri_log_lines(&fresh_contents);
        assert_eq!(fresh_records.len(), 1);
        assert_eq!(fresh_records[0].3, "after-rotate");
    }

    #[test]
    fn test_reopen_log_file_after_truncate_appends_to_truncated_file() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();

        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();
        manager.write_stdout(b"before-truncate\n").unwrap();

        std::fs::write(&log_path, "").unwrap();

        manager.reopen_log_file().unwrap();
        manager.write_stdout(b"after-truncate\n").unwrap();

        let contents = std::fs::read_to_string(&log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].3, "after-truncate");
    }

    #[test]
    fn test_configure_applies_configured_io_owner_to_log_file() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("owned.log");
        let (io_uid, io_gid) = test_io_owner_ids();
        let mut manager = IoManager::new();

        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                io_uid,
                io_gid,
                ..Default::default()
            })
            .unwrap();

        let metadata = std::fs::metadata(&log_path).unwrap();
        assert_eq!(metadata.uid(), io_uid);
        assert_eq!(metadata.gid(), io_gid);
    }

    #[test]
    fn test_control_sockets_apply_configured_io_owner() {
        let temp_dir = tempdir().unwrap();
        let attach_socket = temp_dir.path().join("attach.sock");
        let resize_socket = temp_dir.path().join("resize.sock");
        let reopen_socket = temp_dir.path().join("reopen.sock");
        let (io_uid, io_gid) = test_io_owner_ids();
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                attach_socket: Some(attach_socket.clone()),
                resize_socket: Some(resize_socket.clone()),
                reopen_socket: Some(reopen_socket.clone()),
                io_uid,
                io_gid,
                ..Default::default()
            })
            .unwrap();

        manager.start_attach_server().unwrap();
        manager.start_resize_server().unwrap();
        manager.start_reopen_log_server().unwrap();

        for _ in 0..50 {
            if attach_socket.exists() && resize_socket.exists() && reopen_socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        for socket in [&attach_socket, &resize_socket, &reopen_socket] {
            let metadata = std::fs::metadata(socket).unwrap();
            assert_eq!(metadata.uid(), io_uid);
            assert_eq!(metadata.gid(), io_gid);
        }

        manager.shutdown().unwrap();
    }

    #[test]
    fn test_setup_stdio_pipes_applies_configured_io_owner() {
        let temp_dir = tempdir().unwrap();
        let stdout = temp_dir.path().join("stdio.stdout");
        let stderr = temp_dir.path().join("stdio.stderr");
        let (io_uid, io_gid) = test_io_owner_ids();
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(stdout.clone()),
                stderr: Some(stderr.clone()),
                io_uid,
                io_gid,
                ..Default::default()
            })
            .unwrap();

        let (_stdin, _stdout, _stderr) = manager.setup_stdio_pipes().unwrap();

        for path in [&stdout, &stderr] {
            let metadata = std::fs::metadata(path).unwrap();
            assert_eq!(metadata.uid(), io_uid);
            assert_eq!(metadata.gid(), io_gid);
        }
    }

    #[test]
    fn test_maybe_sync_log_file_skips_sync_when_no_sync_log_enabled() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                no_sync_log: true,
                ..Default::default()
            })
            .unwrap();

        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        assert!(!manager.maybe_sync_log_file(&file).unwrap());
    }

    #[test]
    fn test_maybe_sync_log_file_syncs_by_default() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("stdout.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                ..Default::default()
            })
            .unwrap();

        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        assert!(manager.maybe_sync_log_file(&file).unwrap());
    }

    #[test]
    fn test_apply_terminal_resize_requires_console() {
        let manager = IoManager::new();
        let err = manager.apply_terminal_resize(80, 24).unwrap_err();
        assert!(err
            .to_string()
            .contains("terminal console is not available"));
    }

    #[test]
    fn test_shutdown_removes_control_sockets_and_allows_rebind() {
        let temp_dir = tempdir().unwrap();
        let attach_socket = temp_dir.path().join("attach.sock");
        let resize_socket = temp_dir.path().join("resize.sock");
        let reopen_socket = temp_dir.path().join("reopen.sock");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                attach_socket: Some(attach_socket.clone()),
                resize_socket: Some(resize_socket.clone()),
                reopen_socket: Some(reopen_socket.clone()),
                ..Default::default()
            })
            .unwrap();

        manager.start_attach_server().unwrap();
        manager.start_resize_server().unwrap();
        manager.start_reopen_log_server().unwrap();

        for _ in 0..50 {
            if attach_socket.exists() && resize_socket.exists() && reopen_socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(attach_socket.exists());
        assert!(resize_socket.exists());
        assert!(reopen_socket.exists());

        manager.shutdown().unwrap();

        assert!(!attach_socket.exists());
        assert!(!resize_socket.exists());
        assert!(!reopen_socket.exists());
        UnixListener::bind(&attach_socket).unwrap();
        UnixListener::bind(&resize_socket).unwrap();
        UnixListener::bind(&reopen_socket).unwrap();
    }

    #[test]
    fn test_write_stdout_duplicates_cri_record_to_journald() {
        let temp_dir = tempdir().unwrap();
        let journal_socket = temp_dir.path().join("journal.sock");
        let listener = UnixDatagram::bind(&journal_socket).unwrap();
        listener
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                journald: Some(JournalConfig {
                    socket_path: journal_socket,
                    container_id: "ctr-journal".to_string(),
                    container_name: Some("demo".to_string()),
                    syslog_identifier: "crius-shim".to_string(),
                }),
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"hello journald\n").unwrap();

        let mut buffer = [0u8; 4096];
        let size = listener.recv(&mut buffer).unwrap();
        let entry = &buffer[..size];
        assert!(entry
            .windows("SYSLOG_IDENTIFIER=crius-shim\n".len())
            .any(|window| { window == b"SYSLOG_IDENTIFIER=crius-shim\n" }));
        assert!(entry
            .windows("CONTAINER_ID=ctr-journal\n".len())
            .any(|window| window == b"CONTAINER_ID=ctr-journal\n"));
        assert!(entry
            .windows("CONTAINER_NAME=demo\n".len())
            .any(|window| window == b"CONTAINER_NAME=demo\n"));
        assert!(entry
            .windows("CONTAINER_STREAM=stdout\n".len())
            .any(|window| window == b"CONTAINER_STREAM=stdout\n"));
        assert!(entry
            .windows("CRI_LOG_TAG=F\n".len())
            .any(|window| window == b"CRI_LOG_TAG=F\n"));
        assert!(entry
            .windows("MESSAGE\n".len())
            .any(|window| window == b"MESSAGE\n"));
        assert!(entry.ends_with(b"hello journald\n"));
    }

    #[test]
    fn test_write_stdout_keeps_file_logging_when_journald_socket_is_missing() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("container.log");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                journald: Some(JournalConfig {
                    socket_path: temp_dir.path().join("missing-journal.sock"),
                    container_id: "ctr-journal-missing".to_string(),
                    container_name: None,
                    syslog_identifier: "crius-shim".to_string(),
                }),
                ..Default::default()
            })
            .unwrap();

        manager.write_stdout(b"still-on-file\n").unwrap();

        let contents = std::fs::read_to_string(&log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].3, "still-on-file");
    }
}
