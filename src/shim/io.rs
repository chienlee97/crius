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
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const CRI_LOG_STREAM_STDOUT: &str = "stdout";
const CRI_LOG_STREAM_STDERR: &str = "stderr";
const CRI_LOG_TAG_FULL: &str = "F";
const CRI_LOG_TAG_PARTIAL: &str = "P";
// Match containerd/cri-o's default CRI logger buffer so long lines split
// into `P` / `F` records at the same boundary `crictl logs` expects.
const CRI_LOG_LINE_BUFFER_SIZE: usize = 4096;

#[derive(Debug, Default)]
struct PendingLogBytes {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// IO配置
#[derive(Debug, Clone, Default)]
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
}

/// IO管理器
#[derive(Clone)]
pub struct IoManager {
    config: IoConfig,
    /// 当前attach的客户端
    clients: Arc<Mutex<Vec<ClientConnection>>>,
    /// 日志文件
    log_file: Arc<Mutex<Option<File>>>,
    /// 尚未形成完整 CRI 记录的 stdout/stderr 缓冲
    pending_logs: Arc<Mutex<PendingLogBytes>>,
    /// 当前TTY控制台文件，用于resize
    console_file: Arc<Mutex<Option<File>>>,
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

impl IoManager {
    fn open_output_file(path: &PathBuf) -> Result<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("Failed to open log file {}", path.display()))
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
        pending: &mut Vec<u8>,
        stream: &str,
        data: &[u8],
        flush_partial: bool,
    ) -> Vec<Vec<u8>> {
        pending.extend_from_slice(data);
        let mut records = Vec::new();

        loop {
            let search_len = pending.len().min(CRI_LOG_LINE_BUFFER_SIZE);
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

            if pending.len() < CRI_LOG_LINE_BUFFER_SIZE {
                break;
            }

            let mut split_at = CRI_LOG_LINE_BUFFER_SIZE;
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
        Self::drain_cri_log_records(pending, stream, data, flush_partial)
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

        Ok(())
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
            config: IoConfig::default(),
            clients: Arc::new(Mutex::new(Vec::new())),
            log_file: Arc::new(Mutex::new(None)),
            pending_logs: Arc::new(Mutex::new(PendingLogBytes::default())),
            console_file: Arc::new(Mutex::new(None)),
        }
    }

    /// 配置IO
    pub fn configure(&mut self, config: IoConfig) -> Result<()> {
        self.config = config.clone();

        // 设置日志文件
        if let Some(stdout) = &config.stdout {
            let parent = stdout.parent().context("Invalid stdout path")?;
            std::fs::create_dir_all(parent)?;
            let mut log_file = self.log_file.lock().unwrap();
            *log_file = Some(Self::open_output_file(stdout)?);
            info!("Log file configured: {:?}", stdout);
        }

        Ok(())
    }

    /// 启动attach服务器
    pub fn start_attach_server(&mut self) -> Result<()> {
        if let Some(socket) = &self.config.attach_socket {
            // 删除已存在的socket文件
            let _ = std::fs::remove_file(socket);

            let listener = UnixListener::bind(socket)?;
            info!("Attach server listening on {:?}", socket);

            let clients = self.clients.clone();

            std::thread::spawn(move || {
                for (id, stream) in listener.incoming().enumerate() {
                    match stream {
                        Ok(stream) => {
                            debug!("New attach client connected: {}", id);
                            let mut clients = clients.lock().unwrap();
                            clients.push(ClientConnection { id, stream });
                        }
                        Err(e) => {
                            error!("Failed to accept client: {}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// 启动resize服务器
    pub fn start_resize_server(&self) -> Result<()> {
        if let Some(socket) = &self.config.resize_socket {
            let _ = std::fs::remove_file(socket);

            let listener = UnixListener::bind(socket)?;
            info!("Resize server listening on {:?}", socket);
            let io_manager = self.clone();

            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    match stream {
                        Ok(stream) => {
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
                        Err(e) => {
                            error!("Failed to accept resize client: {}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// 启动日志重开服务器
    pub fn start_reopen_log_server(&self) -> Result<()> {
        if let Some(socket) = &self.config.reopen_socket {
            let _ = std::fs::remove_file(socket);

            let listener = UnixListener::bind(socket)?;
            info!("Log reopen server listening on {:?}", socket);
            let io_manager = self.clone();

            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    match stream {
                        Ok(mut stream) => {
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
                        Err(e) => {
                            error!("Failed to accept reopen-log client: {}", e);
                        }
                    }
                }
            });
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
        let Some(stdout) = self.config.stdout.as_ref() else {
            return Ok(());
        };
        let parent = stdout.parent().context("Invalid stdout path")?;
        std::fs::create_dir_all(parent)?;

        let reopened = Self::open_output_file(stdout)?;
        let mut log_file = self.log_file.lock().unwrap();
        *log_file = Some(reopened);
        info!("Reopened log file: {:?}", stdout);
        Ok(())
    }

    pub fn finish_stdout(&self) -> Result<()> {
        if self.config.stdout.is_some() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDOUT, &[], true);
            self.write_log_records(&records)?;
        }
        Ok(())
    }

    pub fn finish_stderr(&self) -> Result<()> {
        if self.config.stdout.is_some() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDERR, &[], true);
            self.write_log_records(&records)?;
        }
        Ok(())
    }

    /// 写入stdout
    pub fn write_stdout(&self, data: &[u8]) -> Result<()> {
        if self.config.stdout.is_some() {
            let records = self.take_log_records(CRI_LOG_STREAM_STDOUT, data, false);
            self.write_log_records(&records)?;
        }

        // 发送到所有attach客户端
        self.broadcast_output(ATTACH_PIPE_STDOUT, data)?;

        Ok(())
    }

    /// 写入stderr
    pub fn write_stderr(&self, data: &[u8]) -> Result<()> {
        if self.config.stdout.is_some() {
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
        // 创建管道用于与容器通信
        // stdin
        let stdin = if self.config.stdin.is_some() {
            let file = File::open(self.config.stdin.as_ref().unwrap())?;
            Some(file)
        } else {
            None
        };

        // stdout - 直接写入日志文件
        let stdout = if let Some(stdout) = &self.config.stdout {
            let parent = stdout.parent().context("Invalid stdout path")?;
            std::fs::create_dir_all(parent)?;
            let file = Self::open_output_file(stdout)?;
            Some(file)
        } else {
            None
        };

        // stderr
        let stderr = if let Some(stderr) = &self.config.stderr {
            let parent = stderr.parent().context("Invalid stderr path")?;
            std::fs::create_dir_all(parent)?;
            let file = Self::open_output_file(stderr)?;
            Some(file)
        } else {
            None
        };

        Ok((stdin, stdout, stderr))
    }

    /// 关闭所有客户端连接
    pub fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down IO manager");

        let mut clients = self.clients.lock().unwrap();
        clients.clear();

        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            file.flush()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::pty::openpty;
    use std::os::unix::io::{AsRawFd, FromRawFd};
    use std::os::unix::net::UnixStream as StdUnixStream;
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

    #[test]
    fn test_io_manager_creation() {
        let manager = IoManager::new();
        assert!(!manager.config.terminal);
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

        let long_prefix = "a".repeat(CRI_LOG_LINE_BUFFER_SIZE);
        manager.write_stdout(long_prefix.as_bytes()).unwrap();

        let contents = std::fs::read_to_string(&log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, "stdout");
        assert_eq!(records[0].2, "P");
        assert_eq!(records[0].3.len(), CRI_LOG_LINE_BUFFER_SIZE);

        manager.write_stdout(b"tail\n").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        let records = parse_cri_log_lines(&contents);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].2, "P");
        assert_eq!(records[0].3.len(), CRI_LOG_LINE_BUFFER_SIZE);
        assert_eq!(records[1].2, "F");
        assert_eq!(records[1].3, "tail");
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
    fn test_apply_terminal_resize_requires_console() {
        let manager = IoManager::new();
        let err = manager.apply_terminal_resize(80, 24).unwrap_err();
        assert!(err
            .to_string()
            .contains("terminal console is not available"));
    }

    #[test]
    #[ignore = "requires unix socket bind permissions in the current test environment"]
    fn test_resize_server_applies_terminal_dimensions() {
        let temp_dir = tempdir().unwrap();
        let resize_socket = temp_dir.path().join("resize.sock");
        let mut manager = IoManager::new();
        manager
            .configure(IoConfig {
                terminal: true,
                resize_socket: Some(resize_socket.clone()),
                ..Default::default()
            })
            .unwrap();
        manager.start_resize_server().unwrap();

        let pty = openpty(None, None).unwrap();
        let _master = unsafe { File::from_raw_fd(pty.master) };
        let slave = unsafe { File::from_raw_fd(pty.slave) };
        let query_file = slave.try_clone().unwrap();
        manager.start_console_bridge(slave).unwrap();

        for _ in 0..20 {
            if resize_socket.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(resize_socket.exists());

        let mut client = StdUnixStream::connect(&resize_socket).unwrap();
        client.write_all(br#"{"Width":101,"Height":37}"#).unwrap();
        client.write_all(b"\n").unwrap();
        client.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        let mut winsize = nix::libc::winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let rc = unsafe {
            nix::libc::ioctl(query_file.as_raw_fd(), nix::libc::TIOCGWINSZ, &mut winsize)
        };
        assert_eq!(rc, 0);
        assert_eq!(winsize.ws_col, 101);
        assert_eq!(winsize.ws_row, 37);
    }
}
