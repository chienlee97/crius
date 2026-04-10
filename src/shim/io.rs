//! IO管理 - 处理容器的stdin/stdout/stderr
//!
//! 功能：
//! 1. 重定向容器IO流到文件或socket
//! 2. 支持attach功能（多客户端连接）
//! 3. 支持TTY模式
//! 4. 日志持久化（CRI JSON格式）

use crate::attach::{encode_attach_output_frame, ATTACH_PIPE_STDERR, ATTACH_PIPE_STDOUT};
use anyhow::{Context, Result};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// CRI 日志条目格式
#[derive(Debug, Serialize)]
struct CrictlLogEntry {
    time: String,
    stream: String,
    log: String,
}

impl CrictlLogEntry {
    fn new(stream: &str, data: &[u8]) -> Self {
        Self {
            time: chrono::Utc::now().to_rfc3339(),
            stream: stream.to_string(),
            log: String::from_utf8_lossy(data).to_string(),
        }
    }

    fn to_json_line(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(self)?;
        let mut line = json;
        line.push(b'\n');
        Ok(line)
    }
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
        }
    }
}

/// IO管理器
#[derive(Clone)]
pub struct IoManager {
    config: IoConfig,
    /// 当前attach的客户端
    clients: Arc<Mutex<Vec<ClientConnection>>>,
    /// 日志文件
    log_file: Arc<Mutex<Option<File>>>,
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
            *log_file = Some(File::create(stdout)?);
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
                        Ok(_stream) => {
                            if let Err(e) = io_manager.reopen_log_file() {
                                debug!("Failed to reopen log file on control request: {}", e);
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

        let reopened = File::create(stdout)?;
        let mut log_file = self.log_file.lock().unwrap();
        *log_file = Some(reopened);
        info!("Reopened log file: {:?}", stdout);
        Ok(())
    }

    /// 写入stdout
    pub fn write_stdout(&self, data: &[u8]) -> Result<()> {
        // 写入日志文件（CRI JSON格式）
        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            let log_entry = CrictlLogEntry::new("stdout", data);
            if let Ok(json_line) = log_entry.to_json_line() {
                file.write_all(&json_line)?;
                file.flush()?;
            }
        }

        // 发送到所有attach客户端
        self.broadcast_output(ATTACH_PIPE_STDOUT, data)?;

        Ok(())
    }

    /// 写入stderr
    pub fn write_stderr(&self, data: &[u8]) -> Result<()> {
        // 写入日志文件（CRI JSON格式）
        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            let log_entry = CrictlLogEntry::new("stderr", data);
            if let Ok(json_line) = log_entry.to_json_line() {
                file.write_all(&json_line)?;
                file.flush()?;
            }
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
                    Ok(0) => break,
                    Ok(n) => {
                        if let Err(e) = io_for_output.write_stdout(&buffer[..n]) {
                            error!("Failed to forward console output: {}", e);
                        }
                    }
                    Err(e) => {
                        debug!("Console output pump stopped: {}", e);
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
            let file = File::create(stdout)?;
            Some(file)
        } else {
            None
        };

        // stderr
        let stderr = if let Some(stderr) = &self.config.stderr {
            let parent = stderr.parent().context("Invalid stderr path")?;
            std::fs::create_dir_all(parent)?;
            let file = File::create(stderr)?;
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
        manager.write_stdout(b"before").unwrap();
        manager.reopen_log_file().unwrap();
        manager.write_stdout(b"after").unwrap();

        let content = std::fs::read_to_string(log_path).unwrap();
        assert!(content.contains("after"));
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
