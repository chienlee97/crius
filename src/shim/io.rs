//! IO管理 - 处理容器的stdin/stdout/stderr
//!
//! 功能：
//! 1. 重定向容器IO流到文件或socket
//! 2. 支持attach功能（多客户端连接）
//! 3. 支持TTY模式
//! 4. 日志持久化

use crate::attach::{encode_attach_output_frame, ATTACH_PIPE_STDERR, ATTACH_PIPE_STDOUT};
use anyhow::{Context, Result};
use log::{debug, error, info};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            stdin: None,
            stdout: None,
            stderr: None,
            terminal: false,
            attach_socket: None,
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
}

/// 客户端连接
#[derive(Debug)]
struct ClientConnection {
    id: usize,
    stream: UnixStream,
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

    /// 写入stdout
    pub fn write_stdout(&self, data: &[u8]) -> Result<()> {
        // 写入日志文件
        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            file.write_all(data)?;
            file.flush()?;
        }

        // 发送到所有attach客户端
        self.broadcast_output(ATTACH_PIPE_STDOUT, data)?;

        Ok(())
    }

    /// 写入stderr
    pub fn write_stderr(&self, data: &[u8]) -> Result<()> {
        if let Some(file) = &mut *self.log_file.lock().unwrap() {
            file.write_all(data)?;
            file.flush()?;
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
}
