//! Shim管理器 - 管理crius-shim进程
//!
//! 负责启动、监控和与shim进程通信

use anyhow::{Context, Result};
use log::{debug, info};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

/// Shim配置
#[derive(Debug, Clone)]
pub struct ShimConfig {
    /// Shim二进制路径
    pub shim_path: PathBuf,
    /// Shim工作目录
    pub work_dir: PathBuf,
    /// 是否启用debug模式
    pub debug: bool,
    /// 运行时路径(runc)
    pub runtime_path: PathBuf,
}

impl Default for ShimConfig {
    fn default() -> Self {
        Self {
            shim_path: PathBuf::from("crius-shim"),
            work_dir: PathBuf::from("/var/run/crius/shims"),
            debug: false,
            runtime_path: PathBuf::from("runc"),
        }
    }
}

/// Shim进程信息
#[derive(Debug, Clone)]
pub struct ShimProcess {
    /// 容器ID
    pub container_id: String,
    /// Shim进程ID
    pub shim_pid: u32,
    /// 退出码文件路径
    pub exit_code_file: PathBuf,
    /// 日志文件路径
    pub log_file: PathBuf,
    /// Unix socket路径（用于attach）
    pub socket_path: PathBuf,
    /// Bundle目录
    pub bundle_path: PathBuf,
}

/// Shim管理器
#[derive(Debug)]
pub struct ShimManager {
    config: ShimConfig,
    /// 正在运行的shim进程
    processes: Arc<Mutex<Vec<ShimProcess>>>,
}

impl ShimManager {
    /// 创建新的ShimManager
    pub fn new(config: ShimConfig) -> Self {
        // 确保工作目录存在
        let _ = fs::create_dir_all(&config.work_dir);

        Self {
            config,
            processes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 启动shim进程来管理容器
    pub fn start_shim(&self, container_id: &str, bundle_path: &Path) -> Result<ShimProcess> {
        info!("Starting shim for container {}", container_id);

        // 创建shim工作目录
        let shim_dir = self.config.work_dir.join(container_id);
        fs::create_dir_all(&shim_dir)?;

        // 设置文件路径
        let exit_code_file = shim_dir.join("exit_code");
        let log_file = shim_dir.join("shim.log");
        let socket_path = shim_dir.join("attach.sock");

        // 构建shim命令
        let mut cmd = Command::new(&self.config.shim_path);
        cmd.arg("--id")
            .arg(container_id)
            .arg("--bundle")
            .arg(bundle_path)
            .arg("--runtime")
            .arg(&self.config.runtime_path)
            .arg("--exit-code-file")
            .arg(&exit_code_file);

        if self.config.debug {
            cmd.arg("--debug");
        }

        cmd.arg("--log").arg(&log_file);

        // 启动shim进程
        debug!("Executing: {:?}", cmd);

        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("Failed to start shim process")?;

        let shim_pid = child.id();

        // 在后台等待shim进程（避免僵尸进程）
        std::thread::spawn(move || {
            let _ = child.wait();
        });

        info!(
            "Shim started for container {} with PID {}",
            container_id, shim_pid
        );

        let process = ShimProcess {
            container_id: container_id.to_string(),
            shim_pid,
            exit_code_file,
            log_file,
            socket_path,
            bundle_path: bundle_path.to_path_buf(),
        };

        // 添加到进程列表
        let mut processes = self.processes.lock().unwrap();
        processes.push(process.clone());

        Ok(process)
    }

    /// 获取容器的退出码
    pub fn get_exit_code(&self, container_id: &str) -> Result<Option<i32>> {
        let processes = self.processes.lock().unwrap();

        if let Some(process) = processes.iter().find(|p| p.container_id == container_id) {
            if process.exit_code_file.exists() {
                let content = fs::read_to_string(&process.exit_code_file)?;
                let exit_code = content
                    .trim()
                    .parse::<i32>()
                    .context("Failed to parse exit code")?;
                return Ok(Some(exit_code));
            }
        }

        Ok(None)
    }

    /// 停止shim进程
    pub fn stop_shim(&self, container_id: &str) -> Result<()> {
        info!("Stopping shim for container {}", container_id);

        let mut processes = self.processes.lock().unwrap();

        if let Some(index) = processes
            .iter()
            .position(|p| p.container_id == container_id)
        {
            let process = processes.remove(index);

            // 发送SIGTERM给shim进程
            #[cfg(unix)]
            {
                use nix::sys::signal::{self, Signal};
                use nix::unistd::Pid;

                let pid = Pid::from_raw(process.shim_pid as i32);
                let _ = signal::kill(pid, Signal::SIGTERM);
            }

            // 清理socket文件
            let _ = fs::remove_file(&process.socket_path);

            info!("Shim for container {} stopped", container_id);
        }

        Ok(())
    }

    /// 获取所有活跃的shim进程
    pub fn list_shims(&self) -> Vec<ShimProcess> {
        let processes = self.processes.lock().unwrap();
        processes.clone()
    }

    /// 检查shim是否还在运行
    pub fn is_shim_running(&self, container_id: &str) -> bool {
        let processes = self.processes.lock().unwrap();

        if let Some(process) = processes.iter().find(|p| p.container_id == container_id) {
            // 检查进程是否存在
            #[cfg(unix)]
            {
                use nix::sys::signal::{self, Signal};
                use nix::unistd::Pid;

                let pid = Pid::from_raw(process.shim_pid as i32);
                // 发送信号0检查进程是否存在
                signal::kill(pid, Signal::SIGCHLD).is_ok()
            }
            #[cfg(not(unix))]
            {
                false
            }
        } else {
            false
        }
    }

    /// 清理已退出的shim进程
    pub fn cleanup_exited_shims(&self) -> Result<usize> {
        let mut processes = self.processes.lock().unwrap();
        let initial_count = processes.len();

        processes.retain(|process| {
            // 检查进程是否还在运行
            let is_running = {
                #[cfg(unix)]
                {
                    use nix::sys::signal::{self, Signal};
                    use nix::unistd::Pid;

                    let pid = Pid::from_raw(process.shim_pid as i32);
                    signal::kill(pid, Signal::SIGCHLD).is_ok()
                }
                #[cfg(not(unix))]
                {
                    false
                }
            };

            if !is_running {
                debug!(
                    "Cleaning up exited shim for container {}",
                    process.container_id
                );
                // 清理socket文件
                let _ = fs::remove_file(&process.socket_path);
            }

            is_running
        });

        let cleaned = initial_count - processes.len();
        if cleaned > 0 {
            info!("Cleaned up {} exited shim processes", cleaned);
        }

        Ok(cleaned)
    }

    /// 读取shim日志
    pub fn read_shim_log(&self, container_id: &str) -> Result<String> {
        let processes = self.processes.lock().unwrap();

        if let Some(process) = processes.iter().find(|p| p.container_id == container_id) {
            if process.log_file.exists() {
                let content = fs::read_to_string(&process.log_file)?;
                return Ok(content);
            }
        }

        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_shim_config_default() {
        let config = ShimConfig::default();
        assert_eq!(config.shim_path, PathBuf::from("crius-shim"));
        assert!(!config.debug);
    }

    #[test]
    fn test_shim_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let config = ShimConfig {
            work_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let manager = ShimManager::new(config);
        let shims = manager.list_shims();
        assert!(shims.is_empty());
    }
}
