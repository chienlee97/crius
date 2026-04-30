//! Shim管理器 - 管理crius-shim进程
//!
//! 负责启动、监控和与shim进程通信

use anyhow::{Context, Result};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

const DEFAULT_SHIM_WORK_DIR: &str = "/var/run/crius/shims";
const SHIM_METADATA_FILE: &str = "shim.json";

pub fn default_shim_work_dir() -> PathBuf {
    std::env::var("CRIUS_SHIM_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_SHIM_WORK_DIR))
}

/// Shim配置
#[derive(Debug, Clone)]
pub struct ShimConfig {
    /// Shim二进制路径
    pub shim_path: PathBuf,
    /// Shim工作目录
    pub work_dir: PathBuf,
    /// attach/resize socket 根目录
    pub attach_socket_dir: PathBuf,
    /// 容器退出记录根目录
    pub container_exits_dir: PathBuf,
    /// shim 创建的宿主 IO 工件默认 UID。
    pub io_uid: u32,
    /// shim 创建的宿主 IO 工件默认 GID。
    pub io_gid: u32,
    /// 传给 shim 进程的环境变量列表，格式为 `KEY=value`。
    pub monitor_env: Vec<String>,
    /// 是否启用debug模式
    pub debug: bool,
    /// 是否将容器输出双写到 journald。
    pub log_to_journald: bool,
    /// 是否在日志轮转和容器退出时跳过 sync。
    pub no_sync_log: bool,
    /// 是否禁用 pivot_root，改用 MS_MOVE。
    pub no_pivot: bool,
    /// 运行时路径(runc)
    pub runtime_path: PathBuf,
    /// CRI 单条日志记录切分阈值（字节）。
    pub max_container_log_line_size: usize,
}

impl Default for ShimConfig {
    fn default() -> Self {
        Self {
            shim_path: PathBuf::from("crius-shim"),
            work_dir: default_shim_work_dir(),
            attach_socket_dir: default_shim_work_dir(),
            container_exits_dir: PathBuf::from("/var/run/crius/exits"),
            io_uid: 0,
            io_gid: 0,
            monitor_env: Vec::new(),
            debug: false,
            log_to_journald: false,
            no_sync_log: false,
            no_pivot: false,
            runtime_path: PathBuf::from("runc"),
            max_container_log_line_size: 4096,
        }
    }
}

/// Shim进程信息
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    fn metadata_path(&self, container_id: &str) -> PathBuf {
        self.config
            .work_dir
            .join(container_id)
            .join(SHIM_METADATA_FILE)
    }

    fn process_exists(pid: u32) -> bool {
        PathBuf::from("/proc").join(pid.to_string()).exists()
    }

    fn persist_process_metadata(&self, process: &ShimProcess) -> Result<()> {
        let metadata_path = self.metadata_path(&process.container_id);
        if let Some(parent) = metadata_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&metadata_path, serde_json::to_vec_pretty(process)?)?;
        Ok(())
    }

    fn remove_process_metadata(&self, container_id: &str) -> Result<()> {
        let metadata_path = self.metadata_path(container_id);
        if metadata_path.exists() {
            fs::remove_file(metadata_path)?;
        }
        Ok(())
    }

    fn restore_processes_from_disk(config: &ShimConfig) -> Vec<ShimProcess> {
        let mut restored = Vec::new();
        let Ok(entries) = fs::read_dir(&config.work_dir) else {
            return restored;
        };

        for entry in entries.flatten() {
            let metadata_path = entry.path().join(SHIM_METADATA_FILE);
            if !metadata_path.exists() {
                continue;
            }

            let raw = match fs::read(&metadata_path) {
                Ok(raw) => raw,
                Err(err) => {
                    debug!(
                        "Ignoring unreadable shim metadata {}: {}",
                        metadata_path.display(),
                        err
                    );
                    continue;
                }
            };
            let process: ShimProcess = match serde_json::from_slice(&raw) {
                Ok(process) => process,
                Err(err) => {
                    debug!(
                        "Ignoring invalid shim metadata {}: {}",
                        metadata_path.display(),
                        err
                    );
                    continue;
                }
            };

            if Self::process_exists(process.shim_pid) || process.exit_code_file.exists() {
                restored.push(process);
            } else {
                debug!(
                    "Ignoring stale shim metadata for container {} from {}",
                    process.container_id,
                    metadata_path.display()
                );
            }
        }

        restored
    }

    /// 创建新的ShimManager
    pub fn new(config: ShimConfig) -> Self {
        // 确保工作目录存在
        let _ = fs::create_dir_all(&config.work_dir);
        let restored = Self::restore_processes_from_disk(&config);

        Self {
            config,
            processes: Arc::new(Mutex::new(restored)),
        }
    }

    pub fn attach_socket_path(&self, container_id: &str, socket_name: &str) -> PathBuf {
        self.config
            .attach_socket_dir
            .join(container_id)
            .join(socket_name)
    }

    pub fn shim_socket_path(&self, container_id: &str, socket_name: &str) -> PathBuf {
        self.config.work_dir.join(container_id).join(socket_name)
    }

    /// 启动shim进程来管理容器
    pub fn start_shim(&self, container_id: &str, bundle_path: &Path) -> Result<ShimProcess> {
        info!("Starting shim for container {}", container_id);

        // 创建shim工作目录
        let shim_dir = self.config.work_dir.join(container_id);
        fs::create_dir_all(&shim_dir)?;
        let attach_dir = self.config.attach_socket_dir.join(container_id);
        fs::create_dir_all(&attach_dir)?;
        fs::create_dir_all(&self.config.container_exits_dir)?;

        // 设置文件路径
        let exit_code_file = self.config.container_exits_dir.join(container_id);
        let log_file = shim_dir.join("shim.log");
        let socket_path = attach_dir.join("attach.sock");

        // 构建shim命令
        let mut cmd = Command::new(&self.config.shim_path);
        cmd.arg("--id")
            .arg(container_id)
            .arg("--bundle")
            .arg(bundle_path)
            .arg("--runtime")
            .arg(&self.config.runtime_path)
            .arg("--exit-code-file")
            .arg(&exit_code_file)
            .arg("--attach-socket-dir")
            .arg(&self.config.attach_socket_dir)
            .arg("--io-uid")
            .arg(self.config.io_uid.to_string())
            .arg("--io-gid")
            .arg(self.config.io_gid.to_string());

        if self.config.debug {
            cmd.arg("--debug");
        }
        if self.config.log_to_journald {
            cmd.arg("--log-to-journald");
        }
        if self.config.no_sync_log {
            cmd.arg("--no-sync-log");
        }
        if self.config.no_pivot {
            cmd.arg("--no-pivot");
        }
        for env in &self.config.monitor_env {
            let (key, value) = env
                .split_once('=')
                .with_context(|| format!("invalid monitor env entry {env}"))?;
            cmd.env(key, value);
        }

        cmd.arg("--log").arg(&log_file);
        cmd.arg("--max-container-log-line-size")
            .arg(self.config.max_container_log_line_size.to_string());

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
        processes.retain(|existing| existing.container_id != container_id);
        processes.push(process.clone());
        drop(processes);
        self.persist_process_metadata(&process)?;

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
            let _ = process
                .socket_path
                .parent()
                .map(fs::remove_dir_all)
                .transpose();
            let _ = self.remove_process_metadata(container_id);

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
                Self::process_exists(process.shim_pid)
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
                    Self::process_exists(process.shim_pid)
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
                let _ = process
                    .socket_path
                    .parent()
                    .map(fs::remove_dir_all)
                    .transpose();
                let _ = self.remove_process_metadata(&process.container_id);
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
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_shim_config_default() {
        let config = ShimConfig::default();
        assert_eq!(config.shim_path, PathBuf::from("crius-shim"));
        assert_eq!(config.work_dir, default_shim_work_dir());
        assert_eq!(config.attach_socket_dir, default_shim_work_dir());
        assert_eq!(
            config.container_exits_dir,
            PathBuf::from("/var/run/crius/exits")
        );
        assert!(config.monitor_env.is_empty());
        assert!(!config.debug);
        assert!(!config.no_sync_log);
        assert!(!config.no_pivot);
    }

    #[test]
    fn test_shim_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let config = ShimConfig {
            work_dir: temp_dir.path().to_path_buf(),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            ..Default::default()
        };

        let manager = ShimManager::new(config);
        let shims = manager.list_shims();
        assert!(shims.is_empty());
    }

    #[test]
    fn test_shim_manager_restores_live_metadata_from_disk() {
        let temp_dir = tempdir().unwrap();
        let work_dir = temp_dir.path().join("shims");
        let container_dir = work_dir.join("container-1");
        let exits_dir = temp_dir.path().join("exits");
        fs::create_dir_all(&container_dir).unwrap();
        fs::create_dir_all(&exits_dir).unwrap();

        let process = ShimProcess {
            container_id: "container-1".to_string(),
            shim_pid: std::process::id(),
            exit_code_file: exits_dir.join("container-1"),
            log_file: container_dir.join("shim.log"),
            socket_path: container_dir.join("attach.sock"),
            bundle_path: temp_dir.path().join("bundle"),
        };
        fs::write(
            container_dir.join(SHIM_METADATA_FILE),
            serde_json::to_vec_pretty(&process).unwrap(),
        )
        .unwrap();

        let manager = ShimManager::new(ShimConfig {
            work_dir,
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: exits_dir,
            ..Default::default()
        });
        let shims = manager.list_shims();
        assert_eq!(shims.len(), 1);
        assert_eq!(shims[0].container_id, "container-1");
    }

    #[test]
    fn test_shim_manager_ignores_stale_metadata_from_disk() {
        let temp_dir = tempdir().unwrap();
        let work_dir = temp_dir.path().join("shims");
        let container_dir = work_dir.join("stale-container");
        let exits_dir = temp_dir.path().join("exits");
        fs::create_dir_all(&container_dir).unwrap();
        fs::create_dir_all(&exits_dir).unwrap();

        let process = ShimProcess {
            container_id: "stale-container".to_string(),
            shim_pid: 999_999,
            exit_code_file: exits_dir.join("stale-container"),
            log_file: container_dir.join("shim.log"),
            socket_path: container_dir.join("attach.sock"),
            bundle_path: temp_dir.path().join("bundle"),
        };
        fs::write(
            container_dir.join(SHIM_METADATA_FILE),
            serde_json::to_vec_pretty(&process).unwrap(),
        )
        .unwrap();

        let manager = ShimManager::new(ShimConfig {
            work_dir,
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: exits_dir,
            ..Default::default()
        });
        assert!(manager.list_shims().is_empty());
    }

    #[test]
    fn test_start_shim_uses_configured_container_exits_dir() {
        let temp_dir = tempdir().unwrap();
        let shim_path = temp_dir.path().join("fake-shim.sh");
        fs::write(
            &shim_path,
            r#"#!/bin/sh
set -eu
sleep 1
"#,
        )
        .unwrap();
        let mut perms = fs::metadata(&shim_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&shim_path, perms).unwrap();

        let config = ShimConfig {
            shim_path,
            work_dir: temp_dir.path().join("shims"),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            runtime_path: PathBuf::from("/bin/false"),
            ..Default::default()
        };
        let manager = ShimManager::new(config.clone());
        let bundle = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle).unwrap();

        let process = manager.start_shim("container-1", &bundle).unwrap();
        assert_eq!(
            process.exit_code_file,
            config.container_exits_dir.join("container-1")
        );

        manager.stop_shim("container-1").unwrap();
    }

    #[test]
    fn test_start_shim_passes_no_pivot_when_enabled() {
        let temp_dir = tempdir().unwrap();
        let shim_path = temp_dir.path().join("fake-shim.sh");
        let args_path = temp_dir.path().join("shim.args");
        fs::write(
            &shim_path,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
sleep 1
"#,
                args_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&shim_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&shim_path, perms).unwrap();

        let manager = ShimManager::new(ShimConfig {
            shim_path,
            work_dir: temp_dir.path().join("shims"),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            no_pivot: true,
            runtime_path: PathBuf::from("/bin/false"),
            ..Default::default()
        });
        let bundle = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle).unwrap();

        manager.start_shim("container-1", &bundle).unwrap();
        for _ in 0..50 {
            if args_path.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--no-pivot"));

        manager.stop_shim("container-1").unwrap();
    }

    #[test]
    fn test_start_shim_passes_configured_io_owner_flags() {
        let temp_dir = tempdir().unwrap();
        let shim_path = temp_dir.path().join("fake-shim.sh");
        let args_path = temp_dir.path().join("shim.args");
        fs::write(
            &shim_path,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
sleep 1
"#,
                args_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&shim_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&shim_path, perms).unwrap();

        let manager = ShimManager::new(ShimConfig {
            shim_path,
            work_dir: temp_dir.path().join("shims"),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            io_uid: 1234,
            io_gid: 2345,
            runtime_path: PathBuf::from("/bin/false"),
            ..Default::default()
        });
        let bundle = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle).unwrap();

        manager.start_shim("container-1", &bundle).unwrap();
        for _ in 0..50 {
            if args_path.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--io-uid"));
        assert!(args.lines().any(|line| line == "1234"));
        assert!(args.lines().any(|line| line == "--io-gid"));
        assert!(args.lines().any(|line| line == "2345"));

        manager.stop_shim("container-1").unwrap();
    }

    #[test]
    fn test_start_shim_passes_no_sync_log_flag() {
        let temp_dir = tempdir().unwrap();
        let shim_path = temp_dir.path().join("fake-shim.sh");
        let args_path = temp_dir.path().join("shim.args");
        fs::write(
            &shim_path,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
sleep 1
"#,
                args_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&shim_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&shim_path, perms).unwrap();

        let manager = ShimManager::new(ShimConfig {
            shim_path,
            work_dir: temp_dir.path().join("shims"),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            no_sync_log: true,
            runtime_path: PathBuf::from("/bin/false"),
            ..Default::default()
        });
        let bundle = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle).unwrap();

        manager.start_shim("container-1", &bundle).unwrap();
        for _ in 0..50 {
            if args_path.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--no-sync-log"));

        manager.stop_shim("container-1").unwrap();
    }

    #[test]
    fn test_start_shim_injects_configured_monitor_env() {
        let temp_dir = tempdir().unwrap();
        let shim_path = temp_dir.path().join("fake-shim.sh");
        let env_out = temp_dir.path().join("shim.env");
        fs::write(
            &shim_path,
            r#"#!/bin/sh
set -eu
printf '%s\n' "${TEST_MONITOR_VALUE:-}" > "${TEST_MONITOR_ENV_PATH:?}"
sleep 1
"#,
        )
        .unwrap();
        let mut perms = fs::metadata(&shim_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&shim_path, perms).unwrap();

        let manager = ShimManager::new(ShimConfig {
            shim_path,
            work_dir: temp_dir.path().join("shims"),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            monitor_env: vec![
                format!("TEST_MONITOR_ENV_PATH={}", env_out.display()),
                "TEST_MONITOR_VALUE=hello-from-monitor-env".to_string(),
            ],
            runtime_path: PathBuf::from("/bin/false"),
            ..Default::default()
        });
        let bundle = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle).unwrap();

        manager.start_shim("container-1", &bundle).unwrap();
        for _ in 0..50 {
            if env_out.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(
            fs::read_to_string(&env_out).unwrap().trim(),
            "hello-from-monitor-env"
        );

        manager.stop_shim("container-1").unwrap();
    }
}
