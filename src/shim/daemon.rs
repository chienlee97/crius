//! Shim守护进程实现
//!
//! 守护进程负责：
//! 1. 设置子进程收割（PR_SET_CHILD_SUBREAPER）
//! 2. 创建容器进程（通过runc create）
//! 3. 监控容器进程生命周期
//! 4. 记录容器退出码
//! 5. 管理IO流

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use nix::cmsg_space;
use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::IoSliceMut;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::net::UnixListener;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::io::{IoConfig, IoManager, JournalConfig, DEFAULT_JOURNALD_SOCKET_PATH};

const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";

#[derive(Debug, Deserialize, Default)]
struct ShimBundleProcess {
    terminal: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct ShimBundleConfig {
    process: Option<ShimBundleProcess>,
    linux: Option<ShimBundleLinux>,
    annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize, Default)]
struct ShimBundleLinux {
    #[serde(rename = "cgroupsPath")]
    cgroups_path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ShimStoredContainerState {
    log_path: Option<String>,
    metadata_name: Option<String>,
    tty: bool,
    stdin: bool,
    stdin_once: bool,
}

/// Shim守护进程
pub struct Daemon {
    /// 容器ID
    container_id: String,
    /// Bundle目录
    bundle: PathBuf,
    /// Runtime路径
    runtime: PathBuf,
    /// OCI runtime 特定配置文件路径。
    runtime_config_path: PathBuf,
    /// monitor/shim 所在 cgroup。
    monitor_cgroup: String,
    /// 退出码文件路径
    exit_code_file: Option<PathBuf>,
    /// attach/resize socket 根目录
    attach_socket_dir: Option<PathBuf>,
    /// shim 创建的宿主 IO 工件默认 UID。
    io_uid: u32,
    /// shim 创建的宿主 IO 工件默认 GID。
    io_gid: u32,
    /// CRI 单条日志记录切分阈值（字节）。
    max_container_log_line_size: usize,
    /// 是否额外写 journald。
    log_to_journald: bool,
    /// 是否在日志轮转和容器退出时跳过 sync。
    no_sync_log: bool,
    /// 是否禁用 pivot_root，改用 MS_MOVE。
    no_pivot: bool,
    /// 是否禁止创建新的 session keyring。
    no_new_keyring: bool,
    /// runtime 是否启用 systemd cgroup。
    systemd_cgroup: bool,
    /// IO管理器
    io_manager: IoManager,
    /// 是否正在运行
    running: Arc<AtomicBool>,
}

pub struct DaemonOptions {
    pub runtime_config_path: PathBuf,
    pub monitor_cgroup: String,
    pub exit_code_file: Option<PathBuf>,
    pub attach_socket_dir: Option<PathBuf>,
    pub io_uid: u32,
    pub io_gid: u32,
    pub max_container_log_line_size: usize,
    pub log_to_journald: bool,
    pub no_sync_log: bool,
    pub no_pivot: bool,
    pub no_new_keyring: bool,
    pub systemd_cgroup: bool,
}

impl Daemon {
    /// 创建新的守护进程
    pub fn new(
        container_id: String,
        bundle: PathBuf,
        runtime: PathBuf,
        options: DaemonOptions,
    ) -> Self {
        let DaemonOptions {
            runtime_config_path,
            monitor_cgroup,
            exit_code_file,
            attach_socket_dir,
            io_uid,
            io_gid,
            max_container_log_line_size,
            log_to_journald,
            no_sync_log,
            no_pivot,
            no_new_keyring,
            systemd_cgroup,
        } = options;
        Self {
            container_id,
            bundle,
            runtime,
            runtime_config_path,
            monitor_cgroup,
            exit_code_file,
            attach_socket_dir,
            io_uid,
            io_gid,
            max_container_log_line_size,
            log_to_journald,
            no_sync_log,
            no_pivot,
            no_new_keyring,
            systemd_cgroup,
            io_manager: IoManager::new(),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// 运行守护进程
    pub fn run(mut self) -> Result<()> {
        // 1. 设置子进程收割者
        self.setup_subreaper()?;

        // 2. 设置信号处理器
        self.setup_signal_handlers()?;

        // 3. 初始化IO
        self.setup_io()?;

        // 4. 把 shim/monitor 进程放到目标 cgroup。
        self.configure_monitor_cgroup()?;

        // 5. 创建并运行容器
        let result = if self.is_terminal()? {
            let container_pid = self.create_terminal_container()?;
            info!("Container created with PID: {}", container_pid);
            self.monitor_container(container_pid)
        } else {
            self.run_non_terminal_container()
        };
        if let Err(err) = self.io_manager.shutdown() {
            warn!("Failed to shutdown IO manager cleanly: {}", err);
        }
        let exit_code = result?;

        // 6. 记录退出码
        self.record_exit_code(exit_code)?;

        info!(
            "Shim daemon exiting with container exit code: {}",
            exit_code
        );
        std::process::exit(exit_code);
    }

    /// 设置子进程收割者
    fn setup_subreaper(&self) -> Result<()> {
        // 使用libc直接调用prctl设置子进程收割者
        // PR_SET_CHILD_SUBREAPER = 36
        const PR_SET_CHILD_SUBREAPER: i32 = 36;
        let result = unsafe { libc::prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };

        if result != 0 {
            return Err(anyhow::anyhow!(
                "Failed to set subreaper: {}",
                std::io::Error::last_os_error()
            ));
        }

        info!("Set as child subreaper");
        Ok(())
    }

    /// 设置信号处理器
    fn setup_signal_handlers(&self) -> Result<()> {
        // 处理SIGCHLD信号
        let running = self.running.clone();

        ctrlc::set_handler(move || {
            info!("Received SIGINT/SIGTERM, shutting down...");
            running.store(false, Ordering::SeqCst);
        })
        .context("Failed to set signal handler")?;

        Ok(())
    }

    fn shim_dir(&self) -> PathBuf {
        self.attach_socket_dir
            .as_ref()
            .map(|root| root.join(&self.container_id))
            .or_else(|| {
                self.exit_code_file
                    .as_ref()
                    .and_then(|path| path.parent().map(PathBuf::from))
            })
            .unwrap_or_else(|| PathBuf::from("/var/run/crius/shims").join(&self.container_id))
    }

    fn attach_socket_container_dir(&self) -> PathBuf {
        match self.attach_socket_dir.as_ref() {
            Some(root) => root.join(&self.container_id),
            None => self.shim_dir(),
        }
    }

    fn cleanup_attach_socket_directory(&self) {
        let Some(_root) = self.attach_socket_dir.as_ref() else {
            return;
        };
        let path = self.attach_socket_container_dir();
        if let Err(err) = fs::remove_dir_all(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "Failed to remove attach socket directory {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    fn load_bundle_config(&self) -> Result<ShimBundleConfig> {
        let config_path = self.bundle.join("config.json");
        let content = fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read bundle config {:?}", config_path))?;
        let config = serde_json::from_str(&content).context("Failed to parse bundle config")?;
        Ok(config)
    }

    fn runtime_command(&self) -> Command {
        let mut cmd = Command::new(&self.runtime);
        if self.systemd_cgroup {
            cmd.arg("--systemd-cgroup");
        }
        if !self.runtime_config_path.as_os_str().is_empty() {
            cmd.arg("--config").arg(&self.runtime_config_path);
        }
        cmd.env("XDG_RUNTIME_DIR", "/run/user/0");
        cmd
    }

    fn runtime_command_output(&self, args: &[&str]) -> Result<Output> {
        self.runtime_command()
            .args(args)
            .output()
            .with_context(|| format!("Failed to execute runtime {}", self.runtime.display()))
    }

    fn configure_monitor_cgroup(&self) -> Result<()> {
        let target = self.monitor_cgroup.trim();
        if target.is_empty() {
            return Ok(());
        }

        let bundle_config = self.load_bundle_config()?;
        let cgroup_target = if target == "pod" {
            bundle_config
                .linux
                .as_ref()
                .and_then(|linux| linux.cgroups_path.as_ref())
                .map(|path| path.trim())
                .filter(|path| !path.is_empty())
                .ok_or_else(|| anyhow::anyhow!("bundle config is missing linux.cgroupsPath"))?
                .to_string()
        } else {
            target.to_string()
        };

        move_pid_to_cgroup(std::process::id(), &cgroup_target).with_context(|| {
            format!(
                "failed to move shim {} into monitor cgroup {}",
                self.container_id, cgroup_target
            )
        })
    }

    fn load_container_state(&self, config: &ShimBundleConfig) -> Option<ShimStoredContainerState> {
        config
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(INTERNAL_CONTAINER_STATE_KEY))
            .and_then(|raw| serde_json::from_str(raw).ok())
    }

    fn is_terminal(&self) -> Result<bool> {
        let bundle_config = self.load_bundle_config()?;
        let container_state = self
            .load_container_state(&bundle_config)
            .unwrap_or_default();
        Ok(bundle_config
            .process
            .as_ref()
            .and_then(|process| process.terminal)
            .unwrap_or(container_state.tty))
    }

    fn receive_console_fd(listener: &UnixListener) -> Result<RawFd> {
        let (stream, _) = listener
            .accept()
            .context("Failed to accept console socket")?;
        let mut buf = [0u8; 1];
        let mut iov = [IoSliceMut::new(&mut buf)];
        let mut cmsg_buffer = cmsg_space!([RawFd; 1]);
        let message = recvmsg::<()>(
            stream.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_buffer),
            MsgFlags::empty(),
        )
        .context("Failed to receive console fd")?;

        for cmsg in message.cmsgs() {
            if let ControlMessageOwned::ScmRights(fds) = cmsg {
                if let Some(fd) = fds.first() {
                    return Ok(*fd);
                }
            }
        }

        Err(anyhow::anyhow!("No console fd received from runc"))
    }

    /// 设置IO
    fn setup_io(&mut self) -> Result<()> {
        let bundle_config = self.load_bundle_config()?;
        let container_state = self
            .load_container_state(&bundle_config)
            .unwrap_or_default();
        let io_config = IoConfig {
            stdin: None,
            stdout: container_state.log_path.as_ref().map(PathBuf::from),
            stderr: None,
            terminal: bundle_config
                .process
                .as_ref()
                .and_then(|process| process.terminal)
                .unwrap_or(container_state.tty),
            attach_socket: Some(self.attach_socket_container_dir().join("attach.sock")),
            resize_socket: bundle_config
                .process
                .as_ref()
                .and_then(|process| process.terminal)
                .unwrap_or(container_state.tty)
                .then(|| self.attach_socket_container_dir().join("resize.sock")),
            reopen_socket: container_state
                .log_path
                .as_ref()
                .map(|_| self.shim_dir().join("reopen.sock")),
            journald: self.log_to_journald.then(|| JournalConfig {
                socket_path: PathBuf::from(DEFAULT_JOURNALD_SOCKET_PATH),
                container_id: self.container_id.clone(),
                container_name: container_state.metadata_name.clone(),
                syslog_identifier: "crius-shim".to_string(),
            }),
            no_sync_log: self.no_sync_log,
            io_uid: self.io_uid,
            io_gid: self.io_gid,
            max_log_line_size: self.max_container_log_line_size,
        };
        self.io_manager.configure(io_config)?;
        self.io_manager.start_attach_server()?;
        self.io_manager.start_resize_server()?;
        self.io_manager.start_reopen_log_server()?;
        info!("IO setup complete");
        Ok(())
    }

    /// 创建TTY容器
    fn create_terminal_container(&self) -> Result<Pid> {
        // 首先检查容器是否已经存在
        let state_output = self.runtime_command_output(&["state", &self.container_id])?;

        if state_output.status.success() {
            // 容器已存在，获取其PID
            let state: serde_json::Value = serde_json::from_slice(&state_output.stdout)?;
            if let Some(pid) = state.get("pid").and_then(|p| p.as_i64()) {
                return Ok(Pid::from_raw(pid as i32));
            }
        }

        // 创建新容器
        info!("Creating container: {}", self.container_id);

        let tty = self.is_terminal()?;
        let console_socket_path = self.shim_dir().join("console.sock");
        let console_listener = if tty {
            let _ = fs::remove_file(&console_socket_path);
            Some(
                UnixListener::bind(&console_socket_path)
                    .context("Failed to bind console socket")?,
            )
        } else {
            None
        };

        let mut create_args = vec!["create", "--bundle", self.bundle.to_str().unwrap()];
        if self.no_pivot {
            create_args.push("--no-pivot");
        }
        if self.no_new_keyring {
            create_args.push("--no-new-keyring");
        }
        if tty {
            create_args.push("--console-socket");
            create_args.push(console_socket_path.to_str().unwrap());
        }
        create_args.push(&self.container_id);

        let output = self
            .runtime_command()
            .args(&create_args)
            .output()
            .context("Failed to execute runc create")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("runc create failed: {}", stderr);
            return Err(anyhow::anyhow!("Failed to create container: {}", stderr));
        }

        if let Some(listener) = console_listener {
            let console_fd = Self::receive_console_fd(&listener)?;
            let console = unsafe { File::from_raw_fd(console_fd) };
            self.io_manager.start_console_bridge(console)?;
            let _ = fs::remove_file(&console_socket_path);
        }

        // 获取容器PID
        let state_output = self.runtime_command_output(&["state", &self.container_id])?;

        if !state_output.status.success() {
            return Err(anyhow::anyhow!(
                "Failed to get container state after creation"
            ));
        }

        let state: serde_json::Value = serde_json::from_slice(&state_output.stdout)?;
        let pid = state
            .get("pid")
            .and_then(|p| p.as_i64())
            .context("Failed to parse container PID from state")?;

        // 启动容器
        let output = self
            .runtime_command()
            .args(["start", &self.container_id])
            .output()
            .context("Failed to execute runc start")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("runc start warning: {}", stderr);
            // 继续执行，因为某些情况下容器可能已经启动
        }

        Ok(Pid::from_raw(pid as i32))
    }

    fn run_non_terminal_container(&self) -> Result<i32> {
        let bundle_config = self.load_bundle_config()?;
        let container_state = self
            .load_container_state(&bundle_config)
            .unwrap_or_default();

        let mut cmd = self.runtime_command();
        let mut run_args = vec!["run", "--bundle", self.bundle.to_str().unwrap()];
        if self.no_pivot {
            run_args.push("--no-pivot");
        }
        if self.no_new_keyring {
            run_args.push("--no-new-keyring");
        }
        run_args.push(&self.container_id);
        cmd.args(&run_args);

        if container_state.stdin {
            cmd.stdin(std::process::Stdio::piped());
        } else {
            cmd.stdin(std::process::Stdio::null());
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().context("Failed to execute runc run")?;
        info!(
            "Container {} started via foreground runc run (stdin={}, stdin_once={})",
            self.container_id, container_state.stdin, container_state.stdin_once
        );

        let stdout_handle = if let Some(stdout) = child.stdout.take() {
            let io_manager = self.io_manager.clone();
            Some(std::thread::spawn(move || {
                let mut stdout = stdout;
                let mut buffer = [0u8; 8192];
                loop {
                    match std::io::Read::read(&mut stdout, &mut buffer) {
                        Ok(0) => {
                            let _ = io_manager.finish_stdout();
                            break;
                        }
                        Ok(n) => {
                            if let Err(e) = io_manager.write_stdout(&buffer[..n]) {
                                debug!("stdout pump stopped: {}", e);
                                let _ = io_manager.finish_stdout();
                                break;
                            }
                        }
                        Err(e) => {
                            debug!("stdout pump stopped: {}", e);
                            let _ = io_manager.finish_stdout();
                            break;
                        }
                    }
                }
            }))
        } else {
            None
        };

        let stderr_handle = if let Some(stderr) = child.stderr.take() {
            let io_manager = self.io_manager.clone();
            Some(std::thread::spawn(move || {
                let mut stderr = stderr;
                let mut buffer = [0u8; 8192];
                loop {
                    match std::io::Read::read(&mut stderr, &mut buffer) {
                        Ok(0) => {
                            let _ = io_manager.finish_stderr();
                            break;
                        }
                        Ok(n) => {
                            if let Err(e) = io_manager.write_stderr(&buffer[..n]) {
                                debug!("stderr pump stopped: {}", e);
                                let _ = io_manager.finish_stderr();
                                break;
                            }
                        }
                        Err(e) => {
                            debug!("stderr pump stopped: {}", e);
                            let _ = io_manager.finish_stderr();
                            break;
                        }
                    }
                }
            }))
        } else {
            None
        };

        if let Some(mut stdin) = child.stdin.take() {
            let io_manager = self.io_manager.clone();
            std::thread::spawn(move || loop {
                match io_manager.read_stdin() {
                    Ok(data) if !data.is_empty() => {
                        if let Err(e) = std::io::Write::write_all(&mut stdin, &data) {
                            debug!("stdin pump stopped: {}", e);
                            break;
                        }
                        let _ = std::io::Write::flush(&mut stdin);
                    }
                    Ok(_) => std::thread::sleep(std::time::Duration::from_millis(25)),
                    Err(e) => {
                        debug!("stdin pump stopped: {}", e);
                        break;
                    }
                }
            });
        }

        let shutdown_grace = std::time::Duration::from_secs(5);
        let mut shutdown_deadline: Option<std::time::Instant> = None;
        let status = loop {
            match child.try_wait().context("Failed to poll runc run status")? {
                Some(status) => break status,
                None => {
                    if !self.running.load(Ordering::SeqCst) {
                        let deadline = shutdown_deadline
                            .get_or_insert_with(|| std::time::Instant::now() + shutdown_grace);
                        if std::time::Instant::now() >= *deadline {
                            warn!(
                                "Shim shutdown for {} timed out waiting for runc run; force killing child {}",
                                self.container_id,
                                child.id()
                            );
                            let _ = child.kill();
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        };
        let exit_code = match (status.code(), status.signal()) {
            (Some(code), _) => code,
            (None, Some(signal)) => 128 + signal,
            _ => 1,
        };

        if let Some(handle) = stdout_handle {
            let _ = handle.join();
        }
        if let Some(handle) = stderr_handle {
            let _ = handle.join();
        }

        self.cleanup_container()?;
        self.io_manager.shutdown()?;
        self.cleanup_attach_socket_directory();
        Ok(exit_code)
    }

    /// 监控容器进程
    fn monitor_container(&self, container_pid: Pid) -> Result<i32> {
        info!("Monitoring container process: {}", container_pid);

        let mut exit_code = 0;

        while self.running.load(Ordering::SeqCst) {
            // 等待子进程状态变化
            match waitpid(Some(container_pid), Some(WaitPidFlag::WNOHANG)) {
                Ok(WaitStatus::Exited(_pid, code)) => {
                    info!("Container exited with code: {}", code);
                    exit_code = code;
                    break;
                }
                Ok(WaitStatus::Signaled(_pid, signal, _)) => {
                    info!("Container killed by signal: {:?}", signal);
                    exit_code = 128 + signal as i32; // 标准shell约定
                    break;
                }
                Ok(_) => {
                    // 仍在运行或其他状态
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Err(e) => {
                    error!("Error waiting for container: {}", e);
                    break;
                }
            }
        }

        // 清理容器状态
        self.cleanup_container()?;
        self.io_manager.shutdown()?;
        self.cleanup_attach_socket_directory();

        Ok(exit_code)
    }

    /// 清理容器
    fn cleanup_container(&self) -> Result<()> {
        info!("Cleaning up container: {}", self.container_id);

        // 尝试删除容器
        let output = self.runtime_command_output(&["delete", &self.container_id])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Container cleanup warning: {}", stderr);
        }

        Ok(())
    }

    /// 记录退出码
    fn record_exit_code(&self, exit_code: i32) -> Result<()> {
        if let Some(path) = &self.exit_code_file {
            let parent = path.parent().context("Invalid exit code file path")?;
            fs::create_dir_all(parent)?;

            fs::write(path, exit_code.to_string()).context("Failed to write exit code file")?;

            info!("Recorded exit code {} to {:?}", exit_code, path);
        }

        Ok(())
    }
}

fn move_pid_to_cgroup(pid: u32, target: &str) -> Result<()> {
    let mount_point = Path::new("/sys/fs/cgroup");
    let relative = target
        .trim()
        .trim_start_matches('/')
        .trim_start_matches("./");
    if relative.is_empty() {
        return Ok(());
    }

    if mount_point.join("cgroup.controllers").exists() {
        let procs_file = mount_point.join(relative).join("cgroup.procs");
        if let Some(parent) = procs_file.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create cgroup directory {}", parent.display())
            })?;
        }
        fs::write(&procs_file, pid.to_string())
            .with_context(|| format!("Failed to write {}", procs_file.display()))?;
        return Ok(());
    }

    for subsystem in ["cpu", "memory", "pids"] {
        let procs_file = mount_point
            .join(subsystem)
            .join(relative)
            .join("cgroup.procs");
        if let Some(parent) = procs_file.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create cgroup directory {}", parent.display())
            })?;
        }
        fs::write(&procs_file, pid.to_string())
            .with_context(|| format!("Failed to write {}", procs_file.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
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
    fn test_non_terminal_container_stdio_capture() {
        let temp_dir = tempdir().unwrap();
        let bundle_dir = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle_dir).unwrap();

        let log_path = temp_dir.path().join("logs").join("container.log");
        let internal_state = json!({
            "log_path": log_path.to_string_lossy(),
            "tty": false,
            "stdin": false,
            "stdin_once": false,
        });
        let config = json!({
            "process": {
                "terminal": false
            },
            "annotations": {
                INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
            }
        });
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();

        let runtime_path = temp_dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            r#"#!/bin/sh
set -eu

cmd="$1"
shift || true

case "$cmd" in
  run)
    echo "stdout:hello"
    echo "stderr:world" >&2
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
        )
        .unwrap();
        fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

        let exit_code_file = temp_dir.path().join("shim").join("exit_code");
        fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
        let mut daemon = Daemon::new(
            "test-container".to_string(),
            bundle_dir,
            runtime_path,
            DaemonOptions {
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
                exit_code_file: Some(exit_code_file),
                attach_socket_dir: None,
                io_uid: 0,
                io_gid: 0,
                max_container_log_line_size: 4096,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: false,
                no_new_keyring: false,
            },
        );

        daemon
            .io_manager
            .configure(IoConfig {
                stdout: Some(log_path.clone()),
                terminal: false,
                ..Default::default()
            })
            .unwrap();

        let exit_code = daemon.run_non_terminal_container().unwrap();
        assert_eq!(exit_code, 0);

        let log_content = fs::read_to_string(&log_path).unwrap();
        let records = parse_cri_log_lines(&log_content);
        assert_eq!(records.len(), 2);
        for record in &records {
            assert!(chrono::DateTime::parse_from_rfc3339(&record.0).is_ok());
            assert_eq!(record.2, "F");
        }
        assert!(records
            .iter()
            .any(|record| record.1 == "stdout" && record.3 == "stdout:hello"));
        assert!(records
            .iter()
            .any(|record| record.1 == "stderr" && record.3 == "stderr:world"));
    }

    #[test]
    fn test_non_terminal_container_passes_no_pivot_when_enabled() {
        let temp_dir = tempdir().unwrap();
        let bundle_dir = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle_dir).unwrap();

        let log_path = temp_dir.path().join("logs").join("container.log");
        let args_path = temp_dir.path().join("runtime.args");
        let internal_state = json!({
            "log_path": log_path.to_string_lossy(),
            "tty": false,
            "stdin": false,
            "stdin_once": false,
        });
        let config = json!({
            "process": {
                "terminal": false
            },
            "annotations": {
                INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
            }
        });
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();

        let runtime_path = temp_dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            format!(
                r#"#!/bin/sh
set -eu

cmd="$1"
shift || true

case "$cmd" in
  run)
    printf '%s\n' "$@" > "{}"
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
                args_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

        let exit_code_file = temp_dir.path().join("shim").join("exit_code");
        fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
        let mut daemon = Daemon::new(
            "test-container".to_string(),
            bundle_dir,
            runtime_path,
            DaemonOptions {
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
                exit_code_file: Some(exit_code_file),
                attach_socket_dir: None,
                io_uid: 0,
                io_gid: 0,
                max_container_log_line_size: 4096,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: true,
                no_new_keyring: false,
            },
        );

        daemon
            .io_manager
            .configure(IoConfig {
                stdout: Some(log_path),
                terminal: false,
                ..Default::default()
            })
            .unwrap();

        daemon.run_non_terminal_container().unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--no-pivot"));
    }

    #[test]
    fn test_non_terminal_container_passes_no_new_keyring_when_enabled() {
        let temp_dir = tempdir().unwrap();
        let bundle_dir = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle_dir).unwrap();

        let log_path = temp_dir.path().join("logs").join("container.log");
        let args_path = temp_dir.path().join("runtime.args");
        let internal_state = json!({
            "log_path": log_path.to_string_lossy(),
            "tty": false,
            "stdin": false,
            "stdin_once": false,
        });
        let config = json!({
            "process": {
                "terminal": false
            },
            "annotations": {
                INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
            }
        });
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();

        let runtime_path = temp_dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            format!(
                r#"#!/bin/sh
set -eu

cmd="$1"
shift || true

case "$cmd" in
  run)
    printf '%s\n' "$@" > "{}"
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
                args_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

        let exit_code_file = temp_dir.path().join("shim").join("exit_code");
        fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
        let mut daemon = Daemon::new(
            "test-container".to_string(),
            bundle_dir,
            runtime_path,
            DaemonOptions {
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
                exit_code_file: Some(exit_code_file),
                attach_socket_dir: None,
                io_uid: 0,
                io_gid: 0,
                max_container_log_line_size: 4096,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: false,
                no_new_keyring: true,
            },
        );

        daemon
            .io_manager
            .configure(IoConfig {
                stdout: Some(log_path),
                terminal: false,
                ..Default::default()
            })
            .unwrap();

        daemon.run_non_terminal_container().unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.lines().any(|line| line == "--no-new-keyring"));
    }

    #[test]
    fn test_setup_io_places_reopen_socket_under_attach_socket_dir() {
        let temp_dir = tempdir().unwrap();
        let bundle_dir = temp_dir.path().join("bundle");
        fs::create_dir_all(&bundle_dir).unwrap();

        let log_path = temp_dir.path().join("logs").join("container.log");
        let internal_state = json!({
            "log_path": log_path.to_string_lossy(),
            "tty": false,
            "stdin": false,
            "stdin_once": false,
        });
        let config = json!({
            "process": {
                "terminal": false
            },
            "annotations": {
                INTERNAL_CONTAINER_STATE_KEY: internal_state.to_string()
            }
        });
        fs::write(
            bundle_dir.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();

        let runtime_path = temp_dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            r#"#!/bin/sh
set -eu
exit 0
"#,
        )
        .unwrap();
        fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

        let exit_code_file = temp_dir.path().join("exits").join("container-1");
        fs::create_dir_all(exit_code_file.parent().unwrap()).unwrap();
        let attach_socket_dir = temp_dir.path().join("attach");
        let mut daemon = Daemon::new(
            "container-1".to_string(),
            bundle_dir,
            runtime_path,
            DaemonOptions {
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
                exit_code_file: Some(exit_code_file.clone()),
                attach_socket_dir: Some(attach_socket_dir.clone()),
                io_uid: 0,
                io_gid: 0,
                max_container_log_line_size: 4096,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: false,
                no_new_keyring: false,
            },
        );

        daemon.setup_io().unwrap();

        let expected = attach_socket_dir.join("container-1").join("reopen.sock");
        let unexpected = exit_code_file.parent().unwrap().join("reopen.sock");
        assert!(expected.exists());
        assert!(!unexpected.exists());

        daemon.io_manager.shutdown().unwrap();
        daemon.cleanup_attach_socket_directory();
    }
}
