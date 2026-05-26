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
use std::time::{Duration, Instant};

use crate::proto::runtime::v1::LinuxContainerResources;
use crate::services::{InternalEvent, InternalEventSeverity, LedgerInternalEventSink};
use crate::shim_rpc::{
    default_task_socket_path, CheckpointTaskRequest, CreateTaskRequest, DeleteTaskRequest,
    ExecProcessRequest, KillTaskRequest, PauseTaskRequest, ReopenLogRequest, ResizePtyRequest,
    RestoreTaskRequest, ResumeTaskRequest, ShimLinuxResources, ShimRpcClient, ShimRpcRequest,
    ShimRpcResponse, StartTaskRequest, StatusRequest, StatusResponse, UpdateResourcesRequest,
    WaitProcessRequest, WaitProcessResponse,
};
use crate::storage::{ShimProcessRecord, StorageManager};

const DEFAULT_SHIM_WORK_DIR: &str = "/var/run/crius/shims";
const SHIM_METADATA_FILE: &str = "shim.json";
const SHIM_PIDFILE_NAME: &str = "shim.pid";
const SHIM_RPC_READY_TIMEOUT: Duration = Duration::from_secs(5);
const SHIM_RPC_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// OCI runtime 特定配置文件路径。
    pub runtime_config_path: PathBuf,
    /// monitor/shim 所在 cgroup；支持空字符串、`pod` 或 systemd slice。
    pub monitor_cgroup: String,
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
    /// 是否禁止创建新的 session keyring。
    pub no_new_keyring: bool,
    /// 是否让运行时使用 systemd cgroup 模式。
    pub systemd_cgroup: bool,
    /// 运行时路径(runc)
    pub runtime_path: PathBuf,
    /// CRI 单条日志记录切分阈值（字节）。
    pub max_container_log_line_size: usize,
    /// 状态账本数据库路径。
    pub state_db_path: PathBuf,
}

impl Default for ShimConfig {
    fn default() -> Self {
        Self {
            shim_path: PathBuf::from("crius-shim"),
            runtime_config_path: PathBuf::new(),
            monitor_cgroup: String::new(),
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
            no_new_keyring: false,
            systemd_cgroup: false,
            runtime_path: PathBuf::from("runc"),
            max_container_log_line_size: 4096,
            state_db_path: PathBuf::new(),
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

fn ensure_empty_response(operation: &str, response: ShimRpcResponse) -> Result<()> {
    match response {
        ShimRpcResponse::Empty => Ok(()),
        other => Err(anyhow::anyhow!(
            "unexpected shim RPC response for {}: {:?}",
            operation,
            other
        )),
    }
}

impl ShimManager {
    fn ledger_storage(&self) -> Result<Option<StorageManager>> {
        if self.config.state_db_path.as_os_str().is_empty() {
            return Ok(None);
        }
        Ok(Some(StorageManager::new(&self.config.state_db_path)?))
    }

    fn exit_code_file_path(&self, container_id: &str) -> PathBuf {
        self.config.container_exits_dir.join(container_id)
    }

    fn read_exit_code_file(path: &Path) -> Result<Option<i32>> {
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(path)?;
        let exit_code = content
            .trim()
            .parse::<i32>()
            .context("Failed to parse exit code")?;
        Ok(Some(exit_code))
    }

    fn pidfile_path_for(config: &ShimConfig, container_id: &str) -> PathBuf {
        config.work_dir.join(container_id).join(SHIM_PIDFILE_NAME)
    }

    fn metadata_path(&self, container_id: &str) -> PathBuf {
        Self::metadata_path_for(&self.config, container_id)
    }

    fn metadata_path_for(config: &ShimConfig, container_id: &str) -> PathBuf {
        config.work_dir.join(container_id).join(SHIM_METADATA_FILE)
    }

    pub fn pidfile_path(&self, container_id: &str) -> PathBuf {
        Self::pidfile_path_for(&self.config, container_id)
    }

    fn process_exists(pid: u32) -> bool {
        PathBuf::from("/proc").join(pid.to_string()).exists()
    }

    fn persist_shim_ledger_state(
        &self,
        container_id: &str,
        shim_pid: u32,
        exit_code_file: &Path,
        log_file: &Path,
        socket_path: &Path,
        bundle_path: &Path,
        state: &str,
    ) -> Result<()> {
        if let Some(mut storage) = self.ledger_storage()? {
            storage.save_shim_process(&ShimProcessRecord {
                container_id: container_id.to_string(),
                shim_pid,
                work_dir: self.config.work_dir.display().to_string(),
                socket_path: socket_path.display().to_string(),
                exit_code_file: exit_code_file.display().to_string(),
                log_file: log_file.display().to_string(),
                bundle_path: bundle_path.display().to_string(),
                state: state.to_string(),
                last_seen_at: chrono::Utc::now().timestamp(),
            })?;
        }
        Ok(())
    }

    fn persist_process_metadata(&self, process: &ShimProcess) {
        let metadata_path = self.metadata_path(&process.container_id);
        if let Err(err) = (|| -> Result<()> {
            if let Some(parent) = metadata_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&metadata_path, serde_json::to_vec_pretty(process)?)?;
            Ok(())
        })() {
            log::warn!(
                "Failed to write diagnostic shim metadata {}: {}",
                metadata_path.display(),
                err
            );
        }
    }

    fn persist_pidfile(&self, container_id: &str, shim_pid: u32) {
        if let Err(err) = Self::persist_pidfile_for(&self.config, container_id, shim_pid) {
            log::warn!(
                "Failed to write diagnostic shim pidfile for {}: {}",
                container_id,
                err
            );
        }
    }

    fn persist_pidfile_for(config: &ShimConfig, container_id: &str, shim_pid: u32) -> Result<()> {
        let pidfile_path = Self::pidfile_path_for(config, container_id);
        if let Some(parent) = pidfile_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(pidfile_path, format!("{shim_pid}\n"))?;
        Ok(())
    }

    fn remove_process_metadata(&self, container_id: &str) -> Result<()> {
        if let Some(mut storage) = self.ledger_storage()? {
            let _ = storage.delete_shim_process(container_id);
        }
        let metadata_path = self.metadata_path(container_id);
        if metadata_path.exists() {
            fs::remove_file(metadata_path)?;
        }
        Ok(())
    }

    fn remove_pidfile(&self, container_id: &str) -> Result<()> {
        let pidfile_path = self.pidfile_path(container_id);
        if pidfile_path.exists() {
            fs::remove_file(pidfile_path)?;
        }
        Ok(())
    }

    pub fn read_shim_pidfile(&self, container_id: &str) -> Result<Option<u32>> {
        let pidfile_path = self.pidfile_path(container_id);
        if !pidfile_path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&pidfile_path)?;
        let pid = raw
            .trim()
            .parse::<u32>()
            .with_context(|| format!("failed to parse shim pidfile {}", pidfile_path.display()))?;
        Ok(Some(pid))
    }

    fn restore_processes_from_disk(config: &ShimConfig) -> Vec<ShimProcess> {
        if !config.state_db_path.as_os_str().is_empty() {
            if let Ok(mut storage) = StorageManager::new(&config.state_db_path) {
                if let Ok(records) = storage.list_shim_processes() {
                    let restored = records
                        .into_iter()
                        .filter_map(|mut record| {
                            let live_or_exited = Self::process_exists(record.shim_pid)
                                || Path::new(&record.exit_code_file).exists();
                            if !live_or_exited {
                                return None;
                            }

                            let metadata_path =
                                Self::metadata_path_for(config, &record.container_id);
                            let pidfile_path = Self::pidfile_path_for(config, &record.container_id);
                            if !metadata_path.exists() || !pidfile_path.exists() {
                                let previous_state = record.state.clone();
                                record.state = "degraded".to_string();
                                record.last_seen_at = chrono::Utc::now().timestamp();
                                let _ = storage.save_shim_process(&record);
                                let details = format!(
                                    "diagnostic shim files missing: metadata={}, pidfile={}",
                                    metadata_path.exists(),
                                    pidfile_path.exists()
                                );
                                let event = InternalEvent::new(
                                    "shim.degraded",
                                    "shim",
                                    &record.container_id,
                                    InternalEventSeverity::Warning,
                                    serde_json::json!({
                                        "previousState": previous_state,
                                        "state": "degraded",
                                        "details": details,
                                        "metadataExists": metadata_path.exists(),
                                        "pidfileExists": pidfile_path.exists(),
                                    }),
                                );
                                let _ = LedgerInternalEventSink::new(&config.state_db_path)
                                    .publish(&event);
                            }

                            Some(ShimProcess {
                                container_id: record.container_id,
                                shim_pid: record.shim_pid,
                                exit_code_file: PathBuf::from(record.exit_code_file),
                                log_file: PathBuf::from(record.log_file),
                                socket_path: PathBuf::from(record.socket_path),
                                bundle_path: PathBuf::from(record.bundle_path),
                            })
                        })
                        .collect::<Vec<_>>();
                    if !restored.is_empty() {
                        return restored;
                    }
                }
            }
        }
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
        for process in &restored {
            let _ = Self::persist_pidfile_for(&config, &process.container_id, process.shim_pid);
        }

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

    pub fn task_socket_path(&self, container_id: &str) -> PathBuf {
        default_task_socket_path(&self.config.work_dir, container_id)
    }

    fn rpc_client(&self, container_id: &str) -> ShimRpcClient {
        ShimRpcClient::new(self.task_socket_path(container_id), SHIM_RPC_TIMEOUT)
    }

    fn wait_for_rpc_socket(&self, container_id: &str) -> Result<()> {
        let socket_path = self.task_socket_path(container_id);
        let deadline = Instant::now() + SHIM_RPC_READY_TIMEOUT;
        while Instant::now() < deadline {
            if socket_path.exists() {
                let client = self.rpc_client(container_id);
                if matches!(
                    client.request(ShimRpcRequest::Ping),
                    Ok(ShimRpcResponse::Empty)
                ) {
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        Err(anyhow::anyhow!(
            "shim RPC socket {} did not become ready in {:?}",
            socket_path.display(),
            SHIM_RPC_READY_TIMEOUT
        ))
    }

    /// 启动shim进程来管理容器
    pub fn start_shim(&self, container_id: &str, bundle_path: &Path) -> Result<ShimProcess> {
        info!("Starting shim for container {}", container_id);

        if let Some(existing) = self
            .list_shims()
            .into_iter()
            .find(|process| process.container_id == container_id)
        {
            if Self::process_exists(existing.shim_pid) {
                self.wait_for_rpc_socket(container_id)?;
                return Ok(existing);
            }
        }

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

        self.persist_shim_ledger_state(
            container_id,
            0,
            &exit_code_file,
            &log_file,
            &socket_path,
            bundle_path,
            "planned",
        )?;

        // 构建shim命令
        let mut cmd = Command::new(&self.config.shim_path);
        cmd.arg("--id")
            .arg(container_id)
            .arg("--bundle")
            .arg(bundle_path)
            .arg("--runtime")
            .arg(&self.config.runtime_path)
            .arg("--work-dir")
            .arg(&self.config.work_dir)
            .arg("--exit-code-file")
            .arg(&exit_code_file)
            .arg("--attach-socket-dir")
            .arg(&self.config.attach_socket_dir)
            .arg("--io-uid")
            .arg(self.config.io_uid.to_string())
            .arg("--io-gid")
            .arg(self.config.io_gid.to_string());

        if !self.config.state_db_path.as_os_str().is_empty() {
            cmd.arg("--state-db-path").arg(&self.config.state_db_path);
        }

        if !self.config.runtime_config_path.as_os_str().is_empty() {
            cmd.arg("--runtime-config-path")
                .arg(&self.config.runtime_config_path);
        }
        if !self.config.monitor_cgroup.trim().is_empty() {
            cmd.arg("--monitor-cgroup").arg(&self.config.monitor_cgroup);
        }

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
        if self.config.no_new_keyring {
            cmd.arg("--no-new-keyring");
        }
        if self.config.systemd_cgroup {
            cmd.arg("--systemd-cgroup");
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
        self.persist_shim_ledger_state(
            container_id,
            shim_pid,
            &exit_code_file,
            &log_file,
            &socket_path,
            bundle_path,
            "starting",
        )?;
        self.persist_pidfile(container_id, shim_pid);

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
        self.persist_process_metadata(&process);
        self.wait_for_rpc_socket(container_id)?;
        self.persist_shim_ledger_state(
            container_id,
            shim_pid,
            &process.exit_code_file,
            &process.log_file,
            &process.socket_path,
            &process.bundle_path,
            "running",
        )?;

        Ok(process)
    }

    /// 获取容器的退出码
    pub fn get_exit_code(&self, container_id: &str) -> Result<Option<i32>> {
        let processes = self.processes.lock().unwrap();

        if let Some(process) = processes.iter().find(|p| p.container_id == container_id) {
            return Self::read_exit_code_file(&process.exit_code_file);
        }

        Self::read_exit_code_file(&self.exit_code_file_path(container_id))
    }

    pub fn create_task(
        &self,
        container_id: &str,
        bundle_path: &Path,
        rootfs_path: &Path,
    ) -> Result<()> {
        self.start_shim(container_id, bundle_path)?;
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::CreateTask(CreateTaskRequest {
                container_id: container_id.to_string(),
                rootfs_path: rootfs_path.to_path_buf(),
            }))?;
        ensure_empty_response("create_task", response)
    }

    pub fn start_task(&self, container_id: &str, bundle_path: &Path) -> Result<()> {
        self.start_shim(container_id, bundle_path)?;
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::StartTask(StartTaskRequest {
                container_id: container_id.to_string(),
            }))?;
        ensure_empty_response("start_task", response)
    }

    pub fn restore_task(
        &self,
        container_id: &str,
        bundle_path: &Path,
        image_path: &Path,
        work_path: &Path,
        criu_path: &Path,
        no_pivot: bool,
    ) -> Result<()> {
        self.start_shim(container_id, bundle_path)?;
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::RestoreTask(RestoreTaskRequest {
                container_id: container_id.to_string(),
                image_path: image_path.to_path_buf(),
                work_path: work_path.to_path_buf(),
                bundle_path: bundle_path.to_path_buf(),
                criu_path: criu_path.to_path_buf(),
                no_pivot,
            }))?;
        ensure_empty_response("restore_task", response)
    }

    pub fn exec_process(
        &self,
        container_id: &str,
        command: &[String],
        tty: bool,
        exec_cpu_affinity: Option<usize>,
    ) -> Result<i32> {
        match self
            .rpc_client(container_id)
            .request(ShimRpcRequest::ExecProcess(ExecProcessRequest {
                container_id: container_id.to_string(),
                command: command.to_vec(),
                tty,
                capture_output: false,
                timeout_ms: None,
                io_drain_timeout_ms: None,
                exec_cpu_affinity,
            }))? {
            ShimRpcResponse::ExecProcess(response) => Ok(response.exit_code),
            other => Err(anyhow::anyhow!(
                "unexpected shim RPC response for exec_process: {:?}",
                other
            )),
        }
    }

    pub fn exec_sync_process(
        &self,
        container_id: &str,
        command: &[String],
        tty: bool,
        timeout: Option<Duration>,
        exec_cpu_affinity: Option<usize>,
    ) -> Result<(i32, Vec<u8>, Vec<u8>)> {
        match self
            .rpc_client(container_id)
            .request(ShimRpcRequest::ExecProcess(ExecProcessRequest {
                container_id: container_id.to_string(),
                command: command.to_vec(),
                tty,
                capture_output: true,
                timeout_ms: timeout.map(|value| value.as_millis() as u64),
                io_drain_timeout_ms: None,
                exec_cpu_affinity,
            }))? {
            ShimRpcResponse::ExecProcess(response) => {
                Ok((response.exit_code, response.stdout, response.stderr))
            }
            other => Err(anyhow::anyhow!(
                "unexpected shim RPC response for exec_sync_process: {:?}",
                other
            )),
        }
    }

    pub fn wait_task(&self, container_id: &str, timeout: Option<Duration>) -> Result<Option<i32>> {
        match self
            .rpc_client(container_id)
            .request(ShimRpcRequest::WaitProcess(WaitProcessRequest {
                container_id: container_id.to_string(),
                timeout_ms: timeout.map(|value| value.as_millis() as u64),
            }))? {
            ShimRpcResponse::WaitProcess(WaitProcessResponse { exit_code }) => Ok(exit_code),
            other => Err(anyhow::anyhow!(
                "unexpected shim RPC response for wait_task: {:?}",
                other
            )),
        }
    }

    pub fn kill_task(&self, container_id: &str, signal: &str, all: bool) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::KillTask(KillTaskRequest {
                container_id: container_id.to_string(),
                signal: signal.to_string(),
                all,
            }))?;
        ensure_empty_response("kill_task", response)
    }

    pub fn delete_task(&self, container_id: &str) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::DeleteTask(DeleteTaskRequest {
                container_id: container_id.to_string(),
            }))?;
        ensure_empty_response("delete_task", response)
    }

    pub fn update_resources(
        &self,
        container_id: &str,
        resources: &LinuxContainerResources,
    ) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::UpdateResources(UpdateResourcesRequest {
                container_id: container_id.to_string(),
                resources: ShimLinuxResources::from(resources),
            }))?;
        ensure_empty_response("update_resources", response)
    }

    pub fn checkpoint_task(
        &self,
        container_id: &str,
        image_path: &Path,
        work_path: &Path,
    ) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::CheckpointTask(CheckpointTaskRequest {
                container_id: container_id.to_string(),
                image_path: image_path.to_path_buf(),
                work_path: work_path.to_path_buf(),
            }))?;
        ensure_empty_response("checkpoint_task", response)
    }

    pub fn reopen_log(&self, container_id: &str) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::ReopenLog(ReopenLogRequest {
                container_id: container_id.to_string(),
            }))?;
        ensure_empty_response("reopen_log", response)
    }

    pub fn resize_pty(&self, container_id: &str, width: u16, height: u16) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::ResizePty(ResizePtyRequest {
                container_id: container_id.to_string(),
                width,
                height,
            }))?;
        ensure_empty_response("resize_pty", response)
    }

    pub fn status(&self, container_id: &str) -> Result<StatusResponse> {
        match self
            .rpc_client(container_id)
            .request(ShimRpcRequest::Status(StatusRequest {
                container_id: container_id.to_string(),
            }))? {
            ShimRpcResponse::Status(response) => Ok(response),
            other => Err(anyhow::anyhow!(
                "unexpected shim RPC response for status: {:?}",
                other
            )),
        }
    }

    pub fn pause_task(&self, container_id: &str) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::PauseTask(PauseTaskRequest {
                container_id: container_id.to_string(),
            }))?;
        ensure_empty_response("pause_task", response)
    }

    pub fn resume_task(&self, container_id: &str) -> Result<()> {
        let response = self
            .rpc_client(container_id)
            .request(ShimRpcRequest::ResumeTask(ResumeTaskRequest {
                container_id: container_id.to_string(),
            }))?;
        ensure_empty_response("resume_task", response)
    }

    pub fn container_pid(&self, container_id: &str) -> Result<Option<i32>> {
        match self
            .rpc_client(container_id)
            .request(ShimRpcRequest::ContainerPid(StatusRequest {
                container_id: container_id.to_string(),
            }))? {
            ShimRpcResponse::ContainerPid(pid) => Ok(pid),
            other => Err(anyhow::anyhow!(
                "unexpected shim RPC response for container_pid: {:?}",
                other
            )),
        }
    }

    /// 停止shim进程
    pub fn stop_shim(&self, container_id: &str) -> Result<()> {
        info!("Stopping shim for container {}", container_id);

        let _ = self.delete_task(container_id);

        let mut processes = self.processes.lock().unwrap();
        let removed = processes
            .iter()
            .position(|p| p.container_id == container_id)
            .map(|index| processes.remove(index));
        drop(processes);

        let shim_pid = match removed.as_ref() {
            Some(process) => Some(process.shim_pid),
            None => self.read_shim_pidfile(container_id)?,
        };

        if let Some(shim_pid) = shim_pid {
            #[cfg(unix)]
            {
                use nix::sys::signal::{self, Signal};
                use nix::unistd::Pid;

                let pid = Pid::from_raw(shim_pid as i32);
                let _ = signal::kill(pid, Signal::SIGTERM);
            }
        }

        if let Some(process) = removed.as_ref() {
            let _ = process
                .socket_path
                .parent()
                .map(fs::remove_dir_all)
                .transpose();
        } else {
            let _ = fs::remove_dir_all(self.config.attach_socket_dir.join(container_id));
        }

        if shim_pid.is_some() {
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
            self.read_shim_pidfile(container_id)
                .ok()
                .flatten()
                .is_some_and(Self::process_exists)
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
                if process.exit_code_file.exists() {
                    return true;
                }
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
                let _ = self.remove_pidfile(&process.container_id);
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
mod tests;
