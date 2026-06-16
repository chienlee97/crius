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
use std::ffi::CString;
use std::fs;
use std::fs::File;
use std::io::IoSliceMut;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::net::UnixListener;
use std::os::unix::process::CommandExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::io::{IoConfig, IoManager, JournalConfig, DEFAULT_JOURNALD_SOCKET_PATH};
use crate::image::snapshotter::{RootfsHandle, RootfsHandleKind, RootfsMountSpec};
use crate::runtime::RuncRuntime;
use crate::services::{InternalEvent, InternalEventSeverity, LedgerInternalEventSink};
use crate::shim_rpc::server::{default_task_socket_path, serve, ShimRpcHandler};
use crate::shim_rpc::{
    CheckpointTaskRequest, CreateTaskRequest, DeleteTaskRequest, ExecProcessRequest,
    ExecProcessResponse, KillTaskRequest, OpenAttachStreamRequest, OpenAttachStreamResponse,
    OpenExecSessionRequest, OpenExecSessionResponse, PauseTaskRequest, ReopenLogRequest,
    ResizePtyRequest, RestoreTaskRequest, ResumeTaskRequest, ShimRpcRequest, ShimRpcResponse,
    StartTaskRequest, StatusRequest, StatusResponse, TaskState, UpdateResourcesRequest,
    WaitProcessRequest, WaitProcessResponse,
};
use crate::storage::StorageManager;

const INTERNAL_CONTAINER_STATE_KEY: &str = "io.crius.internal/container-state";

fn parse_mount_options(options: &[String]) -> Result<(libc::c_ulong, Option<String>)> {
    let mut flags: libc::c_ulong = 0;
    let mut data = Vec::new();
    for option in options {
        match option.as_str() {
            "" | "rw" => {}
            "ro" => flags |= libc::MS_RDONLY as libc::c_ulong,
            "bind" => flags |= libc::MS_BIND as libc::c_ulong,
            "rbind" => flags |= (libc::MS_BIND | libc::MS_REC) as libc::c_ulong,
            "rec" => flags |= libc::MS_REC as libc::c_ulong,
            "private" => flags |= libc::MS_PRIVATE as libc::c_ulong,
            "rprivate" => flags |= (libc::MS_PRIVATE | libc::MS_REC) as libc::c_ulong,
            "shared" => flags |= libc::MS_SHARED as libc::c_ulong,
            "rshared" => flags |= (libc::MS_SHARED | libc::MS_REC) as libc::c_ulong,
            "slave" => flags |= libc::MS_SLAVE as libc::c_ulong,
            "rslave" => flags |= (libc::MS_SLAVE | libc::MS_REC) as libc::c_ulong,
            "nosuid" => flags |= libc::MS_NOSUID as libc::c_ulong,
            "nodev" => flags |= libc::MS_NODEV as libc::c_ulong,
            "noexec" => flags |= libc::MS_NOEXEC as libc::c_ulong,
            "sync" => flags |= libc::MS_SYNCHRONOUS as libc::c_ulong,
            "dirsync" => flags |= libc::MS_DIRSYNC as libc::c_ulong,
            "noatime" => flags |= libc::MS_NOATIME as libc::c_ulong,
            "nodiratime" => flags |= libc::MS_NODIRATIME as libc::c_ulong,
            "relatime" => flags |= libc::MS_RELATIME as libc::c_ulong,
            other => data.push(other.to_string()),
        }
    }
    Ok((flags, (!data.is_empty()).then(|| data.join(","))))
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DaemonTaskState {
    Init,
    Created,
    Running,
    Paused,
    Stopped,
    Deleted,
}

#[derive(Debug)]
struct ExecSessionHandle {
    io_socket_path: PathBuf,
    resize_socket_path: Option<PathBuf>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy)]
struct AttachStreamHandle {
    tty: bool,
}

impl DaemonTaskState {
    fn as_rpc_state(self) -> TaskState {
        match self {
            Self::Init => TaskState::Init,
            Self::Created => TaskState::Created,
            Self::Running => TaskState::Running,
            Self::Paused => TaskState::Paused,
            Self::Stopped => TaskState::Stopped,
            Self::Deleted => TaskState::Deleted,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Created => "created",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }
}

/// Shim守护进程
#[derive(Clone)]
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
    /// shim 工作目录根路径。
    work_dir: PathBuf,
    /// 统一账本路径。
    state_db_path: Option<PathBuf>,
    /// IO管理器
    io_manager: IoManager,
    /// 是否正在运行
    running: Arc<AtomicBool>,
    /// task 生命周期状态。
    task_state: Arc<Mutex<DaemonTaskState>>,
    /// 最近已知的容器 PID。
    container_pid: Arc<Mutex<Option<i32>>>,
    /// 最近已知的退出码。
    exit_code: Arc<Mutex<Option<i32>>>,
    /// task 后台线程。
    task_thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    /// exec session 后台线程。
    exec_sessions: Arc<Mutex<HashMap<String, ExecSessionHandle>>>,
    /// Active attach RPC streams keyed by stream id.
    attach_streams: Arc<Mutex<HashMap<String, AttachStreamHandle>>>,
    /// Monotonic attach stream sequence for deterministic ids.
    next_attach_stream_id: Arc<Mutex<u64>>,
    /// shim-owned rootfs handle from CreateTask.
    rootfs_handle: Arc<Mutex<Option<ShimRootfsHandle>>>,
}

#[derive(Debug, Clone)]
struct ShimRootfsHandle {
    snapshot_key: Option<String>,
    rootfs_path: PathBuf,
    mount_options: Vec<String>,
    rootfs: Option<RootfsHandle>,
    mounted_targets: Vec<PathBuf>,
}

pub struct DaemonOptions {
    pub runtime_config_path: PathBuf,
    pub monitor_cgroup: String,
    pub work_dir: PathBuf,
    pub state_db_path: Option<PathBuf>,
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
            work_dir,
            state_db_path,
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
            work_dir,
            state_db_path,
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
            task_state: Arc::new(Mutex::new(DaemonTaskState::Init)),
            container_pid: Arc::new(Mutex::new(None)),
            exit_code: Arc::new(Mutex::new(None)),
            task_thread: Arc::new(Mutex::new(None)),
            exec_sessions: Arc::new(Mutex::new(HashMap::new())),
            attach_streams: Arc::new(Mutex::new(HashMap::new())),
            next_attach_stream_id: Arc::new(Mutex::new(1)),
            rootfs_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// 运行守护进程
    pub fn run(self) -> Result<()> {
        // 1. 设置子进程收割者
        self.setup_subreaper()?;

        // 2. 设置信号处理器
        self.setup_signal_handlers()?;

        // 3. 把 shim/monitor 进程放到目标 cgroup。
        self.configure_monitor_cgroup()?;

        // 4. 启动 RPC task service。
        let socket_path = self.task_socket_path();
        info!(
            "Shim daemon ready for container {} on {}",
            self.container_id,
            socket_path.display()
        );
        serve(&socket_path, self.running.clone(), Arc::new(self.clone()))
    }

    fn task_socket_path(&self) -> PathBuf {
        default_task_socket_path(&self.work_dir, &self.container_id)
    }

    fn exec_session_dir(&self, session_id: &str) -> PathBuf {
        self.shim_dir().join("exec").join(session_id)
    }

    fn exec_session_io_socket_path(&self, session_id: &str) -> PathBuf {
        self.exec_session_dir(session_id).join("io.sock")
    }

    fn exec_session_resize_socket_path(&self, session_id: &str) -> PathBuf {
        self.exec_session_dir(session_id).join("resize.sock")
    }

    fn set_task_state(&self, next: DaemonTaskState) {
        let previous = {
            let mut guard = self.task_state.lock().unwrap();
            let previous = *guard;
            *guard = next;
            previous
        };
        if previous != next {
            let _ = self.record_task_event(previous, next, None);
        }
    }

    fn close_all_attach_streams(&self) -> usize {
        self.attach_streams.lock().unwrap().drain().count()
    }

    fn record_task_event(
        &self,
        previous: DaemonTaskState,
        next: DaemonTaskState,
        details: Option<String>,
    ) -> Result<()> {
        let Some(path) = self.state_db_path.as_ref() else {
            return Ok(());
        };
        let mut event_details = serde_json::json!({
            "previousState": previous.as_str(),
            "state": next.as_str(),
        });
        if let Some(details) = details {
            event_details["details"] = serde_json::Value::String(details);
        }
        let event = InternalEvent::new(
            "task.state",
            "task",
            &self.container_id,
            InternalEventSeverity::Info,
            event_details,
        );
        LedgerInternalEventSink::new(path).publish(&event)
    }

    fn record_exec_event(&self, details: &str) -> Result<()> {
        let Some(path) = self.state_db_path.as_ref() else {
            return Ok(());
        };
        let event = InternalEvent::new(
            "exec.event",
            "shim",
            &self.container_id,
            InternalEventSeverity::Info,
            serde_json::json!({
                "message": details,
            }),
        );
        LedgerInternalEventSink::new(path).publish(&event)
    }

    fn apply_rootfs_override(&self, rootfs_path: &Path) -> Result<()> {
        let config_path = self.bundle.join("config.json");
        let raw = fs::read(&config_path)
            .with_context(|| format!("Failed to read bundle config {}", config_path.display()))?;
        let mut config: serde_json::Value = serde_json::from_slice(&raw)
            .with_context(|| format!("Failed to parse bundle config {}", config_path.display()))?;
        let root = config
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("bundle config is not a JSON object"))?
            .entry("root")
            .or_insert_with(|| serde_json::json!({}));
        let root_object = root
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("bundle config root entry is not a JSON object"))?;
        root_object.insert(
            "path".to_string(),
            serde_json::Value::String(rootfs_path.display().to_string()),
        );
        fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
            .with_context(|| format!("Failed to persist bundle config {}", config_path.display()))
    }

    fn apply_rootfs_handle_mounts(&self, rootfs: Option<&RootfsHandle>) -> Result<Vec<PathBuf>> {
        let Some(rootfs) = rootfs else {
            return Ok(Vec::new());
        };
        match rootfs.kind {
            RootfsHandleKind::InternalPath => Ok(Vec::new()),
            RootfsHandleKind::ExternalMountSpec => {
                let mut mounted_targets: Vec<PathBuf> = Vec::new();
                for mount in &rootfs.mounts {
                    if let Err(err) = Self::mount_rootfs_spec(mount) {
                        for target in mounted_targets.iter().rev() {
                            let _ = Self::unmount_rootfs_target(target);
                        }
                        if let Some(snapshot_key) = rootfs.snapshot_key.as_deref() {
                            let _ = self.update_snapshot_state(snapshot_key, "broken");
                        }
                        return Err(err);
                    }
                    mounted_targets.push(mount.target.clone());
                }
                Ok(mounted_targets)
            }
        }
    }

    fn mount_rootfs_spec(mount: &RootfsMountSpec) -> Result<()> {
        if mount.mount_type.trim().is_empty() {
            return Err(anyhow::anyhow!("rootfs mount spec type must not be empty"));
        }
        if mount.target.as_os_str().is_empty() {
            return Err(anyhow::anyhow!(
                "rootfs mount spec target must not be empty"
            ));
        }
        fs::create_dir_all(&mount.target).with_context(|| {
            format!(
                "Failed to create rootfs mount target {}",
                mount.target.display()
            )
        })?;
        let fstype = CString::new(mount.mount_type.as_bytes())
            .context("rootfs mount type contains interior NUL")?;
        let source = if mount.source.as_os_str().is_empty() {
            None
        } else {
            Some(
                CString::new(mount.source.as_os_str().as_bytes())
                    .context("rootfs mount source contains interior NUL")?,
            )
        };
        let target = CString::new(mount.target.as_os_str().as_bytes())
            .context("rootfs mount target contains interior NUL")?;
        let (flags, data) = parse_mount_options(&mount.options)?;
        let data_cstring = data
            .as_ref()
            .map(|value| CString::new(value.as_bytes()))
            .transpose()
            .context("rootfs mount data contains interior NUL")?;
        let data_ptr = data_cstring
            .as_ref()
            .map(|value| value.as_ptr() as *const libc::c_void)
            .unwrap_or(std::ptr::null());

        let rc = unsafe {
            libc::mount(
                source
                    .as_ref()
                    .map(|value| value.as_ptr())
                    .unwrap_or(std::ptr::null()),
                target.as_ptr(),
                fstype.as_ptr(),
                flags,
                data_ptr,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| {
                format!(
                    "Failed to mount rootfs {} on {} as {} with options {:?}",
                    mount.source.display(),
                    mount.target.display(),
                    mount.mount_type,
                    mount.options
                )
            });
        }
        Ok(())
    }

    fn unmount_rootfs_target(target: &Path) -> Result<()> {
        let target = CString::new(target.as_os_str().as_bytes())
            .context("rootfs unmount target contains interior NUL")?;
        let rc = unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error()).context("Failed to unmount rootfs target");
        }
        Ok(())
    }

    fn record_rootfs_handle(&self, request: &CreateTaskRequest) -> Result<()> {
        let mounted_targets = self.apply_rootfs_handle_mounts(request.rootfs.as_ref())?;
        *self.rootfs_handle.lock().unwrap() = Some(ShimRootfsHandle {
            snapshot_key: request.snapshot_key.clone(),
            rootfs_path: request.rootfs_path.clone(),
            mount_options: request.mount_options.clone(),
            rootfs: request.rootfs.clone(),
            mounted_targets,
        });
        let Some(snapshot_key) = request.snapshot_key.as_deref() else {
            return Ok(());
        };
        self.update_snapshot_state(snapshot_key, "mounted")
    }

    fn update_snapshot_state(&self, snapshot_key: &str, state: &str) -> Result<()> {
        let Some(path) = self.state_db_path.as_ref() else {
            return Ok(());
        };
        StorageManager::new(path)?.update_snapshot_state(snapshot_key, state)
    }

    fn delete_snapshot_record(&self, snapshot_key: &str) -> Result<()> {
        let Some(path) = self.state_db_path.as_ref() else {
            return Ok(());
        };
        StorageManager::new(path)?.delete_snapshot(snapshot_key)
    }

    fn cleanup_rootfs_handle(&self, request: &DeleteTaskRequest) -> Result<()> {
        let stored = self.rootfs_handle.lock().unwrap().take();
        let snapshot_key = request.snapshot_key.clone().or_else(|| {
            stored
                .as_ref()
                .and_then(|handle| handle.snapshot_key.clone())
        });
        let rootfs_path = request
            .rootfs_path
            .clone()
            .or_else(|| stored.as_ref().map(|handle| handle.rootfs_path.clone()));

        if let Some(handle) = stored.as_ref() {
            for target in handle.mounted_targets.iter().rev() {
                Self::unmount_rootfs_target(target)
                    .with_context(|| format!("Failed to unmount rootfs {}", target.display()))?;
            }
        }

        if let Some(path) = rootfs_path.as_ref().filter(|path| path.exists()) {
            fs::remove_dir_all(path).with_context(|| {
                format!("Failed to remove shim-owned rootfs {}", path.display())
            })?;
        }

        if let Some(snapshot_key) = snapshot_key.as_deref() {
            self.update_snapshot_state(snapshot_key, "deleted")?;
            self.delete_snapshot_record(snapshot_key)?;
        }

        if let Some(handle) = stored.as_ref() {
            debug!(
                "Deleted shim rootfs handle for container {} snapshot {:?} rootfs {} mount options {:?} rootfs {:?} mounted targets {:?}",
                self.container_id,
                handle.snapshot_key,
                handle.rootfs_path.display(),
                handle.mount_options,
                handle.rootfs,
                handle.mounted_targets
            );
        }

        Ok(())
    }

    fn task_status(&self) -> StatusResponse {
        let state = *self.task_state.lock().unwrap();
        let pid = *self.container_pid.lock().unwrap();
        let exit_code = *self.exit_code.lock().unwrap();
        StatusResponse {
            state: state.as_rpc_state(),
            pid,
            exit_code,
        }
    }

    fn spawn_task_runner(&self) -> Result<()> {
        let state = *self.task_state.lock().unwrap();
        match state {
            DaemonTaskState::Running | DaemonTaskState::Paused => return Ok(()),
            DaemonTaskState::Deleted => {
                return Err(anyhow::anyhow!(
                    "cannot start deleted task {}",
                    self.container_id
                ))
            }
            DaemonTaskState::Stopped => {
                return Err(anyhow::anyhow!(
                    "cannot restart stopped task {}",
                    self.container_id
                ))
            }
            DaemonTaskState::Init => {
                return Err(anyhow::anyhow!(
                    "task {} has not been created",
                    self.container_id
                ))
            }
            DaemonTaskState::Created => {}
        }

        let daemon = self.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let handle = std::thread::spawn(move || {
            daemon.set_task_state(DaemonTaskState::Running);
            let mut started_tx = Some(started_tx);
            let result = if daemon.is_terminal().unwrap_or(false) {
                match daemon.create_terminal_container() {
                    Ok(pid) => {
                        *daemon.container_pid.lock().unwrap() = Some(pid.as_raw());
                        info!("Container created with PID: {}", pid);
                        if let Some(tx) = started_tx.take() {
                            let _ = tx.send(Ok(()));
                        }
                        daemon.monitor_container(pid)
                    }
                    Err(err) => {
                        if let Some(tx) = started_tx.take() {
                            let _ = tx.send(Err(err.to_string()));
                        }
                        Err(err)
                    }
                }
            } else {
                if let Some(tx) = started_tx.take() {
                    let _ = tx.send(Ok(()));
                }
                daemon.run_non_terminal_container()
            };

            match result {
                Ok(exit_code) => {
                    *daemon.exit_code.lock().unwrap() = Some(exit_code);
                    *daemon.container_pid.lock().unwrap() = None;
                    daemon.close_all_attach_streams();
                    daemon.set_task_state(DaemonTaskState::Stopped);
                    if let Err(err) = daemon.record_exit_code(exit_code) {
                        warn!(
                            "Failed to persist shim exit code for {}: {}",
                            daemon.container_id, err
                        );
                    }
                }
                Err(err) => {
                    error!("Task runner for {} failed: {}", daemon.container_id, err);
                    *daemon.exit_code.lock().unwrap() = Some(1);
                    *daemon.container_pid.lock().unwrap() = None;
                    daemon.close_all_attach_streams();
                    daemon.set_task_state(DaemonTaskState::Stopped);
                    let _ = daemon.record_exit_code(1);
                }
            }
        });

        let mut guard = self.task_thread.lock().unwrap();
        *guard = Some(handle);
        started_rx
            .recv()
            .map_err(|err| anyhow::anyhow!("task runner exited before start confirmation: {err}"))?
            .map_err(|err| anyhow::anyhow!("task start failed: {err}"))?;
        Ok(())
    }

    fn open_exec_session_internal(
        &self,
        request: &OpenExecSessionRequest,
    ) -> Result<OpenExecSessionResponse> {
        if request.command.is_empty() {
            return Err(anyhow::anyhow!("exec session command must not be empty"));
        }
        let session_id = uuid::Uuid::new_v4().to_string();
        let session_dir = self.exec_session_dir(&session_id);
        std::fs::create_dir_all(&session_dir).with_context(|| {
            format!(
                "failed to create exec session directory {}",
                session_dir.display()
            )
        })?;
        let io_socket_path = self.exec_session_io_socket_path(&session_id);
        let resize_socket_path = request
            .tty
            .then(|| self.exec_session_resize_socket_path(&session_id));
        let daemon = self.clone();
        let request = request.clone();
        let io_socket_path_for_thread = io_socket_path.clone();
        let resize_socket_path_for_thread = resize_socket_path.clone();
        let session_id_for_thread = session_id.clone();
        let join_handle = std::thread::spawn(move || {
            if let Err(err) = daemon.serve_exec_session(
                &request,
                &session_id_for_thread,
                &io_socket_path_for_thread,
                resize_socket_path_for_thread.as_deref(),
            ) {
                error!(
                    "exec session {} for container {} failed: {}",
                    session_id_for_thread, daemon.container_id, err
                );
            }
        });

        self.exec_sessions.lock().unwrap().insert(
            session_id.clone(),
            ExecSessionHandle {
                io_socket_path: io_socket_path.clone(),
                resize_socket_path: resize_socket_path.clone(),
                join_handle: Some(join_handle),
            },
        );

        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if io_socket_path.exists()
                && resize_socket_path
                    .as_ref()
                    .map(|path| path.exists())
                    .unwrap_or(true)
            {
                return Ok(OpenExecSessionResponse {
                    session_id,
                    io_socket_path,
                    resize_socket_path,
                });
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        Err(anyhow::anyhow!(
            "exec session sockets were not created before deadline for container {}",
            self.container_id
        ))
    }

    fn serve_exec_session(
        &self,
        request: &OpenExecSessionRequest,
        session_id: &str,
        io_socket_path: &Path,
        resize_socket_path: Option<&Path>,
    ) -> Result<()> {
        let mut io_manager = IoManager::new();
        io_manager.configure(IoConfig {
            terminal: request.tty,
            attach_socket: Some(io_socket_path.to_path_buf()),
            resize_socket: resize_socket_path.map(PathBuf::from),
            reopen_socket: None,
            stdout: None,
            stderr: None,
            stdin: None,
            journald: None,
            no_sync_log: true,
            io_uid: self.io_uid,
            io_gid: self.io_gid,
            max_log_line_size: self.max_container_log_line_size,
        })?;
        io_manager.start_attach_server()?;
        io_manager.start_resize_server()?;
        self.record_exec_event(&format!(
            "session-open:{}:{:?}",
            session_id, request.command
        ))?;

        let result = if request.tty {
            self.serve_tty_exec_session(request, &io_manager)
        } else {
            self.serve_pipe_exec_session(request, &io_manager)
        };

        if let Err(err) = io_manager.shutdown() {
            warn!(
                "failed to shutdown exec session {} IO manager for {}: {}",
                session_id, self.container_id, err
            );
        }
        let _ = std::fs::remove_dir_all(self.exec_session_dir(session_id));
        self.exec_sessions.lock().unwrap().remove(session_id);
        self.record_exec_event(&format!(
            "session-close:{}:{}",
            session_id,
            if result.is_ok() { "ok" } else { "err" }
        ))?;
        result
    }

    fn serve_pipe_exec_session(
        &self,
        request: &OpenExecSessionRequest,
        io_manager: &IoManager,
    ) -> Result<()> {
        let mut command = self.runtime_command();
        command.arg("exec");
        if request.stdin {
            command.arg("-i");
        }
        command.arg(&request.container_id);
        for arg in &request.command {
            command.arg(arg);
        }
        crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_std_command(
            &mut command,
            request.exec_cpu_affinity,
        );
        command.stdin(if request.stdin {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        });
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());
        let mut child = command
            .spawn()
            .context("Failed to spawn exec session command")?;

        let stdout_handle = child.stdout.take().map(|stdout| {
            let io_manager = io_manager.clone();
            std::thread::spawn(move || {
                let mut stdout = stdout;
                let mut buffer = [0u8; 8192];
                loop {
                    match std::io::Read::read(&mut stdout, &mut buffer) {
                        Ok(0) => {
                            let _ = io_manager.finish_stdout();
                            break;
                        }
                        Ok(n) => {
                            if let Err(err) = io_manager.write_stdout(&buffer[..n]) {
                                debug!("exec stdout pump stopped: {}", err);
                                let _ = io_manager.finish_stdout();
                                break;
                            }
                        }
                        Err(err) => {
                            debug!("exec stdout pump stopped: {}", err);
                            let _ = io_manager.finish_stdout();
                            break;
                        }
                    }
                }
            })
        });
        let stderr_handle = child.stderr.take().map(|stderr| {
            let io_manager = io_manager.clone();
            std::thread::spawn(move || {
                let mut stderr = stderr;
                let mut buffer = [0u8; 8192];
                loop {
                    match std::io::Read::read(&mut stderr, &mut buffer) {
                        Ok(0) => {
                            let _ = io_manager.finish_stderr();
                            break;
                        }
                        Ok(n) => {
                            if let Err(err) = io_manager.write_stderr(&buffer[..n]) {
                                debug!("exec stderr pump stopped: {}", err);
                                let _ = io_manager.finish_stderr();
                                break;
                            }
                        }
                        Err(err) => {
                            debug!("exec stderr pump stopped: {}", err);
                            let _ = io_manager.finish_stderr();
                            break;
                        }
                    }
                }
            })
        });

        let stop_stdin = Arc::new(AtomicBool::new(false));
        let stdin_handle = child.stdin.take().map(|mut stdin| {
            let io_manager = io_manager.clone();
            let stop_stdin = Arc::clone(&stop_stdin);
            std::thread::spawn(move || loop {
                if stop_stdin.load(Ordering::Relaxed) {
                    break;
                }
                match io_manager.read_stdin() {
                    Ok(data) if !data.is_empty() => {
                        if let Err(err) = std::io::Write::write_all(&mut stdin, &data) {
                            debug!("exec stdin pump stopped: {}", err);
                            break;
                        }
                        let _ = std::io::Write::flush(&mut stdin);
                    }
                    Ok(_) => std::thread::sleep(Duration::from_millis(25)),
                    Err(err) => {
                        debug!("exec stdin pump stopped: {}", err);
                        break;
                    }
                }
            })
        });

        let status = child.wait().context("Failed to wait for exec session")?;
        stop_stdin.store(true, Ordering::Relaxed);
        if let Some(handle) = stdout_handle {
            let _ = handle.join();
        }
        if let Some(handle) = stderr_handle {
            let _ = handle.join();
        }
        if let Some(handle) = stdin_handle {
            let _ = handle.join();
        }
        if !status.success() {
            return Err(anyhow::anyhow!(
                "exec session exited with status {:?}",
                status.code()
            ));
        }
        Ok(())
    }

    fn serve_tty_exec_session(
        &self,
        request: &OpenExecSessionRequest,
        io_manager: &IoManager,
    ) -> Result<()> {
        let pty =
            nix::pty::openpty(None, None).context("Failed to allocate PTY for exec session")?;
        let master = unsafe { File::from_raw_fd(pty.master) };
        let slave = unsafe { File::from_raw_fd(pty.slave) };
        let slave_stdin = slave.try_clone()?;
        let slave_stdout = slave.try_clone()?;
        let slave_stderr = slave;
        let slave_fd = slave_stderr.as_raw_fd();

        let mut command = self.runtime_command();
        command
            .arg("exec")
            .arg("-i")
            .arg("-t")
            .arg(&request.container_id);
        for arg in &request.command {
            command.arg(arg);
        }
        crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_std_command(
            &mut command,
            request.exec_cpu_affinity,
        );
        command.stdin(std::process::Stdio::from(slave_stdin));
        command.stdout(std::process::Stdio::from(slave_stdout));
        command.stderr(std::process::Stdio::from(slave_stderr));
        unsafe {
            command.pre_exec(move || {
                if nix::unistd::setsid().is_err() {
                    return Err(std::io::Error::last_os_error());
                }
                if nix::libc::ioctl(slave_fd, nix::libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        io_manager.start_console_bridge(master)?;
        let status = command
            .spawn()
            .context("Failed to spawn tty exec session command")?
            .wait()
            .context("Failed to wait for tty exec session")?;
        if !status.success() {
            return Err(anyhow::anyhow!(
                "tty exec session exited with status {:?}",
                status.code()
            ));
        }
        Ok(())
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
        self.work_dir.join(&self.container_id)
    }

    fn attach_socket_container_dir(&self) -> PathBuf {
        match self.attach_socket_dir.as_ref() {
            Some(root) => root.join(&self.container_id),
            None => self.shim_dir(),
        }
    }

    fn cleanup_attach_socket_directory(&self) {
        let Some(root) = self.attach_socket_dir.as_ref() else {
            return;
        };
        if root == &self.work_dir {
            return;
        }
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
        let xdg_runtime_dir = std::env::var("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/run/user/0"));
        if !xdg_runtime_dir.as_os_str().is_empty() {
            cmd.env("XDG_RUNTIME_DIR", xdg_runtime_dir);
        }
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

    fn create_io_pipe() -> Result<(File, File)> {
        let mut fds = [0; 2];
        let result = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
        if result < 0 {
            return Err(anyhow::anyhow!(
                "failed to create container IO pipe: {}",
                std::io::Error::last_os_error()
            ));
        }
        let read = unsafe { File::from_raw_fd(fds[0]) };
        let write = unsafe { File::from_raw_fd(fds[1]) };
        Ok((read, write))
    }

    fn spawn_pipe_pump(
        mut pipe: File,
        io_manager: IoManager,
        stream: &'static str,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let mut buffer = [0u8; 8192];
            loop {
                match std::io::Read::read(&mut pipe, &mut buffer) {
                    Ok(0) => {
                        Self::finish_pipe_stream(&io_manager, stream);
                        break;
                    }
                    Ok(n) => {
                        let result = match stream {
                            "stdout" => io_manager.write_stdout(&buffer[..n]),
                            "stderr" => io_manager.write_stderr(&buffer[..n]),
                            _ => unreachable!("unsupported pipe stream {}", stream),
                        };
                        if let Err(e) = result {
                            debug!("{} pump stopped: {}", stream, e);
                            Self::finish_pipe_stream(&io_manager, stream);
                            break;
                        }
                    }
                    Err(e) => {
                        debug!("{} pump stopped: {}", stream, e);
                        Self::finish_pipe_stream(&io_manager, stream);
                        break;
                    }
                }
            }
        })
    }

    fn finish_pipe_stream(io_manager: &IoManager, stream: &str) {
        let result = match stream {
            "stdout" => io_manager.finish_stdout(),
            "stderr" => io_manager.finish_stderr(),
            _ => unreachable!("unsupported pipe stream {}", stream),
        };
        if let Err(e) = result {
            debug!("failed to finish {} stream: {}", stream, e);
        }
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

    fn setup_io_once(&self) -> Result<()> {
        let current = *self.task_state.lock().unwrap();
        if !matches!(current, DaemonTaskState::Init) {
            return Ok(());
        }
        let mut daemon = self.clone();
        daemon.setup_io()
    }

    fn open_attach_stream_internal(
        &self,
        request: &OpenAttachStreamRequest,
    ) -> Result<OpenAttachStreamResponse> {
        if request.container_id != self.container_id {
            return Err(anyhow::anyhow!(
                "attach stream request container {} does not match shim container {}",
                request.container_id,
                self.container_id
            ));
        }
        self.setup_io_once()?;
        let stream_id = {
            let mut next = self.next_attach_stream_id.lock().unwrap();
            let stream_id = format!("attach-{}-{}", request.container_id, *next);
            *next += 1;
            stream_id
        };
        self.attach_streams
            .lock()
            .unwrap()
            .insert(stream_id.clone(), AttachStreamHandle { tty: request.tty });
        Ok(OpenAttachStreamResponse {
            stream_id,
            io_socket_path: self.attach_socket_container_dir().join("attach.sock"),
            resize_socket_path: request
                .tty
                .then(|| self.attach_socket_container_dir().join("resize.sock")),
        })
    }

    fn close_attach_stream_internal(
        &self,
        request: &crate::shim_rpc::CloseAttachStreamRequest,
    ) -> Result<()> {
        if request.container_id != self.container_id {
            return Err(anyhow::anyhow!(
                "attach stream close container {} does not match shim container {}",
                request.container_id,
                self.container_id
            ));
        }
        let removed = self
            .attach_streams
            .lock()
            .unwrap()
            .remove(&request.stream_id);
        if removed.is_none() {
            return Err(anyhow::anyhow!(
                "attach stream {} for container {} is not open",
                request.stream_id,
                request.container_id
            ));
        }
        Ok(())
    }

    fn resize_attach_pty_internal(
        &self,
        request: &crate::shim_rpc::ResizeAttachPtyRequest,
    ) -> Result<()> {
        if request.container_id != self.container_id {
            return Err(anyhow::anyhow!(
                "attach resize container {} does not match shim container {}",
                request.container_id,
                self.container_id
            ));
        }
        let stream_id = request
            .stream_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("attach resize requires stream id"))?;
        let stream = self
            .attach_streams
            .lock()
            .unwrap()
            .get(stream_id)
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "attach stream {} for container {} is not open",
                    stream_id,
                    request.container_id
                )
            })?;
        if !stream.tty {
            return Err(anyhow::anyhow!(
                "attach stream {} for container {} is not a TTY stream",
                stream_id,
                request.container_id
            ));
        }
        self.io_manager
            .apply_terminal_resize(request.width, request.height)
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

        let mut create_cmd = self.runtime_command();
        create_cmd.arg("create").arg("--bundle").arg(&self.bundle);
        if self.no_pivot {
            create_cmd.arg("--no-pivot");
        }
        if self.no_new_keyring {
            create_cmd.arg("--no-new-keyring");
        }
        if tty {
            create_cmd.arg("--console-socket").arg(&console_socket_path);
        }
        create_cmd.arg(&self.container_id);

        let output = create_cmd
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

        let (stdout_read, stdout_write) = Self::create_io_pipe()?;
        let (stderr_read, stderr_write) = Self::create_io_pipe()?;
        let mut cmd = self.runtime_command();
        cmd.arg("run").arg("--bundle").arg(&self.bundle);
        if self.no_pivot {
            cmd.arg("--no-pivot");
        }
        if self.no_new_keyring {
            cmd.arg("--no-new-keyring");
        }
        cmd.arg(&self.container_id);

        if container_state.stdin {
            cmd.stdin(std::process::Stdio::piped());
        } else {
            cmd.stdin(std::process::Stdio::null());
        }
        cmd.stdout(Stdio::from(stdout_write));
        cmd.stderr(Stdio::from(stderr_write));

        let mut child = cmd.spawn().context("Failed to execute runc run")?;
        drop(cmd);
        info!(
            "Container {} started via foreground runc run (stdin={}, stdin_once={})",
            self.container_id, container_state.stdin, container_state.stdin_once
        );

        let stdout_handle = Some(Self::spawn_pipe_pump(
            stdout_read,
            self.io_manager.clone(),
            "stdout",
        ));
        let stderr_handle = Some(Self::spawn_pipe_pump(
            stderr_read,
            self.io_manager.clone(),
            "stderr",
        ));

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

impl Daemon {
    fn wait_for_exit_code(&self, request: &WaitProcessRequest) -> Result<Option<i32>> {
        if let Some(exit_code) = *self.exit_code.lock().unwrap() {
            return Ok(Some(exit_code));
        }

        let deadline = request
            .timeout_ms
            .map(|timeout| Instant::now() + Duration::from_millis(timeout));
        loop {
            if let Some(exit_code) = *self.exit_code.lock().unwrap() {
                return Ok(Some(exit_code));
            }
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    return Ok(None);
                }
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn kill_task_internal(&self, request: &KillTaskRequest) -> Result<()> {
        let mut args = vec![
            "kill",
            request.container_id.as_str(),
            request.signal.as_str(),
        ];
        if request.all {
            args.insert(1, "--all");
        }
        let output = self
            .runtime_command()
            .args(&args)
            .output()
            .with_context(|| {
                format!(
                    "Failed to execute runtime kill for container {}",
                    request.container_id
                )
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow::anyhow!(
                "failed to kill container {}: {}",
                request.container_id,
                stderr
            ));
        }
        Ok(())
    }

    fn delete_task_internal(&self, request: &DeleteTaskRequest) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        if matches!(
            *self.task_state.lock().unwrap(),
            DaemonTaskState::Running | DaemonTaskState::Paused
        ) {
            let _ = self.kill_task_internal(&KillTaskRequest {
                container_id: request.container_id.clone(),
                signal: "KILL".to_string(),
                all: true,
            });
            let _ = self.wait_for_exit_code(&WaitProcessRequest {
                container_id: request.container_id.clone(),
                timeout_ms: Some(2_000),
            });
        }

        if let Some(handle) = self.task_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
        let sessions: Vec<ExecSessionHandle> = self
            .exec_sessions
            .lock()
            .unwrap()
            .drain()
            .map(|(_, handle)| handle)
            .collect();
        for mut session in sessions {
            let _ = std::fs::remove_file(&session.io_socket_path);
            if let Some(path) = session.resize_socket_path.as_ref() {
                let _ = std::fs::remove_file(path);
            }
            if let Some(handle) = session.join_handle.take() {
                let _ = handle.join();
            }
        }
        self.close_all_attach_streams();
        if let Err(err) = self.io_manager.shutdown() {
            warn!("Failed to shutdown IO manager during delete: {}", err);
        }
        self.cleanup_attach_socket_directory();
        self.cleanup_rootfs_handle(request)?;
        self.set_task_state(DaemonTaskState::Deleted);
        Ok(())
    }

    fn exec_process_internal(&self, request: &ExecProcessRequest) -> Result<ExecProcessResponse> {
        if request.command.is_empty() {
            return Err(anyhow::anyhow!("exec command must not be empty"));
        }
        self.record_exec_event(&format!("start:{:?}", request.command))?;
        let mut cmd = self.runtime_command();
        cmd.arg("exec");
        if request.tty {
            cmd.arg("-t");
        }
        cmd.arg(&request.container_id);
        for arg in &request.command {
            cmd.arg(arg);
        }
        crate::runtime::RuncRuntime::apply_exec_cpu_affinity_to_std_command(
            &mut cmd,
            request.exec_cpu_affinity,
        );
        cmd.stdin(std::process::Stdio::null());
        if request.capture_output {
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::piped());
        } else {
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
        }

        let mut child = cmd.spawn().context("Failed to execute runtime exec")?;
        let stdout_task = child.stdout.take().map(|mut stdout| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = tx.send(std::io::Read::read_to_end(&mut stdout, &mut buf).map(|_| buf));
            });
            rx
        });
        let stderr_task = child.stderr.take().map(|mut stderr| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = tx.send(std::io::Read::read_to_end(&mut stderr, &mut buf).map(|_| buf));
            });
            rx
        });

        let deadline = request
            .timeout_ms
            .map(|timeout| Instant::now() + Duration::from_millis(timeout));
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .context("Failed to poll runtime exec status")?
            {
                break status;
            }
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait().context("Failed to wait for killed exec")?;
                    return Err(anyhow::anyhow!(
                        "exec timed out after {}ms in container {}",
                        request.timeout_ms.unwrap_or_default(),
                        request.container_id
                    ));
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        let drain_deadline = request
            .io_drain_timeout_ms
            .map(|timeout| Instant::now() + Duration::from_millis(timeout));
        let stdout = wait_exec_reader(stdout_task, drain_deadline, "stdout")?;
        let stderr = wait_exec_reader(stderr_task, drain_deadline, "stderr")?;

        let exit_code = status
            .code()
            .unwrap_or_else(|| status.signal().map(|signal| 128 + signal).unwrap_or(1));
        self.record_exec_event(&format!("exit:{}:{:?}", exit_code, request.command))?;
        if !status.success() {
            let stderr_message = String::from_utf8_lossy(&stderr).trim().to_string();
            return Err(anyhow::anyhow!(
                "exec failed in container {} with exit code {}: {}",
                request.container_id,
                exit_code,
                stderr_message
            ));
        }
        Ok(ExecProcessResponse {
            exit_code,
            stdout,
            stderr,
        })
    }

    fn update_resources_internal(&self, request: &UpdateResourcesRequest) -> Result<()> {
        let resources: crate::proto::runtime::v1::LinuxContainerResources =
            request.resources.clone().into();
        let limits = RuncRuntime::cri_to_limits(&resources);
        let cgroup_manager = crate::cgroups::CgroupManager::new(request.container_id.clone())
            .context("Failed to create cgroup manager")?;
        cgroup_manager
            .set_resources(&limits)
            .context("Failed to set cgroup resources")?;
        Ok(())
    }

    fn checkpoint_task_internal(&self, request: &CheckpointTaskRequest) -> Result<()> {
        std::fs::create_dir_all(&request.image_path).with_context(|| {
            format!(
                "Failed to create checkpoint image directory {}",
                request.image_path.display()
            )
        })?;
        std::fs::create_dir_all(&request.work_path).with_context(|| {
            format!(
                "Failed to create checkpoint work directory {}",
                request.work_path.display()
            )
        })?;
        let image_path = request.image_path.to_string_lossy().to_string();
        let work_path = request.work_path.to_string_lossy().to_string();
        let mut checkpoint_args = vec![
            "checkpoint",
            "--file-locks",
            "--image-path",
            image_path.as_str(),
            "--work-path",
            work_path.as_str(),
            "--leave-running",
            request.container_id.as_str(),
        ];
        let output = self
            .runtime_command()
            .args(&checkpoint_args)
            .output()
            .context("Failed to execute runtime checkpoint")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow::anyhow!(
                "failed to checkpoint container {}: {}",
                request.container_id,
                stderr
            ));
        }
        checkpoint_args.clear();
        Ok(())
    }

    fn restore_task_internal(&self, request: &RestoreTaskRequest) -> Result<()> {
        self.setup_io_once()?;
        std::fs::create_dir_all(&request.work_path).with_context(|| {
            format!(
                "Failed to create restore work directory {}",
                request.work_path.display()
            )
        })?;
        let mut command = self.runtime_command();
        command
            .arg("restore")
            .arg("-d")
            .arg("--image-path")
            .arg(&request.image_path)
            .arg("--work-path")
            .arg(&request.work_path)
            .arg("--bundle")
            .arg(&request.bundle_path);
        if !request.criu_path.as_os_str().is_empty() {
            command.arg("--criu").arg(&request.criu_path);
        }
        if request.no_pivot {
            command.arg("--no-pivot");
        }
        command.arg(&request.container_id);
        let output = command
            .output()
            .context("Failed to execute runtime restore")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow::anyhow!(
                "failed to restore container {}: {}",
                request.container_id,
                stderr
            ));
        }
        self.set_task_state(DaemonTaskState::Running);
        Ok(())
    }
}

impl ShimRpcHandler for Daemon {
    fn handle_request(&self, request: ShimRpcRequest) -> Result<ShimRpcResponse> {
        match request {
            ShimRpcRequest::Ping => Ok(ShimRpcResponse::Empty),
            ShimRpcRequest::CreateTask(request) => {
                self.apply_rootfs_override(&request.rootfs_path)?;
                self.record_rootfs_handle(&request)?;
                self.setup_io_once()?;
                self.set_task_state(DaemonTaskState::Created);
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::StartTask(StartTaskRequest { .. }) => {
                self.spawn_task_runner()?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::ExecProcess(request) => Ok(ShimRpcResponse::ExecProcess(
                self.exec_process_internal(&request)?,
            )),
            ShimRpcRequest::OpenExecSession(request) => Ok(ShimRpcResponse::OpenExecSession(
                self.open_exec_session_internal(&request)?,
            )),
            ShimRpcRequest::OpenAttachStream(request) => Ok(ShimRpcResponse::OpenAttachStream(
                self.open_attach_stream_internal(&request)?,
            )),
            ShimRpcRequest::CloseAttachStream(request) => {
                self.close_attach_stream_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::WaitProcess(request) => {
                Ok(ShimRpcResponse::WaitProcess(WaitProcessResponse {
                    exit_code: self.wait_for_exit_code(&request)?,
                }))
            }
            ShimRpcRequest::KillTask(request) => {
                self.kill_task_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::DeleteTask(request) => {
                self.delete_task_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::UpdateResources(request) => {
                self.update_resources_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::CheckpointTask(request) => {
                self.checkpoint_task_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::RestoreTask(request) => {
                self.restore_task_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::ReopenLog(ReopenLogRequest { .. }) => {
                self.io_manager.reopen_log_file()?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::ResizePty(ResizePtyRequest { width, height, .. }) => {
                self.io_manager.apply_terminal_resize(width, height)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::ResizeAttachPty(request) => {
                self.resize_attach_pty_internal(&request)?;
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::Status(StatusRequest { .. }) => {
                Ok(ShimRpcResponse::Status(self.task_status()))
            }
            ShimRpcRequest::PauseTask(PauseTaskRequest { container_id }) => {
                let output = self
                    .runtime_command()
                    .args(["pause", container_id.as_str()])
                    .output()
                    .context("Failed to execute runtime pause")?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                    return Err(anyhow::anyhow!(
                        "failed to pause container {}: {}",
                        container_id,
                        stderr
                    ));
                }
                self.set_task_state(DaemonTaskState::Paused);
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::ResumeTask(ResumeTaskRequest { container_id }) => {
                let output = self
                    .runtime_command()
                    .args(["resume", container_id.as_str()])
                    .output()
                    .context("Failed to execute runtime resume")?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                    return Err(anyhow::anyhow!(
                        "failed to resume container {}: {}",
                        container_id,
                        stderr
                    ));
                }
                self.set_task_state(DaemonTaskState::Running);
                Ok(ShimRpcResponse::Empty)
            }
            ShimRpcRequest::ContainerPid(StatusRequest { .. }) => Ok(
                ShimRpcResponse::ContainerPid(*self.container_pid.lock().unwrap()),
            ),
        }
    }
}

fn wait_exec_reader(
    reader: Option<std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>>,
    deadline: Option<Instant>,
    stream_name: &str,
) -> Result<Vec<u8>> {
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };

    match deadline {
        Some(deadline) => {
            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow::anyhow!("exec {} drain timed out", stream_name));
            }
            reader
                .recv_timeout(deadline.saturating_duration_since(now))
                .map_err(|_| anyhow::anyhow!("exec {} drain timed out", stream_name))?
                .context("failed to read exec output")
        }
        None => reader
            .recv()
            .map_err(|_| anyhow::anyhow!("failed to join {} reader for exec", stream_name))?
            .context("failed to read exec output"),
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
mod tests;
