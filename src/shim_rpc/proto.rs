use std::path::PathBuf;

use crate::image::snapshotter::RootfsHandle;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Init,
    Created,
    Running,
    Paused,
    Stopped,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskRequest {
    pub container_id: String,
    pub rootfs_path: PathBuf,
    #[serde(default)]
    pub snapshot_key: Option<String>,
    #[serde(default)]
    pub mount_options: Vec<String>,
    #[serde(default)]
    pub rootfs: Option<RootfsHandle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartTaskRequest {
    pub container_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecProcessRequest {
    pub container_id: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub capture_output: bool,
    pub timeout_ms: Option<u64>,
    pub io_drain_timeout_ms: Option<u64>,
    pub exec_cpu_affinity: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecProcessResponse {
    pub exit_code: i32,
    #[serde(default)]
    pub stdout: Vec<u8>,
    #[serde(default)]
    pub stderr: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenExecSessionRequest {
    pub container_id: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub exec_cpu_affinity: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenExecSessionResponse {
    pub session_id: String,
    pub io_socket_path: PathBuf,
    pub resize_socket_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAttachStreamRequest {
    pub container_id: String,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAttachStreamResponse {
    pub stream_id: String,
    pub io_socket_path: PathBuf,
    pub resize_socket_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseAttachStreamRequest {
    pub container_id: String,
    pub stream_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResizeAttachPtyRequest {
    pub container_id: String,
    pub stream_id: Option<String>,
    pub width: u16,
    pub height: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitProcessRequest {
    pub container_id: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitProcessResponse {
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillTaskRequest {
    pub container_id: String,
    pub signal: String,
    pub all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTaskRequest {
    pub container_id: String,
    #[serde(default)]
    pub snapshot_key: Option<String>,
    #[serde(default)]
    pub rootfs_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShimLinuxResources {
    pub cpu_shares: i64,
    pub cpu_quota: i64,
    pub cpu_period: i64,
    pub cpuset_cpus: String,
    pub cpuset_mems: String,
    pub memory_limit_in_bytes: i64,
    pub memory_swap_limit_in_bytes: i64,
}

impl From<&crate::proto::runtime::v1::LinuxContainerResources> for ShimLinuxResources {
    fn from(value: &crate::proto::runtime::v1::LinuxContainerResources) -> Self {
        Self {
            cpu_shares: value.cpu_shares,
            cpu_quota: value.cpu_quota,
            cpu_period: value.cpu_period,
            cpuset_cpus: value.cpuset_cpus.clone(),
            cpuset_mems: value.cpuset_mems.clone(),
            memory_limit_in_bytes: value.memory_limit_in_bytes,
            memory_swap_limit_in_bytes: value.memory_swap_limit_in_bytes,
        }
    }
}

impl From<ShimLinuxResources> for crate::proto::runtime::v1::LinuxContainerResources {
    fn from(value: ShimLinuxResources) -> Self {
        crate::proto::runtime::v1::LinuxContainerResources {
            cpu_period: value.cpu_period,
            cpu_quota: value.cpu_quota,
            cpu_shares: value.cpu_shares,
            memory_limit_in_bytes: value.memory_limit_in_bytes,
            oom_score_adj: 0,
            cpuset_cpus: value.cpuset_cpus,
            cpuset_mems: value.cpuset_mems,
            hugepage_limits: Vec::new(),
            unified: std::collections::HashMap::new(),
            memory_swap_limit_in_bytes: value.memory_swap_limit_in_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResourcesRequest {
    pub container_id: String,
    pub resources: ShimLinuxResources,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointTaskRequest {
    pub container_id: String,
    pub image_path: PathBuf,
    pub work_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreTaskRequest {
    pub container_id: String,
    pub image_path: PathBuf,
    pub work_path: PathBuf,
    pub bundle_path: PathBuf,
    pub criu_path: PathBuf,
    pub no_pivot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReopenLogRequest {
    pub container_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResizePtyRequest {
    pub container_id: String,
    pub width: u16,
    pub height: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusRequest {
    pub container_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseTaskRequest {
    pub container_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeTaskRequest {
    pub container_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub state: TaskState,
    pub pid: Option<i32>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", content = "payload", rename_all = "snake_case")]
pub enum ShimRpcRequest {
    Ping,
    CreateTask(CreateTaskRequest),
    StartTask(StartTaskRequest),
    ExecProcess(ExecProcessRequest),
    OpenExecSession(OpenExecSessionRequest),
    OpenAttachStream(OpenAttachStreamRequest),
    CloseAttachStream(CloseAttachStreamRequest),
    WaitProcess(WaitProcessRequest),
    KillTask(KillTaskRequest),
    DeleteTask(DeleteTaskRequest),
    UpdateResources(UpdateResourcesRequest),
    CheckpointTask(CheckpointTaskRequest),
    RestoreTask(RestoreTaskRequest),
    ReopenLog(ReopenLogRequest),
    ResizePty(ResizePtyRequest),
    ResizeAttachPty(ResizeAttachPtyRequest),
    Status(StatusRequest),
    PauseTask(PauseTaskRequest),
    ResumeTask(ResumeTaskRequest),
    ContainerPid(StatusRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum ShimRpcResponse {
    Empty,
    ExecProcess(ExecProcessResponse),
    OpenExecSession(OpenExecSessionResponse),
    OpenAttachStream(OpenAttachStreamResponse),
    WaitProcess(WaitProcessResponse),
    Status(StatusResponse),
    ContainerPid(Option<i32>),
}
