#[path = "../shim/rpc/client.rs"]
pub mod client;
#[path = "../shim/rpc/proto.rs"]
pub mod proto;
#[path = "../shim/rpc/server.rs"]
pub mod server;

pub use client::ShimRpcClient;
pub use proto::{
    CheckpointTaskRequest, CreateTaskRequest, DeleteTaskRequest, ExecProcessRequest,
    ExecProcessResponse, KillTaskRequest, OpenExecSessionRequest, OpenExecSessionResponse,
    PauseTaskRequest, ReopenLogRequest, ResizePtyRequest, ResumeTaskRequest, ShimLinuxResources,
    ShimRpcRequest, ShimRpcResponse, StartTaskRequest, StatusRequest, StatusResponse, TaskState,
    UpdateResourcesRequest, WaitProcessRequest, WaitProcessResponse,
};
pub use server::default_task_socket_path;
