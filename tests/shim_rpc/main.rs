use std::sync::Arc;
use std::time::Duration;

use crius::shim_rpc::{
    CreateTaskRequest, ShimRpcRequest, ShimRpcResponse, StartTaskRequest, StatusRequest,
    StatusResponse, TaskState,
};

#[path = "../common/mod.rs"]
mod common;
use common::{FakeShimRpcServer, PingShimHandler, ScriptedShimHandler};

#[test]
fn fake_shim_rpc_server_supports_ping_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let server =
        FakeShimRpcServer::start(dir.path().join("task.sock"), Arc::new(PingShimHandler)).unwrap();

    server.wait_until_ready(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        server.client().request(ShimRpcRequest::Ping).unwrap(),
        ShimRpcResponse::Empty
    ));
}

#[test]
fn scripted_shim_handler_records_requests_and_scripts_responses() {
    let dir = tempfile::tempdir().unwrap();
    let handler = ScriptedShimHandler::new();
    let server =
        FakeShimRpcServer::start(dir.path().join("task.sock"), Arc::new(handler.clone())).unwrap();

    server.wait_until_ready(Duration::from_secs(2)).unwrap();
    handler.clear_requests();
    handler.push_response(ShimRpcResponse::Status(StatusResponse {
        state: TaskState::Created,
        pid: Some(42),
        exit_code: None,
    }));

    let response = server
        .client()
        .request(ShimRpcRequest::Status(StatusRequest {
            container_id: "container-1".to_string(),
        }))
        .unwrap();

    assert!(matches!(
        response,
        ShimRpcResponse::Status(StatusResponse {
            state: TaskState::Created,
            pid: Some(42),
            exit_code: None,
        })
    ));
    let requests = handler.requests();
    assert_eq!(requests.len(), 1);
    assert!(matches!(
        &requests[0],
        ShimRpcRequest::Status(request) if request.container_id == "container-1"
    ));
}

#[test]
fn scripted_shim_handler_propagates_scripted_errors() {
    let dir = tempfile::tempdir().unwrap();
    let handler = ScriptedShimHandler::new();
    let server =
        FakeShimRpcServer::start(dir.path().join("task.sock"), Arc::new(handler.clone())).unwrap();

    server.wait_until_ready(Duration::from_secs(2)).unwrap();
    handler.clear_requests();
    handler.push_error("scripted start failure");

    let err = server
        .client()
        .request(ShimRpcRequest::StartTask(StartTaskRequest {
            container_id: "container-1".to_string(),
        }))
        .unwrap_err();

    assert!(err.to_string().contains("scripted start failure"));
    let requests = handler.requests();
    assert_eq!(requests.len(), 1);
    assert!(matches!(
        &requests[0],
        ShimRpcRequest::StartTask(request) if request.container_id == "container-1"
    ));
}

#[test]
fn create_task_request_carries_rootfs_snapshot_handle() {
    let dir = tempfile::tempdir().unwrap();
    let handler = ScriptedShimHandler::new();
    let server =
        FakeShimRpcServer::start(dir.path().join("task.sock"), Arc::new(handler.clone())).unwrap();

    server.wait_until_ready(Duration::from_secs(2)).unwrap();
    handler.clear_requests();
    handler.push_response(ShimRpcResponse::Empty);

    server
        .client()
        .request(ShimRpcRequest::CreateTask(CreateTaskRequest {
            container_id: "container-1".to_string(),
            rootfs_path: dir.path().join("rootfs"),
            snapshot_key: Some("snapshot-1".to_string()),
            mount_options: vec!["rw".to_string(), "rprivate".to_string()],
            rootfs: Some(crius::image::snapshotter::RootfsHandle::internal_path(
                "snapshot-1",
                "container",
                "container-1",
                dir.path().join("rootfs"),
                false,
            )),
        }))
        .unwrap();

    let requests = handler.requests();
    assert_eq!(requests.len(), 1);
    assert!(matches!(
        &requests[0],
        ShimRpcRequest::CreateTask(request)
            if request.container_id == "container-1"
                && request.snapshot_key.as_deref() == Some("snapshot-1")
                && request.rootfs_path == dir.path().join("rootfs")
                && request.mount_options == ["rw", "rprivate"]
                && request.rootfs.as_ref().and_then(|rootfs| rootfs.rootfs_path())
                    == Some(dir.path().join("rootfs").as_path())
    ));
}
