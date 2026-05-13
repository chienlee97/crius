use std::sync::Arc;
use std::time::Duration;

use crius::shim_rpc::{ShimRpcRequest, ShimRpcResponse};
use crius::test_support::{FakeShimRpcServer, PingShimHandler};

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
