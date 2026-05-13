use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, error};
use serde::{Deserialize, Serialize};

use super::proto::{ShimRpcRequest, ShimRpcResponse};

#[derive(Debug, Serialize, Deserialize)]
struct RpcEnvelope<T> {
    payload: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcResultEnvelope {
    ok: bool,
    payload: Option<ShimRpcResponse>,
    error: Option<String>,
}

pub trait ShimRpcHandler: Send + Sync + 'static {
    fn handle_request(&self, request: ShimRpcRequest) -> Result<ShimRpcResponse>;
}

pub fn serve(
    socket_path: &Path,
    running: Arc<AtomicBool>,
    handler: Arc<dyn ShimRpcHandler>,
) -> Result<()> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create shim RPC socket directory {}",
                parent.display()
            )
        })?;
    }
    cleanup_socket(socket_path);
    let listener = UnixListener::bind(socket_path).with_context(|| {
        format!(
            "failed to bind shim RPC socket {}",
            socket_path.display()
        )
    })?;
    listener
        .set_nonblocking(true)
        .context("failed to configure shim RPC listener as nonblocking")?;

    while running.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(err) = handle_stream(stream, handler.as_ref()) {
                    error!("shim RPC request failed: {}", err);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(err) if running.load(Ordering::Relaxed) => {
                return Err(err).context("shim RPC listener accept failed");
            }
            Err(_) => break,
        }
    }

    cleanup_socket(socket_path);
    Ok(())
}

fn handle_stream(mut stream: UnixStream, handler: &dyn ShimRpcHandler) -> Result<()> {
    let mut request = Vec::new();
    stream
        .read_to_end(&mut request)
        .context("failed to read shim RPC request")?;
    let envelope: RpcEnvelope<ShimRpcRequest> =
        serde_json::from_slice(&request).context("failed to decode shim RPC request")?;

    let response = match handler.handle_request(envelope.payload) {
        Ok(payload) => RpcResultEnvelope {
            ok: true,
            payload: Some(payload),
            error: None,
        },
        Err(err) => {
            debug!("shim RPC request returned an error: {}", err);
            RpcResultEnvelope {
                ok: false,
                payload: None,
                error: Some(err.to_string()),
            }
        }
    };

    let encoded =
        serde_json::to_vec(&response).context("failed to encode shim RPC response payload")?;
    stream
        .write_all(&encoded)
        .context("failed to write shim RPC response")?;
    Ok(())
}

fn cleanup_socket(socket_path: &Path) {
    if let Err(err) = std::fs::remove_file(socket_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            debug!(
                "failed to remove stale shim RPC socket {}: {}",
                socket_path.display(),
                err
            );
        }
    }
}

pub fn default_task_socket_path(work_dir: &Path, container_id: &str) -> PathBuf {
    work_dir.join(container_id).join("task.sock")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    use tempfile::tempdir;

    use crate::shim_rpc::client::ShimRpcClient;

    struct TestHandler;

    impl ShimRpcHandler for TestHandler {
        fn handle_request(&self, request: ShimRpcRequest) -> Result<ShimRpcResponse> {
            match request {
                ShimRpcRequest::Ping => Ok(ShimRpcResponse::Empty),
                _ => Err(anyhow::anyhow!("unsupported request")),
            }
        }
    }

    #[test]
    fn default_task_socket_path_uses_work_dir_and_container_id() {
        let path = default_task_socket_path(Path::new("/var/run/crius/shims"), "abc123");
        assert_eq!(path, PathBuf::from("/var/run/crius/shims/abc123/task.sock"));
    }

    #[test]
    fn shim_rpc_server_handles_ping_round_trip() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("task.sock");
        let running = Arc::new(AtomicBool::new(true));
        let running_for_thread = running.clone();

        let handle = std::thread::spawn(move || {
            serve(&socket_path, running_for_thread, Arc::new(TestHandler)).unwrap();
        });

        let client = ShimRpcClient::new(temp_dir.path().join("task.sock"), Duration::from_secs(1));
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if matches!(client.request(ShimRpcRequest::Ping), Ok(ShimRpcResponse::Empty)) {
                break;
            }
            if Instant::now() >= deadline {
                panic!("shim RPC server did not answer ping before deadline");
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        running.store(false, Ordering::Relaxed);
        let _ = std::os::unix::net::UnixStream::connect(client.socket_path());
        handle.join().unwrap();
    }
}
