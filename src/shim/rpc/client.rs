use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::proto::{ShimRpcRequest, ShimRpcResponse};

#[derive(Debug, Clone)]
pub struct ShimRpcClient {
    socket_path: PathBuf,
    timeout: Duration,
}

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

impl ShimRpcClient {
    pub fn new(socket_path: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self {
            socket_path: socket_path.into(),
            timeout,
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn request(&self, payload: ShimRpcRequest) -> Result<ShimRpcResponse> {
        let mut stream = UnixStream::connect(&self.socket_path).with_context(|| {
            format!(
                "failed to connect to shim RPC socket {}",
                self.socket_path.display()
            )
        })?;
        stream
            .set_read_timeout(Some(self.timeout))
            .context("failed to configure shim RPC read timeout")?;
        stream
            .set_write_timeout(Some(self.timeout))
            .context("failed to configure shim RPC write timeout")?;

        let request = serde_json::to_vec(&RpcEnvelope { payload })
            .context("failed to encode shim RPC request")?;
        stream
            .write_all(&request)
            .context("failed to write shim RPC request")?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .context("failed to half-close shim RPC socket")?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .context("failed to read shim RPC response")?;
        let envelope: RpcResultEnvelope =
            serde_json::from_slice(&response).context("failed to decode shim RPC response")?;
        if envelope.ok {
            envelope
                .payload
                .ok_or_else(|| anyhow::anyhow!("shim RPC response was missing a payload"))
        } else {
            Err(anyhow::anyhow!(
                "{}",
                envelope
                    .error
                    .unwrap_or_else(|| "shim RPC request failed".to_string())
            ))
        }
    }
}
