use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::proto::ShimRpcResponse;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RpcEnvelope<T> {
    payload: T,
}

impl<T> RpcEnvelope<T> {
    pub(super) fn new(payload: T) -> Self {
        Self { payload }
    }

    pub(super) fn into_payload(self) -> T {
        self.payload
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RpcResultEnvelope {
    ok: bool,
    payload: Option<ShimRpcResponse>,
    error: Option<String>,
}

impl RpcResultEnvelope {
    pub(super) fn success(payload: ShimRpcResponse) -> Self {
        Self {
            ok: true,
            payload: Some(payload),
            error: None,
        }
    }

    pub(super) fn failure(error: String) -> Self {
        Self {
            ok: false,
            payload: None,
            error: Some(error),
        }
    }

    pub(super) fn into_result(self) -> Result<ShimRpcResponse> {
        if self.ok {
            self.payload
                .ok_or_else(|| anyhow::anyhow!("shim RPC response was missing a payload"))
        } else {
            Err(anyhow::anyhow!(
                "{}",
                self.error
                    .unwrap_or_else(|| "shim RPC request failed".to_string())
            ))
        }
    }
}
