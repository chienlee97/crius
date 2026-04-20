//! # crius Rust Implementation
//!
//! A Rust implementation of the Kubernetes Container Runtime Interface (CRI).

pub mod attach;
pub mod cgroups;
pub mod config;
pub mod error;
pub mod image;
pub mod metrics;
pub mod network;
pub mod nri;
pub mod oci;
pub mod pod;
pub mod proto;
pub mod rootless;
pub mod runtime;
pub mod security;
pub mod server;
pub mod storage;
pub mod streaming;
pub mod utils;
pub use crate::proto::nri as nri_proto;

#[cfg(feature = "shim")]
pub mod shim;

/// 预导入常用模块
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use anyhow::Context;
    pub use log::{debug, error, info, trace, warn};
}
