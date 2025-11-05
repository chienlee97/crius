//! # crius Rust Implementation
//!
//! A Rust implementation of the Kubernetes Container Runtime Interface (CRI).

pub mod config;
pub mod error;
pub mod image;
pub mod network;
pub mod oci;
pub mod runtime;
pub mod server;
pub mod storage;
pub mod utils;

/// 预导入常用模块
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use anyhow::Context;
    pub use log::{debug, error, info, trace, warn};
}
