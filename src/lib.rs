#![recursion_limit = "512"]

//! # crius Rust Implementation
//!
//! A Rust implementation of the Kubernetes Container Runtime Interface (CRI).

#[cfg(test)]
extern crate self as crius;

pub mod attach;
pub mod cgroups;
pub mod config;
pub mod crs;
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
pub mod services;
pub mod shim_rpc;
pub mod state;
pub mod storage;
pub mod streaming;
pub mod trace_export;
pub mod utils;
pub use crate::proto::nri as nri_proto;

pub(crate) const CRS_RUN_ANNOTATION: &str = "io.crius.internal/crs-run";
pub(crate) const CRS_RUN_ANNOTATION_VALUE: &str = "true";

#[cfg(feature = "shim")]
pub mod shim;

/// 预导入常用模块
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use anyhow::Context;
    pub use log::{debug, error, info, trace, warn};
}
