//! crius-shim 模块
//!
//! 提供OCI容器运行时shim功能

pub mod daemon;
pub mod io;
pub mod process;
pub mod subreaper;

pub mod rpc {
    pub use crate::shim_rpc::*;
}

pub use daemon::{Daemon, DaemonOptions};
pub use io::{IoConfig, IoManager};
