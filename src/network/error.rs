use std::fmt;
use std::io;
use std::process::ExitStatus;

use thiserror::Error;

/// 网络模块错误类型
#[derive(Debug, Error)]
pub enum NetworkError {
    /// I/O 错误
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// JSON 序列化/反序列化错误
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// 命令执行错误
    #[error("Command '{command}' failed with status: {status}")]
    CommandExecutionError { command: String, status: ExitStatus },

    /// 不支持的配置
    #[error("Unsupported configuration: {0}")]
    UnsupportedConfig(String),

    /// 无效参数
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// 网络错误
    #[error("Network error: {0}")]
    Network(String),

    /// 其他错误
    #[error("{0}")]
    Other(String),
}

impl NetworkError {
    /// 创建一个新的命令执行错误
    pub fn command_error<S: Into<String>>(command: S, status: ExitStatus) -> Self {
        Self::CommandExecutionError {
            command: command.into(),
            status,
        }
    }

    /// 创建一个新的无效参数错误
    pub fn invalid_argument<S: Into<String>>(msg: S) -> Self {
        Self::InvalidArgument(msg.into())
    }
}

/// 结果类型别名
pub type Result<T> = std::result::Result<T, NetworkError>;
