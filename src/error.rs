use std::fmt;
use std::io;
use thiserror::Error;

/// 自定义错误类型
#[derive(Error, Debug)]
pub enum Error {
    /// I/O错误
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    
    /// 配置错误
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// 运行时错误
    #[error("Runtime error: {0}")]
    Runtime(String),
    
    /// 镜像错误
    #[error("Image error: {0}")]
    Image(String),
    
    /// 网络错误
    #[error("Network error: {0}")]
    Network(String),
    
    /// 存储错误
    #[error("Storage error: {0}")]
    Storage(String),
    
    /// 其他错误
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// 结果类型别名
pub type Result<T> = std::result::Result<T, Error>;

// 为其他错误类型实现From trait
impl From<toml::de::Error> for Error {
    fn from(err: toml::de::Error) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Config(err.to_string())
    }
}
