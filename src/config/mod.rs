use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs;

use crate::prelude::*;

/// 主配置结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// 根目录
    pub root: String,
    
    /// 运行时配置
    pub runtime: RuntimeConfig,
    
    /// 镜像配置
    pub image: ImageConfig,
    
    /// 网络配置
    pub network: NetworkConfig,
}

/// 运行时配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// 运行时类型
    pub runtime_type: String,
    
    /// 运行时路径
    pub runtime_path: String,
    
    /// 运行时根目录
    pub root: String,
}

/// 镜像配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageConfig {
    /// 镜像存储后端
    pub driver: String,
    
    /// 镜像存储路径
    pub root: String,
}

/// 网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// 网络插件
    pub plugin: String,
    
    /// 网络配置目录
    pub config_dir: String,
}

impl Config {
    /// 从文件加载配置
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
    
    /// 获取默认配置
    pub fn default() -> Self {
        Self {
            root: "/var/lib/crius".to_string(),
            runtime: RuntimeConfig {
                runtime_type: "runc".to_string(),
                runtime_path: "/usr/bin/runc".to_string(),
                root: "/run/crius".to_string(),
            },
            image: ImageConfig {
                driver: "overlay".to_string(),
                root: "/var/lib/containers/storage".to_string(),
            },
            network: NetworkConfig {
                plugin: "cni".to_string(),
                config_dir: "/etc/cni/net.d/".to_string(),
            },
        }
    }
}
