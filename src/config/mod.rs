use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

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

    /// NRI 配置
    #[serde(default)]
    pub nri: NriConfig,
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

/// NRI 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NriConfig {
    /// 是否启用 NRI
    pub enable: bool,
    /// runtime 名称（上报给插件）
    pub runtime_name: String,
    /// runtime 版本（上报给插件）
    pub runtime_version: String,
    /// runtime 侧 NRI socket 路径
    pub socket_path: String,
    /// 预装插件目录
    pub plugin_path: String,
    /// 插件配置目录
    pub plugin_config_path: String,
    /// NRI blockio class 配置文件
    pub blockio_config_path: String,
    /// 全局允许的 NRI annotation 前缀
    pub allowed_annotation_prefixes: Vec<String>,
    /// 按 runtime handler 额外允许的 annotation 前缀
    pub runtime_allowed_annotation_prefixes: HashMap<String, Vec<String>>,
    /// 按 workload 激活 annotation 追加允许的 annotation 前缀
    pub workload_allowed_annotation_prefixes: Vec<NriAnnotationWorkloadConfig>,
    /// 插件注册超时（毫秒）
    pub registration_timeout_ms: i64,
    /// 插件请求超时（毫秒）
    pub request_timeout_ms: i64,
    /// 允许外部插件连接
    pub enable_external_connections: bool,
    /// 默认内建 validator 配置
    pub default_validator: NriDefaultValidatorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NriAnnotationWorkloadConfig {
    /// 用于激活 workload 规则的 annotation key
    pub activation_annotation: String,
    /// 可选的 annotation value 过滤；为空时只要求 key 存在
    pub activation_value: String,
    /// workload 追加允许的 annotation 前缀
    pub allowed_annotation_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NriDefaultValidatorConfig {
    /// 启用默认 validator
    pub enable: bool,
    /// 拒绝 OCI hooks 调整
    pub reject_oci_hook_adjustment: bool,
    /// 拒绝 runtime default seccomp 调整
    pub reject_runtime_default_seccomp_adjustment: bool,
    /// 拒绝 unconfined seccomp 调整
    pub reject_unconfined_seccomp_adjustment: bool,
    /// 拒绝 custom seccomp 调整
    pub reject_custom_seccomp_adjustment: bool,
    /// 拒绝 namespace 调整
    pub reject_namespace_adjustment: bool,
    /// 全局要求存在的插件列表
    pub required_plugins: Vec<String>,
    /// 容忍缺失 required plugins 的 annotation 名
    pub tolerate_missing_plugins_annotation: String,
}

impl Default for NriConfig {
    fn default() -> Self {
        Self {
            enable: false,
            runtime_name: "crius".to_string(),
            runtime_version: env!("CARGO_PKG_VERSION").to_string(),
            socket_path: "/run/crius/nri.sock".to_string(),
            plugin_path: "/opt/nri/plugins".to_string(),
            plugin_config_path: "/etc/nri/conf.d".to_string(),
            blockio_config_path: String::new(),
            allowed_annotation_prefixes: Vec::new(),
            runtime_allowed_annotation_prefixes: HashMap::new(),
            workload_allowed_annotation_prefixes: Vec::new(),
            registration_timeout_ms: 5000,
            request_timeout_ms: 2000,
            enable_external_connections: false,
            default_validator: NriDefaultValidatorConfig::default(),
        }
    }
}

impl Config {
    /// 从文件加载配置
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
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
            nri: NriConfig::default(),
        }
    }
}
