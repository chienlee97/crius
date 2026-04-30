use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::network::{CniConfig, MainIpPreference};
use crate::prelude::*;
use crate::streaming::StreamingConfig;

const MIN_CONTAINER_STOP_TIMEOUT_SECS: u32 = 30;
const DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS: u32 = 240;
const MIN_CONTAINER_CREATE_TIMEOUT_SECS: u32 = 30;
const DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES: u32 = 80 * 1024 * 1024;

/// 守护进程主配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// 持久化根目录。
    pub root: String,

    /// API 配置。
    pub api: ApiConfig,

    /// 运行时配置。
    pub runtime: RuntimeConfig,

    /// 镜像配置。
    pub image: ImageConfig,

    /// 网络配置。
    pub network: NetworkConfig,

    /// 日志配置。
    pub logging: LoggingConfig,

    /// NRI 配置。
    pub nri: NriConfig,
}

/// CRI API 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    /// CRI gRPC 监听地址。
    pub listen: String,
    /// 是否允许通过 TCP 暴露 CRI gRPC 服务。
    pub allow_tcp_service: bool,
    /// gRPC 最大发送消息大小（字节）。
    pub grpc_max_send_msg_size: u32,
    /// gRPC 最大接收消息大小（字节）。
    pub grpc_max_recv_msg_size: u32,
    /// 是否在 GetEvents 流中额外发送 pod-level lifecycle events。
    pub enable_pod_events: bool,
    /// ListPodSandboxMetrics 中包含哪些 pod-level metrics；支持 `all`、`cpu`、`memory`、`network`、`process`、`disk`。
    pub included_pod_metrics: Vec<String>,
    /// Pod/container stats 的缓存周期（秒）；0 表示按请求即时采集。
    pub stats_collection_period: u64,
    /// Pod sandbox metrics 的缓存周期（秒）；0 表示按请求即时采集。
    pub pod_sandbox_metrics_collection_period: u64,
    /// ExecSync 在主进程退出后等待 stdout/stderr EOF 的超时。
    #[serde(
        deserialize_with = "crate::streaming::deserialize_duration",
        serialize_with = "crate::streaming::serialize_duration"
    )]
    pub exec_sync_io_drain_timeout: std::time::Duration,
    /// 流式服务配置。
    pub streaming: StreamingConfig,
}

/// 守护进程日志配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// tracing filter/level。
    pub level: String,
    /// 可选的日志文件路径；为空时输出到 stderr。
    pub file: Option<String>,
    /// daemon/container 默认日志目录。
    pub dir: String,
    /// CRI 单条日志记录切分阈值（字节）。
    pub max_container_log_line_size: usize,
}

/// 守护进程 cgroup driver 配置。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CgroupDriverConfig {
    Systemd,
    Cgroupfs,
}

impl CgroupDriverConfig {
    pub fn as_proto(self) -> crate::proto::runtime::v1::CgroupDriver {
        match self {
            Self::Systemd => crate::proto::runtime::v1::CgroupDriver::Systemd,
            Self::Cgroupfs => crate::proto::runtime::v1::CgroupDriver::Cgroupfs,
        }
    }
}

/// 运行时配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// 默认运行时类型/handler 名称。
    pub runtime_type: String,
    /// OCI runtime 二进制路径。
    pub runtime_path: String,
    /// 运行时状态根目录。
    pub root: String,
    /// 对外暴露的 runtime handlers。
    pub handlers: Vec<String>,
    /// 按 handler 细化的 runtime 配置，参考 CRI-O runtimes 表。
    pub runtimes: HashMap<String, RuntimeHandlerConfig>,
    /// 按 workload 名称定义的 annotation 驱动资源预设。
    pub workloads: HashMap<String, RuntimeWorkloadConfig>,
    /// PodSandbox pause 镜像。
    pub pause_image: String,
    /// pause 镜像内的 infra 命令路径。
    pub pause_command: String,
    /// 可选的 cgroup driver 显式配置。
    pub cgroup_driver: Option<CgroupDriverConfig>,
    /// shim 二进制路径。
    pub shim_path: String,
    /// shim 工作目录。
    pub shim_dir: String,
    /// attach/resize socket 根目录。
    pub attach_socket_dir: String,
    /// 容器退出记录根目录。
    pub container_exits_dir: String,
    /// 干净退出标记文件。
    pub clean_shutdown_file: String,
    /// 容器优雅停止的最小等待时间（秒）。
    pub container_stop_timeout: u32,
    /// 临时版本标记文件，用于识别 reboot 后启动。
    pub version_file: String,
    /// 持久版本标记文件，用于识别升级后的恢复分支。
    pub version_file_persist: String,
    /// 可选的 CRIU 二进制路径；为空时使用 runtime 默认行为。
    pub criu_path: String,
    /// 可选的 CRIU image staging 根目录；为空时沿用 artifact 邻接目录。
    pub criu_image_path: String,
    /// 可选的 CRIU work staging 根目录；为空时默认落到 image 目录下的 `work/`。
    pub criu_work_path: String,
    /// 是否启用 checkpoint/restore 支持。
    pub enable_criu_support: bool,
    /// 是否允许启动期自动清理孤儿 runtime/shim/pod 工件。
    pub internal_wipe: bool,
    /// 是否在 unclean 启动时检查并尝试修复持久化账本。
    pub internal_repair: bool,
    /// 对所有 bind mount source 添加的宿主路径前缀；为空表示不改写。
    pub bind_mount_prefix: String,
    /// 是否禁用 cgroup 支持。
    pub disable_cgroup: bool,
    /// hugetlb controller 缺失时是否容忍并忽略 hugepage limits。
    pub tolerate_missing_hugetlb_controller: bool,
    /// 镜像拉取使用的独立 cgroup；当前版本仅支持空值，非空表示未实现配置。
    pub separate_pull_cgroup: String,
    /// 守护进程默认的 UID 映射，格式为 `container:host:size[,..]`。
    pub uid_mappings: String,
    /// 守护进程默认的 GID 映射，格式为 `container:host:size[,..]`。
    pub gid_mappings: String,
    /// 非 root userns 映射允许使用的最小宿主 UID；-1 表示不限制。
    pub minimum_mappable_uid: i64,
    /// 非 root userns 映射允许使用的最小宿主 GID；-1 表示不限制。
    pub minimum_mappable_gid: i64,
    /// shim 创建的宿主 IO 工件默认 UID。
    pub io_uid: u32,
    /// shim 创建的宿主 IO 工件默认 GID。
    pub io_gid: u32,
    /// 守护进程默认的 pids 限制；-1 表示不设置默认限制。
    pub pids_limit: i64,
    /// exec / execSync 的 CPU 亲和策略；"" 表示 runtime 默认，"first" 表示使用 cpuset 的第一个 CPU。
    pub exec_cpu_affinity: String,
    /// 是否默认将所有 Pod/容器根文件系统设为只读。
    pub read_only: bool,
    /// 是否禁用 pivot_root，改用 MS_MOVE。
    pub no_pivot: bool,
    /// 是否启用 shim debug。
    pub shim_debug: bool,
    /// 传给 monitor/shim 进程的默认环境变量列表，格式为 `KEY=value`。
    pub monitor_env: Vec<String>,
    /// 注入到所有容器的默认环境变量，格式为 `KEY=value`。
    pub default_env: Vec<String>,
    /// daemon 级默认 capabilities 列表。
    pub default_capabilities: Vec<String>,
    /// daemon 级默认 sysctls，格式为 `key = value` 或 `key=value`。
    pub default_sysctls: Vec<String>,
    /// 显式请求 `runtime/default` 时使用的默认 seccomp profile；为空时使用内建最小默认策略。
    pub seccomp_profile: String,
    /// 当 CRI 请求未显式给出 seccomp profile 时使用的默认策略。
    pub unset_seccomp_profile: String,
    /// 是否在 CRI 日志文件之外额外写 journald。
    pub log_to_journald: bool,
    /// 是否在日志轮转和容器退出时跳过 sync。
    pub no_sync_log: bool,
    /// 是否将容器/Pod 的 OOMScoreAdj 下界限制为 daemon 当前值。
    pub restrict_oom_score_adj: bool,
    /// 为非 hostNetwork Pod 默认开启低位端口绑定。
    pub enable_unprivileged_ports: bool,
    /// 为非 hostNetwork 且非 userns Pod 默认开启 ping group range。
    pub enable_unprivileged_icmp: bool,
}

/// 单个 runtime handler 的细化配置。
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeHandlerConfig {
    /// 该 handler 对应的 OCI runtime 二进制路径。
    pub runtime_path: String,
    /// 该 handler 对应的 runtime 状态根目录。
    pub runtime_root: String,
    /// 是否继承默认 handler 的 runtime_path/runtime_root。
    pub inherit_default_runtime: bool,
    /// 该 handler 专属的 monitor/shim 二进制路径；为空时继承 runtime.shim_path。
    pub monitor_path: String,
    /// 该 handler 专属的 monitor/shim 环境变量；未设置时继承 runtime.monitor_env。
    pub monitor_env: Option<Vec<String>>,
    /// 该 handler 额外允许处理的 annotation 前缀。
    pub allowed_annotations: Vec<String>,
    /// 该 handler 默认注入到 OCI annotations 的键值对；显式请求优先。
    pub default_annotations: HashMap<String, String>,
    /// 该 handler 的容器创建超时（秒）；未设置时继承内置默认值。
    pub container_create_timeout: Option<u32>,
    /// 该 handler 专属的 CNI 配置目录；为空时继承全局 network.config_dirs。
    pub cni_conf_dir: String,
    /// 该 handler 专属的 CNI 配置文件最大加载数量；未设置时继承全局 network.max_conf_num。
    #[serde(alias = "cni_max_conf_num")]
    pub cni_max_conf_num: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeWorkloadConfig {
    /// 激活该 workload 的 Pod annotation key。
    pub activation_annotation: String,
    /// 容器级资源覆盖 annotation 前缀。
    pub annotation_prefix: String,
    /// 该 workload 额外允许的 annotation 列表。
    pub allowed_annotations: Vec<String>,
    /// 该 workload 的默认资源预设。
    pub resources: RuntimeWorkloadResources,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeWorkloadResources {
    /// 默认 CPU shares。
    #[serde(rename = "cpushares")]
    pub cpu_shares: i64,
    /// 默认 CPU quota（微秒）。
    #[serde(rename = "cpuquota")]
    pub cpu_quota: i64,
    /// 默认 CPU period（微秒）。
    #[serde(rename = "cpuperiod")]
    pub cpu_period: i64,
    /// 默认 cpuset CPUs。
    #[serde(rename = "cpuset")]
    pub cpuset_cpus: String,
    /// 默认 CPU limit（millicores）。
    #[serde(rename = "cpulimit")]
    pub cpu_limit: i64,
}

impl RuntimeWorkloadResources {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.cpu_shares < 0 {
            return Err("cpushares must be greater than or equal to zero".to_string());
        }
        if self.cpu_quota < 0 {
            return Err("cpuquota must be greater than or equal to zero".to_string());
        }
        if self.cpu_period < 0 {
            return Err("cpuperiod must be greater than or equal to zero".to_string());
        }
        if self.cpu_period != 0 && self.cpu_period < 1_000 {
            return Err("cpuperiod must be at least 1000 microseconds".to_string());
        }
        if self.cpu_limit < 0 {
            return Err("cpulimit must be greater than or equal to zero".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntimeHandlerConfig {
    pub runtime_path: String,
    pub runtime_root: String,
    pub monitor_path: String,
    pub monitor_env: Vec<String>,
    pub allowed_annotations: Vec<String>,
    pub default_annotations: HashMap<String, String>,
    pub container_create_timeout: u32,
}

/// 镜像配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ImageConfig {
    /// 镜像存储后端。
    pub driver: String,
    /// 镜像存储路径。
    pub root: String,
    /// 守护进程级 registry 鉴权文件，兼容 docker config.json 的 auths 结构。
    pub global_auth_file: String,
}

/// 网络配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    /// 网络插件类型。
    pub plugin: String,
    /// CNI 配置目录，兼容旧单目录配置。
    #[serde(
        default,
        alias = "config_dir",
        deserialize_with = "deserialize_string_or_vec"
    )]
    pub config_dirs: Vec<String>,
    /// CNI 插件目录。
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub plugin_dirs: Vec<String>,
    /// CNI 缓存目录。
    pub cache_dir: String,
    /// CNI 配置模板文件路径；开启后 UpdateRuntimeConfig 可基于 PodCIDR 渲染配置文件。
    pub conf_template: String,
    /// 最大允许加载的 CNI 配置文件数量；0 表示不限制。
    #[serde(alias = "cni_max_conf_num")]
    pub max_conf_num: usize,
    /// Pod 主 IP 选择策略。
    pub ip_pref: MainIpPreference,
    /// 显式指定默认使用的 CNI 网络名；为空时按文件名字典序选择第一个。
    #[serde(alias = "cni_default_network")]
    pub default_network_name: Option<String>,
    /// 是否全局禁用 hostPort 映射。
    pub disable_hostport_mapping: bool,
    /// 是否将 netns 挂载统一放到 runtime state dir 下。
    #[serde(alias = "netns_mounts_under_state_dir")]
    pub netns_mounts_under_state_dir: bool,
}

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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

fn deserialize_string_or_vec<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<StringOrVec>::deserialize(deserializer)?;
    Ok(match value {
        Some(StringOrVec::String(value)) => vec![value],
        Some(StringOrVec::Vec(values)) => values,
        None => Vec::new(),
    })
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: "unix:///run/crius/crius.sock".to_string(),
            allow_tcp_service: false,
            grpc_max_send_msg_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES,
            grpc_max_recv_msg_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES,
            enable_pod_events: true,
            included_pod_metrics: vec!["all".to_string()],
            stats_collection_period: 0,
            pod_sandbox_metrics_collection_period: 0,
            exec_sync_io_drain_timeout: std::time::Duration::ZERO,
            streaming: StreamingConfig::default(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: None,
            dir: "/var/log/crius".to_string(),
            max_container_log_line_size: 4096,
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            runtime_type: "runc".to_string(),
            runtime_path: "/usr/bin/runc".to_string(),
            root: "/run/crius".to_string(),
            handlers: Vec::new(),
            runtimes: HashMap::new(),
            workloads: HashMap::new(),
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            pause_command: "/pause".to_string(),
            cgroup_driver: None,
            shim_path: "/usr/bin/crius-shim".to_string(),
            shim_dir: "/var/run/crius/shims".to_string(),
            attach_socket_dir: "/var/run/crius/shims".to_string(),
            container_exits_dir: "/var/run/crius/exits".to_string(),
            clean_shutdown_file: "/var/lib/crius/clean.shutdown".to_string(),
            container_stop_timeout: MIN_CONTAINER_STOP_TIMEOUT_SECS,
            version_file: "/run/crius/version".to_string(),
            version_file_persist: "/var/lib/crius/version".to_string(),
            criu_path: String::new(),
            criu_image_path: String::new(),
            criu_work_path: String::new(),
            enable_criu_support: true,
            internal_wipe: true,
            internal_repair: true,
            bind_mount_prefix: String::new(),
            disable_cgroup: false,
            tolerate_missing_hugetlb_controller: true,
            separate_pull_cgroup: String::new(),
            uid_mappings: String::new(),
            gid_mappings: String::new(),
            minimum_mappable_uid: -1,
            minimum_mappable_gid: -1,
            io_uid: 0,
            io_gid: 0,
            pids_limit: -1,
            exec_cpu_affinity: String::new(),
            read_only: false,
            no_pivot: false,
            shim_debug: false,
            monitor_env: Vec::new(),
            default_env: Vec::new(),
            default_capabilities: vec![
                "CHOWN".to_string(),
                "DAC_OVERRIDE".to_string(),
                "FSETID".to_string(),
                "FOWNER".to_string(),
                "MKNOD".to_string(),
                "NET_RAW".to_string(),
                "SETGID".to_string(),
                "SETUID".to_string(),
                "SETFCAP".to_string(),
                "SETPCAP".to_string(),
                "NET_BIND_SERVICE".to_string(),
                "SYS_CHROOT".to_string(),
                "KILL".to_string(),
                "AUDIT_WRITE".to_string(),
            ],
            default_sysctls: Vec::new(),
            seccomp_profile: String::new(),
            unset_seccomp_profile: "runtime/default".to_string(),
            log_to_journald: false,
            no_sync_log: false,
            restrict_oom_score_adj: false,
            enable_unprivileged_ports: false,
            enable_unprivileged_icmp: false,
        }
    }
}

impl Default for ImageConfig {
    fn default() -> Self {
        Self {
            driver: "overlay".to_string(),
            root: "/var/lib/containers/storage".to_string(),
            global_auth_file: String::new(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            plugin: "cni".to_string(),
            config_dirs: vec![
                "/etc/cni/net.d".to_string(),
                "/etc/kubernetes/cni/net.d".to_string(),
            ],
            plugin_dirs: vec![
                "/opt/cni/bin".to_string(),
                "/usr/lib/cni".to_string(),
                "/usr/libexec/cni".to_string(),
            ],
            cache_dir: "/var/lib/cni/cache".to_string(),
            conf_template: String::new(),
            max_conf_num: 0,
            ip_pref: MainIpPreference::Cni,
            default_network_name: None,
            disable_hostport_mapping: false,
            netns_mounts_under_state_dir: false,
        }
    }
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

impl Default for Config {
    fn default() -> Self {
        Self {
            root: "/var/lib/crius".to_string(),
            api: ApiConfig::default(),
            runtime: RuntimeConfig::default(),
            image: ImageConfig::default(),
            network: NetworkConfig::default(),
            logging: LoggingConfig::default(),
            nri: NriConfig::default(),
        }
    }
}

impl RuntimeConfig {
    pub fn normalized_handlers(&self) -> Vec<String> {
        let mut handlers = Vec::new();
        for handler in &self.handlers {
            let trimmed = handler.trim();
            if !trimmed.is_empty() && !handlers.iter().any(|existing: &String| existing == trimmed)
            {
                handlers.push(trimmed.to_string());
            }
        }
        for handler in self.runtimes.keys() {
            let trimmed = handler.trim();
            if !trimmed.is_empty() && !handlers.iter().any(|existing: &String| existing == trimmed)
            {
                handlers.push(trimmed.to_string());
            }
        }
        if !handlers.iter().any(|handler| handler == &self.runtime_type) {
            handlers.push(self.runtime_type.clone());
        }
        handlers
    }

    pub fn resolved_runtimes(&self) -> Result<HashMap<String, ResolvedRuntimeHandlerConfig>> {
        let default_handler = self.runtime_type.trim();
        let default_runtime = ResolvedRuntimeHandlerConfig {
            runtime_path: self.runtime_path.trim().to_string(),
            runtime_root: self.root.trim().to_string(),
            monitor_path: self.shim_path.trim().to_string(),
            monitor_env: self.monitor_env.clone(),
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            container_create_timeout: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
        };
        let mut resolved = HashMap::from([(default_handler.to_string(), default_runtime.clone())]);

        for handler in self.normalized_handlers() {
            if handler == default_handler {
                continue;
            }

            let Some(config) = self.runtimes.get(&handler) else {
                return Err(Error::Config(format!(
                    "runtime handler {handler} requires [runtime.runtimes.{handler}]"
                )));
            };

            if config.inherit_default_runtime {
                if !config.runtime_path.trim().is_empty() || !config.runtime_root.trim().is_empty()
                {
                    return Err(Error::Config(format!(
                        "runtime.runtimes.{handler} must not set runtime_path/runtime_root when inherit_default_runtime = true"
                    )));
                }
                let mut inherited = default_runtime.clone();
                if !config.monitor_path.trim().is_empty() {
                    inherited.monitor_path = config.monitor_path.trim().to_string();
                }
                if let Some(monitor_env) = config.monitor_env.as_ref() {
                    inherited.monitor_env = monitor_env.clone();
                }
                inherited.allowed_annotations = config.allowed_annotations.clone();
                inherited.default_annotations = config.default_annotations.clone();
                if let Some(timeout) = config.container_create_timeout {
                    inherited.container_create_timeout =
                        timeout.max(MIN_CONTAINER_CREATE_TIMEOUT_SECS);
                }
                resolved.insert(handler, inherited);
                continue;
            }

            if config.runtime_path.trim().is_empty() {
                return Err(Error::Config(format!(
                    "runtime.runtimes.{handler}.runtime_path must not be empty"
                )));
            }

            resolved.insert(
                handler,
                ResolvedRuntimeHandlerConfig {
                    runtime_path: config.runtime_path.trim().to_string(),
                    runtime_root: if config.runtime_root.trim().is_empty() {
                        default_runtime.runtime_root.clone()
                    } else {
                        config.runtime_root.trim().to_string()
                    },
                    monitor_path: if config.monitor_path.trim().is_empty() {
                        default_runtime.monitor_path.clone()
                    } else {
                        config.monitor_path.trim().to_string()
                    },
                    monitor_env: config
                        .monitor_env
                        .clone()
                        .unwrap_or_else(|| self.monitor_env.clone()),
                    allowed_annotations: config.allowed_annotations.clone(),
                    default_annotations: config.default_annotations.clone(),
                    container_create_timeout: config
                        .container_create_timeout
                        .unwrap_or(DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS)
                        .max(MIN_CONTAINER_CREATE_TIMEOUT_SECS),
                },
            );
        }

        Ok(resolved)
    }

    pub fn parsed_uid_mappings(&self) -> Result<Vec<crate::proto::runtime::v1::IdMapping>> {
        parse_id_mappings("runtime.uid_mappings", &self.uid_mappings)
    }

    pub fn parsed_gid_mappings(&self) -> Result<Vec<crate::proto::runtime::v1::IdMapping>> {
        parse_id_mappings("runtime.gid_mappings", &self.gid_mappings)
    }

    pub fn parsed_default_env(&self) -> Vec<(String, String)> {
        self.default_env
            .iter()
            .filter_map(|entry| entry.split_once('='))
            .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
            .collect()
    }

    pub fn parsed_default_sysctls(&self) -> Result<HashMap<String, String>> {
        self.default_sysctls
            .iter()
            .map(|entry| {
                parse_sysctl_assignment(entry)
                    .map_err(|message| Error::Config(format!("runtime.default_sysctls: {message}")))
            })
            .collect()
    }

    pub fn validate_workloads(&self) -> Result<()> {
        let mut seen_activation_annotations = HashSet::new();
        for (name, workload) in &self.workloads {
            let activation_annotation = workload.activation_annotation.trim();
            if activation_annotation.is_empty() {
                return Err(Error::Config(format!(
                    "runtime.workloads.{name}.activation_annotation must not be empty"
                )));
            }
            if !seen_activation_annotations.insert(activation_annotation.to_string()) {
                return Err(Error::Config(format!(
                    "runtime.workloads contains duplicate activation_annotation {activation_annotation}"
                )));
            }
            if workload.annotation_prefix.trim().is_empty() {
                return Err(Error::Config(format!(
                    "runtime.workloads.{name}.annotation_prefix must not be empty"
                )));
            }
            workload
                .resources
                .validate()
                .map_err(|message| Error::Config(format!("runtime.workloads.{name}: {message}")))?;
        }
        Ok(())
    }
}

impl NetworkConfig {
    pub fn cni_config(&self) -> CniConfig {
        let mut cni = CniConfig::new(
            self.config_dirs.iter().map(PathBuf::from).collect(),
            self.plugin_dirs.iter().map(PathBuf::from).collect(),
            PathBuf::from(&self.cache_dir),
            self.max_conf_num,
            self.ip_pref,
            self.default_network_name.clone(),
            self.disable_hostport_mapping,
        );
        if !self.conf_template.trim().is_empty() {
            cni.set_conf_template(Some(PathBuf::from(self.conf_template.trim())));
        }
        cni.set_netns_mounts_under_state_dir(self.netns_mounts_under_state_dir);
        cni
    }
}

impl Config {
    /// 从文件加载配置。
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&content)?;
        config.normalize_runtime_settings();
        config.validate()?;
        Ok(config)
    }

    /// 应用环境变量覆盖。
    pub fn apply_env_overrides(&mut self) -> Result<()> {
        apply_string_override("CRIUS_ROOT", &mut self.root);
        apply_string_override("CRIUS_LISTEN", &mut self.api.listen);
        apply_bool_override("CRIUS_ALLOW_TCP_SERVICE", &mut self.api.allow_tcp_service)?;
        apply_u32_override(
            "CRIUS_GRPC_MAX_SEND_MSG_SIZE",
            &mut self.api.grpc_max_send_msg_size,
        )?;
        apply_u32_override(
            "CRIUS_GRPC_MAX_RECV_MSG_SIZE",
            &mut self.api.grpc_max_recv_msg_size,
        )?;
        apply_bool_override("CRIUS_ENABLE_POD_EVENTS", &mut self.api.enable_pod_events)?;
        apply_csv_override(
            "CRIUS_INCLUDED_POD_METRICS",
            &mut self.api.included_pod_metrics,
        );
        apply_u64_override(
            "CRIUS_STATS_COLLECTION_PERIOD",
            &mut self.api.stats_collection_period,
        )?;
        apply_u64_override(
            "CRIUS_POD_SANDBOX_METRICS_COLLECTION_PERIOD",
            &mut self.api.pod_sandbox_metrics_collection_period,
        )?;
        apply_duration_override(
            "CRIUS_EXEC_SYNC_IO_DRAIN_TIMEOUT",
            &mut self.api.exec_sync_io_drain_timeout,
        )?;
        apply_string_override("CRIUS_STREAM_ADDRESS", &mut self.api.streaming.address);
        apply_u16_override("CRIUS_STREAM_PORT", &mut self.api.streaming.port)?;
        apply_bool_override(
            "CRIUS_STREAM_ENABLE_TLS",
            &mut self.api.streaming.enable_tls,
        )?;
        apply_string_override(
            "CRIUS_STREAM_TLS_CERT",
            &mut self.api.streaming.tls_cert_file,
        );
        apply_string_override(
            "CRIUS_STREAM_TLS_KEY",
            &mut self.api.streaming.tls_key_file,
        );
        apply_string_override(
            "CRIUS_STREAM_TLS_CA",
            &mut self.api.streaming.tls_ca_file,
        );
        apply_string_override(
            "CRIUS_TLS_MIN_VERSION",
            &mut self.api.streaming.tls_min_version,
        );
        apply_duration_override(
            "CRIUS_STREAM_REQUEST_TOKEN_TTL",
            &mut self.api.streaming.request_token_ttl,
        )?;
        apply_duration_override(
            "CRIUS_STREAM_PORT_FORWARD_STREAM_CREATION_TIMEOUT",
            &mut self.api.streaming.port_forward_stream_creation_timeout,
        )?;
        apply_duration_override(
            "CRIUS_STREAM_IDLE_TIMEOUT",
            &mut self.api.streaming.port_forward_idle_timeout,
        )?;

        apply_string_override("CRIUS_RUNTIME_TYPE", &mut self.runtime.runtime_type);
        apply_string_override("CRIUS_RUNTIME_PATH", &mut self.runtime.runtime_path);
        apply_string_override("CRIUS_RUNTIME_ROOT", &mut self.runtime.root);
        apply_csv_override("CRIUS_RUNTIME_HANDLERS", &mut self.runtime.handlers);
        apply_string_override("CRIUS_PAUSE_IMAGE", &mut self.runtime.pause_image);
        apply_string_override("CRIUS_PAUSE_COMMAND", &mut self.runtime.pause_command);
        apply_string_override("CRIUS_SHIM_PATH", &mut self.runtime.shim_path);
        apply_string_override("CRIUS_SHIM_DIR", &mut self.runtime.shim_dir);
        apply_csv_override("CRIUS_MONITOR_ENV", &mut self.runtime.monitor_env);
        apply_csv_override("CRIUS_DEFAULT_ENV", &mut self.runtime.default_env);
        apply_csv_override(
            "CRIUS_DEFAULT_CAPABILITIES",
            &mut self.runtime.default_capabilities,
        );
        apply_csv_override("CRIUS_DEFAULT_SYSCTLS", &mut self.runtime.default_sysctls);
        apply_string_override(
            "CRIUS_SECCOMP_PROFILE",
            &mut self.runtime.seccomp_profile,
        );
        apply_string_override(
            "CRIUS_UNSET_SECCOMP_PROFILE",
            &mut self.runtime.unset_seccomp_profile,
        );
        apply_string_override(
            "CRIUS_ATTACH_SOCKET_DIR",
            &mut self.runtime.attach_socket_dir,
        );
        apply_string_override(
            "CRIUS_CONTAINER_EXITS_DIR",
            &mut self.runtime.container_exits_dir,
        );
        apply_string_override(
            "CRIUS_CLEAN_SHUTDOWN_FILE",
            &mut self.runtime.clean_shutdown_file,
        );
        apply_string_override("CRIUS_VERSION_FILE", &mut self.runtime.version_file);
        apply_string_override(
            "CRIUS_VERSION_FILE_PERSIST",
            &mut self.runtime.version_file_persist,
        );
        apply_string_override("CRIUS_CRIU_PATH", &mut self.runtime.criu_path);
        apply_string_override("CRIUS_CRIU_IMAGE_PATH", &mut self.runtime.criu_image_path);
        apply_string_override("CRIUS_CRIU_WORK_PATH", &mut self.runtime.criu_work_path);
        apply_bool_override(
            "CRIUS_ENABLE_CRIU_SUPPORT",
            &mut self.runtime.enable_criu_support,
        )?;
        apply_bool_override("CRIUS_INTERNAL_WIPE", &mut self.runtime.internal_wipe)?;
        apply_bool_override("CRIUS_INTERNAL_REPAIR", &mut self.runtime.internal_repair)?;
        apply_string_override(
            "CRIUS_BIND_MOUNT_PREFIX",
            &mut self.runtime.bind_mount_prefix,
        );
        apply_bool_override("CRIUS_DISABLE_CGROUP", &mut self.runtime.disable_cgroup)?;
        apply_bool_override(
            "CRIUS_TOLERATE_MISSING_HUGETLB_CONTROLLER",
            &mut self.runtime.tolerate_missing_hugetlb_controller,
        )?;
        apply_string_override(
            "CRIUS_SEPARATE_PULL_CGROUP",
            &mut self.runtime.separate_pull_cgroup,
        );
        apply_string_override("CRIUS_UID_MAPPINGS", &mut self.runtime.uid_mappings);
        apply_string_override("CRIUS_GID_MAPPINGS", &mut self.runtime.gid_mappings);
        apply_i64_override(
            "CRIUS_MINIMUM_MAPPABLE_UID",
            &mut self.runtime.minimum_mappable_uid,
        )?;
        apply_i64_override(
            "CRIUS_MINIMUM_MAPPABLE_GID",
            &mut self.runtime.minimum_mappable_gid,
        )?;
        apply_u32_override("CRIUS_IO_UID", &mut self.runtime.io_uid)?;
        apply_u32_override("CRIUS_IO_GID", &mut self.runtime.io_gid)?;
        apply_i64_override("CRIUS_PIDS_LIMIT", &mut self.runtime.pids_limit)?;
        apply_string_override(
            "CRIUS_EXEC_CPU_AFFINITY",
            &mut self.runtime.exec_cpu_affinity,
        );
        apply_bool_override("CRIUS_READ_ONLY", &mut self.runtime.read_only)?;
        apply_bool_override("CRIUS_NO_PIVOT", &mut self.runtime.no_pivot)?;
        apply_u32_override(
            "CRIUS_CONTAINER_STOP_TIMEOUT",
            &mut self.runtime.container_stop_timeout,
        )?;
        apply_bool_override("CRIUS_SHIM_DEBUG", &mut self.runtime.shim_debug)?;
        apply_bool_override("CRIUS_LOG_TO_JOURNALD", &mut self.runtime.log_to_journald)?;
        apply_bool_override("CRIUS_NO_SYNC_LOG", &mut self.runtime.no_sync_log)?;
        apply_bool_override(
            "CRIUS_RESTRICT_OOM_SCORE_ADJ",
            &mut self.runtime.restrict_oom_score_adj,
        )?;
        apply_bool_override(
            "CRIUS_ENABLE_UNPRIVILEGED_PORTS",
            &mut self.runtime.enable_unprivileged_ports,
        )?;
        apply_bool_override(
            "CRIUS_ENABLE_UNPRIVILEGED_ICMP",
            &mut self.runtime.enable_unprivileged_icmp,
        )?;
        if let Some(driver) = std::env::var_os("CRIUS_CGROUP_DRIVER") {
            self.runtime.cgroup_driver =
                Some(parse_cgroup_driver(&driver.to_string_lossy()).map_err(Error::Config)?);
        }

        apply_string_override("CRIUS_IMAGE_DRIVER", &mut self.image.driver);
        apply_string_override("CRIUS_IMAGE_ROOT", &mut self.image.root);
        apply_string_override(
            "CRIUS_IMAGE_GLOBAL_AUTH_FILE",
            &mut self.image.global_auth_file,
        );

        apply_colon_dirs_override("CRIUS_CNI_CONFIG_DIRS", &mut self.network.config_dirs);
        if std::env::var_os("CRIUS_CNI_PLUGIN_DIRS").is_some() {
            apply_colon_dirs_override("CRIUS_CNI_PLUGIN_DIRS", &mut self.network.plugin_dirs);
        } else if let Some(fallback) = std::env::var_os("CNI_PATH") {
            self.network.plugin_dirs = split_colon_list(&fallback.to_string_lossy());
        }
        apply_string_override("CRIUS_CNI_CACHE_DIR", &mut self.network.cache_dir);
        apply_string_override("CRIUS_CNI_CONF_TEMPLATE", &mut self.network.conf_template);
        apply_usize_override("CRIUS_CNI_MAX_CONF_NUM", &mut self.network.max_conf_num)?;
        if let Some(ip_pref) = std::env::var_os("CRIUS_CNI_IP_PREF") {
            self.network.ip_pref =
                parse_main_ip_preference(&ip_pref.to_string_lossy()).map_err(Error::Config)?;
        }
        apply_optional_string_override(
            "CRIUS_CNI_DEFAULT_NETWORK",
            &mut self.network.default_network_name,
        );
        apply_bool_override(
            "CRIUS_DISABLE_HOSTPORT_MAPPING",
            &mut self.network.disable_hostport_mapping,
        )?;
        apply_bool_override(
            "CRIUS_NETNS_MOUNTS_UNDER_STATE_DIR",
            &mut self.network.netns_mounts_under_state_dir,
        )?;

        apply_string_override("CRIUS_LOG_LEVEL", &mut self.logging.level);
        apply_optional_string_override("CRIUS_LOG_FILE", &mut self.logging.file);
        apply_string_override("CRIUS_LOG_DIR", &mut self.logging.dir);
        apply_usize_override(
            "CRIUS_MAX_CONTAINER_LOG_LINE_SIZE",
            &mut self.logging.max_container_log_line_size,
        )?;

        self.normalize_runtime_settings();
        self.validate()
    }

    fn normalize_runtime_settings(&mut self) {
        self.runtime.container_stop_timeout = self
            .runtime
            .container_stop_timeout
            .max(MIN_CONTAINER_STOP_TIMEOUT_SECS);
        self.network.default_network_name = self
            .network
            .default_network_name
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    }

    /// 启动期统一配置校验。
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty("root", &self.root)?;
        ensure_non_empty("api.listen", &self.api.listen)?;
        validate_listen_address(&self.api.listen, self.api.allow_tcp_service)?;
        if self.api.grpc_max_send_msg_size == 0 {
            return Err(Error::Config(
                "api.grpc_max_send_msg_size must be greater than zero".to_string(),
            ));
        }
        if self.api.grpc_max_recv_msg_size == 0 {
            return Err(Error::Config(
                "api.grpc_max_recv_msg_size must be greater than zero".to_string(),
            ));
        }
        self.api
            .streaming
            .validate()
            .map_err(|err| Error::Config(err.to_string()))?;
        validate_included_pod_metrics(&self.api.included_pod_metrics)?;

        ensure_non_empty("runtime.runtime_type", &self.runtime.runtime_type)?;
        ensure_non_empty("runtime.runtime_path", &self.runtime.runtime_path)?;
        ensure_non_empty("runtime.root", &self.runtime.root)?;
        ensure_non_empty("runtime.pause_image", &self.runtime.pause_image)?;
        ensure_non_empty("runtime.pause_command", &self.runtime.pause_command)?;
        ensure_non_empty("runtime.shim_path", &self.runtime.shim_path)?;
        ensure_non_empty("runtime.shim_dir", &self.runtime.shim_dir)?;
        validate_monitor_env_list("runtime.monitor_env", &self.runtime.monitor_env)?;
        validate_monitor_env_list("runtime.default_env", &self.runtime.default_env)?;
        validate_capability_list(
            "runtime.default_capabilities",
            &self.runtime.default_capabilities,
        )?;
        validate_sysctl_assignments("runtime.default_sysctls", &self.runtime.default_sysctls)?;
        validate_optional_seccomp_profile_path(
            "runtime.seccomp_profile",
            &self.runtime.seccomp_profile,
        )?;
        validate_seccomp_profile_selector(
            "runtime.unset_seccomp_profile",
            &self.runtime.unset_seccomp_profile,
        )?;
        ensure_non_empty("runtime.attach_socket_dir", &self.runtime.attach_socket_dir)?;
        ensure_non_empty(
            "runtime.container_exits_dir",
            &self.runtime.container_exits_dir,
        )?;
        ensure_non_empty(
            "runtime.clean_shutdown_file",
            &self.runtime.clean_shutdown_file,
        )?;
        ensure_non_empty("runtime.version_file", &self.runtime.version_file)?;
        ensure_non_empty(
            "runtime.version_file_persist",
            &self.runtime.version_file_persist,
        )?;
        if self.runtime.pids_limit != -1 && self.runtime.pids_limit <= 0 {
            return Err(Error::Config(
                "runtime.pids_limit must be -1 or greater than zero".to_string(),
            ));
        }
        if self.runtime.minimum_mappable_uid < -1 {
            return Err(Error::Config(
                "runtime.minimum_mappable_uid must be -1 or greater".to_string(),
            ));
        }
        if self.runtime.minimum_mappable_gid < -1 {
            return Err(Error::Config(
                "runtime.minimum_mappable_gid must be -1 or greater".to_string(),
            ));
        }
        if !self.runtime.separate_pull_cgroup.trim().is_empty() {
            return Err(Error::Config(
                "runtime.separate_pull_cgroup is not supported by crius yet".to_string(),
            ));
        }
        match self.runtime.exec_cpu_affinity.trim() {
            "" | "first" => {}
            other => {
                return Err(Error::Config(format!(
                    "runtime.exec_cpu_affinity must be empty or \"first\", got {}",
                    other
                )));
            }
        }
        if !self.runtime.bind_mount_prefix.trim().is_empty()
            && !Path::new(self.runtime.bind_mount_prefix.trim()).is_absolute()
        {
            return Err(Error::Config(
                "runtime.bind_mount_prefix must be an absolute path when set".to_string(),
            ));
        }
        if !self.runtime.criu_image_path.trim().is_empty()
            && !Path::new(self.runtime.criu_image_path.trim()).is_absolute()
        {
            return Err(Error::Config(
                "runtime.criu_image_path must be an absolute path when set".to_string(),
            ));
        }
        if !self.runtime.criu_work_path.trim().is_empty()
            && !Path::new(self.runtime.criu_work_path.trim()).is_absolute()
        {
            return Err(Error::Config(
                "runtime.criu_work_path must be an absolute path when set".to_string(),
            ));
        }
        let uid_mappings = self.runtime.parsed_uid_mappings()?;
        let gid_mappings = self.runtime.parsed_gid_mappings()?;
        if uid_mappings.is_empty() != gid_mappings.is_empty() {
            return Err(Error::Config(
                "runtime.uid_mappings and runtime.gid_mappings must be configured together"
                    .to_string(),
            ));
        }

        ensure_non_empty("image.driver", &self.image.driver)?;
        ensure_non_empty("image.root", &self.image.root)?;
        if self.image.driver.trim() != "overlay" {
            return Err(Error::Config(format!(
                "image.driver must be \"overlay\", got {}",
                self.image.driver.trim()
            )));
        }
        if !self.image.global_auth_file.trim().is_empty()
            && !Path::new(self.image.global_auth_file.trim()).is_absolute()
        {
            return Err(Error::Config(
                "image.global_auth_file must be an absolute path when set".to_string(),
            ));
        }

        ensure_non_empty("network.plugin", &self.network.plugin)?;
        if self.network.plugin != "cni" {
            return Err(Error::Config(format!(
                "unsupported network.plugin {}; only cni is supported",
                self.network.plugin
            )));
        }
        ensure_vec_non_empty("runtime.handlers", &self.runtime.normalized_handlers())?;
        if self
            .runtime
            .runtimes
            .contains_key(&self.runtime.runtime_type)
        {
            return Err(Error::Config(format!(
                "runtime.runtimes.{} must not be set; configure the default handler via runtime.runtime_path/runtime.root",
                self.runtime.runtime_type
            )));
        }
        for (handler, handler_config) in &self.runtime.runtimes {
            if let Some(monitor_env) = handler_config.monitor_env.as_ref() {
                validate_monitor_env_list(
                    &format!("runtime.runtimes.{handler}.monitor_env"),
                    monitor_env,
                )?;
            }
            if !handler_config.monitor_path.trim().is_empty() {
                ensure_non_empty(
                    &format!("runtime.runtimes.{handler}.monitor_path"),
                    &handler_config.monitor_path,
                )?;
            }
            validate_annotation_prefix_list(
                &format!("runtime.runtimes.{handler}.allowed_annotations"),
                &handler_config.allowed_annotations,
            )?;
            validate_annotation_map(
                &format!("runtime.runtimes.{handler}.default_annotations"),
                &handler_config.default_annotations,
            )?;
        }
        self.runtime.resolved_runtimes()?;
        self.runtime.validate_workloads()?;
        ensure_vec_non_empty("network.config_dirs", &self.network.config_dirs)?;
        ensure_vec_non_empty("network.plugin_dirs", &self.network.plugin_dirs)?;
        ensure_non_empty("network.cache_dir", &self.network.cache_dir)?;

        ensure_non_empty("logging.level", &self.logging.level)?;
        ensure_non_empty("logging.dir", &self.logging.dir)?;
        if self.logging.max_container_log_line_size == 0 {
            return Err(Error::Config(
                "logging.max_container_log_line_size must be greater than zero".to_string(),
            ));
        }
        validate_log_filter(&self.logging.level)?;
        if let Some(file) = &self.logging.file {
            ensure_non_empty("logging.file", file)?;
        }

        Ok(())
    }
}

fn apply_string_override(env_name: &str, target: &mut String) {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value.to_string_lossy().trim().to_string();
    }
}

fn apply_optional_string_override(env_name: &str, target: &mut Option<String>) {
    if let Some(value) = std::env::var_os(env_name) {
        let trimmed = value.to_string_lossy().trim().to_string();
        *target = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
}

fn apply_csv_override(env_name: &str, target: &mut Vec<String>) {
    if let Some(value) = std::env::var_os(env_name) {
        *target = split_csv_list(&value.to_string_lossy());
    }
}

fn apply_colon_dirs_override(env_name: &str, target: &mut Vec<String>) {
    if let Some(value) = std::env::var_os(env_name) {
        *target = split_colon_list(&value.to_string_lossy());
    }
}

fn apply_bool_override(env_name: &str, target: &mut bool) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = parse_bool(&value.to_string_lossy())
            .map_err(|err| Error::Config(format!("{env_name}: {err}")))?;
    }
    Ok(())
}

fn apply_u16_override(env_name: &str, target: &mut u16) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<u16>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid u16 value: {err}")))?;
    }
    Ok(())
}

fn apply_u32_override(env_name: &str, target: &mut u32) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<u32>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid u32 value: {err}")))?;
    }
    Ok(())
}

fn apply_u64_override(env_name: &str, target: &mut u64) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<u64>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid u64 value: {err}")))?;
    }
    Ok(())
}

fn apply_i64_override(env_name: &str, target: &mut i64) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<i64>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid i64 value: {err}")))?;
    }
    Ok(())
}

fn apply_usize_override(env_name: &str, target: &mut usize) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = value
            .to_string_lossy()
            .trim()
            .parse::<usize>()
            .map_err(|err| Error::Config(format!("{env_name}: invalid usize value: {err}")))?;
    }
    Ok(())
}

fn apply_duration_override(env_name: &str, target: &mut std::time::Duration) -> Result<()> {
    if let Some(value) = std::env::var_os(env_name) {
        *target = crate::streaming::parse_duration(&value.to_string_lossy())
            .map_err(|err| Error::Config(format!("{env_name}: {err}")))?;
    }
    Ok(())
}

fn split_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn split_colon_list(raw: &str) -> Vec<String> {
    raw.split(':')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_bool(raw: &str) -> std::result::Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value {other}")),
    }
}

fn parse_id_mappings(
    field_name: &str,
    raw: &str,
) -> Result<Vec<crate::proto::runtime::v1::IdMapping>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    trimmed
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let mut parts = entry.split(':').map(str::trim);
            let container_id = parts
                .next()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "{field_name}: invalid mapping {entry}; expected container:host:size"
                    ))
                })?
                .parse::<u32>()
                .map_err(|err| {
                    Error::Config(format!(
                        "{field_name}: invalid container id in mapping {entry}: {err}"
                    ))
                })?;
            let host_id = parts
                .next()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "{field_name}: invalid mapping {entry}; expected container:host:size"
                    ))
                })?
                .parse::<u32>()
                .map_err(|err| {
                    Error::Config(format!(
                        "{field_name}: invalid host id in mapping {entry}: {err}"
                    ))
                })?;
            let length = parts
                .next()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "{field_name}: invalid mapping {entry}; expected container:host:size"
                    ))
                })?
                .parse::<u32>()
                .map_err(|err| {
                    Error::Config(format!(
                        "{field_name}: invalid size in mapping {entry}: {err}"
                    ))
                })?;
            if length == 0 {
                return Err(Error::Config(format!(
                    "{field_name}: mapping {entry} must have size greater than zero"
                )));
            }
            if parts.next().is_some() {
                return Err(Error::Config(format!(
                    "{field_name}: invalid mapping {entry}; expected container:host:size"
                )));
            }
            Ok(crate::proto::runtime::v1::IdMapping {
                host_id,
                container_id,
                length,
            })
        })
        .collect()
}

fn parse_cgroup_driver(raw: &str) -> std::result::Result<CgroupDriverConfig, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "systemd" => Ok(CgroupDriverConfig::Systemd),
        "cgroupfs" => Ok(CgroupDriverConfig::Cgroupfs),
        other => Err(format!(
            "invalid cgroup driver {other}; expected systemd or cgroupfs"
        )),
    }
}

fn parse_main_ip_preference(raw: &str) -> std::result::Result<MainIpPreference, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "ipv4" => Ok(MainIpPreference::Ipv4),
        "ipv6" => Ok(MainIpPreference::Ipv6),
        "cni" => Ok(MainIpPreference::Cni),
        other => Err(format!(
            "invalid CNI ip preference {other}; expected ipv4, ipv6, or cni"
        )),
    }
}

fn validate_included_pod_metrics(values: &[String]) -> Result<()> {
    const ALL: &str = "all";
    const AVAILABLE: &[&str] = &["cpu", "memory", "network", "process", "disk"];

    if values.len() == 1 && values[0].trim().eq_ignore_ascii_case(ALL) {
        return Ok(());
    }

    for value in values {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized == ALL {
            return Err(Error::Config(
                "'all' should be the only value in api.included_pod_metrics".to_string(),
            ));
        }
        if !AVAILABLE.iter().any(|candidate| *candidate == normalized) {
            return Err(Error::Config(format!(
                "invalid pod metric {normalized}; available metrics: {:?}",
                AVAILABLE
            )));
        }
    }
    Ok(())
}

fn ensure_non_empty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::Config(format!("{name} must not be empty")));
    }
    Ok(())
}

fn ensure_vec_non_empty(name: &str, values: &[String]) -> Result<()> {
    if values.is_empty() {
        return Err(Error::Config(format!("{name} must not be empty")));
    }
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(Error::Config(format!(
            "{name} must not contain empty entries"
        )));
    }
    Ok(())
}

fn validate_monitor_env_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        let Some((key, _)) = value.split_once('=') else {
            return Err(Error::Config(format!(
                "{name} entries must be in KEY=value format"
            )));
        };
        if key.trim().is_empty() {
            return Err(Error::Config(format!(
                "{name} entries must have a non-empty KEY"
            )));
        }
    }
    Ok(())
}

fn validate_optional_seccomp_profile_path(name: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(());
    }
    if !Path::new(value).is_absolute() {
        return Err(Error::Config(format!(
            "{name} must be an absolute path when set"
        )));
    }
    Ok(())
}

fn validate_seccomp_profile_selector(name: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty()
        || value.eq_ignore_ascii_case("runtime/default")
        || value.eq_ignore_ascii_case("docker/default")
        || value.eq_ignore_ascii_case("unconfined")
        || value.starts_with("localhost/")
        || Path::new(value).is_absolute()
    {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{name} must be empty, runtime/default, docker/default, unconfined, localhost/<name>, or an absolute path"
    )))
}

fn validate_annotation_prefix_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        if value.trim().is_empty() {
            return Err(Error::Config(format!(
                "{name} entries must not be empty"
            )));
        }
    }
    Ok(())
}

fn validate_annotation_map(name: &str, values: &HashMap<String, String>) -> Result<()> {
    for (key, value) in values {
        if key.trim().is_empty() {
            return Err(Error::Config(format!(
                "{name} keys must not be empty"
            )));
        }
        if value.trim().is_empty() {
            return Err(Error::Config(format!(
                "{name}.{key} must not be empty"
            )));
        }
    }
    Ok(())
}

fn validate_capability_list(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        if value.trim().is_empty() {
            return Err(Error::Config(format!(
                "{name} entries must not be empty"
            )));
        }
    }
    Ok(())
}

fn validate_sysctl_assignments(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        parse_sysctl_assignment(value).map_err(|message| {
            Error::Config(format!("{name} contains invalid entry {value:?}: {message}"))
        })?;
    }
    Ok(())
}

fn parse_sysctl_assignment(value: &str) -> std::result::Result<(String, String), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("entry must not be empty".to_string());
    }
    let (key, raw_value) = trimmed
        .split_once('=')
        .or_else(|| trimmed.split_once(char::is_whitespace))
        .ok_or_else(|| "expected key=value or key = value format".to_string())?;
    let key = key.trim();
    let raw_value = raw_value.trim();
    if key.is_empty() {
        return Err("key must not be empty".to_string());
    }
    if raw_value.is_empty() {
        return Err("value must not be empty".to_string());
    }
    Ok((key.to_string(), raw_value.to_string()))
}

fn validate_listen_address(listen: &str, allow_tcp_service: bool) -> Result<()> {
    if let Some(path) = listen.strip_prefix("unix://") {
        return ensure_non_empty("api.listen", path);
    }

    listen
        .parse::<std::net::SocketAddr>()
        .map_err(|err| Error::Config(format!("invalid api.listen {listen}: {err}")))?;

    if !allow_tcp_service {
        return Err(Error::Config(format!(
            "api.listen {listen} requires api.allow_tcp_service = true"
        )));
    }

    Ok(())
}

fn validate_log_filter(filter: &str) -> Result<()> {
    tracing_subscriber::EnvFilter::try_new(filter)
        .map(|_| ())
        .map_err(|err| Error::Config(format!("invalid logging.level {filter}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    #[test]
    fn runtime_handlers_always_include_default_runtime() {
        let config = RuntimeConfig {
            runtime_type: "runc".to_string(),
            handlers: vec!["kata".to_string(), "runc".to_string(), "".to_string()],
            ..Default::default()
        };

        assert_eq!(config.normalized_handlers(), vec!["kata", "runc"]);
    }

    #[test]
    fn resolved_runtimes_merge_default_and_handler_specific_entries() {
        let config = RuntimeConfig {
            runtime_type: "runc".to_string(),
            runtime_path: "/usr/bin/runc".to_string(),
            root: "/run/crius".to_string(),
            monitor_env: vec!["PATH=/usr/bin".to_string()],
            handlers: vec!["kata".to_string(), "crun".to_string()],
            runtimes: HashMap::from([
                (
                    "kata".to_string(),
                    RuntimeHandlerConfig {
                        runtime_path: "/usr/bin/kata-runtime".to_string(),
                        runtime_root: "/run/crius/kata".to_string(),
                        monitor_path: "/usr/bin/kata-shim".to_string(),
                        monitor_env: Some(vec!["RUST_LOG=debug".to_string()]),
                        allowed_annotations: vec!["io.example.runtime/".to_string()],
                        default_annotations: HashMap::from([(
                            "io.example.runtime/default".to_string(),
                            "kata".to_string(),
                        )]),
                        container_create_timeout: Some(15),
                        cni_conf_dir: String::new(),
                        cni_max_conf_num: None,
                        inherit_default_runtime: false,
                    },
                ),
                (
                    "crun".to_string(),
                    RuntimeHandlerConfig {
                        inherit_default_runtime: true,
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };

        let resolved = config.resolved_runtimes().unwrap();
        assert_eq!(
            resolved.get("runc"),
            Some(&ResolvedRuntimeHandlerConfig {
                runtime_path: "/usr/bin/runc".to_string(),
                runtime_root: "/run/crius".to_string(),
                monitor_path: "/usr/bin/crius-shim".to_string(),
                monitor_env: vec!["PATH=/usr/bin".to_string()],
                allowed_annotations: Vec::new(),
                default_annotations: HashMap::new(),
                container_create_timeout: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
            })
        );
        assert_eq!(
            resolved.get("kata"),
            Some(&ResolvedRuntimeHandlerConfig {
                runtime_path: "/usr/bin/kata-runtime".to_string(),
                runtime_root: "/run/crius/kata".to_string(),
                monitor_path: "/usr/bin/kata-shim".to_string(),
                monitor_env: vec!["RUST_LOG=debug".to_string()],
                allowed_annotations: vec!["io.example.runtime/".to_string()],
                default_annotations: HashMap::from([(
                    "io.example.runtime/default".to_string(),
                    "kata".to_string(),
                )]),
                container_create_timeout: MIN_CONTAINER_CREATE_TIMEOUT_SECS,
            })
        );
        assert_eq!(
            resolved.get("crun"),
            Some(&ResolvedRuntimeHandlerConfig {
                runtime_path: "/usr/bin/runc".to_string(),
                runtime_root: "/run/crius".to_string(),
                monitor_path: "/usr/bin/crius-shim".to_string(),
                monitor_env: vec!["PATH=/usr/bin".to_string()],
                allowed_annotations: Vec::new(),
                default_annotations: HashMap::new(),
                container_create_timeout: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
            })
        );
    }

    #[test]
    fn validate_rejects_non_default_handler_without_runtime_table() {
        let mut config = Config::default();
        config.runtime.handlers = vec!["kata".to_string()];

        let err = config
            .validate()
            .expect_err("missing runtime table must fail");
        assert!(err
            .to_string()
            .contains("runtime handler kata requires [runtime.runtimes.kata]"));
    }

    #[test]
    fn validate_rejects_empty_pause_command() {
        let mut config = Config::default();
        config.runtime.pause_command.clear();

        let err = config
            .validate()
            .expect_err("empty pause command must fail");
        assert!(err
            .to_string()
            .contains("runtime.pause_command must not be empty"));
    }

    #[test]
    fn network_config_accepts_legacy_single_config_dir() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            config_dir = "/etc/cni/net.d"
            "#,
        )
        .expect("legacy config_dir should deserialize");

        assert_eq!(config.network.config_dirs, vec!["/etc/cni/net.d"]);
    }

    #[test]
    fn network_config_accepts_default_network_name() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            default_network_name = "crio-bridge"
            "#,
        )
        .expect("default network name should deserialize");

        assert_eq!(
            config.network.default_network_name.as_deref(),
            Some("crio-bridge")
        );
        assert_eq!(
            config.network.cni_config().default_network_name(),
            Some("crio-bridge")
        );
    }

    #[test]
    fn network_config_accepts_crio_default_network_alias() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            cni_default_network = "crio-bridge"
            "#,
        )
        .expect("CRI-O-style default network alias should deserialize");

        assert_eq!(
            config.network.default_network_name.as_deref(),
            Some("crio-bridge")
        );
    }

    #[test]
    fn network_config_accepts_disable_hostport_mapping() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            disable_hostport_mapping = true
            "#,
        )
        .expect("disable_hostport_mapping should deserialize");

        assert!(config.network.disable_hostport_mapping);
        assert!(config.network.cni_config().disable_hostport_mapping());
    }

    #[test]
    fn network_config_accepts_netns_mounts_under_state_dir() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            netns_mounts_under_state_dir = true
            "#,
        )
        .expect("netns_mounts_under_state_dir should deserialize");

        assert!(config.network.netns_mounts_under_state_dir);
        assert!(config.network.cni_config().netns_mounts_under_state_dir());
    }

    #[test]
    fn runtime_handler_config_accepts_cni_conf_dir() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            cni_conf_dir = "/etc/cni/kata.d"
            "#,
        )
        .expect("handler cni_conf_dir should deserialize");

        assert_eq!(
            config.runtime.runtimes["kata"].cni_conf_dir,
            "/etc/cni/kata.d"
        );
    }

    #[test]
    fn runtime_handler_config_accepts_cni_max_conf_num() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            cni_max_conf_num = 2
            "#,
        )
        .expect("handler cni_max_conf_num should deserialize");

        assert_eq!(config.runtime.runtimes["kata"].cni_max_conf_num, Some(2));
    }

    #[test]
    fn runtime_handler_config_accepts_allowed_and_default_annotations() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            allowed_annotations = ["io.example.runtime/", "workload.example/"]

            [runtime.runtimes.kata.default_annotations]
            "io.example.runtime/default" = "kata"
            "#,
        )
        .expect("handler annotations should deserialize");

        assert_eq!(
            config.runtime.runtimes["kata"].allowed_annotations,
            vec![
                "io.example.runtime/".to_string(),
                "workload.example/".to_string()
            ]
        );
        assert_eq!(
            config.runtime.runtimes["kata"]
                .default_annotations
                .get("io.example.runtime/default")
                .map(String::as_str),
            Some("kata")
        );
    }

    #[test]
    fn runtime_handler_config_accepts_monitor_path() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            monitor_path = "/usr/bin/kata-shim"
            "#,
        )
        .expect("handler monitor_path should deserialize");

        assert_eq!(
            config.runtime.runtimes["kata"].monitor_path,
            "/usr/bin/kata-shim"
        );
    }

    #[test]
    fn runtime_handler_config_accepts_container_create_timeout() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            container_create_timeout = 600
            "#,
        )
        .expect("handler container_create_timeout should deserialize");

        assert_eq!(
            config.runtime.runtimes["kata"].container_create_timeout,
            Some(600)
        );
    }

    #[test]
    fn network_config_accepts_max_conf_num() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            max_conf_num = 2
            "#,
        )
        .expect("max_conf_num should deserialize");

        assert_eq!(config.network.max_conf_num, 2);
        assert_eq!(config.network.cni_config().max_conf_num(), 2);
    }

    #[test]
    fn network_config_accepts_conf_template() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            conf_template = "/etc/cni/template.conflist"
            "#,
        )
        .expect("conf_template should deserialize");

        assert_eq!(config.network.conf_template, "/etc/cni/template.conflist");
        assert_eq!(
            config
                .network
                .cni_config()
                .conf_template()
                .map(|path| path.to_string_lossy().to_string()),
            Some("/etc/cni/template.conflist".to_string())
        );
    }

    #[test]
    fn validate_accepts_included_pod_metrics_subset() {
        let mut config = Config::default();
        config.api.included_pod_metrics = vec!["cpu".to_string(), "memory".to_string()];

        config
            .validate()
            .expect("valid included_pod_metrics subset should pass");
    }

    #[test]
    fn validate_rejects_invalid_included_pod_metrics_entry() {
        let mut config = Config::default();
        config.api.included_pod_metrics = vec!["invalid".to_string()];

        let err = config
            .validate()
            .expect_err("invalid included_pod_metrics entry must fail");
        assert!(err.to_string().contains("invalid pod metric"));
    }

    #[test]
    fn validate_rejects_all_mixed_with_other_included_pod_metrics() {
        let mut config = Config::default();
        config.api.included_pod_metrics = vec!["all".to_string(), "cpu".to_string()];

        let err = config
            .validate()
            .expect_err("'all' mixed with others must fail");
        assert!(err
            .to_string()
            .contains("'all' should be the only value in api.included_pod_metrics"));
    }

    #[test]
    fn network_config_accepts_ip_pref() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            ip_pref = "ipv6"
            "#,
        )
        .expect("ip_pref should deserialize");

        assert_eq!(config.network.ip_pref, MainIpPreference::Ipv6);
        assert_eq!(
            config.network.cni_config().ip_pref(),
            MainIpPreference::Ipv6
        );
    }

    #[test]
    fn validate_rejects_invalid_listen_address() {
        let mut config = Config::default();
        config.api.listen = "not-a-listen-address".to_string();

        let err = config.validate().expect_err("invalid listen must fail");
        assert!(err.to_string().contains("invalid api.listen"));
    }

    #[test]
    fn validate_rejects_zero_max_container_log_line_size() {
        let mut config = Config::default();
        config.logging.max_container_log_line_size = 0;

        let err = config
            .validate()
            .expect_err("zero log line size must fail validation");
        assert!(err
            .to_string()
            .contains("logging.max_container_log_line_size must be greater than zero"));
    }

    #[test]
    fn logging_config_accepts_max_container_log_line_size_from_file() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [logging]
            level = "info"
            dir = "/var/log/crius"
            max_container_log_line_size = 8192
            "#,
        )
        .expect("logging config should deserialize");

        assert_eq!(config.logging.max_container_log_line_size, 8192);
    }

    #[test]
    fn runtime_config_accepts_unprivileged_network_defaults_from_file() {
        let raw = r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc"]
            pause_image = "registry.k8s.io/pause:3.9"
            pause_command = "/pause"
            shim_path = "/usr/bin/crius-shim"
            shim_dir = "/var/run/crius/shims"
            monitor_env = ["PATH=/usr/bin", "RUST_LOG=debug"]
            attach_socket_dir = "/var/run/crius/attach"
            container_exits_dir = "/var/run/crius/exits"
            clean_shutdown_file = "/var/lib/crius/clean.shutdown"
            container_stop_timeout = 5
            version_file = "/run/crius/version"
            version_file_persist = "/var/lib/crius/version"
            criu_path = "/usr/sbin/criu"
            criu_image_path = "/var/lib/crius/checkpoint-images"
            criu_work_path = "/var/lib/crius/checkpoint-work"
            enable_criu_support = false
            internal_wipe = false
            internal_repair = false
            bind_mount_prefix = "/host"
            disable_cgroup = true
            tolerate_missing_hugetlb_controller = false
            separate_pull_cgroup = ""
            uid_mappings = "0:100000:65536"
            gid_mappings = "0:200000:65536"
            minimum_mappable_uid = 100000
            minimum_mappable_gid = 200000
            io_uid = 1000
            io_gid = 2000
            pids_limit = 2048
            exec_cpu_affinity = "first"
            read_only = true
            no_pivot = true
            default_env = ["HTTP_PROXY=http://proxy.internal", "LANG=C.UTF-8"]
            default_capabilities = ["CHOWN", "NET_BIND_SERVICE"]
            default_sysctls = ["net.ipv4.ip_forward = 1", "kernel.shm_rmid_forced=1"]
            seccomp_profile = "/etc/crius/seccomp-default.json"
            unset_seccomp_profile = "unconfined"
            log_to_journald = true
            no_sync_log = true
            restrict_oom_score_adj = true
            enable_unprivileged_ports = true
            enable_unprivileged_icmp = true
            "#;
        let dir = tempdir().unwrap();
        let path = dir.path().join("crius.toml");
        fs::write(&path, raw).unwrap();
        let config = Config::load(&path).expect("runtime config should deserialize");

        assert_eq!(
            config.runtime.monitor_env,
            vec!["PATH=/usr/bin", "RUST_LOG=debug"]
        );
        assert_eq!(config.runtime.attach_socket_dir, "/var/run/crius/attach");
        assert_eq!(config.runtime.container_exits_dir, "/var/run/crius/exits");
        assert_eq!(
            config.runtime.clean_shutdown_file,
            "/var/lib/crius/clean.shutdown"
        );
        assert_eq!(config.runtime.version_file, "/run/crius/version");
        assert_eq!(
            config.runtime.version_file_persist,
            "/var/lib/crius/version"
        );
        assert_eq!(config.runtime.criu_path, "/usr/sbin/criu");
        assert_eq!(
            config.runtime.criu_image_path,
            "/var/lib/crius/checkpoint-images"
        );
        assert_eq!(
            config.runtime.criu_work_path,
            "/var/lib/crius/checkpoint-work"
        );
        assert!(!config.runtime.enable_criu_support);
        assert!(!config.runtime.internal_wipe);
        assert!(!config.runtime.internal_repair);
        assert_eq!(config.runtime.bind_mount_prefix, "/host");
        assert!(config.runtime.disable_cgroup);
        assert!(!config.runtime.tolerate_missing_hugetlb_controller);
        assert_eq!(config.runtime.separate_pull_cgroup, "");
        assert_eq!(config.runtime.uid_mappings, "0:100000:65536");
        assert_eq!(config.runtime.gid_mappings, "0:200000:65536");
        assert_eq!(config.runtime.minimum_mappable_uid, 100000);
        assert_eq!(config.runtime.minimum_mappable_gid, 200000);
        assert_eq!(config.runtime.io_uid, 1000);
        assert_eq!(config.runtime.io_gid, 2000);
        assert_eq!(config.runtime.pids_limit, 2048);
        assert_eq!(config.runtime.exec_cpu_affinity, "first");
        assert!(config.runtime.read_only);
        assert!(config.runtime.no_pivot);
        assert_eq!(
            config.runtime.default_env,
            vec![
                "HTTP_PROXY=http://proxy.internal".to_string(),
                "LANG=C.UTF-8".to_string()
            ]
        );
        assert_eq!(
            config.runtime.default_capabilities,
            vec!["CHOWN".to_string(), "NET_BIND_SERVICE".to_string()]
        );
        assert_eq!(
            config.runtime.default_sysctls,
            vec![
                "net.ipv4.ip_forward = 1".to_string(),
                "kernel.shm_rmid_forced=1".to_string()
            ]
        );
        assert_eq!(
            config.runtime.seccomp_profile,
            "/etc/crius/seccomp-default.json"
        );
        assert_eq!(config.runtime.unset_seccomp_profile, "unconfined");
        assert_eq!(
            config.runtime.container_stop_timeout,
            MIN_CONTAINER_STOP_TIMEOUT_SECS
        );
        assert!(config.runtime.log_to_journald);
        assert!(config.runtime.no_sync_log);
        assert!(config.runtime.restrict_oom_score_adj);
        assert!(config.runtime.enable_unprivileged_ports);
        assert!(config.runtime.enable_unprivileged_icmp);
    }

    #[test]
    fn validate_rejects_unsupported_image_driver() {
        let mut config = Config::default();
        config.image.driver = "btrfs".to_string();

        let err = config
            .validate()
            .expect_err("unsupported image driver must fail validation");
        assert!(err.to_string().contains("image.driver must be \"overlay\""));
    }

    #[test]
    fn image_config_accepts_global_auth_file_from_file() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [image]
            driver = "overlay"
            root = "/var/lib/containers/storage"
            global_auth_file = "/var/lib/kubelet/config.json"
            "#,
        )
        .expect("image config should deserialize");

        assert_eq!(
            config.image.global_auth_file,
            "/var/lib/kubelet/config.json"
        );
    }

    #[test]
    fn validate_rejects_relative_image_global_auth_file() {
        let mut config = Config::default();
        config.image.global_auth_file = "config.json".to_string();

        let err = config
            .validate()
            .expect_err("relative image global auth file must fail validation");
        assert!(err
            .to_string()
            .contains("image.global_auth_file must be an absolute path when set"));
    }

    #[test]
    fn validate_rejects_relative_runtime_seccomp_profile() {
        let mut config = Config::default();
        config.runtime.seccomp_profile = "profiles/default.json".to_string();

        let err = config
            .validate()
            .expect_err("relative runtime seccomp profile must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.seccomp_profile must be an absolute path when set"));
    }

    #[test]
    fn validate_rejects_invalid_runtime_unset_seccomp_profile() {
        let mut config = Config::default();
        config.runtime.unset_seccomp_profile = "runtime/unknown".to_string();

        let err = config
            .validate()
            .expect_err("invalid unset seccomp profile selector must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.unset_seccomp_profile must be empty"));
    }

    #[test]
    fn validate_rejects_zero_runtime_pids_limit() {
        let mut config = Config::default();
        config.runtime.pids_limit = 0;

        let err = config
            .validate()
            .expect_err("zero pids limit must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.pids_limit must be -1 or greater than zero"));
    }

    #[test]
    fn validate_rejects_relative_bind_mount_prefix() {
        let mut config = Config::default();
        config.runtime.bind_mount_prefix = "host".to_string();

        let err = config
            .validate()
            .expect_err("relative bind mount prefix must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.bind_mount_prefix must be an absolute path when set"));
    }

    #[test]
    fn validate_rejects_invalid_monitor_env_entry() {
        let mut config = Config::default();
        config.runtime.monitor_env = vec!["INVALID".to_string()];

        let err = config
            .validate()
            .expect_err("monitor env without equals must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.monitor_env entries must be in KEY=value format"));
    }

    #[test]
    fn validate_rejects_invalid_default_env_entry() {
        let mut config = Config::default();
        config.runtime.default_env = vec!["INVALID".to_string()];

        let err = config
            .validate()
            .expect_err("default env without equals must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.default_env entries must be in KEY=value format"));
    }

    #[test]
    fn validate_rejects_invalid_default_sysctl_entry() {
        let mut config = Config::default();
        config.runtime.default_sysctls = vec!["net.ipv4.ip_forward".to_string()];

        let err = config
            .validate()
            .expect_err("invalid default sysctl must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.default_sysctls contains invalid entry"));
    }

    #[test]
    fn validate_rejects_empty_runtime_handler_allowed_annotation_entry() {
        let mut config = Config::default();
        config.runtime.handlers = vec!["kata".to_string()];
        config.runtime.runtimes.insert(
            "kata".to_string(),
            RuntimeHandlerConfig {
                runtime_path: "/usr/bin/kata-runtime".to_string(),
                runtime_root: "/run/crius/kata".to_string(),
                allowed_annotations: vec![String::new()],
                ..Default::default()
            },
        );

        let err = config
            .validate()
            .expect_err("empty runtime handler allowed annotation must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.runtimes.kata.allowed_annotations entries must not be empty"));
    }

    #[test]
    fn validate_rejects_empty_runtime_handler_default_annotation_value() {
        let mut config = Config::default();
        config.runtime.handlers = vec!["kata".to_string()];
        config.runtime.runtimes.insert(
            "kata".to_string(),
            RuntimeHandlerConfig {
                runtime_path: "/usr/bin/kata-runtime".to_string(),
                runtime_root: "/run/crius/kata".to_string(),
                default_annotations: HashMap::from([(
                    "io.example.runtime/default".to_string(),
                    String::new(),
                )]),
                ..Default::default()
            },
        );

        let err = config
            .validate()
            .expect_err("empty runtime handler default annotation value must fail validation");
        assert!(err.to_string().contains(
            "runtime.runtimes.kata.default_annotations.io.example.runtime/default must not be empty"
        ));
    }

    #[test]
    fn validate_rejects_relative_criu_image_path() {
        let mut config = Config::default();
        config.runtime.criu_image_path = "checkpoint-images".to_string();

        let err = config
            .validate()
            .expect_err("relative criu image path must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.criu_image_path must be an absolute path when set"));
    }

    #[test]
    fn validate_rejects_relative_criu_work_path() {
        let mut config = Config::default();
        config.runtime.criu_work_path = "checkpoint-work".to_string();

        let err = config
            .validate()
            .expect_err("relative criu work path must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.criu_work_path must be an absolute path when set"));
    }

    #[test]
    fn validate_rejects_uid_mappings_without_gid_mappings() {
        let mut config = Config::default();
        config.runtime.uid_mappings = "0:100000:65536".to_string();

        let err = config
            .validate()
            .expect_err("uid mappings without gid mappings must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.uid_mappings and runtime.gid_mappings must be configured together"));
    }

    #[test]
    fn validate_rejects_invalid_runtime_id_mapping() {
        let mut config = Config::default();
        config.runtime.uid_mappings = "0:100000:0".to_string();
        config.runtime.gid_mappings = "0:200000:65536".to_string();

        let err = config
            .validate()
            .expect_err("zero-sized id mapping must fail validation");
        assert!(err.to_string().contains("size greater than zero"));
    }

    #[test]
    fn validate_rejects_invalid_minimum_mappable_uid() {
        let mut config = Config::default();
        config.runtime.minimum_mappable_uid = -2;

        let err = config
            .validate()
            .expect_err("minimum mappable uid below -1 must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.minimum_mappable_uid must be -1 or greater"));
    }

    #[test]
    fn validate_rejects_invalid_minimum_mappable_gid() {
        let mut config = Config::default();
        config.runtime.minimum_mappable_gid = -2;

        let err = config
            .validate()
            .expect_err("minimum mappable gid below -1 must fail validation");
        assert!(err
            .to_string()
            .contains("runtime.minimum_mappable_gid must be -1 or greater"));
    }

    #[test]
    fn runtime_workloads_parse_from_file() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            pause_image = "registry.k8s.io/pause:3.9"
            pause_command = "/pause"
            shim_path = "/usr/bin/crius-shim"
            shim_dir = "/var/run/crius/shims"
            attach_socket_dir = "/var/run/crius/attach"
            container_exits_dir = "/var/run/crius/exits"
            clean_shutdown_file = "/var/lib/crius/clean.shutdown"
            version_file = "/run/crius/version"
            version_file_persist = "/var/lib/crius/version"

            [runtime.workloads.management]
            activation_annotation = "target.workload.openshift.io/management"
            annotation_prefix = "resources.workload.openshift.io"
            allowed_annotations = ["workload.example/"]

            [runtime.workloads.management.resources]
            cpushares = 2048
            cpuquota = 50000
            cpuperiod = 100000
            cpuset = "0-1"
            cpulimit = 1000
            "#,
        )
        .expect("workload config should deserialize");

        let workload = config
            .runtime
            .workloads
            .get("management")
            .expect("workload should be present");
        assert_eq!(
            workload.activation_annotation,
            "target.workload.openshift.io/management"
        );
        assert_eq!(
            workload.annotation_prefix,
            "resources.workload.openshift.io"
        );
        assert_eq!(workload.allowed_annotations, vec!["workload.example/"]);
        assert_eq!(workload.resources.cpu_shares, 2048);
        assert_eq!(workload.resources.cpu_limit, 1000);
    }

    #[test]
    fn validate_rejects_duplicate_workload_activation_annotation() {
        let mut config = Config::default();
        config.runtime.workloads.insert(
            "management".to_string(),
            RuntimeWorkloadConfig {
                activation_annotation: "target.workload.openshift.io/management".to_string(),
                annotation_prefix: "resources.workload.openshift.io".to_string(),
                ..Default::default()
            },
        );
        config.runtime.workloads.insert(
            "management-copy".to_string(),
            RuntimeWorkloadConfig {
                activation_annotation: "target.workload.openshift.io/management".to_string(),
                annotation_prefix: "resources.workload.openshift.io".to_string(),
                ..Default::default()
            },
        );

        let err = config
            .validate()
            .expect_err("duplicate workload activation annotation must fail");
        assert!(err.to_string().contains("duplicate activation_annotation"));
    }

    #[test]
    fn validate_rejects_invalid_workload_cpu_period() {
        let mut config = Config::default();
        config.runtime.workloads.insert(
            "management".to_string(),
            RuntimeWorkloadConfig {
                activation_annotation: "target.workload.openshift.io/management".to_string(),
                annotation_prefix: "resources.workload.openshift.io".to_string(),
                resources: RuntimeWorkloadResources {
                    cpu_period: 500,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = config
            .validate()
            .expect_err("invalid workload cpu period must fail");
        assert!(err.to_string().contains("cpuperiod"));
    }

    #[test]
    fn validate_rejects_invalid_exec_cpu_affinity() {
        let mut config = Config::default();
        config.runtime.exec_cpu_affinity = "last".to_string();

        let err = config
            .validate()
            .expect_err("invalid exec cpu affinity must fail");
        assert!(err.to_string().contains("runtime.exec_cpu_affinity"));
    }

    #[test]
    fn validate_rejects_non_empty_separate_pull_cgroup() {
        let mut config = Config::default();
        config.runtime.separate_pull_cgroup = "pod".to_string();

        let err = config
            .validate()
            .expect_err("non-empty separate pull cgroup must fail");
        assert!(err
            .to_string()
            .contains("runtime.separate_pull_cgroup is not supported by crius yet"));
    }

    #[test]
    fn validate_rejects_tcp_listen_when_tcp_service_is_not_explicitly_enabled() {
        let mut config = Config::default();
        config.api.listen = "127.0.0.1:50051".to_string();

        let err = config
            .validate()
            .expect_err("tcp listen without explicit enablement must fail");
        assert!(err
            .to_string()
            .contains("api.listen 127.0.0.1:50051 requires api.allow_tcp_service = true"));
    }

    #[test]
    fn validate_allows_tcp_listen_when_tcp_service_is_explicitly_enabled() {
        let mut config = Config::default();
        config.api.listen = "127.0.0.1:50051".to_string();
        config.api.allow_tcp_service = true;

        config
            .validate()
            .expect("tcp listen should be accepted when explicitly enabled");
    }

    #[test]
    fn api_config_accepts_grpc_message_size_fields() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"
            grpc_max_send_msg_size = 12345
            grpc_max_recv_msg_size = 23456
            "#,
        )
        .expect("grpc message sizes should deserialize");

        assert_eq!(config.api.grpc_max_send_msg_size, 12345);
        assert_eq!(config.api.grpc_max_recv_msg_size, 23456);
    }

    #[test]
    fn validate_rejects_zero_grpc_max_send_msg_size() {
        let mut config = Config::default();
        config.api.grpc_max_send_msg_size = 0;

        let err = config
            .validate()
            .expect_err("zero grpc max send msg size must fail");
        assert!(err
            .to_string()
            .contains("api.grpc_max_send_msg_size must be greater than zero"));
    }

    #[test]
    fn validate_rejects_zero_grpc_max_recv_msg_size() {
        let mut config = Config::default();
        config.api.grpc_max_recv_msg_size = 0;

        let err = config
            .validate()
            .expect_err("zero grpc max recv msg size must fail");
        assert!(err
            .to_string()
            .contains("api.grpc_max_recv_msg_size must be greater than zero"));
    }

    #[test]
    fn streaming_config_accepts_duration_strings() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"
            exec_sync_io_drain_timeout = "250ms"

            [api.streaming]
            address = "127.0.0.1"
            port = 10010
            request_token_ttl = "45s"
            port_forward_stream_creation_timeout = "90s"
            port_forward_idle_timeout = "2h"
            "#,
        )
        .expect("streaming config should deserialize");

        assert_eq!(config.api.streaming.address, "127.0.0.1");
        assert_eq!(config.api.streaming.port, 10010);
        assert_eq!(config.api.exec_sync_io_drain_timeout.as_millis(), 250);
        assert_eq!(config.api.streaming.request_token_ttl.as_secs(), 45);
        assert_eq!(
            config
                .api
                .streaming
                .port_forward_stream_creation_timeout
                .as_secs(),
            90
        );
        assert_eq!(
            config.api.streaming.port_forward_idle_timeout.as_secs(),
            2 * 60 * 60
        );
    }

    #[test]
    fn streaming_config_accepts_tls_fields() {
        let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"

            [api.streaming]
            address = "127.0.0.1"
            port = 0
            enable_tls = true
            tls_cert_file = "/etc/crius/tls/tls.crt"
            tls_key_file = "/etc/crius/tls/tls.key"
            tls_ca_file = "/etc/crius/tls/ca.crt"
            tls_min_version = "VersionTLS13"
            request_token_ttl = "45s"
            port_forward_stream_creation_timeout = "10s"
            port_forward_idle_timeout = "1h"
            "#,
        )
        .expect("streaming TLS config should deserialize");

        assert!(config.api.streaming.enable_tls);
        assert_eq!(config.api.streaming.tls_min_version, "VersionTLS13");
        assert_eq!(
            config.api.streaming.tls_cert_file,
            "/etc/crius/tls/tls.crt"
        );
    }

    #[test]
    fn validate_rejects_streaming_tls_without_cert_or_key() {
        let mut config = Config::default();
        config.api.streaming.enable_tls = true;
        config.api.streaming.tls_cert_file = "/etc/crius/tls/tls.crt".to_string();

        let err = config
            .validate()
            .expect_err("missing TLS key must fail validation");
        assert!(err
            .to_string()
            .contains("api.streaming.tls_key_file must not be empty"));
    }

    #[test]
    fn env_overrides_take_precedence() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempdir().unwrap();
        let _guard = EnvGuard::set_many(&[
            ("CRIUS_LISTEN", "unix:///tmp/env.sock"),
            ("CRIUS_ALLOW_TCP_SERVICE", "true"),
            ("CRIUS_ENABLE_POD_EVENTS", "false"),
            ("CRIUS_INCLUDED_POD_METRICS", "cpu,memory"),
            ("CRIUS_STATS_COLLECTION_PERIOD", "15"),
            ("CRIUS_POD_SANDBOX_METRICS_COLLECTION_PERIOD", "45"),
            ("CRIUS_EXEC_SYNC_IO_DRAIN_TIMEOUT", "125ms"),
            ("CRIUS_STREAM_ADDRESS", "127.0.0.2"),
            ("CRIUS_STREAM_PORT", "10020"),
            ("CRIUS_STREAM_REQUEST_TOKEN_TTL", "40s"),
            ("CRIUS_STREAM_PORT_FORWARD_STREAM_CREATION_TIMEOUT", "70s"),
            ("CRIUS_STREAM_IDLE_TIMEOUT", "3h"),
            ("CRIUS_RUNTIME_HANDLERS", "kata,runsc"),
            ("CRIUS_PAUSE_COMMAND", "/custom-pause"),
            ("CRIUS_MONITOR_ENV", "PATH=/custom/bin,RUST_LOG=trace"),
            ("CRIUS_CGROUP_DRIVER", "systemd"),
            (
                "CRIUS_CNI_CONFIG_DIRS",
                "/etc/cni/net.d:/etc/kubernetes/cni/net.d",
            ),
            ("CRIUS_CNI_PLUGIN_DIRS", "/opt/cni/bin:/usr/libexec/cni"),
            (
                "CRIUS_CNI_CACHE_DIR",
                dir.path().join("cache").to_str().unwrap(),
            ),
            (
                "CRIUS_CNI_CONF_TEMPLATE",
                dir.path().join("template.conflist").to_str().unwrap(),
            ),
            ("CRIUS_CNI_MAX_CONF_NUM", "3"),
            ("CRIUS_CNI_IP_PREF", "ipv4"),
            ("CRIUS_CNI_DEFAULT_NETWORK", "cluster-bridge"),
            ("CRIUS_DISABLE_HOSTPORT_MAPPING", "true"),
            ("CRIUS_LOG_LEVEL", "debug"),
            (
                "CRIUS_LOG_FILE",
                dir.path().join("crius.log").to_str().unwrap(),
            ),
            ("CRIUS_MAX_CONTAINER_LOG_LINE_SIZE", "8192"),
            ("CRIUS_LOG_TO_JOURNALD", "true"),
            ("CRIUS_NO_SYNC_LOG", "true"),
            ("CRIUS_RESTRICT_OOM_SCORE_ADJ", "true"),
            ("CRIUS_ATTACH_SOCKET_DIR", "/run/crius-attach"),
            ("CRIUS_CONTAINER_EXITS_DIR", "/run/crius-exits"),
            (
                "CRIUS_CLEAN_SHUTDOWN_FILE",
                "/var/lib/crius/custom-clean.shutdown",
            ),
            ("CRIUS_VERSION_FILE", "/run/crius/custom-version"),
            (
                "CRIUS_VERSION_FILE_PERSIST",
                "/var/lib/crius/custom-version-persist",
            ),
            ("CRIUS_CRIU_PATH", "/custom/criu"),
            ("CRIUS_CRIU_IMAGE_PATH", "/custom/criu-images"),
            ("CRIUS_CRIU_WORK_PATH", "/custom/criu-work"),
            ("CRIUS_ENABLE_CRIU_SUPPORT", "false"),
            ("CRIUS_INTERNAL_WIPE", "false"),
            ("CRIUS_INTERNAL_REPAIR", "false"),
            ("CRIUS_BIND_MOUNT_PREFIX", "/host-prefix"),
            ("CRIUS_DISABLE_CGROUP", "true"),
            ("CRIUS_TOLERATE_MISSING_HUGETLB_CONTROLLER", "false"),
            ("CRIUS_SEPARATE_PULL_CGROUP", ""),
            ("CRIUS_UID_MAPPINGS", "0:300000:65536"),
            ("CRIUS_GID_MAPPINGS", "0:400000:65536"),
            ("CRIUS_MINIMUM_MAPPABLE_UID", "300000"),
            ("CRIUS_MINIMUM_MAPPABLE_GID", "400000"),
            ("CRIUS_IO_UID", "3000"),
            ("CRIUS_IO_GID", "4000"),
            ("CRIUS_PIDS_LIMIT", "4096"),
            ("CRIUS_EXEC_CPU_AFFINITY", "first"),
            ("CRIUS_READ_ONLY", "true"),
            ("CRIUS_NO_PIVOT", "true"),
            ("CRIUS_CONTAINER_STOP_TIMEOUT", "7"),
            ("CRIUS_ENABLE_UNPRIVILEGED_PORTS", "true"),
            ("CRIUS_ENABLE_UNPRIVILEGED_ICMP", "true"),
        ]);

        let mut config = Config::default();
        config.runtime.runtimes = HashMap::from([
            (
                "kata".to_string(),
                RuntimeHandlerConfig {
                    inherit_default_runtime: true,
                    cni_conf_dir: String::new(),
                    ..Default::default()
                },
            ),
            (
                "runsc".to_string(),
                RuntimeHandlerConfig {
                    inherit_default_runtime: true,
                    cni_conf_dir: String::new(),
                    ..Default::default()
                },
            ),
        ]);
        config.apply_env_overrides().unwrap();

        assert_eq!(config.api.listen, "unix:///tmp/env.sock");
        assert!(config.api.allow_tcp_service);
        assert!(!config.api.enable_pod_events);
        assert_eq!(config.api.included_pod_metrics, vec!["cpu", "memory"]);
        assert_eq!(config.api.stats_collection_period, 15);
        assert_eq!(config.api.pod_sandbox_metrics_collection_period, 45);
        assert_eq!(config.api.exec_sync_io_drain_timeout.as_millis(), 125);
        assert_eq!(config.api.streaming.address, "127.0.0.2");
        assert_eq!(config.api.streaming.port, 10020);
        assert_eq!(config.api.streaming.request_token_ttl.as_secs(), 40);
        assert_eq!(
            config
                .api
                .streaming
                .port_forward_stream_creation_timeout
                .as_secs(),
            70
        );
        assert_eq!(
            config.api.streaming.port_forward_idle_timeout.as_secs(),
            3 * 60 * 60
        );
        assert_eq!(
            config.runtime.normalized_handlers(),
            vec!["kata", "runsc", "runc"]
        );
        assert_eq!(config.runtime.pause_command, "/custom-pause");
        assert_eq!(
            config.runtime.cgroup_driver,
            Some(CgroupDriverConfig::Systemd)
        );
        assert_eq!(
            config.runtime.monitor_env,
            vec!["PATH=/custom/bin", "RUST_LOG=trace"]
        );
        assert_eq!(config.runtime.attach_socket_dir, "/run/crius-attach");
        assert_eq!(config.runtime.container_exits_dir, "/run/crius-exits");
        assert_eq!(
            config.runtime.clean_shutdown_file,
            "/var/lib/crius/custom-clean.shutdown"
        );
        assert_eq!(config.runtime.version_file, "/run/crius/custom-version");
        assert_eq!(
            config.runtime.version_file_persist,
            "/var/lib/crius/custom-version-persist"
        );
        assert_eq!(config.runtime.criu_path, "/custom/criu");
        assert_eq!(config.runtime.criu_image_path, "/custom/criu-images");
        assert_eq!(config.runtime.criu_work_path, "/custom/criu-work");
        assert!(!config.runtime.enable_criu_support);
        assert!(!config.runtime.internal_wipe);
        assert!(!config.runtime.internal_repair);
        assert_eq!(config.runtime.bind_mount_prefix, "/host-prefix");
        assert!(config.runtime.disable_cgroup);
        assert!(!config.runtime.tolerate_missing_hugetlb_controller);
        assert_eq!(config.runtime.separate_pull_cgroup, "");
        assert_eq!(config.runtime.uid_mappings, "0:300000:65536");
        assert_eq!(config.runtime.gid_mappings, "0:400000:65536");
        assert_eq!(config.runtime.minimum_mappable_uid, 300000);
        assert_eq!(config.runtime.minimum_mappable_gid, 400000);
        assert_eq!(config.runtime.io_uid, 3000);
        assert_eq!(config.runtime.io_gid, 4000);
        assert_eq!(config.runtime.pids_limit, 4096);
        assert_eq!(config.runtime.exec_cpu_affinity, "first");
        assert!(config.runtime.read_only);
        assert!(config.runtime.no_pivot);
        assert!(config.runtime.no_sync_log);
        assert_eq!(
            config.runtime.container_stop_timeout,
            MIN_CONTAINER_STOP_TIMEOUT_SECS
        );
        assert!(config.runtime.log_to_journald);
        assert!(config.runtime.restrict_oom_score_adj);
        assert!(config.runtime.enable_unprivileged_ports);
        assert!(config.runtime.enable_unprivileged_icmp);
        assert_eq!(
            config.network.config_dirs,
            vec!["/etc/cni/net.d", "/etc/kubernetes/cni/net.d"]
        );
        assert_eq!(
            config.network.plugin_dirs,
            vec!["/opt/cni/bin", "/usr/libexec/cni"]
        );
        assert_eq!(
            config.network.conf_template,
            dir.path().join("template.conflist").to_string_lossy()
        );
        assert_eq!(
            config
                .network
                .cni_config()
                .conf_template()
                .map(|path| path.to_string_lossy().to_string()),
            Some(
                dir.path()
                    .join("template.conflist")
                    .to_string_lossy()
                    .to_string()
            )
        );
        assert_eq!(config.network.max_conf_num, 3);
        assert_eq!(config.network.ip_pref, MainIpPreference::Ipv4);
        assert_eq!(
            config.network.default_network_name.as_deref(),
            Some("cluster-bridge")
        );
        assert!(config.network.disable_hostport_mapping);
        assert_eq!(config.logging.level, "debug");
        assert!(config.logging.file.is_some());
        assert_eq!(config.logging.max_container_log_line_size, 8192);
    }

    #[test]
    fn env_overrides_can_enable_tcp_service_for_tcp_listen() {
        let _lock = env_lock().lock().unwrap();
        let _guard = EnvGuard::set_many(&[
            ("CRIUS_LISTEN", "127.0.0.1:50051"),
            ("CRIUS_ALLOW_TCP_SERVICE", "true"),
        ]);

        let mut config = Config::default();
        config
            .apply_env_overrides()
            .expect("tcp listen should validate after explicit env enablement");

        assert_eq!(config.api.listen, "127.0.0.1:50051");
        assert!(config.api.allow_tcp_service);
    }

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        saved: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn set_many(values: &[(&str, &str)]) -> Self {
            let saved = values
                .iter()
                .map(|(key, value)| {
                    let previous = std::env::var(key).ok();
                    std::env::set_var(key, value);
                    ((*key).to_string(), previous)
                })
                .collect();
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, previous) in self.saved.drain(..) {
                if let Some(previous) = previous {
                    std::env::set_var(key, previous);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
}
