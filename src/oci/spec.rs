//! OCI Runtime Spec 实现
//!
//! 实现OCI运行时规范的配置生成
//! 参考: https://github.com/opencontainers/runtime-spec

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OCI运行时配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// 配置版本
    pub oci_version: String,
    /// 进程配置
    pub process: Option<Process>,
    /// 根文件系统配置
    pub root: Option<Root>,
    /// 主机名
    pub hostname: Option<String>,
    /// 挂载点
    pub mounts: Option<Vec<Mount>>,
    /// OCI hooks
    pub hooks: Option<Hooks>,
    /// Linux命名空间配置
    pub linux: Option<Linux>,
    /// 平台特定配置
    pub annotations: Option<HashMap<String, String>>,
}

/// 进程配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    /// 终端配置
    pub terminal: Option<bool>,
    /// 用户配置
    pub user: Option<User>,
    /// 执行参数
    pub args: Vec<String>,
    /// 环境变量
    pub env: Option<Vec<String>>,
    /// 工作目录
    pub cwd: String,
    /// Capabilities
    pub capabilities: Option<LinuxCapabilities>,
    /// RLimits
    pub rlimits: Option<Vec<Rlimit>>,
    /// OOM 分数调整
    pub oom_score_adj: Option<i32>,
    /// 进程属性
    pub no_new_privileges: Option<bool>,
    /// Apparmor配置
    pub apparmor_profile: Option<String>,
    /// SELinux标签
    pub selinux_label: Option<String>,
}

/// 用户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// 用户UID
    pub uid: u32,
    /// 组GID
    pub gid: u32,
    /// 附加组
    pub additional_gids: Option<Vec<u32>>,
    /// 用户名
    pub username: Option<String>,
}

/// Capabilities配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxCapabilities {
    /// Bounding capabilities
    pub bounding: Option<Vec<String>>,
    /// Effective capabilities
    pub effective: Option<Vec<String>>,
    /// Inheritable capabilities
    pub inheritable: Option<Vec<String>>,
    /// Permitted capabilities
    pub permitted: Option<Vec<String>>,
    /// Ambient capabilities
    pub ambient: Option<Vec<String>>,
}

/// Rlimit配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rlimit {
    /// 资源类型
    #[serde(rename = "type")]
    pub rtype: String,
    /// 硬限制
    pub hard: u64,
    /// 软限制
    pub soft: u64,
}

/// 根文件系统配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    /// 根目录路径
    pub path: String,
    /// 是否只读
    pub readonly: Option<bool>,
}

/// 挂载点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mount {
    /// 目标路径
    pub destination: String,
    /// 源路径
    pub source: Option<String>,
    /// 文件系统类型
    #[serde(rename = "type")]
    pub mount_type: Option<String>,
    /// 挂载选项
    pub options: Option<Vec<String>>,
}

/// OCI hooks 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hooks {
    pub prestart: Option<Vec<Hook>>,
    pub create_runtime: Option<Vec<Hook>>,
    pub create_container: Option<Vec<Hook>>,
    pub start_container: Option<Vec<Hook>>,
    pub poststart: Option<Vec<Hook>>,
    pub poststop: Option<Vec<Hook>>,
}

/// 单个 OCI hook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hook {
    pub path: String,
    pub args: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub timeout: Option<i32>,
}

/// Linux配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Linux {
    /// 命名空间配置
    pub namespaces: Option<Vec<Namespace>>,
    /// UID映射
    pub uid_mappings: Option<Vec<IdMapping>>,
    /// GID映射
    pub gid_mappings: Option<Vec<IdMapping>>,
    /// 设备
    pub devices: Option<Vec<Device>>,
    /// Cgroups配置
    pub cgroups_path: Option<String>,
    /// 资源限制
    pub resources: Option<LinuxResources>,
    /// 根文件系统挂载传播
    pub rootfs_propagation: Option<String>,
    /// Seccomp配置
    pub seccomp: Option<Seccomp>,
    /// 系统控制
    pub sysctl: Option<HashMap<String, String>>,
    /// 挂载标签
    pub mount_label: Option<String>,
    /// Intel RDT资源控制
    pub intel_rdt: Option<IntelRdt>,
}

/// 命名空间配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    /// 命名空间类型
    #[serde(rename = "type")]
    pub ns_type: String,
    /// 命名空间路径（用于加入现有命名空间）
    pub path: Option<String>,
}

/// ID映射配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdMapping {
    /// 容器内起始ID
    pub container_id: u32,
    /// 宿主机起始ID
    pub host_id: u32,
    /// 映射大小
    pub size: u32,
}

/// 设备配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    /// 设备类型
    #[serde(rename = "type")]
    pub device_type: String,
    /// 设备路径
    pub path: String,
    /// 主设备号
    pub major: Option<i64>,
    /// 次设备号
    pub minor: Option<i64>,
    /// 文件权限
    pub file_mode: Option<u32>,
    /// UID
    pub uid: Option<u32>,
    /// GID
    pub gid: Option<u32>,
}

/// Linux资源限制
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinuxResources {
    /// 网络限制
    pub network: Option<LinuxNetwork>,
    /// PID限制
    pub pids: Option<LinuxPids>,
    /// 内存限制
    pub memory: Option<LinuxMemory>,
    /// CPU限制
    pub cpu: Option<LinuxCpu>,
    /// 块IO限制
    pub block_io: Option<LinuxBlockIo>,
    /// 巨页限制
    pub hugepage_limits: Option<Vec<LinuxHugepageLimit>>,
    /// 设备限制
    pub devices: Option<Vec<LinuxDeviceCgroup>>,
    /// RDT资源控制
    pub intel_rdt: Option<LinuxIntelRdt>,
    /// 统一资源限制
    pub unified: Option<HashMap<String, String>>,
}

/// 网络限制
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxNetwork {
    /// 接口优先级
    pub class_id: Option<u32>,
    /// 优先级策略
    pub priorities: Option<Vec<LinuxInterfacePriority>>,
}

/// 接口优先级
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinuxInterfacePriority {
    /// 接口名称
    pub name: String,
    /// 优先级
    pub priority: u32,
}

/// PID限制
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinuxPids {
    /// 最大PID数
    pub limit: i64,
}

/// 内存限制
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxMemory {
    /// 内存限制
    pub limit: Option<i64>,
    /// 交换限制
    pub swap: Option<i64>,
    /// 内核内存限制
    pub kernel: Option<i64>,
    /// TCP内存限制
    pub kernel_tcp: Option<i64>,
    /// 内存软限制
    pub reservation: Option<i64>,
    /// Swappiness
    pub swappiness: Option<u64>,
    /// 禁用OOM killer
    pub disable_oom_killer: Option<bool>,
    /// 使用层级内存
    pub use_hierarchy: Option<bool>,
}

/// CPU限制
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxCpu {
    /// CPU shares
    pub shares: Option<u64>,
    /// CPU配额
    pub quota: Option<i64>,
    /// CPU周期
    pub period: Option<u64>,
    /// 实时运行时间
    pub realtime_runtime: Option<i64>,
    /// 实时周期
    pub realtime_period: Option<u64>,
    /// CPU亲和性
    pub cpus: Option<String>,
    /// MEM亲和性
    pub mems: Option<String>,
}

/// 块IO限制
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxBlockIo {
    /// 权重
    pub weight: Option<u16>,
    /// 叶节点权重
    pub leaf_weight: Option<u16>,
    /// 设备权重
    pub weight_device: Option<Vec<LinuxWeightDevice>>,
    /// 读取速率限制
    pub throttle_read_bps_device: Option<Vec<LinuxThrottleDevice>>,
    /// 写入速率限制
    pub throttle_write_bps_device: Option<Vec<LinuxThrottleDevice>>,
    /// 读取IOPS限制
    pub throttle_read_iops_device: Option<Vec<LinuxThrottleDevice>>,
    /// 写入IOPS限制
    pub throttle_write_iops_device: Option<Vec<LinuxThrottleDevice>>,
}

/// 设备权重
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxWeightDevice {
    /// 主设备号
    pub major: i64,
    /// 次设备号
    pub minor: i64,
    /// 权重
    pub weight: Option<u16>,
    /// 叶节点权重
    pub leaf_weight: Option<u16>,
}

/// 速率限制设备
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxThrottleDevice {
    /// 主设备号
    pub major: i64,
    /// 次设备号
    pub minor: i64,
    /// 速率
    pub rate: u64,
}

/// 巨页限制
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxHugepageLimit {
    /// 页大小
    pub page_size: String,
    /// 限制
    pub limit: u64,
}

/// 设备Cgroup配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxDeviceCgroup {
    /// 是否允许
    pub allow: bool,
    /// 设备类型
    #[serde(rename = "type")]
    pub device_type: Option<String>,
    /// 主设备号
    pub major: Option<i64>,
    /// 次设备号
    pub minor: Option<i64>,
    /// 访问权限
    pub access: Option<String>,
}

/// RDT资源控制
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinuxIntelRdt {
    /// 关闭内存带宽分配
    pub clos_id: Option<String>,
    /// L3缓存模式
    pub l3_cache_schema: Option<String>,
    /// 内存带宽模式
    pub mem_bw_schema: Option<String>,
    /// 是否启用内存带宽分配
    pub enable_cmt: Option<bool>,
    /// 是否启用内存带宽监控
    pub enable_mbm: Option<bool>,
}

/// Seccomp配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seccomp {
    /// Seccomp模式
    pub default_action: String,
    /// 默认 errno 返回值
    pub default_errno_ret: Option<u32>,
    /// 架构
    pub architectures: Option<Vec<String>>,
    /// seccomp flags
    pub flags: Option<Vec<String>>,
    /// listener socket path
    pub listener_path: Option<String>,
    /// listener metadata
    pub listener_metadata: Option<String>,
    /// 系统调用
    pub syscalls: Option<Vec<SeccompSyscall>>,
}

/// Seccomp系统调用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeccompSyscall {
    /// 动作
    pub action: String,
    /// 名称
    pub names: Vec<String>,
    /// 条件
    pub args: Option<Vec<SeccompArg>>,
    /// errno 返回值
    pub errno_ret: Option<u32>,
}

/// Seccomp参数条件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeccompArg {
    /// 索引
    pub index: u32,
    /// 值
    pub value: u64,
    /// 值高32位
    pub value_two: Option<u64>,
    /// 操作符
    pub op: String,
}

/// Intel RDT
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntelRdt {
    /// 是否启用内存带宽分配
    pub l3_cache: Option<bool>,
    /// 是否启用内存带宽监控
    pub mem_bw: Option<bool>,
}

impl Spec {
    /// 创建新的OCI配置
    pub fn new(oci_version: impl Into<String>) -> Self {
        Self {
            oci_version: oci_version.into(),
            process: None,
            root: None,
            hostname: None,
            mounts: None,
            hooks: None,
            linux: None,
            annotations: None,
        }
    }

    /// 默认命名空间配置
    pub fn default_namespaces() -> Vec<Namespace> {
        vec![
            Namespace {
                ns_type: "pid".to_string(),
                path: None,
            },
            Namespace {
                ns_type: "network".to_string(),
                path: None,
            },
            Namespace {
                ns_type: "ipc".to_string(),
                path: None,
            },
            Namespace {
                ns_type: "uts".to_string(),
                path: None,
            },
            Namespace {
                ns_type: "mount".to_string(),
                path: None,
            },
        ]
    }

    /// 默认挂载点
    pub fn default_mounts() -> Vec<Mount> {
        vec![
            Mount {
                destination: "/proc".to_string(),
                source: Some("proc".to_string()),
                mount_type: Some("proc".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                ]),
            },
            Mount {
                destination: "/sys".to_string(),
                source: Some("sysfs".to_string()),
                mount_type: Some("sysfs".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                    "ro".to_string(),
                ]),
            },
            Mount {
                destination: "/dev".to_string(),
                source: Some("tmpfs".to_string()),
                mount_type: Some("tmpfs".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "strictatime".to_string(),
                    "mode=755".to_string(),
                    "size=65536k".to_string(),
                ]),
            },
            Mount {
                destination: "/dev/pts".to_string(),
                source: Some("devpts".to_string()),
                mount_type: Some("devpts".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "newinstance".to_string(),
                    "ptmxmode=0666".to_string(),
                    "mode=0620".to_string(),
                    "gid=5".to_string(),
                ]),
            },
            Mount {
                destination: "/dev/shm".to_string(),
                source: Some("tmpfs".to_string()),
                mount_type: Some("tmpfs".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                    "mode=1777".to_string(),
                    "size=65536k".to_string(),
                ]),
            },
            Mount {
                destination: "/dev/mqueue".to_string(),
                source: Some("mqueue".to_string()),
                mount_type: Some("mqueue".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                ]),
            },
            Mount {
                destination: "/dev/hugepages".to_string(),
                source: Some("hugetlbfs".to_string()),
                mount_type: Some("hugetlbfs".to_string()),
                options: Some(vec![
                    "rw".to_string(),
                    "nosuid".to_string(),
                    "strictatime".to_string(),
                    "mode=1777".to_string(),
                    "size=0".to_string(),
                ]),
            },
            Mount {
                destination: "/sys/fs/cgroup".to_string(),
                source: Some("cgroup".to_string()),
                mount_type: Some("cgroup".to_string()),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                    "relatime".to_string(),
                    "ro".to_string(),
                ]),
            },
        ]
    }

    /// 默认设备
    pub fn default_devices() -> Vec<Device> {
        vec![
            Device {
                device_type: "c".to_string(),
                path: "/dev/null".to_string(),
                major: Some(1),
                minor: Some(3),
                file_mode: Some(0o666),
                uid: None,
                gid: None,
            },
            Device {
                device_type: "c".to_string(),
                path: "/dev/zero".to_string(),
                major: Some(1),
                minor: Some(5),
                file_mode: Some(0o666),
                uid: None,
                gid: None,
            },
            Device {
                device_type: "c".to_string(),
                path: "/dev/random".to_string(),
                major: Some(1),
                minor: Some(8),
                file_mode: Some(0o666),
                uid: None,
                gid: None,
            },
            Device {
                device_type: "c".to_string(),
                path: "/dev/urandom".to_string(),
                major: Some(1),
                minor: Some(9),
                file_mode: Some(0o666),
                uid: None,
                gid: None,
            },
            Device {
                device_type: "c".to_string(),
                path: "/dev/tty".to_string(),
                major: Some(5),
                minor: Some(0),
                file_mode: Some(0o666),
                uid: None,
                gid: None,
            },
        ]
    }

    /// 序列化为JSON字符串
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// 保存到文件
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), crate::error::Error> {
        let json = self.to_json()?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// 从JSON字符串解析
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// 从文件加载
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self, crate::error::Error> {
        let content = std::fs::read_to_string(path)?;
        let spec = Self::from_json(&content)?;
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec_serialization() {
        let spec = Spec::new("1.0.2");
        let json = spec.to_json().unwrap();
        // 使用小写的 ociVersion 匹配 camelCase 序列化
        assert!(
            json.contains("\"ociVersion\":\"1.0.2\"") || json.contains("\"ociVersion\": \"1.0.2\"")
        );
    }

    #[test]
    fn test_default_namespaces() {
        let ns = Spec::default_namespaces();
        assert_eq!(ns.len(), 5);
        assert!(ns.iter().any(|n| n.ns_type == "pid"));
        assert!(ns.iter().any(|n| n.ns_type == "network"));
    }

    #[test]
    fn test_default_mounts() {
        let mounts = Spec::default_mounts();
        assert!(!mounts.is_empty());
        assert!(mounts.iter().any(|m| m.destination == "/proc"));
    }

    #[test]
    fn test_hooks_and_oom_score_adj_round_trip() {
        let mut spec = Spec::new("1.0.2");
        spec.process = Some(Process {
            terminal: Some(false),
            user: None,
            args: vec!["/bin/sh".to_string()],
            env: None,
            cwd: "/".to_string(),
            capabilities: None,
            rlimits: None,
            oom_score_adj: Some(321),
            no_new_privileges: Some(true),
            apparmor_profile: None,
            selinux_label: None,
        });
        spec.hooks = Some(Hooks {
            prestart: Some(vec![Hook {
                path: "/usr/bin/prestart-hook".to_string(),
                args: Some(vec!["hook".to_string()]),
                env: Some(vec!["A=B".to_string()]),
                timeout: Some(5),
            }]),
            create_runtime: None,
            create_container: None,
            start_container: None,
            poststart: None,
            poststop: None,
        });

        let json = spec.to_json().expect("serialize spec");
        let parsed = Spec::from_json(&json).expect("deserialize spec");
        let oom = parsed
            .process
            .as_ref()
            .and_then(|p| p.oom_score_adj)
            .expect("oom_score_adj should exist");
        assert_eq!(oom, 321);
        let hook_path = parsed
            .hooks
            .as_ref()
            .and_then(|h| h.prestart.as_ref())
            .and_then(|hooks| hooks.first())
            .map(|hook| hook.path.clone())
            .expect("prestart hook should exist");
        assert_eq!(hook_path, "/usr/bin/prestart-hook");
    }

    #[test]
    fn test_round_trip_rlimits_namespaces_and_seccomp() {
        let mut spec = Spec::new("1.0.2");
        spec.process = Some(Process {
            terminal: Some(false),
            user: None,
            args: vec!["/bin/sleep".to_string(), "10".to_string()],
            env: Some(vec!["A=B".to_string()]),
            cwd: "/".to_string(),
            capabilities: None,
            rlimits: Some(vec![
                Rlimit {
                    rtype: "RLIMIT_NOFILE".to_string(),
                    hard: 65535,
                    soft: 65535,
                },
                Rlimit {
                    rtype: "RLIMIT_NPROC".to_string(),
                    hard: 4096,
                    soft: 2048,
                },
            ]),
            oom_score_adj: Some(-100),
            no_new_privileges: Some(true),
            apparmor_profile: None,
            selinux_label: None,
        });
        spec.linux = Some(Linux {
            namespaces: Some(vec![
                Namespace {
                    ns_type: "network".to_string(),
                    path: Some("/var/run/netns/test-ns".to_string()),
                },
                Namespace {
                    ns_type: "pid".to_string(),
                    path: None,
                },
            ]),
            uid_mappings: None,
            gid_mappings: None,
            devices: None,
            cgroups_path: Some("/kubepods.slice/pod123".to_string()),
            resources: None,
            rootfs_propagation: None,
            seccomp: Some(Seccomp {
                default_action: "SCMP_ACT_ERRNO".to_string(),
                default_errno_ret: Some(1),
                architectures: Some(vec!["SCMP_ARCH_X86_64".to_string()]),
                flags: Some(vec!["SECCOMP_FILTER_FLAG_LOG".to_string()]),
                listener_path: Some("/run/seccomp-agent.sock".to_string()),
                listener_metadata: Some("pod=test".to_string()),
                syscalls: Some(vec![SeccompSyscall {
                    action: "SCMP_ACT_ALLOW".to_string(),
                    names: vec!["read".to_string(), "write".to_string()],
                    args: Some(vec![SeccompArg {
                        index: 0,
                        value: 1,
                        value_two: Some(2),
                        op: "SCMP_CMP_EQ".to_string(),
                    }]),
                    errno_ret: Some(0),
                }]),
            }),
            sysctl: None,
            mount_label: None,
            intel_rdt: None,
        });
        spec.hooks = Some(Hooks {
            prestart: None,
            create_runtime: Some(vec![Hook {
                path: "/usr/bin/create-runtime-hook".to_string(),
                args: Some(vec!["hook".to_string(), "arg".to_string()]),
                env: None,
                timeout: Some(10),
            }]),
            create_container: None,
            start_container: None,
            poststart: None,
            poststop: None,
        });

        let json = spec.to_json().expect("serialize spec");
        let parsed = Spec::from_json(&json).expect("deserialize spec");

        let rlimits = parsed
            .process
            .as_ref()
            .and_then(|p| p.rlimits.as_ref())
            .expect("rlimits should exist");
        assert_eq!(rlimits.len(), 2);
        assert_eq!(rlimits[0].rtype, "RLIMIT_NOFILE");
        assert_eq!(rlimits[1].soft, 2048);

        let namespaces = parsed
            .linux
            .as_ref()
            .and_then(|l| l.namespaces.as_ref())
            .expect("namespaces should exist");
        assert_eq!(namespaces[0].path.as_deref(), Some("/var/run/netns/test-ns"));

        let seccomp = parsed
            .linux
            .as_ref()
            .and_then(|l| l.seccomp.as_ref())
            .expect("seccomp should exist");
        assert_eq!(seccomp.default_action, "SCMP_ACT_ERRNO");
        assert_eq!(seccomp.default_errno_ret, Some(1));
        let syscall = seccomp
            .syscalls
            .as_ref()
            .and_then(|list| list.first())
            .expect("seccomp syscall should exist");
        assert_eq!(syscall.errno_ret, Some(0));
    }
}
