//! Rootless模式支持模块
//!
//! 提供无特权容器运行支持：
//! - UID/GID映射配置
//! - 用户命名空间管理
//! - newuidmap/newgidmap调用
//! - 无特权容器生命周期管理
//! - 存储驱动支持（overlay2 in user namespace）

use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Rootless配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootlessConfig {
    /// 启用rootless模式
    pub enabled: bool,
    /// UID映射（容器UID -> 宿主机UID）
    pub uid_mappings: Vec<IdMapping>,
    /// GID映射（容器GID -> 宿主机GID）
    pub gid_mappings: Vec<IdMapping>,
    /// 子UID范围起始
    pub sub_uid_start: u32,
    /// 子UID数量
    pub sub_uid_count: u32,
    /// 子GID范围起始
    pub sub_gid_start: u32,
    /// 子GID数量
    pub sub_gid_count: u32,
    /// 是否自动配置subuid/subgid
    pub auto_configure_subids: bool,
    /// 是否使用fuse-overlayfs
    pub use_fuse_overlayfs: bool,
    /// 网络模式（slirp4netns/pasta/none）
    pub network_mode: NetworkMode,
    /// 显式覆盖 XDG_RUNTIME_DIR；为空时自动推导。
    pub xdg_runtime_dir: String,
    /// 显式覆盖 XDG_DATA_HOME；为空时自动推导。
    pub xdg_data_home: String,
    /// 显式覆盖 rootless 存储根目录；为空时使用 XDG_DATA_HOME 下的 `crius/storage`。
    pub storage_root: String,
    /// 显式覆盖 rootless runtime state 根目录；为空时使用 XDG_RUNTIME_DIR 下的 `crius`。
    pub runtime_root: String,
    /// 显式覆盖 rootless netns 目录；为空时使用 runtime state 下的 `netns`。
    pub netns_dir: String,
    /// slirp4netns 二进制路径或命令名。
    pub slirp4netns_path: String,
    /// pasta 二进制路径或命令名。
    pub pasta_path: String,
}

/// ID映射
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdMapping {
    /// 容器内ID
    pub container_id: u32,
    /// 宿主机ID
    pub host_id: u32,
    /// 映射大小
    pub size: u32,
}

/// 网络模式
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NetworkMode {
    /// 使用slirp4netns
    Slirp4netns,
    /// 使用pasta
    Pasta,
    /// 使用rootlesskit的网络模式
    Rootlesskit,
    /// 无网络
    None,
}

impl NetworkMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Slirp4netns => "slirp4netns",
            Self::Pasta => "pasta",
            Self::Rootlesskit => "rootlesskit",
            Self::None => "none",
        }
    }

    pub fn is_supported(&self) -> bool {
        match self {
            Self::Slirp4netns | Self::Pasta | Self::None => true,
            Self::Rootlesskit => false,
        }
    }

    pub fn support_reason(&self) -> &'static str {
        match self {
            Self::Slirp4netns => "RootlessSlirp4netnsSupported",
            Self::Pasta => "RootlessPastaSupported",
            Self::None => "RootlessNetworkDisabled",
            Self::Rootlesskit => "RootlesskitUnsupported",
        }
    }
}

impl Default for RootlessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            uid_mappings: vec![],
            gid_mappings: vec![],
            sub_uid_start: 100000,
            sub_uid_count: 65536,
            sub_gid_start: 100000,
            sub_gid_count: 65536,
            auto_configure_subids: true,
            use_fuse_overlayfs: true,
            network_mode: NetworkMode::Slirp4netns,
            xdg_runtime_dir: String::new(),
            xdg_data_home: String::new(),
            storage_root: String::new(),
            runtime_root: String::new(),
            netns_dir: String::new(),
            slirp4netns_path: "slirp4netns".to_string(),
            pasta_path: "pasta".to_string(),
        }
    }
}

impl RootlessConfig {
    /// 创建默认的rootless配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 启用rootless模式
    pub fn enable(mut self) -> Self {
        self.enabled = true;
        // 设置默认的UID/GID映射
        if self.uid_mappings.is_empty() {
            self.uid_mappings = vec![
                IdMapping {
                    container_id: 0,
                    host_id: std::process::id(),
                    size: 1,
                },
                IdMapping {
                    container_id: 1,
                    host_id: self.sub_uid_start,
                    size: self.sub_uid_count,
                },
            ];
        }
        if self.gid_mappings.is_empty() {
            self.gid_mappings = vec![
                IdMapping {
                    container_id: 0,
                    host_id: std::process::id(),
                    size: 1,
                },
                IdMapping {
                    container_id: 1,
                    host_id: self.sub_gid_start,
                    size: self.sub_gid_count,
                },
            ];
        }
        self
    }

    /// 设置子UID范围
    pub fn with_sub_uid(mut self, start: u32, count: u32) -> Self {
        self.sub_uid_start = start;
        self.sub_uid_count = count;
        self
    }

    /// 设置子GID范围
    pub fn with_sub_gid(mut self, start: u32, count: u32) -> Self {
        self.sub_gid_start = start;
        self.sub_gid_count = count;
        self
    }

    /// 设置网络模式
    pub fn with_network_mode(mut self, mode: NetworkMode) -> Self {
        self.network_mode = mode;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveRootlessConfig {
    pub enabled: bool,
    pub current_uid: u32,
    pub current_gid: u32,
    pub in_user_namespace: bool,
    pub xdg_runtime_dir: PathBuf,
    pub xdg_data_home: PathBuf,
    pub storage_root: PathBuf,
    pub runtime_root: PathBuf,
    pub netns_dir: PathBuf,
    pub use_fuse_overlayfs: bool,
    pub network_mode: NetworkMode,
    pub slirp4netns_path: PathBuf,
    pub pasta_path: PathBuf,
    pub disable_cgroup: bool,
    pub tolerate_missing_hugetlb_controller: bool,
}

impl EffectiveRootlessConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            current_uid: RootlessManager::current_uid(),
            current_gid: RootlessManager::current_gid(),
            in_user_namespace: is_in_new_user_namespace(),
            xdg_runtime_dir: PathBuf::new(),
            xdg_data_home: PathBuf::new(),
            storage_root: PathBuf::new(),
            runtime_root: PathBuf::new(),
            netns_dir: PathBuf::new(),
            use_fuse_overlayfs: false,
            network_mode: NetworkMode::None,
            slirp4netns_path: PathBuf::from("slirp4netns"),
            pasta_path: PathBuf::from("pasta"),
            disable_cgroup: false,
            tolerate_missing_hugetlb_controller: false,
        }
    }

    pub fn resolve(config: &RootlessConfig) -> Result<Self> {
        let current_uid = RootlessManager::current_uid();
        let current_gid = RootlessManager::current_gid();
        if !config.enabled {
            return Ok(Self::disabled());
        }

        let xdg_runtime_dir = if !config.xdg_runtime_dir.trim().is_empty() {
            PathBuf::from(config.xdg_runtime_dir.trim())
        } else if let Ok(value) = std::env::var("XDG_RUNTIME_DIR") {
            PathBuf::from(value)
        } else {
            PathBuf::from(format!("/run/user/{current_uid}"))
        };
        let xdg_data_home = if !config.xdg_data_home.trim().is_empty() {
            PathBuf::from(config.xdg_data_home.trim())
        } else if let Ok(value) = std::env::var("XDG_DATA_HOME") {
            PathBuf::from(value)
        } else {
            PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string()))
                .join(".local")
                .join("share")
        };
        let runtime_root = if !config.runtime_root.trim().is_empty() {
            PathBuf::from(config.runtime_root.trim())
        } else {
            xdg_runtime_dir.join("crius")
        };
        let storage_root = if !config.storage_root.trim().is_empty() {
            PathBuf::from(config.storage_root.trim())
        } else {
            xdg_data_home.join("crius").join("storage")
        };
        let netns_dir = if !config.netns_dir.trim().is_empty() {
            PathBuf::from(config.netns_dir.trim())
        } else {
            runtime_root.join("netns")
        };

        Ok(Self {
            enabled: true,
            current_uid,
            current_gid,
            in_user_namespace: is_in_new_user_namespace(),
            xdg_runtime_dir,
            xdg_data_home,
            storage_root,
            runtime_root,
            netns_dir,
            use_fuse_overlayfs: config.use_fuse_overlayfs,
            network_mode: config.network_mode.clone(),
            slirp4netns_path: PathBuf::from(config.slirp4netns_path.trim()),
            pasta_path: PathBuf::from(config.pasta_path.trim()),
            disable_cgroup: true,
            tolerate_missing_hugetlb_controller: true,
        })
    }
}

/// Rootless管理器
pub struct RootlessManager {
    /// 配置
    config: RootlessConfig,
    /// subuid/subgid配置路径
    subuid_path: PathBuf,
    subgid_path: PathBuf,
    /// 是否已配置subids
    subids_configured: bool,
}

impl RootlessManager {
    /// 创建新的rootless管理器
    pub fn new(config: RootlessConfig) -> Result<Self> {
        let uid = Self::current_uid();
        let subuid_path = PathBuf::from("/etc/subuid");
        let subgid_path = PathBuf::from("/etc/subgid");

        let mut manager = Self {
            config,
            subuid_path,
            subgid_path,
            subids_configured: false,
        };

        if manager.config.enabled {
            // 检查当前是否在rootless模式
            if uid == 0 {
                warn!("Running as root, rootless features may not be needed");
            }

            // 检查subuid/subgid配置
            manager.subids_configured = manager.check_subids()?;

            if !manager.subids_configured && manager.config.auto_configure_subids {
                warn!("SubUID/GID not configured, attempting auto-configuration");
                // 这里可以添加自动配置逻辑，但通常需要root权限
            }

            info!(
                "RootlessManager initialized: enabled={}, subids_configured={}",
                manager.config.enabled, manager.subids_configured
            );
        }

        Ok(manager)
    }

    /// 检查是否在rootless模式
    pub fn is_rootless(&self) -> bool {
        self.config.enabled
    }

    /// 检查subuid/subgid是否已配置
    fn check_subids(&self) -> Result<bool> {
        let username = Self::current_username();

        // 检查subuid
        let subuid_configured = if self.subuid_path.exists() {
            let content = fs::read_to_string(&self.subuid_path)?;
            content.lines().any(|line| line.starts_with(&username))
        } else {
            false
        };

        // 检查subgid
        let subgid_configured = if self.subgid_path.exists() {
            let content = fs::read_to_string(&self.subgid_path)?;
            content.lines().any(|line| line.starts_with(&username))
        } else {
            false
        };

        Ok(subuid_configured && subgid_configured)
    }

    /// 获取当前用户名
    fn current_username() -> String {
        std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| format!("uid{}", Self::current_uid()))
    }

    /// 获取当前用户的UID
    pub fn current_uid() -> u32 {
        // Use /proc/self/status to get UID
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|l| l.starts_with("Uid:"))
                    .and_then(|line| {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|uid| uid.parse().ok())
                    })
            })
            .unwrap_or(0)
    }

    /// 获取当前用户的GID
    pub fn current_gid() -> u32 {
        // Use /proc/self/status to get GID
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|l| l.starts_with("Gid:"))
                    .and_then(|line| {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|gid| gid.parse().ok())
                    })
            })
            .unwrap_or(0)
    }

    /// 生成OCI Linux UID映射配置
    pub fn generate_uid_mappings(&self) -> Vec<crate::oci::spec::IdMapping> {
        self.config
            .uid_mappings
            .iter()
            .map(|m| crate::oci::spec::IdMapping {
                container_id: m.container_id,
                host_id: m.host_id,
                size: m.size,
            })
            .collect()
    }

    /// 生成OCI Linux GID映射配置
    pub fn generate_gid_mappings(&self) -> Vec<crate::oci::spec::IdMapping> {
        self.config
            .gid_mappings
            .iter()
            .map(|m| crate::oci::spec::IdMapping {
                container_id: m.container_id,
                host_id: m.host_id,
                size: m.size,
            })
            .collect()
    }

    /// 配置OCI spec以支持rootless
    pub fn configure_oci_spec(&self, spec: &mut crate::oci::spec::Spec) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // 确保有Linux配置
        if spec.linux.is_none() {
            spec.linux = Some(crate::oci::spec::Linux {
                namespaces: None,
                uid_mappings: None,
                gid_mappings: None,
                devices: None,
                net_devices: None,
                cgroups_path: None,
                resources: None,
                rootfs_propagation: None,
                seccomp: None,
                sysctl: None,
                mount_label: None,
                masked_paths: None,
                readonly_paths: None,
                intel_rdt: None,
            });
        }

        let linux = spec.linux.as_mut().unwrap();

        // 设置用户命名空间
        let mut found_user_ns = false;
        if let Some(ref mut namespaces) = linux.namespaces {
            for ns in namespaces.iter_mut() {
                if ns.ns_type == "user" {
                    found_user_ns = true;
                    break;
                }
            }
            if !found_user_ns {
                namespaces.push(crate::oci::spec::Namespace {
                    ns_type: "user".to_string(),
                    path: None,
                });
            }
        } else {
            linux.namespaces = Some(vec![crate::oci::spec::Namespace {
                ns_type: "user".to_string(),
                path: None,
            }]);
        }

        // 设置UID映射
        linux.uid_mappings = Some(self.generate_uid_mappings());

        // 设置GID映射
        linux.gid_mappings = Some(self.generate_gid_mappings());

        // 在rootless模式下禁用某些特权功能
        // 禁用cgroup资源限制（除非使用cgroup v2和systemd）
        if let Some(ref mut _resources) = linux.resources {
            // 在rootless模式下，可能需要调整资源限制
            debug!("Configuring resources for rootless mode");
        }

        // 使用fuse-overlayfs或其他rootless友好的存储驱动
        if self.config.use_fuse_overlayfs {
            debug!("Using fuse-overlayfs for rootless storage");
        }

        debug!("OCI spec configured for rootless mode");
        Ok(())
    }

    /// 获取新uidmap命令参数
    pub fn get_newuidmap_args(&self, pid: u32) -> Vec<String> {
        let mut args = vec![pid.to_string()];

        for mapping in &self.config.uid_mappings {
            args.push(format!(
                "{} {} {}",
                mapping.container_id, mapping.host_id, mapping.size
            ));
        }

        args
    }

    /// 获取newgidmap命令参数
    pub fn get_newgidmap_args(&self, pid: u32) -> Vec<String> {
        let mut args = vec![pid.to_string()];

        for mapping in &self.config.gid_mappings {
            args.push(format!(
                "{} {} {}",
                mapping.container_id, mapping.host_id, mapping.size
            ));
        }

        args
    }

    /// 执行newuidmap
    pub fn exec_newuidmap(&self, pid: u32) -> Result<()> {
        let args = self.get_newuidmap_args(pid);

        let output = Command::new("newuidmap")
            .args(&args)
            .output()
            .context("Failed to execute newuidmap")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("newuidmap failed: {}", stderr));
        }

        debug!("newuidmap executed successfully for pid {}", pid);
        Ok(())
    }

    /// 执行newgidmap
    pub fn exec_newgidmap(&self, pid: u32) -> Result<()> {
        let args = self.get_newgidmap_args(pid);

        let output = Command::new("newgidmap")
            .args(&args)
            .output()
            .context("Failed to execute newgidmap")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("newgidmap failed: {}", stderr));
        }

        debug!("newgidmap executed successfully for pid {}", pid);
        Ok(())
    }

    /// 检查所需的rootless工具是否可用
    pub fn check_rootless_tools(&self) -> Result<RootlessToolsStatus> {
        let mut status = RootlessToolsStatus {
            newuidmap: false,
            newgidmap: false,
            slirp4netns: false,
            pasta: false,
            fuse_overlayfs: false,
            rootlesskit: false,
        };

        // 检查newuidmap
        status.newuidmap = Command::new("which")
            .arg("newuidmap")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        // 检查newgidmap
        status.newgidmap = Command::new("which")
            .arg("newgidmap")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        // 检查slirp4netns
        status.slirp4netns = Command::new("which")
            .arg("slirp4netns")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        // 检查pasta
        status.pasta = Command::new("which")
            .arg("pasta")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        // 检查fuse-overlayfs
        status.fuse_overlayfs = Command::new("which")
            .arg("fuse-overlayfs")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        // 检查rootlesskit
        status.rootlesskit = Command::new("which")
            .arg("rootlesskit")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        info!("Rootless tools status: {:?}", status);
        Ok(status)
    }

    /// 获取配置
    pub fn config(&self) -> &RootlessConfig {
        &self.config
    }

    /// 检查subids是否已配置
    pub fn are_subids_configured(&self) -> bool {
        self.subids_configured
    }
}

/// Rootless工具状态
#[derive(Debug, Clone)]
pub struct RootlessToolsStatus {
    pub newuidmap: bool,
    pub newgidmap: bool,
    pub slirp4netns: bool,
    pub pasta: bool,
    pub fuse_overlayfs: bool,
    pub rootlesskit: bool,
}

/// 检查系统是否支持rootless模式
pub fn is_rootless_supported() -> bool {
    // 检查用户命名空间支持
    let user_ns_supported = Path::new("/proc/self/ns/user").exists();

    // 检查是否可以写入/proc/self/uid_map
    let uid_map_writable = Path::new("/proc/self/uid_map").exists();

    user_ns_supported && uid_map_writable
}

/// 获取当前用户名
pub fn current_username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| format!("uid{}", RootlessManager::current_uid()))
}

/// 检测是否在新用户命名空间中
pub fn is_in_new_user_namespace() -> bool {
    // 通过检查/proc/self/uid_map来判断
    if let Ok(content) = std::fs::read_to_string("/proc/self/uid_map") {
        // 如果uid_map不是默认值（0 0 4294967295），则在新命名空间中
        !content.trim().eq("0 0 4294967295")
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rootless_config_creation() {
        let config = RootlessConfig::new().enable();
        assert!(config.enabled);
        assert!(!config.uid_mappings.is_empty());
        assert!(!config.gid_mappings.is_empty());
    }

    #[test]
    fn test_id_mapping_generation() {
        let config = RootlessConfig::new().enable().with_sub_uid(100000, 65536);

        let manager = RootlessManager::new(config).unwrap();
        let uid_mappings = manager.generate_uid_mappings();

        assert!(!uid_mappings.is_empty());
        // 第一个映射应该是root -> 当前用户
        assert_eq!(uid_mappings[0].container_id, 0);
    }

    #[test]
    fn test_rootless_support_check() {
        // 这个测试只是确保函数不会panic
        let _supported = is_rootless_supported();
    }

    #[test]
    fn test_network_mode() {
        let config = RootlessConfig::new().with_network_mode(NetworkMode::Slirp4netns);

        assert_eq!(config.network_mode, NetworkMode::Slirp4netns);
    }

    #[test]
    fn effective_rootless_config_uses_xdg_layout_by_default() {
        let config = RootlessConfig {
            enabled: true,
            xdg_runtime_dir: "/tmp/crius-rootless-runtime".to_string(),
            xdg_data_home: "/tmp/crius-rootless-data".to_string(),
            ..RootlessConfig::default()
        };

        let effective = EffectiveRootlessConfig::resolve(&config).unwrap();
        assert!(effective.enabled);
        assert_eq!(
            effective.runtime_root,
            PathBuf::from("/tmp/crius-rootless-runtime").join("crius")
        );
        assert_eq!(
            effective.storage_root,
            PathBuf::from("/tmp/crius-rootless-data")
                .join("crius")
                .join("storage")
        );
        assert_eq!(
            effective.netns_dir,
            PathBuf::from("/tmp/crius-rootless-runtime")
                .join("crius")
                .join("netns")
        );
    }
}
