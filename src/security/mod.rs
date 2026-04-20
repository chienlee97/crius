//! 安全模块
//!
//! 提供容器安全功能支持：
//! - SELinux安全上下文管理
//! - AppArmor配置文件管理
//! - Seccomp系统调用过滤
//! - Capabilities能力管理

use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// 安全配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// SELinux配置
    pub selinux: Option<SelinuxConfig>,
    /// AppArmor配置
    pub apparmor: Option<ApparmorConfig>,
    /// Seccomp配置
    pub seccomp: Option<SeccompConfig>,
    /// Capabilities配置
    pub capabilities: Option<CapabilitiesConfig>,
    /// NoNewPrivileges配置
    pub no_new_privileges: bool,
    /// ReadOnlyRootFilesystem
    pub read_only_root_filesystem: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            selinux: None,
            apparmor: None,
            seccomp: None,
            capabilities: None,
            no_new_privileges: false,
            read_only_root_filesystem: false,
        }
    }
}

/// SELinux配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelinuxConfig {
    /// SELinux模式 (enforcing, permissive, disabled)
    pub mode: SelinuxMode,
    /// 用户
    pub user: String,
    /// 角色
    pub role: String,
    /// 类型
    pub selinux_type: String,
    /// 级别
    pub level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelinuxMode {
    Enforcing,
    Permissive,
    Disabled,
}

impl Default for SelinuxConfig {
    fn default() -> Self {
        Self {
            mode: SelinuxMode::Permissive,
            user: "system_u".to_string(),
            role: "system_r".to_string(),
            selinux_type: "container_t".to_string(),
            level: None,
        }
    }
}

/// AppArmor配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApparmorConfig {
    /// AppArmor配置文件名称
    pub profile: String,
    /// 自定义配置文件内容
    pub custom_profile: Option<String>,
}

impl Default for ApparmorConfig {
    fn default() -> Self {
        Self {
            profile: "crius-default".to_string(),
            custom_profile: None,
        }
    }
}

/// Seccomp配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeccompConfig {
    /// Seccomp模式 (default, unconfined, custom)
    pub mode: SeccompMode,
    /// 自定义seccomp配置文件路径
    pub profile_path: Option<String>,
    /// 自定义seccomp配置内容
    pub profile_content: Option<String>,
    /// 允许的系统调用列表
    pub syscalls: Vec<SyscallRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeccompMode {
    Default,
    Unconfined,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyscallRule {
    pub names: Vec<String>,
    pub action: SeccompAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeccompAction {
    Allow,
    Errno,
    Kill,
    Trap,
    Trace,
    Log,
}

impl Default for SeccompConfig {
    fn default() -> Self {
        Self {
            mode: SeccompMode::Default,
            profile_path: None,
            profile_content: None,
            syscalls: vec![],
        }
    }
}

/// Capabilities配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesConfig {
    /// 添加的能力
    pub add: Vec<String>,
    /// 删除的能力
    pub drop: Vec<String>,
}

impl Default for CapabilitiesConfig {
    fn default() -> Self {
        Self {
            add: vec![],
            drop: vec!["ALL".to_string()],
        }
    }
}

/// 安全管理器
pub struct SecurityManager {
    /// SELinux是否可用
    selinux_available: bool,
    /// AppArmor是否可用
    apparmor_available: bool,
    /// Seccomp是否可用
    seccomp_available: bool,
    /// 默认安全配置
    _default_config: SecurityConfig,
}

impl SecurityManager {
    /// 创建新的安全管理器
    pub fn new() -> Self {
        let selinux_available = Self::check_selinux_available();
        let apparmor_available = Self::check_apparmor_available();
        let seccomp_available = Self::check_seccomp_available();

        info!(
            "SecurityManager initialized: SELinux={}, AppArmor={}, Seccomp={}",
            selinux_available, apparmor_available, seccomp_available
        );

        Self {
            selinux_available,
            apparmor_available,
            seccomp_available,
            _default_config: SecurityConfig::default(),
        }
    }

    /// 检查SELinux是否可用
    fn check_selinux_available() -> bool {
        // 检查/selinux或/sys/fs/selinux是否存在
        Path::new("/sys/fs/selinux").exists() || Path::new("/selinux").exists()
    }

    /// 检查AppArmor是否可用
    fn check_apparmor_available() -> bool {
        // 检查/sys/kernel/security/apparmor是否存在
        Path::new("/sys/kernel/security/apparmor").exists()
    }

    /// 检查Seccomp是否可用
    fn check_seccomp_available() -> bool {
        // 检查/proc/self/status中是否有Seccomp行
        std::fs::read_to_string("/proc/self/status")
            .map(|content| content.contains("Seccomp:"))
            .unwrap_or(false)
    }

    /// 检查SELinux是否可用
    pub fn is_selinux_available(&self) -> bool {
        self.selinux_available
    }

    /// 检查AppArmor是否可用
    pub fn is_apparmor_available(&self) -> bool {
        self.apparmor_available
    }

    /// 检查Seccomp是否可用
    pub fn is_seccomp_available(&self) -> bool {
        self.seccomp_available
    }

    /// 设置SELinux上下文
    pub fn set_selinux_context(
        &self,
        config: &SelinuxConfig,
        process_label: &mut Option<String>,
        mount_label: &mut Option<String>,
    ) -> Result<()> {
        if !self.selinux_available {
            warn!("SELinux is not available on this system");
            return Ok(());
        }

        let context = format!(
            "{}:{}:{}:{}",
            config.user,
            config.role,
            config.selinux_type,
            config.level.as_ref().map(|l| l.as_str()).unwrap_or("s0")
        );

        *process_label = Some(context.clone());
        *mount_label = Some(format!("{}", context));

        debug!("SELinux context set: {}", context);
        Ok(())
    }

    /// 获取SELinux标签字符串
    pub fn get_selinux_label_string(config: &SelinuxConfig) -> String {
        format!(
            "{}:{}:{}:{}",
            config.user,
            config.role,
            config.selinux_type,
            config.level.as_ref().map(|l| l.as_str()).unwrap_or("s0")
        )
    }

    /// 加载AppArmor配置文件
    pub fn load_apparmor_profile(&self, config: &ApparmorConfig) -> Result<()> {
        if !self.apparmor_available {
            warn!("AppArmor is not available on this system");
            return Ok(());
        }

        // 如果使用自定义配置文件，需要先加载
        if let Some(ref content) = config.custom_profile {
            self.load_custom_apparmor_profile(&config.profile, content)?;
        }

        info!("AppArmor profile '{}' loaded", config.profile);
        Ok(())
    }

    /// 加载自定义AppArmor配置文件
    fn load_custom_apparmor_profile(&self, name: &str, content: &str) -> Result<()> {
        let profile_path = format!("/etc/apparmor.d/{}", name);

        // 写入配置文件
        std::fs::write(&profile_path, content).context("Failed to write AppArmor profile")?;

        // 加载配置文件
        let output = std::process::Command::new("apparmor_parser")
            .args(&["-r", "-W", &profile_path])
            .output()
            .context("Failed to execute apparmor_parser")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "Failed to load AppArmor profile: {}",
                stderr
            ));
        }

        debug!("Custom AppArmor profile '{}' loaded", name);
        Ok(())
    }

    /// 卸载AppArmor配置文件
    pub fn unload_apparmor_profile(&self, profile_name: &str) -> Result<()> {
        if !self.apparmor_available {
            return Ok(());
        }

        let output = std::process::Command::new("apparmor_parser")
            .args(&["-R", profile_name])
            .output()
            .context("Failed to execute apparmor_parser")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Failed to unload AppArmor profile: {}", stderr);
        } else {
            debug!("AppArmor profile '{}' unloaded", profile_name);
        }

        Ok(())
    }

    /// 生成OCI Linux安全配置
    pub fn generate_oci_linux_config(&self, config: &SecurityConfig) -> crate::oci::spec::Linux {
        let mut linux = crate::oci::spec::Linux {
            namespaces: None,
            uid_mappings: None,
            gid_mappings: None,
            devices: None,
            cgroups_path: None,
            resources: None,
            rootfs_propagation: None,
            seccomp: None,
            sysctl: None,
            mount_label: None,
            intel_rdt: None,
        };

        // 配置SELinux - 使用mount_label字段
        if let Some(ref selinux) = config.selinux {
            if self.selinux_available {
                let label = Self::get_selinux_label_string(selinux);
                linux.mount_label = Some(label);
            }
        }

        // 配置Seccomp
        if let Some(ref seccomp) = config.seccomp {
            if self.seccomp_available {
                linux.seccomp = Some(self.convert_seccomp_config(seccomp));
            }
        }

        linux
    }

    /// 转换seccomp配置到OCI格式
    fn convert_seccomp_config(&self, config: &SeccompConfig) -> crate::oci::spec::Seccomp {
        let default_action = match config.mode {
            SeccompMode::Default => "SCMP_ACT_ERRNO",
            SeccompMode::Unconfined => "SCMP_ACT_ALLOW",
            SeccompMode::Custom => "SCMP_ACT_ERRNO",
        };

        let syscalls = if config.syscalls.is_empty() {
            None
        } else {
            Some(
                config
                    .syscalls
                    .iter()
                    .map(|rule| crate::oci::spec::SeccompSyscall {
                        action: match rule.action {
                            SeccompAction::Allow => "SCMP_ACT_ALLOW".to_string(),
                            SeccompAction::Errno => "SCMP_ACT_ERRNO".to_string(),
                            SeccompAction::Kill => "SCMP_ACT_KILL".to_string(),
                            SeccompAction::Trap => "SCMP_ACT_TRAP".to_string(),
                            SeccompAction::Trace => "SCMP_ACT_TRACE".to_string(),
                            SeccompAction::Log => "SCMP_ACT_LOG".to_string(),
                        },
                        names: rule.names.clone(),
                        args: None,
                        errno_ret: None,
                    })
                    .collect(),
            )
        };

        crate::oci::spec::Seccomp {
            default_action: default_action.to_string(),
            default_errno_ret: None,
            architectures: Some(vec!["SCMP_ARCH_X86_64".to_string()]),
            flags: None,
            listener_path: None,
            listener_metadata: None,
            syscalls,
        }
    }

    /// 创建默认的安全配置
    pub fn create_default_config(&self) -> SecurityConfig {
        SecurityConfig {
            selinux: if self.selinux_available {
                Some(SelinuxConfig::default())
            } else {
                None
            },
            apparmor: if self.apparmor_available {
                Some(ApparmorConfig::default())
            } else {
                None
            },
            seccomp: if self.seccomp_available {
                Some(SeccompConfig::default())
            } else {
                None
            },
            capabilities: Some(CapabilitiesConfig::default()),
            no_new_privileges: true,
            read_only_root_filesystem: true,
        }
    }
}

impl Default for SecurityManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_manager_creation() {
        let _manager = SecurityManager::new();
        assert!(true);
    }

    #[test]
    fn test_selinux_label_string() {
        let config = SelinuxConfig {
            mode: SelinuxMode::Enforcing,
            user: "system_u".to_string(),
            role: "system_r".to_string(),
            selinux_type: "container_t".to_string(),
            level: Some("s0:c1,c2".to_string()),
        };

        let label = SecurityManager::get_selinux_label_string(&config);
        assert_eq!(label, "system_u:system_r:container_t:s0:c1,c2");
    }

    #[test]
    fn test_default_capabilities() {
        let caps = CapabilitiesConfig::default();
        assert!(caps.drop.contains(&"ALL".to_string()));
    }

    #[test]
    fn test_seccomp_conversion() {
        let manager = SecurityManager::new();
        let config = SeccompConfig::default();
        let seccomp = manager.convert_seccomp_config(&config);

        assert_eq!(seccomp.default_action, "SCMP_ACT_ERRNO");
        assert!(seccomp.architectures.is_some());
    }
}
