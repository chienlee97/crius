//! 安全模块
//!
//! 提供容器安全功能支持：
//! - SELinux安全上下文管理
//! - AppArmor配置文件管理
//! - Seccomp系统调用过滤
//! - Capabilities能力管理

use std::path::Path;

use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

pub mod apparmor;
pub mod cdi;
pub mod devices;
pub mod resource_classes;
pub mod seccomp;
pub mod selinux;
pub mod spec_patch;

/// 安全配置
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostCapabilityState {
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostCapabilityProbe {
    pub name: String,
    pub state: HostCapabilityState,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostCapabilityReport {
    pub seccomp: HostCapabilityProbe,
    pub apparmor: HostCapabilityProbe,
    pub selinux: HostCapabilityProbe,
    pub cdi: HostCapabilityProbe,
    pub blockio: HostCapabilityProbe,
    pub rdt: HostCapabilityProbe,
    pub devices: HostCapabilityProbe,
    pub cgroup: HostCapabilityProbe,
}

impl HostCapabilityProbe {
    pub fn available(name: &str, reason: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            state: HostCapabilityState::Available,
            reason: reason.into(),
        }
    }

    pub fn degraded(name: &str, reason: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            state: HostCapabilityState::Degraded,
            reason: reason.into(),
        }
    }

    pub fn unavailable(name: &str, reason: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            state: HostCapabilityState::Unavailable,
            reason: reason.into(),
        }
    }

    pub fn is_available(&self) -> bool {
        self.state == HostCapabilityState::Available
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecurityFeatureRequest {
    pub cdi_devices: Vec<String>,
    pub blockio_class: Option<String>,
    pub rdt_class: Option<String>,
    pub nri_cdi_devices: Vec<String>,
    pub nri_blockio_class: Option<String>,
    pub nri_rdt_class: Option<String>,
    pub nri_rdt_adjustment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityFeatureGate {
    Allowed,
    Rejected { reason: String },
    Degraded { reasons: Vec<String> },
}

impl SecurityFeatureRequest {
    pub fn is_empty(&self) -> bool {
        self.cdi_devices.is_empty()
            && self.blockio_class.is_none()
            && self.rdt_class.is_none()
            && self.nri_cdi_devices.is_empty()
            && self.nri_blockio_class.is_none()
            && self.nri_rdt_class.is_none()
            && self.nri_rdt_adjustment.is_none()
    }
}

impl SecurityFeatureGate {
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
        }
    }

    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

impl HostCapabilityReport {
    pub fn degraded_probes(&self) -> Vec<&HostCapabilityProbe> {
        [
            &self.seccomp,
            &self.apparmor,
            &self.selinux,
            &self.cdi,
            &self.blockio,
            &self.rdt,
            &self.devices,
            &self.cgroup,
        ]
        .into_iter()
        .filter(|probe| !probe.is_available())
        .collect()
    }

    pub fn degraded_capability_names(&self) -> Vec<String> {
        self.degraded_probes()
            .into_iter()
            .map(|probe| probe.name.clone())
            .collect()
    }

    pub fn degraded_reasons(&self) -> Vec<String> {
        self.degraded_probes()
            .into_iter()
            .map(|probe| format!("{}: {}", probe.name, probe.reason))
            .collect()
    }

    pub fn evaluate_feature_request(
        &self,
        request: &SecurityFeatureRequest,
    ) -> SecurityFeatureGate {
        if request.is_empty() {
            return SecurityFeatureGate::Allowed;
        }

        if (!request.cdi_devices.is_empty() || !request.nri_cdi_devices.is_empty())
            && !self.cdi.is_available()
        {
            return SecurityFeatureGate::reject(format!(
                "CDI devices requested but host CDI capability is {}: {}",
                capability_state_name(self.cdi.state),
                self.cdi.reason
            ));
        }

        if (request.blockio_class.is_some() || request.nri_blockio_class.is_some())
            && !self.blockio.is_available()
        {
            return SecurityFeatureGate::reject(format!(
                "blockio class requested but host blockio capability is {}: {}",
                capability_state_name(self.blockio.state),
                self.blockio.reason
            ));
        }

        if (request.rdt_class.is_some()
            || request.nri_rdt_class.is_some()
            || request.nri_rdt_adjustment.is_some())
            && !self.rdt.is_available()
        {
            return SecurityFeatureGate::Degraded {
                reasons: vec![format!(
                    "RDT class requested but host RDT capability is {}: {}",
                    capability_state_name(self.rdt.state),
                    self.rdt.reason
                )],
            };
        }

        SecurityFeatureGate::Allowed
    }
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
        selinux::is_available()
    }

    /// 检查AppArmor是否可用
    fn check_apparmor_available() -> bool {
        apparmor::is_available()
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

    pub fn host_capability_report(
        &self,
        cdi_spec_dirs: &[String],
        blockio_config_path: Option<&str>,
    ) -> HostCapabilityReport {
        HostCapabilityReport {
            seccomp: if self.seccomp_available {
                HostCapabilityProbe::available("seccomp", "/proc/self/status exposes Seccomp")
            } else {
                HostCapabilityProbe::unavailable(
                    "seccomp",
                    "/proc/self/status does not expose Seccomp",
                )
            },
            apparmor: if self.apparmor_available {
                HostCapabilityProbe::available("apparmor", "AppArmor securityfs is mounted")
            } else {
                HostCapabilityProbe::degraded(
                    "apparmor",
                    "AppArmor securityfs is unavailable; runtime/default profiles are skipped",
                )
            },
            selinux: if self.selinux_available {
                HostCapabilityProbe::available("selinux", "SELinux status is available")
            } else {
                HostCapabilityProbe::degraded(
                    "selinux",
                    "SELinux is unavailable or disabled; labels are skipped unless explicitly required",
                )
            },
            cdi: probe_cdi(cdi_spec_dirs),
            blockio: probe_blockio(blockio_config_path),
            rdt: probe_rdt(),
            devices: probe_devices(),
            cgroup: probe_cgroup(),
        }
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
            config.level.as_deref().unwrap_or("s0")
        );

        *process_label = Some(context.clone());
        *mount_label = Some(context.clone());

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
            config.level.as_deref().unwrap_or("s0")
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
            .args(["-r", "-W", &profile_path])
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
            .args(["-R", profile_name])
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

fn capability_state_name(state: HostCapabilityState) -> &'static str {
    match state {
        HostCapabilityState::Available => "available",
        HostCapabilityState::Degraded => "degraded",
        HostCapabilityState::Unavailable => "unavailable",
    }
}

fn probe_cdi(cdi_spec_dirs: &[String]) -> HostCapabilityProbe {
    let configured_dirs: Vec<_> = cdi_spec_dirs
        .iter()
        .map(|dir| dir.trim())
        .filter(|dir| !dir.is_empty())
        .collect();
    if configured_dirs.is_empty() {
        return HostCapabilityProbe::degraded("cdi", "no CDI spec directories are configured");
    }
    if configured_dirs.iter().any(|dir| Path::new(dir).exists()) {
        HostCapabilityProbe::available("cdi", "at least one CDI spec directory exists")
    } else {
        HostCapabilityProbe::degraded(
            "cdi",
            format!(
                "configured CDI spec directories do not exist: {}",
                configured_dirs.join(",")
            ),
        )
    }
}

fn probe_blockio(config_path: Option<&str>) -> HostCapabilityProbe {
    match resource_classes::effective_blockio_config_path(config_path) {
        Some(path) if path.exists() => HostCapabilityProbe::available(
            "blockio",
            format!("blockio config is present at {}", path.display()),
        ),
        Some(path) => HostCapabilityProbe::degraded(
            "blockio",
            format!("blockio config path does not exist: {}", path.display()),
        ),
        None => HostCapabilityProbe::degraded("blockio", "no blockio config path is configured"),
    }
}

fn probe_rdt() -> HostCapabilityProbe {
    let path = Path::new("/sys/fs/resctrl");
    if path.exists() {
        HostCapabilityProbe::available("rdt", "/sys/fs/resctrl exists")
    } else {
        HostCapabilityProbe::degraded("rdt", "/sys/fs/resctrl is not mounted")
    }
}

fn probe_devices() -> HostCapabilityProbe {
    let dev = Path::new("/dev");
    if dev.exists() {
        HostCapabilityProbe::available("devices", "/dev is available")
    } else {
        HostCapabilityProbe::unavailable("devices", "/dev is missing")
    }
}

fn probe_cgroup() -> HostCapabilityProbe {
    let v2 = Path::new("/sys/fs/cgroup/cgroup.controllers");
    let v1 = Path::new("/sys/fs/cgroup/cpu");
    if v2.exists() {
        HostCapabilityProbe::available("cgroup", "cgroup v2 controllers are visible")
    } else if v1.exists() {
        HostCapabilityProbe::available("cgroup", "cgroup v1 cpu controller is visible")
    } else {
        HostCapabilityProbe::degraded("cgroup", "no cgroup controller path is visible")
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

    #[test]
    fn host_capability_report_marks_missing_optional_features_degraded() {
        let manager = SecurityManager::new();
        let report = manager.host_capability_report(&[], None);

        assert_eq!(report.cdi.state, HostCapabilityState::Degraded);
        assert_eq!(report.blockio.state, HostCapabilityState::Degraded);
        assert!(report.cdi.reason.contains("no CDI spec directories"));
        assert!(report.blockio.reason.contains("no blockio config"));
    }

    #[test]
    fn host_capability_report_marks_configured_cdi_and_blockio_available() {
        let dir = tempfile::tempdir().unwrap();
        let blockio = dir.path().join("blockio.json");
        std::fs::write(&blockio, "{}").unwrap();
        let manager = SecurityManager::new();
        let report = manager.host_capability_report(
            &[dir.path().display().to_string()],
            Some(blockio.to_str().unwrap()),
        );

        assert_eq!(report.cdi.state, HostCapabilityState::Available);
        assert_eq!(report.blockio.state, HostCapabilityState::Available);
    }

    #[test]
    fn host_capability_policy_rejects_requested_cdi_when_probe_is_degraded() {
        let manager = SecurityManager::new();
        let report = manager.host_capability_report(&[], None);

        let gate = report.evaluate_feature_request(&SecurityFeatureRequest {
            cdi_devices: vec!["vendor.com/device=gpu0".to_string()],
            ..Default::default()
        });

        match gate {
            SecurityFeatureGate::Rejected { reason } => {
                assert!(reason.contains("CDI devices requested"));
                assert!(reason.contains("degraded"));
            }
            other => panic!("expected rejected gate, got {other:?}"),
        }
    }

    #[test]
    fn host_capability_policy_rejects_blockio_and_degrades_rdt_without_host_support() {
        let manager = SecurityManager::new();
        let report = manager.host_capability_report(&["/tmp".to_string()], None);

        let blockio = report.evaluate_feature_request(&SecurityFeatureRequest {
            blockio_class: Some("gold".to_string()),
            ..Default::default()
        });
        assert!(matches!(blockio, SecurityFeatureGate::Rejected { .. }));

        let rdt_probe = HostCapabilityReport {
            rdt: HostCapabilityProbe::degraded("rdt", "resctrl not mounted"),
            ..report
        };
        let rdt = rdt_probe.evaluate_feature_request(&SecurityFeatureRequest {
            nri_rdt_adjustment: Some("silver".to_string()),
            ..Default::default()
        });
        match rdt {
            SecurityFeatureGate::Degraded { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(reasons[0].contains("RDT class requested"));
                assert!(reasons[0].contains("resctrl not mounted"));
            }
            other => panic!("expected degraded gate, got {other:?}"),
        }
    }

    #[test]
    fn host_capability_report_exposes_degraded_names_and_reasons() {
        let report = HostCapabilityReport {
            seccomp: HostCapabilityProbe::available("seccomp", "ok"),
            apparmor: HostCapabilityProbe::degraded("apparmor", "missing securityfs"),
            selinux: HostCapabilityProbe::available("selinux", "ok"),
            cdi: HostCapabilityProbe::available("cdi", "ok"),
            blockio: HostCapabilityProbe::available("blockio", "ok"),
            rdt: HostCapabilityProbe::available("rdt", "ok"),
            devices: HostCapabilityProbe::available("devices", "ok"),
            cgroup: HostCapabilityProbe::available("cgroup", "ok"),
        };

        assert_eq!(
            report.degraded_capability_names(),
            vec!["apparmor".to_string()]
        );
        assert_eq!(
            report.degraded_reasons(),
            vec!["apparmor: missing securityfs".to_string()]
        );
    }
}
