use std::path::Path;

use serde::Serialize;

use crate::image::{PullCgroupEffectiveConfig, PullCgroupMode, PullCgroupScopeRecord};
use crate::network::CniLoadStatus;
use crate::proto::runtime::v1::RuntimeCondition;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HealthCondition {
    pub ready: bool,
    pub reason: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InternalHealthCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub ready: bool,
    pub reason: String,
    pub message: String,
}

impl InternalHealthCondition {
    fn ready(condition_type: &str, reason: &str, message: String) -> Self {
        Self {
            condition_type: condition_type.to_string(),
            ready: true,
            reason: reason.to_string(),
            message,
        }
    }

    fn not_ready(condition_type: &str, reason: &str, message: String) -> Self {
        Self {
            condition_type: condition_type.to_string(),
            ready: false,
            reason: reason.to_string(),
            message,
        }
    }
}

impl HealthCondition {
    pub fn runtime_condition(&self) -> RuntimeCondition {
        RuntimeCondition {
            r#type: "RuntimeReady".to_string(),
            status: self.ready,
            reason: self.reason.clone(),
            message: self.message.clone(),
        }
    }

    pub fn network_condition(&self) -> RuntimeCondition {
        RuntimeCondition {
            r#type: "NetworkReady".to_string(),
            status: self.ready,
            reason: self.reason.clone(),
            message: self.message.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HealthService;

impl HealthService {
    pub fn runtime_condition_for_path(
        &self,
        path: &Path,
        version: Option<String>,
    ) -> HealthCondition {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(_) => {
                return HealthCondition {
                    ready: false,
                    reason: "RuntimeBinaryMissing".to_string(),
                    message: format!("runtime binary does not exist at {}", path.display()),
                };
            }
        };

        if !metadata.is_file() {
            return HealthCondition {
                ready: false,
                reason: "RuntimeBinaryInvalid".to_string(),
                message: format!("runtime path is not a regular file: {}", path.display()),
            };
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return HealthCondition {
                    ready: false,
                    reason: "RuntimeBinaryNotExecutable".to_string(),
                    message: format!("runtime binary is not executable: {}", path.display()),
                };
            }
        }

        let version = version.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        HealthCondition {
            ready: true,
            reason: "RuntimeIsReady".to_string(),
            message: format!(
                "runtime binary is available at {} ({})",
                path.display(),
                version
            ),
        }
    }

    pub fn network_condition(
        &self,
        cni_status: &CniLoadStatus,
        reload_error: Option<&str>,
        cni_watch_error: Option<&str>,
    ) -> HealthCondition {
        if let Some(error) = reload_error {
            return Self::network_reload_error_condition(error);
        }

        if let Some(error) = cni_watch_error.filter(|error| Self::is_cni_template_error(error)) {
            return HealthCondition {
                ready: false,
                reason: "CniTemplateRenderFailed".to_string(),
                message: error.to_string(),
            };
        }

        let (ready, reason, message) = cni_status.condition();
        if !ready {
            return HealthCondition {
                ready,
                reason,
                message,
            };
        }

        if let Some(error) = cni_watch_error {
            return HealthCondition {
                ready: false,
                reason: "NetworkConfigReloadFailed".to_string(),
                message: error.to_string(),
            };
        }

        HealthCondition {
            ready,
            reason,
            message,
        }
    }

    fn network_reload_error_condition(error: &str) -> HealthCondition {
        let reason = if Self::is_cni_template_error(error) {
            "CniTemplateRenderFailed"
        } else {
            "ConfigReloadFailed"
        };
        HealthCondition {
            ready: false,
            reason: reason.to_string(),
            message: error.to_string(),
        }
    }

    fn is_cni_template_error(error: &str) -> bool {
        error.contains("CNI config template")
            || error.contains("generated CNI config")
            || error.contains("PodCIDR")
    }

    pub fn watcher_status(
        &self,
        reload_watcher_active: bool,
        watcher_status: serde_json::Value,
        watcher_backoff_count: u32,
        watcher_next_retry_unix_millis: Option<i64>,
        watcher_last_error: Option<&str>,
        reload_error: Option<&str>,
        cni_watch_error: Option<&str>,
        shim_reconnect_supported: bool,
    ) -> serde_json::Value {
        serde_json::json!({
            "reloadWatcherActive": reload_watcher_active,
            "watcherStatus": watcher_status,
            "watcherBackoffCount": watcher_backoff_count,
            "watcherNextRetryUnixMillis": watcher_next_retry_unix_millis,
            "watcherLastError": watcher_last_error,
            "lastReloadError": reload_error,
            "lastCniWatchError": cni_watch_error,
            "shimReconnectSupported": shim_reconnect_supported,
        })
    }

    pub fn image_condition(&self, image_root: &Path) -> InternalHealthCondition {
        self.directory_condition(
            "ImageReady",
            image_root,
            DirectoryConditionReasons {
                ready: "ImageStoreReady",
                missing: "ImageStoreMissing",
                invalid: "ImageStoreInvalid",
                read_only: "ImageStoreReadOnly",
                label: "image store",
            },
        )
    }

    pub fn snapshot_condition(&self, snapshot_root: &Path) -> InternalHealthCondition {
        self.directory_condition(
            "SnapshotReady",
            snapshot_root,
            DirectoryConditionReasons {
                ready: "SnapshotRootReady",
                missing: "SnapshotRootMissing",
                invalid: "SnapshotRootInvalid",
                read_only: "SnapshotRootReadOnly",
                label: "snapshot root",
            },
        )
    }

    pub fn shim_condition(
        &self,
        shim_work_dir: &Path,
        reconnect_supported: bool,
    ) -> InternalHealthCondition {
        if !reconnect_supported {
            return InternalHealthCondition::not_ready(
                "ShimReady",
                "ShimReconnectUnsupported",
                "shim reconnect support is disabled".to_string(),
            );
        }

        self.directory_condition(
            "ShimReady",
            shim_work_dir,
            DirectoryConditionReasons {
                ready: "ShimWorkDirReady",
                missing: "ShimWorkDirMissing",
                invalid: "ShimWorkDirInvalid",
                read_only: "ShimWorkDirReadOnly",
                label: "shim work directory",
            },
        )
    }

    pub fn recovery_condition(
        &self,
        attempted_repair: Option<bool>,
        repair_succeeded: Option<bool>,
    ) -> InternalHealthCondition {
        match (attempted_repair, repair_succeeded) {
            (Some(true), Some(false)) => InternalHealthCondition::not_ready(
                "RecoveryReady",
                "RecoveryRepairFailed",
                "startup persistence repair was attempted and failed".to_string(),
            ),
            (Some(true), Some(true)) => InternalHealthCondition::ready(
                "RecoveryReady",
                "RecoveryRepairSucceeded",
                "startup persistence repair completed successfully".to_string(),
            ),
            (Some(false), _) => InternalHealthCondition::ready(
                "RecoveryReady",
                "RecoveryRepairNotNeeded",
                "startup recovery did not require persistence repair".to_string(),
            ),
            (None, _) => InternalHealthCondition::ready(
                "RecoveryReady",
                "RecoveryStatusUnavailable",
                "startup recovery status has not been recorded".to_string(),
            ),
            (Some(true), None) => InternalHealthCondition::ready(
                "RecoveryReady",
                "RecoveryRepairInProgress",
                "startup persistence repair has not recorded a final result".to_string(),
            ),
        }
    }

    pub fn pull_cgroup_condition(
        &self,
        effective: &PullCgroupEffectiveConfig,
        last_scope: Option<&PullCgroupScopeRecord>,
    ) -> InternalHealthCondition {
        if let Some(error) = last_scope.and_then(|scope| scope.error.as_deref()) {
            return InternalHealthCondition::not_ready(
                "PullCgroupReady",
                "PullCgroupScopeFailed",
                error.to_string(),
            );
        }

        if effective.rootless_degraded {
            return InternalHealthCondition::ready(
                "PullCgroupReady",
                "PullCgroupDisabledByRootless",
                format!(
                    "runtime.separate_pull_cgroup={} is disabled because rootless mode disables cgroups",
                    effective.configured
                ),
            );
        }

        if effective.disable_cgroup_degraded {
            return InternalHealthCondition::ready(
                "PullCgroupReady",
                "PullCgroupDisabledByRuntimeConfig",
                format!(
                    "runtime.separate_pull_cgroup={} is disabled because runtime cgroups are disabled",
                    effective.configured
                ),
            );
        }

        match effective.mode {
            PullCgroupMode::Disabled => InternalHealthCondition::ready(
                "PullCgroupReady",
                "PullCgroupDisabled",
                "separate pull cgroup is not configured".to_string(),
            ),
            PullCgroupMode::Pod | PullCgroupMode::Path => InternalHealthCondition::ready(
                "PullCgroupReady",
                "PullCgroupReady",
                format!(
                    "separate pull cgroup is enabled with {:?} mode",
                    effective.mode
                ),
            ),
        }
    }

    pub fn extended_conditions(
        &self,
        image_root: &Path,
        snapshot_root: &Path,
        shim_work_dir: &Path,
        attempted_repair: Option<bool>,
        repair_succeeded: Option<bool>,
        shim_reconnect_supported: bool,
    ) -> Vec<InternalHealthCondition> {
        vec![
            self.image_condition(image_root),
            self.snapshot_condition(snapshot_root),
            self.shim_condition(shim_work_dir, shim_reconnect_supported),
            self.recovery_condition(attempted_repair, repair_succeeded),
        ]
    }

    fn directory_condition(
        &self,
        condition_type: &str,
        path: &Path,
        reasons: DirectoryConditionReasons,
    ) -> InternalHealthCondition {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(_) => {
                return InternalHealthCondition::not_ready(
                    condition_type,
                    reasons.missing,
                    format!("{} does not exist at {}", reasons.label, path.display()),
                );
            }
        };

        if !metadata.is_dir() {
            return InternalHealthCondition::not_ready(
                condition_type,
                reasons.invalid,
                format!("{} is not a directory: {}", reasons.label, path.display()),
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o222 == 0 {
                return InternalHealthCondition::not_ready(
                    condition_type,
                    reasons.read_only,
                    format!("{} is not writable: {}", reasons.label, path.display()),
                );
            }
        }

        InternalHealthCondition::ready(
            condition_type,
            reasons.ready,
            format!("{} is available at {}", reasons.label, path.display()),
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct DirectoryConditionReasons {
    ready: &'static str,
    missing: &'static str,
    invalid: &'static str,
    read_only: &'static str,
    label: &'static str,
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn runtime_condition_reports_missing_binary() {
        let service = HealthService;
        let condition = service.runtime_condition_for_path(Path::new("/definitely/missing"), None);

        assert!(!condition.ready);
        assert_eq!(condition.reason, "RuntimeBinaryMissing");
    }

    #[test]
    fn network_condition_prefers_reload_error() {
        let service = HealthService;
        let status = CniLoadStatus {
            checked_at_unix_millis: 1,
            ready: true,
            reason: "NetworkReady".to_string(),
            message: "ok".to_string(),
            discovered_files: Vec::new(),
            invalid_files: Vec::new(),
            loaded_networks: Vec::new(),
            declared_plugins: Vec::new(),
            missing_plugin_binaries: Vec::new(),
            default_network_name: None,
        };

        let condition = service.network_condition(&status, Some("reload failed"), None);
        assert!(!condition.ready);
        assert_eq!(condition.reason, "ConfigReloadFailed");
        assert_eq!(condition.message, "reload failed");
    }

    #[test]
    fn network_condition_reports_cni_template_reload_failure() {
        let service = HealthService;
        let status = ready_cni_status();

        let condition = service.network_condition(
            &status,
            Some("Failed to render CNI config template: invalid PodCIDR"),
            None,
        );

        assert!(!condition.ready);
        assert_eq!(condition.reason, "CniTemplateRenderFailed");
    }

    #[test]
    fn network_condition_preserves_cni_plugin_missing_reason() {
        let service = HealthService;
        let status = CniLoadStatus {
            ready: false,
            reason: "CNIPluginMissing".to_string(),
            message: "missing or non-executable CNI plugin binary/binaries: bridge".to_string(),
            declared_plugins: vec!["bridge".to_string()],
            missing_plugin_binaries: vec!["bridge".to_string()],
            ..ready_cni_status()
        };

        let condition = service.network_condition(&status, None, Some("missing plugin bridge"));

        assert!(!condition.ready);
        assert_eq!(condition.reason, "CNIPluginMissing");
        assert_eq!(condition.message, status.message);
    }

    #[test]
    fn network_condition_preserves_cni_config_invalid_reason() {
        let service = HealthService;
        let status = CniLoadStatus {
            ready: false,
            reason: "CNIConfigInvalid".to_string(),
            message: "failed to parse 1 CNI config file(s): broken.conflist".to_string(),
            invalid_files: vec!["broken.conflist".to_string()],
            ..ready_cni_status()
        };

        let condition = service.network_condition(&status, None, Some("parse failed"));

        assert!(!condition.ready);
        assert_eq!(condition.reason, "CNIConfigInvalid");
    }

    #[cfg(unix)]
    #[test]
    fn runtime_condition_accepts_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let service = HealthService;
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime");
        std::fs::write(&path, "#!/bin/sh\n").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();

        let condition = service.runtime_condition_for_path(&path, Some("runtime 1.0".to_string()));
        assert!(condition.ready);
        assert_eq!(condition.reason, "RuntimeIsReady");
    }

    fn ready_cni_status() -> CniLoadStatus {
        CniLoadStatus {
            checked_at_unix_millis: 1,
            ready: true,
            reason: "CNINetworkConfigReady".to_string(),
            message: "loaded 1 CNI network config(s) and 1 plugin type(s)".to_string(),
            discovered_files: vec!["10-test.conflist".to_string()],
            invalid_files: Vec::new(),
            loaded_networks: vec!["test".to_string()],
            declared_plugins: vec!["bridge".to_string()],
            missing_plugin_binaries: Vec::new(),
            default_network_name: Some("test".to_string()),
        }
    }

    #[test]
    fn extended_conditions_report_subsystem_readiness() {
        let service = HealthService;
        let dir = tempdir().unwrap();
        let image_root = dir.path().join("images");
        let snapshot_root = image_root.join("snapshots");
        let shim_work_dir = dir.path().join("shims");
        std::fs::create_dir_all(&snapshot_root).unwrap();
        std::fs::create_dir_all(&shim_work_dir).unwrap();

        let conditions = service.extended_conditions(
            &image_root,
            &snapshot_root,
            &shim_work_dir,
            Some(false),
            None,
            true,
        );

        assert_eq!(conditions.len(), 4);
        assert!(conditions.iter().all(|condition| condition.ready));
        assert!(conditions
            .iter()
            .any(|condition| condition.condition_type == "ImageReady"));
        assert!(conditions
            .iter()
            .any(|condition| condition.condition_type == "SnapshotReady"));
        assert!(conditions
            .iter()
            .any(|condition| condition.condition_type == "ShimReady"));
        assert!(conditions
            .iter()
            .any(|condition| condition.condition_type == "RecoveryReady"));
    }

    #[test]
    fn extended_conditions_report_missing_snapshot_root_and_recovery_failure() {
        let service = HealthService;
        let dir = tempdir().unwrap();
        let image_root = dir.path().join("images");
        let shim_work_dir = dir.path().join("shims");
        std::fs::create_dir_all(&image_root).unwrap();
        std::fs::create_dir_all(&shim_work_dir).unwrap();

        let conditions = service.extended_conditions(
            &image_root,
            &image_root.join("snapshots"),
            &shim_work_dir,
            Some(true),
            Some(false),
            true,
        );

        let snapshot = conditions
            .iter()
            .find(|condition| condition.condition_type == "SnapshotReady")
            .unwrap();
        assert!(!snapshot.ready);
        assert_eq!(snapshot.reason, "SnapshotRootMissing");

        let recovery = conditions
            .iter()
            .find(|condition| condition.condition_type == "RecoveryReady")
            .unwrap();
        assert!(!recovery.ready);
        assert_eq!(recovery.reason, "RecoveryRepairFailed");
    }

    #[test]
    fn pull_cgroup_condition_reports_scope_failure() {
        let service = HealthService;
        let effective = PullCgroupEffectiveConfig {
            configured: "kubepods/pod1".to_string(),
            mode: PullCgroupMode::Path,
            enabled: true,
            rootless_degraded: false,
            disable_cgroup_degraded: false,
            cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        };
        let last_scope = PullCgroupScopeRecord {
            configured: "kubepods/pod1".to_string(),
            mode: PullCgroupMode::Path,
            effective_path: Some("/sys/fs/cgroup/kubepods/pod1".to_string()),
            entered: false,
            restored: false,
            error: Some("failed to create pull cgroup".to_string()),
            at_unix_millis: 7,
        };

        let condition = service.pull_cgroup_condition(&effective, Some(&last_scope));

        assert_eq!(condition.condition_type, "PullCgroupReady");
        assert!(!condition.ready);
        assert_eq!(condition.reason, "PullCgroupScopeFailed");
        assert_eq!(condition.message, "failed to create pull cgroup");
    }
}
