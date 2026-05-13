use std::path::Path;

use serde::Serialize;

use crate::network::CniLoadStatus;
use crate::proto::runtime::v1::RuntimeCondition;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HealthCondition {
    pub ready: bool,
    pub reason: String,
    pub message: String,
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
            return HealthCondition {
                ready: false,
                reason: "ConfigReloadFailed".to_string(),
                message: error.to_string(),
            };
        }
        if let Some(error) = cni_watch_error {
            return HealthCondition {
                ready: false,
                reason: "NetworkConfigReloadFailed".to_string(),
                message: error.to_string(),
            };
        }

        let (ready, reason, message) = cni_status.condition();
        HealthCondition {
            ready,
            reason,
            message,
        }
    }

    pub fn watcher_status(
        &self,
        reload_watcher_active: bool,
        reload_error: Option<&str>,
        cni_watch_error: Option<&str>,
        shim_reconnect_supported: bool,
    ) -> serde_json::Value {
        serde_json::json!({
            "reloadWatcherActive": reload_watcher_active,
            "lastReloadError": reload_error,
            "lastCniWatchError": cni_watch_error,
            "shimReconnectSupported": shim_reconnect_supported,
        })
    }
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
}
