use std::time::Duration;

use crate::config::NriConfig;

#[derive(Debug, Clone)]
pub struct NriManagerConfig {
    pub enable: bool,
    pub runtime_name: String,
    pub runtime_version: String,
    pub socket_path: String,
    pub plugin_path: String,
    pub plugin_config_path: String,
    pub registration_timeout: Duration,
    pub request_timeout: Duration,
    pub enable_external_connections: bool,
}

impl From<NriConfig> for NriManagerConfig {
    fn from(value: NriConfig) -> Self {
        Self {
            enable: value.enable,
            runtime_name: value.runtime_name,
            runtime_version: value.runtime_version,
            socket_path: value.socket_path,
            plugin_path: value.plugin_path,
            plugin_config_path: value.plugin_config_path,
            registration_timeout: Duration::from_millis(value.registration_timeout_ms.max(0) as u64),
            request_timeout: Duration::from_millis(value.request_timeout_ms.max(0) as u64),
            enable_external_connections: value.enable_external_connections,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NriManagerConfig;
    use crate::config::NriConfig;

    #[test]
    fn converts_from_global_nri_config() {
        let config = NriConfig {
            enable: true,
            runtime_name: "crius".to_string(),
            runtime_version: "0.1.0".to_string(),
            socket_path: "/run/crius/nri.sock".to_string(),
            plugin_path: "/opt/nri/plugins".to_string(),
            plugin_config_path: "/etc/nri/conf.d".to_string(),
            registration_timeout_ms: 4000,
            request_timeout_ms: 1500,
            enable_external_connections: true,
        };

        let mapped = NriManagerConfig::from(config);
        assert!(mapped.enable);
        assert_eq!(mapped.runtime_name, "crius");
        assert_eq!(mapped.registration_timeout.as_millis(), 4000);
        assert_eq!(mapped.request_timeout.as_millis(), 1500);
        assert!(mapped.enable_external_connections);
    }
}
