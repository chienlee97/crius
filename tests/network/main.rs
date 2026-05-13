use crius::network::CniLoadStatus;
use crius::services::HealthService;

#[test]
fn network_health_suite_uses_health_service_condition() {
    let status = CniLoadStatus {
        checked_at_unix_millis: 1,
        ready: false,
        reason: "NoNetwork".to_string(),
        message: "no configs".to_string(),
        discovered_files: Vec::new(),
        invalid_files: Vec::new(),
        loaded_networks: Vec::new(),
        declared_plugins: Vec::new(),
        missing_plugin_binaries: Vec::new(),
        default_network_name: None,
    };

    let condition = HealthService.network_condition(&status, None, None);
    assert!(!condition.ready);
    assert_eq!(condition.reason, "NoNetwork");
}
