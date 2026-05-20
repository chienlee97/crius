use crius::rootless::{EffectiveRootlessConfig, NetworkMode};
use crius::services::IntrospectionService;

#[test]
fn rootless_introspection_reports_effective_network_mode() {
    let mut rootless = EffectiveRootlessConfig::disabled();
    rootless.enabled = true;
    rootless.network_mode = NetworkMode::Pasta;

    let value = IntrospectionService.rootless(&rootless);

    assert_eq!(value["enabled"], true);
    assert_eq!(value["networkMode"], "pasta");
    assert_eq!(value["networkModeSupported"], true);
    assert_eq!(value["networkModeReason"], "RootlessPastaSupported");
    assert_eq!(value["networkHelperPath"], "pasta");
}
