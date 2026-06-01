use super::*;

#[test]
fn status_feature_flags_cover_core_cri_workflows() {
    let service = lifecycle::test_service();
    let flags = service.runtime_feature_flags();

    assert_eq!(flags["exec"], true);
    assert_eq!(flags["execSync"], true);
    assert_eq!(flags["attach"], true);
    assert_eq!(flags["containerEvents"], true);
    assert_eq!(flags["podLifecycleEvents"], true);
    assert_eq!(flags["updateContainerResources"], true);
}

#[test]
fn security_availability_status_is_structured() {
    let service = lifecycle::test_service();
    let availability = service.security_availability_info();

    assert!(availability["selinuxAvailable"].is_boolean());
    assert!(availability["apparmorAvailable"].is_boolean());
    assert!(availability["seccompAvailable"].is_boolean());
}
