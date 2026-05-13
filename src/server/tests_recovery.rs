use super::*;

#[test]
fn recovery_startup_flags_default_to_unknown_before_recovery_runs() {
    let service = lifecycle::test_service();

    assert_eq!(service.last_startup_clean_shutdown(), None);
    assert_eq!(service.last_startup_detected_reboot(), None);
    assert_eq!(service.last_startup_detected_upgrade(), None);
    assert_eq!(service.last_startup_attempted_repair(), None);
    assert_eq!(service.last_startup_repair_succeeded(), None);
}
