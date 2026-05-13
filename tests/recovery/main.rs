use crius::storage::persistence::PersistenceConfig;

#[test]
fn recovery_suite_has_persistence_config_entrypoint() {
    let dir = tempfile::tempdir().unwrap();
    let config = PersistenceConfig {
        db_path: dir.path().join("crius.db"),
        enable_recovery: true,
        auto_save_interval: 30,
    };

    assert!(config.enable_recovery);
    assert_eq!(config.auto_save_interval, 30);
}
