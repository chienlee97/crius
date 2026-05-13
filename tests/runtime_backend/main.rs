use crius::runtime::{ContainerStatus, RuntimeBackend};
use crius::test_support::FakeRuntimeBackend;

#[test]
fn fake_runtime_backend_exposes_runtime_contract() {
    let backend = FakeRuntimeBackend::new("/tmp/crius-fake-runtime")
        .with_backend_name("fake-runc")
        .with_status(ContainerStatus::Running);

    assert_eq!(backend.backend_name(), "fake-runc");
    assert_eq!(
        backend.bundle_path_for("container-1"),
        std::path::PathBuf::from("/tmp/crius-fake-runtime/container-1")
    );
    assert!(matches!(
        backend.container_status("container-1").unwrap(),
        ContainerStatus::Running
    ));
}
