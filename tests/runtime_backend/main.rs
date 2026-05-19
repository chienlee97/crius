use crius::runtime::{ContainerStatus, RuntimeBackend};

#[path = "../common/mod.rs"]
mod common;
use common::FakeRuntimeBackend;

#[test]
fn fake_runtime_backend_exposes_runtime_contract() {
    let dir = tempfile::tempdir().unwrap();
    let runtime_root = dir.path().join("runtime");
    let backend = FakeRuntimeBackend::new(&runtime_root)
        .with_backend_name("fake-runc")
        .with_status(ContainerStatus::Running);

    assert_eq!(backend.backend_name(), "fake-runc");
    assert_eq!(
        backend.bundle_path_for("container-1"),
        runtime_root.join("container-1")
    );
    assert!(matches!(
        backend.container_status("container-1").unwrap(),
        ContainerStatus::Running
    ));
}
