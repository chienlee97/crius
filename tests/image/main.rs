use crius::image::content_store::{ContentStore, FsContentStore};
use crius::image::snapshotter::{SnapshotState, SnapshotUsage, Snapshotter};

#[path = "../common/mod.rs"]
mod common;
use common::{FakeContentStore, FakeSnapshotter};

#[test]
fn fake_content_store_round_trips_blob_metadata() {
    let store = FakeContentStore::new().unwrap();
    let digest = FsContentStore::compute_digest(b"hello");

    store
        .put_blob(&digest, "application/octet-stream", b"hello")
        .unwrap();

    assert_eq!(store.stat_blob(&digest).unwrap().size, 5);
}

#[test]
fn fake_snapshotter_reports_configured_usage() {
    let dir = tempfile::tempdir().unwrap();
    let snapshotter =
        FakeSnapshotter::new(dir.path().join("snapshots")).with_usage(SnapshotUsage {
            used_bytes: 10,
            inodes_used: 2,
        });

    assert_eq!(snapshotter.usage().unwrap().used_bytes, 10);
    assert_eq!(snapshotter.usage().unwrap().inodes_used, 2);
}

#[test]
fn fake_snapshotter_tracks_snapshot_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("snapshots");
    let snapshotter = FakeSnapshotter::new(&root).with_usage(SnapshotUsage {
        used_bytes: 10,
        inodes_used: 2,
    });

    let prepared = snapshotter
        .prepare("container-1", "image-1", &root.join("container-1/rootfs"))
        .unwrap();
    assert_eq!(prepared.key, "container-1");

    let mount = snapshotter.mount("container-1").unwrap();
    assert_eq!(mount.key, "container-1");
    assert_eq!(mount.mountpoint, root.join("container-1/rootfs"));
    assert!(!mount.readonly);

    let info = snapshotter.commit("container-1").unwrap();
    assert_eq!(info.key, "container-1");
    assert_eq!(info.state, SnapshotState::Committed);

    assert_eq!(snapshotter.usage_for("container-1").unwrap().used_bytes, 10);
    snapshotter.remove("container-1").unwrap();
    assert!(snapshotter.mount("container-1").is_err());
}
