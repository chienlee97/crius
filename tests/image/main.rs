use crius::image::content_store::{ContentStore, FsContentStore};
use crius::image::snapshotter::{SnapshotUsage, Snapshotter};
use crius::test_support::{FakeContentStore, FakeSnapshotter};

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
    let snapshotter = FakeSnapshotter::new("/tmp/snapshots").with_usage(SnapshotUsage {
        used_bytes: 10,
        inodes_used: 2,
    });

    assert_eq!(snapshotter.usage().unwrap().used_bytes, 10);
    assert_eq!(snapshotter.usage().unwrap().inodes_used, 2);
}
