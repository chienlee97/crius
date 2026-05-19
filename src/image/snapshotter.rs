use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use super::content_store::FsContentStore;
use super::metadata_store::FilesystemImageMetadataStore;
use super::ImageMeta;
use crate::storage::{SnapshotRecord, StorageManager};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    InternalOverlayUntar,
    InternalCachedRootfs,
}

pub trait Snapshotter: Send + Sync {
    fn prepare(&self, key: &str, image_ref: &str, destination: &Path) -> Result<PreparedSnapshot>;
    fn mount(&self, key: &str) -> Result<MountView>;
    fn commit(&self, key: &str) -> Result<SnapshotInfo>;
    fn remove(&self, key: &str) -> Result<()>;
    fn usage_for(&self, key: &str) -> Result<SnapshotUsage>;
    fn usage(&self) -> Result<SnapshotUsage>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotState {
    Prepared,
    Mounted,
    Committed,
    Deleted,
    Broken,
}

impl SnapshotState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Mounted => "mounted",
            Self::Committed => "committed",
            Self::Deleted => "deleted",
            Self::Broken => "broken",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value {
            "prepared" => Self::Prepared,
            "mounted" => Self::Mounted,
            "committed" => Self::Committed,
            "deleted" => Self::Deleted,
            "broken" => Self::Broken,
            _ => Self::Broken,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedSnapshot {
    pub key: String,
    pub image_id: String,
    pub rootfs_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountView {
    pub key: String,
    pub mountpoint: PathBuf,
    pub readonly: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub key: String,
    pub image_id: String,
    pub state: SnapshotState,
    pub mountpoint: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotUsage {
    pub used_bytes: u64,
    pub inodes_used: u64,
}

#[derive(Debug, Clone)]
pub struct FilesystemSnapshotter {
    mode: SnapshotMode,
    storage_root: PathBuf,
    metadata_store: FilesystemImageMetadataStore,
    content_store: FsContentStore,
    ledger_db_path: Option<PathBuf>,
}

impl FilesystemSnapshotter {
    pub fn new(
        mode: SnapshotMode,
        storage_root: impl AsRef<Path>,
        metadata_store: FilesystemImageMetadataStore,
        content_store: FsContentStore,
        ledger_db_path: Option<PathBuf>,
    ) -> Self {
        Self {
            mode,
            storage_root: storage_root.as_ref().to_path_buf(),
            metadata_store,
            content_store,
            ledger_db_path,
        }
    }

    fn snapshot_root(&self) -> PathBuf {
        self.storage_root.join("snapshots")
    }

    fn cached_rootfs_dir(&self, image_id: &str) -> PathBuf {
        self.snapshot_root().join(image_id).join("rootfs")
    }

    fn resolve_image(&self, image_ref: &str) -> Result<(ImageMeta, PathBuf)> {
        self.metadata_store
            .find_by_reference(image_ref, |image, requested_ref| {
                if image.id == requested_ref {
                    return true;
                }
                image.repo_tags.iter().any(|tag| tag == requested_ref)
                    || image
                        .repo_digests
                        .iter()
                        .any(|digest| digest == requested_ref)
                    || image.id.starts_with(requested_ref)
            })?
            .map(|record| (record.meta, record.record_dir))
            .ok_or_else(|| anyhow::anyhow!("image {image_ref} is not present locally"))
    }

    fn materialize_layers(
        &self,
        metadata: &ImageMeta,
        record_dir: &Path,
        destination: &Path,
    ) -> Result<()> {
        if destination.exists() {
            std::fs::remove_dir_all(destination)
                .with_context(|| format!("failed to clean {}", destination.display()))?;
        }
        std::fs::create_dir_all(destination)
            .with_context(|| format!("failed to create {}", destination.display()))?;

        let mut layer_paths = Vec::new();
        for layer in &metadata.stored_layers {
            let path = if !layer.digest.trim().is_empty() {
                self.content_store
                    .root()
                    .join(FsContentStore::relative_blob_path_for_digest(&layer.digest))
            } else {
                record_dir.join(&layer.path)
            };
            layer_paths.push((layer.path.clone(), path));
        }
        if layer_paths.is_empty() {
            for entry in std::fs::read_dir(record_dir)? {
                let entry = entry?;
                let path = entry.path();
                if matches!(
                    path.extension().and_then(|ext| ext.to_str()),
                    Some("gz" | "tar")
                ) {
                    let name = path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .unwrap_or_default()
                        .to_string();
                    layer_paths.push((name, path));
                }
            }
        }
        layer_paths.sort_by_key(|(name, _)| {
            name.split('.')
                .next()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(u32::MAX)
        });
        if layer_paths.is_empty() {
            return Err(anyhow::anyhow!(
                "image {} has no stored layers or archives",
                metadata.id
            ));
        }

        for (_, layer_path) in layer_paths {
            unpack_layer_with_tar(&layer_path, destination)?;
        }
        Ok(())
    }

    fn copy_rootfs_tree(source: &Path, destination: &Path) -> Result<()> {
        if destination.exists() {
            std::fs::remove_dir_all(destination)
                .with_context(|| format!("failed to clean {}", destination.display()))?;
        }
        std::fs::create_dir_all(destination)
            .with_context(|| format!("failed to create {}", destination.display()))?;
        let output = Command::new("cp")
            .arg("-a")
            .arg(format!("{}/.", source.display()))
            .arg(destination)
            .output()
            .with_context(|| format!("failed to execute cp for {}", source.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow::anyhow!(
                "failed to copy cached rootfs from {} to {}: {}",
                source.display(),
                destination.display(),
                stderr
            ));
        }
        Ok(())
    }

    fn storage(&self) -> Result<StorageManager> {
        let db_path = self
            .ledger_db_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("snapshot ledger is not configured"))?;
        StorageManager::new(db_path)
    }

    fn snapshot_record(&self, key: &str) -> Result<SnapshotRecord> {
        self.storage()?
            .list_snapshots()?
            .into_iter()
            .find(|record| record.key == key)
            .ok_or_else(|| anyhow::anyhow!("snapshot {key} not found"))
    }

    fn save_snapshot_state(&self, mut record: SnapshotRecord, state: SnapshotState) -> Result<()> {
        record.state = state.as_str().to_string();
        self.storage()?.save_snapshot(&record)
    }

    fn info_from_record(record: SnapshotRecord) -> SnapshotInfo {
        SnapshotInfo {
            key: record.key,
            image_id: record.image_id,
            state: SnapshotState::from_str(&record.state),
            mountpoint: PathBuf::from(record.mountpoint),
        }
    }
}

impl Snapshotter for FilesystemSnapshotter {
    fn prepare(&self, key: &str, image_ref: &str, destination: &Path) -> Result<PreparedSnapshot> {
        let (metadata, record_dir) = self.resolve_image(image_ref)?;
        match self.mode {
            SnapshotMode::InternalOverlayUntar => {
                self.materialize_layers(&metadata, &record_dir, destination)?;
            }
            SnapshotMode::InternalCachedRootfs => {
                let cached_rootfs = self.cached_rootfs_dir(&metadata.id);
                if !cached_rootfs.exists() {
                    self.materialize_layers(&metadata, &record_dir, &cached_rootfs)?;
                }
                Self::copy_rootfs_tree(&cached_rootfs, destination)?;
            }
        }
        if let Some(db_path) = self.ledger_db_path.as_ref() {
            let mut storage = StorageManager::new(db_path)?;
            storage.save_snapshot(&SnapshotRecord {
                key: key.to_string(),
                image_id: metadata.id.clone(),
                owner_kind: "container".to_string(),
                owner_id: key.to_string(),
                state: SnapshotState::Prepared.as_str().to_string(),
                mountpoint: destination.display().to_string(),
            })?;
        }
        Ok(PreparedSnapshot {
            key: key.to_string(),
            image_id: metadata.id,
            rootfs_path: destination.to_path_buf(),
        })
    }

    fn mount(&self, key: &str) -> Result<MountView> {
        let record = self.snapshot_record(key)?;
        let mountpoint = PathBuf::from(&record.mountpoint);
        if !mountpoint.exists() {
            self.save_snapshot_state(record, SnapshotState::Broken)?;
            return Err(anyhow::anyhow!(
                "snapshot {key} mountpoint does not exist: {}",
                mountpoint.display()
            ));
        }
        self.save_snapshot_state(record, SnapshotState::Mounted)?;
        Ok(MountView {
            key: key.to_string(),
            mountpoint,
            readonly: false,
        })
    }

    fn commit(&self, key: &str) -> Result<SnapshotInfo> {
        let record = self.snapshot_record(key)?;
        self.save_snapshot_state(record.clone(), SnapshotState::Committed)?;
        Ok(Self::info_from_record(SnapshotRecord {
            state: SnapshotState::Committed.as_str().to_string(),
            ..record
        }))
    }

    fn remove(&self, key: &str) -> Result<()> {
        let record = self.snapshot_record(key)?;
        let mountpoint = PathBuf::from(&record.mountpoint);
        if mountpoint.exists() {
            std::fs::remove_dir_all(&mountpoint)
                .with_context(|| format!("failed to remove snapshot {}", mountpoint.display()))?;
        }
        self.storage()?.delete_snapshot(key)
    }

    fn usage_for(&self, key: &str) -> Result<SnapshotUsage> {
        let record = self.snapshot_record(key)?;
        let (used_bytes, inodes_used) =
            crate::image::content_store::collect_path_usage(Path::new(&record.mountpoint))?;
        Ok(SnapshotUsage {
            used_bytes,
            inodes_used,
        })
    }

    fn usage(&self) -> Result<SnapshotUsage> {
        let (used_bytes, inodes_used) =
            crate::image::content_store::collect_path_usage(&self.snapshot_root())?;
        Ok(SnapshotUsage {
            used_bytes,
            inodes_used,
        })
    }
}

fn unpack_layer_with_tar(layer_file: &Path, rootfs_dir: &Path) -> Result<()> {
    let mut command = Command::new("tar");
    if layer_file.extension().and_then(|ext| ext.to_str()) == Some("gz") {
        command.arg("-xzf");
    } else {
        command.arg("-xf");
    }
    let output = command
        .arg(layer_file)
        .arg("-C")
        .arg(rootfs_dir)
        .arg("--no-same-owner")
        .arg("--no-same-permissions")
        .output()
        .with_context(|| format!("failed to execute tar for {}", layer_file.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "failed to unpack layer archive {}: {}",
            layer_file.display(),
            stderr.trim()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{FilesystemSnapshotter, SnapshotMode, SnapshotState, Snapshotter};
    use crate::image::content_store::{ContentStore, FsContentStore};
    use crate::image::metadata_store::FilesystemImageMetadataStore;
    use crate::image::{CriusImage, StoredLayerMeta};
    use std::process::Command;

    #[test]
    fn prepares_rootfs_from_content_store_blob() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("crius.db");
        let content_store = FsContentStore::new(dir.path()).unwrap();
        let metadata_store =
            FilesystemImageMetadataStore::new(dir.path(), Vec::new(), Some(db_path.clone()));

        let source = dir.path().join("layer-src");
        std::fs::create_dir_all(source.join("etc")).unwrap();
        std::fs::write(source.join("etc/hello"), "world").unwrap();
        let tar_path = dir.path().join("layer.tar");
        let status = Command::new("tar")
            .arg("-cf")
            .arg(&tar_path)
            .arg("-C")
            .arg(&source)
            .arg(".")
            .status()
            .unwrap();
        assert!(status.success());
        let tar_bytes = std::fs::read(&tar_path).unwrap();
        let blob = content_store
            .put_blob("", "application/vnd.oci.image.layer.v1.tar", &tar_bytes)
            .unwrap();
        metadata_store
            .save(&CriusImage {
                id: "sha256:test".to_string(),
                repo_tags: vec!["busybox:latest".to_string()],
                stored_layers: vec![StoredLayerMeta {
                    digest: blob.digest.clone(),
                    path: blob.relative_path.display().to_string(),
                    media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                    source_media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                    encrypted: false,
                }],
                ..Default::default()
            })
            .unwrap();
        let snapshotter = FilesystemSnapshotter::new(
            SnapshotMode::InternalOverlayUntar,
            dir.path(),
            metadata_store,
            content_store,
            Some(db_path.clone()),
        );
        let rootfs = dir.path().join("rootfs");
        snapshotter
            .prepare("container-1", "busybox:latest", &rootfs)
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/hello")).unwrap(),
            "world"
        );

        let mount = snapshotter.mount("container-1").unwrap();
        assert_eq!(mount.key, "container-1");
        assert_eq!(mount.mountpoint, rootfs);
        assert!(!mount.readonly);

        let usage = snapshotter.usage_for("container-1").unwrap();
        assert!(usage.used_bytes > 0);
        assert!(usage.inodes_used > 0);

        let info = snapshotter.commit("container-1").unwrap();
        assert_eq!(info.key, "container-1");
        assert_eq!(info.image_id, "sha256:test");
        assert_eq!(info.state, SnapshotState::Committed);

        let storage = crate::storage::StorageManager::new(db_path).unwrap();
        let snapshots = storage.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].owner_id, "container-1");
        assert_eq!(snapshots[0].state, SnapshotState::Committed.as_str());

        snapshotter.remove("container-1").unwrap();
        assert!(!mount.mountpoint.exists());
        let storage = crate::storage::StorageManager::new(dir.path().join("crius.db")).unwrap();
        assert!(storage.list_snapshots().unwrap().is_empty());
    }
}
