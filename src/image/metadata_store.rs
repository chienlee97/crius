use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{CriusImage, Image, ImageMeta};
use crate::storage::{ContentBlobRefRecord, ImageRecord, ImageRefRecord, StorageManager};

#[derive(Debug, Clone)]
pub struct StoredImageRecord {
    pub meta: ImageMeta,
    pub record_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FilesystemImageMetadataStore {
    storage_root: PathBuf,
    additional_artifact_stores: Vec<PathBuf>,
    ledger_db_path: Option<PathBuf>,
}

impl FilesystemImageMetadataStore {
    pub fn new(
        storage_root: impl AsRef<Path>,
        additional_artifact_stores: Vec<PathBuf>,
        ledger_db_path: Option<PathBuf>,
    ) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
            additional_artifact_stores,
            ledger_db_path,
        }
    }

    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    pub fn image_records_dir(root: &Path) -> PathBuf {
        root.join("images")
    }

    pub fn artifact_records_dir(root: &Path) -> PathBuf {
        root.join("artifacts")
    }

    pub fn local_record_dir(root: &Path, id: &str, artifact: bool) -> PathBuf {
        if artifact {
            Self::artifact_records_dir(root).join(id)
        } else {
            Self::image_records_dir(root).join(id)
        }
    }

    pub fn image_record_dir(&self, image: &CriusImage) -> PathBuf {
        Self::local_record_dir(
            &self.storage_root,
            &image.id,
            image
                .artifact_type
                .as_ref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false),
        )
    }

    pub fn save(&self, image: &CriusImage) -> Result<PathBuf> {
        if let Some(db_path) = self.ledger_db_path.as_ref() {
            let mut storage = StorageManager::new(db_path)?;
            storage.save_image(&image_to_record(image, self.image_record_dir(image)))?;
            storage.replace_image_refs(&image.id, &image_to_refs(image))?;
            storage.replace_content_blob_refs("image", &image.id, &image_to_blob_refs(image))?;
        }
        let record_dir = self.image_record_dir(image);
        std::fs::create_dir_all(&record_dir)
            .with_context(|| format!("failed to create {}", record_dir.display()))?;
        let meta_path = record_dir.join("metadata.json");
        std::fs::write(&meta_path, serde_json::to_vec(image)?)
            .with_context(|| format!("failed to write {}", meta_path.display()))?;
        Ok(record_dir)
    }

    pub fn delete_by_id(&self, image_id: &str, artifact: bool) -> Result<()> {
        if let Some(db_path) = self.ledger_db_path.as_ref() {
            let mut storage = StorageManager::new(db_path)?;
            storage.delete_image(image_id)?;
        }
        let record_dir = Self::local_record_dir(&self.storage_root, image_id, artifact);
        if record_dir.exists() {
            std::fs::remove_dir_all(&record_dir)
                .with_context(|| format!("failed to remove {}", record_dir.display()))?;
        }
        Ok(())
    }

    pub fn load_meta_from_record_dir(record_dir: &Path) -> Option<ImageMeta> {
        let raw = std::fs::read(record_dir.join("metadata.json")).ok()?;
        serde_json::from_slice(&raw).ok()
    }

    pub fn load_by_id(&self, image_id: &str) -> Option<StoredImageRecord> {
        if let Some(db_path) = self.ledger_db_path.as_ref() {
            if let Ok(storage) = StorageManager::new(db_path) {
                if let Ok(Some(record)) = storage.get_image(image_id) {
                    return Some(StoredImageRecord {
                        meta: meta_from_record(
                            &record,
                            storage.list_image_refs(Some(image_id)).ok(),
                        ),
                        record_dir: record
                            .cache_path
                            .as_deref()
                            .map(PathBuf::from)
                            .unwrap_or_else(|| {
                                Self::image_records_dir(&self.storage_root).join(image_id)
                            }),
                    });
                }
            }
        }
        self.load_all()
            .ok()?
            .into_iter()
            .find(|record| record.meta.id == image_id)
    }

    pub fn load_all(&self) -> Result<Vec<StoredImageRecord>> {
        if let Some(db_path) = self.ledger_db_path.as_ref() {
            let storage = StorageManager::new(db_path)?;
            let records = storage.list_images()?;
            if !records.is_empty() {
                let refs = storage.list_image_refs(None)?;
                return Ok(records
                    .into_iter()
                    .map(|record| {
                        let image_id = record.id.clone();
                        StoredImageRecord {
                            meta: meta_from_record(
                                &record,
                                Some(
                                    refs.iter()
                                        .filter(|candidate| candidate.image_id == image_id)
                                        .cloned()
                                        .collect(),
                                ),
                            ),
                            record_dir: record
                                .cache_path
                                .as_deref()
                                .map(PathBuf::from)
                                .unwrap_or_else(|| {
                                    Self::image_records_dir(&self.storage_root).join(&image_id)
                                }),
                        }
                    })
                    .collect());
            }
        }
        let mut records = Vec::new();
        for root in std::iter::once(self.storage_root.as_path())
            .chain(self.additional_artifact_stores.iter().map(PathBuf::as_path))
        {
            for records_dir in [
                Self::image_records_dir(root),
                Self::artifact_records_dir(root),
            ] {
                if !records_dir.exists() {
                    continue;
                }
                for entry in std::fs::read_dir(&records_dir)
                    .with_context(|| format!("failed to read {}", records_dir.display()))?
                {
                    let entry = entry?;
                    let record_dir = entry.path();
                    let Some(meta) = Self::load_meta_from_record_dir(&record_dir) else {
                        continue;
                    };
                    records.push(StoredImageRecord { meta, record_dir });
                }
            }
        }
        Ok(records)
    }

    pub fn find_by_reference<F>(
        &self,
        requested_ref: &str,
        matcher: F,
    ) -> Result<Option<StoredImageRecord>>
    where
        F: Fn(&Image, &str) -> bool,
    {
        for record in self.load_all()? {
            let image = image_from_meta(&record.meta);
            if matcher(&image, requested_ref) {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    pub fn usage(&self) -> Result<(u64, u64)> {
        let mut bytes = 0u64;
        let mut inodes = 0u64;
        if let Some(db_path) = self.ledger_db_path.as_ref() {
            if db_path.exists() {
                let metadata = std::fs::metadata(db_path)
                    .with_context(|| format!("failed to stat {}", db_path.display()))?;
                bytes = bytes.saturating_add(metadata.len());
                inodes = inodes.saturating_add(1);
            }
        }
        for root in std::iter::once(self.storage_root.as_path())
            .chain(self.additional_artifact_stores.iter().map(PathBuf::as_path))
        {
            for path in [
                Self::image_records_dir(root),
                Self::artifact_records_dir(root),
            ] {
                let (path_bytes, path_inodes) =
                    crate::image::content_store::collect_path_usage(&path)?;
                bytes = bytes.saturating_add(path_bytes);
                inodes = inodes.saturating_add(path_inodes);
            }
        }
        Ok((bytes, inodes))
    }
}

fn image_to_record(image: &CriusImage, cache_path: PathBuf) -> ImageRecord {
    ImageRecord {
        id: image.id.clone(),
        size: image.size,
        pinned: image.pinned,
        pulled_at: image.pulled_at,
        source_reference: image.source_reference.clone(),
        os: image.os.clone(),
        architecture: image.architecture.clone(),
        config_user: image.config_user.clone(),
        config_env_json: serde_json::to_string(&image.config_env)
            .unwrap_or_else(|_| "[]".to_string()),
        config_entrypoint_json: serde_json::to_string(&image.config_entrypoint)
            .unwrap_or_else(|_| "[]".to_string()),
        config_cmd_json: serde_json::to_string(&image.config_cmd)
            .unwrap_or_else(|_| "[]".to_string()),
        config_working_dir: image.config_working_dir.clone(),
        annotations_json: serde_json::to_string(&image.annotations)
            .unwrap_or_else(|_| "{}".to_string()),
        declared_volumes_json: serde_json::to_string(&image.declared_volumes)
            .unwrap_or_else(|_| "[]".to_string()),
        manifest_media_type: image.manifest_media_type.clone(),
        selected_manifest_digest: image.selected_manifest_digest.clone(),
        selected_platform: image.selected_platform.clone(),
        stored_layers_json: serde_json::to_string(&image.stored_layers)
            .unwrap_or_else(|_| "[]".to_string()),
        artifact_type: image.artifact_type.clone(),
        artifact_blobs_json: serde_json::to_string(&image.artifact_blobs)
            .unwrap_or_else(|_| "[]".to_string()),
        cache_path: Some(cache_path.display().to_string()),
    }
}

fn image_to_refs(image: &CriusImage) -> Vec<ImageRefRecord> {
    let mut refs = Vec::new();
    for reference in &image.repo_tags {
        refs.push(ImageRefRecord {
            reference: reference.clone(),
            image_id: image.id.clone(),
            namespace: None,
            ref_kind: "tag".to_string(),
        });
    }
    for reference in &image.repo_digests {
        refs.push(ImageRefRecord {
            reference: reference.clone(),
            image_id: image.id.clone(),
            namespace: None,
            ref_kind: "digest".to_string(),
        });
    }
    if let Some(reference) = image.source_reference.as_ref() {
        refs.push(ImageRefRecord {
            reference: reference.clone(),
            image_id: image.id.clone(),
            namespace: None,
            ref_kind: "source".to_string(),
        });
    }
    refs
}

fn image_to_blob_refs(image: &CriusImage) -> Vec<ContentBlobRefRecord> {
    let layer_refs = image
        .stored_layers
        .iter()
        .filter(|layer| !layer.digest.trim().is_empty())
        .map(|layer| ContentBlobRefRecord {
            owner_kind: "image".to_string(),
            owner_id: image.id.clone(),
            digest: layer.digest.clone(),
            ref_kind: "layer".to_string(),
        });
    let artifact_refs = image
        .artifact_blobs
        .iter()
        .filter(|blob| !blob.digest.trim().is_empty())
        .map(|blob| ContentBlobRefRecord {
            owner_kind: "image".to_string(),
            owner_id: image.id.clone(),
            digest: blob.digest.clone(),
            ref_kind: "artifact".to_string(),
        });
    layer_refs.chain(artifact_refs).collect()
}

fn meta_from_record(record: &ImageRecord, refs: Option<Vec<ImageRefRecord>>) -> ImageMeta {
    let refs = refs.unwrap_or_default();
    let repo_tags = refs
        .iter()
        .filter(|record| record.ref_kind == "tag")
        .map(|record| record.reference.clone())
        .collect();
    let repo_digests = refs
        .iter()
        .filter(|record| record.ref_kind == "digest")
        .map(|record| record.reference.clone())
        .collect();
    ImageMeta {
        id: record.id.clone(),
        repo_tags,
        repo_digests,
        size: record.size,
        pinned: record.pinned,
        pulled_at: record.pulled_at,
        source_reference: record.source_reference.clone(),
        os: record.os.clone(),
        architecture: record.architecture.clone(),
        config_user: record.config_user.clone(),
        config_env: serde_json::from_str(&record.config_env_json).unwrap_or_default(),
        config_entrypoint: serde_json::from_str(&record.config_entrypoint_json).unwrap_or_default(),
        config_cmd: serde_json::from_str(&record.config_cmd_json).unwrap_or_default(),
        config_working_dir: record.config_working_dir.clone(),
        annotations: serde_json::from_str(&record.annotations_json).unwrap_or_default(),
        declared_volumes: serde_json::from_str(&record.declared_volumes_json).unwrap_or_default(),
        manifest_media_type: record.manifest_media_type.clone(),
        selected_manifest_digest: record.selected_manifest_digest.clone(),
        selected_platform: record.selected_platform.clone(),
        stored_layers: serde_json::from_str(&record.stored_layers_json).unwrap_or_default(),
        artifact_type: record.artifact_type.clone(),
        artifact_blobs: serde_json::from_str(&record.artifact_blobs_json).unwrap_or_default(),
    }
}

fn image_from_meta(meta: &ImageMeta) -> Image {
    let requested = meta
        .source_reference
        .clone()
        .or_else(|| meta.repo_tags.first().cloned())
        .unwrap_or_default();
    Image {
        id: meta.id.clone(),
        repo_tags: meta.repo_tags.clone(),
        repo_digests: meta.repo_digests.clone(),
        size: meta.size,
        pinned: meta.pinned,
        uid: meta
            .config_user
            .as_ref()
            .map(|user| crate::proto::runtime::v1::Int64Value {
                value: user.parse::<i64>().unwrap_or(0),
            }),
        username: meta.config_user.clone().unwrap_or_default(),
        spec: Some(crate::proto::runtime::v1::ImageSpec {
            image: requested.clone(),
            user_specified_image: requested,
            annotations: meta.annotations.clone(),
            runtime_handler: String::new(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::FilesystemImageMetadataStore;
    use crate::image::{ArtifactBlobMeta, CriusImage, StoredLayerMeta};

    #[test]
    fn saves_and_loads_metadata_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemImageMetadataStore::new(dir.path(), Vec::new(), None);
        let image = CriusImage {
            id: "sha256:test".to_string(),
            repo_tags: vec!["busybox:latest".to_string()],
            ..Default::default()
        };
        let record_dir = store.save(&image).unwrap();
        assert!(record_dir.join("metadata.json").exists());
        let loaded = store.load_by_id("sha256:test").unwrap();
        assert_eq!(loaded.meta.repo_tags, vec!["busybox:latest"]);
    }

    #[test]
    fn saves_and_loads_metadata_records_via_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("crius.db");
        let store =
            FilesystemImageMetadataStore::new(dir.path(), Vec::new(), Some(db_path.clone()));
        let image = CriusImage {
            id: "sha256:test-ledger".to_string(),
            repo_tags: vec!["busybox:ledger".to_string()],
            repo_digests: vec!["docker.io/library/busybox@sha256:test-ledger".to_string()],
            ..Default::default()
        };
        store.save(&image).unwrap();
        let loaded = store.load_by_id("sha256:test-ledger").unwrap();
        assert_eq!(loaded.meta.repo_tags, vec!["busybox:ledger"]);
        assert_eq!(
            loaded.meta.repo_digests,
            vec!["docker.io/library/busybox@sha256:test-ledger"]
        );
    }

    #[test]
    fn saves_image_blob_refs_via_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("crius.db");
        let store =
            FilesystemImageMetadataStore::new(dir.path(), Vec::new(), Some(db_path.clone()));
        let image = CriusImage {
            id: "sha256:image-with-blobs".to_string(),
            stored_layers: vec![StoredLayerMeta {
                digest: "sha256:layer".to_string(),
                path: "blobs/sha256/la/yer".to_string(),
                media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                source_media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                encrypted: false,
            }],
            artifact_blobs: vec![ArtifactBlobMeta {
                digest: "sha256:artifact".to_string(),
                media_type: "application/vnd.oci.image.config.v1+json".to_string(),
                path: "blobs/sha256/ar/tifact".to_string(),
                size: 42,
                annotations: Default::default(),
            }],
            ..Default::default()
        };

        store.save(&image).unwrap();

        let storage = crate::storage::StorageManager::new(&db_path).unwrap();
        let refs = storage
            .list_content_blob_refs(Some("image"), Some("sha256:image-with-blobs"))
            .unwrap();
        assert_eq!(refs.len(), 2);
        assert!(refs
            .iter()
            .any(|record| record.digest == "sha256:layer" && record.ref_kind == "layer"));
        assert!(refs
            .iter()
            .any(|record| record.digest == "sha256:artifact" && record.ref_kind == "artifact"));

        store
            .delete_by_id("sha256:image-with-blobs", false)
            .unwrap();
        let storage = crate::storage::StorageManager::new(&db_path).unwrap();
        assert!(storage
            .list_content_blob_refs(Some("image"), Some("sha256:image-with-blobs"))
            .unwrap()
            .is_empty());
    }
}
