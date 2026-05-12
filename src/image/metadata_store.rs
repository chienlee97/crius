use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{CriusImage, Image, ImageMeta};

#[derive(Debug, Clone)]
pub struct StoredImageRecord {
    pub meta: ImageMeta,
    pub record_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FilesystemImageMetadataStore {
    storage_root: PathBuf,
    additional_artifact_stores: Vec<PathBuf>,
}

impl FilesystemImageMetadataStore {
    pub fn new(storage_root: impl AsRef<Path>, additional_artifact_stores: Vec<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
            additional_artifact_stores,
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
        let record_dir = self.image_record_dir(image);
        std::fs::create_dir_all(&record_dir)
            .with_context(|| format!("failed to create {}", record_dir.display()))?;
        let meta_path = record_dir.join("metadata.json");
        std::fs::write(&meta_path, serde_json::to_vec(image)?)
            .with_context(|| format!("failed to write {}", meta_path.display()))?;
        Ok(record_dir)
    }

    pub fn delete_by_id(&self, image_id: &str, artifact: bool) -> Result<()> {
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
        self.load_all()
            .ok()?
            .into_iter()
            .find(|record| record.meta.id == image_id)
    }

    pub fn load_all(&self) -> Result<Vec<StoredImageRecord>> {
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
    use crate::image::CriusImage;

    #[test]
    fn saves_and_loads_metadata_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemImageMetadataStore::new(dir.path(), Vec::new());
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
}
