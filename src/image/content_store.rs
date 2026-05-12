use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub trait ContentStore: Send + Sync {
    fn put_blob(&self, digest: &str, media_type: &str, bytes: &[u8]) -> Result<BlobInfo>;
    fn get_blob(&self, digest: &str) -> Result<BlobHandle>;
    fn delete_blob(&self, digest: &str) -> Result<()>;
    fn stat_blob(&self, digest: &str) -> Result<BlobInfo>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobInfo {
    pub digest: String,
    pub media_type: String,
    pub size: u64,
    pub relative_path: PathBuf,
}

#[derive(Debug)]
pub struct BlobHandle {
    pub info: BlobInfo,
    pub file: File,
}

#[derive(Debug, Clone)]
pub struct FsContentStore {
    root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlobMeta {
    digest: String,
    media_type: String,
    size: u64,
    relative_path: PathBuf,
}

impl FsContentStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join("blobs").join("sha256"))
            .with_context(|| format!("failed to create content store root {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn blobs_root(&self) -> PathBuf {
        self.root.join("blobs")
    }

    pub fn compute_digest(bytes: &[u8]) -> String {
        format!("sha256:{:x}", Sha256::digest(bytes))
    }

    pub fn relative_blob_path_for_digest(digest: &str) -> PathBuf {
        let clean = digest.trim_start_matches("sha256:");
        let (prefix, suffix) = clean.split_at(clean.len().min(2));
        PathBuf::from("blobs")
            .join("sha256")
            .join(prefix)
            .join(suffix)
    }

    pub fn blob_path_for_digest(&self, digest: &str) -> PathBuf {
        self.root.join(Self::relative_blob_path_for_digest(digest))
    }

    fn meta_path_for_digest(&self, digest: &str) -> PathBuf {
        let blob_path = self.blob_path_for_digest(digest);
        let file_name = blob_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("blob");
        blob_path.with_file_name(format!("{file_name}.meta.json"))
    }

    fn write_meta(&self, info: &BlobInfo) -> Result<()> {
        let meta = BlobMeta {
            digest: info.digest.clone(),
            media_type: info.media_type.clone(),
            size: info.size,
            relative_path: info.relative_path.clone(),
        };
        let path = self.meta_path_for_digest(&info.digest);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&path, serde_json::to_vec_pretty(&meta)?)
            .with_context(|| format!("failed to write blob metadata {}", path.display()))?;
        Ok(())
    }

    fn read_meta(&self, digest: &str) -> Result<BlobInfo> {
        let path = self.meta_path_for_digest(digest);
        let raw = std::fs::read(&path)
            .with_context(|| format!("failed to read blob metadata {}", path.display()))?;
        let meta: BlobMeta = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse blob metadata {}", path.display()))?;
        Ok(BlobInfo {
            digest: meta.digest,
            media_type: meta.media_type,
            size: meta.size,
            relative_path: meta.relative_path,
        })
    }

    pub fn total_usage(&self) -> Result<(u64, u64)> {
        collect_path_usage(&self.blobs_root())
    }
}

impl ContentStore for FsContentStore {
    fn put_blob(&self, digest: &str, media_type: &str, bytes: &[u8]) -> Result<BlobInfo> {
        let computed = Self::compute_digest(bytes);
        let digest = if digest.trim().is_empty() {
            computed
        } else {
            digest.trim().to_string()
        };
        let path = self.blob_path_for_digest(&digest);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        if !path.exists() {
            let temp = path.with_extension("tmp");
            let mut file = File::create(&temp)
                .with_context(|| format!("failed to create {}", temp.display()))?;
            file.write_all(bytes)
                .with_context(|| format!("failed to write {}", temp.display()))?;
            drop(file);
            std::fs::rename(&temp, &path).with_context(|| {
                format!("failed to rename {} -> {}", temp.display(), path.display())
            })?;
        }
        let size = std::fs::metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?
            .len();
        let info = BlobInfo {
            digest: digest.clone(),
            media_type: media_type.trim().to_string(),
            size,
            relative_path: Self::relative_blob_path_for_digest(&digest),
        };
        self.write_meta(&info)?;
        Ok(info)
    }

    fn get_blob(&self, digest: &str) -> Result<BlobHandle> {
        let info = self.stat_blob(digest)?;
        let path = self.root.join(&info.relative_path);
        let file =
            File::open(&path).with_context(|| format!("failed to open blob {}", path.display()))?;
        Ok(BlobHandle { info, file })
    }

    fn delete_blob(&self, digest: &str) -> Result<()> {
        let blob_path = self.blob_path_for_digest(digest);
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)
                .with_context(|| format!("failed to delete blob {}", blob_path.display()))?;
        }
        let meta_path = self.meta_path_for_digest(digest);
        if meta_path.exists() {
            std::fs::remove_file(&meta_path).with_context(|| {
                format!("failed to delete blob metadata {}", meta_path.display())
            })?;
        }
        Ok(())
    }

    fn stat_blob(&self, digest: &str) -> Result<BlobInfo> {
        match self.read_meta(digest) {
            Ok(info) => Ok(info),
            Err(_) => {
                let path = self.blob_path_for_digest(digest);
                let size = std::fs::metadata(&path)
                    .with_context(|| format!("failed to stat blob {}", path.display()))?
                    .len();
                Ok(BlobInfo {
                    digest: digest.to_string(),
                    media_type: String::new(),
                    size,
                    relative_path: Self::relative_blob_path_for_digest(digest),
                })
            }
        }
    }
}

pub fn collect_path_usage(path: &Path) -> Result<(u64, u64)> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let mut bytes = 0u64;
    let mut inodes = 0u64;
    for entry in std::fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        inodes = inodes.saturating_add(1);
        if metadata.is_dir() {
            let (child_bytes, child_inodes) = collect_path_usage(&entry.path())?;
            bytes = bytes.saturating_add(child_bytes);
            inodes = inodes.saturating_add(child_inodes);
        } else {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((bytes, inodes))
}

#[cfg(test)]
mod tests {
    use super::{collect_path_usage, ContentStore, FsContentStore};

    #[test]
    fn stores_blob_and_persists_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsContentStore::new(dir.path()).unwrap();
        let bytes = b"hello world";
        let info = store
            .put_blob("", "application/vnd.oci.image.layer.v1.tar+gzip", bytes)
            .unwrap();
        assert!(store.blob_path_for_digest(&info.digest).exists());
        let handle = store.get_blob(&info.digest).unwrap();
        assert_eq!(
            handle.info.media_type,
            "application/vnd.oci.image.layer.v1.tar+gzip"
        );
        assert_eq!(handle.info.size, bytes.len() as u64);
    }

    #[test]
    fn reports_content_usage() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsContentStore::new(dir.path()).unwrap();
        store
            .put_blob("", "application/vnd.oci.image.layer.v1.tar", b"abc")
            .unwrap();
        let (bytes, inodes) = store.total_usage().unwrap();
        assert!(bytes >= 3);
        assert!(inodes >= 1);
        let (all_bytes, all_inodes) = collect_path_usage(dir.path()).unwrap();
        assert!(all_bytes >= bytes);
        assert!(all_inodes >= inodes);
    }
}
