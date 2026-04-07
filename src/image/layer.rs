//! 镜像层管理模块
//!
//! 提供OCI镜像层的存储、复用和垃圾回收功能
//! 使用内容寻址存储(CAS)实现层复用

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// 镜像层信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageLayer {
    /// 层ID (sha256 digest)
    pub id: String,
    /// 父层ID (可选)
    pub parent_id: Option<String>,
    /// 层大小 (bytes)
    pub size: u64,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 最后使用时间
    pub last_used_at: DateTime<Utc>,
    /// 引用计数
    pub ref_count: usize,
    /// 媒体类型
    pub media_type: String,
}

/// 层存储管理器
pub struct LayerStore {
    /// 层存储根目录
    root_dir: PathBuf,
    /// 层索引 (layer_id -> ImageLayer)
    layers: HashMap<String, ImageLayer>,
    /// 镜像层映射 (image_id -> Vec<layer_id>)
    image_layers: HashMap<String, Vec<String>>,
    /// 索引文件路径
    index_path: PathBuf,
}

/// 层内容存储 (Content-Addressable Storage)
pub struct ContentStore {
    /// 内容存储根目录 (blobs)
    blobs_dir: PathBuf,
}

impl ContentStore {
    /// 创建新的内容存储
    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let blobs_dir = root_dir.as_ref().join("blobs");
        fs::create_dir_all(&blobs_dir)?;

        // 创建sha256子目录
        let sha256_dir = blobs_dir.join("sha256");
        fs::create_dir_all(&sha256_dir)?;

        Ok(Self { blobs_dir })
    }

    /// 存储内容，返回内容hash
    pub fn store_content(&self, data: &[u8]) -> Result<String> {
        let hash = Self::compute_hash(data);
        let blob_path = self.get_blob_path(&hash);

        // 如果已存在，直接返回hash (去重)
        if blob_path.exists() {
            debug!("Blob {} already exists, skipping write", hash);
            return Ok(hash);
        }

        // 确保父目录存在
        if let Some(parent) = blob_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // 写入临时文件，然后原子重命名
        let temp_path = blob_path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path)?;
        file.write_all(data)?;
        drop(file);

        // 原子重命名
        fs::rename(&temp_path, &blob_path)?;

        info!("Stored blob with hash: {}", hash);
        Ok(hash)
    }

    /// 获取内容
    pub fn get_content(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let blob_path = self.get_blob_path(hash);

        if !blob_path.exists() {
            return Ok(None);
        }

        let data = fs::read(&blob_path)?;
        Ok(Some(data))
    }

    /// 检查内容是否存在
    pub fn has_content(&self, hash: &str) -> bool {
        self.get_blob_path(hash).exists()
    }

    /// 删除内容
    pub fn delete_content(&self, hash: &str) -> Result<()> {
        let blob_path = self.get_blob_path(hash);

        if blob_path.exists() {
            fs::remove_file(&blob_path)?;
            info!("Deleted blob: {}", hash);
        }

        Ok(())
    }

    /// 获取blob路径
    fn get_blob_path(&self, hash: &str) -> PathBuf {
        // 格式: blobs/sha256/ab/ab123... (前2字符作为子目录)
        let hash_clean = hash.trim_start_matches("sha256:");
        let prefix = &hash_clean[..2.min(hash_clean.len())];
        let rest = &hash_clean[2.min(hash_clean.len())..];

        self.blobs_dir.join("sha256").join(prefix).join(rest)
    }

    /// 计算内容hash (sha256)
    fn compute_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        format!("sha256:{:x}", result)
    }

    /// 获取所有blob
    pub fn list_blobs(&self) -> Result<Vec<(String, u64)>> {
        let mut blobs = Vec::new();
        let sha256_dir = self.blobs_dir.join("sha256");

        if !sha256_dir.exists() {
            return Ok(blobs);
        }

        for entry in fs::read_dir(&sha256_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let prefix = entry.file_name();
                let prefix_str = prefix.to_string_lossy().to_string();
                for blob_entry in fs::read_dir(entry.path())? {
                    let blob_entry = blob_entry?;
                    if blob_entry.file_type()?.is_file() {
                        let rest = blob_entry.file_name().to_string_lossy().to_string();
                        let hash = format!("sha256:{}{}", prefix_str, rest);
                        let size = blob_entry.metadata()?.len();
                        blobs.push((hash, size));
                    }
                }
            }
        }

        Ok(blobs)
    }

    /// 获取总存储大小
    pub fn get_total_size(&self) -> Result<u64> {
        let blobs = self.list_blobs()?;
        let total: u64 = blobs.iter().map(|(_, size)| *size).sum();
        Ok(total)
    }
}

impl LayerStore {
    /// 创建新的层存储
    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir)?;

        let index_path = root_dir.join("layers.json");
        let mut store = Self {
            root_dir,
            layers: HashMap::new(),
            image_layers: HashMap::new(),
            index_path,
        };

        // 加载索引
        store.load_index()?;

        Ok(store)
    }

    /// 加载层索引
    fn load_index(&mut self) -> Result<()> {
        if !self.index_path.exists() {
            return Ok(());
        }

        let data = fs::read(&self.index_path)?;
        let index: LayerIndex = serde_json::from_slice(&data)?;

        self.layers = index.layers;
        self.image_layers = index.image_layers;

        info!("Loaded {} layers from index", self.layers.len());
        Ok(())
    }

    /// 保存层索引
    fn save_index(&self) -> Result<()> {
        let index = LayerIndex {
            layers: self.layers.clone(),
            image_layers: self.image_layers.clone(),
        };

        let data = serde_json::to_vec_pretty(&index)?;
        let temp_path = self.index_path.with_extension("tmp");
        fs::write(&temp_path, &data)?;
        fs::rename(&temp_path, &self.index_path)?;

        Ok(())
    }

    /// 添加层
    pub fn add_layer(
        &mut self,
        layer_id: String,
        parent_id: Option<String>,
        size: u64,
        media_type: &str,
    ) -> Result<()> {
        let now = Utc::now();

        // 如果层已存在，更新引用计数
        if let Some(layer) = self.layers.get_mut(&layer_id) {
            layer.ref_count += 1;
            layer.last_used_at = now;
            debug!(
                "Layer {} already exists, ref_count: {}",
                layer_id, layer.ref_count
            );
        } else {
            // 创建新层
            let layer = ImageLayer {
                id: layer_id.clone(),
                parent_id,
                size,
                created_at: now,
                last_used_at: now,
                ref_count: 1,
                media_type: media_type.to_string(),
            };

            self.layers.insert(layer_id.clone(), layer);
            info!("Added new layer: {} (size: {} bytes)", layer_id, size);
        }

        self.save_index()?;
        Ok(())
    }

    /// 关联层到镜像
    pub fn associate_layers(&mut self, image_id: &str, layer_ids: Vec<String>) -> Result<()> {
        self.image_layers.insert(image_id.to_string(), layer_ids);
        self.save_index()?;
        Ok(())
    }

    /// 获取镜像的层列表
    pub fn get_image_layers(&self, image_id: &str) -> Option<Vec<ImageLayer>> {
        let layer_ids = self.image_layers.get(image_id)?;
        let layers: Vec<ImageLayer> = layer_ids
            .iter()
            .filter_map(|id| self.layers.get(id).cloned())
            .collect();
        Some(layers)
    }

    /// 移除镜像关联
    pub fn remove_image(&mut self, image_id: &str) -> Result<Vec<String>> {
        let layer_ids = self.image_layers.remove(image_id).unwrap_or_default();

        // 减少引用计数
        for layer_id in &layer_ids {
            if let Some(layer) = self.layers.get_mut(layer_id) {
                if layer.ref_count > 0 {
                    layer.ref_count -= 1;
                }
            }
        }

        self.save_index()?;
        info!("Removed image {} associations", image_id);
        Ok(layer_ids)
    }

    /// 获取未引用的层列表 (垃圾回收候选)
    pub fn get_unreferenced_layers(&self) -> Vec<ImageLayer> {
        self.layers
            .values()
            .filter(|layer| layer.ref_count == 0)
            .cloned()
            .collect()
    }

    /// 执行垃圾回收
    pub fn garbage_collect(&mut self, content_store: &ContentStore) -> Result<(usize, u64)> {
        let unreferenced: Vec<ImageLayer> = self.get_unreferenced_layers();

        let mut deleted_count = 0;
        let mut freed_bytes = 0u64;

        for layer in &unreferenced {
            // 删除内容
            content_store.delete_content(&layer.id)?;

            // 从索引中删除
            self.layers.remove(&layer.id);

            deleted_count += 1;
            freed_bytes += layer.size;

            info!(
                "Garbage collected layer: {} (freed {} bytes)",
                layer.id, layer.size
            );
        }

        self.save_index()?;

        info!(
            "Garbage collection complete: deleted {} layers, freed {} bytes",
            deleted_count, freed_bytes
        );

        Ok((deleted_count, freed_bytes))
    }

    /// 获取层统计信息
    pub fn get_stats(&self) -> LayerStats {
        let total_layers = self.layers.len();
        let total_size: u64 = self.layers.values().map(|l| l.size).sum();
        let unreferenced_count = self.get_unreferenced_layers().len();
        let referenced_count = total_layers - unreferenced_count;

        LayerStats {
            total_layers,
            referenced_count,
            unreferenced_count,
            total_size,
        }
    }

    /// 获取层
    pub fn get_layer(&self, layer_id: &str) -> Option<&ImageLayer> {
        self.layers.get(layer_id)
    }

    /// 列出所有层
    pub fn list_layers(&self) -> Vec<&ImageLayer> {
        self.layers.values().collect()
    }
}

/// 层索引 (序列化用)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LayerIndex {
    layers: HashMap<String, ImageLayer>,
    image_layers: HashMap<String, Vec<String>>,
}

/// 层统计信息
#[derive(Debug, Clone)]
pub struct LayerStats {
    /// 总层数
    pub total_layers: usize,
    /// 被引用的层数
    pub referenced_count: usize,
    /// 未引用的层数
    pub unreferenced_count: usize,
    /// 总大小 (bytes)
    pub total_size: u64,
}

/// 镜像层管理器 (高层接口)
pub struct LayerManager {
    /// 层存储
    layer_store: LayerStore,
    /// 内容存储
    content_store: ContentStore,
    /// 存储根目录
    root_dir: PathBuf,
}

impl LayerManager {
    /// 创建新的层管理器
    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();

        let layer_store = LayerStore::new(&root_dir)?;
        let content_store = ContentStore::new(&root_dir)?;

        Ok(Self {
            layer_store,
            content_store,
            root_dir,
        })
    }

    /// 存储层内容
    pub fn store_layer(
        &mut self,
        data: &[u8],
        parent_id: Option<String>,
        media_type: &str,
    ) -> Result<String> {
        // 存储内容
        let hash = self.content_store.store_content(data)?;

        // 添加到层索引
        let size = data.len() as u64;
        self.layer_store
            .add_layer(hash.clone(), parent_id, size, media_type)?;

        Ok(hash)
    }

    /// 获取层内容
    pub fn get_layer_content(&self, layer_id: &str) -> Result<Option<Vec<u8>>> {
        self.content_store.get_content(layer_id)
    }

    /// 检查层是否存在
    pub fn has_layer(&self, layer_id: &str) -> bool {
        self.layer_store.get_layer(layer_id).is_some() && self.content_store.has_content(layer_id)
    }

    /// 为镜像添加层
    pub fn add_image_layers(&mut self, image_id: &str, layer_ids: Vec<String>) -> Result<()> {
        self.layer_store.associate_layers(image_id, layer_ids)
    }

    /// 移除镜像 (触发引用计数减少)
    pub fn remove_image(&mut self, image_id: &str) -> Result<Vec<String>> {
        self.layer_store.remove_image(image_id)
    }

    /// 执行垃圾回收
    pub fn garbage_collect(&mut self) -> Result<(usize, u64)> {
        self.layer_store.garbage_collect(&self.content_store)
    }

    /// 获取镜像层列表
    pub fn get_image_layers(&self, image_id: &str) -> Option<Vec<ImageLayer>> {
        self.layer_store.get_image_layers(image_id)
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> LayerStats {
        self.layer_store.get_stats()
    }

    /// 获取存储总大小
    pub fn get_total_size(&self) -> Result<u64> {
        self.content_store.get_total_size()
    }

    /// 获取根目录
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_content_store() {
        let temp_dir = tempdir().unwrap();
        let store = ContentStore::new(temp_dir.path()).unwrap();

        let data = b"test content";
        let hash = store.store_content(data).unwrap();

        // 验证内容存在
        assert!(store.has_content(&hash));

        // 读取内容
        let retrieved = store.get_content(&hash).unwrap().unwrap();
        assert_eq!(retrieved, data);

        // 重复存储应该去重
        let hash2 = store.store_content(data).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_layer_store() {
        let temp_dir = tempdir().unwrap();
        let mut store = LayerStore::new(temp_dir.path()).unwrap();

        // 添加层
        store
            .add_layer(
                "sha256:abc123".to_string(),
                None,
                1024,
                "application/vnd.oci.image.layer.v1.tar+gzip",
            )
            .unwrap();

        // 关联到镜像
        store
            .associate_layers("image-1", vec!["sha256:abc123".to_string()])
            .unwrap();

        // 验证层信息
        let layer = store.get_layer("sha256:abc123").unwrap();
        assert_eq!(layer.ref_count, 1);

        // 移除镜像
        store.remove_image("image-1").unwrap();

        // 验证引用计数减少
        let layer = store.get_layer("sha256:abc123").unwrap();
        assert_eq!(layer.ref_count, 0);

        // 获取未引用层
        let unreferenced = store.get_unreferenced_layers();
        assert_eq!(unreferenced.len(), 1);
    }

    #[test]
    fn test_layer_manager() {
        let temp_dir = tempdir().unwrap();
        let mut manager = LayerManager::new(temp_dir.path()).unwrap();

        // 存储层
        let data = b"layer content";
        let layer_id = manager
            .store_layer(data, None, "application/vnd.oci.image.layer.v1.tar+gzip")
            .unwrap();

        // 验证层存在
        assert!(manager.has_layer(&layer_id));

        // 添加镜像层
        manager
            .add_image_layers("image-1", vec![layer_id.clone()])
            .unwrap();

        // 获取统计
        let stats = manager.get_stats();
        assert_eq!(stats.total_layers, 1);
        assert_eq!(stats.referenced_count, 1);

        // 移除镜像
        manager.remove_image("image-1").unwrap();

        // 执行GC
        let (deleted, freed) = manager.garbage_collect().unwrap();
        assert_eq!(deleted, 1);
        assert!(freed > 0);

        // 验证层已删除
        assert!(!manager.has_layer(&layer_id));
    }
}
