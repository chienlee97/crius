//! 存储卷管理模块
//!
//! 提供容器存储卷的支持：
//! - bind mount卷
//! - tmpfs卷
//! - hostPath卷
//! - emptyDir卷
//! - 卷生命周期管理

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use nix::mount::{mount, umount, MsFlags};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// 卷类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VolumeType {
    /// Bind mount - 挂载主机目录到容器
    Bind,
    /// tmpfs - 内存文件系统
    Tmpfs,
    /// HostPath - 主机路径
    HostPath,
    /// EmptyDir - 空目录卷
    EmptyDir,
}

/// 卷配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    /// 卷名称
    pub name: String,
    /// 卷类型
    pub volume_type: VolumeType,
    /// 源路径（主机路径）
    pub source: Option<String>,
    /// 目标路径（容器内）
    pub target: String,
    /// 是否只读
    pub read_only: bool,
    /// 挂载选项
    pub mount_options: Vec<String>,
    /// tmpfs大小限制（仅tmpfs类型）
    pub size_limit: Option<u64>,
    /// 主机路径类型（仅HostPath类型）
    pub host_path_type: Option<HostPathType>,
}

/// HostPath类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HostPathType {
    /// 默认类型
    Default,
    /// 目录必须存在
    Directory,
    /// 目录或创建
    DirectoryOrCreate,
    /// 文件必须存在
    File,
    /// 文件或创建
    FileOrCreate,
    /// 套接字
    Socket,
    /// 字符设备
    CharDevice,
    /// 块设备
    BlockDevice,
}

impl Default for HostPathType {
    fn default() -> Self {
        HostPathType::DirectoryOrCreate
    }
}

impl VolumeConfig {
    /// 创建bind mount卷配置
    pub fn bind_mount(
        name: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            volume_type: VolumeType::Bind,
            source: Some(source.into()),
            target: target.into(),
            read_only: false,
            mount_options: vec!["rbind".to_string(), "rprivate".to_string()],
            size_limit: None,
            host_path_type: None,
        }
    }

    /// 创建只读bind mount卷配置
    pub fn bind_mount_ro(
        name: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            volume_type: VolumeType::Bind,
            source: Some(source.into()),
            target: target.into(),
            read_only: true,
            mount_options: vec![
                "rbind".to_string(),
                "ro".to_string(),
                "rprivate".to_string(),
            ],
            size_limit: None,
            host_path_type: None,
        }
    }

    /// 创建tmpfs卷配置
    pub fn tmpfs(
        name: impl Into<String>,
        target: impl Into<String>,
        size_limit: Option<u64>,
    ) -> Self {
        Self {
            name: name.into(),
            volume_type: VolumeType::Tmpfs,
            source: None,
            target: target.into(),
            read_only: false,
            mount_options: vec![
                "noexec".to_string(),
                "nosuid".to_string(),
                "nodev".to_string(),
            ],
            size_limit,
            host_path_type: None,
        }
    }

    /// 创建emptyDir卷配置
    pub fn empty_dir(name: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            volume_type: VolumeType::EmptyDir,
            source: None,
            target: target.into(),
            read_only: false,
            mount_options: vec![],
            size_limit: None,
            host_path_type: None,
        }
    }

    /// 创建hostPath卷配置
    pub fn host_path(
        name: impl Into<String>,
        path: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            volume_type: VolumeType::HostPath,
            source: Some(path.into()),
            target: target.into(),
            read_only: false,
            mount_options: vec![],
            size_limit: None,
            host_path_type: Some(HostPathType::default()),
        }
    }

    /// 设置只读
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        if read_only && !self.mount_options.contains(&"ro".to_string()) {
            self.mount_options.push("ro".to_string());
        }
        self
    }
}

/// 挂载的卷信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountedVolume {
    /// 卷名称
    pub name: String,
    /// 卷配置
    pub config: VolumeConfig,
    /// 实际挂载点
    pub mount_point: PathBuf,
    /// 挂载时间
    pub mounted_at: std::time::SystemTime,
    /// 挂载是否成功
    pub is_mounted: bool,
}

/// 卷管理器
pub struct VolumeManager {
    /// 卷存储根目录
    root_dir: PathBuf,
    /// 已挂载的卷
    mounted_volumes: HashMap<String, MountedVolume>,
    /// emptyDir卷存储目录
    empty_dir_root: PathBuf,
}

impl VolumeManager {
    /// 创建新的卷管理器
    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        let empty_dir_root = root_dir.join("emptydirs");

        // 创建目录
        fs::create_dir_all(&root_dir)?;
        fs::create_dir_all(&empty_dir_root)?;

        info!("VolumeManager initialized at {:?}", root_dir);

        Ok(Self {
            root_dir,
            mounted_volumes: HashMap::new(),
            empty_dir_root,
        })
    }

    /// 准备卷（创建必要的目录结构）
    pub fn prepare_volume(&self, config: &VolumeConfig, pod_id: &str) -> Result<PathBuf> {
        match config.volume_type {
            VolumeType::Bind | VolumeType::HostPath => {
                // 对于bind mount，确保源路径存在
                if let Some(ref source) = config.source {
                    let source_path = Path::new(source);
                    if !source_path.exists() {
                        if config.volume_type == VolumeType::Bind {
                            return Err(anyhow::anyhow!(
                                "Bind mount source does not exist: {}",
                                source
                            ));
                        }
                        // HostPath with DirectoryOrCreate
                        if let Some(HostPathType::DirectoryOrCreate) = config.host_path_type {
                            fs::create_dir_all(source_path)?;
                            debug!("Created hostPath directory: {}", source);
                        }
                    }
                }
                Ok(PathBuf::from(config.source.as_ref().unwrap()))
            }
            VolumeType::EmptyDir => {
                // 创建emptyDir目录
                let empty_dir_path = self.empty_dir_root.join(pod_id).join(&config.name);
                fs::create_dir_all(&empty_dir_path)?;
                debug!("Created emptyDir at {:?}", empty_dir_path);
                Ok(empty_dir_path)
            }
            VolumeType::Tmpfs => {
                // tmpfs不需要预创建目录
                Ok(PathBuf::new())
            }
        }
    }

    /// 挂载卷到容器
    pub fn mount_volume(
        &mut self,
        config: &VolumeConfig,
        pod_id: &str,
        container_rootfs: &Path,
    ) -> Result<MountedVolume> {
        // 准备卷
        let source = self.prepare_volume(config, pod_id)?;

        // 计算目标路径
        let target_path = if config.target.starts_with('/') {
            container_rootfs.join(&config.target[1..])
        } else {
            container_rootfs.join(&config.target)
        };

        // 确保目标目录存在
        if config.volume_type != VolumeType::Tmpfs {
            fs::create_dir_all(&target_path)?;
        }

        // 执行挂载
        match config.volume_type {
            VolumeType::Bind | VolumeType::HostPath => {
                self.bind_mount(&source, &target_path, config)?;
            }
            VolumeType::Tmpfs => {
                self.mount_tmpfs(&target_path, config)?;
            }
            VolumeType::EmptyDir => {
                self.bind_mount(&source, &target_path, config)?;
            }
        }

        let mounted_volume = MountedVolume {
            name: config.name.clone(),
            config: config.clone(),
            mount_point: target_path.clone(),
            mounted_at: std::time::SystemTime::now(),
            is_mounted: true,
        };

        self.mounted_volumes
            .insert(config.name.clone(), mounted_volume.clone());

        info!(
            "Mounted volume {} (type: {:?}) from {:?} to {:?}",
            config.name, config.volume_type, source, target_path
        );

        Ok(mounted_volume)
    }

    /// 执行bind mount
    fn bind_mount(&self, source: &Path, target: &Path, config: &VolumeConfig) -> Result<()> {
        let mut flags = MsFlags::MS_BIND | MsFlags::MS_REC;

        // 解析挂载选项
        for opt in &config.mount_options {
            match opt.as_str() {
                "ro" | "readonly" => flags |= MsFlags::MS_RDONLY,
                "noexec" => flags |= MsFlags::MS_NOEXEC,
                "nosuid" => flags |= MsFlags::MS_NOSUID,
                "nodev" => flags |= MsFlags::MS_NODEV,
                "private" => flags |= MsFlags::MS_PRIVATE,
                "rprivate" => flags |= MsFlags::MS_REC | MsFlags::MS_PRIVATE,
                "slave" => flags |= MsFlags::MS_SLAVE,
                "rslave" => flags |= MsFlags::MS_REC | MsFlags::MS_SLAVE,
                "shared" => flags |= MsFlags::MS_SHARED,
                "rshared" => flags |= MsFlags::MS_REC | MsFlags::MS_SHARED,
                _ => {}
            }
        }

        // 首先进行基础bind mount
        mount(Some(source), target, None::<&str>, flags, None::<&str>)
            .context(format!("Failed to bind mount {:?} to {:?}", source, target))?;

        // 如果需要只读，重新挂载为只读
        if config.read_only && !flags.contains(MsFlags::MS_RDONLY) {
            let remount_flags = MsFlags::MS_REMOUNT | MsFlags::MS_RDONLY | MsFlags::MS_BIND;
            mount(
                None::<&str>,
                target,
                None::<&str>,
                remount_flags,
                None::<&str>,
            )
            .context(format!("Failed to remount {:?} as readonly", target))?;
        }

        debug!(
            "Bind mounted {:?} to {:?} with flags {:?}",
            source, target, flags
        );
        Ok(())
    }

    /// 挂载tmpfs
    fn mount_tmpfs(&self, target: &Path, config: &VolumeConfig) -> Result<()> {
        let flags = MsFlags::MS_NOEXEC | MsFlags::MS_NOSUID | MsFlags::MS_NODEV;

        // 构建挂载选项字符串
        let mut options = vec![];

        if let Some(size) = config.size_limit {
            options.push(format!("size={}", size));
        }

        let opts_str = if options.is_empty() {
            None
        } else {
            Some(options.join(","))
        };

        mount(
            Some("tmpfs"),
            target,
            Some("tmpfs"),
            flags,
            opts_str.as_deref(),
        )
        .context(format!("Failed to mount tmpfs at {:?}", target))?;

        debug!("Mounted tmpfs at {:?} with options {:?}", target, opts_str);
        Ok(())
    }

    /// 卸载卷
    pub fn unmount_volume(&mut self, volume_name: &str) -> Result<()> {
        if let Some(volume) = self.mounted_volumes.get(volume_name) {
            if volume.is_mounted {
                umount(&volume.mount_point)
                    .context(format!("Failed to unmount {:?}", volume.mount_point))?;

                info!(
                    "Unmounted volume {} from {:?}",
                    volume_name, volume.mount_point
                );
            }

            self.mounted_volumes.remove(volume_name);
        }

        Ok(())
    }

    /// 卸载Pod的所有卷
    pub fn unmount_pod_volumes(&mut self, pod_id: &str) -> Result<()> {
        // 找到所有属于该Pod的卷
        let pod_volumes: Vec<String> = self
            .mounted_volumes
            .iter()
            .filter(|(_, v)| v.mount_point.to_string_lossy().contains(pod_id))
            .map(|(name, _)| name.clone())
            .collect();

        for volume_name in pod_volumes {
            if let Err(e) = self.unmount_volume(&volume_name) {
                error!("Failed to unmount volume {}: {}", volume_name, e);
            }
        }

        // 清理emptyDir目录
        let pod_empty_dir = self.empty_dir_root.join(pod_id);
        if pod_empty_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&pod_empty_dir) {
                warn!("Failed to remove emptyDir {:?}: {}", pod_empty_dir, e);
            } else {
                debug!("Removed emptyDir directory {:?}", pod_empty_dir);
            }
        }

        Ok(())
    }

    /// 获取已挂载的卷
    pub fn get_mounted_volume(&self, name: &str) -> Option<&MountedVolume> {
        self.mounted_volumes.get(name)
    }

    /// 列出所有已挂载的卷
    pub fn list_mounted_volumes(&self) -> Vec<&MountedVolume> {
        self.mounted_volumes.values().collect()
    }

    /// 检查卷是否已挂载
    pub fn is_mounted(&self, name: &str) -> bool {
        self.mounted_volumes
            .get(name)
            .map(|v| v.is_mounted)
            .unwrap_or(false)
    }

    /// 获取卷存储根目录
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_volume_config_creation() {
        let bind_vol = VolumeConfig::bind_mount("data", "/host/data", "/container/data");
        assert_eq!(bind_vol.volume_type, VolumeType::Bind);
        assert_eq!(bind_vol.source, Some("/host/data".to_string()));
        assert_eq!(bind_vol.target, "/container/data".to_string());
        assert!(!bind_vol.read_only);

        let ro_bind = VolumeConfig::bind_mount_ro("config", "/host/config", "/container/config");
        assert!(ro_bind.read_only);

        let tmpfs_vol = VolumeConfig::tmpfs("cache", "/tmp/cache", Some(100 * 1024 * 1024));
        assert_eq!(tmpfs_vol.volume_type, VolumeType::Tmpfs);
        assert_eq!(tmpfs_vol.size_limit, Some(100 * 1024 * 1024));

        let empty_dir = VolumeConfig::empty_dir("work", "/work");
        assert_eq!(empty_dir.volume_type, VolumeType::EmptyDir);
    }

    #[test]
    fn test_volume_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let manager = VolumeManager::new(temp_dir.path()).unwrap();
        assert!(manager.root_dir().exists());
    }

    #[test]
    fn test_empty_dir_preparation() {
        let temp_dir = tempdir().unwrap();
        let manager = VolumeManager::new(temp_dir.path()).unwrap();

        let config = VolumeConfig::empty_dir("test-empty", "/work");
        let path = manager.prepare_volume(&config, "pod-123").unwrap();

        assert!(path.exists());
        assert!(path.to_string_lossy().contains("pod-123"));
        assert!(path.to_string_lossy().contains("test-empty"));
    }

    #[test]
    fn test_bind_mount_preparation() {
        let temp_dir = tempdir().unwrap();
        let manager = VolumeManager::new(temp_dir.path()).unwrap();

        // 创建源目录
        let source = temp_dir.path().join("source");
        fs::create_dir(&source).unwrap();

        let config = VolumeConfig::bind_mount("data", source.to_str().unwrap(), "/data");
        let path = manager.prepare_volume(&config, "pod-123").unwrap();

        assert_eq!(path, source);
    }
}
