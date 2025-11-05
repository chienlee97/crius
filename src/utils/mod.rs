use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use crate::prelude::*;

/// 规范化路径
pub fn normalize_path<P: AsRef<Path>>(path: P) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|p| p.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// 确保目录存在
pub fn ensure_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

/// 生成随机ID
pub fn generate_id(prefix: &str) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random: u32 = rng.gen();
    format!("{}{:08x}", prefix, random)
}

/// 检查文件是否存在
pub fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    Path::new(path.as_ref()).exists()
}
