use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::oci::spec::{LinuxBlockIo, LinuxIntelRdt};

const BLOCKIO_CONFIG_ENV: &str = "CRIUS_NRI_BLOCKIO_CONFIG";
const POD_QOS_RDT_CLASS: &str = "/PodQos";
const RESCTRL_PATH: &str = "/sys/fs/resctrl";

#[derive(Debug, Error)]
pub enum ResourceClassError {
    #[error("blockio class '{0}' requested but no blockio config path is configured")]
    MissingBlockIoConfig(String),
    #[error("failed to read blockio config {path}: {source}")]
    ReadBlockIoConfig {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse blockio config {path}: {source}")]
    ParseBlockIoConfig {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("blockio class '{class_name}' not found in {path}")]
    BlockIoClassNotFound { class_name: String, path: PathBuf },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClassSupport {
    pub blockio_supported: bool,
    pub blockio_config_path: Option<PathBuf>,
    pub rdt_supported: bool,
    pub rdt_resctrl_path: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct BlockIoClassConfig {
    classes: HashMap<String, LinuxBlockIo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum BlockIoConfigFile {
    Wrapped(BlockIoClassConfig),
    Flat(HashMap<String, LinuxBlockIo>),
}

pub fn effective_blockio_config_path(config_path: Option<&str>) -> Option<PathBuf> {
    config_path
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var(BLOCKIO_CONFIG_ENV)
                .ok()
                .filter(|path| !path.trim().is_empty())
                .map(PathBuf::from)
        })
}

pub fn feature_support(config_path: Option<&str>) -> ResourceClassSupport {
    let blockio_config_path = effective_blockio_config_path(config_path);
    ResourceClassSupport {
        blockio_supported: blockio_config_path.is_some(),
        blockio_config_path,
        rdt_supported: Path::new(RESCTRL_PATH).exists(),
        rdt_resctrl_path: PathBuf::from(RESCTRL_PATH),
    }
}

pub fn resolve_blockio_class(
    class_name: &str,
    config_path: Option<&str>,
) -> Result<Option<LinuxBlockIo>, ResourceClassError> {
    if class_name.trim().is_empty() {
        return Ok(None);
    }

    let path = effective_blockio_config_path(config_path)
        .ok_or_else(|| ResourceClassError::MissingBlockIoConfig(class_name.to_string()))?;
    let content =
        std::fs::read_to_string(&path).map_err(|source| ResourceClassError::ReadBlockIoConfig {
            path: path.clone(),
            source,
        })?;
    let classes = match serde_json::from_str::<BlockIoConfigFile>(&content).map_err(|source| {
        ResourceClassError::ParseBlockIoConfig {
            path: path.clone(),
            source,
        }
    })? {
        BlockIoConfigFile::Wrapped(config) => config.classes,
        BlockIoConfigFile::Flat(classes) => classes,
    };
    classes
        .get(class_name)
        .cloned()
        .ok_or_else(|| ResourceClassError::BlockIoClassNotFound {
            class_name: class_name.to_string(),
            path,
        })
        .map(Some)
}

pub fn resolve_rdt_class(class_name: &str) -> Option<LinuxIntelRdt> {
    let class_name = class_name.trim();
    if class_name.is_empty() || class_name == POD_QOS_RDT_CLASS {
        return None;
    }

    Some(LinuxIntelRdt {
        clos_id: Some(class_name.to_string()),
        l3_cache_schema: None,
        mem_bw_schema: None,
        enable_cmt: None,
        enable_mbm: None,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn resolves_wrapped_blockio_config() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blockio.json");
        fs::write(
            &path,
            r#"{"classes":{"gold":{"weight":500,"leafWeight":300}}}"#,
        )
        .unwrap();

        let blockio = resolve_blockio_class("gold", Some(path.to_str().unwrap()))
            .unwrap()
            .unwrap();

        assert_eq!(blockio.weight, Some(500));
        assert_eq!(blockio.leaf_weight, Some(300));
    }

    #[test]
    fn rdt_pod_qos_class_is_not_runtime_class() {
        assert!(resolve_rdt_class("").is_none());
        assert!(resolve_rdt_class("/PodQos").is_none());
        assert_eq!(
            resolve_rdt_class("gold").and_then(|rdt| rdt.clos_id),
            Some("gold".to_string())
        );
    }
}
