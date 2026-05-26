use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::oci::spec::{Linux, LinuxBlockIo, LinuxIntelRdt, LinuxResources, Spec};

const BLOCKIO_CONFIG_ENV: &str = "CRIUS_NRI_BLOCKIO_CONFIG";
const POD_QOS_RDT_CLASS: &str = "/PodQos";
const RESCTRL_PATH: &str = "/sys/fs/resctrl";
const BLOCKIO_CONTAINER_ANNOTATION: &str = "io.kubernetes.cri.blockio-class";
const BLOCKIO_POD_ANNOTATION: &str = "blockio.resources.beta.kubernetes.io/pod";
const BLOCKIO_POD_CONTAINER_PREFIX: &str = "blockio.resources.beta.kubernetes.io/container.";
const RDT_CONTAINER_ANNOTATION: &str = "io.kubernetes.cri.rdt-class";
const RDT_POD_ANNOTATION: &str = "rdt.resources.beta.kubernetes.io/pod";
const RDT_POD_CONTAINER_PREFIX: &str = "rdt.resources.beta.kubernetes.io/container.";

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceClassRequest {
    pub blockio_class: Option<String>,
    pub rdt_class: Option<String>,
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

fn class_from_annotations(
    container_name: &str,
    container_annotations: &HashMap<String, String>,
    pod_annotations: &HashMap<String, String>,
    container_key: &str,
    pod_key: &str,
    pod_container_prefix: &str,
) -> Option<String> {
    container_annotations
        .get(container_key)
        .or_else(|| pod_annotations.get(&format!("{pod_container_prefix}{container_name}")))
        .or_else(|| pod_annotations.get(pod_key))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn requested_classes_from_annotations(
    container_name: &str,
    container_annotations: &HashMap<String, String>,
    pod_annotations: &HashMap<String, String>,
) -> ResourceClassRequest {
    ResourceClassRequest {
        blockio_class: class_from_annotations(
            container_name,
            container_annotations,
            pod_annotations,
            BLOCKIO_CONTAINER_ANNOTATION,
            BLOCKIO_POD_ANNOTATION,
            BLOCKIO_POD_CONTAINER_PREFIX,
        ),
        rdt_class: class_from_annotations(
            container_name,
            container_annotations,
            pod_annotations,
            RDT_CONTAINER_ANNOTATION,
            RDT_POD_ANNOTATION,
            RDT_POD_CONTAINER_PREFIX,
        ),
    }
}

fn ensure_linux(spec: &mut Spec) -> &mut Linux {
    spec.linux.get_or_insert(Linux {
        namespaces: None,
        uid_mappings: None,
        gid_mappings: None,
        devices: None,
        net_devices: None,
        cgroups_path: None,
        resources: None,
        rootfs_propagation: None,
        seccomp: None,
        sysctl: None,
        mount_label: None,
        masked_paths: None,
        readonly_paths: None,
        intel_rdt: None,
    })
}

fn ensure_linux_resources(spec: &mut Spec) -> &mut LinuxResources {
    ensure_linux(spec).resources.get_or_insert(LinuxResources {
        network: None,
        pids: None,
        memory: None,
        cpu: None,
        block_io: None,
        hugepage_limits: None,
        devices: None,
        intel_rdt: None,
        unified: None,
    })
}

pub fn apply_resource_class_request(
    spec: &mut Spec,
    request: &ResourceClassRequest,
    blockio_config_path: Option<&str>,
) -> Result<(), ResourceClassError> {
    if let Some(blockio_class) = request.blockio_class.as_ref() {
        if let Some(block_io) = resolve_blockio_class(blockio_class, blockio_config_path)? {
            ensure_linux_resources(spec).block_io = Some(block_io);
        }
    }

    if let Some(rdt_class) = request.rdt_class.as_ref() {
        ensure_linux(spec).intel_rdt = resolve_rdt_class(rdt_class);
    }

    Ok(())
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

    #[test]
    fn container_annotations_override_pod_resource_classes() {
        let container_annotations = HashMap::from([
            (
                "io.kubernetes.cri.blockio-class".to_string(),
                "container-blockio".to_string(),
            ),
            (
                "io.kubernetes.cri.rdt-class".to_string(),
                "container-rdt".to_string(),
            ),
        ]);
        let pod_annotations = HashMap::from([
            (
                "blockio.resources.beta.kubernetes.io/container.workload".to_string(),
                "pod-container-blockio".to_string(),
            ),
            (
                "blockio.resources.beta.kubernetes.io/pod".to_string(),
                "pod-blockio".to_string(),
            ),
            (
                "rdt.resources.beta.kubernetes.io/container.workload".to_string(),
                "pod-container-rdt".to_string(),
            ),
            (
                "rdt.resources.beta.kubernetes.io/pod".to_string(),
                "pod-rdt".to_string(),
            ),
        ]);

        let request = requested_classes_from_annotations(
            "workload",
            &container_annotations,
            &pod_annotations,
        );

        assert_eq!(
            request,
            ResourceClassRequest {
                blockio_class: Some("container-blockio".to_string()),
                rdt_class: Some("container-rdt".to_string()),
            }
        );
    }

    #[test]
    fn pod_per_container_resource_class_overrides_pod_default() {
        let pod_annotations = HashMap::from([
            (
                "blockio.resources.beta.kubernetes.io/container.workload".to_string(),
                "pod-container-blockio".to_string(),
            ),
            (
                "blockio.resources.beta.kubernetes.io/pod".to_string(),
                "pod-blockio".to_string(),
            ),
            (
                "rdt.resources.beta.kubernetes.io/container.workload".to_string(),
                "pod-container-rdt".to_string(),
            ),
            (
                "rdt.resources.beta.kubernetes.io/pod".to_string(),
                "pod-rdt".to_string(),
            ),
        ]);

        let request =
            requested_classes_from_annotations("workload", &HashMap::new(), &pod_annotations);

        assert_eq!(
            request.blockio_class.as_deref(),
            Some("pod-container-blockio")
        );
        assert_eq!(request.rdt_class.as_deref(), Some("pod-container-rdt"));
    }

    #[test]
    fn applies_resource_class_request_to_oci_spec() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blockio.json");
        fs::write(&path, r#"{"classes":{"gold":{"weight":500}}}"#).unwrap();
        let mut spec = Spec::new("1.0.2");

        apply_resource_class_request(
            &mut spec,
            &ResourceClassRequest {
                blockio_class: Some("gold".to_string()),
                rdt_class: Some("silver".to_string()),
            },
            Some(path.to_str().unwrap()),
        )
        .unwrap();

        let linux = spec.linux.unwrap();
        assert_eq!(
            linux
                .resources
                .and_then(|resources| resources.block_io)
                .and_then(|block_io| block_io.weight),
            Some(500)
        );
        assert_eq!(
            linux.intel_rdt.and_then(|rdt| rdt.clos_id),
            Some("silver".to_string())
        );
    }
}
