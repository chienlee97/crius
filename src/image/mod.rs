pub mod content_store;
pub mod layer;
pub mod metadata_store;
pub mod policy;
pub mod pull_cgroup;
pub mod snapshotter;

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use base64::Engine;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{error, info, warn};
use oci_distribution::{secrets::RegistryAuth, Reference};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};

use crate::error::Error;
use crate::proto::runtime::v1::{
    image_service_server::ImageService, AuthConfig, FilesystemIdentifier, FilesystemUsage, Image,
    ImageFsInfoRequest, ImageFsInfoResponse, ImageSpec, ImageStatusRequest, ImageStatusResponse,
    Int64Value, ListImagesRequest, ListImagesResponse, PullImageRequest, PullImageResponse,
    RemoveImageRequest, RemoveImageResponse, UInt64Value,
};
use crate::storage::{ContentGcBlocker, StorageManager};
use content_store::{
    ContentStore, ContentTransferRecord, ContentTransferStatus, ContentTransferTracker,
    FsContentStore, RemoteContentProviderKind,
};
use metadata_store::FilesystemImageMetadataStore;
pub use pull_cgroup::{
    validate_pull_cgroup_config, PullCgroupEffectiveConfig, PullCgroupExecutor, PullCgroupMode,
    PullCgroupScopeRecord,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentGcSummary {
    pub dry_run: bool,
    pub candidates: usize,
    pub eligible: usize,
    pub skipped: usize,
    pub deleted: usize,
    pub failed: usize,
    pub bytes_deleted: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentGcDiagnostics {
    pub dry_run: bool,
    pub candidates: Vec<ContentGcCandidateDiagnostics>,
    pub reclaimed_bytes: u64,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentGcCandidateDiagnostics {
    pub object_type: String,
    pub object_id: String,
    pub path: String,
    pub size_bytes: u64,
    pub reason: String,
    pub deleted: bool,
    pub error: Option<String>,
}

/// crius镜像
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CriusImage {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size: u64,
    pub pinned: bool,
    pub pulled_at: i64,
    pub source_reference: Option<String>,
    pub os: Option<String>,
    pub architecture: Option<String>,
    pub config_user: Option<String>,
    pub config_env: Vec<String>,
    pub config_entrypoint: Vec<String>,
    pub config_cmd: Vec<String>,
    pub config_working_dir: Option<String>,
    pub annotations: HashMap<String, String>,
    pub declared_volumes: Vec<String>,
    pub manifest_media_type: Option<String>,
    pub selected_manifest_digest: Option<String>,
    pub selected_platform: Option<String>,
    pub stored_layers: Vec<StoredLayerMeta>,
    pub artifact_type: Option<String>,
    pub artifact_blobs: Vec<ArtifactBlobMeta>,
}

/// 镜像服务实现
#[derive(Clone)]
pub struct ImageServiceImpl {
    // 存储镜像信息的线程安全HashMap
    images: std::sync::Arc<tokio::sync::Mutex<HashMap<String, Image>>>,
    storage_path: PathBuf,
    storage_driver: String,
    storage_options: Vec<String>,
    parsed_storage_options: OverlayImageStorageOptions,
    content_store: Arc<FsContentStore>,
    metadata_store: Arc<FilesystemImageMetadataStore>,
    default_transport: String,
    short_name_mode: String,
    pull_progress_timeout: std::time::Duration,
    max_concurrent_downloads: usize,
    pull_retry_count: u32,
    additional_artifact_stores: Vec<PathBuf>,
    _big_files_temporary_dir: Option<PathBuf>,
    in_progress_pulls: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    transfer_tracker: ContentTransferTracker,
    #[cfg(test)]
    test_pull_handler: std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<TestPullHandler>>>>,
    #[cfg(test)]
    test_pull_scope_observer:
        std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<TestPullScopeObserver>>>>,
    reloadable_config: Arc<RwLock<ReloadableImageConfig>>,
    pull_cgroup: PullCgroupExecutor,
    ledger_db_path: Option<PathBuf>,
}

#[derive(Clone)]
pub struct ImageMetricsProvider {
    images: std::sync::Arc<tokio::sync::Mutex<HashMap<String, Image>>>,
    _storage_path: PathBuf,
    content_store: Arc<FsContentStore>,
    metadata_store: Arc<FilesystemImageMetadataStore>,
}

#[derive(Debug, Clone)]
pub struct ImageServiceOptions {
    pub storage_path: PathBuf,
    pub ledger_db_path: Option<PathBuf>,
    pub storage_driver: String,
    pub storage_options: Vec<String>,
    pub global_auth_file: Option<PathBuf>,
    pub namespaced_auth_dir: Option<PathBuf>,
    pub default_transport: String,
    pub short_name_mode: String,
    pub pull_progress_timeout: std::time::Duration,
    pub max_concurrent_downloads: usize,
    pub pull_retry_count: u32,
    pub registry_config_dir: Option<PathBuf>,
    pub decryption_keys_path: Option<PathBuf>,
    pub decryption_decoder_path: String,
    pub decryption_keyprovider_config: Option<PathBuf>,
    pub additional_artifact_stores: Vec<PathBuf>,
    pub pinned_image_patterns: Vec<String>,
    pub signature_policy: Option<PathBuf>,
    pub signature_policy_dir: Option<PathBuf>,
    pub big_files_temporary_dir: Option<PathBuf>,
    pub separate_pull_cgroup: String,
    pub cgroup_driver: crate::config::CgroupDriverConfig,
    pub rootless: crate::rootless::EffectiveRootlessConfig,
    pub disable_cgroup: bool,
    #[cfg(test)]
    pub pull_cgroup_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverlayImageStorageOptions {
    pub mount_program: Option<PathBuf>,
    pub ignore_chown_errors: bool,
}

impl OverlayImageStorageOptions {
    fn parse(storage_driver: &str, options: &[String]) -> Result<Self, Error> {
        if storage_driver != "overlay" && !options.is_empty() {
            return Err(Error::Config(format!(
                "image.storage_options is not supported for image.driver {storage_driver}"
            )));
        }

        let mut parsed = Self::default();
        let mut seen = HashSet::new();
        for option in options {
            let (key, value) = parse_storage_option(option)?;
            if !seen.insert(key.clone()) {
                return Err(Error::Config(format!(
                    "image.storage_options contains duplicate key {key}"
                )));
            }
            match key.as_str() {
                "overlay.mount_program" => {
                    if !Path::new(&value).is_absolute() {
                        return Err(Error::Config(format!(
                            "image.storage_options overlay.mount_program must be an absolute path, got {value}"
                        )));
                    }
                    parsed.mount_program = Some(PathBuf::from(value));
                }
                "overlay.ignore_chown_errors" => {
                    parsed.ignore_chown_errors =
                        parse_storage_option_bool(&value).map_err(|err| {
                            Error::Config(format!(
                                "image.storage_options overlay.ignore_chown_errors: {err}"
                            ))
                        })?;
                }
                other => {
                    return Err(Error::Config(format!(
                        "unsupported image.storage_options key {other}; supported overlay keys are overlay.mount_program and overlay.ignore_chown_errors"
                    )));
                }
            }
        }

        Ok(parsed)
    }
}

fn parse_storage_option(option: &str) -> Result<(String, String), Error> {
    let trimmed = option.trim();
    let (key, value) = trimmed
        .split_once('=')
        .or_else(|| trimmed.split_once(char::is_whitespace))
        .ok_or_else(|| {
            Error::Config(format!(
                "image.storage_options entry {option:?}: expected key=value format"
            ))
        })?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() {
        return Err(Error::Config(format!(
            "image.storage_options entry {option:?}: key must not be empty"
        )));
    }
    if value.is_empty() {
        return Err(Error::Config(format!(
            "image.storage_options entry {option:?}: value must not be empty"
        )));
    }
    Ok((key.to_string(), value.to_string()))
}

fn parse_storage_option_bool(value: &str) -> std::result::Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value {other}")),
    }
}

fn content_gc_blocker_detail(blocker: &ContentGcBlocker) -> serde_json::Value {
    match blocker {
        ContentGcBlocker::ContentRef {
            owner_kind,
            owner_id,
            ref_kind,
        } => serde_json::json!({
            "kind": "contentRef",
            "ownerKind": owner_kind,
            "ownerId": owner_id,
            "refKind": ref_kind,
        }),
        ContentGcBlocker::ActiveTransfer {
            transfer_id,
            source,
        } => serde_json::json!({
            "kind": "activeTransfer",
            "transferId": transfer_id,
            "source": source,
        }),
    }
}

fn content_gc_candidate_reason(blockers: &[ContentGcBlocker]) -> String {
    if blockers.is_empty() {
        return "unreferenced content blob".to_string();
    }

    serde_json::to_string(
        &blockers
            .iter()
            .map(content_gc_blocker_detail)
            .collect::<Vec<_>>(),
    )
    .unwrap_or_else(|_| "blocked content blob".to_string())
}

fn redact_path_like_words(message: &str) -> String {
    message
        .split_whitespace()
        .map(|part| {
            if part.starts_with('/') {
                "<path>"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone)]
pub struct ReloadableImageConfig {
    pub global_auth_file: Option<PathBuf>,
    pub namespaced_auth_dir: Option<PathBuf>,
    pub registry_config_dir: Option<PathBuf>,
    pub decryption_keys_path: Option<PathBuf>,
    pub decryption_decoder_path: String,
    pub decryption_keyprovider_config: Option<PathBuf>,
    pub pinned_image_patterns: Vec<String>,
    pub signature_policy: Option<PathBuf>,
    pub signature_policy_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ImageMeta {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size: u64,
    pub pinned: bool,
    pub pulled_at: i64,
    pub source_reference: Option<String>,
    pub os: Option<String>,
    pub architecture: Option<String>,
    pub config_user: Option<String>,
    pub config_env: Vec<String>,
    pub config_entrypoint: Vec<String>,
    pub config_cmd: Vec<String>,
    pub config_working_dir: Option<String>,
    pub annotations: HashMap<String, String>,
    pub declared_volumes: Vec<String>,
    pub manifest_media_type: Option<String>,
    pub selected_manifest_digest: Option<String>,
    pub selected_platform: Option<String>,
    pub stored_layers: Vec<StoredLayerMeta>,
    pub artifact_type: Option<String>,
    pub artifact_blobs: Vec<ArtifactBlobMeta>,
}

#[derive(Debug, Clone, Default)]
struct PulledImageMetadata {
    os: Option<String>,
    architecture: Option<String>,
    config_user: Option<String>,
    config_env: Vec<String>,
    config_entrypoint: Vec<String>,
    config_cmd: Vec<String>,
    config_working_dir: Option<String>,
    annotations: HashMap<String, String>,
    declared_volumes: Vec<String>,
    manifest_media_type: Option<String>,
    selected_manifest_digest: Option<String>,
    selected_platform: Option<String>,
    stored_layers: Vec<StoredLayerMeta>,
    artifact_type: Option<String>,
    artifact_blobs: Vec<ArtifactBlobMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StoredLayerMeta {
    #[serde(default)]
    pub digest: String,
    pub path: String,
    pub media_type: String,
    pub source_media_type: String,
    pub encrypted: bool,
}

impl StoredLayerMeta {
    fn relative_display_path(&self) -> Option<String> {
        let path = Path::new(self.path.trim());
        if self.path.trim().is_empty() {
            return None;
        }
        path.strip_prefix("blobs")
            .ok()
            .map(|value| value.display().to_string())
            .or_else(|| Some(self.path.clone()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ArtifactBlobMeta {
    pub digest: String,
    pub media_type: String,
    pub path: String,
    pub size: u64,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedArtifactMount {
    pub source: PathBuf,
    pub relative_path: PathBuf,
}

#[derive(Debug)]
struct PulledLayerData {
    bytes: Vec<u8>,
    media_type: String,
    source_media_type: String,
    encrypted: bool,
}

struct PersistedPullImage {
    requested_ref: String,
    canonical_ref: String,
    reference: Reference,
    image_id: String,
    image_size: u64,
    layers_to_persist: Vec<PulledLayerData>,
    pulled_metadata: PulledImageMetadata,
}

struct LayerDownloadRequest<'a> {
    endpoint: &'a RegistryEndpoint,
    reference: &'a Reference,
    layer_digest: &'a str,
    idx: usize,
}

#[derive(Debug, Deserialize)]
struct DockerConfigFile {
    #[serde(default)]
    auths: HashMap<String, DockerAuthEntry>,
}

#[derive(Debug, Deserialize, Default)]
struct DockerAuthEntry {
    #[serde(default)]
    auth: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct PersistedContainerMountState {
    mounts: Vec<PersistedStoredMount>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct PersistedStoredMount {
    image: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegistryEndpoint {
    base_url: String,
    can_pull: bool,
    can_resolve: bool,
    skip_verify: bool,
}

#[cfg(test)]
pub(crate) type TestPullHandler =
    dyn Fn(TestPullRequest) -> Result<TestPullResponse, Status> + Send + Sync;

#[cfg(test)]
pub(crate) type TestPullScopeObserver = dyn Fn(&'static str, bool) + Send + Sync;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TestRegistryAuth {
    Anonymous,
    Basic { username: String, password: String },
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TestPullRequest {
    pub requested_ref: String,
    pub canonical_ref: String,
    pub pull_namespace: Option<String>,
    pub auth: TestRegistryAuth,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct TestPullResponse {
    pub image_id: String,
    pub size: u64,
    pub annotations: HashMap<String, String>,
    pub declared_volumes: Vec<String>,
}

#[cfg(test)]
const TEST_PULL_LAYER_MEDIA_TYPE: &str = "application/vnd.oci.image.layer.v1.tar+gzip";

#[cfg(test)]
const TEST_EMPTY_LAYER_TAR_GZ: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xed, 0xce, 0xc1, 0x0d, 0x83, 0x30,
    0x0c, 0x05, 0x50, 0x8f, 0x92, 0x09, 0x5a, 0x27, 0x6a, 0x9a, 0x79, 0x98, 0x00, 0x89, 0xd2, 0xfd,
    0x8b, 0xb8, 0x56, 0x82, 0x13, 0x9c, 0xde, 0xbb, 0x7c, 0xeb, 0xdb, 0x07, 0x3f, 0x9e, 0x71, 0xb9,
    0xdc, 0x8c, 0xcc, 0x3d, 0xf3, 0x3f, 0xf7, 0xb9, 0xf6, 0x3a, 0xfa, 0xab, 0xbd, 0x5b, 0xeb, 0x5b,
    0x3f, 0x46, 0xcd, 0x28, 0xfd, 0xfa, 0xd7, 0x22, 0xbe, 0x9f, 0x75, 0x5a, 0x4a, 0x89, 0x65, 0x9e,
    0xd7, 0xa3, 0xbb, 0xb3, 0x3d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xdc, 0xe8,
    0x07, 0x33, 0x84, 0x2a, 0xd3, 0x00, 0x28, 0x00, 0x00,
];

impl ImageServiceImpl {
    fn current_reloadable_config(&self) -> ReloadableImageConfig {
        self.reloadable_config
            .read()
            .expect("image reloadable config lock poisoned")
            .clone()
    }

    pub fn apply_reloadable_config(&self, next: ReloadableImageConfig) {
        *self
            .reloadable_config
            .write()
            .expect("image reloadable config lock poisoned") = next;
    }

    pub fn reloadable_config_snapshot(&self) -> ReloadableImageConfig {
        self.current_reloadable_config()
    }

    pub fn pull_cgroup_effective_config(&self) -> PullCgroupEffectiveConfig {
        self.pull_cgroup.effective_config()
    }

    pub fn last_pull_cgroup_scope(&self) -> Option<PullCgroupScopeRecord> {
        self.pull_cgroup.last_scope()
    }

    pub fn content_transfer_status(&self) -> ContentTransferStatus {
        self.transfer_tracker.snapshot()
    }

    fn persist_content_transfer_record(&self, record: &ContentTransferRecord) -> Result<(), Error> {
        let Some(db_path) = self.ledger_db_path.as_ref() else {
            return Ok(());
        };
        StorageManager::new(db_path)
            .and_then(|mut storage| storage.save_content_transfer(&record.to_storage()))
            .map_err(|err| Error::Storage(format!("failed to persist content transfer: {err}")))
    }

    fn persist_content_transfer_by_id(&self, transfer_id: &str) -> Result<(), Error> {
        let Some(record) = self.transfer_tracker.record(transfer_id) else {
            return Ok(());
        };
        self.persist_content_transfer_record(&record)
    }

    fn publish_image_internal_event(
        &self,
        image_ref: &str,
        kind: &str,
        severity: crate::services::InternalEventSeverity,
        details: serde_json::Value,
    ) {
        let Some(db_path) = self.ledger_db_path.as_ref() else {
            return;
        };
        let event =
            crate::services::InternalEvent::new(kind, "image", image_ref, severity, details);
        if let Err(err) = crate::services::LedgerInternalEventSink::new(db_path).publish(&event) {
            log::debug!("Failed to publish image internal event {kind} for {image_ref}: {err}");
        }
    }

    fn publish_gc_internal_event(
        &self,
        subject_id: &str,
        kind: &str,
        severity: crate::services::InternalEventSeverity,
        details: serde_json::Value,
    ) {
        let Some(db_path) = self.ledger_db_path.as_ref() else {
            return;
        };
        let event = crate::services::InternalEvent::new(kind, "gc", subject_id, severity, details);
        if let Err(err) = crate::services::LedgerInternalEventSink::new(db_path).publish(&event) {
            log::debug!("Failed to publish GC internal event {kind} for {subject_id}: {err}");
        }
    }

    pub fn collect_content_garbage(&self, dry_run: bool) -> Result<ContentGcSummary, Error> {
        let Some(db_path) = self.ledger_db_path.as_ref() else {
            return Ok(ContentGcSummary {
                dry_run,
                ..Default::default()
            });
        };
        let storage = StorageManager::new(db_path)
            .map_err(|err| Error::Storage(format!("failed to open content GC ledger: {err}")))?;
        let candidates = storage.list_content_gc_candidates().map_err(|err| {
            Error::Storage(format!("failed to list content GC candidates: {err}"))
        })?;
        let mut summary = ContentGcSummary {
            dry_run,
            candidates: candidates.len(),
            ..Default::default()
        };

        for candidate in candidates {
            let digest = candidate.blob.digest.clone();
            if candidate.blockers.is_empty() {
                self.publish_gc_internal_event(
                    &digest,
                    "gc.candidate",
                    crate::services::InternalEventSeverity::Info,
                    serde_json::json!({
                        "digest": digest,
                        "size": candidate.blob.size,
                        "dryRun": dry_run,
                    }),
                );
                if dry_run {
                    summary.eligible += 1;
                    continue;
                }
                match self.content_store.delete_blob(&candidate.blob.digest) {
                    Ok(()) => {
                        summary.deleted += 1;
                        summary.bytes_deleted += candidate.blob.size;
                        self.publish_gc_internal_event(
                            &candidate.blob.digest,
                            "gc.delete",
                            crate::services::InternalEventSeverity::Info,
                            serde_json::json!({
                                "digest": candidate.blob.digest,
                                "size": candidate.blob.size,
                            }),
                        );
                    }
                    Err(err) => {
                        summary.failed += 1;
                        self.publish_gc_internal_event(
                            &candidate.blob.digest,
                            "gc.fail",
                            crate::services::InternalEventSeverity::Error,
                            serde_json::json!({
                                "digest": candidate.blob.digest,
                                "message": err.to_string(),
                            }),
                        );
                    }
                }
            } else {
                summary.skipped += 1;
                self.publish_gc_internal_event(
                    &digest,
                    "gc.skip",
                    crate::services::InternalEventSeverity::Debug,
                    serde_json::json!({
                        "digest": digest,
                        "blockers": candidate.blockers.iter().map(content_gc_blocker_detail).collect::<Vec<_>>(),
                    }),
                );
            }
        }

        Ok(summary)
    }

    pub fn content_gc_diagnostics(&self, execute: bool) -> Result<ContentGcDiagnostics, Error> {
        let dry_run = !execute;
        let Some(db_path) = self.ledger_db_path.as_ref() else {
            return Ok(ContentGcDiagnostics {
                dry_run,
                warnings: vec!["content GC ledger is not configured".to_string()],
                ..Default::default()
            });
        };
        let storage = StorageManager::new(db_path)
            .map_err(|err| Error::Storage(format!("failed to open content GC ledger: {err}")))?;
        let candidates = storage.list_content_gc_candidates().map_err(|err| {
            Error::Storage(format!("failed to list content GC candidates: {err}"))
        })?;
        let mut diagnostics = ContentGcDiagnostics {
            dry_run,
            ..Default::default()
        };

        for candidate in candidates {
            let mut item = ContentGcCandidateDiagnostics {
                object_type: "content_blob".to_string(),
                object_id: candidate.blob.digest.clone(),
                path: candidate.blob.relative_path.clone(),
                size_bytes: candidate.blob.size,
                reason: content_gc_candidate_reason(&candidate.blockers),
                ..Default::default()
            };

            if execute && candidate.blockers.is_empty() {
                match self.content_store.delete_blob(&candidate.blob.digest) {
                    Ok(()) => {
                        item.deleted = true;
                        diagnostics.reclaimed_bytes = diagnostics
                            .reclaimed_bytes
                            .saturating_add(candidate.blob.size);
                    }
                    Err(err) => {
                        item.error = Some(redact_path_like_words(&err.to_string()));
                    }
                }
            }

            diagnostics.candidates.push(item);
        }

        Ok(diagnostics)
    }

    fn should_retry_pull_status(status: &Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Internal | tonic::Code::DeadlineExceeded
        )
    }

    async fn execute_pull_with_retries<F, Fut, T>(
        mut attempts_remaining: u32,
        mut operation: F,
    ) -> Result<T, Status>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(status)
                    if attempts_remaining > 0 && Self::should_retry_pull_status(&status) =>
                {
                    attempts_remaining -= 1;
                    warn!(
                        "Image pull operation failed with {}, retrying ({} retries remaining)",
                        status.message(),
                        attempts_remaining
                    );
                }
                Err(status) => return Err(status),
            }
        }
    }

    async fn collect_with_concurrency_limit<T, O, F, Fut>(
        max_concurrent: usize,
        jobs: Vec<T>,
        fetch: F,
    ) -> Result<Vec<O>, Status>
    where
        T: Send,
        F: Fn(T) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<O, Status>> + Send,
        O: Send,
    {
        stream::iter(jobs.into_iter().map(|job| {
            let fetch = fetch.clone();
            async move { fetch(job).await }
        }))
        .buffer_unordered(max_concurrent)
        .try_collect()
        .await
    }

    fn has_registry_component(component: &str) -> bool {
        component.contains('.') || component.contains(':') || component == "localhost"
    }

    fn pinned_pattern_matches(pattern: &str, candidate: &str) -> bool {
        let pattern = pattern.trim();
        if pattern.is_empty() {
            return false;
        }
        if pattern.starts_with('*') && pattern.ends_with('*') && pattern.len() > 2 {
            return candidate.contains(&pattern[1..pattern.len() - 1]);
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return candidate.starts_with(prefix);
        }
        candidate == pattern
    }

    fn image_is_pinned_by_patterns<'a>(
        patterns: &[String],
        refs: impl IntoIterator<Item = &'a str>,
    ) -> bool {
        let refs: Vec<&str> = refs.into_iter().collect();
        patterns.iter().any(|pattern| {
            refs.iter()
                .copied()
                .any(|candidate| Self::pinned_pattern_matches(pattern, candidate))
        })
    }

    fn image_is_pinned_meta(&self, meta: &ImageMeta) -> bool {
        let mut refs = meta
            .repo_tags
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        refs.extend(meta.repo_digests.iter().map(String::as_str));
        if let Some(source_reference) = meta.source_reference.as_deref() {
            refs.push(source_reference);
        }
        let reloadable = self.current_reloadable_config();
        Self::image_is_pinned_by_patterns(&reloadable.pinned_image_patterns, refs)
    }

    fn canonicalize_image_reference(reference: &str) -> String {
        let raw = reference.trim();
        if raw.is_empty() || raw.starts_with("sha256:") {
            return raw.to_string();
        }

        let mut normalized = if raw.split('/').count() == 1 {
            format!("docker.io/library/{}", raw)
        } else {
            let mut parts = raw.splitn(2, '/');
            let first = parts.next().unwrap_or_default();
            let remainder = parts.next().unwrap_or_default();
            if Self::has_registry_component(first) {
                raw.to_string()
            } else {
                format!("docker.io/{}/{}", first, remainder)
            }
        };

        let has_digest = normalized.contains('@');
        let last_segment = normalized.rsplit('/').next().unwrap_or_default();
        if !has_digest && !last_segment.contains(':') {
            normalized.push_str(":latest");
        }

        normalized
    }

    fn resolve_pull_reference(&self, reference: &str) -> Result<String, Status> {
        let raw = reference.trim();
        if raw.is_empty() {
            return Err(Status::invalid_argument(
                "Image reference must not be empty",
            ));
        }

        let transport = self.default_transport.trim();
        let without_transport = if let Some(rest) = raw.strip_prefix("docker://") {
            rest
        } else if raw.contains("://") {
            return Err(Status::invalid_argument(format!(
                "unsupported image transport in reference {}",
                raw
            )));
        } else {
            raw
        };

        let first_component = without_transport.split('/').next().unwrap_or_default();
        let is_short_name =
            !without_transport.contains('/') || !Self::has_registry_component(first_component);
        if is_short_name && self.short_name_mode == "enforcing" {
            return Err(Status::invalid_argument(format!(
                "short image names are rejected when image.short_name_mode = enforcing: {}",
                raw
            )));
        }
        if !transport.is_empty() && transport != "docker://" {
            return Err(Status::invalid_argument(format!(
                "unsupported image.default_transport {}",
                transport
            )));
        }

        Ok(Self::canonicalize_image_reference(without_transport))
    }

    fn normalize_image_id(id: &str) -> &str {
        id.strip_prefix("sha256:").unwrap_or(id)
    }

    fn image_id_matches(image_id: &str, candidate: &str) -> bool {
        if image_id == candidate {
            return true;
        }

        let normalized_image_id = Self::normalize_image_id(image_id);
        let normalized_candidate = Self::normalize_image_id(candidate);

        normalized_image_id == normalized_candidate
            || normalized_image_id.starts_with(normalized_candidate)
            || normalized_candidate.starts_with(normalized_image_id)
    }

    fn image_matches_ref(image: &Image, requested_ref: &str) -> bool {
        let canonical_requested = Self::canonicalize_image_reference(requested_ref);
        Self::image_id_matches(&image.id, requested_ref)
            || image.repo_tags.iter().any(|tag| {
                let canonical_tag = Self::canonicalize_image_reference(tag);
                tag == requested_ref
                    || canonical_tag == canonical_requested
                    || tag.starts_with(requested_ref)
                    || requested_ref.starts_with(tag)
                    || canonical_tag.starts_with(&canonical_requested)
                    || canonical_requested.starts_with(&canonical_tag)
            })
            || image.repo_digests.iter().any(|digest| {
                digest == requested_ref
                    || digest.starts_with(requested_ref)
                    || requested_ref.starts_with(digest)
            })
    }

    fn normalized_image(mut image: Image) -> Image {
        if image.spec.is_none() {
            if let Some(tag) = image.repo_tags.first().cloned() {
                image.spec = Some(ImageSpec {
                    image: tag.clone(),
                    user_specified_image: tag,
                    ..Default::default()
                });
            }
        }
        image
    }

    fn image_user_fields_from_config_user(
        config_user: Option<&str>,
    ) -> (Option<Int64Value>, String) {
        let Some(config_user) = config_user
            .map(str::trim)
            .filter(|config_user| !config_user.is_empty())
        else {
            return (None, String::new());
        };

        let user = config_user.split(':').next().unwrap_or(config_user).trim();
        if user.is_empty() {
            return (None, String::new());
        }

        match user.parse::<i64>() {
            Ok(uid) => (Some(Int64Value { value: uid }), String::new()),
            Err(_) => (None, user.to_string()),
        }
    }

    fn image_from_meta(meta: &ImageMeta) -> Image {
        let (uid, username) = Self::image_user_fields_from_config_user(meta.config_user.as_deref());
        let mut image = Image {
            id: meta.id.clone(),
            repo_tags: meta.repo_tags.clone(),
            repo_digests: meta.repo_digests.clone(),
            size: meta.size,
            uid,
            username,
            pinned: meta.pinned,
            spec: meta.repo_tags.first().map(|tag| ImageSpec {
                image: tag.clone(),
                user_specified_image: tag.clone(),
                annotations: meta.annotations.clone(),
                ..Default::default()
            }),
        };
        image.repo_tags.sort();
        image.repo_tags.dedup();
        image.repo_digests.sort();
        image.repo_digests.dedup();
        image
    }

    fn push_unique(target: &mut Vec<String>, value: &str) {
        if !value.is_empty() && !target.iter().any(|existing| existing == value) {
            target.push(value.to_string());
        }
    }

    fn aggregate_image_records<'a, I>(images: I, meta: Option<&ImageMeta>) -> Option<Image>
    where
        I: IntoIterator<Item = &'a Image>,
    {
        let mut iter = images.into_iter();
        let first = iter.next()?;
        let mut merged = first.clone();
        merged.repo_tags.clear();
        merged.repo_digests.clear();

        for tag in &first.repo_tags {
            Self::push_unique(&mut merged.repo_tags, tag);
        }
        for digest in &first.repo_digests {
            Self::push_unique(&mut merged.repo_digests, digest);
        }

        for image in iter {
            if merged.size == 0 {
                merged.size = image.size;
            } else {
                merged.size = merged.size.max(image.size);
            }
            merged.pinned |= image.pinned;
            if merged.uid.is_none() && image.uid.is_some() {
                merged.uid = image.uid.clone();
            }
            if merged.username.is_empty() && !image.username.is_empty() {
                merged.username = image.username.clone();
            }
            if merged.spec.is_none() && image.spec.is_some() {
                merged.spec = image.spec.clone();
            }
            if let Some(spec) = merged.spec.as_mut() {
                if spec.annotations.is_empty() {
                    spec.annotations = image
                        .spec
                        .as_ref()
                        .map(|candidate| candidate.annotations.clone())
                        .unwrap_or_default();
                }
            }
            for tag in &image.repo_tags {
                Self::push_unique(&mut merged.repo_tags, tag);
            }
            for digest in &image.repo_digests {
                Self::push_unique(&mut merged.repo_digests, digest);
            }
        }

        if let Some(meta) = meta {
            if merged.size == 0 {
                merged.size = meta.size;
            }
            merged.pinned |= meta.pinned;
            let (uid, username) =
                Self::image_user_fields_from_config_user(meta.config_user.as_deref());
            if merged.uid.is_none() {
                merged.uid = uid;
            }
            if merged.username.is_empty() {
                merged.username = username;
            }
            if merged.spec.is_none() {
                merged.spec = meta.repo_tags.first().map(|tag| ImageSpec {
                    image: tag.clone(),
                    user_specified_image: tag.clone(),
                    annotations: meta.annotations.clone(),
                    ..Default::default()
                });
            } else if let Some(spec) = merged.spec.as_mut() {
                if spec.annotations.is_empty() {
                    spec.annotations = meta.annotations.clone();
                }
            }
            for tag in &meta.repo_tags {
                Self::push_unique(&mut merged.repo_tags, tag);
            }
            for digest in &meta.repo_digests {
                Self::push_unique(&mut merged.repo_digests, digest);
            }
        }

        merged.repo_tags.sort();
        merged.repo_tags.dedup();
        merged.repo_digests.sort();
        merged.repo_digests.dedup();

        Some(Self::normalized_image(merged))
    }

    fn decode_auth_field(auth_field: &str) -> Option<(String, String)> {
        let raw = auth_field.trim();
        if raw.is_empty() {
            return None;
        }
        let encoded = raw
            .strip_prefix("Basic ")
            .or_else(|| raw.strip_prefix("basic "))
            .unwrap_or(raw);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .ok()?;
        let decoded = String::from_utf8(decoded).ok()?;
        let (username, password) = decoded.split_once(':')?;
        Some((username.to_string(), password.to_string()))
    }

    fn registry_auth_from_auth_config(auth: AuthConfig) -> Result<RegistryAuth, Status> {
        if !auth.username.is_empty() || !auth.password.is_empty() {
            return Ok(RegistryAuth::Basic(auth.username, auth.password));
        }

        if !auth.auth.trim().is_empty() {
            let (username, password) = Self::decode_auth_field(&auth.auth).ok_or_else(|| {
                Status::invalid_argument(
                    "Invalid auth.auth field: expected base64(username:password)",
                )
            })?;
            return Ok(RegistryAuth::Basic(username, password));
        }

        Ok(RegistryAuth::Anonymous)
    }

    fn normalize_registry_key(value: &str) -> String {
        value
            .trim()
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .trim_end_matches("/v1")
            .trim_end_matches("/v1/")
            .trim_end_matches("/v2")
            .trim_end_matches("/v2/")
            .trim_end_matches("/v1/_catalog")
            .trim_end_matches("/v2/_catalog")
            .to_ascii_lowercase()
    }

    fn registry_auth_aliases(registry: &str) -> Vec<String> {
        let normalized = Self::normalize_registry_key(registry);
        match normalized.as_str() {
            "docker.io" | "registry-1.docker.io" | "index.docker.io" => vec![
                "docker.io".to_string(),
                "registry-1.docker.io".to_string(),
                "index.docker.io".to_string(),
                "index.docker.io/v1".to_string(),
                "index.docker.io/v1/".to_string(),
                "https://index.docker.io/v1/".to_string(),
            ],
            _ => vec![normalized],
        }
    }

    fn registry_auth_from_docker_entry(entry: &DockerAuthEntry) -> Option<RegistryAuth> {
        if !entry.username.trim().is_empty() || !entry.password.trim().is_empty() {
            return Some(RegistryAuth::Basic(
                entry.username.clone(),
                entry.password.clone(),
            ));
        }

        Self::decode_auth_field(&entry.auth)
            .map(|(username, password)| RegistryAuth::Basic(username, password))
    }

    fn registry_auth_from_file(
        path: &Path,
        reference: &Reference,
    ) -> Result<Option<RegistryAuth>, Status> {
        let raw = std::fs::read(path).map_err(|err| {
            Status::failed_precondition(format!(
                "failed to read auth file {}: {}",
                path.display(),
                err
            ))
        })?;
        let config: DockerConfigFile = serde_json::from_slice(&raw).map_err(|err| {
            Status::failed_precondition(format!(
                "failed to parse auth file {}: {}",
                path.display(),
                err
            ))
        })?;

        let aliases = Self::registry_auth_aliases(reference.resolve_registry());
        for alias in aliases {
            for (registry, entry) in &config.auths {
                if Self::normalize_registry_key(registry) == alias {
                    return Ok(Self::registry_auth_from_docker_entry(entry));
                }
            }
        }

        Ok(None)
    }

    fn registry_auth_from_global_auth_file(
        &self,
        reference: &Reference,
    ) -> Result<Option<RegistryAuth>, Status> {
        let reloadable = self.current_reloadable_config();
        let Some(path) = reloadable.global_auth_file.as_ref() else {
            return Ok(None);
        };

        Self::registry_auth_from_file(path, reference)
    }

    fn image_name_for_namespaced_auth(reference: &Reference) -> String {
        format!(
            "{}/{}",
            reference.resolve_registry(),
            reference.repository()
        )
    }

    fn namespaced_auth_file_path(&self, namespace: &str, reference: &Reference) -> Option<PathBuf> {
        let reloadable = self.current_reloadable_config();
        let root = reloadable.namespaced_auth_dir.as_ref()?;
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return None;
        }

        let image_name = Self::image_name_for_namespaced_auth(reference);
        let digest = format!("{:x}", Sha256::digest(image_name.as_bytes()));
        Some(root.join(format!("{namespace}-{digest}.json")))
    }

    fn registry_auth_from_namespaced_auth_dir(
        &self,
        reference: &Reference,
        namespace: Option<&str>,
    ) -> Result<Option<RegistryAuth>, Status> {
        let Some(path) =
            namespace.and_then(|namespace| self.namespaced_auth_file_path(namespace, reference))
        else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }

        Self::registry_auth_from_file(&path, reference)
    }

    fn registry_hosts_toml_path(&self, registry: &str) -> Option<PathBuf> {
        let reloadable = self.current_reloadable_config();
        let config_dir = reloadable.registry_config_dir.as_ref()?;
        for alias in Self::registry_auth_aliases(registry) {
            let exact = config_dir.join(&alias).join("hosts.toml");
            if exact.exists() {
                return Some(exact);
            }
        }

        let default = config_dir.join("_default").join("hosts.toml");
        default.exists().then_some(default)
    }

    fn load_registry_endpoints(&self, registry: &str) -> Result<Vec<RegistryEndpoint>, Status> {
        let Some(path) = self.registry_hosts_toml_path(registry) else {
            return Ok(Vec::new());
        };

        let raw = std::fs::read_to_string(&path).map_err(|err| {
            Status::failed_precondition(format!(
                "failed to read image.registry_config_dir hosts file {}: {}",
                path.display(),
                err
            ))
        })?;
        let value: toml::Value = raw.parse().map_err(|err| {
            Status::failed_precondition(format!(
                "failed to parse image.registry_config_dir hosts file {}: {}",
                path.display(),
                err
            ))
        })?;

        let mut endpoints = Vec::new();
        if let Some(hosts) = value.get("host").and_then(|host| host.as_table()) {
            let mut entries = hosts
                .iter()
                .filter_map(|(url, entry)| {
                    let table = entry.as_table()?;
                    let capabilities = table
                        .get("capabilities")
                        .and_then(|value| value.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str())
                                .map(|item| item.to_string())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_else(|| vec!["pull".to_string(), "resolve".to_string()]);
                    let skip_verify = table
                        .get("skip_verify")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false);
                    Some(RegistryEndpoint {
                        base_url: url.trim_end_matches('/').to_string(),
                        can_pull: capabilities.iter().any(|item| item == "pull"),
                        can_resolve: capabilities.iter().any(|item| item == "resolve"),
                        skip_verify,
                    })
                })
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.base_url.cmp(&right.base_url));
            endpoints.extend(entries);
        }

        if let Some(server) = value.get("server").and_then(|server| server.as_str()) {
            let server = server.trim();
            if !server.is_empty() {
                endpoints.push(RegistryEndpoint {
                    base_url: server.trim_end_matches('/').to_string(),
                    can_pull: true,
                    can_resolve: true,
                    skip_verify: false,
                });
            }
        }

        Ok(endpoints)
    }

    fn registry_endpoints_for(
        &self,
        reference: &Reference,
        require_resolve: bool,
    ) -> Result<Vec<RegistryEndpoint>, Status> {
        let mut endpoints = self.load_registry_endpoints(reference.resolve_registry())?;
        endpoints.retain(|endpoint| {
            if require_resolve {
                endpoint.can_resolve
            } else {
                endpoint.can_pull
            }
        });
        if endpoints.is_empty() {
            endpoints.push(RegistryEndpoint {
                base_url: format!("https://{}", reference.resolve_registry()),
                can_pull: true,
                can_resolve: true,
                skip_verify: false,
            });
        }
        Ok(endpoints)
    }

    fn provided_registry_bearer_token(auth: &AuthConfig) -> Option<String> {
        for candidate in [&auth.registry_token, &auth.identity_token] {
            let token = candidate.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }

        None
    }

    fn canonical_image_id(digest: &str, fallback_seed: &[u8]) -> String {
        let digest = digest.trim();
        if digest.is_empty() || digest == "sha256:unknown" {
            return format!("sha256:{:x}", Sha256::digest(fallback_seed));
        }

        if digest.contains(':') {
            digest.to_string()
        } else {
            format!("sha256:{}", digest)
        }
    }

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }

    fn artifact_records_dir(root: &Path) -> PathBuf {
        FilesystemImageMetadataStore::artifact_records_dir(root)
    }

    fn local_record_dir(root: &Path, id: &str, artifact: bool) -> PathBuf {
        FilesystemImageMetadataStore::local_record_dir(root, id, artifact)
    }

    fn is_artifact_meta(meta: &ImageMeta) -> bool {
        meta.artifact_type
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    }

    fn load_meta_from_record_dir(record_dir: &Path) -> Option<ImageMeta> {
        FilesystemImageMetadataStore::load_meta_from_record_dir(record_dir)
    }

    fn additional_artifact_store_roots(&self) -> impl Iterator<Item = &Path> {
        self.additional_artifact_stores.iter().map(PathBuf::as_path)
    }

    fn artifact_mount_candidates(
        root: &Path,
        requested_ref: &str,
    ) -> Result<Option<(ImageMeta, PathBuf)>, Status> {
        let records_dir = Self::artifact_records_dir(root);
        if !records_dir.exists() {
            return Ok(None);
        }

        for entry in std::fs::read_dir(&records_dir).map_err(|err| {
            Status::internal(format!(
                "failed to read artifact records directory {}: {}",
                records_dir.display(),
                err
            ))
        })? {
            let entry = entry.map_err(|err| {
                Status::internal(format!(
                    "failed to read artifact record entry in {}: {}",
                    records_dir.display(),
                    err
                ))
            })?;
            let Some(meta) = Self::load_meta_from_record_dir(&entry.path()) else {
                continue;
            };
            if !Self::is_artifact_meta(&meta) {
                continue;
            }
            let image = Self::image_from_meta(&meta);
            if Self::image_matches_ref(&image, requested_ref) {
                return Ok(Some((meta, entry.path())));
            }
        }

        Ok(None)
    }

    fn normalize_artifact_sub_path(raw: Option<&str>) -> Result<Option<PathBuf>, Status> {
        let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let path = Path::new(raw);
        if path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::CurDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err(Status::invalid_argument(format!(
                "invalid OCI artifact image_sub_path {}",
                raw
            )));
        }
        Ok(Some(path.to_path_buf()))
    }

    pub(crate) fn resolve_artifact_mounts(
        storage_root: &Path,
        additional_artifact_stores: &[PathBuf],
        requested_ref: &str,
        sub_path: Option<&str>,
    ) -> Result<Vec<ResolvedArtifactMount>, Status> {
        let mut search_roots: Vec<&Path> = additional_artifact_stores
            .iter()
            .map(PathBuf::as_path)
            .collect();
        search_roots.push(storage_root);

        let normalized_sub_path = Self::normalize_artifact_sub_path(sub_path)?;
        for root in search_roots {
            let Some((meta, record_dir)) = Self::artifact_mount_candidates(root, requested_ref)?
            else {
                continue;
            };
            let mut mounts = Vec::new();
            let mut matched = false;
            for (index, blob) in meta.artifact_blobs.iter().enumerate() {
                let blob_path = Path::new(blob.path.trim());
                if blob.path.trim().is_empty()
                    || blob_path.is_absolute()
                    || blob_path.components().any(|component| {
                        matches!(
                            component,
                            std::path::Component::ParentDir
                                | std::path::Component::CurDir
                                | std::path::Component::Prefix(_)
                        )
                    })
                {
                    continue;
                }

                let relative_path = if let Some(sub_path) = normalized_sub_path.as_ref() {
                    if blob_path == sub_path {
                        matched = true;
                        PathBuf::from(
                            blob_path
                                .file_name()
                                .and_then(|value| value.to_str())
                                .unwrap_or("artifact"),
                        )
                    } else if let Ok(stripped) = blob_path.strip_prefix(sub_path) {
                        matched = true;
                        stripped.to_path_buf()
                    } else {
                        continue;
                    }
                } else {
                    matched = true;
                    blob_path.to_path_buf()
                };

                if relative_path.as_os_str().is_empty() {
                    continue;
                }

                mounts.push(ResolvedArtifactMount {
                    source: record_dir.join(format!("{index}.tar.gz")),
                    relative_path,
                });
            }

            if normalized_sub_path.is_some() && !matched {
                return Err(Status::failed_precondition(format!(
                    "OCI artifact sub path {} does not exist in {}",
                    normalized_sub_path
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_default(),
                    requested_ref
                )));
            }

            if mounts.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "OCI artifact {} has no mountable blobs",
                    requested_ref
                )));
            }

            return Ok(mounts);
        }

        Err(Status::not_found(format!(
            "OCI artifact {} is not present locally",
            requested_ref
        )))
    }

    fn repo_digest_for_reference(reference: &Reference, image_id: &str) -> Option<String> {
        if !image_id.contains(':') {
            return None;
        }
        Some(format!(
            "{}/{}@{}",
            reference.resolve_registry(),
            reference.repository(),
            image_id
        ))
    }

    fn build_image_verbose_info(
        image: &Image,
        metadata_store: &FilesystemImageMetadataStore,
        content_store: &FsContentStore,
        storage_driver: &str,
        storage_options: &[String],
        parsed_storage_options: &OverlayImageStorageOptions,
    ) -> Result<HashMap<String, String>, Status> {
        let stored = metadata_store.load_by_id(&image.id);
        let image_dir = stored
            .as_ref()
            .map(|record| record.record_dir.clone())
            .unwrap_or_else(|| {
                FilesystemImageMetadataStore::image_records_dir(metadata_store.storage_root())
                    .join(&image.id)
            });
        let meta = stored.as_ref().map(|record| &record.meta);
        let layer_files = meta
            .map(|meta| {
                if !meta.stored_layers.is_empty() {
                    return meta
                        .stored_layers
                        .iter()
                        .filter_map(StoredLayerMeta::relative_display_path)
                        .collect::<Vec<_>>();
                }
                Vec::new()
            })
            .filter(|layers| !layers.is_empty())
            .unwrap_or_else(|| {
                std::fs::read_dir(&image_dir)
                    .ok()
                    .into_iter()
                    .flat_map(|entries| entries.flatten())
                    .filter_map(|entry| {
                        let path = entry.path();
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .filter(|_| {
                                matches!(
                                    path.extension().and_then(|ext| ext.to_str()),
                                    Some("gz" | "tar")
                                )
                            })
                            .map(|name| name.to_string())
                    })
                    .collect::<Vec<_>>()
            });
        let (metadata_bytes, metadata_inodes) = metadata_store.usage().unwrap_or((0, 0));
        let (content_bytes, content_inodes) = content_store.total_usage().unwrap_or((0, 0));
        let collected_at = Self::now_nanos();

        let payload = serde_json::json!({
            "id": image.id,
            "repoTags": image.repo_tags,
            "repoDigests": image.repo_digests,
            "size": image.size,
            "pinned": image.pinned,
            "pulledAt": meta.map(|meta| meta.pulled_at),
            "sourceReference": meta.and_then(|meta| meta.source_reference.clone()),
            "os": meta.and_then(|meta| meta.os.clone()),
            "architecture": meta.and_then(|meta| meta.architecture.clone()),
            "configUser": meta.and_then(|meta| meta.config_user.clone()),
            "annotations": meta
                .map(|meta| meta.annotations.clone())
                .unwrap_or_default(),
            "declaredVolumes": meta
                .map(|meta| meta.declared_volumes.clone())
                .unwrap_or_default(),
            "manifestMediaType": meta
                .and_then(|meta| meta.manifest_media_type.clone()),
            "selectedManifestDigest": meta
                .and_then(|meta| meta.selected_manifest_digest.clone()),
            "selectedPlatform": meta
                .and_then(|meta| meta.selected_platform.clone()),
            "storedLayers": meta
                .map(|meta| meta.stored_layers.clone())
                .unwrap_or_default(),
            "artifactType": meta.and_then(|meta| meta.artifact_type.clone()),
            "artifactBlobs": meta
                .map(|meta| meta.artifact_blobs.clone())
                .unwrap_or_default(),
            "storagePath": image_dir.display().to_string(),
            "storageDriver": storage_driver,
            "storageOptions": storage_options,
            "effectiveStorageOptions": parsed_storage_options,
            "layers": layer_files,
            "snapshotStats": {
                "metadataBytes": metadata_bytes,
                "metadataInodes": metadata_inodes,
                "contentBytes": content_bytes,
                "contentInodes": content_inodes,
                "layerCount": layer_files.len(),
                "collectedAt": collected_at,
            },
        });

        let mut info = HashMap::new();
        info.insert(
            "info".to_string(),
            serde_json::to_string(&payload).map_err(|e| {
                Status::internal(format!("Failed to encode image verbose info: {}", e))
            })?,
        );
        Ok(info)
    }

    fn database_path(&self) -> Option<PathBuf> {
        if self.storage_path.file_name().and_then(|name| name.to_str()) == Some("storage") {
            self.storage_path
                .parent()
                .map(|parent| parent.join("crius.db"))
        } else {
            Some(self.storage_path.join("crius.db"))
        }
    }

    fn signature_policy_path_for_namespace(&self, namespace: Option<&str>) -> Option<PathBuf> {
        let namespace = namespace
            .map(str::trim)
            .filter(|namespace| !namespace.is_empty());
        let reloadable = self.current_reloadable_config();
        if let (Some(dir), Some(namespace)) = (reloadable.signature_policy_dir.as_ref(), namespace)
        {
            let candidate = dir.join(format!("{namespace}.json"));
            if candidate.exists() {
                return Some(candidate);
            }
        }
        reloadable.signature_policy
    }

    fn enforce_signature_policy(
        &self,
        reference: &Reference,
        namespace: Option<&str>,
    ) -> Result<(), Status> {
        let Some(path) = self.signature_policy_path_for_namespace(namespace) else {
            return Ok(());
        };
        let policy = crate::image::policy::load_signature_policy(&path).map_err(|err| {
            Status::failed_precondition(format!(
                "failed to load signature policy {}: {}",
                path.display(),
                err
            ))
        })?;
        crate::image::policy::evaluate_signature_policy(&policy, reference).map_err(|err| {
            Status::failed_precondition(format!("signature policy rejected {}: {}", reference, err))
        })
    }

    fn decrypted_media_type_for(source_media_type: &str) -> Result<(String, &'static str), Status> {
        match source_media_type.trim() {
            "application/vnd.oci.image.layer.v1.tar+gzip+encrypted"
            | "application/vnd.docker.image.rootfs.diff.tar.gzip+encrypted" => Ok((
                "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                "tar.gz",
            )),
            "application/vnd.oci.image.layer.v1.tar+encrypted"
            | "application/vnd.docker.image.rootfs.diff.tar+encrypted" => {
                Ok(("application/vnd.oci.image.layer.v1.tar".to_string(), "tar"))
            }
            other => Err(Status::failed_precondition(format!(
                "unsupported encrypted layer media type {}",
                other
            ))),
        }
    }

    fn plain_media_type_to_extension(media_type: &str) -> &'static str {
        match media_type.trim() {
            "application/vnd.oci.image.layer.v1.tar"
            | "application/vnd.docker.image.rootfs.diff.tar" => "tar",
            _ => "tar.gz",
        }
    }

    fn image_decryption_enabled(&self) -> bool {
        !self
            .current_reloadable_config()
            .decryption_keys_path
            .as_ref()
            .map(|path| path.as_os_str().is_empty())
            .unwrap_or(true)
    }

    fn decrypt_layer_bytes(
        &self,
        source_media_type: &str,
        encrypted_bytes: &[u8],
    ) -> Result<(Vec<u8>, String), Status> {
        let (decrypted_media_type, _) = Self::decrypted_media_type_for(source_media_type)?;
        let reloadable = self.current_reloadable_config();
        let keys_path = reloadable.decryption_keys_path.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "encrypted image layer requires image.decryption_keys_path to be configured",
            )
        })?;
        let mut command = std::process::Command::new(reloadable.decryption_decoder_path.trim());
        command.arg("--decryption-keys-path").arg(keys_path);
        if let Some(config) = reloadable.decryption_keyprovider_config.as_ref() {
            command.env("OCICRYPT_KEYPROVIDER_CONFIG", config.as_os_str());
        }
        command.stdin(std::process::Stdio::piped());
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());
        let mut child = command.spawn().map_err(|err| {
            Status::failed_precondition(format!(
                "failed to start image decryption decoder {}: {}",
                reloadable.decryption_decoder_path, err
            ))
        })?;
        if let Some(stdin) = child.stdin.as_mut() {
            use std::io::Write;
            stdin.write_all(encrypted_bytes).map_err(|err| {
                Status::internal(format!(
                    "failed to write encrypted layer to decoder stdin: {}",
                    err
                ))
            })?;
        }
        let output = child.wait_with_output().map_err(|err| {
            Status::internal(format!(
                "failed to wait for image decryption decoder: {}",
                err
            ))
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(Status::failed_precondition(format!(
                "image decryption failed for media type {}: {}",
                source_media_type,
                if stderr.is_empty() {
                    format!("decoder exited with {}", output.status)
                } else {
                    stderr
                }
            )));
        }
        Ok((output.stdout, decrypted_media_type))
    }

    fn image_is_in_use(
        &self,
        requested_ref: &str,
        candidate_ids: &HashSet<String>,
        candidate_refs: &HashSet<String>,
    ) -> Result<(), Status> {
        let Some(db_path) = self.database_path() else {
            return Ok(());
        };
        if !db_path.exists() {
            return Ok(());
        }

        let storage = StorageManager::new(&db_path).map_err(|e| {
            Status::internal(format!(
                "Failed to open container metadata database {}: {}",
                db_path.display(),
                e
            ))
        })?;
        let containers = storage.list_containers().map_err(|e| {
            Status::internal(format!(
                "Failed to list containers from metadata database {}: {}",
                db_path.display(),
                e
            ))
        })?;

        for container in containers {
            if candidate_ids
                .iter()
                .any(|image_id| Self::image_id_matches(image_id, &container.image))
                || candidate_refs.contains(&container.image)
                || container.image == requested_ref
            {
                return Err(Status::failed_precondition(format!(
                    "image {} is in use by container {}",
                    requested_ref, container.id
                )));
            }

            let annotations: HashMap<String, String> =
                serde_json::from_str(&container.annotations).unwrap_or_default();
            let mount_state = annotations
                .get("io.crius.internal/container-state")
                .and_then(|raw| serde_json::from_str::<PersistedContainerMountState>(raw).ok())
                .unwrap_or_default();
            if mount_state
                .mounts
                .iter()
                .any(|mount| !mount.image.is_empty() && candidate_refs.contains(&mount.image))
            {
                return Err(Status::failed_precondition(format!(
                    "image {} is in use by container {}",
                    requested_ref, container.id
                )));
            }
        }

        Ok(())
    }

    pub fn metrics_provider(&self) -> ImageMetricsProvider {
        ImageMetricsProvider {
            images: self.images.clone(),
            _storage_path: self.storage_path.clone(),
            content_store: self.content_store.clone(),
            metadata_store: self.metadata_store.clone(),
        }
    }

    pub fn new_with_options(options: ImageServiceOptions) -> Result<Self, Error> {
        let ImageServiceOptions {
            storage_path,
            ledger_db_path,
            storage_driver,
            storage_options,
            global_auth_file,
            namespaced_auth_dir,
            default_transport,
            short_name_mode,
            pull_progress_timeout,
            max_concurrent_downloads,
            pull_retry_count,
            registry_config_dir,
            decryption_keys_path,
            decryption_decoder_path,
            decryption_keyprovider_config,
            additional_artifact_stores,
            pinned_image_patterns,
            signature_policy,
            signature_policy_dir,
            big_files_temporary_dir,
            separate_pull_cgroup,
            cgroup_driver,
            rootless,
            disable_cgroup,
            #[cfg(test)]
            pull_cgroup_root,
        } = options;
        #[cfg(not(test))]
        let pull_cgroup = PullCgroupExecutor::new(
            separate_pull_cgroup,
            cgroup_driver,
            rootless.enabled,
            disable_cgroup,
        );
        #[cfg(test)]
        let pull_cgroup = PullCgroupExecutor::new_with_root(
            separate_pull_cgroup,
            cgroup_driver,
            rootless.enabled,
            disable_cgroup,
            pull_cgroup_root.unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup")),
        );
        let reloadable_config = ReloadableImageConfig {
            global_auth_file,
            namespaced_auth_dir,
            registry_config_dir,
            decryption_keys_path,
            decryption_decoder_path,
            decryption_keyprovider_config,
            pinned_image_patterns,
            signature_policy,
            signature_policy_dir,
        };
        let storage_driver = storage_driver.trim().to_string();

        if storage_driver != "overlay" {
            return Err(Error::Config(format!(
                "image.driver must be \"overlay\", got {}",
                storage_driver
            )));
        }
        let parsed_storage_options =
            OverlayImageStorageOptions::parse(&storage_driver, &storage_options)?;

        if !storage_path.exists() {
            std::fs::create_dir_all(&storage_path).context("Failed to create storage directory")?;
        }

        let images = std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let content_store = Arc::new(FsContentStore::new_with_ledger(
            &storage_path,
            ledger_db_path.clone(),
        )?);
        let transfer_tracker = ContentTransferTracker::new_with_ledger(ledger_db_path.clone())?;
        let metadata_store = Arc::new(FilesystemImageMetadataStore::new(
            &storage_path,
            additional_artifact_stores.clone(),
            ledger_db_path.clone(),
        ));

        Ok(Self {
            images,
            storage_path,
            storage_driver,
            storage_options,
            parsed_storage_options,
            content_store,
            metadata_store,
            default_transport,
            short_name_mode,
            pull_progress_timeout,
            max_concurrent_downloads,
            pull_retry_count,
            additional_artifact_stores,
            _big_files_temporary_dir: big_files_temporary_dir,
            in_progress_pulls: Arc::new(Mutex::new(HashMap::new())),
            transfer_tracker,
            #[cfg(test)]
            test_pull_handler: std::sync::Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            test_pull_scope_observer: std::sync::Arc::new(std::sync::Mutex::new(None)),
            reloadable_config: Arc::new(RwLock::new(reloadable_config)),
            pull_cgroup,
            ledger_db_path,
        })
    }

    pub async fn ensure_image_exists_for_sandbox(
        &self,
        image_ref: &str,
        sandbox_config: &crate::proto::runtime::v1::PodSandboxConfig,
    ) -> Result<Image, Status> {
        let canonical_ref = self.resolve_pull_reference(image_ref)?;
        if let Some(image) = self.find_local_image(&canonical_ref).await {
            return Ok(image);
        }

        ImageService::pull_image(
            self,
            Request::new(PullImageRequest {
                image: Some(ImageSpec {
                    image: image_ref.to_string(),
                    user_specified_image: image_ref.to_string(),
                    ..Default::default()
                }),
                auth: None,
                sandbox_config: Some(sandbox_config.clone()),
            }),
        )
        .await?;

        self.find_local_image(&canonical_ref).await.ok_or_else(|| {
            Status::internal(format!(
                "image {} was pulled for pod sandbox but is still unavailable locally",
                canonical_ref
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn set_test_pull_handler(&self, handler: std::sync::Arc<TestPullHandler>) {
        if let Ok(mut slot) = self.test_pull_handler.lock() {
            *slot = Some(handler);
        }
    }

    #[cfg(test)]
    pub(crate) fn set_test_pull_scope_observer(
        &self,
        observer: std::sync::Arc<TestPullScopeObserver>,
    ) {
        if let Ok(mut slot) = self.test_pull_scope_observer.lock() {
            *slot = Some(observer);
        }
    }

    #[cfg(test)]
    fn snapshot_test_pull_handler(&self) -> Option<std::sync::Arc<TestPullHandler>> {
        self.test_pull_handler
            .lock()
            .ok()
            .and_then(|handler| handler.clone())
    }

    #[cfg(test)]
    fn transfer_provider_kind(&self) -> RemoteContentProviderKind {
        if self.snapshot_test_pull_handler().is_some() {
            RemoteContentProviderKind::Test
        } else {
            RemoteContentProviderKind::Registry
        }
    }

    #[cfg(not(test))]
    fn transfer_provider_kind(&self) -> RemoteContentProviderKind {
        RemoteContentProviderKind::Registry
    }

    #[cfg(test)]
    fn observe_test_pull_scope(&self, stage: &'static str) {
        if let Some(observer) = self
            .test_pull_scope_observer
            .lock()
            .ok()
            .and_then(|observer| observer.clone())
        {
            observer(stage, self.pull_cgroup.scope_active());
        }
    }

    #[cfg(not(test))]
    fn observe_test_pull_scope(&self, _stage: &'static str) {}

    #[cfg(test)]
    fn test_registry_auth(auth: &RegistryAuth) -> TestRegistryAuth {
        match auth {
            RegistryAuth::Anonymous => TestRegistryAuth::Anonymous,
            RegistryAuth::Basic(username, password) => TestRegistryAuth::Basic {
                username: username.clone(),
                password: password.clone(),
            },
        }
    }

    // 加载本地镜像
    pub async fn load_local_images(&self) -> Result<(), Error> {
        let mut images = self.images.lock().await;
        images.clear();
        for record in self.metadata_store.load_all()? {
            let mut meta = record.meta;
            meta.pinned = self.image_is_pinned_meta(&meta);
            let image = Self::image_from_meta(&meta);
            for tag in &meta.repo_tags {
                images.insert(tag.clone(), image.clone());
            }
        }

        Ok(())
    }

    async fn find_local_image(&self, image_ref: &str) -> Option<Image> {
        let canonical_ref = Self::canonicalize_image_reference(image_ref);
        {
            let images = self.images.lock().await;
            if let Some(image) = images.get(image_ref) {
                return Some(image.clone());
            }
            if let Some(image) = images.get(&canonical_ref) {
                return Some(image.clone());
            }
        }

        if let Ok(Some(record)) = self
            .metadata_store
            .find_by_reference(image_ref, Self::image_matches_ref)
        {
            let mut meta = record.meta;
            meta.pinned = self.image_is_pinned_meta(&meta);
            let image = Self::image_from_meta(&meta);
            let mut images = self.images.lock().await;
            for tag in &meta.repo_tags {
                images.insert(tag.clone(), image.clone());
            }
            return Some(image);
        }

        None
    }

    // 保存镜像元数据
    async fn save_image_metadata(&self, image: &CriusImage) -> Result<(), Error> {
        self.metadata_store.save(image)?;
        Ok(())
    }

    async fn persist_pulled_image(
        &self,
        pull: PersistedPullImage,
    ) -> Result<Response<PullImageResponse>, Status> {
        let PersistedPullImage {
            requested_ref,
            canonical_ref,
            reference,
            image_id,
            image_size,
            layers_to_persist,
            pulled_metadata,
        } = pull;
        let repo_digests = Self::repo_digest_for_reference(&reference, &image_id)
            .into_iter()
            .collect::<Vec<_>>();

        let is_artifact = pulled_metadata
            .artifact_type
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        self.observe_test_pull_scope("persist-record-start");
        let record_dir = Self::local_record_dir(&self.storage_path, &image_id, is_artifact);
        std::fs::create_dir_all(&record_dir).map_err(|e: io::Error| {
            Status::internal(format!("Failed to create image record directory: {}", e))
        })?;
        let persisted_layers = layers_to_persist
            .into_iter()
            .map(|layer| {
                self.content_store
                    .put_blob("", &layer.media_type, &layer.bytes)
                    .map(|info| StoredLayerMeta {
                        digest: info.digest,
                        path: info.relative_path.display().to_string(),
                        media_type: layer.media_type.clone(),
                        source_media_type: layer.source_media_type,
                        encrypted: layer.encrypted,
                    })
                    .map_err(|err| {
                        Status::internal(format!("Failed to persist layer blob: {}", err))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut pulled_metadata = pulled_metadata;
        pulled_metadata.stored_layers = persisted_layers;
        let reloadable = self.current_reloadable_config();

        self.save_image_metadata(&CriusImage {
            id: image_id.clone(),
            repo_tags: vec![canonical_ref.clone()],
            repo_digests: repo_digests.clone(),
            size: image_size,
            pinned: Self::image_is_pinned_by_patterns(
                &reloadable.pinned_image_patterns,
                [canonical_ref.as_str(), requested_ref.as_str()],
            ),
            pulled_at: Self::now_nanos(),
            source_reference: (canonical_ref != requested_ref).then_some(requested_ref.clone()),
            os: pulled_metadata.os.clone(),
            architecture: pulled_metadata.architecture.clone(),
            config_user: pulled_metadata.config_user.clone(),
            config_env: pulled_metadata.config_env.clone(),
            config_entrypoint: pulled_metadata.config_entrypoint.clone(),
            config_cmd: pulled_metadata.config_cmd.clone(),
            config_working_dir: pulled_metadata.config_working_dir.clone(),
            annotations: pulled_metadata.annotations.clone(),
            declared_volumes: pulled_metadata.declared_volumes.clone(),
            manifest_media_type: pulled_metadata.manifest_media_type.clone(),
            selected_manifest_digest: pulled_metadata.selected_manifest_digest.clone(),
            selected_platform: pulled_metadata.selected_platform.clone(),
            stored_layers: pulled_metadata.stored_layers.clone(),
            artifact_type: pulled_metadata.artifact_type.clone(),
            artifact_blobs: pulled_metadata.artifact_blobs.clone(),
        })
        .await
        .map_err(|e| {
            error!("Failed to save image metadata: {}", e);
            Status::internal(format!("Failed to save image metadata: {}", e))
        })?;
        self.observe_test_pull_scope("persist-metadata-saved");

        let image = Image {
            id: image_id.clone(),
            repo_tags: vec![canonical_ref.clone()],
            repo_digests,
            size: image_size,
            pinned: Self::image_is_pinned_by_patterns(
                &reloadable.pinned_image_patterns,
                [canonical_ref.as_str(), requested_ref.as_str()],
            ),
            spec: Some(ImageSpec {
                image: canonical_ref.clone(),
                user_specified_image: requested_ref.clone(),
                annotations: pulled_metadata.annotations.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let mut images = self.images.lock().await;
        images.insert(canonical_ref, image);
        drop(images);

        info!("Image {} pulled successfully", image_id);

        Ok(Response::new(PullImageResponse {
            image_ref: image_id,
        }))
    }

    #[cfg(test)]
    async fn pull_via_test_handler(
        &self,
        handler: std::sync::Arc<TestPullHandler>,
        requested_ref: &str,
        canonical_ref: &str,
        auth: &RegistryAuth,
        pull_namespace: Option<&str>,
    ) -> Result<Response<PullImageResponse>, Status> {
        self.observe_test_pull_scope("test-handler");
        let response = handler(TestPullRequest {
            requested_ref: requested_ref.to_string(),
            canonical_ref: canonical_ref.to_string(),
            pull_namespace: pull_namespace.map(ToString::to_string),
            auth: Self::test_registry_auth(auth),
        })?;
        let reference: Reference = canonical_ref
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid image reference: {}", e)))?;
        let layers_to_persist = vec![PulledLayerData {
            bytes: TEST_EMPTY_LAYER_TAR_GZ.to_vec(),
            media_type: TEST_PULL_LAYER_MEDIA_TYPE.to_string(),
            source_media_type: TEST_PULL_LAYER_MEDIA_TYPE.to_string(),
            encrypted: false,
        }];
        let metadata = PulledImageMetadata {
            annotations: response.annotations,
            declared_volumes: response.declared_volumes,
            stored_layers: vec![StoredLayerMeta {
                digest: String::new(),
                path: "0.tar.gz".to_string(),
                media_type: TEST_PULL_LAYER_MEDIA_TYPE.to_string(),
                source_media_type: TEST_PULL_LAYER_MEDIA_TYPE.to_string(),
                encrypted: false,
            }],
            ..Default::default()
        };
        self.persist_pulled_image(PersistedPullImage {
            requested_ref: requested_ref.to_string(),
            canonical_ref: canonical_ref.to_string(),
            reference,
            image_id: response.image_id,
            image_size: response.size.max(TEST_EMPTY_LAYER_TAR_GZ.len() as u64),
            layers_to_persist,
            pulled_metadata: metadata,
        })
        .await
    }

    fn load_image_metadata(&self, image_id: &str) -> Option<ImageMeta> {
        self.metadata_store
            .load_by_id(image_id)
            .map(|record| record.meta)
    }

    async fn persist_local_image_alias(
        &self,
        image: &Image,
        requested_ref: &str,
        canonical_ref: &str,
    ) -> Result<(), Error> {
        let Some(existing) = self.load_image_metadata(&image.id) else {
            return Ok(());
        };
        let reloadable = self.current_reloadable_config();

        let mut repo_tags = existing.repo_tags.clone();
        Self::push_unique(&mut repo_tags, canonical_ref);
        if requested_ref != canonical_ref {
            Self::push_unique(&mut repo_tags, requested_ref);
        }
        repo_tags.sort();
        repo_tags.dedup();

        self.save_image_metadata(&CriusImage {
            id: image.id.clone(),
            repo_tags,
            repo_digests: image.repo_digests.clone(),
            size: image.size,
            pinned: Self::image_is_pinned_by_patterns(
                &reloadable.pinned_image_patterns,
                image
                    .repo_tags
                    .iter()
                    .map(String::as_str)
                    .chain([canonical_ref, requested_ref]),
            ),
            pulled_at: existing.pulled_at,
            source_reference: if requested_ref != canonical_ref {
                Some(requested_ref.to_string())
            } else {
                existing.source_reference
            },
            os: existing.os,
            architecture: existing.architecture,
            config_user: existing.config_user,
            config_env: existing.config_env,
            config_entrypoint: existing.config_entrypoint,
            config_cmd: existing.config_cmd,
            config_working_dir: existing.config_working_dir,
            annotations: existing.annotations,
            declared_volumes: existing.declared_volumes,
            manifest_media_type: existing.manifest_media_type,
            selected_manifest_digest: existing.selected_manifest_digest,
            selected_platform: existing.selected_platform,
            stored_layers: existing.stored_layers,
            artifact_type: existing.artifact_type,
            artifact_blobs: existing.artifact_blobs,
        })
        .await
    }

    async fn persist_image_from_proto(&self, image: &Image) -> Result<(), Error> {
        let existing = self.load_image_metadata(&image.id).unwrap_or_default();
        let reloadable = self.current_reloadable_config();
        let pinned = Self::image_is_pinned_by_patterns(
            &reloadable.pinned_image_patterns,
            image
                .repo_tags
                .iter()
                .map(String::as_str)
                .chain(existing.source_reference.as_deref()),
        );
        self.save_image_metadata(&CriusImage {
            id: image.id.clone(),
            repo_tags: image.repo_tags.clone(),
            repo_digests: image.repo_digests.clone(),
            size: image.size,
            pinned,
            pulled_at: existing.pulled_at,
            source_reference: existing.source_reference,
            os: existing.os,
            architecture: existing.architecture,
            config_user: existing.config_user,
            config_env: existing.config_env,
            config_entrypoint: existing.config_entrypoint,
            config_cmd: existing.config_cmd,
            config_working_dir: existing.config_working_dir,
            annotations: existing.annotations,
            declared_volumes: existing.declared_volumes,
            manifest_media_type: existing.manifest_media_type,
            selected_manifest_digest: existing.selected_manifest_digest,
            selected_platform: existing.selected_platform,
            stored_layers: existing.stored_layers,
            artifact_type: existing.artifact_type,
            artifact_blobs: existing.artifact_blobs,
        })
        .await
    }

    fn parse_bearer_challenge(header: &str) -> Option<(String, Option<String>)> {
        let raw = header.trim();
        if !raw.to_ascii_lowercase().starts_with("bearer ") {
            return None;
        }
        let fields = &raw[7..];
        let mut realm: Option<String> = None;
        let mut service: Option<String> = None;
        for part in fields.split(',') {
            let mut kv = part.trim().splitn(2, '=');
            let key = kv.next()?.trim();
            let value = kv.next()?.trim().trim_matches('"').to_string();
            match key {
                "realm" => realm = Some(value),
                "service" => service = Some(value),
                _ => {}
            }
        }
        realm.map(|r| (r, service))
    }

    async fn read_response_bytes_with_progress_timeout(
        &self,
        response: reqwest::Response,
        context: &str,
    ) -> Result<Vec<u8>, Status> {
        let timeout = self.pull_progress_timeout;
        if timeout.is_zero() {
            return response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| Status::internal(format!("{} failed: {}", context, e)));
        }

        let mut body = Vec::new();
        let mut response = response;
        loop {
            let next_chunk = tokio::time::timeout(timeout, response.chunk())
                .await
                .map_err(|_| {
                    Status::deadline_exceeded(format!(
                        "{} timed out after {:?} without progress",
                        context, timeout
                    ))
                })?;
            let Some(chunk) =
                next_chunk.map_err(|e| Status::internal(format!("{} failed: {}", context, e)))?
            else {
                break;
            };
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn download_layer_via_registry_api(
        &self,
        http: &reqwest::Client,
        auth: &RegistryAuth,
        token: Option<&str>,
        request: LayerDownloadRequest<'_>,
    ) -> Result<(usize, Vec<u8>, u64), Status> {
        let blob_url = Self::blob_url(
            &request.endpoint.base_url,
            request.reference,
            request.layer_digest,
        );
        info!("Downloading layer {} from {}", request.idx, blob_url);
        let mut blob_req = Self::apply_basic_auth(http.get(blob_url), auth);
        if let Some(t) = token {
            blob_req = blob_req.bearer_auth(t);
        }
        let blob_resp = blob_req
            .send()
            .await
            .map_err(|e| Status::internal(format!("blob request failed: {}", e)))?;
        if !blob_resp.status().is_success() {
            let status = blob_resp.status();
            let text = blob_resp.text().await.unwrap_or_default();
            return Err(Status::internal(format!(
                "blob request failed: {} {}",
                status, text
            )));
        }
        let len = blob_resp.content_length().unwrap_or(0);
        let layer = self
            .read_response_bytes_with_progress_timeout(blob_resp, "blob download")
            .await?;
        Ok((request.idx, layer, len))
    }

    fn manifest_url(base_url: &str, reference: &Reference) -> String {
        if let Some(digest) = reference.digest() {
            format!(
                "{}/v2/{}/manifests/{}",
                base_url.trim_end_matches('/'),
                reference.repository(),
                digest
            )
        } else {
            format!(
                "{}/v2/{}/manifests/{}",
                base_url.trim_end_matches('/'),
                reference.repository(),
                reference.tag().unwrap_or("latest")
            )
        }
    }

    fn blob_url(base_url: &str, reference: &Reference, digest: &str) -> String {
        format!(
            "{}/v2/{}/blobs/{}",
            base_url.trim_end_matches('/'),
            reference.repository(),
            digest
        )
    }

    fn apply_basic_auth(
        builder: reqwest::RequestBuilder,
        auth: &RegistryAuth,
    ) -> reqwest::RequestBuilder {
        match auth {
            RegistryAuth::Basic(username, password) => builder.basic_auth(username, Some(password)),
            RegistryAuth::Anonymous => builder,
        }
    }

    async fn request_bearer_token(
        http: &reqwest::Client,
        challenge: &str,
        reference: &Reference,
        auth: &RegistryAuth,
    ) -> Result<Option<String>, Status> {
        let (realm, service) = Self::parse_bearer_challenge(challenge)
            .ok_or_else(|| Status::internal("invalid bearer challenge"))?;
        let scope = format!("repository:{}:pull", reference.repository());
        info!("Requesting bearer token, scope={}", scope);
        let mut token_req = http.get(&realm).query(&[("scope", scope.as_str())]);
        if let Some(s) = service.as_deref() {
            token_req = token_req.query(&[("service", s)]);
        }
        token_req = Self::apply_basic_auth(token_req, auth);
        let token_resp = token_req
            .send()
            .await
            .map_err(|e| Status::internal(format!("token request failed: {}", e)))?;
        if !token_resp.status().is_success() {
            let status = token_resp.status();
            let text = token_resp.text().await.unwrap_or_default();
            return Err(Status::internal(format!(
                "token request failed: {} {}",
                status, text
            )));
        }
        let token_json: serde_json::Value = token_resp
            .json()
            .await
            .map_err(|e| Status::internal(format!("invalid token response: {}", e)))?;
        Ok(token_json
            .get("token")
            .or_else(|| token_json.get("access_token"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()))
    }

    async fn pull_via_registry_api(
        &self,
        reference: &Reference,
        auth: &RegistryAuth,
        initial_bearer_token: Option<&str>,
    ) -> Result<(String, u64, Vec<PulledLayerData>, PulledImageMetadata), Status> {
        let mut last_error = None;
        for endpoint in self.registry_endpoints_for(reference, true)? {
            match self
                .pull_via_registry_api_with_endpoint(
                    &endpoint,
                    reference,
                    auth,
                    initial_bearer_token,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(err) => {
                    warn!(
                        "Registry endpoint {} failed for {}: {}",
                        endpoint.base_url,
                        reference,
                        err.message()
                    );
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            Status::internal(format!("no usable registry endpoints for {}", reference))
        }))
    }

    async fn pull_via_registry_api_with_endpoint(
        &self,
        endpoint: &RegistryEndpoint,
        reference: &Reference,
        auth: &RegistryAuth,
        initial_bearer_token: Option<&str>,
    ) -> Result<(String, u64, Vec<PulledLayerData>, PulledImageMetadata), Status> {
        info!(
            "Using registry API pull flow for {} via {}",
            reference, endpoint.base_url
        );
        let mut http_builder = reqwest::Client::builder();
        if !self.pull_progress_timeout.is_zero() {
            http_builder = http_builder.timeout(self.pull_progress_timeout);
        }
        if endpoint.skip_verify {
            http_builder = http_builder.danger_accept_invalid_certs(true);
        }
        let http = http_builder
            .build()
            .map_err(|e| Status::internal(format!("failed to build registry client: {}", e)))?;
        let ping_url = format!("{}/v2/", endpoint.base_url.trim_end_matches('/'));
        info!("Registry ping: {}", ping_url);
        let ping = Self::apply_basic_auth(http.get(&ping_url), auth)
            .send()
            .await
            .map_err(|e| Status::internal(format!("registry ping failed: {}", e)))?;

        let mut token: Option<String> = initial_bearer_token.map(str::to_string);
        if ping.status() == reqwest::StatusCode::UNAUTHORIZED && token.is_none() {
            let challenge = ping
                .headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| Status::internal("missing WWW-Authenticate header"))?;
            token = Self::request_bearer_token(&http, challenge, reference, auth).await?;
        }

        let manifest_url = Self::manifest_url(&endpoint.base_url, reference);
        info!("Fetching manifest: {}", manifest_url);
        let mut manifest_req = Self::apply_basic_auth(http.get(&manifest_url), auth).header(
            reqwest::header::ACCEPT,
            "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json",
        );
        if let Some(t) = token.as_deref() {
            manifest_req = manifest_req.bearer_auth(t);
        }
        let mut manifest_resp = manifest_req
            .send()
            .await
            .map_err(|e| Status::internal(format!("manifest request failed: {}", e)))?;
        if manifest_resp.status() == reqwest::StatusCode::UNAUTHORIZED && token.is_none() {
            let challenge = manifest_resp
                .headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| Status::internal("missing WWW-Authenticate header"))?;
            token = Self::request_bearer_token(&http, challenge, reference, auth).await?;
            let mut retry_manifest_req = Self::apply_basic_auth(http.get(&manifest_url), auth).header(
                reqwest::header::ACCEPT,
                "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json",
            );
            if let Some(t) = token.as_deref() {
                retry_manifest_req = retry_manifest_req.bearer_auth(t);
            }
            manifest_resp = retry_manifest_req
                .send()
                .await
                .map_err(|e| Status::internal(format!("manifest request failed: {}", e)))?;
        }
        if !manifest_resp.status().is_success() {
            let status = manifest_resp.status();
            let text = manifest_resp.text().await.unwrap_or_default();
            return Err(Status::internal(format!(
                "manifest request failed: {} {}",
                status, text
            )));
        }
        let digest = manifest_resp
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        let manifest_bytes = self
            .read_response_bytes_with_progress_timeout(manifest_resp, "read manifest")
            .await?;
        let mut manifest_json: serde_json::Value = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| Status::internal(format!("parse manifest failed: {}", e)))?;
        let mut metadata = PulledImageMetadata {
            manifest_media_type: manifest_json
                .get("mediaType")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string()),
            artifact_type: manifest_json
                .get("artifactType")
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string()),
            ..Default::default()
        };
        let mut effective_digest = digest;

        if manifest_json
            .get("layers")
            .and_then(|v| v.as_array())
            .is_none()
        {
            let target_arch = match std::env::consts::ARCH {
                "x86_64" => "amd64",
                "aarch64" => "arm64",
                "loongarch64" => "loong64",
                other => other,
            };
            let target_os = std::env::consts::OS;
            let manifests = manifest_json
                .get("manifests")
                .and_then(|v| v.as_array())
                .ok_or_else(|| Status::internal("manifest index missing manifests"))?;
            let selected = manifests.iter().find(|m| {
                let os = m
                    .get("platform")
                    .and_then(|p| p.get("os"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let arch = m
                    .get("platform")
                    .and_then(|p| p.get("architecture"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                os == target_os && arch == target_arch
            });
            let selected_digest = if let Some(entry) = selected {
                entry
                    .get("digest")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Status::internal("selected manifest missing digest"))?
            } else {
                manifests
                    .first()
                    .and_then(|m| m.get("digest"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Status::internal("manifest index has no usable digest"))?
            };
            info!(
                "Manifest index detected, selected child manifest digest={} (platform={}/{})",
                selected_digest, target_os, target_arch
            );
            metadata.selected_manifest_digest = Some(selected_digest.to_string());
            metadata.selected_platform = Some(format!("{target_os}/{target_arch}"));

            let child_url = format!(
                "{}/v2/{}/manifests/{}",
                endpoint.base_url.trim_end_matches('/'),
                reference.repository(),
                selected_digest
            );
            let mut child_req = Self::apply_basic_auth(http.get(child_url), auth).header(
                reqwest::header::ACCEPT,
                "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json",
            );
            if let Some(t) = token.as_deref() {
                child_req = child_req.bearer_auth(t);
            }
            let child_resp = child_req
                .send()
                .await
                .map_err(|e| Status::internal(format!("child manifest request failed: {}", e)))?;
            if !child_resp.status().is_success() {
                let status = child_resp.status();
                let text = child_resp.text().await.unwrap_or_default();
                return Err(Status::internal(format!(
                    "child manifest request failed: {} {}",
                    status, text
                )));
            }
            effective_digest = child_resp
                .headers()
                .get("Docker-Content-Digest")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
                .or_else(|| Some(selected_digest.to_string()));
            let child_bytes = self
                .read_response_bytes_with_progress_timeout(child_resp, "read child manifest")
                .await?;
            manifest_json = serde_json::from_slice(&child_bytes)
                .map_err(|e| Status::internal(format!("parse child manifest failed: {}", e)))?;
            metadata.manifest_media_type = manifest_json
                .get("mediaType")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string());
        }

        if metadata.artifact_type.is_none() {
            if let Some(config_digest) = manifest_json
                .get("config")
                .and_then(|config| config.get("digest"))
                .and_then(|value| value.as_str())
            {
                let config_url =
                    Self::blob_url(&endpoint.base_url, reference, config_digest).to_string();
                let mut config_req = Self::apply_basic_auth(http.get(config_url), auth);
                if let Some(t) = token.as_deref() {
                    config_req = config_req.bearer_auth(t);
                }
                let config_resp = config_req
                    .send()
                    .await
                    .map_err(|e| Status::internal(format!("config request failed: {}", e)))?;
                if !config_resp.status().is_success() {
                    let status = config_resp.status();
                    let text = config_resp.text().await.unwrap_or_default();
                    return Err(Status::internal(format!(
                        "config request failed: {} {}",
                        status, text
                    )));
                }
                let config_bytes = self
                    .read_response_bytes_with_progress_timeout(config_resp, "read image config")
                    .await?;
                let config_json: serde_json::Value = serde_json::from_slice(&config_bytes)
                    .map_err(|e| Status::internal(format!("parse config failed: {}", e)))?;
                metadata.os = config_json
                    .get("os")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                metadata.architecture = config_json
                    .get("architecture")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                metadata.config_user = config_json
                    .get("config")
                    .and_then(|config| config.get("User"))
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string());
                metadata.config_env = config_json
                    .get("config")
                    .and_then(|config| config.get("Env"))
                    .and_then(|value| serde_json::from_value::<Vec<String>>(value.clone()).ok())
                    .unwrap_or_default();
                metadata.config_entrypoint = config_json
                    .get("config")
                    .and_then(|config| config.get("Entrypoint"))
                    .and_then(|value| serde_json::from_value::<Vec<String>>(value.clone()).ok())
                    .unwrap_or_default();
                metadata.config_cmd = config_json
                    .get("config")
                    .and_then(|config| config.get("Cmd"))
                    .and_then(|value| serde_json::from_value::<Vec<String>>(value.clone()).ok())
                    .unwrap_or_default();
                metadata.config_working_dir = config_json
                    .get("config")
                    .and_then(|config| config.get("WorkingDir"))
                    .and_then(|value| value.as_str())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string());
                metadata.annotations = config_json
                    .get("config")
                    .and_then(|config| config.get("Labels"))
                    .and_then(|value| {
                        serde_json::from_value::<HashMap<String, String>>(value.clone()).ok()
                    })
                    .unwrap_or_default();
                metadata.declared_volumes = config_json
                    .get("config")
                    .and_then(|config| config.get("Volumes"))
                    .and_then(|value| value.as_object())
                    .map(|volumes| {
                        let mut declared = volumes.keys().cloned().collect::<Vec<_>>();
                        declared.sort();
                        declared
                    })
                    .unwrap_or_default();
            }
        }

        let layers = manifest_json
            .get("layers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Status::internal("manifest missing layers"))?;

        info!("Start downloading {} layers", layers.len());
        let layer_jobs: Vec<(usize, String, String)> = layers
            .iter()
            .enumerate()
            .map(|(idx, layer)| {
                let annotations = layer
                    .get("annotations")
                    .and_then(|value| {
                        serde_json::from_value::<HashMap<String, String>>(value.clone()).ok()
                    })
                    .unwrap_or_default();
                let path = annotations
                    .get("org.opencontainers.image.title")
                    .cloned()
                    .or_else(|| {
                        annotations
                            .get("org.opencontainers.image.filepath")
                            .cloned()
                    })
                    .unwrap_or_else(|| format!("blob-{idx}"));
                metadata.artifact_blobs.push(ArtifactBlobMeta {
                    digest: layer
                        .get("digest")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    media_type: layer
                        .get("mediaType")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    path,
                    size: layer
                        .get("size")
                        .and_then(|value| value.as_u64())
                        .unwrap_or_default(),
                    annotations,
                });
                let media_type = layer
                    .get("mediaType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("application/vnd.oci.image.layer.v1.tar+gzip");
                layer
                    .get("digest")
                    .and_then(|v| v.as_str())
                    .map(|digest| (idx, digest.to_string(), media_type.to_string()))
                    .ok_or_else(|| Status::internal("layer missing digest"))
            })
            .collect::<Result<_, _>>()?;
        let downloaded_results =
            Self::collect_with_concurrency_limit(self.max_concurrent_downloads, layer_jobs, {
                let http = http.clone();
                let auth = auth.clone();
                let reference = reference.clone();
                let token = token.clone();
                let endpoint = endpoint.clone();
                move |(idx, digest, media_type): (usize, String, String)| {
                    let http = http.clone();
                    let auth = auth.clone();
                    let reference = reference.clone();
                    let token = token.clone();
                    let endpoint = endpoint.clone();
                    async move {
                        let (idx, bytes, len) = self
                            .download_layer_via_registry_api(
                                &http,
                                &auth,
                                token.as_deref(),
                                LayerDownloadRequest {
                                    endpoint: &endpoint,
                                    reference: &reference,
                                    layer_digest: &digest,
                                    idx,
                                },
                            )
                            .await?;
                        Ok::<_, Status>((idx, bytes, len, media_type))
                    }
                }
            })
            .await?;
        let total_size: u64 = downloaded_results
            .iter()
            .map(|(_, bytes, len, _)| (*len).max(bytes.len() as u64))
            .sum();
        let mut downloaded_layers = downloaded_results;
        downloaded_layers.sort_by_key(|(idx, _, _, _)| *idx);
        let mut layer_data = Vec::with_capacity(downloaded_layers.len());
        let mut stored_layers = Vec::with_capacity(downloaded_layers.len());
        for (idx, bytes, _len, source_media_type) in downloaded_layers {
            let encrypted = source_media_type.ends_with("+encrypted");
            let (bytes, media_type) = if encrypted {
                if !self.image_decryption_enabled() {
                    return Err(Status::failed_precondition(format!(
                        "encrypted image layer requires image.decryption_keys_path and a compatible decoder; source media type {}",
                        source_media_type
                    )));
                }
                self.decrypt_layer_bytes(&source_media_type, &bytes)?
            } else {
                (bytes, source_media_type.clone())
            };
            let extension = Self::plain_media_type_to_extension(&media_type);
            stored_layers.push(StoredLayerMeta {
                digest: String::new(),
                path: format!("{idx}.{extension}"),
                media_type: media_type.clone(),
                source_media_type: source_media_type.clone(),
                encrypted,
            });
            layer_data.push(PulledLayerData {
                bytes,
                media_type,
                source_media_type,
                encrypted,
            });
        }
        metadata.stored_layers = stored_layers;

        let image_id = Self::canonical_image_id(
            effective_digest.as_deref().unwrap_or_default(),
            &manifest_bytes,
        );

        Ok((image_id, total_size, layer_data, metadata))
    }
}

impl ImageMetricsProvider {
    pub async fn snapshot(&self) -> crate::metrics::ImageMetricsSnapshot {
        let grouped = {
            let images = self.images.lock().await;
            let mut grouped: HashMap<String, Image> = HashMap::new();
            for image in images.values() {
                grouped
                    .entry(image.id.clone())
                    .or_insert_with(|| image.clone());
            }
            grouped
        };
        let total_image_size_bytes = grouped.values().map(|image| image.size).sum();
        let (metadata_bytes, metadata_inodes) = self.metadata_store.usage().unwrap_or((0, 0));
        let (content_bytes, content_inodes) = self.content_store.total_usage().unwrap_or((0, 0));
        crate::metrics::ImageMetricsSnapshot {
            image_count: grouped.len(),
            total_image_size_bytes,
            image_fs_bytes_used: metadata_bytes.saturating_add(content_bytes),
            image_fs_inodes_used: metadata_inodes.saturating_add(content_inodes),
        }
    }
}

#[tonic::async_trait]
impl ImageService for ImageServiceImpl {
    // 列出镜像
    async fn list_images(
        &self,
        request: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let req = request.into_inner();
        let requested_ref = req
            .filter
            .and_then(|filter| filter.image)
            .map(|image| image.image)
            .filter(|image| !image.is_empty());
        let images: Vec<Image> = {
            let images = self.images.lock().await;
            info!("Number of images in memory: {}", images.len());
            for (key, image) in images.iter() {
                info!("Image: {} -> {}", key, image.id);
            }
            images.values().cloned().collect()
        };
        let mut grouped: HashMap<String, Vec<Image>> = HashMap::new();
        for image in images {
            grouped.entry(image.id.clone()).or_default().push(image);
        }

        let mut images_list = Vec::new();
        for (image_id, group) in grouped {
            let meta = self.load_image_metadata(&image_id);
            let Some(image) = Self::aggregate_image_records(group.iter(), meta.as_ref()) else {
                continue;
            };
            let matched = requested_ref
                .as_ref()
                .map(|requested_ref| Self::image_matches_ref(&image, requested_ref))
                .unwrap_or(true);
            if matched {
                images_list.push(image);
            }
        }
        images_list.sort_by(|left, right| left.id.cmp(&right.id));

        Ok(Response::new(ListImagesResponse {
            images: images_list,
        }))
    }

    // 获取镜像状态
    async fn image_status(
        &self,
        request: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        let req = request.into_inner();
        let image_spec = req
            .image
            .ok_or_else(|| Status::invalid_argument("Image not specified"))?;
        let requested_ref = image_spec.image;
        let images: Vec<Image> = {
            let images = self.images.lock().await;
            images.values().cloned().collect()
        };

        if let Some(matched_image) = images
            .iter()
            .find(|image| Self::image_matches_ref(image, &requested_ref))
        {
            let meta = self.load_image_metadata(&matched_image.id);
            if let Some(mut image) = Self::aggregate_image_records(
                images
                    .iter()
                    .filter(|candidate| candidate.id == matched_image.id),
                meta.as_ref(),
            ) {
                let annotations = image
                    .spec
                    .as_ref()
                    .map(|spec| spec.annotations.clone())
                    .unwrap_or_default();
                image.spec = Some(ImageSpec {
                    image: requested_ref.clone(),
                    user_specified_image: requested_ref.clone(),
                    annotations,
                    ..Default::default()
                });

                return Ok(Response::new(ImageStatusResponse {
                    image: Some(image.clone()),
                    info: if req.verbose {
                        Self::build_image_verbose_info(
                            &image,
                            &self.metadata_store,
                            &self.content_store,
                            &self.storage_driver,
                            &self.storage_options,
                            &self.parsed_storage_options,
                        )?
                    } else {
                        HashMap::new()
                    },
                }));
            }
        }

        Ok(Response::new(ImageStatusResponse {
            image: None,
            info: HashMap::new(),
        }))
    }

    // 拉取镜像
    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let req = request.into_inner();
        let image_spec = req
            .image
            .ok_or_else(|| Status::invalid_argument("Image spec not specified"))?;
        let requested_ref = image_spec.image.clone();
        let canonical_ref = self.resolve_pull_reference(&requested_ref)?;

        // 解析镜像引用
        let reference: Reference = canonical_ref
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid image reference: {}", e)))?;
        let supplied_bearer_token = req
            .auth
            .as_ref()
            .and_then(Self::provided_registry_bearer_token);
        let pull_namespace = req
            .sandbox_config
            .as_ref()
            .and_then(|config| config.metadata.as_ref())
            .map(|metadata| metadata.namespace.clone());
        let pod_cgroup_parent = req
            .sandbox_config
            .as_ref()
            .and_then(|config| config.linux.as_ref())
            .map(|linux| linux.cgroup_parent.as_str());
        let pull_cgroup_target = self
            .pull_cgroup
            .target_for_pod(pod_cgroup_parent)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        self.enforce_signature_policy(&reference, pull_namespace.as_deref())?;

        let auth = match req.auth.clone() {
            Some(auth) => Self::registry_auth_from_auth_config(auth)?,
            None => {
                if let Some(auth) = self
                    .registry_auth_from_namespaced_auth_dir(&reference, pull_namespace.as_deref())?
                {
                    auth
                } else if let Some(auth) = self.registry_auth_from_global_auth_file(&reference)? {
                    auth
                } else {
                    RegistryAuth::Anonymous
                }
            }
        };
        let pull_key = canonical_ref.clone();

        loop {
            let wait_for_existing = {
                let mut in_progress = self.in_progress_pulls.lock().await;
                if let Some(notify) = in_progress.get(&pull_key) {
                    Some(notify.clone())
                } else {
                    in_progress.insert(pull_key.clone(), Arc::new(Notify::new()));
                    None
                }
            };

            if let Some(notify) = wait_for_existing {
                notify.notified().await;
                if let Some(existing_image) = self.find_local_image(&pull_key).await {
                    return Ok(Response::new(PullImageResponse {
                        image_ref: existing_image.id,
                    }));
                }
                continue;
            }

            break;
        }
        info!("Pulling image: {}", canonical_ref);
        info!("Checking whether image exists locally: {}", canonical_ref);
        if let Some(existing_image) = self.find_local_image(&canonical_ref).await {
            self.persist_local_image_alias(&existing_image, &requested_ref, &canonical_ref)
                .await
                .map_err(|e| Status::internal(format!("Failed to persist image alias: {}", e)))?;
            if let Some(notify) = self.in_progress_pulls.lock().await.remove(&pull_key) {
                notify.notify_waiters();
            }
            info!(
                "Image already exists locally: {} -> {}",
                canonical_ref, existing_image.id
            );
            return Ok(Response::new(PullImageResponse {
                image_ref: existing_image.id,
            }));
        }
        info!(
            "Local image not found, start remote pull: {}",
            canonical_ref
        );
        let transfer = self.transfer_tracker.start(
            canonical_ref.clone(),
            self.transfer_provider_kind(),
            "resolving",
        );
        let transfer_id = transfer.id().to_string();
        self.persist_content_transfer_by_id(&transfer_id)
            .map_err(|err| Status::internal(err.to_string()))?;
        self.publish_image_internal_event(
            &canonical_ref,
            "image.pull_start",
            crate::services::InternalEventSeverity::Info,
            serde_json::json!({
                "requestedRef": requested_ref,
                "canonicalRef": canonical_ref,
                "transferId": transfer_id,
                "provider": self.transfer_provider_kind().as_str(),
            }),
        );
        transfer.update("pulling", 0, 0);
        self.persist_content_transfer_by_id(&transfer_id)
            .map_err(|err| Status::internal(err.to_string()))?;
        self.publish_image_internal_event(
            &canonical_ref,
            "image.pull_progress",
            crate::services::InternalEventSeverity::Info,
            serde_json::json!({
                "transferId": transfer_id,
                "stage": "pulling",
                "bytesCompleted": 0,
                "bytesTotal": 0,
            }),
        );

        let pull_outcome = Self::execute_pull_with_retries(self.pull_retry_count, || async {
            transfer.update("attempt", 0, 0);
            self.persist_content_transfer_by_id(&transfer_id)
                .map_err(|err| Status::internal(err.to_string()))?;
            let _pull_cgroup_scope = self
                .pull_cgroup
                .enter(&pull_cgroup_target)
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
            #[cfg(test)]
            if let Some(handler) = self.snapshot_test_pull_handler() {
                return self
                    .pull_via_test_handler(
                        handler,
                        &requested_ref,
                        &canonical_ref,
                        &auth,
                        pull_namespace.as_deref(),
                    )
                    .await;
            }

            let reference = reference.clone();
            let (image_id, image_size, layers_to_persist, pulled_metadata) = self
                .pull_via_registry_api(&reference, &auth, supplied_bearer_token.as_deref())
                .await?;
            self.persist_pulled_image(PersistedPullImage {
                requested_ref: requested_ref.clone(),
                canonical_ref: canonical_ref.clone(),
                reference,
                image_id,
                image_size,
                layers_to_persist,
                pulled_metadata,
            })
            .await
        })
        .await;

        if let Some(notify) = self.in_progress_pulls.lock().await.remove(&pull_key) {
            notify.notify_waiters();
        }

        match pull_outcome {
            Ok(response) => {
                transfer.succeed();
                self.persist_content_transfer_by_id(&transfer_id)
                    .map_err(|err| Status::internal(err.to_string()))?;
                self.publish_image_internal_event(
                    &canonical_ref,
                    "image.pull_success",
                    crate::services::InternalEventSeverity::Info,
                    serde_json::json!({
                        "transferId": transfer_id,
                        "imageRef": response.get_ref().image_ref,
                    }),
                );
                Ok(response)
            }
            Err(status) => {
                transfer.fail(status.message().to_string());
                self.persist_content_transfer_by_id(&transfer_id)
                    .map_err(|err| Status::internal(err.to_string()))?;
                self.publish_image_internal_event(
                    &canonical_ref,
                    "image.pull_fail",
                    crate::services::InternalEventSeverity::Error,
                    serde_json::json!({
                        "transferId": transfer_id,
                        "code": format!("{:?}", status.code()),
                        "message": status.message(),
                    }),
                );
                Err(status)
            }
        }
    }

    // 删除镜像
    async fn remove_image(
        &self,
        request: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        let req = request.into_inner();

        match req.image {
            Some(image_spec) => {
                let requested_ref = image_spec.image;
                for root in self.additional_artifact_store_roots() {
                    if Self::artifact_mount_candidates(root, &requested_ref)
                        .ok()
                        .flatten()
                        .is_some()
                    {
                        return Err(Status::failed_precondition(format!(
                            "image {} is provided by a read-only additional OCI artifact store",
                            requested_ref
                        )));
                    }
                }

                let exact_tag_candidate = {
                    let images = self.images.lock().await;
                    images
                        .values()
                        .find(|image| image.repo_tags.iter().any(|tag| tag == &requested_ref))
                        .cloned()
                        .map(|image| {
                            let sibling_tags = images
                                .values()
                                .filter(|candidate| candidate.id == image.id)
                                .flat_map(|candidate| candidate.repo_tags.iter().cloned())
                                .collect::<HashSet<_>>();
                            (image, sibling_tags)
                        })
                };

                if let Some((mut image, sibling_tags)) = exact_tag_candidate {
                    let requested_is_id_or_digest =
                        Self::image_id_matches(&image.id, &requested_ref)
                            || image
                                .repo_digests
                                .iter()
                                .any(|digest| digest == &requested_ref);
                    if !requested_is_id_or_digest && sibling_tags.len() > 1 {
                        let remaining_tags = sibling_tags
                            .into_iter()
                            .filter(|tag| tag != &requested_ref)
                            .collect::<Vec<_>>();
                        image.repo_tags = remaining_tags.clone();
                        image.spec = remaining_tags.first().cloned().map(|tag| ImageSpec {
                            image: tag.clone(),
                            user_specified_image: tag,
                            ..Default::default()
                        });

                        let mut images = self.images.lock().await;
                        images.remove(&requested_ref);
                        for candidate in images.values_mut() {
                            if candidate.id == image.id {
                                candidate.repo_tags = remaining_tags.clone();
                                candidate.spec = image.spec.clone();
                            }
                        }
                        drop(images);

                        self.persist_image_from_proto(&image).await.map_err(|e| {
                            Status::internal(format!(
                                "Failed to persist image metadata after untag: {}",
                                e
                            ))
                        })?;
                        return Ok(Response::new(RemoveImageResponse {}));
                    }
                }

                let (candidate_ids, candidate_refs): (HashSet<String>, HashSet<String>) = {
                    let images = self.images.lock().await;
                    if let Some(image) = images.get(&requested_ref) {
                        let mut ids = HashSet::new();
                        ids.insert(image.id.clone());
                        let mut refs = HashSet::new();
                        refs.insert(requested_ref.clone());
                        refs.extend(image.repo_tags.iter().cloned());
                        (ids, refs)
                    } else {
                        let matched_images: Vec<&Image> = images
                            .values()
                            .filter(|image| Self::image_matches_ref(image, &requested_ref))
                            .collect();
                        let ids = matched_images
                            .iter()
                            .map(|image| image.id.clone())
                            .collect::<HashSet<_>>();
                        let refs = matched_images
                            .iter()
                            .flat_map(|image| image.repo_tags.iter().cloned())
                            .chain(std::iter::once(requested_ref.clone()))
                            .collect::<HashSet<_>>();
                        (ids, refs)
                    }
                };

                if candidate_ids.is_empty() {
                    Ok(Response::new(RemoveImageResponse {}))
                } else {
                    self.image_is_in_use(&requested_ref, &candidate_ids, &candidate_refs)?;

                    let image_ids_to_remove: Vec<String> = {
                        let mut images = self.images.lock().await;
                        images.retain(|key, image| {
                            !(candidate_ids.contains(&image.id)
                                || candidate_refs.contains(key)
                                || image
                                    .repo_tags
                                    .iter()
                                    .any(|tag| candidate_refs.contains(tag)))
                        });
                        candidate_ids.iter().cloned().collect()
                    };

                    for image_id in image_ids_to_remove {
                        let is_artifact = self
                            .load_image_metadata(&image_id)
                            .as_ref()
                            .map(Self::is_artifact_meta)
                            .unwrap_or(false);
                        if let Err(err) = self.metadata_store.delete_by_id(&image_id, is_artifact) {
                            error!("Failed to delete image metadata for {}: {}", image_id, err);
                        }
                    }

                    Ok(Response::new(RemoveImageResponse {}))
                }
            }
            None => {
                // 如果没有指定镜像，返回成功而不是错误
                Ok(Response::new(RemoveImageResponse {}))
            }
        }
    }

    // 获取镜像文件信息
    async fn image_fs_info(
        &self,
        _request: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        let (metadata_bytes, metadata_inodes) = self
            .metadata_store
            .usage()
            .map_err(|e| Status::internal(format!("Failed to collect metadata usage: {}", e)))?;
        let (content_bytes, content_inodes) = self
            .content_store
            .total_usage()
            .map_err(|e| Status::internal(format!("Failed to collect content usage: {}", e)))?;
        let used_bytes = metadata_bytes.saturating_add(content_bytes);
        let inodes_used = metadata_inodes.saturating_add(content_inodes);

        let usage = FilesystemUsage {
            timestamp: Self::now_nanos(),
            fs_id: Some(FilesystemIdentifier {
                mountpoint: self.storage_path.display().to_string(),
            }),
            used_bytes: Some(UInt64Value { value: used_bytes }),
            inodes_used: Some(UInt64Value { value: inodes_used }),
        };

        Ok(Response::new(ImageFsInfoResponse {
            image_filesystems: vec![usage],
            container_filesystems: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests;
