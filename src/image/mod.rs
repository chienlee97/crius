use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use base64::Engine;
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
use crate::storage::StorageManager;

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
    pub annotations: HashMap<String, String>,
    pub manifest_media_type: Option<String>,
}

/// 镜像服务实现
pub struct ImageServiceImpl {
    // 存储镜像信息的线程安全HashMap
    images: std::sync::Arc<tokio::sync::Mutex<HashMap<String, Image>>>,
    storage_path: PathBuf,
    oci_client: Arc<Mutex<oci_distribution::Client>>,
    in_progress_pulls: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
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
    pub annotations: HashMap<String, String>,
    pub manifest_media_type: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct PulledImageMetadata {
    os: Option<String>,
    architecture: Option<String>,
    config_user: Option<String>,
    annotations: HashMap<String, String>,
    manifest_media_type: Option<String>,
}

impl ImageServiceImpl {
    fn has_registry_component(component: &str) -> bool {
        component.contains('.') || component.contains(':') || component == "localhost"
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
        storage_path: &Path,
    ) -> Result<HashMap<String, String>, Status> {
        let image_dir = storage_path.join("images").join(&image.id);
        let meta: Option<ImageMeta> = std::fs::read(image_dir.join("metadata.json"))
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok());
        let layer_files = std::fs::read_dir(&image_dir)
            .ok()
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .filter_map(|entry| {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let payload = serde_json::json!({
            "id": image.id,
            "repoTags": image.repo_tags,
            "repoDigests": image.repo_digests,
            "size": image.size,
            "pinned": image.pinned,
            "pulledAt": meta.as_ref().map(|meta| meta.pulled_at),
            "sourceReference": meta.as_ref().and_then(|meta| meta.source_reference.clone()),
            "os": meta.as_ref().and_then(|meta| meta.os.clone()),
            "architecture": meta.as_ref().and_then(|meta| meta.architecture.clone()),
            "configUser": meta.as_ref().and_then(|meta| meta.config_user.clone()),
            "annotations": meta
                .as_ref()
                .map(|meta| meta.annotations.clone())
                .unwrap_or_default(),
            "manifestMediaType": meta
                .as_ref()
                .and_then(|meta| meta.manifest_media_type.clone()),
            "storagePath": image_dir.display().to_string(),
            "layers": layer_files,
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

    fn collect_path_usage(path: &Path) -> io::Result<(u64, u64)> {
        if !path.exists() {
            return Ok((0, 0));
        }

        let mut used_bytes = 0u64;
        let mut inodes_used = 0u64;

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            inodes_used += 1;
            if metadata.is_dir() {
                let (child_bytes, child_inodes) = Self::collect_path_usage(&entry.path())?;
                used_bytes = used_bytes.saturating_add(child_bytes);
                inodes_used = inodes_used.saturating_add(child_inodes);
            } else {
                used_bytes = used_bytes.saturating_add(metadata.len());
            }
        }

        Ok((used_bytes, inodes_used))
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
        }

        Ok(())
    }

    pub fn new(storage_path: impl AsRef<Path>) -> Result<Self, Error> {
        let storage_path = storage_path.as_ref().to_path_buf();

        if !storage_path.exists() {
            std::fs::create_dir_all(&storage_path).context("Failed to create storage directory")?;
        }

        let client_config = oci_distribution::client::ClientConfig {
            protocol: oci_distribution::client::ClientProtocol::Https,
            ..Default::default()
        };
        let oci_client = oci_distribution::Client::new(client_config);
        let images = std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        Ok(Self {
            images,
            storage_path,
            oci_client: Arc::new(Mutex::new(oci_client)),
            in_progress_pulls: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // 加载本地镜像
    pub async fn load_local_images(&self) -> Result<(), Error> {
        info!("load_local_images called");
        let imaages_dir = self.storage_path.join("images");
        info!("Images directory: {:?}", imaages_dir);

        if !imaages_dir.exists() {
            info!("Images directory does not exist, creating...");
            std::fs::create_dir_all(&imaages_dir)?;
            return Ok(());
        }

        let mut images = self.images.lock().await;
        info!("Reading images from directory");
        for entry in std::fs::read_dir(imaages_dir).context("Failed to read images directory")? {
            let entry = entry.context("Failed to read entry")?;

            let meta_path = entry.path().join("metadata.json");
            if !meta_path.exists() {
                continue;
            }

            let meta_data = std::fs::read(meta_path).context("Failed to read metadata")?;
            let meta: ImageMeta =
                serde_json::from_slice(&meta_data).context("Failed to parse metadata")?;

            let path = entry.path();
            if path.is_dir() {
                let image = Self::image_from_meta(&meta);
                for tag in &meta.repo_tags {
                    images.insert(tag.clone(), image.clone());
                }
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

        let images_dir = self.storage_path.join("images");
        if !images_dir.exists() {
            return None;
        }

        let entries = match std::fs::read_dir(&images_dir) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to read images directory {:?}: {}", images_dir, e);
                return None;
            }
        };

        for entry in entries.flatten() {
            let meta_path = entry.path().join("metadata.json");
            if !meta_path.exists() {
                continue;
            }
            let meta_data = match std::fs::read(&meta_path) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to read image metadata {:?}: {}", meta_path, e);
                    continue;
                }
            };
            let meta: ImageMeta = match serde_json::from_slice(&meta_data) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to parse image metadata {:?}: {}", meta_path, e);
                    continue;
                }
            };
            let image = Self::image_from_meta(&meta);
            if Self::image_matches_ref(&image, image_ref) {
                let mut images = self.images.lock().await;
                for tag in &meta.repo_tags {
                    images.insert(tag.clone(), image.clone());
                }
                return Some(image);
            }
        }

        None
    }

    // 保存镜像元数据
    async fn save_image_metadata(&self, image: &CriusImage) -> Result<(), Error> {
        let meta_path = self
            .storage_path
            .join("images")
            .join(&image.id)
            .join("metadata.json");
        if !meta_path.exists() {
            std::fs::create_dir_all(meta_path.parent().unwrap())
                .context("Failed to create metadata directory")?;
        }
        let meta_data = serde_json::to_vec(image).context("Failed to serialize metadata")?;
        std::fs::write(meta_path, meta_data).context("Failed to write metadata")?;
        Ok(())
    }

    fn load_image_metadata(&self, image_id: &str) -> Option<ImageMeta> {
        let meta_path = self
            .storage_path
            .join("images")
            .join(image_id)
            .join("metadata.json");
        std::fs::read(&meta_path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<ImageMeta>(&raw).ok())
    }

    async fn persist_image_from_proto(&self, image: &Image) -> Result<(), Error> {
        let existing = self.load_image_metadata(&image.id).unwrap_or_default();
        self.save_image_metadata(&CriusImage {
            id: image.id.clone(),
            repo_tags: image.repo_tags.clone(),
            repo_digests: image.repo_digests.clone(),
            size: image.size,
            pinned: image.pinned,
            pulled_at: existing.pulled_at,
            source_reference: existing.source_reference,
            os: existing.os,
            architecture: existing.architecture,
            config_user: existing.config_user,
            annotations: existing.annotations,
            manifest_media_type: existing.manifest_media_type,
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

    fn manifest_url(reference: &Reference) -> String {
        if let Some(digest) = reference.digest() {
            format!(
                "https://{}/v2/{}/manifests/{}",
                reference.resolve_registry(),
                reference.repository(),
                digest
            )
        } else {
            format!(
                "https://{}/v2/{}/manifests/{}",
                reference.resolve_registry(),
                reference.repository(),
                reference.tag().unwrap_or("latest")
            )
        }
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
    ) -> Result<(String, u64, Vec<Vec<u8>>, PulledImageMetadata), Status> {
        info!("Using registry API pull flow for {}", reference);
        let http = reqwest::Client::new();
        let ping_url = format!("https://{}/v2/", reference.resolve_registry());
        info!("Registry ping: {}", ping_url);
        let ping = Self::apply_basic_auth(http.get(&ping_url), auth)
            .send()
            .await
            .map_err(|e| Status::internal(format!("registry ping failed: {}", e)))?;

        let mut token: Option<String> = initial_bearer_token.map(str::to_string);
        if ping.status() == reqwest::StatusCode::UNAUTHORIZED {
            if token.is_none() {
                let challenge = ping
                    .headers()
                    .get(reqwest::header::WWW_AUTHENTICATE)
                    .and_then(|h| h.to_str().ok())
                    .ok_or_else(|| Status::internal("missing WWW-Authenticate header"))?;
                token = Self::request_bearer_token(&http, challenge, reference, auth).await?;
            }
        }

        let manifest_url = Self::manifest_url(reference);
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
        let manifest_bytes = manifest_resp
            .bytes()
            .await
            .map_err(|e| Status::internal(format!("read manifest failed: {}", e)))?;
        let mut manifest_json: serde_json::Value = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| Status::internal(format!("parse manifest failed: {}", e)))?;
        let mut metadata = PulledImageMetadata {
            manifest_media_type: manifest_json
                .get("mediaType")
                .and_then(|value| value.as_str())
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
                os == "linux" && arch == target_arch
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
                "Manifest index detected, selected child manifest digest={} (arch={})",
                selected_digest, target_arch
            );

            let child_url = format!(
                "https://{}/v2/{}/manifests/{}",
                reference.resolve_registry(),
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
            let child_bytes = child_resp
                .bytes()
                .await
                .map_err(|e| Status::internal(format!("read child manifest failed: {}", e)))?;
            manifest_json = serde_json::from_slice(&child_bytes)
                .map_err(|e| Status::internal(format!("parse child manifest failed: {}", e)))?;
            metadata.manifest_media_type = manifest_json
                .get("mediaType")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string());
        }

        if let Some(config_digest) = manifest_json
            .get("config")
            .and_then(|config| config.get("digest"))
            .and_then(|value| value.as_str())
        {
            let config_url = format!(
                "https://{}/v2/{}/blobs/{}",
                reference.resolve_registry(),
                reference.repository(),
                config_digest
            );
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
            let config_json: serde_json::Value = config_resp
                .json()
                .await
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
            metadata.annotations = config_json
                .get("config")
                .and_then(|config| config.get("Labels"))
                .and_then(|value| {
                    serde_json::from_value::<HashMap<String, String>>(value.clone()).ok()
                })
                .unwrap_or_default();
        }

        let layers = manifest_json
            .get("layers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Status::internal("manifest missing layers"))?;

        let mut total_size: u64 = 0;
        let mut layer_data: Vec<Vec<u8>> = Vec::new();
        info!("Start downloading {} layers", layers.len());
        for (idx, layer) in layers.iter().enumerate() {
            let layer_digest = layer
                .get("digest")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Status::internal("layer missing digest"))?;
            info!(
                "Downloading layer {}/{}: {}",
                idx + 1,
                layers.len(),
                layer_digest
            );
            let blob_url = format!(
                "https://{}/v2/{}/blobs/{}",
                reference.resolve_registry(),
                reference.repository(),
                layer_digest
            );
            let mut blob_req = Self::apply_basic_auth(http.get(blob_url), auth);
            if let Some(t) = token.as_deref() {
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
            let bytes = blob_resp
                .bytes()
                .await
                .map_err(|e| Status::internal(format!("read blob failed: {}", e)))?
                .to_vec();
            total_size += bytes.len() as u64;
            layer_data.push(bytes);
            info!(
                "Layer {}/{} downloaded, size={} bytes, total={} bytes",
                idx + 1,
                layers.len(),
                layer_data.last().map(|v| v.len()).unwrap_or(0),
                total_size
            );
        }

        let image_id = if let Some(d) = effective_digest {
            d
        } else {
            format!("sha256:{:x}", Sha256::digest(&manifest_bytes))
        };

        Ok((image_id, total_size, layer_data, metadata))
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
                        Self::build_image_verbose_info(&image, &self.storage_path)?
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
        let canonical_ref = Self::canonicalize_image_reference(&requested_ref);
        let supplied_bearer_token = req
            .auth
            .as_ref()
            .and_then(Self::provided_registry_bearer_token);

        let auth = match req.auth.clone() {
            Some(auth) => Self::registry_auth_from_auth_config(auth)?,
            None => RegistryAuth::Anonymous,
        };

        // 解析镜像引用
        let reference: Reference = canonical_ref
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid image reference: {}", e)))?;
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

        let pull_outcome = async {
            // 显式 bearer token 需要走自定义 registry API 路径，
            // oci-distribution 目前只支持 basic/anonymous。
            let (image_id, image_size, layers_to_persist, pulled_metadata) =
                if supplied_bearer_token.is_some() {
                    self.pull_via_registry_api(
                        &reference,
                        &auth,
                        supplied_bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    // 拉取镜像（优先 OCI 库，失败时走标准 Registry API）
                    let client = self.oci_client.lock().await;
                    let pull_result = client
                        .pull(
                            &reference,
                            &auth,
                            vec![
                                "application/vnd.oci.image.manifest.v1+json",
                                "application/vnd.docker.distribution.manifest.v2+json",
                            ],
                        )
                        .await;
                    drop(client);

                    match pull_result {
                        Ok(image_data) => {
                            let digest = image_data.digest.unwrap_or_default();
                            let id = Self::canonical_image_id(&digest, canonical_ref.as_bytes());
                            info!(
                                "OCI library pull succeeded for {}, layers={}",
                                canonical_ref,
                                image_data.layers.len()
                            );
                            let layers = image_data
                                .layers
                                .into_iter()
                                .map(|l| l.data)
                                .collect::<Vec<Vec<u8>>>();
                            (id, 0, layers, PulledImageMetadata::default())
                        }
                        Err(e) => {
                            let err_text = e.to_string();
                            if err_text.contains(
                                "application/vnd.docker.distribution.manifest.list.v2+json",
                            ) || err_text.contains("application/vnd.oci.image.index.v1+json")
                            {
                                info!(
                                    "OCI pull returned manifest index for {}, using registry api fallback: {}",
                                    reference, err_text
                                );
                            } else {
                                warn!(
                                    "OCI pull failed for {}, fallback to registry api: {}",
                                    reference, err_text
                                );
                            }
                            self.pull_via_registry_api(&reference, &auth, None).await?
                        }
                    }
                };

            let repo_digests = Self::repo_digest_for_reference(&reference, &image_id)
                .into_iter()
                .collect::<Vec<_>>();

            let image_dir = self.storage_path.join("images").join(&image_id);
            std::fs::create_dir_all(&image_dir).map_err(|e: io::Error| {
                Status::internal(format!("Failed to create image directory: {}", e))
            })?;
            info!(
                "Persisting {} layers to {:?}",
                layers_to_persist.len(),
                image_dir
            );
            for (i, layer) in layers_to_persist.iter().enumerate() {
                let layer_path = image_dir.join(format!("{}.tar.gz", i));
                std::fs::write(&layer_path, layer).map_err(|e: io::Error| {
                    Status::internal(format!("Failed to write layer: {}", e))
                })?;
                info!("Saved layer {} to {:?}", i, layer_path);
            }

            self.save_image_metadata(&CriusImage {
                id: image_id.clone(),
                repo_tags: vec![canonical_ref.clone()],
                repo_digests: repo_digests.clone(),
                size: image_size,
                pinned: false,
                pulled_at: Self::now_nanos(),
                source_reference: (canonical_ref != requested_ref).then_some(requested_ref.clone()),
                os: pulled_metadata.os.clone(),
                architecture: pulled_metadata.architecture.clone(),
                config_user: pulled_metadata.config_user.clone(),
                annotations: pulled_metadata.annotations.clone(),
                manifest_media_type: pulled_metadata.manifest_media_type.clone(),
            })
            .await
            .map_err(|e| {
                error!("Failed to save image metadata: {}", e);
                Status::internal(format!("Failed to save image metadata: {}", e))
            })?;

            let image = Image {
                id: image_id.clone(),
                repo_tags: vec![canonical_ref.clone()],
                repo_digests,
                size: image_size,
                pinned: false,
                spec: Some(ImageSpec {
                    image: canonical_ref.clone(),
                    user_specified_image: requested_ref.clone(),
                    annotations: pulled_metadata.annotations.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            };

            let mut images = self.images.lock().await;
            images.insert(canonical_ref.clone(), image);
            drop(images);

            info!("Image {} pulled successfully", image_id);

            Ok(Response::new(PullImageResponse {
                image_ref: image_id,
            }))
        }
        .await;

        if let Some(notify) = self.in_progress_pulls.lock().await.remove(&pull_key) {
            notify.notify_waiters();
        }

        pull_outcome
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
                        // 清理磁盘上的镜像文件（包含 metadata.json）
                        let image_dir = self.storage_path.join("images").join(&image_id);
                        if image_dir.exists() {
                            info!("Removing image directory: {:?}", image_dir);
                            if let Err(e) = tokio::fs::remove_dir_all(&image_dir).await {
                                error!("Failed to remove image directory {:?}: {}", image_dir, e);
                                // 即使磁盘清理失败，也返回成功，因为内存中的信息已经删除
                            } else {
                                info!("Successfully removed image directory: {:?}", image_dir);
                            }
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
        let images_dir = self.storage_path.join("images");
        let (used_bytes, inodes_used) =
            Self::collect_path_usage(&images_dir).map_err(|e: io::Error| {
                Status::internal(format!(
                    "Failed to collect image filesystem usage from {}: {}",
                    images_dir.display(),
                    e
                ))
            })?;

        let usage = FilesystemUsage {
            timestamp: Self::now_nanos(),
            fs_id: Some(FilesystemIdentifier {
                mountpoint: images_dir.display().to_string(),
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
mod tests {
    use super::*;
    use crate::proto::runtime::v1::{ImageFilter, ImageFsInfoRequest, ImageSpec};
    use crate::storage::{ContainerRecord, StorageManager};
    use chrono::Utc;
    use tempfile::{tempdir, TempDir};

    async fn test_image_service() -> ImageServiceImpl {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        std::mem::forget(dir);
        ImageServiceImpl::new(path).unwrap()
    }

    fn test_image_service_in_tempdir() -> (TempDir, ImageServiceImpl) {
        let dir = tempdir().unwrap();
        let service = ImageServiceImpl::new(dir.path().to_path_buf()).unwrap();
        (dir, service)
    }

    async fn insert_image(service: &ImageServiceImpl, image: Image) {
        let mut images = service.images.lock().await;
        for tag in &image.repo_tags {
            images.insert(tag.clone(), image.clone());
        }
    }

    #[tokio::test]
    async fn list_images_supports_filter_image_ref() {
        let service = test_image_service().await;
        insert_image(
            &service,
            Image {
                id: "sha256:1111111111111111".to_string(),
                repo_tags: vec!["busybox:latest".to_string()],
                ..Default::default()
            },
        )
        .await;
        insert_image(
            &service,
            Image {
                id: "sha256:2222222222222222".to_string(),
                repo_tags: vec!["pause:3.9".to_string()],
                ..Default::default()
            },
        )
        .await;

        let by_tag = ImageService::list_images(
            &service,
            Request::new(ListImagesRequest {
                filter: Some(ImageFilter {
                    image: Some(ImageSpec {
                        image: "busybox:latest".to_string(),
                        ..Default::default()
                    }),
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(by_tag.images.len(), 1);
        assert_eq!(by_tag.images[0].id, "sha256:1111111111111111");

        let by_id_prefix = ImageService::list_images(
            &service,
            Request::new(ListImagesRequest {
                filter: Some(ImageFilter {
                    image: Some(ImageSpec {
                        image: "111111111111".to_string(),
                        ..Default::default()
                    }),
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(by_id_prefix.images.len(), 1);
        assert_eq!(by_id_prefix.images[0].repo_tags, vec!["busybox:latest"]);
    }

    #[tokio::test]
    async fn image_status_returns_empty_response_when_missing() {
        let service = test_image_service().await;

        let response = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "missing:latest".to_string(),
                    ..Default::default()
                }),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.image.is_none());
        assert!(response.info.is_empty());
    }

    #[tokio::test]
    async fn image_status_verbose_returns_structured_info_and_repo_digests() {
        let (dir, service) = test_image_service_in_tempdir();
        let image_dir = dir.path().join("images").join("sha256:img-id");
        std::fs::create_dir_all(&image_dir).unwrap();
        std::fs::write(image_dir.join("0.tar.gz"), b"layer").unwrap();

        insert_image(
            &service,
            Image {
                id: "sha256:img-id".to_string(),
                repo_tags: vec!["busybox:latest".to_string()],
                repo_digests: vec!["docker.io/library/busybox@sha256:img-id".to_string()],
                size: 5,
                ..Default::default()
            },
        )
        .await;

        let response = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        let image = response.image.expect("expected image status");
        assert_eq!(
            image.repo_digests,
            vec!["docker.io/library/busybox@sha256:img-id"]
        );
        assert_eq!(
            image
                .spec
                .expect("expected image spec")
                .user_specified_image,
            "busybox:latest"
        );
        assert!(!image.pinned);
        let info: serde_json::Value =
            serde_json::from_str(response.info.get("info").expect("missing verbose info")).unwrap();
        assert_eq!(info["id"], "sha256:img-id");
        assert_eq!(info["repoTags"][0], "busybox:latest");
        assert_eq!(
            info["repoDigests"][0],
            "docker.io/library/busybox@sha256:img-id"
        );
        assert_eq!(info["pinned"], false);
        assert_eq!(info["layers"][0], "0.tar.gz");
    }

    #[tokio::test]
    async fn remove_image_is_idempotent_when_missing() {
        let service = test_image_service().await;

        ImageService::remove_image(
            &service,
            Request::new(RemoveImageRequest {
                image: Some(ImageSpec {
                    image: "missing:latest".to_string(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn list_images_deduplicates_same_id_across_multiple_tags() {
        let service = test_image_service().await;
        insert_image(
            &service,
            Image {
                id: "sha256:same-id".to_string(),
                repo_tags: vec!["repo/a:latest".to_string()],
                ..Default::default()
            },
        )
        .await;
        insert_image(
            &service,
            Image {
                id: "sha256:same-id".to_string(),
                repo_tags: vec!["repo/b:latest".to_string()],
                ..Default::default()
            },
        )
        .await;

        let response =
            ImageService::list_images(&service, Request::new(ListImagesRequest { filter: None }))
                .await
                .unwrap()
                .into_inner();
        assert_eq!(response.images.len(), 1);
        assert_eq!(response.images[0].id, "sha256:same-id");
        assert_eq!(
            response.images[0].repo_tags,
            vec!["repo/a:latest".to_string(), "repo/b:latest".to_string()]
        );
    }

    #[tokio::test]
    async fn image_status_aggregates_tags_and_user_for_same_id() {
        let (dir, service) = test_image_service_in_tempdir();
        service
            .save_image_metadata(&CriusImage {
                id: "sha256:agg-id".to_string(),
                repo_tags: vec!["repo/a:latest".to_string(), "repo/b:latest".to_string()],
                repo_digests: vec!["repo/a@sha256:agg-id".to_string()],
                size: 42,
                pinned: false,
                pulled_at: 1,
                source_reference: None,
                os: Some("linux".to_string()),
                architecture: Some("amd64".to_string()),
                config_user: Some("1001".to_string()),
                annotations: HashMap::new(),
                manifest_media_type: None,
            })
            .await
            .unwrap();
        let image_dir = dir.path().join("images").join("sha256:agg-id");
        std::fs::create_dir_all(&image_dir).unwrap();

        insert_image(
            &service,
            Image {
                id: "sha256:agg-id".to_string(),
                repo_tags: vec!["repo/a:latest".to_string()],
                repo_digests: vec!["repo/a@sha256:agg-id".to_string()],
                ..Default::default()
            },
        )
        .await;
        insert_image(
            &service,
            Image {
                id: "sha256:agg-id".to_string(),
                repo_tags: vec!["repo/b:latest".to_string()],
                ..Default::default()
            },
        )
        .await;

        let response = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "repo/b:latest".to_string(),
                    ..Default::default()
                }),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let image = response.image.expect("expected aggregated image status");
        assert_eq!(
            image.repo_tags,
            vec!["repo/a:latest".to_string(), "repo/b:latest".to_string()]
        );
        assert_eq!(image.repo_digests, vec!["repo/a@sha256:agg-id".to_string()]);
        let spec = image.spec.expect("expected image spec");
        assert_eq!(spec.image, "repo/b:latest");
        assert_eq!(spec.user_specified_image, "repo/b:latest");
        assert_eq!(
            image.uid.expect("expected uid from image metadata").value,
            1001
        );
        assert!(image.username.is_empty());
    }

    #[tokio::test]
    async fn image_status_restores_spec_annotations_from_metadata() {
        let service = test_image_service().await;
        insert_image(
            &service,
            Image {
                id: "sha256:anno-id".to_string(),
                repo_tags: vec!["repo/anno:latest".to_string()],
                ..Default::default()
            },
        )
        .await;
        service
            .save_image_metadata(&CriusImage {
                id: "sha256:anno-id".to_string(),
                repo_tags: vec!["repo/anno:latest".to_string()],
                repo_digests: Vec::new(),
                size: 7,
                pinned: false,
                pulled_at: 0,
                source_reference: None,
                os: None,
                architecture: None,
                config_user: None,
                annotations: HashMap::from([(
                    "org.opencontainers.image.title".to_string(),
                    "anno".to_string(),
                )]),
                manifest_media_type: None,
            })
            .await
            .unwrap();

        let response = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "repo/anno:latest".to_string(),
                    ..Default::default()
                }),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let image = response.image.expect("expected image");
        assert_eq!(
            image
                .spec
                .expect("expected image spec")
                .annotations
                .get("org.opencontainers.image.title")
                .map(String::as_str),
            Some("anno")
        );
    }

    #[test]
    fn registry_auth_from_auth_config_decodes_auth_field() {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode("demo-user:demo-password".as_bytes());
        let auth = AuthConfig {
            auth: encoded,
            ..Default::default()
        };
        match ImageServiceImpl::registry_auth_from_auth_config(auth).unwrap() {
            RegistryAuth::Basic(username, password) => {
                assert_eq!(username, "demo-user");
                assert_eq!(password, "demo-password");
            }
            RegistryAuth::Anonymous => panic!("expected basic auth from encoded auth field"),
        }
    }

    #[test]
    fn provided_registry_bearer_token_prefers_registry_then_identity_token() {
        let auth = AuthConfig {
            registry_token: "registry-token".to_string(),
            identity_token: "identity-token".to_string(),
            ..Default::default()
        };
        assert_eq!(
            ImageServiceImpl::provided_registry_bearer_token(&auth),
            Some("registry-token".to_string())
        );

        let auth = AuthConfig {
            identity_token: "identity-token".to_string(),
            ..Default::default()
        };
        assert_eq!(
            ImageServiceImpl::provided_registry_bearer_token(&auth),
            Some("identity-token".to_string())
        );
    }

    #[test]
    fn canonical_image_id_keeps_full_digest_without_truncation() {
        let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let id = ImageServiceImpl::canonical_image_id(digest, b"fallback");
        assert_eq!(id, digest);
    }

    #[test]
    fn canonicalize_image_reference_expands_short_names() {
        assert_eq!(
            ImageServiceImpl::canonicalize_image_reference("busybox"),
            "docker.io/library/busybox:latest"
        );
        assert_eq!(
            ImageServiceImpl::canonicalize_image_reference("busybox:1.36"),
            "docker.io/library/busybox:1.36"
        );
        assert_eq!(
            ImageServiceImpl::canonicalize_image_reference("library/busybox"),
            "docker.io/library/busybox:latest"
        );
    }

    #[tokio::test]
    async fn image_status_matches_short_name_against_canonical_tag() {
        let service = test_image_service().await;
        insert_image(
            &service,
            Image {
                id: "sha256:busybox-id".to_string(),
                repo_tags: vec!["docker.io/library/busybox:latest".to_string()],
                ..Default::default()
            },
        )
        .await;

        let response = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "busybox".to_string(),
                    ..Default::default()
                }),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            response.image.expect("expected image for short name").id,
            "sha256:busybox-id"
        );
    }

    #[tokio::test]
    async fn image_fs_info_reports_real_usage() {
        let (dir, service) = test_image_service_in_tempdir();
        let image_dir = dir.path().join("images").join("sha256:test-id");
        std::fs::create_dir_all(&image_dir).unwrap();
        std::fs::write(image_dir.join("layer1.tar"), b"abcde").unwrap();
        std::fs::write(image_dir.join("layer2.tar"), b"123456789").unwrap();

        let response = ImageService::image_fs_info(&service, Request::new(ImageFsInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.image_filesystems.len(), 1);
        let usage = &response.image_filesystems[0];
        assert!(usage.timestamp > 0);
        assert_eq!(
            usage
                .fs_id
                .as_ref()
                .expect("expected filesystem identifier")
                .mountpoint,
            dir.path().join("images").display().to_string()
        );
        let used_bytes = usage
            .used_bytes
            .as_ref()
            .expect("expected used bytes")
            .value;
        assert!(
            used_bytes >= 14,
            "expected used bytes >= 14, got {}",
            used_bytes
        );
    }

    #[tokio::test]
    async fn remove_image_reports_in_use_when_container_references_it() {
        let (dir, service) = test_image_service_in_tempdir();
        insert_image(
            &service,
            Image {
                id: "sha256:busybox-id".to_string(),
                repo_tags: vec!["busybox:latest".to_string()],
                ..Default::default()
            },
        )
        .await;

        let db_path = dir.path().join("crius.db");
        let mut storage = StorageManager::new(&db_path).unwrap();
        storage
            .save_container(&ContainerRecord {
                id: "container-1".to_string(),
                pod_id: "pod-1".to_string(),
                state: "running".to_string(),
                image: "busybox:latest".to_string(),
                command: "sleep 60".to_string(),
                created_at: Utc::now().timestamp(),
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                exit_code: None,
                exit_time: None,
            })
            .unwrap();

        let err = ImageService::remove_image(
            &service,
            Request::new(RemoveImageRequest {
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("in use"));
    }

    #[tokio::test]
    async fn remove_image_untags_single_reference_when_other_tags_remain() {
        let (dir, service) = test_image_service_in_tempdir();
        let image_dir = dir.path().join("images").join("sha256:untag-id");
        std::fs::create_dir_all(&image_dir).unwrap();
        service
            .save_image_metadata(&CriusImage {
                id: "sha256:untag-id".to_string(),
                repo_tags: vec![
                    "docker.io/library/busybox:latest".to_string(),
                    "docker.io/library/busybox:debug".to_string(),
                ],
                repo_digests: vec!["docker.io/library/busybox@sha256:untag-id".to_string()],
                size: 10,
                pinned: false,
                pulled_at: 123,
                source_reference: Some("busybox".to_string()),
                os: Some("linux".to_string()),
                architecture: Some("amd64".to_string()),
                config_user: Some("1000".to_string()),
                annotations: HashMap::from([(
                    "org.opencontainers.image.title".to_string(),
                    "busybox".to_string(),
                )]),
                manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            })
            .await
            .unwrap();
        insert_image(
            &service,
            Image {
                id: "sha256:untag-id".to_string(),
                repo_tags: vec![
                    "docker.io/library/busybox:latest".to_string(),
                    "docker.io/library/busybox:debug".to_string(),
                ],
                repo_digests: vec!["docker.io/library/busybox@sha256:untag-id".to_string()],
                spec: Some(ImageSpec {
                    image: "docker.io/library/busybox:latest".to_string(),
                    user_specified_image: "docker.io/library/busybox:latest".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;

        ImageService::remove_image(
            &service,
            Request::new(RemoveImageRequest {
                image: Some(ImageSpec {
                    image: "docker.io/library/busybox:latest".to_string(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();

        let remaining = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "docker.io/library/busybox:debug".to_string(),
                    ..Default::default()
                }),
                verbose: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let image = remaining.image.expect("expected remaining tag");
        assert_eq!(image.repo_tags, vec!["docker.io/library/busybox:debug"]);
        let info: serde_json::Value =
            serde_json::from_str(remaining.info.get("info").expect("missing verbose info"))
                .unwrap();
        assert_eq!(info["pulledAt"], 123);
        assert_eq!(info["sourceReference"], "busybox");
        assert_eq!(info["os"], "linux");
        assert_eq!(info["architecture"], "amd64");
        assert_eq!(info["configUser"], "1000");
        assert_eq!(
            info["manifestMediaType"],
            "application/vnd.oci.image.manifest.v1+json"
        );
        assert_eq!(
            info["annotations"]["org.opencontainers.image.title"],
            "busybox"
        );
        assert!(
            image_dir.exists(),
            "image directory should remain after untag"
        );

        let removed = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: "docker.io/library/busybox:latest".to_string(),
                    ..Default::default()
                }),
                verbose: false,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(removed.image.is_none());
    }
}

pub mod layer;
