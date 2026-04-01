use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::path::PathBuf;
use std::io;
use std::sync::Arc;

use anyhow::Context;
use tonic::{Request, Response, Status};
use oci_distribution::{
    secrets::RegistryAuth,
    Reference,
};
use log::{info, warn, error};
use serde::{Serialize, Deserialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::error::Error;
use crate::proto::runtime::v1::{
    image_service_server::ImageService, Image, ImageStatusRequest, ImageStatusResponse,ListImagesRequest,
    ListImagesResponse, PullImageRequest, PullImageResponse, RemoveImageRequest,ImageFsInfoRequest,ImageFsInfoResponse,
    RemoveImageResponse,
};

/// crius镜像
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CriusImage {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub size: u64,
}

/// 镜像服务实现
pub struct ImageServiceImpl {
    // 存储镜像信息的线程安全HashMap
    images: std::sync::Arc<tokio::sync::Mutex<HashMap<String, Image>>>,
    storage_path: PathBuf,
    oci_client: Arc<Mutex<oci_distribution::Client>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageMeta {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub size: u64,
}

impl ImageServiceImpl {
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

    pub fn new(storage_path: impl AsRef<Path>) ->  Result<Self, Error> {
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
            let meta: ImageMeta = serde_json::from_slice(&meta_data).context("Failed to parse metadata")?;

            let path = entry.path();
            if path.is_dir() {
                let image = Image {
                    id: meta.id.clone(),
                    repo_tags: meta.repo_tags.clone(),
                    size: meta.size.clone(),
                    // 待填充其他字段...
                    ..Default::default()
                };
                for tag in &meta.repo_tags {
                    images.insert(tag.clone(), image.clone());
                }
            }
        }
        
        Ok(())
     }

    async fn find_local_image(&self, image_ref: &str) -> Option<Image> {
        {
            let images = self.images.lock().await;
            if let Some(image) = images.get(image_ref) {
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
            if meta.repo_tags.iter().any(|tag| tag == image_ref) {
                let image = Image {
                    id: meta.id.clone(),
                    repo_tags: meta.repo_tags.clone(),
                    size: meta.size,
                    ..Default::default()
                };
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
        let meta_path = self.storage_path.join("images").join(&image.id).join("metadata.json");
        if !meta_path.exists() {
            std::fs::create_dir_all(meta_path.parent().unwrap()).context("Failed to create metadata directory")?;
        }
        let meta_data = serde_json::to_vec(image).context("Failed to serialize metadata")?;
        std::fs::write(meta_path, meta_data).context("Failed to write metadata")?;
        Ok(())
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

    async fn pull_via_registry_api(
        &self,
        reference: &Reference,
        auth: &RegistryAuth,
    ) -> Result<(String, u64, Vec<Vec<u8>>), Status> {
        info!(
            "Using registry API pull flow for {}",
            reference
        );
        let http = reqwest::Client::new();
        let ping_url = format!("https://{}/v2/", reference.resolve_registry());
        info!("Registry ping: {}", ping_url);
        let ping = http
            .get(&ping_url)
            .send()
            .await
            .map_err(|e| Status::internal(format!("registry ping failed: {}", e)))?;

        let mut token: Option<String> = None;
        if ping.status() == reqwest::StatusCode::UNAUTHORIZED {
            let challenge = ping
                .headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| Status::internal("missing WWW-Authenticate header"))?;
            let (realm, service) = Self::parse_bearer_challenge(challenge)
                .ok_or_else(|| Status::internal("invalid bearer challenge"))?;
            let scope = format!("repository:{}:pull", reference.repository());
            info!(
                "Requesting bearer token, scope={}",
                scope
            );
            let mut token_req = http.get(&realm).query(&[("scope", scope.as_str())]);
            if let Some(s) = service.as_deref() {
                token_req = token_req.query(&[("service", s)]);
            }
            if let RegistryAuth::Basic(username, password) = auth {
                token_req = token_req.basic_auth(username, Some(password));
            }
            let token_resp = token_req
                .send()
                .await
                .map_err(|e| Status::internal(format!("token request failed: {}", e)))?;
            if !token_resp.status().is_success() {
                let status = token_resp.status();
                let text = token_resp.text().await.unwrap_or_default();
                return Err(Status::internal(format!(
                    "token request failed: {} {}",
                    status,
                    text
                )));
            }
            let token_json: serde_json::Value = token_resp
                .json()
                .await
                .map_err(|e| Status::internal(format!("invalid token response: {}", e)))?;
            token = token_json
                .get("token")
                .or_else(|| token_json.get("access_token"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        let manifest_url = Self::manifest_url(reference);
        info!("Fetching manifest: {}", manifest_url);
        let mut manifest_req = http.get(&manifest_url).header(
            reqwest::header::ACCEPT,
            "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json",
        );
        if let Some(t) = token.as_deref() {
            manifest_req = manifest_req.bearer_auth(t);
        }
        let manifest_resp = manifest_req
            .send()
            .await
            .map_err(|e| Status::internal(format!("manifest request failed: {}", e)))?;
        if !manifest_resp.status().is_success() {
            let status = manifest_resp.status();
            let text = manifest_resp.text().await.unwrap_or_default();
            return Err(Status::internal(format!(
                "manifest request failed: {} {}",
                status,
                text
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
        let mut effective_digest = digest;

        if manifest_json.get("layers").and_then(|v| v.as_array()).is_none() {
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
            let mut child_req = http.get(child_url).header(
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
            let mut blob_req = http.get(blob_url);
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
                    status,
                    text
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

        Ok((image_id, total_size, layer_data))
    }
}

#[tonic::async_trait]
impl ImageService for ImageServiceImpl {
    // 列出镜像
    async fn list_images(
        &self,
        _request: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let images = self.images.lock().await;
        info!("Number of images in memory: {}", images.len());
        for (key, image) in images.iter() {
            info!("Image: {} -> {}", key, image.id);
        }
        let images_list = images.values().cloned().collect();
        
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
        let image_spec = req.image.ok_or_else(|| Status::invalid_argument("Image not specified"))?;
        let requested_ref = image_spec.image;

        let images = self.images.lock().await;
        
        // 尝试通过完整引用查找镜像
        if let Some(image) = images.get(&requested_ref) {
            return Ok(Response::new(ImageStatusResponse {
                image: Some(image.clone()),
                info: HashMap::new(),
            }));
        }

        // 尝试通过ID查找
        for image in images.values() {
            if Self::image_id_matches(&image.id, &requested_ref) {
                return Ok(Response::new(ImageStatusResponse {
                    image: Some(image.clone()),
                    info: HashMap::new(),
                }));
            }
        }

        Err(Status::not_found("Image not found"))
    }

    // 拉取镜像
    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let req = request.into_inner();
        let image_spec = req.image.ok_or_else(|| Status::invalid_argument("Image spec not specified"))?;

        let auth = req.auth.map(|a| RegistryAuth::Basic(
            a.username, 
            a.password
        )).unwrap_or(RegistryAuth::Anonymous);

        // 解析镜像引用
        let reference: Reference = image_spec.image.parse().map_err(|e| {
            Status::invalid_argument(format!("Invalid image reference: {}", e))
        })?;

        info!("Pulling image: {}", image_spec.image);
        info!("Checking whether image exists locally: {}", image_spec.image);
        if let Some(existing_image) = self.find_local_image(&image_spec.image).await {
            info!(
                "Image already exists locally: {} -> {}",
                image_spec.image, existing_image.id
            );
            return Ok(Response::new(PullImageResponse {
                image_ref: existing_image.id,
            }));
        }
        info!("Local image not found, start remote pull: {}", image_spec.image);

        // 拉取镜像（优先 OCI 库，失败时走标准 Registry API）
        let mut client = self.oci_client.lock().await;
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

        let (image_id, image_size, layers_to_persist) = match pull_result {
            Ok(image_data) => {
                let digest = image_data.digest.unwrap_or_else(|| "sha256:unknown".to_string());
                let short = digest.replace("sha256:", "");
                let id = format!("sha256:{}", &short[..short.len().min(12)]);
                info!(
                    "OCI library pull succeeded for {}, layers={}",
                    image_spec.image,
                    image_data.layers.len()
                );
                let layers = image_data
                    .layers
                    .into_iter()
                    .map(|l| l.data)
                    .collect::<Vec<Vec<u8>>>();
                (id, 0, layers)
            }
            Err(e) => {
                let err_text = e.to_string();
                if err_text.contains("application/vnd.docker.distribution.manifest.list.v2+json")
                    || err_text.contains("application/vnd.oci.image.index.v1+json")
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
                self.pull_via_registry_api(&reference, &auth).await?
            }
        };

        let image_dir = self.storage_path.join("images").join(&image_id);
        std::fs::create_dir_all(&image_dir)
            .map_err(|e: io::Error| Status::internal(format!("Failed to create image directory: {}", e)))?;
        info!(
            "Persisting {} layers to {:?}",
            layers_to_persist.len(),
            image_dir
        );
        for (i, layer) in layers_to_persist.iter().enumerate() {
            let layer_path = image_dir.join(format!("{}.tar.gz", i));
            std::fs::write(&layer_path, layer)
                .map_err(|e: io::Error| Status::internal(format!("Failed to write layer: {}", e)))?;
            info!("Saved layer {} to {:?}", i, layer_path);
        }

        // 保存镜像元数据
        self.save_image_metadata(&CriusImage {
            id: image_id.clone(),
            repo_tags: vec![image_spec.image.clone()],
            size: image_size,
        }).await.map_err(|e| {
            error!("Failed to save image metadata: {}", e);
            Status::internal(format!("Failed to save image metadata: {}", e))
        })?;

        let image = Image {
            id: image_id.clone(),
            repo_tags: vec![image_spec.image.clone()],
            size: image_size,
            ..Default::default()
        };

        // 更新内存中镜像数据
        let mut images = self.images.lock().await;
        images.insert(image_spec.image, image);
        
        info!("Image {} pulled successfully", image_id);

        Ok(Response::new(PullImageResponse {
            image_ref: image_id,
        }))
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

                let image_ids_to_remove: Vec<String> = {
                    let mut images = self.images.lock().await;

                    if let Some(image) = images.remove(&requested_ref) {
                        vec![image.id]
                    } else {
                        let matched_ids: HashSet<String> = images
                            .values()
                            .filter(|image| Self::image_id_matches(&image.id, &requested_ref))
                            .map(|image| image.id.clone())
                            .collect();

                        if matched_ids.is_empty() {
                            Vec::new()
                        } else {
                            // 按镜像 ID 删除所有别名/标签映射，避免残留脏数据
                            images.retain(|_, image| !matched_ids.contains(&image.id));
                            matched_ids.into_iter().collect()
                        }
                    }
                };

                if image_ids_to_remove.is_empty() {
                    Err(Status::not_found("Image not found"))
                } else {
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
        Ok(Response::new(ImageFsInfoResponse {
            image_filesystems: Vec::new(),
            container_filesystems: Vec::new(),
        }))
    }
}

pub mod layer;
