use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::io;
use std::sync::Arc;

use anyhow::Context;
use tonic::{Request, Response, Status};
use oci_distribution::{
    client::{ClientConfig, Client, ImageData},
    secrets::RegistryAuth,
    Reference,
};
use log::{info, error};
use serde::{Serialize, Deserialize};
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
     pub async fn load_local_images(&self) -> Result<(()), Error> {
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
            if path.is_file() {
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
        
        // 添加一个测试镜像用于验证
        info!("Adding test image for verification");
        let test_image = Image {
            id: "test123".to_string(),
            repo_tags: vec!["test:latest".to_string()],
            size: 1024,
            ..Default::default()
        };
        images.insert("test:latest".to_string(), test_image);
        info!("Test image added, total images: {}", images.len());
        
        Ok(())
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

        let images = self.images.lock().await;
        
        // 尝试通过完整引用查找镜像
        if let Some(image) = images.get(&image_spec.image) {
            return Ok(Response::new(ImageStatusResponse {
                image: Some(image.clone()),
                info: HashMap::new(),
            }));
        }

        // 尝试通过ID查找
        for image in images.values() {
            if image.id.starts_with(&image_spec.image) {
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

        // 拉取镜像
        let mut client = self.oci_client.lock().await;
        let image_data = client
        .pull(
            &reference,
            &auth,
            vec![
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            ],
        )
        .await
        .map_err(|e| {
            error!("Failed to pull image {}: {}", reference, e);
            Status::internal(format!("Failed to pull image: {}", e))
        })?;
        
        // 生成镜像ID
        let image_id = format!("sha256:{}", &image_data.digest.expect("Digest should be present").replace("sha256:", "")[..12]);

        let image = Image {
            id: image_id.clone(),
            repo_tags: vec![image_spec.image.clone()],
            size: 0,
            // 填充其他字段...
            ..Default::default()
        };

        // 创建镜像存储位置
        let image_dir = self.storage_path.join("images").join(&image_id);
        std::fs::create_dir_all(&image_dir)
            .map_err(|e: io::Error| {
                error!("Failed to create image directory: {}", e);
                tonic::Status::internal(format!("Failed to create image directory: {}", e))
            })?;
        
        // 保存镜像层
        for (i, layer) in image_data.layers.iter().enumerate() {
            let layer_path = image_dir.join(format!("{}.tar.gz", i));
            std::fs::write(&layer_path, &layer.data)
            .map_err(|e: io::Error| {
                error!("Failed to write layer: {}", e);
                tonic::Status::internal(format!("Failed to write layer: {}", e))
            })?;
        }

        // 保存镜像配置
        // let config_path = image_dir.join("config.json");
        // std::fs::write(&config_path, serde_json::to_vec(&image_data).context("Failed to write config")?).context("Failed to write config")?;
        
        // 保存镜像元数据
        self.save_image_metadata(&CriusImage {
            id: image_id.clone(),
            repo_tags: vec![image_spec.image.clone()],
            size: 0,
            // 填充其他字段...
            // ..Default::default()
        }).await.map_err(|e| {
            error!("Failed to save image metadata: {}", e);
            Status::internal(format!("Failed to save image metadata: {}", e))
        })?;
        
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
        let mut images = self.images.lock().await;
        
        if images.remove(&req.image.unwrap().image).is_some() {
            Ok(Response::new(RemoveImageResponse {}))
        } else {
            Err(Status::not_found("Image not found"))
        }
    }

    // 获取镜像文件信息
    async fn image_fs_info(
        &self,
        request: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        Err(Status::not_found("Image not found"))
    }
}
