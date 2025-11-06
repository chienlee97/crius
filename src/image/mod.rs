use std::collections::HashMap;
use tonic::{Request, Response, Status};
use oci_distribution::{
    client::{ClientConfig, Client, ImageData},
    secrets::RegistryAuth,
    Reference,
};
use std::path::PathBuf;

use crate::proto::runtime::v1alpha2::{
    image_service_server::ImageService, Image, ImageStatusRequest, ImageStatusResponse,
    ListImagesResponse, PullImageRequest, PullImageResponse, RemoveImageRequest,
    RemoveImageResponse,
};

/// 镜像服务实现
#[derive(Debug)]
pub struct ImageServiceImpl {
    // 存储镜像信息的线程安全HashMap
    images: std::sync::Arc<tokio::sync::Mutex<HashMap<String, Image>>>,
    storage_path: PathBuf,
    oci_client: oci_distribution::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageMeta {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub size: u64,
}

impl ImageServiceImpl {
    pub fn new(storage_path: impl AsRef<Path>) ->  Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();

        if !storage_path.exists() {
            std::fs::create_dir_all(&storage_path).context("Failed to create storage directory")?;
        }

        let client_config = oci_distribution::ClientConfig {
            protocol: oci_distribution::client::ClientProtocol::Https,
            ..Default::default()
        };
        let oci_client = oci_distribution::Client::new(client_config);
        let images = std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        
        Ok(Self {
            images,
            storage_path,
            oci_client,
        })
    }

    // 加载本地镜像
     async fn load_local_images(&self) -> Result<()> {
        let imaages_dir = self.storage_path.join("images");
        if !imaages_dir.exists() {
            return Ok(());
        }

        let mut images = self.images.lock().await;
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
                    id: meta.id,
                    repo_tags: meta.repo_tags,
                    size: meta.size,
                    // 待填充其他字段...
                    ..Default::default()
                };
                for tag in meta.repo_tags {
                    images.insert(tag.clone(), image.clone());
                }
            }
        }
        Ok(())
     }
}

#[tonic::async_trait]
impl ImageService for ImageServiceImpl {
    // 列出镜像
    async fn list_images(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let images = self.images.lock().await;
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
            Ok(Response::new(ImageStatusResponse {
                image: Some(image.clone()),
                info: HashMap::new(),
            }))
        }

        // 尝试通过ID查找
        for image in images.values() {
            if image.id.starts_with(&image_spec.image) {
                Ok(Response::new(ImageStatusResponse {
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
        let image_spec = req.image.unwrap();
        let image_id = format!("sha256:{}", uuid::Uuid::new_v4());
        
        let mut images = self.images.lock().await;
        let image = Image {
            id: image_id.clone(),
            repo_tags: vec![image_spec.image.clone()],
            size: 0,
            // 填充其他字段...
            ..Default::default()
        };
        
        images.insert(image_spec.image, image);
        
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
}
