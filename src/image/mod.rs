use std::collections::HashMap;
use tonic::{Request, Response, Status};

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
}

impl ImageServiceImpl {
    pub fn new() -> Self {
        Self {
            images: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
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
        let images = self.images.lock().await;
        
        if let Some(image) = images.get(&req.image.unwrap().image) {
            Ok(Response::new(ImageStatusResponse {
                image: Some(image.clone()),
                info: HashMap::new(),
            }))
        } else {
            Err(Status::not_found("Image not found"))
        }
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
