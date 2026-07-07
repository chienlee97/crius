use tonic::{Request, Response, Status};

use crate::proto::local::v1::{
    local_service_server::LocalService, CreateLocalContainerRequest, CreateLocalContainerResponse,
};

#[derive(Clone)]
pub struct LocalServiceImpl {
    runtime: crate::server::RuntimeServiceImpl,
}

impl LocalServiceImpl {
    pub fn new(runtime: crate::server::RuntimeServiceImpl) -> Self {
        Self { runtime }
    }
}

#[tonic::async_trait]
impl LocalService for LocalServiceImpl {
    async fn create_local_container(
        &self,
        request: Request<CreateLocalContainerRequest>,
    ) -> Result<Response<CreateLocalContainerResponse>, Status> {
        self.runtime.create_local_container_impl(request).await
    }
}
