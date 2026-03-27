use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::runtime::v1::{
    runtime_service_server::RuntimeService, Container, ContainerState, ContainerStatus,
    ExecResponse, ExecSyncResponse,
    PortForwardResponse, RunPodSandboxRequest, RunPodSandboxResponse,
    StatusResponse, VersionRequest, VersionResponse,StopPodSandboxRequest,StopPodSandboxResponse,
    ExecRequest,
    ExecSyncRequest,PortForwardRequest,
    StatusRequest,
};
use crate::proto::runtime::v1::{
    RemovePodSandboxRequest,RemovePodSandboxResponse,
    GetEventsRequest,ContainerEventResponse,
    ListMetricDescriptorsRequest,ListMetricDescriptorsResponse,
    ListPodSandboxMetricsRequest,ListPodSandboxMetricsResponse,
    RuntimeConfigRequest,RuntimeConfigResponse,
    CheckpointContainerRequest,CheckpointContainerResponse,
    PodSandboxStatusRequest,PodSandboxStatusResponse,
    ListPodSandboxRequest,ListPodSandboxResponse,
    CreateContainerRequest,CreateContainerResponse,
    StartContainerRequest,StartContainerResponse,
    StopContainerRequest,StopContainerResponse,
    RemoveContainerRequest,RemoveContainerResponse,
    ListContainersRequest,ListContainersResponse,
    ContainerStatusRequest,ContainerStatusResponse,
    ReopenContainerLogRequest,ReopenContainerLogResponse,
    AttachRequest,AttachResponse,
    ContainerStatsRequest,ContainerStatsResponse,
    ListContainerStatsRequest,ListContainerStatsResponse,
    PodSandboxStatsRequest,PodSandboxStatsResponse,
    ListPodSandboxStatsRequest,ListPodSandboxStatsResponse,
    UpdateRuntimeConfigRequest,UpdateRuntimeConfigResponse,
    UpdateContainerResourcesRequest,UpdateContainerResourcesResponse,
    PodSandboxState,PodSandboxStatus,
};

/// 运行时服务实现
#[derive(Debug)]
pub struct RuntimeServiceImpl {
    // 存储容器状态的线程安全HashMap
    containers: Arc<Mutex<HashMap<String, Container>>>,
    // 存储Pod沙箱状态的线程安全HashMap
    pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    // 运行时配置
    config: RuntimeConfig,
}

/// 运行时配置
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: PathBuf,
    pub runtime: String,
    pub runtime_root: PathBuf,
    pub log_dir: PathBuf,
}

impl RuntimeServiceImpl {
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            containers: Arc::new(Mutex::new(HashMap::new())),
            pod_sandboxes: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }
}

#[tonic::async_trait]
impl RuntimeService for RuntimeServiceImpl {
    // 获取运行时版本
    async fn version(
        &self,
        _request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(VersionResponse {
            version: "0.1.0".to_string(),
            runtime_name: "crius".to_string(),
            runtime_version: "0.1.0".to_string(),
            runtime_api_version: "v1".to_string(),
        }))
    }

    // 创建Pod沙箱
    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        let req = request.into_inner();
        let pod_id = format!("pod-{}", uuid::Uuid::new_v4().to_string());
        
        log::info!("Creating pod sandbox with ID: {}", pod_id);
        log::debug!("Pod config: {:?}", req.config);
        
        // 创建Pod沙箱元数据
        let pod_sandbox = crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: req.config.clone().map(|c| c.metadata).flatten(),
            state: PodSandboxState::SandboxReady as i32,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            labels: req.config.clone().map(|c| c.labels).unwrap_or_default(),
            annotations: req.config.clone().map(|c| c.annotations).unwrap_or_default(),
            ..Default::default()
        };
        
        // 存储Pod沙箱信息
        let mut pod_sandboxes = self.pod_sandboxes.lock().await;
        pod_sandboxes.insert(pod_id.clone(), pod_sandbox);
        
        // 这里应该实现实际的Pod沙箱创建逻辑
        // 1. 创建网络命名空间
        // 2. 设置网络
        // 3. 创建pause容器
        
        Ok(Response::new(RunPodSandboxResponse { pod_sandbox_id: pod_id }))
    }

    // 停止Pod沙箱
    async fn stop_pod_sandbox(
        &self,
        _request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        // 实现停止Pod沙箱的逻辑
        Ok(Response::new(StopPodSandboxResponse { }))
    }

    // 获取容器状态
    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        let req = request.into_inner();
        let container_id = req.container_id;
        let containers = self.containers.lock().await;
        
        if let Some(container) = containers.get(&container_id) {
            let status = ContainerStatus {
                id: container.id.clone(),
                state: ContainerState::ContainerRunning as i32,
                // 填充其他状态字段...
                ..Default::default()
            };
            
            Ok(Response::new(ContainerStatusResponse {
                status: Some(status),
                ..Default::default()
            }))
        } else {
            Err(Status::not_found("Container not found"))
        }
    }

    // 列出容器
    async fn list_containers(
        &self,
        _request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        let containers = self.containers.lock().await;
        let containers_list = containers.values().cloned().collect();
        
        Ok(Response::new(ListContainersResponse {
            containers: containers_list,
        }))
    }

    // 执行命令
    async fn exec(
        &self,
        _request: Request<ExecRequest>,
    ) -> Result<Response<ExecResponse>, Status> {
        // 实现执行命令的逻辑
        Ok(Response::new(ExecResponse {
            url: "unix:///var/run/crius/crius.sock".to_string(),
        }))
    }

    // 同步执行命令
    async fn exec_sync(
        &self,
        _request: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        // 实现同步执行命令的逻辑
        Ok(Response::new(ExecSyncResponse {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_code: 0,
        }))
    }

    // 端口转发
    async fn port_forward(
        &self,
        _request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        // 实现端口转发的逻辑
        Ok(Response::new(PortForwardResponse {
            url: "unix:///var/run/crius/crius.sock".to_string(),
        }))
    }

    // 获取运行时状态
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let mut info = HashMap::new();
        info.insert("runtime_name".to_string(), "crius".to_string());
        info.insert("runtime_version".to_string(), "0.1.0".to_string());
        info.insert("runtime_api_version".to_string(), "v1".to_string());
        info.insert("root_dir".to_string(), self.config.root_dir.to_string_lossy().to_string());
        info.insert("runtime".to_string(), self.config.runtime.clone());
        
        Ok(Response::new(StatusResponse {
            status: Some(crate::proto::runtime::v1::RuntimeStatus {
                conditions: Vec::new(),
            }),
            info,
        }))
    }

    // 删除pod_sandbox
    async fn remove_pod_sandbox(
        &self,
        _request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        // 实现删除Pod沙箱的逻辑
        Ok(Response::new(RemovePodSandboxResponse { }))
    }

    // 获取pod_sandbox状态
    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        let req = request.into_inner();
        let pod_sandboxes = self.pod_sandboxes.lock().await;
        
        if let Some(pod_sandbox) = pod_sandboxes.get(&req.pod_sandbox_id) {
            let mut info = HashMap::new();
            info.insert("podSandboxId".to_string(), pod_sandbox.id.clone());
            if let Some(metadata) = &pod_sandbox.metadata {
                info.insert("name".to_string(), metadata.name.clone());
            }
            
            let status = PodSandboxStatus {
                state: pod_sandbox.state,
                created_at: pod_sandbox.created_at,
                ..Default::default()
            };
            
            Ok(Response::new(PodSandboxStatusResponse {
                status: Some(status),
                info,
                containers_statuses: Vec::new(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
            }))
        } else {
            Err(Status::not_found("Pod sandbox not found"))
        }
    }

    // 列出pod_sandbox
    async fn list_pod_sandbox(
        &self,
        _request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        let pod_sandboxes = self.pod_sandboxes.lock().await;
        let items = pod_sandboxes.values().cloned().collect();
        
        Ok(Response::new(ListPodSandboxResponse {
            items,
        }))
    }

    // 创建容器
    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        log::info!("CreateContainer called");
        let req = request.into_inner();
        let pod_sandbox_id = req.pod_sandbox_id.clone();
        let config = req.config.ok_or_else(|| Status::invalid_argument("Container config not specified"))?;
        
        let container_id = format!("container-{}", uuid::Uuid::new_v4().to_string());
        
        log::info!("Creating container with ID: {}", container_id);
        log::debug!("Container config: {:?}", config);
        
        // 创建容器元数据
        let container = Container {
            id: container_id.clone(),
            pod_sandbox_id: pod_sandbox_id.clone(),
            state: ContainerState::ContainerCreated as i32,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            labels: config.labels.clone(),
            metadata: config.metadata.clone(),
            image_ref: config.image.as_ref().map(|i| i.image.clone()).unwrap_or_default(),
            ..Default::default()
        };
        
        // 存储容器信息
        let mut containers = self.containers.lock().await;
        containers.insert(container_id.clone(), container);
        log::info!("Container stored, total containers: {}", containers.len());
        
        // 这里应该实现实际的容器创建逻辑
        // 1. 验证镜像存在
        // 2. 创建容器根目录
        // 3. 设置容器配置
        // 4. 启动容器
        
        Ok(Response::new(CreateContainerResponse {
            container_id,
        }))
    }

    // 启动容器
    async fn start_container(
        &self,
        _request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        // 实现启动容器的逻辑
        Ok(Response::new(StartContainerResponse { }))
    }

    // 停止容器
    async fn stop_container(
        &self,
        _request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        // 实现停止容器的逻辑
        Ok(Response::new(StopContainerResponse { }))
    }

    // 删除容器
    async fn remove_container(
        &self,
        _request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        // 实现删除容器的逻辑
        Ok(Response::new(RemoveContainerResponse { }))
    }

    //重新打开容器日志
    async fn reopen_container_log(
        &self,
        _request: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        // 实现重新打开容器日志的逻辑
        Ok(Response::new(ReopenContainerLogResponse { }))
    }

    //
    async fn attach(
        &self,
        _request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        // 实现 attach 的逻辑
        Ok(Response::new(AttachResponse {
            url: "unix:///var/run/crius/crius.sock".to_string(),
        }))
    }

    // 容器统计信息
    async fn container_stats(
        &self,
        _request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        // 实现 container_stats 的逻辑
        Ok(Response::new(ContainerStatsResponse {
            stats: None,
        }))
    }

    // 容器列表统计信息
    async fn list_container_stats(
        &self,
        _request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        // 实现 list_container_stats 的逻辑
        Ok(Response::new(ListContainerStatsResponse {
            stats: Vec::new(),
        }))
    }

    // pod沙箱统计信息
    async fn pod_sandbox_stats(
        &self,
        _request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        // 实现 pod_sandbox_stats 的逻辑
        Ok(Response::new(PodSandboxStatsResponse {
            stats: None,
        }))
    }

    // pod沙箱列表统计信息
    async fn list_pod_sandbox_stats(
        &self,
        _request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        // 实现 list_pod_sandbox_stats 的逻辑
        Ok(Response::new(ListPodSandboxStatsResponse {
            stats: Vec::new(),
        }))
    }

    // 更新运行时配置
    async fn update_runtime_config(
        &self,
        _request: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        // 实现 update_runtime_config 的逻辑
        Ok(Response::new(UpdateRuntimeConfigResponse { }))
    }

    //
    async fn checkpoint_container(
        &self,
        _request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        // 实现 checkpoint_container 的逻辑
        Ok(Response::new(CheckpointContainerResponse { }))
    }

    type GetContainerEventsStream = ReceiverStream<Result<ContainerEventResponse, Status>>;

    //
    async fn get_container_events(
        &self,
        _request: Request<GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        // 实现 get_container_events 的逻辑
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let stream = ReceiverStream::new(rx);
        Ok(Response::new(stream))
    }

    //
    async fn list_metric_descriptors(
        &self,
        _request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        // 实现 list_metric_descriptors 的逻辑
        Ok(Response::new(ListMetricDescriptorsResponse {
            descriptors: Vec::new(),
        }))
    }

    //
    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        // 实现 list_pod_sandbox_metrics 的逻辑
        Ok(Response::new(ListPodSandboxMetricsResponse {
            pod_metrics: Vec::new(),
        }))
    }

    //
    async fn runtime_config(
        &self,
        _request: Request<RuntimeConfigRequest>,
    ) -> Result<Response<RuntimeConfigResponse>, Status> {
        // 实现 runtime_config 的逻辑
        Ok(Response::new(RuntimeConfigResponse {
            linux: None,
            
        }))
    }

    async fn update_container_resources(
        &self,
        _request: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        // TODO: 实现资源更新逻辑
        Ok(Response::new(UpdateContainerResourcesResponse { }))
    }
}
