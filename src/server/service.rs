use super::*;

/// 运行时服务实现
pub struct RuntimeServiceImpl {
    pub(super) containers: Arc<Mutex<HashMap<String, Container>>>,
    pub(super) pod_sandboxes: Arc<Mutex<HashMap<String, crate::proto::runtime::v1::PodSandbox>>>,
    pub(super) config: RuntimeConfig,
    pub(super) nri_config: NriConfig,
    pub(super) nri: Arc<dyn NriApi>,
    pub(super) runtime: RuncRuntime,
    pub(super) pod_manager: tokio::sync::Mutex<PodSandboxManager<RuncRuntime>>,
    pub(super) persistence: Arc<Mutex<PersistenceManager>>,
    pub(super) streaming: Arc<Mutex<Option<StreamingServer>>>,
    pub(super) events: tokio::sync::broadcast::Sender<ContainerEventResponse>,
    pub(super) shim_work_dir: PathBuf,
    pub(super) runtime_network_config: Arc<Mutex<Option<crate::proto::runtime::v1::NetworkConfig>>>,
    pub(super) exit_monitors: Arc<Mutex<HashSet<String>>>,
}

/// 运行时配置
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: PathBuf,
    pub runtime: String,
    pub runtime_handlers: Vec<String>,
    pub runtime_root: PathBuf,
    pub log_dir: PathBuf,
    pub runtime_path: PathBuf,
    pub pause_image: String,
    pub cni_config: CniConfig,
}

impl RuntimeServiceImpl {
    pub fn new(config: RuntimeConfig) -> Self {
        Self::new_with_nri_config(config, NriConfig::default())
    }

    pub fn new_with_nri_config(config: RuntimeConfig, nri_config: NriConfig) -> Self {
        Self::new_with_shim_work_dir(config, nri_config, default_shim_work_dir())
    }

    pub fn new_with_nri_api(
        config: RuntimeConfig,
        nri_config: NriConfig,
        nri: Arc<dyn NriApi>,
    ) -> Self {
        Self::new_with_shim_work_dir_and_nri(config, nri_config, default_shim_work_dir(), Some(nri))
    }

    pub(super) fn new_with_shim_work_dir(
        config: RuntimeConfig,
        nri_config: NriConfig,
        shim_work_dir: PathBuf,
    ) -> Self {
        Self::new_with_shim_work_dir_and_nri(config, nri_config, shim_work_dir, None)
    }

    pub(super) fn new_with_shim_work_dir_and_nri(
        config: RuntimeConfig,
        nri_config: NriConfig,
        shim_work_dir: PathBuf,
        injected_nri: Option<Arc<dyn NriApi>>,
    ) -> Self {
        let nri_manager_config = NriManagerConfig::from(nri_config.clone());
        let containers = Arc::new(Mutex::new(HashMap::new()));
        let pod_sandboxes = Arc::new(Mutex::new(HashMap::new()));
        let mut config = config;
        let mut handlers = Vec::new();
        for handler in &config.runtime_handlers {
            let trimmed = handler.trim();
            if !trimmed.is_empty() && !handlers.iter().any(|existing: &String| existing == trimmed)
            {
                handlers.push(trimmed.to_string());
            }
        }
        if !handlers.iter().any(|handler| handler == &config.runtime) {
            handlers.push(config.runtime.clone());
        }
        config.runtime_handlers = handlers;

        let shim_config = ShimConfig {
            runtime_path: config.runtime_path.clone(),
            work_dir: shim_work_dir,
            debug: std::env::var("CRIUS_SHIM_DEBUG")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            shim_path: std::env::var("CRIUS_SHIM_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    let local = PathBuf::from("/root/crius/target/debug/crius-shim");
                    if local.exists() {
                        local
                    } else {
                        PathBuf::from("crius-shim")
                    }
                }),
        };
        let resolved_shim_work_dir = shim_config.work_dir.clone();

        let runtime = RuncRuntime::with_shim_and_image_storage(
            config.runtime_path.clone(),
            config.runtime_root.clone(),
            config.root_dir.join("storage"),
            shim_config,
        );

        let pod_manager = PodSandboxManager::new(
            runtime.clone(),
            config.root_dir.join("pods"),
            config.pause_image.clone(),
            config.cni_config.clone(),
        );
        let persistence_config = PersistenceConfig {
            db_path: config.root_dir.join("crius.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        };
        let persistence = PersistenceManager::new(persistence_config)
            .expect("Failed to create persistence manager");
        let persistence = Arc::new(Mutex::new(persistence));
        let (events, _) = tokio::sync::broadcast::channel(256);
        let nri: Arc<dyn NriApi> = injected_nri.unwrap_or_else(|| {
            if nri_manager_config.enable {
                Arc::new(NriManager::with_domain(
                    nri_manager_config,
                    Arc::new(NriRuntimeDomain {
                        containers: containers.clone(),
                        pod_sandboxes: pod_sandboxes.clone(),
                        config: config.clone(),
                        nri_config: nri_config.clone(),
                        runtime: runtime.clone(),
                        persistence: persistence.clone(),
                        events: events.clone(),
                    }),
                ))
            } else {
                Arc::new(NopNri)
            }
        });
        let runtime_network_config = Self::load_runtime_network_config(&config.root_dir)
            .unwrap_or_else(|e| {
                log::warn!(
                    "Failed to load runtime network config from {}: {}",
                    config.root_dir.display(),
                    e
                );
                None
            });

        Self {
            containers,
            pod_sandboxes,
            config,
            nri_config,
            nri,
            runtime,
            pod_manager: tokio::sync::Mutex::new(pod_manager),
            persistence,
            streaming: Arc::new(Mutex::new(None)),
            events,
            shim_work_dir: resolved_shim_work_dir,
            runtime_network_config: Arc::new(Mutex::new(runtime_network_config)),
            exit_monitors: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn initialize_nri(&self) -> Result<(), Status> {
        if !self.nri_config.enable {
            return Ok(());
        }
        log::info!("Initializing NRI");
        self.nri
            .start()
            .await
            .map_err(|e| Status::internal(format!("Failed to start NRI: {}", e)))?;
        log::info!("NRI started");
        self.nri
            .synchronize()
            .await
            .map_err(|e| Status::internal(format!("Failed to synchronize NRI: {}", e)))?;
        log::info!("NRI synchronized");
        Ok(())
    }

    pub fn nri_handle(&self) -> Arc<dyn NriApi> {
        self.nri.clone()
    }

    pub async fn set_streaming_server(&self, streaming_server: StreamingServer) {
        let mut streaming = self.streaming.lock().await;
        *streaming = Some(streaming_server);
    }
}
