use async_trait::async_trait;
use nix::libc;
use protobuf::{Enum, MessageField};
use std::cmp::Ordering;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use tokio::net::UnixListener as TokioUnixListener;
use tokio::sync::{oneshot, Mutex, Notify, OwnedRwLockReadGuard, RwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::nri::{
    multiplex_connection, NopNri, NriApi, NriContainerEvent, NriDomain, NriError, NriManagerConfig,
    NriPodEvent, PluginTtrpcClient, Result, RuntimeTtrpcServer,
};
use crate::nri_proto::api as nri_api;

#[derive(Debug, Clone)]
pub struct PluginRecord {
    pub name: String,
    pub index: String,
    pub events_mask: i32,
    pub socket_path: String,
    pub config: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredPlugin {
    name: String,
    index: String,
    path: PathBuf,
    config: String,
}

#[async_trait]
trait PluginRpc: Send + Sync {
    async fn configure(
        &self,
        req: &nri_api::ConfigureRequest,
    ) -> Result<nri_api::ConfigureResponse>;
    async fn synchronize(
        &self,
        req: &nri_api::SynchronizeRequest,
    ) -> Result<nri_api::SynchronizeResponse>;
    async fn create_container(
        &self,
        req: &nri_api::CreateContainerRequest,
    ) -> Result<nri_api::CreateContainerResponse>;
    async fn update_container(
        &self,
        req: &nri_api::UpdateContainerRequest,
    ) -> Result<nri_api::UpdateContainerResponse>;
    async fn stop_container(
        &self,
        req: &nri_api::StopContainerRequest,
    ) -> Result<nri_api::StopContainerResponse>;
    async fn state_change(&self, req: &nri_api::StateChangeEvent) -> Result<nri_api::Empty>;
    async fn shutdown(&self) -> Result<()>;
}

#[async_trait]
impl PluginRpc for PluginTtrpcClient {
    async fn configure(
        &self,
        req: &nri_api::ConfigureRequest,
    ) -> Result<nri_api::ConfigureResponse> {
        self.configure(req).await
    }

    async fn synchronize(
        &self,
        req: &nri_api::SynchronizeRequest,
    ) -> Result<nri_api::SynchronizeResponse> {
        self.synchronize(req).await
    }

    async fn create_container(
        &self,
        req: &nri_api::CreateContainerRequest,
    ) -> Result<nri_api::CreateContainerResponse> {
        self.create_container(req).await
    }

    async fn update_container(
        &self,
        req: &nri_api::UpdateContainerRequest,
    ) -> Result<nri_api::UpdateContainerResponse> {
        self.update_container(req).await
    }

    async fn stop_container(
        &self,
        req: &nri_api::StopContainerRequest,
    ) -> Result<nri_api::StopContainerResponse> {
        self.stop_container(req).await
    }

    async fn state_change(&self, req: &nri_api::StateChangeEvent) -> Result<nri_api::Empty> {
        self.state_change(req).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown().await
    }
}

struct ManagedPlugin {
    record: PluginRecord,
    registered: bool,
    synchronized: bool,
    client: Option<Arc<dyn PluginRpc>>,
}

impl ManagedPlugin {
    fn key_eq(&self, name: &str, index: &str) -> bool {
        self.record.name == name && self.record.index == index
    }
}

#[derive(Default)]
struct ManagerState {
    plugins: Vec<ManagedPlugin>,
}

#[derive(Clone)]
struct ManagerRuntimeHandler {
    state: Arc<RwLock<ManagerState>>,
    domain: Arc<dyn NriDomain>,
    registration_notify: Arc<Notify>,
    client: Option<Arc<dyn PluginRpc>>,
    config: String,
    expected_plugin: Option<(String, String)>,
    registered_tx: Arc<Mutex<Option<oneshot::Sender<(String, String)>>>>,
}

#[async_trait]
impl crate::nri::transport::RuntimeServiceHandler for ManagerRuntimeHandler {
    async fn register_plugin(&self, req: nri_api::RegisterPluginRequest) -> Result<nri_api::Empty> {
        if req.plugin_name.is_empty() {
            return Err(NriError::InvalidInput(
                "plugin name must not be empty".to_string(),
            ));
        }
        if let Some((expected_name, expected_index)) = &self.expected_plugin {
            if req.plugin_name != *expected_name || req.plugin_idx != *expected_index {
                return Err(NriError::InvalidInput(format!(
                    "unexpected plugin registration {}-{}",
                    req.plugin_idx, req.plugin_name
                )));
            }
        }
        let plugin_name = req.plugin_name.clone();
        let plugin_idx = req.plugin_idx.clone();
        let mut state = self.state.write().await;
        upsert_plugin(
            &mut state.plugins,
            PluginRecord {
                name: plugin_name.clone(),
                index: plugin_idx.clone(),
                events_mask: 0,
                socket_path: String::new(),
                config: self.config.clone(),
            },
            true,
            self.client.clone(),
        );
        self.registration_notify.notify_waiters();
        if let Some(tx) = self.registered_tx.lock().await.take() {
            let _ = tx.send((plugin_name, plugin_idx));
        }
        Ok(nri_api::Empty::new())
    }

    async fn update_containers(
        &self,
        req: nri_api::UpdateContainersRequest,
    ) -> Result<nri_api::UpdateContainersResponse> {
        let mut response = nri_api::UpdateContainersResponse::new();
        response.failed = self.domain.apply_updates(&req.update).await?;
        for eviction in &req.evict {
            self.domain
                .evict(&eviction.container_id, &eviction.reason)
                .await?;
        }
        Ok(response)
    }
}

#[derive(Clone)]
pub struct NriManager {
    config: NriManagerConfig,
    runtime_server: RuntimeTtrpcServer,
    state: Arc<RwLock<ManagerState>>,
    plugin_sync_gate: Arc<RwLock<()>>,
    registration_notify: Arc<Notify>,
    launched_plugins: Arc<std::sync::Mutex<Vec<LaunchedPlugin>>>,
    external_listener_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    external_listener_shutdown: Arc<Notify>,
    domain: Arc<dyn NriDomain>,
    connection_servers: Arc<Mutex<Vec<RuntimeTtrpcServer>>>,
}

struct LaunchedPlugin {
    child: Child,
}

impl NriManager {
    pub fn new(config: NriManagerConfig) -> Self {
        Self::with_domain(config, Arc::new(NopNri))
    }

    pub fn with_domain(config: NriManagerConfig, domain: Arc<dyn NriDomain>) -> Self {
        let state = Arc::new(RwLock::new(ManagerState::default()));
        let registration_notify = Arc::new(Notify::new());
        let runtime_handler = Arc::new(ManagerRuntimeHandler {
            state: state.clone(),
            domain: domain.clone(),
            registration_notify: registration_notify.clone(),
            client: None,
            config: String::new(),
            expected_plugin: None,
            registered_tx: Arc::new(Mutex::new(None)),
        });
        let runtime_server = RuntimeTtrpcServer::with_handler(
            config.socket_path.clone(),
            config.registration_timeout,
            config.request_timeout,
            config.enable_external_connections,
            runtime_handler,
        );
        Self {
            config,
            runtime_server,
            state,
            plugin_sync_gate: Arc::new(RwLock::new(())),
            registration_notify,
            launched_plugins: Arc::new(std::sync::Mutex::new(Vec::new())),
            external_listener_task: Arc::new(Mutex::new(None)),
            external_listener_shutdown: Arc::new(Notify::new()),
            domain,
            connection_servers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn register_plugin(&self, plugin: PluginRecord) {
        let mut state = self.state.write().await;
        upsert_plugin(&mut state.plugins, plugin, true, None);
    }

    pub async fn connect_plugin(&self, socket_path: String) -> Result<()> {
        let plugin = PluginRecord {
            name: socket_path.clone(),
            index: String::new(),
            events_mask: 0,
            socket_path,
            config: String::new(),
        };
        self.connect_plugin_with_record(plugin).await
    }

    pub async fn connect_registered_plugin(
        &self,
        name: String,
        index: String,
        socket_path: String,
    ) -> Result<()> {
        let plugin = PluginRecord {
            name,
            index,
            events_mask: 0,
            socket_path,
            config: String::new(),
        };
        self.connect_plugin_with_record(plugin).await
    }

    pub async fn registered_plugins(&self) -> Vec<PluginRecord> {
        let state = self.state.read().await;
        let mut plugins = state
            .plugins
            .iter()
            .filter(|plugin| plugin.registered)
            .map(|plugin| plugin.record.clone())
            .collect::<Vec<_>>();
        sort_plugin_records(&mut plugins);
        plugins
    }

    pub async fn block_plugin_sync(&self) -> PluginSyncBlock {
        PluginSyncBlock {
            guard: Some(self.plugin_sync_gate.clone().read_owned().await),
        }
    }

    async fn connect_plugin_with_record(&self, mut record: PluginRecord) -> Result<()> {
        let client = Arc::new(PluginTtrpcClient::new(
            record.socket_path.clone(),
            self.config.request_timeout,
        ));
        client.connect().await?;

        let mut configure_req = nri_api::ConfigureRequest::new();
        configure_req.runtime_name = self.config.runtime_name.clone();
        configure_req.runtime_version = self.config.runtime_version.clone();
        configure_req.registration_timeout = self.config.registration_timeout.as_millis() as i64;
        configure_req.request_timeout = self.config.request_timeout.as_millis() as i64;
        configure_req.config = record.config.clone();
        let configure_resp = self
            .call_with_deadline(
                self.config.registration_timeout,
                client.configure(&configure_req),
            )
            .await?;
        record.events_mask = configure_resp.events;

        let mut state = self.state.write().await;
        upsert_plugin(&mut state.plugins, record, false, Some(client));
        Ok(())
    }

    async fn call_with_timeout<T, F>(&self, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        self.call_with_deadline(self.config.request_timeout, future)
            .await
    }

    async fn call_with_deadline<T, F>(&self, deadline: std::time::Duration, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        if deadline.is_zero() {
            return future.await;
        }
        timeout(deadline, future).await.map_err(|_| {
            NriError::Plugin(format!("NRI plugin request timed out after {:?}", deadline))
        })?
    }

    async fn dispatch_lifecycle_with_container_event(
        &self,
        event: nri_api::Event,
        event_payload: &NriContainerEvent,
    ) -> Result<()> {
        let _sync_guard = self.plugin_sync_gate.read().await;
        let plugins = self.plugins_for_event(event).await;
        for plugin in plugins {
            let mut state_event = build_state_change_event(event, Some(event_payload));
            let result = self
                .call_with_timeout(plugin.client.state_change(&state_event))
                .await;
            if let Err(err) = result {
                if should_cleanup_plugin(&err) {
                    self.unregister_plugin(&plugin.record.name, &plugin.record.index)
                        .await;
                    continue;
                }
                return Err(err);
            }
            state_event.special_fields.clear();
        }
        Ok(())
    }

    async fn dispatch_lifecycle_with_pod_event(
        &self,
        event: nri_api::Event,
        event_payload: &NriPodEvent,
    ) -> Result<()> {
        let _sync_guard = self.plugin_sync_gate.read().await;
        let plugins = self.plugins_for_event(event).await;
        for plugin in plugins {
            let req = build_state_change_event_for_pod(event, event_payload);
            let result = self
                .call_with_timeout(plugin.client.state_change(&req))
                .await;
            if let Err(err) = result {
                if should_cleanup_plugin(&err) {
                    self.unregister_plugin(&plugin.record.name, &plugin.record.index)
                        .await;
                    continue;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    async fn dispatch_create_container(&self, event_payload: &NriContainerEvent) -> Result<()> {
        let _sync_guard = self.plugin_sync_gate.read().await;
        let plugins = self
            .plugins_for_event(nri_api::Event::CREATE_CONTAINER)
            .await;
        for plugin in plugins {
            let mut req = nri_api::CreateContainerRequest::new();
            req.container = MessageField::some(container_from_event(event_payload));
            let result = self
                .call_with_timeout(plugin.client.create_container(&req))
                .await;
            if let Err(err) = result {
                if should_cleanup_plugin(&err) {
                    self.unregister_plugin(&plugin.record.name, &plugin.record.index)
                        .await;
                    continue;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    async fn dispatch_update_container(&self, event_payload: &NriContainerEvent) -> Result<()> {
        let _sync_guard = self.plugin_sync_gate.read().await;
        let plugins = self
            .plugins_for_event(nri_api::Event::UPDATE_CONTAINER)
            .await;
        for plugin in plugins {
            let mut req = nri_api::UpdateContainerRequest::new();
            req.container = MessageField::some(container_from_event(event_payload));
            let result = self
                .call_with_timeout(plugin.client.update_container(&req))
                .await;
            if let Err(err) = result {
                if should_cleanup_plugin(&err) {
                    self.unregister_plugin(&plugin.record.name, &plugin.record.index)
                        .await;
                    continue;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    async fn dispatch_stop_container(&self, event_payload: &NriContainerEvent) -> Result<()> {
        let _sync_guard = self.plugin_sync_gate.read().await;
        let plugins = self.plugins_for_event(nri_api::Event::STOP_CONTAINER).await;
        for plugin in plugins {
            let mut req = nri_api::StopContainerRequest::new();
            req.container = MessageField::some(container_from_event(event_payload));
            let result = self
                .call_with_timeout(plugin.client.stop_container(&req))
                .await;
            if let Err(err) = result {
                if should_cleanup_plugin(&err) {
                    self.unregister_plugin(&plugin.record.name, &plugin.record.index)
                        .await;
                    continue;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    async fn plugins_for_event(&self, event: nri_api::Event) -> Vec<DispatchPlugin> {
        let state = self.state.read().await;
        let mut plugins = state
            .plugins
            .iter()
            .filter(|plugin| {
                plugin.registered
                    && plugin.synchronized
                    && event_subscribed(plugin.record.events_mask, event)
                    && plugin.client.is_some()
            })
            .filter_map(|plugin| {
                Some(DispatchPlugin {
                    record: plugin.record.clone(),
                    client: plugin.client.clone()?,
                })
            })
            .collect::<Vec<_>>();
        sort_dispatch_plugins(&mut plugins);
        plugins
    }

    async fn plugins_for_synchronize(&self) -> Vec<DispatchPlugin> {
        let state = self.state.read().await;
        let mut plugins = state
            .plugins
            .iter()
            .filter(|plugin| plugin.registered && !plugin.synchronized && plugin.client.is_some())
            .filter_map(|plugin| {
                Some(DispatchPlugin {
                    record: plugin.record.clone(),
                    client: plugin.client.clone()?,
                })
            })
            .collect::<Vec<_>>();
        sort_dispatch_plugins(&mut plugins);
        plugins
    }

    async fn unregister_plugin(&self, name: &str, index: &str) {
        let mut state = self.state.write().await;
        state.plugins.retain(|plugin| !plugin.key_eq(name, index));
    }

    async fn mark_plugin_synchronized(&self, name: &str, index: &str) {
        let mut state = self.state.write().await;
        if let Some(plugin) = state
            .plugins
            .iter_mut()
            .find(|plugin| plugin.key_eq(name, index))
        {
            plugin.synchronized = true;
        }
    }

    fn discover_preinstalled_plugins(&self) -> Result<Vec<DiscoveredPlugin>> {
        discover_preinstalled_plugins(
            Path::new(&self.config.plugin_path),
            Path::new(&self.config.plugin_config_path),
        )
        .map_err(|err| {
            NriError::Plugin(format!(
                "failed to discover plugins in {}: {}",
                self.config.plugin_path, err
            ))
        })
    }

    async fn launch_preinstalled_plugins(&self, plugins: &[DiscoveredPlugin]) -> Result<()> {
        for plugin in plugins {
            let (local, peer) = UnixStream::pair().map_err(io_to_plugin_error)?;
            let (registered_tx, registered_rx) = oneshot::channel();
            self.attach_plugin_connection(
                local,
                Some(plugin.clone()),
                Arc::new(Mutex::new(Some(registered_tx))),
            )
            .await?;
            let child = spawn_preinstalled_plugin_process(plugin, peer)?;

            self.launched_plugins
                .lock()
                .map_err(|_| NriError::Plugin("launched plugin list is poisoned".to_string()))?
                .push(LaunchedPlugin { child });

            let (name, index) = self
                .wait_for_connection_registration(registered_rx)
                .await
                .map_err(|err| {
                    NriError::Plugin(format!(
                        "failed to register launched plugin {}: {}",
                        plugin.path.display(),
                        err
                    ))
                })?;
            self.configure_plugin(&name, &index).await?;
        }

        Ok(())
    }

    async fn attach_plugin_connection(
        &self,
        stream: UnixStream,
        expected_plugin: Option<DiscoveredPlugin>,
        registered_tx: Arc<Mutex<Option<oneshot::Sender<(String, String)>>>>,
    ) -> Result<()> {
        let multiplexed = multiplex_connection(stream, self.config.request_timeout)?;
        let plugin_client: Arc<dyn PluginRpc> = Arc::new(multiplexed.plugin_client);
        let runtime_handler = Arc::new(ManagerRuntimeHandler {
            state: self.state.clone(),
            domain: self.domain.clone(),
            registration_notify: self.registration_notify.clone(),
            client: Some(plugin_client),
            config: expected_plugin
                .as_ref()
                .map(|plugin| plugin.config.clone())
                .unwrap_or_default(),
            expected_plugin: expected_plugin
                .as_ref()
                .map(|plugin| (plugin.name.clone(), plugin.index.clone())),
            registered_tx,
        });
        let runtime_server = RuntimeTtrpcServer::with_handler(
            self.config.socket_path.clone(),
            self.config.registration_timeout,
            self.config.request_timeout,
            self.config.enable_external_connections,
            runtime_handler,
        );
        runtime_server
            .start_with_listener(multiplexed.runtime_listener)
            .await?;
        self.connection_servers.lock().await.push(runtime_server);
        Ok(())
    }

    async fn wait_for_connection_registration(
        &self,
        registered_rx: oneshot::Receiver<(String, String)>,
    ) -> Result<(String, String)> {
        if self.config.registration_timeout.is_zero() {
            return registered_rx
                .await
                .map_err(|_| NriError::Plugin("plugin registration channel closed".to_string()));
        }
        timeout(self.config.registration_timeout, registered_rx)
            .await
            .map_err(|_| NriError::Plugin("plugin registration timed out".to_string()))?
            .map_err(|_| NriError::Plugin("plugin registration channel closed".to_string()))
    }

    async fn configure_plugin(&self, name: &str, index: &str) -> Result<()> {
        let (client, config) = {
            let state = self.state.read().await;
            let plugin = state
                .plugins
                .iter()
                .find(|plugin| plugin.key_eq(name, index))
                .ok_or_else(|| {
                    NriError::Plugin(format!("plugin {index}-{name} is not registered"))
                })?;
            (
                plugin.client.clone().ok_or_else(|| {
                    NriError::Plugin(format!("plugin {index}-{name} is not connected"))
                })?,
                plugin.record.config.clone(),
            )
        };

        let mut configure_req = nri_api::ConfigureRequest::new();
        configure_req.runtime_name = self.config.runtime_name.clone();
        configure_req.runtime_version = self.config.runtime_version.clone();
        configure_req.registration_timeout = self.config.registration_timeout.as_millis() as i64;
        configure_req.request_timeout = self.config.request_timeout.as_millis() as i64;
        configure_req.config = config;
        let configure_resp = self
            .call_with_deadline(
                self.config.registration_timeout,
                client.configure(&configure_req),
            )
            .await?;

        let mut state = self.state.write().await;
        if let Some(plugin) = state
            .plugins
            .iter_mut()
            .find(|plugin| plugin.key_eq(name, index))
        {
            plugin.record.events_mask = configure_resp.events;
        }
        Ok(())
    }

    async fn start_external_listener(&self) -> Result<()> {
        if !self.config.enable_external_connections {
            return Ok(());
        }
        if self.config.socket_path.is_empty() {
            return Err(NriError::InvalidInput(
                "runtime NRI socket path is empty".to_string(),
            ));
        }

        let socket_path = self
            .config
            .socket_path
            .strip_prefix("unix://")
            .unwrap_or(&self.config.socket_path)
            .to_string();
        if let Some(parent) = Path::new(&socket_path).parent() {
            fs::create_dir_all(parent).map_err(io_to_plugin_error)?;
        }
        let _ = fs::remove_file(&socket_path);
        let listener = TokioUnixListener::bind(&socket_path).map_err(io_to_plugin_error)?;
        let manager = self.clone();
        let shutdown = self.external_listener_shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let manager = manager.clone();
                        tokio::spawn(async move {
                            let std_stream = match stream.into_std() {
                                Ok(stream) => stream,
                                Err(err) => {
                                    log::warn!("failed to convert external plugin stream: {}", err);
                                    return;
                                }
                            };
                            let (registered_tx, registered_rx) = oneshot::channel();
                            if let Err(err) = manager
                                .attach_plugin_connection(
                                    std_stream,
                                    None,
                                    Arc::new(Mutex::new(Some(registered_tx))),
                                )
                                .await
                            {
                                log::warn!("failed to attach external plugin connection: {}", err);
                                return;
                            }
                            let (name, index) = match manager.wait_for_connection_registration(registered_rx).await {
                                Ok(registration) => registration,
                                Err(err) => {
                                    log::warn!("external plugin failed to register: {}", err);
                                    return;
                                }
                            };
                            if let Err(err) = manager.configure_plugin(&name, &index).await {
                                log::warn!("failed to configure external plugin {}-{}: {}", index, name, err);
                                manager.unregister_plugin(&name, &index).await;
                                return;
                            }
                            if let Err(err) = manager.synchronize().await {
                                log::warn!("failed to synchronize external plugin {}-{}: {}", index, name, err);
                                manager.unregister_plugin(&name, &index).await;
                            }
                        });
                    }
                }
            }
        });
        *self.external_listener_task.lock().await = Some(task);
        Ok(())
    }

    fn stop_launched_plugins(&self) {
        let mut launched = match self.launched_plugins.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        for launched_plugin in launched.iter_mut() {
            let _ = launched_plugin.child.kill();
            let _ = launched_plugin.child.wait();
        }
        launched.clear();
    }
}

struct DispatchPlugin {
    record: PluginRecord,
    client: Arc<dyn PluginRpc>,
}

pub struct PluginSyncBlock {
    guard: Option<OwnedRwLockReadGuard<()>>,
}

impl PluginSyncBlock {
    pub fn unblock(mut self) {
        self.guard.take();
    }
}

fn upsert_plugin(
    plugins: &mut Vec<ManagedPlugin>,
    record: PluginRecord,
    registered: bool,
    client: Option<Arc<dyn PluginRpc>>,
) {
    if let Some(existing) = plugins
        .iter_mut()
        .find(|plugin| plugin.key_eq(&record.name, &record.index))
    {
        existing.record.socket_path = if record.socket_path.is_empty() {
            existing.record.socket_path.clone()
        } else {
            record.socket_path
        };
        existing.record.events_mask = if record.events_mask == 0 {
            existing.record.events_mask
        } else {
            record.events_mask
        };
        existing.registered = existing.registered || registered;
        if client.is_some() {
            existing.client = client;
            existing.synchronized = false;
        }
    } else {
        plugins.push(ManagedPlugin {
            record,
            registered,
            synchronized: false,
            client,
        });
    }
    plugins.sort_by(|a, b| compare_plugin_order(&a.record, &b.record));
}

fn compare_plugin_order(left: &PluginRecord, right: &PluginRecord) -> Ordering {
    compare_plugin_index(&left.index, &right.index).then_with(|| left.name.cmp(&right.name))
}

fn compare_plugin_index(left: &str, right: &str) -> Ordering {
    match (left.parse::<u32>(), right.parse::<u32>()) {
        (Ok(l), Ok(r)) => l.cmp(&r),
        _ => left.cmp(right),
    }
}

fn sort_plugin_records(plugins: &mut [PluginRecord]) {
    plugins.sort_by(compare_plugin_order);
}

fn sort_dispatch_plugins(plugins: &mut [DispatchPlugin]) {
    plugins.sort_by(|a, b| compare_plugin_order(&a.record, &b.record));
}

fn event_subscribed(events_mask: i32, event: nri_api::Event) -> bool {
    if events_mask <= 0 {
        return false;
    }
    let bit = 1_i32.checked_shl(event.value() as u32);
    bit.is_some_and(|value| (events_mask & value) != 0)
}

fn build_state_change_event(
    event: nri_api::Event,
    event_payload: Option<&NriContainerEvent>,
) -> nri_api::StateChangeEvent {
    let mut req = nri_api::StateChangeEvent::new();
    req.event = event.into();
    if let Some(event_payload) = event_payload {
        req.container = MessageField::some(container_from_event(event_payload));
        if !event_payload.pod_id.is_empty() {
            let mut pod = nri_api::PodSandbox::new();
            pod.id = event_payload.pod_id.clone();
            req.pod = MessageField::some(pod);
        }
    }
    req
}

fn build_state_change_event_for_pod(
    event: nri_api::Event,
    event_payload: &NriPodEvent,
) -> nri_api::StateChangeEvent {
    let mut req = nri_api::StateChangeEvent::new();
    req.event = event.into();
    if !event_payload.pod_id.is_empty() {
        let mut pod = nri_api::PodSandbox::new();
        pod.id = event_payload.pod_id.clone();
        req.pod = MessageField::some(pod);
    }
    req
}

fn container_from_event(event: &NriContainerEvent) -> nri_api::Container {
    let mut container = nri_api::Container::new();
    container.id = event.container_id.clone();
    container.pod_sandbox_id = event.pod_id.clone();
    container.annotations = event.annotations.clone();
    container
}

fn should_cleanup_plugin(err: &NriError) -> bool {
    matches!(err, NriError::Transport(_))
}

fn discover_preinstalled_plugins(
    plugin_dir: &Path,
    config_dir: &Path,
) -> io::Result<Vec<DiscoveredPlugin>> {
    let entries = match fs::read_dir(plugin_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let mut plugins = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };

        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let (index, name) = parse_plugin_name(file_name).map_err(io::Error::other)?;
        let config = load_plugin_config(config_dir, &index, &name)?;
        plugins.push(DiscoveredPlugin {
            name,
            index,
            path,
            config,
        });
    }

    plugins.sort_by(|left, right| {
        compare_plugin_index(&left.index, &right.index).then_with(|| left.name.cmp(&right.name))
    });
    Ok(plugins)
}

fn parse_plugin_name(name: &str) -> std::result::Result<(String, String), String> {
    let Some((index, base)) = name.split_once('-') else {
        return Err(format!(
            "invalid plugin name {name:?}, idx-pluginname expected"
        ));
    };
    let index_bytes = index.as_bytes();
    if index_bytes.len() != 2 || !index_bytes.iter().all(u8::is_ascii_digit) {
        return Err(format!("invalid plugin index {index:?}, must be 2 digits"));
    }
    if base.is_empty() {
        return Err(format!("invalid plugin name {name:?}, missing base name"));
    }
    Ok((index.to_string(), base.to_string()))
}

fn load_plugin_config(config_dir: &Path, index: &str, name: &str) -> io::Result<String> {
    for candidate in [
        config_dir.join(format!("{index}-{name}.conf")),
        config_dir.join(format!("{name}.conf")),
    ] {
        match fs::read_to_string(&candidate) {
            Ok(config) => return Ok(config),
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(String::new())
}

fn io_to_plugin_error(err: io::Error) -> NriError {
    NriError::Plugin(err.to_string())
}

fn spawn_preinstalled_plugin_process(plugin: &DiscoveredPlugin, peer: UnixStream) -> Result<Child> {
    let peer_fd = peer.as_raw_fd();
    let mut cmd = Command::new(&plugin.path);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env("NRI_PLUGIN_NAME", &plugin.name)
        .env("NRI_PLUGIN_IDX", &plugin.index)
        .env("NRI_PLUGIN_SOCKET", "3");
    unsafe {
        cmd.pre_exec(move || {
            if peer_fd != 3 {
                let ret = libc::dup2(peer_fd, 3);
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }

    cmd.spawn().map_err(|err| {
        NriError::Plugin(format!(
            "failed to launch plugin {}: {}",
            plugin.path.display(),
            err
        ))
    })
}

#[async_trait]
impl NriApi for NriManager {
    async fn start(&self) -> Result<()> {
        if !self.config.enable {
            return Ok(());
        }
        self.start_external_listener().await?;
        let plugins = self.discover_preinstalled_plugins()?;
        self.launch_preinstalled_plugins(&plugins).await?;
        self.synchronize().await
    }

    async fn shutdown(&self) -> Result<()> {
        let plugins = {
            let state = self.state.read().await;
            state
                .plugins
                .iter()
                .filter_map(|plugin| plugin.client.clone())
                .collect::<Vec<_>>()
        };
        for client in plugins {
            let _ = self.call_with_timeout(client.shutdown()).await;
        }
        {
            let mut state = self.state.write().await;
            state.plugins.clear();
        }
        self.external_listener_shutdown.notify_waiters();
        if let Some(task) = self.external_listener_task.lock().await.take() {
            task.abort();
        }
        self.stop_launched_plugins();
        let servers = {
            let mut guard = self.connection_servers.lock().await;
            std::mem::take(&mut *guard)
        };
        for server in servers {
            let _ = server.shutdown().await;
        }
        self.runtime_server.shutdown().await
    }

    async fn synchronize(&self) -> Result<()> {
        if !self.config.enable {
            return Ok(());
        }
        let _sync_gate = self.plugin_sync_gate.write().await;
        let plugins = self.plugins_for_synchronize().await;
        let mut sync_req = nri_api::SynchronizeRequest::new();
        sync_req.more = false;
        for plugin in plugins {
            let result = self
                .call_with_timeout(plugin.client.synchronize(&sync_req))
                .await;
            if let Err(err) = result {
                if should_cleanup_plugin(&err) {
                    self.unregister_plugin(&plugin.record.name, &plugin.record.index)
                        .await;
                    continue;
                }
                return Err(err);
            }
            self.mark_plugin_synchronized(&plugin.record.name, &plugin.record.index)
                .await;
        }
        Ok(())
    }

    async fn run_pod_sandbox(&self, event: NriPodEvent) -> Result<()> {
        self.dispatch_lifecycle_with_pod_event(nri_api::Event::RUN_POD_SANDBOX, &event)
            .await
    }

    async fn stop_pod_sandbox(&self, event: NriPodEvent) -> Result<()> {
        self.dispatch_lifecycle_with_pod_event(nri_api::Event::STOP_POD_SANDBOX, &event)
            .await
    }

    async fn remove_pod_sandbox(&self, event: NriPodEvent) -> Result<()> {
        self.dispatch_lifecycle_with_pod_event(nri_api::Event::REMOVE_POD_SANDBOX, &event)
            .await
    }

    async fn create_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_create_container(&event).await
    }

    async fn post_create_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_lifecycle_with_container_event(nri_api::Event::POST_CREATE_CONTAINER, &event)
            .await
    }

    async fn start_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_lifecycle_with_container_event(nri_api::Event::START_CONTAINER, &event)
            .await
    }

    async fn post_start_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_lifecycle_with_container_event(nri_api::Event::POST_START_CONTAINER, &event)
            .await
    }

    async fn update_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_update_container(&event).await
    }

    async fn post_update_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_lifecycle_with_container_event(nri_api::Event::POST_UPDATE_CONTAINER, &event)
            .await
    }

    async fn stop_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_stop_container(&event).await
    }

    async fn remove_container(&self, event: NriContainerEvent) -> Result<()> {
        self.dispatch_lifecycle_with_container_event(nri_api::Event::REMOVE_CONTAINER, &event)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::Duration;

    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    use super::*;
    use crate::nri::transport::RuntimeServiceHandler;

    #[derive(Default)]
    struct FakePluginClient {
        calls: Mutex<Vec<String>>,
        fail_state_change: bool,
        synchronize_delay: Option<Duration>,
        state_change_delay: Option<Duration>,
        shutdown_calls: AtomicUsize,
    }

    #[derive(Default)]
    struct FakeDomain {
        update_calls: Mutex<Vec<Vec<String>>>,
        evictions: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl NriDomain for FakeDomain {
        async fn snapshot(&self) -> Result<()> {
            Ok(())
        }

        async fn apply_updates(
            &self,
            updates: &[nri_api::ContainerUpdate],
        ) -> Result<Vec<nri_api::ContainerUpdate>> {
            let ids = updates
                .iter()
                .map(|update| update.container_id.clone())
                .collect::<Vec<_>>();
            self.update_calls.lock().await.push(ids);
            Ok(Vec::new())
        }

        async fn evict(&self, container_id: &str, reason: &str) -> Result<()> {
            self.evictions
                .lock()
                .await
                .push((container_id.to_string(), reason.to_string()));
            Ok(())
        }
    }

    #[async_trait]
    impl PluginRpc for FakePluginClient {
        async fn configure(
            &self,
            _req: &nri_api::ConfigureRequest,
        ) -> Result<nri_api::ConfigureResponse> {
            self.calls.lock().await.push("configure".to_string());
            Ok(nri_api::ConfigureResponse::new())
        }

        async fn synchronize(
            &self,
            _req: &nri_api::SynchronizeRequest,
        ) -> Result<nri_api::SynchronizeResponse> {
            if let Some(delay) = self.synchronize_delay {
                sleep(delay).await;
            }
            self.calls.lock().await.push("synchronize".to_string());
            Ok(nri_api::SynchronizeResponse::new())
        }

        async fn create_container(
            &self,
            _req: &nri_api::CreateContainerRequest,
        ) -> Result<nri_api::CreateContainerResponse> {
            self.calls.lock().await.push("create".to_string());
            Ok(nri_api::CreateContainerResponse::new())
        }

        async fn update_container(
            &self,
            _req: &nri_api::UpdateContainerRequest,
        ) -> Result<nri_api::UpdateContainerResponse> {
            self.calls.lock().await.push("update".to_string());
            Ok(nri_api::UpdateContainerResponse::new())
        }

        async fn stop_container(
            &self,
            _req: &nri_api::StopContainerRequest,
        ) -> Result<nri_api::StopContainerResponse> {
            self.calls.lock().await.push("stop".to_string());
            Ok(nri_api::StopContainerResponse::new())
        }

        async fn state_change(&self, req: &nri_api::StateChangeEvent) -> Result<nri_api::Empty> {
            if let Some(delay) = self.state_change_delay {
                sleep(delay).await;
            }
            if self.fail_state_change {
                return Err(NriError::Transport("socket closed".to_string()));
            }
            let event = req
                .event
                .enum_value()
                .map(|event| format!("{event:?}"))
                .unwrap_or_else(|_| "UNKNOWN".to_string());
            self.calls
                .lock()
                .await
                .push(format!("state_change:{event}"));
            Ok(nri_api::Empty::new())
        }

        async fn shutdown(&self) -> Result<()> {
            self.shutdown_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    fn manager_for_tests() -> NriManager {
        manager_with_domain_for_tests(Arc::new(NopNri))
    }

    fn manager_with_domain_for_tests(domain: Arc<dyn NriDomain>) -> NriManager {
        NriManager::with_domain(
            NriManagerConfig {
                enable: true,
                runtime_name: "crius".to_string(),
                runtime_version: "test".to_string(),
                socket_path: "unix:///tmp/crius-nri-manager-tests.sock".to_string(),
                plugin_path: String::new(),
                plugin_config_path: String::new(),
                registration_timeout: Duration::from_secs(1),
                request_timeout: Duration::from_secs(1),
                enable_external_connections: false,
            },
            domain,
        )
    }

    async fn insert_plugin_for_test(
        manager: &NriManager,
        name: &str,
        index: &str,
        events_mask: i32,
        registered: bool,
        client: Arc<dyn PluginRpc>,
    ) {
        let mut state = manager.state.write().await;
        upsert_plugin(
            &mut state.plugins,
            PluginRecord {
                name: name.to_string(),
                index: index.to_string(),
                events_mask,
                socket_path: format!("unix:///tmp/{name}.sock"),
                config: String::new(),
            },
            registered,
            Some(client),
        );
    }

    #[tokio::test]
    async fn plugins_are_sorted_by_index_then_name() {
        let manager = manager_for_tests();
        manager
            .register_plugin(PluginRecord {
                name: "b".to_string(),
                index: "10".to_string(),
                events_mask: 0,
                socket_path: String::new(),
                config: String::new(),
            })
            .await;
        manager
            .register_plugin(PluginRecord {
                name: "a".to_string(),
                index: "2".to_string(),
                events_mask: 0,
                socket_path: String::new(),
                config: String::new(),
            })
            .await;
        manager
            .register_plugin(PluginRecord {
                name: "c".to_string(),
                index: "2".to_string(),
                events_mask: 0,
                socket_path: String::new(),
                config: String::new(),
            })
            .await;

        let plugins = manager.registered_plugins().await;
        assert_eq!(plugins.len(), 3);
        assert_eq!(plugins[0].name, "a");
        assert_eq!(plugins[1].name, "c");
        assert_eq!(plugins[2].name, "b");
    }

    #[tokio::test]
    async fn dispatch_only_targets_subscribed_plugins() {
        let manager = manager_for_tests();
        let subscribed = Arc::new(FakePluginClient::default());
        let unsubscribed = Arc::new(FakePluginClient::default());
        let create_bit = 1_i32 << (nri_api::Event::CREATE_CONTAINER as u32);

        insert_plugin_for_test(
            &manager,
            "subscribed",
            "01",
            create_bit,
            true,
            subscribed.clone(),
        )
        .await;
        insert_plugin_for_test(
            &manager,
            "unsubscribed",
            "02",
            0,
            true,
            unsubscribed.clone(),
        )
        .await;
        manager
            .synchronize()
            .await
            .expect("synchronize should activate subscribed plugin");

        manager
            .create_container(NriContainerEvent {
                pod_id: "pod-1".to_string(),
                container_id: "ctr-1".to_string(),
                annotations: HashMap::new(),
            })
            .await
            .expect("dispatch should succeed");

        let subscribed_calls = subscribed.calls.lock().await.clone();
        let unsubscribed_calls = unsubscribed.calls.lock().await.clone();
        assert_eq!(subscribed_calls, vec!["synchronize", "create"]);
        assert_eq!(unsubscribed_calls, vec!["synchronize"]);
    }

    #[tokio::test]
    async fn transport_error_cleans_up_plugin() {
        let manager = manager_for_tests();
        let create_bit = 1_i32 << (nri_api::Event::POST_START_CONTAINER as u32);
        let failing = Arc::new(FakePluginClient {
            fail_state_change: true,
            ..Default::default()
        });

        insert_plugin_for_test(&manager, "bad", "1", create_bit, true, failing).await;
        manager
            .synchronize()
            .await
            .expect("synchronize should activate plugin");

        manager
            .post_start_container(NriContainerEvent::default())
            .await
            .expect("transport failure should be tolerated");

        let plugins = manager.registered_plugins().await;
        assert!(plugins.is_empty());
    }

    #[tokio::test]
    async fn pending_plugin_is_not_active_until_synchronized() {
        let manager = manager_for_tests();
        let create_bit = 1_i32 << (nri_api::Event::CREATE_CONTAINER as u32);
        let plugin = Arc::new(FakePluginClient::default());

        insert_plugin_for_test(&manager, "late", "1", create_bit, true, plugin.clone()).await;

        manager
            .create_container(NriContainerEvent {
                pod_id: "pod-1".to_string(),
                container_id: "ctr-1".to_string(),
                annotations: HashMap::new(),
            })
            .await
            .expect("unsynchronized plugin should be skipped");
        assert!(plugin.calls.lock().await.is_empty());

        manager
            .synchronize()
            .await
            .expect("synchronize should activate plugin");
        manager
            .create_container(NriContainerEvent {
                pod_id: "pod-1".to_string(),
                container_id: "ctr-1".to_string(),
                annotations: HashMap::new(),
            })
            .await
            .expect("synchronized plugin should receive lifecycle event");

        let calls = plugin.calls.lock().await.clone();
        assert_eq!(calls, vec!["synchronize", "create"]);
    }

    #[tokio::test]
    async fn block_plugin_sync_prevents_sync_completion_mid_lifecycle() {
        let manager = Arc::new(manager_for_tests());
        let create_bit = 1_i32 << (nri_api::Event::CREATE_CONTAINER as u32);
        let plugin = Arc::new(FakePluginClient {
            synchronize_delay: Some(Duration::from_millis(20)),
            ..Default::default()
        });

        insert_plugin_for_test(
            manager.as_ref(),
            "late",
            "1",
            create_bit,
            true,
            plugin.clone(),
        )
        .await;

        let block = manager.block_plugin_sync().await;
        let sync_manager = manager.clone();
        let sync_task = tokio::spawn(async move { sync_manager.synchronize().await });

        sleep(Duration::from_millis(50)).await;
        assert!(plugin.calls.lock().await.is_empty());

        block.unblock();
        sync_task
            .await
            .expect("join should succeed")
            .expect("synchronize should succeed");

        manager
            .create_container(NriContainerEvent {
                pod_id: "pod-1".to_string(),
                container_id: "ctr-1".to_string(),
                annotations: HashMap::new(),
            })
            .await
            .expect("plugin should be active after sync completes");
        let calls = plugin.calls.lock().await.clone();
        assert_eq!(calls, vec!["synchronize", "create"]);
    }

    #[tokio::test]
    async fn request_timeout_is_reported_without_unregistering_plugin() {
        let manager = NriManager::with_domain(
            NriManagerConfig {
                enable: true,
                runtime_name: "crius".to_string(),
                runtime_version: "test".to_string(),
                socket_path: "unix:///tmp/crius-nri-manager-timeout-tests.sock".to_string(),
                plugin_path: String::new(),
                plugin_config_path: String::new(),
                registration_timeout: Duration::from_secs(1),
                request_timeout: Duration::from_millis(10),
                enable_external_connections: false,
            },
            Arc::new(NopNri),
        );
        let event_bit = 1_i32 << (nri_api::Event::POST_START_CONTAINER as u32);
        let slow = Arc::new(FakePluginClient {
            state_change_delay: Some(Duration::from_millis(50)),
            ..Default::default()
        });

        insert_plugin_for_test(&manager, "slow", "1", event_bit, true, slow).await;
        manager
            .synchronize()
            .await
            .expect("synchronize should activate plugin");

        let err = manager
            .post_start_container(NriContainerEvent::default())
            .await
            .expect_err("slow plugin should time out");
        match err {
            NriError::Plugin(msg) => assert!(msg.contains("timed out")),
            other => panic!("unexpected error: {other:?}"),
        }

        let plugins = manager.registered_plugins().await;
        assert_eq!(plugins.len(), 1);
    }

    #[tokio::test]
    async fn runtime_handler_dispatches_unsolicited_updates_and_evictions() {
        let domain = Arc::new(FakeDomain::default());
        let handler = ManagerRuntimeHandler {
            state: Arc::new(RwLock::new(ManagerState::default())),
            domain: domain.clone(),
            registration_notify: Arc::new(Notify::new()),
            client: None,
            config: String::new(),
            expected_plugin: None,
            registered_tx: Arc::new(Mutex::new(None)),
        };
        let mut req = nri_api::UpdateContainersRequest::new();
        let mut update = nri_api::ContainerUpdate::new();
        update.container_id = "ctr-1".to_string();
        req.update.push(update);
        let mut eviction = nri_api::ContainerEviction::new();
        eviction.container_id = "ctr-2".to_string();
        eviction.reason = "policy".to_string();
        req.evict.push(eviction);

        let response = handler
            .update_containers(req)
            .await
            .expect("update dispatch should succeed");

        assert!(response.failed.is_empty());
        assert_eq!(
            domain.update_calls.lock().await.clone(),
            vec![vec!["ctr-1".to_string()]]
        );
        assert_eq!(
            domain.evictions.lock().await.clone(),
            vec![("ctr-2".to_string(), "policy".to_string())]
        );
    }

    #[tokio::test]
    async fn shutdown_notifies_connected_plugins() {
        let manager = manager_for_tests();
        let plugin = Arc::new(FakePluginClient::default());
        insert_plugin_for_test(&manager, "p1", "1", 0, true, plugin.clone()).await;

        manager.shutdown().await.expect("shutdown should succeed");

        assert_eq!(plugin.shutdown_calls.load(AtomicOrdering::SeqCst), 1);
        assert!(manager.registered_plugins().await.is_empty());
    }

    #[test]
    fn discover_preinstalled_plugins_loads_specific_config_and_sorts() {
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("plugins");
        let config_dir = dir.path().join("conf.d");
        fs::create_dir_all(&plugin_dir).unwrap();
        fs::create_dir_all(&config_dir).unwrap();

        let plugin_b = plugin_dir.join("20-beta");
        fs::write(&plugin_b, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&plugin_b).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&plugin_b, perms).unwrap();

        let plugin_a = plugin_dir.join("01-alpha");
        fs::write(&plugin_a, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&plugin_a).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&plugin_a, perms).unwrap();

        fs::write(plugin_dir.join("README.txt"), "ignore me").unwrap();
        fs::write(config_dir.join("alpha.conf"), "base-alpha").unwrap();
        fs::write(config_dir.join("01-alpha.conf"), "specific-alpha").unwrap();
        fs::write(config_dir.join("beta.conf"), "base-beta").unwrap();

        let discovered = discover_preinstalled_plugins(&plugin_dir, &config_dir).unwrap();
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].index, "01");
        assert_eq!(discovered[0].name, "alpha");
        assert_eq!(discovered[0].config, "specific-alpha");
        assert_eq!(discovered[1].index, "20");
        assert_eq!(discovered[1].name, "beta");
        assert_eq!(discovered[1].config, "base-beta");
    }

    #[tokio::test]
    async fn spawn_preinstalled_plugin_process_sets_nri_env() {
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("plugins");
        let output_path = dir.path().join("plugin-env.txt");
        fs::create_dir_all(&plugin_dir).unwrap();

        let plugin_path = plugin_dir.join("01-envdump");
        fs::write(
            &plugin_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n%s\\n%s\\n' \"$NRI_PLUGIN_NAME\" \"$NRI_PLUGIN_IDX\" \"$NRI_PLUGIN_SOCKET\" > '{}'\ntest -e /proc/self/fd/3 && echo fd3=open >> '{}'\n",
                output_path.display(),
                output_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&plugin_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&plugin_path, perms).unwrap();

        let plugins =
            discover_preinstalled_plugins(&plugin_dir, &dir.path().join("conf.d")).unwrap();
        let (_local, peer) = UnixStream::pair().unwrap();
        let mut child = spawn_preinstalled_plugin_process(&plugins[0], peer)
            .expect("plugin launch should succeed");
        sleep(Duration::from_millis(50)).await;
        let _ = child.kill();
        let _ = child.wait();

        let output = fs::read_to_string(&output_path).unwrap();
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines[0], "envdump");
        assert_eq!(lines[1], "01");
        assert_eq!(lines[2], "3");
        assert!(lines.iter().any(|line| *line == "fd3=open"));
    }
}
