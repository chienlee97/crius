use std::cmp::Ordering;
use std::sync::Arc;
use async_trait::async_trait;
use protobuf::{Enum, MessageField};
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::nri::{
    NriApi, NriContainerEvent, NriError, NriManagerConfig, NriPodEvent, PluginTtrpcClient, Result,
    RuntimeTtrpcServer,
};
use crate::nri_proto::api as nri_api;

#[derive(Debug, Clone)]
pub struct PluginRecord {
    pub name: String,
    pub index: String,
    pub events_mask: i32,
    pub socket_path: String,
}

#[async_trait]
trait PluginRpc: Send + Sync {
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
}

#[async_trait]
impl crate::nri::transport::RuntimeServiceHandler for ManagerRuntimeHandler {
    async fn register_plugin(&self, req: nri_api::RegisterPluginRequest) -> Result<nri_api::Empty> {
        let mut state = self.state.write().await;
        upsert_plugin(
            &mut state.plugins,
            PluginRecord {
                name: req.plugin_name,
                index: req.plugin_idx,
                events_mask: 0,
                socket_path: String::new(),
            },
            true,
            None,
        );
        Ok(nri_api::Empty::new())
    }

    async fn update_containers(
        &self,
        _req: nri_api::UpdateContainersRequest,
    ) -> Result<nri_api::UpdateContainersResponse> {
        Ok(nri_api::UpdateContainersResponse::new())
    }
}

pub struct NriManager {
    config: NriManagerConfig,
    runtime_server: RuntimeTtrpcServer,
    state: Arc<RwLock<ManagerState>>,
    plugin_sync_gate: Arc<RwLock<()>>,
}

impl NriManager {
    pub fn new(config: NriManagerConfig) -> Self {
        let state = Arc::new(RwLock::new(ManagerState::default()));
        let runtime_handler = Arc::new(ManagerRuntimeHandler {
            state: state.clone(),
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
        let configure_resp = self.call_with_timeout(client.configure(&configure_req)).await?;
        record.events_mask = configure_resp.events;

        let mut state = self.state.write().await;
        upsert_plugin(&mut state.plugins, record, false, Some(client));
        Ok(())
    }

    async fn call_with_timeout<T, F>(&self, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        if self.config.request_timeout.is_zero() {
            return future.await;
        }
        timeout(self.config.request_timeout, future)
            .await
            .map_err(|_| {
                NriError::Plugin(format!(
                    "NRI plugin request timed out after {:?}",
                    self.config.request_timeout
                ))
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
            let result = self.call_with_timeout(plugin.client.state_change(&state_event)).await;
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
            let result = self.call_with_timeout(plugin.client.state_change(&req)).await;
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
        let plugins = self.plugins_for_event(nri_api::Event::CREATE_CONTAINER).await;
        for plugin in plugins {
            let mut req = nri_api::CreateContainerRequest::new();
            req.container = MessageField::some(container_from_event(event_payload));
            let result = self.call_with_timeout(plugin.client.create_container(&req)).await;
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
        let plugins = self.plugins_for_event(nri_api::Event::UPDATE_CONTAINER).await;
        for plugin in plugins {
            let mut req = nri_api::UpdateContainerRequest::new();
            req.container = MessageField::some(container_from_event(event_payload));
            let result = self.call_with_timeout(plugin.client.update_container(&req)).await;
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
            let result = self.call_with_timeout(plugin.client.stop_container(&req)).await;
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
            .filter(|plugin| plugin.registered && plugin.client.is_some())
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
}

struct DispatchPlugin {
    record: PluginRecord,
    client: Arc<dyn PluginRpc>,
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
        }
    } else {
        plugins.push(ManagedPlugin {
            record,
            registered,
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

#[async_trait]
impl NriApi for NriManager {
    async fn start(&self) -> Result<()> {
        if !self.config.enable {
            return Ok(());
        }
        self.runtime_server.start().await
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
            let result = self.call_with_timeout(plugin.client.synchronize(&sync_req)).await;
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::Duration;

    use tokio::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakePluginClient {
        events_mask: i32,
        calls: Mutex<Vec<String>>,
        fail_state_change: bool,
        shutdown_calls: AtomicUsize,
    }

    #[async_trait]
    impl PluginRpc for FakePluginClient {
        async fn synchronize(
            &self,
            _req: &nri_api::SynchronizeRequest,
        ) -> Result<nri_api::SynchronizeResponse> {
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
            if self.fail_state_change {
                return Err(NriError::Transport("socket closed".to_string()));
            }
            let event = req
                .event
                .enum_value()
                .map(|event| format!("{event:?}"))
                .unwrap_or_else(|_| "UNKNOWN".to_string());
            self.calls.lock().await.push(format!("state_change:{event}"));
            Ok(nri_api::Empty::new())
        }

        async fn shutdown(&self) -> Result<()> {
            self.shutdown_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    fn manager_for_tests() -> NriManager {
        NriManager::new(NriManagerConfig {
            enable: true,
            runtime_name: "crius".to_string(),
            runtime_version: "test".to_string(),
            socket_path: "unix:///tmp/crius-nri-manager-tests.sock".to_string(),
            plugin_path: String::new(),
            plugin_config_path: String::new(),
            registration_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            enable_external_connections: false,
        })
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
            })
            .await;
        manager
            .register_plugin(PluginRecord {
                name: "a".to_string(),
                index: "2".to_string(),
                events_mask: 0,
                socket_path: String::new(),
            })
            .await;
        manager
            .register_plugin(PluginRecord {
                name: "c".to_string(),
                index: "2".to_string(),
                events_mask: 0,
                socket_path: String::new(),
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
        let mut unsubscribed_client = FakePluginClient::default();
        unsubscribed_client.events_mask = 0;
        let unsubscribed = Arc::new(unsubscribed_client);
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
            .create_container(NriContainerEvent {
                pod_id: "pod-1".to_string(),
                container_id: "ctr-1".to_string(),
                annotations: HashMap::new(),
            })
            .await
            .expect("dispatch should succeed");

        let subscribed_calls = subscribed.calls.lock().await.clone();
        let unsubscribed_calls = unsubscribed.calls.lock().await.clone();
        assert_eq!(subscribed_calls, vec!["create"]);
        assert!(unsubscribed_calls.is_empty());
    }

    #[tokio::test]
    async fn transport_error_cleans_up_plugin() {
        let manager = manager_for_tests();
        let create_bit = 1_i32 << (nri_api::Event::POST_START_CONTAINER as u32);
        let failing = Arc::new(FakePluginClient {
            events_mask: create_bit,
            fail_state_change: true,
            ..Default::default()
        });

        insert_plugin_for_test(&manager, "bad", "1", create_bit, true, failing).await;

        manager
            .post_start_container(NriContainerEvent::default())
            .await
            .expect("transport failure should be tolerated");

        let plugins = manager.registered_plugins().await;
        assert!(plugins.is_empty());
    }
}
