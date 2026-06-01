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
    synchronize_snapshots: Mutex<Vec<(Vec<String>, Vec<String>)>>,
    synchronize_more_flags: Mutex<Vec<bool>>,
    validate_requests: Mutex<Vec<nri_api::ValidateContainerAdjustmentRequest>>,
    fail_state_change: bool,
    synchronize_delay: Option<Duration>,
    synchronize_response: Option<nri_api::SynchronizeResponse>,
    synchronize_echo_more: bool,
    state_change_delay: Option<Duration>,
    shutdown_calls: AtomicUsize,
    create_response: Option<nri_api::CreateContainerResponse>,
    update_response: Option<nri_api::UpdateContainerResponse>,
    stop_response: Option<nri_api::StopContainerResponse>,
}

#[derive(Default)]
struct FakeDomain {
    snapshot: crate::nri::domain::RuntimeSnapshot,
    update_calls: Mutex<Vec<Vec<String>>>,
    evictions: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl NriDomain for FakeDomain {
    async fn snapshot(&self) -> Result<crate::nri::domain::RuntimeSnapshot> {
        Ok(self.snapshot.clone())
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
        req: &nri_api::SynchronizeRequest,
    ) -> Result<nri_api::SynchronizeResponse> {
        if let Some(delay) = self.synchronize_delay {
            sleep(delay).await;
        }
        self.synchronize_more_flags.lock().await.push(req.more);
        self.synchronize_snapshots.lock().await.push((
            req.pods.iter().map(|pod| pod.id.clone()).collect(),
            req.containers
                .iter()
                .map(|container| container.id.clone())
                .collect(),
        ));
        self.calls.lock().await.push("synchronize".to_string());
        let mut response = self.synchronize_response.clone().unwrap_or_default();
        if self.synchronize_echo_more {
            response.more = req.more;
        }
        Ok(response)
    }

    async fn create_container(
        &self,
        _req: &nri_api::CreateContainerRequest,
    ) -> Result<nri_api::CreateContainerResponse> {
        self.calls.lock().await.push("create".to_string());
        Ok(self.create_response.clone().unwrap_or_default())
    }

    async fn update_container(
        &self,
        _req: &nri_api::UpdateContainerRequest,
    ) -> Result<nri_api::UpdateContainerResponse> {
        self.calls.lock().await.push("update".to_string());
        Ok(self.update_response.clone().unwrap_or_default())
    }

    async fn stop_container(
        &self,
        _req: &nri_api::StopContainerRequest,
    ) -> Result<nri_api::StopContainerResponse> {
        self.calls.lock().await.push("stop".to_string());
        Ok(self.stop_response.clone().unwrap_or_default())
    }

    async fn update_pod_sandbox(
        &self,
        _req: &nri_api::UpdatePodSandboxRequest,
    ) -> Result<nri_api::UpdatePodSandboxResponse> {
        self.calls.lock().await.push("update_pod".to_string());
        Ok(nri_api::UpdatePodSandboxResponse::new())
    }

    async fn validate_container_adjustment(
        &self,
        req: &nri_api::ValidateContainerAdjustmentRequest,
    ) -> Result<nri_api::ValidateContainerAdjustmentResponse> {
        self.calls.lock().await.push("validate".to_string());
        self.validate_requests.lock().await.push(req.clone());
        Ok(nri_api::ValidateContainerAdjustmentResponse::new())
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
            enable_cdi: true,
            cdi_spec_dirs: vec!["/etc/cdi".to_string(), "/var/run/cdi".to_string()],
            runtime_name: "crius".to_string(),
            runtime_version: "test".to_string(),
            socket_path: "unix:///tmp/crius-nri-manager-tests.sock".to_string(),
            plugin_path: String::new(),
            plugin_config_path: String::new(),
            registration_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            enable_external_connections: false,
            default_validator: Default::default(),
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

fn test_container_event() -> NriContainerEvent {
    let mut pod = nri_api::PodSandbox::new();
    pod.id = "pod-1".to_string();

    let mut container = nri_api::Container::new();
    container.id = "ctr-1".to_string();
    container.pod_sandbox_id = "pod-1".to_string();
    container.annotations = HashMap::new();

    NriContainerEvent {
        pod: Some(pod),
        container,
        linux_resources: None,
    }
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
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
    let unrelated_bit = event_mask_bit(nri_api::Event::REMOVE_CONTAINER).unwrap();

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
        unrelated_bit,
        true,
        unsubscribed.clone(),
    )
    .await;
    manager
        .synchronize()
        .await
        .expect("synchronize should activate subscribed plugin");

    manager
        .create_container(test_container_event())
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
    let create_bit = event_mask_bit(nri_api::Event::POST_START_CONTAINER).unwrap();
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
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
    let plugin = Arc::new(FakePluginClient {
        synchronize_echo_more: true,
        ..Default::default()
    });

    insert_plugin_for_test(&manager, "late", "1", create_bit, true, plugin.clone()).await;

    manager
        .create_container(test_container_event())
        .await
        .expect("unsynchronized plugin should be skipped");
    assert!(plugin.calls.lock().await.is_empty());

    manager
        .synchronize()
        .await
        .expect("synchronize should activate plugin");
    manager
        .create_container(test_container_event())
        .await
        .expect("synchronized plugin should receive lifecycle event");

    let calls = plugin.calls.lock().await.clone();
    assert_eq!(calls, vec!["synchronize", "create"]);
}

#[tokio::test]
async fn update_pod_sandbox_targets_subscribed_plugins() {
    let manager = manager_for_tests();
    let subscribed = Arc::new(FakePluginClient::default());
    let unsubscribed = Arc::new(FakePluginClient::default());
    let event_bit = event_mask_bit(nri_api::Event::UPDATE_POD_SANDBOX).unwrap();
    let unrelated_bit = event_mask_bit(nri_api::Event::REMOVE_CONTAINER).unwrap();

    insert_plugin_for_test(
        &manager,
        "subscribed",
        "01",
        event_bit,
        true,
        subscribed.clone(),
    )
    .await;
    insert_plugin_for_test(
        &manager,
        "unsubscribed",
        "02",
        unrelated_bit,
        true,
        unsubscribed.clone(),
    )
    .await;
    manager
        .synchronize()
        .await
        .expect("synchronize should activate subscribed plugin");

    manager
        .update_pod_sandbox(NriPodEvent::default())
        .await
        .expect("dispatch should succeed");

    assert_eq!(
        subscribed.calls.lock().await.clone(),
        vec!["synchronize", "update_pod"]
    );
    assert_eq!(unsubscribed.calls.lock().await.clone(), vec!["synchronize"]);
}

#[tokio::test]
async fn configure_events_zero_defaults_to_all_events() {
    let manager = manager_for_tests();
    let plugin = Arc::new(FakePluginClient {
        synchronize_echo_more: true,
        ..Default::default()
    });

    {
        let mut state = manager.state.write().await;
        upsert_plugin(
            &mut state.plugins,
            PluginRecord {
                name: "all-events".to_string(),
                index: "01".to_string(),
                events_mask: 0,
                socket_path: String::new(),
                config: String::new(),
            },
            true,
            Some(plugin.clone()),
        );
    }

    manager
        .synchronize()
        .await
        .expect("synchronize should activate plugin");
    manager
        .post_start_container(NriContainerEvent::default())
        .await
        .expect("post start should be dispatched");

    assert_eq!(
        plugin.calls.lock().await.clone(),
        vec!["synchronize", "state_change:POST_START_CONTAINER"]
    );
}

#[tokio::test]
async fn post_update_pod_sandbox_targets_subscribed_plugins() {
    let manager = manager_for_tests();
    let subscribed = Arc::new(FakePluginClient::default());
    let unsubscribed = Arc::new(FakePluginClient::default());
    let event_bit = event_mask_bit(nri_api::Event::POST_UPDATE_POD_SANDBOX).unwrap();
    let unrelated_bit = event_mask_bit(nri_api::Event::REMOVE_CONTAINER).unwrap();

    insert_plugin_for_test(
        &manager,
        "subscribed",
        "01",
        event_bit,
        true,
        subscribed.clone(),
    )
    .await;
    insert_plugin_for_test(
        &manager,
        "unsubscribed",
        "02",
        unrelated_bit,
        true,
        unsubscribed.clone(),
    )
    .await;
    manager
        .synchronize()
        .await
        .expect("synchronize should activate subscribed plugin");

    manager
        .post_update_pod_sandbox(NriPodEvent::default())
        .await
        .expect("dispatch should succeed");

    assert_eq!(
        subscribed.calls.lock().await.clone(),
        vec!["synchronize", "state_change:POST_UPDATE_POD_SANDBOX"]
    );
    assert_eq!(unsubscribed.calls.lock().await.clone(), vec!["synchronize"]);
}

#[tokio::test]
async fn synchronize_splits_snapshot_and_only_accepts_updates_on_final_chunk() {
    let snapshot = crate::nri::domain::RuntimeSnapshot {
        pods: (0..5)
            .map(|idx| {
                let mut pod = nri_api::PodSandbox::new();
                pod.id = format!("pod-{idx}");
                pod
            })
            .collect(),
        containers: (0..4)
            .map(|idx| {
                let mut container = nri_api::Container::new();
                container.id = format!("ctr-{idx}");
                container
            })
            .collect(),
    };

    let plugin = Arc::new(FakePluginClient {
        synchronize_echo_more: true,
        ..Default::default()
    });
    let manager = manager_with_domain_for_tests(Arc::new(FakeDomain {
        snapshot,
        ..Default::default()
    }));

    insert_plugin_for_test(
        &manager,
        "sync",
        "01",
        event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap(),
        true,
        plugin.clone(),
    )
    .await;

    manager
        .synchronize()
        .await
        .expect("synchronize should succeed");

    let more_flags = plugin.synchronize_more_flags.lock().await.clone();
    assert_eq!(more_flags, vec![true, false]);

    let snapshots = plugin.synchronize_snapshots.lock().await.clone();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].0.len() + snapshots[0].1.len(), 8);
    assert_eq!(snapshots[1].0.len() + snapshots[1].1.len(), 1);
}

#[tokio::test]
async fn register_plugin_rejects_invalid_index() {
    let state = Arc::new(RwLock::new(ManagerState::default()));
    let handler = ManagerRuntimeHandler {
        state,
        domain: Arc::new(NopNri),
        registration_notify: Arc::new(Notify::new()),
        client: None,
        config: String::new(),
        plugin_config_path: PathBuf::new(),
        expected_plugin: None,
        registered_tx: Arc::new(Mutex::new(None)),
        connection_identity: Arc::new(Mutex::new(None)),
    };

    let mut req = nri_api::RegisterPluginRequest::new();
    req.plugin_name = "broken".to_string();
    req.plugin_idx = "1".to_string();

    let err = handler.register_plugin(req).await.unwrap_err();
    match err {
        NriError::InvalidInput(message) => {
            assert!(message.contains("must be 2 digits"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn stop_container_merges_plugin_updates() {
    let manager = manager_for_tests();
    let stop_bit = event_mask_bit(nri_api::Event::STOP_CONTAINER).unwrap();

    let mut first_update = nri_api::ContainerUpdate::new();
    first_update.container_id = "sidecar".to_string();
    let mut first_linux = nri_api::LinuxContainerUpdate::new();
    let mut first_resources = nri_api::LinuxResources::new();
    let mut first_cpu = nri_api::LinuxCPU::new();
    let mut shares = nri_api::OptionalUInt64::new();
    shares.value = 512;
    first_cpu.shares = MessageField::some(shares);
    first_resources.cpu = MessageField::some(first_cpu);
    first_linux.resources = MessageField::some(first_resources);
    first_update.linux = MessageField::some(first_linux);
    let mut first_response = nri_api::StopContainerResponse::new();
    first_response.update.push(first_update);

    let mut second_update = nri_api::ContainerUpdate::new();
    second_update.container_id = "sidecar".to_string();
    let mut second_linux = nri_api::LinuxContainerUpdate::new();
    let mut second_resources = nri_api::LinuxResources::new();
    let mut second_memory = nri_api::LinuxMemory::new();
    let mut limit = nri_api::OptionalInt64::new();
    limit.value = 4096;
    second_memory.limit = MessageField::some(limit);
    second_resources.memory = MessageField::some(second_memory);
    second_linux.resources = MessageField::some(second_resources);
    second_update.linux = MessageField::some(second_linux);
    let mut second_response = nri_api::StopContainerResponse::new();
    second_response.update.push(second_update);

    let first = Arc::new(FakePluginClient {
        stop_response: Some(first_response),
        ..Default::default()
    });
    let second = Arc::new(FakePluginClient {
        stop_response: Some(second_response),
        ..Default::default()
    });

    insert_plugin_for_test(&manager, "first", "01", stop_bit, true, first.clone()).await;
    insert_plugin_for_test(&manager, "second", "02", stop_bit, true, second.clone()).await;
    manager
        .synchronize()
        .await
        .expect("synchronize should activate stop plugins");

    let result = manager
        .stop_container(test_container_event())
        .await
        .expect("stop dispatch should succeed");

    assert_eq!(
        first.calls.lock().await.clone(),
        vec!["synchronize", "stop"]
    );
    assert_eq!(
        second.calls.lock().await.clone(),
        vec!["synchronize", "stop"]
    );
    assert_eq!(result.updates.len(), 1);
    let update = &result.updates[0];
    assert_eq!(update.container_id, "sidecar");
    let resources = update
        .linux
        .as_ref()
        .and_then(|linux| linux.resources.as_ref())
        .expect("merged resources should be present");
    assert_eq!(
        resources
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(512)
    );
    assert_eq!(
        resources
            .memory
            .as_ref()
            .and_then(|memory| memory.limit.as_ref())
            .map(|limit| limit.value),
        Some(4096)
    );
}

#[tokio::test]
async fn create_container_merges_plugin_responses_and_validates() {
    let manager = manager_for_tests();
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
    let validate_bit = event_mask_bit(nri_api::Event::VALIDATE_CONTAINER_ADJUSTMENT).unwrap();

    let mut first_response = nri_api::CreateContainerResponse::new();
    let mut first_adjust = nri_api::ContainerAdjustment::new();
    first_adjust
        .annotations
        .insert("a".to_string(), "1".to_string());
    first_response.adjust = MessageField::some(first_adjust);
    let mut update = nri_api::ContainerUpdate::new();
    update.container_id = "u1".to_string();
    let mut linux = nri_api::LinuxContainerUpdate::new();
    let mut resources = nri_api::LinuxResources::new();
    let mut cpu = nri_api::LinuxCPU::new();
    let mut shares = nri_api::OptionalUInt64::new();
    shares.value = 128;
    cpu.shares = MessageField::some(shares);
    resources.cpu = MessageField::some(cpu);
    linux.resources = MessageField::some(resources);
    update.linux = MessageField::some(linux);
    first_response.update.push(update);
    let first = Arc::new(FakePluginClient {
        create_response: Some(first_response),
        ..Default::default()
    });

    let mut second_response = nri_api::CreateContainerResponse::new();
    let mut second_adjust = nri_api::ContainerAdjustment::new();
    second_adjust.args = vec![String::new(), "/bin/echo".to_string()];
    second_response.adjust = MessageField::some(second_adjust);
    let mut second_update = nri_api::ContainerUpdate::new();
    second_update.container_id = "u1".to_string();
    let mut second_linux = nri_api::LinuxContainerUpdate::new();
    let mut second_resources = nri_api::LinuxResources::new();
    let mut second_memory = nri_api::LinuxMemory::new();
    let mut limit = nri_api::OptionalInt64::new();
    limit.value = 4096;
    second_memory.limit = MessageField::some(limit);
    second_resources.memory = MessageField::some(second_memory);
    second_linux.resources = MessageField::some(second_resources);
    second_update.linux = MessageField::some(second_linux);
    second_response.update.push(second_update);
    second_response.evict.push(nri_api::ContainerEviction {
        container_id: "e1".to_string(),
        reason: "test".to_string(),
        ..Default::default()
    });
    let second = Arc::new(FakePluginClient {
        create_response: Some(second_response),
        ..Default::default()
    });
    let validator = Arc::new(FakePluginClient::default());

    insert_plugin_for_test(&manager, "p1", "01", create_bit, true, first.clone()).await;
    insert_plugin_for_test(&manager, "p2", "02", create_bit, true, second.clone()).await;
    insert_plugin_for_test(
        &manager,
        "validator",
        "03",
        validate_bit,
        true,
        validator.clone(),
    )
    .await;
    manager.synchronize().await.unwrap();

    let result = manager
        .create_container(test_container_event())
        .await
        .unwrap();

    assert_eq!(
        result.adjustment.annotations.get("a"),
        Some(&"1".to_string())
    );
    assert_eq!(result.adjustment.args, vec!["/bin/echo".to_string()]);
    assert_eq!(result.updates.len(), 1);
    let resources = result.updates[0]
        .linux
        .as_ref()
        .and_then(|linux| linux.resources.as_ref())
        .expect("merged resources should be present");
    assert_eq!(
        resources
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(128)
    );
    assert_eq!(
        resources
            .memory
            .as_ref()
            .and_then(|memory| memory.limit.as_ref())
            .map(|limit| limit.value),
        Some(4096)
    );
    assert_eq!(result.evictions.len(), 1);
    assert_eq!(
        first.calls.lock().await.clone(),
        vec!["synchronize", "create"]
    );
    assert_eq!(
        second.calls.lock().await.clone(),
        vec!["synchronize", "create"]
    );
    assert_eq!(
        validator.calls.lock().await.clone(),
        vec!["synchronize", "validate"]
    );
    let validate_requests = validator.validate_requests.lock().await.clone();
    assert_eq!(validate_requests.len(), 1);
    let validate_request = &validate_requests[0];
    assert_eq!(validate_request.update.len(), 1);
    let owners = validate_request
        .owners
        .as_ref()
        .and_then(|owners| owners.owners.get("u1"))
        .expect("validator should receive owner metadata for merged updates");
    assert_eq!(
        owners.simple.get(&(nri_api::Field::CPUShares as i32)),
        Some(&"01-p1".to_string())
    );
    assert_eq!(
        owners.simple.get(&(nri_api::Field::MemLimit as i32)),
        Some(&"02-p2".to_string())
    );
}

#[tokio::test]
async fn create_container_rejects_protected_annotations_before_validator() {
    let manager = manager_for_tests();
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
    let validate_bit = event_mask_bit(nri_api::Event::VALIDATE_CONTAINER_ADJUSTMENT).unwrap();

    let mut response = nri_api::CreateContainerResponse::new();
    let mut adjust = nri_api::ContainerAdjustment::new();
    adjust.annotations.insert(
        "io.crius.internal/container-state".to_string(),
        "override".to_string(),
    );
    response.adjust = MessageField::some(adjust);

    let creator = Arc::new(FakePluginClient {
        create_response: Some(response),
        ..Default::default()
    });
    let validator = Arc::new(FakePluginClient::default());

    insert_plugin_for_test(&manager, "creator", "01", create_bit, true, creator.clone()).await;
    insert_plugin_for_test(
        &manager,
        "validator",
        "02",
        validate_bit,
        true,
        validator.clone(),
    )
    .await;
    manager.synchronize().await.unwrap();

    let err = manager
        .create_container(test_container_event())
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("protected annotation"));
    assert_eq!(
        creator.calls.lock().await.clone(),
        vec!["synchronize", "create"]
    );
    assert_eq!(validator.calls.lock().await.clone(), vec!["synchronize"]);
}

#[tokio::test]
async fn create_container_runs_builtin_default_validator_before_external_validators() {
    let manager = NriManager::with_domain(
        NriManagerConfig {
            enable: true,
            enable_cdi: true,
            cdi_spec_dirs: vec!["/etc/cdi".to_string(), "/var/run/cdi".to_string()],
            runtime_name: "crius".to_string(),
            runtime_version: "test".to_string(),
            socket_path: "unix:///tmp/crius-nri-default-validator-tests.sock".to_string(),
            plugin_path: String::new(),
            plugin_config_path: String::new(),
            registration_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            enable_external_connections: false,
            default_validator: crate::config::NriDefaultValidatorConfig {
                enable: true,
                reject_oci_hook_adjustment: true,
                ..Default::default()
            },
        },
        Arc::new(NopNri),
    );
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
    let validate_bit = event_mask_bit(nri_api::Event::VALIDATE_CONTAINER_ADJUSTMENT).unwrap();

    let mut response = nri_api::CreateContainerResponse::new();
    let mut adjust = nri_api::ContainerAdjustment::new();
    let mut hooks = nri_api::Hooks::new();
    hooks.prestart.push(nri_api::Hook::new());
    adjust.hooks = MessageField::some(hooks);
    response.adjust = MessageField::some(adjust);

    let creator = Arc::new(FakePluginClient {
        create_response: Some(response),
        ..Default::default()
    });
    let validator = Arc::new(FakePluginClient::default());

    insert_plugin_for_test(&manager, "creator", "01", create_bit, true, creator.clone()).await;
    insert_plugin_for_test(
        &manager,
        "validator",
        "02",
        validate_bit,
        true,
        validator.clone(),
    )
    .await;
    manager.synchronize().await.unwrap();

    let err = manager
        .create_container(test_container_event())
        .await
        .expect_err("builtin validator should reject OCI hook adjustment");
    assert!(format!("{err}").contains("OCI hook injection"));
    assert_eq!(
        creator.calls.lock().await.clone(),
        vec!["synchronize", "create"]
    );
    assert_eq!(validator.calls.lock().await.clone(), vec!["synchronize"]);
}

#[tokio::test]
async fn create_container_rejects_updates_targeting_container_being_created() {
    let manager = manager_for_tests();
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();

    let mut response = nri_api::CreateContainerResponse::new();
    let mut update = nri_api::ContainerUpdate::new();
    update.container_id = "ctr-1".to_string();
    let mut linux = nri_api::LinuxContainerUpdate::new();
    linux.resources = MessageField::some(nri_api::LinuxResources::new());
    update.linux = MessageField::some(linux);
    response.update.push(update);

    let plugin = Arc::new(FakePluginClient {
        create_response: Some(response),
        ..Default::default()
    });

    insert_plugin_for_test(&manager, "creator", "01", create_bit, true, plugin).await;
    manager.synchronize().await.unwrap();

    let err = manager
        .create_container(test_container_event())
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("targeted container ctr-1 during create"));
}

#[tokio::test]
async fn update_container_merges_target_resources_and_side_effects() {
    let manager = manager_for_tests();
    let update_bit = event_mask_bit(nri_api::Event::UPDATE_CONTAINER).unwrap();

    let mut response = nri_api::UpdateContainerResponse::new();
    let mut target = nri_api::ContainerUpdate::new();
    target.container_id = "ctr-1".to_string();
    let mut target_linux = nri_api::LinuxContainerUpdate::new();
    let mut target_resources = nri_api::LinuxResources::new();
    let mut target_cpu = nri_api::LinuxCPU::new();
    let mut shares = nri_api::OptionalUInt64::new();
    shares.value = 2048;
    target_cpu.shares = MessageField::some(shares);
    target_resources.cpu = MessageField::some(target_cpu);
    target_linux.resources = MessageField::some(target_resources);
    target.linux = MessageField::some(target_linux);
    response.update.push(target);

    let mut sidecar = nri_api::ContainerUpdate::new();
    sidecar.container_id = "ctr-2".to_string();
    let mut sidecar_linux = nri_api::LinuxContainerUpdate::new();
    let mut sidecar_resources = nri_api::LinuxResources::new();
    let mut sidecar_memory = nri_api::LinuxMemory::new();
    let mut limit = nri_api::OptionalInt64::new();
    limit.value = 4096;
    sidecar_memory.limit = MessageField::some(limit);
    sidecar_resources.memory = MessageField::some(sidecar_memory);
    sidecar_linux.resources = MessageField::some(sidecar_resources);
    sidecar.linux = MessageField::some(sidecar_linux);
    response.update.push(sidecar);
    response.evict.push(nri_api::ContainerEviction {
        container_id: "ctr-3".to_string(),
        reason: "policy".to_string(),
        ..Default::default()
    });

    let plugin = Arc::new(FakePluginClient {
        update_response: Some(response),
        ..Default::default()
    });
    insert_plugin_for_test(&manager, "updater", "01", update_bit, true, plugin.clone()).await;
    manager.synchronize().await.unwrap();

    let mut event = test_container_event();
    let mut requested = nri_api::LinuxResources::new();
    let mut requested_cpu = nri_api::LinuxCPU::new();
    let mut quota = nri_api::OptionalInt64::new();
    quota.value = 1000;
    requested_cpu.quota = MessageField::some(quota);
    requested.cpu = MessageField::some(requested_cpu);
    event.linux_resources = Some(requested);

    let result = manager.update_container(event).await.unwrap();

    let cpu = result
        .linux_resources
        .as_ref()
        .and_then(|resources| resources.cpu.as_ref())
        .unwrap();
    assert_eq!(cpu.shares.as_ref().map(|value| value.value), Some(2048));
    assert_eq!(cpu.quota.as_ref().map(|value| value.value), Some(1000));
    assert_eq!(result.updates.len(), 1);
    assert_eq!(result.updates[0].container_id, "ctr-2");
    assert_eq!(result.evictions.len(), 1);
    assert_eq!(
        plugin.calls.lock().await.clone(),
        vec!["synchronize", "update"]
    );
}

#[tokio::test]
async fn block_plugin_sync_prevents_sync_completion_mid_lifecycle() {
    let manager = Arc::new(manager_for_tests());
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
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
        .create_container(test_container_event())
        .await
        .expect("plugin should be active after sync completes");
    let calls = plugin.calls.lock().await.clone();
    assert_eq!(calls, vec!["synchronize", "create"]);
}

#[tokio::test]
async fn synchronize_applies_plugin_requested_updates_before_activation() {
    let domain = Arc::new(FakeDomain::default());
    let manager = manager_with_domain_for_tests(domain.clone());
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();

    let mut update = nri_api::ContainerUpdate::new();
    update.container_id = "ctr-sync-sidecar".to_string();
    let mut synchronize_response = nri_api::SynchronizeResponse::new();
    synchronize_response.update.push(update);
    let plugin = Arc::new(FakePluginClient {
        synchronize_response: Some(synchronize_response),
        ..Default::default()
    });

    insert_plugin_for_test(&manager, "syncer", "01", create_bit, true, plugin.clone()).await;

    manager
        .synchronize()
        .await
        .expect("synchronize should apply plugin updates");

    assert_eq!(
        domain.update_calls.lock().await.clone(),
        vec![vec!["ctr-sync-sidecar".to_string()]]
    );

    manager
        .create_container(test_container_event())
        .await
        .expect("plugin should activate after sync updates succeed");
    assert_eq!(
        plugin.calls.lock().await.clone(),
        vec!["synchronize", "create"]
    );
}

#[tokio::test]
async fn synchronize_uses_current_snapshot_without_replaying_lifecycle_events() {
    #[derive(Default)]
    struct SnapshotDomain;

    #[async_trait]
    impl NriDomain for SnapshotDomain {
        async fn snapshot(&self) -> Result<crate::nri::domain::RuntimeSnapshot> {
            let mut pod = nri_api::PodSandbox::new();
            pod.id = "pod-recovered".to_string();

            let mut container = nri_api::Container::new();
            container.id = "ctr-recovered".to_string();
            container.pod_sandbox_id = "pod-recovered".to_string();

            Ok(crate::nri::domain::RuntimeSnapshot {
                pods: vec![pod],
                containers: vec![container],
            })
        }

        async fn apply_updates(
            &self,
            _updates: &[nri_api::ContainerUpdate],
        ) -> Result<Vec<nri_api::ContainerUpdate>> {
            Ok(Vec::new())
        }

        async fn evict(&self, _container_id: &str, _reason: &str) -> Result<()> {
            Ok(())
        }
    }

    let manager = manager_with_domain_for_tests(Arc::new(SnapshotDomain));
    let plugin = Arc::new(FakePluginClient::default());

    insert_plugin_for_test(&manager, "recover", "01", 0, true, plugin.clone()).await;

    manager
        .synchronize()
        .await
        .expect("synchronize should deliver recovered snapshot");

    assert_eq!(plugin.calls.lock().await.clone(), vec!["synchronize"]);
    assert_eq!(
        plugin.synchronize_snapshots.lock().await.clone(),
        vec![(
            vec!["pod-recovered".to_string()],
            vec!["ctr-recovered".to_string()]
        )]
    );
}

#[tokio::test]
async fn request_timeout_is_reported_without_unregistering_plugin() {
    let manager = NriManager::with_domain(
        NriManagerConfig {
            enable: true,
            enable_cdi: true,
            cdi_spec_dirs: vec!["/etc/cdi".to_string(), "/var/run/cdi".to_string()],
            runtime_name: "crius".to_string(),
            runtime_version: "test".to_string(),
            socket_path: "unix:///tmp/crius-nri-manager-timeout-tests.sock".to_string(),
            plugin_path: String::new(),
            plugin_config_path: String::new(),
            registration_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(10),
            enable_external_connections: false,
            default_validator: Default::default(),
        },
        Arc::new(NopNri),
    );
    let event_bit = event_mask_bit(nri_api::Event::POST_START_CONTAINER).unwrap();
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
        plugin_config_path: PathBuf::new(),
        expected_plugin: None,
        registered_tx: Arc::new(Mutex::new(None)),
        connection_identity: Arc::new(Mutex::new(None)),
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

#[tokio::test]
async fn external_registration_loads_plugin_specific_config() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("conf.d");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(config_dir.join("03-external.conf"), "external-config").unwrap();

    let state = Arc::new(RwLock::new(ManagerState::default()));
    let handler = ManagerRuntimeHandler {
        state: state.clone(),
        domain: Arc::new(NopNri),
        registration_notify: Arc::new(Notify::new()),
        client: None,
        config: String::new(),
        plugin_config_path: config_dir,
        expected_plugin: None,
        registered_tx: Arc::new(Mutex::new(None)),
        connection_identity: Arc::new(Mutex::new(None)),
    };
    let mut req = nri_api::RegisterPluginRequest::new();
    req.plugin_name = "external".to_string();
    req.plugin_idx = "03".to_string();

    handler
        .register_plugin(req)
        .await
        .expect("registration should load plugin config");

    let plugins = state.read().await;
    assert_eq!(plugins.plugins.len(), 1);
    assert_eq!(plugins.plugins[0].record.config, "external-config");
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

    let plugins = discover_preinstalled_plugins(&plugin_dir, &dir.path().join("conf.d")).unwrap();
    let (_local, peer) = UnixStream::pair().unwrap();
    let mut child =
        spawn_preinstalled_plugin_process(&plugins[0], peer).expect("plugin launch should succeed");
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

#[tokio::test]
async fn start_does_not_create_external_socket_when_plugin_discovery_fails() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("plugins");
    fs::create_dir_all(&plugin_dir).unwrap();

    let invalid_plugin = plugin_dir.join("broken-plugin-name");
    fs::write(&invalid_plugin, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&invalid_plugin).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&invalid_plugin, perms).unwrap();

    let socket_path = dir.path().join("nri.sock");
    let manager = NriManager::new(NriManagerConfig {
        enable: true,
        enable_cdi: true,
        cdi_spec_dirs: vec!["/etc/cdi".to_string(), "/var/run/cdi".to_string()],
        runtime_name: "crius".to_string(),
        runtime_version: "test".to_string(),
        socket_path: format!("unix://{}", socket_path.display()),
        plugin_path: plugin_dir.display().to_string(),
        plugin_config_path: dir.path().join("conf.d").display().to_string(),
        registration_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        enable_external_connections: true,
        default_validator: Default::default(),
    });

    let err = manager.start().await.expect_err("start should fail");
    assert!(matches!(err, NriError::Plugin(_)));
    assert!(
        !socket_path.exists(),
        "listener socket should not exist after plugin startup failure"
    );
}

#[test]
fn event_masks_match_upstream_nri_semantics() {
    let create_bit = event_mask_bit(nri_api::Event::CREATE_CONTAINER).unwrap();
    let validate_bit = event_mask_bit(nri_api::Event::VALIDATE_CONTAINER_ADJUSTMENT).unwrap();

    assert_eq!(
        create_bit,
        1 << ((nri_api::Event::CREATE_CONTAINER.value() - 1) as u32)
    );
    assert_eq!(
        validate_bit,
        1 << ((nri_api::Event::VALIDATE_CONTAINER_ADJUSTMENT.value() - 1) as u32)
    );
    assert!(event_subscribed(
        create_bit,
        nri_api::Event::CREATE_CONTAINER
    ));
    assert!(!event_subscribed(
        create_bit,
        nri_api::Event::REMOVE_CONTAINER
    ));
    assert!(event_subscribed(
        validate_bit,
        nri_api::Event::VALIDATE_CONTAINER_ADJUSTMENT
    ));
    assert_eq!(normalize_events_mask(0).unwrap(), valid_events_mask());
    assert_eq!(valid_events_mask() & validate_bit, validate_bit);
}
