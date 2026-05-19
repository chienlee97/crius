#[tokio::test]
async fn recover_state_reconciles_running_container_to_exited() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "stopped");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("recover-container")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(0));
    assert!(state.finished_at.is_some());

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
}

#[tokio::test]
async fn recover_state_marks_ready_pod_notready_when_pause_is_stopped() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-1", "stopped");
    let netns_path = dir.path().join("pause-netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-1".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-1".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-recover")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn recover_state_does_not_replay_historical_events() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "stopped");

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_container(&ContainerRecord {
            id: "recover-container".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: String::new(),
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        })
        .unwrap();

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.recover_state().await.unwrap();

    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "recover_state should not replay historical events"
    );
}

#[tokio::test]
async fn recover_state_re_registers_exit_monitor_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    set_fake_runtime_state(&dir, "recover-running", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_container(&ContainerRecord {
            id: "recover-running".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: String::new(),
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        })
        .unwrap();

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.recover_state().await.unwrap();

    let exit_code_path = dir.path().join("exits").join("recover-running");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "23").unwrap();

    let event = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Ok(event)) = stream.next().await {
                if event.container_id == "recover-running"
                    && event.container_event_type
                        == ContainerEventType::ContainerStoppedEvent as i32
                {
                    return event;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for recovered container stop event");

    assert_eq!(event.container_id, "recover-running");
    let container = service
        .containers
        .lock()
        .await
        .get("recover-running")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(23));
    assert!(state.nri_stop_notified);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
}

#[tokio::test]
async fn recover_state_supports_inspect_and_list_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ip: Some("10.88.0.20".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: Some("10.88.0.20".to_string()),
        })
        .unwrap();

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-container".to_string()),
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let container_status = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "recover-container".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let pod_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-recover".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    let pods =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(
        container_status.status.as_ref().unwrap().state,
        ContainerState::ContainerRunning as i32
    );
    assert_eq!(
        pod_status.status.as_ref().unwrap().state,
        PodSandboxState::SandboxReady as i32
    );
    assert_eq!(containers.containers.len(), 1);
    assert_eq!(containers.containers[0].pod_sandbox_id, "pod-recover");
    assert_eq!(pods.items.len(), 1);
    let info: serde_json::Value =
        serde_json::from_str(container_status.info.get("info").unwrap()).unwrap();
    assert_eq!(info["sandboxID"], "pod-recover");
}

#[tokio::test]
async fn recover_state_restores_pod_hostname_from_internal_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-hostname.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            hostname: Some("restored-hostname".to_string()),
            port_mappings: vec![StoredPortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: String::new(),
            }],
            raw_cni_result: Some(serde_json::json!({
                "cniVersion": "1.0.0",
                "ips": [
                    {"address": "fd00::10/64"},
                    {"address": "10.88.0.11/16"}
                ]
            })),
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            readonly_rootfs: true,
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-name".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = {
        let pod_manager = service.pod_manager.lock().await;
        pod_manager
            .get_pod_sandbox_cloned("pod-recover")
            .expect("pod should be recovered")
    };
    assert_eq!(pod.config.hostname, "restored-hostname");
    assert_eq!(pod.config.port_mappings.len(), 1);
    assert_eq!(pod.config.port_mappings[0].host_port, 8080);
    assert!(pod.config.readonly_rootfs);
    let network_status = pod
        .network_status
        .expect("raw cni result should restore network");
    assert_eq!(network_status.ip.unwrap().to_string(), "fd00::10");
    assert_eq!(
        network_status.interfaces[0].ip.unwrap().to_string(),
        "10.88.0.11"
    );
}

#[tokio::test]
async fn recover_state_uses_configured_ipv4_preference_when_primary_ip_is_missing() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.cni_config = crate::network::CniConfig::new(
        Vec::new(),
        Vec::new(),
        dir.path().join("cache"),
        0,
        crate::network::MainIpPreference::Ipv4,
        None,
        false,
    );
    set_fake_runtime_state(&dir, "pause-recover-ip-pref", "running");
    let netns_path = dir.path().join("recover-ip-pref.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            raw_cni_result: Some(serde_json::json!({
                "cniVersion": "1.0.0",
                "ips": [
                    {"address": "fd00::10/64"},
                    {"address": "10.88.0.11/16"},
                    {"address": "fd00::12/64"}
                ]
            })),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover-ip-pref".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover-ip-pref".to_string(),
            state: "ready".to_string(),
            name: "pod-ip-pref".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover-ip-pref".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover-ip-pref".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = {
        let pod_manager = service.pod_manager.lock().await;
        pod_manager
            .get_pod_sandbox_cloned("pod-recover-ip-pref")
            .expect("pod should be recovered")
    };
    let network_status = pod
        .network_status
        .expect("raw cni result should restore network");
    assert_eq!(network_status.ip.unwrap().to_string(), "10.88.0.11");
    assert_eq!(network_status.interfaces.len(), 2);
    assert_eq!(
        network_status.interfaces[0].ip.unwrap().to_string(),
        "fd00::10"
    );
    assert_eq!(
        network_status.interfaces[1].ip.unwrap().to_string(),
        "fd00::12"
    );
}

#[tokio::test]
async fn recover_state_restores_pod_userns_options_from_internal_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-recover-userns", "running");
    let netns_path = dir.path().join("recover-userns.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover-userns".to_string()),
            namespace_options: Some(StoredNamespaceOptions {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: Some(StoredUserNamespace {
                    mode: NamespaceMode::Pod as i32,
                    uids: vec![StoredIdMapping {
                        host_id: 100000,
                        container_id: 0,
                        length: 65536,
                    }],
                    gids: vec![StoredIdMapping {
                        host_id: 200000,
                        container_id: 0,
                        length: 65536,
                    }],
                }),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover-userns".to_string(),
            state: "ready".to_string(),
            name: "pod-userns".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover-userns".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover-userns".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = {
        let pod_manager = service.pod_manager.lock().await;
        pod_manager
            .get_pod_sandbox_cloned("pod-recover-userns")
            .expect("pod should be recovered")
    };
    let userns = pod
        .config
        .namespace_options
        .and_then(|options| options.userns_options)
        .expect("userns options should be restored");
    assert_eq!(userns.mode, NamespaceMode::Pod as i32);
    assert_eq!(userns.uids[0].host_id, 100000);
    assert_eq!(userns.gids[0].host_id, 200000);
}

#[tokio::test]
async fn recover_state_supports_stop_and_remove_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-remove.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "recover-container".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "recover-container".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .is_none());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn recover_state_marks_ready_pod_notready_when_netns_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-missing-netns", "running");

    let missing_netns_path = dir.path().join("missing.netns");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(missing_netns_path.display().to_string()),
            pause_container_id: Some("pause-missing-netns".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-missing-netns".to_string(),
            state: "ready".to_string(),
            name: "pod-missing-netns".to_string(),
            namespace: "default".to_string(),
            uid: "uid-missing-netns".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: missing_netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-missing-netns".to_string()),
            ip: Some("10.88.0.21".to_string()),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-missing-netns")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-missing-netns")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn recover_state_preserves_ready_pod_when_pause_runtime_state_is_unknown() {
    let (dir, service) = test_service_with_fake_runtime();
    let netns_path = dir.path().join("recover-unknown.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-unknown".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover-unknown".to_string(),
            state: "ready".to_string(),
            name: "pod-recover-unknown".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover-unknown".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-unknown".to_string()),
            ip: Some("10.88.0.22".to_string()),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-recover-unknown")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxReady as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover-unknown")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "ready");
}

#[tokio::test]
async fn list_pod_sandbox_preserves_ready_state_when_pause_running_but_pid_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-running", "running");
    let _ = fs::remove_file(dir.path().join("runtime-state").join("pause-running.pid"));

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-ready-running-no-pid".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "pod-ready-running-no-pid".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "kube-apiserver-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: HashMap::from([(
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            )]),
            annotations,
            runtime_handler: "runc".to_string(),
        },
    );

    let response =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(response.items.len(), 1);
    assert_eq!(
        response.items[0].state,
        PodSandboxState::SandboxReady as i32
    );
}

#[tokio::test]
async fn runtime_state_refresh_keeps_sandbox_ready_when_pause_running_but_pid_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-refresh-running", "running");
    let _ = fs::remove_file(
        dir.path()
            .join("runtime-state")
            .join("pause-refresh-running.pid"),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-refresh-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-refresh-running-no-pid".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "pod-refresh-running-no-pid".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "kube-apiserver-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: HashMap::from([(
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            )]),
            annotations,
            runtime_handler: "runc".to_string(),
        },
    );

    RuntimeServiceImpl::refresh_runtime_state_and_publish_events(
        &service.runtime,
        &service.config,
        &service.containers,
        &service.pod_sandboxes,
        &service.persistence,
        &service.events,
    )
    .await;

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-refresh-running-no-pid")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxReady as i32);
}

#[tokio::test]
async fn remove_pod_sandbox_cascades_recovered_containers_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-cascade.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-container".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(service
        .containers
        .lock()
        .await
        .get("recover-container")
        .is_none());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn container_status_refreshes_stale_runtime_state_on_query() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "refresh-container", "stopped");

    service.containers.lock().await.insert(
        "refresh-container".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("refresh-container", "pod-1", HashMap::new())
        },
    );

    let response = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "refresh-container".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(
        response.status.unwrap().state,
        ContainerState::ContainerExited as i32
    );
}

#[tokio::test]
async fn pod_queries_refresh_pause_container_state_on_query() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-refresh", "stopped");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-refresh".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-refresh".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-refresh", annotations)
        },
    );

    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].state, PodSandboxState::SandboxNotready as i32);

    let inspect = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-refresh".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        inspect.status.unwrap().state,
        PodSandboxState::SandboxNotready as i32
    );
}

#[tokio::test]
async fn pod_queries_mark_ready_sandbox_notready_when_pause_runtime_state_is_unknown() {
    let (_dir, service) = test_service_with_fake_runtime();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-unknown".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-unknown".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-unknown", annotations)
        },
    );

    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].state, PodSandboxState::SandboxNotready as i32);

    let inspect = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-unknown".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        inspect.status.unwrap().state,
        PodSandboxState::SandboxNotready as i32
    );
}

#[tokio::test]
async fn list_pod_sandbox_deduplicates_logical_control_plane_duplicates() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-old", "stopped");
    set_fake_runtime_state(&dir, "pause-new", "running");

    let make_annotations = |pause_container_id: &str| {
        let mut annotations = HashMap::new();
        RuntimeServiceImpl::insert_internal_state(
            &mut annotations,
            INTERNAL_POD_STATE_KEY,
            &StoredPodState {
                runtime_handler: "runc".to_string(),
                pause_container_id: Some(pause_container_id.to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        annotations
    };

    service.pod_sandboxes.lock().await.insert(
        "sandbox-old".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "sandbox-old".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "etcd-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxNotready as i32,
            created_at: 100,
            labels: HashMap::from([
                ("component".to_string(), "etcd".to_string()),
                (
                    "io.kubernetes.pod.uid".to_string(),
                    "logical-pod-uid".to_string(),
                ),
            ]),
            annotations: make_annotations("pause-old"),
            ..Default::default()
        },
    );
    service.pod_sandboxes.lock().await.insert(
        "sandbox-new".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "sandbox-new".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "etcd-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 2,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: 200,
            labels: HashMap::from([
                ("component".to_string(), "etcd".to_string()),
                (
                    "io.kubernetes.pod.uid".to_string(),
                    "logical-pod-uid".to_string(),
                ),
            ]),
            annotations: make_annotations("pause-new"),
            ..Default::default()
        },
    );

    let response =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(response.items.len(), 1);
    assert_eq!(response.items[0].id, "sandbox-new");
    assert_eq!(
        response.items[0].state,
        PodSandboxState::SandboxReady as i32
    );
    assert_eq!(
        response.items[0].metadata.as_ref().map(|m| m.attempt),
        Some(2)
    );
}

#[tokio::test]
async fn list_containers_deduplicates_logical_control_plane_duplicates() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "ctr-old", "stopped");
    set_fake_runtime_state(&dir, "ctr-new", "running");

    service.pod_sandboxes.lock().await.insert(
        "pod-1".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            id: "pod-1".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "kube-controller-manager-vm-crius".to_string(),
                uid: "logical-pod-uid".to_string(),
                namespace: "kube-system".to_string(),
                attempt: 1,
            }),
            state: PodSandboxState::SandboxReady as i32,
            created_at: 10,
            labels: HashMap::from([
                (
                    "component".to_string(),
                    "kube-controller-manager".to_string(),
                ),
                (
                    "io.kubernetes.pod.uid".to_string(),
                    "logical-pod-uid".to_string(),
                ),
            ]),
            ..Default::default()
        },
    );

    let old = Container {
        id: "ctr-old".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "kube-controller-manager".to_string(),
            attempt: 1,
        }),
        state: ContainerState::ContainerExited as i32,
        created_at: 100,
        image: Some(ImageSpec {
            image: "k8s.m.daocloud.io/kube-controller-manager:v1.29.1".to_string(),
            ..Default::default()
        }),
        image_ref: "sha256:old".to_string(),
        labels: HashMap::from([
            (
                "component".to_string(),
                "kube-controller-manager".to_string(),
            ),
            (
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            ),
        ]),
        annotations: HashMap::new(),
    };
    let new = Container {
        id: "ctr-new".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "kube-controller-manager".to_string(),
            attempt: 2,
        }),
        state: ContainerState::ContainerRunning as i32,
        created_at: 200,
        image: Some(ImageSpec {
            image: "k8s.m.daocloud.io/kube-controller-manager:v1.29.1".to_string(),
            ..Default::default()
        }),
        image_ref: "sha256:new".to_string(),
        labels: HashMap::from([
            (
                "component".to_string(),
                "kube-controller-manager".to_string(),
            ),
            (
                "io.kubernetes.pod.uid".to_string(),
                "logical-pod-uid".to_string(),
            ),
        ]),
        annotations: HashMap::new(),
    };
    service.containers.lock().await.insert(old.id.clone(), old);
    service.containers.lock().await.insert(new.id.clone(), new);

    let response =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(response.containers.len(), 1);
    assert_eq!(response.containers[0].id, "ctr-new");
    assert_eq!(
        response.containers[0].state,
        ContainerState::ContainerRunning as i32
    );
    assert_eq!(
        response.containers[0]
            .metadata
            .as_ref()
            .map(|metadata| metadata.attempt),
        Some(2)
    );
}

#[tokio::test]
async fn recover_state_ignores_stale_shim_metadata_artifacts() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");

    let stale_dir = dir.path().join("shims").join("orphan");
    fs::create_dir_all(&stale_dir).unwrap();
    fs::write(stale_dir.join("attach.sock"), "stale").unwrap();
    fs::write(
        stale_dir.join("shim.json"),
        serde_json::to_vec_pretty(&crate::runtime::ShimProcess {
            container_id: "orphan".to_string(),
            shim_pid: 999_999,
            exit_code_file: stale_dir.join("exit_code"),
            log_file: stale_dir.join("shim.log"),
            socket_path: stale_dir.join("attach.sock"),
            bundle_path: dir.path().join("bundles").join("orphan"),
        })
        .unwrap(),
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(containers.containers.len(), 1);
    assert!(
        !stale_dir.exists(),
        "recover_state should clean orphaned shim artifacts"
    );
}

#[tokio::test]
async fn recover_state_skips_orphan_sweeps_after_clean_shutdown_marker() {
    let (dir, service) = test_service_with_fake_runtime();
    service.record_startup_clean_shutdown(true);

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(stale_shim_dir.exists());
    assert!(stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_does_not_skip_orphan_sweeps_after_reboot_even_if_last_shutdown_was_clean() {
    let (dir, service) = test_service_with_fake_runtime();
    service.record_startup_clean_shutdown(true);
    service.record_startup_detected_reboot(true);

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(!stale_shim_dir.exists());
    assert!(!stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_does_not_skip_orphan_sweeps_after_upgrade_even_if_last_shutdown_was_clean() {
    let (dir, service) = test_service_with_fake_runtime();
    service.record_startup_clean_shutdown(true);
    service.record_startup_detected_upgrade(true);

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(!stale_shim_dir.exists());
    assert!(!stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_skips_orphan_sweeps_when_internal_wipe_is_disabled() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.internal_wipe = false;

    let stale_shim_dir = dir.path().join("shims").join("orphan");
    let stale_attach_dir = dir.path().join("attach").join("orphan");
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(stale_shim_dir.join("shim.json"), "{}").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    service.recover_state().await.unwrap();

    assert!(stale_shim_dir.exists());
    assert!(stale_attach_dir.exists());
}

#[tokio::test]
async fn recover_state_cleans_orphaned_attach_socket_artifacts_from_separate_directory() {
    let (dir, mut service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    service.attach_socket_dir = dir.path().join("attach");

    let attach_dir = service.attach_socket_dir.join("orphan");
    let shim_dir = dir.path().join("shims").join("orphan");
    fs::create_dir_all(&attach_dir).unwrap();
    fs::create_dir_all(&shim_dir).unwrap();
    fs::write(attach_dir.join("attach.sock"), "stale").unwrap();
    fs::write(
        shim_dir.join("shim.json"),
        serde_json::to_vec_pretty(&crate::runtime::ShimProcess {
            container_id: "orphan".to_string(),
            shim_pid: 999_999,
            exit_code_file: shim_dir.join("exit_code"),
            log_file: shim_dir.join("shim.log"),
            socket_path: attach_dir.join("attach.sock"),
            bundle_path: dir.path().join("bundles").join("orphan"),
        })
        .unwrap(),
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(
        !attach_dir.exists(),
        "recover_state should clean orphaned attach socket directories"
    );
}

#[tokio::test]
async fn recover_state_recovers_running_container_without_shim_metadata_file_from_ledger() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-ledger-only", "running");
    fs::create_dir_all(dir.path().join("runtime-root").join("recover-ledger-only")).unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-ledger-only",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .update_container_ledger_metadata("recover-ledger-only", Some("runc"), Some("runc"), None)
        .unwrap();

    let mut shim_child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_shim_process_record(&crate::storage::ShimProcessRecord {
            container_id: "recover-ledger-only".to_string(),
            shim_pid: shim_child.id(),
            work_dir: dir.path().join("shims").display().to_string(),
            socket_path: dir
                .path()
                .join("shims")
                .join("recover-ledger-only")
                .join("task.sock")
                .display()
                .to_string(),
            exit_code_file: dir
                .path()
                .join("exits")
                .join("recover-ledger-only")
                .display()
                .to_string(),
            log_file: dir
                .path()
                .join("shims")
                .join("recover-ledger-only")
                .join("shim.log")
                .display()
                .to_string(),
            bundle_path: dir
                .path()
                .join("runtime-root")
                .join("recover-ledger-only")
                .display()
                .to_string(),
            state: "running".to_string(),
            last_seen_at: RuntimeServiceImpl::now_nanos(),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let recovered = service
        .containers
        .lock()
        .await
        .get("recover-ledger-only")
        .cloned()
        .unwrap();
    assert_eq!(recovered.state, ContainerState::ContainerRunning as i32);
    let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &recovered.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap_or_default();
    assert!(internal_state.broken.is_none());

    let _ = shim_child.kill();
    let _ = shim_child.wait();
}

#[tokio::test]
async fn recover_state_marks_container_broken_when_runtime_bundle_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "broken-bundle", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("broken-bundle".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "broken-bundle",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .update_container_ledger_metadata("broken-bundle", Some("runc"), Some("runc"), None)
        .unwrap();

    service.recover_state().await.unwrap();

    let recovered = service
        .containers
        .lock()
        .await
        .get("broken-bundle")
        .cloned()
        .unwrap();
    let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &recovered.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        internal_state
            .broken
            .as_ref()
            .map(|broken| broken.kind.as_str()),
        Some("bundle_missing")
    );
}

#[tokio::test]
async fn recover_state_keeps_live_orphaned_shim_directories_when_ledger_has_live_process() {
    let (dir, service) = test_service_with_fake_runtime();
    let live_shim_dir = dir.path().join("shims").join("orphan-live");
    let live_attach_dir = dir.path().join("attach").join("orphan-live");
    let stale_shim_dir = dir.path().join("shims").join("orphan-stale");
    let stale_attach_dir = dir.path().join("attach").join("orphan-stale");
    fs::create_dir_all(&live_shim_dir).unwrap();
    fs::create_dir_all(&live_attach_dir).unwrap();
    fs::create_dir_all(&stale_shim_dir).unwrap();
    fs::create_dir_all(&stale_attach_dir).unwrap();
    fs::write(live_attach_dir.join("attach.sock"), "live").unwrap();
    fs::write(stale_attach_dir.join("attach.sock"), "stale").unwrap();

    let mut shim_child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_shim_process_record(&crate::storage::ShimProcessRecord {
            container_id: "orphan-live".to_string(),
            shim_pid: shim_child.id(),
            work_dir: dir.path().join("shims").display().to_string(),
            socket_path: live_attach_dir.join("attach.sock").display().to_string(),
            exit_code_file: dir
                .path()
                .join("exits")
                .join("orphan-live")
                .display()
                .to_string(),
            log_file: live_shim_dir.join("shim.log").display().to_string(),
            bundle_path: dir
                .path()
                .join("runtime-root")
                .join("orphan-live")
                .display()
                .to_string(),
            state: "running".to_string(),
            last_seen_at: RuntimeServiceImpl::now_nanos(),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(live_shim_dir.exists());
    assert!(live_attach_dir.exists());
    assert!(!stale_shim_dir.exists());
    assert!(!stale_attach_dir.exists());

    let _ = shim_child.kill();
    let _ = shim_child.wait();
}

#[tokio::test]
async fn recover_state_cleans_orphaned_runtime_bundles_but_keeps_recovered_ones() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");

    let recovered_bundle = dir.path().join("runtime-root").join("recover-container");
    let orphan_bundle = dir.path().join("runtime-root").join("orphan-bundle");
    let live_orphan_bundle = dir.path().join("runtime-root").join("live-orphan");
    fs::create_dir_all(&recovered_bundle).unwrap();
    fs::create_dir_all(&orphan_bundle).unwrap();
    fs::create_dir_all(&live_orphan_bundle).unwrap();
    fs::write(recovered_bundle.join("config.json"), "{}").unwrap();
    fs::write(orphan_bundle.join("config.json"), "{}").unwrap();
    fs::write(live_orphan_bundle.join("config.json"), "{}").unwrap();
    set_fake_runtime_state(&dir, "live-orphan", "running");

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .replace_runtime_artifacts(
            "container",
            "orphan-bundle",
            &[crate::storage::RuntimeArtifactRecord {
                owner_kind: "container".to_string(),
                owner_id: "orphan-bundle".to_string(),
                artifact_kind: "bundle".to_string(),
                path: orphan_bundle.display().to_string(),
                state: "active".to_string(),
                runtime_handler: Some("runc".to_string()),
                runtime_root: Some(dir.path().join("runtime-root").display().to_string()),
            }],
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(
        recovered_bundle.exists(),
        "recover_state should keep recovered runtime bundles"
    );
    assert!(
        !orphan_bundle.exists(),
        "recover_state should clean orphaned runtime bundles"
    );
    assert!(
        live_orphan_bundle.exists(),
        "recover_state should not delete live runtime bundles that still have runtime state"
    );
    {
        let persistence = service.persistence.lock().await;
        assert!(
            !persistence
                .list_runtime_artifacts()
                .unwrap()
                .iter()
                .any(|artifact| artifact.owner_id == "orphan-bundle"),
            "orphan cleanup should delete ledger artifacts with no container owner"
        );
        assert!(persistence
            .storage()
            .get_recent_events("orphan_cleanup", 0)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "reconcile"
                && event.entity_id == "orphan-bundle"
                && event.new_state == "pending"));
    }
}

#[tokio::test]
async fn recover_state_cleans_orphaned_pod_workspaces_but_keeps_recovered_ones() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-recover", "running");

    let recovered_workspace = dir.path().join("root").join("pods").join("pod-recover");
    let orphan_workspace = dir.path().join("root").join("pods").join("orphan-pod");
    fs::create_dir_all(&recovered_workspace).unwrap();
    fs::create_dir_all(&orphan_workspace).unwrap();
    fs::write(
        recovered_workspace.join("resolv.conf"),
        "nameserver 8.8.8.8",
    )
    .unwrap();
    fs::write(orphan_workspace.join("marker"), "stale").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-recover",
            "ready",
            "pod-recover-name",
            "default",
            "pod-recover-uid",
            "",
            &HashMap::new(),
            &annotations,
            Some("pause-recover"),
            None,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .replace_runtime_artifacts(
            "pod",
            "orphan-pod",
            &[crate::storage::RuntimeArtifactRecord {
                owner_kind: "pod".to_string(),
                owner_id: "orphan-pod".to_string(),
                artifact_kind: "workspace".to_string(),
                path: orphan_workspace.display().to_string(),
                state: "active".to_string(),
                runtime_handler: None,
                runtime_root: None,
            }],
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert!(
        recovered_workspace.exists(),
        "recover_state should keep recovered pod workspaces"
    );
    assert!(
        !orphan_workspace.exists(),
        "recover_state should clean orphaned pod workspaces"
    );
    {
        let persistence = service.persistence.lock().await;
        assert!(
            !persistence
                .list_runtime_artifacts()
                .unwrap()
                .iter()
                .any(|artifact| artifact.owner_id == "orphan-pod"),
            "orphan cleanup should delete pod artifact ledger entries with no pod owner"
        );
        assert!(persistence
            .storage()
            .get_recent_events("orphan_cleanup", 0)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "reconcile"
                && event.entity_id == "orphan-pod"
                && event.new_state == "pending"));
    }
}
