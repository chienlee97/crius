#[tokio::test]
async fn start_container_notifies_nri_before_and_after_runtime_start() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start".to_string(),
        test_pod("pod-start", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start".to_string(),
        test_container("container-start", "pod-start", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start",
            "pod-start",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-start")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);

    let post_start_event = fake_nri
        .post_start_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("post start event should be recorded");
    assert_eq!(
        post_start_event.container.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_RUNNING
    );
    assert!(post_start_event.container.started_at > 0);
    let events = service
        .internal_services
        .events
        .recent_internal_events("container", "container-start", 10)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.kind == "container.start_start"
            && event.details["previousState"] == "created"));
    assert!(events
        .iter()
        .any(|event| event.kind == "container.start_success"
            && event.details["state"] == "running"));
}

#[tokio::test]
async fn start_container_succeeds_when_nri_post_start_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_start_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-post-fail".to_string(),
        test_pod("pod-start-post-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-post-fail".to_string(),
        test_container(
            "container-start-post-fail",
            "pod-start-post-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-post-fail",
            "pod-start-post-fail",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-post-fail", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-post-fail".to_string(),
        }),
    )
    .await
    .expect("post-start failure should not fail start");

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-post-fail")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);
}

#[tokio::test]
async fn start_container_falls_back_to_persistence_when_memory_state_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-start".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-persisted-start".to_string(),
        test_pod("pod-persisted-start", HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-start",
            "pod-persisted-start",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-persisted-start", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-persisted-start".to_string(),
        }),
    )
    .await
    .expect("start should rehydrate persisted created containers");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-persisted-start")
        .cloned()
        .expect("start should load persisted container into memory");
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-persisted-start")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "running");
}

#[tokio::test]
async fn start_container_rejects_repeated_start_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-repeat".to_string(),
        test_pod("pod-start-repeat", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-repeat".to_string(),
        test_container(
            "container-start-repeat",
            "pod-start-repeat",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-repeat",
            "pod-start-repeat",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-repeat", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-repeat".to_string(),
        }),
    )
    .await
    .expect("first start should succeed");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-repeat".to_string(),
        }),
    )
    .await
    .expect_err("repeated start should be rejected");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("already running"));
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
    let events = service
        .internal_services
        .events
        .recent_internal_events("container", "container-start-repeat", 10)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.kind == "container.start_failed"
            && event.details["phase"] == "precondition"
            && event.details["state"] == "running"));
}

#[tokio::test]
async fn start_container_rejects_exited_container_with_failed_precondition() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            exit_code: Some(17),
            finished_at: Some(RuntimeServiceImpl::now_nanos()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-exited".to_string(),
        test_pod("pod-start-exited", HashMap::new()),
    );
    let mut container = test_container(
        "container-start-exited",
        "pod-start-exited",
        annotations.clone(),
    );
    container.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-start-exited".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-exited",
            "pod-start-exited",
            crate::runtime::ContainerStatus::Stopped(17),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-start-exited", "stopped");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-exited".to_string(),
        }),
    )
    .await
    .expect_err("exited containers must not be startable again");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("already exited"));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn start_container_undoes_nri_when_runtime_start_fails() {
    let _guard = env_lock().lock().await;
    let fake_nri = Arc::new(FakeNri::default());
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script_with_start_state(dir.path(), "stopped");
    let (dir, service) =
        test_service_with_runtime_path_without_shim_and_nri(dir, runtime_path, fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-fail".to_string(),
        test_pod("pod-start-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-fail".to_string(),
        test_container(
            "container-start-fail",
            "pod-start-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-fail",
            "pod-start-fail",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-fail", &annotations);

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-sidecar".to_string();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 256;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-start-fail", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-start-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-sidecar", "running");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-fail".to_string(),
        }),
    )
    .await
    .expect_err("runtime start failure should be surfaced");
    assert_eq!(err.code(), tonic::Code::Internal);

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "stop_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-start-fail")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);

    let sidecar_update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar_update_payload["cpu"]["shares"], 256);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-start-fail")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            dir.path()
                .join("runtime-root")
                .join("container-start-fail")
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let bundle_annotations = bundle_config
        .get("annotations")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    let bundle_state: StoredContainerState = serde_json::from_str(
        bundle_annotations
            .get(INTERNAL_CONTAINER_STATE_KEY)
            .and_then(|value| value.as_str())
            .expect("bundle should persist internal container state after failed start"),
    )
    .unwrap();
    assert!(bundle_state.nri_stop_notified);
    assert!(bundle_state.started_at.is_some());
    assert!(bundle_state.finished_at.is_some());
    assert_eq!(bundle_state.exit_code, Some(0));
}

#[tokio::test]
async fn start_container_fails_when_runtime_keeps_container_in_created_state() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script_with_start_state(dir.path(), "created");
    let shim_path = write_quick_exit_shim(dir.path());
    let (dir, mut service) =
        test_service_with_runtime_path_and_shim_path(dir, runtime_path.clone(), Some(shim_path));
    service.runtime = RuntimeRegistry::new(
        "runc".to_string(),
        HashMap::from([(
            "runc".to_string(),
            Arc::new(crate::runtime::RuncBackend::new(RuncRuntime::new(
                runtime_path.clone(),
                dir.path().join("runtime-root"),
            ))) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("runc".to_string(), 240)]),
    );
    let mut rx = service.events.subscribe();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-start-created".to_string(),
        test_pod("pod-start-created", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-created".to_string(),
        test_container(
            "container-start-created",
            "pod-start-created",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-created",
            "pod-start-created",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-created", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-created".to_string(),
        }),
    )
    .await
    .expect_err("container that stays created should fail start");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("did not reach running"));

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-created")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerCreated as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
    assert!(state.started_at.is_none());
    assert!(state.finished_at.is_none());
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-start-created")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "created");
    assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());
}

#[tokio::test]
async fn start_container_fails_when_runtime_exits_immediately_and_persists_exit_state() {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script_with_start_state(dir.path(), "stopped");
    let shim_path = write_quick_exit_shim(dir.path());
    let (dir, mut service) =
        test_service_with_runtime_path_and_shim_path(dir, runtime_path.clone(), Some(shim_path));
    service.runtime = RuntimeRegistry::new(
        "runc".to_string(),
        HashMap::from([(
            "runc".to_string(),
            Arc::new(crate::runtime::RuncBackend::new(RuncRuntime::new(
                runtime_path.clone(),
                dir.path().join("runtime-root"),
            ))) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("runc".to_string(), 240)]),
    );
    let mut rx = service.events.subscribe();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-start-stopped".to_string(),
        test_pod("pod-start-stopped", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-stopped".to_string(),
        test_container(
            "container-start-stopped",
            "pod-start-stopped",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-stopped",
            "pod-start-stopped",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-stopped", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-stopped".to_string(),
        }),
    )
    .await
    .expect_err("container that exits immediately should fail start");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("exited"));

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-stopped")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
    assert_eq!(state.exit_code, Some(0));
    assert!(state.finished_at.is_some());
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-start-stopped")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    let event = timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("stopped event should be published")
        .expect("stopped event receiver should succeed");
    assert_eq!(
        event.container_event_type,
        ContainerEventType::ContainerStoppedEvent as i32
    );
}

#[tokio::test]
async fn finalize_container_stop_state_only_sets_finished_at_for_exited_state() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            started_at: Some(11),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-stop-finalize".to_string(),
        test_container("container-stop-finalize", "pod-1", annotations),
    );
    let persisted_annotations = service
        .containers
        .lock()
        .await
        .get("container-stop-finalize")
        .unwrap()
        .annotations
        .clone();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-finalize",
            "pod-1",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &persisted_annotations,
        )
        .unwrap();

    service
        .finalize_container_stop_state("container-stop-finalize", ContainerStatus::Created)
        .await
        .unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("container-stop-finalize")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.started_at, Some(11));
    assert!(state.finished_at.is_none());
    assert!(state.exit_code.is_none());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn remove_after_failed_start_does_not_repeat_nri_stop() {
    let _guard = env_lock().lock().await;
    let fake_nri = Arc::new(FakeNri::default());
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script_with_start_state(dir.path(), "stopped");
    let (dir, service) =
        test_service_with_runtime_path_without_shim_and_nri(dir, runtime_path, fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-remove".to_string(),
        test_pod("pod-start-remove", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-remove".to_string(),
        test_container(
            "container-start-remove",
            "pod-start-remove",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-remove",
            "pod-start-remove",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-remove", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-remove".to_string(),
        }),
    )
    .await
    .expect_err("runtime start failure should be surfaced");
    assert_eq!(err.code(), tonic::Code::Internal);

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-start-remove".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "stop_container", "remove_container"]
    );
}

#[tokio::test]
async fn stop_container_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-stop".to_string(), test_pod("pod-stop", HashMap::new()));
    service.containers.lock().await.insert(
        "container-stop".to_string(),
        test_container("container-stop", "pod-stop", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop",
            "pod-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("first stop should succeed");
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("repeated stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &service
            .containers
            .lock()
            .await
            .get("container-stop")
            .unwrap()
            .annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
}

#[tokio::test]
async fn stop_container_notifies_nri_with_post_stop_state() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-state".to_string(),
        test_pod("pod-stop-state", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-stop-state".to_string(),
        test_container(
            "container-stop-state",
            "pod-stop-state",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-state",
            "pod-stop-state",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop-state", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop-state".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed");

    let stop_event = fake_nri
        .stop_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("stop event should be recorded");
    assert_eq!(
        stop_event.container.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_STOPPED
    );
}

#[tokio::test]
async fn stop_container_succeeds_when_nri_stop_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_stop_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-fail".to_string(),
        test_pod("pod-stop-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-stop-fail".to_string(),
        test_container("container-stop-fail", "pod-stop-fail", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-fail",
            "pod-stop-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop-fail", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop-fail".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("NRI stop failure should not fail container stop");
}

#[tokio::test]
async fn stop_container_skips_nri_for_already_exited_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-exited".to_string(),
        test_pod("pod-exited", HashMap::new()),
    );
    let mut container = test_container("container-exited", "pod-exited", annotations.clone());
    container.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-exited".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-exited",
            "pod-exited",
            crate::runtime::ContainerStatus::Stopped(17),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-exited", "stopped");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-exited".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed");

    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn stop_container_converts_unknown_runtime_state_to_exited() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-unknown".to_string(),
        test_pod("pod-unknown", HashMap::new()),
    );
    let mut container = test_container("container-unknown", "pod-unknown", annotations.clone());
    container.state = ContainerState::ContainerRunning as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-unknown".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-unknown",
            "pod-unknown",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-unknown".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed for unknown runtime state");

    let container = service
        .containers
        .lock()
        .await
        .get("container-unknown")
        .cloned()
        .expect("container should remain tracked");
    assert_eq!(container.state, ContainerState::ContainerExited as i32);

    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .expect("internal state should be updated");
    assert!(state.finished_at.is_some());
    assert_eq!(state.exit_code, Some(-1));

    tokio::time::sleep(Duration::from_millis(50)).await;
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-unknown")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    assert_eq!(persisted.exit_code, Some(-1));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn stop_container_falls_back_to_persistence_when_memory_state_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-stop".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-stop",
            "pod-persisted-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-persisted-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("fallback stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    assert!(
        fake_runtime_state_path(&dir, "container-persisted-stop").exists(),
        "runtime stop should leave a stopped runtime state artifact"
    );

    let container = service
        .containers
        .lock()
        .await
        .get("container-persisted-stop")
        .cloned()
        .expect("fallback stop should rehydrate container into memory");
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
    assert!(state.finished_at.is_some());
    assert_eq!(state.exit_code, Some(0));

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-persisted-stop")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    assert_eq!(persisted.exit_code, Some(0));
}

#[tokio::test]
async fn stop_container_fallback_is_idempotent_across_repeated_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-repeat-stop".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-repeat-stop",
            "pod-persisted-repeat-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-repeat-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-persisted-repeat-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("first fallback stop should succeed");
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-persisted-repeat-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("second fallback stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
}

#[tokio::test]
async fn remove_container_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove".to_string(),
        test_pod("pod-remove", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove".to_string(),
        test_container("container-remove", "pod-remove", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove",
            "pod-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove".to_string(),
        }),
    )
    .await
    .expect("first remove should succeed");
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove".to_string(),
        }),
    )
    .await
    .expect("repeated remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
}

#[tokio::test]
async fn remove_container_succeeds_when_nri_remove_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_remove_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-fail".to_string(),
        test_pod("pod-remove-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove-fail".to_string(),
        test_container(
            "container-remove-fail",
            "pod-remove-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove-fail",
            "pod-remove-fail",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove-fail", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove-fail".to_string(),
        }),
    )
    .await
    .expect("NRI remove failure should not fail container removal");
}

#[tokio::test]
async fn remove_container_notifies_nri_stop_before_remove_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-running".to_string(),
        test_pod("pod-remove-running", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove-running".to_string(),
        test_container(
            "container-remove-running",
            "pod-remove-running",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove-running",
            "pod-remove-running",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove-running", "running");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove-running".to_string(),
        }),
    )
    .await
    .expect("remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "remove_container"]
    );
}

#[tokio::test]
async fn remove_container_falls_back_to_persistence_when_memory_state_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-remove".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-remove",
            "pod-persisted-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-persisted-remove".to_string(),
        }),
    )
    .await
    .expect("fallback remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
    assert!(
        !dir.path()
            .join("runtime-root")
            .join("container-persisted-remove")
            .exists(),
        "bundle directory should be removed"
    );
    assert!(
        service
            .containers
            .lock()
            .await
            .get("container-persisted-remove")
            .is_none(),
        "fallback remove should clean rehydrated memory state"
    );
    assert!(
        service
            .persistence
            .lock()
            .await
            .storage()
            .get_container("container-persisted-remove")
            .unwrap()
            .is_none(),
        "fallback remove should delete persisted state"
    );
}

#[tokio::test]
async fn remove_container_fallback_is_idempotent_across_repeated_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("persisted-repeat-remove".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-persisted-repeat-remove",
            "pod-persisted-repeat-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-persisted-repeat-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-persisted-repeat-remove".to_string(),
        }),
    )
    .await
    .expect("first fallback remove should succeed");
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-persisted-repeat-remove".to_string(),
        }),
    )
    .await
    .expect("second fallback remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
}

#[tokio::test]
async fn stop_pod_sandbox_stops_member_containers_before_pod_notification() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-order".to_string(),
        test_pod("pod-stop-order", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-under-pod-stop".to_string(),
        test_container("container-under-pod-stop", "pod-stop-order", HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-under-pod-stop",
            "pod-stop-order",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-under-pod-stop", "running");

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-stop-order".to_string(),
        }),
    )
    .await
    .expect("pod stop should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "stop_pod"]
    );
    assert_eq!(fake_nri.stop_pod_events.lock().await.len(), 1);
}

#[tokio::test]
async fn remove_pod_sandbox_removes_member_containers_before_pod_notification() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-order".to_string(),
        test_pod("pod-remove-order", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-under-pod-remove".to_string(),
        test_container(
            "container-under-pod-remove",
            "pod-remove-order",
            HashMap::new(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-under-pod-remove",
            "pod-remove-order",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-under-pod-remove", "running");

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-remove-order".to_string(),
        }),
    )
    .await
    .expect("pod remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "remove_container", "remove_pod"]
    );
}

#[tokio::test]
async fn start_container_clears_nri_stop_notification_marker() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            nri_stop_notified: true,
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-restart".to_string(),
        test_pod("pod-restart", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-restart".to_string(),
        test_container("container-restart", "pod-restart", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-restart",
            "pod-restart",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-restart", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-restart".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-restart".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container", "stop_container",]
    );
}

#[tokio::test]
async fn reopen_container_log_validates_running_state_and_log_path() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "missing".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    let mut stopped_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut stopped_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("stopped.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", stopped_annotations),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");

    let stopped = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-stopped".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    let log_path = dir.path().join("logs").join("running.log");
    let mut running_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut running_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", running_annotations),
    );
    set_fake_runtime_state_without_shim(&dir, "container-running", "running");

    let running = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(running.code(), tonic::Code::FailedPrecondition);
    assert!(running.message().contains("is not running"));
}

#[tokio::test]
async fn update_container_resources_validates_state_and_persists_resources() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "missing".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-stopped".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 256,
        cpu_period: 100_000,
        cpu_quota: 50_000,
        memory_limit_in_bytes: 128 * 1024 * 1024,
        cpuset_cpus: "0-1".to_string(),
        cpuset_mems: "0".to_string(),
        ..Default::default()
    };

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(resources.clone()),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let status = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-running".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let linux = status.resources.unwrap().linux.unwrap();
    assert_eq!(linux.cpu_shares, 256);
    assert_eq!(linux.memory_limit_in_bytes, 128 * 1024 * 1024);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        persisted_state
            .linux_resources
            .unwrap()
            .memory_limit_in_bytes,
        128 * 1024 * 1024
    );

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 256);
    assert_eq!(update_payload["memory"]["limit"], 128 * 1024 * 1024);
}

#[tokio::test]
async fn update_container_resources_fails_when_cgroup_support_is_disabled() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.disable_cgroup = true;

    let err = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("disable_cgroup"));
}

#[tokio::test]
async fn update_container_resources_fails_when_hugetlb_is_missing_and_tolerance_is_disabled() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let err = RuntimeServiceImpl::validate_proto_hugetlb_limits_with_flags(
        Some(&crate::proto::runtime::v1::LinuxContainerResources {
            hugepage_limits: vec![crate::proto::runtime::v1::HugepageLimit {
                page_size: "2MB".to_string(),
                limit: 1,
            }],
            ..Default::default()
        }),
        CgroupResourceSupport {
            swap: true,
            hugetlb: false,
            memory_kernel: true,
            memory_kernel_tcp: true,
            memory_swappiness: true,
            memory_disable_oom_killer: true,
            memory_use_hierarchy: true,
            cpu_realtime: true,
            blockio: true,
            rdt: true,
        },
        false,
        "container resource update",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn update_container_resources_accepts_paused_container() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-paused".to_string(),
        test_container("container-paused", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-paused", "paused");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-paused",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-paused".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 777,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .expect("paused container should accept resource updates");

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-paused")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 777);
}

#[tokio::test]
async fn update_container_resources_applies_nri_result_before_post_update() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut target_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut target_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", target_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &target_annotations,
        )
        .unwrap();

    let mut sidecar_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut sidecar_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-1", sidecar_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-sidecar", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &sidecar_annotations,
        )
        .unwrap();

    let mut target_resources = crate::nri_proto::api::LinuxResources::new();
    let mut target_cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 1024;
    target_cpu.shares = protobuf::MessageField::some(shares);
    target_resources.cpu = protobuf::MessageField::some(target_cpu);

    let mut sidecar_update = crate::nri_proto::api::ContainerUpdate::new();
    sidecar_update.container_id = "container-sidecar".to_string();
    let mut sidecar_linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut sidecar_resources = crate::nri_proto::api::LinuxResources::new();
    let mut sidecar_memory = crate::nri_proto::api::LinuxMemory::new();
    let mut sidecar_limit = crate::nri_proto::api::OptionalInt64::new();
    sidecar_limit.value = 64 * 1024 * 1024;
    sidecar_memory.limit = protobuf::MessageField::some(sidecar_limit);
    sidecar_resources.memory = protobuf::MessageField::some(sidecar_memory);
    sidecar_linux.resources = protobuf::MessageField::some(sidecar_resources);
    sidecar_update.linux = protobuf::MessageField::some(sidecar_linux);

    *fake_nri.update_result.lock().await = Some(NriUpdateContainerResult {
        linux_resources: Some(target_resources),
        updates: vec![sidecar_update],
        evictions: Vec::new(),
    });

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let target_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(target_payload["cpu"]["shares"], 1024);

    let sidecar_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar_payload["memory"]["limit"], 64 * 1024 * 1024);

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_container", "post_update_container"]
    );

    let post_update_event = fake_nri
        .post_update_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("post update event should be recorded");
    let post_update_linux = post_update_event
        .container
        .linux
        .as_ref()
        .expect("post update container should include linux state");
    assert_eq!(
        post_update_linux
            .resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|value| value.value),
        Some(1024)
    );
}

#[tokio::test]
async fn update_container_resources_succeeds_when_nri_post_update_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_update_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-update-post-fail".to_string(),
        test_pod("pod-update-post-fail", HashMap::new()),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-update-post-fail".to_string(),
        test_container(
            "container-update-post-fail",
            "pod-update-post-fail",
            annotations.clone(),
        ),
    );
    set_fake_runtime_state(&dir, "container-update-post-fail", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-update-post-fail",
            "pod-update-post-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-update-post-fail".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 512,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .expect("post-update failure should not fail resource update");

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-update-post-fail")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 512);
}

#[tokio::test]
async fn update_container_resources_applies_extended_nri_resources() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let mut extended = crate::nri_proto::api::LinuxResources::new();
    let mut memory = crate::nri_proto::api::LinuxMemory::new();
    let mut reservation = crate::nri_proto::api::OptionalInt64::new();
    reservation.value = 4096;
    memory.reservation = protobuf::MessageField::some(reservation);
    extended.memory = protobuf::MessageField::some(memory);
    extended.pids = Some(crate::nri_proto::api::LinuxPids {
        limit: 42,
        ..Default::default()
    })
    .into();
    extended
        .devices
        .push(crate::nri_proto::api::LinuxDeviceCgroup {
            allow: true,
            type_: "c".to_string(),
            access: "rwm".to_string(),
            major: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 10;
                protobuf::MessageField::some(value)
            },
            minor: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 200;
                protobuf::MessageField::some(value)
            },
            ..Default::default()
        });
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    extended.blockio_class = protobuf::MessageField::some(blockio_class);
    *fake_nri.update_result.lock().await = Some(NriUpdateContainerResult {
        linux_resources: Some(extended),
        updates: Vec::new(),
        evictions: Vec::new(),
    });

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 256);
    assert_eq!(payload["memory"]["reservation"], 4096);
    assert_eq!(payload["pids"]["limit"], 42);
    assert_eq!(payload["devices"][0]["type"], "c");
    assert_eq!(payload["devices"][0]["major"], 10);
    assert_eq!(payload["devices"][0]["minor"], 200);
    assert_eq!(payload["devices"][0]["access"], "rwm");
    assert_eq!(payload["blockIO"]["weight"], 500);
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_container", "post_update_container"]
    );

    let container = service
        .containers
        .lock()
        .await
        .get("container-running")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    let resources = state.linux_resources.unwrap();
    assert_eq!(resources.cpu_shares, 256);
    assert_eq!(resources.memory_reservation_in_bytes, Some(4096));
    assert_eq!(resources.pids_limit, Some(42));
    assert_eq!(resources.devices.len(), 1);
    assert_eq!(resources.devices[0].device_type.as_deref(), Some("c"));
    assert_eq!(resources.devices[0].major, Some(10));
    assert_eq!(resources.devices[0].minor, Some(200));
    assert_eq!(resources.blockio_class.as_deref(), Some("gold"));
}

#[tokio::test]
async fn stop_container_applies_nri_update_side_effects() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut target_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut target_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let mut sidecar_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut sidecar_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));
    service.containers.lock().await.insert(
        "container-stop".to_string(),
        test_container("container-stop", "pod-1", target_annotations.clone()),
    );
    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-1", sidecar_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-stop", "running");
    set_fake_runtime_state(&dir, "container-sidecar", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &target_annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &sidecar_annotations,
        )
        .unwrap();

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-sidecar".to_string();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 2048;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 2048);
    let sidecar = service
        .containers
        .lock()
        .await
        .get("container-sidecar")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &sidecar.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 2048);
}

#[tokio::test]
async fn nri_domain_apply_updates_updates_runtime_and_persistence() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", annotations.clone()));
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-running".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 512;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 512);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(persisted_state.linux_resources.unwrap().cpu_shares, 512);
}
