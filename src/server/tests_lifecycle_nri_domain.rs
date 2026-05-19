#[tokio::test]
async fn nri_domain_snapshot_excludes_exited_containers() {
    let (_dir, service) = test_service_with_fake_runtime();
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
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut running = test_container("container-running", "pod-1", annotations.clone());
    running.state = ContainerState::ContainerRunning as i32;
    let mut exited = test_container("container-exited", "pod-1", annotations);
    exited.state = ContainerState::ContainerExited as i32;

    let mut containers = service.containers.lock().await;
    containers.insert(running.id.clone(), running);
    containers.insert(exited.id.clone(), exited);
    drop(containers);

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let snapshot = domain.snapshot().await.unwrap();
    let ids = snapshot
        .containers
        .iter()
        .map(|container| container.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["container-running"]);
}

#[tokio::test]
async fn nri_domain_snapshot_includes_paused_runtime_containers() {
    let (dir, service) = test_service_with_fake_runtime();
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
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut paused = test_container("container-paused", "pod-1", annotations);
    paused.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert(paused.id.clone(), paused);
    set_fake_runtime_state(&dir, "container-paused", "paused");

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let snapshot = domain.snapshot().await.unwrap();
    assert_eq!(snapshot.containers.len(), 1);
    assert_eq!(snapshot.containers[0].id, "container-paused");
    assert_eq!(
        snapshot.containers[0].state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_PAUSED
    );
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_failures_marked_ignore_failure() {
    let (_dir, service) = test_service_with_fake_runtime();
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
    update.container_id = "missing".to_string();
    update.ignore_failure = true;

    let failed = domain.apply_updates(&[update.clone()]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_missing_containers() {
    let (_dir, service) = test_service_with_fake_runtime();
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
    update.container_id = "missing".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    linux_update.resources =
        protobuf::MessageField::some(crate::nri_proto::api::LinuxResources::new());
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update.clone()]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_accepts_short_container_ids() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running-long".to_string(),
        test_container("container-running-long", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running-long", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running-long",
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
    update.container_id = "container-run".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 768;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running-long")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 768);
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_non_mutable_containers() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-exited".to_string(),
        test_container("container-exited", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-exited", "stopped");

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
    update.container_id = "container-exited".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    linux_update.resources =
        protobuf::MessageField::some(crate::nri_proto::api::LinuxResources::new());
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_accepts_extended_resources() {
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
    resources.pids = Some(crate::nri_proto::api::LinuxPids {
        limit: 7,
        ..Default::default()
    })
    .into();
    resources
        .devices
        .push(crate::nri_proto::api::LinuxDeviceCgroup {
            allow: false,
            type_: "b".to_string(),
            access: "rw".to_string(),
            major: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 8;
                protobuf::MessageField::some(value)
            },
            minor: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 0;
                protobuf::MessageField::some(value)
            },
            ..Default::default()
        });
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 512);
    assert_eq!(payload["pids"]["limit"], 7);
    assert_eq!(payload["devices"][0]["type"], "b");
    assert_eq!(payload["devices"][0]["allow"], false);
    assert_eq!(payload["devices"][0]["major"], 8);
    assert_eq!(payload["blockIO"]["weight"], 500);
}

#[tokio::test]
async fn nri_domain_evict_stops_container_and_persists_state() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let mut container = test_container("container-running", "pod-1", annotations.clone());
    container.state = ContainerState::ContainerRunning as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-running".to_string(), container);
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));
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

    domain.evict("container-running", "policy").await.unwrap();

    let containers = service.containers.lock().await;
    assert_eq!(
        containers.get("container-running").unwrap().state,
        ContainerState::ContainerExited as i32
    );
    drop(containers);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.nri_stop_notified);
}

