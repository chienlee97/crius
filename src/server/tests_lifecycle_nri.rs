#[tokio::test]
async fn initialize_nri_starts_then_synchronizes_once() {
    let fake_nri = Arc::new(FakeNri::default());
    let nri_config = NriConfig {
        enable: true,
        ..Default::default()
    };
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(tempdir().unwrap().keep()),
        nri_config,
        fake_nri.clone(),
    );

    service
        .initialize_nri()
        .await
        .expect("initialize_nri should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start", "synchronize"]
    );
}

#[tokio::test]
async fn initialize_nri_skips_disabled_configuration() {
    let fake_nri = Arc::new(FakeNri::default());
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(tempdir().unwrap().keep()),
        NriConfig::default(),
        fake_nri.clone(),
    );

    service
        .initialize_nri()
        .await
        .expect("disabled NRI should be a no-op");

    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn rollback_failed_pod_sandbox_run_undoes_nri_and_local_state() {
    let fake_nri = Arc::new(FakeNri {
        fail_run_pod_sandbox: true,
        ..Default::default()
    });
    let nri_config = NriConfig {
        enable: true,
        ..Default::default()
    };
    let root_dir = tempdir().unwrap().keep();
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(root_dir.clone()),
        nri_config,
        fake_nri.clone(),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-rollback".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-rollback".to_string(),
        test_pod("pod-rollback", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-rollback",
            "ready",
            "pod",
            "default",
            "uid",
            "/tmp/netns-rollback",
            &HashMap::new(),
            &annotations,
            Some("pause-rollback"),
            None,
        )
        .unwrap();

    service
        .rollback_failed_pod_sandbox_run("pod-rollback")
        .await;

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_pod", "remove_pod"]
    );
    assert!(!service
        .pod_sandboxes
        .lock()
        .await
        .contains_key("pod-rollback"));
    let persistence = service.persistence.lock().await;
    assert!(persistence
        .storage()
        .get_pod_sandbox("pod-rollback")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn rollback_failed_container_create_undoes_nri_and_local_state() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_create_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let container_id = "ctr-create-undo".to_string();
    let pod_id = "pod-create-undo".to_string();
    let sidecar_id = "ctr-create-undo-sidecar".to_string();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert(pod_id.clone(), test_pod(&pod_id, HashMap::new()));
    service.containers.lock().await.insert(
        container_id.clone(),
        test_container(&container_id, &pod_id, HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            &container_id,
            &pod_id,
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["/bin/sh".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    service.containers.lock().await.insert(
        sidecar_id.clone(),
        test_container(&sidecar_id, &pod_id, HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            &sidecar_id,
            &pod_id,
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, &sidecar_id, "running");

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = sidecar_id.clone();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 333;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    let event = service
        .nri_container_event(&pod_id, &container_id, &HashMap::new())
        .await;
    fake_nri.create_container(event.clone()).await.unwrap();
    let _ = fake_nri.post_create_container(event.clone()).await;

    service
        .rollback_failed_container_create(&container_id, event)
        .await;

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec![
            "create",
            "post_create",
            "stop_container",
            "remove_container"
        ]
    );
    let containers = service.containers.lock().await;
    assert!(!containers.contains_key(&container_id));
    assert!(containers.contains_key(&sidecar_id));
    drop(containers);
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .list_containers()
        .unwrap()
        .iter()
        .all(|container| container.id != container_id));
    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, &sidecar_id)).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 333);
}

#[tokio::test]
async fn update_pod_sandbox_resources_updates_pause_container_and_notifies_nri() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update".to_string();
    let pause_id = "pause-pod-update".to_string();
    let netns_path = "/var/run/netns/pod-update".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-update".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update".to_string(),
                attempt: 0,
            }),
            state: PodSandboxState::SandboxReady as i32,
            annotations: annotations.clone(),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update".to_string(),
                hostname: "pod-update".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id.clone(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            &pod_id,
            "ready",
            "pod-update",
            "default",
            "uid-update",
            &netns_path,
            &HashMap::new(),
            &annotations,
            Some(&pause_id),
            None,
        )
        .unwrap();

    let overhead = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 128,
        ..Default::default()
    };
    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 512,
        memory_limit_in_bytes: 4096,
        ..Default::default()
    };

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id.clone(),
            overhead: Some(overhead.clone()),
            resources: Some(resources.clone()),
        }),
    )
    .await
    .unwrap();

    let update_payload: serde_json::Value =
        serde_json::from_slice(&fs::read(fake_runtime_update_path(&dir, &pause_id)).unwrap())
            .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 512);
    assert_eq!(update_payload["memory"]["limit"], 4096);
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_pod", "post_update_pod"]
    );

    let updated_pod = service
        .pod_sandboxes
        .lock()
        .await
        .get(&pod_id)
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &updated_pod.annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.overhead_linux_resources.unwrap().cpu_shares, 128);
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 512);
    let pod = service.nri_pod_event(&pod_id).await.pod.unwrap();
    let pod_linux = pod.linux.unwrap();
    assert_eq!(
        pod_linux
            .pod_overhead
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(128)
    );
    assert_eq!(
        pod_linux
            .pod_resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(512)
    );

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox(&pod_id)
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &persisted_annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        persisted_state.overhead_linux_resources.unwrap().cpu_shares,
        128
    );
    assert_eq!(
        persisted_state
            .linux_resources
            .unwrap()
            .memory_limit_in_bytes,
        4096
    );
}

#[tokio::test]
async fn update_pod_sandbox_resources_succeeds_when_nri_post_update_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_update_pod_sandbox: true,
        ..Default::default()
    });
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-post-fail".to_string();
    let pause_id = "pause-pod-update-post-fail".to_string();
    let netns_path = "/var/run/netns/pod-update-post-fail".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-update-post-fail".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-post-fail".to_string(),
                attempt: 0,
            }),
            state: PodSandboxState::SandboxReady as i32,
            annotations,
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-post-fail".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-post-fail".to_string(),
                hostname: "pod-update-post-fail".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id,
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect("post-update pod failure should not fail sandbox resource update");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_pod", "post_update_pod"]
    );
}

#[tokio::test]
async fn update_pod_sandbox_resources_fails_when_cgroup_support_is_disabled() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.disable_cgroup = true;

    let err = RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: "pod-any".to_string(),
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 128,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("disable_cgroup"));
}

#[tokio::test]
async fn update_pod_sandbox_resources_fails_when_pod_is_not_ready() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-not-ready".to_string();
    let pause_id = "pause-pod-update-not-ready".to_string();
    let netns_path = "/var/run/netns/pod-update-not-ready".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            state: PodSandboxState::SandboxNotready as i32,
            annotations,
            ..test_pod(&pod_id, HashMap::new())
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-not-ready".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-not-ready".to_string(),
                hostname: "pod-update-not-ready".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id,
            state: crate::pod::PodSandboxState::NotReady,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    let err = RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect_err("not ready pod should reject resource updates");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("not ready"));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn update_pod_sandbox_resources_fails_when_pause_container_is_missing() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-missing-pause".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: None,
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            state: PodSandboxState::SandboxReady as i32,
            annotations,
            ..test_pod(&pod_id, HashMap::new())
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-missing-pause".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-missing-pause".to_string(),
                hostname: "pod-update-missing-pause".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::new(),
            pause_container_id: String::new(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    let err = RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect_err("ready pod without pause container should reject resource updates");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("pause container"));
    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn update_pod_sandbox_resources_overhead_only_updates_pause_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-overhead-only".to_string();
    let pause_id = "pause-pod-update-overhead-only".to_string();
    let netns_path = "/var/run/netns/pod-update-overhead-only".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            state: PodSandboxState::SandboxReady as i32,
            annotations: annotations.clone(),
            runtime_handler: "runc".to_string(),
            ..test_pod(&pod_id, HashMap::new())
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-overhead-only".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-overhead-only".to_string(),
                hostname: "pod-update-overhead-only".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id.clone(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id.clone(),
            overhead: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 321,
                ..Default::default()
            }),
            resources: None,
        }),
    )
    .await
    .unwrap();

    let update_payload: serde_json::Value =
        serde_json::from_slice(&fs::read(fake_runtime_update_path(&dir, &pause_id)).unwrap())
            .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 321);

    let updated_pod = service
        .pod_sandboxes
        .lock()
        .await
        .get(&pod_id)
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &updated_pod.annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.overhead_linux_resources.unwrap().cpu_shares, 321);
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 321);
}

#[tokio::test]
async fn nri_pod_event_uses_pause_spec_resources_for_linux_resources() {
    let (dir, service) = test_service_with_fake_runtime();
    let pod_id = "pod-nri-linux-resources".to_string();
    let pause_id = "pause-nri-linux-resources".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            annotations,
            ..Default::default()
        },
    );

    write_test_bundle_config_with_linux(
        &dir,
        &pause_id,
        &HashMap::new(),
        serde_json::json!({
            "resources": {
                "memory": {
                    "limit": 8192
                },
                "cpu": {
                    "shares": 1024
                }
            }
        }),
    );

    let pod = service.nri_pod_event(&pod_id).await.pod.unwrap();
    let linux = pod.linux.unwrap();
    let resources = linux
        .resources
        .into_option()
        .expect("pause OCI resources should be exposed");
    assert_eq!(
        resources
            .memory
            .as_ref()
            .and_then(|memory| memory.limit.as_ref())
            .map(|limit| limit.value),
        Some(8192)
    );
    assert_eq!(
        resources
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(1024)
    );
}

#[tokio::test]
async fn nri_create_result_applies_adjustments_and_side_effects() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
    adjustment
        .annotations
        .insert("plugin.annotation".to_string(), "set".to_string());
    adjustment.env.push(crate::nri_proto::api::KeyValue {
        key: "PLUGIN_ENV".to_string(),
        value: "enabled".to_string(),
        ..Default::default()
    });
    adjustment.args = vec![String::new(), "/bin/echo".to_string(), "plugin".to_string()];

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-update".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 2048;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let create_result = NriCreateContainerResult {
        adjustment,
        updates: vec![update],
        evictions: vec![crate::nri_proto::api::ContainerEviction {
            container_id: "container-evict".to_string(),
            reason: "plugin request".to_string(),
            ..Default::default()
        }],
    };

    service.pod_sandboxes.lock().await.insert(
        "pod-create".to_string(),
        test_pod("pod-create", HashMap::new()),
    );

    let mut update_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut update_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let update_container =
        test_container("container-update", "pod-create", update_annotations.clone());
    let evict_container = test_container("container-evict", "pod-create", HashMap::new());
    service
        .containers
        .lock()
        .await
        .insert("container-update".to_string(), update_container.clone());
    service
        .containers
        .lock()
        .await
        .insert("container-evict".to_string(), evict_container.clone());
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-update",
            "pod-create",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &update_annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-evict",
            "pod-create",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-update", "running");
    set_fake_runtime_state(&dir, "container-evict", "running");

    let mut stored_annotations = HashMap::new();
    stored_annotations.insert("existing".to_string(), "value".to_string());
    let container_config = ContainerConfig {
        name: "created".to_string(),
        image: "busybox:latest".to_string(),
        command: vec!["sleep".to_string()],
        args: vec!["10".to_string()],
        env: Vec::new(),
        working_dir: None,
        mounts: Vec::new(),
        labels: Vec::new(),
        annotations: stored_annotations
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        privileged: false,
        user: None,
        run_as_group: None,
        supplemental_groups: Vec::new(),
        hostname: None,
        tty: false,
        stdin: false,
        stdin_once: false,
        log_path: None,
        readonly_rootfs: false,
        seccomp_notifier: None,
        pids_limit: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        seccomp_profile: None,
        capabilities: None,
        cgroup_parent: None,
        sysctls: HashMap::new(),
        namespace_options: None,
        namespace_paths: NamespacePaths::default(),
        linux_resources: None,
        devices: Vec::new(),
        masked_paths: Vec::new(),
        readonly_paths: Vec::new(),
        rootfs: dir
            .path()
            .join("root")
            .join("containers")
            .join("created")
            .join("rootfs"),
    };
    let mut spec = service
        .runtime
        .build_spec("created", &container_config)
        .unwrap();
    let mut nri_event = NriContainerEvent::default();

    service
        .process_nri_create_side_effects(&create_result)
        .await
        .unwrap();
    apply_container_adjustment(&mut spec, &create_result.adjustment).unwrap();
    RuntimeServiceImpl::apply_adjusted_annotations(
        &mut stored_annotations,
        &create_result.adjustment,
    );
    RuntimeServiceImpl::refresh_nri_event_container_from_spec(
        &mut nri_event,
        &spec,
        &stored_annotations,
    );

    assert_eq!(
        stored_annotations.get("plugin.annotation"),
        Some(&"set".to_string())
    );
    assert_eq!(
        spec.annotations.as_ref().unwrap().get("plugin.annotation"),
        Some(&"set".to_string())
    );
    assert_eq!(
        spec.process.as_ref().unwrap().env.as_ref().unwrap(),
        &vec!["PLUGIN_ENV=enabled".to_string()]
    );
    assert_eq!(
        spec.process.as_ref().unwrap().args,
        vec!["/bin/echo".to_string(), "plugin".to_string()]
    );
    assert_eq!(
        nri_event.container.annotations.get("plugin.annotation"),
        Some(&"set".to_string())
    );

    let updated = service
        .containers
        .lock()
        .await
        .get("container-update")
        .cloned()
        .unwrap();
    let updated_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &updated.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        updated_state
            .linux_resources
            .as_ref()
            .map(|resources| resources.cpu_shares),
        Some(2048)
    );
    let evicted = service
        .containers
        .lock()
        .await
        .get("container-evict")
        .cloned()
        .unwrap();
    assert_eq!(evicted.state, ContainerState::ContainerExited as i32);
}

#[test]
fn enrich_container_annotations_adds_upstream_runtime_metadata() {
    let mut annotations = HashMap::new();
    annotations.insert("existing".to_string(), "value".to_string());
    let pod_state = StoredPodState {
        runtime_handler: "runc".to_string(),
        ..Default::default()
    };
    let log_path = PathBuf::from("/var/log/pods/workload.log");

    RuntimeServiceImpl::enrich_container_annotations(
        super::annotations::ContainerAnnotationContext {
            annotations: &mut annotations,
            container_id: "container-1",
            pod_sandbox_id: "pod-1",
            metadata_name: Some("workload"),
            requested_image: Some("docker.io/library/busybox:latest"),
            resolved_image_name: Some("busybox:latest"),
            log_path: Some(&log_path),
            pod_state: Some(&pod_state),
            default_runtime: "fallback-runtime",
        },
    );

    assert_eq!(
        annotations.get("existing").map(String::as_str),
        Some("value")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_CONTAINER)
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_CONTAINER)
    );
    assert_eq!(
        annotations
            .get(CRIO_RUNTIME_HANDLER_ANNOTATION)
            .map(String::as_str),
        Some("runc")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_RUNTIME_HANDLER_ANNOTATION)
            .map(String::as_str),
        Some("runc")
    );
    assert_eq!(
        annotations
            .get(CRIO_LOG_PATH_ANNOTATION)
            .map(String::as_str),
        Some("/var/log/pods/workload.log")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("workload")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("workload")
    );
    assert_eq!(
        annotations
            .get(CRIO_USER_REQUESTED_IMAGE_ANNOTATION)
            .map(String::as_str),
        Some("docker.io/library/busybox:latest")
    );
    assert_eq!(
        annotations
            .get(CRIO_IMAGE_NAME_ANNOTATION)
            .map(String::as_str),
        Some("busybox:latest")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_IMAGE_NAME_ANNOTATION)
            .map(String::as_str),
        Some("busybox:latest")
    );
}

#[test]
fn apply_runtime_handler_default_annotations_injects_missing_values_only() {
    let mut service = test_service();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .default_annotations = HashMap::from([
        ("io.example/default".to_string(), "kata".to_string()),
        ("existing".to_string(), "from-default".to_string()),
    ]);
    let mut annotations = HashMap::from([("existing".to_string(), "user-value".to_string())]);

    service.apply_runtime_handler_default_annotations(&mut annotations, "kata");

    assert_eq!(
        annotations.get("io.example/default").map(String::as_str),
        Some("kata")
    );
    assert_eq!(
        annotations.get("existing").map(String::as_str),
        Some("user-value")
    );
}

#[test]
fn nri_allowed_annotation_prefixes_include_existing_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let annotations = HashMap::from([
        ("existing.annotation".to_string(), "value".to_string()),
        (
            INTERNAL_CONTAINER_STATE_KEY.to_string(),
            "{\"hidden\":true}".to_string(),
        ),
    ]);

    let allowed = service
        .nri_allowed_annotation_prefixes(&annotations, &annotations, "runc")
        .unwrap();
    assert!(allowed.iter().any(|item| item == "existing.annotation"));
    assert!(!allowed
        .iter()
        .any(|item| item == INTERNAL_CONTAINER_STATE_KEY));
}

#[test]
fn external_pod_annotations_filter_container_reserved_keys_but_keep_user_annotations() {
    let annotations = HashMap::from([
        (
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            "app".to_string(),
        ),
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "default".to_string(),
        ),
        ("example.com/custom".to_string(), "value".to_string()),
    ]);

    let filtered = RuntimeServiceImpl::external_pod_annotations(&annotations);

    assert!(!filtered.contains_key(CRIO_CONTAINER_NAME_ANNOTATION));
    assert_eq!(
        filtered
            .get(CRIO_POD_NAMESPACE_ANNOTATION)
            .map(String::as_str),
        Some("default")
    );
    assert_eq!(
        filtered.get("example.com/custom").map(String::as_str),
        Some("value")
    );
}

#[test]
fn external_container_annotations_filter_pod_reserved_keys_but_keep_user_annotations() {
    let annotations = HashMap::from([
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "default".to_string(),
        ),
        (
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            "app".to_string(),
        ),
        ("example.com/custom".to_string(), "value".to_string()),
    ]);

    let filtered = RuntimeServiceImpl::external_container_annotations(&annotations);

    assert!(!filtered.contains_key(CRIO_POD_NAMESPACE_ANNOTATION));
    assert_eq!(
        filtered
            .get(CRIO_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("app")
    );
    assert_eq!(
        filtered.get("example.com/custom").map(String::as_str),
        Some("value")
    );
}

#[test]
fn external_container_annotations_keep_kubelet_container_metadata_annotations() {
    let annotations = HashMap::from([
        (
            "io.kubernetes.container.restartCount".to_string(),
            "1".to_string(),
        ),
        (
            "io.kubernetes.container.terminationMessagePath".to_string(),
            "/dev/termination-log".to_string(),
        ),
        (
            "io.kubernetes.container.terminationMessagePolicy".to_string(),
            "File".to_string(),
        ),
    ]);

    let filtered = RuntimeServiceImpl::external_container_annotations(&annotations);

    assert_eq!(
        filtered
            .get("io.kubernetes.container.restartCount")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        filtered
            .get("io.kubernetes.container.terminationMessagePath")
            .map(String::as_str),
        Some("/dev/termination-log")
    );
    assert_eq!(
        filtered
            .get("io.kubernetes.container.terminationMessagePolicy")
            .map(String::as_str),
        Some("File")
    );
}

#[test]
fn merge_external_pod_annotations_prefers_spec_for_allowed_pod_keys_only() {
    let base = HashMap::from([
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "default".to_string(),
        ),
        ("example.com/custom".to_string(), "base".to_string()),
    ]);
    let spec = HashMap::from([
        (
            CRIO_POD_NAMESPACE_ANNOTATION.to_string(),
            "kube-system".to_string(),
        ),
        (
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            "should-be-hidden".to_string(),
        ),
        ("example.com/custom".to_string(), "spec".to_string()),
    ]);

    let merged = RuntimeServiceImpl::merge_external_pod_annotations(&base, Some(&spec));

    assert_eq!(
        merged
            .get(CRIO_POD_NAMESPACE_ANNOTATION)
            .map(String::as_str),
        Some("kube-system")
    );
    assert!(!merged.contains_key(CRIO_CONTAINER_NAME_ANNOTATION));
    assert_eq!(
        merged.get("example.com/custom").map(String::as_str),
        Some("spec")
    );
}

#[test]
fn filter_nri_annotation_adjustments_drops_unlisted_keys() {
    let (_dir, service) = test_service_with_fake_runtime();
    let existing = HashMap::from([("existing.annotation".to_string(), "value".to_string())]);
    let adjustments = HashMap::from([
        ("existing.annotation".to_string(), "updated".to_string()),
        ("disallowed.annotation".to_string(), "value".to_string()),
    ]);

    let filtered = service
        .filter_nri_annotation_adjustments(&existing, &adjustments, &existing, "runc")
        .unwrap();
    assert_eq!(
        filtered,
        HashMap::from([("existing.annotation".to_string(), "updated".to_string())])
    );
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_and_workload_overrides() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .nri_config
        .runtime_allowed_annotation_prefixes
        .insert(
            "kata".to_string(),
            vec!["io.kubernetes.cri-o.RuntimeHandler.kata".to_string()],
        );
    service.nri_config.workload_allowed_annotation_prefixes = vec![NriAnnotationWorkloadConfig {
        activation_annotation: "workload.example/class".to_string(),
        activation_value: "latency".to_string(),
        allowed_annotation_prefixes: vec!["workload.example/".to_string()],
    }];

    let activation_annotations =
        HashMap::from([("workload.example/class".to_string(), "latency".to_string())]);
    let existing_annotations = HashMap::new();

    let allowed = service
        .nri_allowed_annotation_prefixes(&activation_annotations, &existing_annotations, "kata")
        .unwrap();

    assert!(allowed
        .iter()
        .any(|item| item == "io.kubernetes.cri-o.RuntimeHandler.kata"));
    assert!(allowed.iter().any(|item| item == "workload.example/"));
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_workload_profile_allowed_annotations() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.workloads.insert(
        "management".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            allowed_annotations: vec!["workload.profile/".to_string()],
            ..Default::default()
        },
    );

    let activation_annotations = HashMap::from([(
        "target.workload.openshift.io/management".to_string(),
        "{\"effect\":\"PreferredDuringScheduling\"}".to_string(),
    )]);
    let allowed = service
        .nri_allowed_annotation_prefixes(&activation_annotations, &HashMap::new(), "runc")
        .unwrap();

    assert!(allowed.iter().any(|item| item == "workload.profile/"));
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_handler_allowed_annotations() {
    let mut service = test_service();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .allowed_annotations = vec!["io.example.runtime/".to_string()];

    let allowed = service
        .nri_allowed_annotation_prefixes(&HashMap::new(), &HashMap::new(), "kata")
        .unwrap();

    assert!(allowed.iter().any(|item| item == "io.example.runtime/"));
}

#[test]
fn workload_resources_from_annotation_merges_defaults_and_overrides() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.workloads.insert(
        "management".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            resources: crate::config::RuntimeWorkloadResources {
                cpu_shares: 1024,
                cpu_period: 100_000,
                cpu_limit: 1500,
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let annotations = HashMap::from([
        (
            "target.workload.openshift.io/management".to_string(),
            "{\"effect\":\"PreferredDuringScheduling\"}".to_string(),
        ),
        (
            "resources.workload.openshift.io/main".to_string(),
            "{\"cpushares\":2048,\"cpuset\":\"0-2\"}".to_string(),
        ),
    ]);

    let resources = service
        .workload_resources_from_annotation(&annotations, "main")
        .unwrap()
        .expect("workload resources should be present");
    assert_eq!(resources.cpu_shares, 2048);
    assert_eq!(resources.cpu_period, 100_000);
    assert_eq!(resources.cpu_limit, 1500);
    assert_eq!(resources.cpuset_cpus, "0-2");
}

#[test]
fn apply_workload_resources_overrides_cpu_settings() {
    let mut resources = StoredLinuxResources::default();
    RuntimeServiceImpl::apply_workload_resources(
        &mut resources,
        &crate::config::RuntimeWorkloadResources {
            cpu_shares: 2048,
            cpu_period: 100_000,
            cpu_limit: 1500,
            cpuset_cpus: "0-2".to_string(),
            ..Default::default()
        },
    );

    assert_eq!(resources.cpu_shares, 2048);
    assert_eq!(resources.cpu_period, 100_000);
    assert_eq!(resources.cpu_quota, 150_000);
    assert_eq!(resources.cpuset_cpus, "0-2");
}

#[test]
fn selected_workload_profile_rejects_multiple_activations() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service.config.workloads.insert(
        "management".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            ..Default::default()
        },
    );
    service.config.workloads.insert(
        "latency".to_string(),
        crate::config::RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/latency".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            ..Default::default()
        },
    );

    let err = service
        .selected_workload_profile(&HashMap::from([
            (
                "target.workload.openshift.io/management".to_string(),
                "{}".to_string(),
            ),
            (
                "target.workload.openshift.io/latency".to_string(),
                "{}".to_string(),
            ),
        ]))
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("multiple workload profiles"));
}

#[test]
fn sanitize_nri_linux_resources_with_flags_clears_unsupported_fields() {
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut memory = crate::nri_proto::api::LinuxMemory::new();
    let mut swap = crate::nri_proto::api::OptionalInt64::new();
    swap.value = 4096;
    memory.swap = protobuf::MessageField::some(swap);
    let mut kernel = crate::nri_proto::api::OptionalInt64::new();
    kernel.value = 1024;
    memory.kernel = protobuf::MessageField::some(kernel);
    resources.memory = protobuf::MessageField::some(memory);
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut runtime = crate::nri_proto::api::OptionalInt64::new();
    runtime.value = 1000;
    cpu.realtime_runtime = protobuf::MessageField::some(runtime);
    resources.cpu = protobuf::MessageField::some(cpu);
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = crate::nri_proto::api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);
    resources
        .hugepage_limits
        .push(crate::nri_proto::api::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
            ..Default::default()
        });

    RuntimeServiceImpl::sanitize_nri_linux_resources_with_flags(
        &mut resources,
        CgroupResourceSupport {
            swap: false,
            hugetlb: false,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: false,
            rdt: false,
        },
    );

    let memory = resources.memory.as_ref().unwrap();
    assert!(memory.swap.is_none());
    assert!(memory.kernel.is_none());
    assert!(resources.cpu.as_ref().unwrap().realtime_runtime.is_none());
    assert!(resources.hugepage_limits.is_empty());
    assert!(resources.blockio_class.is_none());
    assert!(resources.rdt_class.is_none());
}

#[test]
fn sanitize_nri_adjustment_for_nri_config_clears_blockio_without_config() {
    let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
    let mut linux = crate::nri_proto::api::LinuxContainerAdjustment::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = crate::nri_proto::api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);
    linux.resources = protobuf::MessageField::some(resources);
    let mut rdt = crate::nri_proto::api::LinuxRdt::new();
    let mut clos_id = crate::nri_proto::api::OptionalString::new();
    clos_id.value = "silver".to_string();
    rdt.clos_id = protobuf::MessageField::some(clos_id);
    linux.rdt = protobuf::MessageField::some(rdt);
    adjustment.linux = protobuf::MessageField::some(linux);

    RuntimeServiceImpl::sanitize_nri_adjustment_for_nri_config(
        &mut adjustment,
        &NriConfig::default(),
    );

    let linux = adjustment.linux.as_ref().unwrap();
    let resources = linux.resources.as_ref().unwrap();
    assert!(resources.blockio_class.is_none());
    if !Path::new("/sys/fs/resctrl").exists() {
        assert!(resources.rdt_class.is_none());
        assert!(linux.rdt.is_none());
    }
}

