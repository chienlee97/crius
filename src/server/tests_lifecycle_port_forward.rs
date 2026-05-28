#[tokio::test]
async fn port_forward_validates_pod_state() {
    let service = test_service();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut not_ready_pod = test_pod("pod-2", HashMap::new());
    not_ready_pod.state = PodSandboxState::SandboxNotready as i32;
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-2".to_string(), not_ready_pod);
    let not_ready = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-2".to_string(),
            port: Vec::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(not_ready.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn port_forward_requires_existing_netns_and_returns_stream_url() {
    let service = test_service();
    let mut missing_netns_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut missing_netns_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-missing-netns".to_string(),
        test_pod("pod-missing-netns", missing_netns_annotations),
    );
    let missing_netns = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-missing-netns".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing_netns.code(), tonic::Code::FailedPrecondition);

    let netns_dir = tempdir().unwrap();
    let netns_path = netns_dir.path().join("netns");
    fs::write(&netns_path, "placeholder").unwrap();
    let mut ready_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut ready_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-ready".to_string(),
        test_pod("pod-ready", ready_annotations),
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;
    let response = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-ready".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/portforward/"));

    let mut host_network_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut host_network_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            namespace_options: Some(StoredNamespaceOptions {
                network: NamespaceMode::Node as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-host-network".to_string(),
        test_pod("pod-host-network", host_network_annotations),
    );
    let host_network = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-host-network".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(host_network.url.contains("/portforward/"));

    let multi_port = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-ready".to_string(),
            port: vec![80, 443],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(multi_port.url.contains("/portforward/"));
}

#[tokio::test]
async fn rootless_port_forward_requires_rootless_netns_path() {
    let dir = tempdir().unwrap();
    let rootless_netns_dir = dir.path().join("xdg-runtime").join("crius").join("netns");
    fs::create_dir_all(&rootless_netns_dir).unwrap();
    let mut config = test_runtime_config(dir.path().join("state"));
    config.rootless = crate::rootless::EffectiveRootlessConfig {
        enabled: true,
        current_uid: 1000,
        current_gid: 1000,
        in_user_namespace: true,
        xdg_runtime_dir: dir.path().join("xdg-runtime"),
        xdg_data_home: dir.path().join("xdg-data"),
        storage_root: dir.path().join("xdg-data").join("crius").join("storage"),
        runtime_root: dir.path().join("xdg-runtime").join("crius"),
        netns_dir: rootless_netns_dir.clone(),
        use_fuse_overlayfs: true,
        network_mode: crate::rootless::NetworkMode::Pasta,
        slirp4netns_path: PathBuf::from("slirp4netns"),
        pasta_path: PathBuf::from("pasta"),
        disable_cgroup: true,
        tolerate_missing_hugetlb_controller: true,
    };
    config.cni_config.set_netns_mount_dir(rootless_netns_dir.clone());
    let service = RuntimeServiceImpl::new(config);
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let legacy_netns_dir = dir.path().join("var-run-netns");
    fs::create_dir_all(&legacy_netns_dir).unwrap();
    let legacy_netns_path = legacy_netns_dir.join("pod-rootless-legacy");
    fs::write(&legacy_netns_path, "netns").unwrap();
    let mut legacy_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut legacy_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(legacy_netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-rootless-legacy".to_string(),
        test_pod("pod-rootless-legacy", legacy_annotations),
    );

    let err = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-rootless-legacy".to_string(),
            port: vec![8080],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("rootless port-forward"));
    assert!(err
        .message()
        .contains(&rootless_netns_dir.display().to_string()));

    let rootless_netns_path = rootless_netns_dir.join("pod-rootless-ready");
    fs::write(&rootless_netns_path, "netns").unwrap();
    let mut ready_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut ready_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(rootless_netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-rootless-ready".to_string(),
        test_pod("pod-rootless-ready", ready_annotations),
    );

    let response = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-rootless-ready".to_string(),
            port: vec![8080],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/portforward/"));
}

#[tokio::test]
async fn port_forward_rejects_invalid_ports() {
    let service = test_service();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let zero = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: vec![0],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(zero.code(), tonic::Code::InvalidArgument);
    assert!(zero.message().contains("1..=65535"));

    let too_large = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-1".to_string(),
            port: vec![65536],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(too_large.code(), tonic::Code::InvalidArgument);
    assert!(too_large.message().contains("1..=65535"));
}

#[tokio::test]
async fn port_forward_refreshes_stale_pause_container_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-portforward", "stopped");

    let netns_dir = tempdir().unwrap();
    let netns_path = netns_dir.path().join("netns");
    fs::write(&netns_path, "placeholder").unwrap();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-portforward".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-stale-ready".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-stale-ready", annotations)
        },
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let err = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-stale-ready".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("not ready"));
}

#[tokio::test]
async fn stop_and_remove_existing_container_support_repeat_calls() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "container-1", "running");
    service.containers.lock().await.insert(
        "container-1".to_string(),
        test_container("container-1", "pod-1", HashMap::new()),
    );

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-1".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-1".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    set_fake_runtime_state(&dir, "container-2", "running");
    service.containers.lock().await.insert(
        "container-2".to_string(),
        test_container("container-2", "pod-1", HashMap::new()),
    );
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-2".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-2".to_string(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_and_remove_existing_pod_support_repeat_calls() {
    let (dir, service) = test_service_with_fake_runtime();

    let pod = test_pod("pod-1", HashMap::new());
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), pod);
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-1".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-1".to_string(),
        }),
    )
    .await
    .unwrap();

    set_fake_runtime_state(&dir, "container-under-pod", "running");
    service.containers.lock().await.insert(
        "container-under-pod".to_string(),
        test_container("container-under-pod", "pod-2", HashMap::new()),
    );
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-2".to_string(), test_pod("pod-2", HashMap::new()));
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-2".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-2".to_string(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_pod_sandbox_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    set_fake_runtime_state(&dir, "pause-repeat", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-repeat".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let pod = test_pod("pod-repeat", annotations.clone());
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-repeat".to_string(), pod);
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: "pod-repeat".to_string(),
            config: crate::pod::PodSandboxConfig {
                name: "pod-repeat".to_string(),
                namespace: "default".to_string(),
                uid: "uid-repeat".to_string(),
                hostname: "pod-repeat".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: vec![],
                annotations: vec![],
                dns_config: None,
                port_mappings: vec![],
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: vec![],
                readonly_rootfs: false,
                pids_limit: None,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from("/var/run/netns/pod-repeat"),
            pause_container_id: "pause-repeat".to_string(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: RuntimeServiceImpl::now_nanos(),
            ip: String::new(),
            network_status: None,
        });
    }
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-repeat",
            "ready",
            "pod-repeat",
            "default",
            "uid-repeat",
            "/var/run/netns/pod-repeat",
            &HashMap::new(),
            &annotations,
            Some("pause-repeat"),
            None,
        )
        .unwrap();

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-repeat".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-repeat".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(fake_nri.stop_pod_events.lock().await.len(), 1);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_pod"]);
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-repeat")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &persisted_annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.stop_notified);
}

#[tokio::test]
async fn stop_pod_sandbox_fallback_cleans_pause_when_pod_manager_state_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-fallback-stop", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-fallback-stop".to_string()),
            netns_path: Some("/var/run/netns/pod-fallback-stop".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-fallback-stop".to_string(),
        test_pod("pod-fallback-stop", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-fallback-stop",
            "ready",
            "pod-fallback-stop",
            "default",
            "uid-fallback-stop",
            "/var/run/netns/pod-fallback-stop",
            &HashMap::new(),
            &annotations,
            Some("pause-fallback-stop"),
            None,
        )
        .unwrap();

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-fallback-stop".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(!fake_runtime_state_path(&dir, "pause-fallback-stop").exists());
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-fallback-stop")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn remove_pod_sandbox_fallback_cleans_workspace_when_pod_manager_state_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-fallback-remove", "running");
    let pod_workspace = dir
        .path()
        .join("root")
        .join("pods")
        .join("pod-fallback-remove");
    fs::create_dir_all(&pod_workspace).unwrap();
    fs::write(pod_workspace.join("marker"), "workspace").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-fallback-remove".to_string()),
            netns_path: Some("/var/run/netns/pod-fallback-remove".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-fallback-remove".to_string(),
        test_pod("pod-fallback-remove", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-fallback-remove",
            "ready",
            "pod-fallback-remove",
            "default",
            "uid-fallback-remove",
            "/var/run/netns/pod-fallback-remove",
            &HashMap::new(),
            &annotations,
            Some("pause-fallback-remove"),
            None,
        )
        .unwrap();

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-fallback-remove".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(!fake_runtime_state_path(&dir, "pause-fallback-remove").exists());
    assert!(!pod_workspace.exists());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-fallback-remove")
        .unwrap()
        .is_none());
}
