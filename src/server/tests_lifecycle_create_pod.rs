#[tokio::test]
async fn create_container_rejects_missing_image_spec() {
    let service = test_service();
    service.pod_sandboxes.lock().await.insert(
        "pod-missing-image".to_string(),
        test_pod("pod-missing-image", HashMap::new()),
    );

    let err = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-missing-image".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "missing-image".to_string(),
                    attempt: 1,
                }),
                image: None,
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect_err("missing image spec should be rejected");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("ContainerConfig.Image is nil"));
    let pod_metadata = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-missing-image")
        .and_then(|pod| pod.metadata.clone())
        .unwrap();
    let name_key = RuntimeServiceImpl::container_name_key(
        &ContainerMetadata {
            name: "missing-image".to_string(),
            attempt: 1,
        },
        &pod_metadata,
    );
    assert!(
        service.container_id_for_reserved_name(&name_key).is_none(),
        "invalid request must not reserve container names"
    );
}

#[tokio::test]
async fn create_container_rejects_not_ready_sandbox() {
    let service = test_service();
    let mut pod = test_pod("pod-notready", HashMap::new());
    pod.state = PodSandboxState::SandboxNotready as i32;
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-notready".to_string(), pod);

    let err = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-notready".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "workload".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    user_specified_image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect_err("not-ready sandbox should be rejected");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("sandbox is not ready"));
    assert!(err.message().contains("pod-notready"));
}
#[tokio::test]
async fn create_container_reclaims_stale_reserved_name_without_live_container() {
    let service = test_service();
    let name_key = "container:workload:pod-stale-name:default:uid:1".to_string();
    let mut stale_guard = service
        .reserve_container_name("stale-container", &name_key)
        .expect("stale reservation should succeed");
    stale_guard.disarm();

    let guard = service
        .reserve_container_name_for_create("fresh-container", &name_key)
        .await
        .expect("stale reservation should be reclaimed");
    let reserved = service
        .container_id_for_reserved_name(&name_key)
        .expect("name should be reserved for the new create attempt");
    assert_eq!(reserved, "fresh-container");
    drop(guard);
}

#[tokio::test]
async fn create_container_reclaims_exited_reserved_name_without_live_runtime() {
    let (_dir, service) = test_service_with_fake_runtime();
    let name_key = "container:workload:pod-exited-name:default:uid:1".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("workload".to_string()),
            metadata_attempt: Some(1),
            finished_at: Some(RuntimeServiceImpl::now_nanos()),
            exit_code: Some(-1),
            ..Default::default()
        },
    )
    .unwrap();

    let mut stale_guard = service
        .reserve_container_name("exited-container", &name_key)
        .expect("stale reservation should succeed");
    stale_guard.disarm();
    service.containers.lock().await.insert(
        "exited-container".to_string(),
        Container {
            state: ContainerState::ContainerExited as i32,
            metadata: Some(ContainerMetadata {
                name: "workload".to_string(),
                attempt: 1,
            }),
            pod_sandbox_id: "pod-exited-name".to_string(),
            annotations: annotations.clone(),
            ..Default::default()
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "exited-container",
            "pod-exited-name",
            crate::runtime::ContainerStatus::Stopped(-1),
            "busybox:latest",
            &["sleep".to_string()],
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let guard = service
        .reserve_container_name_for_create("fresh-container", &name_key)
        .await
        .expect("exited reservation should be reclaimed");
    let reserved = service
        .container_id_for_reserved_name(&name_key)
        .expect("name should be reserved for the new create attempt");
    assert_eq!(reserved, "fresh-container");
    assert!(service
        .containers
        .lock()
        .await
        .get("exited-container")
        .is_none());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("exited-container")
        .unwrap()
        .is_none());
    drop(guard);
}

#[tokio::test]
async fn create_container_direct_task_backend_skips_oci_context() {
    let dir = tempdir().unwrap();
    let runtime_root = dir.path().join("direct-runtime-root");
    let direct_backend = Arc::new(common::DirectTaskRuntimeBackend::new(&runtime_root));
    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime = "wasm".to_string();
    config.runtime_handlers = vec!["wasm".to_string()];
    config.runtime_root = runtime_root.clone();
    config.runtime_configs = HashMap::from([(
        "wasm".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "wasm".to_string(),
            backend_options: HashMap::from([("sandboxer".to_string(), "test".to_string())]),
            runtime_path: "/definitely/missing/wasm-runtime".to_string(),
            runtime_config_path: String::new(),
            runtime_root: runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/definitely/missing/crius-shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 30,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    )]);
    let service = RuntimeServiceImpl::new_with_runtime_backends(
        config,
        HashMap::from([(
            "wasm".to_string(),
            direct_backend.clone() as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
    );
    let mut pod = test_pod("pod-direct-task", HashMap::new());
    pod.runtime_handler = "wasm".to_string();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-direct-task".to_string(), pod);

    let response = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-direct-task".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "direct-workload".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "wasm.example/workload:v1".to_string(),
                    user_specified_image: "wasm.example/workload:v1".to_string(),
                    ..Default::default()
                }),
                command: vec!["run".to_string()],
                args: vec!["module.wasm".to_string()],
                envs: vec![crate::proto::runtime::v1::KeyValue {
                    key: "RUNTIME".to_string(),
                    value: "direct".to_string(),
                }],
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect("direct task backend should create workload without local OCI context")
    .into_inner();
    assert!(!response.container_id.is_empty());

    let calls = direct_backend.calls();
    assert_eq!(
        calls,
        vec![common::FakeRuntimeCall::CreateContainer {
            container_id: response.container_id.clone()
        }]
    );
    assert!(!runtime_root.join(&response.container_id).join("config.json").exists());
    let record = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container(&response.container_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.runtime_handler.as_deref(), Some("wasm"));
    assert_eq!(record.runtime_backend.as_deref(), Some("direct-task"));
    let events = service
        .internal_services
        .events
        .recent_internal_events("container", &response.container_id, 10)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.kind == "container.create_start"
            && event.details["name"] == "direct-workload"));
    assert!(events
        .iter()
        .any(|event| event.kind == "container.create_success"
            && event.details["runtimeBackend"] == "direct-task"));

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: response.container_id.clone(),
        }),
    )
    .await
    .expect("direct task backend should start workload");
    let calls = direct_backend.calls();
    assert!(calls.contains(&common::FakeRuntimeCall::StartContainer {
        container_id: response.container_id.clone()
    }));
    assert!(calls.contains(&common::FakeRuntimeCall::ContainerStatus {
        container_id: response.container_id,
    }));
}

#[tokio::test]
async fn create_container_applies_cdi_devices_without_nri_adjustment() {
    let cdi_dir = tempdir().unwrap();
    fs::write(
        cdi_dir.path().join("vendor-device.json"),
        r#"{
            "cdiVersion": "0.7.0",
            "kind": "vendor.com/device",
            "containerEdits": {
                "env": ["GLOBAL_CDI=1"]
            },
            "devices": [{
                "name": "gpu0",
                "containerEdits": {
                    "env": ["GPU0=1"],
                    "deviceNodes": [{
                        "path": "/dev/fake-gpu0",
                        "type": "c",
                        "major": 195,
                        "minor": 0,
                        "permissions": "rw"
                    }]
                }
            }, {
                "name": "gpu1",
                "containerEdits": {
                    "env": ["GPU1=1"],
                    "deviceNodes": [{
                        "path": "/dev/fake-gpu1",
                        "type": "c",
                        "major": 195,
                        "minor": 1,
                        "permissions": "rw"
                    }]
                }
            }]
        }"#,
    )
    .unwrap();
    let dir = tempdir().unwrap();
    let runtime_root = dir.path().join("fake-runtime-root");
    let fake_backend = Arc::new(common::FakeRuntimeBackend::new(&runtime_root));
    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime = "fake".to_string();
    config.runtime_handlers = vec!["fake".to_string()];
    config.runtime_root = runtime_root.clone();
    config.runtime_configs = HashMap::from([(
        "fake".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "fake".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/fake/runtime".to_string(),
            runtime_config_path: String::new(),
            runtime_root: runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/fake/shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 30,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    )]);
    let mut nri_config = NriConfig {
        enable: false,
        enable_cdi: true,
        cdi_spec_dirs: vec![cdi_dir.path().display().to_string()],
        ..Default::default()
    };
    nri_config.blockio_config_path = String::new();
    let mut service = RuntimeServiceImpl::new_with_runtime_backends(
        config,
        HashMap::from([(
            "fake".to_string(),
            fake_backend as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
    );
    service.nri_config = nri_config;
    let mut pod = test_pod("pod-cdi", HashMap::new());
    pod.runtime_handler = "fake".to_string();
    service.pod_sandboxes.lock().await.insert(
        "pod-cdi".to_string(),
        pod,
    );

    let response = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-cdi".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "workload-cdi".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    user_specified_image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                command: vec!["sleep".to_string()],
                args: vec!["1".to_string()],
                annotations: HashMap::from([(
                    "cdi.k8s.io/vendor_devices".to_string(),
                    "vendor.com/device=gpu0,vendor.com/device=gpu1".to_string(),
                )]),
                cdi_devices: vec![crate::proto::runtime::v1::CdiDevice {
                    name: "vendor.com/device=gpu0".to_string(),
                }],
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect("CDI devices should be injected from the CRI field and annotations")
    .into_inner();

    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            dir.path()
                .join("fake-runtime-root")
                .join(&response.container_id)
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let env = bundle_config["process"]["env"].as_array().unwrap();
    assert!(env.iter().any(|entry| entry == "GLOBAL_CDI=1"));
    assert!(env.iter().any(|entry| entry == "GPU0=1"));
    assert!(env.iter().any(|entry| entry == "GPU1=1"));
    let devices = bundle_config["linux"]["devices"].as_array().unwrap();
    assert!(devices
        .iter()
        .any(|device| device["path"] == "/dev/fake-gpu0"));
    assert!(devices
        .iter()
        .any(|device| device["path"] == "/dev/fake-gpu1"));
    assert_eq!(
        devices
            .iter()
            .filter(|device| device["path"] == "/dev/fake-gpu0")
            .count(),
        1
    );
}

#[tokio::test]
async fn create_container_merges_nri_cdi_devices_without_duplicate_injection() {
    let cdi_dir = tempdir().unwrap();
    fs::write(
        cdi_dir.path().join("vendor-device.json"),
        r#"{
            "cdiVersion": "0.7.0",
            "kind": "vendor.com/device",
            "devices": [{
                "name": "gpu0",
                "containerEdits": {
                    "env": ["GPU0=1"],
                    "deviceNodes": [{
                        "path": "/dev/fake-gpu0",
                        "type": "c",
                        "major": 195,
                        "minor": 0,
                        "permissions": "rw"
                    }]
                }
            }, {
                "name": "gpu2",
                "containerEdits": {
                    "env": ["GPU2=1"],
                    "deviceNodes": [{
                        "path": "/dev/fake-gpu2",
                        "type": "c",
                        "major": 195,
                        "minor": 2,
                        "permissions": "rw"
                    }]
                }
            }]
        }"#,
    )
    .unwrap();
    let dir = tempdir().unwrap();
    let runtime_root = dir.path().join("fake-runtime-root");
    let fake_backend = Arc::new(common::FakeRuntimeBackend::new(&runtime_root));
    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime = "fake".to_string();
    config.runtime_handlers = vec!["fake".to_string()];
    config.runtime_root = runtime_root.clone();
    config.runtime_configs = HashMap::from([(
        "fake".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "fake".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/fake/runtime".to_string(),
            runtime_config_path: String::new(),
            runtime_root: runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/fake/shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 30,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    )]);
    let fake_nri = Arc::new(FakeNri {
        create_result: tokio::sync::Mutex::new(Some(NriCreateContainerResult {
            adjustment: {
                let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
                adjustment
                    .CDI_devices
                    .push(crate::nri_proto::api::CDIDevice {
                        name: "vendor.com/device=gpu0".to_string(),
                        ..Default::default()
                    });
                adjustment
                    .CDI_devices
                    .push(crate::nri_proto::api::CDIDevice {
                        name: "vendor.com/device=gpu2".to_string(),
                        ..Default::default()
                    });
                adjustment
            },
            ..Default::default()
        })),
        ..Default::default()
    });
    let mut service = RuntimeServiceImpl::new_with_nri_api(
        config,
        NriConfig {
            enable: true,
            enable_cdi: true,
            cdi_spec_dirs: vec![cdi_dir.path().display().to_string()],
            ..Default::default()
        },
        fake_nri,
    );
    service.runtime = RuntimeRegistry::new(
        "fake".to_string(),
        HashMap::from([(
            "fake".to_string(),
            fake_backend as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("fake".to_string(), 30)]),
    );
    let mut pod = test_pod("pod-cdi-nri", HashMap::new());
    pod.runtime_handler = "fake".to_string();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-cdi-nri".to_string(), pod);

    let response = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-cdi-nri".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "workload-cdi-nri".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    user_specified_image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                cdi_devices: vec![crate::proto::runtime::v1::CdiDevice {
                    name: "vendor.com/device=gpu0".to_string(),
                }],
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect("NRI CDI devices should be merged with CRI CDI devices")
    .into_inner();

    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            runtime_root
                .join(&response.container_id)
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let devices = bundle_config["linux"]["devices"].as_array().unwrap();
    assert_eq!(
        devices
            .iter()
            .filter(|device| device["path"] == "/dev/fake-gpu0")
            .count(),
        1
    );
    assert_eq!(
        devices
            .iter()
            .filter(|device| device["path"] == "/dev/fake-gpu2")
            .count(),
        1
    );
}

#[tokio::test]
async fn create_container_applies_blockio_and_rdt_classes_from_annotations() {
    let dir = tempdir().unwrap();
    let blockio_config_path = dir.path().join("blockio.json");
    fs::write(
        &blockio_config_path,
        r#"{
            "classes": {
                "pod-gold": { "weight": 300 },
                "container-gold": { "weight": 700 }
            }
        }"#,
    )
    .unwrap();
    let runtime_root = dir.path().join("fake-runtime-root");
    let fake_backend = Arc::new(common::FakeRuntimeBackend::new(&runtime_root));
    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime = "fake".to_string();
    config.runtime_handlers = vec!["fake".to_string()];
    config.runtime_root = runtime_root.clone();
    config.runtime_configs = HashMap::from([(
        "fake".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "fake".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/fake/runtime".to_string(),
            runtime_config_path: String::new(),
            runtime_root: runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/fake/shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 30,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    )]);
    let mut service = RuntimeServiceImpl::new_with_runtime_backends(
        config,
        HashMap::from([(
            "fake".to_string(),
            fake_backend as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
    );
    service.nri_config.blockio_config_path = blockio_config_path.display().to_string();
    let mut pod = test_pod(
        "pod-resource-class",
        HashMap::from([
            (
                "blockio.resources.beta.kubernetes.io/pod".to_string(),
                "pod-gold".to_string(),
            ),
            (
                "rdt.resources.beta.kubernetes.io/pod".to_string(),
                "pod-rdt".to_string(),
            ),
        ]),
    );
    pod.runtime_handler = "fake".to_string();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-resource-class".to_string(), pod);

    let response = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-resource-class".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "workload-resource-class".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    user_specified_image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                annotations: HashMap::from([
                    (
                        "io.kubernetes.cri.blockio-class".to_string(),
                        "container-gold".to_string(),
                    ),
                    (
                        "io.kubernetes.cri.rdt-class".to_string(),
                        "container-rdt".to_string(),
                    ),
                ]),
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect("resource class annotations should be applied during create")
    .into_inner();

    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            runtime_root
                .join(&response.container_id)
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(bundle_config["linux"]["resources"]["block_io"]["weight"], 700);
    assert_eq!(
        bundle_config["linux"]["intelRdt"]["closId"],
        "container-rdt"
    );

    let container = service
        .containers
        .lock()
        .await
        .get(&response.container_id)
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    let resources = state.linux_resources.unwrap();
    assert_eq!(resources.blockio_class.as_deref(), Some("container-gold"));
    assert_eq!(resources.rdt_class.as_deref(), Some("container-rdt"));
}

#[tokio::test]
async fn create_container_rejects_empty_image_ref() {
    let service = test_service();
    service.pod_sandboxes.lock().await.insert(
        "pod-empty-image".to_string(),
        test_pod("pod-empty-image", HashMap::new()),
    );

    let err = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: "pod-empty-image".to_string(),
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "empty-image".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "   ".to_string(),
                    user_specified_image: String::new(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            sandbox_config: None,
        }),
    )
    .await
    .expect_err("empty image ref should be rejected");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("Image.Image is empty"));
    let pod_metadata = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-empty-image")
        .and_then(|pod| pod.metadata.clone())
        .unwrap();
    let name_key = RuntimeServiceImpl::container_name_key(
        &ContainerMetadata {
            name: "empty-image".to_string(),
            attempt: 1,
        },
        &pod_metadata,
    );
    assert!(
        service.container_id_for_reserved_name(&name_key).is_none(),
        "invalid request must not reserve container names"
    );
}

#[test]
fn build_container_status_snapshot_preserves_empty_legacy_image_ref() {
    let container = Container {
        id: "legacy-empty-image".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        state: ContainerState::ContainerCreated as i32,
        metadata: Some(ContainerMetadata {
            name: "legacy".to_string(),
            attempt: 1,
        }),
        image: None,
        image_ref: String::new(),
        annotations: HashMap::new(),
        created_at: RuntimeServiceImpl::now_nanos(),
        ..Default::default()
    };

    let status = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerCreated as i32,
    );
    assert_eq!(status.image_ref, "");
    assert_eq!(
        status.image.as_ref().map(|image| image.image.as_str()),
        Some("")
    );
}

#[test]
fn resolve_container_log_path_joins_relative_path_under_sandbox_log_directory() {
    let path = RuntimeServiceImpl::resolve_container_log_path(
        Some("/var/log/pods/default_demo_uid"),
        "workload/0.log",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        path,
        PathBuf::from("/var/log/pods/default_demo_uid").join("workload/0.log")
    );
}

#[test]
fn resolve_container_log_path_rejects_absolute_path_when_sandbox_log_directory_is_set() {
    let err = RuntimeServiceImpl::resolve_container_log_path(
        Some("/var/log/pods/default_demo_uid"),
        "/var/log/pods/default_demo_uid/workload/0.log",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("must be relative"));
}

#[test]
fn resolve_container_log_path_rejects_parent_dir_escape_when_sandbox_log_directory_is_set() {
    let err = RuntimeServiceImpl::resolve_container_log_path(
        Some("/var/log/pods/default_demo_uid"),
        "../escape.log",
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("must not escape"));
}

#[test]
fn resolve_container_log_path_requires_absolute_path_without_sandbox_log_directory() {
    let err = RuntimeServiceImpl::resolve_container_log_path(None, "relative.log").unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("must be absolute"));

    let absolute = RuntimeServiceImpl::resolve_container_log_path(
        None,
        "/var/log/pods/default_demo_uid/workload/0.log",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        absolute,
        PathBuf::from("/var/log/pods/default_demo_uid/workload/0.log")
    );
}

#[tokio::test]
async fn recover_state_rebuilds_reserved_pod_and_container_names() {
    let (_dir, service) = test_service_with_fake_runtime();
    let pod_metadata = PodSandboxMetadata {
        name: "recover-pod".to_string(),
        uid: "recover-pod-uid".to_string(),
        namespace: "default".to_string(),
        attempt: 1,
    };
    let pod_name_key = RuntimeServiceImpl::pod_name_key(&pod_metadata);
    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-recover-name".to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-recover-name",
            "ready",
            &pod_metadata.name,
            &pod_metadata.namespace,
            &pod_metadata.uid,
            "",
            &HashMap::new(),
            &pod_annotations,
            Some("pause-recover-name"),
            None,
        )
        .unwrap();

    let container_metadata = ContainerMetadata {
        name: "recover-container".to_string(),
        attempt: 1,
    };
    let container_name_key =
        RuntimeServiceImpl::container_name_key(&container_metadata, &pod_metadata);
    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some(container_metadata.name.clone()),
            metadata_attempt: Some(container_metadata.attempt),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-recover-name",
            "pod-recover-name",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["sleep".to_string(), "1".to_string()],
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    assert_eq!(
        service.pod_id_for_reserved_name(&pod_name_key).as_deref(),
        Some("pod-recover-name")
    );
    assert_eq!(
        service
            .container_id_for_reserved_name(&container_name_key)
            .as_deref(),
        Some("container-recover-name")
    );
}

#[tokio::test]
async fn run_pod_sandbox_auto_pulls_missing_pause_image_into_shared_image_service() {
    let (_dir, service) = test_service_with_fake_runtime();
    let pulls = Arc::new(StdMutex::new(Vec::new()));
    service.image_service().set_test_pull_handler(Arc::new({
        let pulls = pulls.clone();
        move |request| {
            pulls.lock().unwrap().push(request);
            Ok(crate::image::TestPullResponse {
                image_id: "sha256:pause-auto".to_string(),
                size: 64,
                ..Default::default()
            })
        }
    }));

    let response = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "pause-missing",
            "default",
            "pause-missing-uid",
            HashMap::new(),
        )),
    )
    .await
    .expect("missing local pause image should be auto-pulled");
    assert!(!response.into_inner().pod_sandbox_id.is_empty());

    {
        let calls = pulls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].requested_ref, "registry.k8s.io/pause:3.9");
        assert_eq!(calls[0].canonical_ref, "registry.k8s.io/pause:3.9");
        assert_eq!(calls[0].pull_namespace.as_deref(), Some("default"));
        assert_eq!(calls[0].auth, crate::image::TestRegistryAuth::Anonymous);
    }

    let status = ImageService::image_status(
        &service.image_service(),
        Request::new(crate::proto::runtime::v1::ImageStatusRequest {
            image: Some(crate::proto::runtime::v1::ImageSpec {
                image: "registry.k8s.io/pause:3.9".to_string(),
                user_specified_image: String::new(),
                runtime_handler: String::new(),
                annotations: HashMap::new(),
            }),
            verbose: false,
        }),
    )
    .await
    .expect("shared image service should expose the auto-pulled image")
    .into_inner();
    assert_eq!(
        status.image.expect("pause image should exist").id,
        "sha256:pause-auto"
    );
}

#[tokio::test]
async fn run_pod_sandbox_auto_pull_prefers_namespaced_auth_for_overridden_pause_image() {
    let (_dir, service) = test_service_with_fake_runtime_configure(|config| {
        let auth_root = config.root_dir.join("auth");
        let global_auth_file = auth_root.join("global.json");
        let namespaced_auth_dir = auth_root.join("namespaced");
        fs::create_dir_all(&namespaced_auth_dir).unwrap();
        fs::write(
            &global_auth_file,
            r#"{
                "auths": {
                    "registry.example": {
                        "auth": "Z2xvYmFsOnNlY3JldA=="
                    }
                }
            }"#,
        )
        .unwrap();
        let digest = format!("{:x}", Sha256::digest("registry.example/pause".as_bytes()));
        fs::write(
            namespaced_auth_dir.join(format!("default-{digest}.json")),
            r#"{
                "auths": {
                    "registry.example": {
                        "auth": "bmFtZXNwYWNlOnNlY3JldA=="
                    }
                }
            }"#,
        )
        .unwrap();
        config.image_global_auth_file = global_auth_file;
        config.image_namespaced_auth_dir = namespaced_auth_dir;
    });
    let pulls = Arc::new(StdMutex::new(Vec::new()));
    service.image_service().set_test_pull_handler(Arc::new({
        let pulls = pulls.clone();
        move |request| {
            pulls.lock().unwrap().push(request);
            Ok(crate::image::TestPullResponse {
                image_id: "sha256:pause-custom".to_string(),
                size: 64,
                ..Default::default()
            })
        }
    }));

    RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "pause-custom-missing",
            "default",
            "pause-custom-missing-uid",
            HashMap::from([(
                "io.kubernetes.cri.sandbox-image".to_string(),
                "registry.example/pause:custom".to_string(),
            )]),
        )),
    )
    .await
    .expect("overridden pause image should be auto-pulled with namespaced auth");

    let calls = pulls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].requested_ref, "registry.example/pause:custom");
    assert_eq!(calls[0].canonical_ref, "registry.example/pause:custom");
    assert_eq!(calls[0].pull_namespace.as_deref(), Some("default"));
    assert_eq!(
        calls[0].auth,
        crate::image::TestRegistryAuth::Basic {
            username: "namespace".to_string(),
            password: "secret".to_string(),
        }
    );
}

#[tokio::test]
async fn run_pod_sandbox_rejects_untrusted_workload_when_untrusted_handler_is_not_configured() {
    let (_dir, service) = test_service_with_fake_runtime();
    let err = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(pod_sandbox_request_with_namespace_options(
            "untrusted-missing-handler",
            "default",
            "untrusted-missing-handler-uid",
            HashMap::from([(
                CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
                "true".to_string(),
            )]),
            "",
            NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            },
        )),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err
        .message()
        .contains("unsupported runtime handler: untrusted"));
}

#[tokio::test]
async fn run_pod_sandbox_rejects_untrusted_workload_with_host_access() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());

    let err = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "untrusted-host-access",
            "default",
            "untrusted-host-access-uid",
            HashMap::from([(
                CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
                "true".to_string(),
            )]),
        )),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("host access"));
}

#[tokio::test]
async fn run_pod_sandbox_rejects_untrusted_workload_with_conflicting_handler() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .config
        .runtime_handlers
        .push("untrusted".to_string());
    service.config.runtime_handlers.push("kata".to_string());

    let err = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(pod_sandbox_request_with_namespace_options(
            "untrusted-conflicting-handler",
            "default",
            "untrusted-conflicting-handler-uid",
            HashMap::from([(
                CONTAINERD_UNTRUSTED_WORKLOAD_ANNOTATION.to_string(),
                "true".to_string(),
            )]),
            "kata",
            NamespaceOption {
                network: NamespaceMode::Pod as i32,
                pid: NamespaceMode::Pod as i32,
                ipc: NamespaceMode::Pod as i32,
                target_id: String::new(),
                userns_options: None,
            },
        )),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("explicit runtime handler"));
}

#[tokio::test]
async fn stop_and_remove_container_are_idempotent_when_missing() {
    let service = test_service();

    let stop = RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "missing".to_string(),
            timeout: 0,
        }),
    )
    .await;
    assert!(stop.is_ok());

    let remove = RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(remove.is_ok());
}

#[tokio::test]
async fn stop_and_remove_pod_are_idempotent_when_missing() {
    let service = test_service();

    let stop = RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(stop.is_ok());

    let remove = RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(remove.is_ok());
}

#[tokio::test]
async fn pod_lifecycle_internal_events_record_stop_and_remove() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-lifecycle".to_string()),
            netns_path: Some(dir.path().join("pod-lifecycle.netns").display().to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-lifecycle".to_string(),
        test_pod("pod-lifecycle", annotations),
    );
    set_fake_runtime_state(&dir, "pause-lifecycle", "running");

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-lifecycle".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-lifecycle".to_string(),
        }),
    )
    .await
    .unwrap();

    let events = service
        .internal_services
        .events
        .recent_internal_events("pod", "pod-lifecycle", 10)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.kind == "pod.stop_start"
            && event.details["previousState"] == "ready"));
    assert!(events
        .iter()
        .any(|event| event.kind == "pod.stop_success"
            && event.details["state"] == "notready"));
    assert!(events
        .iter()
        .any(|event| event.kind == "pod.remove_start"
            && event.details["hadPod"] == true));
    assert!(events
        .iter()
        .any(|event| event.kind == "pod.remove_success"));
}
