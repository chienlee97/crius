#[tokio::test]
async fn container_status_verbose_returns_json_info() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some("/var/log/pods/c1.log".to_string()),
            tty: true,
            privileged: true,
            seccomp_notifier_action: Some("stop".to_string()),
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                image: String::new(),
                image_sub_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
            run_as_user: Some("1000".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-1".to_string(),
        test_container("container-1", "pod-1", annotations),
    );
    service
        .ensure_seccomp_notifier("container-1", crate::runtime::SeccompNotifierMode::Stop)
        .unwrap();
    set_fake_runtime_state(&dir, "container-1", "running");
    let bundle_dir = dir.path().join("runtime-root").join("container-1");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "process": {
                "terminal": true,
                "cwd": "/"
            }
        })
        .to_string(),
    )
    .unwrap();

    let response = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-1".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.info.contains_key("info"));
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["id"], "container-1");
    assert_eq!(info["sandboxID"], "pod-1");
    assert_eq!(info["privileged"], true);
    assert_eq!(info["user"], "1000");
    assert_eq!(info["seccompNotifierAction"], "stop");
    assert_eq!(info["runtimeSpec"]["process"]["cwd"], "/");
    assert_eq!(response.status.unwrap().mounts.len(), 1);
}

#[tokio::test]
async fn pod_sandbox_status_verbose_returns_json_info() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    annotations.insert(
        "io.kubernetes.cri.sandbox-image".to_string(),
        "registry.k8s.io/pause:3.10".to_string(),
    );
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            hostname: Some("pod-hostname".to_string()),
            port_mappings: vec![StoredPortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: "127.0.0.1".to_string(),
            }],
            supplemental_groups: vec![2000, 2001],
            raw_cni_result: Some(serde_json::json!({
                "cniVersion": "1.0.0",
                "ips": [
                    {"address": "10.88.0.10/16"},
                    {"address": "10.88.0.11/16"}
                ]
            })),
            ip: Some("10.88.0.10".to_string()),
            additional_ips: vec!["10.88.0.11".to_string()],
            netns_path: Some("/var/run/netns/test-pod".to_string()),
            pause_container_id: Some("pause-1".to_string()),
            log_directory: Some("/var/log/pods/test-pod".to_string()),
            readonly_rootfs: true,
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", annotations));
    set_fake_runtime_state(&dir, "pause-1", "running");
    let bundle_dir = dir.path().join("runtime-root").join("pause-1");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": "rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-1".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.info.contains_key("info"));
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["id"], "pod-1");
    assert_eq!(info["hostname"], "pod-hostname");
    assert_eq!(info["ip"], "10.88.0.10");
    assert_eq!(info["additionalIPs"][0], "10.88.0.11");
    assert_eq!(info["supplementalGroups"][0], 2000);
    assert_eq!(info["supplementalGroups"][1], 2001);
    assert_eq!(info["readonlyRootfs"], true);
    assert_eq!(info["rawCniResult"]["cniVersion"], "1.0.0");
    assert_eq!(info["portMappings"][0]["protocol"], "TCP");
    assert_eq!(info["portMappings"][0]["containerPort"], 80);
    assert_eq!(info["portMappings"][0]["hostPort"], 8080);
    assert_eq!(info["image"], "registry.k8s.io/pause:3.10");
    assert_eq!(info["pauseContainerId"], "pause-1");
    assert!(info["pid"].is_number());
    assert_eq!(info["runtimeSpec"]["root"]["path"], "rootfs");
}

#[tokio::test]
async fn pod_sandbox_status_verbose_normalizes_ip_fields_from_stored_state() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-ip".to_string()),
            additional_ips: vec![
                "fd00::10".to_string(),
                "10.88.0.11".to_string(),
                "fd00::10".to_string(),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-ip".to_string(), test_pod("pod-ip", annotations));

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-ip".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();

    assert_eq!(info["ip"], "fd00::10");
    assert_eq!(info["additionalIPs"][0], "10.88.0.11");
}

#[tokio::test]
async fn pod_sandbox_status_snapshot_uses_stored_ip_as_network_status() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            ip: Some("10.88.0.20".to_string()),
            additional_ips: vec!["fd00::20".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-net".to_string(), test_pod("pod-net", annotations));

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-net".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let network = response
        .status
        .and_then(|status| status.network)
        .expect("network status should be populated from stored state");
    assert_eq!(network.ip, "10.88.0.20");
    assert_eq!(network.additional_ips[0].ip, "fd00::20");
}

#[tokio::test]
async fn status_verbose_returns_structured_config() {
    let mut service = test_service();
    service.config.grpc_max_send_msg_size = 12_345;
    service.config.grpc_max_recv_msg_size = 23_456;
    service.config.streaming.enable_tls = true;
    service.config.streaming.tls_cert_file = "/etc/crius/tls/tls.crt".to_string();
    service.config.streaming.tls_key_file = "/etc/crius/tls/tls.key".to_string();
    service.config.streaming.tls_ca_file = "/etc/crius/tls/ca.crt".to_string();
    service.config.streaming.tls_min_version = "VersionTLS13".to_string();
    service.config.privileged_seccomp_profile = "unconfined".to_string();
    service.config.apparmor_default_profile = "crius-default".to_string();
    service.config.disable_apparmor = true;
    service.config.enable_selinux = true;
    service.config.selinux_category_range = 64;
    service.config.hostnetwork_disable_selinux = false;
    service.nri_config.enable = true;
    service.nri_config.enable_cdi = false;
    service.nri_config.cdi_spec_dirs = vec!["/etc/cdi".to_string(), "/var/run/cdi".to_string()];
    service.nri_config.plugin_path = "/opt/nri/plugins".to_string();
    service.nri_config.plugin_config_path = "/etc/nri/conf.d".to_string();
    service.nri_config.blockio_config_path = "/etc/nri/blockio.json".to_string();
    service.config.default_env = vec![
        (
            "HTTP_PROXY".to_string(),
            "http://proxy.internal".to_string(),
        ),
        ("LANG".to_string(), "C.UTF-8".to_string()),
    ];
    service.config.default_capabilities =
        vec!["CAP_CHOWN".to_string(), "CAP_NET_BIND_SERVICE".to_string()];
    service.config.default_sysctls =
        HashMap::from([("kernel.shm_rmid_forced".to_string(), "1".to_string())]);
    service.config.image_default_transport = "docker://".to_string();
    service.config.image_short_name_mode = "enforcing".to_string();
    service.config.image_pull_progress_timeout = std::time::Duration::from_secs(25);
    service.config.image_max_concurrent_downloads = 5;
    service.config.image_pull_retry_count = 2;
    service.config.image_registry_config_dir = PathBuf::from("/etc/containerd/certs.d");
    service.config.image_storage_options = vec![
        "overlay.mount_program=/usr/bin/fuse-overlayfs".to_string(),
        "overlay.ignore_chown_errors=true".to_string(),
    ];
    service.config.image_volumes = "bind".to_string();
    service.config.image_pinned_images = vec![
        "busybox*".to_string(),
        "registry.k8s.io/pause:3.9".to_string(),
    ];
    service.config.hooks_dir = vec![
        PathBuf::from("/usr/share/containers/oci/hooks.d"),
        PathBuf::from("/etc/crius/hooks.d"),
    ];
    service.config.infra_ctr_cpuset = "1".to_string();
    service.config.shared_cpuset = "2-3".to_string();
    service.config.no_new_keyring = true;
    service.config.allowed_devices = vec![PathBuf::from("/dev/null"), PathBuf::from("/dev/zero")];
    service.config.additional_devices = vec![crate::runtime::DeviceMapping {
        source: PathBuf::from("/dev/null"),
        destination: PathBuf::from("/dev/custom-null"),
        permissions: "rw".to_string(),
    }];
    service.config.device_ownership_from_security_context = true;
    service.config.default_mounts_file = PathBuf::from("/etc/containers/mounts.conf");
    service.config.absent_mount_sources_to_reject = vec![PathBuf::from("/etc/hostname")];
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .container_create_timeout = 45;
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .runtime_config_path = "/etc/kata/config.toml".to_string();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .platform_runtime_paths = HashMap::from([(
        "linux/amd64".to_string(),
        "/usr/bin/kata-runtime-amd64".to_string(),
    )]);
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .monitor_cgroup = "system.slice".to_string();
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .backend_options = HashMap::from([
        ("sandbox_type".to_string(), "podsandbox".to_string()),
        ("sandbox_cgroup_only".to_string(), "true".to_string()),
    ]);
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .stream_websockets = true;
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .allowed_annotations = vec!["io.example.runtime/".to_string()];
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .default_annotations =
        HashMap::from([("io.example.runtime/default".to_string(), "kata".to_string())]);
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .privileged_without_host_devices = true;
    service
        .config
        .runtime_configs
        .get_mut("kata")
        .unwrap()
        .privileged_without_host_devices_all_devices_allowed = true;
    service
        .config
        .cni_config
        .set_netns_mount_dir(PathBuf::from("/tmp/crius-test-runtime-root/netns"));
    service
        .config
        .cni_config
        .set_netns_mounts_under_state_dir(true);
    service
        .config
        .cni_config
        .set_namespace_helper_path(Some(PathBuf::from("/usr/bin/pinns")));
    service
        .config
        .cni_config
        .set_handler_config_dirs("kata", vec![PathBuf::from("/etc/cni/kata.d")]);
    service
        .config
        .cni_config
        .set_handler_max_conf_num("kata", 2);
    service.config.rootless = crate::rootless::EffectiveRootlessConfig {
        enabled: true,
        current_uid: 1000,
        current_gid: 1000,
        in_user_namespace: true,
        xdg_runtime_dir: PathBuf::from("/run/user/1000"),
        xdg_data_home: PathBuf::from("/home/test/.local/share"),
        storage_root: PathBuf::from("/home/test/.local/share/crius/storage"),
        runtime_root: PathBuf::from("/run/user/1000/crius"),
        netns_dir: PathBuf::from("/run/user/1000/crius/netns"),
        use_fuse_overlayfs: true,
        network_mode: crate::rootless::NetworkMode::Pasta,
        slirp4netns_path: PathBuf::from("/usr/bin/slirp4netns"),
        pasta_path: PathBuf::from("/usr/bin/pasta"),
        disable_cgroup: true,
        tolerate_missing_hugetlb_controller: true,
    };
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.info.contains_key("config"));
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeName"], "crius");
    assert_eq!(config["runtimeVersion"], env!("CARGO_PKG_VERSION"));
    assert!(config["ociRuntimeVersion"].is_null());
    assert_eq!(config["defaultRuntimeHandler"], "runc");
    assert!(!config["runtimeHandlers"].as_array().unwrap().is_empty());
    assert_eq!(config["imageRoot"], "/tmp/crius-test-images");
    assert_eq!(config["imageDriver"], "overlay");
    assert_eq!(config["imageGlobalAuthFile"], "");
    assert_eq!(config["imageNamespacedAuthDir"], "");
    assert_eq!(config["imageDefaultTransport"], "docker://");
    assert_eq!(config["imageShortNameMode"], "enforcing");
    assert_eq!(config["imagePullProgressTimeoutMillis"], 25000);
    assert_eq!(config["imageMaxConcurrentDownloads"], 5);
    assert_eq!(config["imagePullRetryCount"], 2);
    assert_eq!(config["imageRegistryConfigDir"], "/etc/containerd/certs.d");
    assert_eq!(config["rootless"]["enabled"], true);
    assert_eq!(config["rootless"]["networkMode"], "pasta");
    assert_eq!(config["rootless"]["xdgRuntimeDir"], "/run/user/1000");
    assert_eq!(
        config["rootless"]["storageRoot"],
        "/home/test/.local/share/crius/storage"
    );
    assert_eq!(
        config["rootless"]["limitations"]["cgroup"]["reason"],
        "RootlessCgroupDisabled"
    );
    assert_eq!(
        config["rootless"]["limitations"]["devices"]["degraded"],
        true
    );
    assert_eq!(
        config["imageStorageOptions"],
        serde_json::json!([
            "overlay.mount_program=/usr/bin/fuse-overlayfs",
            "overlay.ignore_chown_errors=true"
        ])
    );
    assert_eq!(config["imageVolumes"], "bind");
    assert_eq!(
        config["pinnedImages"],
        serde_json::json!(["busybox*", "registry.k8s.io/pause:3.9"])
    );
    assert_eq!(config["imageLayout"]["mode"], "single-store-root");
    assert_eq!(
        config["imageLayout"]["imageRecordPathPattern"],
        "/tmp/crius-test-images/images/<imageID>"
    );
    assert_eq!(config["imageLayout"]["separateImageStoreSupported"], false);
    assert_eq!(
        config["imageSnapshotModel"]["snapshotter"],
        "internal-overlay-untar"
    );
    assert_eq!(config["imageSnapshotModel"]["storageDriver"], "overlay");
    assert_eq!(
        config["imageSnapshotModel"]["storageOptions"],
        serde_json::json!([
            "overlay.mount_program=/usr/bin/fuse-overlayfs",
            "overlay.ignore_chown_errors=true"
        ])
    );
    assert_eq!(
        config["imageSnapshotModel"]["externalSnapshotterSupported"],
        false
    );
    assert_eq!(
        config["imageSnapshotModel"]["runtimeSnapshotterOverrideSupported"],
        true
    );
    assert_eq!(
        config["imageSnapshotModel"]["snapshotAnnotationPassthrough"],
        false
    );
    assert_eq!(config["imageSnapshotModel"]["discardUnpackedLayers"], false);
    assert_eq!(config["imageSnapshotModel"]["pullOptionPassthrough"], false);
    assert_eq!(
        config["snapshotStatsCollection"]["strategy"],
        "on-demand-rootfs-walk"
    );
    assert_eq!(
        config["snapshotStatsCollection"]["backgroundCollector"],
        false
    );
    assert!(config["cgroupSupport"]["drivers"]["systemd"]["supported"]
        .as_bool()
        .unwrap());
    assert!(config["cgroupSupport"]["drivers"]["cgroupfs"]["supported"]
        .as_bool()
        .unwrap());
    assert_eq!(
        config["cgroupSupport"]["resourceUpdateStrategy"],
        "runtime-update-resources"
    );
    assert_eq!(
        config["cgroupSupport"]["resourceClasses"]["blockio"]["supported"],
        true
    );
    assert_eq!(
        config["cgroupSupport"]["resourceClasses"]["blockio"]["configPath"],
        "/etc/nri/blockio.json"
    );
    assert!(config["cgroupSupport"]["resourceClasses"]["rdt"]["supported"].is_boolean());
    assert_eq!(
        config["cgroupSupport"]["resourceClasses"]["rdt"]["resctrlPath"],
        "/sys/fs/resctrl"
    );
    assert_eq!(
        config["recovery"]["policy"]["repairScope"],
        "sqlite-persistence-and-ledger"
    );
    assert!(config["recovery"]["ledgerCheck"]["issueCount"].is_u64());
    assert!(config["internalServices"]["introspection"]["recovery"]["ledgerCheck"]["dryRun"]
        .as_bool()
        .unwrap());
    assert!(config["recovery"]["policy"]["wipeScope"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "orphanShimArtifacts"));
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["runtimePath"],
        "/definitely/missing/kata-runtime"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["monitorPath"],
        "/definitely/missing/crius-shim"
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["available"]
            .is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["idmapMounts"]
            .is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["checkpointRestore"]
            .is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["execTty"].is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["cgroup"].is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["rootless"].is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]["shimRpc"].is_boolean()
    );
    assert!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeDetectedFeatures"]
            ["recursiveReadOnlyMounts"]
            .is_boolean()
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["runtimeConfigPath"],
        "/etc/kata/config.toml"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["platformRuntimePaths"]["linux/amd64"],
        "/usr/bin/kata-runtime-amd64"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["monitorCgroup"],
        "system.slice"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["backendOptions"]["sandbox_type"],
        "podsandbox"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["backendOptions"]["sandbox_cgroup_only"],
        "true"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["streamWebsockets"],
        true
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["privilegedWithoutHostDevices"],
        true
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["privilegedWithoutHostDevicesAllDevicesAllowed"],
        true
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["runc"]["runtimeRoot"],
        "/tmp/crius-test-runtime-root"
    );
    assert!(config["monitorEnv"].as_array().unwrap().is_empty());
    assert_eq!(config["defaultEnv"][0], "HTTP_PROXY=http://proxy.internal");
    assert_eq!(config["defaultCapabilities"][0], "CAP_CHOWN");
    assert_eq!(config["defaultSysctls"]["kernel.shm_rmid_forced"], "1");
    assert_eq!(config["allowedDevices"][0], "/dev/null");
    assert_eq!(
        config["additionalDevices"][0],
        "/dev/null:/dev/custom-null:rw"
    );
    assert_eq!(config["deviceOwnershipFromSecurityContext"], true);
    assert_eq!(config["defaultMountsFile"], "/etc/containers/mounts.conf");
    assert_eq!(
        config["hooksDir"],
        serde_json::json!(["/usr/share/containers/oci/hooks.d", "/etc/crius/hooks.d"])
    );
    assert_eq!(config["absentMountSourcesToReject"][0], "/etc/hostname");
    assert_eq!(config["grpcMaxSendMsgSize"], 12345);
    assert_eq!(config["grpcMaxRecvMsgSize"], 23456);
    assert_eq!(config["privilegedSeccompProfile"], "unconfined");
    assert_eq!(config["apparmorDefaultProfile"], "crius-default");
    assert_eq!(config["disableApparmor"], true);
    assert_eq!(config["enableSelinux"], true);
    assert_eq!(config["selinuxCategoryRange"], 64);
    assert_eq!(config["hostnetworkDisableSelinux"], false);
    assert_eq!(config["streaming"]["enableTls"], true);
    assert_eq!(config["streaming"]["tlsCertFile"], "/etc/crius/tls/tls.crt");
    assert_eq!(config["streaming"]["tlsMinVersion"], "VersionTLS13");
    assert_eq!(
        config["reload"]["strategy"],
        "config-file-watch-and-cni-watch"
    );
    assert_eq!(config["reload"]["signalReload"], false);
    assert_eq!(config["reload"]["configFileWatch"], false);
    assert_eq!(
        config["reload"]["current"]["pauseImage"],
        "registry.k8s.io/pause:3.9"
    );
    assert!(config["reload"]["reloadableFields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "network.conf_template"));
    assert!(config["reload"]["watcherStatus"].is_string());
    assert!(config["reload"]["watcherBackoffCount"].is_u64());
    assert!(config["reload"]["watcherNextRetryUnixMillis"].is_null());
    assert_eq!(
        config["reload"]["runtimeConfigApiOnly"][0],
        "UpdateRuntimeConfig.network_config.pod_cidr"
    );
    assert!(config["runtimeHandlerConfigs"]["runc"]["monitorEnv"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(config["runtimeHandlerConfigs"]["kata"]["monitorEnv"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["allowedAnnotations"][0],
        "io.example.runtime/"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["defaultAnnotations"]["io.example.runtime/default"],
        "kata"
    );
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["containerCreateTimeoutSeconds"],
        45
    );
    assert_eq!(
        config["netnsMountDir"],
        "/tmp/crius-test-runtime-root/netns"
    );
    assert_eq!(config["netnsMountsUnderStateDir"], true);
    assert_eq!(
        config["runtimeHandlerConfigs"]["kata"]["cniConfDir"],
        "/etc/cni/kata.d"
    );
    assert_eq!(config["runtimeHandlerConfigs"]["kata"]["cniMaxConfNum"], 2);
    assert_eq!(config["runtimeHandlerConfigs"]["runc"]["cniMaxConfNum"], 0);
    assert!(config["runtimeHandlerConfigs"]["runc"]["cniConfDir"].is_null());
    assert!(config["securityAvailability"]["selinuxAvailable"].is_boolean());
    assert!(config["securityAvailability"]["apparmorAvailable"].is_boolean());
    assert!(config["securityAvailability"]["seccompAvailable"].is_boolean());
    assert!(config["securityAvailability"]["seccompNotifierSupported"].is_boolean());
    assert!(config["securityAvailability"]["seccompNotifierBaseDir"].is_string());
    assert!(config["securityAvailability"]["seccompNotifierActiveContainers"].is_array());
    assert_eq!(
        config["internalServices"]["introspection"]["runtimeBackend"]["defaultHandler"],
        "runc"
    );
    assert_eq!(
        config["internalServices"]["introspection"]["snapshotBackend"]["snapshotter"]["default"],
        "internal-overlay-untar"
    );
    assert_eq!(
        config["internalServices"]["introspection"]["snapshotBackend"]["contentStore"]
            ["storageOptions"],
        serde_json::json!([
            "overlay.mount_program=/usr/bin/fuse-overlayfs",
            "overlay.ignore_chown_errors=true"
        ])
    );
    assert_eq!(
        config["internalServices"]["introspection"]["snapshotBackend"]["contentStore"]
            ["effectiveOptions"]["mountProgram"],
        "/usr/bin/fuse-overlayfs"
    );
    assert_eq!(
        config["internalServices"]["introspection"]["snapshotBackend"]["contentStore"]
            ["effectiveOptions"]["ignoreChownErrors"],
        true
    );
    assert_eq!(
        config["internalServices"]["introspection"]["rootless"]["networkMode"],
        "pasta"
    );
    assert!(config["internalServices"]["health"]["runtime"]["ready"].is_boolean());
    assert!(config["internalServices"]["health"]["network"]["ready"].is_boolean());
    assert!(config["internalServices"]["health"]["watchers"]["watcherStatus"].is_string());
    assert!(config["internalServices"]["health"]["watchers"]["watcherBackoffCount"].is_u64());
    let health_conditions = config["internalServices"]["health"]["conditions"]
        .as_array()
        .unwrap();
    for condition_type in [
        "ImageReady",
        "SnapshotReady",
        "ShimReady",
        "RecoveryReady",
        "PullCgroupReady",
    ] {
        let condition = health_conditions
            .iter()
            .find(|condition| condition["type"] == condition_type)
            .unwrap_or_else(|| panic!("missing {condition_type} condition"));
        assert!(condition["ready"].is_boolean());
        assert!(condition["reason"].is_string());
        assert!(condition["message"].is_string());
    }
    assert!(config["internalServices"]["events"]["subscriberCount"]
        .as_u64()
        .is_some());
    assert_eq!(config["imageDecryption"]["enabled"], false);
    assert_eq!(config["imageDecryption"]["keyModel"], "");
    assert_eq!(config["nri"]["enabled"], true);
    assert_eq!(config["nri"]["enableCdi"], false);
    assert_eq!(config["nri"]["cdiSpecDirs"][0], "/etc/cdi");
    assert_eq!(config["nri"]["pluginPath"], "/opt/nri/plugins");
    assert_eq!(config["nri"]["pluginConfigPath"], "/etc/nri/conf.d");
    assert_eq!(config["nri"]["blockioConfigPath"], "/etc/nri/blockio.json");
    assert_eq!(
        config["workloads"]
            .as_object()
            .map(|workloads| workloads.len()),
        Some(0)
    );
    assert_eq!(config["attachSocketDir"], "/tmp/crius-test-attach");
    assert_eq!(config["containerExitsDir"], "/tmp/crius-test-exits");
    assert_eq!(config["containerStopTimeoutSeconds"], 30);
    assert_eq!(
        config["cleanShutdownFile"],
        "/tmp/crius-test-clean.shutdown"
    );
    assert_eq!(config["versionFile"], "/tmp/crius-test-version");
    assert_eq!(
        config["versionFilePersist"],
        "/tmp/crius-test-version-persist"
    );
    assert_eq!(config["criuPath"], "");
    assert_eq!(config["criuImagePath"], "");
    assert_eq!(config["criuWorkPath"], "");
    assert_eq!(config["enableCriuSupport"], true);
    assert_eq!(config["internalWipe"], true);
    assert_eq!(config["internalRepair"], true);
    assert_eq!(config["bindMountPrefix"], "");
    assert!(config["uidMappings"].as_array().unwrap().is_empty());
    assert!(config["gidMappings"].as_array().unwrap().is_empty());
    assert_eq!(config["minimumMappableUid"], -1);
    assert_eq!(config["minimumMappableGid"], -1);
    assert_eq!(config["ioUid"], 0);
    assert_eq!(config["ioGid"], 0);
    assert_eq!(config["disableCgroup"], false);
    assert_eq!(config["tolerateMissingHugetlbController"], true);
    assert_eq!(config["separatePullCgroup"], "");
    assert_eq!(config["pullCgroup"]["requestedValue"], "");
    assert_eq!(config["pullCgroup"]["effectiveMode"], "disabled");
    assert_eq!(config["pullCgroup"]["enabled"], false);
    assert_eq!(config["pullCgroup"]["lastError"], serde_json::Value::Null);
    assert_eq!(config["pullCgroup"]["effective"]["enabled"], false);
    assert_eq!(config["pullCgroup"]["effective"]["mode"], "disabled");
    assert!(config["pullCgroup"]["lastScope"].is_null());
    assert_eq!(config["pidsLimit"], -1);
    assert_eq!(config["infraCtrCpuset"], "1");
    assert_eq!(config["sharedCpuset"], "2-3");
    assert_eq!(config["execCpuAffinity"], "");
    assert_eq!(config["readOnly"], false);
    assert_eq!(config["noPivot"], false);
    assert_eq!(config["noNewKeyring"], true);
    assert_eq!(config["pauseImage"], "registry.k8s.io/pause:3.9");
    assert_eq!(config["pauseCommand"], "/pause");
    assert_eq!(config["dropInfraCtr"], false);
    assert_eq!(config["pinnsPath"], "/usr/bin/pinns");
    assert!(
        config["shimPidfilePattern"]
            .as_str()
            .unwrap()
            .ends_with("/shims/<containerID>/shim.pid")
    );
    assert_eq!(config["logToJournald"], false);
    assert_eq!(config["noSyncLog"], false);
    assert_eq!(config["enablePodEvents"], true);
    assert_eq!(config["includedPodMetrics"], serde_json::json!(["all"]));
    assert_eq!(config["statsCollectionPeriodSeconds"], 0);
    assert_eq!(config["podSandboxMetricsCollectionPeriodSeconds"], 0);
    assert_eq!(config["restrictOomScoreAdj"], false);
    assert_eq!(config["enableUnprivilegedPorts"], false);
    assert_eq!(config["enableUnprivilegedIcmp"], false);
    assert_eq!(config["cniMaxConfNum"], 0);
    assert!(config["cniConfTemplate"].is_null());
    assert_eq!(config["cniIpPref"], "cni");
    assert_eq!(config["disableHostportMapping"], false);
    assert_eq!(config["maxContainerLogLineSize"], 4096);
    assert_eq!(config["execSyncIoDrainTimeoutMillis"], 0);
    assert_eq!(config["streaming"]["address"], "127.0.0.1");
    assert_eq!(config["streaming"]["port"], 0);
    assert_eq!(config["streaming"]["requestTokenTtlSeconds"], 30);
    assert_eq!(
        config["streaming"]["portForwardStreamCreationTimeoutSeconds"],
        30
    );
    assert_eq!(config["streaming"]["portForwardIdleTimeoutSeconds"], 14400);
    assert!(config["lastCniLoadStatus"]["checked_at_unix_millis"].is_number());
    assert!(config["lastCniLoadStatus"]["ready"].is_boolean());
    assert!(config["lastCniLoadStatus"]["reason"].is_string());
    assert!(config["lastCniLoadStatus"]["message"].is_string());
    assert_eq!(config["recovery"]["startupReconcile"], true);
    assert_eq!(config["recovery"]["eventReplayOnRecovery"], false);
    assert!(config["recovery"]["lastStartupWasCleanShutdown"].is_null());
    assert!(config["recovery"]["lastStartupDetectedReboot"].is_null());
    assert!(config["recovery"]["lastStartupDetectedUpgrade"].is_null());
    assert!(config["recovery"]["lastStartupAttemptedRepair"].is_null());
    assert!(config["recovery"]["lastStartupRepairSucceeded"].is_null());
    assert_eq!(config["recovery"]["internalWipe"], true);
    assert_eq!(config["recovery"]["internalRepair"], true);
    assert_eq!(config["runtimeFeatures"]["updateContainerResources"], true);
    assert_eq!(config["runtimeFeatures"]["containerStats"], true);
    assert_eq!(config["runtimeFeatures"]["podSandboxStats"], true);
    assert_eq!(config["runtimeFeatures"]["podSandboxMetrics"], true);
    assert_eq!(config["runtimeFeatures"]["podLifecycleEvents"], false);
    assert_eq!(config["runtimeFeatures"]["checkpointContainer"], true);
    assert_eq!(
        response.status.unwrap().conditions.len(),
        2,
        "expected runtime and network conditions"
    );
}

#[test]
fn reloadable_config_diff_reports_changed_fields() {
    let service = test_service();
    let mut next = service.current_reloadable_config();
    next.pause_image = "registry.example/pause:v2".to_string();
    next.pinned_images = vec!["registry.example/*".to_string()];
    next.cni_conf_template = Some(PathBuf::from("/etc/crius/cni.template"));

    let changed = service.current_reloadable_config().diff_fields(&next);

    assert!(changed.contains(&"runtime.pause_image".to_string()));
    assert!(changed.contains(&"image.pinned_images".to_string()));
    assert!(changed.contains(&"network.conf_template".to_string()));
}

#[tokio::test]
async fn apply_reloadable_config_updates_image_security_and_network_state() {
    let service = test_service();
    let dir = tempdir().unwrap();
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();
    let seccomp_profile = dir.path().join("seccomp.json");
    fs::write(&seccomp_profile, "{}").unwrap();

    let mut next = service.current_reloadable_config();
    next.pause_image = "registry.example/pause:v2".to_string();
    next.pinned_images = vec!["registry.example/*".to_string()];
    next.registry_config_dir = dir.path().join("certs.d");
    next.global_auth_file = dir.path().join("auth.json");
    next.namespaced_auth_dir = dir.path().join("auth.d");
    next.signature_policy = dir.path().join("policy.json");
    next.signature_policy_dir = dir.path().join("policies");
    next.decryption_keys_path = dir.path().join("ocicrypt");
    next.decryption_decoder_path = "/usr/bin/ctd-decoder".to_string();
    next.decryption_keyprovider_config = dir.path().join("keyprovider.json");
    next.seccomp_profile = seccomp_profile.clone();
    next.apparmor_default_profile = "crius-reloaded".to_string();
    next.cni_config_dirs = vec![cni_dir.clone()];
    next.cni_max_conf_num = 1;
    next.cni_default_network_name = Some("reloaded-net".to_string());

    let changed = service
        .apply_reloadable_config(next.clone(), "test")
        .await
        .unwrap();

    assert!(changed.contains(&"runtime.pause_image".to_string()));
    assert!(changed.contains(&"security.seccomp_profile".to_string()));
    assert!(changed.contains(&"network.config_dirs".to_string()));
    assert_eq!(service.current_reloadable_config(), next);
    let image_config = service.image_service().reloadable_config_snapshot();
    assert_eq!(
        image_config.registry_config_dir.as_deref(),
        Some(dir.path().join("certs.d").as_path())
    );
    assert_eq!(
        image_config.pinned_image_patterns,
        vec!["registry.example/*".to_string()]
    );
    assert_eq!(
        service.current_reloadable_config().apparmor_default_profile,
        "crius-reloaded"
    );
    match service.effective_seccomp_profile_from_proto(None, "", false) {
        Some(crate::runtime::SeccompProfile::Localhost(path)) => {
            assert_eq!(path, seccomp_profile);
        }
        other => panic!("expected localhost seccomp profile, got {other:?}"),
    }
    let cni_config = service.current_cni_config();
    assert_eq!(cni_config.config_dirs(), &[cni_dir]);
    assert_eq!(cni_config.max_conf_num(), 1);
    assert_eq!(cni_config.default_network_name(), Some("reloaded-net"));
    assert_eq!(
        service.current_reload_state().last_reload_source.as_deref(),
        Some("test")
    );
    let events = service
        .internal_services
        .events
        .recent_internal_events("network", "runtime", 10)
        .await
        .unwrap();
    assert!(events.iter().any(|event| {
        event.kind == "network.config_reload"
            && event.details["source"] == "test"
            && event.details["fields"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("network.config_dirs"))
    }));
}

#[tokio::test]
async fn reload_config_file_once_applies_reloadable_subset() {
    let mut service = test_service();
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("crius.toml");
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();

    let mut config = crate::config::Config::default();
    config.runtime.pause_image = "registry.example/pause:v3".to_string();
    config.image.pinned_images = vec!["registry.example/static:*".to_string()];
    config.image.registry_config_dir = dir.path().join("certs").display().to_string();
    config.security.apparmor_default_profile = "crius-file-reload".to_string();
    config.network.config_dirs = vec![cni_dir.display().to_string()];
    config.network.max_conf_num = 2;
    fs::write(&config_path, toml::to_string_pretty(&config).unwrap()).unwrap();
    service.config.config_path = Some(config_path);

    let changed = service.reload_config_file_once().await.unwrap();

    assert!(changed.contains(&"runtime.pause_image".to_string()));
    assert!(changed.contains(&"image.pinned_images".to_string()));
    assert!(changed.contains(&"network.config_dirs".to_string()));
    let current = service.current_reloadable_config();
    assert_eq!(current.pause_image, "registry.example/pause:v3");
    assert_eq!(
        current.pinned_images,
        vec!["registry.example/static:*".to_string()]
    );
    assert_eq!(current.cni_config_dirs, vec![cni_dir]);
    assert_eq!(
        service.current_reload_state().last_reload_source.as_deref(),
        Some("config-file")
    );
}

#[tokio::test]
async fn config_file_watcher_applies_reloadable_subset() {
    let root_dir = tempdir().unwrap().keep();
    let config_path = root_dir.join("crius.toml");
    let cni_dir = root_dir.join("cni");
    fs::create_dir_all(&cni_dir).unwrap();

    let initial_config = crate::config::Config::default();
    fs::write(&config_path, toml::to_string_pretty(&initial_config).unwrap()).unwrap();

    let mut runtime_config = test_runtime_config(root_dir.clone());
    runtime_config.config_path = Some(config_path.clone());
    let service = RuntimeServiceImpl::new(runtime_config);

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if service.current_reload_state().watcher_status == RuntimeReloadWatcherStatus::Running
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("config file watcher did not reach running state");

    let mut updated_config = crate::config::Config::default();
    updated_config.runtime.pause_image = "registry.example/pause:v4-watcher".to_string();
    updated_config.image.pinned_images = vec!["registry.example/watched:*".to_string()];
    updated_config.network.config_dirs = vec![cni_dir.display().to_string()];
    fs::write(&config_path, toml::to_string_pretty(&updated_config).unwrap()).unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let current = service.current_reloadable_config();
            if current.pause_image == "registry.example/pause:v4-watcher"
                && current.pinned_images == vec!["registry.example/watched:*".to_string()]
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("config file watcher did not apply reloadable fields");

    let current = service.current_reloadable_config();
    assert_eq!(current.cni_config_dirs, vec![cni_dir]);
    let reload_state = service.current_reload_state();
    assert_eq!(reload_state.last_reload_source.as_deref(), Some("config-file"));
    assert!(reload_state
        .last_reload_fields
        .contains(&"runtime.pause_image".to_string()));
    assert!(reload_state
        .last_reload_fields
        .contains(&"image.pinned_images".to_string()));
    assert!(reload_state.last_reload_error.is_none());
}

#[tokio::test]
async fn reload_cni_watch_once_records_last_error() {
    let service = test_service();
    let dir = tempdir().unwrap();
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();
    fs::write(cni_dir.join("10-broken.conflist"), "{not-json").unwrap();

    let mut next = service.current_reloadable_config();
    next.cni_config_dirs = vec![cni_dir];
    service.apply_reloadable_config(next, "test").await.unwrap();

    let status = service.reload_cni_watch_once().await;

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIConfigInvalid");
    assert!(service
        .current_reload_state()
        .last_cni_watch_error
        .is_some());
    let events = service
        .internal_services
        .events
        .recent_internal_events("network", "cni", 10)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.kind == "network.reload_result"
            && event.details["ready"] == false
            && event.details["reason"] == "CNIConfigInvalid"));
}

#[tokio::test]
async fn status_verbose_correlates_network_reason_reload_and_recent_events() {
    let service = test_service();
    let dir = tempdir().unwrap();
    let cni_dir = dir.path().join("cni");
    fs::create_dir_all(&cni_dir).unwrap();
    fs::write(cni_dir.join("10-broken.conflist"), "{not-json").unwrap();

    let mut next = service.current_reloadable_config();
    next.cni_config_dirs = vec![cni_dir.clone()];
    service.apply_reloadable_config(next, "test").await.unwrap();
    let reload_status = service.reload_cni_watch_once().await;
    assert_eq!(reload_status.reason, "CNIConfigInvalid");

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let info: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();
    let diagnostics = &info["networkDiagnostics"];

    assert_eq!(diagnostics["ready"], false);
    assert_eq!(diagnostics["reason"], "CNIConfigInvalid");
    assert_eq!(diagnostics["lastCniLoadStatus"]["reason"], "CNIConfigInvalid");
    assert_eq!(
        diagnostics["reload"]["lastCniWatchError"],
        reload_status.message
    );
    assert!(diagnostics["recentEvents"]["runtime"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["kind"] == "network.config_reload"
            && event["details"]["fields"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("network.config_dirs"))));
    assert!(diagnostics["recentEvents"]["cni"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["kind"] == "network.reload_result"
            && event["details"]["reason"] == "CNIConfigInvalid"));
}

#[test]
fn reload_watcher_state_transitions_record_backoff_and_stop() {
    let mut state = RuntimeReloadState::default();

    state.mark_watcher_running();
    assert!(state.watcher_active);
    assert_eq!(state.watcher_status, RuntimeReloadWatcherStatus::Running);
    assert_eq!(state.watcher_backoff_count, 0);
    assert!(state.watcher_next_retry_unix_millis.is_none());

    let first_backoff = state.mark_watcher_error("config reload failed", 1_000);
    assert_eq!(first_backoff, std::time::Duration::from_secs(1));
    assert!(state.watcher_active);
    assert_eq!(state.watcher_status, RuntimeReloadWatcherStatus::Backoff);
    assert_eq!(state.watcher_backoff_count, 1);
    assert_eq!(state.watcher_next_retry_unix_millis, Some(2_000));
    assert_eq!(
        state.watcher_last_error.as_deref(),
        Some("config reload failed")
    );

    let second_backoff = state.mark_watcher_error("cni reload failed", 2_000);
    assert_eq!(second_backoff, std::time::Duration::from_secs(2));
    assert_eq!(state.watcher_backoff_count, 2);
    assert_eq!(state.watcher_next_retry_unix_millis, Some(4_000));

    state.mark_watcher_stopped();
    assert!(!state.watcher_active);
    assert_eq!(state.watcher_status, RuntimeReloadWatcherStatus::Stopped);
    assert!(state.watcher_next_retry_unix_millis.is_none());
}

#[tokio::test]
async fn reload_failure_marks_network_not_ready_condition() {
    let service = test_service();
    service.reload_state.lock().unwrap().last_reload_error = Some("reload failed".to_string());

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: false }))
        .await
        .unwrap()
        .into_inner();

    let network = response
        .status
        .unwrap()
        .conditions
        .into_iter()
        .find(|condition| condition.r#type == "NetworkReady")
        .unwrap();
    assert!(!network.status);
    assert_eq!(network.reason, "ConfigReloadFailed");
    assert_eq!(network.message, "reload failed");
}

#[tokio::test]
async fn rootless_missing_helper_marks_network_not_ready_condition() {
    let dir = tempdir().unwrap();
    let missing_helper = dir.path().join("missing-slirp4netns");
    let mut service = test_service();
    service.config.rootless = crate::rootless::EffectiveRootlessConfig {
        enabled: true,
        current_uid: 1000,
        current_gid: 1000,
        in_user_namespace: true,
        xdg_runtime_dir: dir.path().join("runtime"),
        xdg_data_home: dir.path().join("data"),
        storage_root: dir.path().join("data").join("crius/storage"),
        runtime_root: dir.path().join("runtime").join("crius"),
        netns_dir: dir.path().join("runtime").join("crius/netns"),
        use_fuse_overlayfs: true,
        network_mode: crate::rootless::NetworkMode::Slirp4netns,
        slirp4netns_path: missing_helper.clone(),
        pasta_path: PathBuf::from("pasta"),
        disable_cgroup: true,
        tolerate_missing_hugetlb_controller: true,
    };

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();

    let network = response
        .status
        .unwrap()
        .conditions
        .into_iter()
        .find(|condition| condition.r#type == "NetworkReady")
        .unwrap();
    assert!(!network.status);
    assert_eq!(network.reason, "RootlessNetworkHelperMissing");
    assert!(network.message.contains("slirp4netns"));
    assert!(network.message.contains(&missing_helper.display().to_string()));

    let info: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();
    assert_eq!(
        info["lastCniLoadStatus"]["reason"],
        "RootlessNetworkHelperMissing"
    );
    assert_eq!(
        info["networkReason"],
        serde_json::Value::String("RootlessNetworkHelperMissing".to_string())
    );
}

#[tokio::test]
async fn status_verbose_reports_recovery_ledger_degraded_objects() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            broken: Some(StoredBrokenState {
                kind: "rootfs_missing".to_string(),
                details: "rootfs artifact is missing".to_string(),
                detected_at: RuntimeServiceImpl::now_nanos(),
            }),
            ..Default::default()
        },
    )
    .unwrap();

    {
        let mut persistence = service.persistence.lock().await;
        persistence
            .save_container(
                "recovery-broken",
                "pod-1",
                crate::runtime::ContainerStatus::Running,
                "busybox:latest",
                &Vec::new(),
                &HashMap::new(),
                &annotations,
            )
            .unwrap();
        persistence
            .save_snapshot_record(&crate::storage::SnapshotRecord {
                key: "snapshot-broken".to_string(),
                image_id: "busybox:latest".to_string(),
                owner_kind: "container".to_string(),
                owner_id: "recovery-broken".to_string(),
                state: crate::state::SnapshotLedgerState::Broken
                    .as_str()
                    .to_string(),
                mountpoint: "/tmp/recovery-broken/rootfs".to_string(),
                snapshotter: "internal-overlay-untar".to_string(),
                runtime_managed: true,
            })
            .unwrap();
        persistence
            .replace_runtime_artifacts(
                "container",
                "recovery-broken",
                &[crate::storage::RuntimeArtifactRecord {
                    owner_kind: "container".to_string(),
                    owner_id: "recovery-broken".to_string(),
                    artifact_kind: "rootfs".to_string(),
                    path: "/tmp/recovery-broken/rootfs".to_string(),
                    state: crate::state::RuntimeArtifactLedgerState::Broken
                        .as_str()
                        .to_string(),
                    runtime_handler: Some("runc".to_string()),
                    runtime_root: Some("/tmp/runtime-root".to_string()),
                }],
            )
            .unwrap();
        persistence
            .save_shim_process_record(&crate::storage::ShimProcessRecord {
                container_id: "recovery-broken".to_string(),
                shim_pid: u32::MAX,
                work_dir: "/tmp/shims".to_string(),
                socket_path: "/tmp/shims/recovery-broken/task.sock".to_string(),
                exit_code_file: "/tmp/exits/recovery-broken".to_string(),
                log_file: "/tmp/shims/recovery-broken/shim.log".to_string(),
                bundle_path: "/tmp/runtime-root/recovery-broken".to_string(),
                state: crate::state::ShimLedgerState::Dead.as_str().to_string(),
                last_seen_at: RuntimeServiceImpl::now_nanos(),
            })
            .unwrap();
    }

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();

    assert_eq!(
        config["recovery"]["ledgerSummary"]["brokenContainers"],
        serde_json::json!(1)
    );
    assert_eq!(
        config["recovery"]["ledgerSummary"]["brokenSnapshots"],
        serde_json::json!(1)
    );
    assert_eq!(
        config["recovery"]["ledgerSummary"]["brokenRuntimeArtifacts"],
        serde_json::json!(1)
    );
    assert_eq!(
        config["recovery"]["ledgerSummary"]["deadShims"],
        serde_json::json!(1)
    );
    assert_eq!(
        config["internalServices"]["introspection"]["recovery"]["ledgerSummary"]["deadShims"],
        serde_json::json!(1)
    );
    assert!(
        config["internalServices"]["introspection"]["recovery"]["ledgerCheck"]["issueCount"]
            .as_u64()
            .unwrap()
            >= 4
    );
    assert!(config["internalServices"]["introspection"]["recovery"]["ledgerCheck"]["actionCount"]
        .is_u64());
    assert_eq!(
        config["internalServices"]["introspection"]["recovery"]["unhealthyObjectCount"],
        serde_json::json!(4)
    );
    let recovery = config["internalServices"]["health"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "RecoveryReady")
        .unwrap();
    assert_eq!(recovery["ready"], false);
    assert_eq!(recovery["reason"], "RecoveryObjectsDegraded");
    assert!(recovery["message"]
        .as_str()
        .unwrap()
        .contains("4 unhealthy object"));
}

#[tokio::test]
async fn verbose_status_reports_content_gc_dry_run_candidates() {
    let service = test_service();
    {
        let mut persistence = service.persistence.lock().await;
        let storage = persistence.storage_mut();
        storage
            .save_content_blob(&crate::storage::ContentBlobRecord {
                digest: "sha256:status-gc".to_string(),
                media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                size: 32,
                relative_path: "blobs/sha256/status-gc".to_string(),
                created_at: 1,
                last_used_at: 1,
            })
            .unwrap();
    }

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();

    assert_eq!(config["contentGc"]["dryRunSupported"], true);
    assert_eq!(config["contentGc"]["candidateCount"], 1);
    assert_eq!(config["contentGc"]["reclaimableCount"], 1);
    assert_eq!(
        config["internalServices"]["introspection"]["contentGc"]["candidateCount"],
        serde_json::json!(1)
    );
}

#[test]
fn runtime_registry_returns_handler_specific_create_timeout() {
    let runtime = RuntimeRegistry::new(
        "runc".to_string(),
        HashMap::from([(
            "runc".to_string(),
            Arc::new(crate::runtime::RuncBackend::new(RuncRuntime::new(
                PathBuf::from("/definitely/missing/runc"),
                PathBuf::from("/tmp/crius-test-runtime-root"),
            ))) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("runc".to_string(), 240), ("kata".to_string(), 600)]),
    );

    assert_eq!(runtime.container_create_timeout_for_handler(""), 240);
    assert_eq!(runtime.container_create_timeout_for_handler("kata"), 600);
}

#[derive(Debug)]
struct FakeBackend {
    name: &'static str,
    runtime_root: PathBuf,
}

struct TestShimExecSyncHandler;

impl crate::shim_rpc::server::ShimRpcHandler for TestShimExecSyncHandler {
    fn handle_request(
        &self,
        request: crate::shim_rpc::ShimRpcRequest,
    ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
        match request {
            crate::shim_rpc::ShimRpcRequest::Ping => Ok(crate::shim_rpc::ShimRpcResponse::Empty),
            crate::shim_rpc::ShimRpcRequest::Status(_) => {
                Ok(crate::shim_rpc::ShimRpcResponse::Status(
                    crate::shim_rpc::StatusResponse {
                        state: crate::shim_rpc::TaskState::Running,
                        pid: Some(std::process::id() as i32),
                        exit_code: None,
                    },
                ))
            }
            crate::shim_rpc::ShimRpcRequest::ExecProcess(request) => {
                Ok(crate::shim_rpc::ShimRpcResponse::ExecProcess(
                    crate::shim_rpc::ExecProcessResponse {
                        exit_code: 0,
                        stdout: format!("shim:{}", request.command.join(" ")).into_bytes(),
                        stderr: b"shim-stderr".to_vec(),
                    },
                ))
            }
            other => Err(anyhow::anyhow!(
                "unexpected shim exec sync test request: {:?}",
                other
            )),
        }
    }
}

struct TestShimExecSessionHandler {
    marker_path: PathBuf,
    session_dir: PathBuf,
}

impl crate::shim_rpc::server::ShimRpcHandler for TestShimExecSessionHandler {
    fn handle_request(
        &self,
        request: crate::shim_rpc::ShimRpcRequest,
    ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
        match request {
            crate::shim_rpc::ShimRpcRequest::Ping => Ok(crate::shim_rpc::ShimRpcResponse::Empty),
            crate::shim_rpc::ShimRpcRequest::Status(_) => {
                Ok(crate::shim_rpc::ShimRpcResponse::Status(
                    crate::shim_rpc::StatusResponse {
                        state: crate::shim_rpc::TaskState::Running,
                        pid: Some(std::process::id() as i32),
                        exit_code: None,
                    },
                ))
            }
            crate::shim_rpc::ShimRpcRequest::OpenExecSession(request) => {
                fs::write(&self.marker_path, request.command.join(" ")).unwrap();
                fs::create_dir_all(&self.session_dir).unwrap();
                let io_socket_path = self.session_dir.join("io.sock");
                let resize_socket_path = self.session_dir.join("resize.sock");
                let _ = std::os::unix::net::UnixListener::bind(&io_socket_path);
                let _ = std::os::unix::net::UnixListener::bind(&resize_socket_path);
                Ok(crate::shim_rpc::ShimRpcResponse::OpenExecSession(
                    crate::shim_rpc::OpenExecSessionResponse {
                        session_id: "session-1".to_string(),
                        io_socket_path,
                        resize_socket_path: Some(resize_socket_path),
                    },
                ))
            }
            other => Err(anyhow::anyhow!(
                "unexpected shim exec session test request: {:?}",
                other
            )),
        }
    }
}

impl crate::runtime::TaskController for FakeBackend {
    fn create_container(
        &self,
        _container_id: &str,
        _config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<String> {
        Ok("fake-created".to_string())
    }

    fn start_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop_container(&self, _container_id: &str, _timeout: Option<u32>) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn container_status(
        &self,
        _container_id: &str,
    ) -> anyhow::Result<crate::runtime::ContainerStatus> {
        Ok(crate::runtime::ContainerStatus::Created)
    }

    fn reopen_container_log(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn exec_in_container(
        &self,
        _container_id: &str,
        _command: &[String],
        _tty: bool,
    ) -> anyhow::Result<i32> {
        Ok(0)
    }

    fn update_container_resources(
        &self,
        _container_id: &str,
        _resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_container_paused(&self, _container_id: &str) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn restore_attach_shim(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn open_attach_stream(
        &self,
        container_id: &str,
        _stdin: bool,
        _stdout: bool,
        _stderr: bool,
        tty: bool,
    ) -> anyhow::Result<crate::shim_rpc::OpenAttachStreamResponse> {
        Ok(crate::shim_rpc::OpenAttachStreamResponse {
            stream_id: "fake-attach".to_string(),
            io_socket_path: self.runtime_root.join(container_id).join("attach.sock"),
            resize_socket_path: tty.then(|| self.runtime_root.join(container_id).join("resize.sock")),
        })
    }

    fn close_attach_stream(&self, _container_id: &str, _stream_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn resize_attach_pty(
        &self,
        _container_id: &str,
        _stream_id: Option<&str>,
        _width: u16,
        _height: u16,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn shim_status(
        &self,
        _container_id: &str,
    ) -> anyhow::Result<Option<crate::shim_rpc::StatusResponse>> {
        Ok(None)
    }

    fn restore_container_from_checkpoint(
        &self,
        _container_id: &str,
        _checkpoint_path: &Path,
        _work_path: &Path,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pause_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn checkpoint_container(
        &self,
        _container_id: &str,
        _location: &Path,
        _work_path: &Path,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn resume_container(&self, _container_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn container_pid(&self, _container_id: &str) -> anyhow::Result<Option<i32>> {
        Ok(None)
    }
}

impl crate::runtime::RuntimeContextManager for FakeBackend {
    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.runtime_root.join(container_id)
    }

    fn enforce_oom_score_adj_policy(
        &self,
        _spec: &mut crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn prepare_rootfs(
        &self,
        _container_id: &str,
        _config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<crate::runtime::PreparedRootfsMount> {
        Ok(crate::runtime::PreparedRootfsMount {
            key: "fake".to_string(),
            mountpoint: PathBuf::from("/tmp/fake-rootfs"),
            readonly: false,
            handle: crate::image::snapshotter::RootfsHandle::internal_path(
                "fake",
                "container",
                "fake",
                "/tmp/fake-rootfs",
                false,
            ),
        })
    }

    fn build_spec(
        &self,
        _container_id: &str,
        _config: &crate::runtime::ContainerConfig,
    ) -> anyhow::Result<crate::oci::spec::Spec> {
        Ok(crate::oci::spec::Spec::new("1.0.2"))
    }

    fn write_bundle(
        &self,
        _container_id: &str,
        _rootfs: &Path,
        _spec: &crate::oci::spec::Spec,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn create_task_from_prepared_bundle(
        &self,
        _container_id: &str,
        _rootfs: crate::runtime::PreparedRootfsMount,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn load_spec(&self, _container_id: &str) -> anyhow::Result<crate::oci::spec::Spec> {
        Ok(crate::oci::spec::Spec::new("1.0.2"))
    }

    fn validate_mount_requests(
        &self,
        _config: &crate::runtime::ContainerConfig,
    ) -> std::result::Result<(), crate::runtime::MountSemanticsError> {
        Ok(())
    }
}

impl crate::runtime::RuntimeBackend for FakeBackend {
    fn backend_name(&self) -> &str {
        self.name
    }

    fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    fn runtime_path(&self) -> &Path {
        Path::new("/fake/runtime")
    }

    fn runtime_config_path(&self) -> &Path {
        Path::new("/fake/runtime.conf")
    }

    fn task_controller(&self) -> &dyn crate::runtime::TaskController {
        self
    }

    fn runtime_context(&self) -> &dyn crate::runtime::RuntimeContextManager {
        self
    }

    fn probe_runtime_features(&self) -> crate::runtime::RuntimeFeatureProbe {
        crate::runtime::RuntimeFeatureProbe::default()
    }

    fn cgroup_driver(&self) -> crate::config::CgroupDriverConfig {
        crate::config::CgroupDriverConfig::Cgroupfs
    }
}

#[test]
fn runtime_registry_can_store_trait_object_backends() {
    let runtime = RuntimeRegistry::new(
        "fake".to_string(),
        HashMap::from([(
            "fake".to_string(),
            Arc::new(FakeBackend {
                name: "fake",
                runtime_root: PathBuf::from("/tmp/fake-runtime-root"),
            }) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
        HashMap::from([("fake".to_string(), 33)]),
    );

    let backend = runtime.runtime_for_handler("fake").unwrap();
    assert_eq!(backend.backend_name(), "fake");
    assert_eq!(backend.runtime_root(), Path::new("/tmp/fake-runtime-root"));
    assert_eq!(runtime.container_create_timeout_for_handler("fake"), 33);
}

#[test]
fn runtime_service_can_use_injected_fake_backend_for_handler() {
    let dir = tempdir().unwrap();
    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime = "fake".to_string();
    config.runtime_handlers = vec!["fake".to_string()];
    config.runtime_configs = HashMap::from([(
        "fake".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "fake".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/fake/runtime".to_string(),
            runtime_config_path: String::new(),
            runtime_root: dir.path().join("fake-runtime-root").display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/fake/shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 77,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    )]);
    let service = RuntimeServiceImpl::new_with_runtime_backends(
        config,
        HashMap::from([(
            "fake".to_string(),
            Arc::new(FakeBackend {
                name: "fake",
                runtime_root: dir.path().join("fake-runtime-root"),
            }) as Arc<dyn crate::runtime::RuntimeBackend>,
        )]),
    );

    let backend = service.runtime.runtime_for_handler("fake").unwrap();
    assert_eq!(backend.backend_name(), "fake");
    assert_eq!(
        backend
            .task_controller()
            .container_status("container-1")
            .unwrap(),
        crate::runtime::ContainerStatus::Created
    );
    assert_eq!(service.runtime.container_create_timeout_for_handler("fake"), 77);
}

#[tokio::test]
async fn run_container_create_phase_until_returns_deadline_exceeded() {
    let service = test_service();
    let err = service
        .run_container_create_phase_until(
            ContainerCreateDeadline {
                timeout_secs: 1,
                deadline: std::time::Instant::now() + Duration::from_millis(1),
            },
            "prepare_rootfs",
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<_, Status>(())
            },
        )
        .await
        .expect_err("phase exceeding deadline must fail");

    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    assert!(err.message().contains("prepare_rootfs"));
}

#[tokio::test]
async fn status_verbose_reports_cgroup_disable_runtime_feature_state() {
    let mut service = test_service();
    service.config.disable_cgroup = true;

    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();

    assert_eq!(config["disableCgroup"], true);
    assert_eq!(config["runtimeFeatures"]["updateContainerResources"], false);
}

#[tokio::test]
async fn version_reports_cri_runtime_identity_when_oci_runtime_version_is_available() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-version.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "runc version 1.2.3"
  exit 0
fi
exit 0
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
                    backend_options: HashMap::new(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/definitely/missing/crius-shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 240,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    );
    let service = RuntimeServiceImpl::new(config);
    let response = RuntimeService::version(
        &service,
        Request::new(VersionRequest {
            version: "0.1.0".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.runtime_name, "crius");
    assert_eq!(response.runtime_version, env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn status_verbose_reports_oci_runtime_version_separately() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-version.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "runc version 1.2.3"
  exit 0
fi
exit 0
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
                    backend_options: HashMap::new(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/definitely/missing/crius-shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 240,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    );
    let service = RuntimeServiceImpl::new(config);
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let payload: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();

    assert_eq!(payload["runtimeName"], "crius");
    assert_eq!(payload["runtimeVersion"], env!("CARGO_PKG_VERSION"));
    assert_eq!(payload["ociRuntimeVersion"], "runc version 1.2.3");
}

#[test]
fn parse_cgroup_hint_from_procfs_prefers_relevant_controller_or_unified_line() {
    let v1 = "11:hugetlb:/\n10:memory:/kubepods.slice/pod123/container.scope\n9:cpuset:/\n";
    assert_eq!(
        RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v1),
        Some(PathBuf::from("/kubepods.slice/pod123/container.scope"))
    );

    let v2 = "0::/user.slice/user-0.slice/session-1.scope\n";
    assert_eq!(
        RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v2),
        Some(PathBuf::from("/user.slice/user-0.slice/session-1.scope"))
    );
}

#[tokio::test]
async fn status_reports_runtime_not_ready_when_binary_is_not_executable() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime");
    fs::write(&runtime_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let mut config = test_runtime_config(dir.path().join("root"));
    config.runtime_path = runtime_path.clone();
    config.runtime_configs.insert(
        "runc".to_string(),
        crate::config::ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
                    backend_options: HashMap::new(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: config.runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/definitely/missing/crius-shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 240,
            snapshotter: "internal-overlay-untar".to_string(),
        },
    );
    let service = RuntimeServiceImpl::new(config);
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: false }))
        .await
        .unwrap()
        .into_inner();
    let runtime_condition = response
        .status
        .unwrap()
        .conditions
        .into_iter()
        .find(|condition| condition.r#type == "RuntimeReady")
        .unwrap();
    assert!(!runtime_condition.status);
    assert_eq!(runtime_condition.reason, "RuntimeBinaryNotExecutable");
}

#[tokio::test]
async fn runtime_config_reports_detected_cgroup_driver() {
    let service = test_service();
    let response = RuntimeService::runtime_config(&service, Request::new(RuntimeConfigRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.linux.unwrap().cgroup_driver,
        service.cgroup_driver() as i32
    );
}

#[tokio::test]
async fn runtime_config_prefers_configured_cgroup_driver() {
    let mut runtime_config = test_runtime_config(tempdir().unwrap().keep());
    runtime_config.cgroup_driver = Some(CgroupDriver::Cgroupfs);
    let service = RuntimeServiceImpl::new(runtime_config);

    let response = RuntimeService::runtime_config(&service, Request::new(RuntimeConfigRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.linux.unwrap().cgroup_driver,
        CgroupDriver::Cgroupfs as i32
    );
}

#[tokio::test]
async fn update_runtime_config_persists_network_config_and_exposes_it_via_status() {
    let root_dir = tempdir().unwrap().keep();
    let service = RuntimeServiceImpl::new(test_runtime_config(root_dir.clone()));

    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.244.0.0/16".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let status = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(status.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeNetworkConfig"]["podCIDR"], "10.244.0.0/16");

    let reloaded = RuntimeServiceImpl::new(test_runtime_config(root_dir));
    let reloaded_status =
        RuntimeService::status(&reloaded, Request::new(StatusRequest { verbose: true }))
            .await
            .unwrap()
            .into_inner();
    let reloaded_config: serde_json::Value =
        serde_json::from_str(reloaded_status.info.get("config").unwrap()).unwrap();
    assert_eq!(
        reloaded_config["runtimeNetworkConfig"]["podCIDR"],
        "10.244.0.0/16"
    );
}

#[tokio::test]
async fn update_runtime_config_renders_cni_config_template_from_pod_cidrs() {
    let root_dir = tempdir().unwrap().keep();
    let config_dir = root_dir.join("net.d");
    let cache_dir = root_dir.join("cache");
    let template_path = root_dir.join("template.conflist");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        &template_path,
        r#"
{
  "name": "test-pod-network",
  "cniVersion": "1.0.0",
  "plugins": [
    {
      "type": "ptp",
      "ipam": {
        "type": "host-local",
        "subnet": "{{.PodCIDR}}",
        "ranges": [{{range $i, $range := .PodCIDRRanges}}{{if $i}}, {{end}}[{"subnet": "{{$range}}"}]{{end}}],
        "routes": [{{range $i, $route := .Routes}}{{if $i}}, {{end}}{"dst": "{{$route}}"}{{end}}]
      }
    }
  ]
}
"#,
    )
    .unwrap();

    let mut config = test_runtime_config(root_dir.clone());
    let mut cni_config = crate::network::CniConfig::new(
        vec![config_dir.clone()],
        Vec::new(),
        cache_dir,
        0,
        crate::network::MainIpPreference::Cni,
        None,
        false,
    );
    cni_config.set_conf_template(Some(template_path));
    config.cni_config = cni_config;
    let service = RuntimeServiceImpl::new(config);

    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.0.0.0/24, 2001:4860:4860::/64".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let rendered = fs::read_to_string(config_dir.join("10-crius-net.conflist")).unwrap();
    assert_eq!(
        rendered.trim(),
        r#"{
  "name": "test-pod-network",
  "cniVersion": "1.0.0",
  "plugins": [
    {
      "type": "ptp",
      "ipam": {
        "type": "host-local",
        "subnet": "10.0.0.0/24",
        "ranges": [[{"subnet": "10.0.0.0/24"}], [{"subnet": "2001:4860:4860::/64"}]],
        "routes": [{"dst": "0.0.0.0/0"}, {"dst": "::/0"}]
      }
    }
  ]
}"#
    );
}

#[tokio::test]
async fn update_runtime_config_rejects_invalid_pod_cidr_for_cni_template() {
    let root_dir = tempdir().unwrap().keep();
    let config_dir = root_dir.join("net.d");
    let template_path = root_dir.join("template.conflist");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(&template_path, r#"{"subnet":"{{.PodCIDR}}"}"#).unwrap();

    let mut config = test_runtime_config(root_dir.clone());
    let mut cni_config = crate::network::CniConfig::new(
        vec![config_dir.clone()],
        Vec::new(),
        root_dir.join("cache"),
        0,
        crate::network::MainIpPreference::Cni,
        None,
        false,
    );
    cni_config.set_conf_template(Some(template_path));
    config.cni_config = cni_config;
    let service = RuntimeServiceImpl::new(config);

    let err = RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "not-a-cidr".to_string(),
                }),
            }),
        }),
    )
    .await
    .expect_err("invalid CIDR should reject template rendering");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err
        .message()
        .contains("Failed to render CNI config template"));
    assert!(!config_dir.join("10-crius-net.conflist").exists());
}

#[tokio::test]
async fn run_pod_sandbox_persists_runtime_pod_cidr_in_verbose_info() {
    let (dir, service) = test_service_with_fake_runtime();
    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.88.0.0/16".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let netns_path = dir.path().join("pod-runtime-cidr.netns");
    fs::write(&netns_path, "netns").unwrap();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            runtime_pod_cidr: Some("10.88.0.0/16".to_string()),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-runtime-cidr".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-runtime-cidr".to_string(),
        test_pod("pod-runtime-cidr", annotations),
    );

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-runtime-cidr".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["runtimePodCIDR"], "10.88.0.0/16");
}

#[tokio::test]
async fn run_pod_sandbox_persists_pod_runtime_artifacts_to_ledger() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri);
    service.image_service().set_test_pull_handler(Arc::new(|_| {
        Ok(crate::image::TestPullResponse {
            image_id: "sha256:pause-ledger-artifacts".to_string(),
            size: 1,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
        })
    }));
    let response = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "pod-ledger-artifacts",
            "default",
            "uid-ledger-artifacts",
            HashMap::new(),
        )),
    )
    .await
    .unwrap()
    .into_inner();
    let pod_id = response.pod_sandbox_id;

    for _ in 0..50 {
        let artifacts = service
            .persistence
            .lock()
            .await
            .list_runtime_artifacts()
            .unwrap();
        if artifacts.iter().any(|artifact| {
            artifact.owner_kind == "pod"
                && artifact.owner_id == pod_id
                && artifact.artifact_kind == "workspace"
        }) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("expected pod runtime artifacts to be persisted");
}

#[tokio::test]
async fn create_container_persists_container_ledger_metadata() {
    let fake_nri = Arc::new(FakeNri::default());
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri);
    service.image_service().set_test_pull_handler(Arc::new(|_| {
        Ok(crate::image::TestPullResponse {
            image_id: "sha256:pause-container-ledger".to_string(),
            size: 1,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
        })
    }));
    let pod_response = RuntimeService::run_pod_sandbox(
        &service,
        Request::new(host_network_run_pod_sandbox_request(
            "container-ledger",
            "default",
            "uid-container-ledger",
            HashMap::new(),
        )),
    )
    .await
    .unwrap()
    .into_inner();

    ImageService::pull_image(
        &service.image_service(),
        Request::new(crate::proto::runtime::v1::PullImageRequest {
            image: Some(ImageSpec {
                image: "docker.io/library/busybox:latest".to_string(),
                user_specified_image: "docker.io/library/busybox:latest".to_string(),
                ..Default::default()
            }),
            auth: None,
            sandbox_config: None,
        }),
    )
    .await
    .unwrap();

    let container_id = RuntimeService::create_container(
        &service,
        Request::new(CreateContainerRequest {
            pod_sandbox_id: pod_response.pod_sandbox_id,
            config: Some(crate::proto::runtime::v1::ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "ctr-ledger".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "docker.io/library/busybox:latest".to_string(),
                    user_specified_image: "docker.io/library/busybox:latest".to_string(),
                    ..Default::default()
                }),
                command: vec!["sleep".to_string(), "10".to_string()],
                linux: Some(crate::proto::runtime::v1::LinuxContainerConfig::default()),
                ..Default::default()
            }),
            sandbox_config: Some(crate::proto::runtime::v1::PodSandboxConfig {
                metadata: Some(PodSandboxMetadata {
                    name: "container-ledger".to_string(),
                    uid: "uid-container-ledger".to_string(),
                    namespace: "default".to_string(),
                    attempt: 1,
                }),
                hostname: "container-ledger".to_string(),
                labels: HashMap::new(),
                annotations: HashMap::new(),
                log_directory: String::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                linux: Some(crate::proto::runtime::v1::LinuxPodSandboxConfig {
                    security_context: Some(
                        crate::proto::runtime::v1::LinuxSandboxSecurityContext {
                            namespace_options: Some(NamespaceOption {
                                network: NamespaceMode::Pod as i32,
                                pid: NamespaceMode::Pod as i32,
                                ipc: NamespaceMode::Pod as i32,
                                target_id: String::new(),
                                userns_options: None,
                            }),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .container_id;

    for _ in 0..50 {
        if let Some(record) = service
            .persistence
            .lock()
            .await
            .storage()
            .get_container(&container_id)
            .unwrap()
        {
            if record.runtime_handler.as_deref() == Some("runc")
                && record.runtime_backend.as_deref() == Some("runc")
                && record.snapshot_key.as_deref() == Some(container_id.as_str())
            {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("expected container ledger metadata to be persisted");
}

#[tokio::test]
async fn host_network_pod_verbose_info_omits_managed_netns_and_runtime_pod_cidr() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
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
        test_pod("pod-host-network", annotations),
    );

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-host-network".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert!(info["netnsPath"].is_null());
    assert!(info["runtimePodCIDR"].is_null());
}

#[tokio::test]
async fn network_health_requires_declared_plugin_binary() {
    let _guard = env_lock().lock().await;
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("cni-conf");
    let plugin_dir = dir.path().join("cni-bin");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::write(
        config_dir.join("10-test.conflist"),
        r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
    )
    .unwrap();

    std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
    std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
    let service = test_service_with_env_cni();
    let status = service.probe_cni_load_status().await;
    std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
    std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIPluginMissing");
    assert!(status.message.contains("bridge"));
    assert_eq!(status.loaded_networks, vec!["test"]);
    assert_eq!(status.declared_plugins, vec!["bridge"]);
    assert_eq!(status.missing_plugin_binaries, vec!["bridge"]);
}

#[tokio::test]
async fn network_health_requires_plugin_to_be_executable() {
    let _guard = env_lock().lock().await;
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("cni-conf");
    let plugin_dir = dir.path().join("cni-bin");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::write(
        config_dir.join("10-test.conflist"),
        r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
    )
    .unwrap();
    let plugin_path = plugin_dir.join("bridge");
    fs::write(&plugin_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&plugin_path, perms).unwrap();

    std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
    std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
    let service = test_service_with_env_cni();
    let status = service.probe_cni_load_status().await;
    std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
    std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIPluginMissing");
    assert!(status.message.contains("bridge"));
    assert!(status
        .missing_plugin_binaries
        .contains(&"bridge".to_string()));
}
