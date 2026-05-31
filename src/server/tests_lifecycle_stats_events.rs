#[tokio::test]
async fn inspect_and_info_endpoints_return_consistent_snapshots() {
    let (dir, service) = test_service_with_fake_runtime();
    let netns_path = dir.path().join("inspect.netns");
    fs::write(&netns_path, "netns").unwrap();
    let log_path = dir.path().join("logs").join("inspect.log");

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-inspect".to_string()),
            ip: Some("10.88.0.10".to_string()),
            additional_ips: vec!["fd00::10".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-inspect".to_string(),
        test_pod("pod-inspect", pod_annotations),
    );

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            run_as_user: Some("1000".to_string()),
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                image: String::new(),
                image_sub_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-inspect".to_string(),
        test_container("container-inspect", "pod-inspect", container_annotations),
    );
    set_fake_runtime_state(&dir, "container-inspect", "running");

    let status = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let container = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-inspect".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let pod = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-inspect".to_string(),
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

    let config: serde_json::Value =
        serde_json::from_str(status.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeFeatures"]["containerEvents"], true);
    assert_eq!(container.status.as_ref().unwrap().reason, "Running");
    assert_eq!(container.status.as_ref().unwrap().mounts.len(), 1);
    assert!(container.info.contains_key("info"));
    assert_eq!(
        pod.status.as_ref().unwrap().network.as_ref().unwrap().ip,
        "10.88.0.10"
    );
    assert_eq!(pod.containers_statuses.len(), 1);
    assert_eq!(containers.containers.len(), 1);
    assert_eq!(pods.items.len(), 1);
}

#[tokio::test]
async fn list_container_stats_applies_id_pod_and_label_filters() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut container_a = test_container("container-alpha-123", "pod-alpha", HashMap::new());
    container_a
        .labels
        .insert("app".to_string(), "api".to_string());
    let mut container_b = test_container("container-bravo-456", "pod-bravo", HashMap::new());
    container_b
        .labels
        .insert("app".to_string(), "worker".to_string());

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(
            "pod-alpha".to_string(),
            test_pod("pod-alpha", HashMap::new()),
        );
        pods.insert(
            "pod-bravo".to_string(),
            test_pod("pod-bravo", HashMap::new()),
        );
    }
    {
        let mut containers = service.containers.lock().await;
        containers.insert(container_a.id.clone(), container_a);
        containers.insert(container_b.id.clone(), container_b);
    }
    set_fake_runtime_state(&dir, "container-alpha-123", "running");
    set_fake_runtime_state(&dir, "container-bravo-456", "running");

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "api".to_string());
    let response = RuntimeService::list_container_stats(
        &service,
        Request::new(ListContainerStatsRequest {
            filter: Some(crate::proto::runtime::v1::ContainerStatsFilter {
                id: "container-alpha".to_string(),
                pod_sandbox_id: "pod-alpha".to_string(),
                label_selector: selector,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.stats.len(), 1);
    assert_eq!(
        response.stats[0]
            .attributes
            .as_ref()
            .expect("container stats should include attributes")
            .id,
        "container-alpha-123"
    );
}

#[tokio::test]
async fn list_pod_sandbox_prefers_ready_newer_sandboxes_first() {
    let service = test_service();

    let old_not_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-old".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-1".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 1,
        }),
        state: PodSandboxState::SandboxNotready as i32,
        created_at: 1,
        ..Default::default()
    };
    let new_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-new".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-1".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 2,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: 2,
        ..Default::default()
    };
    let newer_not_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-newer-not-ready".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "kube-controller-manager-vm-crius".to_string(),
            uid: "uid-2".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 3,
        }),
        state: PodSandboxState::SandboxNotready as i32,
        created_at: 3,
        ..Default::default()
    };

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(old_not_ready.id.clone(), old_not_ready);
        pods.insert(new_ready.id.clone(), new_ready);
        pods.insert(newer_not_ready.id.clone(), newer_not_ready);
    }

    let response =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    let ids: Vec<_> = response.items.into_iter().map(|pod| pod.id).collect();
    assert_eq!(ids[0], "pod-new");
    assert_eq!(ids[1], "pod-newer-not-ready");
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn pod_sandbox_status_demotes_stale_ready_duplicate_sandbox() {
    let service = test_service();

    let old_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-old-ready".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-duplicate".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 1,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: 1,
        ..Default::default()
    };
    let new_ready = crate::proto::runtime::v1::PodSandbox {
        id: "pod-new-ready".to_string(),
        metadata: Some(PodSandboxMetadata {
            name: "etcd-vm-crius".to_string(),
            uid: "uid-duplicate".to_string(),
            namespace: "kube-system".to_string(),
            attempt: 2,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: 2,
        ..Default::default()
    };

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(old_ready.id.clone(), old_ready);
        pods.insert(new_ready.id.clone(), new_ready);
    }

    let old_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-old-ready".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let new_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-new-ready".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(old_status.state, PodSandboxState::SandboxNotready as i32);
    assert_eq!(new_status.state, PodSandboxState::SandboxReady as i32);
    assert_eq!(list.items[0].id, "pod-new-ready");
    assert_eq!(list.items[0].state, PodSandboxState::SandboxReady as i32);
    assert_eq!(list.items.len(), 1);
}

#[test]
fn pause_process_root_match_scopes_to_runtime_pod_root() {
    let pod_root = PathBuf::from("/var/lib/crius/pods");
    assert!(
        RuntimeServiceImpl::pause_process_root_matches_runtime_pod_root(
            Path::new("/var/lib/crius/pods/pod-a/pause-rootfs"),
            &pod_root
        )
    );
    assert!(
        !RuntimeServiceImpl::pause_process_root_matches_runtime_pod_root(Path::new("/"), &pod_root)
    );
    assert!(
        !RuntimeServiceImpl::pause_process_root_matches_runtime_pod_root(
            Path::new("/var/lib/other-runtime/pods/pod-a/pause-rootfs"),
            &pod_root
        )
    );
}

#[test]
fn convert_to_proto_container_stats_preserves_optional_fields() {
    let service = test_service();
    let stats = crate::metrics::ContainerStats {
        container_id: "container-stats-proto".to_string(),
        cpu: Some(crate::metrics::CpuStats {
            usage_total: 11,
            usage_user: 7,
            usage_kernel: 5,
            ..Default::default()
        }),
        memory: Some(crate::metrics::MemoryStats {
            usage: 101,
            limit: 1000,
            rss: 55,
            pgfault: 9,
            pgmajfault: 3,
            swap: 77,
            ..Default::default()
        }),
        timestamp: 1234,
        ..Default::default()
    };

    let proto = service.convert_to_proto_container_stats(stats);
    assert_eq!(
        proto
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.usage_core_nano_seconds.as_ref())
            .map(|value| value.value),
        Some(11)
    );
    assert_eq!(
        proto
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.usage_nano_cores.as_ref())
            .map(|value| value.value),
        Some(12)
    );
    assert_eq!(
        proto
            .memory
            .as_ref()
            .and_then(|memory| memory.working_set_bytes.as_ref())
            .map(|value| value.value),
        Some(101)
    );
    assert_eq!(
        proto
            .memory
            .as_ref()
            .and_then(|memory| memory.available_bytes.as_ref())
            .map(|value| value.value),
        Some(899)
    );
    assert_eq!(
        proto
            .swap
            .as_ref()
            .and_then(|swap| swap.swap_usage_bytes.as_ref())
            .map(|value| value.value),
        Some(77)
    );
}

#[test]
fn stats_cache_is_fresh_only_within_positive_period() {
    let now = std::time::Instant::now();
    let just_before = now - std::time::Duration::from_secs(4);
    let just_after = now - std::time::Duration::from_secs(6);

    assert!(RuntimeServiceImpl::stats_cache_is_fresh(
        just_before,
        5,
        now
    ));
    assert!(!RuntimeServiceImpl::stats_cache_is_fresh(
        just_after, 5, now
    ));
    assert!(!RuntimeServiceImpl::stats_cache_is_fresh(
        just_before,
        0,
        now
    ));
}

#[test]
fn build_pod_metrics_returns_all_metrics_when_configured_with_all() {
    let service = test_service();
    let metrics = service.build_pod_metrics(
        "pod-all",
        1,
        crate::server::stats::PodMetricTotals {
            cpu_usage: 10,
            memory_usage: 20,
            memory_limit: 30,
            pids: 40,
            filesystem_usage: 50,
            rx_bytes: 60,
            tx_bytes: 70,
        },
    );
    let names: Vec<&str> = metrics.iter().map(|metric| metric.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "container_cpu_usage_seconds_total",
            "container_memory_working_set_bytes",
            "container_memory_usage_bytes",
            "container_spec_memory_limit_bytes",
            "container_pids_current",
            "container_filesystem_usage_bytes",
            "pod_network_receive_bytes_total",
            "pod_network_transmit_bytes_total",
        ]
    );
}

#[tokio::test]
async fn list_containers_returns_empty_when_short_id_filter_is_ambiguous() {
    let service = test_service();

    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-ambiguous-a".to_string(),
            test_container("container-ambiguous-a", "pod-a", HashMap::new()),
        );
        containers.insert(
            "container-ambiguous-b".to_string(),
            test_container("container-ambiguous-b", "pod-b", HashMap::new()),
        );
    }

    let response = RuntimeService::list_containers(
        &service,
        Request::new(ListContainersRequest {
            filter: Some(crate::proto::runtime::v1::ContainerFilter {
                id: "container-ambiguous".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        response.containers.is_empty(),
        "ambiguous short container ids should behave like cri-o filters and return no entries"
    );
}

#[tokio::test]
async fn container_stats_resolves_short_id_and_list_container_stats_skips_exited() {
    let (dir, service) = test_service_with_fake_runtime();

    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-running-123".to_string(),
            test_container("container-running-123", "pod-1", HashMap::new()),
        );
        containers.insert(
            "container-stopped-456".to_string(),
            test_container("container-stopped-456", "pod-1", HashMap::new()),
        );
    }
    set_fake_runtime_state(&dir, "container-running-123", "running");
    set_fake_runtime_state(&dir, "container-stopped-456", "stopped");

    let stats = RuntimeService::container_stats(
        &service,
        Request::new(ContainerStatsRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .stats
    .expect("short container id should resolve to stats");
    assert_eq!(
        stats
            .attributes
            .as_ref()
            .expect("container stats should include attributes")
            .id,
        "container-running-123"
    );

    let list = RuntimeService::list_container_stats(
        &service,
        Request::new(ListContainerStatsRequest { filter: None }),
    )
    .await
    .unwrap()
    .into_inner();

    let ids = list
        .stats
        .iter()
        .filter_map(|stat| {
            stat.attributes
                .as_ref()
                .map(|attributes| attributes.id.clone())
        })
        .collect::<Vec<_>>();
    assert!(
        ids.iter().any(|id| id == "container-running-123"),
        "running containers should remain visible in list stats"
    );
    assert!(
        !ids.iter().any(|id| id == "container-stopped-456"),
        "exited containers should be filtered out from list stats by default"
    );
}

#[tokio::test]
async fn list_pod_sandbox_stats_applies_id_and_label_filters() {
    let service = test_service();

    let mut pod_alpha = test_pod("pod-alpha", HashMap::new());
    pod_alpha
        .labels
        .insert("tier".to_string(), "frontend".to_string());
    let pod_alpha_uid = pod_alpha
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();

    let mut pod_bravo = test_pod("pod-bravo", HashMap::new());
    pod_bravo
        .labels
        .insert("tier".to_string(), "backend".to_string());
    let pod_bravo_uid = pod_bravo
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert("pod-alpha".to_string(), pod_alpha);
        pods.insert("pod-bravo".to_string(), pod_bravo);
    }

    let mut container_alpha_annotations = HashMap::new();
    container_alpha_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_alpha_uid);
    let mut container_bravo_annotations = HashMap::new();
    container_bravo_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_bravo_uid);
    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-alpha".to_string(),
            test_container("container-alpha", "pod-alpha", container_alpha_annotations),
        );
        containers.insert(
            "container-bravo".to_string(),
            test_container("container-bravo", "pod-bravo", container_bravo_annotations),
        );
    }

    let mut selector = HashMap::new();
    selector.insert("tier".to_string(), "frontend".to_string());
    let response = RuntimeService::list_pod_sandbox_stats(
        &service,
        Request::new(ListPodSandboxStatsRequest {
            filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                id: "pod-al".to_string(),
                label_selector: selector,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.stats.len(), 1);
    assert_eq!(
        response.stats[0]
            .attributes
            .as_ref()
            .expect("pod stats should include attributes")
            .id,
        "pod-alpha"
    );
}

#[tokio::test]
async fn pod_sandbox_stats_resolves_short_id_and_hides_internal_annotations() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut pod_annotations = HashMap::new();
    pod_annotations.insert("visible".to_string(), "true".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-pod-short-123".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-short-123".to_string(),
        test_pod("pod-short-123", pod_annotations),
    );
    service.containers.lock().await.insert(
        "container-pod-short-123".to_string(),
        test_container("container-pod-short-123", "pod-short-123", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-pod-short-123", "running");
    set_fake_runtime_state(&dir, "pause-pod-short-123", "running");

    let response = RuntimeService::pod_sandbox_stats(
        &service,
        Request::new(PodSandboxStatsRequest {
            pod_sandbox_id: "pod-short".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let stats = response
        .stats
        .expect("short pod id should resolve to pod sandbox stats");
    let attributes = stats
        .attributes
        .expect("pod stats should include attributes");
    assert_eq!(attributes.id, "pod-short-123");
    assert_eq!(
        attributes.annotations.get("visible").map(String::as_str),
        Some("true")
    );
    assert!(
        !attributes.annotations.contains_key(INTERNAL_POD_STATE_KEY),
        "internal pod annotations should not leak through statsp responses"
    );
}

#[test]
fn parse_network_stats_from_procfs_aggregates_non_loopback_interfaces() {
    let raw = "\
Inter-|   Receive                                                |  Transmit\n\
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
    lo: 100 1 0 0 0 0 0 0 100 1 0 0 0 0 0 0\n\
  eth0: 200 2 3 4 0 0 0 0 300 5 6 7 0 0 0 0\n\
  eth1: 400 8 9 10 0 0 0 0 500 11 12 13 0 0 0 0\n";

    let stats = RuntimeServiceImpl::parse_network_stats_from_procfs(raw)
        .expect("non-loopback interfaces should produce network stats");
    assert_eq!(stats.rx_bytes, 600);
    assert_eq!(stats.rx_packets, 10);
    assert_eq!(stats.rx_errors, 12);
    assert_eq!(stats.rx_dropped, 14);
    assert_eq!(stats.tx_bytes, 800);
    assert_eq!(stats.tx_packets, 16);
    assert_eq!(stats.tx_errors, 18);
    assert_eq!(stats.tx_dropped, 20);
}

#[tokio::test]
async fn collect_pod_stats_returns_none_when_pod_has_no_collectable_containers() {
    let service = test_service();
    let pod = test_pod("pod-no-stats", HashMap::new());

    let stats = service.collect_pod_stats("pod-no-stats", &pod).await;
    assert!(stats.is_none());
}

#[tokio::test]
async fn list_pod_sandbox_stats_returns_empty_when_short_id_filter_is_ambiguous() {
    let service = test_service();

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(
            "pod-ambiguous-a".to_string(),
            test_pod("pod-ambiguous-a", HashMap::new()),
        );
        pods.insert(
            "pod-ambiguous-b".to_string(),
            test_pod("pod-ambiguous-b", HashMap::new()),
        );
    }

    let response = RuntimeService::list_pod_sandbox_stats(
        &service,
        Request::new(ListPodSandboxStatsRequest {
            filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                id: "pod-ambiguous".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        response.stats.is_empty(),
        "ambiguous short pod ids should return no stats entries"
    );
}

#[tokio::test]
async fn list_metric_descriptors_returns_non_empty_supported_names() {
    let service = test_service();
    let response = RuntimeService::list_metric_descriptors(
        &service,
        Request::new(ListMetricDescriptorsRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        !response.descriptors.is_empty(),
        "metric descriptors should not be empty"
    );
    let names: HashSet<String> = response
        .descriptors
        .into_iter()
        .map(|descriptor| descriptor.name)
        .collect();
    assert!(names.contains("container_cpu_usage_seconds_total"));
    assert!(names.contains("container_memory_working_set_bytes"));
    assert!(names.contains("container_memory_usage_bytes"));
    assert!(names.contains("container_spec_memory_limit_bytes"));
}

#[tokio::test]
async fn list_pod_sandbox_metrics_returns_pod_and_container_entries() {
    let service = test_service();

    let pod = test_pod("pod-metrics", HashMap::new());
    let pod_uid = pod
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-metrics".to_string(), pod);

    let mut container_annotations = HashMap::new();
    container_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_uid);
    service.containers.lock().await.insert(
        "container-metrics".to_string(),
        test_container("container-metrics", "pod-metrics", container_annotations),
    );

    let response = RuntimeService::list_pod_sandbox_metrics(
        &service,
        Request::new(ListPodSandboxMetricsRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.pod_metrics.len(), 1);
    let pod_metrics = &response.pod_metrics[0];
    assert_eq!(pod_metrics.pod_sandbox_id, "pod-metrics");
    assert!(
        !pod_metrics.metrics.is_empty(),
        "pod-level metrics should not be empty"
    );
    assert_eq!(pod_metrics.container_metrics.len(), 1);
    assert_eq!(
        pod_metrics.container_metrics[0].container_id,
        "container-metrics"
    );
    assert!(
        !pod_metrics.container_metrics[0].metrics.is_empty(),
        "container-level metrics should not be empty"
    );
}

#[tokio::test]
async fn list_pod_sandbox_metrics_respects_included_pod_metrics_configuration() {
    let mut service = test_service();
    service.config.included_pod_metrics = vec!["cpu".to_string(), "network".to_string()];

    let pod_metrics = service.build_pod_metrics(
        "pod-metrics-filtered",
        1,
        crate::server::stats::PodMetricTotals {
            cpu_usage: 10,
            memory_usage: 20,
            memory_limit: 30,
            pids: 40,
            filesystem_usage: 50,
            rx_bytes: 60,
            tx_bytes: 70,
        },
    );
    let metric_names: Vec<&str> = pod_metrics
        .iter()
        .map(|metric| metric.name.as_str())
        .collect();
    assert_eq!(
        metric_names,
        vec![
            "container_cpu_usage_seconds_total",
            "pod_network_receive_bytes_total",
            "pod_network_transmit_bytes_total",
        ]
    );
}

#[tokio::test]
async fn get_container_events_streams_broadcast_events() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.publish_event(ContainerEventResponse {
        container_id: "container-1".to_string(),
        container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.container_id, "container-1");
    assert_eq!(
        event.container_event_type,
        ContainerEventType::ContainerCreatedEvent as i32
    );
}

#[tokio::test]
async fn get_container_events_broadcasts_to_multiple_subscribers() {
    let service = test_service();
    let mut stream_a =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();
    let mut stream_b =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.publish_event(ContainerEventResponse {
        container_id: "shared".to_string(),
        container_event_type: ContainerEventType::ContainerStartedEvent as i32,
        created_at: 2,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event_a = stream_a.next().await.unwrap().unwrap();
    let event_b = stream_b.next().await.unwrap().unwrap();
    assert_eq!(event_a.container_id, "shared");
    assert_eq!(event_b.container_id, "shared");
    assert_eq!(event_a.container_event_type, event_b.container_event_type);
}

#[tokio::test]
async fn get_container_events_preserves_publish_order() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    for (id, event_type) in [
        ("created", ContainerEventType::ContainerCreatedEvent),
        ("started", ContainerEventType::ContainerStartedEvent),
        ("stopped", ContainerEventType::ContainerStoppedEvent),
        ("deleted", ContainerEventType::ContainerDeletedEvent),
    ] {
        service.publish_event(ContainerEventResponse {
            container_id: id.to_string(),
            container_event_type: event_type as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    let mut received = Vec::new();
    for _ in 0..4 {
        let event = stream.next().await.unwrap().unwrap();
        received.push((event.container_id, event.container_event_type));
    }

    assert_eq!(
        received,
        vec![
            (
                "created".to_string(),
                ContainerEventType::ContainerCreatedEvent as i32
            ),
            (
                "started".to_string(),
                ContainerEventType::ContainerStartedEvent as i32
            ),
            (
                "stopped".to_string(),
                ContainerEventType::ContainerStoppedEvent as i32
            ),
            (
                "deleted".to_string(),
                ContainerEventType::ContainerDeletedEvent as i32
            ),
        ]
    );
}

#[tokio::test]
async fn pod_events_use_pause_container_id_when_available_and_preserve_order() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-event-1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let pod = test_pod("pod-event-1", annotations);

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service
        .emit_pod_event(ContainerEventType::ContainerCreatedEvent, &pod, Vec::new())
        .await;
    service
        .emit_pod_event(ContainerEventType::ContainerStartedEvent, &pod, Vec::new())
        .await;

    let created = stream.next().await.unwrap().unwrap();
    let started = stream.next().await.unwrap().unwrap();
    assert_eq!(created.container_id, "pause-event-1");
    assert_eq!(started.container_id, "pause-event-1");
    assert_eq!(
        created.container_event_type,
        ContainerEventType::ContainerCreatedEvent as i32
    );
    assert_eq!(
        started.container_event_type,
        ContainerEventType::ContainerStartedEvent as i32
    );
    assert_eq!(
        started
            .pod_sandbox_status
            .expect("pod status should be included")
            .id,
        "pod-event-1"
    );
}

#[tokio::test]
async fn pod_events_can_be_disabled() {
    let mut service = test_service();
    service.config.enable_pod_events = false;

    let pod = test_pod("pod-event-disabled", HashMap::new());
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service
        .emit_pod_event(ContainerEventType::ContainerCreatedEvent, &pod, Vec::new())
        .await;

    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "disabled pod events should not enqueue stream items"
    );
}

#[tokio::test]
async fn publish_event_without_subscribers_does_not_panic() {
    let service = test_service();
    service.publish_event(ContainerEventResponse {
        container_id: "orphan".to_string(),
        container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });
}

#[tokio::test]
async fn get_container_events_reports_lagged_consumers() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    for idx in 0..600 {
        service.publish_event(ContainerEventResponse {
            container_id: format!("container-{}", idx),
            container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
            created_at: idx,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    let mut saw_lagged = false;
    for _ in 0..260 {
        match timeout(Duration::from_millis(200), stream.next()).await {
            Ok(Some(Err(status))) if status.code() == tonic::Code::ResourceExhausted => {
                saw_lagged = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }

    assert!(saw_lagged, "expected lagged consumer error");
}

#[tokio::test]
async fn exit_monitor_publishes_async_stop_events() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "async-stop".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("async-stop", "pod-1", annotations)
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "async-stop",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &service
                .containers
                .lock()
                .await
                .get("async-stop")
                .unwrap()
                .annotations,
        )
        .unwrap();
    service.ensure_exit_monitor_registered("async-stop");

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    let exit_code_path = dir.path().join("exits").join("async-stop");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "17").unwrap();

    let event = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Ok(event)) = stream.next().await {
                if event.container_id == "async-stop"
                    && event.container_event_type
                        == ContainerEventType::ContainerStoppedEvent as i32
                {
                    return event;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for async stop event");

    assert_eq!(event.container_id, "async-stop");
    let container = service
        .containers
        .lock()
        .await
        .get("async-stop")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(17));
    assert!(state.nri_stop_notified);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("async-stop")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.nri_stop_notified);
}
