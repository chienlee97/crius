use super::*;

impl RuntimeServiceImpl {
    pub(super) fn convert_to_proto_container_stats(
        &self,
        stats: crate::metrics::ContainerStats,
    ) -> crate::proto::runtime::v1::ContainerStats {
        use crate::proto::runtime::v1::{
            ContainerStats, CpuUsage, MemoryUsage, SwapUsage, UInt64Value,
        };

        let writable_layer = self.container_writable_layer_usage(&stats.container_id);
        let swap = stats.memory.as_ref().map(|mem| SwapUsage {
            timestamp: stats.timestamp as i64,
            swap_available_bytes: None,
            swap_usage_bytes: Some(UInt64Value { value: mem.swap }),
        });

        ContainerStats {
            attributes: Some(crate::proto::runtime::v1::ContainerAttributes {
                id: stats.container_id.clone(),
                metadata: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
            }),
            cpu: stats.cpu.map(|cpu| CpuUsage {
                timestamp: stats.timestamp as i64,
                usage_core_nano_seconds: Some(UInt64Value {
                    value: cpu.usage_total,
                }),
                usage_nano_cores: Some(UInt64Value {
                    value: cpu.usage_user.saturating_add(cpu.usage_kernel),
                }),
            }),
            memory: stats.memory.map(|mem| MemoryUsage {
                timestamp: stats.timestamp as i64,
                working_set_bytes: Some(UInt64Value { value: mem.usage }),
                available_bytes: Some(UInt64Value {
                    value: mem.limit.saturating_sub(mem.usage),
                }),
                usage_bytes: Some(UInt64Value { value: mem.usage }),
                rss_bytes: Some(UInt64Value { value: mem.rss }),
                page_faults: Some(UInt64Value { value: mem.pgfault }),
                major_page_faults: Some(UInt64Value {
                    value: mem.pgmajfault,
                }),
            }),
            writable_layer,
            swap,
        }
    }

    pub(super) fn populate_container_stats_attributes(
        stats: &mut crate::proto::runtime::v1::ContainerStats,
        container: &Container,
    ) {
        stats.attributes = Some(crate::proto::runtime::v1::ContainerAttributes {
            id: container.id.clone(),
            metadata: container.metadata.clone(),
            labels: container.labels.clone(),
            annotations: Self::external_annotations(&container.annotations),
        });
    }

    pub(super) async fn collect_pod_stats(
        &self,
        pod_id: &str,
        pod: &crate::proto::runtime::v1::PodSandbox,
    ) -> Option<crate::proto::runtime::v1::PodSandboxStats> {
        use crate::proto::runtime::v1::{
            CpuUsage, LinuxPodSandboxStats, MemoryUsage, NetworkInterfaceUsage, NetworkUsage,
            PodSandboxAttributes, PodSandboxStats, ProcessUsage, UInt64Value,
        };

        let containers = self.containers.lock().await;
        let mut total_cpu_usage = 0u64;
        let mut total_memory_usage = 0u64;
        let mut total_memory_limit = 0u64;
        let mut total_pids = 0u64;
        let mut total_rx_bytes = 0u64;
        let mut total_rx_errors = 0u64;
        let mut total_tx_bytes = 0u64;
        let mut total_tx_errors = 0u64;
        let mut has_stats = false;
        let mut container_stats_list = Vec::new();

        let collector = MetricsCollector::new().ok()?;
        let pod_uid = pod.metadata.as_ref().map(|m| m.uid.clone());

        for (container_id, container) in containers.iter() {
            let belongs_to_pod = container.pod_sandbox_id == pod_id
                || pod_uid
                    .as_ref()
                    .and_then(|uid| {
                        container
                            .annotations
                            .get("io.kubernetes.pod.uid")
                            .map(|container_uid| container_uid == uid)
                    })
                    .unwrap_or(false);

            if belongs_to_pod {
                let cgroup_parent = self.container_cgroup_hint(container_id, container).await;

                if let Ok(stats) = collector.collect_container_stats(container_id, &cgroup_parent) {
                    if let Some(ref cpu) = stats.cpu {
                        total_cpu_usage += cpu.usage_total;
                    }
                    if let Some(ref mem) = stats.memory {
                        total_memory_usage += mem.usage;
                        total_memory_limit += mem.limit;
                    }
                    if let Some(ref pids) = stats.pids {
                        total_pids += pids.current;
                    }
                    if let Some(network) = self.container_network_stats(container_id).await {
                        total_rx_bytes = total_rx_bytes.saturating_add(network.rx_bytes);
                        total_rx_errors = total_rx_errors.saturating_add(network.rx_errors);
                        total_tx_bytes = total_tx_bytes.saturating_add(network.tx_bytes);
                        total_tx_errors = total_tx_errors.saturating_add(network.tx_errors);
                    }
                    has_stats = true;

                    let mut proto_stats = self.convert_to_proto_container_stats(stats);
                    Self::populate_container_stats_attributes(&mut proto_stats, container);
                    container_stats_list.push(proto_stats);
                }
            }
        }

        if !has_stats {
            return None;
        }

        if let Some(pause_container_id) =
            Self::read_internal_state::<StoredPodState>(&pod.annotations, INTERNAL_POD_STATE_KEY)
                .and_then(|state| state.pause_container_id)
        {
            if let Some(network) = self.container_network_stats(&pause_container_id).await {
                total_rx_bytes = network.rx_bytes;
                total_rx_errors = network.rx_errors;
                total_tx_bytes = network.tx_bytes;
                total_tx_errors = network.tx_errors;
            }
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let container_count = container_stats_list.len() as u64;

        Some(PodSandboxStats {
            attributes: Some(PodSandboxAttributes {
                id: pod_id.to_string(),
                metadata: pod.metadata.clone(),
                labels: pod.labels.clone(),
                annotations: Self::external_annotations(&pod.annotations),
            }),
            linux: Some(LinuxPodSandboxStats {
                cpu: Some(CpuUsage {
                    timestamp,
                    usage_core_nano_seconds: Some(UInt64Value {
                        value: total_cpu_usage,
                    }),
                    usage_nano_cores: Some(UInt64Value {
                        value: total_cpu_usage,
                    }),
                }),
                memory: Some(MemoryUsage {
                    timestamp,
                    working_set_bytes: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                    available_bytes: Some(UInt64Value {
                        value: total_memory_limit.saturating_sub(total_memory_usage),
                    }),
                    usage_bytes: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                    rss_bytes: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                    page_faults: Some(UInt64Value { value: 0 }),
                    major_page_faults: Some(UInt64Value { value: 0 }),
                }),
                containers: container_stats_list,
                network: Some(NetworkUsage {
                    timestamp,
                    default_interface: Some(NetworkInterfaceUsage {
                        name: "pod".to_string(),
                        rx_bytes: Some(UInt64Value {
                            value: total_rx_bytes,
                        }),
                        rx_errors: Some(UInt64Value {
                            value: total_rx_errors,
                        }),
                        tx_bytes: Some(UInt64Value {
                            value: total_tx_bytes,
                        }),
                        tx_errors: Some(UInt64Value {
                            value: total_tx_errors,
                        }),
                    }),
                    interfaces: Vec::new(),
                }),
                process: Some(ProcessUsage {
                    timestamp,
                    process_count: Some(UInt64Value {
                        value: total_pids.max(container_count),
                    }),
                }),
            }),
            windows: None,
        })
    }

    pub(super) async fn container_stats(
        &self,
        request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let container_id = self.resolve_container_id(&req.container_id).await?;

        let containers = self.containers.lock().await;
        let container = containers
            .get(&container_id)
            .ok_or_else(|| Status::not_found("Container not found"))?;
        let cgroup_parent = self.container_cgroup_hint(&container_id, container).await;

        let stats = match MetricsCollector::new() {
            Ok(collector) => match collector.collect_container_stats(&container_id, &cgroup_parent)
            {
                Ok(stats) => {
                    let mut proto_stats = self.convert_to_proto_container_stats(stats);
                    Self::populate_container_stats_attributes(&mut proto_stats, container);
                    Some(proto_stats)
                }
                Err(e) => {
                    log::warn!(
                        "Failed to collect stats for container {}: {}",
                        container_id,
                        e
                    );
                    None
                }
            },
            Err(e) => {
                log::warn!("Failed to create MetricsCollector: {}", e);
                None
            }
        };

        Ok(Response::new(ContainerStatsResponse { stats }))
    }

    pub(super) async fn list_container_stats(
        &self,
        request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_container_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListContainerStatsResponse {
                        stats: Vec::new(),
                    }));
                };
                filter.id = resolved_id;
            }
            if !filter.pod_sandbox_id.is_empty() {
                let Some(resolved_pod_id) = self
                    .resolve_pod_sandbox_id_for_filter(&filter.pod_sandbox_id)
                    .await
                else {
                    return Ok(Response::new(ListContainerStatsResponse {
                        stats: Vec::new(),
                    }));
                };
                filter.pod_sandbox_id = resolved_pod_id;
            }
            Some(filter)
        } else {
            None
        };

        let containers = self.containers.lock().await;
        let mut all_stats = Vec::new();
        let collector = match MetricsCollector::new() {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Failed to create MetricsCollector: {}", e);
                return Ok(Response::new(ListContainerStatsResponse {
                    stats: Vec::new(),
                }));
            }
        };

        for (container_id, container) in containers.iter() {
            if matches!(
                container.state,
                x if x == ContainerState::ContainerExited as i32
                    || x == ContainerState::ContainerUnknown as i32
            ) {
                continue;
            }
            if let Some(filter) = &filter {
                if !Self::container_matches_stats_filter(container, filter) {
                    continue;
                }
            }

            let cgroup_parent = self.container_cgroup_hint(container_id, container).await;
            if let Ok(stats) = collector.collect_container_stats(container_id, &cgroup_parent) {
                let mut proto_stats = self.convert_to_proto_container_stats(stats);
                Self::populate_container_stats_attributes(&mut proto_stats, container);
                all_stats.push(proto_stats);
            }
        }

        Ok(Response::new(ListContainerStatsResponse {
            stats: all_stats,
        }))
    }

    pub(super) async fn pod_sandbox_stats(
        &self,
        request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let pod_id = self.resolve_pod_sandbox_id(&req.pod_sandbox_id).await?;

        let pods = self.pod_sandboxes.lock().await;
        let pod = pods
            .get(&pod_id)
            .ok_or_else(|| Status::not_found("Pod sandbox not found"))?;
        let stats = self.collect_pod_stats(&pod_id, pod).await;

        Ok(Response::new(PodSandboxStatsResponse { stats }))
    }

    pub(super) async fn list_pod_sandbox_stats(
        &self,
        request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        self.best_effort_refresh_runtime_state().await;
        let req = request.into_inner();
        let filter = if let Some(mut filter) = req.filter {
            if !filter.id.is_empty() {
                let Some(resolved_id) = self.resolve_pod_sandbox_id_for_filter(&filter.id).await
                else {
                    return Ok(Response::new(ListPodSandboxStatsResponse {
                        stats: Vec::new(),
                    }));
                };
                filter.id = resolved_id;
            }
            Some(filter)
        } else {
            None
        };

        let pods: Vec<(String, crate::proto::runtime::v1::PodSandbox)> = {
            let pods = self.pod_sandboxes.lock().await;
            pods.iter()
                .map(|(pod_id, pod)| (pod_id.clone(), pod.clone()))
                .collect()
        };
        let mut all_stats = Vec::new();

        for (pod_id, pod) in pods {
            if let Some(filter) = &filter {
                if !Self::pod_sandbox_matches_stats_filter(&pod, filter) {
                    continue;
                }
            }
            if let Some(stats) = self.collect_pod_stats(&pod_id, &pod).await {
                all_stats.push(stats);
            }
        }

        Ok(Response::new(ListPodSandboxStatsResponse {
            stats: all_stats,
        }))
    }

    pub(super) async fn list_metric_descriptors(
        &self,
        _request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        use crate::proto::runtime::v1::MetricDescriptor;

        let descriptors = vec![
            MetricDescriptor {
                name: "container_cpu_usage_seconds_total".to_string(),
                help: "Total CPU usage in seconds".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_memory_working_set_bytes".to_string(),
                help: "Current working set memory in bytes".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_memory_usage_bytes".to_string(),
                help: "Total memory usage in bytes".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_spec_memory_limit_bytes".to_string(),
                help: "Memory limit in bytes".to_string(),
                label_keys: vec!["container_id".to_string()],
            },
            MetricDescriptor {
                name: "container_pids_current".to_string(),
                help: "Current number of processes in the container cgroup".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "container_filesystem_usage_bytes".to_string(),
                help: "Writable layer usage in bytes".to_string(),
                label_keys: vec!["container_id".to_string(), "pod_id".to_string()],
            },
            MetricDescriptor {
                name: "pod_network_receive_bytes_total".to_string(),
                help: "Total received bytes across pod interfaces".to_string(),
                label_keys: vec!["pod_id".to_string()],
            },
            MetricDescriptor {
                name: "pod_network_transmit_bytes_total".to_string(),
                help: "Total transmitted bytes across pod interfaces".to_string(),
                label_keys: vec!["pod_id".to_string()],
            },
        ];

        Ok(Response::new(ListMetricDescriptorsResponse { descriptors }))
    }

    pub(super) async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        use crate::proto::runtime::v1::{
            ContainerMetrics, Metric, MetricType, PodSandboxMetrics, UInt64Value,
        };
        use std::time::{SystemTime, UNIX_EPOCH};

        let pods = self.pod_sandboxes.lock().await;
        let containers = self.containers.lock().await;
        let mut pod_metrics_list = Vec::new();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        for (pod_id, pod) in pods.iter() {
            let mut metrics = Vec::new();
            let mut container_metrics_list = Vec::new();
            let pod_uid = pod.metadata.as_ref().map(|m| m.uid.clone());

            let mut total_cpu_usage = 0u64;
            let mut total_memory_usage = 0u64;
            let mut total_memory_limit = 0u64;
            let mut total_pids = 0u64;
            let mut total_filesystem_usage = 0u64;
            let mut total_rx_bytes = 0u64;
            let mut total_tx_bytes = 0u64;

            for (container_id, container) in containers.iter() {
                let belongs_to_pod = container.pod_sandbox_id == *pod_id
                    || pod_uid
                        .as_ref()
                        .and_then(|uid| {
                            container
                                .annotations
                                .get("io.kubernetes.pod.uid")
                                .map(|container_uid| container_uid == uid)
                        })
                        .unwrap_or(false);

                if belongs_to_pod {
                    let container_state = Self::read_internal_state::<StoredContainerState>(
                        &container.annotations,
                        INTERNAL_CONTAINER_STATE_KEY,
                    );

                    let cgroup_parent = container_state
                        .as_ref()
                        .and_then(|s| s.cgroup_parent.as_ref())
                        .map(PathBuf::from)
                        .unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup"));

                    if let Ok(collector) = MetricsCollector::new() {
                        if let Ok(stats) =
                            collector.collect_container_stats(container_id, &cgroup_parent)
                        {
                            let container_cpu =
                                stats.cpu.as_ref().map(|c| c.usage_total).unwrap_or(0);
                            let container_mem = stats.memory.as_ref().map(|m| m.usage).unwrap_or(0);
                            let container_mem_limit =
                                stats.memory.as_ref().map(|m| m.limit).unwrap_or(0);
                            let container_pids =
                                stats.pids.as_ref().map(|p| p.current).unwrap_or(0);
                            let container_fs_usage = self
                                .container_writable_layer_usage(container_id)
                                .and_then(|usage| usage.used_bytes.map(|bytes| bytes.value))
                                .unwrap_or(0);
                            let container_network =
                                self.container_network_stats(container_id).await;

                            total_cpu_usage += container_cpu;
                            total_memory_usage += container_mem;
                            total_memory_limit += container_mem_limit;
                            total_pids += container_pids;
                            total_filesystem_usage += container_fs_usage;
                            if let Some(network) = container_network {
                                total_rx_bytes += network.rx_bytes;
                                total_tx_bytes += network.tx_bytes;
                            }

                            let container_metric_list = vec![
                                Metric {
                                    name: "container_cpu_usage_seconds_total".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Counter as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_cpu,
                                    }),
                                },
                                Metric {
                                    name: "container_memory_working_set_bytes".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Gauge as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_mem,
                                    }),
                                },
                                Metric {
                                    name: "container_pids_current".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Gauge as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_pids,
                                    }),
                                },
                                Metric {
                                    name: "container_filesystem_usage_bytes".to_string(),
                                    timestamp,
                                    metric_type: MetricType::Gauge as i32,
                                    label_values: vec![container_id.clone(), pod_id.clone()],
                                    value: Some(UInt64Value {
                                        value: container_fs_usage,
                                    }),
                                },
                            ];

                            container_metrics_list.push(ContainerMetrics {
                                container_id: container_id.clone(),
                                metrics: container_metric_list,
                            });
                        }
                    }
                }
            }

            if !container_metrics_list.is_empty() {
                metrics.push(Metric {
                    name: "container_cpu_usage_seconds_total".to_string(),
                    timestamp,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_cpu_usage,
                    }),
                });
                metrics.push(Metric {
                    name: "container_memory_working_set_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                });
                metrics.push(Metric {
                    name: "container_memory_usage_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_memory_usage,
                    }),
                });
                metrics.push(Metric {
                    name: "container_spec_memory_limit_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_memory_limit,
                    }),
                });
                metrics.push(Metric {
                    name: "container_pids_current".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value { value: total_pids }),
                });
                metrics.push(Metric {
                    name: "container_filesystem_usage_bytes".to_string(),
                    timestamp,
                    metric_type: MetricType::Gauge as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_filesystem_usage,
                    }),
                });
                metrics.push(Metric {
                    name: "pod_network_receive_bytes_total".to_string(),
                    timestamp,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_rx_bytes,
                    }),
                });
                metrics.push(Metric {
                    name: "pod_network_transmit_bytes_total".to_string(),
                    timestamp,
                    metric_type: MetricType::Counter as i32,
                    label_values: vec![pod_id.clone()],
                    value: Some(UInt64Value {
                        value: total_tx_bytes,
                    }),
                });

                pod_metrics_list.push(PodSandboxMetrics {
                    pod_sandbox_id: pod_id.clone(),
                    metrics,
                    container_metrics: container_metrics_list,
                });
            }
        }

        Ok(Response::new(ListPodSandboxMetricsResponse {
            pod_metrics: pod_metrics_list,
        }))
    }
}
