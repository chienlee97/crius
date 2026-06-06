use crate::crs::{
    args::{PodCommand, PodCreateArgs, PodListArgs, PodStateArg, PodStatsArgs},
    builders::{build_pod_sandbox_config, build_resources_from_specs},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, InspectView, PodMetricsView, PodOperationView, PodStatsView, PodView},
    parsers::parse_key_value,
};
use crate::proto::runtime::v1::{
    ListPodSandboxMetricsRequest, ListPodSandboxRequest, ListPodSandboxStatsRequest, PodSandbox,
    PodSandboxFilter, PodSandboxMetrics, PodSandboxState, PodSandboxStateValue, PodSandboxStats,
    PodSandboxStatsFilter, PodSandboxStatsRequest, PodSandboxStatus, PodSandboxStatusRequest,
    RemovePodSandboxRequest, RunPodSandboxRequest, StopPodSandboxRequest,
    UpdatePodSandboxResourcesRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    command: PodCommand,
) -> Result<CommandResult, CliError> {
    match command {
        PodCommand::List(args) => handle_list(ctx, client, args).await,
        PodCommand::Inspect { pod } => handle_inspect(ctx, client, pod).await,
        PodCommand::Run(args) => handle_run(ctx, client, *args).await,
        PodCommand::Stop { pod, timeout } => handle_stop(ctx, client, pod, timeout).await,
        PodCommand::Remove { pod } => handle_remove(ctx, client, pod).await,
        PodCommand::Stats(args) => handle_stats(ctx, client, args).await,
        PodCommand::Metrics => handle_metrics(ctx, client).await,
        PodCommand::UpdateResources {
            pod,
            overhead,
            pod_resource,
        } => handle_update_resources(ctx, client, pod, overhead, pod_resource).await,
        PodCommand::PortForward { .. } => Err(CliError::not_implemented("crs pod port-forward")),
    }
}

pub(crate) async fn handle_list(
    ctx: &CliContext,
    client: &CrsClient,
    args: PodListArgs,
) -> Result<CommandResult, CliError> {
    let filter = pod_filter_from_args(args)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_pod_sandbox(ListPodSandboxRequest { filter })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod list")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let views = response.items.into_iter().map(pod_view).collect();
    render_and_print(ctx, CommandOutput::new("PodList", client.endpoint(), views))
}

pub(crate) async fn handle_inspect(
    ctx: &CliContext,
    client: &CrsClient,
    pod: String,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .pod_sandbox_status(PodSandboxStatusRequest {
                    pod_sandbox_id: pod.clone(),
                    verbose: true,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod inspect")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?
        .into_inner();

    let mut warnings = Vec::new();
    let (info_json, info_raw) = parse_info_map(&response.info, &mut warnings);
    let id = response
        .status
        .as_ref()
        .map(|status| status.id.clone())
        .unwrap_or_else(|| pod.clone());
    let response_json = pod_status_json(response.status.as_ref());

    render_and_print(
        ctx,
        CommandOutput::new(
            "PodInspect",
            client.endpoint(),
            vec![InspectView {
                object_type: "pod".to_string(),
                id,
                response: response_json,
                info_json,
                info_raw,
            }],
        )
        .with_warnings(warnings),
    )
}

pub(crate) async fn handle_run(
    ctx: &CliContext,
    client: &CrsClient,
    args: PodCreateArgs,
) -> Result<CommandResult, CliError> {
    let config = build_pod_sandbox_config(&args).map_err(CliError::invalid_input)?;
    let metadata = config.metadata.clone().unwrap_or_default();
    let runtime_handler = args.runtime_handler.clone().unwrap_or_default();
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .run_pod_sandbox(RunPodSandboxRequest {
                    config: Some(config),
                    runtime_handler,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod run")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    render_and_print(
        ctx,
        CommandOutput::new(
            "PodRun",
            client.endpoint(),
            vec![PodOperationView {
                pod_id: response.pod_sandbox_id.clone(),
                name: metadata.name,
                namespace: metadata.namespace,
                action: "created".to_string(),
                success: true,
            }],
        )
        .with_summary(serde_json::json!({
            "podSandboxId": response.pod_sandbox_id,
            "created": true,
        })),
    )
}

pub(crate) async fn handle_stop(
    ctx: &CliContext,
    client: &CrsClient,
    pod: String,
    timeout: Option<u32>,
) -> Result<CommandResult, CliError> {
    if pod.is_empty() {
        return Err(CliError::invalid_input("pod must not be empty").with_command("crs pod stop"));
    }

    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .stop_pod_sandbox(StopPodSandboxRequest {
                    pod_sandbox_id: pod.clone(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod stop")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?;

    render_and_print(
        ctx,
        CommandOutput::new(
            "PodStop",
            client.endpoint(),
            vec![PodOperationView {
                pod_id: pod.clone(),
                name: String::new(),
                namespace: String::new(),
                action: "stopped".to_string(),
                success: true,
            }],
        )
        .with_summary(serde_json::json!({
            "podSandboxId": pod,
            "stopped": true,
            "timeoutSeconds": timeout.unwrap_or_default(),
        })),
    )
}

pub(crate) async fn handle_remove(
    ctx: &CliContext,
    client: &CrsClient,
    pod: String,
) -> Result<CommandResult, CliError> {
    if pod.is_empty() {
        return Err(CliError::invalid_input("pod must not be empty").with_command("crs pod remove"));
    }

    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .remove_pod_sandbox(RemovePodSandboxRequest {
                    pod_sandbox_id: pod.clone(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod remove")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?;

    render_and_print(
        ctx,
        CommandOutput::new(
            "PodRemove",
            client.endpoint(),
            vec![PodOperationView {
                pod_id: pod.clone(),
                name: String::new(),
                namespace: String::new(),
                action: "removed".to_string(),
                success: true,
            }],
        )
        .with_summary(serde_json::json!({
            "podSandboxId": pod,
            "removed": true,
        })),
    )
}

pub(crate) async fn handle_stats(
    ctx: &CliContext,
    client: &CrsClient,
    args: PodStatsArgs,
) -> Result<CommandResult, CliError> {
    if let Some(pod) = args.pod {
        if pod.is_empty() {
            return Err(
                CliError::invalid_input("pod must not be empty").with_command("crs pod stats")
            );
        }
        let mut runtime = client.runtime()?;
        let response = client
            .with_rpc_timeout(async {
                runtime
                    .pod_sandbox_stats(PodSandboxStatsRequest {
                        pod_sandbox_id: pod.clone(),
                    })
                    .await
                    .map_err(|status| {
                        CliError::from_tonic_status(status)
                            .with_command("crs pod stats")
                            .with_endpoint(client.endpoint())
                            .with_object(format!("pod {pod}"))
                    })
            })
            .await?
            .into_inner();
        let items = response.stats.into_iter().map(pod_stats_view).collect();
        return render_and_print(
            ctx,
            CommandOutput::new("PodStats", client.endpoint(), items),
        );
    }

    let filter = pod_stats_filter(None, args.labels)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_pod_sandbox_stats(ListPodSandboxStatsRequest { filter })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod stats")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();
    let items = response.stats.into_iter().map(pod_stats_view).collect();

    render_and_print(
        ctx,
        CommandOutput::new("PodStats", client.endpoint(), items),
    )
}

pub(crate) async fn handle_metrics(
    ctx: &CliContext,
    client: &CrsClient,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_pod_sandbox_metrics(ListPodSandboxMetricsRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod metrics")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();
    let items = response
        .pod_metrics
        .into_iter()
        .map(pod_metrics_view)
        .collect::<Vec<_>>();
    let total_metrics = items
        .iter()
        .map(|item| item.total_metric_count)
        .sum::<usize>();

    render_and_print(
        ctx,
        CommandOutput::new("PodMetrics", client.endpoint(), items).with_summary(
            serde_json::json!({
                "count": total_metrics,
                "totalMetrics": total_metrics,
            }),
        ),
    )
}

pub(crate) async fn handle_update_resources(
    ctx: &CliContext,
    client: &CrsClient,
    pod: String,
    overhead: Vec<String>,
    pod_resource: Vec<String>,
) -> Result<CommandResult, CliError> {
    if pod.is_empty() {
        return Err(CliError::invalid_input("pod must not be empty")
            .with_command("crs pod update-resources"));
    }
    if overhead.is_empty() && pod_resource.is_empty() {
        return Err(CliError::invalid_input(
            "pod update-resources requires at least one --overhead or --pod-resource field",
        )
        .with_command("crs pod update-resources"));
    }

    let overhead = build_resources_from_specs(&overhead).map_err(CliError::invalid_input)?;
    let resources = build_resources_from_specs(&pod_resource).map_err(CliError::invalid_input)?;
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .update_pod_sandbox_resources(UpdatePodSandboxResourcesRequest {
                    pod_sandbox_id: pod.clone(),
                    overhead,
                    resources,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod update-resources")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?;

    render_and_print(
        ctx,
        CommandOutput::new(
            "PodResourceUpdate",
            client.endpoint(),
            vec![PodOperationView {
                pod_id: pod.clone(),
                name: String::new(),
                namespace: String::new(),
                action: "updated".to_string(),
                success: true,
            }],
        )
        .with_summary(serde_json::json!({
            "podSandboxId": pod,
            "updated": true,
        })),
    )
}

pub(crate) fn pod_filter_from_args(
    args: PodListArgs,
) -> Result<Option<PodSandboxFilter>, CliError> {
    let labels = args
        .labels
        .iter()
        .map(|label| parse_key_value("--label", label).map(|pair| (pair.key, pair.value)))
        .collect::<Result<std::collections::HashMap<_, _>, _>>()
        .map_err(CliError::invalid_input)?;
    let state = if args.all {
        None
    } else {
        Some(PodSandboxStateValue {
            state: pod_state(args.state.unwrap_or(PodStateArg::Ready)) as i32,
        })
    };

    if args.id.is_none() && state.is_none() && labels.is_empty() {
        return Ok(None);
    }

    Ok(Some(PodSandboxFilter {
        id: args.id.unwrap_or_default(),
        state,
        label_selector: labels,
    }))
}

pub(crate) fn pod_stats_filter(
    id: Option<String>,
    labels: Vec<String>,
) -> Result<Option<PodSandboxStatsFilter>, CliError> {
    let labels = labels
        .iter()
        .map(|label| parse_key_value("--label", label).map(|pair| (pair.key, pair.value)))
        .collect::<Result<std::collections::HashMap<_, _>, _>>()
        .map_err(CliError::invalid_input)?;

    if id.is_none() && labels.is_empty() {
        return Ok(None);
    }

    Ok(Some(PodSandboxStatsFilter {
        id: id.unwrap_or_default(),
        label_selector: labels,
    }))
}

pub(crate) fn pod_view(pod: PodSandbox) -> PodView {
    let metadata = pod.metadata.unwrap_or_default();
    PodView {
        pod_id: pod.id,
        name: metadata.name,
        namespace: metadata.namespace,
        state: pod_state_name(pod.state).to_string(),
        ip: String::new(),
        created: pod.created_at.to_string(),
        attempt: metadata.attempt,
    }
}

pub(crate) fn pod_stats_view(stats: PodSandboxStats) -> PodStatsView {
    let attributes = stats.attributes.unwrap_or_default();
    let metadata = attributes.metadata.unwrap_or_default();
    let linux = stats.linux;
    let cpu_nano_cores = linux
        .as_ref()
        .and_then(|linux| linux.cpu.as_ref())
        .and_then(|cpu| cpu.usage_nano_cores.as_ref())
        .map(|value| value.value)
        .unwrap_or_default();
    let memory_bytes = linux
        .as_ref()
        .and_then(|linux| linux.memory.as_ref())
        .and_then(|memory| memory.working_set_bytes.as_ref())
        .map(|value| value.value)
        .unwrap_or_default();
    let network_bytes = linux
        .as_ref()
        .and_then(|linux| linux.network.as_ref())
        .map(network_bytes)
        .unwrap_or_default();
    let pids = linux
        .as_ref()
        .and_then(|linux| linux.process.as_ref())
        .and_then(|process| process.process_count.as_ref())
        .map(|value| value.value)
        .unwrap_or_default();

    PodStatsView {
        id: attributes.id,
        name: metadata.name,
        namespace: metadata.namespace,
        cpu_nano_cores,
        memory_bytes,
        network_bytes,
        pids,
    }
}

pub(crate) fn pod_metrics_view(metrics: PodSandboxMetrics) -> PodMetricsView {
    let pod_metric_count = metrics.metrics.len();
    let container_metric_count = metrics
        .container_metrics
        .iter()
        .map(|container| container.metrics.len())
        .sum::<usize>();

    PodMetricsView {
        pod_id: metrics.pod_sandbox_id,
        pod_metric_count,
        container_metric_count,
        total_metric_count: pod_metric_count + container_metric_count,
    }
}

fn network_bytes(network: &crate::proto::runtime::v1::NetworkUsage) -> u64 {
    let default_bytes = network
        .default_interface
        .as_ref()
        .map(interface_bytes)
        .unwrap_or_default();
    default_bytes + network.interfaces.iter().map(interface_bytes).sum::<u64>()
}

fn interface_bytes(interface: &crate::proto::runtime::v1::NetworkInterfaceUsage) -> u64 {
    interface
        .rx_bytes
        .as_ref()
        .map(|value| value.value)
        .unwrap_or_default()
        + interface
            .tx_bytes
            .as_ref()
            .map(|value| value.value)
            .unwrap_or_default()
}

pub(crate) fn pod_state(state: PodStateArg) -> PodSandboxState {
    match state {
        PodStateArg::Ready => PodSandboxState::SandboxReady,
        PodStateArg::Notready => PodSandboxState::SandboxNotready,
    }
}

fn pod_state_name(state: i32) -> &'static str {
    match PodSandboxState::try_from(state).ok() {
        Some(PodSandboxState::SandboxReady) => "ready",
        Some(PodSandboxState::SandboxNotready) => "notready",
        None => "unknown",
    }
}

fn pod_status_json(status: Option<&PodSandboxStatus>) -> serde_json::Value {
    serde_json::json!({
        "status": status.map(|status| {
            let metadata = status.metadata.as_ref();
            serde_json::json!({
                "id": status.id,
                "metadata": metadata.map(|metadata| serde_json::json!({
                    "name": metadata.name,
                    "uid": metadata.uid,
                    "namespace": metadata.namespace,
                    "attempt": metadata.attempt,
                })),
                "state": pod_state_name(status.state),
                "createdAt": status.created_at,
                "network": status.network.as_ref().map(|network| serde_json::json!({
                    "ip": network.ip,
                    "additionalIps": network.additional_ips.iter().map(|ip| ip.ip.clone()).collect::<Vec<_>>(),
                })),
                "labels": status.labels,
                "annotations": status.annotations,
                "runtimeHandler": status.runtime_handler,
            })
        })
    })
}
