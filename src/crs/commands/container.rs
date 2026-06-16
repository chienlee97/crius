use std::io::Write;

use crate::crs::{
    args::{
        ContainerCommand, ContainerCreateArgs, ContainerListArgs, ContainerStateArg,
        ContainerStatsArgs,
    },
    builders::{build_container_config, build_resources_from_specs},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{
        format_unix_nanos, CommandOutput, ContainerOperationView, ContainerView, InspectView,
        ResourceUsageView,
    },
    parsers::parse_key_value,
};
use crate::proto::runtime::v1::{
    CheckpointContainerRequest, Container, ContainerFilter, ContainerState, ContainerStateValue,
    ContainerStats, ContainerStatsFilter, ContainerStatsRequest, ContainerStatus,
    ContainerStatusRequest, CreateContainerRequest, ExecSyncRequest, ListContainerStatsRequest,
    ListContainersRequest, PodSandboxConfig, PodSandboxMetadata, PodSandboxStatusRequest,
    RemoveContainerRequest, ReopenContainerLogRequest, StartContainerRequest, StopContainerRequest,
    UpdateContainerResourcesRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    command: ContainerCommand,
) -> Result<CommandResult, CliError> {
    match command {
        ContainerCommand::List(args) => handle_list(ctx, client, args).await,
        ContainerCommand::Inspect { id } => handle_inspect(ctx, client, id).await,
        ContainerCommand::Create(args) => handle_create(ctx, client, *args).await,
        ContainerCommand::Start { id } => handle_start(ctx, client, id).await,
        ContainerCommand::Stop { id, timeout } => handle_stop(ctx, client, id, timeout).await,
        ContainerCommand::Remove { id } => handle_remove(ctx, client, id).await,
        ContainerCommand::Exec(_) => Err(CliError::not_implemented("crs container exec")),
        ContainerCommand::ExecSync(args) => handle_exec_sync(ctx, client, args).await,
        ContainerCommand::Attach { .. } => Err(CliError::not_implemented("crs container attach")),
        ContainerCommand::Stats(args) => handle_stats(ctx, client, args).await,
        ContainerCommand::Checkpoint {
            id,
            location,
            timeout,
        } => handle_checkpoint(ctx, client, id, location, timeout).await,
        ContainerCommand::Update {
            id,
            resources,
            annotations,
        } => handle_update(ctx, client, id, resources, annotations).await,
        ContainerCommand::ReopenLog { id } => handle_reopen_log(ctx, client, id).await,
        ContainerCommand::Logs(_) => Err(CliError::not_implemented("crs container logs")),
    }
}

pub(crate) async fn handle_stats(
    ctx: &CliContext,
    client: &CrsClient,
    args: ContainerStatsArgs,
) -> Result<CommandResult, CliError> {
    if let Some(id) = args.id {
        ensure_container_id(&id, "crs container stats")?;
        let mut runtime = client.runtime()?;
        let response = client
            .with_rpc_timeout(async {
                runtime
                    .container_stats(ContainerStatsRequest {
                        container_id: id.clone(),
                    })
                    .await
                    .map_err(|status| {
                        container_status_error(status, client, "crs container stats", &id)
                    })
            })
            .await?
            .into_inner();
        let items = response
            .stats
            .into_iter()
            .map(container_stats_view)
            .collect();
        return render_and_print(
            ctx,
            CommandOutput::new("ContainerStats", client.endpoint(), items),
        );
    }

    handle_stats_list(ctx, client, args.pod, args.labels, "ContainerStats").await
}

pub(crate) async fn handle_stats_list(
    ctx: &CliContext,
    client: &CrsClient,
    pod: Option<String>,
    labels: Vec<String>,
    kind: &'static str,
) -> Result<CommandResult, CliError> {
    let filter = container_stats_filter(None, pod, labels)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_container_stats(ListContainerStatsRequest { filter })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs container stats")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();
    let items = response
        .stats
        .into_iter()
        .map(container_stats_view)
        .collect();

    render_and_print(ctx, CommandOutput::new(kind, client.endpoint(), items))
}

pub(crate) async fn handle_exec_sync(
    ctx: &CliContext,
    client: &CrsClient,
    args: crate::crs::args::ExecArgs,
) -> Result<CommandResult, CliError> {
    exec_sync_with_command(ctx, client, args, "crs container exec-sync").await
}

pub(crate) async fn exec_sync_with_command(
    ctx: &CliContext,
    client: &CrsClient,
    args: crate::crs::args::ExecArgs,
    command_name: &'static str,
) -> Result<CommandResult, CliError> {
    let options = crate::crs::streaming::ExecStreamOptions::from_args(
        args.container,
        args.command,
        args.stream,
    )?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .exec_sync(ExecSyncRequest {
                    container_id: options.container_id.clone(),
                    cmd: options.command.clone(),
                    timeout: 0,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command(command_name)
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {}", options.container_id))
                })
        })
        .await?
        .into_inner();

    if matches!(ctx.output(), crate::crs::args::OutputArg::Json) {
        let envelope = serde_json::json!({
            "kind": "ContainerExecSync",
            "apiVersion": crate::crs::format::API_VERSION,
            "endpoint": client.endpoint(),
            "summary": {
                "containerId": options.container_id,
                "exitCode": response.exit_code,
            },
            "stdout": String::from_utf8_lossy(&response.stdout),
            "stderr": String::from_utf8_lossy(&response.stderr),
            "warnings": [],
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|source| CliError::internal(
                format!("failed to render exec-sync JSON: {source}")
            ))?
        );
    } else {
        std::io::stdout()
            .write_all(&response.stdout)
            .map_err(|source| CliError::internal(format!("failed to write stdout: {source}")))?;
        std::io::stderr()
            .write_all(&response.stderr)
            .map_err(|source| CliError::internal(format!("failed to write stderr: {source}")))?;
    }

    Ok(CommandResult::from_code(response.exit_code))
}

pub(crate) async fn handle_list(
    ctx: &CliContext,
    client: &CrsClient,
    args: ContainerListArgs,
) -> Result<CommandResult, CliError> {
    let filter = container_filter_from_args(args)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_containers(ListContainersRequest { filter })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs container list")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let views = response
        .containers
        .into_iter()
        .map(container_view)
        .collect();
    render_and_print(
        ctx,
        CommandOutput::new("ContainerList", client.endpoint(), views),
    )
}

pub(crate) async fn handle_inspect(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .container_status(ContainerStatusRequest {
                    container_id: id.clone(),
                    verbose: true,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs container inspect")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {id}"))
                })
        })
        .await?
        .into_inner();

    let mut warnings = Vec::new();
    let (mut info_json, mut info_raw) = parse_info_map(&response.info, &mut warnings);
    let id = response
        .status
        .as_ref()
        .map(|status| status.id.clone())
        .unwrap_or_else(|| id.clone());
    let is_local_container = response
        .status
        .as_ref()
        .is_some_and(is_local_container_status);
    if is_local_container {
        hide_empty_sandbox_info(&mut info_json);
        hide_empty_sandbox_info(&mut info_raw);
    }
    let response_json = container_status_json(response.status.as_ref(), is_local_container);

    render_and_print(
        ctx,
        CommandOutput::new(
            "ContainerInspect",
            client.endpoint(),
            vec![InspectView {
                object_type: "container".to_string(),
                id,
                response: response_json,
                info_json,
                info_raw,
            }],
        )
        .with_warnings(warnings),
    )
}

pub(crate) async fn handle_create(
    ctx: &CliContext,
    client: &CrsClient,
    args: ContainerCreateArgs,
) -> Result<CommandResult, CliError> {
    let sandbox_config = fetch_sandbox_config(client, &args.pod, "crs container create").await?;
    let container_config = build_container_config(&args).map_err(CliError::invalid_input)?;
    let image = args.image.clone();
    let pod = args.pod.clone();
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .create_container(CreateContainerRequest {
                    pod_sandbox_id: pod.clone(),
                    config: Some(container_config),
                    sandbox_config: Some(sandbox_config),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs container create")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?
        .into_inner();

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerCreate",
            container_id: response.container_id.clone(),
            pod_id: pod,
            image,
            action: "created",
            summary: serde_json::json!({
                "containerId": response.container_id,
                "created": true,
            }),
        },
    )
}

pub(crate) async fn handle_start(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
) -> Result<CommandResult, CliError> {
    ensure_container_id(&id, "crs container start")?;
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .start_container(StartContainerRequest {
                    container_id: id.clone(),
                })
                .await
                .map_err(|status| {
                    container_status_error(status, client, "crs container start", &id)
                })
        })
        .await?;

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerStart",
            container_id: id.clone(),
            pod_id: String::new(),
            image: String::new(),
            action: "started",
            summary: serde_json::json!({
                "containerId": id,
                "started": true,
            }),
        },
    )
}

pub(crate) async fn handle_stop(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
    timeout: Option<u32>,
) -> Result<CommandResult, CliError> {
    ensure_container_id(&id, "crs container stop")?;
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .stop_container(StopContainerRequest {
                    container_id: id.clone(),
                    timeout: timeout.map(i64::from).unwrap_or_default(),
                })
                .await
                .map_err(|status| container_status_error(status, client, "crs container stop", &id))
        })
        .await?;

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerStop",
            container_id: id.clone(),
            pod_id: String::new(),
            image: String::new(),
            action: "stopped",
            summary: serde_json::json!({
                "containerId": id,
                "stopped": true,
                "timeoutSeconds": timeout.unwrap_or_default(),
            }),
        },
    )
}

pub(crate) async fn handle_remove(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
) -> Result<CommandResult, CliError> {
    handle_remove_with_command(ctx, client, id, "crs container remove").await
}

pub(crate) async fn handle_remove_with_command(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
    command_name: &'static str,
) -> Result<CommandResult, CliError> {
    ensure_container_id(&id, command_name)?;
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .remove_container(RemoveContainerRequest {
                    container_id: id.clone(),
                })
                .await
                .map_err(|status| container_status_error(status, client, command_name, &id))
        })
        .await?;

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerRemove",
            container_id: id.clone(),
            pod_id: String::new(),
            image: String::new(),
            action: "removed",
            summary: serde_json::json!({
                "containerId": id,
                "removed": true,
            }),
        },
    )
}

pub(crate) async fn handle_update(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
    resources: Vec<String>,
    annotations: Vec<String>,
) -> Result<CommandResult, CliError> {
    ensure_container_id(&id, "crs container update")?;
    if resources.is_empty() && annotations.is_empty() {
        return Err(CliError::invalid_input(
            "container update requires at least one --resource or --annotation field",
        )
        .with_command("crs container update"));
    }

    let linux = build_resources_from_specs(&resources).map_err(CliError::invalid_input)?;
    let annotations = annotations
        .iter()
        .map(|annotation| {
            parse_key_value("--annotation", annotation).map(|pair| (pair.key, pair.value))
        })
        .collect::<Result<std::collections::HashMap<_, _>, _>>()
        .map_err(CliError::invalid_input)?;
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .update_container_resources(UpdateContainerResourcesRequest {
                    container_id: id.clone(),
                    linux,
                    windows: None,
                    annotations,
                })
                .await
                .map_err(|status| {
                    container_status_error(status, client, "crs container update", &id)
                })
        })
        .await?;

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerUpdate",
            container_id: id.clone(),
            pod_id: String::new(),
            image: String::new(),
            action: "updated",
            summary: serde_json::json!({
                "containerId": id,
                "updated": true,
            }),
        },
    )
}

pub(crate) async fn handle_checkpoint(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
    location: String,
    timeout: Option<u32>,
) -> Result<CommandResult, CliError> {
    ensure_container_id(&id, "crs container checkpoint")?;
    if location.is_empty() {
        return Err(
            CliError::invalid_input("checkpoint location must not be empty")
                .with_command("crs container checkpoint")
                .with_object(format!("container {id}")),
        );
    }

    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .checkpoint_container(CheckpointContainerRequest {
                    container_id: id.clone(),
                    location: location.clone(),
                    timeout: timeout.map(i64::from).unwrap_or_default(),
                })
                .await
                .map_err(|status| {
                    container_status_error(status, client, "crs container checkpoint", &id)
                })
        })
        .await?;

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerCheckpoint",
            container_id: id.clone(),
            pod_id: String::new(),
            image: String::new(),
            action: "checkpointed",
            summary: serde_json::json!({
                "containerId": id,
                "location": location,
                "checkpointed": true,
                "timeoutSeconds": timeout.unwrap_or_default(),
            }),
        },
    )
}

pub(crate) async fn handle_reopen_log(
    ctx: &CliContext,
    client: &CrsClient,
    id: String,
) -> Result<CommandResult, CliError> {
    ensure_container_id(&id, "crs container reopen-log")?;
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .reopen_container_log(ReopenContainerLogRequest {
                    container_id: id.clone(),
                })
                .await
                .map_err(|status| {
                    container_status_error(status, client, "crs container reopen-log", &id)
                })
        })
        .await?;

    render_container_operation(
        ctx,
        client,
        ContainerOperationRender {
            kind: "ContainerReopenLog",
            container_id: id.clone(),
            pod_id: String::new(),
            image: String::new(),
            action: "reopened",
            summary: serde_json::json!({
                "containerId": id,
                "reopened": true,
            }),
        },
    )
}

pub(crate) fn container_filter_from_args(
    args: ContainerListArgs,
) -> Result<Option<ContainerFilter>, CliError> {
    let labels = args
        .labels
        .iter()
        .map(|label| parse_key_value("--label", label).map(|pair| (pair.key, pair.value)))
        .collect::<Result<std::collections::HashMap<_, _>, _>>()
        .map_err(CliError::invalid_input)?;
    let state = if args.all {
        None
    } else {
        Some(ContainerStateValue {
            state: container_state(args.state.unwrap_or(ContainerStateArg::Running)) as i32,
        })
    };

    if args.id.is_none() && args.pod.is_none() && state.is_none() && labels.is_empty() {
        return Ok(None);
    }

    Ok(Some(ContainerFilter {
        id: args.id.unwrap_or_default(),
        state,
        pod_sandbox_id: args.pod.unwrap_or_default(),
        label_selector: labels,
    }))
}

pub(crate) fn container_stats_filter(
    id: Option<String>,
    pod: Option<String>,
    labels: Vec<String>,
) -> Result<Option<ContainerStatsFilter>, CliError> {
    let labels = labels
        .iter()
        .map(|label| parse_key_value("--label", label).map(|pair| (pair.key, pair.value)))
        .collect::<Result<std::collections::HashMap<_, _>, _>>()
        .map_err(CliError::invalid_input)?;

    if id.is_none() && pod.is_none() && labels.is_empty() {
        return Ok(None);
    }

    Ok(Some(ContainerStatsFilter {
        id: id.unwrap_or_default(),
        pod_sandbox_id: pod.unwrap_or_default(),
        label_selector: labels,
    }))
}

async fn fetch_sandbox_config(
    client: &CrsClient,
    pod: &str,
    command_name: &'static str,
) -> Result<PodSandboxConfig, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .pod_sandbox_status(PodSandboxStatusRequest {
                    pod_sandbox_id: pod.to_string(),
                    verbose: false,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command(command_name)
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?
        .into_inner();

    response
        .status
        .and_then(|status| status.metadata)
        .map(sandbox_config_from_metadata)
        .ok_or_else(|| {
            CliError::invalid_input(format!(
                "daemon did not return sandbox metadata for pod {pod}"
            ))
            .with_command(command_name)
            .with_object(format!("pod {pod}"))
        })
}

fn sandbox_config_from_metadata(metadata: PodSandboxMetadata) -> PodSandboxConfig {
    PodSandboxConfig {
        metadata: Some(metadata),
        ..Default::default()
    }
}

fn ensure_container_id(id: &str, command_name: &'static str) -> Result<(), CliError> {
    if id.is_empty() {
        return Err(
            CliError::invalid_input("container ID must not be empty").with_command(command_name)
        );
    }
    Ok(())
}

fn container_status_error(
    status: tonic::Status,
    client: &CrsClient,
    command_name: &'static str,
    id: &str,
) -> CliError {
    CliError::from_tonic_status(status)
        .with_command(command_name)
        .with_endpoint(client.endpoint())
        .with_object(format!("container {id}"))
}

struct ContainerOperationRender {
    kind: &'static str,
    pod_id: String,
    container_id: String,
    image: String,
    action: &'static str,
    summary: serde_json::Value,
}

fn render_container_operation(
    ctx: &CliContext,
    client: &CrsClient,
    operation: ContainerOperationRender,
) -> Result<CommandResult, CliError> {
    render_and_print(
        ctx,
        CommandOutput::new(
            operation.kind,
            client.endpoint(),
            vec![ContainerOperationView {
                container_id: operation.container_id,
                pod_id: operation.pod_id,
                image: operation.image,
                action: operation.action.to_string(),
                success: true,
            }],
        )
        .with_summary(operation.summary),
    )
}

pub(crate) fn container_view(container: Container) -> ContainerView {
    let metadata = container.metadata.unwrap_or_default();
    let image = container
        .image
        .map(|image| {
            if image.user_specified_image.is_empty() {
                image.image
            } else {
                image.user_specified_image
            }
        })
        .unwrap_or_default();

    ContainerView {
        container_id: container.id,
        pod: container.pod_sandbox_id,
        image,
        state: container_state_name(container.state).to_string(),
        created: format_unix_nanos(container.created_at, std::time::SystemTime::now()),
        name: metadata.name,
        attempt: metadata.attempt,
    }
}

pub(crate) fn container_state(state: ContainerStateArg) -> ContainerState {
    match state {
        ContainerStateArg::Created => ContainerState::ContainerCreated,
        ContainerStateArg::Running => ContainerState::ContainerRunning,
        ContainerStateArg::Exited => ContainerState::ContainerExited,
        ContainerStateArg::Unknown => ContainerState::ContainerUnknown,
    }
}

fn container_state_name(state: i32) -> &'static str {
    match ContainerState::try_from(state).ok() {
        Some(ContainerState::ContainerCreated) => "created",
        Some(ContainerState::ContainerRunning) => "running",
        Some(ContainerState::ContainerExited) => "exited",
        Some(ContainerState::ContainerUnknown) => "unknown",
        None => "unknown",
    }
}

pub(crate) fn container_stats_view(stats: ContainerStats) -> ResourceUsageView {
    let attributes = stats.attributes.unwrap_or_default();
    let metadata = attributes.metadata.unwrap_or_default();
    let cpu_nano_cores = stats
        .cpu
        .and_then(|cpu| cpu.usage_nano_cores)
        .map(|value| value.value)
        .unwrap_or_default();
    let memory_bytes = stats
        .memory
        .and_then(|memory| memory.working_set_bytes)
        .map(|value| value.value)
        .unwrap_or_default();

    ResourceUsageView {
        id: attributes.id,
        name: metadata.name,
        cpu_nano_cores,
        memory_bytes,
        pids: 0,
    }
}

fn container_status_json(
    status: Option<&ContainerStatus>,
    hide_empty_sandbox_fields: bool,
) -> serde_json::Value {
    serde_json::json!({
        "status": status.map(|status| {
            let metadata = status.metadata.as_ref();
            let image = status.image.as_ref();
            let mut annotations = status.annotations.clone();
            if hide_empty_sandbox_fields {
                annotations.retain(|key, value| {
                    !value.is_empty()
                        || !matches!(
                            key.as_str(),
                            "io.kubernetes.cri.sandbox-id" | "io.kubernetes.cri-o.SandboxID"
                        )
                });
            }
            serde_json::json!({
                "id": status.id,
                "metadata": metadata.map(|metadata| serde_json::json!({
                    "name": metadata.name,
                    "attempt": metadata.attempt,
                })),
                "state": container_state_name(status.state),
                "createdAt": status.created_at,
                "startedAt": status.started_at,
                "finishedAt": status.finished_at,
                "exitCode": status.exit_code,
                "image": image.map(|image| serde_json::json!({
                    "image": image.image,
                    "annotations": image.annotations,
                    "userSpecifiedImage": image.user_specified_image,
                    "runtimeHandler": image.runtime_handler,
                })),
                "imageRef": status.image_ref,
                "reason": status.reason,
                "message": status.message,
                "labels": status.labels,
                "annotations": annotations,
                "logPath": status.log_path,
            })
        })
    })
}

fn is_local_container_status(status: &ContainerStatus) -> bool {
    status
        .annotations
        .get("io.kubernetes.cri.sandbox-id")
        .or_else(|| status.annotations.get("io.kubernetes.cri-o.SandboxID"))
        .is_some_and(|pod_id| pod_id.is_empty())
}

fn hide_empty_sandbox_info(value: &mut serde_json::Value) {
    let Some(info_value) = value.get_mut("info") else {
        return;
    };
    if let Some(raw) = info_value.as_str() {
        if let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(raw) {
            remove_empty_sandbox_fields_recursively(&mut parsed);
            if let Ok(sanitized) = serde_json::to_string(&parsed) {
                *info_value = serde_json::Value::String(sanitized);
            }
        }
        return;
    }
    remove_empty_sandbox_fields_recursively(info_value);
}

fn remove_empty_sandbox_fields_recursively(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            let keys: Vec<String> = map
                .iter()
                .filter_map(|(key, value)| {
                    (is_empty_sandbox_field(key, value)).then(|| key.clone())
                })
                .collect();
            for key in keys {
                map.remove(&key);
            }
            for value in map.values_mut() {
                remove_empty_sandbox_fields_recursively(value);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                remove_empty_sandbox_fields_recursively(item);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn is_empty_json_value(value: &serde_json::Value) -> bool {
    value.as_str().is_some_and(str::is_empty)
}

fn is_empty_sandbox_field(key: &str, value: &serde_json::Value) -> bool {
    is_empty_json_value(value)
        && matches!(
            key,
            "sandboxID"
                | "podSandboxId"
                | "io.kubernetes.cri.sandbox-id"
                | "io.kubernetes.cri-o.SandboxID"
        )
}
