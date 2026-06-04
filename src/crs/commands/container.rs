use crate::crs::{
    args::{ContainerCommand, ContainerListArgs, ContainerStateArg},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, ContainerView, InspectView},
    parsers::parse_key_value,
};
use crate::proto::runtime::v1::{
    Container, ContainerFilter, ContainerState, ContainerStateValue, ContainerStatus,
    ContainerStatusRequest, ListContainersRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    command: ContainerCommand,
) -> Result<CommandResult, CliError> {
    match command {
        ContainerCommand::List(args) => handle_list(ctx, client, args).await,
        ContainerCommand::Inspect { id } => handle_inspect(ctx, client, id).await,
        ContainerCommand::Create(_) => Err(CliError::not_implemented("crs container create")),
        ContainerCommand::Start { .. } => Err(CliError::not_implemented("crs container start")),
        ContainerCommand::Stop { .. } => Err(CliError::not_implemented("crs container stop")),
        ContainerCommand::Remove { .. } => Err(CliError::not_implemented("crs container remove")),
        ContainerCommand::Exec(_) => Err(CliError::not_implemented("crs container exec")),
        ContainerCommand::ExecSync(_) => Err(CliError::not_implemented("crs container exec-sync")),
        ContainerCommand::Attach { .. } => Err(CliError::not_implemented("crs container attach")),
        ContainerCommand::Stats(_) => Err(CliError::not_implemented("crs container stats")),
        ContainerCommand::Checkpoint { .. } => {
            Err(CliError::not_implemented("crs container checkpoint"))
        }
        ContainerCommand::Update { .. } => Err(CliError::not_implemented("crs container update")),
        ContainerCommand::ReopenLog { .. } => {
            Err(CliError::not_implemented("crs container reopen-log"))
        }
        ContainerCommand::Logs(_) => Err(CliError::not_implemented("crs container logs")),
    }
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
    let (info_json, info_raw) = parse_info_map(&response.info, &mut warnings);
    let id = response
        .status
        .as_ref()
        .map(|status| status.id.clone())
        .unwrap_or_else(|| id.clone());
    let response_json = container_status_json(response.status.as_ref());

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
        created: container.created_at.to_string(),
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

fn container_status_json(status: Option<&ContainerStatus>) -> serde_json::Value {
    serde_json::json!({
        "status": status.map(|status| {
            let metadata = status.metadata.as_ref();
            let image = status.image.as_ref();
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
                "annotations": status.annotations,
                "logPath": status.log_path,
            })
        })
    })
}
