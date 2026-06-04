use crate::crs::{
    args::{PodCommand, PodListArgs, PodStateArg},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, InspectView, PodView},
    parsers::parse_key_value,
};
use crate::proto::runtime::v1::{
    ListPodSandboxRequest, PodSandbox, PodSandboxFilter, PodSandboxState, PodSandboxStateValue,
    PodSandboxStatus, PodSandboxStatusRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    command: PodCommand,
) -> Result<CommandResult, CliError> {
    match command {
        PodCommand::List(args) => handle_list(ctx, client, args).await,
        PodCommand::Inspect { pod } => handle_inspect(ctx, client, pod).await,
        PodCommand::Run(_) => Err(CliError::not_implemented("crs pod run")),
        PodCommand::Stop { .. } => Err(CliError::not_implemented("crs pod stop")),
        PodCommand::Remove { .. } => Err(CliError::not_implemented("crs pod remove")),
        PodCommand::Stats(_) => Err(CliError::not_implemented("crs pod stats")),
        PodCommand::Metrics => Err(CliError::not_implemented("crs pod metrics")),
        PodCommand::UpdateResources { .. } => {
            Err(CliError::not_implemented("crs pod update-resources"))
        }
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
