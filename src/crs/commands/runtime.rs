use crate::crs::{
    args::{RuntimeArgs, RuntimeCommand},
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, RuntimeConfigUpdateView, RuntimeConfigView},
    parsers::parse_cidr_list,
};
use crate::proto::runtime::v1::{
    CgroupDriver, NetworkConfig, RuntimeConfig, RuntimeConfigRequest, UpdateRuntimeConfigRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: RuntimeArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        RuntimeCommand::Config => handle_config(ctx, client).await,
        RuntimeCommand::Update { pod_cidr } => handle_update(ctx, client, pod_cidr).await,
        RuntimeCommand::Handlers { .. } => Err(CliError::not_implemented("crs runtime handlers")),
    }
}

async fn handle_config(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .runtime_config(RuntimeConfigRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs runtime config")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let cgroup_driver = response
        .linux
        .and_then(|linux| CgroupDriver::try_from(linux.cgroup_driver).ok())
        .map(cgroup_driver_name)
        .unwrap_or("unknown")
        .to_string();

    render_and_print(
        ctx,
        CommandOutput::new(
            "RuntimeConfig",
            client.endpoint(),
            vec![RuntimeConfigView { cgroup_driver }],
        ),
    )
}

async fn handle_update(
    ctx: &CliContext,
    client: &CrsClient,
    pod_cidr: String,
) -> Result<CommandResult, CliError> {
    let pod_cidrs = parse_cidr_list(&pod_cidr)
        .map_err(CliError::invalid_input)?
        .into_iter()
        .collect::<Vec<_>>();
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .update_runtime_config(UpdateRuntimeConfigRequest {
                    runtime_config: Some(RuntimeConfig {
                        network_config: Some(NetworkConfig {
                            pod_cidr: pod_cidrs.join(","),
                        }),
                    }),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs runtime update")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?;

    render_and_print(
        ctx,
        CommandOutput::new(
            "RuntimeConfigUpdate",
            client.endpoint(),
            vec![RuntimeConfigUpdateView {
                pod_cidrs: pod_cidrs.clone(),
                updated: true,
            }],
        )
        .with_summary(serde_json::json!({
            "podCidrs": pod_cidrs,
            "updated": true,
        })),
    )
}

fn cgroup_driver_name(driver: CgroupDriver) -> &'static str {
    match driver {
        CgroupDriver::Systemd => "systemd",
        CgroupDriver::Cgroupfs => "cgroupfs",
    }
}
