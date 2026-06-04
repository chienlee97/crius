use crate::crs::{
    args::{RuntimeArgs, RuntimeCommand},
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, RuntimeConfigView},
};
use crate::proto::runtime::v1::{CgroupDriver, RuntimeConfigRequest};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: RuntimeArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        RuntimeCommand::Config => handle_config(ctx, client).await,
        RuntimeCommand::Update { .. } => Err(CliError::not_implemented("crs runtime update")),
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

fn cgroup_driver_name(driver: CgroupDriver) -> &'static str {
    match driver {
        CgroupDriver::Systemd => "systemd",
        CgroupDriver::Cgroupfs => "cgroupfs",
    }
}
