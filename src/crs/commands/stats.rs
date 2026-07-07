use crate::crs::{
    args::StatsArgs,
    client::CrsClient,
    commands::container,
    context::CliContext,
    error::{CliError, CommandResult},
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    _args: StatsArgs,
) -> Result<CommandResult, CliError> {
    container::handle_stats_list(ctx, client, None, Vec::new(), "ContainerStats").await
}
