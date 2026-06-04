use crate::crs::{
    args::StatsArgs, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: StatsArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs stats"))
}
