use crate::crs::{
    args::ContainerLogsArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ContainerLogsArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs logs"))
}
