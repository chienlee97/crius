use crate::crs::{
    args::ContainerCommand, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _command: ContainerCommand,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs container"))
}
