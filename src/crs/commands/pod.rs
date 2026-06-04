use crate::crs::{
    args::PodCommand, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _command: PodCommand,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs pod"))
}
