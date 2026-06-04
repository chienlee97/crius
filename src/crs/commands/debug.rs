use crate::crs::{
    args::DebugArgs, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: DebugArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs debug"))
}
