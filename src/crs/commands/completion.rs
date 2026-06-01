use crate::crs::{
    args::CompletionArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: CompletionArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs completion"))
}
