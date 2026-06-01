use crate::crs::{
    args::RuntimeArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: RuntimeArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs runtime"))
}
