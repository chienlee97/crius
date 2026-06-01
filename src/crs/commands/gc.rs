use crate::crs::{
    args::GcArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: GcArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs gc"))
}
