use crate::crs::{
    args::RecoveryArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: RecoveryArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs recovery"))
}
