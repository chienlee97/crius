use crate::crs::{
    args::VersionArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: VersionArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs version"))
}
