use crate::crs::{
    args::ConfigArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ConfigArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs config"))
}
