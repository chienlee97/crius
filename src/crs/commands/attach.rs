use crate::crs::{client::CrsClient, context::CliContext, error::CliError, error::CommandResult};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _id: String,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs attach"))
}
