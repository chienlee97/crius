use crate::crs::{client::CrsClient, context::CliContext, error::CliError, error::CommandResult};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _pod: String,
    _forward: Vec<String>,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs pod port-forward"))
}
