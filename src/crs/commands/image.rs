use crate::crs::{
    args::ImageArgs, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ImageArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs image"))
}
