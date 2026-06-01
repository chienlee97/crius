use crate::crs::{
    args::MetricsArgs, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: MetricsArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs metrics"))
}
