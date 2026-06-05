use crate::crs::{
    args::StreamOptions, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult, streaming::AttachStreamOptions,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    id: String,
    stream: StreamOptions,
) -> Result<CommandResult, CliError> {
    let _options = AttachStreamOptions::from_args(id, stream)?;
    Err(CliError::not_implemented("crs attach"))
}
