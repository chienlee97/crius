use crate::crs::{
    args::StreamOptions, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult, streaming,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    id: String,
    stream: StreamOptions,
) -> Result<CommandResult, CliError> {
    let options = streaming::AttachStreamOptions::from_args(id, stream)?;
    streaming::attach(options).await
}
