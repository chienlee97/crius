use crate::crs::{
    client::CrsClient, context::CliContext, error::CliError, error::CommandResult, streaming,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    pod: String,
    forward: Vec<String>,
) -> Result<CommandResult, CliError> {
    let options = streaming::PortForwardOptions::from_args(pod, forward)?;
    streaming::port_forward(options).await
}
