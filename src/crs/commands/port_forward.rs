use crate::crs::{
    client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
    streaming::PortForwardOptions,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    pod: String,
    forward: Vec<String>,
) -> Result<CommandResult, CliError> {
    let _options = PortForwardOptions::from_args(pod, forward)?;
    Err(CliError::not_implemented("crs pod port-forward"))
}
