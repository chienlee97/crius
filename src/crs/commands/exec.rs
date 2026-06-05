use crate::crs::{
    args::ExecArgs, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
    streaming::ExecStreamOptions,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    args: ExecArgs,
) -> Result<CommandResult, CliError> {
    let _options = ExecStreamOptions::from_args(args.container, args.command, args.stream)?;
    Err(CliError::not_implemented("crs exec"))
}
