use crate::crs::{
    args::ExecArgs, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
    streaming,
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    args: ExecArgs,
) -> Result<CommandResult, CliError> {
    let options =
        streaming::ExecStreamOptions::from_args(args.container, args.command, args.stream)?;
    streaming::exec(options).await
}
