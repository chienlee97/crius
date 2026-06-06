use clap::CommandFactory;
use clap_complete::{generate, Shell};

use crate::crs::{
    args::{Args, CompletionArgs},
    client::CrsClient,
    context::CliContext,
    error::{CliError, CommandResult},
};

pub(crate) async fn handle(
    _ctx: &CliContext,
    _client: &CrsClient,
    args: CompletionArgs,
) -> Result<CommandResult, CliError> {
    let mut command = Args::command();
    let bin_name = command.get_name().to_string();
    let shell: Shell = args.shell.into();
    generate(shell, &mut command, bin_name, &mut std::io::stdout());
    Ok(CommandResult::success())
}
