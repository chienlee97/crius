use clap::CommandFactory;
use clap_complete::{generate, Shell};
use std::io::Write;

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
    let mut buffer = Vec::new();
    generate(shell, &mut command, bin_name, &mut buffer);
    let mut stdout = std::io::stdout();
    if let Err(source) = stdout.write_all(&buffer) {
        if source.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(CommandResult::success());
        }
        return Err(CliError::internal(format!(
            "failed to write completion output: {source}"
        )));
    }
    Ok(CommandResult::success())
}
