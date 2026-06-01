pub mod args;
pub mod error;

pub(crate) mod client;
pub(crate) mod commands;
pub(crate) mod context;
pub(crate) mod format;
pub(crate) mod ids;
pub(crate) mod parsers;
pub(crate) mod streaming;

use std::ffi::OsString;

use clap::{error::ErrorKind, Parser};

use crate::crs::{
    args::Args,
    client::CrsClient,
    context::CliContext,
    error::{CommandResult, ExitStatus},
};

pub async fn run_cli<I, T>(args: I) -> CommandResult
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args = match Args::try_parse_from(args) {
        Ok(args) => args,
        Err(error) if matches!(error.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) => {
            let _ = error.print();
            return CommandResult::success();
        }
        Err(error) => {
            let _ = error.print();
            return CommandResult::failure(ExitStatus::Usage);
        }
    };

    let ctx = match CliContext::from_args(&args) {
        Ok(ctx) => ctx,
        Err(error) => {
            eprintln!("error: {error}");
            return CommandResult::failure(ExitStatus::Usage);
        }
    };
    let client = CrsClient::new(ctx.endpoint_display());

    match commands::dispatch(&ctx, &client, args.command).await {
        Ok(result) => result,
        Err(error) => {
            error.render(ctx.output());
            CommandResult::failure(error.exit_status())
        }
    }
}
