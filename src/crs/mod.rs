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
    args::{Args, Command},
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
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
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
    let client = if matches!(args.command, Command::Completion(_)) {
        CrsClient::new(&ctx)
    } else {
        match CrsClient::connect(&ctx).await {
            Ok(client) => client,
            Err(error) => {
                error.render(ctx.output());
                return CommandResult::failure(error.exit_status());
            }
        }
    };

    match commands::dispatch(&ctx, &client, args.command).await {
        Ok(result) => result,
        Err(error) => {
            error.render(ctx.output());
            CommandResult::failure(error.exit_status())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_cli_maps_help_and_version_to_success() {
        assert_eq!(run_cli(["crs", "--help"]).await.code(), 0);
        assert_eq!(run_cli(["crs", "--version"]).await.code(), 0);
        assert_eq!(run_cli(["crs", "version", "--help"]).await.code(), 0);
    }

    #[tokio::test]
    async fn run_cli_maps_clap_errors_to_usage_exit_code() {
        assert_eq!(run_cli(["crs"]).await.code(), 2);
        assert_eq!(
            run_cli(["crs", "--output", "xml", "version"]).await.code(),
            2
        );
        assert_eq!(
            run_cli([
                "crs",
                "image",
                "pull",
                "--auth-json",
                "{}",
                "--username",
                "user",
                "busybox"
            ])
            .await
            .code(),
            2
        );
    }

    #[tokio::test]
    async fn run_cli_maps_context_validation_to_usage_exit_code() {
        assert_eq!(
            run_cli(["crs", "--address", "ftp://127.0.0.1:1234", "version"])
                .await
                .code(),
            2
        );
    }
}
