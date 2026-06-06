use crate::crs::{
    args::{GcArgs, GcCommand},
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult, ExitStatus},
    format::{CommandOutput, GcCandidateView},
};
use crate::proto::diagnostics::v1::{ContentGcRequest, ContentGcResponse};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: GcArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        GcCommand::Candidates => handle_candidates(ctx, client).await,
        GcCommand::Run(mode) => handle_run(ctx, client, mode.execute).await,
    }
}

async fn handle_candidates(
    ctx: &CliContext,
    client: &CrsClient,
) -> Result<CommandResult, CliError> {
    let response = content_gc(client, false, "crs gc candidates").await?;
    render_gc(ctx, client, "GcCandidates", response, true)
}

async fn handle_run(
    ctx: &CliContext,
    client: &CrsClient,
    execute: bool,
) -> Result<CommandResult, CliError> {
    let response = content_gc(client, execute, "crs gc run").await?;
    render_gc(ctx, client, "GcPlan", response, !execute)
}

async fn content_gc(
    client: &CrsClient,
    execute: bool,
    command_name: &'static str,
) -> Result<ContentGcResponse, CliError> {
    let mut diagnostics = client.diagnostics()?;
    Ok(client
        .with_rpc_timeout(async {
            diagnostics
                .content_gc(ContentGcRequest { execute })
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner())
}

fn render_gc(
    ctx: &CliContext,
    client: &CrsClient,
    kind: &'static str,
    response: ContentGcResponse,
    expected_dry_run: bool,
) -> Result<CommandResult, CliError> {
    let total_bytes = response
        .candidates
        .iter()
        .map(|candidate| candidate.size_bytes)
        .sum::<u64>();
    let failed_count = response
        .candidates
        .iter()
        .filter(|candidate| !candidate.error.is_empty())
        .count();
    let deleted_count = response
        .candidates
        .iter()
        .filter(|candidate| candidate.deleted)
        .count();
    let views = response
        .candidates
        .into_iter()
        .map(|candidate| GcCandidateView {
            object_type: candidate.object_type,
            object_id: candidate.object_id,
            path: candidate.path,
            reason: candidate.reason,
            size_bytes: candidate.size_bytes,
            deleted: candidate.deleted,
            error: candidate.error,
        })
        .collect::<Vec<_>>();
    let dry_run = response.dry_run || expected_dry_run;

    render_and_print(
        ctx,
        CommandOutput::new(kind, client.endpoint(), views.clone())
            .with_summary(serde_json::json!({
                "count": views.len(),
                "dryRun": dry_run,
                "totalBytes": total_bytes,
                "reclaimedBytes": response.reclaimed_bytes,
                "deleted": deleted_count,
                "failed": failed_count,
            }))
            .with_warnings(response.warnings),
    )?;

    if !dry_run && failed_count > 0 {
        Ok(CommandResult::failure(ExitStatus::General))
    } else {
        Ok(CommandResult::success())
    }
}
