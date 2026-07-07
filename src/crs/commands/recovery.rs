use crate::crs::{
    args::{RecoveryArgs, RecoveryCommand},
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult, ExitStatus},
    format::{CommandOutput, RecoveryActionView, RecoveryStatusView},
};
use crate::proto::diagnostics::v1::{
    RecoveryCheckRequest, RecoveryCheckResponse, RecoveryStatusRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: RecoveryArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        RecoveryCommand::Status => handle_status(ctx, client).await,
        RecoveryCommand::Check => handle_check(ctx, client).await,
        RecoveryCommand::Repair(mode) => handle_repair(ctx, client, mode.execute).await,
    }
}

async fn handle_status(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut diagnostics = client.diagnostics()?;
    let response = client
        .with_rpc_timeout(async {
            diagnostics
                .recovery_status(RecoveryStatusRequest {})
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command("crs recovery status")
                })
        })
        .await?
        .into_inner();

    let ledger_summary = parse_json_or_raw(&response.ledger_summary_json, "ledgerSummary");
    let warnings = response.warnings.clone();
    let view = RecoveryStatusView {
        status: response.status.clone(),
        last_startup: response.last_startup.clone(),
        unhealthy_object_count: response.unhealthy_object_count,
        ledger_summary: ledger_summary.clone(),
        warnings: response.warnings,
    };

    render_and_print(
        ctx,
        CommandOutput::new("RecoveryStatus", client.endpoint(), vec![view])
            .with_summary(serde_json::json!({
                "status": response.status,
                "lastStartup": response.last_startup,
                "unhealthyObjectCount": response.unhealthy_object_count,
                "ledgerSummary": ledger_summary,
            }))
            .with_warnings(warnings),
    )
}

async fn handle_check(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let response = recovery_check(client, false, "crs recovery check").await?;
    render_recovery_actions(ctx, client, "RecoveryCheck", response, true)
}

async fn handle_repair(
    ctx: &CliContext,
    client: &CrsClient,
    execute: bool,
) -> Result<CommandResult, CliError> {
    let response = recovery_check(client, execute, "crs recovery repair").await?;
    render_recovery_actions(ctx, client, "RecoveryRepairPlan", response, !execute)
}

async fn recovery_check(
    client: &CrsClient,
    execute: bool,
    command_name: &'static str,
) -> Result<RecoveryCheckResponse, CliError> {
    let mut diagnostics = client.diagnostics()?;
    Ok(client
        .with_rpc_timeout(async {
            diagnostics
                .recovery_check(RecoveryCheckRequest { execute })
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner())
}

fn render_recovery_actions(
    ctx: &CliContext,
    client: &CrsClient,
    kind: &'static str,
    response: RecoveryCheckResponse,
    expected_dry_run: bool,
) -> Result<CommandResult, CliError> {
    let failed_count = response
        .actions
        .iter()
        .filter(|action| !action.error.is_empty())
        .count();
    let executed_count = response
        .actions
        .iter()
        .filter(|action| action.executed)
        .count();
    let views = response
        .actions
        .into_iter()
        .map(|action| RecoveryActionView {
            object_type: action.object_type,
            object_id: action.object_id,
            action: action.action,
            reason: action.reason,
            executed: action.executed,
            error: action.error,
        })
        .collect::<Vec<_>>();
    let dry_run = response.dry_run || expected_dry_run;

    render_and_print(
        ctx,
        CommandOutput::new(kind, client.endpoint(), views.clone())
            .with_summary(serde_json::json!({
                "count": views.len(),
                "dryRun": dry_run,
                "executed": executed_count,
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

fn parse_json_or_raw(value: &str, field: &str) -> serde_json::Value {
    if value.trim().is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_str(value).unwrap_or_else(|_| {
        serde_json::json!({
            field: value,
        })
    })
}
