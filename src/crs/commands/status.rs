use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::crs::{
    args::StatusArgs,
    client::CrsClient,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{
        render_output, CommandOutput, ConditionView, FormatOptions, RuntimeStatusView, TableRow,
    },
};
use crate::proto::runtime::v1::StatusRequest;

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: StatusArgs,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .status(StatusRequest {
                    verbose: args.verbose,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs status")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let mut warnings = Vec::new();
    let (info_json, info_raw) = parse_info_map(&response.info, &mut warnings);
    let conditions = response
        .status
        .map(|status| status.conditions)
        .unwrap_or_default()
        .into_iter()
        .map(|condition| ConditionView {
            kind: condition.r#type,
            status: condition.status,
            reason: condition.reason,
            message: condition.message,
        })
        .collect::<Vec<_>>();

    let view = RuntimeStatusView {
        runtime_ready: condition_status(&conditions, "RuntimeReady"),
        network_ready: condition_status(&conditions, "NetworkReady"),
        conditions,
        info_json,
        info_raw,
    };

    render_and_print(
        ctx,
        CommandOutput::new("RuntimeStatus", client.endpoint(), vec![view]).with_warnings(warnings),
    )
}

pub(crate) fn parse_info_map(
    info: &HashMap<String, String>,
    warnings: &mut Vec<String>,
) -> (Value, Value) {
    let mut parsed = Map::new();
    let mut raw = Map::new();

    for (key, value) in info {
        raw.insert(key.clone(), Value::String(value.clone()));
        match serde_json::from_str::<Value>(value) {
            Ok(json) => {
                parsed.insert(key.clone(), json);
            }
            Err(source) => warnings.push(format!(
                "failed to parse verbose info field {key:?} as JSON: {source}"
            )),
        }
    }

    (Value::Object(parsed), Value::Object(raw))
}

pub(crate) fn render_and_print<T>(
    ctx: &CliContext,
    output: CommandOutput<T>,
) -> Result<CommandResult, CliError>
where
    T: serde::Serialize + TableRow,
{
    let rendered = render_output(&output, FormatOptions::from_context(ctx)).map_err(|source| {
        CliError::internal(format!("failed to render command output: {source}"))
    })?;

    if !rendered.stdout.is_empty() {
        println!("{}", rendered.stdout);
    }
    if !rendered.stderr.is_empty() {
        eprintln!("{}", rendered.stderr);
    }

    Ok(CommandResult::success())
}

fn condition_status(conditions: &[ConditionView], kind: &str) -> bool {
    conditions
        .iter()
        .find(|condition| condition.kind == kind)
        .map(|condition| condition.status)
        .unwrap_or(false)
}
