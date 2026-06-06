use crate::crs::{
    args::{ConfigArgs, ConfigCommand},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, ConfigReloadStatusView, EffectiveConfigView},
};
use crate::proto::diagnostics::v1::EffectiveConfigRequest;
use crate::proto::runtime::v1::StatusRequest;

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: ConfigArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        ConfigCommand::Show => handle_show(ctx, client).await,
        ConfigCommand::ReloadStatus => handle_reload_status(ctx, client).await,
    }
}

async fn handle_show(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, redacted_fields) = load_effective_config(client, "crs config show", &mut warnings)
        .await
        .ok_or_else(|| {
            CliError::diagnostics_unavailable(client.endpoint()).with_command("crs config show")
        })?;

    render_and_print(
        ctx,
        CommandOutput::new(
            "EffectiveConfig",
            client.endpoint(),
            vec![EffectiveConfigView {
                config: config.clone(),
                redacted_fields: redacted_fields.clone(),
            }],
        )
        .with_summary(serde_json::json!({
            "redactedFieldCount": redacted_fields.len(),
            "source": config_source(&warnings),
        }))
        .with_warnings(warnings),
    )
}

async fn handle_reload_status(
    ctx: &CliContext,
    client: &CrsClient,
) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, _) = load_effective_config(client, "crs config reload-status", &mut warnings)
        .await
        .ok_or_else(|| {
            CliError::diagnostics_unavailable(client.endpoint())
                .with_command("crs config reload-status")
        })?;
    let view = reload_status_view(&config, &mut warnings);

    render_and_print(
        ctx,
        CommandOutput::new("ConfigReloadStatus", client.endpoint(), vec![view.clone()])
            .with_summary(serde_json::json!({
                "watcher": view.watcher,
                "lastReload": view.last_reload,
                "lastError": view.last_error,
                "cniWatcher": view.cni_watcher,
            }))
            .with_warnings(warnings),
    )
}

pub(crate) async fn load_effective_config(
    client: &CrsClient,
    command_name: &'static str,
    warnings: &mut Vec<String>,
) -> Option<(serde_json::Value, Vec<String>)> {
    if let Ok(mut diagnostics) = client.diagnostics() {
        match client
            .with_rpc_timeout(async {
                diagnostics
                    .effective_config(EffectiveConfigRequest {
                        include_sensitive: false,
                    })
                    .await
                    .map_err(|status| {
                        CliError::from_diagnostics_status(status, client.endpoint())
                            .with_command(command_name)
                    })
            })
            .await
        {
            Ok(response) => {
                let response = response.into_inner();
                warnings.extend(response.warnings);
                return parse_config_json(
                    &response.config_json,
                    response.redacted_fields,
                    "diagnostics EffectiveConfig",
                    warnings,
                );
            }
            Err(error) => warnings.push(format!("failed to read diagnostics config: {error}")),
        }
    } else {
        warnings.push(client.diagnostics_unavailable().to_string());
    }

    load_config_from_verbose_status(client, command_name, warnings).await
}

async fn load_config_from_verbose_status(
    client: &CrsClient,
    command_name: &'static str,
    warnings: &mut Vec<String>,
) -> Option<(serde_json::Value, Vec<String>)> {
    let mut runtime = match client.runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            warnings.push(format!("failed to create runtime client: {error}"));
            return None;
        }
    };

    let response = match client
        .with_rpc_timeout(async {
            runtime
                .status(StatusRequest { verbose: true })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command(command_name)
                        .with_endpoint(client.endpoint())
                })
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(error) => {
            warnings.push(format!("failed to read verbose status config: {error}"));
            return None;
        }
    };

    let (info_json, _) = parse_info_map(&response.info, warnings);
    info_json
        .get("config")
        .cloned()
        .map(|config| (config, Vec::new()))
        .or_else(|| {
            warnings.push("verbose status info did not include config".to_string());
            None
        })
}

fn parse_config_json(
    config_json: &str,
    redacted_fields: Vec<String>,
    source: &str,
    warnings: &mut Vec<String>,
) -> Option<(serde_json::Value, Vec<String>)> {
    match serde_json::from_str::<serde_json::Value>(config_json) {
        Ok(config) => Some((config, redacted_fields)),
        Err(error) => {
            warnings.push(format!("failed to parse {source} JSON: {error}"));
            None
        }
    }
}

fn reload_status_view(
    config: &serde_json::Value,
    warnings: &mut Vec<String>,
) -> ConfigReloadStatusView {
    let reload = config.get("reload").unwrap_or(config);
    ConfigReloadStatusView {
        watcher: field_string(
            reload,
            &["watcherStatus", "status", "watcher", "configFileWatch"],
            "watcher",
            warnings,
        ),
        last_reload: field_string(
            reload,
            &["lastReloadAtUnixMillis", "lastReload", "lastReloadSource"],
            "last reload",
            warnings,
        ),
        last_error: field_string(
            reload,
            &["lastReloadError", "watcherLastError", "error"],
            "last error",
            warnings,
        ),
        cni_watcher: field_string(
            reload,
            &["lastCniWatchError", "cniWatcher", "cniWatchDirs"],
            "CNI watcher",
            warnings,
        ),
    }
}

pub(crate) fn field_string(
    value: &serde_json::Value,
    keys: &[&str],
    label: &str,
    warnings: &mut Vec<String>,
) -> String {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(value_to_display)
        .unwrap_or_else(|| {
            warnings.push(format!("{label} is missing from config; using unknown"));
            "unknown".to_string()
        })
}

pub(crate) fn value_to_display(value: &serde_json::Value) -> Option<String> {
    if value.is_null() {
        None
    } else if let Some(value) = value.as_str() {
        (!value.is_empty()).then(|| value.to_string())
    } else {
        Some(value.to_string())
    }
}

fn config_source(warnings: &[String]) -> &'static str {
    if warnings
        .iter()
        .any(|warning| warning.contains("diagnostics service is not available"))
    {
        "status"
    } else {
        "diagnostics"
    }
}
