use crate::crs::{
    args::{RuntimeArgs, RuntimeCommand},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, RuntimeConfigUpdateView, RuntimeConfigView, RuntimeHandlerView},
    parsers::parse_cidr_list,
};
use crate::proto::diagnostics::v1::RuntimeHandlersRequest;
use crate::proto::runtime::v1::{
    CgroupDriver, NetworkConfig, RuntimeConfig, RuntimeConfigRequest, StatusRequest,
    UpdateRuntimeConfigRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: RuntimeArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        RuntimeCommand::Config => handle_config(ctx, client).await,
        RuntimeCommand::Update { pod_cidr } => handle_update(ctx, client, pod_cidr).await,
        RuntimeCommand::Handlers { verbose } => handle_handlers(ctx, client, verbose).await,
    }
}

async fn handle_config(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .runtime_config(RuntimeConfigRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs runtime config")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let cgroup_driver = response
        .linux
        .and_then(|linux| CgroupDriver::try_from(linux.cgroup_driver).ok())
        .map(cgroup_driver_name)
        .unwrap_or("unknown")
        .to_string();

    render_and_print(
        ctx,
        CommandOutput::new(
            "RuntimeConfig",
            client.endpoint(),
            vec![RuntimeConfigView { cgroup_driver }],
        ),
    )
}

async fn handle_update(
    ctx: &CliContext,
    client: &CrsClient,
    pod_cidr: String,
) -> Result<CommandResult, CliError> {
    let pod_cidrs = parse_cidr_list(&pod_cidr)
        .map_err(CliError::invalid_input)?
        .into_iter()
        .collect::<Vec<_>>();
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .update_runtime_config(UpdateRuntimeConfigRequest {
                    runtime_config: Some(RuntimeConfig {
                        network_config: Some(NetworkConfig {
                            pod_cidr: pod_cidrs.join(","),
                        }),
                    }),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs runtime update")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?;

    render_and_print(
        ctx,
        CommandOutput::new(
            "RuntimeConfigUpdate",
            client.endpoint(),
            vec![RuntimeConfigUpdateView {
                pod_cidrs: pod_cidrs.clone(),
                updated: true,
            }],
        )
        .with_summary(serde_json::json!({
            "podCidrs": pod_cidrs,
            "updated": true,
        })),
    )
}

async fn handle_handlers(
    ctx: &CliContext,
    client: &CrsClient,
    verbose: bool,
) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let views = if let Ok(mut diagnostics) = client.diagnostics() {
        match client
            .with_rpc_timeout(async {
                diagnostics
                    .runtime_handlers(RuntimeHandlersRequest {})
                    .await
                    .map_err(|status| {
                        CliError::from_diagnostics_status(status, client.endpoint())
                            .with_command("crs runtime handlers")
                    })
            })
            .await
        {
            Ok(response) => response
                .into_inner()
                .handlers
                .into_iter()
                .map(|handler| RuntimeHandlerView {
                    name: handler.name,
                    runtime_type: handler.runtime_type,
                    runtime_path: handler.runtime_path,
                    runtime_config_path: if verbose {
                        handler.runtime_config_path
                    } else {
                        String::new()
                    },
                    features: if verbose {
                        handler.features
                    } else {
                        Vec::new()
                    },
                    warnings: handler.warnings,
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                warnings.push(format!(
                    "failed to read diagnostics runtime handlers: {error}"
                ));
                load_handlers_from_status(client, &mut warnings).await?
            }
        }
    } else {
        warnings.push(client.diagnostics_unavailable().to_string());
        load_handlers_from_status(client, &mut warnings).await?
    };

    render_and_print(
        ctx,
        CommandOutput::new("RuntimeHandlers", client.endpoint(), views.clone())
            .with_summary(serde_json::json!({
                "count": views.len(),
                "verbose": verbose,
            }))
            .with_warnings(warnings),
    )
}

async fn load_handlers_from_status(
    client: &CrsClient,
    warnings: &mut Vec<String>,
) -> Result<Vec<RuntimeHandlerView>, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .status(StatusRequest { verbose: true })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs runtime handlers")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();
    let (info_json, _) = parse_info_map(&response.info, warnings);
    let Some(runtime_backend) = info_json.get("runtimeBackend") else {
        warnings.push("verbose status info did not include runtimeBackend".to_string());
        return Ok(Vec::new());
    };

    Ok(runtime_backend_views(runtime_backend))
}

pub(crate) async fn load_handlers_from_status_for_debug(
    client: &CrsClient,
    warnings: &mut Vec<String>,
) -> Result<Vec<RuntimeHandlerView>, CliError> {
    load_handlers_from_status(client, warnings).await
}

fn runtime_backend_views(value: &serde_json::Value) -> Vec<RuntimeHandlerView> {
    if let Some(handlers) = value
        .get("handlers")
        .and_then(|handlers| handlers.as_object())
    {
        return handlers
            .iter()
            .map(|(name, handler)| RuntimeHandlerView {
                name: name.clone(),
                runtime_type: string_field(handler, &["runtimeType", "type", "runtime"])
                    .unwrap_or_default(),
                runtime_path: string_field(handler, &["runtimePath", "path"]).unwrap_or_default(),
                runtime_config_path: string_field(handler, &["runtimeConfigPath", "configPath"])
                    .unwrap_or_default(),
                features: string_array_field(handler, "features"),
                warnings: string_array_field(handler, "warnings"),
            })
            .collect();
    }

    let name = string_field(value, &["defaultHandler", "handler", "name"])
        .unwrap_or_else(|| "default".to_string());
    vec![RuntimeHandlerView {
        name,
        runtime_type: string_field(value, &["runtimeType", "type", "runtime"]).unwrap_or_default(),
        runtime_path: string_field(value, &["runtimePath", "path"]).unwrap_or_default(),
        runtime_config_path: string_field(value, &["runtimeConfigPath", "configPath"])
            .unwrap_or_default(),
        features: string_array_field(value, "features"),
        warnings: string_array_field(value, "warnings"),
    }]
}

fn string_field(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(crate::crs::commands::config::value_to_display)
}

fn string_array_field(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(crate::crs::commands::config::value_to_display)
                .collect()
        })
        .unwrap_or_default()
}

fn cgroup_driver_name(driver: CgroupDriver) -> &'static str {
    match driver {
        CgroupDriver::Systemd => "systemd",
        CgroupDriver::Cgroupfs => "cgroupfs",
    }
}
