use crate::crs::{
    args::{DebugArgs, DebugCommand},
    client::CrsClient,
    commands::{
        config::{load_effective_config, value_to_display},
        runtime,
        status::{parse_info_map, render_and_print},
    },
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, DebugView},
};
use crate::proto::{
    diagnostics::v1::{
        NriStatusRequest, SecurityStatusRequest, ServerInfoRequest, ShimStatusRequest,
    },
    runtime::v1::{RuntimeConfigRequest, StatusRequest, VersionRequest},
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: DebugArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        DebugCommand::Network => debug_network(ctx, client).await,
        DebugCommand::Runtime => debug_runtime(ctx, client).await,
        DebugCommand::Shims => debug_shims(ctx, client).await,
        DebugCommand::Nri => debug_nri(ctx, client).await,
        DebugCommand::Security => debug_security(ctx, client).await,
        DebugCommand::Cgroups => debug_cgroups(ctx, client).await,
        DebugCommand::Streaming => debug_streaming(ctx, client).await,
        DebugCommand::Metrics => debug_metrics(ctx, client).await,
        DebugCommand::Tracing => debug_tracing(ctx, client).await,
        DebugCommand::Rootless => debug_rootless(ctx, client).await,
    }
}

async fn debug_network(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (status_info, network_ready) =
        verbose_status(client, "crs debug network", &mut warnings).await?;
    let config = load_effective_config(client, "crs debug network", &mut warnings)
        .await
        .map(|(config, _)| config)
        .unwrap_or_default();
    let network_diagnostics = status_info
        .get("networkDiagnostics")
        .cloned()
        .unwrap_or_default();
    let local_network = network_diagnostics
        .pointer("/domains/local")
        .cloned()
        .unwrap_or_default();
    let cri_network = network_diagnostics
        .pointer("/domains/cri")
        .cloned()
        .unwrap_or_default();
    let cni_selection = network_diagnostics
        .get("lastCniLoadStatus")
        .cloned()
        .unwrap_or_default();
    add_cni_selection_warnings(&cni_selection, &mut warnings);
    let details = serde_json::json!({
        "networkReady": network_ready,
        "podCIDR": lookup_display(&config, &["/runtimeNetworkConfig/podCIDR", "/network/podCIDR", "/podCIDR"]),
        "cniTemplate": lookup_display(&config, &["/reload/current/cniConfTemplate", "/network/cniConfTemplate", "/cniConfTemplate"]),
        "cniWatcher": lookup_display(&config, &["/reload/lastCniWatchError", "/reload/cniWatchDirs"]),
        "localNetwork": local_network,
        "criNetwork": cri_network,
        "cniSelection": cni_selection,
        "statusInfo": network_diagnostics,
    });
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugNetwork",
            check: "network".to_string(),
            status: status_from_bool(network_ready).to_string(),
            message: "network diagnostics summary".to_string(),
            details,
            warnings,
        },
    )
}

fn add_cni_selection_warnings(cni_selection: &serde_json::Value, warnings: &mut Vec<String>) {
    let mut declared_plugins = cni_selection
        .get("declaredPlugins")
        .and_then(|plugins| plugins.as_array())
        .into_iter()
        .flatten()
        .filter_map(|plugin| plugin.as_str());

    if declared_plugins.any(|plugin| plugin.eq_ignore_ascii_case("calico")) {
        warnings.push(
            "selected CNI configuration uses Calico; Calico may depend on the Kubernetes API"
                .to_string(),
        );
    }
}

async fn debug_runtime(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let version = version_json(client, "crs debug runtime").await?;
    let config = runtime_config_json(client, "crs debug runtime").await?;
    let handlers = match runtime::load_handlers_from_status_for_debug(client, &mut warnings).await {
        Ok(handlers) => handlers,
        Err(error) => {
            warnings.push(format!("failed to load runtime handlers: {error}"));
            Vec::new()
        }
    };
    let details = serde_json::json!({
        "version": version,
        "runtimeConfig": config,
        "handlers": handlers,
    });
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugRuntime",
            check: "runtime".to_string(),
            status: "ok".to_string(),
            message: "runtime diagnostics summary".to_string(),
            details,
            warnings,
        },
    )
}

async fn debug_shims(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut diagnostics = client.diagnostics()?;
    let response = client
        .with_rpc_timeout(async {
            diagnostics
                .shim_status(ShimStatusRequest {
                    container_id: String::new(),
                })
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command("crs debug shims")
                })
        })
        .await?
        .into_inner();
    let count = response.shims.len();
    let shims = response
        .shims
        .into_iter()
        .map(|shim| {
            serde_json::json!({
                "containerId": shim.container_id,
                "pid": shim.pid,
                "taskSocket": shim.task_socket,
                "attachSocket": shim.attach_socket,
                "state": shim.state,
                "error": shim.error,
            })
        })
        .collect::<Vec<_>>();
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugShims",
            check: "shims".to_string(),
            status: "ok".to_string(),
            message: format!("shim count: {count}"),
            details: serde_json::json!({ "shims": shims, "count": count }),
            warnings: Vec::new(),
        },
    )
}

async fn debug_nri(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut diagnostics = client.diagnostics()?;
    let response = client
        .with_rpc_timeout(async {
            diagnostics
                .nri_status(NriStatusRequest {})
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command("crs debug nri")
                })
        })
        .await?
        .into_inner();
    let warnings = response.warnings.clone();
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugNri",
            check: "nri".to_string(),
            status: if response.enabled { "ok" } else { "disabled" }.to_string(),
            message: if response.enabled {
                "NRI is enabled"
            } else {
                "NRI is disabled"
            }
            .to_string(),
            details: serde_json::json!({
                "enabled": response.enabled,
                "cdiEnabled": response.cdi_enabled,
                "cdiSpecDirs": response.cdi_spec_dirs,
                "pluginPath": response.plugin_path,
                "pluginConfigPath": response.plugin_config_path,
                "blockioConfigPath": response.blockio_config_path,
                "blockioSupported": response.blockio_supported,
                "rdtSupported": response.rdt_supported,
            }),
            warnings,
        },
    )
}

async fn debug_security(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let response = security_status(client, "crs debug security").await?;
    let mut warnings = response.warnings.clone();
    let devices_policy = parse_json_or_raw(
        &response.devices_policy_json,
        "devicesPolicyRaw",
        &mut warnings,
    );
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugSecurity",
            check: "security".to_string(),
            status: "ok".to_string(),
            message: "security diagnostics summary".to_string(),
            details: serde_json::json!({
                "seccompAvailable": response.seccomp_available,
                "seccompNotifierSupported": response.seccomp_notifier_supported,
                "apparmorAvailable": response.apparmor_available,
                "selinuxEnabled": response.selinux_enabled,
                "rootlessEnabled": response.rootless_enabled,
                "devicesPolicy": devices_policy,
            }),
            warnings,
        },
    )
}

async fn debug_cgroups(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (status_info, _) = verbose_status(client, "crs debug cgroups", &mut warnings).await?;
    let runtime_config = runtime_config_json(client, "crs debug cgroups").await?;
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugCgroups",
            check: "cgroups".to_string(),
            status: "ok".to_string(),
            message: "cgroup diagnostics summary".to_string(),
            details: serde_json::json!({
                "runtimeConfig": runtime_config,
                "controllers": status_info.get("cgroups").cloned().unwrap_or_default(),
            }),
            warnings,
        },
    )
}

async fn debug_streaming(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, _) = required_config(client, "crs debug streaming", &mut warnings).await?;
    let server_info = server_info(client, "crs debug streaming").await?;
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugStreaming",
            check: "streaming".to_string(),
            status: "ok".to_string(),
            message: "streaming diagnostics summary".to_string(),
            details: serde_json::json!({
                "enabled": lookup_display(&config, &["/streaming/enabled"]).unwrap_or_else(|| "unknown".to_string()),
                "listener": lookup_display(&config, &["/streaming/address", "/streaming/listener"]),
                "tls": lookup_display(&config, &["/streaming/tls/enabled", "/streaming/tls"]),
                "tokenTTL": lookup_display(&config, &["/streaming/tokenTTL", "/streaming/tokenTtl"]),
                "portForwardTimeout": lookup_display(&config, &["/streaming/portForwardTimeout"]),
                "serverInfo": server_info,
            }),
            warnings,
        },
    )
}

async fn debug_metrics(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, _) = required_config(client, "crs debug metrics", &mut warnings).await?;
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugMetrics",
            check: "metrics".to_string(),
            status: "ok".to_string(),
            message: "metrics diagnostics summary".to_string(),
            details: serde_json::json!({
                "enabled": lookup_display(&config, &["/metrics/enabled"]).unwrap_or_else(|| "unknown".to_string()),
                "endpoint": lookup_display(&config, &["/metrics/endpoint", "/metrics/address"]),
                "tls": lookup_display(&config, &["/metrics/tls/enabled", "/metrics/tls"]),
                "collector": lookup_display(&config, &["/metrics/collector"]),
                "podMetrics": lookup_display(&config, &["/metrics/podMetrics"]),
            }),
            warnings,
        },
    )
}

async fn debug_tracing(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, _) = required_config(client, "crs debug tracing", &mut warnings).await?;
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugTracing",
            check: "tracing".to_string(),
            status: "ok".to_string(),
            message: "tracing diagnostics summary".to_string(),
            details: serde_json::json!({
                "enabled": lookup_display(&config, &["/tracing/enabled"]).unwrap_or_else(|| "unknown".to_string()),
                "exporter": lookup_display(&config, &["/tracing/exporter"]),
                "endpoint": lookup_display(&config, &["/tracing/endpoint"]),
                "samplingRate": lookup_display(&config, &["/tracing/samplingRate"]),
                "headers": lookup_display(&config, &["/tracing/headers"]),
            }),
            warnings,
        },
    )
}

async fn debug_rootless(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let response = security_status(client, "crs debug rootless").await?;
    render_debug(
        ctx,
        client,
        DebugRender {
            kind: "DebugRootless",
            check: "rootless".to_string(),
            status: if response.rootless_enabled {
                "ok"
            } else {
                "disabled"
            }
            .to_string(),
            message: if response.rootless_enabled {
                "rootless mode is enabled"
            } else {
                "rootless mode is disabled"
            }
            .to_string(),
            details: serde_json::json!({
                "enabled": response.rootless_enabled,
                "seccompAvailable": response.seccomp_available,
                "apparmorAvailable": response.apparmor_available,
            }),
            warnings: response.warnings,
        },
    )
}

struct DebugRender {
    kind: &'static str,
    check: String,
    status: String,
    message: String,
    details: serde_json::Value,
    warnings: Vec<String>,
}

fn render_debug(
    ctx: &CliContext,
    client: &CrsClient,
    debug: DebugRender,
) -> Result<CommandResult, CliError> {
    render_and_print(
        ctx,
        CommandOutput::new(
            debug.kind,
            client.endpoint(),
            vec![DebugView {
                check: debug.check,
                status: debug.status,
                message: debug.message,
                details: debug.details.clone(),
            }],
        )
        .with_summary(debug.details)
        .with_warnings(debug.warnings),
    )
}

async fn verbose_status(
    client: &CrsClient,
    command_name: &'static str,
    warnings: &mut Vec<String>,
) -> Result<(serde_json::Value, bool), CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .status(StatusRequest { verbose: true })
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner();
    let network_ready = response
        .status
        .as_ref()
        .map(|status| {
            status
                .conditions
                .iter()
                .find(|condition| condition.r#type == "NetworkReady")
                .map(|condition| condition.status)
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let (info_json, _) = parse_info_map(&response.info, warnings);
    Ok((info_json, network_ready))
}

async fn version_json(
    client: &CrsClient,
    command_name: &'static str,
) -> Result<serde_json::Value, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .version(VersionRequest {
                    version: String::new(),
                })
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner();
    Ok(serde_json::json!({
        "runtimeName": response.runtime_name,
        "runtimeVersion": response.runtime_version,
        "runtimeApiVersion": response.runtime_api_version,
    }))
}

async fn runtime_config_json(
    client: &CrsClient,
    command_name: &'static str,
) -> Result<serde_json::Value, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .runtime_config(RuntimeConfigRequest {})
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner();
    let cgroup_driver = response
        .linux
        .and_then(|linux| {
            crate::proto::runtime::v1::CgroupDriver::try_from(linux.cgroup_driver).ok()
        })
        .map(|driver| match driver {
            crate::proto::runtime::v1::CgroupDriver::Systemd => "systemd",
            crate::proto::runtime::v1::CgroupDriver::Cgroupfs => "cgroupfs",
        })
        .unwrap_or("unknown");
    Ok(serde_json::json!({
        "cgroupDriver": cgroup_driver,
    }))
}

async fn required_config(
    client: &CrsClient,
    command_name: &'static str,
    warnings: &mut Vec<String>,
) -> Result<(serde_json::Value, Vec<String>), CliError> {
    load_effective_config(client, command_name, warnings)
        .await
        .ok_or_else(|| {
            CliError::diagnostics_unavailable(client.endpoint()).with_command(command_name)
        })
}

async fn server_info(
    client: &CrsClient,
    command_name: &'static str,
) -> Result<serde_json::Value, CliError> {
    let mut diagnostics = client.diagnostics()?;
    let response = client
        .with_rpc_timeout(async {
            diagnostics
                .server_info(ServerInfoRequest {})
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner();
    Ok(serde_json::json!({
        "version": response.version,
        "gitCommit": response.git_commit,
        "configPath": response.config_path,
        "stateDir": response.state_dir,
        "socketPath": response.socket_path,
    }))
}

async fn security_status(
    client: &CrsClient,
    command_name: &'static str,
) -> Result<crate::proto::diagnostics::v1::SecurityStatusResponse, CliError> {
    let mut diagnostics = client.diagnostics()?;
    Ok(client
        .with_rpc_timeout(async {
            diagnostics
                .security_status(SecurityStatusRequest {})
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command(command_name)
                })
        })
        .await?
        .into_inner())
}

fn lookup_display(value: &serde_json::Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .filter_map(|pointer| value.pointer(pointer))
        .find_map(value_to_display)
}

fn status_from_bool(value: bool) -> &'static str {
    if value {
        "ok"
    } else {
        "warning"
    }
}

fn parse_json_or_raw(value: &str, raw_key: &str, warnings: &mut Vec<String>) -> serde_json::Value {
    if value.trim().is_empty() {
        return serde_json::json!({});
    }
    match serde_json::from_str(value) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "failed to parse {raw_key} as JSON: {error}; preserving raw value"
            ));
            serde_json::json!({ raw_key: value })
        }
    }
}
