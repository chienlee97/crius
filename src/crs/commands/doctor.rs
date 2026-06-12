use crate::crs::{
    args::DoctorArgs,
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, DoctorCheckView},
};
use crate::proto::diagnostics::v1::ServerInfoRequest;
use crate::proto::runtime::v1::{
    CgroupDriver, RuntimeConfigRequest, StatusRequest, VersionRequest,
};
use serde_json::Value;

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    _args: DoctorArgs,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let version = client
        .with_rpc_timeout(async {
            runtime
                .version(VersionRequest {
                    version: String::new(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs doctor")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let mut runtime = client.runtime()?;
    let status = client
        .with_rpc_timeout(async {
            runtime
                .status(StatusRequest { verbose: true })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs doctor")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let mut runtime = client.runtime()?;
    let config = client
        .with_rpc_timeout(async {
            runtime
                .runtime_config(RuntimeConfigRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs doctor")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let conditions = status
        .status
        .map(|status| status.conditions)
        .unwrap_or_default();
    let runtime_ready = conditions
        .iter()
        .find(|condition| condition.r#type == "RuntimeReady")
        .map(|condition| condition.status)
        .unwrap_or(false);
    let network_ready = conditions
        .iter()
        .find(|condition| condition.r#type == "NetworkReady")
        .map(|condition| condition.status)
        .unwrap_or(false);
    let cgroup_driver = config
        .linux
        .and_then(|linux| CgroupDriver::try_from(linux.cgroup_driver).ok())
        .map(|driver| match driver {
            CgroupDriver::Systemd => "systemd",
            CgroupDriver::Cgroupfs => "cgroupfs",
        })
        .unwrap_or("unknown");
    let diagnostics = probe_diagnostics(client).await;

    let network_diagnostics = network_diagnostics_from_status_info(&status.info);
    let local_network = network_diagnostics
        .as_ref()
        .and_then(|diagnostics| diagnostics.pointer("/domains/local/loadStatus"));
    let local_network_ready = local_network
        .and_then(|load_status| load_status.get("ready"))
        .and_then(Value::as_bool)
        .unwrap_or(network_ready);
    let local_network_message = local_network_check_message(local_network);

    let cri_network_message = conditions
        .iter()
        .find(|condition| condition.r#type == "NetworkReady")
        .map(|condition| {
            format!(
                "CRI RuntimeStatus NetworkReady: {}{}",
                condition.reason,
                if condition.message.is_empty() {
                    String::new()
                } else {
                    format!(" - {}", condition.message)
                }
            )
        })
        .unwrap_or_else(|| "CRI RuntimeStatus NetworkReady condition is missing".to_string());

    let mut checks = vec![
        DoctorCheckView {
            check: "runtime".to_string(),
            status: if runtime_ready { "ok" } else { "not-ready" }.to_string(),
            message: format!(
                "{} {} ({})",
                version.runtime_name, version.runtime_version, version.runtime_api_version
            ),
        },
        DoctorCheckView {
            check: "local-network".to_string(),
            status: if local_network_ready {
                "ok"
            } else {
                "not-ready"
            }
            .to_string(),
            message: local_network_message,
        },
        DoctorCheckView {
            check: "cri-network".to_string(),
            status: if network_ready { "ok" } else { "not-ready" }.to_string(),
            message: cri_network_message,
        },
        DoctorCheckView {
            check: "runtime-config".to_string(),
            status: "ok".to_string(),
            message: format!("cgroup driver: {cgroup_driver}"),
        },
    ];
    let mut warnings = Vec::new();
    match diagnostics {
        Ok(message) => checks.push(DoctorCheckView {
            check: "diagnostics".to_string(),
            status: "ok".to_string(),
            message,
        }),
        Err(error) => {
            warnings.push(error.to_string());
            checks.push(DoctorCheckView {
                check: "diagnostics".to_string(),
                status: "warning".to_string(),
                message: error.to_string(),
            });
        }
    }

    render_and_print(
        ctx,
        CommandOutput::new("Doctor", client.endpoint(), checks).with_warnings(warnings),
    )
}

fn network_diagnostics_from_status_info(
    info: &std::collections::HashMap<String, String>,
) -> Option<Value> {
    info.get("config")
        .and_then(|config| serde_json::from_str::<Value>(config).ok())
        .and_then(|config| config.get("networkDiagnostics").cloned())
}

fn local_network_check_message(load_status: Option<&Value>) -> String {
    let Some(load_status) = load_status else {
        return "local CNI diagnostics are unavailable".to_string();
    };
    let reason = load_status
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let message = load_status
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let plugins = load_status
        .get("declaredPlugins")
        .or_else(|| load_status.get("declared_plugins"))
        .and_then(Value::as_array)
        .map(|plugins| {
            plugins
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|plugins| !plugins.is_empty());

    match (message.is_empty(), plugins) {
        (true, Some(plugins)) => format!("{reason}; plugins: {plugins}"),
        (false, Some(plugins)) => format!("{reason}: {message}; plugins: {plugins}"),
        (true, None) => reason.to_string(),
        (false, None) => format!("{reason}: {message}"),
    }
}

async fn probe_diagnostics(client: &CrsClient) -> Result<String, CliError> {
    let mut diagnostics = client.diagnostics()?;
    let response = client
        .with_rpc_timeout(async {
            diagnostics
                .server_info(ServerInfoRequest {})
                .await
                .map_err(|status| {
                    CliError::from_diagnostics_status(status, client.endpoint())
                        .with_command("crs doctor")
                })
        })
        .await?
        .into_inner();
    Ok(format!(
        "{} {} ({})",
        response.version, response.git_commit, response.state_dir
    ))
}
