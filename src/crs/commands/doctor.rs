use crate::crs::{
    args::DoctorArgs,
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, DoctorCheckView},
};
use crate::proto::runtime::v1::{
    CgroupDriver, RuntimeConfigRequest, StatusRequest, VersionRequest,
};

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
                .status(StatusRequest { verbose: false })
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

    let checks = vec![
        DoctorCheckView {
            check: "runtime".to_string(),
            status: if runtime_ready { "ok" } else { "not-ready" }.to_string(),
            message: format!(
                "{} {} ({})",
                version.runtime_name, version.runtime_version, version.runtime_api_version
            ),
        },
        DoctorCheckView {
            check: "network".to_string(),
            status: if network_ready { "ok" } else { "not-ready" }.to_string(),
            message: "CRI RuntimeStatus NetworkReady condition".to_string(),
        },
        DoctorCheckView {
            check: "runtime-config".to_string(),
            status: "ok".to_string(),
            message: format!("cgroup driver: {cgroup_driver}"),
        },
        DoctorCheckView {
            check: "diagnostics".to_string(),
            status: "warning".to_string(),
            message: "diagnostics service is not available from this CLI build".to_string(),
        },
    ];

    render_and_print(
        ctx,
        CommandOutput::new("Doctor", client.endpoint(), checks)
            .with_warnings(vec![client.diagnostics_unavailable().to_string()]),
    )
}
