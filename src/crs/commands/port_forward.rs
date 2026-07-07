use crate::crs::{
    client::CrsClient, context::CliContext, error::CliError, error::CommandResult, streaming,
};
use crate::proto::runtime::v1::PortForwardRequest;

pub(crate) async fn handle(
    _ctx: &CliContext,
    client: &CrsClient,
    pod: String,
    forward: Vec<String>,
) -> Result<CommandResult, CliError> {
    let mut options = streaming::PortForwardOptions::from_args(pod, forward)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .port_forward(PortForwardRequest {
                    pod_sandbox_id: options.pod_id.clone(),
                    port: options
                        .forwards
                        .iter()
                        .map(|forward| i32::from(forward.remote))
                        .collect(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs pod port-forward")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {}", options.pod_id))
                })
        })
        .await?
        .into_inner();
    options.stream_url = Some(response.url);
    streaming::port_forward(options).await
}
