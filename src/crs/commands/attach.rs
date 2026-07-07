use crate::crs::{
    args::StreamOptions, client::CrsClient, context::CliContext, error::CliError,
    error::CommandResult, streaming,
};
use crate::proto::runtime::v1::AttachRequest;

pub(crate) async fn handle(
    _ctx: &CliContext,
    client: &CrsClient,
    id: String,
    stream: StreamOptions,
) -> Result<CommandResult, CliError> {
    let mut options = streaming::AttachStreamOptions::from_args(id, stream)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .attach(AttachRequest {
                    container_id: options.container_id.clone(),
                    stdin: options.stdin,
                    stdout: options.stdout,
                    stderr: options.stderr,
                    tty: options.tty,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs container attach")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {}", options.container_id))
                })
        })
        .await?
        .into_inner();
    options.stream_url = Some(response.url);
    streaming::attach(options).await
}
