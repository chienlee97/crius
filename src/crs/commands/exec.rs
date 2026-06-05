use crate::crs::{
    args::ExecArgs, client::CrsClient, context::CliContext, error::CliError, error::CommandResult,
    streaming,
};
use crate::proto::runtime::v1::ExecRequest;

pub(crate) async fn handle(
    _ctx: &CliContext,
    client: &CrsClient,
    args: ExecArgs,
) -> Result<CommandResult, CliError> {
    let mut options =
        streaming::ExecStreamOptions::from_args(args.container, args.command, args.stream)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .exec(ExecRequest {
                    container_id: options.container_id.clone(),
                    cmd: options.command.clone(),
                    stdin: options.stdin,
                    stdout: options.stdout,
                    stderr: options.stderr,
                    tty: options.tty,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs container exec")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {}", options.container_id))
                })
        })
        .await?
        .into_inner();
    options.stream_url = Some(response.url);
    streaming::exec(options).await
}
