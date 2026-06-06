use std::io::Write;

use futures::StreamExt;

use crate::crs::{
    args::{ContainerLogsArgs, OutputArg},
    client::CrsClient,
    context::CliContext,
    error::{CliError, CommandResult},
    parsers::parse_since,
};
use crate::proto::diagnostics::v1::{ContainerLogChunk, ContainerLogRequest};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: ContainerLogsArgs,
) -> Result<CommandResult, CliError> {
    if args.container.trim().is_empty() {
        return Err(
            CliError::invalid_input("container ID must not be empty").with_command("crs logs")
        );
    }

    let since_unix_nanos = args
        .since
        .as_deref()
        .map(parse_since)
        .transpose()
        .map_err(CliError::invalid_input)?;
    let request = ContainerLogRequest {
        container_id: args.container.clone(),
        follow: args.follow,
        tail_lines: args.tail.unwrap_or_default(),
        since_unix_nanos: since_unix_nanos.unwrap_or_default(),
        timestamps: args.timestamps,
    };

    let mut diagnostics = client.diagnostics()?;
    let mut stream = client
        .with_rpc_timeout(async {
            diagnostics.container_log(request).await.map_err(|status| {
                CliError::from_tonic_status(status)
                    .with_command("crs logs")
                    .with_endpoint(client.endpoint())
                    .with_object(format!("container {}", args.container))
            })
        })
        .await?
        .into_inner();

    match ctx.output() {
        OutputArg::Json => write_json_stream(&mut stream).await?,
        OutputArg::Table | OutputArg::Text => write_text_stream(&mut stream).await?,
    }

    Ok(CommandResult::success())
}

async fn write_text_stream(
    stream: &mut tonic::Streaming<ContainerLogChunk>,
) -> Result<(), CliError> {
    let mut stdout = std::io::stdout();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|status| CliError::from_tonic_status(status).with_command("crs logs"))?;
        stdout.write_all(&chunk.data).map_err(|source| {
            CliError::internal(format!("failed to write log output: {source}"))
        })?;
    }
    stdout
        .flush()
        .map_err(|source| CliError::internal(format!("failed to flush log output: {source}")))?;
    Ok(())
}

async fn write_json_stream(
    stream: &mut tonic::Streaming<ContainerLogChunk>,
) -> Result<(), CliError> {
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|status| CliError::from_tonic_status(status).with_command("crs logs"))?;
        let line = serde_json::json!({
            "data": String::from_utf8_lossy(&chunk.data),
            "stream": chunk.stream,
            "timestamp": chunk.timestamp_unix_nanos,
        });
        println!(
            "{}",
            serde_json::to_string(&line).map_err(|source| {
                CliError::internal(format!("failed to render log JSON: {source}"))
            })?
        );
    }
    Ok(())
}
