use crate::crs::{
    args::VersionArgs,
    client::CrsClient,
    context::CliContext,
    error::CliError,
    error::CommandResult,
    format::{print_envelope, print_table, CommandOutput, RuntimeVersionView},
};
use crate::proto::runtime::v1::VersionRequest;

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    _args: VersionArgs,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .version(VersionRequest {
                    version: String::new(),
                })
                .await
                .map_err(CliError::from_tonic_status)
        })
        .await?
        .into_inner();

    let view = RuntimeVersionView {
        runtime_name: response.runtime_name,
        runtime_version: response.runtime_version,
        runtime_api_version: response.runtime_api_version,
    };
    let output = CommandOutput::new("RuntimeVersion", client.endpoint(), vec![view]);

    match ctx.output() {
        crate::crs::args::OutputArg::Json => {
            println!(
                "{}",
                print_envelope(&output).map_err(|source| CliError::internal(format!(
                    "failed to render JSON: {source}"
                )))?
            );
        }
        crate::crs::args::OutputArg::Table | crate::crs::args::OutputArg::Text => {
            println!("{}", print_table(&output.items, ctx.no_trunc()));
        }
    }

    Ok(CommandResult::success())
}
