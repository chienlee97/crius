use std::time::{Duration, UNIX_EPOCH};

use crate::crs::{
    args::{EventsArgs, OutputArg},
    client::CrsClient,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{print_table, ContainerEventView, TableRow},
};
use crate::proto::runtime::v1::{
    ContainerEventResponse, ContainerEventType, GetEventsRequest, PodSandboxStatus,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    _args: EventsArgs,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .get_container_events(GetEventsRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs events")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?;
    let mut stream = response.into_inner();
    let mut printed_header = false;
    let deadline =
        (!ctx.rpc_timeout().is_zero()).then(|| tokio::time::Instant::now() + ctx.rpc_timeout());

    loop {
        let next_event = stream.message();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Err(CliError::interrupted().with_command("crs events"));
            }
            event = next_event => {
                let event = event.map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs events")
                        .with_endpoint(client.endpoint())
                })?;
                let Some(event) = event else {
                    return Ok(CommandResult::success());
                };
                let view = event_view(event);
                print_event(ctx, &view, &mut printed_header)?;
            }
            _ = sleep_until(deadline), if deadline.is_some() => {
                return Err(CliError::timeout(
                    format!("events timed out after {:?}", ctx.rpc_timeout()),
                    client.endpoint(),
                )
                .with_command("crs events"));
            }
        }
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    }
}

fn print_event(
    ctx: &CliContext,
    view: &ContainerEventView,
    printed_header: &mut bool,
) -> Result<(), CliError> {
    match ctx.output() {
        OutputArg::Json => {
            let line = serde_json::to_string(view).map_err(|source| {
                CliError::internal(format!("failed to render event JSON: {source}"))
            })?;
            println!("{line}");
        }
        OutputArg::Table | OutputArg::Text if ctx.quiet() => {
            println!(
                "{}",
                crate::crs::ids::truncate_field(&view.quiet_cell(), ctx.no_trunc())
            );
        }
        OutputArg::Table | OutputArg::Text => {
            if !*printed_header {
                println!(
                    "{}",
                    print_table(std::slice::from_ref(view), ctx.no_trunc())
                );
                *printed_header = true;
            } else {
                let cells = view
                    .cells()
                    .into_iter()
                    .map(|cell| crate::crs::ids::truncate_field(&cell, ctx.no_trunc()))
                    .collect::<Vec<_>>();
                println!("{}", cells.join("  "));
            }
        }
    }

    Ok(())
}

fn event_view(event: ContainerEventResponse) -> ContainerEventView {
    ContainerEventView {
        time: format_event_time(event.created_at),
        event_type: event_type_name(event.container_event_type).to_string(),
        container_id: event.container_id,
        pod_id: pod_id(event.pod_sandbox_status.as_ref()),
        created_at_unix_nanos: event.created_at,
    }
}

fn event_type_name(event_type: i32) -> &'static str {
    match ContainerEventType::try_from(event_type) {
        Ok(ContainerEventType::ContainerCreatedEvent) => "created",
        Ok(ContainerEventType::ContainerStartedEvent) => "started",
        Ok(ContainerEventType::ContainerStoppedEvent) => "stopped",
        Ok(ContainerEventType::ContainerDeletedEvent) => "deleted",
        Err(_) => "unknown",
    }
}

fn pod_id(status: Option<&PodSandboxStatus>) -> String {
    status.map(|status| status.id.clone()).unwrap_or_default()
}

fn format_event_time(unix_nanos: i64) -> String {
    let nanos = unix_nanos.max(0) as u64;
    chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_nanos(nanos))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
