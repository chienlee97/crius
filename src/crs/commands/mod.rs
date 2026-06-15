pub(crate) mod attach;
pub(crate) mod completion;
pub(crate) mod config;
pub(crate) mod container;
pub(crate) mod debug;
pub(crate) mod doctor;
pub(crate) mod events;
pub(crate) mod exec;
pub(crate) mod gc;
pub(crate) mod image;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod pod;
pub(crate) mod port_forward;
pub(crate) mod recovery;
pub(crate) mod run;
pub(crate) mod runtime;
pub(crate) mod shortcuts;
pub(crate) mod stats;
pub(crate) mod status;
pub(crate) mod version;

use crate::crs::{
    args::{Command, ContainerCommand, PodCommand},
    client::CrsClient,
    context::CliContext,
    error::{CliError, CommandResult},
};

pub(crate) async fn dispatch(
    ctx: &CliContext,
    client: &CrsClient,
    command: Command,
) -> Result<CommandResult, CliError> {
    match command {
        Command::Version(args) => version::handle(ctx, client, args).await,
        Command::Status(args) => status::handle(ctx, client, args).await,
        Command::Doctor(args) => doctor::handle(ctx, client, args).await,
        Command::Ps(args) => shortcuts::handle_ps(ctx, client, args).await,
        Command::Pods(args) => shortcuts::handle_pods(ctx, client, args).await,
        Command::Images(args) => shortcuts::handle_images(ctx, client, args).await,
        Command::Pull(args) => shortcuts::handle_pull(ctx, client, args).await,
        Command::Inspect(args) => shortcuts::handle_inspect(ctx, client, args).await,
        Command::Logs(args) => logs::handle(ctx, client, args).await,
        Command::Exec(args) => exec::handle(ctx, client, args).await,
        Command::Stop(args) => shortcuts::handle_stop(ctx, client, args).await,
        Command::Rm(args) => shortcuts::handle_rm(ctx, client, args).await,
        Command::Rmi { image: image_name } => {
            image::handle_remove_with_command(ctx, client, image_name, "crs rmi").await
        }
        Command::Rmp { pod: pod_name } => {
            pod::handle_remove_with_command(ctx, client, pod_name, "crs rmp").await
        }
        Command::Config(args) => config::handle(ctx, client, args).await,
        Command::Runtime(args) => runtime::handle(ctx, client, args).await,
        Command::Image(args) => image::handle(ctx, client, args).await,
        Command::Pod(args) => match args.command {
            PodCommand::PortForward { pod, forward } => {
                port_forward::handle(ctx, client, pod, forward).await
            }
            command => pod::handle(ctx, client, command).await,
        },
        Command::Container(args) => match args.command {
            ContainerCommand::Attach { id, stream } => {
                attach::handle(ctx, client, id, stream).await
            }
            ContainerCommand::Exec(args) => exec::handle(ctx, client, args).await,
            ContainerCommand::Logs(args) => logs::handle(ctx, client, args).await,
            command => container::handle(ctx, client, command).await,
        },
        Command::Run(args) => run::handle(ctx, client, *args).await,
        Command::Events(args) => events::handle(ctx, client, args).await,
        Command::Stats(args) => stats::handle(ctx, client, args).await,
        Command::Metrics(args) => metrics::handle(ctx, client, args).await,
        Command::Recovery(args) => recovery::handle(ctx, client, args).await,
        Command::Gc(args) => gc::handle(ctx, client, args).await,
        Command::Debug(args) => debug::handle(ctx, client, args).await,
        Command::Completion(args) => completion::handle(ctx, client, args).await,
    }
}
