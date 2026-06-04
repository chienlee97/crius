use crate::crs::{
    args::{
        ContainerListArgs, ImageListArgs, ImagePullArgs, InspectArgs, ListArgs, PodListArgs,
        RemoveArgs, StopArgs,
    },
    client::CrsClient,
    commands::{container, image, pod},
    context::CliContext,
    error::{CliError, CommandResult},
};

pub(crate) async fn handle_ps(
    ctx: &CliContext,
    client: &CrsClient,
    args: ListArgs,
) -> Result<CommandResult, CliError> {
    container::handle_list(
        ctx,
        client,
        ContainerListArgs {
            all: args.all,
            ..Default::default()
        },
    )
    .await
}

pub(crate) async fn handle_pods(
    ctx: &CliContext,
    client: &CrsClient,
    args: ListArgs,
) -> Result<CommandResult, CliError> {
    pod::handle_list(
        ctx,
        client,
        PodListArgs {
            all: args.all,
            ..Default::default()
        },
    )
    .await
}

pub(crate) async fn handle_images(
    ctx: &CliContext,
    client: &CrsClient,
    args: ImageListArgs,
) -> Result<CommandResult, CliError> {
    image::handle_list(ctx, client, args).await
}

pub(crate) async fn handle_pull(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ImagePullArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs pull"))
}

pub(crate) async fn handle_inspect(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: InspectArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs inspect"))
}

pub(crate) async fn handle_stop(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: StopArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs stop"))
}

pub(crate) async fn handle_rm(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: RemoveArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs rm"))
}
