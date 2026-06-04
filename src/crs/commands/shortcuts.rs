use crate::crs::{
    args::{ImageListArgs, ImagePullArgs, InspectArgs, ListArgs, RemoveArgs, StopArgs},
    client::CrsClient,
    context::CliContext,
    error::{CliError, CommandResult},
};

pub(crate) async fn handle_ps(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ListArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs ps"))
}

pub(crate) async fn handle_pods(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ListArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs pods"))
}

pub(crate) async fn handle_images(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: ImageListArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs images"))
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
