use crate::crs::{
    args::{
        ContainerListArgs, ImageListArgs, ImagePullArgs, InspectArgs, ListArgs, ObjectType,
        PodListArgs, RemoveArgs, StopArgs, StopObjectType,
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
    ctx: &CliContext,
    client: &CrsClient,
    args: ImagePullArgs,
) -> Result<CommandResult, CliError> {
    image::handle_pull(ctx, client, args, "crs pull").await
}

pub(crate) async fn handle_inspect(
    _ctx: &CliContext,
    _client: &CrsClient,
    _args: InspectArgs,
) -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs inspect"))
}

pub(crate) async fn handle_stop(
    ctx: &CliContext,
    client: &CrsClient,
    args: StopArgs,
) -> Result<CommandResult, CliError> {
    match args.object_type {
        Some(StopObjectType::Container) => {
            container::handle_stop(ctx, client, args.target, args.timeout).await
        }
        Some(StopObjectType::Pod) => pod::handle_stop(ctx, client, args.target, args.timeout).await,
        None => match resolve_stop_target(client, &args.target).await? {
            StopCandidate::Container => {
                container::handle_stop(ctx, client, args.target, args.timeout).await
            }
            StopCandidate::Pod => pod::handle_stop(ctx, client, args.target, args.timeout).await,
        },
    }
}

pub(crate) async fn handle_rm(
    ctx: &CliContext,
    client: &CrsClient,
    args: RemoveArgs,
) -> Result<CommandResult, CliError> {
    match args.object_type {
        Some(ObjectType::Container) => container::handle_remove(ctx, client, args.target).await,
        Some(ObjectType::Pod) => pod::handle_remove(ctx, client, args.target).await,
        Some(ObjectType::Image) => image::handle_remove(ctx, client, args.target).await,
        None => match resolve_remove_target(client, &args.target).await? {
            RemoveCandidate::Container => container::handle_remove(ctx, client, args.target).await,
            RemoveCandidate::Pod => pod::handle_remove(ctx, client, args.target).await,
            RemoveCandidate::Image => image::handle_remove(ctx, client, args.target).await,
        },
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum StopCandidate {
    Container,
    Pod,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum RemoveCandidate {
    Container,
    Pod,
    Image,
}

async fn resolve_stop_target(client: &CrsClient, target: &str) -> Result<StopCandidate, CliError> {
    let mut candidates = Vec::new();
    if container_exists(client, target, "crs stop").await? {
        candidates.push(StopCandidate::Container);
    }
    if pod_exists(client, target, "crs stop").await? {
        candidates.push(StopCandidate::Pod);
    }

    match candidates.as_slice() {
        [candidate] => Ok(*candidate),
        [] => Err(CliError::invalid_input(format!(
            "target {target} did not match a container or pod; use --type container|pod with a valid ID"
        ))
        .with_command("crs stop")
        .with_object(target.to_string())),
        [_, ..] => Err(CliError::invalid_input(format!(
            "target {target} is ambiguous; specify --type container|pod"
        ))
        .with_command("crs stop")
        .with_object(target.to_string())),
    }
}

async fn resolve_remove_target(
    client: &CrsClient,
    target: &str,
) -> Result<RemoveCandidate, CliError> {
    let mut candidates = Vec::new();
    if container_exists(client, target, "crs rm").await? {
        candidates.push(RemoveCandidate::Container);
    }
    if pod_exists(client, target, "crs rm").await? {
        candidates.push(RemoveCandidate::Pod);
    }
    if image_exists(client, target, "crs rm").await? {
        candidates.push(RemoveCandidate::Image);
    }

    match candidates.as_slice() {
        [candidate] => Ok(*candidate),
        [] => Err(CliError::invalid_input(format!(
            "target {target} did not match a container, pod, or image; use --type container|pod|image with a valid ID or image reference"
        ))
        .with_command("crs rm")
        .with_object(target.to_string())),
        [_, ..] => Err(CliError::invalid_input(format!(
            "target {target} is ambiguous; specify --type container|pod|image"
        ))
        .with_command("crs rm")
        .with_object(target.to_string())),
    }
}

async fn container_exists(
    client: &CrsClient,
    target: &str,
    command_name: &'static str,
) -> Result<bool, CliError> {
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            match runtime
                .container_status(crate::proto::runtime::v1::ContainerStatusRequest {
                    container_id: target.to_string(),
                    verbose: false,
                })
                .await
            {
                Ok(response) => Ok(response.into_inner().status.is_some()),
                Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
                Err(status) => Err(candidate_error(
                    status,
                    client,
                    command_name,
                    "container",
                    target,
                )),
            }
        })
        .await
}

async fn pod_exists(
    client: &CrsClient,
    target: &str,
    command_name: &'static str,
) -> Result<bool, CliError> {
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            match runtime
                .pod_sandbox_status(crate::proto::runtime::v1::PodSandboxStatusRequest {
                    pod_sandbox_id: target.to_string(),
                    verbose: false,
                })
                .await
            {
                Ok(response) => Ok(response
                    .into_inner()
                    .status
                    .as_ref()
                    .map(|status| !pod::is_internal_sandbox_status(status))
                    .unwrap_or(false)),
                Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
                Err(status) => Err(candidate_error(status, client, command_name, "pod", target)),
            }
        })
        .await
}

async fn image_exists(
    client: &CrsClient,
    target: &str,
    command_name: &'static str,
) -> Result<bool, CliError> {
    let mut image_client = client.image()?;
    client
        .with_rpc_timeout(async {
            match image_client
                .image_status(crate::proto::runtime::v1::ImageStatusRequest {
                    image: Some(crate::proto::runtime::v1::ImageSpec {
                        image: target.to_string(),
                        ..Default::default()
                    }),
                    verbose: false,
                })
                .await
            {
                Ok(response) => Ok(response.into_inner().image.is_some()),
                Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
                Err(status) => Err(candidate_error(
                    status,
                    client,
                    command_name,
                    "image",
                    target,
                )),
            }
        })
        .await
}

fn candidate_error(
    status: tonic::Status,
    client: &CrsClient,
    command_name: &'static str,
    object_type: &'static str,
    target: &str,
) -> CliError {
    CliError::from_tonic_status(status)
        .with_command(command_name)
        .with_endpoint(client.endpoint())
        .with_object(format!("{object_type} {target}"))
}
