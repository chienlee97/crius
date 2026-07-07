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
    ctx: &CliContext,
    client: &CrsClient,
    args: InspectArgs,
) -> Result<CommandResult, CliError> {
    match args.object_type {
        Some(ObjectType::Container) => container::handle_inspect(ctx, client, args.target).await,
        Some(ObjectType::Pod) => pod::handle_inspect(ctx, client, args.target).await,
        Some(ObjectType::Image) => image::handle_inspect(ctx, client, args.target).await,
        None => match resolve_inspect_target(client, &args.target).await? {
            InspectCandidate::Container => {
                container::handle_inspect(ctx, client, args.target).await
            }
            InspectCandidate::Pod => pod::handle_inspect(ctx, client, args.target).await,
            InspectCandidate::Image => image::handle_inspect(ctx, client, args.target).await,
        },
    }
}

pub(crate) async fn handle_stop(
    ctx: &CliContext,
    client: &CrsClient,
    args: StopArgs,
) -> Result<CommandResult, CliError> {
    match args.object_type {
        Some(StopObjectType::Container) => {
            if !container_exists(client, &args.target, "crs stop").await? {
                return Err(not_found_error(
                    client,
                    "crs stop",
                    "container",
                    &args.target,
                ));
            }
            container::handle_stop(ctx, client, args.target, args.timeout).await
        }
        Some(StopObjectType::Pod) => {
            if !pod_exists(client, &args.target, "crs stop").await? {
                return Err(not_found_error(client, "crs stop", "pod", &args.target));
            }
            pod::handle_stop(ctx, client, args.target, args.timeout).await
        }
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
    let _force = args.force;
    let target = resolve_container_remove_target(client, &args.target).await?;
    container::handle_remove_with_command(ctx, client, target, "crs rm").await
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum StopCandidate {
    Container,
    Pod,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum InspectCandidate {
    Container,
    Pod,
    Image,
}

async fn resolve_container_remove_target(
    client: &CrsClient,
    target: &str,
) -> Result<String, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_containers(crate::proto::runtime::v1::ListContainersRequest { filter: None })
                .await
                .map_err(|status| candidate_error(status, client, "crs rm", "container", target))
        })
        .await?
        .into_inner();

    let mut matches = response
        .containers
        .into_iter()
        .filter(|container| {
            container.id == target
                || container.id.starts_with(target)
                || container
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.name == target)
                    .unwrap_or(false)
        })
        .map(|container| container.id)
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();

    match matches.as_slice() {
        [id] => Ok(id.clone()),
        [] => Err(not_found_error(client, "crs rm", "container", target)),
        [_, ..] => Err(CliError::invalid_input(format!(
            "container target {target} is ambiguous; retry with the full container ID"
        ))
        .with_command("crs rm")
        .with_object(target.to_string())),
    }
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
        [] => Err(not_found_error(
            client,
            "crs stop",
            "container or pod",
            target,
        )),
        [_, ..] => Err(CliError::invalid_input(format!(
            "target {target} is ambiguous; specify --type container|pod"
        ))
        .with_command("crs stop")
        .with_object(target.to_string())),
    }
}

async fn resolve_inspect_target(
    client: &CrsClient,
    target: &str,
) -> Result<InspectCandidate, CliError> {
    let mut candidates = Vec::new();
    if container_exists(client, target, "crs inspect").await? {
        candidates.push(InspectCandidate::Container);
    }
    if pod_exists(client, target, "crs inspect").await? {
        candidates.push(InspectCandidate::Pod);
    }
    if image_exists(client, target, "crs inspect").await? {
        candidates.push(InspectCandidate::Image);
    }

    match candidates.as_slice() {
        [candidate] => Ok(*candidate),
        [] => Err(CliError::invalid_input(format!(
            "target {target} did not match a container, pod, or image; use --type container|pod|image with a valid ID or image reference"
        ))
        .with_command("crs inspect")
        .with_object(target.to_string())),
        [_, ..] => Err(CliError::invalid_input(format!(
            "target {target} is ambiguous; specify --type container|pod|image"
        ))
        .with_command("crs inspect")
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
                Ok(response) => Ok(response.into_inner().status.is_some()),
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

fn not_found_error(
    client: &CrsClient,
    command_name: &'static str,
    object_type: &'static str,
    target: &str,
) -> CliError {
    CliError::from_tonic_status(tonic::Status::not_found(format!(
        "{object_type} {target} not found"
    )))
    .with_command(command_name)
    .with_endpoint(client.endpoint())
    .with_object(format!("{object_type} {target}"))
}
