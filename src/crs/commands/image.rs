use crate::crs::{
    args::{ImageArgs, ImageCommand, ImageListArgs},
    client::CrsClient,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, FilesystemUsageView, ImageView, InspectView},
};
use crate::proto::runtime::v1::{
    FilesystemUsage, Image, ImageFilter, ImageFsInfoRequest, ImageSpec, ImageStatusRequest,
    ListImagesRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: ImageArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        ImageCommand::List(args) => handle_list(ctx, client, args).await,
        ImageCommand::Inspect { image } => handle_inspect(ctx, client, image).await,
        ImageCommand::FsInfo => handle_fs_info(ctx, client).await,
        ImageCommand::Pull(_) => Err(CliError::not_implemented("crs image pull")),
        ImageCommand::Remove { .. } => Err(CliError::not_implemented("crs image remove")),
        ImageCommand::Transfers => Err(CliError::not_implemented("crs image transfers")),
        ImageCommand::Config => Err(CliError::not_implemented("crs image config")),
    }
}

pub(crate) async fn handle_list(
    ctx: &CliContext,
    client: &CrsClient,
    args: ImageListArgs,
) -> Result<CommandResult, CliError> {
    let mut image_client = client.image()?;
    let response = client
        .with_rpc_timeout(async {
            image_client
                .list_images(ListImagesRequest {
                    filter: args.image.map(|image| ImageFilter {
                        image: Some(ImageSpec {
                            image,
                            ..Default::default()
                        }),
                    }),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs image list")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let views = response.images.into_iter().map(image_view).collect();
    render_and_print(
        ctx,
        CommandOutput::new("ImageList", client.endpoint(), views),
    )
}

pub(crate) async fn handle_inspect(
    ctx: &CliContext,
    client: &CrsClient,
    image: String,
) -> Result<CommandResult, CliError> {
    let mut image_client = client.image()?;
    let response = client
        .with_rpc_timeout(async {
            image_client
                .image_status(ImageStatusRequest {
                    image: Some(ImageSpec {
                        image: image.clone(),
                        ..Default::default()
                    }),
                    verbose: true,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs image inspect")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("image {image}"))
                })
        })
        .await?
        .into_inner();

    let mut warnings = Vec::new();
    let (info_json, info_raw) = parse_info_map(&response.info, &mut warnings);
    let id = response
        .image
        .as_ref()
        .map(|image| image.id.clone())
        .unwrap_or_else(|| image.clone());
    let response_json = image_status_json(response.image.as_ref());

    render_and_print(
        ctx,
        CommandOutput::new(
            "ImageInspect",
            client.endpoint(),
            vec![InspectView {
                object_type: "image".to_string(),
                id,
                response: response_json,
                info_json,
                info_raw,
            }],
        )
        .with_warnings(warnings),
    )
}

async fn handle_fs_info(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut image_client = client.image()?;
    let response = client
        .with_rpc_timeout(async {
            image_client
                .image_fs_info(ImageFsInfoRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs image fs-info")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();

    let image_filesystem_count = response.image_filesystems.len();
    let container_filesystem_count = response.container_filesystems.len();
    let total_used_bytes = response
        .image_filesystems
        .iter()
        .chain(response.container_filesystems.iter())
        .map(used_bytes)
        .sum::<u64>();

    let mut views = response
        .image_filesystems
        .into_iter()
        .map(|usage| filesystem_view("image", usage))
        .collect::<Vec<_>>();
    views.extend(
        response
            .container_filesystems
            .into_iter()
            .map(|usage| filesystem_view("container", usage)),
    );

    render_and_print(
        ctx,
        CommandOutput::new("ImageFsInfo", client.endpoint(), views).with_summary(
            serde_json::json!({
                "count": image_filesystem_count + container_filesystem_count,
                "imageFilesystemCount": image_filesystem_count,
                "containerFilesystemCount": container_filesystem_count,
                "totalUsedBytes": total_used_bytes,
            }),
        ),
    )
}

pub(crate) fn image_view(image: Image) -> ImageView {
    let image_name = image
        .repo_tags
        .first()
        .or_else(|| image.repo_digests.first())
        .cloned()
        .or_else(|| image.spec.as_ref().map(|spec| spec.image.clone()))
        .unwrap_or_default();
    let user_spec = image
        .spec
        .as_ref()
        .map(|spec| spec.user_specified_image.clone())
        .unwrap_or_default();

    ImageView {
        image: image_name,
        image_id: image.id,
        size_bytes: image.size,
        user_spec,
        pinned: image.pinned,
    }
}

fn filesystem_view(kind: &str, usage: FilesystemUsage) -> FilesystemUsageView {
    FilesystemUsageView {
        kind: kind.to_string(),
        mountpoint: usage.fs_id.map(|fs| fs.mountpoint).unwrap_or_default(),
        used_bytes: usage
            .used_bytes
            .as_ref()
            .map(|value| value.value)
            .unwrap_or(0),
        inodes_used: usage
            .inodes_used
            .as_ref()
            .map(|value| value.value)
            .unwrap_or(0),
        timestamp: usage.timestamp,
    }
}

fn used_bytes(usage: &FilesystemUsage) -> u64 {
    usage
        .used_bytes
        .as_ref()
        .map(|value| value.value)
        .unwrap_or(0)
}

fn image_status_json(image: Option<&Image>) -> serde_json::Value {
    serde_json::json!({
        "image": image.map(|image| serde_json::json!({
            "id": image.id,
            "repoTags": image.repo_tags,
            "repoDigests": image.repo_digests,
            "size": image.size,
            "username": image.username,
            "spec": image.spec.as_ref().map(|spec| serde_json::json!({
                "image": spec.image,
                "annotations": spec.annotations,
                "userSpecifiedImage": spec.user_specified_image,
                "runtimeHandler": spec.runtime_handler,
            })),
            "pinned": image.pinned,
        }))
    })
}
