use crate::crs::{
    args::{ImageArgs, ImageCommand, ImageListArgs},
    builders::build_auth_config,
    client::CrsClient,
    commands::config::load_effective_config,
    commands::status::{parse_info_map, render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{
        CommandOutput, FilesystemUsageView, ImageConfigView, ImageOperationView, ImageTransferView,
        ImageView, InspectView,
    },
};
use crate::proto::diagnostics::v1::ImageTransfersRequest;
use crate::proto::runtime::v1::{
    FilesystemUsage, Image, ImageFilter, ImageFsInfoRequest, ImageSpec, ImageStatusRequest,
    ListImagesRequest, PodSandboxStatusRequest, PullImageRequest, RemoveImageRequest,
    StatusRequest,
};

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: ImageArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        ImageCommand::List(args) => handle_list(ctx, client, args).await,
        ImageCommand::Pull(args) => handle_pull(ctx, client, args, "crs image pull").await,
        ImageCommand::Inspect { image } => handle_inspect(ctx, client, image).await,
        ImageCommand::Remove { image } => handle_remove(ctx, client, image).await,
        ImageCommand::FsInfo => handle_fs_info(ctx, client).await,
        ImageCommand::Transfers => handle_transfers(ctx, client).await,
        ImageCommand::Config => handle_config(ctx, client).await,
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

pub(crate) async fn handle_pull(
    ctx: &CliContext,
    client: &CrsClient,
    args: crate::crs::args::ImagePullArgs,
    command_name: &'static str,
) -> Result<CommandResult, CliError> {
    if args.image.is_empty() {
        return Err(CliError::invalid_input("image must not be empty").with_command(command_name));
    }

    let auth = build_auth_config(&args.auth)
        .map_err(CliError::invalid_input)?
        .filter(|auth| !auth_is_empty(auth));
    let sandbox_config = if let Some(pod) = args.pod.as_deref() {
        Some(fetch_sandbox_config(client, pod, command_name).await?)
    } else {
        None
    };

    let mut image_client = client.image()?;
    let request = PullImageRequest {
        image: Some(ImageSpec {
            image: args.image.clone(),
            user_specified_image: args.image.clone(),
            ..Default::default()
        }),
        auth,
        sandbox_config,
    };
    let response = client
        .with_rpc_timeout(async {
            image_client.pull_image(request).await.map_err(|status| {
                CliError::from_tonic_status(status)
                    .with_command(command_name)
                    .with_endpoint(client.endpoint())
                    .with_object(format!("image {}", args.image))
            })
        })
        .await?
        .into_inner();

    let view = ImageOperationView {
        image: args.image.clone(),
        image_ref: response.image_ref,
        action: "pulled".to_string(),
        success: true,
    };
    render_and_print(
        ctx,
        CommandOutput::new("ImagePull", client.endpoint(), vec![view]).with_summary(
            serde_json::json!({
                "image": args.image,
                "pulled": true,
            }),
        ),
    )
}

pub(crate) async fn handle_remove(
    ctx: &CliContext,
    client: &CrsClient,
    image: String,
) -> Result<CommandResult, CliError> {
    handle_remove_with_command(ctx, client, image, "crs image remove").await
}

pub(crate) async fn handle_remove_with_command(
    ctx: &CliContext,
    client: &CrsClient,
    image: String,
    command_name: &'static str,
) -> Result<CommandResult, CliError> {
    if image.is_empty() {
        return Err(CliError::invalid_input("image must not be empty").with_command(command_name));
    }

    let mut image_client = client.image()?;
    let exists = client
        .with_rpc_timeout(async {
            image_client
                .image_status(ImageStatusRequest {
                    image: Some(ImageSpec {
                        image: image.clone(),
                        user_specified_image: image.clone(),
                        ..Default::default()
                    }),
                    verbose: false,
                })
                .await
                .map(|response| response.into_inner().image.is_some())
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command(command_name)
                        .with_endpoint(client.endpoint())
                        .with_object(format!("image {image}"))
                })
        })
        .await?;
    if !exists {
        return Err(
            CliError::from_tonic_status(tonic::Status::not_found(format!(
                "image {image} not found"
            )))
            .with_command(command_name)
            .with_endpoint(client.endpoint())
            .with_object(format!("image {image}")),
        );
    }

    client
        .with_rpc_timeout(async {
            image_client
                .remove_image(RemoveImageRequest {
                    image: Some(ImageSpec {
                        image: image.clone(),
                        user_specified_image: image.clone(),
                        ..Default::default()
                    }),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command(command_name)
                        .with_endpoint(client.endpoint())
                        .with_object(format!("image {image}"))
                })
        })
        .await?;

    let view = ImageOperationView {
        image: image.clone(),
        image_ref: String::new(),
        action: "removed".to_string(),
        success: true,
    };
    render_and_print(
        ctx,
        CommandOutput::new("ImageRemove", client.endpoint(), vec![view]).with_summary(
            serde_json::json!({
                "image": image,
                "removed": true,
            }),
        ),
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

async fn handle_transfers(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let views = if let Ok(mut diagnostics) = client.diagnostics() {
        match client
            .with_rpc_timeout(async {
                diagnostics
                    .image_transfers(ImageTransfersRequest {
                        include_completed: false,
                    })
                    .await
                    .map_err(|status| {
                        CliError::from_diagnostics_status(status, client.endpoint())
                            .with_command("crs image transfers")
                    })
            })
            .await
        {
            Ok(response) => response
                .into_inner()
                .transfers
                .into_iter()
                .filter(|transfer| transfer.status != "succeeded")
                .map(|transfer| ImageTransferView {
                    image: transfer.image,
                    status: transfer.status,
                    updated: crate::crs::format::format_unix_nanos(
                        transfer.updated_at_unix_nanos,
                        std::time::SystemTime::now(),
                    ),
                    error: transfer.error,
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                warnings.push(format!(
                    "failed to read diagnostics image transfers: {error}"
                ));
                load_transfers_from_status(client, &mut warnings).await?
            }
        }
    } else {
        warnings.push(client.diagnostics_unavailable().to_string());
        load_transfers_from_status(client, &mut warnings).await?
    };

    render_and_print(
        ctx,
        CommandOutput::new("ImageTransfers", client.endpoint(), views.clone())
            .with_summary(serde_json::json!({ "count": views.len() }))
            .with_warnings(warnings),
    )
}

async fn handle_config(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, _) = load_effective_config(client, "crs image config", &mut warnings)
        .await
        .ok_or_else(|| {
            CliError::diagnostics_unavailable(client.endpoint()).with_command("crs image config")
        })?;
    let image_config = extract_image_config(&config);
    let view = ImageConfigView {
        snapshotter: string_field(&image_config, &["snapshotter", "defaultSnapshotter"])
            .or_else(|| {
                config
                    .pointer("/imageSnapshotModel/snapshotter")
                    .and_then(crate::crs::commands::config::value_to_display)
            })
            .unwrap_or_else(|| "unknown".to_string()),
        policy: string_field(
            &image_config,
            &["signaturePolicy", "signaturePolicyDir", "policy"],
        )
        .unwrap_or_else(|| "unknown".to_string()),
        auth_configured: auth_summary(&image_config),
        pinned_images: string_array(&image_config, "pinnedImages").join(","),
        config: image_config,
    };

    render_and_print(
        ctx,
        CommandOutput::new("ImageConfig", client.endpoint(), vec![view.clone()])
            .with_summary(serde_json::json!({
                "snapshotter": view.snapshotter,
                "authConfigured": view.auth_configured,
                "pinnedImages": view.pinned_images,
            }))
            .with_warnings(warnings),
    )
}

async fn load_transfers_from_status(
    client: &CrsClient,
    warnings: &mut Vec<String>,
) -> Result<Vec<ImageTransferView>, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .status(StatusRequest { verbose: true })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs image transfers")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();
    let (info_json, _) = parse_info_map(&response.info, warnings);
    let Some(transfers) = info_json.get("imageTransfers") else {
        warnings.push("verbose status info did not include imageTransfers".to_string());
        return Ok(Vec::new());
    };

    let items = transfers
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| string_field(item, &["status"]).as_deref() != Some("succeeded"))
        .map(|item| ImageTransferView {
            image: string_field(item, &["image", "ref", "source"]).unwrap_or_default(),
            status: string_field(item, &["status"]).unwrap_or_default(),
            updated: string_field(item, &["updated", "updatedAt", "updatedAtUnixNanos"])
                .unwrap_or_default(),
            error: string_field(item, &["error"]).unwrap_or_default(),
        })
        .collect();
    Ok(items)
}

fn extract_image_config(config: &serde_json::Value) -> serde_json::Value {
    config
        .get("image")
        .or_else(|| config.get("imageConfig"))
        .cloned()
        .unwrap_or_else(|| config.clone())
}

fn string_field(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(crate::crs::commands::config::value_to_display)
}

fn string_array(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(crate::crs::commands::config::value_to_display)
                .collect()
        })
        .unwrap_or_default()
}

fn auth_summary(config: &serde_json::Value) -> String {
    let auth_keys = [
        "auth",
        "registryAuth",
        "registryConfigDir",
        "globalAuthFile",
        "namespacedAuthDir",
    ];
    if auth_keys.iter().any(|key| config.get(*key).is_some()) {
        "configured".to_string()
    } else {
        "unknown".to_string()
    }
}

async fn fetch_sandbox_config(
    client: &CrsClient,
    pod: &str,
    command_name: &'static str,
) -> Result<crate::proto::runtime::v1::PodSandboxConfig, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .pod_sandbox_status(PodSandboxStatusRequest {
                    pod_sandbox_id: pod.to_string(),
                    verbose: false,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command(command_name)
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod}"))
                })
        })
        .await?
        .into_inner();

    response
        .status
        .and_then(|status| status.metadata)
        .map(|metadata| crate::proto::runtime::v1::PodSandboxConfig {
            metadata: Some(metadata),
            ..Default::default()
        })
        .ok_or_else(|| {
            CliError::invalid_input(format!(
                "daemon did not return sandbox metadata for pod {pod}"
            ))
            .with_command(command_name)
            .with_object(format!("pod {pod}"))
        })
}

fn auth_is_empty(auth: &crate::proto::runtime::v1::AuthConfig) -> bool {
    auth.username.is_empty()
        && auth.password.is_empty()
        && auth.auth.is_empty()
        && auth.server_address.is_empty()
        && auth.identity_token.is_empty()
        && auth.registry_token.is_empty()
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
