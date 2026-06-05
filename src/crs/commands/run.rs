use uuid::Uuid;

use crate::crs::{
    args::{
        ContainerCreateArgs, ContainerCreateOptions, ExecModeArg, PullPolicyArg, RunArgs,
        StreamOptions,
    },
    builders::{build_container_config, build_pod_sandbox_config},
    client::CrsClient,
    commands::status::render_and_print,
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, ContainerOperationView},
    streaming,
};
use crate::proto::runtime::v1::{
    AttachRequest, CreateContainerRequest, ExecSyncRequest, ImageSpec, ImageStatusRequest,
    PodSandboxConfig, PodSandboxStatusRequest, PullImageRequest, RunPodSandboxRequest,
    StartContainerRequest,
};

#[derive(Debug)]
struct RunPlan {
    image: String,
    command: Vec<String>,
    pull: PullPolicyArg,
    detach: bool,
    rm: bool,
    exec_mode: ExecModeArg,
    stream: StreamOptions,
    pod: PodPlan,
    container_options: ContainerCreateOptions,
}

#[derive(Debug)]
enum PodPlan {
    Existing(String),
    Create(Box<crate::crs::args::PodCreateArgs>),
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RunView {
    container_id: String,
    pod_id: String,
    image: String,
    action: String,
    detached: bool,
    pod_created: bool,
}

impl crate::crs::format::TableRow for RunView {
    fn headers() -> &'static [&'static str] {
        &["CONTAINER ID", "POD ID", "IMAGE", "ACTION", "DETACHED"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.container_id.clone(),
            self.pod_id.clone(),
            self.image.clone(),
            self.action.clone(),
            crate::crs::format::format_bool(self.detached).to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.container_id.clone()
    }
}

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: RunArgs,
) -> Result<CommandResult, CliError> {
    let plan = RunPlan::from_args(args)?;
    ensure_image(client, &plan).await?;
    let (pod_id, sandbox_config, pod_created) = prepare_pod(client, &plan).await?;
    let container_id = create_container(client, &plan, &pod_id, sandbox_config).await?;
    start_container(client, &container_id).await?;

    if !plan.detach {
        return run_foreground(ctx, client, &plan, &container_id).await;
    }
    if plan.rm {
        return Err(CliError::not_implemented("crs run --rm"));
    }

    render_and_print(
        ctx,
        CommandOutput::new(
            "Run",
            client.endpoint(),
            vec![RunView {
                container_id: container_id.clone(),
                pod_id: pod_id.clone(),
                image: plan.image.clone(),
                action: "started".to_string(),
                detached: true,
                pod_created,
            }],
        )
        .with_summary(serde_json::json!({
            "containerId": container_id,
            "podSandboxId": pod_id,
            "image": plan.image,
            "detached": true,
            "podCreated": pod_created,
        })),
    )
}

impl RunPlan {
    fn from_args(args: RunArgs) -> Result<Self, CliError> {
        if args.image.trim().is_empty() {
            return Err(CliError::invalid_input("image must not be empty").with_command("crs run"));
        }

        let pod = match args.pod {
            Some(pod) if pod.trim().is_empty() => {
                return Err(
                    CliError::invalid_input("pod id must not be empty").with_command("crs run")
                );
            }
            Some(pod) => PodPlan::Existing(pod),
            None => PodPlan::Create(Box::new(run_pod_args(&args))),
        };

        Ok(Self {
            image: args.image,
            command: args.command,
            pull: args.pull,
            detach: args.detach,
            rm: args.rm,
            exec_mode: args.exec_mode,
            stream: StreamOptions {
                stdin: args.stdin,
                tty: args.tty,
                stdout: true,
                stderr: true,
                resize: None,
                protocol: args.protocol,
            },
            pod,
            container_options: run_container_options(args.container_options, args.name),
        })
    }
}

fn run_pod_args(args: &RunArgs) -> crate::crs::args::PodCreateArgs {
    let options = &args.pod_options;
    crate::crs::args::PodCreateArgs {
        name: Some(
            options
                .pod_name
                .clone()
                .unwrap_or_else(|| format!("crius-{}", Uuid::new_v4().to_simple())),
        ),
        uid: Some(
            options
                .uid
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_simple().to_string()),
        ),
        namespace: Some(
            args.namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
        ),
        attempt: options.pod_attempt,
        hostname: options.hostname.clone(),
        log_dir: options.log_dir.clone(),
        dns_servers: options.dns_servers.clone(),
        dns_searches: options.dns_searches.clone(),
        dns_options: options.dns_options.clone(),
        publish: options.publish.clone(),
        labels: options.labels.clone(),
        annotations: options.annotations.clone(),
        runtime_handler: options.runtime_handler.clone(),
        cgroup_parent: options.cgroup_parent.clone(),
        sysctls: options.sysctls.clone(),
        host_network: options.host_network,
        host_pid: options.host_pid,
        host_ipc: options.host_ipc,
        userns: options.userns.clone(),
        uid_maps: options.uid_maps.clone(),
        gid_maps: options.gid_maps.clone(),
        security: options.security.clone(),
        overhead: options.overhead.clone(),
        pod_resources: options.pod_resources.clone(),
    }
}

fn run_container_options(
    options: crate::crs::args::RunContainerCreateOptions,
    name: Option<String>,
) -> ContainerCreateOptions {
    ContainerCreateOptions {
        name,
        attempt: options.container_attempt,
        commands: options.commands,
        args: options.args,
        workdir: options.workdir,
        env: options.env,
        env_files: options.env_files,
        labels: options.labels,
        annotations: options.annotations,
        mounts: options.mounts,
        devices: options.devices,
        cdi_devices: options.cdi_devices,
        log_path: options.log_path,
        stdin: false,
        tty: false,
        resources: options.resources,
        security: options.security,
    }
}

async fn ensure_image(client: &CrsClient, plan: &RunPlan) -> Result<(), CliError> {
    match plan.pull {
        PullPolicyArg::Always => pull_image(client, &plan.image).await,
        PullPolicyArg::Never => {
            image_status(client, &plan.image).await?;
            Ok(())
        }
        PullPolicyArg::Missing => match image_status(client, &plan.image).await {
            Ok(()) => Ok(()),
            Err(error)
                if matches!(error.exit_status(), crate::crs::error::ExitStatus::NotFound) =>
            {
                pull_image(client, &plan.image).await
            }
            Err(error) => Err(error),
        },
    }
}

async fn image_status(client: &CrsClient, image: &str) -> Result<(), CliError> {
    let mut image_client = client.image()?;
    client
        .with_rpc_timeout(async {
            image_client
                .image_status(ImageStatusRequest {
                    image: Some(ImageSpec {
                        image: image.to_string(),
                        user_specified_image: image.to_string(),
                        ..Default::default()
                    }),
                    verbose: false,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("image {image}"))
                })
        })
        .await?;
    Ok(())
}

async fn pull_image(client: &CrsClient, image: &str) -> Result<(), CliError> {
    let mut image_client = client.image()?;
    client
        .with_rpc_timeout(async {
            image_client
                .pull_image(PullImageRequest {
                    image: Some(ImageSpec {
                        image: image.to_string(),
                        user_specified_image: image.to_string(),
                        ..Default::default()
                    }),
                    auth: None,
                    sandbox_config: None,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("image {image}"))
                })
        })
        .await?;
    Ok(())
}

async fn prepare_pod(
    client: &CrsClient,
    plan: &RunPlan,
) -> Result<(String, PodSandboxConfig, bool), CliError> {
    match &plan.pod {
        PodPlan::Existing(pod_id) => {
            let config = fetch_sandbox_config(client, pod_id).await?;
            Ok((pod_id.clone(), config, false))
        }
        PodPlan::Create(args) => {
            let sandbox_config = build_pod_sandbox_config(args).map_err(CliError::invalid_input)?;
            let mut runtime = client.runtime()?;
            let response = client
                .with_rpc_timeout(async {
                    runtime
                        .run_pod_sandbox(RunPodSandboxRequest {
                            config: Some(sandbox_config.clone()),
                            runtime_handler: args.runtime_handler.clone().unwrap_or_default(),
                        })
                        .await
                        .map_err(|status| {
                            CliError::from_tonic_status(status)
                                .with_command("crs run")
                                .with_endpoint(client.endpoint())
                        })
                })
                .await?
                .into_inner();
            Ok((response.pod_sandbox_id, sandbox_config, true))
        }
    }
}

async fn fetch_sandbox_config(
    client: &CrsClient,
    pod_id: &str,
) -> Result<PodSandboxConfig, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .pod_sandbox_status(PodSandboxStatusRequest {
                    pod_sandbox_id: pod_id.to_string(),
                    verbose: false,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod_id}"))
                })
        })
        .await?
        .into_inner();

    response
        .status
        .and_then(|status| status.metadata)
        .map(|metadata| PodSandboxConfig {
            metadata: Some(metadata),
            ..Default::default()
        })
        .ok_or_else(|| {
            CliError::invalid_input(format!(
                "daemon did not return sandbox metadata for pod {pod_id}"
            ))
            .with_command("crs run")
            .with_object(format!("pod {pod_id}"))
        })
}

async fn create_container(
    client: &CrsClient,
    plan: &RunPlan,
    pod_id: &str,
    sandbox_config: PodSandboxConfig,
) -> Result<String, CliError> {
    let args = ContainerCreateArgs {
        options: plan.container_options.clone(),
        pod: pod_id.to_string(),
        image: plan.image.clone(),
        command: plan.command.clone(),
    };
    let config = build_container_config(&args).map_err(CliError::invalid_input)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .create_container(CreateContainerRequest {
                    pod_sandbox_id: pod_id.to_string(),
                    config: Some(config),
                    sandbox_config: Some(sandbox_config),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod_id}"))
                })
        })
        .await?
        .into_inner();

    Ok(response.container_id)
}

async fn start_container(client: &CrsClient, container_id: &str) -> Result<(), CliError> {
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .start_container(StartContainerRequest {
                    container_id: container_id.to_string(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {container_id}"))
                })
        })
        .await?;
    Ok(())
}

async fn run_foreground(
    ctx: &CliContext,
    client: &CrsClient,
    plan: &RunPlan,
    container_id: &str,
) -> Result<CommandResult, CliError> {
    match plan.exec_mode {
        ExecModeArg::Sync => run_exec_sync(ctx, client, plan, container_id).await,
        ExecModeArg::Attach => run_attach(client, plan, container_id).await,
    }
}

async fn run_exec_sync(
    ctx: &CliContext,
    client: &CrsClient,
    plan: &RunPlan,
    container_id: &str,
) -> Result<CommandResult, CliError> {
    let command = foreground_command(plan)?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .exec_sync(ExecSyncRequest {
                    container_id: container_id.to_string(),
                    cmd: command,
                    timeout: 0,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {container_id}"))
                })
        })
        .await?
        .into_inner();

    if matches!(ctx.output(), crate::crs::args::OutputArg::Json) {
        let envelope = serde_json::json!({
            "kind": "RunExecSync",
            "apiVersion": crate::crs::format::API_VERSION,
            "endpoint": client.endpoint(),
            "summary": {
                "containerId": container_id,
                "image": plan.image,
                "exitCode": response.exit_code,
            },
            "stdout": String::from_utf8_lossy(&response.stdout),
            "stderr": String::from_utf8_lossy(&response.stderr),
            "warnings": [],
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|source| CliError::internal(
                format!("failed to render run JSON: {source}")
            ))?
        );
    } else {
        use std::io::Write;

        std::io::stdout()
            .write_all(&response.stdout)
            .map_err(|source| CliError::internal(format!("failed to write stdout: {source}")))?;
        std::io::stderr()
            .write_all(&response.stderr)
            .map_err(|source| CliError::internal(format!("failed to write stderr: {source}")))?;
    }

    Ok(CommandResult::from_code(response.exit_code))
}

fn foreground_command(plan: &RunPlan) -> Result<Vec<String>, CliError> {
    if !plan.command.is_empty() {
        return Ok(plan.command.clone());
    }
    if !plan.container_options.commands.is_empty() {
        let mut command = plan.container_options.commands.clone();
        command.extend(plan.container_options.args.clone());
        return Ok(command);
    }

    Err(CliError::invalid_input("run --exec-mode sync requires a command").with_command("crs run"))
}

async fn run_attach(
    client: &CrsClient,
    plan: &RunPlan,
    container_id: &str,
) -> Result<CommandResult, CliError> {
    let mut options =
        streaming::AttachStreamOptions::from_args(container_id.to_string(), plan.stream.clone())?;
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .attach(AttachRequest {
                    container_id: options.container_id.clone(),
                    stdin: options.stdin,
                    stdout: options.stdout,
                    stderr: options.stderr,
                    tty: options.tty,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {}", options.container_id))
                })
        })
        .await?
        .into_inner();

    options.stream_url = Some(response.url);
    streaming::attach(options).await
}

#[allow(dead_code)]
fn _container_operation_view_from_run(view: &RunView) -> ContainerOperationView {
    ContainerOperationView {
        container_id: view.container_id.clone(),
        pod_id: view.pod_id.clone(),
        image: view.image.clone(),
        action: view.action.clone(),
        success: true,
    }
}
