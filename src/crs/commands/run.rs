use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::crs::{
    annotations::{INTERNAL_SANDBOX_ANNOTATION, LOCAL_NETWORK_DOMAIN, NETWORK_DOMAIN_ANNOTATION},
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
    AttachRequest, ContainerState, ContainerStatusRequest, CreateContainerRequest, ExecSyncRequest,
    ImageSpec, ImageStatusRequest, NamespaceMode, PodSandboxConfig, PodSandboxStatusRequest,
    PullImageRequest, RemoveContainerRequest, RemovePodSandboxRequest, RunPodSandboxRequest,
    StartContainerRequest, StopContainerRequest, StopPodSandboxRequest,
};

const STOPPED_STREAMING_STATUS_WAIT: Duration = Duration::from_secs(2);
const STOPPED_STREAMING_STATUS_POLL: Duration = Duration::from_millis(100);

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
    let container_id = match create_container(client, &plan, &pod_id, sandbox_config).await {
        Ok(container_id) => container_id,
        Err(error) => {
            emit_leftover_warnings(&leftover_warnings(None, Some((&pod_id, pod_created))));
            return Err(error);
        }
    };
    if let Err(error) = start_container(client, &container_id).await {
        emit_leftover_warnings(&leftover_warnings(
            Some(&container_id),
            Some((&pod_id, pod_created)),
        ));
        return Err(error);
    }

    let outcome = if plan.detach {
        render_detached(ctx, client, &plan, &pod_id, &container_id, pod_created)
    } else {
        match run_foreground(ctx, client, &plan, &container_id).await {
            Ok(result) => Ok(result),
            Err(error) => {
                render_stopped_before_streaming(ctx, client, &plan, &container_id, error).await
            }
        }
    };

    if plan.rm {
        let warnings = cleanup_run_resources(client, &container_id, &pod_id, pod_created).await;
        emit_cleanup_warnings(ctx, &warnings);
    }

    outcome
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
    let mut annotations = options.annotations.clone();
    annotations.push(format!("{INTERNAL_SANDBOX_ANNOTATION}=true"));
    annotations.push(format!(
        "{NETWORK_DOMAIN_ANNOTATION}={LOCAL_NETWORK_DOMAIN}"
    ));

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
        annotations,
        runtime_handler: options.runtime_handler.clone(),
        cgroup_parent: options.cgroup_parent.clone(),
        sysctls: options.sysctls.clone(),
        host_network: true,
        host_pid: true,
        host_ipc: true,
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
            let sandbox_config = internal_sandbox_config(
                build_pod_sandbox_config(args).map_err(CliError::invalid_input)?,
            );
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

fn internal_sandbox_config(mut config: PodSandboxConfig) -> PodSandboxConfig {
    config
        .annotations
        .insert(INTERNAL_SANDBOX_ANNOTATION.to_string(), "true".to_string());
    config.annotations.insert(
        NETWORK_DOMAIN_ANNOTATION.to_string(),
        LOCAL_NETWORK_DOMAIN.to_string(),
    );

    if let Some(linux) = config.linux.as_mut() {
        if let Some(security) = linux.security_context.as_mut() {
            let namespace_options = security
                .namespace_options
                .get_or_insert_with(Default::default);
            namespace_options.network = NamespaceMode::Node as i32;
            namespace_options.pid = NamespaceMode::Node as i32;
            namespace_options.ipc = NamespaceMode::Node as i32;
        }
    }

    config
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

async fn render_stopped_before_streaming(
    ctx: &CliContext,
    client: &CrsClient,
    plan: &RunPlan,
    container_id: &str,
    original_error: CliError,
) -> Result<CommandResult, CliError> {
    let Some(status) = wait_for_exited_status(client, container_id).await? else {
        return Err(original_error);
    };

    let warning = stopped_before_streaming_warning(plan.exec_mode);
    if matches!(ctx.output(), crate::crs::args::OutputArg::Json) {
        let envelope = serde_json::json!({
            "kind": "RunResult",
            "apiVersion": crate::crs::format::API_VERSION,
            "endpoint": client.endpoint(),
            "items": [],
            "summary": {
                "containerId": container_id,
                "image": plan.image,
                "exitCode": status.exit_code,
                "state": "exited",
                "streamingStarted": false,
            },
            "warnings": [warning],
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|source| CliError::internal(
                format!("failed to render run JSON: {source}")
            ))?
        );
    } else {
        eprintln!("warning: {warning}");
    }

    Ok(CommandResult::container_exit(status.exit_code))
}

async fn wait_for_exited_status(
    client: &CrsClient,
    container_id: &str,
) -> Result<Option<crate::proto::runtime::v1::ContainerStatus>, CliError> {
    let deadline = Instant::now() + STOPPED_STREAMING_STATUS_WAIT;
    loop {
        let status = fetch_container_status(client, container_id).await?;
        if status.state == ContainerState::ContainerExited as i32 {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(STOPPED_STREAMING_STATUS_POLL).await;
    }
}

async fn fetch_container_status(
    client: &CrsClient,
    container_id: &str,
) -> Result<crate::proto::runtime::v1::ContainerStatus, CliError> {
    let mut runtime = client.runtime()?;
    client
        .with_rpc_timeout(async {
            runtime
                .container_status(ContainerStatusRequest {
                    container_id: container_id.to_string(),
                    verbose: false,
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
        .into_inner()
        .status
        .ok_or_else(|| {
            CliError::internal(format!(
                "daemon did not return container status for {container_id}"
            ))
            .with_command("crs run")
            .with_endpoint(client.endpoint())
            .with_object(format!("container {container_id}"))
        })
}

fn stopped_before_streaming_warning(exec_mode: ExecModeArg) -> &'static str {
    match exec_mode {
        ExecModeArg::Attach => {
            "container exited before attach could stream output; use --detach for lifecycle-only checks or run a longer-lived command when interactive output is required"
        }
        ExecModeArg::Sync => {
            "container exited before exec-sync could run; --exec-mode sync runs a command after container start, so use a longer-lived container command or run the desired command as the container process"
        }
    }
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

fn render_detached(
    ctx: &CliContext,
    client: &CrsClient,
    plan: &RunPlan,
    pod_id: &str,
    container_id: &str,
    pod_created: bool,
) -> Result<CommandResult, CliError> {
    render_and_print(
        ctx,
        CommandOutput::new(
            "Run",
            client.endpoint(),
            vec![RunView {
                container_id: container_id.to_string(),
                pod_id: pod_id.to_string(),
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

async fn cleanup_run_resources(
    client: &CrsClient,
    container_id: &str,
    pod_id: &str,
    pod_created: bool,
) -> Vec<String> {
    let mut warnings = Vec::new();
    cleanup_stop_container(client, container_id, &mut warnings).await;
    cleanup_remove_container(client, container_id, &mut warnings).await;

    if pod_created {
        cleanup_stop_pod(client, pod_id, &mut warnings).await;
        cleanup_remove_pod(client, pod_id, &mut warnings).await;
    }

    warnings
}

async fn cleanup_stop_container(
    client: &CrsClient,
    container_id: &str,
    warnings: &mut Vec<String>,
) {
    let Ok(mut runtime) = client.runtime() else {
        warnings
            .push("failed to stop run container during cleanup: runtime client unavailable".into());
        return;
    };
    if let Err(error) = client
        .with_rpc_timeout(async {
            runtime
                .stop_container(StopContainerRequest {
                    container_id: container_id.to_string(),
                    timeout: 0,
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run --rm")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {container_id}"))
                })
        })
        .await
    {
        warnings.push(format!(
            "failed to stop run container {container_id}: {error}; run `crs container stop {container_id}`"
        ));
    }
}

async fn cleanup_remove_container(
    client: &CrsClient,
    container_id: &str,
    warnings: &mut Vec<String>,
) {
    let Ok(mut runtime) = client.runtime() else {
        warnings.push(
            "failed to remove run container during cleanup: runtime client unavailable".into(),
        );
        return;
    };
    if let Err(error) = client
        .with_rpc_timeout(async {
            runtime
                .remove_container(RemoveContainerRequest {
                    container_id: container_id.to_string(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run --rm")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("container {container_id}"))
                })
        })
        .await
    {
        warnings.push(format!(
            "failed to remove run container {container_id}: {error}; run `crs container remove {container_id}`"
        ));
    }
}

async fn cleanup_stop_pod(client: &CrsClient, pod_id: &str, warnings: &mut Vec<String>) {
    let Ok(mut runtime) = client.runtime() else {
        warnings.push("failed to stop run pod during cleanup: runtime client unavailable".into());
        return;
    };
    if let Err(error) = client
        .with_rpc_timeout(async {
            runtime
                .stop_pod_sandbox(StopPodSandboxRequest {
                    pod_sandbox_id: pod_id.to_string(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run --rm")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod_id}"))
                })
        })
        .await
    {
        warnings.push(format!(
            "failed to stop run pod {pod_id}: {error}; run `crs pod stop {pod_id}`"
        ));
    }
}

async fn cleanup_remove_pod(client: &CrsClient, pod_id: &str, warnings: &mut Vec<String>) {
    let Ok(mut runtime) = client.runtime() else {
        warnings.push("failed to remove run pod during cleanup: runtime client unavailable".into());
        return;
    };
    if let Err(error) = client
        .with_rpc_timeout(async {
            runtime
                .remove_pod_sandbox(RemovePodSandboxRequest {
                    pod_sandbox_id: pod_id.to_string(),
                })
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs run --rm")
                        .with_endpoint(client.endpoint())
                        .with_object(format!("pod {pod_id}"))
                })
        })
        .await
    {
        warnings.push(format!(
            "failed to remove run pod {pod_id}: {error}; run `crs pod remove {pod_id}`"
        ));
    }
}

fn emit_cleanup_warnings(ctx: &CliContext, warnings: &[String]) {
    if warnings.is_empty() || matches!(ctx.output(), crate::crs::args::OutputArg::Json) {
        return;
    }

    for warning in warnings {
        eprintln!("warning: {warning}");
    }
}

fn leftover_warnings(container_id: Option<&str>, pod: Option<(&str, bool)>) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(container_id) = container_id {
        warnings.push(format!(
            "run created container {container_id} before failing; remove it with `crs container remove {container_id}`"
        ));
    }
    if let Some((pod_id, true)) = pod {
        warnings.push(format!(
            "run created pod {pod_id} before failing; remove it with `crs pod remove {pod_id}`"
        ));
    }
    warnings
}

fn emit_leftover_warnings(warnings: &[String]) {
    for warning in warnings {
        eprintln!("warning: {warning}");
    }
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
