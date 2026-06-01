use std::time::Duration;

use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};

use crate::crs::parsers::{parse_duration, DEFAULT_ENDPOINT};

#[derive(Debug, Parser)]
#[command(name = "crs", version, about = "Local command-line client for crius")]
pub struct Args {
    #[arg(long, env = "CRIUS_ADDRESS", default_value = DEFAULT_ENDPOINT, global = true)]
    pub address: String,
    #[arg(long, default_value = "5s", value_parser = parse_duration, global = true)]
    pub connect_timeout: Duration,
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub timeout: Duration,
    #[arg(long, global = true)]
    pub debug: bool,
    #[arg(long, value_enum, default_value_t = OutputArg::Table, global = true)]
    pub output: OutputArg,
    #[arg(long, global = true)]
    pub quiet: bool,
    #[arg(long, global = true)]
    pub no_trunc: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputArg {
    Table,
    Json,
    Text,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Version(VersionArgs),
    Status(StatusArgs),
    Doctor(DoctorArgs),
    Ps(ListArgs),
    Pods(ListArgs),
    Images(ImageListArgs),
    Pull(ImagePullArgs),
    Inspect(InspectArgs),
    Logs(ContainerLogsArgs),
    Exec(ExecArgs),
    Stop(StopArgs),
    Rm(RemoveArgs),
    Config(ConfigArgs),
    Runtime(RuntimeArgs),
    Image(ImageArgs),
    Pod(PodArgs),
    Container(ContainerArgs),
    Run(RunArgs),
    Events(EventsArgs),
    Stats(StatsArgs),
    Metrics(MetricsArgs),
    Recovery(RecoveryArgs),
    Gc(GcArgs),
    Debug(DebugArgs),
    Completion(CompletionArgs),
}

#[derive(Debug, Default, ClapArgs)]
pub struct VersionArgs {}

#[derive(Debug, Default, ClapArgs)]
pub struct StatusArgs {
    #[arg(long)]
    pub verbose: bool,
}

#[derive(Debug, Default, ClapArgs)]
pub struct DoctorArgs {}

#[derive(Debug, Default, ClapArgs)]
pub struct ListArgs {
    #[arg(long)]
    pub all: bool,
}

#[derive(Debug, Default, ClapArgs)]
pub struct ImageListArgs {
    #[arg(long)]
    pub image: Option<String>,
}

#[derive(Debug, ClapArgs)]
pub struct ImagePullArgs {
    #[command(flatten)]
    pub auth: ImageAuthArgs,
    #[arg(long)]
    pub pod: Option<String>,
    pub image: String,
}

#[derive(Debug, Default, ClapArgs)]
pub struct ImageAuthArgs {
    #[arg(
        long,
        conflicts_with_all = [
            "auth_file",
            "username",
            "password",
            "server",
            "identity_token",
            "registry_token"
        ]
    )]
    pub auth_json: Option<String>,
    #[arg(
        long,
        conflicts_with_all = [
            "auth_json",
            "username",
            "password",
            "server",
            "identity_token",
            "registry_token"
        ]
    )]
    pub auth_file: Option<String>,
    #[arg(long)]
    pub username: Option<String>,
    #[arg(long, requires = "username")]
    pub password: Option<String>,
    #[arg(long, requires = "username")]
    pub server: Option<String>,
    #[arg(long, requires = "username")]
    pub identity_token: Option<String>,
    #[arg(long, requires = "username")]
    pub registry_token: Option<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ObjectType {
    Container,
    Pod,
    Image,
}

#[derive(Debug, ClapArgs)]
pub struct InspectArgs {
    #[arg(long = "type", value_enum)]
    pub object_type: Option<ObjectType>,
    pub target: String,
}

#[derive(Debug, ClapArgs)]
pub struct ContainerLogsArgs {
    pub container: String,
}

#[derive(Debug, ClapArgs)]
pub struct ExecArgs {
    #[command(flatten)]
    pub stream: StreamOptions,
    pub container: String,
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum StopObjectType {
    Container,
    Pod,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum PodStateArg {
    Ready,
    Notready,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ContainerStateArg {
    Created,
    Running,
    Exited,
    Unknown,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum PullPolicyArg {
    Missing,
    Always,
    Never,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ExecModeArg {
    Sync,
    Attach,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum StreamProtocolArg {
    Websocket,
    Spdy,
}

impl Default for StreamProtocolArg {
    fn default() -> Self {
        Self::Websocket
    }
}

#[derive(Debug, Default, ClapArgs)]
pub struct StreamOptions {
    #[arg(short = 'i', long)]
    pub stdin: bool,
    #[arg(short = 't', long)]
    pub tty: bool,
    #[arg(long, default_value_t = true)]
    pub stdout: bool,
    #[arg(long, default_value_t = true)]
    pub stderr: bool,
    #[arg(long)]
    pub resize: Option<String>,
    #[arg(long, value_enum, default_value_t = StreamProtocolArg::Websocket)]
    pub protocol: StreamProtocolArg,
}

#[derive(Debug, ClapArgs)]
pub struct StopArgs {
    #[arg(long = "type", value_enum)]
    pub object_type: Option<StopObjectType>,
    #[arg(long, id = "stop-timeout", value_name = "SECONDS")]
    pub timeout: Option<u32>,
    pub target: String,
}

#[derive(Debug, ClapArgs)]
pub struct RemoveArgs {
    #[arg(long = "type", value_enum)]
    pub object_type: Option<ObjectType>,
    #[arg(long)]
    pub force: bool,
    pub target: String,
}

#[derive(Debug, ClapArgs)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Show,
    ReloadStatus,
}

#[derive(Debug, ClapArgs)]
pub struct RuntimeArgs {
    #[command(subcommand)]
    pub command: RuntimeCommand,
}

#[derive(Debug, Subcommand)]
pub enum RuntimeCommand {
    Config,
    Update { #[arg(long)] pod_cidr: String },
    Handlers { #[arg(long)] verbose: bool },
}

#[derive(Debug, ClapArgs)]
pub struct ImageArgs {
    #[command(subcommand)]
    pub command: ImageCommand,
}

#[derive(Debug, Subcommand)]
pub enum ImageCommand {
    List(ImageListArgs),
    Pull(ImagePullArgs),
    Inspect { image: String },
    Remove { image: String },
    FsInfo,
    Transfers,
    Config,
}

#[derive(Debug, ClapArgs)]
pub struct PodArgs {
    #[command(subcommand)]
    pub command: PodCommand,
}

#[derive(Debug, Subcommand)]
pub enum PodCommand {
    List(PodListArgs),
    Inspect { pod: String },
    Run(PodCreateArgs),
    Stop {
        pod: String,
        #[arg(long, value_name = "SECONDS")]
        timeout: Option<u32>,
    },
    Remove { pod: String },
    Stats(PodStatsArgs),
    Metrics,
    UpdateResources {
        pod: String,
        #[arg(long = "overhead")]
        overhead: Vec<String>,
        #[arg(long = "pod-resource")]
        pod_resource: Vec<String>,
    },
    PortForward { pod: String, #[arg(long)] forward: Vec<String> },
}

#[derive(Debug, Default, ClapArgs)]
pub struct PodListArgs {
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long, value_enum)]
    pub state: Option<PodStateArg>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long)]
    pub all: bool,
}

#[derive(Debug, Default, ClapArgs)]
pub struct PodCreateArgs {
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub namespace: Option<String>,
    #[arg(long)]
    pub runtime_handler: Option<String>,
}

#[derive(Debug, Default, ClapArgs)]
pub struct PodStatsArgs {
    pub pod: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
}

#[derive(Debug, ClapArgs)]
pub struct ContainerArgs {
    #[command(subcommand)]
    pub command: ContainerCommand,
}

#[derive(Debug, Subcommand)]
pub enum ContainerCommand {
    List(ContainerListArgs),
    Inspect { id: String },
    Create(ContainerCreateArgs),
    Start { id: String },
    Stop {
        id: String,
        #[arg(long, value_name = "SECONDS")]
        timeout: Option<u32>,
    },
    Remove { id: String },
    Exec(ExecArgs),
    ExecSync(ExecArgs),
    Attach {
        id: String,
        #[command(flatten)]
        stream: StreamOptions,
    },
    Stats(ContainerStatsArgs),
    Checkpoint { id: String, #[arg(long)] location: String },
    Update { id: String },
    ReopenLog { id: String },
    Logs(ContainerLogsArgs),
}

#[derive(Debug, Default, ClapArgs)]
pub struct ContainerListArgs {
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long)]
    pub pod: Option<String>,
    #[arg(long, value_enum)]
    pub state: Option<ContainerStateArg>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long)]
    pub all: bool,
}

#[derive(Debug, ClapArgs)]
pub struct ContainerCreateArgs {
    pub pod: String,
    pub image: String,
    pub command: Vec<String>,
}

#[derive(Debug, Default, ClapArgs)]
pub struct ContainerStatsArgs {
    pub id: Option<String>,
    #[arg(long)]
    pub pod: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
}

#[derive(Debug, ClapArgs)]
pub struct RunArgs {
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub namespace: Option<String>,
    #[arg(long)]
    pub detach: bool,
    #[arg(long)]
    pub rm: bool,
    #[arg(long)]
    pub tty: bool,
    #[arg(long)]
    pub stdin: bool,
    #[arg(long, value_enum, default_value_t = StreamProtocolArg::Websocket)]
    pub protocol: StreamProtocolArg,
    #[arg(long)]
    pub pod: Option<String>,
    #[arg(long, value_enum, default_value_t = PullPolicyArg::Missing)]
    pub pull: PullPolicyArg,
    #[arg(long, value_enum, default_value_t = ExecModeArg::Attach)]
    pub exec_mode: ExecModeArg,
    pub image: String,
    pub command: Vec<String>,
}

#[derive(Debug, Default, ClapArgs)]
pub struct EventsArgs {}

#[derive(Debug, Default, ClapArgs)]
pub struct StatsArgs {}

#[derive(Debug, ClapArgs)]
pub struct MetricsArgs {
    #[command(subcommand)]
    pub command: MetricsCommand,
}

#[derive(Debug, Subcommand)]
pub enum MetricsCommand {
    Descriptors,
    Scrape,
}

#[derive(Debug, ClapArgs)]
pub struct RecoveryArgs {
    #[command(subcommand)]
    pub command: RecoveryCommand,
}

#[derive(Debug, Subcommand)]
pub enum RecoveryCommand {
    Status,
    Check,
    Repair(ExecuteModeArgs),
}

#[derive(Debug, ClapArgs)]
pub struct GcArgs {
    #[command(subcommand)]
    pub command: GcCommand,
}

#[derive(Debug, Subcommand)]
pub enum GcCommand {
    Candidates,
    Run(ExecuteModeArgs),
}

#[derive(Debug, ClapArgs)]
#[group(required = true, multiple = false)]
pub struct ExecuteModeArgs {
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub execute: bool,
}

#[derive(Debug, ClapArgs)]
pub struct DebugArgs {
    #[command(subcommand)]
    pub command: DebugCommand,
}

#[derive(Debug, Subcommand)]
pub enum DebugCommand {
    Network,
    Runtime,
    Shims,
    Nri,
    Security,
    Cgroups,
    Streaming,
    Metrics,
    Tracing,
    Rootless,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
    Powershell,
}

#[derive(Debug, ClapArgs)]
pub struct CompletionArgs {
    #[arg(value_enum)]
    pub shell: CompletionShell,
}
