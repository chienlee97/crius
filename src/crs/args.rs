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
    pub image: String,
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
    pub container: String,
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum StopObjectType {
    Container,
    Pod,
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
    List(ListArgs),
    Inspect { pod: String },
    Run,
    Stop { pod: String },
    Remove { pod: String },
    Stats { pod: Option<String> },
    Metrics,
    UpdateResources { pod: String },
    PortForward { pod: String, #[arg(long)] forward: Vec<String> },
}

#[derive(Debug, ClapArgs)]
pub struct ContainerArgs {
    #[command(subcommand)]
    pub command: ContainerCommand,
}

#[derive(Debug, Subcommand)]
pub enum ContainerCommand {
    List(ListArgs),
    Inspect { id: String },
    Create { pod: String, image: String, command: Vec<String> },
    Start { id: String },
    Stop { id: String },
    Remove { id: String },
    Exec(ExecArgs),
    ExecSync(ExecArgs),
    Attach { id: String },
    Stats { id: Option<String> },
    Checkpoint { id: String, #[arg(long)] location: String },
    Update { id: String },
    ReopenLog { id: String },
    Logs(ContainerLogsArgs),
}

#[derive(Debug, ClapArgs)]
pub struct RunArgs {
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
    Repair { #[arg(long)] dry_run: bool, #[arg(long)] execute: bool },
}

#[derive(Debug, ClapArgs)]
pub struct GcArgs {
    #[command(subcommand)]
    pub command: GcCommand,
}

#[derive(Debug, Subcommand)]
pub enum GcCommand {
    Candidates,
    Run { #[arg(long)] dry_run: bool, #[arg(long)] execute: bool },
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
