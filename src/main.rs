use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Error;
use clap::Parser;
use crius::config::Config;
use crius::image::ImageServiceImpl;
use crius::proto::runtime::v1::{
    image_service_server::ImageServiceServer, runtime_service_server::RuntimeServiceServer,
};
use crius::runtime::ShimConfig;
use crius::server::{RuntimeConfig, RuntimeServiceImpl};
use crius::streaming::StreamingServer;
use tokio::net::UnixListener as TokioUnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::{debug, info};
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::{fmt, EnvFilter};

const LOCAL_LOG_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f%:z";

#[derive(Debug, Clone, Copy, Default)]
struct LocalLogTimer;

/// crius - OCI-based implementation of Kubernetes Container Runtime Interface
#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[clap(short, long, default_value = "/etc/crius/crius.conf")]
    config: PathBuf,

    /// Enable debug logging
    #[clap(short, long)]
    debug: bool,

    /// Log file path
    #[clap(short, long)]
    log: Option<PathBuf>,

    /// Listen address (IP:port or unix://path/to/socket). TCP requires api.allow_tcp_service=true.
    #[clap(long)]
    listen: Option<String>,

    /// Print the built-in default configuration as TOML and exit.
    #[clap(long, conflicts_with = "write_default_config")]
    dump_default_config: bool,

    /// Write the built-in default configuration as TOML to the given path and exit.
    #[clap(long, value_name = "PATH", conflicts_with = "dump_default_config")]
    write_default_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // 解析命令行参数
    let args = Args::parse();

    if args.dump_default_config || args.write_default_config.is_some() {
        handle_default_config_export(&args)?;
        return Ok(());
    }

    let mut config = match Config::load(&args.config) {
        Ok(cfg) => cfg,
        Err(crius::error::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            Config::default()
        }
        Err(err) => return Err(err.into()),
    };
    config.apply_env_overrides()?;
    apply_cli_overrides(&args, &mut config);
    config.validate()?;
    std::env::set_var(
        "CRIUS_ATTACH_SOCKET_DIR",
        config.runtime.attach_socket_dir.trim(),
    );
    init_logging(&config)?;

    info!("Loaded configuration from {}", args.config.display());

    let runtime_name = config.runtime.runtime_type.clone();
    let runtime_configs = config.runtime.resolved_runtimes()?;
    let uid_mappings = config.runtime.parsed_uid_mappings()?;
    let gid_mappings = config.runtime.parsed_gid_mappings()?;
    let default_env = config.runtime.parsed_default_env();
    let default_sysctls = config.runtime.parsed_default_sysctls()?;
    let mut cni_config = config.network.cni_config();
    if config.network.netns_mounts_under_state_dir {
        cni_config.set_netns_mount_dir(PathBuf::from(&config.runtime.root).join("netns"));
    }
    for (handler, handler_config) in &config.runtime.runtimes {
        let cni_conf_dir = handler_config.cni_conf_dir.trim();
        if !cni_conf_dir.is_empty() {
            cni_config.set_handler_config_dirs(handler.clone(), vec![PathBuf::from(cni_conf_dir)]);
        }
        if let Some(cni_max_conf_num) = handler_config.cni_max_conf_num {
            cni_config.set_handler_max_conf_num(handler.clone(), cni_max_conf_num);
        }
    }
    let runtime_config = RuntimeConfig {
        root_dir: PathBuf::from(&config.root),
        runtime: runtime_name,
        runtime_handlers: config.runtime.normalized_handlers(),
        runtime_configs,
        runtime_root: PathBuf::from(&config.runtime.root),
        log_dir: PathBuf::from(&config.logging.dir),
        runtime_path: PathBuf::from(&config.runtime.runtime_path),
        image_root: PathBuf::from(&config.image.root),
        image_driver: config.image.driver.clone(),
        workloads: config.runtime.workloads.clone(),
        enable_pod_events: config.api.enable_pod_events,
        included_pod_metrics: config.api.included_pod_metrics.clone(),
        stats_collection_period: config.api.stats_collection_period,
        pod_sandbox_metrics_collection_period: config.api.pod_sandbox_metrics_collection_period,
        grpc_max_send_msg_size: config.api.grpc_max_send_msg_size,
        grpc_max_recv_msg_size: config.api.grpc_max_recv_msg_size,
        monitor_env: config.runtime.monitor_env.clone(),
        default_env,
        default_capabilities: config
            .runtime
            .default_capabilities
            .iter()
            .map(|capability| {
                let upper = capability.trim().to_ascii_uppercase();
                if upper.starts_with("CAP_") {
                    upper
                } else {
                    format!("CAP_{upper}")
                }
            })
            .collect(),
        default_sysctls,
        attach_socket_dir: PathBuf::from(&config.runtime.attach_socket_dir),
        container_exits_dir: PathBuf::from(&config.runtime.container_exits_dir),
        clean_shutdown_file: PathBuf::from(&config.runtime.clean_shutdown_file),
        container_stop_timeout: config.runtime.container_stop_timeout,
        version_file: PathBuf::from(&config.runtime.version_file),
        version_file_persist: PathBuf::from(&config.runtime.version_file_persist),
        criu_path: PathBuf::from(&config.runtime.criu_path),
        criu_image_path: PathBuf::from(&config.runtime.criu_image_path),
        criu_work_path: PathBuf::from(&config.runtime.criu_work_path),
        enable_criu_support: config.runtime.enable_criu_support,
        internal_wipe: config.runtime.internal_wipe,
        internal_repair: config.runtime.internal_repair,
        bind_mount_prefix: PathBuf::from(&config.runtime.bind_mount_prefix),
        disable_cgroup: config.runtime.disable_cgroup,
        tolerate_missing_hugetlb_controller: config.runtime.tolerate_missing_hugetlb_controller,
        separate_pull_cgroup: config.runtime.separate_pull_cgroup.clone(),
        seccomp_profile: PathBuf::from(&config.runtime.seccomp_profile),
        unset_seccomp_profile: config.runtime.unset_seccomp_profile.clone(),
        uid_mappings: (!uid_mappings.is_empty()).then_some(uid_mappings),
        gid_mappings: (!gid_mappings.is_empty()).then_some(gid_mappings),
        minimum_mappable_uid: config.runtime.minimum_mappable_uid,
        minimum_mappable_gid: config.runtime.minimum_mappable_gid,
        io_uid: config.runtime.io_uid,
        io_gid: config.runtime.io_gid,
        pids_limit: config.runtime.pids_limit,
        exec_cpu_affinity: config.runtime.exec_cpu_affinity.clone(),
        read_only: config.runtime.read_only,
        no_pivot: config.runtime.no_pivot,
        pause_image: config.runtime.pause_image.clone(),
        pause_command: config.runtime.pause_command.clone(),
        cni_config,
        cgroup_driver: config.runtime.cgroup_driver.map(|driver| driver.as_proto()),
        exec_sync_io_drain_timeout: config.api.exec_sync_io_drain_timeout,
        max_container_log_line_size: config.logging.max_container_log_line_size,
        log_to_journald: config.runtime.log_to_journald,
        no_sync_log: config.runtime.no_sync_log,
        restrict_oom_score_adj: config.runtime.restrict_oom_score_adj,
        enable_unprivileged_ports: config.runtime.enable_unprivileged_ports,
        enable_unprivileged_icmp: config.runtime.enable_unprivileged_icmp,
        shim: ShimConfig {
            shim_path: PathBuf::from(&config.runtime.shim_path),
            work_dir: PathBuf::from(&config.runtime.shim_dir),
            attach_socket_dir: PathBuf::from(&config.runtime.attach_socket_dir),
            container_exits_dir: PathBuf::from(&config.runtime.container_exits_dir),
            io_uid: config.runtime.io_uid,
            io_gid: config.runtime.io_gid,
            monitor_env: config.runtime.monitor_env.clone(),
            debug: config.runtime.shim_debug,
            log_to_journald: config.runtime.log_to_journald,
            no_sync_log: config.runtime.no_sync_log,
            no_pivot: config.runtime.no_pivot,
            runtime_path: PathBuf::from(&config.runtime.runtime_path),
            max_container_log_line_size: config.logging.max_container_log_line_size,
        },
        streaming: config.api.streaming.clone(),
    };
    let listen = config.api.listen.clone();

    // 创建服务实例
    let runtime_service =
        RuntimeServiceImpl::new_with_nri_config(runtime_config.clone(), config.nri.clone());
    let streaming_server = StreamingServer::start_with_config(
        config.api.streaming.clone(),
        runtime_config.runtime_path.clone(),
    )
    .await?;
    runtime_service
        .set_streaming_server(streaming_server.clone())
        .await;

    prepare_runtime_service(&runtime_service).await;
    let shutdown_nri = runtime_service.nri_handle();
    let image_service = ImageServiceImpl::new(
        &config.image.root,
        &config.image.driver,
        (!config.image.global_auth_file.trim().is_empty())
            .then_some(config.image.global_auth_file.as_str()),
    )?;
    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/file_descriptor_set.bin"
        )))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create reflection service: {}", e))?;

    // 加载本地镜像
    info!("About to load local images...");
    match image_service.load_local_images().await {
        Ok(_) => info!("Local images loaded successfully"),
        Err(e) => log::error!("Failed to load local images: {}", e),
    }

    // 创建gRPC服务器
    info!("Starting crius gRPC server on {}", listen);
    debug!("Using configuration: {:?}", runtime_config);
    info!(
        "Streaming server listening on {}",
        streaming_server.base_url()
    );

    let runtime_service_server = RuntimeServiceServer::new(runtime_service)
        .max_encoding_message_size(runtime_config.grpc_max_send_msg_size as usize)
        .max_decoding_message_size(runtime_config.grpc_max_recv_msg_size as usize);
    let image_service_server = ImageServiceServer::new(image_service)
        .max_encoding_message_size(runtime_config.grpc_max_send_msg_size as usize)
        .max_decoding_message_size(runtime_config.grpc_max_recv_msg_size as usize);

    let server = Server::builder()
        .add_service(runtime_service_server)
        .add_service(image_service_server)
        .add_service(reflection_service);

    if listen.starts_with("unix://") {
        // Unix domain socket
        let socket_path = listen.trim_start_matches("unix://");
        let path = Path::new(socket_path);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // 清理旧socket文件
        let _ = fs::remove_file(path);

        // 创建Unix监听器
        let uds = UnixListener::bind(path)?;
        let uds_stream = UnixListenerStream::new(TokioUnixListener::from_std(uds)?);

        // 启动服务
        let serve_result = server
            .serve_with_incoming_shutdown(uds_stream, shutdown_signal())
            .await;
        shutdown_runtime_service(&runtime_config.clean_shutdown_file, shutdown_nri).await;
        serve_result?;
    } else {
        let addr: SocketAddr = listen.parse()?;
        let serve_result = server.serve_with_shutdown(addr, shutdown_signal()).await;
        shutdown_runtime_service(&runtime_config.clean_shutdown_file, shutdown_nri).await;
        serve_result?;
    }

    Ok(())
}

fn render_default_config_toml() -> Result<String, Error> {
    toml::to_string_pretty(&Config::default())
        .map_err(|err| anyhow::anyhow!("Failed to render default config template: {}", err).into())
}

fn handle_default_config_export(args: &Args) -> Result<(), Error> {
    let rendered = render_default_config_toml()?;
    if args.dump_default_config {
        std::io::stdout().write_all(rendered.as_bytes())?;
        return Ok(());
    }

    if let Some(path) = &args.write_default_config {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, rendered)?;
    }

    Ok(())
}

async fn prepare_runtime_service(runtime_service: &RuntimeServiceImpl) {
    match consume_clean_shutdown_marker(runtime_service.clean_shutdown_file()).await {
        Ok(clean) => {
            runtime_service.record_startup_clean_shutdown(clean);
            if clean {
                info!("Detected clean shutdown marker from previous run");
            } else {
                log::warn!(
                    "Clean shutdown marker is missing; treating startup as unclean recovery"
                );
            }
        }
        Err(err) => log::error!("Failed to inspect clean shutdown marker: {}", err),
    }
    match refresh_version_file_marker(runtime_service.version_file(), env!("CARGO_PKG_VERSION"))
        .await
    {
        Ok(detected_reboot) => {
            runtime_service.record_startup_detected_reboot(detected_reboot);
            if detected_reboot {
                log::warn!(
                    "Temporary version marker is missing; treating startup as reboot recovery"
                );
            }
        }
        Err(err) => log::error!("Failed to refresh version marker: {}", err),
    }
    match refresh_version_file_persist_marker(
        runtime_service.version_file_persist(),
        env!("CARGO_PKG_VERSION"),
    )
    .await
    {
        Ok(detected_upgrade) => {
            runtime_service.record_startup_detected_upgrade(detected_upgrade);
            if detected_upgrade {
                log::warn!(
                    "Persistent version marker differs from current version; treating startup as upgrade recovery"
                );
            }
        }
        Err(err) => log::error!("Failed to refresh persistent version marker: {}", err),
    }
    match runtime_service
        .maybe_repair_persistence_after_unclean_shutdown()
        .await
    {
        Ok(()) => {}
        Err(err) => log::error!("Failed to evaluate persistence repair path: {}", err),
    }
    info!("Recovering state from database...");
    if let Err(e) = runtime_service.recover_state().await {
        log::error!("Failed to recover state: {}", e);
    }
    if let Err(e) = runtime_service.initialize_nri().await {
        log::error!("Failed to initialize NRI: {}", e);
    }
}

async fn shutdown_runtime_service(clean_shutdown_file: &Path, nri: Arc<dyn crius::nri::NriApi>) {
    if let Err(err) = write_clean_shutdown_marker(clean_shutdown_file).await {
        log::error!("Failed to persist clean shutdown marker: {}", err);
    }
    if let Err(err) = nri.shutdown().await {
        log::error!("Failed to shutdown NRI: {}", err);
    }
}

async fn consume_clean_shutdown_marker(path: &Path) -> Result<bool, Error> {
    if tokio::fs::try_exists(path).await? {
        tokio::fs::remove_file(path).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn write_clean_shutdown_marker(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, b"clean\n").await?;
    Ok(())
}

async fn refresh_version_file_marker(path: &Path, current_version: &str) -> Result<bool, Error> {
    let detected_reboot = !tokio::fs::try_exists(path).await?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, format!("{current_version}\n")).await?;
    Ok(detected_reboot)
}

async fn refresh_version_file_persist_marker(
    path: &Path,
    current_version: &str,
) -> Result<bool, Error> {
    let detected_upgrade = match tokio::fs::read_to_string(path).await {
        Ok(existing) => existing.trim() != current_version,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
        Err(err) => return Err(err.into()),
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, format!("{current_version}\n")).await?;
    Ok(detected_upgrade)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn apply_cli_overrides(args: &Args, config: &mut Config) {
    if let Some(listen) = &args.listen {
        config.api.listen = listen.clone();
    }
    if let Some(log_file) = &args.log {
        config.logging.file = Some(log_file.display().to_string());
    }
    if args.debug {
        config.logging.level = "debug".to_string();
    }
}

fn init_logging(config: &Config) -> Result<(), Error> {
    let base_filter = EnvFilter::try_from_default_env().or_else(|_| {
        EnvFilter::try_new(format!(
            "crius={},tower_http=info",
            config.logging.level.trim()
        ))
    })?;
    let writer = LogOutput::from_config(config)?;

    fmt()
        .with_env_filter(base_filter)
        .with_timer(LocalLogTimer)
        .with_file(true)
        .with_line_number(true)
        .with_writer(writer)
        .init();

    Ok(())
}

#[derive(Clone)]
enum LogOutput {
    Stderr,
    File(Arc<Mutex<std::fs::File>>),
}

impl LogOutput {
    fn from_config(config: &Config) -> Result<Self, Error> {
        if let Some(path) = &config.logging.file {
            let path = PathBuf::from(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            Ok(Self::File(Arc::new(Mutex::new(file))))
        } else {
            Ok(Self::Stderr)
        }
    }
}

struct LockedFileWriter {
    file: Arc<Mutex<std::fs::File>>,
}

impl std::io::Write for LockedFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("log file mutex poisoned"))?;
        std::io::Write::write(&mut *file, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("log file mutex poisoned"))?;
        std::io::Write::flush(&mut *file)
    }
}

impl<'a> MakeWriter<'a> for LogOutput {
    type Writer = Box<dyn std::io::Write + 'a>;

    fn make_writer(&'a self) -> Self::Writer {
        match self {
            Self::Stderr => Box::new(std::io::stderr()),
            Self::File(file) => Box::new(LockedFileWriter { file: file.clone() }),
        }
    }
}

impl tracing_subscriber::fmt::time::FormatTime for LocalLogTimer {
    fn format_time(
        &self,
        writer: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        write!(
            writer,
            "{}",
            chrono::Local::now().format(LOCAL_LOG_TIME_FORMAT)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::prepare_runtime_service;
    use super::shutdown_runtime_service;
    use super::Args;
    use super::LocalLogTimer;
    use super::RuntimeConfig;
    use crius::config::{Config, NriConfig};
    use crius::network::CniConfig;
    use crius::nri::NriApi;
    use crius::runtime::ShimConfig;
    use crius::server::RuntimeServiceImpl;
    use crius::streaming::StreamingConfig;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use tracing_subscriber::fmt::time::FormatTime;

    #[derive(Default)]
    struct FakeNri {
        calls: Mutex<Vec<&'static str>>,
    }

    #[async_trait::async_trait]
    impl NriApi for FakeNri {
        async fn start(&self) -> crius::nri::Result<()> {
            self.calls.lock().await.push("start");
            Ok(())
        }

        async fn shutdown(&self) -> crius::nri::Result<()> {
            self.calls.lock().await.push("shutdown");
            Ok(())
        }

        async fn synchronize(&self) -> crius::nri::Result<()> {
            self.calls.lock().await.push("synchronize");
            Ok(())
        }
    }

    fn test_runtime_config(root_dir: PathBuf) -> RuntimeConfig {
        RuntimeConfig {
            root_dir,
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_configs: std::collections::HashMap::from([(
                "runc".to_string(),
                crius::config::ResolvedRuntimeHandlerConfig {
                    runtime_path: "/definitely/missing/runc".to_string(),
                    runtime_root: "/tmp/crius-main-test-runtime-root".to_string(),
                    monitor_path: "/definitely/missing/crius-shim".to_string(),
                    monitor_env: Vec::new(),
                    allowed_annotations: Vec::new(),
                    default_annotations: std::collections::HashMap::new(),
                    container_create_timeout: 240,
                },
            )]),
            runtime_root: PathBuf::from("/tmp/crius-main-test-runtime-root"),
            log_dir: PathBuf::from("/tmp/crius-main-test-logs"),
            runtime_path: PathBuf::from("/definitely/missing/runc"),
            image_root: PathBuf::from("/tmp/crius-main-test-images"),
            image_driver: "overlay".to_string(),
            workloads: std::collections::HashMap::new(),
            enable_pod_events: true,
            included_pod_metrics: vec!["all".to_string()],
            stats_collection_period: 0,
            pod_sandbox_metrics_collection_period: 0,
            grpc_max_send_msg_size: 80 * 1024 * 1024,
            grpc_max_recv_msg_size: 80 * 1024 * 1024,
            monitor_env: Vec::new(),
            default_env: Vec::new(),
            default_capabilities: vec![
                "CHOWN".to_string(),
                "DAC_OVERRIDE".to_string(),
                "FSETID".to_string(),
                "FOWNER".to_string(),
                "MKNOD".to_string(),
                "NET_RAW".to_string(),
                "SETGID".to_string(),
                "SETUID".to_string(),
                "SETFCAP".to_string(),
                "SETPCAP".to_string(),
                "NET_BIND_SERVICE".to_string(),
                "SYS_CHROOT".to_string(),
                "KILL".to_string(),
                "AUDIT_WRITE".to_string(),
            ],
            default_sysctls: std::collections::HashMap::new(),
            attach_socket_dir: PathBuf::from("/tmp/crius-main-test-attach"),
            container_exits_dir: PathBuf::from("/tmp/crius-main-test-exits"),
            clean_shutdown_file: PathBuf::from("/tmp/crius-main-test-clean.shutdown"),
            container_stop_timeout: 30,
            version_file: PathBuf::from("/tmp/crius-main-test-version"),
            version_file_persist: PathBuf::from("/tmp/crius-main-test-version-persist"),
            criu_path: PathBuf::new(),
            criu_image_path: PathBuf::new(),
            criu_work_path: PathBuf::new(),
            enable_criu_support: true,
            internal_wipe: true,
            internal_repair: true,
            bind_mount_prefix: PathBuf::new(),
            disable_cgroup: false,
            tolerate_missing_hugetlb_controller: true,
            separate_pull_cgroup: String::new(),
            seccomp_profile: PathBuf::new(),
            unset_seccomp_profile: "runtime/default".to_string(),
            uid_mappings: None,
            gid_mappings: None,
            minimum_mappable_uid: -1,
            minimum_mappable_gid: -1,
            io_uid: 0,
            io_gid: 0,
            pids_limit: -1,
            exec_cpu_affinity: String::new(),
            read_only: false,
            no_pivot: false,
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            pause_command: "/pause".to_string(),
            cni_config: CniConfig::default(),
            cgroup_driver: None,
            exec_sync_io_drain_timeout: std::time::Duration::ZERO,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            restrict_oom_score_adj: false,
            enable_unprivileged_ports: false,
            enable_unprivileged_icmp: false,
            shim: ShimConfig {
                shim_path: PathBuf::from("/definitely/missing/crius-shim"),
                work_dir: PathBuf::from("/tmp/crius-main-test-shims"),
                attach_socket_dir: PathBuf::from("/tmp/crius-main-test-attach"),
                container_exits_dir: PathBuf::from("/tmp/crius-main-test-exits"),
                io_uid: 0,
                io_gid: 0,
                monitor_env: Vec::new(),
                debug: false,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: false,
                runtime_path: PathBuf::from("/definitely/missing/runc"),
                max_container_log_line_size: 4096,
            },
            streaming: StreamingConfig::default(),
        }
    }

    #[test]
    fn local_log_time_uses_rfc3339_local_timestamp() {
        let mut buf = String::new();
        let mut writer = tracing_subscriber::fmt::format::Writer::new(&mut buf);

        LocalLogTimer
            .format_time(&mut writer)
            .expect("local log time should format successfully");

        let parsed = chrono::DateTime::parse_from_rfc3339(&buf)
            .expect("local log time should be valid RFC3339");
        assert_eq!(
            parsed.offset().local_minus_utc(),
            chrono::Local::now().offset().local_minus_utc()
        );
    }

    #[test]
    fn cli_overrides_take_precedence_over_loaded_config() {
        let mut config = Config::default();
        let args = Args {
            config: PathBuf::from("/etc/crius/crius.conf"),
            debug: true,
            log: Some(PathBuf::from("/var/log/crius/daemon.log")),
            listen: Some("unix:///tmp/crius.sock".to_string()),
            dump_default_config: false,
            write_default_config: None,
        };

        super::apply_cli_overrides(&args, &mut config);

        assert_eq!(config.api.listen, "unix:///tmp/crius.sock");
        assert_eq!(config.logging.level, "debug");
        assert_eq!(
            config.logging.file.as_deref(),
            Some("/var/log/crius/daemon.log")
        );
    }

    #[test]
    fn render_default_config_toml_roundtrips() {
        let rendered = super::render_default_config_toml().unwrap();
        let parsed: Config = toml::from_str(&rendered).unwrap();

        assert_eq!(parsed.api.listen, Config::default().api.listen);
        assert_eq!(parsed.runtime.pause_image, Config::default().runtime.pause_image);
        assert_eq!(parsed.image.root, Config::default().image.root);
    }

    #[test]
    fn handle_default_config_export_writes_requested_path() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("generated").join("crius.conf");
        let args = Args {
            config: PathBuf::from("/etc/crius/crius.conf"),
            debug: false,
            log: None,
            listen: None,
            dump_default_config: false,
            write_default_config: Some(output_path.clone()),
        };

        super::handle_default_config_export(&args).unwrap();

        let rendered = fs::read_to_string(output_path).unwrap();
        let parsed: Config = toml::from_str(&rendered).unwrap();
        assert_eq!(parsed.runtime.runtime_type, "runc");
    }

    #[tokio::test]
    async fn prepare_runtime_service_recovers_then_initializes_nri_before_serve() {
        let fake_nri = Arc::new(FakeNri::default());
        let nri_config = NriConfig {
            enable: true,
            ..Default::default()
        };
        let service = RuntimeServiceImpl::new_with_nri_api(
            test_runtime_config(tempdir().unwrap().keep()),
            nri_config,
            fake_nri.clone(),
        );

        prepare_runtime_service(&service).await;

        assert_eq!(
            fake_nri.calls.lock().await.clone(),
            vec!["start", "synchronize"]
        );
    }

    #[tokio::test]
    async fn prepare_runtime_service_consumes_clean_shutdown_marker() {
        let fake_nri = Arc::new(FakeNri::default());
        let root = tempdir().unwrap();
        let mut config = test_runtime_config(root.path().to_path_buf());
        config.clean_shutdown_file = root.path().join("clean.shutdown");
        fs::write(&config.clean_shutdown_file, "clean\n").unwrap();
        let service =
            RuntimeServiceImpl::new_with_nri_api(config.clone(), NriConfig::default(), fake_nri);

        prepare_runtime_service(&service).await;

        assert_eq!(service.last_startup_clean_shutdown(), Some(true));
        assert!(!config.clean_shutdown_file.exists());
    }

    #[tokio::test]
    async fn prepare_runtime_service_detects_reboot_when_version_marker_is_missing() {
        let fake_nri = Arc::new(FakeNri::default());
        let root = tempdir().unwrap();
        let mut config = test_runtime_config(root.path().to_path_buf());
        config.version_file = root.path().join("version");
        let service =
            RuntimeServiceImpl::new_with_nri_api(config.clone(), NriConfig::default(), fake_nri);

        prepare_runtime_service(&service).await;

        assert_eq!(service.last_startup_detected_reboot(), Some(true));
        assert_eq!(
            fs::read_to_string(&config.version_file).unwrap(),
            format!("{}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn prepare_runtime_service_detects_upgrade_when_persistent_version_marker_differs() {
        let fake_nri = Arc::new(FakeNri::default());
        let root = tempdir().unwrap();
        let mut config = test_runtime_config(root.path().to_path_buf());
        config.version_file = root.path().join("version");
        config.version_file_persist = root.path().join("version-persist");
        fs::write(&config.version_file_persist, "0.0.0\n").unwrap();
        let service =
            RuntimeServiceImpl::new_with_nri_api(config.clone(), NriConfig::default(), fake_nri);

        prepare_runtime_service(&service).await;

        assert_eq!(service.last_startup_detected_upgrade(), Some(true));
        assert_eq!(
            fs::read_to_string(&config.version_file_persist).unwrap(),
            format!("{}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn prepare_runtime_service_checks_persistence_integrity_on_unclean_startup() {
        let fake_nri = Arc::new(FakeNri::default());
        let root = tempdir().unwrap();
        let config = test_runtime_config(root.path().to_path_buf());
        let service = RuntimeServiceImpl::new_with_nri_api(config, NriConfig::default(), fake_nri);

        prepare_runtime_service(&service).await;

        assert_eq!(service.last_startup_attempted_repair(), Some(false));
        assert_eq!(service.last_startup_repair_succeeded(), Some(true));
    }

    #[tokio::test]
    async fn shutdown_runtime_service_writes_clean_shutdown_marker_and_invokes_nri_shutdown() {
        let fake_nri = Arc::new(FakeNri::default());
        let root = tempdir().unwrap();
        let mut config = test_runtime_config(root.path().to_path_buf());
        config.clean_shutdown_file = root.path().join("clean.shutdown");
        let _service = RuntimeServiceImpl::new_with_nri_api(
            config.clone(),
            NriConfig::default(),
            fake_nri.clone(),
        );

        shutdown_runtime_service(&config.clean_shutdown_file, fake_nri.clone()).await;

        assert!(config.clean_shutdown_file.exists());
        assert_eq!(fake_nri.calls.lock().await.clone(), vec!["shutdown"]);
    }
}
