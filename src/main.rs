use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Error;
use clap::Parser;
use crius::config::{CgroupDriverConfig, Config};
use crius::oci::spec::Spec;
use crius::proto::diagnostics::v1::diagnostics_service_server::DiagnosticsServiceServer;
use crius::proto::local::v1::local_service_server::LocalServiceServer;
use crius::proto::runtime::v1::{
    image_service_server::ImageServiceServer, runtime_service_server::RuntimeServiceServer,
};
use crius::runtime::ShimConfig;
use crius::server::{IrqBalanceRestoreStatus, RuntimeConfig, RuntimeServiceImpl};
use crius::services::{DiagnosticsServiceImpl, DiagnosticsState, LocalServiceImpl};
use crius::streaming::StreamingServer;
use tokio::net::UnixListener as TokioUnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::{debug, info, warn};
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

const LOCAL_LOG_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f%:z";
const SERVER_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);

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

    /// Override the OCI runtime binary path.
    #[clap(long)]
    runtime_path: Option<PathBuf>,

    /// Override the runtime-specific config file path.
    #[clap(long)]
    runtime_config_path: Option<PathBuf>,

    /// Override the runtime state/runroot directory.
    #[clap(long)]
    runtime_root: Option<PathBuf>,

    /// Override the pause image reference used for PodSandbox.
    #[clap(long)]
    pause_image: Option<String>,

    /// Override CNI config directories, comma-separated.
    #[clap(long, value_delimiter = ',')]
    cni_config_dirs: Vec<String>,

    /// Override CNI plugin directories, comma-separated.
    #[clap(long, value_delimiter = ',')]
    cni_plugin_dirs: Vec<String>,

    /// Override the streaming server bind address.
    #[clap(long)]
    stream_address: Option<String>,

    /// Override the streaming server bind port.
    #[clap(long)]
    stream_port: Option<u16>,

    /// Override whether the streaming server uses TLS.
    #[clap(long)]
    stream_enable_tls: Option<bool>,

    /// Override the streaming TLS certificate file path.
    #[clap(long)]
    stream_tls_cert_file: Option<PathBuf>,

    /// Override the streaming TLS private key file path.
    #[clap(long)]
    stream_tls_key_file: Option<PathBuf>,

    /// Override the streaming TLS client CA file path.
    #[clap(long)]
    stream_tls_ca_file: Option<PathBuf>,

    /// Override the streaming TLS minimum version.
    #[clap(long)]
    stream_tls_min_version: Option<String>,

    /// Override the streaming TLS cipher suite list, comma-separated.
    #[clap(long, value_delimiter = ',')]
    stream_tls_cipher_suites: Vec<String>,

    /// Override the default runtime seccomp profile selector/path.
    #[clap(long)]
    seccomp_profile: Option<String>,

    /// Override the default AppArmor profile name.
    #[clap(long)]
    apparmor_default_profile: Option<String>,

    /// Override whether AppArmor handling is disabled.
    #[clap(long)]
    disable_apparmor: Option<bool>,

    /// Override whether SELinux labeling is enabled.
    #[clap(long)]
    enable_selinux: Option<bool>,

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
    config.normalize_derived_settings();
    config.validate()?;
    let effective_rootless = crius::rootless::EffectiveRootlessConfig::resolve(&config.rootless)
        .map_err(|err| anyhow::anyhow!("failed to resolve rootless config: {}", err))?;
    if effective_rootless.enabled {
        std::env::set_var("XDG_RUNTIME_DIR", &effective_rootless.xdg_runtime_dir);
    }
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
    let default_ulimits = config.runtime.parsed_default_ulimits()?;
    let additional_devices = config.runtime.parsed_additional_devices()?;
    let allowed_devices = config.runtime.parsed_allowed_devices();
    let base_runtime_spec = if config.runtime.base_runtime_spec.trim().is_empty() {
        None
    } else {
        Some(
            Spec::load(PathBuf::from(&config.runtime.base_runtime_spec)).map_err(|err| {
                anyhow::anyhow!(
                    "failed to load base runtime spec {}: {}",
                    config.runtime.base_runtime_spec,
                    err
                )
            })?,
        )
    };
    let mut cni_config = config.network.cni_config();
    let mut local_cni_config = config.network.local_cni_config();
    cni_config.set_rootless_config(Some(effective_rootless.clone()));
    local_cni_config.set_rootless_config(Some(effective_rootless.clone()));
    if effective_rootless.enabled {
        cni_config.set_netns_mount_dir(effective_rootless.netns_dir.clone());
        local_cni_config.set_netns_mount_dir(effective_rootless.netns_dir.clone());
    } else if config.network.netns_mounts_under_state_dir {
        cni_config.set_netns_mount_dir(PathBuf::from(&config.runtime.root).join("netns"));
        local_cni_config.set_netns_mount_dir(PathBuf::from(&config.runtime.root).join("netns"));
    }
    if !config.runtime.pinns_path.trim().is_empty() {
        cni_config.set_namespace_helper_path(Some(PathBuf::from(&config.runtime.pinns_path)));
        local_cni_config.set_namespace_helper_path(Some(PathBuf::from(&config.runtime.pinns_path)));
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
        runtime_config_path: PathBuf::from(&config.runtime.runtime_config_path),
        image_root: PathBuf::from(&config.image.root),
        image_driver: config.image.driver.clone(),
        image_global_auth_file: PathBuf::from(&config.image.global_auth_file),
        image_namespaced_auth_dir: PathBuf::from(&config.image.namespaced_auth_dir),
        image_default_transport: config.image.default_transport.clone(),
        image_short_name_mode: config.image.short_name_mode.clone(),
        image_pull_progress_timeout: config.image.pull_progress_timeout,
        image_max_concurrent_downloads: config.image.max_concurrent_downloads,
        image_pull_retry_count: config.image.pull_retry_count,
        image_registry_config_dir: PathBuf::from(&config.image.registry_config_dir),
        image_decryption_keys_path: PathBuf::from(&config.image.decryption_keys_path),
        image_decryption_decoder_path: config.image.decryption_decoder_path.clone(),
        image_decryption_keyprovider_config: PathBuf::from(
            &config.image.decryption_keyprovider_config,
        ),
        image_additional_artifact_stores: config
            .image
            .additional_artifact_stores
            .iter()
            .map(PathBuf::from)
            .collect(),
        image_signature_policy: PathBuf::from(&config.image.signature_policy),
        image_signature_policy_dir: PathBuf::from(&config.image.signature_policy_dir),
        image_storage_options: config.image.storage_options.clone(),
        image_external_snapshotters: config.image.external_snapshotters.clone(),
        image_volumes: config.image.image_volumes.clone(),
        image_pinned_images: {
            let mut pinned = config.image.pinned_images.clone();
            if !config.runtime.pause_image.trim().is_empty() {
                pinned.push(config.runtime.pause_image.clone());
            }
            pinned.sort();
            pinned.dedup();
            pinned
        },
        image_big_files_temporary_dir: PathBuf::from(&config.image.big_files_temporary_dir),
        image_oci_artifact_mount_support: config.image.oci_artifact_mount_support,
        workloads: config.runtime.workloads.clone(),
        enable_pod_events: config.api.enable_pod_events,
        included_pod_metrics: config.api.included_pod_metrics.clone(),
        stats_collection_period: config.api.stats_collection_period,
        pod_sandbox_metrics_collection_period: config.api.pod_sandbox_metrics_collection_period,
        grpc_max_send_msg_size: config.api.grpc_max_send_msg_size,
        grpc_max_recv_msg_size: config.api.grpc_max_recv_msg_size,
        metrics_enable: config.metrics.enable,
        metrics_host: config.metrics.host.clone(),
        metrics_port: config.metrics.port,
        metrics_socket_path: PathBuf::from(&config.metrics.socket_path),
        metrics_enable_tls: config.metrics.enable_tls,
        metrics_tls_cert_file: PathBuf::from(&config.metrics.tls_cert_file),
        metrics_tls_key_file: PathBuf::from(&config.metrics.tls_key_file),
        metrics_tls_ca_file: PathBuf::from(&config.metrics.tls_ca_file),
        metrics_tls_min_version: config.metrics.tls_min_version.clone(),
        metrics_tls_cipher_suites: config.metrics.tls_cipher_suites.clone(),
        metrics_collectors: config.metrics.collectors.clone(),
        tracing_enable: config.tracing.enable,
        tracing_endpoint: config.tracing.endpoint.clone(),
        tracing_sampling_rate_per_million: config.tracing.sampling_rate_per_million,
        monitor_env: config.runtime.monitor_env.clone(),
        monitor_cgroup: config.runtime.monitor_cgroup.clone(),
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
        default_ulimits,
        allowed_devices,
        additional_devices,
        device_ownership_from_security_context: config
            .runtime
            .device_ownership_from_security_context,
        add_inheritable_capabilities: config.runtime.add_inheritable_capabilities,
        base_runtime_spec,
        default_mounts_file: PathBuf::from(&config.runtime.default_mounts_file),
        hooks_dir: config.runtime.hooks_dir.iter().map(PathBuf::from).collect(),
        absent_mount_sources_to_reject: config
            .runtime
            .absent_mount_sources_to_reject
            .iter()
            .map(PathBuf::from)
            .collect(),
        disable_proc_mount: config.runtime.disable_proc_mount,
        timezone: config.runtime.timezone.clone(),
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
        disable_cgroup: config.runtime.disable_cgroup || effective_rootless.disable_cgroup,
        tolerate_missing_hugetlb_controller: config.runtime.tolerate_missing_hugetlb_controller
            || effective_rootless.tolerate_missing_hugetlb_controller,
        separate_pull_cgroup: config.runtime.separate_pull_cgroup.clone(),
        seccomp_profile: PathBuf::from(&config.security.seccomp_profile),
        privileged_seccomp_profile: config.security.privileged_seccomp_profile.clone(),
        unset_seccomp_profile: config.security.unset_seccomp_profile.clone(),
        apparmor_default_profile: config.security.apparmor_default_profile.clone(),
        disable_apparmor: config.security.disable_apparmor,
        enable_selinux: config.security.enable_selinux,
        selinux_category_range: if config.security.selinux_category_range == 0 {
            1024
        } else {
            config.security.selinux_category_range
        },
        hostnetwork_disable_selinux: config.security.hostnetwork_disable_selinux,
        uid_mappings: (!uid_mappings.is_empty()).then_some(uid_mappings),
        gid_mappings: (!gid_mappings.is_empty()).then_some(gid_mappings),
        minimum_mappable_uid: config.runtime.minimum_mappable_uid,
        minimum_mappable_gid: config.runtime.minimum_mappable_gid,
        io_uid: config.runtime.io_uid,
        io_gid: config.runtime.io_gid,
        pids_limit: config.runtime.pids_limit,
        infra_ctr_cpuset: config.runtime.infra_ctr_cpuset.clone(),
        shared_cpuset: config.runtime.shared_cpuset.clone(),
        exec_cpu_affinity: config.runtime.exec_cpu_affinity.clone(),
        irqbalance_config_file: PathBuf::from(&config.runtime.irqbalance_config_file),
        irqbalance_config_restore_file: config.runtime.irqbalance_config_restore_file.clone(),
        read_only: config.runtime.read_only,
        no_pivot: config.runtime.no_pivot,
        no_new_keyring: config.runtime.no_new_keyring,
        pause_image: config.runtime.pause_image.clone(),
        pause_command: config.runtime.pause_command.clone(),
        drop_infra_ctr: config.runtime.drop_infra_ctr,
        cni_config,
        local_cni_config,
        cgroup_driver: config.runtime.cgroup_driver.map(|driver| driver.as_proto()),
        exec_sync_io_drain_timeout: config.api.exec_sync_io_drain_timeout,
        max_container_log_line_size: config.logging.max_container_log_line_size,
        log_to_journald: config.runtime.log_to_journald,
        no_sync_log: config.runtime.no_sync_log,
        restrict_oom_score_adj: config.runtime.restrict_oom_score_adj,
        enable_unprivileged_ports: config.runtime.enable_unprivileged_ports,
        enable_unprivileged_icmp: config.runtime.enable_unprivileged_icmp,
        rootless: effective_rootless,
        shim: ShimConfig {
            shim_path: PathBuf::from(&config.runtime.shim_path),
            runtime_config_path: PathBuf::from(&config.runtime.runtime_config_path),
            monitor_cgroup: config.runtime.monitor_cgroup.clone(),
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
            no_new_keyring: config.runtime.no_new_keyring,
            systemd_cgroup: matches!(
                config.runtime.cgroup_driver,
                Some(CgroupDriverConfig::Systemd)
            ),
            runtime_path: PathBuf::from(&config.runtime.runtime_path),
            max_container_log_line_size: config.logging.max_container_log_line_size,
            state_db_path: PathBuf::from(&config.root).join("crius.db"),
        },
        streaming: config.api.streaming.clone(),
        config_path: Some(args.config.clone()),
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
    let image_service = runtime_service.image_service();
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

    let metrics_server = crius::metrics::server::MetricsServer::start(
        config.metrics.clone(),
        runtime_service.metrics_provider(),
        image_service.metrics_provider(),
    )
    .await?;

    // 创建gRPC服务器
    info!("Starting crius gRPC server on {}", listen);
    debug!("Using configuration: {:?}", runtime_config);
    info!(
        "Streaming server listening on {}",
        streaming_server.base_url()
    );
    if let Some(addr) = metrics_server.tcp_bind {
        info!("Metrics server listening on {}", addr);
    }
    if let Some(socket_path) = metrics_server.socket_path.as_ref() {
        info!("Metrics unix socket listening on {}", socket_path.display());
    }

    let diagnostics_state = DiagnosticsState::from_runtime(
        env!("CARGO_PKG_VERSION"),
        option_env!("GIT_COMMIT").unwrap_or("unknown"),
        &config,
        &runtime_service,
        listen
            .strip_prefix("unix://")
            .unwrap_or(&listen)
            .to_string(),
    );
    let runtime_service_server = RuntimeServiceServer::new(runtime_service.clone())
        .max_encoding_message_size(runtime_config.grpc_max_send_msg_size as usize)
        .max_decoding_message_size(runtime_config.grpc_max_recv_msg_size as usize);
    let image_service_server = ImageServiceServer::new(image_service)
        .max_encoding_message_size(runtime_config.grpc_max_send_msg_size as usize)
        .max_decoding_message_size(runtime_config.grpc_max_recv_msg_size as usize);
    let diagnostics_service_server =
        DiagnosticsServiceServer::new(DiagnosticsServiceImpl::new(diagnostics_state))
            .max_encoding_message_size(runtime_config.grpc_max_send_msg_size as usize)
            .max_decoding_message_size(runtime_config.grpc_max_recv_msg_size as usize);
    let local_service_server =
        LocalServiceServer::new(LocalServiceImpl::new(runtime_service.clone()))
            .max_encoding_message_size(runtime_config.grpc_max_send_msg_size as usize)
            .max_decoding_message_size(runtime_config.grpc_max_recv_msg_size as usize);

    let server = Server::builder()
        .add_service(runtime_service_server)
        .add_service(image_service_server)
        .add_service(diagnostics_service_server)
        .add_service(local_service_server)
        .add_service(reflection_service);

    let shutdown_watchdog = spawn_shutdown_watchdog();

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
        create_unix_socket_aliases(path, &config.api.listen_aliases)?;
        for alias in &config.api.listen_aliases {
            info!("CRI unix socket alias listening on {}", alias);
        }
        let uds_stream = UnixListenerStream::new(TokioUnixListener::from_std(uds)?);

        let serve_result = server
            .serve_with_incoming_shutdown(uds_stream, shutdown_signal())
            .await;
        shutdown_watchdog.abort();
        shutdown_runtime_service(&runtime_config.clean_shutdown_file, shutdown_nri).await;
        serve_result?;
    } else {
        let addr: SocketAddr = listen.parse()?;
        let serve_result = server.serve_with_shutdown(addr, shutdown_signal()).await;
        shutdown_watchdog.abort();
        shutdown_runtime_service(&runtime_config.clean_shutdown_file, shutdown_nri).await;
        serve_result?;
    }

    Ok(())
}

fn render_default_config_toml() -> Result<String, Error> {
    toml::to_string_pretty(&Config::default())
        .map_err(|err| anyhow::anyhow!("Failed to render default config template: {}", err))
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
            let irqbalance_status = if runtime_service
                .irqbalance_config_restore_file()
                .trim()
                .eq_ignore_ascii_case("disable")
            {
                IrqBalanceRestoreStatus {
                    attempted: false,
                    restored: false,
                    message: "disabled by runtime.irqbalance_config_restore_file=disable"
                        .to_string(),
                }
            } else if !detected_reboot {
                IrqBalanceRestoreStatus {
                    attempted: false,
                    restored: false,
                    message: "startup is not treated as reboot recovery".to_string(),
                }
            } else {
                match restore_irqbalance_config(
                    runtime_service.irqbalance_config_file(),
                    runtime_service.irqbalance_config_restore_file(),
                ) {
                    Ok(status) => status,
                    Err(err) => IrqBalanceRestoreStatus {
                        attempted: true,
                        restored: false,
                        message: format!("restore failed: {err}"),
                    },
                }
            };
            runtime_service.record_irqbalance_restore_status(irqbalance_status);
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

fn unix_socket_path(listen: &str) -> Option<&Path> {
    listen.strip_prefix("unix://").map(Path::new)
}

fn create_unix_socket_aliases(primary_socket_path: &Path, aliases: &[String]) -> Result<(), Error> {
    for alias in aliases {
        let alias_path = unix_socket_path(alias).ok_or_else(|| {
            anyhow::anyhow!("api.listen_aliases value {} must use unix://", alias)
        })?;
        if alias_path == primary_socket_path {
            continue;
        }
        if let Some(parent) = alias_path.parent() {
            fs::create_dir_all(parent)?;
        }
        match fs::symlink_metadata(alias_path) {
            Ok(metadata) => {
                if metadata.file_type().is_dir() {
                    return Err(anyhow::anyhow!(
                        "socket alias {} points to a directory",
                        alias_path.display()
                    ));
                }
                fs::remove_file(alias_path)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        symlink(primary_socket_path, alias_path)?;
    }
    Ok(())
}

fn irqbalance_banned_cpu_key(content: &str) -> Option<&'static str> {
    if content.contains("IRQBALANCE_BANNED_CPULIST") {
        Some("IRQBALANCE_BANNED_CPULIST")
    } else if content.contains("IRQBALANCE_BANNED_CPUS") {
        Some("IRQBALANCE_BANNED_CPUS")
    } else {
        None
    }
}

fn extract_irqbalance_setting(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key == "IRQBALANCE_BANNED_CPUS" || key == "IRQBALANCE_BANNED_CPULIST" {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

fn replace_irqbalance_setting(content: &str, new_value: &str) -> String {
    let key = irqbalance_banned_cpu_key(content).unwrap_or("IRQBALANCE_BANNED_CPUS");
    let mut replaced = false;
    let mut lines = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            lines.push(line.to_string());
            continue;
        }
        if let Some((current_key, _)) = trimmed.split_once('=') {
            let current_key = current_key.trim();
            if current_key == "IRQBALANCE_BANNED_CPUS" || current_key == "IRQBALANCE_BANNED_CPULIST"
            {
                lines.push(format!("{key}={new_value}"));
                replaced = true;
                continue;
            }
        }
        lines.push(line.to_string());
    }
    if !replaced {
        lines.push(format!("{key}={new_value}"));
    }
    let mut output = lines.join("\n");
    output.push('\n');
    output
}

fn smp_affinity_is_full_mask(content: &str) -> bool {
    let trimmed = content.trim().replace(',', "");
    !trimmed.is_empty() && trimmed.chars().all(|ch| matches!(ch, 'f' | 'F'))
}

fn restart_irqbalance_service() -> Result<(), Error> {
    match std::process::Command::new("systemctl")
        .args(["restart", "irqbalance"])
        .status()
    {
        Ok(status) if status.success() => return Ok(()),
        Ok(status) => {
            return Err(anyhow::anyhow!(
                "systemctl restart irqbalance exited with {status}"
            ))
        }
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err.into()),
        Err(_) => {}
    }

    match std::process::Command::new("service")
        .args(["irqbalance", "restart"])
        .status()
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(anyhow::anyhow!(
            "service irqbalance restart exited with {status}"
        )),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn restore_irqbalance_config(
    irqbalance_config_file: &Path,
    restore_file: &str,
) -> Result<IrqBalanceRestoreStatus, Error> {
    restore_irqbalance_config_with_paths(
        irqbalance_config_file,
        Path::new(restore_file),
        Path::new("/proc/irq/default_smp_affinity"),
    )
}

fn restore_irqbalance_config_with_paths(
    irqbalance_config_file: &Path,
    restore_file: &Path,
    smp_affinity_path: &Path,
) -> Result<IrqBalanceRestoreStatus, Error> {
    let current_affinity = fs::read_to_string(smp_affinity_path)?;
    if !smp_affinity_is_full_mask(&current_affinity) {
        return Ok(IrqBalanceRestoreStatus {
            attempted: true,
            restored: false,
            message: "default_smp_affinity is not fully reset; skipping restore".to_string(),
        });
    }

    if irqbalance_config_file.as_os_str().is_empty() || !irqbalance_config_file.exists() {
        return Ok(IrqBalanceRestoreStatus {
            attempted: true,
            restored: false,
            message: "irqbalance config file is not present".to_string(),
        });
    }

    let current_content = fs::read_to_string(irqbalance_config_file)?;
    let current_setting = match extract_irqbalance_setting(&current_content) {
        Some(value) => value,
        None => {
            return Ok(IrqBalanceRestoreStatus {
                attempted: true,
                restored: false,
                message: "irqbalance config does not declare a banned CPU setting".to_string(),
            })
        }
    };

    if !restore_file.exists() {
        if let Some(parent) = restore_file.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(restore_file, format!("{current_setting}\n"))?;
        return Ok(IrqBalanceRestoreStatus {
            attempted: true,
            restored: false,
            message: format!(
                "seeded irqbalance restore file at {}",
                restore_file.display()
            ),
        });
    }

    let restore_setting = fs::read_to_string(restore_file)?.trim().to_string();
    if restore_setting.is_empty() || restore_setting == current_setting {
        return Ok(IrqBalanceRestoreStatus {
            attempted: true,
            restored: false,
            message: "irqbalance banned CPU setting already matches restore file".to_string(),
        });
    }

    let updated = replace_irqbalance_setting(&current_content, &restore_setting);
    fs::write(irqbalance_config_file, updated)?;
    if let Err(err) = restart_irqbalance_service() {
        log::warn!("Failed to restart irqbalance after restore: {}", err);
    }

    Ok(IrqBalanceRestoreStatus {
        attempted: true,
        restored: true,
        message: format!(
            "restored irqbalance banned CPU setting from {}",
            restore_file.display()
        ),
    })
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

fn spawn_shutdown_watchdog() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        shutdown_signal().await;
        info!("Shutdown signal received, initiating CRI server shutdown");
        tokio::time::sleep(SERVER_SHUTDOWN_GRACE_PERIOD).await;
        warn!(
            "CRI server did not stop within {:?}; forcing process exit",
            SERVER_SHUTDOWN_GRACE_PERIOD
        );
        std::process::exit(0);
    })
}

fn apply_cli_overrides(args: &Args, config: &mut Config) {
    if let Some(listen) = &args.listen {
        config.api.listen = listen.clone();
    }
    if let Some(runtime_path) = &args.runtime_path {
        config.runtime.runtime_path = runtime_path.display().to_string();
    }
    if let Some(runtime_config_path) = &args.runtime_config_path {
        config.runtime.runtime_config_path = runtime_config_path.display().to_string();
    }
    if let Some(runtime_root) = &args.runtime_root {
        config.runtime.root = runtime_root.display().to_string();
    }
    if let Some(pause_image) = &args.pause_image {
        config.runtime.pause_image = pause_image.clone();
    }
    if !args.cni_config_dirs.is_empty() {
        config.network.config_dirs = args.cni_config_dirs.clone();
    }
    if !args.cni_plugin_dirs.is_empty() {
        config.network.plugin_dirs = args.cni_plugin_dirs.clone();
    }
    if let Some(stream_address) = &args.stream_address {
        config.api.streaming.address = stream_address.clone();
    }
    if let Some(stream_port) = args.stream_port {
        config.api.streaming.port = stream_port;
    }
    if let Some(stream_enable_tls) = args.stream_enable_tls {
        config.api.streaming.enable_tls = stream_enable_tls;
    }
    if let Some(stream_tls_cert_file) = &args.stream_tls_cert_file {
        config.api.streaming.tls_cert_file = stream_tls_cert_file.display().to_string();
    }
    if let Some(stream_tls_key_file) = &args.stream_tls_key_file {
        config.api.streaming.tls_key_file = stream_tls_key_file.display().to_string();
    }
    if let Some(stream_tls_ca_file) = &args.stream_tls_ca_file {
        config.api.streaming.tls_ca_file = stream_tls_ca_file.display().to_string();
    }
    if let Some(stream_tls_min_version) = &args.stream_tls_min_version {
        config.api.streaming.tls_min_version = stream_tls_min_version.clone();
    }
    if !args.stream_tls_cipher_suites.is_empty() {
        config.api.streaming.tls_cipher_suites = args.stream_tls_cipher_suites.clone();
    }
    if let Some(seccomp_profile) = &args.seccomp_profile {
        config.security.seccomp_profile = seccomp_profile.clone();
    }
    if let Some(apparmor_default_profile) = &args.apparmor_default_profile {
        config.security.apparmor_default_profile = apparmor_default_profile.clone();
    }
    if let Some(disable_apparmor) = args.disable_apparmor {
        config.security.disable_apparmor = disable_apparmor;
    }
    if let Some(enable_selinux) = args.enable_selinux {
        config.security.enable_selinux = enable_selinux;
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
    let fmt_layer = fmt::layer()
        .with_timer(LocalLogTimer)
        .with_file(true)
        .with_line_number(true)
        .with_writer(writer);
    let trace_layer = crius::trace_export::build_layer(&config.tracing)?;

    tracing_subscriber::registry()
        .with(base_filter)
        .with(fmt_layer)
        .with(trace_layer)
        .try_init()?;

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
    use super::restore_irqbalance_config_with_paths;
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
                    backend: "runc".to_string(),
                    backend_options: std::collections::HashMap::new(),
                    runtime_path: "/definitely/missing/runc".to_string(),
                    runtime_config_path: String::new(),
                    runtime_root: "/tmp/crius-main-test-runtime-root".to_string(),
                    platform_runtime_paths: std::collections::HashMap::new(),
                    monitor_path: "/definitely/missing/crius-shim".to_string(),
                    monitor_cgroup: String::new(),
                    monitor_env: Vec::new(),
                    stream_websockets: true,
                    allowed_annotations: Vec::new(),
                    default_annotations: std::collections::HashMap::new(),
                    privileged_without_host_devices: false,
                    privileged_without_host_devices_all_devices_allowed: false,
                    container_create_timeout: 240,
                    snapshotter: "internal-overlay-untar".to_string(),
                },
            )]),
            runtime_root: PathBuf::from("/tmp/crius-main-test-runtime-root"),
            log_dir: PathBuf::from("/tmp/crius-main-test-logs"),
            runtime_path: PathBuf::from("/definitely/missing/runc"),
            runtime_config_path: PathBuf::new(),
            image_root: PathBuf::from("/tmp/crius-main-test-images"),
            image_driver: "overlay".to_string(),
            image_global_auth_file: PathBuf::new(),
            image_namespaced_auth_dir: PathBuf::new(),
            image_default_transport: "docker://".to_string(),
            image_short_name_mode: "disabled".to_string(),
            image_pull_progress_timeout: std::time::Duration::ZERO,
            image_max_concurrent_downloads: 3,
            image_pull_retry_count: 0,
            image_registry_config_dir: PathBuf::new(),
            image_decryption_keys_path: PathBuf::new(),
            image_decryption_decoder_path: "ctd-decoder".to_string(),
            image_decryption_keyprovider_config: PathBuf::new(),
            image_additional_artifact_stores: Vec::new(),
            image_signature_policy: PathBuf::new(),
            image_signature_policy_dir: PathBuf::new(),
            image_storage_options: Vec::new(),
            image_external_snapshotters: std::collections::HashMap::new(),
            image_volumes: "mkdir".to_string(),
            image_pinned_images: Vec::new(),
            image_big_files_temporary_dir: PathBuf::new(),
            image_oci_artifact_mount_support: true,
            workloads: std::collections::HashMap::new(),
            enable_pod_events: true,
            included_pod_metrics: vec!["all".to_string()],
            stats_collection_period: 0,
            pod_sandbox_metrics_collection_period: 0,
            grpc_max_send_msg_size: 80 * 1024 * 1024,
            grpc_max_recv_msg_size: 80 * 1024 * 1024,
            metrics_enable: false,
            metrics_host: "127.0.0.1".to_string(),
            metrics_port: 9090,
            metrics_socket_path: PathBuf::new(),
            metrics_enable_tls: false,
            metrics_tls_cert_file: PathBuf::new(),
            metrics_tls_key_file: PathBuf::new(),
            metrics_tls_ca_file: PathBuf::new(),
            metrics_tls_min_version: "VersionTLS12".to_string(),
            metrics_tls_cipher_suites: Vec::new(),
            metrics_collectors: vec![
                "runtime".to_string(),
                "resources".to_string(),
                "images".to_string(),
            ],
            tracing_enable: false,
            tracing_endpoint: String::new(),
            tracing_sampling_rate_per_million: 0,
            monitor_env: Vec::new(),
            monitor_cgroup: String::new(),
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
            default_ulimits: Vec::new(),
            allowed_devices: Vec::new(),
            additional_devices: Vec::new(),
            device_ownership_from_security_context: false,
            add_inheritable_capabilities: false,
            base_runtime_spec: None,
            default_mounts_file: PathBuf::new(),
            hooks_dir: Vec::new(),
            absent_mount_sources_to_reject: Vec::new(),
            disable_proc_mount: false,
            timezone: String::new(),
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
            privileged_seccomp_profile: "unconfined".to_string(),
            unset_seccomp_profile: "runtime/default".to_string(),
            apparmor_default_profile: "crius-default".to_string(),
            disable_apparmor: false,
            enable_selinux: false,
            selinux_category_range: 1024,
            hostnetwork_disable_selinux: true,
            uid_mappings: None,
            gid_mappings: None,
            minimum_mappable_uid: -1,
            minimum_mappable_gid: -1,
            io_uid: 0,
            io_gid: 0,
            pids_limit: -1,
            infra_ctr_cpuset: String::new(),
            shared_cpuset: String::new(),
            exec_cpu_affinity: String::new(),
            irqbalance_config_file: PathBuf::new(),
            irqbalance_config_restore_file: "disable".to_string(),
            read_only: false,
            no_pivot: false,
            no_new_keyring: false,
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            pause_command: "/pause".to_string(),
            drop_infra_ctr: false,
            cni_config: CniConfig::default(),
            local_cni_config: CniConfig::default(),
            cgroup_driver: None,
            exec_sync_io_drain_timeout: std::time::Duration::ZERO,
            max_container_log_line_size: 4096,
            log_to_journald: false,
            no_sync_log: false,
            restrict_oom_score_adj: false,
            enable_unprivileged_ports: false,
            enable_unprivileged_icmp: false,
            rootless: crius::rootless::EffectiveRootlessConfig::disabled(),
            shim: ShimConfig {
                shim_path: PathBuf::from("/definitely/missing/crius-shim"),
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
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
                no_new_keyring: false,
                systemd_cgroup: false,
                runtime_path: PathBuf::from("/definitely/missing/runc"),
                max_container_log_line_size: 4096,
                state_db_path: PathBuf::from("/tmp/crius-main-test.db"),
            },
            streaming: StreamingConfig::default(),
            config_path: None,
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
            runtime_path: Some(PathBuf::from("/usr/local/bin/runc")),
            runtime_config_path: Some(PathBuf::from("/etc/crius/runtime.conf")),
            runtime_root: Some(PathBuf::from("/run/custom-crius")),
            pause_image: Some("registry.example.com/pause:4.0".to_string()),
            cni_config_dirs: vec!["/etc/cni/custom.d".to_string()],
            cni_plugin_dirs: vec!["/opt/cni/custom-bin".to_string()],
            stream_address: Some("10.0.0.10".to_string()),
            stream_port: Some(10443),
            stream_enable_tls: Some(true),
            stream_tls_cert_file: Some(PathBuf::from("/etc/crius/tls/server.crt")),
            stream_tls_key_file: Some(PathBuf::from("/etc/crius/tls/server.key")),
            stream_tls_ca_file: Some(PathBuf::from("/etc/crius/tls/ca.crt")),
            stream_tls_min_version: Some("VersionTLS13".to_string()),
            stream_tls_cipher_suites: vec![
                "TLS13_AES_256_GCM_SHA384".to_string(),
                "TLS13_AES_128_GCM_SHA256".to_string(),
            ],
            seccomp_profile: Some("/etc/crius/seccomp.json".to_string()),
            apparmor_default_profile: Some("custom-default".to_string()),
            disable_apparmor: Some(true),
            enable_selinux: Some(true),
            dump_default_config: false,
            write_default_config: None,
        };

        super::apply_cli_overrides(&args, &mut config);
        config.normalize_derived_settings();

        assert_eq!(config.api.listen, "unix:///tmp/crius.sock");
        assert_eq!(config.runtime.runtime_path, "/usr/local/bin/runc");
        assert_eq!(
            config.runtime.runtime_config_path,
            "/etc/crius/runtime.conf"
        );
        assert_eq!(config.runtime.root, "/run/custom-crius");
        assert_eq!(config.runtime.pause_image, "registry.example.com/pause:4.0");
        assert_eq!(config.runtime.shim_dir, "/run/custom-crius/shims");
        assert_eq!(config.runtime.attach_socket_dir, "/run/custom-crius/attach");
        assert_eq!(
            config.runtime.container_exits_dir,
            "/run/custom-crius/exits"
        );
        assert_eq!(
            config.runtime.clean_shutdown_file,
            "/var/lib/crius/clean.shutdown"
        );
        assert_eq!(config.network.config_dirs, vec!["/etc/cni/custom.d"]);
        assert_eq!(config.network.plugin_dirs, vec!["/opt/cni/custom-bin"]);
        assert_eq!(config.api.streaming.address, "10.0.0.10");
        assert_eq!(config.api.streaming.port, 10443);
        assert!(config.api.streaming.enable_tls);
        assert_eq!(
            config.api.streaming.tls_cert_file,
            "/etc/crius/tls/server.crt"
        );
        assert_eq!(
            config.api.streaming.tls_key_file,
            "/etc/crius/tls/server.key"
        );
        assert_eq!(config.api.streaming.tls_ca_file, "/etc/crius/tls/ca.crt");
        assert_eq!(config.api.streaming.tls_min_version, "VersionTLS13");
        assert_eq!(
            config.api.streaming.tls_cipher_suites,
            vec![
                "TLS13_AES_256_GCM_SHA384".to_string(),
                "TLS13_AES_128_GCM_SHA256".to_string()
            ]
        );
        assert_eq!(config.security.seccomp_profile, "/etc/crius/seccomp.json");
        assert_eq!(config.security.apparmor_default_profile, "custom-default");
        assert!(config.security.disable_apparmor);
        assert!(config.security.enable_selinux);
        assert_eq!(config.logging.level, "debug");
        assert_eq!(
            config.logging.file.as_deref(),
            Some("/var/log/crius/daemon.log")
        );
    }

    #[test]
    fn create_unix_socket_aliases_replaces_existing_entries_with_symlinks() {
        let dir = tempdir().unwrap();
        let primary = dir.path().join("crius.sock");
        let alias = dir.path().join("compat").join("containerd.sock");
        fs::create_dir_all(alias.parent().unwrap()).unwrap();
        fs::write(&alias, "stale").unwrap();
        fs::write(&primary, "").unwrap();

        super::create_unix_socket_aliases(&primary, &[format!("unix://{}", alias.display())])
            .unwrap();

        let metadata = fs::symlink_metadata(&alias).unwrap();
        assert!(metadata.file_type().is_symlink());
        assert_eq!(fs::read_link(&alias).unwrap(), primary);
    }

    #[test]
    fn create_unix_socket_aliases_rejects_non_unix_aliases() {
        let dir = tempdir().unwrap();
        let primary = dir.path().join("crius.sock");
        fs::write(&primary, "").unwrap();

        let err = super::create_unix_socket_aliases(&primary, &["127.0.0.1:12345".to_string()])
            .expect_err("tcp aliases must be rejected");
        assert!(err.to_string().contains("must use unix://"));
    }

    #[test]
    fn render_default_config_toml_roundtrips() {
        let rendered = super::render_default_config_toml().unwrap();
        let parsed: Config = toml::from_str(&rendered).unwrap();

        assert_eq!(parsed.api.listen, Config::default().api.listen);
        assert_eq!(
            parsed.runtime.pause_image,
            Config::default().runtime.pause_image
        );
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
            runtime_path: None,
            runtime_config_path: None,
            runtime_root: None,
            pause_image: None,
            cni_config_dirs: Vec::new(),
            cni_plugin_dirs: Vec::new(),
            stream_address: None,
            stream_port: None,
            stream_enable_tls: None,
            stream_tls_cert_file: None,
            stream_tls_key_file: None,
            stream_tls_ca_file: None,
            stream_tls_min_version: None,
            stream_tls_cipher_suites: Vec::new(),
            seccomp_profile: None,
            apparmor_default_profile: None,
            disable_apparmor: None,
            enable_selinux: None,
            dump_default_config: false,
            write_default_config: Some(output_path.clone()),
        };

        super::handle_default_config_export(&args).unwrap();

        let rendered = fs::read_to_string(output_path).unwrap();
        let parsed: Config = toml::from_str(&rendered).unwrap();
        assert_eq!(parsed.runtime.runtime_type, "runc");
    }

    #[test]
    fn restore_irqbalance_config_updates_banned_cpu_mask() {
        let dir = tempdir().unwrap();
        let irqbalance_config = dir.path().join("irqbalance.conf");
        let restore_file = dir.path().join("irqbalance.restore");
        let smp_affinity = dir.path().join("default_smp_affinity");
        fs::write(&irqbalance_config, "IRQBALANCE_BANNED_CPUS=ffff0000\n").unwrap();
        fs::write(&restore_file, "000000ff\n").unwrap();
        fs::write(&smp_affinity, "ffffffff\n").unwrap();

        let status =
            restore_irqbalance_config_with_paths(&irqbalance_config, &restore_file, &smp_affinity)
                .unwrap();

        assert!(status.attempted);
        assert!(status.restored);
        assert!(fs::read_to_string(&irqbalance_config)
            .unwrap()
            .contains("IRQBALANCE_BANNED_CPUS=000000ff"));
    }

    #[test]
    fn restore_irqbalance_config_seeds_restore_file_when_missing() {
        let dir = tempdir().unwrap();
        let irqbalance_config = dir.path().join("irqbalance.conf");
        let restore_file = dir.path().join("irqbalance.restore");
        let smp_affinity = dir.path().join("default_smp_affinity");
        fs::write(&irqbalance_config, "IRQBALANCE_BANNED_CPUS=ffff0000\n").unwrap();
        fs::write(&smp_affinity, "ffffffff\n").unwrap();

        let status =
            restore_irqbalance_config_with_paths(&irqbalance_config, &restore_file, &smp_affinity)
                .unwrap();

        assert!(status.attempted);
        assert!(!status.restored);
        assert_eq!(
            fs::read_to_string(&restore_file).unwrap().trim(),
            "ffff0000"
        );
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
