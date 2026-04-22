use std::fs;
use std::net::SocketAddr;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Error;
use clap::Parser;
use crius::config::Config;
use crius::image::ImageServiceImpl;
use crius::network::CniConfig;
use crius::proto::runtime::v1::{
    image_service_server::ImageServiceServer, runtime_service_server::RuntimeServiceServer,
};
use crius::server::{RuntimeConfig, RuntimeServiceImpl};
use crius::streaming::StreamingServer;
use tokio::net::UnixListener as TokioUnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::{debug, info};
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

    /// Listen address (IP:port or unix://path/to/socket)
    #[clap(long, default_value = "unix:///run/crius/crius.sock")]
    listen: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // 初始化日志
    init_logging()?;

    // 解析命令行参数
    let args = Args::parse();

    let file_config = match Config::load(&args.config) {
        Ok(cfg) => cfg,
        Err(err) => {
            info!(
                "Failed to load config from {}: {}. Using defaults.",
                args.config.display(),
                err
            );
            Config::default()
        }
    };

    // 创建运行时配置
    let runtime_name = file_config.runtime.runtime_type.clone();
    let mut runtime_handlers: Vec<String> = std::env::var("CRIUS_RUNTIME_HANDLERS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|handler| !handler.is_empty())
                .map(|handler| handler.to_string())
                .collect()
        })
        .unwrap_or_default();
    if !runtime_handlers
        .iter()
        .any(|handler| handler == &runtime_name)
    {
        runtime_handlers.push(runtime_name.clone());
    }

    let runtime_config = RuntimeConfig {
        root_dir: PathBuf::from(&file_config.root),
        runtime: runtime_name,
        runtime_handlers,
        runtime_root: PathBuf::from(&file_config.runtime.root),
        log_dir: PathBuf::from("/var/log/crius"),
        runtime_path: PathBuf::from(&file_config.runtime.runtime_path),
        pause_image: std::env::var("CRIUS_PAUSE_IMAGE")
            .unwrap_or_else(|_| "registry.k8s.io/pause:3.9".to_string()),
        cni_config: CniConfig::from_env(),
    };

    // 创建服务实例
    let runtime_service =
        RuntimeServiceImpl::new_with_nri_config(runtime_config.clone(), file_config.nri.clone());
    let streaming_server =
        StreamingServer::start("127.0.0.1:0", runtime_config.runtime_path.clone()).await?;
    runtime_service
        .set_streaming_server(streaming_server.clone())
        .await;

    prepare_runtime_service(&runtime_service).await;
    let shutdown_nri = runtime_service.nri_handle();
    let image_service = ImageServiceImpl::new(runtime_config.root_dir.join("storage"))?;
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
    info!("Starting crius gRPC server on {}", args.listen);
    debug!("Using configuration: {:?}", runtime_config);
    info!(
        "Streaming server listening on {}",
        streaming_server.base_url()
    );

    let server = Server::builder()
        .add_service(RuntimeServiceServer::new(runtime_service))
        .add_service(ImageServiceServer::new(image_service))
        .add_service(reflection_service);

    if args.listen.starts_with("unix://") {
        // Unix domain socket
        let socket_path = args.listen.trim_start_matches("unix://");
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
        shutdown_runtime_service(shutdown_nri).await;
        serve_result?;
    } else {
        let addr: SocketAddr = args.listen.parse()?;
        let serve_result = server.serve_with_shutdown(addr, shutdown_signal()).await;
        shutdown_runtime_service(shutdown_nri).await;
        serve_result?;
    }

    Ok(())
}

async fn prepare_runtime_service(runtime_service: &RuntimeServiceImpl) {
    info!("Recovering state from database...");
    if let Err(e) = runtime_service.recover_state().await {
        log::error!("Failed to recover state: {}", e);
    }
    if let Err(e) = runtime_service.initialize_nri().await {
        log::error!("Failed to initialize NRI: {}", e);
    }
}

async fn shutdown_runtime_service(nri: Arc<dyn crius::nri::NriApi>) {
    if let Err(err) = nri.shutdown().await {
        log::error!("Failed to shutdown NRI: {}", err);
    }
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

fn init_logging() -> Result<(), Error> {
    let filter = EnvFilter::from_default_env()
        .add_directive("crius=info".parse()?)
        .add_directive("tower_http=info".parse()?);

    fmt()
        .with_env_filter(filter)
        .with_timer(LocalLogTimer)
        .with_file(true)
        .with_line_number(true)
        .with_writer(std::io::stderr)
        .init();

    Ok(())
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
    use super::LocalLogTimer;
    use super::RuntimeConfig;
    use crius::config::NriConfig;
    use crius::network::CniConfig;
    use crius::nri::NriApi;
    use crius::server::RuntimeServiceImpl;
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
            runtime_root: PathBuf::from("/tmp/crius-main-test-runtime-root"),
            log_dir: PathBuf::from("/tmp/crius-main-test-logs"),
            runtime_path: PathBuf::from("/definitely/missing/runc"),
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            cni_config: CniConfig::default(),
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
    async fn shutdown_runtime_service_invokes_nri_shutdown() {
        let fake_nri = Arc::new(FakeNri::default());

        shutdown_runtime_service(fake_nri.clone()).await;

        assert_eq!(fake_nri.calls.lock().await.clone(), vec!["shutdown"]);
    }
}
