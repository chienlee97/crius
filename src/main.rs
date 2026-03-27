use std::path::{Path, PathBuf};
use std::net::SocketAddr;
use std::os::unix::net::UnixListener;
use std::fs;

use tokio::net::UnixListener as TokioUnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use clap::Parser;
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{debug, info};
use anyhow::Error;

use crate::server::{RuntimeConfig, RuntimeServiceImpl};
use crate::image::ImageServiceImpl;
use crate::proto::runtime::v1::{
    runtime_service_server::RuntimeServiceServer,
    image_service_server::ImageServiceServer,
};

mod server;
mod image;
mod proto;
mod error;

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
    
    // 创建运行时配置
    let runtime_config = RuntimeConfig {
        root_dir: PathBuf::from("/var/lib/crius"),
        runtime: "runc".to_string(),
        runtime_root: PathBuf::from("/var/run/runc"),
        log_dir: PathBuf::from("/var/log/crius"),
    };

    // 创建服务实例
    let runtime_service = RuntimeServiceImpl::new(runtime_config.clone());
    let image_service = ImageServiceImpl::new(runtime_config.root_dir.join("storage"))?;
    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")))
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
        server.serve_with_incoming(uds_stream).await?;
    } else {
        let addr: SocketAddr = args.listen.parse()?;
        server.serve(addr).await?;
    }
    
    Ok(())
}

fn init_logging() -> Result<(), Error> {
       
    let filter = EnvFilter::from_default_env()
        .add_directive("crius=info".parse()?)
        .add_directive("tower_http=info".parse()?);
    
    fmt()
        .with_file(true)
        .with_line_number(true)
        .with_writer(std::io::stderr)
        .init();
    
    Ok(())
}
