//! crius-shim - OCI容器运行时shim
//!
//! Shim是crius daemon和runc之间的中间层，负责：
//! 1. 容器进程生命周期管理（创建、启动、监控）
//! 2. 子进程收割（subreaper）
//! 3. 容器退出码跟踪
//! 4. IO流管理（stdin/stdout/stderr重定向）
//! 5. 信号传递

use anyhow::{Context, Result};
use clap::Parser;
use crius::shim::{Daemon, DaemonOptions};
use log::{debug, info};
use std::fs;
use std::path::PathBuf;

/// crius-shim - OCI runtime shim for crius
#[derive(Parser, Debug)]
#[clap(version, about)]
struct Args {
    /// Container ID
    #[clap(short, long)]
    id: String,

    /// Bundle directory path
    #[clap(short, long)]
    bundle: PathBuf,

    /// Runtime path (runc binary)
    #[clap(short, long, default_value = "runc")]
    runtime: PathBuf,

    /// Optional runtime-specific configuration file path
    #[clap(long)]
    runtime_config_path: Option<PathBuf>,

    /// Target cgroup for the shim/monitor process
    #[clap(long)]
    monitor_cgroup: Option<String>,

    /// Debug mode
    #[clap(short, long)]
    debug: bool,

    /// Duplicate container output to journald in addition to CRI log file
    #[clap(long)]
    log_to_journald: bool,

    /// Skip syncing CRI log files on reopen and container exit
    #[clap(long)]
    no_sync_log: bool,

    /// Disable pivot_root and use MS_MOVE instead
    #[clap(long)]
    no_pivot: bool,

    /// Do not create a new session keyring for the container
    #[clap(long)]
    no_new_keyring: bool,

    /// Start the runtime with systemd cgroup support
    #[clap(long)]
    systemd_cgroup: bool,

    /// Log file path
    #[clap(short, long)]
    log: Option<PathBuf>,

    /// Exit code file path
    #[clap(long)]
    exit_code_file: Option<PathBuf>,

    /// Attach/resize socket root directory
    #[clap(long)]
    attach_socket_dir: Option<PathBuf>,

    /// Owner UID for shim-created host IO artifacts
    #[clap(long, default_value_t = 0)]
    io_uid: u32,

    /// Owner GID for shim-created host IO artifacts
    #[clap(long, default_value_t = 0)]
    io_gid: u32,

    /// Maximum CRI container log line size in bytes
    #[clap(long, default_value_t = 4096)]
    max_container_log_line_size: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // 初始化日志
    init_logging(args.debug, args.log.as_ref())?;

    info!("crius-shim starting for container {}", args.id);
    debug!("Bundle: {:?}", args.bundle);
    debug!("Runtime: {:?}", args.runtime);

    // 验证bundle目录
    if !args.bundle.exists() {
        return Err(anyhow::anyhow!(
            "Bundle directory does not exist: {:?}",
            args.bundle
        ));
    }

    // 验证config.json
    let config_path = args.bundle.join("config.json");
    if !config_path.exists() {
        return Err(anyhow::anyhow!(
            "config.json not found in bundle: {:?}",
            config_path
        ));
    }

    // rootfs 不再要求位于 bundle/rootfs，实际路径以 OCI config.json 的 root.path 为准。

    // 创建并运行shim守护进程
    let daemon = Daemon::new(
        args.id,
        args.bundle,
        args.runtime,
        DaemonOptions {
            runtime_config_path: args.runtime_config_path.unwrap_or_default(),
            monitor_cgroup: args.monitor_cgroup.unwrap_or_default(),
            exit_code_file: args.exit_code_file,
            attach_socket_dir: args.attach_socket_dir,
            io_uid: args.io_uid,
            io_gid: args.io_gid,
            max_container_log_line_size: args.max_container_log_line_size,
            log_to_journald: args.log_to_journald,
            no_sync_log: args.no_sync_log,
            no_pivot: args.no_pivot,
            no_new_keyring: args.no_new_keyring,
            systemd_cgroup: args.systemd_cgroup,
        },
    );

    daemon.run()
}

fn init_logging(debug: bool, log_file: Option<&PathBuf>) -> Result<()> {
    let level = if debug {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    if let Some(path) = log_file {
        // 文件日志
        let parent = path.parent().context("Invalid log file path")?;
        fs::create_dir_all(parent)?;

        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!(
                    "[{} {}] {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    message
                ))
            })
            .level(level)
            .chain(fern::log_file(path)?)
            .apply()?;
    } else {
        // stderr日志 - 使用简单格式
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("[{}] {}", record.level(), message))
            })
            .level(level)
            .chain(std::io::stderr())
            .apply()?;
    }

    Ok(())
}
