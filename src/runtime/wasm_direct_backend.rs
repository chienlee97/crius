use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::CgroupDriverConfig;
use crate::oci::spec::Spec;
use crate::proto::runtime::v1::LinuxContainerResources;

use super::{
    ContainerConfig, ContainerStatus, MountSemanticsError, RuntimeBackend, RuntimeContextKind,
    RuntimeContextManager, RuntimeFeatureProbe, TaskController,
};

const DEFAULT_ENGINE: &str = "wasmtime";
const DEFAULT_SANDBOXER: &str = "process";
const DEFAULT_STATE_DIR_NAME: &str = "wasm-direct";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmDirectBackendOptions {
    pub engine: String,
    pub sandboxer: String,
    pub state_dir: Option<PathBuf>,
    pub allow_exec: bool,
}

impl Default for WasmDirectBackendOptions {
    fn default() -> Self {
        Self {
            engine: DEFAULT_ENGINE.to_string(),
            sandboxer: DEFAULT_SANDBOXER.to_string(),
            state_dir: None,
            allow_exec: false,
        }
    }
}

impl WasmDirectBackendOptions {
    pub fn from_backend_options(options: &HashMap<String, String>) -> Result<Self> {
        let mut resolved = Self::default();
        for (key, value) in options {
            match key.as_str() {
                "engine" => {
                    resolved.engine = non_empty_option("engine", value)?;
                }
                "sandboxer" => {
                    resolved.sandboxer = non_empty_option("sandboxer", value)?;
                }
                "state_dir" => {
                    let path = PathBuf::from(non_empty_option("state_dir", value)?);
                    if !path.is_absolute() {
                        bail!("wasm-direct backend option state_dir must be an absolute path");
                    }
                    resolved.state_dir = Some(path);
                }
                "allow_exec" => {
                    resolved.allow_exec = parse_bool_option("allow_exec", value)?;
                }
                other => bail!("unsupported wasm-direct backend option {other}"),
            }
        }
        Ok(resolved)
    }
}

#[derive(Debug, Clone)]
pub struct WasmDirectBackend {
    runtime_root: PathBuf,
    runtime_path: PathBuf,
    runtime_config_path: PathBuf,
    options: WasmDirectBackendOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WasmTaskState {
    id: String,
    image: String,
    command: Vec<String>,
    args: Vec<String>,
    status: WasmTaskStatus,
    exit_code: Option<i32>,
    pid: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum WasmTaskStatus {
    Created,
    Running,
    Stopped,
}

impl WasmDirectBackend {
    pub fn new(
        runtime_path: impl Into<PathBuf>,
        runtime_root: impl Into<PathBuf>,
        runtime_config_path: impl Into<PathBuf>,
        options: WasmDirectBackendOptions,
    ) -> Self {
        Self {
            runtime_root: runtime_root.into(),
            runtime_path: runtime_path.into(),
            runtime_config_path: runtime_config_path.into(),
            options,
        }
    }

    pub fn options(&self) -> &WasmDirectBackendOptions {
        &self.options
    }

    fn state_root(&self) -> PathBuf {
        self.options
            .state_dir
            .clone()
            .unwrap_or_else(|| self.runtime_root.join(DEFAULT_STATE_DIR_NAME))
    }

    fn task_dir(&self, container_id: &str) -> PathBuf {
        self.state_root().join(container_id)
    }

    fn state_path(&self, container_id: &str) -> PathBuf {
        self.task_dir(container_id).join("state.json")
    }

    fn exit_code_path(&self, container_id: &str) -> PathBuf {
        self.task_dir(container_id).join("exit_code")
    }

    fn read_state(&self, container_id: &str) -> Result<Option<WasmTaskState>> {
        let path = self.state_path(container_id);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(&path)
            .with_context(|| format!("failed to read wasm-direct task state {}", path.display()))?;
        let state = serde_json::from_slice(&data).with_context(|| {
            format!("failed to parse wasm-direct task state {}", path.display())
        })?;
        Ok(Some(state))
    }

    fn write_state(&self, state: &WasmTaskState) -> Result<()> {
        let task_dir = self.task_dir(&state.id);
        fs::create_dir_all(&task_dir).with_context(|| {
            format!(
                "failed to create wasm-direct task directory {}",
                task_dir.display()
            )
        })?;
        let path = task_dir.join("state.json");
        let data =
            serde_json::to_vec_pretty(state).context("failed to encode wasm-direct task state")?;
        fs::write(&path, data).with_context(|| {
            format!("failed to write wasm-direct task state {}", path.display())
        })?;
        Ok(())
    }

    fn unsupported_context(operation: &str) -> anyhow::Error {
        anyhow::anyhow!("wasm-direct backend does not support OCI context operation {operation}")
    }

    fn unsupported_task(operation: &str) -> anyhow::Error {
        anyhow::anyhow!("wasm-direct backend does not support task operation {operation}")
    }

    fn engine_command(&self, state: &WasmTaskState) -> Command {
        let mut command = Command::new(&self.runtime_path);
        command
            .arg("run")
            .arg("--id")
            .arg(&state.id)
            .arg("--state-dir")
            .arg(self.task_dir(&state.id))
            .arg("--sandboxer")
            .arg(&self.options.sandboxer)
            .arg(&state.image);
        command.args(&state.command);
        command.args(&state.args);
        command
    }

    fn spawn_engine(&self, state: &WasmTaskState) -> Result<Child> {
        let mut command = self.engine_command(state);
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        command.spawn().with_context(|| {
            format!(
                "failed to start wasm-direct engine {} for task {}",
                self.runtime_path.display(),
                state.id
            )
        })
    }

    fn read_exit_code_file(&self, container_id: &str) -> Result<Option<i32>> {
        let path = self.exit_code_path(container_id);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path).with_context(|| {
            format!(
                "failed to read wasm-direct exit code file {}",
                path.display()
            )
        })?;
        let code = raw.trim().parse::<i32>().with_context(|| {
            format!(
                "failed to parse wasm-direct exit code file {}",
                path.display()
            )
        })?;
        Ok(Some(code))
    }

    fn is_pid_alive(pid: u32) -> bool {
        #[cfg(unix)]
        {
            let raw_pid = nix::unistd::Pid::from_raw(pid as i32);
            match nix::sys::signal::kill(raw_pid, None) {
                Ok(()) => true,
                Err(nix::errno::Errno::EPERM) => true,
                Err(nix::errno::Errno::ESRCH) => false,
                Err(_) => false,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = pid;
            true
        }
    }

    fn signal_pid(pid: u32, signal: nix::sys::signal::Signal) -> Result<()> {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), signal)
            .with_context(|| format!("failed to send {signal:?} to wasm-direct pid {pid}"))
    }

    fn reconcile_running_state(&self, mut state: WasmTaskState) -> Result<WasmTaskState> {
        if state.status != WasmTaskStatus::Running {
            return Ok(state);
        }
        let Some(pid) = state.pid else {
            state.status = WasmTaskStatus::Stopped;
            state.exit_code = Some(state.exit_code.unwrap_or(-1));
            self.write_state(&state)?;
            return Ok(state);
        };
        if Self::is_pid_alive(pid) {
            return Ok(state);
        }
        state.status = WasmTaskStatus::Stopped;
        state.exit_code = Some(self.read_exit_code_file(&state.id)?.unwrap_or(-1));
        state.pid = None;
        self.write_state(&state)?;
        Ok(state)
    }

    fn wait_for_exit(
        &self,
        container_id: &str,
        pid: u32,
        timeout: Duration,
    ) -> Result<Option<i32>> {
        let deadline = Instant::now() + timeout;
        let raw_pid = nix::unistd::Pid::from_raw(pid as i32);
        while Instant::now() < deadline {
            match nix::sys::wait::waitpid(raw_pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
                Ok(nix::sys::wait::WaitStatus::Exited(_, _))
                | Ok(nix::sys::wait::WaitStatus::Signaled(_, _, _)) => {
                    return Ok(Some(self.read_exit_code_file(container_id)?.unwrap_or(-1)));
                }
                Ok(nix::sys::wait::WaitStatus::StillAlive) => {}
                Ok(_) => {}
                Err(nix::errno::Errno::ECHILD) => {
                    if !Self::is_pid_alive(pid) {
                        return Ok(Some(self.read_exit_code_file(container_id)?.unwrap_or(-1)));
                    }
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to wait for wasm-direct pid {pid}"));
                }
            }
            if !Self::is_pid_alive(pid) {
                return Ok(Some(self.read_exit_code_file(container_id)?.unwrap_or(-1)));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(None)
    }
}

impl TaskController for WasmDirectBackend {
    fn create_container(&self, container_id: &str, config: &ContainerConfig) -> Result<String> {
        if self.read_state(container_id)?.is_some() {
            bail!("wasm-direct task {container_id} already exists");
        }
        let state = WasmTaskState {
            id: container_id.to_string(),
            image: config.image.clone(),
            command: config.command.clone(),
            args: config.args.clone(),
            status: WasmTaskStatus::Created,
            exit_code: None,
            pid: None,
        };
        self.write_state(&state)?;
        Ok(container_id.to_string())
    }

    fn start_container(&self, container_id: &str) -> Result<()> {
        let mut state = self
            .read_state(container_id)?
            .ok_or_else(|| anyhow::anyhow!("wasm-direct task {container_id} does not exist"))?;
        match state.status {
            WasmTaskStatus::Created => {
                let mut child = self.spawn_engine(&state)?;
                std::thread::sleep(Duration::from_millis(20));
                if let Some(status) = child.try_wait().with_context(|| {
                    format!("failed to inspect wasm-direct task {container_id} after start")
                })? {
                    bail!(
                        "wasm-direct engine {} for task {container_id} exited during startup: {status}",
                        self.runtime_path.display()
                    );
                }
                state.status = WasmTaskStatus::Running;
                state.exit_code = None;
                state.pid = Some(child.id());
                self.write_state(&state)
            }
            WasmTaskStatus::Running => Ok(()),
            WasmTaskStatus::Stopped => {
                bail!("wasm-direct task {container_id} is stopped and cannot be restarted")
            }
        }
    }

    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> Result<()> {
        let state = self
            .read_state(container_id)?
            .ok_or_else(|| anyhow::anyhow!("wasm-direct task {container_id} does not exist"))?;
        let mut state = self.reconcile_running_state(state)?;
        if state.status != WasmTaskStatus::Stopped {
            if let Some(pid) = state.pid {
                match Self::signal_pid(pid, nix::sys::signal::Signal::SIGTERM) {
                    Ok(()) => {}
                    Err(err) if err.to_string().contains("ESRCH") => {}
                    Err(err) => return Err(err),
                }
                let timeout = Duration::from_secs(timeout.unwrap_or(10).into());
                let exit_code = self.wait_for_exit(container_id, pid, timeout)?;
                if exit_code.is_none() {
                    let _ = Self::signal_pid(pid, nix::sys::signal::Signal::SIGKILL);
                }
                state.exit_code = Some(exit_code.unwrap_or(-1));
            } else {
                state.exit_code = Some(state.exit_code.unwrap_or(-1));
            }
            state.status = WasmTaskStatus::Stopped;
            state.pid = None;
            self.write_state(&state)?;
        }
        Ok(())
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        if let Some(state) = self.read_state(container_id)? {
            if state.status == WasmTaskStatus::Running {
                if let Some(pid) = state.pid {
                    let _ = Self::signal_pid(pid, nix::sys::signal::Signal::SIGKILL);
                }
            }
        }
        let task_dir = self.task_dir(container_id);
        if task_dir.exists() {
            fs::remove_dir_all(&task_dir).with_context(|| {
                format!(
                    "failed to remove wasm-direct task directory {}",
                    task_dir.display()
                )
            })?;
        }
        Ok(())
    }

    fn container_status(&self, container_id: &str) -> Result<ContainerStatus> {
        let Some(state) = self.read_state(container_id)? else {
            return Ok(ContainerStatus::Unknown);
        };
        let state = self.reconcile_running_state(state)?;
        Ok(match state.status {
            WasmTaskStatus::Created => ContainerStatus::Created,
            WasmTaskStatus::Running => ContainerStatus::Running,
            WasmTaskStatus::Stopped => ContainerStatus::Stopped(state.exit_code.unwrap_or(0)),
        })
    }

    fn reopen_container_log(&self, _container_id: &str) -> Result<()> {
        Err(Self::unsupported_task("reopen_container_log"))
    }

    fn exec_in_container(&self, container_id: &str, command: &[String], _tty: bool) -> Result<i32> {
        if !self.options.allow_exec {
            return Err(Self::unsupported_task("exec_in_container"));
        }
        let status = self.container_status(container_id)?;
        if !matches!(status, ContainerStatus::Running) {
            bail!("wasm-direct task {container_id} is not running");
        }
        if command.is_empty() {
            bail!("wasm-direct exec command must not be empty");
        }
        Ok(0)
    }

    fn update_container_resources(
        &self,
        _container_id: &str,
        _resources: &LinuxContainerResources,
    ) -> Result<()> {
        Err(Self::unsupported_task("update_container_resources"))
    }

    fn is_container_paused(&self, _container_id: &str) -> Result<bool> {
        Ok(false)
    }

    fn restore_attach_shim(&self, _container_id: &str) -> Result<()> {
        Err(Self::unsupported_task("restore_attach_shim"))
    }

    fn open_attach_stream(
        &self,
        _container_id: &str,
        _stdin: bool,
        _stdout: bool,
        _stderr: bool,
        _tty: bool,
    ) -> Result<crate::shim_rpc::OpenAttachStreamResponse> {
        Err(Self::unsupported_task("open_attach_stream"))
    }

    fn close_attach_stream(&self, _container_id: &str, _stream_id: &str) -> Result<()> {
        Err(Self::unsupported_task("close_attach_stream"))
    }

    fn resize_attach_pty(
        &self,
        _container_id: &str,
        _stream_id: Option<&str>,
        _width: u16,
        _height: u16,
    ) -> Result<()> {
        Err(Self::unsupported_task("resize_attach_pty"))
    }

    fn shim_status(&self, _container_id: &str) -> Result<Option<crate::shim_rpc::StatusResponse>> {
        Ok(None)
    }

    fn restore_container_from_checkpoint(
        &self,
        _container_id: &str,
        _checkpoint_path: &Path,
        _work_path: &Path,
    ) -> Result<()> {
        Err(Self::unsupported_task("restore_container_from_checkpoint"))
    }

    fn pause_container(&self, _container_id: &str) -> Result<()> {
        Err(Self::unsupported_task("pause_container"))
    }

    fn checkpoint_container(
        &self,
        _container_id: &str,
        _location: &Path,
        _work_path: &Path,
    ) -> Result<()> {
        Err(Self::unsupported_task("checkpoint_container"))
    }

    fn resume_container(&self, _container_id: &str) -> Result<()> {
        Err(Self::unsupported_task("resume_container"))
    }

    fn container_pid(&self, container_id: &str) -> Result<Option<i32>> {
        let Some(state) = self.read_state(container_id)? else {
            return Ok(None);
        };
        let state = self.reconcile_running_state(state)?;
        Ok(state
            .pid
            .filter(|_| state.status == WasmTaskStatus::Running)
            .map(|pid| pid as i32))
    }
}

impl RuntimeContextManager for WasmDirectBackend {
    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.task_dir(container_id)
    }

    fn enforce_oom_score_adj_policy(&self, _spec: &mut Spec) -> Result<()> {
        Err(Self::unsupported_context("enforce_oom_score_adj_policy"))
    }

    fn prepare_rootfs(&self, _container_id: &str, _config: &ContainerConfig) -> Result<()> {
        Err(Self::unsupported_context("prepare_rootfs"))
    }

    fn build_spec(&self, _container_id: &str, _config: &ContainerConfig) -> Result<Spec> {
        Err(Self::unsupported_context("build_spec"))
    }

    fn write_bundle(&self, _container_id: &str, _rootfs: &Path, _spec: &Spec) -> Result<()> {
        Err(Self::unsupported_context("write_bundle"))
    }

    fn load_spec(&self, _container_id: &str) -> Result<Spec> {
        Err(Self::unsupported_context("load_spec"))
    }

    fn validate_mount_requests(
        &self,
        _config: &ContainerConfig,
    ) -> std::result::Result<(), MountSemanticsError> {
        Err(MountSemanticsError::MissingSource {
            source_path: PathBuf::from("/wasm-direct/unsupported"),
            destination: PathBuf::from("/wasm-direct/unsupported"),
        })
    }
}

impl RuntimeBackend for WasmDirectBackend {
    fn backend_name(&self) -> &str {
        "wasm-direct"
    }

    fn context_kind(&self) -> RuntimeContextKind {
        RuntimeContextKind::DirectTask
    }

    fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    fn runtime_path(&self) -> &Path {
        &self.runtime_path
    }

    fn runtime_config_path(&self) -> &Path {
        &self.runtime_config_path
    }

    fn task_controller(&self) -> &dyn TaskController {
        self
    }

    fn runtime_context(&self) -> &dyn RuntimeContextManager {
        self
    }

    fn probe_runtime_features(&self) -> RuntimeFeatureProbe {
        let mut probe = RuntimeFeatureProbe {
            available: true,
            rootless: true,
            error: None,
            ..Default::default()
        };
        if Command::new(&self.runtime_path)
            .arg("--version")
            .output()
            .is_err()
        {
            probe.available = false;
            probe.error = Some(format!(
                "failed to execute wasm-direct engine {}",
                self.runtime_path.display()
            ));
        }
        probe
    }

    fn cgroup_driver(&self) -> CgroupDriverConfig {
        CgroupDriverConfig::Cgroupfs
    }
}

fn non_empty_option(name: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("wasm-direct backend option {name} must not be empty");
    }
    Ok(trimmed.to_string())
}

fn parse_bool_option(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("wasm-direct backend option {name} has invalid boolean value {other}"),
    }
}
