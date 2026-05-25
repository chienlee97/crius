//! Shared fake implementations for integration tests.
//!
//! These helpers are intentionally small and deterministic. They provide common
//! stand-ins for runtime, image storage and shim RPC boundaries without requiring
//! runc, a real rootfs or a long-running shim process.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crius::config::CgroupDriverConfig;
use crius::image::content_store::{BlobHandle, BlobInfo, ContentStore, FsContentStore};
use crius::image::snapshotter::{
    MountView, PreparedSnapshot, SnapshotInfo, SnapshotState, SnapshotUsage, Snapshotter,
};
use crius::oci::spec::Spec;
use crius::runtime::{
    ContainerConfig, ContainerStatus, MountSemanticsError, RuntimeBackend, RuntimeContextKind,
    RuntimeContextManager, RuntimeFeatureProbe, TaskController,
};
use crius::shim_rpc::{
    server::{serve, ShimRpcHandler},
    ShimRpcClient, ShimRpcRequest, ShimRpcResponse, StatusResponse, TaskState,
};

#[derive(Debug, Clone)]
pub struct FakeRuntimeBackend {
    backend_name: &'static str,
    runtime_root: PathBuf,
    runtime_path: PathBuf,
    runtime_config_path: PathBuf,
    state: Arc<Mutex<FakeRuntimeState>>,
}

#[derive(Debug, Clone)]
struct FakeRuntimeState {
    status: ContainerStatus,
    statuses: HashMap<String, ContainerStatus>,
    calls: Vec<FakeRuntimeCall>,
    failures: HashMap<FakeRuntimeOperation, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeRuntimeCall {
    CreateContainer {
        container_id: String,
    },
    StartContainer {
        container_id: String,
    },
    StopContainer {
        container_id: String,
        timeout: Option<u32>,
    },
    RemoveContainer {
        container_id: String,
    },
    ContainerStatus {
        container_id: String,
    },
    PrepareRootfs {
        container_id: String,
    },
    BuildSpec {
        container_id: String,
    },
    WriteBundle {
        container_id: String,
        rootfs: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FakeRuntimeOperation {
    CreateContainer,
    StartContainer,
    StopContainer,
    RemoveContainer,
    ContainerStatus,
    PrepareRootfs,
    BuildSpec,
    WriteBundle,
}

impl FakeRuntimeBackend {
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            backend_name: "fake",
            runtime_root: runtime_root.into(),
            runtime_path: PathBuf::from("/fake/runtime"),
            runtime_config_path: PathBuf::from("/fake/runtime.conf"),
            state: Arc::new(Mutex::new(FakeRuntimeState {
                status: ContainerStatus::Created,
                statuses: HashMap::new(),
                calls: Vec::new(),
                failures: HashMap::new(),
            })),
        }
    }

    pub fn with_status(self, status: ContainerStatus) -> Self {
        self.set_status(status);
        self
    }

    pub fn with_backend_name(mut self, backend_name: &'static str) -> Self {
        self.backend_name = backend_name;
        self
    }

    pub fn with_failure(self, operation: FakeRuntimeOperation, message: impl Into<String>) -> Self {
        self.fail_operation(operation, message);
        self
    }

    pub fn fail_operation(&self, operation: FakeRuntimeOperation, message: impl Into<String>) {
        if let Ok(mut state) = self.state.lock() {
            state.failures.insert(operation, message.into());
        }
    }

    pub fn clear_failures(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.failures.clear();
        }
    }

    pub fn set_status(&self, status: ContainerStatus) {
        if let Ok(mut state) = self.state.lock() {
            state.status = status;
        }
    }

    pub fn status_snapshot(&self) -> ContainerStatus {
        self.state
            .lock()
            .map(|state| state.status.clone())
            .unwrap_or(ContainerStatus::Unknown)
    }

    pub fn calls(&self) -> Vec<FakeRuntimeCall> {
        self.state
            .lock()
            .map(|state| state.calls.clone())
            .unwrap_or_default()
    }

    fn record(&self, call: FakeRuntimeCall) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("fake runtime backend state lock poisoned"))?;
        state.calls.push(call);
        Ok(())
    }

    fn maybe_fail(&self, operation: FakeRuntimeOperation) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("fake runtime backend state lock poisoned"))?;
        if let Some(message) = state.failures.get(&operation) {
            return Err(anyhow::anyhow!("{}", message));
        }
        Ok(())
    }

    fn transition(&self, container_id: &str, status: ContainerStatus) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("fake runtime backend state lock poisoned"))?;
        state.status = status.clone();
        state.statuses.insert(container_id.to_string(), status);
        Ok(())
    }
}

impl TaskController for FakeRuntimeBackend {
    fn create_container(&self, container_id: &str, _config: &ContainerConfig) -> Result<String> {
        self.record(FakeRuntimeCall::CreateContainer {
            container_id: container_id.to_string(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::CreateContainer)?;
        self.transition(container_id, ContainerStatus::Created)?;
        Ok("fake-created".to_string())
    }

    fn start_container(&self, container_id: &str) -> Result<()> {
        self.record(FakeRuntimeCall::StartContainer {
            container_id: container_id.to_string(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::StartContainer)?;
        self.transition(container_id, ContainerStatus::Running)?;
        Ok(())
    }

    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> Result<()> {
        self.record(FakeRuntimeCall::StopContainer {
            container_id: container_id.to_string(),
            timeout,
        })?;
        self.maybe_fail(FakeRuntimeOperation::StopContainer)?;
        self.transition(container_id, ContainerStatus::Stopped(0))?;
        Ok(())
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        self.record(FakeRuntimeCall::RemoveContainer {
            container_id: container_id.to_string(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::RemoveContainer)?;
        self.transition(container_id, ContainerStatus::Unknown)?;
        Ok(())
    }

    fn container_status(&self, container_id: &str) -> Result<ContainerStatus> {
        self.record(FakeRuntimeCall::ContainerStatus {
            container_id: container_id.to_string(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::ContainerStatus)?;
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("fake runtime backend state lock poisoned"))?;
        Ok(state
            .statuses
            .get(container_id)
            .cloned()
            .unwrap_or_else(|| {
                if state.statuses.is_empty() {
                    state.status.clone()
                } else {
                    ContainerStatus::Unknown
                }
            }))
    }

    fn reopen_container_log(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn exec_in_container(
        &self,
        _container_id: &str,
        _command: &[String],
        _tty: bool,
    ) -> Result<i32> {
        Ok(0)
    }

    fn update_container_resources(
        &self,
        _container_id: &str,
        _resources: &crius::proto::runtime::v1::LinuxContainerResources,
    ) -> Result<()> {
        Ok(())
    }

    fn is_container_paused(&self, _container_id: &str) -> Result<bool> {
        Ok(false)
    }

    fn restore_attach_shim(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn shim_status(&self, _container_id: &str) -> Result<Option<crius::shim_rpc::StatusResponse>> {
        Ok(None)
    }

    fn restore_container_from_checkpoint(
        &self,
        _container_id: &str,
        _checkpoint_path: &Path,
        _work_path: &Path,
    ) -> Result<()> {
        Ok(())
    }

    fn pause_container(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn checkpoint_container(
        &self,
        _container_id: &str,
        _location: &Path,
        _work_path: &Path,
    ) -> Result<()> {
        Ok(())
    }

    fn resume_container(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn container_pid(&self, _container_id: &str) -> Result<Option<i32>> {
        Ok(Some(1))
    }
}

impl RuntimeContextManager for FakeRuntimeBackend {
    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.runtime_root.join(container_id)
    }

    fn enforce_oom_score_adj_policy(&self, _spec: &mut Spec) -> Result<()> {
        Ok(())
    }

    fn prepare_rootfs(&self, container_id: &str, _config: &ContainerConfig) -> Result<()> {
        self.record(FakeRuntimeCall::PrepareRootfs {
            container_id: container_id.to_string(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::PrepareRootfs)?;
        Ok(())
    }

    fn build_spec(&self, container_id: &str, _config: &ContainerConfig) -> Result<Spec> {
        self.record(FakeRuntimeCall::BuildSpec {
            container_id: container_id.to_string(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::BuildSpec)?;
        Ok(Spec::new("1.0.2"))
    }

    fn write_bundle(&self, container_id: &str, rootfs: &Path, _spec: &Spec) -> Result<()> {
        self.record(FakeRuntimeCall::WriteBundle {
            container_id: container_id.to_string(),
            rootfs: rootfs.to_path_buf(),
        })?;
        self.maybe_fail(FakeRuntimeOperation::WriteBundle)?;
        let bundle_dir = self.bundle_path_for(container_id);
        std::fs::create_dir_all(&bundle_dir)
            .with_context(|| format!("failed to create fake bundle {}", bundle_dir.display()))?;
        std::fs::write(bundle_dir.join("config.json"), serde_json::to_vec(_spec)?)
            .with_context(|| format!("failed to write fake bundle {}", bundle_dir.display()))?;
        Ok(())
    }

    fn load_spec(&self, _container_id: &str) -> Result<Spec> {
        Ok(Spec::new("1.0.2"))
    }

    fn validate_mount_requests(
        &self,
        _config: &ContainerConfig,
    ) -> std::result::Result<(), MountSemanticsError> {
        Ok(())
    }
}

impl RuntimeBackend for FakeRuntimeBackend {
    fn backend_name(&self) -> &str {
        self.backend_name
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
        RuntimeFeatureProbe {
            available: true,
            ..Default::default()
        }
    }

    fn cgroup_driver(&self) -> CgroupDriverConfig {
        CgroupDriverConfig::Cgroupfs
    }
}

#[derive(Debug, Clone)]
pub struct DirectTaskRuntimeBackend {
    inner: FakeRuntimeBackend,
}

impl DirectTaskRuntimeBackend {
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            inner: FakeRuntimeBackend::new(runtime_root).with_backend_name("direct-task"),
        }
    }

    pub fn calls(&self) -> Vec<FakeRuntimeCall> {
        self.inner.calls()
    }

    fn unsupported_context(operation: &str) -> anyhow::Error {
        anyhow::anyhow!("direct-task backend must not use OCI context operation {operation}")
    }
}

impl TaskController for DirectTaskRuntimeBackend {
    fn create_container(&self, container_id: &str, config: &ContainerConfig) -> Result<String> {
        self.inner.create_container(container_id, config)
    }

    fn start_container(&self, container_id: &str) -> Result<()> {
        self.inner.start_container(container_id)
    }

    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> Result<()> {
        self.inner.stop_container(container_id, timeout)
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        self.inner.remove_container(container_id)
    }

    fn container_status(&self, container_id: &str) -> Result<ContainerStatus> {
        self.inner.container_status(container_id)
    }

    fn reopen_container_log(&self, container_id: &str) -> Result<()> {
        self.inner.reopen_container_log(container_id)
    }

    fn exec_in_container(&self, container_id: &str, command: &[String], tty: bool) -> Result<i32> {
        self.inner.exec_in_container(container_id, command, tty)
    }

    fn update_container_resources(
        &self,
        container_id: &str,
        resources: &crius::proto::runtime::v1::LinuxContainerResources,
    ) -> Result<()> {
        self.inner
            .update_container_resources(container_id, resources)
    }

    fn is_container_paused(&self, container_id: &str) -> Result<bool> {
        self.inner.is_container_paused(container_id)
    }

    fn restore_attach_shim(&self, container_id: &str) -> Result<()> {
        self.inner.restore_attach_shim(container_id)
    }

    fn shim_status(&self, container_id: &str) -> Result<Option<crius::shim_rpc::StatusResponse>> {
        self.inner.shim_status(container_id)
    }

    fn restore_container_from_checkpoint(
        &self,
        container_id: &str,
        checkpoint_path: &Path,
        work_path: &Path,
    ) -> Result<()> {
        self.inner
            .restore_container_from_checkpoint(container_id, checkpoint_path, work_path)
    }

    fn pause_container(&self, container_id: &str) -> Result<()> {
        self.inner.pause_container(container_id)
    }

    fn checkpoint_container(
        &self,
        container_id: &str,
        location: &Path,
        work_path: &Path,
    ) -> Result<()> {
        self.inner
            .checkpoint_container(container_id, location, work_path)
    }

    fn resume_container(&self, container_id: &str) -> Result<()> {
        self.inner.resume_container(container_id)
    }

    fn container_pid(&self, container_id: &str) -> Result<Option<i32>> {
        self.inner.container_pid(container_id)
    }
}

impl RuntimeContextManager for DirectTaskRuntimeBackend {
    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.inner.bundle_path_for(container_id)
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
            source_path: PathBuf::from("/direct-task/unsupported"),
            destination: PathBuf::from("/direct-task/unsupported"),
        })
    }
}

impl RuntimeBackend for DirectTaskRuntimeBackend {
    fn backend_name(&self) -> &str {
        "direct-task"
    }

    fn context_kind(&self) -> RuntimeContextKind {
        RuntimeContextKind::DirectTask
    }

    fn runtime_root(&self) -> &Path {
        self.inner.runtime_root()
    }

    fn runtime_path(&self) -> &Path {
        self.inner.runtime_path()
    }

    fn runtime_config_path(&self) -> &Path {
        self.inner.runtime_config_path()
    }

    fn task_controller(&self) -> &dyn TaskController {
        self
    }

    fn runtime_context(&self) -> &dyn RuntimeContextManager {
        self
    }

    fn probe_runtime_features(&self) -> RuntimeFeatureProbe {
        RuntimeFeatureProbe {
            available: true,
            rootless: true,
            ..Default::default()
        }
    }

    fn cgroup_driver(&self) -> CgroupDriverConfig {
        CgroupDriverConfig::Cgroupfs
    }
}

#[derive(Debug)]
pub struct FakeContentStore {
    _dir: tempfile::TempDir,
    inner: FsContentStore,
}

impl FakeContentStore {
    pub fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let inner = FsContentStore::new(dir.path())?;
        Ok(Self { _dir: dir, inner })
    }

    pub fn root(&self) -> &Path {
        self.inner.root()
    }
}

impl ContentStore for FakeContentStore {
    fn put_blob(&self, digest: &str, media_type: &str, bytes: &[u8]) -> Result<BlobInfo> {
        self.inner.put_blob(digest, media_type, bytes)
    }

    fn get_blob(&self, digest: &str) -> Result<BlobHandle> {
        self.inner.get_blob(digest)
    }

    fn delete_blob(&self, digest: &str) -> Result<()> {
        self.inner.delete_blob(digest)
    }

    fn stat_blob(&self, digest: &str) -> Result<BlobInfo> {
        self.inner.stat_blob(digest)
    }
}

#[derive(Debug, Clone)]
pub struct FakeSnapshotter {
    root: PathBuf,
    usage: SnapshotUsage,
    snapshots: Arc<Mutex<HashMap<String, SnapshotInfo>>>,
}

impl FakeSnapshotter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            usage: SnapshotUsage::default(),
            snapshots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_usage(mut self, usage: SnapshotUsage) -> Self {
        self.usage = usage;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Snapshotter for FakeSnapshotter {
    fn prepare(&self, key: &str, image_ref: &str, destination: &Path) -> Result<PreparedSnapshot> {
        let rootfs_path = if destination.as_os_str().is_empty() {
            self.root.join(key).join("rootfs")
        } else {
            destination.to_path_buf()
        };
        let prepared = PreparedSnapshot {
            key: key.to_string(),
            image_id: image_ref.to_string(),
            rootfs_path: rootfs_path.clone(),
        };
        let mut snapshots = self
            .snapshots
            .lock()
            .map_err(|_| anyhow::anyhow!("fake snapshotter state lock poisoned"))?;
        snapshots.insert(
            key.to_string(),
            SnapshotInfo {
                key: key.to_string(),
                image_id: image_ref.to_string(),
                state: SnapshotState::Prepared,
                mountpoint: rootfs_path,
            },
        );
        Ok(prepared)
    }

    fn mount(&self, key: &str) -> Result<MountView> {
        let mut snapshots = self
            .snapshots
            .lock()
            .map_err(|_| anyhow::anyhow!("fake snapshotter state lock poisoned"))?;
        let snapshot = snapshots
            .get_mut(key)
            .with_context(|| format!("snapshot {key} not found"))?;
        snapshot.state = SnapshotState::Mounted;
        Ok(MountView {
            key: key.to_string(),
            mountpoint: snapshot.mountpoint.clone(),
            readonly: false,
        })
    }

    fn commit(&self, key: &str) -> Result<SnapshotInfo> {
        let mut snapshots = self
            .snapshots
            .lock()
            .map_err(|_| anyhow::anyhow!("fake snapshotter state lock poisoned"))?;
        let snapshot = snapshots
            .get_mut(key)
            .with_context(|| format!("snapshot {key} not found"))?;
        snapshot.state = SnapshotState::Committed;
        Ok(snapshot.clone())
    }

    fn remove(&self, key: &str) -> Result<()> {
        let mut snapshots = self
            .snapshots
            .lock()
            .map_err(|_| anyhow::anyhow!("fake snapshotter state lock poisoned"))?;
        snapshots
            .remove(key)
            .map(|_| ())
            .with_context(|| format!("snapshot {key} not found"))
    }

    fn usage_for(&self, key: &str) -> Result<SnapshotUsage> {
        let snapshots = self
            .snapshots
            .lock()
            .map_err(|_| anyhow::anyhow!("fake snapshotter state lock poisoned"))?;
        if snapshots.contains_key(key) {
            Ok(self.usage.clone())
        } else {
            Err(anyhow::anyhow!("snapshot {key} not found"))
        }
    }

    fn usage(&self) -> Result<SnapshotUsage> {
        Ok(self.usage.clone())
    }
}

pub struct FakeShimRpcServer {
    socket_path: PathBuf,
    running: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl FakeShimRpcServer {
    pub fn start(
        socket_path: impl Into<PathBuf>,
        handler: Arc<dyn ShimRpcHandler>,
    ) -> Result<Self> {
        let socket_path = socket_path.into();
        let running = Arc::new(AtomicBool::new(true));
        let running_for_thread = running.clone();
        let socket_for_thread = socket_path.clone();
        let handle = std::thread::spawn(move || {
            if let Err(err) = serve(&socket_for_thread, running_for_thread, handler) {
                log::debug!("fake shim RPC server exited with error: {}", err);
            }
        });

        Ok(Self {
            socket_path,
            running,
            handle: Some(handle),
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn client(&self) -> ShimRpcClient {
        ShimRpcClient::new(self.socket_path.clone(), Duration::from_secs(1))
    }

    pub fn wait_until_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if matches!(
                self.client().request(ShimRpcRequest::Ping),
                Ok(ShimRpcResponse::Empty)
            ) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!("fake shim RPC server did not become ready"));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for FakeShimRpcServer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        let _ = std::os::unix::net::UnixStream::connect(&self.socket_path);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub struct PingShimHandler;

impl ShimRpcHandler for PingShimHandler {
    fn handle_request(&self, request: ShimRpcRequest) -> Result<ShimRpcResponse> {
        match request {
            ShimRpcRequest::Ping => Ok(ShimRpcResponse::Empty),
            ShimRpcRequest::Status(_) => {
                Ok(ShimRpcResponse::Status(crius::shim_rpc::StatusResponse {
                    state: TaskState::Running,
                    pid: Some(1),
                    exit_code: None,
                }))
            }
            other => Err(anyhow::anyhow!(
                "unsupported fake shim request: {:?}",
                other
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScriptedShimHandler {
    state: Arc<Mutex<ScriptedShimState>>,
}

#[derive(Debug, Clone)]
struct ScriptedShimState {
    requests: Vec<ShimRpcRequest>,
    responses: VecDeque<Result<ShimRpcResponse, String>>,
    status: StatusResponse,
}

impl ScriptedShimHandler {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedShimState {
                requests: Vec::new(),
                responses: VecDeque::new(),
                status: StatusResponse {
                    state: TaskState::Running,
                    pid: Some(1),
                    exit_code: None,
                },
            })),
        }
    }

    pub fn with_response(self, response: ShimRpcResponse) -> Self {
        self.push_response(response);
        self
    }

    pub fn with_error(self, message: impl Into<String>) -> Self {
        self.push_error(message);
        self
    }

    pub fn push_response(&self, response: ShimRpcResponse) {
        if let Ok(mut state) = self.state.lock() {
            state.responses.push_back(Ok(response));
        }
    }

    pub fn push_error(&self, message: impl Into<String>) {
        if let Ok(mut state) = self.state.lock() {
            state.responses.push_back(Err(message.into()));
        }
    }

    pub fn set_status(&self, status: StatusResponse) {
        if let Ok(mut state) = self.state.lock() {
            state.status = status;
        }
    }

    pub fn requests(&self) -> Vec<ShimRpcRequest> {
        self.state
            .lock()
            .map(|state| state.requests.clone())
            .unwrap_or_default()
    }

    pub fn clear_requests(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.requests.clear();
        }
    }
}

impl Default for ScriptedShimHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ShimRpcHandler for ScriptedShimHandler {
    fn handle_request(&self, request: ShimRpcRequest) -> Result<ShimRpcResponse> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("scripted shim handler state lock poisoned"))?;
        state.requests.push(request.clone());

        if let Some(response) = state.responses.pop_front() {
            return response.map_err(|message| anyhow::anyhow!("{}", message));
        }

        match request {
            ShimRpcRequest::Ping => Ok(ShimRpcResponse::Empty),
            ShimRpcRequest::Status(_) => Ok(ShimRpcResponse::Status(state.status.clone())),
            ShimRpcRequest::ContainerPid(_) => Ok(ShimRpcResponse::ContainerPid(state.status.pid)),
            other => Err(anyhow::anyhow!(
                "unsupported scripted shim request: {:?}",
                other
            )),
        }
    }
}
