//! Shared fake implementations for unit and integration tests.
//!
//! These helpers are intentionally small and deterministic. They provide common
//! stand-ins for runtime, image storage and shim RPC boundaries without requiring
//! runc, a real rootfs or a long-running shim process.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::CgroupDriverConfig;
use crate::image::content_store::{BlobHandle, BlobInfo, ContentStore, FsContentStore};
use crate::image::snapshotter::{PreparedSnapshot, SnapshotUsage, Snapshotter};
use crate::oci::spec::Spec;
use crate::runtime::{
    ContainerConfig, ContainerStatus, MountSemanticsError, RuntimeBackend, RuntimeFeatureProbe,
};
use crate::shim_rpc::{
    server::{serve, ShimRpcHandler},
    ShimRpcClient, ShimRpcRequest, ShimRpcResponse, TaskState,
};

#[derive(Debug, Clone)]
pub struct FakeRuntimeBackend {
    backend_name: &'static str,
    runtime_root: PathBuf,
    runtime_path: PathBuf,
    runtime_config_path: PathBuf,
    status: ContainerStatus,
}

impl FakeRuntimeBackend {
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            backend_name: "fake",
            runtime_root: runtime_root.into(),
            runtime_path: PathBuf::from("/fake/runtime"),
            runtime_config_path: PathBuf::from("/fake/runtime.conf"),
            status: ContainerStatus::Created,
        }
    }

    pub fn with_status(mut self, status: ContainerStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_backend_name(mut self, backend_name: &'static str) -> Self {
        self.backend_name = backend_name;
        self
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

    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.runtime_root.join(container_id)
    }

    fn create_container(&self, _container_id: &str, _config: &ContainerConfig) -> Result<String> {
        Ok("fake-created".to_string())
    }

    fn start_container(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn stop_container(&self, _container_id: &str, _timeout: Option<u32>) -> Result<()> {
        Ok(())
    }

    fn remove_container(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn container_status(&self, _container_id: &str) -> Result<ContainerStatus> {
        Ok(self.status.clone())
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
        _resources: &crate::proto::runtime::v1::LinuxContainerResources,
    ) -> Result<()> {
        Ok(())
    }

    fn is_container_paused(&self, _container_id: &str) -> Result<bool> {
        Ok(false)
    }

    fn restore_attach_shim(&self, _container_id: &str) -> Result<()> {
        Ok(())
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
        Ok(())
    }

    fn enforce_oom_score_adj_policy(&self, _spec: &mut Spec) -> Result<()> {
        Ok(())
    }

    fn prepare_rootfs(&self, _container_id: &str, _config: &ContainerConfig) -> Result<()> {
        Ok(())
    }

    fn build_spec(&self, _container_id: &str, _config: &ContainerConfig) -> Result<Spec> {
        Ok(Spec::new("1.0.2"))
    }

    fn write_bundle(&self, _container_id: &str, _rootfs: &Path, _spec: &Spec) -> Result<()> {
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
}

impl FakeSnapshotter {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            usage: SnapshotUsage::default(),
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
        Ok(PreparedSnapshot {
            key: key.to_string(),
            image_id: image_ref.to_string(),
            rootfs_path,
        })
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
                Ok(ShimRpcResponse::Status(crate::shim_rpc::StatusResponse {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::content_store::ContentStore;
    use crate::image::snapshotter::Snapshotter;

    #[test]
    fn fake_runtime_backend_reports_configured_status() {
        let backend = FakeRuntimeBackend::new("/tmp/fake").with_status(ContainerStatus::Running);
        assert_eq!(backend.backend_name(), "fake");
        assert!(matches!(
            backend.container_status("container").unwrap(),
            ContainerStatus::Running
        ));
    }

    #[test]
    fn fake_content_store_round_trips_blobs() {
        let store = FakeContentStore::new().unwrap();
        let digest = FsContentStore::compute_digest(b"hello");
        store
            .put_blob(&digest, "application/octet-stream", b"hello")
            .unwrap();
        assert_eq!(store.stat_blob(&digest).unwrap().size, 5);
    }

    #[test]
    fn fake_snapshotter_returns_prepared_snapshot() {
        let snapshotter = FakeSnapshotter::new("/tmp/snapshots");
        let prepared = snapshotter
            .prepare("key", "image:latest", Path::new("/tmp/rootfs"))
            .unwrap();
        assert_eq!(prepared.key, "key");
        assert_eq!(prepared.image_id, "image:latest");
    }

    #[test]
    fn fake_shim_rpc_server_handles_ping() {
        let dir = tempfile::tempdir().unwrap();
        let server =
            FakeShimRpcServer::start(dir.path().join("task.sock"), Arc::new(PingShimHandler))
                .unwrap();
        server.wait_until_ready(Duration::from_secs(2)).unwrap();
    }
}
