use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::config::CgroupDriverConfig;
use crate::oci::spec::Spec;
use crate::proto::runtime::v1::LinuxContainerResources;

use super::backend::RuntimeBackend;
use super::{
    ContainerConfig, ContainerRuntime, ContainerStatus, MountSemanticsError, RuncRuntime,
    RuntimeFeatureProbe,
};

#[derive(Debug, Clone)]
pub struct RuncBackend {
    inner: RuncRuntime,
}

impl RuncBackend {
    pub fn new(inner: RuncRuntime) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &RuncRuntime {
        &self.inner
    }
}

impl RuntimeBackend for RuncBackend {
    fn backend_name(&self) -> &str {
        "runc"
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

    fn bundle_path_for(&self, container_id: &str) -> PathBuf {
        self.inner.bundle_path_for(container_id)
    }

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
        resources: &LinuxContainerResources,
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

    fn shim_status(&self, container_id: &str) -> Result<Option<crate::shim_rpc::StatusResponse>> {
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

    fn enforce_oom_score_adj_policy(&self, spec: &mut Spec) -> Result<()> {
        self.inner.enforce_oom_score_adj_policy(spec)
    }

    fn prepare_rootfs(&self, container_id: &str, config: &ContainerConfig) -> Result<()> {
        self.inner.prepare_rootfs(container_id, config)
    }

    fn build_spec(&self, container_id: &str, config: &ContainerConfig) -> Result<Spec> {
        self.inner.build_spec(container_id, config)
    }

    fn write_bundle(&self, container_id: &str, rootfs: &Path, spec: &Spec) -> Result<()> {
        self.inner.write_bundle(container_id, rootfs, spec)
    }

    fn load_spec(&self, container_id: &str) -> Result<Spec> {
        self.inner.load_spec(container_id)
    }

    fn validate_mount_requests(
        &self,
        config: &ContainerConfig,
    ) -> std::result::Result<(), MountSemanticsError> {
        self.inner.validate_mount_requests(config)
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

    fn probe_runtime_features(&self) -> RuntimeFeatureProbe {
        self.inner.probe_runtime_features()
    }

    fn cgroup_driver(&self) -> CgroupDriverConfig {
        self.inner.cgroup_driver()
    }
}
