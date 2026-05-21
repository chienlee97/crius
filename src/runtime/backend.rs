use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::config::CgroupDriverConfig;
use crate::oci::spec::Spec;
use crate::proto::runtime::v1::LinuxContainerResources;

use super::{ContainerConfig, ContainerStatus, MountSemanticsError, RuntimeFeatureProbe};

pub trait TaskController: Send + Sync {
    fn create_container(&self, container_id: &str, config: &ContainerConfig) -> Result<String>;
    fn start_container(&self, container_id: &str) -> Result<()>;
    fn stop_container(&self, container_id: &str, timeout: Option<u32>) -> Result<()>;
    fn remove_container(&self, container_id: &str) -> Result<()>;
    fn container_status(&self, container_id: &str) -> Result<ContainerStatus>;
    fn reopen_container_log(&self, container_id: &str) -> Result<()>;
    fn exec_in_container(&self, container_id: &str, command: &[String], tty: bool) -> Result<i32>;
    fn update_container_resources(
        &self,
        container_id: &str,
        resources: &LinuxContainerResources,
    ) -> Result<()>;
    fn is_container_paused(&self, container_id: &str) -> Result<bool>;
    fn restore_attach_shim(&self, container_id: &str) -> Result<()>;
    fn shim_status(&self, container_id: &str) -> Result<Option<crate::shim_rpc::StatusResponse>>;
    fn restore_container_from_checkpoint(
        &self,
        container_id: &str,
        checkpoint_path: &Path,
        work_path: &Path,
    ) -> Result<()>;
    fn pause_container(&self, container_id: &str) -> Result<()>;
    fn checkpoint_container(
        &self,
        container_id: &str,
        location: &Path,
        work_path: &Path,
    ) -> Result<()>;
    fn resume_container(&self, container_id: &str) -> Result<()>;
    fn container_pid(&self, container_id: &str) -> Result<Option<i32>>;
}

pub trait RuntimeContextManager: Send + Sync {
    fn bundle_path_for(&self, container_id: &str) -> PathBuf;
    fn enforce_oom_score_adj_policy(&self, spec: &mut Spec) -> Result<()>;
    fn prepare_rootfs(&self, container_id: &str, config: &ContainerConfig) -> Result<()>;
    fn build_spec(&self, container_id: &str, config: &ContainerConfig) -> Result<Spec>;
    fn write_bundle(&self, container_id: &str, rootfs: &Path, spec: &Spec) -> Result<()>;
    fn load_spec(&self, container_id: &str) -> Result<Spec>;
    fn validate_mount_requests(
        &self,
        config: &ContainerConfig,
    ) -> std::result::Result<(), MountSemanticsError>;
}

pub trait RuntimeBackend: Send + Sync {
    fn backend_name(&self) -> &str;
    fn runtime_root(&self) -> &Path;
    fn runtime_path(&self) -> &Path;
    fn runtime_config_path(&self) -> &Path;
    fn task_controller(&self) -> &dyn TaskController;
    fn runtime_context(&self) -> &dyn RuntimeContextManager;
    fn probe_runtime_features(&self) -> RuntimeFeatureProbe;
    fn cgroup_driver(&self) -> CgroupDriverConfig;
}
