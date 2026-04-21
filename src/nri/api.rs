use async_trait::async_trait;

use crate::nri::domain::RuntimeSnapshot;
use crate::nri::error::Result;
use crate::nri_proto::api as nri_api;

#[derive(Debug, Clone, Default)]
pub struct NriPodEvent {
    pub pod: Option<nri_api::PodSandbox>,
    pub overhead_linux_resources: Option<nri_api::LinuxResources>,
    pub linux_resources: Option<nri_api::LinuxResources>,
}

#[derive(Debug, Clone, Default)]
pub struct NriContainerEvent {
    pub pod: Option<nri_api::PodSandbox>,
    pub container: nri_api::Container,
    pub linux_resources: Option<nri_api::LinuxResources>,
}

#[derive(Debug, Clone, Default)]
pub struct NriCreateContainerResult {
    pub adjustment: nri_api::ContainerAdjustment,
    pub updates: Vec<nri_api::ContainerUpdate>,
    pub evictions: Vec<nri_api::ContainerEviction>,
}

#[derive(Debug, Clone, Default)]
pub struct NriUpdateContainerResult {
    pub linux_resources: Option<nri_api::LinuxResources>,
    pub updates: Vec<nri_api::ContainerUpdate>,
    pub evictions: Vec<nri_api::ContainerEviction>,
}

#[async_trait]
pub trait NriDomain: Send + Sync {
    async fn snapshot(&self) -> Result<RuntimeSnapshot>;
    async fn apply_updates(
        &self,
        _updates: &[nri_api::ContainerUpdate],
    ) -> Result<Vec<nri_api::ContainerUpdate>>;
    async fn evict(&self, _container_id: &str, _reason: &str) -> Result<()>;
}

#[async_trait]
pub trait NriApi: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;
    async fn synchronize(&self) -> Result<()>;
    async fn run_pod_sandbox(&self, _event: NriPodEvent) -> Result<()> {
        Ok(())
    }
    async fn stop_pod_sandbox(&self, _event: NriPodEvent) -> Result<()> {
        Ok(())
    }
    async fn remove_pod_sandbox(&self, _event: NriPodEvent) -> Result<()> {
        Ok(())
    }
    async fn update_pod_sandbox(&self, _event: NriPodEvent) -> Result<()> {
        Ok(())
    }
    async fn create_container(
        &self,
        _event: NriContainerEvent,
    ) -> Result<NriCreateContainerResult> {
        Ok(NriCreateContainerResult::default())
    }
    async fn post_create_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
    }
    async fn start_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
    }
    async fn post_start_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
    }
    async fn update_container(&self, event: NriContainerEvent) -> Result<NriUpdateContainerResult> {
        Ok(NriUpdateContainerResult {
            linux_resources: event.linux_resources,
            ..Default::default()
        })
    }
    async fn post_update_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
    }
    async fn stop_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
    }
    async fn remove_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct NopNri;

#[async_trait]
impl NriApi for NopNri {
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn synchronize(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl NriDomain for NopNri {
    async fn snapshot(&self) -> Result<RuntimeSnapshot> {
        Ok(RuntimeSnapshot::default())
    }

    async fn apply_updates(
        &self,
        _updates: &[nri_api::ContainerUpdate],
    ) -> Result<Vec<nri_api::ContainerUpdate>> {
        Ok(Vec::new())
    }

    async fn evict(&self, _container_id: &str, _reason: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{NopNri, NriApi, NriContainerEvent, NriCreateContainerResult, NriDomain, NriPodEvent};

    #[tokio::test]
    async fn nop_nri_api_methods_are_noop() {
        let nop = NopNri;
        nop.start().await.expect("start should be noop");
        nop.synchronize().await.expect("sync should be noop");
        nop.run_pod_sandbox(NriPodEvent::default())
            .await
            .expect("run pod should be noop");
        let create = nop
            .create_container(NriContainerEvent::default())
            .await
            .expect("create container should be noop");
        assert!(matches!(create, NriCreateContainerResult { .. }));
        nop.post_create_container(NriContainerEvent::default())
            .await
            .expect("post create should be noop");
        nop.shutdown().await.expect("shutdown should be noop");
    }

    #[tokio::test]
    async fn nop_nri_domain_methods_are_noop() {
        let nop = NopNri;
        let snapshot = nop.snapshot().await.expect("snapshot should be noop");
        assert!(snapshot.pods.is_empty());
        assert!(snapshot.containers.is_empty());
        let failed = nop
            .apply_updates(&[])
            .await
            .expect("apply updates should be noop");
        assert!(failed.is_empty());
        nop.evict("container-1", "test")
            .await
            .expect("evict should be noop");
    }
}
