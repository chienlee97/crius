use async_trait::async_trait;
use std::collections::HashMap;

use crate::nri::error::Result;

#[derive(Debug, Clone, Default)]
pub struct NriPodEvent {
    pub pod_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct NriContainerEvent {
    pub pod_id: String,
    pub container_id: String,
    pub annotations: HashMap<String, String>,
}

#[async_trait]
pub trait NriDomain: Send + Sync {
    async fn snapshot(&self) -> Result<()>;
    async fn apply_updates(&self) -> Result<()>;
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
    async fn create_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
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
    async fn update_container(&self, _event: NriContainerEvent) -> Result<()> {
        Ok(())
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
    async fn snapshot(&self) -> Result<()> {
        Ok(())
    }

    async fn apply_updates(&self) -> Result<()> {
        Ok(())
    }

    async fn evict(&self, _container_id: &str, _reason: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{NopNri, NriApi, NriContainerEvent, NriDomain, NriPodEvent};

    #[tokio::test]
    async fn nop_nri_api_methods_are_noop() {
        let nop = NopNri;
        nop.start().await.expect("start should be noop");
        nop.synchronize().await.expect("sync should be noop");
        nop.run_pod_sandbox(NriPodEvent::default())
            .await
            .expect("run pod should be noop");
        nop.create_container(NriContainerEvent::default())
            .await
            .expect("create container should be noop");
        nop.post_create_container(NriContainerEvent::default())
            .await
            .expect("post create should be noop");
        nop.shutdown().await.expect("shutdown should be noop");
    }

    #[tokio::test]
    async fn nop_nri_domain_methods_are_noop() {
        let nop = NopNri;
        nop.snapshot().await.expect("snapshot should be noop");
        nop.apply_updates().await.expect("apply updates should be noop");
        nop.evict("container-1", "test")
            .await
            .expect("evict should be noop");
    }
}
