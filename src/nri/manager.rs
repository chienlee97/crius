use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::nri::{NriApi, NriManagerConfig, PluginTtrpcClient, Result, RuntimeTtrpcServer};

#[derive(Debug, Clone)]
pub struct PluginRecord {
    pub name: String,
    pub index: String,
    pub events_mask: i32,
    pub socket_path: String,
}

pub struct NriManager {
    config: NriManagerConfig,
    runtime_server: RuntimeTtrpcServer,
    plugins: Arc<RwLock<Vec<PluginRecord>>>,
}

impl NriManager {
    pub fn new(config: NriManagerConfig) -> Self {
        let runtime_server = RuntimeTtrpcServer::new(
            config.socket_path.clone(),
            config.registration_timeout,
            config.request_timeout,
            config.enable_external_connections,
        );
        Self {
            config,
            runtime_server,
            plugins: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn register_plugin(&self, plugin: PluginRecord) {
        let mut plugins = self.plugins.write().await;
        plugins.push(plugin);
        plugins.sort_by(|a, b| {
            a.index
                .cmp(&b.index)
                .then_with(|| a.name.cmp(&b.name))
        });
    }

    pub async fn connect_plugin(&self, socket_path: String) -> Result<()> {
        let client = PluginTtrpcClient::new(socket_path, self.config.request_timeout);
        client.connect().await
    }
}

#[async_trait]
impl NriApi for NriManager {
    async fn start(&self) -> Result<()> {
        if !self.config.enable {
            return Ok(());
        }
        self.runtime_server.start().await
    }

    async fn shutdown(&self) -> Result<()> {
        let mut plugins = self.plugins.write().await;
        plugins.clear();
        Ok(())
    }

    async fn synchronize(&self) -> Result<()> {
        if !self.config.enable {
            return Ok(());
        }
        Ok(())
    }
}
