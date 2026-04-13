//! 状态持久化集成模块
//!
//! 提供与server模块的集成，实现容器和Pod状态的自动持久化

use crate::runtime::ContainerStatus;
use crate::storage::{ContainerRecord, PodSandboxRecord, StorageManager};
use anyhow::Result;
use std::collections::HashMap;

/// 将容器配置转换为存储记录
pub fn container_to_record(
    id: &str,
    pod_id: &str,
    state: ContainerStatus,
    image: &str,
    command: &[String],
    labels: &HashMap<String, String>,
    annotations: &HashMap<String, String>,
) -> ContainerRecord {
    let state_str = match state {
        ContainerStatus::Created => "created",
        ContainerStatus::Running => "running",
        ContainerStatus::Stopped(code) => {
            return ContainerRecord {
                id: id.to_string(),
                pod_id: pod_id.to_string(),
                state: "stopped".to_string(),
                image: image.to_string(),
                command: command.join(" "),
                created_at: chrono::Utc::now().timestamp(),
                labels: serde_json::to_string(labels).unwrap_or_default(),
                annotations: serde_json::to_string(annotations).unwrap_or_default(),
                exit_code: Some(code),
                exit_time: Some(chrono::Utc::now().timestamp()),
            }
        }
        ContainerStatus::Unknown => "unknown",
    };

    ContainerRecord {
        id: id.to_string(),
        pod_id: pod_id.to_string(),
        state: state_str.to_string(),
        image: image.to_string(),
        command: command.join(" "),
        created_at: chrono::Utc::now().timestamp(),
        labels: serde_json::to_string(labels).unwrap_or_default(),
        annotations: serde_json::to_string(annotations).unwrap_or_default(),
        exit_code: None,
        exit_time: None,
    }
}

/// 从存储记录恢复容器状态
pub fn record_to_container_status(record: &ContainerRecord) -> ContainerStatus {
    match record.state.as_str() {
        "created" => ContainerStatus::Created,
        "running" => ContainerStatus::Running,
        "stopped" => ContainerStatus::Stopped(record.exit_code.unwrap_or(0)),
        _ => ContainerStatus::Unknown,
    }
}

/// 将Pod沙箱配置转换为存储记录
pub fn pod_to_record(
    id: &str,
    state: &str,
    name: &str,
    namespace: &str,
    uid: &str,
    netns_path: &str,
    labels: &HashMap<String, String>,
    annotations: &HashMap<String, String>,
    pause_container_id: Option<&str>,
    ip: Option<&str>,
) -> PodSandboxRecord {
    PodSandboxRecord {
        id: id.to_string(),
        state: state.to_string(),
        name: name.to_string(),
        namespace: namespace.to_string(),
        uid: uid.to_string(),
        created_at: chrono::Utc::now().timestamp(),
        netns_path: netns_path.to_string(),
        labels: serde_json::to_string(labels).unwrap_or_default(),
        annotations: serde_json::to_string(annotations).unwrap_or_default(),
        pause_container_id: pause_container_id.map(|s| s.to_string()),
        ip: ip.map(|s| s.to_string()),
    }
}

/// 状态持久化配置
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// 数据库文件路径
    pub db_path: std::path::PathBuf,
    /// 是否启用状态恢复
    pub enable_recovery: bool,
    /// 自动保存间隔（秒）
    pub auto_save_interval: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            db_path: std::path::PathBuf::from("/var/lib/crius/crius.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        }
    }
}

/// 持久化管理器
#[derive(Debug)]
pub struct PersistenceManager {
    storage: StorageManager,
    _config: PersistenceConfig,
}

impl PersistenceManager {
    /// 创建新的持久化管理器
    pub fn new(config: PersistenceConfig) -> Result<Self> {
        let storage = StorageManager::new(&config.db_path)?;

        Ok(Self {
            storage,
            _config: config,
        })
    }

    /// 获取存储管理器的可变引用
    pub fn storage_mut(&mut self) -> &mut StorageManager {
        &mut self.storage
    }

    /// 获取存储管理器的引用
    pub fn storage(&self) -> &StorageManager {
        &self.storage
    }

    /// 保存容器状态
    pub fn save_container(
        &mut self,
        id: &str,
        pod_id: &str,
        state: ContainerStatus,
        image: &str,
        command: &[String],
        labels: &HashMap<String, String>,
        annotations: &HashMap<String, String>,
    ) -> Result<()> {
        let record = container_to_record(id, pod_id, state, image, command, labels, annotations);
        self.storage.save_container(&record)
    }

    /// 更新容器状态
    pub fn update_container_state(
        &mut self,
        container_id: &str,
        new_state: ContainerStatus,
    ) -> Result<()> {
        let (state_str, exit_code) = match new_state {
            ContainerStatus::Created => ("created", None),
            ContainerStatus::Running => ("running", None),
            ContainerStatus::Stopped(code) => ("stopped", Some(code)),
            ContainerStatus::Unknown => ("unknown", None),
        };

        self.storage
            .update_container_state(container_id, state_str, exit_code)
    }

    /// 删除容器记录
    pub fn delete_container(&mut self, container_id: &str) -> Result<()> {
        self.storage.delete_container(container_id)
    }

    /// 保存Pod沙箱
    pub fn save_pod_sandbox(
        &mut self,
        id: &str,
        state: &str,
        name: &str,
        namespace: &str,
        uid: &str,
        netns_path: &str,
        labels: &HashMap<String, String>,
        annotations: &HashMap<String, String>,
        pause_container_id: Option<&str>,
        ip: Option<&str>,
    ) -> Result<()> {
        let record = pod_to_record(
            id,
            state,
            name,
            namespace,
            uid,
            netns_path,
            labels,
            annotations,
            pause_container_id,
            ip,
        );
        self.storage.save_pod_sandbox(&record)
    }

    /// 更新Pod状态
    pub fn update_pod_state(&mut self, pod_id: &str, new_state: &str) -> Result<()> {
        self.storage.update_pod_state(pod_id, new_state)
    }

    /// 删除Pod沙箱
    pub fn delete_pod_sandbox(&mut self, pod_id: &str) -> Result<()> {
        self.storage.delete_pod_sandbox(pod_id)
    }

    /// 恢复所有容器状态
    pub fn recover_containers(&self) -> Result<Vec<(String, ContainerStatus, ContainerRecord)>> {
        let records = self.storage.list_containers()?;

        let mut result = Vec::new();
        for record in records {
            let status = record_to_container_status(&record);
            result.push((record.id.clone(), status, record));
        }

        Ok(result)
    }

    /// 恢复所有Pod沙箱
    pub fn recover_pods(&self) -> Result<Vec<PodSandboxRecord>> {
        self.storage.list_pod_sandboxes()
    }

    /// 关闭持久化管理器
    pub fn close(self) -> Result<()> {
        self.storage.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_persistence_manager() {
        let temp_dir = tempdir().unwrap();
        let config = PersistenceConfig {
            db_path: temp_dir.path().join("test.db"),
            enable_recovery: true,
            auto_save_interval: 30,
        };

        let mut manager = PersistenceManager::new(config).unwrap();

        // 保存容器
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        manager
            .save_container(
                "container-1",
                "pod-1",
                ContainerStatus::Running,
                "test:latest",
                &["echo".to_string(), "hello".to_string()],
                &labels,
                &HashMap::new(),
            )
            .unwrap();

        // 更新状态
        manager
            .update_container_state("container-1", ContainerStatus::Stopped(0))
            .unwrap();

        // 恢复容器
        let recovered = manager.recover_containers().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].0, "container-1");

        // 保存Pod
        manager
            .save_pod_sandbox(
                "pod-1",
                "ready",
                "test-pod",
                "default",
                "uid-123",
                "/var/run/netns/pod-1",
                &labels,
                &HashMap::new(),
                Some("pause-1"),
                Some("10.88.0.1"),
            )
            .unwrap();

        // 恢复Pod
        let recovered_pods = manager.recover_pods().unwrap();
        assert_eq!(recovered_pods.len(), 1);
        assert_eq!(recovered_pods[0].id, "pod-1");
    }
}
