//! 状态持久化存储模块
//!
//! 使用SQLite持久化容器和Pod沙箱状态，支持daemon重启后状态恢复

use anyhow::{Context, Result};
use log::{debug, info};
use rusqlite::{Connection, OptionalExtension};
use std::path::Path;

pub mod volume;
pub use volume::{MountedVolume, VolumeConfig, VolumeManager, VolumeType};

/// 存储管理器
#[derive(Debug)]
pub struct StorageManager {
    conn: Connection,
    db_path: std::path::PathBuf,
}

/// 容器记录
#[derive(Debug, Clone)]
pub struct ContainerRecord {
    pub id: String,
    pub pod_id: String,
    pub state: String,
    pub image: String,
    pub command: String,
    pub created_at: i64,
    pub labels: String,      // JSON
    pub annotations: String, // JSON
    pub exit_code: Option<i32>,
    pub exit_time: Option<i64>,
}

/// Pod沙箱记录
#[derive(Debug, Clone)]
pub struct PodSandboxRecord {
    pub id: String,
    pub state: String,
    pub name: String,
    pub namespace: String,
    pub uid: String,
    pub created_at: i64,
    pub netns_path: String,
    pub labels: String,      // JSON
    pub annotations: String, // JSON
    pub pause_container_id: Option<String>,
    pub ip: Option<String>,
}

impl StorageManager {
    /// 创建新的存储管理器
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();

        // 确保父目录存在
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).context("Failed to create database directory")?;
        }

        let conn = Connection::open(&db_path).context("Failed to open database connection")?;

        let mut manager = Self { conn, db_path };
        manager.init_tables()?;

        info!("Storage manager initialized at {:?}", manager.db_path);
        Ok(manager)
    }

    /// 初始化数据库表
    fn init_tables(&mut self) -> Result<()> {
        // 容器表
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS containers (
                id TEXT PRIMARY KEY,
                pod_id TEXT NOT NULL,
                state TEXT NOT NULL,
                image TEXT NOT NULL,
                command TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                labels TEXT NOT NULL DEFAULT '{}',
                annotations TEXT NOT NULL DEFAULT '{}',
                exit_code INTEGER,
                exit_time INTEGER
            )",
                [],
            )
            .context("Failed to create containers table")?;

        // Pod沙箱表
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS pod_sandboxes (
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                name TEXT NOT NULL,
                namespace TEXT NOT NULL,
                uid TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                netns_path TEXT NOT NULL,
                labels TEXT NOT NULL DEFAULT '{}',
                annotations TEXT NOT NULL DEFAULT '{}',
                pause_container_id TEXT,
                ip TEXT
            )",
                [],
            )
            .context("Failed to create pod_sandboxes table")?;

        // 状态历史表（用于事件通知）
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS state_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                old_state TEXT,
                new_state TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                details TEXT
            )",
                [],
            )
            .context("Failed to create state_events table")?;

        // 创建索引
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_containers_pod ON containers(pod_id)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_containers_state ON containers(state)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pods_state ON pod_sandboxes(state)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_entity ON state_events(entity_type, entity_id)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_time ON state_events(timestamp)",
            [],
        )?;

        debug!("Database tables initialized");
        Ok(())
    }

    /// 保存容器记录
    pub fn save_container(&mut self, record: &ContainerRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO containers 
             (id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                &record.id,
                &record.pod_id,
                &record.state,
                &record.image,
                &record.command,
                record.created_at,
                &record.labels,
                &record.annotations,
                record.exit_code,
                record.exit_time,
            ],
        ).context("Failed to save container")?;

        // 记录状态变更事件
        self.record_state_event("container", &record.id, None, Some(&record.state))?;

        debug!("Container {} saved to database", record.id);
        Ok(())
    }

    /// 更新容器状态
    pub fn update_container_state(
        &mut self,
        container_id: &str,
        new_state: &str,
        exit_code: Option<i32>,
    ) -> Result<()> {
        // 获取旧状态
        let old_state: Option<String> = self
            .conn
            .query_row(
                "SELECT state FROM containers WHERE id = ?1",
                [container_id],
                |row| row.get(0),
            )
            .optional()?;

        let exit_time = if exit_code.is_some() {
            Some(chrono::Utc::now().timestamp())
        } else {
            None
        };

        self.conn
            .execute(
                "UPDATE containers SET state = ?1, exit_code = ?2, exit_time = ?3 WHERE id = ?4",
                rusqlite::params![new_state, exit_code, exit_time, container_id,],
            )
            .context("Failed to update container state")?;

        // 记录状态变更事件
        if old_state.as_deref() != Some(new_state) {
            self.record_state_event(
                "container",
                container_id,
                old_state.as_deref(),
                Some(new_state),
            )?;
        }

        debug!("Container {} state updated to {}", container_id, new_state);
        Ok(())
    }

    /// 获取容器记录
    pub fn get_container(&self, container_id: &str) -> Result<Option<ContainerRecord>> {
        let record = self.conn.query_row(
            "SELECT id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time
             FROM containers WHERE id = ?1",
            [container_id],
            |row| {
                Ok(ContainerRecord {
                    id: row.get(0)?,
                    pod_id: row.get(1)?,
                    state: row.get(2)?,
                    image: row.get(3)?,
                    command: row.get(4)?,
                    created_at: row.get(5)?,
                    labels: row.get(6)?,
                    annotations: row.get(7)?,
                    exit_code: row.get(8)?,
                    exit_time: row.get(9)?,
                })
            },
        ).optional().context("Failed to get container")?;

        Ok(record)
    }

    /// 获取所有容器
    pub fn list_containers(&self) -> Result<Vec<ContainerRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time
             FROM containers"
        )?;

        let records = stmt
            .query_map([], |row| {
                Ok(ContainerRecord {
                    id: row.get(0)?,
                    pod_id: row.get(1)?,
                    state: row.get(2)?,
                    image: row.get(3)?,
                    command: row.get(4)?,
                    created_at: row.get(5)?,
                    labels: row.get(6)?,
                    annotations: row.get(7)?,
                    exit_code: row.get(8)?,
                    exit_time: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list containers")?;

        Ok(records)
    }

    /// 获取Pod下的所有容器
    pub fn list_containers_by_pod(&self, pod_id: &str) -> Result<Vec<ContainerRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time
             FROM containers WHERE pod_id = ?1"
        )?;

        let records = stmt
            .query_map([pod_id], |row| {
                Ok(ContainerRecord {
                    id: row.get(0)?,
                    pod_id: row.get(1)?,
                    state: row.get(2)?,
                    image: row.get(3)?,
                    command: row.get(4)?,
                    created_at: row.get(5)?,
                    labels: row.get(6)?,
                    annotations: row.get(7)?,
                    exit_code: row.get(8)?,
                    exit_time: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list containers by pod")?;

        Ok(records)
    }

    /// 删除容器记录
    pub fn delete_container(&mut self, container_id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM containers WHERE id = ?1", [container_id])
            .context("Failed to delete container")?;

        debug!("Container {} deleted from database", container_id);
        Ok(())
    }

    /// 保存Pod沙箱记录
    pub fn save_pod_sandbox(&mut self, record: &PodSandboxRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO pod_sandboxes 
             (id, state, name, namespace, uid, created_at, netns_path, labels, annotations, pause_container_id, ip)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                &record.id,
                &record.state,
                &record.name,
                &record.namespace,
                &record.uid,
                record.created_at,
                &record.netns_path,
                &record.labels,
                &record.annotations,
                record.pause_container_id.as_deref(),
                record.ip.as_deref(),
            ],
        ).context("Failed to save pod sandbox")?;

        // 记录状态变更事件
        self.record_state_event("pod", &record.id, None, Some(&record.state))?;

        debug!("Pod sandbox {} saved to database", record.id);
        Ok(())
    }

    /// 更新Pod沙箱状态
    pub fn update_pod_state(&mut self, pod_id: &str, new_state: &str) -> Result<()> {
        // 获取旧状态
        let old_state: Option<String> = self
            .conn
            .query_row(
                "SELECT state FROM pod_sandboxes WHERE id = ?1",
                [pod_id],
                |row| row.get(0),
            )
            .optional()?;

        self.conn
            .execute(
                "UPDATE pod_sandboxes SET state = ?1 WHERE id = ?2",
                [new_state, pod_id],
            )
            .context("Failed to update pod state")?;

        // 记录状态变更事件
        if old_state.as_deref() != Some(new_state) {
            self.record_state_event("pod", pod_id, old_state.as_deref(), Some(new_state))?;
        }

        debug!("Pod {} state updated to {}", pod_id, new_state);
        Ok(())
    }

    /// 获取Pod沙箱记录
    pub fn get_pod_sandbox(&self, pod_id: &str) -> Result<Option<PodSandboxRecord>> {
        let record = self.conn.query_row(
            "SELECT id, state, name, namespace, uid, created_at, netns_path, labels, annotations, pause_container_id, ip
             FROM pod_sandboxes WHERE id = ?1",
            [pod_id],
            |row| {
                Ok(PodSandboxRecord {
                    id: row.get(0)?,
                    state: row.get(1)?,
                    name: row.get(2)?,
                    namespace: row.get(3)?,
                    uid: row.get(4)?,
                    created_at: row.get(5)?,
                    netns_path: row.get(6)?,
                    labels: row.get(7)?,
                    annotations: row.get(8)?,
                    pause_container_id: row.get(9)?,
                    ip: row.get(10)?,
                })
            },
        ).optional().context("Failed to get pod sandbox")?;

        Ok(record)
    }

    /// 获取所有Pod沙箱
    pub fn list_pod_sandboxes(&self) -> Result<Vec<PodSandboxRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, state, name, namespace, uid, created_at, netns_path, labels, annotations, pause_container_id, ip
             FROM pod_sandboxes"
        )?;

        let records = stmt
            .query_map([], |row| {
                Ok(PodSandboxRecord {
                    id: row.get(0)?,
                    state: row.get(1)?,
                    name: row.get(2)?,
                    namespace: row.get(3)?,
                    uid: row.get(4)?,
                    created_at: row.get(5)?,
                    netns_path: row.get(6)?,
                    labels: row.get(7)?,
                    annotations: row.get(8)?,
                    pause_container_id: row.get(9)?,
                    ip: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list pod sandboxes")?;

        Ok(records)
    }

    /// 删除Pod沙箱记录
    pub fn delete_pod_sandbox(&mut self, pod_id: &str) -> Result<()> {
        // 先删除关联的容器
        self.conn
            .execute("DELETE FROM containers WHERE pod_id = ?1", [pod_id])
            .context("Failed to delete pod containers")?;

        // 再删除Pod
        self.conn
            .execute("DELETE FROM pod_sandboxes WHERE id = ?1", [pod_id])
            .context("Failed to delete pod sandbox")?;

        debug!(
            "Pod sandbox {} and its containers deleted from database",
            pod_id
        );
        Ok(())
    }

    /// 记录状态变更事件
    fn record_state_event(
        &mut self,
        entity_type: &str,
        entity_id: &str,
        old_state: Option<&str>,
        new_state: Option<&str>,
    ) -> Result<()> {
        let timestamp = chrono::Utc::now().timestamp();

        self.conn
            .execute(
                "INSERT INTO state_events (entity_type, entity_id, old_state, new_state, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5)",
                [
                    entity_type,
                    entity_id,
                    old_state.unwrap_or(""),
                    new_state.unwrap_or(""),
                    &timestamp.to_string(),
                ],
            )
            .context("Failed to record state event")?;

        Ok(())
    }

    /// 获取最近的实体状态事件
    pub fn get_recent_events(&self, entity_type: &str, since: i64) -> Result<Vec<StateEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, entity_type, entity_id, old_state, new_state, timestamp, details
             FROM state_events
             WHERE entity_type = ?1 AND timestamp >= ?2
             ORDER BY timestamp DESC",
        )?;

        let events = stmt
            .query_map([entity_type, &since.to_string()], |row| {
                Ok(StateEvent {
                    id: row.get(0)?,
                    entity_type: row.get(1)?,
                    entity_id: row.get(2)?,
                    old_state: row.get(3)?,
                    new_state: row.get(4)?,
                    timestamp: row.get(5)?,
                    details: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to get recent events")?;

        Ok(events)
    }

    /// 关闭数据库连接
    pub fn close(self) -> Result<()> {
        self.conn
            .close()
            .map_err(|e| anyhow::anyhow!("Failed to close database: {:?}", e))?;
        Ok(())
    }
}

/// 状态变更事件
#[derive(Debug, Clone)]
pub struct StateEvent {
    pub id: i64,
    pub entity_type: String,
    pub entity_id: String,
    pub old_state: String,
    pub new_state: String,
    pub timestamp: i64,
    pub details: Option<String>,
}

pub mod persistence;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_storage_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let manager = StorageManager::new(&db_path);
        assert!(manager.is_ok());
    }

    #[test]
    fn test_container_crud() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let mut manager = StorageManager::new(&db_path).unwrap();

        // 创建容器记录
        let container = ContainerRecord {
            id: "container-1".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "test:latest".to_string(),
            command: "echo hello".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
        };

        manager.save_container(&container).unwrap();

        // 查询容器
        let retrieved = manager.get_container("container-1").unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, "container-1");
        assert_eq!(retrieved.state, "running");

        // 更新状态
        manager
            .update_container_state("container-1", "stopped", Some(0))
            .unwrap();

        let updated = manager.get_container("container-1").unwrap().unwrap();
        assert_eq!(updated.state, "stopped");
        assert_eq!(updated.exit_code, Some(0));

        // 删除容器
        manager.delete_container("container-1").unwrap();
        let deleted = manager.get_container("container-1").unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    fn test_pod_sandbox_crud() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let mut manager = StorageManager::new(&db_path).unwrap();

        // 创建Pod记录
        let pod = PodSandboxRecord {
            id: "pod-1".to_string(),
            state: "ready".to_string(),
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
            uid: "uid-123".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            netns_path: "/var/run/netns/pod-1".to_string(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            pause_container_id: Some("pause-1".to_string()),
            ip: Some("10.88.0.1".to_string()),
        };

        manager.save_pod_sandbox(&pod).unwrap();

        // 查询Pod
        let retrieved = manager.get_pod_sandbox("pod-1").unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, "pod-1");
        assert_eq!(retrieved.name, "test-pod");

        // 更新状态
        manager.update_pod_state("pod-1", "notready").unwrap();

        let updated = manager.get_pod_sandbox("pod-1").unwrap().unwrap();
        assert_eq!(updated.state, "notready");

        // 删除Pod
        manager.delete_pod_sandbox("pod-1").unwrap();
        let deleted = manager.get_pod_sandbox("pod-1").unwrap();
        assert!(deleted.is_none());
    }
}
