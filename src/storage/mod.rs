//! 状态持久化存储模块
//!
//! 使用SQLite持久化容器和Pod沙箱状态，支持daemon重启后状态恢复

use anyhow::{Context, Result};
use log::{debug, info};
use rusqlite::{Connection, OptionalExtension};
use std::path::Path;

pub mod volume;
pub use volume::{MountedVolume, VolumeConfig, VolumeManager, VolumeType};

const CURRENT_SCHEMA_VERSION: i64 = 1;

/// 存储管理器
#[derive(Debug)]
pub struct StorageManager {
    conn: Connection,
    db_path: std::path::PathBuf,
}

fn content_blob_ref_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContentBlobRefRecord> {
    Ok(ContentBlobRefRecord {
        owner_kind: row.get(0)?,
        owner_id: row.get(1)?,
        digest: row.get(2)?,
        ref_kind: row.get(3)?,
    })
}

fn transfer_source_matches_blob(
    transfer: &ContentTransferRecord,
    blob: &ContentBlobRecord,
) -> bool {
    transfer.source == blob.digest
        || transfer.source.contains(&blob.digest)
        || (!blob.relative_path.is_empty() && transfer.source.contains(&blob.relative_path))
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
    pub runtime_handler: Option<String>,
    pub runtime_backend: Option<String>,
    pub snapshot_key: Option<String>,
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

/// 镜像记录
#[derive(Debug, Clone)]
pub struct ImageRecord {
    pub id: String,
    pub size: u64,
    pub pinned: bool,
    pub pulled_at: i64,
    pub source_reference: Option<String>,
    pub os: Option<String>,
    pub architecture: Option<String>,
    pub config_user: Option<String>,
    pub config_env_json: String,
    pub config_entrypoint_json: String,
    pub config_cmd_json: String,
    pub config_working_dir: Option<String>,
    pub annotations_json: String,
    pub declared_volumes_json: String,
    pub manifest_media_type: Option<String>,
    pub selected_manifest_digest: Option<String>,
    pub selected_platform: Option<String>,
    pub stored_layers_json: String,
    pub artifact_type: Option<String>,
    pub artifact_blobs_json: String,
    pub cache_path: Option<String>,
}

/// 镜像引用记录
#[derive(Debug, Clone)]
pub struct ImageRefRecord {
    pub reference: String,
    pub image_id: String,
    pub namespace: Option<String>,
    pub ref_kind: String,
}

/// 内容 blob 记录
#[derive(Debug, Clone)]
pub struct ContentBlobRecord {
    pub digest: String,
    pub media_type: String,
    pub size: u64,
    pub relative_path: String,
    pub created_at: i64,
    pub last_used_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentGcBlocker {
    ContentRef {
        owner_kind: String,
        owner_id: String,
        ref_kind: String,
    },
    ActiveTransfer {
        transfer_id: String,
        source: String,
    },
}

#[derive(Debug, Clone)]
pub struct ContentGcCandidate {
    pub blob: ContentBlobRecord,
    pub blockers: Vec<ContentGcBlocker>,
}

/// 内容 blob 引用记录
#[derive(Debug, Clone)]
pub struct ContentBlobRefRecord {
    pub owner_kind: String,
    pub owner_id: String,
    pub digest: String,
    pub ref_kind: String,
}

#[derive(Debug, Clone)]
pub struct ContentTransferRecord {
    pub id: String,
    pub source: String,
    pub provider: String,
    pub state: String,
    pub current_stage: String,
    pub bytes_total: u64,
    pub bytes_completed: u64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub error: Option<String>,
}

pub struct TypedEventInput<'a> {
    pub event_type: &'a str,
    pub entity_type: &'a str,
    pub entity_id: &'a str,
    pub old_state: Option<&'a str>,
    pub new_state: Option<&'a str>,
    pub details: Option<&'a str>,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct SchemaMigrationRecord {
    pub version: i64,
    pub migration_name: String,
    pub dirty: bool,
    pub applied_at: i64,
}

/// 快照记录
#[derive(Debug, Clone)]
pub struct SnapshotRecord {
    pub key: String,
    pub image_id: String,
    pub owner_kind: String,
    pub owner_id: String,
    pub state: String,
    pub mountpoint: String,
    pub snapshotter: String,
    pub runtime_managed: bool,
}

/// runtime工件记录
#[derive(Debug, Clone)]
pub struct RuntimeArtifactRecord {
    pub owner_kind: String,
    pub owner_id: String,
    pub artifact_kind: String,
    pub path: String,
    pub state: String,
    pub runtime_handler: Option<String>,
    pub runtime_root: Option<String>,
}

/// shim进程记录
#[derive(Debug, Clone)]
pub struct ShimProcessRecord {
    pub container_id: String,
    pub shim_pid: u32,
    pub work_dir: String,
    pub socket_path: String,
    pub exit_code_file: String,
    pub log_file: String,
    pub bundle_path: String,
    pub state: String,
    pub last_seen_at: i64,
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
        self.ensure_schema_version_table()?;
        self.reject_unsupported_or_dirty_schema()?;

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
                exit_time INTEGER,
                runtime_handler TEXT,
                runtime_backend TEXT,
                snapshot_key TEXT
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
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS images (
                id TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                pinned INTEGER NOT NULL DEFAULT 0,
                pulled_at INTEGER NOT NULL DEFAULT 0,
                source_reference TEXT,
                os TEXT,
                architecture TEXT,
                config_user TEXT,
                config_env_json TEXT NOT NULL DEFAULT '[]',
                config_entrypoint_json TEXT NOT NULL DEFAULT '[]',
                config_cmd_json TEXT NOT NULL DEFAULT '[]',
                config_working_dir TEXT,
                annotations_json TEXT NOT NULL DEFAULT '{}',
                declared_volumes_json TEXT NOT NULL DEFAULT '[]',
                manifest_media_type TEXT,
                selected_manifest_digest TEXT,
                selected_platform TEXT,
                stored_layers_json TEXT NOT NULL DEFAULT '[]',
                artifact_type TEXT,
                artifact_blobs_json TEXT NOT NULL DEFAULT '[]',
                cache_path TEXT
            )",
                [],
            )
            .context("Failed to create images table")?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS content_blobs (
                digest TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                relative_path TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL
            )",
                [],
            )
            .context("Failed to create content_blobs table")?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS content_blob_refs (
                owner_kind TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                digest TEXT NOT NULL,
                ref_kind TEXT NOT NULL,
                PRIMARY KEY(owner_kind, owner_id, digest, ref_kind)
            )",
                [],
            )
            .context("Failed to create content_blob_refs table")?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS content_transfers (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                provider TEXT NOT NULL,
                state TEXT NOT NULL,
                current_stage TEXT NOT NULL,
                bytes_total INTEGER NOT NULL DEFAULT 0,
                bytes_completed INTEGER NOT NULL DEFAULT 0,
                started_at INTEGER NOT NULL,
                finished_at INTEGER,
                error TEXT
            )",
                [],
            )
            .context("Failed to create content_transfers table")?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS image_refs (
                reference TEXT NOT NULL,
                image_id TEXT NOT NULL,
                namespace TEXT NOT NULL DEFAULT '',
                ref_kind TEXT NOT NULL,
                PRIMARY KEY(reference, image_id, namespace, ref_kind)
            )",
                [],
            )
            .context("Failed to create image_refs table")?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS snapshots (
                key TEXT PRIMARY KEY,
                image_id TEXT NOT NULL,
                owner_kind TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                state TEXT NOT NULL,
                mountpoint TEXT NOT NULL,
                snapshotter TEXT NOT NULL DEFAULT 'internal-overlay-untar',
                runtime_managed INTEGER NOT NULL DEFAULT 1
            )",
                [],
            )
            .context("Failed to create snapshots table")?;
        self.ensure_snapshot_metadata_columns()?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS runtime_artifacts (
                owner_kind TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                path TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'active',
                runtime_handler TEXT,
                runtime_root TEXT,
                PRIMARY KEY(owner_kind, owner_id, artifact_kind, path)
            )",
                [],
            )
            .context("Failed to create runtime_artifacts table")?;
        self.ensure_runtime_artifact_state_column()?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS shim_processes (
                container_id TEXT PRIMARY KEY,
                shim_pid INTEGER NOT NULL,
                work_dir TEXT NOT NULL,
                socket_path TEXT NOT NULL,
                exit_code_file TEXT NOT NULL,
                log_file TEXT NOT NULL,
                bundle_path TEXT NOT NULL,
                state TEXT NOT NULL,
                last_seen_at INTEGER NOT NULL
            )",
                [],
            )
            .context("Failed to create shim_processes table")?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL DEFAULT 'container',
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                old_state TEXT,
                new_state TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                details TEXT
            )",
                [],
            )
            .context("Failed to create events table")?;
        self.ensure_event_type_column()?;
        self.migrate_state_events_to_events()?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_image_refs_image_id ON image_refs(image_id)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_blobs_last_used ON content_blobs(last_used_at)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_blob_refs_digest ON content_blob_refs(digest)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_transfers_state ON content_transfers(state)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_owner ON snapshots(owner_kind, owner_id)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_artifacts_owner ON runtime_artifacts(owner_kind, owner_id)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_shim_processes_state ON shim_processes(state)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_entity_new ON events(entity_type, entity_id)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_time_new ON events(timestamp)",
            [],
        )?;
        self.run_schema_migrations()?;

        debug!("Database tables initialized");
        Ok(())
    }

    fn ensure_schema_version_table(&mut self) -> Result<()> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                migration_name TEXT NOT NULL,
                dirty INTEGER NOT NULL DEFAULT 0,
                applied_at INTEGER NOT NULL
            )",
                [],
            )
            .context("Failed to create schema_version table")?;
        Ok(())
    }

    fn reject_unsupported_or_dirty_schema(&self) -> Result<()> {
        if let Some(record) = self.latest_schema_migration()? {
            if record.dirty {
                anyhow::bail!(
                    "state database schema migration {} ({}) is dirty; refusing to start",
                    record.version,
                    record.migration_name
                );
            }
            if record.version > CURRENT_SCHEMA_VERSION {
                anyhow::bail!(
                    "state database schema version {} is newer than supported version {}",
                    record.version,
                    CURRENT_SCHEMA_VERSION
                );
            }
        }
        Ok(())
    }

    fn run_schema_migrations(&mut self) -> Result<()> {
        let current = self.schema_version()?;
        if current >= CURRENT_SCHEMA_VERSION {
            return Ok(());
        }
        self.apply_schema_migration(1, "baseline-current-ledger-schema", |manager| {
            manager.verify_current_schema()
        })?;
        Ok(())
    }

    fn apply_schema_migration<F>(&mut self, version: i64, name: &str, apply: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        if self.schema_version()? >= version {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();
        self.conn
            .execute(
                "INSERT INTO schema_version (version, migration_name, dirty, applied_at)
                 VALUES (?1, ?2, 1, ?3)",
                rusqlite::params![version, name, now],
            )
            .with_context(|| format!("Failed to mark schema migration {version} dirty"))?;
        apply(self).with_context(|| format!("Failed to apply schema migration {version}"))?;
        self.conn
            .execute(
                "UPDATE schema_version SET dirty = 0, applied_at = ?2 WHERE version = ?1",
                rusqlite::params![version, chrono::Utc::now().timestamp()],
            )
            .with_context(|| format!("Failed to mark schema migration {version} clean"))?;
        Ok(())
    }

    fn verify_current_schema(&self) -> Result<()> {
        for (table, required_columns) in [
            (
                "containers",
                vec!["runtime_handler", "runtime_backend", "snapshot_key"],
            ),
            ("snapshots", vec!["snapshotter", "runtime_managed"]),
            (
                "runtime_artifacts",
                vec!["state", "runtime_handler", "runtime_root"],
            ),
            (
                "content_transfers",
                vec!["state", "current_stage", "started_at", "finished_at"],
            ),
            ("events", vec!["event_type", "entity_type", "new_state"]),
        ] {
            let columns = self.table_columns(table)?;
            for required in required_columns {
                if !columns.iter().any(|column| column == required) {
                    anyhow::bail!(
                        "schema verification failed: table {table} missing column {required}"
                    );
                }
            }
        }
        Ok(())
    }

    fn ensure_event_type_column(&mut self) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("PRAGMA table_info(events)")
            .context("Failed to inspect events table")?;
        let has_event_type = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to read events table columns")?
            .iter()
            .any(|column| column == "event_type");
        drop(stmt);

        if !has_event_type {
            self.conn
                .execute(
                    "ALTER TABLE events ADD COLUMN event_type TEXT NOT NULL DEFAULT 'container'",
                    [],
                )
                .context("Failed to add event_type column to events")?;
            self.conn
                .execute(
                    "UPDATE events
                     SET event_type = CASE
                       WHEN entity_type = 'pod' THEN 'pod'
                       WHEN entity_type = 'shim_task' THEN 'task'
                       WHEN entity_type = 'shim_exec' THEN 'shim'
                       WHEN entity_type LIKE 'reconcile%' THEN 'reconcile'
                       ELSE 'container'
                     END",
                    [],
                )
                .context("Failed to backfill events.event_type")?;
        }

        Ok(())
    }

    fn ensure_runtime_artifact_state_column(&mut self) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("PRAGMA table_info(runtime_artifacts)")
            .context("Failed to inspect runtime_artifacts table")?;
        let has_state = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to read runtime_artifacts table columns")?
            .iter()
            .any(|column| column == "state");
        drop(stmt);

        if !has_state {
            self.conn
                .execute(
                    "ALTER TABLE runtime_artifacts ADD COLUMN state TEXT NOT NULL DEFAULT 'active'",
                    [],
                )
                .context("Failed to add state column to runtime_artifacts")?;
        }

        Ok(())
    }

    fn ensure_snapshot_metadata_columns(&mut self) -> Result<()> {
        let columns = self.table_columns("snapshots")?;
        if !columns.iter().any(|column| column == "snapshotter") {
            self.conn
                .execute(
                    "ALTER TABLE snapshots ADD COLUMN snapshotter TEXT NOT NULL DEFAULT 'internal-overlay-untar'",
                    [],
                )
                .context("Failed to add snapshotter column to snapshots")?;
        }
        if !columns.iter().any(|column| column == "runtime_managed") {
            self.conn
                .execute(
                    "ALTER TABLE snapshots ADD COLUMN runtime_managed INTEGER NOT NULL DEFAULT 1",
                    [],
                )
                .context("Failed to add runtime_managed column to snapshots")?;
        }
        Ok(())
    }

    fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .with_context(|| format!("Failed to inspect {table} table"))?;
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("Failed to read {table} table columns"))?;
        Ok(columns)
    }

    pub fn schema_version(&self) -> Result<i64> {
        Ok(self
            .latest_schema_migration()?
            .map(|record| record.version)
            .unwrap_or(0))
    }

    pub fn latest_schema_migration(&self) -> Result<Option<SchemaMigrationRecord>> {
        self.conn
            .query_row(
                "SELECT version, migration_name, dirty, applied_at
                 FROM schema_version
                 ORDER BY version DESC
                 LIMIT 1",
                [],
                |row| {
                    Ok(SchemaMigrationRecord {
                        version: row.get(0)?,
                        migration_name: row.get(1)?,
                        dirty: row.get::<_, i64>(2)? != 0,
                        applied_at: row.get(3)?,
                    })
                },
            )
            .optional()
            .context("Failed to read schema_version")
    }

    fn migrate_state_events_to_events(&mut self) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO events
                 (event_type, entity_type, entity_id, old_state, new_state, timestamp, details)
                 SELECT
                   CASE
                     WHEN se.entity_type = 'pod' THEN 'pod'
                     WHEN se.entity_type = 'shim_task' THEN 'task'
                     WHEN se.entity_type = 'shim_exec' THEN 'shim'
                     WHEN se.entity_type LIKE 'reconcile%' THEN 'reconcile'
                     ELSE 'container'
                   END,
                   se.entity_type,
                   se.entity_id,
                   se.old_state,
                   se.new_state,
                   se.timestamp,
                   se.details
                 FROM state_events se
                 WHERE NOT EXISTS (
                   SELECT 1 FROM events e
                   WHERE e.entity_type = se.entity_type
                     AND e.entity_id = se.entity_id
                     AND IFNULL(e.old_state, '') = IFNULL(se.old_state, '')
                     AND IFNULL(e.new_state, '') = IFNULL(se.new_state, '')
                     AND e.timestamp = se.timestamp
                     AND IFNULL(e.details, '') = IFNULL(se.details, '')
                 )",
                [],
            )
            .context("Failed to migrate state_events into events")?;
        Ok(())
    }

    pub fn integrity_check(&self) -> Result<bool> {
        let status: String = self
            .conn
            .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
            .context("Failed to run SQLite integrity_check")?;
        Ok(status.trim().eq_ignore_ascii_case("ok"))
    }

    pub fn attempt_repair(&mut self) -> Result<bool> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); REINDEX; VACUUM;")
            .context("Failed to run SQLite repair commands")?;
        self.integrity_check()
    }

    /// 保存容器记录
    pub fn save_container(&mut self, record: &ContainerRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO containers 
             (id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time, runtime_handler, runtime_backend, snapshot_key)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                record.runtime_handler.as_deref(),
                record.runtime_backend.as_deref(),
                record.snapshot_key.as_deref(),
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
            "SELECT id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time, runtime_handler, runtime_backend, snapshot_key
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
                    runtime_handler: row.get(10)?,
                    runtime_backend: row.get(11)?,
                    snapshot_key: row.get(12)?,
                })
            },
        ).optional().context("Failed to get container")?;

        Ok(record)
    }

    /// 获取所有容器
    pub fn list_containers(&self) -> Result<Vec<ContainerRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time, runtime_handler, runtime_backend, snapshot_key
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
                    runtime_handler: row.get(10)?,
                    runtime_backend: row.get(11)?,
                    snapshot_key: row.get(12)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list containers")?;

        Ok(records)
    }

    /// 获取Pod下的所有容器
    pub fn list_containers_by_pod(&self, pod_id: &str) -> Result<Vec<ContainerRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pod_id, state, image, command, created_at, labels, annotations, exit_code, exit_time, runtime_handler, runtime_backend, snapshot_key
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
                    runtime_handler: row.get(10)?,
                    runtime_backend: row.get(11)?,
                    snapshot_key: row.get(12)?,
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

    pub fn update_container_annotations(
        &mut self,
        container_id: &str,
        annotations: &str,
    ) -> Result<()> {
        self.conn
            .execute(
                "UPDATE containers SET annotations = ?2 WHERE id = ?1",
                (container_id, annotations),
            )
            .context("Failed to update container annotations")?;
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

    pub fn update_pod_annotations(&mut self, pod_id: &str, annotations: &str) -> Result<()> {
        self.conn
            .execute(
                "UPDATE pod_sandboxes SET annotations = ?2 WHERE id = ?1",
                (pod_id, annotations),
            )
            .context("Failed to update pod sandbox annotations")?;
        Ok(())
    }

    pub fn save_image(&mut self, record: &ImageRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO images
             (id, size, pinned, pulled_at, source_reference, os, architecture, config_user,
              config_env_json, config_entrypoint_json, config_cmd_json, config_working_dir,
              annotations_json, declared_volumes_json, manifest_media_type, selected_manifest_digest,
              selected_platform, stored_layers_json, artifact_type, artifact_blobs_json, cache_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            rusqlite::params![
                &record.id,
                record.size,
                record.pinned,
                record.pulled_at,
                record.source_reference.as_deref(),
                record.os.as_deref(),
                record.architecture.as_deref(),
                record.config_user.as_deref(),
                &record.config_env_json,
                &record.config_entrypoint_json,
                &record.config_cmd_json,
                record.config_working_dir.as_deref(),
                &record.annotations_json,
                &record.declared_volumes_json,
                record.manifest_media_type.as_deref(),
                record.selected_manifest_digest.as_deref(),
                record.selected_platform.as_deref(),
                &record.stored_layers_json,
                record.artifact_type.as_deref(),
                &record.artifact_blobs_json,
                record.cache_path.as_deref(),
            ],
        ).context("Failed to save image")?;
        Ok(())
    }

    pub fn get_image(&self, image_id: &str) -> Result<Option<ImageRecord>> {
        let record = self.conn.query_row(
            "SELECT id, size, pinned, pulled_at, source_reference, os, architecture, config_user,
                    config_env_json, config_entrypoint_json, config_cmd_json, config_working_dir,
                    annotations_json, declared_volumes_json, manifest_media_type, selected_manifest_digest,
                    selected_platform, stored_layers_json, artifact_type, artifact_blobs_json, cache_path
             FROM images WHERE id = ?1",
            [image_id],
            |row| {
                Ok(ImageRecord {
                    id: row.get(0)?,
                    size: row.get(1)?,
                    pinned: row.get(2)?,
                    pulled_at: row.get(3)?,
                    source_reference: row.get(4)?,
                    os: row.get(5)?,
                    architecture: row.get(6)?,
                    config_user: row.get(7)?,
                    config_env_json: row.get(8)?,
                    config_entrypoint_json: row.get(9)?,
                    config_cmd_json: row.get(10)?,
                    config_working_dir: row.get(11)?,
                    annotations_json: row.get(12)?,
                    declared_volumes_json: row.get(13)?,
                    manifest_media_type: row.get(14)?,
                    selected_manifest_digest: row.get(15)?,
                    selected_platform: row.get(16)?,
                    stored_layers_json: row.get(17)?,
                    artifact_type: row.get(18)?,
                    artifact_blobs_json: row.get(19)?,
                    cache_path: row.get(20)?,
                })
            },
        ).optional().context("Failed to get image")?;
        Ok(record)
    }

    pub fn list_images(&self) -> Result<Vec<ImageRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, size, pinned, pulled_at, source_reference, os, architecture, config_user,
                    config_env_json, config_entrypoint_json, config_cmd_json, config_working_dir,
                    annotations_json, declared_volumes_json, manifest_media_type, selected_manifest_digest,
                    selected_platform, stored_layers_json, artifact_type, artifact_blobs_json, cache_path
             FROM images",
        )?;
        let records = stmt
            .query_map([], |row| {
                Ok(ImageRecord {
                    id: row.get(0)?,
                    size: row.get(1)?,
                    pinned: row.get(2)?,
                    pulled_at: row.get(3)?,
                    source_reference: row.get(4)?,
                    os: row.get(5)?,
                    architecture: row.get(6)?,
                    config_user: row.get(7)?,
                    config_env_json: row.get(8)?,
                    config_entrypoint_json: row.get(9)?,
                    config_cmd_json: row.get(10)?,
                    config_working_dir: row.get(11)?,
                    annotations_json: row.get(12)?,
                    declared_volumes_json: row.get(13)?,
                    manifest_media_type: row.get(14)?,
                    selected_manifest_digest: row.get(15)?,
                    selected_platform: row.get(16)?,
                    stored_layers_json: row.get(17)?,
                    artifact_type: row.get(18)?,
                    artifact_blobs_json: row.get(19)?,
                    cache_path: row.get(20)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list images")?;
        Ok(records)
    }

    pub fn replace_image_refs(&mut self, image_id: &str, refs: &[ImageRefRecord]) -> Result<()> {
        self.conn
            .execute("DELETE FROM image_refs WHERE image_id = ?1", [image_id])
            .context("Failed to delete old image refs")?;
        for record in refs {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO image_refs (reference, image_id, namespace, ref_kind)
                 VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        &record.reference,
                        &record.image_id,
                        record.namespace.as_deref().unwrap_or(""),
                        &record.ref_kind,
                    ],
                )
                .context("Failed to save image ref")?;
        }
        Ok(())
    }

    pub fn list_image_refs(&self, image_id: Option<&str>) -> Result<Vec<ImageRefRecord>> {
        let sql = if image_id.is_some() {
            "SELECT reference, image_id, namespace, ref_kind FROM image_refs WHERE image_id = ?1"
        } else {
            "SELECT reference, image_id, namespace, ref_kind FROM image_refs"
        };
        let mut stmt = self.conn.prepare(sql)?;
        let mapper = |row: &rusqlite::Row<'_>| {
            Ok(ImageRefRecord {
                reference: row.get(0)?,
                image_id: row.get(1)?,
                namespace: {
                    let value: String = row.get(2)?;
                    (!value.is_empty()).then_some(value)
                },
                ref_kind: row.get(3)?,
            })
        };
        let records = match image_id {
            Some(id) => stmt.query_map([id], mapper)?,
            None => stmt.query_map([], mapper)?,
        }
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to list image refs")?;
        Ok(records)
    }

    pub fn delete_image(&mut self, image_id: &str) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM content_blob_refs WHERE owner_kind = 'image' AND owner_id = ?1",
                [image_id],
            )
            .context("Failed to delete image content blob refs")?;
        self.conn
            .execute("DELETE FROM image_refs WHERE image_id = ?1", [image_id])
            .context("Failed to delete image refs")?;
        self.conn
            .execute("DELETE FROM images WHERE id = ?1", [image_id])
            .context("Failed to delete image")?;
        Ok(())
    }

    pub fn save_content_blob(&mut self, record: &ContentBlobRecord) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO content_blobs
                 (digest, media_type, size, relative_path, created_at, last_used_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(digest) DO UPDATE SET
                   media_type = excluded.media_type,
                   size = excluded.size,
                   relative_path = excluded.relative_path,
                   last_used_at = excluded.last_used_at",
                rusqlite::params![
                    &record.digest,
                    &record.media_type,
                    record.size as i64,
                    &record.relative_path,
                    record.created_at,
                    record.last_used_at,
                ],
            )
            .context("Failed to save content blob")?;
        Ok(())
    }

    pub fn get_content_blob(&self, digest: &str) -> Result<Option<ContentBlobRecord>> {
        self.conn
            .query_row(
                "SELECT digest, media_type, size, relative_path, created_at, last_used_at
                 FROM content_blobs WHERE digest = ?1",
                [digest],
                |row| {
                    let size: i64 = row.get(2)?;
                    Ok(ContentBlobRecord {
                        digest: row.get(0)?,
                        media_type: row.get(1)?,
                        size: size.max(0) as u64,
                        relative_path: row.get(3)?,
                        created_at: row.get(4)?,
                        last_used_at: row.get(5)?,
                    })
                },
            )
            .optional()
            .context("Failed to get content blob")
    }

    pub fn touch_content_blob(&mut self, digest: &str, last_used_at: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE content_blobs SET last_used_at = ?2 WHERE digest = ?1",
                rusqlite::params![digest, last_used_at],
            )
            .context("Failed to touch content blob")?;
        Ok(())
    }

    pub fn delete_content_blob(&mut self, digest: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM content_blob_refs WHERE digest = ?1", [digest])
            .context("Failed to delete content blob refs")?;
        self.conn
            .execute("DELETE FROM content_blobs WHERE digest = ?1", [digest])
            .context("Failed to delete content blob")?;
        Ok(())
    }

    pub fn replace_content_blob_refs(
        &mut self,
        owner_kind: &str,
        owner_id: &str,
        records: &[ContentBlobRefRecord],
    ) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM content_blob_refs WHERE owner_kind = ?1 AND owner_id = ?2",
                [owner_kind, owner_id],
            )
            .context("Failed to delete content blob refs")?;
        for record in records {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO content_blob_refs
                     (owner_kind, owner_id, digest, ref_kind)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        &record.owner_kind,
                        &record.owner_id,
                        &record.digest,
                        &record.ref_kind,
                    ],
                )
                .context("Failed to save content blob ref")?;
        }
        Ok(())
    }

    pub fn list_content_blob_refs(
        &self,
        owner_kind: Option<&str>,
        owner_id: Option<&str>,
    ) -> Result<Vec<ContentBlobRefRecord>> {
        let mut sql =
            "SELECT owner_kind, owner_id, digest, ref_kind FROM content_blob_refs".to_string();
        let records = match (owner_kind, owner_id) {
            (Some(kind), Some(id)) => {
                sql.push_str(" WHERE owner_kind = ?1 AND owner_id = ?2");
                let mut stmt = self.conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(rusqlite::params![kind, id], content_blob_ref_from_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (Some(kind), None) => {
                sql.push_str(" WHERE owner_kind = ?1");
                let mut stmt = self.conn.prepare(&sql)?;
                let rows = stmt.query_map([kind], content_blob_ref_from_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (None, Some(id)) => {
                sql.push_str(" WHERE owner_id = ?1");
                let mut stmt = self.conn.prepare(&sql)?;
                let rows = stmt.query_map([id], content_blob_ref_from_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (None, None) => {
                let mut stmt = self.conn.prepare(&sql)?;
                let rows = stmt.query_map([], content_blob_ref_from_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
        };
        Ok(records)
    }

    pub fn list_content_blobs(&self) -> Result<Vec<ContentBlobRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT digest, media_type, size, relative_path, created_at, last_used_at
             FROM content_blobs",
        )?;
        let records = stmt
            .query_map([], |row| {
                let size: i64 = row.get(2)?;
                Ok(ContentBlobRecord {
                    digest: row.get(0)?,
                    media_type: row.get(1)?,
                    size: size.max(0) as u64,
                    relative_path: row.get(3)?,
                    created_at: row.get(4)?,
                    last_used_at: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list content blobs")?;
        Ok(records)
    }

    pub fn list_content_gc_candidates(&self) -> Result<Vec<ContentGcCandidate>> {
        let refs = self.list_content_blob_refs(None, None)?;
        let transfers = self.list_content_transfers()?;
        self.list_content_blobs()?
            .into_iter()
            .map(|blob| {
                let mut blockers = refs
                    .iter()
                    .filter(|record| record.digest == blob.digest)
                    .map(|record| ContentGcBlocker::ContentRef {
                        owner_kind: record.owner_kind.clone(),
                        owner_id: record.owner_id.clone(),
                        ref_kind: record.ref_kind.clone(),
                    })
                    .collect::<Vec<_>>();
                blockers.extend(
                    transfers
                        .iter()
                        .filter(|record| record.state == "running")
                        .filter(|record| transfer_source_matches_blob(record, &blob))
                        .map(|record| ContentGcBlocker::ActiveTransfer {
                            transfer_id: record.id.clone(),
                            source: record.source.clone(),
                        }),
                );
                Ok(ContentGcCandidate { blob, blockers })
            })
            .collect()
    }

    pub fn save_content_transfer(&mut self, record: &ContentTransferRecord) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO content_transfers
                 (id, source, provider, state, current_stage, bytes_total, bytes_completed, started_at, finished_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(id) DO UPDATE SET
                   source = excluded.source,
                   provider = excluded.provider,
                   state = excluded.state,
                   current_stage = excluded.current_stage,
                   bytes_total = excluded.bytes_total,
                   bytes_completed = excluded.bytes_completed,
                   finished_at = excluded.finished_at,
                   error = excluded.error",
                rusqlite::params![
                    &record.id,
                    &record.source,
                    &record.provider,
                    &record.state,
                    &record.current_stage,
                    record.bytes_total as i64,
                    record.bytes_completed as i64,
                    record.started_at,
                    record.finished_at,
                    record.error.as_deref(),
                ],
            )
            .context("Failed to save content transfer")?;
        Ok(())
    }

    pub fn list_content_transfers(&self) -> Result<Vec<ContentTransferRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source, provider, state, current_stage, bytes_total, bytes_completed, started_at, finished_at, error
             FROM content_transfers
             ORDER BY started_at DESC, id DESC",
        )?;
        let records = stmt
            .query_map([], |row| {
                let bytes_total: i64 = row.get(5)?;
                let bytes_completed: i64 = row.get(6)?;
                Ok(ContentTransferRecord {
                    id: row.get(0)?,
                    source: row.get(1)?,
                    provider: row.get(2)?,
                    state: row.get(3)?,
                    current_stage: row.get(4)?,
                    bytes_total: bytes_total.max(0) as u64,
                    bytes_completed: bytes_completed.max(0) as u64,
                    started_at: row.get(7)?,
                    finished_at: row.get(8)?,
                    error: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list content transfers")?;
        Ok(records)
    }

    pub fn mark_running_content_transfers_interrupted(
        &mut self,
        finished_at: i64,
    ) -> Result<usize> {
        let changed = self
            .conn
            .execute(
                "UPDATE content_transfers
                 SET state = 'interrupted',
                     current_stage = 'interrupted',
                     finished_at = ?1,
                     error = 'daemon restarted before transfer completed'
                 WHERE state = 'running'",
                [finished_at],
            )
            .context("Failed to mark interrupted content transfers")?;
        Ok(changed)
    }

    pub fn save_snapshot(&mut self, record: &SnapshotRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO snapshots (key, image_id, owner_kind, owner_id, state, mountpoint, snapshotter, runtime_managed)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                &record.key,
                &record.image_id,
                &record.owner_kind,
                &record.owner_id,
                &record.state,
                &record.mountpoint,
                &record.snapshotter,
                record.runtime_managed,
            ],
        ).context("Failed to save snapshot")?;
        Ok(())
    }

    pub fn update_snapshot_state(&mut self, key: &str, state: &str) -> Result<()> {
        self.conn
            .execute(
                "UPDATE snapshots SET state = ?2 WHERE key = ?1",
                rusqlite::params![key, state],
            )
            .context("Failed to update snapshot state")?;
        Ok(())
    }

    pub fn list_snapshots(&self) -> Result<Vec<SnapshotRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT key, image_id, owner_kind, owner_id, state, mountpoint, snapshotter, runtime_managed FROM snapshots",
        )?;
        let records = stmt
            .query_map([], |row| {
                Ok(SnapshotRecord {
                    key: row.get(0)?,
                    image_id: row.get(1)?,
                    owner_kind: row.get(2)?,
                    owner_id: row.get(3)?,
                    state: row.get(4)?,
                    mountpoint: row.get(5)?,
                    snapshotter: row.get(6)?,
                    runtime_managed: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list snapshots")?;
        Ok(records)
    }

    pub fn delete_snapshot(&mut self, key: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM snapshots WHERE key = ?1", [key])
            .context("Failed to delete snapshot")?;
        Ok(())
    }

    pub fn replace_runtime_artifacts(
        &mut self,
        owner_kind: &str,
        owner_id: &str,
        records: &[RuntimeArtifactRecord],
    ) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM runtime_artifacts WHERE owner_kind = ?1 AND owner_id = ?2",
                [owner_kind, owner_id],
            )
            .context("Failed to delete runtime artifacts")?;
        for record in records {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO runtime_artifacts
                 (owner_kind, owner_id, artifact_kind, path, state, runtime_handler, runtime_root)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        &record.owner_kind,
                        &record.owner_id,
                        &record.artifact_kind,
                        &record.path,
                        &record.state,
                        record.runtime_handler.as_deref(),
                        record.runtime_root.as_deref(),
                    ],
                )
                .context("Failed to save runtime artifact")?;
        }
        Ok(())
    }

    pub fn list_runtime_artifacts(&self) -> Result<Vec<RuntimeArtifactRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT owner_kind, owner_id, artifact_kind, path, state, runtime_handler, runtime_root
             FROM runtime_artifacts",
        )?;
        let records = stmt
            .query_map([], |row| {
                Ok(RuntimeArtifactRecord {
                    owner_kind: row.get(0)?,
                    owner_id: row.get(1)?,
                    artifact_kind: row.get(2)?,
                    path: row.get(3)?,
                    state: row.get(4)?,
                    runtime_handler: row.get(5)?,
                    runtime_root: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list runtime artifacts")?;
        Ok(records)
    }

    pub fn update_runtime_artifact_state(
        &mut self,
        owner_kind: &str,
        owner_id: &str,
        artifact_kind: &str,
        path: &str,
        state: &str,
    ) -> Result<()> {
        self.conn
            .execute(
                "UPDATE runtime_artifacts
                 SET state = ?5
                 WHERE owner_kind = ?1 AND owner_id = ?2 AND artifact_kind = ?3 AND path = ?4",
                rusqlite::params![owner_kind, owner_id, artifact_kind, path, state],
            )
            .context("Failed to update runtime artifact state")?;
        Ok(())
    }

    pub fn delete_runtime_artifacts(&mut self, owner_kind: &str, owner_id: &str) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM runtime_artifacts WHERE owner_kind = ?1 AND owner_id = ?2",
                [owner_kind, owner_id],
            )
            .context("Failed to delete runtime artifacts")?;
        Ok(())
    }

    pub fn save_shim_process(&mut self, record: &ShimProcessRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO shim_processes
             (container_id, shim_pid, work_dir, socket_path, exit_code_file, log_file, bundle_path, state, last_seen_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                &record.container_id,
                record.shim_pid,
                &record.work_dir,
                &record.socket_path,
                &record.exit_code_file,
                &record.log_file,
                &record.bundle_path,
                &record.state,
                record.last_seen_at,
            ],
        ).context("Failed to save shim process")?;
        Ok(())
    }

    pub fn update_shim_process_state(&mut self, container_id: &str, state: &str) -> Result<()> {
        self.conn
            .execute(
                "UPDATE shim_processes SET state = ?2, last_seen_at = ?3 WHERE container_id = ?1",
                rusqlite::params![container_id, state, chrono::Utc::now().timestamp()],
            )
            .context("Failed to update shim process state")?;
        Ok(())
    }

    pub fn get_shim_process(&self, container_id: &str) -> Result<Option<ShimProcessRecord>> {
        let record = self.conn.query_row(
            "SELECT container_id, shim_pid, work_dir, socket_path, exit_code_file, log_file, bundle_path, state, last_seen_at
             FROM shim_processes WHERE container_id = ?1",
            [container_id],
            |row| {
                Ok(ShimProcessRecord {
                    container_id: row.get(0)?,
                    shim_pid: row.get(1)?,
                    work_dir: row.get(2)?,
                    socket_path: row.get(3)?,
                    exit_code_file: row.get(4)?,
                    log_file: row.get(5)?,
                    bundle_path: row.get(6)?,
                    state: row.get(7)?,
                    last_seen_at: row.get(8)?,
                })
            },
        ).optional().context("Failed to get shim process")?;
        Ok(record)
    }

    pub fn list_shim_processes(&self) -> Result<Vec<ShimProcessRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT container_id, shim_pid, work_dir, socket_path, exit_code_file, log_file, bundle_path, state, last_seen_at
             FROM shim_processes",
        )?;
        let records = stmt
            .query_map([], |row| {
                Ok(ShimProcessRecord {
                    container_id: row.get(0)?,
                    shim_pid: row.get(1)?,
                    work_dir: row.get(2)?,
                    socket_path: row.get(3)?,
                    exit_code_file: row.get(4)?,
                    log_file: row.get(5)?,
                    bundle_path: row.get(6)?,
                    state: row.get(7)?,
                    last_seen_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to list shim processes")?;
        Ok(records)
    }

    pub fn delete_shim_process(&mut self, container_id: &str) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM shim_processes WHERE container_id = ?1",
                [container_id],
            )
            .context("Failed to delete shim process")?;
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
        let event_type = Self::ledger_event_type(entity_type);
        self.append_typed_event(
            event_type,
            entity_type,
            entity_id,
            old_state,
            new_state,
            None,
        )
    }

    fn ledger_event_type(entity_type: &str) -> &'static str {
        match entity_type {
            "pod" => "pod",
            "shim_task" => "task",
            "shim_exec" => "shim",
            value if value.starts_with("reconcile") => "reconcile",
            _ => "container",
        }
    }

    pub fn append_event(
        &mut self,
        entity_type: &str,
        entity_id: &str,
        old_state: Option<&str>,
        new_state: Option<&str>,
        details: Option<&str>,
    ) -> Result<()> {
        let event_type = Self::ledger_event_type(entity_type);
        self.append_typed_event(
            event_type,
            entity_type,
            entity_id,
            old_state,
            new_state,
            details,
        )
    }

    pub fn append_typed_event(
        &mut self,
        event_type: &str,
        entity_type: &str,
        entity_id: &str,
        old_state: Option<&str>,
        new_state: Option<&str>,
        details: Option<&str>,
    ) -> Result<()> {
        let timestamp = chrono::Utc::now().timestamp();
        self.append_typed_event_at(TypedEventInput {
            event_type,
            entity_type,
            entity_id,
            old_state,
            new_state,
            details,
            timestamp,
        })
    }

    pub fn append_typed_event_at(&mut self, input: TypedEventInput<'_>) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO events
                 (event_type, entity_type, entity_id, old_state, new_state, timestamp, details)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    input.event_type,
                    input.entity_type,
                    input.entity_id,
                    input.old_state.unwrap_or(""),
                    input.new_state.unwrap_or(""),
                    input.timestamp,
                    input.details,
                ],
            )
            .context("Failed to append state event")?;
        Ok(())
    }

    /// 获取最近的实体状态事件
    pub fn get_recent_events(&self, entity_type: &str, since: i64) -> Result<Vec<StateEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, entity_type, entity_id, old_state, new_state, timestamp, details
             FROM events
             WHERE entity_type = ?1 AND timestamp >= ?2
             ORDER BY timestamp DESC",
        )?;

        let events = stmt
            .query_map([entity_type, &since.to_string()], |row| {
                Ok(StateEvent {
                    id: row.get(0)?,
                    event_type: row.get(1)?,
                    entity_type: row.get(2)?,
                    entity_id: row.get(3)?,
                    old_state: row.get(4)?,
                    new_state: row.get(5)?,
                    timestamp: row.get(6)?,
                    details: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to get recent events")?;

        Ok(events)
    }

    pub fn get_recent_events_for_subject(
        &self,
        entity_type: &str,
        entity_id: &str,
        limit: usize,
    ) -> Result<Vec<StateEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, entity_type, entity_id, old_state, new_state, timestamp, details
             FROM events
             WHERE entity_type = ?1 AND entity_id = ?2
             ORDER BY timestamp DESC, id DESC
             LIMIT ?3",
        )?;

        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let events = stmt
            .query_map(rusqlite::params![entity_type, entity_id, limit], |row| {
                Ok(StateEvent {
                    id: row.get(0)?,
                    event_type: row.get(1)?,
                    entity_type: row.get(2)?,
                    entity_id: row.get(3)?,
                    old_state: row.get(4)?,
                    new_state: row.get(5)?,
                    timestamp: row.get(6)?,
                    details: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to get recent events for subject")?;

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
    pub event_type: String,
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
    use crate::image::{ArtifactBlobMeta, StoredLayerMeta};
    use tempfile::tempdir;

    #[test]
    fn test_storage_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let manager = StorageManager::new(&db_path);
        assert!(manager.is_ok());
        let manager = manager.unwrap();
        assert_eq!(manager.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
        assert_eq!(
            manager
                .latest_schema_migration()
                .unwrap()
                .unwrap()
                .migration_name,
            "baseline-current-ledger-schema"
        );
    }

    #[test]
    fn schema_migration_is_idempotent_across_reopens() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        StorageManager::new(&db_path).unwrap().close().unwrap();
        StorageManager::new(&db_path).unwrap().close().unwrap();

        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn schema_migration_rejects_dirty_state() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        StorageManager::new(&db_path).unwrap().close().unwrap();
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("UPDATE schema_version SET dirty = 1", [])
            .unwrap();
        drop(conn);

        let err = StorageManager::new(&db_path).unwrap_err();
        assert!(err.to_string().contains("dirty"));
    }

    #[test]
    fn schema_migration_rejects_future_version() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        StorageManager::new(&db_path).unwrap().close().unwrap();
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("DELETE FROM schema_version", []).unwrap();
        conn.execute(
            "INSERT INTO schema_version (version, migration_name, dirty, applied_at)
             VALUES (?1, ?2, 0, ?3)",
            rusqlite::params![CURRENT_SCHEMA_VERSION + 1, "future", 1_i64],
        )
        .unwrap();
        drop(conn);

        let err = StorageManager::new(&db_path).unwrap_err();
        assert!(err.to_string().contains("newer than supported"));
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
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
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
    fn container_state_changes_write_single_typed_event_table() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut manager = StorageManager::new(&db_path).unwrap();

        manager
            .save_container(&ContainerRecord {
                id: "container-events".to_string(),
                pod_id: "pod-1".to_string(),
                state: "created".to_string(),
                image: "test:latest".to_string(),
                command: "sleep 60".to_string(),
                created_at: 1,
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                exit_code: None,
                exit_time: None,
                runtime_handler: None,
                runtime_backend: None,
                snapshot_key: None,
            })
            .unwrap();
        manager
            .update_container_state("container-events", "running", None)
            .unwrap();

        let events = manager.get_recent_events("container", 0).unwrap();
        let running_events = events
            .iter()
            .filter(|event| {
                event.entity_id == "container-events"
                    && event.event_type == "container"
                    && event.old_state == "created"
                    && event.new_state == "running"
            })
            .count();
        assert_eq!(running_events, 1);

        let legacy_count: i64 = manager
            .conn
            .query_row("SELECT COUNT(*) FROM state_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(legacy_count, 0);
    }

    #[test]
    fn legacy_state_events_are_migrated_to_unified_events_once() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let manager = StorageManager::new(&db_path).unwrap();
        manager
            .conn
            .execute(
                "INSERT INTO state_events
                 (entity_type, entity_id, old_state, new_state, timestamp, details)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params!["pod", "pod-events", "notready", "ready", 1234_i64, "legacy"],
            )
            .unwrap();
        manager.close().unwrap();

        let manager = StorageManager::new(&db_path).unwrap();
        let events = manager.get_recent_events("pod", 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "pod");
        assert_eq!(events[0].entity_id, "pod-events");
        assert_eq!(events[0].details.as_deref(), Some("legacy"));
        manager.close().unwrap();

        let manager = StorageManager::new(&db_path).unwrap();
        let event_count: i64 = manager
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(event_count, 1);
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

    #[test]
    fn test_unified_ledger_tables_crud() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut manager = StorageManager::new(&db_path).unwrap();

        manager
            .save_image(&ImageRecord {
                id: "sha256:image-1".to_string(),
                size: 42,
                pinned: true,
                pulled_at: 1,
                source_reference: Some("busybox:latest".to_string()),
                os: Some("linux".to_string()),
                architecture: Some("amd64".to_string()),
                config_user: Some("0".to_string()),
                config_env_json: "[\"A=B\"]".to_string(),
                config_entrypoint_json: "[\"/bin/sh\"]".to_string(),
                config_cmd_json: "[\"-c\",\"echo hi\"]".to_string(),
                config_working_dir: Some("/".to_string()),
                annotations_json: "{\"team\":\"runtime\"}".to_string(),
                declared_volumes_json: "[\"/data\"]".to_string(),
                manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
                selected_manifest_digest: Some("sha256:manifest".to_string()),
                selected_platform: Some("linux/amd64".to_string()),
                stored_layers_json: serde_json::to_string(&vec![StoredLayerMeta {
                    digest: "sha256:layer".to_string(),
                    path: "blobs/sha256/la/yer".to_string(),
                    media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                    source_media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                    encrypted: false,
                }])
                .unwrap(),
                artifact_type: None,
                artifact_blobs_json: serde_json::to_string(&Vec::<ArtifactBlobMeta>::new())
                    .unwrap(),
                cache_path: Some("/tmp/cache".to_string()),
            })
            .unwrap();
        manager
            .replace_image_refs(
                "sha256:image-1",
                &[
                    ImageRefRecord {
                        reference: "busybox:latest".to_string(),
                        image_id: "sha256:image-1".to_string(),
                        namespace: None,
                        ref_kind: "tag".to_string(),
                    },
                    ImageRefRecord {
                        reference: "docker.io/library/busybox@sha256:image-1".to_string(),
                        image_id: "sha256:image-1".to_string(),
                        namespace: None,
                        ref_kind: "digest".to_string(),
                    },
                ],
            )
            .unwrap();
        assert!(manager.get_image("sha256:image-1").unwrap().is_some());
        assert_eq!(
            manager
                .list_image_refs(Some("sha256:image-1"))
                .unwrap()
                .len(),
            2
        );

        manager
            .save_snapshot(&SnapshotRecord {
                key: "container-1".to_string(),
                image_id: "sha256:image-1".to_string(),
                owner_kind: "container".to_string(),
                owner_id: "container-1".to_string(),
                state: "prepared".to_string(),
                mountpoint: "/tmp/rootfs".to_string(),
                snapshotter: "internal-overlay-untar".to_string(),
                runtime_managed: true,
            })
            .unwrap();
        assert_eq!(manager.list_snapshots().unwrap().len(), 1);

        manager
            .replace_runtime_artifacts(
                "container",
                "container-1",
                &[RuntimeArtifactRecord {
                    owner_kind: "container".to_string(),
                    owner_id: "container-1".to_string(),
                    artifact_kind: "bundle".to_string(),
                    path: "/tmp/bundle".to_string(),
                    state: "active".to_string(),
                    runtime_handler: Some("runc".to_string()),
                    runtime_root: Some("/run/crius".to_string()),
                }],
            )
            .unwrap();
        assert_eq!(manager.list_runtime_artifacts().unwrap().len(), 1);

        manager
            .save_shim_process(&ShimProcessRecord {
                container_id: "container-1".to_string(),
                shim_pid: 1234,
                work_dir: "/tmp/shims".to_string(),
                socket_path: "/tmp/shims/container-1/attach.sock".to_string(),
                exit_code_file: "/tmp/exits/container-1".to_string(),
                log_file: "/tmp/shims/container-1/shim.log".to_string(),
                bundle_path: "/tmp/bundle".to_string(),
                state: "running".to_string(),
                last_seen_at: 1,
            })
            .unwrap();
        assert!(manager.get_shim_process("container-1").unwrap().is_some());
    }

    #[test]
    fn content_gc_candidates_report_refs_and_active_transfer_blockers() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut manager = StorageManager::new(&db_path).unwrap();

        for digest in [
            "sha256:shared",
            "sha256:active",
            "sha256:collectable",
            "sha256:done",
        ] {
            manager
                .save_content_blob(&ContentBlobRecord {
                    digest: digest.to_string(),
                    media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                    size: 10,
                    relative_path: format!("blobs/sha256/{}", digest.trim_start_matches("sha256:")),
                    created_at: 1,
                    last_used_at: 1,
                })
                .unwrap();
        }

        manager
            .replace_content_blob_refs(
                "image",
                "sha256:image-a",
                &[ContentBlobRefRecord {
                    owner_kind: "image".to_string(),
                    owner_id: "sha256:image-a".to_string(),
                    digest: "sha256:shared".to_string(),
                    ref_kind: "layer".to_string(),
                }],
            )
            .unwrap();
        manager
            .save_content_transfer(&ContentTransferRecord {
                id: "transfer-active".to_string(),
                source: "pull:sha256:active".to_string(),
                provider: "registry".to_string(),
                state: "running".to_string(),
                current_stage: "downloading".to_string(),
                bytes_total: 10,
                bytes_completed: 5,
                started_at: 2,
                finished_at: None,
                error: None,
            })
            .unwrap();
        manager
            .save_content_transfer(&ContentTransferRecord {
                id: "transfer-done".to_string(),
                source: "pull:sha256:done".to_string(),
                provider: "registry".to_string(),
                state: "succeeded".to_string(),
                current_stage: "done".to_string(),
                bytes_total: 10,
                bytes_completed: 10,
                started_at: 3,
                finished_at: Some(4),
                error: None,
            })
            .unwrap();

        let candidates = manager.list_content_gc_candidates().unwrap();
        let by_digest = candidates
            .iter()
            .map(|candidate| (candidate.blob.digest.as_str(), candidate))
            .collect::<std::collections::HashMap<_, _>>();

        assert!(matches!(
            by_digest["sha256:shared"].blockers.as_slice(),
            [ContentGcBlocker::ContentRef { .. }]
        ));
        assert!(matches!(
            by_digest["sha256:active"].blockers.as_slice(),
            [ContentGcBlocker::ActiveTransfer { .. }]
        ));
        assert!(by_digest["sha256:collectable"].blockers.is_empty());
        assert!(by_digest["sha256:done"].blockers.is_empty());
    }

    #[test]
    fn shared_blob_stays_blocked_until_all_image_refs_are_removed() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut manager = StorageManager::new(&db_path).unwrap();
        manager
            .save_content_blob(&ContentBlobRecord {
                digest: "sha256:shared-layer".to_string(),
                media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                size: 64,
                relative_path: "blobs/sha256/shared-layer".to_string(),
                created_at: 1,
                last_used_at: 1,
            })
            .unwrap();
        for image_id in ["sha256:image-a", "sha256:image-b"] {
            manager
                .replace_content_blob_refs(
                    "image",
                    image_id,
                    &[ContentBlobRefRecord {
                        owner_kind: "image".to_string(),
                        owner_id: image_id.to_string(),
                        digest: "sha256:shared-layer".to_string(),
                        ref_kind: "layer".to_string(),
                    }],
                )
                .unwrap();
        }

        let blockers = |manager: &StorageManager| {
            manager
                .list_content_gc_candidates()
                .unwrap()
                .into_iter()
                .find(|candidate| candidate.blob.digest == "sha256:shared-layer")
                .unwrap()
                .blockers
                .len()
        };
        assert_eq!(blockers(&manager), 2);

        manager.delete_image("sha256:image-a").unwrap();
        assert_eq!(blockers(&manager), 1);

        manager.delete_image("sha256:image-b").unwrap();
        assert_eq!(blockers(&manager), 0);
    }
}
