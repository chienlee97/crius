use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::proto::runtime::v1::ContainerEventResponse;

#[derive(Debug, Clone)]
pub struct LedgerInternalEventSink {
    db_path: std::path::PathBuf,
}

#[derive(Debug, Clone)]
pub struct EventService {
    sender: tokio::sync::broadcast::Sender<ContainerEventResponse>,
    internal_sender: tokio::sync::broadcast::Sender<InternalEvent>,
    ledger:
        Option<std::sync::Arc<tokio::sync::Mutex<crate::storage::persistence::PersistenceManager>>>,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InternalEventSeverity {
    Debug,
    Info,
    Warning,
    Error,
}

impl InternalEventSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }

    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warning" => Ok(Self::Warning),
            "error" => Ok(Self::Error),
            other => anyhow::bail!("invalid internal event severity: {other}"),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InternalEvent {
    pub kind: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub severity: InternalEventSeverity,
    pub timestamp: i64,
    pub details: serde_json::Value,
}

impl InternalEvent {
    pub fn new(
        kind: impl Into<String>,
        subject_kind: impl Into<String>,
        subject_id: impl Into<String>,
        severity: InternalEventSeverity,
        details: serde_json::Value,
    ) -> Self {
        Self::with_timestamp(
            kind,
            subject_kind,
            subject_id,
            severity,
            chrono::Utc::now().timestamp(),
            details,
        )
    }

    pub fn with_timestamp(
        kind: impl Into<String>,
        subject_kind: impl Into<String>,
        subject_id: impl Into<String>,
        severity: InternalEventSeverity,
        timestamp: i64,
        details: serde_json::Value,
    ) -> Self {
        Self {
            kind: kind.into(),
            subject_kind: subject_kind.into(),
            subject_id: subject_id.into(),
            severity,
            timestamp,
            details,
        }
    }

    fn from_state_event(event: crate::storage::StateEvent) -> anyhow::Result<Self> {
        let details = event
            .details
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        Ok(Self {
            kind: event.event_type,
            subject_kind: event.entity_type,
            subject_id: event.entity_id,
            severity: InternalEventSeverity::parse(&event.new_state)?,
            timestamp: event.timestamp,
            details,
        })
    }

    fn details_for_ledger(&self) -> Option<String> {
        (!self.details.is_null()).then(|| self.details.to_string())
    }
}

impl LedgerInternalEventSink {
    pub fn new(db_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }

    pub fn publish(&self, event: &InternalEvent) -> anyhow::Result<()> {
        let mut storage = crate::storage::StorageManager::new(&self.db_path)?;
        storage.append_typed_event_at(crate::storage::TypedEventInput {
            event_type: &event.kind,
            entity_type: &event.subject_kind,
            entity_id: &event.subject_id,
            old_state: None,
            new_state: Some(event.severity.as_str()),
            details: event.details_for_ledger().as_deref(),
            timestamp: event.timestamp,
        })
    }
}

impl EventService {
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        let (internal_sender, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            sender,
            internal_sender,
            ledger: None,
        }
    }

    pub fn from_sender(sender: tokio::sync::broadcast::Sender<ContainerEventResponse>) -> Self {
        let (internal_sender, _) = tokio::sync::broadcast::channel(256);
        Self {
            sender,
            internal_sender,
            ledger: None,
        }
    }

    pub fn with_ledger(
        mut self,
        ledger: std::sync::Arc<tokio::sync::Mutex<crate::storage::persistence::PersistenceManager>>,
    ) -> Self {
        self.ledger = Some(ledger);
        self
    }

    pub fn sender(&self) -> tokio::sync::broadcast::Sender<ContainerEventResponse> {
        self.sender.clone()
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ContainerEventResponse> {
        self.sender.subscribe()
    }

    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    pub fn internal_subscriber_count(&self) -> usize {
        self.internal_sender.receiver_count()
    }

    pub fn publish(&self, event: ContainerEventResponse) {
        if let Err(err) = self.sender.send(event) {
            log::debug!("Dropping CRI event without subscribers: {}", err);
        }
    }

    pub fn subscribe_internal(&self) -> tokio::sync::broadcast::Receiver<InternalEvent> {
        self.internal_sender.subscribe()
    }

    pub async fn publish_internal(&self, event: InternalEvent) -> anyhow::Result<()> {
        let persist_result = self.persist_internal_event(&event).await;
        if let Err(err) = self.internal_sender.send(event) {
            log::debug!("Dropping internal event without subscribers: {}", err);
        }
        persist_result
    }

    pub async fn recent_internal_events(
        &self,
        subject_kind: &str,
        subject_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<InternalEvent>> {
        let Some(ledger) = &self.ledger else {
            return Ok(Vec::new());
        };
        let persistence = ledger.lock().await;
        let ledger = crate::state::StateLedger::new(&persistence);
        let mut events = Vec::new();
        for event in ledger.recent_events_for_subject(subject_kind, subject_id, limit)? {
            match InternalEvent::from_state_event(event) {
                Ok(event) => events.push(event),
                Err(err) => log::debug!("Skipping non-internal ledger event: {}", err),
            }
        }
        Ok(events)
    }

    async fn persist_internal_event(&self, event: &InternalEvent) -> anyhow::Result<()> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut persistence = ledger.lock().await;
        let mut ledger = crate::state::StateLedgerWriter::new(&mut persistence);
        ledger.append_typed_event_at(crate::storage::TypedEventInput {
            event_type: &event.kind,
            entity_type: &event.subject_kind,
            entity_id: &event.subject_id,
            old_state: None,
            new_state: Some(event.severity.as_str()),
            details: event.details_for_ledger().as_deref(),
            timestamp: event.timestamp,
        })
    }

    pub fn stream(&self) -> ReceiverStream<Result<ContainerEventResponse, Status>> {
        let mut events = self.subscribe();
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => {
                        if tx.send(Ok(event)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        if tx
                            .send(Err(Status::resource_exhausted(
                                "CRI event stream lagged behind producer",
                            )))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        ReceiverStream::new(rx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_stream::StreamExt;

    use super::*;
    use crate::proto::runtime::v1::ContainerEventType;
    use crate::storage::persistence::{PersistenceConfig, PersistenceManager};
    use tempfile::tempdir;

    #[tokio::test]
    async fn streams_published_events_to_subscribers() {
        let events = EventService::with_capacity(16);
        let mut stream = events.stream();

        events.publish(ContainerEventResponse {
            container_id: "container-1".to_string(),
            container_event_type: ContainerEventType::ContainerStartedEvent as i32,
            created_at: 42,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });

        let event = stream.next().await.unwrap().unwrap();
        assert_eq!(event.container_id, "container-1");
        assert_eq!(
            event.container_event_type,
            ContainerEventType::ContainerStartedEvent as i32
        );
    }

    #[tokio::test]
    async fn reports_subscriber_count() {
        let events = EventService::with_capacity(16);
        let _rx = events.subscribe();

        assert_eq!(events.subscriber_count(), 1);
    }

    #[tokio::test]
    async fn persists_and_reloads_internal_events_by_subject() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("events.db");
        let persistence = Arc::new(tokio::sync::Mutex::new(
            PersistenceManager::new(PersistenceConfig {
                db_path: db_path.clone(),
                enable_recovery: true,
                auto_save_interval: 30,
            })
            .unwrap(),
        ));
        let events = EventService::with_capacity(16).with_ledger(persistence.clone());
        let mut internal_rx = events.subscribe_internal();

        let event = InternalEvent::with_timestamp(
            "shim.exit",
            "shim",
            "container-1",
            InternalEventSeverity::Warning,
            1234,
            serde_json::json!({
                "socket": "/run/crius/shims/container-1/task.sock",
                "reason": "socket missing",
            }),
        );
        events.publish_internal(event.clone()).await.unwrap();

        let broadcast = internal_rx.try_recv().unwrap();
        assert_eq!(broadcast, event);

        let recent = events
            .recent_internal_events("shim", "container-1", 10)
            .await
            .unwrap();
        assert_eq!(recent, vec![event.clone()]);

        drop(events);
        drop(persistence);
        let reloaded = Arc::new(tokio::sync::Mutex::new(
            PersistenceManager::new(PersistenceConfig {
                db_path,
                enable_recovery: true,
                auto_save_interval: 30,
            })
            .unwrap(),
        ));
        let events = EventService::with_capacity(16).with_ledger(reloaded);
        let recent = events
            .recent_internal_events("shim", "container-1", 10)
            .await
            .unwrap();
        assert_eq!(recent, vec![event]);
    }

    #[tokio::test]
    async fn ledger_sink_persists_events_for_service_queries() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("events.db");
        let sink = LedgerInternalEventSink::new(db_path.clone());
        let event = InternalEvent::with_timestamp(
            "task.state",
            "task",
            "container-1",
            InternalEventSeverity::Info,
            1234,
            serde_json::json!({
                "previousState": "created",
                "state": "running",
            }),
        );

        sink.publish(&event).unwrap();

        let persistence = Arc::new(tokio::sync::Mutex::new(
            PersistenceManager::new(PersistenceConfig {
                db_path,
                enable_recovery: true,
                auto_save_interval: 30,
            })
            .unwrap(),
        ));
        let events = EventService::with_capacity(16).with_ledger(persistence);
        let recent = events
            .recent_internal_events("task", "container-1", 10)
            .await
            .unwrap();
        assert_eq!(recent, vec![event]);
    }

    #[tokio::test]
    async fn internal_events_do_not_require_ledger_or_cri_subscribers() {
        let events = EventService::with_capacity(16);

        events
            .publish_internal(InternalEvent::with_timestamp(
                "reconcile.complete",
                "reconcile",
                "startup",
                InternalEventSeverity::Info,
                42,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();

        assert_eq!(
            events
                .recent_internal_events("reconcile", "startup", 10)
                .await
                .unwrap(),
            Vec::new()
        );
        assert_eq!(events.subscriber_count(), 0);
        assert_eq!(events.internal_subscriber_count(), 0);
    }
}
