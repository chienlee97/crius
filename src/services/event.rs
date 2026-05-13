use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::proto::runtime::v1::ContainerEventResponse;

#[derive(Debug, Clone)]
pub struct EventService {
    sender: tokio::sync::broadcast::Sender<ContainerEventResponse>,
}

impl EventService {
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self { sender }
    }

    pub fn from_sender(sender: tokio::sync::broadcast::Sender<ContainerEventResponse>) -> Self {
        Self { sender }
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

    pub fn publish(&self, event: ContainerEventResponse) {
        if let Err(err) = self.sender.send(event) {
            log::debug!("Dropping CRI event without subscribers: {}", err);
        }
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
    use tokio_stream::StreamExt;

    use super::*;
    use crate::proto::runtime::v1::ContainerEventType;

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
}
