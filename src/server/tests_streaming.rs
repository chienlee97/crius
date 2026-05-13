use super::*;

#[tokio::test]
async fn event_service_sender_is_shared_with_server_events() {
    let service = lifecycle::test_service();
    let mut receiver = service.internal_services.events.subscribe();

    service.publish_event(ContainerEventResponse {
        container_id: "streaming-test".to_string(),
        container_event_type: ContainerEventType::ContainerStartedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event = receiver.recv().await.unwrap();
    assert_eq!(event.container_id, "streaming-test");
}
