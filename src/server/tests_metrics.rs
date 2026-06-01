use super::*;

#[tokio::test]
async fn metrics_provider_snapshot_counts_event_subscribers() {
    let service = lifecycle::test_service();
    let _subscriber = service.internal_services.events.subscribe();

    let snapshot = service.metrics_provider().snapshot().await;

    assert_eq!(snapshot.event_subscriber_count, 1);
}
