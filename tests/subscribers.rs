use kafka_api_gateway::kafka::subscribers::{
    AckRejection, AckedMessage, QUEUE_CAPACITY, SubscriberRegistry,
};

// A message the tests wait on
fn message(topic: &str, offset: i64) -> AckedMessage {
    AckedMessage {
        topic: topic.to_string(),
        partition: 0,
        offset,
    }
}

// D7: ids only increase, even after every connection is gone
#[tokio::test]
async fn ids_only_increase() {
    let registry = SubscriberRegistry::new();

    let (first, _rx1) = registry.register("key-1");
    let (second, _rx2) = registry.register("key-2");
    registry.unregister(first);
    registry.unregister(second);
    let (third, _rx3) = registry.register("key-3");

    assert!(first < second);
    assert!(second < third);
    assert_eq!(registry.session_count(), 1);
}

// The API key id of a connection can be read back, an unknown id has none
#[tokio::test]
async fn api_key_id_is_stored() {
    let registry = SubscriberRegistry::new();
    let (id, _rx) = registry.register("key-1");

    assert_eq!(registry.api_key_id(id).as_deref(), Some("key-1"));
    assert_eq!(registry.api_key_id(id + 100), None);
}

// Subscribing or unsubscribing an unknown connection reports false
#[tokio::test]
async fn unknown_connection_cannot_subscribe() {
    let registry = SubscriberRegistry::new();

    assert!(!registry.subscribe(99, "orders"));
    assert!(!registry.unsubscribe(99, "orders"));
}

// The owner of a topic is the lowest id among the connections subscribed to it
#[tokio::test]
async fn owner_is_the_lowest_subscribed_id() {
    let registry = SubscriberRegistry::new();
    let (first, _rx1) = registry.register("key-1");
    let (second, _rx2) = registry.register("key-2");
    let (third, _rx3) = registry.register("key-3");

    registry.subscribe(second, "orders");
    registry.subscribe(third, "orders");

    // The first connection is not subscribed to orders, so it cannot be the owner
    assert_eq!(registry.pick_owner("orders"), Some(second));

    registry.subscribe(first, "orders");
    assert_eq!(registry.pick_owner("orders"), Some(first));

    registry.unregister(first);
    assert_eq!(registry.pick_owner("orders"), Some(second));
}

// A topic nobody subscribed to has no owner
#[tokio::test]
async fn topic_without_subscribers_has_no_owner() {
    let registry = SubscriberRegistry::new();
    let (_id, _rx) = registry.register("key-1");

    assert_eq!(registry.pick_owner("orders"), None);
}

// A frame reaches every subscriber of the topic and nobody else
#[tokio::test]
async fn deliver_reaches_only_subscribers_of_the_topic() {
    let registry = SubscriberRegistry::new();
    let (a, mut rx_a) = registry.register("key-a");
    let (b, mut rx_b) = registry.register("key-b");
    let (_c, mut rx_c) = registry.register("key-c");
    registry.subscribe(a, "orders");
    registry.subscribe(b, "orders");

    let report = registry.deliver("orders", "frame-1");

    assert_eq!(report.delivered, 2);
    assert!(report.removed.is_empty());
    assert_eq!(rx_a.recv().await.as_deref(), Some("frame-1"));
    assert_eq!(rx_b.recv().await.as_deref(), Some("frame-1"));
    assert!(rx_c.try_recv().is_err());
}

// A topic with no subscribers delivers to nobody
#[tokio::test]
async fn deliver_without_subscribers_delivers_zero() {
    let registry = SubscriberRegistry::new();
    let (_id, _rx) = registry.register("key-1");

    assert_eq!(registry.deliver("orders", "frame").delivered, 0);
}

// After unsubscribing the connection no longer receives the topic
#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let registry = SubscriberRegistry::new();
    let (id, mut rx) = registry.register("key-1");
    registry.subscribe(id, "orders");
    registry.unsubscribe(id, "orders");

    assert_eq!(registry.deliver("orders", "frame").delivered, 0);
    assert!(rx.try_recv().is_err());
}

// A subscriber whose queue is full is removed, the frames already queued can still be read
#[tokio::test]
async fn full_queue_removes_the_slow_subscriber() {
    let registry = SubscriberRegistry::new();
    let (id, mut rx) = registry.register("key-1");
    registry.subscribe(id, "orders");

    for _ in 0..QUEUE_CAPACITY {
        assert_eq!(registry.deliver("orders", "frame").delivered, 1);
    }

    let report = registry.deliver("orders", "overflow");

    assert_eq!(report.delivered, 0);
    assert_eq!(report.removed, vec![id]);
    assert_eq!(registry.session_count(), 0);

    let mut read = 0;
    while rx.recv().await.is_some() {
        read += 1;
    }
    assert_eq!(read, QUEUE_CAPACITY);
}

// One slow subscriber does not stop the others from receiving the frame
#[tokio::test]
async fn slow_subscriber_does_not_block_the_others() {
    let registry = SubscriberRegistry::new();
    let (slow, _slow_rx) = registry.register("slow");
    let (fast, mut fast_rx) = registry.register("fast");
    registry.subscribe(slow, "orders");
    registry.subscribe(fast, "orders");

    for _ in 0..QUEUE_CAPACITY {
        registry.deliver("orders", "frame");
        fast_rx.recv().await;
    }

    let report = registry.deliver("orders", "overflow");

    assert_eq!(report.delivered, 1);
    assert_eq!(report.removed, vec![slow]);
    assert_eq!(fast_rx.recv().await.as_deref(), Some("overflow"));
}

// A valid ACK from the owner completes the wait and returns the message
#[tokio::test]
async fn owner_ack_completes_the_wait() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx) = registry.register("key-1");
    let waiter = registry.register_pending("corr-1", owner, message("orders", 7));

    let acked = registry.acknowledge("corr-1", owner).unwrap();

    assert_eq!(acked, message("orders", 7));
    assert!(waiter.await.is_ok());
}

// An ACK from another connection is rejected and the owner can still acknowledge
#[tokio::test]
async fn ack_from_non_owner_is_rejected() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx1) = registry.register("key-1");
    let (other, _rx2) = registry.register("key-2");
    let waiter = registry.register_pending("corr-1", owner, message("orders", 7));

    assert_eq!(
        registry.acknowledge("corr-1", other),
        Err(AckRejection::WrongOwner)
    );

    registry.acknowledge("corr-1", owner).unwrap();
    assert!(waiter.await.is_ok());
}

// An unknown correlation id and a second ACK of the same message are both rejected
#[tokio::test]
async fn unknown_or_repeated_ack_is_not_found() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx) = registry.register("key-1");
    let _waiter = registry.register_pending("corr-1", owner, message("orders", 7));

    assert_eq!(
        registry.acknowledge("missing", owner),
        Err(AckRejection::NotFound)
    );

    registry.acknowledge("corr-1", owner).unwrap();
    assert_eq!(
        registry.acknowledge("corr-1", owner),
        Err(AckRejection::NotFound)
    );
}

// When the owner disconnects the wait fails so nothing is committed
#[tokio::test]
async fn owner_disconnect_fails_the_wait() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx) = registry.register("key-1");
    let waiter = registry.register_pending("corr-1", owner, message("orders", 7));

    assert_eq!(registry.unregister(owner), 1);

    assert!(waiter.await.is_err());
    assert_eq!(
        registry.acknowledge("corr-1", owner),
        Err(AckRejection::NotFound)
    );
}

// Disconnecting a connection that owns nothing fails nothing
#[tokio::test]
async fn unregister_without_pending_fails_nothing() {
    let registry = SubscriberRegistry::new();
    let (id, _rx) = registry.register("key-1");

    assert_eq!(registry.unregister(id), 0);
}

// An owner removed for being too slow also fails its wait
#[tokio::test]
async fn slow_owner_fails_the_wait() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx) = registry.register("key-1");
    registry.subscribe(owner, "orders");
    let waiter = registry.register_pending("corr-1", owner, message("orders", 7));

    for _ in 0..QUEUE_CAPACITY {
        registry.deliver("orders", "frame");
    }
    registry.deliver("orders", "overflow");

    assert!(waiter.await.is_err());
}

// Registering the same correlation id again supersedes the first wait
#[tokio::test]
async fn second_registration_supersedes_the_first() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx) = registry.register("key-1");
    let first = registry.register_pending("corr-1", owner, message("orders", 7));
    let second = registry.register_pending("corr-1", owner, message("orders", 8));

    assert!(first.await.is_err());

    registry.acknowledge("corr-1", owner).unwrap();
    assert!(second.await.is_ok());
}

// A cancelled pending ACK can no longer be acknowledged and its wait fails
#[tokio::test]
async fn cancelled_pending_cannot_be_acknowledged() {
    let registry = SubscriberRegistry::new();
    let (owner, _rx) = registry.register("key-1");
    let waiter = registry.register_pending("corr-1", owner, message("orders", 7));

    registry.cancel_pending("corr-1");

    assert!(waiter.await.is_err());
    assert_eq!(
        registry.acknowledge("corr-1", owner),
        Err(AckRejection::NotFound)
    );
}

// The reason texts match the old gateway
#[test]
fn rejection_messages_match_the_old_gateway() {
    assert_eq!(
        AckRejection::NotFound.message(),
        "offset not found or already committed"
    );
    assert_eq!(
        AckRejection::WrongOwner.message(),
        "ACK from non-owning connection"
    );
}
