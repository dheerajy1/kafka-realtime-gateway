use std::{sync::Arc, time::Duration};

use kafka_api_gateway::{
    db::store::TopicRow,
    kafka::{
        bridge::{Bridge, InboundRecord, Outcome},
        registry::{TopicPolicy, TopicRegistry},
        subscribers::{AckRejection, SubscriberRegistry},
    },
};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};

const CORRELATION_ID: &str = "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f13";

// Everything one test needs, the stop sender must stay alive for the whole test
struct Setup {
    bridge: Bridge,
    topics: Arc<TopicRegistry>,
    subscribers: Arc<SubscriberRegistry>,
    stop: watch::Sender<bool>,
}

// A topic policy row, publishing by subscribers is not used by the bridge
fn row(name: &str, gateway_consume: bool, ack_required: bool, enabled: bool) -> TopicRow {
    TopicRow {
        topic_name: name.to_string(),
        gateway_consume,
        ack_required,
        subscriber_publish_allowed: false,
        enabled,
    }
}

// A policy built from rows
fn policy(rows: Vec<TopicRow>) -> Arc<TopicPolicy> {
    Arc::new(TopicPolicy::from_rows(rows))
}

// A bridge with short timeouts over a registry loaded with the given rows
fn setup(rows: Vec<TopicRow>) -> Setup {
    let topics = Arc::new(TopicRegistry::default());
    topics.replace(policy(rows));
    let subscribers = Arc::new(SubscriberRegistry::new());
    let (stop, shutdown) = watch::channel(false);

    let bridge = Bridge::new(topics.clone(), subscribers.clone(), shutdown)
        .with_timeouts(Duration::from_millis(300), Duration::from_millis(20));

    Setup {
        bridge,
        topics,
        subscribers,
        stop,
    }
}

// Connects a subscriber and subscribes it to a topic
fn join_topic(setup: &Setup, key: &str, topic: &str) -> (u64, mpsc::Receiver<String>) {
    let (id, rx) = setup.subscribers.register(key);
    setup.subscribers.subscribe(id, topic);

    (id, rx)
}

// A Kafka record with the given raw value
fn record(topic: &str, value: Option<&str>) -> InboundRecord {
    InboundRecord {
        topic: topic.to_string(),
        partition: 0,
        offset: 7,
        headers: vec![("source".to_string(), "crm".to_string())],
        value: value.map(|text| text.as_bytes().to_vec()),
    }
}

// A valid message value
fn valid_value() -> String {
    json!({ "correlationId": CORRELATION_ID, "jobName": "sync" }).to_string()
}

// Parses an event frame
fn parse(frame: &str) -> Value {
    serde_json::from_str(frame).unwrap()
}

// Messages that cannot be used are committed so they do not block the partition
#[tokio::test]
async fn empty_malformed_and_invalid_messages_are_committed() {
    let s = setup(vec![row("events", true, false, true)]);

    assert_eq!(
        s.bridge.process(&record("events", None)).await,
        Outcome::Commit
    );
    assert_eq!(
        s.bridge.process(&record("events", Some("not json"))).await,
        Outcome::Commit
    );
    assert_eq!(
        s.bridge
            .process(&record("events", Some(r#"{"a":1}"#)))
            .await,
        Outcome::Commit
    );
}

// A topic that is not in the policy waits in Kafka
#[tokio::test]
async fn unknown_topic_is_paused() {
    let s = setup(vec![row("events", true, false, true)]);
    let rec = record("other", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Pause);
}

// A disabled topic waits in Kafka
#[tokio::test]
async fn disabled_topic_is_paused() {
    let s = setup(vec![row("events", true, false, false)]);
    let rec = record("events", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Pause);
}

// A topic the gateway does not consume waits in Kafka too
#[tokio::test]
async fn topic_without_gateway_consume_is_paused() {
    let s = setup(vec![row("events", false, false, true)]);
    let rec = record("events", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Pause);
}

// A topic without ACK and without subscribers is dropped and committed
#[tokio::test]
async fn no_ack_topic_without_subscribers_is_committed() {
    let s = setup(vec![row("events", true, false, true)]);
    let rec = record("events", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Commit);
}

// A topic without ACK is sent to every subscriber and committed at once
#[tokio::test]
async fn no_ack_topic_is_delivered_to_all_subscribers() {
    let s = setup(vec![row("events", true, false, true)]);
    let (_a, mut rx_a) = join_topic(&s, "key-a", "events");
    let (_b, mut rx_b) = join_topic(&s, "key-b", "events");
    let (_c, mut rx_c) = s.subscribers.register("key-c");
    let rec = record("events", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Commit);

    let frame = parse(&rx_a.try_recv().unwrap());
    assert_eq!(frame["type"], "event");
    assert_eq!(frame["topic"], "events");
    assert_eq!(frame["partition"], 0);
    assert_eq!(frame["offset"], "7");
    assert_eq!(frame["headers"], json!({ "source": "crm" }));
    assert_eq!(frame["correlationId"], CORRELATION_ID);
    assert_eq!(frame["jobName"], "sync");
    assert!(rx_b.try_recv().is_ok());
    assert!(rx_c.try_recv().is_err());
}

// An ACK topic is committed after the owner acknowledges
#[tokio::test]
async fn ack_topic_is_committed_after_the_owner_acks() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let (owner, mut rx) = join_topic(&s, "key-1", "jobs");
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, frame) = tokio::join!(s.bridge.process(&rec), async {
        let frame = rx.recv().await.unwrap();
        s.subscribers.acknowledge(CORRELATION_ID, owner).unwrap();
        frame
    });

    assert_eq!(outcome, Outcome::Commit);
    assert_eq!(parse(&frame)["correlationId"], CORRELATION_ID);
}

// Only the owner can acknowledge, an ACK from another subscriber is rejected and the wait goes on
#[tokio::test]
async fn ack_from_a_non_owner_does_not_commit() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let (owner, mut owner_rx) = join_topic(&s, "key-1", "jobs");
    let (other, mut other_rx) = join_topic(&s, "key-2", "jobs");
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, rejection) = tokio::join!(s.bridge.process(&rec), async {
        owner_rx.recv().await.unwrap();
        other_rx.recv().await.unwrap();
        let rejection = s
            .subscribers
            .acknowledge(CORRELATION_ID, other)
            .unwrap_err();
        s.subscribers.acknowledge(CORRELATION_ID, owner).unwrap();
        rejection
    });

    assert_eq!(rejection, AckRejection::WrongOwner);
    assert_eq!(outcome, Outcome::Commit);
}

// No ACK within the timeout means the message is read again and the pending ACK is forgotten
#[tokio::test]
async fn missing_ack_times_out_and_retries() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let (owner, _rx) = join_topic(&s, "key-1", "jobs");
    let rec = record("jobs", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Retry);
    assert_eq!(
        s.subscribers.acknowledge(CORRELATION_ID, owner),
        Err(AckRejection::NotFound)
    );
}

// An owner that disconnects before acknowledging means the message is read again
#[tokio::test]
async fn owner_disconnect_retries() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let (owner, mut rx) = join_topic(&s, "key-1", "jobs");
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, _) = tokio::join!(s.bridge.process(&rec), async {
        rx.recv().await.unwrap();
        s.subscribers.unregister(owner);
    });

    assert_eq!(outcome, Outcome::Retry);
}

// An ACK topic waits for a subscriber instead of dropping the message
#[tokio::test]
async fn ack_topic_waits_for_a_subscriber() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, frame) = tokio::join!(s.bridge.process(&rec), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (owner, mut rx) = join_topic(&s, "key-1", "jobs");
        let frame = rx.recv().await.unwrap();
        s.subscribers.acknowledge(CORRELATION_ID, owner).unwrap();
        frame
    });

    assert_eq!(outcome, Outcome::Commit);
    assert_eq!(parse(&frame)["jobName"], "sync");
}

// Shutdown while waiting for a subscriber stops the loop without a commit
#[tokio::test]
async fn shutdown_while_waiting_for_a_subscriber_stops() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, _) = tokio::join!(s.bridge.process(&rec), async {
        tokio::time::sleep(Duration::from_millis(60)).await;
        s.stop.send(true).unwrap();
    });

    assert_eq!(outcome, Outcome::Stop);
}

// Shutdown while waiting for an ACK stops the loop and forgets the pending ACK
#[tokio::test]
async fn shutdown_while_waiting_for_an_ack_stops() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let (owner, mut rx) = join_topic(&s, "key-1", "jobs");
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, _) = tokio::join!(s.bridge.process(&rec), async {
        rx.recv().await.unwrap();
        s.stop.send(true).unwrap();
    });

    assert_eq!(outcome, Outcome::Stop);
    assert_eq!(
        s.subscribers.acknowledge(CORRELATION_ID, owner),
        Err(AckRejection::NotFound)
    );
}

// A topic that is disabled while the bridge waits for a subscriber is paused
#[tokio::test]
async fn topic_disabled_while_waiting_is_paused() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let rec = record("jobs", Some(&valid_value()));

    let (outcome, _) = tokio::join!(s.bridge.process(&rec), async {
        tokio::time::sleep(Duration::from_millis(60)).await;
        s.topics
            .replace(policy(vec![row("jobs", true, true, false)]));
    });

    assert_eq!(outcome, Outcome::Pause);
}

// A subscriber too slow to keep up is dropped, so the ACK wait fails and the message is read again
#[tokio::test]
async fn slow_owner_is_dropped_and_the_message_retried() {
    let s = setup(vec![row("jobs", true, true, true)]);
    let (_owner, _rx) = join_topic(&s, "key-1", "jobs");

    // The owner queue is filled up to its limit so the frame for the record does not fit
    for _ in 0..kafka_api_gateway::kafka::subscribers::QUEUE_CAPACITY {
        s.subscribers.deliver("jobs", "filler");
    }

    let rec = record("jobs", Some(&valid_value()));

    assert_eq!(s.bridge.process(&rec).await, Outcome::Retry);
    assert_eq!(s.subscribers.session_count(), 0);
}
