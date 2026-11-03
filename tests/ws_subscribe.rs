mod common;

use std::sync::Arc;

use common::fakes::FakePublisher;
use kafka_api_gateway::{
    db::store::TopicRow,
    errors::catalog::payload_too_large,
    features::v1::ws_subscribe::{
        model::{ClientMessage, ParseError, parse_client_message},
        session::SubscriberSession,
    },
    kafka::{
        registry::{TopicPolicy, TopicRegistry},
        subscribers::{AckedMessage, SubscriberRegistry},
    },
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

const CORRELATION_ID: &str = "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f13";
const UUID_V4: &str = "550e8400-e29b-41d4-a716-446655440000";

// Parses a JSON value the way a subscriber message arrives
fn parse(value: Value) -> Result<ClientMessage, ParseError> {
    parse_client_message(&value.to_string())
}

// True when the message is invalid and one of its issues contains the text
fn is_invalid_with(result: Result<ClientMessage, ParseError>, text: &str) -> bool {
    match result {
        Err(ParseError::Invalid(issues)) => issues.iter().any(|issue| issue.contains(text)),
        _ => false,
    }
}

// A valid publish message
fn publish_message() -> Value {
    json!({
        "type": "publish",
        "requestId": UUID_V4,
        "topic": "events",
        "key": "order-1",
        "value": { "orderId": 42 },
        "headers": { "retry": "1" }
    })
}

// Subscribe and unsubscribe need a topic
#[test]
fn subscribe_and_unsubscribe_are_parsed() {
    assert_eq!(
        parse(json!({ "type": "subscribe", "topic": "events" })),
        Ok(ClientMessage::Subscribe {
            topic: "events".to_string()
        })
    );
    assert_eq!(
        parse(json!({ "type": "unsubscribe", "topic": "events" })),
        Ok(ClientMessage::Unsubscribe {
            topic: "events".to_string()
        })
    );
}

// An empty topic is invalid
#[test]
fn empty_topic_is_invalid() {
    assert!(is_invalid_with(
        parse(json!({ "type": "subscribe", "topic": "" })),
        "\"topic\" must not be empty"
    ));
}

// processed needs a UUID v7 correlation id
#[test]
fn processed_needs_a_uuid_v7() {
    assert_eq!(
        parse(json!({ "type": "processed", "correlationId": CORRELATION_ID })),
        Ok(ClientMessage::Processed {
            correlation_id: CORRELATION_ID.to_string()
        })
    );
    assert!(is_invalid_with(
        parse(json!({ "type": "processed", "correlationId": UUID_V4 })),
        "UUID v7"
    ));
}

// A full publish message keeps key, value and headers
#[test]
fn publish_is_parsed_with_key_value_and_headers() {
    let Ok(ClientMessage::Publish(command)) = parse(publish_message()) else {
        panic!("expected a publish message");
    };

    assert_eq!(command.request_id, UUID_V4);
    assert_eq!(command.topic, "events");
    assert_eq!(command.key.as_deref(), Some("order-1"));
    assert_eq!(command.value["orderId"], 42);
    assert_eq!(
        command.headers,
        vec![("retry".to_string(), "1".to_string())]
    );
}

// Key and headers are optional
#[test]
fn publish_without_key_and_headers_is_valid() {
    let message = json!({
        "type": "publish",
        "requestId": UUID_V4,
        "topic": "events",
        "value": {}
    });

    let Ok(ClientMessage::Publish(command)) = parse(message) else {
        panic!("expected a publish message");
    };

    assert_eq!(command.key, None);
    assert!(command.headers.is_empty());
}

// A request id that is not a UUID and an empty key are invalid
#[test]
fn publish_with_bad_request_id_or_empty_key_is_invalid() {
    let mut message = publish_message();
    message["requestId"] = json!("not-a-uuid");
    assert!(is_invalid_with(
        parse(message),
        "\"requestId\" must be a UUID"
    ));

    let mut message = publish_message();
    message["key"] = json!("");
    assert!(is_invalid_with(parse(message), "\"key\" must not be empty"));
}

// The publish value has to be an object and the headers have to be strings
#[test]
fn publish_value_must_be_an_object() {
    let mut message = publish_message();
    message["value"] = json!("text");
    assert!(matches!(parse(message), Err(ParseError::Invalid(_))));

    let mut message = publish_message();
    message["headers"] = json!({ "retry": 1 });
    assert!(matches!(parse(message), Err(ParseError::Invalid(_))));
}

// Text that is not JSON is malformed, JSON of the wrong shape is invalid
#[test]
fn malformed_and_invalid_are_told_apart() {
    assert_eq!(parse_client_message("not json"), Err(ParseError::Malformed));
    assert!(matches!(
        parse(json!({ "type": "nope" })),
        Err(ParseError::Invalid(_))
    ));
    assert!(matches!(
        parse(json!({ "topic": "events" })),
        Err(ParseError::Invalid(_))
    ));
    assert!(matches!(parse(json!([1, 2])), Err(ParseError::Invalid(_))));
}

// Everything one session test needs
struct Setup {
    session: SubscriberSession,
    subscribers: Arc<SubscriberRegistry>,
    publisher: Arc<FakePublisher>,
    id: u64,
    // Kept so the subscriber queue stays open
    _rx: mpsc::Receiver<String>,
}

// A connected subscriber over a policy with events (publish allowed) and jobs (publish not allowed)
fn setup(publisher: FakePublisher) -> Setup {
    let rows = vec![
        TopicRow {
            topic_name: "events".to_string(),
            gateway_consume: true,
            ack_required: false,
            subscriber_publish_allowed: true,
            enabled: true,
        },
        TopicRow {
            topic_name: "jobs".to_string(),
            gateway_consume: true,
            ack_required: true,
            subscriber_publish_allowed: false,
            enabled: true,
        },
    ];

    let topics = Arc::new(TopicRegistry::default());
    topics.replace(Arc::new(TopicPolicy::from_rows(rows)));
    let subscribers = Arc::new(SubscriberRegistry::new());
    let publisher = Arc::new(publisher);
    let (id, rx) = subscribers.register("key-1");

    let session = SubscriberSession::new(
        id,
        "billing-service".to_string(),
        topics,
        subscribers.clone(),
        publisher.clone(),
    );

    Setup {
        session,
        subscribers,
        publisher,
        id,
        _rx: rx,
    }
}

// Sends a message through the session and parses the answer frame
async fn send(setup: &Setup, message: Value) -> Value {
    let reply = setup.session.handle_text(&message.to_string()).await;

    serde_json::from_str(&reply).unwrap()
}

// Subscribing answers subscribed and the connection becomes an owner candidate
#[tokio::test]
async fn subscribe_answers_and_registers() {
    let s = setup(FakePublisher::new());

    let reply = send(&s, json!({ "type": "subscribe", "topic": "jobs" })).await;

    assert_eq!(reply, json!({ "type": "subscribed", "topic": "jobs" }));
    assert_eq!(s.subscribers.pick_owner("jobs"), Some(s.id));
}

// A topic the registry does not know can still be subscribed, it just never delivers
#[tokio::test]
async fn subscribing_an_unknown_topic_is_allowed() {
    let s = setup(FakePublisher::new());

    let reply = send(
        &s,
        json!({ "type": "subscribe", "topic": "nobody-consumes" }),
    )
    .await;

    assert_eq!(reply["type"], "subscribed");
}

// Unsubscribing answers unsubscribed and removes the connection from the topic
#[tokio::test]
async fn unsubscribe_answers_and_removes() {
    let s = setup(FakePublisher::new());
    send(&s, json!({ "type": "subscribe", "topic": "jobs" })).await;

    let reply = send(&s, json!({ "type": "unsubscribe", "topic": "jobs" })).await;

    assert_eq!(reply, json!({ "type": "unsubscribed", "topic": "jobs" }));
    assert_eq!(s.subscribers.pick_owner("jobs"), None);
}

// Bad messages get the error frames of the old gateway
#[tokio::test]
async fn bad_messages_get_error_frames() {
    let s = setup(FakePublisher::new());

    let malformed = s.session.handle_text("not json").await;
    let malformed: Value = serde_json::from_str(&malformed).unwrap();
    assert_eq!(
        malformed,
        json!({ "type": "error", "message": "Malformed JSON" })
    );

    let invalid = send(&s, json!({ "type": "subscribe", "topic": "" })).await;
    assert_eq!(invalid["type"], "error");
    assert_eq!(invalid["message"], "Invalid message format");
    assert_eq!(
        invalid["issues"][0]["message"],
        "\"topic\" must not be empty"
    );
}

// The owner's processed message is accepted
#[tokio::test]
async fn processed_from_the_owner_is_accepted() {
    let s = setup(FakePublisher::new());
    let _waiter = s.subscribers.register_pending(
        CORRELATION_ID,
        s.id,
        AckedMessage {
            topic: "jobs".to_string(),
            partition: 0,
            offset: 3,
        },
    );

    let reply = send(
        &s,
        json!({ "type": "processed", "correlationId": CORRELATION_ID }),
    )
    .await;

    assert_eq!(
        reply,
        json!({ "type": "ack-received", "correlationId": CORRELATION_ID })
    );
}

// A processed message for something that is not pending is rejected with the reason
#[tokio::test]
async fn processed_for_nothing_pending_is_rejected() {
    let s = setup(FakePublisher::new());

    let reply = send(
        &s,
        json!({ "type": "processed", "correlationId": CORRELATION_ID }),
    )
    .await;

    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "ACK rejected");
    assert_eq!(reply["reason"], "offset not found or already committed");
    assert_eq!(reply["correlationId"], CORRELATION_ID);
}

// A processed message from a connection that does not own the message is rejected
#[tokio::test]
async fn processed_from_a_non_owner_is_rejected() {
    let s = setup(FakePublisher::new());
    let (other, _other_rx) = s.subscribers.register("key-2");
    let _waiter = s.subscribers.register_pending(
        CORRELATION_ID,
        other,
        AckedMessage {
            topic: "jobs".to_string(),
            partition: 0,
            offset: 3,
        },
    );

    let reply = send(
        &s,
        json!({ "type": "processed", "correlationId": CORRELATION_ID }),
    )
    .await;

    assert_eq!(reply["reason"], "ACK from non-owning connection");
}

// A publish to an allowed topic reaches Kafka and the answer carries partition and offset
#[tokio::test]
async fn publish_to_an_allowed_topic_is_sent_to_kafka() {
    let s = setup(FakePublisher::new());

    let reply = send(&s, publish_message()).await;

    assert_eq!(reply["type"], "published");
    assert_eq!(reply["requestId"], UUID_V4);
    assert_eq!(reply["topic"], "events");
    assert_eq!(reply["partition"], 0);
    assert_eq!(reply["offset"], "0");

    let sent = s.publisher.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].topic, "events");
    assert_eq!(sent[0].key.as_deref(), Some("order-1"));
    assert_eq!(
        sent[0].headers,
        vec![("retry".to_string(), "1".to_string())]
    );
    assert_eq!(sent[0].value, r#"{"orderId":42}"#);
}

// A topic not marked subscriber_publish_allowed is refused and nothing is sent
#[tokio::test]
async fn publish_to_a_topic_that_does_not_allow_it_is_refused() {
    let s = setup(FakePublisher::new());
    let mut message = publish_message();
    message["topic"] = json!("jobs");

    let reply = send(&s, message).await;

    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "Unauthorized topic");
    assert_eq!(reply["reason"], "topic-not-allowed");
    assert_eq!(reply["requestId"], UUID_V4);
    assert!(s.publisher.sent().is_empty());
}

// A topic that is not in the registry is refused the same way
#[tokio::test]
async fn publish_to_an_unknown_topic_is_refused() {
    let s = setup(FakePublisher::new());
    let mut message = publish_message();
    message["topic"] = json!("missing");

    let reply = send(&s, message).await;

    assert_eq!(reply["reason"], "topic-not-allowed");
    assert!(s.publisher.sent().is_empty());
}

// A Kafka failure gets its own error frame with the failure as reason
#[tokio::test]
async fn kafka_failure_gets_its_own_error_frame() {
    let s = setup(FakePublisher::failing(
        payload_too_large::KAFKA_MESSAGE_TOO_LARGE,
    ));

    let reply = send(&s, publish_message()).await;

    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "Kafka publish failed");
    assert_eq!(
        reply["reason"],
        payload_too_large::KAFKA_MESSAGE_TOO_LARGE.error
    );
    assert_eq!(reply["requestId"], UUID_V4);
}
