mod common;

use std::sync::Arc;

use common::fakes::FakePublisher;
use kafka_api_gateway::{
    db::store::TopicRow,
    errors::catalog::bad_gateway,
    features::v1::ws_publish::{
        model::{ParseError, PublishRequest, parse_publish_request},
        session::PublishSession,
    },
    kafka::{
        inbound::is_uuid_v7,
        registry::{TopicPolicy, TopicRegistry},
    },
    middleware::api_key::ApiKeyIdentity,
};
use serde_json::{Value, json};

// A valid request is parsed and extra fields are ignored like the old zod object
#[test]
fn valid_request_is_parsed_and_extra_fields_are_ignored() {
    let text = json!({ "type": "publish", "topic": "jobs", "jobName": "sync", "x": 1 }).to_string();

    assert_eq!(
        parse_publish_request(&text),
        Ok(PublishRequest {
            topic: "jobs".to_string(),
            job_name: "sync".to_string()
        })
    );
}

// Empty or missing fields and wrong types are invalid
#[test]
fn bad_shapes_are_invalid() {
    for value in [
        json!({ "topic": "", "jobName": "sync" }),
        json!({ "topic": "jobs", "jobName": "" }),
        json!({ "topic": "jobs" }),
        json!({ "jobName": "sync" }),
        json!({ "topic": 1, "jobName": "sync" }),
        json!([1, 2]),
    ] {
        assert_eq!(
            parse_publish_request(&value.to_string()),
            Err(ParseError::Invalid),
            "{value}"
        );
    }
}

// Text that is not JSON is malformed
#[test]
fn non_json_is_malformed() {
    assert_eq!(
        parse_publish_request("not json"),
        Err(ParseError::Malformed)
    );
}

// Everything one session test needs
struct Setup {
    session: PublishSession,
    publisher: Arc<FakePublisher>,
}

// A policy row, the subscriber publish flag is not used by ws-publish
fn row(name: &str, subscriber_publish_allowed: bool, enabled: bool) -> TopicRow {
    TopicRow {
        topic_name: name.to_string(),
        gateway_consume: true,
        ack_required: false,
        subscriber_publish_allowed,
        enabled,
    }
}

// A session over a policy with the enabled topic jobs and the disabled topic retired
fn setup(publisher: FakePublisher) -> Setup {
    let topics = Arc::new(TopicRegistry::default());
    topics.replace(Arc::new(TopicPolicy::from_rows(vec![
        row("jobs", false, true),
        row("retired", true, false),
    ])));
    let publisher = Arc::new(publisher);

    let session = PublishSession::new(
        ApiKeyIdentity {
            api_key_id: "key-1".to_string(),
            api_user_id: 7,
        },
        topics,
        publisher.clone(),
    );

    Setup { session, publisher }
}

// Sends a text through the session and parses the answer frame
async fn send(setup: &Setup, text: &str) -> Value {
    serde_json::from_str(&setup.session.handle_text(text).await).unwrap()
}

// The text of a valid publish request
fn request(topic: &str) -> String {
    json!({ "topic": topic, "jobName": "sync" }).to_string()
}

// A valid request is published and answered with an ack that carries the correlation id
#[tokio::test]
async fn publish_is_acknowledged_with_a_uuid_v7() {
    let s = setup(FakePublisher::new());

    let reply = send(&s, &request("jobs")).await;

    assert_eq!(reply["type"], "ack");
    assert_eq!(reply["topic"], "jobs");

    let correlation_id = reply["correlationId"].as_str().unwrap();
    assert!(is_uuid_v7(correlation_id));
}

// The published message has the fields of the old gateway and the API key id as Kafka key
#[tokio::test]
async fn published_message_has_the_job_fields_and_the_api_key_as_key() {
    let s = setup(FakePublisher::new());

    let reply = send(&s, &request("jobs")).await;

    let sent = s.publisher.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].topic, "jobs");
    assert_eq!(sent[0].key.as_deref(), Some("key-1"));
    assert!(sent[0].headers.is_empty());

    let value: Value = serde_json::from_str(&sent[0].value).unwrap();
    assert_eq!(value["correlationId"], reply["correlationId"]);
    assert_eq!(value["apiUserId"], 7);
    assert_eq!(value["jobName"], "sync");

    // An ISO timestamp with milliseconds and a Z, like JavaScript's toISOString
    let timestamp = value["timestamp"].as_str().unwrap();
    assert_eq!(timestamp.len(), 24);
    assert!(timestamp.ends_with('Z'));
}

// Every publish gets its own correlation id
#[tokio::test]
async fn every_publish_gets_a_new_correlation_id() {
    let s = setup(FakePublisher::new());

    let first = send(&s, &request("jobs")).await;
    let second = send(&s, &request("jobs")).await;

    assert_ne!(first["correlationId"], second["correlationId"]);
}

// D5: a topic that is not enabled is refused and nothing is sent
#[tokio::test]
async fn unknown_and_disabled_topics_are_refused() {
    let s = setup(FakePublisher::new());

    for topic in ["missing", "retired"] {
        let reply = send(&s, &request(topic)).await;

        assert_eq!(reply["type"], "error", "{topic}");
        assert_eq!(reply["message"], "Unauthorized topic", "{topic}");
        assert_eq!(reply["reason"], "topic-not-allowed", "{topic}");
    }

    assert!(s.publisher.sent().is_empty());
}

// Bad input gets the error frames of the old gateway
#[tokio::test]
async fn bad_input_gets_error_frames() {
    let s = setup(FakePublisher::new());

    assert_eq!(
        send(&s, "not json").await,
        json!({ "type": "error", "message": "Malformed JSON" })
    );
    assert_eq!(
        send(&s, &json!({ "topic": "jobs" }).to_string()).await,
        json!({ "type": "error", "message": "Invalid publish message" })
    );
}

// D14: a Kafka failure is reported as such and never as Malformed JSON
#[tokio::test]
async fn kafka_failure_is_reported_accurately() {
    let s = setup(FakePublisher::failing(bad_gateway::UPSTREAM_UNAVAILABLE));

    let reply = send(&s, &request("jobs")).await;

    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "Kafka publish failed");
    assert_eq!(reply["reason"], bad_gateway::UPSTREAM_UNAVAILABLE.error);
}
