use kafka_api_gateway::kafka::inbound::{Inbound, event_frame, is_uuid_v7, parse_inbound};
use serde_json::{Map, Value, json};

const CORRELATION_ID: &str = "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f13";
const UUID_V4: &str = "550e8400-e29b-41d4-a716-446655440000";

// Parses a JSON text the way the bridge does
fn parse(text: &str) -> Inbound {
    parse_inbound(Some(text.as_bytes()))
}

// Only a UUID of version 7 is accepted
#[test]
fn uuid_v7_check() {
    assert!(is_uuid_v7(CORRELATION_ID));
    assert!(!is_uuid_v7(UUID_V4));
    assert!(!is_uuid_v7("not-a-uuid"));
    assert!(!is_uuid_v7(""));
}

// A missing or empty value is Empty
#[test]
fn missing_or_empty_value_is_empty() {
    assert_eq!(parse_inbound(None), Inbound::Empty);
    assert_eq!(parse_inbound(Some(b"")), Inbound::Empty);
}

// Text that is not JSON is Malformed, whitespace included
#[test]
fn non_json_is_malformed() {
    assert_eq!(parse("not json"), Inbound::Malformed);
    assert_eq!(parse("   "), Inbound::Malformed);
    assert_eq!(parse("{\"a\":"), Inbound::Malformed);
}

// JSON that is not an object is Invalid
#[test]
fn json_that_is_not_an_object_is_invalid() {
    assert_eq!(parse("[1,2]"), Inbound::Invalid);
    assert_eq!(parse("42"), Inbound::Invalid);
    assert_eq!(parse("null"), Inbound::Invalid);
}

// An object without a usable correlation id is Invalid
#[test]
fn bad_correlation_id_is_invalid() {
    assert_eq!(parse("{}"), Inbound::Invalid);
    assert_eq!(parse(r#"{"correlationId": 5}"#), Inbound::Invalid);
    assert_eq!(
        parse(&json!({ "correlationId": UUID_V4 }).to_string()),
        Inbound::Invalid
    );
}

// A valid message keeps every field as payload
#[test]
fn valid_message_keeps_the_whole_payload() {
    let text = json!({ "correlationId": CORRELATION_ID, "jobName": "sync", "n": 1 }).to_string();

    let Inbound::Valid {
        correlation_id,
        payload,
    } = parse(&text)
    else {
        panic!("expected a valid message");
    };

    assert_eq!(correlation_id, CORRELATION_ID);
    assert_eq!(payload["jobName"], "sync");
    assert_eq!(payload["n"], 1);
    assert_eq!(payload["correlationId"], CORRELATION_ID);
}

// The frame has the fixed fields, the offset is a string and the payload is spread in
#[test]
fn frame_has_fixed_fields_and_payload() {
    let payload: Map<String, Value> =
        serde_json::from_value(json!({ "correlationId": CORRELATION_ID, "jobName": "sync" }))
            .unwrap();
    let headers = vec![("source".to_string(), "crm".to_string())];

    let frame: Value =
        serde_json::from_str(&event_frame("orders", 2, 15, &headers, payload)).unwrap();

    assert_eq!(frame["type"], "event");
    assert_eq!(frame["topic"], "orders");
    assert_eq!(frame["partition"], 2);
    assert_eq!(frame["offset"], "15");
    assert_eq!(frame["headers"], json!({ "source": "crm" }));
    assert_eq!(frame["correlationId"], CORRELATION_ID);
    assert_eq!(frame["jobName"], "sync");
}

// Payload fields come last, so a payload field with a fixed name wins like in the old gateway
#[test]
fn payload_fields_win_over_fixed_fields() {
    let payload: Map<String, Value> =
        serde_json::from_value(json!({ "correlationId": CORRELATION_ID, "type": "custom" }))
            .unwrap();

    let frame: Value = serde_json::from_str(&event_frame("orders", 0, 1, &[], payload)).unwrap();

    assert_eq!(frame["type"], "custom");
    assert_eq!(frame["headers"], json!({}));
}
