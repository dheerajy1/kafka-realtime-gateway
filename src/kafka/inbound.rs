use serde_json::{Map, Value};
use uuid::Uuid;

// The UUID version a correlation id must have
const CORRELATION_ID_VERSION: usize = 7;

// True when the text is a UUID of version 7
pub fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.get_version_num() == CORRELATION_ID_VERSION)
}

// What one Kafka message value turned out to be
#[derive(Debug, Clone, PartialEq)]
pub enum Inbound {
    // A JSON object with a UUID v7 correlationId, the whole object is the payload
    Valid {
        correlation_id: String,
        payload: Map<String, Value>,
    },

    // No value at all, or an empty one
    Empty,

    // The value is not JSON
    Malformed,

    // JSON, but not an object with a UUID v7 correlationId
    Invalid,
}

// Parses the value of one Kafka message, only the correlationId is required, every other field passes through
pub fn parse_inbound(value: Option<&[u8]>) -> Inbound {
    let Some(bytes) = value.filter(|bytes| !bytes.is_empty()) else {
        return Inbound::Empty;
    };

    let Ok(json) = serde_json::from_slice::<Value>(bytes) else {
        return Inbound::Malformed;
    };

    let Value::Object(payload) = json else {
        return Inbound::Invalid;
    };

    match payload.get("correlationId") {
        Some(Value::String(correlation_id)) if is_uuid_v7(correlation_id) => Inbound::Valid {
            correlation_id: correlation_id.clone(),
            payload,
        },
        _ => Inbound::Invalid,
    }
}

// Builds the event frame sent to subscribers, the payload fields come last and win over the fixed ones like the old gateway
pub fn event_frame(
    topic: &str,
    partition: i32,
    offset: i64,
    headers: &[(String, String)],
    payload: Map<String, Value>,
) -> String {
    let headers: Map<String, Value> = headers
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect();

    let mut frame = Map::new();
    frame.insert("type".to_string(), Value::from("event"));
    frame.insert("topic".to_string(), Value::from(topic));
    frame.insert("partition".to_string(), Value::from(partition));
    frame.insert("offset".to_string(), Value::from(offset.to_string()));
    frame.insert("headers".to_string(), Value::Object(headers));
    frame.extend(payload);

    Value::Object(frame).to_string()
}
