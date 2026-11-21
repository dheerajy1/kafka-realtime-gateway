use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::ws::frames::AsText;

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

// The event frame sent to subscribers
// The fixed fields and the headers are borrowed and the payload was moved in, so serde writes the text once and nothing is copied into a map first
struct EventFrame<'a> {
    topic: &'a str,
    partition: i32,
    offset: i64,
    headers: &'a [(String, String)],
    payload: Map<String, Value>,
}

impl Serialize for EventFrame<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        // The payload fields come last and win, so a fixed field the payload also has is left out
        let fixed = |name: &str| !self.payload.contains_key(name);

        if fixed("type") {
            map.serialize_entry("type", "event")?;
        }

        if fixed("topic") {
            map.serialize_entry("topic", self.topic)?;
        }

        if fixed("partition") {
            map.serialize_entry("partition", &self.partition)?;
        }

        // The offset is text because a 64 bit number can lose digits in JavaScript
        if fixed("offset") {
            map.serialize_entry("offset", &AsText(self.offset))?;
        }

        if fixed("headers") {
            map.serialize_entry("headers", &Headers(self.headers))?;
        }

        for (name, value) in &self.payload {
            map.serialize_entry(name, value)?;
        }

        map.end()
    }
}

// The Kafka headers as one JSON object, the last value of a repeated name wins
struct Headers<'a>(&'a [(String, String)]);

impl Serialize for Headers<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(
            self.0
                .iter()
                .enumerate()
                .filter(|(index, (name, _))| {
                    !self.0[index + 1..].iter().any(|(later, _)| later == name)
                })
                .map(|(_, (name, value))| (name.as_str(), value.as_str())),
        )
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
    let frame = EventFrame {
        topic,
        partition,
        offset,
        headers,
        payload,
    };

    // The fields are plain text and numbers, so serializing cannot fail
    serde_json::to_string(&frame).expect("an event frame always serializes")
}
