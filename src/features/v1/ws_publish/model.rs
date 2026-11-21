use serde::{Deserialize, Serialize};
use serde_json::Value;

// The message of a ws-publish client, a job to publish
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    // Kafka topic to publish to
    pub topic: String,

    // Name of the job, it goes into the published message
    pub job_name: String,
}

// The event the gateway publishes for a ws-publish message, it borrows its text from the request and the identity
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedEvent<'a> {
    // UUID v7 that follows the event through Kafka and back
    pub correlation_id: &'a str,

    // Id of the API user that owns the key
    pub api_user_id: i32,

    // Name of the job from the client message
    pub job_name: &'a str,

    // When the gateway accepted the message, ISO 8601 UTC
    pub timestamp: &'a str,
}

impl PublishedEvent<'_> {
    // The JSON text of the event, this is the value of the Kafka message
    pub fn to_text(&self) -> String {
        // The fields are plain text and a number, so serializing cannot fail
        serde_json::to_string(self).expect("the event always serializes")
    }
}

// Why a text could not be turned into a publish request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    // The text is not JSON
    Malformed,

    // JSON of the wrong shape, a missing or empty field
    Invalid,
}

// The shape as it comes over the wire, other fields are ignored like the old zod object
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Wire {
    topic: String,
    job_name: String,
}

// Parses and validates one text message of a ws-publish client
pub fn parse_publish_request(text: &str) -> Result<PublishRequest, ParseError> {
    let json: Value = serde_json::from_str(text).map_err(|_| ParseError::Malformed)?;
    let wire: Wire = serde_json::from_value(json).map_err(|_| ParseError::Invalid)?;

    if wire.topic.is_empty() || wire.job_name.is_empty() {
        return Err(ParseError::Invalid);
    }

    Ok(PublishRequest {
        topic: wire.topic,
        job_name: wire.job_name,
    })
}
