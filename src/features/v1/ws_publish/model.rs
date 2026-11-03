use serde::Deserialize;
use serde_json::Value;

// The message of a ws-publish client, a job to publish
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    // Kafka topic to publish to
    pub topic: String,

    // Name of the job, it goes into the published message
    pub job_name: String,
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
