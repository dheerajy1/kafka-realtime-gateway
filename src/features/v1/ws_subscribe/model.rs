use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::kafka::inbound::is_uuid_v7;

// A message from a subscriber, the shapes of the old gateway
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMessage {
    // Start receiving a topic
    Subscribe { topic: String },

    // Stop receiving a topic
    Unsubscribe { topic: String },

    // The subscriber finished processing the message with this correlation id
    Processed { correlation_id: String },

    // Publish a message to Kafka through the gateway
    Publish(PublishCommand),
}

// A publish request of a subscriber
#[derive(Debug, Clone, PartialEq)]
pub struct PublishCommand {
    // Id the subscriber chose to match the answer to this request
    pub request_id: String,

    // Topic to publish to
    pub topic: String,

    // Kafka message key
    pub key: Option<String>,

    // The event payload
    pub value: Map<String, Value>,

    // Kafka headers
    pub headers: Vec<(String, String)>,
}

// Why a text could not be turned into a message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    // The text is not JSON
    Malformed,

    // JSON of the wrong shape, with what is wrong
    Invalid(Vec<String>),
}

// The shapes as they come over the wire, checked further by parse_client_message
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Wire {
    Subscribe {
        topic: String,
    },
    Unsubscribe {
        topic: String,
    },
    Processed {
        #[serde(rename = "correlationId")]
        correlation_id: String,
    },
    Publish {
        #[serde(rename = "requestId")]
        request_id: String,
        topic: String,
        key: Option<String>,
        value: Map<String, Value>,
        headers: Option<BTreeMap<String, String>>,
    },
}

// Adds an issue when the topic is empty
fn check_topic(topic: &str, issues: &mut Vec<String>) {
    if topic.is_empty() {
        issues.push("\"topic\" must not be empty".to_string());
    }
}

// Parses and validates one text message of a subscriber
pub fn parse_client_message(text: &str) -> Result<ClientMessage, ParseError> {
    let json: Value = serde_json::from_str(text).map_err(|_| ParseError::Malformed)?;

    let wire: Wire = serde_json::from_value(json)
        .map_err(|error| ParseError::Invalid(vec![error.to_string()]))?;

    let mut issues = Vec::new();

    let message = match wire {
        Wire::Subscribe { topic } => {
            check_topic(&topic, &mut issues);
            ClientMessage::Subscribe { topic }
        }
        Wire::Unsubscribe { topic } => {
            check_topic(&topic, &mut issues);
            ClientMessage::Unsubscribe { topic }
        }
        Wire::Processed { correlation_id } => {
            if !is_uuid_v7(&correlation_id) {
                issues.push("\"correlationId\" must be a UUID v7".to_string());
            }
            ClientMessage::Processed { correlation_id }
        }
        Wire::Publish {
            request_id,
            topic,
            key,
            value,
            headers,
        } => {
            if Uuid::parse_str(&request_id).is_err() {
                issues.push("\"requestId\" must be a UUID".to_string());
            }
            check_topic(&topic, &mut issues);
            if key.as_deref().is_some_and(str::is_empty) {
                issues.push("\"key\" must not be empty".to_string());
            }
            ClientMessage::Publish(PublishCommand {
                request_id,
                topic,
                key,
                value,
                headers: headers.unwrap_or_default().into_iter().collect(),
            })
        }
    };

    if issues.is_empty() {
        Ok(message)
    } else {
        Err(ParseError::Invalid(issues))
    }
}
