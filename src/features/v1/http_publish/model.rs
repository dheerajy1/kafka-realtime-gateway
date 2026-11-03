use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use validator::{Validate, ValidationError};

use crate::kafka::inbound::is_uuid_v7;

// Checks that a correlation id is a UUID of version 7
pub fn validate_correlation_id(value: &str) -> Result<(), ValidationError> {
    if is_uuid_v7(value) {
        return Ok(());
    }

    Err(ValidationError::new("correlation_id_uuid_v7")
        .with_message("\"correlationId\" must be a UUID v7".into()))
}

// Body of POST /api/v1/http-publish
#[derive(Debug, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HttpPublishRequest {
    // Kafka topic to publish to
    #[validate(length(min = 1, message = "\"topic\" must not be empty"))]
    pub topic: String,

    // Id that ties the event to its processing, used as the Kafka message key
    #[validate(custom(function = "validate_correlation_id"))]
    pub correlation_id: String,

    // The payload, any JSON value, the gateway does not look inside it
    pub value: Value,
}

// Data returned after the event is accepted
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HttpPublishData {
    // Topic the event was published to
    pub topic: String,

    // The correlation id of the event
    pub correlation_id: String,
}
