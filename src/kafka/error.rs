use kafkit_client::{BrokerError, Error, ProducerError};

use crate::errors::{
    api_error::ApiError,
    catalog::{
        CatalogEntry, bad_gateway, gateway_timeout, internal_server_error, not_found,
        payload_too_large,
    },
};

// Kafka protocol code for UnknownTopicOrPartition
const CODE_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

// Kafka protocol code for RequestTimedOut
const CODE_REQUEST_TIMED_OUT: i16 = 7;

// Kafka protocol code for MessageTooLarge
const CODE_MESSAGE_TOO_LARGE: i16 = 10;

// Picks the catalogue entry for a Kafka client error, the mapping agreed for Phase 3
pub fn classify_kafka_error(err: &Error) -> CatalogEntry {
    match err {
        Error::Producer(
            ProducerError::RecordTooLarge { .. } | ProducerError::RequestTooLarge { .. },
        ) => payload_too_large::KAFKA_MESSAGE_TOO_LARGE,
        Error::Producer(ProducerError::TopicHasNoPartitions { .. }) => {
            not_found::RESOURCE_NOT_FOUND
        }
        Error::Producer(ProducerError::MetadataTimeout { .. }) => gateway_timeout::UPSTREAM_TIMEOUT,
        Error::Producer(
            ProducerError::AttemptsExhausted { .. }
            | ProducerError::BatchFailed { .. }
            | ProducerError::BufferExhausted { .. },
        ) => bad_gateway::UPSTREAM_UNAVAILABLE,
        Error::Broker(BrokerError::Response {
            code, retriable, ..
        }) => match *code {
            CODE_MESSAGE_TOO_LARGE => payload_too_large::KAFKA_MESSAGE_TOO_LARGE,
            CODE_UNKNOWN_TOPIC_OR_PARTITION => not_found::RESOURCE_NOT_FOUND,
            CODE_REQUEST_TIMED_OUT => gateway_timeout::UPSTREAM_TIMEOUT,
            // Any other error the broker marks retriable means it is struggling or unreachable
            _ if *retriable => bad_gateway::UPSTREAM_UNAVAILABLE,
            _ => internal_server_error::THIRD_PARTY_ERROR,
        },
        _ => internal_server_error::THIRD_PARTY_ERROR,
    }
}

// Logs a Kafka error and turns it into the API error the caller returns
pub fn map_kafka_error(err: &Error) -> ApiError {
    tracing::error!(target: "Kafka", "kafka error: {err}");

    ApiError::from_entry(classify_kafka_error(err))
}
