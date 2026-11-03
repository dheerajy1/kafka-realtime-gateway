use kafka_api_gateway::{
    errors::catalog::{
        bad_gateway, gateway_timeout, internal_server_error, not_found, payload_too_large,
    },
    kafka::error::classify_kafka_error,
};
use kafkit_client::{BrokerError, Error, ProducerError};

// Builds a broker response error with the given protocol code and retriable flag
fn broker(code: i16, retriable: bool) -> Error {
    Error::Broker(BrokerError::Response {
        operation: "produce",
        resource: String::new(),
        code,
        name: "Test".to_string(),
        retriable,
        fatal: false,
        transaction_abort_required: false,
    })
}

// The broker's MessageTooLarge is a 413 even though it is not retriable
#[test]
fn broker_message_too_large_maps_to_413_entry() {
    let entry = classify_kafka_error(&broker(10, false));

    assert_eq!(
        entry.error,
        payload_too_large::KAFKA_MESSAGE_TOO_LARGE.error
    );
}

// A record over the producer's own limit never reaches the broker and is also a 413
#[test]
fn client_side_record_too_large_maps_to_413_entry() {
    let err = Error::Producer(ProducerError::RecordTooLarge {
        size: 2,
        limit_name: "max_request_size",
        limit: 1,
    });

    let entry = classify_kafka_error(&err);

    assert_eq!(
        entry.error,
        payload_too_large::KAFKA_MESSAGE_TOO_LARGE.error
    );
}

// A topic the broker does not know wins over the retriable flag
#[test]
fn unknown_topic_maps_to_not_found() {
    let entry = classify_kafka_error(&broker(3, true));

    assert_eq!(entry.error, not_found::RESOURCE_NOT_FOUND.error);
}

// A request timeout wins over the retriable flag
#[test]
fn request_timed_out_maps_to_upstream_timeout() {
    let entry = classify_kafka_error(&broker(7, true));

    assert_eq!(entry.error, gateway_timeout::UPSTREAM_TIMEOUT.error);
}

// A metadata timeout is also a gateway timeout
#[test]
fn metadata_timeout_maps_to_upstream_timeout() {
    let err = Error::Producer(ProducerError::MetadataTimeout {
        topic: "orders".to_string(),
        max_block_ms: 60_000,
    });

    let entry = classify_kafka_error(&err);

    assert_eq!(entry.error, gateway_timeout::UPSTREAM_TIMEOUT.error);
}

// Any other retriable broker error means the broker is struggling or unreachable
#[test]
fn other_retriable_broker_error_maps_to_upstream_unavailable() {
    let entry = classify_kafka_error(&broker(6, true));

    assert_eq!(entry.error, bad_gateway::UPSTREAM_UNAVAILABLE.error);
}

// Exhausted retries mean the broker could not be reached
#[test]
fn attempts_exhausted_maps_to_upstream_unavailable() {
    let err = Error::Producer(ProducerError::AttemptsExhausted {
        operation: "produce",
        resource: String::new(),
        attempts: 3,
    });

    let entry = classify_kafka_error(&err);

    assert_eq!(entry.error, bad_gateway::UPSTREAM_UNAVAILABLE.error);
}

// A non-retriable broker error nobody handles specifically stays generic
#[test]
fn unhandled_broker_error_maps_to_third_party_error() {
    let entry = classify_kafka_error(&broker(29, false));

    assert_eq!(entry.error, internal_server_error::THIRD_PARTY_ERROR.error);
}

// A cancelled operation is not a broker problem and stays generic
#[test]
fn cancelled_maps_to_third_party_error() {
    let entry = classify_kafka_error(&Error::Cancelled);

    assert_eq!(entry.error, internal_server_error::THIRD_PARTY_ERROR.error);
}
