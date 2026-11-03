use async_trait::async_trait;

use crate::errors::api_error::ApiError;

// One message to send to Kafka
#[derive(Debug, Clone)]
pub struct PublishRecord {
    // Target topic
    pub topic: String,

    // Kafka message key, None sends without a key
    pub key: Option<String>,

    // Kafka headers as name and value pairs
    pub headers: Vec<(String, String)>,

    // The message value, JSON text
    pub value: String,
}

// Where the broker stored a published message
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReceipt {
    // Topic the message was written to
    pub topic: String,

    // Partition the message was written to
    pub partition: i32,

    // Offset of the message inside the partition
    pub offset: i64,
}

// Sends messages to Kafka, the real implementation wraps kafkit-client and tests use a fake
#[async_trait]
pub trait Publisher: Send + Sync {
    // Sends one message and waits for the broker acknowledgement, failures are mapped to API errors
    async fn publish(&self, record: PublishRecord) -> Result<PublishReceipt, ApiError>;

    // Flushes and closes the producer during graceful shutdown, later publishes fail
    async fn shutdown(&self);
}
