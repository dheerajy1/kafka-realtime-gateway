use std::sync::Arc;

use async_trait::async_trait;
use kafkit_client::{KafkaProducer, ProduceRecord, ProducerConfig, RecordHeader};
use tokio::sync::RwLock;

use super::{
    activity::ProducerActivity,
    error::map_kafka_error,
    publisher::{PublishReceipt, PublishRecord, Publisher},
};
use crate::errors::{api_error::ApiError, catalog::bad_gateway};

// Largest produce request, the 10 MB message limit (D11) plus room for headers and batch overhead
const MAX_REQUEST_SIZE: usize = 11 * 1024 * 1024;

// How long the broker may take to answer one request, the old kafkajs requestTimeout
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// The real publisher, one shared kafkit producer, None once it has been shut down
pub struct KafkaPublisher {
    producer: RwLock<Option<KafkaProducer>>,

    // Remembers the result of the last publish for the health result
    activity: Arc<ProducerActivity>,
}

impl KafkaPublisher {
    // Connects to the brokers, fails when none is reachable so boot can exit (decision: connect at boot)
    pub async fn connect(
        brokers: Vec<String>,
        client_id: &str,
        activity: Arc<ProducerActivity>,
    ) -> kafkit_client::Result<Self> {
        // The constructor needs one address, the real list replaces it right after
        let config = ProducerConfig::new(String::new())
            .with_bootstrap_servers(brokers)
            .with_client_id(client_id)
            // Acks all, the old kafkajs default
            .with_acks(-1)
            .with_enable_idempotence(true)
            .with_max_request_size(MAX_REQUEST_SIZE)
            .with_request_timeout(REQUEST_TIMEOUT);

        let producer = KafkaProducer::connect(config).await?;

        Ok(Self {
            producer: RwLock::new(Some(producer)),
            activity,
        })
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    async fn publish(&self, record: PublishRecord) -> Result<PublishReceipt, ApiError> {
        let guard = self.producer.read().await;

        // After shutdown there is no producer, answer as if the broker is unreachable
        let Some(producer) = guard.as_ref() else {
            return Err(ApiError::from_entry(bad_gateway::UPSTREAM_UNAVAILABLE));
        };

        let mut message = ProduceRecord::without_partition(record.topic, record.value);

        if let Some(key) = record.key {
            message = message.with_key(key);
        }

        let headers = record
            .headers
            .into_iter()
            .map(|(name, value)| RecordHeader::new(name, value));

        let sent = producer.send(message.with_headers(headers)).await;

        // The health result shows whether the last publish worked
        self.activity.record(sent.is_ok());

        let ack = sent.map_err(|err| map_kafka_error(&err))?;

        Ok(PublishReceipt {
            topic: ack.topic,
            partition: ack.partition,
            offset: ack.offset,
        })
    }

    async fn shutdown(&self) {
        // The write lock waits for publishes that are still running, then the producer is taken out
        let Some(producer) = self.producer.write().await.take() else {
            return;
        };

        if let Err(err) = producer.flush().await {
            tracing::error!(target: "Kafka", "producer flush failed: {err}");
        }

        if let Err(err) = producer.shutdown().await {
            tracing::error!(target: "Kafka", "producer shutdown failed: {err}");
        }
    }
}
