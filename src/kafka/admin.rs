use std::time::Duration;

use async_trait::async_trait;
use kafkit_client::{AdminConfig, KafkaAdmin};

use super::error::map_kafka_error;
use crate::errors::api_error::ApiError;

// How long the broker may take to answer one admin request, same as the producer
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// Lists the topics that exist on the broker, the real implementation wraps kafkit and tests use a fake
#[async_trait]
pub trait TopicLister: Send + Sync {
    // Returns the name of every topic on the broker
    async fn list_topic_names(&self) -> Result<Vec<String>, ApiError>;
}

// The real topic lister, one admin client created at boot and reused for every check
pub struct KafkaTopicLister {
    admin: KafkaAdmin,
}

impl KafkaTopicLister {
    // Connects to the brokers, fails when none is reachable so boot can exit
    pub async fn connect(brokers: Vec<String>, client_id: &str) -> kafkit_client::Result<Self> {
        // The constructor needs one address, the real list replaces it right after
        let config = AdminConfig::new(String::new())
            .with_bootstrap_servers(brokers)
            .with_client_id(client_id)
            .with_request_timeout(REQUEST_TIMEOUT);

        Ok(Self {
            admin: KafkaAdmin::connect(config).await?,
        })
    }
}

#[async_trait]
impl TopicLister for KafkaTopicLister {
    async fn list_topic_names(&self) -> Result<Vec<String>, ApiError> {
        let topics = self
            .admin
            .list_topics()
            .await
            .map_err(|err| map_kafka_error(&err))?;

        Ok(topics.into_iter().map(|topic| topic.name).collect())
    }
}
