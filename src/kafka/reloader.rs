use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;

use super::{registry::TopicPolicy, sync::TopicSync};
use crate::errors::api_error::ApiError;

// What one reload found
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadSummary {
    // How many topics are enabled now
    pub enabled_topics: usize,

    // How many of them the gateway consumes
    pub consume_topics: usize,

    // True when the topics or their flags differ from before the reload
    pub changed: bool,

    // Enabled topics that do not exist in Kafka, sorted
    pub missing_topics: Vec<String>,
}

// Reloads the topic policy on demand, the real implementation is TopicReloader and tests use a fake
#[async_trait]
pub trait TopicReload: Send + Sync {
    // Reloads the policy from the database, applies it to the consumer and returns what changed
    // The trigger names who asked, for example startup or api
    async fn reload(&self, trigger: &str) -> Result<ReloadSummary, ApiError>;
}

// Applies a loaded policy to the Kafka consumer, the real implementation is the bridge consumer
#[async_trait]
pub trait PolicyApplier: Send + Sync {
    // Subscribes to the new topics and resumes the partitions of topics that are consumed again
    async fn apply(&self, subscribable: &[String], policy: &TopicPolicy) -> Result<(), ApiError>;
}

// The real reloader
pub struct TopicReloader {
    sync: TopicSync,
    applier: Arc<dyn PolicyApplier>,

    // Only one reload runs at a time, a second request waits for the first
    serial: tokio::sync::Mutex<()>,

    // The missing topics of the last reload, so the same problem is logged once and not on every reload
    last_missing: Mutex<Vec<String>>,
}

impl TopicReloader {
    pub fn new(sync: TopicSync, applier: Arc<dyn PolicyApplier>) -> Self {
        Self {
            sync,
            applier,
            serial: tokio::sync::Mutex::new(()),
            last_missing: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl TopicReload for TopicReloader {
    async fn reload(&self, trigger: &str) -> Result<ReloadSummary, ApiError> {
        let _serial = self.serial.lock().await;

        let report = self.sync.run().await?;

        self.applier
            .apply(&report.subscribable_topics(), &report.policy)
            .await?;

        let summary = ReloadSummary {
            enabled_topics: report.policy.enabled_topics().len(),
            consume_topics: report.policy.consume_topics().len(),
            changed: report.policy_changed(),
            missing_topics: report.missing_topics.clone(),
        };

        if summary.changed {
            tracing::info!(
                target: "TopicRegistry",
                "TOPICS_RELOADED trigger={trigger} enabled={} consume={} kafkaTopics={}",
                summary.enabled_topics,
                summary.consume_topics,
                report.kafka_topic_count
            );
        }

        let mut last_missing = self
            .last_missing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if *last_missing != summary.missing_topics {
            if summary.missing_topics.is_empty() {
                tracing::info!(target: "TopicRegistry", "ALL_ENABLED_TOPICS_EXIST_IN_KAFKA trigger={trigger}");
            } else {
                tracing::error!(
                    target: "TopicRegistry",
                    "MISSING_KAFKA_TOPICS trigger={trigger} topics={:?}",
                    summary.missing_topics
                );
            }

            last_missing.clone_from(&summary.missing_topics);
        }

        Ok(summary)
    }
}
