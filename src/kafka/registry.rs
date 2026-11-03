use std::{
    collections::HashMap,
    sync::{Arc, PoisonError, RwLock},
};

use crate::{
    db::store::{TopicRow, TopicStore},
    errors::api_error::ApiError,
};

// What the gateway does with one enabled topic
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicFlags {
    // The gateway consumes the topic and forwards it to subscribers
    pub gateway_consume: bool,

    // A subscriber must acknowledge each message before its offset is committed
    pub ack_required: bool,

    // Subscribers may publish to the topic over the WebSocket
    pub subscriber_publish_allowed: bool,
}

// A snapshot of the enabled topics, it never changes after it is built
#[derive(Debug, Default)]
pub struct TopicPolicy {
    topics: HashMap<String, TopicFlags>,
}

impl TopicPolicy {
    // Builds the snapshot from the database rows, disabled topics are dropped
    pub fn from_rows(rows: Vec<TopicRow>) -> Self {
        let topics = rows
            .into_iter()
            .filter(|row| row.enabled)
            .map(|row| {
                (
                    row.topic_name,
                    TopicFlags {
                        gateway_consume: row.gateway_consume,
                        ack_required: row.ack_required,
                        subscriber_publish_allowed: row.subscriber_publish_allowed,
                    },
                )
            })
            .collect();

        Self { topics }
    }

    // The flags of an enabled topic, None for an unknown or disabled topic
    pub fn flags(&self, topic: &str) -> Option<TopicFlags> {
        self.topics.get(topic).copied()
    }

    // True when the topic exists in the policy and is enabled
    pub fn is_enabled(&self, topic: &str) -> bool {
        self.topics.contains_key(topic)
    }

    // True when the topic needs a subscriber ACK before its offset is committed
    pub fn ack_required(&self, topic: &str) -> bool {
        self.flags(topic).is_some_and(|flags| flags.ack_required)
    }

    // True when subscribers may publish to the topic
    pub fn subscriber_publish_allowed(&self, topic: &str) -> bool {
        self.flags(topic)
            .is_some_and(|flags| flags.subscriber_publish_allowed)
    }

    // Every enabled topic name, sorted
    pub fn enabled_topics(&self) -> Vec<String> {
        let mut names: Vec<String> = self.topics.keys().cloned().collect();
        names.sort();
        names
    }

    // Enabled topics the gateway consumes, sorted
    pub fn consume_topics(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .topics
            .iter()
            .filter(|(_, flags)| flags.gateway_consume)
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }

    // Every enabled topic with its flags, sorted by name, used to see whether a reload changed anything
    pub fn describe(&self) -> Vec<(String, TopicFlags)> {
        let mut topics: Vec<(String, TopicFlags)> = self
            .topics
            .iter()
            .map(|(name, flags)| (name.clone(), *flags))
            .collect();
        topics.sort_by(|a, b| a.0.cmp(&b.0));
        topics
    }
}

// The shared topic policy, every reload swaps in a whole new snapshot
#[derive(Debug, Default)]
pub struct TopicRegistry {
    current: RwLock<Arc<TopicPolicy>>,
}

impl TopicRegistry {
    // The current snapshot, cheap to clone and safe to hold while a reload happens
    pub fn snapshot(&self) -> Arc<TopicPolicy> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    // Swaps in a new snapshot
    pub fn replace(&self, policy: Arc<TopicPolicy>) {
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = policy;
    }

    // Loads the rows from the store and swaps in the new snapshot, the old one stays when loading fails
    pub async fn reload(&self, store: &dyn TopicStore) -> Result<Arc<TopicPolicy>, ApiError> {
        let rows = store.load_topics().await?;
        let policy = Arc::new(TopicPolicy::from_rows(rows));
        self.replace(policy.clone());

        Ok(policy)
    }
}
