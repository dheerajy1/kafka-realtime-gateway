use std::{collections::HashSet, sync::Arc};

use super::{
    admin::TopicLister,
    registry::{TopicPolicy, TopicRegistry},
};
use crate::{db::store::TopicStore, errors::api_error::ApiError};

// What one sync found
#[derive(Debug)]
pub struct SyncReport {
    // The policy that was just loaded and swapped in
    pub policy: Arc<TopicPolicy>,

    // The policy that was in place before this sync
    pub previous: Arc<TopicPolicy>,

    // Enabled topics that do not exist on the broker, sorted
    pub missing_topics: Vec<String>,

    // How many topics the broker has
    pub kafka_topic_count: usize,
}

impl SyncReport {
    // True when every enabled topic exists on the broker
    pub fn is_ok(&self) -> bool {
        self.missing_topics.is_empty()
    }

    // Topics the gateway consumes that also exist on the broker, sorted
    pub fn subscribable_topics(&self) -> Vec<String> {
        self.policy
            .consume_topics()
            .into_iter()
            .filter(|topic| !self.missing_topics.contains(topic))
            .collect()
    }

    // True when the topics or their flags differ from the previous policy
    pub fn policy_changed(&self) -> bool {
        self.policy.describe() != self.previous.describe()
    }
}

// Loads the topic policy from the database and checks it against the broker
pub struct TopicSync {
    store: Arc<dyn TopicStore>,
    lister: Arc<dyn TopicLister>,
    registry: Arc<TopicRegistry>,
}

impl TopicSync {
    pub fn new(
        store: Arc<dyn TopicStore>,
        lister: Arc<dyn TopicLister>,
        registry: Arc<TopicRegistry>,
    ) -> Self {
        Self {
            store,
            lister,
            registry,
        }
    }

    // Reloads the policy, then checks that every enabled topic exists on the broker
    // This method does not log, the caller decides what to report so a quiet reload stays quiet
    pub async fn run(&self) -> Result<SyncReport, ApiError> {
        let previous = self.registry.snapshot();
        let policy = self.registry.reload(self.store.as_ref()).await?;

        let kafka_topics: HashSet<String> =
            self.lister.list_topic_names().await?.into_iter().collect();

        let missing_topics: Vec<String> = policy
            .enabled_topics()
            .into_iter()
            .filter(|topic| !kafka_topics.contains(topic))
            .collect();

        Ok(SyncReport {
            policy,
            previous,
            missing_topics,
            kafka_topic_count: kafka_topics.len(),
        })
    }
}
