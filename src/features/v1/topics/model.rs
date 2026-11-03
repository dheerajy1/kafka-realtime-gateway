use serde::Serialize;
use utoipa::ToSchema;

use crate::kafka::reloader::ReloadSummary;

// Data returned after a topic reload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicReloadData {
    // How many topics are enabled now
    pub enabled_topics: usize,

    // How many of them the gateway consumes
    pub consume_topics: usize,

    // True when the topics or their flags differ from before the reload
    pub changed: bool,

    // Enabled topics that do not exist in Kafka, they are not subscribed
    pub missing_topics: Vec<String>,
}

impl From<ReloadSummary> for TopicReloadData {
    fn from(summary: ReloadSummary) -> Self {
        Self {
            enabled_topics: summary.enabled_topics,
            consume_topics: summary.consume_topics,
            changed: summary.changed,
            missing_topics: summary.missing_topics,
        }
    }
}
