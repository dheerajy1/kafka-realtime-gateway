use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use uuid::Uuid;

use super::model::{ParseError, PublishedEvent, parse_publish_request};
use crate::{
    kafka::{
        publisher::{PublishRecord, Publisher},
        registry::TopicRegistry,
    },
    middleware::api_key::ApiKeyIdentity,
    ws::frames::{Frame, error_frame},
};

// The message logic of one ws-publish connection, it knows nothing about the socket so it can be tested without one
pub struct PublishSession {
    // The API key the connection authenticated with
    identity: ApiKeyIdentity,

    topics: Arc<TopicRegistry>,
    publisher: Arc<dyn Publisher>,
}

impl PublishSession {
    pub fn new(
        identity: ApiKeyIdentity,
        topics: Arc<TopicRegistry>,
        publisher: Arc<dyn Publisher>,
    ) -> Self {
        Self {
            identity,
            topics,
            publisher,
        }
    }

    // Handles one text message and returns the frame to answer with
    pub async fn handle_text(&self, text: &str) -> String {
        let request = match parse_publish_request(text) {
            Ok(request) => request,
            Err(ParseError::Malformed) => return error_frame("Malformed JSON"),
            Err(ParseError::Invalid) => return error_frame("Invalid publish message"),
        };

        // D5: only topics that are enabled in the registry may be published to
        if !self.topics.snapshot().is_enabled(&request.topic) {
            tracing::warn!(
                target: "WsPublish",
                "REJECT apiKey={} topic={} reason=topic-not-allowed",
                self.identity.api_key_id, request.topic
            );

            return Frame::Rejected {
                message: "Unauthorized topic",
                reason: "topic-not-allowed",
                request_id: None,
                correlation_id: None,
            }
            .to_text();
        }

        // D15: the correlation id is a UUID v7, the bridge skips anything else
        let correlation_id = Uuid::now_v7().to_string();

        // The event borrows the text it holds, so serde writes it once and nothing is cloned first
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        let value = PublishedEvent {
            correlation_id: &correlation_id,
            api_user_id: self.identity.api_user_id,
            job_name: &request.job_name,
            timestamp: &timestamp,
        }
        .to_text();

        let record = PublishRecord {
            topic: request.topic.clone(),
            key: Some(self.identity.api_key_id.clone()),
            headers: Vec::new(),
            value,
        };

        match self.publisher.publish(record).await {
            Ok(receipt) => {
                tracing::debug!(
                    target: "WsPublish",
                    "SUCCESS apiKey={} topic={} corr={correlation_id} partition={} offset={}",
                    self.identity.api_key_id, request.topic, receipt.partition, receipt.offset
                );

                // This ack means published, not processed
                Frame::Ack {
                    topic: &request.topic,
                    correlation_id: &correlation_id,
                }
                .to_text()
            }
            Err(error) => {
                tracing::warn!(
                    target: "WsPublish",
                    "FAIL apiKey={} topic={} err={}",
                    self.identity.api_key_id, request.topic, error.error()
                );

                // D14: a Kafka failure gets its own error frame, not Malformed JSON
                Frame::Rejected {
                    message: "Kafka publish failed",
                    reason: error.error(),
                    request_id: None,
                    correlation_id: None,
                }
                .to_text()
            }
        }
    }
}
