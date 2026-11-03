use std::sync::Arc;

use serde_json::{Value, json};

use super::model::{ClientMessage, ParseError, PublishCommand, parse_client_message};
use crate::{
    kafka::{
        publisher::{PublishRecord, Publisher},
        registry::TopicRegistry,
        subscribers::SubscriberRegistry,
    },
    ws::{error_frame, frame},
};

// The message logic of one subscriber connection, it knows nothing about the socket so it can be tested without one
pub struct SubscriberSession {
    // Id of this connection in the subscriber registry
    id: u64,

    // The x-subscriber-id the client sent, used in logs
    subscriber: String,

    topics: Arc<TopicRegistry>,
    subscribers: Arc<SubscriberRegistry>,
    publisher: Arc<dyn Publisher>,
}

impl SubscriberSession {
    pub fn new(
        id: u64,
        subscriber: String,
        topics: Arc<TopicRegistry>,
        subscribers: Arc<SubscriberRegistry>,
        publisher: Arc<dyn Publisher>,
    ) -> Self {
        Self {
            id,
            subscriber,
            topics,
            subscribers,
            publisher,
        }
    }

    // Handles one text message and returns the frame to answer with
    pub async fn handle_text(&self, text: &str) -> String {
        match parse_client_message(text) {
            Ok(ClientMessage::Subscribe { topic }) => self.subscribe(topic),
            Ok(ClientMessage::Unsubscribe { topic }) => self.unsubscribe(topic),
            Ok(ClientMessage::Processed { correlation_id }) => self.processed(correlation_id),
            Ok(ClientMessage::Publish(command)) => self.publish(command).await,
            Err(ParseError::Malformed) => {
                tracing::warn!(
                    target: "WsSubscribe",
                    "MALFORMED_JSON subscriber={} id={}",
                    self.subscriber, self.id
                );
                error_frame("Malformed JSON")
            }
            Err(ParseError::Invalid(issues)) => {
                tracing::warn!(
                    target: "WsSubscribe",
                    "INVALID_MESSAGE subscriber={} id={} issues={issues:?}",
                    self.subscriber, self.id
                );

                let issues: Vec<Value> = issues
                    .into_iter()
                    .map(|issue| json!({ "message": issue }))
                    .collect();

                frame(json!({
                    "type": "error",
                    "message": "Invalid message format",
                    "issues": issues,
                }))
            }
        }
    }

    // A subscribe request is not checked against the registry, a topic nobody consumes never delivers
    fn subscribe(&self, topic: String) -> String {
        if !self.subscribers.subscribe(self.id, &topic) {
            return error_frame("Subscriber is not connected");
        }

        tracing::info!(
            target: "WsSubscribe",
            "SUBSCRIBED subscriber={} id={} topic={topic}",
            self.subscriber, self.id
        );

        frame(json!({ "type": "subscribed", "topic": topic }))
    }

    fn unsubscribe(&self, topic: String) -> String {
        if !self.subscribers.unsubscribe(self.id, &topic) {
            return error_frame("Subscriber is not connected");
        }

        tracing::info!(
            target: "WsSubscribe",
            "UNSUBSCRIBED subscriber={} id={} topic={topic}",
            self.subscriber, self.id
        );

        frame(json!({ "type": "unsubscribed", "topic": topic }))
    }

    // The subscriber says it finished a message, only the owner of the message may do that
    fn processed(&self, correlation_id: String) -> String {
        match self.subscribers.acknowledge(&correlation_id, self.id) {
            Ok(message) => {
                tracing::debug!(
                    target: "WsSubscribe",
                    "ACK_RECEIVED subscriber={} id={} corr={correlation_id} topic={} offset={}",
                    self.subscriber, self.id, message.topic, message.offset
                );

                frame(json!({ "type": "ack-received", "correlationId": correlation_id }))
            }
            Err(rejection) => {
                tracing::warn!(
                    target: "WsSubscribe",
                    "ACK_REJECTED subscriber={} id={} corr={correlation_id} reason={}",
                    self.subscriber, self.id, rejection.message()
                );

                frame(json!({
                    "type": "error",
                    "message": "ACK rejected",
                    "reason": rejection.message(),
                    "correlationId": correlation_id,
                }))
            }
        }
    }

    // A subscriber may publish only to topics marked subscriber_publish_allowed
    async fn publish(&self, command: PublishCommand) -> String {
        let PublishCommand {
            request_id,
            topic,
            key,
            value,
            headers,
        } = command;

        if !self.topics.snapshot().subscriber_publish_allowed(&topic) {
            tracing::warn!(
                target: "WsPublish",
                "REJECT subscriber={} id={} requestId={request_id} topic={topic} reason=topic-not-allowed",
                self.subscriber, self.id
            );

            return frame(json!({
                "type": "error",
                "message": "Unauthorized topic",
                "reason": "topic-not-allowed",
                "requestId": request_id,
            }));
        }

        let record = PublishRecord {
            topic: topic.clone(),
            key,
            headers,
            value: Value::Object(value).to_string(),
        };

        match self.publisher.publish(record).await {
            Ok(receipt) => {
                tracing::debug!(
                    target: "WsPublish",
                    "SUCCESS subscriber={} id={} requestId={request_id} topic={topic} partition={} offset={}",
                    self.subscriber, self.id, receipt.partition, receipt.offset
                );

                frame(json!({
                    "type": "published",
                    "requestId": request_id,
                    "topic": receipt.topic,
                    "partition": receipt.partition,
                    "offset": receipt.offset.to_string(),
                }))
            }
            Err(error) => {
                tracing::warn!(
                    target: "WsPublish",
                    "FAIL subscriber={} id={} requestId={request_id} topic={topic} err={}",
                    self.subscriber, self.id, error.error()
                );

                frame(json!({
                    "type": "error",
                    "message": "Kafka publish failed",
                    "reason": error.error(),
                    "requestId": request_id,
                }))
            }
        }
    }
}
