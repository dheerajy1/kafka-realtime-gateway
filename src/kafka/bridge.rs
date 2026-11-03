use std::{sync::Arc, time::Duration};

use tokio::sync::{oneshot, watch};

use super::{
    inbound::{Inbound, event_frame, parse_inbound},
    registry::{TopicFlags, TopicRegistry},
    subscribers::{AckedMessage, SubscriberRegistry},
};

// How long the owner of an ACK topic message has to acknowledge it
pub const ACK_TIMEOUT: Duration = Duration::from_secs(5 * 60);

// How often the bridge looks for a subscriber while an ACK topic has none
pub const OWNER_POLL: Duration = Duration::from_millis(500);

// One Kafka message, already detached from the Kafka client types
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRecord {
    // Topic the message was read from
    pub topic: String,

    // Partition the message was read from
    pub partition: i32,

    // Offset of the message inside the partition
    pub offset: i64,

    // Kafka headers decoded to text, headers without a value are left out
    pub headers: Vec<(String, String)>,

    // The raw message value, None for a message without a value
    pub value: Option<Vec<u8>>,
}

// What the consumer loop has to do after one message was processed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    // The message is done, commit its offset
    Commit,

    // The message was not acknowledged, read it again from its offset
    Retry,

    // The topic is disabled or no longer consumed, pause its partition and read the message again after it is enabled
    Pause,

    // The gateway is shutting down, commit nothing and stop the loop
    Stop,
}

// What waiting for a subscriber ended with
enum OwnerWait {
    Owner(u64),
    Pause,
    Stop,
}

// Turns Kafka messages into subscriber frames and decides when their offsets may be committed
pub struct Bridge {
    topics: Arc<TopicRegistry>,
    subscribers: Arc<SubscriberRegistry>,
    // Turns true when the gateway shuts down
    shutdown: watch::Receiver<bool>,
    ack_timeout: Duration,
    owner_poll: Duration,
}

impl Bridge {
    pub fn new(
        topics: Arc<TopicRegistry>,
        subscribers: Arc<SubscriberRegistry>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            topics,
            subscribers,
            shutdown,
            ack_timeout: ACK_TIMEOUT,
            owner_poll: OWNER_POLL,
        }
    }

    // Replaces the ACK timeout and the subscriber poll interval, tests use short ones
    pub fn with_timeouts(mut self, ack_timeout: Duration, owner_poll: Duration) -> Self {
        self.ack_timeout = ack_timeout;
        self.owner_poll = owner_poll;
        self
    }

    // The flags of a topic that is enabled and consumed by the gateway
    fn consumable_flags(&self, topic: &str) -> Option<TopicFlags> {
        self.topics
            .snapshot()
            .flags(topic)
            .filter(|flags| flags.gateway_consume)
    }

    // Processes one message: parse, route, deliver and, for ACK topics, wait for the ACK
    pub async fn process(&self, record: &InboundRecord) -> Outcome {
        let (correlation_id, payload) = match parse_inbound(record.value.as_deref()) {
            Inbound::Valid {
                correlation_id,
                payload,
            } => (correlation_id, payload),
            // An empty message is skipped without a log, like the old gateway
            Inbound::Empty => return Outcome::Commit,
            Inbound::Malformed => {
                tracing::error!(
                    target: "WsBridge",
                    "MALFORMED_JSON topic={} partition={} offset={}",
                    record.topic, record.partition, record.offset
                );
                return Outcome::Commit;
            }
            Inbound::Invalid => {
                tracing::error!(
                    target: "WsBridge",
                    "INVALID_MESSAGE topic={} partition={} offset={}",
                    record.topic, record.partition, record.offset
                );
                return Outcome::Commit;
            }
        };

        // A topic that was disabled after the gateway subscribed waits in Kafka
        let Some(flags) = self.consumable_flags(&record.topic) else {
            return Outcome::Pause;
        };

        let owner = match self.subscribers.pick_owner(&record.topic) {
            Some(owner) => Some(owner),
            None if flags.ack_required => {
                match self.wait_for_owner(record, &correlation_id).await {
                    OwnerWait::Owner(owner) => Some(owner),
                    OwnerWait::Pause => return Outcome::Pause,
                    OwnerWait::Stop => return Outcome::Stop,
                }
            }
            None => None,
        };

        // Nobody listens to a topic that needs no ACK, the message is dropped like the old gateway
        let Some(owner) = owner else {
            tracing::debug!(
                target: "WsBridge",
                "DROP topic={} corr={correlation_id} reason=no-subscribers",
                record.topic
            );
            return Outcome::Commit;
        };

        let frame = event_frame(
            &record.topic,
            record.partition,
            record.offset,
            &record.headers,
            payload,
        );

        if !flags.ack_required {
            let report = self.subscribers.deliver(&record.topic, &frame);
            tracing::debug!(
                target: "WsBridge",
                "DELIVERED topic={} corr={correlation_id} delivered={}",
                record.topic, report.delivered
            );
            return Outcome::Commit;
        }

        // The pending ACK is registered before the frame goes out, so a fast ACK cannot arrive first
        let waiter = self.subscribers.register_pending(
            &correlation_id,
            owner,
            AckedMessage {
                topic: record.topic.clone(),
                partition: record.partition,
                offset: record.offset,
            },
        );
        self.subscribers.deliver(&record.topic, &frame);

        self.wait_for_ack(record, &correlation_id, waiter).await
    }

    // Waits until a subscriber exists for the topic, the gateway stops or the topic is disabled
    async fn wait_for_owner(&self, record: &InboundRecord, correlation_id: &str) -> OwnerWait {
        tracing::info!(
            target: "WsBridge",
            "WAITING_FOR_SUBSCRIBER topic={} corr={correlation_id} partition={} offset={}",
            record.topic, record.partition, record.offset
        );

        let mut shutdown = self.shutdown.clone();

        loop {
            if *shutdown.borrow() {
                return OwnerWait::Stop;
            }

            if self.consumable_flags(&record.topic).is_none() {
                return OwnerWait::Pause;
            }

            if let Some(owner) = self.subscribers.pick_owner(&record.topic) {
                return OwnerWait::Owner(owner);
            }

            tokio::select! {
                _ = tokio::time::sleep(self.owner_poll) => {}
                _ = shutdown.changed() => {}
            }
        }
    }

    // Waits for the ACK of the owner, a timeout or a lost owner means the message is read again
    async fn wait_for_ack(
        &self,
        record: &InboundRecord,
        correlation_id: &str,
        waiter: oneshot::Receiver<()>,
    ) -> Outcome {
        let mut shutdown = self.shutdown.clone();

        tokio::select! {
            result = tokio::time::timeout(self.ack_timeout, waiter) => match result {
                Ok(Ok(())) => {
                    tracing::debug!(
                        target: "WsBridge",
                        "ACKED topic={} partition={} offset={} corr={correlation_id}",
                        record.topic, record.partition, record.offset
                    );
                    Outcome::Commit
                }
                Ok(Err(_)) => {
                    tracing::warn!(
                        target: "WsBridge",
                        "ACK_WAIT_FAILED topic={} partition={} offset={} corr={correlation_id} reason=owner-gone",
                        record.topic, record.partition, record.offset
                    );
                    Outcome::Retry
                }
                Err(_) => {
                    self.subscribers.cancel_pending(correlation_id);
                    tracing::warn!(
                        target: "WsBridge",
                        "ACK_WAIT_FAILED topic={} partition={} offset={} corr={correlation_id} reason=timeout",
                        record.topic, record.partition, record.offset
                    );
                    Outcome::Retry
                }
            },
            // A closed channel means the gateway is going away, which is the same as a shutdown
            _ = shutdown.wait_for(|stopping| *stopping) => {
                self.subscribers.cancel_pending(correlation_id);
                Outcome::Stop
            }
        }
    }
}
