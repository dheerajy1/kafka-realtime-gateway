use async_trait::async_trait;
use kafkit_client::{
    AutoOffsetReset, CommitOffset, ConsumerConfig, ConsumerRecord, KafkaConsumer, TopicPartition,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};
use tokio::sync::{RwLock, watch};

use super::{
    activity::ConsumerActivity,
    bridge::{Bridge, InboundRecord, Outcome},
    error::map_kafka_error,
    registry::TopicPolicy,
    reloader::PolicyApplier,
};
use crate::errors::api_error::ApiError;

// How long one poll waits for records
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

// How long the loop waits after a failed poll before it tries again
const RETRY_DELAY: Duration = Duration::from_secs(3);

// How long the loop waits while there is nothing to subscribe to yet
const IDLE_DELAY: Duration = Duration::from_secs(1);

// After this many failed polls in a row the consumer is replaced by a new one, its connection is dead and does not recover by itself
const RECONNECT_AFTER_FAILURES: u32 = 3;

// How long closing an old consumer may take, one with a dead connection may never answer
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

// Largest message the consumer can fetch, the 10 MB message limit (D11) plus overhead, same as the producer
const MAX_PARTITION_BYTES: i32 = 11 * 1024 * 1024;

// What the consumer remembers between polls
#[derive(Default)]
struct State {
    // Topics the consumer is subscribed to, it only ever grows
    subscribed: BTreeSet<String>,

    // Partitions paused because their topic is disabled or not consumed
    paused: BTreeSet<TopicPartition>,
}

// The Kafka consumer of the bridge, one consumer group member that feeds every record to the bridge
pub struct BridgeConsumer {
    // None once the consumer was shut down
    consumer: RwLock<Option<KafkaConsumer>>,
    state: Mutex<State>,

    // Needed to build a new consumer after a broken connection
    config: ConsumerConfig,

    // Remembers what the loop did last for the health result
    activity: Arc<ConsumerActivity>,
}

impl BridgeConsumer {
    // Connects to the brokers, offsets are committed by the bridge and never automatically
    pub async fn connect(
        brokers: Vec<String>,
        client_id: &str,
        group_id: &str,
        activity: Arc<ConsumerActivity>,
    ) -> kafkit_client::Result<Self> {
        // The constructor needs one address, the real list replaces it right after
        let config = ConsumerConfig::new(String::new(), group_id)
            .with_bootstrap_servers(brokers)
            .with_client_id(client_id)
            // A new group starts at the end of each topic, like the old kafkajs consumer
            .with_auto_offset_reset(AutoOffsetReset::Latest)
            .with_partition_max_bytes(MAX_PARTITION_BYTES);

        Ok(Self {
            consumer: RwLock::new(Some(KafkaConsumer::connect(config.clone()).await?)),
            state: Mutex::new(State::default()),
            config,
            activity,
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Applies a freshly loaded policy: subscribes to new topics and resumes partitions whose topic is consumed again
    // The subscription only grows, a disabled topic is paused by the bridge when its next message arrives
    pub async fn apply_policy(
        &self,
        subscribable: &[String],
        policy: &TopicPolicy,
    ) -> kafkit_client::Result<()> {
        let guard = self.consumer.read().await;
        let Some(consumer) = guard.as_ref() else {
            return Ok(());
        };

        let (all, added) = {
            let state = self.state();
            let added: Vec<String> = subscribable
                .iter()
                .filter(|topic| !state.subscribed.contains(*topic))
                .cloned()
                .collect();
            let mut all: Vec<String> = state.subscribed.iter().cloned().collect();
            all.extend(added.iter().cloned());

            (all, added)
        };

        if !added.is_empty() {
            // Subscribe replaces the subscription, so the full list is passed
            consumer.subscribe(all).await?;
            self.state().subscribed.extend(added.iter().cloned());
            tracing::info!(target: "TopicRegistry", "CONSUMER_SUBSCRIBED topics={added:?}");
        }

        let resumable: Vec<TopicPartition> = {
            let state = self.state();
            state
                .paused
                .iter()
                .filter(|partition| {
                    policy
                        .flags(&partition.topic)
                        .is_some_and(|flags| flags.gateway_consume)
                })
                .cloned()
                .collect()
        };

        if !resumable.is_empty() {
            consumer.resume(resumable.clone()).await?;
            let mut state = self.state();
            for partition in &resumable {
                state.paused.remove(partition);
            }
            tracing::info!(
                target: "TopicRegistry",
                "CONSUMER_RESUMED partitions={}",
                resumable.len()
            );
        }

        Ok(())
    }

    // Reads records and feeds them to the bridge until the gateway shuts down
    // A failing consumer is retried every few seconds and the same error is logged once
    pub async fn run(&self, bridge: &Bridge, mut shutdown: watch::Receiver<bool>) {
        let mut last_error = String::new();
        let mut last_reconnect_error = String::new();
        let mut failures: u32 = 0;

        self.activity.set_running(true);

        while !*shutdown.borrow() {
            if self.state().subscribed.is_empty() {
                // Nothing to read yet is not a failure, the loop is alive
                self.activity.record_ok();

                tokio::select! {
                    _ = tokio::time::sleep(IDLE_DELAY) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }

            let polled = {
                let guard = self.consumer.read().await;
                let Some(consumer) = guard.as_ref() else {
                    break;
                };
                consumer.poll_for(POLL_TIMEOUT).await
            };

            let records = match polled {
                Ok(records) => {
                    last_error.clear();
                    last_reconnect_error.clear();
                    failures = 0;
                    self.activity.record_ok();
                    records
                }
                Err(error) => {
                    let text = error.to_string();
                    self.activity.record_error(&text);
                    if text != last_error {
                        tracing::error!(
                            target: "WsBridge",
                            "CONSUMER_POLL_FAILED {text}, retrying every {}s",
                            RETRY_DELAY.as_secs()
                        );
                        last_error = text;
                    }

                    failures += 1;

                    // A consumer with a broken connection does not recover by itself, so it is replaced
                    if failures >= RECONNECT_AFTER_FAILURES {
                        match self.reconnect().await {
                            Ok(()) => {
                                tracing::info!(
                                    target: "WsBridge",
                                    "CONSUMER_RECONNECTED after {failures} failed polls"
                                );
                                failures = 0;
                                last_error.clear();
                                last_reconnect_error.clear();
                                continue;
                            }
                            Err(error) => {
                                let text = error.to_string();
                                self.activity.record_error(&text);
                                if text != last_reconnect_error {
                                    tracing::error!(
                                        target: "WsBridge",
                                        "CONSUMER_RECONNECT_FAILED {text}, retrying every {}s",
                                        RETRY_DELAY.as_secs()
                                    );
                                    last_reconnect_error = text;
                                }
                            }
                        }
                    }

                    tokio::select! {
                        _ = tokio::time::sleep(RETRY_DELAY) => {}
                        _ = shutdown.changed() => {}
                    }
                    continue;
                }
            };

            if self.handle(bridge, records.into_inner()).await {
                break;
            }
        }

        self.activity.set_running(false);
    }

    // Processes one poll in order, returns true when the loop has to stop
    // A partition that has to be read again is skipped for the rest of the poll so nothing after it is committed
    async fn handle(&self, bridge: &Bridge, records: Vec<ConsumerRecord>) -> bool {
        let guard = self.consumer.read().await;
        let Some(consumer) = guard.as_ref() else {
            return true;
        };

        let mut halted: BTreeSet<TopicPartition> = BTreeSet::new();

        for record in records {
            let partition = TopicPartition::new(record.topic.clone(), record.partition);

            if halted.contains(&partition) {
                continue;
            }

            let inbound = to_inbound(record);

            match bridge.process(&inbound).await {
                Outcome::Commit => {
                    // The offset to commit is the next one to read, one past the processed message
                    let offsets = vec![CommitOffset {
                        topic: inbound.topic.clone(),
                        partition: inbound.partition,
                        offset: inbound.offset + 1,
                    }];
                    if let Err(error) = consumer.commit_offsets(offsets).await {
                        tracing::error!(
                            target: "WsBridge",
                            "COMMIT_FAILED topic={} partition={} offset={}: {error}",
                            inbound.topic, inbound.partition, inbound.offset
                        );
                    }
                }
                Outcome::Retry => {
                    halted.insert(partition.clone());
                    if let Err(error) = consumer.seek(partition, inbound.offset).await {
                        tracing::error!(target: "WsBridge", "SEEK_FAILED: {error}");
                    }
                }
                Outcome::Pause => {
                    halted.insert(partition.clone());
                    let seek = consumer.seek(partition.clone(), inbound.offset).await;
                    let pause = consumer.pause(vec![partition.clone()]).await;

                    match seek.and(pause) {
                        Ok(()) => {
                            tracing::info!(
                                target: "TopicRegistry",
                                "CONSUMER_PAUSED topic={} partition={} offset={}",
                                inbound.topic, inbound.partition, inbound.offset
                            );
                            self.state().paused.insert(partition);
                        }
                        Err(error) => {
                            tracing::error!(target: "WsBridge", "PAUSE_FAILED: {error}");
                        }
                    }
                }
                Outcome::Stop => return true,
            }
        }

        false
    }

    // Replaces the consumer with a new one that is subscribed to the same topics
    // The offsets committed by the group stay in Kafka, so the new consumer continues where the old one stopped
    async fn reconnect(&self) -> kafkit_client::Result<()> {
        let fresh = KafkaConsumer::connect(self.config.clone()).await?;

        let topics: Vec<String> = self.state().subscribed.iter().cloned().collect();
        if !topics.is_empty() {
            fresh.subscribe(topics).await?;
        }

        let old = {
            let mut guard = self.consumer.write().await;

            // The gateway is shutting down, the new consumer is not needed
            if guard.is_none() {
                close_in_background(fresh);
                return Ok(());
            }

            guard.replace(fresh)
        };

        // The new consumer starts with nothing paused, the bridge pauses a disabled topic again when its next message arrives
        self.state().paused.clear();

        if let Some(old) = old {
            close_in_background(old);
        }

        Ok(())
    }

    // Closes the consumer, called after the loop has stopped
    pub async fn shutdown(&self) {
        let Some(consumer) = self.consumer.write().await.take() else {
            return;
        };

        if let Err(error) = consumer.shutdown().await {
            tracing::error!(target: "WsBridge", "consumer shutdown failed: {error}");
        }
    }
}

// Closes a consumer without waiting for it, one with a dead connection may never answer
fn close_in_background(consumer: KafkaConsumer) {
    tokio::spawn(async move {
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, consumer.shutdown()).await;
    });
}

// Detaches a Kafka record from the client types, headers without a value are left out
fn to_inbound(record: ConsumerRecord) -> InboundRecord {
    InboundRecord {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        headers: record
            .headers
            .into_iter()
            .filter_map(|header| {
                header
                    .value
                    .map(|value| (header.key, String::from_utf8_lossy(&value).into_owned()))
            })
            .collect(),
        value: record.value.map(|value| value.to_vec()),
    }
}

#[async_trait]
impl PolicyApplier for BridgeConsumer {
    async fn apply(&self, subscribable: &[String], policy: &TopicPolicy) -> Result<(), ApiError> {
        self.apply_policy(subscribable, policy)
            .await
            .map_err(|error| map_kafka_error(&error))
    }
}
