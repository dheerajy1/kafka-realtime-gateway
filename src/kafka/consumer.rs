use std::{
    collections::BTreeSet,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use async_trait::async_trait;
use kafkit_client::{
    AutoOffsetReset, CommitOffset, ConsumerConfig, ConsumerRecord, KafkaConsumer, TopicPartition,
};
use tokio::sync::{RwLock, watch};

use super::{
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
}

impl BridgeConsumer {
    // Connects to the brokers, offsets are committed by the bridge and never automatically
    pub async fn connect(
        brokers: Vec<String>,
        client_id: &str,
        group_id: &str,
    ) -> kafkit_client::Result<Self> {
        // The constructor needs one address, the real list replaces it right after
        let config = ConsumerConfig::new(String::new(), group_id)
            .with_bootstrap_servers(brokers)
            .with_client_id(client_id)
            // A new group starts at the end of each topic, like the old kafkajs consumer
            .with_auto_offset_reset(AutoOffsetReset::Latest)
            .with_partition_max_bytes(MAX_PARTITION_BYTES);

        Ok(Self {
            consumer: RwLock::new(Some(KafkaConsumer::connect(config).await?)),
            state: Mutex::new(State::default()),
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

        while !*shutdown.borrow() {
            if self.state().subscribed.is_empty() {
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
                    records
                }
                Err(error) => {
                    let text = error.to_string();
                    if text != last_error {
                        tracing::error!(
                            target: "WsBridge",
                            "CONSUMER_POLL_FAILED {text}, retrying every {}s",
                            RETRY_DELAY.as_secs()
                        );
                        last_error = text;
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
