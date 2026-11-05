use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use kafkit_client::{BrokerLogDirs, ClusterDescription, KafkaAdmin, TopicDescription};

use super::{
    activity::{ConsumerActivity, ProducerActivity},
    health::{
        BrokerDisk, BrokerInfo, ClusterInfo, GatewayKafkaState, GroupHealth, KafkaHealthFull,
        KafkaHealthSummary, KafkaProbe, KafkaStatus, LogDirInfo, TopicHealth,
    },
    registry::TopicRegistry,
    subscribers::SubscriberRegistry,
};

// How long each broker call of a probe may take, a call that is slower counts as failed (D52)
const CALL_TIMEOUT: Duration = Duration::from_secs(3);

// The real Kafka probe, it reuses the admin client created at boot
pub struct KafkaHealthProbe {
    admin: Arc<KafkaAdmin>,
    group_id: String,
    topics: Arc<TopicRegistry>,
    subscribers: Arc<SubscriberRegistry>,
    producer: Arc<ProducerActivity>,
    consumer: Arc<ConsumerActivity>,
}

impl KafkaHealthProbe {
    pub fn new(
        admin: Arc<KafkaAdmin>,
        group_id: &str,
        topics: Arc<TopicRegistry>,
        subscribers: Arc<SubscriberRegistry>,
        producer: Arc<ProducerActivity>,
        consumer: Arc<ConsumerActivity>,
    ) -> Self {
        Self {
            admin,
            group_id: group_id.to_owned(),
            topics,
            subscribers,
            producer,
            consumer,
        }
    }

    // Describes the cluster and measures the round trip in milliseconds
    async fn cluster(&self) -> (Result<ClusterDescription, String>, u64) {
        let start = Instant::now();
        let result = limited(self.admin.describe_cluster()).await;
        let latency = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        (result, latency)
    }

    // What the gateway knows about its own Kafka use, it needs no broker call
    fn gateway_state(&self) -> GatewayKafkaState {
        GatewayKafkaState {
            producer: self.producer.snapshot(),
            consumer: self.consumer.snapshot(),
            consumed_topics: self.topics.snapshot().consume_topics().len(),
            connected_subscribers: self.subscribers.session_count(),
        }
    }
}

#[async_trait]
impl KafkaProbe for KafkaHealthProbe {
    async fn summary(&self) -> KafkaHealthSummary {
        match self.cluster().await {
            (Ok(cluster), latency) => KafkaHealthSummary {
                status: KafkaStatus::Up,
                broker_count: Some(u32::try_from(cluster.brokers.len()).unwrap_or(u32::MAX)),
                latency_ms: Some(latency),
            },
            (Err(_), _) => KafkaHealthSummary {
                status: KafkaStatus::Down,
                broker_count: None,
                latency_ms: None,
            },
        }
    }

    async fn full(&self) -> KafkaHealthFull {
        let enabled = self.topics.snapshot().enabled_topics();
        let gateway = self.gateway_state();
        let checked_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        let (result, latency) = self.cluster().await;

        // Without the cluster call nothing else can be read, the broker counts as down
        let cluster = match result {
            Ok(cluster) => cluster,
            Err(error) => {
                return KafkaHealthFull {
                    status: KafkaStatus::Down,
                    latency_ms: None,
                    checked_at,
                    error: Some(error),
                    cluster: None,
                    gateway,
                    topics: None,
                    consumer_group: None,
                    disks: None,
                    warnings: Vec::new(),
                };
            }
        };

        // The three detail calls run together, each has its own timeout
        let (topics, groups, disks) = tokio::join!(
            limited(self.admin.describe_topics(enabled)),
            limited(self.admin.describe_consumer_groups([self.group_id.clone()])),
            limited(self.admin.describe_log_dirs(cluster.brokers.clone(), None)),
        );

        // A detail call that fails leaves its section empty and adds a warning, the broker is still up
        let mut warnings = Vec::new();

        let topics = match topics {
            Ok(list) => Some(topic_health(list)),
            Err(error) => {
                warnings.push(format!("topics: {error}"));
                None
            }
        };

        let consumer_group = match groups {
            Ok(list) => {
                let found = list
                    .into_iter()
                    .find(|group| group.group_id == self.group_id)
                    .map(|group| GroupHealth {
                        group_id: group.group_id,
                        state: group.state,
                        members: group.members.len(),
                    });

                if found.is_none() {
                    warnings.push(format!("consumerGroup: group {} not found", self.group_id));
                }

                found
            }
            Err(error) => {
                warnings.push(format!("consumerGroup: {error}"));
                None
            }
        };

        let disks = match disks {
            Ok(list) => Some(broker_disks(list)),
            Err(error) => {
                warnings.push(format!("disks: {error}"));
                None
            }
        };

        KafkaHealthFull {
            status: KafkaStatus::Up,
            latency_ms: Some(latency),
            checked_at,
            error: None,
            cluster: Some(cluster_info(&cluster)),
            gateway,
            topics,
            consumer_group,
            disks,
            warnings,
        }
    }
}

// Runs one broker call with the probe timeout, a failure or a timeout is returned as text
async fn limited<T>(call: impl Future<Output = kafkit_client::Result<T>>) -> Result<T, String> {
    match tokio::time::timeout(CALL_TIMEOUT, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("timed out after {} s", CALL_TIMEOUT.as_secs())),
    }
}

// Copies the cluster description into the health type
fn cluster_info(cluster: &ClusterDescription) -> ClusterInfo {
    ClusterInfo {
        cluster_id: cluster.cluster_id.clone(),
        controller_id: cluster.controller_id,
        brokers: cluster
            .brokers
            .iter()
            .map(|broker| BrokerInfo {
                id: broker.broker_id,
                host: broker.host.clone(),
                port: broker.port,
                rack: broker.rack.clone(),
                fenced: broker.is_fenced,
            })
            .collect(),
    }
}

// Counts the partitions of each topic that are under-replicated or have no leader
fn topic_health(topics: Vec<TopicDescription>) -> Vec<TopicHealth> {
    topics
        .into_iter()
        .map(|topic| TopicHealth {
            under_replicated_partitions: topic
                .partitions
                .iter()
                .filter(|partition| partition.isr_nodes.len() < partition.replica_nodes.len())
                .count(),
            leaderless_partitions: topic
                .partitions
                .iter()
                .filter(|partition| partition.leader_id < 0)
                .count(),
            partitions: topic.partitions.len(),
            name: topic.name,
        })
        .collect()
}

// Copies the log directory usage of each broker into the health type
fn broker_disks(brokers: Vec<BrokerLogDirs>) -> Vec<BrokerDisk> {
    brokers
        .into_iter()
        .map(|broker| BrokerDisk {
            broker_id: broker.broker_id,
            log_dirs: broker
                .log_dirs
                .into_iter()
                .map(|dir| LogDirInfo {
                    replica_bytes: dir
                        .replicas
                        .iter()
                        .map(|replica| replica.size_bytes)
                        .sum::<i64>(),
                    failed: dir.error_code != 0,
                    path: dir.log_dir,
                    total_bytes: dir.total_bytes,
                    usable_bytes: dir.usable_bytes,
                })
                .collect(),
        })
        .collect()
}
