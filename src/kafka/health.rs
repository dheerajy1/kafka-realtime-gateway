use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::Mutex;
use utoipa::ToSchema;

use super::activity::{ConsumerSnapshot, ProducerSnapshot};

// Overall Kafka status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum KafkaStatus {
    Up,
    Down,
}

// Small Kafka block of the unified health, always live (D48)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaHealthSummary {
    pub status: KafkaStatus,

    // Number of brokers in the cluster, None when the broker is down
    pub broker_count: Option<u32>,

    // Round trip of the cluster check in milliseconds, None when the broker is down
    pub latency_ms: Option<u64>,
}

// One broker of the cluster
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,

    // True while the controller has fenced the broker
    pub fenced: bool,
}

// The cluster as the broker describes it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: Vec<BrokerInfo>,
}

// What the gateway itself knows about its Kafka use (D53)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewayKafkaState {
    pub producer: ProducerSnapshot,
    pub consumer: ConsumerSnapshot,

    // Number of topics the registry marks as consumed
    pub consumed_topics: usize,

    // Number of WebSocket subscribers connected right now
    pub connected_subscribers: usize,
}

// Partition health of one topic of the registry
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicHealth {
    pub name: String,
    pub partitions: usize,

    // Partitions with fewer in-sync replicas than replicas
    pub under_replicated_partitions: usize,

    // Partitions without a leader
    pub leaderless_partitions: usize,
}

// State of the consumer group of the gateway
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupHealth {
    pub group_id: String,
    pub state: String,
    pub members: usize,
}

// One log directory of a broker
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LogDirInfo {
    pub path: String,

    // Size of the filesystem, None when the broker does not report it
    pub total_bytes: Option<i64>,

    // Free space of the filesystem, None when the broker does not report it
    pub usable_bytes: Option<i64>,

    // Total size of the replica logs in this directory
    pub replica_bytes: i64,

    // True when the broker reports an error for this directory
    pub failed: bool,
}

// Disk usage of one broker
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerDisk {
    pub broker_id: i32,
    pub log_dirs: Vec<LogDirInfo>,
}

// The full Kafka health result of GET /api/v1/health/kafka (D50)
// A section that could not be read is None and named in warnings, the status stays UP
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaHealthFull {
    pub status: KafkaStatus,

    // Round trip of the cluster check in milliseconds, None when the broker is down
    pub latency_ms: Option<u64>,

    // When the check ran, a cached result keeps its original time (D51)
    pub checked_at: String,

    // Why the broker counts as down, None while it is up
    pub error: Option<String>,

    pub cluster: Option<ClusterInfo>,
    pub gateway: GatewayKafkaState,
    pub topics: Option<Vec<TopicHealth>>,
    pub consumer_group: Option<GroupHealth>,
    pub disks: Option<Vec<BrokerDisk>>,
    pub warnings: Vec<String>,
}

// Checks the Kafka broker, the real implementation uses the admin client and tests use a fake
#[async_trait]
pub trait KafkaProbe: Send + Sync {
    // The small live check, one cluster call
    async fn summary(&self) -> KafkaHealthSummary;

    // The full check
    async fn full(&self) -> KafkaHealthFull;
}

// Keeps the full result for a short time so a polling client does not load the broker (D51)
pub struct CachedKafkaProbe {
    inner: Arc<dyn KafkaProbe>,
    ttl: Duration,

    // The lock is held during a refresh, so requests that arrive meanwhile wait and share the new result
    cached: Mutex<Option<(Instant, KafkaHealthFull)>>,
}

impl CachedKafkaProbe {
    pub fn new(inner: Arc<dyn KafkaProbe>, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            cached: Mutex::new(None),
        }
    }
}

#[async_trait]
impl KafkaProbe for CachedKafkaProbe {
    // The summary is never cached
    async fn summary(&self) -> KafkaHealthSummary {
        self.inner.summary().await
    }

    async fn full(&self) -> KafkaHealthFull {
        let mut cached = self.cached.lock().await;

        let fresh = cached
            .as_ref()
            .filter(|(at, _)| at.elapsed() < self.ttl)
            .map(|(_, value)| value.clone());

        if let Some(value) = fresh {
            return value;
        }

        let value = self.inner.full().await;
        *cached = Some((Instant::now(), value.clone()));

        value
    }
}
