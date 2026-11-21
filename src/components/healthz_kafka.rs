use dioxus::prelude::*;

use crate::components::health_parts::{
    KafkaStatusValue, Row, format_ago, format_bytes, format_latency,
};
use crate::components::layout::Layout;
use crate::kafka::{
    activity::ProducerSnapshot,
    health::{BrokerInfo, KafkaHealthFull, LogDirInfo, TopicHealth},
};

#[derive(Props, Clone, PartialEq)]
pub struct HealthzKafkaProps {
    pub title: &'static str,
    pub data: KafkaHealthFull,
}

// One broker as host and port, with a mark when it is fenced
fn broker_line(broker: &BrokerInfo) -> String {
    let fenced = if broker.fenced { " (fenced)" } else { "" };

    format!("{}:{}{fenced}", broker.host, broker.port)
}

// The last publish of the gateway producer
fn producer_line(producer: &ProducerSnapshot) -> String {
    let result = match producer.last_publish_ok {
        Some(true) => " (ok)",
        Some(false) => " (failed)",
        None => "",
    };

    format!("{}{result}", format_ago(producer.last_publish_ago_ms))
}

// Partition counts of one topic
fn topic_line(topic: &TopicHealth) -> String {
    format!(
        "{} partitions, {} under-replicated, {} leaderless",
        topic.partitions, topic.under_replicated_partitions, topic.leaderless_partitions
    )
}

// Free space and replica size of one log directory
fn disk_line(dir: &LogDirInfo) -> String {
    if dir.failed {
        return "failed".to_string();
    }

    let space = match (dir.usable_bytes, dir.total_bytes) {
        (Some(usable), Some(total)) => {
            format!("{} free of {}, ", format_bytes(usable), format_bytes(total))
        }
        _ => String::new(),
    };

    format!("{space}replicas {}", format_bytes(dir.replica_bytes))
}

// Full Kafka health page rendered inside the shared Layout
#[component]
pub fn HealthzKafka(props: HealthzKafkaProps) -> Element {
    let data = &props.data;
    let gateway = &data.gateway;
    let running = if gateway.consumer.running {
        "yes"
    } else {
        "no"
    };
    let last_error = gateway
        .consumer
        .last_error
        .clone()
        .unwrap_or_else(|| "none".to_string());

    rsx! {
        Layout { title: props.title,
            div { class: "max-w-(--width-health) mx-auto mt-8 space-y-6",
                h1 { class: "text-2xl font-bold text-text-primary", "Kafka Health" }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Broker" }
                    Row { label: "Status",
                        KafkaStatusValue { status: data.status }
                    }
                    Row { label: "Latency (ms)", "{format_latency(data.latency_ms)}" }
                    Row { label: "Checked at (UTC)", "{data.checked_at}" }
                    if let Some(error) = &data.error {
                        Row { label: "Error", "{error}" }
                    }
                }

                if let Some(cluster) = &data.cluster {
                    div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                        h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Cluster" }
                        Row { label: "Cluster id", "{cluster.cluster_id}" }
                        Row { label: "Controller", "{cluster.controller_id}" }
                        for broker in cluster.brokers.iter() {
                            Row { label: format!("Broker {}", broker.id), "{broker_line(broker)}" }
                        }
                    }
                }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Gateway" }
                    Row { label: "Producer last publish", "{producer_line(&gateway.producer)}" }
                    Row { label: "Consumer running", "{running}" }
                    Row { label: "Consumer last poll", "{format_ago(gateway.consumer.last_poll_ago_ms)}" }
                    Row { label: "Consumer last error", "{last_error}" }
                    Row { label: "Consumed topics", "{gateway.consumed_topics}" }
                    Row { label: "Connected subscribers", "{gateway.connected_subscribers}" }
                }

                if let Some(topics) = &data.topics {
                    div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                        h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Topics" }
                        for topic in topics.iter() {
                            Row { label: topic.name.clone(), "{topic_line(topic)}" }
                        }
                    }
                }

                if let Some(group) = &data.consumer_group {
                    div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                        h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Consumer group" }
                        Row { label: "Group id", "{group.group_id}" }
                        Row { label: "State", "{group.state}" }
                        Row { label: "Members", "{group.members}" }
                    }
                }

                if let Some(disks) = &data.disks {
                    div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                        h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Disk" }
                        for broker in disks.iter() {
                            for dir in broker.log_dirs.iter() {
                                Row { label: format!("Broker {} {}", broker.broker_id, dir.path), "{disk_line(dir)}" }
                            }
                        }
                    }
                }

                if !data.warnings.is_empty() {
                    div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                        h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Warnings" }
                        for warning in data.warnings.iter() {
                            Row { label: "Warning", "{warning}" }
                        }
                    }
                }
            }
        }
    }
}
