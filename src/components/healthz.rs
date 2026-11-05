use dioxus::prelude::*;

use crate::components::health_parts::{
    DbConnectionValue, DbStatusValue, KafkaStatusValue, Row, format_latency, format_number,
};
use crate::components::icons::CheckCircle;
use crate::components::layout::Layout;
use crate::features::v1::health::model::{AppHealthData, DbHealthData};
use crate::kafka::health::KafkaHealthSummary;

#[derive(Props, Clone, PartialEq)]
pub struct HealthzProps {
    pub title: String,
    pub app: AppHealthData,
    pub db: DbHealthData,
    pub kafka: KafkaHealthSummary,
}

// Combined app and database health page rendered inside the shared Layout
#[component]
pub fn Healthz(props: HealthzProps) -> Element {
    let app = &props.app;
    let db = &props.db;
    let kafka = &props.kafka;

    rsx! {
        Layout { title: props.title.clone(),
            div { class: "max-w-(--width-health) mx-auto mt-8 space-y-6",
                h1 { class: "text-2xl font-bold text-text-primary", "Health Status" }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Application" }
                    Row { label: "Status".to_string(),
                        CheckCircle { class: "text-success" }
                        "{app.status}"
                    }
                    Row { label: "Version".to_string(), "{app.version}" }
                    Row { label: "Uptime (ms)".to_string(), "{format_number(app.uptime_ms)}" }
                    Row { label: "Timestamp (UTC)".to_string(), "{app.timestamp_utc}" }
                }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Database" }
                    Row { label: "Status".to_string(),
                        DbStatusValue { status: db.status }
                    }
                    Row { label: "Connection".to_string(),
                        DbConnectionValue { connection: db.database }
                    }
                    Row { label: "Latency (ms)".to_string(), "{format_latency(db.latency_ms)}" }
                }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Kafka" }
                    Row { label: "Status".to_string(),
                        KafkaStatusValue { status: kafka.status }
                    }
                    Row { label: "Brokers".to_string(), "{format_latency(kafka.broker_count.map(u64::from))}" }
                    Row { label: "Latency (ms)".to_string(), "{format_latency(kafka.latency_ms)}" }
                }
            }
        }
    }
}
