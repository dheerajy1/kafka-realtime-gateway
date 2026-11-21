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
    pub title: &'static str,
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
        Layout { title: props.title,
            div { class: "max-w-(--width-health) mx-auto mt-8 space-y-6",
                h1 { class: "text-2xl font-bold text-text-primary", "Health Status" }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Application" }
                    Row { label: "Status",
                        CheckCircle { class: "text-success" }
                        "{app.status}"
                    }
                    Row { label: "Version", "{app.version}" }
                    Row { label: "Uptime (ms)", "{format_number(app.uptime_ms)}" }
                    Row { label: "Timestamp (UTC)", "{app.timestamp_utc}" }
                }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Database" }
                    Row { label: "Status",
                        DbStatusValue { status: db.status }
                    }
                    Row { label: "Connection",
                        DbConnectionValue { connection: db.database }
                    }
                    Row { label: "Latency (ms)", "{format_latency(db.latency_ms)}" }
                }

                div { class: "bg-surface border border-border-thick p-4 md:p-5 rounded-lg",
                    h2 { class: "text-lg font-semibold mb-3 text-text-primary", "Kafka" }
                    Row { label: "Status",
                        KafkaStatusValue { status: kafka.status }
                    }
                    Row { label: "Brokers", "{format_latency(kafka.broker_count.map(u64::from))}" }
                    Row { label: "Latency (ms)", "{format_latency(kafka.latency_ms)}" }
                }
            }
        }
    }
}
