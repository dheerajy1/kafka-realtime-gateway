pub mod activity;
pub mod admin;
pub mod bridge;
pub mod consumer;
pub mod error;
pub mod health;
pub mod health_probe;
pub mod inbound;
pub mod producer;
pub mod publisher;
pub mod registry;
pub mod reloader;
pub mod subscribers;
pub mod sync;

// Splits the KAFKA_BROKERS value on commas and trims each address, empty parts are dropped (D16)
pub fn parse_brokers(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(str::to_owned)
        .collect()
}
