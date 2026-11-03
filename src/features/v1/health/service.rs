use std::sync::LazyLock;
use std::time::Instant;

use chrono::{SecondsFormat, Utc};

use super::model::{AppHealthData, DbConnection, DbHealthData, DbStatus};

// Moment the process started, recorded on first use
static STARTED_AT: LazyLock<Instant> = LazyLock::new(Instant::now);

// Forces the start time to be recorded at boot instead of at the first request
pub fn mark_started() {
    LazyLock::force(&STARTED_AT);
}

// Builds the application health data from process state
pub fn get_app_health() -> AppHealthData {
    AppHealthData {
        status: "UP".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_ms: STARTED_AT.elapsed().as_millis() as u64,
        timestamp_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    }
}

// Placeholder until Phase 2 adds the database, it always reports the database as down
pub async fn get_db_health() -> DbHealthData {
    DbHealthData {
        status: DbStatus::Down,
        database: DbConnection::Disconnected,
        latency_ms: None,
    }
}
