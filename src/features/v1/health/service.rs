use std::sync::LazyLock;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};

use super::model::{AppHealthData, DbConnection, DbHealthData, DbStatus};
use crate::db::store::DbProbe;

// Moment the process started, recorded on first use
static STARTED_AT: LazyLock<Instant> = LazyLock::new(Instant::now);

// Forces the start time to be recorded at boot instead of at the first request
pub fn mark_started() {
    LazyLock::force(&STARTED_AT);
}

// Milliseconds of a duration as u64, a duration beyond u64::MAX milliseconds is capped instead of wrapping
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

// Builds the application health data from process state
pub fn get_app_health() -> AppHealthData {
    AppHealthData {
        status: "UP",
        version: env!("CARGO_PKG_VERSION"),
        uptime_ms: millis(STARTED_AT.elapsed()),
        timestamp_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    }
}

// Probes the database and measures how long the probe takes
pub async fn get_db_health(probe: &dyn DbProbe) -> DbHealthData {
    let start = Instant::now();

    if probe.ping().await {
        DbHealthData {
            status: DbStatus::Up,
            database: DbConnection::Connected,
            latency_ms: Some(millis(start.elapsed())),
        }
    } else {
        DbHealthData {
            status: DbStatus::Down,
            database: DbConnection::Disconnected,
            latency_ms: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A normal duration gives its milliseconds
    #[test]
    fn millis_of_a_normal_duration() {
        assert_eq!(millis(Duration::from_millis(1_500)), 1_500);
        assert_eq!(millis(Duration::ZERO), 0);
    }

    // A duration beyond u64 milliseconds is capped
    #[test]
    fn millis_is_capped_at_u64_max() {
        assert_eq!(millis(Duration::MAX), u64::MAX);
    }
}
