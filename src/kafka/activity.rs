use serde::Serialize;
use std::{
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use utoipa::ToSchema;

// Longest consumer error text kept for the health result
const MAX_ERROR_CHARS: usize = 200;

// A running consumer with no good round for this long counts as stalled
const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(30);

// Current Unix time in milliseconds
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// Milliseconds since a stored time, None when the event never happened (0 means never)
fn ago_ms(stored: &AtomicU64) -> Option<u64> {
    match stored.load(Ordering::Relaxed) {
        0 => None,
        at => Some(now_ms().saturating_sub(at)),
    }
}

// What the producer did last, written after every publish
#[derive(Default)]
pub struct ProducerActivity {
    last_publish_ms: AtomicU64,
    last_publish_ok: AtomicBool,
}

// A copy of the producer activity for the health result
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProducerSnapshot {
    // Milliseconds since the last publish, None when nothing was published yet
    pub last_publish_ago_ms: Option<u64>,

    // Whether the last publish succeeded, None when nothing was published yet
    pub last_publish_ok: Option<bool>,
}

impl ProducerActivity {
    // Records the result of one publish
    pub fn record(&self, ok: bool) {
        self.last_publish_ok.store(ok, Ordering::Relaxed);
        self.last_publish_ms.store(now_ms(), Ordering::Relaxed);
    }

    // Returns what the producer did last
    pub fn snapshot(&self) -> ProducerSnapshot {
        let ago = ago_ms(&self.last_publish_ms);

        ProducerSnapshot {
            last_publish_ago_ms: ago,
            last_publish_ok: ago.map(|_| self.last_publish_ok.load(Ordering::Relaxed)),
        }
    }
}

// What the consumer loop did last, written by the loop
pub struct ConsumerActivity {
    running: AtomicBool,
    last_ok_ms: AtomicU64,
    last_error: Mutex<Option<String>>,

    // How long without a good round counts as stalled, in milliseconds
    stall_after_ms: u64,
}

impl Default for ConsumerActivity {
    fn default() -> Self {
        Self::with_stall_after(DEFAULT_STALL_AFTER)
    }
}

// A copy of the consumer activity for the health result
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSnapshot {
    // Whether the consumer loop is running
    pub running: bool,

    // True while the loop runs but has had no good round for 30 seconds
    pub stalled: bool,

    // Milliseconds since the last round that worked, None before the first one
    pub last_poll_ago_ms: Option<u64>,

    // The error of the last failed poll, None when the last round worked
    pub last_error: Option<String>,
}

impl ConsumerActivity {
    // Creates the activity with a custom stall time, the default is 30 seconds
    pub fn with_stall_after(stall_after: Duration) -> Self {
        Self {
            running: AtomicBool::new(false),
            last_ok_ms: AtomicU64::new(0),
            last_error: Mutex::new(None),
            stall_after_ms: u64::try_from(stall_after.as_millis()).unwrap_or(u64::MAX),
        }
    }

    fn error_slot(&self) -> MutexGuard<'_, Option<String>> {
        self.last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    // Marks the loop as started or stopped
    pub fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::Relaxed);
    }

    // Records a round that worked, a poll or an idle wait with nothing to subscribe to, and clears the error
    pub fn record_ok(&self) {
        self.last_ok_ms.store(now_ms(), Ordering::Relaxed);
        *self.error_slot() = None;
    }

    // Records the error of a failed poll, shortened to a fixed length
    pub fn record_error(&self, text: &str) {
        *self.error_slot() = Some(text.chars().take(MAX_ERROR_CHARS).collect());
    }

    // Returns what the consumer loop did last
    pub fn snapshot(&self) -> ConsumerSnapshot {
        let running = self.running.load(Ordering::Relaxed);
        let last_poll_ago_ms = ago_ms(&self.last_ok_ms);

        ConsumerSnapshot {
            running,
            stalled: running && last_poll_ago_ms.is_some_and(|ago| ago > self.stall_after_ms),
            last_poll_ago_ms,
            last_error: self.error_slot().clone(),
        }
    }
}
