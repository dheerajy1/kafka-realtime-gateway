use std::time::Duration;

use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

// How often the gateway pings a WebSocket client (D10)
pub const PING_INTERVAL: Duration = Duration::from_secs(30);

// What the keepalive wants the connection to do
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Beat {
    // Send a ping now
    Ping,

    // The previous ping was never answered, close the connection
    Dead,
}

// Pings a client on a fixed interval and reports a client that stops answering
// Any frame from the client counts as an answer, a client that is busy sending still gets its pings
pub struct Keepalive {
    interval: Interval,
    awaiting_answer: bool,
}

impl Keepalive {
    // The first ping is sent one period after the connection opens
    pub fn new(period: Duration) -> Self {
        let mut interval = interval_at(Instant::now() + period, period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            interval,
            awaiting_answer: false,
        }
    }

    // Waits for the next beat, it can be used inside a select
    pub async fn next_beat(&mut self) -> Beat {
        self.interval.tick().await;

        if self.awaiting_answer {
            return Beat::Dead;
        }

        self.awaiting_answer = true;

        Beat::Ping
    }

    // Records that the client answered, call it for every frame received from the client
    pub fn client_is_alive(&mut self) {
        self.awaiting_answer = false;
    }
}
