use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Mutex, MutexGuard, PoisonError},
};

use tokio::sync::{mpsc, oneshot};

// Frames a subscriber may have waiting before the gateway closes its connection
pub const QUEUE_CAPACITY: usize = 1000;

// Why an ACK from a subscriber was rejected
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckRejection {
    // No pending ACK with that correlation id, it was never registered or is already done
    NotFound,

    // The ACK came from a connection that does not own the pending message
    WrongOwner,
}

impl AckRejection {
    // The reason text sent back to the client, same wording as the old gateway
    pub fn message(self) -> &'static str {
        match self {
            Self::NotFound => "offset not found or already committed",
            Self::WrongOwner => "ACK from non-owning connection",
        }
    }
}

// The message an accepted ACK belongs to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckedMessage {
    // Topic of the acknowledged message
    pub topic: String,

    // Partition of the acknowledged message
    pub partition: i32,

    // Offset of the acknowledged message
    pub offset: i64,
}

// What one fan-out did
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeliveryReport {
    // How many subscribers received the frame
    pub delivered: usize,

    // Subscribers removed because their queue was full or their connection was already gone
    pub removed: Vec<u64>,
}

// One connected subscriber
struct Session {
    // API key the connection authenticated with, for logs
    api_key_id: String,

    // Topics the subscriber asked for
    subscriptions: HashSet<String>,

    // Queue of frames the connection task writes to the socket
    tx: mpsc::Sender<String>,
}

// One message waiting for the ACK of its owner
struct PendingAck {
    // Subscriber that has to acknowledge
    owner_id: u64,

    // Where the message came from
    message: AckedMessage,

    // Completed with () on a valid ACK, dropped without a value when the wait fails
    tx: oneshot::Sender<()>,
}

struct Inner {
    // The next subscriber id, it only ever increases (D7)
    next_id: u64,

    // Connected subscribers by id, sorted so the lowest id comes first
    sessions: BTreeMap<u64, Session>,

    // Pending ACKs by correlation id
    pending: HashMap<String, PendingAck>,
}

// Removes a subscriber and fails the pending ACKs it owned, returns how many were failed
fn remove_session(inner: &mut Inner, id: u64) -> usize {
    inner.sessions.remove(&id);

    let before = inner.pending.len();
    inner.pending.retain(|_, pending| pending.owner_id != id);

    before - inner.pending.len()
}

// The connected subscribers and the messages waiting for an ACK
pub struct SubscriberRegistry {
    inner: Mutex<Inner>,
}

impl Default for SubscriberRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next_id: 1,
                sessions: BTreeMap::new(),
                pending: HashMap::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Adds a connection, returns its id and the queue its task has to drain
    pub fn register(&self, api_key_id: &str) -> (u64, mpsc::Receiver<String>) {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let mut inner = self.lock();

        let id = inner.next_id;
        inner.next_id += 1;

        inner.sessions.insert(
            id,
            Session {
                api_key_id: api_key_id.to_string(),
                subscriptions: HashSet::new(),
                tx,
            },
        );

        (id, rx)
    }

    // Removes a connection and fails the pending ACKs it owned, returns how many were failed
    pub fn unregister(&self, id: u64) -> usize {
        remove_session(&mut self.lock(), id)
    }

    // The API key id of a connected subscriber
    pub fn api_key_id(&self, id: u64) -> Option<String> {
        self.lock()
            .sessions
            .get(&id)
            .map(|session| session.api_key_id.clone())
    }

    // Subscribes a connection to a topic, false when the connection is unknown
    pub fn subscribe(&self, id: u64, topic: &str) -> bool {
        match self.lock().sessions.get_mut(&id) {
            Some(session) => {
                session.subscriptions.insert(topic.to_string());
                true
            }
            None => false,
        }
    }

    // Unsubscribes a connection from a topic, false when the connection is unknown
    pub fn unsubscribe(&self, id: u64, topic: &str) -> bool {
        match self.lock().sessions.get_mut(&id) {
            Some(session) => {
                session.subscriptions.remove(topic);
                true
            }
            None => false,
        }
    }

    // How many connections are registered
    pub fn session_count(&self) -> usize {
        self.lock().sessions.len()
    }

    // The subscriber that has to acknowledge a topic, the lowest id among its subscribers
    pub fn pick_owner(&self, topic: &str) -> Option<u64> {
        self.lock()
            .sessions
            .iter()
            .find(|(_, session)| session.subscriptions.contains(topic))
            .map(|(id, _)| *id)
    }

    // Queues the frame for every subscriber of the topic
    // A subscriber whose queue is full or whose connection is gone is removed, its task then closes the socket
    pub fn deliver(&self, topic: &str, frame: &str) -> DeliveryReport {
        let mut inner = self.lock();
        let mut report = DeliveryReport::default();

        for (id, session) in inner.sessions.iter() {
            if !session.subscriptions.contains(topic) {
                continue;
            }

            match session.tx.try_send(frame.to_string()) {
                Ok(()) => report.delivered += 1,
                Err(_) => report.removed.push(*id),
            }
        }

        for id in &report.removed {
            remove_session(&mut inner, *id);
        }

        report
    }

    // Registers a message that waits for the ACK of its owner, a second registration of the same correlation id supersedes the first
    // The receiver gets () on a valid ACK and an error when the wait fails
    pub fn register_pending(
        &self,
        correlation_id: &str,
        owner_id: u64,
        message: AckedMessage,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();

        self.lock().pending.insert(
            correlation_id.to_string(),
            PendingAck {
                owner_id,
                message,
                tx,
            },
        );

        rx
    }

    // Forgets a pending ACK, used when the wait timed out
    pub fn cancel_pending(&self, correlation_id: &str) {
        self.lock().pending.remove(correlation_id);
    }

    // Accepts the ACK of a subscriber, only the owner of the pending message may acknowledge it
    pub fn acknowledge(
        &self,
        correlation_id: &str,
        from_id: u64,
    ) -> Result<AckedMessage, AckRejection> {
        let mut inner = self.lock();

        let Some(pending) = inner.pending.get(correlation_id) else {
            return Err(AckRejection::NotFound);
        };

        if pending.owner_id != from_id {
            return Err(AckRejection::WrongOwner);
        }

        let Some(pending) = inner.pending.remove(correlation_id) else {
            return Err(AckRejection::NotFound);
        };

        // The waiter may already be gone, the ACK is still valid then
        let _ = pending.tx.send(());

        Ok(pending.message)
    }
}
