use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::{
    db::{
        postgres::PgStore,
        store::{ApiKeyStore, DbProbe, UserStore},
    },
    kafka::{
        publisher::Publisher, registry::TopicRegistry, reloader::TopicReload,
        subscribers::SubscriberRegistry,
    },
};

// Everything the Kafka side of the gateway shares with the handlers
#[derive(Clone)]
pub struct KafkaDeps {
    // The enabled topics and their flags
    pub topics: Arc<TopicRegistry>,

    // Sends messages to Kafka
    pub publisher: Arc<dyn Publisher>,

    // Reloads the topic policy on demand
    pub reloader: Arc<dyn TopicReload>,

    // The connected WebSocket subscribers
    pub subscribers: Arc<SubscriberRegistry>,

    // Turns true when the gateway shuts down
    pub shutdown: watch::Receiver<bool>,

    // Every WebSocket task keeps a clone of this sender while it runs, so shutdown can wait until all of them are done
    pub ws_drain: mpsc::Sender<()>,
}

// Shared state every handler can read, cloning it only copies the Arc pointers
#[derive(Clone)]
pub struct AppState {
    // Lookups on the portal and API user tables
    pub users: Arc<dyn UserStore>,

    // The API key verify and create functions
    pub api_keys: Arc<dyn ApiKeyStore>,

    // Database health check
    pub db_probe: Arc<dyn DbProbe>,

    // The enabled topics and their flags
    pub topics: Arc<TopicRegistry>,

    // Sends messages to Kafka
    pub publisher: Arc<dyn Publisher>,

    // Reloads the topic policy on demand
    pub reloader: Arc<dyn TopicReload>,

    // The connected WebSocket subscribers
    pub subscribers: Arc<SubscriberRegistry>,

    // Turns true when the gateway shuts down
    pub shutdown: watch::Receiver<bool>,

    // Held for as long as a WebSocket task runs, never used to send
    pub ws_drain: mpsc::Sender<()>,
}

impl AppState {
    // Builds the state from any implementations, tests pass in-memory fakes here
    pub fn new(
        users: Arc<dyn UserStore>,
        api_keys: Arc<dyn ApiKeyStore>,
        db_probe: Arc<dyn DbProbe>,
        kafka: KafkaDeps,
    ) -> Self {
        Self {
            users,
            api_keys,
            db_probe,
            topics: kafka.topics,
            publisher: kafka.publisher,
            reloader: kafka.reloader,
            subscribers: kafka.subscribers,
            shutdown: kafka.shutdown,
            ws_drain: kafka.ws_drain,
        }
    }

    // Builds the state from the real database, one store serves all three database roles
    pub fn from_pg(store: PgStore, kafka: KafkaDeps) -> Self {
        let store = Arc::new(store);

        Self::new(store.clone(), store.clone(), store, kafka)
    }
}
