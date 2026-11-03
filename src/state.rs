use std::sync::Arc;

use crate::db::{
    postgres::PgStore,
    store::{ApiKeyStore, DbProbe, UserStore},
};

// Shared state every handler can read, cloning it only copies the Arc pointers
#[derive(Clone)]
pub struct AppState {
    // Lookups on the portal and API user tables
    pub users: Arc<dyn UserStore>,

    // The API key verify and create functions
    pub api_keys: Arc<dyn ApiKeyStore>,

    // Database health check
    pub db_probe: Arc<dyn DbProbe>,
}

impl AppState {
    // Builds the state from any implementations, tests pass in-memory fakes here
    pub fn new(
        users: Arc<dyn UserStore>,
        api_keys: Arc<dyn ApiKeyStore>,
        db_probe: Arc<dyn DbProbe>,
    ) -> Self {
        Self {
            users,
            api_keys,
            db_probe,
        }
    }

    // Builds the state from the real database, one store serves all three roles
    pub fn from_pg(store: PgStore) -> Self {
        let store = Arc::new(store);

        Self {
            users: store.clone(),
            api_keys: store.clone(),
            db_probe: store,
        }
    }
}
