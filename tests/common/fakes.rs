use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;
use kafka_api_gateway::{
    db::store::{ApiKeyRecord, ApiKeyStore, ApiUserRecord, DbProbe, UserStore},
    errors::{
        api_error::ApiError,
        catalog::{conflict, internal_server_error, unauthorized},
    },
};

// bcrypt cost used by the fakes, the lowest allowed so the tests stay fast
const FAKE_BCRYPT_COST: u32 = 4;

// In-memory stand-in for the database, it implements all three store traits
#[derive(Default)]
pub struct FakeStore {
    // Answer of the health probe
    db_up: bool,

    // When true every user lookup fails with a database error
    broken: bool,

    // Active portal users, user name to password hash
    portal_users: HashMap<String, String>,

    // Active API users by user name
    api_users: HashMap<String, ApiUserRecord>,

    // API keys by key id, behind a mutex because create_api_key adds to it
    api_keys: Mutex<HashMap<String, ApiKeyRecord>>,
}

impl FakeStore {
    pub fn new() -> Self {
        Self::default()
    }

    // The health probe reports the database as up
    pub fn with_db_up(mut self) -> Self {
        self.db_up = true;
        self
    }

    // Every user lookup returns a database error
    pub fn broken(mut self) -> Self {
        self.broken = true;
        self
    }

    // Adds an active portal user with the given plain password
    pub fn with_portal_user(mut self, username: &str, password: &str) -> Self {
        let hash = bcrypt::hash(password, FAKE_BCRYPT_COST).unwrap();
        self.portal_users.insert(username.to_string(), hash);
        self
    }

    // Adds an active API user with the given plain password and role
    pub fn with_api_user(mut self, username: &str, password: &str, role: &str) -> Self {
        let hash = bcrypt::hash(password, FAKE_BCRYPT_COST).unwrap();
        self.api_users.insert(
            username.to_string(),
            ApiUserRecord {
                user_name: username.to_string(),
                password_hash: hash,
                user_role: role.to_string(),
            },
        );
        self
    }

    // Adds an active API key with the given plain secret
    pub fn with_api_key(self, api_key_id: &str, secret: &str) -> Self {
        let hash = bcrypt::hash(secret, FAKE_BCRYPT_COST).unwrap();
        self.api_keys.lock().unwrap().insert(
            api_key_id.to_string(),
            ApiKeyRecord {
                api_key_id: api_key_id.to_string(),
                api_user_id: 1,
                api_key_secret_hash: hash,
            },
        );
        self
    }

    // Key ids currently stored, used to check what create_api_key added
    pub fn api_key_ids(&self) -> Vec<String> {
        self.api_keys.lock().unwrap().keys().cloned().collect()
    }

    // The error every lookup returns when the store is broken
    fn broken_error() -> ApiError {
        ApiError::from_entry(internal_server_error::DATABASE_ERROR)
    }
}

#[async_trait]
impl UserStore for FakeStore {
    async fn find_portal_user_hash(&self, username: &str) -> Result<Option<String>, ApiError> {
        if self.broken {
            return Err(Self::broken_error());
        }

        Ok(self.portal_users.get(username).cloned())
    }

    async fn find_api_user(&self, username: &str) -> Result<Option<ApiUserRecord>, ApiError> {
        if self.broken {
            return Err(Self::broken_error());
        }

        Ok(self.api_users.get(username).cloned())
    }
}

#[async_trait]
impl ApiKeyStore for FakeStore {
    async fn verify_api_key(&self, api_key_id: &str) -> Result<ApiKeyRecord, ApiError> {
        self.api_keys
            .lock()
            .unwrap()
            .get(api_key_id)
            .cloned()
            .ok_or_else(|| ApiError::from_entry(unauthorized::INVALID_API_KEY))
    }

    async fn create_api_key(
        &self,
        username: &str,
        api_key_id: &str,
        api_key_secret_hash: &str,
    ) -> Result<(), ApiError> {
        let mut keys = self.api_keys.lock().unwrap();

        // An unknown user or a duplicate key id is a conflict, like the real function
        if !self.api_users.contains_key(username) || keys.contains_key(api_key_id) {
            return Err(ApiError::from_entry(conflict::DATABASE_CONFLICT));
        }

        keys.insert(
            api_key_id.to_string(),
            ApiKeyRecord {
                api_key_id: api_key_id.to_string(),
                api_user_id: 1,
                api_key_secret_hash: api_key_secret_hash.to_string(),
            },
        );

        Ok(())
    }
}

#[async_trait]
impl DbProbe for FakeStore {
    async fn ping(&self) -> bool {
        self.db_up
    }
}
