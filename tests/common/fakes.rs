use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;
use kafka_api_gateway::kafka::{
    admin::TopicLister,
    publisher::{PublishReceipt, PublishRecord, Publisher},
    reloader::{ReloadSummary, TopicReload},
};
use kafka_api_gateway::{
    db::store::{
        ApiKeyRecord, ApiKeyStore, ApiUserRecord, DbProbe, TopicRow, TopicStore, UserStore,
    },
    errors::{
        api_error::ApiError,
        catalog::{CatalogEntry, bad_gateway, conflict, internal_server_error, unauthorized},
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

    // Topic policy rows, disabled ones included
    topics: Vec<TopicRow>,
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

    // Adds a topic policy row
    pub fn with_topic(
        mut self,
        topic_name: &str,
        gateway_consume: bool,
        ack_required: bool,
        subscriber_publish_allowed: bool,
        enabled: bool,
    ) -> Self {
        self.topics.push(TopicRow {
            topic_name: topic_name.to_string(),
            gateway_consume,
            ack_required,
            subscriber_publish_allowed,
            enabled,
        });
        self
    }

    // The topic policy rows, used to load a registry in tests
    pub fn topic_rows(&self) -> Vec<TopicRow> {
        self.topics.clone()
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

#[async_trait]
impl TopicStore for FakeStore {
    async fn load_topics(&self) -> Result<Vec<TopicRow>, ApiError> {
        if self.broken {
            return Err(Self::broken_error());
        }

        Ok(self.topics.clone())
    }
}

// In-memory stand-in for the broker's topic list
#[derive(Default)]
pub struct FakeLister {
    // Topic names that exist on the fake broker
    topics: Vec<String>,

    // When true listing fails with a gateway error
    broken: bool,
}

impl FakeLister {
    // A broker that has exactly these topics
    pub fn with_topics(topics: &[&str]) -> Self {
        Self {
            topics: topics.iter().map(|topic| topic.to_string()).collect(),
            broken: false,
        }
    }

    // A broker that cannot be listed
    pub fn broken() -> Self {
        Self {
            topics: Vec::new(),
            broken: true,
        }
    }
}

#[async_trait]
impl TopicLister for FakeLister {
    async fn list_topic_names(&self) -> Result<Vec<String>, ApiError> {
        if self.broken {
            return Err(ApiError::from_entry(bad_gateway::UPSTREAM_UNAVAILABLE));
        }

        Ok(self.topics.clone())
    }
}

// In-memory stand-in for Kafka, it records what was published
#[derive(Default)]
pub struct FakePublisher {
    // Every record published so far, in order
    sent: Mutex<Vec<PublishRecord>>,

    // When set every publish fails with this entry
    failure: Option<CatalogEntry>,
}

impl FakePublisher {
    pub fn new() -> Self {
        Self::default()
    }

    // A publisher whose every publish fails with the given catalogue entry
    pub fn failing(entry: CatalogEntry) -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            failure: Some(entry),
        }
    }

    // The records published so far
    pub fn sent(&self) -> Vec<PublishRecord> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl Publisher for FakePublisher {
    async fn publish(&self, record: PublishRecord) -> Result<PublishReceipt, ApiError> {
        if let Some(entry) = self.failure {
            return Err(ApiError::from_entry(entry));
        }

        let mut sent = self.sent.lock().unwrap();
        let topic = record.topic.clone();
        sent.push(record);

        Ok(PublishReceipt {
            topic,
            partition: 0,
            offset: sent.len() as i64 - 1,
        })
    }

    async fn shutdown(&self) {}
}

// Stand-in for the topic reloader, it records who asked for a reload
pub struct FakeReloader {
    // The trigger of every reload so far, in order
    calls: Mutex<Vec<String>>,

    // What every reload answers, an error entry makes it fail
    result: Result<ReloadSummary, CatalogEntry>,
}

impl FakeReloader {
    // A reloader that answers with an empty, unchanged summary
    pub fn new() -> Self {
        Self::with_summary(ReloadSummary {
            enabled_topics: 0,
            consume_topics: 0,
            changed: false,
            missing_topics: Vec::new(),
        })
    }

    // A reloader that answers with the given summary
    pub fn with_summary(summary: ReloadSummary) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            result: Ok(summary),
        }
    }

    // A reloader whose every reload fails with the given catalogue entry
    pub fn failing(entry: CatalogEntry) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            result: Err(entry),
        }
    }

    // The triggers of the reloads so far
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl TopicReload for FakeReloader {
    async fn reload(&self, trigger: &str) -> Result<ReloadSummary, ApiError> {
        self.calls.lock().unwrap().push(trigger.to_string());

        match &self.result {
            Ok(summary) => Ok(summary.clone()),
            Err(entry) => Err(ApiError::from_entry(*entry)),
        }
    }
}
