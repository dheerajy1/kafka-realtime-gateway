use async_trait::async_trait;

use crate::errors::api_error::ApiError;

// The columns of an api_users row that the API login needs
#[derive(Debug, Clone)]
pub struct ApiUserRecord {
    // Login name, becomes the JWT sub claim
    pub user_name: String,

    // bcrypt hash of the password
    pub password_hash: String,

    // Role of the user, admin or user, becomes the JWT role claim
    pub user_role: String,
}

// What fn_verify_api_key returns for a valid key
#[derive(Debug, Clone)]
pub struct ApiKeyRecord {
    // The API key id that was verified
    pub api_key_id: String,

    // Id of the API user that owns the key
    pub api_user_id: i32,

    // bcrypt hash of the key secret
    pub api_key_secret_hash: String,
}

// Reads the two user tables, both lookups only match active users
#[async_trait]
pub trait UserStore: Send + Sync {
    // Returns the password hash of an active portal user, None when there is no such user
    async fn find_portal_user_hash(&self, username: &str) -> Result<Option<String>, ApiError>;

    // Returns an active API user, None when there is no such user
    async fn find_api_user(&self, username: &str) -> Result<Option<ApiUserRecord>, ApiError>;
}

// Wraps the two API key database functions
#[async_trait]
pub trait ApiKeyStore: Send + Sync {
    // Calls fn_verify_api_key, an unknown, inactive or expired key is an error
    async fn verify_api_key(&self, api_key_id: &str) -> Result<ApiKeyRecord, ApiError>;

    // Calls fn_create_api_key, any failure is returned as 409 DATABASE_CONFLICT
    async fn create_api_key(
        &self,
        username: &str,
        api_key_id: &str,
        api_key_secret_hash: &str,
    ) -> Result<(), ApiError>;
}

// Checks that the database answers, used by the health routes
#[async_trait]
pub trait DbProbe: Send + Sync {
    // Runs SELECT 1 on the default pool, true when it succeeds
    async fn ping(&self) -> bool;
}

// One row of fn_get_gateway_kafka_topics, the routing policy of a topic
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRow {
    // Kafka topic name
    pub topic_name: String,

    // The gateway consumes this topic and forwards it to subscribers
    pub gateway_consume: bool,

    // A subscriber must acknowledge each message before its offset is committed
    pub ack_required: bool,

    // Subscribers may publish to this topic over the WebSocket
    pub subscriber_publish_allowed: bool,

    // Disabled topics are ignored completely
    pub enabled: bool,
}

// Loads the topic routing policy
#[async_trait]
pub trait TopicStore: Send + Sync {
    // Calls fn_get_gateway_kafka_topics, returns every row including disabled ones
    async fn load_topics(&self) -> Result<Vec<TopicRow>, ApiError>;
}
