use serde::Deserialize;
use validator::Validate;

// Default port of the on-prem PostgreSQL server
fn default_db_port() -> u16 {
    5432
}

// Runtime environment the gateway runs in, only these two values are accepted
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AppEnv {
    Development,
    Production,
}

impl AppEnv {
    // Returns the value as written in APP_ENV
    pub fn as_str(self) -> &'static str {
        match self {
            AppEnv::Development => "development",
            AppEnv::Production => "production",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Validate)]
pub struct EnvSchema {
    // Runtime environment, development or production
    pub app_env: AppEnv,

    // Port the server listens on
    #[validate(range(min = 1, message = "APP_PORT must be a valid port number"))]
    pub app_port: u16,

    // Secret used to sign and verify API login tokens
    #[validate(length(
        min = 1,
        max = 50,
        message = "JWT_SECRET must be between 1 and 50 characters"
    ))]
    pub jwt_secret: String,

    // Client id that callers send in the x-client-id header
    #[validate(length(
        min = 1,
        max = 50,
        message = "CLIENT_ID must be between 1 and 50 characters"
    ))]
    pub client_id: String,

    // Client secret that callers send in the x-client-secret header
    #[validate(length(
        min = 1,
        max = 50,
        message = "CLIENT_SECRET must be between 1 and 50 characters"
    ))]
    pub client_secret: String,

    // Application name, also used as the PostgreSQL application_name prefix
    #[validate(length(
        min = 1,
        max = 50,
        message = "APP_NAME must be between 1 and 50 characters"
    ))]
    pub app_name: String,

    // On-prem PostgreSQL host name
    #[validate(length(
        min = 1,
        max = 50,
        message = "ONPREM_DB_SERVER must be between 1 and 50 characters"
    ))]
    pub onprem_db_server: String,

    // On-prem PostgreSQL port, defaults to 5432
    #[serde(default = "default_db_port")]
    #[validate(range(min = 1, message = "ONPREM_DB_PORT must be a valid port number"))]
    pub onprem_db_port: u16,

    // On-prem PostgreSQL database name
    #[validate(length(
        min = 1,
        max = 50,
        message = "ONPREM_DB_DATABASE must be between 1 and 50 characters"
    ))]
    pub onprem_db_database: String,

    // On-prem PostgreSQL user for portal login and public reads
    #[validate(length(
        min = 1,
        max = 50,
        message = "ONPREM_DB_USER_PUBLIC must be between 1 and 50 characters"
    ))]
    pub onprem_db_user_public: String,

    // Password of the public on-prem PostgreSQL user
    #[validate(length(
        min = 1,
        max = 50,
        message = "ONPREM_DB_PASSWORD_PUBLIC must be between 1 and 50 characters"
    ))]
    pub onprem_db_password_public: String,

    // On-prem PostgreSQL user for service queries
    #[validate(length(
        min = 1,
        max = 50,
        message = "ONPREM_DB_USER_SERVICE must be between 1 and 50 characters"
    ))]
    pub onprem_db_user_service: String,

    // Password of the service on-prem PostgreSQL user
    #[validate(length(
        min = 1,
        max = 50,
        message = "ONPREM_DB_PASSWORD_SERVICE must be between 1 and 50 characters"
    ))]
    pub onprem_db_password_service: String,

    // Comma-separated list of Kafka broker addresses
    #[validate(length(min = 1, message = "KAFKA_BROKERS is required and cannot be empty"))]
    pub kafka_brokers: String,

    // Kafka client id that identifies this gateway to the broker
    #[validate(length(
        min = 1,
        max = 100,
        message = "KAFKA_CLIENT_ID must be between 1 and 100 characters"
    ))]
    pub kafka_client_id: String,

    // Kafka consumer group id, unique per environment
    #[validate(length(
        min = 1,
        max = 100,
        message = "KAFKA_GROUP_ID must be between 1 and 100 characters"
    ))]
    pub kafka_group_id: String,
}
