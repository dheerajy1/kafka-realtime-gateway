use async_trait::async_trait;
use sqlx::PgPool;

use super::{
    error::map_db_error,
    store::{ApiKeyRecord, ApiKeyStore, ApiUserRecord, DbProbe, UserStore},
};
use crate::errors::{
    api_error::ApiError,
    catalog::{conflict, unauthorized},
};

// The result the function returns when it succeeds
const RESULT_SUCCESS: &str = "SUCCESS";

// The real database behind the store traits, it holds both pools
#[derive(Clone)]
pub struct PgStore {
    // Public user pool, used for the user lookups and the health probe
    default_pool: PgPool,

    // Service user pool, used for the database functions
    service_pool: PgPool,
}

impl PgStore {
    pub fn new(default_pool: PgPool, service_pool: PgPool) -> Self {
        Self {
            default_pool,
            service_pool,
        }
    }

    // Closes both pools, used at shutdown
    pub async fn close(&self) {
        self.default_pool.close().await;
        self.service_pool.close().await;
    }
}

#[async_trait]
impl UserStore for PgStore {
    async fn find_portal_user_hash(&self, username: &str) -> Result<Option<String>, ApiError> {
        sqlx::query_scalar::<_, String>(
            "SELECT password_hash FROM portal_users WHERE user_name = $1 AND is_active = TRUE",
        )
        .bind(username)
        .fetch_optional(&self.default_pool)
        .await
        .map_err(|error| map_db_error(&error))
    }

    async fn find_api_user(&self, username: &str) -> Result<Option<ApiUserRecord>, ApiError> {
        let row = sqlx::query_as::<_, (String, String, String)>(
            "SELECT user_name, password_hash, user_role FROM api_users WHERE user_name = $1 AND is_active = TRUE",
        )
        .bind(username)
        .fetch_optional(&self.default_pool)
        .await
        .map_err(|error| map_db_error(&error))?;

        Ok(
            row.map(|(user_name, password_hash, user_role)| ApiUserRecord {
                user_name,
                password_hash,
                user_role,
            }),
        )
    }
}

#[async_trait]
impl ApiKeyStore for PgStore {
    async fn verify_api_key(&self, api_key_id: &str) -> Result<ApiKeyRecord, ApiError> {
        // The notes column is JSONB, so the fields are read out in SQL to avoid the sqlx json feature
        let row = sqlx::query_as::<_, (String, Option<String>, Option<i32>, Option<String>)>(
            "SELECT result, notes->>'apiKeyId', (notes->>'apiUserId')::int, notes->>'apiKeySecretHash' \
             FROM fn_verify_api_key($1::varchar)",
        )
        .bind(api_key_id)
        .fetch_optional(&self.service_pool)
        .await
        .map_err(|error| map_db_error(&error))?;

        match row {
            Some((result, Some(api_key_id), Some(api_user_id), Some(api_key_secret_hash)))
                if result == RESULT_SUCCESS =>
            {
                Ok(ApiKeyRecord {
                    api_key_id,
                    api_user_id,
                    api_key_secret_hash,
                })
            }
            // No row or an unexpected result is treated as an invalid key, like the old gateway
            _ => Err(ApiError::from_entry(unauthorized::INVALID_API_KEY)),
        }
    }

    async fn create_api_key(
        &self,
        username: &str,
        api_key_id: &str,
        api_key_secret_hash: &str,
    ) -> Result<(), ApiError> {
        let result = sqlx::query_scalar::<_, String>(
            "SELECT result FROM fn_create_api_key($1::varchar, $2::varchar, $3::varchar)",
        )
        .bind(username)
        .bind(api_key_id)
        .bind(api_key_secret_hash)
        .fetch_optional(&self.service_pool)
        .await;

        match result {
            Ok(Some(result)) if result == RESULT_SUCCESS => Ok(()),
            Ok(_) => {
                tracing::error!(target: "Db", "fn_create_api_key did not return SUCCESS");
                Err(ApiError::from_entry(conflict::DATABASE_CONFLICT))
            }
            // A duplicate key id or an unknown user raises inside the function, every failure is a conflict
            Err(error) => {
                tracing::error!(target: "Db", "fn_create_api_key failed: {error}");
                Err(ApiError::from_entry(conflict::DATABASE_CONFLICT))
            }
        }
    }
}

#[async_trait]
impl DbProbe for PgStore {
    async fn ping(&self) -> bool {
        sqlx::query("SELECT 1")
            .execute(&self.default_pool)
            .await
            .is_ok()
    }
}
