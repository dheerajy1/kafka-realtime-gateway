use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
};

use super::no_auth::NoAuth;
use crate::{
    auth::password::verify_password,
    db::store::ApiKeyStore,
    errors::{
        api_error::ApiError,
        catalog::{bad_request, unauthorized},
    },
    state::AppState,
};

// Header that carries the API key id
pub const API_KEY_HEADER: &str = "x-api-key";

// Header that carries the API key secret
pub const API_SECRET_HEADER: &str = "x-api-secret";

// Longest accepted API key id, same limit as the old gateway
const MAX_API_KEY_LENGTH: usize = 44;

// Longest accepted API key secret, same limit as the old gateway
const MAX_API_SECRET_LENGTH: usize = 100;

// The API key a request was verified with
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyIdentity {
    // The verified API key id
    pub api_key_id: String,

    // Id of the API user that owns the key
    pub api_user_id: i32,
}

// Checks the x-api-key and x-api-secret headers, WebSocket routes call this directly so they can answer with their own error frame
pub async fn check_api_key(
    keys: &dyn ApiKeyStore,
    headers: &HeaderMap,
) -> Result<ApiKeyIdentity, ApiError> {
    // Missing headers return 401 MISSING_API_KEY
    let (Some(key), Some(secret)) = (headers.get(API_KEY_HEADER), headers.get(API_SECRET_HEADER))
    else {
        return Err(ApiError::from_entry(unauthorized::MISSING_API_KEY));
    };

    // Header values that are not readable text return 400
    let (Ok(key), Ok(secret)) = (key.to_str(), secret.to_str()) else {
        return Err(ApiError::from_entry(bad_request::INVALID_HEADER_VALUES));
    };

    let key = key.trim();
    let secret = secret.trim();

    // Present but empty or too long returns 400
    if key.is_empty()
        || key.chars().count() > MAX_API_KEY_LENGTH
        || secret.is_empty()
        || secret.chars().count() > MAX_API_SECRET_LENGTH
    {
        return Err(ApiError::from_entry(bad_request::INVALID_HEADER_VALUES));
    }

    // Unknown, inactive or expired keys and database failures come back as errors here
    let record = keys.verify_api_key(key).await?;

    if !verify_password(secret, &record.api_key_secret_hash).await? {
        return Err(ApiError::from_entry(unauthorized::INVALID_API_KEY));
    }

    Ok(ApiKeyIdentity {
        api_key_id: record.api_key_id,
        api_user_id: record.api_user_id,
    })
}

// Extractor for API key endpoints, it rejects with the HTTP error, a handler that takes it only runs with valid client credentials and a valid API key
#[derive(Debug, Clone)]
pub struct ApiKeyAuth(pub ApiKeyIdentity);

impl FromRequestParts<AppState> for ApiKeyAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        // The client id and secret are checked first, like the old gateway
        NoAuth::from_request_parts(parts, state).await?;

        let identity = check_api_key(state.api_keys.as_ref(), &parts.headers).await?;

        Ok(Self(identity))
    }
}
