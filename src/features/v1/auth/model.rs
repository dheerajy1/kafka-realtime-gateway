use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

// Body of POST /api/v1/login
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct LoginRequest {
    // User name of the API user
    #[validate(length(min = 1, message = "\"username\" must not be empty"))]
    pub username: String,

    // Plain password of the API user
    #[validate(length(min = 6, message = "\"password\" must be at least 6 characters"))]
    pub password: String,
}

// Data returned by a successful login
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginData {
    // JWT to send as a Bearer token
    pub token: String,

    // Moment the token expires, ISO 8601 UTC with milliseconds
    pub expires_at: String,
}
