use chrono::{DateTime, SecondsFormat};

use super::model::{LoginData, LoginRequest};
use crate::{
    auth::{jwt::sign_token, password::verify_password},
    db::store::UserStore,
    errors::{
        api_error::ApiError,
        catalog::{internal_server_error, unauthorized},
    },
};

// Checks the API user credentials and returns a signed token, an unknown user and a wrong password give the same error
pub async fn login(
    users: &dyn UserStore,
    jwt_secret: &str,
    request: &LoginRequest,
) -> Result<LoginData, ApiError> {
    let user = users
        .find_api_user(&request.username)
        .await?
        .ok_or_else(|| ApiError::from_entry(unauthorized::INVALID_CREDENTIALS))?;

    if !verify_password(&request.password, &user.password_hash).await? {
        return Err(ApiError::from_entry(unauthorized::INVALID_CREDENTIALS));
    }

    let signed = sign_token(jwt_secret, &user.user_name, &user.user_role)?;

    // The expiry is sent as ISO 8601 UTC with milliseconds, like the old gateway
    let expires_at = DateTime::from_timestamp(signed.expires_at as i64, 0)
        .ok_or_else(|| ApiError::from_entry(internal_server_error::TOKEN_SIGNING_FAILED))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    Ok(LoginData {
        token: signed.token,
        expires_at,
    })
}
