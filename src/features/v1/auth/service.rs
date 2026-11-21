use chrono::{DateTime, SecondsFormat};

use super::model::{LoginData, LoginRequest};
use crate::{
    auth::{jwt::sign_token, password::verify_password},
    db::store::{ApiUserRecord, UserStore},
    errors::{
        api_error::ApiError,
        catalog::{internal_server_error, unauthorized},
    },
};

// Checks the API user credentials and returns a signed token, an unknown user and a wrong password give the same error
// The request is taken by value so the password moves into the check without a copy
pub async fn login(
    users: &dyn UserStore,
    jwt_secret: &str,
    request: LoginRequest,
) -> Result<LoginData, ApiError> {
    let user = users.find_api_user(&request.username).await?;

    // The hash moves into the check and the name and role stay for the token
    let (hash, who) = match user {
        Some(ApiUserRecord {
            user_name,
            password_hash,
            user_role,
        }) => (Some(password_hash), Some((user_name, user_role))),
        None => (None, None),
    };

    // An unknown user is checked against a dummy hash inside, so it takes as long as a wrong password
    let is_valid = verify_password(request.password, hash).await?;

    let (Some((user_name, user_role)), true) = (who, is_valid) else {
        return Err(ApiError::from_entry(unauthorized::INVALID_CREDENTIALS));
    };

    let signed = sign_token(jwt_secret, &user_name, &user_role)?;

    // The expiry is sent as ISO 8601 UTC with milliseconds, like the old gateway
    let expires_at = i64::try_from(signed.expires_at)
        .ok()
        .and_then(|expires_at| DateTime::from_timestamp(expires_at, 0))
        .ok_or_else(|| ApiError::from_entry(internal_server_error::TOKEN_SIGNING_FAILED))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    Ok(LoginData {
        token: signed.token,
        expires_at,
    })
}
