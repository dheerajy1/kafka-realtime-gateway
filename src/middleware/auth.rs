use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts},
};

use super::no_auth::NoAuth;
use crate::{
    auth::jwt::{AuthUser, verify_token},
    config::env::env,
    errors::{api_error::ApiError, catalog::unauthorized},
};

// Extractor for JWT protected endpoints, a handler that takes it only runs with valid client credentials and a valid Bearer token
#[derive(Debug, Clone)]
pub struct Auth(pub AuthUser);

// Reads the token out of an Authorization header value like "Bearer <token>"
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    let token = token.trim();

    if scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() {
        Some(token)
    } else {
        None
    }
}

impl<S> FromRequestParts<S> for Auth
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // The client id and secret are checked first, like the old gateway
        NoAuth::from_request_parts(parts, state).await?;

        // A missing or non-Bearer Authorization header returns 401 MISSING_AUTHORIZATION_TOKEN
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token)
            .ok_or_else(|| ApiError::from_entry(unauthorized::MISSING_AUTHORIZATION_TOKEN))?;

        let user = verify_token(&env().jwt_secret, token)?;

        Ok(Self(user))
    }
}
