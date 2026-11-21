use std::{
    borrow::Cow,
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode, errors::ErrorKind,
};
use serde::{Deserialize, Serialize};

use crate::errors::{
    api_error::ApiError,
    catalog::{internal_server_error, unauthorized},
};

// Issuer written into every token and required when verifying
pub const JWT_ISSUER: &str = "kafka-api-gateway";

// A token is valid for one day
pub const JWT_LIFETIME_SECONDS: u64 = 60 * 60 * 24;

// Claims written into a new token
// The text is borrowed when a token is signed, so signing copies nothing, and owned when a token is read back
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims<'a> {
    // User name of the API user
    pub sub: Cow<'a, str>,

    // Role of the API user
    pub role: Cow<'a, str>,

    // Expiry as unix seconds
    pub exp: u64,

    // Issuer of the token
    pub iss: Cow<'a, str>,
}

// A freshly signed token and the moment it expires
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedToken {
    // The encoded token
    pub token: String,

    // Expiry as unix seconds
    pub expires_at: u64,
}

// The user a verified token belongs to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthUser {
    // User name of the API user
    pub sub: String,

    // Role of the API user
    pub role: String,
}

// Claims read back from a token, a missing sub or role is reported after the library checks
#[derive(Debug, Deserialize)]
struct RawClaims {
    sub: Option<String>,
    role: Option<String>,
}

// Token header, built once because Header::new allocates the type text on every call
static HEADER: LazyLock<Header> = LazyLock::new(|| Header::new(Algorithm::HS256));

// Verification rules, built once because Validation::new and set_issuer allocate their sets on every call
// The keys are not cached here, they depend on the secret that the caller passes in (S14)
static VALIDATION: LazyLock<Validation> = LazyLock::new(|| {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[JWT_ISSUER]);
    validation.set_required_spec_claims(&["exp", "iss"]);
    // No clock tolerance, an expired token is rejected right away
    validation.leeway = 0;

    validation
});

// Signs a one day HS256 token for an API user
pub fn sign_token(secret: &str, sub: &str, role: &str) -> Result<SignedToken, ApiError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiError::from_entry(internal_server_error::TOKEN_SIGNING_FAILED))?
        .as_secs();

    let expires_at = now + JWT_LIFETIME_SECONDS;

    let claims = Claims {
        sub: Cow::Borrowed(sub),
        role: Cow::Borrowed(role),
        exp: expires_at,
        iss: Cow::Borrowed(JWT_ISSUER),
    };

    let token = encode(
        &HEADER,
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|error| {
        tracing::error!(target: "Auth", "token signing failed: {error}");
        ApiError::from_entry(internal_server_error::TOKEN_SIGNING_FAILED)
    })?;

    Ok(SignedToken { token, expires_at })
}

// Verifies signature, expiry and issuer, then returns the user the token belongs to
pub fn verify_token(secret: &str, token: &str) -> Result<AuthUser, ApiError> {
    let data = decode::<RawClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &VALIDATION,
    )
    .map_err(|error| match error.kind() {
        ErrorKind::ExpiredSignature => ApiError::from_entry(unauthorized::EXPIRED_TOKEN),
        _ => ApiError::from_entry(unauthorized::INVALID_TOKEN),
    })?;

    match (data.claims.sub, data.claims.role) {
        (Some(sub), Some(role)) if !sub.is_empty() && !role.is_empty() => {
            Ok(AuthUser { sub, role })
        }
        _ => Err(ApiError::from_entry(unauthorized::MISSING_SUB_OR_ROLE)),
    }
}
