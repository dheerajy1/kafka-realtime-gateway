use std::fmt::Display;

use crate::errors::{api_error::ApiError, catalog::internal_server_error};

// bcrypt cost for new hashes, the same as the existing hashes in the database
pub const BCRYPT_COST: u32 = 12;

// Valid bcrypt hash (cost 12) of a random password nobody knows, checked when a user does not exist
// Cost 12 matches the real hashes, so a missing user takes as long as a wrong password
const DUMMY_HASH: &str = "$2b$12$fNP4bEScoPqLCQM3fzs3oOmKKjf8GvozM0wL9l3CJO5VsHl4loMyi";

// Logs the real failure and returns the generic server error to the caller
fn password_error(action: &str, detail: impl Display) -> ApiError {
    tracing::error!(target: "Auth", "password {action} failed: {detail}");

    ApiError::from_entry(internal_server_error::SERVER_ERROR)
}

// Checks a plain password against a bcrypt hash on a blocking thread, bcrypt is slow on purpose
// The texts are taken by value because the blocking thread needs its own copy, so a caller that is done with them moves them in
// A missing hash (unknown user) is checked against a dummy hash and never matches,
// so the response time does not show which users exist
pub async fn verify_password(password: String, hash: Option<String>) -> Result<bool, ApiError> {
    let user_exists = hash.is_some();

    let is_valid = tokio::task::spawn_blocking(move || match hash {
        Some(hash) => bcrypt::verify(password, &hash),
        None => bcrypt::verify(password, DUMMY_HASH),
    })
    .await
    // A task that panicked or was cancelled is a server problem
    .map_err(|error| password_error("verification task", error))?
    // A hash that is not valid bcrypt is a server problem, not a wrong password
    .map_err(|error| password_error("verification", error))?;

    Ok(is_valid && user_exists)
}

// Hashes a plain password with bcrypt on a blocking thread
pub async fn hash_password(password: &str) -> Result<String, ApiError> {
    let password = password.to_string();

    tokio::task::spawn_blocking(move || bcrypt::hash(password, BCRYPT_COST))
        .await
        .map_err(|error| password_error("hashing task", error))?
        .map_err(|error| password_error("hashing", error))
}
