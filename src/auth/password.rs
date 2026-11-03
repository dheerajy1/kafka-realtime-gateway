use crate::errors::{api_error::ApiError, catalog::internal_server_error};

// bcrypt cost for new hashes, the same as the existing hashes in the database
pub const BCRYPT_COST: u32 = 12;

// Checks a plain password against a bcrypt hash on a blocking thread, bcrypt is slow on purpose
pub async fn verify_password(password: &str, hash: &str) -> Result<bool, ApiError> {
    let password = password.to_string();
    let hash = hash.to_string();

    match tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash)).await {
        Ok(Ok(valid)) => Ok(valid),
        // A hash that is not valid bcrypt is a server problem, not a wrong password
        Ok(Err(error)) => {
            tracing::error!(target: "Auth", "password verification failed: {error}");
            Err(ApiError::from_entry(internal_server_error::SERVER_ERROR))
        }
        Err(error) => {
            tracing::error!(target: "Auth", "password verification task failed: {error}");
            Err(ApiError::from_entry(internal_server_error::SERVER_ERROR))
        }
    }
}

// Hashes a plain password with bcrypt on a blocking thread
pub async fn hash_password(password: &str) -> Result<String, ApiError> {
    let password = password.to_string();

    match tokio::task::spawn_blocking(move || bcrypt::hash(password, BCRYPT_COST)).await {
        Ok(Ok(hash)) => Ok(hash),
        Ok(Err(error)) => {
            tracing::error!(target: "Auth", "password hashing failed: {error}");
            Err(ApiError::from_entry(internal_server_error::SERVER_ERROR))
        }
        Err(error) => {
            tracing::error!(target: "Auth", "password hashing task failed: {error}");
            Err(ApiError::from_entry(internal_server_error::SERVER_ERROR))
        }
    }
}
