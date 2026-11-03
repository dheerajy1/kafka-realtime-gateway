use super::model::CreateApiKeyData;
use crate::{
    auth::password::hash_password,
    db::store::ApiKeyStore,
    errors::{api_error::ApiError, catalog::internal_server_error},
};

// Number of random bytes in a new secret, 32 bytes become 64 hex characters
const SECRET_BYTES: usize = 32;

// Makes a new random secret as lowercase hex
fn generate_secret() -> Result<String, ApiError> {
    let mut bytes = [0u8; SECRET_BYTES];

    getrandom::fill(&mut bytes).map_err(|error| {
        tracing::error!(target: "Auth", "random secret generation failed: {error}");
        ApiError::from_entry(internal_server_error::SERVER_ERROR)
    })?;

    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

// Creates an API key for a user, only the bcrypt hash of the secret is stored
pub async fn create_api_key(
    keys: &dyn ApiKeyStore,
    username: &str,
    api_key_id: &str,
) -> Result<CreateApiKeyData, ApiError> {
    let secret = generate_secret()?;
    let secret_hash = hash_password(&secret).await?;

    // A duplicate key id or any other database failure comes back as 409 DATABASE_CONFLICT
    keys.create_api_key(username, api_key_id, &secret_hash)
        .await?;

    Ok(CreateApiKeyData {
        api_key_id: api_key_id.to_string(),
        api_key_secret: secret,
    })
}
