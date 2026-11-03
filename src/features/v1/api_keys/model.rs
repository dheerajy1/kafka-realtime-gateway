use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::{Validate, ValidationError};

// Checks that an API key id only has letters, digits, underscore and hyphen
fn validate_api_key_id_characters(value: &str) -> Result<(), ValidationError> {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        Ok(())
    } else {
        Err(ValidationError::new("api_key_id_characters").with_message(
            "\"apiKeyId\" may only contain letters, digits, underscore and hyphen".into(),
        ))
    }
}

// Body of POST /api/v1/api-keys
#[derive(Debug, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyRequest {
    // Id of the new API key, chosen by the caller
    #[validate(
        length(
            min = 8,
            max = 64,
            message = "\"apiKeyId\" must be between 8 and 64 characters"
        ),
        custom(function = "validate_api_key_id_characters")
    )]
    pub api_key_id: String,
}

// Data returned after an API key is created, the secret is shown only this once
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyData {
    // Id of the new API key
    pub api_key_id: String,

    // Secret of the new API key, only the hash is stored
    pub api_key_secret: String,
}
