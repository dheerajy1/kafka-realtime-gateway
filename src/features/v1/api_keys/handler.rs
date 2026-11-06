use axum::{Json, Router, extract::State, routing::post};

use super::{
    model::{CreateApiKeyData, CreateApiKeyRequest},
    service,
};
use crate::{
    errors::api_error::ApiError,
    middleware::{auth::Auth, valid_json::ValidJson},
    schemas::response::SuccessResponse,
    state::AppState,
};

// Routes of the api keys feature, mounted under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().route("/api-keys", post(create_api_key))
}

// POST /api/v1/api-keys, the error responses are added in openapi/errors.rs under the operation id api_keys_create
#[utoipa::path(
    post,
    path = "/api/v1/api-keys",
    operation_id = "api_keys_create",
    tag = "API keys",
    summary = "Create an API key",
    description = "Creates an API key for the logged in API user. The secret is returned once and only its hash is stored. Requires client credentials and a Bearer token.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
        ("authorization" = String, Header, description = "Bearer token from POST /api/v1/login"),
    ),
    request_body = CreateApiKeyRequest,
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<CreateApiKeyData>,
            example = json!({
                "success": true,
                "statusCode": 200,
                "message": "API key created successfully",
                "data": {
                    "apiKeyId": "my-service-key",
                    "apiKeySecret": "9f2c1d7a5b8e4f60a3c9d1e2f4b6a8c0d2e4f6a8b0c2d4e6f8a0b2c4d6e8f0a2"
                }
            })
        )
    )
)]

// POST /api/v1/api-keys
pub async fn create_api_key(
    State(state): State<AppState>,
    Auth(user): Auth,
    ValidJson(body): ValidJson<CreateApiKeyRequest>,
) -> Result<Json<SuccessResponse<CreateApiKeyData>>, ApiError> {
    let data =
        service::create_api_key(state.api_keys.as_ref(), &user.sub, &body.api_key_id).await?;

    Ok(Json(SuccessResponse::ok(
        "API key created successfully",
        data,
    )))
}
