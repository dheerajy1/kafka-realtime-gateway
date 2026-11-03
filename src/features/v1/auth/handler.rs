use axum::{Json, Router, extract::State, routing::post};

use super::{
    model::{LoginData, LoginRequest},
    service,
};
use crate::{
    config::env::env,
    errors::api_error::ApiError,
    middleware::{no_auth::NoAuth, valid_json::ValidJson},
    schemas::response::SuccessResponse,
    state::AppState,
};

// Routes of the auth feature, mounted under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().route("/login", post(login))
}

// POST /api/v1/login, the error responses are added in openapi/errors.rs under the operation id auth_login
#[utoipa::path(
    post,
    path = "/api/v1/login",
    operation_id = "auth_login",
    tag = "Auth",
    summary = "API user login",
    description = "Authenticates an API user and returns a JWT used to manage API keys. Requires client credentials.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
    ),
    request_body = LoginRequest,
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<LoginData>,
            example = json!({
                "success": true,
                "httpCode": 200,
                "message": "Login successful",
                "data": {
                    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                    "expiresAt": "2025-01-02T00:00:00.000Z"
                }
            })
        )
    )
)]

// POST /api/v1/login
pub async fn login(
    State(state): State<AppState>,
    _auth: NoAuth,
    ValidJson(body): ValidJson<LoginRequest>,
) -> Result<Json<SuccessResponse<LoginData>>, ApiError> {
    let data = service::login(state.users.as_ref(), &env().jwt_secret, &body).await?;

    Ok(Json(SuccessResponse::ok("Login successful", data)))
}
