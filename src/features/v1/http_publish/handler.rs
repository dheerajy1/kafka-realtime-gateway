use axum::{Json, Router, extract::State, http::StatusCode, routing::post};

use super::{
    model::{HttpPublishData, HttpPublishRequest},
    service,
};
use crate::{
    errors::api_error::ApiError,
    middleware::{api_key::ApiKeyAuth, valid_json::ValidJson},
    schemas::response::SuccessResponse,
    state::AppState,
};

// Routes of the http publish feature, mounted under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().route("/http-publish", post(http_publish))
}

// POST /api/v1/http-publish, the error responses are added in openapi/errors.rs under the operation id http_publish
#[utoipa::path(
    post,
    path = "/api/v1/http-publish",
    operation_id = "http_publish",
    tag = "Publish",
    summary = "Publish a message to a Kafka topic",
    description = "Accepts a generic transport envelope (topic, correlationId, value). Publishes the value to the topic using the correlationId as the Kafka message key. Returns 202 after Kafka accepted the message. The topic must be enabled in the gateway's topic registry.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
        ("x-api-key" = String, Header, description = "API key id"),
        ("x-api-secret" = String, Header, description = "API key secret"),
    ),
    request_body = HttpPublishRequest,
    responses(
        (
            status = 202,
            description = "Response for status 202",
            body = SuccessResponse<HttpPublishData>,
            example = json!({
                "success": true,
                "httpCode": 202,
                "message": "Event accepted",
                "data": {
                    "topic": "orders",
                    "correlationId": "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f13"
                }
            })
        )
    )
)]

// POST /api/v1/http-publish
pub async fn http_publish(
    State(state): State<AppState>,
    // The API key is checked before the body, like the old gateway
    ApiKeyAuth(_identity): ApiKeyAuth,
    ValidJson(body): ValidJson<HttpPublishRequest>,
) -> Result<(StatusCode, Json<SuccessResponse<HttpPublishData>>), ApiError> {
    let data =
        service::publish_event(state.topics.as_ref(), state.publisher.as_ref(), body).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(SuccessResponse::accepted("Event accepted", data)),
    ))
}
