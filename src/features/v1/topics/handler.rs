use axum::{Json, Router, extract::State, routing::post};

use super::model::TopicReloadData;
use crate::{
    errors::api_error::ApiError, middleware::api_key::ApiKeyAuth,
    schemas::response::SuccessResponse, state::AppState,
};

// Routes of the topics feature, mounted under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().route("/topics/reload", post(reload_topics))
}

// POST /api/v1/topics/reload, the error responses are added in openapi/errors.rs under the operation id topics_reload
#[utoipa::path(
    post,
    path = "/api/v1/topics/reload",
    operation_id = "topics_reload",
    tag = "Topics",
    summary = "Reload the topic policy",
    description = "Reads the topics table again, checks the enabled topics against Kafka and applies the result: new topics are subscribed, disabled topics stop being delivered and topics that are consumed again are resumed. Call it after changing the kafka_topic table. Requires client credentials and an API key.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
        ("x-api-key" = String, Header, description = "API key id"),
        ("x-api-secret" = String, Header, description = "API key secret"),
    ),
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<TopicReloadData>,
            example = json!({
                "success": true,
                "httpCode": 200,
                "message": "Topics reloaded",
                "data": {
                    "enabledTopics": 11,
                    "consumeTopics": 7,
                    "changed": true,
                    "missingTopics": []
                }
            })
        )
    )
)]

// POST /api/v1/topics/reload
pub async fn reload_topics(
    State(state): State<AppState>,
    ApiKeyAuth(_identity): ApiKeyAuth,
) -> Result<Json<SuccessResponse<TopicReloadData>>, ApiError> {
    let summary = state.reloader.reload("api").await?;

    Ok(Json(SuccessResponse::ok(
        "Topics reloaded",
        TopicReloadData::from(summary),
    )))
}
