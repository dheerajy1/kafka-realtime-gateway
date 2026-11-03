pub mod errors;

use axum::{Json, Router, routing::get};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use self::errors::ErrorExamples;
use crate::{
    features::v1::{
        api_keys::handler as api_keys, auth::handler as auth, health::handler as health,
        http_publish::handler as http_publish, topics::handler as topics,
        ws_publish::handler as ws_publish, ws_subscribe::handler as ws_subscribe,
    },
    schemas::response::ErrorResponse,
    state::AppState,
};

// The OpenAPI document, routes are listed in paths and error examples are added by ErrorExamples
#[derive(OpenApi)]
#[openapi(
    info(title = "Kafka Realtime Gateway API", version = "v1"),
    paths(
        health::health,
        health::app_health,
        health::db_health,
        auth::login,
        api_keys::create_api_key,
        http_publish::http_publish,
        topics::reload_topics,
        ws_publish::ws_publish,
        ws_subscribe::ws_subscribe
    ),
    components(schemas(ErrorResponse)),
    modifiers(&ErrorExamples)
)]
pub struct ApiDoc;

// GET /openapi/json returns the raw OpenAPI spec
async fn spec() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

// Routes of the docs, GET /openapi serves the Scalar UI and GET /openapi/json the spec
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/openapi/json", get(spec))
        .merge(Scalar::with_url("/openapi", ApiDoc::openapi()).title("Kafka Realtime Gateway API"))
}
