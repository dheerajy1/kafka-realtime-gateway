pub mod errors;

use std::sync::LazyLock;

use axum::{
    Router,
    http::header,
    response::{Html, IntoResponse},
    routing::get,
};
use utoipa::OpenApi;
use utoipa_scalar::Scalar;

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
        health::kafka_health,
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

// The spec as JSON text, built on the first request and then reused for every request
static SPEC_JSON: LazyLock<String> = LazyLock::new(|| {
    // The spec is built from our own code, so failing to turn it into JSON is a bug
    serde_json::to_string(&ApiDoc::openapi()).expect("the OpenAPI spec always serializes")
});

// GET /openapi/json returns the raw OpenAPI spec, the cached text is lent to the response
async fn spec() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        SPEC_JSON.as_str(),
    )
}

// The Scalar page with the spec inside, built on the first request and then reused for every request
// Scalar's own axum route clones this whole text on every request, so the page is served from here instead
static SCALAR_HTML: LazyLock<String> = LazyLock::new(|| {
    Scalar::new(ApiDoc::openapi())
        .title("Kafka Realtime Gateway API")
        .to_html()
});

// GET /openapi serves the Scalar UI, the cached text is lent to the response
async fn scalar() -> Html<&'static str> {
    Html(SCALAR_HTML.as_str())
}

// Routes of the docs, GET /openapi serves the Scalar UI and GET /openapi/json the spec
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/openapi", get(scalar))
        .route("/openapi/json", get(spec))
}
