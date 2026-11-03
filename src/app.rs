use axum::{Router, extract::DefaultBodyLimit, http::header, response::IntoResponse, routing::get};
use tower_http::catch_panic::CatchPanicLayer;

use crate::{
    errors::{fallback, panic::handle_panic},
    features::v1,
    middleware::{cors::cors_layer, server_gate::server_gate},
    openapi, pages,
    state::AppState,
};

// Largest request body accepted, 10 MB
const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

// Serves the precompiled Tailwind stylesheet embedded in the binary
async fn global_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../public/global.css"),
    )
}

// Serves the logo image embedded in the binary
async fn logo() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "image/png")],
        include_bytes!("../public/logo.png").as_slice(),
    )
}

// Serves the favicon embedded in the binary
async fn favicon() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "image/x-icon")],
        include_bytes!("../public/favicon.ico").as_slice(),
    )
}

// Full application router, shared by main and the tests, the state carries the database access
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/global.css", get(global_css))
        .route("/logo.png", get(logo))
        .route("/favicon.ico", get(favicon))
        .merge(pages::router())
        .merge(v1::router())
        .merge(openapi::router())
        // Sends requests without the login cookie to the login page, it only wraps routes that exist
        .route_layer(axum::middleware::from_fn(server_gate))
        // Unknown paths return the global 404 error shape
        .fallback(fallback::not_found)
        // Known paths with a wrong method return the global 405 error shape
        .method_not_allowed_fallback(fallback::method_not_allowed)
        // Request bodies above 10 MB are rejected
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        // A panic in any handler becomes the global 500 error shape
        .layer(CatchPanicLayer::custom(handle_panic))
        // Outermost layer, answers preflight requests and adds CORS headers to every response
        .layer(cors_layer())
        // Hands the shared state to every handler
        .with_state(state)
}
