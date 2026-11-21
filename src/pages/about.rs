use std::sync::LazyLock;

use axum::{Router, response::Html, routing::get};

use super::render::render_with_props;
use crate::components::about::{About, AboutProps};

// Title of the About page
const ABOUT_TITLE: &str = "About | Kafka realtime gateway";

// Text shown on the About page
const ABOUT_DESCRIPTION: &str = "Kafka realtime gateway is a production-grade custom backend platform designed to prove clean contracts, strict validation, and zero drift between backend and frontend. It demonstrates how a modern Rust backend built with Axum and Dioxus server-side rendering can act as a single source of truth, exposing OpenAPI specifications that directly power frontend types, fetching, and caching strategies.";

// Routes of the About page
pub fn router() -> Router<crate::state::AppState> {
    Router::new().route("/about", get(about))
}

// The About page never changes, so it is rendered once on the first request and reused
static ABOUT_HTML: LazyLock<String> = LazyLock::new(|| {
    render_with_props(
        About,
        AboutProps {
            title: ABOUT_TITLE,
            heading: "About",
            description: ABOUT_DESCRIPTION,
        },
    )
    .0
});

// GET /about, the cached page is lent to the response, so nothing is rendered or copied per request
async fn about() -> Html<&'static str> {
    Html(ABOUT_HTML.as_str())
}
