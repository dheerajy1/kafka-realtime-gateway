use axum::{Router, response::Html, routing::get};

use super::render::render_with_props;
use crate::components::about::{About, AboutProps};

// Text shown on the About page
const ABOUT_DESCRIPTION: &str = "Kafka realtime gateway is a production-grade custom backend platform designed to prove clean contracts, strict validation, and zero drift between backend and frontend. It demonstrates how a modern Rust backend built with Axum and Dioxus server-side rendering can act as a single source of truth, exposing OpenAPI specifications that directly power frontend types, fetching, and caching strategies.";

// Routes of the About page
pub fn router() -> Router {
    Router::new().route("/about", get(about))
}

// GET /about
async fn about() -> Html<String> {
    render_with_props(
        About,
        AboutProps {
            title: "About | Kafka realtime gateway".to_string(),
            heading: "About".to_string(),
            description: ABOUT_DESCRIPTION.to_string(),
        },
    )
}
