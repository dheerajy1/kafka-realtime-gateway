use axum::{Router, response::Html, routing::get};

use super::render::render;
use crate::components::home::Home;

// Routes of the Home page
pub fn router() -> Router {
    Router::new().route("/", get(home))
}

// GET /
async fn home() -> Html<String> {
    render(Home)
}
