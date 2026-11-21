use std::sync::LazyLock;

use axum::{Router, response::Html, routing::get};

use super::render::render;
use crate::components::home::Home;

// Routes of the Home page
pub fn router() -> Router<crate::state::AppState> {
    Router::new().route("/", get(home))
}

// The Home page never changes, so it is rendered once on the first request and reused
static HOME_HTML: LazyLock<String> = LazyLock::new(|| render(Home).0);

// GET /, the cached page is lent to the response, so nothing is rendered or copied per request
async fn home() -> Html<&'static str> {
    Html(HOME_HTML.as_str())
}
