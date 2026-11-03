use axum::{Router, response::Html, routing::get};

use super::render::render_with_props;
use crate::components::login::{Login, LoginProps};

// Routes of the Login page
pub fn router() -> Router {
    Router::new().route("/login", get(login))
}

// GET /login
async fn login() -> Html<String> {
    render_with_props(
        Login,
        LoginProps {
            title: "Login | Kafka realtime gateway".to_string(),
        },
    )
}
