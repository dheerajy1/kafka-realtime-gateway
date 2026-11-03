pub mod api_keys;
pub mod auth;
pub mod health;

use axum::Router;

use crate::state::AppState;

// All v1 routes, nested under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().nest(
        "/api/v1",
        Router::new()
            .nest("/health", health::handler::router())
            .merge(auth::handler::router())
            .merge(api_keys::handler::router()),
    )
}
