pub mod api_keys;
pub mod auth;
pub mod health;
pub mod http_publish;
pub mod topics;
pub mod ws_publish;
pub mod ws_subscribe;

use axum::Router;

use crate::state::AppState;

// All v1 routes, nested under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().nest(
        "/api/v1",
        Router::new()
            .nest("/health", health::handler::router())
            .merge(auth::handler::router())
            .merge(api_keys::handler::router())
            .merge(http_publish::handler::router())
            .merge(topics::handler::router())
            .merge(ws_publish::handler::router())
            .merge(ws_subscribe::handler::router()),
    )
}
