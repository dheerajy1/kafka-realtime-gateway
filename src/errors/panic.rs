use std::any::Any;

use axum::response::{IntoResponse, Response};

use super::{api_error::ApiError, catalog::internal_server_error};

// Turns a panic inside a handler into the global 500 error shape
pub fn handle_panic(payload: Box<dyn Any + Send + 'static>) -> Response {
    // Panic text is only logged, never sent to the client, it is borrowed from the payload and not copied
    let detail = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload");

    tracing::error!(target: "Http", "handler panicked: {detail}");

    ApiError::from_entry(internal_server_error::SERVER_ERROR).into_response()
}
