use axum::{
    extract::ws::{CloseFrame, Message, WebSocket},
    http::HeaderMap,
};
use serde_json::{Value, json};

use crate::{
    errors::api_error::ApiError,
    middleware::{
        api_key::{ApiKeyIdentity, check_api_key},
        no_auth::check_client_credentials,
    },
    state::AppState,
};

pub mod keepalive;

// Close code for a connection that failed authentication, same as the old gateway
pub const CLOSE_AUTH_FAILED: u16 = 4001;

// Close code for a gateway that is shutting down
pub const CLOSE_GOING_AWAY: u16 = 1001;

// Close code for a subscriber that is too slow, it may reconnect
pub const CLOSE_TRY_AGAIN_LATER: u16 = 1013;

// Largest WebSocket message and frame the gateway accepts (D11)
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

// Checks the client credentials and the API key of a connection that just opened, the client credentials come first like on the HTTP routes
pub async fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<ApiKeyIdentity, ApiError> {
    check_client_credentials(headers)?;

    check_api_key(state.api_keys.as_ref(), headers).await
}

// The error frame for a failed authentication, the fields of the HTTP error shape plus the type
pub fn auth_error_frame(error: &ApiError) -> String {
    json!({
        "type": "error",
        "success": false,
        "statusCode": error.code().status_code(),
        "code": error.code().as_str(),
        "message": error.message(),
        "error": error.error(),
    })
    .to_string()
}

// A plain error frame with a message
pub fn error_frame(message: &str) -> String {
    json!({ "type": "error", "message": message }).to_string()
}

// A frame made of the given fields
pub fn frame(fields: Value) -> String {
    fields.to_string()
}

// How a WebSocket connection ended
pub enum Exit {
    // The gateway closes the connection with this code and reason
    Close(u16, &'static str),

    // The client is already gone, there is nobody to send a close frame to
    Gone,

    // The client stopped answering pings, the connection is dropped without a close frame
    NoAnswer,
}

// Sends a close frame, a client that is already gone is ignored
pub async fn close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
}
