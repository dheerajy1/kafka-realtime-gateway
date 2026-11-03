use axum::{
    Router,
    body::Bytes,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::Response,
    routing::get,
};
use tokio::sync::{mpsc, watch};

use super::session::SubscriberSession;
use crate::{
    errors::{api_error::ApiError, catalog::bad_request},
    state::AppState,
    ws::{
        CLOSE_AUTH_FAILED, CLOSE_GOING_AWAY, CLOSE_TRY_AGAIN_LATER, Exit, MAX_MESSAGE_SIZE,
        auth_error_frame, authenticate, close, error_frame, frame,
        keepalive::{Beat, Keepalive, PING_INTERVAL},
    },
};

// Header that names the subscriber, used in logs
const SUBSCRIBER_ID_HEADER: &str = "x-subscriber-id";

// Longest accepted subscriber id, same limit as the old gateway
const MAX_SUBSCRIBER_ID_LENGTH: usize = 44;

// Routes of the ws-subscribe feature, mounted under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().route("/ws-subscribe", get(ws_subscribe))
}

// GET /api/v1/ws-subscribe, the error responses are added in openapi/errors.rs under the operation id ws_subscribe
#[utoipa::path(
    get,
    path = "/api/v1/ws-subscribe",
    operation_id = "ws_subscribe",
    tag = "WebSocket",
    summary = "Subscribe to Kafka topics over a WebSocket",
    description = "Upgrades to a WebSocket. The client id, client secret, API key and API secret are checked after the upgrade: a failure sends an error frame and closes with code 4001. On success the server sends {\"type\":\"ready\"}. Client messages: subscribe, unsubscribe, processed (ACK) and publish. A missing or invalid x-subscriber-id returns 400 before the upgrade.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
        ("x-api-key" = String, Header, description = "API key id"),
        ("x-api-secret" = String, Header, description = "API key secret"),
        ("x-subscriber-id" = String, Header, description = "Name of the subscriber, used in logs"),
    ),
    responses(
        (status = 101, description = "Switching Protocols, the WebSocket is open")
    )
)]

// GET /api/v1/ws-subscribe
pub async fn ws_subscribe(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let subscriber = subscriber_id(&headers)?;

    Ok(upgrade
        .max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| run_socket(socket, state, headers, subscriber)))
}

// Reads the x-subscriber-id header, missing, empty or too long returns 400 before the upgrade
fn subscriber_id(headers: &HeaderMap) -> Result<String, ApiError> {
    let value = headers
        .get(SUBSCRIBER_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.chars().count() <= MAX_SUBSCRIBER_ID_LENGTH);

    match value {
        Some(value) => Ok(value.to_string()),
        None => Err(ApiError::from_entry(bad_request::INVALID_HEADER_VALUES)),
    }
}

// Runs one connection from the first frame to the close
async fn run_socket(
    mut socket: WebSocket,
    state: AppState,
    headers: HeaderMap,
    subscriber: String,
) {
    // The auth result is sent as a frame after the upgrade, like the old gateway
    let identity = match authenticate(&state, &headers).await {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(
                target: "WsSubscribe",
                "AUTH_FAILED subscriber={subscriber} code={}",
                error.code().as_str()
            );
            let _ = socket
                .send(Message::Text(auth_error_frame(&error).into()))
                .await;
            close(&mut socket, CLOSE_AUTH_FAILED, "authentication failed").await;
            return;
        }
    };

    let (id, mut outbound) = state.subscribers.register(&identity.api_key_id);

    tracing::info!(
        target: "WsSubscribe",
        "CONNECTED subscriber={subscriber} id={id} active={}",
        state.subscribers.session_count()
    );

    let session = SubscriberSession::new(
        id,
        subscriber.clone(),
        state.topics.clone(),
        state.subscribers.clone(),
        state.publisher.clone(),
    );

    let exit = serve(&mut socket, &session, &mut outbound, state.shutdown.clone()).await;

    // Pending ACKs of this subscriber fail, so their messages are read again and nothing is committed
    let pending = state.subscribers.unregister(id);

    tracing::info!(
        target: "WsSubscribe",
        "DISCONNECTED subscriber={subscriber} id={id} pendingAcks={pending}"
    );

    match exit {
        Exit::Close(code, reason) => close(&mut socket, code, reason).await,
        Exit::NoAnswer => {
            tracing::warn!(
                target: "WsSubscribe",
                "PING_TIMEOUT subscriber={subscriber} id={id}, connection dropped"
            );
        }
        Exit::Gone => {}
    }
}

// The connection loop: frames for the client, messages from the client, pings and the shutdown signal
async fn serve(
    socket: &mut WebSocket,
    session: &SubscriberSession,
    outbound: &mut mpsc::Receiver<String>,
    mut shutdown: watch::Receiver<bool>,
) -> Exit {
    let ready = frame(serde_json::json!({ "type": "ready" }));
    if socket.send(Message::Text(ready.into())).await.is_err() {
        return Exit::Gone;
    }

    let mut keepalive = Keepalive::new(PING_INTERVAL);

    loop {
        if *shutdown.borrow() {
            return Exit::Close(CLOSE_GOING_AWAY, "server shutting down");
        }

        tokio::select! {
            queued = outbound.recv() => match queued {
                Some(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        return Exit::Gone;
                    }
                }
                // The registry dropped this subscriber because its queue was full
                None => return Exit::Close(CLOSE_TRY_AGAIN_LATER, "subscriber too slow"),
            },

            incoming = socket.recv() => match incoming {
                Some(Ok(message)) => {
                    keepalive.client_is_alive();

                    let reply = match message {
                        Message::Text(text) => Some(session.handle_text(text.as_str()).await),
                        Message::Binary(_) => Some(error_frame("Malformed JSON")),
                        Message::Close(_) => return Exit::Gone,
                        // Pings are answered by axum, pongs only prove the client is alive
                        Message::Ping(_) | Message::Pong(_) => None,
                    };

                    if let Some(reply) = reply
                        && socket.send(Message::Text(reply.into())).await.is_err()
                    {
                        return Exit::Gone;
                    }
                }
                Some(Err(_)) | None => return Exit::Gone,
            },

            beat = keepalive.next_beat() => match beat {
                Beat::Ping => {
                    if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                        return Exit::Gone;
                    }
                }
                Beat::Dead => return Exit::NoAnswer,
            },

            // A closed channel means the gateway is going away, which is the same as a shutdown
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Exit::Close(CLOSE_GOING_AWAY, "server shutting down");
                }
            }
        }
    }
}
