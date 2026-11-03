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
use tokio::sync::watch;

use super::session::PublishSession;
use crate::{
    state::AppState,
    ws::{
        CLOSE_AUTH_FAILED, CLOSE_GOING_AWAY, Exit, MAX_MESSAGE_SIZE, auth_error_frame,
        authenticate, close, error_frame, frame,
        keepalive::{Beat, Keepalive, PING_INTERVAL},
    },
};

// Routes of the ws-publish feature, mounted under /api/v1
pub fn router() -> Router<AppState> {
    Router::new().route("/ws-publish", get(ws_publish))
}

// GET /api/v1/ws-publish, the error responses are added in openapi/errors.rs under the operation id ws_publish
#[utoipa::path(
    get,
    path = "/api/v1/ws-publish",
    operation_id = "ws_publish",
    tag = "WebSocket",
    summary = "Publish jobs to Kafka over a WebSocket",
    description = "Upgrades to a WebSocket. The client id, client secret, API key and API secret are checked after the upgrade: a failure sends an error frame and closes with code 4001. On success the server sends {\"type\":\"ready\"}. The client sends {\"topic\":\"...\",\"jobName\":\"...\"}. The gateway publishes {correlationId, apiUserId, jobName, timestamp} with the API key id as the Kafka key and answers {\"type\":\"ack\",\"topic\":\"...\",\"correlationId\":\"...\"}. The ack means published, not processed. The topic must be enabled in the gateway's topic registry.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
        ("x-api-key" = String, Header, description = "API key id"),
        ("x-api-secret" = String, Header, description = "API key secret"),
    ),
    responses(
        (status = 101, description = "Switching Protocols, the WebSocket is open")
    )
)]

// GET /api/v1/ws-publish
pub async fn ws_publish(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| run_socket(socket, state, headers))
}

// Runs one connection from the first frame to the close
async fn run_socket(mut socket: WebSocket, state: AppState, headers: HeaderMap) {
    // D6: the API key is checked when the connection opens, a failure never reaches the publish code
    let identity = match authenticate(&state, &headers).await {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(
                target: "WsPublish",
                "AUTH_FAILED code={}",
                error.code().as_str()
            );
            let _ = socket
                .send(Message::Text(auth_error_frame(&error).into()))
                .await;
            close(&mut socket, CLOSE_AUTH_FAILED, "authentication failed").await;
            return;
        }
    };

    let api_key_id = identity.api_key_id.clone();

    tracing::info!(target: "WsPublish", "CONNECTED apiKey={api_key_id}");

    let session = PublishSession::new(identity, state.topics.clone(), state.publisher.clone());

    let exit = serve(&mut socket, &session, state.shutdown.clone()).await;

    tracing::info!(target: "WsPublish", "DISCONNECTED apiKey={api_key_id}");

    match exit {
        Exit::Close(code, reason) => close(&mut socket, code, reason).await,
        Exit::NoAnswer => {
            tracing::warn!(
                target: "WsPublish",
                "PING_TIMEOUT apiKey={api_key_id}, connection dropped"
            );
        }
        Exit::Gone => {}
    }
}

// The connection loop: messages from the client, pings and the shutdown signal
async fn serve(
    socket: &mut WebSocket,
    session: &PublishSession,
    mut shutdown: watch::Receiver<bool>,
) -> Exit {
    let ready = frame(serde_json::json!({
        "type": "ready",
        "message": "WebSocket publish connection established",
    }));
    if socket.send(Message::Text(ready.into())).await.is_err() {
        return Exit::Gone;
    }

    let mut keepalive = Keepalive::new(PING_INTERVAL);

    loop {
        if *shutdown.borrow() {
            return Exit::Close(CLOSE_GOING_AWAY, "server shutting down");
        }

        tokio::select! {
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
