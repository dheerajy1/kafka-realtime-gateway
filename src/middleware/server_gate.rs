use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use validator::Validate;

use crate::{
    config::env::env,
    pages::render::wants_html,
    schemas::auth::{CLIENT_ID_HEADER, CLIENT_SECRET_HEADER, NoAuthHeaders},
};

// Name of the portal login cookie
pub const AUTH_COOKIE_NAME: &str = "auth";

// Value of the portal login cookie, plain and unsigned for now
pub const AUTH_COOKIE_VALUE: &str = "ok";

// The portal login cookie lives for one day
const AUTH_COOKIE_MAX_AGE_SECONDS: u32 = 60 * 60 * 24;

// Builds the Set-Cookie header value the portal login sends
pub fn auth_cookie_header() -> String {
    format!(
        "{AUTH_COOKIE_NAME}={AUTH_COOKIE_VALUE}; HttpOnly; SameSite=Strict; Secure; Path=/; Max-Age={AUTH_COOKIE_MAX_AGE_SECONDS}"
    )
}

// True when the request is a WebSocket upgrade
fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

// True when the client id and secret headers are present, well formed and match the configured values
fn has_valid_client_credentials(headers: &HeaderMap) -> bool {
    let read = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());

    let (Some(client_id), Some(client_secret)) =
        (read(CLIENT_ID_HEADER), read(CLIENT_SECRET_HEADER))
    else {
        return false;
    };

    let credentials = NoAuthHeaders::new(client_id, client_secret);

    if credentials.validate().is_err() {
        return false;
    }

    let env = env();

    credentials.client_id == env.client_id && credentials.client_secret == env.client_secret
}

// True when the Cookie header carries the portal login cookie
fn has_auth_cookie(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .any(|pair| pair.trim() == format!("{AUTH_COOKIE_NAME}={AUTH_COOKIE_VALUE}"))
}

// True when the path ends in a file extension like .css or .png
fn has_file_extension(path: &str) -> bool {
    path.rsplit_once('.').is_some_and(|(_, extension)| {
        !extension.is_empty() && extension.chars().all(|c| c.is_ascii_alphanumeric())
    })
}

// Decides whether a request may continue, the checks run in the order of build.md 2.7
fn is_allowed(request: &Request) -> bool {
    let headers = request.headers();
    let path = request.uri().path();

    // 1. WebSocket upgrades and the subscribe path pass, the route authenticates itself
    if is_websocket_upgrade(headers) || path == "/api/v1/ws-subscribe" {
        return true;
    }

    // 2. Callers that are not browsers and send valid client credentials pass
    if !wants_html(headers) && has_valid_client_credentials(headers) {
        return true;
    }

    // 3. The login page and its form post pass
    if path.starts_with("/login") {
        return true;
    }

    // 4. Browsers with the login cookie pass
    if has_auth_cookie(headers) {
        return true;
    }

    // 5. Paths with a file extension pass, these are static assets
    if has_file_extension(path) {
        return true;
    }

    // 6. API paths that are not asked for by a browser pass, the route returns its own JSON 401 or 400
    path.starts_with("/api/") && !wants_html(headers)
}

// Gate in front of every route, anything that is not allowed is redirected to the login page
pub async fn server_gate(request: Request, next: Next) -> Response {
    if is_allowed(&request) {
        return next.run(request).await;
    }

    (StatusCode::FOUND, [(header::LOCATION, "/login")]).into_response()
}
