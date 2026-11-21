use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use super::no_auth::check_client_credentials;
use crate::pages::render::wants_html;

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
    check_client_credentials(headers).is_ok()
}

// True when the Cookie header carries the portal login cookie
fn has_auth_cookie(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .any(|pair| pair.trim().split_once('=') == Some((AUTH_COOKIE_NAME, AUTH_COOKIE_VALUE)))
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

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    // Builds a header map with one Cookie header
    fn cookie(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();

        headers.insert(header::COOKIE, HeaderValue::from_static(value));

        headers
    }

    // Builds a header map with one Upgrade header
    fn upgrade(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();

        headers.insert(header::UPGRADE, HeaderValue::from_static(value));

        headers
    }

    // The login cookie is found alone, next to other cookies and in a second Cookie header
    #[test]
    fn auth_cookie_is_found() {
        assert!(has_auth_cookie(&cookie("auth=ok")));
        assert!(has_auth_cookie(&cookie("theme=dark; auth=ok")));
        assert!(has_auth_cookie(&cookie("auth=ok; theme=dark")));

        let mut two = cookie("theme=dark");
        two.append(header::COOKIE, HeaderValue::from_static("auth=ok"));

        assert!(has_auth_cookie(&two));
    }

    // A wrong value, a longer name, a longer value or no cookie at all is not the login cookie
    #[test]
    fn other_cookies_are_not_the_auth_cookie() {
        assert!(!has_auth_cookie(&cookie("auth=no")));
        assert!(!has_auth_cookie(&cookie("xauth=ok")));
        assert!(!has_auth_cookie(&cookie("auth=ok=more")));
        assert!(!has_auth_cookie(&cookie("auth=")));
        assert!(!has_auth_cookie(&cookie("auth")));
        assert!(!has_auth_cookie(&HeaderMap::new()));
    }

    // The cookie the login sends is the cookie the gate looks for
    #[test]
    fn login_cookie_passes_the_gate_check() {
        let set_cookie = auth_cookie_header();
        let pair = set_cookie.split(';').next().unwrap_or_default();

        assert_eq!(pair, "auth=ok");
        assert!(set_cookie.contains("HttpOnly"));
    }

    // Paths that end in a file extension are static files
    #[test]
    fn file_extensions_are_found() {
        assert!(has_file_extension("/global.css"));
        assert!(has_file_extension("/public/logo.PNG"));
        assert!(has_file_extension("/archive.tar.gz"));
    }

    // API paths, paths without a dot, an empty extension and a dot in a folder name are not files
    #[test]
    fn other_paths_are_not_files() {
        assert!(!has_file_extension("/api/v1/health"));
        assert!(!has_file_extension("/openapi"));
        assert!(!has_file_extension("/file."));
        assert!(!has_file_extension("/some.dir/file"));
        assert!(!has_file_extension(""));
    }

    // An Upgrade header with the value websocket is found in any letter case
    #[test]
    fn websocket_upgrade_is_found() {
        assert!(is_websocket_upgrade(&upgrade("websocket")));
        assert!(is_websocket_upgrade(&upgrade("WebSocket")));
    }

    // Another Upgrade value or no header is not a WebSocket upgrade
    #[test]
    fn other_upgrades_are_not_websocket() {
        assert!(!is_websocket_upgrade(&upgrade("h2c")));
        assert!(!is_websocket_upgrade(&HeaderMap::new()));
    }
}
