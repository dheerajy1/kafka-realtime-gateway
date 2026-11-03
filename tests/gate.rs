mod common;

use axum::http::StatusCode;
use common::{app, auth_headers, get_to, header_value};

// Header a browser sends when it asks for a page
const BROWSER: (&str, &str) = ("accept", "text/html");

// Cookie the portal login sets
const COOKIE: (&str, &str) = ("cookie", "auth=ok");

// Browsers without the cookie are redirected to the login page on every protected page
#[tokio::test]
async fn browser_without_cookie_is_redirected_to_login() {
    for path in [
        "/",
        "/about",
        "/openapi",
        "/healthz",
        "/healthz/app",
        "/healthz/db",
    ] {
        let response = get_to(app(), path, &[BROWSER]).await;

        assert_eq!(response.status(), StatusCode::FOUND, "path {path}");
        assert_eq!(
            header_value(&response, "location").as_deref(),
            Some("/login"),
            "path {path}"
        );
    }
}

// A valid cookie lets a browser through
#[tokio::test]
async fn valid_cookie_passes_the_gate() {
    let response = get_to(app(), "/", &[BROWSER, COOKIE]).await;

    assert_eq!(response.status(), StatusCode::OK);
}

// The cookie is found among other cookies
#[tokio::test]
async fn cookie_among_other_cookies_passes_the_gate() {
    let response = get_to(
        app(),
        "/",
        &[BROWSER, ("cookie", "theme=dark; auth=ok; lang=en")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
}

// A cookie with another value does not pass
#[tokio::test]
async fn wrong_cookie_value_is_redirected() {
    let response = get_to(app(), "/", &[BROWSER, ("cookie", "auth=no")]).await;

    assert_eq!(response.status(), StatusCode::FOUND);
}

// The login page needs no cookie
#[tokio::test]
async fn login_page_needs_no_cookie() {
    let response = get_to(app(), "/login", &[BROWSER]).await;

    assert_eq!(response.status(), StatusCode::OK);
}

// Paths with a file extension need no cookie
#[tokio::test]
async fn static_assets_need_no_cookie() {
    for path in ["/global.css", "/logo.png", "/favicon.ico"] {
        let response = get_to(app(), path, &[BROWSER]).await;

        assert_eq!(response.status(), StatusCode::OK, "path {path}");
    }
}

// Callers that are not browsers and send valid client credentials pass without a cookie
#[tokio::test]
async fn valid_client_credentials_pass_the_gate() {
    let response = get_to(app(), "/healthz/app", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);

    let response = get_to(app(), "/openapi/json", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);
}

// Without credentials or a cookie the same pages are redirected
#[tokio::test]
async fn missing_client_credentials_are_redirected_on_pages() {
    let response = get_to(app(), "/healthz/app", &[]).await;

    assert_eq!(response.status(), StatusCode::FOUND);

    let response = get_to(app(), "/openapi/json", &[]).await;

    assert_eq!(response.status(), StatusCode::FOUND);
}

// Wrong client credentials do not pass the gate on a page, they are redirected, not rejected with 400
#[tokio::test]
async fn wrong_client_credentials_are_redirected_on_pages() {
    let response = get_to(
        app(),
        "/healthz/app",
        &[("x-client-id", "test-client"), ("x-client-secret", "wrong")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::FOUND);
}

// A browser that asks for html is not allowed through just because the path is under /api
#[tokio::test]
async fn api_path_with_html_accept_and_no_cookie_is_redirected() {
    let response = get_to(app(), "/api/v1/health/app", &[BROWSER]).await;

    assert_eq!(response.status(), StatusCode::FOUND);
}

// An API call without html accept reaches the route, which returns its own JSON error
#[tokio::test]
async fn api_path_without_html_accept_reaches_the_route() {
    let response = get_to(app(), "/api/v1/health/app", &[]).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// A WebSocket upgrade request passes the gate
#[tokio::test]
async fn websocket_upgrade_passes_the_gate() {
    let response = get_to(app(), "/", &[BROWSER, ("upgrade", "websocket")]).await;

    assert_eq!(response.status(), StatusCode::OK);
}
