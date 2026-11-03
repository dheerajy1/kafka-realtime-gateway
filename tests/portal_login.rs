mod common;

use axum::http::StatusCode;
use common::{
    app_with, body_json, body_text, fakes::FakeStore, get_to, header_value, post_form_to,
    post_raw_to, state_from,
};
use kafka_api_gateway::{
    errors::catalog::{bad_request, internal_server_error, unauthorized},
    state::AppState,
};

// Header a browser sends when it submits a form
const BROWSER: (&str, &str) = ("accept", "text/html");

// State with one portal user, the password is secret123
fn state() -> AppState {
    state_from(FakeStore::new().with_portal_user("admin", "secret123"))
}

// The page shows an error toast
fn has_toast(html: &str) -> bool {
    html.contains("role=\"alert\"")
}

// The login page opens without a toast
#[tokio::test]
async fn login_page_opens_without_a_toast() {
    let response = get_to(app_with(state()), "/login", &[BROWSER]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let html = body_text(response).await;

    assert!(!has_toast(&html));
}

// A caller that does not ask for html gets the global 403 error shape
#[tokio::test]
async fn post_without_html_accept_returns_403_error_shape() {
    let response = post_form_to(
        app_with(state()),
        "/login",
        &[],
        "username=admin&password=secret123",
    )
    .await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let body = body_json(response).await;

    assert_eq!(body["success"], false);
    assert_eq!(body["httpCode"], 403);
}

// An empty username returns 400 and the page with the input error toast
#[tokio::test]
async fn empty_username_returns_400_with_toast() {
    let response = post_form_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "username=&password=secret123",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        header_value(&response, "content-type")
            .unwrap()
            .starts_with("text/html")
    );

    let html = body_text(response).await;

    assert!(has_toast(&html));
    assert!(html.contains(bad_request::INVALID_INPUT.message));
}

// A password shorter than 6 characters returns 400
#[tokio::test]
async fn short_password_returns_400() {
    let response = post_form_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "username=admin&password=12345",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// A missing field returns 400
#[tokio::test]
async fn missing_password_field_returns_400() {
    let response = post_form_to(app_with(state()), "/login", &[BROWSER], "username=admin").await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let html = body_text(response).await;

    assert!(has_toast(&html));
}

// A body that is not a form returns 400
#[tokio::test]
async fn json_body_returns_400() {
    let response = post_raw_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "application/json",
        "{\"username\":\"admin\",\"password\":\"secret123\"}",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// An unknown user returns 401 with the credentials toast and no cookie
#[tokio::test]
async fn unknown_user_returns_401_without_cookie() {
    let response = post_form_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "username=nobody&password=secret123",
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(header_value(&response, "set-cookie").is_none());

    let html = body_text(response).await;

    assert!(has_toast(&html));
    assert!(html.contains(unauthorized::INVALID_CREDENTIALS.message));
}

// A wrong password returns the same 401 as an unknown user
#[tokio::test]
async fn wrong_password_returns_401_without_cookie() {
    let response = post_form_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "username=admin&password=wrongpass",
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(header_value(&response, "set-cookie").is_none());

    let html = body_text(response).await;

    assert!(html.contains(unauthorized::INVALID_CREDENTIALS.message));
}

// A database failure returns 500 with the safe message, never the database error text
#[tokio::test]
async fn database_failure_returns_500_with_safe_message() {
    let broken = state_from(
        FakeStore::new()
            .with_portal_user("admin", "secret123")
            .broken(),
    );

    let response = post_form_to(
        app_with(broken),
        "/login",
        &[BROWSER],
        "username=admin&password=secret123",
    )
    .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let html = body_text(response).await;

    assert!(has_toast(&html));
    assert!(html.contains(internal_server_error::SERVER_ERROR.message));
    assert!(!html.contains(internal_server_error::DATABASE_ERROR.message));
}

// A correct login redirects to the home page and sets the cookie with all its flags
#[tokio::test]
async fn correct_login_redirects_and_sets_the_cookie() {
    let response = post_form_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "username=admin&password=secret123",
    )
    .await;

    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(header_value(&response, "location").as_deref(), Some("/"));

    let cookie = header_value(&response, "set-cookie").unwrap();

    assert!(cookie.starts_with("auth=ok"));

    for flag in [
        "HttpOnly",
        "SameSite=Strict",
        "Secure",
        "Path=/",
        "Max-Age=86400",
    ] {
        assert!(cookie.contains(flag), "cookie {cookie} is missing {flag}");
    }
}

// The cookie from a correct login opens a protected page
#[tokio::test]
async fn cookie_from_login_opens_a_protected_page() {
    let login = post_form_to(
        app_with(state()),
        "/login",
        &[BROWSER],
        "username=admin&password=secret123",
    )
    .await;

    let cookie = header_value(&login, "set-cookie").unwrap();
    let pair = cookie.split(';').next().unwrap().to_string();

    let response = get_to(app_with(state()), "/", &[BROWSER, ("cookie", &pair)]).await;

    assert_eq!(response.status(), StatusCode::OK);
}
