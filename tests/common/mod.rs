#![allow(dead_code)]

pub mod fakes;

use std::sync::{Arc, Once};

use axum::{
    Router,
    body::Body,
    http::{Request, Response},
};
use http_body_util::BodyExt;
use kafka_api_gateway::state::AppState;
use serde_json::Value;
use tower::ServiceExt;

use self::fakes::FakeStore;

// Credentials the tests configure and send
pub const TEST_CLIENT_ID: &str = "test-client";
pub const TEST_CLIENT_SECRET: &str = "test-secret";

static INIT: Once = Once::new();

// Sets the env variables once, before the global env is first read
pub fn setup_env() {
    INIT.call_once(|| {
        // Runs before any router is built, so no other thread reads the env yet
        unsafe {
            std::env::set_var("APP_PORT", "5000");
            std::env::set_var("JWT_SECRET", "test-jwt-secret");
            std::env::set_var("CLIENT_ID", TEST_CLIENT_ID);
            std::env::set_var("CLIENT_SECRET", TEST_CLIENT_SECRET);
            std::env::set_var("APP_NAME", "test-app");
            std::env::set_var("ONPREM_DB_SERVER", "test-onprem");
            std::env::set_var("ONPREM_DB_PORT", "5432");
            std::env::set_var("ONPREM_DB_DATABASE", "test-onprem-db");
            std::env::set_var("ONPREM_DB_USER_PUBLIC", "test-public-user");
            std::env::set_var("ONPREM_DB_PASSWORD_PUBLIC", "test-public-password");
            std::env::set_var("ONPREM_DB_USER_SERVICE", "test-service-user");
            std::env::set_var("ONPREM_DB_PASSWORD_SERVICE", "test-service-password");
            std::env::set_var("KAFKA_BROKERS", "127.0.0.1:9");
            std::env::set_var("KAFKA_CLIENT_ID", "test-client-id");
            std::env::set_var("KAFKA_GROUP_ID", "test-group-id");
        }
    });
}

// Wraps one fake store as the app state, it plays all three database roles
pub fn state_from(store: FakeStore) -> AppState {
    let store = Arc::new(store);

    AppState::new(store.clone(), store.clone(), store)
}

// Default test state, an empty fake store with the database down
pub fn default_state() -> AppState {
    state_from(FakeStore::new())
}

// Builds the real application router with the test env and the given state
pub fn app_with(state: AppState) -> Router {
    setup_env();
    kafka_api_gateway::app::router(state)
}

// Builds the real application router with the default test state
pub fn app() -> Router {
    app_with(default_state())
}

// Valid client credential headers
pub fn auth_headers() -> [(&'static str, &'static str); 2] {
    [
        ("x-client-id", TEST_CLIENT_ID),
        ("x-client-secret", TEST_CLIENT_SECRET),
    ]
}

// Sends a request with the given method and headers through a fresh router
async fn send(method: &str, uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
    let mut request = Request::builder().method(method).uri(uri);

    for (name, value) in headers {
        request = request.header(*name, *value);
    }

    app()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

// Sends a GET request with the given headers through a fresh router
pub async fn get(uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
    send("GET", uri, headers).await
}

// Sends a POST request with the given headers through a fresh router
pub async fn post(uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
    send("POST", uri, headers).await
}

// Reads the whole response body as text
pub async fn body_text(response: Response<Body>) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// Reads the whole response body as json
pub async fn body_json(response: Response<Body>) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// Sends one request with a body through the given router, the router is consumed so build one per request
pub async fn send_to(
    app: Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Response<Body> {
    let mut request = Request::builder().method(method).uri(uri);

    for (name, value) in headers {
        request = request.header(*name, *value);
    }

    app.oneshot(request.body(body).unwrap()).await.unwrap()
}

// Sends a GET request through the given router
pub async fn get_to(app: Router, uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
    send_to(app, "GET", uri, headers, Body::empty()).await
}

// Sends a POST request with a JSON body through the given router
pub async fn post_json_to(
    app: Router,
    uri: &str,
    headers: &[(&str, &str)],
    json: &Value,
) -> Response<Body> {
    let mut all_headers = headers.to_vec();
    all_headers.push(("content-type", "application/json"));

    send_to(app, "POST", uri, &all_headers, Body::from(json.to_string())).await
}

// Sends a POST request with a raw body and content type through the given router
pub async fn post_raw_to(
    app: Router,
    uri: &str,
    headers: &[(&str, &str)],
    content_type: &str,
    body: &str,
) -> Response<Body> {
    let mut all_headers = headers.to_vec();
    all_headers.push(("content-type", content_type));

    send_to(app, "POST", uri, &all_headers, Body::from(body.to_string())).await
}

// Sends a browser style form POST through the given router
pub async fn post_form_to(
    app: Router,
    uri: &str,
    headers: &[(&str, &str)],
    form: &str,
) -> Response<Body> {
    post_raw_to(app, uri, headers, "application/x-www-form-urlencoded", form).await
}

// Reads one response header as text
pub fn header_value(response: &Response<Body>, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}
