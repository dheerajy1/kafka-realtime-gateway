#![allow(dead_code)]

use std::sync::Once;

use axum::{
    Router,
    body::Body,
    http::{Request, Response},
};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

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

// Builds the real application router with the test env
pub fn app() -> Router {
    setup_env();
    kafka_api_gateway::app::router()
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
