mod common;

use std::sync::Arc;

use axum::{body::Body, http::Response, http::StatusCode};
use common::{
    KafkaFakes, app_with, auth_headers, body_json,
    fakes::{FakeReloader, FakeStore},
    post_json_to, state_with_kafka,
};
use kafka_api_gateway::{
    errors::catalog::{bad_gateway, bad_request, unauthorized},
    kafka::reloader::ReloadSummary,
};
use serde_json::json;

const PATH: &str = "/api/v1/topics/reload";

// Fake store with one API key
fn store() -> Arc<FakeStore> {
    Arc::new(FakeStore::new().with_api_key("key-1", "s3cret"))
}

// Client credentials plus a valid API key and secret
fn api_key_headers() -> Vec<(&'static str, &'static str)> {
    let mut headers = auth_headers().to_vec();
    headers.push(("x-api-key", "key-1"));
    headers.push(("x-api-secret", "s3cret"));

    headers
}

// Sends the reload request with the given headers
async fn reload(fakes: &KafkaFakes, headers: &[(&str, &str)]) -> Response<Body> {
    let app = app_with(state_with_kafka(store(), fakes));

    post_json_to(app, PATH, headers, &json!({})).await
}

// A valid API key triggers one reload with the api trigger and returns its summary
#[tokio::test]
async fn reload_returns_the_summary() {
    let mut fakes = KafkaFakes::new();
    fakes.reloader = Arc::new(FakeReloader::with_summary(ReloadSummary {
        enabled_topics: 11,
        consume_topics: 7,
        changed: true,
        missing_topics: vec!["ghost".to_string()],
    }));

    let response = reload(&fakes, &api_key_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response).await;
    assert_eq!(json["success"], true);
    assert_eq!(json["statusCode"], 200);
    assert_eq!(json["message"], "Topics reloaded");
    assert_eq!(json["data"]["enabledTopics"], 11);
    assert_eq!(json["data"]["consumeTopics"], 7);
    assert_eq!(json["data"]["changed"], true);
    assert_eq!(json["data"]["missingTopics"], json!(["ghost"]));
    assert_eq!(fakes.reloader.calls(), vec!["api"]);
}

// Without API key headers the route answers 401 and nothing is reloaded
#[tokio::test]
async fn missing_api_key_returns_401() {
    let fakes = KafkaFakes::new();

    let response = reload(&fakes, &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        body_json(response).await["error"],
        unauthorized::MISSING_API_KEY.error
    );
    assert!(fakes.reloader.calls().is_empty());
}

// A wrong API secret answers 401 and nothing is reloaded
#[tokio::test]
async fn wrong_api_secret_returns_401() {
    let fakes = KafkaFakes::new();
    let mut headers = auth_headers().to_vec();
    headers.push(("x-api-key", "key-1"));
    headers.push(("x-api-secret", "wrong"));

    let response = reload(&fakes, &headers).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        body_json(response).await["error"],
        unauthorized::INVALID_API_KEY.error
    );
    assert!(fakes.reloader.calls().is_empty());
}

// A wrong client secret is rejected before the API key is looked at
#[tokio::test]
async fn wrong_client_secret_returns_400() {
    let fakes = KafkaFakes::new();
    let headers = [
        ("x-client-id", "test-client"),
        ("x-client-secret", "not-the-secret"),
        ("x-api-key", "key-1"),
        ("x-api-secret", "s3cret"),
    ];

    let response = reload(&fakes, &headers).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(response).await["error"],
        bad_request::INVALID_HEADER_VALUES.error
    );
    assert!(fakes.reloader.calls().is_empty());
}

// A failed reload is returned as the error of the failure
#[tokio::test]
async fn failed_reload_returns_its_error() {
    let mut fakes = KafkaFakes::new();
    fakes.reloader = Arc::new(FakeReloader::failing(bad_gateway::UPSTREAM_UNAVAILABLE));

    let response = reload(&fakes, &api_key_headers()).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        body_json(response).await["error"],
        bad_gateway::UPSTREAM_UNAVAILABLE.error
    );
}
