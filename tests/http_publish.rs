mod common;

use std::sync::Arc;

use axum::{body::Body, http::Response, http::StatusCode};
use common::{
    app_with, auth_headers, body_json,
    fakes::{FakePublisher, FakeStore},
    post_json_to, post_raw_to, state_with_publisher,
};
use kafka_api_gateway::errors::catalog::{
    bad_gateway, bad_request, forbidden, payload_too_large, unauthorized,
};
use serde_json::{Value, json};

// Credentials of the API key the fake store knows
const API_KEY: &str = "key-1";
const API_SECRET: &str = "s3cret";

// A valid UUID v7
const CORRELATION_ID: &str = "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f13";

// A UUID of version 4, not accepted as a correlation id
const UUID_V4: &str = "550e8400-e29b-41d4-a716-446655440000";

const PATH: &str = "/api/v1/http-publish";

// Fake store with one API key, the topic orders enabled and the topic retired disabled
fn store() -> FakeStore {
    FakeStore::new()
        .with_api_key(API_KEY, API_SECRET)
        .with_topic("orders", true, false, false, true)
        .with_topic("retired", true, false, false, false)
}

// Client credentials plus a valid API key and secret
fn publish_headers() -> Vec<(&'static str, &'static str)> {
    let mut headers = auth_headers().to_vec();
    headers.push(("x-api-key", API_KEY));
    headers.push(("x-api-secret", API_SECRET));

    headers
}

// A body that passes every rule
fn valid_body() -> Value {
    json!({
        "topic": "orders",
        "correlationId": CORRELATION_ID,
        "value": { "orderId": 42 }
    })
}

// Sends the body to the route with the given headers and publisher
async fn publish(
    publisher: &Arc<FakePublisher>,
    headers: &[(&str, &str)],
    body: &Value,
) -> Response<Body> {
    let app = app_with(state_with_publisher(store(), publisher.clone()));

    post_json_to(app, PATH, headers, body).await
}

// A valid request is accepted with 202 and the event is published with the correlation id as key
#[tokio::test]
async fn valid_request_is_published_and_returns_202() {
    let publisher = Arc::new(FakePublisher::new());

    let response = publish(&publisher, &publish_headers(), &valid_body()).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let json = body_json(response).await;
    assert_eq!(json["success"], true);
    assert_eq!(json["httpCode"], 202);
    assert_eq!(json["message"], "Event accepted");
    assert_eq!(json["data"]["topic"], "orders");
    assert_eq!(json["data"]["correlationId"], CORRELATION_ID);

    let sent = publisher.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].topic, "orders");
    assert_eq!(sent[0].key.as_deref(), Some(CORRELATION_ID));
    assert_eq!(sent[0].value, r#"{"orderId":42}"#);
}

// The value can be any JSON, including null
#[tokio::test]
async fn null_value_is_accepted() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "orders", "correlationId": CORRELATION_ID, "value": null });

    let response = publish(&publisher, &publish_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(publisher.sent()[0].value, "null");
}

// D5: a topic that is not in the registry is rejected and nothing is sent
#[tokio::test]
async fn unknown_topic_returns_403() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "missing", "correlationId": CORRELATION_ID, "value": {} });

    let response = publish(&publisher, &publish_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(response).await["error"],
        forbidden::ACCESS_DENIED.error
    );
    assert!(publisher.sent().is_empty());
}

// D5: a disabled topic is rejected like an unknown one
#[tokio::test]
async fn disabled_topic_returns_403() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "retired", "correlationId": CORRELATION_ID, "value": {} });

    let response = publish(&publisher, &publish_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(publisher.sent().is_empty());
}

// Without API key headers the route answers 401 and sends nothing
#[tokio::test]
async fn missing_api_key_returns_401() {
    let publisher = Arc::new(FakePublisher::new());

    let response = publish(&publisher, &auth_headers(), &valid_body()).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        body_json(response).await["error"],
        unauthorized::MISSING_API_KEY.error
    );
    assert!(publisher.sent().is_empty());
}

// A wrong API secret answers 401 and sends nothing
#[tokio::test]
async fn wrong_api_secret_returns_401() {
    let publisher = Arc::new(FakePublisher::new());
    let mut headers = auth_headers().to_vec();
    headers.push(("x-api-key", API_KEY));
    headers.push(("x-api-secret", "wrong-secret"));

    let response = publish(&publisher, &headers, &valid_body()).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        body_json(response).await["error"],
        unauthorized::INVALID_API_KEY.error
    );
    assert!(publisher.sent().is_empty());
}

// The API key is checked before the body, so a bad body with no key is still 401
#[tokio::test]
async fn api_key_is_checked_before_the_body() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "", "correlationId": "x", "value": 1 });

    let response = publish(&publisher, &auth_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// A correlation id that is not a UUID v7 returns 400
#[tokio::test]
async fn uuid_v4_correlation_id_returns_400() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "orders", "correlationId": UUID_V4, "value": {} });

    let response = publish(&publisher, &publish_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = body_json(response).await;
    assert_eq!(json["code"], "BAD_REQUEST");
    assert_eq!(json["error"], "\"correlationId\" must be a UUID v7");
    assert!(publisher.sent().is_empty());
}

// An empty topic returns 400
#[tokio::test]
async fn empty_topic_returns_400() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "", "correlationId": CORRELATION_ID, "value": {} });

    let response = publish(&publisher, &publish_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(response).await["error"],
        "\"topic\" must not be empty"
    );
}

// A body without the value field returns 400
#[tokio::test]
async fn missing_value_returns_400() {
    let publisher = Arc::new(FakePublisher::new());
    let body = json!({ "topic": "orders", "correlationId": CORRELATION_ID });

    let response = publish(&publisher, &publish_headers(), &body).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(publisher.sent().is_empty());
}

// A body that is not JSON returns 400
#[tokio::test]
async fn non_json_body_returns_400() {
    let publisher = Arc::new(FakePublisher::new());
    let app = app_with(state_with_publisher(store(), publisher.clone()));

    let response = post_raw_to(
        app,
        PATH,
        &publish_headers(),
        "application/json",
        "not json",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(body_json(response).await["error"].as_str().is_some());
}

// A message the broker rejects as too large returns 413
#[tokio::test]
async fn message_too_large_returns_413() {
    let publisher = Arc::new(FakePublisher::failing(
        payload_too_large::KAFKA_MESSAGE_TOO_LARGE,
    ));

    let response = publish(&publisher, &publish_headers(), &valid_body()).await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        body_json(response).await["error"],
        payload_too_large::KAFKA_MESSAGE_TOO_LARGE.error
    );
}

// A broker that cannot be reached returns 502
#[tokio::test]
async fn broker_down_returns_502() {
    let publisher = Arc::new(FakePublisher::failing(bad_gateway::UPSTREAM_UNAVAILABLE));

    let response = publish(&publisher, &publish_headers(), &valid_body()).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

// A wrong client secret is rejected before the API key is looked at
#[tokio::test]
async fn wrong_client_secret_returns_400() {
    let publisher = Arc::new(FakePublisher::new());
    let headers = [
        ("x-client-id", "test-client"),
        ("x-client-secret", "not-the-secret"),
        ("x-api-key", API_KEY),
        ("x-api-secret", API_SECRET),
    ];

    let response = publish(&publisher, &headers, &valid_body()).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(response).await["error"],
        bad_request::INVALID_HEADER_VALUES.error
    );
}
