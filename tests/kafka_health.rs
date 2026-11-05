mod common;

use std::{sync::Arc, time::Duration};

use axum::{body::Body, http::Response, http::StatusCode};
use common::{
    KafkaFakes, app_with, auth_headers, body_json, body_text,
    fakes::{FakeKafkaProbe, FakeStore},
    get_to, state_with_kafka,
};
use kafka_api_gateway::kafka::health::{CachedKafkaProbe, KafkaProbe, KafkaStatus};

// Headers of a browser that passed the portal gate
const BROWSER: [(&str, &str); 2] = [("accept", "text/html"), ("cookie", "auth=ok")];

// Headers of an API client that passed the portal gate
const JSON: [(&str, &str); 2] = [("accept", "application/json"), ("cookie", "auth=ok")];

// Sends a GET request to an app whose database is up and whose Kafka probe is the given fake
async fn get_with(fakes: &KafkaFakes, uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
    let state = state_with_kafka(Arc::new(FakeStore::new().with_db_up()), fakes);

    get_to(app_with(state), uri, headers).await
}

// The unified health carries the small Kafka block
#[tokio::test]
async fn unified_health_returns_the_kafka_block() {
    let fakes = KafkaFakes::new();

    let response = get_with(&fakes, "/api/v1/health", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["data"]["kafka"]["status"], "UP");
    assert_eq!(body["data"]["kafka"]["brokerCount"], 1);
    assert_eq!(body["data"]["kafka"]["latencyMs"], 2);
}

// A Kafka broker that is down fails the unified health with 503
#[tokio::test]
async fn unified_health_returns_503_while_kafka_is_down() {
    let fakes = KafkaFakes::new();
    fakes.probe.set_up(false);

    let response = get_with(&fakes, "/api/v1/health", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = body_json(response).await;

    assert_eq!(body["success"], false);
    assert_eq!(body["code"], "SERVICE_UNAVAILABLE");
}

// The dedicated endpoint returns the full result
#[tokio::test]
async fn kafka_health_returns_the_full_result() {
    let fakes = KafkaFakes::new();

    let response = get_with(&fakes, "/api/v1/health/kafka", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;
    let data = &body["data"];

    assert_eq!(body["message"], "Kafka connection successful");
    assert_eq!(data["status"], "UP");
    assert_eq!(data["latencyMs"], 2);
    assert!(data["checkedAt"].is_string());
    assert!(data["error"].is_null());
    assert_eq!(data["cluster"]["clusterId"], "test-cluster");
    assert_eq!(data["cluster"]["controllerId"], 1);
    assert_eq!(data["cluster"]["brokers"][0]["host"], "kafka");
    assert_eq!(data["gateway"]["consumer"]["running"], true);
    assert_eq!(data["gateway"]["consumer"]["stalled"], false);
    assert_eq!(data["gateway"]["consumedTopics"], 2);
    assert_eq!(data["gateway"]["connectedSubscribers"], 0);
    assert_eq!(data["topics"][0]["name"], "gateway-test");
    assert_eq!(data["topics"][0]["underReplicatedPartitions"], 0);
    assert_eq!(data["consumerGroup"]["state"], "Stable");
    assert_eq!(data["disks"][0]["logDirs"][0]["usableBytes"], 400);
    assert_eq!(data["warnings"], serde_json::json!([]));
}

// The dedicated endpoint returns 503 while the broker is down
#[tokio::test]
async fn kafka_health_returns_503_while_kafka_is_down() {
    let fakes = KafkaFakes::new();
    fakes.probe.set_up(false);

    let response = get_with(&fakes, "/api/v1/health/kafka", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = body_json(response).await;

    assert_eq!(body["code"], "SERVICE_UNAVAILABLE");
}

// The dedicated endpoint needs the client credentials
#[tokio::test]
async fn kafka_health_returns_401_without_headers() {
    let fakes = KafkaFakes::new();

    let response = get_with(&fakes, "/api/v1/health/kafka", &[]).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// The portal health returns the Kafka block as json
#[tokio::test]
async fn healthz_returns_the_kafka_block() {
    let fakes = KafkaFakes::new();

    let response = get_with(&fakes, "/healthz", &JSON).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["data"]["kafka"]["status"], "UP");
    assert_eq!(body["data"]["kafka"]["brokerCount"], 1);
}

// The portal health stays 200 while the broker is down and shows it in the body
#[tokio::test]
async fn healthz_reports_kafka_down_with_200() {
    let fakes = KafkaFakes::new();
    fakes.probe.set_up(false);

    let response = get_with(&fakes, "/healthz", &JSON).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["data"]["kafka"]["status"], "DOWN");
    assert!(body["data"]["kafka"]["brokerCount"].is_null());
}

// The dedicated portal page returns 200 json that reports the broker as down
#[tokio::test]
async fn healthz_kafka_reports_down_with_200() {
    let fakes = KafkaFakes::new();
    fakes.probe.set_up(false);

    let response = get_with(&fakes, "/healthz/kafka", &JSON).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["message"], "Kafka connection failed");
    assert_eq!(body["data"]["status"], "DOWN");
    assert_eq!(body["data"]["error"], "connection refused");
    assert!(body["data"]["cluster"].is_null());
}

// The dedicated portal page renders every section in html
#[tokio::test]
async fn healthz_kafka_renders_the_sections_in_html() {
    let fakes = KafkaFakes::new();

    let response = get_with(&fakes, "/healthz/kafka", &BROWSER).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.starts_with("<!DOCTYPE html>"));
    assert!(text.contains("Kafka Health"));
    assert!(text.contains("test-cluster"));
    assert!(text.contains("gateway-test"));
    assert!(text.contains("Stable"));
    assert!(text.contains("/var/lib/kafka/data"));
}

// The dedicated portal page shows why the broker is down in html
#[tokio::test]
async fn healthz_kafka_renders_the_error_in_html_while_down() {
    let fakes = KafkaFakes::new();
    fakes.probe.set_up(false);

    let response = get_with(&fakes, "/healthz/kafka", &BROWSER).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.contains("DOWN"));
    assert!(text.contains("connection refused"));
}

// A second full check inside the ttl reuses the first result
#[tokio::test]
async fn cache_reuses_the_result_within_the_ttl() {
    let probe = Arc::new(FakeKafkaProbe::new());
    let cached = CachedKafkaProbe::new(probe.clone(), Duration::from_secs(60));

    cached.full().await;
    cached.full().await;

    assert_eq!(probe.full_calls(), 1);
}

// A full check after the ttl runs again
#[tokio::test]
async fn cache_refreshes_after_the_ttl() {
    let probe = Arc::new(FakeKafkaProbe::new());
    let cached = CachedKafkaProbe::new(probe.clone(), Duration::from_millis(20));

    cached.full().await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    cached.full().await;

    assert_eq!(probe.full_calls(), 2);
}

// Inside the ttl the cached result is returned even if the broker went down meanwhile
#[tokio::test]
async fn cache_keeps_the_result_inside_the_ttl() {
    let probe = Arc::new(FakeKafkaProbe::new());
    let cached = CachedKafkaProbe::new(probe.clone(), Duration::from_secs(60));

    cached.full().await;
    probe.set_up(false);

    assert_eq!(cached.full().await.status, KafkaStatus::Up);
}

// The summary is never cached and never touches the full check
#[tokio::test]
async fn summary_is_never_cached() {
    let probe = Arc::new(FakeKafkaProbe::new());
    let cached = CachedKafkaProbe::new(probe.clone(), Duration::from_secs(60));

    assert_eq!(cached.summary().await.status, KafkaStatus::Up);

    probe.set_up(false);

    assert_eq!(cached.summary().await.status, KafkaStatus::Down);
    assert_eq!(probe.full_calls(), 0);
}
