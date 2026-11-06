mod common;

use axum::http::StatusCode;
use common::{auth_headers, body_json, body_text, get};

// GET /api/v1/health/app with valid headers returns the app health envelope
#[tokio::test]
async fn app_health_returns_200_with_valid_headers() {
    let response = get("/api/v1/health/app", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["success"], true);
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["message"], "Application is running");
    assert_eq!(body["data"]["status"], "UP");
    assert!(body["data"]["uptimeMs"].is_u64());
    assert!(body["data"]["timestampUtc"].is_string());
}

// GET /api/v1/health/app without client headers returns 401
#[tokio::test]
async fn app_health_returns_401_without_headers() {
    let response = get("/api/v1/health/app", &[]).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body = body_json(response).await;

    assert_eq!(body["success"], false);
    assert_eq!(body["code"], "UNAUTHORIZED");
}

// GET /api/v1/health/app with a wrong secret returns 400
#[tokio::test]
async fn app_health_returns_400_with_wrong_secret() {
    let response = get(
        "/api/v1/health/app",
        &[("x-client-id", "test-client"), ("x-client-secret", "wrong")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = body_json(response).await;

    assert_eq!(body["code"], "BAD_REQUEST");
}

// GET /api/v1/health/db returns 503 while the database is a placeholder
#[tokio::test]
async fn db_health_returns_503_while_db_is_down() {
    let response = get("/api/v1/health/db", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = body_json(response).await;

    assert_eq!(body["code"], "SERVICE_UNAVAILABLE");
}

// GET /api/v1/health returns 503 while the database is a placeholder
#[tokio::test]
async fn unified_health_returns_503_while_db_is_down() {
    let response = get("/api/v1/health", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// GET /healthz/app returns html for a browser
#[tokio::test]
async fn healthz_app_returns_html_for_browsers() {
    let response = get(
        "/healthz/app",
        &[("accept", "text/html"), ("cookie", "auth=ok")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.starts_with("<!DOCTYPE html>"));
    assert!(text.contains("App Health"));
}

// GET /healthz/app returns json when Accept is not html
#[tokio::test]
async fn healthz_app_returns_json_for_api_clients() {
    let response = get(
        "/healthz/app",
        &[("accept", "application/json"), ("cookie", "auth=ok")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["status"], "UP");
}

// GET /healthz/db returns 200 json that reports the database as down
#[tokio::test]
async fn healthz_db_reports_down_with_200() {
    let response = get(
        "/healthz/db",
        &[("accept", "application/json"), ("cookie", "auth=ok")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["message"], "Database connection failed");
    assert_eq!(body["data"]["status"], "DOWN");
    assert_eq!(body["data"]["database"], "DISCONNECTED");
    assert!(body["data"]["latencyMs"].is_null());
}

// GET /healthz returns both the app and the db part as json
#[tokio::test]
async fn healthz_returns_app_and_db_json() {
    let response = get(
        "/healthz",
        &[("accept", "application/json"), ("cookie", "auth=ok")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["message"], "Health check OK");
    assert_eq!(body["data"]["app"]["status"], "UP");
    assert_eq!(body["data"]["db"]["status"], "DOWN");
}
