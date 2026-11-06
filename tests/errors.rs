mod common;

use axum::{Router, body::Body, http::Request, http::StatusCode};
use common::{auth_headers, body_json, body_text, get, post};
use kafka_api_gateway::errors::panic::handle_panic;
use tower::ServiceExt;
use tower_http::catch_panic::CatchPanicLayer;

// Handler that always panics, used to test the panic handler
async fn boom() -> &'static str {
    panic!("secret panic detail");
}

// An unknown path returns the global 404 error shape
#[tokio::test]
async fn unknown_path_returns_404_error_shape() {
    let response = get("/api/v1/dummy", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = body_json(response).await;

    assert_eq!(body["success"], false);
    assert_eq!(body["statusCode"], 404);
    assert_eq!(body["code"], "NOT_FOUND");
    assert!(body["message"].is_string());
    assert!(body["error"].is_string());
}

// A known path with the wrong method returns the global 405 error shape
#[tokio::test]
async fn wrong_method_returns_405_error_shape() {
    let response = post("/api/v1/health/app", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

    let body = body_json(response).await;

    assert_eq!(body["success"], false);
    assert_eq!(body["statusCode"], 405);
    assert_eq!(body["code"], "METHOD_NOT_SUPPORTED");
}

// A panic in a handler returns the global 500 error shape and hides the panic text
#[tokio::test]
async fn panic_returns_500_error_shape_without_panic_text() {
    let router = Router::new()
        .route("/boom", axum::routing::get(boom))
        .layer(CatchPanicLayer::custom(handle_panic));

    let response = router
        .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let text = body_text(response).await;

    assert!(!text.contains("secret panic detail"));

    let body: serde_json::Value = serde_json::from_str(&text).unwrap();

    assert_eq!(body["success"], false);
    assert_eq!(body["statusCode"], 500);
    assert_eq!(body["code"], "INTERNAL_SERVER_ERROR");
}

// An empty x-client-id fails header validation with 400
#[tokio::test]
async fn empty_client_id_returns_400() {
    let response = get(
        "/api/v1/health/app",
        &[("x-client-id", ""), ("x-client-secret", "test-secret")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = body_json(response).await;

    assert_eq!(body["code"], "BAD_REQUEST");
}

// An x-client-id longer than 44 characters fails header validation with 400
#[tokio::test]
async fn too_long_client_id_returns_400() {
    let long_id = "a".repeat(45);

    let response = get(
        "/api/v1/health/app",
        &[
            ("x-client-id", long_id.as_str()),
            ("x-client-secret", "test-secret"),
        ],
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// An x-client-secret longer than 100 characters fails header validation with 400
#[tokio::test]
async fn too_long_client_secret_returns_400() {
    let long_secret = "a".repeat(101);

    let response = get(
        "/api/v1/health/app",
        &[
            ("x-client-id", "test-client"),
            ("x-client-secret", long_secret.as_str()),
        ],
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
