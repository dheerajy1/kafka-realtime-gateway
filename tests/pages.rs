mod common;

use axum::http::{StatusCode, header};
use common::{body_json, body_text, get};

// GET / returns the Home page html
#[tokio::test]
async fn home_renders_html() {
    let response = get("/", &[("accept", "text/html")]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.starts_with("<!DOCTYPE html>"));
    assert!(text.contains("Welcome to Kafka realtime gateway"));
    assert!(text.contains("href=\"/healthz\""));
    assert!(text.contains("href=\"/openapi\""));
}

// GET /about returns the About page html
#[tokio::test]
async fn about_renders_html() {
    let response = get("/about", &[("accept", "text/html")]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.starts_with("<!DOCTYPE html>"));
    assert!(text.contains("<title>About | Kafka realtime gateway</title>"));
}

// GET /login returns the Login form with both inputs
#[tokio::test]
async fn login_renders_form() {
    let response = get("/login", &[("accept", "text/html")]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.contains("Kafka realtime gateway Login"));
    assert!(text.contains("name=\"username\""));
    assert!(text.contains("name=\"password\""));
}

// GET /healthz/db renders the database part as down in html
#[tokio::test]
async fn healthz_db_renders_down_in_html() {
    let response = get("/healthz/db", &[("accept", "text/html")]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.contains("Database Health"));
    assert!(text.contains("DOWN"));
    assert!(text.contains("DISCONNECTED"));
}

// The embedded stylesheet is served as css
#[tokio::test]
async fn global_css_is_served() {
    let response = get("/global.css", &[]).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/css; charset=utf-8"
    );
}

// The embedded logo and favicon are served with their image types
#[tokio::test]
async fn logo_and_favicon_are_served() {
    let logo = get("/logo.png", &[]).await;

    assert_eq!(logo.status(), StatusCode::OK);
    assert_eq!(
        logo.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png"
    );

    let favicon = get("/favicon.ico", &[]).await;

    assert_eq!(favicon.status(), StatusCode::OK);
    assert_eq!(
        favicon.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/x-icon"
    );
}

// GET /openapi/json returns the spec with the title and the health paths
#[tokio::test]
async fn openapi_json_lists_health_paths() {
    let response = get("/openapi/json", &[]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["info"]["title"], "Kafka Realtime Gateway API");
    assert!(body["paths"]["/api/v1/health"].is_object());
    assert!(body["paths"]["/api/v1/health/app"].is_object());
    assert!(body["paths"]["/api/v1/health/db"].is_object());
}

// GET /openapi serves the Scalar UI html
#[tokio::test]
async fn openapi_scalar_ui_is_served() {
    let response = get("/openapi", &[("accept", "text/html")]).await;

    assert_eq!(response.status(), StatusCode::OK);

    let text = body_text(response).await;

    assert!(text.contains("Kafka Realtime Gateway API"));
}
