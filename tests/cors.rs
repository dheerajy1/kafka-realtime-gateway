mod common;

use axum::{
    body::Body,
    http::{Request, Response, StatusCode, header},
};
use common::{app, auth_headers, get};
use tower::ServiceExt;

// Reads a response header as text
fn header_text(response: &Response<Body>, name: header::HeaderName) -> Option<&str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

// Sends a GET to a health route with client headers and the given Origin
async fn get_with_origin(origin: &str) -> Response<Body> {
    // Three headers are always sent, so a fixed array is enough
    let [client_id, client_secret] = auth_headers();
    let headers = [("origin", origin), client_id, client_secret];

    get("/api/v1/health/app", &headers).await
}

// Sends a preflight request with the given Origin
async fn preflight(origin: &str) -> Response<Body> {
    let request = Request::builder()
        .method("OPTIONS")
        .uri("/api/v1/http-publish")
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type,x-client-id")
        .body(Body::empty())
        .unwrap();

    app().oneshot(request).await.unwrap()
}

// A fixed origin is echoed back with credentials allowed
#[tokio::test]
async fn fixed_origin_is_allowed() {
    let response = get_with_origin("http://localhost:3000").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_text(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some("http://localhost:3000")
    );
    assert_eq!(
        header_text(&response, header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
        Some("true")
    );
}

// An origin matching a wildcard suffix is echoed back
#[tokio::test]
async fn wildcard_origin_is_allowed() {
    let origin = "https://my-app.vercel.app";
    let response = get_with_origin(origin).await;

    assert_eq!(
        header_text(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(origin)
    );
}

// An unknown origin gets no CORS headers
#[tokio::test]
async fn unknown_origin_is_not_allowed() {
    let response = get_with_origin("https://evil.example.com").await;

    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
}

// A request without an Origin header gets no CORS headers but still works
#[tokio::test]
async fn request_without_origin_is_untouched() {
    let response = get("/api/v1/health/app", &auth_headers()).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
}

// A preflight from an allowed origin is answered before the gate
#[tokio::test]
async fn preflight_from_allowed_origin_succeeds() {
    let response = preflight("http://localhost:3000").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_text(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some("http://localhost:3000")
    );

    let methods = header_text(&response, header::ACCESS_CONTROL_ALLOW_METHODS).unwrap();
    assert!(methods.contains("POST"));

    let headers = header_text(&response, header::ACCESS_CONTROL_ALLOW_HEADERS)
        .unwrap()
        .to_lowercase();
    assert!(headers.contains("content-type"));
    assert!(headers.contains("x-client-id"));
    assert!(headers.contains("x-client-secret"));
}

// A preflight answer can be reused by the browser for one hour
#[tokio::test]
async fn preflight_answer_can_be_cached_for_one_hour() {
    let response = preflight("http://localhost:3000").await;

    assert_eq!(
        header_text(&response, header::ACCESS_CONTROL_MAX_AGE),
        Some("3600")
    );
}

// A preflight from an unknown origin gets no allow-origin header
#[tokio::test]
async fn preflight_from_unknown_origin_is_not_allowed() {
    let response = preflight("https://evil.example.com").await;

    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
}
