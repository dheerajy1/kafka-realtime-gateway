mod common;

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{Router, http::StatusCode, routing::get};
use common::{auth_headers, body_json, fakes::FakeStore, get_to, post_json_to, post_raw_to};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use kafka_api_gateway::{
    auth::jwt::{Claims, JWT_ISSUER, sign_token},
    errors::catalog::{CatalogEntry, bad_request, conflict, internal_server_error, unauthorized},
    middleware::api_key::ApiKeyAuth,
    state::AppState,
};
use serde_json::{Value, json};

// Same value the test env sets as JWT_SECRET
const JWT_SECRET: &str = "test-jwt-secret";

// Plain password of the test API user
const API_PASSWORD: &str = "apipass1";

// Handler that only runs when the API key check passes, it answers with the verified key id
async fn probe(ApiKeyAuth(identity): ApiKeyAuth) -> String {
    identity.api_key_id
}

// Router with one route behind ApiKeyAuth
fn probe_app(state: AppState) -> Router {
    common::setup_env();

    Router::new().route("/probe", get(probe)).with_state(state)
}

// Fake store with one API user and one API key, plus a state that shares it
fn store_and_state() -> (Arc<FakeStore>, AppState) {
    let store = Arc::new(
        FakeStore::new()
            .with_api_user("apiuser", API_PASSWORD, "admin")
            .with_api_key("key-1", "s3cret"),
    );

    let state = AppState::new(store.clone(), store.clone(), store.clone());

    (store, state)
}

// The error code text of a catalogue entry
fn code_of(entry: CatalogEntry) -> &'static str {
    entry.code.as_str()
}

// Seconds since the unix epoch
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// Builds a token with any claims, signed with the given secret
fn custom_token(secret: &str, claims: &Value) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

// Valid client headers plus a Bearer token
fn bearer_headers(token: &str) -> Vec<(&'static str, String)> {
    let mut headers: Vec<(&'static str, String)> = auth_headers()
        .iter()
        .map(|(name, value)| (*name, value.to_string()))
        .collect();

    headers.push(("authorization", format!("Bearer {token}")));

    headers
}

// Turns owned headers into the borrowed form the helpers take
fn as_refs<'a>(headers: &'a [(&'static str, String)]) -> Vec<(&'a str, &'a str)> {
    headers
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect()
}

// Sends POST /api/v1/login with the given body and valid client headers
async fn login(state: AppState, body: &Value) -> axum::http::Response<axum::body::Body> {
    post_json_to(
        common::app_with(state),
        "/api/v1/login",
        &auth_headers(),
        body,
    )
    .await
}

// Sends POST /api/v1/api-keys with a Bearer token and valid client headers
async fn create_key(
    state: AppState,
    token: &str,
    body: &Value,
) -> axum::http::Response<axum::body::Body> {
    let headers = bearer_headers(token);

    post_json_to(
        common::app_with(state),
        "/api/v1/api-keys",
        &as_refs(&headers),
        body,
    )
    .await
}

// A valid token for the test API user
fn valid_token() -> String {
    sign_token(JWT_SECRET, "apiuser", "admin").unwrap().token
}

// A correct login returns a token with the sub, role, issuer and one day expiry
#[tokio::test]
async fn login_returns_token_with_expected_claims() {
    let (_, state) = store_and_state();

    let response = login(
        state,
        &json!({ "username": "apiuser", "password": API_PASSWORD }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["success"], true);
    assert_eq!(body["message"], "Login successful");

    let token = body["data"]["token"].as_str().unwrap();

    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[JWT_ISSUER]);

    let claims = decode::<Claims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET.as_bytes()),
        &validation,
    )
    .unwrap()
    .claims;

    assert_eq!(claims.sub, "apiuser");
    assert_eq!(claims.role, "admin");
    assert_eq!(claims.iss, JWT_ISSUER);
    assert!(claims.exp.abs_diff(now() + 86_400) < 60);

    let expires_at = body["data"]["expiresAt"].as_str().unwrap();

    assert_eq!(expires_at.len(), 24);
    assert!(expires_at.ends_with('Z'));
}

// An unknown user and a wrong password both return 401 INVALID_CREDENTIALS
#[tokio::test]
async fn login_failures_return_the_same_401() {
    for body in [
        json!({ "username": "nobody", "password": API_PASSWORD }),
        json!({ "username": "apiuser", "password": "wrongpass" }),
    ] {
        let (_, state) = store_and_state();
        let response = login(state, &body).await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(unauthorized::INVALID_CREDENTIALS));
    }
}

// Missing client headers return 401
#[tokio::test]
async fn login_without_client_headers_returns_401() {
    let (_, state) = store_and_state();

    let response = post_json_to(
        common::app_with(state),
        "/api/v1/login",
        &[],
        &json!({ "username": "apiuser", "password": API_PASSWORD }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body = body_json(response).await;

    assert_eq!(body["code"], code_of(unauthorized::MISSING_AUTH_HEADERS));
}

// A wrong client secret returns 400
#[tokio::test]
async fn login_with_wrong_client_secret_returns_400() {
    let (_, state) = store_and_state();

    let response = post_json_to(
        common::app_with(state),
        "/api/v1/login",
        &[("x-client-id", "test-client"), ("x-client-secret", "wrong")],
        &json!({ "username": "apiuser", "password": API_PASSWORD }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = body_json(response).await;

    assert_eq!(body["code"], code_of(bad_request::INVALID_HEADER_VALUES));
}

// A body that breaks a rule returns 400 INVALID_INPUT with the field in the error text
#[tokio::test]
async fn login_with_invalid_body_returns_400() {
    for body in [
        json!({ "username": "", "password": API_PASSWORD }),
        json!({ "username": "apiuser", "password": "12345" }),
        json!({ "username": "apiuser" }),
    ] {
        let (_, state) = store_and_state();
        let response = login(state, &body).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(bad_request::INVALID_INPUT));
        assert!(
            body["error"].as_str().unwrap().contains("assword")
                || body["error"].as_str().unwrap().contains("sername")
        );
    }
}

// A body that is not JSON returns 400
#[tokio::test]
async fn login_with_non_json_body_returns_400() {
    let (_, state) = store_and_state();

    let response = post_raw_to(
        common::app_with(state),
        "/api/v1/login",
        &auth_headers(),
        "text/plain",
        "hello",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// A database failure returns 500 DATABASE_ERROR
#[tokio::test]
async fn login_database_failure_returns_500() {
    let broken = common::state_from(
        FakeStore::new()
            .with_api_user("apiuser", API_PASSWORD, "admin")
            .broken(),
    );

    let response = login(
        broken,
        &json!({ "username": "apiuser", "password": API_PASSWORD }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body = body_json(response).await;

    assert_eq!(body["code"], code_of(internal_server_error::DATABASE_ERROR));
}

// A missing or non-Bearer Authorization header returns 401 MISSING_AUTHORIZATION_TOKEN
#[tokio::test]
async fn api_keys_without_bearer_token_returns_401() {
    for authorization in [None, Some("Basic abc123")] {
        let (_, state) = store_and_state();

        let mut headers = auth_headers().to_vec();

        if let Some(value) = authorization {
            headers.push(("authorization", value));
        }

        let response = post_json_to(
            common::app_with(state),
            "/api/v1/api-keys",
            &headers,
            &json!({ "apiKeyId": "my-service-key" }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = body_json(response).await;

        assert_eq!(
            body["code"],
            code_of(unauthorized::MISSING_AUTHORIZATION_TOKEN)
        );
    }
}

// Tokens that fail verification return the matching 401 error
#[tokio::test]
async fn api_keys_with_bad_tokens_return_401() {
    let future = now() + 3600;

    let cases: Vec<(String, CatalogEntry)> = vec![
        ("not-a-token".to_string(), unauthorized::INVALID_TOKEN),
        (
            custom_token(
                "another-secret",
                &json!({ "sub": "apiuser", "role": "admin", "exp": future, "iss": JWT_ISSUER }),
            ),
            unauthorized::INVALID_TOKEN,
        ),
        (
            custom_token(
                JWT_SECRET,
                &json!({ "sub": "apiuser", "role": "admin", "exp": future, "iss": "someone-else" }),
            ),
            unauthorized::INVALID_TOKEN,
        ),
        (
            custom_token(
                JWT_SECRET,
                &json!({ "sub": "apiuser", "role": "admin", "exp": now() - 100, "iss": JWT_ISSUER }),
            ),
            unauthorized::EXPIRED_TOKEN,
        ),
        (
            custom_token(JWT_SECRET, &json!({ "exp": future, "iss": JWT_ISSUER })),
            unauthorized::MISSING_SUB_OR_ROLE,
        ),
    ];

    for (token, expected) in cases {
        let (_, state) = store_and_state();

        let response = create_key(state, &token, &json!({ "apiKeyId": "my-service-key" })).await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "token {token}");

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(expected), "token {token}");
    }
}

// Wrong client credentials are rejected before the token is read
#[tokio::test]
async fn api_keys_with_wrong_client_secret_returns_400() {
    let (_, state) = store_and_state();

    let token = valid_token();
    let authorization = format!("Bearer {token}");

    let response = post_json_to(
        common::app_with(state),
        "/api/v1/api-keys",
        &[
            ("x-client-id", "test-client"),
            ("x-client-secret", "wrong"),
            ("authorization", &authorization),
        ],
        &json!({ "apiKeyId": "my-service-key" }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// A key id that breaks a rule returns 400 INVALID_INPUT
#[tokio::test]
async fn api_keys_with_invalid_key_id_returns_400() {
    let too_long = "k".repeat(65);

    for api_key_id in [
        "short",
        too_long.as_str(),
        "has spaces in it",
        "bad!chars!!",
    ] {
        let (_, state) = store_and_state();

        let response = create_key(state, &valid_token(), &json!({ "apiKeyId": api_key_id })).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "id {api_key_id}"
        );

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(bad_request::INVALID_INPUT));
    }
}

// A valid request creates the key, returns the secret once, and the new key then passes ApiKeyAuth
#[tokio::test]
async fn api_keys_create_returns_a_secret_that_works() {
    let (store, state) = store_and_state();

    let response = create_key(
        state.clone(),
        &valid_token(),
        &json!({ "apiKeyId": "my-service-key" }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_json(response).await;

    assert_eq!(body["message"], "API key created successfully");
    assert_eq!(body["data"]["apiKeyId"], "my-service-key");

    let secret = body["data"]["apiKeySecret"].as_str().unwrap().to_string();

    assert_eq!(secret.len(), 64);
    assert!(
        secret
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    );
    assert!(store.api_key_ids().contains(&"my-service-key".to_string()));

    // The stored hash belongs to the returned secret, so the key works on a protected route
    let mut headers = auth_headers().to_vec();
    headers.push(("x-api-key", "my-service-key"));
    headers.push(("x-api-secret", secret.as_str()));

    let response = get_to(probe_app(state), "/probe", &headers).await;

    assert_eq!(response.status(), StatusCode::OK);
}

// Creating the same key id twice returns 409 DATABASE_CONFLICT
#[tokio::test]
async fn api_keys_duplicate_key_id_returns_409() {
    let (_, state) = store_and_state();
    let body = json!({ "apiKeyId": "my-service-key" });

    let first = create_key(state.clone(), &valid_token(), &body).await;

    assert_eq!(first.status(), StatusCode::OK);

    let second = create_key(state, &valid_token(), &body).await;

    assert_eq!(second.status(), StatusCode::CONFLICT);

    let body = body_json(second).await;

    assert_eq!(body["code"], code_of(conflict::DATABASE_CONFLICT));
}

// Headers of a request to the probe route with the given key and secret
fn probe_headers<'a>(key: &'a str, secret: &'a str) -> Vec<(&'a str, &'a str)> {
    let mut headers = auth_headers().to_vec();
    headers.push(("x-api-key", key));
    headers.push(("x-api-secret", secret));

    headers
}

// A valid API key reaches the handler
#[tokio::test]
async fn api_key_auth_accepts_a_valid_key() {
    let (_, state) = store_and_state();

    let response = get_to(
        probe_app(state),
        "/probe",
        &probe_headers("key-1", "s3cret"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
}

// Missing key headers return 401 MISSING_API_KEY
#[tokio::test]
async fn api_key_auth_without_key_headers_returns_401() {
    for headers in [
        auth_headers().to_vec(),
        vec![
            ("x-client-id", "test-client"),
            ("x-client-secret", "test-secret"),
            ("x-api-key", "key-1"),
        ],
    ] {
        let (_, state) = store_and_state();

        let response = get_to(probe_app(state), "/probe", &headers).await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(unauthorized::MISSING_API_KEY));
    }
}

// An unknown key and a wrong secret both return 401 INVALID_API_KEY
#[tokio::test]
async fn api_key_auth_with_bad_key_or_secret_returns_401() {
    for (key, secret) in [("unknown-key", "s3cret"), ("key-1", "wrong-secret")] {
        let (_, state) = store_and_state();

        let response = get_to(probe_app(state), "/probe", &probe_headers(key, secret)).await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(unauthorized::INVALID_API_KEY));
    }
}

// Empty or too long key headers return 400 INVALID_HEADER_VALUES
#[tokio::test]
async fn api_key_auth_with_malformed_headers_returns_400() {
    let long_key = "k".repeat(45);
    let long_secret = "s".repeat(101);

    for (key, secret) in [
        ("", "s3cret"),
        ("key-1", ""),
        (long_key.as_str(), "s3cret"),
        ("key-1", long_secret.as_str()),
    ] {
        let (_, state) = store_and_state();

        let response = get_to(probe_app(state), "/probe", &probe_headers(key, secret)).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = body_json(response).await;

        assert_eq!(body["code"], code_of(bad_request::INVALID_HEADER_VALUES));
    }
}

// Client credentials are checked before the API key
#[tokio::test]
async fn api_key_auth_checks_client_credentials_first() {
    let (_, state) = store_and_state();

    let response = get_to(
        probe_app(state.clone()),
        "/probe",
        &[("x-api-key", "key-1"), ("x-api-secret", "s3cret")],
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = get_to(
        probe_app(state),
        "/probe",
        &[
            ("x-client-id", "test-client"),
            ("x-client-secret", "wrong"),
            ("x-api-key", "key-1"),
            ("x-api-secret", "s3cret"),
        ],
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
