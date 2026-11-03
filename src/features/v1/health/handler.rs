use axum::{Json, Router, routing::get};

use super::{
    model::{AppHealthData, DbHealthData, DbStatus, HealthData},
    service,
};
use crate::{
    errors::{api_error::ApiError, catalog::service_unavailable},
    middleware::no_auth::NoAuth,
    schemas::response::SuccessResponse,
};

// Routes of the health feature, mounted under /api/v1/health
pub fn router() -> Router {
    Router::new()
        .route("/", get(health))
        .route("/app", get(app_health))
        .route("/db", get(db_health))
}

// GET /api/v1/health, the error responses are added in openapi/errors.rs under the operation id health_unified
#[utoipa::path(
    get,
    path = "/api/v1/health",
    operation_id = "health_unified",
    tag = "Health",
    summary = "Unified health check",
    description = "Returns application and database health. Requires client credentials.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
    ),
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<HealthData>,
            example = json!({
                "success": true,
                "httpCode": 200,
                "message": "Health check OK",
                "data": {
                    "app": {
                        "status": "UP",
                        "version": "0.1.0",
                        "uptimeMs": 123456,
                        "timestampUtc": "2025-01-01T00:00:00.000Z"
                    },
                    "db": {
                        "status": "UP",
                        "database": "CONNECTED",
                        "latencyMs": 12
                    }
                }
            })
        )
    )
)]

// GET /api/v1/health
pub async fn health(_auth: NoAuth) -> Result<Json<SuccessResponse<HealthData>>, ApiError> {
    let app = service::get_app_health();
    let db = service::get_db_health().await;

    // A database that is down fails the unified check with 503
    if db.status == DbStatus::Down {
        return Err(ApiError::from_entry(service_unavailable::DB_PROBE_FAILED));
    }

    Ok(Json(SuccessResponse::ok(
        "Health check OK",
        HealthData { app, db },
    )))
}

// GET /api/v1/health/app, the error responses are added in openapi/errors.rs under the operation id health_app
#[utoipa::path(
    get,
    path = "/api/v1/health/app",
    operation_id = "health_app",
    tag = "Health",
    summary = "Application health",
    description = "Returns process uptime and version.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
    ),
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<AppHealthData>,
            example = json!({
                "success": true,
                "httpCode": 200,
                "message": "Application is running",
                "data": {
                    "status": "UP",
                    "version": "0.1.0",
                    "uptimeMs": 123456,
                    "timestampUtc": "2025-01-01T00:00:00.000Z"
                }
            })
        )
    )
)]

// GET /api/v1/health/app
pub async fn app_health(_auth: NoAuth) -> Json<SuccessResponse<AppHealthData>> {
    Json(SuccessResponse::ok(
        "Application is running",
        service::get_app_health(),
    ))
}

// GET /api/v1/health/db, the error responses are added in openapi/errors.rs under the operation id health_db
#[utoipa::path(
    get,
    path = "/api/v1/health/db",
    operation_id = "health_db",
    tag = "Health",
    summary = "Database health",
    description = "Probes on-prem PostgreSQL connectivity and latency.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
    ),
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<DbHealthData>,
            example = json!({
                "success": true,
                "httpCode": 200,
                "message": "Database connection successful",
                "data": {
                    "status": "UP",
                    "database": "CONNECTED",
                    "latencyMs": 12
                }
            })
        )
    )
)]

// GET /api/v1/health/db
pub async fn db_health(_auth: NoAuth) -> Result<Json<SuccessResponse<DbHealthData>>, ApiError> {
    let data = service::get_db_health().await;

    // A database that is down returns 503
    if data.status == DbStatus::Down {
        return Err(ApiError::from_entry(service_unavailable::DB_PROBE_FAILED));
    }

    Ok(Json(SuccessResponse::ok(
        "Database connection successful",
        data,
    )))
}
