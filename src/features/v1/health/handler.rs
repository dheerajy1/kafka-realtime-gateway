use axum::{Json, Router, extract::State, routing::get};

use super::{
    model::{AppHealthData, DbHealthData, DbStatus, HealthData},
    service,
};
use crate::{
    errors::{api_error::ApiError, catalog::service_unavailable},
    kafka::health::{KafkaHealthFull, KafkaStatus},
    middleware::no_auth::NoAuth,
    schemas::response::SuccessResponse,
    state::AppState,
};

// Routes of the health feature, mounted under /api/v1/health
pub fn router() -> Router<crate::state::AppState> {
    Router::new()
        .route("/", get(health))
        .route("/app", get(app_health))
        .route("/db", get(db_health))
        .route("/kafka", get(kafka_health))
}

// GET /api/v1/health, the error responses are added in openapi/errors.rs under the operation id health_unified
#[utoipa::path(
    get,
    path = "/api/v1/health",
    operation_id = "health_unified",
    tag = "Health",
    summary = "Unified health check",
    description = "Returns application, database and Kafka health. Requires client credentials.",
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
                    },
                    "kafka": {
                        "status": "UP",
                        "brokerCount": 1,
                        "latencyMs": 8
                    }
                }
            })
        )
    )
)]

// GET /api/v1/health
pub async fn health(
    State(state): State<AppState>,
    _auth: NoAuth,
) -> Result<Json<SuccessResponse<HealthData>>, ApiError> {
    let app = service::get_app_health();

    // The database and the Kafka broker are probed at the same time
    let (db, kafka) = tokio::join!(
        service::get_db_health(state.db_probe.as_ref()),
        state.kafka_probe.summary(),
    );

    // A database that is down fails the unified check with 503
    if db.status == DbStatus::Down {
        return Err(ApiError::from_entry(service_unavailable::DB_PROBE_FAILED));
    }

    // A Kafka broker that is down fails the unified check with 503 (D48)
    if kafka.status == KafkaStatus::Down {
        return Err(ApiError::from_entry(
            service_unavailable::KAFKA_PROBE_FAILED,
        ));
    }

    Ok(Json(SuccessResponse::ok(
        "Health check OK",
        HealthData { app, db, kafka },
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
pub async fn db_health(
    State(state): State<AppState>,
    _auth: NoAuth,
) -> Result<Json<SuccessResponse<DbHealthData>>, ApiError> {
    let data = service::get_db_health(state.db_probe.as_ref()).await;

    // A database that is down returns 503
    if data.status == DbStatus::Down {
        return Err(ApiError::from_entry(service_unavailable::DB_PROBE_FAILED));
    }

    Ok(Json(SuccessResponse::ok(
        "Database connection successful",
        data,
    )))
}

// GET /api/v1/health/kafka, the error responses are added in openapi/errors.rs under the operation id health_kafka
#[utoipa::path(
    get,
    path = "/api/v1/health/kafka",
    operation_id = "health_kafka",
    tag = "Health",
    summary = "Kafka health",
    description = "Returns the full Kafka broker health: cluster, brokers, the gateway producer and consumer, topic partitions, the consumer group and broker disk usage. The result is cached for 5 seconds.",
    params(
        ("x-client-id" = String, Header, description = "Client id"),
        ("x-client-secret" = String, Header, description = "Client secret"),
    ),
    responses(
        (
            status = 200,
            description = "Response for status 200",
            body = SuccessResponse<KafkaHealthFull>,
            example = json!({
                "success": true,
                "httpCode": 200,
                "message": "Kafka connection successful",
                "data": {
                    "status": "UP",
                    "latencyMs": 8,
                    "checkedAt": "2025-01-01T00:00:00.000Z",
                    "error": null,
                    "cluster": {
                        "clusterId": "MkU3OEVBNTcwNTJENDM2Qk",
                        "controllerId": 1,
                        "brokers": [
                            { "id": 1, "host": "host.docker.internal", "port": 9094, "rack": null, "fenced": false }
                        ]
                    },
                    "gateway": {
                        "producer": { "lastPublishAgoMs": 1200, "lastPublishOk": true },
                        "consumer": { "running": true, "stalled": false, "lastPollAgoMs": 300, "lastError": null },
                        "consumedTopics": 8,
                        "connectedSubscribers": 2
                    },
                    "topics": [
                        { "name": "gateway-test", "partitions": 1, "underReplicatedPartitions": 0, "leaderlessPartitions": 0 }
                    ],
                    "consumerGroup": { "groupId": "kafka-realtime-gateway", "state": "Stable", "members": 1 },
                    "disks": [
                        {
                            "brokerId": 1,
                            "logDirs": [
                                { "path": "/var/lib/kafka/data", "totalBytes": 100_000_000_000_i64, "usableBytes": 40_000_000_000_i64, "replicaBytes": 5_000_000, "failed": false }
                            ]
                        }
                    ],
                    "warnings": []
                }
            })
        )
    )
)]

// GET /api/v1/health/kafka
pub async fn kafka_health(
    State(state): State<AppState>,
    _auth: NoAuth,
) -> Result<Json<SuccessResponse<KafkaHealthFull>>, ApiError> {
    let data = state.kafka_probe.full().await;

    // A Kafka broker that is down returns 503 (D49)
    if data.status == KafkaStatus::Down {
        return Err(ApiError::from_entry(
            service_unavailable::KAFKA_PROBE_FAILED,
        ));
    }

    Ok(Json(SuccessResponse::ok(
        "Kafka connection successful",
        data,
    )))
}
