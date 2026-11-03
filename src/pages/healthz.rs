use axum::{
    Json, Router,
    extract::State,
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::get,
};

use super::render::{render_with_props, wants_html};
use crate::components::healthz::{Healthz, HealthzProps};
use crate::components::healthz_app::{HealthzApp, HealthzAppProps};
use crate::components::healthz_db::{HealthzDb, HealthzDbProps};
use crate::features::v1::health::{
    model::{DbStatus, HealthData},
    service,
};
use crate::schemas::response::SuccessResponse;
use crate::state::AppState;

// Routes of the health pages, each returns html for browsers and json otherwise
pub fn router() -> Router<crate::state::AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/healthz/app", get(healthz_app))
        .route("/healthz/db", get(healthz_db))
}

// GET /healthz
async fn healthz(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let app = service::get_app_health();
    let db = service::get_db_health(state.db_probe.as_ref()).await;

    if wants_html(&headers) {
        return render_with_props(
            Healthz,
            HealthzProps {
                title: "Health Check | Kafka realtime gateway".to_string(),
                app,
                db,
            },
        )
        .into_response();
    }

    Json(SuccessResponse::ok(
        "Health check OK",
        HealthData { app, db },
    ))
    .into_response()
}

// GET /healthz/app
async fn healthz_app(headers: HeaderMap) -> Response {
    let data = service::get_app_health();

    if wants_html(&headers) {
        return render_with_props(
            HealthzApp,
            HealthzAppProps {
                title: "App Health | Kafka realtime gateway".to_string(),
                data,
            },
        )
        .into_response();
    }

    Json(SuccessResponse::ok("Application is running", data)).into_response()
}

// GET /healthz/db
async fn healthz_db(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let data = service::get_db_health(state.db_probe.as_ref()).await;

    if wants_html(&headers) {
        return render_with_props(
            HealthzDb,
            HealthzDbProps {
                title: "DB Health | Kafka realtime gateway".to_string(),
                data,
            },
        )
        .into_response();
    }

    let message = if data.status == DbStatus::Up {
        "Database connection successful"
    } else {
        "Database connection failed"
    };

    Json(SuccessResponse::ok(message, data)).into_response()
}
