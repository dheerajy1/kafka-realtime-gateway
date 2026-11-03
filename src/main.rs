use std::net::SocketAddr;

use kafka_api_gateway::{
    app,
    config::env,
    db::{pool, postgres::PgStore},
    features::v1::health::service,
    logging,
    state::AppState,
};
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    logging::init();

    // Loads and validates the env at boot, the process exits if it is invalid
    env::init();

    // Records the start time so uptime counts from boot
    service::mark_started();

    let env = env::env();

    // The service pool connects now, the process exits if the database is not reachable
    let service_pool = match pool::connect_service(env).await {
        Ok(pool) => pool,
        Err(error) => {
            tracing::error!(target: "Db", "On-prem PostgreSQL connection failed (service): {error}");
            std::process::exit(1);
        }
    };

    tracing::info!(target: "Db", "On-prem PostgreSQL connected (service)");

    // The default pool opens its first connection on first use
    let default_pool = pool::connect_default_lazy(env);

    let store = PgStore::new(default_pool, service_pool);
    let state = AppState::from_pg(store.clone());

    let port = env.app_port;

    // Listens on all interfaces so other machines on the network can connect
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(address).await?;

    tracing::info!(
        target: "Server",
        "🚀 Kafka realtime gateway running on http://localhost:{port}"
    );

    axum::serve(listener, app::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Closes both database pools before the process exits
    store.close().await;

    tracing::info!(target: "Server", "Shutdown complete");

    Ok(())
}

// Resolves on Ctrl+C or SIGTERM, the signal docker stop sends
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install the Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install the SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!(target: "Server", "Shutdown signal received");
}
