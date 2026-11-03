use std::net::SocketAddr;

use kafka_api_gateway::{app, config::env, features::v1::health::service, logging};
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    logging::init();

    // Loads and validates the env at boot, the process exits if it is invalid
    env::init();

    // Records the start time so uptime counts from boot
    service::mark_started();

    let port = env::env().app_port;

    // Listens on all interfaces so other machines on the network can connect
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(address).await?;

    tracing::info!(
        target: "Server",
        "🚀 Kafka realtime gateway running on http://localhost:{port}"
    );

    axum::serve(listener, app::router())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

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
