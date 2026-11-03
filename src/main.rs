use kafka_api_gateway::{
    app,
    config::env,
    db::{pool, postgres::PgStore},
    features::v1::health::service,
    kafka::{
        admin::KafkaTopicLister,
        bridge::Bridge,
        consumer::BridgeConsumer,
        parse_brokers,
        producer::KafkaPublisher,
        publisher::Publisher,
        registry::TopicRegistry,
        reloader::{TopicReload, TopicReloader},
        subscribers::SubscriberRegistry,
        sync::TopicSync,
    },
    logging,
    state::{AppState, KafkaDeps},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    signal,
    sync::{mpsc, watch},
};

// How long the whole shutdown may take after the signal, under Docker's default 10 s stop timeout
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(8);

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
    let brokers = parse_brokers(&env.kafka_brokers);

    // The producer connects now, the process exits if no broker is reachable
    let publisher = match KafkaPublisher::connect(brokers.clone(), &env.kafka_client_id).await {
        Ok(publisher) => Arc::new(publisher),
        Err(error) => {
            tracing::error!(target: "Kafka", "Kafka producer connection failed: {error}");
            std::process::exit(1);
        }
    };

    tracing::info!(target: "Kafka", "Kafka producer connected");

    // The admin client connects now and is reused for every topic check
    let lister = match KafkaTopicLister::connect(brokers.clone(), &env.kafka_client_id).await {
        Ok(lister) => lister,
        Err(error) => {
            tracing::error!(target: "Kafka", "Kafka admin connection failed: {error}");
            std::process::exit(1);
        }
    };

    // The consumer connects now, it subscribes once the topics are loaded
    let consumer =
        match BridgeConsumer::connect(brokers, &env.kafka_client_id, &env.kafka_group_id).await {
            Ok(consumer) => Arc::new(consumer),
            Err(error) => {
                tracing::error!(target: "Kafka", "Kafka consumer connection failed: {error}");
                std::process::exit(1);
            }
        };

    tracing::info!(target: "Kafka", "Kafka consumer connected");

    let topics = Arc::new(TopicRegistry::default());
    let subscribers = Arc::new(SubscriberRegistry::new());

    // Turns true on shutdown, the sender lives in the graceful shutdown future below
    let (stop_tx, stop_rx) = watch::channel(false);

    // Every WebSocket task keeps a clone of drain_tx through the state, shutdown waits on drain_rx until all of them ended
    let (drain_tx, mut drain_rx) = mpsc::channel::<()>(1);

    let topic_sync = TopicSync::new(Arc::new(store.clone()), Arc::new(lister), topics.clone());
    let reloader = Arc::new(TopicReloader::new(topic_sync, consumer.clone()));

    // The startup load is fatal, both a load failure and an enabled topic missing from Kafka
    match reloader.reload("startup").await {
        Ok(summary) if summary.missing_topics.is_empty() => {}
        Ok(summary) => {
            tracing::error!(
                target: "TopicRegistry",
                "Startup topic validation failed, enabled topics missing from Kafka: {}",
                summary.missing_topics.join(", ")
            );
            std::process::exit(1);
        }
        Err(error) => {
            tracing::error!(target: "TopicRegistry", "Startup topic load failed: {}", error.error());
            std::process::exit(1);
        }
    }

    // The bridge reads Kafka and feeds the subscribers until the shutdown signal
    let bridge = Bridge::new(topics.clone(), subscribers.clone(), stop_rx.clone());
    let bridge_task = tokio::spawn({
        let consumer = consumer.clone();
        let stop_rx = stop_rx.clone();
        async move { consumer.run(&bridge, stop_rx).await }
    });

    let state = AppState::from_pg(
        store.clone(),
        KafkaDeps {
            topics,
            publisher: publisher.clone(),
            reloader,
            subscribers,
            shutdown: stop_rx,
            ws_drain: drain_tx,
        },
    );

    let port = env.app_port;

    // Listens on all interfaces so other machines on the network can connect
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(address).await?;

    tracing::info!(
        target: "Server",
        "🚀 Kafka realtime gateway running on http://localhost:{port}"
    );

    axum::serve(listener, app::router(state))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;

            // Tells the bridge and every waiting task that the gateway is stopping
            // Tells the bridge, every WebSocket and every waiting task that the gateway is stopping
            let _ = stop_tx.send(true);

            // Starts the hard time limit, a stuck step cannot keep the process alive past it
            tokio::spawn(async {
                tokio::time::sleep(SHUTDOWN_LIMIT).await;
                tracing::error!(
                    target: "Server",
                    "Shutdown took longer than {}s, forcing exit",
                    SHUTDOWN_LIMIT.as_secs()
                );
                std::process::exit(1);
            });
        })
        .await?;

    // Waits until every WebSocket task has sent its close frame and ended, the sender only lives inside those tasks now
    let _ = drain_rx.recv().await;

    // The bridge stops on its own once the shutdown flag is set
    if let Err(error) = bridge_task.await {
        tracing::error!(target: "WsBridge", "bridge task failed: {error}");
    }

    // Closes the consumer, then flushes and closes the producer, before the pools close
    consumer.shutdown().await;
    publisher.shutdown().await;

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
