use std::time::Duration;

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};

use crate::config::env::Env;

// Most connections each pool opens
const MAX_CONNECTIONS: u32 = 5;

// A connection that stays unused this long is closed
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

// Runtime part of the PostgreSQL application_name, kept exactly like the old gateway
const RUNTIME_TAG: &str = "local-node";

// Builds the connection options for one database user, without TLS like the old gateway
fn connect_options(env: &Env, user: &str, password: &str) -> PgConnectOptions {
    PgConnectOptions::new()
        .host(&env.onprem_db_server)
        .port(env.onprem_db_port)
        .database(&env.onprem_db_database)
        .username(user)
        .password(password)
        .application_name(&format!("{}@{}", env.app_name, RUNTIME_TAG))
        .ssl_mode(PgSslMode::Disable)
}

// Pool limits shared by both pools
fn pool_options() -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .idle_timeout(Some(IDLE_TIMEOUT))
}

// Default pool for the health probe and the user lookups, it opens its first connection on first use
pub fn connect_default_lazy(env: &Env) -> PgPool {
    pool_options().connect_lazy_with(connect_options(
        env,
        &env.onprem_db_user_public,
        &env.onprem_db_password_public,
    ))
}

// Service pool for the database functions, it connects now so a bad setup fails at boot
pub async fn connect_service(env: &Env) -> Result<PgPool, sqlx::Error> {
    pool_options()
        .connect_with(connect_options(
            env,
            &env.onprem_db_user_service,
            &env.onprem_db_password_service,
        ))
        .await
}
