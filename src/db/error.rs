use std::io::ErrorKind;

use crate::errors::{
    api_error::ApiError,
    catalog::{CatalogEntry, internal_server_error, unauthorized},
};

// Postgres code for insufficient privilege
const SQLSTATE_INSUFFICIENT_PRIVILEGE: &str = "42501";

// Postgres code raised by RAISE EXCEPTION inside the database functions
const SQLSTATE_RAISE_EXCEPTION: &str = "P0001";

// Postgres code for a wrong password at connect time
const SQLSTATE_INVALID_PASSWORD: &str = "28P01";

// Postgres code for a database that does not exist
const SQLSTATE_INVALID_CATALOG_NAME: &str = "3D000";

// Picks the catalogue entry for a Postgres error code and message, the mapping table of build.md 2.9
pub fn classify_pg_error(sqlstate: Option<&str>, message: &str) -> CatalogEntry {
    match sqlstate {
        Some(SQLSTATE_INSUFFICIENT_PRIVILEGE) => internal_server_error::DB_PERMISSION_DENIED,
        Some(SQLSTATE_RAISE_EXCEPTION) => {
            // A function that failed on a missing grant is a server problem, any other raise means a bad key
            if message.to_lowercase().contains("permission denied") {
                internal_server_error::DB_PERMISSION_DENIED
            } else {
                unauthorized::INVALID_API_KEY
            }
        }
        Some(SQLSTATE_INVALID_PASSWORD | SQLSTATE_INVALID_CATALOG_NAME) => {
            internal_server_error::DB_CONNECTION_ERROR
        }
        _ => internal_server_error::DATABASE_ERROR,
    }
}

// Logs a sqlx error and turns it into the API error the caller returns
pub fn map_db_error(err: &sqlx::Error) -> ApiError {
    tracing::error!(target: "Db", "database error: {err}");

    let entry = match err {
        sqlx::Error::Database(db_err) => {
            classify_pg_error(db_err.code().as_deref(), db_err.message())
        }
        // A refused connection means the database server is not reachable
        sqlx::Error::Io(io_err) if io_err.kind() == ErrorKind::ConnectionRefused => {
            internal_server_error::DB_CONNECTION_ERROR
        }
        _ => internal_server_error::DATABASE_ERROR,
    };

    ApiError::from_entry(entry)
}
