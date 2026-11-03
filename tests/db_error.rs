use kafka_api_gateway::{
    db::error::classify_pg_error,
    errors::catalog::{internal_server_error, unauthorized},
};

// Insufficient privilege is a server problem with database permissions
#[test]
fn insufficient_privilege_maps_to_permission_denied() {
    let entry = classify_pg_error(Some("42501"), "permission denied for table api_keys");

    assert_eq!(
        entry.error,
        internal_server_error::DB_PERMISSION_DENIED.error
    );
}

// A function that raised because of a missing grant is a permission problem
#[test]
fn raise_exception_with_permission_denied_maps_to_permission_denied() {
    let entry = classify_pg_error(
        Some("P0001"),
        "Procedure failed: permission denied for function fn_verify_api_key",
    );

    assert_eq!(
        entry.error,
        internal_server_error::DB_PERMISSION_DENIED.error
    );
}

// Any other raise from the function means the key is not valid
#[test]
fn raise_exception_maps_to_invalid_api_key() {
    let entry = classify_pg_error(
        Some("P0001"),
        "Procedure failed: Invalid, inactive or expired API key",
    );

    assert_eq!(entry.error, unauthorized::INVALID_API_KEY.error);
}

// A wrong database password and an unknown database are connection errors
#[test]
fn bad_login_or_database_maps_to_connection_error() {
    for code in ["28P01", "3D000"] {
        let entry = classify_pg_error(Some(code), "connection problem");

        assert_eq!(
            entry.error,
            internal_server_error::DB_CONNECTION_ERROR.error
        );
    }
}

// Every other code, and no code at all, is a plain database error
#[test]
fn everything_else_maps_to_database_error() {
    for code in [Some("23505"), Some("42P01"), None] {
        let entry = classify_pg_error(code, "something else");

        assert_eq!(entry.error, internal_server_error::DATABASE_ERROR.error);
    }
}
