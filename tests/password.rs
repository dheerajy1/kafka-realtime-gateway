use kafka_api_gateway::{auth::password::verify_password, errors::code::ErrorCode};

// bcrypt cost of the test hashes, the lowest allowed so the tests stay fast
const TEST_COST: u32 = 4;

// Makes a bcrypt hash of a password
fn hash_of(password: &str) -> String {
    bcrypt::hash(password, TEST_COST).unwrap()
}

// The right password matches its hash
#[tokio::test]
async fn right_password_matches() {
    let valid = verify_password("secret-1".to_string(), Some(hash_of("secret-1"))).await;

    assert!(matches!(valid, Ok(true)));
}

// A wrong password does not match
#[tokio::test]
async fn wrong_password_does_not_match() {
    let valid = verify_password("secret-2".to_string(), Some(hash_of("secret-1"))).await;

    assert!(matches!(valid, Ok(false)));
}

// A missing hash, an unknown user, never matches and is not an error
#[tokio::test]
async fn missing_hash_never_matches() {
    for password in ["secret-1", "", "not the dummy password"] {
        let valid = verify_password(password.to_string(), None).await;

        assert!(matches!(valid, Ok(false)));
    }
}

// A hash that is not valid bcrypt is a server error, not a wrong password
#[tokio::test]
async fn broken_hash_is_a_server_error() {
    let result = verify_password("secret-1".to_string(), Some("not-a-hash".to_string())).await;

    let Err(error) = result else {
        panic!("a broken hash must be an error");
    };

    assert_eq!(error.code(), ErrorCode::InternalServerError);
}
