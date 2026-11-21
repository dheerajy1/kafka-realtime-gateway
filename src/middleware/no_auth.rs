use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
};
use subtle::ConstantTimeEq;
use validator::Validate;

use crate::{
    config::env::env,
    errors::{
        api_error::ApiError,
        catalog::{bad_request, unauthorized},
    },
    schemas::auth::{CLIENT_ID_HEADER, CLIENT_SECRET_HEADER, NoAuthHeaders},
};

// Checks the client id and secret headers against the given credentials
// The caller reads the credentials from the env, so this function needs no env and a test can call it with any values
pub fn check_client_headers(
    headers: &HeaderMap,
    expected_id: &str,
    expected_secret: &str,
) -> Result<(), ApiError> {
    let client_id = headers.get(CLIENT_ID_HEADER);
    let client_secret = headers.get(CLIENT_SECRET_HEADER);

    // Missing headers return 401
    let (Some(client_id), Some(client_secret)) = (client_id, client_secret) else {
        return Err(ApiError::from_entry(unauthorized::MISSING_AUTH_HEADERS));
    };

    // Header values that are not readable text return 400
    let (Ok(client_id), Ok(client_secret)) = (client_id.to_str(), client_secret.to_str()) else {
        return Err(ApiError::from_entry(bad_request::INVALID_HEADER_VALUES));
    };

    let credentials = NoAuthHeaders::new(client_id, client_secret);

    // Present but empty or too long returns 400
    if credentials.validate().is_err() {
        return Err(ApiError::from_entry(bad_request::INVALID_HEADER_VALUES));
    }

    // ct_eq reads every byte instead of stopping at the first one that differs
    // A plain != answers faster for a wrong first byte, and the timing can reveal the secret
    let id_matches = credentials
        .client_id
        .as_bytes()
        .ct_eq(expected_id.as_bytes());
    let secret_matches = credentials
        .client_secret
        .as_bytes()
        .ct_eq(expected_secret.as_bytes());

    // Present but not matching the configured credentials returns 400
    // The single & compares both pairs, a && would skip the secret when the id is wrong
    if !bool::from(id_matches & secret_matches) {
        return Err(ApiError::from_entry(bad_request::INVALID_HEADER_VALUES));
    }

    Ok(())
}

// Checks the client id and secret headers against the credentials in the env
// The extractor, the WebSocket routes and the global gate all call this, so the rules live in one place
pub fn check_client_credentials(headers: &HeaderMap) -> Result<(), ApiError> {
    let env = env();

    check_client_headers(headers, &env.client_id, &env.client_secret)
}

// Extractor for public endpoints, a handler that takes it only runs with valid client credentials
// It holds nothing, because no handler reads the headers
pub struct NoAuth;

impl<S> FromRequestParts<S> for NoAuth
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        check_client_credentials(&parts.headers)?;

        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;
    use crate::errors::code::ErrorCode;

    // Builds a header map with the two client headers
    fn headers(id: &'static str, secret: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();

        headers.insert(CLIENT_ID_HEADER, HeaderValue::from_static(id));
        headers.insert(CLIENT_SECRET_HEADER, HeaderValue::from_static(secret));

        headers
    }

    // Status of the error a check returned, or 0 when it passed
    fn status_of(result: Result<(), ApiError>) -> u16 {
        match result {
            Ok(()) => 0,
            Err(error) => error.code().status_code(),
        }
    }

    // The right id and secret pass, and spaces around them are ignored
    #[test]
    fn matching_credentials_pass() {
        assert_eq!(
            status_of(check_client_headers(
                &headers("id", "secret"),
                "id",
                "secret"
            )),
            0
        );
        assert_eq!(
            status_of(check_client_headers(
                &headers(" id ", " secret "),
                "id",
                "secret"
            )),
            0
        );
    }

    // A wrong id or a wrong secret returns 400
    #[test]
    fn wrong_credentials_return_400() {
        assert_eq!(
            status_of(check_client_headers(
                &headers("x", "secret"),
                "id",
                "secret"
            )),
            400
        );
        assert_eq!(
            status_of(check_client_headers(&headers("id", "x"), "id", "secret")),
            400
        );
        assert_eq!(
            status_of(check_client_headers(
                &headers("idd", "secret"),
                "id",
                "secret"
            )),
            400
        );
    }

    // Missing headers return 401
    #[test]
    fn missing_headers_return_401() {
        assert_eq!(
            status_of(check_client_headers(&HeaderMap::new(), "id", "secret")),
            401
        );

        let mut only_id = HeaderMap::new();
        only_id.insert(CLIENT_ID_HEADER, HeaderValue::from_static("id"));

        assert_eq!(
            status_of(check_client_headers(&only_id, "id", "secret")),
            401
        );
    }

    // Empty values and values that are not text return 400
    #[test]
    fn empty_and_unreadable_values_return_400() {
        assert_eq!(
            status_of(check_client_headers(&headers("", "secret"), "id", "secret")),
            400
        );

        let mut binary = headers("id", "secret");
        binary.insert(
            CLIENT_SECRET_HEADER,
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );

        assert_eq!(
            status_of(check_client_headers(&binary, "id", "secret")),
            400
        );
    }

    // The error of a wrong credential is the invalid header values error
    #[test]
    fn wrong_credentials_use_the_bad_request_code() {
        let Err(error) = check_client_headers(&headers("x", "secret"), "id", "secret") else {
            panic!("a wrong id must be rejected");
        };

        assert_eq!(error.code(), ErrorCode::BadRequest);
    }
}
