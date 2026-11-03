use axum::{
    Json,
    extract::{FromRequest, Request},
};
use serde::de::DeserializeOwned;
use validator::{Validate, ValidationErrors};

use crate::errors::{api_error::ApiError, code::ErrorCode};

// JSON body extractor that also runs the validator rules, a handler that takes it only runs with a valid body
#[derive(Debug, Clone)]
pub struct ValidJson<T>(pub T);

// Text of the first field problem, fields are sorted so the answer does not change between requests
fn first_validation_message(errors: &ValidationErrors) -> String {
    let mut problems: Vec<(String, String)> = errors
        .field_errors()
        .into_iter()
        .map(|(field, list)| {
            let text = list
                .first()
                .and_then(|error| error.message.as_ref())
                .map(|message| message.to_string())
                .unwrap_or_else(|| format!("\"{field}\" is invalid"));

            (field.to_string(), text)
        })
        .collect();

    problems.sort();

    problems
        .into_iter()
        .next()
        .map(|(_, text)| text)
        .unwrap_or_else(|| "Invalid input data".to_string())
}

impl<S, T> FromRequest<S> for ValidJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Validate,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        // A body that is missing, not JSON or the wrong shape returns 400 INVALID_INPUT
        let Json(value) = Json::<T>::from_request(request, state)
            .await
            .map_err(|rejection| ApiError::new(ErrorCode::BadRequest, rejection.body_text()))?;

        // A body that breaks a validator rule returns 400 INVALID_INPUT with the field problem
        value.validate().map_err(|errors| {
            ApiError::new(ErrorCode::BadRequest, first_validation_message(&errors))
        })?;

        Ok(Self(value))
    }
}
