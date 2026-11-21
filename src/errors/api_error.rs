use std::{borrow::Cow, fmt};

use axum::{
    Json,
    response::{IntoResponse, Response},
};

use super::{catalog::CatalogEntry, code::ErrorCode};
use crate::schemas::response::ErrorResponse;

// The one error type handlers return, rendered as the global error shape
#[derive(Debug, Clone)]
pub struct ApiError {
    code: ErrorCode,

    // Borrowed for the fixed catalogue text and owned for a text built at run time
    message: Cow<'static, str>,
    error: Cow<'static, str>,
}

impl ApiError {
    // Builds the error from a catalogue entry, the normal way to return an error
    pub fn from_entry(entry: CatalogEntry) -> Self {
        Self {
            code: entry.code,
            message: Cow::Borrowed(entry.message),
            error: Cow::Borrowed(entry.error),
        }
    }

    // Builds the error from a code with a custom error text, the message comes from the code
    // A fixed text is borrowed and a text built at run time is moved in, nothing is copied either way
    pub fn new(code: ErrorCode, error: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            message: Cow::Borrowed(code.message()),
            error: error.into(),
        }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn error(&self) -> &str {
        &self.error
    }

    // Gives the message away, used where the text moves into a page
    pub fn into_message(self) -> Cow<'static, str> {
        self.message
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.error)
    }
}

impl std::error::Error for ApiError {}

// Lets a catalogue entry be turned into an error with `.into()` or `?`
impl From<CatalogEntry> for ApiError {
    fn from(entry: CatalogEntry) -> Self {
        Self::from_entry(entry)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorResponse {
            success: false,
            status_code: self.code.status_code(),
            code: self.code.as_str(),
            message: self.message,
            error: self.error,
        };

        (self.code.status(), Json(body)).into_response()
    }
}
