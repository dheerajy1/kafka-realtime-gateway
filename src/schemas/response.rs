use std::borrow::Cow;

use serde::Serialize;
use utoipa::ToSchema;

// Success envelope shared by every API endpoint
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SuccessResponse<T: Serialize> {
    pub success: bool,
    pub status_code: u16,
    // Fixed text written in the source, so it is borrowed and never copied
    pub message: &'static str,
    pub data: T,
}

impl<T: Serialize> SuccessResponse<T> {
    // Builds a 200 OK envelope around the given data
    pub fn ok(message: &'static str, data: T) -> Self {
        Self {
            success: true,
            status_code: 200,
            message,
            data,
        }
    }

    // Builds a 202 Accepted envelope around the given data
    pub fn accepted(message: &'static str, data: T) -> Self {
        Self {
            success: true,
            status_code: 202,
            message,
            data,
        }
    }
}

// Error envelope shared by every API error
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub success: bool,
    pub status_code: u16,
    pub code: &'static str,

    // Borrowed for the fixed catalogue text and owned for a text built at run time
    pub message: Cow<'static, str>,
    pub error: Cow<'static, str>,
}
