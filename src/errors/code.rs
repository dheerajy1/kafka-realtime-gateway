use axum::http::StatusCode;

// Every error code the API can return
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    PaymentRequired,
    Forbidden,
    NotFound,
    MethodNotSupported,
    Timeout,
    Conflict,
    PreconditionFailed,
    PayloadTooLarge,
    UnsupportedMediaType,
    UnprocessableContent,
    PreconditionRequired,
    TooManyRequests,
    ClientClosedRequest,
    InternalServerError,
    NotImplemented,
    BadGateway,
    ServiceUnavailable,
    GatewayTimeout,
}

impl ErrorCode {
    // Machine readable code sent in the response body
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "BAD_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::PaymentRequired => "PAYMENT_REQUIRED",
            Self::Forbidden => "FORBIDDEN",
            Self::NotFound => "NOT_FOUND",
            Self::MethodNotSupported => "METHOD_NOT_SUPPORTED",
            Self::Timeout => "TIMEOUT",
            Self::Conflict => "CONFLICT",
            Self::PreconditionFailed => "PRECONDITION_FAILED",
            Self::PayloadTooLarge => "PAYLOAD_TOO_LARGE",
            Self::UnsupportedMediaType => "UNSUPPORTED_MEDIA_TYPE",
            Self::UnprocessableContent => "UNPROCESSABLE_CONTENT",
            Self::PreconditionRequired => "PRECONDITION_REQUIRED",
            Self::TooManyRequests => "TOO_MANY_REQUESTS",
            Self::ClientClosedRequest => "CLIENT_CLOSED_REQUEST",
            Self::InternalServerError => "INTERNAL_SERVER_ERROR",
            Self::NotImplemented => "NOT_IMPLEMENTED",
            Self::BadGateway => "BAD_GATEWAY",
            Self::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            Self::GatewayTimeout => "GATEWAY_TIMEOUT",
        }
    }

    // Numeric http status sent in the response body
    pub fn status_code(self) -> u16 {
        match self {
            Self::BadRequest => 400,
            Self::Unauthorized => 401,
            Self::PaymentRequired => 402,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::MethodNotSupported => 405,
            Self::Timeout => 408,
            Self::Conflict => 409,
            Self::PreconditionFailed => 412,
            Self::PayloadTooLarge => 413,
            Self::UnsupportedMediaType => 415,
            Self::UnprocessableContent => 422,
            Self::PreconditionRequired => 428,
            Self::TooManyRequests => 429,
            Self::ClientClosedRequest => 499,
            Self::InternalServerError => 500,
            Self::NotImplemented => 501,
            Self::BadGateway => 502,
            Self::ServiceUnavailable => 503,
            Self::GatewayTimeout => 504,
        }
    }

    // Http status used on the response line
    pub fn status(self) -> StatusCode {
        StatusCode::from_u16(self.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    // Generic summary used when no specific catalogue entry is given
    pub fn message(self) -> &'static str {
        match self {
            Self::BadRequest => {
                "The server cannot process the request because the input provided is invalid."
            }
            Self::Unauthorized => {
                "The client request has not been completed because it lacks valid authentication credentials for the requested resource."
            }
            Self::PaymentRequired => {
                "The client request requires payment to access the requested resource."
            }
            Self::Forbidden => "You do not have permission to access the requested resource.",
            Self::NotFound => "The requested resource could not be found.",
            Self::MethodNotSupported => {
                "The server knows the request method, but the target resource doesn't support this method."
            }
            Self::Timeout => "The server would like to shut down this unused connection.",
            Self::Conflict => {
                "The request could not be completed due to a conflict with the current state of the resource."
            }
            Self::PreconditionFailed => "Access to the target resource has been denied.",
            Self::PayloadTooLarge => "Request entity is larger than limits defined by server.",
            Self::UnsupportedMediaType => {
                "The server refuses to accept the request because the payload format is in an unsupported format."
            }
            Self::UnprocessableContent => {
                "The server understands the request method, and the request entity is correct, but the server was unable to process it."
            }
            Self::PreconditionRequired => {
                "The server cannot process the request because a required precondition header is missing."
            }
            Self::TooManyRequests => {
                "The rate limit has been exceeded or too many requests are being sent to the server."
            }
            Self::ClientClosedRequest => "Access to the resource has been denied.",
            Self::InternalServerError => {
                "An unexpected error occurred while processing your request."
            }
            Self::NotImplemented => {
                "The server does not support the functionality required to fulfill the request."
            }
            Self::BadGateway => "The server received an invalid response from the upstream server.",
            Self::ServiceUnavailable => "The server is not ready to handle the request.",
            Self::GatewayTimeout => {
                "The server did not get a response in time from the upstream server."
            }
        }
    }
}
