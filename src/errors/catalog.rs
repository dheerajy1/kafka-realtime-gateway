use super::code::ErrorCode;

// One catalogue entry, a fixed code with its user message and short error text
#[derive(Debug, Clone, Copy)]
pub struct CatalogEntry {
    pub code: ErrorCode,
    pub message: &'static str,
    pub error: &'static str,
}

impl CatalogEntry {
    const fn new(code: ErrorCode, message: &'static str, error: &'static str) -> Self {
        Self {
            code,
            message,
            error,
        }
    }
}

// 401 entries
pub mod unauthorized {
    use super::{CatalogEntry, ErrorCode};

    pub const MISSING_AUTH_HEADERS: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "Authentication information is required to continue.",
        "Missing required authentication headers",
    );

    pub const INVALID_AUTH_HEADERS: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "The authentication information provided is not valid.",
        "Invalid authentication header values",
    );

    pub const MISSING_AUTHORIZATION_TOKEN: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "You must be logged in to access this resource.",
        "Missing required Authorization token",
    );

    pub const EXPIRED_TOKEN: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "Your session has expired. Please log in again.",
        "Expired token",
    );

    pub const INVALID_TOKEN: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "Your session is no longer valid. Please log in again.",
        "Invalid token",
    );

    pub const USER_NOT_FOUND: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "No account was found with the provided credentials.",
        "User does not exist, please signup",
    );

    pub const INVALID_CREDENTIALS: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "The username or password you entered is incorrect.",
        "Invalid credentials",
    );

    pub const MISSING_SUB_OR_ROLE: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "JWT is missing required subject (sub) or role claim",
        "missing_sub_or_role",
    );

    pub const MISSING_API_KEY: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "Authentication information api key or api secret is required to continue.",
        "Missing required authentication api key or api secret headers",
    );

    pub const INVALID_API_KEY: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "The authentication information api key or api secret provided is not valid.",
        "Invalid authentication api key or api secret header values",
    );

    pub const INVALID_API_SECRET: CatalogEntry = CatalogEntry::new(
        ErrorCode::Unauthorized,
        "The authentication information api secret provided is not valid.",
        "Invalid authentication api secret header values",
    );
}

// 400 entries
pub mod bad_request {
    use super::{CatalogEntry, ErrorCode};

    pub const INVALID_HEADER_VALUES: CatalogEntry = CatalogEntry::new(
        ErrorCode::BadRequest,
        "Some request information is invalid or malformed.",
        "Invalid authentication header values",
    );

    pub const INVALID_INPUT: CatalogEntry = CatalogEntry::new(
        ErrorCode::BadRequest,
        "One or more input fields contain invalid values.",
        "Invalid input data",
    );

    pub const EMPTY_LOGS: CatalogEntry = CatalogEntry::new(
        ErrorCode::BadRequest,
        "At least one log entry is required.",
        "Logs array is empty",
    );

    pub const INVALID_LOG_FORMAT: CatalogEntry = CatalogEntry::new(
        ErrorCode::BadRequest,
        "One or more log entries have an invalid format.",
        "Invalid log format",
    );
}

// 403 entries
pub mod forbidden {
    use super::{CatalogEntry, ErrorCode};

    pub const ACCESS_DENIED: CatalogEntry = CatalogEntry::new(
        ErrorCode::Forbidden,
        "You are not allowed to perform this action.",
        "You do not have permission to perform this action",
    );
}

// 404 entries
pub mod not_found {
    use super::{CatalogEntry, ErrorCode};

    pub const RESOURCE_NOT_FOUND: CatalogEntry = CatalogEntry::new(
        ErrorCode::NotFound,
        "The requested resource does not exist or is no longer available.",
        "The requested resource does not exist",
    );
}

// 409 entries
pub mod conflict {
    use super::{CatalogEntry, ErrorCode};

    pub const DATABASE_CONFLICT: CatalogEntry = CatalogEntry::new(
        ErrorCode::Conflict,
        "The operation could not be completed due to a data conflict.",
        "Database conflict error",
    );

    pub const DUPLICATE_RECORD: CatalogEntry = CatalogEntry::new(
        ErrorCode::Conflict,
        "A record with the same details already exists.",
        "Duplicate record constraint violation",
    );

    pub const FOREIGN_KEY_VIOLATION: CatalogEntry = CatalogEntry::new(
        ErrorCode::Conflict,
        "This operation cannot be completed because related data exists.",
        "Foreign key constraint violation",
    );

    pub const INVALID_STATE: CatalogEntry = CatalogEntry::new(
        ErrorCode::Conflict,
        "The request cannot be processed in the current state of the resource.",
        "Invalid resource state",
    );

    pub const BUSINESS_RULE_VIOLATION: CatalogEntry = CatalogEntry::new(
        ErrorCode::Conflict,
        "The operation violates a business rule defined in the system.",
        "Business rule violation",
    );

    pub const CONCURRENCY_CONFLICT: CatalogEntry = CatalogEntry::new(
        ErrorCode::Conflict,
        "The resource was modified by another process. Please retry.",
        "Concurrency conflict detected",
    );
}

// 412 entries
pub mod precondition_failed {
    use super::{CatalogEntry, ErrorCode};

    pub const ETAG_MISMATCH: CatalogEntry = CatalogEntry::new(
        ErrorCode::PreconditionFailed,
        "The provided ETag does not match the current resource.",
        "ETag mismatch",
    );

    pub const IF_MATCH_REQUIRED: CatalogEntry = CatalogEntry::new(
        ErrorCode::PreconditionFailed,
        "The request requires an If-Match header that was not provided.",
        "Missing If-Match header",
    );

    pub const MODIFIED_SINCE_FAILED: CatalogEntry = CatalogEntry::new(
        ErrorCode::PreconditionFailed,
        "The resource has been modified since the specified time.",
        "Resource modified",
    );
}

// 413 entries
pub mod payload_too_large {
    use super::{CatalogEntry, ErrorCode};

    pub const FILE_TOO_LARGE: CatalogEntry = CatalogEntry::new(
        ErrorCode::PayloadTooLarge,
        "The uploaded file exceeds the maximum allowed size.",
        "File size exceeds limit",
    );

    pub const BODY_TOO_LARGE: CatalogEntry = CatalogEntry::new(
        ErrorCode::PayloadTooLarge,
        "The request body exceeds the server's size limit.",
        "Request body too large",
    );

    pub const MULTIPART_TOO_LARGE: CatalogEntry = CatalogEntry::new(
        ErrorCode::PayloadTooLarge,
        "One or more parts of the multipart request are too large.",
        "Multipart payload exceeds limit",
    );

    pub const KAFKA_MESSAGE_TOO_LARGE: CatalogEntry = CatalogEntry::new(
        ErrorCode::PayloadTooLarge,
        "The message is too large for the Kafka broker to accept.",
        "Kafka message exceeds limit",
    );
}

// 415 entries
pub mod unsupported_media_type {
    use super::{CatalogEntry, ErrorCode};

    pub const INVALID_CONTENT_TYPE: CatalogEntry = CatalogEntry::new(
        ErrorCode::UnsupportedMediaType,
        "The Content-Type header is not supported.",
        "Unsupported content type",
    );

    pub const UNSUPPORTED_CHARSET: CatalogEntry = CatalogEntry::new(
        ErrorCode::UnsupportedMediaType,
        "The specified charset is not supported.",
        "Unsupported charset",
    );

    pub const WRONG_ACCEPT_HEADER: CatalogEntry = CatalogEntry::new(
        ErrorCode::UnsupportedMediaType,
        "The Accept header specifies an unsupported format.",
        "Unsupported accept format",
    );
}

// 422 entries
pub mod unprocessable_content {
    use super::{CatalogEntry, ErrorCode};

    pub const VALIDATION_FAILED: CatalogEntry = CatalogEntry::new(
        ErrorCode::UnprocessableContent,
        "Validation of the request data failed.",
        "Validation error",
    );

    pub const SEMANTIC_ERROR: CatalogEntry = CatalogEntry::new(
        ErrorCode::UnprocessableContent,
        "The request is syntactically correct but semantically invalid.",
        "Semantic error",
    );

    pub const MISSING_DEPENDENCY: CatalogEntry = CatalogEntry::new(
        ErrorCode::UnprocessableContent,
        "Required related data is missing or invalid.",
        "Missing dependency",
    );
}

// 428 entries
pub mod precondition_required {
    use super::{CatalogEntry, ErrorCode};

    pub const MISSING_IF_UNMODIFIED_SINCE: CatalogEntry = CatalogEntry::new(
        ErrorCode::PreconditionRequired,
        "The If-Unmodified-Since header is required but missing.",
        "Missing If-Unmodified-Since",
    );

    pub const MISSING_IF_NONE_MATCH: CatalogEntry = CatalogEntry::new(
        ErrorCode::PreconditionRequired,
        "The If-None-Match header is required but missing.",
        "Missing If-None-Match",
    );
}

// 429 entries
pub mod too_many_requests {
    use super::{CatalogEntry, ErrorCode};

    pub const RATE_LIMIT_EXCEEDED: CatalogEntry = CatalogEntry::new(
        ErrorCode::TooManyRequests,
        "You have exceeded the allowed number of requests.",
        "Rate limit exceeded",
    );

    pub const BURST_LIMIT_EXCEEDED: CatalogEntry = CatalogEntry::new(
        ErrorCode::TooManyRequests,
        "Too many requests in a short time window.",
        "Burst rate limit exceeded",
    );

    pub const DAILY_LIMIT_REACHED: CatalogEntry = CatalogEntry::new(
        ErrorCode::TooManyRequests,
        "Your daily request quota has been reached.",
        "Daily quota exceeded",
    );
}

// 499 entries
pub mod client_closed_request {
    use super::{CatalogEntry, ErrorCode};

    pub const CONNECTION_CLOSED: CatalogEntry = CatalogEntry::new(
        ErrorCode::ClientClosedRequest,
        "The client closed the connection before the response was sent.",
        "Client connection closed",
    );

    pub const REQUEST_CANCELLED: CatalogEntry = CatalogEntry::new(
        ErrorCode::ClientClosedRequest,
        "The client cancelled the request.",
        "Request cancelled by client",
    );
}

// 500 entries
pub mod internal_server_error {
    use super::{CatalogEntry, ErrorCode};

    pub const SERVER_ERROR: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "Something went wrong. Please try again later.",
        "Something went wrong on the server",
    );

    pub const TOKEN_SIGNING_FAILED: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "Failed to generate authentication token due to an internal error.",
        "Token signing failed",
    );

    pub const TOKEN_VERIFICATION_FAILED: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "Failed to validate authentication token due to an internal error.",
        "Token verification failed",
    );

    pub const DATABASE_ERROR: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "A database error occurred while processing the request.",
        "Database error",
    );

    pub const DB_PERMISSION_DENIED: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "The server does not have permission to access required database resources.",
        "Database permission denied",
    );

    pub const DB_CONNECTION_ERROR: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "Failed to connect to the database.",
        "Database connection error",
    );

    pub const THIRD_PARTY_ERROR: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "An external service returned an unexpected error.",
        "Third-party service error",
    );

    pub const CACHE_FAILURE: CatalogEntry = CatalogEntry::new(
        ErrorCode::InternalServerError,
        "Failed to read from or write to the cache layer.",
        "Cache operation failed",
    );
}

// 501 entries
pub mod not_implemented {
    use super::{CatalogEntry, ErrorCode};

    pub const FEATURE_NOT_ENABLED: CatalogEntry = CatalogEntry::new(
        ErrorCode::NotImplemented,
        "This feature is not enabled on this server.",
        "Feature disabled",
    );

    pub const METHOD_NOT_IMPLEMENTED: CatalogEntry = CatalogEntry::new(
        ErrorCode::NotImplemented,
        "This HTTP method is not implemented yet.",
        "Method not implemented",
    );
}

// 502 entries
pub mod bad_gateway {
    use super::{CatalogEntry, ErrorCode};

    pub const UPSTREAM_UNAVAILABLE: CatalogEntry = CatalogEntry::new(
        ErrorCode::BadGateway,
        "The upstream service is currently unreachable.",
        "Upstream service unavailable",
    );

    pub const INVALID_UPSTREAM_RESPONSE: CatalogEntry = CatalogEntry::new(
        ErrorCode::BadGateway,
        "The upstream server returned an invalid or corrupted response.",
        "Invalid upstream response",
    );
}

// 503 entries
pub mod service_unavailable {
    use super::{CatalogEntry, ErrorCode};

    pub const MAINTENANCE_MODE: CatalogEntry = CatalogEntry::new(
        ErrorCode::ServiceUnavailable,
        "The service is currently under maintenance.",
        "Service in maintenance",
    );

    pub const OVERLOADED: CatalogEntry = CatalogEntry::new(
        ErrorCode::ServiceUnavailable,
        "The server is temporarily overloaded and cannot handle the request.",
        "Server overloaded",
    );

    pub const CIRCUIT_OPEN: CatalogEntry = CatalogEntry::new(
        ErrorCode::ServiceUnavailable,
        "Circuit breaker is open due to repeated failures.",
        "Circuit breaker open",
    );

    pub const DB_PROBE_FAILED: CatalogEntry = CatalogEntry::new(
        ErrorCode::ServiceUnavailable,
        "Database connection validation probe failed",
        "Database is disconnected or unreachable",
    );
}

// 504 entries
pub mod gateway_timeout {
    use super::{CatalogEntry, ErrorCode};

    pub const UPSTREAM_TIMEOUT: CatalogEntry = CatalogEntry::new(
        ErrorCode::GatewayTimeout,
        "The upstream service did not respond within the timeout period.",
        "Upstream timeout",
    );

    pub const DNS_RESOLUTION_TIMEOUT: CatalogEntry = CatalogEntry::new(
        ErrorCode::GatewayTimeout,
        "Failed to resolve upstream server address in time.",
        "DNS resolution timeout",
    );
}

// 402 entries
pub mod payment_required {
    use super::{CatalogEntry, ErrorCode};

    pub const SUBSCRIPTION_EXPIRED: CatalogEntry = CatalogEntry::new(
        ErrorCode::PaymentRequired,
        "Your subscription has expired. Please renew to continue.",
        "Subscription expired",
    );
}

// 405 entries
pub mod method_not_supported {
    use super::{CatalogEntry, ErrorCode};

    pub const METHOD_NOT_ALLOWED: CatalogEntry = CatalogEntry::new(
        ErrorCode::MethodNotSupported,
        "The HTTP method used is not supported for this endpoint.",
        "Method not allowed",
    );
}

// 408 entries
pub mod timeout {
    use super::{CatalogEntry, ErrorCode};

    pub const REQUEST_TIMEOUT: CatalogEntry = CatalogEntry::new(
        ErrorCode::Timeout,
        "The request took too long to complete.",
        "Request timed out",
    );
}
