use validator::Validate;

// Header that carries the client id
pub const CLIENT_ID_HEADER: &str = "x-client-id";

// Header that carries the client secret
pub const CLIENT_SECRET_HEADER: &str = "x-client-secret";

// Required client credential headers for public endpoints, borrowed from the request
// No Debug or Clone because it holds the client secret
#[derive(Validate)]
pub struct NoAuthHeaders<'a> {
    // x-client-id used for authentication
    #[validate(length(
        min = 1,
        max = 44,
        message = "x-client-id must be between 1 and 44 characters."
    ))]
    pub client_id: &'a str,

    // x-client-secret used for authentication
    #[validate(length(
        min = 1,
        max = 100,
        message = "x-client-secret must be between 1 and 100 characters."
    ))]
    pub client_secret: &'a str,
}

impl<'a> NoAuthHeaders<'a> {
    // Builds the headers struct, trimming whitespace like the old zod schema did
    pub fn new(client_id: &'a str, client_secret: &'a str) -> Self {
        Self {
            client_id: client_id.trim(),
            client_secret: client_secret.trim(),
        }
    }
}
