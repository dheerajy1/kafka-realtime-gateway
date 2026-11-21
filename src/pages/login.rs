use std::{borrow::Cow, sync::LazyLock};

use axum::{
    Form, Router,
    extract::{State, rejection::FormRejection},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use validator::Validate;

use super::render::{render_with_props, wants_html};
use crate::{
    auth::password::verify_password,
    components::login::{Login, LoginProps},
    errors::{
        api_error::ApiError,
        catalog::{bad_request, forbidden, internal_server_error, unauthorized},
    },
    middleware::server_gate::auth_cookie_header,
    state::AppState,
};

// Title of the login page
const LOGIN_TITLE: &str = "Login | Kafka realtime gateway";

// Fields of the login form, missing fields become empty and fail the validation
// No Debug or Clone because it holds the password
#[derive(Deserialize, Validate)]
struct LoginForm {
    #[serde(default)]
    #[validate(length(min = 1))]
    username: String,

    #[serde(default)]
    #[validate(length(min = 6))]
    password: String,
}

// Routes of the Login page
pub fn router() -> Router<AppState> {
    Router::new().route("/login", get(login_page).post(login_submit))
}

// Renders the login page with a status and an optional error toast
fn login_html(status: StatusCode, error: Option<Cow<'static, str>>) -> Response {
    let page = render_with_props(
        Login,
        LoginProps {
            title: LOGIN_TITLE,
            error,
        },
    );

    (status, page).into_response()
}

// Renders the login page for a failed login, server errors only show the safe catalogue message
fn login_error(error: ApiError) -> Response {
    let code = error.code();

    let message = if code.status_code() >= 500 {
        Cow::Borrowed(internal_server_error::SERVER_ERROR.message)
    } else {
        error.into_message()
    };

    login_html(code.status(), Some(message))
}

// The login page without an error never changes, so it is rendered once on the first request and reused
static LOGIN_HTML: LazyLock<String> = LazyLock::new(|| {
    render_with_props(
        Login,
        LoginProps {
            title: LOGIN_TITLE,
            error: None,
        },
    )
    .0
});

// GET /login, the cached page is lent to the response, so nothing is rendered or copied per request
async fn login_page() -> Html<&'static str> {
    Html(LOGIN_HTML.as_str())
}

// Checks the username and password against portal_users, the password moves into the check without a copy
async fn authenticate(state: &AppState, form: LoginForm) -> Result<(), ApiError> {
    let hash = state.users.find_portal_user_hash(&form.username).await?;

    // An unknown user is checked against a dummy hash inside, so it takes as long as a wrong password
    if verify_password(form.password, hash).await? {
        Ok(())
    } else {
        Err(ApiError::from_entry(unauthorized::INVALID_CREDENTIALS))
    }
}

// POST /login
async fn login_submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    form: Result<Form<LoginForm>, FormRejection>,
) -> Response {
    // Only browsers use the portal login, other callers get the global error shape
    if !wants_html(&headers) {
        return ApiError::from_entry(forbidden::ACCESS_DENIED).into_response();
    }

    // A body that is not a valid form is treated like invalid input
    let Ok(Form(form)) = form else {
        return login_error(ApiError::from_entry(bad_request::INVALID_INPUT));
    };

    if form.validate().is_err() {
        return login_error(ApiError::from_entry(bad_request::INVALID_INPUT));
    }

    match authenticate(&state, form).await {
        // On success the cookie is set and the browser goes to the home page
        Ok(()) => (
            StatusCode::FOUND,
            [
                (header::LOCATION, "/".to_string()),
                (header::SET_COOKIE, auth_cookie_header()),
            ],
        )
            .into_response(),
        Err(error) => login_error(error),
    }
}
