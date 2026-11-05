use std::collections::BTreeMap;

use serde_json::json;
use utoipa::{
    Modify,
    openapi::{
        ContentBuilder, OpenApi, Ref, RefOr, Response, ResponseBuilder, example::ExampleBuilder,
    },
};

use crate::errors::catalog::{
    CatalogEntry, bad_gateway, bad_request, conflict, forbidden, gateway_timeout,
    internal_server_error, not_found, payload_too_large, service_unavailable, unauthorized,
};

// Errors documented per operation, keyed by the operation id set in the handler annotation
const OPERATION_ERRORS: &[(&str, &[CatalogEntry])] = &[
    (
        "health_unified",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            bad_request::INVALID_HEADER_VALUES,
            service_unavailable::DB_PROBE_FAILED,
            service_unavailable::KAFKA_PROBE_FAILED,
        ],
    ),
    (
        "health_app",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            bad_request::INVALID_HEADER_VALUES,
        ],
    ),
    (
        "health_db",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            bad_request::INVALID_HEADER_VALUES,
            service_unavailable::DB_PROBE_FAILED,
        ],
    ),
    (
        "health_kafka",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            bad_request::INVALID_HEADER_VALUES,
            service_unavailable::KAFKA_PROBE_FAILED,
        ],
    ),
    (
        "auth_login",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            bad_request::INVALID_HEADER_VALUES,
            bad_request::INVALID_INPUT,
            unauthorized::INVALID_CREDENTIALS,
            internal_server_error::DATABASE_ERROR,
            internal_server_error::TOKEN_SIGNING_FAILED,
        ],
    ),
    (
        "api_keys_create",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            unauthorized::MISSING_AUTHORIZATION_TOKEN,
            unauthorized::INVALID_TOKEN,
            unauthorized::EXPIRED_TOKEN,
            unauthorized::MISSING_SUB_OR_ROLE,
            bad_request::INVALID_HEADER_VALUES,
            bad_request::INVALID_INPUT,
            conflict::DATABASE_CONFLICT,
            internal_server_error::SERVER_ERROR,
        ],
    ),
    (
        "http_publish",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            unauthorized::MISSING_API_KEY,
            unauthorized::INVALID_API_KEY,
            bad_request::INVALID_HEADER_VALUES,
            bad_request::INVALID_INPUT,
            forbidden::ACCESS_DENIED,
            not_found::RESOURCE_NOT_FOUND,
            payload_too_large::KAFKA_MESSAGE_TOO_LARGE,
            internal_server_error::DATABASE_ERROR,
            internal_server_error::THIRD_PARTY_ERROR,
            bad_gateway::UPSTREAM_UNAVAILABLE,
            gateway_timeout::UPSTREAM_TIMEOUT,
        ],
    ),
    (
        "topics_reload",
        &[
            unauthorized::MISSING_AUTH_HEADERS,
            unauthorized::MISSING_API_KEY,
            unauthorized::INVALID_API_KEY,
            bad_request::INVALID_HEADER_VALUES,
            internal_server_error::DATABASE_ERROR,
            internal_server_error::THIRD_PARTY_ERROR,
            bad_gateway::UPSTREAM_UNAVAILABLE,
            gateway_timeout::UPSTREAM_TIMEOUT,
        ],
    ),
    ("ws_subscribe", &[bad_request::INVALID_HEADER_VALUES]),
];

// Builds one response per http status, each with a named example per catalogue entry
fn build_error_responses(entries: &[CatalogEntry]) -> BTreeMap<String, RefOr<Response>> {
    // Groups the entries by http status
    let mut grouped: BTreeMap<u16, Vec<&CatalogEntry>> = BTreeMap::new();

    for entry in entries {
        grouped
            .entry(entry.code.http_code())
            .or_default()
            .push(entry);
    }

    grouped
        .into_iter()
        .map(|(status, group)| {
            // The example name is the short error text of the entry
            let examples = group.iter().map(|entry| {
                let example = ExampleBuilder::new()
                    .value(Some(json!({
                        "success": false,
                        "httpCode": entry.code.http_code(),
                        "code": entry.code.as_str(),
                        "message": entry.message,
                        "error": entry.error,
                    })))
                    .build();

                (entry.error.to_string(), example)
            });

            let content = ContentBuilder::new()
                .schema(Some(Ref::from_schema_name("ErrorResponse")))
                .examples_from_iter(examples)
                .build();

            // The description is the code name with spaces, like the Elysia version
            let response = ResponseBuilder::new()
                .description(group[0].code.as_str().replace('_', " "))
                .content("application/json", content)
                .build();

            (status.to_string(), RefOr::T(response))
        })
        .collect()
}

// Adds the error responses of OPERATION_ERRORS to the matching operations of the spec
pub struct ErrorExamples;

impl Modify for ErrorExamples {
    fn modify(&self, openapi: &mut OpenApi) {
        for path_item in openapi.paths.paths.values_mut() {
            let operations = [
                &mut path_item.get,
                &mut path_item.post,
                &mut path_item.put,
                &mut path_item.delete,
                &mut path_item.patch,
            ];

            for operation in operations.into_iter().flatten() {
                let Some(operation_id) = operation.operation_id.clone() else {
                    continue;
                };

                let Some((_, entries)) = OPERATION_ERRORS
                    .iter()
                    .find(|(name, _)| *name == operation_id)
                else {
                    continue;
                };

                operation
                    .responses
                    .responses
                    .extend(build_error_responses(entries));
            }
        }
    }
}
