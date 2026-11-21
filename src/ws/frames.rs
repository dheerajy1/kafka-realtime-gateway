use std::fmt::Display;

use serde::{Serialize, Serializer};

use crate::errors::api_error::ApiError;

// Every text frame the gateway sends over a WebSocket
// The fields are borrowed, so serde writes the text once and nothing is cloned into a map first
// The tag becomes the "type" field, and two variants share the type "error" because clients tell them apart by their fields
#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum Frame<'a> {
    // The connection is open and the client can send messages
    Ready {
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<&'a str>,
    },

    // The connection failed authentication, same fields as the HTTP error shape
    #[serde(rename = "error")]
    AuthFailed {
        success: bool,
        status_code: u16,
        code: &'a str,
        message: &'a str,
        error: &'a str,
    },

    // A plain error with a message
    #[serde(rename = "error")]
    Message {
        message: &'a str,
    },

    // An error with a reason, and the id of the request or message it is about when there is one
    #[serde(rename = "error")]
    Rejected {
        message: &'a str,
        reason: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        correlation_id: Option<&'a str>,
    },

    // A message of the wrong shape, with what is wrong
    #[serde(rename = "error")]
    InvalidMessage {
        message: &'a str,
        issues: Issues<'a>,
    },

    // The client is now subscribed to the topic
    Subscribed {
        topic: &'a str,
    },

    // The client is no longer subscribed to the topic
    Unsubscribed {
        topic: &'a str,
    },

    // The gateway accepted the processed message of a subscriber
    AckReceived {
        correlation_id: &'a str,
    },

    // The gateway published the message to Kafka, this means published, not processed
    Ack {
        topic: &'a str,
        correlation_id: &'a str,
    },

    // The gateway published the message of a subscriber, the offset is text because a 64 bit number can lose digits in JavaScript
    Published {
        request_id: &'a str,
        topic: &'a str,
        partition: i32,
        offset: AsText<i64>,
    },
}

// A list of problems that serializes as [{"message": "..."}] without building a second list
pub struct Issues<'a>(pub &'a [String]);

impl Serialize for Issues<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Issue<'a> {
            message: &'a str,
        }

        serializer.collect_seq(self.0.iter().map(|message| Issue { message }))
    }
}

// A value that serializes as JSON text through its Display, so no String is built for it
pub struct AsText<T>(pub T);

impl<T: Display> Serialize for AsText<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl Frame<'_> {
    // The JSON text of the frame
    pub fn to_text(&self) -> String {
        // The fields are plain text and numbers, so serializing cannot fail
        serde_json::to_string(self).expect("a frame always serializes")
    }
}

// The error frame for a failed authentication, the fields of the HTTP error shape plus the type
pub fn auth_error_frame(error: &ApiError) -> String {
    Frame::AuthFailed {
        success: false,
        status_code: error.code().status_code(),
        code: error.code().as_str(),
        message: error.message(),
        error: error.error(),
    }
    .to_text()
}

// A plain error frame with a message
pub fn error_frame(message: &str) -> String {
    Frame::Message { message }.to_text()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    // Parses the text of a frame so the tests compare fields and not key order
    fn parsed(frame: &Frame) -> Value {
        serde_json::from_str(&frame.to_text()).unwrap()
    }

    // The ready frame has the message only when it is given
    #[test]
    fn ready_frame_has_an_optional_message() {
        assert_eq!(
            parsed(&Frame::Ready { message: None }),
            json!({ "type": "ready" })
        );
        assert_eq!(
            parsed(&Frame::Ready {
                message: Some("hello")
            }),
            json!({ "type": "ready", "message": "hello" })
        );
    }

    // The two plain error frames both have the type error
    #[test]
    fn error_frames_have_the_error_type() {
        assert_eq!(
            serde_json::from_str::<Value>(&error_frame("Malformed JSON")).unwrap(),
            json!({ "type": "error", "message": "Malformed JSON" })
        );
        assert_eq!(
            parsed(&Frame::Rejected {
                message: "ACK rejected",
                reason: "not-found",
                request_id: None,
                correlation_id: Some("corr-1"),
            }),
            json!({
                "type": "error",
                "message": "ACK rejected",
                "reason": "not-found",
                "correlationId": "corr-1",
            })
        );
        assert_eq!(
            parsed(&Frame::Rejected {
                message: "Unauthorized topic",
                reason: "topic-not-allowed",
                request_id: Some("req-1"),
                correlation_id: None,
            }),
            json!({
                "type": "error",
                "message": "Unauthorized topic",
                "reason": "topic-not-allowed",
                "requestId": "req-1",
            })
        );
    }

    // The invalid message frame lists every problem as an object with a message
    #[test]
    fn invalid_message_lists_the_issues() {
        let issues = vec!["topic is required".to_string(), "bad value".to_string()];

        assert_eq!(
            parsed(&Frame::InvalidMessage {
                message: "Invalid message format",
                issues: Issues(&issues),
            }),
            json!({
                "type": "error",
                "message": "Invalid message format",
                "issues": [{ "message": "topic is required" }, { "message": "bad value" }],
            })
        );
    }

    // The names of the other frames match what clients read
    #[test]
    fn named_frames_match_the_wire_names() {
        assert_eq!(
            parsed(&Frame::Subscribed { topic: "jobs" }),
            json!({ "type": "subscribed", "topic": "jobs" })
        );
        assert_eq!(
            parsed(&Frame::Unsubscribed { topic: "jobs" }),
            json!({ "type": "unsubscribed", "topic": "jobs" })
        );
        assert_eq!(
            parsed(&Frame::AckReceived {
                correlation_id: "corr-1"
            }),
            json!({ "type": "ack-received", "correlationId": "corr-1" })
        );
        assert_eq!(
            parsed(&Frame::Ack {
                topic: "jobs",
                correlation_id: "corr-1"
            }),
            json!({ "type": "ack", "topic": "jobs", "correlationId": "corr-1" })
        );
    }

    // The published frame sends the offset as text, even the largest one
    #[test]
    fn published_frame_sends_the_offset_as_text() {
        assert_eq!(
            parsed(&Frame::Published {
                request_id: "req-1",
                topic: "jobs",
                partition: 2,
                offset: AsText(i64::MAX),
            }),
            json!({
                "type": "published",
                "requestId": "req-1",
                "topic": "jobs",
                "partition": 2,
                "offset": "9223372036854775807",
            })
        );
    }

    // The auth failure frame has the HTTP error shape plus the type
    #[test]
    fn auth_failure_frame_has_the_http_error_shape() {
        let error = ApiError::from_entry(crate::errors::catalog::unauthorized::INVALID_API_KEY);

        let value: Value = serde_json::from_str(&auth_error_frame(&error)).unwrap();

        assert_eq!(value["type"], "error");
        assert_eq!(value["success"], false);
        assert_eq!(value["statusCode"], 401);
        assert_eq!(value["code"], "UNAUTHORIZED");
        assert_eq!(value["message"], error.message());
        assert_eq!(value["error"], error.error());
    }
}
