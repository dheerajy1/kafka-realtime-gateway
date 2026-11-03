use super::model::{HttpPublishData, HttpPublishRequest};
use crate::{
    errors::{
        api_error::ApiError,
        catalog::{forbidden, internal_server_error},
    },
    kafka::{
        publisher::{PublishRecord, Publisher},
        registry::TopicRegistry,
    },
};

// Publishes the event to Kafka with the correlation id as the message key
pub async fn publish_event(
    topics: &TopicRegistry,
    publisher: &dyn Publisher,
    request: HttpPublishRequest,
) -> Result<HttpPublishData, ApiError> {
    // D5: only topics that are enabled in the registry may be published to, no database call here
    if !topics.snapshot().is_enabled(&request.topic) {
        return Err(ApiError::from_entry(forbidden::ACCESS_DENIED));
    }

    let value = serde_json::to_string(&request.value).map_err(|error| {
        tracing::error!(target: "Kafka", "event value serialization failed: {error}");
        ApiError::from_entry(internal_server_error::SERVER_ERROR)
    })?;

    publisher
        .publish(PublishRecord {
            topic: request.topic.clone(),
            key: Some(request.correlation_id.clone()),
            headers: Vec::new(),
            value,
        })
        .await?;

    Ok(HttpPublishData {
        topic: request.topic,
        correlation_id: request.correlation_id,
    })
}
