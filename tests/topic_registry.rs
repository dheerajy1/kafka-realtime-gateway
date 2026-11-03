mod common;

use kafka_api_gateway::kafka::registry::{TopicFlags, TopicRegistry};

use self::common::fakes::FakeStore;

// A disabled topic is not part of the policy at all
#[tokio::test]
async fn disabled_topic_is_dropped() {
    let store = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("old-orders", true, true, true, false);
    let registry = TopicRegistry::default();

    registry.reload(&store).await.unwrap();
    let policy = registry.snapshot();

    assert!(policy.is_enabled("orders"));
    assert!(!policy.is_enabled("old-orders"));
    assert_eq!(policy.flags("old-orders"), None);
}

// An unknown topic has no flags and answers false to every question
#[tokio::test]
async fn unknown_topic_answers_false() {
    let registry = TopicRegistry::default();
    registry
        .reload(&FakeStore::new().with_topic("orders", true, true, true, true))
        .await
        .unwrap();
    let policy = registry.snapshot();

    assert!(!policy.is_enabled("missing"));
    assert!(!policy.ack_required("missing"));
    assert!(!policy.subscriber_publish_allowed("missing"));
}

// Each flag is read from its own column
#[tokio::test]
async fn flags_follow_the_rows() {
    let store = FakeStore::new()
        .with_topic("jobs", true, true, false, true)
        .with_topic("events", false, false, true, true);
    let registry = TopicRegistry::default();

    registry.reload(&store).await.unwrap();
    let policy = registry.snapshot();

    assert_eq!(
        policy.flags("jobs"),
        Some(TopicFlags {
            gateway_consume: true,
            ack_required: true,
            subscriber_publish_allowed: false,
        })
    );
    assert!(policy.ack_required("jobs"));
    assert!(!policy.subscriber_publish_allowed("jobs"));
    assert!(!policy.ack_required("events"));
    assert!(policy.subscriber_publish_allowed("events"));
}

// The consume list holds only enabled topics the gateway consumes, and both lists are sorted
#[tokio::test]
async fn topic_lists_are_sorted_and_filtered() {
    let store = FakeStore::new()
        .with_topic("zeta", true, false, false, true)
        .with_topic("alpha", true, false, false, true)
        .with_topic("publish-only", false, false, true, true)
        .with_topic("disabled", true, false, false, false);
    let registry = TopicRegistry::default();

    registry.reload(&store).await.unwrap();
    let policy = registry.snapshot();

    assert_eq!(
        policy.enabled_topics(),
        vec!["alpha", "publish-only", "zeta"]
    );
    assert_eq!(policy.consume_topics(), vec!["alpha", "zeta"]);
}

// A reload replaces the whole policy, so changed flags, disabled topics and new topics all apply
#[tokio::test]
async fn reload_replaces_the_whole_policy() {
    let registry = TopicRegistry::default();
    let first = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("legacy", true, false, false, true);
    let second = FakeStore::new()
        .with_topic("orders", true, true, false, true)
        .with_topic("legacy", true, false, false, false)
        .with_topic("payments", true, false, false, true);

    registry.reload(&first).await.unwrap();
    assert!(!registry.snapshot().ack_required("orders"));
    assert!(registry.snapshot().is_enabled("legacy"));

    registry.reload(&second).await.unwrap();
    let policy = registry.snapshot();

    assert!(policy.ack_required("orders"));
    assert!(!policy.is_enabled("legacy"));
    assert!(policy.is_enabled("payments"));
}

// A snapshot taken before a reload keeps its content
#[tokio::test]
async fn old_snapshot_is_unchanged_by_a_reload() {
    let registry = TopicRegistry::default();
    registry
        .reload(&FakeStore::new().with_topic("orders", true, false, false, true))
        .await
        .unwrap();
    let before = registry.snapshot();

    registry
        .reload(&FakeStore::new().with_topic("payments", true, false, false, true))
        .await
        .unwrap();

    assert!(before.is_enabled("orders"));
    assert!(!before.is_enabled("payments"));
    assert!(registry.snapshot().is_enabled("payments"));
}

// A failed reload returns the error and keeps the policy that was loaded before
#[tokio::test]
async fn failed_reload_keeps_the_old_policy() {
    let registry = TopicRegistry::default();
    registry
        .reload(&FakeStore::new().with_topic("orders", true, false, false, true))
        .await
        .unwrap();

    let result = registry.reload(&FakeStore::new().broken()).await;

    assert!(result.is_err());
    assert!(registry.snapshot().is_enabled("orders"));
}

// Before the first load the registry holds an empty policy
#[tokio::test]
async fn new_registry_is_empty() {
    let registry = TopicRegistry::default();

    assert!(registry.snapshot().enabled_topics().is_empty());
}
