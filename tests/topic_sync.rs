mod common;

use std::sync::Arc;

use kafka_api_gateway::kafka::{
    registry::TopicRegistry,
    sync::{SyncReport, TopicSync},
};

use self::common::fakes::{FakeLister, FakeStore};

// Builds a sync over the given fake store and fake broker, with its registry
fn sync_with(store: FakeStore, lister: FakeLister) -> (TopicSync, Arc<TopicRegistry>) {
    let registry = Arc::new(TopicRegistry::default());
    let sync = TopicSync::new(Arc::new(store), Arc::new(lister), registry.clone());

    (sync, registry)
}

// Every enabled topic exists on the broker, so the report is ok
#[tokio::test]
async fn report_is_ok_when_all_enabled_topics_exist() {
    let store = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("payments", false, false, true, true);
    let (sync, _) = sync_with(
        store,
        FakeLister::with_topics(&["orders", "payments", "extra"]),
    );

    let report = sync.run().await.unwrap();

    assert!(report.is_ok());
    assert!(report.missing_topics.is_empty());
    assert_eq!(report.kafka_topic_count, 3);
}

// An enabled topic that the broker does not have is reported as missing
#[tokio::test]
async fn missing_enabled_topic_is_reported() {
    let store = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("ghost", true, false, false, true);
    let (sync, _) = sync_with(store, FakeLister::with_topics(&["orders"]));

    let report = sync.run().await.unwrap();

    assert!(!report.is_ok());
    assert_eq!(report.missing_topics, vec!["ghost"]);
}

// A disabled topic is never checked, so it is not reported even when the broker lacks it
#[tokio::test]
async fn disabled_topic_is_not_checked() {
    let store = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("retired", true, false, false, false);
    let (sync, _) = sync_with(store, FakeLister::with_topics(&["orders"]));

    let report = sync.run().await.unwrap();

    assert!(report.is_ok());
}

// Only consumed topics that exist on the broker can be subscribed
#[tokio::test]
async fn subscribable_topics_skip_missing_and_publish_only_topics() {
    let store = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("ghost", true, false, false, true)
        .with_topic("publish-only", false, false, true, true);
    let (sync, _) = sync_with(store, FakeLister::with_topics(&["orders", "publish-only"]));

    let report = sync.run().await.unwrap();

    assert_eq!(report.subscribable_topics(), vec!["orders"]);
}

// The registry holds the new policy even when the broker is missing topics
#[tokio::test]
async fn registry_is_updated_even_when_topics_are_missing() {
    let store = FakeStore::new().with_topic("ghost", true, false, false, true);
    let (sync, registry) = sync_with(store, FakeLister::with_topics(&[]));

    let report = sync.run().await.unwrap();

    assert!(!report.is_ok());
    assert!(registry.snapshot().is_enabled("ghost"));
}

// A database failure returns the error and leaves the registry empty
#[tokio::test]
async fn database_failure_is_returned() {
    let (sync, registry) = sync_with(FakeStore::new().broken(), FakeLister::with_topics(&[]));

    let result = sync.run().await;

    assert!(result.is_err());
    assert!(registry.snapshot().enabled_topics().is_empty());
}

// A broker failure returns the error, the policy was already loaded by then
#[tokio::test]
async fn broker_failure_is_returned() {
    let store = FakeStore::new().with_topic("orders", true, false, false, true);
    let (sync, registry) = sync_with(store, FakeLister::broken());

    let result = sync.run().await;

    assert!(result.is_err());
    assert!(registry.snapshot().is_enabled("orders"));
}

// Runs two syncs against one shared registry and returns the report of the second one
async fn second_report(first: FakeStore, second: FakeStore) -> SyncReport {
    let registry = Arc::new(TopicRegistry::default());
    let lister = Arc::new(FakeLister::with_topics(&["orders", "payments"]));

    TopicSync::new(Arc::new(first), lister.clone(), registry.clone())
        .run()
        .await
        .unwrap();

    TopicSync::new(Arc::new(second), lister, registry)
        .run()
        .await
        .unwrap()
}

// The first load compares against an empty policy, so it counts as a change
#[tokio::test]
async fn first_load_is_a_change() {
    let store = FakeStore::new().with_topic("orders", true, false, false, true);
    let (sync, _) = sync_with(store, FakeLister::with_topics(&["orders"]));

    let report = sync.run().await.unwrap();

    assert!(report.policy_changed());
}

// A reload that finds the same topics and flags is not a change, so it can stay silent
#[tokio::test]
async fn identical_reload_is_not_a_change() {
    let rows = || FakeStore::new().with_topic("orders", true, false, false, true);

    let report = second_report(rows(), rows()).await;

    assert!(!report.policy_changed());
}

// A changed flag is a change
#[tokio::test]
async fn changed_flag_is_a_change() {
    let first = FakeStore::new().with_topic("orders", true, false, false, true);
    let second = FakeStore::new().with_topic("orders", true, true, false, true);

    let report = second_report(first, second).await;

    assert!(report.policy_changed());
}

// A new topic and a disabled topic are both changes
#[tokio::test]
async fn added_or_disabled_topic_is_a_change() {
    let first = FakeStore::new().with_topic("orders", true, false, false, true);
    let added = FakeStore::new()
        .with_topic("orders", true, false, false, true)
        .with_topic("payments", true, false, false, true);
    let disabled = FakeStore::new().with_topic("orders", true, false, false, false);

    assert!(second_report(first, added).await.policy_changed());

    let first = FakeStore::new().with_topic("orders", true, false, false, true);
    assert!(second_report(first, disabled).await.policy_changed());
}

// A disabled row that stays disabled is not a change, disabled topics are not part of the policy
#[tokio::test]
async fn unchanged_disabled_topic_is_not_a_change() {
    let rows = || {
        FakeStore::new()
            .with_topic("orders", true, false, false, true)
            .with_topic("retired", true, false, false, false)
    };

    let report = second_report(rows(), rows()).await;

    assert!(!report.policy_changed());
}
