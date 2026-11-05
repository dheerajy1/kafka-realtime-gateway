use std::{thread::sleep, time::Duration};

use kafka_api_gateway::kafka::activity::{ConsumerActivity, ProducerActivity};

// Before the first publish there is nothing to report
#[test]
fn producer_starts_empty() {
    let snapshot = ProducerActivity::default().snapshot();

    assert_eq!(snapshot.last_publish_ago_ms, None);
    assert_eq!(snapshot.last_publish_ok, None);
}

// The producer remembers whether the last publish worked
#[test]
fn producer_records_the_last_result() {
    let activity = ProducerActivity::default();

    activity.record(true);

    let snapshot = activity.snapshot();

    assert!(snapshot.last_publish_ago_ms.is_some());
    assert_eq!(snapshot.last_publish_ok, Some(true));

    activity.record(false);

    assert_eq!(activity.snapshot().last_publish_ok, Some(false));
}

// A new consumer loop is stopped and has no history
#[test]
fn consumer_starts_stopped() {
    let snapshot = ConsumerActivity::default().snapshot();

    assert!(!snapshot.running);
    assert_eq!(snapshot.last_poll_ago_ms, None);
    assert_eq!(snapshot.last_error, None);
}

// The running flag follows the loop
#[test]
fn consumer_running_flag_can_be_switched() {
    let activity = ConsumerActivity::default();

    activity.set_running(true);
    assert!(activity.snapshot().running);

    activity.set_running(false);
    assert!(!activity.snapshot().running);
}

// A round that worked stamps the time and clears the last error
#[test]
fn consumer_ok_round_clears_the_error() {
    let activity = ConsumerActivity::default();

    activity.record_error("broker not available");
    assert_eq!(
        activity.snapshot().last_error.as_deref(),
        Some("broker not available")
    );

    activity.record_ok();

    let snapshot = activity.snapshot();

    assert!(snapshot.last_poll_ago_ms.is_some());
    assert_eq!(snapshot.last_error, None);
}

// A failed poll keeps the error but does not count as a round that worked
#[test]
fn consumer_error_does_not_stamp_the_time() {
    let activity = ConsumerActivity::default();

    activity.record_error("timed out");

    assert_eq!(activity.snapshot().last_poll_ago_ms, None);
}

// A very long error text is shortened
#[test]
fn consumer_error_is_shortened() {
    let activity = ConsumerActivity::default();

    activity.record_error(&"x".repeat(500));

    assert_eq!(activity.snapshot().last_error.unwrap().chars().count(), 200);
}

// A running consumer with a recent good round is not stalled
#[test]
fn consumer_is_not_stalled_after_a_good_round() {
    let activity = ConsumerActivity::default();

    activity.set_running(true);
    activity.record_ok();

    assert!(!activity.snapshot().stalled);
}

// A running consumer with no good round for the stall time is stalled until the next good round
#[test]
fn consumer_is_stalled_when_the_rounds_stop() {
    let activity = ConsumerActivity::with_stall_after(Duration::from_millis(20));

    activity.set_running(true);
    activity.record_ok();

    sleep(Duration::from_millis(60));

    assert!(activity.snapshot().stalled);

    activity.record_ok();

    assert!(!activity.snapshot().stalled);
}

// A consumer that has not had its first round yet is not stalled
#[test]
fn consumer_without_a_round_is_not_stalled() {
    let activity = ConsumerActivity::with_stall_after(Duration::from_millis(20));

    activity.set_running(true);

    sleep(Duration::from_millis(60));

    assert!(!activity.snapshot().stalled);
}

// A stopped consumer is never stalled
#[test]
fn stopped_consumer_is_not_stalled() {
    let activity = ConsumerActivity::with_stall_after(Duration::from_millis(20));

    activity.record_ok();

    sleep(Duration::from_millis(60));

    assert!(!activity.snapshot().stalled);
}
