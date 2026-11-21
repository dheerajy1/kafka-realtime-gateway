use kafka_api_gateway::components::health_parts::{format_latency, format_number};

// Numbers print with a comma after every group of three digits
#[test]
fn format_number_groups_thousands() {
    assert_eq!(format_number(0).to_string(), "0");
    assert_eq!(format_number(999).to_string(), "999");
    assert_eq!(format_number(1_000).to_string(), "1,000");
    assert_eq!(format_number(12_345).to_string(), "12,345");
    assert_eq!(format_number(123_456).to_string(), "123,456");
    assert_eq!(format_number(1_234_567).to_string(), "1,234,567");
}

// The largest u64 has 20 digits and still fits the stack buffer
#[test]
fn format_number_handles_the_largest_value() {
    assert_eq!(
        format_number(u64::MAX).to_string(),
        "18,446,744,073,709,551,615"
    );
}

// A missing latency prints as a dash, a known one as plain milliseconds
#[test]
fn format_latency_prints_a_dash_or_the_milliseconds() {
    assert_eq!(format_latency(None).to_string(), "—");
    assert_eq!(format_latency(Some(12)).to_string(), "12");
}
