use std::borrow::Cow;
use std::fmt::{self, Write};

use dioxus::prelude::*;

use crate::components::icons::{AlertTriangle, CheckCircle, XCircle};
use crate::features::v1::health::model::{DbConnection, DbStatus};
use crate::kafka::health::KafkaStatus;

#[derive(Props, Clone, PartialEq)]
pub struct RowProps {
    // Borrowed for a fixed label written in the source, owned for a label built at run time such as a broker id
    #[props(into)]
    pub label: Cow<'static, str>,
    pub children: Element,
}

// One label and value line inside a health card
#[component]
pub fn Row(props: RowProps) -> Element {
    rsx! {
        div { class: "flex justify-between py-2 border-b border-border last:border-0",
            span { class: "font-semibold text-text-muted", "{props.label}" }
            span { class: "font-mono text-sm flex items-center gap-2", {props.children} }
        }
    }
}

#[derive(Props, Clone, PartialEq)]
pub struct DbStatusProps {
    pub status: DbStatus,
}

// Icon and text for the overall database status
#[component]
pub fn DbStatusValue(props: DbStatusProps) -> Element {
    match props.status {
        DbStatus::Up => rsx! {
            CheckCircle { class: "text-success" }
            "UP"
        },
        DbStatus::Down => rsx! {
            XCircle { class: "text-error" }
            "DOWN"
        },
    }
}

#[derive(Props, Clone, PartialEq)]
pub struct DbConnectionProps {
    pub connection: DbConnection,
}

// Icon and text for the database connection state
#[component]
pub fn DbConnectionValue(props: DbConnectionProps) -> Element {
    match props.connection {
        DbConnection::Connected => rsx! {
            CheckCircle { class: "text-success" }
            "CONNECTED"
        },
        DbConnection::Connecting => rsx! {
            AlertTriangle { class: "text-warning" }
            "CONNECTING"
        },
        DbConnection::Disconnected => rsx! {
            XCircle { class: "text-error" }
            "DISCONNECTED"
        },
    }
}

#[derive(Props, Clone, PartialEq)]
pub struct KafkaStatusProps {
    pub status: KafkaStatus,
}

// Icon and text for the overall Kafka status
#[component]
pub fn KafkaStatusValue(props: KafkaStatusProps) -> Element {
    match props.status {
        KafkaStatus::Up => rsx! {
            CheckCircle { class: "text-success" }
            "UP"
        },
        KafkaStatus::Down => rsx! {
            XCircle { class: "text-error" }
            "DOWN"
        },
    }
}

// A number that prints with thousands separators, for example 12345 prints as 12,345
struct Grouped(u64);

impl fmt::Display for Grouped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A u64 has at most 20 digits, they are kept on the stack, last digit first
        let mut digits = [0u8; 20];
        let mut count = 0;
        let mut rest = self.0;

        loop {
            // The rest of a division by 10 is below 10, so it always fits in a u8
            digits[count] = b'0' + (rest % 10) as u8;
            rest /= 10;
            count += 1;

            if rest == 0 {
                break;
            }
        }

        // Writes from the first digit to the last, straight into the page
        for (index, digit) in digits[..count].iter().enumerate().rev() {
            f.write_char(char::from(*digit))?;

            // A comma goes after every digit that leaves a multiple of three digits behind it
            if index > 0 && index % 3 == 0 {
                f.write_char(',')?;
            }
        }

        Ok(())
    }
}

// An optional latency that prints as the number of milliseconds, or a dash when it is missing
struct Latency(Option<u64>);

impl fmt::Display for Latency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ms) => write!(f, "{ms}"),
            None => f.write_str("—"),
        }
    }
}

// Formats a number with thousands separators, for example 12345 becomes 12,345
// It returns a value that prints itself, so no String is built before it is written into the page
pub fn format_number(value: u64) -> impl fmt::Display {
    Grouped(value)
}

// Formats the optional latency, using a dash when it is missing
pub fn format_latency(value: Option<u64>) -> impl fmt::Display {
    Latency(value)
}

// Formats a size in bytes with a binary unit, for example 1536 becomes 1.5 KiB
pub fn format_bytes(value: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let mut size = value.max(0) as f64;
    let mut unit = 0;

    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{size:.0} {}", UNITS[unit])
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

// Formats the age of an event in milliseconds, using "never" when it did not happen yet
pub fn format_ago(value: Option<u64>) -> String {
    match value {
        Some(ms) if ms < 1000 => format!("{ms} ms ago"),
        Some(ms) => format!("{:.1} s ago", ms as f64 / 1000.0),
        None => "never".to_string(),
    }
}
