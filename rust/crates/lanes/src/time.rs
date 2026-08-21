//! Timestamps without a chrono dependency: epoch millis tagged strings.
//! Sufficient for age math; swap for RFC3339 when a UI needs pretty dates.

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_iso() -> String {
    format!("epoch-ms:{}", epoch_ms())
}

pub fn age_summary(last_active: &str) -> String {
    let ms = last_active
        .strip_prefix("epoch-ms:")
        .and_then(|s| s.parse::<u128>().ok())
        .unwrap_or(0);
    match elapsed_hours(ms) {
        0 => "under an hour".to_string(),
        1 => "1 hour".to_string(),
        n => format!("{n} hours"),
    }
}

fn epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn elapsed_hours(since_ms: u128) -> u128 {
    epoch_ms().saturating_sub(since_ms) / 3_600_000
}
