//! Per-lane rolling transcript: the memory both `side_question` and `fork`
//! read from. Persisted, capped — the phone consumes deliveries, but the
//! transcript retains what was said.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

const MAX_TURNS_PER_LANE: usize = 40;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub role: String, // "user" | "assistant"
    pub text: String,
    pub ts: u64,
}

#[derive(Default)]
pub struct TranscriptStore {
    path: Option<PathBuf>,
    lanes: Mutex<HashMap<String, Vec<Turn>>>,
}

use std::sync::Mutex;

impl TranscriptStore {
    pub fn load(path: PathBuf) -> Self {
        let lanes = fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Self {
            path: Some(path),
            lanes: Mutex::new(lanes),
        }
    }

    pub fn push(&self, lane_id: &str, role: &str, text: &str) {
        if text.trim().is_empty() {
            return;
        }
        let mut lanes = self.lanes.lock().unwrap();
        let v = lanes.entry(lane_id.to_string()).or_default();
        v.push(Turn {
            role: role.into(),
            text: text.into(),
            ts: now(),
        });
        let excess = v.len().saturating_sub(MAX_TURNS_PER_LANE);
        if excess > 0 {
            v.drain(0..excess);
        }
        drop(lanes);
        self.persist();
    }

    /// Last `n` turns, oldest first.
    pub fn recent(&self, lane_id: &str, n: usize) -> Vec<Turn> {
        let lanes = self.lanes.lock().unwrap();
        let v = lanes.get(lane_id).cloned().unwrap_or_default();
        v.into_iter().rev().take(n).rev().collect()
    }

    fn persist(&self) {
        if let Some(p) = &self.path {
            if let Ok(json) = serde_json::to_string(&*self.lanes.lock().unwrap()) {
                let _ = fs::write(p, json);
            }
        }
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
