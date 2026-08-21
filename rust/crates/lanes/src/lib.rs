//! Lane registry: persistence + mutation. Pure logic lives here;
//! the daemon decides when to load/save.

pub mod meta;
pub mod time;

pub use meta::{execute, match_lane, parse_meta, MetaAction};
pub use time::{age_summary, now_iso};

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Lane {
    pub id: String,
    pub name: String,
    pub created_at: String,
    pub last_active: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interactions: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaneRegistry {
    pub lanes: Vec<Lane>,
    pub active_id: String,
    pub previous_id: String,
}

impl LaneRegistry {
    pub fn default_direct() -> Self {
        let now = now_iso();
        Self {
            lanes: vec![Lane {
                id: "telepathy:direct".into(),
                name: "direct".into(),
                created_at: now.clone(),
                last_active: now,
                interactions: None,
            }],
            active_id: "telepathy:direct".into(),
            previous_id: "telepathy:direct".into(),
        }
    }

    pub fn load(path: &PathBuf) -> Self {
        fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(Self::default_direct)
    }

    pub fn save(&self, path: &PathBuf) {
        if let Some(dir) = path.parent() {
            let _ = fs::create_dir_all(dir);
        }
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = fs::write(path, json);
        }
    }

    /// Panics only if the registry is malformed (empty lanes) — a bug, not input.
    pub fn active(&self) -> &Lane {
        self.lanes.iter().find(|l| l.id == self.active_id).unwrap_or(&self.lanes[0])
    }

    pub fn touch(&mut self, id: &str) {
        if let Some(l) = self.lanes.iter_mut().find(|l| l.id == id) {
            l.last_active = now_iso();
        }
    }

    pub fn switch(&mut self, id: &str) -> Option<Lane> {
        if !self.lanes.iter().any(|l| l.id == id) {
            return None;
        }
        if self.active_id != id {
            self.previous_id = self.active_id.clone();
            self.active_id = id.to_string();
        }
        self.touch(id);
        self.lanes.iter().find(|l| l.id == id).cloned()
    }

    /// Create (or return existing) lane; does NOT switch.
    pub fn create(&mut self, name: &str) -> Lane {
        let slug = slugify(name);
        let id = format!("telepathy:repo:{slug}");
        if let Some(l) = self.lanes.iter().find(|l| l.id == id) {
            return l.clone();
        }
        let now = now_iso();
        let lane = Lane {
            id: id.clone(),
            name: slug,
            created_at: now.clone(),
            last_active: now,
            interactions: None,
        };
        self.lanes.push(lane.clone());
        lane
    }
}

fn slugify(name: &str) -> String {
    let s: String = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    s.trim_matches('-').to_string()
}
