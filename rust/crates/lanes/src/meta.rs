//! The meta grammar: pure function from transcript + registry to action.
//! Requires registry evidence before intercepting lane names — collision
//! safety with coding speech ("switch to main") is structural.

use crate::{Lane, LaneCreateError, LaneRegistry};

#[derive(Debug, Clone)]
pub enum MetaAction {
    Switch(Lane),
    List,
    New(String),
    Brief(Option<Lane>),
    /// "note that X" — persist X to the lane's notes, never conversation.
    Note(String),
    /// "fork [into X]" — new lane seeded with this lane's context.
    Fork(Option<String>),
    Unknown,
}

pub fn normalize(s: &str) -> String {
    let lower = s.to_lowercase();
    let mut out = String::new();
    let mut last_space = true;
    for c in lower.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_string()
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp: Vec<usize> = (0..=b.len()).collect();
    for i in 1..=a.len() {
        let mut prev = dp[0];
        dp[0] = i;
        for j in 1..=b.len() {
            let tmp = dp[j];
            dp[j] = (dp[j] + 1)
                .min(dp[j - 1] + 1)
                .min(prev + if a[i - 1] == b[j - 1] { 0 } else { 1 });
            prev = tmp;
        }
    }
    dp[b.len()]
}

/// Fuzzy lane lookup: exact → substring → bounded edit distance.
/// Space-stripped comparison handles STT mangling of multi-word names.
pub fn match_lane(text: &str, reg: &LaneRegistry) -> Option<Lane> {
    let t = normalize(text);
    let t_flat = t.replace(' ', "");
    if t.is_empty() {
        return None;
    }
    for l in &reg.lanes {
        let n = normalize(&l.name);
        if n == t || l.id == t || n.replace(' ', "") == t_flat {
            return Some(l.clone());
        }
    }
    for l in &reg.lanes {
        let n = normalize(&l.name);
        if t.contains(&n) || t_flat.contains(&n.replace(' ', "")) {
            return Some(l.clone());
        }
    }
    let mut best: Option<Lane> = None;
    let mut best_dist = usize::MAX;
    for l in &reg.lanes {
        let n = normalize(&l.name);
        let tolerance = (n.len() / 3).max(2);
        let d = levenshtein(&t, &n).min(levenshtein(&t_flat, &n.replace(' ', "")));
        if d < best_dist && d <= tolerance {
            best_dist = d;
            best = Some(l.clone());
        }
    }
    best
}

pub fn parse_meta(raw: &str, reg: &LaneRegistry) -> MetaAction {
    let text = normalize(raw);
    if text.is_empty() {
        return MetaAction::Unknown;
    }

    // note that X / remember that X — memory capture, never a task.
    // Content preserves the RAW transcript casing (notes are memory; case matters).
    let raw_trim = raw.trim();
    let raw_folded = raw_trim.to_lowercase();
    let raw_words: Vec<&str> = raw_folded.split(' ').collect();
    if matches!(raw_words.first(), Some(&"note") | Some(&"remember") | Some(&"keep"))
        && raw_words.len() > 1
    {
        fn strip_ci<'a>(raw: &'a str, prefix: &str) -> Option<&'a str> {
            if raw.len() >= prefix.len()
                && raw[..prefix.len()].eq_ignore_ascii_case(prefix)
            {
                Some(raw[prefix.len()..].trim_start())
            } else {
                None
            }
        }
        let content = strip_ci(raw_trim, "keep in mind that ")
            .or_else(|| strip_ci(raw_trim, "remember that "))
            .or_else(|| strip_ci(raw_trim, "note that "))
            .or_else(|| strip_ci(raw_trim, "remember "))
            .or_else(|| strip_ci(raw_trim, "note "))
            .or_else(|| strip_ci(raw_trim, "keep "));
        return match content {
            Some(c) if !c.trim().is_empty() => MetaAction::Note(c.trim().to_string()),
            _ => MetaAction::Unknown, // dangling prefix, no content
        };
    }

    // list conversations
    if (text.starts_with("list")
        || text.starts_with("show")
        || text.starts_with("what are the")
        || text.starts_with("what conversation")
        || text.starts_with("which conversation"))
        && (text.contains("conversation") || text.contains("lane"))
    {
        return MetaAction::List;
    }

    // new conversation [for|about|called] X — keyword required on both sides
    let words: Vec<&str> = text.split(' ').collect();
    if matches!(words.first(), Some(&"new" | &"start" | &"create")) {
        if let Some(kpos) = words
            .iter()
            .position(|w| matches!(*w, "conversation" | "lane" | "chat"))
        {
            // optional preposition after the keyword, then the name
            let rest_start = kpos + 1;
            let name = if rest_start < words.len()
                && matches!(
                    words[rest_start],
                    "for" | "about" | "called" | "named" | "to"
                ) {
                words[rest_start + 1..].join(" ")
            } else {
                words[rest_start..].join(" ")
            };
            if !name.is_empty() {
                return MetaAction::New(name);
            }
        }
    }

    // brief [me] [on X]
    if words.first() == Some(&"brief") || text == "catch me up" || words.first() == Some(&"status")
    {
        let target = if let Some(p) = words
            .iter()
            .position(|w| *w == "on" || *w == "about" || *w == "for")
        {
            Some(words[p + 1..].join(" "))
        } else {
            None
        };
        return MetaAction::Brief(match_lane(target.as_deref().unwrap_or(""), reg));
    }

    // switch/go/work on X — lane match REQUIRED, else fall through
    if matches!(
        words.first(),
        Some(&"switch" | &"go" | &"work" | &"move" | &"jump")
    ) {
        let remainder: String = match words.first() {
            Some(&"go") => {
                // "go to X" / bare "go X"
                if words.get(1) == Some(&&"to") {
                    words[2..].join(" ")
                } else {
                    words[1..].join(" ")
                }
            }
            _ => words[1..].join(" "),
        };
        if !remainder.is_empty() {
            if let Some(lane) = match_lane(&remainder, reg) {
                return MetaAction::Switch(lane);
            }
        }
    }

    // bare lane name → switch
    if let Some(lane) = match_lane(&text, reg) {
        return MetaAction::Switch(lane);
    }

    MetaAction::Unknown
}

/// Apply a parsed action to the registry and produce the spoken confirmation.
/// The daemon (and any other front-end) calls this — one executor everywhere.
pub fn execute(reg: &mut LaneRegistry, action: MetaAction, notes_path: &std::path::Path) -> String {
    match action {
        MetaAction::Switch(lane) => {
            let name = lane.name.clone();
            reg.switch(&lane.id);
            format!("Switched to {name}.")
        }
        MetaAction::List => {
            let active_id = reg.active_id.clone();
            let list = reg
                .lanes
                .iter()
                .map(|l| {
                    if l.id == active_id {
                        format!("{} (active)", l.name)
                    } else {
                        l.name.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("Conversations: {list}.")
        }
        MetaAction::New(name) => match reg.create(&name) {
            Ok(lane) => {
                reg.switch(&lane.id);
                format!("Created {}. You're in it.", lane.name)
            }
            Err(LaneCreateError::CapacityReached) => crate::LANE_CAPACITY_ERROR_MESSAGE.into(),
            Err(_) => "I couldn't create that conversation name. Please use a shorter name.".into(),
        },
        MetaAction::Brief(lane_opt) => {
            let lane = lane_opt.unwrap_or_else(|| reg.active().clone());
            if lane.id == "telepathos:direct" {
                return "Direct line to Hermes. No project context.".into();
            }
            let age = crate::age_summary(&lane.last_active);
            format!(
                "Lane {}. Last active {}. Full briefing arrives with the Hermes connector.",
                lane.name, age
            )
        }
        MetaAction::Note(text) => {
            let ts = crate::now_iso();
            let line = format!(
                "{{\"note\":{},\"at\":\"{ts}\"}}\n",
                serde_json::json!(text)
            );
            if let Some(dir) = notes_path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(notes_path)
                .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
            "Noted.".into()
        }
        MetaAction::Fork(name_opt) => {
            let name = name_opt
                .unwrap_or_else(|| format!("fork-{}", reg.active().name));
            match reg.create(&name) {
                Ok(lane) => {
                    reg.switch(&lane.id);
                    format!("Forked into {}. Context carried over.", lane.name)
                }
                Err(_) => "I couldn't create that conversation name.".into(),
            }
        }
        MetaAction::Unknown => {
            "Meta commands: switch to name, list conversations, new conversation for name, brief, note that, fork."
                .into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_meta_lane_name_is_spoken_and_does_not_mutate_the_registry() {
        let mut registry = LaneRegistry::default_direct();
        let before = registry.clone();

        let reply = execute(
            &mut registry,
            MetaAction::New("x".repeat(crate::MAX_LANE_ID_LENGTH)),
            std::path::Path::new("/tmp/test-notes.jsonl"),
        );

        assert_eq!(
            reply,
            "I couldn't create that conversation name. Please use a shorter name."
        );
        assert_eq!(registry, before);
    }
}
