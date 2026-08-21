//! The meta grammar: pure function from transcript + registry to action.
//! Requires registry evidence before intercepting lane names — collision
//! safety with coding speech ("switch to main") is structural.

use crate::{Lane, LaneRegistry};

pub enum MetaAction {
    Switch(Lane),
    List,
    New(String),
    Brief(Option<Lane>),
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
            dp[j] = (dp[j] + 1).min(dp[j - 1] + 1).min(prev + if a[i - 1] == b[j - 1] { 0 } else { 1 });
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
        if let Some(kpos) = words.iter().position(|w| matches!(*w, "conversation" | "lane" | "chat")) {
            // optional preposition after the keyword, then the name
            let rest_start = kpos + 1;
            let name = if rest_start < words.len()
                && matches!(words[rest_start], "for" | "about" | "called" | "named" | "to")
            {
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
    if words.first() == Some(&"brief") || text == "catch me up" || words.first() == Some(&"status") {
        let target = if let Some(p) = words.iter().position(|w| *w == "on" || *w == "about" || *w == "for") {
            Some(words[p + 1..].join(" "))
        } else {
            None
        };
        return MetaAction::Brief(match_lane(target.as_deref().unwrap_or(""), reg));
    }

    // switch/go/work on X — lane match REQUIRED, else fall through
    if matches!(words.first(), Some(&"switch" | &"go" | &"work" | &"move" | &"jump")) {
        let remainder: String = match words.first() {
            Some(&"go") => {
                // "go to X" / bare "go X"
                if words.get(1) == Some(&&"to") { words[2..].join(" ") } else { words[1..].join(" ") }
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
