//! The steering agent loop, pi-style: the loop is provider-agnostic.
//! `Provider` is injected (StreamFn analog); tools are typed data.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use telepathy_lanes::{match_lane, LaneRegistry};

pub const META_SYSTEM: &str = "You are the steering agent for Telepathy, a voice interface to coding agents. \
Your ONLY job is managing conversation lanes: listing, switching, creating, reporting activity and statistics. \
Rules: your output is spoken aloud through earbuds — be terse, no markdown, no code, no lists over five items. \
Never discuss project content, never answer coding questions — if asked, tell the user to switch to the right \
lane and ask there. Prefer calling tools over guessing. If the target lane is ambiguous, ask one short \
clarifying question. When you switch lanes, confirm with the lane name.";

/// One callable tool, described as data (pi's `AgentTool` analog).
#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    /// JSON schema for the arguments object.
    pub parameters: serde_json::Value,
}

pub fn tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "list_lanes",
            description: "List all conversation lanes with names, ids, and last-active times.",
            parameters: json!({"type":"object","properties":{}}),
        },
        ToolSpec {
            name: "active_lane",
            description: "Return the currently active lane.",
            parameters: json!({"type":"object","properties":{}}),
        },
        ToolSpec {
            name: "switch_lane",
            description: "Make a lane the active conversation. Fuzzy-matches the name.",
            parameters: json!({"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}),
        },
        ToolSpec {
            name: "create_lane",
            description: "Create a new conversation lane and switch to it.",
            parameters: json!({"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}),
        },
        ToolSpec {
            name: "lane_stats",
            description: "Interaction counts and last-active times for all lanes.",
            parameters: json!({"type":"object","properties":{}}),
        },
    ]
}

/// Execute one tool call against the registry. Returns text for the LLM.
pub fn execute_tool(reg: &mut LaneRegistry, name: &str, args: &serde_json::Value) -> String {
    match name {
        "list_lanes" => {
            let active = reg.active();
            reg.lanes
                .iter()
                .map(|l| {
                    format!(
                        "{} — last active {}{}",
                        l.name,
                        l.last_active,
                        if l.id == active.id { " (ACTIVE)" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
        "active_lane" => {
            let l = reg.active();
            format!("{} ({})", l.name, l.id)
        }
        "switch_lane" => {
            let name = args["name"].as_str().unwrap_or("");
            match match_lane(name, reg) {
                Some(l) => {
                    reg.switch(&l.id);
                    format!("Active lane is now {}.", l.name)
                }
                None => format!(
                    "No lane matching \"{name}\". Available: {}",
                    reg.lanes.iter().map(|l| l.name.as_str()).collect::<Vec<_>>().join(", ")
                ),
            }
        }
        "create_lane" => {
            let name = args["name"].as_str().unwrap_or("");
            let lane = reg.create(name);
            reg.switch(&lane.id);
            format!("Created and switched to {}.", lane.name)
        }
        "lane_stats" => reg
            .lanes
            .iter()
            .map(|l| {
                format!(
                    "{}: {} interactions, last active {}",
                    l.name,
                    l.interactions.unwrap_or(0),
                    l.last_active
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => format!("unknown tool {other}"),
    }
}

// ---- provider abstraction (pi's StreamFn analog) ----

#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone)]
pub enum StepOutcome {
    Text(String),
    ToolCalls(Vec<ToolCall>),
}

/// A completion turn: messages in (role/content/tool plumbing), one step out.
/// Concrete providers (OpenAI-compatible, vLLM on the 3090, stubs) implement this.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    async fn step(
        &self,
        system: &str,
        tools: &[ToolSpec],
        messages: &serde_json::Value,
    ) -> Result<StepOutcome>;
}

/// No-network provider for tests and offline boot: always returns a fixed line.
pub struct NullProvider;

#[async_trait::async_trait]
impl Provider for NullProvider {
    async fn step(&self, _system: &str, _tools: &[ToolSpec], _messages: &serde_json::Value) -> Result<StepOutcome> {
        Ok(StepOutcome::Text("Steering agent online (no model configured).".into()))
    }
}

/// The loop: up to 4 tool rounds, then the final spoken text.
pub async fn run<P: Provider>(
    provider: &P,
    reg: &mut LaneRegistry,
    utterance: &str,
) -> Result<String> {
    let tools = tools();
    let mut messages = serde_json::json!([
        { "role": "system", "content": META_SYSTEM },
        { "role": "user", "content": utterance },
    ]);

    for _round in 0..4 {
        match provider.step(META_SYSTEM, &tools, &messages).await? {
            StepOutcome::Text(text) => return Ok(text),
            StepOutcome::ToolCalls(calls) => {
                for call in calls {
                    let args: serde_json::Value =
                        serde_json::from_str(&call.arguments).unwrap_or(json!({}));
                    let result = execute_tool(reg, &call.name, &args);
                    messages.as_array_mut().unwrap().push(json!({
                        "role": "assistant",
                        "tool_calls": [{ "id": call.id, "name": call.name, "arguments": call.arguments }]
                    }));
                    messages.as_array_mut().unwrap().push(json!({
                        "role": "tool", "content": result
                    }));
                }
            }
        }
    }
    Ok("I went in circles — try a simpler command.".into())
}
