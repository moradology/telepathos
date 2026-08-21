//! The steering agent loop. Deliberately NON-abstract: we know every tool
//! that will ever exist, so the tool set is a closed enum, dispatch is a
//! match, and nothing is a string once it's inside.
//!
//! The single deliberate abstraction is `Provider` (pi's StreamFn analog):
//! the loop must not know which LLM serves it.

pub mod openai;

pub use openai::OpenAiProvider;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use telepathy_lanes::{match_lane, LaneRegistry};

// ---- the tool set as a closed type ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SteeringTool {
    ListLanes,
    ActiveLane,
    SwitchLane,
    CreateLane,
    LaneStats,
}

impl SteeringTool {
    /// The permanent surface. A policy test asserts this exact list.
    pub const ALL: &'static [Self] = &[
        Self::ListLanes,
        Self::ActiveLane,
        Self::SwitchLane,
        Self::CreateLane,
        Self::LaneStats,
    ];

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|t| t.name() == name)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::ListLanes => "list_lanes",
            Self::ActiveLane => "active_lane",
            Self::SwitchLane => "switch_lane",
            Self::CreateLane => "create_lane",
            Self::LaneStats => "lane_stats",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::ListLanes => {
                "List all conversation lanes with names, ids, and last-active times."
            }
            Self::ActiveLane => "Return the currently active lane.",
            Self::SwitchLane => "Make a lane the active conversation. Fuzzy-matches the name.",
            Self::CreateLane => "Create a new conversation lane and switch to it.",
            Self::LaneStats => "Interaction counts and last-active times for all lanes.",
        }
    }

    pub fn parameters(&self) -> serde_json::Value {
        match self {
            Self::ListLanes | Self::ActiveLane | Self::LaneStats => json!({
                "type": "object", "properties": {}
            }),
            Self::SwitchLane | Self::CreateLane => json!({
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"]
            }),
        }
    }
}

/// Typed arguments: args stop being `Value` at the boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameArgs {
    pub name: String,
}

/// LLM-facing schema, derived from the enum so there is one source of truth.
pub fn tools() -> Vec<ToolSpec> {
    SteeringTool::ALL
        .iter()
        .map(|t| ToolSpec {
            name: t.name(),
            description: t.description(),
            parameters: t.parameters(),
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: serde_json::Value,
}

/// Execute a RESOLVED tool. String→enum resolution happens in the loop; this
/// function has no unknown-tool path.
pub fn execute_tool(reg: &mut LaneRegistry, tool: SteeringTool, args: &Value) -> String {
    match tool {
        SteeringTool::ListLanes => {
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
        SteeringTool::ActiveLane => {
            let l = reg.active();
            format!("{} ({})", l.name, l.id)
        }
        SteeringTool::SwitchLane => match serde_json::from_value::<NameArgs>(args.clone()) {
            Ok(a) => match match_lane(&a.name, reg) {
                Some(l) => {
                    reg.switch(&l.id);
                    format!("Active lane is now {}.", l.name)
                }
                None => format!(
                    "No lane matching \"{}\". Available: {}",
                    a.name,
                    reg.lanes.iter().map(|l| l.name.as_str()).collect::<Vec<_>>().join(", ")
                ),
            },
            Err(_) => "Argument 'name' is required.".into(),
        },
        SteeringTool::CreateLane => match serde_json::from_value::<NameArgs>(args.clone()) {
            Ok(a) => {
                let lane = reg.create(&a.name);
                reg.switch(&lane.id);
                format!("Created and switched to {}.", lane.name)
            }
            Err(_) => "Argument 'name' is required.".into(),
        },
        SteeringTool::LaneStats => reg
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
    }
}

// ---- provider abstraction (the one intentional interface) ----

#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    /// Raw name from the model; resolved via SteeringTool::from_name in the loop.
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone)]
pub enum StepOutcome {
    Text(String),
    ToolCalls(Vec<ToolCall>),
}

#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    async fn step(
        &self,
        system: &str,
        tools: &[ToolSpec],
        messages: &serde_json::Value,
    ) -> Result<StepOutcome>;
}

/// No-network provider: offline boot + tests.
pub struct NullProvider;

#[async_trait::async_trait]
impl Provider for NullProvider {
    async fn step(&self, _s: &str, _t: &[ToolSpec], _m: &serde_json::Value) -> Result<StepOutcome> {
        Ok(StepOutcome::Text(
            "Steering agent online (no model configured).".into(),
        ))
    }
}

pub const META_SYSTEM: &str = "You are the steering agent for Telepathy, a voice interface to coding agents. \
Your ONLY job is managing conversation lanes: listing, switching, creating, reporting activity and statistics. \
Rules: your output is spoken aloud through earbuds — be terse, no markdown, no code, no lists over five items. \
Never discuss project content, never answer coding questions — if asked, tell the user to switch to the right \
lane and ask there. Prefer calling tools over guessing. If the target lane is ambiguous, ask one short \
clarifying question. When you switch lanes, confirm with the lane name.";

/// The loop: up to 4 tool rounds, then the final spoken text.
pub async fn run<P: Provider>(
    provider: &P,
    reg: &mut LaneRegistry,
    utterance: &str,
) -> Result<String> {
    let tools = tools();
    let mut messages = json!([
        { "role": "system", "content": META_SYSTEM },
        { "role": "user", "content": utterance },
    ]);

    for _round in 0..4 {
        match provider.step(META_SYSTEM, &tools, &messages).await? {
            StepOutcome::Text(text) => return Ok(text),
            StepOutcome::ToolCalls(calls) => {
                for call in calls {
                    // resolve ONCE here; execute_tool never sees strings
                    let Some(tool) = SteeringTool::from_name(&call.name) else {
                        messages.as_array_mut().unwrap().push(json!({
                            "role": "assistant",
                            "tool_calls": [{
                                "id": call.id, "name": call.name, "arguments": call.arguments
                            }]
                        }));
                        messages.as_array_mut().unwrap().push(json!({
                            "role": "tool", "content": format!("unknown tool {}", call.name)
                        }));
                        continue;
                    };
                    let args: Value =
                        serde_json::from_str(&call.arguments).unwrap_or(json!({}));
                    let result = execute_tool(reg, tool, &args);
                    messages.as_array_mut().unwrap().push(json!({
                        "role": "assistant",
                        "tool_calls": [{
                            "id": call.id, "name": call.name, "arguments": call.arguments
                        }]
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
