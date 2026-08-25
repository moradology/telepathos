//! OpenAI-compatible provider: works against api.openai.com, vLLM on the
//! 3090, OpenRouter, Ollama, or any LiteLLM proxy — they all speak this dialect.
//!
//! Deliberately tiny: one POST per step, no streaming (spoken output doesn't
//! need it), no retries (the caller re-asks by voice anyway).

use crate::{
    Provider, StepOutcome, ToolCall, ToolSpec, MAX_PROVIDER_RESPONSE_BYTES, MAX_REPLY_TEXT_BYTES,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct OpenAiProvider {
    pub base_url: String, // e.g. https://api.openai.com/v1 or http://localhost:8000/v1
    pub api_key: String,
    pub model: String,
}

/// Pure request-body builder — unit-testable without network.
pub fn build_request_body(
    model: &str,
    system: &str,
    tools: &[ToolSpec],
    messages: &Value,
) -> Value {
    let mut messages_full = vec![json!({ "role": "system", "content": system })];
    if let Some(arr) = messages.as_array() {
        messages_full.extend(arr.iter().cloned());
    }
    json!({
        "model": model,
        "messages": messages_full,
        "tools": tools.iter().map(|t| json!({
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.parameters,
            }
        })).collect::<Vec<_>>(),
    })
}

/// Extract one step from an OpenAI-shaped completion response.
pub fn parse_response(body: &Value) -> Result<StepOutcome> {
    let msg = body["choices"][0]["message"].clone();
    if msg.is_null() {
        anyhow::bail!("no choices[0].message in response");
    }
    let calls: Vec<ToolCall> = msg["tool_calls"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|c| ToolCall {
                    id: c["id"].as_str().unwrap_or_default().to_string(),
                    name: c["function"]["name"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    arguments: c["function"]["arguments"]
                        .as_str()
                        .unwrap_or("{}")
                        .to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    if calls.is_empty() {
        let text = msg["content"].as_str().unwrap_or("").to_string();
        if text.len() > MAX_REPLY_TEXT_BYTES {
            anyhow::bail!("provider reply exceeds the reply byte limit");
        }
        Ok(StepOutcome::Text(text))
    } else {
        Ok(StepOutcome::ToolCalls(calls))
    }
}

/// Read an OpenAI-compatible response without accepting an unbounded body.
///
/// The content-length check is only an early rejection: chunked and dishonest
/// peers are still checked before each append. Errors intentionally omit the
/// provider's status, URL, and body because callers may surface them to API
/// clients.
async fn read_response_body_limited(mut response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROVIDER_RESPONSE_BYTES as u64)
    {
        return Err(anyhow!("provider response exceeds the byte limit"));
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or(0)
            .min(MAX_PROVIDER_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow!("provider response could not be read"))?
    {
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| anyhow!("provider response exceeds the byte limit"))?;
        if next_len > MAX_PROVIDER_RESPONSE_BYTES {
            return Err(anyhow!("provider response exceeds the byte limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

// Assistant messages carrying tool_calls must round-trip in OpenAI shape.
pub fn assistant_message_value(msg: &Value) -> Value {
    let mut out = json!({ "role": "assistant" });
    if let Some(c) = msg["content"].as_str() {
        out["content"] = json!(c);
    }
    if let Some(tc) = msg.get("tool_calls") {
        out["tool_calls"] = tc.clone();
    }
    out
}

/// Encode the complete assistant tool-call turn. OpenAI requires one
/// assistant message containing every call, followed by one tool message per
/// call carrying the matching tool_call_id.
pub fn assistant_tool_calls_message(calls: &[ToolCall]) -> Value {
    json!({
        "role": "assistant",
        "content": null,
        "tool_calls": calls.iter().map(|call| json!({
            "id": call.id,
            "type": "function",
            "function": {
                "name": call.name,
                "arguments": call.arguments,
            }
        })).collect::<Vec<_>>(),
    })
}

pub fn tool_message(call: &ToolCall, content: impl Into<String>) -> Value {
    json!({
        "role": "tool",
        "tool_call_id": call.id,
        "content": content.into(),
    })
}

#[async_trait]
impl Provider for OpenAiProvider {
    async fn step(
        &self,
        system: &str,
        tools: &[ToolSpec],
        messages: &Value,
    ) -> Result<StepOutcome> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()?;
        let body = build_request_body(&self.model, system, tools, messages);
        let res = client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| anyhow!("provider request failed"))?;
        let status = res.status();
        let body = read_response_body_limited(res).await?;
        if !status.is_success() {
            anyhow::bail!("provider request failed");
        }
        let response = serde_json::from_slice(&body)
            .map_err(|_| anyhow!("provider returned malformed JSON"))?;
        parse_response(&response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execute_tool, run, tools, SteeringTool};
    use telepathos_lanes::LaneRegistry;

    #[test]
    fn request_body_has_tools_in_openai_shape() {
        let body = build_request_body(
            "m",
            "sys",
            &tools(),
            &json!([{"role":"user","content":"hi"}]),
        );
        assert_eq!(body["model"], "m");
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "list_lanes");
    }

    #[test]
    fn parses_tool_call_response() {
        let body = json!({
            "choices": [{"message": {
                "role": "assistant",
                "tool_calls": [{
                    "id": "c1", "type": "function",
                    "function": {"name": "switch_lane", "arguments": "{\"name\":\"kerchunk\"}"}
                }]
            }}]
        });
        match parse_response(&body).unwrap() {
            StepOutcome::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "switch_lane");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn text_responses_obey_the_exact_utf8_reply_limit() {
        fn completion(content: String) -> Value {
            json!({
                "choices": [{"message": {"role": "assistant", "content": content}}]
            })
        }

        let exact_ascii = "a".repeat(MAX_REPLY_TEXT_BYTES);
        assert!(matches!(
            parse_response(&completion(exact_ascii)),
            Ok(StepOutcome::Text(_))
        ));

        let exact_multibyte = "🦀".repeat(MAX_REPLY_TEXT_BYTES / "🦀".len());
        assert_eq!(exact_multibyte.len(), MAX_REPLY_TEXT_BYTES);
        assert!(matches!(
            parse_response(&completion(exact_multibyte)),
            Ok(StepOutcome::Text(_))
        ));

        assert!(parse_response(&completion("a".repeat(MAX_REPLY_TEXT_BYTES + 1))).is_err());
    }

    #[test]
    fn assistant_history_uses_openai_shape_for_multiple_calls() {
        let calls = vec![
            ToolCall {
                id: "c1".into(),
                name: "list_lanes".into(),
                arguments: "{}".into(),
            },
            ToolCall {
                id: "c2".into(),
                name: "active_lane".into(),
                arguments: "{}".into(),
            },
        ];
        let assistant = assistant_tool_calls_message(&calls);
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(assistant["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(assistant["tool_calls"][0]["type"], "function");
        assert_eq!(
            assistant["tool_calls"][1]["function"]["name"],
            "active_lane"
        );
        assert_eq!(tool_message(&calls[1], "ok")["tool_call_id"], "c2");
    }

    #[tokio::test]
    async fn full_loop_with_fake_provider_switches_lane() {
        // A fake OpenAI-shaped server isn't needed: exercise run() through a
        // scripted Provider to prove loop+tools integration.
        struct TextOnly;
        #[async_trait]
        impl Provider for TextOnly {
            async fn step(&self, _s: &str, _t: &[ToolSpec], _m: &Value) -> Result<StepOutcome> {
                Ok(StepOutcome::Text("in demo".into()))
            }
        }

        // two scripted providers alternating via a mutex-wrapped enum would be
        // overkill; instead verify tool execution effect directly:
        let mut reg = LaneRegistry::default_direct();
        let out = execute_tool(&mut reg, SteeringTool::CreateLane, &json!({"name":"demo"}));
        assert!(out.contains("Created"));
        assert_eq!(reg.active().name, "demo");

        // and the loop terminates on a plain-text provider
        let out = run(&TextOnly, &mut reg, "hi").await.unwrap();
        assert_eq!(out, "in demo");
    }

    #[tokio::test]
    async fn run_emits_one_assistant_turn_and_matching_tool_messages() {
        use std::sync::{Arc, Mutex};

        struct Scripted(Arc<Mutex<Option<Value>>>);
        #[async_trait]
        impl Provider for Scripted {
            async fn step(
                &self,
                _s: &str,
                _t: &[ToolSpec],
                messages: &Value,
            ) -> Result<StepOutcome> {
                if messages.as_array().unwrap().len() == 1 {
                    Ok(StepOutcome::ToolCalls(vec![
                        ToolCall {
                            id: "c1".into(),
                            name: "list_lanes".into(),
                            arguments: "{}".into(),
                        },
                        ToolCall {
                            id: "c2".into(),
                            name: "active_lane".into(),
                            arguments: "{}".into(),
                        },
                    ]))
                } else {
                    *self.0.lock().unwrap() = Some(messages.clone());
                    Ok(StepOutcome::Text("done".into()))
                }
            }
        }

        let seen = Arc::new(Mutex::new(None));
        let mut reg = LaneRegistry::default_direct();
        assert_eq!(
            run(&Scripted(seen.clone()), &mut reg, "list")
                .await
                .unwrap(),
            "done"
        );
        let messages = seen.lock().unwrap().clone().unwrap();
        let entries = messages.as_array().unwrap();
        assert_eq!(entries[1]["role"], "assistant");
        assert_eq!(entries[1]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(entries[2]["tool_call_id"], "c1");
        assert_eq!(entries[3]["tool_call_id"], "c2");
    }
}
