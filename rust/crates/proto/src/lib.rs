//! Telepathy wire frames. Must stay in lockstep with
//! `server/src/protocol.ts` and `android/.../Protocol.kt` — the three
//! compilers are the contract's test suite.

use serde::{Deserialize, Serialize};

/// Inbound control frames (client → bridge). Tag mirrors TS `tag` / Kotlin discriminators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMsg {
    Hello {
        device: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    Command {
        command: CommandKind,
    },
    /// Explicit "send now" (tap while capturing).
    UtteranceEnd,
    /// Double-pinch: next utterance goes to the meta agent.
    MetaMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Stop,
    Repeat,
    CancelCapture,
}

impl ControlMsg {
    /// Defensive parse: malformed/unknown input yields `None`, never an error.
    pub fn parse(raw: &str) -> Option<Self> {
        serde_json::from_str(raw).ok()
    }
}

/// Outgoing control frames (bridge → client). Text-only: the phone speaks.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Ready,
    SpeechStart,
    Utterance { samples: usize },
    Stt { text: String },
    AgentDelta { text: String },
    AgentEnd,
    Listening,
    Phase { value: String },
    Error { message: String },
}

/// Meta-plane frame: arm the steering plane for the next utterance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetaModeFrame;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_frames() {
        assert_eq!(
            ControlMsg::parse(r#"{"type":"hello","device":"opendots2"}"#),
            Some(ControlMsg::Hello { device: "opendots2".into(), token: None })
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"command","command":"stop"}"#),
            Some(ControlMsg::Command { command: CommandKind::Stop })
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"utterance_end"}"#),
            Some(ControlMsg::UtteranceEnd)
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"meta_mode"}"#),
            Some(ControlMsg::MetaMode)
        );
    }

    #[test]
    fn rejects_malformed_and_unknown() {
        for s in [
            "not json{{{",
            "{\"type\":",
            "{\"type\":\"command\",\"command\":\"approve\"}", // old protocol word
            "{\"type\":\"unknown_weird\"}",
            "[]",
            "null",
        ] {
            assert_eq!(ControlMsg::parse(s), None, "should reject: {s}");
        }
    }

    #[test]
    fn server_frames_round_trip() {
        let msg = ServerMsg::AgentDelta { text: "hi".into() };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"agent_delta\""));
        assert!(json.contains("\"text\":\"hi\""));
    }
}
