//! Telepathos wire frames. Must stay in lockstep with
//! `server/src/protocol.ts` and `android/.../Protocol.kt` — the three
//! compilers are the contract's test suite.

use serde::{
    ser::{Error as SerError, Serializer},
    Deserialize, Serialize,
};

/// JSON numbers are exchanged with JavaScript and Kotlin clients. Keep every
/// receipt sequence and lane revision inside JavaScript's exact-integer range.
pub const MAX_SAFE_SEQUENCE: u64 = 9_007_199_254_740_991;
/// Matches JavaScript/Kotlin `String.length`: UTF-16 code units, not bytes.
pub const MAX_TURN_TOKEN_LENGTH: usize = 128;
/// Lane IDs are stable wire/persistence keys. Keep this grammar identical in
/// Rust, Node, and Android: bounded in both UTF-8 bytes and UTF-16 code units,
/// ASCII only, and safe as an unquoted identifier component.
pub const MAX_LANE_ID_LENGTH: usize = 128;

/// Shared bound for opaque wire and durable correlation identifiers. The
/// value is measured in both UTF-8 bytes and UTF-16 code units so Rust, Node,
/// and Android reject the same identifiers without normalizing them.
pub const MAX_OPAQUE_ID_LENGTH: usize = 256;
pub const MAX_OPAQUE_ID_BYTES: usize = 256;

pub fn is_valid_lane_id(value: &str) -> bool {
    if !(1..=MAX_LANE_ID_LENGTH).contains(&value.len())
        || !(1..=MAX_LANE_ID_LENGTH).contains(&value.encode_utf16().count())
    {
        return false;
    }
    let bytes = value.as_bytes();
    bytes
        .first()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-'))
}

/// Canonical protocol blankness, independent of a runtime's `trim` behavior:
/// the blank code points are ASCII U+0009..U+000D and U+0020, Unicode
/// White_Space additions U+0085, U+00A0, U+1680, U+2000..U+200A,
/// U+2028..U+2029, U+202F, U+205F, U+3000, plus U+FEFF. A protocol value is
/// blank only when every code point is in that explicit set. This does not
/// trim or normalize the value; callers retain their existing control policy.
fn is_protocol_blank(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            matches!(
                character,
                '\u{0009}'..='\u{000d}'
                    | '\u{0020}'
                    | '\u{0085}'
                    | '\u{00a0}'
                    | '\u{1680}'
                    | '\u{2000}'..='\u{200a}'
                    | '\u{2028}'..='\u{2029}'
                    | '\u{202f}'
                    | '\u{205f}'
                    | '\u{3000}'
                    | '\u{feff}'
            )
        })
}

/// Opaque correlation IDs are nonblank Unicode strings with no C0/C1 control
/// characters. Whitespace and case are significant; no trimming or other
/// normalization occurs.
pub fn is_valid_opaque_id(value: &str) -> bool {
    (1..=MAX_OPAQUE_ID_LENGTH).contains(&value.encode_utf16().count())
        && (1..=MAX_OPAQUE_ID_BYTES).contains(&value.len())
        && !is_protocol_blank(value)
        && value.chars().all(
            |character| !matches!(character, '\u{0000}'..='\u{001f}' | '\u{007f}'..='\u{009f}'),
        )
}

/// Inbound control frames (client → bridge). Tag mirrors TS `tag` / Kotlin discriminators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMsg {
    Hello {
        device: String,
        installation_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    Command {
        command: CommandKind,
        turn_token: String,
    },
    /// Explicit "send now" (tap while capturing).
    UtteranceEnd { turn_token: String },
    /// Double-pinch: next utterance goes to the meta agent.
    MetaMode { turn_token: String },
    /// Phone snapshots the active lane before opening the mic.
    Lane {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision: Option<u64>,
        turn_token: String,
    },
    /// Handset durably recorded the bridge's replayable reply envelope.
    ReplyReceived {
        lane_id: String,
        reply_to: String,
        after_seq: u64,
        through_seq: u64,
        turn_token: String,
        interaction_id: String,
    },
    /// Phone accepted a synchronous reply for playback.
    ReplyAck {
        lane_id: String,
        reply_to: String,
        after_seq: u64,
        through_seq: u64,
        turn_token: String,
        interaction_id: String,
    },
    /// Handset durably recorded the bridge's reply acknowledgement.
    ReplyAckRetire {
        lane_id: String,
        reply_to: String,
        after_seq: u64,
        through_seq: u64,
        turn_token: String,
        interaction_id: String,
    },
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
        let msg = serde_json::from_str(raw).ok()?;
        match &msg {
            Self::ReplyAck {
                lane_id,
                reply_to,
                after_seq,
                through_seq,
                turn_token,
                interaction_id,
            }
            | Self::ReplyAckRetire {
                lane_id,
                reply_to,
                after_seq,
                through_seq,
                turn_token,
                interaction_id,
            } if valid_receipt_fields(
                lane_id,
                reply_to,
                *after_seq,
                *through_seq,
                turn_token,
                interaction_id,
            ) => {}
            Self::ReplyReceived {
                lane_id,
                reply_to,
                after_seq,
                through_seq,
                turn_token,
                interaction_id,
            } if valid_receipt_fields(
                lane_id,
                reply_to,
                *after_seq,
                *through_seq,
                turn_token,
                interaction_id,
            ) => {}
            Self::ReplyReceived { .. } | Self::ReplyAck { .. } | Self::ReplyAckRetire { .. } => {
                return None
            }
            Self::Hello {
                installation_id, ..
            } if !valid_installation_id(installation_id) => return None,
            Self::Command { turn_token, .. }
            | Self::UtteranceEnd { turn_token }
            | Self::MetaMode { turn_token }
            | Self::Lane { turn_token, .. }
                if !valid_turn_token(turn_token) =>
            {
                return None
            }
            Self::Lane { id, .. } if !is_valid_lane_id(id) => return None,
            Self::Lane {
                revision: Some(revision),
                ..
            } if *revision > MAX_SAFE_SEQUENCE => return None,
            _ => {}
        }
        Some(msg)
    }
}

const MAX_INSTALLATION_ID_LENGTH: usize = 128;

fn valid_turn_token(value: &str) -> bool {
    (1..=MAX_TURN_TOKEN_LENGTH).contains(&value.encode_utf16().count()) && !is_protocol_blank(value)
}

/// Matches the Node/Android v5 contract without normalizing the opaque value.
/// UTF-16 length is intentional: JavaScript and Kotlin both count string
/// length in UTF-16 code units.
fn valid_installation_id(value: &str) -> bool {
    (1..=MAX_INSTALLATION_ID_LENGTH).contains(&value.encode_utf16().count())
        && !is_protocol_blank(value)
        && value.chars().all(
            |character| !matches!(character, '\u{0000}'..='\u{001f}' | '\u{007f}'..='\u{009f}'),
        )
}

fn valid_receipt_fields(
    lane_id: &str,
    reply_to: &str,
    after_seq: u64,
    through_seq: u64,
    turn_token: &str,
    interaction_id: &str,
) -> bool {
    valid_delivery_receipt_fields(lane_id, reply_to, after_seq, through_seq)
        && valid_turn_token(turn_token)
        && is_valid_opaque_id(interaction_id)
}

fn valid_delivery_receipt_fields(
    lane_id: &str,
    reply_to: &str,
    after_seq: u64,
    through_seq: u64,
) -> bool {
    is_valid_lane_id(lane_id)
        && is_valid_opaque_id(reply_to)
        && after_seq <= MAX_SAFE_SEQUENCE
        && through_seq <= MAX_SAFE_SEQUENCE
        && through_seq > after_seq
}

fn valid_optional_delivery_receipt(
    lane_id: Option<&str>,
    reply_to: Option<&str>,
    after_seq: Option<u64>,
    through_seq: Option<u64>,
) -> bool {
    match (lane_id, reply_to, after_seq, through_seq) {
        (None, None, None, None) => true,
        (Some(lane_id), Some(reply_to), Some(after_seq), Some(through_seq)) => {
            valid_delivery_receipt_fields(lane_id, reply_to, after_seq, through_seq)
        }
        _ => false,
    }
}

/// Outgoing control frames (bridge → client). Text-only: the phone speaks.
#[derive(Debug, Clone, PartialEq)]
pub enum ServerMsg {
    Ready,
    SpeechStart,
    Utterance {
        samples: usize,
    },
    /// Transcript text and turn identity, with backend-specific metadata when
    /// available. Confidence is a probability in the inclusive 0..=1 range;
    /// this outbound enum is serialize-only, so Android enforces that range
    /// at its defensive parse boundary.
    Stt {
        text: String,
        confidence: Option<f64>,
        repo: Option<String>,
        turn_token: String,
        interaction_id: String,
    },
    AgentDelta {
        text: String,
        turn_token: String,
        interaction_id: String,
    },
    AgentEnd {
        text: String,
        turn_token: String,
        interaction_id: String,
        lane_id: Option<String>,
        reply_to: Option<String>,
        after_seq: Option<u64>,
        through_seq: Option<u64>,
    },
    ReplyReceived {
        lane_id: String,
        reply_to: String,
        after_seq: u64,
        through_seq: u64,
        turn_token: String,
        interaction_id: String,
    },
    ReplyAcknowledged {
        lane_id: String,
        reply_to: String,
        after_seq: u64,
        through_seq: u64,
        turn_token: String,
        interaction_id: String,
    },
    ReplyAckRetired {
        lane_id: String,
        reply_to: String,
        after_seq: u64,
        through_seq: u64,
        turn_token: String,
        interaction_id: String,
    },
    Listening,
    Phase {
        value: String,
    },
    Error {
        message: String,
    },
}

/// The unchecked representation is kept private so every public
/// `ServerMsg` serialization goes through `valid_server_msg` first. Its field
/// order and optional fields intentionally mirror the v5 wire shape exactly.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsgWire<'a> {
    Ready,
    SpeechStart,
    Utterance {
        samples: usize,
    },
    Stt {
        text: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        confidence: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        repo: Option<&'a str>,
        turn_token: &'a str,
        interaction_id: &'a str,
    },
    AgentDelta {
        text: &'a str,
        turn_token: &'a str,
        interaction_id: &'a str,
    },
    AgentEnd {
        text: &'a str,
        turn_token: &'a str,
        interaction_id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        lane_id: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_to: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after_seq: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        through_seq: Option<u64>,
    },
    ReplyReceived {
        lane_id: &'a str,
        reply_to: &'a str,
        after_seq: u64,
        through_seq: u64,
        turn_token: &'a str,
        interaction_id: &'a str,
    },
    ReplyAcknowledged {
        lane_id: &'a str,
        reply_to: &'a str,
        after_seq: u64,
        through_seq: u64,
        turn_token: &'a str,
        interaction_id: &'a str,
    },
    ReplyAckRetired {
        lane_id: &'a str,
        reply_to: &'a str,
        after_seq: u64,
        through_seq: u64,
        turn_token: &'a str,
        interaction_id: &'a str,
    },
    Listening,
    Phase {
        value: &'a str,
    },
    Error {
        message: &'a str,
    },
}

fn server_msg_wire(message: &ServerMsg) -> ServerMsgWire<'_> {
    match message {
        ServerMsg::Ready => ServerMsgWire::Ready,
        ServerMsg::SpeechStart => ServerMsgWire::SpeechStart,
        ServerMsg::Utterance { samples } => ServerMsgWire::Utterance { samples: *samples },
        ServerMsg::Stt {
            text,
            confidence,
            repo,
            turn_token,
            interaction_id,
        } => ServerMsgWire::Stt {
            text,
            confidence: *confidence,
            repo: repo.as_deref(),
            turn_token,
            interaction_id,
        },
        ServerMsg::AgentDelta {
            text,
            turn_token,
            interaction_id,
        } => ServerMsgWire::AgentDelta {
            text,
            turn_token,
            interaction_id,
        },
        ServerMsg::AgentEnd {
            text,
            turn_token,
            interaction_id,
            lane_id,
            reply_to,
            after_seq,
            through_seq,
        } => ServerMsgWire::AgentEnd {
            text,
            turn_token,
            interaction_id,
            lane_id: lane_id.as_deref(),
            reply_to: reply_to.as_deref(),
            after_seq: *after_seq,
            through_seq: *through_seq,
        },
        ServerMsg::ReplyReceived {
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            turn_token,
            interaction_id,
        } => ServerMsgWire::ReplyReceived {
            lane_id,
            reply_to,
            after_seq: *after_seq,
            through_seq: *through_seq,
            turn_token,
            interaction_id,
        },
        ServerMsg::ReplyAcknowledged {
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            turn_token,
            interaction_id,
        } => ServerMsgWire::ReplyAcknowledged {
            lane_id,
            reply_to,
            after_seq: *after_seq,
            through_seq: *through_seq,
            turn_token,
            interaction_id,
        },
        ServerMsg::ReplyAckRetired {
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            turn_token,
            interaction_id,
        } => ServerMsgWire::ReplyAckRetired {
            lane_id,
            reply_to,
            after_seq: *after_seq,
            through_seq: *through_seq,
            turn_token,
            interaction_id,
        },
        ServerMsg::Listening => ServerMsgWire::Listening,
        ServerMsg::Phase { value } => ServerMsgWire::Phase { value },
        ServerMsg::Error { message } => ServerMsgWire::Error { message },
    }
}

fn valid_server_msg(message: &ServerMsg) -> bool {
    match message {
        ServerMsg::Stt {
            turn_token,
            interaction_id,
            ..
        }
        | ServerMsg::AgentDelta {
            turn_token,
            interaction_id,
            ..
        } => valid_turn_token(turn_token) && is_valid_opaque_id(interaction_id),
        ServerMsg::AgentEnd {
            turn_token,
            interaction_id,
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            ..
        } => {
            valid_turn_token(turn_token)
                && is_valid_opaque_id(interaction_id)
                && valid_optional_delivery_receipt(
                    lane_id.as_deref(),
                    reply_to.as_deref(),
                    *after_seq,
                    *through_seq,
                )
        }
        ServerMsg::ReplyReceived {
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            turn_token,
            interaction_id,
        }
        | ServerMsg::ReplyAcknowledged {
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            turn_token,
            interaction_id,
        }
        | ServerMsg::ReplyAckRetired {
            lane_id,
            reply_to,
            after_seq,
            through_seq,
            turn_token,
            interaction_id,
        } => valid_receipt_fields(
            lane_id,
            reply_to,
            *after_seq,
            *through_seq,
            turn_token,
            interaction_id,
        ),
        ServerMsg::Ready
        | ServerMsg::SpeechStart
        | ServerMsg::Utterance { .. }
        | ServerMsg::Listening
        | ServerMsg::Phase { .. }
        | ServerMsg::Error { .. } => true,
    }
}

impl Serialize for ServerMsg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if !valid_server_msg(self) {
            return Err(<S::Error as SerError>::custom(
                "invalid server message correlation fields",
            ));
        }
        server_msg_wire(self).serialize(serializer)
    }
}

/// Meta-plane frame: arm the steering plane for the next utterance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetaModeFrame;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_id_contract_rejects_json_metacharacters_controls_unicode_and_oversize() {
        for value in [
            "",
            " ",
            "telepathos:repo:quote\"",
            "telepathos:repo:backslash\\",
            "telepathos:repo:control\n",
            "telepathos:repo:é",
        ] {
            assert!(
                !is_valid_lane_id(value),
                "unexpected valid lane id: {value:?}"
            );
        }
        assert!(!is_valid_lane_id(&format!(
            "telepathos:repo:{}",
            "a".repeat(MAX_LANE_ID_LENGTH)
        )));
        assert!(is_valid_lane_id("telepathos:direct"));
        assert!(is_valid_lane_id("telepathos:repo:geospatial-migration"));
    }

    #[test]
    fn opaque_id_contract_matches_utf8_utf16_and_control_boundaries() {
        assert!(is_valid_opaque_id("id-1"));
        assert!(is_valid_opaque_id(&"é".repeat(MAX_OPAQUE_ID_BYTES / 2)));
        assert!(is_valid_opaque_id(&"🦀".repeat(MAX_OPAQUE_ID_BYTES / 4)));
        for value in ["", " \t\n", "id\0bad", "id\u{0085}bad"] {
            assert!(
                !is_valid_opaque_id(value),
                "unexpected valid opaque ID: {value:?}"
            );
        }
        assert!(!is_valid_opaque_id(
            &"é".repeat(MAX_OPAQUE_ID_BYTES / 2 + 1)
        ));
        assert!(!is_valid_opaque_id(
            &"🦀".repeat(MAX_OPAQUE_ID_BYTES / 4 + 1)
        ));
    }

    #[test]
    fn outbound_serialization_rejects_oversized_correlation_ids() {
        let message = ServerMsg::AgentEnd {
            text: "reply".into(),
            turn_token: "turn-1".into(),
            interaction_id: "i".repeat(MAX_OPAQUE_ID_LENGTH + 1),
            lane_id: None,
            reply_to: None,
            after_seq: None,
            through_seq: None,
        };
        assert!(serialize_server_msg(&message).is_none());
    }

    #[test]
    fn agent_end_rejects_every_partial_delivery_receipt_combination() {
        for mask in 1..0b1111_u8 {
            let message = ServerMsg::AgentEnd {
                text: "reply".into(),
                turn_token: "turn-1".into(),
                interaction_id: "interaction-1".into(),
                lane_id: (mask & 0b0001 != 0).then(|| "telepathos:direct".into()),
                reply_to: (mask & 0b0010 != 0).then(|| "reply-1".into()),
                after_seq: (mask & 0b0100 != 0).then_some(4),
                through_seq: (mask & 0b1000 != 0).then_some(6),
            };

            assert!(
                serialize_server_msg(&message).is_none(),
                "partial receipt mask {mask:#06b} was serialized"
            );
            assert!(
                serde_json::to_string(&message).is_err(),
                "partial receipt mask {mask:#06b} bypassed the Serialize boundary"
            );
        }
    }

    #[test]
    fn agent_end_requires_valid_delivery_receipt_ids_and_interval() {
        let make_message =
            |lane_id: &str, reply_to: &str, after_seq: u64, through_seq: u64| ServerMsg::AgentEnd {
                text: "reply".into(),
                turn_token: "turn-1".into(),
                interaction_id: "interaction-1".into(),
                lane_id: Some(lane_id.into()),
                reply_to: Some(reply_to.into()),
                after_seq: Some(after_seq),
                through_seq: Some(through_seq),
            };

        let valid = make_message("telepathos:direct", "reply-1", 4, 6);
        assert_eq!(
            serde_json::to_string(&valid).unwrap(),
            r#"{"type":"agent_end","text":"reply","turn_token":"turn-1","interaction_id":"interaction-1","lane_id":"telepathos:direct","reply_to":"reply-1","after_seq":4,"through_seq":6}"#
        );
        assert!(serialize_server_msg(&valid).is_some());

        for (name, message) in [
            (
                "invalid lane",
                make_message("telepathos: direct", "reply-1", 4, 6),
            ),
            (
                "blank reply ID",
                make_message("telepathos:direct", " ", 4, 6),
            ),
            (
                "unsafe after sequence",
                make_message(
                    "telepathos:direct",
                    "reply-1",
                    MAX_SAFE_SEQUENCE + 1,
                    MAX_SAFE_SEQUENCE + 2,
                ),
            ),
            (
                "unsafe through sequence",
                make_message(
                    "telepathos:direct",
                    "reply-1",
                    MAX_SAFE_SEQUENCE - 1,
                    MAX_SAFE_SEQUENCE + 1,
                ),
            ),
            (
                "reversed interval",
                make_message("telepathos:direct", "reply-1", 6, 4),
            ),
            (
                "equal interval",
                make_message("telepathos:direct", "reply-1", 4, 4),
            ),
        ] {
            assert!(
                serialize_server_msg(&message).is_none(),
                "{name} was serialized"
            );
            assert!(
                serde_json::to_string(&message).is_err(),
                "{name} bypassed the Serialize boundary"
            );
        }
    }

    #[test]
    fn parses_valid_frames() {
        assert_eq!(
            ControlMsg::parse(
                r#"{"type":"hello","device":"opendots2","installation_id":"  install-1  "}"#
            ),
            Some(ControlMsg::Hello {
                device: "opendots2".into(),
                installation_id: "  install-1  ".into(),
                token: None
            })
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"command","command":"stop","turn_token":"turn-1"}"#),
            Some(ControlMsg::Command {
                command: CommandKind::Stop,
                turn_token: "turn-1".into(),
            })
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"utterance_end","turn_token":"turn-1"}"#),
            Some(ControlMsg::UtteranceEnd {
                turn_token: "turn-1".into(),
            })
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"meta_mode","turn_token":"turn-1"}"#),
            Some(ControlMsg::MetaMode {
                turn_token: "turn-1".into(),
            })
        );
        assert_eq!(
            ControlMsg::parse(r#"{"type":"lane","id":"telepathos:direct","turn_token":"turn-1"}"#),
            Some(ControlMsg::Lane {
                id: "telepathos:direct".into(),
                revision: None,
                turn_token: "turn-1".into(),
            })
        );
        assert_eq!(
            ControlMsg::parse(
                r#"{"type":"reply_received","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":4,"through_seq":6,"turn_token":"turn-1","interaction_id":"i-1"}"#
            ),
            Some(ControlMsg::ReplyReceived {
                lane_id: "telepathos:direct".into(),
                reply_to: "tp-1".into(),
                after_seq: 4,
                through_seq: 6,
                turn_token: "turn-1".into(),
                interaction_id: "i-1".into(),
            })
        );
        assert_eq!(
            ControlMsg::parse(
                r#"{"type":"reply_ack","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":4,"through_seq":6,"turn_token":"turn-1","interaction_id":"i-1"}"#
            ),
            Some(ControlMsg::ReplyAck {
                lane_id: "telepathos:direct".into(),
                reply_to: "tp-1".into(),
                after_seq: 4,
                through_seq: 6,
                turn_token: "turn-1".into(),
                interaction_id: "i-1".into(),
            })
        );
        assert_eq!(
            ControlMsg::parse(
                r#"{"type":"reply_ack_retire","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":4,"through_seq":6,"turn_token":"turn-1","interaction_id":"i-1"}"#
            ),
            Some(ControlMsg::ReplyAckRetire {
                lane_id: "telepathos:direct".into(),
                reply_to: "tp-1".into(),
                after_seq: 4,
                through_seq: 6,
                turn_token: "turn-1".into(),
                interaction_id: "i-1".into(),
            })
        );
    }

    #[test]
    fn rejects_malformed_and_unknown() {
        for s in [
            "not json{{{",
            "{\"type\":",
            "{\"type\":\"command\",\"command\":\"approve\"}", // old protocol word
            r#"{"type":"hello","device":"opendots2"}"#,
            r#"{"type":"command","command":"stop"}"#,
            r#"{"type":"utterance_end"}"#,
            r#"{"type":"meta_mode","turn_token":""}"#,
            r#"{"type":"lane","id":"telepathos:direct"}"#,
            "{\"type\":\"unknown_weird\"}",
            "[]",
            "null",
            r#"{"type":"reply_received","lane_id":"","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_received","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":1,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_received","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack","lane_id":"","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack","lane_id":"telepathos:direct","reply_to":"","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":6,"through_seq":6,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack_retire","lane_id":"","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack_retire","lane_id":"telepathos:direct","reply_to":"","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack_retire","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":6,"through_seq":6,"turn_token":"turn-1","interaction_id":"i-1"}"#,
            r#"{"type":"reply_ack_retire","lane_id":"telepathos:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"","interaction_id":"i-1"}"#,
        ] {
            assert_eq!(ControlMsg::parse(s), None, "should reject: {s}");
        }
    }

    #[test]
    fn receipt_and_control_bounds_match_js_and_kotlin() {
        let max_token = "t".repeat(MAX_TURN_TOKEN_LENGTH);
        let oversized_token = "t".repeat(MAX_TURN_TOKEN_LENGTH + 1);

        for raw in [
            serde_json::json!({
                "type": "command",
                "command": "stop",
                "turn_token": max_token,
            }),
            serde_json::json!({
                "type": "utterance_end",
                "turn_token": max_token,
            }),
            serde_json::json!({
                "type": "meta_mode",
                "turn_token": max_token,
            }),
            serde_json::json!({
                "type": "lane",
                "id": "telepathos:direct",
                "turn_token": max_token,
            }),
        ] {
            assert!(
                ControlMsg::parse(&raw.to_string()).is_some(),
                "should accept: {raw}"
            );
            let mut oversized = raw;
            oversized["turn_token"] = oversized_token.clone().into();
            assert_eq!(ControlMsg::parse(&oversized.to_string()), None);
        }

        for frame_type in ["reply_received", "reply_ack", "reply_ack_retire"] {
            let at_limit = serde_json::json!({
                "type": frame_type,
                "lane_id": "telepathos:direct",
                "reply_to": "tp-1",
                "after_seq": MAX_SAFE_SEQUENCE - 1,
                "through_seq": MAX_SAFE_SEQUENCE,
                "turn_token": max_token,
                "interaction_id": "i-1",
            });
            assert!(
                ControlMsg::parse(&at_limit.to_string()).is_some(),
                "should accept at-limit {frame_type}"
            );

            let mut oversized_token_frame = at_limit.clone();
            oversized_token_frame["turn_token"] = oversized_token.clone().into();
            assert_eq!(ControlMsg::parse(&oversized_token_frame.to_string()), None);

            let mut oversized_sequence_frame = at_limit;
            oversized_sequence_frame["after_seq"] = MAX_SAFE_SEQUENCE.into();
            oversized_sequence_frame["through_seq"] = (MAX_SAFE_SEQUENCE + 1).into();
            assert_eq!(
                ControlMsg::parse(&oversized_sequence_frame.to_string()),
                None
            );
        }

        assert!(valid_turn_token(&"🦀".repeat(MAX_TURN_TOKEN_LENGTH / 2)));
        assert!(!valid_turn_token(
            &"🦀".repeat(MAX_TURN_TOKEN_LENGTH / 2 + 1)
        ));
    }

    #[test]
    fn opaque_id_blankness_matches_the_explicit_protocol_code_point_set() {
        let cases = [
            ("", false),
            (" ", false),
            ("\t", false),
            ("\n", false),
            ("\u{000b}", false),
            ("\u{000c}", false),
            ("\r", false),
            ("\u{0000}", false),
            ("\u{001f}", false),
            ("\u{007f}", false),
            ("\u{0085}", false),
            ("\u{009f}", false),
            ("\u{00a0}", false),
            ("\u{2007}", false),
            ("\u{202f}", false),
            ("\u{feff}", false),
            ("id", true),
            (" id ", true),
            ("\u{00a0}id\u{00a0}", true),
            ("\u{2007}id\u{2007}", true),
            ("\u{202f}id\u{202f}", true),
            ("\u{feff}id\u{feff}", true),
            ("id\t", false),
            ("id\u{0085}", false),
        ];

        for (value, expected) in cases {
            assert_eq!(
                is_valid_opaque_id(value),
                expected,
                "unexpected opaque-ID validity for {value:?}"
            );
        }
    }

    #[test]
    fn turn_token_blankness_matches_the_explicit_protocol_code_point_set_without_adding_controls() {
        let cases = [
            ("", false),
            (" ", false),
            ("\t", false),
            ("\n", false),
            ("\u{000b}", false),
            ("\u{000c}", false),
            ("\r", false),
            ("\u{0085}", false),
            ("\u{00a0}", false),
            ("\u{1680}", false),
            ("\u{2007}", false),
            ("\u{202f}", false),
            ("\u{3000}", false),
            ("\u{feff}", false),
            // Turn tokens historically have no control-character rejection.
            ("\u{0000}", true),
            ("\u{001f}", true),
            ("\u{007f}", true),
            ("\u{009f}", true),
            ("turn-1", true),
            (" turn-1 ", true),
            ("\u{00a0}turn-1\u{00a0}", true),
            ("\u{2007}turn-1\u{2007}", true),
            ("\u{202f}turn-1\u{202f}", true),
            ("\u{feff}turn-1\u{feff}", true),
            ("turn-1\t", true),
            ("turn-1\u{0085}", true),
        ];

        for (value, expected) in cases {
            assert_eq!(
                valid_turn_token(value),
                expected,
                "unexpected turn-token validity for {value:?}"
            );
            let raw = serde_json::json!({
                "type": "command",
                "command": "stop",
                "turn_token": value,
            })
            .to_string();
            match (ControlMsg::parse(&raw), expected) {
                (Some(ControlMsg::Command { turn_token, .. }), true) => {
                    assert_eq!(turn_token, value)
                }
                (None, false) => {}
                (parsed, expected) => {
                    panic!("unexpected parsed turn token {parsed:?}, expected {expected}")
                }
            }
        }
    }

    #[test]
    fn rejects_lone_utf16_surrogates_at_the_json_boundary() {
        for surrogate in [r#"\ud800"#, r#"\udc00"#] {
            let command = format!(
                r#"{{"type":"command","command":"stop","turn_token":"{}"}}"#,
                surrogate
            );
            assert_eq!(
                ControlMsg::parse(&command),
                None,
                "accepted turn token {surrogate:?}"
            );

            let hello = format!(
                r#"{{"type":"hello","device":"opendots2","installation_id":"{}"}}"#,
                surrogate
            );
            assert_eq!(
                ControlMsg::parse(&hello),
                None,
                "accepted installation ID {surrogate:?}"
            );
        }
    }

    #[test]
    fn lane_revision_matches_the_json_safe_integer_boundary() {
        let at_limit = serde_json::json!({
            "type": "lane",
            "id": "telepathos:direct",
            "revision": MAX_SAFE_SEQUENCE,
            "turn_token": "turn-1",
        });
        assert!(ControlMsg::parse(&at_limit.to_string()).is_some());

        let mut one_over = at_limit;
        one_over["revision"] = (MAX_SAFE_SEQUENCE + 1).into();
        assert_eq!(ControlMsg::parse(&one_over.to_string()), None);
    }

    #[test]
    fn validates_installation_ids_without_normalizing_them() {
        let valid = r#"{"type":"hello","device":"opendots2","installation_id":"  opaque-owner  "}"#;
        assert_eq!(
            ControlMsg::parse(valid),
            Some(ControlMsg::Hello {
                device: "opendots2".into(),
                installation_id: "  opaque-owner  ".into(),
                token: None,
            })
        );

        for installation_id in [
            " ",
            "\t",
            "\n",
            "\u{000b}",
            "\u{000c}",
            "\r",
            "\u{0085}",
            "\u{00a0}",
            "\u{1680}",
            "\u{2007}",
            "\u{202f}",
            "\u{3000}",
            "\u{feff}",
            "\u{0000}",
            "\u{001f}",
            "\u{007f}",
            "\u{009f}",
            "\u{0000}owner",
            "owner\u{001f}",
            "owner\u{007f}",
            "owner\u{009f}",
            "owner\t",
            "owner\u{0085}",
        ] {
            let raw = serde_json::json!({
                "type": "hello",
                "device": "opendots2",
                "installation_id": installation_id,
            })
            .to_string();
            assert_eq!(
                ControlMsg::parse(&raw),
                None,
                "should reject: {installation_id:?}"
            );
        }

        for installation_id in [
            "owner",
            " owner ",
            "\u{00a0}owner\u{00a0}",
            "\u{2007}owner\u{2007}",
            "\u{202f}owner\u{202f}",
            "\u{feff}owner\u{feff}",
        ] {
            assert!(valid_installation_id(installation_id));
            let raw = serde_json::json!({
                "type": "hello",
                "device": "opendots2",
                "installation_id": installation_id,
            })
            .to_string();
            assert_eq!(
                ControlMsg::parse(&raw),
                Some(ControlMsg::Hello {
                    device: "opendots2".into(),
                    installation_id: installation_id.into(),
                    token: None,
                }),
                "valid installation ID was normalized: {installation_id:?}"
            );
        }

        let oversized = "x".repeat(MAX_INSTALLATION_ID_LENGTH + 1);
        let raw = serde_json::json!({
            "type": "hello",
            "device": "opendots2",
            "installation_id": oversized,
        })
        .to_string();
        assert_eq!(
            ControlMsg::parse(&raw),
            None,
            "should reject an oversized ID"
        );

        assert!(valid_installation_id(
            &"🦀".repeat(MAX_INSTALLATION_ID_LENGTH / 2)
        ));
        assert!(!valid_installation_id(
            &"🦀".repeat(MAX_INSTALLATION_ID_LENGTH / 2 + 1)
        ));
    }

    #[test]
    fn hello_round_trips_with_exact_v5_wire_shape() {
        let msg = ControlMsg::Hello {
            device: "opendots2".into(),
            installation_id: "  opaque-owner  ".into(),
            token: Some("secret".into()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"type":"hello","device":"opendots2","installation_id":"  opaque-owner  ","token":"secret"}"#
        );
        assert_eq!(ControlMsg::parse(&json), Some(msg));
    }

    #[test]
    fn receipt_control_variants_round_trip_with_exact_wire_names() {
        let controls = [
            (
                ControlMsg::ReplyReceived {
                    lane_id: "telepathos:direct".into(),
                    reply_to: "tp-1".into(),
                    after_seq: 4,
                    through_seq: 6,
                    turn_token: "turn-1".into(),
                    interaction_id: "i-1".into(),
                },
                "reply_received",
            ),
            (
                ControlMsg::ReplyAck {
                    lane_id: "telepathos:direct".into(),
                    reply_to: "tp-1".into(),
                    after_seq: 4,
                    through_seq: 6,
                    turn_token: "turn-1".into(),
                    interaction_id: "i-1".into(),
                },
                "reply_ack",
            ),
            (
                ControlMsg::ReplyAckRetire {
                    lane_id: "telepathos:direct".into(),
                    reply_to: "tp-1".into(),
                    after_seq: 4,
                    through_seq: 6,
                    turn_token: "turn-1".into(),
                    interaction_id: "i-1".into(),
                },
                "reply_ack_retire",
            ),
        ];

        for (msg, wire_type) in controls {
            let json = serde_json::to_string(&msg).unwrap();
            let value: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert_eq!(value["type"], wire_type);
            assert_eq!(ControlMsg::parse(&json), Some(msg));
        }
    }

    #[test]
    fn server_frames_round_trip() {
        let end = ServerMsg::AgentEnd {
            text: "hello".into(),
            turn_token: "turn-1".into(),
            interaction_id: "i-1".into(),
            lane_id: None,
            reply_to: None,
            after_seq: None,
            through_seq: None,
        };
        let end_value = serde_json::to_value(end).unwrap();
        assert_eq!(end_value["type"], "agent_end");
        assert_eq!(end_value["text"], "hello");

        let msg = ServerMsg::AgentDelta {
            text: "hi".into(),
            turn_token: "turn-1".into(),
            interaction_id: "i-1".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"agent_delta\""));
        assert!(json.contains("\"text\":\"hi\""));
        assert!(json.contains("\"turn_token\":\"turn-1\""));
        assert!(json.contains("\"interaction_id\":\"i-1\""));

        let stt = ServerMsg::Stt {
            text: "heard".into(),
            confidence: Some(0.75),
            repo: Some("telepathos:direct".into()),
            turn_token: "turn-1".into(),
            interaction_id: "i-1".into(),
        };
        let stt_value = serde_json::to_value(stt).unwrap();
        assert_eq!(stt_value["type"], "stt");
        assert_eq!(stt_value["text"], "heard");
        assert_eq!(stt_value["confidence"], 0.75);
        assert_eq!(stt_value["repo"], "telepathos:direct");

        let stt_without_metadata = serde_json::to_value(ServerMsg::Stt {
            text: "heard".into(),
            confidence: None,
            repo: None,
            turn_token: "turn-1".into(),
            interaction_id: "i-1".into(),
        })
        .unwrap();
        assert!(!stt_without_metadata
            .as_object()
            .unwrap()
            .contains_key("confidence"));
        assert!(!stt_without_metadata
            .as_object()
            .unwrap()
            .contains_key("repo"));
    }

    #[test]
    fn server_receipt_variants_use_exact_wire_names_and_fields() {
        let messages = [
            (
                ServerMsg::ReplyReceived {
                    lane_id: "telepathos:direct".into(),
                    reply_to: "tp-1".into(),
                    after_seq: 4,
                    through_seq: 6,
                    turn_token: "turn-1".into(),
                    interaction_id: "i-1".into(),
                },
                "reply_received",
            ),
            (
                ServerMsg::ReplyAcknowledged {
                    lane_id: "telepathos:direct".into(),
                    reply_to: "tp-1".into(),
                    after_seq: 4,
                    through_seq: 6,
                    turn_token: "turn-1".into(),
                    interaction_id: "i-1".into(),
                },
                "reply_acknowledged",
            ),
            (
                ServerMsg::ReplyAckRetired {
                    lane_id: "telepathos:direct".into(),
                    reply_to: "tp-1".into(),
                    after_seq: 4,
                    through_seq: 6,
                    turn_token: "turn-1".into(),
                    interaction_id: "i-1".into(),
                },
                "reply_ack_retired",
            ),
        ];

        for (msg, wire_type) in messages {
            let value = serde_json::to_value(msg).unwrap();
            assert_eq!(value["type"], wire_type);
            assert_eq!(value["lane_id"], "telepathos:direct");
            assert_eq!(value["reply_to"], "tp-1");
            assert_eq!(value["after_seq"], 4);
            assert_eq!(value["through_seq"], 6);
            assert_eq!(value["turn_token"], "turn-1");
            assert_eq!(value["interaction_id"], "i-1");
        }
    }
}

/// Serialize an outbound frame only after validating every correlation field.
/// Callers that need a wire frame should use this helper instead of bypassing
/// the shared ID admission boundary with `serde_json::to_string`.
pub fn serialize_server_msg(message: &ServerMsg) -> Option<String> {
    valid_server_msg(message)
        .then(|| serde_json::to_string(message).ok())
        .flatten()
}
