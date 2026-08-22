//! Hermes Relay↔Connector implementation (contract v1, EXPERIMENTAL upstream).
//!
//! We are the CONNECTOR: Hermes's gateway dials OUT to our `/relay` WebSocket,
//! authenticates with its per-gateway secret (§6.1), and then:
//!   - we push `{"type":"inbound","event":<MessageEvent>}` frames (user speech)
//!   - the gateway pushes action frames back (`send` ops carry replies)
//!
//! Tolerance policy: strictly implement what the contract specifies; LOG any
//! unrecognized frame verbatim instead of failing — first contact with a real
//! gateway will teach us the undocumented deltas.

use anyhow::Result;
use axum::{
    extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
use serde_json::json;
use std::sync::{Arc, Mutex};

/// A reply the gateway sent for a lane, awaiting pickup by the phone bridge.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Delivery {
    pub seq: u64,
    pub chat_id: String,
    pub content: String,
}

/// Channel capacity: utterances arrive at human speaking rate; if the gateway
/// falls 64 behind, we apply backpressure to the phone instead of buffering
/// without limit.
const RELAY_CHANNEL_CAP: usize = 64;

#[derive(Default)]
pub struct RelayState {
    /// Live socket push channel (set while a gateway is connected).
    pub outbound: Arc<Mutex<Option<tokio::sync::mpsc::Sender<String>>>>,
    /// Deliveries from the gateway awaiting phone pickup.
    pub deliveries: Arc<Mutex<Vec<Delivery>>>,
    pub next_seq: Arc<Mutex<u64>>,
}

impl RelayState {
    pub fn queue_delivery(&self, chat_id: &str, content: &str) -> u64 {
        let mut seq = self.next_seq.lock().unwrap();
        *seq += 1;
        let n = *seq;
        self.deliveries
            .lock()
            .unwrap()
            .push(Delivery {
                seq: n,
                chat_id: chat_id.to_string(),
                content: content.to_string(),
            });
        // bound the queue: drop oldest beyond 200
        let mut q = self.deliveries.lock().unwrap();
        if q.len() > 200 {
            let excess = q.len() - 200;
            q.drain(0..excess);
        }
        n
    }

    /// Deliveries after the caller's cursor. With `consume`, returned entries
    /// are removed — the caller has taken responsibility for speaking them.
    pub fn deliveries_after(&self, after: u64, consume: bool) -> (Vec<Delivery>, u64) {
        let mut q = self.deliveries.lock().unwrap();
        let picked: Vec<Delivery> = q.iter().filter(|d| d.seq > after).cloned().collect();
        let latest = q.last().map(|d| d.seq).unwrap_or(after);
        if consume && !picked.is_empty() {
            q.retain(|d| d.seq <= after);
        }
        (picked, latest)
    }

    /// Push an inbound user message to the connected gateway. Awaits channel
    /// capacity: a slow gateway slows the phone down rather than growing memory
    /// without bound. Errors when no gateway is dialed in.
    pub async fn push_inbound(&self, event: &serde_json::Value) -> Result<()> {
        let frame = json!({ "type": "inbound", "event": event });
        let tx = {
            let guard = self.outbound.lock().unwrap();
            guard.as_ref().cloned().ok_or_else(|| anyhow::anyhow!("no gateway connected"))?
        };
        tx.send(serde_json::to_string(&frame)?)
            .await
            .map_err(|e| anyhow::anyhow!("gateway socket closed: {e}"))
    }
}

/// §6.1: token = base64url(payload:exp:sig); sig = HMAC_SHA256(payload:exp, secret).
/// Accepts any secret in the rotation list. Returns the authenticated gateway id.
pub fn verify_relay_token(token_b64: &str, secrets: &[String]) -> Result<String> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token_b64)?;
    let decoded = String::from_utf8(raw)?;
    let parts: Vec<&str> = decoded.split(':').collect();
    if parts.len() != 3 {
        anyhow::bail!("malformed token");
    }
    let (payload, exp, sig_hex) = (parts[0], parts[1], parts[2]);
    let exp_ts: u64 = exp.parse()?;
    if std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        > exp_ts
    {
        anyhow::bail!("token expired");
    }
    let signed_input = format!("{payload}:{exp}");
    for secret in secrets {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
        mac.update(signed_input.as_bytes());
        let expect = hex::encode(mac.finalize().into_bytes());
        if expect == sig_hex.to_lowercase() {
            return Ok(payload.to_string());
        }
    }
    anyhow::bail!("no secret matched")
}

/// Build the CapabilityDescriptor (§2) — our platform's self-description.
pub fn capability_descriptor() -> serde_json::Value {
    json!({
        "contract_version": 1,
        "platform": "telepathy",
        "label": "Telepathy voice",
        "max_message_length": 0,          // 0 → gateway default 4096
        "supports_draft_streaming": false,
        "supports_edit": false,
        "supports_threads": false,
        "markdown_dialect": "plain",      // output is SPOKEN — plain words only
        "len_unit": "chars",
        "emoji": "🧠",
        "platform_hint": "User talks through open-ear earbuds. Replies are converted to \
speech on their phone. Prefer short conversational answers; never emit code blocks, \
tables, or long lists.",
        "supported_ops": ["send", "typing"],
    })
}

/// Normalize an utterance into the relay wire shape (§3 SessionSource + MessageEvent).
pub fn message_event(lane_id: &str, lane_name: &str, text: &str, msg_seq: u64) -> serde_json::Value {
    json!({
        "type": "inbound",
        "event": {
            "text": text,
            "message_type": "text",
            "user_id": "telepathy-user",
            "user_name": null,
            "source": {
                "platform": "telepathy",
                "chat_id": lane_id,
                "chat_type": "dm",
                "chat_name": lane_name,
                "user_id": "telepathy-user",
                "user_name": null,
                "thread_id": null,
                "chat_topic": null,
            },
            "message_id": format!("tp-{msg_seq}"),
        }
    })
}

/// The /relay route. Auth happens at upgrade time; a failure is an HTTP 401
/// (the contract specifies close-code 4401 post-upgrade — noted as a delta to
/// align once tested against a real gateway).
pub fn router(state: Arc<RelayState>, secrets: Vec<String>) -> Router {
    Router::new()
        .route(
            "/",
            get(move |ws: WebSocketUpgrade, headers: HeaderMap| {
                let secrets = secrets.clone();
                let state = state.clone();
                async move {
                    let bearer = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "));
                    let gateway_id = match bearer
                        .map(|t| verify_relay_token(t, &secrets))
                        .transpose()
                    {
                        Ok(Some(id)) => id,
                        _ => {
                            return Ok::<_, StatusCode>(
                                (StatusCode::UNAUTHORIZED, "relay auth failed").into_response(),
                            )
                        }
                    };
                    println!("relay: gateway '{gateway_id}' dialed in");
                    Ok(ws.on_upgrade(move |socket| relay_socket(socket, state)))
                }
            }),
        )
}

async fn relay_socket(mut socket: WebSocket, state: Arc<RelayState>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
    *state.outbound.lock().unwrap() = Some(tx);

    // one loop, both directions: our inbound pushes + the gateway's actions
    loop {
        tokio::select! {
            maybe_frame = rx.recv() => match maybe_frame {
                Some(frame) => {
                    if socket.send(WsMessage::Text(frame)).await.is_err() { break; }
                }
                None => break,
            },
            maybe_msg = socket.recv() => match maybe_msg {
                Some(Ok(WsMessage::Text(text))) => handle_gateway_frame(&state, &text),
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }

    *state.outbound.lock().unwrap() = None;
    println!("relay: gateway disconnected");
}

fn handle_gateway_frame(state: &RelayState, raw: &str) {
    let v: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => {
            println!("relay: non-JSON frame: {raw}");
            return;
        }
    };
    match v["type"].as_str() {
        // §4 send: the reply path. chat_id selects the lane.
        Some("action") | Some("send") => {
            let op = v["op"].as_str().or(v["action"]["op"].as_str());
            match op {
                Some("send") => {
                    let chat_id = v["chat_id"]
                        .as_str()
                        .or(v["action"]["chat_id"].as_str())
                        .unwrap_or("unknown");
                    let content = v["content"]
                        .as_str()
                        .or(v["action"]["content"].as_str())
                        .unwrap_or("");
                    let seq = state.queue_delivery(chat_id, content);
                    println!("relay: send → lane {chat_id} (seq {seq}): {}", truncate(content));
                }
                Some(other) => println!("relay: unhandled op '{other}' — logged, ignored"),
                None => println!("relay: action frame without op: {raw}"),
            }
        }
        other => println!("relay: unknown frame type {other:?}: {}", truncate(raw)),
    }
}

fn truncate(s: &str) -> &str {
    if s.len() > 120 { &s[..120.min(s.len())] } else { s }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mint(payload: &str, exp: u64, secret: &str) -> String {
        use base64::Engine;
        let input = format!("{payload}:{exp}");
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(input.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!("{input}:{sig}"))
    }

    #[test]
    fn valid_token_authenticates() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() + 300;
        let token = mint("gw-1", exp, "s3cret");
        assert_eq!(verify_relay_token(&token, &["s3cret".into()]).unwrap(), "gw-1");
    }

    #[test]
    fn wrong_secret_rejected() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() + 300;
        let token = mint("gw-1", exp, "wrong");
        assert!(verify_relay_token(&token, &["s3cret".into()]).is_err());
    }

    #[test]
    fn expired_token_rejected() {
        let token = mint("gw-1", 1, "s3cret");
        assert!(verify_relay_token(&token, &["s3cret".into()]).is_err());
    }

    #[test]
    fn rotation_list_accepts_second_secret() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() + 300;
        let token = mint("gw-2", exp, "new-secret");
        assert_eq!(
            verify_relay_token(&token, &["old".into(), "new-secret".into()]).unwrap(),
            "gw-2"
        );
    }

    #[test]
    fn descriptor_matches_contract_fields() {
        let d = capability_descriptor();
        assert_eq!(d["contract_version"], 1);
        assert_eq!(d["platform"], "telepathy");
        assert_eq!(d["markdown_dialect"], "plain");
        assert_eq!(d["supports_edit"], false);
    }

    #[test]
    fn message_event_envelope_shape() {
        let ev = message_event("telepathy:direct", "direct", "hello", 7);
        assert_eq!(ev["type"], "inbound");
        assert_eq!(ev["event"]["text"], "hello");
        assert_eq!(ev["event"]["source"]["platform"], "telepathy");
        assert_eq!(ev["event"]["source"]["chat_id"], "telepathy:direct");
        assert_eq!(ev["event"]["source"]["chat_type"], "dm");
        assert_eq!(ev["event"]["message_id"], "tp-7");
    }
}
