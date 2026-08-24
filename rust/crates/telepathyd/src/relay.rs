//! Hermes Relay↔Connector implementation (contract v3, EXPERIMENTAL upstream).
//!
//! We are the CONNECTOR: Hermes's gateway dials OUT to our `/relay` WebSocket,
//! authenticates with its per-gateway secret (§6.1), and then:
//!   - we push versioned `inbound` frames (user speech) until an exact
//!     `inbound_ack` is durably committed
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
use hmac::{digest::OutputSizeUser, Hmac, Mac};
use sha2::Sha256;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use telepathy_lanes::{is_valid_lane_id, LaneRegistry};
use telepathy_proto::{
    is_valid_opaque_id, MAX_OPAQUE_ID_BYTES, MAX_OPAQUE_ID_LENGTH, MAX_SAFE_SEQUENCE,
};

type HmacSha256 = Hmac<Sha256>;
use serde_json::json;
use std::sync::{Arc, Mutex};

/// A reply the gateway sent for a lane, awaiting pickup by the phone bridge.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Delivery {
    pub seq: u64,
    pub chat_id: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    /// Epoch seconds when the delivery was queued — the phone speaks
    /// "3 hours ago at 14:32" from this.
    #[serde(default)]
    pub arrived_at: u64,
}

/// The delivery queue and its sequence high-water mark are one durable
/// snapshot. Keeping them together prevents a successful queue write paired
/// with a failed sidecar sequence write from producing two contradictory
/// restart states.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DeliverySnapshot {
    /// This snapshot evolves as one durable protocol unit. Do not accept a
    /// pre-tombstone snapshot: it cannot distinguish a retry of a retired
    /// acknowledgement from an acknowledgement with a forged result ID.
    version: u32,
    /// Effective wall-clock high-water mark used for result abandonment. It
    /// is persisted with the ledger so a clock rollback cannot make an old
    /// result look newer after restart.
    clock_high_water_ms: u64,
    deliveries: Vec<Delivery>,
    next_seq: u64,
    /// Results keyed by authenticated gateway identity plus the gateway-
    /// provided outbound request ID. This lives in the same snapshot as
    /// deliveries so a reply is never durable without its idempotency record
    /// (or vice versa).
    outbound_results: Vec<OutboundResult>,
    /// Monotonic generation for outbound-result receipts. A request ID may be
    /// reused only after its prior result is durably retired; this generation
    /// keeps a delayed acknowledgement for the old action from retiring the
    /// new one.
    next_outbound_result_id: u64,
    /// Exact receipt pairs that were durably retired. They make a lost
    /// retirement response restart-safe without accepting a different
    /// `resultId` for the same reusable request ID.
    retired_outbound_results: Vec<RetiredOutboundResult>,
}

/// A durable response to a gateway `outbound` action. `delivery_seq` records
/// the delivery created by a successful send, but consumption never releases
/// this request ID: the gateway may have missed the result and retry after the
/// phone speaks the delivery. The gateway retires this record explicitly with
/// the matching `resultId`, after it has received the result. This preserves
/// retry safety without permanently filling the bounded ledger.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OutboundResult {
    /// Authenticated gateway that owns this request/result generation. Request
    /// IDs are only unique within this identity namespace.
    #[serde(default)]
    gateway_id: String,
    request_id: String,
    result_id: u64,
    result: serde_json::Value,
    delivery_seq: Option<u64>,
    /// Time this result was durably created. The
    /// record is abandonable only after this timestamp ages past the
    /// retention window and its gateway has no active connection.
    last_seen_at_ms: u64,
}

/// A bounded, durable proof that one exact request/result receipt was
/// retired. Request IDs may be reused after retirement, so this is keyed by
/// gateway identity plus the request/result pair rather than by request ID
/// alone.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RetiredOutboundResult {
    /// Authenticated gateway that owns this retired request/result generation.
    #[serde(default)]
    gateway_id: String,
    request_id: String,
    result_id: u64,
}

/// A response to a gateway action. Only durable responses carry a result ID
/// and are eligible for explicit retirement. Capacity rejections are not
/// idempotency records: after the gateway makes space by acknowledging an
/// older result, it may retry the action.
#[derive(Debug, Clone, PartialEq)]
struct GatewayActionResult {
    result: serde_json::Value,
    result_id: Option<u64>,
}

impl GatewayActionResult {
    fn durable(record: &OutboundResult) -> Self {
        Self {
            result: record.result.clone(),
            result_id: Some(record.result_id),
        }
    }

    fn transient(result: serde_json::Value) -> Self {
        Self {
            result,
            result_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultRetirement {
    Retired,
    AlreadyRetired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundAckResult {
    Acknowledged,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PendingRequest {
    lane_id: String,
    message_id: String,
}

/// An inbound voice turn waiting for application acknowledgement from the
/// gateway. Transport acceptance is never a terminal handoff signal.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct InboundMessage {
    message_id: String,
    generation: u64,
    frame: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct InboundAckTombstone {
    message_id: String,
    generation: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct InboundSnapshot {
    version: u32,
    next_generation: u64,
    messages: Vec<InboundMessage>,
    acknowledged: Vec<InboundAckTombstone>,
}

/// Channel capacity: utterances arrive at human speaking rate; if the gateway
/// falls 64 behind, we apply backpressure to the phone instead of buffering
/// without limit.
const RELAY_CHANNEL_CAP: usize = 64;
pub const MAX_PENDING_DELIVERIES: usize = 200;
/// Every complete reply/delivery is bounded by the shared protocol limit,
/// measured in UTF-8 bytes before it enters durable state.
pub const MAX_DELIVERY_CONTENT_BYTES: usize = 512 * 1024;
/// Bound the serialized delivery records, not merely the number of records.
/// This includes every caller-controlled string persisted with a delivery and
/// keeps the snapshot comfortably below the relay's 1 MiB frame ceiling per
/// action even when a gateway sends many large replies.
const MAX_PENDING_DELIVERY_BYTES: usize = 8 * 1024 * 1024;
/// Gateway request IDs remain durable until the gateway explicitly confirms
/// its matching result. Never evict a request ID merely to admit a newer
/// action: a gateway that did not receive its result must be able to retry
/// safely.
const MAX_OUTBOUND_RESULTS: usize = 200;
/// The durable result ledger is bounded independently of the delivery queue.
/// Result values are normally tiny, but this protects the snapshot from any
/// future action that accidentally reflects gateway-controlled input.
const MAX_OUTBOUND_RESULT_LEDGER_BYTES: usize = 1024 * 1024;
/// Keep restart-safe retirement retries without permitting a gateway to make
/// acknowledgements an unbounded durable store. Old tombstones are evicted
/// only to admit a newer exact retirement or abandonment; an evicted receipt
/// is rejected rather than treated as a successful acknowledgement.
const MAX_RETIRED_OUTBOUND_RESULTS: usize = MAX_OUTBOUND_RESULTS;
const MAX_RETIRED_OUTBOUND_RESULT_BYTES: usize = 32 * 1024;
const DELIVERY_SNAPSHOT_VERSION: u32 = 4;
/// A disconnected gateway owns its idempotency records for this long. After
/// the window, a retry may be treated as a new action and can create a second
/// delivery. Active connections are never aged out.
const OUTBOUND_RESULT_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const OUTBOUND_RESULT_RETENTION_MS: u64 = OUTBOUND_RESULT_RETENTION.as_millis() as u64;
/// This is intentionally equal to the global cap today. Keeping the explicit
/// per-gateway limit makes the ownership policy visible and leaves room for a
/// stricter per-identity cap without changing the admission algorithm.
const MAX_OUTBOUND_RESULTS_PER_GATEWAY: usize = MAX_OUTBOUND_RESULTS;
/// Direct state/frame helpers are intentionally scoped to this explicit
/// development identity. Authenticated WebSocket traffic always supplies the
/// identity returned by `verify_relay_token`.
const DEV_GATEWAY_ID: &str = "unauthenticated";
/// Operation names are protocol identifiers, not free-form text. Refusing
/// oversized names before formatting an error keeps both the WebSocket result
/// and its durable record below the relay's application frame ceiling.
const MAX_OUTBOUND_OPERATION_BYTES: usize = 256;
/// `resultId` is a JSON number consumed by the gateway. Stay in JavaScript's
/// exact-integer range so a gateway can faithfully echo an acknowledgement.
const MAX_OUTBOUND_RESULT_ID: u64 = MAX_SAFE_SEQUENCE;
const DELIVERY_SEQUENCE_EXHAUSTED_ERROR: &str =
    "delivery sequence limit reached; start a new durable relay snapshot";
/// Request IDs are normally UUIDs, ULIDs, or short gateway-scoped strings.
/// Bound them before copying into the durable idempotency ledger so a single
/// gateway frame cannot consume an outsized amount of memory or disk.
const MAX_OUTBOUND_REQUEST_ID_BYTES: usize = 256;
const MAX_GATEWAY_ID_BYTES: usize = 256;
/// Bound one serialized durable inbound record. The same bound is also used
/// for the NDJSON transport line so admission and transport cannot disagree.
pub(crate) const MAX_INBOUND_RECORD_BYTES: usize = 1 << 20;
const MAX_INBOUND_LINE_BYTES: usize = MAX_INBOUND_RECORD_BYTES;
/// Error returned when the serialized durable record exceeds the application
/// frame ceiling.
const INBOUND_RECORD_TOO_LARGE_ERROR: &str =
    "inbound durable record exceeds its serialized size limit";
/// Use the same serialized-array ceiling as the pending delivery outbox.
const MAX_INBOUND_QUEUE_BYTES: usize = MAX_PENDING_DELIVERY_BYTES;
const INBOUND_SNAPSHOT_VERSION: u32 = 1;
const INBOUND_HANDOFF_VERSION: u32 = 2;
const MAX_INBOUND_TOMBSTONES: usize = MAX_PENDING_DELIVERIES;
const MAX_INBOUND_TOMBSTONE_BYTES: usize = 32 * 1024;
/// The WebSocket transport must never assemble a frame larger than the
/// application-level NDJSON buffer. Keep the transport and parser limits in
/// lockstep so an oversized frame is rejected before tungstenite's 64 MiB
/// default can be used.
const MAX_RELAY_WEBSOCKET_MESSAGE_BYTES: usize = MAX_INBOUND_LINE_BYTES;
const RELAY_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const DURABLE_QUEUE_FULL_ERROR: &str = "durable queue full";
const DURABLE_QUEUE_BYTE_LIMIT_ERROR: &str = "durable queue byte limit reached";
pub const DELIVERY_CONTENT_TOO_LARGE_ERROR: &str = "delivery content exceeds 512 KiB UTF-8 limit";
const DURABLE_REQUEST_LEDGER_FULL_ERROR: &str = "durable request ledger full";
const DURABLE_REQUEST_LEDGER_BYTE_LIMIT_ERROR: &str = "durable request ledger byte limit reached";
const CHAT_ID_REQUIRED_ERROR: &str = "chat_id is required";
const CONTENT_REQUIRED_ERROR: &str = "content is required";
const REPLY_TO_BLANK_ERROR: &str = "reply_to must not be blank";
const OUTBOUND_OPERATION_TOO_LONG_ERROR: &str = "op exceeds maximum length";
const RESULT_ID_REQUIRED_ERROR: &str = "resultId is required";
const RESULT_ID_MISMATCH_ERROR: &str = "resultId does not match outstanding request";
const INBOUND_QUEUE_BYTE_LIMIT_ERROR: &str = "inbound relay queue byte limit reached";
const INBOUND_ACK_MESSAGE_ID_REQUIRED_ERROR: &str = "messageId is required";
const INBOUND_ACK_GENERATION_REQUIRED_ERROR: &str = "generation is required";
const INBOUND_ACK_MISMATCH_ERROR: &str = "inbound acknowledgement does not match a pending message";
const INBOUND_ACK_PERSISTENCE_ERROR: &str = "inbound acknowledgement persistence failed";
static WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InboundRecordTooLarge {
    pub(crate) actual_bytes: usize,
    pub(crate) limit_bytes: usize,
}

impl std::fmt::Display for InboundRecordTooLarge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(INBOUND_RECORD_TOO_LARGE_ERROR)
    }
}

impl std::error::Error for InboundRecordTooLarge {}

pub(crate) fn is_inbound_record_too_large(error: &anyhow::Error) -> bool {
    error.downcast_ref::<InboundRecordTooLarge>().is_some()
}

#[derive(Debug)]
struct AtomicWriteError {
    source: std::io::Error,
    /// `rename` completed, so the target may already contain the new
    /// snapshot even though we could not durably sync its directory.
    renamed: bool,
}

impl AtomicWriteError {
    fn before_rename(source: std::io::Error) -> Self {
        Self {
            source,
            renamed: false,
        }
    }

    fn after_rename(source: std::io::Error) -> Self {
        Self {
            source,
            renamed: true,
        }
    }
}

impl std::fmt::Display for AtomicWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for AtomicWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
struct DurableQueueFull;

impl std::fmt::Display for DurableQueueFull {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(DURABLE_QUEUE_FULL_ERROR)
    }
}

impl std::error::Error for DurableQueueFull {}

struct ActiveConnection {
    id: u64,
    gateway_id: String,
    tx: tokio::sync::mpsc::Sender<String>,
    superseded: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct ConnectionState {
    next_id: u64,
    /// The newest connection that has finished the protocol handshake. A
    /// socket that was waiting for its predecessor to leave must re-check this
    /// before it becomes the relay writer.
    latest_id: u64,
    outbound: Option<ActiveConnection>,
}

/// Durable pending-delivery queue: cron results and lane replies survive
/// daemon restarts until the phone consumes them.
#[derive(Default)]
pub struct RelayState {
    /// The sole live socket that may write inbound turns. A newer completed
    /// handshake supersedes its predecessor before becoming active.
    connections: Arc<Mutex<ConnectionState>>,
    /// Serializes socket ownership. Holding this for an active connection
    /// means a successor cannot replay the durable outbox until its
    /// predecessor has stopped writing it.
    connection_writer_lock: Arc<tokio::sync::Mutex<()>>,
    /// Deliveries from the gateway awaiting phone pickup.
    pub deliveries: Arc<Mutex<Vec<Delivery>>>,
    /// The API and relay share this one registry object. The relay only reads
    /// it while validating an outbound `send`; lane creation remains dynamic
    /// because the API mutates this same object.
    lane_registry: Arc<Mutex<Option<Arc<tokio::sync::Mutex<LaneRegistry>>>>>,
    pub next_seq: Arc<Mutex<u64>>,
    outbound_results: Arc<Mutex<Vec<OutboundResult>>>,
    next_outbound_result_id: Arc<Mutex<u64>>,
    retired_outbound_results: Arc<Mutex<Vec<RetiredOutboundResult>>>,
    clock_high_water_ms: Arc<Mutex<u64>>,
    /// Test-only clock injection keeps expiry and rollback behavior
    /// deterministic without changing production clock semantics.
    #[cfg(test)]
    clock_override_ms: Arc<Mutex<Option<u64>>>,
    pub persist_path: Arc<Mutex<Option<PathBuf>>>,
    queue_mutation_lock: Arc<Mutex<()>>,
    pending_requests: Arc<Mutex<Vec<PendingRequest>>>,
    request_persist_path: Arc<Mutex<Option<PathBuf>>>,
    inbound: Arc<Mutex<Vec<InboundMessage>>>,
    next_inbound_generation: Arc<Mutex<u64>>,
    acknowledged_inbound: Arc<Mutex<Vec<InboundAckTombstone>>>,
    inbound_persist_path: Arc<Mutex<Option<PathBuf>>>,
    persist_lock: Arc<Mutex<()>>,
    /// A post-rename persistence error leaves the final on-disk state
    /// uncertain. Once that happens, do not permit a later operation to
    /// overwrite the snapshot from a potentially stale in-memory rollback.
    persistence_failure: Arc<Mutex<Option<String>>>,
}

impl RelayState {
    pub fn set_lane_registry(&self, registry: Arc<tokio::sync::Mutex<LaneRegistry>>) {
        *self.lane_registry.lock().unwrap() = Some(registry);
    }

    async fn validate_lane_id(&self, lane_id: &str) -> std::result::Result<bool, &'static str> {
        let registry = self.lane_registry.lock().unwrap().clone();
        let Some(registry) = registry else {
            return Err("lane registry is unavailable");
        };
        let known = registry
            .lock()
            .await
            .lanes
            .iter()
            .any(|lane| lane.id == lane_id);
        Ok(known)
    }

    pub fn set_persist_path(&self, p: &PathBuf) {
        *self.persist_path.lock().unwrap() = Some(p.clone());
        let request_path = request_path(p);
        *self.request_persist_path.lock().unwrap() = Some(request_path.clone());
        let inbound_path = inbound_path(p);
        *self.inbound_persist_path.lock().unwrap() = Some(inbound_path.clone());
        let startup_registry = self.lane_registry.lock().unwrap().clone().map(|registry| {
            registry
                .try_lock()
                .expect("lane registry must be available during relay startup")
                .clone()
        });
        // reload any previously-pending entries
        match fs::read_to_string(p) {
            Ok(json) => match serde_json::from_str::<DeliverySnapshot>(&json) {
                Ok(snapshot) => {
                    let list = snapshot.deliveries;
                    let results = snapshot.outbound_results;
                    let retired_results = snapshot.retired_outbound_results;
                    let clock_high_water_ms = snapshot.clock_high_water_ms;
                    if let Err(error) = validate_delivery_snapshot_version(snapshot.version) {
                        panic!(
                            "invalid delivery snapshot {}; refusing to start: {error}",
                            p.display(),
                        );
                    }
                    if let Err(error) = validate_delivery_snapshot(
                        &list,
                        snapshot.next_seq,
                        startup_registry.as_ref(),
                    ) {
                        panic!(
                            "invalid delivery snapshot {}; refusing to start: {error}",
                            p.display(),
                        );
                    }
                    if let Err(error) = validate_outbound_result_snapshot(
                        &results,
                        &retired_results,
                        snapshot.next_outbound_result_id,
                        clock_high_water_ms,
                        snapshot.next_seq,
                    ) {
                        panic!(
                            "invalid outbound request ledger in delivery snapshot {}; refusing to start: {error}",
                            p.display(),
                        );
                    }
                    let mut q = self.deliveries.lock().unwrap();
                    let max_seq = q.iter().map(|d| d.seq).max().unwrap_or(0);
                    let loaded_max = list.iter().map(|d| d.seq).max().unwrap_or(0);
                    q.extend(list.into_iter().filter(|d| d.seq > max_seq));
                    let mut next = self.next_seq.lock().unwrap();
                    *next = (*next)
                        .max(snapshot.next_seq)
                        .max(loaded_max)
                        .max(q.iter().map(|d| d.seq).max().unwrap_or(0));
                    *self.outbound_results.lock().unwrap() = results;
                    *self.next_outbound_result_id.lock().unwrap() =
                        snapshot.next_outbound_result_id;
                    *self.retired_outbound_results.lock().unwrap() = retired_results;
                    let mut clock = self.clock_high_water_ms.lock().unwrap();
                    *clock = (*clock).max(clock_high_water_ms);
                }
                Err(e) => panic!(
                    "corrupt delivery snapshot {}; refusing to start: {e}",
                    p.display()
                ),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!(
                "cannot read delivery snapshot {}; refusing to start: {e}",
                p.display()
            ),
        }
        match fs::read_to_string(&request_path) {
            Ok(json) => match serde_json::from_str::<Vec<PendingRequest>>(&json) {
                Ok(list) => {
                    if let Err(error) = validate_pending_requests(&list) {
                        panic!(
                            "invalid request snapshot {}; refusing to start: {error}",
                            request_path.display()
                        );
                    }
                    *self.pending_requests.lock().unwrap() = list;
                }
                Err(e) => panic!(
                    "corrupt request snapshot {}; refusing to start: {e}",
                    request_path.display()
                ),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!(
                "cannot read request snapshot {}; refusing to start: {e}",
                request_path.display()
            ),
        }
        match fs::read_to_string(&inbound_path) {
            Ok(json) => match serde_json::from_str::<InboundSnapshot>(&json) {
                Ok(snapshot) => {
                    if let Err(error) = validate_inbound_snapshot(&snapshot) {
                        panic!(
                            "invalid inbound snapshot {}; refusing to start: {error}",
                            inbound_path.display()
                        );
                    }
                    *self.inbound.lock().unwrap() = snapshot.messages;
                    *self.next_inbound_generation.lock().unwrap() = snapshot.next_generation;
                    *self.acknowledged_inbound.lock().unwrap() = snapshot.acknowledged;
                }
                Err(e) => panic!(
                    "corrupt inbound snapshot {}; refusing to start: {e}",
                    inbound_path.display()
                ),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!(
                "cannot read inbound snapshot {}; refusing to start: {e}",
                inbound_path.display()
            ),
        }
    }

    /// Reserve a generation for a socket that has successfully completed the
    /// gateway handshake. A newer reservation immediately asks an existing
    /// writer to stop; it cannot itself write until it owns
    /// `connection_writer_lock` and activates below.
    fn begin_connection(&self) -> u64 {
        let (id, previous) = {
            let mut connections = self.connections.lock().unwrap();
            connections.next_id += 1;
            let id = connections.next_id;
            connections.latest_id = id;
            (id, connections.outbound.take())
        };
        if let Some(previous) = previous {
            let _ = previous.superseded.send(true);
        }
        id
    }

    /// Make a handshaken connection the sole relay writer. A connection that
    /// lost a race to a newer handshake never writes or replays the outbox.
    fn activate_connection(
        &self,
        id: u64,
        gateway_id: String,
        tx: tokio::sync::mpsc::Sender<String>,
        superseded: tokio::sync::watch::Sender<bool>,
    ) -> bool {
        let mut connections = self.connections.lock().unwrap();
        if connections.latest_id != id || connections.outbound.is_some() {
            return false;
        }
        connections.outbound = Some(ActiveConnection {
            id,
            gateway_id,
            tx,
            superseded,
        });
        true
    }

    fn is_active_connection(&self, id: u64) -> bool {
        let connections = self.connections.lock().unwrap();
        connections.latest_id == id
            && connections
                .outbound
                .as_ref()
                .is_some_and(|current| current.id == id)
    }

    fn clear_outbound_if(&self, id: u64) {
        let mut connections = self.connections.lock().unwrap();
        if connections
            .outbound
            .as_ref()
            .is_some_and(|current| current.id == id)
        {
            connections.outbound = None;
        }
    }

    fn persist(&self) -> Result<()> {
        self.ensure_persistence_healthy()?;
        let path = self.persist_path.lock().unwrap().clone();
        let Some(path) = path else { return Ok(()) };
        let _guard = self.persist_lock.lock().unwrap();
        let snapshot = DeliverySnapshot {
            version: DELIVERY_SNAPSHOT_VERSION,
            clock_high_water_ms: *self.clock_high_water_ms.lock().unwrap(),
            deliveries: self.deliveries.lock().unwrap().clone(),
            next_seq: *self.next_seq.lock().unwrap(),
            outbound_results: self.outbound_results.lock().unwrap().clone(),
            next_outbound_result_id: *self.next_outbound_result_id.lock().unwrap(),
            retired_outbound_results: self.retired_outbound_results.lock().unwrap().clone(),
        };
        validate_delivery_snapshot(&snapshot.deliveries, snapshot.next_seq, None)?;
        let json = serde_json::to_string_pretty(&snapshot)?;
        if let Err(error) = atomic_write(&path, &json) {
            let message = format!(
                "cannot persist delivery snapshot {}: {error}",
                path.display()
            );
            if error.renamed {
                self.fail_persistence(&message);
            }
            return Err(anyhow::anyhow!(message));
        }
        Ok(())
    }

    fn persist_requests(&self) {
        // `register_request` is intentionally best-effort for its legacy
        // sidecar API, but it must never overwrite a path after a prior write
        // reached rename and then failed its directory sync.
        if self.persistence_failure.lock().unwrap().is_some() {
            return;
        }
        let path = self.request_persist_path.lock().unwrap().clone();
        if let Some(path) = path {
            let _guard = self.persist_lock.lock().unwrap();
            if let Ok(json) = serde_json::to_string_pretty(&*self.pending_requests.lock().unwrap())
            {
                if let Err(e) = atomic_write(&path, &json) {
                    if e.renamed {
                        self.fail_persistence(&format!(
                            "cannot persist requests {} after rename: {e}",
                            path.display()
                        ));
                    }
                    eprintln!("relay: cannot persist requests {}: {e}", path.display());
                }
            }
        }
    }

    fn persist_inbound(&self) -> Result<()> {
        self.ensure_persistence_healthy()?;
        let path = self.inbound_persist_path.lock().unwrap().clone();
        let Some(path) = path else { return Ok(()) };
        let _guard = self.persist_lock.lock().unwrap();
        let snapshot = InboundSnapshot {
            version: INBOUND_SNAPSHOT_VERSION,
            next_generation: *self.next_inbound_generation.lock().unwrap(),
            messages: self.inbound.lock().unwrap().clone(),
            acknowledged: self.acknowledged_inbound.lock().unwrap().clone(),
        };
        validate_inbound_snapshot(&snapshot)?;
        let json = serde_json::to_string_pretty(&snapshot)?;
        if let Err(error) = atomic_write(&path, &json) {
            let message = format!(
                "cannot persist inbound snapshot {}: {error}",
                path.display()
            );
            if error.renamed {
                self.fail_persistence(&message);
            }
            return Err(anyhow::anyhow!(message));
        }
        Ok(())
    }

    fn ensure_persistence_healthy(&self) -> Result<()> {
        let failure = self.persistence_failure.lock().unwrap().clone();
        match failure {
            Some(failure) => Err(anyhow::anyhow!(
                "relay durable state is unavailable after an uncertain commit: {failure}"
            )),
            None => Ok(()),
        }
    }

    fn wall_clock_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0)
    }

    fn effective_clock_millis(&self) -> u64 {
        #[cfg(test)]
        let now = self
            .clock_override_ms
            .lock()
            .unwrap()
            .unwrap_or_else(Self::wall_clock_millis);
        #[cfg(not(test))]
        let now = Self::wall_clock_millis();

        let mut high_water = self.clock_high_water_ms.lock().unwrap();
        *high_water = (*high_water).max(now);
        *high_water
    }

    #[cfg(test)]
    fn set_clock_for_test(&self, millis: u64) {
        *self.clock_override_ms.lock().unwrap() = Some(millis);
    }

    fn active_gateway_id(&self) -> Option<String> {
        self.connections
            .lock()
            .unwrap()
            .outbound
            .as_ref()
            .map(|connection| connection.gateway_id.clone())
    }

    fn fail_persistence(&self, failure: &str) {
        let mut recorded = self.persistence_failure.lock().unwrap();
        if recorded.is_none() {
            *recorded = Some(failure.to_string());
        }
    }

    fn can_roll_back_persistence(&self) -> bool {
        self.persistence_failure.lock().unwrap().is_none()
    }
}

pub(crate) fn request_path(path: &PathBuf) -> PathBuf {
    let mut out = path.clone();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("pending.json");
    out.set_file_name(format!("{name}.requests"));
    out
}

pub(crate) fn inbound_path(path: &PathBuf) -> PathBuf {
    let mut out = path.clone();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("pending.json");
    out.set_file_name(format!("{name}.inbound"));
    out
}

fn create_private_file(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    // Unix creates the temp file with its final mode, so there is no window in
    // which another local user can read voice or reply content before rename.
    // Other platforms retain their native file-permission semantics.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_POST_RENAME_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static NEW_DIRECTORY_PARENT_SYNC_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FAIL_NEW_DIRECTORY_PARENT_SYNC_AT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn fail_next_post_rename_directory_sync() {
    FAIL_NEXT_POST_RENAME_DIRECTORY_SYNC.with(|fail| fail.set(true));
}

#[cfg(test)]
fn reset_new_directory_parent_sync_hook(fail_at: Option<usize>) {
    NEW_DIRECTORY_PARENT_SYNC_CALLS.with(|calls| calls.set(0));
    FAIL_NEW_DIRECTORY_PARENT_SYNC_AT.with(|failure| failure.set(fail_at));
}

#[cfg(test)]
fn new_directory_parent_sync_calls() -> usize {
    NEW_DIRECTORY_PARENT_SYNC_CALLS.with(std::cell::Cell::get)
}

fn sync_parent_directory(parent: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_POST_RENAME_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err(std::io::Error::other(
            "injected post-rename directory sync failure",
        ));
    }
    fs::File::open(parent)?.sync_all()
}

fn sync_new_directory_parent(parent: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    {
        let call = NEW_DIRECTORY_PARENT_SYNC_CALLS.with(|calls| {
            let call = calls.get() + 1;
            calls.set(call);
            call
        });
        if FAIL_NEW_DIRECTORY_PARENT_SYNC_AT.with(|failure| failure.get()) == Some(call) {
            return Err(std::io::Error::other(
                "injected newly-created directory parent sync failure",
            ));
        }
    }
    fs::File::open(parent)?.sync_all()
}

/// Recursively create a snapshot parent directory, syncing the parent of each
/// newly discovered directory entry before the snapshot can rely on that
/// path. `create_dir_all` does not expose which entries it created, so it
/// cannot establish this durability boundary.
fn create_parent_directories_durably(directory: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut current = directory.to_path_buf();

    loop {
        match fs::metadata(&current) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    format!("{} is not a directory", current.display()),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(current.clone());
                current = parent_or_current_directory(&current).to_path_buf();
            }
            Err(error) => return Err(error),
        }
    }

    for created in missing.into_iter().rev() {
        match fs::create_dir(&created) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::metadata(&created)?;
                if !metadata.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!("{} is not a directory", created.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        // A concurrent creator may have won the mkdir race. It is still a
        // newly discovered directory in this write path, so make its parent
        // durable before continuing into that directory.
        sync_new_directory_parent(parent_or_current_directory(&created))?;
    }
    Ok(())
}

fn parent_or_current_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn atomic_write(path: &Path, contents: &str) -> Result<(), AtomicWriteError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    create_parent_directories_durably(parent).map_err(AtomicWriteError::before_rename)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snapshot");
    let nonce = WRITE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp = parent.join(format!(
        ".{name}.tmp-{}-{timestamp}-{nonce}",
        std::process::id()
    ));
    let result = (|| -> Result<(), AtomicWriteError> {
        let mut file = create_private_file(&temp).map_err(AtomicWriteError::before_rename)?;
        file.write_all(contents.as_bytes())
            .map_err(AtomicWriteError::before_rename)?;
        file.sync_all().map_err(AtomicWriteError::before_rename)?;
        fs::rename(&temp, path).map_err(AtomicWriteError::before_rename)?;
        sync_parent_directory(parent).map_err(AtomicWriteError::after_rename)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Serialized size is the relevant durable footprint: JSON escaping may make
/// a string substantially larger than its UTF-8 byte length.
fn serialized_delivery_bytes(delivery: &Delivery) -> Result<usize> {
    Ok(serde_json::to_vec(delivery)?.len())
}

/// Size of the JSON array holding the pending delivery records. This excludes
/// the small snapshot envelope and bounded request-result ledger; the latter
/// has its own entry and request-ID limits.
fn pending_delivery_bytes(deliveries: &[Delivery]) -> Result<usize> {
    let mut bytes = 2usize; // `[]`
    for (index, delivery) in deliveries.iter().enumerate() {
        let separator = usize::from(index > 0);
        let delivery_bytes = serialized_delivery_bytes(delivery)?;
        bytes = bytes
            .checked_add(separator)
            .and_then(|bytes| bytes.checked_add(delivery_bytes))
            .ok_or_else(|| anyhow::anyhow!("pending delivery byte count overflow"))?;
    }
    Ok(bytes)
}

fn would_exceed_pending_delivery_bytes(
    deliveries: &[Delivery],
    candidate: &Delivery,
) -> Result<bool> {
    let current = pending_delivery_bytes(deliveries)?;
    let additional = serialized_delivery_bytes(candidate)?
        .checked_add(usize::from(!deliveries.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("pending delivery byte count overflow"))?;
    Ok(current
        .checked_add(additional)
        .map_or(true, |total| total > MAX_PENDING_DELIVERY_BYTES))
}

/// Delivery cursors cross the JSON protocol boundary, so they must stay
/// exactly representable by JavaScript even when a caller invokes the relay
/// directly instead of going through the HTTP handlers.
fn validate_delivery_sequence_boundary(name: &str, sequence: u64) -> Result<()> {
    if sequence > MAX_SAFE_SEQUENCE {
        anyhow::bail!("{name} exceeds the maximum safe delivery sequence");
    }
    Ok(())
}

fn validate_reply_to(reply_to: Option<&str>) -> Result<()> {
    if let Some(reply_to) = reply_to {
        if !is_valid_opaque_id(reply_to) {
            anyhow::bail!(REPLY_TO_BLANK_ERROR);
        }
    }
    Ok(())
}

fn validate_delivery_content(content: &str) -> Result<()> {
    if content.as_bytes().len() > MAX_DELIVERY_CONTENT_BYTES {
        anyhow::bail!(DELIVERY_CONTENT_TOO_LARGE_ERROR);
    }
    Ok(())
}

fn validate_delivery_snapshot(
    deliveries: &[Delivery],
    next_seq: u64,
    registry: Option<&LaneRegistry>,
) -> Result<()> {
    if deliveries.len() > MAX_PENDING_DELIVERIES {
        anyhow::bail!(DURABLE_QUEUE_FULL_ERROR);
    }
    if pending_delivery_bytes(deliveries)? > MAX_PENDING_DELIVERY_BYTES {
        anyhow::bail!(DURABLE_QUEUE_BYTE_LIMIT_ERROR);
    }

    let mut previous_seq = 0;
    for delivery in deliveries {
        if delivery.seq == 0 || delivery.seq > MAX_SAFE_SEQUENCE || delivery.seq <= previous_seq {
            anyhow::bail!("delivery sequences must be strictly increasing and non-zero");
        }
        if !is_valid_lane_id(&delivery.chat_id) {
            anyhow::bail!(CHAT_ID_REQUIRED_ERROR);
        }
        if let Some(registry) = registry {
            if !registry
                .lanes
                .iter()
                .any(|lane| lane.id == delivery.chat_id)
            {
                anyhow::bail!("unknown lane {}", delivery.chat_id);
            }
        }
        if delivery.content.trim().is_empty() {
            anyhow::bail!(CONTENT_REQUIRED_ERROR);
        }
        validate_delivery_content(&delivery.content)?;
        validate_reply_to(delivery.reply_to.as_deref())?;
        previous_seq = delivery.seq;
    }
    if next_seq < previous_seq {
        anyhow::bail!("next_seq is below the highest persisted delivery sequence");
    }
    if next_seq > MAX_SAFE_SEQUENCE {
        anyhow::bail!("next_seq exceeds the JSON exact-integer range");
    }
    Ok(())
}

fn validate_pending_requests(requests: &[PendingRequest]) -> Result<()> {
    if requests.len() > 200 {
        anyhow::bail!("pending request ledger full");
    }
    if requests
        .iter()
        .any(|request| !is_valid_lane_id(&request.lane_id))
    {
        anyhow::bail!("pending request contains an invalid lane id");
    }
    if requests
        .iter()
        .any(|request| !is_valid_opaque_id(&request.message_id))
    {
        anyhow::bail!("pending request contains an invalid message id");
    }
    Ok(())
}

fn validate_delivery_snapshot_version(version: u32) -> Result<()> {
    if version != DELIVERY_SNAPSHOT_VERSION {
        anyhow::bail!(
            "delivery snapshot version {version} is unsupported; expected {DELIVERY_SNAPSHOT_VERSION}"
        );
    }
    Ok(())
}

/// Serialized size is the durable footprint of the idempotency ledger. Count
/// the array brackets and separators so admission and startup validation use
/// the same representation as the compact JSON snapshot.
fn serialized_outbound_result_bytes(result: &OutboundResult) -> Result<usize> {
    Ok(serde_json::to_vec(result)?.len())
}

fn outbound_result_bytes(results: &[OutboundResult]) -> Result<usize> {
    let mut bytes = 2usize; // `[]`
    for (index, result) in results.iter().enumerate() {
        let separator = usize::from(index > 0);
        let result_bytes = serialized_outbound_result_bytes(result)?;
        bytes = bytes
            .checked_add(separator)
            .and_then(|bytes| bytes.checked_add(result_bytes))
            .ok_or_else(|| anyhow::anyhow!("outbound result byte count overflow"))?;
    }
    Ok(bytes)
}

fn would_exceed_outbound_result_bytes(
    results: &[OutboundResult],
    candidate: &OutboundResult,
) -> Result<bool> {
    let current = outbound_result_bytes(results)?;
    let additional = serialized_outbound_result_bytes(candidate)?
        .checked_add(usize::from(!results.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("outbound result byte count overflow"))?;
    Ok(current
        .checked_add(additional)
        .map_or(true, |total| total > MAX_OUTBOUND_RESULT_LEDGER_BYTES))
}

fn serialized_retired_outbound_result_bytes(result: &RetiredOutboundResult) -> Result<usize> {
    Ok(serde_json::to_vec(result)?.len())
}

fn retired_outbound_result_bytes(results: &[RetiredOutboundResult]) -> Result<usize> {
    let mut bytes = 2usize; // `[]`
    for (index, result) in results.iter().enumerate() {
        let separator = usize::from(index > 0);
        let result_bytes = serialized_retired_outbound_result_bytes(result)?;
        bytes = bytes
            .checked_add(separator)
            .and_then(|bytes| bytes.checked_add(result_bytes))
            .ok_or_else(|| anyhow::anyhow!("retired outbound result byte count overflow"))?;
    }
    Ok(bytes)
}

fn would_exceed_retired_outbound_result_bytes(
    results: &[RetiredOutboundResult],
    candidate: &RetiredOutboundResult,
) -> Result<bool> {
    let current = retired_outbound_result_bytes(results)?;
    let additional = serialized_retired_outbound_result_bytes(candidate)?
        .checked_add(usize::from(!results.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("retired outbound result byte count overflow"))?;
    Ok(current
        .checked_add(additional)
        .map_or(true, |total| total > MAX_RETIRED_OUTBOUND_RESULT_BYTES))
}

fn append_retired_outbound_result(
    results: &mut Vec<RetiredOutboundResult>,
    candidate: RetiredOutboundResult,
) -> Result<()> {
    if serialized_retired_outbound_result_bytes(&candidate)?
        .checked_add(2)
        .map_or(true, |bytes| bytes > MAX_RETIRED_OUTBOUND_RESULT_BYTES)
    {
        anyhow::bail!("retired outbound result exceeds durable tombstone byte limit");
    }
    while results.len() >= MAX_RETIRED_OUTBOUND_RESULTS
        || would_exceed_retired_outbound_result_bytes(results, &candidate)?
    {
        results.remove(0);
    }
    results.push(candidate);
    Ok(())
}

fn validate_outbound_result_snapshot(
    results: &[OutboundResult],
    retired_results: &[RetiredOutboundResult],
    next_outbound_result_id: u64,
    clock_high_water_ms: u64,
    next_seq: u64,
) -> Result<()> {
    if results.len() > MAX_OUTBOUND_RESULTS {
        anyhow::bail!(DURABLE_REQUEST_LEDGER_FULL_ERROR);
    }
    if outbound_result_bytes(results)? > MAX_OUTBOUND_RESULT_LEDGER_BYTES {
        anyhow::bail!(DURABLE_REQUEST_LEDGER_BYTE_LIMIT_ERROR);
    }
    if results.iter().any(|entry| {
        entry.gateway_id.is_empty()
            || entry.gateway_id.len() > MAX_GATEWAY_ID_BYTES
            || !is_valid_opaque_id(&entry.request_id)
            || entry.result_id == 0
            || entry.result_id > next_outbound_result_id
            || entry.result_id > MAX_OUTBOUND_RESULT_ID
            || entry.last_seen_at_ms > clock_high_water_ms
            || entry
                .delivery_seq
                .is_some_and(|seq| seq == 0 || seq > next_seq)
    }) {
        anyhow::bail!("invalid outbound request-result record");
    }
    let mut gateway_counts = std::collections::HashMap::new();
    for entry in results {
        let count = gateway_counts.entry(&entry.gateway_id).or_insert(0usize);
        *count += 1;
        if *count > MAX_OUTBOUND_RESULTS_PER_GATEWAY {
            anyhow::bail!("outbound request-result records exceed per-gateway limit");
        }
    }
    if next_outbound_result_id > MAX_OUTBOUND_RESULT_ID {
        anyhow::bail!("outbound result ID exceeds JSON exact-integer range");
    }
    if results.iter().enumerate().any(|(index, entry)| {
        results[index + 1..].iter().any(|other| {
            (other.gateway_id == entry.gateway_id && other.request_id == entry.request_id)
                || other.result_id == entry.result_id
        })
    }) {
        anyhow::bail!("duplicate outbound request-result record");
    }
    if retired_results.len() > MAX_RETIRED_OUTBOUND_RESULTS {
        anyhow::bail!("retired outbound request ledger full");
    }
    if retired_outbound_result_bytes(retired_results)? > MAX_RETIRED_OUTBOUND_RESULT_BYTES {
        anyhow::bail!("retired outbound request ledger byte limit reached");
    }
    if retired_results.iter().any(|entry| {
        entry.gateway_id.is_empty()
            || entry.gateway_id.len() > MAX_GATEWAY_ID_BYTES
            || !is_valid_opaque_id(&entry.request_id)
            || entry.result_id == 0
            || entry.result_id > next_outbound_result_id
            || entry.result_id > MAX_OUTBOUND_RESULT_ID
    }) {
        anyhow::bail!("invalid retired outbound request-result record");
    }
    if retired_results.iter().enumerate().any(|(index, entry)| {
        retired_results[index + 1..].iter().any(|other| {
            (other.gateway_id == entry.gateway_id && other.result_id == entry.result_id)
                || other.result_id == entry.result_id
        })
    }) {
        anyhow::bail!("duplicate retired outbound request-result record");
    }
    if results.iter().any(|result| {
        retired_results.iter().any(|retired| {
            retired.gateway_id == result.gateway_id && retired.result_id == result.result_id
        })
    }) {
        anyhow::bail!("outbound request-result record is both active and retired");
    }
    Ok(())
}

/// Serialized size is the relevant durable footprint for inbound voice turns,
/// just as it is for pending deliveries. Count the array brackets and commas
/// so the aggregate check follows the exact compact JSON queue model.
fn serialized_inbound_message_bytes(message: &InboundMessage) -> Result<usize> {
    Ok(serde_json::to_vec(message)?.len())
}

fn build_inbound_message(
    message_id: &str,
    generation: u64,
    event: &serde_json::Value,
) -> Result<InboundMessage> {
    let frame = serde_json::to_string(&json!({
        "type": "inbound",
        "handoffVersion": INBOUND_HANDOFF_VERSION,
        "messageId": message_id,
        "generation": generation,
        "event": event,
    }))?;
    Ok(InboundMessage {
        message_id: message_id.to_string(),
        generation,
        frame,
    })
}

/// Return the exact compact JSON size used by inbound admission for one
/// durable record. Callers use this for read-only preflight; admission calls
/// the same builder and validator immediately before queue mutation.
pub(crate) fn inbound_record_size(
    message_id: &str,
    generation: u64,
    event: &serde_json::Value,
) -> Result<usize> {
    serialized_inbound_message_bytes(&build_inbound_message(message_id, generation, event)?)
}

fn validate_inbound_record_size(message: &InboundMessage) -> Result<()> {
    let actual_bytes = serialized_inbound_message_bytes(message)?;
    if actual_bytes > MAX_INBOUND_RECORD_BYTES {
        return Err(InboundRecordTooLarge {
            actual_bytes,
            limit_bytes: MAX_INBOUND_RECORD_BYTES,
        }
        .into());
    }
    Ok(())
}

fn serialized_inbound_tombstone_bytes(tombstone: &InboundAckTombstone) -> Result<usize> {
    Ok(serde_json::to_vec(tombstone)?.len())
}

fn inbound_message_bytes(messages: &[InboundMessage]) -> Result<usize> {
    let mut bytes = 2usize; // `[]`
    for (index, message) in messages.iter().enumerate() {
        let separator = usize::from(index > 0);
        let message_bytes = serialized_inbound_message_bytes(message)?;
        bytes = bytes
            .checked_add(separator)
            .and_then(|bytes| bytes.checked_add(message_bytes))
            .ok_or_else(|| anyhow::anyhow!("inbound message byte count overflow"))?;
    }
    Ok(bytes)
}

fn inbound_tombstone_bytes(tombstones: &[InboundAckTombstone]) -> Result<usize> {
    let mut bytes = 2usize;
    for (index, tombstone) in tombstones.iter().enumerate() {
        bytes = bytes
            .checked_add(usize::from(index > 0))
            .and_then(|bytes| {
                bytes.checked_add(serialized_inbound_tombstone_bytes(tombstone).ok()?)
            })
            .ok_or_else(|| anyhow::anyhow!("inbound tombstone byte count overflow"))?;
    }
    Ok(bytes)
}

fn inbound_identity(frame: &str) -> Option<(String, u64)> {
    let value: serde_json::Value = serde_json::from_str(frame).ok()?;
    if value["type"].as_str() != Some("inbound")
        || value["handoffVersion"].as_u64() != Some(INBOUND_HANDOFF_VERSION as u64)
        || value["event"]["message_id"].as_str() != value["messageId"].as_str()
    {
        return None;
    }
    Some((
        value["messageId"].as_str()?.to_string(),
        value["generation"].as_u64()?,
    ))
}

fn validate_inbound_event_lane_id(event: &serde_json::Value) -> Result<()> {
    let Some(value) = event.get("source").and_then(|source| source.get("chat_id")) else {
        return Ok(());
    };
    let lane_id = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("inbound event lane id must be a string"))?;
    if !is_valid_lane_id(lane_id) {
        anyhow::bail!("inbound event contains an invalid lane id");
    }
    Ok(())
}

fn validate_inbound_snapshot(snapshot: &InboundSnapshot) -> Result<()> {
    if snapshot.version != INBOUND_SNAPSHOT_VERSION {
        anyhow::bail!(
            "inbound snapshot version {} is unsupported; expected {}",
            snapshot.version,
            INBOUND_SNAPSHOT_VERSION
        );
    }
    if snapshot.next_generation > MAX_SAFE_SEQUENCE {
        anyhow::bail!("inbound generation exceeds JSON exact-integer range");
    }
    if snapshot.messages.len() > MAX_PENDING_DELIVERIES {
        return Err(anyhow::anyhow!("inbound relay queue is full"));
    }
    if inbound_message_bytes(&snapshot.messages)? > MAX_INBOUND_QUEUE_BYTES {
        return Err(anyhow::anyhow!(INBOUND_QUEUE_BYTE_LIMIT_ERROR));
    }
    if snapshot.acknowledged.len() > MAX_INBOUND_TOMBSTONES {
        anyhow::bail!("inbound acknowledgement ledger full");
    }
    if inbound_tombstone_bytes(&snapshot.acknowledged)? > MAX_INBOUND_TOMBSTONE_BYTES {
        anyhow::bail!("inbound acknowledgement ledger byte limit reached");
    }
    let mut identities = HashSet::new();
    let mut active_message_ids = HashSet::new();
    for message in &snapshot.messages {
        if !is_valid_opaque_id(&message.message_id)
            || message.generation == 0
            || message.generation > snapshot.next_generation
            || !active_message_ids.insert(message.message_id.clone())
            || !identities.insert((message.message_id.clone(), message.generation))
        {
            anyhow::bail!("invalid inbound message identity");
        }
        let frame = serde_json::from_str::<serde_json::Value>(&message.frame)
            .map_err(|error| anyhow::anyhow!("invalid inbound frame JSON: {error}"))?;
        validate_inbound_event_lane_id(&frame["event"])?;
        if inbound_identity(&message.frame).as_ref()
            != Some(&(message.message_id.clone(), message.generation))
        {
            anyhow::bail!("inbound frame identity does not match durable record");
        }
        validate_inbound_record_size(message)?;
    }
    for tombstone in &snapshot.acknowledged {
        if !is_valid_opaque_id(&tombstone.message_id)
            || tombstone.generation == 0
            || tombstone.generation > snapshot.next_generation
            || !identities.insert((tombstone.message_id.clone(), tombstone.generation))
        {
            anyhow::bail!("invalid inbound acknowledgement tombstone");
        }
    }
    Ok(())
}

fn validate_inbound_enqueue(queue: &[InboundMessage], candidate: &InboundMessage) -> Result<()> {
    if queue.len() >= MAX_PENDING_DELIVERIES {
        return Err(anyhow::anyhow!("inbound relay queue is full"));
    }
    validate_inbound_record_size(candidate)?;
    let current = inbound_message_bytes(queue)?;
    let additional = serialized_inbound_message_bytes(candidate)?
        .checked_add(usize::from(!queue.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("inbound message byte count overflow"))?;
    if current
        .checked_add(additional)
        .map_or(true, |total| total > MAX_INBOUND_QUEUE_BYTES)
    {
        return Err(anyhow::anyhow!(INBOUND_QUEUE_BYTE_LIMIT_ERROR));
    }
    Ok(())
}

fn append_inbound_tombstone(
    tombstones: &mut Vec<InboundAckTombstone>,
    candidate: InboundAckTombstone,
) -> Result<()> {
    if serialized_inbound_tombstone_bytes(&candidate)?
        .checked_add(2)
        .map_or(true, |bytes| bytes > MAX_INBOUND_TOMBSTONE_BYTES)
    {
        anyhow::bail!("inbound acknowledgement tombstone exceeds byte limit");
    }
    while tombstones.len() >= MAX_INBOUND_TOMBSTONES
        || inbound_tombstone_bytes(tombstones)?
            .checked_add(serialized_inbound_tombstone_bytes(&candidate)?)
            .and_then(|bytes| bytes.checked_add(usize::from(!tombstones.is_empty())))
            .map_or(true, |bytes| bytes > MAX_INBOUND_TOMBSTONE_BYTES)
    {
        tombstones.remove(0);
    }
    tombstones.push(candidate);
    Ok(())
}

impl RelayState {
    fn enqueue_delivery(
        &self,
        chat_id: &str,
        content: &str,
        reply_to: Option<String>,
    ) -> Result<u64> {
        if !is_valid_lane_id(chat_id) {
            anyhow::bail!("lane_id must match the lane ID grammar");
        }
        validate_delivery_content(content)?;
        validate_reply_to(reply_to.as_deref())?;
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        if self.deliveries.lock().unwrap().len() >= MAX_PENDING_DELIVERIES {
            return Err(DurableQueueFull.into());
        }
        let previous_next_seq = *self.next_seq.lock().unwrap();
        let n = previous_next_seq
            .checked_add(1)
            .filter(|seq| *seq <= MAX_SAFE_SEQUENCE)
            .ok_or_else(|| anyhow::anyhow!(DELIVERY_SEQUENCE_EXHAUSTED_ERROR))?;
        let candidate = Delivery {
            seq: n,
            chat_id: chat_id.to_string(),
            content: content.to_string(),
            reply_to,
            arrived_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let previous = {
            let mut q = self.deliveries.lock().unwrap();
            if would_exceed_pending_delivery_bytes(&q, &candidate)? {
                return Err(anyhow::anyhow!(DURABLE_QUEUE_BYTE_LIMIT_ERROR));
            }
            let previous = q.clone();
            q.push(candidate);
            previous
        };
        *self.next_seq.lock().unwrap() = n;
        if let Err(error) = self.persist() {
            if self.can_roll_back_persistence() {
                *self.deliveries.lock().unwrap() = previous;
                *self.next_seq.lock().unwrap() = previous_next_seq;
            }
            return Err(error);
        }
        Ok(n)
    }

    pub fn queue_delivery(&self, chat_id: &str, content: &str) -> Result<u64> {
        self.enqueue_delivery(chat_id, content, None)
    }

    /// Track an inbound message for explicit reply_to correlation and cleanup.
    /// A delivery without reply_to remains intentionally generic; correlation
    /// is never inferred from the most recent voice turn.
    pub fn register_request(&self, lane_id: &str, message_id: &str) -> Result<()> {
        if !is_valid_lane_id(lane_id) {
            anyhow::bail!("pending request contains an invalid lane id");
        }
        if !is_valid_opaque_id(message_id) {
            anyhow::bail!("pending request contains an invalid message id");
        }
        {
            let mut requests = self.pending_requests.lock().unwrap();
            requests.push(PendingRequest {
                lane_id: lane_id.to_string(),
                message_id: message_id.to_string(),
            });
            if requests.len() > 200 {
                let excess = requests.len() - 200;
                requests.drain(0..excess);
            }
        }
        self.persist_requests();
        Ok(())
    }

    /// Read-only admission preflight for the one-record serialized bound.
    /// Request callers invoke this before allocating their message sequence;
    /// the enqueue path repeats the same validation while holding its
    /// mutation lock to cover concurrent writers.
    pub(crate) fn preflight_inbound(&self, event: &serde_json::Value) -> Result<()> {
        let message_id = event["message_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("inbound event missing message_id"))?;
        validate_inbound_event_lane_id(event)?;
        if !is_valid_opaque_id(message_id) {
            anyhow::bail!("inbound message_id is empty or exceeds maximum length");
        }

        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        if let Some(existing) = self
            .inbound
            .lock()
            .unwrap()
            .iter()
            .find(|item| item.message_id == message_id)
            .cloned()
        {
            let existing_event = serde_json::from_str::<serde_json::Value>(&existing.frame)
                .ok()
                .and_then(|frame| frame.get("event").cloned());
            if existing_event.as_ref() != Some(event) {
                anyhow::bail!("message_id is already queued with different event content");
            }
            return Ok(());
        }

        let generation = self
            .next_inbound_generation
            .lock()
            .unwrap()
            .checked_add(1)
            .filter(|generation| *generation <= MAX_SAFE_SEQUENCE)
            .ok_or_else(|| anyhow::anyhow!("inbound generation limit reached"))?;
        let candidate = build_inbound_message(message_id, generation, event)?;
        validate_inbound_record_size(&candidate)
    }

    fn enqueue_inbound(
        &self,
        message_id: &str,
        event: &serde_json::Value,
    ) -> Result<(InboundMessage, bool)> {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        validate_inbound_event_lane_id(event)?;
        if !is_valid_opaque_id(message_id) {
            anyhow::bail!("inbound message_id is empty or exceeds maximum length");
        }
        if let Some(existing) = self
            .inbound
            .lock()
            .unwrap()
            .iter()
            .find(|item| item.message_id == message_id)
            .cloned()
        {
            let existing_event = serde_json::from_str::<serde_json::Value>(&existing.frame)
                .ok()
                .and_then(|frame| frame.get("event").cloned());
            if existing_event.as_ref() != Some(event) {
                anyhow::bail!("message_id is already queued with different event content");
            }
            return Ok((existing, false));
        }
        let previous = {
            let mut queue = self.inbound.lock().unwrap();
            let previous_generation = *self.next_inbound_generation.lock().unwrap();
            let generation = previous_generation
                .checked_add(1)
                .filter(|generation| *generation <= MAX_SAFE_SEQUENCE)
                .ok_or_else(|| anyhow::anyhow!("inbound generation limit reached"))?;
            let candidate = build_inbound_message(message_id, generation, event)?;
            validate_inbound_enqueue(&queue, &candidate)?;
            let previous = queue.clone();
            queue.push(candidate);
            *self.next_inbound_generation.lock().unwrap() = generation;
            (
                previous,
                previous_generation,
                queue.last().cloned().unwrap(),
            )
        };
        if let Err(error) = self.persist_inbound() {
            if self.can_roll_back_persistence() {
                *self.inbound.lock().unwrap() = previous.0;
                *self.next_inbound_generation.lock().unwrap() = previous.1;
            }
            return Err(error);
        }
        Ok((previous.2, true))
    }

    fn pending_inbound(&self) -> Vec<InboundMessage> {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.inbound.lock().unwrap().clone()
    }

    pub(crate) fn pending_inbound_count(&self) -> usize {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.inbound.lock().unwrap().len()
    }

    pub(crate) fn pending_request_count(&self) -> usize {
        self.pending_requests.lock().unwrap().len()
    }

    fn inbound_is_pending_identity(&self, message_id: &str, generation: u64) -> bool {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.inbound
            .lock()
            .unwrap()
            .iter()
            .any(|item| item.message_id == message_id && item.generation == generation)
    }

    fn acknowledge_inbound(&self, message_id: &str, generation: u64) -> Result<InboundAckResult> {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let identity = (message_id.to_string(), generation);
        if self
            .acknowledged_inbound
            .lock()
            .unwrap()
            .iter()
            .any(|entry| (entry.message_id.clone(), entry.generation) == identity)
        {
            return Ok(InboundAckResult::AlreadyAcknowledged);
        }
        let (previous, previous_generation, previous_tombstones, removed) = {
            let mut queue = self.inbound.lock().unwrap();
            let previous = queue.clone();
            let previous_generation = *self.next_inbound_generation.lock().unwrap();
            let previous_tombstones = self.acknowledged_inbound.lock().unwrap().clone();
            let before = queue.len();
            queue.retain(|item| !(item.message_id == message_id && item.generation == generation));
            (
                previous,
                previous_generation,
                previous_tombstones,
                before != queue.len(),
            )
        };
        if !removed {
            return Err(anyhow::anyhow!(INBOUND_ACK_MISMATCH_ERROR));
        }
        append_inbound_tombstone(
            &mut self.acknowledged_inbound.lock().unwrap(),
            InboundAckTombstone {
                message_id: message_id.to_string(),
                generation,
            },
        )?;
        if let Err(error) = self.persist_inbound() {
            if self.can_roll_back_persistence() {
                *self.inbound.lock().unwrap() = previous;
                *self.next_inbound_generation.lock().unwrap() = previous_generation;
                *self.acknowledged_inbound.lock().unwrap() = previous_tombstones;
            }
            return Err(error);
        }
        Ok(InboundAckResult::Acknowledged)
    }

    fn take_specific_request(&self, lane_id: &str, message_id: &str) -> Option<String> {
        let mut requests = self.pending_requests.lock().unwrap();
        let index = requests
            .iter()
            .position(|request| request.lane_id == lane_id && request.message_id == message_id)?;
        let removed = requests.remove(index).message_id;
        drop(requests);
        self.persist_requests();
        Some(removed)
    }

    pub fn queue_gateway_delivery(
        &self,
        chat_id: &str,
        content: &str,
        explicit_reply_to: Option<&str>,
    ) -> Result<u64> {
        validate_reply_to(explicit_reply_to)?;
        // A gateway send without an explicit reply_to may be a cron/update
        // message. Never guess that it answers the current voice turn.
        let reply_to = explicit_reply_to.map(str::to_string);
        let seq = self.enqueue_delivery(chat_id, content, reply_to)?;
        if let Some(message_id) = explicit_reply_to {
            let _ = self.take_specific_request(chat_id, message_id);
        }
        Ok(seq)
    }

    fn outbound_result_for_request(
        &self,
        gateway_id: &str,
        request_id: &str,
    ) -> Option<OutboundResult> {
        self.outbound_results
            .lock()
            .unwrap()
            .iter()
            .find(|entry| entry.gateway_id == gateway_id && entry.request_id == request_id)
            .cloned()
    }

    fn live_outbound_result_for_request(
        &self,
        gateway_id: &str,
        request_id: &str,
    ) -> Option<OutboundResult> {
        let active_gateway_id = self.active_gateway_id();
        let now_ms = self.effective_clock_millis();
        self.outbound_results
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.gateway_id == gateway_id
                    && entry.request_id == request_id
                    && (active_gateway_id.as_deref() == Some(gateway_id)
                        || now_ms.saturating_sub(entry.last_seen_at_ms)
                            < OUTBOUND_RESULT_RETENTION_MS)
            })
            .cloned()
    }

    /// Abandon only records whose gateway is disconnected and whose durable
    /// last-seen timestamp is outside the retry window. The exact old pair is
    /// retained as a bounded tombstone, so a late acknowledgement cannot
    /// retire a newly reused request ID. Delivery rows are deliberately left
    /// untouched: their `delivery_seq` remains a historical reference and a
    /// post-window retry is allowed to create a new delivery.
    fn reclaim_expired_outbound_results(
        &self,
        now_ms: u64,
        protected_gateway_id: Option<&str>,
    ) -> Result<bool> {
        let mut results = self.outbound_results.lock().unwrap();
        let mut retired_results = self.retired_outbound_results.lock().unwrap();
        let mut retained = Vec::with_capacity(results.len());
        let mut reclaimed = false;
        for result in results.drain(..) {
            let protected = protected_gateway_id == Some(result.gateway_id.as_str());
            let expired = !protected
                && now_ms.saturating_sub(result.last_seen_at_ms) >= OUTBOUND_RESULT_RETENTION_MS;
            if expired {
                append_retired_outbound_result(
                    &mut retired_results,
                    RetiredOutboundResult {
                        gateway_id: result.gateway_id,
                        request_id: result.request_id,
                        result_id: result.result_id,
                    },
                )?;
                reclaimed = true;
            } else {
                retained.push(result);
            }
        }
        *results = retained;
        Ok(reclaimed)
    }

    /// Durably process one gateway `send` action. The request ID and its
    /// result are committed in the same snapshot as the delivery, before the
    /// caller writes that result to the socket. A retry after a lost socket
    /// write therefore observes the original result and cannot enqueue a
    /// second delivery.
    fn queue_gateway_delivery_for_request(
        &self,
        gateway_id: &str,
        request_id: &str,
        chat_id: &str,
        content: &str,
        explicit_reply_to: Option<&str>,
    ) -> Result<GatewayActionResult> {
        let active_gateway_id = self.active_gateway_id();
        self.queue_gateway_delivery_for_request_with_protection(
            gateway_id,
            request_id,
            chat_id,
            content,
            explicit_reply_to,
            active_gateway_id.as_deref(),
        )
    }

    fn queue_gateway_delivery_for_request_with_protection(
        &self,
        gateway_id: &str,
        request_id: &str,
        chat_id: &str,
        content: &str,
        explicit_reply_to: Option<&str>,
        protected_gateway_id: Option<&str>,
    ) -> Result<GatewayActionResult> {
        validate_delivery_content(content)?;
        validate_reply_to(explicit_reply_to)?;
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let previous_clock_high_water_ms = *self.clock_high_water_ms.lock().unwrap();
        let now_ms = self.effective_clock_millis();
        let pre_reclaim_results = self.outbound_results.lock().unwrap().clone();
        let pre_reclaim_retired_results = self.retired_outbound_results.lock().unwrap().clone();
        let reclaimed = self.reclaim_expired_outbound_results(now_ms, protected_gateway_id)?;
        if reclaimed {
            if let Err(error) = self.persist() {
                if self.can_roll_back_persistence() {
                    *self.outbound_results.lock().unwrap() = pre_reclaim_results;
                    *self.retired_outbound_results.lock().unwrap() = pre_reclaim_retired_results;
                    *self.clock_high_water_ms.lock().unwrap() = previous_clock_high_water_ms;
                }
                return Err(error);
            }
        }
        // Reclamation is now durable. Any later admission failure must roll
        // back only the new action, never restore the already-committed
        // pre-reclamation state.
        let previous_results = self.outbound_results.lock().unwrap().clone();
        let previous_retired_results = self.retired_outbound_results.lock().unwrap().clone();
        let previous_clock_high_water_ms = *self.clock_high_water_ms.lock().unwrap();
        if let Some(existing) = self
            .outbound_results
            .lock()
            .unwrap()
            .iter()
            .find(|entry| entry.gateway_id == gateway_id && entry.request_id == request_id)
            .cloned()
        {
            return Ok(GatewayActionResult::durable(&existing));
        }

        let previous_deliveries;
        let previous_next_seq;
        let previous_result_id;
        {
            let mut deliveries = self.deliveries.lock().unwrap();
            let mut next_seq = self.next_seq.lock().unwrap();
            let mut results = self.outbound_results.lock().unwrap();
            let mut next_result_id = self.next_outbound_result_id.lock().unwrap();
            previous_deliveries = deliveries.clone();
            previous_next_seq = *next_seq;
            previous_result_id = *next_result_id;

            // Delivery-capacity rejections must remain transient. Recording a
            // durable negative result would make a retry with this request ID
            // fail even after the phone consumes a delivery and frees space.
            // Check every delivery admission boundary before allocating a
            // result ID or touching the request-result ledger.
            if deliveries.len() >= MAX_PENDING_DELIVERIES {
                return Ok(GatewayActionResult::transient(json!({
                    "success": false,
                    "error": DURABLE_QUEUE_FULL_ERROR,
                })));
            }
            if *next_seq >= MAX_SAFE_SEQUENCE {
                return Ok(GatewayActionResult::transient(json!({
                    "success": false,
                    "error": DELIVERY_SEQUENCE_EXHAUSTED_ERROR,
                })));
            }
            let seq = next_seq
                .checked_add(1)
                .filter(|seq| *seq <= MAX_SAFE_SEQUENCE)
                .ok_or_else(|| anyhow::anyhow!(DELIVERY_SEQUENCE_EXHAUSTED_ERROR))?;
            let delivery = Delivery {
                seq,
                chat_id: chat_id.to_string(),
                content: content.to_string(),
                reply_to: explicit_reply_to.map(str::to_string),
            };
            if would_exceed_pending_delivery_bytes(&deliveries, &delivery)? {
                return Ok(GatewayActionResult::transient(json!({
                    "success": false,
                    "error": DURABLE_QUEUE_BYTE_LIMIT_ERROR,
                })));
            }
            let gateway_result_count = results
                .iter()
                .filter(|entry| entry.gateway_id == gateway_id)
                .count();
            if results.len() >= MAX_OUTBOUND_RESULTS
                || gateway_result_count >= MAX_OUTBOUND_RESULTS_PER_GATEWAY
            {
                return Ok(GatewayActionResult::transient(json!({
                    "success": false,
                    "error": DURABLE_REQUEST_LEDGER_FULL_ERROR,
                })));
            }

            let result_id = next_result_id
                .checked_add(1)
                .filter(|result_id| *result_id <= MAX_OUTBOUND_RESULT_ID)
                .ok_or_else(|| anyhow::anyhow!("outbound result ID overflow"))?;
            let candidate = OutboundResult {
                gateway_id: gateway_id.to_string(),
                request_id: request_id.to_string(),
                result_id,
                result: json!({ "success": true }),
                delivery_seq: Some(seq),
                last_seen_at_ms: now_ms,
            };
            if would_exceed_outbound_result_bytes(&results, &candidate)? {
                return Ok(GatewayActionResult::transient(json!({
                    "success": false,
                    "error": DURABLE_REQUEST_LEDGER_BYTE_LIMIT_ERROR,
                })));
            }
            *next_seq = delivery.seq;
            deliveries.push(delivery);
            *next_result_id = result_id;
            results.push(candidate);
        }

        if let Err(error) = self.persist() {
            if self.can_roll_back_persistence() {
                *self.deliveries.lock().unwrap() = previous_deliveries;
                *self.next_seq.lock().unwrap() = previous_next_seq;
                *self.outbound_results.lock().unwrap() = previous_results;
                *self.next_outbound_result_id.lock().unwrap() = previous_result_id;
                *self.retired_outbound_results.lock().unwrap() = previous_retired_results;
                *self.clock_high_water_ms.lock().unwrap() = previous_clock_high_water_ms;
            }
            return Err(error);
        }

        if let Some(message_id) = explicit_reply_to {
            let _ = self.take_specific_request(chat_id, message_id);
        }
        let record = self
            .outbound_result_for_request(gateway_id, request_id)
            .expect("new outbound result remains present until retirement");
        Ok(GatewayActionResult::durable(&record))
    }

    /// Store a result-only action in the same durable snapshot as delivery
    /// actions. Every durable response carries a result ID that the gateway
    /// must explicitly retire after receipt.
    fn record_outbound_result(
        &self,
        gateway_id: &str,
        request_id: &str,
        result: serde_json::Value,
    ) -> Result<GatewayActionResult> {
        let active_gateway_id = self.active_gateway_id();
        self.record_outbound_result_with_protection(
            gateway_id,
            request_id,
            result,
            active_gateway_id.as_deref(),
        )
    }

    fn record_outbound_result_with_protection(
        &self,
        gateway_id: &str,
        request_id: &str,
        result: serde_json::Value,
        protected_gateway_id: Option<&str>,
    ) -> Result<GatewayActionResult> {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let previous_clock_high_water_ms = *self.clock_high_water_ms.lock().unwrap();
        let now_ms = self.effective_clock_millis();
        let pre_reclaim_results = self.outbound_results.lock().unwrap().clone();
        let pre_reclaim_retired_results = self.retired_outbound_results.lock().unwrap().clone();
        let reclaimed = self.reclaim_expired_outbound_results(now_ms, protected_gateway_id)?;
        if reclaimed {
            if let Err(error) = self.persist() {
                if self.can_roll_back_persistence() {
                    *self.outbound_results.lock().unwrap() = pre_reclaim_results;
                    *self.retired_outbound_results.lock().unwrap() = pre_reclaim_retired_results;
                    *self.clock_high_water_ms.lock().unwrap() = previous_clock_high_water_ms;
                }
                return Err(error);
            }
        }
        if let Some(existing) = self
            .outbound_results
            .lock()
            .unwrap()
            .iter()
            .find(|entry| entry.gateway_id == gateway_id && entry.request_id == request_id)
            .cloned()
        {
            return Ok(GatewayActionResult::durable(&existing));
        }
        let previous_retired_results = self.retired_outbound_results.lock().unwrap().clone();
        let previous_clock_high_water_ms = *self.clock_high_water_ms.lock().unwrap();
        let gateway_result_count = self
            .outbound_results
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.gateway_id == gateway_id)
            .count();
        if self.outbound_results.lock().unwrap().len() >= MAX_OUTBOUND_RESULTS
            || gateway_result_count >= MAX_OUTBOUND_RESULTS_PER_GATEWAY
        {
            return Ok(GatewayActionResult::transient(json!({
                "success": false,
                "error": DURABLE_REQUEST_LEDGER_FULL_ERROR,
            })));
        }

        let (previous, previous_result_id) = {
            let mut results = self.outbound_results.lock().unwrap();
            let mut next_result_id = self.next_outbound_result_id.lock().unwrap();
            let previous = results.clone();
            let previous_result_id = *next_result_id;
            let result_id = next_result_id
                .checked_add(1)
                .filter(|result_id| *result_id <= MAX_OUTBOUND_RESULT_ID)
                .ok_or_else(|| anyhow::anyhow!("outbound result ID overflow"))?;
            let candidate = OutboundResult {
                gateway_id: gateway_id.to_string(),
                request_id: request_id.to_string(),
                result_id,
                result: result.clone(),
                delivery_seq: None,
                last_seen_at_ms: now_ms,
            };
            if would_exceed_outbound_result_bytes(&results, &candidate)? {
                return Ok(GatewayActionResult::transient(json!({
                    "success": false,
                    "error": DURABLE_REQUEST_LEDGER_BYTE_LIMIT_ERROR,
                })));
            }
            *next_result_id = result_id;
            results.push(candidate);
            (previous, previous_result_id)
        };
        if let Err(error) = self.persist() {
            if self.can_roll_back_persistence() {
                *self.outbound_results.lock().unwrap() = previous;
                *self.next_outbound_result_id.lock().unwrap() = previous_result_id;
                *self.retired_outbound_results.lock().unwrap() = previous_retired_results;
                *self.clock_high_water_ms.lock().unwrap() = previous_clock_high_water_ms;
            }
            return Err(error);
        }
        let record = self
            .outbound_result_for_request(gateway_id, request_id)
            .expect("new outbound result remains present until retirement");
        Ok(GatewayActionResult::durable(&record))
    }

    /// Durably remove exactly one completed outbound action after the gateway
    /// confirms receipt of its result. A repeated acknowledgement after a
    /// successful commit is intentionally accepted: the first response may
    /// have been lost after the snapshot was synced. A delayed acknowledgement
    /// with an old result ID cannot remove a reused request ID. Tombstones
    /// retain the exact retired pair, so an absent request ID is never enough
    /// to claim that an acknowledgement succeeded.
    fn retire_outbound_result(
        &self,
        gateway_id: &str,
        request_id: &str,
        result_id: u64,
    ) -> Result<ResultRetirement> {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let (previous_results, previous_retired_results) = {
            let mut results = self.outbound_results.lock().unwrap();
            if let Some(index) = results
                .iter()
                .position(|entry| entry.gateway_id == gateway_id && entry.request_id == request_id)
            {
                if results[index].result_id != result_id {
                    anyhow::bail!(RESULT_ID_MISMATCH_ERROR);
                }
                let previous_results = results.clone();
                let mut retired_results = self.retired_outbound_results.lock().unwrap();
                let previous_retired_results = retired_results.clone();
                let retired = RetiredOutboundResult {
                    gateway_id: results[index].gateway_id.clone(),
                    request_id: results[index].request_id.clone(),
                    result_id: results[index].result_id,
                };
                results.remove(index);
                append_retired_outbound_result(&mut retired_results, retired)?;
                (previous_results, previous_retired_results)
            } else if self
                .retired_outbound_results
                .lock()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry.gateway_id == gateway_id
                        && entry.request_id == request_id
                        && entry.result_id == result_id
                })
            {
                return Ok(ResultRetirement::AlreadyRetired);
            } else {
                anyhow::bail!(RESULT_ID_MISMATCH_ERROR);
            }
        };
        if let Err(error) = self.persist() {
            if self.can_roll_back_persistence() {
                *self.outbound_results.lock().unwrap() = previous_results;
                *self.retired_outbound_results.lock().unwrap() = previous_retired_results;
            }
            return Err(error);
        }
        Ok(ResultRetirement::Retired)
    }

    /// Queue a gateway action only while this socket still owns the relay.
    /// The ownership mutex is held across the durable queue mutation and the
    /// request association, making `begin_connection` the linearization
    /// point for an outbound action and its idempotency record.
    fn queue_gateway_delivery_for_request_if_active(
        &self,
        connection_id: u64,
        gateway_id: &str,
        request_id: &str,
        chat_id: &str,
        content: &str,
        explicit_reply_to: Option<&str>,
    ) -> Result<Option<GatewayActionResult>> {
        let ownership_guard = self.connections.lock().unwrap();
        if !ownership_guard.outbound.as_ref().is_some_and(|connection| {
            connection.id == connection_id && connection.gateway_id == gateway_id
        }) {
            return Ok(None);
        }
        self.queue_gateway_delivery_for_request_with_protection(
            gateway_id,
            request_id,
            chat_id,
            content,
            explicit_reply_to,
            Some(gateway_id),
        )
        .map(Some)
    }

    fn record_outbound_result_if_active(
        &self,
        connection_id: u64,
        gateway_id: &str,
        request_id: &str,
        result: serde_json::Value,
    ) -> Result<Option<GatewayActionResult>> {
        let ownership_guard = self.connections.lock().unwrap();
        if !ownership_guard.outbound.as_ref().is_some_and(|connection| {
            connection.id == connection_id && connection.gateway_id == gateway_id
        }) {
            return Ok(None);
        }
        self.record_outbound_result_with_protection(
            gateway_id,
            request_id,
            result,
            Some(gateway_id),
        )
        .map(Some)
    }

    fn retire_outbound_result_if_active(
        &self,
        connection_id: u64,
        gateway_id: &str,
        request_id: &str,
        result_id: u64,
    ) -> Result<Option<ResultRetirement>> {
        let ownership_guard = self.connections.lock().unwrap();
        if !ownership_guard.outbound.as_ref().is_some_and(|connection| {
            connection.id == connection_id && connection.gateway_id == gateway_id
        }) {
            return Ok(None);
        }
        self.retire_outbound_result(gateway_id, request_id, result_id)
            .map(Some)
    }

    /// Deliveries after the caller's cursor. With `consume`, returned entries
    /// are removed — the caller has taken responsibility for speaking them.
    /// Items awaiting pickup for one lane, oldest first.
    pub fn pending_for(&self, lane_id: &str) -> Vec<Delivery> {
        self.deliveries
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.chat_id == lane_id)
            .cloned()
            .collect()
    }

    /// Remove exactly the lane rows the handset reports as spoken.
    ///
    /// `/api/pending` may omit durable-receipt-owned correlated rows from its
    /// narration plan. A cursor/through-sequence acknowledgement would erase
    /// those hidden rows (or unrelated rows interleaved before it), so this
    /// hard-cutover API accepts only distinct explicit delivery sequences.
    pub fn consume_lane_sequences(&self, lane_id: &str, sequences: &[u64]) -> Result<usize> {
        if sequences.is_empty() {
            anyhow::bail!("at least one delivery sequence is required");
        }
        if sequences.len() > MAX_PENDING_DELIVERIES {
            anyhow::bail!("too many delivery sequences");
        }
        let mut spoken = HashSet::with_capacity(sequences.len());
        for &sequence in sequences {
            validate_delivery_sequence_boundary("sequence", sequence)?;
            if sequence == 0 {
                anyhow::bail!("delivery sequence must be non-zero");
            }
            if !spoken.insert(sequence) {
                anyhow::bail!("delivery sequences must be distinct");
            }
        }
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let (previous_deliveries, removed) = {
            let mut q = self.deliveries.lock().unwrap();
            let previous_deliveries = q.clone();
            let before = q.len();
            q.retain(|d| !(d.chat_id == lane_id && spoken.contains(&d.seq)));
            (previous_deliveries, before - q.len())
        };
        if let Err(error) = self.persist() {
            if self.can_roll_back_persistence() {
                *self.deliveries.lock().unwrap() = previous_deliveries;
            }
            return Err(error);
        }
        Ok(removed)
    }

    pub fn pending_count(&self, lane_id: &str) -> usize {
        self.deliveries
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.chat_id == lane_id)
            .count()
    }

    /// Return the durable delivery sequence high-water mark without copying
    /// any queued delivery.  Callers use this as a pre-submit cursor: a reply
    /// allocated after this value is necessarily newer than the request.
    ///
    /// The mutation lock is required even though this is read-only. A queue
    /// writer assigns `next_seq` before persisting and can roll it back on a
    /// definite persistence failure; exposing that transient value could make
    /// a caller skip a later, successfully persisted reply.
    pub fn delivery_head(&self) -> Result<u64> {
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let latest = *self.next_seq.lock().unwrap();
        validate_delivery_sequence_boundary("latest", latest)?;
        Ok(latest)
    }

    pub fn deliveries_after(
        &self,
        after: u64,
        consume: bool,
        lane_id: Option<&str>,
        reply_to: Option<&str>,
        through_seq: Option<u64>,
    ) -> Result<(Vec<Delivery>, u64)> {
        validate_delivery_sequence_boundary("after", after)?;
        validate_reply_to(reply_to)?;
        if let Some(through_seq) = through_seq {
            validate_delivery_sequence_boundary("through_seq", through_seq)?;
        }
        let _queue_guard = self.queue_mutation_lock.lock().unwrap();
        self.ensure_persistence_healthy()?;
        let (picked, latest, previous_deliveries) = {
            let mut q = self.deliveries.lock().unwrap();
            let belongs_to_lane = |d: &Delivery| {
                lane_id.map_or(true, |id| d.chat_id == id)
                    && reply_to.map_or(true, |id| d.reply_to.as_deref() == Some(id))
                    && through_seq.map_or(true, |through| d.seq <= through)
            };
            let picked: Vec<Delivery> = q
                .iter()
                .filter(|d| d.seq > after && belongs_to_lane(d))
                .cloned()
                .collect();
            let latest = q.last().map(|d| d.seq).unwrap_or(after);
            let previous_deliveries = if consume && !picked.is_empty() {
                let previous = q.clone();
                q.retain(|d| !(d.seq > after && belongs_to_lane(d)));
                Some(previous)
            } else {
                None
            };
            (picked, latest, previous_deliveries)
        };
        if let Some(previous_deliveries) = previous_deliveries {
            if let Err(error) = self.persist() {
                if self.can_roll_back_persistence() {
                    *self.deliveries.lock().unwrap() = previous_deliveries;
                }
                return Err(error);
            }
        }
        Ok((picked, latest))
    }

    /// Push an inbound user message to the connected gateway. Awaits channel
    /// capacity: a slow gateway slows the phone down rather than growing memory
    /// without bound. Errors when no gateway is dialed in.
    async fn push_inbound_inner(
        &self,
        event: &serde_json::Value,
        request_lane: Option<&str>,
    ) -> Result<()> {
        let message_id = event["message_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("inbound event missing message_id"))?
            .to_string();
        if let Some(lane_id) = request_lane {
            if !is_valid_lane_id(lane_id) {
                anyhow::bail!("pending request contains an invalid lane id");
            }
        }
        // This is deliberately before connection lookup and request
        // registration. A permanent size rejection must not be turned into a
        // retryable 503 or leave a sidecar request row behind.
        self.preflight_inbound(event)?;
        let tx = {
            let guard = self.connections.lock().unwrap();
            guard
                .outbound
                .as_ref()
                .map(|connection| connection.tx.clone())
                .ok_or_else(|| anyhow::anyhow!("no gateway connected"))?
        };
        let (pending, newly_enqueued) = self.enqueue_inbound(&message_id, event)?;
        if !newly_enqueued {
            if let Some(lane_id) = request_lane {
                self.register_request(lane_id, &message_id)?;
            }
            return Ok(());
        }
        if let Some(lane_id) = request_lane {
            self.register_request(lane_id, &message_id)?;
        }
        if let Err(error) = tx.send(pending.frame).await {
            // The row is already durably accepted. The socket may have been
            // superseded and replayed it before this old channel reported its
            // closure; returning 503 here would make a caller retry and could
            // deliver the same turn twice. A successor will replay the row.
            eprintln!("gateway channel closed after durable enqueue: {error}");
        }
        Ok(())
    }

    pub async fn push_inbound(&self, event: &serde_json::Value) -> Result<()> {
        self.push_inbound_inner(event, None).await
    }

    /// Push an inbound user message and register its exact reply correlation
    /// only after durable inbound admission succeeds.
    pub async fn push_inbound_with_request(
        &self,
        lane_id: &str,
        event: &serde_json::Value,
    ) -> Result<()> {
        self.push_inbound_inner(event, Some(lane_id)).await
    }
}

#[cfg(test)]
mod queue_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn masked_text_frame(payload: &[u8]) -> Vec<u8> {
        let mask = [0x12, 0x34, 0x56, 0x78];
        let mut frame = Vec::with_capacity(payload.len() + 14);
        frame.push(0x81); // FIN + text frame
        frame.push(0x80 | 127); // client masking + 64-bit payload length
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % mask.len()]),
        );
        frame
    }

    async fn read_http_headers(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;

        let mut response = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !response.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "WebSocket upgrade closed before responding");
            response.extend_from_slice(&chunk[..read]);
        }
        response
    }

    fn temp_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "telepathyd-{label}-{}-{nonce}.json",
            std::process::id()
        ))
    }

    #[test]
    fn reload_advances_sequence_before_new_delivery() {
        let path = temp_path("sequence");
        fs::write(
            &path,
            serde_json::to_string(&DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: 41,
                    chat_id: "telepathy:direct".into(),
                    content: "persisted".into(),
                    reply_to: None,
                }],
                next_seq: 41,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            })
            .unwrap(),
        )
        .unwrap();

        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        assert_eq!(state.queue_delivery("telepathy:direct", "new").unwrap(), 42);

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        assert_eq!(
            reloaded
                .queue_delivery("telepathy:direct", "after restart")
                .unwrap(),
            43
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn consumed_queue_preserves_sequence_high_water_mark() {
        let path = temp_path("high-water");
        let state = RelayState::default();
        state.set_persist_path(&path);
        let seq = state.queue_delivery("telepathy:direct", "one").unwrap();
        let (picked, _) = state
            .deliveries_after(0, true, Some("telepathy:direct"), None, None)
            .unwrap();
        assert_eq!(picked.len(), 1);

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        assert_eq!(
            reloaded.queue_delivery("telepathy:direct", "two").unwrap(),
            seq + 1
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn persistence_creates_missing_parent_directories() {
        let directory = temp_path("nested-parent");
        let path = directory.join("pending.json");
        let state = RelayState::default();
        state.set_persist_path(&path);
        state.queue_delivery("telepathy:direct", "durable").unwrap();

        assert!(path.exists());
        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        assert_eq!(
            reloaded.pending_for("telepathy:direct")[0].content,
            "durable"
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn lane_id_grammar_is_enforced_for_delivery_snapshots_and_admission() {
        let invalid_lane_ids = [
            "telepathy: direct".to_string(),
            "telepathy:repo:\u{0001}control".to_string(),
            "telepathy:repo:quote\"".to_string(),
            "telepathy:repo:backslash\\".to_string(),
            format!("telepathy:repo:{}", "a".repeat(128)),
            "telepathy:repo:é".to_string(),
        ];
        for (index, lane_id) in invalid_lane_ids.iter().enumerate() {
            let path = temp_path(&format!("invalid-delivery-lane-{index}"));
            let snapshot = DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: 1,
                    chat_id: lane_id.clone(),
                    content: "persisted".into(),
                    reply_to: None,
                }],
                next_seq: 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            };
            let original = serde_json::to_string(&snapshot).unwrap();
            fs::write(&path, &original).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                RelayState::default().set_persist_path(&path)
            }));
            let panic = result.expect_err("malformed delivery snapshot must fail startup");
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
                .unwrap_or_default();
            assert!(message.contains("invalid delivery snapshot"), "{message}");
            assert_eq!(fs::read_to_string(&path).unwrap(), original);
            let _ = fs::remove_file(path);
        }

        let path = temp_path("invalid-delivery-admission");
        let state = RelayState::default();
        state.set_persist_path(&path);
        state.queue_delivery("telepathy:direct", "stable").unwrap();
        let before = fs::read_to_string(&path).unwrap();
        for lane_id in invalid_lane_ids {
            assert!(state.queue_delivery(&lane_id, "rejected").is_err());
            assert_eq!(state.pending_count("telepathy:direct"), 1);
            assert_eq!(fs::read_to_string(&path).unwrap(), before);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn lane_id_grammar_is_enforced_for_request_and_inbound_snapshots() {
        let invalid_lane_ids = [
            "telepathy: direct".to_string(),
            "telepathy:repo:\u{0001}control".to_string(),
            "telepathy:repo:quote\"".to_string(),
            "telepathy:repo:backslash\\".to_string(),
            format!("telepathy:repo:{}", "a".repeat(128)),
            "telepathy:repo:é".to_string(),
        ];
        for (index, lane_id) in invalid_lane_ids.iter().enumerate() {
            let path = temp_path(&format!("invalid-request-lane-{index}"));
            let request_path = request_path(&path);
            let original = serde_json::to_string(&vec![PendingRequest {
                lane_id: lane_id.clone(),
                message_id: "tp-request".into(),
            }])
            .unwrap();
            fs::write(&request_path, &original).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                RelayState::default().set_persist_path(&path)
            }));
            let panic = result.expect_err("malformed request snapshot must fail startup");
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
                .unwrap_or_default();
            assert!(message.contains("invalid request snapshot"), "{message}");
            assert_eq!(fs::read_to_string(&request_path).unwrap(), original);
            let _ = fs::remove_file(&request_path);

            let inbound_path = inbound_path(&path);
            let event = json!({
                "message_id": "tp-inbound",
                "source": { "chat_id": lane_id },
            });
            let frame = json!({
                "type": "inbound",
                "handoffVersion": INBOUND_HANDOFF_VERSION,
                "messageId": "tp-inbound",
                "generation": 1,
                "event": event,
            });
            let inbound_snapshot = InboundSnapshot {
                version: INBOUND_SNAPSHOT_VERSION,
                next_generation: 1,
                messages: vec![InboundMessage {
                    message_id: "tp-inbound".into(),
                    generation: 1,
                    frame: frame.to_string(),
                }],
                acknowledged: vec![],
            };
            let inbound_original = serde_json::to_string(&inbound_snapshot).unwrap();
            fs::write(&inbound_path, &inbound_original).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                RelayState::default().set_persist_path(&path)
            }));
            let panic = result.expect_err("malformed inbound snapshot must fail startup");
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
                .unwrap_or_default();
            assert!(message.contains("invalid inbound snapshot"), "{message}");
            assert_eq!(fs::read_to_string(&inbound_path).unwrap(), inbound_original);

            let _ = fs::remove_file(path);
            let _ = fs::remove_file(request_path);
            let _ = fs::remove_file(inbound_path);
        }

        let invalid_message_ids = [
            "".to_string(),
            " \t\n".to_string(),
            "message\u{0000}control".to_string(),
            "é".repeat(MAX_OPAQUE_ID_BYTES / 2 + 1),
            "🦀".repeat(MAX_OPAQUE_ID_LENGTH / 2 + 1),
        ];
        for (index, message_id) in invalid_message_ids.iter().enumerate() {
            let path = temp_path(&format!("invalid-request-message-{index}"));
            let request_path = request_path(&path);
            let original = serde_json::to_string(&vec![PendingRequest {
                lane_id: "telepathy:direct".into(),
                message_id: message_id.clone(),
            }])
            .unwrap();
            fs::write(&request_path, &original).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                RelayState::default().set_persist_path(&path)
            }));
            let panic = result.expect_err("malformed request ID snapshot must fail startup");
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
                .unwrap_or_default();
            assert!(message.contains("invalid request snapshot"), "{message}");
            assert_eq!(fs::read_to_string(&request_path).unwrap(), original);
            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(&request_path);
        }

        let state = RelayState::default();
        assert!(state
            .register_request("telepathy: direct", "tp-invalid-request")
            .is_err());
        for message_id in ["", " \t\n", "message\u{0000}control"] {
            assert!(state
                .register_request("telepathy:direct", message_id)
                .is_err());
        }
        assert!(state.pending_requests.lock().unwrap().is_empty());
        let error = state
            .enqueue_inbound(
                "tp-new-inbound",
                &json!({
                    "message_id": "tp-new-inbound",
                    "source": { "chat_id": "telepathy: direct" },
                }),
            )
            .unwrap_err();
        assert!(error.to_string().contains("invalid lane id"));
        assert!(state.pending_inbound().is_empty());
    }

    #[test]
    fn atomic_snapshot_write_syncs_each_new_nested_directory_parent() {
        let root = temp_path("nested-parent-sync");
        let path = root.join("one").join("two").join("pending.json");
        reset_new_directory_parent_sync_hook(None);

        atomic_write(&path, "durable relay snapshot").unwrap();

        assert_eq!(
            new_directory_parent_sync_calls(),
            3,
            "root, one, and two are all new directories"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "durable relay snapshot");
        reset_new_directory_parent_sync_hook(None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_nested_directory_parent_sync_failure_is_definite_pre_rename() {
        let root = temp_path("nested-parent-sync-failure");
        let path = root.join("one").join("two").join("pending.json");
        reset_new_directory_parent_sync_hook(Some(2));

        let error = atomic_write(&path, "must not be written").unwrap_err();

        assert!(!error.renamed);
        assert_eq!(new_directory_parent_sync_calls(), 2);
        assert!(!path.exists());
        reset_new_directory_parent_sync_hook(None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn gateway_delivery_reports_persistence_failure_without_acknowledging() {
        let blocker = temp_path("persistence-blocker");
        fs::write(&blocker, "not a directory").unwrap();
        let path = blocker.join("pending.json");
        let state = RelayState::default();
        // Bypass startup loading here: this test targets a write failure after
        // the daemon has already accepted its configured persistence path.
        *state.persist_path.lock().unwrap() = Some(path);

        let result = state.queue_gateway_delivery("telepathy:direct", "lost", None);
        assert!(result.is_err());
        assert!(state.pending_for("telepathy:direct").is_empty());
        let _ = fs::remove_file(blocker);
    }

    #[test]
    fn post_rename_directory_sync_failure_poison_prevents_snapshot_rollback_or_overwrite() {
        let path = temp_path("post-rename-directory-sync");
        let state = RelayState::default();
        state.set_persist_path(&path);

        state
            .queue_gateway_delivery_for_request(
                DEV_GATEWAY_ID,
                "request-before-failure",
                "telepathy:direct",
                "first durable reply",
                None,
            )
            .unwrap();

        fail_next_post_rename_directory_sync();
        let error = state
            .queue_gateway_delivery_for_request(
                DEV_GATEWAY_ID,
                "request-after-rename",
                "telepathy:direct",
                "must remain in the committed snapshot",
                None,
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("injected post-rename directory sync failure"));

        // The rename already made this state visible at `path`; restoring the
        // old in-memory snapshot would let a later write erase it. Keep the
        // new state and reject every subsequent durable mutation instead.
        assert_eq!(state.pending_count("telepathy:direct"), 2);
        assert_eq!(state.outbound_results.lock().unwrap().len(), 2);
        assert!(state
            .queue_gateway_delivery_for_request(
                DEV_GATEWAY_ID,
                "request-after-poison",
                "telepathy:direct",
                "must not overwrite the ambiguous commit",
                None,
            )
            .is_err());

        let snapshot: DeliverySnapshot =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(snapshot.deliveries.len(), 2);
        assert_eq!(snapshot.outbound_results.len(), 2);
        assert_eq!(
            snapshot.deliveries[1].content,
            "must remain in the committed snapshot"
        );

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        assert_eq!(reloaded.pending_count("telepathy:direct"), 2);
        assert_eq!(reloaded.outbound_results.lock().unwrap().len(), 2);
        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn websocket_upgrade_rejects_frame_over_ndjson_ceiling() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let state = Arc::new(RelayState::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router(server_state, vec![]))
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                b"GET / HTTP/1.1\r\n\
Host: localhost\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\r\n",
            )
            .await
            .unwrap();
        let response = read_http_headers(&mut client).await;
        assert!(response.starts_with(b"HTTP/1.1 101"));

        let frame = masked_text_frame(&vec![b'x'; MAX_INBOUND_LINE_BYTES + 1]);
        let write_result = client.write_all(&frame).await;
        if let Err(error) = write_result {
            // The frame-size check happens after the header, so the server may
            // close before the client has written every payload byte.
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ));
        } else if let Err(error) = client.flush().await {
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ));
        } else {
            let mut close = [0_u8; 16];
            match tokio::time::timeout(Duration::from_secs(1), client.read(&mut close))
                .await
                .expect("oversized frame was not rejected")
            {
                Ok(0) => {}
                Ok(read) => assert_eq!(close[0] & 0x0f, 0x08, "unexpected {read}-byte frame"),
                // Hyper is permitted to reset instead of sending a WebSocket close
                // frame when tungstenite rejects a frame during handshake input.
                Err(error) => assert!(matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                )),
            }
        }
        assert_eq!(state.connections.lock().unwrap().latest_id, 0);

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn old_delivery_array_refuses_startup_without_overwriting_it() {
        let path = temp_path("old-format");
        let old = vec![Delivery {
            seq: 7,
            chat_id: "telepathy:direct".into(),
            content: "legacy".into(),
            reply_to: None,
        }];
        fs::write(&path, serde_json::to_string(&old).unwrap()).unwrap();

        let state = RelayState::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.set_persist_path(&path);
        }));
        assert!(result.is_err());
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            serde_json::to_string(&old).unwrap()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn snapshot_without_outbound_request_ledger_refuses_startup_without_overwriting_it() {
        let path = temp_path("missing-outbound-ledger");
        let old = json!({ "deliveries": [], "next_seq": 0 }).to_string();
        fs::write(&path, &old).unwrap();

        let state = RelayState::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.set_persist_path(&path);
        }));
        assert!(result.is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), old);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn relative_delivery_path_can_be_persisted_and_reloaded() {
        let path = PathBuf::from(format!(
            ".telepathyd-relative-pending-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let state = RelayState::default();
        state.set_persist_path(&path);
        state
            .queue_delivery("telepathy:direct", "relative")
            .unwrap();

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        assert_eq!(
            reloaded.pending_for("telepathy:direct")[0].content,
            "relative"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn inbound_outbox_survives_restart_until_socket_acknowledgement() {
        let path = temp_path("inbound-outbox");
        let event = json!({"message_id":"tp-7"});
        let state = RelayState::default();
        state.set_persist_path(&path);
        let (queued, fresh) = state.enqueue_inbound("tp-7", &event).unwrap();
        assert!(fresh);
        assert_eq!(queued.generation, 1);

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        let pending = reloaded.pending_inbound();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message_id, "tp-7");
        reloaded.acknowledge_inbound("tp-7", 1).unwrap();
        assert!(reloaded.pending_inbound().is_empty());
        let inbound = inbound_path(&path);
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(inbound);
    }

    #[test]
    fn inbound_ack_requires_exact_generation_and_is_restart_idempotent() {
        let path = temp_path("inbound-exact-ack");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        state
            .enqueue_inbound("tp-exact", &json!({"message_id":"tp-exact", "text":"one"}))
            .unwrap();

        let wrong = handle_gateway_frame(
            &state,
            r#"{"type":"inbound_ack","messageId":"tp-exact","generation":2}"#,
        )
        .unwrap();
        let wrong: serde_json::Value = serde_json::from_str(&wrong).unwrap();
        assert_eq!(wrong["result"]["success"], false);
        assert_eq!(state.pending_inbound().len(), 1);

        let exact = handle_gateway_frame(
            &state,
            r#"{"type":"inbound_ack","messageId":"tp-exact","generation":1}"#,
        )
        .unwrap();
        let exact: serde_json::Value = serde_json::from_str(&exact).unwrap();
        assert_eq!(exact["result"]["success"], true);
        assert_eq!(exact["result"]["alreadyAcknowledged"], false);
        assert!(state.pending_inbound().is_empty());

        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);
        let repeated = handle_gateway_frame(
            &reloaded,
            r#"{"type":"inbound_ack","messageId":"tp-exact","generation":1}"#,
        )
        .unwrap();
        let repeated: serde_json::Value = serde_json::from_str(&repeated).unwrap();
        assert_eq!(repeated["result"]["alreadyAcknowledged"], true);

        // Reuse is allowed only with a new durable generation. A delayed ACK
        // for generation 1 must not remove generation 2.
        reloaded
            .enqueue_inbound("tp-exact", &json!({"message_id":"tp-exact", "text":"two"}))
            .unwrap();
        assert_eq!(reloaded.pending_inbound()[0].generation, 2);
        let delayed = handle_gateway_frame(
            &reloaded,
            r#"{"type":"inbound_ack","messageId":"tp-exact","generation":1}"#,
        )
        .unwrap();
        let delayed: serde_json::Value = serde_json::from_str(&delayed).unwrap();
        assert_eq!(delayed["result"]["success"], true);
        assert_eq!(delayed["result"]["alreadyAcknowledged"], true);
        assert_eq!(reloaded.pending_inbound()[0].generation, 2);

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(inbound_path(&path));
    }

    #[test]
    fn inbound_ack_malformed_unknown_and_stale_identities_do_not_mutate_queue() {
        let state = Arc::new(RelayState::default());
        state
            .enqueue_inbound("tp-safe", &json!({"message_id":"tp-safe"}))
            .unwrap();
        for raw in [
            r#"{"type":"inbound_ack"}"#,
            r#"{"type":"inbound_ack","messageId":"tp-safe","generation":0}"#,
            r#"{"type":"inbound_ack","messageId":"other","generation":1}"#,
            r#"not-json"#,
        ] {
            if let Some(reply) = handle_gateway_frame(&state, raw) {
                let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
                assert_eq!(reply["result"]["success"], false);
            }
            assert_eq!(state.pending_inbound().len(), 1);
        }
    }

    #[test]
    fn inbound_ack_persistence_ambiguity_is_recovered_by_restart() {
        let path = temp_path("inbound-ack-post-rename");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        state
            .enqueue_inbound("tp-ambiguous", &json!({"message_id":"tp-ambiguous"}))
            .unwrap();
        fail_next_post_rename_directory_sync();
        let failed = handle_gateway_frame(
            &state,
            r#"{"type":"inbound_ack","messageId":"tp-ambiguous","generation":1}"#,
        )
        .unwrap();
        let failed: serde_json::Value = serde_json::from_str(&failed).unwrap();
        assert_eq!(failed["result"]["success"], false);

        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);
        assert!(reloaded.pending_inbound().is_empty());
        let retry = handle_gateway_frame(
            &reloaded,
            r#"{"type":"inbound_ack","messageId":"tp-ambiguous","generation":1}"#,
        )
        .unwrap();
        let retry: serde_json::Value = serde_json::from_str(&retry).unwrap();
        assert_eq!(retry["result"]["alreadyAcknowledged"], true);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(inbound_path(&path));
    }

    #[tokio::test]
    async fn duplicate_inbound_channel_copies_are_collapsed_before_ack() {
        let state = Arc::new(RelayState::default());
        let connection_id = state.begin_connection();
        let (tx, mut rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(connection_id, DEV_GATEWAY_ID.into(), tx, stop_tx));
        let event = json!({"message_id":"tp-channel"});
        state.push_inbound(&event).await.unwrap();
        state.push_inbound(&event).await.unwrap();
        assert!(rx.recv().await.is_some());
        assert!(rx.try_recv().is_err());
        assert_eq!(state.pending_inbound()[0].generation, 1);
    }

    #[tokio::test]
    async fn superseded_gateway_cannot_ack_inbound_after_successor_wins() {
        let state = Arc::new(RelayState::default());
        let first = state.begin_connection();
        let (first_tx, _first_rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (first_stop, _first_stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(first, DEV_GATEWAY_ID.into(), first_tx, first_stop));
        state
            .enqueue_inbound("tp-owner", &json!({"message_id":"tp-owner"}))
            .unwrap();
        let second = state.begin_connection();
        let stale = handle_gateway_frame_for_connection(
            &state,
            first,
            DEV_GATEWAY_ID,
            r#"{"type":"inbound_ack","messageId":"tp-owner","generation":1}"#,
        )
        .await;
        assert!(stale.is_none());
        assert_eq!(state.pending_inbound().len(), 1);

        let (second_tx, _second_rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (second_stop, _second_stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(second, DEV_GATEWAY_ID.into(), second_tx, second_stop));
        let accepted = handle_gateway_frame_for_connection(
            &state,
            second,
            DEV_GATEWAY_ID,
            r#"{"type":"inbound_ack","messageId":"tp-owner","generation":1}"#,
        )
        .await
        .unwrap();
        let accepted: serde_json::Value = serde_json::from_str(&accepted).unwrap();
        assert_eq!(accepted["result"]["success"], true);
        assert!(state.pending_inbound().is_empty());
    }

    #[test]
    fn inbound_snapshot_is_hard_cutover_versioned_and_tombstones_are_bounded() {
        let path = temp_path("inbound-snapshot-version");
        let inbound = inbound_path(&path);
        let old = serde_json::to_string(&vec![json!({"message_id":"old"})]).unwrap();
        fs::write(&inbound, &old).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            RelayState::default().set_persist_path(&path);
        }));
        assert!(result.is_err());
        assert_eq!(fs::read_to_string(&inbound).unwrap(), old);

        let mut tombstones = Vec::new();
        for generation in 1..=(MAX_INBOUND_TOMBSTONES as u64 + 1) {
            append_inbound_tombstone(
                &mut tombstones,
                InboundAckTombstone {
                    message_id: format!("tp-{generation}"),
                    generation,
                },
            )
            .unwrap();
        }
        assert_eq!(tombstones.len(), MAX_INBOUND_TOMBSTONES);
        assert_eq!(tombstones[0].generation, 2);
        assert!(inbound_tombstone_bytes(&tombstones).unwrap() <= MAX_INBOUND_TOMBSTONE_BYTES);
        assert_eq!(
            inbound_identity(
                r#"{"type":"inbound","handoffVersion":2,"messageId":"tp-parser","generation":9,"event":{"message_id":"tp-parser"}}"#
            ),
            Some(("tp-parser".into(), 9))
        );
        assert!(inbound_identity(
            r#"{"type":"inbound","handoffVersion":1,"messageId":"tp-parser","generation":9,"event":{"message_id":"tp-parser"}}"#
        )
        .is_none());
        assert!(inbound_identity(
            r#"{"type":"inbound","handoffVersion":2,"messageId":"tp-parser","generation":9,"event":{"message_id":"other"}}"#
        )
        .is_none());
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(inbound);
    }

    #[test]
    fn oversized_inbound_enqueue_is_rejected_without_mutation_or_snapshot_change() {
        let path = temp_path("inbound-byte-limit");
        let state = RelayState::default();
        state.set_persist_path(&path);
        state
            .enqueue_inbound("tp-existing", &json!({"message_id":"tp-existing"}))
            .unwrap();

        let inbound = inbound_path(&path);
        let before = fs::read(&inbound).unwrap();
        let error = state
            .enqueue_inbound(
                "tp-oversized",
                &json!({"message_id":"tp-oversized", "text":"x".repeat(MAX_INBOUND_RECORD_BYTES)}),
            )
            .unwrap_err();
        assert_eq!(error.to_string(), INBOUND_RECORD_TOO_LARGE_ERROR);
        let size_error = error.downcast_ref::<InboundRecordTooLarge>().unwrap();
        assert!(size_error.actual_bytes > size_error.limit_bytes);
        assert_eq!(state.pending_inbound().len(), 1);
        assert_eq!(fs::read(&inbound).unwrap(), before);

        // A duplicate identity is idempotent only when its event bytes match.
        let duplicate = state
            .enqueue_inbound(
                "tp-existing",
                &json!({"message_id":"tp-existing", "text":"changed"}),
            )
            .unwrap_err();
        assert!(duplicate.to_string().contains("different event content"));
        assert_eq!(state.pending_inbound().len(), 1);
        assert_eq!(fs::read(&inbound).unwrap(), before);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(inbound);
    }

    #[test]
    fn inbound_record_admission_uses_exact_envelope_and_utf8_boundaries() {
        let message_id = "tp-boundary";
        let generation = 1;
        let event_for = |text: String| json!({"message_id": message_id, "text": text});

        let mut low = 0usize;
        let mut high = MAX_INBOUND_RECORD_BYTES;
        while low < high {
            let mid = (low + high + 1) / 2;
            if inbound_record_size(message_id, generation, &event_for("x".repeat(mid))).unwrap()
                <= MAX_INBOUND_RECORD_BYTES
            {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let exact_ascii = "x".repeat(low);
        let exact_ascii_size =
            inbound_record_size(message_id, generation, &event_for(exact_ascii.clone())).unwrap();
        assert_eq!(exact_ascii_size, MAX_INBOUND_RECORD_BYTES);
        assert!(exact_ascii.len() < MAX_INBOUND_RECORD_BYTES);
        assert!(
            inbound_record_size(
                message_id,
                generation,
                &event_for(format!("{exact_ascii}x"))
            )
            .unwrap()
                > MAX_INBOUND_RECORD_BYTES
        );

        let mut multibyte_low = 0usize;
        let mut multibyte_high = MAX_INBOUND_RECORD_BYTES;
        while multibyte_low < multibyte_high {
            let mid = (multibyte_low + multibyte_high + 1) / 2;
            if inbound_record_size(message_id, generation, &event_for("🦀".repeat(mid))).unwrap()
                <= MAX_INBOUND_RECORD_BYTES
            {
                multibyte_low = mid;
            } else {
                multibyte_high = mid - 1;
            }
        }
        let multibyte = "🦀".repeat(multibyte_low);
        let multibyte_size =
            inbound_record_size(message_id, generation, &event_for(multibyte.clone())).unwrap();
        assert!(multibyte_size <= MAX_INBOUND_RECORD_BYTES);
        assert!(
            inbound_record_size(message_id, generation, &event_for(format!("{multibyte}🦀")),)
                .unwrap()
                > MAX_INBOUND_RECORD_BYTES
        );

        let accepted_path = temp_path("inbound-exact-boundary");
        let accepted = RelayState::default();
        accepted.set_persist_path(&accepted_path);
        accepted
            .enqueue_inbound(message_id, &event_for(exact_ascii))
            .unwrap();
        assert_eq!(accepted.pending_inbound_count(), 1);
        let accepted_snapshot = fs::read(inbound_path(&accepted_path)).unwrap();

        let rejected_path = temp_path("inbound-exact-boundary-rejected");
        let rejected = RelayState::default();
        rejected.set_persist_path(&rejected_path);
        let rejected_event = event_for("x".repeat(low + 1));
        let rejected_error = rejected
            .enqueue_inbound(message_id, &rejected_event)
            .unwrap_err();
        assert!(is_inbound_record_too_large(&rejected_error));
        assert_eq!(rejected.pending_inbound_count(), 0);
        assert!(!inbound_path(&rejected_path).exists());

        assert_eq!(
            fs::read(inbound_path(&accepted_path)).unwrap(),
            accepted_snapshot
        );
        let _ = fs::remove_file(&accepted_path);
        let _ = fs::remove_file(inbound_path(&accepted_path));
        let _ = fs::remove_file(&rejected_path);
        let _ = fs::remove_file(inbound_path(&rejected_path));
    }

    #[test]
    fn oversized_inbound_snapshots_refuse_restart_without_overwriting_them() {
        let path = temp_path("inbound-snapshot-byte-limit");
        let inbound = inbound_path(&path);

        let oversized_record = InboundSnapshot {
            version: INBOUND_SNAPSHOT_VERSION,
            next_generation: 1,
            messages: vec![InboundMessage {
                message_id: "tp-oversized-record".into(),
                generation: 1,
                frame: "x".repeat(MAX_INBOUND_RECORD_BYTES),
            }],
            acknowledged: vec![],
        };
        let oversized_record_json = serde_json::to_vec(&oversized_record).unwrap();
        fs::write(&inbound, &oversized_record_json).unwrap();
        let state = RelayState::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.set_persist_path(&path);
        }));
        assert!(result.is_err());
        assert!(state.pending_inbound().is_empty());
        assert_eq!(fs::read(&inbound).unwrap(), oversized_record_json);

        let record = InboundMessage {
            message_id: "tp-aggregate".into(),
            generation: 1,
            frame: "x".repeat(MAX_INBOUND_RECORD_BYTES - 256),
        };
        let record_bytes = serialized_inbound_message_bytes(&record).unwrap();
        let count = MAX_INBOUND_QUEUE_BYTES / record_bytes + 1;
        assert!(count <= MAX_PENDING_DELIVERIES);
        let oversized_aggregate: Vec<_> = (0..count)
            .map(|index| InboundMessage {
                message_id: format!("tp-aggregate-{index}"),
                generation: index as u64 + 1,
                frame: record.frame.clone(),
            })
            .collect();
        assert!(inbound_message_bytes(&oversized_aggregate).unwrap() > MAX_INBOUND_QUEUE_BYTES);
        assert!(oversized_aggregate
            .iter()
            .all(|message| serialized_inbound_message_bytes(message).unwrap()
                <= MAX_INBOUND_RECORD_BYTES));
        let oversized_aggregate_json = serde_json::to_vec(&InboundSnapshot {
            version: INBOUND_SNAPSHOT_VERSION,
            next_generation: count as u64,
            messages: oversized_aggregate,
            acknowledged: vec![],
        })
        .unwrap();
        fs::write(&inbound, &oversized_aggregate_json).unwrap();
        let state = RelayState::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.set_persist_path(&path);
        }));
        assert!(result.is_err());
        assert!(state.pending_inbound().is_empty());
        assert_eq!(fs::read(&inbound).unwrap(), oversized_aggregate_json);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(inbound);
    }

    #[test]
    fn newer_handshake_supersedes_the_active_writer() {
        let state = RelayState::default();
        let first = state.begin_connection();
        let (first_tx, _first_rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (first_stop, first_stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(first, DEV_GATEWAY_ID.to_string(), first_tx, first_stop));
        assert!(state.is_active_connection(first));

        let second = state.begin_connection();
        assert!(*first_stop_rx.borrow());
        assert!(!state.is_active_connection(first));

        // A socket that was queued behind its predecessor cannot become live
        // once a newer handshake has won the generation race.
        let (stale_tx, _stale_rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (stale_stop, _stale_stop_rx) = tokio::sync::watch::channel(false);
        assert!(!state.activate_connection(
            first,
            DEV_GATEWAY_ID.to_string(),
            stale_tx,
            stale_stop
        ));

        let (second_tx, _second_rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (second_stop, _second_stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(
            second,
            DEV_GATEWAY_ID.to_string(),
            second_tx,
            second_stop
        ));
        assert!(state.is_active_connection(second));
    }

    #[tokio::test]
    async fn superseded_connection_cannot_queue_or_acknowledge_gateway_action() {
        let state = Arc::new(RelayState::default());
        let first = state.begin_connection();
        let (first_tx, _first_rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (first_stop, _first_stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(first, DEV_GATEWAY_ID.to_string(), first_tx, first_stop));
        let _ = state.register_request("telepathy:direct", "tp-old");

        // A live connection ID is not sufficient authority if the gateway
        // identity supplied to the frame path does not match its owner.
        let owner_check =
            r#"{"type":"outbound","requestId":"req-owner-check","action":{"op":"typing"}}"#;
        assert!(
            handle_gateway_frame_for_connection(&state, first, "another-gateway", owner_check)
                .await
                .is_none()
        );
        assert!(state.outbound_results.lock().unwrap().is_empty());

        let _second = state.begin_connection();
        let raw = r#"{"type":"outbound","requestId":"req-old","action":{"op":"send","chat_id":"telepathy:direct","content":"stale","reply_to":"tp-old"}}"#;

        assert!(
            handle_gateway_frame_for_connection(&state, first, DEV_GATEWAY_ID, raw)
                .await
                .is_none()
        );
        assert!(state.pending_for("telepathy:direct").is_empty());
        assert_eq!(state.pending_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_mpsc_send_reports_durable_acceptance_for_replay() {
        let path = temp_path("inbound-channel-failure");
        let state = RelayState::default();
        state.set_persist_path(&path);

        let connection_id = state.begin_connection();
        let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        drop(rx);
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(connection_id, DEV_GATEWAY_ID.to_string(), tx, stop_tx));

        let event = json!({ "message_id": "tp-8" });
        assert!(state.push_inbound(&event).await.is_ok());
        assert_eq!(state.pending_inbound()[0].message_id, "tp-8");

        let inbound = inbound_path(&path);
        assert!(fs::read_to_string(&inbound).unwrap().contains("tp-8"));
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(inbound);
    }

    #[tokio::test]
    async fn accepted_mpsc_send_keeps_the_durable_inbound_row() {
        let path = temp_path("inbound-mpsc-accepted");
        let state = RelayState::default();
        state.set_persist_path(&path);

        let connection_id = state.begin_connection();
        let (tx, _rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(connection_id, DEV_GATEWAY_ID.to_string(), tx, stop_tx));

        let event = json!({ "message_id": "tp-9" });
        state.push_inbound(&event).await.unwrap();
        assert_eq!(state.pending_inbound()[0].message_id, "tp-9");
        let persisted = fs::read_to_string(inbound_path(&path)).unwrap();
        assert!(persisted.contains("tp-9"));
        assert!(persisted.contains("handoffVersion"));
        assert!(persisted.contains("generation"));

        let inbound = inbound_path(&path);
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(inbound);
    }

    #[test]
    fn full_delivery_queue_rejection_is_transient_and_retryable() {
        let state = Arc::new(RelayState::default());
        for index in 0..MAX_PENDING_DELIVERIES {
            let content = format!("reply-{index}");
            state
                .queue_gateway_delivery("telepathy:direct", &content, None)
                .unwrap();
        }
        let last_seq = *state.next_seq.lock().unwrap();
        let path = temp_path("delivery-count-capacity-retry");
        state.set_persist_path(&path);
        state.persist().unwrap();
        let snapshot_before_rejection = fs::read_to_string(&path).unwrap();
        let request = r#"{"type":"outbound","requestId":"req-count-limited","action":{"op":"send","chat_id":"telepathy:direct","content":"overflow"}}"#;

        // Capacity failures are neither durable results nor result-ID
        // allocations. The same request ID may be retried after a pickup.
        for _ in 0..3 {
            let reply: serde_json::Value =
                serde_json::from_str(&handle_gateway_frame(&state, request).unwrap()).unwrap();
            assert_eq!(reply["result"]["success"], false);
            assert_eq!(reply["result"]["error"], DURABLE_QUEUE_FULL_ERROR);
            assert!(reply.get("resultId").is_none());
            assert!(state.outbound_results.lock().unwrap().is_empty());
            assert_eq!(*state.next_outbound_result_id.lock().unwrap(), 0);
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                snapshot_before_rejection
            );
        }
        assert_eq!(
            state.pending_count("telepathy:direct"),
            MAX_PENDING_DELIVERIES
        );
        let pending = state.pending_for("telepathy:direct");
        assert_eq!(pending.first().unwrap().content, "reply-0");
        assert_eq!(pending.last().unwrap().content, "reply-199");
        assert_eq!(*state.next_seq.lock().unwrap(), last_seq);

        assert_eq!(
            state
                .consume_lane_sequences("telepathy:direct", &[1])
                .unwrap(),
            1
        );
        let accepted: serde_json::Value =
            serde_json::from_str(&handle_gateway_frame(&state, request).unwrap()).unwrap();
        assert_eq!(accepted["result"]["success"], true);
        assert_eq!(accepted["resultId"], 1);
        assert_eq!(
            state.pending_count("telepathy:direct"),
            MAX_PENDING_DELIVERIES
        );
        assert_eq!(*state.next_seq.lock().unwrap(), last_seq + 1);

        let snapshot: DeliverySnapshot =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(snapshot.deliveries.len(), MAX_PENDING_DELIVERIES);
        assert_eq!(snapshot.deliveries.last().unwrap().content, "overflow");
        assert_eq!(snapshot.outbound_results.len(), 1);
        assert_eq!(snapshot.outbound_results[0].request_id, "req-count-limited");
        assert_eq!(
            snapshot.outbound_results[0].delivery_seq,
            Some(last_seq + 1)
        );
        assert_eq!(snapshot.next_outbound_result_id, 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn duplicate_outbound_request_id_returns_original_result_without_second_delivery() {
        let state = Arc::new(RelayState::default());
        let first = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-duplicate","action":{"op":"send","chat_id":"telepathy:direct","content":"first"}}"#,
        )
        .unwrap();
        let retry = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-duplicate","action":{"op":"send","chat_id":"telepathy:direct","content":" \t\n"}}"#,
        )
        .unwrap();

        assert_eq!(retry, first);
        let pending = state.pending_for("telepathy:direct");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content, "first");
        assert_eq!(state.outbound_results.lock().unwrap().len(), 1);
    }

    #[test]
    fn send_without_valid_content_is_rejected_without_mutating_queue_or_ledger() {
        let path = temp_path("invalid-content");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-seed","action":{"op":"send","chat_id":"telepathy:direct","content":"seed"}}"#,
        )
        .unwrap();

        let deliveries_before = serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap();
        let next_seq_before = *state.next_seq.lock().unwrap();
        let results_before =
            serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap();
        let snapshot_before = fs::read_to_string(&path).unwrap();

        for (request_id, action) in [
            (
                "req-missing-content",
                json!({ "op": "send", "chat_id": "telepathy:direct" }),
            ),
            (
                "req-non-string-content",
                json!({ "op": "send", "chat_id": "telepathy:direct", "content": 7 }),
            ),
            (
                "req-blank-content",
                json!({ "op": "send", "chat_id": "telepathy:direct", "content": " \t\n" }),
            ),
        ] {
            let reply = handle_gateway_frame(
                &state,
                &serde_json::to_string(&json!({
                    "type": "outbound",
                    "requestId": request_id,
                    "action": action,
                }))
                .unwrap(),
            )
            .unwrap();
            let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();

            assert_eq!(reply["requestId"], request_id);
            assert_eq!(reply["result"]["success"], false);
            assert_eq!(reply["result"]["error"], CONTENT_REQUIRED_ERROR);
            assert_eq!(
                serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap(),
                deliveries_before
            );
            assert_eq!(*state.next_seq.lock().unwrap(), next_seq_before);
            assert_eq!(
                serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap(),
                results_before
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_before);
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn blank_gateway_reply_to_is_rejected_without_mutation_or_restart_failure() {
        let path = temp_path("blank-reply-to");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-seed","action":{"op":"send","chat_id":"telepathy:direct","content":"seed"}}"#,
        )
        .unwrap();

        let deliveries_before = serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap();
        let next_seq_before = *state.next_seq.lock().unwrap();
        let results_before =
            serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap();
        let snapshot_before = fs::read_to_string(&path).unwrap();

        for (index, reply_to) in [" ", "\t\n"].into_iter().enumerate() {
            let request_id = format!("req-blank-reply-to-{index}");
            let raw = serde_json::to_string(&json!({
                "type": "outbound",
                "requestId": request_id,
                "action": {
                    "op": "send",
                    "chat_id": "telepathy:direct",
                    "content": "must not queue",
                    "reply_to": reply_to,
                },
            }))
            .unwrap();
            let reply: serde_json::Value =
                serde_json::from_str(&handle_gateway_frame(&state, &raw).unwrap()).unwrap();

            assert_eq!(reply["result"]["success"], false);
            assert_eq!(reply["result"]["error"], REPLY_TO_BLANK_ERROR);
            assert_eq!(
                serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap(),
                deliveries_before
            );
            assert_eq!(*state.next_seq.lock().unwrap(), next_seq_before);
            assert_eq!(
                serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap(),
                results_before
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_before);
        }

        let direct_error = state
            .queue_gateway_delivery("telepathy:direct", "must not queue", Some(" \t"))
            .unwrap_err();
        assert_eq!(direct_error.to_string(), REPLY_TO_BLANK_ERROR);
        let request_error = state
            .queue_gateway_delivery_for_request(
                DEV_GATEWAY_ID,
                "req-direct-blank-reply-to",
                "telepathy:direct",
                "must not queue",
                Some("\n"),
            )
            .unwrap_err();
        assert_eq!(request_error.to_string(), REPLY_TO_BLANK_ERROR);
        assert_eq!(
            serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap(),
            deliveries_before
        );
        assert_eq!(*state.next_seq.lock().unwrap(), next_seq_before);
        assert_eq!(
            serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap(),
            results_before
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_before);

        let reloaded = Arc::new(RelayState::default());
        let startup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reloaded.set_persist_path(&path);
        }));
        assert!(startup.is_ok(), "blank input must not poison the snapshot");

        let valid: serde_json::Value = serde_json::from_str(
            &handle_gateway_frame(
                &reloaded,
                r#"{"type":"outbound","requestId":"req-valid-reply-to","action":{"op":"send","chat_id":"telepathy:direct","content":"valid reply","reply_to":"tp-valid"}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(valid["result"]["success"], true);

        let omitted: serde_json::Value = serde_json::from_str(
            &handle_gateway_frame(
                &reloaded,
                r#"{"type":"outbound","requestId":"req-omitted-reply-to","action":{"op":"send","chat_id":"telepathy:direct","content":"omitted reply"}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(omitted["result"]["success"], true);

        let pending = reloaded.pending_for("telepathy:direct");
        assert_eq!(
            pending
                .iter()
                .find(|delivery| delivery.content == "valid reply")
                .and_then(|delivery| delivery.reply_to.as_deref()),
            Some("tp-valid")
        );
        assert_eq!(
            pending
                .iter()
                .find(|delivery| delivery.content == "omitted reply")
                .and_then(|delivery| delivery.reply_to.as_deref()),
            None
        );

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn gateway_rejects_unknown_lanes_and_accepts_api_created_lanes() {
        let state = Arc::new(RelayState::default());
        let registry = Arc::new(tokio::sync::Mutex::new(LaneRegistry::default_direct()));
        state.set_lane_registry(registry.clone());
        let connection_id = state.begin_connection();
        let (tx, _rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (superseded, _superseded_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(
            connection_id,
            DEV_GATEWAY_ID.to_string(),
            tx,
            superseded
        ));

        let rejected = handle_gateway_frame_for_connection(
            &state,
            connection_id,
            DEV_GATEWAY_ID,
            r#"{"type":"outbound","requestId":"req-unknown-lane","action":{"op":"send","chat_id":"telepathy:repo:later","content":"reply"}}"#,
        )
        .await
        .unwrap();
        let rejected: serde_json::Value = serde_json::from_str(&rejected).unwrap();
        assert_eq!(rejected["result"]["success"], false);
        assert_eq!(
            rejected["result"]["error"],
            "unknown lane telepathy:repo:later"
        );
        assert!(state.deliveries.lock().unwrap().is_empty());
        assert!(state.outbound_results.lock().unwrap().is_empty());

        let lane_id = registry.lock().await.create("later").unwrap().id;
        let accepted = handle_gateway_frame_for_connection(
            &state,
            connection_id,
            DEV_GATEWAY_ID,
            &serde_json::to_string(&json!({
                "type": "outbound",
                "requestId": "req-dynamic-lane",
                "action": { "op": "send", "chat_id": lane_id, "content": "reply" },
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        let accepted: serde_json::Value = serde_json::from_str(&accepted).unwrap();
        assert_eq!(accepted["result"]["success"], true);
        assert_eq!(state.pending_for(&lane_id)[0].content, "reply");
    }

    #[test]
    fn delivery_byte_limit_rejection_is_transient_and_retryable() {
        let path = temp_path("delivery-byte-capacity-retry");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        let initial_delivery_count = 15;
        let content = "x".repeat(MAX_DELIVERY_CONTENT_BYTES);
        for _ in 0..initial_delivery_count {
            state.queue_delivery("telepathy:direct", &content).unwrap();
        }
        let snapshot_before_rejection = fs::read_to_string(&path).unwrap();
        let request = serde_json::to_string(&json!({
            "type": "outbound",
            "requestId": "req-byte-limited",
            "action": { "op": "send", "chat_id": "telepathy:direct", "content": content },
        }))
        .unwrap();

        for _ in 0..3 {
            let rejected: serde_json::Value =
                serde_json::from_str(&handle_gateway_frame(&state, &request).unwrap()).unwrap();
            assert_eq!(rejected["result"]["success"], false);
            assert_eq!(rejected["result"]["error"], DURABLE_QUEUE_BYTE_LIMIT_ERROR);
            assert!(rejected.get("resultId").is_none());
            assert_eq!(state.outbound_results.lock().unwrap().len(), 0);
            assert_eq!(*state.next_outbound_result_id.lock().unwrap(), 0);
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                snapshot_before_rejection
            );
        }

        assert_eq!(
            state
                .consume_lane_sequences("telepathy:direct", &[1])
                .unwrap(),
            1
        );
        let retry = handle_gateway_frame(&state, &request).unwrap();
        let retry_value: serde_json::Value = serde_json::from_str(&retry).unwrap();
        assert_eq!(retry_value["result"]["success"], true);
        assert_eq!(retry_value["resultId"], 1);
        assert_eq!(handle_gateway_frame(&state, &request).unwrap(), retry);
        assert_eq!(
            state.deliveries.lock().unwrap().len(),
            initial_delivery_count
        );
        assert_eq!(state.outbound_results.lock().unwrap().len(), 1);
        let snapshot: DeliverySnapshot =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(snapshot.deliveries.len(), initial_delivery_count);
        assert_eq!(snapshot.outbound_results.len(), 1);
        assert_eq!(snapshot.outbound_results[0].request_id, "req-byte-limited");
        assert_eq!(
            snapshot.outbound_results[0].delivery_seq,
            Some(initial_delivery_count as u64 + 1)
        );
        assert_eq!(snapshot.next_outbound_result_id, 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn delivery_content_limit_is_utf8_exact_and_rejects_without_mutation() {
        let path = temp_path("delivery-content-limit");
        let state = RelayState::default();
        state.set_persist_path(&path);

        let exact_ascii = "a".repeat(MAX_DELIVERY_CONTENT_BYTES);
        state
            .queue_delivery("telepathy:direct", &exact_ascii)
            .unwrap();
        let exact_multibyte = "🦀".repeat(MAX_DELIVERY_CONTENT_BYTES / 4);
        state
            .queue_gateway_delivery("telepathy:direct", &exact_multibyte, None)
            .unwrap();

        let snapshot_before = fs::read(&path).unwrap();
        let oversized = format!("{}x", "🦀".repeat(MAX_DELIVERY_CONTENT_BYTES / 4));
        assert_eq!(oversized.as_bytes().len(), MAX_DELIVERY_CONTENT_BYTES + 1);
        assert_eq!(
            state
                .queue_delivery("telepathy:direct", &oversized)
                .unwrap_err()
                .to_string(),
            DELIVERY_CONTENT_TOO_LARGE_ERROR
        );
        assert_eq!(
            state
                .queue_gateway_delivery("telepathy:direct", &oversized, None)
                .unwrap_err()
                .to_string(),
            DELIVERY_CONTENT_TOO_LARGE_ERROR
        );
        assert_eq!(state.pending_count("telepathy:direct"), 2);
        assert_eq!(fs::read(&path).unwrap(), snapshot_before);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn oversized_gateway_delivery_returns_stable_transient_error_without_persistence() {
        let path = temp_path("gateway-delivery-content-limit");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        state.queue_delivery("telepathy:direct", "seed").unwrap();
        let snapshot_before = fs::read(&path).unwrap();
        let oversized = "x".repeat(MAX_DELIVERY_CONTENT_BYTES + 1);
        let request = serde_json::to_string(&json!({
            "type": "outbound",
            "requestId": "req-oversized-content",
            "action": {
                "op": "send",
                "chat_id": "telepathy:direct",
                "content": oversized,
            },
        }))
        .unwrap();

        for _ in 0..2 {
            let reply: serde_json::Value =
                serde_json::from_str(&handle_gateway_frame(&state, &request).unwrap()).unwrap();
            assert_eq!(reply["result"]["success"], false);
            assert_eq!(reply["result"]["error"], DELIVERY_CONTENT_TOO_LARGE_ERROR);
            assert!(reply.get("resultId").is_none());
            assert_eq!(state.outbound_results.lock().unwrap().len(), 0);
            assert_eq!(fs::read(&path).unwrap(), snapshot_before);
        }
        assert_eq!(state.pending_count("telepathy:direct"), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn oversized_delivery_snapshot_refuses_restart_without_overwriting_it() {
        let path = temp_path("delivery-content-snapshot-limit");
        let snapshot = DeliverySnapshot {
            version: DELIVERY_SNAPSHOT_VERSION,
            clock_high_water_ms: 0,
            deliveries: vec![Delivery {
                seq: 1,
                chat_id: "telepathy:direct".into(),
                content: "x".repeat(MAX_DELIVERY_CONTENT_BYTES + 1),
                reply_to: None,
            }],
            next_seq: 1,
            outbound_results: vec![],
            next_outbound_result_id: 0,
            retired_outbound_results: vec![],
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        fs::write(&path, &bytes).unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            RelayState::default().set_persist_path(&path);
        }));
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn delivery_snapshot_over_byte_limit_refuses_startup() {
        let path = temp_path("delivery-byte-limit");
        let snapshot = DeliverySnapshot {
            version: DELIVERY_SNAPSHOT_VERSION,
            clock_high_water_ms: 0,
            deliveries: vec![Delivery {
                seq: 1,
                chat_id: "telepathy:direct".into(),
                content: "x".repeat(MAX_PENDING_DELIVERY_BYTES),
                reply_to: None,
            }],
            next_seq: 1,
            outbound_results: vec![],
            next_outbound_result_id: 0,
            retired_outbound_results: vec![],
        };
        assert!(pending_delivery_bytes(&snapshot.deliveries).unwrap() > MAX_PENDING_DELIVERY_BYTES);
        fs::write(&path, serde_json::to_string(&snapshot).unwrap()).unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            RelayState::default().set_persist_path(&path);
        }));
        assert!(result.is_err());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_delivery_snapshot_refuses_startup_without_overwriting_it() {
        let path = temp_path("invalid-delivery-fields");
        let registry = Arc::new(tokio::sync::Mutex::new(LaneRegistry::default_direct()));
        let invalid_snapshots = [
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: 1,
                    chat_id: "telepathy:missing".into(),
                    content: "reply".into(),
                    reply_to: None,
                }],
                next_seq: 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: 1,
                    chat_id: " \t\n".into(),
                    content: "reply".into(),
                    reply_to: None,
                }],
                next_seq: 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: 1,
                    chat_id: "telepathy:direct".into(),
                    content: " \t\n".into(),
                    reply_to: None,
                }],
                next_seq: 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: 1,
                    chat_id: "telepathy:direct".into(),
                    content: "reply".into(),
                    reply_to: Some(" \t\n".into()),
                }],
                next_seq: 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![
                    Delivery {
                        seq: 2,
                        chat_id: "telepathy:direct".into(),
                        content: "first".into(),
                        reply_to: None,
                    },
                    Delivery {
                        seq: 1,
                        chat_id: "telepathy:direct".into(),
                        content: "second".into(),
                        reply_to: None,
                    },
                ],
                next_seq: 2,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![Delivery {
                    seq: MAX_SAFE_SEQUENCE + 1,
                    chat_id: "telepathy:direct".into(),
                    content: "reply".into(),
                    reply_to: None,
                }],
                next_seq: MAX_SAFE_SEQUENCE + 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
            DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![],
                next_seq: MAX_SAFE_SEQUENCE + 1,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            },
        ];

        for snapshot in invalid_snapshots {
            let original = serde_json::to_string(&snapshot).unwrap();
            fs::write(&path, &original).unwrap();
            let state = RelayState::default();
            state.set_lane_registry(registry.clone());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                state.set_persist_path(&path);
            }));
            assert!(result.is_err());
            assert_eq!(fs::read_to_string(&path).unwrap(), original);
            assert!(state.deliveries.lock().unwrap().is_empty());
            assert_eq!(*state.next_seq.lock().unwrap(), 0);
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn delivery_sequence_cap_preserves_durable_and_memory_state() {
        let path = temp_path("delivery-sequence-cap");
        let state = RelayState::default();
        state.set_persist_path(&path);
        *state.next_seq.lock().unwrap() = MAX_SAFE_SEQUENCE - 1;

        assert_eq!(
            state
                .queue_delivery("telepathy:direct", "at the exact limit")
                .unwrap(),
            MAX_SAFE_SEQUENCE
        );
        let snapshot_at_limit = fs::read_to_string(&path).unwrap();
        assert!(snapshot_at_limit.contains(&MAX_SAFE_SEQUENCE.to_string()));

        let direct_error = state
            .queue_gateway_delivery("telepathy:direct", "one too many", None)
            .unwrap_err();
        assert!(direct_error
            .to_string()
            .contains(DELIVERY_SEQUENCE_EXHAUSTED_ERROR));
        assert_eq!(*state.next_seq.lock().unwrap(), MAX_SAFE_SEQUENCE);
        assert_eq!(state.pending_count("telepathy:direct"), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_at_limit);

        let request_result = state
            .queue_gateway_delivery_for_request(
                DEV_GATEWAY_ID,
                "request-at-delivery-sequence-cap",
                "telepathy:direct",
                "also one too many",
                None,
            )
            .unwrap();
        assert_eq!(request_result.result["success"], false);
        assert_eq!(
            request_result.result["error"],
            DELIVERY_SEQUENCE_EXHAUSTED_ERROR
        );
        assert_eq!(request_result.result_id, None);
        assert!(state.outbound_results.lock().unwrap().is_empty());
        assert_eq!(*state.next_seq.lock().unwrap(), MAX_SAFE_SEQUENCE);
        assert_eq!(state.pending_count("telepathy:direct"), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_at_limit);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn send_without_chat_id_is_rejected_without_mutating_queue_or_ledger() {
        let path = temp_path("missing-chat-id");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-seed","action":{"op":"send","chat_id":"telepathy:direct","content":"seed"}}"#,
        )
        .unwrap();

        let deliveries_before = serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap();
        let next_seq_before = *state.next_seq.lock().unwrap();
        let results_before =
            serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap();
        let snapshot_before = fs::read_to_string(&path).unwrap();

        for (request_id, action) in [
            (
                "req-missing-chat-id",
                json!({ "op": "send", "content": "missing" }),
            ),
            (
                "req-blank-chat-id",
                json!({ "op": "send", "chat_id": " \t\n", "content": "blank" }),
            ),
        ] {
            let reply = handle_gateway_frame(
                &state,
                &serde_json::to_string(&json!({
                    "type": "outbound",
                    "requestId": request_id,
                    "action": action,
                }))
                .unwrap(),
            )
            .unwrap();
            let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();

            assert_eq!(reply["requestId"], request_id);
            assert_eq!(reply["result"]["success"], false);
            assert_eq!(reply["result"]["error"], CHAT_ID_REQUIRED_ERROR);
            assert_eq!(
                serde_json::to_string(&*state.deliveries.lock().unwrap()).unwrap(),
                deliveries_before
            );
            assert_eq!(*state.next_seq.lock().unwrap(), next_seq_before);
            assert_eq!(
                serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap(),
                results_before
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_before);
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn oversized_outbound_request_id_is_rejected_before_ledger_insertion() {
        let path = temp_path("oversized-request-id");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);

        let boundary_id = "b".repeat(MAX_OUTBOUND_REQUEST_ID_BYTES);
        let boundary_reply = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound",
                "requestId": boundary_id,
                "action": { "op": "typing" },
            }))
            .unwrap(),
        )
        .unwrap();
        let boundary_reply: serde_json::Value = serde_json::from_str(&boundary_reply).unwrap();
        assert_eq!(boundary_reply["result"]["success"], true);
        assert_eq!(
            boundary_reply["requestId"],
            "b".repeat(MAX_OUTBOUND_REQUEST_ID_BYTES)
        );

        let ledger_before =
            serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap();
        let snapshot_before = fs::read_to_string(&path).unwrap();
        let oversized_id = "x".repeat(MAX_OUTBOUND_REQUEST_ID_BYTES + 1);
        let reply = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound",
                "requestId": oversized_id,
                "action": { "op": "typing" },
            }))
            .unwrap(),
        )
        .unwrap();
        let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();

        assert_eq!(reply["requestId"], "");
        assert_eq!(reply["result"]["success"], false);
        assert_eq!(reply["result"]["error"], "requestId exceeds maximum length");
        let ledger_after = serde_json::to_string(&*state.outbound_results.lock().unwrap()).unwrap();
        assert_eq!(ledger_after, ledger_before);
        assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_before);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn oversized_unknown_operation_is_rejected_before_result_ledger_insertion() {
        let path = temp_path("oversized-outbound-op");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        let oversized_op = "x".repeat(MAX_OUTBOUND_OPERATION_BYTES + 1);
        let reply = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound",
                "requestId": "req-oversized-op",
                "action": { "op": oversized_op },
            }))
            .unwrap(),
        )
        .unwrap();
        let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();

        assert_eq!(reply["result"]["success"], false);
        assert_eq!(reply["result"]["error"], OUTBOUND_OPERATION_TOO_LONG_ERROR);
        assert!(reply.get("resultId").is_none());
        assert!(state.outbound_results.lock().unwrap().is_empty());
        assert!(!path.exists());
    }

    #[test]
    fn outbound_result_byte_limit_rejects_before_mutating_ledger() {
        let state = RelayState::default();
        let oversized_result = json!({
            "success": false,
            "error": "x".repeat(MAX_OUTBOUND_RESULT_LEDGER_BYTES),
        });
        let result = state
            .record_outbound_result(DEV_GATEWAY_ID, "req-result-byte-limit", oversized_result)
            .unwrap();

        assert_eq!(result.result["success"], false);
        assert_eq!(
            result.result["error"],
            DURABLE_REQUEST_LEDGER_BYTE_LIMIT_ERROR
        );
        assert!(result.result_id.is_none());
        assert!(state.outbound_results.lock().unwrap().is_empty());
        assert_eq!(*state.next_outbound_result_id.lock().unwrap(), 0);
    }

    #[test]
    fn outbound_result_snapshot_over_byte_limit_refuses_startup() {
        let path = temp_path("outbound-result-byte-limit");
        let snapshot = DeliverySnapshot {
            version: DELIVERY_SNAPSHOT_VERSION,
            clock_high_water_ms: 1,
            deliveries: vec![],
            next_seq: 0,
            outbound_results: vec![OutboundResult {
                gateway_id: DEV_GATEWAY_ID.into(),
                request_id: "req-oversized-snapshot".into(),
                result_id: 1,
                result: json!({
                    "success": false,
                    "error": "x".repeat(MAX_OUTBOUND_RESULT_LEDGER_BYTES),
                }),
                delivery_seq: None,
                last_seen_at_ms: 1,
            }],
            next_outbound_result_id: 1,
            retired_outbound_results: vec![],
        };
        assert!(
            outbound_result_bytes(&snapshot.outbound_results).unwrap()
                > MAX_OUTBOUND_RESULT_LEDGER_BYTES
        );
        fs::write(&path, serde_json::to_string(&snapshot).unwrap()).unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            RelayState::default().set_persist_path(&path);
        }));
        assert!(result.is_err());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn consumed_delivery_does_not_release_outbound_request_result() {
        let state = Arc::new(RelayState::default());
        let first = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-after-consume","action":{"op":"send","chat_id":"telepathy:direct","content":"speak once"}}"#,
        )
        .unwrap();

        let (spoken, _) = state
            .deliveries_after(0, true, Some("telepathy:direct"), None, None)
            .unwrap();
        assert_eq!(spoken.len(), 1);
        assert_eq!(spoken[0].content, "speak once");
        assert!(state.pending_for("telepathy:direct").is_empty());

        let retry = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-after-consume","action":{"op":"send","chat_id":"telepathy:direct","content":"must not speak twice"}}"#,
        )
        .unwrap();

        assert_eq!(retry, first);
        assert!(state.pending_for("telepathy:direct").is_empty());
        assert_eq!(state.outbound_results.lock().unwrap().len(), 1);
    }

    #[test]
    fn outbound_request_result_survives_restart_and_prevents_reenqueue() {
        let path = temp_path("outbound-idempotency");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        let first = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-restart","action":{"op":"send","chat_id":"telepathy:direct","content":"persisted"}}"#,
        )
        .unwrap();

        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);
        let retry = handle_gateway_frame(
            &reloaded,
            r#"{"type":"outbound","requestId":"req-restart","action":{"op":"send","chat_id":"telepathy:direct","content":"must not enqueue"}}"#,
        )
        .unwrap();

        assert_eq!(retry, first);
        let pending = reloaded.pending_for("telepathy:direct");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content, "persisted");
        assert_eq!(reloaded.outbound_results.lock().unwrap().len(), 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn gateway_result_namespace_prevents_cross_gateway_observe_or_retire() {
        let state = Arc::new(RelayState::default());
        let request = "shared-request-id";
        let gateway_a = "gateway-a";
        let gateway_b = "gateway-b";

        let a_reply = handle_gateway_frame_for_gateway(
            &state,
            gateway_a,
            &json!({
                "type": "outbound",
                "requestId": request,
                "action": { "op": "gateway-a-only" },
            })
            .to_string(),
        )
        .unwrap();
        let a_reply: serde_json::Value = serde_json::from_str(&a_reply).unwrap();
        let a_result_id = a_reply["resultId"].as_u64().unwrap();
        assert_eq!(a_reply["result"]["success"], false);
        assert_eq!(
            a_reply["result"]["error"],
            "op gateway-a-only not implemented"
        );

        // The same request ID is a new namespace entry for B, rather than a
        // retry of A's failed action.
        let b_reply = handle_gateway_frame_for_gateway(
            &state,
            gateway_b,
            &json!({
                "type": "outbound",
                "requestId": request,
                "action": { "op": "typing" },
            })
            .to_string(),
        )
        .unwrap();
        let b_reply: serde_json::Value = serde_json::from_str(&b_reply).unwrap();
        let b_result_id = b_reply["resultId"].as_u64().unwrap();
        assert_eq!(b_reply["result"]["success"], true);
        assert_ne!(a_result_id, b_result_id);
        assert_eq!(state.outbound_results.lock().unwrap().len(), 2);

        // B cannot retire A's live generation. A's original result remains
        // visible only in A's namespace and is returned on its retry.
        let b_wrong_ack = handle_gateway_frame_for_gateway(
            &state,
            gateway_b,
            &json!({
                "type": "outbound_result_ack",
                "requestId": request,
                "resultId": a_result_id,
            })
            .to_string(),
        )
        .unwrap();
        let b_wrong_ack: serde_json::Value = serde_json::from_str(&b_wrong_ack).unwrap();
        assert_eq!(b_wrong_ack["result"]["success"], false);
        assert_eq!(b_wrong_ack["result"]["error"], RESULT_ID_MISMATCH_ERROR);
        assert_eq!(state.outbound_results.lock().unwrap().len(), 2);

        let a_retry = handle_gateway_frame_for_gateway(
            &state,
            gateway_a,
            &json!({
                "type": "outbound",
                "requestId": request,
                "action": { "op": "typing" },
            })
            .to_string(),
        )
        .unwrap();
        let a_retry: serde_json::Value = serde_json::from_str(&a_retry).unwrap();
        assert_eq!(
            a_retry["result"]["error"],
            "op gateway-a-only not implemented"
        );
        assert_eq!(a_retry["resultId"].as_u64(), Some(a_result_id));
    }

    #[test]
    fn gateway_tombstones_and_request_reuse_stay_scoped_across_restart() {
        let path = temp_path("gateway-scoped-tombstones");
        let request = "reused-by-two-gateways";
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);

        let a_first = handle_gateway_frame_for_gateway(
            &state,
            "gateway-a",
            &json!({
                "type": "outbound",
                "requestId": request,
                "action": { "op": "typing" },
            })
            .to_string(),
        )
        .unwrap();
        let a_first: serde_json::Value = serde_json::from_str(&a_first).unwrap();
        let a_result_id = a_first["resultId"].as_u64().unwrap();
        let a_retired = handle_gateway_frame_for_gateway(
            &state,
            "gateway-a",
            &json!({
                "type": "outbound_result_ack",
                "requestId": request,
                "resultId": a_result_id,
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&a_retired).unwrap()["result"]
                ["alreadyRetired"],
            false
        );

        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);

        // A's tombstone is not a success proof in B's namespace.
        let b_wrong_ack = handle_gateway_frame_for_gateway(
            &reloaded,
            "gateway-b",
            &json!({
                "type": "outbound_result_ack",
                "requestId": request,
                "resultId": a_result_id,
            })
            .to_string(),
        )
        .unwrap();
        let b_wrong_ack: serde_json::Value = serde_json::from_str(&b_wrong_ack).unwrap();
        assert_eq!(b_wrong_ack["result"]["success"], false);

        let b_first = handle_gateway_frame_for_gateway(
            &reloaded,
            "gateway-b",
            &json!({
                "type": "outbound",
                "requestId": request,
                "action": { "op": "typing" },
            })
            .to_string(),
        )
        .unwrap();
        let b_first: serde_json::Value = serde_json::from_str(&b_first).unwrap();
        let b_result_id = b_first["resultId"].as_u64().unwrap();
        assert!(b_result_id > a_result_id);

        let after_b_restart = Arc::new(RelayState::default());
        after_b_restart.set_persist_path(&path);
        let b_retired = handle_gateway_frame_for_gateway(
            &after_b_restart,
            "gateway-b",
            &json!({
                "type": "outbound_result_ack",
                "requestId": request,
                "resultId": b_result_id,
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&b_retired).unwrap()["result"]
                ["alreadyRetired"],
            false
        );

        // B's retirement does not suppress A's independent reuse. The old A
        // tombstone and generation check reject an old delayed acknowledgement
        // without touching A's new live generation.
        let a_reused = handle_gateway_frame_for_gateway(
            &after_b_restart,
            "gateway-a",
            &json!({
                "type": "outbound",
                "requestId": request,
                "action": { "op": "typing" },
            })
            .to_string(),
        )
        .unwrap();
        let a_reused: serde_json::Value = serde_json::from_str(&a_reused).unwrap();
        let a_reused_id = a_reused["resultId"].as_u64().unwrap();
        assert!(a_reused_id > b_result_id);

        let delayed_a_ack = handle_gateway_frame_for_gateway(
            &after_b_restart,
            "gateway-a",
            &json!({
                "type": "outbound_result_ack",
                "requestId": request,
                "resultId": a_result_id,
            })
            .to_string(),
        )
        .unwrap();
        let delayed_a_ack: serde_json::Value = serde_json::from_str(&delayed_a_ack).unwrap();
        assert_eq!(delayed_a_ack["result"]["success"], false);
        assert_eq!(delayed_a_ack["result"]["error"], RESULT_ID_MISMATCH_ERROR);
        assert_eq!(after_b_restart.outbound_results.lock().unwrap().len(), 1);
        assert_eq!(
            after_b_restart.outbound_results.lock().unwrap()[0].result_id,
            a_reused_id
        );

        let repeated_b_ack = handle_gateway_frame_for_gateway(
            &after_b_restart,
            "gateway-b",
            &json!({
                "type": "outbound_result_ack",
                "requestId": request,
                "resultId": b_result_id,
            })
            .to_string(),
        )
        .unwrap();
        let repeated_b_ack: serde_json::Value = serde_json::from_str(&repeated_b_ack).unwrap();
        assert_eq!(repeated_b_ack["result"]["alreadyRetired"], true);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn disconnected_gateway_results_are_abandoned_for_the_next_gateway() {
        let state = RelayState::default();
        let start = 1_000_000;
        state.set_clock_for_test(start);
        for index in 0..MAX_OUTBOUND_RESULTS {
            state
                .record_outbound_result(
                    "gateway-a",
                    &format!("a-{index}"),
                    json!({ "success": true }),
                )
                .unwrap();
        }
        state.set_clock_for_test(start + OUTBOUND_RESULT_RETENTION_MS);
        let b = state
            .record_outbound_result(
                "gateway-b",
                "b-after-a-disappeared",
                json!({
                    "success": true
                }),
            )
            .unwrap();

        assert_eq!(b.result_id, Some(MAX_OUTBOUND_RESULTS as u64 + 1));
        assert_eq!(
            state
                .outbound_results
                .lock()
                .unwrap()
                .iter()
                .filter(|result| result.gateway_id == "gateway-a")
                .count(),
            0
        );
        assert_eq!(state.outbound_results.lock().unwrap().len(), 1);
        assert!(state
            .retired_outbound_results
            .lock()
            .unwrap()
            .iter()
            .all(|tombstone| tombstone.gateway_id == "gateway-a"));
        assert!(
            outbound_result_bytes(&state.outbound_results.lock().unwrap()).unwrap()
                <= MAX_OUTBOUND_RESULT_LEDGER_BYTES
        );
        assert!(
            retired_outbound_result_bytes(&state.retired_outbound_results.lock().unwrap()).unwrap()
                <= MAX_RETIRED_OUTBOUND_RESULT_BYTES
        );
    }

    #[test]
    fn active_gateway_results_are_never_abandoned_for_another_identity() {
        let state = RelayState::default();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let (superseded, _watch_rx) = tokio::sync::watch::channel(false);
        let connection_id = state.begin_connection();
        assert!(state.activate_connection(connection_id, "gateway-a".into(), tx, superseded,));
        let start = 2_000_000;
        state.set_clock_for_test(start);
        for index in 0..MAX_OUTBOUND_RESULTS {
            state
                .record_outbound_result(
                    "gateway-a",
                    &format!("a-active-{index}"),
                    json!({ "success": true }),
                )
                .unwrap();
        }
        state.set_clock_for_test(start + OUTBOUND_RESULT_RETENTION_MS);
        let b = state
            .record_outbound_result(
                "gateway-b",
                "b-must-wait",
                json!({
                    "success": true
                }),
            )
            .unwrap();

        assert_eq!(b.result_id, None);
        assert_eq!(b.result["error"], DURABLE_REQUEST_LEDGER_FULL_ERROR);
        assert_eq!(
            state.outbound_results.lock().unwrap().len(),
            MAX_OUTBOUND_RESULTS
        );
        assert!(state.retired_outbound_results.lock().unwrap().is_empty());
    }

    #[test]
    fn abandonment_survives_restart_and_keeps_delivery_sequence_historical() {
        let path = temp_path("outbound-abandonment-restart");
        let start = 3_000_000;
        let state = RelayState::default();
        state.set_clock_for_test(start);
        state.set_persist_path(&path);
        let first = state
            .queue_gateway_delivery_for_request(
                "gateway-a",
                "send-once",
                "telepathy:direct",
                "first delivery",
                None,
            )
            .unwrap();
        assert_eq!(first.result_id, Some(1));
        drop(state);

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        reloaded.set_clock_for_test(start + OUTBOUND_RESULT_RETENTION_MS);
        let second = reloaded
            .queue_gateway_delivery_for_request(
                "gateway-b",
                "send-after-expiry",
                "telepathy:direct",
                "second delivery",
                None,
            )
            .unwrap();
        assert_eq!(second.result_id, Some(2));
        assert_eq!(reloaded.pending_for("telepathy:direct").len(), 2);
        assert_eq!(
            reloaded.pending_for("telepathy:direct")[0].seq,
            1,
            "the expired result's delivery remains available to the phone"
        );
        assert_eq!(reloaded.pending_for("telepathy:direct")[1].seq, 2);
        assert!(reloaded
            .retired_outbound_results
            .lock()
            .unwrap()
            .iter()
            .any(|tombstone| tombstone.gateway_id == "gateway-a"));
        let snapshot: DeliverySnapshot =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(snapshot.version, DELIVERY_SNAPSHOT_VERSION);
        assert!(snapshot.clock_high_water_ms >= start + OUTBOUND_RESULT_RETENTION_MS);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn clock_high_water_prevents_expiry_after_rollback() {
        let state = RelayState::default();
        let start = 4_000_000;
        state.set_clock_for_test(start);
        state
            .record_outbound_result("gateway-a", "clock-safe", json!({ "success": true }))
            .unwrap();
        state.set_clock_for_test(start + OUTBOUND_RESULT_RETENTION_MS - 1);
        assert_eq!(
            state.effective_clock_millis(),
            start + OUTBOUND_RESULT_RETENTION_MS - 1
        );
        state.set_clock_for_test(start + 1);
        let b = state
            .record_outbound_result(
                "gateway-b",
                "clock-rollback",
                json!({
                    "success": true
                }),
            )
            .unwrap();

        assert!(b.result_id.is_some());
        assert!(state
            .outbound_results
            .lock()
            .unwrap()
            .iter()
            .any(|result| result.gateway_id == "gateway-a"));
    }

    #[test]
    fn result_retirement_reclaims_capacity_and_guards_request_id_reuse() {
        let state = Arc::new(RelayState::default());
        let mut retired_result_id = None;
        for index in 0..MAX_OUTBOUND_RESULTS {
            let reply = handle_gateway_frame(
                &state,
                &format!(
                    r#"{{"type":"outbound","requestId":"typing-{index}","action":{{"op":"typing"}}}}"#
                ),
            )
            .unwrap();
            let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
            assert_eq!(reply["result"]["success"], true);
            assert!(reply["resultId"].is_u64());
            if index == 0 {
                retired_result_id = reply["resultId"].as_u64();
            }
        }
        let retired_result_id = retired_result_id.unwrap();

        let overflow = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"send-after-ledger-full","action":{"op":"send","chat_id":"telepathy:direct","content":"must not queue"}}"#,
        )
        .unwrap();
        let overflow: serde_json::Value = serde_json::from_str(&overflow).unwrap();
        assert_eq!(overflow["result"]["success"], false);
        assert_eq!(
            overflow["result"]["error"],
            DURABLE_REQUEST_LEDGER_FULL_ERROR
        );
        assert!(state.pending_for("telepathy:direct").is_empty());
        assert_eq!(
            state.outbound_results.lock().unwrap().len(),
            MAX_OUTBOUND_RESULTS
        );

        let retired = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "typing-0",
                "resultId": retired_result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let retired: serde_json::Value = serde_json::from_str(&retired).unwrap();
        assert_eq!(retired["type"], "outbound_result_retired");
        assert_eq!(retired["result"]["success"], true);
        assert_eq!(retired["result"]["alreadyRetired"], false);
        assert_eq!(
            state.outbound_results.lock().unwrap().len(),
            MAX_OUTBOUND_RESULTS - 1
        );

        // The old request ID is reusable only after its matching result has
        // been durably retired. The new generation protects it from delayed
        // acknowledgements for the old action.
        let reused = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"typing-0","action":{"op":"typing"}}"#,
        )
        .unwrap();
        let reused: serde_json::Value = serde_json::from_str(&reused).unwrap();
        let reused_result_id = reused["resultId"].as_u64().unwrap();
        assert_eq!(reused["result"]["success"], true);
        assert!(reused_result_id > retired_result_id);
        assert_eq!(
            state.outbound_results.lock().unwrap().len(),
            MAX_OUTBOUND_RESULTS
        );

        let delayed = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "typing-0",
                "resultId": retired_result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let delayed: serde_json::Value = serde_json::from_str(&delayed).unwrap();
        assert_eq!(delayed["result"]["success"], false);
        assert_eq!(delayed["result"]["error"], RESULT_ID_MISMATCH_ERROR);
        assert_eq!(
            state
                .outbound_result_for_request(DEV_GATEWAY_ID, "typing-0")
                .unwrap()
                .result_id,
            reused_result_id
        );
    }

    #[test]
    fn result_retirement_is_restart_safe_and_ack_retries_are_idempotent() {
        let path = temp_path("result-retirement-restart");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        let first = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-restart-retire","action":{"op":"send","chat_id":"telepathy:direct","content":"speak once"}}"#,
        )
        .unwrap();
        let first: serde_json::Value = serde_json::from_str(&first).unwrap();
        let first_result_id = first["resultId"].as_u64().unwrap();

        // A crash before acknowledgement leaves the durable action result in
        // place, so a retry after restart returns it without enqueuing again.
        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);
        let retry = handle_gateway_frame(
            &reloaded,
            r#"{"type":"outbound","requestId":"req-restart-retire","action":{"op":"send","chat_id":"telepathy:direct","content":"must not enqueue twice"}}"#,
        )
        .unwrap();
        let retry: serde_json::Value = serde_json::from_str(&retry).unwrap();
        assert_eq!(retry, first);
        assert_eq!(reloaded.pending_for("telepathy:direct").len(), 1);

        let retired = handle_gateway_frame(
            &reloaded,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-restart-retire",
                "resultId": first_result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let retired: serde_json::Value = serde_json::from_str(&retired).unwrap();
        assert_eq!(retired["result"]["alreadyRetired"], false);

        // If the retirement response was lost, a later restart can safely
        // retry the acknowledgement even though the ledger entry is gone.
        let after_retirement = Arc::new(RelayState::default());
        after_retirement.set_persist_path(&path);
        let repeated_ack = handle_gateway_frame(
            &after_retirement,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-restart-retire",
                "resultId": first_result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let repeated_ack: serde_json::Value = serde_json::from_str(&repeated_ack).unwrap();
        assert_eq!(repeated_ack["result"]["success"], true);
        assert_eq!(repeated_ack["result"]["alreadyRetired"], true);

        let reused = handle_gateway_frame(
            &after_retirement,
            r#"{"type":"outbound","requestId":"req-restart-retire","action":{"op":"typing"}}"#,
        )
        .unwrap();
        let reused: serde_json::Value = serde_json::from_str(&reused).unwrap();
        assert!(reused["resultId"].as_u64().unwrap() > first_result_id);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn retired_result_ack_requires_an_exact_pair_before_and_after_restart() {
        let path = temp_path("result-retirement-exact-pair");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        let first = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-exact-retirement","action":{"op":"typing"}}"#,
        )
        .unwrap();
        let first: serde_json::Value = serde_json::from_str(&first).unwrap();
        let result_id = first["resultId"].as_u64().unwrap();

        let retired = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-exact-retirement",
                "resultId": result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let retired: serde_json::Value = serde_json::from_str(&retired).unwrap();
        assert_eq!(retired["result"]["alreadyRetired"], false);
        assert_eq!(state.retired_outbound_results.lock().unwrap().len(), 1);

        let wrong_live = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-exact-retirement",
                "resultId": result_id + 1,
            }))
            .unwrap(),
        )
        .unwrap();
        let wrong_live: serde_json::Value = serde_json::from_str(&wrong_live).unwrap();
        assert_eq!(wrong_live["result"]["success"], false);
        assert_eq!(wrong_live["result"]["error"], RESULT_ID_MISMATCH_ERROR);

        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);
        assert_eq!(reloaded.retired_outbound_results.lock().unwrap().len(), 1);

        let wrong_after_restart = handle_gateway_frame(
            &reloaded,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-exact-retirement",
                "resultId": result_id + 1,
            }))
            .unwrap(),
        )
        .unwrap();
        let wrong_after_restart: serde_json::Value =
            serde_json::from_str(&wrong_after_restart).unwrap();
        assert_eq!(wrong_after_restart["result"]["success"], false);
        assert_eq!(
            wrong_after_restart["result"]["error"],
            RESULT_ID_MISMATCH_ERROR
        );

        let exact_after_restart = handle_gateway_frame(
            &reloaded,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-exact-retirement",
                "resultId": result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let exact_after_restart: serde_json::Value =
            serde_json::from_str(&exact_after_restart).unwrap();
        assert_eq!(exact_after_restart["result"]["success"], true);
        assert_eq!(exact_after_restart["result"]["alreadyRetired"], true);

        // Retirement leaves the request ID available for the next outbound
        // action, while the old tombstone still rejects its delayed receipt
        // once a new generation is active.
        let reused = handle_gateway_frame(
            &reloaded,
            r#"{"type":"outbound","requestId":"req-exact-retirement","action":{"op":"typing"}}"#,
        )
        .unwrap();
        let reused: serde_json::Value = serde_json::from_str(&reused).unwrap();
        assert!(reused["resultId"].as_u64().unwrap() > result_id);

        let delayed = handle_gateway_frame(
            &reloaded,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-exact-retirement",
                "resultId": result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let delayed: serde_json::Value = serde_json::from_str(&delayed).unwrap();
        assert_eq!(delayed["result"]["success"], false);
        assert_eq!(delayed["result"]["error"], RESULT_ID_MISMATCH_ERROR);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn retired_outbound_tombstones_are_bounded_and_persisted() {
        let path = temp_path("retired-outbound-tombstone-bounds");
        let mut tombstones = Vec::new();
        for result_id in 1..=(MAX_RETIRED_OUTBOUND_RESULTS as u64 + 1) {
            append_retired_outbound_result(
                &mut tombstones,
                RetiredOutboundResult {
                    gateway_id: DEV_GATEWAY_ID.into(),
                    request_id: format!("retired-request-{result_id}"),
                    result_id,
                },
            )
            .unwrap();
        }
        assert_eq!(tombstones.len(), MAX_RETIRED_OUTBOUND_RESULTS);
        assert_eq!(tombstones.first().unwrap().result_id, 2);
        assert_eq!(
            tombstones.last().unwrap().result_id,
            MAX_RETIRED_OUTBOUND_RESULTS as u64 + 1
        );
        assert!(
            retired_outbound_result_bytes(&tombstones).unwrap()
                <= MAX_RETIRED_OUTBOUND_RESULT_BYTES
        );

        let state = RelayState::default();
        state.set_persist_path(&path);
        *state.next_outbound_result_id.lock().unwrap() = MAX_RETIRED_OUTBOUND_RESULTS as u64 + 1;
        *state.retired_outbound_results.lock().unwrap() = tombstones.clone();
        state.persist().unwrap();

        let reloaded = RelayState::default();
        reloaded.set_persist_path(&path);
        assert_eq!(
            *reloaded.retired_outbound_results.lock().unwrap(),
            tombstones
        );

        let snapshot: DeliverySnapshot =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(snapshot.version, DELIVERY_SNAPSHOT_VERSION);
        assert_eq!(snapshot.retired_outbound_results, tombstones);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn retired_outbound_tombstones_evict_oldest_entries_at_the_byte_bound() {
        let mut tombstones = Vec::new();
        let attempts = MAX_RETIRED_OUTBOUND_RESULTS as u64 * 2;
        for result_id in 1..=attempts {
            let prefix = format!("retired-byte-{result_id:020}-");
            let request_id = format!(
                "{prefix}{}",
                "x".repeat(MAX_OUTBOUND_REQUEST_ID_BYTES - prefix.len())
            );
            assert_eq!(request_id.len(), MAX_OUTBOUND_REQUEST_ID_BYTES);
            append_retired_outbound_result(
                &mut tombstones,
                RetiredOutboundResult {
                    gateway_id: DEV_GATEWAY_ID.into(),
                    request_id,
                    result_id,
                },
            )
            .unwrap();
        }

        assert!(tombstones.len() < MAX_RETIRED_OUTBOUND_RESULTS);
        assert!(tombstones.first().unwrap().result_id > 1);
        assert_eq!(tombstones.last().unwrap().result_id, attempts);
        assert!(
            retired_outbound_result_bytes(&tombstones).unwrap()
                <= MAX_RETIRED_OUTBOUND_RESULT_BYTES
        );
    }

    #[test]
    fn delivery_snapshot_hard_cutover_rejects_invalid_retired_tombstones() {
        let path = temp_path("retired-outbound-tombstone-validation");
        let too_many = (1..=(MAX_RETIRED_OUTBOUND_RESULTS as u64 + 1))
            .map(|result_id| RetiredOutboundResult {
                gateway_id: DEV_GATEWAY_ID.into(),
                request_id: format!("retired-{result_id}"),
                result_id,
            })
            .collect::<Vec<_>>();
        let too_large = (1..=128_u64)
            .map(|result_id| RetiredOutboundResult {
                gateway_id: DEV_GATEWAY_ID.into(),
                request_id: "x".repeat(MAX_OUTBOUND_REQUEST_ID_BYTES),
                result_id,
            })
            .collect::<Vec<_>>();
        assert!(
            retired_outbound_result_bytes(&too_large).unwrap() > MAX_RETIRED_OUTBOUND_RESULT_BYTES
        );

        let invalid_snapshots = vec![
            // The absence of the version field is a hard cutover failure,
            // not an invitation to infer a legacy tombstone-free state.
            json!({
                "deliveries": [],
                "next_seq": 0,
                "outbound_results": [],
                "next_outbound_result_id": 0,
                "retired_outbound_results": [],
            }),
            serde_json::to_value(DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION - 1,
                clock_high_water_ms: 0,
                deliveries: vec![],
                next_seq: 0,
                outbound_results: vec![],
                next_outbound_result_id: 0,
                retired_outbound_results: vec![],
            })
            .unwrap(),
            serde_json::to_value(DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![],
                next_seq: 0,
                outbound_results: vec![],
                next_outbound_result_id: MAX_RETIRED_OUTBOUND_RESULTS as u64 + 1,
                retired_outbound_results: too_many,
            })
            .unwrap(),
            serde_json::to_value(DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 0,
                deliveries: vec![],
                next_seq: 0,
                outbound_results: vec![],
                next_outbound_result_id: 128,
                retired_outbound_results: too_large,
            })
            .unwrap(),
            serde_json::to_value(DeliverySnapshot {
                version: DELIVERY_SNAPSHOT_VERSION,
                clock_high_water_ms: 1,
                deliveries: vec![],
                next_seq: 0,
                outbound_results: vec![OutboundResult {
                    gateway_id: DEV_GATEWAY_ID.into(),
                    request_id: "active".into(),
                    result_id: 1,
                    result: json!({ "success": true }),
                    delivery_seq: None,
                    last_seen_at_ms: 1,
                }],
                next_outbound_result_id: 1,
                retired_outbound_results: vec![RetiredOutboundResult {
                    gateway_id: DEV_GATEWAY_ID.into(),
                    request_id: "retired".into(),
                    result_id: 1,
                }],
            })
            .unwrap(),
        ];

        for snapshot in invalid_snapshots {
            let original = serde_json::to_string(&snapshot).unwrap();
            fs::write(&path, &original).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                RelayState::default().set_persist_path(&path);
            }));
            assert!(result.is_err());
            assert_eq!(fs::read_to_string(&path).unwrap(), original);
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn post_rename_retirement_failure_recovers_safely_after_restart() {
        let path = temp_path("result-retirement-post-rename");
        let state = Arc::new(RelayState::default());
        state.set_persist_path(&path);
        let result = handle_gateway_frame(
            &state,
            r#"{"type":"outbound","requestId":"req-post-rename","action":{"op":"typing"}}"#,
        )
        .unwrap();
        let result: serde_json::Value = serde_json::from_str(&result).unwrap();
        let result_id = result["resultId"].as_u64().unwrap();

        fail_next_post_rename_directory_sync();
        let failed_ack = handle_gateway_frame(
            &state,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-post-rename",
                "resultId": result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let failed_ack: serde_json::Value = serde_json::from_str(&failed_ack).unwrap();
        assert_eq!(
            failed_ack["result"]["error"],
            "result retirement persistence failed"
        );

        // Rename made the new snapshot visible, but the directory sync error
        // means this process cannot safely continue. A fresh process reloads
        // the committed state and treats the replayed acknowledgement as a
        // harmless idempotent success.
        let reloaded = Arc::new(RelayState::default());
        reloaded.set_persist_path(&path);
        let retry = handle_gateway_frame(
            &reloaded,
            &serde_json::to_string(&json!({
                "type": "outbound_result_ack",
                "requestId": "req-post-rename",
                "resultId": result_id,
            }))
            .unwrap(),
        )
        .unwrap();
        let retry: serde_json::Value = serde_json::from_str(&retry).unwrap();
        assert_eq!(retry["result"]["success"], true);
        assert_eq!(retry["result"]["alreadyRetired"], true);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn outbound_batch_over_channel_capacity_retains_every_result() {
        let state = Arc::new(RelayState::default());
        state.set_lane_registry(Arc::new(tokio::sync::Mutex::new(
            LaneRegistry::default_direct(),
        )));
        let connection_id = state.begin_connection();
        let (tx, _rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
        let (superseded_tx, _superseded_rx) = tokio::sync::watch::channel(false);
        assert!(state.activate_connection(
            connection_id,
            DEV_GATEWAY_ID.to_string(),
            tx,
            superseded_tx
        ));
        let batch = (0..=RELAY_CHANNEL_CAP)
            .map(|index| {
                format!(
                    r#"{{"type":"outbound","requestId":"req-{index}","action":{{"op":"send","chat_id":"telepathy:direct","content":"reply-{index}"}}}}"#
                ) + "\n"
            })
            .collect::<String>();

        // This models one gateway text frame containing more actions than the
        // phone/inbound channel can hold. Production drains this same set of
        // lines and writes each returned reply directly to the WebSocket.
        let mut replies: Vec<serde_json::Value> = Vec::new();
        for line in batch.split_inclusive('\n') {
            if let Some(reply) = handle_gateway_frame_for_connection(
                &state,
                connection_id,
                DEV_GATEWAY_ID,
                line.trim(),
            )
            .await
            {
                replies.push(serde_json::from_str(&reply).unwrap());
            }
        }

        assert_eq!(replies.len(), RELAY_CHANNEL_CAP + 1);
        for (index, reply) in replies.iter().enumerate() {
            assert_eq!(reply["type"], "outbound_result");
            assert_eq!(reply["requestId"], format!("req-{index}"));
            assert_eq!(reply["result"]["success"], true);
        }
        assert_eq!(
            state.pending_count("telepathy:direct"),
            RELAY_CHANNEL_CAP + 1
        );
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_replaces_less_private_snapshot_with_owner_only_file() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_path("private-snapshot");
        fs::write(&path, "old").unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&path, permissions).unwrap();

        atomic_write(&path, "voice and reply content").unwrap();

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "voice and reply content"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn consuming_one_lane_preserves_other_lane_deliveries() {
        let state = RelayState::default();
        state
            .queue_delivery("telepathy:direct", "direct reply")
            .unwrap();
        state.queue_delivery("telepathy:repo:x", "x reply").unwrap();

        let (picked, latest) = state
            .deliveries_after(0, true, Some("telepathy:direct"), None, None)
            .unwrap();
        assert_eq!(picked.len(), 1);
        assert_eq!(latest, 2);
        assert_eq!(state.pending_count("telepathy:direct"), 0);
        assert_eq!(state.pending_count("telepathy:repo:x"), 1);
    }

    #[test]
    fn exact_pending_consume_preserves_unspoken_correlated_and_generic_rows() {
        let state = RelayState::default();
        let spoken = state
            .queue_delivery("telepathy:direct", "spoken generic reply")
            .unwrap();
        let receipt_owned = state
            .queue_gateway_delivery(
                "telepathy:direct",
                "receipt-owned correlated reply",
                Some("tp-owned"),
            )
            .unwrap();
        let unspoken_generic = state
            .queue_delivery("telepathy:direct", "unspoken generic reply")
            .unwrap();

        assert_eq!(
            state
                .consume_lane_sequences("telepathy:direct", &[spoken])
                .unwrap(),
            1
        );
        let pending = state.pending_for("telepathy:direct");
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].seq, receipt_owned);
        assert_eq!(pending[0].reply_to.as_deref(), Some("tp-owned"));
        assert_eq!(pending[1].seq, unspoken_generic);
    }

    #[test]
    fn oversized_delivery_boundaries_fail_without_mutating_the_durable_queue() {
        let path = temp_path("unsafe-delivery-boundary");
        let state = RelayState::default();
        state.set_persist_path(&path);
        state
            .queue_delivery("telepathy:direct", "first reply")
            .unwrap();
        state
            .queue_delivery("telepathy:direct", "second reply")
            .unwrap();
        let snapshot_before = fs::read_to_string(&path).unwrap();
        let oversized = MAX_SAFE_SEQUENCE + 1;

        assert!(state
            .consume_lane_sequences("telepathy:direct", &[oversized])
            .is_err());
        assert!(state
            .deliveries_after(oversized, false, Some("telepathy:direct"), None, None)
            .is_err());
        assert!(state
            .deliveries_after(0, true, Some("telepathy:direct"), None, Some(oversized),)
            .is_err());

        assert_eq!(state.pending_count("telepathy:direct"), 2);
        assert_eq!(fs::read_to_string(&path).unwrap(), snapshot_before);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn bounded_reply_ack_preserves_later_reply_for_the_same_request() {
        let state = RelayState::default();
        let first = state
            .queue_gateway_delivery("telepathy:direct", "first", Some("tp-1"))
            .unwrap();
        let second = state
            .queue_gateway_delivery("telepathy:direct", "second", Some("tp-1"))
            .unwrap();

        let (spoken, _) = state
            .deliveries_after(0, true, Some("telepathy:direct"), Some("tp-1"), Some(first))
            .unwrap();
        assert_eq!(spoken.len(), 1);
        assert_eq!(spoken[0].seq, first);
        assert_eq!(state.pending_for("telepathy:direct")[0].seq, second);
    }

    #[test]
    fn uncorrelated_gateway_delivery_stays_pending() {
        let state = RelayState::default();
        let _ = state.register_request("telepathy:direct", "tp-1");
        state
            .queue_gateway_delivery("telepathy:direct", "cron", None)
            .unwrap();

        let (synchronous, _) = state
            .deliveries_after(0, false, Some("telepathy:direct"), Some("tp-1"), None)
            .unwrap();
        assert!(synchronous.is_empty());
        assert_eq!(state.pending_for("telepathy:direct")[0].content, "cron");

        let single = RelayState::default();
        let _ = single.register_request("telepathy:direct", "tp-3");
        single
            .queue_gateway_delivery("telepathy:direct", "reply", Some("tp-3"))
            .unwrap();
        let (reply, _) = single
            .deliveries_after(0, false, Some("telepathy:direct"), Some("tp-3"), None)
            .unwrap();
        assert_eq!(reply[0].content, "reply");
    }

    #[test]
    fn truncation_never_splits_utf8() {
        let text = "🧠".repeat(100);
        let shortened = truncate(&text);
        assert!(shortened.len() <= 120);
        assert!(shortened.is_char_boundary(shortened.len()));
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
    let signature = hex::decode(sig_hex)?;
    if signature.len() != HmacSha256::output_size() {
        anyhow::bail!("invalid signature length");
    }
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
        if mac.verify_slice(&signature).is_ok() {
            return Ok(payload.to_string());
        }
    }
    anyhow::bail!("no secret matched")
}

/// Build the CapabilityDescriptor (§2) — our platform's self-description.
pub fn capability_descriptor() -> serde_json::Value {
    json!({
        "contract_version": 3,
        "platform": "relay",
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
    tables, or long lists. If a result includes a visual artifact (image, file, page), \
    deliver it as a link AND summarize it aloud — an artifact alone is never a complete reply.",
        "supported_ops": ["send", "typing"],
        "outbound_result_ack_required": true,
        "outbound_result_ack_type": "outbound_result_ack",
        "inbound_handoff_version": INBOUND_HANDOFF_VERSION,
        "inbound_ack_required": true,
        "inbound_ack_type": "inbound_ack",
    })
}

/// Normalize an utterance into the relay MessageEvent payload (§3).
pub fn message_event(
    lane_id: &str,
    lane_name: &str,
    text: &str,
    msg_seq: u64,
) -> serde_json::Value {
    json!({
        "text": text,
        "message_type": "text",
        "user_id": "telepathy-user",
        "user_name": null,
        "source": {
            "platform": "relay",
            "chat_id": lane_id,
            "chat_type": "dm",
            "chat_name": lane_name,
            "user_id": "telepathy-user",
            "user_name": null,
            "thread_id": null,
            "chat_topic": null,
        },
        "message_id": format!("tp-{msg_seq}"),
    })
}

/// The /relay route. Auth happens at upgrade time; a failure is an HTTP 401
/// (the contract specifies close-code 4401 post-upgrade — noted as a delta to
/// align once tested against a real gateway).
pub fn router(state: Arc<RelayState>, secrets: Vec<String>) -> Router {
    Router::new().route(
        "/",
        get(move |ws: WebSocketUpgrade, headers: HeaderMap| {
            let secrets = secrets.clone();
            let state = state.clone();
            async move {
                let bearer = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "));
                // No secrets configured -> dev/test mode: accept unauthenticated
                // dials (mirrors ws_transport: absent creds = plain upgrade).
                let gateway_id = if secrets.is_empty() {
                    DEV_GATEWAY_ID.to_string()
                } else {
                    match bearer.map(|t| verify_relay_token(t, &secrets)).transpose() {
                        Ok(Some(id)) => id,
                        _ => {
                            return Ok::<_, StatusCode>(
                                (StatusCode::UNAUTHORIZED, "relay auth failed").into_response(),
                            );
                        }
                    }
                };
                if gateway_id.is_empty() || gateway_id.len() > MAX_GATEWAY_ID_BYTES {
                    return Ok::<_, StatusCode>(
                        (StatusCode::UNAUTHORIZED, "relay auth failed").into_response(),
                    );
                }
                println!("relay: gateway '{gateway_id}' dialed in");
                Ok(configure_relay_websocket_upgrade(ws)
                    .on_upgrade(move |socket| relay_socket(socket, state, gateway_id)))
            }
        }),
    )
}

fn configure_relay_websocket_upgrade(ws: WebSocketUpgrade) -> WebSocketUpgrade {
    // Set both limits: max_message_size prevents fragmented messages from
    // growing beyond the NDJSON buffer, while max_frame_size rejects a single
    // oversized frame before tungstenite buffers it.
    ws.max_message_size(MAX_RELAY_WEBSOCKET_MESSAGE_BYTES)
        .max_frame_size(MAX_RELAY_WEBSOCKET_MESSAGE_BYTES)
}

async fn relay_socket(mut socket: WebSocket, state: Arc<RelayState>, gateway_id: String) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAP);
    let mut inbound = String::new();

    // A relay socket is not a usable gateway writer until it has introduced
    // itself and accepted our descriptor. In particular, do not replay a
    // persisted voice turn into a socket that has not completed this exchange.
    if !complete_gateway_handshake(&mut socket, &mut inbound).await {
        println!("relay: gateway disconnected before completing handshake");
        return;
    }

    // Claim a generation only after the handshake. This asks the previous
    // active writer to stop, then serializes its final write/ack before this
    // socket can replay the outbox.
    let connection_id = state.begin_connection();
    let _writer_guard = state.connection_writer_lock.lock().await;
    let (superseded_tx, mut superseded_rx) = tokio::sync::watch::channel(false);
    if !state.activate_connection(connection_id, gateway_id.clone(), tx.clone(), superseded_tx) {
        // A newer handshaken connection arrived while this socket waited for
        // the previous writer. It must never claim or replay inbound turns.
        return;
    }

    'connection: {
        // A single WebSocket text frame may have carried hello plus complete
        // action lines. Process the action suffix only after ownership is live.
        if !drain_gateway_lines(
            &mut inbound,
            &state,
            &mut socket,
            connection_id,
            &gateway_id,
            &mut superseded_rx,
        )
        .await
        {
            break 'connection;
        }

        let mut sent_inbound = HashSet::new();
        // Replay messages that were durably accepted by /api/message before
        // this connection existed or before its predecessor disconnected. The
        // writer lock makes this the only socket that can claim these rows.
        for pending in state.pending_inbound() {
            if !state.is_active_connection(connection_id) {
                break 'connection;
            }
            if !send_owned_text(
                &mut socket,
                &state,
                connection_id,
                &mut superseded_rx,
                format!("{}\n", pending.frame),
            )
            .await
            {
                // Leave the durable row in place for a later gateway socket.
                println!("relay: gateway disconnected during inbound replay");
                break 'connection;
            }
            sent_inbound.insert((pending.message_id, pending.generation));
        }

        // One loop, both directions: our inbound pushes + the gateway's
        // actions. A successor can interrupt either receive operation so an
        // obsolete socket cannot race its replay against the new owner.
        loop {
            tokio::select! {
                changed = superseded_rx.changed() => {
                    if changed.is_err() || *superseded_rx.borrow() {
                        break;
                    }
                }
                maybe_frame = rx.recv() => match maybe_frame {
                    Some(frame) => {
                        if !state.is_active_connection(connection_id) {
                            break;
                        }
                        // Contract frames are newline-delimited JSON; the gateway's
                        // read loop only processes complete lines.
                        let identity = inbound_identity(&frame);
                        // A turn can enter the channel while this connection is
                        // snapshotting the durable outbox for replay. If replay
                        // has already written it, skip this duplicate channel
                        // copy. The row remains pending until an exact ACK.
                        if identity
                            .as_ref()
                            .is_some_and(|identity| sent_inbound.contains(identity))
                        {
                            continue;
                        }
                        if identity.as_ref().is_some_and(|(id, generation)| {
                            !state.inbound_is_pending_identity(id, *generation)
                        }) {
                            continue;
                        }
                        if !send_owned_text(
                            &mut socket,
                            &state,
                            connection_id,
                            &mut superseded_rx,
                            format!("{frame}\n"),
                        ).await {
                            // The outbox row deliberately remains durable. A later
                            // successful handshake will replay it.
                            break;
                        }
                        if let Some(identity) = identity {
                            sent_inbound.insert(identity);
                        }
                    }
                    None => break,
                },
                maybe_msg = socket.recv() => match maybe_msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        if !append_gateway_text(&mut inbound, &text) {
                            eprintln!("relay: inbound NDJSON frame exceeds {MAX_INBOUND_LINE_BYTES} bytes");
                            break;
                        }
                        if !drain_gateway_lines(
                            &mut inbound,
                            &state,
                            &mut socket,
                            connection_id,
                            &gateway_id,
                            &mut superseded_rx,
                        )
                        .await
                        {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                },
            }
        }
    }

    state.clear_outbound_if(connection_id);
    println!("relay: gateway disconnected");
}

async fn complete_gateway_handshake(socket: &mut WebSocket, inbound: &mut String) -> bool {
    tokio::time::timeout(RELAY_HANDSHAKE_TIMEOUT, async {
        loop {
            match socket.recv().await {
                Some(Ok(WsMessage::Text(text))) => {
                    if !append_gateway_text(inbound, &text) {
                        eprintln!(
                            "relay: inbound NDJSON frame exceeds {MAX_INBOUND_LINE_BYTES} bytes"
                        );
                        return false;
                    }
                    while let Some(end) = inbound.find('\n') {
                        let line: String = inbound.drain(..=end).collect();
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        let hello: serde_json::Value = match serde_json::from_str(line) {
                            Ok(value) => value,
                            Err(_) => {
                                println!(
                                    "relay: expected hello, received non-JSON frame: {}",
                                    truncate(line)
                                );
                                return false;
                            }
                        };
                        if hello["type"].as_str() != Some("hello") {
                            println!("relay: expected hello, received {}", truncate(line));
                            return false;
                        }
                        println!(
                            "relay: hello platform={} botId={}",
                            hello["platform"], hello["botId"]
                        );
                        return socket
                            .send(WsMessage::Text(format!("{}\n", descriptor_frame())))
                            .await
                            .is_ok();
                    }
                }
                Some(Ok(_)) => {}
                _ => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

fn append_gateway_text(inbound: &mut String, text: &str) -> bool {
    if inbound.len().saturating_add(text.len()) > MAX_INBOUND_LINE_BYTES {
        return false;
    }
    inbound.push_str(text);
    true
}

async fn drain_gateway_lines(
    inbound: &mut String,
    state: &Arc<RelayState>,
    socket: &mut WebSocket,
    connection_id: u64,
    gateway_id: &str,
    superseded_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    while let Some(end) = inbound.find('\n') {
        let line: String = inbound.drain(..=end).collect();
        let line = line.trim();
        if !line.is_empty() {
            if !state.is_active_connection(connection_id) || *superseded_rx.borrow() {
                return false;
            }
            if let Some(reply) =
                handle_gateway_frame_for_connection(state, connection_id, gateway_id, line).await
            {
                // Gateway replies bypass the bounded phone/inbound channel.
                // Awaiting the WebSocket write gives each accepted action a
                // result; a failed or superseded write terminates this
                // connection so the gateway can retry rather than observing a
                // silent drop.
                if !send_owned_text(
                    socket,
                    state,
                    connection_id,
                    superseded_rx,
                    format!("{reply}\n"),
                )
                .await
                {
                    eprintln!("relay: gateway disconnected while sending reply");
                    return false;
                }
            }
            if !state.is_active_connection(connection_id) || *superseded_rx.borrow() {
                return false;
            }
        }
    }
    true
}

/// Send a frame only while this socket remains the active relay owner. The
/// watch branch is essential: a WebSocket write can remain pending while a
/// successor completes its handshake, and an obsolete socket must stop before
/// it acknowledges the associated durable row.
async fn send_owned_text(
    socket: &mut WebSocket,
    state: &RelayState,
    connection_id: u64,
    superseded_rx: &mut tokio::sync::watch::Receiver<bool>,
    frame: String,
) -> bool {
    if !state.is_active_connection(connection_id) || *superseded_rx.borrow() {
        return false;
    }

    tokio::select! {
        changed = superseded_rx.changed() => {
            changed.is_ok() && !*superseded_rx.borrow()
        }
        result = socket.send(WsMessage::Text(frame)) => {
            result.is_ok()
                && state.is_active_connection(connection_id)
                && !*superseded_rx.borrow()
        }
    }
}

fn descriptor_frame() -> String {
    serde_json::to_string(&json!({
        "type": "descriptor",
        "descriptor": capability_descriptor(),
    }))
    .expect("capability descriptor is serializable")
}

fn reply_result(request_id: &str, result: serde_json::Value) -> String {
    serde_json::to_string(&json!({
        "type": "outbound_result", "requestId": request_id, "result": result,
    }))
    .expect("outbound result is serializable")
}

fn reply_gateway_action_result(request_id: &str, result: GatewayActionResult) -> String {
    let mut frame = json!({
        "type": "outbound_result",
        "requestId": request_id,
        "result": result.result,
    });
    if let Some(result_id) = result.result_id {
        frame["resultId"] = json!(result_id);
    }
    serde_json::to_string(&frame).expect("outbound result is serializable")
}

fn reply_result_retirement(
    request_id: &str,
    result_id: u64,
    retirement: ResultRetirement,
) -> String {
    serde_json::to_string(&json!({
        "type": "outbound_result_retired",
        "requestId": request_id,
        "resultId": result_id,
        "result": {
            "success": true,
            "alreadyRetired": retirement == ResultRetirement::AlreadyRetired,
        },
    }))
    .expect("outbound result retirement is serializable")
}

fn reply_result_retirement_error(request_id: &str, result_id: u64, error: &str) -> String {
    serde_json::to_string(&json!({
        "type": "outbound_result_retired",
        "requestId": request_id,
        "resultId": result_id,
        "result": { "success": false, "error": error },
    }))
    .expect("outbound result retirement error is serializable")
}

fn reply_inbound_ack(message_id: &str, generation: u64, result: InboundAckResult) -> String {
    serde_json::to_string(&json!({
        "type": "inbound_acknowledged",
        "messageId": message_id,
        "generation": generation,
        "result": {
            "success": true,
            "alreadyAcknowledged": result == InboundAckResult::AlreadyAcknowledged,
        },
    }))
    .expect("inbound acknowledgement response is serializable")
}

fn reply_inbound_ack_error(message_id: &str, generation: u64, error: &str) -> String {
    serde_json::to_string(&json!({
        "type": "inbound_acknowledged",
        "messageId": message_id,
        "generation": generation,
        "result": { "success": false, "error": error },
    }))
    .expect("inbound acknowledgement response is serializable")
}

fn valid_outbound_request_id(frame: &serde_json::Value) -> Option<&str> {
    frame["requestId"]
        .as_str()
        .filter(|request_id| is_valid_opaque_id(request_id))
}

/// The WebSocket ingress path performs this before any durable queue or
/// request-ledger mutation. It reads the same registry that API lane creation
/// mutates, so a lane created by the API is immediately eligible for replies.
async fn reject_unknown_gateway_send_lane(
    state: &Arc<RelayState>,
    gateway_id: &str,
    raw: &str,
) -> Option<String> {
    let frame: serde_json::Value = serde_json::from_str(raw).ok()?;
    if frame["type"].as_str() != Some("outbound") || frame["action"]["op"].as_str() != Some("send")
    {
        return None;
    }
    let request_id = valid_outbound_request_id(&frame)?;
    if let Some(result) = state.live_outbound_result_for_request(gateway_id, request_id) {
        return Some(reply_gateway_action_result(
            request_id,
            GatewayActionResult::durable(&result),
        ));
    }
    let Some(chat_id) = frame["action"]["chat_id"]
        .as_str()
        .filter(|chat_id| !chat_id.trim().is_empty())
    else {
        // The normal frame handler produces the required-field response.
        return None;
    };
    match state.validate_lane_id(chat_id).await {
        Ok(true) => None,
        Ok(false) => Some(reply_result(
            request_id,
            json!({ "success": false, "error": format!("unknown lane {chat_id}") }),
        )),
        Err(error) => Some(reply_result(
            request_id,
            json!({ "success": false, "error" : error }),
        )),
    }
}

fn handle_gateway_frame(state: &Arc<RelayState>, raw: &str) -> Option<String> {
    // This direct/test helper is intentionally the unauthenticated development
    // namespace. Authenticated traffic must use the WebSocket path below.
    handle_gateway_frame_inner(state, raw, None, DEV_GATEWAY_ID)
}

fn handle_gateway_frame_for_gateway(
    state: &Arc<RelayState>,
    gateway_id: &str,
    raw: &str,
) -> Option<String> {
    handle_gateway_frame_inner(state, raw, None, gateway_id)
}

async fn handle_gateway_frame_for_connection(
    state: &Arc<RelayState>,
    connection_id: u64,
    gateway_id: &str,
    raw: &str,
) -> Option<String> {
    let owns_gateway = state
        .connections
        .lock()
        .unwrap()
        .outbound
        .as_ref()
        .is_some_and(|connection| {
            connection.id == connection_id && connection.gateway_id == gateway_id
        });
    if !owns_gateway || !state.is_active_connection(connection_id) {
        return None;
    }
    if let Some(reply) = reject_unknown_gateway_send_lane(state, gateway_id, raw).await {
        return Some(reply);
    }
    handle_gateway_frame_inner(state, raw, Some(connection_id), gateway_id)
}

fn handle_gateway_frame_inner(
    state: &Arc<RelayState>,
    raw: &str,
    connection_id: Option<u64>,
    gateway_id: &str,
) -> Option<String> {
    let v: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => {
            println!("relay: non-JSON frame: {}", truncate(raw));
            return None;
        }
    };

    let reply = match v["type"].as_str() {
        // handshake: gateway introduces itself (possibly several identities),
        // we answer with the capability descriptor it will front.
        Some("hello") => {
            println!(
                "relay: hello platform={} botId={}",
                v["platform"], v["botId"]
            );
            Some(descriptor_frame())
        }

        // A gateway may retry this acknowledgement until it receives the
        // retirement response. The matching durable result ID prevents a
        // delayed acknowledgement from deleting a later reuse of requestId.
        Some("outbound_result_ack") => {
            let request_id = match v["requestId"].as_str() {
                Some(request_id) if !is_valid_opaque_id(request_id) => {
                    return Some(reply_result(
                        "",
                        json!({ "success": false, "error": "requestId is required" }),
                    ));
                }
                Some(request_id) => request_id,
                None => {
                    return Some(reply_result(
                        "",
                        json!({ "success": false, "error": "requestId is required" }),
                    ));
                }
            };
            let result_id = match v["resultId"]
                .as_u64()
                .filter(|result_id| (1..=MAX_OUTBOUND_RESULT_ID).contains(result_id))
            {
                Some(result_id) => result_id,
                None => {
                    return Some(reply_result(
                        request_id,
                        json!({ "success": false, "error": RESULT_ID_REQUIRED_ERROR }),
                    ));
                }
            };
            let retired = match connection_id {
                Some(connection_id) => state.retire_outbound_result_if_active(
                    connection_id,
                    gateway_id,
                    request_id,
                    result_id,
                ),
                None => state
                    .retire_outbound_result(gateway_id, request_id, result_id)
                    .map(Some),
            };
            match retired {
                Ok(None) => return None,
                Ok(Some(retirement)) => {
                    Some(reply_result_retirement(request_id, result_id, retirement))
                }
                Err(error) if error.to_string() == RESULT_ID_MISMATCH_ERROR => Some(
                    reply_result_retirement_error(request_id, result_id, RESULT_ID_MISMATCH_ERROR),
                ),
                Err(error) => {
                    eprintln!("relay: could not durably retire outbound result: {error}");
                    Some(reply_result_retirement_error(
                        request_id,
                        result_id,
                        "result retirement persistence failed",
                    ))
                }
            }
        }

        // An inbound row is removed only by this exact identity pair. The
        // active connection check is repeated here so a superseded socket
        // cannot acknowledge a row after a successor has won ownership.
        Some("inbound_ack") => {
            let message_id = match v["messageId"].as_str() {
                Some(message_id) if is_valid_opaque_id(message_id) => message_id,
                _ => {
                    return Some(reply_inbound_ack_error(
                        "",
                        0,
                        INBOUND_ACK_MESSAGE_ID_REQUIRED_ERROR,
                    ));
                }
            };
            let generation = match v["generation"]
                .as_u64()
                .filter(|generation| (1..=MAX_SAFE_SEQUENCE).contains(generation))
            {
                Some(generation) => generation,
                None => {
                    return Some(reply_inbound_ack_error(
                        message_id,
                        0,
                        INBOUND_ACK_GENERATION_REQUIRED_ERROR,
                    ));
                }
            };
            if let Some(connection_id) = connection_id {
                if !state.is_active_connection(connection_id) {
                    return None;
                }
            }
            match state.acknowledge_inbound(message_id, generation) {
                Ok(result) => Some(reply_inbound_ack(message_id, generation, result)),
                Err(error) if error.to_string() == INBOUND_ACK_MISMATCH_ERROR => Some(
                    reply_inbound_ack_error(message_id, generation, INBOUND_ACK_MISMATCH_ERROR),
                ),
                Err(error) => {
                    eprintln!("relay: could not durably acknowledge inbound {message_id}: {error}");
                    Some(reply_inbound_ack_error(
                        message_id,
                        generation,
                        INBOUND_ACK_PERSISTENCE_ERROR,
                    ))
                }
            }
        }

        // §4 actions, each answered by requestId
        Some("outbound") => {
            let request_id = match v["requestId"].as_str() {
                Some(request_id) if !is_valid_opaque_id(request_id) => {
                    // Do not echo or copy an unbounded request ID into the
                    // response or durable ledger.
                    return Some(reply_result(
                        "",
                        json!({ "success": false, "error": "requestId exceeds maximum length" }),
                    ));
                }
                Some(request_id) => request_id,
                None => {
                    return Some(reply_result(
                        "",
                        json!({ "success": false, "error": "requestId is required" }),
                    ));
                }
            };
            // Request IDs identify a completed action, not its retry's
            // payload. Return the durable original before examining retry
            // fields so malformed retries cannot defeat idempotency.
            if let Some(result) = state.live_outbound_result_for_request(gateway_id, request_id) {
                return Some(reply_gateway_action_result(
                    request_id,
                    GatewayActionResult::durable(&result),
                ));
            }
            let action = &v["action"];
            let op = action["op"].as_str().unwrap_or_default();
            if op.len() > MAX_OUTBOUND_OPERATION_BYTES {
                return Some(reply_result(
                    request_id,
                    json!({ "success": false, "error": OUTBOUND_OPERATION_TOO_LONG_ERROR }),
                ));
            }
            let result = match op {
                "send" => {
                    let chat_id = match action["chat_id"].as_str() {
                        Some(chat_id) if !chat_id.trim().is_empty() => chat_id,
                        _ => {
                            return Some(reply_result(
                                request_id,
                                json!({ "success": false, "error": CHAT_ID_REQUIRED_ERROR }),
                            ));
                        }
                    };
                    let content = match action["content"].as_str() {
                        Some(content) if !content.trim().is_empty() => content,
                        _ => {
                            return Some(reply_result(
                                request_id,
                                json!({ "success": false, "error": CONTENT_REQUIRED_ERROR }),
                            ));
                        }
                    };
                    if let Err(error) = validate_delivery_content(content) {
                        return Some(reply_result(
                            request_id,
                            json!({ "success": false, "error": error.to_string() }),
                        ));
                    }
                    let reply_to = action["reply_to"].as_str();
                    if let Err(error) = validate_reply_to(reply_to) {
                        return Some(reply_result(
                            request_id,
                            json!({ "success": false, "error": error.to_string() }),
                        ));
                    }
                    let queued = match connection_id {
                        Some(connection_id) => state.queue_gateway_delivery_for_request_if_active(
                            connection_id,
                            gateway_id,
                            request_id,
                            chat_id,
                            content,
                            reply_to,
                        ),
                        None => state
                            .queue_gateway_delivery_for_request(
                                gateway_id, request_id, chat_id, content, reply_to,
                            )
                            .map(Some),
                    };
                    match queued {
                        Ok(None) => return None,
                        Ok(Some(result)) => result,
                        Err(error) => {
                            eprintln!("relay: send was not durably queued: {error}");
                            GatewayActionResult::transient(
                                json!({ "success": false, "error": "delivery persistence failed" }),
                            )
                        }
                    }
                }
                "typing" => {
                    let recorded = match connection_id {
                        Some(connection_id) => state.record_outbound_result_if_active(
                            connection_id,
                            gateway_id,
                            request_id,
                            json!({ "success": true }),
                        ),
                        None => state
                            .record_outbound_result(
                                gateway_id,
                                request_id,
                                json!({ "success": true }),
                            )
                            .map(Some),
                    };
                    match recorded {
                        Ok(None) => return None,
                        Ok(Some(result)) => result,
                        Err(error) => {
                            eprintln!("relay: could not durably record typing result: {error}");
                            GatewayActionResult::transient(
                                json!({ "success": false, "error": "result persistence failed" }),
                            )
                        }
                    }
                }
                other => {
                    println!("relay: op '{other}' not implemented — degraded success:false");
                    let recorded = match connection_id {
                        Some(connection_id) => state.record_outbound_result_if_active(
                            connection_id,
                            gateway_id,
                            request_id,
                            json!({ "success": false, "error": format!("op {other} not implemented") }),
                        ),
                        None => state
                            .record_outbound_result(
                                gateway_id,
                                request_id,
                                json!({ "success": false, "error": format!("op {other} not implemented") }),
                            )
                            .map(Some),
                    };
                    match recorded {
                        Ok(None) => return None,
                        Ok(Some(result)) => result,
                        Err(error) => {
                            eprintln!("relay: could not durably record outbound result: {error}");
                            GatewayActionResult::transient(
                                json!({ "success": false, "error": "result persistence failed" }),
                            )
                        }
                    }
                }
            };
            Some(reply_gateway_action_result(request_id, result))
        }

        other => {
            println!("relay: unknown frame type {other:?}: {}", truncate(raw));
            None
        }
    };

    reply
}

fn truncate(s: &str) -> &str {
    if s.len() <= 120 {
        return s;
    }
    let mut end = 0;
    for (index, ch) in s.char_indices() {
        let next = index + ch.len_utf8();
        if next > 120 {
            break;
        }
        end = next;
    }
    &s[..end]
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
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{input}:{sig}"))
    }

    fn mint_with_signature(payload: &str, exp: u64, signature: &str) -> String {
        use base64::Engine;
        let input = format!("{payload}:{exp}");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{input}:{signature}"))
    }

    #[test]
    fn valid_token_authenticates() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let token = mint("gw-1", exp, "s3cret");
        assert_eq!(
            verify_relay_token(&token, &["s3cret".into()]).unwrap(),
            "gw-1"
        );
    }

    #[test]
    fn wrong_secret_rejected() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let token = mint("gw-1", exp, "wrong");
        assert!(verify_relay_token(&token, &["s3cret".into()]).is_err());
    }

    #[test]
    fn uppercase_signature_is_accepted() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let input = format!("gw-1:{exp}");
        let mut mac = HmacSha256::new_from_slice(b"s3cret").unwrap();
        mac.update(input.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes()).to_uppercase();
        let token = mint_with_signature("gw-1", exp, &signature);

        assert_eq!(
            verify_relay_token(&token, &["s3cret".into()]).unwrap(),
            "gw-1"
        );
    }

    #[test]
    fn malformed_signature_is_rejected() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let too_long = "00".repeat(HmacSha256::output_size() + 1);
        for signature in [
            "a",       // odd number of hex digits
            "not-hex", // non-hex characters
            "00",      // too short
            &too_long, // too long
        ] {
            let token = mint_with_signature("gw-1", exp, signature);
            assert!(
                verify_relay_token(&token, &["s3cret".into()]).is_err(),
                "signature should be rejected: {signature}"
            );
        }
    }

    #[test]
    fn wrong_signature_is_rejected() {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let token = mint_with_signature("gw-1", exp, &"00".repeat(HmacSha256::output_size()));
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
            .as_secs()
            + 300;
        let token = mint("gw-2", exp, "new-secret");
        assert_eq!(
            verify_relay_token(&token, &["old".into(), "new-secret".into()]).unwrap(),
            "gw-2"
        );
    }

    #[test]
    fn descriptor_matches_contract_fields() {
        let d = capability_descriptor();
        assert_eq!(d["contract_version"], 3);
        assert_eq!(d["platform"], "relay");
        assert_eq!(d["markdown_dialect"], "plain");
        assert_eq!(d["supports_edit"], false);
        assert_eq!(d["outbound_result_ack_required"], true);
        assert_eq!(d["outbound_result_ack_type"], "outbound_result_ack");
        assert_eq!(d["inbound_handoff_version"], INBOUND_HANDOFF_VERSION);
        assert_eq!(d["inbound_ack_required"], true);
        assert_eq!(d["inbound_ack_type"], "inbound_ack");
    }

    #[test]
    fn message_event_payload_shape() {
        let ev = message_event("telepathy:direct", "direct", "hello", 7);
        assert_eq!(ev["text"], "hello");
        assert_eq!(ev["source"]["platform"], "relay");
        assert_eq!(ev["source"]["chat_id"], "telepathy:direct");
        assert_eq!(ev["source"]["chat_type"], "dm");
        assert_eq!(ev["message_id"], "tp-7");
    }
}
