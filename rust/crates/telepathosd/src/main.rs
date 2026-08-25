//! telepathosd — the steering-plane daemon.
//! v0: lane HTTP API (same endpoints as the Node bridge's api.ts).
//! Next: WS endpoint speaking the telepathos protocol, then the Hermes relay.

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use telepathos_lanes::{Lane,
    is_valid_lane_id, LaneCreateError, LaneRegistry, LaneRegistrySaveError,
    MAX_ENRICHED_LANE_TITLE_CODEPOINTS, MAX_ENRICHED_LANE_TITLE_UTF8_BYTES, MAX_LANE_COUNT,
    MAX_LANE_ID_LENGTH, MAX_LANE_NAME_UTF8_BYTES,
};
use telepathos_proto::{
    is_valid_opaque_id, MAX_OPAQUE_ID_BYTES, MAX_OPAQUE_ID_LENGTH, MAX_SAFE_SEQUENCE,
};
use tokio::sync::Mutex;

mod hermes_search;
mod ntfy;
mod relay;
mod transcript;

use relay::RelayState;
use transcript::TranscriptStore;

struct AppState {
    /// Shared with the relay so gateway `send` actions can validate their
    /// destination against this authoritative registry at ingress.
    reg: Arc<Mutex<LaneRegistry>>,
    path: PathBuf,
    relay: Arc<RelayState>,
    transcript: Arc<TranscriptStore>,
    msg_seq: std::sync::atomic::AtomicU64,
    registry_revision: std::sync::atomic::AtomicU64,
    /// A post-rename directory-sync failure means the lane snapshot may have
    /// committed. Preserve the matching live registry, but reject every later
    /// mutation until a restart can reconcile durable state.
    lane_persistence_uncertain: AtomicBool,
    /// A journal append failure after the journal file was opened may have
    /// written bytes. Do not append again in this process: startup
    /// reconciliation owns that ambiguity.
    interaction_persistence_uncertain: AtomicBool,
    interaction_ledger: Mutex<InteractionLedger>,
    interaction_ledger_path: PathBuf,
    #[cfg(test)]
    lane_save_fault: std::sync::Mutex<Option<LaneSaveFault>>,
}

/// The idempotency contract is deliberately bounded: producers may retry a
/// completed interaction for seven days from the immutable timestamp supplied
/// with that interaction. Older requests are rejected, never re-counted.
const INTERACTION_DEDUPE_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_INTERACTION_DEDUPE_ENTRIES: usize = 10_000;
const MAX_INTERACTION_JOURNAL_ENTRIES: usize = 128;
const MAX_INTERACTION_FUTURE_SKEW_MS: u64 = 5 * 60 * 1_000;
const INTERACTION_LEDGER_VERSION: u32 = 2;
/// A process generation occupies the high 32 revision bits. The low 32 bits
/// are reserved for mutations made by that process.
const REGISTRY_REVISION_GENERATION_STRIDE: u64 = 1u64 << 32;
const REGISTRY_REVISION_MUTATION_MASK: u64 = REGISTRY_REVISION_GENERATION_STRIDE - 1;
/// `MAX_SAFE_SEQUENCE` is `2^53 - 1`, so this leaves all low mutation bits
/// available in the final generation while staying JSON-exact.
const MAX_REGISTRY_REVISION_GENERATION: u64 =
    MAX_SAFE_SEQUENCE / REGISTRY_REVISION_GENERATION_STRIDE;
const API_TOKEN_DIGEST_BYTES: usize = 32;
const API_TOKEN_COMPARISON_CONTEXT: &[u8] = b"telepathosd/api-token-digest-compare/v1";
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionEntry {
    interaction_id: String,
    lane_id: String,
    interaction_created_at_ms: u64,
    lane_interactions: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionLedgerSnapshot {
    version: u32,
    entries: Vec<InteractionEntry>,
}

/// The snapshot is compacted periodically; each normal interaction is only a
/// synced append to the journal. `entries` is intentionally capped so both
/// memory and the snapshot have a fixed upper bound.
#[derive(Debug, Clone, Default)]
struct InteractionLedger {
    entries: std::collections::HashMap<String, InteractionEntry>,
    journal_entries: usize,
    /// Expired entries were removed from memory during recovery, but the
    /// durable snapshot/journal still contain them. A new interaction must
    /// not append until a compaction durably establishes the new generation.
    needs_durable_compaction: bool,
}

static INTERACTION_WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
#[derive(Debug)]
struct DirectorySyncFault {
    path: PathBuf,
}

#[cfg(test)]
static DIRECTORY_SYNC_FAULT: std::sync::Mutex<Option<DirectorySyncFault>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static DIRECTORY_SYNC_LOG: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
#[derive(Debug)]
struct InteractionJournalWriteFault {
    path: PathBuf,
    bytes_before_failure: usize,
}

#[cfg(test)]
static INTERACTION_JOURNAL_WRITE_FAULT: std::sync::Mutex<Option<InteractionJournalWriteFault>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
#[derive(Debug)]
struct InteractionJournalFileSyncFault {
    path: PathBuf,
}

#[cfg(test)]
static INTERACTION_JOURNAL_FILE_SYNC_FAULT: std::sync::Mutex<
    Option<InteractionJournalFileSyncFault>,
> = std::sync::Mutex::new(None);

#[derive(Debug)]
enum InteractionJournalAppendError {
    /// The journal line definitely was not reported as durable.
    Definite(anyhow::Error),
    /// The journal file was opened and an append write or file sync failed.
    /// Some or all of the entry may already be present, so retrying can
    /// duplicate an interaction or turn a partial line into corrupt NDJSON.
    AmbiguousPostOpen(anyhow::Error),
    /// The line's file sync succeeded, but its directory entry could not be
    /// synced. The line may survive a crash, so a same-process retry is unsafe.
    AmbiguousPostAppendDirectorySync(std::io::Error),
}

impl InteractionJournalAppendError {
    fn is_ambiguous(&self) -> bool {
        matches!(
            self,
            Self::AmbiguousPostOpen(_) | Self::AmbiguousPostAppendDirectorySync(_)
        )
    }
}

impl std::fmt::Display for InteractionJournalAppendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Definite(error) => error.fmt(formatter),
            Self::AmbiguousPostOpen(error) => write!(
                formatter,
                "interaction journal append may have written bytes before failing; restart telepathosd to reconcile before recording or retrying interactions: {error}"
            ),
            Self::AmbiguousPostAppendDirectorySync(error) => write!(
                formatter,
                "interaction journal append may have committed but its directory could not be synced: {error}"
            ),
        }
    }
}

impl std::error::Error for InteractionJournalAppendError {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let lanes_path = std::env::var("TELEPATHOS_LANES").unwrap_or_else(|_| "lanes.json".into());
    let transcript = Arc::new(TranscriptStore::load(PathBuf::from(
        std::env::var("TELEPATHOS_TRANSCRIPT").unwrap_or_else(|_| "transcript.json".into()),
    )));
    let relay = Arc::new(RelayState::default());
    let secrets: Vec<String> = std::env::var("TELEPATHOS_RELAY_SECRETS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let pending_path =
        PathBuf::from(std::env::var("TELEPATHOS_PENDING").unwrap_or_else(|_| "pending.json".into()));
    let bind = std::env::var("TELEPATHOS_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let lanes_path_buf = PathBuf::from(&lanes_path);
    let api_token_configured = std::env::var("TELEPATHOS_TOKEN")
        .map(|token| !token.is_empty())
        .unwrap_or(false);
    if api_token_required(&bind, api_token_configured) {
        return Err(anyhow::anyhow!(
            "TELEPATHOS_TOKEN is required when TELEPATHOS_BIND is non-loopback"
        ));
    }
    if relay_credentials_required(&bind, api_token_configured, secrets.is_empty()) {
        return Err(anyhow::anyhow!(
            "TELEPATHOS_RELAY_SECRETS is required when TELEPATHOS_TOKEN is set or TELEPATHOS_BIND is non-loopback"
        ));
    }
    let tls_cert = std::env::var("TELEPATHOS_TLS_CERT").ok();
    let tls_key = std::env::var("TELEPATHOS_TLS_KEY").ok();
    if tls_cert.is_some() != tls_key.is_some() {
        return Err(anyhow::anyhow!(
            "TELEPATHOS_TLS_CERT and TELEPATHOS_TLS_KEY must be configured together"
        ));
    }
    if !is_loopback_bind(&bind)
        && (api_token_configured || !secrets.is_empty())
        && tls_cert.is_none()
    {
        return Err(anyhow::anyhow!(
            "TELEPATHOS_TLS_CERT and TELEPATHOS_TLS_KEY are required for authenticated non-loopback endpoints"
        ));
    }
    let message_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let registry_revision_seed = next_registry_revision(&lanes_path_buf)?;

    let mut registry = LaneRegistry::load(&lanes_path_buf);
    let interaction_ledger_path = interaction_ledger_path(&lanes_path_buf);
    let (interaction_ledger, registry_reconciled) =
        load_interaction_ledger(&interaction_ledger_path, &mut registry)?;
    // A journal entry is written before the lane registry. If the daemon died
    // in that interval, replaying the journal above repairs the aggregate
    // count. Persist the repair before any later compaction can discard that
    // journal entry.
    let mut lane_persistence_uncertain = false;
    if registry_reconciled {
        let before_reconciliation = registry.clone();
        match registry.save(&lanes_path_buf) {
            Ok(()) => {}
            Err(error) if error.is_ambiguous() => {
                // The new snapshot may already be visible. Preserve the live
                // repair and start read-only rather than risk overwriting it.
                lane_persistence_uncertain = true;
            }
            Err(error) => {
                // The replacement definitely did not happen; do not carry a
                // repaired in-memory registry into a partially started daemon.
                registry = before_reconciliation;
                let restored_active_lane = registry.active().id.clone();
                return Err(anyhow::anyhow!(
                    "cannot persist recovered lane interactions for {restored_active_lane}: {error}"
                ));
            }
        }
    }
    let registry = Arc::new(Mutex::new(registry));
    relay.set_lane_registry(registry.clone());
    // The delivery snapshot is validated against the final lane registry.
    // Loading it earlier would allow an unknown lane or malformed delivery to
    // occupy durable capacity before the registry is available.
    relay.set_persist_path(&pending_path);
    let state = Arc::new(AppState {
        reg: registry,
        path: lanes_path_buf,
        relay: relay.clone(),
        transcript: transcript.clone(),
        // Time-seed IDs so a daemon restart cannot reuse tp-0 and collide
        // with a durable late reply from the previous process.
        msg_seq: std::sync::atomic::AtomicU64::new(message_seed),
        registry_revision: std::sync::atomic::AtomicU64::new(registry_revision_seed),
        lane_persistence_uncertain: AtomicBool::new(lane_persistence_uncertain),
        interaction_persistence_uncertain: AtomicBool::new(false),
        interaction_ledger: Mutex::new(interaction_ledger),
        interaction_ledger_path,
        #[cfg(test)]
        lane_save_fault: std::sync::Mutex::new(None),
    });

    // search backend: read-only FTS over the Hermes session store
    if let Ok(db) = std::env::var("TELEPATHOS_HERMES_STATE_DB") {
        telepathos_steering::set_search_backend(move |query| {
            hermes_search::search_sessions(&db, query, &[])
        });
    }

    let relay_router = relay::router(relay.clone(), secrets);

    let api_router = Router::new()
        .route("/api/state", get(get_state))
        .route("/api/pending", get(get_pending))
        .route("/api/pending/consume", post(consume_pending))
        .route("/api/message", post(post_message))
        // The authenticated cursor bootstrap is intentionally separate from
        // /api/delivery: it returns only the durable high-water mark and
        // never clones a potentially multi-megabyte pending queue.
        .route("/api/delivery/head", get(get_delivery_head))
        .route("/api/delivery", get(get_delivery))
        .route("/api/lanes", post(create_lane))
        .route("/api/lanes/active", post(set_active))
        .route("/api/lanes/touch", post(touch))
        .route("/api/lanes/interaction", post(record_interaction))
        .route("/api/meta", post(meta))
        .layer(middleware::from_fn(api_auth));
    let app = api_router
        .nest_service("/relay", relay_router)
        .with_state(state);

    let port = std::env::var("TELEPATHOS_API_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8790);
    let addr = parse_bind_addr(&bind, port)?;
    if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
        println!("telepathosd lane API on https://{addr}");
        axum_server::bind_rustls(addr, tls_config)
            .serve(app.into_make_service())
            .await?;
    } else {
        println!("telepathosd lane API on http://{addr}");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
    }
    Ok(())
}

fn is_loopback_bind(bind: &str) -> bool {
    matches!(bind, "127.0.0.1" | "::1" | "[::1]" | "localhost")
}

fn parse_bind_addr(bind: &str, port: u16) -> anyhow::Result<SocketAddr> {
    let address = if bind == "localhost" {
        format!("127.0.0.1:{port}")
    } else if bind.starts_with('[') {
        format!("{bind}:{port}")
    } else if bind.contains(':') {
        format!("[{bind}]:{port}")
    } else {
        format!("{bind}:{port}")
    };
    address
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid TELEPATHOS_BIND {bind:?}: {error}"))
}

fn api_token_required(bind: &str, configured: bool) -> bool {
    !is_loopback_bind(bind) && !configured
}

fn relay_credentials_required(bind: &str, api_token_configured: bool, secrets_empty: bool) -> bool {
    secrets_empty && (api_token_configured || !is_loopback_bind(bind))
}

fn interaction_ledger_path(lanes_path: &Path) -> PathBuf {
    let mut path = lanes_path.to_path_buf();
    let name = lanes_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("lanes.json");
    path.set_file_name(format!("{name}.interaction-ledger.json"));
    path
}

fn interaction_journal_path(ledger_path: &Path) -> PathBuf {
    let mut path = ledger_path.to_path_buf();
    let name = ledger_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("interaction-ledger.json");
    path.set_file_name(format!("{name}.journal.ndjson"));
    path
}

fn create_private_file(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn containing_directory(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Create a nested state directory one component at a time. Every directory
/// entry created here is synced through its parent before a caller writes a
/// supposedly durable state file beneath it.
fn create_state_directory_durably(directory: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut current = if directory.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        directory.to_path_buf()
    };

    loop {
        match fs::metadata(&current) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("state directory {} is not a directory", current.display()),
                    ));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(current.clone());
                let parent = containing_directory(&current);
                if parent == current {
                    return Err(std::io::Error::other(format!(
                        "cannot find an existing ancestor for state directory {}",
                        directory.display()
                    )));
                }
                current = parent;
            }
            Err(error) => return Err(error),
        }
    }

    for new_directory in missing.iter().rev() {
        match fs::create_dir(new_directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !fs::metadata(new_directory)?.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "state directory {} is not a directory",
                            new_directory.display()
                        ),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        sync_directory(&containing_directory(new_directory))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    {
        let mut fault = DIRECTORY_SYNC_FAULT.lock().unwrap();
        if fault.as_ref().is_some_and(|fault| fault.path == path) {
            *fault = None;
            return Err(std::io::Error::other("injected directory sync failure"));
        }
    }

    fs::File::open(path)?.sync_all()?;

    #[cfg(test)]
    DIRECTORY_SYNC_LOG.lock().unwrap().push(path.to_path_buf());

    Ok(())
}

fn atomic_write_text(path: &Path, contents: &str) -> anyhow::Result<()> {
    let parent = containing_directory(path);
    create_state_directory_durably(&parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("interaction-ledger.json");
    let nonce = INTERACTION_WRITE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".{name}.tmp-{}-{nonce}", std::process::id()));
    let result = (|| {
        let mut file = create_private_file(&temp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        sync_directory(&parent)?;
        Ok::<_, std::io::Error>(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temp);
        return Err(error.into());
    }
    Ok(())
}

fn persist_interaction_ledger(path: &Path, ledger: &InteractionLedger) -> anyhow::Result<()> {
    ledger.validate()?;
    let mut entries: Vec<_> = ledger.entries.values().cloned().collect();
    entries.sort_by(|left, right| {
        left.interaction_created_at_ms
            .cmp(&right.interaction_created_at_ms)
            .then_with(|| left.interaction_id.cmp(&right.interaction_id))
    });
    let json = serde_json::to_string_pretty(&InteractionLedgerSnapshot {
        version: INTERACTION_LEDGER_VERSION,
        entries,
    })?;
    atomic_write_text(path, &json)
}

fn write_interaction_journal_line<W: Write>(
    writer: &mut W,
    entry: &InteractionEntry,
) -> Result<(), InteractionJournalAppendError> {
    serde_json::to_writer(&mut *writer, entry)
        .map_err(|error| InteractionJournalAppendError::AmbiguousPostOpen(error.into()))?;
    writer
        .write_all(b"\n")
        .map_err(|error| InteractionJournalAppendError::AmbiguousPostOpen(error.into()))
}

#[cfg(test)]
struct PartialJournalWriter<'a> {
    file: &'a mut fs::File,
    bytes_before_failure: usize,
}

#[cfg(test)]
impl Write for PartialJournalWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes_before_failure == 0 {
            return Err(std::io::Error::other(
                "injected partial journal append failure",
            ));
        }
        let allowed = self.bytes_before_failure.min(bytes.len());
        let written = self.file.write(&bytes[..allowed])?;
        self.bytes_before_failure = self.bytes_before_failure.saturating_sub(written);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
fn take_interaction_journal_write_fault(path: &Path) -> Option<usize> {
    let mut fault = INTERACTION_JOURNAL_WRITE_FAULT.lock().unwrap();
    if fault.as_ref().is_some_and(|fault| fault.path == path) {
        return fault.take().map(|fault| fault.bytes_before_failure);
    }
    None
}

#[cfg(test)]
fn write_interaction_journal_line_with_fault(
    file: &mut fs::File,
    path: &Path,
    entry: &InteractionEntry,
) -> Result<(), InteractionJournalAppendError> {
    match take_interaction_journal_write_fault(path) {
        Some(bytes_before_failure) => {
            let mut writer = PartialJournalWriter {
                file,
                bytes_before_failure,
            };
            write_interaction_journal_line(&mut writer, entry)
        }
        None => write_interaction_journal_line(file, entry),
    }
}

#[cfg(test)]
fn sync_interaction_journal_file(file: &fs::File, path: &Path) -> std::io::Result<()> {
    let mut fault = INTERACTION_JOURNAL_FILE_SYNC_FAULT.lock().unwrap();
    if fault.as_ref().is_some_and(|fault| fault.path == path) {
        *fault = None;
        return Err(std::io::Error::other("injected journal file sync failure"));
    }
    drop(fault);
    file.sync_all()
}

#[cfg(not(test))]
fn sync_interaction_journal_file(file: &fs::File, _path: &Path) -> std::io::Result<()> {
    file.sync_all()
}

fn append_interaction_journal(
    path: &Path,
    entry: &InteractionEntry,
) -> Result<(), InteractionJournalAppendError> {
    validate_interaction_entry(entry, path).map_err(InteractionJournalAppendError::Definite)?;
    let parent = containing_directory(path);
    create_state_directory_durably(&parent)
        .map_err(|error| InteractionJournalAppendError::Definite(error.into()))?;
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|error| InteractionJournalAppendError::Definite(error.into()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = file
            .metadata()
            .map_err(|error| InteractionJournalAppendError::Definite(error.into()))?
            .permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions)
            .map_err(|error| InteractionJournalAppendError::Definite(error.into()))?;
    }
    #[cfg(test)]
    write_interaction_journal_line_with_fault(&mut file, path, entry)?;
    #[cfg(not(test))]
    write_interaction_journal_line(&mut file, entry)?;
    sync_interaction_journal_file(&file, path)
        .map_err(|error| InteractionJournalAppendError::AmbiguousPostOpen(error.into()))?;
    sync_directory(&parent)
        .map_err(InteractionJournalAppendError::AmbiguousPostAppendDirectorySync)?;
    Ok(())
}

fn compact_interaction_ledger(path: &Path, ledger: &mut InteractionLedger) -> anyhow::Result<()> {
    persist_interaction_ledger(path, ledger)?;
    atomic_write_text(&interaction_journal_path(path), "")?;
    ledger.journal_entries = 0;
    ledger.needs_durable_compaction = false;
    Ok(())
}

/// Remove expired dedupe entries only by first replacing the snapshot and
/// clearing the journal. This is an admission boundary: after it succeeds, a
/// crash can observe either the old generation, the new generation, or both,
/// but never an old durable ID alongside a newly appended reuse of that ID.
///
/// Work on a clone so a failed or ambiguous compaction leaves the in-memory
/// ledger at its previous generation and prevents the caller from appending.
fn compact_expired_interactions_for_admission(
    path: &Path,
    ledger: &mut InteractionLedger,
    oldest_supported_timestamp_ms: u64,
) -> anyhow::Result<()> {
    let mut compacted = ledger.clone();
    let pruned = compacted.prune_expired(oldest_supported_timestamp_ms);
    if !pruned && !compacted.needs_durable_compaction {
        return Ok(());
    }
    compact_interaction_ledger(path, &mut compacted)?;
    *ledger = compacted;
    Ok(())
}

fn parse_interaction_snapshot(path: &Path) -> anyhow::Result<Vec<InteractionEntry>> {
    let json = fs::read_to_string(path)?;
    let value: serde_json::Value = serde_json::from_str(&json).map_err(|error| {
        anyhow::anyhow!("corrupt interaction ledger {}: {error}", path.display())
    })?;
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    if version != Some(INTERACTION_LEDGER_VERSION.into()) {
        return Err(anyhow::anyhow!(
            "interaction ledger {} uses an unsupported format; telepathosd now requires version {} (hard cutover)",
            path.display(),
            INTERACTION_LEDGER_VERSION
        ));
    }
    let snapshot: InteractionLedgerSnapshot = serde_json::from_value(value).map_err(|error| {
        anyhow::anyhow!("corrupt interaction ledger {}: {error}", path.display())
    })?;
    Ok(snapshot.entries)
}

/// Pure fold step: apply one journal line to an in-memory ledger.
/// No filesystem access — crash-interleaving properties are testable here.
fn fold_journal_entry(
    mut ledger: InteractionLedger,
    line: &str,
    line_number: usize,
    journal_label: &str,
) -> anyhow::Result<InteractionLedger> {
    if line.trim().is_empty() {
        anyhow::bail!(
            "corrupt interaction journal {journal_label}: blank entry at line {line_number}"
        );
    }
    ledger.journal_entries += 1;
    if ledger.journal_entries > MAX_INTERACTION_JOURNAL_ENTRIES {
        anyhow::bail!(
            "interaction journal {journal_label} exceeds its {}-entry compaction bound",
            MAX_INTERACTION_JOURNAL_ENTRIES
        );
    }
    let entry: InteractionEntry = serde_json::from_str(line).map_err(|error| {
        anyhow::anyhow!(
            "corrupt interaction journal {journal_label} at line {line_number}: {error}"
        )
    })?;
    insert_interaction_entry(&mut ledger.entries, entry, Path::new(journal_label))?;
    Ok(ledger)
}

/// Pure reconciliation: raise each lane's interaction count to the ledger
/// max. Returns true when any lane was moved (caller persists that).
fn reconcile_lane_interactions(
    lanes: &mut [Lane],
    entries: &std::collections::HashMap<String, InteractionEntry>,
) -> bool {
    let mut reconciled = false;
    for entry in entries.values() {
        if let Some(lane) = lanes.iter_mut().find(|lane| lane.id == entry.lane_id) {
            let current = lane.interactions.unwrap_or(0);
            if entry.lane_interactions > current {
                lane.interactions = Some(entry.lane_interactions);
                reconciled = true;
            }
        }
    }
    reconciled
}

fn insert_interaction_entry(
    entries: &mut std::collections::HashMap<String, InteractionEntry>,
    entry: InteractionEntry,
    path: &Path,
) -> anyhow::Result<()> {
    validate_interaction_entry(&entry, path)?;
    match entries.get(&entry.interaction_id) {
        Some(existing) if existing == &entry => Ok(()),
        Some(_) => Err(anyhow::anyhow!(
            "corrupt interaction ledger {}: interaction_id {} has conflicting records",
            path.display(),
            entry.interaction_id
        )),
        None => {
            entries.insert(entry.interaction_id.clone(), entry);
            Ok(())
        }
    }
}

fn load_interaction_ledger(
    path: &Path,
    registry: &mut LaneRegistry,
) -> anyhow::Result<(InteractionLedger, bool)> {
    registry.validate().map_err(|error| {
        anyhow::anyhow!("invalid lane registry during interaction recovery: {error}")
    })?;
    let mut ledger = InteractionLedger::default();
    match parse_interaction_snapshot(path) {
        Ok(entries) => {
            for entry in entries {
                insert_interaction_entry(&mut ledger.entries, entry, path)?;
            }
        }
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) => {}
        Err(error) => return Err(error),
    }

    let journal_path = interaction_journal_path(path);
    match fs::File::open(&journal_path) {
        Ok(file) => {
            // Reconciliation-as-fold: recovery is a left fold of journal
            // lines over the snapshot state; any crash truncation yields a
            // prefix-consistent ledger.
            for (index, line) in BufReader::new(file).lines().enumerate() {
                ledger = fold_journal_entry(
                    ledger,
                    &line?,
                    index + 1,
                    &journal_path.display().to_string(),
                )?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let now = unix_time_millis()?;
    if ledger.prune_expired(now.saturating_sub(INTERACTION_DEDUPE_WINDOW_MS)) {
        // Startup must not rewrite the ledger before it has durably saved any
        // registry reconciliation. Defer that rewrite to the next admission,
        // which runs only after initialization has completed.
        ledger.needs_durable_compaction = true;
    }
    if ledger.entries.len() > MAX_INTERACTION_DEDUPE_ENTRIES {
        return Err(anyhow::anyhow!(
            "interaction ledger {} exceeds its {}-entry dedupe bound",
            path.display(),
            MAX_INTERACTION_DEDUPE_ENTRIES
        ));
    }
    for entry in ledger.entries.values() {
        if !registry.lanes.iter().any(|lane| lane.id == entry.lane_id) {
            anyhow::bail!(
                "interaction ledger {} references unknown lane {}",
                path.display(),
                entry.lane_id
            );
        }
    }
    let registry_reconciled =
        reconcile_lane_interactions(&mut registry.lanes, &ledger.entries);
    Ok((ledger, registry_reconciled))
}

impl InteractionLedger {
    fn validate(&self) -> anyhow::Result<()> {
        for entry in self.entries.values() {
            validate_interaction_entry(entry, Path::new("in-memory interaction ledger"))?;
        }
        Ok(())
    }

    fn prune_expired(&mut self, oldest_supported_timestamp_ms: u64) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| entry.interaction_created_at_ms >= oldest_supported_timestamp_ms);
        self.entries.len() != before
    }
}

fn validate_interaction_entry(entry: &InteractionEntry, path: &Path) -> anyhow::Result<()> {
    if !is_valid_opaque_id(&entry.interaction_id) {
        return Err(anyhow::anyhow!(
            "interaction ledger {} contains an invalid interaction_id",
            path.display()
        ));
    }
    if !is_valid_lane_id(&entry.lane_id) {
        return Err(anyhow::anyhow!(
            "interaction ledger {} contains invalid lane id",
            path.display()
        ));
    }
    if entry.lane_interactions > MAX_SAFE_SEQUENCE {
        return Err(anyhow::anyhow!(
            "interaction ledger {} contains lane interaction count {} outside the JSON-safe limit",
            path.display(),
            entry.lane_interactions
        ));
    }
    Ok(())
}

fn lane_interaction_count(registry: &LaneRegistry, lane_id: &str) -> u64 {
    registry
        .lanes
        .iter()
        .find(|lane| lane.id == lane_id)
        .and_then(|lane| lane.interactions)
        .unwrap_or(0)
}

fn max_journaled_interaction_count(
    ledger: &InteractionLedger,
    lane_id: &str,
    oldest_supported_timestamp_ms: Option<u64>,
) -> u64 {
    ledger
        .entries
        .values()
        .filter(|entry| entry.lane_id == lane_id)
        .filter(|entry| {
            oldest_supported_timestamp_ms
                .map_or(true, |oldest| entry.interaction_created_at_ms >= oldest)
        })
        .map(|entry| entry.lane_interactions)
        .max()
        .unwrap_or(0)
}

fn next_interaction_count(registry_count: u64, journaled_count: u64) -> anyhow::Result<u64> {
    registry_count
        .max(journaled_count)
        .checked_add(1)
        .filter(|count| *count <= MAX_SAFE_SEQUENCE)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "interaction count would exceed the JSON-safe limit of {MAX_SAFE_SEQUENCE}"
            )
        })
}

fn unix_time_millis() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("system clock is before the Unix epoch: {error}"))?
        .as_millis()
        .try_into()
        .map_err(|_| anyhow::anyhow!("Unix time in milliseconds overflowed u64"))?)
}

fn registry_generation_path(lanes_path: &Path) -> PathBuf {
    let mut generation_path = lanes_path.to_path_buf();
    let name = lanes_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("lanes.json");
    generation_path.set_file_name(format!("{name}.revision-generation"));
    generation_path
}

/// Allocate a new process generation for lane revisions. The generation is
/// persisted before serving requests; mutation counters occupy the low 32 bits
/// so a daemon restart cannot reuse a revision from the previous process.
fn next_registry_revision(lanes_path: &Path) -> anyhow::Result<u64> {
    let generation_path = registry_generation_path(lanes_path);
    let name = generation_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("lanes.json")
        .trim_end_matches(".revision-generation");
    let generation = match fs::read_to_string(&generation_path) {
        Ok(text) => text
            .trim()
            .parse::<u64>()
            .map_err(|error| anyhow::anyhow!("invalid registry revision generation: {error}"))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    if generation > MAX_REGISTRY_REVISION_GENERATION {
        return Err(anyhow::anyhow!(
            "registry revision generation exceeds the JSON-safe limit"
        ));
    }
    let next_generation = generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("registry revision generation exhausted"))?;
    if next_generation > MAX_REGISTRY_REVISION_GENERATION {
        return Err(anyhow::anyhow!(
            "registry revision generation exhausted at the JSON-safe limit"
        ));
    }
    let revision_base = next_generation
        .checked_mul(REGISTRY_REVISION_GENERATION_STRIDE)
        .filter(|revision| *revision <= MAX_SAFE_SEQUENCE)
        .ok_or_else(|| anyhow::anyhow!("registry revision generation overflowed"))?;
    let parent = containing_directory(&generation_path);
    create_state_directory_durably(&parent)?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp = parent.join(format!(
        ".{}-revision.tmp-{}-{nonce}",
        name,
        std::process::id()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(next_generation.to_string().as_bytes())?;
        file.sync_all()?;
        fs::rename(&temp, &generation_path)?;
        sync_directory(&parent)
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temp);
        return Err(error.into());
    }
    Ok(revision_base)
}

/// Advance a process-local lane revision without crossing into the next
/// persisted generation or leaving JavaScript's exact-integer range. A caller
/// must abandon its registry mutation when this fails, because emitting a
/// reused or rounded revision would make remote capture unsafe.
fn advance_registry_revision(revision: &AtomicU64) -> anyhow::Result<u64> {
    revision
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            if current > MAX_SAFE_SEQUENCE
                || current & REGISTRY_REVISION_MUTATION_MASK == REGISTRY_REVISION_MUTATION_MASK
            {
                return None;
            }
            current.checked_add(1).filter(|next| *next <= MAX_SAFE_SEQUENCE)
        })
        .map(|previous| previous + 1)
        .map_err(|current| {
            anyhow::anyhow!(
                "registry revision exhausted at {current}; restart is required before another lane mutation"
            )
        })
}

#[derive(Debug)]
enum LaneMutationError {
    PersistenceUncertain,
    DefiniteSaveFailure(LaneRegistrySaveError),
    AmbiguousSaveFailure(LaneRegistrySaveError),
}

impl std::fmt::Display for LaneMutationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PersistenceUncertain => write!(
                formatter,
                "lane persistence is uncertain after a prior post-rename failure; restart telepathosd to reconcile before changing lanes"
            ),
            Self::DefiniteSaveFailure(error) => write!(formatter, "lane change was rolled back: {error}"),
            Self::AmbiguousSaveFailure(error) => write!(
                formatter,
                "lane change may have committed and further lane writes are blocked until restart: {error}"
            ),
        }
    }
}

impl std::error::Error for LaneMutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DefiniteSaveFailure(error) | Self::AmbiguousSaveFailure(error) => Some(error),
            Self::PersistenceUncertain => None,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum LaneSaveFault {
    PreRename,
    PostRename,
}

fn save_lane_registry(
    state: &AppState,
    registry: &LaneRegistry,
) -> Result<(), LaneRegistrySaveError> {
    #[cfg(test)]
    if let Some(fault) = state.lane_save_fault.lock().unwrap().take() {
        let error = std::io::Error::other("injected lane persistence failure");
        return Err(match fault {
            LaneSaveFault::PreRename => LaneRegistrySaveError::PreRename(error),
            LaneSaveFault::PostRename => LaneRegistrySaveError::AmbiguousPostRename(error),
        });
    }
    registry.save(&state.path)
}

fn require_lane_persistence_available(state: &AppState) -> Result<(), LaneMutationError> {
    if state.lane_persistence_uncertain.load(Ordering::SeqCst) {
        Err(LaneMutationError::PersistenceUncertain)
    } else {
        Ok(())
    }
}

fn require_interaction_persistence_available(state: &AppState) -> anyhow::Result<()> {
    if state
        .interaction_persistence_uncertain
        .load(Ordering::SeqCst)
    {
        Err(anyhow::anyhow!(
            "interaction journal persistence is uncertain after a prior append failure that may have written bytes; restart telepathosd to reconcile before recording or retrying interactions"
        ))
    } else {
        Ok(())
    }
}

/// Persist a registry mutation while its mutex is held. A failure before
/// `rename` has a definite old on-disk snapshot, so restoring the old live
/// registry and revision is safe. A failure after `rename` is ambiguous, so
/// keep the new live registry and fence every subsequent write.
fn commit_lane_mutation(
    state: &AppState,
    registry: &mut LaneRegistry,
    before: LaneRegistry,
    revision_before: u64,
) -> Result<(), LaneMutationError> {
    require_lane_persistence_available(state)?;
    match save_lane_registry(state, registry) {
        Ok(()) => Ok(()),
        Err(error) if error.is_ambiguous() => {
            state
                .lane_persistence_uncertain
                .store(true, Ordering::SeqCst);
            Err(LaneMutationError::AmbiguousSaveFailure(error))
        }
        Err(error) => {
            *registry = before;
            state
                .registry_revision
                .store(revision_before, Ordering::SeqCst);
            Err(LaneMutationError::DefiniteSaveFailure(error))
        }
    }
}

fn lane_error_response(status: StatusCode, error: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": error.into() }))).into_response()
}

/// A full lane registry is a permanent caller condition, not a persistence
/// failure. Keep it distinct from the 500/503 durable-write mapping.
fn lane_create_error_response(error: LaneCreateError) -> Response {
    let status = match error {
        LaneCreateError::CapacityReached => StatusCode::CONFLICT,
        LaneCreateError::BlankName | LaneCreateError::InvalidGeneratedId => StatusCode::BAD_REQUEST,
    };
    lane_error_response(status, error.to_string())
}

/// A definite persistence failure has a known rollback and is an internal
/// server error. An ambiguous failure, or a process already latched against
/// further writes, is temporarily unavailable until restart/reconciliation.
fn lane_mutation_error_response(error: LaneMutationError) -> Response {
    lane_error_response(lane_mutation_error_status(&error), error.to_string())
}

/// A definite save failure is safe to roll back; all ambiguous or fenced
/// states require a restart/reconciliation before another write is accepted.
/// Keep this mapping shared by the lane, interaction, and meta APIs.
fn lane_mutation_error_status(error: &LaneMutationError) -> StatusCode {
    match error {
        LaneMutationError::PersistenceUncertain | LaneMutationError::AmbiguousSaveFailure(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        LaneMutationError::DefiniteSaveFailure(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Protect the HTTP steering API when a shared token is configured. The relay
/// has its own gateway secret and is intentionally outside this middleware.
async fn api_auth(request: Request, next: Next) -> Response {
    let expected = std::env::var("TELEPATHOS_TOKEN").unwrap_or_default();
    if expected.is_empty() {
        return next.run(request).await;
    }

    if api_token_matches(request.headers(), &expected) {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

fn api_token_matches(headers: &HeaderMap, expected: &str) -> bool {
    let supplied = headers
        .get("x-telepathos-token")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        });
    let expected_digest = api_token_digest(expected);
    supplied.is_some_and(|supplied| {
        api_token_digests_match(&expected_digest, &api_token_digest(supplied))
    })
}

/// Hash the raw token before comparison, so this API never performs a
/// prefix-sensitive equality check over bearer/header secrets.
fn api_token_digest(token: &str) -> [u8; API_TOKEN_DIGEST_BYTES] {
    Sha256::digest(token.as_bytes()).into()
}

/// Compare fixed-size token digests through `Mac::verify_slice`, which uses a
/// constant-time tag comparison. HMAC keys and values are both the fixed-size
/// digests, so no raw token participates in the comparison stage.
fn api_token_digests_match(
    expected: &[u8; API_TOKEN_DIGEST_BYTES],
    supplied: &[u8; API_TOKEN_DIGEST_BYTES],
) -> bool {
    let mut expected_mac = HmacSha256::new_from_slice(expected)
        .expect("SHA-256 token digest is always a valid HMAC key");
    expected_mac.update(API_TOKEN_COMPARISON_CONTEXT);
    let expected_tag = expected_mac.finalize().into_bytes();

    let mut supplied_mac = HmacSha256::new_from_slice(supplied)
        .expect("SHA-256 token digest is always a valid HMAC key");
    supplied_mac.update(API_TOKEN_COMPARISON_CONTEXT);
    supplied_mac.verify_slice(&expected_tag).is_ok()
}

/// Pending (undelivered) items for the ACTIVE lane — phone checks on mic-open.
async fn get_pending(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let (lane_id, revision) = {
        let reg = state.reg.lock().await;
        (
            reg.active().id.clone(),
            state
                .registry_revision
                .load(std::sync::atomic::Ordering::SeqCst),
        )
    };
    let items = state.relay.pending_for(&lane_id);
    Json(serde_json::json!({
        "lane_id": lane_id,
        "revision": revision,
        "count": items.len(),
        "items": items,
    }))
}

fn lane_id_is_non_blank(lane_id: &str) -> bool {
    is_valid_lane_id(lane_id)
}

/// POST /api/pending/consume — acknowledge exactly the pending rows whose
/// speech completed. This deliberately does not accept a through-sequence:
/// `/api/pending` can hide receipt-owned correlated rows from normal Android
/// narration, and a broad cursor could delete them without speaking them.
async fn consume_pending(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let lane_id = body["lane_id"].as_str().unwrap_or_default().to_string();
    if !lane_id_is_non_blank(&lane_id) {
        return (
            StatusCode::BAD_REQUEST,
            "lane_id must match the lane ID grammar",
        )
            .into_response();
    }
    let Some(raw_sequences) = body.get("sequences").and_then(serde_json::Value::as_array) else {
        return (StatusCode::BAD_REQUEST, "sequences array required").into_response();
    };
    if raw_sequences.is_empty() {
        return (StatusCode::BAD_REQUEST, "at least one sequence is required").into_response();
    }
    if raw_sequences.len() > relay::MAX_PENDING_DELIVERIES {
        return (StatusCode::BAD_REQUEST, "too many sequences").into_response();
    }
    let mut seen = HashSet::with_capacity(raw_sequences.len());
    let mut sequences = Vec::with_capacity(raw_sequences.len());
    for raw_sequence in raw_sequences {
        let Some(sequence) = raw_sequence.as_u64() else {
            return (
                StatusCode::BAD_REQUEST,
                "sequences must be unsigned integers",
            )
                .into_response();
        };
        if sequence == 0 || sequence > MAX_SAFE_SEQUENCE {
            return (
                StatusCode::BAD_REQUEST,
                "sequences must be non-zero JSON-safe integers",
            )
                .into_response();
        }
        if !seen.insert(sequence) {
            return (StatusCode::BAD_REQUEST, "sequences must be distinct").into_response();
        }
        sequences.push(sequence);
    }
    let known_lane = {
        let reg = state.reg.lock().await;
        reg.lanes.iter().any(|lane| lane.id == lane_id)
    };
    if !known_lane {
        return (StatusCode::NOT_FOUND, format!("unknown lane {lane_id}")).into_response();
    }
    match state.relay.consume_lane_sequences(&lane_id, &sequences) {
        Ok(count) => Json(serde_json::json!({ "ok": true, "lane_id": lane_id, "count": count }))
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// Phone bridge → lane: wrap as MessageEvent and push to the gateway.
/// 400 missing text · 404 unknown lane · 413 permanently oversized record ·
/// 503 no gateway dialed in or transient relay failure.
async fn post_message(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let lane_id = match body.get("lane_id") {
        None => "telepathos:direct".to_string(),
        Some(value) => match value.as_str().filter(|lane_id| is_valid_lane_id(lane_id)) {
            Some(lane_id) => lane_id.to_string(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    "lane_id must match the lane ID grammar",
                )
                    .into_response();
            }
        },
    };
    let text = body["text"].as_str().unwrap_or_default().to_string();
    if text.is_empty() {
        return (StatusCode::BAD_REQUEST, "text required").into_response();
    }
    let lane_name = {
        let reg = state.reg.lock().await;
        match reg.lanes.iter().find(|l| l.id == lane_id) {
            Some(l) => l.name.clone(),
            None => {
                return (StatusCode::NOT_FOUND, format!("unknown lane {lane_id}")).into_response()
            }
        }
    };

    // Preflight against the exact next message identity before allocating it.
    // A compare-exchange retry keeps concurrent callers from sharing an ID;
    // the relay repeats this same size check under its queue lock.
    let event = loop {
        let seq = state.msg_seq.load(Ordering::SeqCst);
        let next_seq = match seq.checked_add(1) {
            Some(next_seq) => next_seq,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "message sequence limit reached",
                )
                    .into_response()
            }
        };
        let event = relay::message_event(&lane_id, &lane_name, &text, seq);
        if let Err(error) = state.relay.preflight_inbound(&event) {
            let status = if relay::is_inbound_record_too_large(&error) {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            return (status, error.to_string()).into_response();
        }
        if state
            .msg_seq
            .compare_exchange(seq, next_seq, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            break event;
        }
    };
    let message_id = event["message_id"].as_str().unwrap_or_default();

    match state
        .relay
        .push_inbound_with_request(&lane_id, &event)
        .await
    {
        Ok(()) => Json(serde_json::json!({"ok": true, "message_id": message_id})).into_response(),
        Err(e) => {
            let status = if relay::is_inbound_record_too_large(&e) {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (status, e.to_string()).into_response()
        }
    }
}

/// Phone bridge polls for gateway replies (chat_id-filtered by caller).
#[derive(serde::Deserialize)]
struct DeliveryQuery {
    #[serde(default)]
    after: u64,
    /// true → remove returned entries (phone has taken responsibility)
    #[serde(default)]
    consume: bool,
    /// Required when consuming: only this lane may be removed.
    lane_id: Option<String>,
    /// When present, return/consume only the reply to this inbound message.
    reply_to: Option<String>,
    /// Upper sequence boundary for an explicit handset acknowledgement.
    through_seq: Option<u64>,
}

/// Return a bounded delivery cursor for a pre-submit handshake. This route is
/// behind the same API authentication middleware as every other /api endpoint.
async fn get_delivery_head(State(state): State<Arc<AppState>>) -> Response {
    match state.relay.delivery_head() {
        Ok(latest) => Json(serde_json::json!({ "latest": latest })).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn get_delivery(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<DeliveryQuery>,
) -> Response {
    if q.lane_id
        .as_deref()
        .is_some_and(|lane_id| !is_valid_lane_id(lane_id))
    {
        return (
            StatusCode::BAD_REQUEST,
            "lane_id must match the lane ID grammar",
        )
            .into_response();
    }
    if q.after > MAX_SAFE_SEQUENCE {
        return (
            StatusCode::BAD_REQUEST,
            "after exceeds the maximum safe sequence",
        )
            .into_response();
    }
    if q.through_seq
        .is_some_and(|through_seq| through_seq > MAX_SAFE_SEQUENCE)
    {
        return (
            StatusCode::BAD_REQUEST,
            "through_seq exceeds the maximum safe sequence",
        )
            .into_response();
    }
    if q.consume && (q.lane_id.is_none() || q.reply_to.is_none() || q.through_seq.is_none()) {
        return (
            StatusCode::BAD_REQUEST,
            "lane_id, reply_to, and through_seq are required when consume=true",
        )
            .into_response();
    }
    if q.reply_to
        .as_deref()
        .is_some_and(|reply_to| !is_valid_opaque_id(reply_to))
    {
        return (
            StatusCode::BAD_REQUEST,
            "reply_to is not a valid opaque identifier",
        )
            .into_response();
    }
    if q.consume
        && q.through_seq
            .is_some_and(|through_seq| through_seq <= q.after)
    {
        return (
            StatusCode::BAD_REQUEST,
            "through_seq must be greater than after when consume=true",
        )
            .into_response();
    }
    if q.consume {
        let lane_id = q.lane_id.as_deref().expect("checked above");
        if !lane_id_is_non_blank(lane_id) {
            return (
                StatusCode::BAD_REQUEST,
                "lane_id must match the lane ID grammar",
            )
                .into_response();
        }
        let known_lane = {
            let reg = state.reg.lock().await;
            reg.lanes.iter().any(|lane| lane.id == lane_id)
        };
        if !known_lane {
            return (StatusCode::NOT_FOUND, format!("unknown lane {lane_id}")).into_response();
        }
    }
    let (items, latest) = match state.relay.deliveries_after(
        q.after,
        q.consume,
        q.lane_id.as_deref(),
        q.reply_to.as_deref(),
        q.through_seq,
    ) {
        Ok(result) => result,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    };
    Json(serde_json::json!({ "deliveries": items, "latest": latest })).into_response()
}

async fn get_state(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let (mut body, active, revision) = {
        let reg = state.reg.lock().await;
        (
            serde_json::to_value(&*reg).unwrap(),
            reg.active().name.clone(),
            state
                .registry_revision
                .load(std::sync::atomic::Ordering::SeqCst),
        )
    };

    // enrich lanes with session titles from the Hermes store, when available
    if let Ok(db) = std::env::var("TELEPATHOS_HERMES_STATE_DB") {
        enrich_state_titles(&mut body, hermes_search::latest_titles(&db));
    }

    if let Some(lanes) = body["lanes"].as_array_mut() {
        for lane in lanes.iter_mut() {
            if let Some(id) = lane["id"].as_str() {
                lane["pending"] = serde_json::json!(state.relay.pending_count(id));
            }
        }
    }

    body["active"] = serde_json::json!(active);
    body["revision"] = serde_json::json!(revision);
    Json(body)
}

/// Titles are external enrichment rather than durable lane metadata. Bound
/// them again at the state boundary so a future search backend cannot make the
/// whole state response exceed the Node ↔ daemon transport contract.
fn enrich_state_titles(body: &mut serde_json::Value, titles: Vec<(String, String)>) {
    let titles: HashMap<_, _> = titles
        .into_iter()
        .filter_map(|(id, title)| hermes_search::bounded_title(&title).map(|title| (id, title)))
        .collect();
    if let Some(lanes) = body["lanes"].as_array_mut() {
        for lane in lanes.iter_mut() {
            if let Some(id) = lane["id"].as_str() {
                if let Some(title) = titles.get(id) {
                    lane["title"] = serde_json::json!(title);
                }
            }
        }
    }
}

async fn create_lane(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let name = body["name"].as_str().unwrap_or_default().to_string();
    if name.is_empty() {
        return lane_error_response(StatusCode::BAD_REQUEST, "name required");
    }
    if let Err(error) = LaneRegistry::validate_create_name(&name) {
        return lane_error_response(StatusCode::BAD_REQUEST, error.to_string());
    }
    let mut reg = state.reg.lock().await;
    if let Err(error) = reg.validate_create(&name) {
        return lane_create_error_response(error);
    }
    if let Err(error) = require_lane_persistence_available(&state) {
        return lane_mutation_error_response(error);
    }
    let before = reg.clone();
    let revision_before = state.registry_revision.load(Ordering::SeqCst);
    let lane = match reg.create(&name) {
        Ok(lane) => lane,
        Err(error) => return lane_create_error_response(error),
    };
    reg.switch(&lane.id);
    if lane_selection_changed(&before, &reg) {
        if let Err(error) = advance_registry_revision(&state.registry_revision) {
            *reg = before;
            return lane_error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
    }
    if let Err(error) = commit_lane_mutation(&state, &mut reg, before, revision_before) {
        return lane_mutation_error_response(error);
    }
    Json(serde_json::json!({ "ok": true, "lane": lane })).into_response()
}

async fn set_active(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let id = body["id"].as_str().unwrap_or_default().to_string();
    if !is_valid_lane_id(&id) {
        return lane_error_response(StatusCode::BAD_REQUEST, "id must match the lane ID grammar");
    }
    let mut reg = state.reg.lock().await;
    if !reg.lanes.iter().any(|lane| lane.id == id) {
        return lane_error_response(StatusCode::NOT_FOUND, format!("unknown lane {id}"));
    }
    if let Err(error) = require_lane_persistence_available(&state) {
        return lane_mutation_error_response(error);
    }
    let before = reg.clone();
    let revision_before = state.registry_revision.load(Ordering::SeqCst);
    let selection_changed = reg.active_id != id;
    match reg.switch(&id) {
        Some(lane) => {
            if selection_changed {
                if let Err(error) = advance_registry_revision(&state.registry_revision) {
                    *reg = before;
                    return lane_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        error.to_string(),
                    );
                }
            }
            if let Err(error) = commit_lane_mutation(&state, &mut reg, before, revision_before) {
                return lane_mutation_error_response(error);
            }
            Json(serde_json::json!({ "ok": true, "lane": lane })).into_response()
        }
        None => lane_error_response(StatusCode::NOT_FOUND, format!("unknown lane {id}")),
    }
}

async fn touch(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let id = body["id"].as_str().unwrap_or_default().to_string();
    if !is_valid_lane_id(&id) {
        return lane_error_response(StatusCode::BAD_REQUEST, "id must match the lane ID grammar");
    }
    let mut reg = state.reg.lock().await;
    if !reg.lanes.iter().any(|lane| lane.id == id) {
        return lane_error_response(StatusCode::NOT_FOUND, format!("unknown lane {id}"));
    }
    if let Err(error) = require_lane_persistence_available(&state) {
        return lane_mutation_error_response(error);
    }
    let before = reg.clone();
    let revision_before = state.registry_revision.load(Ordering::SeqCst);
    if reg.touch(&id).is_none() {
        return lane_error_response(StatusCode::NOT_FOUND, format!("unknown lane {id}"));
    }
    if let Err(error) = commit_lane_mutation(&state, &mut reg, before, revision_before) {
        return lane_mutation_error_response(error);
    }
    Json(serde_json::json!({ "ok": true })).into_response()
}

/// POST /api/lanes/interaction
/// {"id": "...", "interaction_id": "...", "interaction_created_at_ms": 0}
/// records one completed voice turn exactly once within the explicit seven-day
/// retry horizon. An expired retry is rejected rather than counted again.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionBody {
    id: String,
    interaction_id: String,
    interaction_created_at_ms: u64,
}

async fn record_interaction(
    State(state): State<Arc<AppState>>,
    Json(body): Json<InteractionBody>,
) -> Response {
    let id = body.id;
    if body.interaction_id.len() > MAX_OPAQUE_ID_BYTES
        || body.interaction_id.encode_utf16().count() > MAX_OPAQUE_ID_LENGTH
    {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "interaction_id exceeds the maximum supported length",
        )
            .into_response();
    }
    if !is_valid_lane_id(&id) || !is_valid_opaque_id(&body.interaction_id) {
        return (StatusCode::BAD_REQUEST, "id must match the lane ID grammar").into_response();
    }
    if let Err(error) = require_interaction_persistence_available(&state) {
        return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
    }
    let mut reg = state.reg.lock().await;
    if let Err(error) = require_lane_persistence_available(&state) {
        return lane_mutation_error_response(error);
    }
    if let Err(error) = reg.validate() {
        return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response();
    }
    if !reg.lanes.iter().any(|lane| lane.id == id) {
        return (StatusCode::NOT_FOUND, format!("unknown lane {id}")).into_response();
    }
    let mut ledger = state.interaction_ledger.lock().await;
    if let Err(error) = ledger.validate() {
        return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
    }
    // A concurrent request can have passed the first check while waiting for
    // these locks, then hit an ambiguous append. Recheck under the same lock
    // pair before this request can append a second physical journal line.
    if let Err(error) = require_interaction_persistence_available(&state) {
        return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
    }
    let now = match unix_time_millis() {
        Ok(now) => now,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    };
    let oldest_supported_timestamp_ms = now.saturating_sub(INTERACTION_DEDUPE_WINDOW_MS);
    if body.interaction_created_at_ms < oldest_supported_timestamp_ms {
        return (
            StatusCode::GONE,
            "interaction_id is outside the seven-day dedupe retry horizon; it was not counted",
        )
            .into_response();
    }
    if body.interaction_created_at_ms > now.saturating_add(MAX_INTERACTION_FUTURE_SKEW_MS) {
        return (
            StatusCode::BAD_REQUEST,
            "interaction_created_at_ms is more than five minutes in the future",
        )
            .into_response();
    }
    let existing_live = ledger
        .entries
        .get(&body.interaction_id)
        .is_some_and(|entry| entry.interaction_created_at_ms >= oldest_supported_timestamp_ms);
    if !existing_live {
        // A fresh ID, including a post-horizon reuse, may only append after
        // expired durable entries have crossed a snapshot+journal compaction
        // boundary. In particular, never prune only the in-memory map and
        // then append a different record for the same interaction ID.
        if let Err(error) = compact_expired_interactions_for_admission(
            &state.interaction_ledger_path,
            &mut ledger,
            oldest_supported_timestamp_ms,
        ) {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    }
    if let Some(existing_entry) = ledger.entries.get(&body.interaction_id).cloned() {
        if existing_entry.lane_id != id
            || existing_entry.interaction_created_at_ms != body.interaction_created_at_ms
        {
            return (
                StatusCode::CONFLICT,
                "interaction_id was already recorded with different interaction metadata",
            )
                .into_response();
        }
        // A previous request can have written its journal entry and then hit
        // a definite pre-rename registry failure. Retry its durable record
        // before treating it as a completed duplicate, so this process does
        // not remain below the journaled interaction count.
        let current_count = reg
            .lanes
            .iter()
            .find(|lane| lane.id == id)
            .and_then(|lane| lane.interactions)
            .unwrap_or(0);
        if current_count < existing_entry.lane_interactions {
            let before = reg.clone();
            let revision_before = state.registry_revision.load(Ordering::SeqCst);
            if let Some(lane) = reg.lanes.iter_mut().find(|lane| lane.id == id) {
                lane.interactions = Some(existing_entry.lane_interactions);
            }
            reg.touch(&id);
            if let Err(error) = commit_lane_mutation(&state, &mut reg, before, revision_before) {
                return lane_mutation_error_response(error);
            }
        }
        return Json(serde_json::json!({
            "ok": true,
            "id": id,
            "duplicate": true,
        }))
        .into_response();
    }
    let registry_count = lane_interaction_count(&reg, &id);
    let journaled_count = max_journaled_interaction_count(&ledger, &id, None);
    let next_count = match next_interaction_count(registry_count, journaled_count) {
        Ok(count) => count,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    };
    if ledger.journal_entries >= MAX_INTERACTION_JOURNAL_ENTRIES {
        // Compaction is only allowed after the previous journal entries have
        // already been reflected in the durable lane registry. The startup
        // path performs that recovery before requests can reach this point.
        if let Err(error) = compact_interaction_ledger(&state.interaction_ledger_path, &mut ledger)
        {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    }
    if ledger.entries.len() >= MAX_INTERACTION_DEDUPE_ENTRIES {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "interaction dedupe capacity is full; retry without creating a new interaction_id",
        )
            .into_response();
    }
    let entry = InteractionEntry {
        interaction_id: body.interaction_id,
        lane_id: id.clone(),
        interaction_created_at_ms: body.interaction_created_at_ms,
        lane_interactions: next_count,
    };
    if let Err(error) = append_interaction_journal(
        &interaction_journal_path(&state.interaction_ledger_path),
        &entry,
    ) {
        if error.is_ambiguous() {
            state
                .interaction_persistence_uncertain
                .store(true, Ordering::SeqCst);
            return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
        }
        return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
    }
    ledger.entries.insert(entry.interaction_id.clone(), entry);
    ledger.journal_entries += 1;
    let before = reg.clone();
    let revision_before = state.registry_revision.load(Ordering::SeqCst);
    if let Some(lane) = reg.lanes.iter_mut().find(|lane| lane.id == id) {
        lane.interactions = Some(next_count);
    }
    reg.touch(&id);
    if let Err(error) = commit_lane_mutation(&state, &mut reg, before, revision_before) {
        return lane_mutation_error_response(error);
    }
    Json(serde_json::json!({ "ok": true, "id": id, "duplicate": false })).into_response()
}

/// POST /api/meta {"utterance": "..."} — deterministic grammar first,
/// then (when TELEPATHOS_META_MODEL is set) the steering agent.
async fn meta(State(state): State<Arc<AppState>>, Json(body): Json<serde_json::Value>) -> Response {
    let utterance = body["utterance"].as_str().unwrap_or_default().to_string();
    // parse needs the lock only briefly
    let action = {
        let reg = state.reg.lock().await;
        telepathos_lanes::parse_meta(&utterance, &reg)
    };
    if let telepathos_lanes::MetaAction::New(name) = &action {
        if let Err(error) = LaneRegistry::validate_create_name(name) {
            return lane_error_response(StatusCode::BAD_REQUEST, error.to_string());
        }
    }
    let reply = match &action {
        // deterministic verbs run locally, instantly
        telepathos_lanes::MetaAction::Switch(_)
        | telepathos_lanes::MetaAction::List
        | telepathos_lanes::MetaAction::New(_)
        | telepathos_lanes::MetaAction::Brief(_)
        | telepathos_lanes::MetaAction::Note(_)
        | telepathos_lanes::MetaAction::Fork(_) => {
            let mut reg = state.reg.lock().await;
            if let telepathos_lanes::MetaAction::New(name) = &action {
                if let Err(error) = reg.validate_create(name) {
                    return lane_create_error_response(error);
                }
            }
            if let Err(error) = require_lane_persistence_available(&state) {
                return lane_mutation_error_response(error);
            }
            let before = reg.clone();
            let revision_before = state.registry_revision.load(Ordering::SeqCst);
            let source_lane = reg.active().id.clone();
            let notes_path = std::env::var("TELEPATHOS_NOTES").unwrap_or_else(|_| "notes.jsonl".into());
            let reply = telepathos_lanes::execute(&mut reg, action.clone(), std::path::Path::new(&notes_path));
            // fork: carry a context seed into the new lane's transcript
            if let telepathos_lanes::MetaAction::Fork(_) = &action {
                let new_lane = reg.active().id.clone();
                if new_lane != source_lane {
                    let turns = state.transcript.recent(&source_lane, 10);
                    if !turns.is_empty() {
                        let seed = turns.iter()
                            .map(|t| format!("{}: {}", t.role, t.text))
                            .collect::<Vec<_>>()
                            .join("\n");
                        state.transcript.push(&new_lane, "assistant",
                            &format!("Context from {} (forked):\n{source_lane}\n---\n{seed}",
                                     reg.active().name));
                    }
                    let seq = state.msg_seq.fetch_add(1, Ordering::SeqCst);
                    let event = relay::message_event(&new_lane, &reg.active().name,
                        &format!("This lane continues from '{source_lane}'. Recent context:\n{}",
                            turns.iter().map(|t| format!("{}: {}", t.role, t.text))
                                .collect::<Vec<_>>().join("\n")), seq);
                    if let Err(e) = state.relay.push_inbound(&event).await {
                        println!("fork seed push: {e}");
                    }
                }
            }
            if lane_selection_changed(&before, &reg) {
                if let Err(error) = advance_registry_revision(&state.registry_revision) {
                    *reg = before;
                    state
                        .registry_revision
                        .store(revision_before, Ordering::SeqCst);
                    return lane_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        error.to_string(),
                    );
                }
            }
            if *reg != before {
                if let Err(error) = commit_lane_mutation(&state, &mut reg, before, revision_before)
                {
                    return lane_mutation_error_response(error);
                }
            }
            reply
        }
        // grammar miss → steering agent when configured
        telepathos_lanes::MetaAction::Unknown => {
            let model = std::env::var("TELEPATHOS_META_MODEL").unwrap_or_default();
            if model.is_empty() {
                let mut reg = state.reg.lock().await;
                if let Err(error) = require_lane_persistence_available(&state) {
                    return lane_mutation_error_response(error);
                }
                let before = reg.clone();
                let revision_before = state.registry_revision.load(Ordering::SeqCst);
                let notes_path = std::env::var("TELEPATHOS_NOTES").unwrap_or_else(|_| "notes.jsonl".into());
            let reply = telepathos_lanes::execute(&mut reg, action.clone(), std::path::Path::new(&notes_path));
                if lane_selection_changed(&before, &reg) {
                    if let Err(error) = advance_registry_revision(&state.registry_revision) {
                        *reg = before;
                        state
                            .registry_revision
                            .store(revision_before, Ordering::SeqCst);
                        return lane_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            error.to_string(),
                        );
                    }
                }
                if *reg != before {
                    if let Err(error) =
                        commit_lane_mutation(&state, &mut reg, before, revision_before)
                    {
                        return lane_mutation_error_response(error);
                    }
                }
                reply
            } else {
                let provider = telepathos_steering::OpenAiProvider {
                    base_url: std::env::var("TELEPATHOS_META_BASE_URL")
                        .unwrap_or_else(|_| "https://api.openai.com/v1".into()),
                    api_key: std::env::var("OPENAI_API_KEY").unwrap_or_default(),
                    model,
                };
                // Run model I/O against a private snapshot. Never hold the
                // registry mutex across an await; merge the result below so a
                // concurrent lane switch/create is not overwritten.
                let (base, base_revision) = {
                    let reg = state.reg.lock().await;
                    (reg.clone(), state.registry_revision.load(Ordering::SeqCst))
                };
                let mut proposed = base.clone();
                let reply =
                    match telepathos_steering::run(&provider, &mut proposed, &utterance).await {
                        Ok(reply) => reply,
                        // Provider failures are an upstream failure, not spoken
                        // content. Do not expose provider bodies/URLs/errors or
                        // merge the private proposed registry on this path.
                        Err(_) => {
                            return lane_error_response(
                                StatusCode::BAD_GATEWAY,
                                "steering provider unavailable",
                            );
                        }
                    };
                let mut reg = state.reg.lock().await;
                if let Err(error) = require_lane_persistence_available(&state) {
                    return lane_mutation_error_response(error);
                }
                let before = reg.clone();
                let revision_before = state.registry_revision.load(Ordering::SeqCst);
                if let Err(error) =
                    merge_model_registry(&base, &proposed, &mut reg, base_revision, revision_before)
                {
                    return lane_create_error_response(error);
                }
                if lane_selection_changed(&before, &reg) {
                    if let Err(error) = advance_registry_revision(&state.registry_revision) {
                        *reg = before;
                        state
                            .registry_revision
                            .store(revision_before, Ordering::SeqCst);
                        return lane_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            error.to_string(),
                        );
                    }
                }
                if *reg != before {
                    if let Err(error) =
                        commit_lane_mutation(&state, &mut reg, before, revision_before)
                    {
                        return lane_mutation_error_response(error);
                    }
                }
                reply
            }
        }
    };
    Json(serde_json::json!({ "reply": reply })).into_response()
}

fn lane_selection_changed(before: &LaneRegistry, after: &LaneRegistry) -> bool {
    before.active_id != after.active_id
        || before.previous_id != after.previous_id
        || before.lanes.len() != after.lanes.len()
        || before
            .lanes
            .iter()
            .zip(&after.lanes)
            .any(|(left, right)| left.id != right.id)
}

/// Merge model-side lane mutations without clobbering concurrent API/meta
/// changes made while the model was thinking. Lane deletion is not supported,
/// so this merge is additive plus conflict-aware replacement of untouched
/// existing lanes. Selection is applied only when the registry revision still
/// matches the revision captured with `base`; the full registry selection tuple
/// is not an ABA-safe conflict token.
fn merge_model_registry(
    base: &LaneRegistry,
    proposed: &LaneRegistry,
    current: &mut LaneRegistry,
    base_revision: u64,
    current_revision: u64,
) -> Result<(), LaneCreateError> {
    let additions = proposed
        .lanes
        .iter()
        .filter(|proposed_lane| {
            !base.lanes.iter().any(|lane| lane.id == proposed_lane.id)
                && !current.lanes.iter().any(|lane| lane.id == proposed_lane.id)
        })
        .count();
    if current.lanes.len().saturating_add(additions) > MAX_LANE_COUNT {
        return Err(LaneCreateError::CapacityReached);
    }
    for proposed_lane in &proposed.lanes {
        match base.lanes.iter().find(|lane| lane.id == proposed_lane.id) {
            None => {
                if !current.lanes.iter().any(|lane| lane.id == proposed_lane.id) {
                    current.lanes.push(proposed_lane.clone());
                }
            }
            Some(base_lane) => {
                if let Some(current_lane) = current
                    .lanes
                    .iter_mut()
                    .find(|lane| lane.id == proposed_lane.id)
                {
                    if current_lane == base_lane {
                        *current_lane = proposed_lane.clone();
                    }
                }
            }
        }
    }
    if current_revision == base_revision {
        current.active_id = proposed.active_id.clone();
        current.previous_id = proposed.previous_id.clone();
    }
    Ok(())
}

/// Property tests for the journal fold: determinism, prefix consistency
/// under simulated crash truncation, and idempotent re-application.
#[cfg(test)]
mod interaction_fold_tests {
    use super::*;

    fn entry(id: &str, lane: &str, count: u64) -> String {
        format!(
            r#"{{"interaction_id":"{id}","lane_id":"{lane}","interaction_created_at_ms":1000,"lane_interactions":{count}}}"#
        )
    }

    fn line_json(id: &str, lane: &str, count: u64) -> String { entry(id, lane, count) }

    #[test]
    fn fold_is_deterministic() {
        let lines = vec![
            line_json("i1", "telepathos:direct", 3),
            line_json("i2", "telepathos:direct", 5),
        ];
        let a: anyhow::Result<InteractionLedger> =
            lines.iter().enumerate().try_fold(
                InteractionLedger::default(),
                |acc, (i, l)| fold_journal_entry(acc, l, i + 1, "j"),
            );
        let b: anyhow::Result<InteractionLedger> =
            lines.iter().enumerate().try_fold(
                InteractionLedger::default(),
                |acc, (i, l)| fold_journal_entry(acc, l, i + 1, "j"),
            );
        assert_eq!(a.unwrap().entries.len(), b.unwrap().entries.len());
    }

    #[test]
    fn crash_truncation_yields_prefix_consistent_ledger() {
        let lines = [
            line_json("i1", "telepathos:direct", 3),
            line_json("i2", "telepathos:direct", 5),
            line_json("i3", "telepathos:forks", 2),
        ];
        let full: InteractionLedger = lines
            .iter()
            .enumerate()
            .try_fold(InteractionLedger::default(), |acc, (i, l)| {
                fold_journal_entry(acc, l, i + 1, "j")
            })
            .unwrap();
        // A crash at every possible truncation point leaves a valid ledger
        // whose journal count equals the prefix length.
        for cut in 0..=lines.len() {
            let prefix: InteractionLedger = lines[..cut]
                .iter()
                .enumerate()
                .try_fold(InteractionLedger::default(), |acc, (i, l)| {
                    fold_journal_entry(acc, l, i + 1, "j")
                })
                .unwrap();
            assert_eq!(prefix.journal_entries, cut);
            assert!(prefix.entries.len() <= full.entries.len());
        }
    }

    #[test]
    fn duplicate_identical_entries_are_idempotent() {
        let line = line_json("i1", "telepathos:direct", 3);
        let once =
            fold_journal_entry(InteractionLedger::default(), &line, 1, "j").unwrap();
        let twice = fold_journal_entry(once.clone(), &line, 2, "j").unwrap();
        assert_eq!(once.entries, twice.entries);
        // Conflicting record for the same id must fail loudly.
        let conflict = line_json("i1", "telepathos:direct", 9);
        assert!(fold_journal_entry(twice, &conflict, 3, "j").is_err());
    }

    #[test]
    fn garbage_and_blank_lines_error_at_their_line_number() {
        let r = fold_journal_entry(InteractionLedger::default(), "", 4, "j");
        let msg = r.err().unwrap().to_string();
        assert!(msg.contains("line 4"), "{msg}");
        let r = fold_journal_entry(InteractionLedger::default(), "{not json", 7, "j");
        let msg = r.err().unwrap().to_string();
        assert!(msg.contains("line 7"), "{msg}");
    }

    #[test]
    fn reconciliation_takes_max_and_reports_change() {
        let mut lanes = vec![telepathos_lanes::Lane {
            id: "telepathos:direct".into(),
            name: "Direct".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            last_active: "2026-01-01T00:00:00Z".into(),
            interactions: Some(4),
        }];
        let mut entries = std::collections::HashMap::new();
        entries.insert(
            "i1".to_string(),
            serde_json::from_str::<InteractionEntry>(&entry("i1", "telepathos:direct", 6))
                .unwrap(),
        );
        assert!(reconcile_lane_interactions(&mut lanes, &entries));
        assert_eq!(lanes[0].interactions, Some(6));
        // Reconciling again is a no-op and reports no change.
        assert!(!reconcile_lane_interactions(&mut lanes, &entries));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    static META_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn temp_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("telepathosd-main-{label}-{nonce}.json"))
    }

    fn temp_directory(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("telepathosd-main-{label}-{nonce}"))
    }

    fn inject_directory_sync_failure(path: PathBuf) {
        *DIRECTORY_SYNC_FAULT.lock().unwrap() = Some(DirectorySyncFault { path });
    }

    fn inject_interaction_journal_write_failure(path: PathBuf, bytes_before_failure: usize) {
        *INTERACTION_JOURNAL_WRITE_FAULT.lock().unwrap() = Some(InteractionJournalWriteFault {
            path,
            bytes_before_failure,
        });
    }

    fn inject_interaction_journal_file_sync_failure(path: PathBuf) {
        *INTERACTION_JOURNAL_FILE_SYNC_FAULT.lock().unwrap() =
            Some(InteractionJournalFileSyncFault { path });
    }

    fn assert_directory_was_synced(path: &Path) {
        assert!(
            DIRECTORY_SYNC_LOG
                .lock()
                .unwrap()
                .iter()
                .any(|synced| synced == path),
            "directory {} was not synced",
            path.display(),
        );
    }

    fn lane_mutation_test_state(path: PathBuf, interaction_ledger_path: PathBuf) -> Arc<AppState> {
        Arc::new(AppState {
            reg: Arc::new(Mutex::new(LaneRegistry::default_direct())),
            path,
            transcript: Arc::new(TranscriptStore::default()),
            relay: Arc::new(RelayState::default()),
            msg_seq: AtomicU64::new(0),
            registry_revision: AtomicU64::new(10),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path,
            lane_save_fault: std::sync::Mutex::new(None),
        })
    }

    fn full_lane_registry() -> LaneRegistry {
        let mut registry = LaneRegistry::default_direct();
        for index in 1..MAX_LANE_COUNT {
            registry
                .create(&format!("{index:03}-{}", "x".repeat(108)))
                .unwrap();
        }
        assert_eq!(registry.lanes.len(), MAX_LANE_COUNT);
        registry
    }

    async fn response_json(response: Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    struct EnvironmentGuard {
        saved: Vec<(String, Option<String>)>,
    }

    impl EnvironmentGuard {
        fn new() -> Self {
            Self { saved: Vec::new() }
        }

        fn set(&mut self, key: &str, value: impl Into<String>) {
            if !self.saved.iter().any(|(saved_key, _)| saved_key == key) {
                self.saved.push((key.to_string(), std::env::var(key).ok()));
            }
            std::env::set_var(key, value.into());
        }
    }

    impl Drop for EnvironmentGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.iter().rev() {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }

    fn openai_json_response(body: serde_json::Value) -> Vec<u8> {
        let body = serde_json::to_vec(&body).unwrap();
        openai_http_response("200 OK", &body, body.len())
    }

    fn openai_http_response(status: &str, body: &[u8], declared_length: usize) -> Vec<u8> {
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {declared_length}\r\nConnection: close\r\n\r\n"
        );
        let mut response = head.into_bytes();
        response.extend_from_slice(body);
        response
    }

    async fn spawn_meta_test_server_with_responses(
        responses: Vec<Vec<u8>>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 8192];
                let _ = stream.read(&mut request).await;
                // A bounded client is expected to drop over-cap responses
                // before consuming their declared body, so a broken pipe is
                // a successful fake-provider outcome here.
                let _ = stream.write_all(&response).await;
            }
        });
        (address, task)
    }

    async fn spawn_meta_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let responses = vec![
            openai_json_response(serde_json::json!({
                "choices": [{"message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call-switch",
                        "type": "function",
                        "function": {
                            "name": "switch_lane",
                            "arguments": "{\"name\":\"revision target\"}"
                        }
                    }]
                }}]
            })),
            openai_json_response(serde_json::json!({
                "choices": [{"message": {
                    "role": "assistant",
                    "content": "switched"
                }}]
            })),
        ];
        spawn_meta_test_server_with_responses(responses).await
    }

    #[test]
    fn model_merge_preserves_concurrent_switch_and_adds_model_lane() {
        let base = LaneRegistry::default_direct();
        let mut proposed = base.clone();
        let model_lane = proposed.create("model lane").unwrap();
        proposed.switch(&model_lane.id);

        let mut current = base.clone();
        let concurrent_lane = current.create("concurrent lane").unwrap();
        current.switch(&concurrent_lane.id);
        merge_model_registry(&base, &proposed, &mut current, 10, 11).unwrap();

        assert!(current.lanes.iter().any(|lane| lane.id == model_lane.id));
        assert!(current
            .lanes
            .iter()
            .any(|lane| lane.id == concurrent_lane.id));
        assert_eq!(current.active_id, concurrent_lane.id);
    }

    #[test]
    fn model_merge_applies_non_conflicting_selection() {
        let mut base = LaneRegistry::default_direct();
        let model_target = base.create("model target").unwrap();
        let mut proposed = base.clone();
        proposed.switch(&model_target.id);

        let mut current = base.clone();
        merge_model_registry(&base, &proposed, &mut current, 10, 10).unwrap();

        assert_eq!(current.active_id, model_target.id);
        assert_eq!(current.previous_id, base.active_id);
    }

    #[test]
    fn model_merge_preserves_aba_concurrent_selection() {
        let base = LaneRegistry::default_direct();
        let mut proposed = base.clone();
        let model_lane = proposed.create("model lane").unwrap();
        proposed.switch(&model_lane.id);

        let mut current = base.clone();
        let concurrent_lane = current.create("concurrent lane").unwrap();
        current.switch(&concurrent_lane.id);
        current.switch(&base.active_id);
        assert_eq!(current.active_id, base.active_id);
        assert_ne!(current.previous_id, base.previous_id);

        merge_model_registry(&base, &proposed, &mut current, 10, 11).unwrap();

        assert!(current.lanes.iter().any(|lane| lane.id == model_lane.id));
        assert_eq!(current.active_id, base.active_id);
        assert_eq!(current.previous_id, concurrent_lane.id);
    }

    #[test]
    fn model_merge_preserves_exact_tuple_aba_selection() {
        let mut base = LaneRegistry::default_direct();
        let previous_lane = base.create("base previous").unwrap();
        let active_lane = base.create("base active").unwrap();
        base.switch(&previous_lane.id);
        base.switch(&active_lane.id);
        assert_eq!(base.active_id, active_lane.id);
        assert_eq!(base.previous_id, previous_lane.id);

        let mut proposed = base.clone();
        let model_lane = proposed.create("model lane").unwrap();
        proposed.switch(&model_lane.id);

        let mut current = base.clone();
        current.switch(&previous_lane.id);
        current.switch(&active_lane.id);
        assert_eq!(current.active_id, base.active_id);
        assert_eq!(current.previous_id, base.previous_id);

        merge_model_registry(&base, &proposed, &mut current, 10, 12).unwrap();

        assert!(current.lanes.iter().any(|lane| lane.id == model_lane.id));
        assert_eq!(current.active_id, active_lane.id);
        assert_eq!(current.previous_id, previous_lane.id);
    }

    #[test]
    fn model_merge_rejects_concurrent_capacity_overflow_without_mutation() {
        let mut base = LaneRegistry::default_direct();
        for index in 1..MAX_LANE_COUNT - 1 {
            base.create(&format!("base-{index}")).unwrap();
        }
        let mut proposed = base.clone();
        proposed.create("model lane").unwrap();
        let mut current = base.clone();
        current.create("concurrent lane").unwrap();
        let before = current.clone();

        assert_eq!(
            merge_model_registry(&base, &proposed, &mut current, 10, 10),
            Err(LaneCreateError::CapacityReached)
        );
        assert_eq!(current, before);
    }

    #[tokio::test]
    async fn full_lane_state_response_stays_well_below_the_transport_cap() {
        const STATE_RESPONSE_COMFORT_LIMIT_BYTES: usize = 128 * 1024;
        let state = lane_mutation_test_state(
            temp_path("lane-capacity-state-size"),
            temp_path("lane-capacity-state-size-ledger"),
        );
        *state.reg.lock().await = full_lane_registry();

        let response = get_state(State(state)).await.into_response();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            bytes.len() < STATE_RESPONSE_COMFORT_LIMIT_BYTES,
            "{} bytes exceeds the {} byte comfort limit",
            bytes.len(),
            STATE_RESPONSE_COMFORT_LIMIT_BYTES
        );
    }

    #[tokio::test]
    async fn maximum_valid_metadata_and_enriched_titles_stay_below_state_transport_cap() {
        const STATE_RESPONSE_TRANSPORT_CAP_BYTES: usize = 1024 * 1024;
        let state = lane_mutation_test_state(
            temp_path("lane-max-metadata-state-size"),
            temp_path("lane-max-metadata-state-size-ledger"),
        );
        let mut registry = full_lane_registry();
        for lane in &mut registry.lanes {
            // C0 controls take the largest JSON escape representation. They
            // are legal display metadata, so this measures the true envelope
            // worst case rather than just ordinary ASCII lane names.
            lane.name = "\0".repeat(MAX_LANE_NAME_UTF8_BYTES);
            lane.created_at = "epoch-ms:9007199254740991".into();
            lane.last_active = "epoch-ms:9007199254740991".into();
            lane.interactions = Some(MAX_SAFE_SEQUENCE);
        }
        registry.validate().unwrap();
        let titles = registry
            .lanes
            .iter()
            .map(|lane| {
                (
                    lane.id.clone(),
                    "\0".repeat(MAX_ENRICHED_LANE_TITLE_CODEPOINTS),
                )
            })
            .collect::<Vec<_>>();
        *state.reg.lock().await = registry;

        let response = get_state(State(state)).await.into_response();
        let mut body = response_json(response).await;
        enrich_state_titles(&mut body, titles);
        let bytes = serde_json::to_vec(&body).unwrap();
        assert!(
            bytes.len() < STATE_RESPONSE_TRANSPORT_CAP_BYTES,
            "{} bytes exceeds the {} byte transport cap",
            bytes.len(),
            STATE_RESPONSE_TRANSPORT_CAP_BYTES
        );
        assert!(
            bytes.len() < 512 * 1024,
            "{} bytes leaves too little margin below the transport cap",
            bytes.len()
        );
    }

    #[test]
    fn state_title_enrichment_safely_truncates_without_touching_persisted_lane_metadata() {
        let registry = LaneRegistry::default_direct();
        let before = registry.clone();
        let mut body = serde_json::to_value(&registry).unwrap();
        let huge_title = format!("{}é", "é".repeat(MAX_ENRICHED_LANE_TITLE_CODEPOINTS));
        enrich_state_titles(&mut body, vec![("telepathos:direct".into(), huge_title)]);
        let title = body["lanes"][0]["title"].as_str().unwrap();
        assert_eq!(title, "é".repeat(MAX_ENRICHED_LANE_TITLE_CODEPOINTS));
        assert_eq!(title.len(), MAX_ENRICHED_LANE_TITLE_UTF8_BYTES);
        assert_eq!(registry, before);
    }

    #[tokio::test]
    async fn pre_rename_lane_save_failure_rolls_back_registry_and_revision_without_panicking() {
        let path = temp_path("lane-pre-rename");
        let state = lane_mutation_test_state(path.clone(), temp_path("lane-pre-rename-ledger"));
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = create_lane(
            State(state.clone()),
            Json(serde_json::json!({ "name": "must roll back" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response_json(response).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("rolled back")));
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!state.lane_persistence_uncertain.load(Ordering::SeqCst));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn lane_mutation_api_maps_client_errors_and_success_statuses() {
        let path = temp_path("lane-api-statuses");
        let state = lane_mutation_test_state(path.clone(), temp_path("lane-api-statuses-ledger"));

        let missing_name = create_lane(State(state.clone()), Json(serde_json::json!({}))).await;
        assert_eq!(missing_name.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response_json(missing_name).await["error"], "name required");

        let malformed_id = set_active(
            State(state.clone()),
            Json(serde_json::json!({ "id": "not a lane id" })),
        )
        .await;
        assert_eq!(malformed_id.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(malformed_id).await["error"],
            "id must match the lane ID grammar"
        );

        let unknown_id = set_active(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:missing" })),
        )
        .await;
        assert_eq!(unknown_id.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(unknown_id).await["error"],
            "unknown lane telepathos:missing"
        );

        let success = create_lane(
            State(state.clone()),
            Json(serde_json::json!({ "name": "status success" })),
        )
        .await;
        assert_eq!(success.status(), StatusCode::OK);
        assert_eq!(response_json(success).await["ok"], true);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn lane_capacity_is_a_permanent_api_error_without_mutation_or_persistence() {
        let path = temp_path("lane-capacity-api");
        let state = lane_mutation_test_state(path.clone(), temp_path("lane-capacity-api-ledger"));
        *state.reg.lock().await = full_lane_registry();
        let before = state.reg.lock().await.clone();
        state
            .lane_persistence_uncertain
            .store(true, Ordering::SeqCst);
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = create_lane(
            State(state.clone()),
            Json(serde_json::json!({ "name": "one too many" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(response).await["error"],
            telepathos_lanes::LANE_CAPACITY_ERROR_MESSAGE
        );
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn simultaneous_over_capacity_creates_are_all_permanent_noops() {
        let path = temp_path("lane-capacity-concurrent");
        let state =
            lane_mutation_test_state(path.clone(), temp_path("lane-capacity-concurrent-ledger"));
        *state.reg.lock().await = full_lane_registry();
        let before = state.reg.lock().await.clone();

        let (first, second) = tokio::join!(
            create_lane(
                State(state.clone()),
                Json(serde_json::json!({ "name": "one too many first" })),
            ),
            create_lane(
                State(state.clone()),
                Json(serde_json::json!({ "name": "one too many second" })),
            )
        );
        assert_eq!(first.status(), StatusCode::CONFLICT);
        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn existing_lane_creation_still_works_at_capacity() {
        let path = temp_path("lane-capacity-existing");
        let state =
            lane_mutation_test_state(path.clone(), temp_path("lane-capacity-existing-ledger"));
        *state.reg.lock().await = full_lane_registry();
        let existing_name = state.reg.lock().await.lanes[1].name.clone();

        let response = create_lane(
            State(state.clone()),
            Json(serde_json::json!({ "name": existing_name })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.reg.lock().await.lanes.len(), MAX_LANE_COUNT);
        assert!(path.exists());
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn oversized_lane_name_returns_400_without_panicking_mutating_or_persisting() {
        let path = temp_path("oversized-lane-name");
        let state = lane_mutation_test_state(path.clone(), temp_path("oversized-lane-name-ledger"));
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);
        state
            .lane_persistence_uncertain
            .store(true, Ordering::SeqCst);

        let response = create_lane(
            State(state.clone()),
            Json(serde_json::json!({ "name": "a".repeat(MAX_LANE_ID_LENGTH) })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await["error"],
            "lane name is too long to produce a valid lane identifier"
        );
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn pre_rename_lane_activation_failure_returns_500_and_rolls_back() {
        let path = temp_path("lane-activate-pre-rename");
        let state =
            lane_mutation_test_state(path.clone(), temp_path("lane-activate-pre-rename-ledger"));
        let target_id = {
            let mut registry = state.reg.lock().await;
            registry.create("activation target").unwrap().id
        };
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = set_active(
            State(state.clone()),
            Json(serde_json::json!({ "id": target_id })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response_json(response).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("rolled back")));
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!state.lane_persistence_uncertain.load(Ordering::SeqCst));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn touching_unknown_lane_returns_404_without_mutating_or_persisting() {
        let path = temp_path("touch-unknown");
        let state = lane_mutation_test_state(path.clone(), temp_path("touch-unknown-ledger"));
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:missing" })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"], "unknown lane telepathos:missing");
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn touching_malformed_lane_returns_400_without_mutating_or_persisting() {
        let path = temp_path("touch-malformed");
        let state = lane_mutation_test_state(path.clone(), temp_path("touch-malformed-ledger"));
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "not a lane id" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await["error"],
            "id must match the lane ID grammar"
        );
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn touching_when_persistence_is_uncertain_returns_503_without_mutating() {
        let path = temp_path("touch-persistence-uncertain");
        let state = lane_mutation_test_state(
            path.clone(),
            temp_path("touch-persistence-uncertain-ledger"),
        );
        let before = state.reg.lock().await.clone();
        state
            .lane_persistence_uncertain
            .store(true, Ordering::SeqCst);
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:direct" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(response).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("persistence is uncertain")));
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn pre_rename_touch_failure_returns_500_and_rolls_back() {
        let path = temp_path("touch-pre-rename");
        let state = lane_mutation_test_state(path.clone(), temp_path("touch-pre-rename-ledger"));
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:direct" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response_json(response).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("rolled back")));
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!state.lane_persistence_uncertain.load(Ordering::SeqCst));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn post_rename_touch_failure_returns_503_and_latches_writes() {
        let path = temp_path("touch-post-rename");
        let state = lane_mutation_test_state(path.clone(), temp_path("touch-post-rename-ledger"));
        state.reg.lock().await.lanes[0].last_active = "epoch-ms:0".into();
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PostRename);

        let response = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:direct" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(response).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("may have committed")));
        let after_ambiguous_save = state.reg.lock().await.clone();
        assert_ne!(after_ambiguous_save, before);
        assert!(state.lane_persistence_uncertain.load(Ordering::SeqCst));

        let blocked = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:direct" })),
        )
        .await;
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(blocked).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("persistence is uncertain")));
        assert_eq!(*state.reg.lock().await, after_ambiguous_save);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn successful_touch_returns_200_only_after_registry_is_persisted() {
        let path = temp_path("touch-success");
        let state = lane_mutation_test_state(path.clone(), temp_path("touch-success-ledger"));

        let response = touch(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:direct" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["ok"], true);
        assert!(path.exists());
        assert_eq!(LaneRegistry::load(&path), *state.reg.lock().await);

        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn post_rename_lane_save_failure_preserves_live_state_and_latches_writes() {
        let state = lane_mutation_test_state(
            temp_path("lane-post-rename"),
            temp_path("lane-post-rename-ledger"),
        );
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PostRename);

        let response = create_lane(
            State(state.clone()),
            Json(serde_json::json!({ "name": "may have committed" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(response).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("may have committed")));
        let after_ambiguous_save = state.reg.lock().await.clone();
        assert!(after_ambiguous_save
            .lanes
            .iter()
            .any(|lane| lane.name == "may-have-committed"));
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 11);
        assert!(state.lane_persistence_uncertain.load(Ordering::SeqCst));

        let blocked = set_active(
            State(state.clone()),
            Json(serde_json::json!({ "id": "telepathos:direct" })),
        )
        .await;
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(blocked).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("persistence is uncertain")));
        assert_eq!(*state.reg.lock().await, after_ambiguous_save);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 11);
    }

    #[tokio::test]
    async fn interaction_pre_rename_failure_preserves_live_registry_and_retries_journaled_record() {
        let path = temp_path("interaction-pre-rename");
        let ledger_path = temp_path("interaction-pre-rename-ledger");
        let journal_path = interaction_journal_path(&ledger_path);
        let state = lane_mutation_test_state(path.clone(), ledger_path.clone());
        let before = state.reg.lock().await.clone();
        let created_at_ms = unix_time_millis().unwrap();
        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: "journaled-pre-rename".into(),
            interaction_created_at_ms: created_at_ms,
        };
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let failed = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(state.registry_revision.load(Ordering::SeqCst), 10);
        assert!(!state.lane_persistence_uncertain.load(Ordering::SeqCst));

        let retried = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(retried.status(), StatusCode::OK);
        assert_eq!(state.reg.lock().await.active().interactions, Some(1));
        assert!(path.exists());

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(ledger_path);
        let _ = fs::remove_file(journal_path);
    }

    #[tokio::test]
    async fn interaction_ambiguous_lane_save_returns_503_fences_writes_and_recovers_on_restart() {
        let path = temp_path("interaction-post-rename");
        let ledger_path = temp_path("interaction-post-rename-ledger");
        let journal_path = interaction_journal_path(&ledger_path);
        let state = lane_mutation_test_state(path.clone(), ledger_path.clone());
        let created_at_ms = unix_time_millis().unwrap();
        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: "journaled-post-rename".into(),
            interaction_created_at_ms: created_at_ms,
        };
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PostRename);

        let failed = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(failed).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("may have committed")));
        assert!(state.lane_persistence_uncertain.load(Ordering::SeqCst));

        let blocked = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(blocked).await["error"]
            .as_str()
            .is_some_and(|error| error.contains("persistence is uncertain")));

        // The journal append is already durable. Restart reconciliation owns
        // the ambiguous lane snapshot and restores the exact interaction
        // count without a second interaction record.
        let mut restarted = LaneRegistry::load(&path);
        let (ledger, reconciled) = load_interaction_ledger(&ledger_path, &mut restarted).unwrap();
        assert!(reconciled);
        assert_eq!(ledger.entries.len(), 1);
        assert_eq!(restarted.active().interactions, Some(1));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(ledger_path);
        let _ = fs::remove_file(journal_path);
    }

    #[tokio::test]
    async fn ambiguous_journal_directory_sync_fences_interaction_retries_until_restart() {
        let root = temp_directory("ambiguous-journal-append");
        let path = root.join("lanes.json");
        let ledger_path = root.join("state").join("nested").join("ledger.json");
        let journal_path = interaction_journal_path(&ledger_path);
        let state = lane_mutation_test_state(path.clone(), ledger_path.clone());
        let created_at_ms = unix_time_millis().unwrap();
        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: "journal-directory-sync-ambiguous".into(),
            interaction_created_at_ms: created_at_ms,
        };

        // The journal file sync succeeds first. Failing only the final parent
        // directory sync models a line that may survive a crash.
        inject_directory_sync_failure(containing_directory(&journal_path));
        let failed = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(state
            .interaction_persistence_uncertain
            .load(Ordering::SeqCst));
        assert_eq!(state.reg.lock().await.active().interactions, None);
        assert!(state.interaction_ledger.lock().await.entries.is_empty());

        let journal_after_ambiguous_append = fs::read_to_string(&journal_path).unwrap();
        assert_eq!(journal_after_ambiguous_append.lines().count(), 1);
        assert!(journal_after_ambiguous_append.contains("journal-directory-sync-ambiguous"));

        // The retry must not append a second physical line in this process.
        let retried = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(retried.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            fs::read_to_string(&journal_path).unwrap(),
            journal_after_ambiguous_append
        );

        // A restart reads the one journal line and can reconcile it before
        // accepting later writes; it never starts with an over-cap journal.
        let mut restarted_registry = LaneRegistry::default_direct();
        let (restarted_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut restarted_registry).unwrap();
        assert!(reconciled);
        assert_eq!(restarted_ledger.journal_entries, 1);
        assert_eq!(restarted_ledger.entries.len(), 1);
        assert_eq!(restarted_registry.active().interactions, Some(1));

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn partial_journal_append_failure_fences_retries_and_restart_refuses_corruption() {
        let root = temp_directory("partial-journal-append");
        let path = root.join("lanes.json");
        let ledger_path = root.join("state").join("nested").join("ledger.json");
        let journal_path = interaction_journal_path(&ledger_path);
        let state = lane_mutation_test_state(path, ledger_path.clone());
        let created_at_ms = unix_time_millis().unwrap();
        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: "journal-partial-write-ambiguous".into(),
            interaction_created_at_ms: created_at_ms,
        };
        let entry = InteractionEntry {
            interaction_id: "journal-partial-write-ambiguous".into(),
            lane_id: "telepathos:direct".into(),
            interaction_created_at_ms: created_at_ms,
            lane_interactions: 1,
        };
        let encoded = serde_json::to_vec(&entry).unwrap();
        let partial_bytes = 7;
        inject_interaction_journal_write_failure(journal_path.clone(), partial_bytes);

        let failed = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(state
            .interaction_persistence_uncertain
            .load(Ordering::SeqCst));
        assert_eq!(state.reg.lock().await.active().interactions, None);
        assert!(state.interaction_ledger.lock().await.entries.is_empty());

        let journal_after_partial_failure = fs::read(&journal_path).unwrap();
        assert_eq!(journal_after_partial_failure, encoded[..partial_bytes]);
        assert!(!journal_after_partial_failure.ends_with(b"\n"));

        // This process must not retry into the partial line; that would turn
        // the journal into a malformed/duplicate interaction record.
        let retried = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(retried.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            fs::read(&journal_path).unwrap(),
            journal_after_partial_failure
        );

        // A restart never silently accepts the partial line: reconciliation
        // fails explicitly rather than replaying a duplicate or corrupt entry.
        let mut restarted_registry = LaneRegistry::default_direct();
        let error = load_interaction_ledger(&ledger_path, &mut restarted_registry).unwrap_err();
        assert!(error.to_string().contains("corrupt interaction journal"));
        assert_eq!(restarted_registry.active().interactions, None);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn journal_file_sync_failure_fences_retries_and_restart_reconciles_once() {
        let root = temp_directory("journal-file-sync-failure");
        let path = root.join("lanes.json");
        let ledger_path = root.join("state").join("nested").join("ledger.json");
        let journal_path = interaction_journal_path(&ledger_path);
        let state = lane_mutation_test_state(path, ledger_path.clone());
        let created_at_ms = unix_time_millis().unwrap();
        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: "journal-file-sync-ambiguous".into(),
            interaction_created_at_ms: created_at_ms,
        };
        inject_interaction_journal_file_sync_failure(journal_path.clone());

        let failed = record_interaction(State(state.clone()), Json(body())).await;
        assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(state
            .interaction_persistence_uncertain
            .load(Ordering::SeqCst));

        let journal_after_sync_failure = fs::read_to_string(&journal_path).unwrap();
        assert_eq!(journal_after_sync_failure.lines().count(), 1);
        assert!(journal_after_sync_failure.contains("journal-file-sync-ambiguous"));

        let retried = record_interaction(State(state), Json(body())).await;
        assert_eq!(retried.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            fs::read_to_string(&journal_path).unwrap(),
            journal_after_sync_failure
        );

        let mut restarted_registry = LaneRegistry::default_direct();
        let (restarted_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut restarted_registry).unwrap();
        assert!(reconciled);
        assert_eq!(restarted_ledger.entries.len(), 1);
        assert_eq!(restarted_ledger.journal_entries, 1);
        assert_eq!(restarted_registry.active().interactions, Some(1));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_token_uses_fixed_digests_and_preserves_header_bearer_semantics() {
        let expected_digest = api_token_digest("secret");
        let same_digest = api_token_digest("secret");
        let wrong_digest = api_token_digest("secret-with-a-different-suffix");
        assert_eq!(expected_digest.len(), API_TOKEN_DIGEST_BYTES);
        assert!(api_token_digests_match(&expected_digest, &same_digest));
        assert!(!api_token_digests_match(&expected_digest, &wrong_digest));

        let mut headers = HeaderMap::new();
        headers.insert("x-telepathos-token", HeaderValue::from_static("secret"));
        assert!(api_token_matches(&headers, "secret"));
        assert!(!api_token_matches(&headers, "wrong"));

        let mut bearer = HeaderMap::new();
        bearer.insert("authorization", HeaderValue::from_static("Bearer secret"));
        assert!(api_token_matches(&bearer, "secret"));

        // The dedicated header remains authoritative when both forms are
        // present, preserving the prior API contract without exposing either
        // raw token in any error response.
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("x-telepathos-token", HeaderValue::from_static("wrong"));
        assert!(!api_token_matches(&headers, "secret"));
        assert!(!api_token_matches(&HeaderMap::new(), "secret"));
    }

    #[test]
    fn relay_credentials_are_required_for_authenticated_or_non_loopback_daemons() {
        assert!(relay_credentials_required("127.0.0.1", true, true));
        assert!(relay_credentials_required("0.0.0.0", false, true));
        assert!(!relay_credentials_required("127.0.0.1", false, true));
        assert!(!relay_credentials_required("0.0.0.0", true, false));
    }

    #[test]
    fn non_loopback_daemons_require_the_api_token() {
        assert!(is_loopback_bind("127.0.0.1"));
        assert!(is_loopback_bind("::1"));
        assert!(is_loopback_bind("[::1]"));
        assert!(!is_loopback_bind("0.0.0.0"));
        assert!(api_token_required("0.0.0.0", false));
        assert!(!api_token_required("0.0.0.0", true));
        assert!(!api_token_required("127.0.0.1", false));
    }

    #[test]
    fn bind_address_supports_ipv6_without_fallback() {
        assert_eq!(
            parse_bind_addr("::1", 8790).unwrap(),
            "[::1]:8790".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            parse_bind_addr("[::1]", 8790).unwrap(),
            "[::1]:8790".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            parse_bind_addr("localhost", 8790).unwrap(),
            SocketAddr::from(([127, 0, 0, 1], 8790))
        );
        assert!(parse_bind_addr("not a host", 8790).is_err());
    }

    #[test]
    fn registry_revision_generation_changes_across_restarts() {
        let lanes_path = temp_path("revision");
        let first = next_registry_revision(&lanes_path).unwrap();
        let second = next_registry_revision(&lanes_path).unwrap();
        assert!(second > first);
        assert_eq!(second - first, 1u64 << 32);
        let generation_path = registry_generation_path(&lanes_path);
        let _ = std::fs::remove_file(generation_path);
    }

    #[test]
    fn registry_revision_generation_stops_at_json_safe_limit_without_overwriting() {
        let lanes_path = temp_path("revision-safe-boundary");
        let generation_path = registry_generation_path(&lanes_path);
        fs::write(
            &generation_path,
            (MAX_REGISTRY_REVISION_GENERATION - 1).to_string(),
        )
        .unwrap();

        assert_eq!(
            next_registry_revision(&lanes_path).unwrap(),
            MAX_REGISTRY_REVISION_GENERATION * REGISTRY_REVISION_GENERATION_STRIDE
        );
        assert_eq!(
            fs::read_to_string(&generation_path).unwrap(),
            MAX_REGISTRY_REVISION_GENERATION.to_string()
        );

        let persisted_before_rejection = fs::read(&generation_path).unwrap();
        assert!(next_registry_revision(&lanes_path).is_err());
        assert_eq!(
            fs::read(&generation_path).unwrap(),
            persisted_before_rejection
        );
        let _ = fs::remove_file(generation_path);
    }

    #[test]
    fn invalid_registry_revision_generation_is_rejected_without_overwriting() {
        let lanes_path = temp_path("revision-invalid-generation");
        let generation_path = registry_generation_path(&lanes_path);
        let invalid_generation = MAX_REGISTRY_REVISION_GENERATION + 1;
        fs::write(&generation_path, invalid_generation.to_string()).unwrap();
        let persisted_before_rejection = fs::read(&generation_path).unwrap();

        assert!(next_registry_revision(&lanes_path).is_err());
        assert_eq!(
            fs::read(&generation_path).unwrap(),
            persisted_before_rejection
        );
        let _ = fs::remove_file(generation_path);
    }

    #[test]
    fn registry_revision_mutations_stop_before_crossing_a_generation() {
        let revision = AtomicU64::new(MAX_SAFE_SEQUENCE - 1);
        assert_eq!(
            advance_registry_revision(&revision).unwrap(),
            MAX_SAFE_SEQUENCE
        );
        assert!(advance_registry_revision(&revision).is_err());
        assert_eq!(revision.load(Ordering::SeqCst), MAX_SAFE_SEQUENCE);

        let generation_boundary = AtomicU64::new(REGISTRY_REVISION_MUTATION_MASK);
        assert!(advance_registry_revision(&generation_boundary).is_err());
        assert_eq!(
            generation_boundary.load(Ordering::SeqCst),
            REGISTRY_REVISION_MUTATION_MASK
        );
    }

    #[test]
    fn relative_registry_revision_path_uses_current_directory() {
        let name = format!(
            ".telepathosd-relative-revision-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let lanes_path = PathBuf::from(name);
        let generation_path = registry_generation_path(&lanes_path);
        let _ = std::fs::remove_file(&generation_path);

        assert!(next_registry_revision(&lanes_path).is_ok());

        let _ = std::fs::remove_file(generation_path);
    }

    #[test]
    fn nested_state_directory_creation_syncs_each_parent_before_atomic_writes() {
        let root = temp_directory("nested-state-directories");
        let snapshot_path = root.join("state").join("nested").join("ledger.json");
        atomic_write_text(&snapshot_path, "snapshot").unwrap();
        assert_eq!(fs::read_to_string(&snapshot_path).unwrap(), "snapshot");

        // `root`, `state`, and `nested` were absent. Their respective parents
        // must have been synced, and the final state directory is synced after
        // the rename.
        assert_directory_was_synced(&containing_directory(&root));
        assert_directory_was_synced(&root);
        assert_directory_was_synced(&root.join("state"));
        assert_directory_was_synced(&root.join("state").join("nested"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nested_revision_generation_directories_use_the_durable_creation_path() {
        let root = temp_directory("nested-revision-directories");
        let lanes_path = root.join("state").join("nested").join("lanes.json");
        assert!(next_registry_revision(&lanes_path).is_ok());

        assert_directory_was_synced(&containing_directory(&root));
        assert_directory_was_synced(&root);
        assert_directory_was_synced(&root.join("state"));
        assert_directory_was_synced(&root.join("state").join("nested"));

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn deterministic_meta_revision_exhaustion_returns_500_without_reply_or_mutation() {
        let path = temp_path("meta-revision-exhaustion-deterministic");
        let state = lane_mutation_test_state(
            path.clone(),
            temp_path("meta-revision-exhaustion-deterministic-ledger"),
        );
        {
            let mut registry = state.reg.lock().await;
            registry.create("revision target").unwrap();
        }
        state
            .registry_revision
            .store(REGISTRY_REVISION_MUTATION_MASK, Ordering::SeqCst);
        let before = state.reg.lock().await.clone();
        let revision_before = state.registry_revision.load(Ordering::SeqCst);

        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({
                "utterance": "switch to revision target"
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert!(body.get("reply").is_none());
        assert!(body["error"]
            .as_str()
            .is_some_and(|error| error.contains("registry revision exhausted")));
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(
            state.registry_revision.load(Ordering::SeqCst),
            revision_before
        );
        assert!(!state.lane_persistence_uncertain.load(Ordering::SeqCst));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn unknown_meta_without_model_at_revision_ceiling_remains_read_only() {
        let _env_lock = META_ENV_LOCK.lock().unwrap();
        let mut environment = EnvironmentGuard::new();
        environment.set("TELEPATHOS_META_MODEL", "");

        let path = temp_path("meta-revision-exhaustion-no-model");
        let state = lane_mutation_test_state(
            path.clone(),
            temp_path("meta-revision-exhaustion-no-model-ledger"),
        );
        state
            .registry_revision
            .store(REGISTRY_REVISION_MUTATION_MASK, Ordering::SeqCst);
        let before = state.reg.lock().await.clone();
        let revision_before = state.registry_revision.load(Ordering::SeqCst);

        // MetaAction::Unknown is a read-only help response when no model is
        // configured; it must not be mistaken for an exhausted mutation.
        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({ "utterance": "not a lane command" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(body["reply"].as_str().is_some());
        assert!(body.get("error").is_none());
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(
            state.registry_revision.load(Ordering::SeqCst),
            revision_before
        );
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn model_meta_revision_exhaustion_returns_500_without_reply_or_mutation() {
        let _env_lock = META_ENV_LOCK.lock().unwrap();
        let mut environment = EnvironmentGuard::new();
        environment.set("TELEPATHOS_META_MODEL", "test-model");
        environment.set("OPENAI_API_KEY", "test-key");
        let (server_address, server) = spawn_meta_test_server().await;
        environment.set(
            "TELEPATHOS_META_BASE_URL",
            format!("http://{server_address}"),
        );

        let path = temp_path("meta-revision-exhaustion-model");
        let state = lane_mutation_test_state(
            path.clone(),
            temp_path("meta-revision-exhaustion-model-ledger"),
        );
        {
            let mut registry = state.reg.lock().await;
            registry.create("revision target").unwrap();
        }
        state
            .registry_revision
            .store(REGISTRY_REVISION_MUTATION_MASK, Ordering::SeqCst);
        let before = state.reg.lock().await.clone();
        let revision_before = state.registry_revision.load(Ordering::SeqCst);

        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({ "utterance": "please handle this" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert!(body.get("reply").is_none());
        assert!(body["error"]
            .as_str()
            .is_some_and(|error| error.contains("registry revision exhausted")));
        assert_eq!(*state.reg.lock().await, before);
        assert_eq!(
            state.registry_revision.load(Ordering::SeqCst),
            revision_before
        );
        assert!(!state.lane_persistence_uncertain.load(Ordering::SeqCst));
        assert!(!path.exists());

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn model_meta_provider_failures_are_sanitized_and_never_merge_mutations() {
        let _env_lock = META_ENV_LOCK.lock().unwrap();
        let mut environment = EnvironmentGuard::new();
        environment.set("TELEPATHOS_META_MODEL", "test-model");
        environment.set("OPENAI_API_KEY", "test-key");
        let secret = "provider-secret-must-not-reach-api";
        let oversized = telepathos_steering::MAX_PROVIDER_RESPONSE_BYTES + 1;
        let tool_call = openai_json_response(serde_json::json!({
            "choices": [{"message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "switch-before-failure",
                    "type": "function",
                    "function": {
                        "name": "switch_lane",
                        "arguments": "{\"name\":\"provider target\"}"
                    }
                }]
            }}]
        }));
        let (server_address, server) = spawn_meta_test_server_with_responses(vec![
            openai_http_response(
                "401 Unauthorized",
                format!("{{\"error\":\"{secret}\"}}").as_bytes(),
                secret.len() + 12,
            ),
            openai_http_response("200 OK", b"{not valid JSON", b"{not valid JSON".len()),
            // The body is deliberately absent. The declared size must reject
            // before a client tries to accumulate it in memory.
            openai_http_response("200 OK", b"", oversized),
            openai_http_response("503 Service Unavailable", b"", oversized),
            tool_call,
            openai_http_response(
                "502 Bad Gateway",
                format!("{{\"error\":\"{secret}\"}}").as_bytes(),
                secret.len() + 12,
            ),
        ])
        .await;
        environment.set(
            "TELEPATHOS_META_BASE_URL",
            format!("http://{server_address}"),
        );

        for label in [
            "non-2xx",
            "malformed",
            "oversized-success",
            "oversized-error",
        ] {
            let path = temp_path(&format!("meta-provider-{label}"));
            let state = lane_mutation_test_state(
                path.clone(),
                temp_path(&format!("meta-provider-{label}-ledger")),
            );
            let before = state.reg.lock().await.clone();

            let response = meta(
                State(state.clone()),
                Json(serde_json::json!({ "utterance": "ask the model" })),
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{label}");
            let body = response_json(response).await;
            assert_eq!(body["error"], "steering provider unavailable", "{label}");
            assert!(body.get("reply").is_none(), "{label}");
            assert!(!serde_json::to_string(&body).unwrap().contains(secret));
            assert_eq!(*state.reg.lock().await, before, "{label}");
            assert!(!path.exists(), "{label}");
        }

        // A failure after a model tool turn must also discard the private
        // proposed registry rather than committing its pending switch.
        let path = temp_path("meta-provider-tool-failure");
        let state =
            lane_mutation_test_state(path.clone(), temp_path("meta-provider-tool-failure-ledger"));
        {
            let mut registry = state.reg.lock().await;
            registry.create("provider target").unwrap();
        }
        let before = state.reg.lock().await.clone();
        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({ "utterance": "ask the model" })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response_json(response).await;
        assert_eq!(body["error"], "steering provider unavailable");
        assert!(!serde_json::to_string(&body).unwrap().contains(secret));
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn model_meta_transport_failure_is_a_sanitized_502_without_mutation() {
        let _env_lock = META_ENV_LOCK.lock().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut environment = EnvironmentGuard::new();
        environment.set("TELEPATHOS_META_MODEL", "test-model");
        environment.set("OPENAI_API_KEY", "test-key");
        environment.set("TELEPATHOS_META_BASE_URL", format!("http://{address}"));

        let path = temp_path("meta-provider-transport");
        let state =
            lane_mutation_test_state(path.clone(), temp_path("meta-provider-transport-ledger"));
        let before = state.reg.lock().await.clone();
        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({ "utterance": "ask the model" })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            response_json(response).await["error"],
            "steering provider unavailable"
        );
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn model_meta_enforces_exact_reply_text_boundaries_before_serializing() {
        let _env_lock = META_ENV_LOCK.lock().unwrap();
        let mut environment = EnvironmentGuard::new();
        environment.set("TELEPATHOS_META_MODEL", "test-model");
        environment.set("OPENAI_API_KEY", "test-key");
        let exact_ascii = "a".repeat(telepathos_steering::MAX_REPLY_TEXT_BYTES);
        let exact_multibyte = "🦀".repeat(telepathos_steering::MAX_REPLY_TEXT_BYTES / "🦀".len());
        assert_eq!(
            exact_multibyte.len(),
            telepathos_steering::MAX_REPLY_TEXT_BYTES
        );
        let too_large = "a".repeat(telepathos_steering::MAX_REPLY_TEXT_BYTES + 1);
        let (server_address, server) = spawn_meta_test_server_with_responses(vec![
            openai_json_response(serde_json::json!({
                "choices": [{"message": {"role": "assistant", "content": exact_ascii}}]
            })),
            openai_json_response(serde_json::json!({
                "choices": [{"message": {"role": "assistant", "content": exact_multibyte}}]
            })),
            openai_json_response(serde_json::json!({
                "choices": [{"message": {"role": "assistant", "content": too_large}}]
            })),
        ])
        .await;
        environment.set(
            "TELEPATHOS_META_BASE_URL",
            format!("http://{server_address}"),
        );

        for (label, expected) in [
            ("exact-ascii", telepathos_steering::MAX_REPLY_TEXT_BYTES),
            ("exact-multibyte", telepathos_steering::MAX_REPLY_TEXT_BYTES),
        ] {
            let path = temp_path(&format!("meta-provider-reply-{label}"));
            let state = lane_mutation_test_state(
                path.clone(),
                temp_path(&format!("meta-provider-reply-{label}-ledger")),
            );
            let response = meta(
                State(state),
                Json(serde_json::json!({ "utterance": "ask the model" })),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{label}");
            let reply = response_json(response).await["reply"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(reply.len(), expected, "{label}");
            assert!(!path.exists(), "{label}");
        }

        let path = temp_path("meta-provider-reply-oversized");
        let state = lane_mutation_test_state(
            path.clone(),
            temp_path("meta-provider-reply-oversized-ledger"),
        );
        let before = state.reg.lock().await.clone();
        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({ "utterance": "ask the model" })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            response_json(response).await["error"],
            "steering provider unavailable"
        );
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn deterministic_meta_mutation_is_persisted() {
        let path = temp_path("meta");
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(LaneRegistry::default_direct())),
            path: path.clone(),
            transcript: Arc::new(TranscriptStore::default()),
            relay: Arc::new(RelayState::default()),
            msg_seq: std::sync::atomic::AtomicU64::new(0),
            registry_revision: std::sync::atomic::AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path: temp_path("meta-interactions"),
            lane_save_fault: std::sync::Mutex::new(None),
        });
        let _ = meta(
            State(state),
            Json(serde_json::json!({
                "utterance": "new conversation for persisted lane"
            })),
        )
        .await;

        let saved = LaneRegistry::load(&path);
        assert!(saved.lanes.iter().any(|lane| lane.name == "persisted-lane"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn meta_durability_failures_are_http_errors_not_spoken_successes() {
        let definite_path = temp_path("meta-definite-save-failure");
        let definite = lane_mutation_test_state(
            definite_path.clone(),
            temp_path("meta-definite-save-failure-ledger"),
        );
        let definite_before = definite.reg.lock().await.clone();
        *definite.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = meta(
            State(definite.clone()),
            Json(serde_json::json!({
                "utterance": "new conversation for definite durability failure"
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert!(body.get("reply").is_none());
        assert!(body["error"]
            .as_str()
            .is_some_and(|error| error.contains("rolled back")));
        assert_eq!(*definite.reg.lock().await, definite_before);
        assert!(!definite_path.exists());

        let ambiguous_path = temp_path("meta-ambiguous-save-failure");
        let ambiguous = lane_mutation_test_state(
            ambiguous_path.clone(),
            temp_path("meta-ambiguous-save-failure-ledger"),
        );
        *ambiguous.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PostRename);

        let response = meta(
            State(ambiguous.clone()),
            Json(serde_json::json!({
                "utterance": "new conversation for ambiguous durability failure"
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response_json(response).await;
        assert!(body.get("reply").is_none());
        assert!(body["error"]
            .as_str()
            .is_some_and(|error| error.contains("may have committed")));
        assert!(ambiguous.lane_persistence_uncertain.load(Ordering::SeqCst));

        let blocked = meta(
            State(ambiguous.clone()),
            Json(serde_json::json!({ "utterance": "list conversations" })),
        )
        .await;
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response_json(blocked).await.get("reply").is_none());

        let _ = fs::remove_file(definite_path);
        let _ = fs::remove_file(ambiguous_path);
    }

    #[tokio::test]
    async fn meta_oversized_lane_name_returns_400_without_mutation() {
        let path = temp_path("meta-oversized-lane-name");
        let state =
            lane_mutation_test_state(path.clone(), temp_path("meta-oversized-lane-name-ledger"));
        let before = state.reg.lock().await.clone();
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);
        state
            .lane_persistence_uncertain
            .store(true, Ordering::SeqCst);

        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({
                "utterance": format!("new conversation for {}", "x".repeat(MAX_LANE_ID_LENGTH))
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(body.get("reply").is_none());
        assert_eq!(
            body["error"],
            "lane name is too long to produce a valid lane identifier"
        );
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn meta_lane_capacity_returns_a_stable_permanent_error_without_mutation() {
        let path = temp_path("meta-lane-capacity");
        let state = lane_mutation_test_state(path.clone(), temp_path("meta-lane-capacity-ledger"));
        *state.reg.lock().await = full_lane_registry();
        let before = state.reg.lock().await.clone();
        state
            .lane_persistence_uncertain
            .store(true, Ordering::SeqCst);
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);

        let response = meta(
            State(state.clone()),
            Json(serde_json::json!({
                "utterance": "new conversation for one too many"
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert!(body.get("reply").is_none());
        assert_eq!(body["error"], telepathos_lanes::LANE_CAPACITY_ERROR_MESSAGE);
        assert_eq!(*state.reg.lock().await, before);
        assert!(!path.exists());
        assert!(state.lane_save_fault.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn pending_consume_removes_only_requested_spoken_rows() {
        let mut reg = LaneRegistry::default_direct();
        let other = reg.create("other").unwrap();
        reg.switch(&other.id);
        let transcript = Arc::new(TranscriptStore::load(PathBuf::from(
            std::env::var("TELEPATHOS_TRANSCRIPT").unwrap_or_else(|_| "transcript.json".into()),
        )));
    let relay = Arc::new(RelayState::default());
        let spoken = relay
            .queue_delivery("telepathos:direct", "direct reply")
            .unwrap();
        let correlated = relay
            .queue_gateway_delivery("telepathos:direct", "receipt-owned reply", Some("tp-owned"))
            .unwrap();
        let unspoken = relay
            .queue_delivery("telepathos:direct", "later generic reply")
            .unwrap();
        relay.queue_delivery(&other.id, "other reply").unwrap();
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(reg)),
            path: temp_path("pending"),
            transcript: Arc::new(TranscriptStore::default()),
            relay: relay.clone(),
            msg_seq: std::sync::atomic::AtomicU64::new(0),
            registry_revision: std::sync::atomic::AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path: temp_path("pending-interactions"),
            lane_save_fault: std::sync::Mutex::new(None),
        });

        let response = consume_pending(
            State(state),
            Json(serde_json::json!({
                "lane_id": "telepathos:direct",
                "sequences": [spoken],
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let remaining = relay.pending_for("telepathos:direct");
        assert_eq!(
            remaining
                .iter()
                .map(|delivery| delivery.seq)
                .collect::<Vec<_>>(),
            vec![correlated, unspoken]
        );
        assert_eq!(remaining[0].reply_to.as_deref(), Some("tp-owned"));
        assert_eq!(relay.pending_count(&other.id), 1);
    }

    #[tokio::test]
    async fn delivery_head_is_bounded_and_tracks_the_durable_high_water_mark() {
        let state = lane_mutation_test_state(
            temp_path("delivery-head"),
            temp_path("delivery-head-interactions"),
        );
        // Together these valid rows exceed the old 576 KiB Node reply cap.
        // The head response must remain tiny because it never materializes
        // them into an HTTP response.
        let large = "x".repeat(300 * 1024);
        state
            .relay
            .queue_delivery("telepathos:other", &large)
            .unwrap();
        state
            .relay
            .queue_delivery("telepathos:other", &large)
            .unwrap();

        let response = get_delivery_head(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            body.len() < 64,
            "delivery head unexpectedly grew to {} bytes",
            body.len()
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["latest"],
            2
        );
        assert_eq!(state.relay.pending_count("telepathos:other"), 2);

        state
            .relay
            .deliveries_after(0, true, Some("telepathos:other"), None, None)
            .unwrap();
        // The queue is empty, but the cursor must preserve the global high
        // water mark so a newly queued reply cannot be mistaken for old work.
        assert_eq!(state.relay.delivery_head().unwrap(), 2);
    }

    #[tokio::test]
    async fn post_message_rejects_present_malformed_lane_without_registering_or_pushing() {
        let relay_path = temp_path("malformed-message-lane-relay");
        let state = lane_mutation_test_state(
            temp_path("malformed-message-lane-registry"),
            temp_path("malformed-message-lane-interactions"),
        );
        state.relay.set_persist_path(&relay_path);

        let malformed_lane_ids = [
            serde_json::Value::Null,
            serde_json::json!(7),
            serde_json::json!(""),
            serde_json::json!(" \t\n"),
            serde_json::json!({}),
            serde_json::json!([]),
        ];
        for lane_id in malformed_lane_ids {
            let response = post_message(
                State(state.clone()),
                Json(serde_json::json!({
                    "lane_id": lane_id,
                    "text": "must not be registered",
                })),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        assert_eq!(state.msg_seq.load(Ordering::SeqCst), 0);
        assert!(!relay_path.exists());
        assert!(state.relay.pending_for("telepathos:direct").is_empty());

        let _ = fs::remove_file(&relay_path);
    }

    #[tokio::test]
    async fn post_message_rejects_permanent_oversize_as_413_without_retry_mutation() {
        let relay_path = temp_path("oversized-message-http");
        let state = lane_mutation_test_state(
            temp_path("oversized-message-http-registry"),
            temp_path("oversized-message-http-interactions"),
        );
        state.relay.set_persist_path(&relay_path);

        let envelope_overhead_text = "x".repeat(relay::MAX_INBOUND_RECORD_BYTES - 1);
        let multibyte_text = "🦀".repeat(relay::MAX_INBOUND_RECORD_BYTES / 4 + 1);
        for text in [envelope_overhead_text, multibyte_text] {
            let event = relay::message_event("telepathos:direct", "direct", &text, 0);
            assert!(
                relay::inbound_record_size("tp-0", 1, &event).unwrap()
                    > relay::MAX_INBOUND_RECORD_BYTES
            );

            for _ in 0..2 {
                let response = post_message(
                    State(state.clone()),
                    Json(serde_json::json!({
                        "lane_id": "telepathos:direct",
                        "text": text,
                    })),
                )
                .await;
                assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            }
        }

        assert_eq!(state.msg_seq.load(Ordering::SeqCst), 0);
        assert_eq!(state.relay.pending_inbound_count(), 0);
        assert_eq!(state.relay.pending_request_count(), 0);
        assert!(!relay_path.exists());
        assert!(!relay::request_path(&relay_path).exists());
        assert!(!relay::inbound_path(&relay_path).exists());

        let _ = fs::remove_file(&relay_path);
        let _ = fs::remove_file(relay::request_path(&relay_path));
        let _ = fs::remove_file(relay::inbound_path(&relay_path));
    }

    #[tokio::test]
    async fn oversized_http_delivery_boundaries_are_rejected_without_mutation() {
        let pending_path = temp_path("unsafe-delivery-boundary");
        let transcript = Arc::new(TranscriptStore::load(PathBuf::from(
            std::env::var("TELEPATHOS_TRANSCRIPT").unwrap_or_else(|_| "transcript.json".into()),
        )));
    let relay = Arc::new(RelayState::default());
        relay.set_persist_path(&pending_path);
        relay
            .queue_delivery("telepathos:direct", "first reply")
            .unwrap();
        relay
            .queue_delivery("telepathos:direct", "second reply")
            .unwrap();
        let snapshot_before = fs::read_to_string(&pending_path).unwrap();
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(LaneRegistry::default_direct())),
            path: temp_path("unsafe-delivery-boundary-lanes"),
            transcript: Arc::new(TranscriptStore::default()),
            relay: relay.clone(),
            msg_seq: std::sync::atomic::AtomicU64::new(0),
            registry_revision: std::sync::atomic::AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path: temp_path("unsafe-delivery-boundary-interactions"),
            lane_save_fault: std::sync::Mutex::new(None),
        });
        let oversized = MAX_SAFE_SEQUENCE + 1;

        // /api/pending/consume is an explicit-sequence hard cutover. A legacy
        // broad cursor cannot erase receipt-owned rows hidden from narration.
        let response = consume_pending(
            State(state.clone()),
            Json(serde_json::json!({
                "lane_id": "telepathos:direct",
                "through_seq": 2,
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = consume_pending(
            State(state.clone()),
            Json(serde_json::json!({
                "lane_id": "telepathos:direct",
                "sequences": [oversized],
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: oversized,
                consume: false,
                lane_id: None,
                reply_to: None,
                through_seq: None,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: 0,
                consume: true,
                lane_id: Some("telepathos:direct".into()),
                reply_to: None,
                through_seq: Some(oversized),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(relay.pending_count("telepathos:direct"), 2);
        assert_eq!(fs::read_to_string(&pending_path).unwrap(), snapshot_before);

        let _ = fs::remove_file(pending_path);
    }

    #[tokio::test]
    async fn consuming_delivery_rejects_non_advancing_through_seq_without_mutation() {
        let pending_path = temp_path("non-advancing-delivery-boundary");
        let state = lane_mutation_test_state(
            temp_path("non-advancing-delivery-boundary-lanes"),
            temp_path("non-advancing-delivery-boundary-interactions"),
        );
        let relay = state.relay.clone();
        relay.set_persist_path(&pending_path);
        relay
            .queue_delivery("telepathos:direct", "first reply")
            .unwrap();
        relay
            .queue_delivery("telepathos:direct", "second reply")
            .unwrap();
        let snapshot_before = fs::read(&pending_path).unwrap();

        for (after, through_seq) in [(1, 1), (2, 1)] {
            let response = get_delivery(
                State(state.clone()),
                axum::extract::Query(DeliveryQuery {
                    after,
                    consume: true,
                    lane_id: Some("telepathos:direct".into()),
                    reply_to: None,
                    through_seq: Some(through_seq),
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(relay.pending_count("telepathos:direct"), 2);
            assert_eq!(fs::read(&pending_path).unwrap(), snapshot_before);
        }

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: 1,
                consume: false,
                lane_id: Some("telepathos:direct".into()),
                reply_to: None,
                through_seq: Some(1),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(relay.pending_count("telepathos:direct"), 2);

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: MAX_SAFE_SEQUENCE,
                consume: false,
                lane_id: None,
                reply_to: None,
                through_seq: Some(MAX_SAFE_SEQUENCE),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let _ = fs::remove_file(pending_path);
    }

    #[tokio::test]
    async fn consuming_delivery_rejects_malformed_or_unknown_lane_without_mutation() {
        let pending_path = temp_path("delivery-lane-validation");
        let transcript = Arc::new(TranscriptStore::load(PathBuf::from(
            std::env::var("TELEPATHOS_TRANSCRIPT").unwrap_or_else(|_| "transcript.json".into()),
        )));
    let relay = Arc::new(RelayState::default());
        relay.set_persist_path(&pending_path);
        let direct_seq = relay
            .queue_gateway_delivery("telepathos:direct", "direct reply", Some("tp-direct"))
            .unwrap();
        let other_seq = relay
            .queue_delivery("telepathos:other", "other reply")
            .unwrap();
        let snapshot_before = fs::read_to_string(&pending_path).unwrap();
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(LaneRegistry::default_direct())),
            path: temp_path("delivery-lane-validation-lanes"),
            transcript: Arc::new(TranscriptStore::default()),
            relay: relay.clone(),
            msg_seq: AtomicU64::new(0),
            registry_revision: AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path: temp_path("delivery-lane-validation-interactions"),
            lane_save_fault: std::sync::Mutex::new(None),
        });

        for (lane_id, expected_status) in [
            ("", StatusCode::BAD_REQUEST),
            (" \t\n", StatusCode::BAD_REQUEST),
            ("telepathos:missing", StatusCode::NOT_FOUND),
        ] {
            let response = get_delivery(
                State(state.clone()),
                axum::extract::Query(DeliveryQuery {
                    after: 0,
                    consume: true,
                    lane_id: Some(lane_id.into()),
                    reply_to: Some("tp-missing".into()),
                    through_seq: Some(other_seq),
                }),
            )
            .await;
            assert_eq!(response.status(), expected_status);
            assert_eq!(relay.pending_count("telepathos:direct"), 1);
            assert_eq!(relay.pending_count("telepathos:other"), 1);
            assert_eq!(fs::read_to_string(&pending_path).unwrap(), snapshot_before);
        }

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: 0,
                consume: false,
                lane_id: Some("telepathos:missing".into()),
                reply_to: None,
                through_seq: None,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(relay.pending_count("telepathos:direct"), 1);
        assert_eq!(relay.pending_count("telepathos:other"), 1);
        assert_eq!(fs::read_to_string(&pending_path).unwrap(), snapshot_before);

        let response = get_delivery(
            State(state),
            axum::extract::Query(DeliveryQuery {
                after: 0,
                consume: true,
                lane_id: Some("telepathos:direct".into()),
                reply_to: Some("tp-direct".into()),
                through_seq: Some(direct_seq),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(relay.pending_count("telepathos:direct"), 0);
        assert_eq!(relay.pending_count("telepathos:other"), 1);

        let _ = fs::remove_file(pending_path);
    }

    #[tokio::test]
    async fn consuming_delivery_requires_exact_nonblank_reply_to_without_mutation() {
        let pending_path = temp_path("delivery-reply-to-validation");
        let state = lane_mutation_test_state(
            temp_path("delivery-reply-to-validation-lanes"),
            temp_path("delivery-reply-to-validation-interactions"),
        );
        let relay = state.relay.clone();
        relay.set_persist_path(&pending_path);
        let generic = relay
            .queue_delivery("telepathos:direct", "generic update")
            .unwrap();
        let owned = relay
            .queue_gateway_delivery("telepathos:direct", "owned reply", Some("tp-owned"))
            .unwrap();
        let other = relay
            .queue_gateway_delivery("telepathos:direct", "other reply", Some("tp-other"))
            .unwrap();
        let snapshot_before = fs::read(&pending_path).unwrap();

        for reply_to in [None, Some("".to_string()), Some(" \t\n".to_string())] {
            let response = get_delivery(
                State(state.clone()),
                axum::extract::Query(DeliveryQuery {
                    after: 0,
                    consume: true,
                    lane_id: Some("telepathos:direct".into()),
                    reply_to,
                    through_seq: Some(other),
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(state.relay.pending_count("telepathos:direct"), 3);
            assert_eq!(fs::read(&pending_path).unwrap(), snapshot_before);
        }

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: 0,
                consume: false,
                lane_id: None,
                reply_to: None,
                through_seq: None,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.relay.pending_count("telepathos:direct"), 3);

        let response = get_delivery(
            State(state.clone()),
            axum::extract::Query(DeliveryQuery {
                after: 0,
                consume: true,
                lane_id: Some("telepathos:direct".into()),
                reply_to: Some("tp-owned".into()),
                through_seq: Some(owned),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.relay.pending_count("telepathos:direct"), 2);
        let remaining = state.relay.pending_for("telepathos:direct");
        assert_eq!(remaining[0].seq, generic);
        assert_eq!(remaining[1].seq, other);

        let _ = fs::remove_file(pending_path);
    }

    #[tokio::test]
    async fn interaction_recording_recovers_a_journaled_write_and_stays_idempotent() {
        let path = temp_path("interaction");
        let ledger_path = temp_path("interaction-ledger");
        let created_at_ms = unix_time_millis().unwrap();
        let entry = InteractionEntry {
            interaction_id: "i-test-1".into(),
            lane_id: "telepathos:direct".into(),
            interaction_created_at_ms: created_at_ms,
            lane_interactions: 1,
        };
        // Simulate a crash after the durable journal write and before the lane
        // registry save. Startup must repair the aggregate count first.
        append_interaction_journal(&interaction_journal_path(&ledger_path), &entry).unwrap();
        let mut reloaded = LaneRegistry::default_direct();
        let (ledger, reconciled) = load_interaction_ledger(&ledger_path, &mut reloaded).unwrap();
        assert!(reconciled);
        assert_eq!(reloaded.active().interactions, Some(1));
        reloaded.save(&path).unwrap();

        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(reloaded)),
            path: path.clone(),
            transcript: Arc::new(TranscriptStore::default()),
            relay: Arc::new(RelayState::default()),
            msg_seq: std::sync::atomic::AtomicU64::new(0),
            registry_revision: std::sync::atomic::AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(ledger),
            interaction_ledger_path: ledger_path.clone(),
            lane_save_fault: std::sync::Mutex::new(None),
        });

        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: "i-test-1".into(),
            interaction_created_at_ms: created_at_ms,
        };
        assert_eq!(
            record_interaction(State(state.clone()), Json(body()))
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            record_interaction(State(state.clone()), Json(body()))
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(state.reg.lock().await.active().interactions, Some(1));

        let mut restarted = LaneRegistry::load(&path);
        let (restarted_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut restarted).unwrap();
        assert!(!reconciled);
        assert_eq!(restarted.active().interactions, Some(1));
        assert_eq!(restarted_ledger.entries.len(), 1);
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(&ledger_path);
        let _ = fs::remove_file(interaction_journal_path(&ledger_path));
    }

    #[tokio::test]
    async fn expired_interaction_id_reuse_compacts_before_append_and_recovers_after_crash() {
        let root = temp_directory("interaction-reuse-crash");
        let path = root.join("lanes.json");
        let ledger_path = root.join("interaction-ledger.json");
        let journal_path = interaction_journal_path(&ledger_path);
        let now = unix_time_millis().unwrap();
        let expired_entry = InteractionEntry {
            interaction_id: "reused-after-horizon".into(),
            lane_id: "telepathos:direct".into(),
            interaction_created_at_ms: now
                .saturating_sub(INTERACTION_DEDUPE_WINDOW_MS)
                .saturating_sub(1),
            lane_interactions: 1,
        };
        let mut durable_ledger = InteractionLedger::default();
        durable_ledger
            .entries
            .insert(expired_entry.interaction_id.clone(), expired_entry.clone());
        persist_interaction_ledger(&ledger_path, &durable_ledger).unwrap();

        let mut persisted_registry = LaneRegistry::default_direct();
        persisted_registry.lanes[0].interactions = Some(1);
        persisted_registry.save(&path).unwrap();
        let (loaded_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut persisted_registry).unwrap();
        assert!(!reconciled);
        assert!(loaded_ledger.entries.is_empty());
        assert!(loaded_ledger.needs_durable_compaction);

        let state = lane_mutation_test_state(path.clone(), ledger_path.clone());
        *state.reg.lock().await = persisted_registry;
        *state.interaction_ledger.lock().await = loaded_ledger;
        // Model a crash after the new journal append but before the lane
        // registry save. The old ID must already be absent from the durable
        // generation, otherwise restart would reject conflicting records.
        *state.lane_save_fault.lock().unwrap() = Some(LaneSaveFault::PreRename);
        let response = record_interaction(
            State(state),
            Json(InteractionBody {
                id: "telepathos:direct".into(),
                interaction_id: expired_entry.interaction_id.clone(),
                interaction_created_at_ms: now,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(parse_interaction_snapshot(&ledger_path).unwrap().is_empty());
        let journal = fs::read_to_string(&journal_path).unwrap();
        assert!(journal.contains("reused-after-horizon"));

        let mut restarted_registry = LaneRegistry::load(&path);
        let (restarted_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut restarted_registry).unwrap();
        assert!(reconciled);
        assert_eq!(restarted_registry.active().interactions, Some(2));
        assert_eq!(restarted_ledger.entries.len(), 1);
        assert_eq!(
            restarted_ledger.entries["reused-after-horizon"].interaction_created_at_ms,
            now
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn failed_expired_interaction_compaction_does_not_admit_reuse_and_retry_survives_restart()
    {
        let root = temp_directory("interaction-reuse-compaction-failure");
        let path = root.join("lanes.json");
        let ledger_path = root.join("interaction-ledger.json");
        let journal_path = interaction_journal_path(&ledger_path);
        let now = unix_time_millis().unwrap();
        let interaction_id = "retry-after-compaction-failure".to_string();
        let mut durable_ledger = InteractionLedger::default();
        durable_ledger.entries.insert(
            interaction_id.clone(),
            InteractionEntry {
                interaction_id: interaction_id.clone(),
                lane_id: "telepathos:direct".into(),
                interaction_created_at_ms: now
                    .saturating_sub(INTERACTION_DEDUPE_WINDOW_MS)
                    .saturating_sub(1),
                lane_interactions: 1,
            },
        );
        persist_interaction_ledger(&ledger_path, &durable_ledger).unwrap();

        let mut persisted_registry = LaneRegistry::default_direct();
        persisted_registry.lanes[0].interactions = Some(1);
        persisted_registry.save(&path).unwrap();
        let (loaded_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut persisted_registry).unwrap();
        assert!(!reconciled);
        let state = lane_mutation_test_state(path.clone(), ledger_path.clone());
        *state.reg.lock().await = persisted_registry;
        *state.interaction_ledger.lock().await = loaded_ledger;

        // `atomic_write_text` has renamed the replacement snapshot before its
        // directory sync. Treat that ambiguous point as a failed admission:
        // no new journal line may be appended in this process.
        inject_directory_sync_failure(containing_directory(&ledger_path));
        let body = || InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: interaction_id.clone(),
            interaction_created_at_ms: now,
        };
        let failed = record_interaction(State(state), Json(body())).await;
        assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !journal_path.exists()
                || !fs::read_to_string(&journal_path)
                    .unwrap()
                    .contains(&interaction_id)
        );

        // A restart accepts either side of the failed atomic replacement, and
        // the next admission can safely complete the reuse without a legacy
        // snapshot record colliding with the new journal line.
        let mut restarted_registry = LaneRegistry::load(&path);
        let (restarted_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut restarted_registry).unwrap();
        assert!(!reconciled);
        let restarted = lane_mutation_test_state(path.clone(), ledger_path.clone());
        *restarted.reg.lock().await = restarted_registry;
        *restarted.interaction_ledger.lock().await = restarted_ledger;
        let successful = record_interaction(State(restarted), Json(body())).await;
        assert_eq!(successful.status(), StatusCode::OK);

        let mut final_registry = LaneRegistry::load(&path);
        let (final_ledger, reconciled) =
            load_interaction_ledger(&ledger_path, &mut final_registry).unwrap();
        assert!(!reconciled);
        assert_eq!(final_registry.active().interactions, Some(2));
        assert_eq!(final_ledger.entries.len(), 1);
        assert_eq!(
            final_ledger.entries[&interaction_id].interaction_created_at_ms,
            now
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn interaction_count_json_safe_boundary_rejects_without_mutation() {
        let path = temp_path("interaction-safe-boundary");
        let ledger_path = temp_path("interaction-safe-boundary-ledger");
        let journal_path = interaction_journal_path(&ledger_path);
        let created_at_ms = unix_time_millis().unwrap();
        let mut registry = LaneRegistry::default_direct();
        registry.lanes[0].interactions = Some(MAX_SAFE_SEQUENCE);
        registry.save(&path).unwrap();

        let seed_entry = InteractionEntry {
            interaction_id: "i-safe-boundary-seed".into(),
            lane_id: "telepathos:direct".into(),
            interaction_created_at_ms: created_at_ms,
            lane_interactions: MAX_SAFE_SEQUENCE,
        };
        append_interaction_journal(&journal_path, &seed_entry).unwrap();
        let journal_before = fs::read(&journal_path).unwrap();
        let registry_before = registry.clone();
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(registry)),
            path: path.clone(),
            transcript: Arc::new(TranscriptStore::default()),
            relay: Arc::new(RelayState::default()),
            msg_seq: AtomicU64::new(0),
            registry_revision: AtomicU64::new(17),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger {
                entries: [(seed_entry.interaction_id.clone(), seed_entry)]
                    .into_iter()
                    .collect(),
                journal_entries: 1,
                needs_durable_compaction: false,
            }),
            interaction_ledger_path: ledger_path.clone(),
            lane_save_fault: std::sync::Mutex::new(None),
        });
        let ledger_before = state.interaction_ledger.lock().await.clone();
        let revision_before = state.registry_revision.load(Ordering::SeqCst);
        let registry_snapshot_before = fs::read(&path).unwrap();

        let response = record_interaction(
            State(state.clone()),
            Json(InteractionBody {
                id: "telepathos:direct".into(),
                interaction_id: "i-safe-boundary-new".into(),
                interaction_created_at_ms: created_at_ms,
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(*state.reg.lock().await, registry_before);
        assert_eq!(
            state.registry_revision.load(Ordering::SeqCst),
            revision_before
        );
        assert_eq!(fs::read(&path).unwrap(), registry_snapshot_before);
        assert_eq!(fs::read(&journal_path).unwrap(), journal_before);
        let ledger_after = state.interaction_ledger.lock().await.clone();
        assert_eq!(ledger_after.entries, ledger_before.entries);
        assert_eq!(ledger_after.journal_entries, ledger_before.journal_entries);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(ledger_path);
        let _ = fs::remove_file(journal_path);
    }

    #[test]
    fn interaction_recovery_rejects_unsafe_count_without_registry_reconciliation() {
        let ledger_path = temp_path("interaction-unsafe-recovery-ledger");
        let journal_path = interaction_journal_path(&ledger_path);
        let entry = InteractionEntry {
            interaction_id: "i-unsafe-recovery".into(),
            lane_id: "telepathos:direct".into(),
            interaction_created_at_ms: unix_time_millis().unwrap(),
            lane_interactions: MAX_SAFE_SEQUENCE + 1,
        };
        fs::write(
            &journal_path,
            format!("{}\n", serde_json::to_string(&entry).unwrap()),
        )
        .unwrap();
        let journal_before = fs::read(&journal_path).unwrap();
        let mut registry = LaneRegistry::default_direct();
        let registry_before = registry.clone();

        let error = load_interaction_ledger(&ledger_path, &mut registry).unwrap_err();

        assert!(error.to_string().contains("outside the JSON-safe limit"));
        assert_eq!(registry, registry_before);
        assert_eq!(fs::read(&journal_path).unwrap(), journal_before);

        let _ = fs::remove_file(ledger_path);
        let _ = fs::remove_file(journal_path);
    }

    #[tokio::test]
    async fn interaction_id_length_boundary_is_valid_and_oversize_is_non_mutating() {
        let path = temp_path("interaction-id-length");
        let ledger_path = temp_path("interaction-id-length-ledger");
        let journal_path = interaction_journal_path(&ledger_path);
        let created_at_ms = unix_time_millis().unwrap();
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(LaneRegistry::default_direct())),
            path: path.clone(),
            transcript: Arc::new(TranscriptStore::default()),
            relay: Arc::new(RelayState::default()),
            msg_seq: std::sync::atomic::AtomicU64::new(0),
            registry_revision: std::sync::atomic::AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path: ledger_path.clone(),
            lane_save_fault: std::sync::Mutex::new(None),
        });

        let body = |interaction_id: String| InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id,
            interaction_created_at_ms: created_at_ms,
        };
        let boundary_id = "i".repeat(MAX_OPAQUE_ID_BYTES);
        assert_eq!(boundary_id.len(), MAX_OPAQUE_ID_BYTES);
        assert_eq!(
            record_interaction(State(state.clone()), Json(body(boundary_id.clone())))
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            record_interaction(State(state.clone()), Json(body(boundary_id)))
                .await
                .status(),
            StatusCode::OK
        );

        let journal_before = fs::read(&journal_path).unwrap();
        let ledger = state.interaction_ledger.lock().await;
        let entry_count_before = ledger.entries.len();
        let journal_entry_count_before = ledger.journal_entries;
        drop(ledger);
        let interactions_before = state.reg.lock().await.active().interactions;
        let oversized_id = "o".repeat(MAX_OPAQUE_ID_BYTES + 1);
        assert_eq!(oversized_id.len(), MAX_OPAQUE_ID_BYTES + 1);
        assert_eq!(
            record_interaction(State(state.clone()), Json(body(oversized_id)))
                .await
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );

        let ledger = state.interaction_ledger.lock().await;
        assert_eq!(ledger.entries.len(), entry_count_before);
        assert_eq!(ledger.journal_entries, journal_entry_count_before);
        drop(ledger);
        assert_eq!(
            state.reg.lock().await.active().interactions,
            interactions_before
        );
        assert_eq!(fs::read(&journal_path).unwrap(), journal_before);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(&ledger_path);
        let _ = fs::remove_file(journal_path);
    }

    #[tokio::test]
    async fn interaction_ledger_bounds_journal_and_rejects_expired_or_conflicting_retries() {
        let path = temp_path("interaction-bounds");
        let ledger_path = temp_path("interaction-bounds-ledger");
        let created_at_ms = unix_time_millis().unwrap();
        let state = Arc::new(AppState {
            reg: Arc::new(Mutex::new(LaneRegistry::default_direct())),
            path: path.clone(),
            transcript: Arc::new(TranscriptStore::default()),
            relay: Arc::new(RelayState::default()),
            msg_seq: std::sync::atomic::AtomicU64::new(0),
            registry_revision: std::sync::atomic::AtomicU64::new(0),
            lane_persistence_uncertain: AtomicBool::new(false),
            interaction_persistence_uncertain: AtomicBool::new(false),
            interaction_ledger: Mutex::new(InteractionLedger::default()),
            interaction_ledger_path: ledger_path.clone(),
            lane_save_fault: std::sync::Mutex::new(None),
        });

        let body = |interaction_id: &str, interaction_created_at_ms| InteractionBody {
            id: "telepathos:direct".into(),
            interaction_id: interaction_id.into(),
            interaction_created_at_ms,
        };
        assert_eq!(
            record_interaction(
                State(state.clone()),
                Json(body("i-bounded-1", created_at_ms))
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            record_interaction(
                State(state.clone()),
                Json(body("i-bounded-1", created_at_ms.saturating_add(1))),
            )
            .await
            .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            record_interaction(
                State(state.clone()),
                Json(body(
                    "i-expired",
                    created_at_ms.saturating_sub(INTERACTION_DEDUPE_WINDOW_MS + 1),
                )),
            )
            .await
            .status(),
            StatusCode::GONE
        );
        assert_eq!(state.reg.lock().await.active().interactions, Some(1));

        let mut ledger = state.interaction_ledger.lock().await;
        for index in 0..MAX_INTERACTION_DEDUPE_ENTRIES {
            ledger.entries.insert(
                format!("i-capacity-{index}"),
                InteractionEntry {
                    interaction_id: format!("i-capacity-{index}"),
                    lane_id: "telepathos:direct".into(),
                    interaction_created_at_ms: created_at_ms,
                    lane_interactions: 1,
                },
            );
        }
        drop(ledger);
        assert_eq!(
            record_interaction(
                State(state.clone()),
                Json(body("i-capacity-new", created_at_ms))
            )
            .await
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(state.reg.lock().await.active().interactions, Some(1));

        let mut compactable = InteractionLedger::default();
        for index in 0..MAX_INTERACTION_JOURNAL_ENTRIES {
            let entry = InteractionEntry {
                interaction_id: format!("i-journal-{index}"),
                lane_id: "telepathos:direct".into(),
                interaction_created_at_ms: created_at_ms,
                lane_interactions: index as u64 + 1,
            };
            append_interaction_journal(&interaction_journal_path(&ledger_path), &entry).unwrap();
            compactable
                .entries
                .insert(entry.interaction_id.clone(), entry);
            compactable.journal_entries += 1;
        }
        compact_interaction_ledger(&ledger_path, &mut compactable).unwrap();
        assert_eq!(compactable.journal_entries, 0);
        let journal = fs::read_to_string(interaction_journal_path(&ledger_path)).unwrap();
        assert!(journal.is_empty());
        let mut restarted = LaneRegistry::default_direct();
        let (restarted_ledger, _) = load_interaction_ledger(&ledger_path, &mut restarted).unwrap();
        assert_eq!(
            restarted_ledger.entries.len(),
            MAX_INTERACTION_JOURNAL_ENTRIES
        );
        assert_eq!(
            restarted.active().interactions,
            Some(MAX_INTERACTION_JOURNAL_ENTRIES as u64)
        );

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(&ledger_path);
        let _ = fs::remove_file(interaction_journal_path(&ledger_path));
    }
}
