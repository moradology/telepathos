# telepathy

Control coding agents by voice, through open-ear earbuds.

Pinch your Shokz OpenDots 2 → speak → a pi coding agent on your server does the
work → the answer is spoken back into your ears. Hands stay free, ears stay open.

## Architecture

```
OpenDots 2 (mic + speaker, HFP over BT)
   │
Pixel 9  —  Android app: pinch → VoiceInteractionService → capture PCM → WebSocket
   │          (media-button taps → JSON commands: stop / repeat / cancel_capture;
   │           tap-to-flush is the internal utterance-end gesture)
   │ cellular or wifi
3090 server  —  Node bridge:
   │             energy VAD → faster-whisper (Python worker) → pi SDK agent session
   │             → text agent_end → Android LocalAnnouncer (local TTS)
   └──────────► back down the same socket → earbuds
```

## Repo layout

- `server/` — TypeScript WebSocket bridge + pi SDK integration (runs anywhere; dev on Mac)
- `android/` — Kotlin client: assistant trigger, audio capture, WS client
- `docs/` — protocol and architecture notes

## Protocol (v5)

Single WebSocket connection. Text frames are JSON, binary frames are raw
PCM16 mono @ 16 kHz from the mic.

Client → server:
- `{"type":"hello","device":"opendots2","installation_id":"<stable opaque id>"}` — required first frame after connect; `installation_id` is a persisted per-installation owner ID (1–128 nonblank, non-control characters), never the human device label. The bridge drops every other client frame until it is accepted and returns `ready`.
- `{"type":"lane","id":"telepathy:direct","revision":7,"turn_token":"<uuid>"}` — starts a capture turn before any mic audio. `turn_token` is a fresh, opaque client value for each capture.
- binary — mic audio, accepted only after a token-bound lane frame
- `{"type":"utterance_end","turn_token":"<uuid>"}` — client-side end-of-speech (optional if server VAD is on)
- `{"type":"meta_mode","turn_token":"<uuid>"}` — directs that same capture turn to the meta agent
- `{"type":"command","command":"stop"|"repeat"|"cancel_capture","turn_token":"<uuid>"}` — from earbud taps. Repeat uses a newly generated token; Stop/Cancel echo the current capture token.
- `{"type":"reply_received",...,"turn_token":"<uuid>","interaction_id":"i-42"}` — proves Android durably stored the complete replay envelope; it never consumes the delivery
- `{"type":"reply_ack",...,"turn_token":"<uuid>","interaction_id":"i-42"}` — direct playback or saved-text recovery acknowledgement for an exact reply delivery, accepted only after `reply_received`
- `{"type":"reply_ack_retire",...,"turn_token":"<uuid>","interaction_id":"i-42"}` — sent only after the handset has durably recorded `reply_acknowledged`; asks the bridge to retire that completed delivery record

`turn_token` is mandatory on every turn-bound client frame, must be nonblank,
and may contain at most 128 UTF-16 code units. Receipt `after_seq` and
`through_seq` values are JSON-safe integers in `0..9_007_199_254_740_991`
(`through_seq > after_seq`). Untagged v1 controls are rejected; the protocol
has no compatibility mode.

`reply_to`, gateway `message_id`/`messageId` and `interaction_id` are opaque
identifiers. They must be nonblank, contain no C0/C1 control characters, and
fit in both 256 UTF-8 bytes and 256 UTF-16 code units. Whitespace and case are
significant; identifiers are never trimmed, normalized, or truncated. This
same grammar is enforced at every wire, API, gateway, durable-state, and
outbound-frame boundary.

Server → client:
- `{"type":"ready"}` — hello handshake ack; Android does not open capture or send reply traffic before it
- `{"type":"listening"}` / `{"type":"speech_start"}` / `{"type":"utterance","samples":N}` — VAD state
- `{"type":"stt","text":"...","turn_token":"<uuid>","interaction_id":"i-42",...}` — transcript;
  `confidence` present only when the backend reports it (faster-whisper), `repo` when
  `TELEPATHY_REPO` is set
- `{"type":"agent_delta","text":"...","turn_token":"<uuid>","interaction_id":"i-42"}` / `{"type":"agent_end","text":"<complete reply>","turn_token":"<uuid>","interaction_id":"i-42"}` — streamed text reply. A complete reply is limited to 512 KiB UTF-8 bytes; Node enforces the bound while streaming and Android enforces it independently for deltas and terminal frames. Android's `LocalAnnouncer` performs local TTS on the complete `agent_end` text. Receipt-bearing `agent_end` frames retain the complete text durably and are replayed before `ready` until Android proves it stored their receipt.
- `{"type":"reply_received",...}` — the bridge durably recorded Android's replay receipt, so playback or saved-text recovery may now advance that receipt
- `{"type":"reply_acknowledged",...}` — the bridge durably recorded that telepathyd consumed an exact reply delivery. The handset persists its terminal retry state before sending `reply_ack_retire`.
- `{"type":"reply_ack_retired",...}` — the bridge durably removed that completed delivery binding. Only then may the handset remove its terminal retry record.
- `{"type":"listening"}` — server VAD is live again after an interaction
- `{"type":"error","message":"..."}`

Reply receipt handling is a durable five-phase exchange: the bridge persists a
full `prepared` `agent_end` envelope; Android persists it locally and sends
`reply_received`; the bridge durably changes it to `received` and confirms;
only then may Android send `reply_ack` after playback or saved-text recovery.
Normal pending narration excludes receipt-owned correlated rows and consumes
only its exact spoken sequence IDs. The bridge persists `consumed`, then Android and the bridge complete
the existing `reply_ack_retire` / `reply_ack_retired` terminal handshake.
Prepared envelopes are replayed before `ready` only to the same
`installation_id` that owns them, and every confirmation is idempotent. The
WebSocket wire protocol is v5; the Node reply-ack persisted store hard-requires
v8; Android's local `ReplyAckSnapshot` hard-requires v8; and the Rust relay
delivery snapshot is v4, its inbound handoff snapshot is v1, and its relay
descriptor contract is v3. These are hard-cutover contracts; there is no
compatibility mode for earlier snapshots.

Node v8 retains up to 64 live receipt bindings and 64 exact terminal tombstones
separately. A consumed binding older than `TELEPATHY_REPLY_ACK_CONSUMED_RETENTION_MS`
is replaced by a tombstone so the original installation can send a late exact
`reply_ack` after reconnect; tombstones are bounded by
`TELEPATHY_REPLY_ACK_TOMBSTONE_RETENTION_MS` (default seven days) and are removed
by exact `reply_ack_retire` or expiry. Tombstones never rotate owners or call
telepathyd again.

Every Node durable reply binding, terminal tombstone, and interaction-outbox
record also carries a `target_identity`: a SHA-256 digest of the normalized
`TELEPATHY_HERMES_URL` and effective `TELEPATHY_TOKEN` auth configuration. The
token is never persisted. URL changes, credential rotation, and switching to or
from the local/unconfigured target fail closed at startup and at runtime; old
rows stay on disk and are only replayed, acknowledged, retired, or flushed
after the original target is restored. Node reply-ack and interaction-outbox
snapshots are hard-cut at v8 and v3 respectively. Interaction-outbox loads
reject snapshots over configured capacity before retaining records and bound
the shared opaque-ID grammar to interaction IDs.

## Lane registry contract

Node and `telepathyd` share a hard maximum of **256 total lanes**, including
`telepathy:direct`. The bound is enforced before a new lane can mutate memory
or replace `lanes.json`, and on every snapshot load and save; over-cap snapshots
are hard-rejected without migration or rewrite. Existing lanes remain usable at
capacity. A request for a new lane at capacity is a permanent `409` error with
`lane capacity reached; use an existing conversation`, not a retryable storage
failure.

The shared hard-cut metadata contract for every `lanes.json` row is:

- `id`, `active_id`, and `previous_id` use the existing ASCII lane-ID grammar
  (at most 128 UTF-8 bytes / UTF-16 units).
- `name` is nonempty and at most 128 UTF-8 bytes, 128 UTF-16 units, and 128
  Unicode scalar values. It is never trimmed or normalized during load.
- `created_at` and `last_active` are at most 64 UTF-8 bytes, UTF-16 units, and
  Unicode scalar values, and must be either `epoch-ms:<0..9007199254740991>`
  or an exact UTC `YYYY-MM-DDTHH:mm:ss.sssZ` calendar timestamp. Neither owner
  coerces or repairs a stored timestamp.
- `interactions`, when present, is a non-negative JSON-safe integer no larger
  than `9007199254740991`.

Malformed, over-bound, or out-of-range snapshots fail closed on restart and
are rejected before a save can replace an existing snapshot; there is no
migration or compatibility rewrite. Hermes database titles are not persisted,
but state enrichment truncates them on UTF-8 boundaries to 256 bytes and 128
Unicode scalar values. With 256 lanes, the maximum permitted metadata and
worst-case JSON escaping plus title enrichment serialize below 512 KiB, leaving
at least a 2× margin below the 1 MiB Node-to-`telepathyd` state-response cap.

## Auth model

Tailscale is the identity layer — services assume they run inside your
tailnet; no application auth. See `deploy/AUTH.md` for deployment, TLS via
`tailscale serve`, and the optional-token/relay-secret hardening knobs.

## Status

- [x] Server: WS bridge, VAD, whisper worker, pi session, text replies
- [x] Android: local agent-reply TTS via `LocalAnnouncer`
- [x] Android: pinch→assistant skeleton (the make-or-break test)
- [x] Android: full audio client with **capture-on-demand** (mic open on pinch,
      closed after `listening` — zero radio/mic power between interactions)
- [x] Typed interaction state machine (server-authoritative, phase broadcasts)
- [x] Steering agent: LLM tool-calling loop over the lane API, catches grammar
      misses (`TELEPATHY_META_MODEL` to enable); per-lane interaction stats
- [ ] Piper TTS on the 3090
- [ ] Hardware validation: pinch mapping, SCO routing, taps-during-SCO, carrier NAT

## Hermes relay contract (v3)

`telepathyd`'s authenticated Hermes gateway relay uses newline-delimited JSON
over its WebSocket. Each accepted gateway action receives a durable result:

Inbound voice turns use an explicit application handoff. The relay writes the
same versioned frame until the gateway has processed it and sends the exact
message identity and generation back:

```json
{"type":"inbound","handoffVersion":2,"messageId":"tp-42","generation":17,"event":{"message_id":"tp-42", "text":"hello"}}
```

```json
{"type":"inbound_ack","messageId":"tp-42","generation":17}
```

Only an exact `(messageId, generation)` ACK from the currently active,
authenticated connection can remove the durable inbound row. Transport write
success, channel acceptance, reconnect, or supersession is not an ACK. The
relay responds with `inbound_acknowledged` only after the removal and its
tombstone are durably persisted; if persistence is ambiguous it returns an
error and fences further durable mutations until daemon restart. The gateway
must retry the ACK after that restart. Repeating an exact
ACK is safe and returns `alreadyAcknowledged: true`; wrong, stale, unknown, or
malformed identities never remove a row. Duplicate API/channel copies are
collapsed by the active identity and a reconnect replays the same frame.

Inbound snapshot files are hard-cut at version 1 and reject the old unversioned
array format at startup. The durable inbound queue is bounded at 200 rows and
8 MiB. Exact acknowledgement tombstones are bounded at 200 entries and 32
KiB; the oldest tombstones may be evicted. A message ID may be reused only
with a new monotonically persisted generation, and an ACK for an evicted
tombstone is rejected. Generation exhaustion is terminal and requires a new
relay snapshot. The gateway must therefore retain and retry the exact pair
until it receives `alreadyAcknowledged` or the first successful confirmation.

For a synchronous Hermes voice-turn reply, the gateway's `send` action must set
`reply_to` to the exact `message_id` returned by `/api/message`; Node polls for
that reply using the same value. A `send` action without `reply_to` remains an
intentional uncorrelated delivery for cron/update messages and is narrated
through generic pending delivery. The relay never infers correlation from the
most recent voice turn.

```json
{"type":"outbound_result","requestId":"gateway-request-id","resultId":42,"result":{"success":true}}
```

`resultId` is a durable, relay-generated generation for that exact action.
The request/result namespace is the authenticated gateway identity plus
`requestId`; two gateways may use the same request ID without seeing,
deduplicating, or retiring one another's result. The unauthenticated local
development mode uses one explicit `unauthenticated` namespace.
After recording the result, the gateway must retry this acknowledgement until
it receives the matching retirement response:

```json
{"type":"outbound_result_ack","requestId":"gateway-request-id","resultId":42}
```

```json
{"type":"outbound_result_retired","requestId":"gateway-request-id","resultId":42,"result":{"success":true,"alreadyRetired":false}}
```

The relay durably removes the completed request only before sending that
retirement response. Repeating the same acknowledgement is safe and returns
`alreadyRetired: true`; an acknowledgement with the wrong result ID is
rejected. A request ID may be reused only after the gateway has received its
retirement response. This is a hard cutover from relay contract v1: gateways
that do not acknowledge results eventually hit the bounded request ledger and
are paused rather than having idempotency records silently evicted.

Each result is retained for a 24-hour retry/receipt window measured from its
durable creation. A result is abandoned only when that window has
elapsed and its gateway has no active relay connection; active gateway records
are never evicted to admit another identity. Abandonment writes a bounded exact
request/result tombstone, and leaves any already-created delivery in the phone
outbox. A retry after abandonment is therefore allowed to create a new
delivery, while a late acknowledgement for the old `resultId` cannot retire
the new generation. The relay persists a wall-clock high-water mark with the
ledger, so a clock rollback cannot make records expire early; old snapshot
versions are rejected at startup.

## Secure remote deployment

When `TELEPATHY_TOKEN` is set and a phone connects over a non-loopback
interface, run both endpoints with TLS. The bridge accepts PEM paths through
`TELEPATHY_TLS_CERT` and `TELEPATHY_TLS_KEY`; telepathyd uses the same variables
for its HTTPS lane API. Configure the phone with `wss://...:8787` and
`https://...:8790`. The daemon rejects a token-authenticated non-loopback bind
without the certificate pair, and the Android client refuses to send a token
over cleartext connections.

For local development without a token, `ws://` and `http://` remain supported
on loopback only. Set an explicit token, TLS certificate pair, and non-loopback
bind before exposing either endpoint to a network.

Remote interaction activity records are durably retried by the bridge and
deduplicated by `telepathyd` for seven days. A retry older than that is rejected
without incrementing the lane count; the bounded ledger refuses new records at
capacity rather than silently double-counting or dropping history.

## Steering-agent tool policy (permanent)

Capabilities are added ONLY as named, typed tools against bridge state —
never as primitives (no bash, no read_file, no grep, no write).
Rationale: the tool list IS the sandbox; prompt rules are just UX.
Current full surface: `list_lanes`, `active_lane`, `switch_lane`,
`create_lane`, `lane_stats`, `search_conversations`. New tools must be single-operation,
state-typed, and explicable in one sentence.
