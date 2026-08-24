import https from "node:https";
import { randomUUID } from "node:crypto";
import { readFileSync, appendFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";
import { config } from "./config.js";
import { EnergyVad } from "./vad.js";
import { transcribe, Transcript } from "./transcriber.js";
import { ProviderResponseError } from "./provider-response.js";
import {
  parseControl,
  isValidOpaqueId,
  assertNever,
  type ReplyAck,
  type ReplyAckRetire,
  type ReplyReceived,
  type ServerMsg,
} from "./protocol.js";
import { InteractionState, InteractionEvent, transition, micOpen } from "./fsm.js";
import {
  loadLanes,
  mutateAndSaveLanes,
  activeLane,
  switchLane,
  createLane,
  touchLane,
  LaneNameError,
  LaneRegistry,
  laneSelectionRevision,
  laneNameValidationError,
} from "./lanes.js";
import { parseMeta, MetaAction } from "./meta.js";
import { runMetaAgent } from "./meta-agent.js";
import { mergeMetaLaneProposal, replaceLaneRegistry } from "./meta-lane-merge.js";
import {
  acknowledgeTelepathydDelivery,
  DeliveryReceipt,
  HermesReply,
  fetchTelepathydState,
  InteractionRetryExpiredError,
  recordTelepathydInteraction,
  respondViaHermes,
  respondViaTelepathydMeta,
  setCurrentLaneIdFn,
} from "./hermes.js";
import { currentTelepathydTargetIdentity } from "./target-scope.js";
import { sharedTokenMatches, startApiServer, TlsMaterial } from "./api.js";
import {
  InteractionOutbox,
  InteractionOutboxBlockedError,
  InteractionOutboxFullError,
  InteractionOutboxRecoverablePersistenceError,
  type InteractionRecord,
} from "./interaction-outbox.js";
import {
  MAX_STORED_REPLY_ACKS,
  MAX_STORED_REPLY_ACK_TOMBSTONES,
  ReplyAckStore,
  type ReplyAckBinding,
  type ReplyAckTombstone,
} from "./reply-ack-store.js";
import { ReplyAckOwnerHighWaterCache } from "./reply-ack-owner-cache.js";
import {
  MAX_REPLY_TEXT_BYTES,
  ReplyTextByteAccumulator,
  ReplyTextLimitError,
  isReplyTextWithinLimit,
} from "./reply-text.js";

/**
 * telepathy bridge — v0.1 stub brain.
 * Protocol: see ../README.md. The agent step is a placeholder ("echo");
 * Hermes/pi plugs in at `respond()`.
 *
 * Robustness:
 * - utterance buffers are capped (open mics + low VAD thresholds must not OOM us)
 * - text reply frames are emitted as JSON; audio playback is local to Android
 * - optional shared-token auth via TELEPATHY_TOKEN (client puts it in hello)
 */

const MAX_UTTERANCE_BYTES = 16000 * 2 * 60; // 60 s of 16 kHz PCM16
const PREROLL_BYTES = 16000 * 2 * 0.32;     // ~320 ms of pre-speech audio
const MAX_PREVALIDATION_AUDIO_BYTES = 16000 * 2 * 2; // cap a remote lane check at 2 s
const DEFAULT_CAPTURE_PREPARATION_DEADLINE_MS = 5_000;
const CAPTURE_PREPARATION_DEADLINE_MS = (() => {
  const raw = process.env.TELEPATHY_CAPTURE_PREPARATION_DEADLINE_MS;
  if (raw === undefined || raw === "") return DEFAULT_CAPTURE_PREPARATION_DEADLINE_MS;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > 10 * 60 * 1_000) {
    throw new Error("TELEPATHY_CAPTURE_PREPARATION_DEADLINE_MS must be an integer from 1 through 600000");
  }
  return value;
})();
const replyAckRetryTimers = new Map<string, NodeJS.Timeout>();
const replyAckInFlight = new Map<string, Promise<void>>();
const MAX_SEEN_TURN_TOKENS = 128;
const PHONE_GENERIC_ERROR_MESSAGE = "request failed";
const REPLY_TOO_LARGE_ERROR_MESSAGE = "reply exceeds the 512 KiB UTF-8 byte limit";
const SAFE_PUBLIC_ERROR_MESSAGES = new Set([
  "lane snapshot required",
  "lane selection changed before capture started",
  "capture preparation timed out",
  "nothing to replay yet",
]);
const SAFE_LANE_ERROR_MESSAGES = new Set([
  "lane name must be a string",
  "lane name must not be blank",
  "lane name is too long to produce a valid lane identifier",
  "lane capacity reached; use an existing conversation",
]);

function logPhoneError(error: unknown, context?: string): void {
  console.error(`phone-safe error${context === undefined ? "" : ` (${context})`}:`, error);
}

/**
 * Return only an allowlisted public message to the handset.  Exception
 * details stay in the bridge log; filesystem paths, provider bodies, and
 * arbitrary causes must never become phone-facing protocol data.
 */
export function phoneSafeErrorMessage(error: unknown, context?: "stt"): string {
  if (error instanceof ProviderResponseError) {
    return context === "stt" ? "stt provider unavailable" : "provider unavailable";
  }
  if (error instanceof ReplyTextLimitError) {
    return REPLY_TOO_LARGE_ERROR_MESSAGE;
  }
  if (error instanceof LaneNameError && SAFE_LANE_ERROR_MESSAGES.has(error.message)) {
    return error.message;
  }
  if (error instanceof InteractionOutboxFullError) {
    return "remote interaction outbox is full";
  }
  if (error instanceof InteractionOutboxBlockedError) {
    return error.message === "interaction retry expired"
      ? "remote interaction retry expired"
      : "remote interaction outbox unavailable";
  }
  if (error instanceof Error && SAFE_PUBLIC_ERROR_MESSAGES.has(error.message)) {
    return error.message;
  }
  logPhoneError(error, context);
  return context === "stt" ? "stt provider unavailable" : PHONE_GENERIC_ERROR_MESSAGE;
}

const REPLY_ACK_OWNER_ABANDONMENT_MS = durationFromEnvironment(
  "TELEPATHY_REPLY_ACK_ABANDONMENT_MS",
  24 * 60 * 60 * 1_000,
);
const REPLY_ACK_CONSUMED_RETENTION_MS = durationFromEnvironment(
  "TELEPATHY_REPLY_ACK_CONSUMED_RETENTION_MS",
  24 * 60 * 60 * 1_000,
);
const REPLY_ACK_TOMBSTONE_RETENTION_MS = durationFromEnvironment(
  "TELEPATHY_REPLY_ACK_TOMBSTONE_RETENTION_MS",
  7 * 24 * 60 * 60 * 1_000,
);
if (REPLY_ACK_TOMBSTONE_RETENTION_MS <= REPLY_ACK_CONSUMED_RETENTION_MS) {
  throw new Error("TELEPATHY_REPLY_ACK_TOMBSTONE_RETENTION_MS must exceed consumed retention");
}

const replyAckStore = new ReplyAckStore();
const replyAckBindings = new Map<string, ReplyAckBinding>();
const replyAckTombstones = new Map<string, ReplyAckTombstone>();
const activeReplyAckOwners = new Map<string, number>();
const replyAckProcessStartedAtMs = Date.now();
const recentlySeenReplyAckOwners = new ReplyAckOwnerHighWaterCache(replyAckProcessStartedAtMs);
const replyAckSnapshot = replyAckStore.loadSnapshot();
for (const binding of replyAckSnapshot.bindings) {
  // A v8 prepared binding contains a complete agent_end envelope and is
  // safe to replay. It cannot authorize telepathyd consumption until the
  // handset durably records and proves its receipt.
  replyAckBindings.set(
    `${binding.laneId}\u0000${binding.replyTo}\u0000${binding.afterSeq}\u0000${binding.throughSeq}`,
    binding,
  );
}
for (const tombstone of replyAckSnapshot.tombstones) {
  replyAckTombstones.set(replyAckKey(tombstone), tombstone);
}

function durationFromEnvironment(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > 31 * 24 * 60 * 60 * 1_000) {
    throw new Error(`${name} must be an integer from 1 through 2678400000`);
  }
  return value;
}

interface ClientState {
  vad: EnergyVad;
  utterance: Buffer[];
  utteranceBytes: number;
  preroll: Buffer[];      // rolling window kept so utterance starts aren't clipped
  prerollBytes: number;
  fsm: InteractionState;  // single source of truth for interaction lifecycle
  authenticated: boolean;
  /** Stable owner from hello.installation_id; null before the hello barrier. */
  installationId: string | null;
  cancelRequested: boolean; // double-tap stop (features.md M3)
  lastReply: string | null; // for triple-tap replay
  metaMode: boolean;        // double-pinch: next utterance goes to the meta agent
  metaModeTurnToken: string | null;
  captureLaneId: string | null; // snapshot sent by the phone before mic-open
  captureLaneRevision: number | null;
  captureTurnToken: string | null;
  captureLaneValidated: boolean;
  pendingCaptureAudio: Buffer[];
  pendingCaptureAudioBytes: number;
  captureGeneration: number;
  capturePreparation: CapturePreparation | null;
  captureRemoteInteraction: InteractionRecord | null;
  activeInteractionId: string | null;
  activeTurnToken: string | null;
  activeAbort: AbortController | null;
  pendingReplyAcks: Map<string, ReplyAckBinding>;
  seenTurnTokens: Set<string>;
}

interface CapturePreparation {
  socket: WebSocket;
  turnToken: string;
  generation: number;
  deadlineAtMs: number;
  audioStarted: boolean;
  timer: NodeJS.Timeout | null;
}

/** Pure ownership/deadline predicate used by the timer and deterministic tests. */
export function shouldExpireCapturePreparation(
  preparation: Pick<CapturePreparation, "socket" | "turnToken" | "generation" | "deadlineAtMs" | "audioStarted"> | null,
  socket: object,
  turnToken: string,
  generation: number,
  nowMs: number,
): boolean {
  return preparation !== null &&
    preparation.socket === socket &&
    preparation.turnToken === turnToken &&
    preparation.generation === generation &&
    !preparation.audioStarted &&
    nowMs >= preparation.deadlineAtMs;
}

const lanes: LaneRegistry = loadLanes();
const bridgeInstanceId = randomUUID();
let nextInteractionId = 0;
const interactionOutbox = new InteractionOutbox();
let interactionFlushPromise: Promise<void> | null = null;
let interactionRetryTimer: NodeJS.Timeout | null = null;
let interactionRetryDelayMs = 1_000;
let remoteOutboxPersistenceFailure: string | null = null;

/** Match hermesConfig(): whitespace-only configuration means local mode. */
function telepathydConfigured(): boolean {
  return Boolean(process.env.TELEPATHY_HERMES_URL?.trim());
}

function remoteTurnsUnavailableReason(): string | null {
  // InteractionOutbox.reserve() is the sole authority for outbox admission.
  // In particular, it must get a chance to sweep a recoverable reservation
  // deletion before deciding that capacity is full. Keep only bridge-wide
  // persistence gates here; a preflight on interactionOutbox.unavailableReason()
  // would bypass that cleanup.
  return remoteOutboxPersistenceFailure ??
    replyAckAvailabilityReason();
}

function markRemoteOutboxPersistenceFailure(error: unknown): void {
  if (error instanceof InteractionOutboxRecoverablePersistenceError) {
    console.error(
      "remote interaction outbox persistence failed before rename; the prior snapshot remains durable and a later write may recover:",
      error,
    );
    return;
  }
  remoteOutboxPersistenceFailure = "remote interaction outbox persistence failed; remote turns are paused";
  console.error(remoteOutboxPersistenceFailure, error);
}

/**
 * An expired record is a special case: it already represents a completed
 * remote side effect and must be retried to a durable terminal state before
 * new turns proceed. This pause is cleared by that retry; ordinary
 * pre-rename reserve/promote failures never use it.
 */
function pauseRemoteTurnsForInteractionOutboxRetry(error: unknown): void {
  remoteOutboxPersistenceFailure = "remote interaction outbox persistence failed; remote turns are paused";
  console.error(remoteOutboxPersistenceFailure, error);
}

function clearRemoteOutboxPersistenceFailure(): void {
  remoteOutboxPersistenceFailure = null;
}

/**
 * Interaction IDs are part of telepathyd's durable dedupe key. The bridge
 * instance UUID separates restarts; the sequence must span WebSockets so a
 * handset reconnect cannot reuse an ID allocated by its old connection.
 */
function allocateInteractionId(): string {
  return `i-${bridgeInstanceId}-${++nextInteractionId}`;
}

function scheduleInteractionOutboxRetry() {
  if (interactionRetryTimer !== null || !telepathydConfigured()) return;
  interactionRetryTimer = setTimeout(() => {
    interactionRetryTimer = null;
    flushInteractionOutbox();
  }, interactionRetryDelayMs);
  interactionRetryTimer.unref();
}

function flushInteractionOutbox() {
  if (interactionFlushPromise !== null || !telepathydConfigured()) return;
  interactionFlushPromise = (async () => {
    let records: InteractionRecord[];
    try {
      interactionOutbox.assertCurrentTarget();
      records = interactionOutbox.pending();
    } catch (error) {
      remoteOutboxPersistenceFailure = "remote interaction outbox persistence failed; remote turns are paused";
      console.error(remoteOutboxPersistenceFailure, error);
      return;
    }
    for (const record of records) {
      try {
        const targetIdentity = interactionOutbox.targetScope();
        if (currentTelepathydTargetIdentity() !== targetIdentity) {
          throw new Error("telepathyd target identity changed; durable interactions remain pending");
        }
        await recordTelepathydInteraction(
          record.laneId,
          record.interactionId,
          record.interactionCreatedAtMs,
          targetIdentity,
        );
        // A request may have completed against the old daemon while the
        // process configuration changed. Never retire that row under the new
        // target; it remains recoverable when the original target returns.
        if (currentTelepathydTargetIdentity() !== targetIdentity) {
          throw new Error("telepathyd target identity changed while interaction was in flight; durable interaction remains pending");
        }
        interactionOutbox.removeDelivered(record);
        interactionRetryDelayMs = 1_000;
      } catch (error) {
        if (error instanceof InteractionRetryExpiredError) {
          // Never discard a completed turn once telepathyd's seven-day dedupe
          // horizon has passed. Keep it as a terminal durable record and stop
          // accepting remote turns until an operator reconciles it.
          try {
            interactionOutbox.markExpired(record);
            // A successful durable expiration is itself fail-closed via
            // unavailableReason(). Do not leave a stale filesystem-error
            // reason around after the retry repairs the snapshot.
            clearRemoteOutboxPersistenceFailure();
            console.error("telepathyd interaction: retry expired; remote turns are paused:", error.message);
          } catch (persistenceError) {
            // The record remains pending when markExpired rolls back. Pausing
            // new remote turns prevents additional side effects while the
            // durable state is uncertain; retry so a transient filesystem
            // failure can still turn this into its terminal expired record.
            if (persistenceError instanceof InteractionOutboxRecoverablePersistenceError) {
              pauseRemoteTurnsForInteractionOutboxRetry(persistenceError);
            } else {
              markRemoteOutboxPersistenceFailure(persistenceError);
            }
            interactionRetryDelayMs = Math.min(interactionRetryDelayMs * 2, 30_000);
            scheduleInteractionOutboxRetry();
          }
          break;
        }
        console.error("telepathyd interaction: retrying durable activity record:", (error as Error).message);
        interactionRetryDelayMs = Math.min(interactionRetryDelayMs * 2, 30_000);
        scheduleInteractionOutboxRetry();
        break;
      }
    }
    try {
      if (interactionOutbox.pending().length > 0) scheduleInteractionOutboxRetry();
    } catch (error) {
      remoteOutboxPersistenceFailure = "remote interaction outbox persistence failed; remote turns are paused";
      console.error(remoteOutboxPersistenceFailure, error);
    }
  })().finally(() => {
    interactionFlushPromise = null;
  });
}

let tlsServer: ReturnType<typeof https.createServer> | null = null;
let wss: WebSocketServer | null = null;
const isLoopbackHost = (host: string) =>
  host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "[::1]";
const handshakeTimers = new WeakMap<WebSocket, NodeJS.Timeout>();

function startBridgeServer(): void {
  const tlsCertPath = process.env.TELEPATHY_TLS_CERT;
  const tlsKeyPath = process.env.TELEPATHY_TLS_KEY;
  if (Boolean(tlsCertPath) !== Boolean(tlsKeyPath)) {
    throw new Error("TELEPATHY_TLS_CERT and TELEPATHY_TLS_KEY must be configured together");
  }
  const tlsMaterial: TlsMaterial | undefined = tlsCertPath && tlsKeyPath
    ? { cert: readFileSync(tlsCertPath), key: readFileSync(tlsKeyPath) }
    : undefined;
  tlsServer = tlsMaterial ? https.createServer(tlsMaterial) : null;
  if ((!isLoopbackHost(config.host) || !isLoopbackHost(config.apiHost)) &&
      (!process.env.TELEPATHY_TOKEN || !tlsMaterial)) {
    throw new Error(
      "non-loopback endpoints require TELEPATHY_TOKEN and TELEPATHY_TLS_CERT/TELEPATHY_TLS_KEY",
    );
  }

  startApiServer(lanes, config.apiPort, config.apiHost, tlsMaterial);
  setCurrentLaneIdFn(() => lanes.activeId);
  const replyAckSweepInterval = setInterval(
    () => sweepExpiredConsumedReplyAcks(),
    Math.max(1, Math.min(REPLY_ACK_OWNER_ABANDONMENT_MS, REPLY_ACK_CONSUMED_RETENTION_MS, 60_000)),
  );
  replyAckSweepInterval.unref();

  wss = tlsServer
    ? new WebSocketServer({ server: tlsServer, maxPayload: 1 << 20 })
    : new WebSocketServer({ port: config.port, host: config.host, maxPayload: 1 << 20 });

  wss.on("connection", (ws) => {
  const state: ClientState = {
    vad: new EnergyVad(config.vadThreshold, config.vadSilenceMs, config.vadMinSpeechMs),
    utterance: [],
    utteranceBytes: 0,
    preroll: [],
    prerollBytes: 0,
    fsm: { phase: "listening" },
    // Hello is an ordering barrier even on an un-tokened local bridge. The
    // Android client queues it before any lane/audio/ack traffic.
    authenticated: false,
    installationId: null,
    cancelRequested: false,
    lastReply: null,
    metaMode: false,
    metaModeTurnToken: null,
    captureLaneId: null,
    captureLaneRevision: null,
    captureTurnToken: null,
    captureLaneValidated: false,
    pendingCaptureAudio: [],
    pendingCaptureAudioBytes: 0,
    captureGeneration: 0,
    capturePreparation: null,
    captureRemoteInteraction: null,
    activeInteractionId: null,
    activeTurnToken: null,
    activeAbort: null,
    pendingReplyAcks: new Map(),
    seenTurnTokens: new Set(),
  };
  console.log("client connected");
  const handshakeTimer = setTimeout(() => {
    if (!state.authenticated && ws.readyState === WebSocket.OPEN) {
      console.warn("closing client that did not complete hello handshake");
      ws.close(4008, "hello timeout");
    }
  }, 5_000);
  handshakeTimer.unref();
  handshakeTimers.set(ws, handshakeTimer);
  ws.on("message", (data, isBinary) => {
    if (!state.authenticated) {
      // The handshake is the only control frame accepted before auth. In
      // particular, do not drop hello itself or token-authenticated clients
      // can never become ready.
      if (isBinary) return;
      const raw = (data as Buffer).toString();
      if (parseControl(raw)?.tag !== "hello") return;
      return onControl(ws, state, raw);
    }
    if (isBinary) return onAudio(ws, state, data as Buffer);
    onControl(ws, state, (data as Buffer).toString());
  });
  ws.on("close", () => {
    const handshakeTimer = handshakeTimers.get(ws);
    if (handshakeTimer !== undefined) {
      clearTimeout(handshakeTimer);
      handshakeTimers.delete(ws);
    }
    console.log("client disconnected");
    if (state.installationId !== null) markReplyAckOwnerDisconnected(state.installationId);
    // A disconnected handset cannot hear a reply. Abort any in-flight STT,
    // meta, or Hermes work and prevent its finally block from recording a
    // completed interaction on behalf of a client that is gone.
    state.cancelRequested = true;
    const activeAbort = state.activeAbort;
    state.activeAbort = null;
    activeAbort?.abort();
    state.activeInteractionId = null;
    state.activeTurnToken = null;
    state.metaMode = false;
    state.metaModeTurnToken = null;
    clearCapturePreparation(state, ws);
    discardCaptureRemoteInteraction(state);
    state.captureLaneId = null;
    state.captureLaneRevision = null;
    state.captureTurnToken = null;
    state.captureLaneValidated = false;
    state.pendingCaptureAudio = [];
    state.pendingCaptureAudioBytes = 0;
    state.pendingReplyAcks.clear();
    state.utterance = [];
    state.utteranceBytes = 0;
    state.preroll = [];
    state.prerollBytes = 0;
    state.vad.reset();
  });
  ws.on("error", (e) => console.error("ws error:", e.message));
  });

// Recover completed interactions left by a bridge restart before they reached
// the authoritative daemon.
  flushInteractionOutbox();

  wss.on("error", handleListenError);
  tlsServer?.on("error", handleListenError);
  if (tlsServer) {
    tlsServer.listen(config.port, config.host);
  }

  console.log(`telepathy bridge listening on ${tlsServer ? "wss" : "ws"}://${config.host}:${config.port} (stt=${config.stt}${process.env.TELEPATHY_TOKEN ? " auth=on" : ""})`);

  for (const sig of ["SIGINT", "SIGTERM"] as const) {
    process.on(sig, () => {
      console.log(`\n${sig} — closing ${wss!.clients.size} connection(s)`);
      for (const c of wss!.clients) c.close(1001, "server shutting down");
      wss!.close(() => {
        if (tlsServer?.listening) tlsServer.close(() => process.exit(0));
        else process.exit(0);
      });
      setTimeout(() => process.exit(0), 2000).unref();
    });
  }
}

function handleListenError(e: NodeJS.ErrnoException) {
  if (e.code === "EADDRINUSE") {
    console.error(`port ${config.port} already in use — is another bridge running?`);
  } else {
    console.error("server error:", e.message);
  }
  process.exit(1);
}

/** Apply an event to the client's machine; broadcast phase-name changes to the phone. */
function step(ws: WebSocket, state: ClientState, event: InteractionEvent) {
  const prev = state.fsm;
  const next = transition(prev, event);
  state.fsm = next;
  if (next.phase !== prev.phase) {
    console.log("fsm:", prev.phase, "→", next.phase);
    send(ws, { type: "phase", value: next.phase });
  }
}

function onAudio(ws: WebSocket, state: ClientState, pcm: Buffer) {
  const phase = state.fsm.phase;
  // capturing still consumes audio — only thinking/speaking close the mic
  if (phase !== "listening" && phase !== "capturing") return;
  // Raw PCM frames cannot carry a token. A valid lane snapshot is therefore
  // the capture-start fence: audio that arrives before one (including queued
  // frames from a cancelled turn) has no turn to belong to and is discarded.
  if (state.captureTurnToken === null) return;
  // This synchronous fence runs before buffering or VAD work. Once this exact
  // socket/generation has received audio, its reservation belongs to a real
  // capture and the preparation deadline may never cancel it.
  const preparation = state.capturePreparation;
  if (preparation !== null &&
      preparation.socket === ws &&
      preparation.turnToken === state.captureTurnToken &&
      preparation.generation === state.captureGeneration) {
    preparation.audioStarted = true;
    clearCapturePreparationTimer(preparation);
  }
  // In remote mode the lane revision is checked asynchronously against the
  // authoritative daemon. Keep a small bounded prefix while that check is in
  // flight so a lane switch cannot silently route the first words elsewhere.
  if (!state.captureLaneValidated) {
    state.pendingCaptureAudio.push(pcm);
    state.pendingCaptureAudioBytes += pcm.length;
    while (state.pendingCaptureAudioBytes > MAX_PREVALIDATION_AUDIO_BYTES) {
      const dropped = state.pendingCaptureAudio.shift();
      if (dropped === undefined) break;
      state.pendingCaptureAudioBytes -= dropped.length;
    }
    return;
  }

  if (phase === "listening") {
    // maintain rolling pre-roll so the first word isn't clipped by VAD ramp-up
    state.preroll.push(pcm);
    state.prerollBytes += pcm.length;
    while (state.prerollBytes - state.preroll[0].length > PREROLL_BYTES) {
      state.prerollBytes -= state.preroll.shift()!.length;
    }
    if (state.vad.process(pcm) === "start") {
      const preroll = [...state.preroll];
      const bytes = state.prerollBytes;
      state.preroll = [];
      state.prerollBytes = 0;
      state.utterance = preroll;
      state.utteranceBytes = bytes;
      step(ws, state, { kind: "SPEECH_START", prerollBytes: bytes });
      send(ws, { type: "speech_start" });
    }
    return;
  }

  // capturing
  state.utterance.push(pcm);
  state.utteranceBytes += pcm.length;

  // hard cap: force-end pathological/never-ending input instead of growing forever
  const forced = state.utteranceBytes >= MAX_UTTERANCE_BYTES;
  const ended = state.vad.process(pcm) === "end";

  if (ended || forced) {
    if (forced) {
      console.warn("utterance hit 60s cap — forcing end");
      state.vad.reset(); // otherwise VAD stays "speaking" and eats the next utterance
      step(ws, state, { kind: "FORCE_END" });
    } else {
      step(ws, state, { kind: "UTTERANCE_END" });
    }
    void handleUtterance(ws, state);
  } else {
    step(ws, state, { kind: "SPEECH_CHUNK", bytes: pcm.length });
  }
}

async function handleUtterance(ws: WebSocket, state: ClientState) {
  // Bind the whole interaction to the lane active when speech ended. The
  // agent may take long enough for a separate lane switch to occur.
  const captureLaneId = state.captureLaneId;
  const captureLaneRevision = state.captureLaneRevision;
  const turnToken = state.captureTurnToken;
  const captureGeneration = state.captureGeneration;
  // onAudio only enters capturing after a token-bound lane snapshot. Keep the
  // guard anyway: no unbound capture may ever reach STT or Hermes.
  if (turnToken === null) {
    step(ws, state, { kind: "CANCEL" });
    return;
  }
  clearCapturePreparation(state, ws, turnToken, captureGeneration);
  const remoteInteraction = state.captureRemoteInteraction;
  if (telepathydConfigured() && remoteInteraction === null) {
    send(ws, { type: "error", message: "remote turns paused: no durable activity-record reservation" });
    step(ws, state, { kind: "CANCEL" });
    return;
  }
  const interactionLaneId = remoteInteraction?.laneId ?? captureLaneId ?? activeLane(lanes).id;
  const interactionId = remoteInteraction?.interactionId ?? allocateInteractionId();
  const abortController = new AbortController();
  state.activeInteractionId = interactionId;
  state.activeTurnToken = turnToken;
  state.activeAbort = abortController;
  state.captureLaneId = null;
  state.captureLaneRevision = null;
  state.captureTurnToken = null;
  state.captureRemoteInteraction = null;
  state.captureLaneValidated = false;
  state.pendingCaptureAudio = [];
  state.pendingCaptureAudioBytes = 0;
  let transcribed = false;
  let remoteInteractionPromoted = false;
  const pcm = Buffer.concat(state.utterance);
  state.utterance = [];
  state.utteranceBytes = 0;
  const samples = pcm.length >> 1;
  send(ws, { type: "utterance", samples });
  state.cancelRequested = false;

  try {
    // Once the authoritative daemon is configured, an unsnapshotted phone
    // turn is unsafe: the Node registry may describe a different lane.
    if (telepathydConfigured()) {
      if (captureLaneId === null || captureLaneRevision === null) {
        send(ws, { type: "error", message: "lane snapshot required" });
        return;
      }
    }

    // PCM16@16k → WAV container for STT
    const wav = wrapWav(pcm, 16000);
    const t0 = Date.now();
    let transcript: Transcript | null;
    try {
      transcript = await transcribe(wav, abortController.signal);
    } catch (e) {
      // STT failure must not be routed to Hermes as if it were user text.
      if (isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) {
        send(ws, { type: "error", message: phoneSafeErrorMessage(e, "stt") });
      }
      return;
    }
    if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
    const text = transcript?.text ?? "(transcription unavailable)";
    if (remoteInteraction !== null) {
      try {
        // Persist the completed turn before it can cause a remote side effect.
        interactionOutbox.promote(remoteInteraction);
        remoteInteractionPromoted = true;
        flushInteractionOutbox();
      } catch (error) {
        markRemoteOutboxPersistenceFailure(error);
        send(ws, { type: "error", message: `remote turns paused: ${phoneSafeErrorMessage(error)}` });
        return;
      }
    }
    transcribed = true;
    send(ws, {
      type: "stt",
      text,
      ...(transcript?.confidence !== undefined && { confidence: transcript.confidence }),
      ...(process.env.TELEPATHY_REPO && { repo: process.env.TELEPATHY_REPO }),
      turn_token: turnToken,
      interaction_id: interactionId,
    });
    console.log(`stt (${Date.now() - t0}ms):`, text,
      transcript?.confidence !== undefined ? `[conf ${transcript.confidence}]` : "");

    if (state.cancelRequested || !isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;

    // ---- meta agent plane: double-pinch or codeword routes here, never to Hermes ----
    const codeword = text.match(/^(meta|telepathy)[,: ]+(.*)$/i);
    if (state.metaMode || codeword) {
      const stripped = codeword ? codeword[2] : text;
      let reply: string;
      const remoteReply = await respondViaTelepathydMeta(stripped, abortController.signal);
      if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
      if (remoteReply !== null) {
        reply = remoteReply;
      } else {
        const action = parseMeta(stripped, lanes);
        if (action.op === "unknown" && config.metaModel) {
          // grammar miss → steering agent (LLM with lane tools)
          const proposalBase = structuredClone(lanes);
          const baseSelectionRevision = laneSelectionRevision(lanes);
          const proposed = structuredClone(proposalBase);
          const proposalBefore = structuredClone(proposed);
          reply = await runMetaAgent(
            { baseUrl: config.metaBaseUrl, apiKey: process.env.OPENAI_API_KEY ?? "", model: config.metaModel },
            proposed, stripped, abortController.signal,
          );
          if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
          // An invalid create_lane tool call leaves the private proposal
          // unchanged. Do not turn that read-only result into a durable
          // snapshot write; valid tool mutations retain the existing
          // transactional durability behavior.
          if (JSON.stringify(proposed) !== JSON.stringify(proposalBefore)) {
            const applyProposal = () => replaceLaneRegistry(
              lanes,
              mergeMetaLaneProposal(
                proposalBase,
                proposed,
                lanes,
                baseSelectionRevision,
                laneSelectionRevision(lanes),
              ),
            );
            if (!telepathydConfigured()) {
              mutateAndSaveLanes(lanes, applyProposal);
            } else {
              applyProposal();
            }
          }
        } else {
          reply = telepathydConfigured()
            ? executeMeta(action)
            : executeMetaTurn(action);
        }
      }
      if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
      if (!await streamReply(ws, state, turnToken, interactionId, reply, abortController.signal)) return;
      if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
      finish(ws, state, turnToken, interactionId, reply);
      } else {
        const response = await respond(text, state, interactionLaneId, abortController.signal);
        if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
        if (!await streamReply(ws, state, turnToken, interactionId, response.text, abortController.signal)) return;
        if (!isCurrentInteraction(state, interactionId, turnToken, abortController.signal)) return;
        finish(ws, state, turnToken, interactionId, response.text, response.receipt);
      }
  } catch (e) {
    if (e instanceof ReplyTextLimitError) {
      // No agent_end or durable receipt is emitted for an unfinishable reply.
      // Abort any upstream request and let finally restore the bridge to listening.
      if (!abortController.signal.aborted) {
        send(ws, { type: "error", message: phoneSafeErrorMessage(e) });
        abortController.abort();
      }
    } else if (!abortController.signal.aborted) {
      send(ws, { type: "error", message: phoneSafeErrorMessage(e) });
    }
  } finally {
    if (remoteInteraction !== null && !remoteInteractionPromoted) {
      cancelRemoteInteractionReservation(remoteInteraction);
    }
    const ownsInteraction = state.activeInteractionId === interactionId;
    if (ownsInteraction) {
      if (state.activeAbort === abortController) state.activeAbort = null;
      state.activeInteractionId = null;
      state.activeTurnToken = null;
      if (transcribed && remoteInteraction === null) {
        try {
          mutateAndSaveLanes(lanes, () => {
            const lane = lanes.lanes.find((candidate) => candidate.id === interactionLaneId) ?? activeLane(lanes);
            lane.interactions = (lane.interactions ?? 0) + 1;
            touchLane(lanes, lane.id);
          });
        } catch (error) {
          // The interaction has already completed.  Persistence failure must
          // not strand this websocket in processing or leave uncommitted
          // stats visible in the shared registry.
          console.error("cannot persist standalone lane activity:", error);
          if (!abortController.signal.aborted) {
            send(ws, { type: "error", message: `lane activity was not persisted: ${phoneSafeErrorMessage(error)}` });
          }
        }
      }
      state.metaMode = false; // one-shot plane
      // whatever happened, land back on listening so the mic can reopen on next pinch
      if (!micOpen(state.fsm)) step(ws, state, { kind: "CANCEL" });
      if (!state.cancelRequested) send(ws, { type: "listening" });
    }
  }
}

function isCurrentInteraction(
  currentState: ClientState,
  id: string,
  turnToken: string,
  signal: AbortSignal,
): boolean {
  return currentState.activeInteractionId === id &&
    currentState.activeTurnToken === turnToken &&
    !signal.aborted;
}

/** A command may only affect the token-bound capture or interaction in flight. */
function matchesCurrentTurn(state: ClientState, turnToken: string): boolean {
  return state.captureTurnToken === turnToken || state.activeTurnToken === turnToken;
}

function reserveTurnToken(state: ClientState, turnToken: string): boolean {
  if (state.seenTurnTokens.has(turnToken)) return false;
  while (state.seenTurnTokens.size >= MAX_SEEN_TURN_TOKENS) {
    const oldest = state.seenTurnTokens.values().next().value;
    if (oldest === undefined) break;
    state.seenTurnTokens.delete(oldest);
  }
  state.seenTurnTokens.add(turnToken);
  return true;
}

function replyAckKey(binding: Pick<ReplyAckBinding, "laneId" | "replyTo" | "afterSeq" | "throughSeq">): string {
  return `${binding.laneId}\u0000${binding.replyTo}\u0000${binding.afterSeq}\u0000${binding.throughSeq}`;
}

function replyAckAvailabilityReason(): string | null {
  if (replyAckStore.unavailableReason() !== null) return replyAckStore.unavailableReason();
  if (replyAckBindings.size < MAX_STORED_REPLY_ACKS) return null;
  return replyAckTombstones.size >= MAX_STORED_REPLY_ACK_TOMBSTONES
    ? "reply acknowledgement capacity is full; retaining durable playback completion records and unexpired terminal tombstones"
    : "reply acknowledgement capacity is full; retaining durable playback completion records";
}

function replyAckTargetIsCurrent(): boolean {
  try {
    replyAckStore.assertCurrentTarget();
    return true;
  } catch (error) {
    console.error("reply acknowledgement target scope is fenced:", (error as Error).message);
    return false;
  }
}

function noteReplyAckOwnerSeen(installationId: string, nowMs = Date.now()): void {
  recentlySeenReplyAckOwners.note(installationId, nowMs);
  recentlySeenReplyAckOwners.prune(activeReplyAckOwners, [
    ...replyAckBindings.values(),
    ...replyAckTombstones.values(),
  ]);
}

function activeReplyAckOwner(installationId: string): boolean {
  return (activeReplyAckOwners.get(installationId) ?? 0) > 0;
}

/**
 * Wall-clock timestamps are the only clock that survives a bridge restart.
 * A clock rollback must delay reconciliation, never make an owner expire
 * early. The process-start floor is conservative after a restart whose last
 * close event could not be persisted.
 */
function replyAckOwnerLastSeenAt(binding: ReplyAckBinding): number {
  return Math.max(
    binding.ownerLastSeenAtMs,
    recentlySeenReplyAckOwners.lastSeenAt(binding.installationId) ?? 0,
    replyAckProcessStartedAtMs,
  );
}

/** Include the current observation only when this path represents a sighting. */
function replyAckOwnerLastSeenAtForPersistence(binding: ReplyAckBinding, nowMs = Date.now()): number {
  return Math.max(replyAckOwnerLastSeenAt(binding), nowMs);
}

function replyAckOwnerSeenAtForPersistence(installationId: string, nowMs: number): number {
  return Math.max(
    recentlySeenReplyAckOwners.lastSeenAt(installationId) ?? 0,
    nowMs,
    replyAckProcessStartedAtMs,
  );
}

function replyAckOwnerIsAbandoned(binding: ReplyAckBinding, nowMs: number): boolean {
  const lastSeenAtMs = replyAckOwnerLastSeenAt(binding);
  return nowMs >= lastSeenAtMs && nowMs - lastSeenAtMs >= REPLY_ACK_OWNER_ABANDONMENT_MS;
}

function consumedReplyAckIsExpired(binding: ReplyAckBinding, nowMs: number): boolean {
  return binding.state === "consumed" &&
    binding.consumedAtMs !== null &&
    nowMs >= binding.consumedAtMs &&
    nowMs - binding.consumedAtMs >= REPLY_ACK_CONSUMED_RETENTION_MS;
}

function replyAckTombstoneIsExpired(tombstone: ReplyAckTombstone, nowMs: number): boolean {
  return nowMs >= tombstone.tombstonedAtMs &&
    nowMs - tombstone.tombstonedAtMs >= REPLY_ACK_TOMBSTONE_RETENTION_MS;
}

function tombstoneFor(binding: ReplyAckBinding, nowMs: number): ReplyAckTombstone {
  return {
    targetIdentity: binding.targetIdentity,
    installationId: binding.installationId,
    laneId: binding.laneId,
    replyTo: binding.replyTo,
    afterSeq: binding.afterSeq,
    throughSeq: binding.throughSeq,
    turnToken: binding.turnToken,
    interactionId: binding.interactionId,
    consumedAtMs: binding.consumedAtMs!,
    tombstonedAtMs: nowMs,
  };
}

class ReplyAckTombstoneCapacityError extends Error {
  constructor(required: number, available: number) {
    super(
      `reply acknowledgement terminal capacity is full; retaining ${required} consumed ` +
      `binding${required === 1 ? "" : "s"} that still need exact terminal proof ` +
      `(${available} tombstone slot${available === 1 ? "" : "s"} available)`,
    );
  }
}

function pruneExpiredReplyAckTombstones(
  tombstones: Map<string, ReplyAckTombstone>,
  nowMs: number,
): boolean {
  let changed = false;
  for (const [key, tombstone] of tombstones) {
    if (replyAckTombstoneIsExpired(tombstone, nowMs)) {
      tombstones.delete(key);
      changed = true;
    }
  }
  return changed;
}

/** Keep the terminal namespace bounded without evicting an unexpired proof. */
function retainReplyAckTombstone(
  nextTombstones: Map<string, ReplyAckTombstone>,
  tombstone: ReplyAckTombstone,
  nowMs: number,
): void {
  pruneExpiredReplyAckTombstones(nextTombstones, nowMs);
  const key = replyAckKey(tombstone);
  if (!nextTombstones.has(key) && nextTombstones.size >= MAX_STORED_REPLY_ACK_TOMBSTONES) {
    throw new ReplyAckTombstoneCapacityError(1, 0);
  }
  nextTombstones.set(key, tombstone);
}

interface ReplyAckReclaimResult {
  changed: boolean;
  blocked: boolean;
  reclaimed: Set<string>;
}

/**
 * Plan terminal reclamation as one transaction. Expired tombstones are safe
 * to remove first, but a consumed binding is deleted only if every candidate
 * in this sweep has a terminal slot. The caller persists the returned maps
 * only after this complete preflight succeeds.
 */
function reclaimExpiredConsumedReplyAcks(
  nextBindings: Map<string, ReplyAckBinding>,
  nextTombstones: Map<string, ReplyAckTombstone>,
  candidates: Iterable<[string, ReplyAckBinding]>,
  nowMs: number,
): ReplyAckReclaimResult {
  let changed = pruneExpiredReplyAckTombstones(nextTombstones, nowMs);
  const pending = [...candidates].filter(([key]) => nextBindings.has(key));
  const available = MAX_STORED_REPLY_ACK_TOMBSTONES - nextTombstones.size;
  if (pending.length > available) {
    console.error(new ReplyAckTombstoneCapacityError(pending.length, Math.max(0, available)).message);
    return { changed, blocked: true, reclaimed: new Set() };
  }

  const reclaimed = new Set<string>();
  for (const [key, binding] of pending) {
    nextBindings.delete(key);
    retainReplyAckTombstone(nextTombstones, tombstoneFor(binding, nowMs), nowMs);
    reclaimed.add(key);
    changed = true;
  }
  return { changed, blocked: false, reclaimed };
}

function persistedReplyAckBindings(nextBindings: Map<string, ReplyAckBinding>): Map<string, ReplyAckBinding> {
  const normalized = new Map<string, ReplyAckBinding>();
  for (const [key, binding] of nextBindings) {
    const ownerLastSeenAtMs = replyAckOwnerLastSeenAt(binding);
    normalized.set(
      key,
      ownerLastSeenAtMs === binding.ownerLastSeenAtMs
        ? binding
        : { ...binding, ownerLastSeenAtMs },
    );
  }
  return normalized;
}

/** Persist a snapshot after preserving every in-memory owner high-water mark. */
function persistReplyAckBindings(
  nextBindings: Map<string, ReplyAckBinding>,
  nextTombstones: Map<string, ReplyAckTombstone> = replyAckTombstones,
): Map<string, ReplyAckBinding> {
  const normalized = persistedReplyAckBindings(nextBindings);
  replyAckStore.save(normalized.values(), nextTombstones.values());
  replyAckBindings.clear();
  for (const [key, binding] of normalized) replyAckBindings.set(key, binding);
  replyAckTombstones.clear();
  for (const [key, tombstone] of nextTombstones) replyAckTombstones.set(key, tombstone);
  recentlySeenReplyAckOwners.prune(activeReplyAckOwners, [
    ...replyAckBindings.values(),
    ...replyAckTombstones.values(),
  ]);
  return normalized;
}

function saveReplyAckBindings(
  nextBindings: Map<string, ReplyAckBinding>,
  nextTombstones: Map<string, ReplyAckTombstone> = replyAckTombstones,
): boolean {
  try {
    persistReplyAckBindings(nextBindings, nextTombstones);
  } catch (error) {
    console.error("reply acknowledgement reconciliation persistence failed:", (error as Error).message);
    return false;
  }
  return true;
}

/**
 * Reconcile only under a new server-issued ownership decision. A client can
 * never migrate a binding by sending an old receipt: this function runs after
 * hello, persists the new installation owner first, and only then replays.
 *
 * prepared and received bindings are retained because telepathyd still owns
 * their delivery. An abandoned received proof is installation-local, so the
 * replacement installation starts at prepared and must prove its own local
 * durable copy. consumed bindings no longer represent telepathyd ownership;
 * their retention expiry can therefore reclaim capacity without losing a
 * remotely owned reply.
 */
function reconcileReplyAcksForInstallation(state: ClientState, nowMs = Date.now()): void {
  if (!replyAckTargetIsCurrent()) return;
  const installationId = state.installationId;
  if (installationId === null) return;
  noteReplyAckOwnerSeen(installationId, nowMs);
  const nextBindings = new Map(replyAckBindings);
  const nextTombstones = new Map(replyAckTombstones);
  const reclaimCandidates = [...replyAckBindings].filter(([, binding]) =>
    binding.state === "consumed" &&
    consumedReplyAckIsExpired(binding, nowMs) &&
    !replyAckInFlight.has(replyAckKey(binding))
  );
  const reclaim = reclaimExpiredConsumedReplyAcks(
    nextBindings,
    nextTombstones,
    reclaimCandidates,
    nowMs,
  );
  let changed = reclaim.changed;
  for (const [key, binding] of replyAckBindings) {
    if (reclaim.reclaimed.has(key)) continue;
    if (binding.installationId === installationId) {
      const ownerLastSeenAtMs = replyAckOwnerLastSeenAtForPersistence(binding, nowMs);
      if (binding.ownerLastSeenAtMs !== ownerLastSeenAtMs) {
        nextBindings.set(key, { ...binding, ownerLastSeenAtMs });
        changed = true;
      }
      continue;
    }
    if (activeReplyAckOwner(binding.installationId)) continue;
    if (binding.state === "consumed") continue;
    if (!replyAckOwnerIsAbandoned(binding, nowMs)) continue;
    // An old consume task may still finish after this ownership transfer.
    // Exact delivery consumption is idempotent in telepathyd; fence the old
    // task by replacing the map object and cancel its retry timer. The task
    // checks that identity before persisting `consumed` or sending a stale
    // confirmation, while the new owner starts from its own prepared proof.
    const retryTimer = replyAckRetryTimers.get(key);
    if (retryTimer !== undefined) {
      clearTimeout(retryTimer);
      replyAckRetryTimers.delete(key);
    }
    replyAckInFlight.delete(key);
    nextBindings.set(key, {
      ...binding,
      installationId,
      state: "prepared",
      preparedAtMs: nowMs,
      ownerLastSeenAtMs: replyAckOwnerLastSeenAtForPersistence(binding, nowMs),
      receivedAtMs: null,
      consumedAtMs: null,
    });
    changed = true;
  }
  for (const [key, tombstone] of replyAckTombstones) {
    if (replyAckTombstoneIsExpired(tombstone, nowMs)) {
      nextTombstones.delete(key);
      changed = true;
    }
  }
  if (changed && saveReplyAckBindings(nextBindings, nextTombstones)) {
    // replayPreparedReplyAcks() runs after this function and sends only these
    // newly authorized prepared records to the hello installation.
  }
}

/** Reclaim only terminal records whose external delivery is already consumed. */
function sweepExpiredConsumedReplyAcks(nowMs = Date.now()): void {
  if (!replyAckTargetIsCurrent()) return;
  const nextBindings = new Map(replyAckBindings);
  const nextTombstones = new Map(replyAckTombstones);
  const reclaimCandidates = [...replyAckBindings].filter(([, binding]) =>
    !activeReplyAckOwner(binding.installationId) &&
    consumedReplyAckIsExpired(binding, nowMs) &&
    !replyAckInFlight.has(replyAckKey(binding))
  );
  const reclaim = reclaimExpiredConsumedReplyAcks(
    nextBindings,
    nextTombstones,
    reclaimCandidates,
    nowMs,
  );
  if (reclaim.changed) saveReplyAckBindings(nextBindings, nextTombstones);
}

function markReplyAckOwnerConnected(installationId: string): void {
  activeReplyAckOwners.set(installationId, (activeReplyAckOwners.get(installationId) ?? 0) + 1);
  noteReplyAckOwnerSeen(installationId);
}

function markReplyAckOwnerDisconnected(installationId: string): void {
  const count = activeReplyAckOwners.get(installationId) ?? 0;
  if (count <= 1) activeReplyAckOwners.delete(installationId);
  else activeReplyAckOwners.set(installationId, count - 1);
  noteReplyAckOwnerSeen(installationId);
  // Persist the close timestamp when possible. The in-memory recent-seen map
  // remains a conservative fence if the filesystem is temporarily unavailable.
  const nextBindings = new Map(replyAckBindings);
  let changed = false;
  for (const [key, binding] of replyAckBindings) {
    const ownerLastSeenAtMs = replyAckOwnerLastSeenAt(binding);
    if (binding.installationId === installationId && binding.ownerLastSeenAtMs !== ownerLastSeenAtMs) {
      nextBindings.set(key, {
        ...binding,
        ownerLastSeenAtMs,
      });
      changed = true;
    }
  }
  if (changed) saveReplyAckBindings(nextBindings);
}

function sameReplyAckBinding(a: ReplyAckBinding, b: ReplyAckBinding): boolean {
  return a.targetIdentity === b.targetIdentity &&
    a.installationId === b.installationId &&
    a.laneId === b.laneId &&
    a.replyTo === b.replyTo &&
    a.afterSeq === b.afterSeq &&
    a.throughSeq === b.throughSeq &&
    a.turnToken === b.turnToken &&
    a.interactionId === b.interactionId &&
    a.replyText === b.replyText;
}

/** The replayable application envelope is durable before its first send. */
function agentEndFor(binding: ReplyAckBinding): ServerMsg {
  return {
    type: "agent_end",
    text: binding.replyText,
    turn_token: binding.turnToken,
    interaction_id: binding.interactionId,
    lane_id: binding.laneId,
    reply_to: binding.replyTo,
    after_seq: binding.afterSeq,
    through_seq: binding.throughSeq,
  };
}

/**
 * Re-send only envelopes the handset has not yet durably recorded.  A send is
 * intentionally not a state transition: a close after ws.send may still mean
 * the handset saw nothing.
 */
function replayPreparedReplyAcks(ws: WebSocket, state: ClientState): void {
  const installationId = state.installationId;
  if (installationId === null || !replyAckTargetIsCurrent()) return;
  for (const binding of replyAckBindings.values()) {
    if (binding.state !== "prepared" || binding.installationId !== installationId) continue;
    state.pendingReplyAcks.set(replyAckKey(binding), binding);
    if (!send(ws, agentEndFor(binding))) return;
  }
}

function prepareReplyAck(
  state: ClientState,
  receipt: DeliveryReceipt,
  turnToken: string,
  interactionId: string,
  replyText: string,
): ReplyAckBinding | null {
  const installationId = state.installationId;
  if (installationId === null) {
    throw new Error("cannot prepare reply acknowledgement before installation hello");
  }
  if (receipt.targetIdentity !== currentTelepathydTargetIdentity()) {
    throw new Error("telepathyd target identity changed; remote reply acknowledgement remains pending");
  }
  sweepExpiredConsumedReplyAcks();
  const nowMs = Date.now();
  const binding: ReplyAckBinding = {
    targetIdentity: receipt.targetIdentity,
    installationId,
    laneId: receipt.laneId,
    replyTo: receipt.replyTo,
    afterSeq: receipt.afterSeq,
    throughSeq: receipt.throughSeq,
    turnToken,
    interactionId,
    replyText,
    state: "prepared",
    preparedAtMs: nowMs,
    ownerLastSeenAtMs: replyAckOwnerSeenAtForPersistence(installationId, nowMs),
    receivedAtMs: null,
    consumedAtMs: null,
  };
  const key = replyAckKey(binding);
  const unavailableReason = replyAckAvailabilityReason();
  if (unavailableReason !== null) {
    // A failed post-rename directory sync may already have committed a
    // receipt binding. Do not send another agent_end or mutate that snapshot
    // until the store has been reconciled outside this process.
    console.error(`remote replies paused: ${unavailableReason}`);
    return null;
  }
  // A handset may reconnect after hearing the reply but before its ack is
  // delivered. Keep the authorization in the bridge process, not only in the
  // old socket's ClientState, so the new socket can retry that exact receipt.
  const existing = replyAckBindings.get(key);
  if (existing !== undefined) {
    if (!sameReplyAckBinding(existing, binding)) {
      throw new Error("reply acknowledgement receipt conflicts with an outstanding binding");
    }
    state.pendingReplyAcks.set(key, existing);
    return existing;
  }
  if (replyAckTombstones.has(key)) {
    throw new Error("reply acknowledgement receipt conflicts with a retained terminal tombstone");
  }
  const capacityReason = replyAckAvailabilityReason();
  if (capacityReason !== null) {
    // Do not send agent_end without a durable authorization binding: the
    // handset would play a reply it can never safely acknowledge.
    console.error(`remote replies paused: ${capacityReason}`);
    return null;
  }
  // Persist the complete replay envelope before agent_end is sent. A bridge
  // restart resends this exact binding instead of inferring receipt from a
  // local WebSocket handoff.
  const nextBindings = new Map(replyAckBindings);
  nextBindings.set(key, binding);
  try {
    persistReplyAckBindings(nextBindings);
  } catch (error) {
    // A post-rename failure may have committed this `prepared` record. It is
    // intentionally not installed in this process: a fresh bridge can safely
    // replay the full envelope, while `reply_ack` remains unauthorized until
    // it durably receives the handset's reply_received proof.
    throw error;
  }
  return replyAckBindings.get(key) ?? binding;
}

/** A global receipt record is usable only by the installation that owns it. */
function ownedReplyAckBinding(state: ClientState, key: string): ReplyAckBinding | undefined {
  const installationId = state.installationId;
  if (installationId === null) return undefined;
  const pending = state.pendingReplyAcks.get(key);
  if (pending !== undefined) {
    return pending.installationId === installationId ? pending : undefined;
  }
  const binding = replyAckBindings.get(key);
  return binding?.installationId === installationId ? binding : undefined;
}

/**
 * The handset may send this only after atomically persisting the replay
 * envelope locally. Persist that proof before allowing the later reply_ack
 * to consume telepathyd's delivery.
 */
function receiveReplyAck(ws: WebSocket, state: ClientState, msg: ReplyReceived): void {
  if (!replyAckTargetIsCurrent()) return;
  const key = replyAckKey(msg);
  const binding = ownedReplyAckBinding(state, key);
  if (binding === undefined ||
      binding.turnToken !== msg.turnToken ||
      binding.interactionId !== msg.interactionId) {
    return;
  }
  let received = binding;
  if (binding.state === "prepared") {
    const unavailableReason = replyAckStore.unavailableReason();
    if (unavailableReason !== null) {
      console.error(`reply receipt persistence is unavailable: ${unavailableReason}`);
      return;
    }
    received = { ...binding, state: "received", receivedAtMs: Date.now() };
    const nextBindings = new Map(replyAckBindings);
    nextBindings.set(key, received);
    try {
      persistReplyAckBindings(nextBindings);
    } catch (error) {
      console.error("reply receipt persistence failed:", (error as Error).message);
      return;
    }
    received = replyAckBindings.get(key) ?? received;
    state.pendingReplyAcks.set(key, received);
  }
  // The confirmation is only emitted after `prepared -> received` is durable.
  // Repeating it after reconnect is harmless and lets Android retry a lost
  // confirmation without replaying or consuming the delivery.
  void sendAcknowledgement(ws, {
    type: "reply_received",
    lane_id: received.laneId,
    reply_to: received.replyTo,
    after_seq: received.afterSeq,
    through_seq: received.throughSeq,
    turn_token: received.turnToken,
    interaction_id: received.interactionId,
  });
}

function acknowledgeReplyAck(ws: WebSocket, state: ClientState, msg: ReplyAck) {
  if (!replyAckTargetIsCurrent()) return;
  const key = replyAckKey(msg);
  const binding = ownedReplyAckBinding(state, key);
  const tombstone = replyAckTombstones.get(key);
  if (binding === undefined && tombstone !== undefined) {
    if (tombstone.installationId !== state.installationId ||
        tombstone.turnToken !== msg.turnToken ||
        tombstone.interactionId !== msg.interactionId) return;
    // The external delivery was consumed before this record was reclaimed.
    // A late exact ack is only a confirmation replay; it never calls telepathyd.
    void sendAcknowledgement(ws, {
      type: "reply_acknowledged",
      lane_id: tombstone.laneId,
      reply_to: tombstone.replyTo,
      after_seq: tombstone.afterSeq,
      through_seq: tombstone.throughSeq,
      turn_token: tombstone.turnToken,
      interaction_id: tombstone.interactionId,
    });
    return;
  }
  if (binding === undefined ||
      (binding.state !== "received" && binding.state !== "consumed") ||
      binding.turnToken !== msg.turnToken ||
      binding.interactionId !== msg.interactionId) {
    return;
  }
  const existingTimer = replyAckRetryTimers.get(key);
  if (existingTimer) {
    clearTimeout(existingTimer);
    replyAckRetryTimers.delete(key);
  }
  const confirmation: ServerMsg = {
    type: "reply_acknowledged",
    lane_id: binding.laneId,
    reply_to: binding.replyTo,
    after_seq: binding.afterSeq,
    through_seq: binding.throughSeq,
    turn_token: binding.turnToken,
    interaction_id: binding.interactionId,
  };
  if (replyAckInFlight.has(key)) return;
  const task = (async () => {
    let consumed = binding;
    if (binding.state === "received") {
      // Never tell Android to enter terminal retirement unless this bridge can
      // first durably record `received -> consumed`. A prepared record is
      // replay-only: it cannot authorize the external consume.
      const unavailableReason = replyAckStore.unavailableReason();
      if (unavailableReason !== null) {
        throw new Error(`reply acknowledgement persistence is unavailable: ${unavailableReason}`);
      }
      await acknowledgeTelepathydDelivery({ ...msg, targetIdentity: binding.targetIdentity });
      if (replyAckBindings.get(key) !== binding) return;
      // Persist external consumption before asking Android to durably record
      // it. If the bridge dies here, a restart either sees `consumed`, or
      // retries the idempotent telepathyd consume from `received`.
      consumed = { ...binding, state: "consumed" };
      consumed = { ...consumed, consumedAtMs: Date.now() };
      const nextBindings = new Map(replyAckBindings);
      nextBindings.set(key, consumed);
      persistReplyAckBindings(nextBindings);
      consumed = replyAckBindings.get(key) ?? consumed;
    }
    if (!await sendAcknowledgement(ws, confirmation)) return;
    if (state.pendingReplyAcks.get(key) === binding || state.pendingReplyAcks.get(key) === consumed) {
      state.pendingReplyAcks.delete(key);
    }
    })();
  replyAckInFlight.set(key, task);
  void task.catch((error) => {
    console.error("telepathyd delivery ack: failed to persist consumption:", (error as Error).message);
    if (!replyAckTargetIsCurrent()) return;
    if (state.pendingReplyAcks.get(key) !== binding && replyAckBindings.get(key) !== binding) return;
    const timer = setTimeout(() => {
      replyAckRetryTimers.delete(key);
      acknowledgeReplyAck(ws, state, msg);
    }, 1_000);
    timer.unref();
    replyAckRetryTimers.set(key, timer);
  }).finally(() => {
    if (replyAckInFlight.get(key) === task) replyAckInFlight.delete(key);
  });
}

/**
 * Retire a completed receipt only after Android has durably moved it into its
 * terminal retry state. The removal is persisted before the terminal reply.
 *
 * Once the removal is durable, a restarted bridge has no record to inspect.
 * Repeating `reply_ack_retire` is nevertheless safe to confirm: it cannot
 * authorize telepathyd consumption and Android only emits it after persisting
 * a prior `reply_acknowledged` frame. This makes the final frame idempotent
 * across a crash after removal but before its WebSocket handoff.
 */
function retireReplyAck(ws: WebSocket, state: ClientState, msg: ReplyAckRetire): void {
  if (!replyAckTargetIsCurrent()) return;
  const key = replyAckKey(msg);
  const binding = replyAckBindings.get(key);
  const confirmation: ServerMsg = {
    type: "reply_ack_retired",
    lane_id: msg.laneId,
    reply_to: msg.replyTo,
    after_seq: msg.afterSeq,
    through_seq: msg.throughSeq,
    turn_token: msg.turnToken,
    interaction_id: msg.interactionId,
  };
  if (binding === undefined) {
    const tombstone = replyAckTombstones.get(key);
    if (tombstone !== undefined) {
      if (tombstone.installationId !== state.installationId ||
          tombstone.turnToken !== msg.turnToken ||
          tombstone.interactionId !== msg.interactionId) return;
      const nextTombstones = new Map(replyAckTombstones);
      nextTombstones.delete(key);
      try {
        persistReplyAckBindings(replyAckBindings, nextTombstones);
      } catch (error) {
        console.error("reply acknowledgement tombstone retirement persistence failed:", (error as Error).message);
        return;
      }
    }
    void sendAcknowledgement(ws, confirmation);
    return;
  }
  if (binding.installationId !== state.installationId ||
      binding.state !== "consumed" ||
      binding.turnToken !== msg.turnToken ||
      binding.interactionId !== msg.interactionId) {
    return;
  }
  const nextBindings = new Map(replyAckBindings);
  nextBindings.delete(key);
  try {
    // Android's reply_ack_retire is its durable proof that it retained the
    // terminal retry record. Never free a slot merely after a transport send.
    persistReplyAckBindings(nextBindings);
  } catch (error) {
    console.error("reply acknowledgement retirement persistence failed:", (error as Error).message);
    return;
  }
  replyAckBindings.delete(key);
  state.pendingReplyAcks.delete(key);
  const timer = replyAckRetryTimers.get(key);
  if (timer !== undefined) {
    clearTimeout(timer);
    replyAckRetryTimers.delete(key);
  }
  void sendAcknowledgement(ws, confirmation);
}

/**
 * A websocket send callback is the earliest safe handoff point available to
 * the bridge: it reports that ws accepted the frame for the connected peer.
 * If the socket closed first, leave the durable reply-ack binding in place so
 * the phone can repeat its receipt on its next connection.
 */
function sendAcknowledgement(ws: WebSocket, message: ServerMsg): Promise<boolean> {
  if (ws.readyState !== WebSocket.OPEN) return Promise.resolve(false);
  const encoded = JSON.stringify(message);
  return new Promise((resolve) => {
    try {
      ws.send(encoded, (error) => resolve(
        (error === undefined || error === null) && ws.readyState === WebSocket.OPEN,
      ));
    } catch {
      resolve(false);
    }
  });
}

function finish(
  ws: WebSocket,
  state: ClientState,
  turnToken: string,
  interactionId: string,
  reply?: string,
  receipt?: DeliveryReceipt,
) {
  if (state.cancelRequested ||
      state.activeInteractionId !== interactionId ||
      state.activeTurnToken !== turnToken) {
    console.log("interaction cancelled by user");
    return;
  }
  const replyText = reply ?? "";
  if (!isReplyTextWithinLimit(replyText)) {
    send(ws, { type: "error", message: REPLY_TOO_LARGE_ERROR_MESSAGE });
    state.activeAbort?.abort();
    return;
  }
  // A remote reply is replayable only through its persisted receipt binding.
  // Evict a prior receipt-less reply before attempting that persistence, so a
  // failed preparation cannot leave Repeat able to fabricate an untracked
  // agent_end while the correlated delivery remains pending.
  if (receipt !== undefined) state.lastReply = null;
  let replyAckBinding: ReplyAckBinding | null = null;
  if (receipt) {
    try {
      replyAckBinding = prepareReplyAck(state, receipt, turnToken, interactionId, replyText);
      if (replyAckBinding === null) {
        send(ws, {
          type: "error",
          message: `remote replies paused: ${replyAckAvailabilityReason() ?? "reply acknowledgement persistence is unavailable"}`,
        });
        return;
      }
    } catch (error) {
      send(ws, { type: "error", message: `remote replies paused: ${phoneSafeErrorMessage(error)}` });
      return;
    }
  }
  // Local replies have no receipt lifecycle, so they retain the triple-tap
  // repeat behavior.
  if (reply !== undefined && receipt === undefined) state.lastReply = reply;
  const agentEnd: ServerMsg = replyAckBinding === null
    ? { type: "agent_end", text: replyText, turn_token: turnToken, interaction_id: interactionId }
    : agentEndFor(replyAckBinding);
  // This transport handoff deliberately has no durable state transition. The
  // prepared envelope remains replayable until Android proves its local copy.
  if (send(ws, agentEnd) && replyAckBinding !== null) {
    state.pendingReplyAcks.set(replyAckKey(replyAckBinding), replyAckBinding);
  }
}

function clearCapturePreparationTimer(preparation: CapturePreparation): void {
  if (preparation.timer !== null) {
    clearTimeout(preparation.timer);
    preparation.timer = null;
  }
}

function ownsCapturePreparation(
  ws: WebSocket,
  state: ClientState,
  turnToken: string,
  generation: number,
): boolean {
  const preparation = state.capturePreparation;
  return preparation !== null &&
    preparation.socket === ws &&
    preparation.turnToken === turnToken &&
    preparation.generation === generation &&
    state.captureTurnToken === turnToken &&
    state.captureGeneration === generation;
}

function clearCapturePreparation(
  state: ClientState,
  ws: WebSocket,
  turnToken?: string,
  generation?: number,
): void {
  const preparation = state.capturePreparation;
  if (preparation === null || preparation.socket !== ws) return;
  if (turnToken !== undefined && preparation.turnToken !== turnToken) return;
  if (generation !== undefined && preparation.generation !== generation) return;
  clearCapturePreparationTimer(preparation);
  state.capturePreparation = null;
}

function resetUnstartedCapturePreparation(
  ws: WebSocket,
  state: ClientState,
  turnToken: string,
  generation: number,
): void {
  if (!ownsCapturePreparation(ws, state, turnToken, generation)) return;
  const preparation = state.capturePreparation!;
  if (preparation.audioStarted) return;
  clearCapturePreparation(state, ws, turnToken, generation);
  discardCaptureRemoteInteraction(state);
  state.captureLaneId = null;
  state.captureLaneRevision = null;
  state.captureTurnToken = null;
  state.captureLaneValidated = false;
  state.pendingCaptureAudio = [];
  state.pendingCaptureAudioBytes = 0;
  state.metaMode = false;
  state.metaModeTurnToken = null;
  state.utterance = [];
  state.utteranceBytes = 0;
  state.preroll = [];
  state.prerollBytes = 0;
  state.vad.reset();
  send(ws, { type: "error", message: "capture preparation timed out" });
  send(ws, { type: "listening" });
}

function expireCapturePreparation(
  ws: WebSocket,
  state: ClientState,
  turnToken: string,
  generation: number,
): void {
  const preparation = state.capturePreparation;
  if (!shouldExpireCapturePreparation(preparation, ws, turnToken, generation, Date.now())) return;
  if (!ownsCapturePreparation(ws, state, turnToken, generation)) return;
  resetUnstartedCapturePreparation(ws, state, turnToken, generation);
}

function armCapturePreparationDeadline(preparation: CapturePreparation, state: ClientState): void {
  clearCapturePreparationTimer(preparation);
  if (preparation.audioStarted) return;
  const delay = Math.max(1, preparation.deadlineAtMs - Date.now());
  preparation.timer = setTimeout(() => {
    preparation.timer = null;
    expireCapturePreparation(
      preparation.socket,
      state,
      preparation.turnToken,
      preparation.generation,
    );
  }, delay);
  preparation.timer.unref();
}

function beginCapturePreparation(ws: WebSocket, state: ClientState, turnToken: string): number {
  const generation = ++state.captureGeneration;
  const preparation: CapturePreparation = {
    socket: ws,
    turnToken,
    generation,
    deadlineAtMs: Date.now() + CAPTURE_PREPARATION_DEADLINE_MS,
    audioStarted: false,
    timer: null,
  };
  state.capturePreparation = preparation;
  armCapturePreparationDeadline(preparation, state);
  return generation;
}

function reserveRemoteInteraction(state: ClientState, laneId: string): void {
  const reason = remoteTurnsUnavailableReason();
  if (reason !== null) throw new InteractionOutboxBlockedError(reason);
  const record: InteractionRecord = {
    laneId,
    interactionId: allocateInteractionId(),
    interactionCreatedAtMs: Date.now(),
  };
  try {
    interactionOutbox.reserve(record);
  } catch (error) {
    if (!(error instanceof InteractionOutboxFullError) && !(error instanceof InteractionOutboxBlockedError)) {
      markRemoteOutboxPersistenceFailure(error);
    }
    throw error;
  }
  state.captureRemoteInteraction = record;
}

function cancelRemoteInteractionReservation(record: InteractionRecord): void {
  try {
    interactionOutbox.cancelReservation(record);
  } catch (error) {
    markRemoteOutboxPersistenceFailure(error);
  }
}

function discardCaptureRemoteInteraction(state: ClientState): void {
  const record = state.captureRemoteInteraction;
  state.captureRemoteInteraction = null;
  if (record !== null) cancelRemoteInteractionReservation(record);
}

async function validateLaneSnapshot(
  ws: WebSocket,
  state: ClientState,
  laneId: string,
  revision: number | undefined,
  turnToken: string,
  generation: number,
) {
  try {
    if (revision === undefined) throw new Error("lane snapshot required");
    const remoteState = await fetchTelepathydState();
    if (remoteState === null ||
        remoteState.revision !== revision ||
        !remoteState.lanes.some((lane) => lane.id === laneId)) {
      throw new Error("lane selection changed before capture started");
    }
    if (!ownsCapturePreparation(ws, state, turnToken, generation)) return;
    if (shouldExpireCapturePreparation(state.capturePreparation, ws, turnToken, generation, Date.now())) {
      resetUnstartedCapturePreparation(ws, state, turnToken, generation);
      return;
    }
    reserveRemoteInteraction(state, laneId);
    state.captureLaneValidated = true;
    if (shouldExpireCapturePreparation(state.capturePreparation, ws, turnToken, generation, Date.now())) {
      resetUnstartedCapturePreparation(ws, state, turnToken, generation);
      return;
    }
    const pending = state.pendingCaptureAudio;
    state.pendingCaptureAudio = [];
    state.pendingCaptureAudioBytes = 0;
    for (const pcm of pending) {
      if (!ownsCapturePreparation(ws, state, turnToken, generation) || !state.captureLaneValidated) return;
      onAudio(ws, state, pcm);
    }
  } catch (error) {
    if (!ownsCapturePreparation(ws, state, turnToken, generation)) return;
    clearCapturePreparation(state, ws, turnToken, generation);
    discardCaptureRemoteInteraction(state);
    state.captureLaneId = null;
    state.captureLaneRevision = null;
    state.captureTurnToken = null;
    state.captureLaneValidated = false;
    state.pendingCaptureAudio = [];
    state.pendingCaptureAudioBytes = 0;
    state.metaMode = false;
    state.metaModeTurnToken = null;
    state.utterance = [];
    state.utteranceBytes = 0;
    state.preroll = [];
    state.prerollBytes = 0;
    state.vad.reset();
    const message = phoneSafeErrorMessage(error);
    if (message === "lane snapshot required") {
      send(ws, { type: "error", message });
    } else if (error instanceof InteractionOutboxFullError || error instanceof InteractionOutboxBlockedError) {
      send(ws, { type: "error", message: `remote turns paused: ${message}` });
    } else {
      send(ws, { type: "error", message: `lane snapshot: ${message}` });
    }
    send(ws, { type: "listening" });
  }
}

function onControl(ws: WebSocket, state: ClientState, raw: string) {
  const msg = parseControl(raw);
  if (msg === null) return; // garbage or unknown — ignore

  switch (msg.tag) {
    case "hello": {
      if (state.authenticated) return; // already handshaked
      if (process.env.TELEPATHY_TOKEN &&
          !sharedTokenMatches(process.env.TELEPATHY_TOKEN, msg.token)) {
        console.warn("auth failed — closing");
        ws.close(4001, "unauthorized");
        return;
      }
      state.authenticated = true;
      state.installationId = msg.installationId;
      markReplyAckOwnerConnected(msg.installationId);
      const handshakeTimer = handshakeTimers.get(ws);
      if (handshakeTimer !== undefined) {
        clearTimeout(handshakeTimer);
        handshakeTimers.delete(ws);
      }
      console.log("hello from", msg.device, "installation", msg.installationId);
      // The durable replay stream precedes ready. Android uses ready as its
      // capture gate, so it has persisted every recovered receipt before a
      // pending narration can consume the matching delivery.
      reconcileReplyAcksForInstallation(state);
      replayPreparedReplyAcks(ws, state);
      send(ws, { type: "ready" });
      break;
    }
    case "command": {
      console.log("command:", msg.kind);
      // exhaustive over the command union — a new kind fails compilation here
      switch (msg.kind) {
        case "stop":
          if (!matchesCurrentTurn(state, msg.turnToken)) break;
          state.metaMode = false;
          state.metaModeTurnToken = null;
          clearCapturePreparation(state, ws, msg.turnToken);
          discardCaptureRemoteInteraction(state);
          state.captureLaneId = null;
          state.captureLaneRevision = null;
          state.captureTurnToken = null;
          state.captureLaneValidated = false;
          state.pendingCaptureAudio = [];
          state.pendingCaptureAudioBytes = 0;
          state.utterance = [];
          state.utteranceBytes = 0;
          state.preroll = [];
          state.prerollBytes = 0;
          state.vad.reset();
          if (state.fsm.phase === "capturing") {
            state.cancelRequested = true;
            state.activeInteractionId = null;
            state.activeTurnToken = null;
            step(ws, state, { kind: "CANCEL" });
            send(ws, { type: "listening" });
          } else if (state.fsm.phase === "processing") {
            state.cancelRequested = true;
            const activeAbort = state.activeAbort;
            state.activeAbort = null;
            activeAbort?.abort();
            state.activeInteractionId = null;
            state.activeTurnToken = null;
            state.metaMode = false;
            step(ws, state, { kind: "CANCEL" });
            send(ws, { type: "listening" });
          }
          break;
        case "repeat":
          // A repeat starts its own reply-only turn. Do not let a delayed
          // repeat interrupt an armed capture or an active interaction.
          if (state.fsm.phase !== "listening" || state.captureTurnToken !== null || state.activeInteractionId !== null) break;
          if (!reserveTurnToken(state, msg.turnToken)) break;
          if (state.lastReply) void replayLast(ws, state, msg.turnToken);
          else send(ws, { type: "error", message: "nothing to replay yet" });
          break;
        case "cancel_capture":
          if (!matchesCurrentTurn(state, msg.turnToken)) break;
          state.metaMode = false;
          state.metaModeTurnToken = null;
          clearCapturePreparation(state, ws, msg.turnToken);
          discardCaptureRemoteInteraction(state);
          state.captureLaneId = null;
          state.captureLaneRevision = null;
          state.captureTurnToken = null;
          state.captureLaneValidated = false;
          state.pendingCaptureAudio = [];
          state.pendingCaptureAudioBytes = 0;
          state.utterance = [];
          state.utteranceBytes = 0;
          state.preroll = [];
          state.prerollBytes = 0;
          state.vad.reset();
          if (state.fsm.phase === "capturing") {
            step(ws, state, { kind: "CANCEL" });
          } else if (state.fsm.phase === "processing") {
            state.cancelRequested = true;
            const activeAbort = state.activeAbort;
            state.activeAbort = null;
            activeAbort?.abort();
            state.activeInteractionId = null;
            state.activeTurnToken = null;
            step(ws, state, { kind: "CANCEL" });
            send(ws, { type: "listening" });
          }
          break;
        default:
          assertNever(msg.kind);
      }
      break;
    }
    case "reply_received":
      receiveReplyAck(ws, state, msg);
      break;
    case "reply_ack":
      acknowledgeReplyAck(ws, state, msg);
      break;
    case "reply_ack_retire":
      retireReplyAck(ws, state, msg);
      break;
    case "utterance_end": {
      // explicit "send now" (tap while capturing) — beats waiting for VAD silence
      if (state.fsm.phase === "capturing" && state.captureTurnToken === msg.turnToken) {
        state.vad.reset();
        step(ws, state, { kind: "UTTERANCE_END" });
        void handleUtterance(ws, state);
      }
      break;
    }
    case "meta_mode": {
      // A meta arm may arrive immediately before or after its lane snapshot,
      // but it must name that exact capture turn. A delayed prior arm cannot
      // turn a newer normal capture into a meta request.
      if (state.activeInteractionId !== null) break;
      if (state.captureTurnToken === msg.turnToken) {
        state.metaMode = true;
        state.metaModeTurnToken = null;
        console.log("meta mode armed");
      } else if (state.fsm.phase === "listening" && state.captureTurnToken === null) {
        state.metaModeTurnToken = msg.turnToken;
      }
      break;
    }
    case "lane": {
      // Only accept a lane snapshot before speech starts. Once the server has
      // entered capturing/processing, later switches belong to later turns.
      if (state.fsm.phase === "listening" && state.captureTurnToken === null && state.activeInteractionId === null) {
        if (telepathydConfigured()) {
          const reason = remoteTurnsUnavailableReason();
          if (reason !== null) {
            send(ws, { type: "error", message: `remote turns paused: ${reason}` });
            send(ws, { type: "listening" });
            break;
          }
        }
        if (!reserveTurnToken(state, msg.turnToken)) break;
        // A lane snapshot starts a fresh capture preparation. This also
        // discards binary frames that were already queued when a prior
        // pre-VAD capture was cancelled.
        state.utterance = [];
        state.utteranceBytes = 0;
        state.preroll = [];
        state.prerollBytes = 0;
        state.vad.reset();
        state.captureLaneId = msg.id;
        state.captureLaneRevision = msg.revision ?? null;
        state.captureTurnToken = msg.turnToken;
        state.captureLaneValidated = !telepathydConfigured();
        state.captureRemoteInteraction = null;
        state.pendingCaptureAudio = [];
        state.pendingCaptureAudioBytes = 0;
        const captureGeneration = state.captureLaneValidated
          ? ++state.captureGeneration
          : beginCapturePreparation(ws, state, msg.turnToken);
        state.cancelRequested = false;
        state.metaMode = state.metaModeTurnToken === msg.turnToken;
        state.metaModeTurnToken = null;
        if (!state.captureLaneValidated) {
          void validateLaneSnapshot(ws, state, msg.id, msg.revision, msg.turnToken, captureGeneration);
        }
      }
      break;
    }
    default:
      assertNever(msg);
  }
}

/** Triple-tap: re-send the last reply as text; the phone speaks it again. */
async function replayLast(ws: WebSocket, state: ClientState, turnToken: string) {
  const reply = state.lastReply!;
  const interactionId = allocateInteractionId();
  const abortController = new AbortController();
  state.activeInteractionId = interactionId;
  state.activeTurnToken = turnToken;
  state.activeAbort = abortController;
  state.cancelRequested = false;
  try {
    if (!await streamReply(ws, state, turnToken, interactionId, reply, abortController.signal)) return;
    finish(ws, state, turnToken, interactionId, reply);
  } catch (e) {
    if (e instanceof ReplyTextLimitError) {
      send(ws, { type: "error", message: phoneSafeErrorMessage(e) });
      abortController.abort();
    } else if (!abortController.signal.aborted) {
      send(ws, { type: "error", message: phoneSafeErrorMessage(e) });
    }
  } finally {
    if (state.activeInteractionId === interactionId) {
      state.activeInteractionId = null;
      state.activeTurnToken = null;
      if (state.activeAbort === abortController) state.activeAbort = null;
    }
    if (!micOpen(state.fsm)) step(ws, state, { kind: "CANCEL" });
    send(ws, { type: "listening" });
  }
}

/**
 * Execute a parsed meta action against the lane registry.
 * No Hermes involvement — this plane must work even when the agent is down.
 */
export function executeMeta(action: MetaAction, reg: LaneRegistry = lanes): string {
  switch (action.op) {
    case "switch": {
      const lane = switchLane(reg, action.lane.id);
      return `Switched to ${lane.name}.`;
    }
    case "list": {
      const active = activeLane(reg);
      const list = reg.lanes
        .map((l) => `${l.name}${l.id === active.id ? " (active)" : ""}`)
        .join(", ");
      return `Conversations: ${list}.`;
    }
    case "new": {
      const invalid = laneNameValidationError(action.name);
      if (invalid) return invalid.message;
      try {
        const lane = createLane(reg, action.name);
        switchLane(reg, lane.id);
        return `Created ${lane.name}. You're in it.`;
      } catch (error) {
        if (error instanceof LaneNameError) return error.message;
        throw error;
      }
    }
    case "brief": {
      const lane = action.lane ?? activeLane(reg);
      const age = Math.round((Date.now() - new Date(lane.lastActive).getTime()) / 3600000);
      const ageText = age < 1 ? "under an hour" : `${age} hour${age > 1 ? "s" : ""}`;
      if (lane.id === "telepathy:direct") {
        return `Direct line to Hermes. No project context.`;
      }
      return `Lane ${lane.name}. Last active ${ageText} ago. Full briefing arrives with the Hermes connector.`;
    }
    case "note": {
      const line = JSON.stringify({ note: action.text, at: new Date().toISOString(), lane: activeLane(reg).id }) + "\n";
      try { appendFileSync("notes.jsonl", line); } catch {}
      return "Noted.";
    }
    case "fork": {
      const name = (action.name || `fork-${activeLane(reg).name}`).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
      const invalid = laneNameValidationError(name);
      if (invalid) return invalid.message;
      const lane = createLane(reg, name);
      switchLane(reg, lane.id);
      // context seeding runs in telepathyd when configured (transcript summary)
      return `Forked into ${lane.name}. Context carry-over requires the telepathyd daemon.`;
    }
    case "unknown":
      return "Meta commands: switch to name, list conversations, new conversation for name, brief, note that, fork.";
  }
}

/** Apply a local bridge meta turn transactionally, with invalid names fenced before save. */
export function executeMetaTurn(action: MetaAction, reg: LaneRegistry = lanes): string {
  const invalid = action.op === "new" ? laneNameValidationError(action.name) : null;
  if (invalid) return invalid.message;
  return mutateAndSaveLanes(reg, () => executeMeta(action, reg));
}

/**
 * Placeholder brain. Replace with the Hermes relay call; the lane's chat_id
 * (activeLane(lanes).id) is what stamps the relay MessageEvent.
 */
async function respond(
  text: string,
  _state: ClientState,
  laneId: string,
  signal: AbortSignal,
): Promise<HermesReply> {
  // Hermes plane when configured; echo stub otherwise.
  const hermesReply = await respondViaHermes(text, laneId, signal);
  if (hermesReply !== null) return hermesReply;
  return { text: `Heard you say: ${text}` };
}

async function streamReply(
  ws: WebSocket,
  state: ClientState,
  turnToken: string,
  interactionId: string,
  reply: string,
  signal: AbortSignal,
): Promise<boolean> {
  if (!isReplyTextWithinLimit(reply)) {
    throw new ReplyTextLimitError(`reply exceeds the ${MAX_REPLY_TEXT_BYTES} UTF-8 byte limit`);
  }
  const accumulator = new ReplyTextByteAccumulator();
  for (const delta of chunks(reply)) {
    signal.throwIfAborted();
    if (state.cancelRequested || !isCurrentInteraction(state, interactionId, turnToken, signal)) return false;
    if (!accumulator.append(delta)) {
      throw new ReplyTextLimitError(`reply exceeds the ${MAX_REPLY_TEXT_BYTES} UTF-8 byte limit`);
    }
    if (!send(ws, { type: "agent_delta", text: delta, turn_token: turnToken, interaction_id: interactionId })) return false;
  }
  return true;
}

function* chunks(s: string): Generator<string> {
  for (let i = 0; i < s.length; i += 80) yield s.slice(i, i + 80);
}

function send(ws: WebSocket, obj: ServerMsg): boolean {
  if (ws.readyState !== WebSocket.OPEN) return false;
  // Correlation-bearing frames are serialized only after the shared opaque-ID
  // contract has been checked. This keeps an oversized ID from inflating an
  // agent_end frame or escaping through a future producer.
  for (const key of ["interaction_id", "reply_to"] as const) {
    const value = (obj as Record<string, unknown>)[key];
    if (value !== undefined && !isValidOpaqueId(value)) return false;
  }
  try {
    ws.send(JSON.stringify(obj));
    return true;
  } catch {
    return false;
  }
}

function wrapWav(pcm: Buffer, sampleRate: number): Buffer {
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([header, pcm]);
}

if (process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1]) {
  startBridgeServer();
}
