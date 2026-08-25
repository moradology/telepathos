/**
 * Hermes delivery client: sends lane utterances to telepathosd (which pushes
 * them to the Hermes gateway over the relay), then polls for the agent's
 * replies until they arrive or we time out.
 *
 * Backpressure notes:
 * - Poll interval is 1 s: Hermes thinks for seconds; polling faster is waste.
 * - The cursor only advances on successful fetch; a failed poll retries the
 *   same window (telepathosd keeps unconsumed entries until picked up).
 */
import { ReplyTextByteAccumulator, ReplyTextLimitError, isReplyTextWithinLimit } from "./reply-text.js";
import { isValidLaneId, isValidOpaqueId, MAX_SAFE_SEQUENCE } from "./protocol.js";
import {
  normalizeTelepathosdBaseUrl,
  targetIdentityFor,
} from "./target-scope.js";

export interface HermesConfig {
  baseUrl: string;
  timeoutMs: number;
  targetIdentity: string;
  /** Captured for one operation; runtime env changes must not retarget it. */
  token?: string;
}

export interface TelepathosdLane {
  id: string;
  name: string;
  created_at: string;
  last_active: string;
  interactions?: number;
}

export interface TelepathosdState {
  lanes: TelepathosdLane[];
  active_id: string;
  previous_id: string;
  revision: number;
  active?: string;
}

/**
 * telepathosd's contracts deliberately keep phone-facing replies below 512 KiB.
 * Allow a small JSON envelope, but never let a peer choose an unbounded body.
 */
export const TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES = 64 * 1024;
export const TELEPATHOSD_REPLY_RESPONSE_MAX_BYTES = 576 * 1024;
export const TELEPATHOSD_STATE_RESPONSE_MAX_BYTES = 1024 * 1024;

export type TelepathosdResponseFailure =
  | "too-large"
  | "invalid-utf8"
  | "invalid-json"
  | "read-failed";

/** A deliberately body-free failure suitable for phone/API-facing callers. */
export class TelepathosdResponseError extends Error {
  constructor(public readonly failure: TelepathosdResponseFailure) {
    super(`telepathosd response ${failure}`);
    this.name = "TelepathosdResponseError";
  }
}

function contentLengthExceeds(response: Response, maxBytes: number): boolean {
  const header = response.headers.get("content-length");
  if (header === null || !/^[0-9]+$/.test(header)) return false;
  const length = Number(header);
  return Number.isSafeInteger(length) && length > maxBytes;
}

/**
 * Read one remote HTTP response without trusting Content-Length or chunk
 * boundaries. The TextDecoder is intentionally fatal and runs once only after
 * the complete, bounded byte sequence has been collected.
 */
export async function readTelepathosdJson(
  response: Response,
  maxBytes: number,
): Promise<unknown> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new TypeError("response byte limit must be a non-negative safe integer");
  }
  if (contentLengthExceeds(response, maxBytes)) {
    void response.body?.cancel().catch(() => undefined);
    throw new TelepathosdResponseError("too-large");
  }
  if (response.body === null) {
    throw new TelepathosdResponseError("invalid-json");
  }

  const reader = response.body.getReader();
  const chunks: Buffer[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = Buffer.from(value);
      if (chunk.length > maxBytes - total) {
        await reader.cancel().catch(() => undefined);
        throw new TelepathosdResponseError("too-large");
      }
      total += chunk.length;
      chunks.push(chunk);
    }
  } catch (error) {
    if (error instanceof TelepathosdResponseError) throw error;
    throw new TelepathosdResponseError("read-failed");
  } finally {
    reader.releaseLock();
  }

  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, total));
  } catch {
    throw new TelepathosdResponseError("invalid-utf8");
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new TelepathosdResponseError("invalid-json");
  }
}

function telepathosdHttpError(context: string, status: number): Error {
  if (status >= 400 && status < 500) {
    return new Error(`${context}: request rejected (${status})`);
  }
  return new Error(`${context}: unavailable`);
}

async function readSuccessfulTelepathosdJson(
  response: Response,
  maxBytes: number,
  context: string,
): Promise<unknown> {
  let body: unknown;
  try {
    body = await readTelepathosdJson(response, maxBytes);
  } catch (error) {
    // Some existing daemon 4xx handlers intentionally use a plain-text body.
    // The status remains useful (notably permanent 413) but the body never
    // crosses this boundary or becomes spoken text.
    if (!response.ok) throw telepathosdHttpError(context, response.status);
    throw error;
  }
  if (!response.ok) throw telepathosdHttpError(context, response.status);
  return body;
}

function isLoopbackHostname(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" ||
    hostname === "::1" || hostname === "[::1]";
}

/** Token-bearing bridge traffic must not leave the host over cleartext HTTP. */
export function telepathosdTransportError(baseUrl: string): string | null {
  if (!process.env.TELEPATHOS_TOKEN) return null;
  let parsed: URL;
  try {
    parsed = new URL(baseUrl);
  } catch {
    return "TELEPATHOS_HERMES_URL is not a valid URL";
  }
  if (parsed.protocol === "http:" && !isLoopbackHostname(parsed.hostname)) {
    return "TELEPATHOS_HERMES_URL must use https:// when TELEPATHOS_TOKEN is set unless it is loopback";
  }
  return null;
}

export function hermesConfig(): HermesConfig | null {
  const raw = process.env.TELEPATHOS_HERMES_URL;
  if (!raw || !raw.trim()) return null;
  const baseUrl = normalizeTelepathosdBaseUrl(raw);
  const transportError = telepathosdTransportError(baseUrl);
  if (transportError) throw new Error(transportError);
  const token = process.env.TELEPATHOS_TOKEN || undefined;
  return {
    baseUrl,
    timeoutMs: Number(process.env.TELEPATHOS_HERMES_TIMEOUT ?? 120_000),
    targetIdentity: targetIdentityFor(baseUrl, token),
    token,
  };
}

function telepathosdHeaders(cfg: HermesConfig): Record<string, string> {
  const headers: Record<string, string> = {};
  if (cfg.token) headers["x-telepathos-token"] = cfg.token;
  return headers;
}

export class TelepathosdTargetChangedError extends Error {}

/** Fail closed if a captured operation no longer matches process config. */
export function assertCurrentTelepathosdTarget(expected: string): HermesConfig {
  const cfg = hermesConfig();
  if (!cfg || cfg.targetIdentity !== expected) {
    throw new TelepathosdTargetChangedError(
      "telepathosd target identity changed; durable remote state remains pending until the original target is restored",
    );
  }
  return cfg;
}

/** telepathosd owns the lane registry used by both the phone and Hermes. */
export async function fetchTelepathosdState(
  cfg: HermesConfig | null = hermesConfig(),
  signal?: AbortSignal,
): Promise<TelepathosdState | null> {
  if (!cfg) return null;
  const r = await fetch(`${cfg.baseUrl}/api/state`, {
    headers: telepathosdHeaders(cfg),
    signal: requestSignal(signal, 2_000),
  });
  const state = await readSuccessfulTelepathosdJson(
    r,
    TELEPATHOSD_STATE_RESPONSE_MAX_BYTES,
    "telepathosd state",
  ) as TelepathosdState;
  if (!Array.isArray(state.lanes) ||
      typeof state.active_id !== "string" ||
      typeof state.previous_id !== "string" ||
      !Number.isSafeInteger(state.revision) || state.revision < 0) {
    throw new Error("telepathosd state: invalid lane registry");
  }
  if (!state.lanes.every((lane) => isValidLaneId(lane?.id)) ||
      !isValidLaneId(state.active_id) || !isValidLaneId(state.previous_id)) {
    throw new Error("telepathosd state: invalid lane ID");
  }
  return state;
}

interface Delivery {
  seq: number;
  chat_id: string;
  content: string;
  reply_to?: string | null;
}

interface DeliveryBatch {
  deliveries: Delivery[];
  latest: number;
}

/** A complete remote batch was not admissible for this polling window. */
class DeliveryBatchRejectedError extends Error {
  constructor() {
    super("telepathosd delivery: response rejected");
    this.name = "DeliveryBatchRejectedError";
  }
}

export interface DeliveryReceipt {
  laneId: string;
  replyTo: string;
  afterSeq: number;
  throughSeq: number;
  targetIdentity: string;
}

export interface HermesReply {
  text: string;
  receipt?: DeliveryReceipt;
  targetIdentity?: string;
}

/** Send an utterance for a lane; resolve with the concatenated agent reply. */
export async function deliverAndWait(
  cfg: HermesConfig,
  lanesRegistry: () => { id: string },
  text: string,
  requestedLaneId?: string,
  signal?: AbortSignal,
): Promise<string> {
  return (await deliverAndWaitWithReceipt(cfg, lanesRegistry, text, requestedLaneId, signal)).text;
}

/** Variant that keeps the relay delivery available until handset acknowledgement. */
export async function deliverAndWaitWithReceipt(
  cfg: HermesConfig,
  lanesRegistry: () => { id: string },
  text: string,
  requestedLaneId?: string,
  signal?: AbortSignal,
): Promise<HermesReply> {
  signal?.throwIfAborted();
  const remoteState = await fetchTelepathosdState(cfg, signal);
  // A voice interaction is bound to the lane that was active when speech
  // ended. The remote registry is the fallback for callers without a bound
  // interaction, but must not retarget an in-flight utterance after a switch.
  const lane = {
    id: requestedLaneId ?? remoteState?.active_id ?? lanesRegistry().id,
  };
  if (!isValidLaneId(lane.id)) throw new Error("invalid lane ID");
  // Capture the cursor before POST: a fast gateway reply may be queued while
  // /api/message is in flight. Reading latest afterwards would skip it.
  let cursor = await latestSeq(cfg, signal);
  const res = await fetch(`${cfg.baseUrl}/api/message`, {
    method: "POST",
    headers: { ...telepathosdHeaders(cfg), "Content-Type": "application/json" },
    body: JSON.stringify({ lane_id: lane.id, text }),
    signal: requestSignal(signal, cfg.timeoutMs),
  });
  const responseBody = await readSuccessfulTelepathosdJson(
    res,
    TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES,
    "hermes rejected utterance",
  ) as { message_id?: unknown };
  if (!isValidOpaqueId(responseBody.message_id)) {
    throw new Error("hermes rejected utterance: missing message_id");
  }
  const messageId = responseBody.message_id;

  // poll for replies addressed to this lane
  const deadline = Date.now() + cfg.timeoutMs;
  const parts: string[] = [];
  const replyBytes = new ReplyTextByteAccumulator();

  while (Date.now() < deadline) {
    await waitFor(1000, signal);
    const after = cursor;
    let batch: DeliveryBatch;
    try {
      batch = await fetchDeliveries(cfg, cursor, lane.id, messageId, signal);
    } catch (error) {
      if (!(error instanceof DeliveryBatchRejectedError)) throw error;
      // Reject the complete batch. In particular, do not move the cursor past
      // a response that could belong to another turn; the next poll retries
      // the same durable window and can only admit an exact batch.
      continue;
    }
    const matching = batch.deliveries;
    for (const d of matching) {
      const content = d.content.trim();
      if (parts.length > 0 && !replyBytes.append(" ")) {
        throw new ReplyTextLimitError("reply exceeds the 512 KiB UTF-8 byte limit");
      }
      if (!replyBytes.append(content)) {
        throw new ReplyTextLimitError("reply exceeds the 512 KiB UTF-8 byte limit");
      }
      parts.push(content);
    }
    cursor = Math.max(cursor, batch.latest);
    if (parts.length > 0) {
      const throughSeq = Math.max(...matching.map((delivery) => delivery.seq));
      if (!Number.isSafeInteger(after) || !Number.isSafeInteger(throughSeq) ||
          throughSeq <= after || throughSeq > batch.latest) {
        throw new DeliveryBatchRejectedError();
      }
      return {
        text: parts.join(" "),
        receipt: {
          laneId: lane.id,
          replyTo: messageId,
          afterSeq: after,
          throughSeq,
          targetIdentity: cfg.targetIdentity,
        },
      };
    }
    // quiet polling: keep waiting — async agents think in minutes sometimes
  }
  throw new Error(`no reply from hermes within ${cfg.timeoutMs / 1000}s`);
}

async function latestSeq(cfg: HermesConfig, signal?: AbortSignal): Promise<number> {
  // This endpoint deliberately returns only the durable sequence high-water
  // mark. Fetching /api/delivery?after=0 would deserialize every unrelated
  // pending reply merely to learn the cursor and could make a valid backlog
  // permanently block new turns at the response byte limit.
  const r = await fetch(`${cfg.baseUrl}/api/delivery/head`, {
    headers: telepathosdHeaders(cfg),
    signal: requestSignal(signal, 2_000),
  });
  let j: unknown;
  try {
    j = await readTelepathosdJson(r, TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES);
  } catch {
    throw new DeliveryBatchRejectedError();
  }
  if (!r.ok) throw new DeliveryBatchRejectedError();
  const latest = j !== null && typeof j === "object" && !Array.isArray(j)
    ? (j as { latest?: unknown }).latest
    : undefined;
  if (typeof latest !== "number" ||
      !Number.isSafeInteger(latest) ||
      latest < 0 || latest > MAX_SAFE_SEQUENCE) {
    throw new DeliveryBatchRejectedError();
  }
  return latest;
}

async function fetchDeliveries(
  cfg: HermesConfig,
  after: number,
  laneId: string,
  replyTo?: string,
  signal?: AbortSignal,
): Promise<DeliveryBatch> {
  const params = new URLSearchParams({
    after: String(after),
    consume: "false",
    lane_id: laneId,
  });
  if (replyTo !== undefined) params.set("reply_to", replyTo);
  const r = await fetch(`${cfg.baseUrl}/api/delivery?${params}`, {
    headers: telepathosdHeaders(cfg),
    signal: requestSignal(signal, 2_000),
  });
  let body: { deliveries?: unknown; latest?: unknown };
  try {
    body = await readTelepathosdJson(r, TELEPATHOSD_REPLY_RESPONSE_MAX_BYTES) as {
      deliveries?: unknown;
      latest?: unknown;
    };
  } catch (error) {
    if (!r.ok) return { deliveries: [], latest: after };
    throw error;
  }
  if (!r.ok) return { deliveries: [], latest: after };
  if (!Array.isArray(body.deliveries) ||
      !Number.isSafeInteger(body.latest) || (body.latest as number) < 0 ||
      (body.latest as number) < after) {
    throw new DeliveryBatchRejectedError();
  }
  const latest = body.latest as number;
  const deliveries: Delivery[] = [];
  let previousSeq = after;
  for (const value of body.deliveries) {
    if (value === null || typeof value !== "object") {
      throw new DeliveryBatchRejectedError();
    }
    const delivery = value as Record<string, unknown>;
    if (!Number.isSafeInteger(delivery.seq) || (delivery.seq as number) <= after ||
        (delivery.seq as number) > latest || (delivery.seq as number) <= previousSeq ||
        typeof delivery.chat_id !== "string" || !isValidLaneId(delivery.chat_id) ||
        delivery.chat_id !== laneId || typeof delivery.content !== "string" ||
        !delivery.content.trim()) {
      throw new DeliveryBatchRejectedError();
    }
    if (replyTo !== undefined) {
      // Correlated request/reply is exact. Missing, null, or another valid
      // opaque ID is just as unsafe as a malformed one.
      if (delivery.reply_to !== replyTo) throw new DeliveryBatchRejectedError();
    } else if (delivery.reply_to !== undefined && delivery.reply_to !== null &&
               !isValidOpaqueId(delivery.reply_to)) {
      throw new DeliveryBatchRejectedError();
    }
    previousSeq = delivery.seq as number;
    deliveries.push(delivery as unknown as Delivery);
  }
  return { deliveries, latest };
}

/** Convenience wrapper used by index.ts: returns null when hermes is not configured. */
export async function respondViaHermes(
  text: string,
  laneId?: string,
  signal?: AbortSignal,
): Promise<HermesReply | null> {
  const cfg = hermesConfig();
  if (!cfg) return null;
  try {
    return await deliverAndWaitWithReceipt(
      cfg,
      () => ({ id: laneId ?? currentLaneId() }),
      text,
      laneId,
      signal,
    );
  } catch (e) {
    if (signal?.aborted) throw e;
    if (e instanceof ReplyTextLimitError) throw e;
    const msg = (e as Error).message;
    // timeout ≠ failure: Hermes keeps working; the reply lands in the lane's
    // durable queue and gets announced at your next pinch
    if (msg.includes("no reply")) {
      return { text: "Nothing yet. I'll read it to you when it lands." };
    }
    return { text: `Hermes error: ${msg}` };
  }
}

/** Remove a synchronous reply only after the handset has accepted playback. */
export async function acknowledgeTelepathosdDelivery(receipt: DeliveryReceipt): Promise<void> {
  const cfg = assertCurrentTelepathosdTarget(receipt.targetIdentity);
  const params = new URLSearchParams({
    after: String(receipt.afterSeq),
    through_seq: String(receipt.throughSeq),
    consume: "true",
    lane_id: receipt.laneId,
    reply_to: receipt.replyTo,
  });
  const r = await fetch(`${cfg.baseUrl}/api/delivery?${params}`, {
    headers: telepathosdHeaders(cfg),
    signal: AbortSignal.timeout(2_000),
  });
  await readSuccessfulTelepathosdJson(
    r,
    TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES,
    "telepathosd delivery ack",
  );
}

/** Route meta-plane mutations through the same authoritative lane registry. */
export async function respondViaTelepathosdMeta(
  text: string,
  signal?: AbortSignal,
): Promise<string | null> {
  const cfg = hermesConfig();
  if (!cfg) return null;
  const r = await fetch(`${cfg.baseUrl}/api/meta`, {
    method: "POST",
    headers: { ...telepathosdHeaders(cfg), "Content-Type": "application/json" },
    body: JSON.stringify({ utterance: text }),
    signal: requestSignal(signal, 120_000),
  });
  const body = await readSuccessfulTelepathosdJson(
    r,
    TELEPATHOSD_REPLY_RESPONSE_MAX_BYTES,
    "telepathosd meta",
  ) as { reply?: unknown };
  if (typeof body.reply !== "string") throw new Error("telepathosd meta: invalid reply");
  if (!isReplyTextWithinLimit(body.reply)) {
    throw new ReplyTextLimitError("reply exceeds the 512 KiB UTF-8 byte limit");
  }
  assertCurrentTelepathosdTarget(cfg.targetIdentity);
  return body.reply;
}

/** telepathosd rejected a retry after its explicit bounded dedupe horizon. */
export class InteractionRetryExpiredError extends Error {}

/** Record a completed voice interaction in telepathosd's idempotent ledger. */
export async function recordTelepathosdInteraction(
  laneId: string,
  interactionId: string,
  interactionCreatedAtMs: number,
  expectedTargetIdentity?: string,
): Promise<void> {
  const cfg = hermesConfig();
  if (!cfg) return;
  if (!isValidLaneId(laneId) || !isValidOpaqueId(interactionId)) {
    throw new Error("telepathosd interaction: invalid correlation identity");
  }
  if (expectedTargetIdentity !== undefined && cfg.targetIdentity !== expectedTargetIdentity) {
    throw new TelepathosdTargetChangedError(
      "telepathosd target identity changed; interaction outbox remains pending until the original target is restored",
    );
  }
  const r = await fetch(`${cfg.baseUrl}/api/lanes/interaction`, {
    method: "POST",
    headers: { ...telepathosdHeaders(cfg), "Content-Type": "application/json" },
    body: JSON.stringify({
      id: laneId,
      interaction_id: interactionId,
      interaction_created_at_ms: interactionCreatedAtMs,
    }),
    signal: AbortSignal.timeout(2_000),
  });
  try {
    await readTelepathosdJson(r, TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES);
  } catch (error) {
    if (r.status === 410) {
      throw new InteractionRetryExpiredError("telepathosd interaction: retry expired");
    }
    if (!r.ok) throw telepathosdHttpError("telepathosd interaction", r.status);
    throw error;
  }
  if (r.status === 410) {
    throw new InteractionRetryExpiredError("telepathosd interaction: retry expired");
  }
  if (!r.ok) throw telepathosdHttpError("telepathosd interaction", r.status);
}

// set by index.ts each turn — avoids threading the registry through every call
let currentLaneIdFn: () => string = () => "telepathos:direct";
export function setCurrentLaneIdFn(fn: () => string) { currentLaneIdFn = fn; }
function currentLaneId(): string { return currentLaneIdFn(); }

function requestSignal(signal: AbortSignal | undefined, timeoutMs: number): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

function waitFor(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new Error("aborted"));
    };
    if (signal?.aborted) {
      clearTimeout(timer);
      reject(signal.reason ?? new Error("aborted"));
      return;
    }
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
