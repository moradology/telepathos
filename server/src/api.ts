import http from "node:http";
import https from "node:https";
import { createHash, timingSafeEqual } from "node:crypto";
import {
  LaneCapacityError,
  LanePersistenceError,
  LaneRegistry,
  activeLane,
  createLane,
  isValidLaneName,
  mutateAndSaveLanes,
  switchLane,
  touchLane,
} from "./lanes.js";
import {
  readTelepathosdJson,
  TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES,
  TELEPATHOSD_STATE_RESPONSE_MAX_BYTES,
} from "./hermes.js";
import { normalizeTelepathosdBaseUrl, targetIdentityFor } from "./target-scope.js";
import { isValidLaneId } from "./protocol.js";

/**
 * Agent-facing control API — the tools Hermes calls to inspect and modify
 * bridge state. Bound to localhost by default: the agent runs on the same
 * box (or reaches it over the tailnet with TELEPATHOS_API_HOST set).
 *
 *   GET  /api/state                 full registry + active lane
 *   POST /api/lanes/active {"id"}  switch active lane
 *   POST /api/lanes        {"name"} create lane (and switch to it)
 *   POST /api/lanes/touch  {"id"}   mark lane active-now (agent did work there)
 *
 * This is the surface to describe in Hermes tool definitions:
 *   list_lanes()          → GET /api/state
 *   switch_lane(id)       → POST /api/lanes/active
 *   create_lane(name)     → POST /api/lanes
 *   mark_lane_active(id)  → POST /api/lanes/touch
 */

export interface TlsMaterial {
  cert: Buffer;
  key: Buffer;
}

export const API_REQUEST_MAX_BYTES = 1_000_000;

export class ApiRequestBodyError extends Error {
  constructor(public readonly status: 400 | 413) {
    super(status === 413 ? "request body too large" : "request body is not valid UTF-8");
    this.name = "ApiRequestBodyError";
  }
}

/** Testable byte-level half of request ingestion; never accepts decoded chunks. */
export function decodeApiRequestBytes(
  chunks: Iterable<Uint8Array>,
  maxBytes = API_REQUEST_MAX_BYTES,
): string {
  let total = 0;
  const copies: Buffer[] = [];
  for (const value of chunks) {
    const chunk = Buffer.from(value);
    if (chunk.length > maxBytes - total) throw new ApiRequestBodyError(413);
    total += chunk.length;
    copies.push(chunk);
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(copies, total));
  } catch {
    throw new ApiRequestBodyError(400);
  }
}

function declaredLengthExceeds(headers: http.IncomingHttpHeaders, maxBytes: number): boolean {
  const header = headers["content-length"];
  if (typeof header !== "string" || !/^[0-9]+$/.test(header)) return false;
  const length = Number(header);
  return Number.isSafeInteger(length) && length > maxBytes;
}

/**
 * Accumulate request bytes first, then decode exactly once. Decoding each
 * stream chunk corrupts multibyte UTF-8 when a character crosses a boundary.
 */
export async function readApiRequestBody(
  req: http.IncomingMessage,
  maxBytes = API_REQUEST_MAX_BYTES,
): Promise<string> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new TypeError("request byte limit must be a non-negative safe integer");
  }
  if (declaredLengthExceeds(req.headers, maxBytes)) {
    req.resume();
    throw new ApiRequestBodyError(413);
  }

  const chunks: Buffer[] = [];
  let total = 0;
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      req.removeListener("data", onData);
      req.removeListener("end", onEnd);
      req.removeListener("error", onError);
      req.removeListener("aborted", onAborted);
    };
    const fail = (error: Error) => {
      if (settled) return;
      settled = true;
      cleanup();
      // Keep consuming without retaining bytes so the 413/400 response can
      // complete cleanly even when the client is still writing its body.
      req.resume();
      reject(error);
    };
    const onData = (value: Buffer) => {
      if (!Buffer.isBuffer(value)) {
        fail(new ApiRequestBodyError(400));
        return;
      }
      if (value.length > maxBytes - total) {
        fail(new ApiRequestBodyError(413));
        return;
      }
      total += value.length;
      chunks.push(Buffer.from(value));
    };
    const onEnd = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    };
    const onError = () => fail(new ApiRequestBodyError(400));
    const onAborted = () => fail(new ApiRequestBodyError(400));
    req.on("data", onData);
    req.once("end", onEnd);
    req.once("error", onError);
    req.once("aborted", onAborted);
  });

  return decodeApiRequestBytes(chunks, maxBytes);
}

function telepathosdProxyResponseLimit(url: string): number {
  return url === "/api/state" || url === "/api/lanes"
    ? TELEPATHOSD_STATE_RESPONSE_MAX_BYTES
    : TELEPATHOSD_SMALL_RESPONSE_MAX_BYTES;
}

/** Preserve deterministic caller errors but never relay daemon error bodies. */
function telepathosdProxyFailureStatus(status: number): number {
  return status >= 400 && status < 500 ? status : 502;
}

const INVALID_SHARED_TOKEN = "\u0000telepathos-invalid-shared-token";

function sharedTokenDigest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

/**
 * Compare a configured shared token with an untrusted value without exposing
 * the token through a variable-length comparison. Both inputs are hashed to
 * the fixed-size SHA-256 output before timingSafeEqual is called.
 *
 * The caller still decides whether a missing/empty configured token disables
 * authentication. A missing or malformed presented value can never match.
 */
export function sharedTokenMatches(configured: unknown, presented: unknown): boolean {
  const configuredToken = typeof configured === "string" && configured.length > 0
    ? configured
    : INVALID_SHARED_TOKEN;
  const presentedToken = typeof presented === "string"
    ? presented
    : INVALID_SHARED_TOKEN;
  const equal = timingSafeEqual(
    sharedTokenDigest(configuredToken),
    sharedTokenDigest(presentedToken),
  );
  return typeof configured === "string" && configured.length > 0 &&
    typeof presented === "string" && equal;
}

/** Preserve the API's x-telepathos-token header, with Bearer as its fallback. */
export function sharedTokenFromHeaders(headers: http.IncomingHttpHeaders): unknown {
  const sharedHeader = headers["x-telepathos-token"];
  if (sharedHeader !== undefined) {
    return typeof sharedHeader === "string" ? sharedHeader : undefined;
  }
  const authorization = headers.authorization;
  if (typeof authorization !== "string" || !authorization.startsWith("Bearer ")) {
    return undefined;
  }
  return authorization.slice("Bearer ".length);
}

/**
 * Keep all lane-mutation endpoints honest about the durable outcome. A
 * pre-rename failure definitely did not replace the snapshot; a post-rename
 * or already-latched failure is ambiguous and must fence the caller.
 */
export function laneMutationFailureStatus(error: unknown): 409 | 500 | 503 {
  if (error instanceof LaneCapacityError) return 409;
  if (error instanceof LanePersistenceError && error.phase !== "pre-rename") {
    return 503;
  }
  return 500;
}

interface ApiTelepathosdSnapshot {
  readonly baseUrl: string | null;
  readonly token?: string;
  readonly targetIdentity: string;
  readonly transportError: string | null;
}

const API_TARGET_CHANGED_ERROR = "telepathosd target configuration changed; restart API server";

function isLoopbackHostname(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" ||
    hostname === "::1" || hostname === "[::1]";
}

/** Validate transport using the token from this exact environment snapshot. */
function telepathosdTransportErrorForSnapshot(
  baseUrl: string,
  token: string | undefined,
): string | null {
  if (!token) return null;
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

/** Read URL, token, identity, and transport policy as one synchronous value. */
function captureApiTelepathosdSnapshot(): ApiTelepathosdSnapshot {
  const rawRemoteBase = process.env.TELEPATHOS_HERMES_URL;
  const baseUrl = rawRemoteBase && rawRemoteBase.trim()
    ? normalizeTelepathosdBaseUrl(rawRemoteBase)
    : null;
  const token = process.env.TELEPATHOS_TOKEN || undefined;
  return {
    baseUrl,
    token,
    targetIdentity: targetIdentityFor(baseUrl, token),
    transportError: baseUrl ? telepathosdTransportErrorForSnapshot(baseUrl, token) : null,
  };
}

export function startApiServer(
  reg: LaneRegistry,
  port: number,
  host: string,
  tls?: TlsMaterial,
): http.Server | https.Server {
  const startupTelepathosd = captureApiTelepathosdSnapshot();
  const remotePaths = new Set([
    "/api/state",
    "/api/lanes",
    "/api/lanes/active",
    "/api/lanes/touch",
    "/api/lanes/interaction",
  ]);
  const handler = async (req: http.IncomingMessage, res: http.ServerResponse) => {
    const json = (code: number, body: unknown) => {
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(JSON.stringify(body));
    };

    const rejectTargetDrift = () => {
      req.resume();
      return json(503, { error: API_TARGET_CHANGED_ERROR });
    };
    let telepathosd = captureApiTelepathosdSnapshot();
    if (telepathosd.targetIdentity !== startupTelepathosd.targetIdentity) {
      return rejectTargetDrift();
    }

    if (startupTelepathosd.token &&
        !sharedTokenMatches(startupTelepathosd.token, sharedTokenFromHeaders(req.headers))) {
      return json(401, { error: "unauthorized" });
    }

    let body: string;
    try {
      body = await readApiRequestBody(req);
    } catch (error) {
      if (error instanceof ApiRequestBodyError) {
        return json(error.status, { error: error.status === 413 ? "request body too large" : "request body must be valid UTF-8" });
      }
      return json(400, { error: "invalid request body" });
    }

    try {
      // Body intake yields to the event loop. Re-read the complete config
      // immediately before any upstream fetch or local lane mutation so a
      // runtime env rotation cannot cross that side-effect boundary.
      telepathosd = captureApiTelepathosdSnapshot();
      if (telepathosd.targetIdentity !== startupTelepathosd.targetIdentity) {
        return rejectTargetDrift();
      }
      const url = req.url ?? "/";
      if (telepathosd.baseUrl !== null && remotePaths.has(url)) {
        if (telepathosd.transportError) {
          return json(502, { error: telepathosd.transportError });
        }
        try {
          const headers: Record<string, string> = { "Content-Type": "application/json" };
          if (telepathosd.token) headers["x-telepathos-token"] = telepathosd.token;
          const upstream = await fetch(`${telepathosd.baseUrl}${url}`, {
            method: req.method,
            headers,
            ...(req.method === "GET" || req.method === "HEAD" ? {} : { body }),
            signal: AbortSignal.timeout(2_000),
          });
          let upstreamBody: unknown;
          try {
            upstreamBody = await readTelepathosdJson(upstream, telepathosdProxyResponseLimit(url));
          } catch (error) {
            // Daemon 4xx bodies can be legacy plain text. Preserve the
            // actionable status but never pass that body through the proxy.
            if (!upstream.ok) {
              return json(telepathosdProxyFailureStatus(upstream.status), {
                error: `telepathosd rejected request (${upstream.status})`,
              });
            }
            throw error;
          }
          if (!upstream.ok) {
            return json(telepathosdProxyFailureStatus(upstream.status), {
              error: `telepathosd rejected request (${upstream.status})`,
            });
          }
          return json(upstream.status, upstreamBody);
        } catch {
          return json(502, { error: "telepathosd unavailable" });
        }
      }
        if (req.method === "GET" && url === "/api/state") {
          return json(200, { ...structuredClone(reg), active: activeLane(reg).name });
        }
        if (req.method === "POST" && url === "/api/lanes/active") {
          const { id } = JSON.parse(body);
          if (!isValidLaneId(id)) return json(400, { error: "id must match the lane ID grammar" });
          if (!reg.lanes.some((lane) => lane.id === id)) {
            return json(404, { error: `unknown lane ${id}` });
          }
          let lane;
          try {
            lane = mutateAndSaveLanes(reg, () => switchLane(reg, id));
          } catch (error) {
            return json(laneMutationFailureStatus(error), { error: String((error as Error).message ?? error) });
          }
          return json(200, { ok: true, lane });
        }
        if (req.method === "POST" && url === "/api/lanes") {
          const { name } = JSON.parse(body);
          if (!isValidLaneName(name)) return json(400, { error: "name must produce a valid lane ID" });
          let lane;
          try {
            lane = mutateAndSaveLanes(reg, () => {
              const created = createLane(reg, name);
              switchLane(reg, created.id);
              return created;
            });
          } catch (error) {
            return json(laneMutationFailureStatus(error), { error: String((error as Error).message ?? error) });
          }
          return json(200, { ok: true, lane });
        }
        if (req.method === "POST" && url === "/api/lanes/touch") {
          const { id } = JSON.parse(body);
          if (!isValidLaneId(id)) return json(400, { error: "id must match the lane ID grammar" });
          if (!reg.lanes.some((lane) => lane.id === id)) {
            return json(404, { error: `unknown lane ${id}` });
          }
          try {
            mutateAndSaveLanes(reg, () => touchLane(reg, id));
          } catch (error) {
            return json(laneMutationFailureStatus(error), { error: String((error as Error).message ?? error) });
          }
          return json(200, { ok: true });
        }
        return json(404, { error: "not found" });
    } catch {
      return json(400, { error: "invalid request" });
    }
  };
  const server = tls ? https.createServer(tls, handler) : http.createServer(handler);

  server.listen(port, host, () => {
    console.log(`lane API on ${tls ? "https" : "http"}://${host}:${port} (agent tools: list/switch/create/touch)`);
  });
  server.on("error", (e: NodeJS.ErrnoException) => {
    console.error(`lane API: ${e.message}`);
  });
  return server;
}
