// Hermes connector contract tests. The fake telepathosd endpoint deliberately
// queues a reply during POST /api/message; the client must have captured the
// cursor before that POST or it will skip the fast reply.
import http from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  deliverAndWait,
  deliverAndWaitWithReceipt,
  fetchTelepathosdState,
  readTelepathosdJson,
  respondViaTelepathosdMeta,
  TELEPATHOSD_STATE_RESPONSE_MAX_BYTES,
  TelepathosdResponseError,
  telepathosdTransportError,
} from "./dist/hermes.js";
import { API_REQUEST_MAX_BYTES, decodeApiRequestBytes, startApiServer } from "./dist/api.js";
import { normalizeTelepathosdBaseUrl, targetIdentityFor } from "./dist/target-scope.js";

let failures = 0;
const check = (name, ok, detail = "") => {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
  if (!ok) failures++;
};

const requests = [];
const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  requests.push({ method: req.method, path: url.pathname, query: Object.fromEntries(url.searchParams) });
  if (req.method === "GET" && url.pathname === "/api/delivery/head") {
    res.setHeader("Content-Type", "application/json");
    // Cursor captured before POST sees the pre-existing queue at seq 5.
    res.end(JSON.stringify({ latest: 5 }));
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/delivery") {
    const after = Number(url.searchParams.get("after"));
    res.setHeader("Content-Type", "application/json");
    if (after === 5) res.end(JSON.stringify({
      deliveries: [{ seq: 6, chat_id: "telepathos:direct", content: "fast reply", reply_to: "tp-1" }],
      latest: 6,
    }));
    else res.end(JSON.stringify({ deliveries: [], latest: 5 }));
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/state") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({
      lanes: [{ id: "telepathos:direct", name: "direct", created_at: "2020-01-01", last_active: "2020-01-01" }],
      active_id: "telepathos:direct",
      previous_id: "telepathos:direct",
      revision: 0,
    }));
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/message") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, message_id: "tp-1" }));
    return;
  }
  res.writeHead(404).end();
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const address = server.address();
const cfg = {
  baseUrl: `http://127.0.0.1:${address.port}`,
  timeoutMs: 5_000,
  targetIdentity: targetIdentityFor(`http://127.0.0.1:${address.port}`),
};
const reply = await deliverAndWait(cfg, () => ({ id: "telepathos:direct" }), "hello");

check("fast reply is not skipped", reply === "fast reply", reply);
check("cursor head is captured before POST", requests[1]?.path === "/api/delivery/head" && requests[2]?.path === "/api/message");
check("poll is lane-scoped, correlated, and defers consumption",
  requests[3]?.query.lane_id === "telepathos:direct" &&
  requests[3]?.query.reply_to === "tp-1" &&
  requests[3]?.query.consume === "false");
check("normalized URL identity is stable", cfg.targetIdentity === targetIdentityFor(`${cfg.baseUrl}/`));
check("credential rotation changes target identity", cfg.targetIdentity !== targetIdentityFor(cfg.baseUrl, "rotated"));

for (const [label, url, secrets] of [
  ["username", "https://alice:unused@example.test", ["alice", "unused"]],
  ["password", "https://unused:super-secret@example.test", ["unused", "super-secret"]],
  ["username and password", "https://alice:super-secret@example.test", ["alice", "super-secret"]],
]) {
  let error = null;
  try {
    normalizeTelepathosdBaseUrl(url);
  } catch (e) {
    error = String(e?.message ?? e);
  }
  check(`userinfo ${label} is rejected without surfacing credentials`,
    error !== null && !secrets.some((secret) => error.includes(secret)), error ?? "accepted");
}

const previousToken = process.env.TELEPATHOS_TOKEN;
process.env.TELEPATHOS_TOKEN = "secret";
check("token blocks remote cleartext telepathosd", Boolean(
  telepathosdTransportError("http://192.168.1.10:8790"),
));
check("token permits loopback cleartext telepathosd", !telepathosdTransportError(
  "http://127.0.0.1:8790",
));
if (previousToken === undefined) delete process.env.TELEPATHOS_TOKEN;
else process.env.TELEPATHOS_TOKEN = previousToken;

// Request bodies are bytes, not independently decodable string chunks. Each
// boundary of 2/3/4-byte code points must survive exactly as sent.
for (const value of ["\u00e9", "\u20ac", "\ud83d\ude00"]) {
  const bytes = Buffer.from(value, "utf8");
  for (let boundary = 1; boundary < bytes.length; boundary++) {
    check(`request UTF-8 ${bytes.length}-byte code point survives split ${boundary}`,
      decodeApiRequestBytes([bytes.subarray(0, boundary), bytes.subarray(boundary)]) === value);
  }
}
let malformedRequestRejected = false;
try { decodeApiRequestBytes([Buffer.from([0xc3, 0x28])]); } catch (error) {
  malformedRequestRejected = error?.status === 400;
}
check("request malformed UTF-8 is rejected", malformedRequestRejected);
const requestBoundary = Buffer.from("x".repeat(128));
check("request exact byte limit is accepted",
  decodeApiRequestBytes([requestBoundary], requestBoundary.length) === requestBoundary.toString("utf8"));
let oversizedRequestRejected = false;
try { decodeApiRequestBytes([requestBoundary, Buffer.from("x")], requestBoundary.length); } catch (error) {
  oversizedRequestRejected = error?.status === 413;
}
check("request over byte limit is rejected", oversizedRequestRejected);

const listen = (httpServer) => new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
const close = (httpServer) => new Promise((resolve) => httpServer.close(resolve));

// The pre-POST cursor is a durable positioning decision. Invalid head values
// must fail closed before /api/message, polling, or receipt creation.
const runInvalidHeadCase = async (label, headBody) => {
  const headRequests = [];
  let messagePosts = 0;
  const headServer = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    if (req.method === "GET" && url.pathname === "/api/state") {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({
        lanes: [{ id: "telepathos:direct", name: "direct", created_at: "", last_active: "" }],
        active_id: "telepathos:direct",
        previous_id: "telepathos:direct",
        revision: 0,
      }));
      return;
    }
    if (req.method === "GET" && url.pathname === "/api/delivery/head") {
      headRequests.push({ path: url.pathname, query: url.search });
      res.setHeader("Content-Type", "application/json");
      res.end(headBody);
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/message") {
      messagePosts++;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, message_id: "tp-invalid-cursor" }));
      return;
    }
    res.writeHead(404).end();
  });
  await listen(headServer);
  const headUrl = `http://127.0.0.1:${headServer.address().port}`;
  const headCfg = {
    baseUrl: headUrl,
    timeoutMs: 2_000,
    targetIdentity: targetIdentityFor(headUrl),
  };
  let result = null;
  let error = null;
  try {
    result = await deliverAndWaitWithReceipt(
      headCfg,
      () => ({ id: "telepathos:direct" }),
      "hello",
      "telepathos:direct",
    );
  } catch (caught) {
    error = caught;
  }
  check(`${label} returns the stable sanitized head failure`,
    error?.message === "telepathosd delivery: response rejected",
    error?.message ?? "no error");
  check(`${label} does not POST or create a receipt`,
    messagePosts === 0 && result === null,
    `posts=${messagePosts}`);
  check(`${label} does not poll or consume after head capture`,
    headRequests.length === 1 && headRequests[0].path === "/api/delivery/head" &&
    headRequests[0].query === "",
    JSON.stringify(headRequests));
  await close(headServer);
};

for (const [label, headBody] of [
  ["head string", JSON.stringify({ latest: "5" })],
  ["head float", JSON.stringify({ latest: 1.5 })],
  ["head NaN-like string", JSON.stringify({ latest: "NaN" })],
  ["head Infinity-like string", JSON.stringify({ latest: "Infinity" })],
  ["head missing", JSON.stringify({})],
  ["head null", JSON.stringify({ latest: null })],
  ["head negative", JSON.stringify({ latest: -1 })],
  ["head unsafe integer", JSON.stringify({ latest: Number.MAX_SAFE_INTEGER + 1 })],
  ["head overflow", '{"latest":1e400}'],
]) {
  await runInvalidHeadCase(label, headBody);
}

// Valid zero and the maximum safe cursor both remain admissible. Abort after
// POST so these boundary cases do not wait for a reply that cannot advance
// the cursor further at MAX_SAFE_SEQUENCE.
const runValidHeadBoundaryCase = async (label, latest) => {
  const headRequests = [];
  let messagePosts = 0;
  let postedResolve;
  const posted = new Promise((resolve) => { postedResolve = resolve; });
  const boundaryServer = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    if (req.method === "GET" && url.pathname === "/api/state") {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({
        lanes: [{ id: "telepathos:direct", name: "direct", created_at: "", last_active: "" }],
        active_id: "telepathos:direct",
        previous_id: "telepathos:direct",
        revision: 0,
      }));
      return;
    }
    if (req.method === "GET" && url.pathname === "/api/delivery/head") {
      headRequests.push({ path: url.pathname, query: url.search });
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ latest }));
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/message") {
      messagePosts++;
      postedResolve();
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, message_id: `tp-${label}` }));
      return;
    }
    res.writeHead(404).end();
  });
  await listen(boundaryServer);
  const boundaryUrl = `http://127.0.0.1:${boundaryServer.address().port}`;
  const controller = new AbortController();
  const boundaryCfg = {
    baseUrl: boundaryUrl,
    timeoutMs: 10_000,
    targetIdentity: targetIdentityFor(boundaryUrl),
  };
  const operation = deliverAndWaitWithReceipt(
    boundaryCfg,
    () => ({ id: "telepathos:direct" }),
    "hello",
    "telepathos:direct",
    controller.signal,
  ).then(
    (value) => ({ kind: "result", value }),
    (error) => ({ kind: "error", error }),
  );
  const firstOutcome = await Promise.race([
    posted.then(() => ({ kind: "posted" })),
    operation,
  ]);
  if (firstOutcome.kind === "posted") controller.abort();
  const outcome = firstOutcome.kind === "posted" ? await operation : firstOutcome;
  check(`${label} boundary is accepted before POST`,
    firstOutcome.kind === "posted" && messagePosts === 1,
    `outcome=${outcome.kind}, posts=${messagePosts}`);
  check(`${label} boundary captures the exact head`,
    headRequests.length === 1 && headRequests[0].path === "/api/delivery/head" &&
    headRequests[0].query === "",
    JSON.stringify(headRequests));
  await close(boundaryServer);
};

await runValidHeadBoundaryCase("zero", 0);
await runValidHeadBoundaryCase("max-safe", Number.MAX_SAFE_INTEGER);

// A valid, unrelated relay backlog can exceed the reply-response cap. The
// cursor bootstrap must never deserialize it just to find the head sequence:
// it still has to POST and accept the subsequent exact correlated reply.
const oversizedUnrelatedContent = "u".repeat(300 * 1024);
const oversizedUnrelatedBatch = JSON.stringify({
  deliveries: [
    { seq: 1, chat_id: "telepathos:other", content: oversizedUnrelatedContent },
    { seq: 2, chat_id: "telepathos:other", content: oversizedUnrelatedContent },
  ],
  latest: 2,
});
check("oversized unrelated backlog fixture exceeds the legacy reply cap",
  Buffer.byteLength(oversizedUnrelatedBatch) > 576 * 1024,
  String(Buffer.byteLength(oversizedUnrelatedBatch)));
const oversizedBacklogRequests = [];
let oversizedBacklogPosts = 0;
const oversizedBacklogServer = http.createServer((req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  oversizedBacklogRequests.push({ path: url.pathname, after: url.searchParams.get("after") });
  if (req.method === "GET" && url.pathname === "/api/state") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({
      lanes: [{ id: "telepathos:direct", name: "direct", created_at: "", last_active: "" }],
      active_id: "telepathos:direct",
      previous_id: "telepathos:direct",
      revision: 0,
    }));
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/delivery/head") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ latest: 2 }));
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/delivery") {
    res.setHeader("Content-Type", "application/json");
    if (url.searchParams.get("after") === "0") {
      res.end(oversizedUnrelatedBatch);
    } else {
      res.end(JSON.stringify({
        deliveries: [{
          seq: 3,
          chat_id: "telepathos:direct",
          content: "reply after oversized backlog",
          reply_to: "tp-backlog",
        }],
        latest: 3,
      }));
    }
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/message") {
    oversizedBacklogPosts++;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, message_id: "tp-backlog" }));
    return;
  }
  res.writeHead(404).end();
});
await listen(oversizedBacklogServer);
const oversizedBacklogUrl = `http://127.0.0.1:${oversizedBacklogServer.address().port}`;
const oversizedBacklogReply = await deliverAndWaitWithReceipt(
  {
    baseUrl: oversizedBacklogUrl,
    timeoutMs: 2_500,
    targetIdentity: targetIdentityFor(oversizedBacklogUrl),
  },
  () => ({ id: "telepathos:direct" }),
  "hello",
  "telepathos:direct",
);
check("oversized unrelated backlog still permits a new reply",
  oversizedBacklogReply.text === "reply after oversized backlog" && oversizedBacklogPosts === 1,
  oversizedBacklogReply.text);
check("oversized unrelated backlog uses the bounded head instead of after=0",
  oversizedBacklogRequests.some((request) => request.path === "/api/delivery/head") &&
    !oversizedBacklogRequests.some((request) =>
      request.path === "/api/delivery" && request.after === "0"),
  JSON.stringify(oversizedBacklogRequests));
await close(oversizedBacklogServer);

const apiRequest = (port, path, body, headers = {}) => new Promise((resolve, reject) => {
  const request = http.request({
    host: "127.0.0.1",
    port,
    path,
    method: "POST",
    headers: { "Content-Length": String(body.length), ...headers },
  }, (response) => {
    const chunks = [];
    response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    response.on("end", () => resolve({ status: response.statusCode, text: Buffer.concat(chunks).toString("utf8") }));
  });
  request.on("error", reject);
  for (const chunk of body) request.write(chunk);
  request.end();
});

const originalHermesUrl = process.env.TELEPATHOS_HERMES_URL;
delete process.env.TELEPATHOS_HERMES_URL;
const apiRegistry = {
  lanes: [{ id: "telepathos:direct", name: "direct", createdAt: "", lastActive: "" }],
  activeId: "telepathos:direct",
  previousId: "telepathos:direct",
};
const apiServer = startApiServer(apiRegistry, 0, "127.0.0.1");
await new Promise((resolve) => apiServer.once("listening", resolve));
const apiPort = apiServer.address().port;
const apiBeforeMalformed = JSON.stringify(apiRegistry);
const malformedApiResponse = await apiRequest(
  apiPort,
  "/api/lanes",
  [Buffer.from('{"name":"'), Buffer.from([0xc3, 0x28]), Buffer.from('"}')],
);
check("malformed API request returns 400 without mutation",
  malformedApiResponse.status === 400 && JSON.stringify(apiRegistry) === apiBeforeMalformed);
await close(apiServer);
if (originalHermesUrl === undefined) delete process.env.TELEPATHOS_HERMES_URL;
else process.env.TELEPATHOS_HERMES_URL = originalHermesUrl;

const exactRemoteLimit = 256;
const exactRemoteBody = JSON.stringify({ value: "x".repeat(exactRemoteLimit - 12) });
check("remote exact test fixture is exact byte limit", Buffer.byteLength(exactRemoteBody) === exactRemoteLimit);
const transportServer = http.createServer((req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (url.pathname === "/exact") {
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Length", String(Buffer.byteLength(exactRemoteBody)));
    res.end(exactRemoteBody);
    return;
  }
  if (url.pathname === "/split-utf8") {
    const body = Buffer.from('{"value":"\ud83d\ude00"}', "utf8");
    const emojiStart = body.indexOf(Buffer.from("\ud83d\ude00", "utf8"));
    res.setHeader("Content-Type", "application/json");
    res.write(body.subarray(0, emojiStart + 1));
    res.write(body.subarray(emojiStart + 1, emojiStart + 3));
    res.end(body.subarray(emojiStart + 3));
    return;
  }
  if (url.pathname === "/huge-chunked" || url.pathname === "/huge-error") {
    res.writeHead(url.pathname === "/huge-error" ? 500 : 200, { "Content-Type": "application/json" });
    res.write('{"error":"');
    res.write("x".repeat(400));
    res.end('"}');
    return;
  }
  if (url.pathname === "/lying-length") {
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Length", "1");
    res.end('{"value":"longer than declared"}');
    return;
  }
  if (url.pathname === "/malformed-json") {
    res.setHeader("Content-Type", "application/json");
    res.end('{"value":');
    return;
  }
  if (url.pathname === "/malformed-utf8") {
    res.setHeader("Content-Type", "application/json");
    res.end(Buffer.from([0x7b, 0xc3, 0x28, 0x7d]));
    return;
  }
  if (url.pathname === "/api/state") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({
      lanes: [{ id: "telepathos:direct", name: "direct", created_at: "", last_active: "" }],
      active_id: "telepathos:direct", previous_id: "telepathos:direct", revision: 0,
    }));
    return;
  }
  if (url.pathname === "/api/delivery/head") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ latest: 0 }));
    return;
  }
  if (url.pathname === "/api/delivery") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ deliveries: [], latest: 0 }));
    return;
  }
  if (url.pathname === "/api/message") {
    res.writeHead(413, { "Content-Type": "text/plain" });
    res.end("daemon-secret-message-error");
    return;
  }
  if (url.pathname === "/api/lanes") {
    res.writeHead(413, { "Content-Type": "text/plain" });
    res.end("daemon-secret-lanes-error");
    return;
  }
  if (url.pathname === "/api/meta") {
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end('{"error":"daemon-secret-meta-error"}');
    return;
  }
  res.writeHead(404, { "Content-Type": "application/json" }).end('{"error":"daemon-secret-not-found"}');
});
await listen(transportServer);
const transportUrl = `http://127.0.0.1:${transportServer.address().port}`;

const readRemote = async (path, limit = exactRemoteLimit) => {
  try {
    return { value: await readTelepathosdJson(await fetch(`${transportUrl}${path}`), limit) };
  } catch (error) {
    return { failure: error instanceof TelepathosdResponseError ? error.failure : String(error) };
  }
};
check("remote exact Content-Length body is accepted", (await readRemote("/exact")).value?.value?.length === exactRemoteLimit - 12);
check("remote split UTF-8 is decoded once", (await readRemote("/split-utf8")).value?.value === "\ud83d\ude00");
check("remote huge chunked success is bounded", (await readRemote("/huge-chunked", 128)).failure === "too-large");
check("remote huge chunked error is bounded", (await readRemote("/huge-error", 128)).failure === "too-large");
check("remote lying Content-Length is rejected safely", Boolean((await readRemote("/lying-length")).failure));
check("remote malformed JSON is rejected", (await readRemote("/malformed-json")).failure === "invalid-json");
check("remote malformed UTF-8 is rejected", (await readRemote("/malformed-utf8")).failure === "invalid-utf8");

const transportCfg = {
  baseUrl: transportUrl,
  timeoutMs: 500,
  targetIdentity: targetIdentityFor(transportUrl),
};
check("telepathosd state success still works", (await fetchTelepathosdState(transportCfg))?.active_id === "telepathos:direct");
let postError = "";
try { await deliverAndWait(transportCfg, () => ({ id: "telepathos:direct" }), "hello"); } catch (error) {
  postError = String(error?.message ?? error);
}
check("Hermes post preserves permanent 413 without daemon body", postError.includes("413") && !postError.includes("daemon-secret"));
const savedTransportEnv = process.env.TELEPATHOS_HERMES_URL;
process.env.TELEPATHOS_HERMES_URL = transportUrl;
let metaError = "";
try { await respondViaTelepathosdMeta("hello"); } catch (error) { metaError = String(error?.message ?? error); }
check("Hermes meta sanitizes daemon error body", metaError.includes("unavailable") && !metaError.includes("daemon-secret"));

// A meta response can be fully valid after the target changes while the
// captured request is still in flight. The response must not cross that fence.
const savedMetaUrl = process.env.TELEPATHOS_HERMES_URL;
const savedMetaToken = process.env.TELEPATHOS_TOKEN;
let targetAReadyResolve;
let releaseTargetAResolve;
const targetAReady = new Promise((resolve) => { targetAReadyResolve = resolve; });
const targetAResponseReleased = new Promise((resolve) => { releaseTargetAResolve = resolve; });
let targetARequests = 0;
let targetBRequests = 0;
const targetA = http.createServer((req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (req.method !== "POST" || url.pathname !== "/api/meta") {
    res.writeHead(404).end();
    return;
  }
  req.resume();
  targetARequests++;
  if (targetARequests === 1) {
    targetAReadyResolve();
    targetAResponseReleased.then(() => {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ reply: "stale target A reply" }));
    });
    return;
  }
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ reply: "same target A reply" }));
});
const targetB = http.createServer((req, res) => {
  targetBRequests++;
  req.resume();
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ reply: "target B reply" }));
});
await listen(targetA);
await listen(targetB);
const targetAUrl = `http://127.0.0.1:${targetA.address().port}`;
const targetBUrl = `http://127.0.0.1:${targetB.address().port}`;
try {
  process.env.TELEPATHOS_HERMES_URL = targetAUrl;
  process.env.TELEPATHOS_TOKEN = "target-token-a";
  const staleOperation = respondViaTelepathosdMeta("cut over safely");
  await targetAReady;

  process.env.TELEPATHOS_HERMES_URL = targetBUrl;
  process.env.TELEPATHOS_TOKEN = "target-token-b";
  releaseTargetAResolve();
  let staleError = null;
  try { await staleOperation; } catch (error) { staleError = error; }
  check("meta response is fenced after URL and token rotation",
    staleError?.message === "telepathosd target identity changed; durable remote state remains pending until the original target is restored" &&
    targetARequests === 1 && targetBRequests === 0,
    staleError?.message ?? "stale reply returned");

  process.env.TELEPATHOS_HERMES_URL = targetAUrl;
  process.env.TELEPATHOS_TOKEN = "target-token-a";
  const sameTargetReply = await respondViaTelepathosdMeta("same target");
  check("meta response succeeds when target identity is unchanged",
    sameTargetReply === "same target A reply" && targetARequests === 2,
    sameTargetReply ?? "no reply");
} finally {
  releaseTargetAResolve();
  await close(targetA);
  await close(targetB);
  if (savedMetaUrl === undefined) delete process.env.TELEPATHOS_HERMES_URL;
  else process.env.TELEPATHOS_HERMES_URL = savedMetaUrl;
  if (savedMetaToken === undefined) delete process.env.TELEPATHOS_TOKEN;
  else process.env.TELEPATHOS_TOKEN = savedMetaToken;
}

const proxyRegistry = {
  lanes: [{ id: "telepathos:direct", name: "direct", createdAt: "", lastActive: "" }],
  activeId: "telepathos:direct",
  previousId: "telepathos:direct",
};
const proxyServer = startApiServer(proxyRegistry, 0, "127.0.0.1");
await new Promise((resolve) => proxyServer.once("listening", resolve));
const proxyBase = `http://127.0.0.1:${proxyServer.address().port}`;
const proxyMessage = await fetch(`${proxyBase}/api/lanes`, { method: "POST", body: "{}" });
const proxyMessageText = await proxyMessage.text();
check("local proxy preserves 413 but strips daemon error body",
  proxyMessage.status === 413 && !proxyMessageText.includes("daemon-secret"));
await close(proxyServer);
if (savedTransportEnv === undefined) delete process.env.TELEPATHOS_HERMES_URL;
else process.env.TELEPATHOS_HERMES_URL = savedTransportEnv;
await close(transportServer);

// The API proxy must use one URL/token snapshot for auth, transport policy,
// and upstream fetches. A runtime rotation must fence the request before it
// can reach either the old daemon or local lane mutation code.
const savedProxyUrl = process.env.TELEPATHOS_HERMES_URL;
const savedProxyToken = process.env.TELEPATHOS_TOKEN;
const savedProxyLanes = process.env.TELEPATHOS_LANES;
const proxyTestDirectory = mkdtempSync(join(tmpdir(), "telepathos-api-proxy-"));
process.env.TELEPATHOS_LANES = join(proxyTestDirectory, "lanes.json");
const proxyTargets = [];
const proxyApiServers = [];
const startProxyTarget = async (label) => {
  const requests = [];
  const target = http.createServer((req, res) => {
    requests.push({ method: req.method, path: req.url, token: req.headers["x-telepathos-token"] });
    req.resume();
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ ok: true, target: label }));
  });
  await listen(target);
  const url = `http://127.0.0.1:${target.address().port}`;
  proxyTargets.push(target);
  return { requests, server: target, url };
};
const startProxyApi = async (registry) => {
  const api = startApiServer(registry, 0, "127.0.0.1");
  await new Promise((resolve) => api.once("listening", resolve));
  proxyApiServers.push(api);
  return `http://127.0.0.1:${api.address().port}`;
};
const proxyRequest = async (base, path, method, token, body) => {
  const headers = {};
  if (token !== undefined) headers["x-telepathos-token"] = token;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(`${base}${path}`, {
    method,
    headers,
    ...(body === undefined ? {} : { body }),
  });
  return { status: response.status, text: await response.text() };
};
const freshProxyRegistry = () => ({
  lanes: [{ id: "telepathos:direct", name: "direct", createdAt: "", lastActive: "" }],
  activeId: "telepathos:direct",
  previousId: "telepathos:direct",
});

try {
  const targetA = await startProxyTarget("A");
  const targetB = await startProxyTarget("B");

  process.env.TELEPATHOS_HERMES_URL = targetA.url;
  process.env.TELEPATHOS_TOKEN = "proxy-token-a";
  const urlProxyBase = await startProxyApi(freshProxyRegistry());
  const initialProxyResponse = await proxyRequest(urlProxyBase, "/api/lanes", "POST", "proxy-token-a", "{}");
  check("API proxy uses the startup target/token snapshot",
    initialProxyResponse.status === 200 &&
    targetA.requests.length === 1 &&
    targetA.requests[0].token === "proxy-token-a" &&
    targetB.requests.length === 0);

  const beforeUrlRotationA = targetA.requests.length;
  process.env.TELEPATHOS_HERMES_URL = targetB.url;
  const urlRotationResponse = await proxyRequest(urlProxyBase, "/api/lanes", "POST", "proxy-token-a", "{}");
  check("A to B URL rotation fences before either daemon receives the request",
    urlRotationResponse.status === 503 &&
    targetA.requests.length === beforeUrlRotationA &&
    targetB.requests.length === 0);

  process.env.TELEPATHOS_HERMES_URL = targetA.url;
  process.env.TELEPATHOS_TOKEN = "proxy-token-a";
  const beforeTokenRotationA = targetA.requests.length;
  process.env.TELEPATHOS_TOKEN = "proxy-token-b";
  const tokenRotationResponse = await proxyRequest(urlProxyBase, "/api/lanes", "POST", "proxy-token-b", "{}");
  check("token rotation fences before the old daemon receives a newly-authenticated request",
    tokenRotationResponse.status === 503 &&
    targetA.requests.length === beforeTokenRotationA &&
    targetB.requests.length === 0);

  process.env.TELEPATHOS_HERMES_URL = targetA.url;
  process.env.TELEPATHOS_TOKEN = "proxy-token-a";
  const remoteToLocalRegistry = freshProxyRegistry();
  const remoteToLocalBase = await startProxyApi(remoteToLocalRegistry);
  const beforeRemoteToLocalA = targetA.requests.length;
  delete process.env.TELEPATHOS_HERMES_URL;
  delete process.env.TELEPATHOS_TOKEN;
  const remoteToLocalResponse = await proxyRequest(
    remoteToLocalBase,
    "/api/lanes",
    "POST",
    undefined,
    JSON.stringify({ name: "must-not-be-created" }),
  );
  check("remote to local mode rotation fences before local mutation",
    remoteToLocalResponse.status === 503 &&
    JSON.stringify(remoteToLocalRegistry) === JSON.stringify(freshProxyRegistry()) &&
    targetA.requests.length === beforeRemoteToLocalA);

  const localToRemoteRegistry = freshProxyRegistry();
  const localToRemoteBase = await startProxyApi(localToRemoteRegistry);
  const beforeLocalToRemoteA = targetA.requests.length;
  process.env.TELEPATHOS_HERMES_URL = targetA.url;
  process.env.TELEPATHOS_TOKEN = "proxy-token-a";
  const localToRemoteResponse = await proxyRequest(
    localToRemoteBase,
    "/api/lanes",
    "POST",
    "proxy-token-a",
    JSON.stringify({ name: "must-not-be-created" }),
  );
  check("local to remote mode rotation fences before upstream fetch",
    localToRemoteResponse.status === 503 &&
    JSON.stringify(localToRemoteRegistry) === JSON.stringify(freshProxyRegistry()) &&
    targetA.requests.length === beforeLocalToRemoteA);

  process.env.TELEPATHOS_HERMES_URL = "http://192.0.2.1:9";
  process.env.TELEPATHOS_TOKEN = "proxy-token-a";
  const cleartextProxyBase = await startProxyApi(freshProxyRegistry());
  const cleartextResponse = await proxyRequest(cleartextProxyBase, "/api/state", "GET", "proxy-token-a");
  check("token-bearing cleartext non-loopback target is rejected before fetch",
    cleartextResponse.status === 502 &&
    cleartextResponse.text.includes("must use https://") &&
    targetA.requests.length === beforeLocalToRemoteA);
} finally {
  for (const api of proxyApiServers) await close(api);
  for (const target of proxyTargets) await close(target);
  if (savedProxyUrl === undefined) delete process.env.TELEPATHOS_HERMES_URL;
  else process.env.TELEPATHOS_HERMES_URL = savedProxyUrl;
  if (savedProxyToken === undefined) delete process.env.TELEPATHOS_TOKEN;
  else process.env.TELEPATHOS_TOKEN = savedProxyToken;
  if (savedProxyLanes === undefined) delete process.env.TELEPATHOS_LANES;
  else process.env.TELEPATHOS_LANES = savedProxyLanes;
  rmSync(proxyTestDirectory, { recursive: true, force: true });
}

// A correlated poll is an all-or-nothing admission boundary. A peer that
// returns a valid delivery for another turn, a generic delivery, another lane,
// or an impossible sequence interval must not make Hermes speak it or move its
// cursor past the durable entry. The next poll must retry the same window.
const runCorrelationCase = async (label, firstBatch, expectedPolls, expectedText = "exact reply") => {
  const correlationRequests = [];
  let deliveryPoll = 0;
  const exactBatch = {
    deliveries: [{
      seq: 6,
      chat_id: "telepathos:direct",
      content: expectedText,
      reply_to: "tp-1",
    }],
    latest: 6,
  };
  const batches = firstBatch === null ? [exactBatch] : [firstBatch, exactBatch];
  const correlationServer = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    if (req.method === "GET" && url.pathname === "/api/state") {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({
        lanes: [{ id: "telepathos:direct", name: "direct", created_at: "", last_active: "" }],
        active_id: "telepathos:direct",
        previous_id: "telepathos:direct",
        revision: 0,
      }));
      return;
    }
    if (req.method === "GET" && url.pathname === "/api/delivery/head") {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ latest: 5 }));
      return;
    }
    if (req.method === "GET" && url.pathname === "/api/delivery") {
      const after = Number(url.searchParams.get("after"));
      const query = Object.fromEntries(url.searchParams);
      correlationRequests.push({ after, query });
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(batches[Math.min(deliveryPoll++, batches.length - 1)]));
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/message") {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, message_id: "tp-1" }));
      return;
    }
    res.writeHead(404).end();
  });
  await listen(correlationServer);
  const correlationUrl = `http://127.0.0.1:${correlationServer.address().port}`;
  const correlationCfg = {
    baseUrl: correlationUrl,
    timeoutMs: 2_500,
    targetIdentity: targetIdentityFor(correlationUrl),
  };
  let result = null;
  let error = null;
  try {
    result = await deliverAndWaitWithReceipt(
      correlationCfg,
      () => ({ id: "telepathos:direct" }),
      "hello",
      "telepathos:direct",
    );
  } catch (caught) {
    error = caught;
  }
  const deliveryRequests = correlationRequests;
  check(`${label} returns only the exact correlated reply`,
    error === null && result?.text === expectedText, error?.message ?? result?.text ?? "no result");
  check(`${label} receipt is created only for the exact interval`,
    error === null && result?.receipt?.replyTo === "tp-1" &&
    result.receipt.afterSeq === 5 && result.receipt.throughSeq === 6,
    error?.message ?? "invalid receipt");
  check(`${label} retries without advancing the cursor`,
    deliveryRequests.length === expectedPolls &&
    deliveryRequests.every((request) => request.after === 5),
    JSON.stringify(deliveryRequests.map((request) => request.after)));
  check(`${label} never asks the daemon to consume a rejected batch`,
    deliveryRequests.every((request) => request.query.consume === "false"),
    JSON.stringify(deliveryRequests.map((request) => request.query.consume)));
  await close(correlationServer);
};

await runCorrelationCase(
  "same-lane wrong reply ID",
  { deliveries: [{ seq: 6, chat_id: "telepathos:direct", content: "wrong turn", reply_to: "tp-other" }], latest: 6 },
  2,
);
await runCorrelationCase(
  "mixed exact and wrong reply IDs",
  { deliveries: [
    { seq: 6, chat_id: "telepathos:direct", content: "exact but mixed", reply_to: "tp-1" },
    { seq: 7, chat_id: "telepathos:direct", content: "wrong turn", reply_to: "tp-other" },
  ], latest: 7 },
  2,
);
await runCorrelationCase(
  "missing reply ID",
  { deliveries: [{ seq: 6, chat_id: "telepathos:direct", content: "generic reply" }], latest: 6 },
  2,
);
await runCorrelationCase(
  "wrong lane",
  { deliveries: [{ seq: 6, chat_id: "telepathos:other", content: "other lane", reply_to: "tp-1" }], latest: 6 },
  2,
);
await runCorrelationCase(
  "malformed receipt interval",
  { deliveries: [{ seq: 5, chat_id: "telepathos:direct", content: "non-advancing", reply_to: "tp-1" }], latest: 5 },
  2,
);
await runCorrelationCase("valid exact correlation", null, 1);

server.close();
console.log(failures === 0 ? "HERMES TESTS PASS" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);
