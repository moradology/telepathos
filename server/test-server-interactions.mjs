// Focused regression coverage for STT failure handling and lane activity.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import WebSocket from "ws";

const serverDir = new URL(".", import.meta.url).pathname;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const bridgeLogs = new WeakMap();
const { shouldExpireCapturePreparation } = await import("./dist/index.js");
const { MAX_STORED_REPLY_ACK_TOMBSTONES } = await import("./dist/reply-ack-store.js");
const { targetIdentityFor } = await import("./dist/target-scope.js");

async function waitForEvent(events, start, predicate, description) {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    const match = events.slice(start).find(predicate);
    if (match) return match;
    await sleep(10);
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function waitForCondition(predicate, description) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function waitForBridgeOutput(child, predicate, description) {
  await waitForCondition(() => predicate(bridgeLogs.get(child) ?? ""), description);
}

/** Every protocol-v5 client must cross the hello/ready ordering barrier. */
async function completeHello(ws, token, installationId = "server-interactions-installation") {
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for hello ready")), 2000);
    const onMessage = (data, isBinary) => {
      if (!isBinary && JSON.parse(data.toString()).type === "ready") {
        clearTimeout(timer);
        ws.off("message", onMessage);
        resolve();
      }
    };
    ws.on("message", onMessage);
    ws.send(JSON.stringify({
      type: "hello",
      device: "server-interactions",
      installation_id: installationId,
      ...(token && { token }),
    }));
  });
}

async function freePort() {
  const probe = net.createServer();
  await new Promise((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", resolve);
  });
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

async function waitForPort(port) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const connected = await new Promise((resolve) => {
      const socket = net.createConnection({ port, host: "127.0.0.1" });
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => resolve(false));
    });
    if (connected) return;
    await sleep(20);
  }
  throw new Error(`timed out waiting for port ${port}`);
}

async function startBridge(env) {
  // Stamp hand-authored fixtures with the child bridge's exact target. The
  // production store still hard-rejects all pre-cutover snapshot versions.
  const childEnv = { ...process.env, ...env };
  const targetIdentity = targetIdentityFor(
    childEnv.TELEPATHOS_HERMES_URL?.trim() || null,
    childEnv.TELEPATHOS_TOKEN || undefined,
  );
  const lanesPath = childEnv.TELEPATHOS_LANES;
  if (lanesPath) {
    const ackPath = `${lanesPath}.reply-ack-bindings.json`;
    if (existsSync(ackPath)) {
      const snapshot = JSON.parse(readFileSync(ackPath, "utf8"));
      snapshot.bindings = (snapshot.bindings ?? []).map((entry) => ({ ...entry, target_identity: targetIdentity }));
      snapshot.tombstones = (snapshot.tombstones ?? []).map((entry) => ({ ...entry, target_identity: targetIdentity }));
      writeFileSync(ackPath, JSON.stringify(snapshot));
    }
    const outboxPath = `${lanesPath}.interaction-outbox.json`;
    if (existsSync(outboxPath)) {
      const snapshot = JSON.parse(readFileSync(outboxPath, "utf8"));
      snapshot.records = (snapshot.records ?? []).map((entry) => ({ ...entry, target_identity: targetIdentity }));
      writeFileSync(outboxPath, JSON.stringify(snapshot));
    }
  }
  const child = spawn(process.execPath, ["dist/index.js"], {
    cwd: serverDir,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  bridgeLogs.set(child, output);
  await new Promise((resolve, reject) => {
    const onData = (data) => {
      output += data.toString();
      bridgeLogs.set(child, output);
      if (output.includes("telepathos bridge listening")) resolve();
    };
    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      reject(new Error(`bridge exited before startup: code=${code} signal=${signal}\n${output}`));
    });
  });
  await waitForPort(Number(env.TELEPATHOS_PORT));
  await waitForPort(Number(env.TELEPATHOS_API_PORT));
  return child;
}

async function stopBridge(child) {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  await new Promise((resolve) => {
    const timer = setTimeout(resolve, 2000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function startHermesProbe({ stateDelayMs = 0 } = {}) {
  const requests = [];
  let stateCalls = 0;
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => { body += chunk; });
    req.on("end", () => {
      if (req.url === "/api/message") requests.push({ method: req.method, body });
      if (req.method === "GET" && req.url === "/api/state") {
        stateCalls += 1;
        const state = JSON.stringify({
          lanes: [{ id: "telepathos:direct", name: "direct", created_at: "2020-01-01", last_active: "2020-01-01" }],
          active_id: "telepathos:direct",
          previous_id: "telepathos:direct",
          revision: 0,
        });
        const delay = typeof stateDelayMs === "function" ? stateDelayMs() : stateDelayMs;
        setTimeout(() => {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(state);
        }, delay);
        return;
      }
      if (req.method === "GET" && req.url === "/api/delivery/head") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ latest: 0 }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      if (req.method === "POST" && req.url === "/api/message") {
        res.end(JSON.stringify({ ok: true, message_id: "tp-0" }));
      } else {
        res.end(JSON.stringify({ latest: 0, deliveries: [] }));
      }
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { server, requests, stateCalls: () => stateCalls, url: `http://127.0.0.1:${server.address().port}` };
}

/**
 * Models telepathosd's interaction ledger closely enough to catch a bridge
 * reusing an ID with different completed-turn metadata after a reconnect.
 */
async function startInteractionLedgerProbe() {
  const interactions = new Map();
  let nextMessageSeq = 0;
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => { body += chunk; });
    req.on("end", () => {
      if (req.method === "POST" && req.url === "/api/lanes/interaction") {
        const record = JSON.parse(body);
        const existing = interactions.get(record.interaction_id);
        if (existing !== undefined &&
            (existing.id !== record.id ||
             existing.interaction_created_at_ms !== record.interaction_created_at_ms)) {
          res.writeHead(409, { "Content-Type": "text/plain" });
          res.end("interaction_id was already recorded with different interaction metadata");
          return;
        }
        interactions.set(record.interaction_id, record);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, duplicate: existing !== undefined }));
        return;
      }
      if (req.method === "GET" && req.url === "/api/state") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          lanes: [{ id: "telepathos:direct", name: "direct", created_at: "2020-01-01", last_active: "2020-01-01" }],
          active_id: "telepathos:direct",
          previous_id: "telepathos:direct",
          revision: 0,
        }));
        return;
      }
      if (req.method === "POST" && req.url === "/api/message") {
        const messageId = `tp-${++nextMessageSeq}`;
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, message_id: messageId }));
        return;
      }
      if (req.method === "GET" && req.url === "/api/delivery/head") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ latest: nextMessageSeq }));
        return;
      }
      if (req.method === "GET" && req.url?.startsWith("/api/delivery?")) {
        const replyTo = new URL(req.url, "http://127.0.0.1").searchParams.get("reply_to");
        const sequence = replyTo === null ? 0 : Number(replyTo.slice(3));
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          latest: sequence,
          deliveries: replyTo === null ? [] : [{
            seq: sequence,
            chat_id: "telepathos:direct",
            content: "acknowledged",
            reply_to: replyTo,
          }],
        }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { server, interactions, url: `http://127.0.0.1:${server.address().port}` };
}

async function startDelayedDeliveryAckProbe() {
  let acknowledgeRequests = 0;
  let releaseFirstAcknowledgement;
  let firstAcknowledgementResolve;
  const firstAcknowledgement = new Promise((resolve) => {
    firstAcknowledgementResolve = resolve;
  });
  const releaseFirst = new Promise((resolve) => {
    releaseFirstAcknowledgement = resolve;
  });
  const server = createServer((req, res) => {
    if (req.method === "GET" && req.url?.startsWith("/api/delivery?")) {
      acknowledgeRequests += 1;
      const respond = () => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ latest: 1, deliveries: [] }));
      };
      if (acknowledgeRequests === 1) {
        firstAcknowledgementResolve();
        void releaseFirst.then(respond);
      } else {
        respond();
      }
      return;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server,
    url: `http://127.0.0.1:${server.address().port}`,
    firstAcknowledgement,
    releaseFirst: () => releaseFirstAcknowledgement(),
    acknowledgeRequests: () => acknowledgeRequests,
  };
}

async function startExpiredInteractionProbe() {
  let interactionRequests = 0;
  let firstInteractionResolve;
  let releaseFirstInteraction;
  const firstInteraction = new Promise((resolve) => {
    firstInteractionResolve = resolve;
  });
  const releaseFirst = new Promise((resolve) => {
    releaseFirstInteraction = resolve;
  });
  const server = createServer((req, res) => {
    if (req.method === "POST" && req.url === "/api/lanes/interaction") {
      interactionRequests += 1;
      const expire = () => {
        res.writeHead(410, { "Content-Type": "text/plain" });
        res.end("dedupe horizon expired");
      };
      if (interactionRequests === 1) {
        firstInteractionResolve();
        void releaseFirst.then(expire);
      } else {
        expire();
      }
      return;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server,
    url: `http://127.0.0.1:${server.address().port}`,
    firstInteraction,
    releaseFirst: () => releaseFirstInteraction(),
    interactionRequests: () => interactionRequests,
  };
}

async function startMetaModelProbe(toolCall) {
  const requests = [];
  let firstRequestResolve;
  let releaseFirstResponse;
  const firstRequest = new Promise((resolve) => { firstRequestResolve = resolve; });
  const firstResponseReleased = new Promise((resolve) => { releaseFirstResponse = resolve; });
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => { body += chunk; });
    req.on("end", async () => {
      if (req.method !== "POST" || req.url !== "/chat/completions") {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "not found" }));
        return;
      }
      const request = JSON.parse(body);
      requests.push(request);
      if (requests.length === 1) {
        firstRequestResolve(request);
        await firstResponseReleased;
      }
      const response = requests.length === 1
        ? { choices: [{ message: { tool_calls: [toolCall] } }] }
        : { choices: [{ message: { content: "model mutation applied" } }] };
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(response));
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server,
    url: `http://127.0.0.1:${server.address().port}`,
    requests,
    firstRequest,
    releaseFirst: () => releaseFirstResponse(),
  };
}

async function beginMetaUtterance(port, laneId, turnToken) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const events = [];
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  ws.on("message", (data, isBinary) => {
    if (!isBinary) events.push(JSON.parse(data.toString()));
  });
  await completeHello(ws);
  ws.send(JSON.stringify({ type: "meta_mode", turn_token: turnToken }));
  ws.send(JSON.stringify({ type: "lane", id: laneId, turn_token: turnToken }));

  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud);
  const completion = waitForEvent(
    events,
    0,
    (event) => event.type === "listening" && events.some((candidate) => candidate.type === "agent_end"),
    "meta model turn completion",
  );
  const quiet = Buffer.alloc(3200);
  const quietSender = (async () => {
    for (let n = 0; n < 22; n++) {
      ws.send(quiet);
      await sleep(80);
    }
  })();
  return {
    ws,
    events,
    done: Promise.all([quietSender, completion]),
  };
}

async function postLocalJson(apiPort, path, body) {
  const response = await fetch(`http://127.0.0.1:${apiPort}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  assert.equal(response.status, 200, `${path} failed: ${JSON.stringify(payload)}`);
  return payload;
}

async function testMetaModelProposalPreservesConcurrentApiMutation() {
  const directory = mkdtempSync(join(tmpdir(), "telepathos-meta-proposal-concurrency-"));
  const lanesPath = join(directory, "lanes.json");
  const wsPort = await freePort();
  const apiPort = await freePort();
  writeFileSync(lanesPath, JSON.stringify({
    lanes: [
      { id: "telepathos:direct", name: "direct", created_at: "2020-01-01T00:00:00.000Z", last_active: "2020-01-01T00:00:00.000Z" },
      { id: "telepathos:repo:lane-b", name: "lane-b", created_at: "2020-01-01T00:00:00.000Z", last_active: "2020-01-01T00:00:00.000Z" },
    ],
    active_id: "telepathos:direct",
    previous_id: "telepathos:direct",
  }));
  const probe = await startMetaModelProbe({
    id: "model-create",
    function: { name: "create_lane", arguments: JSON.stringify({ name: "model-added" }) },
  });
  let bridge;
  let turn;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      // A blank-looking config is local mode, just like hermesConfig(). This
      // verifies the model merge remains durable rather than taking an
      // in-memory-only remote branch because the raw environment is truthy.
      TELEPATHOS_HERMES_URL: "   ",
      TELEPATHOS_META_MODEL: "deterministic-meta-regression-model",
      TELEPATHOS_META_BASE_URL: probe.url,
    });
    turn = await beginMetaUtterance(wsPort, "telepathos:direct", "turn-meta-api-mutation");
    await probe.firstRequest;

    // Both mutations happen while runMetaAgent is still awaiting its first
    // provider response. The API's lane creation switches to that lane, then
    // a second API switch makes the final selection and previousId explicit;
    // the touch exercises a concurrent non-selection field update as well.
    await postLocalJson(apiPort, "/api/lanes", { name: "api-added" });
    await postLocalJson(apiPort, "/api/lanes/active", { id: "telepathos:repo:lane-b" });
    await postLocalJson(apiPort, "/api/lanes/touch", { id: "telepathos:repo:lane-b" });
    const during = await (await fetch(`http://127.0.0.1:${apiPort}/api/state`)).json();
    assert.equal(during.active, "lane-b");
    assert.equal(during.previousId, "telepathos:repo:api-added");
    const duringLaneB = during.lanes.find((lane) => lane.id === "telepathos:repo:lane-b");
    assert.notEqual(duringLaneB?.lastActive, "2020-01-01T00:00:00.000Z");

    // Releasing the exact deferred request lets the model's private proposal
    // create its independent lane. Its stale active/previous selection must
    // not replace the API's newer selection, and the API-added lane must stay.
    probe.releaseFirst();
    await turn.done;
    assert.equal(probe.requests.length, 2, "the deterministic model completed its tool round");
    const state = await (await fetch(`http://127.0.0.1:${apiPort}/api/state`)).json();
    assert.equal(state.active, "lane-b");
    assert.equal(state.activeId, "telepathos:repo:lane-b");
    assert.equal(state.previousId, "telepathos:repo:api-added");
    assert.equal(
      state.lanes.find((lane) => lane.id === "telepathos:repo:lane-b")?.lastActive,
      duringLaneB.lastActive,
    );
    assert.deepEqual(
      state.lanes.map((lane) => lane.name).sort(),
      ["api-added", "direct", "lane-b", "model-added"],
    );
    const durable = JSON.parse(readFileSync(lanesPath, "utf8"));
    assert.equal(durable.active_id, "telepathos:repo:lane-b");
    assert.equal(durable.previous_id, "telepathos:repo:api-added");
    assert(durable.lanes.some((lane) => lane.name === "api-added"));
    assert(durable.lanes.some((lane) => lane.name === "model-added"));
  } finally {
    probe.releaseFirst();
    turn?.ws.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

async function testMetaModelProposalPreservesAbaSelection() {
  const directory = mkdtempSync(join(tmpdir(), "telepathos-meta-proposal-aba-"));
  const lanesPath = join(directory, "lanes.json");
  const wsPort = await freePort();
  const apiPort = await freePort();
  writeFileSync(lanesPath, JSON.stringify({
    lanes: [
      { id: "telepathos:direct", name: "direct", created_at: "2020-01-01T00:00:00.000Z", last_active: "2020-01-01T00:00:00.000Z" },
      { id: "telepathos:repo:lane-b", name: "lane-b", created_at: "2020-01-01T00:00:00.000Z", last_active: "2020-01-01T00:00:00.000Z" },
      { id: "telepathos:repo:lane-c", name: "lane-c", created_at: "2020-01-01T00:00:00.000Z", last_active: "2020-01-01T00:00:00.000Z" },
    ],
    active_id: "telepathos:direct",
    previous_id: "telepathos:repo:lane-b",
  }));
  const probe = await startMetaModelProbe({
    id: "model-switch",
    function: { name: "switch_lane", arguments: JSON.stringify({ name: "lane-c" }) },
  });
  let bridge;
  let turn;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: "",
      TELEPATHOS_META_MODEL: "deterministic-meta-regression-model",
      TELEPATHOS_META_BASE_URL: probe.url,
    });
    turn = await beginMetaUtterance(wsPort, "telepathos:direct", "turn-meta-aba");
    await probe.firstRequest;

    // The API performs A→B→A while the model still owns an A snapshot. Both
    // final selection fields exactly equal the captured base, so structural
    // three-way comparison alone cannot see this true ABA transition.
    await postLocalJson(apiPort, "/api/lanes/active", { id: "telepathos:repo:lane-b" });
    await postLocalJson(apiPort, "/api/lanes/active", { id: "telepathos:direct" });
    const during = await (await fetch(`http://127.0.0.1:${apiPort}/api/state`)).json();
    assert.equal(during.activeId, "telepathos:direct");
    assert.equal(during.previousId, "telepathos:repo:lane-b");

    probe.releaseFirst();
    await turn.done;
    assert.equal(probe.requests.length, 2);
    const state = await (await fetch(`http://127.0.0.1:${apiPort}/api/state`)).json();
    assert.equal(state.activeId, "telepathos:direct");
    assert.equal(state.active, "direct");
    assert.equal(state.previousId, "telepathos:repo:lane-b");
    const durable = JSON.parse(readFileSync(lanesPath, "utf8"));
    assert.equal(durable.active_id, "telepathos:direct");
    assert.equal(durable.previous_id, "telepathos:repo:lane-b");
  } finally {
    probe.releaseFirst();
    turn?.ws.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

async function sendUtterance(port, {
  laneId = "telepathos:direct",
  revision = 0,
  includeRevision = true,
  turnToken = `turn-${process.pid}-${Date.now()}`,
} = {}) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const events = [];
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  ws.on("message", (data, isBinary) => {
    if (!isBinary) events.push(JSON.parse(data.toString()));
  });
  await completeHello(ws);
  if (laneId !== null) {
    const lane = { type: "lane", id: laneId, turn_token: turnToken };
    if (includeRevision) lane.revision = revision;
    ws.send(JSON.stringify(lane));
  }

  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud);
  const quiet = Buffer.alloc(3200);
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for interaction completion")), 10000);
    const onMessage = (data, isBinary) => {
      if (!isBinary && JSON.parse(data.toString()).type === "listening") {
        clearTimeout(timer);
        ws.off("message", onMessage);
        resolve();
      }
    };
    ws.on("message", onMessage);
    (async () => {
      for (let n = 0; n < 22; n++) {
        ws.send(quiet);
        await sleep(80);
      }
    })().catch(reject);
  });
  ws.close();
  await sleep(50);
  return events;
}

async function testSttFailureFailsClosed() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-failure.json`;
  const probe = await startHermesProbe();
  let bridge;
  const sttSecret = "regression-invalid-backend-secret";
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: sttSecret,
      TELEPATHOS_HERMES_URL: probe.url,
    });
    const events = await sendUtterance(wsPort);
    assert(events.some((event) => event.type === "error" && event.message === "stt provider unavailable"));
    assert(!events.some((event) => JSON.stringify(event).includes(sttSecret)),
      "arbitrary STT exception details must never appear in handset frames");
    assert(!events.some((event) => event.type === "agent_delta"));
    assert.equal(probe.requests.length, 0, "STT failures must not be posted to Hermes");
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
  }
}

async function testVoiceInteractionPersistsLaneActivity() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-lane.json`;
  const oldLastActive = "2020-01-01T00:00:00.000Z";
  writeFileSync(lanesPath, JSON.stringify({
    lanes: [{ id: "telepathos:direct", name: "direct", created_at: oldLastActive, last_active: oldLastActive, interactions: 4 }],
    active_id: "telepathos:direct",
    previous_id: "telepathos:direct",
  }));
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: "",
    });
    const events = await sendUtterance(wsPort);
    assert(events.some((event) => event.type === "agent_delta" && event.text.includes("transcription unavailable")));
    const saved = JSON.parse(readFileSync(lanesPath, "utf8"));
    assert.equal(saved.lanes[0].interactions, 5);
    assert.notEqual(saved.lanes[0].last_active, oldLastActive);
  } finally {
    if (bridge) await stopBridge(bridge);
    try { rmSync(lanesPath); } catch {}
  }
}

async function testStandaloneLaneSaveFailureRecoversFsm() {
  const directory = mkdtempSync(join(tmpdir(), "telepathos-lane-save-failure-"));
  const blockedDirectory = `${directory}-blocked`;
  const lanesPath = join(directory, "lanes.json");
  const wsPort = await freePort();
  const apiPort = await freePort();
  const oldLastActive = "2020-01-01T00:00:00.000Z";
  writeFileSync(lanesPath, JSON.stringify({
    lanes: [{ id: "telepathos:direct", name: "direct", created_at: oldLastActive, last_active: oldLastActive, interactions: 4 }],
    active_id: "telepathos:direct",
    previous_id: "telepathos:direct",
  }));
  let bridge;
  let blocked = false;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: "",
    });

    // The bridge has already loaded its registry.  Make only later writes
    // fail before rename, so the transaction must roll back and the finally
    // path must still release the FSM from processing.
    renameSync(directory, blockedDirectory);
    writeFileSync(directory, "not a directory");
    blocked = true;
    const failedEvents = await sendUtterance(wsPort, { turnToken: "turn-standalone-save-failure" });
    assert(failedEvents.some((event) => event.type === "error" &&
      event.message === "lane activity was not persisted: request failed"),
    "standalone lane-stat persistence failure must be reported");
    assert(!failedEvents.some((event) => JSON.stringify(event).includes(lanesPath) ||
      JSON.stringify(event).includes(blockedDirectory)),
    "lane persistence paths and filesystem details must never appear in handset frames");
    assert(failedEvents.some((event) => event.type === "listening"),
      "standalone lane-stat persistence failure must return the FSM to listening");

    rmSync(directory, { force: true });
    renameSync(blockedDirectory, directory);
    blocked = false;
    const recoveredEvents = await sendUtterance(wsPort, { turnToken: "turn-standalone-save-recovery" });
    assert(recoveredEvents.some((event) => event.type === "agent_delta"),
      "a later capture must be accepted after standalone persistence recovers");
    const saved = JSON.parse(readFileSync(lanesPath, "utf8"));
    assert.equal(saved.lanes[0].interactions, 5,
      "the failed interaction must not remain in standalone lane stats");
  } finally {
    if (bridge) await stopBridge(bridge);
    if (blocked) {
      try { rmSync(directory, { force: true }); } catch {}
      try { renameSync(blockedDirectory, directory); } catch {}
    }
    rmSync(directory, { recursive: true, force: true });
    rmSync(blockedDirectory, { recursive: true, force: true });
  }
}

async function testRemoteInteractionIdsRemainUniqueAcrossReconnects() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-interaction-id-reconnect.json`;
  const probe = await startInteractionLedgerProbe();
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });

    await sendUtterance(wsPort, { turnToken: "turn-reconnect-first" });
    await waitForCondition(() => probe.interactions.size === 1, "first accepted interaction record");
    // telepathosd's idempotency key includes immutable creation metadata. Make
    // this an unambiguously new completed turn, not a retry of the first.
    await sleep(20);

    await sendUtterance(wsPort, { turnToken: "turn-reconnect-second" });
    await waitForCondition(() => probe.interactions.size === 2, "second accepted interaction record");

    const records = [...probe.interactions.values()];
    assert.equal(records.length, 2);
    assert.notEqual(records[0].interaction_id, records[1].interaction_id);
    assert(records.every((record) => record.id === "telepathos:direct"));
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(`${lanesPath}.interaction-outbox.json`); } catch {}
  }
}

async function testRemoteRejectsUnsnapshottedLane() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-snapshot.json`;
  const probe = await startHermesProbe();
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    const events = await sendUtterance(wsPort, { includeRevision: false });
    assert(events.some((event) => event.type === "error" && event.message === "lane snapshot required"));
    assert.equal(probe.requests.length, 0, "unsnapshotted remote turns must not reach Hermes");
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
  }
}

async function testCapturePreparationDeadlineFencesValidationAndAudio() {
  const preparation = { socket: {}, turnToken: "turn-a", generation: 7, deadlineAtMs: 100, audioStarted: false };
  assert.equal(shouldExpireCapturePreparation(preparation, preparation.socket, "turn-a", 7, 99), false);
  assert.equal(shouldExpireCapturePreparation(preparation, preparation.socket, "turn-a", 7, 100), true);
  assert.equal(shouldExpireCapturePreparation(
    { ...preparation, audioStarted: true }, preparation.socket, "turn-a", 7, 1000), false);
  assert.equal(shouldExpireCapturePreparation(preparation, {}, "turn-a", 7, 100), false);
  assert.equal(shouldExpireCapturePreparation(preparation, preparation.socket, "turn-b", 7, 100), false);
  assert.equal(shouldExpireCapturePreparation(preparation, preparation.socket, "turn-a", 8, 100), false);

  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-capture-deadline.json`;
  const outboxPath = `${lanesPath}.interaction-outbox.json`;
  let delayed = true;
  const probe = await startHermesProbe({ stateDelayMs: () => delayed ? 100 : 0 });
  let bridge;
  let ws;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_CAPTURE_PREPARATION_DEADLINE_MS: "30",
    });
    ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    ws.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    await completeHello(ws);

    // Validation outlives the deadline. The timer must be armed before the
    // await, and its stale continuation must not reserve after timeout.
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: "turn-timeout-before-validation" }));
    await waitForEvent(events, 0, (event) => event.type === "error" && event.message === "capture preparation timed out",
      "capture preparation timeout");
    await waitForCondition(() => probe.stateCalls() >= 1, "delayed lane validation");
    await sleep(20);
    assert(!existsSync(outboxPath), "stale validation must not create a reservation after timeout");

    // A later validated preparation can reserve the reclaimed capacity and
    // its own timeout cancels exactly that reservation.
    delayed = false;
    const secondStart = events.length;
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: "turn-timeout-after-validation" }));
    await waitForCondition(() => existsSync(outboxPath) &&
      JSON.parse(readFileSync(outboxPath, "utf8")).records.length === 1,
    "validated remote reservation");
    await waitForEvent(events, secondStart, (event) => event.type === "error" && event.message === "capture preparation timed out",
      "validated capture preparation timeout");
    await waitForCondition(() => JSON.parse(readFileSync(outboxPath, "utf8")).records.length === 0,
      "validated reservation cleanup");

    // Audio fences the exact preparation before its deadline. The reservation
    // must survive the deadline until the existing explicit cancellation path
    // is used.
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: "turn-audio-started" }));
    await waitForCondition(() => existsSync(outboxPath) &&
      JSON.parse(readFileSync(outboxPath, "utf8")).records.length === 1,
    "audio-turn reservation");
    const loud = Buffer.alloc(3200);
    for (let i = 0; i < loud.length / 2; i++) loud.writeInt16LE(8000, i * 2);
    ws.send(loud);
    await sleep(60);
    assert.equal(JSON.parse(readFileSync(outboxPath, "utf8")).records.length, 1,
      "preparation deadline must not cancel after audio begins");
    ws.send(JSON.stringify({ type: "command", command: "cancel_capture", turn_token: "turn-audio-started" }));
    await waitForCondition(() => JSON.parse(readFileSync(outboxPath, "utf8")).records.length === 0,
      "explicit audio-turn cancellation");
  } finally {
    ws?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(outboxPath); } catch {}
  }
}

async function testReservationCleanupRunsBeforeCapacityPreflight() {
  const directory = mkdtempSync(join(tmpdir(), "telepathos-remote-reservation-recovery-"));
  const lanesPath = join(directory, "lanes.json");
  const outboxPath = `${lanesPath}.interaction-outbox.json`;
  const wsPort = await freePort();
  const apiPort = await freePort();
  const probe = await startHermesProbe();
  let bridge;
  let ws;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_INTERACTION_OUTBOX_MAX: "1",
      TELEPATHOS_CAPTURE_PREPARATION_DEADLINE_MS: "60000",
      TELEPATHOS_TEST_FAIL_NEXT_INTERACTION_OUTBOX_CANCEL_BEFORE_RENAME: "1",
    });
    ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    ws.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    await completeHello(ws);

    const firstTurn = "turn-reservation-cleanup-first";
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: firstTurn }));
    await waitForCondition(() => existsSync(outboxPath) &&
      JSON.parse(readFileSync(outboxPath, "utf8")).records.length === 1,
    "first capacity reservation");
    const firstInteractionId = JSON.parse(readFileSync(outboxPath, "utf8")).records[0].interaction_id;

    // The injected pre-rename failure leaves the reserved row durable and
    // records its deletion for a later, safe retry.
    ws.send(JSON.stringify({ type: "command", command: "cancel_capture", turn_token: firstTurn }));
    await waitForBridgeOutput(
      bridge,
      (output) => output.includes("persistence failed before rename"),
      "recoverable reservation-cancel failure",
    );
    assert.equal(JSON.parse(readFileSync(outboxPath, "utf8")).records.length, 1,
      "failed reservation cleanup must retain the durable reserved row");

    // A second lane snapshot must reach reserve(), which sweeps the stale row
    // before applying the capacity-1 check. The old interaction must never be
    // promoted or emitted as a remote side effect.
    const secondTurn = "turn-reservation-cleanup-second";
    const eventStart = events.length;
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: secondTurn }));
    await waitForCondition(() => {
      if (!existsSync(outboxPath)) return false;
      const records = JSON.parse(readFileSync(outboxPath, "utf8")).records;
      return records.length === 1 && records[0].interaction_id !== firstInteractionId;
    }, "capacity reservation after cleanup recovery");
    assert(!events.slice(eventStart).some((event) => event.type === "error" &&
      event.message.includes("outbox is full")),
    "recoverable cleanup must not be rejected by the capacity preflight");

    ws.send(JSON.stringify({ type: "command", command: "cancel_capture", turn_token: secondTurn }));
    await waitForCondition(() => JSON.parse(readFileSync(outboxPath, "utf8")).records.length === 0,
      "second reservation cleanup");
  } finally {
    ws?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

async function testLostAgentEndReplaysBeforePendingNarrationAndSurvivesRestart() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-lost-agent-end.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const probe = await startDelayedDeliveryAckProbe();
  const receipt = {
    lane_id: "telepathos:direct",
    reply_to: "tp-lost-agent-end",
    after_seq: 1,
    through_seq: 2,
    turn_token: "turn-lost-agent-end",
    interaction_id: "i-lost-agent-end",
  };
  writeFileSync(ackPath, JSON.stringify({
    // Models a bridge crash after the original agent_end was persisted but
    // before the handset received it. The v8 record has the full replay text
    // and durable installation owner.
    version: 8,
    bindings: [{ ...receipt,
      installation_id: "server-interactions-installation",
      reply_text: "durably replayed pending narration",
      state: "prepared",
      prepared_at_ms: 1_700_000_000_000,
      owner_last_seen_at_ms: 1_700_000_000_100,
      received_at_ms: null,
      consumed_at_ms: null,
    }],
    tombstones: [],
  }));
  let bridge;
  let firstSocket;
  let secondSocket;
  let thirdSocket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    firstSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const firstEvents = [];
    firstSocket.on("message", (data, isBinary) => {
      if (!isBinary) firstEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      firstSocket.once("open", resolve);
      firstSocket.once("error", reject);
    });
    await completeHello(firstSocket);
    const replay = await waitForEvent(
      firstEvents,
      0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "first recovered agent_end",
    );
    assert.equal(replay.text, "durably replayed pending narration");
    assert(firstEvents.findIndex((event) => event === replay) < firstEvents.findIndex((event) => event.type === "ready"),
      "recovered agent_end must arrive before ready opens capture/pending traffic");

    // Simulate another socket/process loss before Android can prove its local
    // receipt. A fresh bridge must replay exactly the same durable envelope.
    firstSocket.terminate();
    await stopBridge(bridge);
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    secondSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const secondEvents = [];
    secondSocket.on("message", (data, isBinary) => {
      if (!isBinary) secondEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      secondSocket.once("open", resolve);
      secondSocket.once("error", reject);
    });
    await completeHello(secondSocket);
    const secondReplay = await waitForEvent(
      secondEvents,
      0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "recovered agent_end after bridge restart",
    );
    assert.deepEqual(secondReplay, replay, "replay must be byte-for-byte envelope-equivalent");

    // Android has atomically stored the replay record. Only this proof lets
    // the bridge accept the reply_ack emitted after pending narration speaks
    // and consumes the matching delivery.
    const receivedStart = secondEvents.length;
    secondSocket.send(JSON.stringify({ type: "reply_received", ...receipt }));
    await waitForEvent(
      secondEvents,
      receivedStart,
      (event) => event.type === "reply_received" && event.reply_to === receipt.reply_to,
      "durable handset receipt confirmation",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].state, "received");
    const acknowledgementStart = secondEvents.length;
    secondSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await probe.firstAcknowledgement;
    probe.releaseFirst();
    await waitForEvent(
      secondEvents,
      acknowledgementStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipt.reply_to,
      "pending narration delivery acknowledgement",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].state, "consumed");

    // A restart after external consume must only replay the bridge-side
    // acknowledgement; it must never consume the delivery twice.
    secondSocket.terminate();
    await stopBridge(bridge);
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    thirdSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const thirdEvents = [];
    thirdSocket.on("message", (data, isBinary) => {
      if (!isBinary) thirdEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      thirdSocket.once("open", resolve);
      thirdSocket.once("error", reject);
    });
    await completeHello(thirdSocket);
    const restartAckStart = thirdEvents.length;
    thirdSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await waitForEvent(
      thirdEvents,
      restartAckStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipt.reply_to,
      "post-restart acknowledgement",
    );
    assert.equal(probe.acknowledgeRequests(), 1,
      "a consumed delivery must not be consumed a second time after restart");
    thirdSocket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForCondition(() => JSON.parse(readFileSync(ackPath, "utf8")).bindings.length === 0,
      "terminal reply binding removal");
  } finally {
    firstSocket?.terminate();
    secondSocket?.terminate();
    thirdSocket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testReplyReceiptsRequireInstallationOwner() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-receipt-owner.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const ownerA = "owner-a-installation";
  const ownerB = "owner-b-installation";
  const receipt = {
    lane_id: "telepathos:direct",
    reply_to: "tp-receipt-owner",
    after_seq: 10,
    through_seq: 11,
    turn_token: "turn-receipt-owner",
    interaction_id: "i-receipt-owner",
  };
  writeFileSync(ackPath, JSON.stringify({
    version: 8,
    bindings: [{
      ...receipt,
      installation_id: ownerA,
      reply_text: "owner A's durable reply",
      state: "prepared",
      prepared_at_ms: 1_700_000_000_000,
      owner_last_seen_at_ms: 1_700_000_000_100,
      received_at_ms: null,
      consumed_at_ms: null,
    }],
    tombstones: [],
  }));
  const probe = await startDelayedDeliveryAckProbe();
  let bridge;
  let foreignSocket;
  let firstOwnerSocket;
  let recoveredOwnerSocket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    const openSocket = async () => {
      const socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
      const events = [];
      socket.on("message", (data, isBinary) => {
        if (!isBinary) events.push(JSON.parse(data.toString()));
      });
      await new Promise((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });
      return { socket, events };
    };

    const foreign = await openSocket();
    foreignSocket = foreign.socket;
    await completeHello(foreignSocket, undefined, ownerB);
    await sleep(80);
    assert(!foreign.events.some((event) => event.type === "agent_end" && event.reply_to === receipt.reply_to),
      "a different installation must never receive an owner A replay");
    const foreignReceiptStart = foreign.events.length;
    foreignSocket.send(JSON.stringify({ type: "reply_received", ...receipt }));
    await sleep(80);
    assert(!foreign.events.slice(foreignReceiptStart).some((event) => event.type === "reply_received"),
      "a different installation must not transition a prepared receipt");
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].state, "prepared");

    // The first owner socket loses the frame before it can prove local
    // durability. A new socket from that same installation must replay it.
    const firstOwner = await openSocket();
    firstOwnerSocket = firstOwner.socket;
    await completeHello(firstOwnerSocket, undefined, ownerA);
    await waitForEvent(
      firstOwner.events,
      0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "first owner-specific recovered agent_end",
    );
    firstOwnerSocket.terminate();

    const recoveredOwner = await openSocket();
    recoveredOwnerSocket = recoveredOwner.socket;
    await completeHello(recoveredOwnerSocket, undefined, ownerA);
    await waitForEvent(
      recoveredOwner.events,
      0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "same-installation recovered agent_end",
    );
    const ownerReceiptStart = recoveredOwner.events.length;
    recoveredOwnerSocket.send(JSON.stringify({ type: "reply_received", ...receipt }));
    await waitForEvent(
      recoveredOwner.events,
      ownerReceiptStart,
      (event) => event.type === "reply_received" && event.reply_to === receipt.reply_to,
      "owner receipt confirmation",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].state, "received");

    const foreignAckStart = foreign.events.length;
    foreignSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await sleep(80);
    assert.equal(probe.acknowledgeRequests(), 0, "foreign owner must not consume delivery");
    assert(!foreign.events.slice(foreignAckStart).some((event) => event.type === "reply_acknowledged"),
      "foreign owner must not receive an acknowledgement confirmation");

    const ownerAckStart = recoveredOwner.events.length;
    recoveredOwnerSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await probe.firstAcknowledgement;
    probe.releaseFirst();
    await waitForEvent(
      recoveredOwner.events,
      ownerAckStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipt.reply_to,
      "owner delivery acknowledgement",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].state, "consumed");

    const foreignRetireStart = foreign.events.length;
    foreignSocket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await sleep(80);
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 1,
      "foreign owner must not retire a consumed receipt");
    assert(!foreign.events.slice(foreignRetireStart).some((event) => event.type === "reply_ack_retired"),
      "foreign owner must not receive a retirement confirmation");

    const ownerRetireStart = recoveredOwner.events.length;
    recoveredOwnerSocket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForEvent(
      recoveredOwner.events,
      ownerRetireStart,
      (event) => event.type === "reply_ack_retired" && event.reply_to === receipt.reply_to,
      "owner retirement confirmation",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 0);
  } finally {
    foreignSocket?.terminate();
    firstOwnerSocket?.terminate();
    recoveredOwnerSocket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testAbandonedReplyAckOwnerReconcilesToRotatedInstallation() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-reply-ack-rotation.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const ownerA = "rotated-owner-a";
  const ownerB = "rotated-owner-b";
  const receipt = {
    lane_id: "telepathos:direct",
    reply_to: "tp-rotated-owner",
    after_seq: 20,
    through_seq: 21,
    turn_token: "turn-rotated-owner",
    interaction_id: "i-rotated-owner",
  };
  writeFileSync(ackPath, JSON.stringify({
    version: 8,
    bindings: [{
      ...receipt,
      installation_id: ownerA,
      reply_text: "reply survives installation rotation",
      state: "prepared",
      prepared_at_ms: Date.now() - 10_000,
      owner_last_seen_at_ms: Date.now() - 10_000,
      received_at_ms: null,
      consumed_at_ms: null,
    }],
    tombstones: [],
  }));
  const probe = await startDelayedDeliveryAckProbe();
  let bridge;
  let oldSocket;
  let newSocket;
  let lateOldSocket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_REPLY_ACK_ABANDONMENT_MS: "80",
    });
    const open = async (installationId) => {
      const events = [];
      const socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
      socket.on("message", (data, isBinary) => {
        if (!isBinary) events.push(JSON.parse(data.toString()));
      });
      await new Promise((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });
      await completeHello(socket, undefined, installationId);
      return { socket, events };
    };

    // An old installation may reconnect during the abandonment grace period.
    // Its active socket fences migration, and closing it records a fresh
    // durable last-seen timestamp.
    const old = await open(ownerA);
    oldSocket = old.socket;
    await waitForEvent(old.events, 0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "old-owner replay before rotation");
    oldSocket.terminate();
    await sleep(140);

    const replacement = await open(ownerB);
    newSocket = replacement.socket;
    const replay = await waitForEvent(replacement.events, 0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "replacement-owner replay after rotation");
    assert.equal(replay.text, "reply survives installation rotation");
    const migrated = JSON.parse(readFileSync(ackPath, "utf8")).bindings[0];
    assert.equal(migrated.installation_id, ownerB,
      "only the server reconciliation path may change the binding owner");
    assert.equal(migrated.state, "prepared",
      "an old installation receipt proof must not authorize the replacement");

    // The old installation can still present the exact old receipt after its
    // socket is gone; the new owner binding rejects it because ownership is
    // checked before phase transition or external consume.
    const lateOld = await open(ownerA);
    lateOldSocket = lateOld.socket;
    const oldAttemptStart = lateOld.events.length;
    lateOldSocket.send(JSON.stringify({ type: "reply_received", ...receipt }));
    await sleep(80);
    assert.equal(probe.acknowledgeRequests(), 0,
      "a revoked installation must not consume the still-owned delivery");
    assert(!lateOld.events.slice(oldAttemptStart).some((event) => event.type === "reply_received"));
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].installation_id, ownerB);

    const receivedStart = replacement.events.length;
    newSocket.send(JSON.stringify({ type: "reply_received", ...receipt }));
    await waitForEvent(replacement.events, receivedStart,
      (event) => event.type === "reply_received" && event.reply_to === receipt.reply_to,
      "replacement durable receipt proof");
    const ackStart = replacement.events.length;
    newSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await probe.firstAcknowledgement;
    probe.releaseFirst();
    await waitForEvent(replacement.events, ackStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipt.reply_to,
      "replacement delivery acknowledgement");
    const retireStart = replacement.events.length;
    newSocket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForEvent(replacement.events, retireStart,
      (event) => event.type === "reply_ack_retired" && event.reply_to === receipt.reply_to,
      "replacement terminal retirement");
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 0);
  } finally {
    oldSocket?.terminate();
    newSocket?.terminate();
    lateOldSocket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testReplyAckOwnerClockRollbackPreservesHighWaterMarkAcrossRestart() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-reply-ack-clock-rollback.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const ownerA = "clock-rollback-owner-a";
  const ownerB = "clock-rollback-owner-b";
  const receipt = {
    lane_id: "telepathos:direct",
    reply_to: "tp-clock-rollback",
    after_seq: 30,
    through_seq: 31,
    turn_token: "turn-clock-rollback",
    interaction_id: "i-clock-rollback",
  };
  const durableHighWaterMark = Date.now() + 60_000;
  writeFileSync(ackPath, JSON.stringify({
    version: 8,
    bindings: [{
      ...receipt,
      installation_id: ownerA,
      reply_text: "reply protected by the durable clock high-water mark",
      state: "prepared",
      prepared_at_ms: Date.now(),
      owner_last_seen_at_ms: durableHighWaterMark,
      received_at_ms: null,
      consumed_at_ms: null,
    }],
    tombstones: [],
  }));

  let bridge;
  let ownerSocket;
  let competitorSocket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_REPLY_ACK_ABANDONMENT_MS: "80",
    });

    const ownerEvents = [];
    ownerSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    ownerSocket.on("message", (data, isBinary) => {
      if (!isBinary) ownerEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      ownerSocket.once("open", resolve);
      ownerSocket.once("error", reject);
    });
    await completeHello(ownerSocket, undefined, ownerA);
    await waitForEvent(
      ownerEvents,
      0,
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
      "clock rollback owner replay",
    );

    // The process clock is below the durable timestamp. Reconnecting and
    // disconnecting must not overwrite that future high-water mark.
    const ownerClosed = new Promise((resolve) => ownerSocket.once("close", resolve));
    ownerSocket.terminate();
    await ownerClosed;
    const afterDisconnect = JSON.parse(readFileSync(ackPath, "utf8"));
    assert.equal(afterDisconnect.bindings[0].owner_last_seen_at_ms, durableHighWaterMark);

    await stopBridge(bridge);
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_REPLY_ACK_ABANDONMENT_MS: "80",
    });

    // Give the buggy lower timestamp enough time to pass the abandonment
    // window while the durable high-water mark is still safely in the future.
    await sleep(200);
    const competitorEvents = [];
    competitorSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    competitorSocket.on("message", (data, isBinary) => {
      if (!isBinary) competitorEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      competitorSocket.once("open", resolve);
      competitorSocket.once("error", reject);
    });
    await completeHello(competitorSocket, undefined, ownerB);
    await sleep(150);
    assert(!competitorEvents.some(
      (event) => event.type === "agent_end" && event.reply_to === receipt.reply_to,
    ), "clock rollback must not authorize early reply-ack migration");
    assert.equal(
      JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].installation_id,
      ownerA,
    );
  } finally {
    ownerSocket?.terminate();
    competitorSocket?.terminate();
    if (bridge) await stopBridge(bridge);
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testExpiredConsumedReplyAcksReclaimCapacity() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-reply-ack-expiry.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const now = Date.now() - 10_000;
  const bindings = Array.from({ length: 64 }, (_, index) => ({
    lane_id: "telepathos:direct",
    reply_to: `tp-expired-${index}`,
    after_seq: index * 2,
    through_seq: index * 2 + 1,
    turn_token: `turn-expired-${index}`,
    interaction_id: `i-expired-${index}`,
    installation_id: "abandoned-consumed-owner",
    reply_text: `already consumed ${index}`,
    state: "consumed",
    prepared_at_ms: now,
    owner_last_seen_at_ms: now,
    received_at_ms: now + 1,
    consumed_at_ms: now + 2,
  }));
  writeFileSync(ackPath, JSON.stringify({ version: 8, bindings, tombstones: [] }));
  const probe = await startInteractionLedgerProbe();
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_REPLY_ACK_CONSUMED_RETENTION_MS: "50",
    });
    await waitForCondition(
      () => JSON.parse(readFileSync(ackPath, "utf8")).bindings.length === 0,
      "expired consumed reply-ack capacity recovery",
    );
    const events = await sendUtterance(wsPort, { turnToken: "turn-after-expiry" });
    assert(events.some((event) => event.type === "agent_end" && event.reply_to === "tp-1"),
      "capacity recovered from terminal abandoned bindings");
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 1);
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testReplyAckTombstoneCapacityFailsClosed() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const directory = mkdtempSync(join(tmpdir(), "telepathos-reply-ack-tombstone-capacity-"));
  const lanesPath = join(directory, "lanes.json");
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const now = Date.now();
  const bindings = Array.from({ length: 64 }, (_, index) => ({
    lane_id: "telepathos:direct",
    reply_to: `tp-full-live-${index}`,
    after_seq: index * 2,
    through_seq: index * 2 + 1,
    turn_token: `turn-full-live-${index}`,
    interaction_id: `i-full-live-${index}`,
    installation_id: "offline-consumed-owner",
    reply_text: `already consumed ${index}`,
    state: "consumed",
    prepared_at_ms: now - 2_000,
    owner_last_seen_at_ms: now - 2_000,
    received_at_ms: now - 1_500,
    consumed_at_ms: now - 1_000,
  }));
  const tombstones = Array.from({ length: MAX_STORED_REPLY_ACK_TOMBSTONES }, (_, index) => ({
    installation_id: "offline-consumed-owner",
    lane_id: "telepathos:direct",
    reply_to: `tp-unexpired-terminal-${index}`,
    after_seq: 10_000 + index * 2,
    through_seq: 10_001 + index * 2,
    turn_token: `turn-unexpired-terminal-${index}`,
    interaction_id: `i-unexpired-terminal-${index}`,
    consumed_at_ms: now - 2_000,
    tombstoned_at_ms: now - 100,
  }));
  writeFileSync(ackPath, JSON.stringify({ version: 8, bindings, tombstones }));
  const probe = await startInteractionLedgerProbe();
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_REPLY_ACK_CONSUMED_RETENTION_MS: "50",
      TELEPATHOS_REPLY_ACK_TOMBSTONE_RETENTION_MS: "5000",
    });
    await waitForBridgeOutput(
      bridge,
      (output) => output.includes("reply acknowledgement terminal capacity is full"),
      "full unexpired reply-ack tombstone capacity block",
    );
    const blockedSnapshot = JSON.parse(readFileSync(ackPath, "utf8"));
    assert.equal(blockedSnapshot.bindings.length, 64,
      "full terminal capacity must retain every consumed live binding");
    assert.equal(blockedSnapshot.tombstones.length, 64,
      "full terminal capacity must retain every unexpired tombstone");
    assert.equal(blockedSnapshot.tombstones[0].reply_to, "tp-unexpired-terminal-0",
      "an unexpired tombstone must never be rotated out");

    const events = await sendUtterance(wsPort, { turnToken: "turn-full-terminal-capacity" });
    const blocked = events.find((event) => event.type === "error" && event.message.includes("reply acknowledgement capacity is full"));
    assert(blocked, `full live capacity must surface a fail-closed availability error: ${JSON.stringify(events)}`);
    assert.match(blocked.message, /unexpired terminal tombstones/);
    assert(!events.some((event) => event.type === "agent_end"),
      "capacity blocking must not emit an untracked remote reply");
    const afterAttempt = JSON.parse(readFileSync(ackPath, "utf8"));
    assert.equal(afterAttempt.bindings.length, 64);
    assert.equal(afterAttempt.tombstones.length, 64);
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

async function testReplyAckTombstoneSweepIsAtomicAndRestartSafe() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const directory = mkdtempSync(join(tmpdir(), "telepathos-reply-ack-tombstone-sweep-"));
  const lanesPath = join(directory, "lanes.json");
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const now = Date.now();
  const tombstone = (index, tombstonedAtMs = now - 100) => {
    const sequenceIndex = typeof index === "number" ? index : 1_000;
    return {
      installation_id: "late-ack-sweep-owner",
      lane_id: "telepathos:direct",
      reply_to: `tp-sweep-terminal-${index}`,
      after_seq: 20_000 + sequenceIndex * 2,
      through_seq: 20_001 + sequenceIndex * 2,
      turn_token: `turn-sweep-terminal-${index}`,
      interaction_id: `i-sweep-terminal-${index}`,
      consumed_at_ms: now - 20_000,
      tombstoned_at_ms: tombstonedAtMs,
    };
  };
  const binding = (index) => ({
    lane_id: "telepathos:direct",
    reply_to: `tp-sweep-live-${index}`,
    after_seq: 30_000 + index * 2,
    through_seq: 30_001 + index * 2,
    turn_token: `turn-sweep-live-${index}`,
    interaction_id: `i-sweep-live-${index}`,
    installation_id: "late-ack-sweep-owner",
    reply_text: `already consumed sweep ${index}`,
    state: "consumed",
    prepared_at_ms: now - 2_000,
    owner_last_seen_at_ms: now - 2_000,
    received_at_ms: now - 1_500,
    consumed_at_ms: now - 1_000,
  });
  // One expired terminal slot is available, but two consumed candidates are
  // ready. The sweep must prune the expired tombstone without reclaiming only
  // the first live binding.
  writeFileSync(ackPath, JSON.stringify({
    version: 8,
    bindings: [binding(0), binding(1)],
    tombstones: [tombstone("expired", now - 10_000), ...Array.from({ length: 63 }, (_, index) => tombstone(index))],
  }));
  const probe = await startDelayedDeliveryAckProbe();
  let bridge;
  let socket;
  try {
    const env = {
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_REPLY_ACK_CONSUMED_RETENTION_MS: "50",
      TELEPATHOS_REPLY_ACK_TOMBSTONE_RETENTION_MS: "5000",
    };
    bridge = await startBridge(env);
    await waitForCondition(() => {
      const snapshot = JSON.parse(readFileSync(ackPath, "utf8"));
      return snapshot.bindings.length === 2 && snapshot.tombstones.length === 63;
    }, "atomic multi-candidate reply-ack sweep");
    const blockedBatch = JSON.parse(readFileSync(ackPath, "utf8"));
    assert.equal(blockedBatch.bindings[0].reply_to, "tp-sweep-live-0");
    assert.equal(blockedBatch.bindings[1].reply_to, "tp-sweep-live-1");
    assert(!blockedBatch.tombstones.some((entry) => entry.reply_to === "tp-sweep-terminal-expired"));

    // Restart with only one candidate. The same expired slot is now reusable,
    // and the resulting tombstone must survive another bridge restart.
    await stopBridge(bridge);
    bridge = null;
    writeFileSync(ackPath, JSON.stringify({
      version: 8,
      bindings: [binding(0)],
      tombstones: [tombstone("expired-again", now - 10_000), ...Array.from({ length: 63 }, (_, index) => tombstone(index))],
    }));
    bridge = await startBridge(env);
    await waitForCondition(() => {
      const snapshot = JSON.parse(readFileSync(ackPath, "utf8"));
      return snapshot.bindings.length === 0 && snapshot.tombstones.length === 64;
    }, "expired tombstone slot reuse");
    const reclaimed = JSON.parse(readFileSync(ackPath, "utf8"));
    assert(reclaimed.tombstones.some((entry) => entry.reply_to === "tp-sweep-live-0"),
      "the consumed candidate must become an exact tombstone");
    assert(!reclaimed.tombstones.some((entry) => entry.reply_to === "tp-sweep-terminal-expired-again"),
      "the expired slot must be pruned before reuse");

    await stopBridge(bridge);
    bridge = await startBridge(env);
    socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    socket.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    await completeHello(socket, null, "late-ack-sweep-owner");
    const receipt = {
      lane_id: "telepathos:direct",
      reply_to: "tp-sweep-live-0",
      after_seq: 30_000,
      through_seq: 30_001,
      turn_token: "turn-sweep-live-0",
      interaction_id: "i-sweep-live-0",
    };
    socket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await waitForEvent(events, 0, (event) => event.type === "reply_acknowledged", "late exact reply acknowledgement");
    assert.equal(probe.acknowledgeRequests(), 0, "a late tombstone ack must not re-consume telepathosd");
    socket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForEvent(events, 0, (event) => event.type === "reply_ack_retired", "exact tombstone retirement");
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).tombstones.length, 63,
      "exact terminal retirement must free the reused tombstone slot");
  } finally {
    socket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

async function testReclaimedConsumedReplyAckTombstoneSurvivesRestart() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-reply-ack-tombstone.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const owner = "late-ack-original-owner";
  const receipt = {
    lane_id: "telepathos:direct",
    reply_to: "tp-late-after-retention",
    after_seq: 90,
    through_seq: 91,
    turn_token: "turn-late-after-retention",
    interaction_id: "i-late-after-retention",
  };
  const now = Date.now();
  writeFileSync(ackPath, JSON.stringify({
    version: 8,
    // This consumed binding is older than consumed retention while the Android
    // owner is offline; its hello-triggered reconciliation must reclaim it.
    bindings: [{
      ...receipt,
      installation_id: owner,
      reply_text: "already consumed before the handset reconnects",
      state: "consumed",
      prepared_at_ms: now - 2_000,
      owner_last_seen_at_ms: now - 1_500,
      received_at_ms: now - 1_200,
      consumed_at_ms: now - 1_000,
    }],
    tombstones: [],
  }));
  const probe = await startDelayedDeliveryAckProbe();
  let bridge;
  let socket;
  let foreignSocket;
  try {
    const env = {
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_REPLY_ACK_CONSUMED_RETENTION_MS: "50",
      TELEPATHOS_REPLY_ACK_TOMBSTONE_RETENTION_MS: "5000",
    };
    bridge = await startBridge(env);
    socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    socket.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    await completeHello(socket, null, owner);
    foreignSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const foreignEvents = [];
    foreignSocket.on("message", (data, isBinary) => {
      if (!isBinary) foreignEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      foreignSocket.once("open", resolve);
      foreignSocket.once("error", reject);
    });
    await completeHello(foreignSocket, null, "wrong-tombstone-owner");
    foreignSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await sleep(100);
    assert(!foreignEvents.some((event) => event.type === "reply_acknowledged"));
    foreignSocket.close();
    socket.send(JSON.stringify({ type: "reply_ack", ...receipt, turn_token: "wrong-turn" }));
    await sleep(100);
    assert(!events.some((event) => event.type === "reply_acknowledged"));
    socket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await waitForEvent(events, 0, (event) => event.type === "reply_acknowledged", "late tombstone acknowledgement");
    assert.equal(probe.acknowledgeRequests(), 0, "late tombstone ack must not re-consume telepathosd");
    socket.close();
    await new Promise((resolve) => socket.once("close", resolve));
    await stopBridge(bridge);
    bridge = await startBridge(env);
    socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const restartedEvents = [];
    socket.on("message", (data, isBinary) => {
      if (!isBinary) restartedEvents.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    await completeHello(socket, null, owner);
    socket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await waitForEvent(restartedEvents, 0, (event) => event.type === "reply_acknowledged", "restarted late tombstone acknowledgement");
    socket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForEvent(restartedEvents, 0, (event) => event.type === "reply_ack_retired", "tombstone retirement");
    assert.deepEqual(JSON.parse(readFileSync(ackPath, "utf8")).tombstones, []);
    assert.equal(probe.acknowledgeRequests(), 0);
  } finally {
    socket?.terminate();
    foreignSocket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testStopCancelsRemoteWait(command = "stop") {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-${command}.json`;
  const probe = await startHermesProbe();
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_HERMES_TIMEOUT: "60000",
    });
    const ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const turnToken = `turn-${command}`;
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    await completeHello(ws);
    const result = await new Promise((resolve, reject) => {
      let stopSent = false;
      let stopAt = 0;
      const timer = setTimeout(() => reject(new Error("stop did not cancel remote wait")), 3000);
      ws.on("message", (data, isBinary) => {
        if (isBinary) return;
        const event = JSON.parse(data.toString());
        if (event.type === "phase" && event.value === "processing" && !stopSent) {
          stopSent = true;
          stopAt = Date.now();
          ws.send(JSON.stringify({ type: "command", command, turn_token: turnToken }));
        }
        if (stopSent && event.type === "listening") {
          clearTimeout(timer);
          resolve({ stopSent, elapsed: Date.now() - stopAt });
        }
      });
      ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: turnToken }));
      const loud = Buffer.alloc(3200);
      for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
      for (let n = 0; n < 10; n++) ws.send(loud);
      const quiet = Buffer.alloc(3200);
      (async () => {
        for (let n = 0; n < 22; n++) {
          ws.send(quiet);
          await sleep(80);
        }
      })().catch(reject);
    });
    assert.equal(result.stopSent, true);
    assert(result.elapsed < 500, `stop took ${result.elapsed}ms`);
    ws.close();
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
  }
}

async function testEarlyStopResetsVad() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-early-stop.json`;
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: "",
    });
    const ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    await completeHello(ws);
    ws.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });

    // 20 ms of loud PCM is below VAD's 80 ms start threshold. Stop while the
    // server is still listening, then send only 60 ms for the next attempt.
    // Without resetting VAD, the two chunks add to 80 ms and create a false
    // speech_start in the second attempt.
    const loud = (bytes) => {
      const pcm = Buffer.alloc(bytes);
      for (let i = 0; i < bytes / 2; i++) pcm.writeInt16LE(8000, i * 2);
      return pcm;
    };
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: "turn-early-a" }));
    ws.send(loud(640));
    await sleep(20);
    ws.send(JSON.stringify({ type: "command", command: "stop", turn_token: "turn-early-a" }));
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: "turn-early-b" }));
    ws.send(loud(1920));
    await sleep(250);

    assert(!events.some((event) => event.type === "speech_start"));
    assert(!events.some((event) => event.type === "phase" && event.value === "capturing"));
    ws.close();
  } finally {
    if (bridge) await stopBridge(bridge);
    try { rmSync(lanesPath); } catch {}
  }
}

async function testDisconnectCancelsRemotePost() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-disconnect.json`;
  const probe = await startHermesProbe({ stateDelayMs: 200 });
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
      TELEPATHOS_HERMES_TIMEOUT: "5000",
    });
    const ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    await completeHello(ws);
    let disconnected = false;
    const closePromise = new Promise((resolve) => ws.once("close", resolve));
    ws.on("message", (data, isBinary) => {
      if (isBinary || disconnected) return;
      const event = JSON.parse(data.toString());
      if (event.type === "phase" && event.value === "processing") {
        disconnected = true;
        ws.close();
      }
    });
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: "turn-disconnect" }));
    const loud = Buffer.alloc(3200);
    for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
    for (let n = 0; n < 10; n++) ws.send(loud);
    const quiet = Buffer.alloc(3200);
    (async () => {
      for (let n = 0; n < 22 && !disconnected; n++) {
        ws.send(quiet);
        await sleep(80);
      }
    })();
    await Promise.race([
      closePromise,
      sleep(3000).then(() => { throw new Error("socket did not close after disconnect test"); }),
    ]);
    await sleep(450);
    assert.equal(probe.requests.length, 0, "closed clients must not post abandoned turns to Hermes");
  } finally {
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
  }
}

async function testTurnTokenRejectsStaleControlAndTagsReplies() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-turn-token.json`;
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: "",
    });
    const ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    await completeHello(ws);
    ws.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    const loud = Buffer.alloc(3200);
    for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);

    // Untagged legacy lane frames are invalid, and their raw PCM must not
    // start an unbound interaction.
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0 }));
    for (let n = 0; n < 4; n++) ws.send(loud);
    await sleep(150);
    assert(!events.some((event) => event.type === "speech_start"), "legacy capture must be ignored");

    const turnToken = "turn-current";
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: turnToken }));
    const firstStart = events.length;
    for (let n = 0; n < 4; n++) ws.send(loud);
    await waitForEvent(events, firstStart,
      (event) => event.type === "phase" && event.value === "capturing", "token-bound capture");

    // A delayed flush for a previous turn cannot end this capture.
    const beforeStaleFlush = events.length;
    ws.send(JSON.stringify({ type: "utterance_end", turn_token: "turn-stale" }));
    await sleep(150);
    assert(!events.slice(beforeStaleFlush).some(
      (event) => event.type === "phase" && event.value === "processing"),
    "stale flush must not end the current capture");

    const responseStart = events.length;
    ws.send(JSON.stringify({ type: "utterance_end", turn_token: turnToken }));
    await waitForEvent(events, responseStart, (event) => event.type === "listening", "first interaction completion");
    const replyFrames = events.slice(responseStart).filter((event) =>
      event.type === "stt" || event.type === "agent_delta" || event.type === "agent_end");
    assert(replyFrames.some((event) => event.type === "stt"));
    assert(replyFrames.some((event) => event.type === "agent_delta"));
    assert(replyFrames.some((event) => event.type === "agent_end"));
    assert(replyFrames.every((event) => event.turn_token === turnToken));
    const interactionId = replyFrames[0].interaction_id;
    assert.equal(typeof interactionId, "string");
    assert(interactionId.length > 0);
    assert(replyFrames.every((event) => event.interaction_id === interactionId));

    // A stale stop cannot cancel a newer capture, while its matching cancel can.
    const newTurnToken = "turn-new";
    ws.send(JSON.stringify({ type: "lane", id: "telepathos:direct", revision: 0, turn_token: newTurnToken }));
    const captureStart = events.length;
    for (let n = 0; n < 4; n++) ws.send(loud);
    await waitForEvent(events, captureStart,
      (event) => event.type === "phase" && event.value === "capturing", "new capture");
    const beforeStaleStop = events.length;
    ws.send(JSON.stringify({ type: "command", command: "stop", turn_token: turnToken }));
    await sleep(150);
    assert(!events.slice(beforeStaleStop).some((event) => event.type === "listening"),
      "stale stop must not cancel a newer capture");
    const cancelStart = events.length;
    ws.send(JSON.stringify({ type: "command", command: "cancel_capture", turn_token: newTurnToken }));
    await waitForEvent(events, cancelStart,
      (event) => event.type === "phase" && event.value === "listening", "matching capture cancellation");

    // Repeat is a fresh reply-only turn and must have its own token and server id.
    const replayStart = events.length;
    ws.send(JSON.stringify({ type: "command", command: "repeat", turn_token: "turn-repeat" }));
    const replayEnd = await waitForEvent(events, replayStart,
      (event) => event.type === "agent_end" && event.turn_token === "turn-repeat", "repeat completion");
    const replayFrames = events.slice(replayStart).filter((event) =>
      event.type === "stt" || event.type === "agent_delta" || event.type === "agent_end");
    assert(!replayFrames.some((event) => event.type === "stt"));
    assert(replayFrames.some((event) => event.type === "agent_delta"));
    assert(replayFrames.every((event) => event.turn_token === "turn-repeat"));
    assert(replayFrames.every((event) => typeof event.interaction_id === "string" && event.interaction_id.length > 0));
    const initialEnd = replyFrames.find((event) => event.type === "agent_end");
    assert(initialEnd, "the initial local reply must have a terminal frame");
    const replayedText = replayFrames
      .filter((event) => event.type === "agent_delta")
      .map((event) => event.text)
      .join("");
    assert.equal(replayEnd.text, initialEnd.text,
      "Repeat must carry the replayed reply in its terminal frame for handset playback");
    assert.equal(replayEnd.text, replayedText,
      "Repeat terminal text must match the streamed replay content");
    ws.close();
  } finally {
    if (bridge) await stopBridge(bridge);
    try { rmSync(lanesPath); } catch {}
  }
}

async function testAuthenticatedHandshake() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-auth.json`;
  const configuredToken = "regression-token";
  const equalLengthInvalidToken = "x".repeat(configuredToken.length);
  const differentLengthInvalidToken = "AUTH-LEAK-different-length";
  assert.equal(equalLengthInvalidToken.length, configuredToken.length);
  assert.notEqual(differentLengthInvalidToken.length, configuredToken.length);
  let bridge;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_TOKEN: configuredToken,
      TELEPATHOS_HERMES_URL: "",
    });

    const apiRequest = (headers = {}) => fetch(`http://127.0.0.1:${apiPort}/api/state`, { headers });
    const validHeader = await apiRequest({ "x-telepathos-token": configuredToken });
    assert.equal(validHeader.status, 200, "the shared-token header authenticates the API");
    const validBearer = await apiRequest({ Authorization: `Bearer ${configuredToken}` });
    assert.equal(validBearer.status, 200, "the Bearer authorization header authenticates the API");
    for (const [label, headers, rawToken] of [
      ["equal-length invalid header", { "x-telepathos-token": equalLengthInvalidToken }, equalLengthInvalidToken],
      ["different-length invalid header", { "x-telepathos-token": differentLengthInvalidToken }, differentLengthInvalidToken],
      ["missing header", {}, null],
      ["invalid Bearer token", { Authorization: `Bearer ${differentLengthInvalidToken}` }, differentLengthInvalidToken],
    ]) {
      const response = await apiRequest(headers);
      const body = await response.text();
      assert.equal(response.status, 401, `${label} is rejected`);
      assert.equal(body.includes(configuredToken), false, `${label} does not echo the configured token`);
      if (rawToken !== null) {
        assert.equal(body.includes(rawToken), false, `${label} does not echo the presented token`);
      }
    }

    const openSocket = () => {
      const ws = new WebSocket(`ws://127.0.0.1:${wsPort}`);
      return new Promise((resolve, reject) => {
        ws.once("open", () => resolve(ws));
        ws.once("error", reject);
      });
    };
    const expectRejectedHello = async (label, token) => {
      const ws = await openSocket();
      const close = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), 2000);
        ws.once("close", (code, reason) => {
          clearTimeout(timer);
          resolve({ code, reason: reason.toString() });
        });
        ws.once("error", reject);
        ws.send(JSON.stringify({
          type: "hello",
          device: "regression",
          installation_id: `rejected-${label.replaceAll(" ", "-")}`,
          ...(token === undefined ? {} : { token }),
        }));
      });
      assert.equal(close.code, 4001, `${label} closes as unauthorized`);
      assert.equal(close.reason.includes(configuredToken), false, `${label} close reason does not contain the configured token`);
      assert.equal((bridgeLogs.get(bridge) ?? "").includes(token ?? "missing-token-sentinel"), false, `${label} token is not logged`);
    };

    await expectRejectedHello("equal-length", equalLengthInvalidToken);
    await expectRejectedHello("different-length", differentLengthInvalidToken);
    await expectRejectedHello("missing", undefined);

    const ws = await openSocket();
    const events = [];
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out opening authenticated socket")), 2000);
      ws.on("message", (data, isBinary) => {
        if (!isBinary) events.push(JSON.parse(data.toString()));
        if (!isBinary && JSON.parse(data.toString()).type === "ready") {
          clearTimeout(timer);
          resolve();
        }
      });
      ws.once("error", reject);
      ws.send(JSON.stringify({
        type: "hello",
        device: "regression",
        installation_id: "authenticated-regression-installation",
        token: configuredToken,
      }));
    });
    assert(events.some((event) => event.type === "ready"));
    ws.close();
  } finally {
    if (bridge) await stopBridge(bridge);
    try { rmSync(lanesPath); } catch {}
  }
}

async function testReplyAckRetryAfterConfirmationHandoff() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-reply-ack.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const receipt = {
    lane_id: "telepathos:direct",
    reply_to: "tp-reply-ack",
    after_seq: 4,
    through_seq: 5,
    turn_token: "turn-reply-ack",
    interaction_id: "bridge-reply-ack",
  };
  writeFileSync(ackPath, JSON.stringify({
    version: 8,
    bindings: [{
      ...receipt,
      installation_id: "server-interactions-installation",
      reply_text: "reply acknowledgement retry",
      state: "received",
      prepared_at_ms: 1_700_000_000_000,
      owner_last_seen_at_ms: 1_700_000_000_100,
      received_at_ms: 1_700_000_000_200,
      consumed_at_ms: null,
    }],
    tombstones: [],
  }));
  const probe = await startDelayedDeliveryAckProbe();
  let bridge;
  let firstSocket;
  let secondSocket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    firstSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const firstConfirmation = new Promise((resolve) => {
      firstSocket.on("message", (data, isBinary) => {
        if (!isBinary && JSON.parse(data.toString()).type === "reply_acknowledged") resolve();
      });
    });
    await new Promise((resolve, reject) => {
      firstSocket.once("open", resolve);
      firstSocket.once("error", reject);
    });
    await completeHello(firstSocket);
    firstSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    await probe.firstAcknowledgement;

    // The bridge completed the WebSocket send handoff, but simulate Android
    // losing that confirmation before it can retire its persisted receipt.
    // A transport callback is not application-level acknowledgement.
    probe.releaseFirst();
    await firstConfirmation;
    const firstClosed = new Promise((resolve) => firstSocket.once("close", resolve));
    firstSocket.terminate();
    await firstClosed;

    // The bridge records external consumption before it tells Android. It
    // must not be deleted merely because the first socket's send callback
    // succeeded: Android may have lost that frame before persisting its
    // terminal retirement retry state.
    const consumedSnapshot = JSON.parse(readFileSync(ackPath, "utf8"));
    assert.equal(consumedSnapshot.bindings.length, 1);
    assert.equal(consumedSnapshot.bindings[0].state, "consumed");

    const events = [];
    secondSocket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    secondSocket.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      secondSocket.once("open", resolve);
      secondSocket.once("error", reject);
    });
    await completeHello(secondSocket);
    const eventStart = events.length;
    secondSocket.send(JSON.stringify({ type: "reply_ack", ...receipt }));
    const confirmation = await waitForEvent(
      events,
      eventStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipt.reply_to,
      "reply acknowledgement after reconnect",
    );
    assert.equal(confirmation.turn_token, receipt.turn_token);
    assert.equal(probe.acknowledgeRequests(), 1,
      "a consumed binding must resend only its bridge confirmation, not repeat telepathosd consumption");
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 1);

    // Android persists a terminal retry record before it sends this frame.
    // The bridge can now durably free the capacity slot, then confirm that
    // deletion. A duplicated terminal frame is idempotent after deletion.
    const retirementStart = events.length;
    secondSocket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForEvent(
      events,
      retirementStart,
      (event) => event.type === "reply_ack_retired" && event.reply_to === receipt.reply_to,
      "terminal reply acknowledgement retirement",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 0);
    const duplicateRetirementStart = events.length;
    secondSocket.send(JSON.stringify({ type: "reply_ack_retire", ...receipt }));
    await waitForEvent(
      events,
      duplicateRetirementStart,
      (event) => event.type === "reply_ack_retired" && event.reply_to === receipt.reply_to,
      "idempotent terminal reply acknowledgement retirement",
    );
  } finally {
    firstSocket?.terminate();
    secondSocket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testReplyAckRetirementReclaimsAllCapacityAcrossBridgeRestart() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const lanesPath = `/tmp/telepathos-server-interactions-${process.pid}-reply-ack-capacity.json`;
  const ackPath = `${lanesPath}.reply-ack-bindings.json`;
  const receipts = Array.from({ length: 64 }, (_, index) => ({
    lane_id: "telepathos:direct",
    reply_to: `tp-capacity-${index}`,
    after_seq: index * 2,
    through_seq: index * 2 + 1,
    turn_token: `turn-capacity-${index}`,
    interaction_id: `interaction-capacity-${index}`,
  }));
  const binding = (receipt) => ({
    ...receipt,
    installation_id: "server-interactions-installation",
    reply_text: `reply ${receipt.reply_to}`,
    state: "received",
    prepared_at_ms: 1_700_000_000_000,
    owner_last_seen_at_ms: 1_700_000_000_100,
    received_at_ms: 1_700_000_000_200,
    consumed_at_ms: null,
  });
  writeFileSync(ackPath, JSON.stringify({ version: 8, bindings: receipts.map(binding), tombstones: [] }));
  const probe = await startDelayedDeliveryAckProbe();
  let interactionProbe;
  let bridge;
  let socket;
  let events = [];
  const openSocket = async () => {
    const events = [];
    const next = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    next.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      next.once("open", resolve);
      next.once("error", reject);
    });
    await completeHello(next);
    return { socket: next, events };
  };
  const acknowledgeAndRetire = async (client, receipt) => {
    const frame = {
      lane_id: receipt.lane_id,
      reply_to: receipt.reply_to,
      after_seq: receipt.after_seq,
      through_seq: receipt.through_seq,
      turn_token: receipt.turn_token,
      interaction_id: receipt.interaction_id,
    };
    const receiptStart = client.events.length;
    client.socket.send(JSON.stringify({ type: "reply_received", ...frame }));
    await waitForEvent(
      client.events,
      receiptStart,
      (event) => event.type === "reply_received" && event.reply_to === receipt.reply_to,
      `durable handset receipt for ${receipt.reply_to}`,
    );
    const acknowledgementStart = client.events.length;
    client.socket.send(JSON.stringify({ type: "reply_ack", ...frame }));
    await waitForEvent(
      client.events,
      acknowledgementStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipt.reply_to,
      `reply acknowledgement for ${receipt.reply_to}`,
    );
    const retirementStart = client.events.length;
    client.socket.send(JSON.stringify({ type: "reply_ack_retire", ...frame }));
    await waitForEvent(
      client.events,
      retirementStart,
      (event) => event.type === "reply_ack_retired" && event.reply_to === receipt.reply_to,
      `reply acknowledgement retirement for ${receipt.reply_to}`,
    );
  };
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    ({ socket, events } = await openSocket());

    // Delay the first external consume long enough to prove that a bridge
    // restart sees the persisted `consumed` phase and resends only its
    // confirmation, not a second external consume.
    const firstAcknowledgementStart = events.length;
    socket.send(JSON.stringify({ type: "reply_ack", ...receipts[0] }));
    await probe.firstAcknowledgement;
    probe.releaseFirst();
    await waitForEvent(
      events,
      firstAcknowledgementStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipts[0].reply_to,
      "first capacity acknowledgement",
    );
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings[0].state, "consumed");
    socket.terminate();
    await stopBridge(bridge);
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    ({ socket, events } = await openSocket());
    const replayStart = events.length;
    socket.send(JSON.stringify({ type: "reply_ack", ...receipts[0] }));
    await waitForEvent(
      events,
      replayStart,
      (event) => event.type === "reply_acknowledged" && event.reply_to === receipts[0].reply_to,
      "consumed acknowledgement after bridge restart",
    );
    assert.equal(probe.acknowledgeRequests(), 1,
      "bridge restart must not repeat a durably consumed delivery");

    // Simulate a crash after the durable removal but before Android receives
    // reply_ack_retired. The persisted Android terminal retry is safe to
    // repeat against a restarted bridge with no matching binding left.
    socket.send(JSON.stringify({ type: "reply_ack_retire", ...receipts[0] }));
    await waitForCondition(
      () => JSON.parse(readFileSync(ackPath, "utf8")).bindings.length === 63,
      "first durable reply-ack removal",
    );
    socket.terminate();
    await stopBridge(bridge);
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    ({ socket, events } = await openSocket());
    const terminalReplayStart = events.length;
    socket.send(JSON.stringify({ type: "reply_ack_retire", ...receipts[0] }));
    await waitForEvent(
      events,
      terminalReplayStart,
      (event) => event.type === "reply_ack_retired" && event.reply_to === receipts[0].reply_to,
      "terminal retirement after bridge restart",
    );

    for (const receipt of receipts.slice(1)) {
      await acknowledgeAndRetire({ socket, events }, receipt);
    }
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 0,
      "64 successful retirement handshakes must reclaim every durable slot");

    // The 65th completed reply would have been blocked before this fix. Run a
    // real remote turn after the 64 retirements, rather than injecting another
    // snapshot entry, so the bridge's cap check and `prepareReplyAck` path are
    // covered too.
    socket.terminate();
    await stopBridge(bridge);
    interactionProbe = await startInteractionLedgerProbe();
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: interactionProbe.url,
    });
    const sixtyFifthEvents = await sendUtterance(wsPort, { turnToken: "turn-capacity-64" });
    const sixtyFifthEnd = sixtyFifthEvents.find((event) =>
      event.type === "agent_end" && event.reply_to === "tp-1");
    assert(sixtyFifthEnd,
      "a remote reply after 64 retirements must not be paused by reply-ack capacity");
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 1);
    ({ socket, events } = await openSocket());
    await acknowledgeAndRetire({ socket, events }, sixtyFifthEnd);
    assert.equal(JSON.parse(readFileSync(ackPath, "utf8")).bindings.length, 0);
    assert.equal(probe.acknowledgeRequests(), 64);
  } finally {
    socket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    if (interactionProbe) await new Promise((resolve) => interactionProbe.server.close(resolve));
    try { rmSync(lanesPath); } catch {}
    try { rmSync(ackPath); } catch {}
  }
}

async function testPausedRemoteReplyCannotBeReplayedWithoutReceipt() {
  const wsPort = await freePort();
  const apiPort = await freePort();
  const directory = mkdtempSync(join(tmpdir(), "telepathos-paused-remote-reply-"));
  const lanesPath = join(directory, "lanes.json");
  let releaseReply;
  let markReplyRequested;
  const replyRequested = new Promise((resolve) => { markReplyRequested = resolve; });
  const replyReleased = new Promise((resolve) => { releaseReply = resolve; });
  const probe = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => { body += chunk; });
    req.on("end", async () => {
      if (req.method === "GET" && req.url === "/api/state") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          lanes: [{ id: "telepathos:direct", name: "direct", created_at: "2020-01-01", last_active: "2020-01-01" }],
          active_id: "telepathos:direct",
          previous_id: "telepathos:direct",
          revision: 0,
        }));
        return;
      }
      if (req.method === "POST" && req.url === "/api/lanes/interaction") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      if (req.method === "POST" && req.url === "/api/meta") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ reply: "receipt-less reply A" }));
        return;
      }
      if (req.method === "POST" && req.url === "/api/message") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, message_id: "tp-paused-remote-reply" }));
        return;
      }
      if (req.method === "GET" && req.url === "/api/delivery/head") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ latest: 0 }));
        return;
      }
      if (req.method === "GET" && req.url?.startsWith("/api/delivery?")) {
        const replyTo = new URL(req.url, "http://127.0.0.1").searchParams.get("reply_to");
        if (replyTo === "tp-paused-remote-reply") {
          markReplyRequested();
          await replyReleased;
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({
            latest: 1,
            deliveries: [{
              seq: 1,
              chat_id: "telepathos:direct",
              content: "reply whose receipt cannot be persisted",
              reply_to: replyTo,
            }],
          }));
          return;
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ latest: 0, deliveries: [] }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, body }));
    });
  });
  await new Promise((resolve) => probe.listen(0, "127.0.0.1", resolve));
  let bridge;
  let socket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: `http://127.0.0.1:${probe.address().port}`,
    });
    socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    socket.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    await completeHello(socket);

    // Seed the normal, receipt-less replay cache. The following correlated
    // remote reply must evict it even if receipt persistence later fails.
    const localTurn = "turn-local-reply";
    const localStart = events.length;
    socket.send(JSON.stringify({ type: "meta_mode", turn_token: localTurn }));
    socket.send(JSON.stringify({
      type: "lane",
      id: "telepathos:direct",
      revision: 0,
      turn_token: localTurn,
    }));
    const loud = Buffer.alloc(3200);
    for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
    for (let index = 0; index < 10; index++) socket.send(loud);
    const quiet = Buffer.alloc(3200);
    for (let index = 0; index < 22; index++) {
      socket.send(quiet);
      await sleep(80);
    }
    const localEnd = await waitForEvent(
      events,
      localStart,
      (event) => event.type === "agent_end" && event.turn_token === localTurn,
      "receipt-less local reply",
    );
    assert.equal(localEnd.text, "receipt-less reply A");

    const remoteTurn = "turn-paused-remote-reply";
    const remoteStart = events.length;
    socket.send(JSON.stringify({
      type: "lane",
      id: "telepathos:direct",
      revision: 0,
      turn_token: remoteTurn,
    }));
    for (let index = 0; index < 10; index++) socket.send(loud);
    for (let index = 0; index < 22; index++) {
      socket.send(quiet);
      await sleep(80);
    }
    await replyRequested;
    // The capture and remote response have both succeeded. Fail only the
    // durable receipt write performed by finish(), which is the boundary this
    // regression protects.
    chmodSync(directory, 0o500);
    releaseReply();
    const paused = await waitForEvent(
      events,
      remoteStart,
      (event) => event.type === "error" && event.message.startsWith("remote replies paused:"),
      "remote reply pause after receipt persistence failure",
    );
    assert.equal(paused.message, "remote replies paused: request failed");
    assert(!paused.message.includes(directory),
      "reply persistence paths must never appear in handset frames");
    await waitForEvent(
      events,
      remoteStart,
      (event) => event.type === "listening",
      "paused remote reply cleanup",
    );
    assert(!events.slice(remoteStart).some((event) => event.type === "agent_end"),
      "a paused remote reply must not emit an untracked agent_end");

    const repeatStart = events.length;
    socket.send(JSON.stringify({ type: "command", command: "repeat", turn_token: "turn-repeat-paused-remote" }));
    const replayError = await waitForEvent(
      events,
      repeatStart,
      (event) => event.type === "error" && event.message === "nothing to replay yet",
      "repeat rejection after paused remote reply",
    );
    assert.equal(replayError.message, "nothing to replay yet");
    await sleep(100);
    assert(!events.slice(repeatStart).some((event) =>
      event.type === "agent_delta" || event.type === "agent_end"),
    "repeat must not fabricate an untracked replay for a paused remote reply");
  } finally {
    releaseReply?.();
    try { chmodSync(directory, 0o700); } catch {}
    socket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

async function testExpiredInteractionPersistenceFailurePausesAndRetries() {
  const directory = mkdtempSync(join(tmpdir(), "telepathos-expired-interaction-"));
  const lanesPath = join(directory, "lanes.json");
  const outboxPath = `${lanesPath}.interaction-outbox.json`;
  const wsPort = await freePort();
  const apiPort = await freePort();
  const record = {
    lane_id: "telepathos:direct",
    interaction_id: "expired-interaction",
    interaction_created_at_ms: 1_700_000_000_000,
    state: "pending",
  };
  writeFileSync(outboxPath, JSON.stringify({ version: 3, records: [record] }));
  const probe = await startExpiredInteractionProbe();
  let bridge;
  let socket;
  try {
    bridge = await startBridge({
      TELEPATHOS_PORT: String(wsPort),
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_PORT: String(apiPort),
      TELEPATHOS_LANES: lanesPath,
      TELEPATHOS_STT: "echo",
      TELEPATHOS_HERMES_URL: probe.url,
    });
    await probe.firstInteraction;
    // Force the terminal-state write to fail after telepathosd reports its
    // bounded dedupe horizon has expired.
    chmodSync(directory, 0o500);
    probe.releaseFirst();
    await waitForBridgeOutput(
      bridge,
      (output) => output.includes("remote interaction outbox persistence failed"),
      "expiration persistence failure",
    );

    socket = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const events = [];
    socket.on("message", (data, isBinary) => {
      if (!isBinary) events.push(JSON.parse(data.toString()));
    });
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    await completeHello(socket);
    const eventStart = events.length;
    socket.send(JSON.stringify({
      type: "lane",
      id: "telepathos:direct",
      revision: 0,
      turn_token: "turn-expired-recovery",
    }));
    const blocked = await waitForEvent(
      events,
      eventStart,
      (event) => event.type === "error" && event.message.includes("outbox persistence failed"),
      "fail-closed response after expiration persistence failure",
    );
    assert.match(blocked.message, /^remote turns paused:/);

    // Once the filesystem is writable again, the scheduled retry must persist
    // the terminal record instead of leaving it pending or throwing.
    chmodSync(directory, 0o700);
    await waitForCondition(() => {
      const snapshot = JSON.parse(readFileSync(outboxPath, "utf8"));
      return probe.interactionRequests() >= 2 && snapshot.records[0]?.state === "expired";
    }, "expired interaction outbox retry");
  } finally {
    socket?.terminate();
    if (bridge) await stopBridge(bridge);
    await new Promise((resolve) => probe.server.close(resolve));
    try { chmodSync(directory, 0o700); } catch {}
    rmSync(directory, { recursive: true, force: true });
  }
}

await testSttFailureFailsClosed();
await testVoiceInteractionPersistsLaneActivity();
await testStandaloneLaneSaveFailureRecoversFsm();
await testRemoteInteractionIdsRemainUniqueAcrossReconnects();
await testRemoteRejectsUnsnapshottedLane();
await testCapturePreparationDeadlineFencesValidationAndAudio();
await testReservationCleanupRunsBeforeCapacityPreflight();
await testLostAgentEndReplaysBeforePendingNarrationAndSurvivesRestart();
await testReplyReceiptsRequireInstallationOwner();
await testAbandonedReplyAckOwnerReconcilesToRotatedInstallation();
await testReplyAckOwnerClockRollbackPreservesHighWaterMarkAcrossRestart();
await testExpiredConsumedReplyAcksReclaimCapacity();
await testReplyAckTombstoneCapacityFailsClosed();
await testReplyAckTombstoneSweepIsAtomicAndRestartSafe();
await testReclaimedConsumedReplyAckTombstoneSurvivesRestart();
await testStopCancelsRemoteWait();
await testStopCancelsRemoteWait("cancel_capture");
await testEarlyStopResetsVad();
await testDisconnectCancelsRemotePost();
await testTurnTokenRejectsStaleControlAndTagsReplies();
await testMetaModelProposalPreservesConcurrentApiMutation();
await testMetaModelProposalPreservesAbaSelection();
await testAuthenticatedHandshake();
await testReplyAckRetryAfterConfirmationHandoff();
await testReplyAckRetirementReclaimsAllCapacityAcrossBridgeRestart();
await testPausedRemoteReplyCannotBeReplayedWithoutReceipt();
await testExpiredInteractionPersistenceFailurePausesAndRetries();
console.log("SERVER INTERACTION TESTS PASS");
