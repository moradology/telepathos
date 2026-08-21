// Robustness tests. Run: node test-resilience.mjs
import WebSocket from "ws";

import { results, check } from "./check.mjs";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function test1_normalFlow() {
  const ws = new WebSocket("ws://localhost:8787");
  const events = [];
  await new Promise((res, rej) => { ws.on("open", res); ws.on("error", rej); });
  ws.on("message", (data) => {
    const m = JSON.parse(data.toString());
    events.push(m.type);
    if (m.type === "agent_end") ws.close();
  });
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud);
  const quiet = Buffer.alloc(3200);
  for (let n = 0; n < 25; n++) { ws.send(quiet); await sleep(80); }
  await sleep(4000);
  check("normal flow: full event sequence", events.includes("ready") && events.includes("speech_start")
    && events.includes("utterance") && events.includes("stt") && events.includes("agent_delta")
    && events.includes("agent_end"));
  check("normal flow: reply delivered as text (phone speaks it)", events.includes("agent_delta"));
}

async function test2_malformedFrames() {
  const ws = new WebSocket("ws://localhost:8787");
  await new Promise((res) => ws.on("open", res));
  let survived = true;
  ws.on("close", () => {});
  ws.send("this is not json{{{");
  ws.send('{"type":');
  ws.send('{"type":"unknown_weird"}');
  ws.send('{"type":"stt"}'); // stt without text field
  await sleep(500);
  // socket should still work: do a real utterance
  const events = [];
  ws.on("message", (d, bin) => { if (!bin) events.push(JSON.parse(d.toString()).type); });
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud);
  const quiet = Buffer.alloc(3200);
  for (let n = 0; n < 25; n++) { ws.send(quiet); await sleep(80); }
  await sleep(3000);
  check("malformed frames don't kill connection or processing", events.includes("agent_end"), events.join(","));
  ws.close();
}

async function test3_serverRestartMidStream() {
  // client behavior contract: after server dies mid-connection, a NEW client can connect.
  // (the phone app's own reconnect loop is exercised by backoff logic; here we verify server recovery)
  const ws = new WebSocket("ws://localhost:8787");
  await new Promise((res) => ws.on("open", res));
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE(9000, i * 2);
  ws.send(loud); // start an utterance...
  ws.terminate(); // ...then vanish abruptly (no close frame)
  await sleep(300);
  // reconnect and confirm the server handles it cleanly
  const ws2 = new WebSocket("ws://localhost:8787");
  let reconnected = false;
  const t = setTimeout(() => {}, 5000);
  await new Promise((res) => {
    ws2.on("open", () => { reconnected = true; clearTimeout(t); res(); });
    ws2.on("error", () => res());
  });
  check("abrupt client termination → clean reconnect", reconnected);
  if (reconnected) ws2.close();
}

for (const t of [test1_normalFlow, test2_malformedFrames, test3_serverRestartMidStream,
                  test4_preRollIntegrity, test5_forcedCapRecovery, test7_flush]) {
  try { await t(); } catch (e) { check(t.name, false, String(e)); }
}
console.log(results.join("\n"));
process.exit(results.some((r) => r.startsWith("FAIL")) ? 1 : 0);

// ---- pass 3 additions ----
async function test4_preRollIntegrity() {
  // First word must not be clipped: server keeps ~320ms pre-roll.
  const ws = new WebSocket("ws://localhost:8787");
  await new Promise((res) => ws.on("open", res));
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  const quiet = Buffer.alloc(3200);
  let utteranceSamples = null;
  const kill = setTimeout(() => {}, 10000);
  ws.on("message", (d, bin) => {
    if (!bin) {
      const m = JSON.parse(d.toString());
      if (m.type === "utterance") utteranceSamples = m.samples;
      if (m.type === "agent_end") { clearTimeout(kill); ws.close(); }
    }
  });
  // 2 chunks quiet, then speech without pause — pre-roll should include the 2 quiet + ramp chunks
  for (let n = 0; n < 2; n++) { ws.send(quiet); await sleep(30); }
  for (let n = 0; n < 10; n++) { ws.send(loud); await sleep(10); }
  for (let n = 0; n < 25; n++) { ws.send(quiet); await sleep(80); }
  await sleep(5000);
  // expect ≥ 12 chunks*1600 samples (2 preroll + 10 loud) rather than just ~10
  check("pre-roll: utterance includes pre-speech audio", utteranceSamples >= 19000, `${utteranceSamples} samples`);
}

async function test5_forcedCapRecovery() {
  // 60s of continuous loud audio → forced end → immediately speak again → must work
  const ws = new WebSocket("ws://localhost:8787");
  await new Promise((res) => ws.on("open", res));
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  const events = [];
  let phase = 1;
  ws.on("message", (d, bin) => {
    if (bin) return;
    const m = JSON.parse(d.toString());
    if (m.type === "listening" && phase === 1) {
      phase = 2;
      // mic is live again — speak again immediately
      (async () => {
        for (let n = 0; n < 6; n++) { ws.send(loud); await sleep(10); }
        const q = Buffer.alloc(3200);
        for (let n = 0; n < 25; n++) { ws.send(q); await sleep(60); }
      })();
    }
    if (m.type === "agent_end") events.push("end");
    if (events.length === 2) { clearTimeout(t); ws.close(); }
  });
  for (let n = 0; n < 620; n++) ws.send(loud); // ~62s > cap, no silence
  const t = setTimeout(() => {}, 30000);
  await sleep(20000);
  check("forced cap: next utterance still works after cap", events.length === 2, `ends=${events.length}`);
}


// ---- pass 4 addition: explicit flush (utterance_end) ----
async function test7_flush() {
  const ws = new WebSocket("ws://localhost:8787");
  const events = [];
  await new Promise((res) => ws.on("open", res));
  ws.on("message", (d, bin) => {
    if (bin) return;
    const m = JSON.parse(d.toString());
    events.push(m.type);
    if (m.type === "agent_end") { clearTimeout(kill); ws.close(); }
  });
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud);   // speech...
  ws.send('{"type":"utterance_end"}');           // ...then tap-to-send, NO silence wait
  const kill = setTimeout(() => {}, 5000);
  await sleep(4000);
  check("flush: utterance_end ends capture without VAD silence",
    events.includes("utterance") && events.includes("stt") && events.includes("agent_end"),
    events.join(","));
}
