// Simulates a Hermes gateway: dials /relay, handshakes, pushes a send action.
// Validates the connector end-to-end without a real Hermes install.
import WebSocket from "ws";
import fs from "node:fs";

const PORT = process.env.TELEPATHOS_PORT ?? "8802";
let failures = 0;
const check = (name, ok, detail = "") => {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
  if (!ok) failures++;
};

// fresh state
for (const f of ["/tmp/tp-lanes.json", "/tmp/tp-pending.json"]) {
  try { fs.rmSync(f); } catch {}
}

const ws = new WebSocket(`ws://127.0.0.1:${PORT}/relay`);
const ndjson = (obj) => ws.send(JSON.stringify(obj) + "\n");

let descriptor = null;
const results = [];

ws.on("open", () => {
  ndjson({ type: "hello", platform: "relay", botId: "" });
});

ws.on("message", (data) => {
  const frame = JSON.parse(data.toString());
  switch (frame.type) {
    case "descriptor":
      descriptor = frame.descriptor;
      break;
    case "outbound_result":
      results.push(frame);
      break;
  }
});

await new Promise((res, rej) => { ws.on("open", res); ws.on("error", rej); });

// wait for descriptor
for (let i = 0; i < 30 && !descriptor; i++) await new Promise(r => setTimeout(r, 100));
check("handshake: descriptor received", !!descriptor,
  descriptor ? `platform=${descriptor.platform}` : "none");

// push a user utterance INTO hermes (inbound direction is us → gateway; here
// we instead act AS the gateway sending a reply to prove outbound works)
ndjson({
  type: "outbound",
  requestId: "r1",
  action: { op: "send", chat_id: "telepathos:direct", content: "Nightly tests: 142 passed." },
});
for (let i = 0; i < 30 && results.length === 0; i++) await new Promise(r => setTimeout(r, 100));
check("outbound_result received", results.length === 1, JSON.stringify(results[0] ?? {}));
check("result reports success", results[0]?.result?.success === true);

// second reply to build up pending state
ndjson({
  type: "outbound",
  requestId: "r2",
  action: { op: "send", chat_id: "telepathos:direct", content: "Deploy finished." },
});
await new Promise((r) => setTimeout(r, 500));

ws.close();
console.log(failures === 0 ? "GATEWAY-SIM PASS" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);
