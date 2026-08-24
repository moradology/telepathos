// Test 6: backpressure + severance via chaos proxy.
// Client speaks through :8888 (throttled). Server's text reply must still arrive
// despite the retry path being forced, and the server must survive the severing.
import WebSocket from "ws";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function test6_backpressureUnderChaos() {
  const ws = new WebSocket("ws://localhost:8888"); // through the chaos proxy
  await new Promise((res) => ws.on("open", res));

  let agentText = "";
  let sawAgentEnd = false;

  ws.on("message", (data, isBinary) => {
    if (isBinary) return;
    const message = JSON.parse(data.toString());
    if (message.type === "agent_delta") agentText += message.text ?? "";
    if (message.type === "agent_end") sawAgentEnd = true;
  });

  // speak
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud);
  const quiet = Buffer.alloc(3200);
  for (let n = 0; n < 25; n++) { ws.send(quiet); await sleep(80); }

  // The proxy severs at ~8s: after one full throttled interaction. This forces the
  // server's backpressure retry path (throttled text frames),
  // then kills the socket mid-idle. Assert the full reply made it through.
  await sleep(12000);
  check("chaos: text reply streamed through throttle", sawAgentEnd && agentText.length > 0,
    `end=${sawAgentEnd} text=${JSON.stringify(agentText)}`);

  // fresh client through the same proxy — server must be healthy
  const ws2 = new WebSocket("ws://localhost:8888");
  let freshOk = false;
  await new Promise((res) => ws2.on("open", res));
  ws2.on("message", (d, bin) => {
    if (!bin && JSON.parse(d.toString()).type === "stt") freshOk = true;
  });
  for (let n = 0; n < 10; n++) ws2.send(loud);
  for (let n = 0; n < 25; n++) { ws2.send(quiet); await sleep(80); }
  await sleep(10000);
  check("chaos: server healthy after severed connections", freshOk);
  try { ws.close(); ws2.close(); } catch {}
}
import { results, check } from "./check.mjs";
await test6_backpressureUnderChaos();
console.log(results.join("\n"));
process.exit(results.some((r) => r.startsWith("FAIL")) ? 1 : 0);
