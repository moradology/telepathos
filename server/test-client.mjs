// Smoke test: simulates a phone client. Run: node test-client.mjs
import WebSocket from "ws";

const ws = new WebSocket("ws://localhost:8787");
const events = [];
let ttsBytes = 0;

ws.on("open", () => {
  ws.send(JSON.stringify({
    type: "hello",
    device: "test",
    installation_id: "test-client-installation",
  }));
  const loud = Buffer.alloc(3200);
  for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  for (let n = 0; n < 10; n++) ws.send(loud); // 1s loud "speech"
  const quiet = Buffer.alloc(3200);
  let count = 0;
  const iv = setInterval(() => {
    ws.send(quiet); // 2s silence → VAD end
    if (++count >= 25) clearInterval(iv);
  }, 80);
});

ws.on("message", (data, isBinary) => {
  if (isBinary) { ttsBytes += data.length; return; }
  const m = JSON.parse(data.toString());
  events.push(m.type + (m.text ? ": " + m.text.slice(0, 60) : ""));
  if (m.type === "agent_end") {
    setTimeout(() => {
      console.log(events.join("\n"));
      console.log("tts binary bytes:", ttsBytes);
      process.exit(0);
    }, 3000);
  }
});

setTimeout(() => { console.log("TIMEOUT\n" + events.join("\n")); process.exit(1); }, 15000);
