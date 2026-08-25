import WebSocket from "ws";
const ws = new WebSocket("ws://localhost:8787");
const TOKEN = "debug-turn-1";
ws.on("message", (d) => {
  const m = JSON.parse(d.toString());
  console.log("<<", m.type + (m.text ? `:${m.text.slice(0, 40)}` : ""));
  if (m.type === "ready") {
    ws.send(JSON.stringify({ type: "lane", id: "telepathy:direct", turn_token: TOKEN }));
  }
  if (m.type === "lane_ack" || m.type === "phase") {
    if (!global.loudSent) {
      global.loudSent = true;
      const loud = Buffer.alloc(3200);
      for (let i = 0; i < 1600; i++) loud.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
      for (let n = 0; n < 10; n++) ws.send(loud);
      const quiet = Buffer.alloc(3200);
      let count = 0;
      const iv = setInterval(() => {
        ws.send(quiet);
        if (++count >= 25) { clearInterval(iv); ws.send(JSON.stringify({ type: "utterance_end", turn_token: TOKEN })); }
      }, 60);
    }
  }
});
setTimeout(() => process.exit(0), 10000);
