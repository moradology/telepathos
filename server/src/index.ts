import { WebSocketServer, WebSocket } from "ws";
import { config } from "./config.js";
import { EnergyVad } from "./vad.js";
import { transcribe, Transcript } from "./transcriber.js";
import { parseControl, assertNever } from "./protocol.js";
import { InteractionState, InteractionEvent, transition, micOpen } from "./fsm.js";
import { loadLanes, saveLanes, activeLane, switchLane, createLane, LaneRegistry } from "./lanes.js";
import { parseMeta, MetaAction } from "./meta.js";
import { runMetaAgent } from "./meta-agent.js";
import { startApiServer } from "./api.js";

/**
 * telepathy bridge — v0.1 stub brain.
 * Protocol: see ../README.md. The agent step is a placeholder ("echo");
 * Hermes/pi plugs in at `respond()`.
 *
 * Robustness:
 * - utterance buffers are capped (open mics + low VAD thresholds must not OOM us)
 * - TTS frames are written with backpressure awareness
 * - optional shared-token auth via TELEPATHY_TOKEN (client puts it in hello)
 */

const MAX_UTTERANCE_BYTES = 16000 * 2 * 60; // 60 s of 16 kHz PCM16
const PREROLL_BYTES = 16000 * 2 * 0.32;     // ~320 ms of pre-speech audio

interface ClientState {
  vad: EnergyVad;
  utterance: Buffer[];
  utteranceBytes: number;
  preroll: Buffer[];      // rolling window kept so utterance starts aren't clipped
  prerollBytes: number;
  fsm: InteractionState;  // single source of truth for interaction lifecycle
  authenticated: boolean;
  cancelRequested: boolean; // double-tap stop (features.md M3)
  lastReply: string | null; // for triple-tap replay
  metaMode: boolean;        // double-pinch: next utterance goes to the meta agent
}

const lanes: LaneRegistry = loadLanes();
startApiServer(lanes, config.apiPort, config.apiHost);

const wss = new WebSocketServer({ port: config.port, host: "0.0.0.0", maxPayload: 1 << 20 });

wss.on("connection", (ws) => {
  const state: ClientState = {
    vad: new EnergyVad(config.vadThreshold, config.vadSilenceMs, config.vadMinSpeechMs),
    utterance: [],
    utteranceBytes: 0,
    preroll: [],
    prerollBytes: 0,
    fsm: { phase: "listening" },
    authenticated: !process.env.TELEPATHY_TOKEN, // no token configured → open
    cancelRequested: false,
    lastReply: null,
    metaMode: false,
  };
  console.log("client connected");
  // no token configured → open; say hello immediately per protocol
  if (!process.env.TELEPATHY_TOKEN) send(ws, { type: "ready" });
  ws.on("message", (data, isBinary) => {
    if (!state.authenticated) return; // ignore everything until hello authenticates
    if (isBinary) return onAudio(ws, state, data as Buffer);
    onControl(ws, state, (data as Buffer).toString());
  });
  ws.on("close", () => console.log("client disconnected"));
  ws.on("error", (e) => console.error("ws error:", e.message));
});

wss.on("error", (e: NodeJS.ErrnoException) => {
  if (e.code === "EADDRINUSE") {
    console.error(`port ${config.port} already in use — is another bridge running?`);
  } else {
    console.error("server error:", e.message);
  }
  process.exit(1);
});

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
  const pcm = Buffer.concat(state.utterance);
  state.utterance = [];
  state.utteranceBytes = 0;
  const samples = pcm.length >> 1;
  send(ws, { type: "utterance", samples });
  state.cancelRequested = false;

  try {
    // PCM16@16k → WAV container for STT
    const wav = wrapWav(pcm, 16000);
    const t0 = Date.now();
    let transcript: Transcript | null;
    try {
      transcript = await transcribe(wav);
    } catch (e) {
      // STT failure must not kill the interaction — degrade to a spoken error
      send(ws, { type: "error", message: `stt: ${(e as Error).message}` });
      transcript = null;
    }
    const text = transcript?.text ?? "(transcription unavailable)";
    send(ws, {
      type: "stt",
      text,
      ...(transcript?.confidence !== undefined && { confidence: transcript.confidence }),
      ...(process.env.TELEPATHY_REPO && { repo: process.env.TELEPATHY_REPO }),
    });
    console.log(`stt (${Date.now() - t0}ms):`, text,
      transcript?.confidence !== undefined ? `[conf ${transcript.confidence}]` : "");

    if (state.cancelRequested) return finish(ws, state);

    // ---- meta agent plane: double-pinch or codeword routes here, never to Hermes ----
    const codeword = text.match(/^(meta|telepathy)[,: ]+(.*)$/i);
    if (state.metaMode || codeword) {
      const stripped = codeword ? codeword[2] : text;
      const action = parseMeta(stripped, lanes);
      let reply: string;
      if (action.op === "unknown" && config.metaModel) {
        // grammar miss → steering agent (LLM with lane tools)
        reply = await runMetaAgent(
          { baseUrl: config.metaBaseUrl, apiKey: process.env.OPENAI_API_KEY ?? "", model: config.metaModel },
          lanes, stripped,
        );
      } else {
        reply = executeMeta(action);
      }
      saveLanes(lanes);
      for await (const delta of chunks(reply)) {
        if (state.cancelRequested) break;
        send(ws, { type: "agent_delta", text: delta });
      }
      finish(ws, state, reply);
    } else {
      const reply = await respond(text, state);
      for await (const delta of chunks(reply)) {
        if (state.cancelRequested) break;
        send(ws, { type: "agent_delta", text: delta });
      }
      finish(ws, state, reply);
    }
  } catch (e) {
    send(ws, { type: "error", message: String((e as Error).message ?? e) });
  } finally {
    const lane = lanes.lanes.find((l) => l.id === lanes.activeId);
    if (lane) lane.interactions = (lane.interactions ?? 0) + 1;
    state.metaMode = false; // one-shot plane
    // whatever happened, land back on listening so the mic can reopen on next pinch
    if (!micOpen(state.fsm)) step(ws, state, { kind: "CANCEL" });
    send(ws, { type: "listening" });
  }
}

function finish(ws: WebSocket, state: ClientState, reply?: string) {
  if (reply !== undefined) state.lastReply = reply;
  if (state.cancelRequested) console.log("interaction cancelled by user");
  send(ws, { type: "agent_end" });
}

function onControl(ws: WebSocket, state: ClientState, raw: string) {
  const msg = parseControl(raw);
  if (msg === null) return; // garbage or unknown — ignore

  switch (msg.tag) {
    case "hello": {
      if (state.authenticated) return; // already handshaked
      if (process.env.TELEPATHY_TOKEN && msg.token !== process.env.TELEPATHY_TOKEN) {
        console.warn("auth failed — closing");
        ws.close(4001, "unauthorized");
        return;
      }
      state.authenticated = true;
      console.log("hello from", msg.device);
      send(ws, { type: "ready" });
      break;
    }
    case "command": {
      console.log("command:", msg.kind);
      // exhaustive over the command union — a new kind fails compilation here
      switch (msg.kind) {
        case "stop":
          if (!micOpen(state.fsm)) state.cancelRequested = true;
          break;
        case "repeat":
          if (micOpen(state.fsm) && state.lastReply) void replayLast(ws, state);
          else if (micOpen(state.fsm)) send(ws, { type: "error", message: "nothing to replay yet" });
          break;
        case "cancel_capture":
          if (state.fsm.phase === "capturing") {
            state.utterance = [];
            state.utteranceBytes = 0;
            state.vad.reset();
            step(ws, state, { kind: "CANCEL" });
          }
          break;
        default:
          assertNever(msg.kind);
      }
      break;
    }
    case "utterance_end": {
      // explicit "send now" (tap while capturing) — beats waiting for VAD silence
      if (state.fsm.phase === "capturing") {
        state.vad.reset();
        step(ws, state, { kind: "UTTERANCE_END" });
        void handleUtterance(ws, state);
      }
      break;
    }
    case "meta_mode": {
      // double-pinch: arm the meta plane for the next utterance
      state.metaMode = true;
      console.log("meta mode armed");
      break;
    }
    default:
      assertNever(msg);
  }
}

/** Triple-tap: re-send the last reply as text; the phone speaks it again. */
async function replayLast(ws: WebSocket, state: ClientState) {
  const reply = state.lastReply!;
  state.cancelRequested = false;
  try {
    for await (const delta of chunks(reply)) {
      if (state.cancelRequested) break;
      send(ws, { type: "agent_delta", text: delta });
    }
    finish(ws, state);
  } catch (e) {
    send(ws, { type: "error", message: String((e as Error).message ?? e) });
  } finally {
    if (!micOpen(state.fsm)) step(ws, state, { kind: "CANCEL" });
    send(ws, { type: "listening" });
  }
}

/**
 * Execute a parsed meta action against the lane registry.
 * No Hermes involvement — this plane must work even when the agent is down.
 */
function executeMeta(action: MetaAction): string {
  switch (action.op) {
    case "switch": {
      const lane = switchLane(lanes, action.lane.id);
      return `Switched to ${lane.name}.`;
    }
    case "list": {
      const active = activeLane(lanes);
      const list = lanes.lanes
        .map((l) => `${l.name}${l.id === active.id ? " (active)" : ""}`)
        .join(", ");
      return `Conversations: ${list}.`;
    }
    case "new": {
      const lane = createLane(lanes, action.name);
      switchLane(lanes, lane.id);
      return `Created ${lane.name}. You're in it.`;
    }
    case "brief": {
      const lane = action.lane ?? activeLane(lanes);
      const age = Math.round((Date.now() - new Date(lane.lastActive).getTime()) / 3600000);
      const ageText = age < 1 ? "under an hour" : `${age} hour${age > 1 ? "s" : ""}`;
      if (lane.id === "telepathy:direct") {
        return `Direct line to Hermes. No project context.`;
      }
      return `Lane ${lane.name}. Last active ${ageText} ago. Full briefing arrives with the Hermes connector.`;
    }
    case "unknown":
      return "Meta commands: switch to name, list conversations, new conversation for name, brief.";
  }
}

/**
 * Placeholder brain. Replace with the Hermes relay call; the lane's chat_id
 * (activeLane(lanes).id) is what stamps the relay MessageEvent.
 */
async function respond(text: string, _state: ClientState): Promise<string> {
  return `Heard you say: ${text}`;
}

function* chunks(s: string): Generator<string> {
  for (let i = 0; i < s.length; i += 80) yield s.slice(i, i + 80);
}

function send(ws: WebSocket, obj: object) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
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

console.log(`telepathy bridge listening on :${config.port} (stt=${config.stt}${process.env.TELEPATHY_TOKEN ? " auth=on" : ""})`);

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, () => {
    console.log(`\n${sig} — closing ${wss.clients.size} connection(s)`);
    for (const c of wss.clients) c.close(1001, "server shutting down");
    wss.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 2000).unref();
  });
}
