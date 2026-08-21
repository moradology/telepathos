// Interaction state machine tests: legal paths, illegal no-ops, cancel-everywhere.
import { transition } from "./dist/fsm.js";

const L = { phase: "listening" };
let failures = 0;
const check = (name, ok, detail = "") => {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
  if (!ok) failures++;
};

const ALL_EVENTS = [
  { kind: "SPEECH_START", prerollBytes: 100 },
  { kind: "SPEECH_CHUNK", bytes: 10 },
  { kind: "UTTERANCE_END" },
  { kind: "FORCE_END" },
  { kind: "BEGIN_ECHO", sampleRate: 22050 },
  { kind: "BEGIN_REPLY", sampleRate: 22050 },
  { kind: "PLAYBACK_DONE" },
  { kind: "CANCEL" },
];

// 1. happy path
let s = L;
s = transition(s, { kind: "SPEECH_START", prerollBytes: 3200 });
check("listening → capturing on SPEECH_START", s.phase === "capturing");
s = transition(s, { kind: "SPEECH_CHUNK", bytes: 800 });
check("capturing accumulates bytes", s.bytes === 4000);
s = transition(s, { kind: "UTTERANCE_END" });
check("capturing → processing on UTTERANCE_END", s.phase === "processing");
s = transition(s, { kind: "BEGIN_ECHO", sampleRate: 22050 });
check("processing → echoing on BEGIN_ECHO", s.phase === "echoing");
s = transition(s, { kind: "PLAYBACK_DONE" });
check("echoing → processing on PLAYBACK_DONE", s.phase === "processing");
s = transition(s, { kind: "BEGIN_REPLY", sampleRate: 22050 });
check("processing → speaking on BEGIN_REPLY", s.phase === "speaking");
s = transition(s, { kind: "PLAYBACK_DONE" });
check("speaking → listening on PLAYBACK_DONE", s.phase === "listening");

// 2. illegal events are identity (no-op), never crash
for (const phase of ["listening", "capturing", "processing", "echoing", "speaking"]) {
  let st = { phase };
  if (phase === "capturing") st = { phase, bytes: 0 };
  if (phase === "echoing" || phase === "speaking") st = { phase, sampleRate: 22050 };
  for (const ev of ALL_EVENTS) {
    const next = transition(st, ev);
    const legal =
      // the transitions asserted by the happy path above plus CANCEL escapes
      (phase === "listening" && ev.kind === "SPEECH_START") ||
      (phase === "capturing" && ["SPEECH_CHUNK", "UTTERANCE_END", "FORCE_END", "CANCEL"].includes(ev.kind)) ||
      (phase === "processing" && ["BEGIN_ECHO", "BEGIN_REPLY", "CANCEL"].includes(ev.kind)) ||
      (phase === "echoing" && ["BEGIN_REPLY", "PLAYBACK_DONE", "CANCEL"].includes(ev.kind)) ||
      (phase === "speaking" && ["PLAYBACK_DONE", "CANCEL"].includes(ev.kind));
    if (!legal) {
      // must be an identity transition
      if (JSON.stringify(next) !== JSON.stringify(st)) {
        check(`illegal ${ev.kind} in ${phase} is a no-op`, false, `became ${next.phase}`);
      }
    }
  }
}
check("all illegal event/state combos are no-ops", true);

// 3. CANCEL reaches listening from every non-listening state
for (const phase of ["capturing", "processing", "echoing", "speaking"]) {
  let st = { phase };
  if (phase === "capturing") st = { phase, bytes: 1 };
  if (phase === "echoing" || phase === "speaking") st = { phase, sampleRate: 22050 };
  const next = transition(st, { kind: "CANCEL" });
  check(`CANCEL from ${phase} → listening`, next.phase === "listening");
}

// 4. random walk fuzzing: machine never invents phases, always lands valid
let seed = 42;
const rand = () => { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; return (seed >>> 0) / 0xffffffff; };
const VALID_PHASES = ["listening", "capturing", "processing", "echoing", "speaking"];
let st = L;
for (let i = 0; i < 20_000; i++) {
  const ev = ALL_EVENTS[Math.floor(rand() * ALL_EVENTS.length)];
  st = transition(st, ev);
  if (!VALID_PHASES.includes(st.phase)) {
    check("fuzz produced invalid phase", false, JSON.stringify(st));
    break;
  }
}
check("20k-step random walk stays in valid phases", true);

console.log(failures === 0 ? "FSM TESTS PASS" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);
