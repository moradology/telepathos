// VAD fold property tests: seeded random walk over pure vadStep with
// invariants that must hold for ANY input sequence.
import { readFileSync } from "node:fs";
import { vadStep, VAD_INITIAL, rms } from "./dist/vad.js";

const vectors = JSON.parse(
  readFileSync(new URL("../protocol/vectors.json", import.meta.url), "utf8"),
); // loaded to keep the suite shape uniform; unused fields ignored

const cfg = { threshold: 600, silenceMsToEnd: 1500, minSpeechMs: 500 };

let seed = 0x5eed;
const rand = () => {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff;
  return seed / 0x7fffffff;
};

function chunk(loud) {
  const b = Buffer.alloc(3200); // 100 ms
  if (loud) for (let i = 0; i < 1600; i++) b.writeInt16LE((i % 20) < 10 ? 8000 : -8000, i * 2);
  return b;
}

let failures = 0;
const check = (name, ok) => {
  if (!ok) failures++;
  if (!ok) console.log(`FAIL ${name}`);
};

// 1. seeded random walk: state stays in legal ranges; "end" only from speaking
seed = 7;
let s = VAD_INITIAL;
let sawStart = false;
for (let i = 0; i < 20_000; i++) {
  const loud = rand() < 0.5;
  const r = vadStep(s, chunk(loud), cfg);
  s = r.state;
  if (r.event === "start") sawStart = true;
  if (r.event === "end") check("end only after speaking", s.speaking === false);
  if (s.speaking) check("speaking implies speechMs > 0", s.speechMs > 0);
}
check("walk produced a start event", sawStart);

// 2. silence after speech always ends within silenceMsToEnd + one chunk
let s2 = VAD_INITIAL;
s2 = vadStep(s2, chunk(true), cfg).state; // trigger start ramp
for (let i = 0; i < 20; i++) s2 = vadStep(s2, chunk(true), cfg).state; // speak
let ended = false;
for (let i = 0; i < Math.ceil(cfg.silenceMsToEnd / 100) + 2; i++) {
  const r = vadStep(s2 === undefined ? VAD_INITIAL : s2, chunk(false), cfg);
  s2 = r.state;
  if (r.event === "end") { ended = true; break; }
  if (s2.speaking === false) { ended = true; break; }
}
check("silence ends the utterance promptly", ended);

// 3. rms: silence is quiet, loud is loud
check("rms silence < threshold", rms(Buffer.alloc(3200)) < cfg.threshold);
check("rms loud > threshold", rms(chunk(true)) > cfg.threshold);

// 4. determinism: same input sequence → same event sequence
function runSequence() {
  let st = VAD_INITIAL;
  const events = [];
  seed = 99;
  for (let i = 0; i < 500; i++) {
    const loud = rand() < 0.6;
    const r = vadStep(st, chunk(loud), cfg);
    st = r.state;
    if (r.event) events.push(r.event);
  }
  return events;
}
const a = runSequence();
const b = runSequence();
check("deterministic replay", JSON.stringify(a) === JSON.stringify(b), `${a.length} vs ${b.length}`);

console.log(failures === 0 ? "VAD FOLD TESTS PASS" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);
