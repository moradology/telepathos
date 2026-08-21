// Property test for parseControl: garbage in → null out, never a throw.
// Deterministic PRNG so failures reproduce.
import { parseControl } from "./dist/protocol.js";

// xorshift PRNG, seeded
let seed = 0x2545f491;
const rand = () => {
  seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
  return (seed >>> 0) / 0xffffffff;
};
const pick = (arr) => arr[Math.floor(rand() * arr.length)];

const ALPHABET = "abcehiklmnoprstuy{}\":,.[].0123456789 ".split("");
const VALID = [
  '{"type":"hello","device":"x"}',
  '{"type":"command","command":"stop"}',
  '{"type":"command","command":"repeat"}',
  '{"type":"command","command":"cancel_capture"}',
  '{"type":"utterance_end"}',
];
const VALID_KINDS = ["stop", "repeat", "cancel_capture"];

let failures = 0;

// 1. 50k random strings: must never throw; must return null or a valid msg
for (let i = 0; i < 50_000; i++) {
  const len = Math.floor(rand() * 80);
  let s = "";
  for (let j = 0; j < len; j++) s += pick(ALPHABET);
  try {
    const m = parseControl(s);
    if (m !== null && !["hello", "command"].includes(m.tag)) {
      console.log("FAIL: invented variant", m, "from", JSON.stringify(s));
      failures++;
    }
  } catch (e) {
    console.log("FAIL: threw on", JSON.stringify(s), e);
    failures++;
  }
}

// 2. all valid forms must parse and round-trip
for (const s of VALID) {
  const m = parseControl(s);
  if (m === null) { console.log("FAIL: rejected valid", s); failures++; }
}
for (const kind of VALID_KINDS) {
  const m = parseControl(`{"type":"command","command":"${kind}"}`);
  if (m?.tag !== "command" || m.kind !== kind) {
    console.log("FAIL: bad round-trip for", kind); failures++;
  }
}

// 3. near-misses must be rejected (typos, wrong types)
const NEAR_MISS = [
  '{"type":"command","command":"STOP"}',
  '{"type":"command","command":"approve"}',     // old protocol word
  '{"type":"hello"}',
  '{"type":"hello","device":42}',
  '{"type":"Command","command":"stop"}',
  '[]',
  'null',
  '"stop"',
];
for (const s of NEAR_MISS) {
  if (parseControl(s) !== null) { console.log("FAIL: accepted near-miss", s); failures++; }
}

console.log(failures === 0 ? "ALL PROPERTY TESTS PASS (50k+ cases)" : `${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
