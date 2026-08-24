// Property test for parseControl: garbage in → null out, never a throw.
// Deterministic PRNG so failures reproduce.
import {
  MAX_INSTALLATION_ID_LENGTH,
  MAX_LANE_ID_LENGTH,
  MAX_TURN_TOKEN_LENGTH,
  MAX_OPAQUE_ID_LENGTH,
  MAX_OPAQUE_ID_BYTES,
  isValidLaneId,
  isValidOpaqueId,
  isValidTurnToken,
  isValidInstallationId,
  parseControl,
} from "./dist/protocol.js";

for (const id of ["id-1", "é".repeat(MAX_OPAQUE_ID_BYTES / 2), "🦀".repeat(MAX_OPAQUE_ID_BYTES / 4)]) {
  if (!isValidOpaqueId(id)) throw new Error(`valid opaque ID rejected: ${JSON.stringify(id)}`);
}
for (const id of ["", " \t\n", "id\u0000bad", "id\u0085bad", "é".repeat(MAX_OPAQUE_ID_BYTES / 2 + 1), "i".repeat(MAX_OPAQUE_ID_LENGTH + 1)]) {
  if (isValidOpaqueId(id)) throw new Error(`invalid opaque ID accepted: ${JSON.stringify(id)}`);
}
const OPAQUE_ID_BLANKNESS_CASES = [
  ["", false],
  [" ", false],
  ["\t", false],
  ["\n", false],
  ["\u000b", false],
  ["\u000c", false],
  ["\r", false],
  ["\u0000", false],
  ["\u001f", false],
  ["\u007f", false],
  ["\u0085", false],
  ["\u009f", false],
  ["\u00a0", false],
  ["\u2007", false],
  ["\u202f", false],
  ["\ufeff", false],
  ["id", true],
  [" id ", true],
  ["\u00a0id\u00a0", true],
  ["\u2007id\u2007", true],
  ["\u202fid\u202f", true],
  ["\ufeffid\ufeff", true],
  ["id\t", false],
  ["id\u0085", false],
];
for (const [id, expected] of OPAQUE_ID_BLANKNESS_CASES) {
  if (isValidOpaqueId(id) !== expected) {
    throw new Error(`opaque-ID blankness mismatch for ${JSON.stringify(id)}`);
  }
}

const TURN_TOKEN_BLANKNESS_CASES = [
  ["", false],
  [" ", false],
  ["\t", false],
  ["\n", false],
  ["\u000b", false],
  ["\u000c", false],
  ["\r", false],
  ["\u0085", false],
  ["\u00a0", false],
  ["\u1680", false],
  ["\u2007", false],
  ["\u202f", false],
  ["\u3000", false],
  ["\ufeff", false],
  ["\ud800", false],
  ["\udc00", false],
  // Turn tokens historically have no control-character rejection.
  ["\u0000", true],
  ["\u001f", true],
  ["\u007f", true],
  ["\u009f", true],
  ["turn-1", true],
  [" turn-1 ", true],
  ["\u00a0turn-1\u00a0", true],
  ["\u2007turn-1\u2007", true],
  ["\u202fturn-1\u202f", true],
  ["\ufeffturn-1\ufeff", true],
  ["turn-1\t", true],
  ["turn-1\u0085", true],
];
for (const [token, expected] of TURN_TOKEN_BLANKNESS_CASES) {
  if (isValidTurnToken(token) !== expected) {
    throw new Error(`turn-token blankness mismatch for ${JSON.stringify(token)}`);
  }
  const parsed = parseControl(JSON.stringify({ type: "command", command: "stop", turn_token: token }));
  if ((parsed !== null) !== expected || (expected && parsed.turnToken !== token)) {
    throw new Error(`turn-token parser mismatch for ${JSON.stringify(token)}`);
  }
}

const INSTALLATION_ID_BLANKNESS_CASES = [
  ["", false],
  [" ", false],
  ["\t", false],
  ["\n", false],
  ["\u000b", false],
  ["\u000c", false],
  ["\r", false],
  ["\u0085", false],
  ["\u00a0", false],
  ["\u1680", false],
  ["\u2007", false],
  ["\u202f", false],
  ["\u3000", false],
  ["\ufeff", false],
  ["\ud800", false],
  ["\udc00", false],
  ["\u0000", false],
  ["\u001f", false],
  ["\u007f", false],
  ["\u009f", false],
  ["owner", true],
  [" owner ", true],
  ["\u00a0owner\u00a0", true],
  ["\u2007owner\u2007", true],
  ["\u202fowner\u202f", true],
  ["\ufeffowner\ufeff", true],
  ["owner\t", false],
  ["owner\u0085", false],
  ["owner\u0000", false],
];
for (const [installationId, expected] of INSTALLATION_ID_BLANKNESS_CASES) {
  if (isValidInstallationId(installationId) !== expected) {
    throw new Error(`installation-ID blankness mismatch for ${JSON.stringify(installationId)}`);
  }
  const parsed = parseControl(JSON.stringify({ type: "hello", device: "x", installation_id: installationId }));
  if ((parsed !== null) !== expected || (expected && parsed.installationId !== installationId)) {
    throw new Error(`installation-ID parser mismatch for ${JSON.stringify(installationId)}`);
  }
}

// xorshift PRNG, seeded
let seed = 0x2545f491;
const rand = () => {
  seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
  return (seed >>> 0) / 0xffffffff;
};
const pick = (arr) => arr[Math.floor(rand() * arr.length)];

const ALPHABET = "abcehiklmnoprstuy{}\":,.[].0123456789 ".split("");
const VALID = [
  '{"type":"hello","device":"x","installation_id":"installation-x"}',
  '{"type":"command","command":"stop","turn_token":"turn-1"}',
  '{"type":"command","command":"repeat","turn_token":"turn-1"}',
  '{"type":"command","command":"cancel_capture","turn_token":"turn-1"}',
  '{"type":"utterance_end","turn_token":"turn-1"}',
  '{"type":"meta_mode","turn_token":"turn-1"}',
  '{"type":"lane","id":"telepathy:direct","turn_token":"turn-1"}',
  '{"type":"reply_received","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}',
  '{"type":"reply_ack","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}',
  '{"type":"reply_ack_retire","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}',
];
const VALID_KINDS = ["stop", "repeat", "cancel_capture"];
let failures = 0;

for (const laneId of [
  "telepathy:direct",
  "telepathy:repo:geospatial-migration",
]) {
  if (!isValidLaneId(laneId)) {
    console.log("FAIL: rejected valid lane ID", laneId); failures++;
  }
}
for (const laneId of [
  'telepathy:repo:quote"',
  "telepathy:repo:backslash\\",
  "telepathy:repo:control\u0000",
  "telepathy:repo:é",
  `telepathy:repo:${"a".repeat(MAX_LANE_ID_LENGTH)}`,
]) {
  const frame = {
    type: "reply_received", lane_id: laneId, reply_to: "tp-1",
    after_seq: 0, through_seq: 1, turn_token: "turn-1", interaction_id: "i-1",
  };
  if (parseControl(JSON.stringify(frame)) !== null) {
    console.log("FAIL: protocol accepted invalid lane ID", JSON.stringify(laneId)); failures++;
  }
}
for (const laneId of [
  "",
  " ",
  'telepathy:repo:quote"',
  "telepathy:repo:backslash\\",
  "telepathy:repo:control\n",
  "telepathy:repo:é",
  `telepathy:repo:${"a".repeat(MAX_LANE_ID_LENGTH)}`,
]) {
  if (isValidLaneId(laneId)) {
    console.log("FAIL: accepted invalid lane ID", JSON.stringify(laneId)); failures++;
  }
}

// 1. 50k random strings: must never throw; must return null or a valid msg
for (let i = 0; i < 50_000; i++) {
  const len = Math.floor(rand() * 80);
  let s = "";
  for (let j = 0; j < len; j++) s += pick(ALPHABET);
  try {
    const m = parseControl(s);
    if (m !== null && !["hello", "command", "utterance_end", "meta_mode", "lane", "reply_received", "reply_ack", "reply_ack_retire"].includes(m.tag)) {
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
  const m = parseControl(`{"type":"command","command":"${kind}","turn_token":"turn-1"}`);
  if (m?.tag !== "command" || m.kind !== kind || m.turnToken !== "turn-1") {
    console.log("FAIL: bad round-trip for", kind); failures++;
  }
}
const lane = parseControl('{"type":"lane","id":"telepathy:direct","turn_token":"turn-1"}');
if (lane?.tag !== "lane" || lane.id !== "telepathy:direct" || lane.turnToken !== "turn-1") {
  console.log("FAIL: bad lane snapshot"); failures++;
}
const hello = parseControl('{"type":"hello","device":"x","installation_id":"installation-x"}');
if (hello?.tag !== "hello" || hello.installationId !== "installation-x") {
  console.log("FAIL: bad installation-owned hello"); failures++;
}
if (hello?.token !== undefined) {
  console.log("FAIL: no-token hello did not preserve local mode"); failures++;
}
const exactToken = " token/exact-value ";
const tokenHello = parseControl(JSON.stringify({
  type: "hello", device: "x", installation_id: "installation-x", token: exactToken,
}));
if (tokenHello?.tag !== "hello" || tokenHello.token !== exactToken) {
  console.log("FAIL: exact hello token was not preserved"); failures++;
}
for (const token of [42, {}, null, []]) {
  const invalidTokenHello = parseControl(JSON.stringify({
    type: "hello", device: "x", installation_id: "installation-x", token,
  }));
  if (invalidTokenHello !== null) {
    console.log("FAIL: accepted invalid hello token", JSON.stringify(token)); failures++;
  }
}
const replyAck = parseControl('{"type":"reply_ack","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}');
if (replyAck?.tag !== "reply_ack" || replyAck.laneId !== "telepathy:direct" || replyAck.replyTo !== "tp-1" || replyAck.throughSeq !== 1 || replyAck.turnToken !== "turn-1" || replyAck.interactionId !== "i-1") {
  console.log("FAIL: bad reply acknowledgement"); failures++;
}
const replyReceived = parseControl('{"type":"reply_received","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}');
if (replyReceived?.tag !== "reply_received" || replyReceived.laneId !== "telepathy:direct" || replyReceived.replyTo !== "tp-1" || replyReceived.throughSeq !== 1 || replyReceived.turnToken !== "turn-1" || replyReceived.interactionId !== "i-1") {
  console.log("FAIL: bad durable reply receipt"); failures++;
}
const replyAckRetire = parseControl('{"type":"reply_ack_retire","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1,"turn_token":"turn-1","interaction_id":"i-1"}');
if (replyAckRetire?.tag !== "reply_ack_retire" || replyAckRetire.laneId !== "telepathy:direct" || replyAckRetire.replyTo !== "tp-1" || replyAckRetire.throughSeq !== 1 || replyAckRetire.turnToken !== "turn-1" || replyAckRetire.interactionId !== "i-1") {
  console.log("FAIL: bad reply acknowledgement retirement"); failures++;
}

// 3. turn tokens are bounded before any caller can retain them.
const generatedUuid = "123e4567-e89b-12d3-a456-426614174000";
if (generatedUuid.length >= MAX_TURN_TOKEN_LENGTH || parseControl(JSON.stringify({
  type: "lane", id: "telepathy:direct", turn_token: generatedUuid,
})) === null) {
  console.log("FAIL: rejected a generated UUID turn token"); failures++;
}

const TURN_TOKEN_FRAMES = [
  ["command", { type: "command", command: "stop" }],
  ["utterance_end", { type: "utterance_end" }],
  ["meta_mode", { type: "meta_mode" }],
  ["lane", { type: "lane", id: "telepathy:direct" }],
  ["reply_received", {
    type: "reply_received", lane_id: "telepathy:direct", reply_to: "tp-1",
    after_seq: 0, through_seq: 1, interaction_id: "i-1",
  }],
  ["reply_ack", {
    type: "reply_ack", lane_id: "telepathy:direct", reply_to: "tp-1",
    after_seq: 0, through_seq: 1, interaction_id: "i-1",
  }],
  ["reply_ack_retire", {
    type: "reply_ack_retire", lane_id: "telepathy:direct", reply_to: "tp-1",
    after_seq: 0, through_seq: 1, interaction_id: "i-1",
  }],
];
for (const [label, frame] of TURN_TOKEN_FRAMES) {
  const atLimit = parseControl(JSON.stringify({
    ...frame, turn_token: "t".repeat(MAX_TURN_TOKEN_LENGTH),
  }));
  if (atLimit === null) {
    console.log("FAIL: rejected maximum-length turn token for", label); failures++;
  }

  const oversized = parseControl(JSON.stringify({
    ...frame, turn_token: "t".repeat(MAX_TURN_TOKEN_LENGTH + 1),
  }));
  if (oversized !== null) {
    console.log("FAIL: accepted oversized turn token for", label); failures++;
  }
}

// 4. Installation ownership IDs are opaque but bounded and safe to persist.
const installationAtLimit = parseControl(JSON.stringify({
  type: "hello", device: "x", installation_id: "i".repeat(MAX_INSTALLATION_ID_LENGTH),
}));
if (installationAtLimit === null) {
  console.log("FAIL: rejected maximum-length installation ID"); failures++;
}
for (const installationId of [
  "",
  "   ",
  "i".repeat(MAX_INSTALLATION_ID_LENGTH + 1),
  "installation\nnewline",
  "installation\u0000nul",
  "installation\u0085next-line",
]) {
  if (parseControl(JSON.stringify({ type: "hello", device: "x", installation_id: installationId })) !== null) {
    console.log("FAIL: accepted invalid installation ID", JSON.stringify(installationId)); failures++;
  }
}

// 5. near-misses must be rejected (typos, wrong types)
const NEAR_MISS = [
  '{"type":"command","command":"STOP"}',
  '{"type":"command","command":"approve"}',     // old protocol word
  '{"type":"command","command":"stop"}',         // hard cutover: token required
  '{"type":"utterance_end"}',
  '{"type":"meta_mode","turn_token":""}',
  '{"type":"lane","id":"telepathy:direct"}',
  '{"type":"reply_received","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1}',
  '{"type":"reply_ack","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1}',
  '{"type":"reply_ack_retire","lane_id":"telepathy:direct","reply_to":"tp-1","after_seq":0,"through_seq":1}',
  '{"type":"hello"}',
  '{"type":"hello","device":42}',
  '{"type":"hello","device":"x","installation_id":42}',
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
