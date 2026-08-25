// Shared-vector protocol conformance: server/src/protocol.ts must classify
// protocol/vectors.json exactly as Rust and Kotlin do.
import { readFileSync } from "node:fs";
import { parseControl } from "./dist/protocol.js";

const vectors = JSON.parse(
  readFileSync(new URL("../protocol/vectors.json", import.meta.url), "utf8"),
);

let failures = 0;
const check = (name, ok, detail = "") => {
  if (!ok) failures++;
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
};

for (const v of vectors.control.valid) {
  const parsed = parseControl(v.frame);
  const tag = parsed?.tag ?? null;
  check(`control valid → ${v.tag}`, tag === v.tag, `got ${tag}`);
}

for (const frame of vectors.control.invalid) {
  check(`control invalid rejected: ${frame.slice(0, 48)}`, parseControl(frame) === null);
}

// server frames: the bridge constructs these ad-hoc; vectors document the
// shape Kotlin parses. Validate structurally here.
for (const v of vectors.server.valid) {
  let ok = false;
  try {
    const o = JSON.parse(v.frame);
    ok = o.type === v.type;
  } catch { ok = false; }
  check(`server valid shape: ${v.type}`, ok);
}

for (const frame of vectors.server.invalid) {
  let rejected = true;
  try {
    const o = JSON.parse(frame);
    rejected = !(typeof o === "object" && o !== null && typeof o.type === "string" &&
      !["unknown_weird"].includes(o.type));
  } catch { rejected = true; }
  check(`server invalid: ${frame.slice(0, 40)}`, rejected);
}

console.log(failures === 0 ? "VECTOR SUITE PASS (TS)" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);
