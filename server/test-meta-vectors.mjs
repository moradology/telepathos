// Meta-grammar parity: Node parseMeta vs protocol/meta-vectors.json.
import { readFileSync } from "node:fs";
import { parseMeta } from "./dist/meta.js";
import { loadLanes } from "./dist/lanes.js";

const vectors = JSON.parse(
  readFileSync(new URL("../protocol/meta-vectors.json", import.meta.url), "utf8"),
);
const reg = loadLanes();
// seed kerchunk so switch/brief vectors have a target
if (!reg.lanes.some((l) => l.name === "kerchunk")) {
  reg.lanes.push({ id: "telepathy:repo:kerchunk", name: "kerchunk", createdAt: "", lastActive: "" });
}

let failures = 0;
for (const c of vectors.cases) {
  const a = parseMeta(c.transcript, reg);
  const gotOp = a.op ?? "(none)";
  if (gotOp !== c.op) {
    console.log(`FAIL "${c.transcript}": op ${gotOp} != ${c.op}`);
    failures++;
    continue;
  }
  if (c.lane && (a.lane?.name ?? a.lane?.id) !== c.lane && a.lane?.name !== c.lane) {
    console.log(`FAIL "${c.transcript}": lane ${a.lane?.name ?? a.lane?.id} != ${c.lane}`);
    failures++;
  }
  if (c.name !== undefined && (a.name ?? null) !== c.name) {
    console.log(`FAIL "${c.transcript}": name ${a.name} != ${c.name}`);
    failures++;
  }
  if (c.text !== undefined && (a.text ?? null) !== c.text) {
    console.log(`FAIL "${c.transcript}": text ${a.text} != ${c.text}`);
    failures++;
  }
}
console.log(failures === 0 ? "META VECTORS PASS (node)" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);
