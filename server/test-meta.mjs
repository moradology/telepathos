// Meta grammar + lane registry tests (pure functions, no server needed).
import { parseMeta } from "./dist/meta.js";
import { loadLanes, saveLanes, switchLane, createLane, activeLane } from "./dist/lanes.js";

let failures = 0;
const check = (name, ok, detail = "") => {
  if (!ok) failures++;
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
};

// fresh registry with two lanes
const reg = {
  lanes: [
    { id: "telepathy:direct", name: "direct", createdAt: "", lastActive: "" },
    { id: "telepathy:repo:kerchunk", name: "kerchunk", createdAt: "", lastActive: "" },
    { id: "telepathy:repo:telepathy", name: "telepathy", createdAt: "", lastActive: "" },
  ],
  activeId: "telepathy:repo:telepathy",
  previousId: "telepathy:direct",
};

// 1. switch commands, including STT-mangled lane names
check("switch by name", parseMeta("switch to kerchunk", reg).op === "switch");
check("switch mangled STT ('kirk chunk')", (() => {
  const a = parseMeta("switch to kirk chunk", reg);
  return a.op === "switch" && a.lane.name === "kerchunk";
})());
check("go to X", parseMeta("go to direct", reg).op === "switch");
check("work on the telepathy", parseMeta("work on the telepathy", reg).op === "switch");
check("bare lane name switches", parseMeta("kerchunk", reg).op === "switch");

// 2. collision safety: non-lane targets must NOT intercept
check("'switch to main' does not intercept", parseMeta("switch to main", reg).op === "unknown");
check("'switch to the other implementation' does not intercept",
  parseMeta("switch to the other implementation", reg).op === "unknown");

// 3. list / new / brief   (note: "switch back" deliberately NOT a verb —
// history/navigation is the agent's job via the lane API)
check("list conversations", parseMeta("list conversations", reg).op === "list");
check("what conversations do I have", parseMeta("what conversations do I have", reg).op === "list");
check("new conversation for geospatial migration", (() => {
  const a = parseMeta("new conversation for geospatial migration", reg);
  return a.op === "new" && a.name === "geospatial migration";
})());
check("brief me", parseMeta("brief me", reg).op === "brief");
check("brief on kerchunk", (() => {
  const a = parseMeta("brief on kerchunk", reg);
  return a.op === "brief" && a.lane?.name === "kerchunk";
})());

// 4. non-commands fall through
check("random sentence is unknown", parseMeta("why do the tests fail on tuesdays", reg).op === "unknown");
check("empty is unknown", parseMeta("", reg).op === "unknown");

// 5. registry mechanics (temp file)
process.env.TELEPATHY_LANES = "/tmp/telepathy-test-lanes.json";
const { rmSync } = await import("node:fs");
try { rmSync("/tmp/telepathy-test-lanes.json"); } catch {}
const reg2 = loadLanes();
check("fresh registry has direct lane", reg2.lanes.length === 1 && reg2.lanes[0].name === "direct");
switchLane(reg2, "telepathy:direct");
const lane = createLane(reg2, "Geospatial Migration!");
check("createLane slugs", lane.id === "telepathy:repo:geospatial-migration" && lane.name === "geospatial-migration");
switchLane(reg2, lane.id);
check("active is new lane", activeLane(reg2).id === lane.id);
switchLane(reg2, "telepathy:direct");
saveLanes(reg2);
const reloaded = loadLanes();
check("persist + reload", reloaded.lanes.length === 2 && reloaded.activeId === "telepathy:direct");

console.log(failures === 0 ? "META TESTS PASS" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);

// 6. agent-facing lane API
import { spawn } from "node:child_process";
process.env.TELEPATHY_LANES = "/tmp/tp-api-lanes.json";
process.env.TELEPATHY_PORT = "8791";
process.env.TELEPATHY_API_PORT = "8792";
try { rmSync("/tmp/tp-api-lanes.json"); } catch {}
const srv = spawn("node", ["dist/index.js"], { env: process.env, stdio: "ignore" });
await new Promise((r) => setTimeout(r, 1200));
const base = "http://127.0.0.1:8792";
const post = (p, b) => fetch(base + p, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b) });
const st = await (await fetch(base + "/api/state")).json();
check("api: state has direct lane", st.lanes.length === 1 && st.active === "direct");
await post("/api/lanes", { name: "kerchunk" });
const st2 = await (await fetch(base + "/api/state")).json();
check("api: create+switch via tools", st2.active === "kerchunk" && st2.lanes.length === 2);
await post("/api/lanes/active", { id: "telepathy:direct" });
const st3 = await (await fetch(base + "/api/state")).json();
check("api: switch via tools", st3.active === "direct");
srv.kill();
