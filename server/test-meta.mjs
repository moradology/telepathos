// Meta grammar, lane registry, and local API transaction tests.
import { parseMeta } from "./dist/meta.js";
import {
  LANE_CAPACITY_ERROR_MESSAGE,
  LaneCapacityError,
  LanePersistenceError,
  MAX_ENRICHED_LANE_TITLE_CODEPOINTS,
  MAX_LANE_COUNT,
  MAX_LANE_NAME_UTF8_BYTES,
  activeLane,
  createLane,
  isValidLaneTimestamp,
  isValidPersistedLaneName,
  laneNameValidationError,
  laneWritesUnavailableReason,
  loadLanes,
  mutateAndSaveLanes,
  saveLanes,
  setAfterRenameHookForTests,
  setLaneDirectorySyncHookForTests,
  switchLane,
  writeAll,
} from "./dist/lanes.js";
import { executeMetaTurn } from "./dist/index.js";
import { mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { join } from "node:path";

let failures = 0;
const check = (name, ok, detail = "") => {
  if (!ok) failures++;
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
};

// fresh registry with two lanes
const reg = {
  lanes: [
    { id: "telepathos:direct", name: "direct", createdAt: "", lastActive: "" },
    { id: "telepathos:repo:kerchunk", name: "kerchunk", createdAt: "", lastActive: "" },
    { id: "telepathos:repo:telepathos", name: "telepathos", createdAt: "", lastActive: "" },
  ],
  activeId: "telepathos:repo:telepathos",
  previousId: "telepathos:direct",
};

// 1. switch commands, including STT-mangled lane names
check("switch by name", parseMeta("switch to kerchunk", reg).op === "switch");
check("switch mangled STT ('kirk chunk')", (() => {
  const a = parseMeta("switch to kirk chunk", reg);
  return a.op === "switch" && a.lane.name === "kerchunk";
})());
check("go to X", parseMeta("go to direct", reg).op === "switch");
check("work on the telepathos", parseMeta("work on the telepathos", reg).op === "switch");
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
process.env.TELEPATHOS_LANES = "/tmp/telepathos-test-lanes.json";
try { rmSync("/tmp/telepathos-test-lanes.json"); } catch {}
const reg2 = loadLanes();
check("fresh registry has direct lane", reg2.lanes.length === 1 && reg2.lanes[0].name === "direct");
switchLane(reg2, "telepathos:direct");
const lane = createLane(reg2, "Geospatial Migration!");
check("createLane slugs", lane.id === "telepathos:repo:geospatial-migration" && lane.name === "geospatial-migration");
const punctuationLane = createLane(reg2, "A ! B");
check("createLane preserves Rust's internal dash runs", punctuationLane.id === "telepathos:repo:a---b");
switchLane(reg2, lane.id);
check("active is new lane", activeLane(reg2).id === lane.id);
switchLane(reg2, "telepathos:direct");
saveLanes(reg2);
const reloaded = loadLanes();
check("persist + reload", reloaded.lanes.length === 3 && reloaded.activeId === "telepathos:direct");

// The Node and Rust registry owners share a hard 256-lane limit. Construct
// maximum-length normal lanes so this also proves the complete daemon state
// envelope stays comfortably below the 1 MiB bridge transport cap.
// Slug budget: "telepathos:repo:" prefix (16) leaves 112 slug chars under
// MAX_LANE_ID_LENGTH; the 4-char index prefix leaves 108 for the x-run.
const laneNameAtCapacity = (index) => `${String(index).padStart(3, "0")}-${"x".repeat(108)}`;
const capacityRoot = mkdtempSync("/tmp/telepathos-node-lane-capacity-");
const capacityPath = join(capacityRoot, "lanes.json");
const lanePathBeforeCapacityTests = process.env.TELEPATHOS_LANES;
let capacityApiChild;
try {
  process.env.TELEPATHOS_LANES = capacityPath;
  const capacityRegistry = loadLanes();
  for (let index = 1; index < MAX_LANE_COUNT; index++) {
    createLane(capacityRegistry, laneNameAtCapacity(index));
  }
  saveLanes(capacityRegistry);
  const capacitySnapshot = readFileSync(capacityPath, "utf8");
  check("lane count exact boundary is admitted", capacityRegistry.lanes.length === MAX_LANE_COUNT);

  const fullStateEnvelope = JSON.stringify({
    lanes: capacityRegistry.lanes.map((lane) => ({
      id: lane.id,
      name: lane.name,
      created_at: lane.createdAt,
      last_active: lane.lastActive,
      pending: 0,
    })),
    active_id: capacityRegistry.activeId,
    previous_id: capacityRegistry.previousId,
    active: activeLane(capacityRegistry).name,
    revision: 9,
  });
  check("full maximum normal lane state stays below 128 KiB",
    Buffer.byteLength(fullStateEnvelope, "utf8") < 128 * 1024,
    `${Buffer.byteLength(fullStateEnvelope, "utf8")} bytes`);

  // This deliberately uses control characters: they expand to six-byte JSON
  // escapes, so it is stricter than a normal ASCII maximum-size state.
  const maximumMetadataStateEnvelope = JSON.stringify({
    lanes: capacityRegistry.lanes.map((lane) => ({
      id: lane.id,
      name: "\u0000".repeat(MAX_LANE_NAME_UTF8_BYTES),
      created_at: "epoch-ms:9007199254740991",
      last_active: "epoch-ms:9007199254740991",
      interactions: Number.MAX_SAFE_INTEGER,
      title: "\u0000".repeat(MAX_ENRICHED_LANE_TITLE_CODEPOINTS),
      pending: Number.MAX_SAFE_INTEGER,
    })),
    active_id: capacityRegistry.activeId,
    previous_id: capacityRegistry.previousId,
    active: "\u0000".repeat(MAX_LANE_NAME_UTF8_BYTES),
    revision: Number.MAX_SAFE_INTEGER,
  });
  check("full maximum metadata/title state stays below 512 KiB",
    Buffer.byteLength(maximumMetadataStateEnvelope, "utf8") < 512 * 1024,
    `${Buffer.byteLength(maximumMetadataStateEnvelope, "utf8")} bytes`);

  const existing = createLane(capacityRegistry, laneNameAtCapacity(1));
  check("existing lane remains available at capacity",
    existing.id === capacityRegistry.lanes[1].id && capacityRegistry.lanes.length === MAX_LANE_COUNT);

  const beforeCapacityRejection = structuredClone(capacityRegistry);
  let sequentialCapacityError;
  try { createLane(capacityRegistry, "one too many sequential"); } catch (error) { sequentialCapacityError = error; }
  check("sequential over-cap create returns stable capacity error",
    sequentialCapacityError instanceof LaneCapacityError &&
    sequentialCapacityError.message === LANE_CAPACITY_ERROR_MESSAGE);
  check("sequential over-cap create does not mutate memory",
    JSON.stringify(capacityRegistry) === JSON.stringify(beforeCapacityRejection));
  check("sequential over-cap create does not write the snapshot",
    readFileSync(capacityPath, "utf8") === capacitySnapshot);

  const concurrentResults = await Promise.all([
    "one too many concurrent one",
    "one too many concurrent two",
  ].map(async (name) => {
    try {
      createLane(capacityRegistry, name);
      return null;
    } catch (error) {
      return error;
    }
  }));
  check("concurrent over-cap creates are all permanent no-ops",
    concurrentResults.every((error) => error instanceof LaneCapacityError) &&
    JSON.stringify(capacityRegistry) === JSON.stringify(beforeCapacityRejection) &&
    readFileSync(capacityPath, "utf8") === capacitySnapshot);

  const metaReply = executeMetaTurn({ op: "new", name: "one too many meta" }, capacityRegistry);
  check("direct meta returns the stable lane-capacity reply", metaReply === LANE_CAPACITY_ERROR_MESSAGE);
  check("direct meta capacity reply does not mutate or rewrite",
    JSON.stringify(capacityRegistry) === JSON.stringify(beforeCapacityRejection) &&
    readFileSync(capacityPath, "utf8") === capacitySnapshot);

  const overCapacityRegistry = structuredClone(capacityRegistry);
  overCapacityRegistry.lanes.push({
    id: "telepathos:repo:overflow",
    name: "overflow",
    createdAt: "2026-01-01T00:00:00.000Z",
    lastActive: "2026-01-01T00:00:00.000Z",
  });
  let rejectedOverCapacitySave = false;
  try { saveLanes(overCapacityRegistry); } catch { rejectedOverCapacitySave = true; }
  check("over-cap save is rejected before replacing the snapshot",
    rejectedOverCapacitySave && readFileSync(capacityPath, "utf8") === capacitySnapshot);

  const persistedOverCapacity = JSON.stringify({
    lanes: overCapacityRegistry.lanes.map((lane) => ({
      id: lane.id,
      name: lane.name,
      created_at: lane.createdAt,
      last_active: lane.lastActive,
    })),
    active_id: overCapacityRegistry.activeId,
    previous_id: overCapacityRegistry.previousId,
  });
  writeFileSync(capacityPath, persistedOverCapacity);
  let rejectedOverCapacityRestart = false;
  try { loadLanes(); } catch { rejectedOverCapacityRestart = true; }
  check("over-cap snapshot hard-rejects on restart without overwrite",
    rejectedOverCapacityRestart && readFileSync(capacityPath, "utf8") === persistedOverCapacity);
  writeFileSync(capacityPath, capacitySnapshot);

  const capacityApiSource = [
    'import { loadLanes } from "./dist/lanes.js";',
    'import { startApiServer } from "./dist/api.js";',
    'startApiServer(loadLanes(), Number(process.env.TELEPATHOS_API_PORT), "127.0.0.1");',
  ].join("\n");
  capacityApiChild = spawn(process.execPath, ["--input-type=module", "--eval", capacityApiSource], {
    env: {
      ...process.env,
      TELEPATHOS_LANES: capacityPath,
      TELEPATHOS_API_PORT: "8796",
      TELEPATHOS_HERMES_URL: "",
      TELEPATHOS_TOKEN: "",
    },
    stdio: "ignore",
  });
  const capacityApiBase = "http://127.0.0.1:8796";
  let capacityApiState;
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      const response = await fetch(`${capacityApiBase}/api/state`);
      if (response.ok) {
        capacityApiState = await response.json();
        break;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  if (!capacityApiState) throw new Error("timed out waiting for lane-capacity API server");
  const capacityApiPost = (name) => fetch(`${capacityApiBase}/api/lanes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  const sequentialApi = await capacityApiPost("one too many API sequential");
  check("API classifies a full registry as permanent HTTP 409", sequentialApi.status === 409);
  check("API exposes the stable capacity error", (await sequentialApi.json()).error === LANE_CAPACITY_ERROR_MESSAGE);
  const concurrentApi = await Promise.all([
    "one too many API concurrent one",
    "one too many API concurrent two",
  ].map(capacityApiPost));
  check("concurrent API over-cap creates all return 409", concurrentApi.every((response) => response.status === 409));
  check("API capacity rejection preserves memory and disk",
    JSON.stringify(await (await fetch(`${capacityApiBase}/api/state`)).json()) === JSON.stringify(capacityApiState) &&
    readFileSync(capacityPath, "utf8") === capacitySnapshot);
} finally {
  if (capacityApiChild && capacityApiChild.exitCode === null) {
    capacityApiChild.kill("SIGTERM");
    await new Promise((resolve) => capacityApiChild.once("exit", resolve));
  }
  if (lanePathBeforeCapacityTests === undefined) delete process.env.TELEPATHOS_LANES;
  else process.env.TELEPATHOS_LANES = lanePathBeforeCapacityTests;
  rmSync(capacityRoot, { recursive: true, force: true });
}
const beforeInvalidSwitch = structuredClone(reg2);
let rejectedInvalidSwitch = false;
try { switchLane(reg2, 'telepathos:repo:bad"quote'); } catch { rejectedInvalidSwitch = true; }
check("invalid switch is rejected without mutation", rejectedInvalidSwitch &&
  JSON.stringify(reg2) === JSON.stringify(beforeInvalidSwitch));
for (const [label, name] of [
  ["non-string", null],
  ["ASCII whitespace", " \t\n"],
  ["Unicode Rust whitespace", "\u00a0\u2007\u202f\u3000"],
  ["punctuation with no slug", "!!!"],
  ["oversized generated ID", "x".repeat(114)],
]) {
  const before = structuredClone(reg2);
  let rejected = false;
  try { createLane(reg2, name); } catch { rejected = true; }
  check(`createLane rejects ${label} name without mutation`, rejected &&
    JSON.stringify(reg2) === JSON.stringify(before));
}

// Meta mutation paths must turn all constructor-level invalid names into the
// same deterministic Rust-compatible response before touching memory or disk.
const invalidMetaNames = [
  ["ASCII oversized", "x".repeat(114), "lane name is too long to produce a valid lane identifier"],
  ["Unicode blank", "\u00a0\u2007\u202f\u3000", "lane name must not be blank"],
  ["Unicode slugless", "💥", "lane name is too long to produce a valid lane identifier"],
];
const directMetaPath = "/tmp/telepathos-direct-meta-invalid-name.json";
try { rmSync(directMetaPath); } catch {}
process.env.TELEPATHOS_LANES = directMetaPath;
const directMetaRegistry = loadLanes();
saveLanes(directMetaRegistry);
for (const [label, name, expected] of invalidMetaNames) {
  const beforeMemory = structuredClone(directMetaRegistry);
  const beforeDisk = readFileSync(directMetaPath, "utf8");
  const reply = executeMetaTurn({ op: "new", name }, directMetaRegistry);
  check(`direct meta ${label} returns Rust-compatible invalid-name response`, reply === expected);
  check(`direct meta ${label} does not mutate memory`,
    JSON.stringify(directMetaRegistry) === JSON.stringify(beforeMemory));
  check(`direct meta ${label} does not rewrite the durable snapshot`,
    readFileSync(directMetaPath, "utf8") === beforeDisk);
}
try { rmSync(directMetaPath); } catch {}
process.env.TELEPATHOS_LANES = "/tmp/telepathos-test-lanes.json";

// A first write into a nested state path must sync the parent of every
// directory created by recursive mkdir, followed by the final directory
// sync after the snapshot rename.
const originalLanePath = process.env.TELEPATHOS_LANES;
const nestedLaneRoot = mkdtempSync("/tmp/telepathos-node-nested-lanes-");
const nestedLanePath = join(nestedLaneRoot, "created", "state", "lanes.json");
const nestedDirectorySyncs = [];
process.env.TELEPATHOS_LANES = nestedLanePath;
setLaneDirectorySyncHookForTests((syncedPath) => nestedDirectorySyncs.push(syncedPath));
try {
  saveLanes(loadLanes());
} finally {
  setLaneDirectorySyncHookForTests(null);
  if (originalLanePath === undefined) delete process.env.TELEPATHOS_LANES;
  else process.env.TELEPATHOS_LANES = originalLanePath;
  rmSync(nestedLaneRoot, { recursive: true, force: true });
}
check("nested lane snapshots sync every created directory parent", JSON.stringify(nestedDirectorySyncs) === JSON.stringify([
  nestedLaneRoot,
  join(nestedLaneRoot, "created"),
  join(nestedLaneRoot, "created", "state"),
]));

const shortWriteContents = "lane snapshot with unicode: λ";
const shortWriteOutput = Buffer.alloc(Buffer.byteLength(shortWriteContents));
let shortWriteCalls = 0;
writeAll(0, shortWriteContents, (_fd, buffer, offset, length) => {
  const count = Math.min(length, 3);
  buffer.copy(shortWriteOutput, offset, offset, offset + count);
  shortWriteCalls++;
  return count;
});
check("lane snapshots handle short writes", shortWriteCalls > 1 &&
  shortWriteOutput.equals(Buffer.from(shortWriteContents, "utf8")));

// malformed authoritative files must fail closed instead of turning missing
// fields into the literal string "undefined".
writeFileSync("/tmp/telepathos-test-lanes.json", JSON.stringify({
  lanes: [{ id: "telepathos:direct", name: "direct", created_at: "", last_active: "" }],
  active_id: "telepathos:missing",
  previous_id: "telepathos:direct",
}));
let rejectedMalformed = false;
try { loadLanes(); } catch { rejectedMalformed = true; }
check("reject malformed lane registry", rejectedMalformed);

writeFileSync("/tmp/telepathos-test-lanes.json", JSON.stringify({
  lanes: [{ id: 'telepathos:repo:bad"quote', name: "bad", created_at: "now", last_active: "now" }],
  active_id: 'telepathos:repo:bad"quote',
  previous_id: 'telepathos:repo:bad"quote',
}));
let rejectedInvalidId = false;
try { loadLanes(); } catch { rejectedInvalidId = true; }
check("reject invalid persisted lane ID", rejectedInvalidId);

// The Node and Rust owners apply exactly the same durable metadata contract:
// bounded names, only canonical writer timestamp forms, and JSON-safe epoch
// values. Invalid snapshots are hard failures and neither path repairs them.
const metadataRoot = mkdtempSync("/tmp/telepathos-node-lane-metadata-");
const metadataPath = join(metadataRoot, "lanes.json");
const previousMetadataPath = process.env.TELEPATHOS_LANES;
try {
  process.env.TELEPATHOS_LANES = metadataPath;
  for (const name of [
    "a".repeat(MAX_LANE_NAME_UTF8_BYTES),
    "é".repeat(MAX_LANE_NAME_UTF8_BYTES / 2),
  ]) {
    check(`persisted lane name exact boundary is valid (${Buffer.byteLength(name)} bytes)`,
      isValidPersistedLaneName(name));
  }
  for (const name of [
    "a".repeat(MAX_LANE_NAME_UTF8_BYTES + 1),
    "é".repeat(MAX_LANE_NAME_UTF8_BYTES / 2 + 1),
    "\ud800",
  ]) {
    check("persisted lane name over-bound or malformed UTF-16 is rejected",
      !isValidPersistedLaneName(name));
  }
  for (const timestamp of [
    "epoch-ms:9007199254740991",
    "2024-02-29T23:59:59.999Z",
    "9999-12-31T23:59:59.999Z",
  ]) {
    check(`canonical timestamp is valid (${timestamp})`, isValidLaneTimestamp(timestamp));
  }
  for (const timestamp of [
    "epoch-ms:9007199254740992",
    "epoch-ms:-1",
    "epoch-ms:1.5",
    "2023-02-29T00:00:00.000Z",
    "2024-01-01T24:00:00.000Z",
    "2024-01-01T00:00:00.000+00:00",
  ]) {
    check(`invalid timestamp is rejected (${timestamp})`, !isValidLaneTimestamp(timestamp));
  }

  const acceptedSnapshot = JSON.stringify({
    lanes: [{
      id: "telepathos:direct",
      name: "é".repeat(MAX_LANE_NAME_UTF8_BYTES / 2),
      created_at: "epoch-ms:9007199254740991",
      last_active: "2024-02-29T23:59:59.999Z",
      interactions: Number.MAX_SAFE_INTEGER,
    }],
    active_id: "telepathos:direct",
    previous_id: "telepathos:direct",
  });
  writeFileSync(metadataPath, acceptedSnapshot);
  const accepted = loadLanes();
  check("bounded multibyte metadata snapshot reloads exactly", JSON.stringify(accepted.lanes[0]) === JSON.stringify({
    id: "telepathos:direct",
    name: "é".repeat(MAX_LANE_NAME_UTF8_BYTES / 2),
    createdAt: "epoch-ms:9007199254740991",
    lastActive: "2024-02-29T23:59:59.999Z",
    interactions: Number.MAX_SAFE_INTEGER,
  }));
  const beforeInvalidSave = readFileSync(metadataPath, "utf8");
  accepted.lanes[0].name = "x".repeat(MAX_LANE_NAME_UTF8_BYTES + 1);
  let rejectedInvalidSave = false;
  try { saveLanes(accepted); } catch { rejectedInvalidSave = true; }
  check("over-bound metadata save rejects without overwriting", rejectedInvalidSave &&
    readFileSync(metadataPath, "utf8") === beforeInvalidSave);

  const invalidRestartSnapshot = JSON.stringify({
    lanes: [{
      id: "telepathos:direct",
      name: "direct",
      created_at: "epoch-ms:9007199254740992",
      last_active: "2024-01-01T00:00:00.000Z",
    }],
    active_id: "telepathos:direct",
    previous_id: "telepathos:direct",
  });
  writeFileSync(metadataPath, invalidRestartSnapshot);
  let rejectedInvalidMetadataRestart = false;
  try { loadLanes(); } catch { rejectedInvalidMetadataRestart = true; }
  check("out-of-range timestamp snapshot hard-rejects on restart without overwrite",
    rejectedInvalidMetadataRestart && readFileSync(metadataPath, "utf8") === invalidRestartSnapshot);
} finally {
  if (previousMetadataPath === undefined) delete process.env.TELEPATHOS_LANES;
  else process.env.TELEPATHOS_LANES = previousMetadataPath;
  rmSync(metadataRoot, { recursive: true, force: true });
}

// The API must not leave its shared in-memory registry ahead of the durable
// snapshot when a synchronous save fails after a mutation. Replacing the
// registry directory with a file makes mkdirSync fail deterministically while
// keeping the child process's already-loaded registry available for GETs.
const apiFaultDir = "/tmp/telepathos-api-transaction";
const apiFaultBackupDir = "/tmp/telepathos-api-transaction-backup";
const apiFaultPath = `${apiFaultDir}/lanes.json`;
const apiFaultRegistry = {
  lanes: [
    { id: "telepathos:direct", name: "direct", created_at: "2020-01-01T00:00:00.000Z", last_active: "2020-01-01T00:00:00.000Z" },
    { id: "telepathos:repo:second", name: "second", created_at: "2020-01-02T00:00:00.000Z", last_active: "2020-01-02T00:00:00.000Z" },
  ],
  active_id: "telepathos:direct",
  previous_id: "telepathos:direct",
};
let apiChild;
try {
  rmSync(apiFaultDir, { recursive: true, force: true });
  rmSync(apiFaultBackupDir, { recursive: true, force: true });
  mkdirSync(apiFaultDir, { recursive: true });
  writeFileSync(apiFaultPath, JSON.stringify(apiFaultRegistry));
  apiChild = spawn(process.execPath, ["dist/index.js"], {
    env: {
      ...process.env,
      TELEPATHOS_LANES: apiFaultPath,
      TELEPATHOS_PORT: "8793",
      TELEPATHOS_API_PORT: "8794",
      TELEPATHOS_HOST: "127.0.0.1",
      TELEPATHOS_API_HOST: "127.0.0.1",
      TELEPATHOS_HERMES_URL: "",
      TELEPATHOS_TOKEN: "",
      TELEPATHOS_TLS_CERT: "",
      TELEPATHOS_TLS_KEY: "",
      TELEPATHOS_STT: "echo",
    },
    stdio: "ignore",
  });
  const apiBase = "http://127.0.0.1:8794";
  let baseline;
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      const response = await fetch(`${apiBase}/api/state`);
      if (response.ok) {
        baseline = await response.json();
        break;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  if (!baseline) throw new Error("timed out waiting for transactional API test server");

  const post = (path, body) => fetch(`${apiBase}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const state = async () => (await fetch(`${apiBase}/api/state`)).json();
  const checkRejectedWithoutMutation = async (label, path, body, expectedStatus) => {
    const before = await state();
    const beforeSnapshot = readFileSync(apiFaultPath, "utf8");
    const response = await post(path, body);
    check(`${label}: HTTP ${expectedStatus}`, response.status === expectedStatus);
    check(`${label}: registry remains unchanged`, JSON.stringify(await state()) === JSON.stringify(before));
    check(`${label}: snapshot remains unchanged`, readFileSync(apiFaultPath, "utf8") === beforeSnapshot);
  };

  await checkRejectedWithoutMutation("active missing ID", "/api/lanes/active", {}, 400);
  await checkRejectedWithoutMutation("active malformed ID", "/api/lanes/active", { id: 'telepathos:repo:bad"quote' }, 400);
  await checkRejectedWithoutMutation("active unknown ID", "/api/lanes/active", { id: "telepathos:missing" }, 404);
  await checkRejectedWithoutMutation("touch missing ID", "/api/lanes/touch", {}, 400);
  await checkRejectedWithoutMutation("touch unknown ID", "/api/lanes/touch", { id: "telepathos:missing" }, 404);
  await checkRejectedWithoutMutation("create missing name", "/api/lanes", {}, 400);
  await checkRejectedWithoutMutation("create non-string name", "/api/lanes", { name: null }, 400);
  await checkRejectedWithoutMutation("create ASCII-blank name", "/api/lanes", { name: " \t\n" }, 400);
  await checkRejectedWithoutMutation("create Unicode-blank name", "/api/lanes", { name: "\u00a0\u2007\u202f\u3000" }, 400);
  await checkRejectedWithoutMutation("create slugless name", "/api/lanes", { name: "!!!" }, 400);
  await checkRejectedWithoutMutation("create oversized name", "/api/lanes", { name: "x".repeat(114) }, 400);

  const successfulActive = await post("/api/lanes/active", { id: "telepathos:repo:second" });
  check("active succeeds after admission", successfulActive.status === 200);
  const successfulCreate = await post("/api/lanes", { name: "created success" });
  check("create succeeds after admission", successfulCreate.status === 200);
  const successfulTouch = await post("/api/lanes/touch", { id: "telepathos:repo:created-success" });
  check("touch succeeds after admission", successfulTouch.status === 200);
  baseline = await state();

  renameSync(apiFaultDir, apiFaultBackupDir);
  writeFileSync(apiFaultDir, "not a directory");
  const checkRollback = async (label, path, body) => {
    const before = await state();
    const beforeSnapshot = readFileSync(`${apiFaultBackupDir}/lanes.json`, "utf8");
    const response = await post(path, body);
    const after = await state();
    check(`${label}: definite save failure is HTTP 500`, response.status === 500);
    check(`${label}: in-memory registry rolls back`, JSON.stringify(after) === JSON.stringify(before));
    check(`${label}: durable snapshot remains unchanged`,
      readFileSync(`${apiFaultBackupDir}/lanes.json`, "utf8") === beforeSnapshot);
  };

  check("transactional API baseline loaded", JSON.stringify(await state()) === JSON.stringify(baseline));
  await checkRollback("switch active lane", "/api/lanes/active", { id: "telepathos:repo:second" });
  await checkRollback("create lane", "/api/lanes", { name: "third" });
  await checkRollback("touch lane", "/api/lanes/touch", { id: "telepathos:repo:second" });
} finally {
  if (apiChild && apiChild.exitCode === null) {
    apiChild.kill("SIGTERM");
    await new Promise((resolve) => apiChild.once("exit", resolve));
  }
  rmSync(apiFaultDir, { recursive: true, force: true });
  rmSync(apiFaultBackupDir, { recursive: true, force: true });
}

// Exercise the same post-rename fault seam through HTTP. This child owns its
// latch, so the direct registry tests below remain independent.
const apiAmbiguityDir = "/tmp/telepathos-api-ambiguity";
const apiAmbiguityPath = `${apiAmbiguityDir}/lanes.json`;
let ambiguityApiChild;
try {
  rmSync(apiAmbiguityDir, { recursive: true, force: true });
  mkdirSync(apiAmbiguityDir, { recursive: true });
  writeFileSync(apiAmbiguityPath, JSON.stringify(apiFaultRegistry));
  const ambiguityServer = [
    'import { loadLanes, setAfterRenameHookForTests } from "./dist/lanes.js";',
    'import { startApiServer } from "./dist/api.js";',
    'setAfterRenameHookForTests(() => { throw new Error("injected parent fsync failure"); });',
    'startApiServer(loadLanes(), Number(process.env.TELEPATHOS_API_PORT), "127.0.0.1");',
  ].join("\n");
  ambiguityApiChild = spawn(process.execPath, ["--input-type=module", "--eval", ambiguityServer], {
    env: {
      ...process.env,
      TELEPATHOS_LANES: apiAmbiguityPath,
      TELEPATHOS_API_PORT: "8795",
      TELEPATHOS_HERMES_URL: "",
      TELEPATHOS_TOKEN: "",
    },
    stdio: "ignore",
  });
  const ambiguityApiBase = "http://127.0.0.1:8795";
  let ambiguityBaseline;
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      const response = await fetch(`${ambiguityApiBase}/api/state`);
      if (response.ok) {
        ambiguityBaseline = await response.json();
        break;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  if (!ambiguityBaseline) throw new Error("timed out waiting for ambiguous API test server");
  const ambiguityPost = (path, body) => fetch(`${ambiguityApiBase}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const ambiguityState = async () => (await fetch(`${ambiguityApiBase}/api/state`)).json();

  const ambiguousActive = await ambiguityPost("/api/lanes/active", { id: "telepathos:repo:second" });
  check("active post-rename failure is HTTP 503", ambiguousActive.status === 503);
  const afterAmbiguousActive = await ambiguityState();
  check("active post-rename failure preserves the renamed registry in memory",
    afterAmbiguousActive.activeId === "telepathos:repo:second");
  const afterAmbiguousSnapshot = readFileSync(apiAmbiguityPath, "utf8");
  check("active post-rename failure leaves the renamed snapshot on disk",
    JSON.parse(afterAmbiguousSnapshot).active_id === "telepathos:repo:second");

  const latchedTouch = await ambiguityPost("/api/lanes/touch", { id: "telepathos:repo:second" });
  check("touch while persistence is latched is HTTP 503", latchedTouch.status === 503);
  check("latched touch rolls back the attempted in-memory mutation",
    JSON.stringify(await ambiguityState()) === JSON.stringify(afterAmbiguousActive));
  check("latched touch does not replace the ambiguous snapshot",
    readFileSync(apiAmbiguityPath, "utf8") === afterAmbiguousSnapshot);

  for (const [label, path, body, expectedStatus] of [
    ["latched malformed touch", "/api/lanes/touch", {}, 400],
    ["latched unknown touch", "/api/lanes/touch", { id: "telepathos:missing" }, 404],
    ["latched blank create", "/api/lanes", { name: "\u00a0\u2007" }, 400],
  ]) {
    const response = await ambiguityPost(path, body);
    check(`${label} precedes durability: HTTP ${expectedStatus}`, response.status === expectedStatus);
    check(`${label} does not mutate state`,
      JSON.stringify(await ambiguityState()) === JSON.stringify(afterAmbiguousActive));
    check(`${label} does not replace the snapshot`, readFileSync(apiAmbiguityPath, "utf8") === afterAmbiguousSnapshot);
  }
} finally {
  if (ambiguityApiChild && ambiguityApiChild.exitCode === null) {
    ambiguityApiChild.kill("SIGTERM");
    await new Promise((resolve) => ambiguityApiChild.once("exit", resolve));
  }
  rmSync(apiAmbiguityDir, { recursive: true, force: true });
}

// A rename makes the new filename observable before the directory fsync.  A
// failure after that point is ambiguous: preserve the new in-memory snapshot
// and refuse subsequent writes rather than letting stale state overwrite it.
const ambiguityPath = "/tmp/telepathos-lane-post-rename-ambiguity.json";
try { rmSync(ambiguityPath); } catch {}
process.env.TELEPATHOS_LANES = ambiguityPath;
const ambiguityRegistry = loadLanes();
const ambiguityLane = createLane(ambiguityRegistry, "ambiguous commit");
saveLanes(ambiguityRegistry);
const beforeAmbiguousSave = structuredClone(ambiguityRegistry);
let ambiguousError;
setAfterRenameHookForTests(() => { throw new Error("injected parent fsync failure"); });
try {
  mutateAndSaveLanes(ambiguityRegistry, () => switchLane(ambiguityRegistry, ambiguityLane.id));
} catch (error) {
  ambiguousError = error;
} finally {
  setAfterRenameHookForTests(null);
}
check("post-rename lane save is classified as ambiguous",
  ambiguousError instanceof LanePersistenceError && ambiguousError.phase === "post-rename");
check("post-rename failure preserves the renamed registry in memory",
  ambiguityRegistry.activeId === ambiguityLane.id && ambiguityRegistry.activeId !== beforeAmbiguousSave.activeId);
check("post-rename failure leaves the renamed registry observable on disk",
  loadLanes().activeId === ambiguityLane.id);
check("post-rename failure latches later lane writes", laneWritesUnavailableReason() !== null);
const afterAmbiguousSave = structuredClone(ambiguityRegistry);
let blockedWrite;
try {
  mutateAndSaveLanes(ambiguityRegistry, () => createLane(ambiguityRegistry, "must not persist"));
} catch (error) {
  blockedWrite = error;
}
check("latched lane write is rejected", blockedWrite instanceof LanePersistenceError && blockedWrite.phase === "unavailable");
check("latched lane write rolls back its attempted mutation",
  JSON.stringify(ambiguityRegistry) === JSON.stringify(afterAmbiguousSave));
try { rmSync(ambiguityPath); } catch {}

const toolMetaPath = "/tmp/telepathos-tool-invalid-name.json";
try { rmSync(toolMetaPath); } catch {}
process.env.TELEPATHOS_LANES = toolMetaPath;
const toolMetaRegistry = loadLanes();
writeFileSync(toolMetaPath, JSON.stringify({
  lanes: toolMetaRegistry.lanes.map((lane) => ({
    id: lane.id,
    name: lane.name,
    created_at: lane.createdAt,
    last_active: lane.lastActive,
    ...(lane.interactions !== undefined && { interactions: lane.interactions }),
  })),
  active_id: toolMetaRegistry.activeId,
  previous_id: toolMetaRegistry.previousId,
}));
for (const [label, name, expected] of invalidMetaNames) {
  const beforeMemory = structuredClone(toolMetaRegistry);
  const beforeDisk = readFileSync(toolMetaPath, "utf8");
  const reply = executeTool(toolMetaRegistry, "create_lane", { name });
  check(`meta-agent create_lane ${label} returns Rust-compatible invalid-name response`, reply === expected);
  check(`meta-agent create_lane ${label} does not mutate memory`,
    JSON.stringify(toolMetaRegistry) === JSON.stringify(beforeMemory));
  check(`meta-agent create_lane ${label} does not write the durable snapshot`,
    readFileSync(toolMetaPath, "utf8") === beforeDisk);
}
try { rmSync(toolMetaPath); } catch {}

console.log(failures === 0 ? "META TESTS PASS" : `${failures} FAILURES`);
process.exit(failures ? 1 : 0);

// 6. agent-facing lane API
process.env.TELEPATHOS_LANES = "/tmp/tp-api-lanes.json";
process.env.TELEPATHOS_PORT = "8791";
process.env.TELEPATHOS_API_PORT = "8792";
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
await post("/api/lanes/active", { id: "telepathos:direct" });
const st3 = await (await fetch(base + "/api/state")).json();
check("api: switch via tools", st3.active === "direct");
srv.kill();

// 7. steering agent tool executor (pure, no network)
import { executeTool, metaTools } from "./dist/meta-agent.js";
const reg3 = {
  lanes: [
    { id: "telepathos:direct", name: "direct", createdAt: "", lastActive: "", interactions: 5 },
    { id: "telepathos:repo:x", name: "x", createdAt: "", lastActive: "" },
  ],
  activeId: "telepathos:direct",
  previousId: "telepathos:direct",
};
check("tool: list_lanes", executeTool(reg3, "list_lanes", {}).includes("(ACTIVE)"));
check("tool: switch fuzzy", executeTool(reg3, "switch_lane", { name: "x" }).includes("now x"));
check("tool: create", executeTool(reg3, "create_lane", { name: "new thing" }).includes("Created"));
check("tool: stats", executeTool(reg3, "lane_stats", {}).includes("direct: 5 interactions"));
check("tool surface preserves the five tools and adds search", JSON.stringify(metaTools().map((tool) => tool.function.name)) === JSON.stringify([
  "list_lanes", "active_lane", "switch_lane", "create_lane", "lane_stats", "search_conversations",
]));
check("tool: search schema fallback", executeTool(reg3, "search_conversations", { query: "topic" }) === "Search is not available right now.");
check("tool: search typed execution", executeTool(reg3, "search_conversations", { query: "topic" }, (query) => `found ${query}`).includes("found topic"));
check("tool: search rejects missing query", executeTool(reg3, "search_conversations", {}) === "Argument 'query' is required.");
