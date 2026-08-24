import {
  closeSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeSync,
} from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { isValidLaneId, MAX_SAFE_SEQUENCE } from "./protocol.js";

/**
 * Conversation lanes: the registry of parallel conversations the user can
 * switch between. Persisted to lanes.json next to the process cwd.
 *
 * A lane maps 1:1 to a Hermes session lane via chat_id (relay contract):
 *   "telepathy:direct"        → talking to Hermes himself, no project
 *   "telepathy:repo:<name>"   → per-project conversation
 */

export interface Lane {
  id: string;          // stable id, doubles as hermes chat_id
  name: string;        // spoken/display name ("kerchunk")
  createdAt: string;
  lastActive: string;
  interactions?: number; // lifetime voice-interaction count (for stats tools)
}

export interface LaneRegistry {
  lanes: Lane[];
  activeId: string;
  previousId: string;  // for "switch back"
}

/**
 * Shared with `telepathy-lanes::MAX_LANE_COUNT`.
 *
 * A complete state response for 256 normal maximum-length generated lanes,
 * including `pending`, `active`, and `revision`, is under 128 KiB. That is an
 * eightfold margin below the 1 MiB Node ↔ telepathyd state transport cap.
 */
export const MAX_LANE_COUNT = 256;

/**
 * Shared with `telepathy-lanes`.  These caps bound both durable snapshots and
 * the `/api/state` envelope without altering the semantic value of an
 * admitted string.  Rust additionally calls out the same scalar-value limit;
 * Node checks it too so a malformed UTF-16 string cannot split the contract.
 */
export const MAX_LANE_NAME_UTF8_BYTES = 128;
export const MAX_LANE_NAME_UTF16_CODE_UNITS = 128;
export const MAX_LANE_NAME_CODEPOINTS = 128;
export const MAX_LANE_TIMESTAMP_UTF8_BYTES = 64;
export const MAX_LANE_TIMESTAMP_UTF16_CODE_UNITS = 64;
export const MAX_LANE_TIMESTAMP_CODEPOINTS = 64;
/** Shared with telepathyd's external-state enrichment, never persisted. */
export const MAX_ENRICHED_LANE_TITLE_UTF8_BYTES = 256;
export const MAX_ENRICHED_LANE_TITLE_UTF16_CODE_UNITS = 128;
export const MAX_ENRICHED_LANE_TITLE_CODEPOINTS = 128;

/** Stable, permanent caller error: choose an existing conversation instead. */
export const LANE_CAPACITY_ERROR_MESSAGE = "lane capacity reached; use an existing conversation";

const DEFAULT_REGISTRY: LaneRegistry = {
  lanes: [
    {
      id: "telepathy:direct",
      name: "direct",
      createdAt: new Date().toISOString(),
      lastActive: new Date().toISOString(),
    },
  ],
  activeId: "telepathy:direct",
  previousId: "telepathy:direct",
};

let saveCounter = 0;
let lanePersistenceUncertain: LanePersistenceError | null = null;
let afterRenameHookForTests: (() => void) | null = null;
let directorySyncHookForTests: ((path: string) => void) | null = null;
// These revisions intentionally live only in process memory: the durable
// registry format remains canonical and shared with telepathyd.  They let a
// long-running private meta-model proposal distinguish no selection change
// from an ABA switch (A -> B -> A) that has the same final snapshot.
const selectionRevisions = new WeakMap<LaneRegistry, number>();

export function laneSelectionRevision(reg: LaneRegistry): number {
  return selectionRevisions.get(reg) ?? 0;
}

function advanceLaneSelectionRevision(reg: LaneRegistry, before: LaneRegistry): void {
  if (reg.activeId !== before.activeId || reg.previousId !== before.previousId) {
    selectionRevisions.set(reg, laneSelectionRevision(reg) + 1);
  }
}

/**
 * A lane save either failed before the authoritative filename changed, or it
 * failed after rename while making that change durable.  The latter cannot be
 * rolled back safely: another process (or a later restart) may observe the
 * new snapshot even though the caller received an error.
 */
export class LanePersistenceError extends Error {
  constructor(
    readonly phase: "pre-rename" | "post-rename" | "unavailable",
    message: string,
  ) {
    super(message);
    this.name = "LanePersistenceError";
  }
}

/** Invalid caller input for a new lane. It is rejected before a registry can mutate. */
export class LaneNameError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LaneNameError";
  }
}

/**
 * Deliberately extends LaneNameError so existing direct/meta callers already
 * handle it as a spoken, permanent client result without a retry loop.
 */
export class LaneCapacityError extends LaneNameError {
  constructor() {
    super(LANE_CAPACITY_ERROR_MESSAGE);
    this.name = "LaneCapacityError";
  }
}

export function isAmbiguousLanePersistenceFailure(error: unknown): boolean {
  return error instanceof LanePersistenceError && error.phase === "post-rename";
}

/**
 * Once a rename has happened but syncing its parent directory has failed, a
 * later save from stale process memory could erase the renamed snapshot.
 * Keep the process read-only until an operator restarts or reconciles it.
 */
export function laneWritesUnavailableReason(): string | null {
  return lanePersistenceUncertain?.message ?? null;
}

/** Deterministic fault seam used only by the focused persistence regression. */
export function setAfterRenameHookForTests(hook: (() => void) | null): void {
  afterRenameHookForTests = hook;
}

/** Observe completed directory fsyncs in the focused nested-state test. */
export function setLaneDirectorySyncHookForTests(
  hook: ((path: string) => void) | null,
): void {
  directorySyncHookForTests = hook;
}

type BufferWriter = (
  fd: number,
  buffer: Buffer,
  offset: number,
  length: number,
  position: null,
) => number;

function writeBuffer(
  fd: number,
  buffer: Buffer,
  offset: number,
  length: number,
  position: null,
): number {
  return writeSync(fd, buffer, offset, length, position);
}

/** Write a complete UTF-8 snapshot before it is fsynced and renamed. */
export function writeAll(fd: number, contents: string, writer: BufferWriter = writeBuffer): void {
  const buffer = Buffer.from(contents, "utf8");
  let offset = 0;
  while (offset < buffer.length) {
    const remaining = buffer.length - offset;
    const written = writer(fd, buffer, offset, remaining, null);
    if (!Number.isInteger(written) || written <= 0 || written > remaining) {
      throw new Error(`lane registry write made invalid progress: ${written}`);
    }
    offset += written;
  }
}

function syncDirectory(path: string): void {
  const dirFd = openSync(path, "r");
  try {
    fsyncSync(dirFd);
  } finally {
    closeSync(dirFd);
  }
  directorySyncHookForTests?.(path);
}

/**
 * Recursively create a snapshot parent and sync each newly created directory's
 * parent before a file can be reported durable.  Node returns the first path
 * created by recursive mkdir; walk from there to the requested parent.
 */
function mkdirWithDurableParents(parent: string): void {
  const firstCreated = mkdirSync(parent, { recursive: true });
  if (firstCreated === undefined) return;

  const first = resolve(firstCreated);
  const target = resolve(parent);
  const remainder = relative(first, target);
  const components = remainder === "" ? [] : remainder.split(sep);
  if (components.some((component) => component === "" || component === "." || component === "..")) {
    throw new Error(`recursive mkdir returned a non-ancestor path for ${parent}`);
  }

  let current = first;
  syncDirectory(dirname(current));
  for (const component of components) {
    current = join(current, component);
    syncDirectory(dirname(current));
  }
}

function parseRegistry(raw: any, path: string): LaneRegistry {
  if (!Array.isArray(raw?.lanes) || raw.lanes.length === 0 ||
      typeof raw.active_id !== "string" || raw.active_id.length === 0 ||
      typeof raw.previous_id !== "string" || raw.previous_id.length === 0) {
    throw new Error(`invalid lane registry ${path}: expected canonical snake_case schema`);
  }
  if (raw.lanes.length > MAX_LANE_COUNT) {
    throw new Error(
      `invalid lane registry ${path}: lane count exceeds the ${MAX_LANE_COUNT} lane limit`,
    );
  }

  const ids = new Set<string>();
  const lanes = raw.lanes.map((lane: any, index: number): Lane => {
    if (lane === null || typeof lane !== "object" ||
        !isValidLaneId(lane.id) ||
        !isValidPersistedLaneName(lane.name) ||
        !isValidLaneTimestamp(lane.created_at) ||
        !isValidLaneTimestamp(lane.last_active)) {
      throw new Error(`invalid lane registry ${path}: malformed lane at index ${index}`);
    }
    if (ids.has(lane.id)) {
      throw new Error(`invalid lane registry ${path}: duplicate lane id ${lane.id}`);
    }
    ids.add(lane.id);
    if (lane.interactions !== undefined &&
        (!Number.isSafeInteger(lane.interactions) || lane.interactions < 0)) {
      throw new Error(`invalid lane registry ${path}: malformed interactions for ${lane.id}`);
    }
    return {
      id: lane.id,
      name: lane.name,
      createdAt: lane.created_at,
      lastActive: lane.last_active,
      ...(lane.interactions !== undefined && { interactions: lane.interactions }),
    };
  });
  if (!ids.has(raw.active_id) || !ids.has(raw.previous_id)) {
    throw new Error(`invalid lane registry ${path}: active/previous lane is missing`);
  }
  return {
    lanes,
    activeId: raw.active_id,
    previousId: raw.previous_id,
  };
}

function registryPath(): string {
  return process.env.TELEPATHY_LANES ?? "lanes.json";
}

function isWellFormedUtf16(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function codePointCount(value: string): number {
  let count = 0;
  for (const _ of value) count += 1;
  return count;
}

function isWithinStringCaps(
  value: string,
  maxUtf8Bytes: number,
  maxUtf16CodeUnits: number,
  maxCodePoints: number,
): boolean {
  return value.length <= maxUtf16CodeUnits &&
    isWellFormedUtf16(value) &&
    Buffer.byteLength(value, "utf8") <= maxUtf8Bytes &&
    codePointCount(value) <= maxCodePoints;
}

/** A persisted lane name is nonempty and bounded, but never trimmed or normalized. */
export function isValidPersistedLaneName(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 &&
    isWithinStringCaps(
      value,
      MAX_LANE_NAME_UTF8_BYTES,
      MAX_LANE_NAME_UTF16_CODE_UNITS,
      MAX_LANE_NAME_CODEPOINTS,
    );
}

function isAsciiDigits(value: string): boolean {
  return value.length > 0 && /^[0-9]+$/.test(value);
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}

function parseFixedDigits(value: string, start: number, length: number): number | null {
  const part = value.slice(start, start + length);
  return /^[0-9]+$/.test(part) ? Number(part) : null;
}

/**
 * Both current registry writers have a stable timestamp spelling: Node writes
 * canonical UTC millisecond ISO strings and telepathyd writes `epoch-ms:`
 * plus a JSON-safe integer.  Keep both exact spellings readable because they
 * are already authoritative snapshots, but do not coerce or repair either.
 */
export function isValidLaneTimestamp(value: unknown): value is string {
  if (typeof value !== "string" || !isWithinStringCaps(
    value,
    MAX_LANE_TIMESTAMP_UTF8_BYTES,
    MAX_LANE_TIMESTAMP_UTF16_CODE_UNITS,
    MAX_LANE_TIMESTAMP_CODEPOINTS,
  )) return false;

  if (value.startsWith("epoch-ms:")) {
    const milliseconds = value.slice("epoch-ms:".length);
    return milliseconds.length <= 16 && isAsciiDigits(milliseconds) &&
      BigInt(milliseconds) <= BigInt(MAX_SAFE_SEQUENCE);
  }

  if (value.length !== 24 || value[4] !== "-" || value[7] !== "-" ||
      value[10] !== "T" || value[13] !== ":" || value[16] !== ":" ||
      value[19] !== "." || value[23] !== "Z") return false;
  const year = parseFixedDigits(value, 0, 4);
  const month = parseFixedDigits(value, 5, 2);
  const day = parseFixedDigits(value, 8, 2);
  const hour = parseFixedDigits(value, 11, 2);
  const minute = parseFixedDigits(value, 14, 2);
  const second = parseFixedDigits(value, 17, 2);
  const millisecond = parseFixedDigits(value, 20, 3);
  if (year === null || month === null || day === null || hour === null ||
      minute === null || second === null || millisecond === null ||
      year === 0 || month < 1 || month > 12 || hour > 23 || minute > 59 ||
      second > 59 || millisecond > 999) return false;
  const daysInMonth = [31, isLeapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return day >= 1 && day <= daysInMonth[month - 1];
}

export function loadLanes(): LaneRegistry {
  const path = registryPath();
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") {
      return structuredClone(DEFAULT_REGISTRY);
    }
    throw new Error(`cannot read lane registry ${path}: ${(e as Error).message}`);
  }

  let raw: any;
  try {
    raw = JSON.parse(text);
  } catch (e) {
    throw new Error(`corrupt lane registry ${path}: ${(e as Error).message}`);
  }
  return parseRegistry(raw, path);
}

export function saveLanes(reg: LaneRegistry): void {
  const path = registryPath();
  if (lanePersistenceUncertain !== null) {
    throw new LanePersistenceError(
      "unavailable",
      `lane registry ${path} has an unresolved post-rename persistence failure; restart or reconcile before writing again`,
    );
  }
  // Reject malformed in-memory state before it can become the next startup's
  // authoritative registry. This is a hard-cutover wire format.
  parseRegistry({
    lanes: reg.lanes.map((lane) => ({
      id: lane.id,
      name: lane.name,
      created_at: lane.createdAt,
      last_active: lane.lastActive,
      ...(lane.interactions !== undefined && { interactions: lane.interactions }),
    })),
    active_id: reg.activeId,
    previous_id: reg.previousId,
  }, path);
  // Rust telepathyd is the other registry owner; keep one canonical wire
  // shape on disk so either process can load the same file after restart.
  const json = JSON.stringify({
    lanes: reg.lanes.map((lane) => ({
      id: lane.id,
      name: lane.name,
      created_at: lane.createdAt,
      last_active: lane.lastActive,
      ...(lane.interactions !== undefined && { interactions: lane.interactions }),
    })),
    active_id: reg.activeId,
    previous_id: reg.previousId,
  }, null, 2);
  const temp = `${path}.tmp-${process.pid}-${Date.now()}-${++saveCounter}`;
  let fd: number | undefined;
  let renamed = false;
  try {
    mkdirWithDurableParents(dirname(path));
    fd = openSync(temp, "wx", 0o600);
    writeAll(fd, json);
    fsyncSync(fd);
    closeSync(fd);
    fd = undefined;
    renameSync(temp, path);
    renamed = true;
    afterRenameHookForTests?.();
    syncDirectory(dirname(path));
  } catch (e) {
    if (fd !== undefined) {
      try { closeSync(fd); } catch {}
    }
    try { unlinkSync(temp); } catch {}
    const error = new LanePersistenceError(
      renamed ? "post-rename" : "pre-rename",
      `cannot persist lane registry ${path}: ${(e as Error).message}`,
    );
    if (renamed) lanePersistenceUncertain = error;
    throw error;
  }
}

function restoreRegistry(reg: LaneRegistry, snapshot: LaneRegistry): void {
  // The bridge and API deliberately share this registry object.  Preserve the
  // object and array identities while rolling back a definitely-uncommitted
  // mutation.
  reg.lanes.splice(0, reg.lanes.length, ...snapshot.lanes.map((lane) => ({ ...lane })));
  reg.activeId = snapshot.activeId;
  reg.previousId = snapshot.previousId;
}

/**
 * Mutate the shared registry only transactionally.  Pre-rename failures leave
 * the old snapshot authoritative, while post-rename failures preserve the
 * new in-memory value and permanently block later writes in this process.
 */
export function mutateAndSaveLanes<T>(reg: LaneRegistry, mutate: () => T): T {
  const snapshot = structuredClone(reg);
  try {
    const result = mutate();
    // A rejected direct/meta create is intentionally a no-op. Avoid rewriting
    // a durable snapshot just to report its permanent admission error.
    if (JSON.stringify(reg) !== JSON.stringify(snapshot)) {
      saveLanes(reg);
      advanceLaneSelectionRevision(reg, snapshot);
    }
    return result;
  } catch (error) {
    if (!isAmbiguousLanePersistenceFailure(error)) restoreRegistry(reg, snapshot);
    throw error;
  }
}

export function activeLane(reg: LaneRegistry): Lane {
  return reg.lanes.find((l) => l.id === reg.activeId) ?? reg.lanes[0];
}

export function touchLane(reg: LaneRegistry, id: string): void {
  if (!isValidLaneId(id)) throw new Error("invalid lane id");
  const lane = reg.lanes.find((l) => l.id === id);
  if (!lane) throw new Error(`unknown lane ${id}`);
  lane.lastActive = new Date().toISOString();
}

/**
 * Rust's `str::trim()` whitespace set.  Do not use JavaScript's `trim()`:
 * it has a different definition (notably it accepts U+FEFF), and lane names
 * have to produce the same result under either registry owner.
 */
function isRustTrimWhitespace(codePoint: number): boolean {
  return (codePoint >= 0x0009 && codePoint <= 0x000d) ||
    codePoint === 0x0020 ||
    codePoint === 0x0085 ||
    codePoint === 0x00a0 ||
    codePoint === 0x1680 ||
    (codePoint >= 0x2000 && codePoint <= 0x200a) ||
    (codePoint >= 0x2028 && codePoint <= 0x2029) ||
    codePoint === 0x202f ||
    codePoint === 0x205f ||
    codePoint === 0x3000;
}

function isRustBlankLaneName(name: string): boolean {
  let sawCodePoint = false;
  for (const character of name) {
    sawCodePoint = true;
    if (!isRustTrimWhitespace(character.codePointAt(0)!)) return false;
  }
  return sawCodePoint;
}

function laneIdentityForName(name: unknown): { id: string; slug: string } {
  if (typeof name !== "string") {
    throw new LaneNameError("lane name must be a string");
  }
  if (isRustBlankLaneName(name)) {
    throw new LaneNameError("lane name must not be blank");
  }

  // Mirror Rust exactly: lower-case first, replace every non-ASCII
  // alphanumeric code point (without collapsing runs), then trim edge dashes.
  const slug = [...name.toLowerCase()]
    .map((character) => /[a-z0-9]/.test(character) ? character : "-")
    .join("")
    .replace(/^-+|-+$/g, "");
  const id = `telepathy:repo:${slug}`;
  if (!isValidLaneId(id)) {
    throw new LaneNameError("lane name is too long to produce a valid lane identifier");
  }
  return { id, slug };
}

/** Return the deterministic invalid-input error without mutating or persisting a registry. */
export function laneNameValidationError(name: unknown): LaneNameError | null {
  try {
    laneIdentityForName(name);
    return null;
  } catch (error) {
    if (error instanceof LaneNameError) return error;
    throw error;
  }
}

/** Validate an untrusted lane name without mutating or checking durability. */
export function isValidLaneName(name: unknown): name is string {
  return laneNameValidationError(name) === null;
}

export function createLane(reg: LaneRegistry, name: unknown): Lane {
  const { id, slug } = laneIdentityForName(name);
  const existing = reg.lanes.find((l) => l.id === id);
  if (existing) return existing;
  if (reg.lanes.length >= MAX_LANE_COUNT) throw new LaneCapacityError();
  const lane: Lane = {
    id,
    name: slug,
    createdAt: new Date().toISOString(),
    lastActive: new Date().toISOString(),
  };
  reg.lanes.push(lane);
  return lane;
}

export function switchLane(reg: LaneRegistry, id: string): Lane {
  if (!isValidLaneId(id)) throw new Error("invalid lane id");
  const lane = reg.lanes.find((l) => l.id === id);
  if (!lane) throw new Error(`unknown lane ${id}`);
  if (reg.activeId !== id) {
    reg.previousId = reg.activeId;
    reg.activeId = id;
  }
  touchLane(reg, id);
  return lane;
}
