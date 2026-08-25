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
import { isValidLaneId, isValidOpaqueId } from "./protocol.js";
import { currentTelepathosdTargetIdentity, isTargetIdentity } from "./target-scope.js";

export { MAX_OPAQUE_ID_LENGTH as MAX_INTERACTION_ID_LENGTH, MAX_OPAQUE_ID_BYTES as MAX_INTERACTION_ID_BYTES } from "./protocol.js";

/** A remote voice turn with a stable ID in telepathosd's seven-day dedupe window. */
export interface InteractionRecord {
  laneId: string;
  interactionId: string;
  interactionCreatedAtMs: number;
}

type RecordState = "reserved" | "pending" | "expired";
interface StoredRecord extends InteractionRecord {
  state: RecordState;
  targetIdentity: string;
}

export interface InteractionOutboxStatus {
  capacity: number;
  used: number;
  pending: number;
  reserved: number;
  expired: number;
  accepting: boolean;
}

const SNAPSHOT_VERSION = 3;
const DEFAULT_CAPACITY = 128;
let writeCounter = 0;
let failNextWriteBeforeRenameForTest = false;
let failNextPostRenameDirectorySyncForTest = false;
let writeChunkLimitForTest: number | null = null;
let directorySyncHookForTest: ((path: string) => void) | null = null;

export class InteractionOutboxFullError extends Error {}
export class InteractionOutboxBlockedError extends Error {}
/** A failed write that happened before rename, so the prior snapshot remains authoritative. */
export class InteractionOutboxRecoverablePersistenceError extends Error {}

class AtomicWriteError extends Error {
  constructor(
    path: string,
    readonly renamed: boolean,
    cause: Error,
  ) {
    super(`cannot persist interaction outbox ${path}: ${cause.message}`);
  }
}

/** Test-only deterministic fault injection for the post-rename durability boundary. */
export function failNextInteractionOutboxPostRenameDirectorySyncForTest(): void {
  failNextPostRenameDirectorySyncForTest = true;
}

/** Test-only deterministic fault injection for the rollback-safe durability boundary. */
export function failNextInteractionOutboxWriteBeforeRenameForTest(): void {
  failNextWriteBeforeRenameForTest = true;
}

/**
 * Test-only subprocess hook for the server integration regression. It lets a
 * child bridge fail exactly the next reservation cancellation before rename,
 * without exposing a production control path.
 */
function injectReservationCancellationFailureForTest(): void {
  if (process.env.TELEPATHOS_TEST_FAIL_NEXT_INTERACTION_OUTBOX_CANCEL_BEFORE_RENAME !== "1") return;
  delete process.env.TELEPATHOS_TEST_FAIL_NEXT_INTERACTION_OUTBOX_CANCEL_BEFORE_RENAME;
  failNextInteractionOutboxWriteBeforeRenameForTest();
}

/** Test-only cap for an individual write, exercising the short-write loop. */
export function setInteractionOutboxWriteChunkLimitForTest(limit: number | null): void {
  if (limit !== null && (!Number.isSafeInteger(limit) || limit < 1)) {
    throw new Error("interaction outbox test write chunk limit must be a positive integer or null");
  }
  writeChunkLimitForTest = limit;
}

/** Observe completed directory fsyncs in the focused nested-state test. */
export function setInteractionOutboxDirectorySyncHookForTest(
  hook: ((path: string) => void) | null,
): void {
  directorySyncHookForTest = hook;
}

export function defaultInteractionOutboxPath(): string {
  return `${process.env.TELEPATHOS_LANES ?? "lanes.json"}.interaction-outbox.json`;
}

export function interactionOutboxCapacityFromEnvironment(): number {
  const raw = process.env.TELEPATHOS_INTERACTION_OUTBOX_MAX;
  if (raw === undefined || raw === "") return DEFAULT_CAPACITY;
  const capacity = Number(raw);
  if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 10_000) {
    throw new Error("TELEPATHOS_INTERACTION_OUTBOX_MAX must be an integer from 1 through 10000");
  }
  return capacity;
}

function recordKey(record: InteractionRecord): string {
  return `${record.laneId}\u0000${record.interactionId}`;
}

function validateRecord(value: unknown, path: string, index: number): StoredRecord {
  if (value === null || typeof value !== "object") {
    throw new Error(`invalid interaction outbox ${path}: malformed entry at index ${index}`);
  }
  const record = value as Record<string, unknown>;
  if (!isTargetIdentity(record.target_identity) ||
      !isValidLaneId(record.lane_id) ||
      !isValidOpaqueId(record.interaction_id) ||
      !Number.isSafeInteger(record.interaction_created_at_ms) ||
      (record.interaction_created_at_ms as number) < 0 ||
      (record.state !== "reserved" && record.state !== "pending" && record.state !== "expired")) {
    throw new Error(`invalid interaction outbox ${path}: malformed entry at index ${index}`);
  }
  return {
    targetIdentity: record.target_identity,
    laneId: record.lane_id,
    interactionId: record.interaction_id,
    interactionCreatedAtMs: record.interaction_created_at_ms as number,
    state: record.state,
  };
}

function load(path: string, capacity: number, targetIdentity: string): StoredRecord[] {
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw new Error(`cannot read interaction outbox ${path}: ${(error as Error).message}`);
  }
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (error) {
    throw new Error(`corrupt interaction outbox ${path}: ${(error as Error).message}`);
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(`invalid interaction outbox ${path}: expected v${SNAPSHOT_VERSION} snapshot object`);
  }
  const snapshot = raw as { version?: unknown; records?: unknown };
  if (snapshot.version !== SNAPSHOT_VERSION || !Array.isArray(snapshot.records)) {
    throw new Error(`invalid interaction outbox ${path}: expected v${SNAPSHOT_VERSION} snapshot object`);
  }
  // Reject before map() so an oversized snapshot cannot be retained in memory.
  if (snapshot.records.length > capacity) {
    throw new InteractionOutboxFullError(
      `invalid interaction outbox ${path}: snapshot has ${snapshot.records.length} records but capacity is ${capacity}`,
    );
  }
  const seen = new Set<string>();
  return snapshot.records.map((entry, index) => {
    const record = validateRecord(entry, path, index);
    if (record.targetIdentity !== targetIdentity) {
      throw new Error(`invalid interaction outbox ${path}: target identity mismatch at index ${index}`);
    }
    const key = recordKey(record);
    if (seen.has(key)) {
      throw new Error(`invalid interaction outbox ${path}: duplicate entry at index ${index}`);
    }
    seen.add(key);
    return record;
  });
}

function syncDirectory(path: string): void {
  const dirFd = openSync(path, "r");
  try {
    fsyncSync(dirFd);
  } finally {
    closeSync(dirFd);
  }
  directorySyncHookForTest?.(path);
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

function atomicWrite(path: string, contents: string): void {
  const parent = dirname(path);
  const temp = `${path}.tmp-${process.pid}-${Date.now()}-${++writeCounter}`;
  let fd: number | undefined;
  let renamed = false;
  try {
    mkdirWithDurableParents(parent);
    fd = openSync(temp, "wx", 0o600);
    if (failNextWriteBeforeRenameForTest) {
      failNextWriteBeforeRenameForTest = false;
      throw new Error("injected pre-rename write failure");
    }
    writeAll(fd, contents);
    fsyncSync(fd);
    closeSync(fd);
    fd = undefined;
    renameSync(temp, path);
    renamed = true;
    if (failNextPostRenameDirectorySyncForTest) {
      failNextPostRenameDirectorySyncForTest = false;
      throw new Error("injected post-rename directory fsync failure");
    }
    syncDirectory(parent);
  } catch (error) {
    if (fd !== undefined) closeSync(fd);
    if (!renamed) {
      try { unlinkSync(temp); } catch {}
    }
    throw new AtomicWriteError(path, renamed, error as Error);
  }
}

/** `writeSync` may legally return fewer bytes than requested. */
function writeAll(fd: number, contents: string): void {
  const bytes = Buffer.from(contents, "utf8");
  let offset = 0;
  while (offset < bytes.length) {
    const remaining = bytes.length - offset;
    const requested = writeChunkLimitForTest === null
      ? remaining
      : Math.min(remaining, writeChunkLimitForTest);
    const written = writeSync(fd, bytes, offset, requested, null);
    if (!Number.isSafeInteger(written) || written < 1 || written > requested) {
      throw new Error("short write while persisting interaction outbox");
    }
    offset += written;
  }
}

function serialize(records: StoredRecord[]): string {
  return JSON.stringify({
    version: SNAPSHOT_VERSION,
    records: records.map((record) => ({
      lane_id: record.laneId,
      target_identity: record.targetIdentity,
      interaction_id: record.interactionId,
      interaction_created_at_ms: record.interactionCreatedAtMs,
      state: record.state,
    })),
  }, null, 2);
}

function copy(record: InteractionRecord): InteractionRecord {
  return {
    laneId: record.laneId,
    interactionId: record.interactionId,
    interactionCreatedAtMs: record.interactionCreatedAtMs,
  };
}

/**
 * Bounded durable queue. A capture reserves a slot before it starts; successful
 * STT promotes that slot before any remote side effect. On restart, only
 * reservations are reclaimed because they cannot represent a completed turn.
 */
export class InteractionOutbox {
  private readonly path: string;
  private readonly capacity: number;
  private readonly targetIdentity: string;
  private records: StoredRecord[];
  /**
   * A cancellation whose snapshot delete failed before rename. The reserved
   * row is still authoritative, but it must be retried before another slot is
   * admitted. Reserved rows have not been promoted, so retrying their delete
   * cannot duplicate a remote side effect.
   */
  private readonly pendingReservationCleanups = new Map<string, InteractionRecord>();
  private persistenceFailure: string | null = null;

  constructor(path = defaultInteractionOutboxPath(), capacity = interactionOutboxCapacityFromEnvironment()) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 10_000) {
      throw new Error("interaction outbox capacity must be an integer from 1 through 10000");
    }
    this.path = path;
    this.capacity = capacity;
    this.targetIdentity = currentTelepathosdTargetIdentity();
    this.records = load(path, capacity, this.targetIdentity);
    // Promotion is durably persisted before Hermes/telepathosd is called, so
    // no reserved row has a remote side effect to replay or count.
    if (this.records.some((record) => record.state === "reserved")) {
      this.records = this.records.filter((record) => record.state !== "reserved");
      atomicWrite(this.path, serialize(this.records));
    }
  }

  pending(): InteractionRecord[] {
    this.assertCurrentTarget();
    return this.records.filter((record) => record.state === "pending").map(copy);
  }

  targetScope(): string { return this.targetIdentity; }

  assertCurrentTarget(): void {
    if (currentTelepathosdTargetIdentity() !== this.targetIdentity) {
      throw new InteractionOutboxBlockedError(
        `interaction outbox ${this.path}: telepathosd target identity changed; durable interactions remain pending`,
      );
    }
  }

  status(): InteractionOutboxStatus {
    let pending = 0;
    let reserved = 0;
    let expired = 0;
    for (const record of this.records) {
      if (record.state === "pending") pending += 1;
      else if (record.state === "reserved") reserved += 1;
      else expired += 1;
    }
    return {
      capacity: this.capacity,
      used: this.records.length,
      pending,
      reserved,
      expired,
      accepting: this.persistenceFailure === null &&
        currentTelepathosdTargetIdentity() === this.targetIdentity &&
        expired === 0 && this.records.length < this.capacity,
    };
  }

  unavailableReason(): string | null {
    if (this.persistenceFailure !== null) return this.persistenceFailure;
    if (currentTelepathosdTargetIdentity() !== this.targetIdentity) {
      return "telepathosd target identity changed; durable interactions remain pending until the original target is restored";
    }
    const status = this.status();
    if (status.expired > 0) {
      return `${status.expired} remote interaction record(s) exceeded telepathosd's seven-day retry window; operator reconciliation is required`;
    }
    if (status.used >= status.capacity) {
      return `remote interaction outbox is full (${status.used}/${status.capacity}); remote turns are paused until telepathosd records activity`;
    }
    return null;
  }

  reserve(record: InteractionRecord): void {
    this.validateInput(record);
    this.assertCurrentTarget();
    this.requirePersistenceHealthy();
    this.sweepPendingReservationCleanups();
    const existing = this.find(record);
    if (existing !== undefined) {
      this.requireSame(existing, record);
      return;
    }
    const reason = this.unavailableReason();
    if (reason !== null) {
      if (this.status().expired > 0) throw new InteractionOutboxBlockedError(reason);
      throw new InteractionOutboxFullError(reason);
    }
    this.persist([...this.records, { ...record, targetIdentity: this.targetIdentity, state: "reserved" }]);
  }

  promote(record: InteractionRecord): void {
    this.validateInput(record);
    this.assertCurrentTarget();
    this.requirePersistenceHealthy();
    if (this.pendingReservationCleanups.has(recordKey(record))) {
      throw new InteractionOutboxBlockedError(
        `interaction outbox ${this.path}: reservation cleanup is pending before promotion`,
      );
    }
    const index = this.indexOf(record);
    if (index === -1) throw new Error(`interaction outbox ${this.path}: cannot promote an unreserved interaction`);
    const existing = this.records[index];
    this.requireSame(existing, record);
    if (existing.state === "pending") return;
    if (existing.state === "expired") throw new InteractionOutboxBlockedError("interaction retry expired");
    const next = [...this.records];
    next[index] = { ...existing, state: "pending" };
    this.persist(next);
  }

  cancelReservation(record: InteractionRecord): void {
    this.assertCurrentTarget();
    this.requirePersistenceHealthy();
    const index = this.indexOf(record);
    if (index === -1) return;
    const existing = this.records[index];
    this.requireSame(existing, record);
    if (existing.state !== "reserved") return;
    try {
      injectReservationCancellationFailureForTest();
      this.persist(this.records.filter((_, current) => current !== index));
      this.pendingReservationCleanups.delete(recordKey(record));
    } catch (error) {
      if (error instanceof InteractionOutboxRecoverablePersistenceError) {
        this.pendingReservationCleanups.set(recordKey(record), copy(record));
      }
      throw error;
    }
  }

  removeDelivered(record: InteractionRecord): void {
    this.assertCurrentTarget();
    this.requirePersistenceHealthy();
    const index = this.indexOf(record);
    if (index === -1) return;
    const existing = this.records[index];
    this.requireSame(existing, record);
    if (existing.state !== "pending") {
      throw new Error(`interaction outbox ${this.path}: cannot remove a ${existing.state} interaction record`);
    }
    this.persist(this.records.filter((_, current) => current !== index));
  }

  markExpired(record: InteractionRecord): void {
    this.assertCurrentTarget();
    this.requirePersistenceHealthy();
    const index = this.indexOf(record);
    if (index === -1) return;
    const existing = this.records[index];
    this.requireSame(existing, record);
    if (existing.state === "expired") return;
    if (existing.state !== "pending") {
      throw new Error(`interaction outbox ${this.path}: cannot expire a ${existing.state} interaction record`);
    }
    const next = [...this.records];
    next[index] = { ...existing, state: "expired" };
    this.persist(next);
  }

  private validateInput(record: InteractionRecord): void {
    validateRecord({
      target_identity: this.targetIdentity,
      lane_id: record.laneId,
      interaction_id: record.interactionId,
      interaction_created_at_ms: record.interactionCreatedAtMs,
      state: "reserved",
    }, this.path, 0);
  }

  private find(record: InteractionRecord): StoredRecord | undefined {
    return this.records.find((current) => recordKey(current) === recordKey(record));
  }

  private indexOf(record: InteractionRecord): number {
    return this.records.findIndex((current) => recordKey(current) === recordKey(record));
  }

  private requireSame(existing: StoredRecord, incoming: InteractionRecord): void {
    if (existing.interactionCreatedAtMs !== incoming.interactionCreatedAtMs) {
      throw new Error(`interaction outbox ${this.path}: interaction ID was reused with a different timestamp`);
    }
  }

  private requirePersistenceHealthy(): void {
    if (this.persistenceFailure !== null) {
      throw new InteractionOutboxBlockedError(this.persistenceFailure);
    }
  }

  private sweepPendingReservationCleanups(): void {
    for (const [key, record] of this.pendingReservationCleanups) {
      const index = this.indexOf(record);
      if (index === -1) {
        this.pendingReservationCleanups.delete(key);
        continue;
      }
      const existing = this.records[index];
      this.requireSame(existing, record);
      if (existing.state !== "reserved") {
        throw new InteractionOutboxBlockedError(
          `interaction outbox ${this.path}: canceled reservation is no longer reserved`,
        );
      }
      this.persist(this.records.filter((_, current) => current !== index));
      this.pendingReservationCleanups.delete(key);
    }
  }

  private persist(next: StoredRecord[]): void {
    const previous = this.records;
    this.records = next;
    try {
      atomicWrite(this.path, serialize(this.records));
    } catch (error) {
      if (error instanceof AtomicWriteError && error.renamed) {
        this.persistenceFailure = "remote interaction outbox durability is uncertain after a post-rename persistence failure; remote turns are paused until operator reconciliation is complete";
        throw new InteractionOutboxBlockedError(this.persistenceFailure);
      }
      this.records = previous;
      if (error instanceof AtomicWriteError) {
        throw new InteractionOutboxRecoverablePersistenceError(error.message);
      }
      throw error;
    }
  }
}
