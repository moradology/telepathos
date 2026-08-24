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
import {
  isValidInstallationId,
  isValidLaneId,
  isValidOpaqueId,
  isValidReceiptSequence,
  isValidTurnToken,
} from "./protocol.js";
import { MAX_REPLY_TEXT_BYTES } from "./reply-text.js";
import {
  currentTelepathydTargetIdentity,
  isTargetIdentity,
} from "./target-scope.js";

export interface ReplyAckBinding {
  /** Hash of the normalized telepathyd URL and effective auth configuration. */
  targetIdentity: string;
  /** Stable opaque owner from hello.installation_id; never a device label. */
  installationId: string;
  laneId: string;
  replyTo: string;
  afterSeq: number;
  throughSeq: number;
  turnToken: string;
  interactionId: string;
  /**
   * Complete text in the agent_end replay envelope.  A prepared binding is
   * therefore safe to resend after a process or socket loss; it never relies
   * on local transport enqueue as proof of handset receipt.
   */
  replyText: string;
  /** prepared -> received -> consumed -> removed */
  state: "prepared" | "received" | "consumed";
  /** Wall-clock time at which this replay envelope was prepared. */
  preparedAtMs: number;
  /** Durable last-seen time for the installation currently owning this binding. */
  ownerLastSeenAtMs: number;
  /** Wall-clock time at which the handset proved local receipt, if any. */
  receivedAtMs: number | null;
  /** Wall-clock time at which telepathyd consumption completed, if any. */
  consumedAtMs: number | null;
}

/** Exact terminal identity retained after a consumed binding is reclaimed. */
export interface ReplyAckTombstone {
  targetIdentity: string;
  installationId: string;
  laneId: string;
  replyTo: string;
  afterSeq: number;
  throughSeq: number;
  turnToken: string;
  interactionId: string;
  consumedAtMs: number;
  tombstonedAtMs: number;
}

/** Outstanding handset receipts are bounded; callers must fail closed at this limit. */
export const MAX_STORED_REPLY_ACKS = 64;
/** Tombstones have their own bound so reclaiming an active slot cannot deadlock capacity. */
export const MAX_STORED_REPLY_ACK_TOMBSTONES = 64;

export class ReplyAckStoreCapacityError extends Error {}

/**
 * The replacement snapshot was renamed into place, but its directory entry
 * could not be synced. The caller cannot know whether a crash would retain
 * the replacement, so subsequent writes must not overwrite that ambiguity.
 */
export class ReplyAckStorePostRenameError extends Error {}

/** The store has an unresolved post-rename durability ambiguity. */
export class ReplyAckStoreUnavailableError extends Error {}

let writeCounter = 0;
let failNextPreRenameWriteForTests = false;
let failNextPostRenameDirectorySyncForTests = false;
let shortWriteChunkSizeForTests: number | null = null;
let directorySyncHookForTests: ((path: string) => void) | null = null;

/** Deterministic fault injection for the focused persistence regression test. */
export function failNextReplyAckStoreWriteBeforeRenameForTests(): void {
  failNextPreRenameWriteForTests = true;
}

/** Deterministic fault injection after rename but before parent-directory fsync. */
export function failNextReplyAckStoreDirectorySyncAfterRenameForTests(): void {
  failNextPostRenameDirectorySyncForTests = true;
}

/**
 * Makes the next snapshot write use short successful writeSync calls. This
 * exercises the write-all loop without relying on filesystem-specific short
 * write behavior.
 */
export function useShortWritesForNextReplyAckStoreWriteForTests(maxBytes: number): void {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new Error("reply-ack test write chunk size must be a positive integer");
  }
  shortWriteChunkSizeForTests = maxBytes;
}

/** Observe completed directory fsyncs in the focused nested-state test. */
export function setReplyAckStoreDirectorySyncHookForTests(
  hook: ((path: string) => void) | null,
): void {
  directorySyncHookForTests = hook;
}

function defaultPath(): string {
  return `${process.env.TELEPATHY_LANES ?? "lanes.json"}.reply-ack-bindings.json`;
}

function key(binding: Pick<ReplyAckBinding, "laneId" | "replyTo" | "afterSeq" | "throughSeq">): string {
  return `${binding.laneId}\u0000${binding.replyTo}\u0000${binding.afterSeq}\u0000${binding.throughSeq}`;
}

function isValidTimestamp(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

/** Keep each recovered WebSocket envelope well below the 1 MiB frame ceiling. */
export const MAX_REPLY_REPLAY_TEXT_BYTES = MAX_REPLY_TEXT_BYTES;

function parse(value: unknown, path: string, index: number): ReplyAckBinding {
  if (value === null || typeof value !== "object") {
    throw new Error(`invalid reply-ack store ${path}: malformed entry at index ${index}`);
  }
  const item = value as Record<string, unknown>;
  if (!isTargetIdentity(item.target_identity) ||
      !isValidLaneId(item.lane_id) ||
      !isValidOpaqueId(item.reply_to) ||
      !isValidOpaqueId(item.interaction_id) ||
      !isValidInstallationId(item.installation_id) ||
      !isValidTurnToken(item.turn_token) ||
      typeof item.reply_text !== "string" || Buffer.byteLength(item.reply_text, "utf8") > MAX_REPLY_REPLAY_TEXT_BYTES ||
      !isValidReceiptSequence(item.after_seq) ||
      !isValidReceiptSequence(item.through_seq) || (item.through_seq as number) <= (item.after_seq as number) ||
      (item.state !== "prepared" && item.state !== "received" && item.state !== "consumed") ||
      !isValidTimestamp(item.prepared_at_ms) ||
      !isValidTimestamp(item.owner_last_seen_at_ms) ||
      (item.state === "prepared"
        ? item.received_at_ms !== null || item.consumed_at_ms !== null
        : !isValidTimestamp(item.received_at_ms) ||
          (item.state === "received"
            ? item.consumed_at_ms !== null
            : !isValidTimestamp(item.consumed_at_ms))) ||
      (isValidTimestamp(item.received_at_ms) &&
        (item.received_at_ms as number) < (item.prepared_at_ms as number)) ||
      (isValidTimestamp(item.consumed_at_ms) &&
        (!isValidTimestamp(item.received_at_ms) ||
          (item.consumed_at_ms as number) < (item.received_at_ms as number)))) {
    throw new Error(`invalid reply-ack store ${path}: malformed entry at index ${index}`);
  }
  return {
    targetIdentity: item.target_identity,
    installationId: item.installation_id,
    laneId: item.lane_id as string,
    replyTo: item.reply_to as string,
    afterSeq: item.after_seq as number,
    throughSeq: item.through_seq as number,
    turnToken: item.turn_token as string,
    interactionId: item.interaction_id as string,
    replyText: item.reply_text as string,
    state: item.state,
    preparedAtMs: item.prepared_at_ms as number,
    ownerLastSeenAtMs: item.owner_last_seen_at_ms as number,
    receivedAtMs: item.received_at_ms as number | null,
    consumedAtMs: item.consumed_at_ms as number | null,
  };
}

function parseTombstone(value: unknown, path: string, index: number): ReplyAckTombstone {
  if (value === null || typeof value !== "object") {
    throw new Error(`invalid reply-ack store ${path}: malformed tombstone at index ${index}`);
  }
  const item = value as Record<string, unknown>;
  if (!isTargetIdentity(item.target_identity) ||
      typeof item.installation_id !== "string" ||
      !isValidInstallationId(item.installation_id) ||
      !isValidLaneId(item.lane_id) ||
      !isValidOpaqueId(item.reply_to) ||
      !isValidReceiptSequence(item.after_seq) ||
      !isValidReceiptSequence(item.through_seq) ||
      (item.through_seq as number) <= (item.after_seq as number) ||
      !isValidTurnToken(item.turn_token) ||
      !isValidOpaqueId(item.interaction_id) ||
      !isValidTimestamp(item.consumed_at_ms) ||
      !isValidTimestamp(item.tombstoned_at_ms) ||
      (item.tombstoned_at_ms as number) < (item.consumed_at_ms as number)) {
    throw new Error(`invalid reply-ack store ${path}: malformed tombstone at index ${index}`);
  }
  return {
    targetIdentity: item.target_identity,
    installationId: item.installation_id,
    laneId: item.lane_id,
    replyTo: item.reply_to,
    afterSeq: item.after_seq as number,
    throughSeq: item.through_seq as number,
    turnToken: item.turn_token,
    interactionId: item.interaction_id,
    consumedAtMs: item.consumed_at_ms as number,
    tombstonedAtMs: item.tombstoned_at_ms as number,
  };
}

/** Write every byte before fsync/rename; writeSync is allowed to be short. */
function writeAllSync(fd: number, text: string): void {
  const bytes = Buffer.from(text, "utf8");
  const testChunkSize = shortWriteChunkSizeForTests;
  shortWriteChunkSizeForTests = null;
  let offset = 0;
  while (offset < bytes.length) {
    const length = Math.min(bytes.length - offset, testChunkSize ?? bytes.length);
    const written = writeSync(fd, bytes, offset, length, null);
    if (written <= 0) {
      throw new Error("reply-ack snapshot write made no progress");
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

function writeAtomically(path: string, text: string): void {
  const parent = dirname(path);
  const temp = `${path}.tmp-${process.pid}-${Date.now()}-${++writeCounter}`;
  let fd: number | undefined;
  let renamed = false;
  try {
    mkdirWithDurableParents(parent);
    fd = openSync(temp, "wx", 0o600);
    writeAllSync(fd, text);
    fsyncSync(fd);
    closeSync(fd);
    fd = undefined;
    if (failNextPreRenameWriteForTests) {
      failNextPreRenameWriteForTests = false;
      throw new Error("injected pre-rename reply-ack persistence failure");
    }
    renameSync(temp, path);
    renamed = true;
    if (failNextPostRenameDirectorySyncForTests) {
      failNextPostRenameDirectorySyncForTests = false;
      throw new Error("injected post-rename reply-ack directory fsync failure");
    }
    syncDirectory(parent);
  } catch (error) {
    if (fd !== undefined) closeSync(fd);
    try { unlinkSync(temp); } catch {}
    const message = `cannot persist reply-ack store ${path}: ${(error as Error).message}`;
    if (renamed) {
      throw new ReplyAckStorePostRenameError(
        `${message}; replacement was renamed before the failure, so durable state is uncertain`,
      );
    }
    throw new Error(message);
  }
}

function wire(binding: ReplyAckBinding) {
  return {
    target_identity: binding.targetIdentity,
    installation_id: binding.installationId,
    lane_id: binding.laneId,
    reply_to: binding.replyTo,
    after_seq: binding.afterSeq,
    through_seq: binding.throughSeq,
    turn_token: binding.turnToken,
    interaction_id: binding.interactionId,
    reply_text: binding.replyText,
    state: binding.state,
    prepared_at_ms: binding.preparedAtMs,
    owner_last_seen_at_ms: binding.ownerLastSeenAtMs,
    received_at_ms: binding.receivedAtMs,
    consumed_at_ms: binding.consumedAtMs,
  };
}

function wireTombstone(tombstone: ReplyAckTombstone) {
  return {
    target_identity: tombstone.targetIdentity,
    installation_id: tombstone.installationId,
    lane_id: tombstone.laneId,
    reply_to: tombstone.replyTo,
    after_seq: tombstone.afterSeq,
    through_seq: tombstone.throughSeq,
    turn_token: tombstone.turnToken,
    interaction_id: tombstone.interactionId,
    consumed_at_ms: tombstone.consumedAtMs,
    tombstoned_at_ms: tombstone.tombstonedAtMs,
  };
}

interface ReplyAckSnapshot {
  version: 8;
  bindings: unknown[];
  tombstones: unknown[];
}

export interface ReplyAckStoreSnapshot {
  bindings: ReplyAckBinding[];
  tombstones: ReplyAckTombstone[];
}

export class ReplyAckStore {
  private readonly path: string;
  private readonly targetIdentity: string;
  private persistenceFailure: string | null = null;

  constructor(path = defaultPath(), targetIdentity = currentTelepathydTargetIdentity()) {
    this.path = path;
    this.targetIdentity = targetIdentity;
  }

  /** Throws on runtime URL/token changes; durable rows remain untouched. */
  assertCurrentTarget(): void {
    const current = currentTelepathydTargetIdentity();
    if (current !== this.targetIdentity) {
      throw new Error(
        `reply-ack store ${this.path}: telepathyd target identity changed; durable receipts remain pending`,
      );
    }
  }

  targetScope(): string { return this.targetIdentity; }

  loadSnapshot(): ReplyAckStoreSnapshot {
    this.assertCurrentTarget();
    let text: string;
    try {
      text = readFileSync(this.path, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return { bindings: [], tombstones: [] };
      throw new Error(`cannot read reply-ack store ${this.path}: ${(error as Error).message}`);
    }
    let raw: unknown;
    try {
      raw = JSON.parse(text);
    } catch (error) {
      throw new Error(`corrupt reply-ack store ${this.path}: ${(error as Error).message}`);
    }
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
      throw new Error(`invalid reply-ack store ${this.path}: expected v8 snapshot object`);
    }
    const snapshot = raw as Partial<ReplyAckSnapshot>;
    if (snapshot.version !== 8 || !Array.isArray(snapshot.bindings) || !Array.isArray(snapshot.tombstones)) {
      throw new Error(`invalid reply-ack store ${this.path}: expected v8 snapshot with bindings and tombstones`);
    }
    if (snapshot.bindings.length > MAX_STORED_REPLY_ACKS ||
        snapshot.tombstones.length > MAX_STORED_REPLY_ACK_TOMBSTONES) {
      throw new ReplyAckStoreCapacityError(`invalid reply-ack store ${this.path}: too many entries`);
    }
    const seen = new Set<string>();
    const bindings = snapshot.bindings.map((value, index) => {
      const binding = parse(value, this.path, index);
      if (binding.targetIdentity !== this.targetIdentity) {
        throw new Error(`invalid reply-ack store ${this.path}: target identity mismatch at index ${index}`);
      }
      const identity = key(binding);
      if (seen.has(identity)) {
        throw new Error(`invalid reply-ack store ${this.path}: duplicate entry at index ${index}`);
      }
      seen.add(identity);
      return binding;
    });
    const tombstones = snapshot.tombstones.map((value, index) => {
      const tombstone = parseTombstone(value, this.path, index);
      if (tombstone.targetIdentity !== this.targetIdentity) {
        throw new Error(`invalid reply-ack store ${this.path}: target identity mismatch for tombstone at index ${index}`);
      }
      const identity = key(tombstone);
      if (seen.has(identity)) {
        throw new Error(`invalid reply-ack store ${this.path}: duplicate terminal entry at index ${index}`);
      }
      seen.add(identity);
      return tombstone;
    });
    this.assertCurrentTarget();
    return { bindings, tombstones };
  }

  load(): ReplyAckBinding[] {
    return this.loadSnapshot().bindings;
  }

  /**
   * A post-rename failure is not safely recoverable in this process. A later
   * snapshot write could erase a binding that survived the failed directory
   * sync, so callers must pause new remote replies until restart/operator
   * reconciliation.
   */
  unavailableReason(): string | null {
    try {
      this.assertCurrentTarget();
    } catch (error) {
      return (error as Error).message;
    }
    return this.persistenceFailure;
  }

  save(bindings: Iterable<ReplyAckBinding>, tombstones: Iterable<ReplyAckTombstone> = []): void {
    this.assertCurrentTarget();
    if (this.persistenceFailure !== null) {
      throw new ReplyAckStoreUnavailableError(this.persistenceFailure);
    }
    const records = [...bindings].map((binding) => ({
      ...binding,
      targetIdentity: binding.targetIdentity ?? this.targetIdentity,
    }));
    const terminalRecords = [...tombstones].map((tombstone) => ({
      ...tombstone,
      targetIdentity: tombstone.targetIdentity ?? this.targetIdentity,
    }));
    if (records.length > MAX_STORED_REPLY_ACKS) {
      throw new ReplyAckStoreCapacityError(`reply-ack store ${this.path}: too many entries`);
    }
    if (terminalRecords.length > MAX_STORED_REPLY_ACK_TOMBSTONES) {
      throw new ReplyAckStoreCapacityError(`reply-ack store ${this.path}: too many tombstones`);
    }
    const seen = new Set<string>();
    for (const binding of records) {
      if (binding.targetIdentity !== this.targetIdentity) {
        throw new Error(`reply-ack store ${this.path}: target identity mismatch`);
      }
      parse(wire(binding), this.path, 0);
      const identity = key(binding);
      if (seen.has(identity)) throw new Error(`reply-ack store ${this.path}: duplicate entry`);
      seen.add(identity);
    }
    for (const tombstone of terminalRecords) {
      if (tombstone.targetIdentity !== this.targetIdentity) {
        throw new Error(`reply-ack store ${this.path}: tombstone target identity mismatch`);
      }
      parseTombstone(wireTombstone(tombstone), this.path, 0);
      const identity = key(tombstone);
      if (seen.has(identity)) throw new Error(`reply-ack store ${this.path}: duplicate terminal entry`);
      seen.add(identity);
    }
    try {
      writeAtomically(this.path, JSON.stringify({
        version: 8,
        bindings: records.map(wire),
        tombstones: terminalRecords.map(wireTombstone),
      }, null, 2));
    } catch (error) {
      if (error instanceof ReplyAckStorePostRenameError) {
        this.persistenceFailure =
          `reply acknowledgement persistence is uncertain after a post-rename directory sync failure; ` +
          "remote replies are paused to preserve durable receipt authorization";
      }
      throw error;
    }
  }

}
