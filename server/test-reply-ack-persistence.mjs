import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const {
  ReplyAckStore,
  ReplyAckStorePostRenameError,
  ReplyAckStoreUnavailableError,
  MAX_STORED_REPLY_ACK_TOMBSTONES,
  failNextReplyAckStoreWriteBeforeRenameForTests,
  failNextReplyAckStoreDirectorySyncAfterRenameForTests,
  setReplyAckStoreDirectorySyncHookForTests,
  useShortWritesForNextReplyAckStoreWriteForTests,
} = await import("./dist/reply-ack-store.js");
const { targetIdentityFor } = await import("./dist/target-scope.js");
const {
  MAX_INSTALLATION_ID_LENGTH,
  MAX_SAFE_SEQUENCE,
  MAX_TURN_TOKEN_LENGTH,
} = await import("./dist/protocol.js");

const directory = await mkdtemp(join(tmpdir(), "telepathos-reply-ack-persistence-"));
const path = join(directory, "reply-acks.json");
const legacyPath = join(directory, "legacy-reply-acks.json");
const targetIdentity = targetIdentityFor(null);
const first = {
  targetIdentity,
  installationId: "persistence-installation",
  laneId: "telepathos:direct",
  replyTo: "tp-stable",
  afterSeq: 2,
  throughSeq: 3,
  turnToken: "turn-stable",
  interactionId: "i-stable",
  replyText: "stable replay text",
  state: "received",
  preparedAtMs: 1_700_000_000_000,
  ownerLastSeenAtMs: 1_700_000_000_100,
  receivedAtMs: 1_700_000_000_200,
  consumedAtMs: null,
};
const uncertain = {
  targetIdentity,
  installationId: "persistence-installation",
  laneId: "telepathos:direct",
  replyTo: "tp-uncertain",
  afterSeq: 4,
  throughSeq: 5,
  turnToken: "turn-uncertain",
  interactionId: "i-uncertain",
  replyText: "uncertain replay text",
  state: "received",
  preparedAtMs: 1_700_000_000_000,
  ownerLastSeenAtMs: 1_700_000_000_100,
  receivedAtMs: 1_700_000_000_200,
  consumedAtMs: null,
};
const consumed = {
  ...first,
  replyTo: "tp-consumed",
  afterSeq: 8,
  throughSeq: 9,
  turnToken: "turn-consumed",
  interactionId: "i-consumed",
  state: "consumed",
  preparedAtMs: 1_700_000_000_000,
  ownerLastSeenAtMs: 1_700_000_000_100,
  receivedAtMs: 1_700_000_000_200,
  consumedAtMs: 1_700_000_000_300,
};
const tombstone = {
  targetIdentity,
  installationId: "persistence-installation",
  laneId: "telepathos:direct",
  replyTo: "tp-tombstone",
  afterSeq: 12,
  throughSeq: 13,
  turnToken: "turn-tombstone",
  interactionId: "i-tombstone",
  consumedAtMs: 1_700_000_000_300,
  tombstonedAtMs: 1_700_000_000_400,
};

function wireBinding(binding) {
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

function wireTombstone(value) {
  return {
    target_identity: value.targetIdentity,
    installation_id: value.installationId,
    lane_id: value.laneId,
    reply_to: value.replyTo,
    after_seq: value.afterSeq,
    through_seq: value.throughSeq,
    turn_token: value.turnToken,
    interaction_id: value.interactionId,
    consumed_at_ms: value.consumedAtMs,
    tombstoned_at_ms: value.tombstonedAtMs,
  };
}

const bindingTombstoneCollision = {
  ...tombstone,
  replyTo: first.replyTo,
  afterSeq: first.afterSeq,
  throughSeq: first.throughSeq,
  turnToken: first.turnToken,
  interactionId: first.interactionId,
  consumedAtMs: first.receivedAtMs,
  tombstonedAtMs: first.receivedAtMs,
};
const differentBindingCollisionPayload = {
  ...first,
  turnToken: "turn-binding-collision-payload",
  interactionId: "i-binding-collision-payload",
};
const differentTombstoneCollisionPayload = {
  ...bindingTombstoneCollision,
  turnToken: "turn-tombstone-collision-payload",
  interactionId: "i-tombstone-collision-payload",
};

const duplicateSnapshots = [
  ["binding same payload in original order", [first, { ...first }], []],
  ["binding same payload in reverse order", [{ ...first }, first], []],
  ["binding different payload in original order", [first, { ...first, replyText: "different replay text" }], []],
  ["binding different payload in reverse order", [{ ...first, replyText: "different replay text" }, first], []],
  ["tombstone same payload in original order", [], [tombstone, { ...tombstone }]],
  ["tombstone same payload in reverse order", [], [{ ...tombstone }, tombstone]],
  ["tombstone different payload in original order", [], [tombstone, { ...tombstone, consumedAtMs: tombstone.consumedAtMs + 1, tombstonedAtMs: tombstone.tombstonedAtMs + 1 }]],
  ["tombstone different payload in reverse order", [], [{ ...tombstone, consumedAtMs: tombstone.consumedAtMs + 1, tombstonedAtMs: tombstone.tombstonedAtMs + 1 }, tombstone]],
  ["binding/tombstone cross-list collision", [first], [bindingTombstoneCollision]],
  ["binding/tombstone cross-list collision with different payloads", [differentBindingCollisionPayload], [differentTombstoneCollisionPayload]],
];

// A first write into a nested state path must sync the parent of every
// directory created by recursive mkdir, followed by the final directory
// sync after the snapshot rename.
const nestedPath = join(directory, "created", "state", "reply-acks.json");
const nestedDirectorySyncs = [];
setReplyAckStoreDirectorySyncHookForTests((syncedPath) => nestedDirectorySyncs.push(syncedPath));
try {
  new ReplyAckStore(nestedPath).save([first]);
} finally {
  setReplyAckStoreDirectorySyncHookForTests(null);
}
assert.deepEqual(nestedDirectorySyncs, [
  directory,
  join(directory, "created"),
  join(directory, "created", "state"),
]);

try {
  // v5 did not persist the lifecycle/ownership clock needed for reconciliation. Refuse it rather
  // than silently allowing cross-device replay or acknowledgement.
  await writeFile(legacyPath, JSON.stringify({ version: 5, bindings: [] }));
  assert.throws(() => new ReplyAckStore(legacyPath).load(), /expected v8 snapshot with bindings and tombstones/);

  // URL spelling is canonicalized, while credential rotation creates a new
  // namespace. The digest must not expose either credential or URL contents.
  const normalizedA = targetIdentityFor("HTTP://LOCALHOST:8790///", "token-a");
  assert.equal(normalizedA, targetIdentityFor("http://localhost:8790", "token-a"));
  assert.notEqual(normalizedA, targetIdentityFor("http://localhost:8790", "token-b"));
  assert(!normalizedA.includes("token-a"));

  const wrongTargetPath = join(directory, "wrong-target.json");
  await writeFile(wrongTargetPath, JSON.stringify({
    version: 8,
    bindings: [{
      target_identity: normalizedA,
      installation_id: first.installationId,
      lane_id: first.laneId,
      reply_to: first.replyTo,
      after_seq: first.afterSeq,
      through_seq: first.throughSeq,
      turn_token: first.turnToken,
      interaction_id: first.interactionId,
      reply_text: first.replyText,
      state: first.state,
      prepared_at_ms: first.preparedAtMs,
      owner_last_seen_at_ms: first.ownerLastSeenAtMs,
      received_at_ms: first.receivedAtMs,
      consumed_at_ms: first.consumedAtMs,
    }],
    tombstones: [],
  }));
  const beforeWrongTarget = await readFile(wrongTargetPath, "utf8");
  const previousHermesUrl = process.env.TELEPATHOS_HERMES_URL;
  const previousToken = process.env.TELEPATHOS_TOKEN;
  process.env.TELEPATHOS_HERMES_URL = "http://localhost:8790";
  process.env.TELEPATHOS_TOKEN = "token-b";
  assert.throws(
    () => new ReplyAckStore(wrongTargetPath).load(),
    /target identity mismatch/,
  );
  if (previousHermesUrl === undefined) delete process.env.TELEPATHOS_HERMES_URL;
  else process.env.TELEPATHOS_HERMES_URL = previousHermesUrl;
  if (previousToken === undefined) delete process.env.TELEPATHOS_TOKEN;
  else process.env.TELEPATHOS_TOKEN = previousToken;
  assert.equal(await readFile(wrongTargetPath, "utf8"), beforeWrongTarget);

  // A live endpoint or credential change fences the already-open store. The
  // old rows remain byte-for-byte recoverable for a restart under the old
  // target; they are never acknowledged or retired under the new one.
  const runtimePath = join(directory, "runtime-switch.json");
  const runtimeStore = new ReplyAckStore(runtimePath);
  runtimeStore.save([first]);
  const beforeRuntimeSwitch = await readFile(runtimePath, "utf8");
  const runtimeUrl = process.env.TELEPATHOS_HERMES_URL;
  const runtimeToken = process.env.TELEPATHOS_TOKEN;
  try {
    process.env.TELEPATHOS_HERMES_URL = "http://localhost:8791///";
    process.env.TELEPATHOS_TOKEN = "rotated-runtime-token";
    assert.throws(() => runtimeStore.load(), /target identity changed/);
    assert.equal(await readFile(runtimePath, "utf8"), beforeRuntimeSwitch);
  } finally {
    if (runtimeUrl === undefined) delete process.env.TELEPATHOS_HERMES_URL;
    else process.env.TELEPATHOS_HERMES_URL = runtimeUrl;
    if (runtimeToken === undefined) delete process.env.TELEPATHOS_TOKEN;
    else process.env.TELEPATHOS_TOKEN = runtimeToken;
  }

  const store = new ReplyAckStore(path);

  // Persisted v8 bindings use the same live parser bounds as a socket frame:
  // exact JSON-safe sequence values and UTF-16 token/owner limits.
  const atLimit = {
    ...first,
    installationId: "i".repeat(MAX_INSTALLATION_ID_LENGTH),
    afterSeq: MAX_SAFE_SEQUENCE - 1,
    throughSeq: MAX_SAFE_SEQUENCE,
    turnToken: "t".repeat(MAX_TURN_TOKEN_LENGTH),
  };
  store.save([atLimit]);
  assert.deepEqual(new ReplyAckStore(path).load(), [atLimit]);

  for (const [label, invalid] of [
    ["lane id with spaces", { ...atLimit, laneId: "telepathos: direct" }],
    ["lane id with controls", { ...atLimit, laneId: "telepathos:repo:\u0000control" }],
    ["lane id with quote", { ...atLimit, laneId: 'telepathos:repo:quote"' }],
    ["lane id with backslash", { ...atLimit, laneId: "telepathos:repo:backslash\\" }],
    ["oversized lane id", { ...atLimit, laneId: `telepathos:repo:${"a".repeat(128)}` }],
    ["unicode lane id", { ...atLimit, laneId: "telepathos:repo:é" }],
    ["oversized turn token", { ...atLimit, turnToken: "t".repeat(MAX_TURN_TOKEN_LENGTH + 1) }],
    ["one-over receipt sequence", { ...atLimit, afterSeq: MAX_SAFE_SEQUENCE, throughSeq: MAX_SAFE_SEQUENCE + 1 }],
    ["oversized installation ID", { ...atLimit, installationId: "i".repeat(MAX_INSTALLATION_ID_LENGTH + 1) }],
    ["control-character installation ID", { ...atLimit, installationId: "owner\ncontrol" }],
  ]) {
    const invalidPath = join(directory, `${label.replaceAll(" ", "-")}.json`);
    const original = JSON.stringify({
      version: 8,
      bindings: [{
        installation_id: invalid.installationId,
        target_identity: invalid.targetIdentity,
        lane_id: invalid.laneId,
        reply_to: invalid.replyTo,
        after_seq: invalid.afterSeq,
        through_seq: invalid.throughSeq,
        turn_token: invalid.turnToken,
        interaction_id: invalid.interactionId,
        reply_text: invalid.replyText,
        state: invalid.state,
        prepared_at_ms: invalid.preparedAtMs,
        owner_last_seen_at_ms: invalid.ownerLastSeenAtMs,
        received_at_ms: invalid.receivedAtMs,
        consumed_at_ms: invalid.consumedAtMs,
      }],
      tombstones: [],
    });
    await writeFile(invalidPath, original);
    assert.throws(() => new ReplyAckStore(invalidPath).load(), /malformed entry/);
    assert.equal(await readFile(invalidPath, "utf8"), original, label);
  }

  // Tombstones are independently replayed terminal identities and must use
  // the same lane grammar as live bindings.
  for (const [label, laneId] of [
    ["spaces", "telepathos: direct"],
    ["controls", "telepathos:repo:\u0001control"],
    ["quotes", 'telepathos:repo:quote"'],
    ["backslashes", "telepathos:repo:backslash\\"],
    ["oversize", `telepathos:repo:${"a".repeat(128)}`],
    ["unicode", "telepathos:repo:é"],
  ]) {
    const invalidPath = join(directory, `invalid-tombstone-${label}.json`);
    const original = JSON.stringify({
      version: 8,
      bindings: [],
      tombstones: [{
        installation_id: tombstone.installationId,
        target_identity: tombstone.targetIdentity,
        lane_id: laneId,
        reply_to: tombstone.replyTo,
        after_seq: tombstone.afterSeq,
        through_seq: tombstone.throughSeq,
        turn_token: tombstone.turnToken,
        interaction_id: tombstone.interactionId,
        consumed_at_ms: tombstone.consumedAtMs,
        tombstoned_at_ms: tombstone.tombstonedAtMs,
      }],
    });
    await writeFile(invalidPath, original);
    assert.throws(() => new ReplyAckStore(invalidPath).load(), /malformed tombstone/);
    assert.equal(await readFile(invalidPath, "utf8"), original, `tombstone ${label}`);
  }

  // Receipt identity is unique across both live bindings and terminal
  // tombstones. A restarted store must reject duplicate records before
  // exposing any live state, regardless of payload or list order, and leave
  // the malformed snapshot byte-for-byte untouched.
  for (const [index, [label, bindingEntries, tombstoneEntries]] of duplicateSnapshots.entries()) {
    const duplicatePath = join(directory, `duplicate-receipt-${index}.json`);
    const original = JSON.stringify({
      version: 8,
      bindings: bindingEntries.map(wireBinding),
      tombstones: tombstoneEntries.map(wireTombstone),
    });
    await writeFile(duplicatePath, original);
    assert.throws(
      () => new ReplyAckStore(duplicatePath).loadSnapshot(),
      /duplicate (entry|terminal entry)/,
      label,
    );
    assert.equal(await readFile(duplicatePath, "utf8"), original, `${label}: restart preserves snapshot`);
  }

  // Save validation is also fail-closed: duplicate input must not replace a
  // previously durable snapshot, including binding/tombstone cross-list
  // collisions. Distinct receipt identities remain valid together.
  const duplicateSavePath = join(directory, "duplicate-save.json");
  const duplicateSaveStore = new ReplyAckStore(duplicateSavePath);
  duplicateSaveStore.save([first], [tombstone]);
  const beforeDuplicateSave = await readFile(duplicateSavePath, "utf8");
  for (const [label, bindingEntries, tombstoneEntries] of [
    ["save duplicate binding", [first, { ...first }], []],
    ["save duplicate binding with different payload and order", [{ ...first, replyText: "different replay text" }, first], []],
    ["save duplicate tombstone", [], [tombstone, { ...tombstone }]],
    ["save duplicate tombstone with different payload and order", [], [{ ...tombstone, consumedAtMs: tombstone.consumedAtMs + 1, tombstonedAtMs: tombstone.tombstonedAtMs + 1 }, tombstone]],
    ["save binding/tombstone collision", [first], [bindingTombstoneCollision]],
  ]) {
    assert.throws(
      () => duplicateSaveStore.save(bindingEntries, tombstoneEntries),
      /duplicate (entry|terminal entry)/,
      label,
    );
    assert.equal(await readFile(duplicateSavePath, "utf8"), beforeDuplicateSave, `${label}: save is nonmutating`);
  }

  const distinctBinding = {
    ...first,
    replyTo: "tp-distinct-binding",
    afterSeq: 20,
    throughSeq: 21,
    turnToken: "turn-distinct-binding",
    interactionId: "i-distinct-binding",
  };
  const distinctTombstone = {
    ...tombstone,
    replyTo: "tp-distinct-tombstone",
    afterSeq: 22,
    throughSeq: 23,
    turnToken: "turn-distinct-tombstone",
    interactionId: "i-distinct-tombstone",
  };
  const distinctStorePath = join(directory, "distinct-receipts.json");
  const distinctStore = new ReplyAckStore(distinctStorePath);
  distinctStore.save([first, distinctBinding], [tombstone, distinctTombstone]);
  assert.deepEqual(new ReplyAckStore(distinctStorePath).loadSnapshot(), {
    bindings: [first, distinctBinding],
    tombstones: [tombstone, distinctTombstone],
  });

  // writeSync can complete only part of a requested buffer. The snapshot must
  // not be fsynced and renamed until every byte has been written.
  useShortWritesForNextReplyAckStoreWriteForTests(7);
  store.save([first]);
  assert.deepEqual(new ReplyAckStore(path).load(), [first]);

  // A failure before rename leaves the old snapshot intact and does not poison
  // the store: retrying the same intended mutation is safe.
  failNextReplyAckStoreWriteBeforeRenameForTests();
  assert.throws(() => store.save([first, uncertain]), Error);
  assert.equal(store.unavailableReason(), null);
  assert.deepEqual(new ReplyAckStore(path).load(), [first]);
  store.save([first, uncertain]);

  // Delivery consumption is a distinct durable phase. A restart may safely
  // resend its acknowledgement without re-authorizing the external consume,
  // and a later terminal retirement can reclaim the slot.
  store.save([first, uncertain, consumed]);
  assert.deepEqual(new ReplyAckStore(path).load(), [first, uncertain, consumed]);
  store.save([first, uncertain]);
  assert.deepEqual(new ReplyAckStore(path).load(), [first, uncertain]);

  store.save([first], [tombstone]);
  assert.deepEqual(new ReplyAckStore(path).loadSnapshot(), {
    bindings: [first],
    tombstones: [tombstone],
  });
  const boundedTombstones = Array.from({ length: MAX_STORED_REPLY_ACK_TOMBSTONES + 1 }, (_, index) => ({
    ...tombstone,
    replyTo: `tp-tombstone-${index}`,
    afterSeq: 100 + index * 2,
    throughSeq: 101 + index * 2,
    turnToken: `turn-tombstone-${index}`,
    interactionId: `i-tombstone-${index}`,
  }));
  assert.throws(() => store.save([], boundedTombstones), /too many tombstones/);

  const afterPostRenameFailure = {
    ...uncertain,
    replyTo: "tp-after-post-rename",
    afterSeq: 6,
    throughSeq: 7,
    state: "prepared",
    replyText: "full replay after a post-rename failure",
    preparedAtMs: 1_700_000_000_400,
    ownerLastSeenAtMs: 1_700_000_000_500,
    receivedAtMs: null,
    consumedAtMs: null,
  };
  failNextReplyAckStoreDirectorySyncAfterRenameForTests();
  assert.throws(
    () => store.save([first, uncertain, afterPostRenameFailure]),
    ReplyAckStorePostRenameError,
  );
  assert.match(store.unavailableReason() ?? "", /persistence is uncertain/);

  // The renamed replacement may be the durable one. A fresh store can retain
  // and replay the complete prepared envelope, but it still never authorizes
  // external consumption before the handset sends reply_received.
  const restarted = new ReplyAckStore(path);
  assert.deepEqual(restarted.load(), [first, uncertain, afterPostRenameFailure]);
  assert.equal(restarted.unavailableReason(), null);
  restarted.save([first, uncertain]);
  assert.throws(() => store.save([first]), ReplyAckStoreUnavailableError);
  assert.deepEqual(new ReplyAckStore(path).load(), [first, uncertain]);
  console.log("REPLY ACK PERSISTENCE TEST PASS");
} finally {
  await rm(directory, { recursive: true, force: true });
}
