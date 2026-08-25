import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const {
  InteractionOutbox,
  InteractionOutboxFullError,
  InteractionOutboxBlockedError,
  InteractionOutboxRecoverablePersistenceError,
  failNextInteractionOutboxPostRenameDirectorySyncForTest,
  failNextInteractionOutboxWriteBeforeRenameForTest,
  setInteractionOutboxDirectorySyncHookForTest,
  setInteractionOutboxWriteChunkLimitForTest,
  MAX_INTERACTION_ID_LENGTH,
  MAX_INTERACTION_ID_BYTES,
} = await import("./dist/interaction-outbox.js");
const { ReplyAckStore, ReplyAckStoreCapacityError } = await import("./dist/reply-ack-store.js");
const { currentTelepathosdTargetIdentity } = await import("./dist/target-scope.js");
const directory = await mkdtemp(join(tmpdir(), "telepathos-interaction-outbox-"));
const path = join(directory, "outbox.json");
try {
  const targetIdentity = currentTelepathosdTargetIdentity();
  const legacyOutboxPath = join(directory, "legacy-v2.json");
  await writeFile(legacyOutboxPath, JSON.stringify({ version: 2, records: [] }));
  assert.throws(() => new InteractionOutbox(legacyOutboxPath, 2), /expected v3 snapshot object/);
  // Restart/load must reject every malformed lane spelling without changing
  // the snapshot on disk, and admission must reject it before reservation.
  for (const [label, laneId] of [
    ["spaces", "telepathos: direct"],
    ["controls", "telepathos:repo:\u0001control"],
    ["quotes", 'telepathos:repo:quote"'],
    ["backslashes", "telepathos:repo:backslash\\"],
    ["oversize", `telepathos:repo:${"a".repeat(128)}`],
    ["unicode", "telepathos:repo:é"],
  ]) {
    const invalidPath = join(directory, `invalid-${label}.json`);
    const original = JSON.stringify({
      version: 3,
      records: [{
        target_identity: targetIdentity,
        lane_id: laneId,
        interaction_id: `invalid-${label}`,
        interaction_created_at_ms: 1_700_000_000_000,
        state: "pending",
      }],
    });
    await writeFile(invalidPath, original);
    assert.throws(() => new InteractionOutbox(invalidPath, 2), /malformed entry/);
    assert.equal(await readFile(invalidPath, "utf8"), original, label);
  }
  const admission = new InteractionOutbox(join(directory, "invalid-admission.json"), 2);
  for (const laneId of [
    "telepathos: direct",
    "telepathos:repo:\u0001control",
    'telepathos:repo:quote"',
    "telepathos:repo:backslash\\",
    `telepathos:repo:${"a".repeat(128)}`,
    "telepathos:repo:é",
  ]) {
    assert.throws(() => admission.reserve({
      laneId,
      interactionId: `rejected-${laneId.length}`,
      interactionCreatedAtMs: 1_700_000_000_001,
    }), /malformed entry/);
  }
  assert.deepEqual(admission.pending(), []);
  assert.throws(() => admission.reserve({
    laneId: "telepathos:direct",
    interactionId: "x".repeat(MAX_INTERACTION_ID_LENGTH + 1),
    interactionCreatedAtMs: 1_700_000_000_100,
  }), /malformed entry/);
  assert(!existsSync(join(directory, "invalid-admission.json")));

  // Snapshot count is rejected before map/retention, and malformed IDs leave
  // the original bytes untouched for operator reconciliation.
  const oversizedSnapshotPath = join(directory, "oversized-snapshot.json");
  const oversizedSnapshot = {
    version: 3,
    records: Array.from({ length: 3 }, (_, index) => ({
      target_identity: targetIdentity,
      lane_id: "telepathos:direct",
      interaction_id: `i-oversized-snapshot-${index}`,
      interaction_created_at_ms: 1_700_000_000_200 + index,
      state: "pending",
    })),
  };
  await writeFile(oversizedSnapshotPath, JSON.stringify(oversizedSnapshot));
  const beforeOversizedSnapshot = await readFile(oversizedSnapshotPath, "utf8");
  assert.throws(() => new InteractionOutbox(oversizedSnapshotPath, 2), InteractionOutboxFullError);
  assert.equal(await readFile(oversizedSnapshotPath, "utf8"), beforeOversizedSnapshot);

  const oversizedLoadedIdPath = join(directory, "oversized-loaded-id.json");
  const oversizedLoadedId = {
    version: 3,
    records: [{
      target_identity: targetIdentity,
      lane_id: "telepathos:direct",
      interaction_id: "é".repeat(Math.ceil(MAX_INTERACTION_ID_BYTES / 2) + 1),
      interaction_created_at_ms: 1_700_000_000_300,
      state: "pending",
    }],
  };
  await writeFile(oversizedLoadedIdPath, JSON.stringify(oversizedLoadedId));
  const beforeOversizedLoadedId = await readFile(oversizedLoadedIdPath, "utf8");
  assert.throws(() => new InteractionOutbox(oversizedLoadedIdPath, 2), /malformed entry/);
  assert.equal(await readFile(oversizedLoadedIdPath, "utf8"), beforeOversizedLoadedId);

  // Restart must reject duplicate (lane_id, interaction_id) keys before the
  // records reach live state. This also protects the snapshot from the
  // reserved-row cleanup that would otherwise rewrite it during construction.
  for (const [label, duplicateTimestamp] of [
    ["same-timestamp", 1_700_000_000_500],
    ["conflicting-timestamp", 1_700_000_000_501],
  ]) {
    const duplicatePath = join(directory, `duplicate-${label}.json`);
    const duplicateSnapshot = {
      version: 3,
      records: [
        {
          target_identity: targetIdentity,
          lane_id: "telepathos:direct",
          interaction_id: "i-duplicate-restart",
          interaction_created_at_ms: 1_700_000_000_500,
          state: "reserved",
        },
        {
          target_identity: targetIdentity,
          lane_id: "telepathos:direct",
          interaction_id: "i-duplicate-restart",
          interaction_created_at_ms: duplicateTimestamp,
          state: "reserved",
        },
      ],
    };
    const original = JSON.stringify(duplicateSnapshot);
    await writeFile(duplicatePath, original);
    assert.throws(
      () => new InteractionOutbox(duplicatePath, 2),
      /duplicate entry at index 1/,
      label,
    );
    assert.equal(await readFile(duplicatePath, "utf8"), original, label);
  }

  // The lane is part of the durable key: the same interaction ID remains
  // valid in another lane, including when its creation timestamp differs.
  const crossLanePath = join(directory, "cross-lane-interaction-ids.json");
  const crossLaneSnapshot = {
    version: 3,
    records: [
      {
        target_identity: targetIdentity,
        lane_id: "telepathos:direct",
        interaction_id: "i-cross-lane-same-time",
        interaction_created_at_ms: 1_700_000_000_600,
        state: "pending",
      },
      {
        target_identity: targetIdentity,
        lane_id: "telepathos:other",
        interaction_id: "i-cross-lane-same-time",
        interaction_created_at_ms: 1_700_000_000_600,
        state: "pending",
      },
      {
        target_identity: targetIdentity,
        lane_id: "telepathos:direct",
        interaction_id: "i-cross-lane-different-time",
        interaction_created_at_ms: 1_700_000_000_601,
        state: "pending",
      },
      {
        target_identity: targetIdentity,
        lane_id: "telepathos:other",
        interaction_id: "i-cross-lane-different-time",
        interaction_created_at_ms: 1_700_000_000_602,
        state: "pending",
      },
    ],
  };
  await writeFile(crossLanePath, JSON.stringify(crossLaneSnapshot));
  assert.deepEqual(new InteractionOutbox(crossLanePath, 4).pending(), [
    {
      laneId: "telepathos:direct",
      interactionId: "i-cross-lane-same-time",
      interactionCreatedAtMs: 1_700_000_000_600,
    },
    {
      laneId: "telepathos:other",
      interactionId: "i-cross-lane-same-time",
      interactionCreatedAtMs: 1_700_000_000_600,
    },
    {
      laneId: "telepathos:direct",
      interactionId: "i-cross-lane-different-time",
      interactionCreatedAtMs: 1_700_000_000_601,
    },
    {
      laneId: "telepathos:other",
      interactionId: "i-cross-lane-different-time",
      interactionCreatedAtMs: 1_700_000_000_602,
    },
  ]);

  const runtimePath = join(directory, "runtime-target-switch.json");
  const runtimeOutbox = new InteractionOutbox(runtimePath, 2);
  const runtimeRecord = {
    laneId: "telepathos:direct",
    interactionId: "i-runtime-target-switch",
    interactionCreatedAtMs: 1_700_000_000_400,
  };
  runtimeOutbox.reserve(runtimeRecord);
  runtimeOutbox.promote(runtimeRecord);
  const beforeRuntimeSwitch = await readFile(runtimePath, "utf8");
  const previousRuntimeUrl = process.env.TELEPATHOS_HERMES_URL;
  const previousRuntimeToken = process.env.TELEPATHOS_TOKEN;
  try {
    process.env.TELEPATHOS_HERMES_URL = "http://localhost:8791///";
    process.env.TELEPATHOS_TOKEN = "rotated-runtime-token";
    assert.throws(() => runtimeOutbox.pending(), InteractionOutboxBlockedError);
    assert.equal(await readFile(runtimePath, "utf8"), beforeRuntimeSwitch);
  } finally {
    if (previousRuntimeUrl === undefined) delete process.env.TELEPATHOS_HERMES_URL;
    else process.env.TELEPATHOS_HERMES_URL = previousRuntimeUrl;
    if (previousRuntimeToken === undefined) delete process.env.TELEPATHOS_TOKEN;
    else process.env.TELEPATHOS_TOKEN = previousRuntimeToken;
  }

  const first = new InteractionOutbox(path, 2);
  const interaction = {
    laneId: "telepathos:direct",
    interactionId: "i-test-1",
    interactionCreatedAtMs: 1_700_000_000_000,
  };
  const abandoned = {
    laneId: "telepathos:direct",
    interactionId: "i-test-abandoned",
    interactionCreatedAtMs: 1_700_000_000_001,
  };
  first.reserve(interaction);
  first.promote(interaction);
  first.reserve(abandoned);

  // A restart retains the completed record but reclaims a pre-STT reservation:
  // promotion happens before any remote side effect, so it cannot double-count.
  const reloaded = new InteractionOutbox(path, 2);
  assert.deepEqual(reloaded.pending(), [
    interaction,
  ]);
  assert.deepEqual(reloaded.status(), {
    capacity: 2,
    used: 1,
    pending: 1,
    reserved: 0,
    expired: 0,
    accepting: true,
  });

  const second = { ...interaction, interactionId: "i-test-2", interactionCreatedAtMs: 1_700_000_000_002 };
  reloaded.reserve(second);
  assert.throws(
    () => reloaded.reserve({ ...interaction, interactionId: "i-test-3", interactionCreatedAtMs: 1_700_000_000_003 }),
    InteractionOutboxFullError,
  );
  reloaded.promote(second);
  reloaded.markExpired(second);
  assert.throws(
    () => reloaded.reserve({ ...interaction, interactionId: "i-test-4", interactionCreatedAtMs: 1_700_000_000_004 }),
    InteractionOutboxBlockedError,
  );
  reloaded.removeDelivered(interaction);
  assert.deepEqual(new InteractionOutbox(path, 2).pending(), []);
  assert.equal(new InteractionOutbox(path, 2).status().expired, 1);

  const poisonedPath = join(directory, "post-rename-failure.json");
  const poisoned = new InteractionOutbox(poisonedPath, 2);
  const uncertain = {
    laneId: "telepathos:direct",
    interactionId: "i-post-rename",
    interactionCreatedAtMs: 1_700_000_000_005,
  };
  poisoned.reserve(uncertain);
  failNextInteractionOutboxPostRenameDirectorySyncForTest();
  assert.throws(
    () => poisoned.promote(uncertain),
    (error) => error instanceof InteractionOutboxBlockedError &&
      /durability is uncertain after a post-rename persistence failure/.test(error.message),
  );
  // The rename already made the pending snapshot visible, so preserve the
  // matching in-memory state rather than reverting to the old reservation.
  assert.deepEqual(poisoned.pending(), [uncertain]);
  assert.equal(poisoned.status().accepting, false);
  assert.match(poisoned.unavailableReason() ?? "", /durability is uncertain/);
  assert.deepEqual(new InteractionOutbox(poisonedPath, 2).pending(), [uncertain]);
  assert.throws(
    () => poisoned.removeDelivered(uncertain),
    InteractionOutboxBlockedError,
  );

  const recoveryPath = join(directory, "pre-rename-recovery.json");
  const recovering = new InteractionOutbox(recoveryPath, 2);
  const retried = {
    laneId: "telepathos:direct",
    interactionId: "i-pre-rename-retry",
    interactionCreatedAtMs: 1_700_000_000_006,
  };
  failNextInteractionOutboxWriteBeforeRenameForTest();
  assert.throws(
    () => recovering.reserve(retried),
    InteractionOutboxRecoverablePersistenceError,
  );
  assert.deepEqual(recovering.pending(), []);
  assert.equal(recovering.unavailableReason(), null);
  assert.equal(recovering.status().accepting, true);
  // The next write is safe: no global poison is retained for the rolled-back
  // mutation, and the outbox can accept a later remote turn.
  recovering.reserve(retried);
  recovering.promote(retried);
  assert.deepEqual(new InteractionOutbox(recoveryPath, 2).pending(), [retried]);

  // A first write into a nested state path must sync the parent of every
  // directory created by recursive mkdir, followed by the final directory
  // sync after the snapshot rename.
  const nestedOutboxPath = join(directory, "created", "state", "outbox.json");
  const nestedDirectorySyncs = [];
  setInteractionOutboxDirectorySyncHookForTest((syncedPath) => nestedDirectorySyncs.push(syncedPath));
  try {
    const nestedOutbox = new InteractionOutbox(nestedOutboxPath, 2);
    nestedOutbox.reserve({
      laneId: "telepathos:direct",
      interactionId: "i-nested-state",
      interactionCreatedAtMs: 1_700_000_000_011,
    });
  } finally {
    setInteractionOutboxDirectorySyncHookForTest(null);
  }
  assert.deepEqual(nestedDirectorySyncs, [
    directory,
    join(directory, "created"),
    join(directory, "created", "state"),
  ]);

  const cancelRecoveryPath = join(directory, "cancel-recovery.json");
  const cancelRecovery = new InteractionOutbox(cancelRecoveryPath, 1);
  const canceled = {
    laneId: "telepathos:direct",
    interactionId: "i-cancel-pre-rename",
    interactionCreatedAtMs: 1_700_000_000_008,
  };
  const afterRecovery = {
    ...canceled,
    interactionId: "i-cancel-after-recovery",
    interactionCreatedAtMs: 1_700_000_000_009,
  };
  const afterSecondCancel = {
    ...canceled,
    interactionId: "i-cancel-after-second-recovery",
    interactionCreatedAtMs: 1_700_000_000_010,
  };
  cancelRecovery.reserve(canceled);
  failNextInteractionOutboxWriteBeforeRenameForTest();
  assert.throws(
    () => cancelRecovery.cancelReservation(canceled),
    InteractionOutboxRecoverablePersistenceError,
  );
  // The failed delete leaves only a pre-STT reservation. It is retryable and
  // must not be treated as a pending remote side effect.
  assert.deepEqual(cancelRecovery.pending(), []);
  assert.deepEqual(cancelRecovery.status(), {
    capacity: 1,
    used: 1,
    pending: 0,
    reserved: 1,
    expired: 0,
    accepting: false,
  });

  // Storage has recovered. The next reservation sweeps the abandoned row
  // before checking capacity, then admits the new reservation.
  cancelRecovery.reserve(afterRecovery);
  assert.deepEqual(cancelRecovery.pending(), []);
  assert.deepEqual(cancelRecovery.status(), {
    capacity: 1,
    used: 1,
    pending: 0,
    reserved: 1,
    expired: 0,
    accepting: false,
  });
  cancelRecovery.cancelReservation(afterRecovery);

  // A later reservation remains possible after the recovered cleanup; a
  // canceled reservation never becomes a durable pending remote interaction.
  cancelRecovery.reserve(afterSecondCancel);
  assert.deepEqual(cancelRecovery.pending(), []);

  const shortWritePath = join(directory, "short-write.json");
  const shortWrite = new InteractionOutbox(shortWritePath, 2);
  const chunked = {
    laneId: "telepathos:direct",
    interactionId: "i-short-write",
    interactionCreatedAtMs: 1_700_000_000_007,
  };
  setInteractionOutboxWriteChunkLimitForTest(1);
  try {
    shortWrite.reserve(chunked);
    shortWrite.promote(chunked);
  } finally {
    setInteractionOutboxWriteChunkLimitForTest(null);
  }
  assert.deepEqual(new InteractionOutbox(shortWritePath, 2).pending(), [chunked]);

  const ackPath = join(directory, "reply-acks.json");
  const ackStore = new ReplyAckStore(ackPath);
  const binding = {
    targetIdentity,
    installationId: "interaction-outbox-installation",
    laneId: "telepathos:direct",
    replyTo: "tp-1",
    afterSeq: 4,
    throughSeq: 6,
    turnToken: "turn-1",
    interactionId: "i-bridge-1",
    replyText: "durable reply text",
    state: "received",
    preparedAtMs: 1_700_000_000_000,
    ownerLastSeenAtMs: 1_700_000_000_100,
    receivedAtMs: 1_700_000_000_200,
    consumedAtMs: null,
  };
  ackStore.save([binding]);
  assert.deepEqual(new ReplyAckStore(ackPath).load(), [binding]);
  const bindings = Array.from({ length: 64 }, (_, index) => ({
    ...binding,
    replyTo: `tp-${index}`,
    afterSeq: index * 2,
    throughSeq: index * 2 + 1,
    turnToken: `turn-${index}`,
    interactionId: `i-${index}`,
  }));
  ackStore.save(bindings);
  assert.throws(() => ackStore.save([...bindings, { ...binding, replyTo: "tp-overflow", afterSeq: 200, throughSeq: 201 }]),
    ReplyAckStoreCapacityError);
  // A confirmed consumption frees a durable slot; the next receipt can be saved.
  ackStore.save([...bindings.slice(1), { ...binding, replyTo: "tp-next", afterSeq: 202, throughSeq: 203 }]);
  assert.equal(new ReplyAckStore(ackPath).load().length, 64);
  console.log("INTERACTION OUTBOX TEST PASS");
} finally {
  await rm(directory, { recursive: true, force: true });
}
