import assert from "node:assert/strict";

const { ReplyAckOwnerHighWaterCache } = await import("./dist/reply-ack-owner-cache.js");

const activeOwners = new Map();
let durableBindings = [];
const cache = new ReplyAckOwnerHighWaterCache(1_000);
const prune = () => cache.prune(activeOwners, durableBindings);

// A connect/disconnect stream with unique, unbound installation IDs must not
// leave one cache entry per authenticated client.
for (let index = 0; index < 10_000; index++) {
  const installationId = `unbound-${index}`;
  activeOwners.set(installationId, 1);
  cache.note(installationId, 2_000 + index);
  prune();
  activeOwners.delete(installationId);
  prune();
}
assert.equal(cache.size(), 0);

// A durable binding keeps its owner's high-water mark, including across a
// rollback. Removing the binding makes the inactive owner collectible.
const durableOwner = "durable-owner";
cache.note(durableOwner, 5_000);
durableBindings = [{ installationId: durableOwner }];
prune();
cache.note(durableOwner, 4_000);
assert.equal(cache.lastSeenAt(durableOwner), 5_000);
assert.equal(cache.size(), 1);
durableBindings = [];
prune();
assert.equal(cache.size(), 0);

// Active owners are retained even without a durable binding. Same-owner
// socket counts keep the mark until the final socket disconnects.
const activeOwner = "active-owner";
activeOwners.set(activeOwner, 2);
cache.note(activeOwner, 6_000);
prune();
assert.equal(cache.size(), 1);
activeOwners.set(activeOwner, 1);
prune();
assert.equal(cache.size(), 1);
activeOwners.delete(activeOwner);
prune();
assert.equal(cache.size(), 0);

// Migration changes the durable reference, so the old inactive owner's mark
// can be removed while the new owner's rollback-safe mark remains.
const oldOwner = "old-owner";
const newOwner = "new-owner";
cache.note(oldOwner, 7_000);
durableBindings = [{ installationId: oldOwner }];
prune();
cache.note(newOwner, 8_000);
durableBindings = [{ installationId: newOwner }];
prune();
assert.equal(cache.lastSeenAt(oldOwner), undefined);
assert.equal(cache.lastSeenAt(newOwner), 8_000);
cache.note(newOwner, 7_500);
assert.equal(cache.lastSeenAt(newOwner), 8_000);

console.log("REPLY ACK OWNER CACHE TEST PASS");
