import assert from "node:assert/strict";
import { sharedTokenFromHeaders, sharedTokenMatches } from "./dist/api.js";

const configured = "configured-shared-token";
const equalLengthInvalid = "x".repeat(configured.length);
assert.equal(equalLengthInvalid.length, configured.length);
assert.notEqual("wrong".length, configured.length);

assert.equal(sharedTokenMatches(configured, configured), true, "the exact token is accepted");
assert.equal(
  sharedTokenMatches(configured, equalLengthInvalid),
  false,
  "an equal-length invalid token is rejected",
);
assert.equal(
  sharedTokenMatches(configured, "wrong"),
  false,
  "a different-length invalid token is rejected",
);
assert.equal(sharedTokenMatches(configured, undefined), false, "a missing presented token is rejected");
assert.equal(sharedTokenMatches(undefined, configured), false, "a missing configured token never authenticates directly");
assert.equal(sharedTokenMatches("", configured), false, "an empty configured token is not authenticated");

assert.equal(
  sharedTokenMatches(configured, sharedTokenFromHeaders({ "x-telepathos-token": configured })),
  true,
  "the shared-token header is accepted",
);
assert.equal(
  sharedTokenMatches(configured, sharedTokenFromHeaders({ authorization: `Bearer ${configured}` })),
  true,
  "the Bearer authorization header is accepted",
);
assert.equal(
  sharedTokenMatches(configured, sharedTokenFromHeaders({
    "x-telepathos-token": equalLengthInvalid,
    authorization: `Bearer ${configured}`,
  })),
  false,
  "the shared-token header takes precedence over Bearer",
);
assert.equal(sharedTokenFromHeaders({}), undefined, "missing headers produce no token");
assert.equal(sharedTokenFromHeaders({ "x-telepathos-token": [configured] }), undefined, "malformed header values are rejected");
assert.equal(sharedTokenFromHeaders({ authorization: ["Bearer ", configured] }), undefined, "malformed authorization values are rejected");
assert.equal(sharedTokenFromHeaders({ authorization: configured }), undefined, "non-Bearer authorization is ignored");

for (const malformed of [null, 42, {}, [], [configured]]) {
  assert.doesNotThrow(
    () => sharedTokenMatches(configured, malformed),
    `malformed token input must not throw: ${Object.prototype.toString.call(malformed)}`,
  );
  assert.equal(
    sharedTokenMatches(configured, malformed),
    false,
    `malformed token input must be rejected: ${Object.prototype.toString.call(malformed)}`,
  );
}

console.log("AUTH TESTS PASS");
