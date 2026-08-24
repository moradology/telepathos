import assert from "node:assert/strict";

const {
  MAX_REPLY_TEXT_BYTES,
  ReplyTextByteAccumulator,
  isReplyTextWithinLimit,
  utf8ByteLength,
} = await import("./dist/reply-text.js");

assert.equal(utf8ByteLength("a".repeat(MAX_REPLY_TEXT_BYTES)), MAX_REPLY_TEXT_BYTES);
assert(isReplyTextWithinLimit("a".repeat(MAX_REPLY_TEXT_BYTES)));
assert(!isReplyTextWithinLimit("a".repeat(MAX_REPLY_TEXT_BYTES + 1)));
assert.equal(utf8ByteLength("🦀"), 4);
assert(isReplyTextWithinLimit("🦀".repeat(MAX_REPLY_TEXT_BYTES / 4)));
assert(!isReplyTextWithinLimit("🦀".repeat(MAX_REPLY_TEXT_BYTES / 4) + "x"));

const splitEmoji = new ReplyTextByteAccumulator();
assert(splitEmoji.append("\ud83e"));
assert(splitEmoji.append("\udd80"));
assert.equal(splitEmoji.byteLength(), 4, "chunked multibyte text has exact UTF-8 accounting");

const boundary = new ReplyTextByteAccumulator();
assert(boundary.append("a".repeat(MAX_REPLY_TEXT_BYTES - 1)));
assert(!boundary.append("é"), "a two-byte delta must be rejected before append at the boundary");
assert.equal(boundary.byteLength(), MAX_REPLY_TEXT_BYTES - 1);

const over = new ReplyTextByteAccumulator();
assert(!over.append("🦀".repeat(MAX_REPLY_TEXT_BYTES / 4 + 1)));
assert.equal(over.byteLength(), 0, "overflow does not retain a partial terminal reply");
console.log("REPLY TEXT TEST PASS");
