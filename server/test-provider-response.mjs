import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  PROVIDER_RESPONSE_MAX_BYTES,
  ProviderResponseError,
  boundedProviderText,
  fetchProviderJson,
  readProviderJson,
} from "./dist/provider-response.js";
import { MAX_REPLY_TEXT_BYTES } from "./dist/reply-text.js";

function chunkedResponse(chunks, { status = 200, headers = {} } = {}) {
  return new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  }), { status, headers });
}

function expectProviderFailure(promise, failure) {
  return assert.rejects(promise, (error) =>
    error instanceof ProviderResponseError &&
    error.failure === failure &&
    error.message === "provider unavailable",
  );
}

const acceptText = (value) => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  return boundedProviderText(value.text) === null ? null : value.text;
};

// The body limit is enforced on the actual stream, irrespective of a lying
// Content-Length. Error bodies are cancelled without decoding or exposing
// their content.
await expectProviderFailure(
  readProviderJson(chunkedResponse([Buffer.alloc(129)]), acceptText, 128),
  "too-large",
);
let hugeErrorCancelled = false;
const hugeError = new Response(new ReadableStream({
  start(controller) {
    controller.enqueue(Buffer.alloc(PROVIDER_RESPONSE_MAX_BYTES + 1));
  },
  cancel() {
    hugeErrorCancelled = true;
  },
}), { status: 503 });
await expectProviderFailure(readProviderJson(hugeError, acceptText), "http-error");
await new Promise((resolve) => setTimeout(resolve, 0));
assert(hugeErrorCancelled, "non-2xx provider streams must be cancelled without reading their bodies");
await expectProviderFailure(
  readProviderJson(
    chunkedResponse([Buffer.alloc(129)], { headers: { "Content-Length": "1" } }),
    acceptText,
    128,
  ),
  "too-large",
);
await expectProviderFailure(
  readProviderJson(
    chunkedResponse([Buffer.from("{}")], {
      headers: { "Content-Length": `1${"0".repeat(1000)}` },
    }),
    acceptText,
  ),
  "too-large",
);
await expectProviderFailure(
  readProviderJson(chunkedResponse([Buffer.from([0xc3, 0x28])]), acceptText),
  "invalid-utf8",
);
await expectProviderFailure(
  readProviderJson(chunkedResponse([Buffer.from('{"text":')]), acceptText),
  "invalid-json",
);
await expectProviderFailure(
  readProviderJson(chunkedResponse([Buffer.from('{"unexpected":true}')]), acceptText),
  "invalid-schema",
);

const exactEnvelope = Buffer.from(JSON.stringify({ text: "a".repeat(MAX_REPLY_TEXT_BYTES) }));
assert.equal(
  await readProviderJson(chunkedResponse([exactEnvelope]), acceptText),
  "a".repeat(MAX_REPLY_TEXT_BYTES),
  "the exact UTF-8 text limit must remain valid",
);
assert.equal(boundedProviderText("a".repeat(MAX_REPLY_TEXT_BYTES + 1)), null);

process.env.TELEPATHOS_STT = "openai";
const { transcribe } = await import("./dist/transcriber.js");
const { runMetaAgent } = await import("./dist/meta-agent.js");

const originalFetch = globalThis.fetch;
try {
  const postReadAbort = new Error("cancelled after provider response");
  const postReadController = new AbortController();
  globalThis.fetch = async () => chunkedResponse([
    Buffer.from(JSON.stringify({ text: "still-valid" })),
  ]);
  await assert.rejects(
    fetchProviderJson(
      "https://provider.invalid/transcribe",
      { signal: postReadController.signal },
      (value) => {
        const text = acceptText(value);
        postReadController.abort(postReadAbort);
        return text;
      },
    ),
    (error) => error === postReadAbort,
    "provider cancellation must win even after the body has been read",
  );

  globalThis.fetch = async () => chunkedResponse([
    Buffer.from(JSON.stringify({ text: "a".repeat(MAX_REPLY_TEXT_BYTES) })),
  ]);
  assert.equal((await transcribe(Buffer.from("audio")))?.text.length, MAX_REPLY_TEXT_BYTES,
    "STT accepts an exact-boundary valid response");

  globalThis.fetch = async () => new Response("provider-secret=stt-secret", { status: 500 });
  await expectProviderFailure(transcribe(Buffer.from("audio")), "http-error");

  globalThis.fetch = async () => { throw new Error("provider-secret=transport-secret"); };
  await expectProviderFailure(transcribe(Buffer.from("audio")), "transport");

  const registry = {
    lanes: [{ id: "telepathos:direct", name: "direct", createdAt: "", lastActive: "" }],
    activeId: "telepathos:direct",
    previousId: "telepathos:direct",
  };
  const metaConfig = { baseUrl: "http://provider.invalid", apiKey: "meta-key", model: "meta" };

  globalThis.fetch = async () => chunkedResponse([
    Buffer.from(JSON.stringify({ choices: [{ message: { content: "a".repeat(MAX_REPLY_TEXT_BYTES) } }] })),
  ]);
  assert.equal((await runMetaAgent(metaConfig, structuredClone(registry), "status")).length, MAX_REPLY_TEXT_BYTES,
    "meta model accepts an exact-boundary valid response");

  globalThis.fetch = async () => chunkedResponse([
    Buffer.from(JSON.stringify({ choices: [{ message: { content: "a".repeat(MAX_REPLY_TEXT_BYTES + 1) } }] })),
  ]);
  await expectProviderFailure(runMetaAgent(metaConfig, structuredClone(registry), "status"), "invalid-schema");

  globalThis.fetch = async () => new Response("provider-secret=meta-secret", { status: 429 });
  await expectProviderFailure(runMetaAgent(metaConfig, structuredClone(registry), "status"), "http-error");

  globalThis.fetch = async () => chunkedResponse([Buffer.from('{"choices":[]}')]);
  await expectProviderFailure(runMetaAgent(metaConfig, structuredClone(registry), "status"), "invalid-schema");

  globalThis.fetch = async () => chunkedResponse([Buffer.from([0xc3, 0x28])]);
  await expectProviderFailure(runMetaAgent(metaConfig, structuredClone(registry), "status"), "invalid-utf8");
} finally {
  globalThis.fetch = originalFetch;
}

const lanesDir = await mkdtemp(join(tmpdir(), "telepathos-provider-error-test-"));
process.env.TELEPATHOS_LANES = join(lanesDir, "lanes.json");
try {
  const { phoneSafeErrorMessage } = await import("./dist/index.js");
  const { LaneNameError, LanePersistenceError } = await import("./dist/lanes.js");
  const { ReplyTextLimitError } = await import("./dist/reply-text.js");
  const providerMessage = phoneSafeErrorMessage(new ProviderResponseError("http-error"));
  assert.equal(providerMessage, "provider unavailable");
  assert(!providerMessage.includes("secret"), "handset provider errors must never include provider bodies");
  assert.equal(phoneSafeErrorMessage(new ProviderResponseError("transport"), "stt"), "stt provider unavailable");
  assert.equal(
    phoneSafeErrorMessage(new LanePersistenceError("pre-rename", `cannot persist lane registry ${process.env.TELEPATHOS_LANES}: secret-path`)),
    "request failed",
    "lane persistence details must use the stable handset fallback",
  );
  assert.equal(
    phoneSafeErrorMessage(new Error("arbitrary provider secret /var/private/credentials")),
    "request failed",
    "arbitrary exception details must use the stable handset fallback",
  );
  assert.equal(
    phoneSafeErrorMessage(new Error("arbitrary STT secret /var/private/audio"), "stt"),
    "stt provider unavailable",
    "unexpected STT details must preserve the stable provider-unavailable wording",
  );
  assert.equal(
    phoneSafeErrorMessage(new LaneNameError("lane name must not be blank")),
    "lane name must not be blank",
    "allowlisted public lane errors remain stable",
  );
  for (const original of [
    "reply exceeds the 512 KiB UTF-8 byte limit",
    `reply exceeds ${MAX_REPLY_TEXT_BYTES} UTF-8 byte limit`,
    "reply-size secret /var/private/reply",
  ]) {
    assert.equal(
      phoneSafeErrorMessage(new ReplyTextLimitError(original)),
      "reply exceeds the 512 KiB UTF-8 byte limit",
      `ReplyTextLimitError must use the canonical safe message: ${original}`,
    );
  }
} finally {
  await rm(lanesDir, { recursive: true, force: true });
}

console.log("PROVIDER RESPONSE TESTS PASS");
