// Regression tests for aborting both STT backends.
import assert from "node:assert/strict";
import { access, chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForFile(path, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await access(path);
      return;
    } catch {
      await sleep(10);
    }
  }
  throw new Error(`timed out waiting for ${path}`);
}

async function waitForRequests(path, count, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const requests = (await readFile(path, "utf8")).trim().split("\n").filter(Boolean);
      if (requests.length >= count) return;
    } catch {
      // The worker has not received its first request yet.
    }
    await sleep(10);
  }
  throw new Error(`timed out waiting for ${count} fake-worker requests`);
}

async function expectProviderFailure(request, rawDetail) {
  await assert.rejects(request, (error) => {
    assert.equal(error?.name, "ProviderResponseError");
    assert.equal(error?.message, "provider unavailable");
    if (rawDetail) assert.doesNotMatch(String(error), new RegExp(rawDetail));
    return true;
  });
}

async function testOpenAiCancellation() {
  process.env.TELEPATHOS_STT = "openai";
  const { transcribe } = await import("./dist/transcriber.js");
  const controller = new AbortController();
  let receivedSignal;
  globalThis.fetch = async (_url, init) => {
    receivedSignal = init.signal;
    return await new Promise((_resolve, reject) => {
      init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true });
    });
  };

  const request = transcribe(Buffer.from("audio"), controller.signal);
  await Promise.resolve();
  assert.equal(receivedSignal, controller.signal, "OpenAI fetch must receive the caller signal");
  controller.abort();
  await assert.rejects(request, (error) => error === controller.signal.reason);
}

async function testLocalCancellation(markerPath) {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_FAKE_WORKER_DELAY = "1000";
  process.env.TELEPATHOS_FAKE_WORKER_MARKER = markerPath;
  const { transcribe } = await import("./dist/transcriber.js");
  const controller = new AbortController();
  const request = transcribe(Buffer.from("audio"), controller.signal);
  await waitForFile(markerPath);
  controller.abort();
  await assert.rejects(request, (error) => error === controller.signal.reason);

  // A canceled request must stop the worker, not leave it busy until its
  // original inference completes and delay the next request.
  process.env.TELEPATHOS_FAKE_WORKER_DELAY = "10";
  const started = Date.now();
  const next = await transcribe(Buffer.from("audio"));
  assert.equal(next.text, "after-cancel");
  assert.ok(Date.now() - started < 500, "the next request must use a restarted worker");
}

async function testLocalConcurrentCancellation(markerPath) {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_FAKE_WORKER_DELAY = "25";
  process.env.TELEPATHOS_FAKE_WORKER_MARKER = markerPath;
  process.env.TELEPATHOS_FAKE_WORKER_EXPECTED_REQUESTS = "2";
  const { transcribe } = await import("./dist/transcriber.js");
  const controller = new AbortController();
  const canceled = transcribe(Buffer.from("first"), controller.signal);
  const survivor = transcribe(Buffer.from("second"));

  // Abort only after both requests have reached the same singleton worker.
  await waitForRequests(markerPath, 2);
  controller.abort();
  await assert.rejects(canceled, (error) => error === controller.signal.reason);
  assert.deepEqual(await survivor, { text: "after-cancel", confidence: 0.9 });
}

async function testLocalVocabPrompt(markerPath, vocabPath) {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_VOCAB_FILE = vocabPath;
  process.env.TELEPATHOS_FAKE_WORKER_DELAY = "10";
  process.env.TELEPATHOS_FAKE_WORKER_MARKER = markerPath;
  const { transcribe } = await import("./dist/transcriber.js");
  const result = await transcribe(Buffer.from("audio"));
  assert.deepEqual(result, { text: "after-cancel", confidence: 0.9 });
  const requests = (await readFile(markerPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  assert.equal(requests[0].prompt, "Telepathos, LaneRegistry");
}

async function testLocalWorkerProtocol(markerPath) {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_FAKE_WORKER_MODE = "overflow";
  process.env.TELEPATHOS_FAKE_WORKER_MARKER = markerPath;
  process.env.TELEPATHOS_FAKE_WORKER_EXPECTED_REQUESTS = "2";
  const { transcribe } = await import("./dist/transcriber.js");

  // A broken shared worker must fail every request it owns, not only the
  // request whose response happens to overflow first.
  const first = expectProviderFailure(transcribe(Buffer.from("first")));
  const second = expectProviderFailure(transcribe(Buffer.from("second")));
  await waitForRequests(markerPath, 2);
  await Promise.all([first, second]);

  // Overflow kills the process. A subsequent request must receive a new
  // worker rather than inheriting its incomplete byte buffer.
  process.env.TELEPATHOS_FAKE_WORKER_MODE = "normal";
  delete process.env.TELEPATHOS_FAKE_WORKER_EXPECTED_REQUESTS;
  const recovered = await transcribe(Buffer.from("recovered"));
  assert.deepEqual(recovered, { text: "after-cancel", confidence: 0.9 });
}

async function testLocalWorkerInvalidOutput(mode, rawDetail) {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_FAKE_WORKER_MODE = mode;
  const { transcribe } = await import("./dist/transcriber.js");
  await expectProviderFailure(transcribe(Buffer.from("audio")), rawDetail);
}

async function testLocalWorkerSplitUtf8() {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_FAKE_WORKER_MODE = "split-utf8";
  const { transcribe } = await import("./dist/transcriber.js");
  assert.deepEqual(await transcribe(Buffer.from("audio")), {
    text: "café 🎙",
    confidence: 0.9,
  });
}

async function testLocalWorkerEscapedExactBound() {
  process.env.TELEPATHOS_STT = "local";
  process.env.TELEPATHOS_FAKE_WORKER_MODE = "escaped-exact-bound";
  const { MAX_REPLY_TEXT_BYTES } = await import("./dist/reply-text.js");
  const { transcribe } = await import("./dist/transcriber.js");
  const transcript = await transcribe(Buffer.from("audio"));
  assert.equal(Buffer.byteLength(transcript.text, "utf8"), MAX_REPLY_TEXT_BYTES);
  assert.equal(transcript.text.length, MAX_REPLY_TEXT_BYTES);
}

async function runCase(name, env) {
  const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {
    env: { ...process.env, ...env, TELEPATHOS_TRANSCRIBER_CASE: name },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout.on("data", (chunk) => { output += chunk; });
  child.stderr.on("data", (chunk) => { output += chunk; });
  await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${name} case failed (code=${code}, signal=${signal})\n${output}`));
    });
  });
}

const caseName = process.env.TELEPATHOS_TRANSCRIBER_CASE;
if (caseName === "openai") {
  await testOpenAiCancellation();
  console.log("OPENAI TRANSCRIBER CANCELLATION PASS");
} else if (caseName === "local") {
  await testLocalCancellation(process.env.TELEPATHOS_FAKE_WORKER_MARKER);
  console.log("LOCAL TRANSCRIBER CANCELLATION PASS");
} else if (caseName === "local-concurrent") {
  await testLocalConcurrentCancellation(process.env.TELEPATHOS_FAKE_WORKER_MARKER);
  console.log("LOCAL CONCURRENT TRANSCRIBER CANCELLATION PASS");
} else if (caseName === "local-vocab") {
  await testLocalVocabPrompt(
    process.env.TELEPATHOS_FAKE_WORKER_MARKER,
    process.env.TELEPATHOS_VOCAB_FILE,
  );
  console.log("LOCAL VOCAB PROMPT PASS");
} else if (caseName === "local-worker-protocol") {
  await testLocalWorkerProtocol(process.env.TELEPATHOS_FAKE_WORKER_MARKER);
  console.log("LOCAL WORKER OVERFLOW AND RECOVERY PASS");
} else if (caseName === "local-worker-oversized") {
  await testLocalWorkerInvalidOutput("oversized");
  console.log("LOCAL WORKER OVERSIZED TRANSCRIPT PASS");
} else if (caseName === "local-worker-invalid-utf8") {
  await testLocalWorkerInvalidOutput("invalid-utf8");
  console.log("LOCAL WORKER INVALID UTF-8 PASS");
} else if (caseName === "local-worker-invalid-json") {
  await testLocalWorkerInvalidOutput("invalid-json");
  console.log("LOCAL WORKER INVALID JSON PASS");
} else if (caseName === "local-worker-invalid-schema") {
  await testLocalWorkerInvalidOutput("invalid-schema");
  console.log("LOCAL WORKER INVALID SCHEMA PASS");
} else if (caseName === "local-worker-invalid-confidence") {
  await testLocalWorkerInvalidOutput("invalid-confidence");
  console.log("LOCAL WORKER INVALID CONFIDENCE PASS");
} else if (caseName === "local-worker-wrong-id") {
  await testLocalWorkerInvalidOutput("wrong-id");
  console.log("LOCAL WORKER WRONG ID PASS");
} else if (caseName === "local-worker-raw-error") {
  await testLocalWorkerInvalidOutput("raw-error", "private worker failure");
  console.log("LOCAL WORKER ERROR SANITIZATION PASS");
} else if (caseName === "local-worker-split-utf8") {
  await testLocalWorkerSplitUtf8();
  console.log("LOCAL WORKER SPLIT UTF-8 PASS");
} else if (caseName === "local-worker-escaped-exact-bound") {
  await testLocalWorkerEscapedExactBound();
  console.log("LOCAL WORKER ESCAPED EXACT BOUND PASS");
} else {
  const temp = await mkdtemp(join(tmpdir(), "telepathos-transcriber-test-"));
  const fakePython = join(temp, "python3");
  const marker = join(temp, "request-received");
  const concurrentMarker = join(temp, "concurrent-requests-received");
  const vocabPath = join(temp, "vocab.txt");
  await writeFile(vocabPath, "Telepathos\n\nLaneRegistry\n");
  await writeFile(fakePython, `#!/usr/bin/env node
import fs from "node:fs";

process.stdout.write(JSON.stringify({ event: "ready", model: "fake" }) + "\\n");
let buffer = "";
const requests = [];
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let index;
  while ((index = buffer.indexOf("\\n")) >= 0) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (!line.trim()) continue;
    const request = JSON.parse(line);
    requests.push(request);
    fs.appendFileSync(process.env.TELEPATHOS_FAKE_WORKER_MARKER, JSON.stringify(request) + "\\n");
    const expectedRequests = Number(process.env.TELEPATHOS_FAKE_WORKER_EXPECTED_REQUESTS ?? "1");
    if (requests.length < expectedRequests) continue;
    const mode = process.env.TELEPATHOS_FAKE_WORKER_MODE ?? "normal";
    if (mode === "overflow") {
      // Greater than the sixfold escaped-transcript allowance.
      process.stdout.write(Buffer.alloc(4 * 1024 * 1024, 0x61));
      continue;
    }
    if (mode === "oversized") {
      process.stdout.write(JSON.stringify({ id: request.id, text: "x".repeat(512 * 1024 + 1) }) + "\\n");
      continue;
    }
    if (mode === "invalid-utf8") {
      process.stdout.write(Buffer.concat([
        Buffer.from('{"id":' + JSON.stringify(request.id) + ',"text":"bad'),
        Buffer.from([0xc3, 0x28]),
        Buffer.from('"}\\n'),
      ]));
      continue;
    }
    if (mode === "invalid-json") {
      process.stdout.write('{"id":' + JSON.stringify(request.id) + ',"text":\\n');
      continue;
    }
    if (mode === "invalid-schema") {
      process.stdout.write(JSON.stringify({ id: request.id, text: 42 }) + "\\n");
      continue;
    }
    if (mode === "invalid-confidence") {
      process.stdout.write(JSON.stringify({ id: request.id, text: "valid", confidence: 1.1 }) + "\\n");
      continue;
    }
    if (mode === "wrong-id") {
      process.stdout.write(JSON.stringify({ id: "unrelated-request", text: "valid" }) + "\\n");
      continue;
    }
    if (mode === "raw-error") {
      process.stdout.write(
        JSON.stringify({ id: request.id, error: "private worker failure /tmp/secret" }) + "\\n",
        () => process.exit(0),
      );
      continue;
    }
    if (mode === "split-utf8") {
      const response = Buffer.from(JSON.stringify({ id: request.id, text: "café 🎙", confidence: 0.9 }) + "\\n");
      const splitAt = response.indexOf(Buffer.from("é")) + 1;
      process.stdout.write(response.subarray(0, splitAt));
      setTimeout(() => process.stdout.write(response.subarray(splitAt), () => process.exit(0)), 10);
      continue;
    }
    if (mode === "escaped-exact-bound") {
      process.stdout.write(JSON.stringify({ id: request.id, text: "\\0".repeat(512 * 1024) }) + "\\n", () => process.exit(0));
      continue;
    }
    const delay = Number(process.env.TELEPATHOS_FAKE_WORKER_DELAY ?? "1000");
    setTimeout(() => {
      for (const pending of requests) {
        process.stdout.write(JSON.stringify({ id: pending.id, text: "after-cancel", confidence: 0.9 }) + "\\n");
      }
      process.exit(0);
    }, delay);
  }
});
`, { mode: 0o755 });
  try {
    await runCase("openai", { TELEPATHOS_STT: "openai" });
    await runCase("local", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: marker,
    });
    await runCase("local-concurrent", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: concurrentMarker,
    });
    await runCase("local-vocab", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "vocab-requests"),
      TELEPATHOS_VOCAB_FILE: vocabPath,
    });
    await runCase("local-worker-protocol", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "protocol-requests"),
    });
    await runCase("local-worker-oversized", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "oversized-requests"),
    });
    await runCase("local-worker-invalid-utf8", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "invalid-utf8-requests"),
    });
    await runCase("local-worker-invalid-json", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "invalid-json-requests"),
    });
    await runCase("local-worker-invalid-schema", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "invalid-schema-requests"),
    });
    await runCase("local-worker-invalid-confidence", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "invalid-confidence-requests"),
    });
    await runCase("local-worker-wrong-id", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "wrong-id-requests"),
    });
    await runCase("local-worker-raw-error", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "raw-error-requests"),
    });
    await runCase("local-worker-split-utf8", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "split-utf8-requests"),
    });
    await runCase("local-worker-escaped-exact-bound", {
      PATH: `${temp}:${process.env.PATH ?? ""}`,
      TELEPATHOS_STT: "local",
      TELEPATHOS_FAKE_WORKER_MARKER: join(temp, "escaped-exact-bound-requests"),
    });
    console.log("TRANSCRIBER CANCELLATION TESTS PASS");
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
}
