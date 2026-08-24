// Regression test for the compiled local-STT worker path.
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { localWorkerScriptPath } from "./dist/transcriber.js";

const expected = fileURLToPath(new URL("./scripts/whisper_worker.py", import.meta.url));
const actual = localWorkerScriptPath();

assert.equal(actual, expected, "compiled local STT must resolve the server worker");
assert.ok(existsSync(actual), `worker script does not exist: ${actual}`);
console.log("TRANSCRIBER PATH TEST PASS");
