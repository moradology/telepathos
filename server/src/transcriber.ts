import { spawn, ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { writeFile, unlink } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { config } from "./config.js";
import {
  boundedProviderText,
  fetchProviderJson,
  ProviderResponseError,
} from "./provider-response.js";
import { MAX_REPLY_TEXT_BYTES } from "./reply-text.js";

/**
 * STT for a complete utterance (WAV bytes). Backends (TELEPATHOS_STT):
 *  - "openai" : whisper-1 API — no confidence score available
 *  - "local"  : faster-whisper via scripts/whisper_worker.py — confidence +
 *               vocabulary boosting (initial_prompt); the 3090 endgame
 *  - "echo"   : dev stub, no transcription
 *
 * All backends return Transcript; `confidence` is undefined when unknown.
 */
export interface Transcript {
  text: string;
  /** mean token probability [0..1] when the backend reports it */
  confidence?: number;
}

function parseOpenAiTranscript(value: unknown): Transcript | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const text = boundedProviderText((value as { text?: unknown }).text);
  return text === null ? null : { text };
}

/** Vocabulary hint: repo terms make Whisper stop mangling identifiers. */
function vocabPrompt(): string | undefined {
  const direct = process.env.TELEPATHOS_VOCAB;
  if (direct) return direct;
  // comma/newline separated file, generated from the repo (git ls-files etc.)
  const file = process.env.TELEPATHOS_VOCAB_FILE;
  if (file) {
    try {
      return readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean).join(", ").slice(0, 2000);
    } catch {
      return undefined;
    }
  }
  return undefined;
}

export async function transcribe(wav: Buffer, signal?: AbortSignal): Promise<Transcript | null> {
  if (config.stt === "echo") return null;
  if (config.stt === "openai") {
    signal?.throwIfAborted();
    const form = new FormData();
    form.append("file", new Blob([new Uint8Array(wav)]), "utterance.wav");
    form.append("model", "whisper-1");
    const prompt = vocabPrompt();
    if (prompt) form.append("prompt", prompt);
    return await fetchProviderJson("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: form,
      signal,
    }, parseOpenAiTranscript);
  }
  if (config.stt === "local") {
    const result = await localWorker().transcribe(wav, signal);
    return result;
  }
  throw new Error(`unknown stt backend: ${config.stt}`);
}

// ---- local faster-whisper worker ----

export function localWorkerScriptPath(): string {
  return fileURLToPath(new URL("../scripts/whisper_worker.py", import.meta.url));
}

interface Pending {
  proc: ChildProcess;
  resolve: (t: Transcript) => void;
  reject: (e: unknown) => void;
  timer: NodeJS.Timeout;
  signal?: AbortSignal;
  onAbort?: () => void;
}

/**
 * Python's default JSON encoder can expand a valid UTF-8 transcript by up to
 * six times: a one-byte ASCII control character becomes a six-byte
 * `\\u00xx` escape. (Non-BMP text expands only threefold as a surrogate-pair
 * escape.) Keep that worst-case allowance explicit while still bounding every
 * stdout line before it is materialized or decoded.
 */
const MAX_WORKER_LINE_BYTES = MAX_REPLY_TEXT_BYTES * 6 + 64 * 1024;
const MAX_WORKER_LABEL_BYTES = 256;
const MAX_ABANDONED_RESPONSE_IDS = 128;

/** A byte-oriented, line-delimited parser that never grows beyond its cap. */
class BoundedNdjsonReader {
  private parts: Buffer[] = [];
  private length = 0;

  constructor(private readonly maxLineBytes: number) {}

  push(chunk: Buffer, handleLine: (line: Buffer) => boolean): boolean {
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf(0x0a, start);
      const end = newline === -1 ? chunk.length : newline;
      if (!this.append(chunk.subarray(start, end))) return false;
      if (newline === -1) return true;

      const line = Buffer.concat(this.parts, this.length);
      this.parts = [];
      this.length = 0;
      if (!handleLine(line)) return false;
      start = newline + 1;
    }
    return true;
  }

  private append(part: Buffer): boolean {
    if (part.length > this.maxLineBytes - this.length) return false;
    if (part.length > 0) this.parts.push(part);
    this.length += part.length;
    return true;
  }
}

function isBoundedWorkerLabel(value: unknown): value is string {
  return typeof value === "string" && Buffer.byteLength(value, "utf8") <= MAX_WORKER_LABEL_BYTES;
}

class LocalWhisper {
  private proc?: ChildProcess;
  private pending = new Map<string, Pending>();
  /** One bounded late-response allowance per canceled/expired request. */
  private abandonedResponseIds = new Map<ChildProcess, Set<string>>();
  private seq = 0;

  async transcribe(wav: Buffer, signal?: AbortSignal): Promise<Transcript> {
    signal?.throwIfAborted();
    const proc = this.ensure();
    const id = `u${++this.seq}-${randomUUID()}`;
    const path = `/tmp/telepathos-utt-${id}.wav`;
    try {
      // Utterances are private voice data. Set the mode at creation so a
      // permissive process umask cannot expose the in-flight WAV to another
      // local user; the finally block removes it after the worker settles.
      await writeFile(path, wav, { signal, mode: 0o600, flag: "wx" });
      signal?.throwIfAborted();
      return await new Promise<Transcript>((resolve, reject) => {
        const onAbort = () => {
          this.abort(id, signal?.reason ?? new Error("stt aborted"));
        };
        const timer = setTimeout(() => {
          this.abandon(id, new ProviderResponseError("transport"));
        }, 60_000);
        timer.unref();
        this.pending.set(id, { proc, resolve, reject, timer, signal, onAbort });
        signal?.addEventListener("abort", onAbort, { once: true });
        if (signal?.aborted) {
          onAbort();
          return;
        }
        try {
          proc.stdin!.write(
            JSON.stringify({ id, path, prompt: vocabPrompt() }) + "\n",
            (error) => {
              if (error) this.failWorker(proc, "transport", "failed to write request", error);
            },
          );
        } catch (error) {
          this.failWorker(proc, "transport", "failed to write request", error);
        }
      });
    } finally {
      void unlink(path).catch(() => {});
    }
  }

  private resolve(id: string, transcript: Transcript): void {
    this.settle(id, (pending) => pending.resolve(transcript));
  }

  private reject(id: string, error: unknown, terminateWorker = false): void {
    const pending = this.pending.get(id);
    if (!pending || !this.settle(id, (current) => current.reject(error))) return;
    if (terminateWorker) this.terminate(pending.proc);
  }

  private abort(id: string, error: unknown): void {
    const pending = this.pending.get(id);
    if (!pending || !this.settle(id, (current) => current.reject(error))) return;

    // The worker is shared.  Do not turn one caller's cancellation into an
    // error for unrelated requests that are already using this process.
    if (![...this.pending.values()].some((current) => current.proc === pending.proc)) {
      this.terminate(pending.proc);
    } else if (!this.rememberAbandonedResponse(pending.proc, id)) {
      this.failWorker(pending.proc, "transport", "too many abandoned requests");
    }
  }

  /**
   * A timeout has the same late-response shape as cancellation, except the
   * caller receives a provider-safe failure rather than its AbortSignal.
   */
  private abandon(id: string, error: Error): void {
    const pending = this.pending.get(id);
    if (!pending || !this.settle(id, (current) => current.reject(error))) return;
    if (![...this.pending.values()].some((current) => current.proc === pending.proc)) {
      this.terminate(pending.proc);
    } else if (!this.rememberAbandonedResponse(pending.proc, id)) {
      this.failWorker(pending.proc, "transport", "too many abandoned requests");
    }
  }

  private settle(id: string, finish: (pending: Pending) => void): boolean {
    const pending = this.pending.get(id);
    if (!pending) return false;
    this.pending.delete(id);
    clearTimeout(pending.timer);
    if (pending.signal && pending.onAbort) {
      pending.signal.removeEventListener("abort", pending.onAbort);
    }
    finish(pending);
    return true;
  }

  private rejectPendingFor(proc: ChildProcess, error: Error): void {
    for (const [id, pending] of this.pending) {
      if (pending.proc === proc) this.reject(id, error);
    }
  }

  private rememberAbandonedResponse(proc: ChildProcess, id: string): boolean {
    let ids = this.abandonedResponseIds.get(proc);
    if (!ids) {
      ids = new Set();
      this.abandonedResponseIds.set(proc, ids);
    }
    if (ids.size >= MAX_ABANDONED_RESPONSE_IDS) return false;
    ids.add(id);
    return true;
  }

  /**
   * A worker owns multiple concurrent requests. A broken stdout protocol or
   * process must fail the whole ownership set before the process is replaced;
   * otherwise a later response could be incorrectly associated with a new
   * request on a restarted singleton.
   */
  private failWorker(
    proc: ChildProcess,
    failure: ConstructorParameters<typeof ProviderResponseError>[0],
    detail: string,
    error?: unknown,
  ): void {
    const stillOwned = this.proc === proc || [...this.pending.values()].some((pending) => pending.proc === proc);
    if (!stillOwned) return;
    if (error === undefined) console.warn(`whisper worker protocol failure: ${detail}`);
    else console.error(`whisper worker ${detail}:`, error);
    this.rejectPendingFor(proc, new ProviderResponseError(failure));
    this.terminate(proc);
  }

  private terminate(proc: ChildProcess): void {
    if (this.proc === proc) this.proc = undefined;
    this.abandonedResponseIds.delete(proc);
    if (!proc.killed) proc.kill();
  }

  private handleWorkerLine(proc: ChildProcess, line: Buffer): boolean {
    let value: unknown;
    try {
      value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(line)) as unknown;
    } catch (error) {
      const failure = error instanceof TypeError ? "invalid-utf8" : "invalid-json";
      this.failWorker(proc, failure, "invalid NDJSON response");
      return false;
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      this.failWorker(proc, "invalid-schema", "response is not an object");
      return false;
    }

    const message = value as Record<string, unknown>;
    if (message.event === "ready") {
      if (!isBoundedWorkerLabel(message.model) ||
          (message.device !== undefined && !isBoundedWorkerLabel(message.device))) {
        this.failWorker(proc, "invalid-schema", "invalid ready response");
        return false;
      }
      console.log("whisper worker ready");
      return true;
    }

    if (message.event !== undefined || typeof message.id !== "string") {
      this.failWorker(proc, "invalid-schema", "response is missing a request id");
      return false;
    }
    const pending = this.pending.get(message.id);
    if (!pending || pending.proc !== proc) {
      const abandoned = this.abandonedResponseIds.get(proc);
      if (abandoned?.delete(message.id)) return true;
      this.failWorker(proc, "invalid-schema", "response does not match a pending request");
      return false;
    }

    if (message.error !== undefined) {
      if (message.error !== "stt unavailable") {
        this.failWorker(proc, "invalid-schema", "invalid worker error response");
        return false;
      }
      // Worker errors are deliberately generic on the wire. Detailed Python
      // exceptions are emitted to the server's stderr instead.
      console.warn("whisper worker could not transcribe a request");
      this.reject(message.id, new ProviderResponseError("transport"));
      return true;
    }

    const text = boundedProviderText(message.text);
    if (text === null) {
      this.failWorker(proc, "invalid-schema", "invalid transcript text");
      return false;
    }
    const confidence = message.confidence;
    if (confidence !== undefined &&
        (typeof confidence !== "number" || !Number.isFinite(confidence) || confidence < 0 || confidence > 1)) {
      this.failWorker(proc, "invalid-schema", "invalid transcript confidence");
      return false;
    }
    this.resolve(message.id, confidence === undefined ? { text } : { text, confidence });
    return true;
  }

  private ensure(): ChildProcess {
    if (this.proc && this.proc.stdin!.writable) return this.proc;
    if (this.proc) this.failWorker(this.proc, "transport", "stdin is no longer writable");

    const proc = spawn("python3", [localWorkerScriptPath()], {
      env: process.env,
      stdio: ["pipe", "pipe", "inherit"],
    });
    this.proc = proc;
    console.log("whisper worker starting…");

    const output = new BoundedNdjsonReader(MAX_WORKER_LINE_BYTES);
    proc.stdout!.on("data", (d: Buffer) => {
      if (this.proc !== proc) return;
      if (!output.push(Buffer.from(d), (line) => this.handleWorkerLine(proc, line))) {
        this.failWorker(proc, "too-large", "stdout line exceeds its byte limit");
      }
    });

    proc.on("error", (error) => {
      this.failWorker(proc, "transport", "process error", error);
    });

    proc.on("exit", () => {
      console.warn("whisper worker exited");
      if (this.proc === proc) this.proc = undefined;
      this.abandonedResponseIds.delete(proc);
      this.rejectPendingFor(proc, new ProviderResponseError("transport"));
    });

    return proc;
  }
}

const localWorker_ = new LocalWhisper();
function localWorker() { return localWorker_; }
