import { spawn, ChildProcess } from "node:child_process";
import { writeFile, unlink } from "node:fs/promises";
import { config } from "./config.js";

/**
 * STT for a complete utterance (WAV bytes). Backends (TELEPATHY_STT):
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

/** Vocabulary hint: repo terms make Whisper stop mangling identifiers. */
function vocabPrompt(): string | undefined {
  const direct = process.env.TELEPATHY_VOCAB;
  if (direct) return direct;
  // comma/newline separated file, generated from the repo (git ls-files etc.)
  const file = process.env.TELEPATHY_VOCAB_FILE;
  if (file) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { readFileSync } = require("node:fs");
      return readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean).join(", ").slice(0, 2000);
    } catch {
      return undefined;
    }
  }
  return undefined;
}

export async function transcribe(wav: Buffer): Promise<Transcript | null> {
  if (config.stt === "echo") return null;
  if (config.stt === "openai") {
    const form = new FormData();
    form.append("file", new Blob([new Uint8Array(wav)]), "utterance.wav");
    form.append("model", "whisper-1");
    const prompt = vocabPrompt();
    if (prompt) form.append("prompt", prompt);
    const res = await fetch("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: form,
    });
    if (!res.ok) throw new Error(`stt failed: ${res.status} ${await res.text()}`);
    const json = (await res.json()) as { text: string };
    return { text: json.text };
  }
  if (config.stt === "local") {
    const result = await localWorker().transcribe(wav);
    return result;
  }
  throw new Error(`unknown stt backend: ${config.stt}`);
}

// ---- local faster-whisper worker ----

interface Pending {
  resolve: (t: Transcript) => void;
  reject: (e: Error) => void;
}

class LocalWhisper {
  private proc?: ChildProcess;
  private pending = new Map<string, Pending>();
  private seq = 0;

  async transcribe(wav: Buffer): Promise<Transcript> {
    const proc = this.ensure();
    const id = `u${++this.seq}-${Date.now()}`;
    const path = `/tmp/telepathy-utt-${id}.wav`;
    await writeFile(path, wav);
    try {
      return await new Promise<Transcript>((resolve, reject) => {
        this.pending.set(id, { resolve, reject });
        proc.stdin!.write(JSON.stringify({ id, path, prompt: vocabPrompt() }) + "\n");
        setTimeout(() => {
          if (this.pending.delete(id)) reject(new Error("stt timeout"));
        }, 60_000).unref();
      });
    } finally {
      void unlink(path).catch(() => {});
    }
  }

  private ensure(): ChildProcess {
    if (this.proc && this.proc.stdin!.writable) return this.proc;

    this.proc = spawn("python3", [new URL("../../scripts/whisper_worker.py", import.meta.url).pathname], {
      env: process.env,
      stdio: ["pipe", "pipe", "inherit"],
    });
    console.log("whisper worker starting…");

    let buffer = "";
    this.proc.stdout!.on("data", (d: Buffer) => {
      buffer += d.toString();
      let idx;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line) continue;
        let msg: any;
        try { msg = JSON.parse(line); } catch { continue; }
        if (msg.event === "ready") { console.log("whisper worker ready:", msg.model); continue; }
        const p = this.pending.get(msg.id);
        if (!p) continue;
        this.pending.delete(msg.id);
        if (msg.error) p.reject(new Error(msg.error));
        else p.resolve({ text: msg.text, confidence: msg.confidence });
      }
    });

    this.proc.on("exit", () => {
      console.warn("whisper worker exited");
      this.proc = undefined;
      for (const [, p] of this.pending) p.reject(new Error("whisper worker died"));
      this.pending.clear();
    });

    return this.proc;
  }
}

const localWorker_ = new LocalWhisper();
function localWorker() { return localWorker_; }
