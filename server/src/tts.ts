import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFile, unlink } from "node:fs/promises";
import { config } from "./config.js";

const run = promisify(execFile);

/**
 * TTS → raw PCM16 mono. Backends (TELEPATHY_TTS):
 *  - "say"   : macOS dev (this Mac)
 *  - "piper" : Linux deploy target (3090 box) — needs `piper` in PATH and
 *              TELEPATHY_PIPER_MODEL pointing at a .onnx voice
 *  - "none"  : skip synthesis entirely
 * All return the same shape so callers never care which ran.
 */
export async function synthesize(text: string): Promise<{ pcm: Buffer; sampleRate: number } | null> {
  switch (config.tts) {
    case "none":
      return null;
    case "say":
      return viaSay(text);
    case "piper":
      return viaPiper(text);
    default:
      throw new Error(`unknown tts backend: ${config.tts}`);
  }
}

async function viaSay(text: string) {
  const out = `/tmp/telepathy-tts-${Date.now()}.wav`;
  try {
    // LEI16@22050 = 16-bit little-endian PCM at 22.05 kHz mono
    await run("say", ["--data-format=LEI16@22050", "-o", out, text], { timeout: 15000 });
    const wav = await readFile(out);
    return { pcm: wav.subarray(44), sampleRate: 22050 }; // strip canonical WAV header
  } finally {
    void unlink(out).catch(() => {}); // don't leak temp files
  }
}

async function viaPiper(text: string) {
  const model = process.env.TELEPATHY_PIPER_MODEL;
  if (!model) throw new Error("piper selected but TELEPATHY_PIPER_MODEL is not set");
  const out = `/tmp/telepathy-tts-${Date.now()}.wav`;
  try {
    await run("bash", ["-c", `piper --model '${model}' --output_file '${out}' <<< ${JSON.stringify(text)}`],
      { timeout: 30000 });
    const wav = await readFile(out);
    // piper writes 22050 Hz PCM16 mono WAV (header size varies by writer; find data chunk)
    return { pcm: stripWavHeader(wav), sampleRate: 22050 };
  } finally {
    void unlink(out).catch(() => {});
  }
}

/** Locate the data chunk properly instead of assuming a fixed header size. */
function stripWavHeader(wav: Buffer): Buffer {
  if (wav.length < 44 || wav.toString("ascii", 0, 4) !== "RIFF") {
    throw new Error("tts output is not a WAV file");
  }
  let off = 12;
  while (off + 8 <= wav.length) {
    const id = wav.toString("ascii", off, off + 4);
    const size = wav.readUInt32LE(off + 4);
    if (id === "data") return wav.subarray(off + 8, off + 8 + size);
    off += 8 + size + (size % 2);
  }
  throw new Error("WAV data chunk not found");
}
