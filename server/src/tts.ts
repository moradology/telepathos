import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFile, unlink } from "node:fs/promises";
import { config } from "./config.js";

const run = promisify(execFile);

/**
 * TTS → raw PCM16 mono. macOS `say` for dev; Piper on the 3090 later
 * (same return shape, different backend).
 */
export async function synthesize(text: string): Promise<{ pcm: Buffer; sampleRate: number } | null> {
  if (config.tts === "none") return null;
  if (config.tts !== "say") throw new Error(`unknown tts backend: ${config.tts}`);

  const out = `/tmp/telepathy-tts-${Date.now()}.wav`;
  try {
    // LEI16@22050 = 16-bit little-endian PCM at 22.05 kHz mono
    await run("say", ["--data-format=LEI16@22050", "-o", out, text], { timeout: 15000 });
    const wav = await readFile(out);
    // strip the 44-byte canonical WAV header written by `say`
    return { pcm: wav.subarray(44), sampleRate: 22050 };
  } finally {
    void unlink(out).catch(() => {}); // don't leak temp files
  }
}
