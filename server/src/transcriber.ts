import { config } from "./config.js";

/**
 * STT for a complete utterance (WAV bytes). Backends:
 *  - "openai": OpenAI whisper-1 API (needs OPENAI_API_KEY) — fine for dev on Mac
 *  - "echo":   dev stub, no transcription (server replies with a canned line)
 */
export async function transcribe(wav: Buffer): Promise<string | null> {
  if (config.stt === "echo") return null;
  if (config.stt === "openai") {
    const form = new FormData();
    form.append("file", new Blob([new Uint8Array(wav)]), "utterance.wav");
    form.append("model", "whisper-1");
    const res = await fetch("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: form,
    });
    if (!res.ok) throw new Error(`stt failed: ${res.status} ${await res.text()}`);
    const json = (await res.json()) as { text: string };
    return json.text;
  }
  throw new Error(`unknown stt backend: ${config.stt}`);
}
