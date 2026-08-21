export const config = {
  port: Number(process.env.TELEPATHY_PORT ?? 8787),
  /** RMS threshold for speech detection (16-bit PCM). Tune per mic. */
  vadThreshold: Number(process.env.TELEPATHY_VAD_THRESHOLD ?? 600),
  /** Silence duration (ms) that ends an utterance. */
  vadSilenceMs: Number(process.env.TELEPATHY_VAD_SILENCE_MS ?? 1500),
  /** Minimum speech duration (ms) for a valid utterance. */
  vadMinSpeechMs: Number(process.env.TELEPATHY_VAD_MIN_SPEECH_MS ?? 500),
  /** "openai" (uses OPENAI_API_KEY + whisper-1) or "echo" (dev: no STT). */
  stt: process.env.TELEPATHY_STT ?? "echo",
  /** "say" (macOS) or "none" (skip TTS). Piper on the 3090 later. */
  tts: process.env.TELEPATHY_TTS ?? "say",
  /** Speak "Working on: …" confirmation before agent work (see docs/features.md M5). */
  echo: process.env.TELEPATHY_ECHO ?? "on",
};
