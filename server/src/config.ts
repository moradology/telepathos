export const config = {
  port: Number(process.env.TELEPATHY_PORT ?? 8787),
  /** RMS threshold for speech detection (16-bit PCM). Tune per mic. */
  vadThreshold: Number(process.env.TELEPATHY_VAD_THRESHOLD ?? 600),
  /** Silence duration (ms) that ends an utterance. */
  vadSilenceMs: Number(process.env.TELEPATHY_VAD_SILENCE_MS ?? 1500),
  /** Minimum speech duration (ms) for a valid utterance. */
  vadMinSpeechMs: Number(process.env.TELEPATHY_VAD_MIN_SPEECH_MS ?? 500),
  /** "openai" (whisper-1 API) | "local" (faster-whisper worker) | "echo" (dev stub). */
  stt: process.env.TELEPATHY_STT ?? "echo",
  // NOTE: no TTS config — the phone speaks replies via its own TTS engine.
};
