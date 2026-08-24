export const config = {
  port: Number(process.env.TELEPATHY_PORT ?? 8787),
  // Remote binds require an explicit token + TLS configuration. Keep the
  // default loopback-only so a fresh install cannot expose an unauthenticated
  // cleartext microphone endpoint on the LAN.
  host: process.env.TELEPATHY_HOST ?? "127.0.0.1",
  /** RMS threshold for speech detection (16-bit PCM). Tune per mic. */
  vadThreshold: Number(process.env.TELEPATHY_VAD_THRESHOLD ?? 600),
  /** Silence duration (ms) that ends an utterance. */
  vadSilenceMs: Number(process.env.TELEPATHY_VAD_SILENCE_MS ?? 1500),
  /** Minimum speech duration (ms) for a valid utterance. */
  vadMinSpeechMs: Number(process.env.TELEPATHY_VAD_MIN_SPEECH_MS ?? 500),
  /** "openai" (whisper-1 API) | "local" (faster-whisper worker) | "echo" (dev stub). */
  stt: process.env.TELEPATHY_STT ?? "echo",
  /** Agent-facing lane API (Hermes tools). localhost: only the same box needs it. */
  apiPort: Number(process.env.TELEPATHY_API_PORT ?? 8788),
  apiHost: process.env.TELEPATHY_API_HOST ?? "127.0.0.1",
  /** Steering agent (catches what the meta grammar misses). Unset = disabled,
   *  grammar-miss falls back to the spoken help text. */
  metaModel: process.env.TELEPATHY_META_MODEL,
  metaBaseUrl: process.env.TELEPATHY_META_BASE_URL ?? "https://api.openai.com/v1",
  // NOTE: no TTS config — the phone speaks replies via its own TTS engine.
};
