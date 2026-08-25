/**
 * Energy-based voice activity detector as a PURE FOLD:
 *   (state, chunk, config) → (state, event)
 * No mutable class, no hidden state — property-testable by construction.
 *
 * Good enough for a quiet room / earbud mic; replace with Silero later.
 */

export interface VadState {
  readonly speaking: boolean;
  readonly speechMs: number;
  readonly silenceMs: number;
}

export const VAD_INITIAL: VadState = {
  speaking: false,
  speechMs: 0,
  silenceMs: 0,
};

export type VadEvent = "start" | "end" | null;

export interface VadConfig {
  /** RMS threshold for speech detection (16-bit PCM). */
  threshold: number;
  /** Silence duration (ms) that ends an utterance. */
  silenceMsToEnd: number;
  /** Minimum speech duration (ms) for a valid utterance. */
  minSpeechMs: number;
}

export function rms(pcm: Buffer): number {
  let sum = 0;
  const n = pcm.length >> 1;
  for (let i = 0; i < n; i++) {
    const s = pcm.readInt16LE(i * 2);
    sum += s * s;
  }
  return Math.sqrt(sum / Math.max(1, n));
}

/** One pure step: chunk duration derives from byte length (16 kHz PCM16). */
export function vadStep(
  state: VadState,
  pcm: Buffer,
  cfg: VadConfig,
): { state: VadState; event: VadEvent } {
  const durationMs = (pcm.length / 2 / 16000) * 1000;
  const loud = rms(pcm) > cfg.threshold;

  if (!state.speaking) {
    if (loud) {
      const speechMs = state.speechMs + durationMs;
      if (speechMs >= 80) {
        return {
          state: { speaking: true, speechMs, silenceMs: 0 },
          event: "start",
        };
      }
      return { state: { ...state, speechMs }, event: null };
    }
    return { state: VAD_INITIAL, event: null };
  }

  // speaking
  if (loud) {
    return { state: { ...state, speechMs: state.speechMs + durationMs }, event: null };
  }
  const silenceMs = state.silenceMs + durationMs;
  if (silenceMs >= cfg.silenceMsToEnd) {
    const valid = state.speechMs >= cfg.minSpeechMs;
    return { state: VAD_INITIAL, event: valid ? "end" : null };
  }
  return { state: { ...state, silenceMs }, event: null };
}

/**
 * Thin adapter kept for the bridge's per-client state slot. Delegates to the
 * pure fold; reset is just VAD_INITIAL.
 */
export class EnergyVad {
  private state: VadState = VAD_INITIAL;
  constructor(private readonly cfg: VadConfig) {}

  process(pcm: Buffer): "start" | "end" | null {
    const r = vadStep(this.state, pcm, this.cfg);
    this.state = r.state;
    return r.event;
  }

  reset() {
    this.state = VAD_INITIAL;
  }

  get isSpeaking() {
    return this.state.speaking;
  }
}
