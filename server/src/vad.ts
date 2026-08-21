/**
 * Energy-based voice activity detector for 16 kHz PCM16 mono chunks.
 * Good enough for a quiet room / earbud mic; replace with Silero later.
 */
export class EnergyVad {
  private speaking = false;
  private speechMs = 0;
  private silenceMs = 0;

  constructor(
    private threshold: number,
    private silenceMsToEnd: number,
    private minSpeechMs: number,
  ) {}

  /** Feed one chunk; returns "start" | "end" | null. Chunk duration = samples/16000 s. */
  process(pcm: Buffer): "start" | "end" | null {
    const durationMs = (pcm.length / 2 / 16000) * 1000;
    const loud = rms(pcm) > this.threshold;

    if (!this.speaking) {
      if (loud) {
        this.speechMs += durationMs;
        if (this.speechMs >= 80) {
          this.speaking = true;
          this.silenceMs = 0;
          return "start";
        }
      } else {
        this.speechMs = 0;
      }
      return null;
    }

    // speaking
    if (loud) {
      this.speechMs += durationMs;
      this.silenceMs = 0;
    } else {
      this.silenceMs += durationMs;
      if (this.silenceMs >= this.silenceMsToEnd) {
        const valid = this.speechMs >= this.minSpeechMs;
        this.reset();
        return valid ? "end" : null;
      }
    }
    return null;
  }

  reset() {
    this.speaking = false;
    this.speechMs = 0;
    this.silenceMs = 0;
  }

  get isSpeaking() {
    return this.speaking;
  }
}

function rms(pcm: Buffer): number {
  let sum = 0;
  const n = pcm.length >> 1;
  for (let i = 0; i < n; i++) {
    const s = pcm.readInt16LE(i * 2);
    sum += s * s;
  }
  return Math.sqrt(sum / Math.max(1, n));
}
