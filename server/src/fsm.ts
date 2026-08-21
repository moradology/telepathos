/**
 * Interaction lifecycle as a typed state machine.
 *
 * The server's job ends at TEXT: it transcribes, thinks, and streams words.
 * Speaking those words is the phone's business (on-device TTS), so there are
 * no audio phases here anymore.
 *
 * Constraints:
 * - States carry data ONLY when meaningful in that state.
 * - Every state handles EVERY event explicitly. No defaults.
 * - Invalid combos are identity transitions (hardware delivers what it delivers).
 */

export type PhaseName =
  | "listening"
  | "capturing"
  | "processing";

export type InteractionState =
  | { phase: "listening" }                        // mic live, VAD armed
  | { phase: "capturing"; bytes: number }         // speech detected, buffering
  | { phase: "processing" };                      // STT + agent working, text incoming

export type InteractionEvent =
  | { kind: "SPEECH_START"; prerollBytes: number }
  | { kind: "SPEECH_CHUNK"; bytes: number }
  | { kind: "UTTERANCE_END" }                     // VAD silence OR explicit client flush
  | { kind: "FORCE_END" }                         // 60 s cap hit
  | { kind: "CANCEL" };                           // double-tap / teardown / done

const LISTENING: InteractionState = { phase: "listening" };

export function transition(state: InteractionState, event: InteractionEvent): InteractionState {
  switch (state.phase) {
    case "listening":
      switch (event.kind) {
        case "SPEECH_START": return { phase: "capturing", bytes: event.prerollBytes };
        case "SPEECH_CHUNK": return state;
        case "UTTERANCE_END": return state;
        case "FORCE_END": return state;
        case "CANCEL": return state;
      }
      break;

    case "capturing":
      switch (event.kind) {
        case "SPEECH_START": return state;
        case "SPEECH_CHUNK": return { phase: "capturing", bytes: state.bytes + event.bytes };
        case "UTTERANCE_END": return { phase: "processing" };
        case "FORCE_END": return { phase: "processing" };
        case "CANCEL": return LISTENING;
      }
      break;

    case "processing":
      switch (event.kind) {
        case "SPEECH_START": return state;
        case "SPEECH_CHUNK": return state;
        case "UTTERANCE_END": return state;
        case "FORCE_END": return state;
        case "CANCEL": return LISTENING;
      }
      break;
  }

  // Unreachable when both unions are exhaustive — kept for the type checker.
  return state;
}

/** Convenience predicate: is the microphone logically open for business? */
export function micOpen(state: InteractionState): boolean {
  return state.phase === "listening";
}
