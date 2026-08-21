/**
 * Interaction lifecycle as a typed state machine (docs/features.md, deferred item #3).
 *
 * This is the single source of truth for "what is the interaction doing",
 * replacing scattered boolean flags (busy/speaking). Constraints:
 *
 * - States carry data ONLY when that data is meaningful in that state
 *   (e.g. you cannot be Speaking without a sampleRate — it won't compile).
 * - Every state handles EVERY event explicitly. No `default:` branches.
 *   Adding an event kind breaks compilation in all five states until each
 *   one decides what it means.
 * - Invalid event/state combinations are identity transitions (logged by caller),
 *   because hardware reality delivers events we can't forbid.
 */

export type PhaseName =
  | "listening"
  | "capturing"
  | "processing"
  | "echoing"
  | "speaking";

export type InteractionState =
  | { phase: "listening" }                          // mic live, VAD armed
  | { phase: "capturing"; bytes: number }           // speech detected, buffering
  | { phase: "processing" }                         // STT + agent thinking
  | { phase: "echoing"; sampleRate: number }        // speaking STT confirmation (M5)
  | { phase: "speaking"; sampleRate: number };      // speaking agent reply

export type InteractionEvent =
  | { kind: "SPEECH_START"; prerollBytes: number }
  | { kind: "SPEECH_CHUNK"; bytes: number }
  | { kind: "UTTERANCE_END" }                       // VAD-detected silence
  | { kind: "FORCE_END" }                           // 60 s cap hit
  | { kind: "BEGIN_ECHO"; sampleRate: number }
  | { kind: "BEGIN_REPLY"; sampleRate: number }
  | { kind: "PLAYBACK_DONE" }
  | { kind: "CANCEL" };                             // double-tap stop / teardown

const LISTENING: InteractionState = { phase: "listening" };

export function transition(state: InteractionState, event: InteractionEvent): InteractionState {
  switch (state.phase) {
    case "listening":
      switch (event.kind) {
        case "SPEECH_START": return { phase: "capturing", bytes: event.prerollBytes };
        case "SPEECH_CHUNK": return state;
        case "UTTERANCE_END": return state;
        case "FORCE_END": return state;
        case "BEGIN_ECHO": return state;
        case "BEGIN_REPLY": return state;
        case "PLAYBACK_DONE": return state;
        case "CANCEL": return state;
      }
      break;

    case "capturing":
      switch (event.kind) {
        case "SPEECH_START": return state;
        case "SPEECH_CHUNK": return { phase: "capturing", bytes: state.bytes + event.bytes };
        case "UTTERANCE_END": return { phase: "processing" };
        case "FORCE_END": return { phase: "processing" };
        case "BEGIN_ECHO": return state;
        case "BEGIN_REPLY": return state;
        case "PLAYBACK_DONE": return state;
        case "CANCEL": return LISTENING;
      }
      break;

    case "processing":
      switch (event.kind) {
        case "SPEECH_START": return state;
        case "SPEECH_CHUNK": return state;
        case "UTTERANCE_END": return state;
        case "FORCE_END": return state;
        case "BEGIN_ECHO": return { phase: "echoing", sampleRate: event.sampleRate };
        case "BEGIN_REPLY": return { phase: "speaking", sampleRate: event.sampleRate };
        case "PLAYBACK_DONE": return state;
        case "CANCEL": return LISTENING;
      }
      break;

    case "echoing":
      switch (event.kind) {
        case "SPEECH_START": return state;
        case "SPEECH_CHUNK": return state;
        case "UTTERANCE_END": return state;
        case "FORCE_END": return state;
        case "BEGIN_ECHO": return state;
        case "BEGIN_REPLY": return { phase: "speaking", sampleRate: event.sampleRate };
        case "PLAYBACK_DONE": return { phase: "processing" }; // confirmation done, reply pending
        case "CANCEL": return LISTENING;
      }
      break;

    case "speaking":
      switch (event.kind) {
        case "SPEECH_START": return state;
        case "SPEECH_CHUNK": return state;
        case "UTTERANCE_END": return state;
        case "FORCE_END": return state;
        case "BEGIN_ECHO": return state;
        case "BEGIN_REPLY": return state;
        case "PLAYBACK_DONE": return LISTENING;
        case "CANCEL": return LISTENING;
      }
      break;
  }

  // Unreachable when the two unions above are exhaustive — kept for the type checker.
  return state;
}

/** Convenience predicate: is the microphone logically closed for business? */
export function micOpen(state: InteractionState): boolean {
  return state.phase === "listening";
}
