/**
 * Inbound control-plane protocol as tagged unions (mirrors Protocol.kt).
 *
 * The rule: raw JSON only exists at the socket boundary. Everything inside the
 * server sees parsed, closed variants — and every consumer switches exhaustively,
 * so adding a variant is a compile error everywhere it matters.
 */

export type ClientCommandKind = "stop" | "repeat" | "cancel_capture";

export interface Hello {
  tag: "hello";
  device: string;
  token?: string;
}

export interface Command {
  tag: "command";
  kind: ClientCommandKind;
}

/** Client says: stop capturing, send what you have (tap during capturing). */
export interface UtteranceEnd {
  tag: "utterance_end";
}

/** Double-pinch: next utterance goes to the meta agent, not Hermes. */
export interface MetaMode {
  tag: "meta_mode";
}

export type ControlMsg = Hello | Command | UtteranceEnd | MetaMode;

/** Compile-time exhaustiveness guard: if a variant is unhandled, this fails to build. */
export function assertNever(x: never): never {
  throw new Error(`unhandled variant: ${JSON.stringify(x)}`);
}

const COMMAND_KINDS: readonly ClientCommandKind[] = ["stop", "repeat", "cancel_capture"];

/** Defensive parse: anything malformed or unknown becomes null. Total function. */
export function parseControl(raw: string): ControlMsg | null {
  let msg: any;
  try {
    msg = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof msg !== "object" || msg === null) return null;

  switch (msg.type) {
    case "hello":
      return typeof msg.device === "string"
        ? { tag: "hello", device: msg.device, token: msg.token }
        : null;
    case "command":
      return COMMAND_KINDS.includes(msg.command)
        ? { tag: "command", kind: msg.command }
        : null;
    case "utterance_end":
      return { tag: "utterance_end" };
    case "meta_mode":
      return { tag: "meta_mode" };
    default:
      return null;
  }
}
