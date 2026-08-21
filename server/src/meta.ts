/**
 * The meta agent's brain: a PURE function from transcript + registry to a
 * meta-action. No I/O, no LLM, fully property-testable.
 *
 * Entry is via double-pinch (meta mode) or the "meta"/"telepathy" voice
 * codeword — the caller strips the codeword and hands us the remainder.
 */

import { Lane, LaneRegistry } from "./lanes.js";

export type MetaAction =
  | { op: "switch"; lane: Lane }
  | { op: "back"; lane: Lane }
  | { op: "list" }
  | { op: "new"; name: string }
  | { op: "brief"; lane: Lane | null }
  | { op: "unknown" };

function normalize(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9 ]+/g, " ").replace(/\s+/g, " ").trim();
}

function levenshtein(a: string, b: string): number {
  const dp = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const tmp = dp[j];
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + (a[i - 1] === b[j - 1] ? 0 : 1));
      prev = tmp;
    }
  }
  return dp[b.length];
}

/** Fuzzy lane lookup: exact → substring → bounded edit distance.
 *  Space-stripped comparison handles STT mangling of multi-word names
 *  ("kirk chunk" → "kerchunk"). */
function matchLane(text: string, reg: LaneRegistry): Lane | null {
  const t = normalize(text);
  const tFlat = t.replace(/ /g, "");
  if (!t) return null;
  for (const lane of reg.lanes) {
    const n = normalize(lane.name);
    if (n === t || lane.id === t || n.replace(/ /g, "") === tFlat) return lane;
  }
  for (const lane of reg.lanes) {
    const n = normalize(lane.name);
    if (t.includes(n) || tFlat.includes(n.replace(/ /g, ""))) return lane;
  }
  let best: Lane | null = null;
  let bestDist = Infinity;
  for (const lane of reg.lanes) {
    const n = normalize(lane.name);
    const tolerance = Math.max(2, Math.floor(n.length / 3));
    const d = Math.min(levenshtein(t, n), levenshtein(tFlat, n.replace(/ /g, "")));
    if (d < bestDist && d <= tolerance) { bestDist = d; best = lane; }
  }
  return best;
}

function stripLead(text: string, patterns: string[]): string {
  for (const p of patterns) {
    const m = text.match(new RegExp(`^${p}\\b\\s*(.*)$`, "i"));
    if (m) return m[1];
  }
  return text;
}

export function parseMeta(rawTranscript: string, reg: LaneRegistry): MetaAction {
  const text = normalize(rawTranscript);
  if (!text) return { op: "unknown" };

  // switch back
  if (/^(switch|go) back$/.test(text) || /^back$/.test(text)) {
    const lane = reg.lanes.find((l) => l.id === reg.previousId) ?? reg.lanes[0];
    return { op: "back", lane };
  }

  // list
  if (/^(list|show|what are the|what conversations?|which conversations?)/.test(text) &&
      /(conversation|lane)/.test(text)) {
    return { op: "list" };
  }

  // new conversation [for|about|called] X — keyword required on both sides
  const isNew = text.match(
    /^(?:new|start|create)(?: a new)? (?:conversation|lane|chat)(?: for| about| called| named| to work on)? (.+)$/);
  if (isNew) {
    return { op: "new", name: isNew[1].trim() };
  }

  // brief [me] [on X]
  const isBrief = text.match(/^(brief|catch me up|status)( me)?( (on|about|for) (.*))?$/);
  if (isBrief) {
    const target = isBrief[5];
    return { op: "brief", lane: target ? matchLane(target, reg) : null };
  }

  // switch/go/work on X  — lane match REQUIRED, otherwise fall through to unknown
  const isSwitch = text.match(/^(switch|go|work|move|jump)( to| on| to the| over to)?\s*(.*)$/);
  if (isSwitch && isSwitch[3]) {
    const lane = matchLane(isSwitch[3], reg);
    if (lane) return { op: "switch", lane };
  }

  // bare lane name → switch (double-pinch then just say the name)
  const bare = matchLane(text, reg);
  if (bare) return { op: "switch", lane: bare };

  return { op: "unknown" };
}
