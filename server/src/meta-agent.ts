import { LaneRegistry, activeLane, createLane, switchLane } from "./lanes.js";
import { matchLane } from "./meta.js";

/**
 * The steering agent: a small tool-calling loop that catches everything the
 * deterministic meta grammar doesn't. It shares the lane registry with the
 * bridge but has its own mandate:
 *
 *   - steer lanes, report state and statistics
 *   - NEVER discuss project content — that belongs to the lane agents
 *   - output is spoken aloud: terse, no markdown, no lists longer than ~5
 */

export interface MetaAgentConfig {
  baseUrl: string;   // OpenAI-compatible endpoint (3090 vLLM works fine)
  apiKey: string;
  model: string;
}

export const META_SYSTEM = `You are the steering agent for Telepathy, a voice interface to coding agents.
Your ONLY job is managing conversation lanes: listing, switching, creating, reporting activity and statistics.
Rules:
- Your output is spoken aloud through earbuds. Be terse. No markdown, no code, no lists over five items.
- Never discuss project content, never answer coding questions — if asked, tell the user to switch to the right lane and ask there.
- Prefer calling tools over guessing. If the user's target lane is ambiguous, ask one short clarifying question.
- When you switch lanes, confirm with the lane name.`;

export function metaTools() {
  return [
    {
      type: "function",
      function: {
        name: "list_lanes",
        description: "List all conversation lanes with their names, ids, and last-active times.",
        parameters: { type: "object", properties: {} },
      },
    },
    {
      type: "function",
      function: {
        name: "active_lane",
        description: "Return the currently active lane.",
        parameters: { type: "object", properties: {} },
      },
    },
    {
      type: "function",
      function: {
        name: "switch_lane",
        description: "Make a lane the active conversation. Fuzzy-matches the name.",
        parameters: {
          type: "object",
          properties: { name: { type: "string", description: "Lane name or id" } },
          required: ["name"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "create_lane",
        description: "Create a new conversation lane and switch to it.",
        parameters: {
          type: "object",
          properties: { name: { type: "string", description: "Short spoken name" } },
          required: ["name"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "lane_stats",
        description: "Interaction counts and last-active times for all lanes.",
        parameters: { type: "object", properties: {} },
      },
    },
  ];
}

/** Execute one tool call against the shared registry. Returns text for the LLM. */
export function executeTool(reg: LaneRegistry, name: string, args: any): string {
  switch (name) {
    case "list_lanes": {
      const active = activeLane(reg);
      return reg.lanes
        .map((l) => `${l.name}${l.id === active.id ? " (ACTIVE)" : ""} — last active ${l.lastActive}`)
        .join("\n");
    }
    case "active_lane": {
      const l = activeLane(reg);
      return `${l.name} (${l.id})`;
    }
    case "switch_lane": {
      const lane = matchLane(String(args.name ?? ""), reg);
      if (!lane) return `No lane matching "${args.name}". Available: ${reg.lanes.map((l) => l.name).join(", ")}`;
      switchLane(reg, lane.id);
      return `Active lane is now ${lane.name}.`;
    }
    case "create_lane": {
      const lane = createLane(reg, String(args.name ?? ""));
      switchLane(reg, lane.id);
      return `Created and switched to ${lane.name}.`;
    }
    case "lane_stats": {
      return reg.lanes
        .map((l) => `${l.name}: ${l.interactions ?? 0} interactions, last active ${l.lastActive}`)
        .join("\n");
    }
    default:
      return `unknown tool ${name}`;
  }
}

/** One steering turn: up to 4 tool rounds, then the final spoken text. */
export async function runMetaAgent(
  cfg: MetaAgentConfig,
  reg: LaneRegistry,
  utterance: string,
): Promise<string> {
  const messages: any[] = [
    { role: "system", content: META_SYSTEM },
    { role: "user", content: utterance },
  ];

  for (let round = 0; round < 4; round++) {
    const res = await fetch(`${cfg.baseUrl}/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${cfg.apiKey}` },
      body: JSON.stringify({ model: cfg.model, messages, tools: metaTools() }),
    });
    if (!res.ok) throw new Error(`meta agent: ${res.status} ${await res.text()}`);
    const json = await res.json();
    const msg = json.choices?.[0]?.message;
    if (!msg) throw new Error("meta agent: empty response");

    const calls = msg.tool_calls;
    if (!calls?.length) return msg.content ?? "Done.";

    messages.push(msg);
    for (const call of calls) {
      let args: any = {};
      try { args = JSON.parse(call.function.arguments || "{}"); } catch { /* keep {} */ }
      const result = executeTool(reg, call.function.name, args);
      saveAndEcho(reg);
      messages.push({ role: "tool", tool_call_id: call.id, content: result });
    }
  }
  return "I went in circles — try a simpler command.";
}

function saveAndEcho(_reg: LaneRegistry) {
  // persistence handled by caller (index.ts) after the turn; hook kept for symmetry
}
