import {
  LaneNameError,
  LaneRegistry,
  activeLane,
  createLane,
  laneNameValidationError,
  switchLane,
} from "./lanes.js";
import { matchLane } from "./meta.js";
import { boundedProviderText, fetchProviderJson } from "./provider-response.js";

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
  searchConversations?: (query: string) => string;
}

export interface SearchConversationsArgs {
  query: string;
}

interface MetaToolCall {
  id: string;
  function: {
    name: string;
    arguments?: string;
  };
}

interface MetaProviderMessage {
  content?: string | null;
  tool_calls?: MetaToolCall[];
}

interface MetaProviderResponse {
  choices: Array<{ message: MetaProviderMessage }>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Admit only the OpenAI-compatible fields this tool loop actually consumes. */
function parseMetaProviderResponse(value: unknown): MetaProviderResponse | null {
  if (!isRecord(value) || !Array.isArray(value.choices) || value.choices.length === 0) return null;
  const choices: MetaProviderResponse["choices"] = [];
  for (const choice of value.choices) {
    if (!isRecord(choice) || !isRecord(choice.message)) return null;
    const message = choice.message;
    const content = message.content;
    if (content !== undefined && content !== null && boundedProviderText(content) === null) return null;
    const rawCalls = message.tool_calls;
    let toolCalls: MetaToolCall[] | undefined;
    if (rawCalls !== undefined) {
      if (!Array.isArray(rawCalls)) return null;
      toolCalls = [];
      for (const call of rawCalls) {
        if (!isRecord(call) || typeof call.id !== "string" || !isRecord(call.function) ||
            typeof call.function.name !== "string" ||
            (call.function.arguments !== undefined && typeof call.function.arguments !== "string")) {
          return null;
        }
        toolCalls.push({
          id: call.id,
          function: {
            name: call.function.name,
            ...(call.function.arguments !== undefined && { arguments: call.function.arguments }),
          },
        });
      }
    }
    choices.push({
      message: {
        ...(content !== undefined && { content: content as string | null }),
        ...(toolCalls !== undefined && { tool_calls: toolCalls }),
      },
    });
  }
  return { choices };
}

export const META_SYSTEM = `You are the steering agent for Telepathy, a voice interface to coding agents.
Your ONLY job is managing conversation lanes: listing, switching, creating, searching, reporting activity and statistics.
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
    {
      type: "function",
      function: {
        name: "search_conversations",
        description: "Search past conversations for a topic. Returns which lanes mention it, not the content.",
        parameters: {
          type: "object",
          properties: { query: { type: "string", description: "Topic to search for" } },
          required: ["query"],
        },
      },
    },
  ];
}

/** Execute one tool call against the shared registry. Returns text for the LLM. */
export function executeTool(
  reg: LaneRegistry,
  name: string,
  args: any,
  searchConversations?: (query: string) => string,
): string {
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
      const name = String(args.name ?? "");
      const invalid = laneNameValidationError(name);
      if (invalid) return invalid.message;
      try {
        const lane = createLane(reg, name);
        switchLane(reg, lane.id);
        return `Created and switched to ${lane.name}.`;
      } catch (error) {
        if (error instanceof LaneNameError) return error.message;
        throw error;
      }
    }
    case "lane_stats": {
      return reg.lanes
        .map((l) => `${l.name}: ${l.interactions ?? 0} interactions, last active ${l.lastActive}`)
        .join("\n");
    }
    case "search_conversations": {
      const query = (args as Partial<SearchConversationsArgs> | null)?.query;
      if (typeof query !== "string") return "Argument 'query' is required.";
      if (!searchConversations || query.length === 0) return "Search is not available right now.";
      return searchConversations(query);
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
  signal?: AbortSignal,
): Promise<string> {
  const messages: any[] = [
    { role: "system", content: META_SYSTEM },
    { role: "user", content: utterance },
  ];

  for (let round = 0; round < 4; round++) {
    signal?.throwIfAborted();
    const json = await fetchProviderJson(`${cfg.baseUrl}/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${cfg.apiKey}` },
      body: JSON.stringify({ model: cfg.model, messages, tools: metaTools() }),
      signal,
    }, parseMetaProviderResponse);
    const msg = json.choices?.[0]?.message;
    if (!msg) throw new Error("meta agent: empty response");

    const calls = msg.tool_calls;
    if (!calls?.length) return msg.content ?? "Done.";

    messages.push(msg);
    for (const call of calls) {
      let args: any = {};
      try { args = JSON.parse(call.function.arguments || "{}"); } catch { /* keep {} */ }
      const result = executeTool(reg, call.function.name, args, cfg.searchConversations);
      saveAndEcho(reg);
      messages.push({ role: "tool", tool_call_id: call.id, content: result });
    }
  }
  return "I went in circles — try a simpler command.";
}

function saveAndEcho(_reg: LaneRegistry) {
  // persistence handled by caller (index.ts) after the turn; hook kept for symmetry
}
