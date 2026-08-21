import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

/**
 * Conversation lanes: the registry of parallel conversations the user can
 * switch between. Persisted to lanes.json next to the process cwd.
 *
 * A lane maps 1:1 to a Hermes session lane via chat_id (relay contract):
 *   "telepathy:direct"        → talking to Hermes himself, no project
 *   "telepathy:repo:<name>"   → per-project conversation
 */

export interface Lane {
  id: string;          // stable id, doubles as hermes chat_id
  name: string;        // spoken/display name ("kerchunk")
  createdAt: string;
  lastActive: string;
  interactions?: number; // lifetime voice-interaction count (for stats tools)
}

export interface LaneRegistry {
  lanes: Lane[];
  activeId: string;
  previousId: string;  // for "switch back"
}

const DEFAULT_REGISTRY: LaneRegistry = {
  lanes: [
    {
      id: "telepathy:direct",
      name: "direct",
      createdAt: new Date().toISOString(),
      lastActive: new Date().toISOString(),
    },
  ],
  activeId: "telepathy:direct",
  previousId: "telepathy:direct",
};

function registryPath(): string {
  return process.env.TELEPATHY_LANES ?? "lanes.json";
}

export function loadLanes(): LaneRegistry {
  try {
    const raw = JSON.parse(readFileSync(registryPath(), "utf8"));
    if (Array.isArray(raw.lanes) && raw.lanes.length > 0) return raw as LaneRegistry;
  } catch {
    /* first run */
  }
  return structuredClone(DEFAULT_REGISTRY);
}

export function saveLanes(reg: LaneRegistry): void {
  mkdirSync(dirname(registryPath()), { recursive: true });
  writeFileSync(registryPath(), JSON.stringify(reg, null, 2));
}

export function activeLane(reg: LaneRegistry): Lane {
  return reg.lanes.find((l) => l.id === reg.activeId) ?? reg.lanes[0];
}

export function touchLane(reg: LaneRegistry, id: string): void {
  const lane = reg.lanes.find((l) => l.id === id);
  if (lane) lane.lastActive = new Date().toISOString();
}

export function createLane(reg: LaneRegistry, name: string): Lane {
  const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "lane";
  const id = `telepathy:repo:${slug}`;
  const existing = reg.lanes.find((l) => l.id === id);
  if (existing) return existing;
  const lane: Lane = {
    id,
    name: slug,
    createdAt: new Date().toISOString(),
    lastActive: new Date().toISOString(),
  };
  reg.lanes.push(lane);
  return lane;
}

export function switchLane(reg: LaneRegistry, id: string): Lane {
  const lane = reg.lanes.find((l) => l.id === id);
  if (!lane) throw new Error(`unknown lane ${id}`);
  if (reg.activeId !== id) {
    reg.previousId = reg.activeId;
    reg.activeId = id;
  }
  touchLane(reg, id);
  return lane;
}
