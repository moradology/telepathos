import { activeLane } from "./lanes.js";

/**
 * Hermes delivery client: sends lane utterances to telepathyd (which pushes
 * them to the Hermes gateway over the relay), then polls for the agent's
 * replies until they arrive or we time out.
 *
 * Backpressure notes:
 * - Poll interval is 1 s: Hermes thinks for seconds; polling faster is waste.
 * - The cursor only advances on successful fetch; a failed poll retries the
 *   same window (telepathyd keeps unconsumed entries until picked up).
 */

export interface HermesConfig {
  baseUrl: string;
  timeoutMs: number;
}

export function hermesConfig(): HermesConfig | null {
  const baseUrl = process.env.TELEPATHY_HERMES_URL?.replace(/\/+$/, "");
  if (!baseUrl) return null;
  return {
    baseUrl,
    timeoutMs: Number(process.env.TELEPATHY_HERMES_TIMEOUT ?? 120_000),
  };
}

interface Delivery {
  seq: number;
  chat_id: string;
  content: string;
}

/** Send an utterance for a lane; resolve with the concatenated agent reply. */
export async function deliverAndWait(
  cfg: HermesConfig,
  lanesRegistry: () => { id: string },
  text: string,
): Promise<string> {
  const lane = lanesRegistry();
  const res = await fetch(`${cfg.baseUrl}/api/message`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lane_id: lane.id, text }),
  });
  if (!res.ok) {
    throw new Error(`hermes rejected utterance: ${res.status} ${await res.text()}`);
  }

  // poll for replies addressed to this lane
  let cursor = await latestSeq(cfg);
  const deadline = Date.now() + cfg.timeoutMs;
  const parts: string[] = [];

  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 1000));
    const batch = await fetchDeliveries(cfg, cursor);
    for (const d of batch.deliveries) {
      if (d.chat_id === lane.id && d.content.trim()) parts.push(d.content.trim());
    }
    cursor = Math.max(cursor, batch.latest);
    if (parts.length > 0) return parts.join(" ");
    // quiet polling: keep waiting — async agents think in minutes sometimes
  }
  throw new Error(`no reply from hermes within ${cfg.timeoutMs / 1000}s`);
}

async function latestSeq(cfg: HermesConfig): Promise<number> {
  const r = await fetch(`${cfg.baseUrl}/api/delivery?after=0`);
  if (!r.ok) return 0;
  const j = await r.json();
  return Number(j.latest ?? 0);
}

async function fetchDeliveries(cfg: HermesConfig, after: number): Promise<{ deliveries: Delivery[]; latest: number }> {
  const r = await fetch(`${cfg.baseUrl}/api/delivery?after=${after}&consume=true`);
  if (!r.ok) return { deliveries: [], latest: after };
  return r.json();
}

/** Convenience wrapper used by index.ts: returns null when hermes is not configured. */
export async function respondViaHermes(text: string): Promise<string | null> {
  const cfg = hermesConfig();
  if (!cfg) return null;
  try {
    return await deliverAndWait(cfg, () => ({ id: currentLaneId() }), text);
  } catch (e) {
    const msg = (e as Error).message;
    // timeout ≠ failure: Hermes keeps working; the reply lands in the lane's
    // durable queue and gets announced at your next pinch
    if (msg.includes("no reply")) {
      return "Nothing yet. I'll read it to you when it lands.";
    }
    return `Hermes error: ${msg}`;
  }
}

// set by index.ts each turn — avoids threading the registry through every call
let currentLaneIdFn: () => string = () => "telepathy:direct";
export function setCurrentLaneIdFn(fn: () => string) { currentLaneIdFn = fn; }
function currentLaneId(): string { return currentLaneIdFn(); }
