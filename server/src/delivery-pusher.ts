import { appendFileSync } from "node:fs";

/**
 * Live delivery poller: while a phone is connected to the bridge, poll
 * telepathosd's delivery queue and patch new items through to the phone as
 * spoken "incoming" announcements. When no phone is connected, stop polling —
 * items stay in telepathosd's durable inbox and are read at the next
 * double-pinch instead (the inbox path, not the live path).
 */

export interface DeliveryPollerDeps {
  hermesBaseUrl: () => string | null;
  clientCount: () => number;
  broadcast: (frame: unknown) => void;
}

interface Delivery {
  seq: number;
  chat_id: string;
  content: string;
}

export function startDeliveryPoller(deps: DeliveryPollerDeps): void {
  let cursor = 0;
  let laneNames = new Map<string, string>();
  let namesAge = Number.MAX_SAFE_INTEGER;

  setInterval(async () => {
    const base = deps.hermesBaseUrl()?.replace(/\/+$/, "");
    if (!base) return;
    if (deps.clientCount() === 0) return; // no phone listening → inbox path

    try {
      // lane-name map refreshes every ~30s; on-demand when an unknown id appears
      if (namesAge > 30_000) {
        const res = await fetch(`${base}/api/state`);
        if (res.ok) {
          const body = await res.json();
          laneNames = new Map(
            (body.lanes ?? []).map((l: any) => [l.id, l.name]),
          );
        }
        namesAge = 0;
      }

      const res = await fetch(`${base}/api/delivery?after=${cursor}&consume=true`);
      if (!res.ok) return;
      const body = await res.json();
      const deliveries: Delivery[] = body.deliveries ?? [];
      cursor = Math.max(cursor, Number(body.latest ?? cursor));

      for (const d of deliveries) {
        const lane = laneNames.get(d.chat_id) ?? d.chat_id;
        deps.broadcast({
          type: "incoming",
          lane,
          text: d.content,
        });
      }
    } catch {
      // unreachable telepathosd: silent — the durable queue holds everything
    }
  }, 2500);
}
