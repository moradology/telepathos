import http from "node:http";
import { LaneRegistry, activeLane, createLane, switchLane, saveLanes, touchLane } from "./lanes.js";

/**
 * Agent-facing control API — the tools Hermes calls to inspect and modify
 * bridge state. Bound to localhost by default: the agent runs on the same
 * box (or reaches it over the tailnet with TELEPATHY_API_HOST set).
 *
 *   GET  /api/state                 full registry + active lane
 *   POST /api/lanes/active {"id"}  switch active lane
 *   POST /api/lanes        {"name"} create lane (and switch to it)
 *   POST /api/lanes/touch  {"id"}   mark lane active-now (agent did work there)
 *
 * This is the surface to describe in Hermes tool definitions:
 *   list_lanes()          → GET /api/state
 *   switch_lane(id)       → POST /api/lanes/active
 *   create_lane(name)     → POST /api/lanes
 *   mark_lane_active(id)  → POST /api/lanes/touch
 */

export function startApiServer(reg: LaneRegistry, port: number, host: string): void {
  const server = http.createServer((req, res) => {
    const json = (code: number, body: unknown) => {
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(JSON.stringify(body));
    };

    const auth = process.env.TELEPATHY_TOKEN;
    if (auth && req.headers["x-telepathy-token"] !== auth) {
      return json(401, { error: "unauthorized" });
    }

    let body = "";
    req.on("data", (c) => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on("end", () => {
      try {
        const url = req.url ?? "/";
        if (req.method === "GET" && url === "/api/state") {
          touchLane(reg, reg.activeId);
          return json(200, { ...structuredClone(reg), active: activeLane(reg).name });
        }
        if (req.method === "POST" && url === "/api/lanes/active") {
          const { id } = JSON.parse(body);
          const lane = switchLane(reg, id);
          saveLanes(reg);
          return json(200, { ok: true, lane });
        }
        if (req.method === "POST" && url === "/api/lanes") {
          const { name } = JSON.parse(body);
          if (!name) return json(400, { error: "name required" });
          const lane = createLane(reg, name);
          switchLane(reg, lane.id);
          saveLanes(reg);
          return json(200, { ok: true, lane });
        }
        if (req.method === "POST" && url === "/api/lanes/touch") {
          const { id } = JSON.parse(body);
          touchLane(reg, id);
          saveLanes(reg);
          return json(200, { ok: true });
        }
        return json(404, { error: "not found" });
      } catch (e) {
        return json(400, { error: String((e as Error).message ?? e) });
      }
    });
  });

  server.listen(port, host, () => {
    console.log(`lane API on http://${host}:${port} (agent tools: list/switch/create/touch)`);
  });
  server.on("error", (e: NodeJS.ErrnoException) => {
    console.error(`lane API: ${e.message}`);
  });
}
