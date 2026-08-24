import {
  Lane,
  LaneCapacityError,
  LaneRegistry,
  MAX_LANE_COUNT,
} from "./lanes.js";

/** JSON-compatible lane records have no nested mutable fields. */
function sameValue(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

/**
 * Merge non-overlapping changes to one pre-existing lane.  A concurrent
 * writer wins only for the individual fields it changed, so a model switch's
 * last-active touch cannot erase an independently updated interaction count.
 */
function mergeExistingLane(base: Lane, proposed: Lane, current: Lane): Lane {
  const merged: Lane = { ...current };
  const fields: (keyof Lane)[] = ["id", "name", "createdAt", "lastActive", "interactions"];
  for (const field of fields) {
    const proposedChanged = !sameValue(proposed[field], base[field]);
    const currentChanged = !sameValue(current[field], base[field]);
    if (proposedChanged && !currentChanged) {
      if (proposed[field] === undefined) delete merged[field];
      else merged[field] = proposed[field] as never;
    }
  }
  return merged;
}

function hasLane(registry: LaneRegistry, id: string): boolean {
  return registry.lanes.some((lane) => lane.id === id);
}

/**
 * Three-way merge a private meta-model proposal into the shared registry.
 *
 * `base` is the snapshot shown to the model, `proposed` is its tool-mutated
 * copy, and `current` is the registry after any overlapping API/WebSocket
 * work.  Current changes win on conflicts; independent proposed additions
 * and field edits are retained.  Selection uses a separate revision because
 * an A -> B -> A switch is indistinguishable from no change structurally.
 */
export function mergeMetaLaneProposal(
  base: LaneRegistry,
  proposed: LaneRegistry,
  current: LaneRegistry,
  baseSelectionRevision: number,
  currentSelectionRevision: number,
): LaneRegistry {
  const baseById = new Map(base.lanes.map((lane) => [lane.id, lane]));
  const proposedById = new Map(proposed.lanes.map((lane) => [lane.id, lane]));
  const currentById = new Map(current.lanes.map((lane) => [lane.id, lane]));
  const mergedLanes: Lane[] = [];

  // Retain the current ordering and every concurrent addition.  This makes a
  // stale proposal incapable of deleting a lane it never observed.
  for (const currentLane of current.lanes) {
    const baseLane = baseById.get(currentLane.id);
    const proposedLane = proposedById.get(currentLane.id);
    if (!baseLane || !proposedLane) {
      mergedLanes.push({ ...currentLane });
      continue;
    }
    mergedLanes.push(mergeExistingLane(baseLane, proposedLane, currentLane));
  }

  // A lane present only in the proposal was created by the model.  Add it
  // only when no concurrent writer claimed the same stable ID and capacity
  // still permits it.  The caller's transaction rolls back the whole merge on
  // capacity or durable-validation failure.
  for (const proposedLane of proposed.lanes) {
    if (baseById.has(proposedLane.id) || currentById.has(proposedLane.id)) continue;
    if (mergedLanes.length >= MAX_LANE_COUNT) throw new LaneCapacityError();
    mergedLanes.push({ ...proposedLane });
  }

  const proposalChangedSelection =
    base.activeId !== proposed.activeId || base.previousId !== proposed.previousId;
  const selectionWasChangedConcurrently = currentSelectionRevision !== baseSelectionRevision;
  const activeId = proposalChangedSelection && !selectionWasChangedConcurrently
    ? proposed.activeId
    : current.activeId;
  const previousId = proposalChangedSelection && !selectionWasChangedConcurrently
    ? proposed.previousId
    : current.previousId;

  const merged: LaneRegistry = { lanes: mergedLanes, activeId, previousId };
  // Let the registry's existing canonical validator provide the final durable
  // admission check.  These early checks keep an invalid stale selection from
  // being mistaken for an unrelated persistence failure.
  if (!hasLane(merged, activeId) || !hasLane(merged, previousId)) {
    throw new Error("meta lane proposal selected a lane that no longer exists");
  }
  return merged;
}

/** Preserve the shared registry and lane-array identities while applying a merge. */
export function replaceLaneRegistry(target: LaneRegistry, source: LaneRegistry): void {
  target.lanes.splice(0, target.lanes.length, ...source.lanes.map((lane) => ({ ...lane })));
  target.activeId = source.activeId;
  target.previousId = source.previousId;
}
