export interface ReplyAckOwnerBinding {
  installationId: string;
}

/**
 * In-memory owner high-water marks are only needed while an owner is active or
 * still has a durable reply-ack binding. The cache deliberately has no
 * secondary ordering/index structure: pruning scans the bounded durable
 * binding set and the existing active-owner map.
 */
export class ReplyAckOwnerHighWaterCache {
  private readonly seen = new Map<string, number>();

  constructor(private readonly processStartedAtMs: number) {}

  note(installationId: string, nowMs: number): void {
    this.seen.set(
      installationId,
      Math.max(this.seen.get(installationId) ?? 0, nowMs, this.processStartedAtMs),
    );
  }

  lastSeenAt(installationId: string): number | undefined {
    return this.seen.get(installationId);
  }

  prune(
    activeOwners: ReadonlyMap<string, number>,
    durableBindings: Iterable<ReplyAckOwnerBinding>,
  ): void {
    for (const installationId of this.seen.keys()) {
      if ((activeOwners.get(installationId) ?? 0) > 0) continue;
      let hasDurableBinding = false;
      for (const binding of durableBindings) {
        if (binding.installationId === installationId) {
          hasDurableBinding = true;
          break;
        }
      }
      if (!hasDurableBinding) this.seen.delete(installationId);
    }
  }

  size(): number {
    return this.seen.size;
  }
}
