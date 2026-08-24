import { createHash } from "node:crypto";

/** The durable target identity format is intentionally independent of secrets. */
export const TARGET_IDENTITY_VERSION = 1;

export interface TelepathydTargetScope {
  baseUrl: string | null;
  targetIdentity: string;
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

/** Canonicalize the exact base URL used by the bridge before hashing it. */
export function normalizeTelepathydBaseUrl(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) throw new Error("TELEPATHY_HERMES_URL must not be empty");
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error("TELEPATHY_HERMES_URL is not a valid URL");
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("TELEPATHY_HERMES_URL must use http:// or https://");
  }
  if (parsed.username || parsed.password) {
    throw new Error("TELEPATHY_HERMES_URL must not contain username or password");
  }
  // Query and fragment components make `${base}/api/...` ambiguous.
  if (parsed.search || parsed.hash) {
    throw new Error("TELEPATHY_HERMES_URL must not contain a query or fragment");
  }
  return parsed.href.replace(/\/+$/, "");
}

/** Hash the whole target configuration; plaintext credentials never persist. */
export function targetIdentityFor(baseUrl: string | null, token?: string): string {
  const normalized = baseUrl === null ? null : normalizeTelepathydBaseUrl(baseUrl);
  const auth = token
    ? { kind: "telepathy-token", tokenSha256: sha256(token) }
    : { kind: "none" };
  return `v${TARGET_IDENTITY_VERSION}-sha256:${sha256(JSON.stringify({
    baseUrl: normalized,
    auth,
  }))}`;
}

export function isTargetIdentity(value: unknown): value is string {
  return typeof value === "string" &&
    new RegExp(`^v${TARGET_IDENTITY_VERSION}-sha256:[0-9a-f]{64}$`).test(value);
}

export function currentTelepathydTargetScope(): TelepathydTargetScope {
  const raw = process.env.TELEPATHY_HERMES_URL;
  const baseUrl = raw && raw.trim() ? normalizeTelepathydBaseUrl(raw) : null;
  return {
    baseUrl,
    targetIdentity: targetIdentityFor(baseUrl, process.env.TELEPATHY_TOKEN || undefined),
  };
}

export function currentTelepathydTargetIdentity(): string {
  return currentTelepathydTargetScope().targetIdentity;
}
