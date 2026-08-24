import { MAX_REPLY_TEXT_BYTES, isReplyTextWithinLimit } from "./reply-text.js";

/**
 * Provider JSON is never allowed to be larger than a complete spoken reply
 * plus its protocol envelope.  This protects both OpenAI STT and the local
 * steering model from a peer that lies about Content-Length or streams
 * forever.
 */
export const PROVIDER_RESPONSE_MAX_BYTES = MAX_REPLY_TEXT_BYTES + 64 * 1024;

export type ProviderResponseFailure =
  | "http-error"
  | "too-large"
  | "invalid-utf8"
  | "invalid-json"
  | "invalid-schema"
  | "transport";

/** A body-free error which is safe to forward to the handset. */
export class ProviderResponseError extends Error {
  constructor(public readonly failure: ProviderResponseFailure) {
    super("provider unavailable");
    this.name = "ProviderResponseError";
  }
}

export type ProviderJsonValidator<T> = (value: unknown) => T | null;

function contentLengthExceeds(response: Response, maxBytes: number): boolean {
  const header = response.headers.get("content-length");
  if (header === null || !/^[0-9]+$/.test(header)) return false;
  const normalized = header.replace(/^0+/, "") || "0";
  const limit = String(maxBytes);
  return normalized.length > limit.length ||
    (normalized.length === limit.length && normalized > limit);
}

async function cancelBody(response: Response): Promise<void> {
  await response.body?.cancel().catch(() => undefined);
}

/**
 * Read, decode, parse, and validate one provider response without trusting
 * Content-Length or chunk boundaries.  Error bodies are deliberately never
 * decoded: a provider must not be able to place credentials in a spoken
 * error, and cancellation releases an endless response promptly.
 */
export async function readProviderJson<T>(
  response: Response,
  validate: ProviderJsonValidator<T>,
  maxBytes = PROVIDER_RESPONSE_MAX_BYTES,
): Promise<T> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new TypeError("provider response byte limit must be a non-negative safe integer");
  }
  if (contentLengthExceeds(response, maxBytes)) {
    void cancelBody(response);
    throw new ProviderResponseError("too-large");
  }
  if (!response.ok) {
    void cancelBody(response);
    throw new ProviderResponseError("http-error");
  }
  if (response.body === null) throw new ProviderResponseError("invalid-json");

  const reader = response.body.getReader();
  const chunks: Buffer[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = Buffer.from(value);
      if (chunk.length > maxBytes - total) {
        await reader.cancel().catch(() => undefined);
        throw new ProviderResponseError("too-large");
      }
      total += chunk.length;
      chunks.push(chunk);
    }
  } catch (error) {
    if (error instanceof ProviderResponseError) throw error;
    throw new ProviderResponseError("transport");
  } finally {
    reader.releaseLock();
  }

  let parsed: unknown;
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, total));
    parsed = JSON.parse(text) as unknown;
  } catch (error) {
    if (error instanceof TypeError) throw new ProviderResponseError("invalid-utf8");
    throw new ProviderResponseError("invalid-json");
  }

  try {
    const value = validate(parsed);
    if (value === null) throw new ProviderResponseError("invalid-schema");
    return value;
  } catch (error) {
    if (error instanceof ProviderResponseError) throw error;
    throw new ProviderResponseError("invalid-schema");
  }
}

/** Preserve caller cancellation, while sanitizing all other fetch failures. */
export async function fetchProviderJson<T>(
  input: RequestInfo | URL,
  init: RequestInit,
  validate: ProviderJsonValidator<T>,
  maxBytes = PROVIDER_RESPONSE_MAX_BYTES,
): Promise<T> {
  try {
    const response = await fetch(input, init);
    const value = await readProviderJson(response, validate, maxBytes);
    init.signal?.throwIfAborted();
    return value;
  } catch (error) {
    if (init.signal?.aborted) {
      init.signal.throwIfAborted();
    }
    if (error instanceof ProviderResponseError) throw error;
    throw new ProviderResponseError("transport");
  }
}

/** Enforce the same exact UTF-8 bound used for every spoken reply. */
export function boundedProviderText(value: unknown): string | null {
  return typeof value === "string" && isReplyTextWithinLimit(value) ? value : null;
}
