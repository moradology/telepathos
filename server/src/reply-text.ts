/**
 * The complete reply limit shared by every server-side reply path.
 * This is a UTF-8 byte limit, not a JavaScript UTF-16 character limit.
 */
export const MAX_REPLY_TEXT_BYTES = 512 * 1024;

export class ReplyTextLimitError extends Error {}

/** Node encodes an unpaired UTF-16 surrogate as the three-byte replacement character. */
export function utf8ByteLength(text: string): number {
  let bytes = 0;
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    if (code <= 0x7f) bytes += 1;
    else if (code <= 0x7ff) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length) {
      const next = text.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 3;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) bytes += 3;
    else bytes += 3;
  }
  return bytes;
}

/** Incremental UTF-8 accounting remains exact when a chunk splits an emoji. */
export class ReplyTextByteAccumulator {
  private bytes = 0;
  private lastCharCode: number | null = null;

  append(text: string): boolean {
    const nextBytes = utf8ByteLength(text);
    const joinsSurrogate = this.lastCharCode !== null &&
      this.lastCharCode >= 0xd800 && this.lastCharCode <= 0xdbff &&
      text.length > 0 && text.charCodeAt(0) >= 0xdc00 && text.charCodeAt(0) <= 0xdfff;
    const added = nextBytes - (joinsSurrogate ? 2 : 0);
    if (this.bytes + added > MAX_REPLY_TEXT_BYTES) return false;
    this.bytes += added;
    this.lastCharCode = text.length === 0 ? this.lastCharCode : text.charCodeAt(text.length - 1);
    return true;
  }

  byteLength(): number {
    return this.bytes;
  }
}

export function isReplyTextWithinLimit(text: string): boolean {
  return utf8ByteLength(text) <= MAX_REPLY_TEXT_BYTES;
}
