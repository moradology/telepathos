package dev.telepathos

/** The complete reply limit is shared with Node: 512 KiB of UTF-8 bytes. */
const val MAX_REPLY_TEXT_BYTES = 512 * 1024

internal fun utf8ByteLength(text: String): Int {
    var bytes = 0
    var index = 0
    while (index < text.length) {
        val code = text[index].code
        when {
            code <= 0x7f -> bytes += 1
            code <= 0x7ff -> bytes += 2
            code in 0xd800..0xdbff && index + 1 < text.length &&
                text[index + 1].code in 0xdc00..0xdfff -> {
                bytes += 4
                index += 1
            }
            code in 0xd800..0xdfff -> bytes += 3
            else -> bytes += 3
        }
        index += 1
    }
    return bytes
}

internal fun isReplyTextWithinLimit(text: String): Boolean =
    utf8ByteLength(text) <= MAX_REPLY_TEXT_BYTES

/** Tracks whether a live reply stream had an accepted delta, including an empty one. */
internal class ReplyDeltaTracker {
    private var acceptedDelta = false

    fun reset() {
        acceptedDelta = false
    }

    fun accept(delta: String, accumulator: ReplyTextAccumulator): Boolean {
        if (!accumulator.append(delta)) return false
        acceptedDelta = true
        return true
    }

    fun terminalTextMatches(accumulatedText: String, terminalText: String): Boolean =
        !acceptedDelta || accumulatedText == terminalText
}

/** Bounded delta accumulator; it never appends a chunk that crosses the byte limit. */
internal class ReplyTextAccumulator {
    private val value = StringBuilder()
    private var bytes = 0
    private var lastChar: Char? = null

    fun append(delta: String): Boolean {
        val joinsSurrogate = lastChar?.isHighSurrogate() == true &&
            delta.firstOrNull()?.isLowSurrogate() == true
        val added = utf8ByteLength(delta) - if (joinsSurrogate) 2 else 0
        if (added < 0 || bytes > MAX_REPLY_TEXT_BYTES - added) return false
        value.append(delta)
        bytes += added
        if (delta.isNotEmpty()) lastChar = delta.last()
        return true
    }

    fun replace(text: String): Boolean {
        if (!isReplyTextWithinLimit(text)) return false
        value.setLength(0)
        value.append(text)
        bytes = utf8ByteLength(text)
        lastChar = text.lastOrNull()
        return true
    }

    fun clear() {
        value.setLength(0)
        bytes = 0
        lastChar = null
    }

    fun text(): String = value.toString()
    fun byteLength(): Int = bytes
}
