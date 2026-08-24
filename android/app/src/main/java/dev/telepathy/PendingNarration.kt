package dev.telepathy

/** Pure TTS chunking in UTF-16 code units, matching Android String.length. */
internal object PendingNarrationChunker {
    const val MAX_CHUNK_UTF16_UNITS = 180

    /**
     * Split text into bounded chunks without losing whitespace or splitting a
     * UTF-16 surrogate pair. Prefer a whitespace boundary when one fits.
     */
    fun chunk(text: String, maxUnits: Int = MAX_CHUNK_UTF16_UNITS): List<String> {
        require(maxUnits > 0) { "maxUnits must be positive" }
        if (text.isEmpty()) return emptyList()

        val chunks = mutableListOf<String>()
        var offset = 0
        while (offset < text.length) {
            var end = minOf(offset + maxUnits, text.length)
            if (end < text.length && text[end - 1].isHighSurrogate()) end -= 1
            require(end > offset) { "maxUnits must accommodate one UTF-16 code point" }

            if (end < text.length) {
                for (index in end - 1 downTo offset) {
                    if (text[index].isWhitespace()) {
                        end = index + 1
                        break
                    }
                }
            }

            chunks += text.substring(offset, end)
            offset = end
        }
        return chunks
    }
}
