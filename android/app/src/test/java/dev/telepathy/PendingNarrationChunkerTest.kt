package dev.telepathy

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PendingNarrationChunkerTest {
    private fun hasUnpairedSurrogate(text: String): Boolean {
        text.forEachIndexed { index, char ->
            if (char.isHighSurrogate() &&
                (index + 1 == text.length || !text[index + 1].isLowSurrogate())
            ) return true
            if (char.isLowSurrogate() &&
                (index == 0 || !text[index - 1].isHighSurrogate())
            ) return true
        }
        return false
    }

    @Test
    fun longEmojiWithoutSpacesNeverSplitsSurrogatePairs() {
        val text = "🦀".repeat(181)

        val chunks = PendingNarrationChunker.chunk(text)

        assertEquals(text, chunks.joinToString(""))
        assertTrue(chunks.all { it.length <= 180 })
        assertTrue(chunks.none(::hasUnpairedSurrogate))
    }

    @Test
    fun nonBmpPairStartingAtTheBoundaryMovesTogetherToTheNextChunk() {
        val text = "a".repeat(179) + "🧠" + "tail"

        val chunks = PendingNarrationChunker.chunk(text)

        assertEquals(listOf("a".repeat(179), "🧠tail"), chunks)
        assertEquals(text, chunks.joinToString(""))
        assertTrue(chunks.none(::hasUnpairedSurrogate))
    }

    @Test
    fun whitespaceBoundaryIsPreferredAndWhitespaceIsReconstructed() {
        val text = "a".repeat(179) + " " + "b"

        val chunks = PendingNarrationChunker.chunk(text)

        assertEquals(listOf("a".repeat(179) + " ", "b"), chunks)
        assertEquals(text, chunks.joinToString(""))
    }

    @Test
    fun exactBoundaryAndShortTextRemainUnchanged() {
        val text = "x".repeat(180)

        assertEquals(listOf(text), PendingNarrationChunker.chunk(text))
        assertEquals(listOf("hello"), PendingNarrationChunker.chunk("hello"))
    }

    @Test
    fun multipleWhitespaceKindsAndNonBmpTextReconstructExactly() {
        val text = ("word\t🧠 word\n").repeat(40)

        val chunks = PendingNarrationChunker.chunk(text)

        assertEquals(text, chunks.joinToString(""))
        assertTrue(chunks.all { it.length <= 180 })
        assertTrue(chunks.none(::hasUnpairedSurrogate))
    }
}
