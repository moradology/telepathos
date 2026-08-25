package dev.telepathos

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertNotNull
import org.junit.Test

class PendingItemsTest {
    private fun record(
        sequence: Any?,
        content: Any? = "pending reply",
        replyTo: Any? = null,
    ) = PendingItemRecord(sequence = sequence, content = content, replyTo = replyTo)

    @Test
    fun exactSafeSequenceBoundIsAcceptedForPendingDelivery() {
        val parsed = PendingItemsParser.parse(listOf(record(MAX_SAFE_SEQUENCE)))

        assertNotNull(parsed)
        assertEquals(
            listOf(PendingItem(MAX_SAFE_SEQUENCE, "pending reply", null)),
            parsed!!.items,
        )
    }

    @Test
    fun oneOverSafeSequenceBoundRejectsTheEntirePendingBatch() {
        val parsed = PendingItemsParser.parse(listOf(record(MAX_SAFE_SEQUENCE + 1)))

        assertNull(parsed)
    }

    @Test
    fun malformedSequenceRejectsTheWholeBatchBeforeItsContentCanBeUsed() {
        val valid = record(7L, "must not be narrated")
        val malformed = listOf<Any?>(null, 1.5, MAX_SAFE_SEQUENCE + 1, 0L, -1L)

        malformed.forEach { sequence ->
            assertNull(
                "sequence $sequence must reject the complete pending batch",
                PendingItemsParser.parse(listOf(valid, record(sequence, "malformed"))),
            )
        }
    }

    @Test
    fun absentNonStringAndBlankContentRejectTheWholePendingBatch() {
        val valid = record(7L, "must not be narrated")
        val malformed = listOf<Any?>(null, 42L, "", " \t\n")

        malformed.forEach { content ->
            assertNull(
                "content $content must reject the complete pending batch",
                PendingItemsParser.parse(
                    listOf(valid, PendingItemRecord(sequence = 8L, content = content, replyTo = null)),
                ),
            )
        }
    }

    @Test
    fun validPendingContentIsPreservedInOrder() {
        val parsed = PendingItemsParser.parse(
            listOf(
                record(7L, "first"),
                record(9L, "second"),
            ),
        )

        assertNotNull(parsed)
        assertEquals(
            listOf(
                PendingItem(7L, "first", null),
                PendingItem(9L, "second", null),
            ),
            parsed!!.items,
        )
    }

    @Test
    fun replyToIsPreservedAndMalformedCorrelationRejectsTheWholeBatch() {
        val parsed = PendingItemsParser.parse(
            listOf(record(7L, "correlated", "tp-7")),
        )

        assertEquals(
            listOf(PendingItem(7L, "correlated", "tp-7")),
            parsed!!.items,
        )
        listOf<Any?>("", " \t\n", 7L).forEach { malformedReplyTo ->
            assertNull(PendingItemsParser.parse(listOf(record(8L, replyTo = malformedReplyTo))))
        }
    }

    @Test
    fun duplicateOrOutOfOrderSequencesRejectTheWholeExactConsumeBatch() {
        assertNull(PendingItemsParser.parse(listOf(record(7L), record(7L))))
        assertNull(PendingItemsParser.parse(listOf(record(8L), record(7L))))
    }
}
