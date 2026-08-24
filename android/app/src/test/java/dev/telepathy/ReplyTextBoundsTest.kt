package dev.telepathy

import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ReplyTextBoundsTest {
    @Test
    fun acceptedEmptyDeltaRequiresAnEqualTerminalText() {
        val contract = ReplyDeltaTracker()
        val accumulator = ReplyTextAccumulator()
        assertTrue(contract.accept("", accumulator))

        assertTrue(contract.terminalTextMatches(accumulator.text(), ""))
        assertFalse(contract.terminalTextMatches(accumulator.text(), "reply"))
    }

    @Test
    fun noDeltaAllowsTerminalReplayAndResetBoundariesClearTheContract() {
        val contract = ReplyDeltaTracker()

        assertTrue(contract.terminalTextMatches("", "replayed reply"))
        assertTrue(contract.accept("", ReplyTextAccumulator()))
        assertFalse(contract.terminalTextMatches("", "replayed reply"))

        contract.reset()
        assertTrue(contract.terminalTextMatches("", "replayed reply"))
        assertTrue(contract.accept("partial", ReplyTextAccumulator()))
        contract.reset()
        assertTrue(contract.terminalTextMatches("partial", "replayed reply"))
    }

    @Test
    fun acceptsExactUtf8BoundaryAndRejectsOneByteOver() {
        assertTrue(isReplyTextWithinLimit("a".repeat(MAX_REPLY_TEXT_BYTES)))
        assertFalse(isReplyTextWithinLimit("a".repeat(MAX_REPLY_TEXT_BYTES + 1)))
        assertTrue(isReplyTextWithinLimit("🦀".repeat(MAX_REPLY_TEXT_BYTES / 4)))
        assertFalse(isReplyTextWithinLimit("🦀".repeat(MAX_REPLY_TEXT_BYTES / 4) + "x"))
    }

    @Test
    fun chunkedMultibyteDeltasUseExactUtf8BytesAndDoNotRetainOverflow() {
        val accumulator = ReplyTextAccumulator()
        assertTrue(accumulator.append("\ud83e"))
        assertTrue(accumulator.append("\udd80"))
        assertEquals(4, accumulator.byteLength())

        val boundary = ReplyTextAccumulator()
        assertTrue(boundary.append("a".repeat(MAX_REPLY_TEXT_BYTES - 1)))
        assertFalse(boundary.append("é"))
        assertEquals(MAX_REPLY_TEXT_BYTES - 1, boundary.byteLength())
    }

    @Test
    fun oversizedAgentEndAndDurableReplayTextAreRejectedBeforePlayback() {
        val oversized = "x".repeat(MAX_REPLY_TEXT_BYTES + 1)
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "agent_end")
            .put("text", oversized)
            .put("turn_token", "turn-oversized")
            .put("interaction_id", "i-oversized")
            .toString()))
        assertFalse(isReplyTextWithinLimit(oversized))

        val ack = JSONObject()
            .put("lane_id", "telepathy:direct")
            .put("reply_to", "tp-oversized-replay")
            .put("after_seq", 1)
            .put("through_seq", 2)
            .put("turn_token", "turn-replay")
            .put("interaction_id", "i-replay")
            .put("reply_text", oversized)
            .put("state", "awaiting_playback")
        val snapshot = JSONObject()
            .put("version", ReplyAckSnapshot.VERSION)
            .put("installation_id", "android-test-owner")
            .put("acks", JSONArray().put(ack))
            .toString()
        var rejected = false
        try {
            ReplyAckSnapshot.decode(snapshot, "android-test-owner", 64)
        } catch (_: IllegalArgumentException) {
            rejected = true
        }
        assertTrue(rejected)
    }
}
