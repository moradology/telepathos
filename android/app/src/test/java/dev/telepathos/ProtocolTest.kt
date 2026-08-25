package dev.telepathos

import java.math.BigDecimal
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.fail
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.json.JSONObject

class ProtocolTest {
    @Test
    fun opaqueCorrelationIdsShareUtf8Utf16AndControlCharacterBounds() {
        assertTrue(isValidOpaqueId("id-1"))
        assertTrue(isValidOpaqueId("é".repeat(MAX_OPAQUE_ID_BYTES / 2)))
        assertTrue(isValidOpaqueId("🦀".repeat(MAX_OPAQUE_ID_BYTES / 4)))
        listOf(
            "",
            " \t\n",
            "id\u0000bad",
            "id\u0085bad",
            "é".repeat(MAX_OPAQUE_ID_BYTES / 2 + 1),
            "🦀".repeat(MAX_OPAQUE_ID_LENGTH / 2 + 1),
        ).forEach { assertFalse("unexpected valid opaque ID: $it", isValidOpaqueId(it)) }
    }

    @Test
    fun opaqueIdBlanknessMatchesTheExplicitProtocolCodePointSet() {
        val cases = listOf(
            "" to false,
            " " to false,
            "\t" to false,
            "\n" to false,
            "\u000b" to false,
            "\u000c" to false,
            "\r" to false,
            "\u0000" to false,
            "\u001f" to false,
            "\u007f" to false,
            "\u0085" to false,
            "\u009f" to false,
            "\u00a0" to false,
            "\u2007" to false,
            "\u202f" to false,
            "\ufeff" to false,
            "id" to true,
            " id " to true,
            "\u00a0id\u00a0" to true,
            "\u2007id\u2007" to true,
            "\u202fid\u202f" to true,
            "\ufeffid\ufeff" to true,
            "id\t" to false,
            "id\u0085" to false,
        )
        cases.forEach { (value, expected) ->
            assertEquals("unexpected opaque-ID validity for ${value.toCharArray().toList()}", expected, isValidOpaqueId(value))
        }
    }

    @Test
    fun turnTokenBlanknessUsesTheExplicitProtocolCodePointSetWithoutAddingControls() {
        val cases = listOf(
            "" to false,
            " " to false,
            "\t" to false,
            "\n" to false,
            "\u000b" to false,
            "\u000c" to false,
            "\r" to false,
            "\u0085" to false,
            "\u00a0" to false,
            "\u1680" to false,
            "\u2007" to false,
            "\u202f" to false,
            "\u3000" to false,
            "\ufeff" to false,
            // Turn tokens historically have no control-character rejection.
            "\u0000" to true,
            "\u001f" to true,
            "\u007f" to true,
            "\u009f" to true,
            "turn-1" to true,
            " turn-1 " to true,
            "\u00a0turn-1\u00a0" to true,
            "\u2007turn-1\u2007" to true,
            "\u202fturn-1\u202f" to true,
            "\ufeffturn-1\ufeff" to true,
            "turn-1\t" to true,
            "turn-1\u0085" to true,
        )
        cases.forEach { (value, expected) ->
            assertEquals("unexpected turn-token validity for ${value.toCharArray().toList()}", expected, isValidTurnToken(value))
        }
        val exact = "\u00a0turn-1\u00a0"
        assertEquals(exact, parseTurnToken(exact))
    }

    @Test
    fun turnTokensRejectLoneUtf16SurrogatesAndTheirJsonParserForms() {
        listOf("\uD800", "\uDC00").forEach { value ->
            assertFalse(isValidTurnToken(value))
            assertNull(parseTurnToken(value))
        }

        listOf("\\ud800", "\\udc00").forEach { escaped ->
            val raw = """{"type":"stt","text":"reply","turn_token":"$escaped","interaction_id":"interaction-1"}"""
            assertNull(ServerMsg.parse(raw))
        }
    }

    @Test
    fun receiptParserRejectsMalformedOpaqueIdsWithoutCreatingReceiptState() {
        listOf(
            "",
            " \t\n",
            "reply\u0000bad",
            "r".repeat(MAX_OPAQUE_ID_BYTES + 1),
        ).forEach { replyTo ->
            val frame = JSONObject()
                .put("type", "reply_received")
                .put("lane_id", "telepathos:direct")
                .put("reply_to", replyTo)
                .put("after_seq", 0)
                .put("through_seq", 1)
                .put("turn_token", "turn-1")
                .put("interaction_id", "interaction-1")
            assertNull(ServerMsg.parse(frame.toString()))
        }
    }

    @Test
    fun laneIdsUseTheSharedAsciiContractAndRejectJsonMetacharacters() {
        assertTrue(isValidLaneId("telepathos:direct"))
        assertTrue(isValidLaneId("telepathos:repo:geospatial-migration"))
        listOf(
            "",
            " ",
            "telepathos:repo:quote\"",
            "telepathos:repo:backslash\\",
            "telepathos:repo:control\n",
            "telepathos:repo:é",
            "telepathos:repo:" + "a".repeat(MAX_LANE_ID_LENGTH),
        ).forEach { assertFalse("unexpected valid lane id: $it", isValidLaneId(it)) }
    }

    @Test
    fun laneSwitchSerializerUsesJSONObjectAndDoesNotMutateOnInvalidIds() {
        val valid: String = checkNotNull(LaneStore.laneSwitchRequestJson("telepathos:repo:slug"))
        assertEquals("telepathos:repo:slug", JSONObject(valid).getString("id"))
        assertEquals("{\"id\":\"telepathos:repo:slug\"}", valid)
        assertEquals(null, LaneStore.laneSwitchRequestJson("telepathos:repo:quote\"altered"))
        assertEquals(null, LaneStore.laneSwitchRequestJson("telepathos:repo:backslash\\altered"))
    }

    @Test
    fun serverLaneReceiptParserRejectsInvalidLaneIdsBeforeCreatingState() {
        val invalidIds = listOf(
            "telepathos:repo:quote\"",
            "telepathos:repo:backslash\\",
            "telepathos:repo:control\u0000",
            "telepathos:repo:é",
            "telepathos:repo:" + "a".repeat(MAX_LANE_ID_LENGTH),
        )
        invalidIds.forEach { laneId ->
            val frame = JSONObject()
                .put("type", "reply_received")
                .put("lane_id", laneId)
                .put("reply_to", "tp-1")
                .put("after_seq", 0)
                .put("through_seq", 1)
                .put("turn_token", "turn-1")
                .put("interaction_id", "i-1")
            assertNull(ServerMsg.parse(frame.toString()))
        }
    }

    @Test
    fun textBearingFramesRequireJsonStringsAndKeepAbsentOptionalRepoAbsent() {
        val common = JSONObject()
            .put("turn_token", "turn-1")
            .put("interaction_id", "interaction-1")

        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("type", "agent_delta")
            .put("text", 7)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("type", "stt")
            .put("text", JSONObject.NULL)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("type", "stt")
            .put("text", "heard")
            .put("repo", 7)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("type", 7)
            .put("text", "heard")
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "error")
            .put("message", 7)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "phase")
            .put("value", JSONObject.NULL)
            .toString()))

        val stt = ServerMsg.parse(JSONObject(common.toString())
            .put("type", "stt")
            .put("text", "heard")
            .toString()) as ServerMsg.Stt
        assertEquals("heard", stt.text)
        assertNull(stt.repo)
    }

    @Test
    fun inboundControlFrameLimitCountsUtf8BytesAtAMultibyteBoundary() {
        val atLimit = controlFrameAtUtf8ByteSize(MAX_INBOUND_CONTROL_FRAME_BYTES)
        assertEquals(MAX_INBOUND_CONTROL_FRAME_BYTES, utf8ByteLength(atLimit))
        assertTrue(isInboundControlFrameWithinLimit(atLimit))

        val oneByteOver = controlFrameAtUtf8ByteSize(MAX_INBOUND_CONTROL_FRAME_BYTES + 1)
        assertEquals(MAX_INBOUND_CONTROL_FRAME_BYTES + 1, utf8ByteLength(oneByteOver))
        assertFalse(isInboundControlFrameWithinLimit(oneByteOver))

        // A lone surrogate cannot be represented as a well-formed UTF-8 frame.
        assertFalse(isInboundControlFrameWithinLimit("{\"type\":\"ready\",\"padding\":\"\uD800\"}"))
    }

    @Test
    fun escapedControlTextAtTheDecodedReplyLimitFitsTheTransportLimit() {
        val exactRaw = escapedAgentEndFrame(MAX_REPLY_TEXT_BYTES)
        assertEquals(MAX_REPLY_TEXT_BYTES * 6L + 64 * 1024L, MAX_INBOUND_CONTROL_FRAME_BYTES.toLong())
        assertTrue(utf8ByteLength(exactRaw).toLong() <= MAX_INBOUND_CONTROL_FRAME_BYTES.toLong())
        assertTrue(isInboundControlFrameWithinLimit(exactRaw))

        val accepted = ServerMsg.parse(exactRaw) as ServerMsg.AgentEnd
        assertEquals(MAX_REPLY_TEXT_BYTES, accepted.text.length)
        assertEquals(MAX_REPLY_TEXT_BYTES, utf8ByteLength(accepted.text))
        assertEquals('\u0000', accepted.text.first())
    }

    @Test
    fun oneEscapedControlByteOverTheDecodedReplyLimitIsRejected() {
        val oneOverRaw = escapedAgentEndFrame(MAX_REPLY_TEXT_BYTES + 1)
        // The extra escape is still below the transport cap; this assertion
        // proves the parser rejects the decoded field bound, not the envelope.
        assertTrue(utf8ByteLength(oneOverRaw).toLong() <= MAX_INBOUND_CONTROL_FRAME_BYTES.toLong())
        assertTrue(isInboundControlFrameWithinLimit(oneOverRaw))
        assertNull(ServerMsg.parse(oneOverRaw))
    }

    @Test
    fun parseRejectsOversizedInboundTextFieldsAndFullFrames() {
        // Keep one bounded payload for all field cases; each serialized frame
        // is built and parsed immediately so the test does not retain a list of
        // multi-hundred-kilobyte copies.
        val oversized = "x".repeat(MAX_REPLY_TEXT_BYTES + 1)

        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "stt")
            .put("text", oversized)
            .put("turn_token", "turn-stt")
            .put("interaction_id", "interaction-stt")
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "agent_delta")
            .put("text", oversized)
            .put("turn_token", "turn-delta")
            .put("interaction_id", "interaction-delta")
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "agent_end")
            .put("text", oversized)
            .put("turn_token", "turn-end")
            .put("interaction_id", "interaction-end")
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "error")
            .put("message", oversized)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "phase")
            .put("value", oversized)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject()
            .put("type", "stt")
            .put("text", "heard")
            .put("repo", oversized)
            .put("turn_token", "turn-repo")
            .put("interaction_id", "interaction-repo")
            .toString()))

        assertNull(ServerMsg.parse(controlFrameAtUtf8ByteSize(MAX_INBOUND_CONTROL_FRAME_BYTES + 1)))
    }

    @Test
    fun parseRejectsOversizedTurnInteractionReplyAndLaneFields() {
        assertNull(ServerMsg.parse(replyReceivedFrame(
            turnToken = "t".repeat(MAX_TURN_TOKEN_LENGTH + 1),
        )))
        assertNull(ServerMsg.parse(replyReceivedFrame(
            interactionId = "i".repeat(MAX_OPAQUE_ID_BYTES + 1),
        )))
        assertNull(ServerMsg.parse(replyReceivedFrame(
            replyTo = "r".repeat(MAX_OPAQUE_ID_BYTES + 1),
        )))
        assertNull(ServerMsg.parse(replyReceivedFrame(
            laneId = "a".repeat(MAX_LANE_ID_LENGTH + 1),
        )))
    }

    @Test
    fun emptyTextIsStillAValidRequiredString() {
        val frame = ServerMsg.parse(JSONObject()
            .put("type", "agent_delta")
            .put("text", "")
            .put("turn_token", "turn-1")
            .put("interaction_id", "interaction-1")
            .toString()) as ServerMsg.AgentDelta

        assertEquals("", frame.text)
    }

    @Test
    fun confidenceRequiresAFiniteJsonNumberInsideItsProbabilityRange() {
        val common = JSONObject()
            .put("type", "stt")
            .put("text", "heard")
            .put("turn_token", "turn-1")
            .put("interaction_id", "interaction-1")

        val valid = ServerMsg.parse(JSONObject(common.toString())
            .put("confidence", 0.75)
            .toString()) as ServerMsg.Stt
        assertEquals(0.75, valid.confidence!!, 0.0)

        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("confidence", "0.75")
            .toString()))
        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("confidence", JSONObject.NULL)
            .toString()))
        assertNull(parseConfidence(Double.NaN))
        assertNull(parseConfidence(Double.POSITIVE_INFINITY))
        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("confidence", -0.01)
            .toString()))
        assertNull(ServerMsg.parse(JSONObject(common.toString())
            .put("confidence", 1.01)
            .toString()))
    }

    @Test
    fun terminalReplyRetirementPreservesTheExactReceiptIdentity() {
        val command = ClientCommand.ReplyAckRetire(
            laneId = "telepathos:direct",
            replyTo = "tp-1",
            afterSeq = 4,
            throughSeq = 6,
            turnToken = "turn-1",
            interactionId = "i-1",
        )

        val terminal = ServerMsg.ReplyAckRetired(
            ServerMsg.DeliveryReceipt(
                laneId = command.laneId,
                replyTo = command.replyTo,
                afterSeq = command.afterSeq,
                throughSeq = command.throughSeq,
                turnToken = command.turnToken,
                interactionId = command.interactionId,
            )
        )

        assertEquals("telepathos:direct", command.laneId)
        assertEquals("tp-1", command.replyTo)
        assertEquals(4L, command.afterSeq)
        assertEquals(6L, command.throughSeq)
        assertEquals("turn-1", command.turnToken)
        assertEquals("i-1", command.interactionId)
        assertEquals(command.laneId, terminal.receipt.laneId)
        assertEquals(command.replyTo, terminal.receipt.replyTo)
        assertEquals(command.afterSeq, terminal.receipt.afterSeq)
        assertEquals(command.throughSeq, terminal.receipt.throughSeq)
        assertEquals(command.turnToken, terminal.receipt.turnToken)
        assertEquals(command.interactionId, terminal.receipt.interactionId)
    }

    @Test
    fun v4CarriesReplayTextAndDurableReceiptProofIdentity() {
        val receipt = ServerMsg.DeliveryReceipt(
            laneId = "telepathos:direct",
            replyTo = "tp-1",
            afterSeq = 4,
            throughSeq = 6,
            turnToken = "turn-1",
            interactionId = "i-1",
        )
        val replay = ServerMsg.AgentEnd(
            text = "full recovered reply",
            turnToken = receipt.turnToken,
            interactionId = receipt.interactionId,
            receipt = receipt,
        )
        assertEquals("full recovered reply", replay.text)
        assertEquals("tp-1", replay.receipt?.replyTo)

        val received = ClientCommand.ReplyReceived(
            laneId = "telepathos:direct",
            replyTo = "tp-1",
            afterSeq = 4,
            throughSeq = 6,
            turnToken = "turn-1",
            interactionId = "i-1",
        )
        assertEquals(receipt.replyTo, received.replyTo)
        assertEquals(receipt.throughSeq, received.throughSeq)
    }

    @Test
    fun serverTurnTokenParserAcceptsTheUtf16LimitAndRejectsOneOver() {
        val tokenAtLimit = "t".repeat(MAX_TURN_TOKEN_LENGTH)
        val oversizedToken = "t".repeat(MAX_TURN_TOKEN_LENGTH + 1)
        assertEquals(tokenAtLimit, parseTurnToken(tokenAtLimit))
        assertNull(parseTurnToken(oversizedToken))
        assertNull(parseTurnToken(" \t\n"))
        assertNull(parseTurnToken(7))

        assertTrue(isValidTurnToken("🦀".repeat(MAX_TURN_TOKEN_LENGTH / 2)))
        assertTrue(!isValidTurnToken("🦀".repeat(MAX_TURN_TOKEN_LENGTH / 2 + 1)))
    }

    @Test
    fun serverReceiptSequenceParserAcceptsTheSafeLimitAndRejectsOneOver() {
        assertEquals(MAX_SAFE_SEQUENCE, parseSafeSequence(MAX_SAFE_SEQUENCE))
        assertEquals(MAX_SAFE_SEQUENCE - 1, parseSafeSequence(MAX_SAFE_SEQUENCE - 1))
        assertNull(parseSafeSequence(MAX_SAFE_SEQUENCE + 1))
        assertNull(parseSafeSequence(-1L))
        assertNull(parseSafeSequence(1.0))
        assertNull(parseSafeSequence(1.5))
        assertNull(parseSafeSequence(Double.NaN))
        assertNull(parseSafeSequence(Double.POSITIVE_INFINITY))
        assertNull(parseSafeSequence(JSONObject.NULL))
        assertNull(parseSafeSequence("1"))
        assertNull(parseSafeSequence(BigDecimal("1.0")))
    }

    @Test
    fun laneSnapshotAcceptsOnlyJsonSafeRevisions() {
        val atLimit = ClientCommand.LaneSnapshot(
            id = "telepathos:direct",
            turnToken = "turn-1",
            revision = MAX_SAFE_SEQUENCE,
        )
        assertEquals(MAX_SAFE_SEQUENCE, atLimit.revision)

        try {
            ClientCommand.LaneSnapshot(
                id = "telepathos:direct",
                turnToken = "turn-1",
                revision = MAX_SAFE_SEQUENCE + 1,
            )
            fail("one-over lane revision must be rejected")
        } catch (_: IllegalArgumentException) {
            // Expected: a received unsafe revision must never reach the wire.
        }
    }

    private fun controlFrameAtUtf8ByteSize(targetBytes: Int): String {
        val prefix = "{\"type\":\"ready\",\"padding\":\""
        val marker = "🦀"
        val suffix = "\"}"
        val paddingBytes = targetBytes - utf8ByteLength(prefix) - utf8ByteLength(marker) - utf8ByteLength(suffix)
        require(paddingBytes >= 0)
        return buildString(prefix.length + paddingBytes + marker.length + suffix.length) {
            append(prefix)
            repeat(paddingBytes) { append('a') }
            append(marker)
            append(suffix)
        }
    }

    private fun escapedAgentEndFrame(decodedTextLength: Int): String {
        val prefix = "{\"type\":\"agent_end\",\"text\":\""
        val escape = "\\u0000"
        val suffix = "\",\"turn_token\":\"turn-escaped\",\"interaction_id\":\"interaction-escaped\"}"
        return buildString(prefix.length + decodedTextLength * escape.length + suffix.length) {
            append(prefix)
            repeat(decodedTextLength) { append(escape) }
            append(suffix)
        }
    }

    private fun replyReceivedFrame(
        laneId: String = "telepathos:direct",
        replyTo: String = "reply-1",
        turnToken: String = "turn-1",
        interactionId: String = "interaction-1",
    ): String = JSONObject()
        .put("type", "reply_received")
        .put("lane_id", laneId)
        .put("reply_to", replyTo)
        .put("after_seq", 0)
        .put("through_seq", 1)
        .put("turn_token", turnToken)
        .put("interaction_id", interactionId)
        .toString()
}
