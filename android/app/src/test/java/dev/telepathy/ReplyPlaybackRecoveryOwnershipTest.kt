package dev.telepathy

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ReplyPlaybackRecoveryOwnershipTest {
    private fun ack() = ClientCommand.ReplyAck(
        laneId = "telepathy:direct",
        replyTo = "delivery-1",
        afterSeq = 1,
        throughSeq = 2,
        turnToken = "turn-1",
        interactionId = "interaction-1",
    )

    @Test
    fun staleCompletionCannotReleaseOrDuplicateAfterReconnectRecoveryAttempt() {
        val receipt = ack()
        val inFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()

        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 1))

        // Disconnect invalidates attempt 1; reconnect admits a new attempt for
        // the same durable receipt before the old TTS callback arrives.
        inFlight.clear()
        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 2))

        assertFalse(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 1))
        assertEquals(2L, inFlight[receipt])
        assertTrue(
            ReplyAckDurability.awaitingPlaybackRecovery(
                entries = listOf(DurableReplyAck(receipt, "saved reply", ReplyAckPlaybackState.AwaitingPlayback)),
                inFlight = inFlight.keys,
            ).isEmpty(),
        )

        assertTrue(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 2))
        assertTrue(inFlight.isEmpty())
    }

    @Test
    fun activeDirectPlaybackLeaseBlocksRecoveryUntilItsOwnCallbackReleasesIt() {
        val receipt = ack()
        val inFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()
        val awaiting = DurableReplyAck(receipt, "saved reply", ReplyAckPlaybackState.AwaitingPlayback)

        // beginConfirmedReplyPlayback acquires the same lease before starting
        // direct TTS. A periodic retry must therefore find no recovery work.
        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 11))
        assertTrue(
            ReplyAckDurability.awaitingPlaybackRecovery(
                entries = listOf(awaiting),
                inFlight = inFlight.keys,
            ).isEmpty(),
        )
        assertFalse(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 12))

        // Only the callback holding the direct attempt's token can release it.
        assertFalse(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 12))
        assertEquals(11L, inFlight[receipt])
        assertTrue(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 11))
        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 12))
    }

    @Test
    fun staleCallbackCannotPromoteAReceiptSuppressedByStop() {
        val receipt = ack()
        val awaiting = DurableReplyAck(receipt, "saved reply", ReplyAckPlaybackState.AwaitingPlayback)
        val inFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()

        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 21))
        val stopped = ReplyAckDurability.suppressPlayback(listOf(awaiting), setOf(receipt))
        inFlight.clear()

        // The old TTS callback has no lease, and the durable Stop state cannot
        // be promoted even if a callback races with a later recovery attempt.
        assertFalse(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 21))
        assertNull(ReplyAckDurability.markPlaybackHeard(stopped, receipt))
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(stopped).isEmpty())
        assertEquals(ReplyAckPlaybackState.PlaybackSuppressed, stopped.single().state)
    }

    @Test
    fun startingCaptureWhileReceiptTtsSpeaksSuppressesItAndUnblocksTheMic() {
        val receipt = ack()
        val awaiting = DurableReplyAck(receipt, "saved reply", ReplyAckPlaybackState.AwaitingPlayback)
        val inFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()

        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 41))

        // StartCapture replaces the old turn before calling announcer.stop.
        // Its active lease must become durable suppression rather than an
        // AwaitingPlayback receipt with no possible callback.
        val toSuppress = ReplyAckDurability.activeAcksForSupersession(
            entries = listOf(awaiting),
            supersededTurnToken = receipt.turnToken,
            playbackLeases = inFlight.keys,
        )
        val superseded = ReplyAckDurability.suppressPlayback(listOf(awaiting), toSuppress)
        inFlight.clear()

        assertFalse(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 41))
        assertEquals(ReplyAckPlaybackState.PlaybackSuppressed, superseded.single().state)
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(superseded).isEmpty())
        assertTrue(
            ReplyAckCaptureGate.allowsMicAfterPendingFetch(
                entries = superseded,
                maxEntries = 64,
                stateCorrupt = false,
                persistenceFailed = false,
            ),
        )

        // Ready/reconnect is the only recovery boundary for the interrupted
        // reply; it restores the exact stored envelope for a later TTS attempt.
        val rearmed = ReplyAckDurability.resumeSuppressedPlayback(superseded)
        assertEquals(listOf(rearmed.single()), ReplyAckDurability.awaitingPlaybackRecovery(rearmed))
    }

    @Test
    fun repeatPreemptionReleasesTheOldReceiptLeaseWithoutAuthorizingIt() {
        val receipt = ack()
        val awaiting = DurableReplyAck(receipt, "saved reply", ReplyAckPlaybackState.AwaitingPlayback)
        val inFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()

        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 51))

        // Repeat creates a new local turn, so the prior TTS callback is
        // discarded. Its durable receipt stays silent and the old lease must
        // be removed immediately instead of waiting for that callback.
        val superseded = ReplyAckDurability.suppressPlayback(
            entries = listOf(awaiting),
            activeAcks = ReplyAckDurability.activeAcksForSupersession(
                entries = listOf(awaiting),
                supersededTurnToken = receipt.turnToken,
                playbackLeases = inFlight.keys,
            ),
        )
        inFlight.clear()

        assertFalse(ReplyPlaybackOwnership.finish(inFlight, receipt, attemptId = 51))
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(superseded).isEmpty())
        assertNull(ReplyAckDurability.markPlaybackHeard(superseded, receipt))

        val rearmed = ReplyAckDurability.resumeSuppressedPlayback(superseded)
        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 52))
        assertTrue(
            ReplyAckDurability.awaitingPlaybackRecovery(
                entries = rearmed,
                inFlight = inFlight.keys,
            ).isEmpty(),
        )
    }

    @Test
    fun stopGenerationAndLeaseFenceRejectsAStaleTtsEnqueue() {
        val receipt = ack()
        val inFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()

        assertTrue(ReplyPlaybackOwnership.tryStart(inFlight, receipt, attemptId = 31))
        assertTrue(
            ReplyPlaybackStartGuard.canEnqueue(
                inFlight = inFlight,
                ack = receipt,
                attemptId = 31,
                attemptGeneration = 8,
                currentGeneration = 8,
                cancelled = false,
            ),
        )

        // Stop advances the generation and removes the lease before calling
        // announcer.stop. The old path must therefore fail its start gate.
        inFlight.clear()
        assertFalse(
            ReplyPlaybackStartGuard.canEnqueue(
                inFlight = inFlight,
                ack = receipt,
                attemptId = 31,
                attemptGeneration = 8,
                currentGeneration = 9,
                cancelled = true,
            ),
        )
    }

    @Test
    fun localPlaybackStartGuardRejectsStopOrTurnReplacement() {
        assertTrue(
            ReplyPlaybackStartGuard.canEnqueueLocal(
                turnToken = "turn-1",
                currentTurnToken = "turn-1",
                attemptGeneration = 8,
                currentGeneration = 8,
                cancelled = false,
            ),
        )
        assertFalse(
            ReplyPlaybackStartGuard.canEnqueueLocal(
                turnToken = "turn-1",
                currentTurnToken = null,
                attemptGeneration = 8,
                currentGeneration = 9,
                cancelled = true,
            ),
        )
        assertFalse(
            ReplyPlaybackStartGuard.canEnqueueLocal(
                turnToken = "turn-1",
                currentTurnToken = "turn-2",
                attemptGeneration = 8,
                currentGeneration = 8,
                cancelled = false,
            ),
        )
    }
}
