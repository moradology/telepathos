package dev.telepathos

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ReplyAckDurabilityTest {
    private fun awaitingAck(
        laneId: String = "telepathos:direct",
        throughSeq: Long = 9,
        suffix: String = "1",
    ) = DurableReplyAck(
        ClientCommand.ReplyAck(
            laneId = laneId,
            replyTo = "delivery-$suffix",
            afterSeq = throughSeq - 5,
            throughSeq = throughSeq,
            turnToken = "turn-$suffix",
            interactionId = "interaction-$suffix",
        ),
        "reply text $suffix",
        ReplyAckPlaybackState.AwaitingPlayback,
    )

    @Test
    fun directPlaybackReceiptIsNotReplayedOrConsumedByThePendingPoll() {
        val directlyPlayed = awaitingAck().copy(state = ReplyAckPlaybackState.ReadyToAcknowledge)
        val retiring = awaitingAck(throughSeq = 14, suffix = "retiring")
            .copy(state = ReplyAckPlaybackState.RetirementPending)
        val pending = listOf(
            PendingItem(9, "direct reply", directlyPlayed.ack.replyTo),
            PendingItem(10, "ordinary update", null),
            PendingItem(14, "retiring reply", retiring.ack.replyTo),
            PendingItem(15, "unowned correlated update", "tp-later"),
        )

        val spoken = PendingPlaybackOwnership.spokenItems(
            entries = listOf(directlyPlayed, retiring),
            laneId = "telepathos:direct",
            items = pending,
        )

        assertEquals(listOf(10L, 15L), spoken.map(PendingItem::sequence))
        assertEquals(
            listOf("ordinary update", "unowned correlated update"),
            spoken.map(PendingItem::content),
        )
    }

    @Test
    fun awaitingPlaybackRecoveryUsesStoredExactTextThenAuthorizesOnlyItsReceipt() {
        val failedDirectReply = awaitingAck()
        val matchingPending = PendingItem(9, "transport copy", failedDirectReply.ack.replyTo)

        // The normal pending poll cannot claim the correlated delivery while
        // the durable receipt owns recovery.
        assertTrue(PendingPlaybackOwnership.spokenItems(
            entries = listOf(failedDirectReply),
            laneId = failedDirectReply.ack.laneId,
            items = listOf(matchingPending),
        ).isEmpty())

        val recovery = ReplyAckDurability.awaitingPlaybackRecovery(
            entries = listOf(failedDirectReply),
        )
        assertEquals(listOf("reply text 1"), recovery.map(DurableReplyAck::replyText))

        val authorized = ReplyAckDurability.markPlaybackHeard(
            entries = recovery,
            ack = failedDirectReply.ack,
        )
        assertEquals(ReplyAckPlaybackState.ReadyToAcknowledge, authorized!!.single().state)
        assertEquals(failedDirectReply.ack, ReplyAckDurability.retryCommand(authorized.single()))
    }

    @Test
    fun userSupersessionSuppressesActiveReceiptUntilAnExplicitRecoveryBoundary() {
        val awaiting = awaitingAck()

        val stopped = ReplyAckDurability.suppressPlayback(
            entries = listOf(awaiting),
            activeAcks = setOf(awaiting.ack),
        )

        assertEquals(ReplyAckPlaybackState.PlaybackSuppressed, stopped.single().state)
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(stopped).isEmpty())
        // A failed persistence write still keeps the in-memory fence out
        // of the one-second recovery selector until reconnect can re-arm it.
        assertTrue(
            ReplyAckDurability.awaitingPlaybackRecovery(
                entries = listOf(awaiting),
                suppressed = setOf(awaiting.ack),
            ).isEmpty(),
        )
        assertNull(ReplyAckDurability.retryCommand(stopped.single()))
        // A stale completion after supersession cannot authorize bridge consumption.
        assertNull(ReplyAckDurability.markPlaybackHeard(stopped, awaiting.ack))

        val rearmed = ReplyAckDurability.resumeSuppressedPlayback(stopped)
        assertEquals(ReplyAckPlaybackState.AwaitingPlayback, rearmed.single().state)
        assertEquals(listOf(rearmed.single()), ReplyAckDurability.awaitingPlaybackRecovery(rearmed))
    }

    @Test
    fun userSupersessionDurablySuppressesReceiptBeforeBridgeProofArrivesAndRetriesProof() {
        val pending = awaitingAck().copy(state = ReplyAckPlaybackState.ReceiptPending)

        val stopped = ReplyAckDurability.suppressPlayback(
            entries = listOf(pending),
            activeAcks = setOf(pending.ack),
        )

        assertEquals(ReplyAckPlaybackState.ReceiptPendingSuppressed, stopped.single().state)
        assertTrue(ReplyAckDurability.retryCommand(stopped.single()) is ClientCommand.ReplyReceived)
        assertNull(ReplyAckDurability.markPlaybackHeard(stopped, pending.ack))
        assertTrue(
            PendingPlaybackOwnership.spokenItems(
                entries = stopped,
                laneId = pending.ack.laneId,
                items = listOf(PendingItem(9, "stopped reply", pending.ack.replyTo)),
            ).isEmpty(),
        )
    }

    @Test
    fun reservationAfterUserSupersessionKeepsTheAcceptedReplyProofPendingButSilent() {
        val pending = awaitingAck(suffix = "race").copy(
            state = ReplyAckPlaybackState.ReceiptPendingSuppressed,
        )

        // The agent_end was accepted at generation 7, then a user action cleared the
        // turn and advanced the generation before reserveReplyReceipt ran.
        assertEquals(
            ReplyAckPlaybackState.ReceiptPendingSuppressed,
            ReplyAckDurability.reservationState(
                acceptedTurnToken = pending.ack.turnToken,
                acceptedGeneration = 7,
                currentTurnToken = null,
                currentGeneration = 8,
                playbackCancelled = true,
            ),
        )
        assertEquals(
            ClientCommand.ReplyReceived(
                laneId = pending.ack.laneId,
                replyTo = pending.ack.replyTo,
                afterSeq = pending.ack.afterSeq,
                throughSeq = pending.ack.throughSeq,
                turnToken = pending.ack.turnToken,
                interactionId = pending.ack.interactionId,
            ),
            ReplyAckDurability.retryCommand(pending),
        )
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(listOf(pending)).isEmpty())

        val confirmed = ReplyAckDurability.confirmReceipt(listOf(pending), pending.ack)!!
        assertEquals(ReplyAckPlaybackState.PlaybackSuppressed, confirmed.single().state)
        assertNull(ReplyAckDurability.markPlaybackHeard(confirmed, pending.ack))
    }

    @Test
    fun cancelCaptureFenceKeepsLateReceiptProofPendingAndSilent() {
        val cancelledTurn = "turn-cancelled-late"
        val fences = SupersededTurnFence.record(
            existing = emptyList(),
            supersededTurnToken = cancelledTurn,
            maxEntries = 4,
        )
        val lateAck = awaitingAck(suffix = "late").ack.copy(turnToken = cancelledTurn)
        val suppressed = ReplyAckDurability.reservationState(
            acceptedTurnToken = null,
            acceptedGeneration = null,
            currentTurnToken = null,
            currentGeneration = 12,
            playbackCancelled = true,
            playbackSuppressed = SupersededTurnFence.contains(fences, lateAck.turnToken),
        )

        // This is the CancelCapture -> late receipt-bearing agent_end path: it is
        // accepted from its own envelope, but remains proof-pending and silent.
        assertEquals(ReplyAckPlaybackState.ReceiptPendingSuppressed, suppressed)
        val pending = DurableReplyAck(lateAck, "late reply", suppressed)
        assertTrue(ReplyAckDurability.retryCommand(pending) is ClientCommand.ReplyReceived)

        val confirmed = ReplyAckDurability.confirmReceipt(listOf(pending), lateAck)!!
        assertEquals(ReplyAckPlaybackState.PlaybackSuppressed, confirmed.single().state)
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(confirmed).isEmpty())
    }

    @Test
    fun supersededTurnFenceIsBoundedAndDoesNotMatchAnotherReplay() {
        val fences = SupersededTurnFence.record(
            existing = listOf("turn-old", "turn-previous"),
            supersededTurnToken = "turn-superseded",
            maxEntries = 2,
        )

        assertEquals(listOf("turn-previous", "turn-superseded"), fences)
        assertTrue(SupersededTurnFence.contains(fences, "turn-superseded"))
        assertFalse(SupersededTurnFence.contains(fences, "turn-old"))
        assertFalse(SupersededTurnFence.contains(fences, "turn-legitimate-reconnect"))
        assertEquals(
            ReplyAckPlaybackState.ReceiptPending,
            ReplyAckDurability.reservationState(
                acceptedTurnToken = null,
                acceptedGeneration = null,
                currentTurnToken = null,
                currentGeneration = 13,
                playbackCancelled = true,
                playbackSuppressed = SupersededTurnFence.contains(fences, "turn-legitimate-reconnect"),
            ),
        )
    }

    @Test
    fun supersedingOneTurnDoesNotSuppressAnUnrelatedReceipt() {
        val superseded = awaitingAck(suffix = "superseded").copy(
            state = ReplyAckPlaybackState.ReceiptPending,
            ack = awaitingAck(suffix = "superseded").ack.copy(turnToken = "turn-superseded"),
        )
        val unrelated = awaitingAck(suffix = "unrelated").copy(
            state = ReplyAckPlaybackState.AwaitingPlayback,
            ack = awaitingAck(suffix = "unrelated").ack.copy(turnToken = "turn-unrelated"),
        )

        val toSuppress = ReplyAckDurability.activeAcksForSupersession(
            entries = listOf(superseded, unrelated),
            supersededTurnToken = "turn-superseded",
            playbackLeases = emptySet(),
        )

        assertEquals(setOf(superseded.ack), toSuppress)
        val afterSupersession = ReplyAckDurability.suppressPlayback(
            entries = listOf(superseded, unrelated),
            activeAcks = toSuppress,
        )
        assertEquals(ReplyAckPlaybackState.ReceiptPendingSuppressed, afterSupersession[0].state)
        assertEquals(ReplyAckPlaybackState.AwaitingPlayback, afterSupersession[1].state)
        assertEquals(
            listOf(unrelated),
            ReplyAckDurability.awaitingPlaybackRecovery(afterSupersession),
        )
    }

    @Test
    fun bridgeProofKeepsSupersededReceiptSilentUntilReadyRearmsIt() {
        val pending = awaitingAck().copy(state = ReplyAckPlaybackState.ReceiptPending)
        val stopped = ReplyAckDurability.suppressPlayback(listOf(pending), setOf(pending.ack))

        val confirmedWhileStopped = ReplyAckDurability.confirmReceipt(stopped, pending.ack)
        assertEquals(ReplyAckPlaybackState.PlaybackSuppressed, confirmedWhileStopped!!.single().state)
        assertTrue(ReplyAckDurability.awaitingPlaybackRecovery(confirmedWhileStopped).isEmpty())

        val rearmed = ReplyAckDurability.resumeSuppressedPlayback(confirmedWhileStopped)
        assertEquals(ReplyAckPlaybackState.AwaitingPlayback, rearmed.single().state)
        assertEquals(listOf(rearmed.single()), ReplyAckDurability.awaitingPlaybackRecovery(rearmed))

        // If Ready re-armed before the proof arrived, the proof remains the
        // required handshake and only then creates an AwaitingPlayback job.
        val rearmedBeforeProof = ReplyAckDurability.resumeSuppressedPlayback(stopped)
        assertEquals(ReplyAckPlaybackState.ReceiptPending, rearmedBeforeProof.single().state)
        assertTrue(ReplyAckDurability.retryCommand(rearmedBeforeProof.single()) is ClientCommand.ReplyReceived)
        val confirmedAfterRearm = ReplyAckDurability.confirmReceipt(rearmedBeforeProof, pending.ack)
        assertEquals(ReplyAckPlaybackState.AwaitingPlayback, confirmedAfterRearm!!.single().state)
    }

    @Test
    fun snapshotRoundTripRetainsProofPendingUserSuppression() {
        val owner = "persisted-owner-a"
        val stopped = awaitingAck().copy(state = ReplyAckPlaybackState.ReceiptPendingSuppressed)

        val decoded = ReplyAckSnapshot.decode(
            raw = ReplyAckSnapshot.encode(owner, listOf(stopped)),
            currentOwner = owner,
            maxEntries = 64,
        )

        assertEquals(listOf(stopped), decoded)
    }

    @Test
    fun oneInFlightReplyCanStillBeDurablyReservedAtCaptureCapacity() {
        assertTrue(ReplyAckDurability.canReserveReceipt(storedCount = 64, maxStored = 65))
        assertFalse(ReplyAckDurability.canReserveReceipt(storedCount = 65, maxStored = 65))
    }

    @Test
    fun acknowledgedReplyRemainsDurableUntilItsTerminalRetirementConfirmation() {
        val ready = awaitingAck().copy(state = ReplyAckPlaybackState.ReadyToAcknowledge)

        val retirementPending = ReplyAckDurability.beginRetirement(listOf(ready), ready.ack)

        assertEquals(ReplyAckPlaybackState.RetirementPending, retirementPending!!.single().state)
        assertEquals(
            ClientCommand.ReplyAckRetire(
                laneId = ready.ack.laneId,
                replyTo = ready.ack.replyTo,
                afterSeq = ready.ack.afterSeq,
                throughSeq = ready.ack.throughSeq,
                turnToken = ready.ack.turnToken,
                interactionId = ready.ack.interactionId,
            ),
            ReplyAckDurability.retryCommand(retirementPending.single()),
        )
        // A bridge restart can repeat reply_acknowledged: the persisted state
        // remains terminal-pending and resends reply_ack_retire, not reply_ack.
        assertEquals(retirementPending, ReplyAckDurability.beginRetirement(retirementPending, ready.ack))
        assertNull(ReplyAckDurability.completeRetirement(listOf(ready), ready.ack))
        assertTrue(ReplyAckDurability.completeRetirement(retirementPending, ready.ack)!!.isEmpty())
    }

    @Test
    fun sequentialTerminalRetirementsReleaseMoreThanTheServerBindingCap() {
        var stored = emptyList<DurableReplyAck>()

        repeat(65) { index ->
            assertTrue(ReplyAckDurability.canReserveReceipt(stored.size, maxStored = 64))
            val ready = awaitingAck(throughSeq = index + 10L, suffix = index.toString())
                .copy(state = ReplyAckPlaybackState.ReadyToAcknowledge)
            stored = ReplyAckDurability.beginRetirement(listOf(ready), ready.ack)!!
            stored = ReplyAckDurability.completeRetirement(stored, ready.ack)!!
            assertTrue(stored.isEmpty())
        }
    }

    @Test
    fun terminalFramesNeverPromoteAnUnheardReplyOrClearAnAlreadyRemovedRecord() {
        val awaiting = awaitingAck()

        assertNull(ReplyAckDurability.beginRetirement(listOf(awaiting), awaiting.ack))
        assertNull(ReplyAckDurability.completeRetirement(emptyList(), awaiting.ack))
        assertNull(ReplyAckDurability.retryCommand(awaiting))
    }

    @Test
    fun awaitingPlaybackRecoversSavedTextAfterProcessDeathEvenWhenLaneChanged() {
        val savedOnOldLane = awaitingAck(laneId = "telepathos:research")

        // Model a fresh service after process death: there is no in-memory
        // recovery attempt, and the currently selected lane is different.
        val recovery = ReplyAckDurability.awaitingPlaybackRecovery(
            entries = listOf(savedOnOldLane),
            inFlight = emptySet(),
        )

        assertEquals(1, recovery.size)
        assertEquals("reply text 1", recovery.single().replyText)
        assertEquals("telepathos:research", recovery.single().ack.laneId)
        // AwaitingPlayback is recovered by local TTS, never by a wire ack.
        assertNull(ReplyAckDurability.retryCommand(recovery.single()))
    }

    @Test
    fun duplicateReadyOrReconnectDoesNotStartTheSameRecoveryTwice() {
        val awaiting = awaitingAck()

        val firstAttempt = ReplyAckDurability.awaitingPlaybackRecovery(
            entries = listOf(awaiting),
            inFlight = emptySet(),
        )
        val duplicateAttempt = ReplyAckDurability.awaitingPlaybackRecovery(
            entries = listOf(awaiting),
            inFlight = setOf(awaiting.ack),
        )

        assertEquals(listOf(awaiting), firstAttempt)
        assertTrue(duplicateAttempt.isEmpty())
    }

    @Test
    fun receiptPendingDeliveryStaysHiddenUntilTheBridgeConfirmsItsReceipt() {
        val receiptPending = awaitingAck().copy(state = ReplyAckPlaybackState.ReceiptPending)

        // The bridge has not persisted its copy yet, so neither normal pending
        // narration nor its exact consume list may claim this correlated row.
        val spoken = PendingPlaybackOwnership.spokenItems(
            entries = listOf(receiptPending),
            laneId = receiptPending.ack.laneId,
            items = listOf(PendingItem(9, "not yet ours to speak", receiptPending.ack.replyTo)),
        )
        assertTrue(spoken.isEmpty())
        assertEquals(
            ClientCommand.ReplyReceived(
                laneId = receiptPending.ack.laneId,
                replyTo = receiptPending.ack.replyTo,
                afterSeq = receiptPending.ack.afterSeq,
                throughSeq = receiptPending.ack.throughSeq,
                turnToken = receiptPending.ack.turnToken,
                interactionId = receiptPending.ack.interactionId,
            ),
            ReplyAckDurability.retryCommand(receiptPending),
        )
    }

    @Test
    fun serverIdentityChangesWithCredentialsWithoutStoringRawToken() {
        val first = ReplyAckDurability.serverIdentity("wss://bridge.example", "secret-token")
        val same = ReplyAckDurability.serverIdentity("wss://bridge.example", "secret-token")
        val otherToken = ReplyAckDurability.serverIdentity("wss://bridge.example", "other-token")
        val otherServer = ReplyAckDurability.serverIdentity("wss://other.example", "secret-token")

        assertEquals(first, same)
        assertNotEquals(first, otherToken)
        assertNotEquals(first, otherServer)
        assertFalse(first.contains("secret-token"))
    }

    @Test
    fun durableReceiptValidationUsesTheSameTurnAndSequenceLimitsAsTheWireParser() {
        val atLimit = ClientCommand.ReplyAck(
            laneId = "telepathos:direct",
            replyTo = "tp-limit",
            afterSeq = MAX_SAFE_SEQUENCE - 1,
            throughSeq = MAX_SAFE_SEQUENCE,
            turnToken = "t".repeat(MAX_TURN_TOKEN_LENGTH),
            interactionId = "i-limit",
        )
        assertTrue(ReplyAckDurability.isValidStoredReceipt(atLimit))
        assertFalse(
            ReplyAckDurability.isValidStoredReceipt(
                atLimit.copy(turnToken = "t".repeat(MAX_TURN_TOKEN_LENGTH + 1)),
            ),
        )
        assertFalse(
            ReplyAckDurability.isValidStoredReceipt(
                atLimit.copy(afterSeq = MAX_SAFE_SEQUENCE, throughSeq = MAX_SAFE_SEQUENCE + 1),
            ),
        )
    }

    @Test
    fun snapshotOwnerValidationAcceptsTheExactInstallationOwner() {
        val owner = "persisted-owner-a"
        ReplyAckSnapshot.validateOwner(ReplyAckSnapshot.VERSION, owner, owner)
    }

    @Test(expected = IllegalArgumentException::class)
    fun rotatedInstallationCannotLoadThePreviousOwnersReceipts() {
        var persisted: String? = null
        val rotatedOwner = InstallationIdentity.loadOrCreate(
            current = persisted,
            generate = { "persisted-owner-b" },
            persist = { value -> persisted = value; true },
        )

        assertEquals("persisted-owner-b", rotatedOwner)
        ReplyAckSnapshot.validateOwner(ReplyAckSnapshot.VERSION, "persisted-owner-a", rotatedOwner)
    }

    @Test(expected = IllegalArgumentException::class)
    fun snapshotWithoutAnOwnerFailsClosedInsteadOfMigrating() {
        val owner = "persisted-owner-a"
        ReplyAckSnapshot.validateOwner(ReplyAckSnapshot.VERSION, null, owner)
    }

    @Test(expected = IllegalArgumentException::class)
    fun previousSnapshotVersionIsNotSilentlyMigrated() {
        val owner = "persisted-owner-a"
        ReplyAckSnapshot.validateOwner(ReplyAckSnapshot.VERSION - 1, owner, owner)
    }

    @Test
    fun failedDirectReplyEndsTurnThenReceiptRecoveryAuthorizesTheExactReply() {
        val failedDirectReply = awaitingAck()
        val activeTurn = ReplyPlaybackTurnState(
            turnToken = "turn-direct",
            interactionId = "interaction-direct",
            endAccepted = true,
            cancelled = false,
            generation = 11,
        )

        val afterFailure = ReplyPlaybackFailure.invalidateCurrentTurn(
            state = activeTurn,
            callbackGeneration = 11,
        )

        assertNull(afterFailure.turnToken)
        assertNull(afterFailure.interactionId)
        assertTrue(afterFailure.cancelled)
        assertEquals(12L, afterFailure.generation)
        // The durable receipt is not removed or authorized by the failed TTS.
        assertEquals(ReplyAckPlaybackState.AwaitingPlayback, failedDirectReply.state)
        assertTrue(
            ReplyAckCaptureGate.allowsPendingFetch(
                entries = listOf(failedDirectReply),
                maxEntries = 64,
                stateCorrupt = false,
                persistenceFailed = false,
            ),
        )
        assertTrue(
            ReplyAckCaptureGate.allowsPendingFetch(
                entries = listOf(failedDirectReply),
                maxEntries = 1,
                stateCorrupt = false,
                persistenceFailed = false,
            ),
        )

        // The same-socket next pinch may fetch generic work, but the mic stays
        // closed until the receipt-recovery owner has spoken its exact text.
        assertFalse(
            ReplyAckCaptureGate.allowsMicAfterPendingFetch(
                entries = listOf(failedDirectReply),
                maxEntries = 64,
                stateCorrupt = false,
                persistenceFailed = false,
            ),
        )

        val recovery = ReplyAckDurability.awaitingPlaybackRecovery(
            entries = listOf(failedDirectReply),
        )
        val completed = ReplyAckDurability.markPlaybackHeard(
            entries = recovery,
            ack = failedDirectReply.ack,
        )!!
        assertEquals(ReplyAckPlaybackState.ReadyToAcknowledge, completed.single().state)
        assertTrue(
            ReplyAckCaptureGate.allowsMicAfterPendingFetch(
                entries = completed,
                maxEntries = 64,
                stateCorrupt = false,
                persistenceFailed = false,
            ),
        )
    }
}
