package dev.telepathy

import android.app.Service
import android.content.Intent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.IntentFilter
import android.media.AudioDeviceInfo
import android.media.AudioDeviceCallback
import android.media.AudioManager
import android.media.ToneGenerator
import android.media.session.MediaSession
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.util.Log
import android.view.KeyEvent
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString.Companion.toByteString
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.ArrayDeque
import org.json.JSONArray
import org.json.JSONObject

/** Durable lifecycle for a bridge delivery receipt. */
internal enum class ReplyAckPlaybackState {
    /** Local replay envelope is durable; bridge proof still needs confirmation. */
    ReceiptPending,
    /** A user action superseded playback before bridge proof; retain the proof obligation. */
    ReceiptPendingSuppressed,
    AwaitingPlayback,
    /** A user action superseded playback; retain the receipt without retrying it. */
    PlaybackSuppressed,
    ReadyToAcknowledge,
    /** The bridge durably consumed the acknowledgement; retry terminal retirement. */
    RetirementPending,
}

internal data class DurableReplyAck(
    val ack: ClientCommand.ReplyAck,
    /** Complete durable reply text used after a lost direct agent_end. */
    val replyText: String,
    val state: ReplyAckPlaybackState,
)

/**
 * Immutable origin for one pending-delivery snapshot.  Pending narration can
 * outlive a settings change, so a later consume must never re-read a different
 * endpoint or credential from shared preferences.
 */
internal data class PendingConsumeContext(
    val apiBaseUrl: String?,
    val token: String?,
    val configuredSocketUrl: String,
    val socketIdentity: Any,
    val socketUrl: String?,
    val socketToken: String?,
)

/** Pure identity check for the deferred /api/pending/consume worker. */
internal object PendingConsumeGuard {
    fun isCurrent(
        captured: PendingConsumeContext,
        currentApiBaseUrl: String?,
        currentToken: String?,
        currentSocketUrl: String,
        currentSocket: Any?,
        currentSocketConfigUrl: String?,
        currentSocketConfigToken: String?,
    ): Boolean =
        captured.apiBaseUrl == currentApiBaseUrl &&
            captured.token == currentToken &&
            equivalentWebSocketEndpoint(captured.configuredSocketUrl, currentSocketUrl) &&
            captured.socketIdentity === currentSocket &&
            captured.socketUrl != null &&
            equivalentWebSocketEndpoint(captured.socketUrl, captured.configuredSocketUrl) &&
            captured.socketToken == captured.token &&
            currentSocketConfigUrl != null &&
            equivalentWebSocketEndpoint(captured.socketUrl, currentSocketConfigUrl) &&
            captured.socketToken == currentSocketConfigToken
}

/** Stops delayed service work from running or re-scheduling after teardown. */
internal class ServiceTeardownGuard {
    @Volatile private var tornDown = false

    fun isActive(): Boolean = !tornDown

    fun runIfActive(action: () -> Unit): Boolean = synchronized(this) {
        if (tornDown) return@synchronized false
        action()
        true
    }

    @Synchronized
    fun beginTeardown(): Boolean {
        if (tornDown) return false
        tornDown = true
        return true
    }
}

internal data class PendingItemRecord(
    val sequence: Any?,
    val content: Any?,
    /** Optional correlation token; absent only for independently pending work. */
    val replyTo: Any?,
)

/** One fully validated row from telepathyd's pending-delivery snapshot. */
internal data class PendingItem(
    val sequence: Long,
    val content: String,
    val replyTo: String?,
)

internal data class ParsedPendingItems(
    val items: List<PendingItem>,
)

/** Validates every pending record before any content can be narrated or consumed. */
internal object PendingItemsParser {
    fun parse(records: List<PendingItemRecord>): ParsedPendingItems? {
        val items = ArrayList<PendingItem>(records.size)
        var previousSequence = 0L
        for (record in records) {
            val sequence = parseSafeSequence(record.sequence) ?: return null
            // `/api/pending` is a queue snapshot, so accepting duplicates or
            // reordering would make a later exact acknowledgement ambiguous.
            if (sequence <= previousSequence) return null
            val content = record.content as? String ?: return null
            if (content.isBlank()) return null
            if (!isReplyTextWithinLimit(content)) return null
            val replyTo = when (val raw = record.replyTo) {
                null -> null
                is String -> raw.takeIf(::isValidOpaqueId) ?: return null
                else -> return null
            }
            items += PendingItem(sequence, content, replyTo)
            previousSequence = sequence
        }
        return ParsedPendingItems(items)
    }
}

/**
 * Selects the playback owner for rows returned by `/api/pending`.
 *
 * A durable reply receipt owns only its correlated, bounded sequence range.
 * Those rows stay out of normal pending narration: an AwaitingPlayback receipt
 * is retried from its exact stored text, while either suppressed receipt state
 * stays owned until its proof/recovery boundary. Every other receipt state is
 * already directly playing, ready to acknowledge, or terminally retiring.
 * Generic rows and unowned correlated rows remain normal pending work.
 */
internal object PendingPlaybackOwnership {
    private fun isOwned(
        entry: DurableReplyAck,
        laneId: String?,
        item: PendingItem,
    ): Boolean =
        laneId == entry.ack.laneId &&
            item.replyTo == entry.ack.replyTo &&
            item.sequence > entry.ack.afterSeq &&
            item.sequence <= entry.ack.throughSeq

    fun spokenItems(
        entries: Collection<DurableReplyAck>,
        laneId: String?,
        items: List<PendingItem>,
    ): List<PendingItem> = items.filter { item ->
        entries.none { entry -> isOwned(entry, laneId, item) }
    }
}

/** Immutable owner for one listener's pending reply-ack send pass. */
internal data class ReplyAckSendContext(
    val serverUrl: String,
    val token: String?,
    val identity: String,
    val socketIdentity: Any,
)

/** Pure identity check that prevents a superseded socket from sending another bridge's receipts. */
internal object ReplyAckSendGuard {
    fun isCurrent(
        captured: ReplyAckSendContext,
        currentReplyAckIdentity: String?,
        currentServerUrl: String,
        currentToken: String?,
        currentSocket: Any?,
        currentSocketConfigUrl: String?,
        currentSocketConfigToken: String?,
    ): Boolean =
        captured.identity == currentReplyAckIdentity &&
            equivalentWebSocketEndpoint(captured.serverUrl, currentServerUrl) &&
            captured.token == currentToken &&
            captured.socketIdentity === currentSocket &&
            currentSocketConfigUrl != null &&
            equivalentWebSocketEndpoint(captured.serverUrl, currentSocketConfigUrl) &&
            captured.token == currentSocketConfigToken
}

/** Immutable ownership fence for the final phase of a WebSocket [WebSocketListener.onOpen]. */
internal data class SocketOpenContext(
    val serverUrl: String,
    val token: String?,
    val socketIdentity: Any,
    val generation: Long,
)

/**
 * A listener may be descheduled after its first onOpen check.  It must prove
 * that it still owns the active configuration before it publishes connection
 * state or initiates traffic for that configuration.
 */
internal object SocketOpenGuard {
    fun isCurrent(
        captured: SocketOpenContext,
        currentServerUrl: String,
        currentToken: String?,
        currentSocket: Any?,
        currentSocketConfigUrl: String?,
        currentSocketConfigToken: String?,
        currentGeneration: Long,
        wantsConnection: Boolean,
    ): Boolean =
        wantsConnection &&
            equivalentWebSocketEndpoint(captured.serverUrl, currentServerUrl) &&
            captured.token == currentToken &&
            captured.socketIdentity === currentSocket &&
            currentSocketConfigUrl != null &&
            equivalentWebSocketEndpoint(captured.serverUrl, currentSocketConfigUrl) &&
            captured.token == currentSocketConfigToken &&
            captured.generation == currentGeneration
}

/**
 * A socket is usable only after its own hello was queued and its own bridge
 * returned ready. Keeping this pure lets the race boundary be tested without
 * an OkHttp scheduler.
 */
internal object HelloReadinessGuard {
    fun canPublish(
        helloQueued: Boolean,
        readyReceived: Boolean,
        contextIsCurrent: Boolean,
    ): Boolean = helloQueued && readyReceived && contextIsCurrent
}

/** Pure receipt helpers kept here so the service's durability invariants are unit-testable. */
internal object ReplyAckDurability {
    fun canReserveReceipt(storedCount: Int, maxStored: Int): Boolean = storedCount < maxStored

    /**
     * Keep an accepted live-turn reply tied to that turn until its receipt is
     * reserved. If a user action superseded the callback or a newer turn won
     * that interval, retain the bridge proof obligation but suppress playback
     * until the explicit reconnect/ready re-arm.
     *
     * A replayed agent_end has no live-turn generation and may create its
     * ordinary proof-pending receipt, unless an in-memory supersession fence already
     * owns that receipt.
     */
    fun reservationState(
        acceptedTurnToken: String?,
        acceptedGeneration: Long?,
        currentTurnToken: String?,
        currentGeneration: Long,
        playbackCancelled: Boolean,
        playbackSuppressed: Boolean = false,
    ): ReplyAckPlaybackState {
        if (playbackSuppressed) return ReplyAckPlaybackState.ReceiptPendingSuppressed
        if (acceptedGeneration == null) return ReplyAckPlaybackState.ReceiptPending
        val acceptedTurnIsCurrent =
            acceptedTurnToken == currentTurnToken &&
                acceptedGeneration == currentGeneration &&
                !playbackCancelled
        return if (acceptedTurnIsCurrent) {
            ReplyAckPlaybackState.ReceiptPending
        } else {
            ReplyAckPlaybackState.ReceiptPendingSuppressed
        }
    }

    /** Used before a locally persisted receipt can authorize a later retry. */
    fun isValidStoredReceipt(ack: ClientCommand.ReplyAck): Boolean =
        isValidLaneId(ack.laneId) &&
            isValidOpaqueId(ack.replyTo) &&
            isValidTurnToken(ack.turnToken) &&
            isValidOpaqueId(ack.interactionId) &&
            ack.afterSeq in 0L..MAX_SAFE_SEQUENCE &&
            ack.throughSeq in 0L..MAX_SAFE_SEQUENCE &&
            ack.throughSeq > ack.afterSeq

    /**
     * Playback permission becomes consumption authority only after the text
     * was heard. Both direct replay and recovery use this one durable state
     * transition; terminal states are handled by their retry paths instead.
     */
    fun markPlaybackHeard(
        entries: List<DurableReplyAck>,
        ack: ClientCommand.ReplyAck,
    ): List<DurableReplyAck>? {
        val existing = entries.firstOrNull { entry -> entry.ack == ack } ?: return null
        return when (existing.state) {
            ReplyAckPlaybackState.AwaitingPlayback -> entries.map { entry ->
                if (entry.ack == ack) entry.copy(state = ReplyAckPlaybackState.ReadyToAcknowledge) else entry
            }
            ReplyAckPlaybackState.ReceiptPending,
            ReplyAckPlaybackState.ReceiptPendingSuppressed,
            ReplyAckPlaybackState.PlaybackSuppressed,
            ReplyAckPlaybackState.ReadyToAcknowledge,
            ReplyAckPlaybackState.RetirementPending -> null
        }
    }

    /**
     * Apply the bridge's durable `reply_received` proof without authorizing
     * playback for a reply that was superseded before the proof arrived.
     */
    fun confirmReceipt(
        entries: List<DurableReplyAck>,
        ack: ClientCommand.ReplyAck,
        playbackSuppressed: Boolean = false,
    ): List<DurableReplyAck>? {
        val existing = entries.firstOrNull { entry -> entry.ack == ack } ?: return null
        val nextState = when (existing.state) {
            ReplyAckPlaybackState.ReceiptPending ->
                if (playbackSuppressed) ReplyAckPlaybackState.PlaybackSuppressed
                else ReplyAckPlaybackState.AwaitingPlayback
            ReplyAckPlaybackState.ReceiptPendingSuppressed -> ReplyAckPlaybackState.PlaybackSuppressed
            ReplyAckPlaybackState.AwaitingPlayback,
            ReplyAckPlaybackState.PlaybackSuppressed,
            ReplyAckPlaybackState.ReadyToAcknowledge,
            ReplyAckPlaybackState.RetirementPending -> return null
        }
        return entries.map { entry ->
            if (entry.ack == ack) entry.copy(state = nextState) else entry
        }
    }

    /**
     * A user action may supersede an active receipt-backed TTS attempt, but it must
     * not authorize bridge consumption. Persisting a separate state prevents
     * the retry loop from treating an intentional cancellation as TTS failure.
     * ReceiptPending becomes ReceiptPendingSuppressed: its bridge proof still
     * needs to be retried, but reconnect must not forget the user's choice.
     */
    fun suppressPlayback(
        entries: List<DurableReplyAck>,
        activeAcks: Set<ClientCommand.ReplyAck>,
    ): List<DurableReplyAck> = entries.map { entry ->
        if (entry.ack !in activeAcks) return@map entry
        when (entry.state) {
            ReplyAckPlaybackState.ReceiptPending -> entry.copy(state = ReplyAckPlaybackState.ReceiptPendingSuppressed)
            ReplyAckPlaybackState.AwaitingPlayback -> entry.copy(state = ReplyAckPlaybackState.PlaybackSuppressed)
            ReplyAckPlaybackState.ReceiptPendingSuppressed,
            ReplyAckPlaybackState.PlaybackSuppressed,
            ReplyAckPlaybackState.ReadyToAcknowledge,
            ReplyAckPlaybackState.RetirementPending -> entry
        }
    }

    /**
     * Select only the receipt obligations a user supersession is allowed to
     * fence: pending playback owned by the replaced turn and any receipt with an
     * active TTS lease. Older unrelated receipts remain recoverable.
     */
    fun activeAcksForSupersession(
        entries: Collection<DurableReplyAck>,
        supersededTurnToken: String?,
        playbackLeases: Set<ClientCommand.ReplyAck>,
    ): Set<ClientCommand.ReplyAck> = buildSet {
        addAll(playbackLeases)
        if (supersededTurnToken != null) {
            entries
                .filter { entry ->
                    entry.ack.turnToken == supersededTurnToken &&
                        when (entry.state) {
                            ReplyAckPlaybackState.ReceiptPending,
                            ReplyAckPlaybackState.ReceiptPendingSuppressed,
                            ReplyAckPlaybackState.AwaitingPlayback -> true
                            ReplyAckPlaybackState.PlaybackSuppressed,
                            ReplyAckPlaybackState.ReadyToAcknowledge,
                            ReplyAckPlaybackState.RetirementPending -> false
                        }
                }
                .forEach { entry -> add(entry.ack) }
        }
    }

    /** Re-arm user-suppressed receipts only at an explicit recovery boundary. */
    fun resumeSuppressedPlayback(entries: List<DurableReplyAck>): List<DurableReplyAck> =
        entries.map { entry ->
            when (entry.state) {
                ReplyAckPlaybackState.ReceiptPendingSuppressed ->
                    entry.copy(state = ReplyAckPlaybackState.ReceiptPending)
                ReplyAckPlaybackState.PlaybackSuppressed ->
                    entry.copy(state = ReplyAckPlaybackState.AwaitingPlayback)
                ReplyAckPlaybackState.ReceiptPending,
                ReplyAckPlaybackState.AwaitingPlayback,
                ReplyAckPlaybackState.ReadyToAcknowledge,
                ReplyAckPlaybackState.RetirementPending -> entry
            }
        }

    fun serverIdentity(url: String, token: String?): String {
        val canonicalUrl = normalizeWebSocketEndpoint(url)
        val credentialHash = sha256Hex(token.orEmpty())
        val material = "reply-state-v2\u0000$canonicalUrl\u0000$credentialHash".toByteArray(Charsets.UTF_8)
        return MessageDigest.getInstance("SHA-256").digest(material)
            .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
    }

    /**
     * The bridge's reply_acknowledged frame is not permission to erase the
     * local receipt.  First persist this state, then retry reply_ack_retire
     * until the bridge's durably-produced reply_ack_retired confirmation.
     *
     * A duplicate reply_acknowledged is intentionally idempotent: it returns
     * the already-retiring entry unchanged so its terminal frame is resent.
     * A confirmation for an acknowledgement that was never durably ready is
     * rejected rather than skipping the consumption authorization boundary.
     */
    fun beginRetirement(
        entries: List<DurableReplyAck>,
        ack: ClientCommand.ReplyAck,
    ): List<DurableReplyAck>? {
        val existing = entries.firstOrNull { entry -> entry.ack == ack } ?: return null
        return when (existing.state) {
            ReplyAckPlaybackState.ReadyToAcknowledge -> entries.map { entry ->
                if (entry.ack == ack) entry.copy(state = ReplyAckPlaybackState.RetirementPending) else entry
            }
            ReplyAckPlaybackState.RetirementPending -> entries
            ReplyAckPlaybackState.ReceiptPending -> null
            ReplyAckPlaybackState.ReceiptPendingSuppressed -> null
            ReplyAckPlaybackState.AwaitingPlayback -> null
            ReplyAckPlaybackState.PlaybackSuppressed -> null
        }
    }

    /** Only an already-durable retirement request may be cleared by the bridge. */
    fun completeRetirement(
        entries: List<DurableReplyAck>,
        ack: ClientCommand.ReplyAck,
    ): List<DurableReplyAck>? {
        val existing = entries.firstOrNull { entry -> entry.ack == ack } ?: return null
        if (existing.state != ReplyAckPlaybackState.RetirementPending) return null
        return entries.filterNot { entry -> entry.ack == ack }
    }

    fun retirementCommand(ack: ClientCommand.ReplyAck): ClientCommand.ReplyAckRetire =
        ClientCommand.ReplyAckRetire(
            laneId = ack.laneId,
            replyTo = ack.replyTo,
            afterSeq = ack.afterSeq,
            throughSeq = ack.throughSeq,
            turnToken = ack.turnToken,
            interactionId = ack.interactionId,
        )

    fun retryCommand(entry: DurableReplyAck): ClientCommand? = when (entry.state) {
        ReplyAckPlaybackState.ReceiptPending,
        ReplyAckPlaybackState.ReceiptPendingSuppressed -> ClientCommand.ReplyReceived(
            laneId = entry.ack.laneId,
            replyTo = entry.ack.replyTo,
            afterSeq = entry.ack.afterSeq,
            throughSeq = entry.ack.throughSeq,
            turnToken = entry.ack.turnToken,
            interactionId = entry.ack.interactionId,
        )
        ReplyAckPlaybackState.AwaitingPlayback -> null
        ReplyAckPlaybackState.PlaybackSuppressed -> null
        ReplyAckPlaybackState.ReadyToAcknowledge -> entry.ack
        ReplyAckPlaybackState.RetirementPending -> retirementCommand(entry.ack)
    }

    /**
     * AwaitingPlayback is a local playback obligation, not a wire command.
     * Reconnect recovery deliberately ignores the currently selected lane:
     * the saved receipt owns its original lane and text.
     */
    fun awaitingPlaybackRecovery(
        entries: List<DurableReplyAck>,
        inFlight: Set<ClientCommand.ReplyAck> = emptySet(),
        suppressed: Set<ClientCommand.ReplyAck> = emptySet(),
    ): List<DurableReplyAck> = entries.filter { entry ->
        entry.state == ReplyAckPlaybackState.AwaitingPlayback &&
            entry.ack !in inFlight &&
            entry.ack !in suppressed
    }
}

/**
 * Bounded in-memory fence for turn-bound replies arriving after a user action
 * supersedes their turn (Stop, CancelCapture, StartCapture, or Repeat).
 * The durable receipt remains the source of truth; this only closes the
 * interval between Stop and the late agent_end reservation.
 */
internal object SupersededTurnFence {
    fun record(
        existing: Collection<String>,
        supersededTurnToken: String?,
        maxEntries: Int,
    ): List<String> {
        require(maxEntries > 0)
        val next = LinkedHashSet<String>()
        existing.filter { it.isNotBlank() }.forEach { next.add(it) }
        if (!supersededTurnToken.isNullOrBlank()) {
            next.remove(supersededTurnToken)
            next.add(supersededTurnToken)
        }
        while (next.size > maxEntries) next.remove(next.first())
        return next.toList()
    }

    fun contains(fences: Collection<String>, turnToken: String?): Boolean =
        !turnToken.isNullOrBlank() && fences.contains(turnToken)
}

/** Hard-cutover durable snapshot for receipts owned by one app installation. */
internal object ReplyAckSnapshot {
    const val VERSION = 8

    fun validateOwner(snapshotVersion: Int, snapshotOwner: String?, currentOwner: String) {
        require(InstallationIdentity.isValid(currentOwner)) {
            "invalid current reply acknowledgement owner"
        }
        if (snapshotVersion != VERSION) {
            throw IllegalArgumentException("unsupported reply acknowledgement state version")
        }
        if (snapshotOwner == null) {
            throw IllegalArgumentException("reply acknowledgement state has no installation owner")
        }
        if (!InstallationIdentity.isValid(snapshotOwner) || snapshotOwner != currentOwner) {
            throw IllegalArgumentException("reply acknowledgement state belongs to another installation")
        }
    }

    fun encode(owner: String, entries: Collection<DurableReplyAck>): String {
        require(InstallationIdentity.isValid(owner)) { "invalid reply acknowledgement owner" }
        val array = JSONArray()
        entries.forEach { entry ->
            val ack = entry.ack
            require(ReplyAckDurability.isValidStoredReceipt(ack)) {
                "invalid reply acknowledgement receipt"
            }
            require(isReplyTextWithinLimit(entry.replyText)) {
                "reply text exceeds the UTF-8 byte limit"
            }
            array.put(JSONObject()
                .put("lane_id", ack.laneId)
                .put("reply_to", ack.replyTo)
                .put("after_seq", ack.afterSeq)
                .put("through_seq", ack.throughSeq)
                .put("turn_token", ack.turnToken)
                .put("interaction_id", ack.interactionId)
                .put("reply_text", entry.replyText)
                .put(
                    "state",
                    when (entry.state) {
                        ReplyAckPlaybackState.ReceiptPending -> "receipt_pending"
                        ReplyAckPlaybackState.ReceiptPendingSuppressed -> "receipt_pending_suppressed"
                        ReplyAckPlaybackState.AwaitingPlayback -> "awaiting_playback"
                        ReplyAckPlaybackState.PlaybackSuppressed -> "playback_suppressed"
                        ReplyAckPlaybackState.ReadyToAcknowledge -> "ready_to_acknowledge"
                        ReplyAckPlaybackState.RetirementPending -> "retirement_pending"
                    },
                ))
        }
        return JSONObject()
            .put("version", VERSION)
            .put("installation_id", owner)
            .put("acks", array)
            .toString()
    }

    /**
     * Parse only a snapshot owned by [currentOwner]. Missing, old, malformed,
     * and cross-installation snapshots all fail closed for the caller.
     */
    fun decode(raw: String, currentOwner: String, maxEntries: Int): List<DurableReplyAck> {
        val snapshot = JSONObject(raw)
        val owner = snapshot.opt("installation_id") as? String
        validateOwner(snapshot.getInt("version"), owner, currentOwner)
        val array = snapshot.getJSONArray("acks")
        if (array.length() > maxEntries) {
            throw IllegalArgumentException("pending reply acknowledgement state exceeds storage capacity")
        }
        val entries = ArrayList<DurableReplyAck>(array.length())
        for (index in 0 until array.length()) {
            val item = array.getJSONObject(index)
            val ack = ClientCommand.ReplyAck(
                laneId = item.getString("lane_id"),
                replyTo = item.getString("reply_to"),
                afterSeq = parseSafeSequence(item.opt("after_seq"))
                    ?: throw IllegalArgumentException("invalid receipt after_seq at index $index"),
                throughSeq = parseSafeSequence(item.opt("through_seq"))
                    ?: throw IllegalArgumentException("invalid receipt through_seq at index $index"),
                turnToken = item.getString("turn_token"),
                interactionId = item.getString("interaction_id"),
            )
            val replyText = item.getString("reply_text")
            if (!isReplyTextWithinLimit(replyText)) {
                throw IllegalArgumentException("reply text exceeds the UTF-8 byte limit at index $index")
            }
            val state = when (item.getString("state")) {
                "receipt_pending" -> ReplyAckPlaybackState.ReceiptPending
                "receipt_pending_suppressed" -> ReplyAckPlaybackState.ReceiptPendingSuppressed
                "awaiting_playback" -> ReplyAckPlaybackState.AwaitingPlayback
                "playback_suppressed" -> ReplyAckPlaybackState.PlaybackSuppressed
                "ready_to_acknowledge" -> ReplyAckPlaybackState.ReadyToAcknowledge
                "retirement_pending" -> ReplyAckPlaybackState.RetirementPending
                else -> throw IllegalArgumentException("invalid reply acknowledgement state at index $index")
            }
            if (!ReplyAckDurability.isValidStoredReceipt(ack) || entries.any { entry -> entry.ack == ack }) {
                throw IllegalArgumentException("invalid pending reply acknowledgement at index $index")
            }
            entries += DurableReplyAck(ack, replyText, state)
        }
        return entries
    }
}

/** Exact owner for one direct or recovered playback attempt. */
internal object ReplyPlaybackOwnership {
    fun tryStart(
        inFlight: MutableMap<ClientCommand.ReplyAck, Long>,
        ack: ClientCommand.ReplyAck,
        attemptId: Long,
    ): Boolean {
        if (inFlight.containsKey(ack)) return false
        inFlight[ack] = attemptId
        return true
    }

    fun finish(
        inFlight: MutableMap<ClientCommand.ReplyAck, Long>,
        ack: ClientCommand.ReplyAck,
        attemptId: Long,
    ): Boolean {
        if (inFlight[ack] != attemptId) return false
        inFlight.remove(ack)
        return true
    }
}

/**
 * The lease, generation, and cancellation check that must guard the actual
 * TTS enqueue. The service calls this while holding replyStateLock.
 */
internal object ReplyPlaybackStartGuard {
    fun canEnqueue(
        inFlight: Map<ClientCommand.ReplyAck, Long>,
        ack: ClientCommand.ReplyAck,
        attemptId: Long,
        attemptGeneration: Long,
        currentGeneration: Long,
        cancelled: Boolean,
    ): Boolean =
        !cancelled &&
            attemptGeneration == currentGeneration &&
            inFlight[ack] == attemptId

    fun canEnqueueLocal(
        turnToken: String?,
        currentTurnToken: String?,
        attemptGeneration: Long,
        currentGeneration: Long,
        cancelled: Boolean,
    ): Boolean =
        !cancelled &&
            turnToken != null &&
            turnToken == currentTurnToken &&
            attemptGeneration == currentGeneration
}

/** State transition for a current direct TTS failure. */
internal data class ReplyPlaybackTurnState(
    val turnToken: String?,
    val interactionId: String?,
    val endAccepted: Boolean,
    val cancelled: Boolean,
    val generation: Long,
)

internal object ReplyPlaybackFailure {
    /**
     * A current direct playback failure ends only the active turn. The durable
     * receipt remains AwaitingPlayback so the next pending-delivery pass can
     * authorize it after the matching text was actually heard.
     */
    fun invalidateCurrentTurn(
        state: ReplyPlaybackTurnState,
        callbackGeneration: Long,
    ): ReplyPlaybackTurnState {
        if (state.cancelled || state.generation != callbackGeneration || state.turnToken == null) {
            return state
        }
        return state.copy(
            turnToken = null,
            interactionId = null,
            endAccepted = false,
            cancelled = true,
            generation = state.generation + 1,
        )
    }
}

/** Capture gates for the two phases of pending-delivery recovery. */
internal object ReplyAckCaptureGate {
    /**
     * AwaitingPlayback is intentionally allowed through this first gate,
     * including at capacity: recovery does not reserve another receipt slot.
     */
    fun allowsPendingFetch(
        entries: Collection<DurableReplyAck>,
        maxEntries: Int,
        stateCorrupt: Boolean,
        persistenceFailed: Boolean,
    ): Boolean =
        !stateCorrupt && !persistenceFailed &&
            (entries.size < maxEntries ||
                entries.any { entry -> entry.state == ReplyAckPlaybackState.AwaitingPlayback })

    /** The mic may open only after any AwaitingPlayback receipt was promoted. */
    fun allowsMicAfterPendingFetch(
        entries: Collection<DurableReplyAck>,
        maxEntries: Int,
        stateCorrupt: Boolean,
        persistenceFailed: Boolean,
    ): Boolean =
        allowsPendingFetch(entries, maxEntries, stateCorrupt, persistenceFailed) &&
            entries.none { entry -> entry.state == ReplyAckPlaybackState.AwaitingPlayback }
}

/**
 * Foreground service: captures HFP mic audio (16 kHz PCM16 mono), streams it
 * over WebSocket, and plays TTS PCM coming back.
 *
 * Robustness notes (learned the hard way by everyone):
 * - The socket WILL die silently behind NAT/cellular. We reconnect with backoff,
 *   forever, until the service is stopped.
 * - Each (re)connect tears down any previous recorder/thread first.
 * - All inbound JSON is parsed defensively; malformed frames must never crash us.
 */
class AudioCaptureService : Service() {

    private val client by lazy {
        OkHttpClient.Builder()
            // 15s kept radios from settling; 30s still beats NAT idle timeouts comfortably
            .pingInterval(30, TimeUnit.SECONDS)
            .connectTimeout(10, TimeUnit.SECONDS)
            .build()
    }

    private val announcer by lazy { LocalAnnouncer(this) }
    private var mediaSession: MediaSession? = null
    private val audioManager by lazy { getSystemService(AUDIO_SERVICE) as AudioManager }
    @Volatile private var audioDeviceCallback: AudioDeviceCallback? = null

    private val socketStateLock = Any()
    @Volatile private var ws: WebSocket? = null
    @Volatile private var socketConfigUrl: String? = null
    @Volatile private var socketConfigToken: String? = null
    /** Advances whenever a listener can no longer publish the active connection. */
    @Volatile private var socketGeneration = 0L
    private val mic by lazy { MicController(this) }

    // phone-TTS mode: the server sends TEXT only; we speak it on-device.
    private val replyText = ReplyTextAccumulator()
    private val replyDeltaTracker = ReplyDeltaTracker()
    private val replyStateLock = Any()
    private var replyPlaybackCancelled = false
    private var replyPlaybackGeneration = 0L
    /**
     * The only client turn allowed to consume reply-bearing server frames.
     * These share [replyStateLock] with the text buffer so a new capture or a
     * Stop cannot interleave with a stale delta/end callback.
     */
    private var activeTurnToken: String? = null
    private var activeInteractionId: String? = null
    private var replyEndAccepted = false
    /**
     * A receipt is reserved before TTS begins. Failed TTS keeps it in
     * [ReplyAckPlaybackState.AwaitingPlayback] until its stored exact text is
     * replayed by the receipt-recovery path; only then is it sent to the bridge.
     * A user Stop moves an active receipt to a suppressed state: proof-pending
     * receipts retain their bridge handshake, while confirmed receipts retain
     * their text until a reconnect re-arms playback.
     */
    private val pendingReplyAcks = ArrayDeque<DurableReplyAck>()
    private val replyAckPrefs by lazy { getSharedPreferences("reply_ack_state", MODE_PRIVATE) }
    /** Hash of the configured URL/token pair that owns [pendingReplyAcks]. */
    private var replyAckStateIdentity: String? = null
    /** An invalid persisted endpoint has no receipt namespace; capture stays paused until corrected. */
    private var replyAckConfigurationInvalid = false
    /** A full or malformed durable queue must pause capture, never discard an ack. */
    private var replyAckStateCorrupt = false
    private var replyAckPersistenceFailed = false
    /** Exactly one direct or recovered TTS attempt may own each durable receipt. */
    private val replyPlaybackInFlight = mutableMapOf<ClientCommand.ReplyAck, Long>()
    /**
     * Ephemeral Stop fences for receipts still waiting for bridge proof, or
     * whose state transition could not be persisted. A reconnect clears this
     * fence only after the durable state has been re-armed.
     */
    private val replyPlaybackSuppressed = mutableSetOf<ClientCommand.ReplyAck>()
    /** Late receipt-bearing frames for the most recently superseded turns. */
    private val supersededTurnFences = LinkedHashSet<String>()
    private var nextReplyPlaybackAttemptId = 0L

    /**
     * Capture policy (capture-on-demand): the pinch sets this; `listening` clears it.
     * Between interactions the mic is CLOSED — zero radio, zero mic power.
     */
    @Volatile private var captureRequested = false

    @Volatile private var wantConnection = false
    private var reconnectAttempt = 0

    /** Set by double-pinch: next capture opens in meta mode. */
    private val metaCapture = MetaCaptureArm()

    /** Identifies the socket/fetch that currently owns pending narration. */
    private val preparation = PreparationGate()

    // ---- capture start choreography (SCO-first, cued) ----

    @Volatile private var lastPhase = "listening"
    @Volatile private var scoPending = false
    private val mainHandler = Handler(Looper.getMainLooper())
    private val teardownGuard = ServiceTeardownGuard()
    /** Retries a durable reply acknowledgement/retirement while this socket stays up. */
    private val replyAckRetryRunnable = Runnable { retryPendingReplyAcks() }
    private val stopScoRunnable = Runnable { stopSco() }
    private val tone by lazy {
        try { ToneGenerator(AudioManager.STREAM_VOICE_CALL, 80) } catch (_: Exception) { null }
    }

    /** Plays through the voice-call stream → lands in the earbuds once SCO is up. */
    private fun playCue(toneId: Int, ms: Int) {
        try { tone?.startTone(toneId, ms) } catch (_: Exception) {}
    }

    private val scoFallback = Runnable {
        if (scoPending && captureRequested && !mic.isOpen) {
            Log.w(TAG, "SCO didn't connect in 1.5s — opening phone mic instead")
            scoPending = false
            openMicNow()
        }
    }

    private val scoReceiver = object : BroadcastReceiver() {
        override fun onReceive(ctx: Context?, intent: Intent?) {
            val state = intent?.getIntExtra(
                AudioManager.EXTRA_SCO_AUDIO_STATE, AudioManager.SCO_AUDIO_STATE_DISCONNECTED)
            if (state == AudioManager.SCO_AUDIO_STATE_CONNECTED &&
                captureRequested && !mic.isOpen) {
                // only stand down the fallback once the mic actually opened;
                // if the socket isn't up yet, onOpen will re-run requestCaptureStart
                // fire-and-forget: completion (or failure) re-runs via onOpen/retry
                openMicNow()
                scoPending = false
                mainHandler.removeCallbacks(scoFallback)
            }
        }
    }

    override fun onCreate() {
        super.onCreate()
        val startupUrl = configuredWebSocketEndpoint("service startup")
        synchronized(replyStateLock) {
            if (startupUrl == null) {
                replyAckConfigurationInvalid = true
            } else {
                selectReplyAckStateLocked(startupUrl, serverToken())
            }
        }
        // RECEIVER_NOT_EXPORTED: system broadcast, other apps must not spoof it.
        // Required flag on API 34+, otherwise registration throws.
        registerReceiver(
            scoReceiver,
            IntentFilter(AudioManager.ACTION_SCO_AUDIO_STATE_UPDATED),
            Context.RECEIVER_NOT_EXPORTED
        )
        // Earbud (dis)connection tracking — M4
        val callback = object : AudioDeviceCallback() {
            override fun onAudioDevicesAdded(added: Array<out AudioDeviceInfo>) {
                teardownGuard.runIfActive { refreshBuds() }
            }
            override fun onAudioDevicesRemoved(removed: Array<out AudioDeviceInfo>) {
                teardownGuard.runIfActive { refreshBuds() }
            }
        }
        audioDeviceCallback = callback
        audioManager.registerAudioDeviceCallback(callback, null)
        refreshBuds()
        mainHandler.postDelayed(notifRefresh, 30_000)

        // Earbud taps arrive as AVRCP media keys (features.md M3).
        // Key→command mapping lives in ClientCommand.fromMediaKey (pure, testable).
        // Session is INACTIVE except mid-interaction (B2 fix): inactive sessions
        // leave media keys to whichever app is actually playing.
        mediaSession = MediaSession(this, "Telepathy").apply {
            setCallback(object : MediaSession.Callback() {
                override fun onMediaButtonEvent(mediaButtonIntent: Intent): Boolean {
                    val ev = mediaButtonIntent.getParcelableExtra<KeyEvent>(Intent.EXTRA_KEY_EVENT)
                    if (ev?.action == KeyEvent.ACTION_DOWN) {
                        ClientCommand.fromMediaKey(ev.keyCode, lastPhase)?.let(::sendCommand)
                    }
                    return true
                }
            })
            isActive = false
        }
    }

    private fun refreshBuds() {
        val buds = audioManager.getDevices(AudioManager.GET_DEVICES_OUTPUTS).any {
            it.type == AudioDeviceInfo.TYPE_BLUETOOTH_SCO ||
            it.type == AudioDeviceInfo.TYPE_BLUETOOTH_A2DP
        }
        LinkState.setBuds(buds)
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun sendCommand(action: ClientCommand.Action) {
        TriggerLog.record(this, "gesture → ${action.name.lowercase()}")
        val command = when (action) {
            ClientCommand.Action.Stop -> {
                // The server's stop command does not cancel phone-side TTS.
                // Do that locally, including pending-delivery narration.
                captureRequested = false
                preparation.invalidate()
                metaCapture.clear()
                val turnToken = supersedeCurrentTurnForUserAction()
                mic.close()
                finishInteraction()
                turnToken?.let { ClientCommand.Command(ClientCommand.Kind.Stop, it) }
            }
            ClientCommand.Action.Repeat -> {
                // Replay has no STT frame, so it must start a fresh local buffer.
                if (lastPhase != "listening" || mic.isOpen) return
                // Repeat starts a new reply stream, so it must first fence
                // any TTS it interrupts. The old durable receipt remains
                // recoverable only after a later reconnect/Ready boundary.
                supersedeCurrentTurnForUserAction()
                val turnToken = newTurnToken()
                beginTurn(turnToken, clearText = true)
                mediaSession?.isActive = true
                ClientCommand.Command(ClientCommand.Kind.Repeat, turnToken)
            }
            ClientCommand.Action.CancelCapture -> {
                // pinch-hold: drop the utterance AND the mic — next pinch reopens
                captureRequested = false
                preparation.invalidate()
                metaCapture.clear()
                val turnToken = supersedeCurrentTurnForUserAction()
                mic.close()
                finishInteraction()
                turnToken?.let {
                    ClientCommand.Command(ClientCommand.Kind.CancelCapture, it)
                }
            }
            ClientCommand.Action.FlushUtterance -> currentTurnToken()?.let {
                ClientCommand.FlushUtterance(it)
            }
        }
        if (command == null) return
        val socket = ws
        if (socket == null || !LinkState.current.wsUp || !socket.send(command.toJson())) {
            if (action == ClientCommand.Action.Repeat) {
                (command as? ClientCommand.Command)?.turnToken?.let(::abandonTurn)
            }
            Log.w(TAG, "could not send turn-bound ${action.name.lowercase()} command")
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(Foreground.notifyId(), Foreground.start(this, "pinch to talk"))
        // Validate before touching a current socket or receipt namespace. A bad
        // live preference value must not detach the old bridge or orphan its
        // durable acknowledgements.
        val desiredUrl = configuredWebSocketEndpoint("capture start") ?: return START_STICKY
        val desiredToken = serverToken()
        mainHandler.removeCallbacks(stopScoRunnable)
        // A new capture supersedes any reply/pending narration that is still
        // speaking. Clear its completion gate before starting a new prep so an
        // old TTS callback cannot consume the new prep's pending snapshot.
        if (!mic.isOpen && lastPhase == "listening") {
            preparation.invalidate()
            // A requested capture supersedes any completed reply that may
            // still have frames queued on this socket. Fence and durably
            // suppress it before stopping TTS, so its discarded callback
            // cannot strand the receipt or consume the old reply.
            supersedeCurrentTurnForUserAction()
        }
        var socket = ws
        val configuredUrl = socketConfigUrl
        if (socket != null &&
            (configuredUrl == null || !equivalentWebSocketEndpoint(configuredUrl, desiredUrl) || socketConfigToken != desiredToken)) {
            // Settings are saved before this start command arrives. Do not
            // reuse an already-open cleartext socket after switching to a
            // token/WSS configuration (or vice versa).
            preparation.invalidate()
            announcer.stop()
            cancelCurrentTurn(clearText = true)
            captureRequested = false
            mic.close()
            // The old socket may still be closing asynchronously, so its
            // listener cannot be relied on to clear this connection bit.
            // Do it before the new capture request is evaluated; otherwise
            // the first token-authenticated capture can open against a socket
            // that has not completed its new hello handshake.
            detachSocketIfCurrent(socket)
            resetAfterDisconnect()
            socket.close(1000, "configuration changed")
            socket = null
        }
        // Detach the old socket before changing acknowledgement scope: an
        // in-flight callback from the old bridge must never persist a receipt
        // into the new bridge's namespace.
        synchronized(replyStateLock) { selectReplyAckStateLocked(desiredUrl, desiredToken) }
        val metaNow = metaCapture.setForStart(
            intent?.getBooleanExtra(EXTRA_META, false) == true,
            mic.isOpen && socket != null,
        )
        if (metaNow && socket != null && LinkState.current.wsUp) {
            val turnToken = currentTurnToken()
            if (turnToken == null || !socket.send(ClientCommand.MetaMode(turnToken).toJson())) {
                Log.w(TAG, "could not arm meta mode for the active capture")
            }
        }
        // Every pinch lands here (idempotent while running). It means "I want to talk":
        if (!wantConnection || ws == null) {
            wantConnection = true
            connect()
        }
        requestCaptureStart()
        return START_STICKY
    }

    /**
     * The ONLY place capture begins. Choreography:
     * buds present → raise SCO first (300-800ms), mic + "go" cue when it's up;
     * no buds (rehearsal) or SCO failure → phone mic immediately.
     * The user hears the cue EXACTLY when the real mic is live. No clipping, ever.
     */
    private fun requestCaptureStart() {
        if (lastPhase != "listening") return
        val replyAckBlocker = synchronized(replyStateLock) { replyAckPreparationBlockerLocked() }
        if (replyAckBlocker != null) {
            TriggerLog.record(this, replyAckBlocker)
            announcer.say("Waiting for earlier reply acknowledgements.")
            return
        }
        captureRequested = true
        if (!wantConnection || !LinkState.current.wsUp) return // mic opens on next onOpen
        if (LinkState.current.budsOn && !mic.isOpen) {
            scoPending = true
            audioManager.startBluetoothSco()
            audioManager.setBluetoothScoOn(true)
            mainHandler.removeCallbacks(scoFallback)
            mainHandler.postDelayed(scoFallback, 1500)
        } else if (!mic.isOpen) {
            openMicNow()
        }
    }

    /** @return true iff the mic actually opened (false: no socket / init failure). */
    private fun openMicNow() {
        val socket = ws ?: return
        if (!captureRequested || mic.isOpen || !LinkState.current.wsUp) return
        val generation = preparation.begin(socket) ?: return
        val pendingContext = capturePendingConsumeContext(socket)
        // Before opening the floor: fetch undelivered lane items (cron results,
        // async replies), SPEAK them while the mic is still closed — our own voice
        // must never trigger VAD — then cue and open.
        if (!metaCapture.isArmed()) {
            Thread {
                val batch = runCatching { fetchPendingItems(pendingContext) }
                    .onFailure { Log.w(TAG, "pending worker: ${it.message}") }
                    .getOrDefault(PendingBatch.empty(pendingContext))
                mainHandler.post {
                    speakPendingThenOpen(socket, generation, batch)
                }
            }.start()
        } else {
            // Meta skips pending narration, but it still needs the same lane
            // snapshot as a normal capture so its interaction stats are exact.
            Thread {
                val batch = runCatching { fetchPendingItems(pendingContext) }
                    .onFailure { Log.w(TAG, "meta lane worker: ${it.message}") }
                    .getOrDefault(PendingBatch.empty(pendingContext))
                // Meta entry: templated lane status + the timestamped inbox —
                // "which lane am I in" and "what did I miss" answered at entry.
                // Consumed after being heard (receipt-aware per-row ack).
                val inbox = runCatching { fetchPendingInbox(pendingContext) }
                    .onFailure { Log.w(TAG, "meta inbox worker: ${it.message}") }
                    .getOrDefault(PendingInbox.empty())
                val status = runCatching { fetchLaneStatus(pendingContext) }
                    .onFailure { Log.w(TAG, "meta status worker: ${it.message}") }
                    .getOrNull()
                mainHandler.post {
                    if (!preparation.isCurrent(socket, generation) ||
                        !captureRequested || mic.isOpen || ws !== socket) {
                        preparation.finish(socket, generation)
                        return@post
                    }
                    val spoken = buildString {
                        append(status ?: "Meta.")
                        if (inbox.items.isNotEmpty()) {
                            append(" ")
                            append(if (inbox.items.size == 1) "One update." else "${inbox.items.size} updates.")
                            append(" ")
                            append(inbox.items.joinToString(" … ") { it.spoken() })
                        }
                    }
                    announcer.speakReply(
                        spoken,
                        onDone = {
                            mainHandler.post {
                                Thread {
                                    inbox.consume(pendingContext)
                                    mainHandler.post {
                                        openAfterLaneValidation(socket, generation, batch, consumeBatch = false)
                                    }
                                }.start()
                            }
                        },
                    )
                }
            }.start()
        }
    }

    private fun speakPendingThenOpen(
        socket: WebSocket,
        generation: Long,
        fetched: PendingBatch,
    ) {
        if (!preparation.isCurrent(socket, generation) ||
            !captureRequested || mic.isOpen || ws !== socket) {
            preparation.finish(socket, generation)
            return
        }
        if (LaneStore.isConfigured(this) && fetched.laneId.isNullOrBlank()) {
            captureRequested = false
            preparation.finish(socket, generation)
            announcer.say("Lane state unavailable.")
            finishInteraction()
            return
        }
        // The endpoint returns reply_to, so receipt-owned correlated rows are
        // never silently treated as generic pending narration.  Take this
        // snapshot under the same lock that receives agent_end/ack frames.
        val batch = synchronized(replyStateLock) {
            fetched.copy(items = PendingPlaybackOwnership.spokenItems(
                entries = pendingReplyAcks,
                laneId = fetched.laneId,
                items = fetched.items,
            ))
        }
        if (batch.items.isEmpty()) {
            openAfterLaneValidation(socket, generation, batch, consumeBatch = true)
            return
        }

        speakPendingChunk(socket, generation, batch, pendingNarration(batch.items), 0)
    }

    /** Speak every pending character in bounded TTS utterances before acking. */
    private fun speakPendingChunk(
        socket: WebSocket,
        generation: Long,
        batch: PendingBatch,
        narration: List<String>,
        index: Int,
    ) {
        if (!preparation.isCurrent(socket, generation) ||
            !captureRequested || mic.isOpen || ws !== socket) {
            preparation.finish(socket, generation)
            return
        }

        announcer.speakReply(
            narration[index],
            onDone = {
                mainHandler.post {
                    if (!preparation.isCurrent(socket, generation) ||
                        !captureRequested || mic.isOpen || ws !== socket) {
                        preparation.finish(socket, generation)
                        return@post
                    }
                    if (index + 1 < narration.size) {
                        speakPendingChunk(socket, generation, batch, narration, index + 1)
                        return@post
                    }
                    // Revalidate the lane after TTS. A lane switch during
                    // narration must not open the old lane or consume its
                    // queue under the new selection.
                    openAfterLaneValidation(socket, generation, batch, consumeBatch = true)
                }
            },
            onFailure = {
                // The text was not heard; leave the durable snapshot queued.
                mainHandler.post {
                    if (!preparation.isCurrent(socket, generation) ||
                        !captureRequested || mic.isOpen || ws !== socket) {
                        preparation.finish(socket, generation)
                        return@post
                    }
                    openAfterLaneValidation(socket, generation, batch, consumeBatch = false)
                }
            },
        )
    }

    /**
     * Re-read authoritative lane state immediately before opening the mic.
     * Preparation can include arbitrarily long phone TTS, so the first pending
     * snapshot is not safe to use as the final lane selection. If the lane or
     * registry revision changed, keep the old snapshot queued and open the
     * current lane instead.
     */
    private fun openAfterLaneValidation(
        socket: WebSocket,
        generation: Long,
        original: PendingBatch,
        consumeBatch: Boolean,
    ) {
        Thread {
            val configured = LaneStore.isConfigured(this)
            val current = if (configured) {
                runCatching { fetchPendingItems(original.context) }
                    .onFailure { Log.w(TAG, "lane validation worker: ${it.message}") }
                    .getOrNull()
            } else {
                original
            }
            mainHandler.post {
                if (!preparation.isCurrent(socket, generation) ||
                    !captureRequested || mic.isOpen || ws !== socket) {
                    preparation.finish(socket, generation)
                    return@post
                }
                // Do not open a capture or acknowledge spoken pending items
                // using a snapshot from a replaced server/socket. The fetch
                // itself is pinned to the captured endpoint, and this rejects
                // a configuration change before any effect is applied.
                if (!isCurrentPendingConsumeContext(original.context)) {
                    preparation.finish(socket, generation)
                    return@post
                }
                if (configured && (current == null || current.laneId.isNullOrBlank() ||
                    current.revision == null)) {
                    captureRequested = false
                    preparation.finish(socket, generation)
                    announcer.say("Lane state unavailable.")
                    finishInteraction()
                    return@post
                }

                val selected = current ?: original
                val changed = configured && (
                    selected.laneId != original.laneId ||
                        selected.revision != original.revision
                    )
                if (consumeBatch && !changed) {
                    // Only the exact snapshot rows that were spoken may be
                    // removed. Receipt-owned correlated rows were omitted
                    // above and remain for their receipt-specific bridge ack.
                    if (original.items.isNotEmpty()) {
                        Thread {
                            runCatching { consumePending(original) }
                                .onFailure { Log.w(TAG, "pending consume worker: ${it.message}") }
                        }.start()
                    }
                }
                // Recovery narration may have durably promoted a failed-TTS
                // receipt, but the normal 64-slot cap still prohibits a new
                // microphone turn until the bridge confirms enough acks.
                val replyAckBlocker = synchronized(replyStateLock) { replyAckBlockerLocked() }
                if (replyAckBlocker != null) {
                    captureRequested = false
                    preparation.finish(socket, generation)
                    announcer.say("Waiting for earlier reply acknowledgements.")
                    finishInteraction()
                    return@post
                }
                preparation.finish(socket, generation)
                // A standalone bridge has the same built-in direct lane as
                // telepathyd, but no lane API to query. It still receives a
                // token-bound lane frame; unbound audio is never permitted.
                actuallyOpenMic(
                    socket,
                    selected.laneId ?: DEFAULT_LANE_ID,
                    selected.revision,
                )
            }
        }.start()
    }

    private fun pendingNarration(items: List<PendingItem>): List<String> {
        val out = mutableListOf(
            "While you were away, " +
                (if (items.size == 1) "one update." else "${items.size} updates."),
        )
        for (item in items) out += PendingNarrationChunker.chunk(item.content.trim())
        return out
    }

    private fun actuallyOpenMic(
        socket: WebSocket,
        laneId: String? = null,
        laneRevision: Long? = null,
    ): Boolean {
        if (!captureRequested || mic.isOpen || ws !== socket) return false
        if (laneId == null || !isValidLaneId(laneId)) {
            // Protocol v2 has no unbound capture mode: without a lane frame
            // the bridge will discard all audio, so do not power the mic.
            captureRequested = false
            announcer.say("Lane state unavailable.")
            finishInteraction()
            return false
        }
        // Consume the one-shot mode before attempting the open. A failed mic
        // start must not leak a prior meta request into a later normal capture.
        val meta = metaCapture.take()
        val turnToken = newTurnToken()
        beginTurn(turnToken)
        // Freeze the lane before any audio can arrive. OkHttp preserves WebSocket
        // message order, and the microphone is not opened until this enqueue
        // succeeds, so every binary frame belongs to this exact capture token.
        if (!socket.send(ClientCommand.LaneSnapshot(laneId, turnToken, laneRevision).toJson())) {
            abandonTurn(turnToken)
            captureRequested = false
            Log.w(TAG, "could not send token-bound lane snapshot")
            return false
        }
        if (meta && !socket.send(ClientCommand.MetaMode(turnToken).toJson())) {
            abandonTurn(turnToken)
            captureRequested = false
            Log.w(TAG, "could not send token-bound meta mode")
            return false
        }
        // open mic first (user may start talking immediately after the cue),
        // then speak any pre-speech note through the same earbuds
        val opened = mic.open { chunk ->
            if (canSendCaptureAudio(socket, turnToken)) {
                socket.send(chunk.toByteString())
            }
        }
        if (!opened) {
            socket.send(ClientCommand.Command(ClientCommand.Kind.CancelCapture, turnToken).toJson())
            abandonTurn(turnToken)
            captureRequested = false
            announcer.say("Microphone unavailable.")
            return false
        }
        mediaSession?.isActive = true
        if (meta) {
            playCue(ToneGenerator.TONE_PROP_BEEP2, 90)
            mainHandler.postDelayed({ playCue(ToneGenerator.TONE_PROP_BEEP2, 90) }, 180)
            updateNotification("meta agent — state your command")
        } else {
            playCue(ToneGenerator.TONE_PROP_BEEP, 120)   // "go ahead"
            updateNotification()
        }
        return true
    }

    private data class PendingBatch(
        val laneId: String?,
        /** Exactly the rows this batch may narrate and later acknowledge. */
        val items: List<PendingItem>,
        val revision: Long?,
        val context: PendingConsumeContext,
    ) {
        companion object {
            fun empty(context: PendingConsumeContext) =
                PendingBatch(null, emptyList(), null, context)
        }
    }

    /** Capture the endpoint, credentials, and socket that own one pending batch. */
    private fun capturePendingConsumeContext(socket: WebSocket) = PendingConsumeContext(
        apiBaseUrl = LaneStore.baseUrl(this),
        token = serverToken(),
        configuredSocketUrl = serverUrl(),
        socketIdentity = socket,
        socketUrl = socketConfigUrl,
        socketToken = socketConfigToken,
    )

    private fun isCurrentPendingConsumeContext(context: PendingConsumeContext): Boolean =
        PendingConsumeGuard.isCurrent(
            captured = context,
            currentApiBaseUrl = LaneStore.baseUrl(this),
            currentToken = serverToken(),
            currentSocketUrl = serverUrl(),
            currentSocket = ws,
            currentSocketConfigUrl = socketConfigUrl,
            currentSocketConfigToken = socketConfigToken,
        )

    /**
     * Templated meta-entry status — deterministic, never LLM-generated:
     * "Meta. On lane kerchunk — cache bug. 2 pending updates."
     */
    private fun fetchLaneStatus(context: PendingConsumeContext): String? {
        val base = context.apiBaseUrl ?: return null
        return try {
            val request = Request.Builder().url("$base/api/state").apply {
                context.token?.let { header("x-telepathy-token", it) }
            }.build()
            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                response.close()
                return null
            }
            val body = BoundedHttpResponse.readJsonObject(response, HttpResponseLimits.LANE_STATE_BYTES)
                ?: return null
            val name = body.optString("active")
            var title = ""
            body.optJSONArray("lanes")?.let { arr ->
                val activeId = body.optString("active_id")
                for (i in 0 until arr.length()) {
                    val l = arr.optJSONObject(i) ?: continue
                    if (l.optString("id") == activeId) title = l.optString("title")
                }
            }
            var pending = 0
            val pReq = Request.Builder().url("$base/api/pending").apply {
                context.token?.let { header("x-telepathy-token", it) }
            }.build()
            val pRes = client.newCall(pReq).execute()
            pRes.use { pr ->
                if (pr.isSuccessful) {
                    val po = BoundedHttpResponse.readJsonObject(pr, HttpResponseLimits.PENDING_BYTES)
                    pending = po?.optInt("count", 0) ?: 0
                }
            }
            val t = if (title.isNotBlank()) " — $title" else ""
            val pTxt = when (pending) {
                0 -> "No pending updates."
                1 -> "1 pending update."
                else -> "$pending pending updates."
            }
            "Meta. On lane $name$t. $pTxt"
        } catch (e: Exception) {
            Log.w(TAG, "meta status: ${e.message}")
            null
        }
    }

    /** Fetch pending items for the active lane (oldest first). */
    private fun fetchPendingItems(context: PendingConsumeContext): PendingBatch {
        val hermes = context.apiBaseUrl ?: return PendingBatch.empty(context)
        return try {
            val request = Request.Builder().url("$hermes/api/pending").apply {
                context.token?.let { header("x-telepathy-token", it) }
            }.build()
            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                response.close()
                return PendingBatch.empty(context)
            }
            val body = BoundedHttpResponse.readJsonObject(response, HttpResponseLimits.PENDING_BYTES)
                ?: return PendingBatch.empty(context)
            val laneId = body.optString("lane_id").takeIf(::isValidLaneId)
            val hasRevision = body.has("revision")
            val revision = if (hasRevision) parseSafeSequence(body.opt("revision")) else null
            if (hasRevision && revision == null) {
                // A configured telepathyd endpoint must provide an exact
                // JSON-safe revision. Do not let optLong round an unsafe
                // value into a lane snapshot that the bridge cannot
                // validate consistently.
                return PendingBatch.empty(context)
            }
            val arr = body.optJSONArray("items")
                ?: return PendingBatch(laneId, emptyList(), revision, context)
            val records = ArrayList<PendingItemRecord>(arr.length())
            for (i in 0 until arr.length()) {
                val item = arr.optJSONObject(i) ?: return PendingBatch.empty(context)
                records += PendingItemRecord(
                    sequence = item.opt("seq"),
                    content = item.opt("content"),
                    replyTo = item.opt("reply_to"),
                )
            }
            val parsed = PendingItemsParser.parse(records)
                ?: return PendingBatch.empty(context)
            PendingBatch(laneId, parsed.items, revision, context)
        } catch (e: Exception) {
            Log.w(TAG, "pending fetch: ${e.message}")
            PendingBatch.empty(context)
        }
    }

    /** Acknowledge that a lane's pending items have been spoken. */
    private fun consumePending(batch: PendingBatch) {
        val context = batch.context
        val hermes = context.apiBaseUrl ?: return
        if (batch.laneId == null || !isValidLaneId(batch.laneId) || batch.items.isEmpty()) return
        if (!isCurrentPendingConsumeContext(context)) {
            Log.i(TAG, "pending consume cancelled after endpoint or socket changed")
            return
        }
        try {
            val body = org.json.JSONObject()
                .put("lane_id", batch.laneId)
                .put("sequences", JSONArray().apply {
                    batch.items.forEach { item -> put(item.sequence) }
                })
                .toString()
                .toRequestBody("application/json".toMediaTypeOrNull())
            val request = Request.Builder().url("$hermes/api/pending/consume").apply {
                context.token?.let { header("x-telepathy-token", it) }
            }.post(body).build()
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) Log.w(TAG, "pending consume: HTTP ${response.code}")
            }
        } catch (_: Exception) {}
    }

    override fun onDestroy() {
        teardownGuard.beginTeardown()
        wantConnection = false
        captureRequested = false
        preparation.invalidate()
        metaCapture.clear()
        cancelCurrentTurn(clearText = true)
        mainHandler.removeCallbacks(scoFallback)
        mainHandler.removeCallbacks(replyAckRetryRunnable)
        mainHandler.removeCallbacks(stopScoRunnable)
        mainHandler.removeCallbacks(notifRefresh)
        clearInteractionAudioState()
        stopSco()
        unregisterAudioDeviceCallback()
        unregisterReceiver(scoReceiver)
        mic.close()
        detachSocket()
            ?.close(1000, "bye")
        mediaSession?.release()
        mediaSession = null
        try { tone?.release() } catch (_: Exception) {}
        announcer.shutdown()
        super.onDestroy()
    }

    /** Keep the persistent notification truthful (M4) and lane-aware. */
    private fun updateNotification(text: String? = null) {
        if (text != null) {
            teardownGuard.runIfActive { Foreground.update(this, text) }
            return
        }
        Thread {
            val st = LaneStore.fetchState(this)
            mainHandler.post {
                teardownGuard.runIfActive {
                    val lane = st?.first?.find { it.active }
                    val pendingTotal = st?.first?.sumOf { it.pending } ?: 0
                    val line = buildString {
                        append(LinkState.current.summary)
                        if ((st?.first?.size ?: 0) > 0 && pendingTotal > 0) append(" · $pendingTotal pending")
                    }
                    Foreground.update(this, line, lane?.name ?: st?.second, pendingTotal)
                }
            }
        }.start()
    }

    // light periodic refresh while the service lives: keeps lane/pending honest
    private val notifRefresh = object : Runnable {
        override fun run() {
            if (!teardownGuard.isActive()) return
            if (wantConnection) updateNotification()
            teardownGuard.runIfActive { mainHandler.postDelayed(this, 30_000) }
        }
    }

    private fun unregisterAudioDeviceCallback() {
        val callback = audioDeviceCallback ?: return
        audioDeviceCallback = null
        try {
            audioManager.unregisterAudioDeviceCallback(callback)
        } catch (_: Exception) {
            // Teardown must continue even if the platform rejects a duplicate removal.
        }
    }

    // ---- connection ----

    private fun serverUrl(): String =
        getSharedPreferences("cfg", MODE_PRIVATE).getString("server", null)
            ?: "wss://192.168.1.10:8787"

    /**
     * Reject bad persisted configuration before it reaches durable ownership.
     * Callers intentionally leave the current socket and reply-ack state alone
     * when this returns null.
     */
    private fun configuredWebSocketEndpoint(stage: String): String? =
        when (val validation = validateWebSocketEndpoint(serverUrl())) {
            is WebSocketEndpointValidation.Valid -> validation.canonicalUrl
            is WebSocketEndpointValidation.Invalid -> {
                Log.e(TAG, "$stage rejected configured WebSocket endpoint: ${validation.reason}")
                TriggerLog.record(this, "invalid server address ($stage)")
                announcer.say("Invalid server address.")
                null
            }
        }

    private fun serverToken(): String? =
        getSharedPreferences("cfg", MODE_PRIVATE).getString("token", null)
            ?.takeIf { it.isNotBlank() }

    /** Stable opaque owner for every hello; never derive this from the device label. */
    private val installationId: String by lazy { InstallationIdentity.getOrCreate(this) }

    private fun newTurnToken(): String = UUID.randomUUID().toString().also { token ->
        check(isValidTurnToken(token)) { "generated an invalid turn token" }
    }

    private fun currentTurnToken(): String? = synchronized(replyStateLock) { activeTurnToken }

    /** Start a new local turn and invalidate every reply frame from its predecessor. */
    private fun beginTurn(turnToken: String, clearText: Boolean = false): Long = synchronized(replyStateLock) {
        if (clearText) replyText.clear()
        replyDeltaTracker.reset()
        activeTurnToken = turnToken
        activeInteractionId = null
        replyEndAccepted = false
        replyPlaybackCancelled = false
        replyPlaybackGeneration += 1
        replyPlaybackGeneration
    }

    /** Cancel and detach the current turn atomically before sending a turn-bound command. */
    private fun cancelCurrentTurn(
        clearText: Boolean = false,
        suppressReplyPlayback: Boolean = false,
        recordSupersededTurnFence: Boolean = false,
    ): String? = synchronized(replyStateLock) {
        val turnToken = activeTurnToken
        if (clearText) replyText.clear()
        replyDeltaTracker.reset()
        if (recordSupersededTurnFence) {
            val updatedFences = SupersededTurnFence.record(
                existing = supersededTurnFences,
                supersededTurnToken = turnToken,
                maxEntries = MAX_SUPERSEDED_TURN_FENCES,
            )
            supersededTurnFences.clear()
            supersededTurnFences.addAll(updatedFences)
        }
        if (suppressReplyPlayback) suppressSupersededReplyPlaybackLocked(turnToken)
        replyPlaybackCancelled = true
        replyPlaybackGeneration += 1
        activeTurnToken = null
        activeInteractionId = null
        replyEndAccepted = false
        replyPlaybackInFlight.clear()
        turnToken
    }

    /**
     * Every user action that replaces a turn must fence its late agent_end,
     * suppress the matching durable receipt, and invalidate its lease before
     * TTS is stopped. Stop, CancelCapture, StartCapture, and Repeat share
     * exactly this path; disconnect and teardown deliberately do not.
     */
    private fun supersedeCurrentTurnForUserAction(): String? {
        val turnToken = cancelCurrentTurn(
            clearText = true,
            suppressReplyPlayback = true,
            recordSupersededTurnFence = true,
        )
        announcer.stop()
        return turnToken
    }

    /**
     * Fence only the receipts owned by the user-superseded turn or active TTS
     * leases. TTS failure does not call this path and therefore remains
     * AwaitingPlayback for ordinary recovery.
     */
    private fun suppressSupersededReplyPlaybackLocked(turnToken: String?) {
        val activeAcks = ReplyAckDurability.activeAcksForSupersession(
            entries = pendingReplyAcks,
            supersededTurnToken = turnToken,
            playbackLeases = replyPlaybackInFlight.keys,
        )
        if (activeAcks.isEmpty()) return
        replyPlaybackSuppressed.addAll(activeAcks)

        val previous = pendingReplyAcks.toList()
        val updated = ReplyAckDurability.suppressPlayback(previous, activeAcks)
        if (updated == previous) return
        replacePendingReplyAcksLocked(updated)
        if (!persistPendingReplyAcksLocked()) {
            // Keep the new state in memory. The next durable retry can write
            // exactly this suppression rather than silently reverting to a
            // replayable ReceiptPending/AwaitingPlayback state.
            Log.e(TAG, "could not persist user-superseded reply playback; retaining suppressed receipt for retry")
            scheduleReplyAckRetryIfNeeded()
        }
    }

    /** Drop a failed capture attempt without disturbing a newer turn. */
    private fun abandonTurn(turnToken: String) = synchronized(replyStateLock) {
        if (activeTurnToken != turnToken) return@synchronized
        replyPlaybackCancelled = true
        replyPlaybackGeneration += 1
        activeTurnToken = null
        activeInteractionId = null
        replyEndAccepted = false
        replyDeltaTracker.reset()
        replyPlaybackInFlight.clear()
    }

    /** A binary mic frame is legal only while its token-bound capture is current. */
    private fun canSendCaptureAudio(socket: WebSocket, turnToken: String): Boolean =
        captureRequested && ws === socket && LinkState.current.wsUp && synchronized(replyStateLock) {
            activeTurnToken == turnToken && activeInteractionId == null && !replyPlaybackCancelled
        }

    /** Bind the first valid reply frame and reject mismatched/stale reply streams. */
    private fun acceptsReplyFrame(
        turnToken: String,
        interactionId: String,
        sourceIdentity: String,
    ): Boolean {
        if (sourceIdentity != replyAckStateIdentity) return false
        if (activeTurnToken != turnToken || replyPlaybackCancelled || replyEndAccepted) return false
        return when (val expectedInteractionId = activeInteractionId) {
            null -> {
                activeInteractionId = interactionId
                true
            }
            interactionId -> true
            else -> false
        }
    }

    private fun acceptStt(msg: ServerMsg.Stt, sourceIdentity: String): Boolean = synchronized(replyStateLock) {
        if (!acceptsReplyFrame(msg.turnToken, msg.interactionId, sourceIdentity)) return@synchronized false
        replyText.clear()
        replyDeltaTracker.reset()
        true
    }

    private fun acceptAgentDelta(msg: ServerMsg.AgentDelta, sourceIdentity: String): Boolean = synchronized(replyStateLock) {
        if (!acceptsReplyFrame(msg.turnToken, msg.interactionId, sourceIdentity)) return@synchronized false
        replyDeltaTracker.accept(msg.text, replyText)
    }

    /** Abort a live turn when the peer violates a complete-reply invariant. */
    private fun rejectReplyStream(turnToken: String, reason: String) {
        val current = synchronized(replyStateLock) { activeTurnToken == turnToken }
        if (!current) return
        val socket = ws
        val cancelled = cancelCurrentTurn(clearText = true)
        announcer.stop()
        mic.close()
        finishInteraction()
        if (cancelled != null && socket != null && LinkState.current.wsUp) {
            socket.send(ClientCommand.Command(ClientCommand.Kind.Stop, cancelled).toJson())
        }
        Log.e(TAG, reason)
    }

    private fun rejectOversizedReply(turnToken: String) {
        rejectReplyStream(turnToken, "server reply exceeded the ${MAX_REPLY_TEXT_BYTES}-byte UTF-8 limit")
    }

    private data class AcceptedAgentEnd(
        val replyText: String,
        /** Non-null only when this end was accepted for the live local turn. */
        val turnToken: String?,
        /** Null means a replay recovered before any active local turn. */
        val playbackGeneration: Long?,
        /** True when a user action fenced this turn before its receipt was reserved. */
        val playbackSuppressed: Boolean = false,
    )

    /**
     * Accept a current turn's end, a supersession-fenced late end, or a durable bridge
     * replay while no turn is active. Replayed envelopes never borrow a newer
     * turn's reply buffer.
     */
    private fun replyForEnd(
        msg: ServerMsg.AgentEnd,
        sourceIdentity: String,
    ): AcceptedAgentEnd? = synchronized(replyStateLock) {
        if (sourceIdentity != replyAckStateIdentity) return@synchronized null
        if (!isReplyTextWithinLimit(msg.text)) return@synchronized null
        if (activeTurnToken == msg.turnToken && activeInteractionId == msg.interactionId &&
            !replyDeltaTracker.terminalTextMatches(replyText.text(), msg.text)) {
            Log.e(TAG, "agent_end text does not match the bounded delta stream")
            return@synchronized null
        }
        if (acceptsReplyFrame(msg.turnToken, msg.interactionId, sourceIdentity)) {
            replyEndAccepted = true
            return@synchronized AcceptedAgentEnd(msg.text, msg.turnToken, replyPlaybackGeneration)
        }
        if (msg.receipt != null && SupersededTurnFence.contains(supersededTurnFences, msg.turnToken)) {
            // A user action may have cleared the live turn, or a newer turn may already
            // be active, before this old receipt-bearing end arrives. Keep the
            // old text isolated and reserve its proof obligation silently.
            return@synchronized AcceptedAgentEnd(
                replyText = msg.text,
                turnToken = null,
                playbackGeneration = null,
                playbackSuppressed = true,
            )
        }
        if (msg.receipt != null && activeTurnToken == null && activeInteractionId == null) {
            // A lost agent_end is replayed before a fresh capture starts. Its
            // own text is the only safe source; the old stream was cleared on
            // reconnect and must never be reconstructed from a new turn.
            return@synchronized AcceptedAgentEnd(msg.text, null, null)
        }
        null
    }

    private fun replyAckFor(receipt: ServerMsg.DeliveryReceipt) = ClientCommand.ReplyAck(
        laneId = receipt.laneId,
        replyTo = receipt.replyTo,
        afterSeq = receipt.afterSeq,
        throughSeq = receipt.throughSeq,
        turnToken = receipt.turnToken,
        interactionId = receipt.interactionId,
    )

    private fun deliveryReceiptFor(ack: ClientCommand.ReplyAck) = ServerMsg.DeliveryReceipt(
        laneId = ack.laneId,
        replyTo = ack.replyTo,
        afterSeq = ack.afterSeq,
        throughSeq = ack.throughSeq,
        turnToken = ack.turnToken,
        interactionId = ack.interactionId,
    )

    private fun replyReceivedFor(receipt: ServerMsg.DeliveryReceipt) = ClientCommand.ReplyReceived(
        laneId = receipt.laneId,
        replyTo = receipt.replyTo,
        afterSeq = receipt.afterSeq,
        throughSeq = receipt.throughSeq,
        turnToken = receipt.turnToken,
        interactionId = receipt.interactionId,
    )

    private fun replyAckRetireFor(ack: ClientCommand.ReplyAck) = ReplyAckDurability.retirementCommand(ack)

    /**
     * Reserve the receipt before speaking. This makes a full acknowledgement
     * queue a capture blocker, rather than a reason to drop a reply that has
     * already been played.
     */
    private fun reserveReplyReceipt(
        receipt: ServerMsg.DeliveryReceipt?,
        reply: String,
        sourceIdentity: String,
        accepted: AcceptedAgentEnd,
    ): Boolean = synchronized(replyStateLock) {
        if (receipt == null) return@synchronized true
        if (sourceIdentity != replyAckStateIdentity) return@synchronized false
        val ack = replyAckFor(receipt)
        val reservationState = ReplyAckDurability.reservationState(
            acceptedTurnToken = accepted.turnToken,
            acceptedGeneration = accepted.playbackGeneration,
            currentTurnToken = activeTurnToken,
            currentGeneration = replyPlaybackGeneration,
            playbackCancelled = replyPlaybackCancelled,
            playbackSuppressed = accepted.playbackSuppressed || ack in replyPlaybackSuppressed,
        )
        val existing = pendingReplyAcks.firstOrNull { entry -> entry.ack == ack }
        if (existing != null) {
            if (existing.replyText != reply) {
                Log.e(TAG, "replayed reply receipt conflicts with its durable text")
                return@synchronized false
            }
            if (reservationState == ReplyAckPlaybackState.ReceiptPendingSuppressed &&
                (existing.state == ReplyAckPlaybackState.ReceiptPending ||
                    existing.state == ReplyAckPlaybackState.AwaitingPlayback)
            ) {
                replyPlaybackSuppressed.add(ack)
                val previous = pendingReplyAcks.toList()
                replacePendingReplyAcksLocked(
                    ReplyAckDurability.suppressPlayback(previous, setOf(ack)),
                )
                if (!persistPendingReplyAcksLocked()) {
                    replacePendingReplyAcksLocked(previous)
                    Log.e(TAG, "could not durably suppress a superseded reply acknowledgement")
                    scheduleReplyAckRetryIfNeeded()
                }
            }
            return@synchronized true
        }
        if (!ReplyAckDurability.canReserveReceipt(pendingReplyAcks.size, MAX_STORED_REPLY_ACKS)) {
            // A capture cannot start once the normal 64-slot cap is reached,
            // so this can only happen with corrupt/unexpected concurrent state.
            // Do not speak: the bridge still retains the unacknowledged reply.
            Log.e(TAG, "reply acknowledgement storage is full; retaining bridge delivery")
            TriggerLog.record(this, "reply acknowledgement storage is full; captures paused")
            return@synchronized false
        }
        val previous = pendingReplyAcks.toList()
        if (reservationState == ReplyAckPlaybackState.ReceiptPendingSuppressed) {
            // Keep an ephemeral fence as well as the durable state. If the
            // first persistence attempt fails, a bridge replay cannot turn
            // this stopped receipt back into ordinary playback in this run.
            replyPlaybackSuppressed.add(ack)
        }
        pendingReplyAcks.addLast(DurableReplyAck(ack, reply, reservationState))
        if (persistPendingReplyAcksLocked()) return@synchronized true
        replacePendingReplyAcksLocked(previous)
        Log.e(TAG, "could not durably reserve reply acknowledgement; retaining bridge delivery")
        false
    }

    /** Complete TTS and acknowledge only if Stop/Cancel did not supersede it. */
    private fun completeReplyPlayback(
        receipt: ServerMsg.DeliveryReceipt?,
        generation: Long,
        heard: Boolean,
    ): Boolean = synchronized(replyStateLock) {
        completeReplyPlaybackLocked(receipt, generation, heard)
    }

    /** [replyStateLock] is held so a playback lease cannot be released early. */
    private fun completeReplyPlaybackLocked(
        receipt: ServerMsg.DeliveryReceipt?,
        generation: Long,
        heard: Boolean,
    ): Boolean {
        if (replyPlaybackCancelled || generation != replyPlaybackGeneration) return false
        if (!heard) {
            invalidateCurrentTurnAfterPlaybackFailureLocked(generation)
            return true
        }
        if (receipt == null) return true
        val ack = replyAckFor(receipt)
        val previous = pendingReplyAcks.toList()
        val existing = previous.firstOrNull { entry -> entry.ack == ack }
        if (existing == null) {
            Log.e(TAG, "missing reserved reply acknowledgement; retaining bridge delivery")
            return false
        }
        if (existing.state == ReplyAckPlaybackState.RetirementPending) {
            // A late/double TTS callback must never reverse the terminal
            // retirement phase back into a delivery acknowledgement.
            val socket = ws
            if (socket != null && LinkState.current.wsUp) socket.send(replyAckRetireFor(ack).toJson())
            return true
        }
        if (existing.state == ReplyAckPlaybackState.ReadyToAcknowledge) {
            // Duplicate terminal TTS callbacks are harmless: the durable
            // acknowledgement was already marked ready, so resend it.
            val socket = ws
            if (socket != null && LinkState.current.wsUp) socket.send(ack.toJson())
            return true
        }
        val updated = ReplyAckDurability.markPlaybackHeard(previous, ack)
            ?: return false
        replacePendingReplyAcksLocked(updated)
        if (!persistPendingReplyAcksLocked()) {
            replacePendingReplyAcksLocked(previous)
            Log.e(TAG, "could not durably mark reply acknowledgement ready; pausing new captures")
            return false
        }
        val socket = ws
        if (socket != null && LinkState.current.wsUp) socket.send(ack.toJson())
        return true
    }

    private fun invalidateCurrentTurnAfterPlaybackFailureLocked(callbackGeneration: Long) {
        val updated = ReplyPlaybackFailure.invalidateCurrentTurn(
            state = ReplyPlaybackTurnState(
                turnToken = activeTurnToken,
                interactionId = activeInteractionId,
                endAccepted = replyEndAccepted,
                cancelled = replyPlaybackCancelled,
                generation = replyPlaybackGeneration,
            ),
            callbackGeneration = callbackGeneration,
        )
        activeTurnToken = updated.turnToken
        activeInteractionId = updated.interactionId
        replyEndAccepted = updated.endAccepted
        replyPlaybackCancelled = updated.cancelled
        replyPlaybackGeneration = updated.generation
    }

    /**
     * The bridge sends this only after its `prepared -> received` snapshot is
     * durable. Until then pending narration is blocked: consuming the same
     * telepathyd rows would otherwise orphan the prepared bridge binding.
     */
    private fun beginConfirmedReplyPlayback(
        receipt: ServerMsg.DeliveryReceipt,
        sourceIdentity: String,
    ) {
        val attempt = synchronized(replyStateLock) {
            if (sourceIdentity != replyAckStateIdentity) return@synchronized null
            val ack = replyAckFor(receipt)
            val previous = pendingReplyAcks.toList()
            val existing = previous.firstOrNull { entry -> entry.ack == ack } ?: return@synchronized null
            val updated = ReplyAckDurability.confirmReceipt(
                entries = previous,
                ack = ack,
                playbackSuppressed = ack in replyPlaybackSuppressed,
            ) ?: return@synchronized null
            val nextState = updated.first { entry -> entry.ack == ack }.state
            replacePendingReplyAcksLocked(updated)
            if (!persistPendingReplyAcksLocked()) {
                replacePendingReplyAcksLocked(previous)
                Log.e(TAG, "could not persist bridge receipt confirmation; retaining replay proof for retry")
                scheduleReplyAckRetryIfNeeded()
                return@synchronized null
            }
            if (nextState == ReplyAckPlaybackState.PlaybackSuppressed) {
                // Keep the proof durable, but do not start a user-cancelled
                // TTS attempt.
                scheduleReplyAckRetryIfNeeded()
                return@synchronized null
            }
            if (replyPlaybackCancelled) return@synchronized null
            val socket = ws ?: return@synchronized null
            val attemptId = ++nextReplyPlaybackAttemptId
            check(ReplyPlaybackOwnership.tryStart(
                inFlight = replyPlaybackInFlight,
                ack = ack,
                attemptId = attemptId,
            )) { "reply playback was already in flight" }
            ReplyPlaybackAttempt(existing, socket, replyPlaybackGeneration, attemptId)
        } ?: run {
            // A cancelled/direct attempt can still receive the bridge's
            // prepared -> received confirmation. The durable state now owns
            // either AwaitingPlayback or a Stop suppression; leave recovery
            // to the normal retry/reconnect boundary.
            scheduleReplyAckRetryIfNeeded()
            return
        }

        if (attempt.entry.replyText.isNotEmpty()) {
            enqueueReplyPlayback(attempt)
        } else {
            finishReplyPlaybackAttempt(attempt, heard = true)
        }
    }

    private fun isCurrentReplyAckSendContextLocked(context: ReplyAckSendContext): Boolean =
        ReplyAckSendGuard.isCurrent(
            captured = context,
            currentReplyAckIdentity = replyAckStateIdentity,
            currentServerUrl = serverUrl(),
            currentToken = serverToken(),
            currentSocket = ws,
            currentSocketConfigUrl = socketConfigUrl,
            currentSocketConfigToken = socketConfigToken,
        )

    private fun isCurrentReplyAckSendContext(context: ReplyAckSendContext): Boolean =
        synchronized(replyStateLock) { isCurrentReplyAckSendContextLocked(context) }

    private fun sendPendingReplyAck(
        socket: WebSocket,
        context: ReplyAckSendContext,
    ) {
        if (context.socketIdentity !== socket) return
        val pending = synchronized(replyStateLock) {
            if (!isCurrentReplyAckSendContextLocked(context)) return
            if (replyAckStateCorrupt) return
            if (replyAckPersistenceFailed && !persistPendingReplyAcksLocked()) return
            pendingReplyAcks.mapNotNull(ReplyAckDurability::retryCommand)
        }
        for (command in pending) {
            if (!isCurrentReplyAckSendContext(context) || !socket.send(command.toJson())) return
        }
    }

    /** One lease shared by direct delivery and saved-receipt recovery. */
    private data class ReplyPlaybackAttempt(
        val entry: DurableReplyAck,
        val socket: WebSocket,
        val generation: Long,
        val attemptId: Long,
    )

    /**
     * Revalidate and enqueue under one reply-state critical section. Stop also
     * fences the lease under this lock, so it cannot stop TTS and then be
     * overtaken by an old path that has not enqueued yet.
     */
    private fun enqueueReplyPlayback(attempt: ReplyPlaybackAttempt) {
        synchronized(replyStateLock) {
            if (!ReplyPlaybackStartGuard.canEnqueue(
                    inFlight = replyPlaybackInFlight,
                    ack = attempt.entry.ack,
                    attemptId = attempt.attemptId,
                    attemptGeneration = attempt.generation,
                    currentGeneration = replyPlaybackGeneration,
                    cancelled = replyPlaybackCancelled,
                ) || ws !== attempt.socket || !LinkState.current.wsUp
            ) {
                ReplyPlaybackOwnership.finish(
                    inFlight = replyPlaybackInFlight,
                    ack = attempt.entry.ack,
                    attemptId = attempt.attemptId,
                )
                return@synchronized
            }
            announcer.speakReply(
                attempt.entry.replyText,
                onDone = { finishReplyPlaybackAttempt(attempt, heard = true) },
                onFailure = { finishReplyPlaybackAttempt(attempt, heard = false) },
            )
        }
    }

    /**
     * Resume one locally durable AwaitingPlayback envelope after this socket's
     * hello/ready barrier. This path is independent of the active lane and of
     * /api/pending, so a process death or lane switch cannot strand the text.
     */
    private fun resumeAwaitingReplyPlayback() {
        val socket = ws ?: return
        if (!LinkState.current.wsUp || mic.isOpen) return
        val attempt = synchronized(replyStateLock) {
            if (replyAckStateCorrupt || replyAckPersistenceFailed || activeTurnToken != null) {
                return@synchronized null
            }
            val entry = ReplyAckDurability.awaitingPlaybackRecovery(
                entries = pendingReplyAcks.toList(),
                inFlight = replyPlaybackInFlight.keys,
                suppressed = replyPlaybackSuppressed,
            ).firstOrNull() ?: return@synchronized null
            val attemptId = ++nextReplyPlaybackAttemptId
            check(ReplyPlaybackOwnership.tryStart(
                inFlight = replyPlaybackInFlight,
                ack = entry.ack,
                attemptId = attemptId,
            )) { "reply playback was already in flight" }
            // A previous socket loss or local capture cancellation invalidates
            // the old callback. Starting recovery establishes a fresh attempt.
            replyPlaybackCancelled = false
            replyPlaybackGeneration += 1
            ReplyPlaybackAttempt(entry, socket, replyPlaybackGeneration, attemptId)
        } ?: return

        if (attempt.entry.replyText.isBlank()) {
            finishReplyPlaybackAttempt(attempt, heard = true)
        } else {
            enqueueReplyPlayback(attempt)
        }
    }

    private fun finishReplyPlaybackAttempt(
        attempt: ReplyPlaybackAttempt,
        heard: Boolean,
    ) {
        val completed = synchronized(replyStateLock) {
            // A stale callback cannot touch receipt state or release a newer
            // lease for the same reply.  Keep the lease held through the
            // durable state transition so retry cannot start another TTS in
            // the gap between completion and ReadyToAcknowledge.
            if (replyPlaybackInFlight[attempt.entry.ack] != attempt.attemptId) {
                return@synchronized null
            }
            val socketIsCurrent = ws === attempt.socket && LinkState.current.wsUp
            val result = if (socketIsCurrent) {
                completeReplyPlaybackLocked(
                    receipt = deliveryReceiptFor(attempt.entry.ack),
                    generation = attempt.generation,
                    heard = heard,
                )
            } else {
                false
            }
            check(ReplyPlaybackOwnership.finish(
                inFlight = replyPlaybackInFlight,
                ack = attempt.entry.ack,
                attemptId = attempt.attemptId,
            )) { "current reply playback lease disappeared" }
            result
        }
        if (completed == null) return
        if (completed) {
            finishInteraction()
            if (heard) resumeAwaitingReplyPlayback()
        }
        scheduleReplyAckRetryIfNeeded()
    }

    /** Re-arm user-superseded receipts only after a deliberate reconnect. */
    private fun rearmSuppressedPlaybackLocked(): Boolean {
        if (replyAckStateCorrupt) return false
        if (replyAckPersistenceFailed && !persistPendingReplyAcksLocked()) return false

        val previous = pendingReplyAcks.toList()
        val updated = ReplyAckDurability.resumeSuppressedPlayback(previous)
        if (updated != previous) {
            replacePendingReplyAcksLocked(updated)
            if (!persistPendingReplyAcksLocked()) {
                replacePendingReplyAcksLocked(previous)
                return false
            }
        }
        replyPlaybackSuppressed.clear()
        supersededTurnFences.clear()
        return true
    }

    /**
     * Send the current durable receipt phase only after the hello gate opens.
     * A ready frame is also an intentional reconnect boundary: only there may
     * a user-suppressed receipt be re-armed for local recovery.
     */
    private fun sendDurableReplyState(rearmSuppressedPlayback: Boolean = false) {
        val socket = ws ?: return
        val url = socketConfigUrl ?: return
        val identity = synchronized(replyStateLock) { replyAckStateIdentity } ?: return
        if (!LinkState.current.wsUp) return
        sendPendingReplyAck(
            socket,
            ReplyAckSendContext(
                serverUrl = url,
                token = socketConfigToken,
                identity = identity,
                socketIdentity = socket,
            ),
        )
        if (rearmSuppressedPlayback) {
            synchronized(replyStateLock) {
                rearmSuppressedPlaybackLocked()
            }
        }
        resumeAwaitingReplyPlayback()
        scheduleReplyAckRetryIfNeeded()
    }

    /** A live connection retries durable terminal frames even when it never reconnects. */
    private fun retryPendingReplyAcks() {
        val socket = ws ?: return
        val url = socketConfigUrl ?: return
        val identity = synchronized(replyStateLock) { replyAckStateIdentity } ?: return
        sendPendingReplyAck(
            socket,
            ReplyAckSendContext(
                serverUrl = url,
                token = socketConfigToken,
                identity = identity,
                socketIdentity = socket,
            ),
        )
        resumeAwaitingReplyPlayback()
        scheduleReplyAckRetryIfNeeded()
    }

    private fun scheduleReplyAckRetryIfNeeded() {
        val retryNeeded = synchronized(replyStateLock) {
            replyAckPersistenceFailed ||
                pendingReplyAcks.any { entry ->
                        entry.state == ReplyAckPlaybackState.ReceiptPending ||
                        entry.state == ReplyAckPlaybackState.ReceiptPendingSuppressed ||
                        entry.state == ReplyAckPlaybackState.AwaitingPlayback ||
                        entry.state == ReplyAckPlaybackState.ReadyToAcknowledge ||
                        entry.state == ReplyAckPlaybackState.RetirementPending
                }
        }
        mainHandler.removeCallbacks(replyAckRetryRunnable)
        if (retryNeeded && ws != null && LinkState.current.wsUp) {
            mainHandler.postDelayed(replyAckRetryRunnable, REPLY_ACK_RETRY_MS)
        }
    }

    /**
     * The bridge persisted sent->consumed before emitting reply_acknowledged.
     * Persist retirement_pending before replying so an Android crash/reconnect
     * always resumes with reply_ack_retire rather than losing the terminal
     * handoff.
     */
    private fun beginReplyAckRetirement(
        receipt: ServerMsg.DeliveryReceipt,
        sourceIdentity: String,
    ) =
        synchronized(replyStateLock) {
            if (sourceIdentity != replyAckStateIdentity) return@synchronized
            val previous = pendingReplyAcks.toList()
            val ack = replyAckFor(receipt)
            val updated = ReplyAckDurability.beginRetirement(previous, ack) ?: return@synchronized
            replacePendingReplyAcksLocked(updated)
            if (!persistPendingReplyAcksLocked()) {
                replacePendingReplyAcksLocked(previous)
                Log.e(TAG, "could not persist reply acknowledgement retirement; retaining delivery ack for retry")
                scheduleReplyAckRetryIfNeeded()
                return@synchronized
            }
            val socket = ws
            if (socket != null && LinkState.current.wsUp) socket.send(replyAckRetireFor(ack).toJson())
            scheduleReplyAckRetryIfNeeded()
        }

    /** Only the bridge's terminal, durable reply_ack_retired frame may erase this receipt. */
    private fun removeRetiredReply(
        receipt: ServerMsg.DeliveryReceipt,
        sourceIdentity: String,
    ) =
        synchronized(replyStateLock) {
            if (sourceIdentity != replyAckStateIdentity) return@synchronized
            val previous = pendingReplyAcks.toList()
            val ack = replyAckFor(receipt)
            val updated = ReplyAckDurability.completeRetirement(previous, ack) ?: return@synchronized
            replacePendingReplyAcksLocked(updated)
            if (!persistPendingReplyAcksLocked()) {
                replacePendingReplyAcksLocked(previous)
                Log.e(TAG, "could not persist retired reply acknowledgement removal; retrying terminal retirement")
            }
            scheduleReplyAckRetryIfNeeded()
        }

    /** Switch to the isolated durable state for this exact URL/token pair. */
    private fun selectReplyAckStateLocked(url: String, token: String?) {
        val identity = ReplyAckDurability.serverIdentity(url, token)
        if (identity == replyAckStateIdentity) return
        pendingReplyAcks.clear()
        replyPlaybackInFlight.clear()
        replyPlaybackSuppressed.clear()
        supersededTurnFences.clear()
        replyAckConfigurationInvalid = false
        replyAckStateCorrupt = false
        replyAckPersistenceFailed = false
        replyAckStateIdentity = identity
        loadPendingReplyAcksLocked()
    }

    private fun loadPendingReplyAcksLocked() {
        val identity = checkNotNull(replyAckStateIdentity)
        val raw = replyAckPrefs.getString(replyAckStorageKey(identity), null)
        val hasOldReplyAckState = replyAckPrefs.all.keys.any { key ->
            key.startsWith(LEGACY_PENDING_REPLY_ACKS_KEY_PREFIX) ||
                key.startsWith(PREVIOUS_PENDING_REPLY_ACKS_KEY_PREFIX)
        }
        if (raw == null && hasOldReplyAckState) {
            // v3 did not retain the full replay envelope or the handset-proof
            // phase. Refuse it rather than silently dropping a receipt or
            // treating an old bridge state as compatible.
            replyAckStateCorrupt = true
            Log.e(TAG, "legacy reply acknowledgement state requires hard-cutover recovery; captures paused")
            return
        }
        raw ?: return
        try {
            pendingReplyAcks.addAll(
                ReplyAckSnapshot.decode(
                    raw = raw,
                    currentOwner = installationId,
                    maxEntries = MAX_STORED_REPLY_ACKS,
                ),
            )
        } catch (error: Exception) {
            // Never rewrite a snapshot we cannot validate: doing so could
            // erase an acknowledgement that still authorizes consumption.
            pendingReplyAcks.clear()
            replyAckStateCorrupt = true
            Log.e(TAG, "malformed pending reply acknowledgement state; captures paused", error)
        }
    }

    private fun replyAckBlockerLocked(): String? {
        if (replyAckConfigurationInvalid) return "server address is invalid; captures paused"
        if (replyAckStateCorrupt) return "reply acknowledgement state is corrupt; captures paused"
        if (replyAckPersistenceFailed) return "reply acknowledgement persistence failed; captures paused"
        if (!ReplyAckCaptureGate.allowsMicAfterPendingFetch(
                entries = pendingReplyAcks,
                maxEntries = MAX_PENDING_REPLY_ACKS,
                stateCorrupt = replyAckStateCorrupt,
                persistenceFailed = replyAckPersistenceFailed,
            )) {
            if (pendingReplyAcks.any { entry -> entry.state == ReplyAckPlaybackState.AwaitingPlayback }) {
                return "saved reply playback is still pending; captures paused"
            }
            return "reply acknowledgement capacity is full; captures paused"
        }
        return null
    }

    /**
     * A full queue normally blocks another turn. Awaiting receipts are the
     * exception: they need one pending-narration pass to become ready, after
     * which the ordinary capacity gate stops the mic before capture begins.
     */
    private fun replyAckPreparationBlockerLocked(): String? {
        if (replyAckConfigurationInvalid) return "server address is invalid; captures paused"
        if (replyAckStateCorrupt) return "reply acknowledgement state is corrupt; captures paused"
        if (replyAckPersistenceFailed) return "reply acknowledgement persistence failed; captures paused"
        if (!ReplyAckCaptureGate.allowsPendingFetch(
                entries = pendingReplyAcks,
                maxEntries = MAX_PENDING_REPLY_ACKS,
                stateCorrupt = replyAckStateCorrupt,
                persistenceFailed = replyAckPersistenceFailed,
            )) {
            return "reply acknowledgement capacity is full; captures paused"
        }
        return null
    }

    private fun replacePendingReplyAcksLocked(entries: List<DurableReplyAck>) {
        pendingReplyAcks.clear()
        pendingReplyAcks.addAll(entries)
    }

    private fun persistPendingReplyAcksLocked(): Boolean {
        if (replyAckStateCorrupt) return false
        val identity = checkNotNull(replyAckStateIdentity)
        val snapshot = ReplyAckSnapshot.encode(installationId, pendingReplyAcks)
        val persisted = replyAckPrefs.edit()
            .putString(replyAckStorageKey(identity), snapshot)
            .commit()
        replyAckPersistenceFailed = !persisted
        return persisted
    }

    private fun detachSocketIfCurrent(socket: WebSocket): Boolean = synchronized(socketStateLock) {
        if (ws !== socket) return@synchronized false
        socketGeneration += 1
        ws = null
        socketConfigUrl = null
        socketConfigToken = null
        // Publish the disconnected state while the generation is fenced so a
        // delayed old onOpen cannot turn it back on during a settings switch.
        LinkState.setWs(false)
        true
    }

    private fun detachSocket(): WebSocket? = synchronized(socketStateLock) {
        socketGeneration += 1
        val socket = ws
        ws = null
        socketConfigUrl = null
        socketConfigToken = null
        LinkState.setWs(false)
        socket
    }

    private fun beginSocketConnection(url: String, token: String?): Long = synchronized(socketStateLock) {
        socketGeneration += 1
        socketConfigUrl = url
        socketConfigToken = token
        socketGeneration
    }

    private fun installSocketIfCurrent(
        socket: WebSocket,
        url: String,
        token: String?,
        generation: Long,
    ): Boolean = synchronized(socketStateLock) {
        val configuredUrl = socketConfigUrl
        if (!wantConnection || socketGeneration != generation ||
            configuredUrl == null || !equivalentWebSocketEndpoint(configuredUrl, url) || socketConfigToken != token ||
            !equivalentWebSocketEndpoint(serverUrl(), url) || serverToken() != token || ws != null) {
            return@synchronized false
        }
        ws = socket
        true
    }

    private fun isCurrentSocketOpenContextLocked(context: SocketOpenContext): Boolean =
        SocketOpenGuard.isCurrent(
            captured = context,
            currentServerUrl = serverUrl(),
            currentToken = serverToken(),
            currentSocket = ws,
            currentSocketConfigUrl = socketConfigUrl,
            currentSocketConfigToken = socketConfigToken,
            currentGeneration = socketGeneration,
            wantsConnection = wantConnection,
        )

    private fun isCurrentSocketOpenContext(context: SocketOpenContext): Boolean =
        synchronized(socketStateLock) { isCurrentSocketOpenContextLocked(context) }

    /** Do not let a delayed listener change acknowledgement ownership after a settings switch. */
    private fun selectReplyAckStateIfCurrent(context: SocketOpenContext): Boolean =
        synchronized(socketStateLock) {
            if (!isCurrentSocketOpenContextLocked(context)) return@synchronized false
            synchronized(replyStateLock) {
                selectReplyAckStateLocked(context.serverUrl, context.token)
            }
            true
        }

    /**
     * Atomically revalidate a listener immediately before opening the traffic
     * gate. This is deliberately after `hello` was successfully queued.
     */
    private fun publishHelloReadyIfCurrent(
        context: SocketOpenContext,
        helloQueued: Boolean,
    ): Boolean =
        synchronized(socketStateLock) {
            if (!HelloReadinessGuard.canPublish(
                    helloQueued = helloQueued,
                    readyReceived = true,
                    contextIsCurrent = isCurrentSocketOpenContextLocked(context),
                )) return@synchronized false
            LinkState.setWs(true)
            true
        }

    private fun connect() {
        if (!wantConnection) return
        val url = configuredWebSocketEndpoint("connection") ?: run {
            wantConnection = false
            return
        }
        val token = serverToken()
        synchronized(replyStateLock) { selectReplyAckStateLocked(url, token) }
        if (token != null && !url.startsWith("wss://")) {
            Log.e(TAG, "token-bearing WebSocket requires wss://")
            TriggerLog.record(this, "secure WebSocket required")
            announcer.say("Secure connection required.")
            return
        }
        Log.i(TAG, "connecting (attempt $reconnectAttempt)…")
        val request = Request.Builder().url(url).build()
        val generation = beginSocketConnection(url, token)
        val socket = client.newWebSocket(request, SocketListener(url, token, generation))
        if (!installSocketIfCurrent(socket, url, token, generation)) {
            socket.close(1000, "superseded")
        }
    }

    private fun scheduleReconnect(reason: String) {
        if (!wantConnection) return
        preparation.invalidate()
        mic.close() // recorder is tied to a dead socket
        reconnectAttempt = Math.min(reconnectAttempt + 1, 6)
        val delayMs = (1000L shl (reconnectAttempt - 1)).coerceAtMost(30_000) // 1s..30s backoff
        Log.w(TAG, "disconnected ($reason); retry in ${delayMs}ms")
        TriggerLog.record(this, "reconnecting in ${delayMs / 1000}s ($reason)")
        Thread {
            try { Thread.sleep(delayMs) } catch (_: InterruptedException) {}
            if (wantConnection && ws == null) connect()
        }.also { it.isDaemon = true; it.start() }
    }

    private fun resetAfterDisconnect() {
        captureRequested = false
        lastPhase = "listening"
        LinkState.setPhase("listening")
    }

    private inner class SocketListener(
        private val connectionUrl: String,
        private val connectionToken: String?,
        private val connectionGeneration: Long,
    ) : WebSocketListener() {
        private val replyAckIdentity = ReplyAckDurability.serverIdentity(connectionUrl, connectionToken)
        @Volatile private var helloQueued = false

        override fun onOpen(webSocket: WebSocket, response: Response) {
            val context = SocketOpenContext(
                serverUrl = connectionUrl,
                token = connectionToken,
                socketIdentity = webSocket,
                generation = connectionGeneration,
            )
            if (!isCurrentSocketOpenContext(context)) {
                webSocket.close(1000, "superseded")
                return
            }
            if (!selectReplyAckStateIfCurrent(context)) {
                webSocket.close(1000, "superseded")
                return
            }
            reconnectAttempt = 0
            Log.i(TAG, "ws open")
            val hello = ClientHello(
                installationId = installationId,
                token = connectionToken,
            )
            if (!isCurrentSocketOpenContext(context)) {
                webSocket.close(1000, "superseded")
                return
            }
            if (!webSocket.send(hello.toJson())) {
                webSocket.close(1000, "hello enqueue failed")
                return
            }
            helloQueued = true
            if (!isCurrentSocketOpenContext(context)) {
                webSocket.close(1000, "superseded")
                return
            }
            // Wait for the bridge's explicit ready frame. It is queued after
            // recovered agent_end envelopes, so the handset can persist their
            // receipt state before a pending narration or capture starts.
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            val context = SocketOpenContext(
                serverUrl = connectionUrl,
                token = connectionToken,
                socketIdentity = webSocket,
                generation = connectionGeneration,
            )
            if (!isCurrentSocketOpenContext(context)) return
            handleControl(webSocket, context, helloQueued, text, replyAckIdentity)
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            val wasConnected = LinkState.current.wsUp
            if (!detachSocketIfCurrent(webSocket)) return
            announcer.stop()
            cancelCurrentTurn(clearText = true)
            resetAfterDisconnect()
            if (wasConnected) {
                // we HAD a link and lost it — say so out loud (M4: feedback lives in the ears)
                announcer.say("Connection lost. Reconnecting.")
            }
            finishInteraction()
            updateNotification()
            scheduleReconnect(t.message ?: "failure")
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            if (!detachSocketIfCurrent(webSocket)) return
            announcer.stop()
            cancelCurrentTurn(clearText = true)
            resetAfterDisconnect()
            finishInteraction()
            updateNotification()
            scheduleReconnect("closed $code")
        }
    }

    private fun handleControl(
        socket: WebSocket,
        socketContext: SocketOpenContext,
        helloQueued: Boolean,
        text: String,
        sourceIdentity: String,
    ) {
        if (ws !== socket) return
        if (synchronized(replyStateLock) { sourceIdentity != replyAckStateIdentity }) return
        val msg = ServerMsg.parse(text) ?: run {
            // Do not treat an invalid peer frame as a server event: it must
            // not be logged, narrated, acknowledged, or allowed to alter the
            // current turn. Closing makes the connection generation unusable
            // and lets the normal reconnect fence establish a fresh peer.
            if (isCurrentSocketOpenContext(socketContext)) {
                socket.close(1008, "invalid control frame")
            }
            return
        }
        when (msg) {
            // exhaustive over the union — a new ServerMsg variant fails compilation here
            is ServerMsg.Stt -> {
                if (!acceptStt(msg, sourceIdentity)) return
                mediaSession?.isActive = true  // taps matter from here until the reply ends

                // M5 echo-back with confidence awareness: flag uncertain transcriptions
                val conf = msg.confidence
                val prefix = when {
                    conf != null && conf < 0.6 -> "Not sure I got that — working on:"
                    else -> "Working on:"
                }
                TriggerLog.record(this, buildString {
                    append("heard: ${msg.text}")
                    if (conf != null) append(String.format(" [%.0f%%]", conf * 100))
                    if (msg.repo != null) append(" @${msg.repo}")
                })
                announcer.say("$prefix ${msg.text}")
            }
            is ServerMsg.AgentDelta -> {
                if (!acceptAgentDelta(msg, sourceIdentity)) {
                    val current = synchronized(replyStateLock) {
                        activeTurnToken == msg.turnToken && activeInteractionId == msg.interactionId
                    }
                    if (current) rejectOversizedReply(msg.turnToken)
                    return
                }
            }
            is ServerMsg.ReplyReceived -> beginConfirmedReplyPlayback(msg.receipt, sourceIdentity)
            is ServerMsg.ReplyAcknowledged -> beginReplyAckRetirement(msg.receipt, sourceIdentity)
            is ServerMsg.ReplyAckRetired -> removeRetiredReply(msg.receipt, sourceIdentity)
            is ServerMsg.Error -> {
                TriggerLog.record(this, "server error: ${msg.message}")
                announcer.say("Server error.")
                finishInteraction()
            }
            is ServerMsg.Phase -> {
                if (msg.value == "processing" && lastPhase == "capturing") {
                    playCue(ToneGenerator.TONE_CDMA_PIP, 80) // "heard you — thinking"
                    // The server no longer consumes audio after utterance_end;
                    // close the phone mic immediately instead of waiting for
                    // the eventual listening broadcast.
                    captureRequested = false
                    mic.close()
                }
                lastPhase = msg.value
                LinkState.setPhase(msg.value)
                TriggerLog.record(this, "· ${msg.value}")
            }
            ServerMsg.Ready -> {
                if (!publishHelloReadyIfCurrent(socketContext, helloQueued)) return
                TriggerLog.record(this, "server ready")
                sendDurableReplyState(rearmSuppressedPlayback = true)
                if (captureRequested) requestCaptureStart() else updateNotification()
            }
            ServerMsg.Listening -> {
                // mic closes NOW; SCO/session release waits until speech is done
                captureRequested = false
                mic.close()
                updateNotification()
            }
            is ServerMsg.AgentEnd -> {
                if (!isReplyTextWithinLimit(msg.text)) {
                    rejectOversizedReply(msg.turnToken)
                    return
                }
                val accepted = replyForEnd(msg, sourceIdentity) ?: run {
                    val current = synchronized(replyStateLock) {
                        activeTurnToken == msg.turnToken && activeInteractionId == msg.interactionId
                    }
                    if (current) rejectReplyStream(msg.turnToken, "server agent_end did not match its bounded delta stream")
                    return
                }
                // Persist the full receipt envelope before speech. The bridge
                // will replay it after a lost agent_end until it has durable
                // proof, so a later /api/pending consume cannot orphan it.
                if (!reserveReplyReceipt(msg.receipt, accepted.replyText, sourceIdentity, accepted)) {
                    finishInteraction()
                    return
                }
                if (msg.receipt != null) {
                    // Playback waits for the bridge's durable receipt proof.
                    // `sendDurableReplyState` is a no-op before hello becomes
                    // ready; the Ready handler retries it in-order.
                    sendDurableReplyState()
                    return
                }
                val generation = accepted.playbackGeneration ?: return
                if (accepted.replyText.isNotEmpty()) {
                    synchronized(replyStateLock) {
                        if (!ReplyPlaybackStartGuard.canEnqueueLocal(
                                turnToken = accepted.turnToken,
                                currentTurnToken = activeTurnToken,
                                attemptGeneration = generation,
                                currentGeneration = replyPlaybackGeneration,
                                cancelled = replyPlaybackCancelled,
                            )
                        ) return@synchronized
                        announcer.speakReply(
                            accepted.replyText,
                            onDone = {
                                if (completeReplyPlayback(msg.receipt, generation, heard = true)) finishInteraction()
                                scheduleReplyAckRetryIfNeeded()
                            },
                            onFailure = {
                                // Keep the durable receipt awaiting playback. Its
                                // receipt-recovery path retries the exact stored
                                // text and only then authorizes bridge consumption.
                                if (completeReplyPlayback(msg.receipt, generation, heard = false)) finishInteraction()
                                scheduleReplyAckRetryIfNeeded()
                            },
                        )
                    }
                } else {
                    if (completeReplyPlayback(msg.receipt, generation, heard = true)) finishInteraction()
                    scheduleReplyAckRetryIfNeeded()
                }
            }
        }
    }

    /**
     * Speech finished (or nothing to say): release the media session back to real
     * media apps and drop SCO call-routing after a short grace period.
     */
    private fun finishInteraction() {
        clearInteractionAudioState()
        mainHandler.removeCallbacks(stopScoRunnable)
        mainHandler.postDelayed(stopScoRunnable, 800)
    }

    private fun clearInteractionAudioState() {
        mediaSession?.isActive = false
        scoPending = false
        mainHandler.removeCallbacks(scoFallback)
    }

    private fun stopSco() {
        try {
            audioManager.stopBluetoothSco()
            audioManager.setBluetoothScoOn(false)
        } catch (_: Exception) {}
    }

    companion object {
        private const val TAG = "Telepathy"
        private const val DEFAULT_LANE_ID = "telepathy:direct"
        private const val MAX_PENDING_REPLY_ACKS = 64
        /** One active reply may finish after the 64-slot capture gate closes. */
        private const val MAX_STORED_REPLY_ACKS = MAX_PENDING_REPLY_ACKS + 1
        private const val REPLY_ACK_RETRY_MS = 1_000L
        private const val MAX_SUPERSEDED_TURN_FENCES = 32
        private const val PENDING_REPLY_ACKS_KEY_PREFIX = "pending.v4."
        private const val PREVIOUS_PENDING_REPLY_ACKS_KEY_PREFIX = "pending.v3."
        private const val LEGACY_PENDING_REPLY_ACKS_KEY_PREFIX = "pending.v2."

        private fun replyAckStorageKey(serverIdentity: String): String =
            "$PENDING_REPLY_ACKS_KEY_PREFIX$serverIdentity"

        const val EXTRA_META = "meta"
    }
}

/** One pending inbox item with its arrival time, for the meta-entry readout. */
internal data class PendingInboxItem(
    val sequence: Long,
    val content: String,
    val arrivedAtSec: Long,
) {
    fun spoken(): String {
        val age = (System.currentTimeMillis() / 1000) - arrivedAtSec
        val rel = when {
            age < 60 -> "just now"
            age < 3600 -> "${age / 60} minutes ago"
            age < 86400 -> "${age / 3600} hour" + (if (age / 3600 > 1) "s" else "") + " ago"
            else -> "${age / 86400} day" + (if (age / 86400 > 1) "s" else "") + " ago"
        }
        val clock = java.text.SimpleDateFormat("HH:mm", java.util.Locale.US)
            .format(java.util.Date(arrivedAtSec * 1000))
        return "$rel at $clock: ${content.take(160)}"
    }
}

internal data class PendingInbox(
    val laneId: String,
    val items: List<PendingInboxItem>,
) {
    companion object {
        fun empty() = PendingInbox("", emptyList())
    }

    /** Acknowledge exactly the spoken rows via the receipt-aware consume API. */
    fun consume(context: PendingConsumeContext) {
        val base = context.apiBaseUrl ?: return
        try {
            val body = org.json.JSONObject().apply {
                put("lane_id", laneId)
                put("sequences", org.json.JSONArray().apply { items.forEach { put(it.sequence) } })
            }
            val request = Request.Builder().url("$base/api/pending/consume")
                .post("{}".toRequestBody("application/json".toMediaTypeOrNull()))
                .apply { context.token?.let { header("x-telepathy-token", it) } }
                .build()
            OkHttpClient().newCall(request).execute().close()
        } catch (e: Exception) {
            Log.w("Telepathy", "inbox consume: ${e.message}")
        }
    }
}

/// Raw inbox fetch: seq + content + arrived_at per item (meta-entry readout).
private fun fetchPendingInbox(context: PendingConsumeContext): PendingInbox {
    val base = context.apiBaseUrl ?: return PendingInbox.empty()
    return try {
        val request = Request.Builder().url("$base/api/pending").apply {
            context.token?.let { header("x-telepathy-token", it) }
        }.build()
        val response = OkHttpClient().newCall(request).execute()
        response.use { r ->
            if (!r.isSuccessful) return PendingInbox.empty()
            val body = BoundedHttpResponse.readJsonObject(r, HttpResponseLimits.PENDING_BYTES)
                ?: return PendingInbox.empty()
            val laneId = body.optString("lane_id")
            val arr = body.optJSONArray("items") ?: return PendingInbox(laneId, emptyList())
            val items = (0 until arr.length()).mapNotNull { i ->
                val o = arr.optJSONObject(i) ?: return@mapNotNull null
                val content = o.optString("content").takeIf { it.isNotBlank() } ?: return@mapNotNull null
                PendingInboxItem(
                    sequence = parseSafeSequence(o.opt("seq")) ?: return@mapNotNull null,
                    content = content,
                    arrivedAtSec = parseSafeSequence(o.opt("arrived_at")) ?: 0L,
                )
            }
            PendingInbox(laneId, items)
        }
    } catch (e: Exception) {
        Log.w("Telepathy", "inbox fetch: ${e.message}")
        PendingInbox.empty()
    }
}
