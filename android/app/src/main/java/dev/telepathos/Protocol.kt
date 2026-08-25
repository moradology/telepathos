package dev.telepathos

import android.view.KeyEvent
import java.math.BigDecimal
import java.math.BigInteger
import org.json.JSONObject

/** Must remain inside JavaScript's exact integer range on every v5 hop. */
internal const val MAX_SAFE_SEQUENCE = 9_007_199_254_740_991L
/** JavaScript and Kotlin both measure String length in UTF-16 code units. */
internal const val MAX_TURN_TOKEN_LENGTH = 128
/** Must stay identical to the Rust and Node lane-ID contract. */
internal const val MAX_LANE_ID_LENGTH = 128
/** Shared opaque correlation-ID bound, measured in UTF-16 units and UTF-8 bytes. */
internal const val MAX_OPAQUE_ID_LENGTH = 256
internal const val MAX_OPAQUE_ID_BYTES = 256

/**
 * A server control frame can carry one complete 512 KiB UTF-8 reply plus a
 * bounded JSON envelope (correlation IDs, receipt data, and message type).
 * JSON may encode one raw reply byte as a six-byte `\\u00XX` escape, so the
 * transport limit reserves that worst case instead of rejecting a valid reply
 * merely because its JSON spelling is longer than its decoded UTF-8 text.
 * This limit is checked before JSONObject sees the frame.
 */
internal const val MAX_INBOUND_CONTROL_FRAME_BYTES = MAX_REPLY_TEXT_BYTES * 6 + 64 * 1024
internal const val MAX_INBOUND_STT_TEXT_BYTES = MAX_REPLY_TEXT_BYTES
internal const val MAX_INBOUND_AGENT_DELTA_BYTES = MAX_REPLY_TEXT_BYTES
internal const val MAX_INBOUND_ERROR_MESSAGE_BYTES = 4 * 1024
internal const val MAX_INBOUND_PHASE_BYTES = 32
internal const val MAX_INBOUND_REPO_BYTES = 1024
private const val MAX_INBOUND_TYPE_BYTES = 32

/**
 * Counts UTF-8 bytes without materializing a second byte array. Lone UTF-16
 * surrogates are rejected so the transport limit is defined over code points,
 * not replacement-character behavior that varies between runtimes.
 */
internal fun isUtf8CodePointTextWithinLimit(value: String, maxBytes: Int): Boolean {
    require(maxBytes >= 0) { "maxBytes must be non-negative" }
    var bytes = 0
    var index = 0
    while (index < value.length) {
        val code = value[index].code
        val added = when {
            code <= 0x7f -> 1
            code <= 0x7ff -> 2
            code in 0xd800..0xdbff -> {
                if (index + 1 >= value.length || !value[index + 1].isLowSurrogate()) return false
                index += 1
                4
            }
            code in 0xdc00..0xdfff -> return false
            else -> 3
        }
        if (bytes > maxBytes - added) return false
        bytes += added
        index += 1
    }
    return true
}

internal fun isInboundControlFrameWithinLimit(raw: String): Boolean =
    isUtf8CodePointTextWithinLimit(raw, MAX_INBOUND_CONTROL_FRAME_BYTES)

internal fun isValidLaneId(value: String): Boolean {
    if (value.isEmpty() || value.length > MAX_LANE_ID_LENGTH ||
        value.toByteArray(Charsets.UTF_8).size > MAX_LANE_ID_LENGTH) return false
    fun asciiAlphaNumeric(char: Char): Boolean =
        char in 'a'..'z' || char in 'A'..'Z' || char in '0'..'9'
    if (!asciiAlphaNumeric(value.first()) || !asciiAlphaNumeric(value.last())) return false
    return value.all { asciiAlphaNumeric(it) || it == ':' || it == '_' || it == '-' }
}

/**
 * Canonical protocol blankness, independent of a runtime's `isBlank` behavior:
 * the blank code points are ASCII U+0009..U+000D and U+0020, Unicode
 * White_Space additions U+0085, U+00A0, U+1680, U+2000..U+200A,
 * U+2028..U+2029, U+202F, U+205F, U+3000, plus U+FEFF. A protocol value is
 * blank only when every code point is in that explicit set. This does not
 * trim or normalize the value; callers retain their existing control policy.
 */
internal fun isProtocolBlank(value: String): Boolean {
    if (value.isEmpty()) return false
    var index = 0
    while (index < value.length) {
        val codePoint = Character.codePointAt(value, index)
        val isBlankCodePoint = codePoint in 0x0009..0x000d ||
            codePoint == 0x0020 ||
            codePoint == 0x0085 ||
            codePoint == 0x00a0 ||
            codePoint == 0x1680 ||
            codePoint in 0x2000..0x200a ||
            codePoint in 0x2028..0x2029 ||
            codePoint == 0x202f ||
            codePoint == 0x205f ||
            codePoint == 0x3000 ||
            codePoint == 0xfeff
        if (!isBlankCodePoint) return false
        index += Character.charCount(codePoint)
    }
    return true
}

/** Opaque IDs are never trimmed or otherwise normalized. */
internal fun isValidOpaqueId(value: String): Boolean =
    value.isNotEmpty() &&
        value.length <= MAX_OPAQUE_ID_LENGTH &&
        value.toByteArray(Charsets.UTF_8).size in 1..MAX_OPAQUE_ID_BYTES &&
        !isProtocolBlank(value) &&
        value.isWellFormedUtf16() &&
        value.all { it.code !in 0x00..0x1f && it.code !in 0x7f..0x9f }

internal fun String.isWellFormedUtf16(): Boolean {
    var index = 0
    while (index < length) {
        val char = this[index]
        if (char.isHighSurrogate()) {
            if (index + 1 >= length || !this[index + 1].isLowSurrogate()) return false
            index += 2
        } else if (char.isLowSurrogate()) {
            return false
        } else {
            index += 1
        }
    }
    return true
}

internal fun isValidTurnToken(value: String): Boolean =
    value.isNotEmpty() &&
        value.length <= MAX_TURN_TOKEN_LENGTH &&
        !isProtocolBlank(value) &&
        value.isWellFormedUtf16()

internal fun parseTurnToken(value: Any?): String? =
    (value as? String)?.takeIf(::isValidTurnToken)

/**
 * JSON integer values must remain integers on the wire: `1.0` is a floating
 * representation even though it has an integral mathematical value.
 */
internal fun parseSafeSequence(value: Any?): Long? {
    val sequence = when (value) {
        is Byte -> value.toLong()
        is Short -> value.toLong()
        is Int -> value.toLong()
        is Long -> value
        is BigInteger -> value.takeIf {
            it.signum() >= 0 && it <= BigInteger.valueOf(MAX_SAFE_SEQUENCE)
        }?.toLong()
        is BigDecimal -> if (value.scale() == 0) {
            value.toBigIntegerExact().takeIf {
                it.signum() >= 0 && it <= BigInteger.valueOf(MAX_SAFE_SEQUENCE)
            }?.toLong()
        } else {
            null
        }
        else -> null
    } ?: return null
    return sequence.takeIf { it in 0L..MAX_SAFE_SEQUENCE }
}

/** The local transcription backend reports confidence as a probability. */
internal fun parseConfidence(value: Any?): Double? {
    val number = value as? Number ?: return null
    val confidence = number.toDouble()
    if (confidence.isNaN() || confidence.isInfinite()) return null
    return confidence.takeIf { it in 0.0..1.0 }
}

internal fun isValidLaneRevision(value: Long): Boolean = value in 0L..MAX_SAFE_SEQUENCE

/**
 * The wire protocol as discriminated unions (features.md README protocol section).
 *
 * ServerMsg covers everything the server can say to us;
 * ClientCommand covers everything we can say back beyond audio.
 *
 * The win: `when` over these is EXHAUSTIVE — adding a variant breaks compilation
 * in every unhandled place, instead of dropping frames at runtime.
 */

/**
 * The first frame on every socket. [deviceLabel] is informational only;
 * installation ownership is carried by the opaque persisted ID.
 */
internal data class ClientHello(
    val installationId: String,
    val deviceLabel: String = "opendots2-pixel9",
    val token: String? = null,
) {
    init {
        require(InstallationIdentity.isValid(installationId)) {
            "hello requires a valid installation_id"
        }
    }

    fun toJson(): String = buildString {
        append("{\"type\":\"hello\",\"device\":")
        append(jsonQuote(deviceLabel))
        append(",\"installation_id\":")
        append(jsonQuote(installationId))
        token?.let {
            append(",\"token\":")
            append(jsonQuote(it))
        }
        append('}')
    }

    private fun jsonQuote(value: String): String = buildString {
        append('"')
        value.forEach { char ->
            when (char) {
                '"' -> append("\\\"")
                '\\' -> append("\\\\")
                '\b' -> append("\\b")
                '\u000c' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                in '\u0000'..'\u001f' -> append("\\u%04x".format(char.code))
                else -> append(char)
            }
        }
        append('"')
    }
}

sealed interface ServerMsg {
    data class Stt(
        val text: String,
        val confidence: Double? = null,
        val repo: String? = null,
        val turnToken: String,
        val interactionId: String,
    ) : ServerMsg { init { require(isValidOpaqueId(interactionId)); require(isValidTurnToken(turnToken)) } }
    data class AgentDelta(
        val text: String,
        val turnToken: String,
        val interactionId: String,
    ) : ServerMsg { init { require(isValidOpaqueId(interactionId)); require(isValidTurnToken(turnToken)) } }
    data class Error(val message: String) : ServerMsg
    /** Interaction lifecycle broadcast from the server's state machine (docs/features.md). */
    data class Phase(val value: String) : ServerMsg
    /** Live delivery: spoken immediately, deferred if we're mid-interaction. */
    data class Incoming(val lane: String, val text: String) : ServerMsg
    data class DeliveryReceipt(
        val laneId: String,
        val replyTo: String,
        val afterSeq: Long,
        val throughSeq: Long,
        /** The exact reply turn this durable receipt belongs to. */
        val turnToken: String,
        val interactionId: String,
    ) {
        init {
            require(isValidLaneId(laneId)) { "invalid lane id" }
            require(isValidOpaqueId(replyTo)) { "invalid reply_to" }
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            require(isValidOpaqueId(interactionId)) { "invalid interaction_id" }
            require(afterSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq > afterSeq) {
                "invalid receipt sequence range"
            }
        }
    }

    // no payload → singletons
    data object Ready : ServerMsg
    data object Listening : ServerMsg
    data class AgentEnd(
        /** Complete replayable reply text; required even when empty. */
        val text: String,
        val turnToken: String,
        val interactionId: String,
        val receipt: DeliveryReceipt? = null,
    ) : ServerMsg {
        init {
            require(isValidOpaqueId(interactionId))
            require(isValidTurnToken(turnToken))
            require(receipt == null || (receipt.interactionId == interactionId && receipt.turnToken == turnToken))
        }
    }
    /** The bridge durably recorded Android's local replay receipt. */
    data class ReplyReceived(val receipt: DeliveryReceipt) : ServerMsg
    /** The bridge durably consumed [ClientCommand.ReplyAck] and awaits terminal retirement. */
    data class ReplyAcknowledged(val receipt: DeliveryReceipt) : ServerMsg
    /** The bridge durably retired a consumed receipt; Android may remove its local record. */
    data class ReplyAckRetired(val receipt: DeliveryReceipt) : ServerMsg

    companion object {
        private val serverPhases = setOf("listening", "capturing", "processing")

        private fun boundedText(value: Any?, maxBytes: Int, allowEmpty: Boolean = true): String? =
            (value as? String)?.takeIf {
                (allowEmpty || it.isNotEmpty()) && isUtf8CodePointTextWithinLimit(it, maxBytes)
            }

        private fun requiredBoundedText(
            o: JSONObject,
            key: String,
            maxBytes: Int,
            allowEmpty: Boolean = true,
        ): String? = boundedText(o.opt(key), maxBytes, allowEmpty)

        private fun optionalBoundedText(
            o: JSONObject,
            key: String,
            maxBytes: Int,
            allowEmpty: Boolean = true,
        ): String? {
            if (!o.has(key)) return null
            return boundedText(o.opt(key), maxBytes, allowEmpty)
        }

        private fun requiredTurnToken(o: JSONObject): String? =
            parseTurnToken(o.opt("turn_token"))

        private fun deliveryReceipt(
            o: JSONObject,
            turnToken: String,
            interactionId: String,
        ): DeliveryReceipt? {
            val laneId = (o.opt("lane_id") as? String)?.takeIf(::isValidLaneId) ?: return null
            val replyTo = (o.opt("reply_to") as? String)?.takeIf(::isValidOpaqueId) ?: return null
            val afterSeq = parseSafeSequence(o.opt("after_seq")) ?: return null
            val throughSeq = parseSafeSequence(o.opt("through_seq")) ?: return null
            if (throughSeq <= afterSeq) return null
            return DeliveryReceipt(laneId, replyTo, afterSeq, throughSeq, turnToken, interactionId)
        }

        private fun hasAnyDeliveryReceiptField(o: JSONObject): Boolean =
            listOf("lane_id", "reply_to", "after_seq", "through_seq").any(o::has)

        /** Defensive parse: malformed/unknown frames yield null, never an exception. */
        fun parse(raw: String): ServerMsg? {
            if (!isInboundControlFrameWithinLimit(raw)) return null
            return runCatching {
                val o = JSONObject(raw)
                when (requiredBoundedText(o, "type", MAX_INBOUND_TYPE_BYTES, allowEmpty = false)) {
                    "stt" -> {
                        val turnToken = requiredTurnToken(o) ?: return@runCatching null
                        val interactionId = (o.opt("interaction_id") as? String)?.takeIf(::isValidOpaqueId) ?: return@runCatching null
                        val text = requiredBoundedText(o, "text", MAX_INBOUND_STT_TEXT_BYTES) ?: return@runCatching null
                        val repo = optionalBoundedText(o, "repo", MAX_INBOUND_REPO_BYTES, allowEmpty = false)
                        if (o.has("repo") && (repo == null || isProtocolBlank(repo))) return@runCatching null
                        val confidence = if (o.has("confidence")) {
                            parseConfidence(o.opt("confidence")) ?: return@runCatching null
                        } else {
                            null
                        }
                        Stt(
                            text = text,
                            confidence = confidence,
                            repo = repo,
                            turnToken = turnToken,
                            interactionId = interactionId,
                        )
                    }
                    "agent_delta" -> {
                        val turnToken = requiredTurnToken(o) ?: return@runCatching null
                        val interactionId = (o.opt("interaction_id") as? String)?.takeIf(::isValidOpaqueId) ?: return@runCatching null
                        val text = requiredBoundedText(o, "text", MAX_INBOUND_AGENT_DELTA_BYTES) ?: return@runCatching null
                        AgentDelta(text, turnToken, interactionId)
                    }
                    "error" -> Error(
                        requiredBoundedText(o, "message", MAX_INBOUND_ERROR_MESSAGE_BYTES, allowEmpty = false)
                            ?: return@runCatching null,
                    )
                    "phase" -> {
                        val phase = requiredBoundedText(o, "value", MAX_INBOUND_PHASE_BYTES, allowEmpty = false)
                            ?.takeIf(serverPhases::contains)
                            ?: return@runCatching null
                        Phase(phase)
                    }
                    "incoming" -> {
                        val lane = requiredBoundedText(o, "lane", MAX_LANE_ID_LENGTH, allowEmpty = false)
                            ?.takeIf(::isValidLaneId) ?: return@runCatching null
                        val text = requiredBoundedText(o, "text", MAX_REPLY_TEXT_BYTES) ?: return@runCatching null
                        Incoming(lane, text)
                    }
                    "ready" -> Ready
                    "listening" -> Listening
                    "agent_end" -> {
                        val text = requiredBoundedText(o, "text", MAX_REPLY_TEXT_BYTES) ?: return@runCatching null
                        val turnToken = requiredTurnToken(o) ?: return@runCatching null
                        val interactionId = (o.opt("interaction_id") as? String)?.takeIf(::isValidOpaqueId) ?: return@runCatching null
                        val hasReceipt = hasAnyDeliveryReceiptField(o)
                        val receipt = deliveryReceipt(o, turnToken, interactionId)
                        if (hasReceipt && receipt == null) return@runCatching null
                        AgentEnd(
                            text = text,
                            turnToken = turnToken,
                            interactionId = interactionId,
                            receipt = receipt,
                        )
                    }
                    "reply_received" -> {
                        val turnToken = requiredTurnToken(o) ?: return@runCatching null
                        val interactionId = (o.opt("interaction_id") as? String)?.takeIf(::isValidOpaqueId) ?: return@runCatching null
                        val receipt = deliveryReceipt(o, turnToken, interactionId)
                            ?: return@runCatching null
                        ReplyReceived(receipt)
                    }
                    "reply_acknowledged" -> {
                        val turnToken = requiredTurnToken(o) ?: return@runCatching null
                        val interactionId = (o.opt("interaction_id") as? String)?.takeIf(::isValidOpaqueId) ?: return@runCatching null
                        val receipt = deliveryReceipt(o, turnToken, interactionId)
                            ?: return@runCatching null
                        ReplyAcknowledged(receipt)
                    }
                    "reply_ack_retired" -> {
                        val turnToken = requiredTurnToken(o) ?: return@runCatching null
                        val interactionId = (o.opt("interaction_id") as? String)?.takeIf(::isValidOpaqueId) ?: return@runCatching null
                        val receipt = deliveryReceipt(o, turnToken, interactionId)
                            ?: return@runCatching null
                        ReplyAckRetired(receipt)
                    }
                    else -> null
                }
            }.getOrNull()
        }
    }
}

sealed interface ClientCommand {
    /** Local media-key intent. The service supplies the active or fresh token. */
    enum class Action {
        Stop,
        Repeat,
        CancelCapture,
        FlushUtterance,
    }

    enum class Kind(val wireName: String) {
        Stop("stop"),
        Repeat("repeat"),
        CancelCapture("cancel_capture"),
    }

    /** A turn-bound media command. There is deliberately no untagged variant. */
    data class Command(val kind: Kind, val turnToken: String) : ClientCommand
    data class FlushUtterance(val turnToken: String) : ClientCommand // "send now" — end capture early
    /** Bind this exact capture token before any audio can be transmitted. */
    data class LaneSnapshot(
        val id: String,
        val turnToken: String,
        val revision: Long? = null,
    ) : ClientCommand {
        init {
            require(isValidLaneId(id)) { "invalid lane id" }
            require(revision == null || isValidLaneRevision(revision)) {
                "lane revision must be inside JavaScript's exact-integer range"
            }
        }
    }
    data class MetaMode(val turnToken: String) : ClientCommand
    data class ReplyAck(
        val laneId: String,
        val replyTo: String,
        val afterSeq: Long,
        val throughSeq: Long,
        val turnToken: String,
        val interactionId: String,
    ) : ClientCommand
    /** Proves Android durably stored the full reply envelope, before playback. */
    data class ReplyReceived(
        val laneId: String,
        val replyTo: String,
        val afterSeq: Long,
        val throughSeq: Long,
        val turnToken: String,
        val interactionId: String,
    ) : ClientCommand {
        init {
            require(isValidLaneId(laneId)) { "invalid lane id" }
            require(isValidOpaqueId(replyTo)) { "invalid reply_to" }
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            require(isValidOpaqueId(interactionId)) { "invalid interaction_id" }
            require(afterSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq > afterSeq) {
                "invalid receipt sequence range"
            }
        }
    }
    /**
     * Terminally retires a bridge receipt after [ReplyAck] was durably consumed.
     * This has the same immutable receipt identity as ReplyAck, but never
     * authorizes consumption itself.
     */
    data class ReplyAckRetire(
        val laneId: String,
        val replyTo: String,
        val afterSeq: Long,
        val throughSeq: Long,
        val turnToken: String,
        val interactionId: String,
    ) : ClientCommand {
        init {
            require(isValidLaneId(laneId)) { "invalid lane id" }
            require(isValidOpaqueId(replyTo)) { "invalid reply_to" }
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            require(isValidOpaqueId(interactionId)) { "invalid interaction_id" }
            require(afterSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq > afterSeq) {
                "invalid receipt sequence range"
            }
        }
    }

    fun toJson(): String = when (this) {
        is Command -> {
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            JSONObject()
                .put("type", "command")
                .put("command", kind.wireName)
                .put("turn_token", turnToken)
                .toString()
        }
        is FlushUtterance -> {
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            JSONObject()
                .put("type", "utterance_end")
                .put("turn_token", turnToken)
                .toString()
        }
        is LaneSnapshot -> {
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            JSONObject()
                .put("type", "lane")
                .put("id", id)
                .put("turn_token", turnToken)
                .apply { revision?.let { put("revision", it) } }
                .toString()
        }
        is MetaMode -> {
            require(isValidTurnToken(turnToken)) { "invalid turn token" }
            JSONObject()
                .put("type", "meta_mode")
                .put("turn_token", turnToken)
                .toString()
        }
        is ReplyAck -> {
            requireValidReceiptCommand(laneId, replyTo, afterSeq, throughSeq, turnToken, interactionId)
            JSONObject()
            .put("type", "reply_ack")
            .put("lane_id", laneId)
            .put("reply_to", replyTo)
            .put("after_seq", afterSeq)
            .put("through_seq", throughSeq)
            .put("turn_token", turnToken)
            .put("interaction_id", interactionId)
            .toString()
        }
        is ReplyReceived -> {
            requireValidReceiptCommand(laneId, replyTo, afterSeq, throughSeq, turnToken, interactionId)
            JSONObject()
            .put("type", "reply_received")
            .put("lane_id", laneId)
            .put("reply_to", replyTo)
            .put("after_seq", afterSeq)
            .put("through_seq", throughSeq)
            .put("turn_token", turnToken)
            .put("interaction_id", interactionId)
            .toString()
        }
        is ReplyAckRetire -> {
            requireValidReceiptCommand(laneId, replyTo, afterSeq, throughSeq, turnToken, interactionId)
            JSONObject()
            .put("type", "reply_ack_retire")
            .put("lane_id", laneId)
            .put("reply_to", replyTo)
            .put("after_seq", afterSeq)
            .put("through_seq", throughSeq)
            .put("turn_token", turnToken)
            .put("interaction_id", interactionId)
            .toString()
        }
    }

    private fun requireValidReceiptCommand(
        laneId: String,
        replyTo: String,
        afterSeq: Long,
        throughSeq: Long,
        turnToken: String,
        interactionId: String,
    ) {
        require(isValidLaneId(laneId)) { "invalid lane id" }
        require(isValidOpaqueId(replyTo)) { "invalid reply_to" }
        require(isValidTurnToken(turnToken)) { "invalid turn token" }
        require(isValidOpaqueId(interactionId)) { "invalid interaction_id" }
        require(afterSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq in 0L..MAX_SAFE_SEQUENCE && throughSeq > afterSeq) {
            "invalid receipt sequence range"
        }
    }

    companion object {
        /**
         * Pure mapping from AVRCP media key + current interaction phase to command.
         * Same tap means different things depending on where the interaction is —
         * which is exactly why it must be a total function over both inputs.
         *
         * capturing: tap = send now · 2×tap = drop utterance
         * otherwise: tap = stop agent · 3×tap = replay
         */
        fun fromMediaKey(keyCode: Int, phase: String): Action? = when (keyCode) {
            KeyEvent.KEYCODE_MEDIA_PLAY_PAUSE ->
                if (phase == "capturing") Action.FlushUtterance else Action.Stop
            KeyEvent.KEYCODE_MEDIA_NEXT ->
                if (phase == "capturing") Action.CancelCapture else Action.Stop
            KeyEvent.KEYCODE_MEDIA_PREVIOUS -> Action.Repeat
            else -> null
        }
    }
}
