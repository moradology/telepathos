package dev.telepathy

import android.view.KeyEvent
import org.json.JSONObject

/**
 * The wire protocol as discriminated unions (features.md README protocol section).
 *
 * ServerMsg covers everything the server can say to us;
 * ClientCommand covers everything we can say back beyond audio.
 *
 * The win: `when` over these is EXHAUSTIVE — adding a variant breaks compilation
 * in every unhandled place, instead of dropping frames at runtime.
 */

sealed interface ServerMsg {
    data class Stt(val text: String) : ServerMsg
    data class AgentDelta(val text: String) : ServerMsg
    data class Error(val message: String) : ServerMsg
    /** Interaction lifecycle broadcast from the server's state machine (docs/features.md). */
    data class Phase(val value: String) : ServerMsg

    // no payload → singletons
    data object Ready : ServerMsg
    data object Listening : ServerMsg
    data object AgentEnd : ServerMsg

    companion object {
        /** Defensive parse: malformed/unknown frames yield null, never an exception. */
        fun parse(raw: String): ServerMsg? = runCatching {
            val o = JSONObject(raw)
            when (o.optString("type")) {
                "stt" -> Stt(o.optString("text"))
                "agent_delta" -> AgentDelta(o.optString("text"))
                "error" -> Error(o.optString("message"))
                "phase" -> Phase(o.optString("value"))
                "ready" -> Ready
                "listening" -> Listening
                "agent_end" -> AgentEnd
                else -> null
            }
        }.getOrNull()
    }
}

sealed interface ClientCommand {
    data object Stop : ClientCommand          // interrupt agent / TTS mid-flight
    data object Repeat : ClientCommand         // replay last reply
    data object CancelCapture : ClientCommand  // drop utterance currently being spoken
    data object FlushUtterance : ClientCommand // "send now" — end capture early

    fun toJson(): String = when (this) {
        Stop -> """{"type":"command","command":"stop"}"""
        Repeat -> """{"type":"command","command":"repeat"}"""
        CancelCapture -> """{"type":"command","command":"cancel_capture"}"""
        FlushUtterance -> """{"type":"utterance_end"}"""
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
        fun fromMediaKey(keyCode: Int, phase: String): ClientCommand? = when (keyCode) {
            KeyEvent.KEYCODE_MEDIA_PLAY_PAUSE ->
                if (phase == "capturing") FlushUtterance else Stop
            KeyEvent.KEYCODE_MEDIA_NEXT ->
                if (phase == "capturing") CancelCapture else Stop
            KeyEvent.KEYCODE_MEDIA_PREVIOUS -> Repeat
            else -> null
        }
    }
}
