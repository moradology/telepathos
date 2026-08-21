package dev.telepathy

import android.content.Context
import android.speech.tts.TextToSpeech
import android.util.Log
import java.util.Locale

/**
 * On-device TTS for announcements that must work when the server is unreachable
 * (features.md M4): "server unreachable", "reconnecting", etc.
 */
class LocalAnnouncer(context: Context) {

    private var tts: TextToSpeech? = null
    private var ready = false

    init {
        tts = TextToSpeech(context.applicationContext) { status ->
            ready = status == TextToSpeech.SUCCESS
            if (ready) {
                tts?.language = Locale.US
                // mono-safe by construction: TTS routes through the active call/audio path
                Log.i(TAG, "local announcer ready")
            } else {
                Log.w(TAG, "no local TTS engine available")
            }
        }
    }

    /** Queue a short spoken announcement. Safe to call from any thread. */
    fun say(text: String) {
        if (!ready) return
        tts?.speak(text, TextToSpeech.QUEUE_ADD, null, "telepathy-${System.nanoTime()}")
    }

    fun shutdown() {
        try { tts?.stop(); tts?.shutdown() } catch (_: Exception) {}
        tts = null
    }

    companion object { private const val TAG = "Telepathy" }
}
