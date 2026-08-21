package dev.telepathy

import android.content.Context
import android.media.AudioAttributes
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import android.util.Log
import java.util.Locale

/**
 * On-device TTS. Two roles:
 * 1. Announcements that must work offline (M4): "connection lost", etc.
 * 2. Reply playback when the server doesn't send audio frames (phone-TTS mode):
 *    text is tiny over the wire, the Pixel's neural voice is excellent,
 *    and SCO routing is handled via USAGE_VOICE_COMMUNICATION attributes.
 */
class LocalAnnouncer(context: Context) {

    private var tts: TextToSpeech? = null
    private var ready = false
    private var replyDone: (() -> Unit)? = null

    init {
        tts = TextToSpeech(context.applicationContext) { status ->
            ready = status == TextToSpeech.SUCCESS
            if (ready) {
                tts?.let {
                    it.language = Locale.US
                    // voice-call usage → routed into the SCO link while it's up
                    it.setAudioAttributes(
                        AudioAttributes.Builder()
                            .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                            .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                            .build()
                    )
                    it.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
                        override fun onStart(id: String?) {}
                        override fun onDone(id: String?) {
                            if (id == REPLY_ID) replyDone?.let { f -> replyDone = null; f() }
                        }
                        override fun onError(id: String?) {
                            if (id == REPLY_ID) replyDone?.let { f -> replyDone = null; f() }
                        }
                    })
                }
                Log.i(TAG, "local announcer ready")
            } else {
                Log.w(TAG, "no local TTS engine available")
            }
        }
    }

    /** Queue a short announcement. Safe from any thread; never blocks interaction end. */
    fun say(text: String) {
        if (!ready) return
        tts?.speak(text, TextToSpeech.QUEUE_ADD, null, "telepathy-${System.nanoTime()}")
    }

    /**
     * Speak an agent reply; [onDone] fires when speech actually finishes
     * (or immediately if no TTS engine) — callers gate SCO teardown on this.
     */
    fun speakReply(text: String, onDone: () -> Unit) {
        if (!ready || text.isBlank()) { onDone(); return }
        replyDone = onDone
        tts?.speak(text, TextToSpeech.QUEUE_ADD, null, REPLY_ID)
    }

    fun shutdown() {
        try { tts?.stop(); tts?.shutdown() } catch (_: Exception) {}
        tts = null
    }

    companion object {
        private const val TAG = "Telepathy"
        private const val REPLY_ID = "telepathy-reply"
    }
}
