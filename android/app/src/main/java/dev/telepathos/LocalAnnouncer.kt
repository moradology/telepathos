package dev.telepathos

import android.content.Context
import android.media.AudioAttributes
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import android.util.Log
import java.util.ArrayDeque
import java.util.HashMap
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

/**
 * On-device TTS. Two roles:
 * 1. Announcements that must work offline (M4): "connection lost", etc.
 * 2. Reply playback when the server doesn't send audio frames (phone-TTS mode):
 *    text is tiny over the wire, the Pixel's neural voice is excellent,
 *    and SCO routing is handled via USAGE_VOICE_COMMUNICATION attributes.
 */
class LocalAnnouncer(context: Context) {

    private val callbacks = LocalAnnouncerCallbackDispatcher()
    private val replyCallbacks = LocalAnnouncerReplyCallbacks()
    private val replySequence = AtomicLong()
    private var tts: TextToSpeech? = null
    private var ready = false
    private var shuttingDown = false
    private var pendingInitStatus: Int? = null

    init {
        // TextToSpeech is allowed to invoke the initialization callback before
        // its constructor has returned. Keep that status until the instance is
        // published under the same monitor used by every other operation.
        val created = TextToSpeech(context.applicationContext) { status ->
            onInitialized(status)
        }
        val statusToApply: Int?
        synchronized(callbacks.lock) {
            tts = created
            statusToApply = pendingInitStatus
            pendingInitStatus = null
        }
        statusToApply?.let(::onInitialized)
    }

    private fun onInitialized(status: Int) {
        synchronized(callbacks.lock) {
            if (shuttingDown) return
            if (tts == null) {
                pendingInitStatus = status
                return
            }
            ready = false
            if (status == TextToSpeech.SUCCESS) {
                try {
                    tts?.language = Locale.US
                    // voice-call usage → routed into the SCO link while it's up
                    tts?.setAudioAttributes(
                        AudioAttributes.Builder()
                            .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                            .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                            .build()
                    )
                    tts?.setOnUtteranceProgressListener(
                        object : UtteranceProgressListener() {
                            override fun onStart(id: String?) {}

                            override fun onDone(id: String?) {
                                if (id == null) return
                                completeReply(id, succeeded = true)
                            }

                            override fun onError(id: String?) {
                                if (id == null) return
                                completeReply(id, succeeded = false)
                            }
                        },
                    )
                    ready = true
                    Log.i(TAG, "local announcer ready")
                } catch (error: Exception) {
                    Log.w(TAG, "local TTS engine failed during setup", error)
                }
            } else {
                Log.w(TAG, "no local TTS engine available")
            }
        }
        callbacks.drain()
    }

    /** Queue a short announcement. Safe from any thread; never blocks interaction end. */
    fun say(text: String) {
        synchronized(callbacks.lock) {
            if (shuttingDown || !ready) return
            try {
                tts?.speak(
                    text,
                    TextToSpeech.QUEUE_ADD,
                    null,
                    "telepathos-${replySequence.incrementAndGet()}",
                )
            } catch (_: Exception) {
                // An engine can disappear between initialization and speak.
            }
        }
        callbacks.drain()
    }

    /**
     * Speak an agent reply; [onDone] fires only when speech actually finishes.
     * [onFailure] leaves durable pending text unconsumed when TTS is unavailable.
     */
    fun speakReply(text: String, onDone: () -> Unit, onFailure: () -> Unit = {}) {
        if (text.isBlank()) {
            callbacks.enqueue(onDone)
            return
        }

        var immediateFailure: (() -> Unit)? = null
        synchronized(callbacks.lock) {
            if (shuttingDown || !ready) {
                immediateFailure = onFailure
            } else {
                val id = "telepathos-reply-${replySequence.incrementAndGet()}"
                replyCallbacks.register(id, onDone, onFailure)
                try {
                    val result = tts?.speak(text, TextToSpeech.QUEUE_ADD, null, id)
                        ?: TextToSpeech.ERROR
                    if (result == TextToSpeech.ERROR) {
                        replyCallbacks.complete(id, succeeded = false)?.let {
                            callbacks.enqueueLocked(it)
                        }
                    }
                } catch (_: Exception) {
                    replyCallbacks.complete(id, succeeded = false)?.let {
                        callbacks.enqueueLocked(it)
                    }
                }
            }
        }
        immediateFailure?.let(callbacks::enqueue)
        callbacks.drain()
    }

    /** Stop every queued phone announcement/reply and abandon its completion callback. */
    fun stop() {
        synchronized(callbacks.lock) {
            replyCallbacks.clear()
            try {
                tts?.stop()
            } catch (_: Exception) {
                // Stopping an already-dead engine is harmless.
            }
        }
        callbacks.drain()
    }

    fun shutdown() {
        synchronized(callbacks.lock) {
            if (shuttingDown) return
            shuttingDown = true
            ready = false
            replyCallbacks.clear()
            try {
                tts?.stop()
                tts?.shutdown()
            } catch (_: Exception) {
                // Still publish the shut-down state even if the engine fails.
            }
            tts = null
        }
        callbacks.drain()
    }

    private fun completeReply(id: String, succeeded: Boolean) {
        synchronized(callbacks.lock) {
            replyCallbacks.complete(id, succeeded)?.let {
                callbacks.enqueueLocked(it)
            }
        }
        callbacks.drain()
    }

    companion object {
        private const val TAG = "Telepathos"
    }
}

/**
 * Completion callbacks are queued while the announcer monitor is held. This
 * keeps callbacks out of TTS calls and lets a callback safely call stop/speak
 * again without re-entering a partially updated announcer state.
 */
internal class LocalAnnouncerCallbackDispatcher {
    val lock = Any()
    private val pending = ArrayDeque<() -> Unit>()
    private var draining = false

    fun enqueue(callback: () -> Unit) {
        synchronized(lock) {
            pending.addLast(callback)
        }
        drain()
    }

    /** Caller must hold [lock]. */
    fun enqueueLocked(callback: () -> Unit) {
        pending.addLast(callback)
    }

    fun drain() {
        // A synchronous TTS callback can arrive while speak() holds the
        // monitor. The outer operation drains it after releasing the monitor.
        if (Thread.holdsLock(lock)) return
        synchronized(lock) {
            if (draining) return
            draining = true
        }
        try {
            while (true) {
                val callback = synchronized(lock) {
                    if (pending.isEmpty()) {
                        draining = false
                        null
                    } else {
                        pending.removeFirst()
                    }
                } ?: return
                callback()
            }
        } catch (error: Throwable) {
            synchronized(lock) {
                draining = false
            }
            throw error
        }
    }
}

/** Reply callback ownership; callers serialize this object with the announcer monitor. */
internal class LocalAnnouncerReplyCallbacks {
    private val done = HashMap<String, () -> Unit>()
    private val failed = HashMap<String, () -> Unit>()

    fun register(id: String, onDone: () -> Unit, onFailure: () -> Unit) {
        done[id] = onDone
        failed[id] = onFailure
    }

    /** Remove both outcomes and return only the outcome that won the race. */
    fun complete(id: String, succeeded: Boolean): (() -> Unit)? {
        val callback = if (succeeded) done.remove(id) else failed.remove(id)
        if (succeeded) failed.remove(id) else done.remove(id)
        return callback
    }

    fun clear() {
        done.clear()
        failed.clear()
    }
}
