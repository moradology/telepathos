package dev.telepathy

import android.content.Context
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import android.util.Log

/**
 * Black-box microphone: open(stream), close(). That's the entire contract.
 *
 * Mechanics live here (AudioRecord lifecycle, reader thread, teardown races);
 * POLICY lives outside — the service decides WHEN based on protocol events
 * (pinch opens, `listening` closes). This class knows nothing about either.
 *
 * Battery contract: closed == zero mic power, zero radio transmission.
 */
class MicController(private val context: Context) {

    private var recorder: AudioRecord? = null
    private var readerThread: Thread? = null

    @Volatile private var active = false
    val isOpen: Boolean get() = active

    /**
     * Open the mic and pump 16 kHz PCM16 mono chunks to [send] until closed().
     * Returns false if the recorder couldn't initialize (no permission / BT oddity).
     * Idempotent: opening twice closes first — never two readers.
     */
    fun open(send: (ByteArray) -> Unit): Boolean {
        close()
        val sampleRate = 16000
        val minBuf = AudioRecord.getMinBufferSize(
            sampleRate, AudioFormat.CHANNEL_IN_MONO, AudioFormat.ENCODING_PCM_16BIT)
        val rec = AudioRecord(
            MediaRecorder.AudioSource.VOICE_COMMUNICATION, // HFP mic when SCO active
            sampleRate, AudioFormat.CHANNEL_IN_MONO,
            AudioFormat.ENCODING_PCM_16BIT, minBuf * 2
        )
        if (rec.state != AudioRecord.STATE_INITIALIZED) {
            Log.e(TAG, "AudioRecord init failed")
            rec.release()
            return false
        }
        recorder = rec
        active = true
        rec.startRecording()
        readerThread = Thread {
            val buf = ByteArray(3200) // 100 ms chunks
            while (!Thread.currentThread().isInterrupted && active && recorder === rec) {
                val n = rec.read(buf, 0, buf.size)
                if (n > 0) send(buf.copyOf(n))
            }
        }.also { it.start() }
        TriggerLog.record(context, "mic OPEN")
        return true
    }

    /** Unblock + join the reader, release the mic. Safe from any thread, any state. */
    fun close() {
        if (!active && recorder == null) return
        active = false
        val rec = recorder
        recorder = null
        try { rec?.stop() } catch (_: Exception) {}   // unblocks read()
        try { rec?.release() } catch (_: Exception) {}
        readerThread?.join(1000)
        readerThread = null
        TriggerLog.record(context, "mic CLOSED")
    }

    companion object { private const val TAG = "Telepathy" }
}
