package dev.telepathy

import android.app.Service
import android.content.Intent
import android.media.AudioAttributes
import android.media.AudioDeviceInfo
import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioRecord
import android.media.AudioTrack
import android.media.MediaRecorder
import android.media.AudioDeviceCallback
import android.media.session.MediaSession
import android.os.IBinder
import android.util.Log
import android.view.KeyEvent
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString
import java.util.concurrent.TimeUnit

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

    private var ws: WebSocket? = null
    private var recorder: AudioRecord? = null
    private var readerThread: Thread? = null
    private var track: AudioTrack? = null

    @Volatile private var serviceRunning = false
    @Volatile private var wantConnection = false
    private var reconnectAttempt = 0

    override fun onCreate() {
        super.onCreate()
        // Earbud (dis)connection tracking — M4
        audioManager.registerAudioDeviceCallback(object : AudioDeviceCallback() {
            override fun onAudioDevicesAdded(added: Array<out AudioDeviceInfo>) { refreshBuds() }
            override fun onAudioDevicesRemoved(removed: Array<out AudioDeviceInfo>) { refreshBuds() }
        }, null)
        refreshBuds()

        // Earbud taps arrive as AVRCP media keys (features.md M3).
        // Key→command mapping lives in ClientCommand.fromMediaKey (pure, testable).
        // Session is INACTIVE except mid-interaction (B2 fix): inactive sessions
        // leave media keys to whichever app is actually playing.
        mediaSession = MediaSession(this, "Telepathy").apply {
            setCallback(object : MediaSession.Callback() {
                override fun onMediaButtonEvent(mediaButtonIntent: Intent): Boolean {
                    val ev = mediaButtonIntent.getParcelableExtra<KeyEvent>(Intent.EXTRA_KEY_EVENT)
                    if (ev?.action == KeyEvent.ACTION_DOWN) {
                        ClientCommand.fromMediaKey(ev.keyCode)?.let(::sendCommand)
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

    private fun sendCommand(command: ClientCommand) {
        TriggerLog.record(this, "gesture → ${command::class.simpleName?.lowercase()}")
        ws?.send(command.toJson())
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(Foreground.notifyId(), Foreground.start(this, "listening…"))
        if (!wantConnection) {
            wantConnection = true
            connect()
        }
        return START_STICKY
    }

    private fun writeToTrack(arr: ByteArray) {
        synchronized(trackLock) {
            val t = track ?: return
            try { t.write(arr, 0, arr.size) } catch (e: Exception) { Log.w(TAG, "track write: ${e.message}") }
        }
    }

    private fun stopPlayback() {
        synchronized(trackLock) {
            try { track?.stop(); track?.release() } catch (_: Exception) {}
            track = null
        }
    }

    override fun onDestroy() {
        wantConnection = false
        stopRecording()
        stopPlayback()
        ws?.close(1000, "bye")
        mediaSession?.release()
        mediaSession = null
        announcer.shutdown()
        super.onDestroy()
    }

    /** Keep the persistent notification truthful (M4); wording comes from ConnectionState. */
    private fun updateNotification() {
        Foreground.update(this, LinkState.current.summary)
    }

    // ---- connection ----

    private fun serverUrl(): String =
        getSharedPreferences("cfg", MODE_PRIVATE).getString("server", null)
            ?: "ws://192.168.1.10:8787"

    private fun connect() {
        if (!wantConnection) return
        Log.i(TAG, "connecting (attempt $reconnectAttempt)…")
        val request = Request.Builder().url(serverUrl()).build()
        ws = client.newWebSocket(request, SocketListener())
    }

    private fun scheduleReconnect(reason: String) {
        if (!wantConnection) return
        stopRecording()
        reconnectAttempt = Math.min(reconnectAttempt + 1, 6)
        val delayMs = (1000L shl (reconnectAttempt - 1)).coerceAtMost(30_000) // 1s..30s backoff
        Log.w(TAG, "disconnected ($reason); retry in ${delayMs}ms")
        TriggerLog.record(this, "reconnecting in ${delayMs / 1000}s ($reason)")
        Thread {
            try { Thread.sleep(delayMs) } catch (_: InterruptedException) {}
            if (wantConnection) connect()
        }.also { it.isDaemon = true; it.start() }
    }

    private inner class SocketListener : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            reconnectAttempt = 0
            Log.i(TAG, "ws open")
            LinkState.setWs(true)
            webSocket.send("""{"type":"hello","device":"opendots2-pixel9"}""")
            startRecording(webSocket)
        }

        override fun onMessage(webSocket: WebSocket, text: String) = handleControl(text)

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            writeToTrack(bytes.toByteArray())
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            if (LinkState.current.wsUp) {
                // we HAD a link and lost it — say so out loud (M4: feedback lives in the ears)
                announcer.say("Connection lost. Reconnecting.")
            }
            LinkState.setWs(false)
            updateNotification()
            scheduleReconnect(t.message ?: "failure")
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            LinkState.setWs(false)
            updateNotification()
            scheduleReconnect("closed $code")
        }
    }

    private fun handleControl(text: String) {
        when (val msg = ServerMsg.parse(text)) {
            // exhaustive over the union — a new ServerMsg variant fails compilation here
            null -> Log.w(TAG, "dropping unparseable control frame")
            is ServerMsg.Stt -> TriggerLog.record(this, "heard: ${msg.text}")
            is ServerMsg.TtsStart -> {
                mediaSession?.isActive = true   // our taps matter while we're talking (B2)
                startPlayback(msg.sampleRate)
            }
            is ServerMsg.Error -> {
                TriggerLog.record(this, "server error: ${msg.message}")
                announcer.say("Server error.")
            }
            is ServerMsg.Phase -> TriggerLog.record(this, "· ${msg.value}")
            ServerMsg.Ready -> TriggerLog.record(this, "server ready")
            ServerMsg.Listening -> onInteractionEnd()
            ServerMsg.AgentEnd -> Unit // state we track elsewhere
        }
    }

    /**
     * Interaction over: release the media session back to real media apps (B2)
     * and drop the SCO call-routing shortly after, so Bluetooth leaves
     * power-hungry call mode (B3). Grace period lets buffered audio drain.
     */
    private fun onInteractionEnd() {
        mediaSession?.isActive = false
        updateNotification()
        android.os.Handler(android.os.Looper.getMainLooper()).postDelayed({
            try {
                audioManager.stopBluetoothSco()
                audioManager.setBluetoothScoOn(false)
            } catch (_: Exception) {}
        }, 800)
    }

    // ---- playback ----

    /** Route TTS to the active call stream so it lands in the earbuds during SCO. */
    private fun startPlayback(sampleRate: Int) {
        val minBuf = AudioTrack.getMinBufferSize(
            sampleRate, AudioFormat.CHANNEL_OUT_MONO, AudioFormat.ENCODING_PCM_16BIT)
        val t = AudioTrack.Builder()
            .setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build())
            .setAudioFormat(
                AudioFormat.Builder()
                    .setEncoding(AudioFormat.ENCODING_PCM_16BIT)
                    .setSampleRate(sampleRate)
                    .setChannelMask(AudioFormat.CHANNEL_OUT_MONO)
                    .build())
            .setTransferMode(AudioTrack.MODE_STREAM)
            .setBufferSizeInBytes(minBuf * 2)
            .build()
        synchronized(trackLock) {
            try { track?.stop(); track?.release() } catch (_: Exception) {}
            track = t
        }
        t.play()
        // make sure BT SCO link is up so audio goes to the earbuds, not the phone speaker
        val am = getSystemService(AUDIO_SERVICE) as AudioManager
        if (!am.isBluetoothScoOn) {
            am.startBluetoothSco()
            am.setBluetoothScoOn(true)
        }
    }

    // ---- capture ----

    private fun startRecording(socket: WebSocket) {
        stopRecording() // never two readers
        val sampleRate = 16000
        val minBuf = AudioRecord.getMinBufferSize(
            sampleRate, AudioFormat.CHANNEL_IN_MONO, AudioFormat.ENCODING_PCM_16BIT)
        val rec = AudioRecord(
            MediaRecorder.AudioSource.VOICE_COMMUNICATION,  // routes to HFP mic when SCO active
            sampleRate, AudioFormat.CHANNEL_IN_MONO,
            AudioFormat.ENCODING_PCM_16BIT, minBuf * 2
        )
        if (rec.state != AudioRecord.STATE_INITIALIZED) {
            Log.e(TAG, "AudioRecord init failed")
            rec.release()
            stopSelf()
            return
        }
        recorder = rec
        rec.startRecording()
        readerThread = Thread {
            val buf = ByteArray(3200) // 100 ms chunks
            while (!Thread.currentThread().isInterrupted && recorder === rec) {
                val n = rec.read(buf, 0, buf.size)
                if (n > 0) socket.send(buf.copyOf(n).toByteString())
            }
        }.also { it.start() }
        TriggerLog.record(this, "mic streaming started → ${serverUrl()}")
    }

    /** Unblock + join the reader, release the mic. Safe to call repeatedly. */
    private fun stopRecording() {
        val rec = recorder
        recorder = null
        try { rec?.stop() } catch (_: Exception) {}   // unblocks read()
        try { rec?.release() } catch (_: Exception) {}
        readerThread?.join(1000)
        readerThread = null
    }

    private val trackLock = Any()

    companion object { private const val TAG = "Telepathy" }
}
