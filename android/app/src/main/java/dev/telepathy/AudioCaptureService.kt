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
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
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
    private val mic by lazy { MicController(this) }

    // phone-TTS mode: the server sends TEXT only; we speak it on-device.
    private val replyText = StringBuilder()

    /**
     * Capture policy (capture-on-demand): the pinch sets this; `listening` clears it.
     * Between interactions the mic is CLOSED — zero radio, zero mic power.
     */
    @Volatile private var captureRequested = false

    @Volatile private var wantConnection = false
    private var reconnectAttempt = 0

    // ---- capture start choreography (SCO-first, cued) ----

    @Volatile private var lastPhase = "listening"
    @Volatile private var scoPending = false
    private val mainHandler = Handler(Looper.getMainLooper())
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
                if (openMicNow()) {
                    scoPending = false
                    mainHandler.removeCallbacks(scoFallback)
                }
            }
        }
    }

    override fun onCreate() {
        super.onCreate()
        // RECEIVER_NOT_EXPORTED: system broadcast, other apps must not spoof it.
        // Required flag on API 34+, otherwise registration throws.
        registerReceiver(
            scoReceiver,
            IntentFilter(AudioManager.ACTION_SCO_AUDIO_STATE_UPDATED),
            Context.RECEIVER_NOT_EXPORTED
        )
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

    private fun sendCommand(command: ClientCommand) {
        TriggerLog.record(this, "gesture → ${command::class.simpleName?.lowercase()}")
        if (command == ClientCommand.CancelCapture) {
            // pinch-hold: drop the utterance AND the mic — next pinch reopens
            captureRequested = false
            mic.close()
        }
        ws?.send(command.toJson())
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(Foreground.notifyId(), Foreground.start(this, "pinch to talk"))
        // Every pinch lands here (idempotent while running). It means "I want to talk":
        if (!wantConnection) {
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
        captureRequested = true
        if (!wantConnection) return // mic opens on next onOpen
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
    private fun openMicNow(): Boolean {
        val socket = ws ?: return false
        if (!captureRequested || mic.isOpen) return false
        return if (mic.open { chunk -> socket.send(chunk.toByteString()) }) {
            playCue(ToneGenerator.TONE_PROP_BEEP, 120)   // "go ahead"
            updateNotification()
            true
        } else {
            announcer.say("Microphone unavailable.")
            false
        }
    }

    override fun onDestroy() {
        wantConnection = false
        captureRequested = false
        mainHandler.removeCallbacks(scoFallback)
        unregisterReceiver(scoReceiver)
        mic.close()
        ws?.close(1000, "bye")
        mediaSession?.release()
        mediaSession = null
        try { tone?.release() } catch (_: Exception) {}
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
        mic.close() // recorder is tied to a dead socket
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
            if (captureRequested) requestCaptureStart()
            else updateNotification()
        }

        override fun onMessage(webSocket: WebSocket, text: String) = handleControl(text)

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
            is ServerMsg.Stt -> {
                replyText.clear()          // new interaction
                mediaSession?.isActive = true  // taps matter from here until the reply ends
                TriggerLog.record(this, "heard: ${msg.text}")
                // M5 echo-back, client-side: confirmation before the agent acts
                announcer.say("Working on: ${msg.text}")
            }
            is ServerMsg.AgentDelta -> replyText.append(msg.text)
            is ServerMsg.Error -> {
                TriggerLog.record(this, "server error: ${msg.message}")
                announcer.say("Server error.")
            }
            is ServerMsg.Phase -> {
                if (msg.value == "processing" && lastPhase == "capturing") {
                    playCue(ToneGenerator.TONE_CDMA_PIP, 80) // "heard you — thinking"
                }
                lastPhase = msg.value
                LinkState.setPhase(msg.value)
                TriggerLog.record(this, "· ${msg.value}")
            }
            ServerMsg.Ready -> TriggerLog.record(this, "server ready")
            ServerMsg.Listening -> {
                // mic closes NOW; SCO/session release waits until speech is done
                captureRequested = false
                mic.close()
                updateNotification()
            }
            ServerMsg.AgentEnd -> {
                val reply = replyText.toString().trim()
                if (reply.isNotEmpty()) {
                    announcer.speakReply(reply) { finishInteraction() }
                } else {
                    finishInteraction()
                }
            }
        }
    }

    /**
     * Speech finished (or nothing to say): release the media session back to real
     * media apps and drop SCO call-routing after a short grace period.
     */
    private fun finishInteraction() {
        mediaSession?.isActive = false
        scoPending = false
        mainHandler.removeCallbacks(scoFallback)
        android.os.Handler(android.os.Looper.getMainLooper()).postDelayed({
            try {
                audioManager.stopBluetoothSco()
                audioManager.setBluetoothScoOn(false)
            } catch (_: Exception) {}
        }, 800)
    }

    companion object { private const val TAG = "Telepathy" }
}
