package dev.telepathy

import android.content.Intent
import android.os.Bundle
import android.os.SystemClock
import android.service.voice.VoiceInteractionSession
import android.service.voice.VoiceInteractionSessionService
import android.util.Log

/**
 * The system instantiates this when an assist gesture launches a session.
 * For v1 we show nothing and immediately close — we only need proof of life,
 * then hand off to AudioCaptureService.
 */
class TelepathyInteractionSessionService : VoiceInteractionSessionService() {
    override fun onNewSession(args: Bundle?): VoiceInteractionSession {
        Log.i("Telepathy", "SESSION CREATED — assist gesture reached us")
        return TelepathyInteractionSession(this)
    }
}

class TelepathyInteractionSession(context: android.content.Context) : VoiceInteractionSession(context) {

    override fun onShow(args: Bundle?, showFlags: Int) {
        super.onShow(args, showFlags)
        // Double-pinch detection: two assist triggers within 700ms = meta agent.
        // One pinch = talk to the active lane; two = talk to the meta plane.
        val now = SystemClock.elapsedRealtime()
        val isMeta = now - lastPinch < 700
        lastPinch = now
        Log.i("Telepathy", "pinch (meta=$isMeta)")
        TriggerLog.record(context, if (isMeta) "double pinch → meta" else "pinch → capture")

        // Voice-interaction sessions are an exempted context for background FGS
        // starts. Idempotent: onStartCommand's guards make repeat pinches cheap.
        val intent = Intent(context, AudioCaptureService::class.java)
        intent.putExtra(AudioCaptureService.EXTRA_META, isMeta)
        context.startForegroundService(intent)

        hide()
    }

    companion object {
        @Volatile private var lastPinch = 0L
    }
}
