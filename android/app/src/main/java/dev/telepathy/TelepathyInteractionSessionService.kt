package dev.telepathy

import android.os.Bundle
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
        Log.i("Telepathy", "SESSION SHOWN — pinch works! flags=$showFlags")
        TriggerLog.record(context, "session_shown")

        // v1: prove the pipeline. Later: start AudioCaptureService here.
        hide()
    }
}
