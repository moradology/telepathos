package dev.telepathos

import android.os.Bundle
import android.service.voice.VoiceInteractionService
import android.util.Log

/**
 * Bound by the system when Telepathos is the default digital assistant and an
 * assist gesture occurs (Shokz pinch, long-press power, corner swipe...).
 *
 * THE TEST: if this class logs "onReady" when you pinch the OpenDots,
 * the whole architecture is viable.
 */
class TelepathosVoiceInteractionService : VoiceInteractionService() {

    override fun onReady() {
        super.onReady()
        Log.i(TAG, "onReady: telepathos assistant registered")
    }

    companion object {
        private const val TAG = "Telepathos"
    }
}
