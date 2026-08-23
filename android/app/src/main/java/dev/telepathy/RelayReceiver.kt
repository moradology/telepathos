package dev.telepathy

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Notification action buttons. Runs on the main thread; network work is
 * delegated to the service/threads.
 */
class RelayReceiver : BroadcastReceiver() {
    override fun onReceive(ctx: Context, intent: Intent) {
        when (intent.action) {
            Foreground.ACTION_TALK -> {
                val i = Intent(ctx, AudioCaptureService::class.java)
                ctx.startForegroundService(i)
            }
            Foreground.ACTION_STOP -> {
                ctx.stopService(Intent(ctx, AudioCaptureService::class.java))
            }
            Foreground.ACTION_SWITCH -> LaneStore.cycle(ctx)
        }
    }
}
