package dev.telepathos

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import androidx.core.app.NotificationCompat

object Foreground {
    const val CHANNEL_ID = "telepathos"
    private const val NOTIF_ID = 1
    const val ACTION_SWITCH = "dev.telepathos.SWITCH_LANE"
    const val ACTION_TALK = "dev.telepathos.TALK"
    const val ACTION_STOP = "dev.telepathos.STOP"

    fun ensureChannel(ctx: Context) {
        val nm = ctx.getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "Telepathos", NotificationManager.IMPORTANCE_LOW)
        )
    }

    /**
     * The app's real home screen (it lives in the shade, not the launcher):
     * active lane + pending count + the three verbs that matter.
     */
    fun start(ctx: Context, text: String): Notification = build(ctx, text, null, 0).build()

    /** Re-post with current lane/pending state. */
    fun update(ctx: Context, text: String, lane: String? = null, pending: Int = 0) {
        ensureChannel(ctx)
        ctx.getSystemService(NotificationManager::class.java)
            .notify(NOTIF_ID, build(ctx, text, lane, pending).build())
    }

    private fun build(ctx: Context, text: String, lane: String?, pending: Int): NotificationCompat.Builder {
        ensureChannel(ctx)
        val b = NotificationCompat.Builder(ctx, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.ic_btn_speak_now)
            .setContentTitle(if (lane != null) "TELEPATHOS · $lane" else "Telepathos")
            .setContentText(text)
            .setOngoing(true)

        if (lane != null && pending > 0) b.setSubText("📌 $pending pending")

        b.addAction(0, "Talk", serviceIntent(ctx, AudioCaptureService::class.java))
        b.addAction(0, "Switch lane", broadcastIntent(ctx, ACTION_SWITCH))
        b.addAction(0, "Stop", broadcastIntent(ctx, ACTION_STOP))

        // tapping the body opens the console
        val open = PendingIntent.getActivity(
            ctx, 0, Intent(ctx, MainActivity::class.java),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        b.setContentIntent(open)
        return b
    }

    private fun serviceIntent(ctx: Context, cls: Class<*>) = PendingIntent.getForegroundService(
        ctx, 1, Intent(ctx, cls),
        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
    )

    private fun broadcastIntent(ctx: Context, action: String) = PendingIntent.getBroadcast(
        ctx, 2, Intent(ctx, RelayReceiver::class.java).setAction(action),
        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
    )

    fun notifyId() = NOTIF_ID
}
