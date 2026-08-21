package dev.telepathy

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.content.Intent
import androidx.core.app.NotificationCompat

object Foreground {
    const val CHANNEL_ID = "telepathy"
    private const val NOTIF_ID = 1

    fun ensureChannel(ctx: Context) {
        val nm = ctx.getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "Telepathy", NotificationManager.IMPORTANCE_LOW)
        )
    }

    fun start(ctx: Context, text: String): Notification = build(ctx, text)

    /** Re-post the notification with new text (connection state changes, M4). */
    fun update(ctx: Context, text: String) {
        ensureChannel(ctx)
        ctx.getSystemService(NotificationManager::class.java).notify(NOTIF_ID, build(ctx, text))
    }

    private fun build(ctx: Context, text: String): Notification =
        NotificationCompat.Builder(ctx, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.ic_btn_speak_now)
            .setContentTitle("Telepathy")
            .setContentText(text)
            .setOngoing(true)
            .build()

    fun notifyId() = NOTIF_ID
}
