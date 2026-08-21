package dev.telepathy

import android.content.Context
import android.media.AudioAttributes
import android.media.MediaPlayer
import android.util.Log
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/** In-app event log so the pinch test has visible output without adb. */
object TriggerLog {
    private const val PREFS = "trigger_log"
    private const val KEY = "events"
    private val listeners = mutableListOf<(String) -> Unit>()

    fun record(ctx: Context, what: String) {
        val ts = SimpleDateFormat("HH:mm:ss.SSS", Locale.US).format(Date())
        val line = "$ts  $what"
        Log.i("Telepathy", line)
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .edit()
            .putString(KEY, listOf(line, *(load(ctx).lines().take(50).toTypedArray())).joinToString("\n"))
            .apply()
        beep(ctx)
        listeners.forEach { it(line) }
    }

    fun load(ctx: Context): String =
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(KEY, "") ?: ""

    fun onChange(fn: (String) -> Unit) { listeners.add(fn) }

    private fun beep(ctx: Context) {
        try {
            MediaPlayer.create(ctx, android.provider.Settings.System.DEFAULT_NOTIFICATION_URI)?.let {
                it.setAudioAttributes(
                    AudioAttributes.Builder()
                        .setUsage(AudioAttributes.USAGE_ASSISTANCE_SONIFICATION).build()
                )
                it.setOnCompletionListener { m -> m.release() }
                it.start()
            }
        } catch (e: Exception) {
            Log.w("Telepathy", "beep failed: ${e.message}")
        }
    }
}
