package dev.telepathos

import android.content.ComponentName
import android.content.Context
import android.provider.Settings

/** Setup-doctor checks (features.md M1). */
object AssistantChecks {

    /** Is Telepathos registered as the system's default digital assistant? */
    fun isDefaultAssistant(ctx: Context): Boolean {
        val current = Settings.Secure.getString(
            ctx.contentResolver, "voice_interaction_service") ?: return false
        val cn = ComponentName(ctx, TelepathosVoiceInteractionService::class.java)
        return current == cn.flattenToString() || current == cn.flattenToShortString()
    }
}
