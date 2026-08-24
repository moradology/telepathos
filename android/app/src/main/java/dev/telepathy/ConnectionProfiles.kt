package dev.telepathy

import android.content.Context
import org.json.JSONObject

/**
 * Named connection profiles: a saved set of fields (bridge WS, telepathyd HTTP,
 * token) with one active/default selection. First run seeds a sensible
 * emulator-oriented default so the app is never blank.
 */
object ConnectionProfiles {

    data class Profile(
        val server: String,
        val telepathyd: String,
        val token: String,
    )

    private const val KEY = "connection_profiles"

    const val DEFAULT_NAME = "default"

    fun defaults() = Profile(
        server = "ws://10.0.2.2:8787",
        telepathyd = "http://10.0.2.2:8790",
        token = "",
    )

    fun load(ctx: Context): MutableMap<String, Profile> {
        val out = linkedMapOf<String, Profile>()
        val prefs = ctx.getSharedPreferences(KEY, Context.MODE_PRIVATE)
        val raw = prefs.getString("profiles", null)
        if (raw != null) {
            try {
                val o = JSONObject(raw)
                val names = o.keys()
                while (names.hasNext()) {
                    val name = names.next()
                    val p = o.getJSONObject(name)
                    out[name] = Profile(
                        server = p.optString("server"),
                        telepathyd = p.optString("telepathyd"),
                        token = p.optString("token"),
                    )
                }
            } catch (_: Exception) { /* corrupt store -> reseed */ }
        }
        if (out.isEmpty()) {
            out[DEFAULT_NAME] = defaults()
            save(ctx, out, DEFAULT_NAME)
        }
        return out
    }

    fun activeName(ctx: Context): String {
        val prefs = ctx.getSharedPreferences(KEY, Context.MODE_PRIVATE)
        val name = prefs.getString("active", null) ?: DEFAULT_NAME
        val profiles = load(ctx)
        return if (profiles.containsKey(name)) name else profiles.keys.first()
    }

    fun save(ctx: Context, profiles: Map<String, Profile>, active: String) {
        val o = JSONObject()
        for ((name, p) in profiles) {
            o.put(name, JSONObject().apply {
                put("server", p.server)
                put("telepathyd", p.telepathyd)
                put("token", p.token)
            })
        }
        ctx.getSharedPreferences(KEY, Context.MODE_PRIVATE).edit()
            .putString("profiles", o.toString())
            .putString("active", active)
            .apply()
    }

    /** Push the active profile into the legacy "cfg" prefs the services read. */
    fun applyActive(ctx: Context) {
        val profiles = load(ctx)
        val name = activeName(ctx)
        val p = profiles[name] ?: return
        ctx.getSharedPreferences("cfg", Context.MODE_PRIVATE).edit()
            .putString("server", p.server)
            .putString("hermes", p.telepathyd)
            .putString("token", p.token)
            .apply()
    }
}
