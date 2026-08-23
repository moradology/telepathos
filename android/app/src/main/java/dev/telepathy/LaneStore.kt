package dev.telepathy

import android.content.Context
import android.util.Log
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject

/**
 * Phone-side view of the lane registry, read from telepathyd's /api/state.
 * All calls synchronous — invoke from worker threads only.
 */
object LaneStore {

    private val client = OkHttpClient()

    data class LaneUi(
        val id: String,
        val name: String,
        val title: String?,
        val active: Boolean,
        val pending: Int,
    )

    fun baseUrl(ctx: Context): String? =
        ctx.getSharedPreferences("cfg", Context.MODE_PRIVATE)
            .getString("hermes", null)?.takeIf { it.isNotBlank() }

    /** Full registry + active name + per-lane pending counts. Null when unreachable/unset. */
    fun fetchState(ctx: Context): Pair<List<LaneUi>, String>? {
        val base = baseUrl(ctx) ?: return null
        val res = client.newCall(Request.Builder().url("$base/api/state").build()).execute()
        res.use { r ->
            if (!r.isSuccessful) return null
            val o = JSONObject(r.body?.string() ?: return null)
            val activeName = o.optString("active")
            val activeId = o.optString("active_id")
            val pendingByLane = pendingMap(base)
            val arr = o.optJSONArray("lanes") ?: return null
            val lanes = (0 until arr.length()).mapNotNull { i ->
                val l = arr.optJSONObject(i) ?: return@mapNotNull null
                val id = l.optString("id")
                LaneUi(
                    id = id,
                    name = l.optString("name"),
                    title = l.optString("title").takeIf { it.isNotEmpty() },
                    active = id == activeId,
                    pending = pendingByLane[id] ?: 0,
                )
            }
            return lanes to activeName
        }
    }

    private fun pendingMap(base: String): Map<String, Int> {
        val res = client.newCall(
            Request.Builder().url("$base/api/pending").build()
        ).execute()
        res.use { r ->
            if (!r.isSuccessful) return emptyMap()
            val o = JSONObject(r.body?.string() ?: return emptyMap())
            return mapOf(o.optString("lane_id") to o.optInt("count", 0))
        }
    }

    /** Switch to a lane by id; announces via the event log. */
    fun switch(ctx: Context, laneId: String): Boolean {
        val base = baseUrl(ctx) ?: return false
        val body = """{"id":"$laneId"}""".toRequestBody("application/json".toMediaTypeOrNull())
        val res = client.newCall(
            Request.Builder().url("$base/api/lanes/active").post(body).build()
        ).execute()
        res.use { r ->
            if (!r.isSuccessful) return false
            val o = JSONObject(r.body?.string() ?: return false)
            return o.optBoolean("ok", false)
        }
    }

    /** Cycle to the next lane in registry order. Fire-and-forget; logs outcome. */
    fun cycle(ctx: Context) {
        Thread {
            val state = fetchState(ctx) ?: run {
                Log.w("Telepathy", "cycle: server unreachable"); return@Thread
            }
            if (state.first.isEmpty()) return@Thread
            val idx = state.first.indexOfFirst { it.active }.coerceAtLeast(0)
            val next = state.first[(idx + 1) % state.first.size]
            if (switch(ctx, next.id)) {
                TriggerLog.record(ctx, "→ ${next.name} (${state.first.size} lanes)")
            } else {
                Log.w("Telepathy", "cycle switch failed")
            }
        }.start()
    }
}
