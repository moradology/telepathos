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

    private fun configuredUrl(ctx: Context): String? =
        ctx.getSharedPreferences("cfg", Context.MODE_PRIVATE)
            .getString("hermes", null)?.trimEnd('/')?.takeIf { it.isNotBlank() }

    private fun token(ctx: Context): String? =
        ctx.getSharedPreferences("cfg", Context.MODE_PRIVATE)
            .getString("token", null)?.takeIf { it.isNotBlank() }

    /** True when an authoritative telepathyd endpoint has been configured. */
    fun isConfigured(ctx: Context): Boolean = configuredUrl(ctx) != null

    fun baseUrl(ctx: Context): String? {
        val url = configuredUrl(ctx) ?: return null
        if (token(ctx) != null && !url.startsWith("https://", ignoreCase = true)) {
            Log.e("Telepathy", "token-bearing lane API requires https://")
            return null
        }
        return url
    }

    private fun request(ctx: Context, url: String): Request.Builder =
        Request.Builder().url(url).apply {
            token(ctx)?.let { header("x-telepathy-token", it) }
        }

    sealed interface LaneStateResult {
        data class Ok(val lanes: List<LaneUi>, val activeName: String) : LaneStateResult
        data object NotConfigured : LaneStateResult
        data class Unreachable(val reason: String) : LaneStateResult
    }

    /** Full registry + active name + per-lane pending counts, with a truthful cause. */
    fun fetchState(ctx: Context): LaneStateResult {
        val base = baseUrl(ctx) ?: return LaneStateResult.NotConfigured
        return try {
            val response = client.newCall(request(ctx, "$base/api/state").build()).execute()
            if (!response.isSuccessful) {
                response.close()
                return LaneStateResult.Unreachable("HTTP ${response.code}")
            }
            val o = BoundedHttpResponse.readJsonObject(response, HttpResponseLimits.LANE_STATE_BYTES)
                ?: return LaneStateResult.Unreachable("malformed state body")
            val activeName = o.optString("active")
            val activeId = o.optString("active_id")
            if (!isValidLaneId(activeId)) return LaneStateResult.Unreachable("invalid active lane id")
            val arr = o.optJSONArray("lanes")
                ?: return LaneStateResult.Unreachable("malformed state body")
            val lanes = (0 until arr.length()).map { i ->
                val l = arr.optJSONObject(i)
                    ?: throw IllegalArgumentException("invalid lane entry")
                val id = l.optString("id")
                if (!isValidLaneId(id)) throw IllegalArgumentException("invalid lane id")
                LaneUi(
                    id = id,
                    name = l.optString("name"),
                    title = l.optString("title").takeIf { it.isNotEmpty() },
                    active = id == activeId,
                    pending = l.optInt("pending", 0),
                )
            }
            LaneStateResult.Ok(lanes, activeName)
        } catch (e: IllegalArgumentException) {
            LaneStateResult.Unreachable(e.message ?: "malformed state body")
        } catch (e: Exception) {
            LaneStateResult.Unreachable(e.message ?: "unreachable")
        }
    }

    /** Switch to a lane by id; announces via the event log. */
    fun switch(ctx: Context, laneId: String): Boolean {
        val base = baseUrl(ctx) ?: return false
        val json = laneSwitchRequestJson(laneId) ?: return false
        val body = json.toRequestBody("application/json".toMediaTypeOrNull())
        return try {
            val response = client.newCall(
                request(ctx, "$base/api/lanes/active").post(body).build()
            ).execute()
            if (!response.isSuccessful) {
                response.close()
                return false
            }
            val o = BoundedHttpResponse.readJsonObject(response, HttpResponseLimits.LANE_MUTATION_BYTES)
                ?: return false
            o.optBoolean("ok", false)
        } catch (e: Exception) {
            Log.w("Telepathy", "lane switch unavailable: ${e.message}")
            false
        }
    }

    /** Pure, validated serializer used by [switch]; JSONObject performs JSON escaping. */
    internal fun laneSwitchRequestJson(laneId: String): String? =
        laneId.takeIf(::isValidLaneId)?.let { JSONObject().put("id", it).toString() }

    /** Cycle to the next lane in registry order. Fire-and-forget; logs outcome. */
    fun cycle(ctx: Context) {
        Thread {
            val state = fetchState(ctx)
            val lanes = (state as? LaneStateResult.Ok)?.lanes ?: run {
                Log.w("Telepathy", "cycle: ${state}"); return@Thread
            }
            if (lanes.isEmpty()) return@Thread
            val idx = lanes.indexOfFirst { it.active }.coerceAtLeast(0)
            val next = lanes[(idx + 1) % lanes.size]
            if (switch(ctx, next.id)) {
                TriggerLog.record(ctx, "→ ${next.name} (${lanes.size} lanes)")
            } else {
                Log.w("Telepathy", "cycle switch failed")
            }
        }.start()
    }
}
