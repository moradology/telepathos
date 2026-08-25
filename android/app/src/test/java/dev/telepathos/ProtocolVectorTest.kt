package dev.telepathos

import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.File

/**
 * Shared-vector conformance: Protocol.kt must classify
 * protocol/vectors.json exactly as server/src/protocol.ts and
 * telepathos-proto do. Vectors live at the repo root; the gradle test
 * working directory is android/app, hence ../../.
 *
 * Deliberate divergences (documented, not drift):
 * - speech_start / utterance frames: informational, superseded by phase
 *   broadcasts on the phone. Kotlin parse returns null for them by design.
 */
class ProtocolVectorTest {

    private val ignoredServerTypes = setOf("speech_start", "utterance")

    private val vectors: JSONObject by lazy {
        val candidates = listOf(
            File("../../protocol/vectors.json"),
            File("protocol/vectors.json"),
        )
        val file = candidates.firstOrNull { it.exists() }
            ?: throw IllegalStateException("vectors.json not found")
        JSONObject(file.readText())
    }

    private fun typeName(msg: ServerMsg): String = when (msg) {
        is ServerMsg.Stt -> "stt"
        is ServerMsg.AgentDelta -> "agent_delta"
        is ServerMsg.AgentEnd -> "agent_end"
        is ServerMsg.Error -> "error"
        is ServerMsg.Phase -> "phase"
        is ServerMsg.Incoming -> "incoming"
        is ServerMsg.Ready -> "ready"
        is ServerMsg.Listening -> "listening"
        is ServerMsg.ReplyReceived -> "reply_received"
        is ServerMsg.ReplyAcknowledged -> "reply_acknowledged"
        is ServerMsg.ReplyAckRetired -> "reply_ack_retired"
    }

    private fun cases(plane: String): JSONArray = vectors.getJSONObject(plane).getJSONArray("valid")

    @Test
    fun server_vectors_match_reference() {
        var failures = 0
        var checked = 0
        for (entry in cases("server")) {
            val case = entry as JSONObject
            val frame = case.optString("frame")
            val expected = case.optString("type")
            if (expected in ignoredServerTypes) continue // documented divergence
            checked++
            val got = ServerMsg.parse(frame)?.let { typeName(it) }
            if (got != expected) {
                println("FAIL server $frame: got $got, expected $expected")
                failures++
            }
        }
        for (entryFrame in vectors.getJSONObject("server").getJSONArray("invalid")) {
            val frame = entryFrame as String
            if (ServerMsg.parse(frame) != null) {
                println("FAIL server invalid accepted: $frame")
                failures++
            }
        }
        assertTrue("checked $checked server vectors", checked > 0)
        if (failures > 0) fail("$failures server vector failures")
    }

    @Test
    fun control_serialization_matches_reference_grammar() {
        // Kotlin BUILDS control frames rather than parsing them. Assert the
        // builders emit frames satisfying the reference grammar's key rules.
        var failures = 0

        val hello = ClientHello(installationId = "inst-1").toJson()
        val helloJson = JSONObject(hello)
        if (helloJson.optString("type") != "hello" ||
            helloJson.optString("installation_id") != "inst-1") {
            println("FAIL hello serialization: $helloJson")
            failures++
        }

        val stop = ClientCommand.Command(ClientCommand.Kind.Stop, "t1").toJson()
        val stopJson = JSONObject(stop)
        if (stopJson.optString("command") != "stop" ||
            stopJson.optString("turn_token") != "t1") {
            println("FAIL command serialization: $stopJson")
            failures++
        }

        val lane = ClientCommand.LaneSnapshot(
            id = "telepathos:direct", turnToken = "t1", revision = 7,
        ).toJson()
        val laneJson = JSONObject(lane)
        if (laneJson.optString("id") != "telepathos:direct" ||
            laneJson.optInt("revision") != 7) {
            println("FAIL lane serialization: $laneJson")
            failures++
        }

        val flush = ClientCommand.FlushUtterance("t1").toJson()
        if (JSONObject(flush).optString("type") != "utterance_end") {
            println("FAIL flush serialization: $flush")
            failures++
        }

        if (failures > 0) fail("$failures control serialization failures")
    }
}
