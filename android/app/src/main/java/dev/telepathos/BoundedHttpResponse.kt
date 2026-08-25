package dev.telepathos

import okhttp3.Response
import org.json.JSONException
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

/** Response limits for the bounded HTTP endpoints used by the Android client. */
internal object HttpResponseLimits {
    /** Matches Node's TELEPATHOSD_STATE_RESPONSE_MAX_BYTES bounded registry response. */
    const val LANE_STATE_BYTES = 1024 * 1024

    /** A successful lane mutation returns only a small acknowledgement envelope. */
    const val LANE_MUTATION_BYTES = 16 * 1024

    /** telepathosd bounds serialized pending deliveries at 8 MiB, plus its envelope. */
    const val PENDING_BYTES = 8 * 1024 * 1024 + 64 * 1024
}

/**
 * Reads an OkHttp response without allowing an untrusted body to become an
 * unbounded String. The response is always closed by this helper, including
 * preflight, read, decode, cancellation, and parse failures.
 */
internal object BoundedHttpResponse {
    fun readUtf8(
        response: Response,
        maxBytes: Int,
        isCancelled: () -> Boolean = { Thread.currentThread().isInterrupted },
    ): String? {
        require(maxBytes > 0) { "maxBytes must be positive" }
        return try {
            response.use { value ->
                if (isCancelled()) return@use null
                val body = value.body ?: return@use null
                val declaredLength = body.contentLength()
                if (declaredLength < -1L || declaredLength > maxBytes.toLong()) {
                    return@use null
                }

                val source = body.source()
                val bytes = ByteArrayOutputStream(minOf(maxBytes, 8 * 1024))
                val chunk = ByteArray(8 * 1024)
                var total = 0
                while (true) {
                    if (isCancelled()) return@use null
                    val count = source.read(
                        chunk,
                        0,
                        minOf(chunk.size, maxBytes - total + 1),
                    )
                    if (count < 0) break
                    if (count == 0 || count > maxBytes - total) return@use null
                    bytes.write(chunk, 0, count)
                    total += count
                }

                strictUtf8(bytes.toByteArray())
            }
        } catch (_: Exception) {
            // Network failures, cancellation, malformed UTF-8, and fake or
            // platform response-body failures are all safe endpoint failures.
            null
        }
    }

    fun readJsonObject(
        response: Response,
        maxBytes: Int,
        isCancelled: () -> Boolean = { Thread.currentThread().isInterrupted },
    ): JSONObject? {
        val text = readUtf8(response, maxBytes, isCancelled) ?: return null
        return try {
            JSONObject(text)
        } catch (_: JSONException) {
            null
        } catch (_: RuntimeException) {
            null
        }
    }

    private fun strictUtf8(bytes: ByteArray): String =
        StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
            .toString()
}
