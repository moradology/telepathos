package dev.telepathy

import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody
import okhttp3.MediaType
import okio.Buffer
import okio.BufferedSource
import okio.ForwardingSource
import okio.buffer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class BoundedHttpResponseTest {
    @Test
    fun unknownChunkedLengthIsReadInSmallChunksAndPreservesSplitUtf8() {
        val body = FakeResponseBody("A🦀Z".toByteArray(), advertisedLength = -1, maxRead = 1)

        assertEquals(
            "A🦀Z",
            BoundedHttpResponse.readUtf8(response(body), maxBytes = 16),
        )
        assertTrue(body.closed)
    }

    @Test
    fun exactCapSucceedsButIncrementalOverCapFails() {
        val exact = FakeResponseBody("12345".toByteArray(), advertisedLength = -1)
        val over = FakeResponseBody("123456".toByteArray(), advertisedLength = -1)

        assertEquals("12345", BoundedHttpResponse.readUtf8(response(exact), maxBytes = 5))
        assertNull(BoundedHttpResponse.readUtf8(response(over), maxBytes = 5))
        assertTrue(exact.closed)
        assertTrue(over.closed)
    }

    @Test
    fun knownOversizeContentLengthFailsBeforeReadingAndCloses() {
        val body = FakeResponseBody("ignored".toByteArray(), advertisedLength = 6)

        assertNull(BoundedHttpResponse.readUtf8(response(body), maxBytes = 5))
        assertEquals(0, body.readCalls)
        assertTrue(body.closed)
    }

    @Test
    fun smallerLyingContentLengthStillUsesIncrementalCap() {
        val body = FakeResponseBody("valid".toByteArray(), advertisedLength = 1)

        assertEquals("valid", BoundedHttpResponse.readUtf8(response(body), maxBytes = 5))
        assertTrue(body.closed)
    }

    @Test
    fun malformedUtf8FailsWithoutReplacementAndCloses() {
        val body = FakeResponseBody(byteArrayOf(0xc3.toByte(), 0x28), advertisedLength = -1)

        assertNull(BoundedHttpResponse.readUtf8(response(body), maxBytes = 8))
        assertTrue(body.closed)
    }

    @Test
    fun cancellationStopsReadAndClosesResponse() {
        val body = FakeResponseBody("cancelled".toByteArray(), advertisedLength = -1)

        assertNull(
            BoundedHttpResponse.readUtf8(
                response(body),
                maxBytes = 32,
                isCancelled = { true },
            ),
        )
        assertEquals(0, body.readCalls)
        assertTrue(body.closed)
    }

    @Test
    fun malformedJsonFailsAndValidJsonSucceeds() {
        val malformed = FakeResponseBody("{bad".toByteArray(), advertisedLength = -1)
        val valid = FakeResponseBody("{\"ok\":true}".toByteArray(), advertisedLength = -1)

        assertNull(BoundedHttpResponse.readJsonObject(response(malformed), maxBytes = 32))
        val parsed = BoundedHttpResponse.readJsonObject(response(valid), maxBytes = 32)
        assertTrue(parsed?.optBoolean("ok", false) == true)
        assertTrue(malformed.closed)
        assertTrue(valid.closed)
    }

    private fun response(body: FakeResponseBody): Response = Response.Builder()
        .request(Request.Builder().url("http://localhost/").build())
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body(body)
        .build()

    private class FakeResponseBody(
        bytes: ByteArray,
        private val advertisedLength: Long,
        private val maxRead: Int = Int.MAX_VALUE,
    ) : ResponseBody() {
        var readCalls = 0
        var closed = false
        private val sourceBuffer = Buffer().write(bytes)
        private val forwardingSource = object : ForwardingSource(sourceBuffer) {
            override fun read(sink: Buffer, byteCount: Long): Long {
                readCalls++
                return super.read(sink, minOf(byteCount, maxRead.toLong()))
            }

            override fun close() {
                closed = true
                super.close()
            }
        }
        private val bufferedSource: BufferedSource = forwardingSource.buffer()

        override fun contentType(): MediaType? = null
        override fun contentLength(): Long = advertisedLength
        override fun source(): BufferedSource = bufferedSource
    }
}
