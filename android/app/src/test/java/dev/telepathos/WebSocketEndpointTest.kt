package dev.telepathos

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class WebSocketEndpointTest {
    @Test
    fun canonicalizesOnlySemanticallyEquivalentOkHttpEndpoints() {
        assertEquals("wss://bridge.example/", normalizeWebSocketEndpoint("WSS://BRIDGE.EXAMPLE"))
        assertTrue(equivalentWebSocketEndpoint("wss://bridge.example", "wss://BRIDGE.EXAMPLE:443/"))
        assertTrue(equivalentWebSocketEndpoint("ws://bridge.example", "ws://BRIDGE.EXAMPLE:80/"))
        assertTrue(equivalentWebSocketEndpoint("wss://bridge.example/replies", "wss://BRIDGE.EXAMPLE:443/replies"))
        assertEquals(
            "wss://bridge.example/replies/%7Edevice",
            normalizeWebSocketEndpoint("WSS://BRIDGE.EXAMPLE:443/replies/%7Edevice"),
        )
        assertEquals(
            "ws://bridge.example:8080/replies",
            normalizeWebSocketEndpoint("WS://BRIDGE.EXAMPLE:8080/replies"),
        )
        assertFalse(equivalentWebSocketEndpoint("wss://bridge.example/replies", "wss://bridge.example/other"))
        assertFalse(equivalentWebSocketEndpoint("wss://bridge.example:8443", "wss://bridge.example"))
    }

    @Test
    fun rejectsUnsupportedOrAmbiguousEndpoints() {
        listOf(
            "https://bridge.example",
            "wss://user:password@bridge.example",
            "wss://@bridge.example",
            "wss://bridge.example/path?token=secret",
            "wss://bridge.example/path#fragment",
            "not a URL",
        ).forEach { raw ->
            kotlin.runCatching { normalizeWebSocketEndpoint(raw) }
                .onSuccess { error("accepted invalid endpoint $raw as $it") }
        }
    }

    @Test
    fun startupValidationRejectsBlankMalformedAndUnsupportedPersistedEndpointsWithoutThrowing() {
        listOf(
            "",
            "   ",
            "not a URL",
            "https://bridge.example",
        ).forEach { raw ->
            val validation = validateWebSocketEndpoint(raw)
            assertTrue("expected $raw to be rejected", validation is WebSocketEndpointValidation.Invalid)
            assertNull((validation as? WebSocketEndpointValidation.Valid)?.canonicalUrl)
        }
    }

    @Test
    fun liveInvalidEndpointIsRejectedBeforeItCanReplaceTheExistingReceiptScope() {
        val existingUrl = "wss://bridge.example/replies"
        val existingIdentity = ReplyAckDurability.serverIdentity(existingUrl, "token-a")

        listOf("", "wss://", "ftp://bridge.example").forEach { replacement ->
            val candidate = validateWebSocketEndpoint(replacement)
            val selectedIdentity = when (candidate) {
                is WebSocketEndpointValidation.Valid ->
                    ReplyAckDurability.serverIdentity(candidate.canonicalUrl, "token-a")
                is WebSocketEndpointValidation.Invalid -> existingIdentity
            }
            assertEquals("invalid live endpoint must retain the current scope", existingIdentity, selectedIdentity)
        }
    }

    @Test
    fun identityUsesCanonicalEndpointAndRotatesOnTokenChange() {
        val equivalent = ReplyAckDurability.serverIdentity("WSS://BRIDGE.EXAMPLE", "token-a")
        val same = ReplyAckDurability.serverIdentity("wss://bridge.example:443/", "token-a")
        val rotated = ReplyAckDurability.serverIdentity("wss://bridge.example/", "token-b")

        assertEquals(equivalent, same)
        assertNotEquals(equivalent, rotated)
        assertFalse(equivalent.contains("token-a"))
    }
}
