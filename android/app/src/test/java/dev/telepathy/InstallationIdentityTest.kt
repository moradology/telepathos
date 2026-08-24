package dev.telepathy

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class InstallationIdentityTest {
    @Test
    fun generatedIdentityIsOpaqueRandomAndWithinTheWireLimit() {
        val first = InstallationIdentity.generate()
        val second = InstallationIdentity.generate()

        assertTrue(InstallationIdentity.isValid(first))
        assertTrue(InstallationIdentity.isValid(second))
        assertEquals(22, first.length)
        assertNotEquals(first, second)
        assertFalse(first.contains("opendots2-pixel9"))
    }

    @Test
    fun persistedIdentityIsReusedAndNeverRegenerated() {
        var persisted: String? = null
        val generated = "persisted-random-owner"

        val first = InstallationIdentity.loadOrCreate(
            current = persisted,
            generate = { generated },
            persist = { value -> persisted = value; true },
        )
        val second = InstallationIdentity.loadOrCreate(
            current = persisted,
            generate = { error("a valid persisted identity must be reused") },
            persist = { error("a valid persisted identity must not be rewritten") },
        )

        assertEquals(generated, first)
        assertEquals(first, persisted)
        assertEquals(first, second)
    }

    @Test
    fun validCopiedIdentityIsRotatedWhenTheKeystoreSentinelIsMissing() {
        var persisted: String? = "copied-owner"

        val rotated = InstallationIdentity.loadOrCreate(
            current = persisted,
            sentinelState = InstallationIdentity.SentinelState.Missing,
            generate = { "fresh-owner" },
            persist = { value -> persisted = value; true },
        )

        assertEquals("fresh-owner", rotated)
        assertEquals("fresh-owner", persisted)
    }

    @Test
    fun identityDecisionRequiresBothAValidOwnerAndTheDeviceSentinel() {
        assertFalse(
            InstallationIdentity.shouldGenerate(
                "existing-owner",
                InstallationIdentity.SentinelState.Present,
            ),
        )
        assertTrue(
            InstallationIdentity.shouldGenerate(
                "existing-owner",
                InstallationIdentity.SentinelState.Missing,
            ),
        )
        assertTrue(
            InstallationIdentity.shouldGenerate(
                null,
                InstallationIdentity.SentinelState.Present,
            ),
        )
    }

    @Test
    fun helloCarriesInstallationIdentitySeparatelyFromHumanDeviceLabel() {
        val json = ClientHello(
            installationId = "opaque-random-owner",
            deviceLabel = "human-pixel-label",
            token = "token-1",
        ).toJson()

        assertTrue(json.contains("\"type\":\"hello\""))
        assertTrue(json.contains("\"device\":\"human-pixel-label\""))
        assertTrue(json.contains("\"installation_id\":\"opaque-random-owner\""))
        assertTrue(json.contains("\"token\":\"token-1\""))
        assertNotEquals("human-pixel-label", "opaque-random-owner")
    }

    @Test
    fun validationMatchesWireLengthWhitespaceAndControlRules() {
        assertTrue(InstallationIdentity.isValid(" owner-with-surrounding-space "))
        assertTrue(InstallationIdentity.isValid("x".repeat(128)))
        assertFalse(InstallationIdentity.isValid("x".repeat(129)))

        val cases = listOf(
            "" to false,
            " " to false,
            "\t" to false,
            "\n" to false,
            "\u000b" to false,
            "\u000c" to false,
            "\r" to false,
            "\u0085" to false,
            "\u00a0" to false,
            "\u1680" to false,
            "\u2007" to false,
            "\u202f" to false,
            "\u3000" to false,
            "\ufeff" to false,
            "\u0000" to false,
            "\u001f" to false,
            "\u007f" to false,
            "\u009f" to false,
            "owner" to true,
            " owner " to true,
            "\u00a0owner\u00a0" to true,
            "\u2007owner\u2007" to true,
            "\u202fowner\u202f" to true,
            "\ufeffowner\ufeff" to true,
            "owner\t" to false,
            "owner\u0085" to false,
            "owner\u0000" to false,
        )
        cases.forEach { (value, expected) ->
            assertEquals("unexpected installation-ID validity for ${value.toCharArray().toList()}", expected, InstallationIdentity.isValid(value))
        }
    }

    @Test
    fun installationIdsRejectLoneUtf16SurrogatesBeforeHelloSerialization() {
        listOf("\uD800", "\uDC00").forEach { value ->
            assertFalse(InstallationIdentity.isValid(value))
            try {
                ClientHello(installationId = value)
                fail("accepted malformed installation ID")
            } catch (_: IllegalArgumentException) {
                // Expected: malformed UTF-16 must not reach the wire serializer.
            }
        }
    }
}
