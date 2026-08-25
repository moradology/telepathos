package dev.telepathos

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class PendingConsumeGuardTest {
    private val socketA = Any()
    private val sourceA = PendingConsumeContext(
        apiBaseUrl = "https://bridge-a.example",
        token = "token-a",
        configuredSocketUrl = "wss://bridge-a.example/ws",
        socketIdentity = socketA,
        socketUrl = "wss://bridge-a.example/ws",
        socketToken = "token-a",
    )

    @Test
    fun deferredConsumeIsCancelledWhenEndpointChangesAfterNarration() {
        val allowed = PendingConsumeGuard.isCurrent(
            captured = sourceA,
            currentApiBaseUrl = "https://bridge-b.example",
            currentToken = "token-b",
            currentSocketUrl = "wss://bridge-b.example/ws",
            // The configuration can change before the service has torn down
            // the old websocket, which is the reported cross-endpoint race.
            currentSocket = socketA,
            currentSocketConfigUrl = sourceA.socketUrl,
            currentSocketConfigToken = sourceA.socketToken,
        )

        assertFalse(allowed)
    }

    @Test
    fun deferredConsumeIsCancelledWhenOnlyCredentialsChange() {
        val allowed = PendingConsumeGuard.isCurrent(
            captured = sourceA,
            currentApiBaseUrl = sourceA.apiBaseUrl,
            currentToken = "token-b",
            currentSocketUrl = sourceA.configuredSocketUrl,
            currentSocket = socketA,
            currentSocketConfigUrl = sourceA.socketUrl,
            currentSocketConfigToken = sourceA.socketToken,
        )

        assertFalse(allowed)
    }

    @Test
    fun deferredConsumeRejectsAnOldSocketAfterSettingsChangeBeforeTeardown() {
        val settingsBOnSocketA = PendingConsumeContext(
            apiBaseUrl = "https://bridge-b.example",
            token = "token-b",
            configuredSocketUrl = "wss://bridge-b.example/ws",
            socketIdentity = socketA,
            socketUrl = sourceA.socketUrl,
            socketToken = sourceA.socketToken,
        )

        assertFalse(PendingConsumeGuard.isCurrent(
            captured = settingsBOnSocketA,
            currentApiBaseUrl = settingsBOnSocketA.apiBaseUrl,
            currentToken = settingsBOnSocketA.token,
            currentSocketUrl = settingsBOnSocketA.configuredSocketUrl,
            currentSocket = socketA,
            currentSocketConfigUrl = sourceA.socketUrl,
            currentSocketConfigToken = sourceA.socketToken,
        ))
    }

    @Test
    fun deferredConsumeRequiresTheOriginalSocketEvenWhenSettingsMatch() {
        val allowed = PendingConsumeGuard.isCurrent(
            captured = sourceA,
            currentApiBaseUrl = sourceA.apiBaseUrl,
            currentToken = sourceA.token,
            currentSocketUrl = sourceA.configuredSocketUrl,
            currentSocket = Any(),
            currentSocketConfigUrl = sourceA.socketUrl,
            currentSocketConfigToken = sourceA.socketToken,
        )

        assertFalse(allowed)
    }

    @Test
    fun deferredConsumeAllowsItsUnchangedCapturedEndpointAndSocket() {
        val allowed = PendingConsumeGuard.isCurrent(
            captured = sourceA,
            currentApiBaseUrl = sourceA.apiBaseUrl,
            currentToken = sourceA.token,
            currentSocketUrl = sourceA.configuredSocketUrl,
            currentSocket = socketA,
            currentSocketConfigUrl = sourceA.socketUrl,
            currentSocketConfigToken = sourceA.socketToken,
        )

        assertTrue(allowed)
    }

    @Test
    fun standaloneCaptureRemainsValidWithoutALaneApiEndpoint() {
        val socket = Any()
        val standalone = PendingConsumeContext(
            apiBaseUrl = null,
            token = null,
            configuredSocketUrl = "wss://bridge.example/ws",
            socketIdentity = socket,
            socketUrl = "wss://bridge.example/ws",
            socketToken = null,
        )

        assertTrue(PendingConsumeGuard.isCurrent(
            captured = standalone,
            currentApiBaseUrl = null,
            currentToken = null,
            currentSocketUrl = standalone.configuredSocketUrl,
            currentSocket = socket,
            currentSocketConfigUrl = standalone.socketUrl,
            currentSocketConfigToken = standalone.socketToken,
        ))
    }
}
