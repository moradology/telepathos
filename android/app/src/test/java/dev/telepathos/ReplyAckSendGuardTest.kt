package dev.telepathos

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ReplyAckSendGuardTest {
    private val socketA = Any()
    private val sourceA = ReplyAckSendContext(
        serverUrl = "wss://bridge-a.example/ws",
        token = "token-a",
        identity = ReplyAckDurability.serverIdentity("wss://bridge-a.example/ws", "token-a"),
        socketIdentity = socketA,
    )

    private fun isAllowed(
        serverUrl: String = sourceA.serverUrl,
        token: String? = sourceA.token,
        socket: Any? = socketA,
        socketConfigUrl: String? = sourceA.serverUrl,
        socketConfigToken: String? = sourceA.token,
        identity: String? = sourceA.identity,
    ) = ReplyAckSendGuard.isCurrent(
        captured = sourceA,
        currentReplyAckIdentity = identity,
        currentServerUrl = serverUrl,
        currentToken = token,
        currentSocket = socket,
        currentSocketConfigUrl = socketConfigUrl,
        currentSocketConfigToken = socketConfigToken,
    )

    @Test
    fun sendIsCancelledWhenSettingsSwitchEndpointsBeforeTheLoop() {
        assertFalse(isAllowed(
            serverUrl = "wss://bridge-b.example/ws",
            token = "token-b",
            identity = ReplyAckDurability.serverIdentity("wss://bridge-b.example/ws", "token-b"),
        ))
    }

    @Test
    fun sendIsCancelledWhenOnlyTheCredentialChanges() {
        assertFalse(isAllowed(
            token = "token-b",
            identity = ReplyAckDurability.serverIdentity(sourceA.serverUrl, "token-b"),
        ))
    }

    @Test
    fun sendIsCancelledWhenTheSocketWasReplacedDuringTheLoop() {
        assertFalse(isAllowed(socket = Any()))
    }

    @Test
    fun unchangedReconnectCanRetryItsOwnPendingReceipts() {
        assertTrue(isAllowed())
    }
}
