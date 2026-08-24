package dev.telepathy

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SocketOpenGuardTest {
    private val socketA = Any()
    private val socketB = Any()
    private val openA = SocketOpenContext(
        serverUrl = "wss://bridge-a.example/ws",
        token = "token-a",
        socketIdentity = socketA,
        generation = 41,
    )

    private fun isCurrent(
        context: SocketOpenContext = openA,
        serverUrl: String = context.serverUrl,
        token: String? = context.token,
        socket: Any? = context.socketIdentity,
        socketConfigUrl: String? = context.serverUrl,
        socketConfigToken: String? = context.token,
        generation: Long = context.generation,
        wantsConnection: Boolean = true,
    ) = SocketOpenGuard.isCurrent(
        captured = context,
        currentServerUrl = serverUrl,
        currentToken = token,
        currentSocket = socket,
        currentSocketConfigUrl = socketConfigUrl,
        currentSocketConfigToken = socketConfigToken,
        currentGeneration = generation,
        wantsConnection = wantsConnection,
    )

    @Test
    fun delayedOpenCannotPublishAfterSettingsInstallsAnotherSocket() {
        // A passes its original onOpen validation, then the settings switch
        // advances the generation and installs B before A resumes.
        assertTrue(isCurrent())

        assertFalse(isCurrent(
            serverUrl = "wss://bridge-b.example/ws",
            token = "token-b",
            socket = socketB,
            socketConfigUrl = "wss://bridge-b.example/ws",
            socketConfigToken = "token-b",
            generation = 42,
        ))
    }

    @Test
    fun delayedOpenCannotPublishAfterSameEndpointReconnectReplacesTheSocket() {
        assertFalse(isCurrent(socket = socketB, generation = 42))
    }

    @Test
    fun currentReconnectCanPublishAndStartItsOwnTraffic() {
        val reconnect = openA.copy(socketIdentity = socketB, generation = 42)

        assertTrue(isCurrent(
            context = reconnect,
            socket = socketB,
            generation = 42,
        ))
    }

    @Test
    fun readyCannotOpenTrafficBeforeThisGenerationQueuedHello() {
        // Model the deterministic bad interleaving: an old ready arrives
        // while this listener has not queued its hello, then the current
        // listener queues hello and receives its own ready.
        assertFalse(HelloReadinessGuard.canPublish(
            helloQueued = false,
            readyReceived = true,
            contextIsCurrent = true,
        ))
        assertFalse(HelloReadinessGuard.canPublish(
            helloQueued = true,
            readyReceived = true,
            contextIsCurrent = false,
        ))
        assertTrue(HelloReadinessGuard.canPublish(
            helloQueued = true,
            readyReceived = true,
            contextIsCurrent = true,
        ))
    }
}
