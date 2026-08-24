package dev.telepathy

/**
 * Owns one asynchronous pending-delivery preparation at a time.
 *
 * The owner is the WebSocket instance and the generation also changes when a
 * preparation is invalidated, so an old fetch/TTS callback cannot affect a
 * replacement socket or a newer capture on the same socket.
 */
internal class PreparationGate {
    private val lock = Any()
    private var owner: Any? = null
    private var generation = 0L

    fun begin(nextOwner: Any): Long? = synchronized(lock) {
        if (owner === nextOwner) return@synchronized null
        generation += 1
        owner = nextOwner
        generation
    }

    fun isCurrent(currentOwner: Any, currentGeneration: Long): Boolean =
        synchronized(lock) {
            owner === currentOwner && generation == currentGeneration
        }

    fun finish(currentOwner: Any, currentGeneration: Long) = synchronized(lock) {
        if (owner === currentOwner && generation == currentGeneration) {
            owner = null
        }
    }

    fun invalidate() = synchronized(lock) {
        generation += 1
        owner = null
    }
}
