package dev.telepathos

import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicReference

/**
 * Connection state as an immutable value (features.md M4).
 * Updates are pure transformations; observers receive the new snapshot,
 * so UI code never reads possibly-stale globals.
 */
data class ConnectionState(
    val wsUp: Boolean = false,
    val budsOn: Boolean = false,
) {
    /** Human summary; the notification and the setup screen share this exact wording. */
    val summary: String
        get() = when {
            wsUp && budsOn -> "connected · buds + server"
            wsUp -> "server only — no earbuds detected"
            budsOn -> "earbuds only — server unreachable"
            else -> "reconnecting…"
        }
}

object LinkState {

    private val ref = AtomicReference(ConnectionState())
    private val listeners = CopyOnWriteArraySet<(ConnectionState) -> Unit>()

    /** Latest interaction phase broadcast by the server ("listening", "capturing", …). */
    @Volatile var phase: String = "—"
        private set

    val current: ConnectionState get() = ref.get()

    fun onChange(fn: (ConnectionState) -> Unit) = listeners.add(fn)
    fun onPhaseChange(fn: (String) -> Unit) = phaseListeners.add(fn)
    private val phaseListeners = CopyOnWriteArraySet<(String) -> Unit>()

    fun setWs(up: Boolean) = update { it.copy(wsUp = up) }
    fun setBuds(on: Boolean) = update { it.copy(budsOn = on) }

    fun setPhase(p: String) {
        if (phase != p) { phase = p; phaseListeners.forEach { it(p) } }
    }

    private fun update(transform: (ConnectionState) -> ConnectionState) {
        var changed = false
        ref.updateAndGet { prev ->
            val next = transform(prev)
            changed = next != prev
            next
        }
        if (changed) listeners.forEach { it(ref.get()) }
    }
}
