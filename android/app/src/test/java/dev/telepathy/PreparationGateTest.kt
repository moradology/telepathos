package dev.telepathy

import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class PreparationGateTest {
    @Test
    fun invalidationRejectsOldSocketAndAllowsReplacement() {
        val gate = PreparationGate()
        val oldSocket = Any()
        val newSocket = Any()
        val oldGeneration = gate.begin(oldSocket)

        gate.invalidate()

        assertFalse(gate.isCurrent(oldSocket, oldGeneration!!))
        assertNotNull(gate.begin(newSocket))
    }

    @Test
    fun invalidationAllowsFreshPreparationOnSameSocket() {
        val gate = PreparationGate()
        val socket = Any()
        val oldGeneration = gate.begin(socket)

        gate.invalidate()

        assertNotNull(gate.begin(socket))
        assertFalse(gate.isCurrent(socket, oldGeneration!!))
        assertNull(gate.begin(socket))
    }
}
