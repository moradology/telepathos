package dev.telepathy

import org.junit.Assert.assertFalse
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ServiceTeardownGuardTest {
    @Test
    fun teardownStopsDelayedWorkAndIsIdempotent() {
        val guard = ServiceTeardownGuard()

        assertTrue(guard.isActive())
        assertTrue(guard.beginTeardown())
        assertFalse(guard.isActive())
        assertFalse(guard.beginTeardown())
        assertFalse(guard.isActive())
    }

    @Test
    fun teardownRejectsWorkThatWasRacingWithTheFinalRemoval() {
        val guard = ServiceTeardownGuard()
        var scheduled = 0

        assertTrue(guard.runIfActive { scheduled += 1 })
        guard.beginTeardown()
        assertFalse(guard.runIfActive { scheduled += 1 })

        assertEquals(1, scheduled)
    }
}
