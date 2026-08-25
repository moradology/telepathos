package dev.telepathos

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class LocalAnnouncerTest {
    @Test
    fun completionAndFailureHaveExactlyOneOwner() {
        val callbacks = LocalAnnouncerReplyCallbacks()
        var done = 0
        var failed = 0

        callbacks.register("reply-1", { done += 1 }, { failed += 1 })

        val completion = callbacks.complete("reply-1", succeeded = true)
        assertTrue(completion != null)
        completion!!.invoke()

        assertNull(callbacks.complete("reply-1", succeeded = false))
        assertEquals(1, done)
        assertEquals(0, failed)
    }

    @Test
    fun failureCompletionWinsAndDoneIsDiscarded() {
        val callbacks = LocalAnnouncerReplyCallbacks()
        var done = 0
        var failed = 0

        callbacks.register("reply-1", { done += 1 }, { failed += 1 })

        val completion = callbacks.complete("reply-1", succeeded = false)
        assertTrue(completion != null)
        completion!!.invoke()

        assertNull(callbacks.complete("reply-1", succeeded = true))
        assertEquals(0, done)
        assertEquals(1, failed)
    }

    @Test
    fun stopDiscardRemovesBothPossibleOutcomes() {
        val callbacks = LocalAnnouncerReplyCallbacks()
        callbacks.register("reply-1", {}, {})

        callbacks.clear()

        assertNull(callbacks.complete("reply-1", succeeded = true))
        assertNull(callbacks.complete("reply-1", succeeded = false))
    }

    @Test
    fun completionCallbackCanReenterWithoutRunningUnderTheAnnouncerLock() {
        val dispatcher = LocalAnnouncerCallbackDispatcher()
        val events = mutableListOf<String>()
        var callbackHeldLock = false

        dispatcher.enqueue {
            callbackHeldLock = Thread.holdsLock(dispatcher.lock)
            events += "first"
            dispatcher.enqueue { events += "reentrant" }
        }

        assertFalse(callbackHeldLock)
        assertEquals(listOf("first", "reentrant"), events)
    }

    @Test
    fun callbackQueuedWhileTheLockIsHeldIsDrainedAfterRelease() {
        val dispatcher = LocalAnnouncerCallbackDispatcher()
        val events = mutableListOf<String>()

        synchronized(dispatcher.lock) {
            dispatcher.enqueueLocked { events += "done" }
            dispatcher.drain()
            assertTrue(events.isEmpty())
        }
        dispatcher.drain()

        assertEquals(listOf("done"), events)
    }
}
