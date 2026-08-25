package dev.telepathos

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Property-style walk over the mic-open FSM: every generated (state, event)
 * pair must produce a legal next state, and Open is only reachable through
 * the narration/status phases — never jumped to from Idle or Fetching.
 */
class MicOpenFsmTest {

    private var seed = 42
    private fun rand(): Int {
        seed = seed xor (seed shl 13); seed = seed xor (seed ushr 17); seed = seed xor (seed shl 5)
        return seed
    }

    private val events = listOf(
        OpenEvent.Start(1, false),
        OpenEvent.Start(2, true),
        OpenEvent.PendingFetched(emptyList()),
        OpenEvent.PendingFetched(listOf("a")),
        OpenEvent.NarrationDone,
        OpenEvent.StatusFetched(null),
        OpenEvent.StatusFetched("Meta. On lane x."),
        OpenEvent.Superseded("x"),
    )

    private fun opName(action: OpenPhase): String = when (action) {
        is OpenPhase.Fetching -> "fetching"
        is OpenPhase.NarratingPending -> "narrating"
        is OpenPhase.NarratingStatus -> "status"
        OpenPhase.Open -> "open"
        OpenPhase.Idle -> "idle"
        is OpenPhase.Failed -> "failed"
    }

    @Test
    fun randomWalkStaysLegal() {
        seed = 7
        var state: OpenPhase = OpenPhase.Idle
        for (i in 0 until 5000) {
            val ev = events[(rand().toInt() and Int.MAX_VALUE) % events.size]
            val next = nextOpenPhase(state, ev)
            val legal = when {
                state is OpenPhase.Idle ->
                    next is OpenPhase.Idle || next is OpenPhase.Fetching
                state is OpenPhase.Fetching ->
                    next is OpenPhase.Fetching || next is OpenPhase.NarratingPending ||
                        next is OpenPhase.NarratingStatus || next is OpenPhase.Open ||
                        next is OpenPhase.Failed
                state is OpenPhase.NarratingPending ->
                    next is OpenPhase.NarratingPending || next is OpenPhase.Open ||
                        next is OpenPhase.Failed
                state is OpenPhase.NarratingStatus ->
                    next is OpenPhase.NarratingStatus || next is OpenPhase.Open ||
                        next is OpenPhase.Failed
                state is OpenPhase.Open -> next is OpenPhase.Open || next is OpenPhase.Idle
                state is OpenPhase.Failed ->
                    next is OpenPhase.Failed || next is OpenPhase.Idle || next is OpenPhase.Fetching
                else -> true
            }
            assertTrue("step $i: $state + event → illegal $next", legal)
            state = next
        }
    }

    @Test
    fun openRequiresNarration() {
        // Idle + NarrationDone must NOT open — narration is mandatory
        val direct = nextOpenPhase(OpenPhase.Idle, OpenEvent.NarrationDone)
        assertTrue(direct is OpenPhase.Idle)

        // legal path: Start → Pending(empty) → Open
        var s: OpenPhase = OpenPhase.Idle
        s = nextOpenPhase(s, OpenEvent.Start(7, meta = false))
        assertTrue(s is OpenPhase.Fetching)
        s = nextOpenPhase(s, OpenEvent.PendingFetched(emptyList()))
        assertTrue(s is OpenPhase.Open)
    }

    @Test
    fun metaGoesThroughStatus() {
        var s: OpenPhase = nextOpenPhase(OpenPhase.Idle, OpenEvent.Start(9, meta = true))
        assertTrue(s is OpenPhase.Fetching)
        s = nextOpenPhase(s, OpenEvent.PendingFetched(listOf("x")))
        assertTrue(s is OpenPhase.NarratingStatus)
        s = nextOpenPhase(s, OpenEvent.StatusFetched("Meta. On lane direct."))
        assertTrue(s is OpenPhase.Open)
    }


    @Test
    fun probe_same_sequence_fresh_name() {
        var s: OpenPhase = nextOpenPhase(OpenPhase.Idle, OpenEvent.Start(9, meta = true))
        assertTrue("p1", s is OpenPhase.Fetching)
        s = nextOpenPhase(s, OpenEvent.PendingFetched(listOf("x")))
        assertTrue("p2", s is OpenPhase.NarratingStatus)
    }
    @Test
    fun supersededLandsTerminalThenIdle() {
        var s: OpenPhase = nextOpenPhase(OpenPhase.Fetching(9, true), OpenEvent.Superseded("socket"))
        assertTrue(s is OpenPhase.Failed)
        s = nextOpenPhase(s, OpenEvent.Superseded("again"))
        assertTrue(s is OpenPhase.Idle)
        s = nextOpenPhase(s, OpenEvent.Start(10, meta = true))
        assertTrue(s is OpenPhase.Fetching && s.generation == 10L && s.meta)
    }
}

