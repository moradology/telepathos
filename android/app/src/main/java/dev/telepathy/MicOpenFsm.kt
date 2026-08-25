package dev.telepathy

//! The mic-open choreography as an explicit state machine.
//!
//! States are the phases of opening the floor; events are the completions
//! and cancellations that move between them. Pure: `next(state, event)`
//! returns the new state; the service executes side effects at transitions.

sealed interface OpenPhase {
    /** Nothing in flight; a pinch may begin a generation. */
    data object Idle : OpenPhase

    /** Fetching pending items / lane snapshot from telepathyd. */
    data class Fetching(val generation: Long, val meta: Boolean) : OpenPhase

    /** Speaking the pending-items briefing (mic still closed). */
    data class NarratingPending(val generation: Long, val items: List<String>) : OpenPhase

    /** Speaking the templated meta status (mic still closed). */
    data class NarratingStatus(val generation: Long) : OpenPhase

    /** Mic open, cue playing — the user may talk. */
    data object Open : OpenPhase

    /** Terminal this-generation failure; back to Idle for the next pinch. */
    data class Failed(val reason: String) : OpenPhase
}

sealed interface OpenEvent {
    /** A pinch (or notification Talk) began a new generation. */
    data class Start(val generation: Long, val meta: Boolean) : OpenEvent
    /** Pending fetch completed. */
    data class PendingFetched(val items: List<String>) : OpenEvent
    /** Pending narration finished speaking. */
    data object NarrationDone : OpenEvent
    /** Meta status fetch completed (null = unreachable). */
    data class StatusFetched(val status: String?) : OpenEvent
    /** The generation lost its race: socket changed, capture cancelled. */
    data class Superseded(val reason: String) : OpenEvent
}

fun nextOpenPhase(state: OpenPhase, event: OpenEvent): OpenPhase {
    // Every state × every event, explicitly. A new event kind or phase fails
    // compilation here until its meaning is decided.
    return when (state) {
        is OpenPhase.Idle -> when (event) {
            is OpenEvent.Start -> {
                if (event.meta) OpenPhase.Fetching(event.generation, meta = true)
                else OpenPhase.Fetching(event.generation, meta = false)
            }
            is OpenEvent.PendingFetched -> state
            is OpenEvent.NarrationDone -> state
            is OpenEvent.StatusFetched -> state
            is OpenEvent.Superseded -> state
        }

        is OpenPhase.Fetching -> when (event) {
            is OpenEvent.PendingFetched ->
                if (state.meta) OpenPhase.NarratingStatus(state.generation)
                else if (event.items.isEmpty()) OpenPhase.Open
                else OpenPhase.NarratingPending(state.generation, event.items)
            is OpenEvent.StatusFetched -> state
            is OpenEvent.Superseded -> OpenPhase.Failed(event.reason)
            is OpenEvent.Start -> state // pinch during fetch: generation owns this
            is OpenEvent.NarrationDone -> state
        }

        is OpenPhase.NarratingPending -> when (event) {
            is OpenEvent.NarrationDone -> OpenPhase.Open
            is OpenEvent.Superseded -> OpenPhase.Failed(event.reason)
            is OpenEvent.Start -> state
            is OpenEvent.PendingFetched -> state
            is OpenEvent.StatusFetched -> state
        }

        is OpenPhase.NarratingStatus -> when (event) {
            is OpenEvent.NarrationDone -> OpenPhase.Open
            // status delivered = done speaking it (the driver's onDone fires this)
            is OpenEvent.StatusFetched -> OpenPhase.Open
            is OpenEvent.Superseded -> OpenPhase.Failed(event.reason)
            is OpenEvent.Start -> state
            is OpenEvent.PendingFetched -> state
        }

        is OpenPhase.Open -> when (event) {
            is OpenEvent.Superseded -> OpenPhase.Idle
            is OpenEvent.Start -> state // already open; a pinch is a no-op
            is OpenEvent.PendingFetched -> state
            is OpenEvent.NarrationDone -> state
            is OpenEvent.StatusFetched -> state
        }

        is OpenPhase.Failed -> when (event) {
            is OpenEvent.Start -> {
                if (event.meta) OpenPhase.Fetching(event.generation, meta = true)
                else OpenPhase.Fetching(event.generation, meta = false)
            }
            is OpenEvent.Superseded -> OpenPhase.Idle
            is OpenEvent.PendingFetched -> state
            is OpenEvent.NarrationDone -> state
            is OpenEvent.StatusFetched -> state
        }
    }
}


