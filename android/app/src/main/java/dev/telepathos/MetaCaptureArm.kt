package dev.telepathos

/** One-shot meta-plane selection for the next capture attempt. */
internal class MetaCaptureArm {
    @Volatile private var armed = false

    /** Returns true when an already-open capture should receive meta mode now. */
    fun setForStart(meta: Boolean, captureOpen: Boolean = false): Boolean {
        if (meta && captureOpen) {
            armed = false
            return true
        }
        armed = meta
        return false
    }

    fun clear() {
        armed = false
    }

    fun isArmed(): Boolean = armed

    fun take(): Boolean {
        val wasArmed = armed
        armed = false
        return wasArmed
    }
}
