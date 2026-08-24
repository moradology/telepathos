package dev.telepathy

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MetaCaptureArmTest {
    @Test
    fun normalStartClearsAnEarlierMetaArm() {
        val arm = MetaCaptureArm()
        arm.setForStart(true)
        arm.setForStart(false)

        assertFalse(arm.take())
    }

    @Test
    fun stopOrCancelClearTheOneShotArm() {
        val arm = MetaCaptureArm()
        arm.setForStart(true)
        arm.clear()

        assertFalse(arm.take())
    }

    @Test
    fun takingTheArmIsOneShot() {
        val arm = MetaCaptureArm()
        arm.setForStart(true)

        assertTrue(arm.take())
        assertFalse(arm.take())
    }

    @Test
    fun metaStartDuringOpenCaptureRoutesTheCurrentUtterance() {
        val arm = MetaCaptureArm()

        assertTrue(arm.setForStart(meta = true, captureOpen = true))
        assertFalse(arm.take())
    }
}
