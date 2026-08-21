package dev.telepathy

import android.Manifest
import android.content.ComponentName
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

/**
 * Setup doctor (features.md M1): every known failure mode gets a live status row.
 * Plus the mic-capture controls and the event log.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var logView: TextView
    private lateinit var urlInput: EditText
    private lateinit var statusView: TextView
    private val rows = mutableListOf<String>()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val pad = (16 * resources.displayMetrics.density).toInt()
        val root = ScrollView(this)
        val col = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(pad, pad, pad, pad)
        }
        root.addView(col)
        setContentView(root)

        statusView = TextView(this).apply { textSize = 14f }
        urlInput = EditText(this).apply {
            hint = "ws://<mac-ip>:8787"
            setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("server", ""))
        }
        val startBtn = Button(this).apply { text = "Start listening" }
        val stopBtn = Button(this).apply { text = "Stop" }
        val assistBtn = Button(this).apply { text = "Open assistant settings" }
        logView = TextView(this).apply { setTextIsSelectable(true); textSize = 12f }

        col.addView(statusView)
        col.addView(urlInput)
        col.addView(startBtn)
        col.addView(stopBtn)
        col.addView(assistBtn)
        col.addView(TextView(this).apply {
            text = """
                Shokz app checklist:
                • disable Smart Wear Detection while testing
                  (controls are dead unless the bud thinks it's seated!)
                • map pinch → voice assistant
                • taps → next / previous / play-pause
            """.trimIndent()
            textSize = 13f
        })
        col.addView(logView)

        startBtn.setOnClickListener {
            getSharedPreferences("cfg", MODE_PRIVATE).edit()
                .putString("server", urlInput.text.toString().trim()).apply()
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
                != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(this, arrayOf(Manifest.permission.RECORD_AUDIO), 1)
            } else {
                Foreground.ensureChannel(this)
                startForegroundService(Intent(this, AudioCaptureService::class.java))
            }
        }
        stopBtn.setOnClickListener { stopService(Intent(this, AudioCaptureService::class.java)) }
        assistBtn.setOnClickListener {
            startActivity(Intent(Settings.ACTION_VOICE_INPUT_SETTINGS))
        }

        refreshStatus()
        LinkState.onChange { runOnUiThread { refreshStatus() } }
        TriggerLog.onChange { runOnUiThread { refreshLog() } }
    }

    override fun onResume() {
        super.onResume()
        refreshStatus() // user may have toggled the assistant setting and come back
    }

    private fun refreshStatus() {
        val s = LinkState.current
        rows.clear()
        rows.add(if (AssistantChecks.isDefaultAssistant(this))
            "✓ default digital assistant" else "✗ NOT default digital assistant")
        rows.add(if (s.wsUp) "✓ server connected" else "✗ server not connected")
        rows.add(if (s.budsOn) "✓ earbuds detected" else "✗ no earbuds detected")

        // mic permission is a precondition for everything else
        val micOk = ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO) ==
                PackageManager.PERMISSION_GRANTED
        rows.add(if (micOk) "✓ microphone permission" else "✗ no microphone permission")

        statusView.text = "TELEPATHY — setup\n\n" + rows.joinToString("\n")
    }

    override fun onRequestPermissionsResult(
        requestCode: Int, permissions: Array<out String>, grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == 1 && grantResults.firstOrNull() == PackageManager.PERMISSION_GRANTED) {
            Foreground.ensureChannel(this)
            startForegroundService(Intent(this, AudioCaptureService::class.java))
        }
        refreshStatus()
    }

    private fun refreshLog() {
        logView.text = "Events:\n${TriggerLog.load(this).ifEmpty { "(none yet)" }}"
        refreshStatus()
    }
}
