package dev.telepathy

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.graphics.Typeface
import android.os.Bundle
import android.provider.Settings
import android.text.Editable
import android.text.TextWatcher
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

/**
 * Diagnostic console (features.md M1). Design rules:
 * - monospace everything; it's a terminal, not a marketing page
 * - status is a fixed set of named checks with ✓/✗ and nothing else
 * - the current interaction phase is the biggest thing on screen — it's the
 *   answer to "why is nothing happening" 90% of the time
 * - log is newest-first so live events are visible without scrolling
 */
class MainActivity : AppCompatActivity() {

    private lateinit var phaseView: TextView
    private lateinit var statusView: TextView
    private lateinit var urlInput: EditText
    private lateinit var logView: TextView

    private val green = Color.parseColor("#2E7D32")
    private val red = Color.parseColor("#C62828")
    private val dim = Color.parseColor("#666666")

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val dp = resources.displayMetrics.density
        fun px(v: Int) = (v * dp).toInt()
        val mono = Typeface.MONOSPACE

        val scroll = ScrollView(this).apply { setBackgroundColor(Color.WHITE) }
        val col = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(px(16), px(16), px(16), px(16))
        }
        scroll.addView(col)
        setContentView(scroll)

        fun label(text: String, size: Float, color: Int) = TextView(this).apply {
            this.text = text; textSize = size; setTextColor(color); typeface = mono
        }

        // header + phase
        col.addView(label("TELEPATHY", 18f, Color.BLACK))
        phaseView = TextView(this).apply {
            typeface = mono; textSize = 26f; setTextColor(Color.parseColor("#0D47A1"))
            text = "· listening ·"
        }
        col.addView(phaseView)

        col.addView(label("─── status ────────────────", 12f, dim))
        statusView = TextView(this).apply { typeface = mono; textSize = 14f }
        col.addView(statusView)

        // server control
        col.addView(label("─── server ────────────────", 12f, dim))
        urlInput = EditText(this).apply {
            typeface = mono; textSize = 13f
            hint = "ws://<host>:8787  (tailscale ok)"
            setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("server", ""))
        }
        col.addView(urlInput)
        val buttons = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL }
        val startBtn = Button(this).apply { text = "talk" }
        val stopBtn = Button(this).apply { text = "stop" }
        val assistBtn = Button(this).apply { text = "assistant…" }
        buttons.addView(startBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        buttons.addView(stopBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        buttons.addView(assistBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        col.addView(buttons)

        // gestures reference (one line each, no prose)
        col.addView(label("─── gestures ──────────────", 12f, dim))
        col.addView(label(
            "pinch      talk (wait for beep)\n" +
            "tap        capturing: send now | else: stop\n" +
            "2× tap     capturing: drop | else: stop\n" +
            "3× tap     replay last reply", 12f, Color.DKGRAY))

        // log
        col.addView(label("─── log ───────────────────", 12f, dim))
        logView = TextView(this).apply {
            typeface = mono; textSize = 11f; setTextColor(dim); setTextIsSelectable(true)
        }
        col.addView(logView)

        startBtn.setOnClickListener {
            saveUrl()
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
                != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(this, arrayOf(Manifest.permission.RECORD_AUDIO), 1)
            } else {
                Foreground.ensureChannel(this)
                startForegroundService(Intent(this, AudioCaptureService::class.java))
            }
        }
        stopBtn.setOnClickListener { stopService(Intent(this, AudioCaptureService::class.java)) }
        assistBtn.setOnClickListener { startActivity(Intent(Settings.ACTION_VOICE_INPUT_SETTINGS)) }
        urlInput.addTextChangedListener(object : TextWatcher {
            override fun afterTextChanged(s: Editable?) { refreshStatus() }
            override fun beforeTextChanged(p0: CharSequence?, p1: Int, p2: Int, p3: Int) {}
            override fun onTextChanged(p0: CharSequence?, p1: Int, p2: Int, p3: Int) {}
        })

        refreshAll()
        LinkState.onChange { runOnUiThread { refreshAll() } }
        LinkState.onPhaseChange { runOnUiThread { refreshAll() } }
        TriggerLog.onChange { runOnUiThread { refreshLog() } }
    }

    override fun onResume() {
        super.onResume()
        refreshAll() // user may have toggled assistant setting and come back
    }

    private fun saveUrl() {
        getSharedPreferences("cfg", MODE_PRIVATE).edit()
            .putString("server", urlInput.text.toString().trim()).apply()
    }

    private fun refreshAll() = refreshStatus().also { refreshLog() }

    private fun refreshStatus(): Unit {
        val s = LinkState.current
        val checks = listOf(
            "mic permission" to (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
                    == PackageManager.PERMISSION_GRANTED),
            "assistant default" to AssistantChecks.isDefaultAssistant(this),
            "server link" to s.wsUp,
            "earbuds" to s.budsOn,
        )
        statusView.text = checks.joinToString("\n") { (name, ok) ->
            val mark = if (ok) "✓" else "✗"
            "$mark $name"
        }
        statusView.setTextColor(if (checks.all { it.second }) green else red)

        phaseView.text = "· ${LinkState.phase} ·"
        phaseView.setTextColor(when (LinkState.phase) {
            "listening" -> green
            "capturing" -> Color.parseColor("#E65100")   // recording: attention
            "processing", "echoing" -> Color.parseColor("#0D47A1")
            "speaking" -> Color.parseColor("#6A1B9A")
            else -> dim
        })
    }

    override fun onRequestPermissionsResult(
        requestCode: Int, permissions: Array<out String>, grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == 1 && grantResults.firstOrNull() == PackageManager.PERMISSION_GRANTED) {
            Foreground.ensureChannel(this)
            startForegroundService(Intent(this, AudioCaptureService::class.java))
        }
        refreshAll()
    }

    private fun refreshLog() {
        logView.text = TriggerLog.load(this).ifEmpty { "(no events yet)" }
    }
}
