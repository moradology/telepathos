package dev.telepathy

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.graphics.Typeface
import android.os.Bundle
import android.provider.Settings
import android.text.Editable
import android.text.InputType
import android.text.TextWatcher
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.Spinner
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
    private lateinit var telepathydInput: EditText
    private lateinit var tokenInput: EditText
    private lateinit var profileSpinner: Spinner
    private var profiles: MutableMap<String, ConnectionProfiles.Profile> = linkedMapOf()
    private var activeProfile: String = ConnectionProfiles.DEFAULT_NAME
    private var suppressSpinner = false
    private lateinit var logView: TextView
    private lateinit var lanesView: TextView

    private val mainHandler = android.os.Handler(android.os.Looper.getMainLooper())
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

        // server control — connection profiles
        col.addView(label("─── connection ─────────────", 12f, dim))
        profileSpinner = Spinner(this).apply { gravity = android.view.Gravity.CENTER }
        col.addView(profileSpinner)
        val profileButtons = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL }
        val saveProfileBtn = Button(this).apply { text = "save" }
        val newProfileBtn = Button(this).apply { text = "new" }
        profileButtons.addView(saveProfileBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        profileButtons.addView(newProfileBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        col.addView(profileButtons)
        urlInput = EditText(this).apply {
            typeface = mono; textSize = 13f
            hint = "wss://<host>:8787  (token mode)"
            setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("server", ""))
        }
        col.addView(urlInput)
        col.addView(label("telepathyd base URL", 12f, dim))
        telepathydInput = EditText(this).apply {
            typeface = mono; textSize = 13f
            hint = "https://<host>:8790  (token mode)"
            setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("hermes", ""))
        }
        col.addView(telepathydInput)
        col.addView(label("bridge token (optional)", 12f, dim))
        tokenInput = EditText(this).apply {
            typeface = mono; textSize = 13f
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD
            hint = "TELEPATHY_TOKEN"
            setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("token", ""))
        }
        col.addView(tokenInput)
        ConnectionProfiles.applyActive(this)
        urlInput.setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("server", ""))
        telepathydInput.setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("hermes", ""))
        tokenInput.setText(getSharedPreferences("cfg", MODE_PRIVATE).getString("token", ""))
        loadProfiles()
        saveProfileBtn.setOnClickListener { saveCurrentInto(activeProfile); refreshProfiles() }
        newProfileBtn.setOnClickListener {
            val input = EditText(this).apply { hint = "profile name" }
            android.app.AlertDialog.Builder(this)
                .setTitle("New connection profile")
                .setView(input)
                .setPositiveButton("Create") { _, _ ->
                    val name = input.text.toString().trim()
                    if (name.isNotEmpty()) {
                        saveCurrentInto(name)
                        activeProfile = name
                        refreshProfiles()
                    }
                }
                .setNegativeButton("Cancel", null)
                .show()
        }
        val buttons = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL }
        val startBtn = Button(this).apply { text = "talk" }
        val stopBtn = Button(this).apply { text = "stop" }
        val assistBtn = Button(this).apply { text = "assistant…" }
        buttons.addView(startBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        buttons.addView(stopBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        buttons.addView(assistBtn, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        col.addView(buttons)

        // gestures reference (one line each, no prose)
        col.addView(label("─── lanes ─────────────────", 12f, dim))
        lanesView = TextView(this).apply {
            typeface = mono; textSize = 12f
            setOnClickListener {
                LaneStore.cycle(this@MainActivity)
                mainHandler.postDelayed({ refreshLanes() }, 800)
            }
        }
        col.addView(lanesView)

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
            if (!saveUrl()) return@setOnClickListener
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
        refreshLanes()
        LinkState.onChange { runOnUiThread { refreshAll() } }
        LinkState.onPhaseChange { runOnUiThread { refreshAll() } }
        TriggerLog.onChange { runOnUiThread { refreshLog() } }
    }

    override fun onResume() {
        super.onResume()
        refreshAll()
        refreshLanes() // user may have toggled settings and come back
    }

    private fun loadProfiles() {
        profiles = ConnectionProfiles.load(this)
        activeProfile = ConnectionProfiles.activeName(this)
        refreshProfiles()
        val p = profiles[activeProfile] ?: return
        // populate fields from the ACTIVE profile (fields may have been edited
        // elsewhere; profile is source of truth on entry)
        urlInput.setText(p.server)
        telepathydInput.setText(p.telepathyd)
        tokenInput.setText(p.token)
    }

    private fun refreshProfiles() {
        suppressSpinner = true
        profileSpinner.adapter = android.widget.ArrayAdapter(
            this, android.R.layout.simple_spinner_dropdown_item,
            profiles.keys.toList().map { if (it == activeProfile) "★ $it" else it }
        )
        val names = profiles.keys.toList()
        profileSpinner.setSelection(names.indexOf(activeProfile).coerceAtLeast(0))
        suppressSpinner = false
        profileSpinner.onItemSelectedListener = object : android.widget.AdapterView.OnItemSelectedListener {
            override fun onItemSelected(parent: android.widget.AdapterView<*>?, view: android.view.View?, pos: Int, id: Long) {
                if (suppressSpinner) return
                val name = profiles.keys.toList().getOrNull(pos) ?: return
                if (name != activeProfile) {
                    activeProfile = name
                    ConnectionProfiles.save(this@MainActivity, profiles, activeProfile)
                    ConnectionProfiles.applyActive(this@MainActivity)
                    val p = profiles[name] ?: return
                    urlInput.setText(p.server)
                    telepathydInput.setText(p.telepathyd)
                    tokenInput.setText(p.token)
                    refreshStatus()
                }
            }
            override fun onNothingSelected(parent: android.widget.AdapterView<*>?) {}
        }
    }

    private fun saveCurrentInto(name: String) {
        profiles[name] = ConnectionProfiles.Profile(
            server = urlInput.text.toString().trim(),
            telepathyd = telepathydInput.text.toString().trim(),
            token = tokenInput.text.toString().trim(),
        )
        ConnectionProfiles.save(this, profiles, activeProfile)
    }

    private fun saveUrl(): Boolean {
        val endpoint = when (val validation = validateWebSocketEndpoint(urlInput.text.toString().trim())) {
            is WebSocketEndpointValidation.Valid -> validation.canonicalUrl
            is WebSocketEndpointValidation.Invalid -> {
                urlInput.error = "Invalid server address: ${validation.reason}"
                return false
            }
        }
        getSharedPreferences("cfg", MODE_PRIVATE).edit()
            .putString("server", endpoint)
            .putString("hermes", telepathydInput.text.toString().trim())
            .putString("token", tokenInput.text.toString().trim())
            .apply()
        saveCurrentInto(activeProfile)
        urlInput.error = null
        if (urlInput.text.toString() != endpoint) urlInput.setText(endpoint)
        return true
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

    /** Lane list from telepathyd; tap a lane row to switch to it. */
    private fun refreshLanes() {
        Thread {
            val state = LaneStore.fetchState(this)
            runOnUiThread {
                if (state == null) {
                    lanesView.setTextColor(dim)
                    lanesView.text = "(server unreachable)"
                    return@runOnUiThread
                }
                val (lanes, _) = state
                lanesView.text = lanes.joinToString("\n") { l ->
                    val mark = if (l.active) "▸" else " "
                    val badge = if (l.pending > 0) "  📌${l.pending}" else ""
                    val t = l.title?.let { " — $it" } ?: ""
                    "$mark ${l.name}$t$badge"
                }
                lanesView.setTextColor(Color.BLACK)
            }
        }.start()
    }

    private fun refreshLog() {
        logView.text = TriggerLog.load(this).ifEmpty { "(no events yet)" }
    }
}
