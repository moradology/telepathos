# Feature plan — post-research checklist

Source context: OpenDots 2 research (Shokz product pages, support docs, reviews).
Each item lists WHY it exists so future-us doesn't have to reverse-engineer intent.

## Must-support

### [M1] Setup doctor flow — IMPLEMENTED (assistant check + live link status + Shokz checklist in-app)
**Why:** Known failure modes produce identical symptoms ("nothing happens"):
1. Smart Wear Detection blocks controls unless bud is seated (Shokz support confirms)
2. Telepathy not set as default digital assistant
3. Shokz app gesture not mapped to voice assistant
4. Server unreachable (wrong URL / Mac asleep / LAN changed)
**Done when:** App shows live status for each check; user can tap-to-fix where possible
(assistant setting deep-link), see WS connectivity in real time, and get a hint for
Shokz-app-only steps.

### [M2] Mono operation — INVARIANT HELD (all AudioTrack/TTS paths are CHANNEL_OUT_MONO)
**Why:** Either bud works in either ear (Dynamic Ear Detection); one-bud wear is a core
use case, likely OUR core case (other ear free).
**Invariant:** All playback mono (CHANNEL_OUT_MONO), no left/right meaning, no stereo cues.
**Done when:** Invariant holds everywhere audio is played; noted in code.

### [M3] Gesture vocabulary — IMPLEMENTED (MediaSession → stop/repeat/cancel_capture; server honors all three incl. mid-TTS cancel & replay). NOTE: whether buds deliver keys while SCO-active needs hardware test
**Why:** Five distinct physical inputs exist (tap, 2×tap, 3×tap, pinch, pinch-hold);
muscle memory gets built around whatever we ship first.
**Mapping (defaults), phase-aware (see ClientCommand.fromMediaKey):**
- pinch (assist trigger) → SCO up → "go" cue → mic opens; talk when you hear it
- tap (media PLAY/PAUSE): capturing → send now (flush); otherwise → stop agent
- double-tap (media NEXT): capturing → drop utterance; otherwise → stop agent
- triple-tap (media PREV) → replay last reply
Cues: beep on mic-live ("talk"), pip on utterance-sent ("thinking").
Earbud taps surface as AVRCP media keys on the phone; a MediaSession receives them.
**Done when:** Media keys while the service runs produce `{"type":"command",...}` frames;
server acknowledges them in logs; replay-last-reply works end-to-end.

### [M4] Connection-state awareness — IMPLEMENTED (truthful notification, spoken 'connection lost' via on-device TTS, bud tracking via AudioDeviceCallback)
**Why:** Buds die mid-day (10 h rating); BT drops; server sleeps. User attention lives in
the ears, so feedback must go there too.
**States:** buds+server / buds only / neither — shown in notification + setup screen.
**Behavior:** if an interaction fails, announce locally via on-device TTS (works offline),
never silently.
**Done when:** Notification reflects real state; failure produces spoken feedback without
the server.

### [M5] Transcription echo-back — IMPLEMENTED server-side (TELEPATHY_ECHO=on default; verified in test flow: two tts_start events per interaction)
**Why:** Input quality (bone-conduction mic + beamforming) should make STT errors rare but
catastrophic when they happen (silent wrong-agent-action). Echo what was heard before acting.
**Behavior:** After STT, server sends/speaks "Working on: …" confirmation, then proceeds.
Double-tap-stop (M3) is the safety net.
**Config:** `TELEPATHY_ECHO=on|off` (default on for now).
**Done when:** Echo sentence is heard before agent reply in normal flow.

## Should-support

### [S1] Cellular-latency resilience
**Why:** IP57 invites outdoor use; networks vary. Defer until real-world latency measured.
**Not started by design.**

### [S2] Gesture remapping UI
**Why:** Defaults above may feel wrong in practice. Config-file territory until proven needed.
**Not started by design.**

## Explicitly rejected

### [X1] MultiPoint awareness
**Why rejected:** It's a "don't break" property, not a feature. We already only activate
SCO at tts_start, which avoids audio-focus fights in practice. Revisit only if a real
conflict is observed.
