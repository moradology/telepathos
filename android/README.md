# Android client

## Building from CLI (no Android Studio required)

One-time setup already done on this Mac:
- JDK 17: `brew install openjdk@17` → `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`
- SDK: `~/Library/Android/sdk` (cmdline-tools, platforms;android-35, build-tools;35.0.0)

```sh
cd android
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
./gradlew assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

## The make-or-break test (do this first, ~30 min)

1. Open `android/` in Android Studio, build, install on the Pixel 9.
2. **Settings → Apps → Default apps → Digital assistant app → Telepathos**
   (also set "Use screenshot/skin" style options to Telepathos if asked).
3. In the Shokz app: map the pinch gesture (or any gesture) to *voice assistant*.
4. Pinch. Open Telepathos — if `SESSION SHOWN` appears in the event log, the
   trigger reaches third-party code and the architecture is viable.
5. If nothing appears: try long-press-power / corner-swipe assist as a sanity
   check that the assistant registration itself works. If those work but the
   pinch doesn't, the Shokz gesture is hard-wired to Gemini/Assistant and we
   fall back to a Pixel-side wake word/VAD service.

## What's implemented

- `TelepathosVoiceInteractionService` + session service/session — assist trigger chain
- `AudioCaptureService` — foreground mic capture (16 kHz PCM16) → WebSocket binary frames
- `LocalAnnouncer` — local TTS for `agent_end` replies and offline status announcements
- AVRCP media-key taps — `stop`, `repeat`, `cancel_capture`; tap-to-flush sends `utterance_end`
- Protocol matches `../README.md`

## Durable v8 `ReplyAckSnapshot` reply-ack retirement (hard cutover)

Every WebSocket hello includes a stable opaque `installation_id` generated
from secure randomness and persisted in app `SharedPreferences`. It is the
receipt owner identity; the human-readable `device` label is informational and
is never used for ownership.

Remote replies carry an exact delivery receipt and the complete agent-end
replay text. Android durably stores both before playback and follows this
protocol without a compatibility mode:

1. Persist `receipt_pending`, then retry `reply_received`. The bridge only
   confirms after it durably records the handset receipt, and Android then
   changes to `awaiting_playback`. Pending narration never claims the
   receipt-owned `reply_to` rows while this proof is outstanding.
2. After direct playback or receipt-owned saved-text recovery, persist
   `ready_to_acknowledge`, then retry
   `reply_ack` with the receipt fields.
3. The bridge durably changes its binding from `received` to `consumed`, then
   emits `reply_acknowledged`.
4. Android durably changes `ready_to_acknowledge` to `retirement_pending`,
   then retries `reply_ack_retire` with the same immutable receipt fields.
5. The bridge durably removes its consumed binding and emits
   `reply_ack_retired`. Android removes its local record only after that
   removal commits.

`reply_ack_retire` never authorizes consumption. Therefore a bridge may
idempotently answer an unknown duplicate retire with `reply_ack_retired` after
a restart or after deletion. Android retries receipt-pending, ready, and
retirement-pending records while connected. After `ready`, Android also resumes
each locally durable `awaiting_playback` reply from its saved text, independent
of the currently selected lane; only successful playback advances it to
`ready_to_acknowledge`. This recovery is serialized so a duplicate ready or
reconnect cannot speak the same record twice at once. The bridge replays
prepared reply envelopes before `ready`, and Android opens its traffic gate only
after its own hello was queued and this explicit ready arrives. The WebSocket
wire protocol is v5; the Node reply-ack persisted store hard-requires v8;
Android's local `ReplyAckSnapshot` hard-requires v8; and the Rust relay delivery
snapshot is v4. These are separate hard-cutover contracts; older receipt state
is not silently migrated or treated as a different owner.

Complete replies are capped at 512 KiB of UTF-8 bytes. Android applies this
limit independently to streamed deltas, terminal `agent_end`, and durable replay
text before TTS or acknowledgement.

Reply, message, and interaction IDs are opaque and shared across runtimes:
nonblank, free of C0/C1 controls, and limited to 256 UTF-8 bytes and 256
UTF-16 code units. Android rejects malformed values on wire ingress, pending
delivery admission, local receipt snapshot load/save, and command
serialization. WebSocket endpoint identity uses OkHttp's canonical `ws`/`wss`
URL form (lowercase scheme/host, default-port normalization, root slash),
rejecting query, fragment, userinfo, and unsupported schemes. The identity
hash is versioned and includes only a SHA-256 credential digest; secrets are
never persisted. Equivalent endpoint spellings share state, while the old
identity/snapshot formats are rejected rather than migrated.

Normal `/api/pending` narration preserves `reply_to` and excludes rows covered
by a durable receipt in every receipt state. It acknowledges only the explicit
sequence IDs it actually spoke; `/api/pending/consume` has no through-sequence
compatibility mode.

## Not yet

- SCO/HFP routing verification (`startBluetoothSco` may be needed on some stacks)
- Server URL settings UI
