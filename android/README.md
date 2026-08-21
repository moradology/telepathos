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
2. **Settings → Apps → Default apps → Digital assistant app → Telepathy**
   (also set "Use screenshot/skin" style options to Telepathy if asked).
3. In the Shokz app: map the pinch gesture (or any gesture) to *voice assistant*.
4. Pinch. Open Telepathy — if `SESSION SHOWN` appears in the event log, the
   trigger reaches third-party code and the architecture is viable.
5. If nothing appears: try long-press-power / corner-swipe assist as a sanity
   check that the assistant registration itself works. If those work but the
   pinch doesn't, the Shokz gesture is hard-wired to Gemini/Assistant and we
   fall back to a Pixel-side wake word/VAD service.

## What's implemented

- `TelepathyVoiceInteractionService` + session service/session — assist trigger chain
- `AudioCaptureService` — foreground mic capture (16 kHz PCM16) → WebSocket binary frames
- Protocol matches `../README.md`

## Not yet

- SCO/HFP routing verification (`startBluetoothSco` may be needed on some stacks)
- TTS playback path (server → AudioTrack)
- Tap gestures → command JSON
- Server URL settings UI
