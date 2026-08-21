# telepathy

Control coding agents by voice, through open-ear earbuds.

Pinch your Shokz OpenDots 2 → speak → a pi coding agent on your server does the
work → the answer is spoken back into your ears. Hands stay free, ears stay open.

## Architecture

```
OpenDots 2 (mic + speaker, HFP over BT)
   │
Pixel 9  —  Android app: pinch → VoiceInteractionService → capture PCM → WebSocket
   │          (media-button taps → JSON commands: stop / approve / status)
   │ cellular or wifi
3090 server  —  Node bridge:
   │             energy VAD → faster-whisper (Python worker) → pi SDK agent session
   │             → text deltas streamed back → TTS (say/piper) → WAV frames
   └──────────► back down the same socket → earbuds
```

## Repo layout

- `server/` — TypeScript WebSocket bridge + pi SDK integration (runs anywhere; dev on Mac)
- `android/` — Kotlin client: assistant trigger, audio capture, WS client
- `docs/` — protocol and architecture notes

## Protocol (v1)

Single WebSocket connection. Text frames are JSON, binary frames are raw
PCM16 mono @ 16 kHz from the mic.

Client → server:
- `{"type":"hello","device":"opendots2"}` — first frame after connect
- binary — mic audio
- `{"type":"utterance_end"}` — client-side end-of-speech (optional if server VAD is on)
- `{"type":"command","command":"stop"|"repeat"|"cancel_capture"}` — from earbud taps (docs/features.md M3)

Server → client:
- `{"type":"ready"}` — handshake ack
- `{"type":"listening"}` / `{"type":"speech_start"}` / `{"type":"utterance","samples":N}` — VAD state
- `{"type":"stt","text":"..."}` — transcript
- `{"type":"agent_start"}` / `{"type":"agent_delta","text":"..."}` / `{"type":"agent_end"}` — pi streaming (`agent_end` = full reply incl. audio sent)
- `{"type":"tts_start","sampleRate":N,"samples":N}` then binary PCM frames — spoken reply (raw PCM16 mono, no WAV header)
- `{"type":"listening"}` — server VAD is live again after an interaction
- `{"type":"error","message":"..."}`

## Status

- [x] Server: WS bridge, VAD, whisper worker, pi session, TTS
- [x] Android: pinch→assistant skeleton (the make-or-break test)
- [x] Android: full audio client with **capture-on-demand** (mic open on pinch,
      closed after `listening` — zero radio/mic power between interactions)
- [x] Typed interaction state machine (server-authoritative, phase broadcasts)
- [ ] Piper TTS on the 3090
- [ ] Hardware validation: pinch mapping, SCO routing, taps-during-SCO, carrier NAT
