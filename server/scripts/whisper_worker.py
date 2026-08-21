#!/usr/bin/env python3
"""
faster-whisper worker: reads JSON requests on stdin, writes JSON responses on
stdout. One model, loaded once, reused for every utterance.

Request:  {"id": "...", "path": "/tmp/utt.wav", "prompt": "vocab hints or null"}
Response: {"id": "...", "text": "...", "confidence": 0.83, "language": "en"}

Config via env:
  TELEPATHY_WHISPER_MODEL  model size/name (default: large-v3)
  TELEPATHY_WHISPER_DEVICE cuda | cpu (default: cuda)
"""

import json
import math
import os
import sys


def main():
    from faster_whisper import WhisperModel

    model_size = os.environ.get("TELEPATHY_WHISPER_MODEL", "large-v3")
    device = os.environ.get("TELEPATHY_WHISPER_DEVICE", "cuda")
    compute_type = "float16" if device == "cuda" else "int8"

    model = WhisperModel(model_size, device=device, compute_type=compute_type)
    print(json.dumps({"event": "ready", "model": model_size, "device": device}), flush=True)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            segments, info = model.transcribe(
                req["path"],
                language="en",
                initial_prompt=req.get("prompt") or None,
                beam_size=1,
                vad_filter=True,
            )
            texts, logprobs = [], []
            for seg in segments:
                texts.append(seg.text.strip())
                logprobs.append(seg.avg_logprob)
            # avg_logprob is a log-probability; report mean probability [0..1]
            confidence = (
                sum(math.exp(p) for p in logprobs) / len(logprobs) if logprobs else 0.0
            )
            print(json.dumps({
                "id": req["id"],
                "text": " ".join(texts),
                "confidence": round(confidence, 3),
                "language": info.language,
            }), flush=True)
        except Exception as e:  # never let one bad request kill the worker
            print(json.dumps({"id": req.get("id", "?"), "error": str(e)}), flush=True)


if __name__ == "__main__":
    main()
