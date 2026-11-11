#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
# Copyright (C) 2026 Eren Yilmaz
"""Record one clip, or transcribe an existing file: python3 tools/voice.py [audio]."""
import argparse
import getpass
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import wave

from openai import OpenAI

MODEL = "gpt-transcribe"


def record(path):
    process = subprocess.Popen(
        ["pw-record", "--rate", "16000", "--channels", "1", "--format", "s16", str(path)],
        stdin=subprocess.DEVNULL, start_new_session=True,
    )
    try:
        print("Recording microphone. Press Enter to finish.", file=sys.stderr)
        input()
    finally:
        stopped = process.poll() is None
        if stopped:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise RuntimeError("pw-record did not stop cleanly")
    # PipeWire returns 1 for a normal SIGINT stop on this machine. Check the WAV too.
    if process.returncode != 0 and not (stopped and process.returncode in (1, -signal.SIGINT)):
        raise RuntimeError("pw-record failed")
    with wave.open(str(path), "rb") as audio:
        if audio.getnframes() == 0:
            raise ValueError("Recording is empty")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audio", nargs="?", type=Path, help="existing audio file; omit to record")
    args = parser.parse_args()
    path = args.audio.expanduser().resolve() if args.audio else None
    temporary = path is None
    try:
        key = os.environ.get("OPENAI_API_KEY", "").strip() or getpass.getpass("OpenAI API key: ").strip()
        if not key:
            raise ValueError("No API key supplied")
        if temporary:
            descriptor, filename = tempfile.mkstemp(prefix="voice-", suffix=".wav")
            os.close(descriptor)
            path = Path(filename)
            record(path)
        if not 0 < path.stat().st_size <= 25_000_000:
            raise ValueError("Audio must be nonempty and at most 25 MB")
        with OpenAI(api_key=key, base_url="https://api.openai.com/v1",
                    max_retries=0, timeout=90) as client:
            with path.open("rb") as audio:
                transcript = client.audio.transcriptions.create(model=MODEL, file=audio)
            print(transcript.text)
        if temporary:
            path.unlink()
        return 0
    except (Exception, KeyboardInterrupt) as error:
        # Avoid printing an API error body: it can contain parts of an invalid key.
        status = getattr(error, "status_code", None)
        detail = f", HTTP {status}" if status is not None else ""
        if isinstance(error, (RuntimeError, ValueError, OSError)):
            detail += f": {error}"
        print(f"Voice failed ({type(error).__name__}{detail}). No automatic retry.", file=sys.stderr)
        if path is not None and path.is_file():
            print(f"Audio kept: {path}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
