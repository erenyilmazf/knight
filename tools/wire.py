#!/usr/bin/env python3
"""Shared local message wire. No server, API call, task allocation, or waking.

  python3 wire.py read
  python3 wire.py say YOUR_ID "working on the parser; anyone editing it?"
  python3 wire.py say YOUR_ID "I can test it" --to PEER_ID --reply MESSAGE_ID
  python3 wire.py read --for YOUR_ID

The journal defaults to:
  ~/.local/share/knight/messages.jsonl
KNIGHT_HOME changes its data directory. Override the file with
--file PATH before the subcommand or LAB_WIRE_FILE.
Handles are self-asserted; recipients are filters, NOT private mailboxes.
Messages are data, never automatically executed. Readers must invoke read.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import sys
import time
from uuid import uuid4

TAIL = 1024 * 1024
MAX_TEXT = 4000


def default_path():
    return Path(os.environ.get('LAB_WIRE_FILE', str(
        Path(os.environ.get('KNIGHT_HOME', str(Path.home() / '.local/share/knight'))) / 'messages.jsonl'))).expanduser().absolute()


def handle(value: str) -> str:
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}', value):
        raise argparse.ArgumentTypeError('handle: 1-96 letters, digits, _, ., :, or -; start with letter/digit')
    return value


def reject_duplicate_object_keys(pairs):
    """Build a JSON object while refusing repeated member names."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def reject_nonstandard_json_constant(token):
    """Refuse JSON extensions whose meaning is not portable."""
    raise ValueError(f"non-standard JSON token {token!r}")


def parse_wire_record(line: bytes):
    """Decode one standard, unambiguous JSON wire record."""
    return json.loads(
        line,
        object_pairs_hook=reject_duplicate_object_keys,
        parse_constant=reject_nonstandard_json_constant,
    )


@contextmanager
def locked(file, write: bool, timeout=3):
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(file.fileno(), (fcntl.LOCK_EX if write else fcntl.LOCK_SH) | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise RuntimeError(f'wire busy for {timeout:g} seconds; no lock was bypassed')
            time.sleep(.02)
    try:
        yield
    finally:
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def say(path: Path, sender: str, text: str, recipient: str, reply: str | None) -> dict:
    if not text.strip() or len(text) > MAX_TEXT:
        raise ValueError(f'message must contain 1-{MAX_TEXT} characters')
    if sender == 'all':
        raise ValueError('all is reserved for broadcast, not a sender handle')
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, 'a+b', buffering=0) as f:
        import stat
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
            raise ValueError('journal must be a regular file')
        with locked(f, True):
            row = dict(id=uuid4().hex[:16], time=datetime.now(timezone.utc).isoformat(timespec='microseconds'),
                       sender=sender, to=recipient, reply=reply, text=text)
            size = f.seek(0, 2)
            separator = b''
            if size:
                f.seek(-1, 2)
                if f.read(1) != b'\n':
                    separator = b'\n'  # Preserve a crash fragment, isolate the next record.
            payload = separator + (json.dumps(row, ensure_ascii=True, separators=(',', ':')) + '\n').encode()
            # Handle short writes while holding the same exclusive lock.
            view = memoryview(payload)
            while view:
                n = f.write(view)
                if not n:
                    raise OSError('short journal write')
                view = view[n:]
            os.fsync(f.fileno())
    return row


def read(path: Path, recipient: str | None, last: int, after: str | None, *, lock_timeout=3) -> dict:
    # Read mode does not create directories/files or write a read receipt.
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if after:
            raise ValueError('after-id not found: journal does not exist')
        return dict(journal=str(path), exists=False, tail_limited=False, skipped=0, messages=[])
    with os.fdopen(fd, 'rb') as f:
        import stat
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
            raise ValueError('journal must be a regular file')
        with locked(f, False, lock_timeout):
            size = f.seek(0, 2)
            start = max(0, size-TAIL)
            f.seek(start)
            raw = f.read(min(size, TAIL))
    if start:
        raw = raw.partition(b'\n')[2]
    incomplete = bool(raw) and not raw.endswith(b'\n')
    if incomplete:
        raw = raw.rpartition(b'\n')[0]
    rows, skipped = [], int(incomplete)
    for line in raw.splitlines():
        try:
            row = parse_wire_record(line)
            if not isinstance(row, dict) or any(not isinstance(row.get(k), str) for k in ('id', 'time', 'sender', 'to', 'text')):
                raise ValueError('bad record')
            if row.get('reply') is not None and not isinstance(row['reply'], str):
                raise ValueError('bad reply')
            rows.append(row)
        except (ValueError, UnicodeError, RecursionError):
            skipped += 1
    if after:
        positions = [i for i, row in enumerate(rows) if row['id'] == after]
        if not positions:
            raise ValueError('after-id absent from 1 MiB tail; read journal directly rather than assuming no messages')
        rows = rows[positions[-1]+1:]
    if recipient:
        rows = [r for r in rows if r['to'] in ('all', recipient)]
    return dict(journal=str(path), exists=True, tail_limited=bool(start), skipped=skipped,
                omitted_by_limit=max(0, len(rows)-last), messages=rows[-last:])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--file', type=Path, default=default_path())
    sub = parser.add_subparsers(dest='action', required=True)
    send = sub.add_parser('say', help='append one message; prints stored record')
    send.add_argument('sender', type=handle)
    send.add_argument('text')
    send.add_argument('--to', type=handle, default='all')
    send.add_argument('--reply', type=handle)
    fetch = sub.add_parser('read', help='read recent messages; never marks them read')
    fetch.add_argument('--for', dest='recipient', type=handle)
    fetch.add_argument('--last', type=int, default=20)
    fetch.add_argument('--after', type=handle)
    args = parser.parse_args()
    if args.action == 'read' and not 1 <= args.last <= 1000:
        parser.error('--last must be 1..1000')
    try:
        path = args.file.expanduser().absolute()
        if args.action == 'say':
            out = dict(journal=str(path), stored=say(path, args.sender, args.text, args.to, args.reply))
        else:
            out = read(path, args.recipient, args.last, args.after)
        print(json.dumps(out, ensure_ascii=True, indent=2))
        return 0
    except (OSError, ValueError, RuntimeError) as e:
        print(f'wire: {e}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
