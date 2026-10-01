"""Command line entry point: python3 -m dedupe_window --state <dir> <command>."""

from __future__ import annotations

import json
import os
import re
import sys

from .window import Window, read_settings

# JSON whitespace (RFC 8259): space, tab, line feed, carriage return.
_JSON_WHITESPACE = re.compile(r"[ \t\n\r]*")

DEFAULT_SPAN = 60
DEFAULT_CAPACITY = 1024

USAGE = (
    "usage: python3 -m dedupe_window --state <dir> "
    "{observe <key> | seen <key> | stats | export <seq> | restore}"
)

_DIGITS = frozenset("0123456789")


def _usage():
    print(USAGE, file=sys.stderr)
    raise SystemExit(2)


def _corrupt(reason):
    print(f"state file is corrupt: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _restore_failed(reason):
    print(f"restore failed: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _parse_seq(text):
    """Parse a 1-based decimal commit number for the export command.

    Only ASCII decimal digits are accepted (no sign, no whitespace, no
    ``0x``/``1.0`` spellings); anything else is a usage error.  The value's
    positivity and existence are settled against the state file later.
    """
    if not text or any(ch not in _DIGITS for ch in text):
        _usage()
    return int(text)


def _parse(argv):
    state = None
    rest = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--state":
            if i + 1 >= len(argv):
                _usage()
            state = argv[i + 1]
            i += 2
        elif arg.startswith("--state="):
            state = arg[len("--state="):]
            i += 1
        else:
            rest.append(arg)
            i += 1
    if not state or len(rest) == 0:
        _usage()
    command, operands = rest[0], rest[1:]
    if command in ("observe", "seen"):
        if len(operands) != 1:
            _usage()
    elif command == "export":
        if len(operands) != 1:
            _usage()
        operands = [_parse_seq(operands[0])]
    elif command in ("stats", "restore"):
        if operands:
            _usage()
    else:
        _usage()
    return state, command, operands


def _open_window(state):
    """Load the persisted window, or start an empty one with the defaults.

    A missing data file means a fresh window (only possible for the first
    observe); every other read failure is corruption and exits 1.
    """
    path = os.path.join(state, "window.json")
    span, capacity = DEFAULT_SPAN, DEFAULT_CAPACITY
    if os.path.exists(path):
        try:
            span, capacity = read_settings(path)
        except ValueError as exc:
            _corrupt(str(exc))
    try:
        window = Window(state, span, capacity)
    except (TypeError, ValueError) as exc:
        _corrupt(str(exc))
    if os.path.exists(path):
        try:
            window.load()
        except FileNotFoundError:
            pass  # another process removed it between the checks above
        except ValueError as exc:
            _corrupt(str(exc))
    return window


def _run_export(state, path, seq):
    """Locate commit ``seq`` and write its export document to stdout.

    Pure read: a missing state directory or data file, a corrupt file, a
    non-positive or an unknown commit number all exit 1 with an empty
    stdout and touch nothing.
    """
    if not os.path.exists(path):
        _corrupt("state file is missing")
    window = _open_window(state)
    try:
        document = window.export(seq)
    except ValueError as exc:
        _corrupt(str(exc))
    print(json.dumps(document, separators=(",", ":")))


def _run_restore(state):
    """Read one export object from stdin and atomically reset the window.

    The document is fully validated (and the stdin content checked for
    trailing data) before anything on disk is touched, so every failure
    leaves any previous state -- missing, healthy or corrupt -- untouched
    and never creates the state directory.
    """
    try:
        raw = sys.stdin.buffer.read()
    except OSError as exc:
        _restore_failed(f"cannot read standard input: {exc}")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        _restore_failed(f"input is not valid UTF-8: {exc}")
    try:
        # raw_decode itself does not skip leading whitespace; skip it here so
        # the document may be surrounded by whitespace on all sides.
        start = _JSON_WHITESPACE.match(text).end()
        document, end = json.JSONDecoder().raw_decode(text, start)
    except json.JSONDecodeError as exc:
        _restore_failed(f"input is not valid JSON: {exc}")
    if not isinstance(document, dict):
        _restore_failed("input must be a single JSON object")
    # Only JSON whitespace may follow; anything else (including a second
    # document) is trailing content and rejects the whole input.
    if _JSON_WHITESPACE.match(text, end).end() != len(text):
        _restore_failed("trailing content follows the document")
    # Construction has no filesystem side effects; the document supplies
    # span/capacity, so the construction defaults are placeholders only.
    window = Window(state, DEFAULT_SPAN, DEFAULT_CAPACITY)
    try:
        window.restore(document)
    except (TypeError, ValueError, OSError) as exc:
        # The document is validated before the directory is created and the
        # file is replaced via temp + os.replace, so a failure here leaves any
        # previous state (missing, healthy or corrupt) byte-for-byte intact.
        _restore_failed(str(exc))
    print(json.dumps(window.stats(), separators=(",", ":")))


def main(argv=None):
    state, command, operands = _parse(list(sys.argv[1:] if argv is None else argv))
    path = os.path.join(state, "window.json")
    if command == "observe":
        os.makedirs(state, exist_ok=True)
        window = _open_window(state)
        result = window.observe(operands[0])  # one locked, durable transaction
        print(json.dumps(result))
    elif command == "seen":
        # Pure read: never create the directory, never touch the data file.
        if not os.path.exists(path):
            print(json.dumps(False))
            return 0
        window = _open_window(state)
        print(json.dumps(window.seen(operands[0])))
    elif command == "export":
        _run_export(state, path, operands[0])
    elif command == "restore":
        _run_restore(state)
    else:
        # Pure read: never create the directory, never touch the data file.
        if not os.path.exists(path):
            print(
                json.dumps(
                    {
                        "span": DEFAULT_SPAN,
                        "capacity": DEFAULT_CAPACITY,
                        "retained": 0,
                        "admitted": 0,
                        "expired": 0,
                    },
                    separators=(",", ":"),
                )
            )
            return 0
        window = _open_window(state)
        print(json.dumps(window.stats(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
