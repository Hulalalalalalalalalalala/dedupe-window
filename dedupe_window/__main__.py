"""Command line entry point: python3 -m dedupe_window --state <dir> <command>."""

from __future__ import annotations

import json
import os
import sys

from .window import Window, read_settings

DEFAULT_SPAN = 60
DEFAULT_CAPACITY = 1024

USAGE = (
    "usage: python3 -m dedupe_window --state <dir> "
    "{observe <key> | seen <key> | stats}"
)


def _usage():
    print(USAGE, file=sys.stderr)
    raise SystemExit(2)


def _corrupt(reason):
    print(f"state file is corrupt: {reason}", file=sys.stderr)
    raise SystemExit(1)


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
    elif command == "stats":
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
