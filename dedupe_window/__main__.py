"""Command line entry point: python3 -m dedupe_window --state <dir> <command>."""

from __future__ import annotations

import json
import os
import sys

from .window import Window

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
    """Load the persisted window, or start an empty one with the defaults."""
    path = os.path.join(state, "window.json")
    span, capacity = DEFAULT_SPAN, DEFAULT_CAPACITY
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                settings = json.load(fh)
            span = settings["span"]
            capacity = settings["capacity"]
        except Exception:
            _corrupt("cannot read span and capacity")
    try:
        window = Window(state, span, capacity)
    except (TypeError, ValueError) as exc:
        _corrupt(str(exc))
    if os.path.exists(path):
        try:
            window.load()
        except ValueError as exc:
            _corrupt(str(exc))
    return window


def main(argv=None):
    state, command, operands = _parse(list(sys.argv[1:] if argv is None else argv))
    os.makedirs(state, exist_ok=True)
    window = _open_window(state)
    if command == "observe":
        result = window.observe(operands[0])
        window.save()
        print(json.dumps(result))
    elif command == "seen":
        print(json.dumps(window.seen(operands[0])))
    else:
        print(json.dumps(window.stats(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
