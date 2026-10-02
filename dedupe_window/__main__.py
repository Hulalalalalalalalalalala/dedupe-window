"""Command line entry point: python3 -m dedupe_window --state <dir> <command>."""

from __future__ import annotations

import json
import math
import os
import re
import sys

from .window import Window, read_settings

DEFAULT_SPAN = 60
DEFAULT_CAPACITY = 1024

USAGE = (
    "usage: python3 -m dedupe_window --state <dir> "
    "{observe <key> | seen <key> | stats | export <seq> | restore"
    " | observe-batch | observe-events | deliver-events | probe-batch}"
)

# A decimal commit number: one or more decimal digits, optionally signed with
# a leading minus (so non-positive decimals still parse and are rejected as
# out of range rather than as usage errors).
_DECIMAL_INT = re.compile(r"-?[0-9]+\Z")


def _usage():
    print(USAGE, file=sys.stderr)
    raise SystemExit(2)


def _corrupt(reason):
    print(f"state file is corrupt: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _restore_failed(reason):
    print(f"restore failed: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _batch_failed(reason):
    print(f"batch failed: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _event_batch_failed(reason):
    print(f"event batch failed: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _delivery_failed(reason):
    print(f"delivery failed: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _probe_failed(reason):
    print(f"probe failed: {reason}", file=sys.stderr)
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
    if command in ("observe", "seen", "export"):
        if len(operands) != 1:
            _usage()
    elif command in ("stats", "restore", "observe-batch", "observe-events",
                     "deliver-events", "probe-batch"):
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


def _run_export(state, seq):
    """Write the self-checking document of commit ``seq`` to stdout.

    Pure read: takes no lock, creates neither the directory nor a state file
    and never changes the current state.  A missing/corrupt state file, a
    non-positive or unknown commit number is reported as corruption (exit 1)
    with an empty stdout.
    """
    path = os.path.join(state, "window.json")
    try:
        span, capacity = read_settings(path)
    except FileNotFoundError:
        _corrupt("state file is missing")
    except ValueError as exc:
        _corrupt(str(exc))
    try:
        window = Window(state, span, capacity)
        document = window.export(seq)
    except (TypeError, ValueError) as exc:
        _corrupt(str(exc))
    sys.stdout.write(json.dumps(document, separators=(",", ":")) + "\n")


def _run_observe_batch(state):
    """Atomically observe one JSON array of strings read from stdin.

    The whole input -- JSON syntax, list type, element types -- is validated
    and an empty result returned before any filesystem call, so an invalid
    input creates no directory and leaves an existing state byte for byte
    unchanged.  On success stdout holds one compact line of booleans aligned
    with the input.  Any failure exits 1 with one ``batch failed`` line on
    stderr and an empty stdout.
    """
    raw = sys.stdin.buffer.read()
    try:
        keys = json.loads(raw)
    except ValueError as exc:
        # Covers an empty input, malformed JSON and trailing junk.
        _batch_failed(f"input is not one JSON array: {exc}")
    if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
        _batch_failed("input is not a JSON array of strings")
    if not keys:
        sys.stdout.write("[]\n")
        return
    # Open with the persisted settings so a settings mismatch cannot arise;
    # every failure (including creating the directory or reading the data
    # file) is a batch failure, never the generic corruption text.
    path = os.path.join(state, "window.json")
    span, capacity = DEFAULT_SPAN, DEFAULT_CAPACITY
    try:
        if os.path.exists(path):
            span, capacity = read_settings(path)
        os.makedirs(state, exist_ok=True)
        window = Window(state, span, capacity)
        hits = window.observe_many(keys)  # one locked, durable transaction
    except (TypeError, ValueError, OSError) as exc:
        _batch_failed(str(exc))
    sys.stdout.write(json.dumps(hits, separators=(",", ":")) + "\n")


def _run_observe_events(state):
    """Atomically process one event-time batch read from stdin.

    The input is exactly one JSON object holding an ``events`` list and a
    ``watermark``; extra fields are ignored.  Input types and ranges are
    validated before any filesystem call, so an invalid input creates no
    directory and leaves an existing state byte for byte unchanged.  On
    success stdout holds one compact JSON line of per-item results.  Any
    failure exits 1 with one ``event batch failed`` line on stderr and an
    empty stdout.
    """
    raw = sys.stdin.buffer.read()
    try:
        document = json.loads(raw)
    except ValueError as exc:
        # Covers an empty input, malformed JSON and trailing junk.
        _event_batch_failed(f"input is not one JSON object: {exc}")
    if not isinstance(document, dict) or "events" not in document \
            or "watermark" not in document:
        _event_batch_failed("input must be one JSON object with events and watermark")
    events = document["events"]
    watermark = document["watermark"]
    if not isinstance(events, list):
        _event_batch_failed("events must be a JSON array")
    if not isinstance(watermark, (int, float)) or isinstance(watermark, bool):
        _event_batch_failed("watermark must be an int or float")
    if not math.isfinite(watermark) or watermark < 0:
        _event_batch_failed("watermark must be finite and non-negative")
    prepared = []
    for event in events:
        if not isinstance(event, dict) or "key" not in event \
                or "timestamp" not in event:
            _event_batch_failed("each event must be an object with key and timestamp")
        key = event["key"]
        timestamp = event["timestamp"]
        if not isinstance(key, str):
            _event_batch_failed("event key must be a string")
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
            _event_batch_failed("event timestamp must be an int or float")
        if not math.isfinite(timestamp) or timestamp < 0:
            _event_batch_failed("event timestamp must be finite and non-negative")
        if timestamp > watermark:
            _event_batch_failed("event timestamp is later than the watermark")
        prepared.append({"key": key, "timestamp": timestamp})
    # Open with the persisted settings so a settings mismatch cannot arise;
    # the method itself creates the directory only when the batch actually
    # has to commit.  Every failure (creating the directory, reading the
    # data file) is an event batch failure, never the generic corruption text.
    path = os.path.join(state, "window.json")
    span, capacity = DEFAULT_SPAN, DEFAULT_CAPACITY
    try:
        if os.path.exists(path):
            span, capacity = read_settings(path)
        window = Window(state, span, capacity)
        kinds = window.observe_events(prepared, watermark)
    except (TypeError, ValueError, OSError) as exc:
        _event_batch_failed(str(exc))
    sys.stdout.write(json.dumps(kinds, separators=(",", ":")) + "\n")


def _run_deliver_events(state):
    """Idempotently deliver one event-time batch read from stdin.

    The input is exactly one JSON object holding a string ``delivery_id``, an
    ``events`` list and a ``watermark``; extra fields are ignored.  Input
    types and ranges are validated before any filesystem call, so an invalid
    input creates no directory and leaves an existing state byte for byte
    unchanged.  A retained identifier replays the original result without
    touching the file.  On success stdout holds one compact JSON line of
    per-item results.  Any failure exits 1 with one ``delivery failed`` line
    on stderr and an empty stdout.
    """
    raw = sys.stdin.buffer.read()
    try:
        document = json.loads(raw)
    except ValueError as exc:
        # Covers an empty input, malformed JSON and trailing junk.
        _delivery_failed(f"input is not one JSON object: {exc}")
    if (
        not isinstance(document, dict)
        or "delivery_id" not in document
        or "events" not in document
        or "watermark" not in document
    ):
        _delivery_failed(
            "input must be one JSON object with delivery_id, events and watermark"
        )
    delivery_id = document["delivery_id"]
    events = document["events"]
    watermark = document["watermark"]
    if not isinstance(delivery_id, str):
        _delivery_failed("delivery_id must be a string")
    if delivery_id == "":
        _delivery_failed("delivery_id must not be empty")
    if not isinstance(events, list):
        _delivery_failed("events must be a JSON array")
    if not isinstance(watermark, (int, float)) or isinstance(watermark, bool):
        _delivery_failed("watermark must be an int or float")
    if not math.isfinite(watermark) or watermark < 0:
        _delivery_failed("watermark must be finite and non-negative")
    prepared = []
    for event in events:
        if not isinstance(event, dict) or "key" not in event \
                or "timestamp" not in event:
            _delivery_failed("each event must be an object with key and timestamp")
        key = event["key"]
        timestamp = event["timestamp"]
        if not isinstance(key, str):
            _delivery_failed("event key must be a string")
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
            _delivery_failed("event timestamp must be an int or float")
        if not math.isfinite(timestamp) or timestamp < 0:
            _delivery_failed("event timestamp must be finite and non-negative")
        if timestamp > watermark:
            _delivery_failed("event timestamp is later than the watermark")
        prepared.append({"key": key, "timestamp": timestamp})
    # Open with the persisted settings so a settings mismatch cannot arise;
    # the method itself creates the directory as part of the transaction.
    # Every failure (creating the directory, reading the data file) is a
    # delivery failure, never the generic corruption text.
    path = os.path.join(state, "window.json")
    span, capacity = DEFAULT_SPAN, DEFAULT_CAPACITY
    try:
        if os.path.exists(path):
            span, capacity = read_settings(path)
        window = Window(state, span, capacity)
        kinds = window.deliver_events(delivery_id, prepared, watermark)
    except (TypeError, ValueError, OSError) as exc:
        _delivery_failed(str(exc))
    sys.stdout.write(json.dumps(kinds, separators=(",", ":")) + "\n")


def _run_probe_batch(state):
    """Probe a batch of keys read as one JSON object from stdin.

    The input is exactly one JSON object holding a ``keys`` array of strings
    plus optional ``bits`` and ``hashes`` sizing fields; extra fields are
    ignored.  Input types and ranges are validated before any filesystem
    call, so an invalid input creates no directory and leaves an existing
    state byte for byte unchanged.  On success stdout holds one compact JSON
    line with the same fields :meth:`Window.probe_many` returns.  Any
    failure exits 1 with one ``probe failed`` line on stderr and an empty
    stdout.
    """
    raw = sys.stdin.buffer.read()
    try:
        document = json.loads(raw)
    except ValueError as exc:
        # Covers an empty input, malformed JSON and trailing junk.
        _probe_failed(f"input is not one JSON object: {exc}")
    if not isinstance(document, dict) or "keys" not in document:
        _probe_failed("input must be one JSON object with keys")
    keys = document["keys"]
    bits = document.get("bits", 8192)
    hashes = document.get("hashes", 4)
    if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
        _probe_failed("keys must be a JSON array of strings")
    if not isinstance(bits, int) or isinstance(bits, bool):
        _probe_failed("bits must be an integer")
    if not isinstance(hashes, int) or isinstance(hashes, bool):
        _probe_failed("hashes must be an integer")
    if not 1 <= bits <= 1048576:
        _probe_failed("bits must be between 1 and 1048576")
    if not 1 <= hashes <= 16:
        _probe_failed("hashes must be between 1 and 16")
    # Pure read: never create the directory, never touch the data file.
    path = os.path.join(state, "window.json")
    span, capacity = DEFAULT_SPAN, DEFAULT_CAPACITY
    try:
        if os.path.exists(path):
            span, capacity = read_settings(path)
        window = Window(state, span, capacity)
        report = window.probe_many(keys, bits=bits, hashes=hashes)
    except (TypeError, ValueError, OSError) as exc:
        _probe_failed(str(exc))
    sys.stdout.write(json.dumps(report, separators=(",", ":")) + "\n")


def _run_restore(state):
    """Reset the state atomically from exactly one JSON object on stdin.

    The whole document -- JSON type, fields and checksum -- is validated
    before any filesystem call, so an invalid input creates no directory and
    modifies no state file.  A valid document replaces a missing, healthy or
    corrupt old state, after which the same compact JSON a ``stats`` call
    prints describes the restored window.
    """
    raw = sys.stdin.buffer.read()
    try:
        document = json.loads(raw)
    except ValueError as exc:
        # Covers malformed JSON, an empty input and trailing non-whitespace.
        _restore_failed(f"input is not one JSON object: {exc}")
    if not isinstance(document, dict):
        _restore_failed("input is not a JSON object")
    try:
        # Pure validation only: no directory or file may be touched until the
        # document is known to be a valid checkpoint.
        _seq, span, capacity, _now, _admitted, _expired, _entries, _receipts = (
            Window._check_restore_document(document)
        )
        window = Window(state, span, capacity)
        window.restore(document)  # one locked, atomic base-file commit
    except (TypeError, ValueError, OSError) as exc:
        _restore_failed(str(exc))
    sys.stdout.write(json.dumps(window.stats(), separators=(",", ":")) + "\n")


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
        token = operands[0]
        if not _DECIMAL_INT.match(token):
            _usage()
        _run_export(state, int(token))
    elif command == "restore":
        _run_restore(state)
    elif command == "observe-batch":
        _run_observe_batch(state)
    elif command == "observe-events":
        _run_observe_events(state)
    elif command == "deliver-events":
        _run_deliver_events(state)
    elif command == "probe-batch":
        _run_probe_batch(state)
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
