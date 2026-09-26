# dedupe-window

Bounded deduplication window for a stream of keys, so a consumer that sees repeated deliveries can tell a first sighting from a repeat that is still inside the retained span.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m dedupe_window --state ./window observe <key>
    python3 -m dedupe_window --state ./window seen <key>
    python3 -m dedupe_window --state ./window stats

## Public interface

`dedupe_window.Window(state, span, capacity)` opens the window directory `state`.
- `observe(key) -> bool` records a sighting and returns whether the key was newly admitted.
- `seen(key) -> bool` reports membership without recording anything.
- `advance(now) -> int` drops everything older than the span and returns how many keys went.
- `keys() -> list[str]` retained keys, oldest sighting first.
- `stats() -> dict` reports span, capacity, retained, admitted and expired counts.
- `save() -> None` and `load() -> None` persist the window and re-read it before replacing memory.

State is committed atomically and incrementally. `window.json` is a log-structured
document: a checksummed base snapshot followed by small checksummed delta segments, so
each `observe`/`advance` appends only its own change instead of mirroring the whole
window, and a compaction periodically merges the segments back into a fresh snapshot
(written to a temporary file and atomically renamed over `window.json`). A crash, a
truncated file or external tampering makes `load` raise `ValueError` (or
`FileNotFoundError` when the file is absent) instead of restoring a partial state; an
interrupted append only tears the uncommitted tail, which readers ignore and the next
writer truncates. Documents are versioned: older versions are upgraded on `load` and
rewritten in the current format on the next commit. Several local processes may share
one state directory; a file lock serializes their writes.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Multiple local processes coordinate with a file lock; there is no network or remote support.
Keys are text; no value is stored with them.
Span and capacity are the only eviction rules.
