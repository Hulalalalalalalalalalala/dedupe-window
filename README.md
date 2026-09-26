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

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Single host; no network. Concurrent processes on the same state directory
serialize through a file lock, and `save` commits atomically, so a crash
mid-write leaves the last committed state intact.
Keys are text; no value is stored with them.
Span and capacity are the only eviction rules.
