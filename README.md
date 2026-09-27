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
    python3 -m dedupe_window --state ./window export [<point>] <path>
    python3 -m dedupe_window --state ./window restore <path>

## Public interface

`dedupe_window.Window(state, span, capacity)` opens the window directory `state`.
Opening the window never creates the directory.
- `observe(key) -> bool` records a sighting and returns whether the key was newly admitted.
- `seen(key) -> bool` reports membership without recording anything.
- `advance(now) -> int` drops everything older than the span and returns how many keys went.
- `keys() -> list[str]` retained keys, oldest sighting first.
- `stats() -> dict` reports span, capacity, retained, admitted and expired counts.
- `save() -> None` and `load() -> None` persist the window and re-read it before replacing memory.
- `export(path, point=None) -> int` writes the complete window at one commit point to `path` and returns that point; omit `point` for the latest.
- `restore(path) -> int` installs the window from an export document as a new commit and returns the new point.

`seen`, `keys`, `stats` and `load` neither take the write lock nor create the
directory or data file, so they never block a process that is appending or
compacting; every disk read comes back as exactly one complete commit -- never a
torn tail, a mid-compaction state or a mixture of old and new bytes.

Each mutation is a numbered commit point, starting at one and only increasing;
compaction adds no point and renumbers none. `export` writes a self-contained,
versioned and checksummed compact JSON document (fixed key order, one trailing
newline). Points past the latest or already compacted away raise `ValueError`;
a non-integer point raises `TypeError`; a missing target parent directory raises
`FileNotFoundError`. `restore` raises `FileNotFoundError` for a missing document
and `ValueError` for a document that is missing its version, carries an unknown
version, fails its checksum or was otherwise altered; a failed restore changes
neither memory nor the state file. A point restored and exported again reproduces
the original document byte for byte.

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
