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
    python3 -m dedupe_window --state ./window export <seq>
    python3 -m dedupe_window --state ./window restore < checkpoint.json

`export <seq>` writes the self-checking checkpoint of commit `seq` (numbered
from 1) as one JSON object on standard output. It is a pure read: it never
creates the state directory or touches the state file, and a missing or
corrupt state file, a non-positive or unknown commit number exits 1 with
`state file is corrupt` on standard error and empty standard output.

`restore` reads exactly one such JSON object from standard input (only
whitespace may surround it) and atomically resets the window to it, over a
missing, healthy or corrupt old state; on success it prints the same compact
JSON `stats` would show, and later commits continue from the checkpoint's
commit number plus one. Invalid input (not JSON, not an object, a missing or
wrong field, a checksum mismatch, or trailing content) exits 1 with one
`restore failed` line on standard error, empty standard output, and no state
directory or file created or modified.

## Public interface

`dedupe_window.Window(state, span, capacity)` opens the window directory `state`.
Constructing a `Window` never creates the directory or the data file; only a
mutation (`observe`/`advance`/`restore`) does. Read-only calls on a window that
has never committed answer from the empty state and leave the filesystem alone.
- `observe(key) -> bool` records a sighting and returns whether the key was newly admitted.
- `seen(key) -> bool` reports membership without recording anything.
- `advance(now) -> int` drops everything older than the span and returns how many keys went.
- `keys() -> list[str]` retained keys, oldest sighting first.
- `stats() -> dict` reports span, capacity, retained, admitted and expired counts.
- `export(seq) -> dict` returns the complete state of commit `seq` (commits are numbered from 1 in arrival order): retained keys in order, counts, current time, settings, the commit number and a checksum. The document is self-checking and survives a JSON round trip unchanged. A non-integer `seq` (booleans included) raises `TypeError`; a non-positive, never-committed or no-longer-locatable number raises `ValueError`.
- `restore(document) -> None` atomically resets the whole window to an exported state; key order, counts, time and settings match the document and later commits continue from the exported commit's successor. Anything that is not an export document raises `TypeError`; a missing/mistyped field or a bad checksum raises `ValueError`.
- `save() -> None` and `load() -> None` persist the window and re-read it before replacing memory.

`keys`, `seen`, `stats` and `export` are readers: they never take the lock, so a
commit or background compaction in progress neither waits for them nor blocks
them, and they never return a half-written state. Each read reports exactly one
complete commit — its key order, counts and current time together, never a mix
of an old and a new commit.

State is committed atomically and incrementally. `window.json` is a log-structured
document: a checksummed base snapshot followed by small checksummed delta segments, so
each `observe`/`advance` appends only its own change instead of mirroring the whole
window. Compaction periodically appends a checksummed *anchor* snapshot rather than
rewriting the file, so the history before it stays in place: every commit keeps the
number it arrived with and an export of a given commit is identical before and after
any number of compactions. Writes go through a temporary file, fsync and atomic
`os.replace` (the base, and a restore), with deltas appended in place. A crash, a
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
