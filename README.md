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
    python3 -m dedupe_window --state ./window restore
    python3 -m dedupe_window --state ./window observe-batch < keys.json

`observe-batch` takes no operand; it reads one JSON array of strings from
standard input (surrounding whitespace is allowed) and prints one compact JSON
array of booleans aligned item by item with the input. The whole array is one
atomic transaction: every admission is stamped with the single current time at
batch start, time never advances, repeats neither refresh time nor order, and a
full window evicts the oldest key, so a key evicted earlier in the same batch
is admitted again if it reappears. An accepting batch adds exactly one commit
(the `admitted` count rises by the number of `true`s; capacity eviction does
not raise `expired`); an all-duplicate batch commits nothing; `[]` prints `[]`
and neither reads nor creates state. Exit 0 prints the boolean line; empty
input, malformed JSON, a non-array, a non-string element or trailing
non-whitespace exits 1 with an empty stdout and one `batch failed` line on
stderr, leaving the state (and a missing directory) untouched.

`export <seq>` writes the complete self-checking document of commit `seq`
(numbered from 1) as one JSON object on standard output, without changing the
state. `restore` reads exactly one such document from standard input and
atomically resets the window to it, printing the same compact JSON `stats`
prints; later commits continue from the restored commit's successor. Pipe an
export straight into a restore to move or roll back state without touching
business code:

    python3 -m dedupe_window --state ./old export 12 \
      | python3 -m dedupe_window --state ./new restore

A bad command line exits 2 with a usage message. `export` exits 1 with a
`state file is corrupt` message when the state file is missing or corrupt, or
the commit number is non-positive or unknown; `restore` exits 1 with a
`restore failed` message for anything that is not one valid checkpoint
document, and never creates or partly modifies the state in that case.
`observe-batch` exits 1 with a `batch failed` message for empty input, invalid
JSON, a non-string-array, trailing non-whitespace, or a missing/corrupt state
or storage failure, likewise without creating or partly modifying the state.

## Public interface

`dedupe_window.Window(state, span, capacity)` opens the window directory `state`.
Constructing a `Window` never creates the directory or the data file; only a
mutation (`observe`/`advance`/`restore`) does. Read-only calls on a window that
has never committed answer from the empty state and leave the filesystem alone.
- `observe(key) -> bool` records a sighting and returns whether the key was newly admitted.
- `observe_many(keys) -> list[bool]` records a list of string keys as one atomic batch, returning a boolean per key in input order. It stamps admissions with the single current time at batch start (never advancing time), keeps a repeat's original time and order, evicts the oldest retained key when full (so a key evicted within the batch can be re-admitted), and raises `admitted` by the number of `true`s without touching `expired`. An accepting batch adds exactly one commit; an all-duplicate batch commits nothing; `[]` returns `[]` without reading or creating state. A non-list or any non-string element raises `TypeError` before the filesystem is touched; corrupt state or a settings mismatch raises `ValueError` without overwriting it.
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
