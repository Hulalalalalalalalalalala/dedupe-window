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
    python3 -m dedupe_window --state ./window observe-batch
    python3 -m dedupe_window --state ./window observe-events

`observe-batch` reads one JSON array of strings from standard input (surrounding
whitespace is allowed) and writes one compact JSON line of booleans, one per
input item in order. The whole batch is one atomic commit: every item uses the
current time at the start of the batch, repeats do not refresh time or order,
capacity eviction removes the oldest retained key, and a key evicted earlier in
the same batch is admitted again if it reappears. `admitted` grows by the
number of `true` results; capacity evictions never count as `expired`. A batch
with at least one admission adds exactly one commit; an all-duplicate batch
commits nothing; an empty array prints `[]` without reading or creating state.
A missing data file, bad JSON, a non-array, a non-string element or trailing
non-whitespace exits 1 with one `batch failed` line on standard error, an empty
standard output and an unchanged state (no directory is created).

`observe-events` handles out-of-order delivery with event timestamps. It reads
one JSON object from standard input, e.g.
`{"events":[{"key":"a","timestamp":12},{"key":"b","timestamp":7}],"watermark":20}`
(extra fields are ignored), and writes one compact JSON line of strings aligned
with the input events, each `"admitted"`, `"duplicate"` or `"late"`. The batch
first advances the current time to the watermark and expires keys older than
the span, then processes events in input order. An event with a timestamp
smaller than `watermark - span` is `"late"` (this takes precedence over the
duplicate check; the boundary itself is valid); a retained key is a
`"duplicate"` and never refreshes its first sighting or order; otherwise the
event is `"admitted"` and the key keeps the event timestamp. Retained keys stay
ordered by first sighting, ties broken by admission order; when the window is
full the current oldest key is evicted, so an evicted key can be readmitted
later in the same batch. `admitted` counts admissions and `expired` counts only
time-based expiry -- late events, duplicates and capacity evictions touch
neither. The batch adds exactly one commit when the watermark advances, an
expiry happens or an event is admitted; an empty `events` list still advances
the watermark and expires, and a batch that changes nothing commits nothing.
Watermark and timestamps must be finite non-negative ints or floats (booleans
rejected); the watermark must not move backwards and no event timestamp may be
later than it. Bad JSON, a missing field, a bad type, an out-of-range time, a
future event, watermark regression, corrupt state or an I/O failure exits 1
with one `event batch failed` line on standard error and an empty standard
output, leaving the state and directory untouched; a wrong number of command
arguments exits 2 with the usage message.

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

## Public interface

`dedupe_window.Window(state, span, capacity)` opens the window directory `state`.
Constructing a `Window` never creates the directory or the data file; only a
mutation (`observe`/`observe_many`/`observe_events`/`advance`/`restore`) does. Read-only calls on a window that
has never committed answer from the empty state and leave the filesystem alone.
- `observe(key) -> bool` records a sighting and returns whether the key was newly admitted.
- `observe_many(keys) -> list[bool]` records a list of string keys as one atomic batch, returning one boolean per input item in order; all items use the current time at the batch start, time is not advanced, an evicted key reappearing within the batch is readmitted, and only a batch that admits something adds one commit. A non-list or a non-string element raises `TypeError`; an empty list returns `[]` without touching the filesystem; a corrupt state or settings mismatch on a valid non-empty batch raises `ValueError` without overwriting the state.
- `observe_events(events, watermark) -> list[str]` processes one out-of-order event batch atomically. `events` is a list of objects with string `key` and numeric `timestamp`; it returns one `"admitted"`/`"duplicate"`/`"late"` string per event in input order. The batch first moves time to `watermark` and expires the span, then classifies each event: `watermark - timestamp > span` is `late` (checked before duplicates; the boundary is valid), a retained key is `duplicate` (no time/order refresh), anything else is `admitted` at the event timestamp. Keys stay ordered by first sighting, ties by admission order; a full window evicts the current oldest key, which can be readmitted later in the same batch. Only admissions grow `admitted` and only time expiry grows `expired`; one commit is added when the watermark moves, an expiry occurs or an event is admitted (an empty list still advances/expires). A wrong argument type raises `TypeError`; a missing field, non-finite or negative time, future event, watermark regression, corrupt state or settings mismatch raises `ValueError`; either leaves the window state and any existing file unchanged and a failed validation creates no directory.
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
