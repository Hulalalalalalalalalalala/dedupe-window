"""Bounded deduplication window.

A :class:`Window` remembers the first sighting time of each key currently
retained.  Two eviction rules apply:

* capacity: admitting a new key while the window is full evicts the oldest
  key first;
* span: :meth:`Window.advance` drops every key whose first sighting is more
  than ``span`` behind the current time.

Persistence is crash safe and versioned.  The state directory holds exactly
one data file, ``window.json``, in a log-structured format: a checksummed
base snapshot followed by checksummed records, one per line, chained by
sequence number and the previous record's checksum.  Most records are small
delta segments, one per committed mutation, so the bytes hitting disk stay
proportional to the change instead of mirroring the whole retained state
every time.  Periodically a compaction appends an *anchor*: a full snapshot
of one commit.  Unlike a rewrite, appending an anchor leaves every earlier
record in place, so each commit keeps the number it arrived with and stays
locatable: an export returns the same document whether it runs before or
after a compaction.  Anchors also bound how far a reader replays.

A mutation commits by appending its record in place and fsyncing.  A crash
therefore either leaves the last committed state fully recoverable -- an
interrupted append only tears the uncommitted tail, which readers ignore and
the next writer truncates -- or produces a deterministic error from
``load``; a half-restored state is never returned.  A truncated file, a bad
checksum or any other tampering raises ``ValueError``; a missing data file
raises ``FileNotFoundError``.

Documents carry a version number.  The current format is version 3;
version 1 documents (a single whole-state JSON object) and version 2
documents (a base plus deltas whose compaction rewrote history away) are
still read, upgraded in memory and rewritten in the current format on the
next commit.  A document without a version, with an unknown version, or
left in a half-migrated shape raises ``ValueError``.

Read calls never take the lock.  ``keys``, ``seen``, ``stats``,
``probe_many`` and ``export`` open the data file independently of any
writer, so a commit or a
compaction in progress neither blocks them nor leaks a half-written state:
a reader always observes one whole committed prefix and reports exactly
that commit -- its key order, counts and current time together, never a
mix of an old and a new commit.  Only mutating operations take the advisory
``flock``, held on a file descriptor opened on the state directory itself,
so no lock file is ever left behind and the kernel releases the lock if a
process dies while holding it.  Mutations are linearizable: each runs as
lock -> reload -> mutate -> atomic commit, equivalent to some serial
execution in arrival order.  Local deployment only; no networking.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import math
import os
import tempfile

_STATE_FILE = "window.json"
_TMP_PREFIX = _STATE_FILE + "."
_TMP_SUFFIX = ".tmp"
_VERSION = 3
_REQUIRED_FIELDS = (
    "version",
    "span",
    "capacity",
    "now",
    "admitted",
    "expired",
    "keys",
)
# Compaction policy: append a fresh anchor once the deltas since the last
# anchor are bigger than it (with a floor so tiny states are not anchored
# every commit) or simply too numerous to replay cheaply.
_MAX_DELTAS = 512
_MIN_ANCHOR_BYTES = 4096

_EXPORT_FORMAT = "dedupe-window-export"

# A batch-level idempotency receipt remembers the original classification of
# a successful :meth:`Window.deliver_events` call.  Receipts form a ring of at
# most this many distinct identifiers, evicted oldest first by first
# successful commit; a replay never refreshes an entry's position.
_RECEIPT_LIMIT = 128

# Batch membership probes layer a deterministic bloom index over the exact
# retained set.  The bounds keep the bit table small (never more than
# ceil(bits/8) bytes) and the hash count cheap.
_PROBE_DEFAULT_BITS = 8192
_PROBE_DEFAULT_HASHES = 4
_PROBE_MAX_BITS = 1 << 20
_PROBE_MAX_HASHES = 16


def _is_number(value):
    """Numbers are ints or floats; booleans do not count."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_finite_time(value):
    """A timestamp or watermark: a non-boolean finite non-negative number."""
    return _is_number(value) and math.isfinite(value) and value >= 0


def _is_count(value):
    """Counts are non-negative ints; booleans do not count."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _check_key(key):
    if not isinstance(key, str):
        raise TypeError("key must be a string")


def _canonical(body):
    """Deterministic byte serialization covered by the checksum."""
    return json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _checksum(body):
    return hashlib.sha256(_canonical(body)).hexdigest()


def _bloom_positions(key, bits, hashes):
    """Bloom positions for ``key`` from sha256 double hashing.

    The digest is a pure function of the key's UTF-8 bytes, so the same
    retained set and parameters produce the same positive/negative answers
    in every process, across restarts.
    """
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    first = int.from_bytes(digest[:16], "big")
    second = int.from_bytes(digest[16:], "big")
    return [(first + i * second) % bits for i in range(hashes)]


def _bloom_build(keys, bits, hashes):
    """A bloom table of exactly ceil(bits/8) bytes over ``keys``."""
    table = bytearray((bits + 7) // 8)
    for key in keys:
        for pos in _bloom_positions(key, bits, hashes):
            table[pos >> 3] |= 1 << (pos & 7)
    return table


def _bloom_might_contain(table, key, bits, hashes):
    return all(
        table[pos >> 3] & (1 << (pos & 7))
        for pos in _bloom_positions(key, bits, hashes)
    )


def _stamp_of(path):
    """Identity of the data file's current content, for change detection."""
    st = os.stat(path)
    return (st.st_ino, st.st_size, st.st_mtime_ns)


def _verified_body(document, what):
    """Check the integrity checksum of one document; return its body."""
    if "checksum" not in document:
        raise ValueError(f"{what} is missing integrity information")
    digest = document["checksum"]
    if not isinstance(digest, str) or not digest:
        raise ValueError(f"{what} has a malformed integrity checksum")
    body = {key: value for key, value in document.items() if key != "checksum"}
    expected = _checksum(body).encode("ascii")
    if not hmac.compare_digest(digest.encode("utf-8", "ignore"), expected):
        raise ValueError(f"{what} integrity checksum does not match")
    return body


def _version_of(document):
    if "version" not in document:
        raise ValueError("state file is missing a version number")
    version = document["version"]
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError("state file has an invalid version")
    return version


def _check_entry(item, what):
    """Validate one ``[key, first_seen]`` pair and return it."""
    if (
        not isinstance(item, list)
        or len(item) != 2
        or not isinstance(item[0], str)
        or not _is_number(item[1])
    ):
        raise ValueError(f"{what} holds a malformed key entry")
    return item[0], item[1]


def _validate_keys(raw_keys, now, span, capacity, what="state file"):
    """Validate a retained key list of a snapshot; return entries."""
    if not isinstance(raw_keys, list):
        raise ValueError(f"{what} keys must be a list")
    if len(raw_keys) > capacity:
        raise ValueError(f"{what} holds more keys than the capacity allows")
    entries = []
    seen_keys = set()
    previous = None
    for item in raw_keys:
        key, first = _check_entry(item, what)
        if key in seen_keys:
            raise ValueError(f"{what} holds a duplicate key")
        if first < 0 or first > now:
            raise ValueError(f"{what} holds a sighting outside the timeline")
        if now - first > span:
            raise ValueError(f"{what} holds a key older than the span")
        if previous is not None and first < previous:
            raise ValueError(f"{what} keys are not ordered by first sighting")
        seen_keys.add(key)
        previous = first
        entries.append([key, first])
    return entries


def _validate_settings_body(body, *, what, require_version):
    """Validate span/capacity/now/counts/keys of one snapshot document."""
    required = set(_REQUIRED_FIELDS)
    if not require_version:
        required.discard("version")
    missing = required - body.keys()
    if missing:
        raise ValueError(f"{what} is missing fields: {sorted(missing)}")
    span = body["span"]
    capacity = body["capacity"]
    now = body["now"]
    admitted = body["admitted"]
    expired = body["expired"]
    if not _is_number(span) or not span > 0:
        raise ValueError(f"{what} has an invalid span")
    if not _is_number(capacity) or not capacity > 0:
        raise ValueError(f"{what} has an invalid capacity")
    if not _is_number(now) or now < 0:
        raise ValueError(f"{what} has an invalid current time")
    if not _is_count(admitted):
        raise ValueError(f"{what} has an invalid admitted count")
    if not _is_count(expired):
        raise ValueError(f"{what} has an invalid expired count")
    entries = _validate_keys(body["keys"], now, span, capacity, what)
    return span, capacity, now, admitted, expired, entries


def _validate_snapshot(body):
    """Validate the fields every on-disk snapshot carries."""
    return _validate_settings_body(body, what="state file", require_version=True)


def _validate_receipts(raw, what):
    """Validate an ordered list of delivery receipts; return entries.

    Each entry is ``[delivery_id, watermark, events, kinds]`` with events a
    list of ``[key, timestamp]`` pairs and kinds one classification per event.
    Identifiers are non-empty strings, unique within the list, kept in first
    successful commit order.  The on-disk values must be well typed (an int
    and an equal float are equivalent when a replay compares against them, but
    the stored bytes themselves must be valid).
    """
    if not isinstance(raw, list):
        raise ValueError(f"{what} receipts must be a list")
    if len(raw) > _RECEIPT_LIMIT:
        raise ValueError(f"{what} holds more receipts than the receipt limit")
    receipts = []
    seen_ids = set()
    for item in raw:
        if not isinstance(item, list) or len(item) != 4:
            raise ValueError(f"{what} holds a malformed receipt")
        delivery_id, watermark, events, kinds = item
        if not isinstance(delivery_id, str) or not delivery_id:
            raise ValueError(f"{what} holds a receipt with a malformed identifier")
        if delivery_id in seen_ids:
            raise ValueError(f"{what} holds a duplicate receipt identifier")
        if not _is_finite_time(watermark):
            raise ValueError(f"{what} holds a receipt with an invalid watermark")
        if not isinstance(events, list):
            raise ValueError(f"{what} holds a receipt with malformed events")
        prepared = []
        for pair in events:
            key, timestamp = _check_entry(pair, what)
            if not _is_finite_time(timestamp) or timestamp > watermark:
                raise ValueError(f"{what} holds a receipt event outside the timeline")
            prepared.append([key, timestamp])
        if (
            not isinstance(kinds, list)
            or len(kinds) != len(prepared)
            or not all(isinstance(kind, str) and kind in _EVENT_KINDS
                       for kind in kinds)
        ):
            raise ValueError(f"{what} holds a receipt with malformed results")
        seen_ids.add(delivery_id)
        receipts.append([delivery_id, watermark, prepared, list(kinds)])
    return receipts


def _check_receipt_advance(old, new, now):
    """Validate the ring transition of one commit at an anchor boundary.

    An anchor is one commit past its predecessor, so the ring is either
    unchanged (a non-delivery commit) or gains exactly one entry at the tail
    -- dropping the head first only when the limit is reached -- whose
    watermark equals the anchor's current time.  The entries themselves are
    already checksummed and structurally validated.
    """
    if new == old:
        return
    if not (len(old) <= _RECEIPT_LIMIT and len(new) <= _RECEIPT_LIMIT):
        raise ValueError("state file anchor receipts are inconsistent")
    dropped = len(old) + 1 - len(new)
    if dropped not in (0, 1):
        raise ValueError("state file anchor receipts are inconsistent")
    if old[dropped:] != new[:len(old) - dropped]:
        raise ValueError("state file anchor receipts are inconsistent")
    added = new[-1]
    if added[0] in {entry[0] for entry in old} or added[1] != now:
        raise ValueError("state file anchor receipts are inconsistent")


class _ParsedState:
    """The verified result of reading a state file."""

    __slots__ = (
        "version",
        "span",
        "capacity",
        "now",
        "admitted",
        "expired",
        "entries",
        "receipts",
        "seq",
        "tip",
        "anchor_bytes",
        "delta_bytes",
        "delta_count",
        "good_offset",
    )

    def __init__(self, **fields):
        for name in self.__slots__:
            setattr(self, name, fields[name])


# ---------------------------------------------------------------------------
# Version 1 and version 2 parsing (legacy documents, still readable)
# ---------------------------------------------------------------------------


def _parse_v1(document, size):
    """Parse a version 1 document: one whole-state JSON object."""
    body = _verified_body(document, "state file")
    span, capacity, now, admitted, expired, entries = _validate_snapshot(body)
    return _ParsedState(
        version=1,
        span=span,
        capacity=capacity,
        now=now,
        admitted=admitted,
        expired=expired,
        entries=entries,
        receipts=[],
        seq=0,
        tip=document["checksum"],
        anchor_bytes=size,
        delta_bytes=0,
        delta_count=0,
        good_offset=size,
    )


def _parse_v2_base(base_doc):
    """Validate a version 2 base snapshot line."""
    if not isinstance(base_doc, dict) or base_doc.get("kind") != "base":
        raise ValueError("state file has an invalid base snapshot")
    body = _verified_body(base_doc, "state file")
    seq = body.get("seq")
    if not _is_count(seq):
        raise ValueError("state file has an invalid sequence number")
    span, capacity, now, admitted, expired, entries = _validate_snapshot(body)
    return body, seq, span, capacity, now, admitted, expired, entries


def _parse_v2_single(document, size):
    """Parse a version 2 file holding only its base snapshot."""
    _, seq, span, capacity, now, admitted, expired, entries = _parse_v2_base(document)
    return _ParsedState(
        version=2,
        span=span,
        capacity=capacity,
        now=now,
        admitted=admitted,
        expired=expired,
        entries=entries,
        receipts=[],
        seq=seq,
        tip=document["checksum"],
        anchor_bytes=size,
        delta_bytes=0,
        delta_count=0,
        good_offset=size,
    )


def _apply_v2_segment(line, *, span, capacity, now, admitted, expired, seq, tip,
                      entries, index, max_first):
    """Validate and replay one version 2 delta segment.

    Returns the new ``(now, admitted, expired, seq, tip, max_first)``.
    """
    try:
        document = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"state file segment is not valid JSON: {exc}") from exc
    if not isinstance(document, dict) or document.get("kind") != "delta":
        raise ValueError("state file holds a malformed segment")
    body = _verified_body(document, "state file segment")
    fields = ("seq", "prev", "op", "now", "admitted", "expired", "drop", "add")
    if any(field not in body for field in fields):
        raise ValueError("state file holds a malformed segment")
    seg_seq = body["seq"]
    if not isinstance(seg_seq, int) or isinstance(seg_seq, bool) or seg_seq != seq + 1:
        raise ValueError("state file segments are out of order")
    if body["prev"] != tip:
        raise ValueError("state file segment chain is broken")
    op = body["op"]
    if op not in ("admit", "sweep"):
        raise ValueError("state file holds a malformed segment")
    new_now = body["now"]
    if not _is_number(new_now) or new_now < now:
        raise ValueError("state file segment moves time backwards")
    new_admitted = body["admitted"]
    new_expired = body["expired"]
    if not _is_count(new_admitted) or not _is_count(new_expired):
        raise ValueError("state file segment has an invalid count")
    drops = body["drop"]
    if not isinstance(drops, list) or not all(isinstance(key, str) for key in drops):
        raise ValueError("state file segment holds malformed evictions")
    raw_adds = body["add"]
    if not isinstance(raw_adds, list):
        raise ValueError("state file segment holds a malformed key entry")
    adds = []
    for item in raw_adds:
        key, first = _check_entry(item, "state file segment")
        if key in index:
            raise ValueError("state file segment admits a duplicate key")
        if first < 0 or first > new_now:
            raise ValueError("state file segment holds a sighting outside the timeline")
        if new_now - first > span:
            raise ValueError("state file segment holds a key older than the span")
        if first < max_first:
            raise ValueError("state file segment keys are not ordered by first sighting")
        adds.append([key, first])
    if len(drops) > len(entries) or [key for key, _ in entries[:len(drops)]] != drops:
        raise ValueError("state file segment evictions do not match the key order")
    if op == "admit":
        if new_admitted != admitted + len(adds) or new_expired != expired:
            raise ValueError("state file segment counts are inconsistent")
    else:
        if adds or new_admitted != admitted or new_expired != expired + len(drops):
            raise ValueError("state file segment counts are inconsistent")
    if drops:
        del entries[:len(drops)]
        for key in drops:
            index.discard(key)
    for key, first in adds:
        entries.append([key, first])
        index.add(key)
        max_first = first
    if len(entries) > capacity:
        raise ValueError("state file holds more keys than the capacity allows")
    return new_now, new_admitted, new_expired, seg_seq, document["checksum"], max_first


def _parse_v2_log(text):
    """Parse a log-structured version 2 file: base line plus delta lines.

    Only the final line may be torn (an interrupted append); it is ignored
    and its offset recorded so the next writer can truncate it.
    """
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise ValueError("state file is not valid JSON: the file is empty")
    try:
        base_doc = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise ValueError(f"state file is not valid JSON: {exc}") from exc
    if not isinstance(base_doc, dict):
        raise ValueError("state file must contain a JSON object")
    if _version_of(base_doc) != 2:
        raise ValueError("state file has an unsupported version")
    _, seq, span, capacity, now, admitted, expired, entries = _parse_v2_base(base_doc)
    tip = base_doc["checksum"]
    index = {key for key, _ in entries}
    max_first = entries[-1][1] if entries else 0
    base_bytes = len(lines[0].encode("utf-8")) + 1
    good_offset = base_bytes
    segment_bytes = 0
    segment_count = 0
    last = len(lines) - 1
    for i in range(1, len(lines)):
        line_len = len(lines[i].encode("utf-8")) + 1
        try:
            now, admitted, expired, seq, tip, max_first = _apply_v2_segment(
                lines[i],
                span=span,
                capacity=capacity,
                now=now,
                admitted=admitted,
                expired=expired,
                seq=seq,
                tip=tip,
                entries=entries,
                index=index,
                max_first=max_first,
            )
        except ValueError:
            if i == last:
                break  # torn tail of an interrupted append: ignore it
            raise
        good_offset += line_len
        segment_bytes += line_len
        segment_count += 1
    return _ParsedState(
        version=2,
        span=span,
        capacity=capacity,
        now=now,
        admitted=admitted,
        expired=expired,
        entries=entries,
        receipts=[],
        seq=seq,
        tip=tip,
        anchor_bytes=base_bytes,
        delta_bytes=segment_bytes,
        delta_count=segment_count,
        good_offset=good_offset,
    )


# ---------------------------------------------------------------------------
# Version 3 parsing: base, appended anchor checkpoints and delta segments
# ---------------------------------------------------------------------------


def _v3_anchor_info(document, *, allow_base):
    """Validate one v3 base/anchor record; return its snapshot info."""
    kind = document.get("kind") if isinstance(document, dict) else None
    if allow_base:
        kinds = ("base", "anchor")
    else:
        kinds = ("anchor",)
    if kind not in kinds:
        raise ValueError("state file has an invalid anchor record")
    body = _verified_body(document, "state file")
    seq = body.get("seq")
    if not _is_count(seq):
        raise ValueError("state file has an invalid sequence number")
    if kind == "base":
        if "prev" in body:
            raise ValueError("state file base must not name a predecessor")
        prev = None
    else:
        if seq <= 0:
            raise ValueError("state file anchor precedes every commit")
        prev = body.get("prev")
        if not isinstance(prev, str):
            raise ValueError("state file anchor has a malformed predecessor")
    span, capacity, now, admitted, expired, entries = _validate_snapshot(body)
    receipts = _validate_receipts(body.get("receipts", []), "state file")
    return {
        "kind": kind,
        "seq": seq,
        "prev": body.get("prev"),
        "tip": document["checksum"],
        "span": span,
        "capacity": capacity,
        "now": now,
        "admitted": admitted,
        "expired": expired,
        "entries": entries,
        "receipts": receipts,
    }


def _apply_v3_delta(line, state, index, max_first):
    """Validate and replay one v3 delta; mutate ``state`` in place.

    The delta is small (evicted keys plus newly admitted ``[key, first]``
    pairs); the post-commit state is derived by replay, never stored.
    """
    try:
        document = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"state file segment is not valid JSON: {exc}") from exc
    if not isinstance(document, dict) or document.get("kind") != "delta":
        raise ValueError("state file holds a malformed segment")
    body = _verified_body(document, "state file segment")
    common = ("seq", "prev", "op")
    if any(field not in body for field in common):
        raise ValueError("state file holds a malformed segment")
    seg_seq = body["seq"]
    if not isinstance(seg_seq, int) or isinstance(seg_seq, bool) or seg_seq <= 0:
        raise ValueError("state file has an invalid sequence number")
    if seg_seq != state["seq"] + 1:
        raise ValueError("state file segments are out of order")
    if body["prev"] != state["tip"]:
        raise ValueError("state file segment chain is broken")
    op = body["op"]
    if op == "batch":
        return _apply_v3_batch_delta(
            body, state, index, max_first, document["checksum"]
        )
    if op == "events":
        return _apply_v3_events_delta(
            body, state, index, max_first, document["checksum"]
        )
    if op == "deliver":
        return _apply_v3_deliver_delta(
            body, state, index, max_first, document["checksum"]
        )
    fields = ("now", "admitted", "expired", "drop", "add")
    if any(field not in body for field in fields):
        raise ValueError("state file holds a malformed segment")
    if op not in ("admit", "sweep"):
        raise ValueError("state file holds a malformed segment")
    new_now = body["now"]
    if not _is_number(new_now) or new_now < state["now"]:
        raise ValueError("state file segment moves time backwards")
    new_admitted = body["admitted"]
    new_expired = body["expired"]
    if not _is_count(new_admitted) or not _is_count(new_expired):
        raise ValueError("state file segment has an invalid count")
    drops = body["drop"]
    if not isinstance(drops, list) or not all(isinstance(key, str) for key in drops):
        raise ValueError("state file segment holds malformed evictions")
    raw_adds = body["add"]
    if not isinstance(raw_adds, list):
        raise ValueError("state file segment holds a malformed key entry")
    entries = state["entries"]
    adds = []
    for item in raw_adds:
        key, first = _check_entry(item, "state file segment")
        if key in index:
            raise ValueError("state file segment admits a duplicate key")
        if first < 0 or first > new_now:
            raise ValueError("state file segment holds a sighting outside the timeline")
        if new_now - first > state["span"]:
            raise ValueError("state file segment holds a key older than the span")
        if first < max_first:
            raise ValueError("state file segment keys are not ordered by first sighting")
        adds.append([key, first])
    if len(drops) > len(entries) or [key for key, _ in entries[:len(drops)]] != drops:
        raise ValueError("state file segment evictions do not match the key order")
    if op == "admit":
        if new_admitted != state["admitted"] + len(adds) or new_expired != state["expired"]:
            raise ValueError("state file segment counts are inconsistent")
    else:
        if adds or new_admitted != state["admitted"] or new_expired != state["expired"] + len(drops):
            raise ValueError("state file segment counts are inconsistent")
    if drops:
        del entries[:len(drops)]
        for key in drops:
            index.discard(key)
    for key, first in adds:
        entries.append([key, first])
        index.add(key)
        max_first = first
    if len(entries) > state["capacity"]:
        raise ValueError("state file holds more keys than the capacity allows")
    state["now"] = new_now
    state["admitted"] = new_admitted
    state["expired"] = new_expired
    state["seq"] = seg_seq
    state["tip"] = document["checksum"]
    return max_first


def _apply_v3_batch_delta(body, state, index, max_first, checksum):
    """Validate and replay one v3 atomic batch segment.

    A batch records the input keys and the per-item admission results; the
    post-commit retained set is fully determined by replaying the inputs
    against the pre-batch state, so replay recomputes it and cross-checks
    every stored field, including keys admitted and evicted again within the
    same commit.
    """
    fields = ("now", "admitted", "expired", "keys", "hits")
    if any(field not in body for field in fields):
        raise ValueError("state file holds a malformed segment")
    now = body["now"]
    if not _is_number(now) or now != state["now"]:
        raise ValueError("state file batch segment moves the current time")
    new_admitted = body["admitted"]
    new_expired = body["expired"]
    if not _is_count(new_admitted) or not _is_count(new_expired):
        raise ValueError("state file segment has an invalid count")
    raw_keys = body["keys"]
    raw_hits = body["hits"]
    if (
        not isinstance(raw_keys, list)
        or not raw_keys
        or not all(isinstance(key, str) for key in raw_keys)
        or not isinstance(raw_hits, list)
        or len(raw_hits) != len(raw_keys)
        or not all(isinstance(hit, bool) for hit in raw_hits)
    ):
        raise ValueError("state file batch segment is malformed")
    simulated = [list(pair) for pair in state["entries"]]
    simulated_index = set(index)
    expected_hits = []
    capacity = state["capacity"]
    for key in raw_keys:
        if key in simulated_index:
            expected_hits.append(False)
            continue
        expected_hits.append(True)
        while len(simulated) >= capacity:
            oldest, _ = simulated.pop(0)
            simulated_index.discard(oldest)
        simulated.append([key, now])
        simulated_index.add(key)
    if raw_hits != expected_hits:
        raise ValueError("state file batch segment results are inconsistent")
    admitted_count = sum(1 for hit in expected_hits if hit)
    if new_admitted != state["admitted"] + admitted_count:
        raise ValueError("state file segment counts are inconsistent")
    if new_expired != state["expired"]:
        # Capacity eviction inside a batch is never an expiry.
        raise ValueError("state file segment counts are inconsistent")
    if len(simulated) > capacity:
        raise ValueError("state file holds more keys than the capacity allows")
    state["entries"] = simulated
    index.clear()
    index.update(simulated_index)
    state["admitted"] = new_admitted
    state["expired"] = new_expired
    state["seq"] = body["seq"]
    state["tip"] = checksum
    return simulated[-1][1] if simulated else max_first


_EVENT_KINDS = ("admitted", "duplicate", "late")


def _insert_by_first(entries, pair):
    """Insert ``[key, first]`` ordered by first sighting, ties last.

    Equal timestamps keep admission order: an earlier entry (retained from a
    previous commit or admitted earlier in this batch) stays ahead of a new
    one.
    """
    timestamp = pair[1]
    lo, hi = 0, len(entries)
    while lo < hi:
        mid = (lo + hi) // 2
        if entries[mid][1] <= timestamp:
            lo = mid + 1
        else:
            hi = mid
    entries.insert(lo, pair)


def _simulate_event_batch(state, index, max_first, new_now, events):
    """Replay an event-time batch against a simulated pre-batch state.

    Advances the simulated window to ``new_now`` (expiring the span prefix)
    and classifies each ``(key, timestamp)`` event in input order.  Returns
    the simulated retained entries/index, the per-event kinds, the admission
    and expiry counts and the new max first sighting, without touching
    ``state`` itself.
    """
    span = state["span"]
    capacity = state["capacity"]
    simulated = [list(pair) for pair in state["entries"]]
    simulated_index = set(index)
    expiry_count = 0
    while simulated and new_now - simulated[0][1] > span:
        oldest, _ = simulated.pop(0)
        simulated_index.discard(oldest)
        expiry_count += 1
    expected_kinds = []
    admitted_count = 0
    for key, timestamp in events:
        if new_now - timestamp > span:
            expected_kinds.append("late")
            continue
        if key in simulated_index:
            expected_kinds.append("duplicate")
            continue
        expected_kinds.append("admitted")
        admitted_count += 1
        while len(simulated) >= capacity:
            oldest, _ = simulated.pop(0)
            simulated_index.discard(oldest)
        pair = [key, timestamp]
        _insert_by_first(simulated, pair)
        simulated_index.add(key)
    new_max_first = simulated[-1][1] if simulated else max_first
    return (
        simulated,
        simulated_index,
        expected_kinds,
        admitted_count,
        expiry_count,
        new_max_first,
    )


def _parse_segment_events(raw_events, new_now):
    """Validate a segment's ``events`` field; return ``(key, timestamp)`` list."""
    if not isinstance(raw_events, list):
        raise ValueError("state file events segment is malformed")
    events = []
    for item in raw_events:
        key, timestamp = _check_entry(item, "state file segment")
        if not _is_finite_time(timestamp) or timestamp > new_now:
            raise ValueError("state file segment holds an event outside the timeline")
        events.append((key, timestamp))
    return events


def _adopt_simulated(state, index, simulated, simulated_index, new_now,
                     new_admitted, new_expired, seq, checksum):
    """Commit a simulated post-batch state into the replay ``state``."""
    if len(simulated) > state["capacity"]:
        raise ValueError("state file holds more keys than the capacity allows")
    state["entries"] = simulated
    index.clear()
    index.update(simulated_index)
    state["now"] = new_now
    state["admitted"] = new_admitted
    state["expired"] = new_expired
    state["seq"] = seq
    state["tip"] = checksum


def _apply_v3_events_delta(body, state, index, max_first, checksum):
    """Validate and replay one v3 event-time batch segment.

    Like the key batch segment, the segment stores the inputs (each event's
    key and timestamp) and the per-item result kinds; replay advances the
    watermark, expires the span prefix and recomputes every classification,
    including a key expired or capacity-evicted and readmitted within the
    same commit.
    """
    fields = ("now", "admitted", "expired", "wmark", "events", "kinds")
    if any(field not in body for field in fields):
        raise ValueError("state file holds a malformed segment")
    pre_now = state["now"]
    wmark = body["wmark"]
    if not _is_finite_time(wmark) or wmark != pre_now:
        raise ValueError("state file events segment moves the previous watermark")
    new_now = body["now"]
    if not _is_finite_time(new_now) or new_now < pre_now:
        raise ValueError("state file segment moves time backwards")
    new_admitted = body["admitted"]
    new_expired = body["expired"]
    if not _is_count(new_admitted) or not _is_count(new_expired):
        raise ValueError("state file segment has an invalid count")
    raw_kinds = body["kinds"]
    events = _parse_segment_events(body["events"], new_now)
    if (
        not isinstance(raw_kinds, list)
        or len(raw_kinds) != len(events)
        or not all(isinstance(kind, str) and kind in _EVENT_KINDS
                   for kind in raw_kinds)
    ):
        raise ValueError("state file events segment is malformed")

    (
        simulated,
        simulated_index,
        expected_kinds,
        admitted_count,
        expiry_count,
        new_max_first,
    ) = _simulate_event_batch(state, index, max_first, new_now, events)
    if raw_kinds != expected_kinds:
        raise ValueError("state file events segment results are inconsistent")
    if new_admitted != state["admitted"] + admitted_count:
        raise ValueError("state file segment counts are inconsistent")
    if new_expired != state["expired"] + expiry_count:
        raise ValueError("state file segment counts are inconsistent")
    _adopt_simulated(
        state, index, simulated, simulated_index, new_now,
        new_admitted, new_expired, body["seq"], checksum,
    )
    return new_max_first


def _apply_v3_deliver_delta(body, state, index, max_first, checksum):
    """Validate and replay one v3 idempotent delivery segment.

    A delivery behaves exactly like an event-time batch, and additionally
    records its receipt: the identifier, requested watermark, ordered inputs
    and original result kinds, plus the ring of receipts after evicting the
    oldest entries beyond the limit.  Replay recomputes the classification and
    cross-checks it against the stored receipt, so a tampered input or result
    is detected even though the envelope checksum still verifies.
    """
    fields = (
        "now", "admitted", "expired", "wmark", "events", "kinds",
        "receipt", "receipts",
    )
    if any(field not in body for field in fields):
        raise ValueError("state file holds a malformed segment")
    pre_now = state["now"]
    wmark = body["wmark"]
    if not _is_finite_time(wmark) or wmark != pre_now:
        raise ValueError("state file events segment moves the previous watermark")
    new_now = body["now"]
    if not _is_finite_time(new_now) or new_now < pre_now:
        raise ValueError("state file segment moves time backwards")
    new_admitted = body["admitted"]
    new_expired = body["expired"]
    if not _is_count(new_admitted) or not _is_count(new_expired):
        raise ValueError("state file segment has an invalid count")
    raw_kinds = body["kinds"]
    events = _parse_segment_events(body["events"], new_now)
    if (
        not isinstance(raw_kinds, list)
        or len(raw_kinds) != len(events)
        or not all(isinstance(kind, str) and kind in _EVENT_KINDS
                   for kind in raw_kinds)
    ):
        raise ValueError("state file events segment is malformed")

    receipt = body["receipt"]
    if not isinstance(receipt, list) or len(receipt) != 3:
        raise ValueError("state file delivery segment holds a malformed receipt")
    delivery_id, req_wmark, req_events = receipt
    if not isinstance(delivery_id, str) or not delivery_id:
        raise ValueError("state file delivery segment holds a malformed identifier")
    if not _is_finite_time(req_wmark) or req_wmark != new_now:
        raise ValueError("state file delivery segment holds an inconsistent watermark")
    stored_reqs = _parse_segment_events(req_events, new_now)
    if stored_reqs != list(events):
        raise ValueError("state file delivery segment receipt does not match its events")

    prior = state.get("receipts")
    if prior is None:
        # Replaying a delivery delta without a receipt-bearing predecessor is
        # unrecoverable state, never a silently empty ring.
        raise ValueError("state file delivery segment is missing its receipt history")
    if any(entry[0] == delivery_id for entry in prior):
        raise ValueError("state file delivery segment repeats a live receipt identifier")
    receipts = _validate_receipts(body["receipts"], "state file segment")
    expected_ring = prior + [[delivery_id, new_now,
                              [[k, t] for k, t in events], list(raw_kinds)]]
    if len(expected_ring) > _RECEIPT_LIMIT:
        expected_ring = expected_ring[len(expected_ring) - _RECEIPT_LIMIT:]
    if receipts != expected_ring:
        raise ValueError("state file delivery segment receipts are inconsistent")

    (
        simulated,
        simulated_index,
        expected_kinds,
        admitted_count,
        expiry_count,
        new_max_first,
    ) = _simulate_event_batch(state, index, max_first, new_now, events)
    if raw_kinds != expected_kinds:
        raise ValueError("state file events segment results are inconsistent")
    if new_admitted != state["admitted"] + admitted_count:
        raise ValueError("state file segment counts are inconsistent")
    if new_expired != state["expired"] + expiry_count:
        raise ValueError("state file segment counts are inconsistent")
    _adopt_simulated(
        state, index, simulated, simulated_index, new_now,
        new_admitted, new_expired, body["seq"], checksum,
    )
    state["receipts"] = receipts
    return new_max_first



def _state_from_anchor(info):
    return {
        "seq": info["seq"],
        "tip": info["tip"],
        "span": info["span"],
        "capacity": info["capacity"],
        "now": info["now"],
        "admitted": info["admitted"],
        "expired": info["expired"],
        "entries": [list(pair) for pair in info["entries"]],
        "receipts": [[r[0], r[1], [list(p) for p in r[2]], list(r[3])]
                     for r in info["receipts"]],
    }


def _split_lines(text):
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _parse_v3(text):
    """Parse a version 3 log from its base snapshot.

    Every record is checksum-verified and the prev/seq chain is checked
    against the record immediately before it, so a tampered record is caught
    even when an anchor follows it.  Only the final line may be torn (an
    interrupted append); it is ignored.
    """
    lines = _split_lines(text)
    if not lines:
        raise ValueError("state file is not valid JSON: the file is empty")
    last = len(lines) - 1

    def load_line(i):
        try:
            return json.loads(lines[i])
        except json.JSONDecodeError as exc:
            raise ValueError(f"state file is not valid JSON: {exc}") from exc

    # The first record is always the base snapshot (commit 0).
    base_doc = load_line(0)
    if not isinstance(base_doc, dict):
        raise ValueError("state file must contain a JSON object")
    base_info = _v3_anchor_info(base_doc, allow_base=True)
    if base_info["kind"] != "base":
        raise ValueError("state file has an invalid base snapshot")

    state = _state_from_anchor(base_info)
    index = {key for key, _ in state["entries"]}
    max_first = state["entries"][-1][1] if state["entries"] else 0
    base_len = len(lines[0].encode("utf-8")) + 1
    good_offset = base_len
    delta_bytes = 0
    delta_count = 0
    anchor_len = base_len
    prev_tip = base_info["tip"]
    prev_seq = base_info["seq"]

    for i in range(1, len(lines)):
        line_len = len(lines[i].encode("utf-8")) + 1
        torn = i == last
        try:
            document = load_line(i)
            if not isinstance(document, dict):
                raise ValueError("state file must contain a JSON object")
            kind = document.get("kind")
            if kind == "anchor":
                info = _v3_anchor_info(document, allow_base=False)
                if info["prev"] != prev_tip or info["seq"] != prev_seq + 1:
                    raise ValueError("state file anchor chain is broken")
                if info["span"] != state["span"] or info["capacity"] != state["capacity"]:
                    raise ValueError("state file anchor changes the settings")
                # Counts are monotone across commits; the anchor's own
                # checksum already binds its snapshot to its bytes.
                if info["admitted"] < state["admitted"] or info["expired"] < state["expired"]:
                    raise ValueError("state file anchor counts are inconsistent")
                _check_receipt_advance(
                    state["receipts"], info["receipts"], info["now"]
                )
            elif kind == "delta":
                max_first = _apply_v3_delta(lines[i], state, index, max_first)
            else:
                raise ValueError("state file holds a malformed record")
        except ValueError:
            if torn:
                break  # torn tail of an interrupted append: ignore it
            raise
        if kind == "anchor":
            state = _state_from_anchor(info)
            index = {key for key, _ in state["entries"]}
            max_first = state["entries"][-1][1] if state["entries"] else 0
            anchor_len = line_len
            delta_bytes = 0
            delta_count = 0
        else:
            delta_bytes += line_len
            delta_count += 1
        prev_tip = state["tip"]
        prev_seq = state["seq"]
        good_offset += line_len

    return _ParsedState(
        version=3,
        span=state["span"],
        capacity=state["capacity"],
        now=state["now"],
        admitted=state["admitted"],
        expired=state["expired"],
        entries=state["entries"],
        receipts=state["receipts"],
        seq=state["seq"],
        tip=state["tip"],
        anchor_bytes=anchor_len,
        delta_bytes=delta_bytes,
        delta_count=delta_count,
        good_offset=good_offset,
    )


# ---------------------------------------------------------------------------
# File dispatch
# ---------------------------------------------------------------------------


def _decode(raw):
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"state file is not valid UTF-8: {exc}") from exc


def _parse_log_text(text):
    """Parse a multi-line log, dispatching on the version of its first line."""
    first_line = text.split("\n", 1)[0]
    try:
        first = json.loads(first_line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"state file is not valid JSON: {exc}") from exc
    if not isinstance(first, dict):
        raise ValueError("state file must contain a JSON object")
    version = _version_of(first)
    if version == 2:
        return _parse_v2_log(text)
    if version == 3:
        return _parse_v3(text)
    raise ValueError("state file has an unsupported version")


def _parse_state_file(path):
    """Read, verify and replay the state file at ``path``.

    Raises FileNotFoundError when the file is absent and ValueError for
    every form of corruption: bad formatting, a missing or unknown version,
    a missing or mismatched checksum, a broken chain or state that violates
    the window invariants.  A torn final record left by a crashed writer is
    not corruption; it is ignored.

    The read takes no lock.  It observes one whole file content: a commit
    appended concurrently can only tear the final line (ignored), and a
    compaction or restore that replaces the file via ``os.replace`` lands as
    either the old or the new complete file -- never a mixture.
    """
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ValueError(f"state file cannot be read: {exc}") from exc
    return _parse_state_bytes(raw)


def _parse_state_bytes(raw):
    """Verify and replay one whole state file's contents (any version)."""
    text = _decode(raw)
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        document = None
    if document is not None:
        # A single JSON object: a version 1/2 document, or a v3 file that so
        # far holds only its base snapshot.
        if not isinstance(document, dict):
            raise ValueError("state file must contain a JSON object")
        version = _version_of(document)
        if version == 1:
            return _parse_v1(document, len(raw))
        if version == 2:
            return _parse_v2_single(document, len(raw))
        if version == 3:
            info = _v3_anchor_info(document, allow_base=True)
            if info["kind"] != "base":
                raise ValueError("state file has an invalid base snapshot")
            return _ParsedState(
                version=3,
                span=info["span"],
                capacity=info["capacity"],
                now=info["now"],
                admitted=info["admitted"],
                expired=info["expired"],
                entries=info["entries"],
                receipts=info["receipts"],
                seq=info["seq"],
                tip=info["tip"],
                anchor_bytes=len(raw),
                delta_bytes=0,
                delta_count=0,
                good_offset=len(raw),
            )
        raise ValueError("state file has an unsupported version")
    return _parse_log_text(text)


def _parse_tip_state(path):
    """Reconstruct only the latest committed state, without the lock.

    Starts replay at the newest anchor record, so the work is bounded by the
    records since the last compaction rather than by the whole history.  The
    anchor is itself checksummed and self-validating; full-chain validation
    stays with :func:`_parse_state_file`, used by ``load`` and every writer.
    Raises FileNotFoundError when absent and ValueError on corruption.
    """
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ValueError(f"state file cannot be read: {exc}") from exc
    return _parse_tip_bytes(raw)


def _parse_tip_bytes(raw):
    """Reconstruct the latest committed state from one file's contents.

    Same replay strategy as :func:`_parse_tip_state`, but takes the bytes the
    caller already read, so the caller decides how read failures surface.
    """
    text = _decode(raw)
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        document = None
    if document is not None:
        # One whole-state object: the file's single record, parse it fully.
        return _parse_state_bytes(raw)
    lines = _split_lines(text)
    if not lines:
        raise ValueError("state file is not valid JSON: the file is empty")
    last = len(lines) - 1
    first_doc = json.loads(lines[0])
    if not isinstance(first_doc, dict):
        raise ValueError("state file must contain a JSON object")
    version = _version_of(first_doc)
    if version == 2:
        # v2 has exactly one base and compaction rewrote the file, so replay
        # is already from the only anchor.
        return _parse_v2_log(text)
    if version != 3:
        raise ValueError("state file has an unsupported version")
    anchor_i = None
    anchor_info = None
    for i in range(last, -1, -1):
        try:
            doc = json.loads(lines[i])
        except json.JSONDecodeError:
            continue
        if isinstance(doc, dict) and doc.get("kind") in ("base", "anchor"):
            try:
                anchor_info = _v3_anchor_info(doc, allow_base=True)
            except ValueError:
                # A torn compaction anchor at the tail; try the previous one.
                continue
            anchor_i = i
            break
    if anchor_i is None:
        raise ValueError("state file has no recoverable anchor")
    info = anchor_info
    state = _state_from_anchor(info)
    index = {key for key, _ in state["entries"]}
    max_first = state["entries"][-1][1] if state["entries"] else 0
    for i in range(anchor_i + 1, len(lines)):
        try:
            max_first = _apply_v3_delta(lines[i], state, index, max_first)
        except ValueError:
            if i == last:
                break  # torn tail of an append in progress
            raise
    return _ParsedState(
        version=3,
        span=state["span"],
        capacity=state["capacity"],
        now=state["now"],
        admitted=state["admitted"],
        expired=state["expired"],
        entries=state["entries"],
        receipts=state["receipts"],
        seq=state["seq"],
        tip=state["tip"],
        anchor_bytes=0,
        delta_bytes=0,
        delta_count=0,
        good_offset=-1,
    )


def read_settings(path):
    """Return ``(span, capacity)`` from a state file without opening a Window.

    Used by the command line to learn the construction settings before the
    window can be built.  Raises ValueError for a missing or corrupt file.
    """
    parsed = _parse_state_file(path)
    return parsed.span, parsed.capacity


# ---------------------------------------------------------------------------
# Export documents
# ---------------------------------------------------------------------------


def _serialize_receipts(receipts):
    """Deep-copy a receipt ring into plain JSON-ready lists."""
    return [
        [
            delivery_id,
            watermark,
            [[key, timestamp] for key, timestamp in events],
            list(kinds),
        ]
        for delivery_id, watermark, events, kinds in receipts
    ]


def _export_body(snapshot):
    """The checksum-covered body of an export document.

    The receipt ring is included only when non-empty, so an export of a commit
    with no live receipts -- every commit of an old state included -- is byte
    for byte the document it always was.
    """
    body = {
        "format": _EXPORT_FORMAT,
        "seq": snapshot["seq"],
        "span": snapshot["span"],
        "capacity": snapshot["capacity"],
        "now": snapshot["now"],
        "admitted": snapshot["admitted"],
        "expired": snapshot["expired"],
        "keys": [[key, first] for key, first in snapshot["entries"]],
    }
    receipts = _serialize_receipts(snapshot.get("receipts", []))
    if receipts:
        body["receipts"] = receipts
    return body


def _make_export_document(snapshot):
    body = _export_body(snapshot)
    envelope = dict(body)
    envelope["checksum"] = _checksum(body)
    return envelope


def _validate_export(body):
    """Validate the contents of an export document; return its fields.

    A document without a ``receipts`` field is an old export and carries an
    empty ring.
    """
    if body.get("format") != _EXPORT_FORMAT:
        raise ValueError("export document has an unrecognized format")
    seq = body.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq <= 0:
        raise ValueError("export document has an invalid sequence number")
    span, capacity, now, admitted, expired, entries = _validate_settings_body(
        body, what="export document", require_version=False
    )
    receipts = _validate_receipts(body.get("receipts", []), "export document")
    return seq, span, capacity, now, admitted, expired, entries, receipts


class Window:
    """A bounded deduplication window persisted under a state directory."""

    def __init__(self, state, span, capacity):
        if not _is_number(span):
            raise TypeError("span must be an int or float")
        if not _is_number(capacity):
            raise TypeError("capacity must be an int or float")
        if not span > 0:
            raise ValueError("span must be greater than zero")
        if not capacity > 0:
            raise ValueError("capacity must be greater than zero")
        self._state_dir = os.fspath(state)
        self._span = span
        self._capacity = capacity
        self._now = 0
        self._clock = 0    # newest time this process itself asked for or loaded
        self._entries = []  # [key, first_seen] pairs, oldest sighting first
        self._index = {}    # key -> first_seen, mirrors _entries
        self._admitted = 0
        self._expired = 0
        # Idempotency receipts, oldest first-successful commit first; each is
        # [delivery_id, watermark, [[key, timestamp], ...], [kind, ...]].
        self._receipts = []
        self._last_commit = None    # checksum at the tip of the chain last seen
        self._seq = 0               # sequence number of the last commit
        self._loaded_version = _VERSION  # format of the file last read
        self._file_present = False
        self._file_stamp = None
        self._anchor_bytes = 0
        self._delta_bytes = 0
        self._delta_count = 0
        self._good_offset = 0  # end of the committed prefix of the data file
        # Construction has no filesystem side effects: read-only calls on a
        # window whose directory or data file is absent never create either.

    @contextlib.contextmanager
    def _locked(self, exclusive):
        """Hold a flock on the state directory; the directory inode is stable.

        Closing the fd releases the lock, so a process killed (or crashed)
        while holding it never leaves a deadlock behind.  The caller is
        responsible for ensuring the directory exists.
        """
        fd = os.open(self._state_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            os.close(fd)

    def _ensure_dir(self):
        os.makedirs(self._state_dir, exist_ok=True)

    # -- mutating, locked operations --------------------------------------

    def observe(self, key):
        """Record a sighting; return True only when the key is newly admitted.

        Runs as one locked, durable transaction so concurrent processes are
        serialized as if their calls ran one after another.
        """
        _check_key(key)
        self._ensure_dir()
        with self._locked(True):
            self._reload_if_present()
            admitted, evicted = self._admit(key)
            if admitted:
                self._commit("admit", drop=evicted, add=[[key, self._now]])
            return admitted

    def _admit(self, key):
        """Update memory for one sighting; return (admitted, evicted keys)."""
        if key in self._index:
            return False, []
        evicted = []
        while len(self._entries) >= self._capacity:
            oldest, _ = self._entries.pop(0)
            del self._index[oldest]
            evicted.append(oldest)
        self._entries.append([key, self._now])
        self._index[key] = self._now
        self._admitted += 1
        return True, evicted

    def observe_many(self, keys):
        """Record a batch of sightings as one atomic commit.

        ``keys`` must be a list of strings; the returned list of booleans
        aligns item by item with the input.  Semantics per item match
        :meth:`observe`, but every item uses the current time of the latest
        committed state at the start of the batch -- time never advances on
        its own -- and a key evicted by capacity earlier in the same batch is
        admitted again if it reappears.  Repeats never refresh a key's first
        sighting or its order.

        Validation runs before any filesystem access: a non-list or a
        non-string element raises ``TypeError`` and creates neither the
        state directory nor the data file; an empty list returns ``[]``
        without reading or creating state.  A valid, non-empty batch that
        meets a corrupt state or a settings mismatch raises ``ValueError``
        and leaves the original state untouched.  When at least one item is
        admitted the batch adds exactly one commit; an all-duplicate batch
        commits nothing.
        """
        if not isinstance(keys, list):
            raise TypeError("keys must be a list of strings")
        if not all(isinstance(key, str) for key in keys):
            raise TypeError("keys must be a list of strings")
        if not keys:
            return []
        self._ensure_dir()
        with self._locked(True):
            self._reload_if_present()
            now = self._now
            hits = []
            any_admitted = False
            for key in keys:
                if key in self._index:
                    hits.append(False)
                    continue
                hits.append(True)
                any_admitted = True
                while len(self._entries) >= self._capacity:
                    oldest, _ = self._entries.pop(0)
                    del self._index[oldest]
                self._entries.append([key, now])
                self._index[key] = now
                self._admitted += 1
            if any_admitted:
                self._commit("batch", [], [], batch_keys=keys, batch_hits=hits)
            return hits

    @staticmethod
    def _validate_event_request(events, watermark):
        """Validate an event batch's arguments; return prepared pairs.

        Mirrors the checks :meth:`observe_events` documents: wrong types raise
        ``TypeError``; missing fields, non-finite or negative times and events
        later than the watermark raise ``ValueError``.
        """
        if not isinstance(events, list):
            raise TypeError("events must be a list of event objects")
        if not _is_number(watermark):
            raise TypeError("watermark must be an int or float")
        if not math.isfinite(watermark) or watermark < 0:
            raise ValueError("watermark must be finite and non-negative")
        prepared = []
        for event in events:
            if not isinstance(event, dict):
                raise TypeError("each event must be an object with key and timestamp")
            if "key" not in event or "timestamp" not in event:
                raise ValueError("each event must carry key and timestamp")
            key = event["key"]
            timestamp = event["timestamp"]
            if not isinstance(key, str):
                raise TypeError("event key must be a string")
            if not _is_number(timestamp):
                raise TypeError("event timestamp must be an int or float")
            if not math.isfinite(timestamp) or timestamp < 0:
                raise ValueError("event timestamp must be finite and non-negative")
            if timestamp > watermark:
                raise ValueError("event timestamp is later than the watermark")
            prepared.append((key, timestamp))
        return prepared

    def _mutate_event_batch(self, prepared, watermark):
        """Advance time, expire and classify; caller holds the lock.

        Returns ``(kinds, previous_now, moved, dropped, admitted_any)``.
        """
        moved = watermark != self._now
        previous_now = self._now
        self._now = watermark
        dropped = []
        while self._entries and watermark - self._entries[0][1] > self._span:
            key, _ = self._entries.pop(0)
            del self._index[key]
            dropped.append(key)
        self._expired += len(dropped)
        kinds = []
        admitted_any = False
        for key, timestamp in prepared:
            if watermark - timestamp > self._span:
                kinds.append("late")
                continue
            if key in self._index:
                kinds.append("duplicate")
                continue
            kinds.append("admitted")
            admitted_any = True
            while len(self._entries) >= self._capacity:
                oldest, _ = self._entries.pop(0)
                del self._index[oldest]
            pair = [key, timestamp]
            _insert_by_first(self._entries, pair)
            self._index[key] = timestamp
            self._admitted += 1
        return kinds, previous_now, moved, dropped, admitted_any

    def observe_events(self, events, watermark):
        """Process one event-time batch against ``watermark``.

        ``events`` is a list of objects with string ``key`` and numeric
        ``timestamp`` fields; the returned list of strings -- one of
        ``"admitted"``, ``"duplicate"`` or ``"late"`` per item, in input
        order -- classifies each event.

        The whole batch runs as one locked, durable transaction: time first
        advances to ``watermark`` and keys older than the span expire, then
        the events are processed in input order.  An event whose timestamp
        is smaller than ``watermark - span`` is ``"late"`` (this beats the
        duplicate check; the boundary itself stays valid).  A non-late event
        whose key is retained is a ``"duplicate"`` and refreshes neither its
        first sighting nor its order; otherwise it is ``"admitted"`` and the
        key keeps that event's timestamp.  Retained keys stay ordered by
        first sighting, ties broken by admission order; a full window
        evicts the current oldest key first, so an evicted key readmitted
        later in the same batch counts as a fresh admission.

        ``admitted`` grows by the number of admissions; only time-based
        expiry grows ``expired`` -- late events, duplicates and capacity
        evictions touch neither counter.  Exactly one commit is added when
        the watermark advances, an expiry happens or an event is admitted;
        a batch that does none of that commits nothing.  An empty list
        still advances the watermark and expires.

        Validation completes before any filesystem access: a wrong type
        raises ``TypeError`` and a missing field, a non-finite or negative
        time, a watermark below the current time, an event after the
        watermark, a corrupt state or a settings mismatch raises
        ``ValueError``; neither creates the state directory nor changes an
        existing state file.
        """
        prepared = self._validate_event_request(events, watermark)
        # Monotonicity against this process's own clock is checkable without
        # touching the filesystem, so reject it before any directory exists.
        if watermark < self._clock:
            raise ValueError("watermark is earlier than the current time")
        # With no committed state on disk the window starts empty at time 0,
        # where the only no-op batch is an empty one at watermark 0.  Answer
        # that without creating the directory; everything else takes the
        # locked transaction path, which rechecks against the state on disk
        # (a concurrent process may have created it in the meantime).
        state_path = os.path.join(self._state_dir, _STATE_FILE)
        if (
            self._last_commit is None
            and not os.path.exists(state_path)
            and watermark == 0
            and not prepared
        ):
            return []
        self._ensure_dir()
        with self._locked(True):
            self._reload_if_present()
            if watermark < self._now:
                # Another process already committed past this watermark; a
                # regression through shared state is rejected, not contended.
                raise ValueError("watermark is earlier than the current time")
            self._clock = watermark
            kinds, previous_now, moved, dropped, admitted_any = (
                self._mutate_event_batch(prepared, watermark)
            )
            if moved or dropped or admitted_any:
                self._commit(
                    "events",
                    dropped,
                    [],
                    event_items=prepared,
                    event_kinds=kinds,
                    event_wmark=previous_now,
                )
            return kinds

    @staticmethod
    def _find_receipt(receipts, delivery_id):
        """Return the live receipt for ``delivery_id`` (exact string match)."""
        for receipt in receipts:
            if receipt[0] == delivery_id:
                return receipt
        return None

    def deliver_events(self, delivery_id, events, watermark):
        """Idempotently deliver one event-time batch.

        Behaves exactly like :meth:`observe_events` on the first successful
        delivery of ``delivery_id`` -- same classification, eviction and
        counter semantics, one commit shared by the event changes and the
        idempotency receipt (an empty batch or an all-duplicate batch commits
        too) -- and replays the original classification on every later call
        with the same identifier.

        A retained identifier replays only when the watermark and the ordered
        ``key``/``timestamp`` contents match (extra event fields are ignored;
        an int and a float compare equal when numerically equal).  A replay
        returns the original list without advancing time, moving a counter or
        touching the file, and succeeds even when its watermark now lags the
        current time.  A content mismatch raises ``ValueError``.  Identifiers
        are compared as raw strings; a non-string raises ``TypeError`` and an
        empty string raises ``ValueError``.  Once an identifier falls out of
        the newest 128 receipts it is treated as a new request, for which a
        watermark behind the current time raises ``ValueError``.

        As with :meth:`observe_events`, all argument validation completes
        before any filesystem access; a corrupt state or settings mismatch
        raises ``ValueError`` and a failed validation creates no directory.
        """
        if not isinstance(delivery_id, str):
            raise TypeError("delivery_id must be a string")
        if delivery_id == "":
            raise ValueError("delivery_id must not be empty")
        prepared = self._validate_event_request(events, watermark)
        # Unlike observe_events this always takes the lock: an empty or
        # all-duplicate delivery still records its receipt, and a live
        # identifier replays regardless of the current watermark, so the
        # fresh-window fast path and the process-clock shortcut do not apply.
        self._ensure_dir()
        with self._locked(True):
            self._reload_if_present()
            replay = self._find_receipt(self._receipts, delivery_id)
            if replay is not None:
                _, old_wmark, old_events, old_kinds = replay
                if (
                    old_wmark != watermark
                    or old_events != [[key, ts] for key, ts in prepared]
                ):
                    raise ValueError(
                        "delivery content does not match the original delivery"
                    )
                return list(old_kinds)
            if watermark < self._now or watermark < self._clock:
                # The identifier is not retained, so this is a new request;
                # new deliveries may not move the watermark backwards, either
                # past the shared committed time or this process's own clock.
                raise ValueError("watermark is earlier than the current time")
            self._clock = watermark
            kinds, previous_now, _moved, dropped, _admitted_any = (
                self._mutate_event_batch(prepared, watermark)
            )
            receipt = [
                delivery_id,
                watermark,
                [[key, ts] for key, ts in prepared],
                list(kinds),
            ]
            self._receipts.append(receipt)
            if len(self._receipts) > _RECEIPT_LIMIT:
                del self._receipts[: len(self._receipts) - _RECEIPT_LIMIT]
            self._commit(
                "deliver",
                dropped,
                [],
                event_items=prepared,
                event_kinds=kinds,
                event_wmark=previous_now,
                delivery_id=delivery_id,
            )
            return kinds

    def advance(self, now):
        """Move the current time to ``now`` and drop keys older than the span.

        Like :meth:`observe`, one locked and durable transaction.
        """
        if not _is_number(now):
            raise TypeError("now must be an int or float")
        self._ensure_dir()
        with self._locked(True):
            # Monotonicity is per process: this call must not go back past a
            # time this same process already loaded or requested.
            if now < self._clock:
                raise ValueError("now is earlier than the current time")
            self._reload_if_present()
            if now < self._now:
                # A newer commit (another process) already moved time at least
                # this far forward and performed the expiry this call would
                # do.  Treat it as contention, not as a clock regression.
                self._clock = max(self._clock, now)
                return 0
            moved = now != self._now
            self._now = now
            self._clock = now
            dropped = []
            while self._entries and now - self._entries[0][1] > self._span:
                key, _ = self._entries.pop(0)
                del self._index[key]
                dropped.append(key)
            self._expired += len(dropped)
            if moved or dropped:
                self._commit("sweep", drop=dropped, add=[])
            return len(dropped)

    @staticmethod
    def _check_restore_document(document):
        """Validate a would-be restore document without touching the filesystem.

        Pure: performs only the checksum/field checks :meth:`restore` performs
        before it creates the state directory, so a caller can reject an
        invalid document without any filesystem side effect.  Returns the
        fields ``restore`` adopts; raises ``ValueError`` on any mismatch.
        """
        body = _verified_body(document, "export document")
        return _validate_export(body)

    def restore(self, document):
        """Reset the whole window to the state an :meth:`export` captured.

        The reset is one atomic commit: after it returns, key order, counts,
        current time and settings match the document field by field and the
        following commits continue numbering from the exported commit's
        successor.  A crash either leaves the previous state fully intact or
        the restored state fully in place.

        Raises ``TypeError`` when ``document`` is not an export document
        object (no guessing from other types) and ``ValueError`` when a field
        is missing or mistyped, or the checksum does not match the contents.
        """
        if not isinstance(document, dict):
            raise TypeError("restore expects an export document object")
        (
            seq,
            span,
            capacity,
            now,
            admitted,
            expired,
            entries,
            receipts,
        ) = self._check_restore_document(document)
        self._ensure_dir()
        with self._locked(True):
            self._span = span
            self._capacity = capacity
            self._now = now
            self._admitted = admitted
            self._expired = expired
            self._entries = [list(pair) for pair in entries]
            self._index = {key: first for key, first in self._entries}
            self._receipts = _serialize_receipts(receipts)
            self._clock = now
            self._seq = seq
            # The restored state replaces the whole file atomically; it
            # becomes the base of a fresh chain at the exported commit.
            self._write_base_file()

    # -- read-only, lock-free operations ----------------------------------

    def _committed_or_memory(self):
        """Return one complete committed state, or this window's memory.

        Reads the data file without taking the lock, so a writer in progress
        neither blocks this call nor shows a half-written record.  When the
        data file is absent (nothing committed yet) or unreadable, memory is
        itself a complete state -- the empty window or this process's last
        good commit -- so a reader always receives whole state, never half.
        ``load`` and ``export`` are the strict entry points and report a
        corrupt file as ``ValueError`` instead.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            parsed = _parse_tip_state(path)
        except (FileNotFoundError, ValueError):
            parsed = None
        if parsed is None:
            return {
                "span": self._span,
                "capacity": self._capacity,
                "now": self._now,
                "admitted": self._admitted,
                "expired": self._expired,
                "entries": self._entries,
                "receipts": self._receipts,
                "seq": self._seq,
            }
        return {
            "span": parsed.span,
            "capacity": parsed.capacity,
            "now": parsed.now,
            "admitted": parsed.admitted,
            "expired": parsed.expired,
            "entries": parsed.entries,
            "receipts": parsed.receipts,
            "seq": parsed.seq,
        }

    def seen(self, key):
        """Report membership without recording anything or waiting on a writer.

        The answer comes from one single committed state; a commit landing
        concurrently can never split it across an old and a new state.
        """
        _check_key(key)
        snapshot = self._committed_or_memory()
        return any(k == key for k, _ in snapshot["entries"])

    def keys(self):
        """Retained keys of one committed state, oldest sighting first.

        Read without taking the lock, so a commit in progress neither blocks
        this nor shows a half-written state.
        """
        snapshot = self._committed_or_memory()
        return [key for key, _ in snapshot["entries"]]

    def stats(self):
        """Span, capacity, retained, admitted and expired counts.

        The retained number and the two counts come from the same single
        committed state as :meth:`keys`.
        """
        snapshot = self._committed_or_memory()
        return {
            "span": snapshot["span"],
            "capacity": snapshot["capacity"],
            "retained": len(snapshot["entries"]),
            "admitted": snapshot["admitted"],
            "expired": snapshot["expired"],
        }

    def probe_many(self, keys, bits=_PROBE_DEFAULT_BITS,
                   hashes=_PROBE_DEFAULT_HASHES):
        """Probe a batch of keys for membership in one committed state.

        Returns a report dict.  ``results`` holds one exact membership
        boolean per input key in input order: duplicates are answered
        separately and keys compare as raw strings, so the empty string
        and non-ASCII keys need no special casing.  ``seq`` is the commit
        the answers come from (0 when nothing is committed yet) and
        ``queries`` the number of input keys.  ``bloom_positive`` counts
        the keys a bloom index over the retained keys could not rule out,
        ``matches`` the keys exactly retained, ``false_positives`` the
        positives that missed, ``hit_rate`` is ``matches / queries`` and
        ``false_positive_rate`` is ``false_positives / (queries -
        matches)``; each ratio is 0 when its denominator is 0.

        The bloom index is built from the retained keys of the same single
        committed state the exact answers come from, and the read takes no
        lock, so a commit in progress neither blocks the probe nor splits
        it across two commits, and a commit, eviction or restore by
        another process is reflected in the next call.  A bloom negative
        is definitive; a positive is always rechecked against the exact
        set, so a hash collision can only add to ``false_positives``,
        never change ``results``.  The index occupies at most
        ``ceil(bits / 8)`` bytes and hashes deterministically, so the same
        retained set and parameters give the same positives in every
        process.

        All argument validation completes before the state is read:
        ``keys`` must be a list of strings and ``bits``/``hashes`` must be
        non-boolean integers, a wrong type raising ``TypeError``; ``bits``
        outside 1..1048576 or ``hashes`` outside 1..16 raises
        ``ValueError``.  An empty list still reads the snapshot and
        reports zero-query statistics.  A missing state file is the empty
        window; a recoverable uncommitted tail is ignored as elsewhere;
        any other corruption or a settings mismatch raises ``ValueError``
        and any other read failure raises ``OSError``.  The probe is a
        pure read: it creates nothing and changes no state, counter or
        receipt.
        """
        if not isinstance(keys, list):
            raise TypeError("keys must be a list of strings")
        if not all(isinstance(key, str) for key in keys):
            raise TypeError("keys must be a list of strings")
        if not isinstance(bits, int) or isinstance(bits, bool):
            raise TypeError("bits must be an integer")
        if not 1 <= bits <= _PROBE_MAX_BITS:
            raise ValueError(f"bits must be between 1 and {_PROBE_MAX_BITS}")
        if not isinstance(hashes, int) or isinstance(hashes, bool):
            raise TypeError("hashes must be an integer")
        if not 1 <= hashes <= _PROBE_MAX_HASHES:
            raise ValueError(f"hashes must be between 1 and {_PROBE_MAX_HASHES}")
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            # Nothing committed yet: probe the empty window at commit 0.
            entries, seq = [], 0
        else:
            parsed = _parse_tip_bytes(raw)
            if parsed.span != self._span or parsed.capacity != self._capacity:
                raise ValueError("state file settings do not match this window")
            entries, seq = parsed.entries, parsed.seq
        retained = {key for key, _ in entries}
        table = _bloom_build(retained, bits, hashes)
        results = []
        bloom_positive = 0
        matches = 0
        for key in keys:
            hit = False
            if _bloom_might_contain(table, key, bits, hashes):
                # A bloom positive is only a hint; the exact set decides.
                bloom_positive += 1
                hit = key in retained
            if hit:
                matches += 1
            results.append(hit)
        false_positives = bloom_positive - matches
        queries = len(keys)
        misses = queries - matches
        return {
            "results": results,
            "seq": seq,
            "queries": queries,
            "bloom_positive": bloom_positive,
            "matches": matches,
            "false_positives": false_positives,
            "hit_rate": matches / queries if queries else 0,
            "false_positive_rate": false_positives / misses if misses else 0,
        }

    def export(self, seq):
        """Export the complete state of one commit as a self-checking document.

        ``seq`` is the commit number assigned in arrival order, starting at
        1.  The returned dict carries the retained keys in order, the
        admitted/expired counts, the current time, the settings, the commit
        number and a checksum over all of it.  A JSON round trip leaves the
        document unchanged and :meth:`restore` turns it back into exactly
        that state.  The same commit exports identically before and after a
        compaction.

        Raises ``TypeError`` when ``seq`` is not an integer (booleans
        included, with no implicit conversion) and ``ValueError`` when it is
        not positive, has never been committed, or can no longer be located.
        """
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise TypeError("seq must be an integer")
        if seq <= 0:
            raise ValueError("seq must be a positive commit number")
        snapshot = self._locate_snapshot(seq)
        if snapshot is None:
            raise ValueError(f"commit {seq} cannot be located")
        return _make_export_document(snapshot)

    # -- persistence ------------------------------------------------------

    def save(self):
        """Durably persist the window state in the current format.

        A full snapshot goes to a same-directory temporary file, is fsynced
        and then renamed over the data file.  When another process has
        committed a newer state after this window's last load/commit, this
        save does not regress it: this window's changes already reached disk
        earlier.  When the state is already durable in the current format
        there is nothing to do.
        """
        self._ensure_dir()
        with self._locked(True):
            path = os.path.join(self._state_dir, _STATE_FILE)
            try:
                parsed = _parse_state_file(path)
            except (FileNotFoundError, ValueError):
                parsed = None  # missing or corrupt: overwrite, as before
            if parsed is not None and self._last_commit is not None:
                if parsed.tip != self._last_commit:
                    # A newer commit landed after this window's last
                    # load/commit; never regress the shared file.
                    return
                if parsed.version == _VERSION:
                    return  # already durable in the current format
            self._write_base_file()

    def load(self):
        """Re-read the state file and replace memory only once it checks out.

        Older document versions are upgraded in memory; the next commit
        rewrites them in the current format.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        with self._locked(False):
            parsed = _parse_state_file(path)  # FileNotFoundError if absent
            self._adopt(parsed)
            self._file_present = True
            self._file_stamp = _stamp_of(path)
        self._clock = self._now

    def _reload_if_present(self):
        """Adopt the latest committed state; a missing file starts from empty."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            stamp = _stamp_of(path)
        except FileNotFoundError:
            self._file_present = False
            return
        self._file_present = True
        if self._file_stamp == stamp and self._last_commit is not None:
            return  # no other process committed since our last load/commit
        parsed = _parse_state_file(path)
        self._adopt(parsed)
        self._file_stamp = stamp

    def _adopt(self, parsed):
        """Replace memory with a verified state; settings must match."""
        if parsed.span != self._span or parsed.capacity != self._capacity:
            raise ValueError("state file settings do not match this window")
        self._now = parsed.now
        self._admitted = parsed.admitted
        self._expired = parsed.expired
        self._entries = [list(pair) for pair in parsed.entries]
        self._index = {key: first for key, first in self._entries}
        self._receipts = _serialize_receipts(parsed.receipts)
        self._last_commit = parsed.tip
        self._seq = parsed.seq
        self._loaded_version = parsed.version
        self._anchor_bytes = parsed.anchor_bytes
        self._delta_bytes = parsed.delta_bytes
        self._delta_count = parsed.delta_count
        self._good_offset = parsed.good_offset

    def _sweep_temp_files(self):
        """Remove leftovers of writes interrupted by a process crash.

        The caller holds the exclusive lock, so any matching file must belong
        to a writer that is already gone.
        """
        try:
            names = os.listdir(self._state_dir)
        except FileNotFoundError:
            return
        for name in names:
            if name.startswith(_TMP_PREFIX) and name.endswith(_TMP_SUFFIX):
                try:
                    os.unlink(os.path.join(self._state_dir, name))
                except FileNotFoundError:
                    pass

    # -- commit machinery -------------------------------------------------

    def _needs_anchor(self):
        """Whether the next commit must append an anchor checkpoint.

        The first durable record of a v3 file is a base; an upgraded legacy
        file starts a fresh v3 chain.  Afterwards anchors bound replay once
        the deltas grow large or numerous.  History is never discarded, so
        old commits keep their number and stay locatable.
        """
        if not self._file_present or self._loaded_version != _VERSION:
            return True
        if self._delta_count >= _MAX_DELTAS:
            return True
        return self._delta_bytes >= max(_MIN_ANCHOR_BYTES, self._anchor_bytes)

    def _snapshot_body(self, kind, seq, prev):
        body = {
            "version": _VERSION,
            "kind": kind,
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "seq": seq,
            "keys": [[key, first] for key, first in self._entries],
        }
        # The receipt ring rides in every base/anchor snapshot; omitting an
        # empty ring keeps the no-receipt shape byte-identical to before.
        receipts = _serialize_receipts(self._receipts)
        if receipts:
            body["receipts"] = receipts
        if prev is not None:
            body["prev"] = prev
        return body

    def _envelope(self, body):
        envelope = dict(body)
        envelope["checksum"] = _checksum(body)
        return envelope

    def _write_whole_file(self, envelope):
        """Write a record document via temp + fsync + atomic os.replace."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        data = (
            json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        fd, tmp_name = tempfile.mkstemp(
            prefix=_TMP_PREFIX, suffix=_TMP_SUFFIX, dir=self._state_dir
        )
        tmp_left = True
        try:
            os.fchmod(fd, 0o644)
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
            tmp_left = False
            dir_fd = os.open(self._state_dir, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if tmp_left:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
        return data

    def _write_base_file(self):
        """Replace the whole file with one base snapshot atomically.

        Used for the empty initial ``save``, for a format upgrade and for
        ``restore``.  The base captures the current in-memory commit, so for
        a never-committed window its seq is 0; for an upgrade it is the
        legacy file's last commit number and for a restore it is the
        exported commit number.
        """
        body = self._snapshot_body("base", self._seq, None)
        envelope = self._envelope(body)
        data = self._write_whole_file(envelope)
        self._last_commit = envelope["checksum"]
        self._loaded_version = _VERSION
        self._file_present = True
        self._anchor_bytes = len(data)
        self._delta_bytes = 0
        self._delta_count = 0
        self._good_offset = len(data)
        self._file_stamp = _stamp_of(os.path.join(self._state_dir, _STATE_FILE))

    def _append_record(self, body):
        """Append one checksummed record line in place and fsync it."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        envelope = self._envelope(body)
        data = (
            json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        fd = os.open(path, os.O_WRONLY)
        try:
            # Drop a torn tail left by a crashed writer before extending.
            os.ftruncate(fd, self._good_offset)
            os.lseek(fd, self._good_offset, os.SEEK_SET)
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
        finally:
            os.close(fd)
        return envelope["checksum"], data

    def _commit(self, op, drop, add, batch_keys=(), batch_hits=(),
                event_items=(), event_kinds=(), event_wmark=None,
                delivery_id=None):
        """Commit the in-memory state as the next commit; caller holds lock."""
        self._sweep_temp_files()
        if self._needs_anchor():
            if not self._file_present or self._loaded_version != _VERSION:
                # First commit, or the next commit after loading a legacy
                # document: start a fresh v3 file at THIS commit so the base
                # snapshot captures the post-commit state.
                self._seq += 1
                self._write_base_file()
                return
            body = self._snapshot_body("anchor", self._seq + 1, self._last_commit)
            self._seq += 1
            checksum, data = self._append_record(body)
            self._last_commit = checksum
            self._anchor_bytes = len(data)
            self._delta_bytes = 0
            self._delta_count = 0
            self._good_offset += len(data)
            self._file_stamp = _stamp_of(os.path.join(self._state_dir, _STATE_FILE))
            return
        body = {
            "version": _VERSION,
            "kind": "delta",
            "op": op,
            "seq": self._seq + 1,
            "prev": self._last_commit,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
        }
        if op == "batch":
            # The full inputs and per-item results make the post-commit set
            # fully derivable by replay, including keys evicted and readmitted
            # within this same commit.
            body["keys"] = list(batch_keys)
            body["hits"] = list(batch_hits)
        elif op == "events":
            # Watermark before the batch plus the inputs (key and timestamp)
            # and the per-item kinds let replay recompute the post-commit
            # state, including expiries and intra-batch readmissions.
            body["wmark"] = event_wmark
            body["events"] = [[key, timestamp] for key, timestamp in event_items]
            body["kinds"] = list(event_kinds)
        elif op == "deliver":
            # Same replay payload as an events segment, plus this delivery's
            # receipt and the full post-commit ring.  The event changes and
            # the receipt share the single record, so they commit or vanish
            # together.
            body["wmark"] = event_wmark
            body["events"] = [[key, timestamp] for key, timestamp in event_items]
            body["kinds"] = list(event_kinds)
            body["receipt"] = [
                delivery_id,
                self._now,
                [[key, timestamp] for key, timestamp in event_items],
            ]
            body["receipts"] = _serialize_receipts(self._receipts)
        else:
            body["drop"] = list(drop)
            body["add"] = [list(pair) for pair in add]
        checksum, data = self._append_record(body)
        self._seq += 1
        self._last_commit = checksum
        self._delta_bytes += len(data)
        self._delta_count += 1
        self._good_offset += len(data)
        self._file_stamp = _stamp_of(os.path.join(self._state_dir, _STATE_FILE))

    # -- export location --------------------------------------------------

    def _locate_snapshot(self, seq):
        """Find the state of commit ``seq`` without taking the lock.

        The log is walked from the start; anchors restart replay at their
        commit, so the walk costs one replay between anchors at most.  A
        commit an anchor has jumped past without its records present (as
        happens after a :meth:`restore`, or in an externally trimmed file)
        cannot be located and returns ``None``.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ValueError(f"state file cannot be read: {exc}") from exc
        text = _decode(raw)
        try:
            document = json.loads(text)
        except json.JSONDecodeError:
            document = None
        if document is not None:
            if not isinstance(document, dict):
                raise ValueError("state file must contain a JSON object")
            version = _version_of(document)
            if version == 1:
                return None  # v1 predates commit numbering
            if version == 2:
                parsed = _parse_v2_single(document, len(raw))
                return self._snapshot_if(parsed, seq)
            if version == 3:
                info = _v3_anchor_info(document, allow_base=True)
                if info["kind"] != "base":
                    raise ValueError("state file has an invalid base snapshot")
                return self._snapshot_from_info(info, seq)
            raise ValueError("state file has an unsupported version")
        return self._locate_in_log(text, seq)

    @staticmethod
    def _snapshot_if(parsed, seq):
        if parsed.seq != seq:
            return None
        return {
            "seq": parsed.seq,
            "span": parsed.span,
            "capacity": parsed.capacity,
            "now": parsed.now,
            "admitted": parsed.admitted,
            "expired": parsed.expired,
            "entries": [list(pair) for pair in parsed.entries],
            "receipts": _serialize_receipts(parsed.receipts),
        }

    @staticmethod
    def _snapshot_from_info(info, seq):
        if info["seq"] != seq:
            return None
        return {
            "seq": info["seq"],
            "span": info["span"],
            "capacity": info["capacity"],
            "now": info["now"],
            "admitted": info["admitted"],
            "expired": info["expired"],
            "entries": [list(pair) for pair in info["entries"]],
            "receipts": _serialize_receipts(info["receipts"]),
        }

    def _locate_in_log(self, text, seq):
        lines = _split_lines(text)
        last = len(lines) - 1
        first_doc = json.loads(lines[0])
        if not isinstance(first_doc, dict):
            raise ValueError("state file must contain a JSON object")
        version = _version_of(first_doc)
        if version == 2:
            return self._locate_v2(lines, seq, last)
        if version != 3:
            raise ValueError("state file has an unsupported version")
        info = _v3_anchor_info(first_doc, allow_base=True)
        if info["kind"] != "base":
            raise ValueError("state file has an invalid base snapshot")
        state = _state_from_anchor(info)
        index = {key for key, _ in state["entries"]}
        max_first = state["entries"][-1][1] if state["entries"] else 0
        if state["seq"] == seq:
            return self._snapshot_of_state(state)
        if state["seq"] > seq:
            return None
        for i in range(1, len(lines)):
            try:
                document = json.loads(lines[i])
            except json.JSONDecodeError:
                if i == last:
                    break
                raise ValueError(f"state file is not valid JSON on line {i}")
            if not isinstance(document, dict):
                raise ValueError("state file must contain a JSON object")
            kind = document.get("kind")
            if kind == "anchor":
                info = _v3_anchor_info(document, allow_base=False)
                if info["prev"] != state["tip"] or info["seq"] != state["seq"] + 1:
                    raise ValueError("state file anchor chain is broken")
                state = _state_from_anchor(info)
                index = {key for key, _ in state["entries"]}
                max_first = state["entries"][-1][1] if state["entries"] else 0
            elif kind == "delta":
                try:
                    max_first = _apply_v3_delta(lines[i], state, index, max_first)
                except ValueError:
                    if i == last:
                        break
                    raise
            else:
                raise ValueError("state file holds a malformed record")
            if state["seq"] == seq:
                return self._snapshot_of_state(state)
            if state["seq"] > seq:
                return None
        return None

    def _locate_v2(self, lines, seq, last):
        _, base_seq, span, capacity, now, admitted, expired, entries = _parse_v2_base(
            json.loads(lines[0])
        )
        tip = json.loads(lines[0])["checksum"]
        index = {key for key, _ in entries}
        max_first = entries[-1][1] if entries else 0
        if base_seq == seq:
            return self._snapshot_of_state({
                "seq": base_seq, "span": span, "capacity": capacity, "now": now,
                "admitted": admitted, "expired": expired, "entries": entries,
            })
        for i in range(1, len(lines)):
            try:
                now, admitted, expired, base_seq, tip, max_first = _apply_v2_segment(
                    lines[i], span=span, capacity=capacity, now=now,
                    admitted=admitted, expired=expired, seq=base_seq, tip=tip,
                    entries=entries, index=index, max_first=max_first,
                )
            except ValueError:
                if i == last:
                    break
                raise
            if base_seq == seq:
                return self._snapshot_of_state({
                    "seq": base_seq, "span": span, "capacity": capacity, "now": now,
                    "admitted": admitted, "expired": expired, "entries": entries,
                })
        return None

    @staticmethod
    def _snapshot_of_state(state):
        return {
            "seq": state["seq"],
            "span": state["span"],
            "capacity": state["capacity"],
            "now": state["now"],
            "admitted": state["admitted"],
            "expired": state["expired"],
            "entries": [list(pair) for pair in state["entries"]],
            "receipts": _serialize_receipts(state.get("receipts", [])),
        }
