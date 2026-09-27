"""Bounded deduplication window.

A :class:`Window` remembers the first sighting time of each key currently
retained.  Two eviction rules apply:

* capacity: admitting a new key while the window is full evicts the oldest
  key first;
* span: :meth:`Window.advance` drops every key whose first sighting is more
  than ``span`` behind the current time.

Persistence is crash safe and versioned.  The state directory holds exactly
one data file, ``window.json``, in a log-structured format: a checksummed
base snapshot on the first line followed by small checksummed delta
segments, one per committed mutation, chained together by sequence number
and the previous segment's checksum.  A mutation commits by appending its
segment in place and fsyncing, so the bytes hitting disk stay proportional
to the change instead of mirroring the whole retained state every time.
Once the segments grow large relative to the base, the next commit
compacts: it merges base and segments into a fresh snapshot, writes it to a
same-directory temporary file, fsyncs it and atomically replaces the data
file via ``os.replace``.  A crash therefore either leaves the last
committed state fully recoverable -- an interrupted append only tears the
uncommitted tail, which readers ignore and the next writer truncates -- or
produces a deterministic error from ``load``; a half-restored state is
never returned.  A truncated file, a bad checksum or any other tampering
raises ``ValueError``; a missing data file raises ``FileNotFoundError``.

Documents carry a version number.  The current format is version 2;
version 1 documents (a single whole-state JSON object) are still read,
upgraded in memory and rewritten in the current format on the next commit.
A document without a version, with an unknown version, or left in a
half-migrated shape raises ``ValueError``.

Multiple processes may work the same state directory.  Mutual exclusion uses
an advisory ``flock`` on a file descriptor opened on the state directory
itself, so no lock file is ever left behind and the kernel releases the lock
if a process dies while holding it.  The lock only serializes writers
against each other: mutating operations run as lock -> reload -> mutate ->
atomic commit, equivalent to some serial execution in arrival order.
Readers never take the lock.  They open the data file directly and parse one
byte image, so they neither wait for a writer nor block one, and what they
get is always one whole committed state: an interrupted append only tears
the uncommitted tail, which is ignored, and a compaction atomically replaces
the file, so an open reader finishes the old image and a new reader opens the
new one -- nobody ever observes a half-merged state.  Local deployment only;
no networking.

Every durable mutation (an admitted key or an ``advance`` that moves time
or expires keys) is a commit and, in arrival order, carries a sequence
number starting at 1.  A base snapshot records the sequence of the state it
holds; delta segments chain from there.  :meth:`Window.export` locates the
state of any commit still present in the file -- the base snapshot's commit
and every segment after it -- and returns a self-verifying JSON document.
Once a compaction merges the log, commits older than the fresh base can no
longer be located and exporting them raises ``ValueError``; a commit still
present exports the identical document whether its state is read from the
base snapshot or replayed from segments.
:meth:`Window.restore` validates such a document and atomically puts the
whole window back at that commit, with subsequent commits numbering on from
it.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import tempfile

_STATE_FILE = "window.json"
_TMP_PREFIX = _STATE_FILE + "."
_TMP_SUFFIX = ".tmp"
_VERSION = 2
_EXPORT_DOCUMENT = "dedupe-window-export"
_EXPORT_VERSION = 1
_REQUIRED_FIELDS = (
    "version",
    "span",
    "capacity",
    "now",
    "admitted",
    "expired",
    "keys",
)
_EXPORT_FIELDS = (
    "format",
    "export_version",
    "seq",
    "span",
    "capacity",
    "now",
    "admitted",
    "expired",
    "keys",
)
# Compaction policy: rewrite the base snapshot once the appended segments
# are bigger than it (with a floor so tiny states are not rewritten every
# commit) or simply too numerous to replay cheaply.
_MAX_SEGMENTS = 512
_MIN_BASE_BYTES = 4096


def _is_number(value):
    """Numbers are ints or floats; booleans do not count."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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


def _validate_keys(raw_keys, now, span, capacity):
    """Validate the retained key list of a snapshot; return entries."""
    if not isinstance(raw_keys, list):
        raise ValueError("state file keys must be a list")
    if len(raw_keys) > capacity:
        raise ValueError("state file holds more keys than the capacity allows")
    entries = []
    seen_keys = set()
    previous = None
    for item in raw_keys:
        key, first = _check_entry(item, "state file")
        if key in seen_keys:
            raise ValueError("state file holds a duplicate key")
        if first < 0 or first > now:
            raise ValueError("state file holds a sighting outside the timeline")
        if now - first > span:
            raise ValueError("state file holds a key older than the span")
        if previous is not None and first < previous:
            raise ValueError("state file keys are not ordered by first sighting")
        seen_keys.add(key)
        previous = first
        entries.append([key, first])
    return entries


def _validate_snapshot(body):
    """Validate the fields every snapshot carries, whatever the version."""
    missing = set(_REQUIRED_FIELDS) - body.keys()
    if missing:
        raise ValueError(f"state file is missing fields: {sorted(missing)}")
    span = body["span"]
    capacity = body["capacity"]
    now = body["now"]
    admitted = body["admitted"]
    expired = body["expired"]
    if not _is_number(span) or not span > 0:
        raise ValueError("state file has an invalid span")
    if not _is_number(capacity) or not capacity > 0:
        raise ValueError("state file has an invalid capacity")
    if not _is_number(now) or now < 0:
        raise ValueError("state file has an invalid current time")
    if not _is_count(admitted):
        raise ValueError("state file has an invalid admitted count")
    if not _is_count(expired):
        raise ValueError("state file has an invalid expired count")
    entries = _validate_keys(body["keys"], now, span, capacity)
    return span, capacity, now, admitted, expired, entries


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
        "seq",
        "tip",
        "base_bytes",
        "segment_bytes",
        "segment_count",
        "good_offset",
        "captured",
    )

    def __init__(self, **fields):
        fields.setdefault("captured", None)
        for name in self.__slots__:
            setattr(self, name, fields[name])


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
        seq=0,
        tip=document["checksum"],
        base_bytes=size,
        segment_bytes=0,
        segment_count=0,
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


def _parse_v2_single(document, size, capture_seq=None):
    """Parse a version 2 file holding only its base snapshot."""
    _, seq, span, capacity, now, admitted, expired, entries = _parse_v2_base(document)
    captured = None
    if capture_seq is not None and seq == capture_seq:
        captured = (seq, span, capacity, now, admitted, expired,
                    [list(pair) for pair in entries])
    return _ParsedState(
        version=2,
        span=span,
        capacity=capacity,
        now=now,
        admitted=admitted,
        expired=expired,
        entries=entries,
        seq=seq,
        tip=document["checksum"],
        base_bytes=size,
        segment_bytes=0,
        segment_count=0,
        good_offset=size,
        captured=captured,
    )


def _apply_segment(line, *, span, capacity, now, admitted, expired, seq, tip,
                   entries, index, max_first):
    """Validate and replay one delta segment onto the running state.

    Returns the new ``(now, admitted, expired, seq, tip, max_first)``.  Every
    inconsistency raises ValueError; the caller decides whether the segment
    sits at the end of the file (a torn tail from a crashed writer, to be
    ignored) or in the middle (corruption, to be reported).
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
    # Evictions always take the oldest keys first, so the dropped keys are
    # exactly a prefix of the retained ones.
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


def _parse_v2_log(text, capture_seq=None):
    """Parse a log-structured version 2 file: base line plus delta lines.

    Only the final line may be torn (an interrupted append); it is ignored
    and its offset recorded so the next writer can truncate it.  Any other
    inconsistency is corruption and raises ValueError.

    When ``capture_seq`` names a commit present in the file, the returned
    state carries the full verified state at exactly that commit.
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
    _, base_seq, span, capacity, now, admitted, expired, entries = _parse_v2_base(base_doc)
    seq = base_seq
    tip = base_doc["checksum"]
    index = {key for key, _ in entries}
    max_first = entries[-1][1] if entries else 0
    base_bytes = len(lines[0].encode("utf-8")) + 1
    good_offset = base_bytes
    segment_bytes = 0
    segment_count = 0
    captured = None
    if capture_seq is not None and base_seq == capture_seq:
        captured = (base_seq, span, capacity, now, admitted, expired,
                    [list(pair) for pair in entries])
    last = len(lines) - 1
    for i in range(1, len(lines)):
        line_len = len(lines[i].encode("utf-8")) + 1
        try:
            now, admitted, expired, seq, tip, max_first = _apply_segment(
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
        if capture_seq is not None and seq == capture_seq:
            captured = (seq, span, capacity, now, admitted, expired,
                        [list(pair) for pair in entries])
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
        seq=seq,
        tip=tip,
        base_bytes=base_bytes,
        segment_bytes=segment_bytes,
        segment_count=segment_count,
        good_offset=good_offset,
        captured=captured,
    )


def _parse_state_file(path, capture_seq=None):
    """Read, verify and replay the state file at ``path``.

    Raises FileNotFoundError when the file is absent and ValueError for
    every form of corruption: bad formatting, a missing or unknown version,
    a missing or mismatched checksum, a broken segment chain or state that
    violates the window invariants.  A torn final segment left by a crashed
    writer is not corruption; it is ignored and its offset reported.

    With ``capture_seq`` set, the parsed result additionally carries the
    full state at that commit when the file still contains it.
    """
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ValueError(f"state file cannot be read: {exc}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"state file is not valid UTF-8: {exc}") from exc
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        document = None
    if document is not None:
        # A single JSON object: a version 1 document, or a version 2 file
        # that so far holds only its base snapshot.
        if not isinstance(document, dict):
            raise ValueError("state file must contain a JSON object")
        version = _version_of(document)
        if version == 1:
            return _parse_v1(document, len(raw))
        if version == 2:
            return _parse_v2_single(document, len(raw), capture_seq)
        raise ValueError("state file has an unsupported version")
    return _parse_v2_log(text, capture_seq)


def read_settings(path):
    """Return ``(span, capacity)`` from a state file without opening a Window.

    Used by the command line to learn the construction settings before the
    window can be built.  Raises ValueError for a missing or corrupt file.
    """
    parsed = _parse_state_file(path)
    return parsed.span, parsed.capacity


def _validate_export_document(document):
    """Validate an export document and return its checked state tuple.

    Raises TypeError when ``document`` is not a JSON object and ValueError
    for a document that is missing fields, carries unexpected fields, uses
    unsupported format markers, types the fields wrongly, fails its own
    checksum or describes a state that violates the window invariants.
    """
    if not isinstance(document, dict):
        raise TypeError("restore expects an export document object")
    expected = set(_EXPORT_FIELDS) | {"checksum"}
    if set(document) != expected:
        raise ValueError("export document has missing or unexpected fields")
    body = _verified_body(document, "export document")
    if body.get("format") != _EXPORT_DOCUMENT:
        raise ValueError("export document has an unrecognized format")
    export_version = body.get("export_version")
    if not isinstance(export_version, int) or isinstance(export_version, bool):
        raise ValueError("export document has an invalid version")
    if export_version != _EXPORT_VERSION:
        raise ValueError("export document has an unsupported version")
    seq = body.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise ValueError("export document has an invalid sequence number")
    # The state fields carry exactly the same invariants as a base snapshot;
    # the export document simply names its format markers differently.
    snapshot_body = dict(body)
    snapshot_body["version"] = _EXPORT_VERSION
    span, capacity, now, admitted, expired, entries = _validate_snapshot(
        snapshot_body
    )
    return seq, span, capacity, now, admitted, expired, entries


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
        self._last_commit = None    # checksum at the tip of the chain last seen
        self._seq = 0               # sequence number of the last segment
        self._loaded_version = _VERSION  # format of the file last read
        self._file_present = False
        self._file_stamp = None
        self._base_bytes = 0
        self._segment_bytes = 0
        self._segment_count = 0
        self._good_offset = 0  # end of the committed prefix of the data file
        # Constructing a window has no filesystem effect: read-only calls
        # never create the state directory or the data file; a writer
        # creates the directory on its first commit.

    @contextlib.contextmanager
    def _locked(self, exclusive):
        """Hold a flock on the state directory; the directory inode is stable.

        Closing the fd releases the lock, so a process killed (or crashed)
        while holding it never leaves a deadlock behind.
        """
        fd = os.open(self._state_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            os.close(fd)

    def observe(self, key):
        """Record a sighting; return True only when the key is newly admitted.

        Runs as one locked, durable transaction so concurrent processes are
        serialized as if their calls ran one after another.
        """
        _check_key(key)
        os.makedirs(self._state_dir, exist_ok=True)
        with self._locked(True):
            self._reload_if_present()
            admitted, evicted = self._admit(key)
            if admitted:
                self._commit_state("admit", drop=evicted, add=[[key, self._now]])
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

    def seen(self, key):
        """Report membership without recording anything."""
        _check_key(key)
        return key in self._index

    def advance(self, now):
        """Move the current time to ``now`` and drop keys older than the span.

        Like :meth:`observe`, one locked and durable transaction.
        """
        if not _is_number(now):
            raise TypeError("now must be an int or float")
        # Monotonicity is per process: this call must not go back past a
        # time this same process already loaded or requested.  Check it
        # before touching the filesystem so a rejected call creates nothing.
        if now < self._clock:
            raise ValueError("now is earlier than the current time")
        os.makedirs(self._state_dir, exist_ok=True)
        with self._locked(True):
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
                self._commit_state("sweep", drop=dropped, add=[])
            return len(dropped)

    def keys(self):
        """Retained keys, oldest sighting first."""
        return [key for key, _ in self._entries]

    def stats(self):
        """Span, capacity, retained, admitted and expired counts."""
        return {
            "span": self._span,
            "capacity": self._capacity,
            "retained": len(self._entries),
            "admitted": self._admitted,
            "expired": self._expired,
        }

    def export(self, seq):
        """Return a self-verifying JSON document of commit ``seq``.

        ``seq`` is the 1-based arrival-order commit number: 1 is the first
        mutation ever committed and each later ``observe``/``advance`` commit
        counts one more.  The document captures the complete state at exactly
        that commit -- retained keys in first-sighting order, the admitted and
        expired counters, the current time and the span/capacity settings --
        plus the sequence number and a checksum over all of it, so a JSON
        round trip leaves the contents unchanged and :meth:`restore` can
        detect any alteration.

        Like the other read paths this takes no lock and sees one whole
        committed state even while a writer is appending or compacting.  A
        non-integer (including ``bool``) raises TypeError; a non-positive
        number, a commit that was never made, or one merged away by a
        compaction and therefore no longer locatable raises ValueError.
        """
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise TypeError("sequence number must be an integer")
        if seq < 1:
            raise ValueError("sequence number must be positive")
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            parsed = _parse_state_file(path, capture_seq=seq)
        except FileNotFoundError:
            raise ValueError(f"commit {seq} cannot be located") from None
        captured = parsed.captured
        if captured is None:
            raise ValueError(f"commit {seq} cannot be located")
        cseq, span, capacity, now, admitted, expired, entries = captured
        body = {
            "format": _EXPORT_DOCUMENT,
            "export_version": _EXPORT_VERSION,
            "seq": cseq,
            "span": span,
            "capacity": capacity,
            "now": now,
            "admitted": admitted,
            "expired": expired,
            "keys": [[key, first] for key, first in entries],
        }
        document = dict(body)
        document["checksum"] = _checksum(body)
        return document

    def restore(self, document):
        """Put the whole window back at the commit an export captured.

        ``document`` must be an object returned by :meth:`export` (possibly
        after a JSON round trip).  Key order, counters, current time and the
        settings are brought back item for item, the replacement lands as one
        atomic durable commit, and later commits number on from the restored
        sequence.  Anything that is not an export document object raises
        TypeError; a document with missing or mistyped fields or a failing
        checksum raises ValueError, as does one whose settings do not match
        this window's construction.  On failure neither the file nor this
        window's memory is touched.
        """
        seq, span, capacity, now, admitted, expired, entries = (
            _validate_export_document(document)
        )
        if span != self._span or capacity != self._capacity:
            raise ValueError("export document settings do not match this window")
        entries = [list(pair) for pair in entries]
        os.makedirs(self._state_dir, exist_ok=True)
        with self._locked(True):
            self._sweep_temp_files()
            body = {
                "version": _VERSION,
                "kind": "base",
                "span": span,
                "capacity": capacity,
                "now": now,
                "admitted": admitted,
                "expired": expired,
                "seq": seq,
                "keys": [[key, first] for key, first in entries],
            }
            checksum, length, stamp = self._write_base_file(body)
        self._span = span
        self._capacity = capacity
        self._now = now
        self._clock = now
        self._admitted = admitted
        self._expired = expired
        self._entries = entries
        self._index = {key: first for key, first in entries}
        self._seq = seq
        self._last_commit = checksum
        self._loaded_version = _VERSION
        self._file_present = True
        self._file_stamp = stamp
        self._base_bytes = length
        self._segment_bytes = 0
        self._segment_count = 0
        self._good_offset = length

    def save(self):
        """Durably persist the window state in the current format.

        A full snapshot goes to a same-directory temporary file, is fsynced
        and then renamed over the data file.  When another process has
        committed a newer state after this window's last load/commit, this
        save does not regress it: this window's changes already reached disk
        earlier.  When the state is already durable in the current format
        there is nothing to do.
        """
        os.makedirs(self._state_dir, exist_ok=True)
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
            self._commit_base()

    def load(self):
        """Re-read the state file and replace memory only once it checks out.

        Older document versions are upgraded in memory; the next commit
        rewrites them in the current format.

        This read is lock-free on purpose: it opens the data file directly
        rather than queuing for the writer lock, so a reader neither waits on
        nor blocks an in-flight commit or compaction.  One ``open``/``read``
        yields a single byte image -- a compaction that atomically replaces
        the file lands entirely before or after that open, and a torn
        append tail is ignored -- so the adopted state is always one whole
        committed state, never pieces of two.  The read is bracketed by file
        stamps: when a commit lands while the bytes are being read (the
        lock-free equivalent of losing the race for the old shared lock),
        the file is re-read until one byte image and its stamp agree, so the
        recorded stamp can never silently describe a newer commit than the
        state in memory.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        stamp = _stamp_of(path)  # FileNotFoundError if absent
        while True:
            parsed = _parse_state_file(path)
            after = _stamp_of(path)
            if after == stamp:
                break
            stamp = after
        self._adopt(parsed)
        self._file_present = True
        self._file_stamp = stamp
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
        self._last_commit = parsed.tip
        self._seq = parsed.seq
        self._loaded_version = parsed.version
        self._base_bytes = parsed.base_bytes
        self._segment_bytes = parsed.segment_bytes
        self._segment_count = parsed.segment_count
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

    def _needs_base_write(self):
        """True when a commit must rewrite the whole snapshot instead of
        appending one segment: no usable current-format file, or the
        segments grew enough that it is time to compact them away."""
        if not self._file_present or self._loaded_version != _VERSION:
            return True
        if self._segment_count >= _MAX_SEGMENTS:
            return True
        return self._segment_bytes >= max(_MIN_BASE_BYTES, self._base_bytes)

    def _commit_state(self, op=None, drop=(), add=()):
        """Commit the in-memory state; caller holds the write lock.

        Every mutation commit takes the next sequence number, whether it
        lands as an appended delta segment or as a fresh base snapshot.  A
        base write triggered by the compaction threshold is not an extra
        commit: it is simply the same numbered mutation stored as a
        snapshot, and the commits merged out of the file stop being
        locatable.  A crash before the atomic rename leaves the previous
        tip (and every commit up to it) intact and locatable; the new
        commit simply has not happened yet.
        """
        os.makedirs(self._state_dir, exist_ok=True)
        self._sweep_temp_files()
        new_seq = self._seq + 1
        if op is None or self._needs_base_write():
            self._commit_base(new_seq)
            return
        try:
            self._append_segment(op, drop, add, new_seq)
        except FileNotFoundError:
            # The data file vanished from under us; rewrite it wholesale.
            self._commit_base(new_seq)

    def _write_base_file(self, body):
        """Durably write one base snapshot via temp + fsync + atomic rename.

        Returns ``(checksum, data_length, file_stamp)``; the caller updates
        memory once the replacement is on disk.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        checksum = _checksum(body)
        envelope = dict(body)
        envelope["checksum"] = checksum
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
        return checksum, len(data), _stamp_of(path)

    def _commit_base(self, seq=None):
        """Write a full snapshot via temp + fsync + atomic rename."""
        if seq is None:
            seq = self._seq
        body = {
            "version": _VERSION,
            "kind": "base",
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "seq": seq,
            "keys": [[key, first] for key, first in self._entries],
        }
        checksum, length, stamp = self._write_base_file(body)
        self._seq = seq
        self._last_commit = checksum
        self._loaded_version = _VERSION
        self._file_present = True
        self._base_bytes = length
        self._segment_bytes = 0
        self._segment_count = 0
        self._good_offset = length
        self._file_stamp = stamp

    def _append_segment(self, op, drop, add, new_seq):
        """Commit one mutation by appending a small segment in place.

        Only the tail of the data file is written, so the bytes hitting disk
        stay proportional to the change, not to the retained state.  A crash
        can tear the appended line; readers ignore the uncommitted tail and
        the next writer truncates it before extending the file.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        body = {
            "kind": "delta",
            "op": op,
            "seq": new_seq,
            "prev": self._last_commit,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "drop": list(drop),
            "add": [list(pair) for pair in add],
        }
        checksum = _checksum(body)
        envelope = dict(body)
        envelope["checksum"] = checksum
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
        self._seq = new_seq
        self._last_commit = checksum
        self._segment_bytes += len(data)
        self._segment_count += 1
        self._good_offset += len(data)
        self._file_stamp = _stamp_of(path)
