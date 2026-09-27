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

Every mutation commits as a numbered point, starting at 1.  Compaction and
``save`` rewrites only re-persist existing points: they neither create new
numbers nor change old ones, and a crash-interrupted write leaves no
addressable point behind.  :meth:`Window.export` writes the complete window
at one still-available committed point (the latest by default) as a
self-contained, versioned and checksummed JSON document;
:meth:`Window.restore` reads such a document back, replaces the key order,
current time and counts with it and lands the result as the next commit
point.  Points merged away by compaction are no longer addressable; asking
for one raises ``ValueError``.

Multiple processes may work the same state directory.  Mutual exclusion uses
an advisory ``flock`` on a file descriptor opened on the state directory
itself, so no lock file is ever left behind and the kernel releases the lock
if a process dies while holding it.  Mutating operations are linearizable:
each runs as lock -> reload -> mutate -> atomic commit, equivalent to some
serial execution in arrival order.  Readers take no lock at all and never
block a writer: committed bytes are never rewritten in place (appends only
extend the tail, compaction renames a complete temporary file over the data
file) and the parser ignores a torn uncommitted tail, so a read always sees
exactly one complete commit -- never a torn tail, never a compaction
intermediate, never a mix of old and new.  Opening a window does not create
its state directory; the directory appears on the first commit.  Local
deployment only; no networking.
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
_EXPORT_VERSION = 1
_EXPORT_KIND = "export"
_REQUIRED_FIELDS = (
    "version",
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
    )

    def __init__(self, **fields):
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
        seq=seq,
        tip=document["checksum"],
        base_bytes=size,
        segment_bytes=0,
        segment_count=0,
        good_offset=size,
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


def _parse_v2_log(text, stop_seq=None):
    """Parse a log-structured version 2 file: base line plus delta lines.

    Only the final line may be torn (an interrupted append); it is ignored
    and its offset recorded so the next writer can truncate it.  Any other
    inconsistency is corruption and raises ValueError.  With ``stop_seq``
    the replay halts at that commit point; a point the file no longer holds
    (newer than the tip, or merged away by compaction) raises ValueError.
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
    if stop_seq is None or seq != stop_seq:
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
            good_offset += line_len
            segment_bytes += line_len
            segment_count += 1
            if stop_seq is not None and seq == stop_seq:
                break
    if stop_seq is not None and seq != stop_seq:
        raise ValueError(f"state file holds no commit point {stop_seq}")
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
    )


def _parse_state_file(path, stop_seq=None):
    """Read, verify and replay the state file at ``path``.

    Raises FileNotFoundError when the file is absent and ValueError for
    every form of corruption: bad formatting, a missing or unknown version,
    a missing or mismatched checksum, a broken segment chain or state that
    violates the window invariants.  A torn final segment left by a crashed
    writer is not corruption; it is ignored and its offset reported.  With
    ``stop_seq`` the state is replayed only up to that commit point; a
    point the file does not hold raises ValueError.
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
            parsed = _parse_v1(document, len(raw))
        elif version == 2:
            parsed = _parse_v2_single(document, len(raw))
        else:
            raise ValueError("state file has an unsupported version")
        if stop_seq is not None and parsed.seq != stop_seq:
            raise ValueError(f"state file holds no commit point {stop_seq}")
        return parsed
    return _parse_v2_log(text, stop_seq)


def _parse_export_file(path):
    """Read, verify and validate an export document; return its snapshot.

    Raises FileNotFoundError when the file is absent and ValueError for a
    missing or unknown version, a missing or mismatched checksum, external
    tampering or any other malformation.
    """
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ValueError(f"export file cannot be read: {exc}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"export file is not valid UTF-8: {exc}") from exc
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"export file is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("export file must contain a JSON object")
    if "version" not in document:
        raise ValueError("export file is missing a version number")
    version = document["version"]
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError("export file has an invalid version")
    if version != _EXPORT_VERSION:
        raise ValueError("export file has an unsupported version")
    if document.get("kind") != _EXPORT_KIND:
        raise ValueError("export file is not an export document")
    body = _verified_body(document, "export file")
    return _validate_snapshot(body)


def read_settings(path):
    """Return ``(span, capacity)`` from a state file without opening a Window.

    Used by the command line to learn the construction settings before the
    window can be built.  Raises ValueError for a missing or corrupt file.
    """
    parsed = _parse_state_file(path)
    return parsed.span, parsed.capacity


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
        # Opening a window creates nothing; the state directory appears on
        # the first commit.

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
        os.makedirs(self._state_dir, exist_ok=True)
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
            self._commit_base(self._seq)  # re-persist, not a new point

    def load(self):
        """Re-read the state file and replace memory only once it checks out.

        The read takes no lock and never blocks a writer: committed bytes
        are never rewritten in place, so whatever is read -- with any torn
        uncommitted tail ignored -- is exactly one complete commit.  Older
        document versions are upgraded in memory; the next commit rewrites
        them in the current format.
        """
        path = os.path.join(self._state_dir, _STATE_FILE)
        # Stamp before reading: if a writer commits in between, the recorded
        # stamp is too old, which only costs a harmless extra reload later.
        stamp = _stamp_of(path)  # FileNotFoundError if absent
        parsed = _parse_state_file(path)
        self._adopt(parsed)
        self._file_present = True
        self._file_stamp = stamp
        self._clock = self._now

    def export(self, seq=None, path=None):
        """Write the complete window at one committed point to ``path``.

        ``seq`` selects the commit point; the latest committed point is used
        when it is omitted.  For convenience the arguments may also be given
        as ``export(path)`` or ``export(path, seq)``.  A ``seq`` that is not
        a positive integer raises TypeError; one the state no longer holds
        (beyond the latest point, or merged away by compaction) raises
        ValueError.  The document is self-contained -- version, snapshot and
        integrity checksum -- with fixed key order, compact whitespace and a
        trailing newline, so exporting the same point again always yields
        the same bytes.  A missing state file or a missing parent directory
        of ``path`` raises FileNotFoundError.  Returns the point's number.
        """
        if isinstance(seq, (str, os.PathLike)) and (
            path is None or isinstance(path, int)
        ):
            seq, path = path, seq
        if path is None:
            raise TypeError("export requires a destination path")
        if seq is not None and (
            not isinstance(seq, int) or isinstance(seq, bool) or seq <= 0
        ):
            raise TypeError("commit sequence number must be a positive integer")
        parsed = _parse_state_file(
            os.path.join(self._state_dir, _STATE_FILE), stop_seq=seq
        )
        body = {
            "version": _EXPORT_VERSION,
            "kind": _EXPORT_KIND,
            "span": parsed.span,
            "capacity": parsed.capacity,
            "now": parsed.now,
            "admitted": parsed.admitted,
            "expired": parsed.expired,
            "keys": [[key, first] for key, first in parsed.entries],
        }
        envelope = dict(body)
        envelope["checksum"] = _checksum(body)
        data = (
            json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        with open(os.fspath(path), "wb") as fh:
            fh.write(data)
        return parsed.seq

    def restore(self, path):
        """Replace the window with the point held in an export document.

        The document's key order, current time and both counts are adopted
        and committed as the next commit point; the window's span and
        capacity must match the document's.  A missing export file raises
        FileNotFoundError; a missing or unknown version, a checksum
        mismatch, tampering or any other malformation raises ValueError.
        Returns the sequence number of the new commit.
        """
        span, capacity, now, admitted, expired, entries = _parse_export_file(path)
        if span != self._span or capacity != self._capacity:
            raise ValueError("export file settings do not match this window")
        os.makedirs(self._state_dir, exist_ok=True)
        with self._locked(True):
            self._sweep_temp_files()
            self._reload_if_present()
            self._now = now
            self._admitted = admitted
            self._expired = expired
            self._entries = [list(pair) for pair in entries]
            self._index = {key: first for key, first in self._entries}
            self._commit_base(self._seq + 1)  # the restore is the next point
        self._clock = self._now
        return self._seq

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

        A mutation (``op`` given) is the next numbered commit point, whether
        it lands as an appended segment or as a rewritten base snapshot; a
        plain re-persist (``save``, compaction) keeps the current number.
        """
        self._sweep_temp_files()
        seq = self._seq + 1 if op is not None else self._seq
        if op is None or self._needs_base_write():
            self._commit_base(seq)
            return
        try:
            self._append_segment(seq, op, drop, add)
        except FileNotFoundError:
            # The data file vanished from under us; rewrite it wholesale.
            self._commit_base(seq)

    def _commit_base(self, seq):
        """Write a full snapshot via temp + fsync + atomic rename."""
        path = os.path.join(self._state_dir, _STATE_FILE)
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
        self._last_commit = checksum
        self._seq = seq
        self._loaded_version = _VERSION
        self._file_present = True
        self._base_bytes = len(data)
        self._segment_bytes = 0
        self._segment_count = 0
        self._good_offset = len(data)
        self._file_stamp = _stamp_of(path)

    def _append_segment(self, seq, op, drop, add):
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
            "seq": seq,
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
        self._seq = seq
        self._last_commit = checksum
        self._segment_bytes += len(data)
        self._segment_count += 1
        self._good_offset += len(data)
        self._file_stamp = _stamp_of(path)
