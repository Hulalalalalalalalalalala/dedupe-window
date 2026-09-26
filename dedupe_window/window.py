"""Bounded deduplication window.

A :class:`Window` remembers the first sighting time of each key currently
retained.  Two eviction rules apply:

* capacity: admitting a new key while the window is full evicts the oldest
  key first;
* span: :meth:`Window.advance` drops every key whose first sighting is more
  than ``span`` behind the current time.

Persistence is crash safe and versioned.  The state directory holds one
compact base snapshot (``window.json``) plus, between compactions, a short
run of small delta segments (``window.json.seg.<n>``).  A mutating
transaction on a large window writes only a delta segment, so the bytes
hitting disk stay proportional to the change instead of mirroring the whole
key set every time; once segments accumulate, the next commit merges them
back into a fresh base snapshot (compaction) and deletes them, so the
directory converges to a single data file again.  ``save`` always compacts.
Every write goes to a temporary file in the state directory, is fsynced and
atomically renamed over its target, so a crash or power loss mid-write
leaves either the last committed state or a deterministic error behind,
never half a state.  A SHA-256 checksum over a canonical serialization of
every document guards integrity: a truncated file or external tampering can
never be mistaken for a committed state.

Documents carry a format version.  Version 1 documents (flat snapshots) are
still read: loading one upgrades it in memory and the next commit writes
the current format.  A missing or unknown version, a half-migrated
document, a segment that does not belong to its base snapshot or a gap in
the segment chain all make ``load`` raise ``ValueError``; a missing data
file raises ``FileNotFoundError``.

Multiple processes may work the same state directory.  Mutual exclusion uses
an advisory ``flock`` on a file descriptor opened on the state directory
itself, so no lock file is ever left behind and the kernel releases the lock
if a process dies while holding it.  Mutating operations are linearizable:
each runs as lock -> reload -> mutate -> atomic commit, equivalent to some
serial execution in arrival order.  Local deployment only; no networking.
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
_SEG_PREFIX = _STATE_FILE + ".seg."
_TMP_PREFIX = _STATE_FILE + "."
_TMP_SUFFIX = ".tmp"
_VERSION = 2
_READABLE_VERSIONS = (1, _VERSION)
_BASE_KIND = "base"
_SEGMENT_KIND = "segment"
_REQUIRED_FIELDS = (
    "version",
    "span",
    "capacity",
    "now",
    "admitted",
    "expired",
    "keys",
)

# Compaction policy.  A small state is always committed as one compact base
# snapshot: a full mirror is as cheap as a delta and keeps a single file on
# disk.  A large state accumulates delta segments until there are too many
# or their total outweighs a slice of the base, and the next commit then
# merges everything back into a fresh base.
_SMALL_STATE_BYTES = 1 << 18      # estimated base size below which commits compact
_COMPACT_SEGMENT_LIMIT = 64       # max live segments before a merge
_COMPACT_SEGMENT_BYTES = 1 << 16  # pending segment bytes that force a merge ...
_COMPACT_SEGMENT_RATIO = 8        # ... or an eighth of the base snapshot size


def _is_number(value):
    """Numbers are ints or floats; booleans do not count."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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


def _decode(raw):
    """Decode a state document, distinguishing format errors from tampering."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"state file is not valid UTF-8: {exc}") from exc
    try:
        envelope = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"state file is not valid JSON: {exc}") from exc
    if not isinstance(envelope, dict):
        raise ValueError("state file must contain a JSON object")
    digest = envelope.get("checksum")
    if "checksum" not in envelope:
        raise ValueError("state file is missing integrity information")
    if not isinstance(digest, str) or not digest:
        raise ValueError("state file has a malformed integrity checksum")
    body = {key: value for key, value in envelope.items() if key != "checksum"}
    expected = _checksum(body).encode("ascii")
    if not hmac.compare_digest(digest.encode("utf-8", "ignore"), expected):
        raise ValueError("state file integrity checksum does not match")
    return envelope


def _check_version(envelope):
    """Validate the format version of a decoded document."""
    if "version" not in envelope:
        raise ValueError("state file is missing fields: ['version']")
    version = envelope["version"]
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError("state file has an invalid version")
    if version not in _READABLE_VERSIONS:
        raise ValueError("state file has an unsupported version")
    return version


def _validate_entries(raw_keys, now, span, capacity):
    """Validate a key list and return it as fresh [key, first] pairs."""
    if not isinstance(raw_keys, list):
        raise ValueError("state file keys must be a list")
    if len(raw_keys) > capacity:
        raise ValueError("state file holds more keys than the capacity allows")
    entries = []
    seen_keys = set()
    previous = None
    for item in raw_keys:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not _is_number(item[1])
        ):
            raise ValueError("state file holds a malformed key entry")
        key, first = item
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


def _read_document(path):
    """Read and verify a state document at ``path``.

    Raises FileNotFoundError when the file is absent and ValueError for every
    form of corruption, with a reason precise enough to tell apart bad
    formatting, a missing checksum and a checksum mismatch.
    """
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ValueError(f"state file cannot be read: {exc}") from exc
    return _decode(raw)


def _signed_document(body):
    """Serialize a document with its checksum; return (bytes, checksum)."""
    checksum = _checksum(body)
    envelope = dict(body)
    envelope["checksum"] = checksum
    text = json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
    return text.encode("utf-8"), checksum


def read_settings(path):
    """Return ``(span, capacity)`` from a state file without opening a Window.

    Used by the command line to learn the construction settings before the
    window can be built.  Raises ValueError for a missing or corrupt file.
    """
    envelope = _read_document(path)
    _check_version(envelope)
    if "span" not in envelope or "capacity" not in envelope:
        raise ValueError("state file is missing span or capacity settings")
    span = envelope["span"]
    capacity = envelope["capacity"]
    if not _is_number(span) or not span > 0:
        raise ValueError("state file has an invalid span")
    if not _is_number(capacity) or not capacity > 0:
        raise ValueError("state file has an invalid capacity")
    return span, capacity


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
        self._keys_bytes = 128  # running estimate of the serialized key payload
        # Bookkeeping for the on-disk layout this window's memory reflects.
        self._seq = 0            # sequence number of the tip state in memory
        self._base_seq = 0       # sequence number merged into the on-disk base
        self._base_checksum = None
        self._base_bytes = 0
        self._segment_seqs = []  # live on-disk segments on top of the base
        self._segment_bytes = 0
        self._base_missing = True
        self._needs_upgrade = False
        self._last_commit = None  # disk identity last loaded/committed

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
        with self._locked(True):
            self._reload_if_present()
            admitted = self._admit(key)
            if admitted:
                self._commit_state(admit=[[key, self._now]])
            return admitted

    def _admit(self, key):
        if key in self._index:
            return False
        while len(self._entries) >= self._capacity:
            oldest, _ = self._entries.pop(0)
            del self._index[oldest]
            self._keys_bytes -= len(oldest) + 24
        self._entries.append([key, self._now])
        self._index[key] = self._now
        self._keys_bytes += len(key) + 24
        self._admitted += 1
        return True

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
            dropped = 0
            while self._entries and now - self._entries[0][1] > self._span:
                key, _ = self._entries.pop(0)
                del self._index[key]
                self._keys_bytes -= len(key) + 24
                dropped += 1
            self._expired += dropped
            if moved or dropped:
                self._commit_state()
            return dropped

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
        """Atomically write the whole window state as one compact snapshot.

        Any pending delta segments are merged into the base document
        (compaction), so after ``save`` the state directory holds exactly one
        data file.  The document goes to a same-directory temporary file, is
        fsynced and then renamed over the data file.  When another process
        has committed a newer state after this window's last load/commit,
        this save does not regress it: this window's changes already reached
        disk earlier.
        """
        os.makedirs(self._state_dir, exist_ok=True)
        with self._locked(True):
            self._sweep_temp_files()
            path = os.path.join(self._state_dir, _STATE_FILE)
            try:
                _read_document(path)
            except FileNotFoundError:
                pass  # first save: create the base
            except ValueError:
                pass  # corrupt or unreadable: overwrite, as before
            else:
                if (
                    self._last_commit is not None
                    and self._disk_identity() != self._last_commit
                ):
                    # A newer commit landed after this window's last
                    # load/commit; never regress the shared file with older
                    # in-memory state.
                    return
            self._commit_base()

    def load(self):
        """Re-read the state file and replace memory only once it checks out."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        with self._locked(False):
            identity = self._disk_identity()
            bundle = self._read_state(path)  # FileNotFoundError if absent
            self._adopt(bundle, identity)
        self._clock = self._now

    # ------------------------------------------------------------------
    # Reading: base snapshot plus the delta segments committed on top of it
    # ------------------------------------------------------------------

    def _reload_if_present(self):
        """Adopt the latest committed state; a missing file starts from empty."""
        identity = self._disk_identity()
        if self._last_commit is not None and identity == self._last_commit:
            return  # nothing changed on disk since our last load/commit
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            bundle = self._read_state(path)
        except FileNotFoundError:
            self._base_missing = True
            self._last_commit = identity
            return
        self._adopt(bundle, identity)

    def _read_state(self, path):
        """Read base + live segments and replay them; does not touch memory."""
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            raise
        except OSError as exc:
            raise ValueError(f"state file cannot be read: {exc}") from exc
        envelope = _decode(raw)
        base = self._parse_base(envelope)
        segments, segment_bytes = self._read_segments(
            envelope["checksum"], base[1]
        )
        if segments and base[0] != _VERSION:
            raise ValueError("state file is half migrated to a new version")
        state = self._replay(base, segments)
        return (envelope, base, len(raw), segments, segment_bytes, state)

    def _parse_base(self, envelope):
        """Validate a base document of any readable version.

        Returns ``(version, seq, now, admitted, expired, entries)``.  A
        version 1 document is a flat snapshot and gets sequence number 0.
        """
        version = _check_version(envelope)
        missing = set(_REQUIRED_FIELDS) - envelope.keys()
        if missing:
            raise ValueError(f"state file is missing fields: {sorted(missing)}")
        if version == _VERSION:
            if envelope.get("kind") != _BASE_KIND:
                raise ValueError("state file is not a base snapshot")
            seq = envelope.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
                raise ValueError("state file has an invalid sequence number")
        else:
            seq = 0
        span = envelope["span"]
        capacity = envelope["capacity"]
        now = envelope["now"]
        admitted = envelope["admitted"]
        expired = envelope["expired"]
        if not _is_number(span) or not span > 0:
            raise ValueError("state file has an invalid span")
        if not _is_number(capacity) or not capacity > 0:
            raise ValueError("state file has an invalid capacity")
        if span != self._span or capacity != self._capacity:
            raise ValueError("state file settings do not match this window")
        if not _is_number(now) or now < 0:
            raise ValueError("state file has an invalid current time")
        if not isinstance(admitted, int) or isinstance(admitted, bool) or admitted < 0:
            raise ValueError("state file has an invalid admitted count")
        if not isinstance(expired, int) or isinstance(expired, bool) or expired < 0:
            raise ValueError("state file has an invalid expired count")
        entries = _validate_entries(envelope["keys"], now, span, capacity)
        return version, seq, now, admitted, expired, entries

    def _parse_segment(self, envelope):
        """Validate a delta segment document; return its fields as a tuple."""
        version = _check_version(envelope)
        if version != _VERSION:
            raise ValueError("state file segment has an unsupported version")
        if envelope.get("kind") != _SEGMENT_KIND:
            raise ValueError("state file segment is malformed")
        missing = {"seq", "base", "now", "admitted", "expired", "admit"} - envelope.keys()
        if missing:
            raise ValueError(f"state file segment is missing fields: {sorted(missing)}")
        seq = envelope["seq"]
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
            raise ValueError("state file segment has an invalid sequence number")
        base = envelope["base"]
        if not isinstance(base, str) or not base:
            raise ValueError("state file segment has an invalid base reference")
        now = envelope["now"]
        if not _is_number(now) or now < 0:
            raise ValueError("state file segment has an invalid current time")
        admitted = envelope["admitted"]
        expired = envelope["expired"]
        if not isinstance(admitted, int) or isinstance(admitted, bool) or admitted < 0:
            raise ValueError("state file segment has an invalid admitted count")
        if not isinstance(expired, int) or isinstance(expired, bool) or expired < 0:
            raise ValueError("state file segment has an invalid expired count")
        admit = envelope["admit"]
        if not isinstance(admit, list):
            raise ValueError("state file segment admissions must be a list")
        pairs = []
        for item in admit:
            if (
                not isinstance(item, list)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not _is_number(item[1])
            ):
                raise ValueError("state file segment holds a malformed key entry")
            key, first = item
            if first < 0 or first > now:
                raise ValueError(
                    "state file segment holds a sighting outside the timeline"
                )
            pairs.append([key, first])
        return seq, base, now, admitted, expired, pairs

    def _read_segments(self, base_checksum, base_seq):
        """Read the live segments on top of the base, in sequence order.

        Segments at or below the base sequence number are stale leftovers of
        an interrupted compaction and are ignored.  Anything else that does
        not check out -- a corrupt, foreign or missing segment -- is a
        deterministic ValueError.
        """
        pending = {}
        total_bytes = 0
        for name in self._segment_files():
            seq = int(name[len(_SEG_PREFIX):])
            if seq <= base_seq:
                continue  # stale leftover of an interrupted compaction
            path = os.path.join(self._state_dir, name)
            try:
                with open(path, "rb") as fh:
                    raw = fh.read()
            except FileNotFoundError as exc:
                raise ValueError(
                    f"state file segment vanished while reading: {name}"
                ) from exc
            except OSError as exc:
                raise ValueError(f"state file segment cannot be read: {exc}") from exc
            envelope = _decode(raw)
            seg_seq, seg_base, now, admitted, expired, admit = self._parse_segment(
                envelope
            )
            if seg_seq != seq:
                raise ValueError(
                    "state file segment sequence does not match its name"
                )
            if seg_base != base_checksum:
                raise ValueError(
                    "state file segment does not belong to the base snapshot"
                )
            pending[seq] = (now, admitted, expired, admit)
            total_bytes += len(raw)
        ordered = []
        for seq in sorted(pending):
            if seq != base_seq + len(ordered) + 1:
                raise ValueError("state file is missing a segment")
            ordered.append(pending[seq])
        return ordered, total_bytes

    def _replay(self, base, segments):
        """Fold segments into the base, checking the merged state throughout.

        Evictions are re-derived from the window rules, so a segment only
        records the new time, the admissions and the cumulative counts; the
        counts are then verified against the replayed evictions exactly.
        """
        _, _, now, admitted, expired, entries = base
        index = {key: first for key, first in entries}
        for seg_now, seg_admitted, seg_expired, admit in segments:
            if seg_now < now:
                raise ValueError("state file segments move time backwards")
            now = seg_now
            drops = 0
            while entries and now - entries[0][1] > self._span:
                key, _ = entries.pop(0)
                del index[key]
                drops += 1
            for key, first in admit:
                if key in index:
                    raise ValueError("state file holds a duplicate key")
                if entries and first < entries[-1][1]:
                    raise ValueError(
                        "state file keys are not ordered by first sighting"
                    )
                while len(entries) >= self._capacity:
                    oldest, _ = entries.pop(0)
                    del index[oldest]
                entries.append([key, first])
                index[key] = first
            if seg_admitted != admitted + len(admit):
                raise ValueError("state file segment counts do not add up")
            if seg_expired != expired + drops:
                raise ValueError("state file segment counts do not add up")
            admitted, expired = seg_admitted, seg_expired
        # The merged result must satisfy the same invariants as a base snapshot.
        _validate_entries(entries, now, self._span, self._capacity)
        return now, admitted, expired, entries

    def _adopt(self, bundle, identity):
        """Replace memory with a state that has been fully read and verified."""
        envelope, base, base_bytes, segments, segment_bytes, state = bundle
        version, base_seq = base[0], base[1]
        self._now, self._admitted, self._expired, self._entries = state
        self._index = {key: first for key, first in self._entries}
        self._keys_bytes = 128 + sum(len(key) + 24 for key, _ in self._entries)
        self._seq = base_seq + len(segments)
        self._base_seq = base_seq
        self._base_checksum = envelope["checksum"]
        self._base_bytes = base_bytes
        self._segment_seqs = [base_seq + i + 1 for i in range(len(segments))]
        self._segment_bytes = segment_bytes
        self._needs_upgrade = version != _VERSION
        self._base_missing = False
        self._last_commit = identity

    def _disk_identity(self):
        """Fingerprint of the committed files; changes with every commit.

        Used both to skip redundant reloads and to notice when another
        process committed after this window's last load/commit.  Temporary
        files are ignored: they are never part of a committed state.
        """
        try:
            names = os.listdir(self._state_dir)
        except FileNotFoundError:
            return None
        identity = []
        for name in sorted(names):
            if name != _STATE_FILE and not name.startswith(_TMP_PREFIX):
                continue
            if name.endswith(_TMP_SUFFIX):
                continue
            try:
                st = os.stat(os.path.join(self._state_dir, name))
            except FileNotFoundError:
                continue
            identity.append((name, st.st_ino, st.st_size, st.st_mtime_ns))
        return tuple(identity)

    def _segment_files(self):
        """Names of committed segment files in the state directory."""
        try:
            names = os.listdir(self._state_dir)
        except FileNotFoundError:
            return []
        return [
            name
            for name in names
            if name.startswith(_SEG_PREFIX)
            and not name.endswith(_TMP_SUFFIX)
            and name[len(_SEG_PREFIX):].isdigit()
        ]

    # ------------------------------------------------------------------
    # Writing: delta segments, compaction, atomic renames
    # ------------------------------------------------------------------

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

    def _sweep_stale_segments(self):
        """Remove segments already merged into the on-disk base."""
        for name in self._segment_files():
            if int(name[len(_SEG_PREFIX):]) <= self._base_seq:
                try:
                    os.unlink(os.path.join(self._state_dir, name))
                except FileNotFoundError:
                    pass

    def _commit_state(self, admit=()):
        """Persist the in-memory tip; caller holds the exclusive lock.

        Small states and upgrades are written as one compact base snapshot;
        large states write only a delta segment and compact once the segments
        accumulate, keeping each commit's written bytes proportional to the
        change rather than to the whole key set.
        """
        self._sweep_temp_files()
        if self._base_missing or self._needs_upgrade or self._small_state():
            # A direct base snapshot is itself the commit for this
            # transaction, so the sequence number advances now.
            self._seq += 1
            self._commit_base()
            return
        self._sweep_stale_segments()
        self._commit_segment(admit)
        if (
            len(self._segment_seqs) >= _COMPACT_SEGMENT_LIMIT
            or self._segment_bytes
            >= max(_COMPACT_SEGMENT_BYTES, self._base_bytes // _COMPACT_SEGMENT_RATIO)
        ):
            # The segment above already claimed this transaction's sequence
            # number; merging it into the base must not advance it again.
            self._commit_base()

    def _small_state(self):
        return self._keys_bytes < _SMALL_STATE_BYTES

    def _commit_base(self):
        """Merge everything into one compact base snapshot; atomic rename."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        body = {
            "version": _VERSION,
            "kind": _BASE_KIND,
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "seq": self._seq,
            "keys": [[key, first] for key, first in self._entries],
        }
        data, checksum = _signed_document(body)
        self._write_atomically(path, data, _TMP_PREFIX)
        # The new base subsumes every segment; remove them all.  A crash
        # before this finishes leaves stale segments behind, which readers
        # ignore and the next commit sweeps.
        for name in self._segment_files():
            try:
                os.unlink(os.path.join(self._state_dir, name))
            except FileNotFoundError:
                pass
        self._fsync_dir()
        self._base_seq = self._seq
        self._base_checksum = checksum
        self._base_bytes = len(data)
        self._segment_seqs = []
        self._segment_bytes = 0
        self._needs_upgrade = False
        self._base_missing = False
        self._last_commit = self._disk_identity()

    def _commit_segment(self, admit):
        """Write only the delta since the base as a small segment file."""
        seq = self._seq + 1
        body = {
            "version": _VERSION,
            "kind": _SEGMENT_KIND,
            "seq": seq,
            "base": self._base_checksum,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "admit": [[key, first] for key, first in admit],
        }
        data, _ = _signed_document(body)
        name = _SEG_PREFIX + str(seq)
        self._write_atomically(
            os.path.join(self._state_dir, name), data, _SEG_PREFIX
        )
        self._seq = seq
        self._segment_seqs.append(seq)
        self._segment_bytes += len(data)
        self._last_commit = self._disk_identity()

    def _write_atomically(self, path, data, tmp_prefix):
        """Temp file in the same directory + fsync + atomic rename."""
        fd, tmp_name = tempfile.mkstemp(
            prefix=tmp_prefix, suffix=_TMP_SUFFIX, dir=self._state_dir
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
            self._fsync_dir()
        finally:
            if tmp_left:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    def _fsync_dir(self):
        dir_fd = os.open(self._state_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
