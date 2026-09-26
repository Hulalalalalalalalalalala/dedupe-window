"""Bounded deduplication window.

A :class:`Window` remembers the first sighting time of each key currently
retained.  Two eviction rules apply:

* capacity: admitting a new key while the window is full evicts the oldest
  key first;
* span: :meth:`Window.advance` drops every key whose first sighting is more
  than ``span`` behind the current time.

Persistence is crash safe.  ``save`` (and every mutating transaction) writes
the whole document to a temporary file in the state directory, fsyncs it and
atomically replaces ``window.json`` via ``os.replace``.  A SHA-256 checksum
over a canonical serialization of the document guards integrity, so a crash
mid-write, a truncated file or external tampering can never be mistaken for
a committed state: ``load`` either restores the last complete commit or raises
a deterministic error.

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
_TMP_PREFIX = _STATE_FILE + "."
_TMP_SUFFIX = ".tmp"
_VERSION = 1
_REQUIRED_FIELDS = (
    "version",
    "span",
    "capacity",
    "now",
    "admitted",
    "expired",
    "keys",
)


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


def read_settings(path):
    """Return ``(span, capacity)`` from a state file without opening a Window.

    Used by the command line to learn the construction settings before the
    window can be built.  Raises ValueError for a missing or corrupt file.
    """
    envelope = _read_document(path)
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
        self._last_commit = None  # checksum of the state last loaded/committed
        os.makedirs(self._state_dir, exist_ok=True)

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
                self._commit_state()
            return admitted

    def _admit(self, key):
        if key in self._index:
            return False
        while len(self._entries) >= self._capacity:
            oldest, _ = self._entries.pop(0)
            del self._index[oldest]
        self._entries.append([key, self._now])
        self._index[key] = self._now
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
        """Atomically write the whole window state to ``window.json``.

        The document goes to a same-directory temporary file, is fsynced and
        then renamed over the data file.  When another process has committed
        a newer state after this window's last load/commit, this save does not
        regress it: this window's changes already reached disk earlier.
        """
        os.makedirs(self._state_dir, exist_ok=True)
        with self._locked(True):
            path = os.path.join(self._state_dir, _STATE_FILE)
            try:
                envelope = _read_document(path)
            except FileNotFoundError:
                envelope = None
            except ValueError:
                envelope = None  # corrupt or unreadable: overwrite, as before
            if (
                envelope is not None
                and self._last_commit is not None
                and envelope["checksum"] != self._last_commit
            ):
                # A newer commit landed after this window's last load/commit;
                # never regress the shared file with older in-memory state.
                return
            self._commit_state()

    def load(self):
        """Re-read the state file and replace memory only once it checks out."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        with self._locked(False):
            envelope = _read_document(path)  # FileNotFoundError if absent
            now, admitted, expired, entries = self._restore(envelope)
        self._now = now
        self._clock = now
        self._admitted = admitted
        self._expired = expired
        self._entries = entries
        self._index = {key: first for key, first in entries}
        self._last_commit = envelope["checksum"]

    def _reload_if_present(self):
        """Adopt the latest committed state; a missing file starts from empty."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        try:
            envelope = _read_document(path)
        except FileNotFoundError:
            return
        now, admitted, expired, entries = self._restore(envelope)
        self._now = now
        self._admitted = admitted
        self._expired = expired
        self._entries = entries
        self._index = {key: first for key, first in entries}
        self._last_commit = envelope["checksum"]

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

    def _commit_state(self):
        """Write temp + fsync + atomic rename; caller holds the write lock."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        self._sweep_temp_files()
        body = {
            "version": _VERSION,
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "keys": [[key, first] for key, first in self._entries],
        }
        checksum = _checksum(body)
        envelope = dict(body)
        envelope["checksum"] = checksum
        data = json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        fd, tmp_name = tempfile.mkstemp(
            prefix=_TMP_PREFIX, suffix=_TMP_SUFFIX, dir=self._state_dir
        )
        tmp_left = True
        try:
            os.fchmod(fd, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
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

    def _restore(self, envelope):
        """Validate a decoded state document without touching the window."""
        missing = set(_REQUIRED_FIELDS) - envelope.keys()
        if missing:
            raise ValueError(f"state file is missing fields: {sorted(missing)}")
        version = envelope["version"]
        span = envelope["span"]
        capacity = envelope["capacity"]
        now = envelope["now"]
        admitted = envelope["admitted"]
        expired = envelope["expired"]
        raw_keys = envelope["keys"]
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValueError("state file has an invalid version")
        if version != _VERSION:
            raise ValueError("state file has an unsupported version")
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
        return now, admitted, expired, entries
