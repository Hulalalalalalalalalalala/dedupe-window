"""Bounded deduplication window.

A :class:`Window` remembers the first sighting time of each key currently
retained.  Two eviction rules apply:

* capacity: admitting a new key while the window is full evicts the oldest
  key first;
* span: :meth:`Window.advance` drops every key whose first sighting is more
  than ``span`` behind the current time.

Persistence is a crash-safe atomic commit: :meth:`Window.save` writes a
checksummed document to a temporary file in the state directory and
atomically renames it over the single data file, so a crash mid-write
leaves the previous commit intact.  Processes coordinate through an
``flock`` lock on the state directory, so concurrent writers serialize
instead of clobbering each other, and a process dying while holding the
lock releases it automatically.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None

_STATE_FILE = "window.json"
_TEMP_PREFIX = _STATE_FILE + "."
_TEMP_SUFFIX = ".tmp"
_SIGNED_FIELDS = ("span", "capacity", "now", "admitted", "expired", "keys")


def _is_number(value):
    """Numbers are ints or floats; booleans do not count."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_key(key):
    if not isinstance(key, str):
        raise TypeError("key must be a string")


def _digest(payload):
    """A deterministic integrity digest over a state document."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
        self._entries = []  # [key, first_seen] pairs, oldest sighting first
        self._index = {}    # key -> first_seen, mirrors _entries
        self._admitted = 0
        self._expired = 0
        self._lock_depth = 0

    @contextlib.contextmanager
    def _locked(self, create=False):
        """Hold the state directory's file lock; re-entrant per window.

        The lock is an ``flock`` on the directory itself, so it leaves no
        extra file behind and is released by the kernel if the holder dies.
        """
        if self._lock_depth:
            self._lock_depth += 1
            try:
                yield
            finally:
                self._lock_depth -= 1
            return
        if create:
            os.makedirs(self._state_dir, exist_ok=True)
        if fcntl is None:
            yield
            return
        fd = os.open(self._state_dir, os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            self._lock_depth = 1
            try:
                yield
            finally:
                self._lock_depth = 0
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def observe(self, key):
        """Record a sighting; return True only when the key is newly admitted."""
        _check_key(key)
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
        """Move the current time to ``now`` and drop keys older than the span."""
        if not _is_number(now):
            raise TypeError("now must be an int or float")
        if now < self._now:
            raise ValueError("now is earlier than the current time")
        self._now = now
        dropped = 0
        while self._entries and now - self._entries[0][1] > self._span:
            key, _ = self._entries.pop(0)
            del self._index[key]
            dropped += 1
        self._expired += dropped
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
        """Atomically replace the state file with a checksummed snapshot.

        The document is written to a temporary file next to the data file
        and renamed over it, so a crash mid-write cannot truncate the last
        committed state and no intermediate file survives the rename.
        """
        payload = {
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "keys": [[key, first] for key, first in self._entries],
        }
        payload["checksum"] = _digest(
            {field: payload[field] for field in _SIGNED_FIELDS}
        )
        text = json.dumps(payload)
        with self._locked(create=True):
            self._discard_stale_temps()
            fd, tmp = tempfile.mkstemp(
                dir=self._state_dir, prefix=_TEMP_PREFIX, suffix=_TEMP_SUFFIX
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(text)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, os.path.join(self._state_dir, _STATE_FILE))
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
                raise
            self._fsync_dir()

    def _discard_stale_temps(self):
        """Remove temp files left behind by a crashed writer.

        Only called while holding the lock, and temp files are only created
        under the lock, so any temp file still present is stale.
        """
        for name in os.listdir(self._state_dir):
            if name.startswith(_TEMP_PREFIX) and name.endswith(_TEMP_SUFFIX):
                with contextlib.suppress(OSError):
                    os.unlink(os.path.join(self._state_dir, name))

    def _fsync_dir(self):
        """Best-effort durability barrier for the rename itself."""
        try:
            fd = os.open(self._state_dir, os.O_RDONLY)
        except OSError:
            return
        try:
            with contextlib.suppress(OSError):
                os.fsync(fd)
        finally:
            os.close(fd)

    def load(self):
        """Re-read the state file and replace memory only once it checks out."""
        path = os.path.join(self._state_dir, _STATE_FILE)
        if not os.path.exists(path):
            raise FileNotFoundError(f"state file not found: {path}")
        with self._locked():
            with open(path, "r", encoding="utf-8") as fh:
                raw = fh.read()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"state file is not valid JSON: {exc}") from exc
            now, admitted, expired, entries = self._restore(data)
            self._now = now
            self._admitted = admitted
            self._expired = expired
            self._entries = entries
            self._index = {key: first for key, first in entries}

    def _restore(self, data):
        """Validate a decoded state document without touching the window."""
        if not isinstance(data, dict):
            raise ValueError("state file must hold a JSON object")
        required = set(_SIGNED_FIELDS) | {"checksum"}
        missing = required - data.keys()
        if missing:
            raise ValueError(f"state file is missing fields: {sorted(missing)}")
        checksum = data["checksum"]
        if not isinstance(checksum, str):
            raise ValueError("state file has an invalid checksum")
        signed = {field: data[field] for field in _SIGNED_FIELDS}
        if _digest(signed) != checksum:
            raise ValueError("state file checksum does not match its contents")
        span = data["span"]
        capacity = data["capacity"]
        now = data["now"]
        admitted = data["admitted"]
        expired = data["expired"]
        raw_keys = data["keys"]
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
            if previous is not None and first < previous:
                raise ValueError("state file keys are not ordered by first sighting")
            seen_keys.add(key)
            previous = first
            entries.append([key, first])
        return now, admitted, expired, entries
