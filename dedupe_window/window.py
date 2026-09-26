"""Bounded deduplication window.

A :class:`Window` remembers the first sighting time of each key currently
retained.  Two eviction rules apply:

* capacity: admitting a new key while the window is full evicts the oldest
  key first;
* span: :meth:`Window.advance` drops every key whose first sighting is more
  than ``span`` behind the current time.

Single process only; no cross-process locking.
"""

from __future__ import annotations

import json
import os

_STATE_FILE = "window.json"


def _is_number(value):
    """Numbers are ints or floats; booleans do not count."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_key(key):
    if not isinstance(key, str):
        raise TypeError("key must be a string")


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
        os.makedirs(self._state_dir, exist_ok=True)

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
        """Write the whole window state to ``window.json`` in the state directory."""
        os.makedirs(self._state_dir, exist_ok=True)
        payload = {
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "admitted": self._admitted,
            "expired": self._expired,
            "keys": [[key, first] for key, first in self._entries],
        }
        path = os.path.join(self._state_dir, _STATE_FILE)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)

    def load(self):
        """Re-read the state file and replace memory only once it checks out."""
        path = os.path.join(self._state_dir, _STATE_FILE)
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
        missing = {"span", "capacity", "now", "admitted", "expired", "keys"} - data.keys()
        if missing:
            raise ValueError(f"state file is missing fields: {sorted(missing)}")
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
