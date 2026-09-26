"""Bounded deduplication window for a stream of text keys."""

from __future__ import annotations

import json
import os

__all__ = ["Window"]

STATE_FILE = "window.json"


def _check_number(value, name):
    """Return value if it is a real number (int/float, not bool)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    return value


def _check_positive(value, name):
    value = _check_number(value, name)
    if not value > 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


class Window:
    """A bounded deduplication window.

    Each key is remembered with the moment of its first sighting. A key
    leaves the window either because the capacity forces out the oldest
    key, or because ``advance`` moves the current moment far enough that
    the sighting falls outside the span.
    """

    def __init__(self, state, span, capacity):
        if not isinstance(state, (str, os.PathLike)):
            raise TypeError("state must be a path-like value")
        self._state = os.fspath(state)
        self._span = _check_positive(span, "span")
        self._capacity = _check_positive(capacity, "capacity")
        self._now = 0
        self._entries = {}  # key -> first sighting, oldest first
        self._admitted = 0
        self._expired = 0

    def observe(self, key):
        """Record a sighting; return True only if the key is newly admitted."""
        if not isinstance(key, str):
            raise TypeError(f"key must be a string, got {type(key).__name__}")
        if key in self._entries:
            return False
        if len(self._entries) >= self._capacity:
            oldest = next(iter(self._entries))
            del self._entries[oldest]
        self._entries[key] = self._now
        self._admitted += 1
        return True

    def seen(self, key):
        """Report membership without recording anything."""
        if not isinstance(key, str):
            raise TypeError(f"key must be a string, got {type(key).__name__}")
        return key in self._entries

    def advance(self, now):
        """Move the current moment and drop sightings older than the span."""
        now = _check_number(now, "now")
        if now < self._now:
            raise ValueError("now is earlier than the current moment")
        self._now = now
        cutoff = now - self._span
        doomed = [k for k, t in self._entries.items() if t < cutoff]
        for k in doomed:
            del self._entries[k]
        self._expired += len(doomed)
        return len(doomed)

    def keys(self):
        """Retained keys, oldest sighting first."""
        return list(self._entries)

    def stats(self):
        """Report span, capacity, retained, admitted and expired counts."""
        return {
            "span": self._span,
            "capacity": self._capacity,
            "retained": len(self._entries),
            "admitted": self._admitted,
            "expired": self._expired,
        }

    def save(self):
        """Persist the whole window into ``<state>/window.json``."""
        os.makedirs(self._state, exist_ok=True)
        data = {
            "span": self._span,
            "capacity": self._capacity,
            "now": self._now,
            "entries": [[key, t] for key, t in self._entries.items()],
            "admitted": self._admitted,
            "expired": self._expired,
        }
        path = os.path.join(self._state, STATE_FILE)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp, path)

    def load(self):
        """Re-read the state file and replace memory with it.

        Raises FileNotFoundError if no state was saved, ValueError if the
        file is not valid JSON, does not hold a complete state, or its
        settings do not match this window.
        """
        path = os.path.join(self._state, STATE_FILE)
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"state file is not valid JSON: {exc}") from exc

        span, capacity, now, entries, admitted, expired = self._validate(data)

        self._now = now
        self._entries = entries
        self._admitted = admitted
        self._expired = expired

    def _validate(self, data):
        if not isinstance(data, dict):
            raise ValueError("state file does not hold a window state")
        required = ("span", "capacity", "now", "entries", "admitted", "expired")
        missing = [name for name in required if name not in data]
        if missing:
            raise ValueError(f"state file is missing: {', '.join(missing)}")

        span = data["span"]
        capacity = data["capacity"]
        for name, value in (("span", span), ("capacity", capacity)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
                raise ValueError(f"state file has an invalid {name}")
        if span != self._span or capacity != self._capacity:
            raise ValueError("state file settings do not match this window")

        now = data["now"]
        if isinstance(now, bool) or not isinstance(now, (int, float)) or now < 0:
            raise ValueError("state file has an invalid current moment")

        raw_entries = data["entries"]
        if not isinstance(raw_entries, list):
            raise ValueError("state file has invalid entries")
        entries = {}
        for item in raw_entries:
            if (
                not isinstance(item, (list, tuple))
                or len(item) != 2
                or not isinstance(item[0], str)
                or isinstance(item[1], bool)
                or not isinstance(item[1], (int, float))
            ):
                raise ValueError("state file has an invalid entry")
            key, moment = item
            if key in entries:
                raise ValueError("state file has a duplicate key")
            entries[key] = moment
        if len(entries) > capacity:
            raise ValueError("state file retains more keys than the capacity")

        admitted = data["admitted"]
        expired = data["expired"]
        for name, value in (("admitted", admitted), ("expired", expired)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"state file has an invalid {name} count")

        return span, capacity, now, entries, admitted, expired
