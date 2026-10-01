"""Core :class:`Window` implementation.

The window remembers the first-seen time of every retained text key. Two
eviction rules apply:

* time -- a key is dropped once its first-seen time is older than ``span``;
* capacity -- when more than ``capacity`` keys are retained, the oldest go.

Only the standard library is used and state is a single JSON file, so the
window can be handed between short-lived processes.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
import time
from typing import Any

STATE_FILENAME = "window.json"


def _is_real_int(value: Any) -> bool:
    """True for genuine ints (bool is an int subclass but means a flag)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_real_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_span(span: Any) -> float:
    if not _is_real_number(span):
        raise TypeError("span must be a number of seconds")
    span = float(span)
    if not math.isfinite(span) or span <= 0:
        raise ValueError("span must be a finite positive number of seconds")
    return span


def _validate_capacity(capacity: Any) -> int:
    if not _is_real_int(capacity):
        raise TypeError("capacity must be an integer")
    if capacity <= 0:
        raise ValueError("capacity must be a positive integer")
    return int(capacity)


def _validate_now(now: Any) -> float:
    if not _is_real_number(now):
        raise TypeError("now must be a number of Unix seconds")
    now = float(now)
    if not math.isfinite(now):
        raise ValueError("now must be a finite number of Unix seconds")
    return now


class Window:
    """A bounded deduplication window backed by ``state/window.json``."""

    def __init__(self, state: str | os.PathLike[str], span: Any, capacity: Any) -> None:
        if not isinstance(state, (str, os.PathLike)):
            raise TypeError("state must be a directory path")
        self._state = os.fspath(state)
        self._span = _validate_span(span)
        self._capacity = _validate_capacity(capacity)
        # Insertion order breaks ties between equal first-seen timestamps.
        self._first_seen: dict[str, float] = {}
        self._admitted = 0
        self._expired = 0

    @property
    def state_path(self) -> str:
        return os.path.join(self._state, STATE_FILENAME)

    def _prune_expired(self, now: float) -> int:
        """Drop keys whose first-seen time is earlier than ``now - span``."""
        horizon = now - self._span
        dead = [key for key, seen_at in self._first_seen.items() if seen_at < horizon]
        for key in dead:
            del self._first_seen[key]
        self._expired += len(dead)
        return len(dead)

    def _prune_capacity(self) -> None:
        """While over capacity, evict the oldest first-seen key."""
        while len(self._first_seen) > self._capacity:
            oldest = min(self._first_seen, key=self._first_seen.get)
            del self._first_seen[oldest]
            self._expired += 1

    def observe(self, key: Any) -> bool:
        """Record a sighting; return True if ``key`` is newly admitted.

        Expired keys are evicted first using the current Unix time. A repeat of
        a retained key returns False and never refreshes its first-seen time.
        """
        if not isinstance(key, str):
            raise TypeError("key must be text (str)")
        now = time.time()
        self._prune_expired(now)
        if key in self._first_seen:
            return False
        self._first_seen[key] = now
        self._admitted += 1
        self._prune_capacity()
        return True

    def seen(self, key: Any) -> bool:
        """Report current membership without recording or evicting anything."""
        if not isinstance(key, str):
            raise TypeError("key must be text (str)")
        return key in self._first_seen

    def advance(self, now: Any) -> int:
        """Drop keys first seen before ``now - span``; return how many went."""
        now = _validate_now(now)
        removed = self._prune_expired(now)
        self._prune_capacity()
        return removed

    def keys(self) -> list[str]:
        """Retained keys ordered by first-seen time, oldest first."""
        return sorted(self._first_seen, key=self._first_seen.get)

    def stats(self) -> dict[str, Any]:
        return {
            "span": self._span,
            "capacity": self._capacity,
            "retained": len(self._first_seen),
            "admitted": self._admitted,
            "expired": self._expired,
        }

    def save(self) -> None:
        """Persist span, capacity, counters and every key's first-seen time."""
        os.makedirs(self._state, exist_ok=True)
        payload = {
            "span": self._span,
            "capacity": self._capacity,
            "admitted": self._admitted,
            "expired": self._expired,
            "keys": dict(self._first_seen),
        }
        # Write to a sibling temp file and atomically replace so a failed
        # write never leaves a truncated window.json behind.
        fd, tmp_name = tempfile.mkstemp(dir=self._state, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
            os.replace(tmp_name, self.state_path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    def load(self) -> None:
        """Replace in-memory state with the file, but only if it is fully valid."""
        try:
            with open(self.state_path, encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            raise
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"invalid {STATE_FILENAME}: {exc}") from exc

        span, capacity, admitted, expired, first_seen = _parse_payload(data)

        # Commit only after every field has been validated.
        self._span = span
        self._capacity = capacity
        self._admitted = admitted
        self._expired = expired
        self._first_seen = first_seen


def _parse_payload(data: Any) -> tuple[float, int, int, int, dict[str, float]]:
    if not isinstance(data, dict):
        raise ValueError(f"invalid {STATE_FILENAME}: top-level value must be an object")

    required = ("span", "capacity", "admitted", "expired", "keys")
    missing = [name for name in required if name not in data]
    if missing:
        raise ValueError(f"invalid {STATE_FILENAME}: missing fields {', '.join(missing)}")

    # Configuration types surface as TypeError from direct use, but a loaded
    # file with the wrong types is invalid content -> ValueError.
    try:
        span = _validate_span(data["span"])
        capacity = _validate_capacity(data["capacity"])
    except TypeError as exc:
        raise ValueError(f"invalid {STATE_FILENAME}: {exc}") from exc

    if not _is_real_int(data["admitted"]) or data["admitted"] < 0:
        raise ValueError(f"invalid {STATE_FILENAME}: admitted must be a non-negative integer")
    if not _is_real_int(data["expired"]) or data["expired"] < 0:
        raise ValueError(f"invalid {STATE_FILENAME}: expired must be a non-negative integer")
    admitted = int(data["admitted"])
    expired = int(data["expired"])

    raw_keys = data["keys"]
    if not isinstance(raw_keys, dict):
        raise ValueError(f"invalid {STATE_FILENAME}: keys must be an object")

    first_seen: dict[str, float] = {}
    for key, seen_at in raw_keys.items():
        if not isinstance(key, str):
            raise ValueError(f"invalid {STATE_FILENAME}: keys must map text keys to times")
        if not _is_real_number(seen_at):
            raise ValueError(f"invalid {STATE_FILENAME}: first-seen time for {key!r} must be a number")
        seen_at = float(seen_at)
        if not math.isfinite(seen_at):
            raise ValueError(f"invalid {STATE_FILENAME}: first-seen time for {key!r} must be finite")
        first_seen[key] = seen_at

    if len(first_seen) > capacity:
        raise ValueError(f"invalid {STATE_FILENAME}: more keys retained than capacity allows")
    if admitted < len(first_seen):
        raise ValueError(f"invalid {STATE_FILENAME}: admitted is smaller than retained key count")

    return span, capacity, admitted, expired, first_seen
