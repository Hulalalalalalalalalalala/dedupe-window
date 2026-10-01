"""In-memory deduplication window with JSON persistence."""

from __future__ import annotations

import json
import math
import os
import time
from pathlib import Path
from typing import Union

_STATE_FILENAME = "window.json"


def _validate_span(span: object) -> float:
    if isinstance(span, bool) or not isinstance(span, (int, float)):
        raise TypeError("span must be a finite positive number of seconds")
    value = float(span)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("span must be a finite positive number of seconds")
    return value


def _validate_capacity(capacity: object) -> int:
    if isinstance(capacity, bool) or not isinstance(capacity, int):
        raise TypeError("capacity must be a positive integer")
    if capacity <= 0:
        raise ValueError("capacity must be a positive integer")
    return capacity


def _validate_now(now: object) -> float:
    if isinstance(now, bool) or not isinstance(now, (int, float)):
        raise TypeError("now must be a finite number of seconds")
    value = float(now)
    if not math.isfinite(value):
        raise ValueError("now must be a finite number of seconds")
    return value


class Window:
    """Retains text keys for a bounded time span and a bounded key count.

    Each key remembers only the Unix time of its first sighting within the
    current window. Repeated sightings never refresh that time.
    """

    def __init__(self, state: Union[str, os.PathLike[str]], span: float, capacity: int):
        if not isinstance(state, (str, os.PathLike)):
            raise TypeError("state must be a directory path")
        self._state_path = Path(state)
        self._span = _validate_span(span)
        self._capacity = _validate_capacity(capacity)
        self._first_seen: dict[str, float] = {}
        self._admitted = 0
        self._expired = 0

    @property
    def state_path(self) -> Path:
        return self._state_path

    def observe(self, key: str) -> bool:
        if not isinstance(key, str):
            raise TypeError("key must be text")
        now = time.time()
        self._evict_expired(now)
        if key in self._first_seen:
            return False
        self._first_seen[key] = now
        self._admitted += 1
        self._evict_over_capacity()
        return True

    def seen(self, key: str) -> bool:
        if not isinstance(key, str):
            raise TypeError("key must be text")
        return key in self._first_seen

    def advance(self, now: float) -> int:
        now_value = _validate_now(now)
        return self._evict_expired(now_value)

    def keys(self) -> list[str]:
        return self._ordered_keys()

    def _ordered_keys(self, limit: int | None = None) -> list[str]:
        # dict insertion order supplies a stable oldest-first tiebreaker.
        ranked = [(first_at, rank, key) for rank, (key, first_at) in enumerate(self._first_seen.items())]
        ranked.sort(key=lambda item: (item[0], item[1]))
        if limit is not None:
            ranked = ranked[:limit]
        return [key for _, _, key in ranked]

    def stats(self) -> dict:
        return {
            "span": self._span,
            "capacity": self._capacity,
            "retained": len(self._first_seen),
            "admitted": self._admitted,
            "expired": self._expired,
        }

    def save(self) -> None:
        payload = {
            "span": self._span,
            "capacity": self._capacity,
            "admitted": self._admitted,
            "expired": self._expired,
            "keys": dict(self._first_seen),
        }
        self._state_path.mkdir(parents=True, exist_ok=True)
        final_path = self._state_path / _STATE_FILENAME
        temp_path = self._state_path / f".{_STATE_FILENAME}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, final_path)

    def load(self) -> None:
        path = self._state_path / _STATE_FILENAME
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
        span, capacity, admitted, expired, first_seen = _parse_payload(raw)
        # Replace memory only after the whole document has validated.
        self._span = span
        self._capacity = capacity
        self._admitted = admitted
        self._expired = expired
        self._first_seen = first_seen

    def _evict_expired(self, now: float) -> int:
        cutoff = now - self._span
        expired_keys = [key for key, first_at in self._first_seen.items() if first_at < cutoff]
        for key in expired_keys:
            del self._first_seen[key]
        self._expired += len(expired_keys)
        return len(expired_keys)

    def _evict_over_capacity(self) -> None:
        excess = len(self._first_seen) - self._capacity
        if excess <= 0:
            return
        for key in self._ordered_keys(excess):
            del self._first_seen[key]
        self._expired += excess


def _parse_payload(raw: object) -> tuple[float, int, int, int, dict[str, float]]:
    if not isinstance(raw, dict):
        raise ValueError("window state must be a JSON object")
    missing = {"span", "capacity", "admitted", "expired", "keys"} - raw.keys()
    if missing:
        raise ValueError(f"window state is missing fields: {sorted(missing)}")

    try:
        span = _validate_span(raw["span"])
        capacity = _validate_capacity(raw["capacity"])
    except TypeError as exc:
        # A loaded document with wrong types is invalid content, not a
        # programming error, so load() reports it as ValueError.
        raise ValueError(str(exc)) from exc

    admitted = raw["admitted"]
    expired = raw["expired"]
    if isinstance(admitted, bool) or not isinstance(admitted, int) or admitted < 0:
        raise ValueError("admitted must be a non-negative integer")
    if isinstance(expired, bool) or not isinstance(expired, int) or expired < 0:
        raise ValueError("expired must be a non-negative integer")

    raw_keys = raw["keys"]
    if not isinstance(raw_keys, dict):
        raise ValueError("keys must be a JSON object")
    first_seen: dict[str, float] = {}
    for key, first_at in raw_keys.items():
        if not isinstance(key, str):
            raise ValueError("keys must be text")
        if isinstance(first_at, bool) or not isinstance(first_at, (int, float)):
            raise ValueError("key timestamps must be numbers")
        first_at_value = float(first_at)
        if not math.isfinite(first_at_value):
            raise ValueError("key timestamps must be finite numbers")
        first_seen[key] = first_at_value

    return span, capacity, admitted, expired, first_seen
