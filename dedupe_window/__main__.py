"""Command-line entry point: ``python -m dedupe_window ...``."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Sequence

from .window import STATE_FILENAME, Window

DEFAULT_SPAN = 60.0
DEFAULT_CAPACITY = 10_000


def _positive_seconds(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid span: {raw!r}") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("span must be a finite positive number of seconds")
    return value


def _positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid capacity: {raw!r}") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("capacity must be a positive integer")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dedupe_window",
        description="Bounded deduplication window for a stream of text keys.",
    )
    parser.add_argument("--state", required=True, help="window state directory")
    parser.add_argument(
        "--span",
        type=_positive_seconds,
        default=DEFAULT_SPAN,
        help="retention span in seconds for a new window (default: 60)",
    )
    parser.add_argument(
        "--capacity",
        type=_positive_int,
        default=DEFAULT_CAPACITY,
        help="maximum retained keys for a new window (default: 10000)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    observe_parser = subparsers.add_parser("observe", help="record a sighting")
    observe_parser.add_argument("key")

    seen_parser = subparsers.add_parser("seen", help="report membership without recording")
    seen_parser.add_argument("key")

    subparsers.add_parser("stats", help="report window statistics")
    return parser


def _open_window(state: str, span: float, capacity: int) -> Window:
    """Load window.json when it exists, otherwise start from the options."""
    window = Window(state, span, capacity)
    if os.path.exists(window.state_path):
        window.load()
    return window


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        window = _open_window(args.state, args.span, args.capacity)
        if args.command == "observe":
            result: object = window.observe(args.key)
            window.save()
        elif args.command == "seen":
            result = window.seen(args.key)
        else:  # stats
            result = window.stats()
    except (OSError, ValueError) as exc:
        print(f"dedupe_window: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
