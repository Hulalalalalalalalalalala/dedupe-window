"""Command-line interface for the deduplication window."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .window import Window

_STATE_FILENAME = "window.json"
_DEFAULT_SPAN = 60.0
_DEFAULT_CAPACITY = 10000


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dedupe_window")
    parser.add_argument("--state", required=True, help="window state directory")
    parser.add_argument("--span", default=None, help="span in seconds for a new window")
    parser.add_argument(
        "--capacity",
        default=None,
        help="maximum number of retained keys for a new window",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    observe_parser = subparsers.add_parser("observe")
    observe_parser.add_argument("key")

    seen_parser = subparsers.add_parser("seen")
    seen_parser.add_argument("key")

    subparsers.add_parser("stats")
    return parser


def _parse_options(span_raw: str | None, capacity_raw: str | None) -> tuple[float, int]:
    span = _DEFAULT_SPAN
    capacity = _DEFAULT_CAPACITY
    if span_raw is not None:
        try:
            span = float(span_raw)
        except ValueError:
            raise _ArgumentError(f"invalid --span value: {span_raw!r}") from None
    if capacity_raw is not None:
        try:
            capacity = int(capacity_raw)
        except ValueError:
            raise _ArgumentError(f"invalid --capacity value: {capacity_raw!r}") from None
    return span, capacity


class _ArgumentError(Exception):
    """Raised for command-line arguments that never produce a valid Window."""


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        # argparse exits 2 on bad arguments, unknown options or subcommands.
        return int(exc.code) if exc.code is not None else 0

    try:
        span, capacity = _parse_options(args.span, args.capacity)
    except _ArgumentError as exc:
        print(f"dedupe_window: {exc}", file=sys.stderr)
        return 2

    try:
        window = Window(args.state, span, capacity)
    except (TypeError, ValueError) as exc:
        print(f"dedupe_window: {exc}", file=sys.stderr)
        return 2

    state_file = Path(args.state) / _STATE_FILENAME
    if state_file.exists():
        try:
            window.load()
        except (OSError, ValueError) as exc:
            print(f"dedupe_window: cannot read {state_file}: {exc}", file=sys.stderr)
            return 1

    if args.command == "observe":
        result = window.observe(args.key)
        try:
            window.save()
        except OSError as exc:
            print(f"dedupe_window: cannot write {state_file}: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result))
    elif args.command == "seen":
        print(json.dumps(window.seen(args.key)))
    else:
        print(json.dumps(window.stats()))
    return 0
