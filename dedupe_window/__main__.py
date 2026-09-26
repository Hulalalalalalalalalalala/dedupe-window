"""Command line entry point: python3 -m dedupe_window --state DIR <command>."""

from __future__ import annotations

import argparse
import json
import sys

from . import Window

DEFAULT_SPAN = 60
DEFAULT_CAPACITY = 1024


def _parser():
    parser = argparse.ArgumentParser(
        prog="dedupe_window",
        description="Bounded deduplication window for a stream of keys.",
    )
    parser.add_argument("--state", required=True, help="window state directory")
    sub = parser.add_subparsers(dest="command", required=True)
    observe = sub.add_parser("observe", help="record a sighting of a key")
    observe.add_argument("key")
    seen = sub.add_parser("seen", help="report whether a key is retained")
    seen.add_argument("key")
    sub.add_parser("stats", help="print window statistics")
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)

    window = Window(args.state, span=DEFAULT_SPAN, capacity=DEFAULT_CAPACITY)
    try:
        window.load()
    except FileNotFoundError:
        pass  # no saved window yet: start empty
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.command == "observe":
        print(json.dumps(window.observe(args.key)))
        window.save()
    elif args.command == "seen":
        print(json.dumps(window.seen(args.key)))
    else:  # stats
        print(json.dumps(window.stats(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
