"""Command-line entry point: ``python -m dedupe_window``."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
