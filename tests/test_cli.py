"""End-to-end tests for the ``python -m dedupe_window`` command."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr

from dedupe_window.__main__ import main


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.state = self.tmp.name
        self.addCleanup(self.tmp.cleanup)

    def run_cli(self, *argv: str) -> tuple[int, str]:
        out = io.StringIO()
        err = io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                code = main(argv)
            except SystemExit as exc:
                code = int(exc.code) if exc.code is not None else 0
        return code, out.getvalue()

    def last_json_line(self, output: str) -> object:
        lines = [line for line in output.splitlines() if line.strip()]
        self.assertEqual(len(lines), 1, output)
        return json.loads(lines[0])

    def test_observe_then_seen_across_processes(self) -> None:
        code, out = self.run_cli("--state", self.state, "observe", "k")
        self.assertEqual(code, 0)
        self.assertIs(self.last_json_line(out), True)

        code, out = self.run_cli("--state", self.state, "observe", "k")
        self.assertEqual(code, 0)
        self.assertIs(self.last_json_line(out), False)

        code, out = self.run_cli("--state", self.state, "seen", "k")
        self.assertEqual(code, 0)
        self.assertIs(self.last_json_line(out), True)

        code, out = self.run_cli("--state", self.state, "seen", "other")
        self.assertEqual(code, 0)
        self.assertIs(self.last_json_line(out), False)

    def test_stats_output_and_persisted_counters(self) -> None:
        self.run_cli("--state", self.state, "--span", "15", "--capacity", "2", "observe", "a")
        self.run_cli("--state", self.state, "observe", "b")
        self.run_cli("--state", self.state, "observe", "c")  # evicts a
        code, out = self.run_cli("--state", self.state, "stats")
        self.assertEqual(code, 0)
        stats = self.last_json_line(out)
        self.assertEqual(
            stats,
            {"span": 15.0, "capacity": 2, "retained": 2, "admitted": 3, "expired": 1},
        )

    def test_flags_ignored_once_window_exists(self) -> None:
        self.run_cli("--state", self.state, "--span", "15", "--capacity", "2", "observe", "a")
        # Existing window keeps its own span/capacity; flags only seed a new one.
        code, out = self.run_cli(
            "--state", self.state, "--span", "999", "--capacity", "999", "stats"
        )
        self.assertEqual(code, 0)
        stats = self.last_json_line(out)
        self.assertEqual(stats["span"], 15.0)
        self.assertEqual(stats["capacity"], 2)

    def test_seen_does_not_create_state_file(self) -> None:
        code, _ = self.run_cli("--state", self.state, "seen", "a")
        self.assertEqual(code, 0)
        self.assertFalse(os.path.exists(os.path.join(self.state, "window.json")))

    def test_missing_state_argument_is_exit_2(self) -> None:
        code, _ = self.run_cli("observe", "a")
        self.assertEqual(code, 2)

    def test_unknown_subcommand_is_exit_2(self) -> None:
        code, _ = self.run_cli("--state", self.state, "frobnicate")
        self.assertEqual(code, 2)

    def test_unknown_option_is_exit_2(self) -> None:
        code, _ = self.run_cli("--bogus", "observe", "a")
        self.assertEqual(code, 2)

    def test_invalid_span_and_capacity_are_exit_2(self) -> None:
        code, _ = self.run_cli("--state", self.state, "--span", "0", "observe", "a")
        self.assertEqual(code, 2)
        code, _ = self.run_cli("--state", self.state, "--capacity", "x", "observe", "a")
        self.assertEqual(code, 2)
        code, _ = self.run_cli("--state", self.state, "--capacity", "-1", "stats")
        self.assertEqual(code, 2)

    def test_corrupt_state_is_exit_1(self) -> None:
        os.makedirs(self.state, exist_ok=True)
        with open(os.path.join(self.state, "window.json"), "w") as handle:
            handle.write("not json at all")
        code, _ = self.run_cli("--state", self.state, "stats")
        self.assertEqual(code, 1)

    def test_unwritable_state_is_exit_1(self) -> None:
        # A path that cannot be created (its parent is a regular file).
        blocker = os.path.join(self.state, "file")
        with open(blocker, "w") as handle:
            handle.write("x")
        bad_state = os.path.join(blocker, "window-dir")
        code, _ = self.run_cli("--state", bad_state, "observe", "a")
        self.assertEqual(code, 1)

    def test_module_entry_point(self) -> None:
        import subprocess

        env = dict(os.environ)
        result = subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", self.state, "observe", "m"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), True)


if __name__ == "__main__":
    unittest.main()
