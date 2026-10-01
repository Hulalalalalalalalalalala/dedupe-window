"""End-to-end tests for the command-line interface."""

import io
import json
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory

from dedupe_window.cli import main


class CliTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state = self._tmp.name
        self.state_file = Path(self.state) / "window.json"

    def run_cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(list(args))
        return code, out.getvalue(), err.getvalue()

    def test_observe_seen_stats_share_state(self):
        base = ["--state", self.state]

        code, out, _ = self.run_cli(*base, "observe", "a")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), True)

        code, out, _ = self.run_cli(*base, "observe", "a")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), False)

        code, out, _ = self.run_cli(*base, "seen", "a")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), True)

        code, out, _ = self.run_cli(*base, "seen", "b")
        self.assertEqual(json.loads(out), False)

        code, out, _ = self.run_cli(*base, "stats")
        self.assertEqual(code, 0)
        stats = json.loads(out)
        self.assertEqual(stats["span"], 60.0)
        self.assertEqual(stats["capacity"], 10000)
        self.assertEqual(stats["retained"], 1)
        self.assertEqual(stats["admitted"], 1)
        self.assertEqual(stats["expired"], 0)
        self.assertTrue(self.state_file.exists())

    def test_new_window_overrides(self):
        code, _, _ = self.run_cli(
            "--state", self.state, "--span", "5", "--capacity", "2", "stats"
        )
        self.assertEqual(code, 0)
        # stats alone does not persist; an observe creates the file.
        code, _, _ = self.run_cli(
            "--state", self.state, "--span", "5", "--capacity", "2", "observe", "x"
        )
        self.assertEqual(code, 0)
        payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(payload["span"], 5.0)
        self.assertEqual(payload["capacity"], 2)

    def test_loaded_state_ignores_new_window_options(self):
        self.run_cli("--state", self.state, "--span", "5", "--capacity", "2", "observe", "x")
        code, out, _ = self.run_cli(
            "--state", self.state, "--span", "999", "--capacity", "999", "stats"
        )
        self.assertEqual(code, 0)
        stats = json.loads(out)
        self.assertEqual(stats["span"], 5.0)
        self.assertEqual(stats["capacity"], 2)

    def test_unknown_subcommand_exits_2(self):
        code, _, _ = self.run_cli("--state", self.state, "bogus")
        self.assertEqual(code, 2)

    def test_missing_required_arg_exits_2(self):
        code, _, _ = self.run_cli("observe", "x")
        self.assertEqual(code, 2)

    def test_invalid_option_value_exits_2(self):
        code, _, _ = self.run_cli("--state", self.state, "--span", "soon", "stats")
        self.assertEqual(code, 2)
        code, _, _ = self.run_cli("--state", self.state, "--capacity", "lots", "stats")
        self.assertEqual(code, 2)

    def test_corrupt_state_exits_1(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text("broken", encoding="utf-8")
        code, out, err = self.run_cli("--state", self.state, "stats")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("window.json", err)


if __name__ == "__main__":
    unittest.main()
