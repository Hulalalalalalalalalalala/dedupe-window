import json
import os
import subprocess
import sys
import tempfile
import unittest

from dedupe_window import Window

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class WindowBehaviourTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "window")

    def make_window(self, span=10, capacity=3):
        return Window(self.state, span=span, capacity=capacity)

    def test_new_key_is_admitted_once(self):
        w = self.make_window()
        self.assertTrue(w.observe("a"))
        self.assertFalse(w.observe("a"))
        self.assertEqual(w.stats()["admitted"], 1)

    def test_empty_string_is_a_key(self):
        w = self.make_window()
        self.assertTrue(w.observe(""))
        self.assertTrue(w.seen(""))
        self.assertFalse(w.observe(""))

    def test_repeat_does_not_refresh_or_reorder(self):
        w = self.make_window()
        w.observe("a")
        w.advance(5)
        w.observe("b")
        self.assertFalse(w.observe("a"))
        self.assertEqual(w.keys(), ["a", "b"])
        w.advance(11)  # a is 11 old, b is 6 old: only a expires
        self.assertEqual(w.keys(), ["b"])

    def test_seen_does_not_record(self):
        w = self.make_window()
        self.assertFalse(w.seen("a"))
        self.assertFalse(w.seen("a"))
        self.assertEqual(w.stats()["admitted"], 0)
        self.assertEqual(w.keys(), [])

    def test_capacity_evicts_oldest(self):
        w = self.make_window(capacity=2)
        w.observe("a")
        w.observe("b")
        self.assertTrue(w.observe("c"))
        self.assertEqual(w.keys(), ["b", "c"])
        self.assertFalse(w.seen("a"))

    def test_capacity_eviction_is_not_expiry(self):
        w = self.make_window(capacity=1)
        w.observe("a")
        w.observe("b")
        stats = w.stats()
        self.assertEqual(stats["admitted"], 2)
        self.assertEqual(stats["expired"], 0)
        self.assertEqual(stats["retained"], 1)

    def test_repeat_of_full_window_does_not_evict(self):
        w = self.make_window(capacity=2)
        w.observe("a")
        w.observe("b")
        self.assertFalse(w.observe("a"))
        self.assertEqual(w.keys(), ["a", "b"])

    def test_advance_expires_strictly_older_than_span(self):
        w = self.make_window(span=10)
        w.observe("old")
        w.advance(5)
        w.observe("edge")
        self.assertEqual(w.advance(15), 1)  # old is 15 old, edge exactly 10
        self.assertEqual(w.keys(), ["edge"])
        self.assertEqual(w.stats()["expired"], 1)

    def test_advance_returns_count_and_accumulates(self):
        w = self.make_window(span=1)
        w.observe("a")
        w.observe("b")
        self.assertEqual(w.advance(10), 2)
        self.assertEqual(w.advance(10), 0)
        self.assertEqual(w.stats()["expired"], 2)

    def test_advance_rejects_earlier_now(self):
        w = self.make_window()
        w.advance(5)
        with self.assertRaises(ValueError):
            w.advance(4)
        w.advance(5)  # equal is fine

    def test_keys_are_oldest_first(self):
        w = self.make_window()
        for key in ("c", "a", "b"):
            w.observe(key)
        self.assertEqual(w.keys(), ["c", "a", "b"])

    def test_stats_fields_and_order(self):
        w = self.make_window(span=7, capacity=5)
        w.observe("a")
        self.assertEqual(
            list(w.stats().items()),
            [("span", 7), ("capacity", 5), ("retained", 1),
             ("admitted", 1), ("expired", 0)],
        )


class WindowValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = self.tmp.name

    def test_non_positive_span_or_capacity(self):
        for bad in (0, -1, -0.5):
            with self.assertRaises(ValueError):
                Window(self.state, span=bad, capacity=1)
            with self.assertRaises(ValueError):
                Window(self.state, span=1, capacity=bad)

    def test_zero_is_not_positive(self):
        with self.assertRaises(ValueError):
            Window(self.state, span=0, capacity=10)

    def test_wrong_types_raise_type_error(self):
        for bad in ("10", None, True, [1]):
            with self.assertRaises(TypeError):
                Window(self.state, span=bad, capacity=1)
            with self.assertRaises(TypeError):
                Window(self.state, span=1, capacity=bad)
        w = Window(self.state, span=1, capacity=1)
        with self.assertRaises(TypeError):
            w.advance("later")
        with self.assertRaises(TypeError):
            w.advance(True)

    def test_float_span_and_capacity_are_numbers(self):
        w = Window(self.state, span=2.5, capacity=1.5)
        w.observe("a")
        self.assertEqual(w.advance(2.5), 0)  # exactly at the span: kept
        self.assertEqual(w.advance(2.6), 1)

    def test_key_must_be_string(self):
        w = Window(self.state, span=1, capacity=1)
        for bad in (1, 1.0, None, b"a", True):
            with self.assertRaises(TypeError):
                w.observe(bad)
            with self.assertRaises(TypeError):
                w.seen(bad)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "window")

    def test_save_creates_state_directory_and_file(self):
        w = Window(self.state, span=10, capacity=4)
        w.observe("a")
        w.save()
        self.assertTrue(os.path.isfile(os.path.join(self.state, "window.json")))

    def test_roundtrip_restores_everything(self):
        w = Window(self.state, span=10, capacity=4)
        w.observe("a")
        w.observe("b")
        w.advance(3)
        w.observe("c")
        w.save()

        other = Window(self.state, span=10, capacity=4)
        other.load()
        self.assertEqual(other.keys(), ["a", "b", "c"])
        self.assertEqual(other.stats(), w.stats())
        # First-sighting moments survive: a and b were seen at t=0, c at t=3.
        self.assertEqual(other.advance(13), 2)  # a, b are 13 old; c exactly 10
        self.assertEqual(other.keys(), ["c"])

    def test_load_missing_file(self):
        w = Window(self.state, span=10, capacity=4)
        with self.assertRaises(FileNotFoundError):
            w.load()

    def write_state(self, content):
        os.makedirs(self.state, exist_ok=True)
        with open(os.path.join(self.state, "window.json"), "w",
                  encoding="utf-8") as fh:
            fh.write(content)

    def test_load_invalid_json(self):
        self.write_state("{not json")
        with self.assertRaises(ValueError):
            Window(self.state, span=10, capacity=4).load()

    def test_load_incomplete_state(self):
        self.write_state(json.dumps({"span": 10, "capacity": 4}))
        with self.assertRaises(ValueError):
            Window(self.state, span=10, capacity=4).load()

    def test_load_settings_mismatch(self):
        w = Window(self.state, span=10, capacity=4)
        w.save()
        with self.assertRaises(ValueError):
            Window(self.state, span=11, capacity=4).load()
        with self.assertRaises(ValueError):
            Window(self.state, span=10, capacity=5).load()

    def test_load_replaces_memory_only_after_full_read(self):
        w = Window(self.state, span=10, capacity=4)
        w.observe("kept")
        w.save()
        w.observe("transient")
        w.load()
        self.assertEqual(w.keys(), ["kept"])

    def test_failed_load_leaves_memory_untouched(self):
        w = Window(self.state, span=10, capacity=4)
        w.observe("kept")
        self.write_state("garbage")
        with self.assertRaises(ValueError):
            w.load()
        self.assertEqual(w.keys(), ["kept"])


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "window")

    def run_cli(self, *argv):
        env = dict(os.environ, PYTHONPATH=REPO_ROOT)
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", *argv],
            capture_output=True, text=True, env=env, cwd=REPO_ROOT,
        )

    def test_observe_seen_stats_flow(self):
        r = self.run_cli("--state", self.state, "observe", "a")
        self.assertEqual((r.returncode, r.stdout), (0, "true\n"))
        r = self.run_cli("--state", self.state, "observe", "a")
        self.assertEqual((r.returncode, r.stdout), (0, "false\n"))
        r = self.run_cli("--state", self.state, "seen", "a")
        self.assertEqual((r.returncode, r.stdout), (0, "true\n"))
        r = self.run_cli("--state", self.state, "seen", "b")
        self.assertEqual((r.returncode, r.stdout), (0, "false\n"))
        r = self.run_cli("--state", self.state, "stats")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(
            r.stdout,
            '{"span":60,"capacity":1024,"retained":1,"admitted":1,"expired":0}\n',
        )

    def test_seen_and_stats_do_not_write(self):
        self.run_cli("--state", self.state, "seen", "a")
        self.run_cli("--state", self.state, "stats")
        self.assertFalse(os.path.exists(self.state))

    def test_observe_persists_across_invocations(self):
        self.run_cli("--state", self.state, "observe", "x")
        r = self.run_cli("--state", self.state, "observe", "x")
        self.assertEqual(r.stdout, "false\n")

    def test_usage_errors_exit_2(self):
        for argv in (
            ("observe", "a"),                    # missing --state
            ("--state", self.state),             # missing subcommand
            ("--state", self.state, "frobnicate"),  # unknown subcommand
            ("--state", self.state, "observe"),  # missing key
            ("--state", self.state, "observe", "a", "b"),  # extra arg
            ("--state", self.state, "stats", "a"),  # extra arg
        ):
            result = self.run_cli(*argv)
            self.assertEqual(result.returncode, 2, argv)
            self.assertIn("usage", (result.stderr + result.stdout).lower())

    def test_corrupt_state_exits_1(self):
        os.makedirs(self.state)
        with open(os.path.join(self.state, "window.json"), "w",
                  encoding="utf-8") as fh:
            fh.write("not json at all")
        result = self.run_cli("--state", self.state, "stats")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.strip())


if __name__ == "__main__":
    unittest.main()
