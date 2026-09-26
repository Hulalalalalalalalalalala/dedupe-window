import json
import os
import subprocess
import sys
import tempfile
import unittest

from dedupe_window import Window


class WindowBehaviourTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.window = Window(self.dir, 10, 3)

    def test_new_key_is_admitted_once(self):
        self.assertTrue(self.window.observe("a"))
        self.assertFalse(self.window.observe("a"))
        self.assertTrue(self.window.seen("a"))
        self.assertFalse(self.window.seen("b"))

    def test_empty_string_is_a_key(self):
        self.assertTrue(self.window.observe(""))
        self.assertFalse(self.window.observe(""))

    def test_duplicate_does_not_refresh_or_reorder(self):
        self.window.observe("a")
        self.window.advance(5)
        self.window.observe("b")
        self.assertFalse(self.window.observe("a"))
        self.assertEqual(self.window.keys(), ["a", "b"])
        # "a" still expires by its first sighting, not the repeat.
        self.assertEqual(self.window.advance(11), 1)
        self.assertEqual(self.window.keys(), ["b"])

    def test_capacity_evicts_oldest(self):
        for key in "abc":
            self.window.observe(key)
        self.assertTrue(self.window.observe("d"))
        self.assertEqual(self.window.keys(), ["b", "c", "d"])
        self.assertFalse(self.window.seen("a"))

    def test_capacity_eviction_is_not_expiry(self):
        for key in "abc":
            self.window.observe(key)
        self.window.observe("d")
        stats = self.window.stats()
        self.assertEqual(stats["admitted"], 4)
        self.assertEqual(stats["expired"], 0)
        self.assertEqual(stats["retained"], 3)

    def test_span_eviction_boundary(self):
        self.window.observe("a")          # first seen at 0
        self.window.advance(4)
        self.window.observe("b")          # first seen at 4
        # now - first == span keeps the key.
        self.assertEqual(self.window.advance(10), 0)
        self.assertEqual(self.window.keys(), ["a", "b"])
        self.assertEqual(self.window.advance(11), 1)
        self.assertEqual(self.window.keys(), ["b"])
        self.assertEqual(self.window.advance(15), 1)
        self.assertEqual(self.window.keys(), [])

    def test_advance_counts_and_expired(self):
        self.window.observe("a")
        self.window.observe("b")
        self.assertEqual(self.window.advance(11), 2)
        self.assertEqual(self.window.stats()["expired"], 2)
        self.assertEqual(self.window.advance(20), 0)

    def test_advance_rejects_earlier_now(self):
        self.window.advance(5)
        with self.assertRaises(ValueError):
            self.window.advance(4)
        self.assertEqual(self.window.advance(5), 0)

    def test_stats_fields_and_order(self):
        self.assertEqual(
            list(self.window.stats()),
            ["span", "capacity", "retained", "admitted", "expired"],
        )
        self.assertEqual(
            self.window.stats(),
            {"span": 10, "capacity": 3, "retained": 0, "admitted": 0, "expired": 0},
        )

    def test_constructor_validation(self):
        with self.assertRaises(ValueError):
            Window(self.dir, 0, 3)
        with self.assertRaises(ValueError):
            Window(self.dir, 10, -1)
        with self.assertRaises(TypeError):
            Window(self.dir, "10", 3)
        with self.assertRaises(TypeError):
            Window(self.dir, 10, True)
        with self.assertRaises(TypeError):
            Window(self.dir, True, 3)

    def test_key_type_validation(self):
        with self.assertRaises(TypeError):
            self.window.observe(1)
        with self.assertRaises(TypeError):
            self.window.seen(None)

    def test_advance_type_validation(self):
        with self.assertRaises(TypeError):
            self.window.advance("5")
        with self.assertRaises(TypeError):
            self.window.advance(True)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_roundtrip(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        window.observe("b")
        window.advance(4)
        window.observe("c")
        window.save()

        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b", "c"])
        self.assertEqual(clone.stats(), window.stats())
        self.assertEqual(clone.advance(11), 2)  # "a" and "b" first seen at 0
        self.assertEqual(clone.keys(), ["c"])
        self.assertEqual(clone.advance(15), 1)  # "c" first seen at 4
        self.assertEqual(clone.keys(), [])

    def test_load_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            Window(self.dir, 10, 3).load()

    def _write(self, payload):
        path = os.path.join(self.dir, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(payload)

    def test_load_invalid_json(self):
        self._write("{not json")
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_load_missing_field(self):
        self._write(json.dumps({"span": 10, "capacity": 3}))
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_load_settings_mismatch(self):
        Window(self.dir, 10, 3).save()
        with self.assertRaises(ValueError):
            Window(self.dir, 99, 3).load()
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 4).load()

    def test_load_inconsistent_state(self):
        Window(self.dir, 10, 3).save()
        path = os.path.join(self.dir, "window.json")
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        data["keys"] = [["a", 0], ["a", 1]]  # duplicate key
        self._write(json.dumps(data))
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_failed_load_leaves_memory_untouched(self):
        window = Window(self.dir, 10, 3)
        window.observe("keep")
        self._write("{broken")
        with self.assertRaises(ValueError):
            window.load()
        self.assertEqual(window.keys(), ["keep"])


class CliTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", *args],
            capture_output=True,
            text=True,
        )

    def test_observe_seen_stats_flow(self):
        state = os.path.join(self.dir, "window")
        first = self.run_cli("--state", state, "observe", "a")
        self.assertEqual((first.returncode, first.stdout), (0, "true\n"))
        again = self.run_cli("--state", state, "observe", "a")
        self.assertEqual((again.returncode, again.stdout), (0, "false\n"))
        seen = self.run_cli("--state", state, "seen", "a")
        self.assertEqual((seen.returncode, seen.stdout), (0, "true\n"))
        stats = self.run_cli("--state", state, "stats")
        self.assertEqual(stats.returncode, 0)
        self.assertEqual(
            stats.stdout,
            '{"span":60,"capacity":1024,"retained":1,"admitted":1,"expired":0}\n',
        )

    def test_seen_and_stats_do_not_write(self):
        missing = os.path.join(self.dir, "window")
        self.run_cli("--state", missing, "seen", "a")
        self.run_cli("--state", missing, "stats")
        self.assertFalse(os.path.exists(os.path.join(missing, "window.json")))

    def test_usage_errors_exit_2(self):
        for args in (
            [],
            ["observe", "a"],
            ["--state", self.dir],
            ["--state", self.dir, "bogus"],
            ["--state", self.dir, "observe"],
            ["--state", self.dir, "observe", "a", "b"],
            ["--state", self.dir, "stats", "extra"],
        ):
            result = self.run_cli(*args)
            self.assertEqual(result.returncode, 2, args)
            self.assertIn("usage", result.stderr.lower())

    def test_corrupt_state_exits_1(self):
        state = os.path.join(self.dir, "window")
        os.makedirs(state)
        with open(os.path.join(state, "window.json"), "w", encoding="utf-8") as fh:
            fh.write("{broken")
        result = self.run_cli("--state", state, "stats")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
