"""Unit tests for the Window core semantics."""

import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from dedupe_window import Window


class WindowBehaviorTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state = self._tmp.name

    def make_window(self, span=60.0, capacity=100):
        return Window(self.state, span, capacity)

    def seed(self, window, entries):
        """entries: dict key -> first-seen timestamp, in given order."""
        for key, first_at in entries.items():
            window._first_seen[key] = float(first_at)

    def test_observe_admits_new_key_and_rejects_repeat_without_refresh(self):
        window = self.make_window()
        self.assertTrue(window.observe("a"))
        first = window._first_seen["a"]
        time.sleep(0.005)
        self.assertFalse(window.observe("a"))
        self.assertEqual(window._first_seen["a"], first)
        self.assertEqual(window.stats()["admitted"], 1)

    def test_seen_reports_membership_without_recording(self):
        window = self.make_window()
        self.assertFalse(window.seen("a"))
        window.observe("a")
        self.assertTrue(window.seen("a"))
        self.assertFalse(window.seen("b"))
        self.assertEqual(window.stats()["admitted"], 1)

    def test_observe_evicts_expired_keys_first(self):
        window = self.make_window(span=10.0)
        self.seed(window, {"old": -1.0, "fresh": 5.0})
        # now=10.0: cutoff is 0.0, first_at < cutoff evicts only "old".
        self.assertEqual(window.advance(10.0), 1)
        self.assertEqual(window.keys(), ["fresh"])
        self.assertEqual(window.stats()["expired"], 1)

    def test_advance_boundary_is_exclusive(self):
        window = self.make_window(span=10.0)
        self.seed(window, {"edge": 0.0})
        # Exactly at now - span the key is retained.
        self.assertEqual(window.advance(10.0), 0)
        self.assertEqual(window.keys(), ["edge"])
        # An instant later it expires.
        self.assertEqual(window.advance(10.0 + 1e-9), 1)

    def test_keys_ordered_oldest_first(self):
        window = self.make_window()
        self.seed(window, {"c": 30.0, "a": 10.0, "b": 20.0})
        self.assertEqual(window.keys(), ["a", "b", "c"])

    def test_capacity_evicts_oldest_first(self):
        now = time.time()
        window = self.make_window(span=1000.0, capacity=2)
        self.seed(window, {"a": now - 2.0, "b": now - 1.0})
        window._admitted = 2
        self.assertTrue(window.observe("c"))
        self.assertEqual(window.keys(), ["b", "c"])
        stats = window.stats()
        self.assertEqual(stats["retained"], 2)
        self.assertEqual(stats["admitted"], 3)
        self.assertEqual(stats["expired"], 1)

    def test_expired_counts_both_kinds_of_eviction(self):
        window = self.make_window(span=5.0, capacity=1)
        self.seed(window, {"old": 0.0})
        window.observe("new")  # evicts "old" by time, then fills capacity
        self.assertTrue(window.observe("newer"))  # evicts "new" by capacity
        stats = window.stats()
        self.assertEqual(stats["expired"], 2)
        self.assertEqual(stats["retained"], 1)

    def test_stats_fields(self):
        window = self.make_window(span=15.0, capacity=7)
        stats = window.stats()
        self.assertEqual(stats, {"span": 15.0, "capacity": 7, "retained": 0, "admitted": 0, "expired": 0})


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_invalid_span(self):
        for bad in (0, -1, float("inf"), float("nan")):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    Window(self._tmp.name, bad, 10)

    def test_span_wrong_type(self):
        with self.assertRaises(TypeError):
            Window(self._tmp.name, "60", 10)

    def test_invalid_capacity(self):
        for bad in (0, -1):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    Window(self._tmp.name, 60.0, bad)

    def test_capacity_wrong_type(self):
        for bad in (1.5, "10", True):
            with self.subTest(bad=bad):
                with self.assertRaises(TypeError):
                    Window(self._tmp.name, 60.0, bad)

    def test_key_must_be_text(self):
        window = Window(self._tmp.name, 60.0, 10)
        with self.assertRaises(TypeError):
            window.observe(1)
        with self.assertRaises(TypeError):
            window.seen(None)

    def test_advance_validation(self):
        window = Window(self._tmp.name, 60.0, 10)
        with self.assertRaises(TypeError):
            window.advance("now")
        for bad in (float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                window.advance(bad)

    def test_state_wrong_type(self):
        with self.assertRaises(TypeError):
            Window(123, 60.0, 10)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state = self._tmp.name
        self.path = Path(self.state) / "window.json"

    def test_save_then_load_round_trip(self):
        window = Window(self.state, 25.5, 42)
        window.observe("a")
        window.observe("b")
        window.observe("a")  # repeat, not admitted
        window.advance(time.time() + 1000)
        window.save()
        self.assertTrue(self.path.exists())

        restored = Window(self.state, 1.0, 1)
        restored.load()
        stats = restored.stats()
        self.assertEqual(stats["span"], 25.5)
        self.assertEqual(stats["capacity"], 42)
        self.assertEqual(stats["admitted"], 2)
        self.assertEqual(stats["expired"], 2)
        self.assertEqual(restored.keys(), [])

    def test_load_missing_file_raises_file_not_found(self):
        window = Window(self.state, 60.0, 10)
        with self.assertRaises(FileNotFoundError):
            window.load()

    def test_load_rejects_corrupt_or_invalid_documents(self):
        cases = {
            "not json": "not json",
            "not object": "[]",
            "missing field": json.dumps({"span": 10.0, "capacity": 2}),
            "bad span type": json.dumps(
                {"span": "10", "capacity": 2, "admitted": 0, "expired": 0, "keys": {}}
            ),
            "bad span value": json.dumps(
                {"span": 0, "capacity": 2, "admitted": 0, "expired": 0, "keys": {}}
            ),
            "bad capacity": json.dumps(
                {"span": 10.0, "capacity": 0, "admitted": 0, "expired": 0, "keys": {}}
            ),
            "keys not object": json.dumps(
                {"span": 10.0, "capacity": 2, "admitted": 0, "expired": 0, "keys": []}
            ),
            "bad timestamp": json.dumps(
                {"span": 10.0, "capacity": 2, "admitted": 0, "expired": 0, "keys": {"a": "soon"}}
            ),
            "negative admitted": json.dumps(
                {"span": 10.0, "capacity": 2, "admitted": -1, "expired": 0, "keys": {}}
            ),
        }
        for label, content in cases.items():
            with self.subTest(label=label):
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self.path.write_text(content, encoding="utf-8")
                window = Window(self.state, 60.0, 10)
                with self.assertRaises(ValueError):
                    window.load()

    def test_failed_load_leaves_memory_untouched(self):
        window = Window(self.state, 60.0, 10)
        window.observe("keep")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            window.load()
        self.assertTrue(window.seen("keep"))
        self.assertEqual(window.stats()["span"], 60.0)


if __name__ == "__main__":
    unittest.main()
