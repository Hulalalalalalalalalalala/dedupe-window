"""Acceptance tests for dedupe_window.Window."""

from __future__ import annotations

import json
import os
import time
import unittest
from unittest import mock

from dedupe_window import Window
from dedupe_window.window import STATE_FILENAME


class WindowTest(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.state = self.tmp.name
        self.addCleanup(self.tmp.cleanup)

    def window(self, span: float = 60.0, capacity: int = 10_000) -> Window:
        return Window(self.state, span, capacity)

    def path(self) -> str:
        return os.path.join(self.state, STATE_FILENAME)

    def test_first_sighting_vs_repeat_within_window(self) -> None:
        window = self.window()
        self.assertTrue(window.observe("a"))
        self.assertFalse(window.observe("a"))
        self.assertTrue(window.seen("a"))

    def test_seen_does_not_record(self) -> None:
        window = self.window()
        self.assertFalse(window.seen("ghost"))
        self.assertTrue(window.observe("ghost"))
        self.assertFalse(window.seen("other"))
        self.assertEqual(window.keys(), ["ghost"])

    def test_repeat_does_not_refresh_first_seen(self) -> None:
        window = self.window(span=10.0)
        with mock.patch("dedupe_window.window.time.time", return_value=100.0):
            self.assertTrue(window.observe("a"))
        with mock.patch("dedupe_window.window.time.time", return_value=105.0):
            self.assertFalse(window.observe("a"))
        # A refreshed key would survive to 116; at 111 it must already be gone.
        with mock.patch("dedupe_window.window.time.time", return_value=111.0):
            self.assertTrue(window.observe("a"))

    def test_observe_evicts_expired_before_deciding(self) -> None:
        window = self.window(span=10.0)
        with mock.patch("dedupe_window.window.time.time", return_value=100.0):
            self.assertTrue(window.observe("a"))
        with mock.patch("dedupe_window.window.time.time", return_value=111.0):
            self.assertTrue(window.observe("a"))  # expired, so new again

    def test_advance_drops_before_horizon_and_counts(self) -> None:
        window = self.window(span=10.0)
        with mock.patch("dedupe_window.window.time.time", return_value=100.0):
            window.observe("a")
        with mock.patch("dedupe_window.window.time.time", return_value=105.0):
            window.observe("b")
        # horizon = 110 - 10 = 100; seen at exactly 100 is retained (< is strict)
        self.assertEqual(window.advance(110), 0)
        self.assertEqual(window.advance(110.000001), 1)
        self.assertEqual(window.keys(), ["b"])
        self.assertEqual(window.advance(999), 1)
        self.assertEqual(window.keys(), [])

    def test_capacity_evicts_oldest_first(self) -> None:
        window = self.window(span=1000.0, capacity=2)
        for i, key in enumerate(("a", "b", "c")):
            with mock.patch("dedupe_window.window.time.time", return_value=100.0 + i):
                self.assertTrue(window.observe(key))
        self.assertEqual(window.keys(), ["b", "c"])

    def test_capacity_eviction_expired_counter(self) -> None:
        window = self.window(span=1000.0, capacity=1)
        with mock.patch("dedupe_window.window.time.time", return_value=100.0):
            window.observe("a")
        with mock.patch("dedupe_window.window.time.time", return_value=101.0):
            window.observe("b")
        stats = window.stats()
        self.assertEqual(stats["retained"], 1)
        self.assertEqual(stats["admitted"], 2)
        self.assertEqual(stats["expired"], 1)  # capacity eviction counts too

    def test_keys_ordered_oldest_first_with_advancing_time(self) -> None:
        window = self.window(span=1000.0)
        for i, key in enumerate(("a", "b", "c")):
            with mock.patch("dedupe_window.window.time.time", return_value=100.0 + i):
                window.observe(key)
        self.assertEqual(window.keys(), ["a", "b", "c"])

    def test_stats_shape_and_cumulative_admitted(self) -> None:
        window = self.window(span=12.0, capacity=3)
        with mock.patch("dedupe_window.window.time.time", return_value=1_000.0):
            window.observe("a")
            self.assertFalse(window.observe("a"))
        stats = window.stats()
        self.assertEqual(
            stats,
            {"span": 12.0, "capacity": 3, "retained": 1, "admitted": 1, "expired": 0},
        )

    def test_expired_counts_both_kinds(self) -> None:
        window = self.window(span=5.0, capacity=1)
        with mock.patch("dedupe_window.window.time.time", return_value=0.0):
            window.observe("a")
        with mock.patch("dedupe_window.window.time.time", return_value=1.0):
            window.observe("b")  # capacity eviction -> expired 1
        with mock.patch("dedupe_window.window.time.time", return_value=10.0):
            window.observe("c")  # b time-expired -> expired 2
        self.assertEqual(window.stats()["expired"], 2)

    # --- persistence -----------------------------------------------------

    def test_save_creates_state_directory_and_file(self) -> None:
        state = os.path.join(self.state, "nested", "window-dir")
        window = Window(state, 30.0, 7)
        with mock.patch("dedupe_window.window.time.time", return_value=42.0):
            window.observe("k")
        window.save()
        self.assertTrue(os.path.exists(os.path.join(state, STATE_FILENAME)))

    def test_roundtrip_preserves_observe_seen_keys_stats(self) -> None:
        window = self.window(span=33.5, capacity=4)
        with mock.patch("dedupe_window.window.time.time", return_value=1000.0):
            window.observe("x")
            window.observe("y")
            window.observe("x")
        window.save()

        restored = self.window()  # constructor args are replaced by load()
        restored.load()
        self.assertTrue(restored.seen("x"))
        self.assertTrue(restored.seen("y"))
        self.assertFalse(restored.seen("z"))
        with mock.patch("dedupe_window.window.time.time", return_value=1001.0):
            self.assertFalse(restored.observe("y"))
        self.assertEqual(restored.keys(), ["x", "y"])
        self.assertEqual(
            restored.stats(),
            {"span": 33.5, "capacity": 4, "retained": 2, "admitted": 2, "expired": 0},
        )

    def test_roundtrip_after_eviction(self) -> None:
        window = self.window(span=5.0, capacity=1)
        with mock.patch("dedupe_window.window.time.time", return_value=0.0):
            window.observe("a")
        with mock.patch("dedupe_window.window.time.time", return_value=1.0):
            window.observe("b")
        window.save()
        restored = self.window()
        restored.load()
        self.assertEqual(restored.keys(), ["b"])
        self.assertEqual(restored.stats()["expired"], 1)

    def test_load_missing_file_raises_filenotfound(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.window().load()

    def _write_state(self, payload: object) -> None:
        os.makedirs(self.state, exist_ok=True)
        with open(self.path(), "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def test_load_corrupt_json_raises_value_error(self) -> None:
        os.makedirs(self.state, exist_ok=True)
        with open(self.path(), "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with self.assertRaises(ValueError):
            self.window().load()

    def test_load_missing_fields_raises_value_error(self) -> None:
        self._write_state({"span": 60.0, "capacity": 10})
        with self.assertRaises(ValueError):
            self.window().load()

    def test_load_wrong_types_raises_value_error(self) -> None:
        good = {
            "span": 60.0,
            "capacity": 10,
            "admitted": 0,
            "expired": 0,
            "keys": {"a": 1.0},
        }
        for mutated in (
            {**good, "span": "60"},
            {**good, "capacity": None},
            {**good, "admitted": 1.5},
            {**good, "expired": -1},
            {**good, "keys": [("a", 1.0)]},
            {**good, "keys": {"a": "soon"}},
        ):
            with self.subTest(mutated=mutated):
                self._write_state(mutated)
                with self.assertRaises(ValueError):
                    self.window().load()

    def test_load_invalid_values_raises_value_error(self) -> None:
        good = {
            "span": 60.0,
            "capacity": 10,
            "admitted": 1,
            "expired": 0,
            "keys": {"a": 1.0},
        }
        for mutated in (
            {**good, "span": 0},
            {**good, "capacity": 0},
            {**good, "capacity": 0, "keys": {}},
            {**good, "keys": {"a": float("nan")}},
            {**good, "capacity": 0, "keys": {"a": 1.0, "b": 2.0}},
        ):
            with self.subTest(mutated=mutated):
                self._write_state(mutated)
                with self.assertRaises(ValueError):
                    self.window().load()

    def test_failed_load_keeps_memory_state(self) -> None:
        window = self.window()
        with mock.patch("dedupe_window.window.time.time", return_value=100.0):
            window.observe("keep")
        self._write_state({"span": 60})  # invalid -> load must not mutate memory
        with self.assertRaises(ValueError):
            window.load()
        self.assertTrue(window.seen("keep"))
        self.assertEqual(window.stats()["admitted"], 1)

    # --- validation ------------------------------------------------------

    def test_invalid_constructor_arguments(self) -> None:
        with self.assertRaises(TypeError):
            Window(123, 60, 10)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            Window(self.state, "60", 10)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            Window(self.state, 60, 2.5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            Window(self.state, 0, 10)
        with self.assertRaises(ValueError):
            Window(self.state, float("inf"), 10)
        with self.assertRaises(ValueError):
            Window(self.state, 60, 0)

    def test_non_text_key_is_type_error(self) -> None:
        window = self.window()
        with self.assertRaises(TypeError):
            window.observe(42)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            window.seen(None)  # type: ignore[arg-type]

    def test_advance_validates_now(self) -> None:
        window = self.window()
        with self.assertRaises(TypeError):
            window.advance("now")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            window.advance(float("nan"))


if __name__ == "__main__":
    unittest.main()
