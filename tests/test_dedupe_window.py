import hashlib
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
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

    def _corrupt_file(self, state, content):
        os.makedirs(state, exist_ok=True)
        with open(os.path.join(state, "window.json"), "w", encoding="utf-8") as fh:
            fh.write(content)

    def test_cli_distinguishes_corruption_reasons(self):
        cases = [
            ("{broken", "not valid JSON"),
            (json.dumps({"version": 1, "span": 60, "capacity": 1024,
                         "now": 0, "admitted": 0, "expired": 0, "keys": []}),
             "missing integrity information"),
            (json.dumps({"version": 1, "span": 60, "capacity": 1024, "now": 0,
                         "admitted": 0, "expired": 0, "keys": [],
                         "checksum": "0" * 64}),
             "checksum"),
        ]
        for content, fragment in cases:
            state = os.path.join(self.dir, "w_" + str(len(fragment)))
            self._corrupt_file(state, content)
            result = self.run_cli("--state", state, "stats")
            self.assertEqual(result.returncode, 1, content)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1)
            self.assertIn(fragment, result.stderr)

    def test_cli_missing_file_for_observe_starts_empty(self):
        state = os.path.join(self.dir, "fresh")
        result = self.run_cli("--state", state, "observe", "a")
        self.assertEqual((result.returncode, result.stdout), (0, "true\n"))
        again = self.run_cli("--state", state, "observe", "a")
        self.assertEqual((again.returncode, again.stdout), (0, "false\n"))


class AtomicCommitTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _document(self):
        with open(os.path.join(self.dir, "window.json"), encoding="utf-8") as fh:
            return json.load(fh)

    def test_only_data_file_remains_after_saves(self):
        window = Window(self.dir, 10, 3)
        for key in "abcdef":
            window.observe(key)
        self.assertEqual(sorted(os.listdir(self.dir)), ["window.json"])
        window.save()
        window.save()
        self.assertEqual(sorted(os.listdir(self.dir)), ["window.json"])

    def test_document_carries_checksum_and_version(self):
        Window(self.dir, 10, 3).save()
        doc = self._document()
        self.assertIn("checksum", doc)
        self.assertEqual(doc["version"], 2)
        self.assertEqual(doc["kind"], "base")
        self.assertEqual(len(doc["checksum"]), 64)

    def test_missing_integrity_field_is_value_error(self):
        path = os.path.join(self.dir, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"version": 1, "span": 10, "capacity": 3, "now": 0,
                       "admitted": 0, "expired": 0, "keys": []}, fh)
        with self.assertRaises(ValueError) as cm:
            Window(self.dir, 10, 3).load()
        self.assertIn("missing integrity information", str(cm.exception))

    def test_checksum_mismatch_is_value_error(self):
        Window(self.dir, 10, 3).save()
        path = os.path.join(self.dir, "window.json")
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
        doc["admitted"] = 1  # alter payload without recomputing the checksum
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        with self.assertRaises(ValueError) as cm:
            Window(self.dir, 10, 3).load()
        self.assertIn("checksum does not match", str(cm.exception))

    def test_non_ascii_checksum_does_not_raise_type_error(self):
        with open(os.path.join(self.dir, "window.json"), "w", encoding="utf-8") as fh:
            fh.write('{"version":1,"span":10,"capacity":3,"now":0,'
                     '"admitted":0,"expired":0,"keys":[],"checksum":"é"}')
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_roundtrip_preserves_order_counts_and_now(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        window.advance(4)
        window.observe("b")
        window.observe("c")
        window.save()
        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b", "c"])
        self.assertEqual(clone.stats(), window.stats())

    def test_stale_temp_file_from_dead_writer_is_swept(self):
        window = Window(self.dir, 10, 3)
        with open(os.path.join(self.dir, "window.json.deadbeef.tmp"), "w") as fh:
            fh.write("partial")
        window.observe("a")
        self.assertEqual(sorted(os.listdir(self.dir)), ["window.json"])


class SegmentedStateTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "window.json")

    def _raw(self):
        with open(self.path, "rb") as fh:
            return fh.read()

    def _lines(self):
        return self._raw().decode("utf-8").splitlines()

    def _write_v1(self, keys, now=0, admitted=0, expired=0, span=10, capacity=3):
        body = {"version": 1, "span": span, "capacity": capacity, "now": now,
                "admitted": admitted, "expired": expired, "keys": keys}
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
        envelope = dict(body)
        envelope["checksum"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n")

    def test_commits_append_segments_not_full_mirrors(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        window.observe("b")
        lines = self._lines()
        self.assertEqual(len(lines), 2)
        base = json.loads(lines[0])
        self.assertEqual((base["version"], base["kind"]), (2, "base"))
        segment = json.loads(lines[1])
        self.assertEqual((segment["kind"], segment["op"]), ("delta", "admit"))
        self.assertEqual(segment["add"], [["b", 0]])
        self.assertEqual(segment["prev"], base["checksum"])

    def test_segments_chain_and_replay_exactly(self):
        window = Window(self.dir, 10, 3)
        for key in "abcd":  # capacity 3: "a" is evicted by "d"
            window.observe(key)
        window.advance(4)
        window.observe("e")
        window.advance(12)
        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), window.keys())
        self.assertEqual(clone.stats(), window.stats())

    def test_write_volume_stays_proportional_to_the_change(self):
        window = Window(self.dir, 10**9, 100000)
        for i in range(2000):
            window.observe(f"key-{i}")
        size = os.path.getsize(self.path)
        self.assertGreater(size, 20000)  # a real state, not a toy
        for i in range(2000, 2010):
            window.observe(f"key-{i}")
        growth = os.path.getsize(self.path) - size
        # Ten commits grew the file by a few small segments (or compacted,
        # which shrinks it) -- never by another full mirror of the state.
        self.assertLess(growth, size // 10)

    def test_compaction_keeps_single_file_and_state(self):
        window = Window(self.dir, 60, 1000)
        for i in range(300):
            window.observe(f"k{i}")
            window.advance(i)  # exercises sweep segments and expiries
        self.assertEqual(sorted(os.listdir(self.dir)), ["window.json"])
        clone = Window(self.dir, 60, 1000)
        clone.load()
        self.assertEqual(clone.keys(), window.keys())
        self.assertEqual(clone.stats(), window.stats())

    def test_v1_document_is_upgraded_on_load(self):
        self._write_v1([["a", 0], ["b", 0]], admitted=2)
        window = Window(self.dir, 10, 3)
        window.load()
        self.assertEqual(window.keys(), ["a", "b"])
        self.assertEqual(window.stats()["admitted"], 2)

    def test_v1_document_is_rewritten_as_v2_on_next_commit(self):
        self._write_v1([["a", 0]], admitted=1)
        window = Window(self.dir, 10, 3)
        window.load()
        window.observe("b")
        lines = self._lines()
        self.assertEqual(json.loads(lines[0])["version"], 2)
        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b"])
        self.assertEqual(clone.stats()["admitted"], 2)

    def test_v1_document_is_rewritten_as_v2_on_save(self):
        self._write_v1([["a", 0]], admitted=1)
        window = Window(self.dir, 10, 3)
        window.load()
        window.save()
        self.assertEqual(json.loads(self._lines()[0])["version"], 2)

    def test_missing_version_is_value_error(self):
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump({"span": 10, "capacity": 3, "now": 0,
                       "admitted": 0, "expired": 0, "keys": []}, fh)
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_unknown_version_is_value_error(self):
        self._write_v1([])
        with open(self.path, encoding="utf-8") as fh:
            doc = json.load(fh)
        doc["version"] = 99
        canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"))
        doc["checksum"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_torn_tail_is_ignored_by_readers(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        window.observe("b")
        with open(self.path, "ab") as fh:
            fh.write(b'{"kind":"delta","op":"admi')  # interrupted append
        clone = Window(self.dir, 10, 3)
        clone.load()  # read-only: restores the last committed state
        self.assertEqual(clone.keys(), ["a", "b"])
        self.assertEqual(clone.stats()["admitted"], 2)

    def test_torn_tail_is_repaired_by_the_next_writer(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        with open(self.path, "ab") as fh:
            fh.write(b'{"kind":"delta","op":"admi')
        window.observe("b")  # must not raise; truncates the torn bytes
        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b"])
        self.assertEqual(sorted(os.listdir(self.dir)), ["window.json"])

    def test_truncated_base_is_value_error(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        raw = self._raw()
        with open(self.path, "wb") as fh:
            fh.write(raw[: len(raw) // 2])
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()

    def test_corrupt_middle_segment_is_value_error(self):
        window = Window(self.dir, 10, 3)
        for key in "abc":
            window.observe(key)
        lines = self._lines()
        line = lines[1]
        lines[1] = line[:10] + ("X" if line[10] != "X" else "Y") + line[11:]
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        with self.assertRaises(ValueError):
            Window(self.dir, 10, 3).load()


class CommitPointTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _base(self, window):
        with open(os.path.join(self.dir, "window.json"), "rb") as fh:
            first = fh.read().split(b"\n", 1)[0]
        return json.loads(first)

    def test_points_start_at_one_and_grow_per_mutation(self):
        window = Window(self.dir, 10, 3)
        self.assertEqual(window.observe("a"), True)
        self.assertEqual(window._seq, 1)
        self.assertEqual(self._base(window)["seq"], 1)
        window.observe("b")
        window.advance(4)
        window.observe("c")
        self.assertEqual(window._seq, 4)

    def test_advance_without_change_is_not_a_commit(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")          # point 1
        window.advance(0)            # no movement, no commit
        window.observe("a")          # duplicate, no commit
        self.assertEqual(window._seq, 1)

    def test_compaction_adds_no_point(self):
        window = Window(self.dir, 60, 1000)
        for i in range(300):
            window.observe(f"k{i}")
            window.advance(i)
        seq = window._seq
        base = self._base(window)
        # The compacted base carries a real point number but adds none: the
        # tip is unchanged and a replayed clone reaches the same point.
        self.assertGreaterEqual(base["seq"], 1)
        clone = Window(self.dir, 60, 1000)
        clone.load()
        self.assertEqual(clone._seq, seq)


class ExportRestoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.state = os.path.join(self.dir, "window")
        self.out = os.path.join(self.dir, "exports")
        os.makedirs(self.state)
        self.window = Window(self.state, 10, 3)
        self.window.observe("a")
        self.window.observe("b")
        self.window.advance(5)
        self.window.observe("c")
        # points: a=1, b=2, advance=3, c=4

    def _read_export(self, name):
        with open(os.path.join(self.out, name), "rb") as fh:
            return fh.read()

    def test_export_latest_returns_point_and_self_contained_doc(self):
        os.makedirs(self.out, exist_ok=True)
        point = self.window.export(os.path.join(self.out, "latest.json"))
        self.assertEqual(point, 4)
        raw = self._read_export("latest.json")
        self.assertTrue(raw.endswith(b"\n"))
        doc = json.loads(raw)
        self.assertEqual(doc["version"], 1)
        self.assertEqual(doc["kind"], "export")
        self.assertEqual(doc["span"], 10)
        self.assertEqual(doc["capacity"], 3)
        self.assertEqual(doc["now"], 5)
        self.assertEqual(doc["keys"], [["a", 0], ["b", 0], ["c", 5]])
        self.assertEqual(len(doc["checksum"]), 64)
        # Compact, sorted-key, no superfluous whitespace.
        self.assertEqual(
            raw,
            json.dumps(doc, sort_keys=True, separators=(",", ":")) .encode() + b"\n",
        )

    def test_export_specific_point(self):
        os.makedirs(self.out, exist_ok=True)
        point = self.window.export(os.path.join(self.out, "p2.json"), 2)
        self.assertEqual(point, 2)
        doc = json.loads(self._read_export("p2.json"))
        self.assertEqual(doc["keys"], [["a", 0], ["b", 0]])
        self.assertEqual(doc["now"], 0)

    def test_export_writes_to_missing_parent_dirs_is_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.window.export(os.path.join(self.dir, "no", "such", "dir", "x.json"))

    def test_export_without_any_commit_is_value_error(self):
        fresh = os.path.join(self.dir, "fresh")
        os.makedirs(fresh)
        window = Window(fresh, 10, 3)
        target = os.path.join(self.out, "x.json")
        os.makedirs(self.out, exist_ok=True)
        with self.assertRaises(ValueError):
            window.export(target)

    def test_export_missing_state_file_is_value_error(self):
        ghost = os.path.join(self.dir, "ghost")
        os.makedirs(ghost)
        window = Window(ghost, 10, 3)
        with self.assertRaises(ValueError):
            window.export(os.path.join(self.out, "x.json"))

    def test_point_type_must_be_int(self):
        target = os.path.join(self.out, "x.json")
        os.makedirs(self.out, exist_ok=True)
        for bad in ("1", 1.0, True, [1]):
            with self.assertRaises(TypeError):
                self.window.export(target, bad)

    def test_point_out_of_range_is_value_error(self):
        target = os.path.join(self.out, "x.json")
        os.makedirs(self.out, exist_ok=True)
        for bad in (0, -1, 5, 1000):
            with self.assertRaises(ValueError):
                self.window.export(target, bad)

    def test_point_compacted_away_is_value_error(self):
        import dedupe_window.window as mod
        window = Window(os.path.join(self.dir, "compact"), 1000, 1000)
        for i in range(20):
            window.observe(f"k{i}")
        old_segments, old_min = mod._MAX_SEGMENTS, mod._MIN_BASE_BYTES
        mod._MAX_SEGMENTS = 3
        mod._MIN_BASE_BYTES = 1
        try:
            for i in range(20, 40):
                window.observe(f"k{i}")
            target = os.path.join(self.out, "old.json")
            with self.assertRaises(ValueError):
                window.export(target, 1)
        finally:
            mod._MAX_SEGMENTS, mod._MIN_BASE_BYTES = old_segments, old_min

    def test_restore_installs_keys_now_counts_as_new_commit(self):
        os.makedirs(self.out, exist_ok=True)
        self.window.export(os.path.join(self.out, "p2.json"), 2)
        self.window.advance(11)   # a,b expire; point 5
        self.assertEqual(self.window.keys(), ["c"])
        new_point = self.window.restore(os.path.join(self.out, "p2.json"))
        self.assertEqual(new_point, 6)
        self.assertEqual(self.window.keys(), ["a", "b"])
        stats = self.window.stats()
        self.assertEqual(stats["admitted"], 2)
        self.assertEqual(stats["expired"], 0)
        self.assertEqual(stats["retained"], 2)

    def test_restore_persists_and_reloads(self):
        os.makedirs(self.out, exist_ok=True)
        self.window.export(os.path.join(self.out, "p3.json"), 3)
        point = self.window.restore(os.path.join(self.out, "p3.json"))
        clone = Window(self.state, 10, 3)
        clone.load()
        self.assertEqual(clone._seq, point)
        self.assertEqual(clone.keys(), ["a", "b"])

    def test_reexport_same_point_is_byte_identical(self):
        os.makedirs(self.out, exist_ok=True)
        first = os.path.join(self.out, "first.json")
        self.window.export(first, 2)
        point = self.window.restore(first)
        second = os.path.join(self.out, "second.json")
        again = self.window.export(second)
        self.assertEqual(again, point)
        with open(first, "rb") as a, open(second, "rb") as b:
            self.assertEqual(a.read(), b.read())

    def test_restore_into_empty_state_directory(self):
        os.makedirs(self.out, exist_ok=True)
        doc = os.path.join(self.out, "p2.json")
        self.window.export(doc, 2)
        fresh = os.path.join(self.dir, "brand-new")
        os.makedirs(fresh)
        window = Window(fresh, 10, 3)
        point = window.restore(doc)
        self.assertEqual(point, 1)
        self.assertEqual(window.keys(), ["a", "b"])
        self.assertTrue(os.path.exists(os.path.join(fresh, "window.json")))

    def test_restore_missing_file_is_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.window.restore(os.path.join(self.out, "absent.json"))

    def test_restore_settings_mismatch_is_value_error(self):
        os.makedirs(self.out, exist_ok=True)
        doc = os.path.join(self.out, "p2.json")
        self.window.export(doc, 2)
        other = Window(os.path.join(self.dir, "other"), 99, 3)
        with self.assertRaises(ValueError):
            other.restore(doc)

    def _tamper(self, name, mutate):
        os.makedirs(self.out, exist_ok=True)
        doc = os.path.join(self.out, "orig.json")
        self.window.export(doc, 2)
        with open(doc, "rb") as fh:
            payload = json.loads(fh.read())
        mutate(payload)
        target = os.path.join(self.out, name)
        with open(target, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return target

    def test_restore_missing_version_is_value_error(self):
        def mutate(doc):
            del doc["version"]
        target = self._tamper("noversion.json", mutate)
        with self.assertRaises(ValueError):
            self.window.restore(target)

    def test_restore_unknown_version_is_value_error(self):
        def mutate(doc):
            doc["version"] = 99
            doc["checksum"] = "0" * 64
        target = self._tamper("unknown.json", mutate)
        with self.assertRaises(ValueError):
            self.window.restore(target)

    def test_restore_bad_checksum_is_value_error(self):
        def mutate(doc):
            doc["admitted"] = 42
        target = self._tamper("bad.json", mutate)
        with self.assertRaises(ValueError):
            self.window.restore(target)

    def test_failed_restore_leaves_memory_and_file_untouched(self):
        os.makedirs(self.out, exist_ok=True)
        good = os.path.join(self.out, "good.json")
        self.window.export(good, 2)
        bad = self._tamper("bad2.json", lambda d: d.update(now=9))
        before_keys = self.window.keys()
        with self.assertRaises(ValueError):
            self.window.restore(bad)
        self.assertEqual(self.window.keys(), before_keys)
        clone = Window(self.state, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), before_keys)


class ReaderIsolationTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_opening_window_does_not_create_directory(self):
        state = os.path.join(self.dir, "never-created")
        Window(state, 10, 3)
        self.assertFalse(os.path.exists(state))

    def test_seen_stats_keys_do_not_create_or_write(self):
        state = os.path.join(self.dir, "window")
        os.makedirs(state)
        window = Window(state, 10, 3)
        window.seen("a")
        window.stats()
        window.keys()
        self.assertEqual(os.listdir(state), [])

    def test_load_does_not_block_on_exclusive_lock(self):
        # A writer holding the exclusive lock must not stall a lockless load.
        state = os.path.join(self.dir, "window")
        window = Window(state, 10, 3)
        window.observe("a")
        script = textwrap.dedent(f"""
            import sys, time
            sys.path.insert(0, {_REPO_ROOT!r})
            from dedupe_window import Window
            w = Window({state!r}, 10, 3)
            with w._locked(True):
                time.sleep(1.5)
        """).strip()
        env = dict(os.environ, PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        holder = subprocess.Popen([sys.executable, "-c", script], env=env)
        time.sleep(0.4)
        try:
            reader = Window(state, 10, 3)
            started = time.monotonic()
            reader.load()  # must return promptly, not wait ~1.1s
            self.assertLess(time.monotonic() - started, 0.8)
            self.assertEqual(reader.keys(), ["a"])
        finally:
            holder.communicate()


class ExportInterleaveTests(unittest.TestCase):
    """Exports racing with appending and compacting writers.

    Every observed export must be internally consistent and correspond to
    exactly one commit point, as if readers and writers ran one at a time.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.state = os.path.join(self.dir, "window")
        os.makedirs(self.state)

    def _env(self):
        return dict(
            os.environ,
            PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""),
        )

    def _script(self, body):
        return textwrap.dedent(f"""
            import sys, time, json, os, tempfile
            sys.path.insert(0, {_REPO_ROOT!r})
            import dedupe_window.window as M
            from dedupe_window import Window
            state, out = sys.argv[1], sys.argv[2]
        """).strip() + "\n" + textwrap.dedent(body).strip() + "\n"

    def test_exports_consistent_under_appending_and_compacting_writers(self):
        # Aggressive thresholds so the writer compacts repeatedly.
        writer = self._script("""
            M._MAX_SEGMENTS = 4
            M._MIN_BASE_BYTES = 1
            w = Window(state, 100000.0, 40)
            for i in range(300):
                w.observe(f"k{i}")
                if i % 3 == 0:
                    w.advance(i)
        """)
        reader = self._script("""
            w = Window(state, 100000.0, 40)
            seen_points = set()
            end = time.time() + 15
            n = 0
            while time.time() < end:
                target = os.path.join(out, f"r-{os.getpid()}-{n}.json")
                try:
                    point = w.export(target)
                except FileNotFoundError:
                    continue
                except ValueError:
                    # A point compacted away between reads is a legal serial
                    # outcome; it is not a torn or mixed document.
                    continue
                doc = json.load(open(target))
                keys = doc["keys"]
                firsts = [f for _, f in keys]
                assert len(keys) <= 40, "capacity violated"
                assert len({k for k, _ in keys}) == len(keys), "duplicate key"
                assert firsts == sorted(firsts), "keys out of order"
                assert 1 <= point, "point not numbered from one"
                seen_points.add(point)
                n += 1
                if point >= 399:
                    break
            print(f"{n} {max(seen_points) if seen_points else 0}")
        """)
        out = tempfile.mkdtemp(dir=self.dir)
        env = self._env()
        writer_proc = subprocess.Popen(
            [sys.executable, "-c", writer, self.state, out], env=env
        )
        readers = [
            subprocess.Popen(
                [sys.executable, "-c", reader, self.state, out],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for _ in range(3)
        ]
        self.assertEqual(writer_proc.wait(), 0)
        for proc in readers:
            stdout, stderr = proc.communicate()
            self.assertEqual(proc.returncode, 0, stderr.decode())
            count, top = map(int, stdout.decode().split())
            self.assertGreater(count, 0)
            self.assertEqual(top, 399)  # writer's final point: 300 admits + 99 sweeps


class CliExportRestoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.state = os.path.join(self.dir, "window")

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", *args],
            capture_output=True,
            text=True,
        )

    def _observe(self, key):
        result = self.run_cli("--state", self.state, "observe", key)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_export_and_restore_print_points_and_round_trip(self):
        self._observe("a")
        self._observe("b")
        first = os.path.join(self.dir, "p2.json")
        exported = self.run_cli("--state", self.state, "export", first)
        self.assertEqual((exported.returncode, exported.stdout), (0, "2\n"))
        self._observe("c")
        second_state = os.path.join(self.dir, "window2")
        restored = self.run_cli("--state", second_state, "restore", first)
        self.assertEqual((restored.returncode, restored.stdout), (0, "1\n"))
        again = os.path.join(self.dir, "p2-again.json")
        reexport = self.run_cli("--state", second_state, "export", again)
        self.assertEqual((reexport.returncode, reexport.stdout), (0, "1\n"))
        with open(first, "rb") as a, open(again, "rb") as b:
            self.assertEqual(a.read(), b.read())

    def test_export_explicit_point(self):
        self._observe("a")
        self._observe("b")
        target = os.path.join(self.dir, "p1.json")
        result = self.run_cli("--state", self.state, "export", "1", target)
        self.assertEqual((result.returncode, result.stdout), (0, "1\n"))
        with open(target, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["keys"], [["a", 0]])

    def test_export_bad_point_or_arity_exits_2(self):
        self._observe("a")
        target = os.path.join(self.dir, "x.json")
        for args in (
            ["export"],
            ["export", "1", target, "extra"],
            ["export", "0", target],
            ["export", "-2", target],
            ["export", "abc", target],
            ["export", "1.5", target],
            ["restore"],
            ["restore", target, "extra"],
        ):
            result = self.run_cli("--state", self.state, *args)
            self.assertEqual(result.returncode, 2, args)
            self.assertIn("usage", result.stderr.lower())

    def test_state_errors_exit_1_with_one_reason_line(self):
        self._observe("a")
        target = os.path.join(self.dir, "x.json")
        # Point past the latest commit.
        result = self.run_cli("--state", self.state, "export", "99", target)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        # Missing parent directory of the export target.
        result = self.run_cli(
            "--state", self.state,
            "export", os.path.join(self.dir, "no-dir", "x.json"),
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        # Missing export document for restore.
        result = self.run_cli(
            "--state", self.state, "restore", os.path.join(self.dir, "missing.json")
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        # Export from a state directory that has no state file.
        empty = os.path.join(self.dir, "empty")
        os.makedirs(empty)
        result = self.run_cli("--state", empty, "export", target)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)

    def test_restore_rejects_tampered_document_exit_1(self):
        self._observe("a")
        target = os.path.join(self.dir, "good.json")
        self.run_cli("--state", self.state, "export", target)
        with open(target, encoding="utf-8") as fh:
            doc = json.load(fh)
        doc["admitted"] = 7
        bad = os.path.join(self.dir, "bad.json")
        with open(bad, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        result = self.run_cli("--state", self.state, "restore", bad)
        self.assertEqual(result.returncode, 1)
        self.assertIn("checksum", result.stderr)


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _worker(script):
    return textwrap.dedent(script).strip()


class MultiProcessTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _run_workers(self, script, count, *args):
        env = dict(os.environ, PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(i), self.dir, *map(str, args)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for i in range(count)
        ]
        results = [p.communicate() for p in procs]
        return procs, results

    def test_concurrent_observers_match_serial_counts(self):
        script = _worker("""
            import sys
            from dedupe_window import Window
            idx, state, n = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
            w = Window(state, 100000, 10000)
            for i in range(idx * n, idx * n + n):
                w.observe(f"k{i}")
        """)
        n_workers, per_worker = 8, 25
        procs, _ = self._run_workers(script, n_workers, per_worker)
        self.assertEqual([p.returncode for p in procs], [0] * n_workers)
        window = Window(self.dir, 100000, 10000)
        window.load()
        stats = window.stats()
        self.assertEqual(stats["admitted"], n_workers * per_worker)
        self.assertEqual(stats["retained"], n_workers * per_worker)
        self.assertEqual(stats["expired"], 0)
        self.assertEqual(len(window.keys()), len(set(window.keys())))
        self.assertEqual(sorted(os.listdir(self.dir)), ["window.json"])

    def test_duplicate_across_processes_is_admitted_once(self):
        script = _worker("""
            import sys
            from dedupe_window import Window
            _, state = sys.argv[1], sys.argv[2]
            w = Window(state, 100000, 100)
            for _ in range(4):
                w.observe("shared")
        """)
        procs, _ = self._run_workers(script, 10)
        self.assertEqual([p.returncode for p in procs], [0] * 10)
        window = Window(self.dir, 100000, 100)
        window.load()
        self.assertEqual(window.stats()["admitted"], 1)
        self.assertTrue(window.seen("shared"))

    def test_capacity_under_contention(self):
        script = _worker("""
            import sys
            from dedupe_window import Window
            idx, state, n, cap = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
            w = Window(state, 100000, cap)
            for i in range(idx * n, idx * n + n):
                w.observe(f"k{i}")
        """)
        cap = 30
        procs, _ = self._run_workers(script, 8, 25, cap)
        self.assertEqual([p.returncode for p in procs], [0] * 8)
        window = Window(self.dir, 100000, cap)
        window.load()
        stats = window.stats()
        self.assertEqual(stats["retained"], cap)
        self.assertEqual(stats["admitted"], 8 * 25)

    def test_interleaved_advance_keeps_window_consistent(self):
        # Three advancers drive the same monotone clock; none may raise merely
        # because a peer committed a newer time.
        script = _worker("""
            import sys, time
            from dedupe_window import Window
            _, state = sys.argv[1], sys.argv[2]
            w = Window(state, 10, 100)
            for t in (5, 12, 20, 35):
                w.advance(t)
                time.sleep(0.005)
        """)
        procs, _ = self._run_workers(script, 3)
        self.assertEqual([p.returncode for p in procs], [0, 0, 0])
        window = Window(self.dir, 10, 100)
        window.load()
        stats = window.stats()
        self.assertEqual(stats["admitted"], stats["retained"] + stats["expired"])

    def test_killed_lock_holder_does_not_deadlock(self):
        script = _worker("""
            import os, sys
            from dedupe_window import Window
            _, state = sys.argv[1], sys.argv[2]
            w = Window(state, 10, 3)
            with w._locked(True):
                os.kill(os.getpid(), 9)
        """)
        env = dict(os.environ, PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        victim = subprocess.Popen([sys.executable, "-c", script, "0", self.dir], env=env)
        victim.communicate()
        self.assertLess(victim.returncode, 0)
        window = Window(self.dir, 10, 3)
        window.observe("after-kill")  # must not block forever or raise
        self.assertTrue(window.seen("after-kill"))

    def test_waiter_blocks_until_lock_released(self):
        script = _worker("""
            import sys, time
            from dedupe_window import Window
            _, state, hold = sys.argv[1], sys.argv[2], float(sys.argv[3])
            w = Window(state, 10, 3)
            with w._locked(True):
                time.sleep(hold)
        """)
        env = dict(os.environ, PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        holder = subprocess.Popen([sys.executable, "-c", script, "0", self.dir, "1.5"], env=env)
        time.sleep(0.3)
        try:
            window = Window(self.dir, 10, 3)
            started = time.monotonic()
            window.observe("late")
            self.assertGreaterEqual(time.monotonic() - started, 0.9)
            Window(self.dir, 10, 3).load()
        finally:
            holder.communicate()


if __name__ == "__main__":
    unittest.main()
