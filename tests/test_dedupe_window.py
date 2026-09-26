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


def _signed(body):
    """Serialize a document body with the checksum the reader expects."""
    from dedupe_window import window as mod

    data, _ = mod._signed_document(dict(body))
    return data.decode("utf-8")


def _v1_document(span=100000, capacity=100000, now=0,
                 admitted=0, expired=0, keys=()):
    return {
        "version": 1,
        "span": span,
        "capacity": capacity,
        "now": now,
        "admitted": admitted,
        "expired": expired,
        "keys": [list(item) for item in keys],
    }


def _segment_document(seq, base, now, admitted, expired, admit=()):
    return {
        "version": 2,
        "kind": "segment",
        "seq": seq,
        "base": base,
        "now": now,
        "admitted": admitted,
        "expired": expired,
        "admit": [list(item) for item in admit],
    }


class SegmentedSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        from dedupe_window import window as mod
        self.mod = mod
        self._saved = (
            mod._SMALL_STATE_BYTES,
            mod._COMPACT_SEGMENT_LIMIT,
            mod._COMPACT_SEGMENT_BYTES,
            mod._COMPACT_SEGMENT_RATIO,
        )
        mod._SMALL_STATE_BYTES = 1024
        mod._COMPACT_SEGMENT_LIMIT = 64
        mod._COMPACT_SEGMENT_BYTES = 1 << 30
        mod._COMPACT_SEGMENT_RATIO = 1 << 30

    def tearDown(self):
        (
            self.mod._SMALL_STATE_BYTES,
            self.mod._COMPACT_SEGMENT_LIMIT,
            self.mod._COMPACT_SEGMENT_BYTES,
            self.mod._COMPACT_SEGMENT_RATIO,
        ) = self._saved

    def _path(self, name="window.json"):
        return os.path.join(self.dir, name)

    def test_large_state_commits_delta_segments(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(40):
            window.observe(f"key-{i:08d}")
        names = sorted(os.listdir(self.dir))
        self.assertEqual(names[0], "window.json")
        self.assertTrue(
            any(name.startswith("window.json.seg.") for name in names), names
        )
        base_before = os.stat(self._path()).st_mtime_ns
        clone = Window(self.dir, 100000, 100000)
        clone.load()
        self.assertEqual(clone.keys(), [f"key-{i:08d}" for i in range(40)])
        self.assertEqual(clone.stats()["admitted"], 40)
        # A single further sighting must not rewrite the base snapshot.
        window.observe("key-00000040")
        self.assertEqual(os.stat(self._path()).st_mtime_ns, base_before)
        clone2 = Window(self.dir, 100000, 100000)
        clone2.load()
        self.assertEqual(len(clone2.keys()), 41)

    def test_observe_bytes_are_proportional_to_the_change(self):
        window = Window(self.dir, 100000, 1_000_000)
        for i in range(800):
            window.observe(f"bulk-{i:08d}")
        window.save()  # compact: one large base file
        base_size = os.stat(self._path()).st_size
        self.assertGreater(base_size, 10000)
        window.observe("one-more-key")
        names = [n for n in os.listdir(self.dir) if n.startswith("window.json.seg.")]
        self.assertEqual(len(names), 1)
        # The commit writes the delta only, not another full mirror.
        self.assertLess(os.stat(self._path(names[0])).st_size, base_size // 50)
        clone = Window(self.dir, 100000, 1_000_000)
        clone.load()
        self.assertEqual(clone.stats()["retained"], 801)
        self.assertTrue(clone.seen("one-more-key"))

    def test_save_compacts_segments_into_one_file(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(40):
            window.observe(f"key-{i:08d}")
        self.assertGreater(len(os.listdir(self.dir)), 1)
        window.save()
        self.assertEqual(os.listdir(self.dir), ["window.json"])
        with open(self._path()) as fh:
            doc = json.load(fh)
        self.assertEqual(doc["version"], 2)
        self.assertEqual(doc["kind"], "base")
        self.assertEqual(doc["seq"], doc["seq"])
        clone = Window(self.dir, 100000, 100000)
        clone.load()
        self.assertEqual(clone.keys(), [f"key-{i:08d}" for i in range(40)])
        self.assertEqual(clone.stats(), window.stats())

    def test_automatic_compaction_after_segment_limit(self):
        self.mod._COMPACT_SEGMENT_LIMIT = 3
        window = Window(self.dir, 100000, 100000)
        compacted_at_least_once = False
        for i in range(20):
            window.observe(f"key-{i:08d}")
            if os.listdir(self.dir) == ["window.json"] and i > 5:
                compacted_at_least_once = True
        self.assertTrue(compacted_at_least_once)
        clone = Window(self.dir, 100000, 100000)
        clone.load()
        self.assertEqual(len(clone.keys()), 20)
        self.assertEqual(clone.stats()["admitted"], 20)

    def test_expiry_replays_through_segments(self):
        window = Window(self.dir, 10, 100000)
        for i in range(40):
            window.observe(f"key-{i:08d}")
        self.assertEqual(window.advance(11), 40)
        clone = Window(self.dir, 10, 100000)
        clone.load()
        self.assertEqual(clone.keys(), [])
        self.assertEqual(clone.stats()["admitted"], 40)
        self.assertEqual(clone.stats()["expired"], 40)
        window.save()
        clone2 = Window(self.dir, 10, 100000)
        clone2.load()
        self.assertEqual(clone2.stats(), window.stats())

    def test_capacity_eviction_replays_through_segments(self):
        window = Window(self.dir, 100000, 30)
        for i in range(40):
            window.observe(f"key-{i:08d}")
        clone = Window(self.dir, 100000, 30)
        clone.load()
        self.assertEqual(len(clone.keys()), 30)
        self.assertEqual(clone.keys()[0], "key-00000010")
        self.assertEqual(clone.stats()["admitted"], 40)
        self.assertEqual(clone.stats()["expired"], 0)

    def test_v1_document_is_loaded_and_upgraded_on_next_commit(self):
        body = _v1_document(
            now=4, admitted=2, expired=0,
            keys=[["a", 0], ["b", 4]],
        )
        with open(self._path(), "w", encoding="utf-8") as fh:
            fh.write(_signed(body))
        window = Window(self.dir, 100000, 100000)
        window.load()  # old version reads transparently
        self.assertEqual(window.keys(), ["a", "b"])
        self.assertEqual(window.stats()["admitted"], 2)
        window.observe("c")  # upgrade is written with the next commit
        with open(self._path()) as fh:
            doc = json.load(fh)
        self.assertEqual(doc["version"], 2)
        self.assertEqual(doc["kind"], "base")
        self.assertEqual(os.listdir(self.dir), ["window.json"])
        clone = Window(self.dir, 100000, 100000)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b", "c"])

    def test_v1_document_is_upgraded_by_save(self):
        with open(self._path(), "w", encoding="utf-8") as fh:
            fh.write(_signed(_v1_document()))
        window = Window(self.dir, 100000, 100000)
        window.load()
        window.save()
        with open(self._path()) as fh:
            doc = json.load(fh)
        self.assertEqual(doc["version"], 2)

    def test_missing_version_is_value_error(self):
        body = _v1_document()
        del body["version"]
        with open(self._path(), "w", encoding="utf-8") as fh:
            fh.write(_signed(body))
        with self.assertRaises(ValueError):
            Window(self.dir, 100000, 100000).load()

    def test_unknown_version_is_value_error(self):
        body = _v1_document()
        body["version"] = 99
        with open(self._path(), "w", encoding="utf-8") as fh:
            fh.write(_signed(body))
        with self.assertRaises(ValueError) as cm:
            Window(self.dir, 100000, 100000).load()
        self.assertIn("unsupported version", str(cm.exception))

    def test_segment_with_foreign_base_is_value_error(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(30):
            window.observe(f"key-{i:08d}")
        name = "window.json.seg.999"
        seg = _segment_document(999, "0" * 64, 0, 0, 0)
        with open(self._path(name), "w", encoding="utf-8") as fh:
            fh.write(_signed(seg))
        with self.assertRaises(ValueError) as cm:
            Window(self.dir, 100000, 100000).load()
        self.assertIn("does not belong", str(cm.exception))

    def test_gap_in_segment_chain_is_value_error(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(30):
            window.observe(f"key-{i:08d}")
        with open(self._path()) as fh:
            base = json.load(fh)
        gap_seq = base["seq"] + 50
        seg = _segment_document(
            gap_seq, base["checksum"], 0, 0, 0
        )
        with open(self._path(f"window.json.seg.{gap_seq}"), "w") as fh:
            fh.write(_signed(seg))
        with self.assertRaises(ValueError) as cm:
            Window(self.dir, 100000, 100000).load()
        self.assertIn("missing a segment", str(cm.exception))

    def test_tampered_segment_is_value_error(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(30):
            window.observe(f"key-{i:08d}")
        seg_names = sorted(
            n for n in os.listdir(self.dir) if n.startswith("window.json.seg.")
        )
        path = self._path(seg_names[0])
        with open(path, encoding="utf-8") as fh:
            seg = json.load(fh)
        seg["admitted"] = 999
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(seg, fh)
        with self.assertRaises(ValueError):
            Window(self.dir, 100000, 100000).load()

    def test_truncated_segment_is_value_error(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(30):
            window.observe(f"key-{i:08d}")
        seg_names = sorted(
            n for n in os.listdir(self.dir) if n.startswith("window.json.seg.")
        )
        path = self._path(seg_names[0])
        os.truncate(path, os.path.getsize(path) // 2)
        with self.assertRaises(ValueError):
            Window(self.dir, 100000, 100000).load()

    def test_stale_segment_after_interrupted_compaction_is_ignored(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(40):
            window.observe(f"key-{i:08d}")
        window.save()  # base seq > 0, segments removed
        with open(self._path()) as fh:
            doc = json.load(fh)
        stale = _segment_document(1, "deadbeef" * 8, 0, 0, 0)
        with open(self._path("window.json.seg.1"), "w", encoding="utf-8") as fh:
            fh.write(_signed(stale))
        clone = Window(self.dir, 100000, 100000)
        clone.load()  # must not confuse the stale segment with live state
        self.assertEqual(len(clone.keys()), 40)
        clone.observe("key-00000040")
        self.assertFalse(
            os.path.exists(self._path("window.json.seg.1"))
        )

    def test_v1_base_with_segment_is_half_migration_error(self):
        body = _v1_document()
        text = _signed(body)
        with open(self._path(), "w", encoding="utf-8") as fh:
            fh.write(text)
        base_checksum = json.loads(text)["checksum"]
        seg = _segment_document(1, base_checksum, 0, 0, 0)
        with open(self._path("window.json.seg.1"), "w", encoding="utf-8") as fh:
            fh.write(_signed(seg))
        with self.assertRaises(ValueError) as cm:
            Window(self.dir, 100000, 100000).load()
        self.assertIn("half migrated", str(cm.exception))

    def test_no_residue_after_many_transactions(self):
        window = Window(self.dir, 100000, 100000)
        for i in range(40):
            window.observe(f"key-{i:08d}")
        window.advance(5)
        window.save()
        for name in os.listdir(self.dir):
            self.assertFalse(name.endswith(".tmp"))
        self.assertEqual(os.listdir(self.dir), ["window.json"])

    def test_concurrent_observers_roundtrip_and_compact(self):
        script = textwrap.dedent("""
            import sys
            from dedupe_window import Window
            idx, state, n = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
            w = Window(state, 100000, 1000000)
            for i in range(idx * n, idx * n + n):
                w.observe(f"k{i:08d}")
        """)
        env = dict(
            os.environ,
            PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""),
        )
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(i), self.dir, "25"],
                env=env,
            )
            for i in range(8)
        ]
        self.assertEqual([p.wait() for p in procs], [0] * 8)
        clone = Window(self.dir, 100000, 1000000)
        clone.load()
        self.assertEqual(clone.stats()["admitted"], 200)
        self.assertEqual(len(clone.keys()), 200)
        clone.save()
        self.assertEqual(os.listdir(self.dir), ["window.json"])


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
