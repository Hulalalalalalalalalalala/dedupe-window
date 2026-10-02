import hashlib
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from unittest import mock

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
        self.assertEqual(doc["version"], 3)
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
        self.assertEqual((base["version"], base["kind"]), (3, "base"))
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
        self.assertEqual(json.loads(lines[0])["version"], 3)
        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b"])
        self.assertEqual(clone.stats()["admitted"], 2)

    def test_v1_document_is_rewritten_as_v2_on_save(self):
        self._write_v1([["a", 0]], admitted=1)
        window = Window(self.dir, 10, 3)
        window.load()
        window.save()
        self.assertEqual(json.loads(self._lines()[0])["version"], 3)

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


def _export_checksum(doc):
    body = {key: value for key, value in doc.items() if key != "checksum"}
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class ConstructorSideEffectTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def test_constructor_creates_no_directory_or_file(self):
        state = os.path.join(self.root, "missing", "window")
        Window(state, 10, 3)
        self.assertFalse(os.path.exists(state))

    def test_read_only_calls_create_nothing(self):
        state = os.path.join(self.root, "window")
        window = Window(state, 10, 3)
        self.assertEqual(window.keys(), [])
        self.assertFalse(window.seen("a"))
        self.assertEqual(
            window.stats(),
            {"span": 10, "capacity": 3, "retained": 0, "admitted": 0, "expired": 0},
        )
        self.assertFalse(os.path.exists(state))

    def test_first_mutation_creates_state(self):
        state = os.path.join(self.root, "window")
        window = Window(state, 10, 3)
        self.assertTrue(window.observe("a"))
        self.assertTrue(os.path.exists(os.path.join(state, "window.json")))

    def test_reads_work_with_only_a_file_and_no_directory_entry(self):
        # The directory exists but the data file is absent: reads answer from
        # the empty state, load() still reports the missing file.
        state = os.path.join(self.root, "window")
        os.makedirs(state)
        window = Window(state, 10, 3)
        self.assertEqual(window.keys(), [])
        with self.assertRaises(FileNotFoundError):
            window.load()


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.window = Window(self.dir, 100000, 100000)

    def _build(self):
        self.window.observe("a")   # commit 1, now 0
        self.window.advance(5)    # commit 2, now 5
        self.window.observe("b")   # commit 3, now 5
        return self.window

    def test_export_captures_complete_state_in_order(self):
        self._build()
        doc = self.window.export(1)
        self.assertEqual(doc["seq"], 1)
        self.assertEqual(doc["keys"], [["a", 0]])
        self.assertEqual(doc["now"], 0)
        self.assertEqual((doc["admitted"], doc["expired"]), (1, 0))
        doc3 = self.window.export(3)
        self.assertEqual(doc3["keys"], [["a", 0], ["b", 5]])
        self.assertEqual(doc3["now"], 5)
        self.assertEqual(doc3["admitted"], 2)

    def test_export_is_self_checking(self):
        self._build()
        doc = self.window.export(2)
        self.assertEqual(len(doc["checksum"]), 64)
        self.assertEqual(doc["checksum"], _export_checksum(doc))
        tampered = json.loads(json.dumps(doc))
        tampered["admitted"] = 99
        self.assertNotEqual(tampered["checksum"], _export_checksum(tampered))

    def test_export_survives_json_roundtrip_unchanged(self):
        self._build()
        for seq in (1, 2, 3):
            doc = self.window.export(seq)
            self.assertEqual(json.loads(json.dumps(doc)), doc)

    def test_export_rejects_non_integer_seq_with_type_error(self):
        self._build()
        for bad in (1.0, 1.5, "1", None, [1], (1,), object()):
            with self.assertRaises(TypeError, msg=repr(bad)):
                self.window.export(bad)

    def test_export_rejects_bool_seq_with_type_error(self):
        for bad in (True, False):
            with self.assertRaises(TypeError):
                self.window.export(bad)

    def test_export_rejects_bad_or_unknown_commit_with_value_error(self):
        self._build()
        for bad in (0, -1, -100):
            with self.assertRaises(ValueError):
                self.window.export(bad)
        for unknown in (4, 5, 10 ** 9):
            with self.assertRaises(ValueError):
                self.window.export(unknown)

    def test_export_before_any_commit(self):
        with self.assertRaises(ValueError):
            self.window.export(1)

    def test_every_commit_is_exportable_with_distinct_number(self):
        window = Window(self.dir, 100000, 4)
        for i, key in enumerate("abcd", start=1):
            window.observe(key)
            doc = window.export(i)
            self.assertEqual(doc["seq"], i)
            self.assertEqual(doc["admitted"], i)
        # capacity eviction: "a" gone at commit 5
        window.observe("e")
        doc = window.export(5)
        self.assertEqual([k for k, _ in doc["keys"]], ["b", "c", "d", "e"])

    def test_export_identical_before_and_after_compaction(self):
        import dedupe_window.window as mod
        window = Window(self.dir, 10 ** 9, 10 ** 9)
        before = {}
        with mock.patch.object(mod, "_MAX_DELTAS", 8), \
                mock.patch.object(mod, "_MIN_ANCHOR_BYTES", 64):
            for i in range(60):
                window.observe(f"k{i}")
            for seq in range(1, 61):
                before[seq] = window.export(seq)
            for i in range(60, 300):
                window.observe(f"k{i}")
            with open(os.path.join(self.dir, "window.json"), "rb") as fh:
                raw = fh.read().decode()
            self.assertGreaterEqual(raw.count('"kind":"anchor"'), 1)
            for seq, doc in before.items():
                self.assertEqual(window.export(seq), doc, seq)

    def test_tip_export_matches_a_freshly_loaded_window(self):
        window = Window(self.dir, 60, 1000)
        for i in range(120):
            window.observe(f"k{i}")
            window.advance(i)
        clone = Window(self.dir, 60, 1000)
        clone.load()
        tip_seq = window._seq
        latest = window.export(tip_seq)
        self.assertEqual([k for k, _ in latest["keys"]], clone.keys())
        self.assertEqual(latest["admitted"], clone.stats()["admitted"])
        self.assertEqual(latest["expired"], clone.stats()["expired"])


class RestoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _exported(self, span=100000, capacity=100000, n=20, **advances):
        source = Window(tempfile.mkdtemp(), span, capacity)
        for i in range(n):
            source.observe(f"k{i}")
            if "step" in advances:
                source.advance(i * advances["step"])
        return source, source.export(n)

    def test_restore_matches_document_field_by_field(self):
        source, doc = self._exported(span=100000, n=10)
        target = Window(self.dir, 10, 3)  # deliberately different settings
        target.observe("unrelated")
        target.restore(doc)
        self.assertEqual(target.keys(), [k for k, _ in doc["keys"]])
        stats = target.stats()
        self.assertEqual(stats["span"], doc["span"])
        self.assertEqual(stats["capacity"], doc["capacity"])
        self.assertEqual(stats["admitted"], doc["admitted"])
        self.assertEqual(stats["expired"], doc["expired"])
        self.assertEqual(stats["retained"], len(doc["keys"]))

    def test_restored_state_persists(self):
        _, doc = self._exported(n=8)
        Window(self.dir, 100000, 100000).restore(doc)
        clone = Window(self.dir, doc["span"], doc["capacity"])
        clone.load()
        self.assertEqual(clone.keys(), [k for k, _ in doc["keys"]])
        self.assertEqual(clone.export(doc["seq"]), doc)

    def test_commits_after_restore_continue_the_sequence(self):
        _, doc = self._exported(n=5)
        window = Window(self.dir, doc["span"], doc["capacity"])
        window.restore(doc)
        window.observe("continued")
        self.assertEqual(window.export(doc["seq"] + 1)["seq"], doc["seq"] + 1)
        self.assertIn(["continued", doc["now"]],
                      window.export(doc["seq"] + 1)["keys"])
        # the restored point is still exportable, older points are not
        self.assertEqual(window.export(doc["seq"]), doc)
        with self.assertRaises(ValueError):
            window.export(1)

    def test_restore_rejects_non_document_with_type_error(self):
        window = Window(self.dir, 10, 3)
        for bad in (None, 1, 1.5, "string", ["a"], ("x",), json.dumps({})):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.restore(bad)

    def test_restore_rejects_missing_or_mistyped_fields(self):
        _, doc = self._exported(n=4)
        window = Window(self.dir, 100000, 100000)
        for drop in ("seq", "span", "capacity", "now", "admitted",
                     "expired", "keys", "format"):
            partial = {k: v for k, v in doc.items() if k != drop}
            with self.assertRaises(ValueError, msg=drop):
                window.restore(partial)
        wrong_type = json.loads(json.dumps(doc))
        wrong_type["seq"] = "3"
        with self.assertRaises(ValueError):
            window.restore(wrong_type)
        wrong_type = json.loads(json.dumps(doc))
        wrong_type["keys"] = [["k", "late"]]
        with self.assertRaises(ValueError):
            window.restore(wrong_type)

    def test_restore_rejects_checksum_mismatch(self):
        _, doc = self._exported(n=4)
        window = Window(self.dir, 100000, 100000)
        for field, value in (("admitted", 999), ("now", 42), ("seq", 2)):
            bad = json.loads(json.dumps(doc))
            bad[field] = value
            with self.assertRaises(ValueError):
                window.restore(bad)

    def test_restore_then_export_roundtrip_is_stable(self):
        source = Window(tempfile.mkdtemp(), 10 ** 9, 10 ** 9)
        for i in range(30):
            source.observe(f"s{i}")
        docs = {seq: source.export(seq) for seq in (1, 10, 20, 30)}
        window = Window(self.dir, 10 ** 9, 10 ** 9)
        for seq, doc in docs.items():
            window.restore(doc)
            self.assertEqual(window.export(seq), doc)
            self.assertEqual(window.keys(), [k for k, _ in doc["keys"]])

    def test_restore_leaves_one_base_file_at_the_commit(self):
        _, doc = self._exported(n=6)
        Window(self.dir, 100000, 100000).restore(doc)
        with open(os.path.join(self.dir, "window.json"), "rb") as fh:
            lines = [line for line in fh.read().decode().splitlines() if line]
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual((record["version"], record["kind"]), (3, "base"))
        self.assertEqual(record["seq"], doc["seq"])


class LockFreeReadTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_reads_do_not_block_on_a_held_write_lock(self):
        writer = Window(self.dir, 10 ** 9, 10 ** 6)
        for i in range(100):
            writer.observe(f"k{i}")
        reader = Window(self.dir, 10 ** 9, 10 ** 6)
        with writer._locked(True):
            done = threading.Event()
            answer = {}

            def read():
                answer["keys"] = reader.keys()
                answer["stats"] = reader.stats()
                answer["seen"] = reader.seen("k0")
                done.set()

            thread = threading.Thread(target=read)
            thread.start()
            self.assertTrue(done.wait(2.0), "read blocked behind the write lock")
            thread.join()
        # the read observed one complete commit: retained matches the count
        self.assertEqual(len(answer["keys"]), answer["stats"]["retained"])
        self.assertEqual(answer["stats"]["admitted"], len(answer["keys"])
                         + answer["stats"]["expired"])

    def test_concurrent_writer_never_exposes_a_broken_state(self):
        writer = Window(self.dir, 10 ** 9, 10 ** 6)
        reader = Window(self.dir, 10 ** 9, 10 ** 6)
        stop = threading.Event()
        failures = []

        def read_loop():
            while not stop.is_set():
                try:
                    stats = reader.stats()
                    keys = reader.keys()
                    if stats["retained"] > stats["admitted"]:
                        failures.append("counts split across commits")
                    if len(keys) != len(set(keys)):
                        failures.append("duplicate keys in a read")
                except Exception as exc:  # readers must not raise
                    failures.append(repr(exc))

        threads = [threading.Thread(target=read_loop) for _ in range(3)]
        for t in threads:
            t.start()
        for i in range(2000):
            writer.observe(f"k{i}")
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(failures, [])

    def test_reader_ignores_torn_tail_without_error(self):
        writer = Window(self.dir, 10 ** 9, 10)
        for i in range(5):
            writer.observe(f"k{i}")
        path = os.path.join(self.dir, "window.json")
        with open(path, "ab") as fh:
            fh.write(b'{"kind":"delta","op":"admi')  # interrupted append
        reader = Window(self.dir, 10 ** 9, 10)
        self.assertEqual(reader.keys(), [f"k{i}" for i in range(5)])
        self.assertEqual(reader.stats()["admitted"], 5)
        self.assertTrue(reader.seen("k4"))


class CliExportRestoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.src = os.path.join(self.dir, "src")
        for key in "abc":
            subprocess.run(
                [sys.executable, "-m", "dedupe_window", "--state", self.src,
                 "observe", key],
                capture_output=True, text=True, check=True,
            )

    def run_cli(self, *args, stdin=None):
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", *args],
            capture_output=True, text=True, input=stdin,
        )

    def _document(self, seq):
        result = self.run_cli("--state", self.src, "export", str(seq))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(result.stderr), 0)
        self.assertTrue(result.stdout.endswith("\n"))
        return json.loads(result.stdout)

    def test_export_writes_one_self_checking_object(self):
        window = Window(self.src, 60, 1024)
        window.load()
        for seq in (1, 2, 3):
            result = self.run_cli("--state", self.src, "export", str(seq))
            self.assertEqual(result.returncode, 0, result.stderr)
            # exactly one JSON object followed by a single newline
            self.assertEqual(result.stdout.count("\n"), 1)
            doc = json.loads(result.stdout)
            self.assertEqual(doc, window.export(seq))
            # JSON round trip leaves content and checksum unchanged
            self.assertEqual(json.loads(json.dumps(doc)), doc)
            self.assertEqual(_export_checksum(doc), doc["checksum"])

    def test_export_is_a_pure_read(self):
        before = os.listdir(self.src)
        stamp = os.stat(os.path.join(self.src, "window.json")).st_mtime_ns
        for seq in (1, 2, 3):
            result = self.run_cli("--state", self.src, "export", str(seq))
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(os.listdir(self.src), before)
        self.assertEqual(
            os.stat(os.path.join(self.src, "window.json")).st_mtime_ns, stamp
        )

    def test_export_creates_nothing_when_state_is_missing(self):
        missing = os.path.join(self.dir, "never")
        result = self.run_cli("--state", missing, "export", "1")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertTrue(result.stderr.startswith("state file is corrupt"))
        self.assertFalse(os.path.exists(missing))

    def test_export_on_corrupt_state_exits_1_with_empty_stdout(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        with open(os.path.join(state, "window.json"), "w", encoding="utf-8") as fh:
            fh.write("{broken")
        result = self.run_cli("--state", state, "export", "1")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith("state file is corrupt"))

    def test_export_non_positive_or_unknown_seq_exits_1(self):
        for seq in ("0", "-1", "-100", "4", "1000000000"):
            result = self.run_cli("--state", self.src, "export", seq)
            self.assertEqual(result.returncode, 1, seq)
            self.assertEqual(result.stdout, "", seq)
            self.assertTrue(result.stderr.startswith("state file is corrupt"), seq)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, seq)

    def test_export_usage_errors_exit_2(self):
        for args in (
            ["--state", self.src, "export"],
            ["--state", self.src, "export", "1", "2"],
            ["--state", self.src, "export", "1.0"],
            ["--state", self.src, "export", "0x1"],
            ["--state", self.src, "export", "+1"],
            ["--state", self.src, "export", "abc"],
            ["--state", self.src, "export", "1 "],
            ["--state", self.src, "export", " 1"],
            ["--state", self.src, "export", "true"],
            ["--state", self.src, "restore", "extra"],
        ):
            result = self.run_cli(*args)
            self.assertEqual(result.returncode, 2, args)
            self.assertEqual(result.stdout, "", args)
            self.assertIn("usage", result.stderr.lower(), args)

    def test_pipe_export_into_restore(self):
        dst = os.path.join(self.dir, "dst")
        exported = self.run_cli("--state", self.src, "export", "3")
        restored = self.run_cli("--state", dst, "restore",
                                stdin=exported.stdout)
        self.assertEqual(restored.returncode, 0, restored.stderr)
        stats = self.run_cli("--state", dst, "stats")
        self.assertEqual(
            stats.stdout,
            '{"span":60,"capacity":1024,"retained":3,'
            '"admitted":3,"expired":0}\n',
        )
        self.assertEqual(restored.stdout, stats.stdout)
        # the restored document is byte-identical when exported again
        again = self.run_cli("--state", dst, "export", "3")
        self.assertEqual(again.stdout, exported.stdout)

    def test_restore_overwrites_a_healthy_state(self):
        dst = os.path.join(self.dir, "dst")
        for key in "zzz":
            self.run_cli("--state", dst, "observe", key)
        doc = self._document(2)
        result = self.run_cli("--state", dst, "restore",
                              stdin=json.dumps(doc))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["retained"], 2)
        window = Window(dst, doc["span"], doc["capacity"])
        window.load()
        self.assertEqual(window.export(2), doc)

    def test_restore_overwrites_a_corrupt_state(self):
        dst = os.path.join(self.dir, "dst")
        os.makedirs(dst)
        with open(os.path.join(dst, "window.json"), "w", encoding="utf-8") as fh:
            fh.write('{"version":3,"kind":"base",garbage')
        doc = self._document(1)
        result = self.run_cli("--state", dst, "restore",
                              stdin=json.dumps(doc))
        self.assertEqual(result.returncode, 0, result.stderr)
        window = Window(dst, doc["span"], doc["capacity"])
        window.load()
        self.assertEqual(window.export(1), doc)

    def test_commits_after_restore_continue_from_the_next_number(self):
        dst = os.path.join(self.dir, "dst")
        doc = self._document(3)
        self.run_cli("--state", dst, "restore", stdin=json.dumps(doc))
        admitted = self.run_cli("--state", dst, "observe", "d")
        self.assertEqual(admitted.stdout, "true\n")
        exported = self.run_cli("--state", dst, "export", "4")
        self.assertEqual(exported.returncode, 0)
        self.assertEqual(json.loads(exported.stdout)["seq"], 4)
        gone = self.run_cli("--state", dst, "export", "2")
        # commit 2 predates the restored base and can no longer be located
        self.assertEqual(gone.returncode, 1)

    def test_restore_accepts_surrounding_whitespace_only(self):
        doc = self._document(1)
        result = self.run_cli(
            "--state", os.path.join(self.dir, "w1"), "restore",
            stdin=" \n\t " + json.dumps(doc) + "\n  \t\n",
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_restore_failures_exit_1_and_change_nothing(self):
        doc = self._document(2)

        def tampered(mutate):
            bad = json.loads(json.dumps(doc))
            mutate(bad)
            return json.dumps(bad)

        inputs = {
            "empty": "",
            "whitespace only": "   \n\t",
            "not json": "{not json",
            "json array": json.dumps([doc]),
            "json number": "123",
            "json string": json.dumps(json.dumps(doc)),
            "two objects": json.dumps(doc) + json.dumps(doc),
            "trailing junk": json.dumps(doc) + "junk",
            "trailing comma": json.dumps(doc)[:-1] + ",}",
            "missing field": json.dumps({k: v for k, v in doc.items()
                                         if k != "seq"}),
            "wrong seq type": tampered(lambda d: d.__setitem__("seq", "2")),
            "bad checksum": tampered(lambda d: d.__setitem__("admitted", 99)),
        }
        for name, payload in inputs.items():
            dst = os.path.join(self.dir, "case_" + name.replace(" ", "_"))
            result = self.run_cli("--state", dst, "restore", stdin=payload)
            self.assertEqual(result.returncode, 1, name)
            self.assertEqual(result.stdout, "", name)
            self.assertTrue(result.stderr.startswith("restore failed"), name)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, name)
            self.assertFalse(os.path.exists(dst), name)

    def test_failed_restore_keeps_the_old_state_file_byte_for_byte(self):
        dst = os.path.join(self.dir, "dst")
        self.run_cli("--state", dst, "observe", "keep")
        with open(os.path.join(dst, "window.json"), "rb") as fh:
            before = fh.read()
        result = self.run_cli("--state", dst, "restore", stdin="not json")
        self.assertEqual(result.returncode, 1)
        with open(os.path.join(dst, "window.json"), "rb") as fh:
            after = fh.read()
        self.assertEqual(after, before)
        listing = os.listdir(dst)
        self.assertEqual(listing, ["window.json"])


class ObserveManyTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_spec_example(self):
        window = Window(self.dir, 100000, 2)
        window.observe("a")
        self.assertEqual(window.observe_many(["b", "b", "c", "a"]),
                         [True, False, True, True])
        self.assertEqual(window.keys(), ["c", "a"])

    def test_empty_batch_is_a_pure_noop(self):
        state = os.path.join(self.dir, "never")
        window = Window(state, 10, 3)
        self.assertEqual(window.observe_many([]), [])
        self.assertFalse(os.path.exists(state))

    def test_non_list_raises_type_error_and_creates_nothing(self):
        state = os.path.join(self.dir, "never")
        window = Window(state, 10, 3)
        for bad in (None, 1, 1.5, "ab", ("a", "b"), {"a": 1}, True):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_many(bad)
        self.assertFalse(os.path.exists(state))

    def test_non_string_element_raises_type_error_and_creates_nothing(self):
        state = os.path.join(self.dir, "never")
        window = Window(state, 10, 3)
        for bad in (["a", 1], ["a", None], [1], [["a"]], [True], [b"a"]):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_many(bad)
        self.assertFalse(os.path.exists(state))

    def test_admitted_count_is_number_of_trues(self):
        window = Window(self.dir, 100000, 10)
        window.observe("a")
        hits = window.observe_many(["a", "b", "b", "c", "a", "d"])
        self.assertEqual(hits, [False, True, False, True, False, True])
        stats = window.stats()
        self.assertEqual(stats["admitted"], 4)
        self.assertEqual(stats["expired"], 0)
        self.assertEqual(stats["retained"], 4)

    def test_capacity_eviction_in_batch_is_not_expiry(self):
        window = Window(self.dir, 100000, 2)
        window.observe("a")
        window.observe_many(["b", "c"])
        stats = window.stats()
        self.assertEqual((stats["retained"], stats["admitted"], stats["expired"]),
                         (2, 3, 0))
        self.assertEqual(window.keys(), ["b", "c"])

    def test_intra_batch_evicted_key_is_readmitted(self):
        window = Window(self.dir, 100000, 2)
        hits = window.observe_many(["a", "b", "c", "a", "b"])
        self.assertEqual(hits, [True, True, True, True, True])
        self.assertEqual(window.keys(), ["a", "b"])
        self.assertEqual(window.stats()["admitted"], 5)

    def test_intra_batch_duplicate_does_not_refresh_order(self):
        window = Window(self.dir, 10, 2)
        window.observe("a")           # first seen 0
        window.advance(5)
        hits = window.observe_many(["b", "a", "c"])
        self.assertEqual(hits, [True, False, True])
        self.assertEqual(window.keys(), ["b", "c"])
        # "b" and "c" were seen at 5, the time at the batch start.
        doc = window.export(window._seq)
        self.assertEqual(doc["keys"], [["b", 5], ["c", 5]])
        self.assertEqual(doc["now"], 5)

    def test_batch_does_not_advance_time(self):
        window = Window(self.dir, 10, 3)
        window.advance(7)
        window.observe_many(["a", "b"])
        self.assertEqual(window.export(window._seq)["now"], 7)
        # time is still 7: an advance to 7 is a no-op commit
        self.assertEqual(window.advance(7), 0)

    def test_admitting_batch_adds_exactly_one_commit(self):
        window = Window(self.dir, 100000, 100)
        window.observe("seed")
        before = window._seq
        window.observe_many([f"k{i}" for i in range(20)])
        self.assertEqual(window._seq, before + 1)
        path = os.path.join(self.dir, "window.json")
        with open(path, "rb") as fh:
            lines = fh.read().decode().splitlines()
        segment = json.loads(lines[-1])
        self.assertEqual((segment["kind"], segment["op"]), ("delta", "batch"))
        self.assertEqual(segment["seq"], before + 1)
        self.assertEqual(len(segment["keys"]), 20)
        self.assertEqual(segment["hits"], [True] * 20)
        self.assertNotIn("drop", segment)
        self.assertNotIn("add", segment)

    def test_all_duplicate_batch_commits_nothing(self):
        window = Window(self.dir, 100000, 3)
        window.observe("a")
        window.observe("b")
        path = os.path.join(self.dir, "window.json")
        before_bytes = open(path, "rb").read()
        before_seq = window._seq
        hits = window.observe_many(["a", "b", "a"])
        self.assertEqual(hits, [False, False, False])
        self.assertEqual(window._seq, before_seq)
        self.assertEqual(open(path, "rb").read(), before_bytes)

    def test_batch_persists_across_reopen(self):
        window = Window(self.dir, 100000, 2)
        window.observe("a")
        window.observe_many(["b", "b", "c", "a"])
        clone = Window(self.dir, 100000, 2)
        clone.load()
        self.assertEqual(clone.keys(), ["c", "a"])
        stats = clone.stats()
        self.assertEqual((stats["admitted"], stats["expired"]), (4, 0))

    def test_batch_commit_is_exportable_and_survives_compaction(self):
        import dedupe_window.window as mod
        window = Window(self.dir, 10 ** 9, 4)
        with mock.patch.object(mod, "_MAX_DELTAS", 4), \
                mock.patch.object(mod, "_MIN_ANCHOR_BYTES", 64):
            window.observe_many(["a", "b", "c"])
            seq = window._seq
            doc = window.export(seq)
            self.assertEqual(doc["seq"], 1)
            self.assertEqual([k for k, _ in doc["keys"]], ["a", "b", "c"])
            for i in range(40):
                window.observe(f"k{i}")
            self.assertEqual(window.export(seq), doc)

    def test_restore_then_batch_continues_numbering(self):
        source = Window(tempfile.mkdtemp(), 100000, 100)
        for i in range(5):
            source.observe(f"s{i}")
        doc = source.export(5)
        window = Window(self.dir, doc["span"], doc["capacity"])
        window.restore(doc)
        window.observe_many(["n1", "n2"])
        self.assertEqual(window._seq, 6)
        exported = window.export(6)
        self.assertEqual(exported["seq"], 6)
        self.assertEqual(exported["admitted"], 7)
        self.assertEqual(window.export(5), doc)

    def _write_raw(self, payload):
        path = os.path.join(self.dir, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(payload)

    def test_corrupt_state_batch_raises_value_error_without_overwrite(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        path = os.path.join(state, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{broken")
        raw_before = open(path, "rb").read()
        window = Window(state, 10, 3)
        with self.assertRaises(ValueError):
            window.observe_many(["a"])
        self.assertEqual(open(path, "rb").read(), raw_before)

    def test_settings_mismatch_batch_raises_value_error(self):
        Window(self.dir, 10, 3).save()
        with self.assertRaises(ValueError):
            Window(self.dir, 99, 3).observe_many(["a"])

    def test_tampered_batch_hits_are_detected_on_replay(self):
        window = Window(self.dir, 100000, 2)
        window.observe("a")
        window.observe_many(["b", "b", "c"])
        path = os.path.join(self.dir, "window.json")
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        segment = json.loads(lines[-1])
        self.assertEqual(segment["op"], "batch")
        segment["hits"] = [True, True, True]  # lie about the duplicate
        # Re-sign so the envelope checksum still verifies; replay itself must
        # notice the results cannot follow from the inputs.  A torn tail is
        # appended so the tampered batch segment is not the (ignorable) tail.
        lines[-1] = json.dumps(segment, sort_keys=True, separators=(",", ":"))
        tampered = json.loads(lines[-1])
        lines[-1] = json.dumps(
            {**tampered, "checksum": _export_checksum(tampered)},
            sort_keys=True, separators=(",", ":"),
        )
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + '\n{"kind":"delta",')
        with self.assertRaises(ValueError):
            Window(self.dir, 100000, 2).load()

    def test_concurrent_batches_match_serial_counts(self):
        script = textwrap.dedent("""
            import sys
            from dedupe_window import Window
            idx, state, n = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
            w = Window(state, 100000, 10000)
            for b in range(n):
                base = (idx * n + b) * 4
                w.observe_many([f"k{base+j}" for j in range(4)])
        """)
        env = dict(os.environ,
                   PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        n_workers, per_worker = 8, 25
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(i), self.dir, str(per_worker)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for i in range(n_workers)
        ]
        for p in procs:
            out, err = p.communicate()
            self.assertEqual(p.returncode, 0, err)
        window = Window(self.dir, 100000, 10000)
        window.load()
        stats = window.stats()
        self.assertEqual(stats["admitted"], n_workers * per_worker * 4)
        self.assertEqual(stats["retained"], n_workers * per_worker * 4)
        self.assertEqual(window._seq, n_workers * per_worker)


class CliObserveBatchTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_cli(self, payload, *args):
        state = args[0] if args else os.path.join(self.dir, "window")
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "observe-batch"],
            capture_output=True, text=True, input=payload,
        )

    def test_batch_flow(self):
        state = os.path.join(self.dir, "window")
        seed = subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "observe", "a"],
            capture_output=True, text=True,
        )
        self.assertEqual(seed.returncode, 0)
        result = self.run_cli(json.dumps(["b", "b", "c", "a"]), state)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "[true,false,true,false]\n")
        stats = subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state, "stats"],
            capture_output=True, text=True,
        )
        self.assertEqual(
            stats.stdout,
            '{"span":60,"capacity":1024,"retained":3,"admitted":3,"expired":0}\n',
        )
        # The batch committed: another batch now sees every key as a repeat.
        again = self.run_cli(json.dumps(["a", "c"]), state)
        self.assertEqual((again.returncode, again.stdout), (0, "[false,false]\n"))

    def test_all_duplicate_batch_creates_no_commit(self):
        state = os.path.join(self.dir, "window")
        for key in "ab":
            subprocess.run(
                [sys.executable, "-m", "dedupe_window", "--state", state,
                 "observe", key],
                capture_output=True, text=True, check=True,
            )
        path = os.path.join(state, "window.json")
        before = open(path, "rb").read()
        result = self.run_cli(json.dumps(["a", "b", "a"]), state)
        self.assertEqual((result.returncode, result.stdout),
                         (0, "[false,false,false]\n"))
        self.assertEqual(open(path, "rb").read(), before)

    def test_surrounding_whitespace_is_allowed(self):
        result = self.run_cli(' \n\t ["a", "b"]\n  \t\n')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "[true,true]\n")

    def test_empty_array_outputs_empty_list_and_creates_nothing(self):
        state = os.path.join(self.dir, "never")
        result = self.run_cli("[]", state)
        self.assertEqual((result.returncode, result.stdout), (0, "[]\n"))
        self.assertFalse(os.path.exists(state))

    def test_failures_exit_1_one_line_prefix_and_empty_stdout(self):
        cases = {
            "empty input": "",
            "whitespace only": "  \n\t",
            "not json": "{not json",
            "json object": json.dumps({"a": 1}),
            "json string": json.dumps("a"),
            "json number": "123",
            "non-string element": json.dumps(["a", 1]),
            "null element": json.dumps(["a", None]),
            "bool element": json.dumps([True]),
            "trailing junk": '["a"] junk',
            "two arrays": '["a"]["b"]',
        }
        for name, payload in cases.items():
            state = os.path.join(self.dir, "case_" + name.replace(" ", "_"))
            result = self.run_cli(payload, state)
            self.assertEqual(result.returncode, 1, name)
            self.assertEqual(result.stdout, "", name)
            self.assertTrue(result.stderr.startswith("batch failed"), name)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, name)
            self.assertFalse(os.path.exists(state), name)

    def test_failure_keeps_old_state_byte_for_byte(self):
        state = os.path.join(self.dir, "window")
        subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "observe", "keep"],
            capture_output=True, text=True, check=True,
        )
        path = os.path.join(state, "window.json")
        before = open(path, "rb").read()
        result = self.run_cli("{not json", state)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(open(path, "rb").read(), before)

    def test_corrupt_state_exits_1_with_batch_failed(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        with open(os.path.join(state, "window.json"), "w", encoding="utf-8") as fh:
            fh.write("{broken")
        result = self.run_cli('["a"]', state)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith("batch failed"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)

    def test_operand_is_a_usage_error_exit_2(self):
        result = subprocess.run(
            [sys.executable, "-m", "dedupe_window",
             "--state", os.path.join(self.dir, "w"), "observe-batch", "x"],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("usage", result.stderr.lower())


class ObserveEventsTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def window(self, span=100000, capacity=10):
        return Window(self.dir, span, capacity)

    def test_classification_in_input_order(self):
        window = self.window()
        kinds = window.observe_events(
            [
                {"key": "a", "timestamp": 0},
                {"key": "b", "timestamp": 5},
                {"key": "a", "timestamp": 2},
                {"key": "c", "timestamp": 5},
            ],
            10,
        )
        self.assertEqual(kinds, ["admitted", "admitted", "duplicate", "admitted"])

    def test_watermark_and_timestamps_accept_int_and_float(self):
        window = self.window()
        kinds = window.observe_events(
            [{"key": "a", "timestamp": 2.5}, {"key": "b", "timestamp": 3}],
            7.5,
        )
        self.assertEqual(kinds, ["admitted", "admitted"])

    def test_retained_keys_ordered_by_event_time_ties_admission(self):
        window = self.window()
        window.observe_events(
            [
                {"key": "c", "timestamp": 9},
                {"key": "a", "timestamp": 2},
                {"key": "b", "timestamp": 2},
                {"key": "d", "timestamp": 9},
            ],
            10,
        )
        self.assertEqual(window.keys(), ["a", "b", "c", "d"])
        doc = window.export(window._seq)
        self.assertEqual(
            doc["keys"], [["a", 2], ["b", 2], ["c", 9], ["d", 9]]
        )

    def test_duplicate_does_not_refresh_time_or_order(self):
        window = self.window(span=10, capacity=10)
        window.observe_events([{"key": "a", "timestamp": 0}], 0)
        window.observe_events(
            [
                {"key": "b", "timestamp": 5},
                {"key": "a", "timestamp": 5},  # duplicate, keeps first time 0
            ],
            5,
        )
        self.assertEqual(window.keys(), ["a", "b"])
        # "a" still expires by its first sighting at 0.
        window.observe_events([], 11)
        self.assertEqual(window.keys(), ["b"])

    def test_late_boundary_is_inclusive(self):
        window = self.window(span=10, capacity=10)
        kinds = window.observe_events(
            [
                {"key": "a", "timestamp": 0.0},
                {"key": "b", "timestamp": 0.1},
            ],
            10,
        )
        # 10 - 0 == span: valid; 10 - 0.1 < span: valid.
        self.assertEqual(kinds, ["admitted", "admitted"])
        kinds = window.observe_events(
            [
                {"key": "c", "timestamp": 0.9},
                {"key": "d", "timestamp": 1},
            ],
            11,
        )
        # 11 - 0.9 > 10: late; 11 - 1 == 10: valid.
        self.assertEqual(kinds, ["late", "admitted"])

    def test_late_takes_precedence_over_duplicate(self):
        window = self.window(span=10, capacity=10)
        window.observe_events([{"key": "a", "timestamp": 9}], 10)
        # "a" is retained (first 9), but this older delivery is late even
        # though the key is present -- late beats duplicate.
        kinds = window.observe_events([{"key": "a", "timestamp": 0}], 11)
        self.assertEqual(kinds, ["late"])
        self.assertTrue(window.seen("a"))
        self.assertEqual(window.keys(), ["a"])  # first sighting untouched

    def test_late_event_records_nothing(self):
        window = self.window(span=10, capacity=10)
        kinds = window.observe_events([{"key": "a", "timestamp": 0}], 11)
        self.assertEqual(kinds, ["late"])
        self.assertEqual(window.keys(), [])
        stats = window.stats()
        # A late event was never admitted, so it neither counts nor expires.
        self.assertEqual((stats["admitted"], stats["expired"]), (0, 0))

    def test_watermark_advance_expires_keys(self):
        window = self.window(span=10, capacity=10)
        window.observe_events(
            [{"key": "a", "timestamp": 0}, {"key": "b", "timestamp": 5}],
            5,
        )
        kinds = window.observe_events([{"key": "c", "timestamp": 12}], 12)
        self.assertEqual(kinds, ["admitted"])
        self.assertEqual(window.keys(), ["b", "c"])  # "a" expired (12 - 0 > 10)
        self.assertEqual(window.stats()["expired"], 1)

    def test_empty_list_still_advances_and_expires(self):
        window = self.window(span=10, capacity=10)
        window.observe_events([{"key": "a", "timestamp": 0}], 0)
        seq = window._seq
        kinds = window.observe_events([], 11)
        self.assertEqual(kinds, [])
        self.assertEqual(window.keys(), [])
        self.assertEqual(window.stats()["expired"], 1)
        self.assertEqual(window._seq, seq + 1)

    def test_capacity_eviction_then_readmission_in_batch(self):
        window = self.window(span=100000, capacity=2)
        kinds = window.observe_events(
            [
                {"key": "a", "timestamp": 0},
                {"key": "b", "timestamp": 1},
                {"key": "c", "timestamp": 2},  # evicts "a"
                {"key": "a", "timestamp": 3},  # "a" readmitted; evicts "b"
                {"key": "b", "timestamp": 4},  # "b" evicted too: readmitted
            ],
            10,
        )
        self.assertEqual(
            kinds, ["admitted"] * 5
        )
        self.assertEqual(window.keys(), ["a", "b"])
        stats = window.stats()
        self.assertEqual((stats["admitted"], stats["expired"]), (5, 0))

    def test_duplicate_of_retained_key_inside_event_batch(self):
        window = self.window(span=100000, capacity=3)
        kinds = window.observe_events(
            [
                {"key": "a", "timestamp": 0},
                {"key": "b", "timestamp": 1},
                {"key": "a", "timestamp": 2},  # retained: duplicate
            ],
            10,
        )
        self.assertEqual(kinds, ["admitted", "admitted", "duplicate"])
        self.assertEqual(window.keys(), ["a", "b"])
        self.assertEqual(window.stats()["admitted"], 2)

    def test_capacity_eviction_is_not_expiry(self):
        window = self.window(span=100000, capacity=2)
        window.observe_events(
            [
                {"key": "a", "timestamp": 0},
                {"key": "b", "timestamp": 0},
                {"key": "c", "timestamp": 0},
            ],
            0,
        )
        stats = window.stats()
        self.assertEqual((stats["retained"], stats["admitted"], stats["expired"]),
                         (2, 3, 0))

    def test_watermark_advance_alone_is_one_commit(self):
        window = self.window()
        window.observe_events([{"key": "a", "timestamp": 0}], 0)
        before = window._seq
        window.observe_events([], 5)
        self.assertEqual(window._seq, before + 1)

    def test_admission_and_expiry_share_one_commit(self):
        window = self.window(span=10, capacity=10)
        window.observe_events([{"key": "a", "timestamp": 0}], 0)
        before = window._seq
        window.observe_events([{"key": "b", "timestamp": 11}], 11)
        self.assertEqual(window._seq, before + 1)
        stats = window.stats()
        self.assertEqual((stats["admitted"], stats["expired"]), (2, 1))

    def test_no_change_batch_commits_nothing(self):
        window = self.window()
        window.observe_events([{"key": "a", "timestamp": 0}], 0)
        path = os.path.join(self.dir, "window.json")
        before_bytes = open(path, "rb").read()
        before_seq = window._seq
        kinds = window.observe_events(
            [{"key": "a", "timestamp": 0}, {"key": "a", "timestamp": 0}], 0
        )
        self.assertEqual(kinds, ["duplicate", "duplicate"])
        self.assertEqual(window._seq, before_seq)
        self.assertEqual(open(path, "rb").read(), before_bytes)

    def test_watermark_may_stay_equal(self):
        window = self.window()
        window.observe_events([{"key": "a", "timestamp": 0}], 5)
        kinds = window.observe_events([{"key": "b", "timestamp": 5}], 5)
        self.assertEqual(kinds, ["admitted"])

    # -- validation -------------------------------------------------------

    def test_non_list_events_raises_type_error(self):
        window = self.window()
        for bad in (None, 1, 1.5, "x", (), {}, True):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_events(bad, 0)

    def test_watermark_type_errors(self):
        window = self.window()
        for bad in ("5", None, [1], True, False):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_events([], bad)

    def test_watermark_range_errors(self):
        window = self.window()
        for bad in (-1, -0.5, float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError, msg=repr(bad)):
                window.observe_events([], bad)

    def test_event_shape_and_types(self):
        window = self.window()
        for bad in (None, 1, "a", [], [1]):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_events([bad], 0)
        with self.assertRaises(ValueError):
            window.observe_events([{"key": "a"}], 0)  # missing timestamp
        with self.assertRaises(ValueError):
            window.observe_events([{"timestamp": 0}], 0)  # missing key
        with self.assertRaises(TypeError):
            window.observe_events([{"key": 1, "timestamp": 0}], 0)
        with self.assertRaises(TypeError):
            window.observe_events([{"key": "a", "timestamp": "0"}], 0)
        with self.assertRaises(TypeError):
            window.observe_events([{"key": "a", "timestamp": True}], 0)
        for bad in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError, msg=repr(bad)):
                window.observe_events([{"key": "a", "timestamp": bad}], 1)

    def test_future_event_raises_value_error(self):
        window = self.window()
        with self.assertRaises(ValueError):
            window.observe_events([{"key": "a", "timestamp": 6}], 5)
        self.assertEqual(window.keys(), [])

    def test_watermark_regression_raises_value_error(self):
        window = self.window()
        window.observe_events([{"key": "a", "timestamp": 0}], 10)
        with self.assertRaises(ValueError):
            window.observe_events([], 9)
        # equal is still allowed
        self.assertEqual(window.observe_events([], 10), [])

    def test_validation_failure_creates_no_directory(self):
        state = os.path.join(self.dir, "never")
        window = Window(state, 10, 3)
        with self.assertRaises(TypeError):
            window.observe_events("nope", 0)
        with self.assertRaises(ValueError):
            window.observe_events([], -1)
        with self.assertRaises(ValueError):
            window.observe_events([{"key": "a", "timestamp": 1}], 0)
        self.assertFalse(os.path.exists(state))

    def test_empty_batch_at_zero_on_fresh_window_creates_nothing(self):
        state = os.path.join(self.dir, "never2")
        window = Window(state, 10, 3)
        self.assertEqual(window.observe_events([], 0), [])
        self.assertFalse(os.path.exists(state))

    def test_failed_batch_keeps_state_byte_for_byte(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        path = os.path.join(state, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{broken")
        before = open(path, "rb").read()
        window = Window(state, 10, 3)
        with self.assertRaises(ValueError):
            window.observe_events([{"key": "a", "timestamp": 0}], 0)
        self.assertEqual(open(path, "rb").read(), before)

    def test_settings_mismatch_raises_value_error(self):
        Window(self.dir, 10, 3).save()
        with self.assertRaises(ValueError):
            Window(self.dir, 99, 3).observe_events([], 0)

    # -- persistence ------------------------------------------------------

    def test_roundtrip_preserves_order_counts_time(self):
        window = self.window(span=10, capacity=3)
        window.observe_events(
            [
                {"key": "c", "timestamp": 4},
                {"key": "a", "timestamp": 0},
                {"key": "b", "timestamp": 2},
            ],
            5,
        )
        clone = Window(self.dir, 10, 3)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b", "c"])
        self.assertEqual(clone.stats(), window.stats())
        kinds = clone.observe_events([], 11)
        self.assertEqual(kinds, [])
        # 11 - 0 > 10 expires "a"; "b"@2 and "c"@4 are still inside the span.
        self.assertEqual(clone.keys(), ["b", "c"])
        self.assertEqual(clone.stats()["expired"], 1)
        clone.observe_events([], 13)  # 13 - 2 > 10 expires "b"
        self.assertEqual(clone.keys(), ["c"])
        self.assertEqual(clone.stats()["expired"], 2)

    def test_events_segment_is_small_and_chained(self):
        window = self.window()
        window.observe("seed")
        before = window._seq
        window.observe_events(
            [{"key": "a", "timestamp": 1}, {"key": "b", "timestamp": 2}], 3
        )
        path = os.path.join(self.dir, "window.json")
        lines = open(path, "rb").read().decode().splitlines()
        segment = json.loads(lines[-1])
        self.assertEqual((segment["kind"], segment["op"]), ("delta", "events"))
        self.assertEqual(segment["seq"], before + 1)
        self.assertEqual(segment["wmark"], 0)
        self.assertEqual(segment["events"], [["a", 1], ["b", 2]])
        self.assertEqual(segment["kinds"], ["admitted", "admitted"])
        self.assertEqual(segment["now"], 3)
        self.assertEqual(segment["prev"], json.loads(lines[-2])["checksum"])
        self.assertNotIn("drop", segment)
        self.assertNotIn("add", segment)

    def test_events_batch_exportable_before_and_after_compaction(self):
        import dedupe_window.window as mod
        window = Window(self.dir, 10 ** 9, 10 ** 9)
        with mock.patch.object(mod, "_MAX_DELTAS", 4), \
                mock.patch.object(mod, "_MIN_ANCHOR_BYTES", 64):
            window.observe_events(
                [{"key": "c", "timestamp": 2}, {"key": "a", "timestamp": 1}], 2
            )
            seq = window._seq
            doc = window.export(seq)
            self.assertEqual(doc["seq"], 1)
            self.assertEqual(doc["keys"], [["a", 1], ["c", 2]])
            for i in range(40):
                window.observe_events([{"key": f"k{i}", "timestamp": i + 3}],
                                      i + 3)
            self.assertEqual(window.export(seq), doc)

    def test_restore_then_events_continues_numbering(self):
        source = Window(tempfile.mkdtemp(), 100000, 100)
        for i in range(5):
            source.observe(f"s{i}")
        doc = source.export(5)
        window = Window(self.dir, doc["span"], doc["capacity"])
        window.restore(doc)
        window.observe_events([{"key": "n", "timestamp": doc["now"]}],
                              doc["now"])
        self.assertEqual(window._seq, 6)
        self.assertEqual(window.export(6)["seq"], 6)
        self.assertEqual(window.export(5), doc)

    def test_tampered_event_kinds_detected_on_replay(self):
        window = self.window(span=100000, capacity=5)
        window.observe("seed")
        window.observe_events(
            [{"key": "a", "timestamp": 0}, {"key": "a", "timestamp": 0}], 0
        )
        path = os.path.join(self.dir, "window.json")
        lines = open(path, encoding="utf-8").read().splitlines()
        segment = json.loads(lines[-1])
        self.assertEqual(segment["op"], "events")
        segment["kinds"] = ["admitted", "admitted"]
        lines[-1] = json.dumps(segment, sort_keys=True, separators=(",", ":"))
        tampered = json.loads(lines[-1])
        lines[-1] = json.dumps(
            {**tampered, "checksum": _export_checksum(tampered)},
            sort_keys=True, separators=(",", ":"),
        )
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + '\n{"kind":"delta",')
        with self.assertRaises(ValueError):
            Window(self.dir, 100000, 5).load()

    def test_concurrent_event_batches_match_serial_counts(self):
        script = textwrap.dedent("""
            import sys
            from dedupe_window import Window
            idx, state, n = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
            w = Window(state, 100000, 10000)
            for b in range(n):
                base = (idx * n + b) * 4
                while True:
                    wm = w._now + 1
                    ev = [{"key": f"k{base+j}", "timestamp": wm}
                          for j in range(4)]
                    try:
                        w.observe_events(ev, wm)
                        break
                    except ValueError:
                        pass
        """)
        env = dict(os.environ,
                   PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        n_workers, per_worker = 8, 25
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(i), self.dir,
                 str(per_worker)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for i in range(n_workers)
        ]
        for p in procs:
            out, err = p.communicate()
            self.assertEqual(p.returncode, 0, err)
        window = Window(self.dir, 100000, 10000)
        window.load()
        total = n_workers * per_worker * 4
        stats = window.stats()
        self.assertEqual(stats["admitted"], total)
        self.assertEqual(stats["retained"], total)
        self.assertEqual(window._seq, n_workers * per_worker)


class CliObserveEventsTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_cli(self, payload, state=None):
        state = state or os.path.join(self.dir, "window")
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "observe-events"],
            capture_output=True, text=True, input=payload,
        )

    def test_event_flow_with_stats(self):
        state = os.path.join(self.dir, "window")
        seed = subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "observe-events"],
            capture_output=True, text=True,
            input=json.dumps({"events": [{"key": "a", "timestamp": 0}],
                              "watermark": 0}),
        )
        self.assertEqual(seed.returncode, 0, seed.stderr)
        self.assertEqual(seed.stdout, '["admitted"]\n')
        result = self.run_cli(
            json.dumps({
                "events": [
                    {"key": "b", "timestamp": 5},
                    {"key": "a", "timestamp": 2},
                    {"key": "c", "timestamp": 5},
                ],
                "watermark": 10,
            }),
            state,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout, '["admitted","duplicate","admitted"]\n'
        )
        stats = subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "stats"],
            capture_output=True, text=True,
        )
        doc = json.loads(stats.stdout)
        self.assertEqual((doc["retained"], doc["admitted"], doc["expired"]),
                         (3, 3, 0))

    def test_initial_time_is_zero_and_late_classification(self):
        state = os.path.join(self.dir, "window")
        result = self.run_cli(
            json.dumps({"events": [
                {"key": "a", "timestamp": 0},
                {"key": "b", "timestamp": 1},
            ], "watermark": 61}),  # default span 60: "a" late, boundary "b" valid
            state,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '["late","admitted"]\n')

    def test_empty_events_advances_watermark(self):
        state = os.path.join(self.dir, "window")
        first = self.run_cli(json.dumps({"events": [], "watermark": 0}), state)
        self.assertEqual((first.returncode, first.stdout), (0, "[]\n"))
        # nothing committed at time 0, so the directory was never created
        self.assertFalse(os.path.exists(state))
        moved = self.run_cli(json.dumps({"events": [], "watermark": 5}), state)
        self.assertEqual((moved.returncode, moved.stdout), (0, "[]\n"))
        self.assertTrue(os.path.exists(os.path.join(state, "window.json")))

    def test_extra_fields_ignored(self):
        result = self.run_cli(
            json.dumps({"events": [{"key": "a", "timestamp": 0, "x": 1}],
                        "watermark": 0, "note": "hi"})
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '["admitted"]\n')

    def test_surrounding_whitespace_allowed(self):
        result = self.run_cli(
            ' \n\t {"events":[{"key":"a","timestamp":0}],"watermark":0}\n \t'
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '["admitted"]\n')

    def test_failures_exit_1_one_line_prefix_empty_stdout(self):
        cases = {
            "empty input": "",
            "whitespace only": "  \n\t",
            "not json": "{not json",
            "json array": json.dumps([]),
            "json number": "123",
            "missing watermark": json.dumps({"events": []}),
            "missing events": json.dumps({"watermark": 0}),
            "events not list": json.dumps({"events": {}, "watermark": 0}),
            "event not object": json.dumps(
                {"events": [["a", 0]], "watermark": 0}),
            "event missing key": json.dumps(
                {"events": [{"timestamp": 0}], "watermark": 0}),
            "event missing timestamp": json.dumps(
                {"events": [{"key": "a"}], "watermark": 0}),
            "non-string key": json.dumps(
                {"events": [{"key": 1, "timestamp": 0}], "watermark": 0}),
            "string timestamp": json.dumps(
                {"events": [{"key": "a", "timestamp": "0"}], "watermark": 0}),
            "bool timestamp": json.dumps(
                {"events": [{"key": "a", "timestamp": True}],
                 "watermark": 0}),
            "bool watermark": json.dumps({"events": [], "watermark": True}),
            "nan watermark": json.dumps({"events": [], "watermark": float("nan")}),
            "negative watermark": json.dumps({"events": [], "watermark": -1}),
            "future event": json.dumps(
                {"events": [{"key": "a", "timestamp": 6}], "watermark": 5}),
            "trailing junk": '{"events":[],"watermark":0} junk',
            "two objects": '{"events":[],"watermark":0}' * 2,
        }
        for name, payload in cases.items():
            state = os.path.join(self.dir, "case_" + name.replace(" ", "_"))
            result = self.run_cli(payload, state)
            self.assertEqual(result.returncode, 1, name)
            self.assertEqual(result.stdout, "", name)
            self.assertTrue(result.stderr.startswith("event batch failed"), name)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, name)
            self.assertFalse(os.path.exists(state), name)

    def test_failure_keeps_old_state_byte_for_byte(self):
        state = os.path.join(self.dir, "window")
        self.run_cli(json.dumps({"events": [{"key": "keep", "timestamp": 0}],
                                 "watermark": 0}), state)
        path = os.path.join(state, "window.json")
        before = open(path, "rb").read()
        result = self.run_cli("{not json", state)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(open(path, "rb").read(), before)

    def test_corrupt_state_exits_1_with_event_batch_failed(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        with open(os.path.join(state, "window.json"), "w",
                  encoding="utf-8") as fh:
            fh.write("{broken")
        result = self.run_cli(
            json.dumps({"events": [{"key": "a", "timestamp": 0}],
                        "watermark": 0}),
            state,
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith("event batch failed"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)

    def test_watermark_regression_exits_1(self):
        state = os.path.join(self.dir, "window")
        self.run_cli(json.dumps({"events": [], "watermark": 10}), state)
        result = self.run_cli(json.dumps({"events": [], "watermark": 9}), state)
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("event batch failed"))

    def test_operand_is_a_usage_error_exit_2(self):
        result = subprocess.run(
            [sys.executable, "-m", "dedupe_window",
             "--state", os.path.join(self.dir, "w"), "observe-events", "x"],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("usage", result.stderr.lower())


class DeliverEventsTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def window(self, span=100000, capacity=10):
        return Window(self.dir, span, capacity)

    def _ev(self, *pairs):
        return [{"key": k, "timestamp": t} for k, t in pairs]

    def test_first_delivery_matches_observe_events_classification(self):
        window = self.window()
        events = self._ev(("a", 0), ("b", 5), ("a", 2), ("c", 5))
        self.assertEqual(
            window.deliver_events("id1", events, 10),
            ["admitted", "admitted", "duplicate", "admitted"],
        )

    def test_replay_after_expiry_and_eviction_returns_original(self):
        window = self.window(span=10, capacity=10)
        events = self._ev(("a", 0), ("b", 5))
        original = window.deliver_events("id1", events, 5)
        # Expire "a", so a fresh observe_events would now re-admit it.
        window.observe_events([], 11)
        self.assertEqual(window.keys(), ["b"])
        self.assertEqual(window.deliver_events("id1", events, 5), original)
        # Capacity-evicted path: force "b" out through a tight window, then
        # replay must still report the original "admitted" for it.
        tight = Window(tempfile.mkdtemp(), 100000, 1)
        ev = self._ev(("x", 0), ("y", 1))
        first = tight.deliver_events("d", ev, 1)
        self.assertEqual(first, ["admitted", "admitted"])
        self.assertEqual(tight.keys(), ["y"])  # "x" capacity-evicted
        self.assertEqual(tight.deliver_events("d", ev, 1), first)

    def test_replay_changes_nothing_and_succeeds_with_lagging_watermark(self):
        window = self.window(span=10, capacity=10)
        events = self._ev(("a", 0))
        self.assertEqual(window.deliver_events("id", events, 0), ["admitted"])
        window.observe_events([], 20)
        path = os.path.join(self.dir, "window.json")
        before = open(path, "rb").read()
        stats_before = window.stats()
        seq_before = window._seq
        self.assertEqual(window.deliver_events("id", events, 0), ["admitted"])
        self.assertEqual(window._seq, seq_before)
        self.assertEqual(window.stats(), stats_before)
        self.assertEqual(open(path, "rb").read(), before)

    def test_content_conflict_raises_value_error(self):
        window = self.window()
        events = self._ev(("a", 0), ("b", 1))
        window.deliver_events("id", events, 5)
        with self.assertRaises(ValueError):
            window.deliver_events("id", self._ev(("a", 0)), 5)
        with self.assertRaises(ValueError):
            window.deliver_events("id", events, 4)
        with self.assertRaises(ValueError):
            window.deliver_events("id", self._ev(("a", 0), ("x", 1)), 5)

    def test_extra_fields_ignored_and_int_float_equivalent(self):
        window = self.window()
        events = [{"key": "a", "timestamp": 2.0, "note": 1}]
        self.assertEqual(window.deliver_events("id", events, 5.0), ["admitted"])
        self.assertEqual(
            window.deliver_events("id", [{"key": "a", "timestamp": 2}], 5),
            ["admitted"],
        )

    def test_delivery_id_validation(self):
        window = self.window()
        for bad in (None, 1, 1.5, True, [], {}):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.deliver_events(bad, [], 0)
        with self.assertRaises(ValueError):
            window.deliver_events("", [], 0)

    def test_other_input_errors_match_observe_events(self):
        window = self.window()
        with self.assertRaises(TypeError):
            window.deliver_events("id", "nope", 0)
        with self.assertRaises(ValueError):
            window.deliver_events("id", [], -1)
        with self.assertRaises(ValueError):
            window.deliver_events("id", self._ev(("a", 1)), 0)

    def test_empty_and_all_duplicate_batches_commit(self):
        window = self.window()
        window.deliver_events("empty", [], 0)
        self.assertEqual(window._seq, 1)
        self.assertTrue(
            os.path.exists(os.path.join(self.dir, "window.json"))
        )
        before = window._seq
        window.deliver_events("dup", self._ev(("a", 0)), 0)
        window.deliver_events("dup2", self._ev(("a", 0)), 0)
        self.assertEqual(window._seq, before + 2)
        # A replay, by contrast, adds nothing.
        seq = window._seq
        window.deliver_events("dup2", self._ev(("a", 0)), 0)
        self.assertEqual(window._seq, seq)

    def test_ring_eviction_is_fifo_and_replay_does_not_refresh(self):
        import dedupe_window.window as mod
        window = self.window()
        with mock.patch.object(mod, "_RECEIPT_LIMIT", 4):
            for i in range(4):
                window.deliver_events(f"r{i}", self._ev((f"k{i}", 0)), 0)
            # Touching r0 must not move it to the back.
            window.deliver_events("r0", self._ev(("k0", 0)), 0)
            window.deliver_events("r4", self._ev(("k4", 0)), 0)
            # r0 is evicted despite the recent replay; r1 is still live and
            # replays its original result.
            self.assertEqual(
                window.deliver_events("r1", self._ev(("k1", 0)), 0),
                ["admitted"],
            )
            # Re-delivering the evicted r0 is a brand new request: it
            # re-classifies the still-retained key as a duplicate (and pushes
            # r1 out of the ring in its turn).
            self.assertEqual(
                window.deliver_events("r0", self._ev(("k0", 0)), 0),
                ["duplicate"],
            )
            self.assertEqual(
                window.deliver_events("r1", self._ev(("k1", 0)), 0),
                ["duplicate"],
            )

    def test_evicted_identifier_is_a_new_request_that_must_not_regress(self):
        import dedupe_window.window as mod
        window = self.window()
        with mock.patch.object(mod, "_RECEIPT_LIMIT", 1):
            window.deliver_events("only", self._ev(("a", 0)), 0)
            window.deliver_events("next", self._ev(("b", 0)), 5)
            with self.assertRaises(ValueError):
                window.deliver_events("only", self._ev(("a", 0)), 0)

    def test_receipts_survive_restart_and_compaction(self):
        import dedupe_window.window as mod
        window = self.window()
        events = self._ev(("a", 1))
        with mock.patch.object(mod, "_MAX_DELTAS", 4), \
                mock.patch.object(mod, "_MIN_ANCHOR_BYTES", 64):
            window.deliver_events("keep", events, 1)
            for i in range(2, 20):
                window.deliver_events(f"d{i}", self._ev((f"k{i}", i)), i)
            clone = Window(self.dir, 100000, 10)
            clone.load()
            self.assertEqual(clone.deliver_events("keep", events, 1), ["admitted"])

    def test_export_and_restore_preserve_receipts_and_conflict(self):
        window = self.window()
        events = self._ev(("a", 0), ("b", 1))
        window.deliver_events("id", events, 5)
        doc = window.export(window._seq)
        self.assertIn("receipts", doc)
        self.assertTrue(any(e[0] == "id" for e in doc["receipts"]))
        target = Window(tempfile.mkdtemp(), 10, 3)
        target.restore(doc)
        self.assertEqual(target.deliver_events("id", events, 5),
                         ["admitted", "admitted"])
        with self.assertRaises(ValueError):
            target.deliver_events("id", self._ev(("a", 0)), 5)

    def test_old_export_restores_with_no_receipts(self):
        import hashlib as _hashlib
        window = self.window()
        window.deliver_events("id", self._ev(("a", 0)), 0)
        doc = window.export(window._seq)
        old = {k: v for k, v in doc.items() if k != "receipts"}
        body = {k: v for k, v in old.items() if k != "checksum"}
        old["checksum"] = _hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        target = Window(tempfile.mkdtemp(), 10, 3)
        target.restore(old)  # no error: receipts treated as absent
        self.assertEqual(
            target.deliver_events("id", self._ev(("a", 0)), 0), ["duplicate"]
        )

    def test_tampered_delivery_segment_is_detected_on_load(self):
        window = self.window()
        window.observe("seed")
        window.deliver_events("id", self._ev(("a", 0)), 0)
        path = os.path.join(self.dir, "window.json")
        lines = open(path, encoding="utf-8").read().splitlines()
        segment = json.loads(lines[-1])
        segment["kinds"] = ["duplicate"]
        lines[-1] = json.dumps(segment, sort_keys=True, separators=(",", ":"))
        tampered = json.loads(lines[-1])
        lines[-1] = json.dumps(
            {**tampered, "checksum": _export_checksum(tampered)},
            sort_keys=True, separators=(",", ":"),
        )
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + '\n{"kind":"delta",')
        with self.assertRaises(ValueError):
            Window(self.dir, 100000, 10).load()


class CliDeliverEventsTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_cli(self, payload, state=None):
        state = state or os.path.join(self.dir, "window")
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "deliver-events"],
            capture_output=True, text=True, input=payload,
        )

    def test_deliver_flow_and_replay(self):
        state = os.path.join(self.dir, "window")
        payload = json.dumps({"delivery_id": "d1",
                              "events": [{"key": "a", "timestamp": 0}],
                              "watermark": 0})
        first = self.run_cli(payload, state)
        self.assertEqual((first.returncode, first.stdout), (0, '["admitted"]\n'))
        again = self.run_cli(payload, state)
        self.assertEqual((again.returncode, again.stdout), (0, '["admitted"]\n'))

    def test_empty_batch_commits_and_outputs_empty_list(self):
        state = os.path.join(self.dir, "window")
        result = self.run_cli(
            json.dumps({"delivery_id": "d", "events": [], "watermark": 5}), state
        )
        self.assertEqual((result.returncode, result.stdout), (0, "[]\n"))
        self.assertTrue(os.path.exists(os.path.join(state, "window.json")))

    def test_failures_exit_1_with_delivery_failed(self):
        cases = {
            "empty": "",
            "not json": "{x",
            "json array": json.dumps([]),
            "missing id": json.dumps({"events": [], "watermark": 0}),
            "missing events": json.dumps({"delivery_id": "x", "watermark": 0}),
            "missing watermark": json.dumps({"delivery_id": "x", "events": []}),
            "non-string id": json.dumps(
                {"delivery_id": 1, "events": [], "watermark": 0}),
            "empty id": json.dumps(
                {"delivery_id": "", "events": [], "watermark": 0}),
            "bad watermark": json.dumps(
                {"delivery_id": "x", "events": [], "watermark": -1}),
            "future event": json.dumps({"delivery_id": "x",
                                        "events": [{"key": "a", "timestamp": 9}],
                                        "watermark": 0}),
            "two objects": '{"delivery_id":"x","events":[],"watermark":0}' * 2,
        }
        for name, payload in cases.items():
            state = os.path.join(self.dir, "c_" + name.replace(" ", "_"))
            result = self.run_cli(payload, state)
            self.assertEqual(result.returncode, 1, name)
            self.assertEqual(result.stdout, "", name)
            self.assertTrue(result.stderr.startswith("delivery failed"), name)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, name)
            self.assertFalse(os.path.exists(state), name)

    def test_conflict_exits_1(self):
        state = os.path.join(self.dir, "window")
        self.run_cli(json.dumps({"delivery_id": "d",
                                 "events": [{"key": "a", "timestamp": 0}],
                                 "watermark": 0}), state)
        result = self.run_cli(json.dumps({"delivery_id": "d",
                                          "events": [{"key": "b", "timestamp": 0}],
                                          "watermark": 0}), state)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith("delivery failed"))

    def test_operand_is_a_usage_error_exit_2(self):
        result = subprocess.run(
            [sys.executable, "-m", "dedupe_window",
             "--state", os.path.join(self.dir, "w"), "deliver-events", "x"],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("usage", result.stderr.lower())

    def test_concurrent_identical_deliveries_commit_once(self):
        state = os.path.join(self.dir, "mp")
        script = textwrap.dedent("""
            import json, sys
            from dedupe_window import Window
            w = Window(sys.argv[1], 100000, 1000)
            print(json.dumps(w.deliver_events(
                "same", [{"key": "a", "timestamp": 0},
                         {"key": "b", "timestamp": 1}], 5)))
        """)
        env = dict(os.environ,
                   PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        procs = [
            subprocess.Popen([sys.executable, "-c", script, state], env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            for _ in range(6)
        ]
        outs = [p.communicate() for p in procs]
        self.assertEqual([p.returncode for p in procs], [0] * 6)
        self.assertEqual({o[0].decode().strip() for o in outs},
                         {json.dumps(["admitted", "admitted"])})
        window = Window(state, 100000, 1000)
        window.load()
        self.assertEqual(window.stats()["admitted"], 2)
        self.assertEqual(window._seq, 1)


class ProbeManyTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.window = Window(self.dir, 10, 3)

    def test_report_fields_and_order(self):
        report = self.window.probe_many(["a"])
        self.assertEqual(
            list(report),
            ["results", "seq", "queries", "bloom_positive", "matches",
             "false_positives", "hit_rate", "false_positive_rate"],
        )

    def test_results_align_with_input_and_duplicates_counted(self):
        self.window.observe("a")
        self.window.observe("b")
        report = self.window.probe_many(["a", "x", "b", "a"])
        self.assertEqual(report["results"], [True, False, True, True])
        self.assertEqual(report["queries"], 4)
        self.assertEqual(report["matches"], 3)
        self.assertEqual(report["seq"], 2)
        self.assertEqual(report["hit_rate"], 3 / 4)

    def test_empty_string_and_cjk_keys(self):
        self.window.observe("")
        self.window.observe("中文")
        report = self.window.probe_many(["", "中文", "中 文", "a"])
        self.assertEqual(report["results"], [True, True, False, False])

    def test_collisions_never_change_results(self):
        # One single bloom bit forces every key to collide; the exact set
        # still decides every answer and the misses show up as false
        # positives only.
        self.window.observe("a")
        report = self.window.probe_many(["a", "b", "c"], bits=1, hashes=1)
        self.assertEqual(report["results"], [True, False, False])
        self.assertEqual(report["bloom_positive"], 3)
        self.assertEqual(report["matches"], 1)
        self.assertEqual(report["false_positives"], 2)
        self.assertEqual(report["false_positive_rate"], 1.0)

    def test_bloom_negative_is_definitive(self):
        for key in "abc":
            self.window.observe(key)
        report = self.window.probe_many(["a", "b", "c"])
        self.assertEqual(report["results"], [True, True, True])
        self.assertEqual(report["false_positives"], 0)
        self.assertEqual(report["hit_rate"], 1.0)
        self.assertEqual(report["false_positive_rate"], 0)

    def test_zero_denominators_report_zero(self):
        self.window.observe("a")
        empty = self.window.probe_many([])
        self.assertEqual(empty["queries"], 0)
        self.assertEqual(empty["hit_rate"], 0)
        self.assertEqual(empty["false_positive_rate"], 0)
        hits_only = self.window.probe_many(["a", "a"])
        self.assertEqual(hits_only["hit_rate"], 1.0)
        self.assertEqual(hits_only["false_positive_rate"], 0)

    def test_empty_list_still_reads_the_snapshot(self):
        self.window.observe("a")
        self.window.observe("b")
        report = self.window.probe_many([])
        self.assertEqual(report["results"], [])
        self.assertEqual(report["seq"], 2)
        # A corrupt state is reported even for an empty query list.
        path = os.path.join(self.dir, "window.json")
        with open(path, "a") as fh:
            fh.write('{"kind":"delta","broken":1}\n{"kind":"delta","x":2}\n')
        with self.assertRaises(ValueError):
            self.window.probe_many([])

    def test_missing_file_is_the_empty_window(self):
        report = self.window.probe_many(["a", "b"])
        self.assertEqual(report["results"], [False, False])
        self.assertEqual(report["seq"], 0)
        self.assertFalse(os.path.exists(os.path.join(self.dir, "window.json")))

    def test_later_commits_evictions_and_restores_are_seen(self):
        self.window.observe("a")
        self.window.observe("b")
        other = Window(self.dir, 10, 3)
        other.observe("c")
        other.observe("d")  # capacity 3: evicts "a"
        report = self.window.probe_many(["a", "b", "c", "d"])
        self.assertEqual(report["results"], [False, True, True, True])
        self.assertEqual(report["seq"], 4)
        document = other.export(2)
        other.restore(document)
        report = self.window.probe_many(["a", "b", "c", "d"])
        self.assertEqual(report["results"], [True, True, False, False])
        self.assertEqual(report["seq"], 2)

    def test_probe_creates_nothing_and_changes_nothing(self):
        self.window.observe("a")
        path = os.path.join(self.dir, "window.json")
        before = open(path, "rb").read()
        self.window.probe_many(["a", "b"])
        self.assertEqual(open(path, "rb").read(), before)
        stats = self.window.stats()
        self.assertEqual((stats["admitted"], stats["expired"]), (1, 0))
        fresh = Window(os.path.join(self.dir, "never"), 10, 3)
        fresh.probe_many(["a"])
        self.assertFalse(os.path.exists(os.path.join(self.dir, "never")))

    def test_validation_runs_before_the_state_is_read(self):
        path = os.path.join(self.dir, "window.json")
        os.makedirs(self.dir, exist_ok=True)
        with open(path, "w") as fh:
            fh.write("{broken")
        for call in (
            lambda: self.window.probe_many("a"),
            lambda: self.window.probe_many(["a", 1]),
            lambda: self.window.probe_many([], bits=True),
            lambda: self.window.probe_many([], bits=0),
            lambda: self.window.probe_many([], hashes=1.5),
            lambda: self.window.probe_many([], hashes=17),
        ):
            with self.assertRaises((TypeError, ValueError)) as caught:
                call()
            # The complaint is about the arguments, not the corrupt file.
            self.assertNotIn("state file", str(caught.exception))

    def test_type_and_range_validation(self):
        for keys in ("a", ("a",), None, 1, ["a", None], ["a", True]):
            with self.assertRaises(TypeError):
                self.window.probe_many(keys)
        for bits in (True, 1.5, "8192", None):
            with self.assertRaises(TypeError):
                self.window.probe_many([], bits=bits)
        for bits in (0, -1, 1048577):
            with self.assertRaises(ValueError):
                self.window.probe_many([], bits=bits)
        for hashes in (True, 2.0, "4", None):
            with self.assertRaises(TypeError):
                self.window.probe_many([], hashes=hashes)
        for hashes in (0, -1, 17):
            with self.assertRaises(ValueError):
                self.window.probe_many([], hashes=hashes)
        # The boundaries themselves are valid.
        self.window.probe_many([], bits=1, hashes=1)
        self.window.probe_many([], bits=1048576, hashes=16)

    def test_corrupt_state_and_settings_mismatch_raise_value_error(self):
        self.window.observe("a")
        with self.assertRaises(ValueError):
            Window(self.dir, 99, 3).probe_many(["a"])
        path = os.path.join(self.dir, "window.json")
        with open(path, "a") as fh:
            fh.write('{"kind":"delta","broken":1}\n{"kind":"delta","x":2}\n')
        with self.assertRaises(ValueError):
            self.window.probe_many(["a"])

    def test_torn_tail_is_ignored(self):
        self.window.observe("a")
        path = os.path.join(self.dir, "window.json")
        with open(path, "ab") as fh:
            fh.write(b'{"kind":"delta","seq":2,"pre')
        report = self.window.probe_many(["a"])
        self.assertEqual(report["results"], [True])
        self.assertEqual(report["seq"], 1)

    def test_bit_storage_bound(self):
        from dedupe_window.window import _bloom_build
        for bits in (1, 7, 8, 9, 8192, 1048576):
            table = _bloom_build(["a", "b"], bits, 4)
            self.assertEqual(len(table), (bits + 7) // 8)

    def test_positives_are_deterministic_across_processes(self):
        for key in ("alpha", "beta", "gamma"):
            self.window.observe(key)
        script = textwrap.dedent("""
            import sys
            from dedupe_window import Window
            report = Window(sys.argv[1], 10, 3).probe_many(
                ["alpha", "delta", "epsilon"], bits=16, hashes=2)
            print(report["bloom_positive"], report["results"])
        """)
        env = dict(os.environ,
                   PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        outs = [
            subprocess.run([sys.executable, "-c", script, self.dir], env=env,
                           capture_output=True, text=True, check=True).stdout
            for _ in range(2)
        ]
        self.assertEqual(outs[0], outs[1])
        self.assertIn("[True, False, False]", outs[0])


class CliProbeBatchTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_cli(self, payload, *args):
        state = args[0] if args else os.path.join(self.dir, "window")
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", "--state", state,
             "probe-batch"],
            capture_output=True, text=True, input=payload,
        )

    def seed(self, state, *keys):
        for key in keys:
            subprocess.run(
                [sys.executable, "-m", "dedupe_window", "--state", state,
                 "observe", key],
                capture_output=True, text=True, check=True,
            )

    def test_probe_flow(self):
        state = os.path.join(self.dir, "window")
        self.seed(state, "a", "中文")
        result = self.run_cli(json.dumps({"keys": ["a", "b", "中文", "a"]}), state)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["results"], [True, False, True, True])
        self.assertEqual(report["seq"], 2)
        self.assertEqual(report["queries"], 4)
        self.assertEqual(report["matches"], 3)
        self.assertEqual(result.stdout, json.dumps(report, separators=(",", ":")) + "\n")

    def test_optional_fields_and_defaults(self):
        state = os.path.join(self.dir, "window")
        self.seed(state, "a")
        defaulted = self.run_cli(json.dumps({"keys": ["a", "b"]}), state)
        explicit = self.run_cli(
            json.dumps({"keys": ["a", "b"], "bits": 8192, "hashes": 4}), state)
        self.assertEqual(defaulted.returncode, 0, defaulted.stderr)
        self.assertEqual(defaulted.stdout, explicit.stdout)
        tiny = self.run_cli(json.dumps({"keys": ["a", "b"], "bits": 1, "hashes": 1}), state)
        report = json.loads(tiny.stdout)
        self.assertEqual(report["results"], [True, False])
        self.assertEqual(report["bloom_positive"], 2)
        self.assertEqual(report["false_positives"], 1)

    def test_extra_fields_ignored_and_whitespace_allowed(self):
        state = os.path.join(self.dir, "window")
        self.seed(state, "a")
        result = self.run_cli(' \n {"keys": ["a"], "other": [1, 2]} \t\n', state)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["results"], [True])

    def test_success_creates_nothing_and_changes_nothing(self):
        state = os.path.join(self.dir, "window")
        self.seed(state, "a")
        path = os.path.join(state, "window.json")
        before = open(path, "rb").read()
        result = self.run_cli(json.dumps({"keys": ["a", "b"]}), state)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(open(path, "rb").read(), before)
        missing = os.path.join(self.dir, "never")
        result = self.run_cli(json.dumps({"keys": ["a"]}), missing)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["seq"], 0)
        self.assertFalse(os.path.exists(missing))

    def test_failures_exit_1_one_line_prefix_and_empty_stdout(self):
        cases = {
            "empty input": "",
            "whitespace only": "  \n\t",
            "not json": "{not json",
            "json array": json.dumps(["a"]),
            "json string": json.dumps("a"),
            "missing keys": json.dumps({"bits": 4}),
            "keys not a list": json.dumps({"keys": "a"}),
            "non-string element": json.dumps({"keys": ["a", 1]}),
            "bool element": json.dumps({"keys": [True]}),
            "bits wrong type": json.dumps({"keys": [], "bits": "8"}),
            "bits bool": json.dumps({"keys": [], "bits": True}),
            "bits out of range": json.dumps({"keys": [], "bits": 0}),
            "bits too large": json.dumps({"keys": [], "bits": 1048577}),
            "hashes wrong type": json.dumps({"keys": [], "hashes": 1.5}),
            "hashes out of range": json.dumps({"keys": [], "hashes": 17}),
            "trailing junk": '{"keys": []} junk',
        }
        for name, payload in cases.items():
            state = os.path.join(self.dir, "case_" + name.replace(" ", "_"))
            result = self.run_cli(payload, state)
            self.assertEqual(result.returncode, 1, name)
            self.assertEqual(result.stdout, "", name)
            self.assertTrue(result.stderr.startswith("probe failed"), name)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, name)
            self.assertFalse(os.path.exists(state), name)

    def test_corrupt_state_exits_1_with_probe_failed(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        with open(os.path.join(state, "window.json"), "w", encoding="utf-8") as fh:
            fh.write("{broken")
        result = self.run_cli(json.dumps({"keys": ["a"]}), state)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith("probe failed"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)

    def test_failure_keeps_old_state_byte_for_byte(self):
        state = os.path.join(self.dir, "window")
        self.seed(state, "keep")
        path = os.path.join(state, "window.json")
        before = open(path, "rb").read()
        result = self.run_cli("{not json", state)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(open(path, "rb").read(), before)

    def test_operand_is_a_usage_error_exit_2(self):
        result = subprocess.run(
            [sys.executable, "-m", "dedupe_window",
             "--state", os.path.join(self.dir, "w"), "probe-batch", "x"],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("usage", result.stderr.lower())


if __name__ == "__main__":
    unittest.main()