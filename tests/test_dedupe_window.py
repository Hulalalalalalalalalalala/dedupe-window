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


class ObserveBatchTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_spec_example(self):
        window = Window(self.dir, 100, 2)
        window.observe("a")
        results = window.observe_many(["b", "b", "c", "a"])
        self.assertEqual(results, [True, False, True, True])
        self.assertEqual(window.keys(), ["c", "a"])
        stats = window.stats()
        self.assertEqual(stats["admitted"], 4)   # a + b + c + re-admitted a
        self.assertEqual(stats["expired"], 0)
        self.assertEqual(stats["retained"], 2)

    def test_results_align_with_input_order(self):
        window = Window(self.dir, 100000, 10)
        window.observe("seen")
        self.assertEqual(
            window.observe_many(["x", "seen", "y", "x", "z"]),
            [True, False, True, False, True],
        )
        self.assertEqual(window.keys(), ["seen", "x", "y", "z"])

    def test_empty_batch_returns_empty_without_filesystem(self):
        state = os.path.join(self.dir, "missing", "window")
        window = Window(state, 10, 3)
        self.assertEqual(window.observe_many([]), [])
        self.assertFalse(os.path.exists(state))
        self.assertEqual(window._seq, 0)

    def test_non_list_raises_type_error_without_creating_state(self):
        state = os.path.join(self.dir, "window")
        window = Window(state, 10, 3)
        for bad in (("a",), "ab", None, 1, True, {"a": 1}):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_many(bad)
        self.assertFalse(os.path.exists(state))

    def test_non_string_element_raises_type_error(self):
        window = Window(self.dir, 10, 3)
        for bad in (["a", 1], [None], [True], [1.0], [["a"]], [{"a": 1}]):
            with self.assertRaises(TypeError, msg=repr(bad)):
                window.observe_many(bad)
        self.assertFalse(os.path.exists(os.path.join(self.dir, "window.json")))

    def test_batch_uses_batch_start_time_without_advancing(self):
        window = Window(self.dir, 100, 10)
        window.advance(8)
        self.assertEqual(window.observe_many(["b", "c"]), [True, True])
        snapshot = window.export(window._seq)
        self.assertEqual(snapshot["now"], 8)
        self.assertEqual(snapshot["keys"], [["b", 8], ["c", 8]])

    def test_duplicate_in_batch_does_not_refresh_time_or_order(self):
        window = Window(self.dir, 10, 3)
        window.observe("a")
        window.advance(5)
        window.observe("b")
        self.assertEqual(window.observe_many(["a", "c"]), [False, True])
        self.assertEqual(window.keys(), ["a", "b", "c"])
        self.assertEqual(window.advance(11), 1)  # "a" still expires from time 0
        self.assertEqual(window.keys(), ["b", "c"])

    def test_accepting_batch_adds_exactly_one_commit(self):
        window = Window(self.dir, 100000, 100000)
        window.observe("a")
        seq = window._seq
        window.observe_many(["b", "c", "d", "b"])
        self.assertEqual(window._seq, seq + 1)

    def test_all_duplicate_batch_commits_nothing(self):
        window = Window(self.dir, 10, 5)
        window.observe("a")
        window.observe("b")
        seq = window._seq
        self.assertEqual(window.observe_many(["a", "b", "a"]),
                         [False, False, False])
        self.assertEqual(window._seq, seq)

    def test_evicted_in_batch_can_be_readmitted(self):
        window = Window(self.dir, 100, 2)
        window.observe("a")
        # b fills the window; c evicts a; a comes back and is admitted again.
        self.assertEqual(window.observe_many(["b", "c", "a"]),
                         [True, True, True])
        self.assertEqual(window.keys(), ["c", "a"])

    def test_intra_batch_churn_counts_every_admission(self):
        window = Window(self.dir, 100, 2)
        window.observe("a")
        # b: (a,b); c evicts a -> (b,c); d evicts b -> (c,d); b re-admitted.
        self.assertEqual(window.observe_many(["b", "c", "d", "b"]),
                         [True, True, True, True])
        self.assertEqual(window.keys(), ["d", "b"])
        self.assertEqual(window.stats()["admitted"], 5)
        self.assertEqual(window.stats()["expired"], 0)

    def test_batch_persists_and_reopens(self):
        window = Window(self.dir, 100, 4)
        window.observe_many(["a", "b", "a", "c"])
        clone = Window(self.dir, 100, 4)
        clone.load()
        self.assertEqual(clone.keys(), ["a", "b", "c"])
        self.assertEqual(clone.stats(), window.stats())

    def test_batch_segment_is_one_delta_with_gained(self):
        path = os.path.join(self.dir, "window.json")
        window = Window(self.dir, 100, 2)
        window.observe("a")
        window.observe_many(["b", "c", "b"])
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        segment = json.loads(lines[-1])
        self.assertEqual((segment["kind"], segment["op"]), ("delta", "batch"))
        self.assertEqual(segment["gained"], 2)  # b and c admitted; b repeat false

    def test_corrupt_state_makes_batch_fail_without_overwriting(self):
        os.makedirs(self.dir, exist_ok=True)
        path = os.path.join(self.dir, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{broken")
        with open(path, "rb") as fh:
            before = fh.read()
        with self.assertRaises(ValueError):
            Window(self.dir, 60, 1024).observe_many(["a"])
        with open(path, "rb") as fh:
            self.assertEqual(fh.read(), before)

    def test_settings_mismatch_makes_batch_fail(self):
        Window(self.dir, 10, 3).observe("z")
        with self.assertRaises(ValueError):
            Window(self.dir, 99, 3).observe_many(["a"])

    def test_restore_then_batch_continues_numbering(self):
        source = Window(tempfile.mkdtemp(), 100, 10)
        source.observe("a")
        source.observe_many(["b", "c"])
        doc = source.export(source._seq)
        target = Window(self.dir, 1, 1)
        target.restore(doc)
        target.observe_many(["d"])
        self.assertEqual(target.export(doc["seq"] + 1)["seq"], doc["seq"] + 1)
        self.assertEqual(target.export(doc["seq"]), doc)


class ObserveBatchCliTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_cli(self, *args, stdin=None):
        return subprocess.run(
            [sys.executable, "-m", "dedupe_window", *args],
            capture_output=True, text=True, input=stdin,
        )

    def test_success_writes_one_compact_boolean_array(self):
        state = os.path.join(self.dir, "w")
        result = self.run_cli("--state", state, "observe-batch",
                              stdin=json.dumps(["b", "b", "c", "a"]))
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "[true,false,true,true]\n")
        self.assertEqual(result.stderr, "")

    def test_surrounding_whitespace_is_allowed(self):
        state = os.path.join(self.dir, "w")
        result = self.run_cli("--state", state, "observe-batch",
                              stdin='  \n\t' + json.dumps(["a"]) + '\n  \t')
        self.assertEqual((result.returncode, result.stdout), (0, "[true]\n"))

    def test_empty_array_is_success_and_creates_nothing(self):
        for payload in ("[]", "  [ ]  \n"):
            state = os.path.join(self.dir, "never-" + str(len(payload)))
            result = self.run_cli("--state", state, "observe-batch", stdin=payload)
            self.assertEqual((result.returncode, result.stdout), (0, "[]\n"))
            self.assertFalse(os.path.exists(state))

    def test_bad_inputs_exit_1_change_nothing(self):
        cases = {
            "empty": "",
            "whitespace": "  \n\t",
            "not json": "{nope",
            "object": "{}",
            "string": json.dumps("ab"),
            "number": "42",
            "int element": json.dumps(["a", 1]),
            "null element": json.dumps([None]),
            "bool element": json.dumps([True]),
            "nested": json.dumps([["a"]]),
            "two arrays": json.dumps(["a"]) + json.dumps(["b"]),
            "trailing junk": json.dumps(["a"]) + "x",
        }
        for name, payload in cases.items():
            state = os.path.join(self.dir, "case-" + name.replace(" ", "-"))
            result = self.run_cli("--state", state, "observe-batch", stdin=payload)
            self.assertEqual(result.returncode, 1, name)
            self.assertEqual(result.stdout, "", name)
            self.assertTrue(result.stderr.startswith("batch failed"), name)
            self.assertEqual(len(result.stderr.strip().splitlines()), 1, name)
            self.assertFalse(os.path.exists(state), name)

    def test_usage_error_exits_2(self):
        state = os.path.join(self.dir, "w")
        result = self.run_cli("--state", state, "observe-batch", "extra")
        self.assertEqual(result.returncode, 2)
        self.assertIn("usage", result.stderr.lower())

    def test_corrupt_state_exits_1_and_is_untouched(self):
        state = os.path.join(self.dir, "broken")
        os.makedirs(state)
        path = os.path.join(state, "window.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("{broken")
        with open(path, "rb") as fh:
            before = fh.read()
        result = self.run_cli("--state", state, "observe-batch",
                              stdin=json.dumps(["a"]))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.startswith("batch failed"))
        with open(path, "rb") as fh:
            self.assertEqual(fh.read(), before)

    def test_batches_persist_between_invocations(self):
        state = os.path.join(self.dir, "w")
        first = self.run_cli("--state", state, "observe-batch",
                             stdin=json.dumps(["b", "b", "c", "a"]))
        self.assertEqual(first.returncode, 0)
        second = self.run_cli("--state", state, "observe-batch",
                              stdin=json.dumps(["a", "z"]))
        self.assertEqual(second.stdout, "[false,true]\n")
        stats = self.run_cli("--state", state, "stats")
        self.assertEqual(
            json.loads(stats.stdout),
            {"span": 60, "capacity": 1024, "retained": 4,
             "admitted": 4, "expired": 0},
        )
        exported = self.run_cli("--state", state, "export", "2")
        self.assertEqual(exported.returncode, 0)
        self.assertEqual([k for k, _ in json.loads(exported.stdout)["keys"]],
                         ["b", "c", "a", "z"])


if __name__ == "__main__":
    unittest.main()
