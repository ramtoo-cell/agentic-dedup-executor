'''Tests for the deduplicating executor.'''

import threading
import time
import unittest

from src.dedup_executor import DedupExecutor


class DedupExecutorTests(unittest.TestCase):
    def test_same_key_single_execution(self):
        ex = DedupExecutor()
        calls = []
        release = threading.Barrier(6)
        proceed = threading.Event()

        def compute():
            # the owner blocks here, so every waiter is guaranteed to
            # attach to the in-flight future before it resolves
            calls.append(1)
            proceed.wait(5)
            return "v"

        def worker(out, i):
            release.wait()
            out[i] = ex.submit("k", compute)

        out = [None] * 6
        threads = [threading.Thread(target=worker, args=(out, i))
                   for i in range(6)]
        for t in threads:
            t.start()
        deadline = time.monotonic() + 5
        while not calls and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(calls, "owner never started computing")
        time.sleep(0.5)  # let the five waiters attach to the future
        proceed.set()
        for t in threads:
            t.join()
        self.assertEqual(len(calls), 1)

    def test_different_keys_independent(self):
        ex = DedupExecutor()
        self.assertEqual(ex.submit("a", lambda: 1), 1)
        self.assertEqual(ex.submit("b", lambda: 2), 2)

    def test_error_shared_with_waiters(self):
        ex = DedupExecutor()
        barrier = threading.Barrier(2)
        errors = []

        def boom():
            raise RuntimeError("boom")

        def worker():
            barrier.wait()
            try:
                ex.submit("k", boom)
            except RuntimeError as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, ["boom", "boom"])

    def test_failure_cached_briefly(self):
        ex = DedupExecutor(failure_ttl=60.0)
        calls = []

        def fail():
            calls.append(1)
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            ex.submit("k", fail)
        with self.assertRaises(RuntimeError):
            ex.submit("k", fail)
        self.assertEqual(len(calls), 1)






class DedupExecutorExtraTests(unittest.TestCase):
    def test_clear_failures(self):
        ex = DedupExecutor()
        def boom():
            raise RuntimeError("down")
        with self.assertRaises(RuntimeError):
            ex.submit("k", boom)
        self.assertEqual(ex.failure_count(), 1)
        self.assertEqual(ex.clear_failures(), 1)
        self.assertEqual(ex.failure_count(), 0)
        self.assertEqual(ex.submit("k", lambda: "ok"), "ok")





if __name__ == "__main__":
    unittest.main()
