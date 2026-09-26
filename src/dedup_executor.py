'''Deduplicating executor for agent tool calls.

When several agents (or retries) fire the same tool call concurrently,
only one execution should happen.  ``submit`` registers a call under an
idempotency key: the first submitter executes, late submitters with the
same key block on the same future and receive the same result.

``submit_batch`` collects calls for a short window and executes the
unique keys once each.  Failures are cached briefly so a retry storm
does not hammer a failing tool; successes are not cached (pair with a
result cache for that).
'''

from __future__ import annotations

import threading
import time


class _Future:
    def __init__(self):
        self._event = threading.Event()
        self._value = None
        self._error = None

    def set(self, value=None, error=None):
        self._value, self._error = value, error
        self._event.set()

    def get(self, timeout=None):
        if not self._event.wait(timeout):
            raise TimeoutError("dedup future timed out")
        if self._error is not None:
            raise self._error
        return self._value


class DedupExecutor:
    '''Runs each idempotency key at most once concurrently.'''

    def __init__(self, failure_ttl=1.0):
        self.failure_ttl = failure_ttl
        self._inflight = {}
        self._failures = {}  # key -> (error, recorded_at)
        self._lock = threading.Lock()

    def _recent_failure(self, key):
        entry = self._failures.get(key)
        if entry is None:
            return None
        error, at = entry
        if time.monotonic() - at < self.failure_ttl:
            return error
        del self._failures[key]
        return None

    def submit(self, key, fn, timeout=None):
        '''Execute fn under key; concurrent same-key calls share it.'''
        with self._lock:
            failure = self._recent_failure(key)
            if failure is not None:
                raise failure
            future = self._inflight.get(key)
            if future is None:
                future = _Future()
                self._inflight[key] = future
                owner = True
            else:
                owner = False
        if owner:
            try:
                future.set(value=fn())
            except Exception as exc:  # noqa: BLE001 - share with waiters
                with self._lock:
                    self._failures[key] = (exc, time.monotonic())
                future.set(error=exc)
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
        return future.get(timeout=timeout)

    def submit_batch(self, calls, window=0.05):
        '''calls: [(key, fn)]. Unique keys execute once each.'''
        deadline = time.monotonic() + window
        pending = list(calls)
        results, errors = {}, {}
        while pending and time.monotonic() < deadline:
            key, fn = pending.pop(0)
            if key in results or key in errors:
                continue
            try:
                results[key] = self.submit(key, fn, timeout=window)
            except Exception as exc:  # noqa: BLE001 - per-key errors
                errors[key] = exc
        return results, errors

    def inflight(self):
        with self._lock:
            return len(self._inflight)

    def clear_failures(self):
        '''Drop cached failure records. Returns the count cleared.'''
        with self._lock:
            count = len(self._failures)
            self._failures.clear()
            return count

    def failure_count(self):
        with self._lock:
            return len(self._failures)

    def keys_inflight(self):
        '''Idempotency keys currently executing.'''
        with self._lock:
            return sorted(self._inflight)

    def submit_all(self, calls, timeout=None):
        '''calls: [(key, fn)] -> {key: result}; raises on first error.'''
        return {key: self.submit(key, fn, timeout=timeout)
                for key, fn in calls}

    def has_failure(self, key):
        '''True when a recent (unexpired) failure is cached for key.'''
        with self._lock:
            return self._recent_failure(key) is not None
