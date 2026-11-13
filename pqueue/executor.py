"""Concurrent worker pool executing scheduled tasks."""

import threading
import time

from .dedup import DedupRegistry
from .policy import check_scope
from .queue import BoundedPriorityQueue
from .report import build_report

DRAIN = "drain"
DROP = "drop"


class Scheduler:
    """Priority-aware, deduplicating, scope-enforcing task scheduler.

    workers:        max number of concurrent worker threads.
    queue_size:     bounded queue capacity (backpressure kicks in when full).
    backpressure:   'block' (producer waits, counted as backoff) or
                    'reject' (submission refused, counted as rejected).
    shutdown_mode:  'drain' (finish queued tasks by priority) or
                    'drop' (discard queued tasks and count them).
    """

    def __init__(self, workers=4, queue_size=100, backpressure="block",
                 shutdown_mode=DRAIN):
        if workers <= 0:
            raise ValueError("workers must be positive")
        if shutdown_mode not in (DRAIN, DROP):
            raise ValueError("shutdown_mode must be 'drain' or 'drop'")
        self._workers = workers
        self._shutdown_mode = shutdown_mode
        self._queue = BoundedPriorityQueue(queue_size, backpressure)
        self._dedup = DedupRegistry()
        self._threads = []
        self._executed = []
        self._blocked = []
        self._state_lock = threading.Lock()
        self._submit_lock = threading.Lock()
        self._concurrency = 0
        self._closed = False
        self.peak_concurrency = 0
        self.accepted_count = 0
        self.rejected_count = 0
        self.dropped_count = 0

    def submit(self, task) -> str:
        """Submit a task. Returns 'accepted', 'merged', 'rejected' or 'closed'."""
        with self._submit_lock:
            if self._closed:
                return "closed"
            if not self._dedup.try_register(task.key):
                return "merged"
            result = self._queue.put(task.priority, task)
            if result == "accepted":
                self.accepted_count += 1
            elif result == "rejected":
                self.rejected_count += 1
            return result

    def start(self) -> None:
        for _ in range(self._workers):
            thread = threading.Thread(target=self._worker, daemon=True)
            thread.start()
            self._threads.append(thread)

    def close(self) -> None:
        """Stop accepting new tasks; finish or drop queued ones per config."""
        with self._submit_lock:
            self._closed = True
        if self._shutdown_mode == DROP:
            dropped = self._queue.drain()
            self.dropped_count += len(dropped)
        self._queue.close()

    def join(self) -> None:
        for thread in self._threads:
            thread.join()

    def run(self, tasks):
        """Convenience: start, submit all, close, join, return the report."""
        self.start()
        for task in tasks:
            self.submit(task)
        self.close()
        self.join()
        return self.report()

    def report(self) -> dict:
        with self._state_lock:
            return build_report(
                executed=list(self._executed),
                merged_count=self._dedup.merged_count,
                rejected_count=self.rejected_count,
                blocked_requests=list(self._blocked),
                backoff_events=self._queue.backoff_events,
                peak_concurrency=self.peak_concurrency,
                accepted_count=self.accepted_count,
                dropped_count=self.dropped_count,
            )

    def _worker(self) -> None:
        while True:
            task = self._queue.get()
            if task is None:
                return
            self._execute(task)

    def _execute(self, task) -> None:
        with self._state_lock:
            self._concurrency += 1
            if self._concurrency > self.peak_concurrency:
                self.peak_concurrency = self._concurrency
        start = time.perf_counter()
        try:
            reason = check_scope(task)
            if reason is not None:
                status = "blocked"
                with self._state_lock:
                    self._blocked.append({
                        "task_id": task.id,
                        "url": task.url,
                        "reason": reason,
                    })
            else:
                self._deliver(task)
                status = "ok"
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            with self._state_lock:
                self._concurrency -= 1
                self._executed.append({
                    "id": task.id,
                    "key": task.key,
                    "status": status,
                    "duration_ms": duration_ms,
                })

    def _deliver(self, task) -> None:
        """Simulated request delivery. Only ever called after the scope
        check passed, so an out-of-scope request is never sent."""
        sleep_ms = task.payload.get("sleep_ms", 1)
        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)
