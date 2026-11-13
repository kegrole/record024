"""Bounded thread-safe priority queue with configurable backpressure."""

import heapq
import itertools
import threading


class BoundedPriorityQueue:
    """Priority queue with a capacity limit.

    Higher priority values dequeue first; ties break by insertion order.
    When full, producers either block ("block" strategy, counted as a
    backoff event) or are rejected ("reject" strategy).
    """

    def __init__(self, capacity: int, backpressure: str = "block"):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if backpressure not in ("block", "reject"):
            raise ValueError("backpressure must be 'block' or 'reject'")
        self._capacity = capacity
        self._backpressure = backpressure
        self._heap = []
        self._seq = itertools.count()
        self._cond = threading.Condition()
        self._closed = False
        self.backoff_events = 0

    def put(self, priority: int, item) -> str:
        """Enqueue item. Returns 'accepted', 'rejected' or 'closed'."""
        with self._cond:
            if self._closed:
                return "closed"
            if len(self._heap) >= self._capacity:
                if self._backpressure == "reject":
                    return "rejected"
                self.backoff_events += 1
                while len(self._heap) >= self._capacity and not self._closed:
                    self._cond.wait()
                if self._closed:
                    return "closed"
            heapq.heappush(self._heap, (-priority, next(self._seq), item))
            self._cond.notify_all()
            return "accepted"

    def get(self):
        """Dequeue the highest-priority item, or None when closed and empty."""
        with self._cond:
            while not self._heap:
                if self._closed:
                    return None
                self._cond.wait()
            _, _, item = heapq.heappop(self._heap)
            self._cond.notify_all()
            return item

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def drain(self) -> list:
        """Remove and return all remaining items in dequeue order."""
        with self._cond:
            items = []
            while self._heap:
                items.append(heapq.heappop(self._heap)[2])
            self._cond.notify_all()
            return items

    def __len__(self) -> int:
        with self._cond:
            return len(self._heap)
