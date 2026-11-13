"""Duplicate-key merging."""

import threading


class DedupRegistry:
    """Tracks keys already seen by the scheduler.

    A key is registered the first time it is submitted; any later
    submission of the same key (while the scheduler is alive) is merged
    into the first one and counted, so a key never runs twice.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._seen = set()
        self._merged_per_key = {}
        self.merged_count = 0

    def try_register(self, key: str) -> bool:
        """Return True if key is new, False (and count a merge) if known."""
        with self._lock:
            if key in self._seen:
                self._merged_per_key[key] = self._merged_per_key.get(key, 0) + 1
                self.merged_count += 1
                return False
            self._seen.add(key)
            return True

    def merged_for(self, key: str) -> int:
        with self._lock:
            return self._merged_per_key.get(key, 0)
