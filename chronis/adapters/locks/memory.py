"""In-memory lock adapter for testing."""

import threading
import time

from chronis.adapters.base import LockAdapter


class InMemoryLockAdapter(LockAdapter):
    """In-memory lock adapter (for local development/testing)."""

    def __init__(self) -> None:
        self._locks: dict[str, float] = {}  # lock_key -> expiry_time
        self._mutex = threading.Lock()

    def acquire(self, lock_key: str, ttl_seconds: int, blocking: bool = False) -> bool:
        """Acquire in-memory lock (TTL simulation)."""
        with self._mutex:
            current_time = time.time()
            # Clean up expired locks
            if lock_key in self._locks and self._locks[lock_key] < current_time:
                del self._locks[lock_key]

            # Try to acquire lock
            if lock_key not in self._locks:
                self._locks[lock_key] = current_time + ttl_seconds
                return True
            return False

    def release(self, lock_key: str) -> bool:
        """Release in-memory lock."""
        with self._mutex:
            if lock_key in self._locks:
                del self._locks[lock_key]
                return True
            return False
