"""Shared fixtures and utilities for E2E tests."""

import threading

import pytest

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler


class ThreadSafeCounter:
    """Thread-safe execution counter for E2E tests.

    This replaces the pattern:
        execution_count = {"count": 0}
        count_lock = threading.Lock()

        def func():
            with count_lock:
                execution_count["count"] += 1

    Usage:
        counter = ThreadSafeCounter()

        def func():
            counter.increment()

        # Later
        assert counter.value >= 2
    """

    def __init__(self):
        """Initialize counter with lock."""
        self._count = 0
        self._lock = threading.Lock()

    def increment(self):
        """Increment counter in thread-safe manner."""
        with self._lock:
            self._count += 1

    @property
    def value(self) -> int:
        """Get current count value."""
        with self._lock:
            return self._count

    def reset(self):
        """Reset counter to zero."""
        with self._lock:
            self._count = 0


class ThreadSafeExecutionTracker:
    """Thread-safe execution tracker with per-job counting.

    Usage:
        tracker = ThreadSafeExecutionTracker()

        def func(job_id: str):
            tracker.record(job_id)

        # Later
        assert tracker.count("job_1") == 2
        assert tracker.total_count() == 5
    """

    def __init__(self):
        """Initialize tracker with lock."""
        self._counts: dict[str, int] = {}
        self._timestamps: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def record(self, job_id: str):
        """Record execution for a job."""
        import time

        with self._lock:
            self._counts[job_id] = self._counts.get(job_id, 0) + 1
            if job_id not in self._timestamps:
                self._timestamps[job_id] = []
            self._timestamps[job_id].append(time.time())

    def count(self, job_id: str) -> int:
        """Get execution count for specific job."""
        with self._lock:
            return self._counts.get(job_id, 0)

    def total_count(self) -> int:
        """Get total execution count across all jobs."""
        with self._lock:
            return sum(self._counts.values())

    def job_count(self) -> int:
        """Get number of unique jobs executed."""
        with self._lock:
            return len(self._counts)

    def get_timestamps(self, job_id: str) -> list[float]:
        """Get all execution timestamps for a job."""
        with self._lock:
            return self._timestamps.get(job_id, []).copy()


@pytest.fixture
def thread_safe_counter():
    """Provide a thread-safe counter for E2E tests."""
    return ThreadSafeCounter()


@pytest.fixture
def thread_safe_tracker():
    """Provide a thread-safe execution tracker for E2E tests."""
    return ThreadSafeExecutionTracker()


@pytest.fixture
def distributed_schedulers():
    """Create multiple schedulers with shared storage/lock.

    Returns:
        tuple: (schedulers, storage, lock) where schedulers is a list

    Usage:
        def test_distributed(distributed_schedulers):
            schedulers, storage, lock = distributed_schedulers

            # Register function on all schedulers
            for scheduler in schedulers:
                scheduler.register_job_function("test_func", test_func)

            # Create job on first scheduler (shared storage)
            schedulers[0].create_interval_job(...)

            # Start all schedulers
            for scheduler in schedulers:
                scheduler.start()
    """
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    schedulers = []
    for _ in range(3):
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            lock_ttl_seconds=5,
            verbose=False,
        )
        schedulers.append(scheduler)

    yield schedulers, storage, lock

    # Cleanup
    for scheduler in schedulers:
        if scheduler.is_running():
            scheduler.stop()


def cleanup_schedulers(schedulers: list[PollingScheduler]) -> None:
    """Stop all running schedulers.

    Args:
        schedulers: List of scheduler instances

    Usage:
        try:
            # ... test code ...
        finally:
            cleanup_schedulers(schedulers)
    """
    for scheduler in schedulers:
        if scheduler.is_running():
            scheduler.stop()
