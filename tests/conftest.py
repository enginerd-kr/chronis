"""Common test fixtures and utilities."""

import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import pytest

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler


def wait_for(
    condition: Callable[[], bool],
    timeout: float = 10.0,
    interval: float = 0.1,
    error_message: str | None = None,
) -> bool:
    """
    Wait until condition is True, polling at interval (eventually pattern).

    Similar to Kotest's eventually, this polls a condition until it becomes true
    or timeout is reached.

    Args:
        condition: Function that returns bool
        timeout: Maximum wait time in seconds
        interval: Polling interval in seconds
        error_message: Custom error message if timeout

    Returns:
        True if condition met

    Raises:
        AssertionError: If timeout exceeded

    Example:
        wait_for(lambda: len(executions) >= 2, timeout=10)
    """
    start = time.time()
    last_exception = None

    while time.time() - start < timeout:
        try:
            if condition():
                return True
        except Exception as e:
            # Store exception to report if timeout
            last_exception = e
        time.sleep(interval)

    # Build detailed error message
    elapsed = time.time() - start
    if error_message is None:
        error_message = f"Condition not met within {timeout}s (elapsed: {elapsed:.2f}s)"

    if last_exception:
        error_message += f"\nLast exception: {last_exception}"

    raise AssertionError(error_message)


def eventually(
    assertion_fn: Callable[[], None],
    timeout: float = 10.0,
    interval: float = 0.1,
) -> None:
    """
    Kotest-style eventually assertion.

    Repeatedly calls assertion_fn until it passes (no exception) or timeout.

    Args:
        assertion_fn: Function containing assertions (raises AssertionError if fails)
        timeout: Maximum wait time in seconds
        interval: Polling interval in seconds

    Raises:
        AssertionError: If assertions never pass within timeout

    Example:
        def check():
            assert len(executions) >= 2
            assert executions[0] < executions[1]

        eventually(check, timeout=10)
    """
    start = time.time()
    last_error = None

    while time.time() - start < timeout:
        try:
            assertion_fn()
            return  # Success
        except AssertionError as e:
            last_error = e
            time.sleep(interval)

    # Timeout - raise last error
    elapsed = time.time() - start
    if last_error:
        raise AssertionError(
            f"Assertions never passed within {timeout}s (elapsed: {elapsed:.2f}s)\n"
            f"Last assertion error: {last_error}"
        )
    else:
        raise AssertionError(f"No assertions passed within {timeout}s")


def execute_job_immediately(
    scheduler: PollingScheduler,
    job_id: str,
    wait_seconds: float = 0.5,
) -> dict[str, Any] | None:
    """
    Execute a job immediately by directly calling scheduler methods.

    This bypasses APScheduler background threads for deterministic testing.

    Args:
        scheduler: PollingScheduler instance
        job_id: Job ID to execute
        wait_seconds: Time to wait for background execution to complete

    Returns:
        Updated job data from storage, or None if deleted
    """
    from datetime import UTC

    job = scheduler.storage.get_job(job_id)
    if job is None:
        raise AssertionError(f"Job {job_id} not found in storage")

    # Make job ready by setting next_run_time to past
    scheduler.storage.update_job(
        job_id, {"next_run_time": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()}
    )

    # Poll and enqueue
    added = scheduler._enqueue_jobs()

    if added == 0:
        raise AssertionError(
            f"Job {job_id} was not added to queue. "
            f"Status: {job.get('status')}, next_run_time: {job.get('next_run_time')}"
        )

    # Get from queue and execute
    # OPTIMIZED: Queue now returns only job_id, fetch data from storage
    queued_job_id = scheduler._job_queue.get_next_job()

    if queued_job_id is None:
        raise AssertionError("No job in queue")

    if queued_job_id != job_id:
        raise AssertionError(f"Expected job {job_id}, got {queued_job_id}")

    # Fetch job data from storage
    job_data = scheduler.storage.get_job(queued_job_id)
    if job_data is None:
        raise AssertionError(f"Job {queued_job_id} not found in storage")

    # Execute (this submits to thread pool and returns immediately)
    success = scheduler._execution_coordinator.try_execute(job_data, lambda job_id: None)

    if not success:
        raise AssertionError(f"Failed to acquire lock for job {job_id}")

    # Wait for background execution to complete
    time.sleep(wait_seconds)

    # Return updated job data
    return scheduler.storage.get_job(job_id)


class ExecutionTracker:
    """
    Helper class to track job executions with timestamps and order.

    Example:
        tracker = ExecutionTracker()
        scheduler.register_job_function("job1", lambda: tracker.record("job1"))
        # ... execute jobs ...
        assert tracker.count("job1") == 2
        assert tracker.order() == ["job1", "job2", "job1"]
    """

    def __init__(self):
        """Initialize empty execution tracking."""
        self._executions = []
        self._timestamps = []

    def record(self, job_id: str) -> None:
        """Record an execution with current timestamp."""
        import time

        self._executions.append(job_id)
        self._timestamps.append(time.time())

    def count(self, job_id: str | None = None) -> int:
        """
        Count executions.

        Args:
            job_id: If provided, count only this job's executions.
                   If None, count all executions.

        Returns:
            Execution count
        """
        if job_id is None:
            return len(self._executions)
        return self._executions.count(job_id)

    def order(self) -> list[str]:
        """Get execution order as list of job IDs."""
        return self._executions.copy()

    def clear(self) -> None:
        """Clear all recorded executions."""
        self._executions.clear()
        self._timestamps.clear()

    def last_timestamp(self, job_id: str | None = None) -> float | None:
        """
        Get timestamp of last execution.

        Args:
            job_id: If provided, get last timestamp for this job.
                   If None, get last timestamp overall.

        Returns:
            Timestamp or None if no executions
        """
        if job_id is None:
            return self._timestamps[-1] if self._timestamps else None

        # Find last occurrence of job_id
        for i in range(len(self._executions) - 1, -1, -1):
            if self._executions[i] == job_id:
                return self._timestamps[i]
        return None


def register_dummy_job(scheduler: PollingScheduler, func_name: str = "dummy") -> None:
    """
    Register a dummy no-op function for testing.

    Args:
        scheduler: Scheduler instance
        func_name: Function name (default: "dummy")

    Example:
        register_dummy_job(scheduler)
        scheduler.create_interval_job(func="dummy", seconds=30)
    """

    def dummy():
        pass

    scheduler.register_job_function(func_name, dummy)


def assert_job_in_storage(
    scheduler: PollingScheduler,
    job_id: str,
    status: str | None = None,
) -> dict[str, Any]:
    """
    Assert job exists in storage and optionally check status.

    Args:
        scheduler: Scheduler instance
        job_id: Job ID to check
        status: Expected status (optional)

    Returns:
        Job data from storage

    Raises:
        AssertionError: If job not found or status mismatch

    Example:
        job_data = assert_job_in_storage(scheduler, "job-1", status="scheduled")
    """
    stored = scheduler.storage.get_job(job_id)
    assert stored is not None, f"Job {job_id} not found in storage"

    if status is not None:
        actual_status = stored.get("status")
        assert actual_status == status, (
            f"Job {job_id} status mismatch: expected {status}, got {actual_status}"
        )

    return stored


@pytest.fixture
def execution_tracker():
    """
    Fixture providing ExecutionTracker instance.

    Example:
        def test_execution(execution_tracker):
            def job():
                execution_tracker.record("job1")
            # ... execute jobs ...
            assert execution_tracker.count() == 1
    """
    return ExecutionTracker()


@pytest.fixture
def fast_scheduler():
    """
    Create scheduler with fast intervals for testing.

    Does NOT start the scheduler - tests should call methods directly.
    """
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,
        executor_interval_seconds=0.5,
        verbose=False,
    )
    yield scheduler
    if scheduler.is_running():
        scheduler.stop()


@pytest.fixture
def basic_scheduler():
    """
    Create a basic scheduler without starting it.

    Similar to fast_scheduler but with default intervals.
    Use this when you need a fresh scheduler for each test.
    """
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
    )
    yield scheduler
    if scheduler.is_running():
        scheduler.stop()


@pytest.fixture
def sample_job_data():
    """
    Create sample job data dict for testing storage operations.

    Returns:
        Dict with all required job fields

    Example:
        def test_create(storage, sample_job_data):
            storage.create_job(sample_job_data)
    """
    from chronis.core.state import JobStatus
    from chronis.core.state.enums import TriggerType
    from chronis.utils.time import utc_now

    return {
        "job_id": "test-job-1",
        "name": "Test Job",
        "trigger_type": TriggerType.INTERVAL.value,
        "trigger_args": {"seconds": 30},
        "timezone": "UTC",
        "status": JobStatus.SCHEDULED.value,
        "next_run_time": utc_now().isoformat(),
        "next_run_time_local": utc_now().isoformat(),
        "metadata": {},
        "created_at": utc_now().isoformat(),
        "updated_at": utc_now().isoformat(),
    }
