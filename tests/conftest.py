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
    added = scheduler._scheduling_orchestrator.poll_and_enqueue()

    if added == 0:
        raise AssertionError(
            f"Job {job_id} was not added to queue. "
            f"Status: {job.get('status')}, next_run_time: {job.get('next_run_time')}"
        )

    # Get from queue and execute
    job_data = scheduler._scheduling_orchestrator.get_next_job_from_queue()

    if job_data is None:
        raise AssertionError("No job in queue")

    if job_data["job_id"] != job_id:
        raise AssertionError(f"Expected job {job_id}, got {job_data['job_id']}")

    # Execute (this submits to thread pool and returns immediately)
    success = scheduler._execution_coordinator.try_execute(job_data, lambda job_id: None)

    if not success:
        raise AssertionError(f"Failed to acquire lock for job {job_id}")

    # Wait for background execution to complete
    time.sleep(wait_seconds)

    # Return updated job data
    return scheduler.storage.get_job(job_id)


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
