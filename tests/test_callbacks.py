"""Tests for on_failure and on_success callback functionality."""

import time
from threading import Event
from typing import Any

import pytest

from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.jobs.definition import JobInfo
from chronis.core.scheduler import PollingScheduler


class CallbackTracker:
    """Helper class to track callback invocations."""

    def __init__(self) -> None:
        self.failures: list[dict[str, Any]] = []
        self.successes: list[dict[str, Any]] = []
        self.failure_event = Event()
        self.success_event = Event()

    def record_failure(self, job_id: str, error: Exception, job_info: JobInfo) -> None:
        """Record a failure for testing."""
        self.failures.append({"job_id": job_id, "error": error, "job_info": job_info})
        self.failure_event.set()

    def record_success(self, job_id: str, job_info: JobInfo) -> None:
        """Record a success for testing."""
        self.successes.append({"job_id": job_id, "job_info": job_info})
        self.success_event.set()

    def wait_for_failure(self, timeout: float = 5.0) -> bool:
        """Wait for at least one failure to be recorded."""
        return self.failure_event.wait(timeout)

    def wait_for_success(self, timeout: float = 5.0) -> bool:
        """Wait for at least one success to be recorded."""
        return self.success_event.wait(timeout)

    def clear(self) -> None:
        """Clear recorded callbacks."""
        self.failures.clear()
        self.successes.clear()
        self.failure_event.clear()
        self.success_event.clear()


@pytest.fixture
def storage():
    """Create in-memory storage adapter."""
    return InMemoryStorageAdapter()


@pytest.fixture
def scheduler_with_global_handlers(storage):
    """Create scheduler with global failure and success handlers."""
    tracker = CallbackTracker()

    def global_failure_handler(job_id: str, error: Exception, job_info: JobInfo) -> None:
        tracker.record_failure(job_id, error, job_info)

    def global_success_handler(job_id: str, job_info: JobInfo) -> None:
        tracker.record_success(job_id, job_info)

    from chronis.adapters.lock import InMemoryLockAdapter

    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=InMemoryLockAdapter(),
        polling_interval_seconds=1,
        lock_ttl_seconds=10,
        on_failure=global_failure_handler,
        on_success=global_success_handler,
    )

    scheduler.tracker = tracker  # Attach tracker for test access
    return scheduler


@pytest.fixture
def scheduler_without_global_handlers(storage):
    """Create scheduler without global handlers."""
    from chronis.adapters.lock import InMemoryLockAdapter

    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=InMemoryLockAdapter(),
        polling_interval_seconds=1,
        lock_ttl_seconds=10,
    )
    return scheduler


def failing_job():
    """Job that always fails."""
    raise ValueError("Intentional failure for testing")


def successful_job():
    """Job that always succeeds."""
    pass


# ============================================================================
# Failure Handler Tests
# ============================================================================


def test_global_failure_handler_is_invoked(scheduler_with_global_handlers):
    """Test that global on_failure handler is invoked when job fails."""
    scheduler = scheduler_with_global_handlers
    tracker: CallbackTracker = scheduler.tracker

    # Register failing job with full module path
    func_name = f"{failing_job.__module__}.{failing_job.__name__}"
    scheduler.register_job_function(func_name, failing_job)

    # Create job
    job = scheduler.create_interval_job(func=failing_job, seconds=1)

    # Start scheduler
    scheduler.start()

    try:
        # Wait for failure
        assert tracker.wait_for_failure(timeout=5.0), "Global failure handler was not invoked"

        # Verify failure was recorded
        assert len(tracker.failures) == 1
        failure = tracker.failures[0]
        assert failure["job_id"] == job.job_id
        assert isinstance(failure["error"], ValueError)
        assert str(failure["error"]) == "Intentional failure for testing"
        assert failure["job_info"].job_id == job.job_id

    finally:
        scheduler.stop()


def test_job_specific_and_global_failure_handlers_both_called(scheduler_without_global_handlers):
    """Test that both job-specific and global failure handlers are called."""
    scheduler = scheduler_without_global_handlers

    # Create two trackers
    global_tracker = CallbackTracker()
    job_specific_tracker = CallbackTracker()

    def global_handler(job_id: str, error: Exception, job_info: JobInfo) -> None:
        global_tracker.record_failure(job_id, error, job_info)

    def job_specific_handler(job_id: str, error: Exception, job_info: JobInfo) -> None:
        job_specific_tracker.record_failure(job_id, error, job_info)

    # Set global handler
    scheduler.on_failure = global_handler
    scheduler._execution_coordinator.global_on_failure = global_handler

    # Register failing job with full module path
    func_name = f"{failing_job.__module__}.{failing_job.__name__}"
    scheduler.register_job_function(func_name, failing_job)

    # Create job with job-specific handler
    job = scheduler.create_interval_job(
        func=failing_job, seconds=1, on_failure=job_specific_handler
    )

    # Start scheduler
    scheduler.start()

    try:
        # Wait for failure
        assert job_specific_tracker.wait_for_failure(timeout=5.0), (
            "Job-specific failure handler was not invoked"
        )

        # Verify both handlers were called
        assert len(job_specific_tracker.failures) == 1
        assert len(global_tracker.failures) == 1, "Global handler should also be invoked"

        # Verify job-specific handler details
        failure = job_specific_tracker.failures[0]
        assert failure["job_id"] == job.job_id
        assert isinstance(failure["error"], ValueError)

        # Verify global handler details
        global_failure = global_tracker.failures[0]
        assert global_failure["job_id"] == job.job_id
        assert isinstance(global_failure["error"], ValueError)

    finally:
        scheduler.stop()


# ============================================================================
# Success Handler Tests
# ============================================================================


def test_global_success_handler_is_invoked(scheduler_with_global_handlers):
    """Test that global on_success handler is invoked when job succeeds."""
    scheduler = scheduler_with_global_handlers
    tracker: CallbackTracker = scheduler.tracker

    # Register successful job with full module path
    func_name = f"{successful_job.__module__}.{successful_job.__name__}"
    scheduler.register_job_function(func_name, successful_job)

    # Create job
    job = scheduler.create_interval_job(func=successful_job, seconds=1)

    # Start scheduler
    scheduler.start()

    try:
        # Wait for success
        assert tracker.wait_for_success(timeout=5.0), "Global success handler was not invoked"

        # Verify success was recorded
        assert len(tracker.successes) >= 1
        success = tracker.successes[0]
        assert success["job_id"] == job.job_id
        assert success["job_info"].job_id == job.job_id

    finally:
        scheduler.stop()


def test_job_specific_and_global_success_handlers_both_called(scheduler_without_global_handlers):
    """Test that both job-specific and global success handlers are called."""
    scheduler = scheduler_without_global_handlers

    # Create two trackers
    global_tracker = CallbackTracker()
    job_specific_tracker = CallbackTracker()

    def global_handler(job_id: str, job_info: JobInfo) -> None:
        global_tracker.record_success(job_id, job_info)

    def job_specific_handler(job_id: str, job_info: JobInfo) -> None:
        job_specific_tracker.record_success(job_id, job_info)

    # Set global handler
    scheduler.on_success = global_handler
    scheduler._execution_coordinator.global_on_success = global_handler

    # Register successful job with full module path
    func_name = f"{successful_job.__module__}.{successful_job.__name__}"
    scheduler.register_job_function(func_name, successful_job)

    # Create job with job-specific handler
    job = scheduler.create_interval_job(
        func=successful_job, seconds=1, on_success=job_specific_handler
    )

    # Start scheduler
    scheduler.start()

    try:
        # Wait for success
        assert job_specific_tracker.wait_for_success(timeout=5.0), (
            "Job-specific success handler was not invoked"
        )

        # Verify both handlers were called
        assert len(job_specific_tracker.successes) >= 1
        assert len(global_tracker.successes) >= 1, "Global handler should also be invoked"

        # Verify job-specific handler details
        success = job_specific_tracker.successes[0]
        assert success["job_id"] == job.job_id

        # Verify global handler details
        global_success = global_tracker.successes[0]
        assert global_success["job_id"] == job.job_id

    finally:
        scheduler.stop()


def test_both_handlers_on_same_job(scheduler_without_global_handlers):
    """Test that both on_failure and on_success can be set on the same job."""
    scheduler = scheduler_without_global_handlers
    tracker = CallbackTracker()

    def failure_handler(job_id: str, error: Exception, job_info: JobInfo) -> None:
        tracker.record_failure(job_id, error, job_info)

    def success_handler(job_id: str, job_info: JobInfo) -> None:
        tracker.record_success(job_id, job_info)

    # Register both jobs
    fail_func_name = f"{failing_job.__module__}.{failing_job.__name__}"
    success_func_name = f"{successful_job.__module__}.{successful_job.__name__}"
    scheduler.register_job_function(fail_func_name, failing_job)
    scheduler.register_job_function(success_func_name, successful_job)

    # Create job that will fail
    failing = scheduler.create_interval_job(
        func=failing_job,
        seconds=1,
        on_failure=failure_handler,
        on_success=success_handler,
    )

    # Create job that will succeed
    succeeding = scheduler.create_interval_job(
        func=successful_job,
        seconds=1,
        on_failure=failure_handler,
        on_success=success_handler,
    )

    scheduler.start()

    try:
        # Wait for both callbacks
        assert tracker.wait_for_failure(timeout=5.0), "Failure handler was not invoked"
        assert tracker.wait_for_success(timeout=5.0), "Success handler was not invoked"

        # Verify both were recorded
        assert len(tracker.failures) >= 1
        assert len(tracker.successes) >= 1

        # Verify correct job IDs
        assert tracker.failures[0]["job_id"] == failing.job_id
        assert tracker.successes[0]["job_id"] == succeeding.job_id

    finally:
        scheduler.stop()


def test_handler_exception_does_not_crash_scheduler(scheduler_without_global_handlers):
    """Test that exceptions in handlers don't crash the scheduler."""
    scheduler = scheduler_without_global_handlers

    def buggy_failure_handler(  # noqa: ARG001
        job_id: str, error: Exception, job_info: JobInfo
    ) -> None:
        """Handler that raises an exception."""
        raise RuntimeError("Failure handler is buggy!")

    def buggy_success_handler(job_id: str, job_info: JobInfo) -> None:  # noqa: ARG001
        """Handler that raises an exception."""
        raise RuntimeError("Success handler is buggy!")

    # Set buggy handlers
    scheduler.on_failure = buggy_failure_handler
    scheduler.on_success = buggy_success_handler
    scheduler._execution_coordinator.global_on_failure = buggy_failure_handler
    scheduler._execution_coordinator.global_on_success = buggy_success_handler

    # Register both jobs
    fail_func_name = f"{failing_job.__module__}.{failing_job.__name__}"
    success_func_name = f"{successful_job.__module__}.{successful_job.__name__}"
    scheduler.register_job_function(fail_func_name, failing_job)
    scheduler.register_job_function(success_func_name, successful_job)

    # Create jobs
    failing = scheduler.create_interval_job(func=failing_job, seconds=1)
    succeeding = scheduler.create_interval_job(func=successful_job, seconds=1)

    scheduler.start()

    try:
        # Wait for jobs to execute
        time.sleep(3)

        # Scheduler should still be running
        assert scheduler.is_running()

        # Jobs should still have proper status
        failing_job_info = scheduler.get_job(failing.job_id)
        succeeding_job_info = scheduler.get_job(succeeding.job_id)

        assert failing_job_info is not None
        assert failing_job_info.status.value == "failed"
        assert succeeding_job_info is not None
        assert succeeding_job_info.status.value == "scheduled"

    finally:
        scheduler.stop()


async def async_failing_job():
    """Async job that always fails."""
    raise ValueError("Async job intentional failure")


async def async_successful_job():
    """Async job that always succeeds."""
    pass


def test_async_job_callbacks(scheduler_with_global_handlers):
    """Test that async job callbacks are invoked."""
    scheduler = scheduler_with_global_handlers
    tracker: CallbackTracker = scheduler.tracker

    # Register async jobs
    fail_func_name = f"{async_failing_job.__module__}.{async_failing_job.__name__}"
    success_func_name = f"{async_successful_job.__module__}.{async_successful_job.__name__}"
    scheduler.register_job_function(fail_func_name, async_failing_job)
    scheduler.register_job_function(success_func_name, async_successful_job)

    # Create async jobs
    failing = scheduler.create_interval_job(func=async_failing_job, seconds=1)
    succeeding = scheduler.create_interval_job(func=async_successful_job, seconds=1)

    scheduler.start()

    try:
        # Wait for both callbacks
        assert tracker.wait_for_failure(timeout=5.0), "Async failure handler not invoked"
        assert tracker.wait_for_success(timeout=5.0), "Async success handler not invoked"

        # Give async jobs more time to complete
        time.sleep(2)

        # Verify both were recorded
        assert len(tracker.failures) >= 1
        assert len(tracker.successes) >= 1

        # Find the specific successes and failures
        failing_found = any(f["job_id"] == failing.job_id for f in tracker.failures)
        succeeding_found = any(s["job_id"] == succeeding.job_id for s in tracker.successes)

        assert failing_found, f"Async failing job not found in failures: {tracker.failures}"
        assert succeeding_found, f"Async succeeding job not found in successes: {tracker.successes}"

    finally:
        scheduler.stop()
