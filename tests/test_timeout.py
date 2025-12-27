"""Test job timeout functionality."""

import time
from datetime import datetime, timedelta

import pytest

from chronis import PollingScheduler
from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.state import JobStatus


class TestJobTimeout:
    """Test job timeout functionality with minimal waits."""

    @pytest.fixture
    def scheduler(self):
        """Create scheduler with in-memory adapters."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            executor_interval_seconds=1,
            verbose=True,
        )
        yield scheduler
        if scheduler.is_running():
            scheduler.stop()

    def test_sync_job_timeout(self, scheduler):
        """Test that sync job times out after timeout_seconds."""
        execution_started = []

        def long_running_job():
            """Job that runs for 3 seconds."""
            execution_started.append(1)
            time.sleep(3)

        # Register job function
        scheduler.register_job_function("long_running_job", long_running_job)

        # Create job with 1 second timeout (will timeout)
        job = scheduler.create_date_job(
            func="long_running_job",
            run_date=datetime.now() + timedelta(seconds=2),
            timeout_seconds=1,
        )

        assert job.timeout_seconds == 1

        # Start scheduler
        scheduler.start()

        # Wait for execution and timeout
        time.sleep(5)

        # Should have started execution
        assert len(execution_started) >= 1

        # Job should be FAILED (date jobs stay in storage when failed)
        final_job = scheduler.get_job(job.job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.FAILED

        scheduler.stop()

    def test_sync_job_completes_within_timeout(self, scheduler):
        """Test that sync job completes successfully within timeout."""
        execution_count = []

        def quick_job():
            """Job that completes instantly."""
            execution_count.append(1)

        # Register job function
        scheduler.register_job_function("quick_job", quick_job)

        # Create job with generous timeout
        job = scheduler.create_date_job(
            func="quick_job",
            run_date=datetime.now() + timedelta(seconds=2),
            timeout_seconds=5,
        )

        # Start scheduler
        scheduler.start()

        # Wait for execution
        time.sleep(4)

        # Should have executed successfully and been deleted (one-time job)
        assert len(execution_count) == 1

        # Date job auto-deletes after success
        final_job = scheduler.get_job(job.job_id)
        assert final_job is None

        scheduler.stop()

    def test_job_without_timeout(self, scheduler):
        """Test that job without timeout works normally."""
        execution_count = []

        def normal_job():
            """Normal job."""
            execution_count.append(1)

        # Register job function
        scheduler.register_job_function("normal_job", normal_job)

        # Create job without timeout
        job = scheduler.create_interval_job(
            func="normal_job",
            seconds=2,
            timeout_seconds=None,  # No timeout
        )

        assert job.timeout_seconds is None

        # Start scheduler
        scheduler.start()

        # Wait for execution
        time.sleep(4)

        # Should execute normally
        assert len(execution_count) >= 1

        final_job = scheduler.get_job(job.job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.SCHEDULED

        scheduler.stop()

    def test_timeout_with_failure_handler(self, scheduler):
        """Test that failure handler is called on timeout."""
        execution_started = []
        timeout_errors = []

        def slow_job():
            """Job that runs for 3 seconds."""
            execution_started.append(1)
            time.sleep(3)

        def on_timeout(job_id, error, _job_info):
            """Timeout handler."""
            timeout_errors.append((job_id, str(error)))

        # Register job function
        scheduler.register_job_function("slow_job", slow_job)

        # Create job with timeout and failure handler
        job = scheduler.create_date_job(
            func="slow_job",
            run_date=datetime.now() + timedelta(seconds=2),
            timeout_seconds=1,
            on_failure=on_timeout,
        )

        # Start scheduler
        scheduler.start()

        # Wait for timeout
        time.sleep(5)

        # Execution should have started
        assert len(execution_started) >= 1

        # Failure handler should be called with timeout error
        assert len(timeout_errors) >= 1
        assert job.job_id in timeout_errors[0][0]
        assert "timeout" in timeout_errors[0][1].lower()

        scheduler.stop()
