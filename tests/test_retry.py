"""Test retry functionality."""

import time
from datetime import datetime, timedelta

import pytest

from chronis import PollingScheduler
from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.state import JobStatus


class TestRetryFunctionality:
    """Test job retry with exponential backoff."""

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

    def test_job_retry_on_failure(self, scheduler):
        """Test that job retries on failure with exponential backoff."""
        # Track execution attempts
        execution_attempts = []

        def failing_job():
            """Job that fails on first 2 attempts, succeeds on 3rd."""
            execution_attempts.append(datetime.now())
            if len(execution_attempts) < 3:
                raise ValueError(f"Attempt {len(execution_attempts)} failed")
            # Success on 3rd attempt

        # Register job function
        scheduler.register_job_function("failing_job", failing_job)

        # Create job with retry (max 3 retries, 2 second base delay)
        job = scheduler.create_interval_job(
            func="failing_job",
            seconds=10,
            max_retries=3,
            retry_delay_seconds=2,
        )

        assert job.max_retries == 3
        assert job.retry_delay_seconds == 2
        assert job.retry_count == 0

        # Start scheduler
        scheduler.start()

        # Wait for job to execute and retries
        # Initial execution at ~10s, retry at +2s, retry at +4s, retry at +8s
        # Total: ~24s
        time.sleep(26)

        # Should have 3 attempts (succeeds on 3rd)
        assert len(execution_attempts) >= 3

        # Verify exponential backoff delays (first 2 retries)
        if len(execution_attempts) >= 3:
            delay1 = (execution_attempts[1] - execution_attempts[0]).total_seconds()
            delay2 = (execution_attempts[2] - execution_attempts[1]).total_seconds()

            # First retry: ~2s (2 * 2^0), allowing some tolerance
            assert 1.5 < delay1 < 4.5

            # Second retry: ~4s (2 * 2^1), allowing some tolerance
            assert 3.5 < delay2 < 6.5

        # Check final job status (should be SCHEDULED after success)
        final_job = scheduler.get_job(job.job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.SCHEDULED
        assert final_job.retry_count == 0  # Reset after success

        scheduler.stop()

    def test_job_fails_after_max_retries(self, scheduler):
        """Test that job fails permanently after exceeding max retries."""
        # Track execution attempts
        execution_attempts = []

        def always_failing_job():
            """Job that always fails."""
            execution_attempts.append(datetime.now())
            raise ValueError("Always fails")

        # Register job function
        scheduler.register_job_function("always_failing_job", always_failing_job)

        # Create job with 2 retries
        job = scheduler.create_interval_job(
            func="always_failing_job",
            seconds=10,
            max_retries=2,
            retry_delay_seconds=1,
        )

        # Start scheduler
        scheduler.start()

        # Wait for all retries
        # Initial at ~10s, retry at +1s, retry at +2s, retry at +4s = ~17s total
        time.sleep(19)

        # Should have 3 total attempts (initial + 2 retries)
        assert len(execution_attempts) == 3

        # Job should be in FAILED status
        final_job = scheduler.get_job(job.job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.FAILED
        assert final_job.retry_count == 2

        scheduler.stop()

    def test_job_no_retry_by_default(self, scheduler):
        """Test that jobs don't retry by default (max_retries=0)."""
        execution_count = []

        def failing_job_no_retry():
            """Job that fails."""
            execution_count.append(1)
            raise ValueError("Fails")

        # Register job function
        scheduler.register_job_function("failing_job_no_retry", failing_job_no_retry)

        # Create job without retry (default)
        job = scheduler.create_interval_job(
            func="failing_job_no_retry",
            seconds=10,
        )

        assert job.max_retries == 0
        assert job.retry_count == 0

        # Start scheduler
        scheduler.start()

        # Wait for initial execution
        time.sleep(12)

        # Should have only 1 attempt
        assert len(execution_count) == 1

        # Job should be in FAILED status
        final_job = scheduler.get_job(job.job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.FAILED

        scheduler.stop()

    def test_retry_with_success_handler(self, scheduler):
        """Test that success handler is called after successful retry."""
        execution_attempts = []
        success_called = []

        def failing_then_success():
            """Fails once, then succeeds."""
            execution_attempts.append(1)
            if len(execution_attempts) == 1:
                raise ValueError("First attempt fails")

        def on_success(job_id, _job_info):
            """Success handler."""
            success_called.append(job_id)

        # Register job function
        scheduler.register_job_function("failing_then_success", failing_then_success)

        # Create job with retry and success handler
        job = scheduler.create_interval_job(
            func="failing_then_success",
            seconds=10,
            max_retries=2,
            retry_delay_seconds=1,
            on_success=on_success,
        )

        # Start scheduler
        scheduler.start()

        # Wait for retry and success
        # Initial at ~10s fails, retry at +1s succeeds = ~12s total
        time.sleep(14)

        # Should have 2 attempts (1 fail + 1 success)
        assert len(execution_attempts) == 2

        # Success handler should be called
        assert len(success_called) == 1
        assert success_called[0] == job.job_id

        scheduler.stop()

    def test_retry_with_failure_handler(self, scheduler):
        """Test that failure handler is called only after all retries exhausted."""
        execution_attempts = []
        failure_called = []

        def always_fails():
            """Always fails."""
            execution_attempts.append(1)
            raise ValueError("Always fails")

        def on_failure(job_id, error, _job_info):
            """Failure handler."""
            failure_called.append((job_id, str(error)))

        # Register job function
        scheduler.register_job_function("always_fails", always_fails)

        # Create job with retry and failure handler
        job = scheduler.create_interval_job(
            func="always_fails",
            seconds=10,
            max_retries=2,
            retry_delay_seconds=1,
            on_failure=on_failure,
        )

        # Start scheduler
        scheduler.start()

        # Wait for all retries
        # Initial at ~10s, retry at +1s, retry at +2s, retry at +4s = ~17s total
        time.sleep(19)

        # Should have 3 attempts (initial + 2 retries)
        assert len(execution_attempts) == 3

        # Failure handler should be called once (after all retries)
        assert len(failure_called) == 1
        assert failure_called[0][0] == job.job_id

        scheduler.stop()

    def test_retry_delay_cap(self, scheduler):
        """Test that retry delay is capped at 1 hour (3600s)."""
        execution_attempts = []

        def failing_job():
            """Job that fails."""
            execution_attempts.append(datetime.now())
            raise ValueError("Fails")

        # Register job function
        scheduler.register_job_function("failing_job_cap", failing_job)

        # Create job with large retry count
        # With base delay 60s, 10th retry would be 60 * 2^9 = 30720s without cap
        job = scheduler.create_date_job(
            func="failing_job_cap",
            run_date=datetime.now() + timedelta(seconds=1),
            max_retries=10,
            retry_delay_seconds=60,
        )

        # Start scheduler
        scheduler.start()

        # Wait for first failure and retry scheduling
        time.sleep(3)

        # Get job info to check next_run_time
        job_info = scheduler.get_job(job.job_id)
        assert job_info is not None

        # Should be scheduled for retry
        assert job_info.status == JobStatus.SCHEDULED
        assert job_info.retry_count == 1

        # Next run should be within ~60 seconds (first retry: 60 * 2^0 = 60s)
        if job_info.next_run_time:
            delay = (job_info.next_run_time - datetime.now().astimezone()).total_seconds()
            assert 55 < delay < 65

        scheduler.stop()
