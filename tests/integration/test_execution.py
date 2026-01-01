"""Integration tests for job execution (direct method calls, deterministic)."""

from datetime import UTC, datetime

import pytest
from conftest import ExecutionTracker, execute_job_immediately


@pytest.fixture
def scheduler(fast_scheduler):
    """Use fast_scheduler from conftest."""
    return fast_scheduler


class TestJobExecution:
    """Test job execution integration."""

    def test_job_executes_successfully(self, scheduler):
        """Test that job function executes and completes."""
        tracker = ExecutionTracker()

        scheduler.register_job_function("test_func", lambda: tracker.record("test_func"))

        # Create interval job
        job = scheduler.create_interval_job(func="test_func", seconds=30)

        # Execute directly
        execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # Verify executed
        assert tracker.count() == 1

        # Interval job should still exist and be scheduled
        updated = scheduler.storage.get_job(job.job_id)
        assert updated is not None
        assert updated["status"] == "scheduled"

    def test_one_time_job_deletes_after_success(self, scheduler):
        """Test that one-time job is deleted after success."""
        tracker = ExecutionTracker()

        scheduler.register_job_function("test_func", lambda: tracker.record("test_func"))

        # Create interval job, but we'll manually delete it
        # (since we can't use date job with past date)
        job = scheduler.create_interval_job(func="test_func", seconds=30)

        # Manually change to DATE trigger to test one-time behavior
        scheduler.storage.update_job(
            job.job_id,
            {"trigger_type": "date"},
        )

        # Execute
        result = execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # One-time job should be deleted
        assert result is None
        assert tracker.count() == 1

    def test_job_with_arguments(self, scheduler):
        """Test that job arguments are passed correctly."""
        results = []

        def job_with_args(name: str, count: int):
            results.append({"name": name, "count": count})

        scheduler.register_job_function("job_with_args", job_with_args)

        # Create job with arguments
        job = scheduler.create_interval_job(
            func="job_with_args",
            seconds=30,
            args=("test",),
            kwargs={"count": 5},
        )

        # Execute
        execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # Verify arguments passed
        assert len(results) == 1
        assert results[0]["name"] == "test"
        assert results[0]["count"] == 5


class TestRetryExecution:
    """Test retry execution integration."""

    def test_failed_job_schedules_retry(self, scheduler):
        """Test that failed job schedules retry."""
        tracker = ExecutionTracker()

        def failing_job():
            tracker.record("failing_job")
            raise ValueError("Fail")

        scheduler.register_job_function("failing_job", failing_job)

        # Create job with retry
        job = scheduler.create_interval_job(
            func="failing_job",
            seconds=30,
            max_retries=2,
            retry_delay_seconds=60,
        )

        # Execute
        execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # Verify retry scheduled
        updated = scheduler.storage.get_job(job.job_id)
        assert updated["retry_count"] == 1
        assert updated["status"] == "scheduled"

        # Next run time should be in future (~60s)
        next_run = datetime.fromisoformat(updated["next_run_time"])
        delay = (next_run - datetime.now(UTC)).total_seconds()
        assert 55 < delay < 65

    def test_exhausted_retries_marks_failed(self, scheduler):
        """Test that exhausted retries marks job as failed."""
        tracker = ExecutionTracker()

        def failing_job():
            tracker.record("failing_job")
            raise ValueError("Fail")

        scheduler.register_job_function("failing_job", failing_job)

        # Create job with NO retries
        job = scheduler.create_interval_job(
            func="failing_job",
            seconds=30,
            max_retries=0,
        )

        # Execute
        execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # Should be FAILED
        updated = scheduler.storage.get_job(job.job_id)
        assert updated["status"] == "failed"
        assert tracker.count() == 1


class TestCallbackExecution:
    """Test callback execution integration."""

    def test_success_callback_called(self, scheduler):
        """Test that success callback is called."""
        success_calls = []

        def on_success(job_id, job_info):
            success_calls.append(job_id)

        scheduler.on_success = on_success
        scheduler._execution_coordinator.global_on_success = on_success

        def successful_job():
            pass

        scheduler.register_job_function("successful_job", successful_job)

        job = scheduler.create_interval_job(func="successful_job", seconds=30)

        # Execute
        execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # Callback should be called
        assert len(success_calls) == 1
        assert success_calls[0] == job.job_id

    def test_failure_callback_called(self, scheduler):
        """Test that failure callback is called."""
        failure_calls = []

        def on_failure(job_id, error, job_info):
            failure_calls.append((job_id, str(error)))

        scheduler.on_failure = on_failure
        scheduler._execution_coordinator.global_on_failure = on_failure

        def failing_job():
            raise ValueError("Test error")

        scheduler.register_job_function("failing_job", failing_job)

        job = scheduler.create_interval_job(func="failing_job", seconds=30, max_retries=0)

        # Execute
        execute_job_immediately(scheduler, job.job_id, wait_seconds=0.5)

        # Callback should be called
        assert len(failure_calls) == 1
        assert "Test error" in failure_calls[0][1]
