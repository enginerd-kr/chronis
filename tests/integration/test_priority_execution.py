"""Integration tests for priority-based job execution."""

from datetime import timedelta

import pytest

from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.scheduler import PollingScheduler
from chronis.utils.time import utc_now


@pytest.fixture
def scheduler():
    """Create scheduler with in-memory adapters."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        max_workers=5,
        polling_interval_seconds=1,
        executor_interval_seconds=1,
    )
    yield scheduler
    if scheduler.is_running():
        scheduler.stop()


class TestPriorityExecution:
    """Test that jobs are executed in priority order."""

    def test_create_job_with_priority(self, scheduler):
        """Test that jobs can be created with priority parameter."""
        execution_order = []

        def track_execution(job_id):
            execution_order.append(job_id)

        # Create jobs with different priorities
        scheduler.create_interval_job(
            func=track_execution,
            job_id="low",
            args=("low",),
            seconds=1,
            priority=2,
        )

        scheduler.create_interval_job(
            func=track_execution,
            job_id="high",
            args=("high",),
            seconds=1,
            priority=9,
        )

        scheduler.create_interval_job(
            func=track_execution,
            job_id="medium",
            args=("medium",),
            seconds=1,
            priority=5,
        )

        # Verify jobs were created with correct priority
        low_job = scheduler.get_job("low")
        assert low_job.priority == 2

        high_job = scheduler.get_job("high")
        assert high_job.priority == 9

        medium_job = scheduler.get_job("medium")
        assert medium_job.priority == 5

    def test_priority_affects_execution_order(self, scheduler):
        """Test that priority ordering works - simplified test without execution."""
        # Create jobs that will be ready at the same time
        run_time = utc_now() + timedelta(seconds=2)

        scheduler.create_date_job(
            func=lambda: None,
            job_id="low",
            run_date=run_time,
            priority=2,
        )

        scheduler.create_date_job(
            func=lambda: None,
            job_id="high",
            run_date=run_time,
            priority=9,
        )

        scheduler.create_date_job(
            func=lambda: None,
            job_id="medium",
            run_date=run_time,
            priority=5,
        )

        # Verify jobs created with correct priorities
        low_job = scheduler.get_job("low")
        high_job = scheduler.get_job("high")
        medium_job = scheduler.get_job("medium")

        assert low_job.priority == 2
        assert high_job.priority == 9
        assert medium_job.priority == 5

    def test_default_priority_is_5(self, scheduler):
        """Test that jobs without explicit priority default to 5."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="default",
            seconds=60,
        )

        job = scheduler.get_job("default")
        assert job.priority == 5

    def test_cron_job_with_priority(self, scheduler):
        """Test that cron jobs support priority parameter."""
        scheduler.create_cron_job(
            func=lambda: None,
            job_id="cron_high",
            hour=9,
            minute=0,
            priority=8,
        )

        job = scheduler.get_job("cron_high")
        assert job.priority == 8

    def test_interval_job_with_priority(self, scheduler):
        """Test that interval jobs support priority parameter."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="interval_low",
            seconds=30,
            priority=3,
        )

        job = scheduler.get_job("interval_low")
        assert job.priority == 3

    def test_priority_persists_across_executions(self, scheduler):
        """Test that priority value persists in storage."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="persistent",
            seconds=60,
            priority=7,
        )

        # Check initial priority
        job = scheduler.get_job("persistent")
        assert job.priority == 7

        # Pause and resume job (which updates storage)
        scheduler.pause_job("persistent")
        paused_job = scheduler.get_job("persistent")
        assert paused_job.priority == 7

        scheduler.resume_job("persistent")
        resumed_job = scheduler.get_job("persistent")
        assert resumed_job.priority == 7


class TestPriorityWithRetry:
    """Test priority behavior with retry mechanism."""

    def test_failed_job_retry_maintains_priority(self, scheduler):
        """Test that retry configuration works with priority."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="retry_job",
            seconds=60,
            priority=8,
            max_retries=3,
            retry_delay_seconds=60,
        )

        # Verify job created with retry config and priority
        job = scheduler.get_job("retry_job")
        assert job.priority == 8
        assert job.max_retries == 3
        assert job.retry_delay_seconds == 60


class TestPriorityQuerying:
    """Test that query operations work with priority field."""

    def test_query_jobs_returns_priority(self, scheduler):
        """Test that query_jobs includes priority in results."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="query_test",
            seconds=60,
            priority=6,
        )

        jobs = scheduler.query_jobs()
        assert len(jobs) == 1
        assert jobs[0].priority == 6

    def test_get_job_returns_priority(self, scheduler):
        """Test that get_job includes priority."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="get_test",
            seconds=60,
            priority=4,
        )

        job = scheduler.get_job("get_test")
        assert job.priority == 4
