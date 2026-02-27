"""E2E tests for scheduler background execution (real timing)."""

from datetime import UTC, datetime, timedelta

import pytest
from conftest import wait_for

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler


@pytest.fixture
def scheduler():
    """Create scheduler with realistic intervals."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Fast for testing
        executor_interval_seconds=1,
        verbose=False,
    )
    yield scheduler
    if scheduler.is_running():
        scheduler.stop()


@pytest.mark.e2e
@pytest.mark.slow
class TestBackgroundPolling:
    """Test that background polling works (E2E only)."""

    def test_interval_job_executes_repeatedly(self, scheduler, thread_safe_counter):
        """Test that interval job executes multiple times via background polling."""

        def interval_task():
            thread_safe_counter.increment()

        scheduler.register_job_function("interval_task", interval_task)

        # Create interval job with 2 second interval
        scheduler.create_interval_job(func="interval_task", seconds=2)

        # Start background scheduler
        scheduler.start()

        try:
            # Wait for at least 2 executions (first at ~2s, second at ~4s)
            wait_for(
                lambda: thread_safe_counter.value >= 2,
                timeout=8,
                error_message=f"Expected at least 2 executions, got {thread_safe_counter.value}",
            )

            # Verify we got at least 2 executions
            assert thread_safe_counter.value >= 2

        finally:
            scheduler.stop()

    def test_cron_job_executes_at_scheduled_time(self, scheduler, thread_safe_counter):
        """Test that cron job actually executes at the scheduled minute.

        This creates a cron job scheduled for the next minute and verifies it executes.
        """

        def cron_task():
            thread_safe_counter.increment()

        scheduler.register_job_function("cron_task", cron_task)

        # Schedule for next minute (use UTC to match timezone="UTC" in job creation)
        # Example: if now is 10:30:45 UTC, schedule for 10:31:00 UTC
        now = datetime.now(UTC)
        next_minute = now + timedelta(minutes=1)
        target_minute = next_minute.minute

        # Create cron job for next minute
        job = scheduler.create_cron_job(
            func="cron_task",
            minute=target_minute,
            timezone="UTC",
        )

        # Start scheduler
        scheduler.start()

        try:
            # Wait up to 90 seconds for execution (next minute + buffer)
            # The job should execute within 60 seconds, but we add buffer for CI
            wait_for(
                lambda: thread_safe_counter.value >= 1,
                timeout=90,
                error_message=f"Expected cron job to execute, got {thread_safe_counter.value} executions",
            )

            # Verify job executed
            assert thread_safe_counter.value >= 1

            # Job should remain scheduled (cron jobs are recurring)
            final_job = scheduler.get_job(job.job_id)
            assert final_job is not None
            assert final_job.status.value == "scheduled"
            assert final_job.next_run_time is not None
            assert final_job.next_run_time.minute == target_minute

        finally:
            scheduler.stop()
