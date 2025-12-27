"""E2E tests for scheduler background execution (real timing, APScheduler)."""

import time

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
    """Test that APScheduler background polling works (E2E only)."""

    def test_interval_job_executes_repeatedly(self, scheduler):
        """Test that interval job executes multiple times via background polling."""
        executions = []

        def interval_task():
            executions.append(time.time())

        scheduler.register_job_function("interval_task", interval_task)

        # Create interval job with 2 second interval
        scheduler.create_interval_job(func="interval_task", seconds=2)

        # Start background scheduler (real APScheduler)
        scheduler.start()

        try:
            # Wait for at least 2 executions (first at ~2s, second at ~4s)
            wait_for(
                lambda: len(executions) >= 2,
                timeout=8,
                error_message=f"Expected at least 2 executions, got {len(executions)}",
            )

            # Verify interval timing (with generous tolerance for CI)
            if len(executions) >= 2:
                interval = executions[1] - executions[0]
                # Allow generous tolerance due to polling + execution delays (1s - 5s)
                assert 1.0 < interval < 5.0, f"Expected ~2s interval, got {interval}s"

        finally:
            scheduler.stop()

    def test_cron_job_scheduled_correctly(self, scheduler):
        """Test that cron job is scheduled (not executed, just verified scheduled)."""

        def cron_task():
            pass

        scheduler.register_job_function("cron_task", cron_task)

        # Create cron job for 9 AM
        job = scheduler.create_cron_job(func="cron_task", hour=9, minute=0, timezone="UTC")

        # Start scheduler
        scheduler.start()

        try:
            # Wait a moment
            time.sleep(1)

            # Job should remain scheduled (not execute since it's not 9 AM)
            final_job = scheduler.get_job(job.job_id)
            assert final_job is not None
            assert final_job.status.value == "scheduled"
            assert final_job.next_run_time is not None
            assert final_job.next_run_time.hour == 9
            assert final_job.next_run_time.minute == 0

        finally:
            scheduler.stop()


@pytest.mark.e2e
@pytest.mark.slow
class TestDistributedExecution:
    """Test distributed execution (E2E only)."""

    def test_multiple_schedulers_no_duplicate_execution(self):
        """Test that multiple schedulers don't execute same job multiple times."""
        # Shared storage and lock
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()

        import threading

        execution_count = {"count": 0}
        count_lock = threading.Lock()

        def test_func():
            with count_lock:
                execution_count["count"] += 1
            time.sleep(0.05)  # Small delay

        # Create 2 schedulers sharing same storage/lock
        schedulers = []
        for _ in range(2):
            scheduler = PollingScheduler(
                storage_adapter=storage,
                lock_adapter=lock,
                polling_interval_seconds=1,
                lock_ttl_seconds=10,
                lock_prefix="shared:lock:",
                verbose=False,
            )
            scheduler.register_job_function("test_func", test_func)
            schedulers.append(scheduler)

        # Create job with 2 second interval
        schedulers[0].create_interval_job(job_id="shared-job", func="test_func", seconds=2)

        # Start all schedulers
        for scheduler in schedulers:
            scheduler.start()

        try:
            # Wait for ~2 executions (first at ~2s, second at ~4s)
            time.sleep(5)

            # Stop all
            for scheduler in schedulers:
                scheduler.stop()

            # With lock working: 2-3 executions (some timing variance)
            # Without lock: 4-6 executions (2 schedulers × 2-3 executions each)
            assert 1 <= execution_count["count"] <= 3, (
                f"Expected 1-3 executions (lock working), got {execution_count['count']}"
            )

        finally:
            for scheduler in schedulers:
                if scheduler.is_running():
                    scheduler.stop()
