"""Tests for distributed lock functionality with multiple schedulers (simplified)."""

import time
from datetime import datetime, timedelta

import pytest
from conftest import wait_for


@pytest.mark.slow
class TestDistributedLock:
    """Test distributed locking with multiple schedulers (simplified)."""

    def test_prevent_duplicate_execution(self, distributed_schedulers, thread_safe_counter):
        """Test that same job doesn't execute multiple times with multiple schedulers."""
        schedulers, storage, lock = distributed_schedulers

        def test_func():
            thread_safe_counter.increment()
            time.sleep(0.01)

        # Register function on all schedulers
        for scheduler in schedulers:
            scheduler.register_job_function("test_func", test_func)

        # Schedule a job that runs with short interval
        schedulers[0].create_interval_job(
            job_id="test_job",
            name="Test Job",
            func="test_func",
            seconds=1,
        )

        # Start all schedulers
        for scheduler in schedulers:
            scheduler.start()

        try:
            # Wait for at least 2 executions (first at ~1s, second at ~2s, with polling delays)
            wait_for(
                lambda: thread_safe_counter.value >= 2,
                timeout=8,
                error_message=f"Expected at least 2 executions, got {thread_safe_counter.value}",
            )

            # Stop all schedulers
            for scheduler in schedulers:
                scheduler.stop()

            # Verify lock is working: execution count should be 2-5
            # Without lock, would be 6-12 (3 schedulers × 2-4 executions each)
            assert 2 <= thread_safe_counter.value <= 5, (
                f"Expected 2-5 executions (lock working), got {thread_safe_counter.value}"
            )

        finally:
            for scheduler in schedulers:
                if scheduler.is_running():
                    scheduler.stop()

    @pytest.mark.parametrize("n_jobs", [10, 20])
    def test_job_distribution_with_lock(self, distributed_schedulers, thread_safe_tracker, n_jobs):
        """Test that jobs are distributed across schedulers without excessive duplication.

        This test combines the previous test_lock_contention_with_many_jobs (20 jobs)
        and test_job_distribution_across_schedulers (10 jobs) into one parametrized test.
        """
        schedulers, storage, lock = distributed_schedulers

        def test_func(job_id: str):
            thread_safe_tracker.record(job_id)
            time.sleep(0.001)

        # Register function on all schedulers
        for scheduler in schedulers:
            scheduler.register_job_function("test_func", test_func)

        # Schedule n_jobs jobs at same time
        run_time = datetime.now() + timedelta(seconds=2)
        for i in range(n_jobs):
            job_id = f"job_{i}"
            schedulers[i % 3].create_date_job(
                job_id=job_id,
                name=f"Job {i}",
                func="test_func",
                run_date=run_time,
                kwargs={"job_id": job_id},
            )

        # Start all schedulers
        for scheduler in schedulers:
            scheduler.start()

        try:
            # Wait for all jobs to execute at least once
            wait_for(
                lambda: thread_safe_tracker.job_count() >= n_jobs,
                timeout=8,
                error_message=(f"Expected {n_jobs} jobs, got {thread_safe_tracker.job_count()}"),
            )

            # Stop all schedulers
            for scheduler in schedulers:
                scheduler.stop()

            # Verify all jobs executed
            assert thread_safe_tracker.job_count() == n_jobs

            # With lock, total executions should be significantly less than n_jobs * 3
            # Allow up to 2x duplicates (some duplicates due to InMemoryLockAdapter limitations)
            max_expected = n_jobs * 2
            total_executions = thread_safe_tracker.total_count()
            assert total_executions <= max_expected, (
                f"Expected at most {max_expected} total executions (lock working), "
                f"got {total_executions}. Without lock would be {n_jobs * 3} executions."
            )

        finally:
            for scheduler in schedulers:
                if scheduler.is_running():
                    scheduler.stop()
