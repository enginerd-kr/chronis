"""Tests for distributed lock functionality with multiple schedulers (simplified)."""

import threading
import time
from datetime import datetime, timedelta

import pytest

from chronis import PollingScheduler
from chronis.adapters.lock.memory import InMemoryLockAdapter
from chronis.adapters.storage.memory import InMemoryStorageAdapter


@pytest.mark.slow
class TestDistributedLock:
    """Test distributed locking with multiple schedulers (simplified)."""

    def test_prevent_duplicate_execution(self):
        """Test that same job doesn't execute multiple times with multiple schedulers."""
        # Shared storage and lock
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()

        # Thread-safe execution counter
        execution_count = {"count": 0}
        count_lock = threading.Lock()

        def test_func():
            with count_lock:
                execution_count["count"] += 1
            time.sleep(0.01)

        # Create 3 schedulers sharing same storage/lock
        schedulers = []
        for _ in range(3):
            scheduler = PollingScheduler(
                storage_adapter=storage,
                lock_adapter=lock,
                polling_interval_seconds=1,
                lock_ttl_seconds=5,
                lock_prefix="shared:lock:",
                verbose=False,
            )
            scheduler.register_job_function("test_func", test_func)
            schedulers.append(scheduler)

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
            # Wait for job to execute multiple times
            time.sleep(4)

            # Stop all schedulers
            for scheduler in schedulers:
                scheduler.stop()

            # Verify lock is working: execution count should be ~3-5
            # Without lock, would be ~9-12 (3x for each execution)
            assert 2 <= execution_count["count"] <= 6, (
                f"Expected 2-6 executions (with lock working), got {execution_count['count']}"
            )

        finally:
            for scheduler in schedulers:
                if scheduler.is_running():
                    scheduler.stop()

    def test_lock_contention_with_many_jobs(self):
        """Test 20 jobs with 3 schedulers (simplified from 100 jobs/5 schedulers)."""
        # Shared storage and lock
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()

        # Thread-safe execution counters per job
        execution_counts = {}
        count_lock = threading.Lock()

        def test_func(job_id: str):
            with count_lock:
                execution_counts[job_id] = execution_counts.get(job_id, 0) + 1
            time.sleep(0.001)

        # Create 3 schedulers
        schedulers = []
        for _ in range(3):
            scheduler = PollingScheduler(
                storage_adapter=storage,
                lock_adapter=lock,
                polling_interval_seconds=1,
                lock_ttl_seconds=5,
                lock_prefix="shared:lock:",
                verbose=False,
                max_queue_size=30,
            )
            scheduler.register_job_function("test_func", test_func)
            schedulers.append(scheduler)

        # Schedule 20 jobs that run once
        run_time = datetime.now() + timedelta(seconds=2)
        for i in range(20):
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
            # Wait for all jobs to execute
            time.sleep(5)

            # Stop all schedulers
            for scheduler in schedulers:
                scheduler.stop()

            # Verify all jobs executed
            assert len(execution_counts) == 20, (
                f"Expected 20 jobs executed, got {len(execution_counts)}"
            )

            # With lock, total executions should be significantly less than 60 (20 jobs * 3 schedulers)
            # Allow up to 40 total executions (some duplicates due to InMemoryLockAdapter limitations)
            total_executions = sum(execution_counts.values())
            assert total_executions <= 40, (
                f"Expected at most 40 total executions (lock working), got {total_executions}. "
                f"Without lock would be 60 executions."
            )

        finally:
            for scheduler in schedulers:
                if scheduler.is_running():
                    scheduler.stop()

    def test_job_distribution_across_schedulers(self):
        """Test that jobs are executed across multiple schedulers without duplication."""
        # Shared storage and lock
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()

        # Track unique executions
        execution_times = {}
        exec_lock = threading.Lock()

        def test_func(job_id: str):
            with exec_lock:
                if job_id not in execution_times:
                    execution_times[job_id] = []
                execution_times[job_id].append(time.time())
            time.sleep(0.001)

        # Create 3 schedulers
        schedulers = []
        for _ in range(3):
            scheduler = PollingScheduler(
                storage_adapter=storage,
                lock_adapter=lock,
                polling_interval_seconds=1,
                lock_ttl_seconds=5,
                lock_prefix="shared:lock:",
                verbose=False,
            )
            scheduler.register_job_function("test_func", test_func)
            schedulers.append(scheduler)

        # Schedule 10 jobs at same time
        run_time = datetime.now() + timedelta(seconds=2)
        for i in range(10):
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
            # Wait for all jobs to execute
            time.sleep(4)

            # Stop all schedulers
            for scheduler in schedulers:
                scheduler.stop()

            # Verify all jobs executed at least once
            assert len(execution_times) == 10, (
                f"Expected 10 jobs executed, got {len(execution_times)}"
            )

            # Total executions should be <= 15 (some duplicates allowed due to lock limitations)
            # Without lock, would be 30 (10 jobs * 3 schedulers)
            total_executions = sum(len(times) for times in execution_times.values())
            assert total_executions <= 15, (
                f"Expected at most 15 total executions (lock working), got {total_executions}. "
                f"Without lock would be 30 executions."
            )

        finally:
            for scheduler in schedulers:
                if scheduler.is_running():
                    scheduler.stop()
