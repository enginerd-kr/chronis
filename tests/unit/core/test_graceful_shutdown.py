"""Tests for graceful shutdown of PollingScheduler."""

import asyncio
import time
from datetime import UTC, datetime, timedelta

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler


class TestPollingSchedulerGracefulShutdown:
    """Test PollingScheduler graceful shutdown behavior."""

    def test_stop_waits_for_sync_job(self):
        """Test that stop() waits for sync jobs to complete."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            verbose=False,
        )

        execution_log = []

        def long_sync_job():
            execution_log.append("start")
            time.sleep(3)
            execution_log.append("end")

        scheduler.register_job_function("long_job", long_sync_job)
        scheduler.create_date_job(
            func="long_job", run_date=datetime.now(UTC) + timedelta(seconds=1)
        )

        scheduler.start()
        time.sleep(2.5)  # Let job start

        # Stop should wait
        start_time = time.time()
        result = scheduler.stop()
        stop_duration = time.time() - start_time

        # Verify
        assert result["sync_jobs_completed"] is True
        assert "start" in execution_log
        assert "end" in execution_log
        assert stop_duration >= 0.5, "stop() should have waited"

    def test_stop_waits_for_async_job(self):
        """Test that stop() waits for async jobs to complete."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            verbose=False,
        )

        execution_log = []

        async def long_async_job():
            execution_log.append("start")
            await asyncio.sleep(3)
            execution_log.append("end")

        scheduler.register_job_function("async_job", long_async_job)
        scheduler.create_date_job(
            func="async_job", run_date=datetime.now(UTC) + timedelta(seconds=1)
        )

        scheduler.start()
        time.sleep(2.5)  # Let job start

        # Stop should wait
        start_time = time.time()
        result = scheduler.stop()
        stop_duration = time.time() - start_time

        # Verify
        assert result["async_jobs_completed"] is True, "Async job should complete"
        assert "start" in execution_log
        assert "end" in execution_log
        assert stop_duration >= 0.5, "stop() should have waited"

    def test_stop_waits_for_async_job_to_complete(self):
        """Test that stop() waits for async jobs (same as sync) since they run in ThreadPool."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            verbose=False,
        )

        execution_log = []

        async def medium_async_job():
            execution_log.append("start")
            await asyncio.sleep(3)
            execution_log.append("end")

        scheduler.register_job_function("async_job", medium_async_job)
        scheduler.create_date_job(
            func="async_job", run_date=datetime.now(UTC) + timedelta(seconds=1)
        )

        scheduler.start()
        time.sleep(2.5)  # Let job start

        # Stop - async jobs now run in ThreadPool so shutdown waits for completion
        start_time = time.time()
        result = scheduler.stop()
        stop_duration = time.time() - start_time

        # Verify: async jobs complete like sync jobs
        assert result["async_jobs_completed"] is True
        assert "start" in execution_log
        assert "end" in execution_log
        assert stop_duration >= 0.5, "stop() should have waited for async job"

    def test_stop_when_not_running(self):
        """Test that stop() returns immediately when scheduler not running."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
        )

        # Stop without starting
        result = scheduler.stop()

        # Verify
        assert result["was_running"] is False
        assert result["sync_jobs_completed"] is True
        assert result["async_jobs_completed"] is True

    def test_stop_waits_for_long_async_job(self):
        """Test that stop() waits for long async jobs since they run in ThreadPool."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            verbose=False,
        )

        execution_log = []

        async def medium_async_job():
            execution_log.append("start")
            await asyncio.sleep(4)
            execution_log.append("end")

        scheduler.register_job_function("job", medium_async_job)
        scheduler.create_date_job(func="job", run_date=datetime.now(UTC) + timedelta(seconds=1))

        scheduler.start()
        time.sleep(2.5)  # Let job start

        # Stop - ThreadPool shutdown waits for all workers
        result = scheduler.stop()

        # Verify
        assert result["async_jobs_completed"] is True
        assert "end" in execution_log
