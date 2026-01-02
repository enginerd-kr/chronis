"""Tests for graceful shutdown of AsyncExecutor and PollingScheduler."""

import asyncio
import time
from datetime import UTC, datetime, timedelta

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
from chronis.core.execution.async_loop import AsyncExecutor


class TestAsyncExecutorGracefulShutdown:
    """Test AsyncExecutor graceful shutdown behavior."""

    def test_stop_waits_for_async_task_to_complete(self):
        """Test that stop() waits for running async tasks to complete."""
        executor = AsyncExecutor()
        executor.start()

        execution_log = []

        async def long_task():
            execution_log.append("start")
            await asyncio.sleep(2)
            execution_log.append("end")

        # Start async task
        executor.execute_coroutine(long_task())

        # Give task time to start
        time.sleep(0.5)

        # Stop should wait for task
        start_time = time.time()
        result = executor.stop(timeout=5.0)
        stop_duration = time.time() - start_time

        # Verify
        assert result is True, "Task should complete successfully"
        assert "start" in execution_log, "Task should have started"
        assert "end" in execution_log, "Task should have completed"
        assert stop_duration >= 1.4, (
            "stop() should have waited for task"
        )  # Allow small timing variance

    def test_stop_timeout_cancels_long_running_task(self):
        """Test that stop() times out and cancels tasks that exceed timeout."""
        executor = AsyncExecutor()
        executor.start()

        execution_log = []

        async def very_long_task():
            execution_log.append("start")
            try:
                await asyncio.sleep(10)  # 10 seconds
                execution_log.append("end")
            except asyncio.CancelledError:
                execution_log.append("cancelled")
                raise

        # Start async task
        executor.execute_coroutine(very_long_task())

        # Give task time to start
        time.sleep(0.5)

        # Stop with short timeout
        start_time = time.time()
        result = executor.stop(timeout=1.0)
        stop_duration = time.time() - start_time

        # Verify
        assert result is False, "Should return False on timeout"
        assert "start" in execution_log, "Task should have started"
        assert "end" not in execution_log, "Task should NOT have completed normally"
        assert stop_duration < 3.0, "stop() should timeout quickly"

    def test_stop_with_no_running_tasks(self):
        """Test that stop() returns immediately when no tasks are running."""
        executor = AsyncExecutor()
        executor.start()

        # Stop without any tasks
        start_time = time.time()
        result = executor.stop(timeout=5.0)
        stop_duration = time.time() - start_time

        # Verify
        assert result is True, "Should complete successfully with no tasks"
        assert stop_duration < 0.5, "Should return immediately with no tasks"

    def test_stop_when_not_started(self):
        """Test that stop() returns True when executor was never started."""
        executor = AsyncExecutor()

        # Stop without starting
        result = executor.stop()

        # Verify
        assert result is True, "Should return True when not started"
        assert executor._loop is None


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
        result = scheduler.stop(timeout=10.0)
        stop_duration = time.time() - start_time

        # Verify
        assert result["async_jobs_completed"] is True, "Async job should complete"
        assert "start" in execution_log
        assert "end" in execution_log
        assert stop_duration >= 0.5, "stop() should have waited"

    def test_stop_timeout_on_long_async_job(self):
        """Test that stop() times out on very long async jobs."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            verbose=False,
        )

        execution_log = []

        async def very_long_async_job():
            execution_log.append("start")
            try:
                await asyncio.sleep(20)  # Very long
                execution_log.append("end")
            except asyncio.CancelledError:
                execution_log.append("cancelled")
                raise

        scheduler.register_job_function("long_job", very_long_async_job)
        scheduler.create_date_job(
            func="long_job", run_date=datetime.now(UTC) + timedelta(seconds=1)
        )

        scheduler.start()
        time.sleep(2.5)  # Let job start

        # Stop with short timeout
        start_time = time.time()
        result = scheduler.stop(timeout=2.0)
        stop_duration = time.time() - start_time

        # Verify
        assert result["async_jobs_completed"] is False, "Should timeout"
        assert "start" in execution_log
        assert "end" not in execution_log
        assert stop_duration < 5.0, "Should timeout quickly"

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

    def test_stop_custom_timeout(self):
        """Test that custom timeout parameter works."""
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

        # Stop with sufficient timeout
        result = scheduler.stop(timeout=10.0)

        # Verify
        assert result["async_jobs_completed"] is True
        assert "end" in execution_log
